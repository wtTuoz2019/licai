from __future__ import annotations

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from decimal import Decimal

from .config import Settings, d, fmt_amount, is_mmr_sentinel, load_settings, settings_for_account, settle_asset_of
from .cycle import HedgeCycle, margin_use_pct, position_plan
from .monitor import (
    enter_advice,
    exit_ip,
    harvest_advice,
    position_notional,
    price_stability,
    resolve_harvest_fee,
    schedule_hint,
)
from .pipeline import Pipeline, StepResult
from .store import Account, AccountStore
from .webshare import WebshareClient, WebshareError, proxy_endpoint



def _detail(value) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, default=str)


# 收利/补仓诊断必须留住的步骤名关键词（不能只存末尾 4 步）
_KEEP_STEP_KEYS = (
    "触发止盈",
    "平仓",
    "补仓",
    "保证金",
    "转出",
    "可转",
    "归集统一",
    "单边",
    "市价仍单边",
    "挂单未齐",
    "收利完成",
    "平仓异常",
    "平仓跳过",
    "平仓未净",
    "已平",
    "已补",
    "继续平",
    "半平",
    "补齐缺口",
    "数量拉平",
    "对冲已齐",
    "收利未齐",
    "等待平稳",
    "价格已平稳",
)


def _event_summary(steps: list[StepResult], *, limit: int = 2000) -> str:
    """失败步骤 + 关键节点优先入事件，避免只留末 4 步把补仓原因裁掉。"""
    if not steps:
        return "无步骤"

    def line(step: StepResult) -> str:
        flag = "ok" if step.ok else "FAIL"
        return f"{step.name}[{flag}]: {_detail(step.detail)}"

    keep_idx: list[int] = []
    seen: set[int] = set()

    def add(i: int) -> None:
        if i not in seen:
            seen.add(i)
            keep_idx.append(i)

    for i, step in enumerate(steps):
        if not step.ok:
            add(i)
    for i, step in enumerate(steps):
        if any(k in step.name for k in _KEEP_STEP_KEYS):
            add(i)
    # 再按时间顺序补全，直到接近上限
    for i in range(len(steps)):
        add(i)
        draft = "；".join(line(steps[j]) for j in sorted(seen))
        if len(draft) >= limit:
            break

    ordered = sorted(seen)
    text = "；".join(line(steps[i]) for i in ordered)
    if len(text) <= limit:
        return text
    # 超长时：失败优先，再关键，再截断
    failed = [i for i in ordered if not steps[i].ok]
    key = [i for i in ordered if i not in failed and any(k in steps[i].name for k in _KEEP_STEP_KEYS)]
    rest = [i for i in ordered if i not in failed and i not in key]
    picked: list[int] = []
    for group in (failed, key, rest):
        for i in group:
            trial = "；".join(line(steps[j]) for j in sorted(picked + [i]))
            if trial and len(trial) > limit and picked:
                continue
            picked.append(i)
    out = "；".join(line(steps[i]) for i in sorted(picked))
    return out[:limit]


def _sizing_settings(settings: Settings, legs) -> Settings:
    """加仓/入场预览按仓位实际杠杆算，不按目标倍数虚高。"""
    actual = int(getattr(legs, "leverage", 0) or 0)
    if actual > 0 and actual != int(settings.hedge_leverage or 0):
        return replace(settings, hedge_leverage=actual)
    return settings


def steps_payload(steps: list[StepResult]) -> list[dict]:
    return [
        {"name": step.name, "ok": step.ok, "detail": _detail(step.detail), "dry_run": step.dry_run}
        for step in steps
    ]


def format_uni_mmr(mmr: Decimal, has_pos: bool) -> str:
    if is_mmr_sentinel(mmr):
        return "—" if has_pos else "无仓"
    if mmr >= 100:
        return fmt_amount(mmr, 1)
    if mmr >= 10:
        return fmt_amount(mmr, 2)
    return fmt_amount(mmr, 4)


def _events_payload(account_id: int, events) -> list[dict]:
    return [
        {
            "id": event.id,
            "account_id": account_id,
            "action": event.action,
            "ok": event.ok,
            "detail": event.detail,
            "created_at": event.created_at,
        }
        for event in events
    ]


def _harvest_notional(
    legs,
    mid: Decimal,
    plan: dict | None,
    *,
    collateral: Decimal = Decimal("0"),
    settings: Settings | None = None,
) -> Decimal:
    """有仓用实际名义；无仓用预计开仓单边名义，避免门槛退化成 √本金。

    资金在理财里时 available 可能为 0，plan.target_qty 会被压成 0，
    这时按本金×占用×杠杆估算开仓后名义（与 position_plan 一致）。
    """
    live = position_notional(legs, mid)
    if live > 0:
        return live
    if plan:
        qty = d(plan.get("target_qty") or "0")
        if qty > 0 and mid > 0:
            return qty * mid
    if settings and collateral > 0:
        use = margin_use_pct(settings)
        lev = Decimal(max(int(settings.hedge_leverage), 1))
        return collateral * use * lev / Decimal("2")
    return Decimal("0")


def _display_leverage(legs, settings: Settings) -> tuple[int, int]:
    """返回 (展示用实际杠杆, 目标杠杆)。无仓时用 settings 里已按上限压过的有效倍数。"""
    target = int(settings.hedge_leverage_target or settings.hedge_leverage or 0)
    actual = int(getattr(legs, "leverage", 0) or 0)
    if actual <= 0:
        actual = int(settings.hedge_leverage or target or 0)
    if target <= 0:
        target = actual
    return actual, target


def _gather(jobs: dict) -> tuple[dict, dict]:
    if not jobs:
        return {}, {}
    out: dict = {}
    errors: dict = {}
    with ThreadPoolExecutor(max_workers=min(6, len(jobs))) as pool:
        fut_of = {pool.submit(fn): name for name, fn in jobs.items()}
        for fut, name in fut_of.items():
            try:
                out[name] = fut.result()
            except Exception as exc:
                errors[name] = exc
    return out, errors


def pipeline_for(
    account: Account,
    base: Settings | None = None,
    dry_run: bool = True,
    *,
    ops: "OpsService | None" = None,
) -> Pipeline:
    settings = settings_for_account(
        base or load_settings(),
        api_key=account.api_key,
        api_secret=account.api_secret,
        proxy=account.proxy,
        dry_run=dry_run,
        hedge_symbol=account.hedge_symbol,
        hedge_leverage=account.hedge_leverage,
        leverage_cap=account.leverage_cap,
        leverage_unlock_at=account.leverage_unlock_at,
        take_profit_usdt=account.take_profit_usdt,
        take_profit_custom=account.take_profit_custom,
    )
    pipe = Pipeline(settings)
    if ops is not None and account.proxy:
        account_id = account.id

        def _rotate(bad: str, exc: BaseException) -> str | None:
            return ops.rotate_dead_proxy(account_id, bad, exc)

        pipe.client.bind_proxy_rotator(_rotate)
    return pipe


class OpsService:
    def __init__(self, store: AccountStore | None = None, base: Settings | None = None):
        self.store = store or AccountStore()
        self.base = base or load_settings()
        self._snap_cache: dict[int, tuple[float, dict]] = {}
        self._snap_ttl = 86400.0
        self._risk_cache: dict[int, tuple[float, dict]] = {}
        self._risk_ttl = 60.0
        self._action_locks: dict[int, threading.Lock] = {}
        self._action_guard = threading.Lock()
        self._busy: set[int] = set()
        self._webshare = WebshareClient(
            self.base.webshare_api_token,
            mode=self.base.webshare_mode,
            country=self.base.webshare_country or None,
        )
        self._bad_proxy_endpoints: set[str] = set()
        self._proxy_rotate_lock = threading.Lock()

    def webshare_status(self) -> dict:
        ok = self._webshare.configured
        count = 0
        error = ""
        if ok:
            try:
                count = len(self._webshare.list_proxies())
            except WebshareError as exc:
                error = str(exc)
        return {
            "configured": ok,
            "mode": self.base.webshare_mode,
            "country": self.base.webshare_country or "",
            "auto_assign": bool(self.base.webshare_auto_assign),
            "proxy_count": count,
            "bad_marked": len(self._bad_proxy_endpoints),
            "error": error,
        }

    def assign_webshare_proxy(self, account: Account, *, verify: bool = True, force_refresh: bool = True) -> Account:
        if not self._webshare.configured:
            raise WebshareError("未配置 WEBSHARE_API_TOKEN，请在 .env 里填写")
        used_urls, used_endpoints = self._used_proxies(exclude_id=account.id)
        used_endpoints |= set(self._bad_proxy_endpoints)
        picked = self._webshare.pick(
            used_urls=used_urls,
            used_endpoints=used_endpoints,
            force=force_refresh,
            verify=verify,
        )
        updated = self.store.update(account.id, proxy=picked.url)
        self.invalidate_snapshot(account.id)
        self.store.add_event(
            account.id,
            "proxy",
            True,
            f"Webshare 分配 {picked.endpoint}"
            + (f" {picked.country}/{picked.city}" if picked.country or picked.city else ""),
        )
        return updated

    def _used_proxies(self, *, exclude_id: int | None = None) -> tuple[set[str], set[str]]:
        used_urls: set[str] = set()
        used_endpoints: set[str] = set()
        for other in self.store.list_accounts():
            if exclude_id is not None and other.id == exclude_id:
                continue
            if other.proxy:
                used_urls.add(other.proxy.strip())
                ep = proxy_endpoint(other.proxy)
                if ep:
                    used_endpoints.add(ep)
        return used_urls, used_endpoints

    def rotate_dead_proxy(self, account_id: int, bad_proxy: str, exc: BaseException) -> str | None:
        """代理失效：拉 Webshare 换一条；都试不通则清空代理走直连。"""
        with self._proxy_rotate_lock:
            bad_ep = proxy_endpoint(bad_proxy)
            if bad_ep:
                self._bad_proxy_endpoints.add(bad_ep)
            detail_err = str(exc)[:160]
            if not self._webshare.configured:
                try:
                    self.store.update(account_id, proxy="")
                    self.invalidate_snapshot(account_id)
                    self.store.add_event(
                        account_id,
                        "proxy",
                        False,
                        f"代理失效改直连（未配 Webshare）：{bad_ep or bad_proxy} · {detail_err}",
                    )
                except Exception:
                    pass
                return None
            used_urls, used_endpoints = self._used_proxies(exclude_id=account_id)
            used_endpoints |= set(self._bad_proxy_endpoints)
            if bad_proxy:
                used_urls.add(bad_proxy.strip())
            try:
                picked = self._webshare.pick(
                    used_urls=used_urls,
                    used_endpoints=used_endpoints,
                    force=True,
                    verify=True,
                )
            except WebshareError as web_exc:
                try:
                    self.store.update(account_id, proxy="")
                    self.invalidate_snapshot(account_id)
                    self.store.add_event(
                        account_id,
                        "proxy",
                        False,
                        f"代理均无效，已改直连。失效 {bad_ep or bad_proxy} · {detail_err} · {web_exc}",
                    )
                except Exception:
                    pass
                print(f"代理轮换失败 account={account_id} 改直连: {web_exc}")
                return None
            updated = self.store.update(account_id, proxy=picked.url)
            self.invalidate_snapshot(account_id)
            self.store.add_event(
                account_id,
                "proxy",
                True,
                f"代理失效已更换 {bad_ep or '?'} → {picked.endpoint} · {detail_err}",
            )
            print(f"代理已更换 account={account_id} {bad_ep} -> {picked.endpoint}")
            return updated.proxy

    def try_begin_action(self, account_id: int) -> bool:
        with self._action_guard:
            if account_id in self._busy:
                return False
            self._busy.add(account_id)
            return True

    def end_action(self, account_id: int) -> None:
        with self._action_guard:
            self._busy.discard(account_id)

    def _idle_spot(self, account: Account) -> Decimal:
        hit = self._snap_cache.get(account.id)
        if not hit:
            return Decimal("0")
        return d(hit[1].get("spot_usdt") or "0")

    def invalidate_snapshot(self, account_id: int) -> None:
        self._snap_cache.pop(account_id, None)
        self._risk_cache.pop(account_id, None)

    def _remember_risk(self, account_id: int, risk: dict) -> dict:
        stored = {
            "uni_mmr": risk.get("uni_mmr") or Decimal("0"),
            "equity": risk.get("equity") or Decimal("0"),
            "available": risk.get("available") or Decimal("0"),
        }
        self._risk_cache[account_id] = (time.monotonic(), stored)
        return stored

    def _cached_risk(self, account_id: int) -> dict | None:
        hit = self._risk_cache.get(account_id)
        if not hit:
            return None
        return hit[1]

    def _risk_for(self, account: Account, futures, *, refresh: bool) -> dict:
        empty = {"uni_mmr": Decimal("0"), "equity": Decimal("0"), "available": Decimal("0")}
        now = time.monotonic()
        hit = self._risk_cache.get(account.id)
        if not refresh:
            if hit and now - hit[0] < self._risk_ttl:
                return hit[1]
            # 缓存缺失或过期：轮询也要拉一次，否则有仓位时 uniMMR 会一直显示 —
            refresh = True
        if not getattr(futures, "unified", True):
            return self._remember_risk(account.id, empty)
        try:
            risk = futures.refresh_account_risk()
        except Exception:
            return (hit[1] if hit else empty)
        return self._remember_risk(account.id, risk)

    def _legs_payload(self, legs) -> dict:
        return {
            "long_qty": fmt_amount(legs.long_qty),
            "short_qty": fmt_amount(legs.short_qty),
            "long_pnl": fmt_amount(legs.long_pnl, 4),
            "short_pnl": fmt_amount(legs.short_pnl, 4),
            "long_entry": fmt_amount(legs.long_entry) if legs.long_entry > 0 else "",
            "short_entry": fmt_amount(legs.short_entry) if legs.short_entry > 0 else "",
            "missing": legs.missing_side,
        }

    def _other_positions(self, futures, symbol: str, legs) -> list[dict]:
        if legs.long_qty > 0 or legs.short_qty > 0:
            return []
        try:
            rows = futures.open_um_symbols()
        except Exception:
            return []
        sym = (symbol or "").upper()
        return [r for r in rows if r.get("symbol") != sym]

    def _refresh_cached_legs(self, account: Account, data: dict) -> dict:
        """资金可沿用缓存，仓位/浮盈必须现拉，避免线上一直显示无仓。"""
        try:
            pipe = pipeline_for(account, self.base, dry_run=True, ops=self)
            settings = pipe.settings
            futures = pipe.futures
            legs = futures.legs(settings.hedge_symbol)
            risk = self._risk_for(account, futures, refresh=False)
            mmr = risk.get("uni_mmr") or Decimal("0")
            try:
                stability = price_stability(futures, settings)
            except Exception:
                from .monitor import Stability

                mid = d(data.get("price") or "0")
                stability = Stability(
                    settings.hedge_symbol, mid, Decimal("1"), Decimal("1"), Decimal("1"), False, "价格未刷新"
                )
            coll = d(data.get("collateral_usdt") or data.get("bfusd") or "0")
            if coll <= 0:
                coll = risk.get("equity") or Decimal("0")
            fee = resolve_harvest_fee(futures, settings.hedge_symbol, legs, stability.mid, settings)
            last_harvest = self.store.last_ok_action_at(account.id, "harvest")
            sized = _sizing_settings(settings, legs)
            plan = position_plan(
                sized,
                stability.mid,
                coll,
                legs,
                mmr,
                lambda q: futures.round_qty(q, Decimal("0.001")),
                available=risk.get("available"),
            )
            advice = harvest_advice(
                legs,
                stability,
                sized,
                last_harvest,
                principal=coll,
                fee=fee,
                notional=_harvest_notional(
                    legs, stability.mid, plan, collateral=coll, settings=sized
                ),
            )
            has_pos = legs.long_qty > 0 or legs.short_qty > 0
            lev, lev_tgt = _display_leverage(legs, settings)
            data["symbol"] = settings.hedge_symbol
            data["settle_asset"] = settle_asset_of(settings.hedge_symbol)
            data["leverage"] = lev
            data["leverage_target"] = lev_tgt
            data["uni_mmr"] = format_uni_mmr(mmr, has_pos)
            data["price"] = stability.as_dict().get("mid") or data.get("price") or "0"
            data["legs"] = self._legs_payload(legs)
            data["other_positions"] = self._other_positions(futures, settings.hedge_symbol, legs)
            data["stability"] = stability.as_dict()
            data["enter"] = enter_advice(legs, stability, d(data.get("spot_usdt") or "0"), plan)
            data["harvest"] = advice.as_dict()
            data["account"] = account.public_dict()
            data["events"] = _events_payload(account.id, self.store.recent_events(account.id, 12))
        except Exception as exc:
            data["legs_refresh_error"] = str(exc)
        return data

    def snapshot(self, account: Account, *, force: bool = False) -> dict:
        now = time.monotonic()
        if not force:
            hit = self._snap_cache.get(account.id)
            if hit and now - hit[0] < self._snap_ttl:
                data = dict(hit[1])
                data = self._refresh_cached_legs(account, data)
                data["from_cache"] = True
                data["cache_age"] = int(now - hit[0])
                return data
        data = self._build_snapshot(account)
        out = dict(data)
        out["from_cache"] = False
        out["cache_age"] = 0
        if data.get("ok"):
            self._snap_cache[account.id] = (now, data)
        else:
            self.invalidate_snapshot(account.id)
        return out

    def monitor(self, account: Account) -> dict:
        try:
            pipe = pipeline_for(account, self.base, dry_run=True, ops=self)
        except Exception as exc:
            return {"ok": False, "error": str(exc)}
        settings = pipe.settings
        futures = pipe.futures
        try:
            legs = futures.legs(settings.hedge_symbol)
        except Exception as exc:
            return {"ok": False, "error": str(exc)}
        risk = self._risk_for(account, futures, refresh=False)
        mmr = risk.get("uni_mmr") or Decimal("0")
        try:
            stability = price_stability(futures, settings)
        except Exception as exc:
            return {"ok": False, "error": f"价格监控失败: {exc}"}
        last_harvest = self.store.last_ok_action_at(account.id, "harvest")
        coll = Decimal("0")
        hit = self._snap_cache.get(account.id)
        if hit:
            coll = d(hit[1].get("collateral_usdt") or hit[1].get("bfusd") or "0")
        if coll <= 0:
            coll = risk.get("equity") or Decimal("0")
        fee = resolve_harvest_fee(futures, settings.hedge_symbol, legs, stability.mid, settings)
        sized = _sizing_settings(settings, legs)
        plan = position_plan(
            sized,
            stability.mid,
            coll,
            legs,
            mmr,
            lambda q: futures.round_qty(q, Decimal("0.001")),
            available=risk.get("available"),
        )
        advice = harvest_advice(
            legs,
            stability,
            sized,
            last_harvest,
            principal=coll,
            fee=fee,
            notional=_harvest_notional(
                legs, stability.mid, plan, collateral=coll, settings=sized
            ),
        )
        mmr_text = format_uni_mmr(mmr, legs.long_qty > 0 or legs.short_qty > 0)
        lev, lev_tgt = _display_leverage(legs, settings)
        return {
            "ok": True,
            "account": account.public_dict(),
            "symbol": settings.hedge_symbol,
            "leverage": lev,
            "leverage_target": lev_tgt,
            "uni_mmr": mmr_text,
            "live_poll_seconds": int(settings.live_poll_seconds),
            "auto_harvest_seconds": int(getattr(settings, "auto_harvest_seconds", 45) or 45),
            "price": stability.as_dict().get("mid") or "0",
            "legs": self._legs_payload(legs),
            "other_positions": self._other_positions(futures, settings.hedge_symbol, legs),
            "stability": stability.as_dict(),
            "enter": enter_advice(legs, stability, self._idle_spot(account), plan),
            "harvest": advice.as_dict(),
            "events": _events_payload(account.id, self.store.recent_events(account.id, 12)),
        }

    def _build_snapshot(self, account: Account) -> dict:
        base = {
            "account": account.public_dict(),
            "proxy_mode": "账号代理" if account.proxy else "默认出口 IP",
            "schedule": schedule_hint(self.base),
            "exit_ip": "未知",
            "hedge_symbols": list(self.base.hedge_symbols),
        }
        try:
            pipe = pipeline_for(account, self.base, dry_run=True, ops=self)
        except Exception as exc:
            return {**base, "ok": False, "error": str(exc)}
        settings = pipe.settings
        futures = pipe.futures
        fetched, errs = _gather(
            {
                "wallet": pipe.wallet_view,
                "legs": lambda: futures.legs(settings.hedge_symbol),
                "risk": lambda: self._risk_for(account, futures, refresh=True),
                "stability": lambda: price_stability(futures, settings),
                "filters": lambda: futures.filters(settings.hedge_symbol),
            }
        )
        base["exit_ip"] = exit_ip(pipe.client, force=False)
        base["schedule"] = schedule_hint(settings)
        earn_error = str(errs["wallet"]) if "wallet" in errs else ""
        wallet = fetched.get("wallet")
        if not isinstance(wallet, dict):
            wallet = {
                "spot_usdt": "0",
                "usdt_flexible": "0",
                "bfusd": "0",
                "earn_total": "0",
                "holdings": [],
                "next_buy": None,
                "status": earn_error,
            }
        if "legs" in errs:
            return {**base, "ok": False, "error": str(errs["legs"])}
        if "stability" in errs:
            return {**base, "ok": False, "error": f"价格监控失败: {errs['stability']}"}
        legs = fetched["legs"]
        risk = fetched.get("risk") or {"uni_mmr": Decimal("0"), "equity": Decimal("0"), "available": Decimal("0")}
        stability = fetched["stability"]
        spot_usdt = d(wallet.get("spot_usdt") or "0")
        collateral = d(wallet.get("earn_total") or "0") + spot_usdt
        mmr = risk.get("uni_mmr") or Decimal("0")
        equity = risk.get("equity") or Decimal("0")
        last_harvest = self.store.last_ok_action_at(account.id, "harvest")
        fee = resolve_harvest_fee(futures, settings.hedge_symbol, legs, stability.mid, settings)
        filters = fetched.get("filters")
        step = filters[1] if isinstance(filters, tuple) and len(filters) > 1 else Decimal("0.001")
        sized = _sizing_settings(settings, legs)
        plan = position_plan(
            sized,
            stability.mid,
            collateral,
            legs,
            mmr,
            lambda q: futures.round_qty(q, step),
            available=risk.get("available"),
        )
        advice = harvest_advice(
            legs,
            stability,
            sized,
            last_harvest,
            principal=collateral,
            fee=fee,
            notional=_harvest_notional(
                legs, stability.mid, plan, collateral=collateral, settings=sized
            ),
        )
        enter = enter_advice(legs, stability, spot_usdt, plan)
        try:
            switch = pipe.switch_advice(mmr=mmr, equity=equity)
        except Exception as exc:
            switch = {"needed": False, "can_click": False, "reason": str(exc)}
        try:
            margin = pipe.margin_status(equity=equity)
        except Exception as exc:
            margin = {"equity": "0", "need_move": False, "can_click": False, "summary": str(exc), "rows": []}
        has_pos = legs.long_qty > 0 or legs.short_qty > 0
        mmr_text = format_uni_mmr(mmr, has_pos)
        lev, lev_tgt = _display_leverage(legs, settings)
        return {
            **base,
            "ok": True,
            "error": earn_error,
            "symbol": settings.hedge_symbol,
            "settle_asset": settle_asset_of(settings.hedge_symbol),
            "leverage": lev,
            "leverage_target": lev_tgt,
            "live_poll_seconds": int(settings.live_poll_seconds),
            "collateral_usdt": fmt_amount(collateral, 4),
            "spot_usdt": wallet.get("spot_usdt") or fmt_amount(spot_usdt, 4),
            "earn_total": wallet.get("earn_total") or "0",
            "usdt_flexible": wallet.get("usdt_flexible") or "0",
            "bfusd": wallet.get("bfusd") or "0",
            "earn_yesterday": wallet.get("earn_yesterday") or "0",
            "earn_yesterday_source": wallet.get("earn_yesterday_source") or "none",
            "earn_yesterday_error": wallet.get("earn_yesterday_error") or "",
            "price": stability.as_dict().get("mid") or "0",
            "wallet_status": wallet.get("status") or "",
            "next_buy": wallet.get("next_buy"),
            "uni_mmr": mmr_text,
            "pm_equity": margin.get("equity") or "0",
            "account_total": margin.get("equity") or fmt_amount(collateral, 4),
            "margin": margin,
            "legs": self._legs_payload(legs),
            "other_positions": self._other_positions(futures, settings.hedge_symbol, legs),
            "stability": stability.as_dict(),
            "enter": enter,
            "harvest": advice.as_dict(),
            "switch": switch,
            "holdings": wallet.get("holdings") or [],
            "events": _events_payload(account.id, self.store.recent_events(account.id)),
        }

    def run_action(
        self,
        account: Account,
        action: str,
        force: bool = False,
        mode: str | None = None,
        *,
        _locked: bool = False,
        auto: bool = False,
    ) -> dict:
        own_lock = False
        if not _locked:
            if not self.try_begin_action(account.id):
                return {
                    "ok": False,
                    "blocked": True,
                    "live": True,
                    "reason": "该账号正在执行其他操作，请稍后再试",
                    "steps": [],
                }
            own_lock = True
        try:
            return self._run_action_body(account, action, force=force, mode=mode, auto=auto)
        finally:
            if own_lock:
                self.end_action(account.id)

    def _run_action_body(
        self,
        account: Account,
        action: str,
        force: bool = False,
        mode: str | None = None,
        *,
        auto: bool = False,
    ) -> dict:
        if action not in {"enter", "harvest", "switch", "sweep", "hedge", "recycle", "fund", "leverage", "scale"}:
            raise ValueError(f"未知动作: {action}")
        try:
            pipe = pipeline_for(account, self.base, dry_run=False, ops=self)
        except Exception as exc:
            return {
                "ok": False,
                "blocked": False,
                "live": True,
                "steps": [],
                "error": str(exc),
                "snapshot": self.snapshot(account, force=True),
            }
        cycle = HedgeCycle(pipe)
        if action == "harvest":
            live = self.monitor(account)
            harvest = (live or {}).get("harvest") or {}
            # 自动收利只走完整条件（含价格平稳）；手动仍可强行
            if auto:
                allowed = bool(harvest.get("can_click"))
            else:
                allowed = harvest.get("can_click") or (force and harvest.get("force_ok"))
            if not allowed:
                reason = harvest.get("reason") or live.get("error") or "现在不适合平仓"
                self.store.add_event(account.id, action, False, ("自动收利已拦截：" if auto else "已拦截：") + reason[:500])
                return {
                    "ok": False,
                    "blocked": True,
                    "live": True,
                    "reason": reason,
                    "steps": [],
                }
        if action == "scale":
            live = self.monitor(account)
            enter = (live or {}).get("enter") or {}
            allowed = enter.get("scale_ok") or force
            if not allowed:
                reason = enter.get("reason") or "当前不能加仓（uniMMR 不够或已开满）"
                self.store.add_event(account.id, action, False, "已拦截：" + reason[:500])
                return {
                    "ok": False,
                    "blocked": True,
                    "live": True,
                    "reason": reason,
                    "steps": [],
                }
        if action == "switch":
            snap = self.snapshot(account)
            switch = snap.get("switch") or {}
            if not switch.get("can_click"):
                reason = switch.get("reason") or "现在不适合换产品"
                self.store.add_event(account.id, action, False, "已拦截：" + reason[:500])
                return {
                    "ok": False,
                    "blocked": True,
                    "reason": reason,
                    "steps": [],
                }
        if action == "harvest":
            steps = cycle.harvest_once(wait_stable=not force)
        elif action == "scale":
            steps = cycle.scale_once(force=force or mode == "force")
        elif action == "switch":
            steps = pipe.switch_to_best()
        elif action == "leverage":
            steps = cycle.apply_leverage()
        else:
            steps = cycle.setup_once(
                force_hedge=force or mode == "force",
                skip_hedge=mode == "earn_only",
            )
        if getattr(cycle, "leverage_cap_changed", False):
            account = self.store.set_leverage_cap(
                account.id,
                getattr(cycle, "leverage_cap", None),
                getattr(cycle, "leverage_unlock_at", None),
            )
        ok = all(step.ok for step in steps) if steps else True
        summary = _event_summary(steps, limit=1900)
        if auto:
            summary = "自动收利：" + summary
        self.store.add_event(account.id, action, ok, summary[:2000])
        if not ok:
            fails = [f"{s.name}: {_detail(s.detail)}" for s in steps if not s.ok]
            if fails:
                print(f"动作失败 account={account.id} action={action} " + " | ".join(fails)[:800])
        self.invalidate_snapshot(account.id)
        return {
            "ok": ok,
            "blocked": False,
            "live": True,
            "steps": steps_payload(steps),
            "snapshot": self.snapshot(account, force=True),
        }
