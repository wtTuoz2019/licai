from __future__ import annotations

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from decimal import Decimal

from .config import Settings, d, fmt_amount, is_mmr_sentinel, load_settings, settings_for_account, settle_asset_of
from .cycle import HedgeCycle, position_plan
from .monitor import enter_advice, exit_ip, harvest_advice, price_stability, resolve_harvest_fee, schedule_hint
from .pipeline import Pipeline, StepResult
from .store import Account, AccountStore


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


def pipeline_for(account: Account, base: Settings | None = None, dry_run: bool = True) -> Pipeline:
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
    return Pipeline(settings)


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
            pipe = pipeline_for(account, self.base, dry_run=True)
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
            advice = harvest_advice(legs, stability, settings, last_harvest, principal=coll, fee=fee)
            plan = position_plan(
                _sizing_settings(settings, legs),
                stability.mid,
                coll,
                legs,
                mmr,
                lambda q: futures.round_qty(q, Decimal("0.001")),
                available=risk.get("available"),
            )
            has_pos = legs.long_qty > 0 or legs.short_qty > 0
            data["symbol"] = settings.hedge_symbol
            data["settle_asset"] = settle_asset_of(settings.hedge_symbol)
            data["leverage"] = int(legs.leverage or settings.hedge_leverage or 0) or int(settings.hedge_leverage)
            data["uni_mmr"] = format_uni_mmr(mmr, has_pos)
            data["price"] = stability.as_dict().get("mid") or data.get("price") or "0"
            data["legs"] = self._legs_payload(legs)
            data["other_positions"] = self._other_positions(futures, settings.hedge_symbol, legs)
            data["stability"] = stability.as_dict()
            data["enter"] = enter_advice(legs, stability, d(data.get("spot_usdt") or "0"), plan)
            data["harvest"] = advice.as_dict()
            data["account"] = account.public_dict()
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
            pipe = pipeline_for(account, self.base, dry_run=True)
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
        advice = harvest_advice(legs, stability, settings, last_harvest, principal=coll, fee=fee)
        mmr_text = format_uni_mmr(mmr, legs.long_qty > 0 or legs.short_qty > 0)
        lev = int(legs.leverage or settings.hedge_leverage)
        plan = position_plan(
            _sizing_settings(settings, legs),
            stability.mid,
            coll,
            legs,
            mmr,
            lambda q: futures.round_qty(q, Decimal("0.001")),
            available=risk.get("available"),
        )
        return {
            "ok": True,
            "account": account.public_dict(),
            "symbol": settings.hedge_symbol,
            "leverage": lev,
            "leverage_target": int(settings.hedge_leverage_target or settings.hedge_leverage),
            "uni_mmr": mmr_text,
            "live_poll_seconds": int(settings.live_poll_seconds),
            "auto_harvest_seconds": int(getattr(settings, "auto_harvest_seconds", 45) or 45),
            "price": stability.as_dict().get("mid") or "0",
            "legs": self._legs_payload(legs),
            "other_positions": self._other_positions(futures, settings.hedge_symbol, legs),
            "stability": stability.as_dict(),
            "enter": enter_advice(legs, stability, self._idle_spot(account), plan),
            "harvest": advice.as_dict(),
            "events": [
                {
                    "id": event.id,
                    "action": event.action,
                    "ok": event.ok,
                    "detail": event.detail,
                    "created_at": event.created_at,
                }
                for event in self.store.recent_events(account.id, 12)
            ],
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
            pipe = pipeline_for(account, self.base, dry_run=True)
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
        advice = harvest_advice(legs, stability, settings, last_harvest, principal=collateral, fee=fee)
        filters = fetched.get("filters")
        step = filters[1] if isinstance(filters, tuple) and len(filters) > 1 else Decimal("0.001")
        plan = position_plan(
            _sizing_settings(settings, legs),
            stability.mid,
            collateral,
            legs,
            mmr,
            lambda q: futures.round_qty(q, step),
            available=risk.get("available"),
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
        lev = int(legs.leverage or settings.hedge_leverage or 0) or int(settings.hedge_leverage)
        return {
            **base,
            "ok": True,
            "error": earn_error,
            "symbol": settings.hedge_symbol,
            "settle_asset": settle_asset_of(settings.hedge_symbol),
            "leverage": lev,
            "leverage_target": int(settings.hedge_leverage_target or settings.hedge_leverage),
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
            "events": [
                {
                    "id": event.id,
                    "action": event.action,
                    "ok": event.ok,
                    "detail": event.detail,
                    "created_at": event.created_at,
                }
                for event in self.store.recent_events(account.id)
            ],
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
            pipe = pipeline_for(account, self.base, dry_run=False)
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
            steps = cycle.harvest_once()
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
