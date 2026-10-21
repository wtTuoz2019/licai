from __future__ import annotations

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from decimal import Decimal

from .config import (
    ROOT,
    Settings,
    d,
    fmt_amount,
    is_mmr_sentinel,
    load_settings,
    settings_for_account,
    settle_asset_of,
)
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


def _post_only_fail(step: StepResult) -> bool:
    if step.ok:
        return False
    text = str(step.detail or "").lower()
    return "-5022" in text or "post only" in text or "could not be executed as maker" in text


def _action_ok(steps: list[StepResult], *, error: Exception | None = None) -> bool:
    """最终对冲/收利成功则算成功；中间可恢复的 -5022 不把整单打成失败。"""
    if error is not None:
        return False
    if not steps:
        return True
    success_names = ("对冲平衡", "对冲已齐", "收利完成", "单边已平")
    if any(s.ok and s.name in success_names for s in steps):
        for s in steps:
            if s.ok:
                continue
            if _post_only_fail(s):
                continue
            if s.name in {"PostOnly 改价", "第二腿追价", "单边兜底", "挂单未齐", "本轮平仓挂单超时"}:
                continue
            text = str(s.detail or "").lower()
            name = str(s.name or "")
            # ReduceOnly 拒单但仓位已平，或理财代币划转失败但仓位已齐，不盖掉收利成功
            if "-2022" in text or "reduceonly" in text:
                continue
            if "-3027" in text or "not a valid margin asset" in text:
                continue
            # BFUSD 等资产 earn-asset-transfer 常直接 -1100；钱已在理财计息，对冲已齐不算失败
            if "-1100" in text or "illegal characters" in text:
                continue
            if "从理财转入统一账户" in name or name.endswith("暂留"):
                continue
            # 其它失败（保证金不够、异常等）仍算失败
            return False
        return True
    return all(s.ok for s in steps)


# 收利/补仓/开仓诊断必须留住的步骤名关键词
_KEEP_STEP_KEYS = (
    "触发止盈",
    "平仓",
    "补仓",
    "开仓",
    "挂单",
    "对冲",
    "入场顺序",
    "PostOnly",
    "第二腿",
    "单边兜底",
    "市价",
    "杠杆",
    "保证金",
    "预选理财",
    "归集理财",
    "申购",
    "填补负余额",
    "负余额",
    "转出",
    "可转",
    "归集统一",
    "换产品",
    "赎回",
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
    "对冲平衡",
    "收利未齐",
    "等待平稳",
    "价格已平稳",
    "异常中断",
)


def _event_summary(steps: list[StepResult], *, limit: int = 12000) -> str:
    """失败 + 关键节点优先；剩余额度从后往前补，避免开仓挂单被前面的赎回 JSON 挤掉。"""
    if not steps:
        return "无步骤"

    def line(step: StepResult) -> str:
        flag = "ok" if step.ok else "FAIL"
        stamp = f"{step.at} " if getattr(step, "at", "") else ""
        return f"{stamp}{step.name}[{flag}]: {_detail(step.detail)}"

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
    # 从后往前补全（开仓/挂单通常在末尾）
    for i in range(len(steps) - 1, -1, -1):
        add(i)
        draft = "；".join(line(steps[j]) for j in sorted(seen))
        if len(draft) >= limit:
            break

    ordered = sorted(seen)
    text = "；".join(line(steps[i]) for i in ordered)
    if len(text) <= limit:
        return text
    failed = [i for i in ordered if not steps[i].ok]
    key = [i for i in ordered if i not in failed and any(k in steps[i].name for k in _KEEP_STEP_KEYS)]
    rest = [i for i in ordered if i not in failed and i not in key]
    # 剩余也优先靠后的步骤
    rest = sorted(rest, reverse=True)
    picked: list[int] = []
    for group in (failed, key, rest):
        for i in group:
            trial = sorted(picked + [i])
            draft = "；".join(line(steps[j]) for j in trial)
            if len(draft) > limit and picked:
                continue
            picked.append(i)
            if len(draft) >= limit:
                break
    return "；".join(line(steps[i]) for i in sorted(picked))[:limit]


def _sizing_settings(settings: Settings, legs) -> Settings:
    """加仓/入场预览按仓位实际杠杆算，不按目标倍数虚高。"""
    actual = int(getattr(legs, "leverage", 0) or 0)
    if actual > 0 and actual != int(settings.hedge_leverage or 0):
        return replace(settings, hedge_leverage=actual)
    return settings


def steps_payload(steps: list[StepResult]) -> list[dict]:
    return [
        {
            "name": step.name,
            "ok": step.ok,
            "detail": _detail(step.detail),
            "dry_run": step.dry_run,
            "at": getattr(step, "at", "") or "",
        }
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


def relative_earn_apy(income: Decimal, principal: Decimal) -> str:
    """相对年化：实际理财收入（日）× 365 / 账户权益。

    理财当保证金、用合约放大后，分母是权益本金而不是理财面额，所以会高于产品挂牌利率。
    """
    if income <= 0 or principal <= 0:
        return ""
    pct = income * Decimal(365) / principal * Decimal(100)
    return f"{pct:.2f}%"


def _sizing_collateral(risk: dict, fallback: Decimal = Decimal("0")) -> Decimal:
    """开仓/加仓预估本金：与 cycle._scale_to_collateral 一致，优先 PM equity。

    理财总额常大于统一账户权益（钱在赚币里），用 earn 会把「可加仓」算得过大，
    确认框会显示还能加、点下去却已按安全上限开满。
    """
    equity = d(risk.get("equity") or 0)
    if equity > 0:
        return equity
    return d(fallback or 0)


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
    """返回 (展示用实际杠杆, 目标杠杆)。

    有仓用交易所仓位杠杆。无仓不用目标冒充实际：优先有效倍数（已含子账户上限），
    再退到 leverage_cap；都没有才显示目标。
    """
    target = int(settings.hedge_leverage_target or settings.hedge_leverage or 0)
    actual = int(getattr(legs, "leverage", 0) or 0)
    if actual <= 0:
        effective = int(settings.hedge_leverage or 0)
        cap = int(settings.leverage_cap or 0)
        if effective > 0:
            actual = effective
        elif cap > 0:
            actual = min(target, cap) if target > 0 else cap
        else:
            actual = target
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
        pending_scale_qty=account.scale_add_qty,
    )
    pipe = Pipeline(settings)
    if ops is not None:
        blocked = ops._earn_region_block.setdefault(account.id, set())
        pipe._runtime_denylist = blocked
        pipe._on_denylist_change = ops.save_earn_region_block
        if account.proxy:
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
        # 换币迁仓挂起：点「一键收利」后后台等到交叉+平稳再执行，不堵 HTTP
        # account_id -> {"force": bool, "armed_at": float, "last_event_at": float}
        self._migrate_armed: dict[int, dict] = {}
        self._webshare = WebshareClient(
            self.base.webshare_api_token,
            mode=self.base.webshare_mode,
            country=self.base.webshare_country or None,
        )
        self._bad_proxy_endpoints: set[str] = set()
        self._proxy_rotate_lock = threading.Lock()
        # account_id -> 地区/合规不可用资产（落盘，避免重启后又申购 RWUSD 空转）
        self._earn_region_block: dict[int, set[str]] = {}
        self._region_block_path = ROOT / "data" / "earn_region_block.json"
        self._load_earn_region_block()

    def _load_earn_region_block(self) -> None:
        path = getattr(self, "_region_block_path", ROOT / "data" / "earn_region_block.json")
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            raw = {}
        out: dict[int, set[str]] = {}
        if isinstance(raw, dict):
            for key, assets in raw.items():
                try:
                    aid = int(key)
                except (TypeError, ValueError):
                    continue
                if isinstance(assets, list):
                    out[aid] = {str(a).upper() for a in assets if a}
                elif isinstance(assets, str) and assets:
                    out[aid] = {assets.upper()}
        self._earn_region_block = out

    def save_earn_region_block(self) -> None:
        path = getattr(self, "_region_block_path", ROOT / "data" / "earn_region_block.json")
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                str(aid): sorted(assets)
                for aid, assets in sorted(self._earn_region_block.items())
                if assets
            }
            path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        except Exception:
            pass

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

    def arm_migrate(self, account_id: int, *, force: bool = False) -> dict:
        """挂起换币迁仓；已挂起则更新 force。"""
        now = time.monotonic()
        with self._action_guard:
            prev = self._migrate_armed.get(account_id)
            if prev:
                prev["force"] = bool(force) or bool(prev.get("force"))
                return dict(prev)
            row = {"force": bool(force), "armed_at": now, "last_event_at": 0.0}
            self._migrate_armed[account_id] = row
            return dict(row)

    def clear_migrate_arm(self, account_id: int) -> None:
        with self._action_guard:
            self._migrate_armed.pop(account_id, None)

    def migrate_arm(self, account_id: int) -> dict | None:
        with self._action_guard:
            hit = self._migrate_armed.get(account_id)
            return dict(hit) if hit else None

    def touch_migrate_event(self, account_id: int) -> None:
        with self._action_guard:
            hit = self._migrate_armed.get(account_id)
            if hit:
                hit["last_event_at"] = time.monotonic()

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

    def _other_positions(self, futures, symbol: str, legs=None) -> tuple[list[dict], str | None]:
        """当前套保以外的仓。返回 (列表, 错误)。读失败时错误非空，列表不可当作「已空」。"""
        try:
            rows = futures.open_um_symbols()
        except Exception as exc:
            return [], str(exc)
        cur = (symbol or "").upper()
        out: list[dict] = []
        seen: set[str] = set()
        errors: list[str] = []
        for row in rows or []:
            sym = str(row.get("symbol") or "").upper()
            if not sym or sym == cur or sym in seen:
                continue
            seen.add(sym)
            try:
                ol = futures.legs(sym)
            except Exception as exc:
                errors.append(f"{sym} 读仓失败：{exc}")
                continue
            if ol.long_qty <= 0 and ol.short_qty <= 0:
                continue
            if ol.long_qty > 0 and ol.short_qty > 0:
                side = "BOTH" if ol.long_qty == ol.short_qty else "IMBALANCE"
            elif ol.long_qty > 0:
                side = "LONG"
            else:
                side = "SHORT"
            out.append(
                {
                    "symbol": sym,
                    "side": side,
                    "qty": fmt_amount(max(ol.long_qty, ol.short_qty)),
                    "long_qty": fmt_amount(ol.long_qty),
                    "short_qty": fmt_amount(ol.short_qty),
                    "long_pnl": fmt_amount(ol.long_pnl, 4),
                    "short_pnl": fmt_amount(ol.short_pnl, 4),
                    "long_entry": fmt_amount(ol.long_entry) if ol.long_entry > 0 else "",
                    "short_entry": fmt_amount(ol.short_entry) if ol.short_entry > 0 else "",
                }
            )
        return out, ("；".join(errors) if errors else None)

    @staticmethod
    def _apply_migrate_harvest_advice(
        advice: dict,
        others: list[dict],
        current_symbol: str,
        *,
        armed: bool = False,
        auto_harvest: bool = False,
    ) -> dict:
        """当前套保无仓但旧币种有仓：一键收利改为「挂单全平旧仓」；开仓留给下次交叉。"""
        if not others:
            return advice
        tip = "、".join(
            f"{r.get('symbol')} 多{r.get('long_qty') or 0}/空{r.get('short_qty') or 0}"
            for r in others[:6]
        )
        out = dict(advice)
        # 迁仓挂起后由后台等时机；点按钮不要求当前已平稳
        out["migrate"] = True
        out["migrate_armed"] = bool(armed)
        out["force_ok"] = not armed
        # 未挂起时可点：自动收利开着时按钮用于强行；未开自动时用于挂起
        out["can_click"] = not armed
        out["stable_ok"] = bool(advice.get("stable_ok"))
        if armed:
            out["reason"] = (
                f"迁仓已挂起：旧仓 {tip}。后台等到金叉/死叉与收利级平稳后会挂单全平；"
                f"清完后等下次交叉再开 {current_symbol}。可在事件里看进度。"
            )
        elif auto_harvest:
            out["reason"] = (
                f"已换套保到 {current_symbol}，旧仓仍在：{tip}。"
                "自动收利已开，后台会自动等交叉与平稳后挂单全平旧仓，"
                "再等下次交叉开新仓；不稳时可点「一键收利」强行尽快平。"
            )
        else:
            out["reason"] = (
                f"已换套保到 {current_symbol}，旧仓仍在：{tip}。"
                "点「迁仓开新」会挂起任务（不堵页面），后台等到交叉与平稳后挂单全平；"
                "清完后等下次交叉再开。不稳时可强行挂起尽快平仓。"
            )
        return out

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
            fallback = d(data.get("collateral_usdt") or data.get("bfusd") or "0")
            coll = _sizing_collateral(risk, fallback)
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
            try:
                harvest_stab = price_stability(futures, settings, purpose="harvest")
            except Exception:
                harvest_stab = stability
            advice = harvest_advice(
                legs,
                harvest_stab,
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
            others, others_err = self._other_positions(futures, settings.hedge_symbol, legs)
            data["other_positions"] = others
            data["other_positions_error"] = others_err or ""
            data["stability"] = stability.as_dict()
            data["enter"] = enter_advice(legs, stability, d(data.get("spot_usdt") or "0"), plan)
            data["harvest"] = self._apply_migrate_harvest_advice(
                advice.as_dict(),
                others,
                settings.hedge_symbol,
                armed=bool(self.migrate_arm(account.id)),
                auto_harvest=bool(account.auto_harvest),
            )
            data["account"] = account.public_dict()
            data["events"] = _events_payload(account.id, self.store.recent_events(account.id, 30))
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
        fallback = Decimal("0")
        hit = self._snap_cache.get(account.id)
        if hit:
            fallback = d(hit[1].get("collateral_usdt") or hit[1].get("bfusd") or "0")
        coll = _sizing_collateral(risk, fallback)
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
        try:
            harvest_stab = price_stability(futures, settings, purpose="harvest")
        except Exception:
            harvest_stab = stability
        advice = harvest_advice(
            legs,
            harvest_stab,
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
        # 自动归集现货读的是 monitor.spot_usdt；不能只靠快照缓存（常为空）
        spot_usdt = self._idle_spot(account)
        try:
            spot_usdt = pipe.spot_cash("USDT")
        except Exception:
            pass
        others, others_err = self._other_positions(futures, settings.hedge_symbol, legs)
        armed = self.migrate_arm(account.id)
        return {
            "ok": True,
            "account": account.public_dict(),
            "symbol": settings.hedge_symbol,
            "leverage": lev,
            "leverage_target": lev_tgt,
            "uni_mmr": mmr_text,
            "live_poll_seconds": int(settings.live_poll_seconds),
            "auto_harvest_seconds": int(getattr(settings, "auto_harvest_seconds", 60) or 60),
            "price": stability.as_dict().get("mid") or "0",
            "spot_usdt": fmt_amount(spot_usdt, 4),
            "action_busy": account.id in self._busy,
            "migrate_armed": bool(armed),
            "legs": self._legs_payload(legs),
            "other_positions": others,
            "other_positions_error": others_err or "",
            "stability": stability.as_dict(),
            "enter": enter_advice(legs, stability, spot_usdt, plan),
            "harvest": self._apply_migrate_harvest_advice(
                advice.as_dict(),
                others,
                settings.hedge_symbol,
                armed=bool(armed),
                auto_harvest=bool(account.auto_harvest),
            ),
            "events": _events_payload(account.id, self.store.recent_events(account.id, 30)),
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
        # 页面「理财总额」仍展示 earn；算仓/加仓预估改用 PM equity，与真实加仓一致
        earn_collateral = d(wallet.get("earn_total") or "0") + spot_usdt
        mmr = risk.get("uni_mmr") or Decimal("0")
        equity = risk.get("equity") or Decimal("0")
        sizing_coll = _sizing_collateral(risk, earn_collateral)
        last_harvest = self.store.last_ok_action_at(account.id, "harvest")
        fee = resolve_harvest_fee(futures, settings.hedge_symbol, legs, stability.mid, settings)
        filters = fetched.get("filters")
        step = filters[1] if isinstance(filters, tuple) and len(filters) > 1 else Decimal("0.001")
        sized = _sizing_settings(settings, legs)
        plan = position_plan(
            sized,
            stability.mid,
            sizing_coll,
            legs,
            mmr,
            lambda q: futures.round_qty(q, step),
            available=risk.get("available"),
        )
        advice = harvest_advice(
            legs,
            price_stability(futures, settings, purpose="harvest"),
            sized,
            last_harvest,
            principal=sizing_coll,
            fee=fee,
            notional=_harvest_notional(
                legs, stability.mid, plan, collateral=sizing_coll, settings=sized
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
        others, others_err = self._other_positions(futures, settings.hedge_symbol, legs)
        armed = self.migrate_arm(account.id)
        return {
            **base,
            "ok": True,
            "error": earn_error,
            "symbol": settings.hedge_symbol,
            "settle_asset": settle_asset_of(settings.hedge_symbol),
            "leverage": lev,
            "leverage_target": lev_tgt,
            "live_poll_seconds": int(settings.live_poll_seconds),
            "collateral_usdt": fmt_amount(earn_collateral, 4),
            "spot_usdt": wallet.get("spot_usdt") or fmt_amount(spot_usdt, 4),
            "earn_total": wallet.get("earn_total") or "0",
            "usdt_flexible": wallet.get("usdt_flexible") or "0",
            "usdt_flexible_label": wallet.get("usdt_flexible_label") or "USDT 活期",
            "bfusd": wallet.get("bfusd") or "0",
            "earn_apy": relative_earn_apy(d(wallet.get("earn_yesterday") or 0), equity),
            "earn_yesterday": wallet.get("earn_yesterday") or "0",
            "earn_yesterday_source": wallet.get("earn_yesterday_source") or "none",
            "earn_yesterday_day": wallet.get("earn_yesterday_day") or "",
            "earn_yesterday_error": wallet.get("earn_yesterday_error") or "",
            "price": stability.as_dict().get("mid") or "0",
            "wallet_status": wallet.get("status") or "",
            "next_buy": wallet.get("next_buy"),
            "uni_mmr": mmr_text,
            "pm_equity": margin.get("equity") or "0",
            "account_total": margin.get("equity") or fmt_amount(earn_collateral, 4),
            "margin": margin,
            "legs": self._legs_payload(legs),
            "other_positions": others,
            "other_positions_error": others_err or "",
            "migrate_armed": bool(armed),
            "stability": stability.as_dict(),
            "enter": enter,
            "harvest": self._apply_migrate_harvest_advice(
                advice.as_dict(),
                others,
                settings.hedge_symbol,
                armed=bool(armed),
                auto_harvest=bool(account.auto_harvest),
            ),
            "switch": switch,
            "holdings": wallet.get("holdings") or [],
            "events": _events_payload(account.id, self.store.recent_events(account.id, 30)),
        }

    def list_events(self, account: Account, *, limit: int = 100, before_id: int | None = None) -> dict:
        limit = max(1, min(int(limit or 100), 500))
        rows = self.store.list_events(account.id, limit=limit, before_id=before_id)
        from .store import ACTIONS_JSONL, DB_PATH

        return {
            "ok": True,
            "account_id": account.id,
            "events": _events_payload(account.id, rows),
            "has_more": len(rows) >= limit,
            "db_path": str(self.store.path),
            "actions_log": str(ACTIONS_JSONL),
            "default_db": str(DB_PATH),
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
        harvest_side: str | None = None,
        with_snapshot: bool = True,
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
            return self._run_action_body(
                account,
                action,
                force=force,
                mode=mode,
                auto=auto,
                harvest_side=harvest_side,
                with_snapshot=with_snapshot,
            )
        finally:
            if own_lock:
                self.end_action(account.id)

    def refresh_account_snapshot(self, account: Account) -> dict:
        """一整串操作都结束后再等落账并拉快照。"""
        self.invalidate_snapshot(account.id)
        settle = max(int(getattr(self.base, "settle_seconds", 0) or 0), 2)
        if settle > 0 and not bool(getattr(self.base, "dry_run", False)):
            time.sleep(settle)
        return self.snapshot(account, force=True)

    @staticmethod
    def _auto_side_harvest_ok(live: dict, harvest: dict, side: str) -> bool:
        """自动收利指定腿：收利平稳与交叉同时满足 + 冷却 + 该腿浮盈 + 对冲齐全。"""
        if not harvest.get("stable_ok"):
            return False
        if not harvest.get("cooldown_ok"):
            return False
        legs = (live or {}).get("legs") or {}
        if legs.get("missing"):
            return False
        min_profit = d(harvest.get("min_profit") or "0")
        key = "long_pnl" if side == "LONG" else "short_pnl"
        pnl = d(legs.get(key) or "0")
        return pnl > 0 and pnl >= min_profit

    def _run_action_body(
        self,
        account: Account,
        action: str,
        force: bool = False,
        mode: str | None = None,
        *,
        auto: bool = False,
        harvest_side: str | None = None,
        with_snapshot: bool = True,
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
            side = (harvest_side or "").upper() or None
            if side not in {"LONG", "SHORT"}:
                side = None
            # 自动收利：可按 MACD 指定腿；手动仍可强行。
            # 换币迁仓：自动收利开着时后台自动挂起等时机；手动点可挂起/强行。
            if auto and harvest.get("migrate"):
                others = (live or {}).get("other_positions") or []
                tip = "、".join(
                    f"{r.get('symbol')} 多{r.get('long_qty') or 0}/空{r.get('short_qty') or 0}"
                    for r in others[:6]
                ) or "旧仓"
                try:
                    sym = pipe.settings.hedge_symbol
                except Exception:
                    sym = (live or {}).get("symbol") or ""
                prev = self.migrate_arm(account.id)
                self.arm_migrate(account.id, force=False)
                detail = (
                    f"自动挂起迁仓（等金叉/死叉与平稳）：{tip}，先挂单全平；"
                    f"清完后等下次交叉再开 {sym or '新套保'}。"
                    if not prev
                    else f"迁仓已在待命：{tip} → 全平后等下次开 {sym or '新套保'}"
                )
                if not prev:
                    self.store.add_event(account.id, "harvest", True, detail[:500])
                    self.touch_migrate_event(account.id)
                    self.invalidate_snapshot(account.id)
                return {
                    "ok": True,
                    "blocked": False,
                    "queued": True,
                    "migrate_armed": True,
                    "live": True,
                    "reason": detail,
                    "steps": [{"name": "迁仓挂起", "ok": True, "detail": detail}],
                }
            if auto:
                if side:
                    allowed = self._auto_side_harvest_ok(live, harvest, side)
                else:
                    allowed = bool(harvest.get("can_click"))
            else:
                allowed = harvest.get("can_click") or (force and harvest.get("force_ok"))
            if not allowed:
                reason = harvest.get("reason") or live.get("error") or "现在不适合平仓"
                if auto and side:
                    reason = f"自动收利({side})未满足：{reason}"
                # 自动拦截是常态轮询，不写事件；手动点了被拦才记
                if not auto:
                    self.store.add_event(account.id, action, False, "已拦截：" + reason[:500])
                return {
                    "ok": False,
                    "blocked": True,
                    "live": True,
                    "reason": reason,
                    "steps": [],
                }
            # 手动迁仓：挂起给后台等时机，立刻返回（不堵 HTTP）
            if (not auto) and harvest.get("migrate"):
                others = (live or {}).get("other_positions") or []
                tip = "、".join(
                    f"{r.get('symbol')} 多{r.get('long_qty') or 0}/空{r.get('short_qty') or 0}"
                    for r in others[:6]
                ) or "旧仓"
                try:
                    sym = pipe.settings.hedge_symbol
                except Exception:
                    sym = (live or {}).get("symbol") or ""
                self.arm_migrate(account.id, force=force)
                detail = (
                    f"已挂起迁仓（{'强行尽快平' if force else '等金叉/死叉与平稳同时满足'}）："
                    f"{tip}，先挂单全平；清完后等下次交叉再开 {sym or '新套保'}。"
                    "后台执行，可看事件进度。"
                )
                self.store.add_event(account.id, "harvest", True, detail[:500])
                self.touch_migrate_event(account.id)
                snap = self.snapshot(account, force=True) if with_snapshot else None
                return {
                    "ok": True,
                    "blocked": False,
                    "queued": True,
                    "migrate_armed": True,
                    "live": True,
                    "reason": detail,
                    "steps": [{"name": "迁仓挂起", "ok": True, "detail": detail}],
                    "snapshot": snap,
                }
        if action == "scale":
            live = self.monitor(account)
            enter = (live or {}).get("enter") or {}
            allowed = enter.get("scale_ok") or force
            if not allowed:
                reason = enter.get("reason") or "当前不能加仓（uniMMR 不够或已开满）"
                if not auto:
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
                if not auto:
                    self.store.add_event(account.id, action, False, "已拦截：" + reason[:500])
                return {
                    "ok": False,
                    "blocked": True,
                    "reason": reason,
                    "steps": [],
                }
        if action == "harvest":
            side = (harvest_side or "").upper() or None
            if side not in {"LONG", "SHORT"}:
                side = None
            # 自动收利：交叉与收利级平稳须同时满足；迁仓仅手动
            wait_stable = not force
            run = lambda: cycle.harvest_once(
                wait_stable=wait_stable,
                side=side if auto else None,
                allow_migrate=not auto,
                auto=auto,
            )
        elif action == "scale":
            # 自动挂起加仓：按账户填写的两边各加数量；手动加仓仍按保证金目标加满
            pending_qty = None
            if auto:
                raw = getattr(account, "scale_add_qty", None)
                try:
                    pending_qty = d(raw) if raw not in (None, "") else None
                except Exception:
                    pending_qty = None
                if (
                    not getattr(account, "auto_scale", False)
                    or pending_qty is None
                    or pending_qty <= 0
                ):
                    return {
                        "ok": True,
                        "blocked": True,
                        "reason": "未挂起自动加仓",
                        "steps": [],
                    }
            run = lambda: cycle.scale_once(
                force=force or mode == "force",
                pending_qty=pending_qty,
            )
        elif action == "switch":
            run = lambda: cycle.switch_earn()
        elif action == "leverage":
            run = lambda: cycle.apply_leverage()
        elif action in {"sweep", "recycle", "fund"}:
            # 闲置保证金/现货 → 当前最高年化理财
            run = lambda: cycle.recycle_idle_to_earn()
        else:
            run = lambda: cycle.setup_once(
                force_hedge=force or mode == "force",
                skip_hedge=mode == "earn_only",
            )

        steps: list[StepResult] = []
        err: Exception | None = None
        hedge_actions = {"harvest", "enter", "scale"}
        need_hedge_guard = action in hedge_actions and mode != "earn_only"
        try:
            steps = list(run() or [])
            if getattr(cycle, "leverage_cap_changed", False):
                account = self.store.set_leverage_cap(
                    account.id,
                    getattr(cycle, "leverage_cap", None),
                    getattr(cycle, "leverage_unlock_at", None),
                )
        except Exception as exc:
            err = exc
            steps.append(StepResult("异常中断", False, str(exc)))
        finally:
            # 开仓/收利/加仓：无论成败/抛错，收尾再扫一遍缺口（补齐、削齐或市价平裸仓）
            if need_hedge_guard:
                try:
                    extra = cycle.emergency_hedge_guard()
                    if extra:
                        steps.extend(extra)
                except Exception as guard_exc:
                    steps.append(StepResult("收尾裸仓守卫失败", False, str(guard_exc)))
                # 硬闸写入步骤：结束后仍有敞口则记失败，绝不靠中间「对冲平衡」冒充成功
                try:
                    legs = cycle.futures.legs(cycle.symbol)
                    safe = legs.missing_side is None or (
                        legs.long_qty <= 0 and legs.short_qty <= 0
                    )
                    if not safe:
                        steps.append(
                            StepResult(
                                "对冲校验",
                                False,
                                f"结束后仍缺口：{cycle._legs_text(legs)}",
                            )
                        )
                except Exception as check_exc:
                    steps.append(StepResult("对冲校验失败", False, str(check_exc)))
            # 无论成功、失败还是中途抛错，都落库 + jsonl，避免重启后看不到开单过程
            self._persist_action_log(account, action, steps, auto=auto, error=err)

        if err is not None:
            raise err

        ok = _action_ok(steps, error=None)
        if not ok:
            fails = [f"{s.name}: {_detail(s.detail)}" for s in steps if not s.ok and not _post_only_fail(s)]
            if not fails:
                fails = [f"{s.name}: {_detail(s.detail)}" for s in steps if not s.ok]
            if fails:
                print(f"动作失败 account={account.id} action={action} " + " | ".join(fails)[:800])
        # 挂起自动加仓：真正加到仓后清空数量并关闭开关；软跳过（满仓/暂不加）保留挂起
        if ok and auto and action == "scale" and getattr(account, "auto_scale", False):
            soft = any(
                s.name in {"仓位已满", "暂不加仓", "加仓跳过"} for s in steps
            )
            # 「挂起加仓」只是计划步骤；须有实际成交步骤才清
            traded = any(
                s.ok and s.name in {"加仓完成", "加仓已拉齐", "加仓已削平"} for s in steps
            )
            if traded and not soft:
                try:
                    account = self.store.update(
                        account.id,
                        scale_add_qty=None,
                        scale_add_qty_set=True,
                        auto_scale=False,
                    )
                except Exception:
                    print(f"清空挂起加仓失败 account={account.id}")
        # 默认在本动作全部步骤完成后再等落账拉快照；串联子动作可 with_snapshot=False，由外层统一刷
        if with_snapshot:
            snap = self.refresh_account_snapshot(account)
        else:
            self.invalidate_snapshot(account.id)
            snap = None
        return {
            "ok": ok,
            "blocked": False,
            "live": True,
            "steps": steps_payload(steps),
            "snapshot": snap,
            "account": account.public_dict() if hasattr(account, "public_dict") else None,
        }

    @staticmethod
    def _is_noise_action_log(
        action: str,
        steps: list[StepResult],
        *,
        auto: bool,
        error: Exception | None,
    ) -> bool:
        """自动空跑（已满仓/暂不加/仅设杠杆/无成交暂停）不写事件，避免刷屏。"""
        if not auto or error is not None:
            return False
        if not steps:
            return True
        # 任何失败步骤都要记（含加仓跳过 FAIL）
        if any(not s.ok for s in steps):
            return False
        soft_noise = {
            "仓位已满",
            "暂不加仓",
            "加仓跳过",
            "设置杠杆",
            "查询杠杆",
            "读取杠杆",
            "开始分批换产品",
            "暂停换产品",
            "本轮结束",
            "闲置保证金",
            "归集理财",
        }
        # 真正动过仓/理财才值得记（不含空泛的「换产品/加仓」字样）
        real_keys = (
            "随保证金加仓",
            "加仓完成",
            "开仓",
            "平仓",
            "收利",
            "赎回",
            "申购",
            "划转",
            "兑换",
            "归集",
            "入金",
            "对冲",
            "挂单",
            "市价",
            "迁仓",
            "转回",
            "转入统一",
            "闲置保证金转现货",
        )
        has_real = False
        for s in steps:
            if s.name in soft_noise:
                continue
            if any(k in s.name for k in real_keys):
                has_real = True
                break
        if has_real:
            return False
        if action == "scale":
            return True
        if action == "switch":
            # 纯暂停/开场白、没有申购划转 → 噪声
            return True
        return all(s.name in soft_noise for s in steps)

    def _persist_action_log(
        self,
        account: Account,
        action: str,
        steps: list[StepResult],
        *,
        auto: bool = False,
        error: Exception | None = None,
    ) -> None:
        if self._is_noise_action_log(action, steps, auto=auto, error=error):
            return
        ok = _action_ok(steps, error=error)
        summary = _event_summary(steps, limit=12000)
        if error is not None and "异常中断" not in summary:
            summary = (summary + "；" if summary else "") + f"异常中断[FAIL]: {error}"
        if auto:
            prefix = {
                "enter": "自动开仓：",
                "switch": "自动换年化：",
                "scale": "自动加仓：",
                "sweep": "自动归集：",
                "harvest": "自动收利：",
            }.get(action, "自动：")
            summary = prefix + summary
        try:
            self.store.add_event(account.id, action, ok, summary[:12000])
        except Exception as exc:
            print(f"写 events 失败 account={account.id} action={action}: {exc}", flush=True)
        try:
            self.store.append_action_jsonl(
                {
                    "account_id": account.id,
                    "action": action,
                    "ok": ok,
                    "auto": auto,
                    "error": str(error) if error else "",
                    "steps": steps_payload(steps),
                    "summary": summary[:12000],
                }
            )
        except Exception as exc:
            print(f"写 actions.jsonl 失败 account={account.id} action={action}: {exc}", flush=True)
