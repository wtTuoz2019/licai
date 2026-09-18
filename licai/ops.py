from __future__ import annotations

import json
import time
from decimal import Decimal

from .config import Settings, d, fmt_amount, is_mmr_sentinel, load_settings, settings_for_account
from .cycle import HedgeCycle, position_plan
from .monitor import enter_advice, exit_ip, harvest_advice, price_stability, resolve_harvest_fee, schedule_hint
from .pipeline import Pipeline, StepResult
from .store import Account, AccountStore


def _detail(value) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, default=str)


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


def pipeline_for(account: Account, base: Settings | None = None, dry_run: bool = True) -> Pipeline:
    settings = settings_for_account(
        base or load_settings(),
        api_key=account.api_key,
        api_secret=account.api_secret,
        proxy=account.proxy,
        dry_run=dry_run,
        hedge_symbol=account.hedge_symbol,
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
        self._risk_cache: dict[int, dict] = {}

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
        self._risk_cache[account_id] = stored
        return stored

    def _risk_for(self, account: Account, futures, *, refresh: bool) -> dict:
        empty = {"uni_mmr": Decimal("0"), "equity": Decimal("0"), "available": Decimal("0")}
        if not refresh:
            return self._risk_cache.get(account.id) or empty
        if not getattr(futures, "unified", True):
            return self._remember_risk(account.id, empty)
        try:
            risk = futures.refresh_account_risk()
        except Exception:
            return self._risk_cache.get(account.id) or empty
        return self._remember_risk(account.id, risk)

    def snapshot(self, account: Account, *, force: bool = False) -> dict:
        now = time.monotonic()
        if not force:
            hit = self._snap_cache.get(account.id)
            if hit and now - hit[0] < self._snap_ttl:
                data = dict(hit[1])
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
            settings,
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
            "leverage": lev,
            "uni_mmr": mmr_text,
            "live_poll_seconds": int(settings.live_poll_seconds),
            "legs": {
                "long_qty": fmt_amount(legs.long_qty),
                "short_qty": fmt_amount(legs.short_qty),
                "long_pnl": fmt_amount(legs.long_pnl, 4),
                "short_pnl": fmt_amount(legs.short_pnl, 4),
                "missing": legs.missing_side,
            },
            "stability": stability.as_dict(),
            "enter": enter_advice(legs, stability, self._idle_spot(account), plan),
            "harvest": advice.as_dict(),
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
        ip = exit_ip(pipe.client)
        base["exit_ip"] = ip
        base["schedule"] = schedule_hint(settings)
        earn_error = ""
        try:
            wallet = pipe.wallet_view()
        except Exception as exc:
            earn_error = str(exc)
            wallet = {
                "spot_usdt": "0",
                "usdt_flexible": "0",
                "bfusd": "0",
                "earn_total": "0",
                "holdings": [],
                "next_buy": None,
                "status": str(exc),
            }
        spot_usdt = d(wallet.get("spot_usdt") or "0")
        collateral = d(wallet.get("earn_total") or "0") + spot_usdt
        try:
            legs = futures.legs(settings.hedge_symbol)
        except Exception as exc:
            return {**base, "ok": False, "error": str(exc)}
        risk = self._risk_for(account, futures, refresh=True)
        mmr = risk.get("uni_mmr") or Decimal("0")
        try:
            stability = price_stability(futures, settings)
        except Exception as exc:
            return {**base, "ok": False, "error": f"价格监控失败: {exc}"}
        last_harvest = self.store.last_ok_action_at(account.id, "harvest")
        fee = resolve_harvest_fee(futures, settings.hedge_symbol, legs, stability.mid, settings)
        advice = harvest_advice(legs, stability, settings, last_harvest, principal=collateral, fee=fee)
        try:
            _, step = futures.filters(settings.hedge_symbol)
        except Exception:
            step = Decimal("0.001")
        plan = position_plan(
            settings,
            stability.mid,
            collateral,
            legs,
            mmr,
            lambda q: futures.round_qty(q, step),
            available=risk.get("available"),
        )
        enter = enter_advice(legs, stability, spot_usdt, plan)
        try:
            switch = pipe.switch_advice()
        except Exception as exc:
            switch = {"needed": False, "can_click": False, "reason": str(exc)}
        try:
            margin = pipe.margin_status()
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
            "leverage": lev,
            "live_poll_seconds": int(settings.live_poll_seconds),
            "collateral_usdt": fmt_amount(collateral, 4),
            "spot_usdt": wallet.get("spot_usdt") or fmt_amount(spot_usdt, 4),
            "earn_total": wallet.get("earn_total") or "0",
            "usdt_flexible": wallet.get("usdt_flexible") or "0",
            "bfusd": wallet.get("bfusd") or "0",
            "wallet_status": wallet.get("status") or "",
            "next_buy": wallet.get("next_buy"),
            "uni_mmr": mmr_text,
            "pm_equity": margin.get("equity") or "0",
            "margin": margin,
            "legs": {
                "long_qty": fmt_amount(legs.long_qty),
                "short_qty": fmt_amount(legs.short_qty),
                "long_pnl": fmt_amount(legs.long_pnl, 4),
                "short_pnl": fmt_amount(legs.short_pnl, 4),
                "missing": legs.missing_side,
            },
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

    def run_action(self, account: Account, action: str, force: bool = False, mode: str | None = None) -> dict:
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
            allowed = harvest.get("can_click") or (force and harvest.get("force_ok"))
            if not allowed:
                reason = harvest.get("reason") or live.get("error") or "现在不适合平仓"
                self.store.add_event(account.id, action, False, "已拦截：" + reason[:500])
                return {
                    "ok": False,
                    "blocked": True,
                    "live": True,
                    "reason": reason,
                    "steps": [],
                    "snapshot": self.snapshot(account),
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
                    "snapshot": self.snapshot(account),
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
                    "snapshot": snap,
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
        summary = "；".join(f"{step.name}: {_detail(step.detail)}" for step in steps[-4:]) or "无步骤"
        self.store.add_event(account.id, action, ok, summary[:2000])
        return {
            "ok": ok,
            "blocked": False,
            "live": True,
            "steps": steps_payload(steps),
            "snapshot": self.snapshot(account, force=True),
        }
