from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
import time

from .client import BinanceClient
from .config import Settings, d, fmt_amount
from .futures import FuturesAPI, HedgeLegs


@dataclass
class Stability:
    symbol: str
    mid: Decimal
    spread_pct: Decimal
    range_15m_pct: Decimal
    range_5m_pct: Decimal
    stable: bool
    hint: str

    def as_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "mid": fmt_amount(self.mid, 2),
            "spread_pct": _pct(self.spread_pct),
            "range_15m_pct": _pct(self.range_15m_pct),
            "range_5m_pct": _pct(self.range_5m_pct),
            "stable": self.stable,
            "hint": self.hint,
        }


@dataclass
class HarvestAdvice:
    winner: str | None
    winner_pnl: Decimal
    round_trip_fee: Decimal
    min_profit: Decimal
    net_after_fee: Decimal
    fee_cover_multiple: Decimal
    cooldown_left_seconds: int
    profit_ok: bool
    cooldown_ok: bool
    stable_ok: bool
    can_click: bool
    force_ok: bool
    reason: str
    auto_profit: Decimal = Decimal("0")
    custom: bool = False

    def as_dict(self) -> dict:
        return {
            "winner": self.winner,
            "winner_pnl": fmt_amount(self.winner_pnl, 4),
            "round_trip_fee": fmt_amount(self.round_trip_fee, 4),
            "min_profit": fmt_amount(self.min_profit, 4),
            "net_after_fee": fmt_amount(self.net_after_fee, 4),
            "fee_cover_multiple": fmt_amount(self.fee_cover_multiple, 2),
            "cooldown_left_seconds": self.cooldown_left_seconds,
            "profit_ok": self.profit_ok,
            "cooldown_ok": self.cooldown_ok,
            "stable_ok": self.stable_ok,
            "can_click": self.can_click,
            "force_ok": self.force_ok,
            "reason": self.reason,
            "auto_profit": fmt_amount(self.auto_profit, 4),
            "custom": self.custom,
        }


def _pct(value: Decimal) -> str:
    return f"{(value * Decimal('100')):.3f}"


_IP_TTL = 6 * 3600.0
_ip_cache: dict[str, tuple[float, str]] = {}


def exit_ip(client: BinanceClient, *, force: bool = False) -> str:
    """出口 IP。默认只用缓存（过期也复用），不拖慢刷新；force=True 才重查。"""
    key = client.proxy or ""
    now = time.monotonic()
    hit = _ip_cache.get(key)
    if hit and (not force) and now - hit[0] < _IP_TTL:
        return hit[1]
    if hit and not force:
        return hit[1]
    try:
        response = client.session.get("https://api.ipify.org", timeout=4)
        response.raise_for_status()
        ip = response.text.strip() or "未知"
    except Exception:
        ip = hit[1] if hit else "未知"
    _ip_cache[key] = (now, ip)
    return ip


def price_stability(
    futures: FuturesAPI,
    settings: Settings,
    *,
    range_pct: Decimal | None = None,
    spread_pct_limit: Decimal | None = None,
    purpose: str = "enter",
) -> Stability:
    """平稳判定：15/5/2 分钟振幅 + 盘口价差。

    purpose=harvest 时用收利专用更宽阈值（harvest_stable_*），入场仍用更严的 stable_*。
    """
    symbol = settings.hedge_symbol
    bid, ask = futures.book(symbol)
    mid = (bid + ask) / 2 if bid + ask > 0 else Decimal("0")
    spread_pct = (ask - bid) / mid if mid > 0 else Decimal("1")
    rows = futures.klines(symbol, "1m", 30)
    if not rows:
        return Stability(symbol, mid, spread_pct, Decimal("1"), Decimal("1"), False, "拉不到 K 线，暂不入场、也不平仓")
    last = d(rows[-1][4]) or mid
    if last <= 0:
        return Stability(symbol, mid, spread_pct, Decimal("1"), Decimal("1"), False, "现价异常，暂不入场、也不平仓")

    def _range(slice_rows) -> Decimal:
        if not slice_rows:
            return Decimal("1")
        hi = max(d(row[2]) for row in slice_rows)
        lo = min(d(row[3]) for row in slice_rows)
        return (hi - lo) / last

    range_15m = _range(rows[-15:])
    range_5m = _range(rows[-5:])
    range_2m = _range(rows[-2:])
    if range_pct is not None:
        limit = range_pct
    elif purpose == "harvest":
        limit = getattr(settings, "harvest_stable_range_pct", None) or settings.stable_range_pct
    else:
        limit = settings.stable_range_pct
    limit_5m = limit * Decimal("0.45")
    limit_2m = limit * Decimal("0.28")
    if spread_pct_limit is not None:
        spread_limit = spread_pct_limit
    elif purpose == "harvest":
        spread_limit = (
            getattr(settings, "harvest_stable_spread_pct", None)
            or getattr(settings, "stable_spread_pct", None)
            or Decimal("0.0003")
        )
    else:
        spread_limit = getattr(settings, "stable_spread_pct", None) or Decimal("0.0003")
    stable = (
        range_15m <= limit
        and range_5m <= limit_5m
        and range_2m <= limit_2m
        and spread_pct <= spread_limit
    )
    tag = "收利" if purpose == "harvest" else "入场"
    if stable:
        hint = (
            f"价格较稳（{tag}）：15 分 {_pct(range_15m)}% / 5 分 {_pct(range_5m)}% / 近 2 分 {_pct(range_2m)}%，"
            f"价差 {_pct(spread_pct)}%，适合挂单"
        )
    elif range_15m > limit:
        hint = f"波动偏大：15 分钟振幅 {_pct(range_15m)}%，超过 {_pct(limit)}%（{tag}），挂单难成交、易滑点"
    elif range_5m > limit_5m:
        hint = f"近 5 分钟还在晃：振幅 {_pct(range_5m)}%，超过 {_pct(limit_5m)}%（{tag}），再等一会再挂单"
    elif range_2m > limit_2m:
        hint = f"刚有跳动：近 2 分钟振幅 {_pct(range_2m)}%，等盘口稳住再挂单平/开"
    else:
        hint = f"盘口偏宽：价差 {_pct(spread_pct)}%，超过 {_pct(spread_limit)}%（{tag}），限价不易两边一起成交"
    return Stability(symbol, mid, spread_pct, range_15m, range_5m, stable, hint)


def harvest_fee(legs: HedgeLegs, mid: Decimal, fee_rate: Decimal) -> Decimal:
    qty = max(legs.long_qty, legs.short_qty)
    if qty <= 0 or mid <= 0:
        return Decimal("0")
    return qty * mid * fee_rate * Decimal("2")


def harvest_fee_estimate(legs: HedgeLegs, mid: Decimal, settings) -> Decimal:
    rate = getattr(settings, "taker_fee_rate", None) or settings.maker_fee_rate
    return harvest_fee(legs, mid, rate)


def resolve_harvest_fee(futures, symbol: str, legs: HedgeLegs, mid: Decimal, settings) -> Decimal:
    qty = max(legs.long_qty, legs.short_qty)
    try:
        info = futures.round_trip_fee(symbol, qty, mid)
        fee = info.get("fee") or Decimal("0")
        if fee > 0:
            return fee
    except Exception:
        pass
    return harvest_fee_estimate(legs, mid, settings)


def position_notional(legs: HedgeLegs, mid: Decimal) -> Decimal:
    qty = max(legs.long_qty, legs.short_qty)
    if qty <= 0 or mid <= 0:
        return Decimal("0")
    return qty * mid


def auto_harvest_profit(
    principal: Decimal,
    fee: Decimal,
    settings: Settings,
    notional: Decimal = Decimal("0"),
) -> Decimal:
    need = fee * settings.min_profit_fee_multiple
    if settings.harvest_min_usdt > 0:
        need = max(need, settings.harvest_min_usdt)
    if notional > 0 and settings.harvest_pos_pct > 0:
        need = max(need, notional * settings.harvest_pos_pct)
    elif principal > 0:
        need = max(need, principal * settings.harvest_principal_pct)
        if settings.harvest_sqrt_coeff > 0:
            need = max(need, settings.harvest_sqrt_coeff * principal.sqrt())
    return need


def min_harvest_profit(
    principal: Decimal,
    fee: Decimal,
    settings: Settings,
    notional: Decimal = Decimal("0"),
) -> Decimal:
    # 自己填了「每次收利」就按填写值，不再被手续费×8 顶回去。
    if settings.take_profit_custom and settings.take_profit_usdt > 0:
        floor = fee if fee > 0 else Decimal("0")
        if settings.harvest_min_usdt > 0:
            floor = max(floor, settings.harvest_min_usdt)
        return max(settings.take_profit_usdt, floor)
    return auto_harvest_profit(principal, fee, settings, notional)


def cooldown_left(last_ok_at: str | None, minutes: int) -> int:
    if not last_ok_at:
        return 0
    try:
        last = datetime.fromisoformat(last_ok_at.replace("Z", "+00:00"))
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
    except ValueError:
        return 0
    elapsed = (datetime.now(timezone.utc) - last).total_seconds()
    remain = minutes * 60 - elapsed
    return int(remain) if remain > 0 else 0


def enter_advice(legs: HedgeLegs, stability: Stability, idle_spot: Decimal = Decimal("0"), plan: dict | None = None) -> dict:
    repairing = legs.missing_side in {"LONG", "SHORT", "IMBALANCE"}
    if repairing:
        return {
            "can_click": True,
            "stable_ok": stability.stable,
            "scale_ok": False,
            "reason": f"对冲缺口（{legs.missing_side}），尽快点「一键入场」补仓，不必等价格平稳",
        }
    plan = plan or {}
    hedged = legs.missing_side is None and (legs.long_qty > 0 or legs.short_qty > 0)
    if hedged:
        add = plan.get("add_qty") or "0"
        target = plan.get("target_qty") or "-"
        current = plan.get("current_qty") or fmt_amount(legs.long_qty)
        mmr = plan.get("uni_mmr") or "-"
        scale_ok = bool(plan.get("scale_ok"))
        if scale_ok:
            reason = (
                f"已有对冲。uniMMR {mmr}（越大越安全，爆仓约 1.05）。"
                f"目标 {target}，当前 {current}，还可加 {add}。点「加仓」手动放大。"
            )
        else:
            reason = f"仓位已按安全上限开满。uniMMR {mmr}，目标 {target}。"
        return {
            "can_click": True,
            "stable_ok": stability.stable,
            "scale_ok": scale_ok,
            "target_qty": str(target),
            "current_qty": str(current),
            "add_qty": str(add),
            "uni_mmr": str(mmr),
            "reason": reason,
        }
    if idle_spot > 0:
        extra = (
            "价格已经平稳，这次会申购并开对冲。"
            if stability.stable
            else "价格还不稳，这次先买理财、转入保证金，先不开对冲。等标签变绿再点一次开仓。"
        )
        return {
            "can_click": True,
            "stable_ok": stability.stable,
            "scale_ok": False,
            "reason": f"现货还有 {fmt_amount(idle_spot, 2)} USDT 闲着，可以先点「一键入场」。{extra}",
        }
    if not stability.stable:
        return {
            "can_click": False,
            "stable_ok": False,
            "scale_ok": False,
            "reason": "理财已就绪的话，等价格平稳再开对冲。" + stability.hint,
        }
    return {
        "can_click": True,
        "stable_ok": True,
        "scale_ok": False,
        "reason": "价格平稳，可以点「一键入场」开/补对冲",
    }


def harvest_advice(
    legs: HedgeLegs,
    stability: Stability,
    settings: Settings,
    last_harvest_at: str | None,
    principal: Decimal = Decimal("0"),
    fee: Decimal | None = None,
    notional: Decimal | None = None,
) -> HarvestAdvice:
    if fee is None:
        fee = harvest_fee_estimate(legs, stability.mid, settings)
    if notional is None or notional <= 0:
        notional = position_notional(legs, stability.mid)
    auto = auto_harvest_profit(principal, fee, settings, notional)
    min_profit = min_harvest_profit(principal, fee, settings, notional)
    custom = bool(settings.take_profit_custom and settings.take_profit_usdt > 0)
    if legs.long_qty <= 0 and legs.short_qty <= 0:
        lev = int(getattr(settings, "hedge_leverage", 0) or 0)
        if notional > 0:
            reason = (
                f"还没有对冲仓位。按预计 {lev}x 开仓名义约 {fmt_amount(notional, 0)}，"
                f"建议每次收利 {fmt_amount(min_profit, 4)}"
            )
        else:
            reason = "还没有对冲仓位"
        return HarvestAdvice(
            None,
            Decimal("0"),
            fee,
            min_profit,
            Decimal("0"),
            Decimal("0"),
            0,
            False,
            True,
            stability.stable,
            False,
            False,
            reason,
            auto,
            custom,
        )
    winner = "LONG" if legs.long_pnl >= legs.short_pnl else "SHORT"
    pnl = legs.long_pnl if winner == "LONG" else legs.short_pnl
    net = pnl - fee
    multiple = (pnl / fee) if fee > 0 else Decimal("0")
    left = cooldown_left(last_harvest_at, settings.cooldown_minutes)
    profit_ok = pnl >= min_profit and pnl > 0
    cooldown_ok = left <= 0
    stable_ok = stability.stable
    force_ok = profit_ok and cooldown_ok and legs.missing_side is None
    can_click = force_ok and stable_ok
    if legs.missing_side is not None:
        reason = "对冲缺口，先点「一键入场」补仓，不要在单边时平仓"
    elif not profit_ok:
        if custom:
            reason = f"浮盈 {fmt_amount(pnl, 4)}，自定义每次收利 {fmt_amount(min_profit, 4)} USDT"
        else:
            reason = (
                f"浮盈 {fmt_amount(pnl, 4)}，门槛 {fmt_amount(min_profit, 4)}（仓位 {fmt_amount(notional, 0)}，"
                f"手续费约 {fmt_amount(fee, 4)}×{fmt_amount(settings.min_profit_fee_multiple, 0)}）。等够了再收，避免磨损"
            )
    elif not cooldown_ok:
        reason = f"距上次平仓还差 {left} 秒冷却，先别连点"
    elif not stable_ok:
        reason = "价格还不够稳（收利已放宽振幅，仍超限），等稳一点再收；也可强行收利（易吃单）"
    else:
        reason = (
            f"{winner} 浮盈 {fmt_amount(pnl, 4)}，扣完约 {fmt_amount(fee, 4)} 手续费后净利约 {fmt_amount(net, 4)}。"
            "价格平稳，可以点「一键收利」"
        )
    return HarvestAdvice(
        winner,
        pnl,
        fee,
        min_profit,
        net,
        multiple,
        left,
        profit_ok,
        cooldown_ok,
        stable_ok,
        can_click,
        force_ok,
        reason,
        auto,
        custom,
    )


def schedule_hint(settings: Settings) -> str:
    return (
        f"资金/理财一天拉一次，点「刷新」才重拉。"
        f"仓位浮盈大约每 {int(settings.live_poll_seconds)} 秒更新。"
        f"入场要够稳（15 分 ≤ {_pct(settings.stable_range_pct)}%）；"
        f"收利更宽（15 分 ≤ {_pct(getattr(settings, 'harvest_stable_range_pct', None) or settings.stable_range_pct)}%）。"
        f"收利门槛按仓位名义和约 {fmt_amount(settings.min_profit_fee_multiple, 0)} 倍手续费、"
        f"距上次平仓超过 {settings.cooldown_minutes} 分钟。"
    )
