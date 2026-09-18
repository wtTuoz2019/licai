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


def exit_ip(client: BinanceClient) -> str:
    key = client.proxy or ""
    now = time.monotonic()
    hit = _ip_cache.get(key)
    if hit and now - hit[0] < _IP_TTL:
        return hit[1]
    try:
        response = client.session.get("https://api.ipify.org", timeout=4)
        response.raise_for_status()
        ip = response.text.strip() or "未知"
    except Exception:
        ip = "未知"
    _ip_cache[key] = (now, ip)
    return ip


def price_stability(futures: FuturesAPI, settings: Settings) -> Stability:
    symbol = settings.hedge_symbol
    bid, ask = futures.book(symbol)
    mid = (bid + ask) / 2 if bid + ask > 0 else Decimal("0")
    spread_pct = (ask - bid) / mid if mid > 0 else Decimal("1")
    rows = futures.klines(symbol, "1m", 20)
    if not rows:
        return Stability(symbol, mid, spread_pct, Decimal("1"), Decimal("1"), False, "拉不到 K 线，暂不入场、也不平仓")
    highs = [d(row[2]) for row in rows]
    lows = [d(row[3]) for row in rows]
    last = d(rows[-1][4]) or mid
    range_15m = (max(highs) - min(lows)) / last if last > 0 else Decimal("1")
    last5 = rows[-5:]
    range_5m = (max(d(row[2]) for row in last5) - min(d(row[3]) for row in last5)) / last if last > 0 else Decimal("1")
    limit = settings.stable_range_pct
    stable = range_15m <= limit and range_5m <= (limit / 2) and spread_pct <= Decimal("0.0004")
    if stable:
        hint = f"价格较稳：15 分钟振幅 {_pct(range_15m)}%，盘口价差 {_pct(spread_pct)}%，适合入场或平仓"
    elif range_15m > limit:
        hint = f"波动偏大：15 分钟振幅 {_pct(range_15m)}%，超过 {_pct(limit)}%，现在入场/平仓容易滑点、两边难一起成交"
    elif range_5m > limit / 2:
        hint = f"近 5 分钟还在晃：振幅 {_pct(range_5m)}%，建议再等一会儿再入场或平仓"
    else:
        hint = f"盘口偏宽：价差 {_pct(spread_pct)}%，限价不容易两边一起成交"
    return Stability(symbol, mid, spread_pct, range_15m, range_5m, stable, hint)


def harvest_fee(legs: HedgeLegs, mid: Decimal, fee_rate: Decimal) -> Decimal:
    qty = max(legs.long_qty, legs.short_qty)
    if qty <= 0 or mid <= 0:
        return Decimal("0")
    # 平盈利腿 + 补回另一边，按传入费率估两笔
    return qty * mid * fee_rate * Decimal("2")


def harvest_fee_estimate(legs: HedgeLegs, mid: Decimal, settings) -> Decimal:
    # 页面统计按吃单费率估，避免把 0.05% 当成 0.02% 低估
    rate = getattr(settings, "taker_fee_rate", None) or settings.maker_fee_rate
    return harvest_fee(legs, mid, rate)


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
        extra = (
            "点「一键入场」两边一起加。"
            if stability.stable
            else "价格不稳，点入场后选「强行加仓」。"
        )
        if scale_ok:
            reason = (
                f"已有对冲。uniMMR {mmr}（越大越安全，爆仓约 1.05）。"
                f"目标 {target}，当前 {current}，还可加 {add}。{extra}"
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
) -> HarvestAdvice:
    fee = harvest_fee_estimate(legs, stability.mid, settings)
    notional = position_notional(legs, stability.mid)
    auto = auto_harvest_profit(principal, fee, settings, notional)
    min_profit = min_harvest_profit(principal, fee, settings, notional)
    custom = bool(settings.take_profit_custom and settings.take_profit_usdt > 0)
    if legs.long_qty <= 0 and legs.short_qty <= 0:
        return HarvestAdvice(
            None,
            Decimal("0"),
            Decimal("0"),
            min_profit,
            Decimal("0"),
            Decimal("0"),
            0,
            False,
            True,
            stability.stable,
            False,
            False,
            "还没有对冲仓位",
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
        reason = "价格不稳，可以点「一键收利」后选择强行收利"
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
        f"价格 15 分钟振幅 ≤ {_pct(settings.stable_range_pct)}%、"
        f"收利门槛按仓位名义和约 {fmt_amount(settings.min_profit_fee_multiple, 0)} 倍手续费计算、"
        f"且距上次平仓超过 {settings.cooldown_minutes} 分钟时，再点「一键收利」。"
    )
