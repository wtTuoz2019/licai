from __future__ import annotations

import time
from decimal import Decimal

from .client import BinanceAPIError
from .config import d, fmt_amount, is_mmr_sentinel
from .futures import FuturesAPI, HedgeLegs
from .monitor import min_harvest_profit, price_stability, resolve_harvest_fee
from .pipeline import Pipeline, StepResult


LIMIT_FILL_TRIES = 3


def _subscribed(steps: list[StepResult]) -> bool:
    return any((not step.dry_run) and step.ok and ("申购" in step.name) for step in steps)


def _no_margin(step: StepResult) -> bool:
    if step.ok:
        return False
    text = str(step.detail or "").lower()
    return "-2019" in text or "margin is insufficient" in text


def _post_only_reject(step: StepResult) -> bool:
    if step.ok:
        return False
    text = str(step.detail or "").lower()
    return "-5022" in text or "post only" in text or "could not be executed as maker" in text


def margin_use_pct(settings) -> Decimal:
    cap = settings.hedge_margin_use_pct
    lev = max(int(settings.hedge_leverage), 1)
    return min(cap, Decimal("40") / Decimal(lev))


def mmr_qty_cap(current: Decimal, mmr: Decimal, min_uni_mmr: Decimal) -> Decimal | None:
    if current <= 0 or is_mmr_sentinel(mmr):
        return None
    if mmr < min_uni_mmr:
        return current
    headroom = min_uni_mmr * Decimal("1.5")
    if headroom <= 0:
        return None
    return current * mmr / headroom


def position_plan(
    settings,
    mid: Decimal,
    collateral: Decimal,
    legs: HedgeLegs,
    mmr: Decimal,
    round_qty,
    available: Decimal | None = None,
) -> dict:
    use = margin_use_pct(settings)
    lev = Decimal(max(int(settings.hedge_leverage), 1))
    # 双向持仓：多空两边都要初始保证金。单边名义 = 本金×占用×杠杆 / 2
    raw = collateral * use * lev / (Decimal("2") * mid) if mid > 0 and collateral > 0 else Decimal("0")
    target = round_qty(raw) if raw > 0 else Decimal("0")
    current = (
        min(legs.long_qty, legs.short_qty)
        if legs.long_qty > 0 and legs.short_qty > 0
        else max(legs.long_qty, legs.short_qty)
    )
    cap = mmr_qty_cap(current, mmr, settings.min_uni_mmr)
    if cap is not None:
        target = min(target, round_qty(cap))
    if available is not None and available > 0 and mid > 0:
        add_room = round_qty(available * Decimal("0.95") * lev / (Decimal("2") * mid))
        if current > 0:
            target = min(target, current + add_room)
        else:
            target = min(target, add_room)
    add = target - current if current > 0 else target
    if add < 0:
        add = Decimal("0")
    min_add = max(Decimal("0.001"), current * settings.scale_min_add_pct) if current > 0 else Decimal("0")
    scale_ok = add >= min_add if current > 0 else target > 0
    return {
        "use_pct": fmt_amount(use, 4),
        "leverage": int(settings.hedge_leverage),
        "target_qty": fmt_amount(target),
        "current_qty": fmt_amount(current),
        "add_qty": fmt_amount(add),
        "uni_mmr": None if is_mmr_sentinel(mmr) else fmt_amount(mmr, 4),
        "min_uni_mmr": fmt_amount(settings.min_uni_mmr, 2),
        "scale_ok": scale_ok,
    }


class HedgeCycle:
    """
    理财仓位当保证金，多空对冲。

    一键收利：平掉浮盈腿 → 归集 → 最大可转出 USDT 转到现货 → 立刻补回对冲
    → 现货 USDT 买理财 → 再转入统一账户当保证金。理财代币本身不抽走。
    """

    def __init__(self, pipeline: Pipeline):
        self.pipeline = pipeline
        self.settings = pipeline.settings
        self.futures: FuturesAPI = pipeline.futures
        self.symbol = self.settings.hedge_symbol
        self.leverage_cap = self.settings.leverage_cap
        self.leverage_unlock_at = self.settings.leverage_unlock_at
        self.leverage_cap_changed = False

    def run(self, watch: bool = True) -> list[StepResult]:
        steps: list[StepResult] = []
        if self.settings.hedge_leverage >= 50:
            steps.append(
                StepResult(
                    "风险提示",
                    True,
                    f"杠杆 {self.settings.hedge_leverage}x，单边浮盈平仓后若未立刻补仓，1% 波动就可能爆掉剩下的腿",
                )
            )
        steps.extend(self._bootstrap_funds())
        if any(not s.ok for s in steps):
            return steps
        steps.extend(self._prepare_account())
        if any(not s.ok for s in steps):
            return steps
        steps.extend(self._ensure_hedge())
        if any(not s.ok for s in steps):
            return steps
        if self.settings.dry_run or not watch:
            steps.append(StepResult("盯盘", True, "模拟盘只走完开仓准备；操作页一键执行，默认不自动盯盘下单"))
            return steps
        return self._watch_loop(steps)

    def setup_once(self, force_hedge: bool = False, skip_hedge: bool = False) -> list[StepResult]:
        steps: list[StepResult] = []
        legs = self.futures.legs(self.symbol)
        hedged = legs.missing_side is None and legs.long_qty > 0 and legs.short_qty > 0
        repairing = legs.missing_side in {"LONG", "SHORT", "IMBALANCE"}

        if skip_hedge and not repairing:
            steps.extend(self._bootstrap_funds())
            steps.append(StepResult("暂缓开对冲", True, "按选择只处理理财"))
            return steps

        if hedged:
            if not force_hedge:
                try:
                    stability = price_stability(self.futures, self.settings)
                except Exception:
                    stability = None
                if stability is not None and not stability.stable:
                    return [
                        StepResult("暂不加仓", True, "已有对冲，价格不稳。要加仓请选「强行加仓」"),
                    ]
            try:
                idle = self.pipeline.spot_cash()
            except Exception:
                idle = Decimal("0")
            if idle >= 1:
                steps.extend(self._bootstrap_funds())
                if any(not s.ok for s in steps):
                    return steps
            steps.extend(self._scale_to_collateral())
            return steps

        if repairing:
            steps.extend(self._ensure_hedge())
            return steps

        steps.extend(self._bootstrap_funds())
        if any(not s.ok for s in steps):
            return steps
        steps.extend(self._prepare_account())
        if any(not s.ok for s in steps):
            return steps
        try:
            stability = price_stability(self.futures, self.settings)
        except Exception:
            stability = None
        if not force_hedge and stability is not None and not stability.stable:
            steps.append(
                StepResult(
                    "暂缓开对冲",
                    True,
                    "理财已在保证金。现在价格不稳，等标签变绿后再开，或下次选择「强行开仓」",
                )
            )
            return steps
        steps.extend(self._ensure_hedge())
        return steps

    def _bootstrap_funds(self) -> list[StepResult]:
        steps = list(self.pipeline.sweep_spot_to_earn())
        if any(not s.ok for s in steps):
            return steps
        bought = _subscribed(steps)
        if bought and not self.settings.dry_run:
            time.sleep(max(int(self.settings.settle_seconds or 0), 3))
        steps.extend(self._fund_after_earn(steps, check_idle_ldusdt=True))
        return steps

    def _fund_after_earn(self, earn_steps: list[StepResult], *, check_idle_ldusdt: bool) -> list[StepResult]:
        bought = _subscribed(earn_steps)
        bought_flex = any((not s.dry_run) and s.ok and "申购活期" in s.name for s in earn_steps)
        check_ld = bought_flex or (check_idle_ldusdt and not bought)
        if bought or self.pipeline.spot_needs_move() or check_ld:
            return self.pipeline.fund_unified(check_ldusdt=check_ld)
        return []

    def harvest_once(self) -> list[StepResult]:
        legs = self.futures.legs(self.symbol)
        if legs.missing_side is not None:
            return [StepResult("平仓跳过", False, f"对冲不平衡，先补仓：{self._legs_text(legs)}")]
        winner = "LONG" if legs.long_pnl >= legs.short_pnl else "SHORT"
        pnl = legs.long_pnl if winner == "LONG" else legs.short_pnl
        if pnl <= 0:
            return [StepResult("平仓跳过", False, "两边都没有浮盈")]
        return self._harvest(winner, legs)

    def _mutate(self, name: str, fn) -> StepResult:
        if self.settings.dry_run:
            return StepResult(name, True, "dry-run 未真实下单", dry_run=True)
        try:
            return StepResult(name, True, fn(), dry_run=False)
        except BinanceAPIError as exc:
            return StepResult(name, False, str(exc), dry_run=False)

    def apply_leverage(self) -> list[StepResult]:
        target = int(self.settings.hedge_leverage_target or self.settings.hedge_leverage)
        if self.settings.dry_run:
            return [StepResult("设置杠杆", True, f"dry-run 将按优先级尝试 {target}x", dry_run=True)]
        try:
            data = self.futures.apply_best_leverage(self.symbol, target)
        except BinanceAPIError as exc:
            return [StepResult("设置杠杆", False, str(exc))]
        applied = int(data.get("leverage") or target)
        self.settings.hedge_leverage = applied
        self.leverage_cap = applied
        self.leverage_unlock_at = data.get("unlock_at")
        self.leverage_cap_changed = True
        return [StepResult("设置杠杆", True, data.get("note") or f"已设置 {applied}x")]

    def _prepare_account(self) -> list[StepResult]:
        steps = [self._mutate("开启双向持仓", self.futures.enable_hedge_mode)]
        target = int(self.settings.hedge_leverage_target or self.settings.hedge_leverage)
        known = int(self.settings.leverage_cap or 0)
        if known <= 0:
            try:
                known = self.futures.current_leverage(self.symbol)
            except Exception:
                known = 0
        if known <= 0:
            known = 5
        self.settings.hedge_leverage = min(target, known)
        if self.settings.hedge_leverage < target:
            steps.append(
                StepResult(
                    "杠杆",
                    True,
                    f"入场按 {self.settings.hedge_leverage}x 算仓。要调高请用设置里的「设置杠杆」",
                )
            )
        return steps

    def _ensure_hedge(self) -> list[StepResult]:
        if self.settings.dry_run:
            qty = self._collateral_qty()
            try:
                collateral = self.pipeline.earn.earn_margin_usdt()
            except Exception:
                collateral = Decimal("0")
            return [
                StepResult(
                    "开对冲",
                    True,
                    f"理财保证金约 {fmt_amount(collateral)} USDT，按 {margin_use_pct(self.settings)} 占用、{self.settings.hedge_leverage}x、双边各占保证金，开 "
                    f"两边先挂单（GTX），不成交再改价，最后市价补齐  数量={fmt_amount(qty)} {self.symbol}",
                    dry_run=True,
                )
            ]
        legs = self.futures.legs(self.symbol)
        if legs.missing_side is None:
            steps = [StepResult("对冲已平衡", True, self._legs_text(legs))]
            steps.extend(self._scale_to_collateral())
            return steps
        qty = self._restore_qty(legs)
        if qty <= 0:
            qty = self._collateral_qty()
        if qty <= 0:
            return [StepResult("开对冲", False, "算出的下单数量为 0，检查理财仓位或 hedge_qty")]
        steps = self._quote_until_balanced(qty, reduce_only=False)
        legs = self.futures.legs(self.symbol)
        if legs.missing_side is None:
            steps.extend(self._scale_to_collateral())
        return steps

    def _watch_loop(self, steps: list[StepResult]) -> list[StepResult]:
        print("进入盯盘，Ctrl+C 结束")
        try:
            while True:
                legs = self.futures.legs(self.symbol)
                print(self._legs_text(legs))
                if legs.missing_side is not None:
                    print("对冲缺口，先补仓再把闲置盈利申购理财")
                    steps.extend(self._quote_until_balanced(self._restore_qty(legs), reduce_only=False))
                else:
                    try:
                        risk = self.futures.account_risk()
                        principal = risk.get("equity") or Decimal("0")
                    except Exception:
                        principal = Decimal("0")
                    bid, ask = self.futures.book(self.symbol)
                    mid = (bid + ask) / 2
                    need = min_harvest_profit(
                        principal,
                        resolve_harvest_fee(self.futures, self.symbol, legs, mid, self.settings),
                        self.settings,
                        max(legs.long_qty, legs.short_qty) * mid,
                    )
                    if legs.long_pnl >= need:
                        steps.extend(self._harvest("LONG", legs))
                    elif legs.short_pnl >= need:
                        steps.extend(self._harvest("SHORT", legs))
                time.sleep(self.settings.watch_seconds)
        except KeyboardInterrupt:
            steps.append(self._mutate(f"撤销 {self.symbol} 挂单", lambda: self.futures.cancel_open(self.symbol)))
            steps.append(StepResult("停止", True, "已手动停止盯盘"))
        return steps

    def _harvest(self, side: str, legs: HedgeLegs) -> list[StepResult]:
        qty = legs.long_qty if side == "LONG" else legs.short_qty
        pnl = legs.long_pnl if side == "LONG" else legs.short_pnl
        steps = [
            StepResult(
                "触发止盈",
                True,
                f"平掉 {side} 浮盈约 {fmt_amount(pnl)} USDT，归集后转到现货买理财，再补仓",
            )
        ]
        close_side = "SELL" if side == "LONG" else "BUY"
        steps.extend(self._quote_side(close_side, side, qty, reduce_only=True))
        after = self.futures.legs(self.symbol)
        if after.missing_side is None:
            steps.append(StepResult("平仓异常", False, "平仓后两侧仍在，没有实现盈利可转出"))
            return steps
        steps.extend(self._profit_to_spot(pnl))
        refill_qty = after.short_qty if side == "LONG" else after.long_qty
        refill_pos = "LONG" if side == "LONG" else "SHORT"
        refill_order = "BUY" if refill_pos == "LONG" else "SELL"
        steps.extend(self._quote_side(refill_order, refill_pos, refill_qty, reduce_only=False))
        restored = self.futures.legs(self.symbol)
        if restored.missing_side is not None:
            steps.append(StepResult("补仓未完成", False, "市价后仍缺一边，平掉已开的单边，避免裸仓"))
            steps.extend(self._flatten_if_naked())
            restored = self.futures.legs(self.symbol)
        earn_steps = self._spot_profit_to_earn()
        steps.extend(earn_steps)
        if not self.settings.dry_run:
            time.sleep(max(int(self.settings.settle_seconds or 0), 2))
        steps.extend(self._fund_after_earn(earn_steps, check_idle_ldusdt=False))
        if restored.missing_side is None:
            steps.extend(self._scale_to_collateral())
        return steps

    def _profit_to_spot(self, realized: Decimal) -> list[StepResult]:
        steps: list[StepResult] = []
        if not self.settings.dry_run:
            time.sleep(max(int(self.settings.settle_seconds or 0), 2))
        collect = self._mutate("归集统一账户 USDT", lambda: self.futures.collect_to_margin("USDT"))
        steps.append(collect)
        if not self.settings.dry_run:
            time.sleep(1)
        try:
            transferable = self.futures.max_withdraw("USDT")
        except BinanceAPIError as exc:
            steps.append(StepResult("查询可转出", False, str(exc)))
            return steps
        if transferable < 1:
            steps.append(
                StepResult(
                    "转出现货",
                    True,
                    f"最大可转出 {fmt_amount(transferable)} USDT，不足 1，先补仓",
                )
            )
            return steps
        amount = transferable
        moved = self._mutate(
            f"最大可转出 {fmt_amount(amount)} USDT 转到现货（本次浮盈约 {fmt_amount(realized)}）",
            lambda: self.futures.unified_to_spot("USDT", amount),
        )
        if moved.ok and not moved.dry_run:
            self.pipeline.earn.invalidate()
        steps.append(moved)
        return steps

    def _spot_profit_to_earn(self) -> list[StepResult]:
        return self.pipeline.sweep_spot_to_earn()

    def _quote_until_balanced(self, qty: Decimal, reduce_only: bool) -> list[StepResult]:
        steps: list[StepResult] = []
        wait = max(1, self.settings.quote_refresh_seconds)
        for i in range(LIMIT_FILL_TRIES):
            legs = self.futures.legs(self.symbol)
            if legs.missing_side is None:
                steps.append(StepResult("对冲平衡", True, self._legs_text(legs)))
                return steps
            extra = self._hedge_pass(qty, reduce_only, market=False, aggressive=i > 0, flatten=False)
            steps.extend(extra)
            if (not reduce_only) and any(_no_margin(item) for item in extra):
                steps.append(StepResult("保证金不够", False, "另一边开不出，平掉已成交的单边"))
                steps.extend(self._flatten_if_naked())
                return steps
            time.sleep(wait)
            legs = self.futures.legs(self.symbol)
            if legs.missing_side is None:
                steps.append(StepResult("对冲平衡", True, self._legs_text(legs)))
                return steps
        steps.append(StepResult("挂单未齐", True, f"限价挂了 {LIMIT_FILL_TRIES} 次仍有缺口，改市价补另一边"))
        for _ in range(2):
            extra = self._hedge_pass(qty, reduce_only, market=True, aggressive=True, flatten=False)
            steps.extend(extra)
            if (not reduce_only) and any(_no_margin(item) for item in extra):
                steps.append(StepResult("保证金不够", False, "市价也开不出另一边，平掉已成交的单边"))
                steps.extend(self._flatten_if_naked())
                return steps
            legs = self.futures.legs(self.symbol)
            if legs.missing_side is None:
                steps.append(StepResult("对冲平衡", True, self._legs_text(legs)))
                return steps
            time.sleep(1)
        legs = self.futures.legs(self.symbol)
        if legs.long_qty > 0 and legs.short_qty > 0:
            steps.append(StepResult("数量仍不一致", True, "市价补腿后仍不等，平掉多出来的一边"))
            steps.extend(self._hedge_pass(qty, True, market=True, aggressive=True, flatten=True))
            legs = self.futures.legs(self.symbol)
            if legs.missing_side is None:
                steps.append(StepResult("对冲平衡", True, self._legs_text(legs)))
                return steps
        if (legs.long_qty > 0) != (legs.short_qty > 0):
            steps.append(StepResult("市价仍单边", False, "补不齐，平掉已开的一边，避免裸仓"))
            steps.extend(self._flatten_if_naked())
            return steps
        steps.append(StepResult("对冲未平衡", False, f"市价后仍不平衡：{self._legs_text(legs)}"))
        return steps

    def _flatten_if_naked(self) -> list[StepResult]:
        steps: list[StepResult] = []
        for _ in range(3):
            try:
                self.futures.cancel_open(self.symbol)
            except BinanceAPIError:
                pass
            legs = self.futures.legs(self.symbol)
            if legs.long_qty <= 0 and legs.short_qty <= 0:
                steps.append(StepResult("单边已平", True, self._legs_text(legs)))
                return steps
            if legs.missing_side is None:
                return steps
            bid, ask = self.futures.book(self.symbol)
            tick, step = self.futures.filters(self.symbol)
            if legs.long_qty > 0 and legs.short_qty <= 0:
                qty = self.futures.round_qty(legs.long_qty, step)
                px = self.futures.round_price(bid, tick)
                steps.append(self._place("SELL", "LONG", qty, px, True, market=True))
            elif legs.short_qty > 0 and legs.long_qty <= 0:
                qty = self.futures.round_qty(legs.short_qty, step)
                px = self.futures.round_price(ask, tick)
                steps.append(self._place("BUY", "SHORT", qty, px, True, market=True))
            else:
                return steps
            time.sleep(1)
        steps.append(StepResult("单边未平净", False, self._legs_text(self.futures.legs(self.symbol))))
        return steps

    def _hedge_pass(self, qty: Decimal, reduce_only: bool, *, market: bool, aggressive: bool, flatten: bool) -> list[StepResult]:
        steps: list[StepResult] = []
        try:
            self.futures.cancel_open(self.symbol)
        except BinanceAPIError:
            pass
        legs = self.futures.legs(self.symbol)
        if legs.missing_side is None:
            return steps
        bid, ask = self.futures.book(self.symbol)
        tick, step = self.futures.filters(self.symbol)
        buy_px, sell_px = self._maker_prices(bid, ask, tick, improve=aggressive)
        qty = self.futures.round_qty(qty, step)
        if legs.long_qty > 0 and legs.short_qty > 0:
            extra = self.futures.round_qty(abs(legs.long_qty - legs.short_qty), step)
            if extra <= 0:
                return steps
            if flatten or reduce_only:
                if legs.long_qty > legs.short_qty:
                    steps.append(self._place("SELL", "LONG", extra, sell_px, True, market=market))
                else:
                    steps.append(self._place("BUY", "SHORT", extra, buy_px, True, market=market))
                return steps
            if legs.long_qty > legs.short_qty:
                steps.append(self._place("SELL", "SHORT", extra, sell_px, False, market=market))
            else:
                steps.append(self._place("BUY", "LONG", extra, buy_px, False, market=market))
            return steps
        target = legs.long_qty if legs.long_qty > 0 else legs.short_qty if legs.short_qty > 0 else qty
        target = self.futures.round_qty(target, step)
        if target <= 0:
            return [StepResult("开对冲", False, "算出的下单数量为 0")]
        if legs.long_qty <= 0:
            steps.append(self._place("BUY", "LONG", target, buy_px, reduce_only, market=market))
        if legs.short_qty <= 0:
            steps.append(self._place("SELL", "SHORT", target, sell_px, reduce_only, market=market))
        return steps

    def _maker_prices(self, bid: Decimal, ask: Decimal, tick: Decimal, *, improve: bool = False) -> tuple[Decimal, Decimal]:
        """挂单价必须站在盘口内侧，GTX 才不会变成吃单。"""
        if bid <= 0 or ask <= 0 or ask <= bid:
            mid = self.futures.round_price(max(bid, ask), tick)
            return mid, mid
        mid = self.futures.round_price((bid + ask) / 2, tick)
        if improve:
            buy_px = self.futures.round_price(min(mid, ask - tick), tick)
            sell_px = self.futures.round_price(max(mid, bid + tick), tick)
        else:
            buy_px = self.futures.round_price(bid, tick)
            sell_px = self.futures.round_price(ask, tick)
        if buy_px >= ask:
            buy_px = self.futures.round_price(ask - tick, tick)
        if sell_px <= bid:
            sell_px = self.futures.round_price(bid + tick, tick)
        if buy_px <= 0:
            buy_px = self.futures.round_price(bid, tick)
        if sell_px <= 0:
            sell_px = self.futures.round_price(ask, tick)
        return buy_px, sell_px

    def _quote_side(self, order_side: str, position_side: str, qty: Decimal, reduce_only: bool) -> list[StepResult]:
        steps: list[StepResult] = []
        wait = max(1, self.settings.quote_refresh_seconds)
        done_name = f"已平 {position_side}" if reduce_only else f"已补 {position_side}"

        def finished(legs: HedgeLegs) -> bool:
            current = legs.long_qty if position_side == "LONG" else legs.short_qty
            return current <= 0 if reduce_only else current > 0

        for i in range(LIMIT_FILL_TRIES):
            legs = self.futures.legs(self.symbol)
            if finished(legs):
                steps.append(StepResult(done_name, True, self._legs_text(legs)))
                return steps
            steps.extend(
                self._side_pass(order_side, position_side, qty, reduce_only, market=False, aggressive=i > 0)
            )
            time.sleep(wait)
            legs = self.futures.legs(self.symbol)
            if finished(legs):
                steps.append(StepResult(done_name, True, self._legs_text(legs)))
                return steps
        steps.append(StepResult("挂单未齐", True, f"{position_side} 挂单 {LIMIT_FILL_TRIES} 次未完成，改市价"))
        for _ in range(2):
            steps.extend(self._side_pass(order_side, position_side, qty, reduce_only, market=True, aggressive=True))
            legs = self.futures.legs(self.symbol)
            if finished(legs):
                steps.append(StepResult(done_name, True, self._legs_text(legs)))
                return steps
            time.sleep(1)
        steps.append(StepResult(f"{position_side} 未完成", False, self._legs_text(legs)))
        return steps

    def _side_pass(
        self,
        order_side: str,
        position_side: str,
        qty: Decimal,
        reduce_only: bool,
        *,
        market: bool,
        aggressive: bool,
    ) -> list[StepResult]:
        try:
            self.futures.cancel_open(self.symbol)
        except BinanceAPIError:
            pass
        bid, ask = self.futures.book(self.symbol)
        tick, step = self.futures.filters(self.symbol)
        qty = self.futures.round_qty(qty, step)
        if qty <= 0:
            return [StepResult("下单跳过", False, "数量为 0")]
        buy_px, sell_px = self._maker_prices(bid, ask, tick, improve=aggressive)
        price = buy_px if order_side == "BUY" else sell_px
        return [self._place(order_side, position_side, qty, price, reduce_only, market=market)]

    def _place(
        self,
        side: str,
        position_side: str,
        qty: Decimal,
        price: Decimal,
        reduce_only: bool,
        market: bool = False,
    ) -> StepResult:
        action = "平仓" if reduce_only else "开仓"
        if qty <= 0:
            return StepResult(f"{action}跳过", False, "数量为 0")
        if market:
            return self._mutate(
                f"{action} 市价 {side} {position_side} {fmt_amount(qty)}",
                lambda: self.futures.place_market(self.symbol, side, position_side, qty, reduce_only),
            )
        return self._mutate(
            f"{action} 挂单 {side} {position_side} {fmt_amount(qty)} @ {fmt_amount(price)}",
            lambda: self.futures.place_maker(self.symbol, side, position_side, qty, price, reduce_only),
        )

    def _collateral_qty(self) -> Decimal:
        if self.settings.hedge_qty > 0:
            return self.settings.hedge_qty
        bid, ask = self.futures.book(self.symbol)
        mid = (bid + ask) / 2
        if mid <= 0:
            return Decimal("0")
        _, step = self.futures.filters(self.symbol)
        collateral = Decimal("0")
        if self.settings.unified_account:
            try:
                collateral = self.futures.refresh_account_risk().get("equity") or Decimal("0")
            except Exception:
                collateral = Decimal("0")
        if collateral < 1:
            try:
                collateral = self.pipeline.earn.earn_margin_usdt()
            except BinanceAPIError:
                collateral = self.pipeline.earn.spot_free("USDT")
        notional = collateral * margin_use_pct(self.settings) * Decimal(self.settings.hedge_leverage) / Decimal("2")
        return self.futures.round_qty(notional / mid, step)

    def _restore_qty(self, legs: HedgeLegs) -> Decimal:
        if legs.long_qty > 0 and legs.short_qty <= 0:
            return legs.long_qty
        if legs.short_qty > 0 and legs.long_qty <= 0:
            return legs.short_qty
        return self._collateral_qty()

    def _scale_to_collateral(self) -> list[StepResult]:
        if self.settings.hedge_qty > 0:
            return []
        if self.settings.dry_run:
            return []
        legs = self.futures.legs(self.symbol)
        if legs.missing_side is not None:
            return []
        current = min(legs.long_qty, legs.short_qty)
        if current <= 0:
            return []
        bid, ask = self.futures.book(self.symbol)
        mid = (bid + ask) / 2
        _, step = self.futures.filters(self.symbol)
        try:
            risk = self.futures.account_risk()
        except BinanceAPIError:
            risk = {"uni_mmr": Decimal("0"), "equity": Decimal("0"), "available": Decimal("0")}
        collateral = risk.get("equity") or Decimal("0")
        mmr = risk.get("uni_mmr") or Decimal("0")
        available = risk.get("available")
        if not is_mmr_sentinel(mmr) and mmr < self.settings.min_uni_mmr:
            return [
                StepResult(
                    "暂不加仓",
                    True,
                    f"uniMMR={fmt_amount(mmr)} 低于安全线 {fmt_amount(self.settings.min_uni_mmr)}，先不加",
                )
            ]
        plan = position_plan(
            self.settings,
            mid,
            collateral,
            legs,
            mmr,
            lambda q: self.futures.round_qty(q, step),
            available=available,
        )
        target = d(plan["target_qty"])
        add = self.futures.round_qty(target - current, step)
        min_add = max(step, current * self.settings.scale_min_add_pct)
        if add < min_add:
            mmr_text = plan.get("uni_mmr") or fmt_amount(mmr)
            return [
                StepResult(
                    "仓位已满",
                    True,
                    f"当前 {fmt_amount(current)}，目标 {fmt_amount(target)}，uniMMR={mmr_text}，已按安全上限开满",
                )
            ]
        mmr_text = plan.get("uni_mmr") or fmt_amount(mmr)
        steps = [
            StepResult(
                "随保证金加仓",
                True,
                f"uniMMR={mmr_text}，占用 {plan['use_pct']}、{self.settings.hedge_leverage}x，两边各加 {fmt_amount(add)}，目标 {fmt_amount(target)}",
            )
        ]
        steps.extend(self._add_both(add))
        return steps

    def _add_both(self, add: Decimal) -> list[StepResult]:
        start = self.futures.legs(self.symbol)
        goal_long = start.long_qty + add
        goal_short = start.short_qty + add
        steps: list[StepResult] = []
        wait = max(1, self.settings.quote_refresh_seconds)
        for i in range(LIMIT_FILL_TRIES):
            legs = self.futures.legs(self.symbol)
            if legs.long_qty >= goal_long and legs.short_qty >= goal_short:
                steps.append(StepResult("加仓完成", True, self._legs_text(legs)))
                return steps
            self.futures.cancel_open(self.symbol)
            bid, ask = self.futures.book(self.symbol)
            tick, step = self.futures.filters(self.symbol)
            buy_px, sell_px = self._maker_prices(bid, ask, tick, improve=i > 0)
            if legs.long_qty < goal_long:
                leftover = self.futures.round_qty(goal_long - legs.long_qty, step)
                if leftover > 0:
                    placed = self._place("BUY", "LONG", leftover, buy_px, False)
                    steps.append(placed)
                    if _no_margin(placed):
                        steps.append(StepResult("保证金不够", False, "另一边加不上，把多出来的一边平回平衡"))
                        steps.extend(self._hedge_pass(add, True, market=True, aggressive=True, flatten=True))
                        steps.extend(self._flatten_if_naked())
                        return steps
            if legs.short_qty < goal_short:
                leftover = self.futures.round_qty(goal_short - legs.short_qty, step)
                if leftover > 0:
                    placed = self._place("SELL", "SHORT", leftover, sell_px, False)
                    steps.append(placed)
                    if _no_margin(placed):
                        steps.append(StepResult("保证金不够", False, "另一边加不上，把多出来的一边平回平衡"))
                        steps.extend(self._hedge_pass(add, True, market=True, aggressive=True, flatten=True))
                        steps.extend(self._flatten_if_naked())
                        return steps
            time.sleep(wait)
        steps.append(StepResult("挂单未齐", True, f"加仓限价 {LIMIT_FILL_TRIES} 次未齐，改市价补"))
        bid, ask = self.futures.book(self.symbol)
        tick, step = self.futures.filters(self.symbol)
        legs = self.futures.legs(self.symbol)
        if legs.long_qty < goal_long:
            leftover = self.futures.round_qty(goal_long - legs.long_qty, step)
            if leftover > 0:
                placed = self._place("BUY", "LONG", leftover, ask, False, market=True)
                steps.append(placed)
                if _no_margin(placed):
                    steps.extend(self._hedge_pass(add, True, market=True, aggressive=True, flatten=True))
                    steps.extend(self._flatten_if_naked())
                    return steps
        if legs.short_qty < goal_short:
            leftover = self.futures.round_qty(goal_short - legs.short_qty, step)
            if leftover > 0:
                placed = self._place("SELL", "SHORT", leftover, bid, False, market=True)
                steps.append(placed)
                if _no_margin(placed):
                    steps.extend(self._hedge_pass(add, True, market=True, aggressive=True, flatten=True))
                    steps.extend(self._flatten_if_naked())
                    return steps
        time.sleep(1)
        legs = self.futures.legs(self.symbol)
        if legs.long_qty >= goal_long and legs.short_qty >= goal_short:
            steps.append(StepResult("加仓完成", True, self._legs_text(legs)))
            return steps
        if legs.long_qty > 0 and legs.short_qty > 0 and legs.long_qty != legs.short_qty:
            steps.extend(self._hedge_pass(add, True, market=True, aggressive=True, flatten=True))
            legs = self.futures.legs(self.symbol)
            if legs.missing_side is None:
                steps.append(StepResult("加仓已拉平", True, self._legs_text(legs)))
                return steps
        steps.extend(self._flatten_if_naked())
        steps.append(StepResult("加仓未完成", False, self._legs_text(self.futures.legs(self.symbol))))
        return steps

    def _legs_text(self, legs: HedgeLegs) -> str:
        return (
            f"{self.symbol} 多 {fmt_amount(legs.long_qty)} 盈 {fmt_amount(legs.long_pnl)} | "
            f"空 {fmt_amount(legs.short_qty)} 盈 {fmt_amount(legs.short_pnl)} | 缺口={legs.missing_side or '无'}"
        )
