from __future__ import annotations

import time
from decimal import ROUND_DOWN, Decimal

from .client import BinanceAPIError
from .config import d, fmt_amount, is_mmr_sentinel, settle_asset_of
from .futures import FuturesAPI, HedgeLegs
from .macd import fetch_macd_state
from .monitor import min_harvest_profit, price_stability, resolve_harvest_fee
from .pipeline import Pipeline, StepResult


LIMIT_FILL_TRIES = 3
FILL_POLL = 0.25


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


def _order_filled_hint(step: StepResult) -> bool:
    text = str(step.detail or "")
    if "已挂未成交" in text:
        return False
    return "FILLED" in text or "PARTIALLY_FILLED" in text or ("成交" in text and "未成交" not in text)


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

    一键收利（顺序固定，不可改成先补后转）：
    平掉浮盈腿（半平则继续平净，必要时市价扫尾）→ 最大可转出转到现货 → 补回对冲
    → 缺口/数量不等则继续补齐或削平，最终双腿等量（实在不行双平）
    →（USDC 则先换成 USDT）买理财 → 划入统一账户。
    平仓后可转额度本身已扣手续费等，约等于本次实现盈利。
    """

    def __init__(self, pipeline: Pipeline):
        self.pipeline = pipeline
        self.settings = pipeline.settings
        self.futures: FuturesAPI = pipeline.futures
        self.symbol = self.settings.hedge_symbol
        self.settle_asset = settle_asset_of(self.symbol)
        self.leverage_cap = self.settings.leverage_cap
        self.leverage_unlock_at = self.settings.leverage_unlock_at
        self.leverage_cap_changed = False
        self._enter_first_side: str | None = None  # LONG|SHORT，入场按 MACD 决定先挂哪边
        self._enter_order_note: str = ""

    def _limit_tries(self) -> int:
        return max(1, int(getattr(self.settings, "hedge_quote_retries", None) or LIMIT_FILL_TRIES))

    def _wait_until(self, pred, *, timeout: float, interval: float = FILL_POLL) -> bool:
        """短轮询直到条件成立，避免盲目 sleep。"""
        deadline = time.monotonic() + max(0.0, timeout)
        while True:
            if pred():
                return True
            if time.monotonic() >= deadline:
                return False
            time.sleep(interval)

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
            if any(not s.ok for s in steps):
                return steps
            steps.append(StepResult("暂缓开对冲", True, "按选择只处理理财"))
            return steps

        if hedged:
            # 已有对冲：仍归集其它活期/现货尘埃；现货闲置或非目标活期都要处理
            try:
                idle = self.pipeline.spot_cash()
            except Exception:
                idle = Decimal("0")
            try:
                target = self.pipeline.pick()
                other_earn = any(
                    not self.pipeline._same_product(p, target) and amt > 0
                    for p, amt in self.pipeline.earn_holdings()
                )
            except Exception:
                other_earn = False
            if idle >= Decimal("0.01") or other_earn:
                steps.extend(self._bootstrap_funds())
                if any(not s.ok for s in steps):
                    return steps
            steps.append(
                StepResult(
                    "已有对冲",
                    True,
                    "对冲已齐。要放大仓位请点「加仓」，这里不再自动加",
                )
            )
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
        # 先把其它活期（如 USDT 活期）赎成当前最高年化产品，再扫现货申购并入金
        legs = self.futures.legs(self.symbol)
        has_pos = legs.long_qty > 0 or legs.short_qty > 0
        steps = list(self.pipeline.switch_to_best(full=not has_pos, has_hedge=has_pos))
        if any(not s.ok for s in steps):
            return steps
        sweep = self.pipeline.sweep_spot_to_earn()
        steps.extend(sweep)
        if any(not s.ok for s in sweep):
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

    def harvest_once(self, *, wait_stable: bool = True, side: str | None = None) -> list[StepResult]:
        legs = self.futures.legs(self.symbol)
        if legs.missing_side is not None:
            return [StepResult("平仓跳过", False, f"对冲不平衡，先补仓：{self._legs_text(legs)}")]
        want = (side or "").upper()
        if want in {"LONG", "SHORT"}:
            winner = want
            note = f"指定平 {winner}"
        else:
            winner = "LONG" if legs.long_pnl >= legs.short_pnl else "SHORT"
            note = f"按浮盈选 {winner}"
        pnl = legs.long_pnl if winner == "LONG" else legs.short_pnl
        if pnl <= 0:
            return [StepResult("平仓跳过", False, f"{note}：该腿没有浮盈（{fmt_amount(pnl, 4)}）")]
        return self._harvest(winner, legs, wait_stable=wait_stable)

    def _wait_until_stable(self, purpose: str, *, timeout: float | None = None) -> list[StepResult]:
        """尽可能等到价格平稳再下单；超时不阻断，继续用挂单路径。"""
        if self.settings.dry_run:
            return [StepResult("等待平稳", True, f"dry-run 跳过（{purpose}）", dry_run=True)]
        limit = timeout
        if limit is None:
            limit = float(getattr(self.settings, "harvest_stable_wait_seconds", 90) or 90)
        limit = max(0.0, float(limit))
        if limit <= 0:
            return []
        try:
            first = price_stability(self.futures, self.settings, purpose="harvest")
            if first.stable:
                return [StepResult("价格已平稳", True, f"{purpose}：{first.hint}")]
        except Exception as exc:
            return [StepResult("平稳检测", True, f"{purpose}：检测失败仍继续（{exc}）")]

        deadline = time.monotonic() + limit
        steps = [
            StepResult(
                "等待平稳",
                True,
                f"{purpose}：当前不稳，最多等 {int(limit)}s 再操作",
            )
        ]
        while time.monotonic() < deadline:
            time.sleep(2.0)
            try:
                st = price_stability(self.futures, self.settings, purpose="harvest")
            except Exception:
                continue
            if st.stable:
                steps.append(StepResult("价格已平稳", True, f"{purpose}：{st.hint}"))
                return steps
        steps.append(
            StepResult(
                "等待平稳超时",
                True,
                f"{purpose}：已等 {int(limit)}s 仍不够稳，继续挂单（尽量少吃单）",
            )
        )
        return steps

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
        # 以交易所回报 / 再读一遍仓位杠杆为准，算仓不按目标虚高
        try:
            live = int(self.futures.current_leverage(self.symbol) or 0)
        except Exception:
            live = 0
        if live > 0:
            applied = live
        self.settings.hedge_leverage = applied
        self.leverage_cap = applied
        self.leverage_unlock_at = data.get("unlock_at")
        self.leverage_cap_changed = True
        return [StepResult("设置杠杆", True, data.get("note") or f"已设置 {applied}x")]

    def _use_actual_leverage(self, legs: HedgeLegs | None = None) -> int:
        """算仓只用交易所实际杠杆；目标倍数只用于尝试上调。"""
        actual = int(legs.leverage) if legs and legs.leverage > 0 else 0
        if actual <= 0:
            try:
                actual = int(self.futures.current_leverage(self.symbol) or 0)
            except Exception:
                actual = 0
        if actual > 0:
            self.settings.hedge_leverage = actual
        return int(self.settings.hedge_leverage)

    def _prepare_account(self) -> list[StepResult]:
        steps = [self._mutate("开启双向持仓", self.futures.enable_hedge_mode)]
        steps.extend(self.apply_leverage())
        applied = self._use_actual_leverage()
        target = int(self.settings.hedge_leverage_target or applied)
        if applied < target:
            steps.append(
                StepResult(
                    "杠杆",
                    True,
                    f"实际 {applied}x（目标 {target}x），按实际倍数算仓。解禁后再点「设置杠杆」上调",
                )
            )
        return steps

    def _open_maker_room(self) -> tuple[int, float]:
        """入场、加仓、补仓：先挂够久，尽量 maker；不成交再市价。"""
        tries = max(12, self._limit_tries() + 4)
        wait = max(1.0, min(1.5, float(self.settings.quote_refresh_seconds or 1) * 0.75))
        return tries, wait

    def _close_maker_room(self) -> tuple[int, float]:
        """平仓：只挂 GTX，追价窗口更长，绝不市价。"""
        tries = max(20, self._limit_tries() * 3)
        wait = 0.7
        return tries, wait

    def _refill_maker_room(self) -> tuple[int, float]:
        """收利补仓：比普通开仓再多挂一会儿，减少吃单。"""
        tries = max(16, self._limit_tries() + 8)
        wait = max(1.0, min(1.6, float(self.settings.quote_refresh_seconds or 1) * 0.8))
        return tries, wait

    def _resolve_enter_leg_order(self) -> list[StepResult]:
        """按 MACD 涨跌决定入场先挂哪边：上涨先多，下跌先空。"""
        url = getattr(self.settings, "macd_indicator_url", None) or ""
        state = fetch_macd_state(url)
        if state is None:
            self._enter_first_side = "LONG"
            self._enter_order_note = "MACD 不可用，默认先挂多再挂空"
        elif state.bias == "bear":
            self._enter_first_side = "SHORT"
            self._enter_order_note = f"MACD 下跌@{state.time}，先挂空再挂多"
        else:
            self._enter_first_side = "LONG"
            self._enter_order_note = f"MACD 上涨@{state.time}，先挂多再挂空"
        return [StepResult("入场顺序", True, self._enter_order_note)]

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
                    f"两边先挂单（GTX，首挂买一/卖一外约 {getattr(self.settings, 'maker_passive_bps', 2)}bp/"
                    f"{getattr(self.settings, 'maker_passive_ticks', 2)} 档），按 MACD 涨跌决定先多/先空；"
                    f"单边后最多等 {getattr(self.settings, 'hedge_max_wait_seconds', 2.0)}s，"
                    f"再 maker 追价 {getattr(self.settings, 'hedge_second_leg_chases', 2)} 次，仍不成或滑点 "
                    f"{getattr(self.settings, 'hedge_max_slippage_bps', 3.5)}bp 才市价补齐  数量={fmt_amount(qty)} {self.symbol}",
                    dry_run=True,
                )
            ]
        legs = self.futures.legs(self.symbol)
        if legs.missing_side is None:
            return [StepResult("对冲已平衡", True, self._legs_text(legs))]
        qty = self._restore_qty(legs)
        if qty <= 0:
            qty = self._collateral_qty()
        if qty <= 0:
            return [StepResult("开对冲", False, "算出的下单数量为 0，检查理财仓位或 hedge_qty")]
        steps: list[StepResult] = []
        # 双边都还没仓时，按 MACD 定先后
        if legs.long_qty <= 0 and legs.short_qty <= 0:
            steps.extend(self._resolve_enter_leg_order())
        steps.extend(self._quote_until_balanced(qty, reduce_only=False))
        return steps

    def scale_once(self, force: bool = False) -> list[StepResult]:
        legs = self.futures.legs(self.symbol)
        if legs.missing_side is not None or legs.long_qty <= 0 or legs.short_qty <= 0:
            return [StepResult("加仓跳过", False, f"对冲不齐，先入场补仓：{self._legs_text(legs)}")]
        if not force:
            try:
                stability = price_stability(self.futures, self.settings)
            except Exception:
                stability = None
            if stability is not None and not stability.stable:
                return [
                    StepResult(
                        "暂不加仓",
                        True,
                        "价格不稳。要加仓请选「强行加仓」",
                    )
                ]
        return self._scale_to_collateral()

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

    def _harvest(self, side: str, legs: HedgeLegs, *, wait_stable: bool = True) -> list[StepResult]:
        qty = legs.long_qty if side == "LONG" else legs.short_qty
        pnl = legs.long_pnl if side == "LONG" else legs.short_pnl
        steps = [
            StepResult(
                "触发止盈",
                True,
                f"平掉 {side} 浮盈约 {fmt_amount(pnl)} {self.settle_asset}，"
                f"半平则继续平净，再转出补仓；尽量等平稳后挂单，最终保持对冲平衡",
            )
        ]
        if wait_stable:
            steps.extend(self._wait_until_stable("平仓前"))
        steps.extend(self._close_winner_for_harvest(side, qty, wait_stable=wait_stable))
        after = self.futures.legs(self.symbol)
        still = after.long_qty if side == "LONG" else after.short_qty
        # 盈利腿必须归零才转出；否则先把仓位拉回平衡再停
        if still > 0:
            steps.append(
                StepResult(
                    "平仓未净",
                    False,
                    f"{side} 仍剩 {fmt_amount(still)}，无法安全转出，先恢复对冲平衡",
                )
            )
            steps.extend(self._restore_harvest_balance(wait_stable=wait_stable))
            return steps
        if after.missing_side is None:
            steps.append(StepResult("平仓异常", False, "平仓后两侧仍在，没有实现盈利可转出"))
            steps.extend(self._restore_harvest_balance(wait_stable=wait_stable))
            return steps

        # 顺序固定：先转出再补仓
        steps.extend(self._profit_to_spot(pnl))
        refill_qty = after.short_qty if side == "LONG" else after.long_qty
        refill_pos = "LONG" if side == "LONG" else "SHORT"
        refill_order = "BUY" if refill_pos == "LONG" else "SELL"
        if refill_qty > 0:
            if wait_stable:
                steps.extend(self._wait_until_stable("补仓前"))
            steps.extend(self._quote_side(refill_order, refill_pos, refill_qty, reduce_only=False))
        # 补不齐或数量不一致：再补一轮 / 削平多余，最终必须平衡或双平
        steps.extend(self._restore_harvest_balance(wait_stable=wait_stable))
        restored = self.futures.legs(self.symbol)

        earn_steps = self._spot_profit_to_earn()
        steps.extend(earn_steps)
        if not self.settings.dry_run:
            time.sleep(max(int(self.settings.settle_seconds or 0), 2))
        steps.extend(self._fund_after_earn(earn_steps, check_idle_ldusdt=False))
        if restored.missing_side is None:
            steps.append(StepResult("收利完成", True, "对冲已齐。要放大仓位请点「加仓」"))
        elif restored.long_qty <= 0 and restored.short_qty <= 0:
            steps.append(StepResult("收利完成", True, "仓位已清零（未能补回对冲）。要重开请点「一键入场」"))
        else:
            steps.append(
                StepResult(
                    "收利未齐",
                    False,
                    f"结束后仍不平衡：{self._legs_text(restored)}",
                )
            )
        return steps

    def _close_winner_for_harvest(
        self, side: str, qty: Decimal, *, wait_stable: bool = True
    ) -> list[StepResult]:
        """平盈利腿：挂单为主；半平则继续平剩余；仍不净则等平稳后市价扫尾。"""
        steps: list[StepResult] = []
        close_side = "SELL" if side == "LONG" else "BUY"
        remain = qty
        for i in range(5):
            legs = self.futures.legs(self.symbol)
            remain = legs.long_qty if side == "LONG" else legs.short_qty
            if remain <= 0:
                steps.append(StepResult(f"已平 {side}", True, self._legs_text(legs)))
                return steps
            if i > 0:
                if wait_stable:
                    steps.extend(self._wait_until_stable("继续平剩余前", timeout=45))
                steps.append(
                    StepResult("继续平剩余", True, f"{side} 还剩 {fmt_amount(remain)}，继续挂单平净")
                )
            steps.extend(self._quote_side(close_side, side, remain, reduce_only=True))
            legs = self.futures.legs(self.symbol)
            remain = legs.long_qty if side == "LONG" else legs.short_qty
            if remain <= 0:
                steps.append(StepResult(f"已平 {side}", True, self._legs_text(legs)))
                return steps

        # 挂单仍不净：尽量等平稳再市价扫尾
        legs = self.futures.legs(self.symbol)
        remain = legs.long_qty if side == "LONG" else legs.short_qty
        if remain <= 0:
            return steps
        if wait_stable:
            steps.extend(self._wait_until_stable("市价扫尾前", timeout=60))
        steps.append(
            StepResult(
                "半平市价扫尾",
                True,
                f"{side} 挂单后仍剩 {fmt_amount(remain)}，市价平净以便补仓平衡",
            )
        )
        for _ in range(3):
            legs = self.futures.legs(self.symbol)
            remain = legs.long_qty if side == "LONG" else legs.short_qty
            if remain <= 0:
                break
            steps.extend(
                self._side_pass(close_side, side, remain, True, market=True, aggressive=True)
            )
            if self._wait_until(
                lambda: (
                    self.futures.legs(self.symbol).long_qty
                    if side == "LONG"
                    else self.futures.legs(self.symbol).short_qty
                )
                <= 0,
                timeout=1.0,
            ):
                break
        legs = self.futures.legs(self.symbol)
        remain = legs.long_qty if side == "LONG" else legs.short_qty
        if remain <= 0:
            steps.append(StepResult(f"已平 {side}", True, self._legs_text(legs)))
        else:
            steps.append(
                StepResult(
                    f"{side} 未完成",
                    False,
                    f"市价扫尾后仍剩 {fmt_amount(remain)}：{self._legs_text(legs)}",
                )
            )
        return steps

    def _restore_harvest_balance(self, *, wait_stable: bool = True) -> list[StepResult]:
        """收利后强制回到双腿等量，或双平。半边/数量不一致都处理。"""
        steps: list[StepResult] = []
        for round_i in range(3):
            legs = self.futures.legs(self.symbol)
            miss = legs.missing_side
            if miss is None:
                if round_i > 0:
                    steps.append(StepResult("对冲已齐", True, self._legs_text(legs)))
                return steps
            if legs.long_qty <= 0 and legs.short_qty <= 0:
                steps.append(StepResult("仓位已清", True, self._legs_text(legs)))
                return steps
            if miss in {"LONG", "SHORT"}:
                target = legs.short_qty if miss == "LONG" else legs.long_qty
                order = "BUY" if miss == "LONG" else "SELL"
                if target > 0:
                    if wait_stable:
                        steps.extend(self._wait_until_stable(f"补齐 {miss} 前", timeout=60))
                    steps.append(
                        StepResult(
                            "补齐缺口",
                            True,
                            f"缺 {miss}，按 {fmt_amount(target)} 补仓（第 {round_i + 1} 轮）",
                        )
                    )
                    steps.extend(self._quote_side(order, miss, target, reduce_only=False))
                continue
            if miss == "IMBALANCE":
                if wait_stable:
                    steps.extend(self._wait_until_stable("数量拉平前", timeout=45))
                steps.append(
                    StepResult(
                        "数量拉平",
                        True,
                        f"多空不等，削平多余：{self._legs_text(legs)}",
                    )
                )
                target = max(legs.long_qty, legs.short_qty)
                steps.extend(
                    self._hedge_pass(target, True, market=True, aggressive=True, flatten=True)
                )
                continue
            if miss == "BOTH":
                return steps
        legs = self.futures.legs(self.symbol)
        if legs.missing_side is None:
            steps.append(StepResult("对冲已齐", True, self._legs_text(legs)))
            return steps
        if (legs.long_qty > 0) != (legs.short_qty > 0):
            steps.append(StepResult("补仓未完成", False, "仍缺一边，平掉单边避免裸仓"))
            steps.extend(self._flatten_if_naked())
        elif legs.missing_side == "IMBALANCE":
            steps.append(StepResult("数量仍不一致", False, self._legs_text(legs)))
            steps.extend(
                self._hedge_pass(
                    max(legs.long_qty, legs.short_qty), True, market=True, aggressive=True, flatten=True
                )
            )
        return steps

    def _profit_to_spot(self, realized: Decimal) -> list[StepResult]:
        steps: list[StepResult] = []
        asset = self.settle_asset
        # 顺序保持：先转出再补仓。用短轮询等可转出，尽快进入补仓。
        collect = self._mutate(f"归集统一账户 {asset}", lambda: self.futures.collect_to_margin(asset))
        steps.append(collect)
        transferable = Decimal("0")
        last_err: BinanceAPIError | None = None

        def ready() -> bool:
            nonlocal transferable, last_err
            try:
                transferable = self.futures.max_withdraw(asset)
                last_err = None
                return transferable >= 1
            except BinanceAPIError as exc:
                last_err = exc
                return False

        if not self.settings.dry_run:
            if not ready():
                self._wait_until(ready, timeout=2.0, interval=0.3)
        else:
            ready()
        if last_err is not None and transferable < 1:
            steps.append(StepResult("查询可转出", False, str(last_err)))
            return steps
        if transferable < 1:
            steps.append(
                StepResult(
                    "转出现货",
                    True,
                    f"最大可转出 {fmt_amount(transferable)} {asset}，不足 1，先补仓",
                )
            )
            return steps
        amount = transferable
        moved = self._mutate(
            f"最大可转出 {fmt_amount(amount)} {asset} 转到现货（本次浮盈约 {fmt_amount(realized)}）",
            lambda: self.futures.unified_to_spot(asset, amount),
        )
        if moved.ok and not moved.dry_run:
            self.pipeline.earn.invalidate()
        steps.append(moved)
        return steps

    def _spot_profit_to_earn(self) -> list[StepResult]:
        return self.pipeline.sweep_spot_to_earn()

    def _quote_until_balanced(self, qty: Decimal, reduce_only: bool) -> list[StepResult]:
        """双边挂单直到对冲齐。

        双腿都未成：GTX 首挂防守距 + 追价；任一腿 -5022 则撤双侧、向外改价重挂。
        单边已成（第二腿）：先等 hedge_max_wait_seconds → maker 追价数次 → 再市价。
        """
        steps: list[StepResult] = []
        tries, wait = self._open_maker_room()
        naked_since: float | None = None
        anchor = Decimal("0")  # 第一腿成交均价

        for i in range(tries):
            legs = self.futures.legs(self.symbol)
            if legs.missing_side is None:
                steps.append(StepResult("对冲平衡", True, self._legs_text(legs)))
                return steps

            naked = (legs.long_qty > 0) != (legs.short_qty > 0)
            if naked:
                if naked_since is None:
                    naked_since = time.monotonic()
                    anchor = legs.long_entry if legs.long_qty > 0 else legs.short_entry
                if not reduce_only:
                    reason = self._second_leg_bailout_reason(legs, naked_since, anchor)
                    if reason:
                        steps.append(StepResult("单边兜底", True, reason))
                        steps.extend(self._second_leg_soft_fill(qty, reduce_only, reason))
                        return self._finish_hedge_or_flatten(steps, reduce_only)
            else:
                naked_since = None
                anchor = Decimal("0")

            # 双边未成时前几轮保持偏防守，少贴盘口；单边补缺才积极追价
            dual_open = legs.long_qty <= 0 and legs.short_qty <= 0
            aggressive = naked or (i > 2 and not dual_open) or (i > 4)
            extra = self._hedge_pass(
                qty,
                reduce_only,
                market=False,
                aggressive=aggressive,
                flatten=False,
                pass_n=i + (2 if naked else 0),
            )
            steps.extend(extra)
            if (not reduce_only) and any(_no_margin(item) for item in extra):
                steps.append(StepResult("保证金不够", False, "另一边开不出，平掉已成交的单边"))
                steps.extend(self._flatten_if_naked())
                return steps

            legs = self.futures.legs(self.symbol)
            if legs.missing_side is None:
                steps.append(StepResult("对冲平衡", True, self._legs_text(legs)))
                return steps
            # 本轮挂单后可能刚变成单边：立刻记锚点
            naked = (legs.long_qty > 0) != (legs.short_qty > 0)
            if naked and naked_since is None:
                naked_since = time.monotonic()
                anchor = legs.long_entry if legs.long_qty > 0 else legs.short_entry

            # 等待期间轮询：平衡 / 时间 / 滑点
            round_wait = wait if not naked else min(wait, self._hedge_max_wait_seconds())
            done, bail = self._wait_second_leg(
                timeout=round_wait,
                reduce_only=reduce_only,
                naked_since=naked_since,
                anchor=anchor,
            )
            if done:
                steps.append(StepResult("对冲平衡", True, self._legs_text(self.futures.legs(self.symbol))))
                return steps
            if bail:
                steps.append(StepResult("单边兜底", True, bail))
                steps.extend(self._second_leg_soft_fill(qty, reduce_only, bail))
                return self._finish_hedge_or_flatten(steps, reduce_only)

            # 刷新锚点（均价可能在部成后变化）
            legs = self.futures.legs(self.symbol)
            if (legs.long_qty > 0) != (legs.short_qty > 0):
                if naked_since is None:
                    naked_since = time.monotonic()
                anchor = legs.long_entry if legs.long_qty > 0 else legs.short_entry
            else:
                naked_since = None
                anchor = Decimal("0")

        steps.append(StepResult("挂单未齐", True, f"限价挂了 {tries} 次仍有缺口，改市价补另一边"))
        steps.extend(self._market_fill_second_leg(qty, reduce_only))
        return self._finish_hedge_or_flatten(steps, reduce_only)

    def _hedge_max_wait_seconds(self) -> float:
        return max(0.05, float(getattr(self.settings, "hedge_max_wait_seconds", 2.0) or 2.0))

    def _hedge_second_leg_chases(self) -> int:
        return max(0, int(getattr(self.settings, "hedge_second_leg_chases", 2) or 0))

    def _hedge_max_slippage_bps(self) -> Decimal:
        raw = d(getattr(self.settings, "hedge_max_slippage_bps", None) or "3.5")
        return raw if raw > 0 else Decimal("3.5")

    def _second_leg_bailout_reason(
        self,
        legs: HedgeLegs,
        naked_since: float | None,
        anchor: Decimal,
    ) -> str | None:
        """单边已成时：超时或 BBO 相对第一腿不利偏离过大 → 返回原因文案。"""
        if naked_since is not None:
            elapsed = time.monotonic() - naked_since
            limit = self._hedge_max_wait_seconds()
            if elapsed >= limit:
                return (
                    f"第二腿超时 {elapsed:.2f}s≥{limit:.2f}s，先 maker 追价再市价"
                    f"（{'补空' if legs.long_qty > 0 else '补多'}）"
                )
        try:
            bid, ask = self.futures.book(self.symbol)
        except BinanceAPIError:
            return None
        ref = anchor
        if ref <= 0:
            ref = legs.long_entry if legs.long_qty > 0 else legs.short_entry
        if ref <= 0:
            return None
        max_bps = self._hedge_max_slippage_bps()
        # 缺多：成本=卖一；缺空：成本=买一。不利偏离相对第一腿成交价。
        if legs.long_qty <= 0 and legs.short_qty > 0:
            cost = ask
            slip = (cost - ref) / ref * Decimal("10000")
            side = "补多"
        elif legs.short_qty <= 0 and legs.long_qty > 0:
            cost = bid
            slip = (ref - cost) / ref * Decimal("10000")
            side = "补空"
        else:
            return None
        if slip >= max_bps:
            return (
                f"第二腿价格逃逸 {fmt_amount(slip, 2)}bp≥{fmt_amount(max_bps, 2)}bp，"
                f"锚={fmt_amount(ref)} 现成本={fmt_amount(cost)}，{side}先追价再市价"
            )
        return None

    def _second_leg_soft_fill(self, qty: Decimal, reduce_only: bool, reason: str) -> list[StepResult]:
        """超时/逃逸：先 GTX 追价数次，仍不成再市价（数量按当前缺口）。"""
        steps: list[StepResult] = []
        chases = 0 if reduce_only else self._hedge_second_leg_chases()
        for n in range(chases):
            legs = self.futures.legs(self.symbol)
            if legs.missing_side is None:
                steps.append(StepResult("对冲平衡", True, self._legs_text(legs)))
                return steps
            steps.append(
                StepResult(
                    "第二腿追价",
                    True,
                    f"市价前 maker 追价 {n + 1}/{chases}（{reason.split('，')[0]}）",
                )
            )
            self._cancel_open_clean()
            need = self._restore_qty(legs)
            if need <= 0:
                need = qty
            steps.extend(
                self._hedge_pass(
                    need,
                    reduce_only,
                    market=False,
                    aggressive=True,
                    flatten=False,
                    pass_n=n + 3,
                )
            )
            done, _ = self._wait_second_leg(
                timeout=0.7,
                reduce_only=reduce_only,
                naked_since=None,
                anchor=Decimal("0"),
            )
            if done:
                steps.append(StepResult("对冲平衡", True, self._legs_text(self.futures.legs(self.symbol))))
                return steps
        steps.extend(self._market_fill_second_leg(qty, reduce_only))
        return steps

    def _wait_second_leg(
        self,
        *,
        timeout: float,
        reduce_only: bool,
        naked_since: float | None,
        anchor: Decimal,
    ) -> tuple[bool, str | None]:
        """等待第二腿：返回 (已平衡, 兜底原因)。"""
        deadline = time.monotonic() + max(0.0, timeout)
        while True:
            legs = self.futures.legs(self.symbol)
            if legs.missing_side is None:
                return True, None
            naked = (legs.long_qty > 0) != (legs.short_qty > 0)
            if naked and not reduce_only:
                since = naked_since if naked_since is not None else time.monotonic()
                ref = anchor if anchor > 0 else (
                    legs.long_entry if legs.long_qty > 0 else legs.short_entry
                )
                reason = self._second_leg_bailout_reason(legs, since, ref)
                if reason:
                    return False, reason
            if time.monotonic() >= deadline:
                return False, None
            time.sleep(FILL_POLL)

    def _cancel_open_clean(self) -> None:
        """撤净本币对挂单，短等确认，减少叠挂。"""
        for _ in range(3):
            try:
                self.futures.cancel_open(self.symbol)
            except BinanceAPIError:
                pass
            try:
                opens = self.futures.um_open_orders(self.symbol)
            except BinanceAPIError:
                return
            if not opens:
                return
            time.sleep(0.08)

    def _market_fill_second_leg(self, qty: Decimal, reduce_only: bool) -> list[StepResult]:
        """撤掉挂单 → 确认仓位 → 市价补缺口（最多 2 次，防重复超开）。"""
        steps: list[StepResult] = []
        self._cancel_open_clean()
        # 给交易所一点时间消化撤单/部成，避免挂单与市价叠仓
        self._wait_until(
            lambda: self.futures.legs(self.symbol).missing_side is None,
            timeout=0.25,
        )
        for attempt in range(2):
            legs = self.futures.legs(self.symbol)
            if legs.missing_side is None:
                steps.append(StepResult("对冲平衡", True, self._legs_text(legs)))
                return steps
            # 按当前缺口下市价，不用陈旧 qty，降低超开风险
            need = self._restore_qty(legs)
            if need <= 0:
                need = qty
            extra = self._hedge_pass(
                need, reduce_only, market=True, aggressive=True, flatten=False, pass_n=attempt
            )
            steps.extend(extra)
            if any(_no_margin(item) for item in extra):
                steps.append(StepResult("保证金不够", False, "市价也开不出另一边，平掉已成交的单边"))
                steps.extend(self._flatten_if_naked())
                return steps
            if self._wait_until(
                lambda: self.futures.legs(self.symbol).missing_side is None,
                timeout=0.8,
            ):
                steps.append(StepResult("对冲平衡", True, self._legs_text(self.futures.legs(self.symbol))))
                return steps
        return steps

    def _finish_hedge_or_flatten(self, steps: list[StepResult], reduce_only: bool) -> list[StepResult]:
        """市价补腿后的收尾：齐了就返回；仍单边则平掉；数量不等则削平。"""
        if any((not s.ok) and "保证金不够" in s.name for s in steps):
            return steps
        legs = self.futures.legs(self.symbol)
        if legs.missing_side is None:
            if not any(s.name == "对冲平衡" for s in steps[-3:]):
                steps.append(StepResult("对冲平衡", True, self._legs_text(legs)))
            return steps
        if legs.long_qty > 0 and legs.short_qty > 0:
            steps.append(StepResult("数量仍不一致", True, "市价补腿后仍不等，平掉多出来的一边"))
            steps.extend(self._hedge_pass(Decimal("0"), True, market=True, aggressive=True, flatten=True))
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
            flat = self._wait_until(
                lambda: (
                    (lambda legs: legs.long_qty <= 0 and legs.short_qty <= 0)(self.futures.legs(self.symbol))
                ),
                timeout=0.8,
            )
            if flat:
                steps.append(StepResult("单边已平", True, self._legs_text(self.futures.legs(self.symbol))))
                return steps
        steps.append(StepResult("单边未平净", False, self._legs_text(self.futures.legs(self.symbol))))
        return steps

    def _hedge_pass(
        self,
        qty: Decimal,
        reduce_only: bool,
        *,
        market: bool,
        aggressive: bool,
        flatten: bool,
        _retried: bool = False,
        pass_n: int = 0,
        retreat_n: int = 0,
    ) -> list[StepResult]:
        steps: list[StepResult] = []
        legs = self.futures.legs(self.symbol)
        if legs.missing_side is None:
            return steps
        bid, ask = self.futures.book(self.symbol)
        tick, step = self.futures.filters(self.symbol)
        naked = (legs.long_qty > 0) != (legs.short_qty > 0)
        dual_open = legs.long_qty <= 0 and legs.short_qty <= 0
        # -5022 后向外退：不往盘口内侧追
        if retreat_n > 0:
            buy_px, sell_px = self._maker_prices(
                bid, ask, tick, improve=False, pass_n=0, retreat_n=retreat_n
            )
        else:
            buy_px, sell_px = self._maker_prices(
                bid,
                ask,
                tick,
                improve=(aggressive or naked or pass_n > 0) and not dual_open,
                pass_n=max(int(pass_n), 2 if naked else 0),
                retreat_n=0,
            )
            # 双边齐开时即使追价也保持至少 2 档间距，避免 84129.2/84129.3 贴死
            if dual_open and sell_px - buy_px < tick * 2:
                mid = (bid + ask) / 2
                buy_px = self.futures.round_price(mid - tick, tick)
                sell_px = self.futures.round_price(mid + tick, tick)
                if buy_px >= bid:
                    buy_px = self.futures.round_price(bid - tick, tick)
                if sell_px <= ask:
                    sell_px = self.futures.round_price(ask + tick, tick)
        qty = self.futures.round_qty(qty, step)

        # 单边补缺：目标价未变则保留挂单排队，避免每轮撤挂掉队
        if (not market) and naked and not reduce_only and retreat_n <= 0:
            if legs.long_qty <= 0:
                kept = self._keep_resting_maker("BUY", "LONG", buy_px, bid, ask, tick)
                if kept is not None:
                    return [kept]
            if legs.short_qty <= 0:
                kept = self._keep_resting_maker("SELL", "SHORT", sell_px, bid, ask, tick)
                if kept is not None:
                    return [kept]

        self._cancel_open_clean()
        legs = self.futures.legs(self.symbol)
        if legs.missing_side is None:
            return steps
        if legs.long_qty > 0 and legs.short_qty > 0:
            extra = self.futures.round_qty(abs(legs.long_qty - legs.short_qty), step)
            if extra <= 0:
                return steps
            if flatten or reduce_only:
                if legs.long_qty > legs.short_qty:
                    steps.append(self._place("SELL", "LONG", extra, sell_px, True, market=market))
                else:
                    steps.append(self._place("BUY", "SHORT", extra, buy_px, True, market=market))
            elif legs.long_qty > legs.short_qty:
                steps.append(self._place("SELL", "SHORT", extra, sell_px, False, market=market))
            else:
                steps.append(self._place("BUY", "LONG", extra, buy_px, False, market=market))
        else:
            target = legs.long_qty if legs.long_qty > 0 else legs.short_qty if legs.short_qty > 0 else qty
            target = self.futures.round_qty(target, step)
            if target <= 0:
                return [StepResult("开对冲", False, "算出的下单数量为 0")]
            to_place: list[tuple[str, str, Decimal]] = []
            if legs.long_qty <= 0:
                to_place.append(("BUY", "LONG", buy_px))
            if legs.short_qty <= 0:
                to_place.append(("SELL", "SHORT", sell_px))
            # 双边齐开：按 MACD 涨跌决定先后（下跌先空，上涨先多）
            if dual_open and len(to_place) == 2 and not reduce_only:
                first = (self._enter_first_side or "LONG").upper()
                to_place.sort(key=lambda item: 0 if item[1] == first else 1)
                if retreat_n <= 0 and pass_n <= 0 and not market:
                    # 先挂优先腿，稍等再挂另一边，提高趋势腿先挂上的概率
                    side0, pos0, px0 = to_place[0]
                    steps.append(self._place(side0, pos0, target, px0, reduce_only, market=market))
                    time.sleep(0.25)
                    legs = self.futures.legs(self.symbol)
                    if legs.missing_side is None:
                        return steps
                    # 另一边仍缺才挂
                    side1, pos1, px1 = to_place[1]
                    need_other = (pos1 == "LONG" and legs.long_qty <= 0) or (
                        pos1 == "SHORT" and legs.short_qty <= 0
                    )
                    if need_other:
                        steps.append(self._place(side1, pos1, target, px1, reduce_only, market=market))
                    to_place = []
            for side, pos, px in to_place:
                steps.append(self._place(side, pos, target, px, reduce_only, market=market))

        # -5022：立刻撤双侧（含已挂成的另一腿），按新盘口向外改价重挂，禁止同价重试
        if (not market) and any(_post_only_reject(item) for item in steps):
            rejected = [s for s in steps if _post_only_reject(s)]
            detail = rejected[0].detail if rejected else ""
            next_retreat = retreat_n + 1
            if next_retreat <= 3:
                steps.append(
                    StepResult(
                        "PostOnly 改价",
                        True,
                        f"-5022 撤双侧，买下调/卖上调再挂（第 {next_retreat} 次）：{detail}",
                    )
                )
                self._cancel_open_clean()
                steps.extend(
                    self._hedge_pass(
                        qty,
                        reduce_only,
                        market=False,
                        aggressive=False,
                        flatten=flatten,
                        _retried=True,
                        pass_n=0,
                        retreat_n=next_retreat,
                    )
                )
        return steps

    def _maker_prices(
        self,
        bid: Decimal,
        ask: Decimal,
        tick: Decimal,
        *,
        improve: bool = False,
        pass_n: int = 0,
        retreat_n: int = 0,
    ) -> tuple[Decimal, Decimal]:
        """
        GTX 挂单价：买必须 < 卖一，卖必须 > 买一。

        首挂（improve=False, pass_n=0）：相对买一/卖一往外让 maker_passive_bps，
        且至少离盘口 maker_passive_ticks 档；买更低、卖更高。

        retreat_n>0（-5022 后）：在首挂基础上再往外退 n 档，禁止原价重发。

        追价：再按 maker_improve_bps 往盘口内侧靠（仍留 1 档），轮次越高越近对手价。
        """
        if tick <= 0:
            tick = Decimal("0.01")
        if bid <= 0 or ask <= 0 or ask <= bid:
            mid = self.futures.round_price(max(bid, ask), tick)
            return mid, mid

        bid = self.futures.round_price(bid, tick)
        ask = self.futures.round_price(ask, tick)
        if ask <= bid:
            ask = self.futures.round_price(bid + tick, tick)

        mid = (bid + ask) / 2
        min_ticks = max(1, int(getattr(self.settings, "maker_passive_ticks", 2) or 2))
        min_ticks += max(0, int(retreat_n))

        # 首挂 / -5022 外退：买更低、卖更高
        if (not improve and pass_n <= 0) or retreat_n > 0:
            passive = d(getattr(self.settings, "maker_passive_bps", None) or "0")
            if passive < 0:
                passive = Decimal("0")
            # 外退时再多让约 0.5bp * retreat
            if retreat_n > 0:
                passive = passive + (Decimal("0.5") * Decimal(retreat_n))
            factor = passive / Decimal("10000")
            raw_buy = bid * (Decimal("1") - factor) if factor > 0 else bid - tick * min_ticks
            raw_sell = ask * (Decimal("1") + factor) if factor > 0 else ask + tick * min_ticks
            buy_px = self.futures.round_price(raw_buy, tick)
            sell_px = self.futures.round_price(raw_sell, tick)
            # 至少离盘口 min_ticks 档
            buy_max = self.futures.round_price(bid - tick * min_ticks, tick)
            sell_min = self.futures.round_price(ask + tick * min_ticks, tick)
            if buy_px > buy_max:
                buy_px = buy_max
            if sell_px < sell_min:
                sell_px = sell_min
            if buy_px <= 0:
                buy_px = buy_max if buy_max > 0 else bid
            if sell_px <= 0:
                sell_px = sell_min if sell_min > 0 else ask
            return buy_px, sell_px

        spread_ticks = int(((ask - bid) / tick).to_integral_value(rounding=ROUND_DOWN))
        room = max(0, spread_ticks - 1)

        bps = d(getattr(self.settings, "maker_improve_bps", None) or "1")
        if bps < 0:
            bps = Decimal("0")
        # 相对现价最多让利 bps；换算成档位，各币 tick/价格不同也能对齐
        max_by_bps = int(((mid * bps) / Decimal("10000") / tick).to_integral_value(rounding=ROUND_DOWN))
        max_by_spread = max(0, (spread_ticks * 2) // 5)
        max_improve = min(max(0, max_by_bps), max_by_spread, room)

        if max_improve <= 0:
            n = 0
        else:
            # 分约 4 步从轻到重靠拢，第一轮至少 1 档（有空间时）
            step = max(1, (max_improve + 3) // 4)
            n = min(max_improve, room, step * (max(0, int(pass_n)) + 1))
            if improve and n < 1 and room >= 1:
                n = 1

        buy_px = self.futures.round_price(bid + (tick * n), tick)
        sell_px = self.futures.round_price(ask - (tick * n), tick)

        buy_ceil = self.futures.round_price(ask - tick, tick)
        sell_floor = self.futures.round_price(bid + tick, tick)
        if buy_px > buy_ceil:
            buy_px = buy_ceil
        if sell_px < sell_floor:
            sell_px = sell_floor
        if buy_px <= 0 or buy_px >= ask:
            buy_px = buy_ceil if buy_ceil > 0 else bid
        if sell_px <= 0 or sell_px <= bid:
            sell_px = sell_floor if sell_floor > 0 else ask
        # 双边价至少隔 2 档，避免贴死互相 -5022
        if sell_px - buy_px < tick * 2:
            buy_px = self.futures.round_price(mid - tick, tick)
            sell_px = self.futures.round_price(mid + tick, tick)
            if buy_px >= ask:
                buy_px = buy_ceil
            if sell_px <= bid:
                sell_px = sell_floor
        return buy_px, sell_px

    def _quote_side(self, order_side: str, position_side: str, qty: Decimal, reduce_only: bool) -> list[StepResult]:
        """
        平仓(reduce_only)：只挂 GTX，绝不市价；长窗口追价直到成交或超时失败。
        补仓：更长挂单追价，仍不成交再市价，避免长时间单边。
        """
        steps: list[StepResult] = []
        done_name = f"已平 {position_side}" if reduce_only else f"已补 {position_side}"

        def finished(legs: HedgeLegs) -> bool:
            current = legs.long_qty if position_side == "LONG" else legs.short_qty
            return current <= 0 if reduce_only else current > 0

        if reduce_only:
            tries, wait = self._close_maker_room()
            allow_market = False
        else:
            tries, wait = self._refill_maker_room()
            allow_market = True

        for i in range(tries):
            legs = self.futures.legs(self.symbol)
            if finished(legs):
                steps.append(StepResult(done_name, True, self._legs_text(legs)))
                return steps
            # 始终往盘口内侧松一点挂；轮次越高越靠近对手价，仍保持 GTX
            extra = self._side_pass(
                order_side, position_side, qty, reduce_only, market=False, aggressive=True, pass_n=i
            )
            steps.extend(extra)
            if any(_order_filled_hint(item) for item in extra):
                legs = self.futures.legs(self.symbol)
                if finished(legs):
                    steps.append(StepResult(done_name, True, self._legs_text(legs)))
                    return steps
            if self._wait_until(lambda: finished(self.futures.legs(self.symbol)), timeout=wait):
                steps.append(StepResult(done_name, True, self._legs_text(self.futures.legs(self.symbol))))
                return steps

        if not allow_market:
            try:
                self.futures.cancel_open(self.symbol)
            except BinanceAPIError:
                pass
            steps.append(
                StepResult(
                    f"{position_side} 未完成",
                    False,
                    f"平仓只允许挂单，已追价 {tries} 次仍未成交，已撤单。请等盘口更稳后再收。"
                    f" {self._legs_text(self.futures.legs(self.symbol))}",
                )
            )
            return steps

        steps.append(StepResult("挂单未齐", True, f"{position_side} 已挂单 {tries} 次未完成，改市价补仓（尽量少用）"))
        for _ in range(2):
            steps.extend(self._side_pass(order_side, position_side, qty, reduce_only, market=True, aggressive=True))
            if self._wait_until(lambda: finished(self.futures.legs(self.symbol)), timeout=0.8):
                steps.append(StepResult(done_name, True, self._legs_text(self.futures.legs(self.symbol))))
                return steps
        steps.append(StepResult(f"{position_side} 未完成", False, self._legs_text(self.futures.legs(self.symbol))))
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
        pass_n: int = 0,
    ) -> list[StepResult]:
        bid, ask = self.futures.book(self.symbol)
        tick, step = self.futures.filters(self.symbol)
        qty = self.futures.round_qty(qty, step)
        if qty <= 0:
            return [StepResult("下单跳过", False, "数量为 0")]
        buy_px, sell_px = self._maker_prices(
            bid, ask, tick, improve=aggressive or pass_n > 0, pass_n=pass_n
        )
        price = buy_px if order_side == "BUY" else sell_px
        # 价差只有 1 档、无需让利时：用 QUEUE 贴买一/卖一（Binance 官方 BBO），心理价位更稳
        use_queue = (not market) and buy_px == bid and sell_px == ask and (
            (order_side == "BUY" and price == bid) or (order_side == "SELL" and price == ask)
        )

        if not market:
            kept = self._keep_resting_maker(order_side, position_side, price, bid, ask, tick)
            if kept is not None:
                return [kept]

        self._cancel_open_clean()

        placed = self._place(
            order_side,
            position_side,
            qty,
            price,
            reduce_only,
            market=market,
            price_match="QUEUE" if use_queue else None,
        )
        # GTX 被盘口吃掉（-5022）：向外退档再挂，禁止同价重试
        if (not market) and _post_only_reject(placed):
            self._cancel_open_clean()
            bid, ask = self.futures.book(self.symbol)
            buy_px, sell_px = self._maker_prices(
                bid, ask, tick, improve=False, pass_n=0, retreat_n=max(pass_n, 1)
            )
            price = buy_px if order_side == "BUY" else sell_px
            retry = self._place(order_side, position_side, qty, price, reduce_only, market=False)
            out = [placed, StepResult("PostOnly 改价", True, f"-5022 后改挂 @ {fmt_amount(price)}"), retry]
            if _post_only_reject(retry):
                self._cancel_open_clean()
                bid, ask = self.futures.book(self.symbol)
                buy_px, sell_px = self._maker_prices(
                    bid, ask, tick, improve=False, pass_n=0, retreat_n=max(pass_n, 1) + 1
                )
                price = buy_px if order_side == "BUY" else sell_px
                out.append(self._place(order_side, position_side, qty, price, reduce_only, market=False))
            return out
        return [placed]

    def _keep_resting_maker(
        self,
        order_side: str,
        position_side: str,
        target: Decimal,
        bid: Decimal,
        ask: Decimal,
        tick: Decimal,
    ) -> StepResult | None:
        """盘口目标价未变时保留挂单，保住排队位置（反复撤挂会掉队）。"""
        try:
            opens = self.futures.um_open_orders(self.symbol)
        except BinanceAPIError:
            return None
        for row in opens:
            if str(row.get("side") or "").upper() != order_side:
                continue
            if str(row.get("positionSide") or "").upper() != position_side:
                continue
            status = str(row.get("status") or "").upper()
            if status and status not in {"NEW", "PARTIALLY_FILLED"}:
                continue
            opx = d(row.get("price"))
            if opx <= 0:
                continue
            # 仍必须是 maker：买价 < 卖一，卖价 > 买一
            if order_side == "BUY" and opx >= ask:
                continue
            if order_side == "SELL" and opx <= bid:
                continue
            # 与本轮心理目标价相差不超过 1 档，继续排队
            if abs(opx - target) <= tick:
                return StepResult(
                    "挂单排队",
                    True,
                    f"已挂未成交 {row.get('executedQty') or 0}/{row.get('origQty') or '?'} @ {fmt_amount(opx)} GTX 保留排队",
                )
        return None

    def _place(
        self,
        side: str,
        position_side: str,
        qty: Decimal,
        price: Decimal,
        reduce_only: bool,
        market: bool = False,
        price_match: str | None = None,
    ) -> StepResult:
        action = "平仓" if reduce_only else "开仓"
        if qty <= 0:
            return StepResult(f"{action}跳过", False, "数量为 0")
        if market:
            step = self._mutate(
                f"{action} 市价 {side} {position_side} {fmt_amount(qty)}",
                lambda: self.futures.place_market(self.symbol, side, position_side, qty, reduce_only),
            )
        elif price_match:
            step = self._mutate(
                f"{action} 挂单 {side} {position_side} {fmt_amount(qty)} @ {price_match}",
                lambda: self.futures.place_maker(
                    self.symbol, side, position_side, qty, None, reduce_only, price_match=price_match
                ),
            )
        else:
            step = self._mutate(
                f"{action} 挂单 {side} {position_side} {fmt_amount(qty)} @ {fmt_amount(price)}",
                lambda: self.futures.place_maker(self.symbol, side, position_side, qty, price, reduce_only),
            )
        if step.ok and isinstance(step.detail, dict):
            step.detail = self._order_summary(step.detail)
        return step

    @staticmethod
    def _order_summary(data: dict) -> str:
        status = str(data.get("status") or "")
        filled = str(data.get("executedQty") or data.get("cumQty") or "0")
        orig = str(data.get("origQty") or "")
        avg = d(data.get("avgPrice"))
        px = str(data.get("avgPrice") or data.get("price") or "")
        otype = str(data.get("type") or "").upper()
        tif = str(data.get("timeInForce") or "")
        if otype == "MARKET" or (not tif and avg > 0 and d(filled) > 0):
            return f"{status or 'FILLED'} 市价成交 {filled}/{orig or filled} @ {px or fmt_amount(avg)}"
        if status in {"NEW", "NEW_INSURANCE", "NEW_ADL"} and d(filled) <= 0:
            return f"已挂未成交 {filled}/{orig} @ {px} {tif or 'GTX'}"
        if status in {"FILLED", "PARTIALLY_FILLED"} or d(filled) > 0:
            return f"{status} 成交 {filled}/{orig} @ {px} {tif}"
        return f"{status} {filled}/{orig} @ {px} {tif}"

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
        prep = self.apply_leverage()
        if any(not step.ok for step in prep):
            return prep
        legs = self.futures.legs(self.symbol)
        self._use_actual_leverage(legs)
        if legs.missing_side is not None:
            return prep
        current = min(legs.long_qty, legs.short_qty)
        if current <= 0:
            return prep
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
            return prep + [
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
            return prep + [
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
        return prep + steps

    def _add_both(self, add: Decimal) -> list[StepResult]:
        start = self.futures.legs(self.symbol)
        goal_long = start.long_qty + add
        goal_short = start.short_qty + add
        steps: list[StepResult] = []
        tries, wait = self._open_maker_room()

        def goals_met(legs: HedgeLegs) -> bool:
            return legs.long_qty >= goal_long and legs.short_qty >= goal_short

        for i in range(tries):
            legs = self.futures.legs(self.symbol)
            if goals_met(legs):
                steps.append(StepResult("加仓完成", True, self._legs_text(legs)))
                return steps
            self.futures.cancel_open(self.symbol)
            bid, ask = self.futures.book(self.symbol)
            tick, step = self.futures.filters(self.symbol)
            buy_px, sell_px = self._maker_prices(bid, ask, tick, improve=True, pass_n=i)
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
            if self._wait_until(lambda: goals_met(self.futures.legs(self.symbol)), timeout=wait):
                steps.append(StepResult("加仓完成", True, self._legs_text(self.futures.legs(self.symbol))))
                return steps
        steps.append(StepResult("挂单未齐", True, f"加仓限价 {tries} 次未齐，改市价补"))
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
        self._wait_until(lambda: goals_met(self.futures.legs(self.symbol)), timeout=0.8)
        legs = self.futures.legs(self.symbol)
        if goals_met(legs):
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
