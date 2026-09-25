#!/usr/bin/env python3
"""MACD 挂单开仓/收利参数网格测试（独立脚本，不改正式流程）。

目标：
  - 一律等 MACD 金叉/死叉；用交叉前柱 |hist|（敞口）过滤波动大小。
  - 开仓：固定 0.001 BTC；贴盘 GTX 追价，尽量挂单成交；
    单边超过约 4s 或不利价差过大才市价补齐（不裸仓）；
    对冲齐后继续测收利，记录开仓价差作对比，不因价差整组重开。
  - 收利：止盈网格约 0.05/0.08/0.12；交叉且敞口够才收；先挂单追价。
  - 流程：每组参数 → 开仓 → 收利 → 全平 → 下一组。

固定数量：BTC 0.001（可用 --qty 改）。

用法：
  nohup .venv/bin/python scripts/macd_abtest.py --account-no 4 --live --confirm \\
      >> logs/macd_abtest.log 2>&1 &
  .venv/bin/python scripts/macd_abtest.py --summary
  # 汇总会写 data/macd_abtest_scorecard.json + .csv，按权益差/价差/市价次数排优势
"""

from __future__ import annotations

import argparse
import itertools
import json
import logging
import sys
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import ROUND_DOWN, Decimal
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from licai.client import BinanceAPIError  # noqa: E402
from licai.config import ROOT as PKG_ROOT, d, fmt_amount, load_settings  # noqa: E402
from licai.cycle import HedgeCycle, _no_margin, _order_filled_hint  # noqa: E402
from licai.futures import HedgeLegs  # noqa: E402
from licai.macd import fetch_macd_cross, fetch_macd_state  # noqa: E402
from licai.monitor import resolve_harvest_fee  # noqa: E402
from licai.ops import OpsService, pipeline_for  # noqa: E402
from licai.pipeline import StepResult  # noqa: E402
from licai.store import Account, AccountStore  # noqa: E402

log = logging.getLogger("macd_abtest")
LOG_PATH = (PKG_ROOT / "data") / "macd_abtest.jsonl"
SCORECARD_PATH = (PKG_ROOT / "data") / "macd_abtest_scorecard.json"
FIXED_QTY = Decimal("0.001")
# 开仓：尽量 GTX 追到齐；仅单边拖太久或不利价差过大才市价补（保证对冲）
ENTER_BAIL_SPREAD_BPS = Decimal("8")
ENTER_BAIL_NAKED_SECONDS = 4.0
# 单边补腿时挂单追价间隔（秒）；越小越勤改价贴盘
ENTER_CHASE_INTERVAL = 0.12

# 开仓参数网格（开仓价差写入日志对比；单边过大时用市价兜底，不整组重开）
ENTER_PASSIVE_BPS = (Decimal("0.5"), Decimal("1"))  # 首挂外让 bp（越小越贴盘）

# 收利止盈网格
HARVEST_TP_USDT = (Decimal("0.05"), Decimal("0.08"), Decimal("0.12"))
# 交叉前柱 |hist| 最小敞口：0=任意交叉；越大只吃大波动交叉（图上柱高约到 50）
MACD_MIN_GAP = (Decimal("0"), Decimal("10"), Decimal("25"))


@dataclass(frozen=True)
class TrialParams:
    passive_bps: Decimal
    tp_usdt: Decimal
    min_gap: Decimal  # 交叉时要求的最小敞口（|hist| 前柱）

    @property
    def name(self) -> str:
        return f"pas{self.passive_bps}_tp{self.tp_usdt}_gap{self.min_gap}"


def build_trials() -> list[TrialParams]:
    out: list[TrialParams] = []
    for pas, tp, gap in itertools.product(ENTER_PASSIVE_BPS, HARVEST_TP_USDT, MACD_MIN_GAP):
        out.append(TrialParams(pas, tp, gap))
    return out


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _print(msg: str) -> None:
    print(msg, flush=True)
    log.info(msg)


def resolve_account_by_no(store: AccountStore, account_no: int) -> Account:
    accounts = store.list_accounts()
    if account_no < 1 or account_no > len(accounts):
        raise SystemExit(f"账号编号 #{account_no} 不存在（当前共 {len(accounts)} 个）")
    return accounts[account_no - 1]


def append_log(row: dict[str, Any], path: Path | None = None) -> None:
    target = path or LOG_PATH
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")


def entry_spread_bps(legs: HedgeLegs) -> Decimal | None:
    """正值 = short_entry > long_entry，开仓吃到正价差；负值 = 不利敞口。"""
    if legs.long_qty <= 0 or legs.short_qty <= 0:
        return None
    if legs.long_entry <= 0 or legs.short_entry <= 0:
        return None
    mid = (legs.long_entry + legs.short_entry) / 2
    if mid <= 0:
        return None
    return (legs.short_entry - legs.long_entry) / mid * Decimal("10000")


def _legs_snap(legs: HedgeLegs) -> dict[str, str]:
    return {
        "long": str(legs.long_qty),
        "short": str(legs.short_qty),
        "long_entry": str(legs.long_entry),
        "short_entry": str(legs.short_entry),
        "long_pnl": str(legs.long_pnl),
        "short_pnl": str(legs.short_pnl),
        "missing": str(legs.missing_side or ""),
    }


def _read_equity(cycle: AbTestCycle) -> Decimal:
    try:
        return d((cycle.futures.account_risk() or {}).get("equity") or "0")
    except Exception:
        return Decimal("0")


class AbTestCycle(HedgeCycle):
    """测试专用：紧对冲开仓 / 挂单收利 / 全平；不碰理财划转。"""

    def __init__(self, pipeline, *, passive_bps: Decimal = Decimal("2")):
        super().__init__(pipeline)
        self.ab_passive_bps = passive_bps
        self.ab_bail_spread_bps = ENTER_BAIL_SPREAD_BPS
        self.ab_bail_naked_s = ENTER_BAIL_NAKED_SECONDS
        self.settings = replace(
            self.settings,
            maker_only=False,  # 允许市价兜底；正常路径仍先 GTX
            maker_passive_bps=passive_bps,
            maker_passive_ticks=0,  # 允许贴买一/卖一排队（见 _maker_prices 覆盖）
            maker_improve_bps=Decimal("2"),
            quote_refresh_seconds=0.2,
            hedge_max_wait_seconds=ENTER_BAIL_NAKED_SECONDS,
            hedge_max_slippage_bps=ENTER_BAIL_SPREAD_BPS,
        )

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
        """测试专用：默认贴买一/卖一排队；-5022 才外退；追价尽量贴盘。"""
        if tick <= 0:
            tick = Decimal("0.01")
        if bid <= 0 or ask <= 0 or ask <= bid:
            mid = self.futures.round_price(max(bid, ask), tick)
            return mid, mid
        bid = self.futures.round_price(bid, tick)
        ask = self.futures.round_price(ask, tick)
        if ask <= bid:
            ask = self.futures.round_price(bid + tick, tick)

        # -5022 外退：买更低、卖更高
        if retreat_n > 0:
            n = max(1, int(retreat_n))
            buy_px = self.futures.round_price(bid - tick * n, tick)
            sell_px = self.futures.round_price(ask + tick * n, tick)
            return buy_px, sell_px

        # 首挂：按 passive_bps 略外让；0.5bp 以下直接贴买一/卖一
        if not improve and pass_n <= 0:
            passive = d(self.ab_passive_bps)
            if passive <= 0:
                return bid, ask
            factor = passive / Decimal("10000")
            buy_px = self.futures.round_price(bid * (Decimal("1") - factor), tick)
            sell_px = self.futures.round_price(ask * (Decimal("1") + factor), tick)
            if buy_px >= bid:
                buy_px = bid
            if sell_px <= ask:
                sell_px = ask
            # 仍须严格 maker：买 < 卖一、卖 > 买一
            if buy_px >= ask:
                buy_px = self.futures.round_price(ask - tick, tick)
            if sell_px <= bid:
                sell_px = self.futures.round_price(bid + tick, tick)
            return buy_px, sell_px

        # 追价：尽快贴到买一/卖一（仍留 1 档不穿盘）
        spread_ticks = int(((ask - bid) / tick).to_integral_value(rounding=ROUND_DOWN))
        room = max(0, spread_ticks - 1)
        # pass_n 越大越贴；单边补腿直接贴盘
        if pass_n >= 2 or improve:
            buy_px = bid
            sell_px = ask
        else:
            n = min(room, max(0, int(pass_n) + 1))
            buy_px = self.futures.round_price(bid + tick * n, tick)
            sell_px = self.futures.round_price(ask - tick * n, tick)
        buy_ceil = self.futures.round_price(ask - tick, tick)
        sell_floor = self.futures.round_price(bid + tick, tick)
        if buy_px > buy_ceil:
            buy_px = buy_ceil if buy_ceil > 0 else bid
        if sell_px < sell_floor:
            sell_px = sell_floor if sell_floor > 0 else ask
        if buy_px <= 0:
            buy_px = bid
        if sell_px <= 0:
            sell_px = ask
        return buy_px, sell_px

    def _keep_resting_maker(self, *args, **kwargs):  # noqa: ANN002, ANN003
        """测试追价：不保留远处旧单，每轮按新盘口改挂。"""
        return None

    def ensure_flat(self) -> list[StepResult]:
        """撤挂 + 双边市价扫平（收尾用；测试组之间必须空仓）。"""
        steps: list[StepResult] = []
        try:
            self.futures.cancel_open(self.symbol)
        except BinanceAPIError:
            pass
        for _ in range(5):
            legs = self.futures.legs(self.symbol)
            if legs.long_qty <= 0 and legs.short_qty <= 0:
                steps.append(StepResult("已全平", True, self._legs_text(legs)))
                return steps
            bid, ask = self.futures.book(self.symbol, force=True)
            tick, step = self.futures.filters(self.symbol)
            if legs.long_qty > 0:
                q = self.futures.round_qty(legs.long_qty, step)
                if q > 0:
                    steps.append(self._place("SELL", "LONG", q, bid, True, market=True))
            if legs.short_qty > 0:
                q = self.futures.round_qty(legs.short_qty, step)
                if q > 0:
                    steps.append(self._place("BUY", "SHORT", q, ask, True, market=True))
            self._wait_until(
                lambda: (
                    self.futures.legs(self.symbol).long_qty <= 0
                    and self.futures.legs(self.symbol).short_qty <= 0
                ),
                timeout=1.0,
                interval=0.15,
            )
        legs = self.futures.legs(self.symbol)
        ok = legs.long_qty <= 0 and legs.short_qty <= 0
        steps.append(StepResult("全平", ok, self._legs_text(legs)))
        return steps

    def enter_tight(self, qty: Decimal = FIXED_QTY) -> list[StepResult]:
        """开仓：金叉先多/死叉先空；双边 GTX 追到齐；价差只记录，不因价差全平。"""
        steps: list[StepResult] = []
        steps.extend(self._prepare_account())
        if any(not s.ok for s in steps):
            return steps
        legs = self.futures.legs(self.symbol)
        if legs.long_qty > 0 or legs.short_qty > 0:
            steps.append(StepResult("开仓前不净", False, f"先全平再开：{self._legs_text(legs)}"))
            steps.extend(self.ensure_flat())
            legs = self.futures.legs(self.symbol)
            if legs.long_qty > 0 or legs.short_qty > 0:
                return steps

        steps.extend(self._ab_resolve_enter_order())
        _, step = self.futures.filters(self.symbol)
        qty = self.futures.round_qty(qty if qty > 0 else FIXED_QTY, step)
        if qty <= 0:
            steps.append(StepResult("开仓", False, "数量无效"))
            return steps

        steps.append(
            StepResult(
                "紧对冲开仓",
                True,
                f"qty={fmt_amount(qty)} 首挂外让 {self.ab_passive_bps}bp；"
                f"GTX 优先，单边>{self.ab_bail_naked_s}s 或不利>{self.ab_bail_spread_bps}bp 市价补齐",
            )
        )
        steps.extend(self._ab_quote_tight(qty))
        legs = self.futures.legs(self.symbol)
        if legs.missing_side is None and legs.long_qty > 0 and legs.short_qty > 0:
            spr = entry_spread_bps(legs)
            if spr is None:
                steps.append(StepResult("开仓齐", True, self._legs_text(legs)))
            else:
                hint = "正价差" if spr >= 0 else "不利价差"
                steps.append(
                    StepResult(
                        "开仓齐",
                        True,
                        f"{hint} {fmt_amount(spr, 2)}bp "
                        f"L@{fmt_amount(legs.long_entry)} S@{fmt_amount(legs.short_entry)}",
                    )
                )
            return steps
        # 仍未齐：最后市价兜底一次
        steps.append(StepResult("开仓市价兜底", True, "挂单未齐，市价补缺口"))
        steps.extend(self._market_fill_second_leg(qty, False))
        legs = self.futures.legs(self.symbol)
        ok = legs.missing_side is None and legs.long_qty > 0 and legs.short_qty > 0
        if not ok and (legs.long_qty > 0 or legs.short_qty > 0):
            steps.extend(self.ensure_flat())
        steps.append(StepResult("开仓结果", ok, self._legs_text(self.futures.legs(self.symbol))))
        return steps

    def harvest_tight(self, side: str) -> list[StepResult]:
        """平盈利腿 + 补齐对冲；先挂单，不成市价兜底；不理财。"""
        side = side.upper()
        legs = self.futures.legs(self.symbol)
        qty = legs.long_qty if side == "LONG" else legs.short_qty
        pnl = legs.long_pnl if side == "LONG" else legs.short_pnl
        steps: list[StepResult] = [
            StepResult("测试收利", True, f"平 {side} 浮盈≈{fmt_amount(pnl)}，再补齐对冲")
        ]
        steps.extend(self._ab_close_winner(side, qty))
        after = self.futures.legs(self.symbol)
        still = after.long_qty if side == "LONG" else after.short_qty
        if still > 0:
            # 挂单+侧内市价仍剩：再强制市价扫尾
            steps.append(StepResult("平仓扫尾", True, f"{side} 仍剩 {fmt_amount(still)}，市价扫净"))
            close_side = "SELL" if side == "LONG" else "BUY"
            for _ in range(3):
                legs = self.futures.legs(self.symbol)
                remain = legs.long_qty if side == "LONG" else legs.short_qty
                if remain <= 0:
                    break
                steps.extend(
                    self._side_pass(close_side, side, remain, True, market=True, aggressive=True)
                )
                self._wait_until(
                    lambda: (
                        self.futures.legs(self.symbol).long_qty
                        if side == "LONG"
                        else self.futures.legs(self.symbol).short_qty
                    )
                    <= 0,
                    timeout=0.8,
                    interval=0.1,
                )
            after = self.futures.legs(self.symbol)
            still = after.long_qty if side == "LONG" else after.short_qty
            if still > 0:
                steps.append(StepResult("平仓未净", False, f"{side} 市价后仍剩 {fmt_amount(still)}"))
                return steps
        refill_qty = after.short_qty if side == "LONG" else after.long_qty
        refill_pos = "LONG" if side == "LONG" else "SHORT"
        refill_order = "BUY" if refill_pos == "LONG" else "SELL"
        if refill_qty > 0:
            steps.extend(self._ab_quote_side(refill_order, refill_pos, refill_qty, reduce_only=False))
        restored = self.futures.legs(self.symbol)
        if restored.missing_side is None and restored.long_qty > 0:
            steps.append(StepResult("收利完成", True, self._legs_text(restored)))
        else:
            steps.append(StepResult("收利未齐", False, self._legs_text(restored)))
        return steps

    def _ab_resolve_enter_order(self) -> list[StepResult]:
        url = getattr(self.settings, "macd_indicator_url", None) or ""
        state = fetch_macd_state(url)
        if state is None:
            self._enter_first_side = "LONG"
            note = "MACD 不可用，默认先多后空"
        elif state.cross == "golden":
            self._enter_first_side = "LONG"
            note = f"金叉@{state.time}，先多后空"
        elif state.cross == "death":
            self._enter_first_side = "SHORT"
            note = f"死叉@{state.time}，先空后多"
        elif state.bias == "bear":
            self._enter_first_side = "SHORT"
            note = f"下跌@{state.time}，先空后多"
        else:
            self._enter_first_side = "LONG"
            note = f"上涨@{state.time}，先多后空"
        self._enter_order_note = note
        return [StepResult("入场顺序", True, note)]

    def _ab_quote_tight(self, qty: Decimal) -> list[StepResult]:
        """双边：先 GTX 贴盘追价；仅单边过久/价差过大才市价补齐。"""
        steps: list[StepResult] = []
        # 4s 裸仓窗口 / 0.12s 一轮 ≈ 30+ 轮追价
        tries = max(50, int(self.ab_bail_naked_s / ENTER_CHASE_INTERVAL) + 20)
        wait_dual = 0.25
        wait_naked = ENTER_CHASE_INTERVAL
        naked_since: float | None = None
        for i in range(tries):
            legs = self.futures.legs(self.symbol)
            if legs.missing_side is None and legs.long_qty > 0 and legs.short_qty > 0:
                steps.append(StepResult("对冲平衡", True, self._legs_text(legs)))
                return steps

            naked = (legs.long_qty > 0) != (legs.short_qty > 0)
            if naked:
                if naked_since is None:
                    naked_since = time.monotonic()
                elapsed = time.monotonic() - naked_since
                adverse = self._live_adverse_bps(legs)
                bail = elapsed >= self.ab_bail_naked_s or (
                    adverse is not None and adverse >= self.ab_bail_spread_bps
                )
                if bail:
                    why = (
                        f"单边 {elapsed:.2f}s"
                        if elapsed >= self.ab_bail_naked_s
                        else f"不利约 {fmt_amount(adverse or 0, 2)}bp"
                    )
                    steps.append(StepResult("市价补第二腿", True, f"{why}，市价兜底保证对冲"))
                    steps.extend(self._market_fill_second_leg(qty, False))
                    legs = self.futures.legs(self.symbol)
                    if legs.missing_side is None and legs.long_qty > 0:
                        steps.append(StepResult("对冲平衡", True, self._legs_text(legs)))
                        return steps
                    steps.append(StepResult("市价后仍缺口", False, self._legs_text(legs)))
                    return steps
            else:
                naked_since = None

            # 单边：每轮强制贴盘追（aggressive + 高 pass_n）；双边：前几轮略防守
            aggressive = naked or i >= 1
            pass_n = (i + 4) if naked else i
            extra = self._hedge_pass(
                qty,
                False,
                market=False,
                aggressive=aggressive,
                flatten=False,
                pass_n=pass_n,
            )
            steps.extend(extra)
            if any(_no_margin(item) for item in extra):
                steps.append(StepResult("保证金不够", False, "开不出"))
                return steps

            if self._wait_until(
                lambda: (
                    self.futures.legs(self.symbol).missing_side is None
                    and self.futures.legs(self.symbol).long_qty > 0
                ),
                timeout=wait_naked if naked else wait_dual,
                interval=0.06 if naked else 0.12,
            ):
                legs = self.futures.legs(self.symbol)
                if legs.missing_side is None:
                    steps.append(StepResult("对冲平衡", True, self._legs_text(legs)))
                    return steps

        steps.append(StepResult("挂单超时市价", True, f"GTX 追了 {tries} 轮未齐，市价补齐"))
        steps.extend(self._market_fill_second_leg(qty, False))
        legs = self.futures.legs(self.symbol)
        if legs.missing_side is None:
            steps.append(StepResult("对冲平衡", True, self._legs_text(legs)))
        return steps

    def _live_adverse_bps(self, legs: HedgeLegs) -> Decimal | None:
        """相对已成交腿，用当前 BBO 估补另一边的不利 bp。"""
        try:
            bid, ask = self.futures.book(self.symbol, force=True)
        except Exception:
            return None
        if legs.long_qty > 0 and legs.short_qty <= 0 and legs.long_entry > 0:
            return (legs.long_entry - bid) / legs.long_entry * Decimal("10000")
        if legs.short_qty > 0 and legs.long_qty <= 0 and legs.short_entry > 0:
            return (ask - legs.short_entry) / legs.short_entry * Decimal("10000")
        return None

    def _ab_quote_side(
        self, order_side: str, position_side: str, qty: Decimal, *, reduce_only: bool
    ) -> list[StepResult]:
        steps: list[StepResult] = []
        done_name = f"已平 {position_side}" if reduce_only else f"已补 {position_side}"
        # 收利/补仓：多轮贴盘 GTX，尽量不成交才市价
        tries = 48
        wait = 0.18

        def finished(legs: HedgeLegs) -> bool:
            current = legs.long_qty if position_side == "LONG" else legs.short_qty
            return current <= 0 if reduce_only else current > 0

        for i in range(tries):
            legs = self.futures.legs(self.symbol)
            if finished(legs):
                steps.append(StepResult(done_name, True, self._legs_text(legs)))
                return steps
            extra = self._side_pass(
                order_side,
                position_side,
                qty,
                reduce_only,
                market=False,
                aggressive=True,
                pass_n=i + 2,
            )
            steps.extend(extra)
            if any(_order_filled_hint(item) for item in extra) and finished(self.futures.legs(self.symbol)):
                steps.append(StepResult(done_name, True, self._legs_text(self.futures.legs(self.symbol))))
                return steps
            if self._wait_until(lambda: finished(self.futures.legs(self.symbol)), timeout=wait, interval=0.06):
                steps.append(StepResult(done_name, True, self._legs_text(self.futures.legs(self.symbol))))
                return steps

        # 挂单不成：市价兜底，保证平净/补齐
        try:
            self.futures.cancel_open(self.symbol)
        except BinanceAPIError:
            pass
        steps.append(StepResult("市价兜底", True, f"{position_side} 挂单追价 {tries} 轮未成，改市价"))
        for _ in range(2):
            legs = self.futures.legs(self.symbol)
            if finished(legs):
                steps.append(StepResult(done_name, True, self._legs_text(legs)))
                return steps
            need = legs.long_qty if position_side == "LONG" else legs.short_qty
            if not reduce_only:
                other = legs.short_qty if position_side == "LONG" else legs.long_qty
                need = other if other > 0 else qty
            if need <= 0:
                need = qty
            steps.extend(
                self._side_pass(
                    order_side, position_side, need, reduce_only, market=True, aggressive=True
                )
            )
            if self._wait_until(lambda: finished(self.futures.legs(self.symbol)), timeout=0.8, interval=0.1):
                steps.append(StepResult(done_name, True, self._legs_text(self.futures.legs(self.symbol))))
                return steps
        steps.append(StepResult(f"{position_side} 未完成", False, self._legs_text(self.futures.legs(self.symbol))))
        return steps

    def _ab_close_winner(self, side: str, qty: Decimal) -> list[StepResult]:
        steps: list[StepResult] = []
        close_side = "SELL" if side == "LONG" else "BUY"
        for i in range(8):
            legs = self.futures.legs(self.symbol)
            remain = legs.long_qty if side == "LONG" else legs.short_qty
            if remain <= 0:
                steps.append(StepResult(f"已平 {side}", True, self._legs_text(legs)))
                return steps
            if i > 0:
                steps.append(StepResult("继续平剩余", True, f"{side} 剩 {fmt_amount(remain)}"))
            steps.extend(self._ab_quote_side(close_side, side, remain, reduce_only=True))
            if (self.futures.legs(self.symbol).long_qty if side == "LONG" else self.futures.legs(self.symbol).short_qty) <= 0:
                steps.append(StepResult(f"已平 {side}", True, self._legs_text(self.futures.legs(self.symbol))))
                return steps
        # 多轮挂单路径仍剩：再市价扫
        remain = (
            self.futures.legs(self.symbol).long_qty
            if side == "LONG"
            else self.futures.legs(self.symbol).short_qty
        )
        if remain > 0:
            steps.append(StepResult("平仓市价扫尾", True, f"{side} 挂单路径后剩 {fmt_amount(remain)}"))
            for _ in range(3):
                legs = self.futures.legs(self.symbol)
                remain = legs.long_qty if side == "LONG" else legs.short_qty
                if remain <= 0:
                    break
                steps.extend(
                    self._side_pass(close_side, side, remain, True, market=True, aggressive=True)
                )
                self._wait_until(
                    lambda: (
                        self.futures.legs(self.symbol).long_qty
                        if side == "LONG"
                        else self.futures.legs(self.symbol).short_qty
                    )
                    <= 0,
                    timeout=0.8,
                    interval=0.1,
                )
        legs = self.futures.legs(self.symbol)
        remain = legs.long_qty if side == "LONG" else legs.short_qty
        if remain <= 0:
            steps.append(StepResult(f"已平 {side}", True, self._legs_text(legs)))
        else:
            steps.append(StepResult(f"{side} 未完成", False, f"市价后仍剩 {fmt_amount(remain)}"))
        return steps


class GridRunner:
    """参数网格：开仓 → 收利 → 全平 → 下一组。"""

    def __init__(
        self,
        ops: OpsService,
        account: Account,
        *,
        account_no: int,
        qty: Decimal = FIXED_QTY,
        poll_seconds: float = 15.0,
        enter_timeout: float = 3600.0,
        harvest_timeout: float = 7200.0,
        trials: list[TrialParams] | None = None,
        log_path: Path | None = None,
    ):
        self.ops = ops
        self.account = account
        self.account_no = account_no
        self.qty = qty
        self.poll_seconds = max(8.0, float(poll_seconds))
        self.enter_timeout = enter_timeout
        self.harvest_timeout = harvest_timeout
        self.trials = trials or build_trials()
        self.log_path = log_path or LOG_PATH
        self._last_cross_key = ""
        # 当前组实验快照，供 trial_done / scorecard 聚合
        self._trial_metrics: dict[str, Any] = {}

    def _log(self, payload: dict[str, Any]) -> None:
        row = {
            "ts": _now(),
            "account_no": self.account_no,
            "account_id": self.account.id,
            "account_name": self.account.name,
            "qty": str(self.qty),
            **payload,
        }
        append_log(row, self.log_path)
        _print(
            f"[abtest] #{self.account_no} {payload.get('trial') or ''} "
            f"{payload.get('event')} {payload.get('detail') or ''}"
        )

    def _cycle(self, trial: TrialParams) -> AbTestCycle:
        pipe = pipeline_for(self.account, self.ops.base, dry_run=False, ops=self.ops)
        return AbTestCycle(pipe, passive_bps=trial.passive_bps)

    def _macd_snap(self) -> dict | None:
        url = getattr(self.ops.base, "macd_indicator_url", None) or ""
        state = fetch_macd_state(url)
        if state is None:
            return None
        return {
            "bias": state.bias,
            "cross": state.cross,
            "label": state.label,
            "time": state.time,
            "macd": str(state.macd),
            "signal": str(state.signal),
            "hist": str(state.hist),
            "hist_abs": str(abs(state.hist)),
            "gap_abs": str(state.gap_abs),
            "enter_first": state.enter_first_side,
            "harvest_side": state.harvest_side,
        }

    def _fresh_cross(self, *, min_gap: Decimal | None = None):
        """取未用过的金叉/死叉；若 min_gap 给定则要求交叉前柱敞口够大。"""
        url = getattr(self.ops.base, "macd_indicator_url", None) or ""
        cross = fetch_macd_cross(url)
        if cross is None:
            return None, "等待金叉/死叉"
        key = f"{cross.kind}:{cross.time}"
        if key == self._last_cross_key:
            return None, f"{cross.label}@{cross.time} 已用过"
        if min_gap is not None and cross.gap_abs < min_gap:
            # 敞口不够：消费掉这根，避免死等同一根小波动交叉
            self._last_cross_key = key
            return (
                None,
                f"{cross.label}@{cross.time} 敞口{cross.gap_abs}<{min_gap}，跳过",
            )
        return cross, f"{cross.label}@{cross.time} 敞口={cross.gap_abs}"

    def run(self) -> None:
        _print(
            f"网格测试启动 #{self.account_no} id={self.account.id} {self.account.name} "
            f"qty={self.qty} trials={len(self.trials)} → {self.log_path}"
        )
        self._log({"event": "grid_start", "trials": len(self.trials), "detail": "开仓→收利→全平循环"})
        for idx, trial in enumerate(self.trials, start=1):
            self._run_one(idx, trial)
        self._log({"event": "grid_done", "detail": f"共 {len(self.trials)} 组，见 --summary"})
        _print(summarize(self.log_path))

    def _run_one(self, idx: int, trial: TrialParams) -> None:
        tag = trial.name
        self._trial_metrics = {
            "trial": tag,
            "trial_i": idx,
            "params": {
                "passive_bps": str(trial.passive_bps),
                "tp_usdt": str(trial.tp_usdt),
                "min_gap": str(trial.min_gap),
            },
            "qty": str(self.qty),
        }
        self._log(
            {
                "event": "trial_start",
                "trial": tag,
                "trial_i": idx,
                "params": self._trial_metrics["params"],
            }
        )
        if not self.ops.try_begin_action(self.account.id):
            self._log({"event": "trial_skip", "trial": tag, "detail": "账号忙"})
            return
        try:
            cycle = self._cycle(trial)
            flat_steps = cycle.ensure_flat()
            eq0 = _read_equity(cycle)
            self._trial_metrics["equity_before"] = str(eq0)
            self._log(
                {
                    "event": "pre_flat",
                    "trial": tag,
                    "ok": all(s.ok for s in flat_steps) if flat_steps else True,
                    "equity": str(eq0),
                    "detail": "；".join(f"{s.name}:{s.detail}" for s in flat_steps[-3:]),
                }
            )
        finally:
            self.ops.end_action(self.account.id)

        enter_ok = self._wait_and_enter(cycle, trial, tag)
        if not enter_ok:
            if self.ops.try_begin_action(self.account.id):
                try:
                    cycle.ensure_flat()
                    eq_fail = _read_equity(cycle)
                finally:
                    self.ops.end_action(self.account.id)
            else:
                eq_fail = None
            self._trial_metrics.update(
                {
                    "enter_ok": False,
                    "harvest_ok": False,
                    "equity_after_flat": str(eq_fail) if eq_fail is not None else None,
                    "cycle_equity_delta": (
                        str(eq_fail - d(self._trial_metrics.get("equity_before") or "0"))
                        if eq_fail is not None
                        else None
                    ),
                }
            )
            self._log(
                {
                    "event": "trial_fail_enter",
                    "trial": tag,
                    "metrics": dict(self._trial_metrics),
                    "detail": "开仓失败，已全平",
                }
            )
            return

        try:
            legs = cycle.futures.legs(cycle.symbol)
            spr = entry_spread_bps(legs)
            eq_enter = _read_equity(cycle)
            self._trial_metrics.update(
                {
                    "enter_ok": True,
                    "entry_spread_bps": str(spr) if spr is not None else None,
                    "equity_after_enter": str(eq_enter),
                    "legs_after_enter": _legs_snap(legs),
                }
            )
            self._log(
                {
                    "event": "enter_ok",
                    "trial": tag,
                    "entry_spread_bps": str(spr) if spr is not None else None,
                    "equity": str(eq_enter),
                    "legs": _legs_snap(legs),
                    "macd": self._macd_snap(),
                }
            )
        except Exception as exc:
            self._log({"event": "enter_ok_log_fail", "trial": tag, "detail": str(exc)})

        harvest_ok = self._wait_and_harvest(cycle, trial, tag)
        self._trial_metrics["harvest_ok"] = harvest_ok

        if self.ops.try_begin_action(self.account.id):
            try:
                eq_before_flat = _read_equity(cycle)
                legs_before_flat = _legs_snap(cycle.futures.legs(cycle.symbol))
                flat_steps = cycle.ensure_flat()
                flat_ok = (
                    cycle.futures.legs(cycle.symbol).long_qty <= 0
                    and cycle.futures.legs(cycle.symbol).short_qty <= 0
                )
                eq_after = _read_equity(cycle)
                eq0 = d(self._trial_metrics.get("equity_before") or "0")
                delta = eq_after - eq0
                self._trial_metrics.update(
                    {
                        "equity_before_flat": str(eq_before_flat),
                        "legs_before_flat": legs_before_flat,
                        "equity_after_flat": str(eq_after),
                        "flat_ok": flat_ok,
                        "cycle_equity_delta": str(delta),
                        # 简单优势分：权益差为主，开仓正价差加分，市价次数减分（后面 summarize 再用）
                        "score_hint": str(delta),
                    }
                )
                self._log(
                    {
                        "event": "trial_done",
                        "trial": tag,
                        "enter_ok": True,
                        "harvest_ok": harvest_ok,
                        "flat_ok": flat_ok,
                        "equity_before": self._trial_metrics.get("equity_before"),
                        "equity_after_enter": self._trial_metrics.get("equity_after_enter"),
                        "equity_before_flat": str(eq_before_flat),
                        "equity_after_flat": str(eq_after),
                        "cycle_equity_delta": str(delta),
                        "entry_spread_bps": self._trial_metrics.get("entry_spread_bps"),
                        "enter_used_market": self._trial_metrics.get("enter_used_market"),
                        "harvest_used_market": self._trial_metrics.get("harvest_used_market"),
                        "enter_elapsed_s": self._trial_metrics.get("enter_elapsed_s"),
                        "harvest_elapsed_s": self._trial_metrics.get("harvest_elapsed_s"),
                        "harvest_side": self._trial_metrics.get("harvest_side"),
                        "params": self._trial_metrics.get("params"),
                        "metrics": dict(self._trial_metrics),
                        "detail": "；".join(f"{s.name}:{s.detail}" for s in flat_steps[-3:]),
                    }
                )
            finally:
                self.ops.end_action(self.account.id)
        else:
            self._log(
                {
                    "event": "trial_done",
                    "trial": tag,
                    "harvest_ok": harvest_ok,
                    "flat_ok": False,
                    "metrics": dict(self._trial_metrics),
                    "detail": "结束全平时账号忙，请手动检查仓位",
                }
            )

    def _wait_and_enter(self, cycle: AbTestCycle, trial: TrialParams, tag: str) -> bool:
        deadline = time.monotonic() + self.enter_timeout
        while time.monotonic() < deadline:
            cross, note = self._fresh_cross(min_gap=trial.min_gap)
            if cross is None:
                self._log({"event": "wait_enter", "trial": tag, "detail": note, "macd": self._macd_snap()})
                time.sleep(self.poll_seconds)
                continue
            if not self.ops.try_begin_action(self.account.id):
                self._log({"event": "enter_busy", "trial": tag, "detail": "账号忙，稍后重试（本交叉未消费）"})
                time.sleep(self.poll_seconds)
                continue
            # 拿到锁后再标记交叉已用，避免 busy 时白白跳过信号
            self._last_cross_key = f"{cross.kind}:{cross.time}"
            self._trial_metrics["enter_gap_abs"] = str(cross.gap_abs)
            self._log(
                {
                    "event": "enter_start",
                    "trial": tag,
                    "detail": note,
                    "gap_abs": str(cross.gap_abs),
                    "min_gap": str(trial.min_gap),
                    "macd": self._macd_snap(),
                }
            )
            t0 = time.monotonic()
            try:
                steps = cycle.enter_tight(self.qty)
            finally:
                self.ops.end_action(self.account.id)
            elapsed = time.monotonic() - t0
            legs = cycle.futures.legs(cycle.symbol)
            ok = legs.missing_side is None and legs.long_qty > 0 and legs.short_qty > 0
            spr = entry_spread_bps(legs) if ok else None
            used_market = any("市价" in str(s.name) or "市价" in str(s.detail) for s in steps)
            self._trial_metrics.update(
                {
                    "enter_elapsed_s": round(elapsed, 3),
                    "enter_used_market": used_market,
                    "entry_spread_bps": str(spr) if spr is not None else None,
                    "enter_ok": ok,
                }
            )
            self._log(
                {
                    "event": "enter_done",
                    "trial": tag,
                    "ok": ok,
                    "elapsed_s": round(elapsed, 3),
                    "entry_spread_bps": str(spr) if spr is not None else None,
                    "used_market": used_market,
                    "gap_abs": str(cross.gap_abs),
                    "equity": str(_read_equity(cycle)),
                    "legs": _legs_snap(legs),
                    "detail": "；".join(f"{s.name}:{s.detail}" for s in steps[-8:])[:2000],
                    "macd": self._macd_snap(),
                    "params": self._trial_metrics.get("params"),
                }
            )
            if ok:
                return True
            if self.ops.try_begin_action(self.account.id):
                try:
                    cycle.ensure_flat()
                finally:
                    self.ops.end_action(self.account.id)
            # 本根交叉用过了，等下一根
            time.sleep(self.poll_seconds)
        return False

    def _wait_and_harvest(self, cycle: AbTestCycle, trial: TrialParams, tag: str) -> bool:
        deadline = time.monotonic() + self.harvest_timeout
        while time.monotonic() < deadline:
            legs = cycle.futures.legs(cycle.symbol)
            if legs.long_qty <= 0 and legs.short_qty <= 0:
                self._log({"event": "harvest_abort", "trial": tag, "detail": "仓位已空"})
                return False
            bid, ask = cycle.futures.book(cycle.symbol, force=True)
            mid = (bid + ask) / 2 if bid > 0 and ask > 0 else Decimal("0")
            try:
                equity = (cycle.futures.account_risk() or {}).get("equity") or Decimal("0")
            except Exception:
                equity = Decimal("0")
            fee = resolve_harvest_fee(cycle.futures, cycle.symbol, legs, mid, cycle.settings)
            # 小仓测试：用网格绝对值门槛，至少覆盖约 1 倍手续费
            need = max(trial.tp_usdt, fee if fee > 0 else Decimal("0"))
            long_ready = legs.long_pnl >= need
            short_ready = legs.short_pnl >= need
            if not long_ready and not short_ready:
                self._log(
                    {
                        "event": "wait_profit",
                        "trial": tag,
                        "detail": (
                            f"L={fmt_amount(legs.long_pnl)} S={fmt_amount(legs.short_pnl)} "
                            f"need={fmt_amount(need)} equity={fmt_amount(equity)}"
                        ),
                    }
                )
                time.sleep(self.poll_seconds)
                continue

            # 一律等 MACD 交叉，且敞口 ≥ min_gap
            cross, note = self._fresh_cross(min_gap=trial.min_gap)
            if cross is None:
                self._log(
                    {
                        "event": "wait_harvest_cross",
                        "trial": tag,
                        "detail": note,
                        "long_ready": long_ready,
                        "short_ready": short_ready,
                        "macd": self._macd_snap(),
                    }
                )
                time.sleep(self.poll_seconds)
                continue
            side = cross.harvest_side
            pnl = legs.long_pnl if side == "LONG" else legs.short_pnl
            if pnl < need:
                self._last_cross_key = f"{cross.kind}:{cross.time}"
                self._log(
                    {
                        "event": "cross_skip_pnl",
                        "trial": tag,
                        "detail": (
                            f"{note}→收{side} pnl={fmt_amount(pnl)}<{fmt_amount(need)} "
                            f"敞口={cross.gap_abs}"
                        ),
                        "gap_abs": str(cross.gap_abs),
                    }
                )
                time.sleep(self.poll_seconds)
                continue
            if not self.ops.try_begin_action(self.account.id):
                self._log(
                    {
                        "event": "harvest_busy",
                        "trial": tag,
                        "detail": "账号忙，稍后重试（本交叉未消费）",
                    }
                )
                time.sleep(self.poll_seconds)
                continue
            self._last_cross_key = f"{cross.kind}:{cross.time}"
            self._trial_metrics["harvest_gap_abs"] = str(cross.gap_abs)

            self._log(
                {
                    "event": "harvest_start",
                    "trial": tag,
                    "detail": f"{note} → 收{side}",
                    "side": side,
                    "need": str(need),
                    "gap_abs": str(cross.gap_abs),
                    "min_gap": str(trial.min_gap),
                    "macd": self._macd_snap(),
                }
            )
            t0 = time.monotonic()
            try:
                steps = cycle.harvest_tight(side)
            finally:
                self.ops.end_action(self.account.id)
            elapsed = time.monotonic() - t0
            after = cycle.futures.legs(cycle.symbol)
            ok = after.missing_side is None and after.long_qty > 0
            used_market = any("市价" in str(s.name) or "市价" in str(s.detail) for s in steps)
            self._trial_metrics.update(
                {
                    "harvest_ok": ok,
                    "harvest_elapsed_s": round(elapsed, 3),
                    "harvest_used_market": used_market,
                    "harvest_side": side,
                    "equity_after_harvest": str(_read_equity(cycle)),
                    "legs_after_harvest": _legs_snap(after),
                }
            )
            self._log(
                {
                    "event": "harvest_done",
                    "trial": tag,
                    "ok": ok,
                    "elapsed_s": round(elapsed, 3),
                    "side": side,
                    "used_market": used_market,
                    "gap_abs": str(cross.gap_abs),
                    "equity": str(_read_equity(cycle)),
                    "legs": _legs_snap(after),
                    "detail": "；".join(f"{s.name}:{s.detail}" for s in steps[-10:])[:2000],
                    "params": self._trial_metrics.get("params"),
                }
            )
            return ok
        self._log({"event": "harvest_timeout", "trial": tag, "detail": f"{self.harvest_timeout}s 未达收利"})
        return False



def summarize(path: Path | None = None, *, write_scorecard: bool = True) -> str:
    """按组汇总优势：权益差优先，其次开仓价差、少市价、耗时。"""
    target = path or LOG_PATH
    if not target.exists():
        return f"还没有日志: {target}"

    # 两遍：先标出有 trial_done 的组，再聚合，避免 enter/harvest 与 trial_done 重复计价
    raw_rows: list[dict[str, Any]] = []
    for line in target.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            raw_rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue

    trials_with_done: set[str] = {
        str(r["trial"]) for r in raw_rows if r.get("event") == "trial_done" and r.get("trial")
    }

    by_trial: dict[str, dict[str, Any]] = {}
    for row in raw_rows:
        trial = str(row.get("trial") or "")
        if not trial:
            continue
        slot = by_trial.setdefault(
            trial,
            {
                "params": row.get("params") or {},
                "n_done": 0,
                "enter_ok": 0,
                "enter_fail": 0,
                "harvest_ok": 0,
                "harvest_fail": 0,
                "spreads": [],
                "equity_deltas": [],
                "enter_elapsed": [],
                "harvest_elapsed": [],
                "enter_market": 0,
                "harvest_market": 0,
                "samples": [],
            },
        )
        ev = row.get("event")
        if ev == "trial_done":
            slot["n_done"] += 1
            if row.get("enter_ok", True):
                slot["enter_ok"] += 1
            if row.get("harvest_ok"):
                slot["harvest_ok"] += 1
            else:
                slot["harvest_fail"] += 1
            if row.get("entry_spread_bps") is not None:
                try:
                    slot["spreads"].append(float(row["entry_spread_bps"]))
                except (TypeError, ValueError):
                    pass
            if row.get("cycle_equity_delta") is not None:
                try:
                    slot["equity_deltas"].append(float(row["cycle_equity_delta"]))
                except (TypeError, ValueError):
                    pass
            if row.get("enter_elapsed_s") is not None:
                slot["enter_elapsed"].append(float(row["enter_elapsed_s"]))
            if row.get("harvest_elapsed_s") is not None:
                slot["harvest_elapsed"].append(float(row["harvest_elapsed_s"]))
            if row.get("enter_used_market"):
                slot["enter_market"] += 1
            if row.get("harvest_used_market"):
                slot["harvest_market"] += 1
            if row.get("params"):
                slot["params"] = row["params"]
            slot["samples"].append(
                {
                    "ts": row.get("ts"),
                    "cycle_equity_delta": row.get("cycle_equity_delta"),
                    "entry_spread_bps": row.get("entry_spread_bps"),
                    "enter_used_market": row.get("enter_used_market"),
                    "harvest_used_market": row.get("harvest_used_market"),
                    "harvest_ok": row.get("harvest_ok"),
                    "flat_ok": row.get("flat_ok"),
                }
            )
        elif ev == "trial_fail_enter":
            slot["enter_fail"] += 1
            metrics = row.get("metrics") or {}
            if metrics.get("cycle_equity_delta") is not None:
                try:
                    slot["equity_deltas"].append(float(metrics["cycle_equity_delta"]))
                except (TypeError, ValueError):
                    pass
            if row.get("params") or metrics.get("params"):
                slot["params"] = row.get("params") or metrics.get("params")
        elif ev == "enter_done" and trial not in trials_with_done:
            slot["enter_ok" if row.get("ok") else "enter_fail"] += 1
            if row.get("entry_spread_bps") is not None:
                try:
                    slot["spreads"].append(float(row["entry_spread_bps"]))
                except (TypeError, ValueError):
                    pass
            if row.get("elapsed_s") is not None:
                slot["enter_elapsed"].append(float(row["elapsed_s"]))
            if row.get("used_market"):
                slot["enter_market"] += 1
        elif ev == "harvest_done" and trial not in trials_with_done:
            slot["harvest_ok" if row.get("ok") else "harvest_fail"] += 1
            if row.get("elapsed_s") is not None:
                slot["harvest_elapsed"].append(float(row["elapsed_s"]))
            if row.get("used_market"):
                slot["harvest_market"] += 1

    lines = [
        f"日志 {target}",
        f"评分卡会写入 {SCORECARD_PATH}",
        "排序：均权益差 ↓ → 开仓价差(正更好) → 少市价 → 入场更快",
        "",
    ]
    ranked: list[tuple[tuple, dict]] = []
    for trial, slot in by_trial.items():
        deltas = slot["equity_deltas"]
        spreads = slot["spreads"]
        avg_delta = sum(deltas) / len(deltas) if deltas else 0.0
        avg_spr = sum(spreads) / len(spreads) if spreads else 0.0
        avg_e = sum(slot["enter_elapsed"]) / len(slot["enter_elapsed"]) if slot["enter_elapsed"] else 0.0
        avg_h = sum(slot["harvest_elapsed"]) / len(slot["harvest_elapsed"]) if slot["harvest_elapsed"] else 0.0
        market_n = slot["enter_market"] + slot["harvest_market"]
        h_ok = slot["harvest_ok"]
        e_ok = slot["enter_ok"]
        # 综合分：权益差权重大；正价差加分；市价惩罚；耗时轻微惩罚
        score = (
            avg_delta * 100.0
            + avg_spr * 0.05
            - market_n * 0.02
            - avg_e * 0.0005
            + h_ok * 0.5
        )
        rec = {
            "trial": trial,
            "params": slot["params"],
            "n_done": slot["n_done"],
            "enter_ok": e_ok,
            "enter_fail": slot["enter_fail"],
            "harvest_ok": h_ok,
            "harvest_fail": slot["harvest_fail"],
            "avg_equity_delta": round(avg_delta, 6),
            "avg_entry_spread_bps": round(avg_spr, 4),
            "avg_enter_elapsed_s": round(avg_e, 3),
            "avg_harvest_elapsed_s": round(avg_h, 3),
            "enter_market_count": slot["enter_market"],
            "harvest_market_count": slot["harvest_market"],
            "score": round(score, 6),
            "samples": slot["samples"],
        }
        line = (
            f"{trial}  score={score:.4f}  均权益差={avg_delta:.4f}  "
            f"均开仓价差={avg_spr:.2f}bp  入场{e_ok}/{e_ok+slot['enter_fail']}  "
            f"收利{h_ok}/{h_ok+slot['harvest_fail']}  市价入{slot['enter_market']}/收{slot['harvest_market']}  "
            f"耗时入{avg_e:.1f}s/收{avg_h:.1f}s"
        )
        lines.append(line)
        ranked.append(((score, avg_delta, avg_spr, -market_n, -avg_e), rec))

    ranked.sort(key=lambda x: x[0], reverse=True)
    best = ranked[0][1] if ranked else None
    if best:
        lines += [
            "",
            f"优势最大: {best['trial']}",
            f"  params={best['params']}",
            f"  均权益差={best['avg_equity_delta']}  均开仓价差={best['avg_entry_spread_bps']}bp  score={best['score']}",
            "落地建议：把该组 passive_bps / tp_usdt / min_gap 写回正式自动收利逻辑。",
        ]

    if write_scorecard:
        SCORECARD_PATH.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "generated_at": _now(),
            "log_path": str(target),
            "best": best,
            "ranking": [r[1] for r in ranked],
        }
        SCORECARD_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        # CSV 方便表格对比
        csv_path = SCORECARD_PATH.with_suffix(".csv")
        headers = [
            "rank",
            "trial",
            "score",
            "avg_equity_delta",
            "avg_entry_spread_bps",
            "enter_ok",
            "enter_fail",
            "harvest_ok",
            "harvest_fail",
            "enter_market_count",
            "harvest_market_count",
            "avg_enter_elapsed_s",
            "avg_harvest_elapsed_s",
            "passive_bps",
            "tp_usdt",
            "min_gap",
        ]
        with csv_path.open("w", encoding="utf-8") as fh:
            fh.write(",".join(headers) + "\n")
            for i, (_, rec) in enumerate(ranked, start=1):
                p = rec.get("params") or {}
                fh.write(
                    ",".join(
                        [
                            str(i),
                            rec["trial"],
                            str(rec["score"]),
                            str(rec["avg_equity_delta"]),
                            str(rec["avg_entry_spread_bps"]),
                            str(rec["enter_ok"]),
                            str(rec["enter_fail"]),
                            str(rec["harvest_ok"]),
                            str(rec["harvest_fail"]),
                            str(rec["enter_market_count"]),
                            str(rec["harvest_market_count"]),
                            str(rec["avg_enter_elapsed_s"]),
                            str(rec["avg_harvest_elapsed_s"]),
                            str(p.get("passive_bps", "")),
                            str(p.get("tp_usdt", "")),
                            str(p.get("min_gap", "")),
                        ]
                    )
                    + "\n"
                )
        lines.append(f"已写评分卡 JSON: {SCORECARD_PATH}")
        lines.append(f"已写评分卡 CSV:  {csv_path}")

    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="MACD 紧对冲开仓/收利参数网格（独立测试）")
    parser.add_argument("--account-no", type=int, default=4)
    parser.add_argument("--account-id", type=int, default=None)
    parser.add_argument("--qty", default=str(FIXED_QTY))
    parser.add_argument("--poll", type=float, default=15.0)
    parser.add_argument("--enter-timeout", type=float, default=3600.0, help="单组等开仓最长秒数")
    parser.add_argument("--harvest-timeout", type=float, default=7200.0, help="单组等收利最长秒数")
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--confirm", action="store_true")
    parser.add_argument("--summary", action="store_true")
    parser.add_argument("--list-trials", action="store_true", help="只列出将要跑的参数组")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    if args.summary:
        print(summarize())
        return 0

    trials = build_trials()
    if args.list_trials:
        print(f"共 {len(trials)} 组：")
        for i, t in enumerate(trials, 1):
            print(f"  {i:2d}. {t.name}")
        return 0

    if not args.live or not args.confirm:
        raise SystemExit("实盘必须加 --live --confirm")

    store = AccountStore()
    ops = OpsService(store=store, base=load_settings(dry_run_override=False))
    if args.account_id is not None:
        account = store.get(int(args.account_id))
        nos = [i + 1 for i, a in enumerate(store.list_accounts()) if a.id == account.id]
        account_no = nos[0] if nos else int(args.account_id)
    else:
        account_no = int(args.account_no)
        account = resolve_account_by_no(store, account_no)

    if account.auto_harvest:
        _print(f"提示：#{account_no} 开着自动收利，建议关掉避免冲突")

    qty = d(args.qty)
    if qty <= 0:
        qty = FIXED_QTY

    runner = GridRunner(
        ops,
        account,
        account_no=account_no,
        qty=qty,
        poll_seconds=args.poll,
        enter_timeout=args.enter_timeout,
        harvest_timeout=args.harvest_timeout,
        trials=trials,
    )
    try:
        runner.run()
    except KeyboardInterrupt:
        _print("已停止；尽量全平当前仓…")
        try:
            if ops.try_begin_action(account.id):
                try:
                    AbTestCycle(pipeline_for(account, ops.base, dry_run=False, ops=ops)).ensure_flat()
                finally:
                    ops.end_action(account.id)
        except Exception:
            log.exception("停止时全平失败")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
