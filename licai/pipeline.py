from __future__ import annotations

import time
from dataclasses import dataclass, replace
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from typing import Any, Callable

from .client import BinanceAPIError, BinanceClient
from .config import Settings, d, fmt_amount, is_mmr_sentinel, settle_asset_of
from .convert import ConvertAPI
from .earn import EarnAPI, FlexibleProduct
from .futures import FuturesAPI
from .market import collateral_rates


def apr_percent(apr: Decimal) -> Decimal:
    return apr if apr > 1 else apr * Decimal("100")


_BFUSD_TIER_USDT = Decimal("800")


def apr_choice_text(product: FlexibleProduct) -> str:
    """BFUSD：前 800 用高档，超出部分用低档。不看账户总资金。"""
    text = f"{apr_percent(product.apr):.2f}%"
    raw = product.raw or {}
    base = raw.get("base_apr")
    small = raw.get("small_balance_apr")
    if product.kind == "bfusd" and small not in (None, "", "0") and base not in (None, "", "0"):
        held = d(raw.get("bfusd_held") or 0)
        held_text = f"持仓 {fmt_amount(held, 2)}" if held > 0 else "尚无持仓"
        return (
            text
            + f"（{held_text}；800以内 {apr_percent(d(small)):.2f}%，以上 {apr_percent(d(base)):.2f}%）"
        )
    return text


def apr_ratio(apr: Decimal) -> Decimal:
    if apr <= 0:
        return Decimal("0")
    return apr / Decimal("100") if apr > 1 else apr


def earns_as_um_margin(product: FlexibleProduct, rates: dict[str, Decimal]) -> bool:
    """申购之后实际拿在手里的代币，必须能当统一账户保证金去开 USDT 或 USDC 合约。

    活期仓位看 LD 代币（USDT 活期是 LDUSDT）。BFUSD、RWUSD 本身就是抵押资产。
    现货 BTC 能做抵押，不代表 BTC 活期锁仓后还能开 U 本位。
    """
    if product.kind in {"bfusd", "rwusd"}:
        token = product.asset.upper()
    else:
        token = (product.margin_token or f"LD{product.asset}").upper()
    return rates.get(token, Decimal("0")) > 0


@dataclass
class StepResult:
    name: str
    ok: bool
    detail: Any
    dry_run: bool = False
    at: str = ""

    def __post_init__(self) -> None:
        if not self.at:
            self.at = time.strftime("%H:%M:%S")


SPOT_MIN = Decimal("1")
# 币安 Convert 常见下限；低于此数询价会 345233，不当整笔换年化失败
CONVERT_MIN = Decimal("0.01")


class Pipeline:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = BinanceClient(
            settings.api_key,
            settings.api_secret,
            recv_window=settings.recv_window,
            proxy=settings.proxy,
        )
        self.earn = EarnAPI(self.client)
        self.convert_api = ConvertAPI(self.client)
        self.futures = FuturesAPI(self.client, unified_account=settings.unified_account)
        self._product_list: list[FlexibleProduct] | None = None
        self._product_list_at = 0.0
        self._wallet_transfer_blocked = False
        # 本进程内：地区/合规不可用的产品（如 RWUSD -9000），避免反复赎回后申购失败把 U 卡在现货
        self._runtime_denylist: set[str] = set()
        self._on_denylist_change: Callable[[], None] | None = None

    def spot_cash(self, asset: str | None = None) -> Decimal:
        return self.earn.spot_free((asset or self.settings.source_asset).upper())

    def spot_needs_move(self) -> bool:
        return any(self.spot_cash(asset) >= SPOT_MIN for asset in ("USDT", "BFUSD", "RWUSD"))

    def require_keys(self) -> None:
        if not self.settings.api_key or not self.settings.api_secret:
            raise SystemExit("请先在 .env 里填写 BINANCE_API_KEY 和 BINANCE_API_SECRET")

    def margin_assets(self) -> set[str]:
        configured = {a.upper() for a in self.settings.margin_earn_assets if a}
        if configured:
            return configured
        return {asset for asset, rate in self._collateral_rate_map().items() if rate > 0}

    def _account_funds(self) -> Decimal:
        """统一账户权益，加上还在现货、没进统一账户的稳定币。"""
        equity = Decimal("0")
        try:
            if self.settings.unified_account:
                equity = self.futures.account_equity()
        except BinanceAPIError:
            equity = Decimal("0")
        spot = Decimal("0")
        for asset in ("USDT", "USDC", "BFUSD", "RWUSD"):
            try:
                spot += self.earn.spot_free(asset)
            except BinanceAPIError:
                continue
        return equity + spot

    def _bfusd_held(self) -> Decimal:
        """BFUSD 持仓（理财、现货、统一账户取最大），用来判断还在不在 800 高档里。"""
        try:
            earn_amt = self.earn.bfusd_balance()
        except BinanceAPIError:
            earn_amt = Decimal("0")
        try:
            spot = self.earn.spot_free("BFUSD")
        except BinanceAPIError:
            spot = Decimal("0")
        pm = self._pm_balances().get("BFUSD", Decimal("0"))
        return max(earn_amt, spot, pm)

    def _bfusd_product(self) -> FlexibleProduct:
        product = self.earn.bfusd_product()
        raw = dict(product.raw or {})
        base = d(raw.get("base_apr") or product.apr)
        small = d(raw.get("small_balance_apr") or 0)
        held = self._bfusd_held()
        raw["base_apr"] = str(base)
        raw["bfusd_held"] = str(held)
        # 还没满 800 时，下一笔按高档参与比较；满了之后新增的按低档
        use_small = small > base and held < _BFUSD_TIER_USDT
        raw["apr_tier"] = "under_800" if use_small else "over_800"
        return replace(product, apr=small if use_small else base, raw=raw)

    def _bfusd_subscribe_room(self, product: FlexibleProduct) -> Decimal | None:
        """高档值得买、低档不值得时，申购最多补到 800。两档都值得则不封顶。"""
        raw = product.raw or {}
        base = d(raw.get("base_apr") or product.apr)
        small = d(raw.get("small_balance_apr") or 0)
        if small <= base:
            return None
        gap = d(getattr(self.settings, "switch_min_apr_gap", None) or "0.1")
        if gap < 0:
            gap = Decimal("0")
        best_other = Decimal("0")
        for item in self.list_products():
            if item.kind == "bfusd" or not self._eligible(item, self.margin_assets()):
                continue
            best_other = max(best_other, apr_percent(item.apr))
        base_pct = apr_percent(base)
        small_pct = apr_percent(small)
        if base_pct > 0 and base_pct + gap >= best_other:
            return None
        if small_pct <= 0 or small_pct + gap < best_other:
            return Decimal("0")
        held = d(raw.get("bfusd_held") or 0)
        return max(_BFUSD_TIER_USDT - held, Decimal("0"))

    def _bfusd_keep_amount(self, product: FlexibleProduct, amount: Decimal, target: FlexibleProduct) -> Decimal:
        """800 以内高档不低于目标时，留下不超过 800 的 BFUSD，只换超出的低档。"""
        if product.kind != "bfusd" or amount <= 0:
            return Decimal("0")
        raw = product.raw or {}
        base = d(raw.get("base_apr") or product.apr)
        small = d(raw.get("small_balance_apr") or 0)
        if small <= base:
            return Decimal("0")
        gap = d(getattr(self.settings, "switch_min_apr_gap", None) or "0.1")
        if gap < 0:
            gap = Decimal("0")
        target_pct = apr_percent(target.apr)
        base_pct = apr_percent(base)
        small_pct = apr_percent(small)
        if base_pct > 0 and base_pct + gap >= target_pct:
            return amount
        if small_pct <= 0 or small_pct + gap < target_pct:
            return Decimal("0")
        return min(amount, _BFUSD_TIER_USDT)

    def _apr_gap(self) -> Decimal:
        gap = d(getattr(self.settings, "switch_min_apr_gap", None) or "0.1")
        return gap if gap > 0 else Decimal("0")

    @staticmethod
    def _alloc_key(product: FlexibleProduct) -> str:
        if product.kind in {"bfusd", "rwusd"}:
            return product.kind
        return f"flex:{product.product_id or product.asset}"

    def _yield_slices(self) -> list[tuple[FlexibleProduct, Decimal, Decimal | None]]:
        """每个产品按自己的额度切档：(产品, 年化百分比, 该档额度)。额度 None 表示剩余资金。"""
        slices: list[tuple[FlexibleProduct, Decimal, Decimal | None]] = []
        for product in self.list_products():
            if not self._eligible(product, self.margin_assets()):
                continue
            if product.kind == "bfusd":
                raw = product.raw or {}
                base = apr_percent(d(raw.get("base_apr") or product.apr))
                small = apr_percent(d(raw.get("small_balance_apr") or 0))
                if small > base:
                    slices.append((product, small, _BFUSD_TIER_USDT))
                    slices.append((product, base, None))
                else:
                    slices.append((product, base if base > 0 else apr_percent(product.apr), None))
            else:
                slices.append((product, apr_percent(product.apr), None))
        slices.sort(key=lambda item: item[1], reverse=True)
        return slices

    def _flexible_product_balance(self, product: FlexibleProduct) -> Decimal:
        """理财钱包里该活期产品的可用余额（不含统一账户占保部分）。"""
        try:
            self.earn.invalidate()
            rows = self.earn.positions(product.asset)
        except Exception:
            return Decimal("0")
        total = Decimal("0")
        pid = str(product.product_id or "")
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            if pid and str(row.get("productId") or "") not in {"", pid}:
                continue
            total += d(
                row.get("totalAmount")
                or row.get("amount")
                or row.get("collateralAmount")
                or 0
            )
        return total

    def _movable_holding_amount(self, product: FlexibleProduct, amount: Decimal) -> Decimal:
        """统一账户里当保证金的仓，只能按可转回额度算可换数量；转不出的不要反复列入换年化。"""
        if amount <= 0:
            return Decimal("0")
        located = str((product.raw or {}).get("located") or "")
        in_pm = located == "pm" or product.kind in {"bfusd", "rwusd"}
        if not in_pm:
            return amount
        try:
            legs = self.futures.legs(self.settings.hedge_symbol)
            has_hedge = legs.long_qty > 0 or legs.short_qty > 0
        except Exception:
            has_hedge = True
        try:
            if product.kind == "flexible" and product.asset.upper() == "USDT":
                cap = self.futures.earn_asset_transferable("LDUSDT", "FUTURE_TO_EARN")
            elif product.kind in {"bfusd", "rwusd"}:
                try:
                    cap = self.futures.earn_asset_transferable(product.asset, "FUTURE_TO_EARN")
                except BinanceAPIError:
                    cap = self.futures.max_withdraw(product.asset)
            else:
                return amount
        except BinanceAPIError:
            return Decimal("0") if has_hedge else amount
        if cap < SPOT_MIN:
            return Decimal("0") if has_hedge else amount
        return min(amount, cap)

    def _allocation_moves(
        self,
        holdings: list[tuple[FlexibleProduct, Decimal]] | None = None,
        *,
        stable_only: bool = False,
    ) -> list[tuple[FlexibleProduct, Decimal, FlexibleProduct, str]]:
        """按各产品额度从高年化往低填，只挪超出额度、且年化差得过最小差距的部分。"""
        holdings = holdings if holdings is not None else self.earn_holdings()
        slices = self._yield_slices()
        if not slices:
            return []
        gap = self._apr_gap()
        stable_rows: list[tuple[FlexibleProduct, Decimal]] = []
        alt_rows: list[tuple[FlexibleProduct, Decimal]] = []
        dust = SPOT_MIN  # <1 的稳定币灰尘不进换年化，避免 -6060/-6009
        for product, amount in holdings:
            amount = self._movable_holding_amount(product, amount)
            if amount <= 0:
                continue
            if self._is_stable_earn_asset(product):
                if amount < dust:
                    continue
                stable_rows.append((product, amount))
            elif not stable_only:
                alt_rows.append((product, amount))
        total = sum((amt for _, amt in stable_rows), Decimal("0"))
        remaining = total
        slice_want: list[Decimal] = []
        for _product, _apr, cap in slices:
            if remaining <= 0:
                slice_want.append(Decimal("0"))
                continue
            take = remaining if cap is None else min(remaining, cap)
            slice_want.append(take)
            remaining -= take
        left: dict[str, Decimal] = {}
        by_key: dict[str, FlexibleProduct] = {}
        for product, amount in stable_rows:
            key = self._alloc_key(product)
            left[key] = left.get(key, Decimal("0")) + amount
            by_key[key] = product
        slice_have: list[Decimal] = []
        for product, _apr, cap in slices:
            key = self._alloc_key(product)
            avail = left.get(key, Decimal("0"))
            take = avail if cap is None else min(avail, cap)
            slice_have.append(take)
            left[key] = avail - take
        overs: list[tuple[Decimal, FlexibleProduct, Decimal]] = []
        shorts: list[tuple[Decimal, FlexibleProduct, Decimal]] = []
        for (product, apr, _cap), have, want in zip(slices, slice_have, slice_want):
            if have > want:
                overs.append((apr, product, have - want))
            elif want > have:
                shorts.append((apr, product, want - have))
        # 未进任何年化档的持仓：只有产品当前仍可选时才算「超额」；
        # 地区黑名单/不可申购的产品（如临时禁用 RWUSD）绝不能当成 0% 倒卖出去。
        slice_keys = {self._alloc_key(p) for p, _apr, _cap in slices}
        for key, amt in left.items():
            if amt > 0 and key in slice_keys:
                overs.append((Decimal("0"), by_key[key], amt))
        overs.sort(key=lambda item: item[0])
        shorts.sort(key=lambda item: item[0], reverse=True)
        moves: list[tuple[FlexibleProduct, Decimal, FlexibleProduct, str]] = []
        for src_apr, src, extra in overs:
            for idx, (dst_apr, dst, need) in enumerate(shorts):
                if extra <= 0 or need <= 0:
                    continue
                if self._alloc_key(src) == self._alloc_key(dst):
                    continue
                if dst_apr < src_apr + gap:
                    continue
                amt = min(extra, need)
                moves.append(
                    (
                        src,
                        amt,
                        dst,
                        f"{src.asset} {fmt_amount(amt, 2)}（{src_apr:.2f}%）→ {dst.asset}（{dst_apr:.2f}%）",
                    )
                )
                extra -= amt
                shorts[idx] = (dst_apr, dst, need - amt)
        dest, dest_apr = slices[0][0], slices[0][1]
        open_short = next((item for item in shorts if item[2] > 0), None)
        if open_short is not None:
            dest_apr, dest, _need = open_short
        for product, amount in alt_rows:
            moves.append(
                (
                    product,
                    amount,
                    dest,
                    f"{product.asset} {fmt_amount(amount, 2)} → {dest.asset}（{dest_apr:.2f}%）",
                )
            )
        return moves

    def list_products(self) -> list[FlexibleProduct]:
        now = time.monotonic()
        if self._product_list is not None and now - self._product_list_at < 60:
            return self._product_list
        if self.settings.api_key and self.settings.api_secret:
            try:
                products = self.earn.list_flexible()
            except BinanceAPIError:
                products = self.earn.list_flexible_public()
        else:
            try:
                products = self.earn.list_flexible_public()
            except Exception:
                products = []
        # BFUSD / RWUSD 走专项接口，不用活期列表里可能缺年化、不可申购的那一行
        filtered = [p for p in products if p.asset not in {"BFUSD", "RWUSD"}]
        for product in filtered:
            if not product.margin_token:
                product.margin_token = f"LD{product.asset}"
        rates = self._collateral_rate_map()
        if rates.get("BFUSD", Decimal("0")) > 0:
            filtered.append(self._bfusd_product())
        if rates.get("RWUSD", Decimal("0")) > 0:
            filtered.append(self.earn.rwusd_product())
        self._product_list = filtered
        self._product_list_at = now
        return filtered

    def pick(self, products: list[FlexibleProduct] | None = None, margin_assets: set[str] | None = None) -> FlexibleProduct:
        products = products if products is not None else self.list_products()
        margin_assets = margin_assets if margin_assets is not None else self.margin_assets()
        candidates = [p for p in products if self._eligible(p, margin_assets)]
        if self._wallet_transfer_blocked:
            # 现货划不进统一账户时，只留申购后能用理财代币转入的产品（LDUSDT 这类）
            via_earn = [p for p in candidates if (p.margin_token or "").upper().startswith("LD")]
            if via_earn:
                candidates = via_earn
        if not candidates:
            raise RuntimeError("没有可作统一账户保证金、用来开 USDT/USDC 合约的活期")
        candidates.sort(key=lambda p: apr_ratio(p.apr), reverse=True)
        return candidates[0]

    def _collateral_rate_map(self) -> dict[str, Decimal]:
        try:
            rates = collateral_rates()
        except Exception:
            rates = {}
        return rates

    def _eligible(self, product: FlexibleProduct, margin_assets: set[str]) -> bool:
        del margin_assets
        if not product.purchasable:
            return False
        asset = (product.asset or "").upper()
        if asset in self.settings.asset_denylist or asset in self._runtime_denylist:
            return False
        if self.settings.asset_allowlist and asset not in self.settings.asset_allowlist:
            return False
        if not earns_as_um_margin(product, self._collateral_rate_map()):
            return False
        percent = apr_percent(product.apr)
        if percent < self.settings.min_apr:
            return False
        if self.settings.max_apr > 0 and percent > self.settings.max_apr:
            return False
        return True

    @staticmethod
    def _region_blocked_detail(detail: object) -> bool:
        text = str(detail or "")
        low = text.lower()
        return (
            "-9000" in text
            or "not available in your region" in low
            or "address verification" in low
            or "local regulations" in low
        )

    def _mark_asset_unavailable(self, asset: str, detail: object) -> bool:
        """地区/合规不可用时记入运行时黑名单，后续 pick/换年化跳过。"""
        if not self._region_blocked_detail(detail):
            return False
        name = (asset or "").upper()
        if not name:
            return False
        if name not in self._runtime_denylist:
            self._runtime_denylist.add(name)
            self._product_list = None
            if self._on_denylist_change is not None:
                try:
                    self._on_denylist_change()
                except Exception:
                    pass
        return True

    def _fallback_subscribe(self, failed: FlexibleProduct, amount: Decimal, fail_detail: object) -> list[StepResult]:
        """申购失败后改买次优可作保证金的产品（常见：RWUSD 地区限制 → BFUSD / USDT 活期）。"""
        self._mark_asset_unavailable(failed.asset, fail_detail)
        steps: list[StepResult] = []
        try:
            leftover = self.earn.spot_free(self.settings.source_asset)
        except BinanceAPIError:
            leftover = Decimal("0")
        buy_amt = leftover if leftover >= SPOT_MIN else amount
        if buy_amt < SPOT_MIN:
            return steps
        alt: FlexibleProduct | None = None
        try:
            alt = self.pick()
        except Exception:
            alt = None
        if alt is None or self._same_product(alt, failed):
            alt = self._best_usdt_flexible()
        if alt is None or self._same_product(alt, failed):
            return steps + [
                StepResult(
                    "改申购失败",
                    False,
                    f"{failed.asset} 不可用，也没有可替代的理财产品",
                )
            ]
        steps.append(
            StepResult(
                "改申购",
                True,
                f"{failed.asset} 不可用（{fail_detail}），改买 {alt.asset} 年化 {apr_percent(alt.apr):.2f}%",
            )
        )
        before = len(steps)
        steps.extend(self.buy(alt, buy_amt))
        if any(not s.ok for s in steps[before:]):
            # 再试 USDT 活期
            flex = self._best_usdt_flexible()
            if flex is not None and not self._same_product(flex, alt):
                try:
                    leftover = self.earn.spot_free(self.settings.source_asset)
                except BinanceAPIError:
                    leftover = buy_amt
                if leftover >= SPOT_MIN:
                    steps.append(
                        StepResult(
                            "再改申购 USDT 活期",
                            True,
                            f"{alt.asset} 仍失败，改买 USDT 活期",
                        )
                    )
                    steps.extend(self.buy(flex, leftover))
        return steps

    @staticmethod
    def _is_margin_asset(asset: str, margin_assets: set[str]) -> bool:
        asset = asset.upper()
        return asset in margin_assets or f"LD{asset}" in margin_assets

    def plan_amount(self, product: FlexibleProduct, sweep_all: bool = False) -> Decimal:
        free = self.spot_cash()
        if free < SPOT_MIN:
            return Decimal("0")
        if sweep_all or self.settings.cycle_sweep_all and self.settings.amount == 0:
            requested = free
            amount = free
        else:
            requested = self.settings.amount if self.settings.amount > 0 else free
            amount = min(requested, self.settings.max_amount, free)
        if sweep_all:
            amount = free
        if amount <= 0:
            return Decimal("0")
        if self.settings.source_asset == product.asset and amount < product.min_purchase:
            raise RuntimeError(
                f"金额 {fmt_amount(amount)} 低于最小申购额 {fmt_amount(product.min_purchase)} {product.asset}"
            )
        try:
            if product.kind in {"bfusd", "rwusd"}:
                quota = amount
            else:
                quota = self.earn.left_quota(product.product_id)
        except BinanceAPIError:
            quota = amount
        if quota <= 0:
            raise RuntimeError(f"{product.asset} 活期剩余额度不足")
        if self.settings.source_asset == product.asset:
            amount = min(amount, quota)
        if product.kind == "bfusd":
            room = self._bfusd_subscribe_room(product)
            if room is not None:
                amount = min(amount, room)
        return amount

    def buy(self, product: FlexibleProduct, amount: Decimal) -> list[StepResult]:
        steps: list[StepResult] = []
        subscribe_amount = amount
        if product.kind in {"bfusd", "rwusd"}:
            pay_asset = self.settings.source_asset
            subscribe_fn = self.earn.subscribe_bfusd if product.kind == "bfusd" else self.earn.subscribe_rwusd
            subscribe = self._mutate(
                f"申购 {product.asset}（用 {pay_asset}）数量={fmt_amount(subscribe_amount)}",
                lambda fn=subscribe_fn: fn(subscribe_amount, pay_asset),
            )
            if not subscribe.ok:
                self._mark_asset_unavailable(product.asset, subscribe.detail)
            steps.append(subscribe)
            steps.append(StepResult("subscribe_amount", True, subscribe_amount))
            return steps

        if self.settings.source_asset != product.asset:
            convert_result = self._mutate(
                f"兑换 {fmt_amount(amount)} {self.settings.source_asset} -> {product.asset}",
                lambda: self.convert_api.convert(self.settings.source_asset, product.asset, amount),
            )
            steps.append(convert_result)
            if convert_result.ok and not convert_result.dry_run:
                to_amount = (
                    convert_result.detail.get("quote", {}).get("toAmount")
                    if isinstance(convert_result.detail, dict)
                    else None
                )
                if to_amount:
                    subscribe_amount = d(to_amount)
        else:
            steps.append(StepResult("无需兑换申购资产", True, f"已持有 {product.asset}"))

        if subscribe_amount < product.min_purchase and not self.settings.dry_run:
            raise RuntimeError(f"申购数量 {fmt_amount(subscribe_amount)} 低于最小额 {fmt_amount(product.min_purchase)}")

        subscribe = self._mutate(
            f"申购活期 {product.asset} productId={product.product_id} 数量={fmt_amount(subscribe_amount)}",
            lambda: self.earn.subscribe(product.product_id, subscribe_amount, self.settings.source_account),
        )
        steps.append(subscribe)
        steps.append(StepResult("subscribe_amount", True, subscribe_amount))
        return steps

    def _best_usdt_flexible(self) -> FlexibleProduct | None:
        products = [p for p in self.list_products() if p.asset == "USDT" and p.kind != "bfusd"]
        eligible = [p for p in products if self._eligible(p, self.margin_assets())]
        if not eligible:
            return None
        eligible.sort(key=lambda p: apr_ratio(p.apr), reverse=True)
        return eligible[0]

    def margin_token_of(self, product: FlexibleProduct) -> str:
        if product.kind in {"bfusd", "rwusd"}:
            return product.asset.upper()
        return (product.margin_token or f"LD{product.asset}").upper()

    def plan_earn_target(self) -> tuple[FlexibleProduct | None, StepResult]:
        """收利平仓前预选：查好要兑/申购哪个理财，避免平完再查。"""
        try:
            product = self.pick()
        except Exception as exc:
            return None, StepResult("预选理财", False, str(exc))
        settle = settle_asset_of(self.settings.hedge_symbol).upper()
        source = self.settings.source_asset.upper()
        path: list[str] = []
        if settle != source:
            path.append(f"{settle}->{source}")
        if product.kind in {"bfusd", "rwusd"}:
            path.append(f"申购 {product.asset}")
        else:
            if source != product.asset.upper():
                path.append(f"{source}->{product.asset}")
            path.append(f"申购 {product.asset} 活期")
        token = self.margin_token_of(product)
        path.append(f"保证金 {token}")
        return product, StepResult(
            "预选理财",
            True,
            f"{' → '.join(path)}，年化 {apr_choice_text(product)}",
        )

    def _wait_spot_cash(self, asset: str | None = None, *, min_amt: Decimal = SPOT_MIN, timeout: float = 5.0) -> Decimal:
        """兑换/划转后余额可能滞后，短轮询再读现货。"""
        free = Decimal("0")
        deadline = time.monotonic() + max(0.0, timeout)
        while True:
            self.earn.invalidate()
            try:
                free = self.spot_cash(asset)
            except BinanceAPIError:
                free = Decimal("0")
            if free >= min_amt or time.monotonic() >= deadline:
                return free
            time.sleep(0.2)

    def sweep_spot_to_earn(self, product: FlexibleProduct | None = None) -> list[StepResult]:
        steps: list[StepResult] = []
        # USDC 本位收利后现货是 USDC：先换成 USDT，再走原来的理财申购
        settle = settle_asset_of(self.settings.hedge_symbol)
        if settle != self.settings.source_asset:
            try:
                free_settle = self.spot_cash(settle)
            except BinanceAPIError as exc:
                return [StepResult("归集理财", False, str(exc))]
            if free_settle >= SPOT_MIN:
                converted = self._mutate(
                    f"兑换 {fmt_amount(free_settle)} {settle} -> {self.settings.source_asset}（再申购理财）",
                    lambda amt=free_settle: self.convert_api.convert(settle, self.settings.source_asset, amt),
                )
                steps.append(converted)
                if not converted.ok:
                    return steps
                if not converted.dry_run:
                    free = self._wait_spot_cash(timeout=3.0)
                else:
                    free = free_settle
            else:
                free = Decimal("0")
        else:
            free = Decimal("0")
        if free < SPOT_MIN:
            try:
                # 划转刚到现货可能滞后；够了立刻返回，最多等 2 秒
                free = self._wait_spot_cash(timeout=2.0)
            except BinanceAPIError as exc:
                return steps + [StepResult("归集理财", False, str(exc))]
        if free < SPOT_MIN:
            if steps:
                steps.append(StepResult("归集理财", True, f"已换到 {self.settings.source_asset}，但可申购余额不足，跳过申购"))
                return steps
            return [StepResult("归集理财", True, "现货没有可申购余额，跳过申购")]
        fixed_product = product
        bought_any = False
        for _round in range(4):
            if free < SPOT_MIN:
                break
            if product is None or _round > 0:
                self._product_list = None
                try:
                    product = self.pick()
                except Exception as exc:
                    return steps + [StepResult("归集理财", False, str(exc))]
            amount = self.plan_amount(product, sweep_all=True)
            if amount < SPOT_MIN:
                if steps:
                    break
                return steps + [StepResult("归集理财", True, "现货没有可申购余额，跳过申购")]
            steps.append(
                StepResult(
                    "归集理财",
                    True,
                    f"把现货 {fmt_amount(amount)} {self.settings.source_asset} 申购 {product.asset} 年化 {apr_percent(product.apr)}%",
                )
            )
            before = len(steps)
            steps.extend(self.buy(product, amount))
            if any(not s.ok for s in steps[before:]) and product.kind in {"bfusd", "rwusd"}:
                fail = next((s for s in reversed(steps) if not s.ok), None)
                fb = len(steps)
                steps.extend(self._fallback_subscribe(product, amount, fail.detail if fail else "未知"))
                if fb < len(steps) and all(s.ok for s in steps[fb:]):
                    for s in steps[before:fb]:
                        if not s.ok:
                            s.ok = True
                            s.detail = f"失败后已改申购：{s.detail}"
            if any(not s.ok for s in steps[before:]):
                return steps
            bought_any = True
            # 调用方指定了产品时只买这一笔；自动归集则额度用满后继续填下一档
            if fixed_product is not None or amount + SPOT_MIN >= free:
                break
            self.earn.invalidate()
            self._product_list = None
            try:
                free = self.spot_cash()
            except BinanceAPIError as exc:
                return steps + [StepResult("归集理财", False, str(exc))]
            product = None
        if not bought_any and free >= SPOT_MIN:
            return steps + [StepResult("归集理财", True, "现货没有可申购余额，跳过申购")]
        return steps

    def earn_holdings(self) -> list[tuple[FlexibleProduct, Decimal]]:
        holdings: list[tuple[FlexibleProduct, Decimal]] = []
        try:
            rows = self.earn.positions()
        except BinanceAPIError:
            rows = []
        for row in rows:
            amount = d(row.get("totalAmount") or row.get("amount") or row.get("redeemableAmount"))
            if amount <= 0:
                continue
            product = FlexibleProduct.from_row(row)
            if not product.product_id or product.asset in {"BFUSD", "RWUSD"}:
                continue
            if not product.margin_token:
                product.margin_token = f"LD{product.asset}"
            product.kind = "flexible"
            holdings.append((product, amount))
        bfusd = max(self.earn.bfusd_balance(), self.earn.spot_free("BFUSD"))
        if bfusd > 0:
            holdings.append((self._bfusd_product(), bfusd))
        rwusd = max(self.earn.rwusd_balance(), self.earn.spot_free("RWUSD"))
        if rwusd > 0:
            holdings.append((self.earn.rwusd_product(), rwusd))
        return self._overlay_pm_balances(holdings)

    def _pm_balances(self) -> dict[str, Decimal]:
        try:
            return self.futures.papi_balances()
        except BinanceAPIError:
            return {}

    def _overlay_pm_balances(
        self, holdings: list[tuple[FlexibleProduct, Decimal]]
    ) -> list[tuple[FlexibleProduct, Decimal]]:
        """转入统一账户后，活期接口经常变成 0，余额在 LDUSDT / BFUSD / RWUSD 上。"""
        pm = self._pm_balances()

        def split(asset: str, kind: str) -> tuple[list[tuple[FlexibleProduct, Decimal]], Decimal]:
            kept: list[tuple[FlexibleProduct, Decimal]] = []
            total = Decimal("0")
            for product, amount in holdings:
                if product.asset == asset and product.kind == kind:
                    total += amount
                else:
                    kept.append((product, amount))
            return kept, total

        holdings, usdt_amt = split("USDT", "flexible")
        ld = pm.get("LDUSDT", Decimal("0"))
        # 理财钱包的 USDT 活期（现货 LDUSDT）和统一账户里的 LDUSDT 是两笔。
        # 用 max 合并时，PM 数量更大就会把整笔标成保证金，可转出为 0，钱包仓再也进不了换年化。
        # 活期接口在全部划入统一账户后通常是 0；现货没有 LD 时不要把接口余额再算一遍。
        try:
            spot_ld = self.earn.spot_free("LDUSDT")
        except BinanceAPIError:
            spot_ld = Decimal("0")
        flex = self._usdt_flex_product()
        if usdt_amt > 0 and (ld <= 0 or spot_ld >= SPOT_MIN):
            holdings.append((flex, usdt_amt))
        if ld > 0:
            holdings.append((
                replace(flex, margin_token="LDUSDT", raw={**flex.raw, "located": "pm"}),
                ld,
            ))

        holdings, bfusd_amt = split("BFUSD", "bfusd")
        bfusd_show = max(bfusd_amt, pm.get("BFUSD", Decimal("0")))
        if bfusd_show > 0:
            product = self._bfusd_product()
            if pm.get("BFUSD", Decimal("0")) >= bfusd_amt and pm.get("BFUSD", Decimal("0")) > 0:
                product = replace(product, raw={**product.raw, "located": "pm"})
            holdings.append((product, bfusd_show))

        holdings, rw_amt = split("RWUSD", "rwusd")
        rw_show = max(rw_amt, pm.get("RWUSD", Decimal("0")))
        if rw_show > 0:
            product = self.earn.rwusd_product()
            if pm.get("RWUSD", Decimal("0")) >= rw_amt and pm.get("RWUSD", Decimal("0")) > 0:
                product = replace(product, raw={**product.raw, "located": "pm"})
            holdings.append((product, rw_show))
        return holdings

    def _usdt_flex_product(self) -> FlexibleProduct:
        for product in self.list_products():
            if product.asset == "USDT" and product.kind == "flexible":
                return product
        return FlexibleProduct(
            product_id="USDT001",
            asset="USDT",
            apr=Decimal("0"),
            can_purchase=True,
            can_redeem=True,
            is_sold_out=False,
            min_purchase=Decimal("0.1"),
            status="PURCHASING",
            hot=False,
            raw={},
            kind="flexible",
            margin_token="LDUSDT",
        )

    def wallet_view(self) -> dict:
        spot = self.earn.spot_free("USDT")
        holdings_raw = self.earn_holdings()
        usdt_flex = Decimal("0")
        bfusd = Decimal("0")
        other = Decimal("0")
        flex_label = "USDT 活期"
        holdings = []
        for product, amount in holdings_raw:
            if product.kind == "bfusd":
                bfusd += amount
                label = "BFUSD"
            elif product.kind == "rwusd":
                other += amount
                label = "RWUSD"
            elif product.asset == "USDT":
                usdt_flex += amount
                label = "LDUSDT" if product.raw.get("located") == "pm" else "USDT 活期"
                flex_label = label
            else:
                other += amount
                label = product.margin_token or product.asset
            holdings.append(
                {
                    "label": label,
                    "asset": product.asset,
                    "kind": product.kind,
                    "amount": fmt_amount(amount, 4),
                    "apr": str(apr_percent(product.apr)),
                    "product_id": product.product_id,
                }
            )
        next_buy = None
        if spot >= SPOT_MIN:
            try:
                target = self.pick()
                next_buy = {
                    "asset": target.asset,
                    "apr": str(apr_percent(target.apr)),
                    "kind": target.asset if target.kind in {"bfusd", "rwusd"} else f"{target.asset} 活期",
                    "amount": fmt_amount(spot, 4),
                }
            except Exception:
                next_buy = None
        if usdt_flex + bfusd + other <= 0 and spot > 0:
            status = f"还没理财。{fmt_amount(spot, 2)} USDT 在现货闲着，不会生息。"
        elif spot > 0:
            status = f"已有理财仓位，现货还闲着 {fmt_amount(spot, 2)} USDT，可再申购。"
        else:
            status = "现货已扫完，保证金主要是理财仓位。"
        try:
            yday = self.earn.yesterday_earn_reward()
        except Exception as exc:
            yday = {"amount": Decimal("0"), "text": "0", "source": "error", "errors": [str(exc)]}
        yday_amt = yday.get("amount") or Decimal("0")
        yday_errs = yday.get("errors") or []
        return {
            "spot_usdt": fmt_amount(spot, 4),
            "usdt_flexible": fmt_amount(usdt_flex, 4),
            "usdt_flexible_label": flex_label,
            "bfusd": fmt_amount(bfusd, 4),
            "earn_total": fmt_amount(usdt_flex + bfusd + other, 4),
            "earn_yesterday": fmt_amount(yday_amt, 4) if yday_amt > 0 else "0",
            "earn_yesterday_source": str(yday.get("source") or "none"),
            "earn_yesterday_day": str(yday.get("day") or ""),
            "earn_yesterday_error": "; ".join(str(x) for x in yday_errs[:3]) if yday_errs else "",
            "holdings": holdings,
            "next_buy": next_buy,
            "status": status,
        }

    def margin_status(self, *, equity: Decimal | None = None) -> dict:
        spot = self.earn.spot_free("USDT")
        holdings = self.earn_holdings()
        usdt_flex = sum((amt for p, amt in holdings if p.asset == "USDT" and p.kind == "flexible"), Decimal("0"))
        bfusd = sum((amt for p, amt in holdings if p.kind == "bfusd"), Decimal("0"))
        try:
            rates = self.futures.collateral_rates()
        except BinanceAPIError:
            rates = {}
        try:
            pm = self.futures.papi_balances()
        except BinanceAPIError:
            pm = {}
        if equity is None:
            try:
                equity = self.futures.account_equity() if self.settings.unified_account else Decimal("0")
            except BinanceAPIError:
                equity = Decimal("0")
        try:
            ld_free = self.futures.earn_to_pm_balance("LDUSDT")
        except BinanceAPIError:
            ld_free = Decimal("0")

        def rate_of(*names: str) -> Decimal | None:
            for name in names:
                if name in rates:
                    return rates[name]
            return None

        rows = []
        need_move = False
        usdt_in_pm = pm.get("USDT", Decimal("0"))
        bfusd_in_pm = pm.get("BFUSD", Decimal("0")) + pm.get("LDUSDT", Decimal("0"))
        ld_in_pm = pm.get("LDUSDT", Decimal("0"))

        usdt_rate = rate_of("USDT")
        spot_ok = usdt_in_pm >= spot * Decimal("0.5") if spot > 0 else usdt_in_pm > 0 or equity > 0
        if spot > 0 and usdt_in_pm + equity <= 0:
            spot_ok = False
        if spot > 0 and equity <= 0 and usdt_in_pm <= 0:
            need_move = True
            spot_note = "现货 USDT 还没进统一账户，不能开合约。请划入统一账户（全仓），不是划到经典 U 本位。"
        elif usdt_rate is None:
            spot_note = "USDT 在抵押名单里查不到，先看统一账户权益。"
        else:
            spot_note = f"USDT 抵押率 {usdt_rate}，现货余额在统一账户开通后一般直接算保证金，不用划到经典合约。"
            if equity > 0:
                spot_ok = True
        rows.append(
            {
                "name": "现货 USDT",
                "amount": fmt_amount(spot, 4),
                "in_pm": fmt_amount(usdt_in_pm, 4),
                "rate": fmt_amount(usdt_rate, 4) if usdt_rate is not None else "-",
                "ok": spot_ok and spot > 0 or (spot <= 0 and equity > 0),
                "note": spot_note if spot > 0 else "现货没有 USDT",
            }
        )

        flex_rate = rate_of("LDUSDT", "USDT")
        flex_ok = ld_in_pm > 0 or (usdt_flex <= 0)
        if usdt_flex > 0 and ld_free > 0:
            need_move = True
            flex_ok = False
            flex_note = f"USDT 活期对应 LDUSDT，可转入统一账户 {fmt_amount(ld_free, 4)}。不是现货划转到经典合约。"
        elif usdt_flex > 0 and ld_in_pm <= 0 and equity <= 0:
            need_move = True
            flex_ok = False
            flex_note = "活期仓还没被统一账户算进保证金，需要理财→统一账户。"
        elif usdt_flex > 0:
            flex_ok = True
            flex_note = "USDT 活期（LDUSDT）可作为统一账户保证金。"
        else:
            flex_note = "还没买 USDT 活期。"
        rows.append(
            {
                "name": "USDT 活期 / LDUSDT",
                "amount": fmt_amount(usdt_flex, 4),
                "in_pm": fmt_amount(ld_in_pm, 4),
                "rate": fmt_amount(flex_rate, 4) if flex_rate is not None else "-",
                "ok": flex_ok,
                "note": flex_note,
            }
        )

        bfusd_rate = rate_of("BFUSD")
        if bfusd > 0 and bfusd_in_pm <= 0 and equity <= 0:
            need_move = True
            bfusd_ok = False
            bfusd_note = "BFUSD 买在理财/现货里时，有的账户不会自动当保证金，要转入统一账户。"
        elif bfusd > 0 and bfusd_rate is None:
            need_move = True
            bfusd_ok = False
            bfusd_note = "当前统一账户抵押名单里没有 BFUSD，不能直接当保证金。"
        elif bfusd > 0:
            bfusd_ok = True
            bfusd_note = f"BFUSD 抵押率 {bfusd_rate if bfusd_rate is not None else '-'}，已可作为保证金。"
        else:
            bfusd_ok = True
            bfusd_note = "还没买 BFUSD。"
        rows.append(
            {
                "name": "BFUSD",
                "amount": fmt_amount(bfusd, 4),
                "in_pm": fmt_amount(pm.get("BFUSD", Decimal("0")), 4),
                "rate": fmt_amount(bfusd_rate, 4) if bfusd_rate is not None else "-",
                "ok": bfusd_ok,
                "note": bfusd_note,
            }
        )

        rwusd = sum((amt for p, amt in holdings if p.kind == "rwusd"), Decimal("0"))
        rw_in_pm = pm.get("RWUSD", Decimal("0"))
        rw_rate = rate_of("RWUSD")
        if rwusd > 0:
            rows.append(
                {
                    "name": "RWUSD",
                    "amount": fmt_amount(rwusd, 4),
                    "in_pm": fmt_amount(rw_in_pm, 4),
                    "rate": fmt_amount(rw_rate, 4) if rw_rate is not None else "-",
                    "ok": rw_in_pm > 0 or equity > 0,
                    "note": "RWUSD 可作为统一账户保证金。" if rwusd > 0 else "还没买 RWUSD。",
                }
            )

        if equity > 0:
            summary = f"统一账户权益约 {fmt_amount(equity, 4)} USDT，这部分已经能开合约。"
        else:
            summary = "统一账户权益还是 0。现货/理财还没算进保证金，开对冲会失败。不要划到经典 U 本位合约。"
        return {
            "equity": fmt_amount(equity, 4),
            "need_move": need_move or equity <= 0,
            "can_click": need_move or (spot > 0 and equity <= 0) or ld_free > 0,
            "summary": summary,
            "rows": rows,
            "ld_transferable": fmt_amount(ld_free, 4),
        }

    def fund_unified(
        self, *, check_ldusdt: bool = True, allow_redeem_fallback: bool = False
    ) -> list[StepResult]:
        if not self.settings.unified_account:
            return [StepResult("保证金", False, "当前配置不是统一账户")]
        steps: list[StepResult] = []
        spot = self.spot_cash("USDT")
        if spot >= SPOT_MIN:
            steps.extend(self._fund_spot_asset("USDT", spot))
        usdc_spot = self.spot_cash("USDC")
        if usdc_spot >= SPOT_MIN:
            steps.extend(self._fund_spot_asset("USDC", usdc_spot))
        bfusd_spot = self.spot_cash("BFUSD")
        if bfusd_spot >= SPOT_MIN:
            steps.extend(
                self._fund_spot_asset("BFUSD", bfusd_spot, allow_redeem_fallback=allow_redeem_fallback)
            )
        rwusd_spot = self.spot_cash("RWUSD")
        if rwusd_spot >= SPOT_MIN:
            steps.extend(
                self._fund_spot_asset("RWUSD", rwusd_spot, allow_redeem_fallback=allow_redeem_fallback)
            )
        already_ld = any("LDUSDT" in s.name and s.ok for s in steps)
        if check_ldusdt and not already_ld:
            try:
                ld_free = self.futures.earn_to_pm_balance("LDUSDT")
            except BinanceAPIError as exc:
                ld_free = Decimal("0")
                steps.append(StepResult("查询 LDUSDT", True, str(exc)))
            if ld_free > 0:
                steps.append(
                    self._mutate(
                        f"USDT 活期 LDUSDT {fmt_amount(ld_free)} 转入统一账户",
                        lambda: self.futures.earn_to_pm("LDUSDT", ld_free),
                    )
                )
                if steps[-1].ok and not steps[-1].dry_run:
                    self.earn.invalidate()
        if not steps:
            return [StepResult("无需划转", True, "现货没有可划入统一账户的余额")]
        return steps

    def _fund_spot_asset(
        self, asset: str, amount: Decimal, *, allow_redeem_fallback: bool = False
    ) -> list[StepResult]:
        if amount < SPOT_MIN:
            return [
                StepResult(
                    f"{asset} 灰尘跳过",
                    True,
                    f"{asset} {fmt_amount(amount)} < {fmt_amount(SPOT_MIN)}，忽略",
                )
            ]
        if self._wallet_transfer_blocked:
            return self._spot_via_ldusdt(asset, amount)
        # BFUSD / RWUSD 不能走 MAIN_PORTFOLIO_MARGIN（会 -3027），优先用理财代币转入
        if asset in {"BFUSD", "RWUSD"}:
            return self._fund_earn_token(asset, amount, allow_redeem_fallback=allow_redeem_fallback)
        step = self._move_spot(asset, amount)
        if step.ok:
            return [step]
        if not self._unauthorized(step):
            return [step]
        self._wallet_transfer_blocked = True
        note = StepResult(
            "现货无法划转",
            True,
            "API 没有现货↔统一账户划转权限（-1002）。不划 U 本位，改用 USDT 活期 LDUSDT 入金。",
        )
        return [note, *self._spot_via_ldusdt(asset, amount)]

    def _fund_earn_token(
        self, asset: str, amount: Decimal, *, allow_redeem_fallback: bool = False
    ) -> list[StepResult]:
        """BFUSD / RWUSD：先试 EARN_TO_FUTURE；默认不赎回改低年化（避免 RWUSD 地区失败后 BFUSD↔USDT 空转）。"""
        steps: list[StepResult] = []
        if amount < SPOT_MIN:
            return [
                StepResult(
                    f"{asset} 灰尘跳过",
                    True,
                    f"{asset} {fmt_amount(amount)} < {fmt_amount(SPOT_MIN)}，不划转不赎回",
                )
            ]
        try:
            cap = self.futures.earn_to_pm_balance(asset)
        except BinanceAPIError as exc:
            # -1100 等查询异常时仍按持仓量尝试转入，避免误走赎回
            cap = amount
            steps.append(StepResult(f"查询 {asset} 可转入", True, str(exc)))
        move = min(amount, cap) if cap > 0 else amount
        move = move.quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)
        if move >= SPOT_MIN:
            step = self._mutate(
                f"{asset} {fmt_amount(move)} 从理财转入统一账户",
                lambda qty=move: self.futures.earn_to_pm(asset, qty),
            )
            steps.append(step)
            if step.ok and not step.dry_run:
                self.earn.invalidate()
            if step.ok:
                leftover = self.spot_cash(asset)
                if leftover < SPOT_MIN:
                    return steps
                amount = leftover
            else:
                # 转入失败：默认留着继续吃利息，不赎回砸成 USDT 活期
                if not allow_redeem_fallback:
                    steps.append(
                        StepResult(
                            f"{asset} 暂留",
                            True,
                            f"{asset} 暂不能入统一账户，留在理财/现货继续计息（不赎回改低年化）",
                        )
                    )
                    return steps
        if not allow_redeem_fallback:
            steps.append(
                StepResult(
                    f"{asset} 暂留",
                    True,
                    f"{asset} 未转入统一账户，留着计息（禁止赎回空转）",
                )
            )
            return steps
        # 仅入场急需保证金时：赎回成稳定币再走 LDUSDT
        note = StepResult(
            f"{asset} 改走赎回",
            True,
            f"现货 {asset} 不能直接划入统一账户（-3027），先赎回再申购 USDT 活期入金。",
        )
        via = self._spot_via_ldusdt(asset, amount)
        if any(not s.ok for s in via):
            via = [
                s
                if s.ok
                else StepResult(
                    s.name,
                    True,
                    f"{s.detail}（{asset} 暂留现货，对冲已齐，下次再入统一账户）",
                )
                for s in via
            ]
        return [*steps, note, *via]

    def _spot_via_ldusdt(self, asset: str, amount: Decimal) -> list[StepResult]:
        steps: list[StepResult] = []
        if asset in {"BFUSD", "RWUSD"}:
            redeem_fn = self.earn.redeem_bfusd if asset == "BFUSD" else self.earn.redeem_rwusd
            redeem = self._mutate(
                f"赎回 {asset} {fmt_amount(amount)}（FAST，回到现货）",
                lambda fn=redeem_fn: fn(amount, "FAST"),
            )
            steps.append(redeem)
            if not redeem.ok:
                return steps
            if not self.settings.dry_run:
                time.sleep(max(int(self.settings.settle_seconds or 0), 3))
                self.earn.invalidate()
                converted = self._convert_usdc_proceeds()
                if converted is not None:
                    steps.append(converted)
                    if not converted.ok:
                        return steps
            amount = self.spot_cash("USDT")
        steps.extend(self._subscribe_usdt_and_move_ld(amount))
        return steps

    def _subscribe_usdt_and_move_ld(self, amount: Decimal) -> list[StepResult]:
        amount = min(amount, self.spot_cash("USDT"))
        if amount < SPOT_MIN:
            return [StepResult("LDUSDT 入金", False, "现货 USDT 不足 1，无法走活期入金")]
        alt = self._best_usdt_flexible()
        if alt is None:
            return [StepResult("申购 USDT 活期", False, "没有可申购的 USDT 活期产品")]
        steps = list(self.buy(alt, amount))
        if any(not s.ok for s in steps):
            return steps
        if not self.settings.dry_run:
            time.sleep(max(int(self.settings.settle_seconds or 0), 3))
            self.earn.invalidate()
        try:
            ld_free = self.futures.earn_to_pm_balance("LDUSDT")
        except BinanceAPIError as exc:
            return steps + [StepResult("查询 LDUSDT", False, str(exc))]
        if ld_free <= 0:
            return steps + [StepResult("LDUSDT 入金", False, "申购后还查不到可转入的 LDUSDT，稍后再点一次入场")]
        move = self._mutate(
            f"USDT 活期 LDUSDT {fmt_amount(ld_free)} 转入统一账户",
            lambda: self.futures.earn_to_pm("LDUSDT", ld_free),
        )
        if move.ok and not move.dry_run:
            self.earn.invalidate()
        steps.append(move)
        return steps

    def _move_spot(self, asset: str, amount: Decimal) -> StepResult:
        step = self._mutate(
            f"现货 {asset} {fmt_amount(amount)} 划入统一账户全仓（不是 U 本位合约）",
            lambda: self.futures.spot_to_unified(asset, amount),
        )
        if step.ok and not step.dry_run:
            self.earn.invalidate()
        return step

    def _papi_wallet_balance(self, asset: str) -> Decimal:
        """统一账户钱包余额（含负数）；papi_balances 会滤掉 ≤0。"""
        asset = asset.upper()
        try:
            data = self.client.signed("GET", "/papi/v1/balance", papi=True)
        except BinanceAPIError:
            return Decimal("0")
        rows = data if isinstance(data, list) else (data.get("balances") or data.get("data") or [])
        for row in rows:
            if str(row.get("asset") or "").upper() == asset:
                total = d(row.get("totalWalletBalance"))
                if total == 0 and "crossMarginFree" in row:
                    total = d(row.get("crossMarginAsset")) + d(row.get("umWalletBalance")) + d(
                        row.get("cmWalletBalance")
                    )
                return total
        return Decimal("0")

    def _repair_pm_negative_settle(self) -> list[StepResult]:
        """无对冲仓时：用现货稳定币填上统一账户结算币负余额，否则 BFUSD 等 maxWithdraw 会被卡住。"""
        settle = settle_asset_of(self.settings.hedge_symbol).upper()
        bal = self._papi_wallet_balance(settle)
        if bal >= -SPOT_MIN:
            return []
        need = (-bal).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        if need < SPOT_MIN:
            return []
        steps = [
            StepResult(
                "填补负余额",
                True,
                f"统一账户 {settle}={fmt_amount(bal)}，无对冲仓，用现货填约 {fmt_amount(need)} 以便转出理财代币",
            )
        ]
        # 1) 现货已有结算币
        try:
            spot_settle = self.spot_cash(settle)
        except BinanceAPIError:
            spot_settle = Decimal("0")
        if spot_settle >= SPOT_MIN:
            move = min(spot_settle, need)
            fund = self._fund_spot_asset(settle, move)
            steps.extend(fund)
            if any(s.ok and not s.dry_run for s in fund):
                need = (need - move).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        # 2) 现货 USDT 兑成结算币再划入
        if need >= SPOT_MIN and settle != (self.settings.source_asset or "USDT").upper():
            try:
                spot_src = self.spot_cash(self.settings.source_asset)
            except BinanceAPIError:
                spot_src = Decimal("0")
            if spot_src >= SPOT_MIN:
                conv_amt = min(spot_src, need)
                converted = self._mutate(
                    f"兑换 {fmt_amount(conv_amt)} {self.settings.source_asset} -> {settle}（填负余额）",
                    lambda amt=conv_amt: self.convert_api.convert(
                        self.settings.source_asset, settle, amt
                    ),
                )
                steps.append(converted)
                if converted.ok and not converted.dry_run:
                    self.earn.invalidate()
                    to_amt = conv_amt
                    if isinstance(converted.detail, dict):
                        raw_to = (converted.detail.get("quote") or {}).get("toAmount")
                        if raw_to:
                            to_amt = d(raw_to)
                    fund = self._fund_spot_asset(settle, to_amt)
                    steps.extend(fund)
        after = self._papi_wallet_balance(settle)
        steps.append(
            StepResult(
                "负余额结果",
                True,
                f"{settle} 现为 {fmt_amount(after)}（填前 {fmt_amount(bal)}）",
            )
        )
        return steps

    def _return_pm_earn(
        self, asset: str, amount: Decimal, *, has_hedge: bool = True
    ) -> tuple[list[StepResult], Decimal]:
        """按币安 FUTURE_TO_EARN 可转上限转回。总余额大于这个上限时，多转会 -3020。"""
        try:
            cap = self.futures.earn_asset_transferable(asset, "FUTURE_TO_EARN")
        except BinanceAPIError as exc:
            text = str(exc)
            # BFUSD/RWUSD 等不走 earn-asset-transfer（会 -1100），交给现货划转路径
            if "-1100" in text or "illegal characters" in text.lower():
                return [
                    StepResult(
                        f"{asset} 不走理财划转",
                        True,
                        f"接口不支持 FUTURE_TO_EARN（{exc}），改从统一账户转现货再赎回",
                    )
                ], Decimal("0")
            return [StepResult(f"查询 {asset} 可转出", False, str(exc))], Decimal("0")
        move = amount if amount > 0 else cap
        if move > cap:
            move = cap
        move = move.quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)
        if move < SPOT_MIN:
            if has_hedge:
                return [
                    StepResult(
                        "暂停换产品",
                        True,
                        f"统一账户 {asset} 现在最多能转回理财 {fmt_amount(cap, 8)}。"
                        "有对冲仓时其余要留作保证金，这次不转。",
                    )
                ], Decimal("0")
            # 无仓：PM 侧可转=0 多半是代币已在理财钱包，继续赎回即可
            return [
                StepResult(
                    f"统一账户无 {asset} 可转回",
                    True,
                    f"FUTURE_TO_EARN={fmt_amount(cap, 8)}，无对冲仓，按理财侧余额继续赎回",
                )
            ], Decimal("0")
        step = self._mutate(
            f"统一账户 {asset} {fmt_amount(move)} 转回理财，准备换产品",
            lambda qty=move: self.futures.pm_to_earn(asset, qty),
        )
        if step.ok and not step.dry_run:
            time.sleep(max(int(self.settings.settle_seconds or 0), 3))
            self.earn.invalidate()
        if not step.ok:
            return [step], Decimal("0")
        return [step, StepResult("转回数量", True, move)], move

    def _pull_pm_asset_to_spot(self, asset: str, want: Decimal) -> tuple[list[StepResult], Decimal]:
        """BFUSD/RWUSD 在统一账户里时，earn-asset-transfer 不可用：走 maxWithdraw → 现货再赎回。"""
        asset = asset.upper()
        steps: list[StepResult] = []
        try:
            cap = self.futures.max_withdraw(asset)
        except BinanceAPIError as exc:
            return [StepResult(f"查询 {asset} 可转出现货", False, str(exc))], Decimal("0")
        move = min(want if want > 0 else cap, cap)
        move = move.quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)
        if move < SPOT_MIN:
            return [
                StepResult(
                    f"统一账户 {asset} 暂不可转出现货",
                    True,
                    f"maxWithdraw={fmt_amount(cap, 8)}（若结算币有负余额会卡住，无仓时应先填补）",
                )
            ], Decimal("0")
        step = self._mutate(
            f"统一账户 {asset} {fmt_amount(move)} 转出现货，准备赎回",
            lambda qty=move: self.futures.unified_to_spot(asset, qty),
        )
        steps.append(step)
        if not step.ok:
            return steps, Decimal("0")
        if not step.dry_run:
            self.earn.invalidate()
            time.sleep(max(int(self.settings.settle_seconds or 0), 1))
        return steps, move

    def _convert_usdc_proceeds(self) -> StepResult | None:
        """RWUSD 赎回有时回到 USDC。申购入口用的是 source_asset，先换过去。"""
        return self._convert_spot_to_source("USDC")

    def _convert_spot_to_source(self, asset: str) -> StepResult | None:
        """把现货里的某资产兑成申购用的 source_asset（默认 USDT）。"""
        asset = (asset or "").upper()
        target = (self.settings.source_asset or "USDT").upper()
        if not asset or asset == target or self.settings.dry_run:
            return None
        try:
            free = self.earn.spot_free(asset)
        except BinanceAPIError:
            return None
        if free <= 0:
            return None
        # 低于 Convert 下限（常 0.01）直接跳过，避免 345233 打断换年化
        if free < CONVERT_MIN:
            return StepResult(
                "兑换跳过",
                True,
                f"{asset} {fmt_amount(free)} < {fmt_amount(CONVERT_MIN)}，低于币安兑换下限，留着灰尘",
            )
        step = self._mutate(
            f"兑换 {fmt_amount(free)} {asset} -> {target}",
            lambda amt=free, a=asset, t=target: self.convert_api.convert(a, t, amt),
        )
        if step.ok and not step.dry_run:
            self.earn.invalidate()
        return step

    @staticmethod
    def _is_stable_earn_asset(product: FlexibleProduct) -> bool:
        if product.kind in {"bfusd", "rwusd"}:
            return True
        return product.asset.upper() in {
            "USDT",
            "USDC",
            "BFUSD",
            "RWUSD",
            "FDUSD",
            "TUSD",
            "BUSD",
            "USD1",
            "USDE",
        }

    @staticmethod
    def _unauthorized(step: StepResult) -> bool:
        text = str(step.detail or "").lower()
        return "-1002" in text or "not authorized" in text

    def _switch_sources(
        self,
        target: FlexibleProduct,
        holdings: list[tuple[FlexibleProduct, Decimal]] | None = None,
        *,
        stable_only: bool = False,
    ) -> list[tuple[FlexibleProduct, Decimal]]:
        """低于目标年化、可赎回的仓位。stable_only 时只动稳定币理财（自动换年化用）。"""
        holdings = holdings if holdings is not None else self.earn_holdings()
        gap = d(getattr(self.settings, "switch_min_apr_gap", None) or "0.1")
        if gap < 0:
            gap = Decimal("0")
        target_pct = apr_percent(target.apr)
        stable = {"USDT", "USDC", "BFUSD", "RWUSD", "FDUSD", "TUSD", "BUSD", "USD1", "USDE"}
        out: list[tuple[FlexibleProduct, Decimal]] = []
        for product, amount in holdings:
            if amount <= 0 or self._same_product(product, target):
                continue
            if stable_only and product.kind not in {"bfusd", "rwusd"} and product.asset.upper() not in stable:
                continue
            src_pct = apr_percent(product.apr)
            # 源年化未知/为 0 也视为可换；要比目标低出 gap 个百分点才动
            if product.kind == "bfusd":
                keep = self._bfusd_keep_amount(product, amount, target)
                amount = amount - keep
                if amount <= 0:
                    continue
            elif src_pct > 0 and src_pct + gap >= target_pct:
                continue
            out.append((product, amount))
        return out

    def switch_advice(self, *, mmr: Decimal | None = None, equity: Decimal | None = None) -> dict:
        holdings = self.earn_holdings()
        try:
            moves = self._allocation_moves(holdings)
            target = moves[0][2] if moves else self.pick()
        except Exception as exc:
            return {"needed": False, "can_click": False, "reason": str(exc)}
        if mmr is None or equity is None:
            try:
                got_mmr, got_equity = self._mmr_equity() if self.settings.unified_account else (Decimal("0"), Decimal("0"))
            except BinanceAPIError as exc:
                return {"needed": False, "can_click": False, "reason": str(exc)}
            if mmr is None:
                mmr = got_mmr
            if equity is None:
                equity = got_equity
        safe = self.settings.switch_safe_uni_mmr
        leftover = sum((amt for _src, amt, _dst, _note in moves), Decimal("0"))
        if leftover <= 0:
            if not holdings:
                return {
                    "needed": False,
                    "can_click": False,
                    "target": target.asset,
                    "target_apr": str(apr_percent(target.apr)),
                    "current_mmr": fmt_amount(mmr, 4),
                    "safe_mmr": fmt_amount(safe, 4),
                    "next_batch": "0",
                    "remaining": "0",
                    "reason": (
                        f"还没有理财仓位，钱还在现货。"
                        f"点「换年化」会按各产品额度申购，当前优先 {target.asset}，年化 {apr_choice_text(target)}"
                    ),
                }
            stuck: list[str] = []
            for product, amount in holdings:
                if amount < SPOT_MIN:
                    continue
                movable = self._movable_holding_amount(product, amount)
                if movable >= SPOT_MIN:
                    continue
                # 已经在最高年化上、只是可转出为 0：那是保证金，不是还没换的低收益仓
                if target is not None and self._alloc_key(product) == self._alloc_key(target):
                    continue
                stuck.append(product.asset)
            if stuck:
                reason = (
                    f"{'/'.join(stuck)} 在统一账户里当保证金，当前可转回不足 1，先不动"
                )
            else:
                reason = "已经按各产品自己的额度配在更高年化上，不用换"
            return {
                "needed": False,
                "can_click": False,
                "target": target.asset,
                "target_apr": str(apr_percent(target.apr)),
                "current_mmr": fmt_amount(mmr, 4),
                "safe_mmr": fmt_amount(safe, 4),
                "next_batch": "0",
                "remaining": "0",
                "reason": reason,
            }
        try:
            legs = self.futures.legs(self.settings.hedge_symbol)
            hedge_open = legs.long_qty > 0 or legs.short_qty > 0
        except Exception:
            hedge_open = True
        next_batch = self._next_switch_batch(
            leftover, mmr, equity, full=True, hedge_open=hedge_open
        )
        can = next_batch > 0
        if not is_mmr_sentinel(mmr) and mmr < safe:
            reason = f"当前 uniMMR={fmt_amount(mmr, 4)} 低于安全线 {fmt_amount(safe, 4)}，先不要赎回换产品"
        elif next_batch <= 0:
            reason = "这批按 uniMMR 估出来能赎的数量太小，先不要动"
        else:
            plan = "；".join(note for _src, _amt, _dst, note in moves[:4])
            if next_batch >= leftover:
                batch_hint = f"当前空间够一次换完约 {fmt_amount(leftover, 2)} USDT"
            else:
                batch_hint = (
                    f"当前空间本批约 {fmt_amount(next_batch, 2)} / 剩余 {fmt_amount(leftover, 2)} USDT，"
                    f"会按赎回后最新 uniMMR 继续下一批"
                )
            reason = (
                f"按各产品额度凑更高收益：{plan}。"
                f"{batch_hint}（目标 uniMMR ≥ {fmt_amount(safe, 4)}）。"
                f"点一次会按实际情况自动分批，换完或碰到安全线才停。"
            )
        return {
            "needed": True,
            "can_click": can,
            "target": target.asset,
            "target_apr": str(apr_percent(target.apr)),
            "current_mmr": fmt_amount(mmr, 4),
            "safe_mmr": fmt_amount(safe, 4),
            "next_batch": fmt_amount(next_batch, 2),
            "remaining": fmt_amount(leftover, 2),
            "reason": reason,
        }

    def _apr_comparison(self) -> str:
        ranked = [p for p in self.list_products() if self._eligible(p, self.margin_assets())]
        ranked.sort(key=lambda p: apr_ratio(p.apr), reverse=True)
        if not ranked:
            return "没有可比较的产品"
        return "、".join(f"{p.asset} {apr_choice_text(p)}" for p in ranked[:5])

    def switch_to_best(self, *, full: bool = False, has_hedge: bool | None = None) -> list[StepResult]:
        """把非目标活期换成当前最高年化产品。

        full=True：换年化/入场用。无仓时尽量一次赎完；有仓时仍守 uniMMR，但放宽单批与批次数，尽量一次换完。
        has_hedge：是否已有对冲仓。无仓时不按 uniMMR 估赎回量（文案与批次都跳过）。
        """
        try:
            moves = self._allocation_moves()
        except Exception as exc:
            return [StepResult("换产品", False, str(exc))]
        if not moves:
            return [StepResult("换产品", True, "已经按各产品自己的额度配在更高年化上，不用换")]
        target = moves[0][2]
        plan_text = "；".join(note for _src, _amt, _dst, note in moves[:6])
        if has_hedge is None:
            try:
                legs = self.futures.legs(self.settings.hedge_symbol)
                has_hedge = legs.long_qty > 0 or legs.short_qty > 0
            except Exception:
                # 读仓失败按有仓处理，避免误走整笔强赎
                has_hedge = True
        hedge_open = bool(has_hedge)
        total_left = sum((amt for _src, amt, _dst, _note in moves), Decimal("0"))
        min_b = max(self.settings.switch_min_batch, Decimal("1"))
        # 批次数按剩余仓位估，再留余量；真正停条件是换完或碰到 uniMMR 安全线
        if full:
            need = int((total_left / min_b).to_integral_value(rounding=ROUND_UP)) + len(moves) + 5
            max_batches = max(20, min(need, 200))
        else:
            max_batches = self.settings.switch_max_batches
        compared = self._apr_comparison()
        if full and not hedge_open:
            start_detail = (
                f"按各产品额度凑更高收益：{plan_text}。"
                f"无对冲仓，按剩余整笔归集（约 {max_batches} 批封顶）。"
                f"比较 {compared}"
            )
        else:
            start_detail = (
                f"按各产品额度凑更高收益：{plan_text}。"
                f"每批按当时 uniMMR≥{fmt_amount(self.settings.switch_safe_uni_mmr, 4)} 自动估赎回量，"
                f"换完或碰到安全线为止（约 {max_batches} 批封顶）。比较 {compared}"
            )
        steps = [StepResult("开始分批换产品", True, start_detail)]
        if full and not hedge_open and not self.settings.dry_run:
            steps.extend(self._repair_pm_negative_settle())
        batches = 0
        dust_cut = Decimal("1")  # 入场：小于 1 的非目标活期直接 redeemAll，避免 0.01 取整漏尾
        redeem_kw = {"has_hedge": hedge_open}

        def _redeem(product: FlexibleProduct, amt: Decimal, *, redeem_all: bool = False) -> list[StepResult]:
            return self._redeem_then_subscribe(
                product, target, amt, redeem_all=redeem_all, **redeem_kw
            )

        for product, amount, target, _note in moves:
            remaining = amount
            # 稳定币灰尘：不赎回，避免 -6060/-6009 刷屏
            if (
                remaining > 0
                and remaining < dust_cut
                and self._is_stable_earn_asset(product)
            ):
                steps.append(
                    StepResult(
                        "灰尘跳过",
                        True,
                        f"{product.asset} 仅 {fmt_amount(remaining)} < {fmt_amount(dust_cut)}，不赎回",
                    )
                )
                continue
            # 山寨活期数量单位不是 U，不能按 USDT 批次估；一律整笔赎→兑 USDT→申购
            if remaining > 0 and not self._is_stable_earn_asset(product):
                steps.append(
                    StepResult(
                        "清山寨活期",
                        True,
                        f"{product.asset} {fmt_amount(remaining)} 整笔赎回并兑成 {self.settings.source_asset} 再申购 {target.asset}",
                    )
                )
                steps.extend(_redeem(product, remaining, redeem_all=True))
                self._soften_dust_failures(steps)
                continue
            if full and remaining > 0 and remaining < dust_cut:
                steps.append(
                    StepResult(
                        "清尾数",
                        True,
                        f"{product.asset} 仅剩 {fmt_amount(remaining)}，整笔赎回并入 {target.asset}",
                    )
                )
                steps.extend(_redeem(product, remaining, redeem_all=True))
                self._soften_dust_failures(steps)
                continue
            while remaining > 0 and batches < max_batches:
                if (not full) and remaining < self.settings.switch_min_batch:
                    break
                mmr, equity = self._mmr_equity()
                batch = self._next_switch_batch(
                    remaining, mmr, equity, full=full, hedge_open=hedge_open
                )
                if batch <= 0:
                    # 有对冲：绝不能因 MMR 不够/读失败而整笔强赎；无仓才允许清尾
                    if full and (not hedge_open) and remaining > 0:
                        steps.append(
                            StepResult(
                                "清尾数",
                                True,
                                f"{product.asset} 还剩 {fmt_amount(remaining)}，整笔赎回清掉",
                            )
                        )
                        steps.extend(_redeem(product, remaining, redeem_all=True))
                        self._soften_dust_failures(steps)
                        remaining = Decimal("0")
                        break
                    steps.append(
                        StepResult(
                            "暂停换产品",
                            True,
                            f"uniMMR={fmt_amount(mmr, 4)}，这批只能赎 {fmt_amount(batch, 2)}，先停在安全线内。剩下 {fmt_amount(remaining, 4)} 下次再点",
                        )
                    )
                    return steps
                if (not full) and batch < min(self.settings.switch_min_batch, remaining):
                    steps.append(
                        StepResult(
                            "暂停换产品",
                            True,
                            f"uniMMR={fmt_amount(mmr, 4)}，这批只能赎 {fmt_amount(batch, 2)}，先停在安全线内。剩下 {fmt_amount(remaining, 4)} 下次再点",
                        )
                    )
                    return steps
                # 本批若本身已是尘埃：仅真正的灰尘才整笔赎；有仓且剩余较大时停住
                if full and batch < dust_cut:
                    if hedge_open and remaining >= dust_cut:
                        steps.append(
                            StepResult(
                                "暂停换产品",
                                True,
                                f"uniMMR={fmt_amount(mmr, 4)}，可赎空间过小，先停。剩下 {fmt_amount(remaining, 4)} 下次再点",
                            )
                        )
                        return steps
                    steps.extend(_redeem(product, remaining, redeem_all=True))
                    self._soften_dust_failures(steps)
                    remaining = Decimal("0")
                    break
                before = len(steps)
                steps.extend(_redeem(product, batch))
                fresh = steps[before:]
                if (target.asset or "").upper() in self._runtime_denylist:
                    steps.append(
                        StepResult(
                            "暂停换产品",
                            True,
                            f"{target.asset} 本地区不可用，已尽量改买其它产品，停止继续换年化",
                        )
                    )
                    return steps
                if any(not s.ok for s in fresh):
                    return steps
                if any(s.name == "暂停换产品" for s in fresh):
                    # 转不出则换下一种持仓；有对冲时绝不随后清尾强赎
                    remaining = Decimal("0")
                    break
                pulled = [s for s in fresh if s.name == "转回数量"]
                remaining -= d(pulled[-1].detail) if pulled else batch
                batches += 1
                if hedge_open and not self.settings.dry_run and self.settings.settle_seconds > 0:
                    time.sleep(self.settings.settle_seconds)
                    mmr_after, _ = self._mmr_equity()
                    if not is_mmr_sentinel(mmr_after) and mmr_after < self.settings.switch_safe_uni_mmr:
                        steps.append(
                            StepResult(
                                "暂停换产品",
                                True,
                                f"申购后 uniMMR={fmt_amount(mmr_after, 4)} 碰到安全线，剩下 {fmt_amount(remaining, 4)} 下次再点",
                            )
                        )
                        return steps
                elif self.settings.dry_run:
                    continue
                elif full and not hedge_open and not self.settings.dry_run and self.settings.settle_seconds > 0:
                    time.sleep(self.settings.settle_seconds)
            if remaining > 0 and batches >= max_batches:
                steps.append(
                    StepResult(
                        "本轮结束",
                        True,
                        f"已换 {batches} 批，还剩 {fmt_amount(remaining, 4)} {product.asset}，再点一次继续",
                    )
                )
                return steps
            # 清尾数仅无对冲时；有仓剩余留给下次（可转额度/uniMMR 恢复后再动）
            if full and remaining > 0 and not hedge_open:
                steps.append(
                    StepResult("清尾数", True, f"{product.asset} 循环后仍剩 {fmt_amount(remaining)}，整笔赎回")
                )
                steps.extend(_redeem(product, remaining, redeem_all=True))
                self._soften_dust_failures(steps)
                remaining = Decimal("0")
            elif 0 < remaining < self.settings.switch_min_batch and not hedge_open:
                mmr, equity = self._mmr_equity()
                if self._next_switch_batch(remaining, mmr, equity, full=full, hedge_open=hedge_open) >= remaining:
                    steps.extend(_redeem(product, remaining))
                    remaining = Decimal("0")
        if full:
            # 入场时也不再强清 <1 稳定币灰尘（币安常拒，只会 -6060 限频）
            self.earn.invalidate()
            self._product_list = None
        if batches == 0 and all(s.ok for s in steps) and not any("清尾数" in s.name for s in steps):
            # 若已有清尾数步骤，不要盖成「没有达到最小批次」
            if len(steps) <= 1:
                steps.append(StepResult("换产品", True, "没有达到最小批次，先不动"))
        return steps

    @staticmethod
    def _soften_dust_failures(steps: list[StepResult]) -> None:
        """尾数赎回/兑换/申购失败不阻断；币安常有最小赎回额或兑换下限。"""
        for s in steps:
            if s.ok:
                continue
            if any(k in s.name for k in ("赎回", "申购", "subscribe", "兑换")):
                s.ok = True
                s.detail = f"尾数未成（可忽略，可能低于交易所最小额）：{s.detail}"

    def _mmr_equity(self) -> tuple[Decimal, Decimal]:
        if not self.settings.unified_account:
            return Decimal("0"), Decimal("0")
        try:
            risk = self.futures.account_risk()
            return risk.get("uni_mmr") or Decimal("0"), risk.get("equity") or Decimal("0")
        except BinanceAPIError:
            # 读失败：返回哨兵，有仓时换年化必须停，绝不能当成可整笔赎回
            return Decimal("0"), Decimal("0")

    def _next_switch_batch(
        self,
        remaining: Decimal,
        mmr: Decimal,
        equity: Decimal,
        *,
        full: bool = False,
        hedge_open: bool = True,
    ) -> Decimal:
        """按当时 uniMMR / 权益 / 剩余仓位估本批赎回量，不写死固定额度。"""
        if remaining <= 0:
            return Decimal("0")
        # 无对冲仓：赎回不影响保证金，一次尽量赎完
        if full and not hedge_open:
            q = remaining.quantize(Decimal("0.01"), rounding=ROUND_DOWN)
            return q if q > 0 else remaining
        safe = self.settings.switch_safe_uni_mmr
        # 有对冲时：MMR 读不到 / 权益为 0 → 停，不臆测可赎完
        if hedge_open and (is_mmr_sentinel(mmr) or equity <= 0):
            return Decimal("0")
        if not is_mmr_sentinel(mmr) and mmr < safe:
            return Decimal("0")
        if equity > 0 and mmr > 0 and not is_mmr_sentinel(mmr):
            # 赎回瞬间保证金会少一块：只取把 uniMMR 压到安全线之上仍富余的空间
            cap = equity * (Decimal("1") - safe / mmr) * (Decimal("0.9") if full else Decimal("0.8"))
        else:
            # 无仓/无权益读数：允许按剩余估（上面 hedge_open 已拦截）
            cap = remaining
        if cap <= 0:
            return Decimal("0")
        if full:
            # 换年化：本批 = min(剩余, 当前可腾出的保证金空间)，能一次就一次
            batch = min(remaining, cap)
            if remaining - batch > 0 and remaining - batch < self.settings.switch_min_batch:
                if remaining <= cap:
                    batch = remaining
            q = batch.quantize(Decimal("0.01"), rounding=ROUND_DOWN)
            # 量化为 0：仅真灰尘返回原值；否则返回 0 让上层暂停
            if q <= 0 and remaining > 0:
                if remaining < Decimal("0.01"):
                    return remaining
                return Decimal("0")
            return q
        # 保守路径（入场有仓等）：仍用配置的单批上限与比例，但上限不超过当前 MMR 空间
        hard = self.settings.switch_batch_usdt
        pct = remaining * self.settings.switch_batch_pct
        raw = min(remaining, cap, pct if pct > 0 else remaining, hard)
        floor = min(self.settings.switch_min_batch, remaining, cap, hard)
        batch = raw if raw >= floor else floor
        if remaining - batch > 0 and remaining - batch < self.settings.switch_min_batch:
            if remaining <= cap and remaining <= hard:
                batch = remaining
        return batch.quantize(Decimal("0.01"), rounding=ROUND_DOWN)

    def _redeem_then_subscribe(
        self,
        source: FlexibleProduct,
        target: FlexibleProduct,
        amount: Decimal,
        *,
        redeem_all: bool = False,
        has_hedge: bool = True,
    ) -> list[StepResult]:
        steps: list[StepResult] = []
        if not source.can_redeem and source.kind not in {"bfusd", "rwusd"}:
            return [
                StepResult(
                    f"赎回活期 {source.asset}",
                    False,
                    f"交易所标记不可赎回 canRedeem=false，productId={source.product_id}",
                )
            ]
        moved_out = Decimal("0")
        if source.asset == "USDT" and source.kind == "flexible":
            requested = amount
            back, moved = self._return_pm_earn("LDUSDT", amount, has_hedge=has_hedge)
            steps.extend(back)
            if any(not s.ok for s in back):
                return steps
            paused = any(s.name == "暂停换产品" for s in back)
            # 占保锁在统一账户：停。理财钱包里的活期和 PM 不是同一笔，转不出也要赎钱包。
            if paused and source.raw.get("located") == "pm":
                return steps
            if paused or moved > 0:
                if moved > 0 and not self.settings.dry_run:
                    self.earn.invalidate()
                earn_bal = self._flexible_product_balance(source)
                if paused and earn_bal < SPOT_MIN:
                    return steps
                if paused:
                    steps.append(
                        StepResult(
                            "理财侧已有余额，继续赎回",
                            True,
                            f"统一账户暂不可转，理财 {source.asset} 约 {fmt_amount(earn_bal)}，按理财余额赎回",
                        )
                    )
                # 刚从统一账户转回的和本来就在理财钱包的，一起赎，别只赎转出的那截
                redeemable = earn_bal if earn_bal > 0 else moved
                amount = min(requested, redeemable)
                moved_out = amount if amount > 0 else moved
        if source.kind in {"bfusd", "rwusd"}:
            # BFUSD/RWUSD 常在统一账户里；earn-asset-transfer 不支持，先转现货再赎
            try:
                pm_bal = self.futures.papi_balances().get(source.asset.upper(), Decimal("0"))
            except BinanceAPIError:
                pm_bal = Decimal("0")
            if pm_bal >= SPOT_MIN:
                pull_steps, pulled = self._pull_pm_asset_to_spot(source.asset, amount)
                steps.extend(pull_steps)
                if any(not s.ok for s in pull_steps):
                    return steps
                if pulled > 0:
                    moved_out = pulled
                    amount = pulled
                elif has_hedge:
                    steps.append(
                        StepResult(
                            "暂停换产品",
                            True,
                            f"统一账户有 {source.asset} 但转不出，有对冲仓先留作保证金",
                        )
                    )
                    return steps
                elif pulled <= 0:
                    # 无仓仍转不出：别硬赎 PM 里的量（会 -6018）
                    try:
                        spot_only = self.earn.spot_free(source.asset)
                    except BinanceAPIError:
                        spot_only = Decimal("0")
                    if spot_only < SPOT_MIN:
                        steps.append(
                            StepResult(
                                f"赎回 {source.asset} 跳过",
                                True,
                                f"统一账户转不出且现货不足 1，先处理负余额/权限后再换",
                            )
                        )
                        return steps
                    amount = spot_only
            if not self.settings.dry_run:
                self.earn.invalidate()
                try:
                    spot_tok = self.earn.spot_free(source.asset)
                except BinanceAPIError:
                    spot_tok = Decimal("0")
                if spot_tok >= SPOT_MIN:
                    amount = min(amount, spot_tok)
        if source.kind == "bfusd":
            redeem = self._mutate(
                f"赎回 BFUSD {fmt_amount(amount)}（FAST，回现货 USDT）",
                lambda amt=amount: self.earn.redeem_bfusd(amt, "FAST"),
            )
        elif source.kind == "rwusd":
            redeem = self._mutate(
                f"赎回 RWUSD {fmt_amount(amount)}（FAST，回到现货）",
                lambda amt=amount: self.earn.redeem_rwusd(amt, "FAST"),
            )
        elif redeem_all:
            redeem = self._mutate(
                f"赎回活期 {source.asset} 全部 productId={source.product_id}",
                lambda: self.earn.redeem(source.product_id, redeem_all=True),
            )
        else:
            redeem = self._mutate(
                f"赎回活期 {source.asset} {fmt_amount(amount)} productId={source.product_id}",
                lambda: self.earn.redeem(source.product_id, amount=amount),
            )
        if not redeem.ok and self._unauthorized(redeem):
            redeem.detail = (
                f"{redeem.detail}。"
                "活期赎回/申购需要 API Key 勾选「允许现货及杠杆交易」(Enable Spot & Margin Trading)；"
                "仅合约/只读权限会报 -1002。子账户要用该子账户自己的 Key。"
            )
        steps.append(redeem)
        if not redeem.ok:
            return steps
        if moved_out <= 0:
            moved_out = amount
        if not any(s.name == "转回数量" for s in steps):
            steps.append(StepResult("转回数量", True, moved_out))
        if not self.settings.dry_run and self.settings.settle_seconds > 0:
            time.sleep(self.settings.settle_seconds)
        # 赎回物可能是 USDC / 山寨币：先兑成申购资产再买目标理财
        if not self.settings.dry_run:
            self.earn.invalidate()
            for asset in ("USDC", source.asset.upper()):
                if asset in {"", "USDT"} or asset == (self.settings.source_asset or "USDT").upper():
                    continue
                converted = self._convert_spot_to_source(asset)
                if converted is None:
                    continue
                steps.append(converted)
                if not converted.ok:
                    # 灰尘/低于 Convert 下限：不当整笔换产品失败
                    detail = str(converted.detail or "")
                    soft = (
                        redeem_all
                        or amount < SPOT_MIN
                        or "345233" in detail
                        or "between 0.01" in detail.lower()
                        or "兑换下限" in detail
                    )
                    if soft:
                        converted.ok = True
                        converted.detail = f"尾数过小未兑换：{converted.detail}"
                    else:
                        return steps
        free = amount if self.settings.dry_run else self.earn.spot_free(self.settings.source_asset or "USDT")
        buy_amount = min(amount, free) if free > 0 else (free if redeem_all else amount)
        if redeem_all and not self.settings.dry_run:
            self.earn.invalidate()
            free = self.earn.spot_free(self.settings.source_asset or "USDT")
            buy_amount = free if free > 0 else Decimal("0")
        if buy_amount <= 0:
            if redeem_all:
                return steps + [
                    StepResult("申购跳过", True, "赎回后现货 USDT 仍为 0（可能尚未到账或已是尾数）")
                ]
            return steps + [StepResult("申购跳过", False, "赎回后现货 USDT 不足，无法申购目标产品")]
        before = len(steps)
        steps.extend(self.buy(target, buy_amount))
        if any(not s.ok for s in steps[before:]) and buy_amount < SPOT_MIN:
            for s in steps[before:]:
                if not s.ok:
                    s.ok = True
                    s.detail = f"尾数过小未申购：{s.detail}"
        elif any(not s.ok for s in steps[before:]):
            fail = next((s for s in reversed(steps[before:]) if not s.ok), None)
            fb_at = len(steps)
            steps.extend(self._fallback_subscribe(target, buy_amount, fail.detail if fail else "未知"))
            if fb_at < len(steps) and all(s.ok for s in steps[fb_at:]):
                for s in steps[before:fb_at]:
                    if not s.ok:
                        s.ok = True
                        s.detail = f"失败后已改申购：{s.detail}"
        return steps

    @staticmethod
    def _same_product(left: FlexibleProduct, right: FlexibleProduct) -> bool:
        return left.kind == right.kind and left.product_id == right.product_id

    def position_amount(self, product: FlexibleProduct) -> Decimal | None:
        try:
            rows = self.earn.positions(product.asset)
        except BinanceAPIError:
            return None
        for row in rows:
            if str(row.get("productId") or "") == product.product_id:
                total = d(row.get("totalAmount") or row.get("amount"))
                return total if total > 0 else None
        return None

    def to_margin(self, product: FlexibleProduct, amount: Decimal, margin_assets: set[str]) -> list[StepResult]:
        if self.settings.unified_account and self._is_margin_asset(product.asset, margin_assets):
            return [
                StepResult(
                    "统一账户",
                    True,
                    f"{product.asset} 活期仓位已在统一账户内，可直接作为合约保证金，无需划转",
                )
            ]

        steps: list[StepResult] = []
        if self.settings.keep_earn:
            if self.settings.unified_account:
                steps.append(
                    StepResult(
                        "保留活期",
                        True,
                        f"{product.asset} 已申购并留在统一账户；该资产当前不在保证金列表，不会划转",
                    )
                )
                return steps
            transfer_asset = product.asset
            ld_asset = f"LD{product.asset}"
            if ld_asset in margin_assets:
                transfer_asset = ld_asset
            if self._is_margin_asset(product.asset, margin_assets):
                steps.extend(self._transfer_margin(transfer_asset, amount, margin_assets))
            else:
                steps.append(
                    StepResult(
                        "保留活期",
                        True,
                        f"{product.asset} 不能直接当 U 本位保证金，keep_earn=true 因此跳过兑换",
                    )
                )
            return steps

        redeem = self._mutate(
            f"赎回活期 {product.asset} {fmt_amount(amount)}",
            lambda: self.earn.redeem(product.product_id, amount=amount),
        )
        steps.append(redeem)
        if not self.settings.dry_run and self.settings.settle_seconds > 0:
            time.sleep(self.settings.settle_seconds)

        target = self._choose_margin_asset(product.asset, margin_assets)
        if product.asset != target:
            convert = self._mutate(
                f"兑换 {fmt_amount(amount)} {product.asset} -> {target}",
                lambda: self.convert_api.convert(product.asset, target, amount),
            )
            steps.append(convert)
            transfer_amount = amount
            if convert.ok and not convert.dry_run and isinstance(convert.detail, dict):
                to_amount = convert.detail.get("quote", {}).get("toAmount")
                if to_amount:
                    transfer_amount = d(to_amount)
        else:
            transfer_amount = amount
            steps.append(StepResult("无需再兑保证金资产", True, f"{product.asset} 已可作保证金"))

        steps.extend(self._transfer_margin(target, transfer_amount, margin_assets))
        return steps

    def run(self) -> list[StepResult]:
        products = self.list_products()
        margin_assets = self.margin_assets()
        product = self.pick(products, margin_assets)
        amount = self.plan_amount(product)
        steps = [
            StepResult(
                "选定活期",
                True,
                {
                    "asset": product.asset,
                    "apr_percent": str(apr_percent(product.apr)),
                    "product_id": product.product_id,
                    "amount": fmt_amount(amount),
                    "source_asset": self.settings.source_asset,
                    "is_margin": self._is_margin_asset(product.asset, margin_assets),
                },
            )
        ]
        buy_steps = self.buy(product, amount)
        subscribe_amount = amount
        for step in buy_steps:
            if step.name == "subscribe_amount":
                subscribe_amount = d(step.detail)
            else:
                steps.append(step)
        if any(not s.ok for s in steps):
            return steps
        steps.extend(self.to_margin(product, subscribe_amount, margin_assets))
        return steps

    def _choose_margin_asset(self, current: str, margin_assets: set[str]) -> str:
        if current in self.settings.target_margin_assets and current in margin_assets:
            return current
        for asset in self.settings.target_margin_assets:
            if asset in margin_assets:
                return asset
        if current in margin_assets:
            return current
        return self.settings.margin_asset

    def _transfer_margin(self, asset: str, amount: Decimal, margin_assets: set[str]) -> list[StepResult]:
        steps: list[StepResult] = []
        if self.settings.unified_account or not self.settings.transfer_to_futures:
            reason = "统一账户无需划转" if self.settings.unified_account else "config.transfer_to_futures=false"
            steps.append(StepResult("跳过划转合约", True, reason))
            return steps
        if asset not in margin_assets:
            steps.append(StepResult("无法划转", False, f"{asset} 不在 U 本位联合保证金列表里"))
            return steps
        if self.settings.enable_multi_assets:
            steps.append(
                self._mutate("开启 U 本位联合保证金（多资产模式）", self.futures.enable_multi_assets)
            )
        if not self.settings.dry_run:
            free = self.earn.spot_free(asset)
            amount = min(amount, free)
            if amount <= 0:
                steps.append(StepResult("划转合约", False, f"现货 {asset} 余额为 0，无法划转"))
                return steps
        steps.append(
            self._mutate(
                f"划转 {fmt_amount(amount)} {asset} 现货 -> U本位合约",
                lambda: self.futures.transfer_to_um_futures(asset, amount),
            )
        )
        return steps

    def _mutate(self, name: str, fn: Callable[[], Any]) -> StepResult:
        if self.settings.dry_run:
            return StepResult(name, True, "dry-run 未真实下单", dry_run=True)
        try:
            return StepResult(name, True, fn(), dry_run=False)
        except BinanceAPIError as exc:
            return StepResult(name, False, str(exc), dry_run=False)
        except Exception as exc:
            # 兑换询价等非 BinanceAPIError 也不能冒泡中断收利补仓
            return StepResult(name, False, str(exc), dry_run=False)
