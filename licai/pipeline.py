from __future__ import annotations

import time
from dataclasses import dataclass, replace
from decimal import ROUND_DOWN, Decimal
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
    """BFUSD：账户资金不超过 800U 用高档，超过则用理财页年化。"""
    text = f"{apr_percent(product.apr):.2f}%"
    raw = product.raw or {}
    tier = raw.get("apr_tier")
    funds = d(raw.get("account_funds") or 0)
    fund_text = f"资金 {fmt_amount(funds, 2)}U" if funds > 0 else "资金未知"
    base = raw.get("base_apr")
    small = raw.get("small_balance_apr")
    if tier == "under_800":
        return text + f"（{fund_text}，800U以内）"
    if tier == "over_800" and small not in (None, "", "0"):
        return text + f"（{fund_text}，800U以上；800U以内 {apr_percent(d(small)):.2f}%）"
    if small not in (None, "", "0") and base not in (None, "", "0"):
        return text + f"（800U以上 {apr_percent(d(base)):.2f}%，800U以内 {apr_percent(d(small)):.2f}%）"
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

    def _bfusd_product(self) -> FlexibleProduct:
        product = self.earn.bfusd_product()
        raw = dict(product.raw or {})
        base = d(raw.get("base_apr") or product.apr)
        small = d(raw.get("small_balance_apr") or 0)
        funds = self._account_funds()
        raw["account_funds"] = str(funds)
        raw["base_apr"] = str(base)
        use_small = small > base and funds > 0 and funds <= _BFUSD_TIER_USDT
        raw["apr_tier"] = "under_800" if use_small else "over_800"
        return replace(product, apr=small if use_small else base, raw=raw)

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
        if product.asset in self.settings.asset_denylist:
            return False
        if self.settings.asset_allowlist and product.asset not in self.settings.asset_allowlist:
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

    def sweep_spot_to_earn(self) -> list[StepResult]:
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
                    self.earn.invalidate()
        try:
            free = self.spot_cash()
        except BinanceAPIError as exc:
            return steps + [StepResult("归集理财", False, str(exc))]
        if free < SPOT_MIN:
            if steps:
                steps.append(StepResult("归集理财", True, f"已换到 {self.settings.source_asset}，但可申购余额不足，跳过申购"))
                return steps
            return [StepResult("归集理财", True, "现货没有可申购余额，跳过申购")]
        product = self.pick()
        amount = self.plan_amount(product, sweep_all=True)
        if amount < SPOT_MIN:
            return steps + [StepResult("归集理财", True, "现货没有可申购余额，跳过申购")]
        steps.append(
            StepResult(
                "归集理财",
                True,
                f"把现货 {fmt_amount(amount)} {self.settings.source_asset} 申购 {product.asset} 年化 {apr_percent(product.apr)}%",
            )
        )
        steps.extend(self.buy(product, amount))
        if any(not s.ok for s in steps) and product.kind in {"bfusd", "rwusd"}:
            alt = self._best_usdt_flexible()
            leftover = self.earn.spot_free(self.settings.source_asset)
            if alt is not None and leftover > 0:
                fail = next((s for s in reversed(steps) if not s.ok), None)
                before = len(steps)
                steps.append(
                    StepResult(
                        "改申购 USDT 活期",
                        True,
                        f"{product.asset} 申购失败（{fail.detail if fail else '未知'}），改买 USDT 活期",
                    )
                )
                steps.extend(self.buy(alt, leftover))
                if all(s.ok for s in steps[before:]):
                    for s in steps[:before]:
                        if not s.ok:
                            s.ok = True
                            s.detail = f"失败后已改申购 USDT 活期：{s.detail}"
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
        show = max(usdt_amt, ld)
        if show > 0:
            product = self._usdt_flex_product()
            if ld > 0 and ld >= usdt_amt:
                product = replace(product, margin_token="LDUSDT", raw={**product.raw, "located": "pm"})
            holdings.append((product, show))

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

    def fund_unified(self, *, check_ldusdt: bool = True) -> list[StepResult]:
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
            steps.extend(self._fund_spot_asset("BFUSD", bfusd_spot))
        rwusd_spot = self.spot_cash("RWUSD")
        if rwusd_spot >= SPOT_MIN:
            steps.extend(self._fund_spot_asset("RWUSD", rwusd_spot))
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

    def _fund_spot_asset(self, asset: str, amount: Decimal) -> list[StepResult]:
        if self._wallet_transfer_blocked:
            return self._spot_via_ldusdt(asset, amount)
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

    def _return_pm_earn(self, asset: str, amount: Decimal) -> tuple[list[StepResult], Decimal]:
        """按币安 FUTURE_TO_EARN 可转上限转回。总余额大于这个上限时，多转会 -3020。"""
        try:
            cap = self.futures.earn_asset_transferable(asset, "FUTURE_TO_EARN")
        except BinanceAPIError as exc:
            return [StepResult(f"查询 {asset} 可转出", False, str(exc))], Decimal("0")
        move = amount if amount > 0 else cap
        if move > cap:
            move = cap
        move = move.quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)
        if move < SPOT_MIN:
            return [
                StepResult(
                    "暂停换产品",
                    True,
                    f"统一账户 {asset} 现在最多能转回理财 {fmt_amount(cap, 8)}。"
                    "有对冲仓时其余要留作保证金，这次不转。",
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

    def _convert_usdc_proceeds(self) -> StepResult | None:
        """RWUSD 赎回有时回到 USDC。申购入口用的是 source_asset，先换过去。"""
        if self.settings.dry_run or self.settings.source_asset == "USDC":
            return None
        try:
            usdc = self.earn.spot_free("USDC")
        except BinanceAPIError:
            return None
        if usdc < SPOT_MIN:
            return None
        step = self._mutate(
            f"兑换 {fmt_amount(usdc)} USDC -> {self.settings.source_asset}",
            lambda amt=usdc: self.convert_api.convert("USDC", self.settings.source_asset, amt),
        )
        if step.ok and not step.dry_run:
            self.earn.invalidate()
        return step

    @staticmethod
    def _unauthorized(step: StepResult) -> bool:
        text = str(step.detail or "").lower()
        return "-1002" in text or "not authorized" in text

    def switch_advice(self, *, mmr: Decimal | None = None, equity: Decimal | None = None) -> dict:
        try:
            target = self.pick()
        except Exception as exc:
            return {"needed": False, "can_click": False, "reason": str(exc)}
        holdings = self.earn_holdings()
        sources = [(p, amt) for p, amt in holdings if not self._same_product(p, target)]
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
        leftover = sum((amt for _, amt in sources), Decimal("0"))
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
                        f"点「换年化」会申购 {target.asset}，当前年化 {apr_choice_text(target)}"
                    ),
                }
            return {
                "needed": False,
                "can_click": False,
                "target": target.asset,
                "target_apr": str(apr_percent(target.apr)),
                "current_mmr": fmt_amount(mmr, 4),
                "safe_mmr": fmt_amount(safe, 4),
                "next_batch": "0",
                "remaining": "0",
                "reason": f"理财仓已经在最高年化 {target.asset} {apr_choice_text(target)}，不用换",
            }
        next_batch = self._next_switch_batch(leftover, mmr, equity)
        can = next_batch > 0
        if not is_mmr_sentinel(mmr) and mmr < safe:
            reason = f"当前 uniMMR={fmt_amount(mmr, 4)} 低于安全线 {fmt_amount(safe, 4)}，先不要赎回换产品"
        elif next_batch <= 0:
            reason = "这批按 uniMMR 估出来能赎的数量太小，先不要动"
        else:
            from_text = "、".join(f"{p.asset} {fmt_amount(amt, 2)}" for p, amt in sources)
            reason = (
                f"分批把 {from_text} 换成 {target.asset}（年化 {apr_choice_text(target)}）。"
                f"本批最多 {fmt_amount(next_batch, 2)} USDT，按赎回瞬间保证金暂时少一块来估，"
                f"目标 uniMMR ≥ {fmt_amount(safe, 4)}。点一次最多 {self.settings.switch_max_batches} 批。"
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

        full=True：一键入场用。无仓时尽量一次赎完；有仓时仍守 uniMMR 安全线，但放宽批次数。
        has_hedge：是否已有对冲仓。无仓时不按 uniMMR 估赎回量（文案与批次都跳过）。
        """
        try:
            target = self.pick()
        except RuntimeError as exc:
            return [StepResult("换产品", False, str(exc))]
        sources = [(p, amt) for p, amt in self.earn_holdings() if not self._same_product(p, target)]
        if not sources:
            return [StepResult("换产品", True, f"已经在 {target.asset}，不用换")]
        if has_hedge is None:
            try:
                legs = self.futures.legs(self.settings.hedge_symbol)
                has_hedge = legs.long_qty > 0 or legs.short_qty > 0
            except Exception:
                has_hedge = False
        hedge_open = bool(has_hedge)
        max_batches = 50 if full else self.settings.switch_max_batches
        compared = self._apr_comparison()
        if full and not hedge_open:
            start_detail = (
                f"目标 {target.asset} 年化 {apr_choice_text(target)}；"
                f"无对冲仓，其它活期整笔归集（不按 uniMMR 估批），最多 {max_batches} 批。"
                f"比较 {compared}"
            )
        else:
            start_detail = (
                f"目标 {target.asset} 年化 {apr_choice_text(target)}；"
                f"{'入场归集其它活期，' if full else ''}"
                f"每批按 uniMMR≥{fmt_amount(self.settings.switch_safe_uni_mmr, 4)} 估赎回量，"
                f"最多 {max_batches} 批。比较 {compared}"
            )
        steps = [StepResult("开始分批换产品", True, start_detail)]
        batches = 0
        dust_cut = Decimal("1")  # 入场：小于 1 的非目标活期直接 redeemAll，避免 0.01 取整漏尾
        for product, amount in sources:
            remaining = amount
            if full and remaining > 0 and remaining < dust_cut:
                steps.append(
                    StepResult(
                        "清尾数",
                        True,
                        f"{product.asset} 仅剩 {fmt_amount(remaining)}，整笔赎回并入 {target.asset}",
                    )
                )
                steps.extend(self._redeem_then_subscribe(product, target, remaining, redeem_all=True))
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
                    if full and remaining > 0:
                        steps.append(
                            StepResult(
                                "清尾数",
                                True,
                                f"{product.asset} 还剩 {fmt_amount(remaining)}，整笔赎回清掉",
                            )
                        )
                        steps.extend(self._redeem_then_subscribe(product, target, remaining, redeem_all=True))
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
                # 本批若本身已是尘埃，改整笔赎，避免按金额被拒
                if full and batch < dust_cut:
                    steps.extend(self._redeem_then_subscribe(product, target, remaining, redeem_all=True))
                    self._soften_dust_failures(steps)
                    remaining = Decimal("0")
                    break
                before = len(steps)
                steps.extend(self._redeem_then_subscribe(product, target, batch))
                fresh = steps[before:]
                if any(not s.ok for s in fresh):
                    return steps
                if any(s.name == "暂停换产品" for s in fresh):
                    # 这一笔转不出来就换下一种持仓，不要把已经申购的产品卡在统一账户外面
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
            if full and remaining > 0:
                steps.append(
                    StepResult("清尾数", True, f"{product.asset} 循环后仍剩 {fmt_amount(remaining)}，整笔赎回")
                )
                steps.extend(self._redeem_then_subscribe(product, target, remaining, redeem_all=True))
                self._soften_dust_failures(steps)
                remaining = Decimal("0")
            elif 0 < remaining < self.settings.switch_min_batch:
                mmr, equity = self._mmr_equity()
                if self._next_switch_batch(remaining, mmr, equity, full=full, hedge_open=hedge_open) >= remaining:
                    steps.extend(self._redeem_then_subscribe(product, target, remaining))
                    remaining = Decimal("0")
        if full:
            # 再扫一遍持仓，清掉交易所账面尘埃（如 0.0065）
            self.earn.invalidate()
            leftovers = [(p, amt) for p, amt in self.earn_holdings() if not self._same_product(p, target) and amt > 0]
            for product, amount in leftovers:
                steps.append(
                    StepResult(
                        "清尾数",
                        True,
                        f"复查仍有 {product.asset} {fmt_amount(amount)}，整笔赎回",
                    )
                )
                steps.extend(self._redeem_then_subscribe(product, target, amount, redeem_all=True))
                self._soften_dust_failures(steps)
        if batches == 0 and all(s.ok for s in steps) and not any("清尾数" in s.name for s in steps):
            # 若已有清尾数步骤，不要盖成「没有达到最小批次」
            if len(steps) <= 1:
                steps.append(StepResult("换产品", True, "没有达到最小批次，先不动"))
        return steps

    @staticmethod
    def _soften_dust_failures(steps: list[StepResult]) -> None:
        """尾数赎回/申购失败不阻断入场；币安常有最小赎回额。"""
        for s in steps:
            if s.ok:
                continue
            if any(k in s.name for k in ("赎回", "申购", "subscribe")):
                s.ok = True
                s.detail = f"尾数未成（可忽略，可能低于交易所最小额）：{s.detail}"

    def _mmr_equity(self) -> tuple[Decimal, Decimal]:
        if not self.settings.unified_account:
            return Decimal("0"), Decimal("0")
        try:
            risk = self.futures.account_risk()
            return risk.get("uni_mmr") or Decimal("0"), risk.get("equity") or Decimal("0")
        except BinanceAPIError:
            return Decimal("1"), Decimal("0")

    def _next_switch_batch(
        self,
        remaining: Decimal,
        mmr: Decimal,
        equity: Decimal,
        *,
        full: bool = False,
        hedge_open: bool = True,
    ) -> Decimal:
        if remaining <= 0:
            return Decimal("0")
        # 无对冲仓：赎回不影响保证金，不必按 uniMMR 砍批次
        if full and not hedge_open:
            hard = max(self.settings.switch_batch_usdt, Decimal("2000"))
            batch = min(remaining, hard)
            q = batch.quantize(Decimal("0.01"), rounding=ROUND_DOWN)
            if q <= 0 and remaining > 0:
                return remaining
            if remaining - q > 0 and remaining - q < self.settings.switch_min_batch and remaining <= hard:
                return remaining.quantize(Decimal("0.01"), rounding=ROUND_DOWN) or remaining
            return q
        safe = self.settings.switch_safe_uni_mmr
        if not is_mmr_sentinel(mmr) and mmr < safe:
            return Decimal("0")
        if equity > 0 and mmr > 0 and not is_mmr_sentinel(mmr):
            cap = equity * (Decimal("1") - safe / mmr) * (Decimal("0.9") if full else Decimal("0.8"))
        else:
            # 无权益/无仓：其它活期多半还不在保证金里，允许一次赎完
            cap = remaining
        if cap <= 0:
            return Decimal("0")
        if full:
            # 入场归集：在安全线内尽量整笔换到最高年化；单笔仍封顶，避免一次过大
            hard = max(self.settings.switch_batch_usdt, Decimal("2000"))
            batch = min(remaining, cap, hard)
            if remaining - batch > 0 and remaining - batch < self.settings.switch_min_batch:
                if remaining <= cap:
                    batch = remaining
            q = batch.quantize(Decimal("0.01"), rounding=ROUND_DOWN)
            # 不到 0.01 的尾数：仍返回原值，后续走 redeemAll 清掉
            if q <= 0 and remaining > 0:
                return remaining
            return q
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
        if source.asset == "USDT" and source.kind == "flexible":
            back, moved = self._return_pm_earn("LDUSDT", amount)
            steps.extend(back)
            if any(not s.ok for s in back) or any(s.name == "暂停换产品" for s in back):
                return steps
            if source.raw.get("located") == "pm" and moved > 0:
                amount = moved
        if source.kind == "bfusd":
            redeem = self._mutate(
                f"赎回 BFUSD {fmt_amount(amount)}（FAST，回现货 USDT）",
                lambda: self.earn.redeem_bfusd(amount, "FAST"),
            )
        elif source.kind == "rwusd":
            redeem = self._mutate(
                f"赎回 RWUSD {fmt_amount(amount)}（FAST，回到现货）",
                lambda: self.earn.redeem_rwusd(amount, "FAST"),
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
        if not self.settings.dry_run and self.settings.settle_seconds > 0:
            time.sleep(self.settings.settle_seconds)
            converted = self._convert_usdc_proceeds()
            if converted is not None:
                steps.append(converted)
                if not converted.ok:
                    return steps
        free = amount if self.settings.dry_run else self.earn.spot_free("USDT")
        buy_amount = min(amount, free) if free > 0 else (free if redeem_all else amount)
        if redeem_all and not self.settings.dry_run:
            self.earn.invalidate()
            converted = self._convert_usdc_proceeds()
            if converted is not None:
                steps.append(converted)
                if not converted.ok:
                    return steps
            free = self.earn.spot_free("USDT")
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
