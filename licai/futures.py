from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN

from .client import BinanceAPIError, BinanceClient
from .config import d, fmt_amount

_NEW_ACCOUNT_LEV_RE = re.compile(
    r"more than\s+(\d+)x\s+leverage\s+by\s+(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2})",
    re.I,
)
_GREATER_THAN_LEV_RE = re.compile(r"greater than\s+(\d+)x", re.I)
LEVERAGE_LADDER = [100, 75, 50, 25, 20, 10, 5]


def parse_leverage_limit(exc: BinanceAPIError) -> tuple[int, str | None, str]:
    text = str(exc)
    if isinstance(exc.payload, dict):
        text = str(exc.payload.get("msg") or text)
    code = exc.code
    dated = _NEW_ACCOUNT_LEV_RE.search(text)
    if code in (-4300, "-4300") or dated or "-4300" in str(exc):
        if dated:
            cap = int(dated.group(1))
            try:
                unlock = datetime.strptime(dated.group(2), "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
                unlock_at = unlock.isoformat()
            except ValueError:
                unlock_at = None
            return cap, unlock_at, "新合约账号"
        return 20, None, "新合约账号"
    greater = _GREATER_THAN_LEV_RE.search(text)
    if code in (-4421, "-4421") or "Subaccounts are restricted" in text or "-4421" in str(exc):
        cap = int(greater.group(1)) if greater else 5
        return cap, None, "子账户"
    if greater:
        return int(greater.group(1)), None, "账号限制"
    return 0, None, ""


@dataclass
class HedgeLegs:
    long_qty: Decimal
    short_qty: Decimal
    long_pnl: Decimal
    short_pnl: Decimal
    long_entry: Decimal
    short_entry: Decimal

    @property
    def missing_side(self) -> str | None:
        if self.long_qty <= 0 and self.short_qty <= 0:
            return "BOTH"
        if self.long_qty <= 0:
            return "LONG"
        if self.short_qty <= 0:
            return "SHORT"
        if self.long_qty != self.short_qty:
            return "IMBALANCE"
        return None


class FuturesAPI:
    def __init__(self, client: BinanceClient, unified_account: bool = True):
        self.client = client
        self.unified = unified_account

    def _um_signed(self, method: str, classic_path: str, papi_path: str, params: dict | None = None):
        if self.unified:
            return self.client.signed(method, papi_path, params, papi=True)
        return self.client.signed(method, classic_path, params, futures=True)

    def margin_assets(self) -> set[str]:
        data = self.client.public("GET", "/fapi/v1/assetIndex", futures=True)
        assets: set[str] = set()
        rows = data if isinstance(data, list) else [data]
        for row in rows:
            symbol = str(row.get("symbol") or "")
            if symbol.endswith("USD"):
                assets.add(symbol[:-3].upper())
        assets.update({"USDT", "USDC", "BFUSD", "FDUSD", "BNFCR", "LDUSDT", "RWUSD", "USD1"})
        return {a for a in assets if a}

    def portfolio_margin_assets(self) -> set[str]:
        data = self.client.public("GET", "/sapi/v1/portfolio/collateralRate")
        assets: set[str] = set()
        rows = data if isinstance(data, list) else data.get("collateralRate") or data.get("data") or []
        for row in rows:
            asset = str(row.get("asset") or "").upper()
            if asset:
                assets.add(asset)
        assets.update({"USDT", "USDC", "BFUSD", "FDUSD", "BNFCR", "LDUSDT", "RWUSD", "USD1"})
        return assets

    def multi_assets_enabled(self) -> bool:
        data = self.client.signed("GET", "/fapi/v1/multiAssetsMargin", futures=True)
        return str(data.get("multiAssetsMargin")).lower() == "true"

    def enable_multi_assets(self) -> dict:
        if self.multi_assets_enabled():
            return {"already": True, "multiAssetsMargin": True}
        return self.client.signed(
            "POST",
            "/fapi/v1/multiAssetsMargin",
            {"multiAssetsMargin": "true"},
            futures=True,
        )

    def transfer_to_um_futures(self, asset: str, amount: Decimal) -> dict:
        return self.client.signed(
            "POST",
            "/sapi/v1/asset/transfer",
            {
                "type": "MAIN_UMFUTURE",
                "asset": asset.upper(),
                "amount": fmt_amount(amount),
            },
        )

    def enable_hedge_mode(self) -> dict:
        data = self._um_signed("GET", "/fapi/v1/positionSide/dual", "/papi/v1/um/positionSide/dual")
        if str(data.get("dualSidePosition")).lower() == "true":
            return {"already": True, "dualSidePosition": True}
        return self._um_signed(
            "POST",
            "/fapi/v1/positionSide/dual",
            "/papi/v1/um/positionSide/dual",
            {"dualSidePosition": "true"},
        )

    def max_leverage(self, symbol: str) -> int:
        try:
            data = self._um_signed("GET", "/fapi/v1/leverageBracket", "/papi/v1/um/leverageBracket", {"symbol": symbol})
        except BinanceAPIError:
            try:
                data = self._um_signed("GET", "/fapi/v1/leverageBracket", "/papi/v1/um/leverageBracket")
            except BinanceAPIError:
                return 0
        rows = data if isinstance(data, list) else [data]
        best = 0
        for row in rows:
            name = str(row.get("symbol") or "")
            if name and name != symbol:
                continue
            for bracket in row.get("brackets") or []:
                lev = int(bracket.get("initialLeverage") or 0)
                if lev > best:
                    best = lev
            if name == symbol and best:
                return best
        return best

    def current_leverage(self, symbol: str) -> int:
        rows = self._um_signed("GET", "/fapi/v2/positionRisk", "/papi/v1/um/positionRisk", {"symbol": symbol})
        if isinstance(rows, dict):
            rows = [rows]
        best = 0
        for row in rows or []:
            if str(row.get("symbol") or "") not in {"", symbol}:
                continue
            try:
                lev = int(row.get("leverage") or 0)
            except (TypeError, ValueError):
                lev = 0
            if lev > best:
                best = lev
        return best

    def _post_leverage(self, symbol: str, leverage: int) -> dict:
        data = self._um_signed(
            "POST",
            "/fapi/v1/leverage",
            "/papi/v1/um/leverage",
            {"symbol": symbol, "leverage": leverage},
        )
        if not isinstance(data, dict):
            data = {"result": data}
        data.setdefault("leverage", leverage)
        return data

    def apply_best_leverage(self, symbol: str, target: int) -> dict:
        order: list[int] = []
        for lev in [target, *LEVERAGE_LADDER]:
            if lev > 0 and lev <= target and lev not in order:
                order.append(lev)
        last_exc: BinanceAPIError | None = None
        unlock_at = None
        kind = ""
        i = 0
        seen: set[int] = set()
        while i < len(order):
            lev = order[i]
            i += 1
            if lev in seen:
                continue
            seen.add(lev)
            try:
                data = self._post_leverage(symbol, lev)
            except BinanceAPIError as exc:
                last_exc = exc
                cap, unlock, parsed_kind = parse_leverage_limit(exc)
                if unlock:
                    unlock_at = unlock
                if parsed_kind:
                    kind = parsed_kind
                if cap > 0 and cap not in seen:
                    order.insert(i, cap)
                continue
            data["requested"] = target
            data["leverage"] = int(data.get("leverage") or lev)
            data["unlock_at"] = unlock_at
            applied = int(data["leverage"])
            if applied < target:
                data["capped"] = True
                who = kind or "账号限制"
                if unlock_at:
                    later = f"{unlock_at.replace('T', ' ').replace('+00:00', ' UTC')} 后可再试更高。"
                else:
                    later = "以后可在设置里再点「设置杠杆」试更高。"
                data["note"] = f"{who}，最高 {applied}x，已按 {applied}x 设置。{later}"
            else:
                data["capped"] = False
                data["note"] = f"已设置 {applied}x {symbol}"
            return data
        if last_exc:
            raise last_exc
        raise BinanceAPIError("无法设置杠杆")

    def set_leverage(self, symbol: str, leverage: int) -> dict:
        return self.apply_best_leverage(symbol, leverage)

    def book(self, symbol: str) -> tuple[Decimal, Decimal]:
        data = self.client.public("GET", "/fapi/v1/ticker/bookTicker", {"symbol": symbol}, futures=True)
        return d(data["bidPrice"]), d(data["askPrice"])

    def klines(self, symbol: str, interval: str = "1m", limit: int = 20) -> list:
        return self.client.public(
            "GET",
            "/fapi/v1/klines",
            {"symbol": symbol, "interval": interval, "limit": limit},
            futures=True,
        )

    def filters(self, symbol: str) -> tuple[Decimal, Decimal]:
        cached = getattr(self, "_filters_cache", {}).get(symbol)
        if cached:
            return cached
        info = self.client.public("GET", "/fapi/v1/exchangeInfo", futures=True)
        for item in info.get("symbols") or []:
            if item.get("symbol") != symbol:
                continue
            tick = Decimal("0.01")
            step = Decimal("0.001")
            for filt in item.get("filters") or []:
                if filt.get("filterType") == "PRICE_FILTER":
                    tick = d(filt.get("tickSize"), "0.01")
                if filt.get("filterType") == "LOT_SIZE":
                    step = d(filt.get("stepSize"), "0.001")
            self._filters_cache = {**getattr(self, "_filters_cache", {}), symbol: (tick, step)}
            return tick, step
        raise RuntimeError(f"找不到合约 {symbol}")

    def round_price(self, price: Decimal, tick: Decimal) -> Decimal:
        return (price / tick).to_integral_value(rounding=ROUND_DOWN) * tick

    def round_qty(self, qty: Decimal, step: Decimal) -> Decimal:
        return (qty / step).to_integral_value(rounding=ROUND_DOWN) * step

    def legs(self, symbol: str) -> HedgeLegs:
        rows = self._um_signed("GET", "/fapi/v2/positionRisk", "/papi/v1/um/positionRisk", {"symbol": symbol})
        if isinstance(rows, dict):
            rows = [rows]
        long_qty = short_qty = long_pnl = short_pnl = long_entry = short_entry = Decimal("0")
        for row in rows or []:
            if str(row.get("symbol")) != symbol:
                continue
            side = str(row.get("positionSide") or "").upper()
            amt = d(row.get("positionAmt"))
            pnl = d(row.get("unRealizedProfit"))
            entry = d(row.get("entryPrice"))
            if side == "LONG" or (side in {"BOTH", ""} and amt > 0):
                long_qty = abs(amt)
                long_pnl = pnl
                long_entry = entry
            elif side == "SHORT" or (side in {"BOTH", ""} and amt < 0):
                short_qty = abs(amt)
                short_pnl = pnl
                short_entry = entry
        return HedgeLegs(long_qty, short_qty, long_pnl, short_pnl, long_entry, short_entry)

    def available_usdt(self) -> Decimal:
        if self.unified:
            data = self.client.signed("GET", "/papi/v1/account", papi=True)
            return d(data.get("totalAvailableBalance") or data.get("actualEquity") or data.get("accountEquity"))
        data = self.client.signed("GET", "/fapi/v2/account", futures=True)
        return d(data.get("availableBalance"))

    def collateral_rates(self) -> dict[str, Decimal]:
        data = self.client.public("GET", "/sapi/v1/portfolio/collateralRate")
        rows = data if isinstance(data, list) else data.get("collateralRate") or data.get("data") or []
        rates: dict[str, Decimal] = {}
        for row in rows:
            asset = str(row.get("asset") or "").upper()
            if not asset:
                continue
            rates[asset] = d(row.get("collateralRate") or row.get("rate") or row.get("collateralRateLevel"))
        return rates

    def papi_balances(self) -> dict[str, Decimal]:
        data = self.client.signed("GET", "/papi/v1/balance", papi=True)
        rows = data if isinstance(data, list) else data.get("balances") or data.get("data") or []
        balances: dict[str, Decimal] = {}
        for row in rows:
            asset = str(row.get("asset") or "").upper()
            if not asset:
                continue
            total = d(row.get("totalWalletBalance"))
            if total <= 0:
                total = d(row.get("crossMarginAsset")) + d(row.get("umWalletBalance")) + d(row.get("cmWalletBalance"))
            balances[asset] = total
        return {k: v for k, v in balances.items() if v > 0}

    def earn_to_pm_balance(self, asset: str = "LDUSDT") -> Decimal:
        data = self.client.signed(
            "GET",
            "/sapi/v1/portfolio/earn-asset-balance",
            {"asset": asset.upper(), "transferType": "EARN_TO_FUTURE"},
        )
        if isinstance(data, list):
            total = Decimal("0")
            for row in data:
                total += d(row.get("amount") or row.get("balance") or row.get("transferableAmount"))
            return total
        return d(data.get("amount") or data.get("balance") or data.get("transferableAmount"))

    def earn_to_pm(self, asset: str, amount: Decimal) -> dict:
        return self.client.signed(
            "POST",
            "/sapi/v1/portfolio/earn-asset-transfer",
            {"asset": asset.upper(), "transferType": "EARN_TO_FUTURE", "amount": fmt_amount(amount)},
        )

    def spot_to_unified(self, asset: str, amount: Decimal) -> dict:
        return self.client.signed(
            "POST",
            "/sapi/v1/asset/transfer",
            {"type": "MAIN_MARGIN", "asset": asset.upper(), "amount": fmt_amount(amount)},
        )

    def collect_to_margin(self, asset: str = "USDT") -> dict:
        last_exc: BinanceAPIError | None = None
        try:
            return self.client.signed("POST", "/papi/v1/asset-collection", {"asset": asset.upper()}, papi=True)
        except BinanceAPIError as exc:
            last_exc = exc
        try:
            return self.client.signed("POST", "/papi/v1/auto-collection", {}, papi=True)
        except BinanceAPIError as exc:
            last_exc = exc
        if last_exc:
            raise last_exc
        return {"collected": False}

    def max_withdraw(self, asset: str = "USDT") -> Decimal:
        try:
            data = self.client.signed(
                "GET",
                "/papi/v1/margin/maxWithdraw",
                {"asset": asset.upper()},
                papi=True,
            )
        except BinanceAPIError:
            data = self.client.signed(
                "GET",
                "/sapi/v1/margin/maxTransferable",
                {"asset": asset.upper()},
            )
        if isinstance(data, list):
            total = Decimal("0")
            for row in data:
                if str(row.get("asset") or "").upper() in {"", asset.upper()}:
                    total += d(row.get("amount") or row.get("maxWithdrawAmount") or row.get("transferable"))
            return total
        return d(data.get("amount") or data.get("maxWithdrawAmount") or data.get("transferable"))

    def unified_to_spot(self, asset: str, amount: Decimal) -> dict:
        params = {"asset": asset.upper(), "amount": fmt_amount(amount)}
        try:
            return self.client.signed(
                "POST",
                "/sapi/v1/asset/transfer",
                {"type": "PORTFOLIO_MARGIN_MAIN", **params},
            )
        except BinanceAPIError:
            return self.client.signed(
                "POST",
                "/sapi/v1/asset/transfer",
                {"type": "MARGIN_MAIN", **params},
            )

    def account_risk(self) -> dict:
        if not self.unified:
            data = self.client.signed("GET", "/fapi/v2/account", futures=True)
            equity = d(data.get("totalMarginBalance") or data.get("availableBalance"))
            available = d(data.get("availableBalance"))
            return {"uni_mmr": Decimal("0"), "equity": equity, "available": available}
        data = self.client.signed("GET", "/papi/v1/account", papi=True)
        equity = d(data.get("actualEquity") or data.get("accountEquity") or data.get("totalAvailableBalance"))
        available = d(data.get("totalAvailableBalance") or data.get("accountEquity"))
        return {
            "uni_mmr": d(data.get("uniMMR")),
            "equity": equity,
            "available": available,
        }

    def uni_mmr(self) -> Decimal:
        if not self.unified:
            return Decimal("0")
        return self.account_risk()["uni_mmr"]

    def account_equity(self) -> Decimal:
        if self.unified:
            data = self.client.signed("GET", "/papi/v1/account", papi=True)
            return d(data.get("actualEquity") or data.get("accountEquity") or data.get("totalAvailableBalance"))
        data = self.client.signed("GET", "/fapi/v2/account", futures=True)
        return d(data.get("totalMarginBalance") or data.get("availableBalance"))

    def place_limit(
        self,
        symbol: str,
        side: str,
        position_side: str,
        qty: Decimal,
        price: Decimal,
        reduce_only: bool = False,
        time_in_force: str = "GTC",
    ) -> dict:
        params = {
            "symbol": symbol,
            "side": side,
            "positionSide": position_side,
            "type": "LIMIT",
            "timeInForce": time_in_force,
            "quantity": fmt_amount(qty),
            "price": fmt_amount(price),
        }
        if reduce_only and not self.unified:
            params["reduceOnly"] = "true"
        return self._um_signed("POST", "/fapi/v1/order", "/papi/v1/um/order", params)

    def place_maker(
        self,
        symbol: str,
        side: str,
        position_side: str,
        qty: Decimal,
        price: Decimal,
        reduce_only: bool = False,
    ) -> dict:
        return self.place_limit(symbol, side, position_side, qty, price, reduce_only, "GTX")

    def place_market(
        self,
        symbol: str,
        side: str,
        position_side: str,
        qty: Decimal,
        reduce_only: bool = False,
    ) -> dict:
        params = {
            "symbol": symbol,
            "side": side,
            "positionSide": position_side,
            "type": "MARKET",
            "quantity": fmt_amount(qty),
        }
        if reduce_only and not self.unified:
            params["reduceOnly"] = "true"
        return self._um_signed("POST", "/fapi/v1/order", "/papi/v1/um/order", params)

    def cancel_open(self, symbol: str) -> dict:
        try:
            return self._um_signed("DELETE", "/fapi/v1/allOpenOrders", "/papi/v1/um/allOpenOrders", {"symbol": symbol})
        except BinanceAPIError as exc:
            if exc.payload and isinstance(exc.payload, dict) and exc.payload.get("code") in (-2011, 2011, "-2011"):
                return {"cancelled": False, "reason": "no open orders"}
            raise
