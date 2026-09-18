from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Literal

from .client import BinanceAPIError, BinanceClient
from .config import d, fmt_amount


SubscribeKind = Literal["flexible", "bfusd"]


@dataclass
class FlexibleProduct:
    product_id: str
    asset: str
    apr: Decimal
    can_purchase: bool
    can_redeem: bool
    is_sold_out: bool
    min_purchase: Decimal
    status: str
    hot: bool
    raw: dict
    kind: SubscribeKind = "flexible"

    @property
    def purchasable(self) -> bool:
        status_ok = self.status.upper() in {"PURCHASING", "CREATED", "SUCCESS", ""}
        return self.can_purchase and not self.is_sold_out and status_ok and bool(self.product_id)

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "FlexibleProduct":
        sold_out = row.get("isSoldOut") if "isSoldOut" in row else row.get("sellOut")
        return cls(
            product_id=str(row.get("productId") or row.get("id") or ""),
            asset=str(row.get("asset") or "").upper(),
            apr=d(row.get("latestAnnualPercentageRate") or row.get("latestAnnualInterestRate")),
            can_purchase=bool(row.get("canPurchase")),
            can_redeem=bool(row.get("canRedeem", True)),
            is_sold_out=bool(sold_out),
            min_purchase=d(row.get("minPurchaseAmount")),
            status=str(row.get("status") or ""),
            hot=bool(row.get("hot") or row.get("featured") or row.get("hotPush")),
            raw=row,
        )


class EarnAPI:
    PUBLIC_LIST = "https://www.binance.com/bapi/earn/v1/friendly/lending/daily/product/list"

    def __init__(self, client: BinanceClient):
        self.client = client
        self._spot: dict[str, Decimal] = {}
        self._positions: dict[str, list[dict]] = {}
        self._bfusd: Decimal | None = None

    def _bust(self) -> None:
        self._spot.clear()
        self._positions.clear()
        self._bfusd = None

    def list_flexible(self) -> list[FlexibleProduct]:
        products: list[FlexibleProduct] = []
        page = 1
        while True:
            data = self.client.signed(
                "GET",
                "/sapi/v1/simple-earn/flexible/list",
                {"current": page, "size": 100},
            )
            rows = data.get("rows") or []
            products.extend(FlexibleProduct.from_row(row) for row in rows)
            total = int(data.get("total") or 0)
            if page * 100 >= total or not rows:
                break
            page += 1
        return products

    def list_flexible_public(self, asset: str | None = None) -> list[FlexibleProduct]:
        products: list[FlexibleProduct] = []
        page = 1
        while True:
            params: dict[str, object] = {"pageIndex": page, "pageSize": 50}
            if asset:
                params["asset"] = asset
            response = self.client.session.get(
                self.PUBLIC_LIST,
                params=params,
                headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
                timeout=self.client.timeout,
            )
            try:
                data = response.json()
            except ValueError as exc:
                raise RuntimeError(f"公开理财接口无法解析: {response.text[:300]}") from exc
            if not response.ok or str(data.get("code")) not in {"000000", "0"}:
                raise RuntimeError(f"公开理财接口失败: {data}")
            rows = data.get("data") or []
            products.extend(FlexibleProduct.from_row(row) for row in rows)
            total = int(data.get("total") or 0)
            if page * 50 >= total or not rows:
                break
            page += 1
        return products

    def subscribe_bfusd(self, amount: Decimal, source_asset: str = "USDT") -> dict:
        self._bust()
        return self.client.signed(
            "POST",
            "/sapi/v1/bfusd/subscribe",
            {"asset": source_asset.upper(), "amount": fmt_amount(amount)},
        )

    def bfusd_apr(self) -> Decimal:
        try:
            data = self.client.signed(
                "GET",
                "/sapi/v1/bfusd/history/rateHistory",
                {"current": 1, "size": 1},
            )
        except BinanceAPIError:
            return Decimal("0")
        rows = data.get("rows") or []
        if not rows:
            return Decimal("0")
        row = rows[0]
        return d(
            row.get("annualPercentageRate")
            or row.get("latestAnnualPercentageRate")
            or row.get("apr")
            or row.get("rate")
        )

    def bfusd_product(self) -> FlexibleProduct:
        apr = self.bfusd_apr() if self.client.api_key else Decimal("0")
        return FlexibleProduct(
            product_id="BFUSD",
            asset="BFUSD",
            apr=apr,
            can_purchase=True,
            can_redeem=True,
            is_sold_out=False,
            min_purchase=Decimal("0.1"),
            status="PURCHASING",
            hot=True,
            raw={"source": "bfusd"},
            kind="bfusd",
        )

    def left_quota(self, product_id: str) -> Decimal:
        data = self.client.signed(
            "GET",
            "/sapi/v1/simple-earn/flexible/personalLeftQuota",
            {"productId": product_id},
        )
        return d(data.get("leftPersonalQuota"))

    def subscribe(self, product_id: str, amount: Decimal, source_account: str = "SPOT") -> dict:
        self._bust()
        return self.client.signed(
            "POST",
            "/sapi/v1/simple-earn/flexible/subscribe",
            {
                "productId": product_id,
                "amount": fmt_amount(amount),
                "autoSubscribe": "true",
                "sourceAccount": source_account,
            },
        )

    def redeem(self, product_id: str, amount: Decimal | None = None, redeem_all: bool = False) -> dict:
        params: dict[str, object] = {
            "productId": product_id,
            "redeemAll": str(redeem_all).lower(),
            "destAccount": "SPOT",
        }
        if not redeem_all:
            if amount is None:
                raise ValueError("赎回必须指定 amount 或 redeem_all")
            params["amount"] = fmt_amount(amount)
        self._bust()
        return self.client.signed("POST", "/sapi/v1/simple-earn/flexible/redeem", params)

    def positions(self, asset: str | None = None) -> list[dict]:
        key = (asset or "").upper()
        hit = self._positions.get(key)
        if hit is not None:
            return hit
        params = {"size": 100, "current": 1}
        if asset:
            params["asset"] = asset
        data = self.client.signed("GET", "/sapi/v1/simple-earn/flexible/position", params)
        rows = data.get("rows") or []
        self._positions[key] = rows
        return rows

    def spot_free(self, asset: str) -> Decimal:
        name = asset.upper()
        if name in self._spot:
            return self._spot[name]
        data = self.client.signed("GET", "/api/v3/account")
        found = Decimal("0")
        for item in data.get("balances") or []:
            token = str(item.get("asset", "")).upper()
            free = d(item.get("free"))
            self._spot[token] = free
            if token == name:
                found = free
        if name not in self._spot:
            self._spot[name] = found
        return found

    def bfusd_balance(self) -> Decimal:
        if self._bfusd is not None:
            return self._bfusd
        try:
            data = self.client.signed("GET", "/sapi/v1/bfusd/account")
        except BinanceAPIError:
            self._bfusd = Decimal("0")
            return self._bfusd
        self._bfusd = d(data.get("bfusdAmount") or data.get("totalAmount") or data.get("amount"))
        return self._bfusd

    def redeem_bfusd(self, amount: Decimal, redeem_type: str = "FAST") -> dict:
        self._bust()
        return self.client.signed(
            "POST",
            "/sapi/v1/bfusd/redeem",
            {"amount": fmt_amount(amount), "type": redeem_type},
        )

    def earn_margin_usdt(self) -> Decimal:
        total = self.spot_free("USDT")
        try:
            for row in self.positions("USDT"):
                total += d(row.get("totalAmount") or row.get("amount"))
        except BinanceAPIError:
            pass
        total += self.bfusd_balance()
        return total
