from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Literal

from .client import BinanceAPIError, BinanceClient
from .config import d, fmt_amount


def _apr(value: object) -> Decimal:
    apr = d(value)
    if apr > 1:
        return apr / Decimal("100")
    return apr


SubscribeKind = Literal["flexible", "bfusd", "rwusd"]

_APR_KEYS = (
    "annualPercentageRate",
    "latestAnnualPercentageRate",
    "latestAnnualInterestRate",
    "apr",
    "rate",
)


def _apr_from_row(row: dict) -> Decimal:
    for key in _APR_KEYS:
        if row.get(key) not in (None, ""):
            apr = _apr(row.get(key))
            if apr > 0:
                return apr
    for key, value in row.items():
        name = str(key).lower()
        if any(skip in name for skip in ("time", "amount", "asset", "quota")):
            continue
        if any(mark in name for mark in ("apr", "rate", "interest")):
            apr = _apr(value)
            if apr > 0:
                return apr
    return Decimal("0")


_HOMEPAGE_OVERVIEW = (
    "https://www.binance.com/bapi/earn/v1/friendly/finance-earn/homepage/overview"
)


def _homepage_min_aprs(client: BinanceClient) -> dict[str, Decimal]:
    """热门产品预估年化。BFUSD / RWUSD 在各币种的 productSummary 里，取 minApr。"""
    found: dict[str, Decimal] = {}
    try:
        response = client.session.get(
            _HOMEPAGE_OVERVIEW,
            params={"pageSize": 100, "pageIndex": 1},
            headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json", "lang": "zh-CN"},
            timeout=client.timeout,
        )
        payload = response.json()
    except (OSError, ValueError):
        return found
    data = payload.get("data") if isinstance(payload, dict) else None
    items = data.get("list") if isinstance(data, dict) else None
    if not isinstance(items, list):
        return found
    for item in items:
        if not isinstance(item, dict):
            continue
        for summary in item.get("productSummary") or []:
            if not isinstance(summary, dict):
                continue
            kind = str(summary.get("productType") or "").upper()
            if kind not in {"BFUSD", "RWUSD"}:
                continue
            aprs = []
            for key in ("minApr", "maxApr"):
                apr = _apr(summary.get(key))
                if Decimal("0") < apr < Decimal("1"):
                    aprs.append(apr)
            if aprs:
                found[kind] = min(aprs)
    return found


def _latest_history_row(rows: list) -> dict:
    stamped = [row for row in rows if isinstance(row, dict)]
    if not stamped:
        return {}

    def stamp(row: dict) -> int:
        raw = row.get("time") or row.get("timestamp") or row.get("calcTime") or 0
        try:
            return int(raw)
        except (TypeError, ValueError):
            return 0

    newest = max(stamped, key=stamp)
    if stamp(newest) == 0:
        return stamped[0]
    return newest

_YDAY_REWARD_CACHE: dict[str, tuple[float, dict]] = {}
_YDAY_REWARD_TTL = 600.0


def _reward_query_window_ms(now_ms: int | None = None) -> tuple[int, int]:
    """覆盖最近一次发放：BFUSD 约 UTC 09:00（北京 17:00）入账，time 多为发放时刻。

    now_ms 应用币安校准后的时间（client.timestamp()），不要用本机裸时钟，
    否则本机快/慢一天就会查到空窗口。
    """
    if now_ms is None:
        now_ms = int(time.time() * 1000)
    now = datetime.fromtimestamp(now_ms / 1000.0, tz=timezone.utc)
    start = (now - timedelta(days=3)).replace(hour=0, minute=0, second=0, microsecond=0)
    return int(start.timestamp() * 1000), int(now_ms)


def _row_reward_amount(row: dict) -> Decimal:
    return d(
        row.get("rewardsAmount")  # BFUSD rewardsHistory
        or row.get("rewards")
        or row.get("amount")
        or row.get("reward")
        or row.get("interest")
        or row.get("assetReward")
    )


def _rewards_by_day(rows: list) -> dict[str, Decimal]:
    by_day: dict[str, Decimal] = {}
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        amt = _row_reward_amount(row)
        if amt <= 0:
            continue
        raw = row.get("time") or row.get("timestamp") or row.get("createTime")
        try:
            ts = int(raw)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            day = "unknown"
        else:
            if ts > 10_000_000_000:
                ts //= 1000
            day = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
        by_day[day] = by_day.get(day, Decimal("0")) + amt
    return by_day


def _latest_day_totals(flex_rows: list, bfusd_rows: list) -> tuple[Decimal, Decimal, Decimal]:
    flex_days = _rewards_by_day(flex_rows)
    bfusd_days = _rewards_by_day(bfusd_rows)
    days = set(flex_days) | set(bfusd_days)
    if not days:
        return Decimal("0"), Decimal("0"), Decimal("0")
    latest = max(days)
    flex = flex_days.get(latest, Decimal("0"))
    bfusd = bfusd_days.get(latest, Decimal("0"))
    return flex, bfusd, flex + bfusd


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
    margin_token: str = ""

    @property
    def purchasable(self) -> bool:
        status_ok = self.status.upper() in {"PURCHASING", "CREATED", "SUCCESS", ""}
        return self.can_purchase and not self.is_sold_out and status_ok and bool(self.product_id)

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "FlexibleProduct":
        sold_out = row.get("isSoldOut") if "isSoldOut" in row else row.get("sellOut")
        asset = str(row.get("asset") or "").upper()
        token = str(row.get("token") or "").upper()
        return cls(
            product_id=str(row.get("productId") or row.get("id") or ""),
            asset=asset,
            apr=_apr(row.get("latestAnnualPercentageRate") or row.get("latestAnnualInterestRate")),
            can_purchase=bool(row.get("canPurchase")),
            can_redeem=bool(row.get("canRedeem", True)),
            is_sold_out=bool(sold_out),
            min_purchase=d(row.get("minPurchaseAmount")),
            status=str(row.get("status") or ""),
            hot=bool(row.get("hot") or row.get("featured") or row.get("hotPush")),
            raw=row,
            margin_token=token or (f"LD{asset}" if asset else ""),
        )


class EarnAPI:
    PUBLIC_LIST = "https://www.binance.com/bapi/earn/v1/friendly/lending/daily/product/list"

    def __init__(self, client: BinanceClient):
        self.client = client
        self._spot: dict[str, Decimal] = {}
        self._positions: dict[str, list[dict]] = {}
        self._bfusd: Decimal | None = None
        self._rwusd: Decimal | None = None

    def _bust(self) -> None:
        self._spot.clear()
        self._positions.clear()
        self._bfusd = None
        self._rwusd = None

    def invalidate(self) -> None:
        self._bust()

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

    def _special_apr(self, path: str) -> Decimal:
        if not self.client.api_key:
            return Decimal("0")
        now_ms = self.client.timestamp()
        params = {
            "current": 1,
            "size": 10,
            "startTime": now_ms - 30 * 86400 * 1000,
            "endTime": now_ms,
        }
        apr = self._apr_from_history(path, params)
        if apr > 0 or not path.endswith("/rateHistory"):
            return apr
        return self._apr_from_history(path.replace("/rateHistory", "/rewardsHistory"), params)

    def _apr_from_history(self, path: str, params: dict) -> Decimal:
        try:
            data = self.client.signed("GET", path, params)
        except BinanceAPIError:
            return Decimal("0")
        rows = data.get("rows") if isinstance(data, dict) else None
        if not rows:
            return Decimal("0")
        return _apr_from_row(_latest_history_row(rows))

    def bfusd_apr(self) -> Decimal:
        rank, _small = self.bfusd_rate_pair()
        return rank

    def bfusd_rate_pair(self) -> tuple[Decimal, Decimal]:
        """返回 (800U以上的页面年化, 800U以内的更高一日)。

        理财页预估与近 7 日均值一致，给余额超过 800U 的账户用。
        最近一日更高时，那是 800U 以内的高档。用哪一档由账户资金决定。
        """
        latest, average = self._daily_and_average("/sapi/v1/bfusd/history/rateHistory", days=7)
        card = self._homepage_apr("BFUSD")
        base = card if card > 0 else average if average > 0 else latest
        small = latest if base > 0 and latest > base else Decimal("0")
        return base, small

    def bfusd_product(self) -> FlexibleProduct:
        base, small = self.bfusd_rate_pair()
        extra: dict[str, object] = {"base_apr": str(base)}
        if small > 0:
            extra["small_balance_apr"] = str(small)
        return self._special_product("BFUSD", "bfusd", base, extra)

    def _daily_and_average(self, path: str, days: int) -> tuple[Decimal, Decimal]:
        if not self.client.api_key:
            return Decimal("0"), Decimal("0")
        now_ms = self.client.timestamp()
        params = {
            "current": 1,
            "size": max(days, 10),
            "startTime": now_ms - 30 * 86400 * 1000,
            "endTime": now_ms,
        }
        try:
            data = self.client.signed("GET", path, params)
        except BinanceAPIError:
            return Decimal("0"), Decimal("0")
        rows = data.get("rows") if isinstance(data, dict) else None
        if not rows:
            return Decimal("0"), Decimal("0")
        def _stamp(row: dict) -> int:
            raw = row.get("time") or row.get("timestamp") or 0
            try:
                return int(raw)
            except (TypeError, ValueError):
                return 0

        ordered = sorted(
            (row for row in rows if isinstance(row, dict)),
            key=_stamp,
            reverse=True,
        )
        rates = [apr for row in ordered if (apr := _apr_from_row(row)) > 0]
        if not rates:
            return Decimal("0"), Decimal("0")
        window = rates[:days]
        average = sum(window) / Decimal(len(window))
        return rates[0], average

    def _homepage_apr(self, product_type: str) -> Decimal:
        """币安理财「热门产品」里该产品的预估年化（minApr）。"""
        now = time.monotonic()
        cached = getattr(self, "_homepage_aprs", None)
        cached_at = getattr(self, "_homepage_aprs_at", 0.0)
        if cached is None or now - cached_at > 60:
            cached = _homepage_min_aprs(self.client)
            self._homepage_aprs = cached
            self._homepage_aprs_at = now
        return cached.get(product_type.upper(), Decimal("0"))

    def subscribe_rwusd(self, amount: Decimal, source_asset: str = "USDT") -> dict:
        self._bust()
        return self.client.signed(
            "POST",
            "/sapi/v1/rwusd/subscribe",
            {"asset": source_asset.upper(), "amount": fmt_amount(amount)},
        )

    def rwusd_apr(self) -> Decimal:
        return self._special_apr("/sapi/v1/rwusd/history/rateHistory")

    def rwusd_product(self) -> FlexibleProduct:
        return self._special_product("RWUSD", "rwusd", self.rwusd_apr())

    def _special_product(
        self,
        asset: str,
        kind: SubscribeKind,
        apr: Decimal,
        extra: dict | None = None,
    ) -> FlexibleProduct:
        raw: dict[str, object] = {"source": kind}
        if extra:
            raw.update(extra)
        return FlexibleProduct(
            product_id=asset,
            asset=asset,
            apr=apr,
            can_purchase=True,
            can_redeem=True,
            is_sold_out=False,
            min_purchase=Decimal("0.1"),
            status="PURCHASING",
            hot=True,
            raw=raw,
            kind=kind,
            margin_token=asset,
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

    def rwusd_balance(self) -> Decimal:
        if self._rwusd is not None:
            return self._rwusd
        try:
            data = self.client.signed("GET", "/sapi/v1/rwusd/account")
        except BinanceAPIError:
            self._rwusd = Decimal("0")
            return self._rwusd
        self._rwusd = d(data.get("rwusdAmount") or data.get("totalAmount") or data.get("amount"))
        return self._rwusd

    def redeem_rwusd(self, amount: Decimal, redeem_type: str = "FAST") -> dict:
        self._bust()
        return self.client.signed(
            "POST",
            "/sapi/v1/rwusd/redeem",
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
        total += self.rwusd_balance()
        return total

    def yesterday_earn_reward(self) -> dict[str, object]:
        """理财昨日收益：只认奖励记录，没有就返回 0，不做估算。"""
        key = (self.client.api_key or "")[:12]
        now = time.monotonic()
        hit = _YDAY_REWARD_CACHE.get(key)
        if hit and now - hit[0] < _YDAY_REWARD_TTL:
            return hit[1]
        start_ms, end_ms = _reward_query_window_ms(self.client.timestamp())
        # 再扩一档：最近 30 天，避免刚申购后窗口太窄 / 发放日对不齐
        start30 = end_ms - 30 * 86400 * 1000
        flex_rows: list = []
        bfusd_rows: list = []
        errors: list[str] = []

        def _pull_flex(start: int, end: int) -> list:
            out_rows: list = []
            for typ in ("BONUS", "REALTIME"):
                try:
                    data = self.client.signed(
                        "GET",
                        "/sapi/v1/simple-earn/flexible/history/rewardsRecord",
                        {
                            "type": typ,
                            "asset": "USDT",
                            "startTime": start,
                            "endTime": end,
                            "size": 100,
                            "current": 1,
                        },
                    )
                    rows = data.get("rows") if isinstance(data, dict) else []
                    if isinstance(rows, list):
                        out_rows.extend(rows)
                except BinanceAPIError as exc:
                    errors.append(f"flex/{typ}: {exc}")
            return out_rows

        def _pull_bfusd(start: int, end: int) -> list:
            try:
                data = self.client.signed(
                    "GET",
                    "/sapi/v1/bfusd/history/rewardsHistory",
                    {"startTime": start, "endTime": end, "size": 100, "current": 1},
                )
                rows = data.get("rows") if isinstance(data, dict) else []
                return rows if isinstance(rows, list) else []
            except BinanceAPIError as exc:
                errors.append(f"bfusd: {exc}")
                return []

        flex_rows = _pull_flex(start_ms, end_ms)
        bfusd_rows = _pull_bfusd(start_ms, end_ms)
        if not flex_rows and not bfusd_rows:
            flex_rows = _pull_flex(start30, end_ms)
            bfusd_rows = _pull_bfusd(start30, end_ms)

        # 兜底：资金分红里可能有 BFUSD/USDT 利息入账
        dividend_rows: list = []
        for asset in ("USDT", "BFUSD"):
            try:
                data = self.client.signed(
                    "GET",
                    "/sapi/v1/asset/assetDividend",
                    {"asset": asset, "startTime": start30, "endTime": end_ms, "limit": 50},
                )
                rows = data.get("rows") if isinstance(data, dict) else []
                if isinstance(rows, list):
                    dividend_rows.extend(rows)
            except BinanceAPIError as exc:
                errors.append(f"dividend/{asset}: {exc}")

        flex, bfusd, total = _latest_day_totals(flex_rows, bfusd_rows)
        if total <= 0 and dividend_rows:
            div_amt = _rewards_by_day(dividend_rows)
            if div_amt:
                latest = max(div_amt)
                total = div_amt[latest]
                bfusd = total

        source = "records" if total > 0 else ("error" if errors and not flex_rows and not bfusd_rows else "none")
        out = {
            "amount": total,
            "flex": flex,
            "bfusd": bfusd,
            "source": source,
            "text": fmt_amount(total, 4) if total > 0 else "0",
            "errors": errors[:6],
        }
        _YDAY_REWARD_CACHE[key] = (now, out)
        return out
