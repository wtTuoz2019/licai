from __future__ import annotations

import threading
import time
from decimal import Decimal

import requests

from .config import d

FAPI = "https://fapi.binance.com"
SPOT = "https://api.binance.com"
BOOK_TTL = 2.0
KLINE_TTL = 20.0
FILTER_TTL = 6 * 3600.0
RATE_TTL = 10 * 60.0

_lock = threading.Lock()
_session: requests.Session | None = None
_books: dict[str, tuple[Decimal, Decimal]] = {}
_books_at = 0.0
_klines: dict[tuple[str, str, int], tuple[float, list]] = {}
_filters: dict[str, tuple[Decimal, Decimal]] = {}
_filters_at = 0.0
_rates: dict[str, Decimal] = {}
_rates_at = 0.0
_assets: set[str] = set()
_assets_at = 0.0


def _sess() -> requests.Session:
    global _session
    if _session is None:
        session = requests.Session()
        session.trust_env = False
        session.headers.update({"Accept": "application/json", "User-Agent": "licai-market"})
        _session = session
    return _session


def _get(url: str, params: dict | None = None) -> object:
    response = _sess().get(url, params=params, timeout=8)
    try:
        data = response.json()
    except ValueError as exc:
        raise RuntimeError(f"行情无法解析: {response.text[:200]}") from exc
    if response.status_code >= 400:
        msg = data.get("msg") if isinstance(data, dict) else str(data)
        raise RuntimeError(f"行情错误 {response.status_code}: {msg}")
    return data


def _refresh_books_locked() -> None:
    global _books, _books_at
    now = time.monotonic()
    if _books and now - _books_at < BOOK_TTL:
        return
    data = _get(f"{FAPI}/fapi/v1/ticker/bookTicker")
    rows = data if isinstance(data, list) else [data]
    books: dict[str, tuple[Decimal, Decimal]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol") or "")
        if not symbol:
            continue
        books[symbol] = (d(row.get("bidPrice")), d(row.get("askPrice")))
    if books:
        _books = books
        _books_at = now


def book(symbol: str) -> tuple[Decimal, Decimal]:
    symbol = symbol.upper()
    with _lock:
        try:
            _refresh_books_locked()
        except Exception:
            hit = _books.get(symbol)
            if hit:
                return hit
            raise
        hit = _books.get(symbol)
        if hit:
            return hit
    data = _get(f"{FAPI}/fapi/v1/ticker/bookTicker", {"symbol": symbol})
    if isinstance(data, list):
        data = data[0] if data else {}
    bid, ask = d(data.get("bidPrice")), d(data.get("askPrice"))
    with _lock:
        _books[symbol] = (bid, ask)
    return bid, ask


def klines(symbol: str, interval: str = "1m", limit: int = 20) -> list:
    symbol = symbol.upper()
    key = (symbol, interval, int(limit))
    now = time.monotonic()
    with _lock:
        hit = _klines.get(key)
        if hit and now - hit[0] < KLINE_TTL:
            return hit[1]
    rows = _get(
        f"{FAPI}/fapi/v1/klines",
        {"symbol": symbol, "interval": interval, "limit": int(limit)},
    )
    if not isinstance(rows, list):
        rows = []
    with _lock:
        _klines[key] = (time.monotonic(), rows)
    return rows


def _refresh_filters_locked() -> None:
    global _filters, _filters_at
    now = time.monotonic()
    if _filters and now - _filters_at < FILTER_TTL:
        return
    info = _get(f"{FAPI}/fapi/v1/exchangeInfo")
    filters: dict[str, tuple[Decimal, Decimal]] = {}
    for item in (info.get("symbols") if isinstance(info, dict) else None) or []:
        symbol = str(item.get("symbol") or "")
        if not symbol:
            continue
        tick = Decimal("0.01")
        step = Decimal("0.001")
        for filt in item.get("filters") or []:
            if filt.get("filterType") == "PRICE_FILTER":
                tick = d(filt.get("tickSize"), "0.01")
            if filt.get("filterType") == "LOT_SIZE":
                step = d(filt.get("stepSize"), "0.001")
        filters[symbol] = (tick, step)
    if filters:
        _filters = filters
        _filters_at = now


def filters(symbol: str) -> tuple[Decimal, Decimal]:
    symbol = symbol.upper()
    with _lock:
        _refresh_filters_locked()
        hit = _filters.get(symbol)
        if hit:
            return hit
    raise RuntimeError(f"找不到合约 {symbol}")


def collateral_rates() -> dict[str, Decimal]:
    global _rates, _rates_at
    now = time.monotonic()
    with _lock:
        if _rates and now - _rates_at < RATE_TTL:
            return dict(_rates)
    data = _get(f"{SPOT}/sapi/v1/portfolio/collateralRate")
    rows = data if isinstance(data, list) else (data.get("collateralRate") or data.get("data") or [] if isinstance(data, dict) else [])
    rates: dict[str, Decimal] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        asset = str(row.get("asset") or "").upper()
        if not asset:
            continue
        rates[asset] = d(row.get("collateralRate") or row.get("rate") or row.get("collateralRateLevel"))
    with _lock:
        _rates = rates
        _rates_at = time.monotonic()
    return dict(rates)


def margin_assets() -> set[str]:
    global _assets, _assets_at
    now = time.monotonic()
    with _lock:
        if _assets and now - _assets_at < RATE_TTL:
            return set(_assets)
    data = _get(f"{FAPI}/fapi/v1/assetIndex")
    rows = data if isinstance(data, list) else [data]
    assets: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol") or "")
        if symbol.endswith("USD"):
            assets.add(symbol[:-3].upper())
    assets.update({"USDT", "USDC", "BFUSD", "FDUSD", "BNFCR", "LDUSDT", "RWUSD", "USD1"})
    assets = {a for a in assets if a}
    with _lock:
        _assets = assets
        _assets_at = time.monotonic()
    return set(assets)
