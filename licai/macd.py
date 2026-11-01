"""按币对 MACD：默认用币安 K 线自算；可选外部 URL（支持 {symbol}）。

自动收利看金叉/死叉；入场看涨跌方向决定先挂哪一边。
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from decimal import Decimal
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import requests

from .config import d

log = logging.getLogger("licai.macd")

DEFAULT_URL = "https://c7.pro/capi/indicator"
# 公共行情，不需 API Key；与原 c7 分钟级交叉对齐
_BINANCE_KLINES = "https://fapi.binance.com/fapi/v1/klines"
_MACD_FAST = 12
_MACD_SLOW = 26
_MACD_SIGNAL = 9
_KLINE_LIMIT = 120
_CACHE_TTL = 55.0
# 失败后即使 force=True 也先退避，避免每个自动轮询都卡超时
_FAIL_BACKOFF = 180.0
# cache_key -> (mono_ts, state|None, err)
_cache: dict[str, tuple[float, "MacdState | None", str]] = {}
_last_fail_log: dict[str, float] = {}


@dataclass(frozen=True)
class MacdState:
    """最新一根：相对 Signal 的方向；若刚交叉则带 cross。"""

    bias: str  # bull | bear
    cross: str | None  # golden | death | None
    time: str
    macd: Decimal
    signal: Decimal
    hist: Decimal
    # 交叉前一根 |hist|（开口敞口）；无交叉则为当前 |hist|
    gap_abs: Decimal = Decimal("0")
    symbol: str = ""

    @property
    def kind(self) -> str:
        """兼容旧字段：golden | death（无交叉为空串）。"""
        return self.cross or ""

    @property
    def harvest_side(self) -> str:
        # 金叉收空、死叉收多；方向同理：多头方向收空、空头方向收多
        return "SHORT" if self.bias == "bull" else "LONG"

    @property
    def enter_first_side(self) -> str:
        # 上涨先挂多，下跌先挂空
        return "LONG" if self.bias == "bull" else "SHORT"

    @property
    def label(self) -> str:
        if self.cross == "golden":
            return "金叉"
        if self.cross == "death":
            return "死叉"
        return "上涨" if self.bias == "bull" else "下跌"


# 兼容旧名
MacdCross = MacdState


def _norm_symbol(symbol: str | None) -> str:
    return str(symbol or "").strip().upper()


def _cache_key(url: str | None, symbol: str, *, source: str) -> str:
    return f"{source}|{(url or '').strip()}|{symbol or '-'}"


def _parse_rows(payload) -> list[dict]:
    if not isinstance(payload, list):
        return []
    rows: list[dict] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        t = str(item.get("time") or "").strip()
        if not t:
            continue
        rows.append(
            {
                "time": t,
                "macd": d(item.get("MACD") or item.get("macd")),
                "signal": d(item.get("Signal") or item.get("signal")),
                "hist": d(item.get("MACD_Hist") or item.get("hist") or item.get("Histogram")),
            }
        )
    rows.sort(key=lambda r: r["time"])
    return rows


def _ema_series(values: list[Decimal], period: int) -> list[Decimal | None]:
    """标准 EMA；前 period-1 根为 None，第 period 根用 SMA 播种。"""
    out: list[Decimal | None] = [None] * len(values)
    if period <= 0 or len(values) < period:
        return out
    seed = sum(values[:period]) / Decimal(period)
    out[period - 1] = seed
    mult = Decimal(2) / Decimal(period + 1)
    prev = seed
    for i in range(period, len(values)):
        prev = (values[i] - prev) * mult + prev
        out[i] = prev
    return out


def _macd_rows_from_closes(closes: list[Decimal], times: list[str]) -> list[dict]:
    if len(closes) < _MACD_SLOW + _MACD_SIGNAL:
        return []
    ema_fast = _ema_series(closes, _MACD_FAST)
    ema_slow = _ema_series(closes, _MACD_SLOW)
    macd_line: list[Decimal | None] = [None] * len(closes)
    for i in range(len(closes)):
        if ema_fast[i] is None or ema_slow[i] is None:
            continue
        macd_line[i] = ema_fast[i] - ema_slow[i]
    # Signal = EMA of MACD line（只对已有 MACD 的点）
    macd_vals = [v for v in macd_line if v is not None]
    if len(macd_vals) < _MACD_SIGNAL:
        return []
    signal_on_macd = _ema_series(macd_vals, _MACD_SIGNAL)
    rows: list[dict] = []
    mi = 0
    for i, macd in enumerate(macd_line):
        if macd is None:
            continue
        sig = signal_on_macd[mi]
        mi += 1
        if sig is None:
            continue
        hist = macd - sig
        rows.append(
            {
                "time": times[i],
                "macd": macd,
                "signal": sig,
                "hist": hist,
            }
        )
    return rows


def _detect_state(rows: list[dict], *, symbol: str = "") -> MacdState | None:
    if not rows:
        return None
    curr = rows[-1]
    diff = curr["macd"] - curr["signal"]
    if diff > 0:
        bias = "bull"
    elif diff < 0:
        bias = "bear"
    else:
        return None
    cross = None
    prev_hist = curr["hist"]
    if len(rows) >= 2:
        prev = rows[-2]
        prev_hist = prev["hist"]
        prev_diff = prev["macd"] - prev["signal"]
        if prev_diff <= 0 and diff > 0:
            cross = "golden"
        elif prev_diff >= 0 and diff < 0:
            cross = "death"
    gap_abs = abs(prev_hist) if cross else abs(curr["hist"])
    return MacdState(
        bias=bias,
        cross=cross,
        time=str(curr["time"]),
        macd=curr["macd"],
        signal=curr["signal"],
        hist=curr["hist"],
        gap_abs=gap_abs,
        symbol=symbol,
    )


def _detect_cross(rows: list[dict], *, symbol: str = "") -> MacdState | None:
    state = _detect_state(rows, symbol=symbol)
    if state is None or not state.cross:
        return None
    return state


def _format_kline_time(open_ms: int) -> str:
    # 与 c7 类似的本地分钟串；用 UTC 分钟避免时区漂
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(int(open_ms) / 1000.0))


def _fetch_binance_macd_rows(symbol: str, *, timeout: float) -> list[dict]:
    """拉 1m K 线并丢掉未收盘那根，交叉只认已收盘的上一条。"""
    sym = _norm_symbol(symbol)
    if not sym:
        return []
    resp = requests.get(
        _BINANCE_KLINES,
        params={"symbol": sym, "interval": "1m", "limit": _KLINE_LIMIT},
        timeout=timeout,
        proxies={"http": None, "https": None},
    )
    resp.raise_for_status()
    raw = resp.json()
    if not isinstance(raw, list) or not raw:
        return []
    now_ms = int(time.time() * 1000)
    closes: list[Decimal] = []
    times: list[str] = []
    for row in raw:
        if not isinstance(row, (list, tuple)) or len(row) < 5:
            continue
        # index6=close_time；未收盘 K 的 close_time 还在未来 → 跳过
        close_ms = 0
        if len(row) > 6:
            try:
                close_ms = int(row[6])
            except Exception:
                close_ms = 0
        if close_ms > now_ms:
            continue
        closes.append(d(row[4]))
        times.append(_format_kline_time(int(row[0])))
    # 兜底：没带 close_time 时丢掉最后一根（多为当前分钟未收盘）
    if closes and (not isinstance(raw[-1], (list, tuple)) or len(raw[-1]) <= 6):
        closes = closes[:-1]
        times = times[:-1]
    return _macd_rows_from_closes(closes, times)


def _url_for_symbol(url: str, symbol: str) -> str:
    """支持 {symbol}；否则若像裸 indicator 地址则追加 symbol 查询参数。"""
    sym = _norm_symbol(symbol)
    if not url:
        return url
    if "{symbol}" in url:
        return url.replace("{symbol}", sym or "")
    if not sym:
        return url
    parts = urlsplit(url)
    q = dict(parse_qsl(parts.query, keep_blank_values=True))
    if "symbol" not in q and "{symbol}" not in url:
        q["symbol"] = sym
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(q), parts.fragment))


def _fetch_remote_rows(url: str, *, timeout: float) -> list[dict]:
    resp = requests.get(url, timeout=timeout, proxies={"http": None, "https": None})
    resp.raise_for_status()
    return _parse_rows(resp.json())


def _load_state(
    url: str | None,
    *,
    symbol: str | None = None,
    timeout: float = 4.0,
    force: bool = False,
) -> MacdState | None:
    sym = _norm_symbol(symbol)
    # 有币对：优先币安 K 线自算（c7 实际忽略 symbol）
    source = "binance" if sym else "remote"
    endpoint = (url or DEFAULT_URL).strip() or DEFAULT_URL
    key = _cache_key(endpoint, sym, source=source)
    now = time.monotonic()
    hit = _cache.get(key)
    if hit:
        ts, state, err = hit
        age = now - ts
        if err and age < _FAIL_BACKOFF:
            return state
        if not force and age < _CACHE_TTL:
            return state
    try:
        if sym:
            rows = _fetch_binance_macd_rows(sym, timeout=timeout)
            state = _detect_state(rows, symbol=sym)
        else:
            rows = _fetch_remote_rows(endpoint, timeout=timeout)
            state = _detect_state(rows, symbol="")
        _cache[key] = (now, state, "")
        return state
    except Exception as exc:
        last = _last_fail_log.get(key, 0.0)
        if now - last >= _FAIL_BACKOFF:
            log.warning(
                "MACD 指标拉取失败 symbol=%s（%ss 内不再重试）: %s",
                sym or "-",
                int(_FAIL_BACKOFF),
                exc,
            )
            _last_fail_log[key] = now
        # 有币对时币安失败：再试一次带 symbol 的远程 URL（兼容以后 c7 支持）
        if sym and endpoint:
            try:
                remote = _url_for_symbol(endpoint, sym)
                rows = _fetch_remote_rows(remote, timeout=timeout)
                state = _detect_state(rows, symbol=sym)
                _cache[key] = (now, state, "")
                return state
            except Exception:
                pass
        _cache[key] = (now, None, str(exc))
        return None


def fetch_macd_state(
    url: str | None = None,
    *,
    symbol: str | None = None,
    timeout: float = 4.0,
    force: bool = False,
) -> MacdState | None:
    """最新涨跌方向；失败返回 None。有 symbol 时按该币对算。"""
    return _load_state(url, symbol=symbol, timeout=timeout, force=force)


def fetch_macd_bias(
    url: str | None = None,
    *,
    symbol: str | None = None,
    timeout: float = 4.0,
    force: bool = False,
) -> str | None:
    """返回 bull / bear；失败 None。"""
    state = _load_state(url, symbol=symbol, timeout=timeout, force=force)
    return None if state is None else state.bias


def fetch_macd_cross(
    url: str | None = None,
    *,
    symbol: str | None = None,
    timeout: float = 4.0,
    force: bool = False,
) -> MacdState | None:
    """仅在刚交叉时返回；无交叉或失败返回 None。"""
    state = _load_state(url, symbol=symbol, timeout=timeout, force=force)
    if state is None or not state.cross:
        return None
    return state
