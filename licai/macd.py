"""c7.pro MACD：自动收利看金叉/死叉；入场看涨跌方向决定先挂哪一边。"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from decimal import Decimal

import requests

from .config import d

log = logging.getLogger("licai.macd")

DEFAULT_URL = "https://c7.pro/capi/indicator"
_CACHE_TTL = 55.0
# endpoint -> (mono_ts, state|None, err)
_cache: dict[str, tuple[float, "MacdState | None", str]] = {}


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
    # 交叉当下 hist≈0，用前柱衡量这波波动大小
    gap_abs: Decimal = Decimal("0")

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


def _detect_state(rows: list[dict]) -> MacdState | None:
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
    # 交叉时用前柱 |hist| 作敞口；平时用当前 |hist|
    gap_abs = abs(prev_hist) if cross else abs(curr["hist"])
    return MacdState(
        bias=bias,
        cross=cross,
        time=str(curr["time"]),
        macd=curr["macd"],
        signal=curr["signal"],
        hist=curr["hist"],
        gap_abs=gap_abs,
    )


def _detect_cross(rows: list[dict]) -> MacdState | None:
    """仅在刚交叉时返回。"""
    state = _detect_state(rows)
    if state is None or not state.cross:
        return None
    return state


def _load_state(url: str | None, *, timeout: float = 4.0) -> MacdState | None:
    endpoint = (url or DEFAULT_URL).strip() or DEFAULT_URL
    now = time.monotonic()
    hit = _cache.get(endpoint)
    if hit and now - hit[0] < _CACHE_TTL:
        return hit[1]
    try:
        resp = requests.get(endpoint, timeout=timeout, proxies={"http": None, "https": None})
        resp.raise_for_status()
        state = _detect_state(_parse_rows(resp.json()))
        _cache[endpoint] = (now, state, "")
        return state
    except Exception as exc:
        log.warning("MACD 指标拉取失败: %s", exc)
        _cache[endpoint] = (now, None, str(exc))
        return None


def fetch_macd_state(url: str | None = None, *, timeout: float = 4.0) -> MacdState | None:
    """最新涨跌方向；失败返回 None。"""
    return _load_state(url, timeout=timeout)


def fetch_macd_bias(url: str | None = None, *, timeout: float = 4.0) -> str | None:
    """返回 bull / bear；失败 None。"""
    state = _load_state(url, timeout=timeout)
    return None if state is None else state.bias


def fetch_macd_cross(url: str | None = None, *, timeout: float = 4.0) -> MacdState | None:
    """仅在刚交叉时返回；无交叉或失败返回 None。"""
    state = _load_state(url, timeout=timeout)
    if state is None or not state.cross:
        return None
    return state
