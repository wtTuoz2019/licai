"""c7.pro MACD 金叉/死叉信号，仅用于自动收利过滤。"""

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
_cache: dict[str, tuple[float, "MacdCross | None", str]] = {}


@dataclass(frozen=True)
class MacdCross:
    """最近两根 K 线是否刚发生金叉/死叉。"""

    kind: str  # golden | death
    time: str
    macd: Decimal
    signal: Decimal
    hist: Decimal

    @property
    def harvest_side(self) -> str:
        # 金叉收空、死叉收多
        return "SHORT" if self.kind == "golden" else "LONG"

    @property
    def label(self) -> str:
        return "金叉" if self.kind == "golden" else "死叉"


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


def _detect_cross(rows: list[dict]) -> MacdCross | None:
    if len(rows) < 2:
        return None
    prev, curr = rows[-2], rows[-1]
    prev_diff = prev["macd"] - prev["signal"]
    curr_diff = curr["macd"] - curr["signal"]
    kind = ""
    if prev_diff <= 0 and curr_diff > 0:
        kind = "golden"
    elif prev_diff >= 0 and curr_diff < 0:
        kind = "death"
    if not kind:
        return None
    return MacdCross(
        kind=kind,
        time=str(curr["time"]),
        macd=curr["macd"],
        signal=curr["signal"],
        hist=curr["hist"],
    )


def fetch_macd_cross(url: str | None = None, *, timeout: float = 4.0) -> MacdCross | None:
    """拉取指标；失败返回 None（自动收利视为无信号，不触发）。"""
    endpoint = (url or DEFAULT_URL).strip() or DEFAULT_URL
    now = time.monotonic()
    hit = _cache.get(endpoint)
    if hit and now - hit[0] < _CACHE_TTL:
        return hit[1]
    try:
        resp = requests.get(endpoint, timeout=timeout, proxies={"http": None, "https": None})
        resp.raise_for_status()
        rows = _parse_rows(resp.json())
        cross = _detect_cross(rows)
        _cache[endpoint] = (now, cross, "")
        return cross
    except Exception as exc:
        log.warning("MACD 指标拉取失败: %s", exc)
        # 短暂缓存失败，避免打爆接口；不沿用过期金叉以免误触发
        _cache[endpoint] = (now, None, str(exc))
        return None
