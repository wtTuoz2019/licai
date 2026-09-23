from __future__ import annotations

import logging
import threading
import time
from typing import TYPE_CHECKING

from .config import d
from .macd import fetch_macd_cross

if TYPE_CHECKING:
    from .ops import OpsService

log = logging.getLogger("licai.autoharvest")


def _print(msg: str) -> None:
    print(msg, flush=True)
    log.info(msg)


class AutoHarvestWorker:
    """后台轮询：账号开启自动收利且满足条件时执行收利。

    在原有浮盈/冷却/平稳条件之上，额外要求 c7 MACD：
    金叉 → 只收空；死叉 → 只收多。手动一键收利不受影响。
    """

    def __init__(self, ops: OpsService):
        self.ops = ops
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._last_cross_key = ""

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="auto-harvest", daemon=True)
        self._thread.start()
        _print("自动收利后台已启动")

    def stop(self) -> None:
        self._stop.set()
        t = self._thread
        if t and t.is_alive():
            t.join(timeout=5)
        self._thread = None

    def _interval(self) -> float:
        sec = int(getattr(self.ops.base, "auto_harvest_seconds", 0) or 0)
        if sec <= 0:
            sec = int(getattr(self.ops.base, "watch_seconds", 60) or 60)
        return max(60.0, float(sec))

    def _loop(self) -> None:
        while not self._stop.wait(self._interval()):
            try:
                self._tick()
            except Exception:
                log.exception("自动收利轮询异常")

    def _tick(self) -> None:
        for account in self.ops.store.list_accounts():
            if self._stop.is_set():
                return
            if not account.auto_harvest:
                continue
            try:
                self._maybe_harvest(account)
            except Exception:
                log.exception("账号 %s 自动收利失败", account.id)

    def _macd_side(self) -> tuple[str | None, str]:
        """返回 (harvest_side, 说明)。无信号时 side=None。"""
        base = self.ops.base
        if not bool(getattr(base, "macd_auto_harvest", True)):
            return None, "MACD 过滤已关闭"
        url = getattr(base, "macd_indicator_url", None) or ""
        cross = fetch_macd_cross(url)
        if cross is None:
            return None, "无金叉/死叉或指标不可用"
        key = f"{cross.kind}:{cross.time}"
        if key == self._last_cross_key:
            return None, f"{cross.label}@{cross.time} 已处理过"
        return cross.harvest_side, f"{cross.label}@{cross.time} → 收{cross.harvest_side}"

    def _maybe_harvest(self, account) -> None:
        if not self.ops.try_begin_action(account.id):
            return
        try:
            live = self.ops.monitor(account)
            if not live.get("ok"):
                return
            harvest = live.get("harvest") or {}
            legs = live.get("legs") or {}

            side, macd_note = self._macd_side()
            if bool(getattr(self.ops.base, "macd_auto_harvest", True)):
                if not side:
                    return
                # 指定腿：平稳/冷却/该腿浮盈达标（不要求当前 winner 就是这一腿）
                if not harvest.get("stable_ok") or not harvest.get("cooldown_ok"):
                    return
                if legs.get("missing"):
                    return
                min_profit = d(harvest.get("min_profit") or "0")
                pnl = d(legs.get("long_pnl" if side == "LONG" else "short_pnl") or "0")
                if pnl <= 0 or pnl < min_profit:
                    return
            else:
                # 关闭 MACD 过滤时走原逻辑
                if not harvest.get("can_click"):
                    return
                side = None
                macd_note = "未启用 MACD 过滤"

            _print(
                f"自动收利触发 account={account.id} side={side or harvest.get('winner')} "
                f"pnl={harvest.get('winner_pnl')} macd={macd_note}"
            )
            result = self.ops.run_action(
                account,
                "harvest",
                force=False,
                mode=None,
                _locked=True,
                auto=True,
                harvest_side=side,
            )
            ok = bool(result.get("ok"))
            detail = result.get("reason") or ""
            steps = result.get("steps") or []
            if steps:
                fails = [s for s in steps if not s.get("ok")]
                pick = fails or steps[-4:]
                detail = "；".join(f"{s.get('name')}: {s.get('detail')}" for s in pick)
            if ok and side:
                # 记住本根交叉，避免同一金叉/死叉重复打
                cross = fetch_macd_cross(getattr(self.ops.base, "macd_indicator_url", None))
                if cross and cross.harvest_side == side:
                    self._last_cross_key = f"{cross.kind}:{cross.time}"
            _print(f"自动收利结束 account={account.id} ok={ok} {detail[:500]}")
        finally:
            self.ops.end_action(account.id)
