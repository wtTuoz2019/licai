from __future__ import annotations

import logging
import threading
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .ops import OpsService

log = logging.getLogger("licai.autoharvest")


def _print(msg: str) -> None:
    print(msg, flush=True)
    log.info(msg)


class AutoHarvestWorker:
    """后台轮询：账号开启自动收利且满足 can_click 时执行收利。"""

    def __init__(self, ops: OpsService):
        self.ops = ops
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

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
        return max(15.0, float(sec))

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

    def _maybe_harvest(self, account) -> None:
        if not self.ops.try_begin_action(account.id):
            return
        try:
            live = self.ops.monitor(account)
            if not live.get("ok"):
                return
            harvest = live.get("harvest") or {}
            if not harvest.get("can_click"):
                return
            _print(
                f"自动收利触发 account={account.id} winner={harvest.get('winner')} pnl={harvest.get('winner_pnl')}"
            )
            result = self.ops.run_action(account, "harvest", force=False, mode=None, _locked=True, auto=True)
            ok = bool(result.get("ok"))
            detail = result.get("reason") or ""
            steps = result.get("steps") or []
            if steps:
                fails = [s for s in steps if not s.get("ok")]
                pick = fails or steps[-4:]
                detail = "；".join(f"{s.get('name')}: {s.get('detail')}" for s in pick)
            _print(f"自动收利结束 account={account.id} ok={ok} {detail[:500]}")
        finally:
            self.ops.end_action(account.id)
