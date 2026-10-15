from __future__ import annotations

import json
import logging
import threading
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from . import earn as earn_mod
from .config import ROOT, d
from .macd import fetch_macd_cross

if TYPE_CHECKING:
    from .ops import OpsService

log = logging.getLogger("licai.autoharvest")

_EARN_DAY_PATH = ROOT / "data" / "earn_day_done.json"


def _print(msg: str) -> None:
    print(msg, flush=True)
    log.info(msg)


def _qty(legs: dict, key: str):
    return d((legs or {}).get(key) or "0")


class AutoHarvestWorker:
    """后台轮询：

    - 每日：拉全部理财奖励记录；仅开启自动收利、且 auto_switch_earn 时，按各产品额度自动换到更高年化
    - 账号开启「自动收利」时：无仓等金叉/死叉+严平稳开仓；有仓等交叉+浮盈达标收利
    手动一键入场/收利不受影响。
    """

    def __init__(self, ops: OpsService):
        self.ops = ops
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._last_cross_key = ""
        # account_id -> UTC 日期，每日理财检查只跑一次（落盘，重启不重试）
        self._earn_day_done: dict[int, str] = self._load_earn_day_done()

    @staticmethod
    def _load_earn_day_done() -> dict[int, str]:
        try:
            raw = json.loads(_EARN_DAY_PATH.read_text(encoding="utf-8"))
            return {int(k): str(v) for k, v in (raw or {}).items()}
        except Exception:
            return {}

    def _mark_earn_day_done(self, account_id: int, day: str) -> None:
        self._earn_day_done[account_id] = day
        try:
            _EARN_DAY_PATH.parent.mkdir(parents=True, exist_ok=True)
            payload = {str(k): v for k, v in self._earn_day_done.items()}
            _EARN_DAY_PATH.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        except Exception:
            log.exception("写入每日理财检查标记失败")

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="auto-harvest", daemon=True)
        self._thread.start()
        if bool(getattr(self.ops.base, "macd_auto_harvest", True)):
            note = "无仓/有仓都须金叉或死叉；有仓还要浮盈达到名义比例"
        else:
            note = "MACD 过滤已关：无仓严平稳开仓；有仓按名义比例收"
        extras = []
        if bool(getattr(self.ops.base, "auto_switch_earn", True)):
            extras.append("每日换低收益理财")
        if bool(getattr(self.ops.base, "auto_sweep_spot", True)):
            extras.append("闲置现货自动申购")
        if bool(getattr(self.ops.base, "auto_scale", True)):
            extras.append("金叉/死叉自动加仓")
        extra = "；".join(extras) if extras else "无额外理财自动化"
        _print(f"自动收利后台已启动（{note}；{extra}）")

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

    def _utc_day(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def _tick(self) -> None:
        for account in self.ops.store.list_accounts():
            if self._stop.is_set():
                return
            try:
                self._maybe_daily_earn(account)
            except Exception:
                log.exception("账号 %s 每日理财检查失败", account.id)
            if not account.auto_harvest:
                continue
            try:
                self._maybe_act(account)
            except Exception:
                log.exception("账号 %s 自动开仓/收利失败", account.id)

    def _maybe_daily_earn(self, account) -> None:
        """每日拉一次全部理财奖励；有低收益稳定币仓且安全时自动换到最高年化。"""
        day = self._utc_day()
        if self._earn_day_done.get(account.id) == day:
            return
        if not self.ops.try_begin_action(account.id):
            return
        try:
            # 清奖励缓存，强制走接口记录（不是年化粗算）
            earn_mod._YDAY_REWARD_CACHE.clear()
            self.ops.invalidate_snapshot(account.id)
            live = self.ops.snapshot(account, force=True)
            if not live.get("ok"):
                _print(f"每日理财检查跳过 account={account.id}：{live.get('error') or '快照失败'}")
                return
            yday = live.get("earn_yesterday") or "0"
            src = live.get("earn_yesterday_source") or ""
            _print(f"每日理财收益 account={account.id} 昨日={yday} source={src}")

            if not account.auto_harvest:
                _print(f"每日换年化跳过 account={account.id}：未开自动收利，只在手动点「换年化」时换")
                self._mark_earn_day_done(account.id, day)
                return

            if not bool(getattr(self.ops.base, "auto_switch_earn", True)):
                self._mark_earn_day_done(account.id, day)
                return

            switch = live.get("switch") or {}
            if not switch.get("needed"):
                _print(f"每日换年化跳过 account={account.id}：已是最高或无需换")
                self._mark_earn_day_done(account.id, day)
                return
            if not switch.get("can_click"):
                _print(
                    f"每日换年化暂缓 account={account.id}："
                    f"{switch.get('reason') or 'uniMMR/批次不够'}"
                )
                # 今天仍算检查过，避免每分钟重试打爆；安全线恢复后次日再换
                self._mark_earn_day_done(account.id, day)
                return

            # 先打上今日已处理，避免重启/异常后同一天反复空转
            self._mark_earn_day_done(account.id, day)
            _print(
                f"每日自动换年化 account={account.id} "
                f"→ {switch.get('target')} {switch.get('target_apr')}% · {switch.get('reason')}"
            )
            result = self.ops.run_action(
                account,
                "switch",
                force=False,
                mode=None,
                _locked=True,
                auto=True,
            )
            ok = bool(result.get("ok"))
            detail = result.get("reason") or ""
            steps = result.get("steps") or []
            if steps:
                fails = [s for s in steps if not s.get("ok")]
                pick = fails or steps[-4:]
                detail = "；".join(f"{s.get('name')}: {s.get('detail')}" for s in pick)
            paused = any(
                (s.get("name") == "暂停换产品") or ("先不动" in str(s.get("detail") or ""))
                for s in steps
            )
            if paused and ok:
                _print(
                    f"每日自动换年化结束 account={account.id} ok={ok} "
                    f"未实际换出（保证金占用/批次不足），今日不再重试：{detail[:400]}"
                )
            else:
                _print(f"每日自动换年化结束 account={account.id} ok={ok} {detail[:500]}")
        finally:
            self.ops.end_action(account.id)

    def _macd_side(self) -> tuple[str | None, str]:
        """仅金叉/死叉那一根才返回腿；否则 None。"""
        base = self.ops.base
        if not bool(getattr(base, "macd_auto_harvest", True)):
            return None, "MACD 过滤已关闭"
        url = getattr(base, "macd_indicator_url", None) or ""
        cross = fetch_macd_cross(url)
        if cross is None:
            return None, "当前不是金叉/死叉那一根"
        key = f"{cross.kind}:{cross.time}"
        if key == self._last_cross_key:
            return None, f"{cross.label}@{cross.time} 已处理过"
        return cross.harvest_side, f"{cross.label}@{cross.time} → 收{cross.harvest_side}"

    def _maybe_act(self, account) -> None:
        if not self.ops.try_begin_action(account.id):
            return
        did_trade = False
        try:
            live = self.ops.monitor(account)
            if not live.get("ok"):
                return
            # 现货加了 U：先申购理财入保证金（不等交叉）
            if self._maybe_sweep_spot(account, live):
                did_trade = True
                # 申购后权益变了，重拉再决定开仓/加仓
                live = self.ops.monitor(account)
                if not live.get("ok"):
                    return
            legs = live.get("legs") or {}
            flat = _qty(legs, "long_qty") <= 0 and _qty(legs, "short_qty") <= 0
            if flat:
                if self._maybe_enter(account, live):
                    did_trade = True
            else:
                # 有仓：优先收利；否则在好时机自动加仓
                if self._maybe_harvest(account, live):
                    did_trade = True
                elif self._maybe_scale(account, live):
                    did_trade = True
        finally:
            # 一整串（归集→开仓/收利/加仓）都结束后再等落账刷快照
            if did_trade:
                try:
                    self.ops.refresh_account_snapshot(account)
                except Exception:
                    log.exception("账号 %s 操作后刷新快照失败", account.id)
            self.ops.end_action(account.id)

    def _maybe_sweep_spot(self, account, live: dict) -> bool:
        """现货闲置稳定币 → 申购理财并入统一账户。返回是否实际跑过申购。"""
        if not bool(getattr(self.ops.base, "auto_sweep_spot", True)):
            return False
        spot = d(live.get("spot_usdt") or "0")
        if spot < 1:
            # monitor 旧版可能没带 spot_usdt；再读快照缓存
            spot = self.ops._idle_spot(account)
        # 其它稳定币闲置也从 wallet_status / holdings 难直接读；现货 USDT 是主路径
        if spot < 1:
            return False
        _print(f"自动归集现货 account={account.id} spot_usdt={spot}")
        result = self.ops.run_action(
            account,
            "enter",
            force=False,
            mode="earn_only",
            _locked=True,
            auto=True,
            with_snapshot=False,
        )
        ok = bool(result.get("ok"))
        detail = result.get("reason") or ""
        steps = result.get("steps") or []
        if steps:
            fails = [s for s in steps if not s.get("ok")]
            pick = fails or steps[-4:]
            detail = "；".join(f"{s.get('name')}: {s.get('detail')}" for s in pick)
        _print(f"自动归集现货结束 account={account.id} ok={ok} {detail[:500]}")
        return True

    def _maybe_scale(self, account, live: dict) -> bool:
        """有对冲且还能加：等金叉/死叉 + 入场级平稳，自动加仓。"""
        if not bool(getattr(self.ops.base, "auto_scale", True)):
            return False
        enter = live.get("enter") or {}
        stability = live.get("stability") or {}
        if not enter.get("scale_ok"):
            return False
        if not enter.get("stable_ok", stability.get("stable")):
            return False
        macd_note = "未启用 MACD 过滤"
        if bool(getattr(self.ops.base, "macd_auto_harvest", True)):
            side, macd_note = self._macd_side()
            if not side:
                return False
        _print(
            f"自动加仓触发 account={account.id} add={enter.get('add_qty')} "
            f"target={enter.get('target_qty')} · {macd_note}"
        )
        result = self.ops.run_action(
            account,
            "scale",
            force=False,
            mode=None,
            _locked=True,
            auto=True,
            with_snapshot=False,
        )
        ok = bool(result.get("ok"))
        detail = result.get("reason") or ""
        steps = result.get("steps") or []
        if steps:
            fails = [s for s in steps if not s.get("ok")]
            pick = fails or steps[-4:]
            detail = "；".join(f"{s.get('name')}: {s.get('detail')}" for s in pick)
        if ok and bool(getattr(self.ops.base, "macd_auto_harvest", True)):
            cross = fetch_macd_cross(getattr(self.ops.base, "macd_indicator_url", None))
            if cross:
                self._last_cross_key = f"{cross.kind}:{cross.time}"
        _print(f"自动加仓结束 account={account.id} ok={ok} {detail[:500]}")
        return True

    def _maybe_enter(self, account, live: dict) -> bool:
        """无仓：金叉/死叉那一根 + 严平稳，才自动双边开仓。"""
        stability = live.get("stability") or {}
        enter = live.get("enter") or {}
        if not stability.get("stable"):
            return False
        if enter.get("can_click") is False and not enter.get("stable_ok"):
            return False
        if not enter.get("stable_ok", stability.get("stable")):
            return False
        macd_note = "未启用 MACD 过滤"
        if bool(getattr(self.ops.base, "macd_auto_harvest", True)):
            side, macd_note = self._macd_side()
            if not side:
                return False
        _print(f"自动开仓触发 account={account.id} 严平稳 · {macd_note}，双边挂单入场")
        result = self.ops.run_action(
            account,
            "enter",
            force=False,
            mode=None,
            _locked=True,
            auto=True,
            with_snapshot=False,
        )
        ok = bool(result.get("ok"))
        detail = result.get("reason") or ""
        steps = result.get("steps") or []
        if steps:
            fails = [s for s in steps if not s.get("ok")]
            pick = fails or steps[-4:]
            detail = "；".join(f"{s.get('name')}: {s.get('detail')}" for s in pick)
        if ok and bool(getattr(self.ops.base, "macd_auto_harvest", True)):
            cross = fetch_macd_cross(getattr(self.ops.base, "macd_indicator_url", None))
            if cross:
                self._last_cross_key = f"{cross.kind}:{cross.time}"
        _print(f"自动开仓结束 account={account.id} ok={ok} {detail[:500]}")
        return True

    def _maybe_harvest(self, account, live: dict) -> bool:
        harvest = live.get("harvest") or {}
        legs = live.get("legs") or {}

        side, macd_note = self._macd_side()
        if bool(getattr(self.ops.base, "macd_auto_harvest", True)):
            if not side:
                return False
            # 必须交叉那一根 + 收利平稳(更宽) + 浮盈 + 冷却
            if not harvest.get("stable_ok") or not harvest.get("cooldown_ok"):
                return False
            if legs.get("missing"):
                return False
            min_profit = d(harvest.get("min_profit") or "0")
            pnl = d(legs.get("long_pnl" if side == "LONG" else "short_pnl") or "0")
            if pnl <= 0 or pnl < min_profit:
                return False
        else:
            if not harvest.get("can_click"):
                return False
            side = None
            macd_note = "未启用 MACD 过滤"

        _print(
            f"自动收利触发 account={account.id} side={side or harvest.get('winner')} "
            f"pnl={harvest.get('winner_pnl')} macd={macd_note}"
        )
        result: dict = {}
        try:
            result = self.ops.run_action(
                account,
                "harvest",
                force=False,
                mode=None,
                _locked=True,
                auto=True,
                harvest_side=side,
                with_snapshot=False,
            )
        except Exception as exc:
            log.exception("账号 %s 自动收利异常，尝试修复裸仓", account.id)
            result = {"ok": False, "reason": str(exc), "steps": []}
            self._repair_naked_hedge(account)
        ok = bool(result.get("ok"))
        detail = result.get("reason") or ""
        steps = result.get("steps") or []
        if steps:
            fails = [s for s in steps if not s.get("ok")]
            pick = fails or steps[-4:]
            detail = "；".join(f"{s.get('name')}: {s.get('detail')}" for s in pick)
        # 收利后无论成败，再确认一次没有单边敞口
        self._repair_naked_hedge(account)
        if ok and side:
            cross = fetch_macd_cross(getattr(self.ops.base, "macd_indicator_url", None))
            if cross and cross.harvest_side == side:
                self._last_cross_key = f"{cross.kind}:{cross.time}"
        _print(f"自动收利结束 account={account.id} ok={ok} {detail[:500]}")
        return True

    def _repair_naked_hedge(self, account) -> None:
        """收利后若仍单边，强行一键补仓/平裸仓，避免爆仓。"""
        try:
            live = self.ops.monitor(account)
        except Exception:
            log.exception("账号 %s 裸仓检查读仓失败", account.id)
            return
        legs = live.get("legs") or {}
        missing = legs.get("missing")
        if not missing:
            return
        _print(f"裸仓修复 account={account.id} missing={missing} → 强制入场补齐")
        try:
            self.ops.run_action(
                account,
                "enter",
                force=True,
                mode="force",
                _locked=True,
                auto=True,
                with_snapshot=False,
            )
        except Exception:
            log.exception("账号 %s 裸仓强制补仓失败", account.id)
