from __future__ import annotations

import json
import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from . import earn as earn_mod
from .config import ROOT, d
from .cycle import HedgeCycle
from .macd import fetch_macd_cross
from .ops import pipeline_for

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

    - 每日：等理财利息到账后，自动换年化一次（需开自动收利且 auto_switch_earn）
    - 账号开启「自动收利」时：无仓须金叉/死叉与严平稳同时满足才开仓；
      有仓须金叉/死叉与收利级平稳同时满足，且浮盈达标才收利
    手动一键入场/收利不受影响。
    """

    def __init__(self, ops: OpsService):
        self.ops = ops
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        # account_id -> 已处理的金叉/死叉 key（多账号不能共用一根）
        self._last_cross_key: dict[int, str] = {}
        # account_id -> UTC 日期：利息到账并完成换年化检查后才记完成（落盘）
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
            note = "无仓/有仓都须金叉或死叉与平稳同时满足；有仓还要浮盈达到名义比例"
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
            # 开了自动收利且有旧套保仓：自动挂起迁仓，不必再点按钮
            try:
                self._maybe_auto_arm_migrate(account)
            except Exception:
                log.exception("账号 %s 自动挂起迁仓失败", account.id)
            # 换币迁仓挂起：与是否开自动收利无关，后台等到时机再执行
            try:
                self._maybe_armed_migrate(account)
            except Exception:
                log.exception("账号 %s 挂起迁仓检查失败", account.id)
            if not account.auto_harvest:
                continue
            try:
                self._maybe_act(account)
            except Exception:
                log.exception("账号 %s 自动开仓/收利失败", account.id)

    def _maybe_auto_arm_migrate(self, account) -> bool:
        """自动收利开启时：发现旧套保仓就挂起迁仓，等交叉+平稳后执行。"""
        if not account.auto_harvest:
            return False
        if self.ops.migrate_arm(account.id):
            return False
        try:
            live = self.ops.monitor(account)
        except Exception:
            return False
        if not live.get("ok"):
            return False
        if live.get("other_positions_error"):
            return False
        others = live.get("other_positions") or []
        if not others:
            return False
        tip = "、".join(
            f"{r.get('symbol')} 多{r.get('long_qty') or 0}/空{r.get('short_qty') or 0}"
            for r in others[:6]
        ) or "旧仓"
        sym = (live.get("symbol") or account.hedge_symbol or "").upper()
        self.ops.arm_migrate(account.id, force=False)
        detail = (
            f"自动挂起迁仓（等金叉/死叉与平稳）：{tip}，完成后开 {sym or '新套保'}。"
            "开了自动收利，无需再点「迁仓开新」。"
        )
        self.ops.store.add_event(account.id, "harvest", True, detail[:500])
        self.ops.touch_migrate_event(account.id)
        self.ops.invalidate_snapshot(account.id)
        _print(f"自动挂起迁仓 account={account.id} {detail[:200]}")
        return True

    def _maybe_armed_migrate(self, account) -> bool:
        """挂起的换币迁仓：时机到了才动手，否则写进度事件。"""
        arm = self.ops.migrate_arm(account.id)
        if not arm:
            return False
        if not self.ops.try_begin_action(account.id):
            return True
        try:
            force = bool(arm.get("force"))
            live = self.ops.monitor(account)
            read_err = ""
            if not live.get("ok"):
                read_err = str(live.get("error") or "监控失败")
            else:
                read_err = str(live.get("other_positions_error") or "")
            others = (live or {}).get("other_positions") or []
            if read_err:
                now = time.monotonic()
                if now - float(arm.get("last_event_at") or 0) >= 120:
                    # 读仓失败是中间态，只打控制台，不写页面事件刷屏
                    self.ops.touch_migrate_event(account.id)
                _print(f"迁仓挂起保留 account={account.id} 读仓失败 {read_err[:200]}")
                return True
            if not others:
                self.ops.clear_migrate_arm(account.id)
                self.ops.store.add_event(
                    account.id, "harvest", True, "迁仓挂起取消：旧仓已不在"
                )
                self.ops.invalidate_snapshot(account.id)
                _print(f"迁仓挂起取消 account={account.id}：旧仓已清")
                return True

            naked = [
                str(r.get("symbol") or "")
                for r in others
                if str(r.get("side") or "").upper() in {"LONG", "SHORT"} and r.get("symbol")
            ]
            if naked:
                try:
                    pipe = pipeline_for(account, self.ops.base, dry_run=False, ops=self.ops)
                    cycle = HedgeCycle(pipe)
                    rescue_steps = []
                    for sym in naked:
                        rescue_steps.extend(cycle.rescue_naked_symbol(sym))
                except Exception as exc:
                    log.exception("账号 %s 旧仓单边守卫失败", account.id)
                    self.ops.store.add_event(
                        account.id, "harvest", False, f"旧仓单边守卫失败：{exc}"[:500]
                    )
                    self.ops.touch_migrate_event(account.id)
                    return True
                fails = [s for s in rescue_steps if not getattr(s, "ok", True)]
                detail = "；".join(
                    f"{getattr(s, 'name', '')}: {getattr(s, 'detail', '')}"
                    for s in (fails or rescue_steps[-3:])
                )
                if fails:
                    self.ops.store.add_event(
                        account.id, "harvest", False, f"旧仓单边未平净：{detail}"[:500]
                    )
                    self.ops.touch_migrate_event(account.id)
                    _print(f"旧仓单边未平净 account={account.id} {detail[:200]}")
                    return True
                if rescue_steps:
                    self.ops.store.add_event(
                        account.id, "harvest", True, f"旧仓单边已市价平净：{detail}"[:500]
                    )
                    _print(f"旧仓单边已平 account={account.id} {detail[:200]}")
                again = self.ops.monitor(account)
                if again.get("ok") and not again.get("other_positions_error"):
                    left = again.get("other_positions") or []
                    if not left:
                        pipe = pipeline_for(account, self.ops.base, dry_run=False, ops=self.ops)
                        cycle = HedgeCycle(pipe)
                        open_steps = cycle.setup_once(force_hedge=True)
                        open_fails = [s for s in open_steps if not getattr(s, "ok", True)]
                        open_detail = "；".join(
                            f"{getattr(s, 'name', '')}: {getattr(s, 'detail', '')}"
                            for s in (open_fails or open_steps[-3:])
                        )
                        self.ops.store.add_event(
                            account.id,
                            "harvest",
                            not open_fails,
                            (("旧仓已清，开新对冲：" if not open_fails else "旧仓已清但开新失败：") + open_detail)[:500],
                        )
                        if not open_fails:
                            self.ops.clear_migrate_arm(account.id)
                        self.ops.refresh_account_snapshot(account)
                        return True
                self.ops.refresh_account_snapshot(account)
                return True

            tip = "、".join(
                f"{r.get('symbol')} 多{r.get('long_qty')}/空{r.get('short_qty')}"
                for r in others[:4]
            )
            now = time.monotonic()
            last_ev = float(arm.get("last_event_at") or 0)
            # 约每 2 分钟写一条待命进度
            if now - last_ev >= 120:
                waited = int(now - float(arm.get("armed_at") or now))
                note = (
                    f"迁仓待命：旧仓 {tip}，已挂起 {waited}s"
                    + ("（强行，下一轮即平）" if force else "，等金叉/死叉与平稳同时满足")
                )
                self.ops.store.add_event(account.id, "harvest", True, note[:500])
                self.ops.touch_migrate_event(account.id)

            prefer = "LONG"
            if not force:
                try:
                    pipe = pipeline_for(account, self.ops.base, dry_run=True, ops=self.ops)
                    cycle = HedgeCycle(pipe)
                    first_sym = str(others[0].get("symbol") or "")
                    ready, detail, prefer = cycle.migrate_timing_ready(first_sym)
                except Exception as exc:
                    ready, detail = False, str(exc)
                if not ready:
                    _print(f"迁仓待命 account={account.id} {detail}")
                    return True
                timing_note = detail
            else:
                timing_note = "强行迁仓，跳过等交叉/平稳"

            _print(f"迁仓执行 account={account.id} {timing_note} · {tip}")
            try:
                pipe = pipeline_for(account, self.ops.base, dry_run=False, ops=self.ops)
                cycle = HedgeCycle(pipe)
                steps = cycle.migrate_symbol_and_enter(
                    [str(r.get("symbol") or "") for r in others if r.get("symbol")],
                    wait_stable=False,
                    prefer=prefer,
                )
            except Exception as exc:
                log.exception("账号 %s 迁仓执行异常", account.id)
                self.ops.store.add_event(
                    account.id, "harvest", False, f"迁仓执行异常：{exc}"[:500]
                )
                self.ops.touch_migrate_event(account.id)
                return True

            ok = all(getattr(s, "ok", True) for s in steps) if steps else False
            # 允许部分步骤名含提示但仍 ok
            fails = [s for s in steps if not getattr(s, "ok", True)]
            ok = not fails
            detail = "；".join(
                f"{getattr(s, 'name', '')}: {getattr(s, 'detail', '')}"
                for s in (fails or steps[-4:])
            )
            self.ops.store.add_event(
                account.id,
                "harvest",
                ok,
                (("迁仓完成：" if ok else "迁仓未完成：") + detail)[:500],
            )
            if ok:
                self.ops.clear_migrate_arm(account.id)
            else:
                self.ops.touch_migrate_event(account.id)
            self.ops.refresh_account_snapshot(account)
            _print(f"迁仓结束 account={account.id} ok={ok} {detail[:300]}")
            return True
        finally:
            self.ops.end_action(account.id)

    def _interest_arrived(self, live: dict) -> bool:
        """理财利息是否已到账（奖励记录落在今天或昨天 UTC）。"""
        src = str(live.get("earn_yesterday_source") or "")
        amt = d(live.get("earn_yesterday") or "0")
        reward_day = str(live.get("earn_yesterday_day") or "")
        if src != "records" or amt <= 0 or not reward_day:
            return False
        today = self._utc_day()
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
        return reward_day in {today, yesterday}

    def _maybe_daily_earn(self, account) -> None:
        """利息到账后每日自动换年化一次；有低收益稳定币仓且安全时换到最高年化。"""
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
                _print(f"每日换年化跳过 account={account.id}：{live.get('error') or '快照失败'}")
                return
            yday = live.get("earn_yesterday") or "0"
            src = live.get("earn_yesterday_source") or ""
            reward_day = live.get("earn_yesterday_day") or ""
            _print(
                f"每日理财收益 account={account.id} 昨日={yday} day={reward_day} source={src}"
            )

            if not account.auto_harvest:
                _print(f"每日换年化跳过 account={account.id}：未开自动收利，只在手动点「换年化」时换")
                self._mark_earn_day_done(account.id, day)
                return

            if not bool(getattr(self.ops.base, "auto_switch_earn", True)):
                self._mark_earn_day_done(account.id, day)
                return

            earn_total = d(live.get("earn_total") or "0")
            if earn_total > 0 and not self._interest_arrived(live):
                _print(
                    f"每日换年化等待利息到账 account={account.id} "
                    f"昨日={yday} day={reward_day} source={src}"
                )
                return

            switch = live.get("switch") or {}
            if not switch.get("needed"):
                _print(f"每日换年化跳过 account={account.id}：已是最高或无需换")
                self._mark_earn_day_done(account.id, day)
                return
            if not switch.get("can_click"):
                _print(
                    f"每日换年化暂缓 account={account.id}："
                    f"{switch.get('reason') or 'uniMMR/批次不够'}（利息已到，安全线够了再换）"
                )
                return

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
            subscribed = any(
                s.get("ok") and ("申购" in str(s.get("name") or ""))
                for s in steps
            )
            margin_locked = any(
                s.get("name") == "暂停换产品"
                and (
                    "留作保证金" in str(s.get("detail") or "")
                    or "最多能转回" in str(s.get("detail") or "")
                )
                for s in steps
            )
            if paused:
                # 已换出一部分，或剩余被保证金锁死：今日不再空转刷事件
                if subscribed or margin_locked:
                    self._mark_earn_day_done(account.id, day)
                    why = "已换出一部分，剩余保证金占用" if subscribed else "可转额度被保证金占用"
                    _print(
                        f"每日自动换年化结束 account={account.id} ok={ok} "
                        f"{why}，今日不再重试：{detail[:400]}"
                    )
                    return
                _print(
                    f"每日自动换年化暂缓 account={account.id} ok={ok} "
                    f"碰到安全线，稍后重试：{detail[:400]}"
                )
                return
            if not ok:
                _print(
                    f"每日自动换年化失败 account={account.id} "
                    f"今日稍后重试：{detail[:500]}"
                )
                return
            # 真正换完才记今日完成
            self._mark_earn_day_done(account.id, day)
            _print(f"每日自动换年化结束 account={account.id} ok={ok} {detail[:500]}")
        finally:
            self.ops.end_action(account.id)

    def _macd_side(self, account_id: int) -> tuple[str | None, str]:
        """仅金叉/死叉那一根才返回腿；否则 None。"""
        base = self.ops.base
        if not bool(getattr(base, "macd_auto_harvest", True)):
            return None, "MACD 过滤已关闭"
        url = getattr(base, "macd_indicator_url", None) or ""
        cross = fetch_macd_cross(url, force=True)
        if cross is None:
            return None, "当前不是金叉/死叉那一根"
        key = f"{cross.kind}:{cross.time}"
        if self._last_cross_key.get(account_id) == key:
            return None, f"{cross.label}@{cross.time} 已处理过"
        return cross.harvest_side, f"{cross.label}@{cross.time} → 收{cross.harvest_side}"

    def _remember_cross(self, account_id: int) -> None:
        cross = fetch_macd_cross(getattr(self.ops.base, "macd_indicator_url", None))
        if cross:
            self._last_cross_key[account_id] = f"{cross.kind}:{cross.time}"

    def _maybe_act(self, account) -> None:
        if not self.ops.try_begin_action(account.id):
            return
        did_trade = False
        try:
            live = self.ops.monitor(account)
            if not live.get("ok"):
                return
            # 旧套保还在：只走迁仓，禁止在新币对上空开/收利/加仓
            others = live.get("other_positions") or []
            if others or (live.get("harvest") or {}).get("migrate"):
                if not self.ops.migrate_arm(account.id):
                    self._maybe_auto_arm_migrate(account)
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
        """有对冲且还能加：金叉/死叉与入场级平稳同时满足才加仓。"""
        if not bool(getattr(self.ops.base, "auto_scale", True)):
            return False
        if self.ops.migrate_arm(account.id):
            return False
        enter = live.get("enter") or {}
        stability = live.get("stability") or {}
        if not enter.get("scale_ok"):
            return False
        if not enter.get("stable_ok", stability.get("stable")):
            return False
        macd_note = "未启用 MACD 过滤"
        if bool(getattr(self.ops.base, "macd_auto_harvest", True)):
            side, macd_note = self._macd_side(account.id)
            if not side:
                return False
            # 动手前再确认交叉与平稳仍同时成立
            if not (live.get("stability") or {}).get("stable") and not enter.get("stable_ok"):
                return False
            cross = fetch_macd_cross(getattr(self.ops.base, "macd_indicator_url", None), force=True)
            if cross is None:
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
            # 已满仓/暂不加 不算真正加仓，不能吃掉这根交叉
            soft = any(
                (s.get("name") or "") in {"仓位已满", "暂不加仓", "加仓跳过"}
                for s in steps
            )
            if not soft:
                self._remember_cross(account.id)
        _print(f"自动加仓结束 account={account.id} ok={ok} {detail[:500]}")
        return True

    def _maybe_enter(self, account, live: dict) -> bool:
        """无仓：金叉/死叉与严平稳同时满足，才自动双边开仓。"""
        if self.ops.migrate_arm(account.id):
            return False
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
            side, macd_note = self._macd_side(account.id)
            if not side:
                return False
            cross = fetch_macd_cross(getattr(self.ops.base, "macd_indicator_url", None), force=True)
            if cross is None or not stability.get("stable"):
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
            self._remember_cross(account.id)
        _print(f"自动开仓结束 account={account.id} ok={ok} {detail[:500]}")
        return True

    def _maybe_harvest(self, account, live: dict) -> bool:
        harvest = live.get("harvest") or {}
        legs = live.get("legs") or {}
        if self.ops.migrate_arm(account.id) or harvest.get("migrate"):
            return False

        side, macd_note = self._macd_side(account.id)
        if bool(getattr(self.ops.base, "macd_auto_harvest", True)):
            # 金叉/死叉与收利级平稳必须同一轮同时满足（与迁仓一致）
            if not side:
                return False
            if not harvest.get("stable_ok"):
                return False
            if not harvest.get("cooldown_ok"):
                return False
            if legs.get("missing"):
                return False
            min_profit = d(harvest.get("min_profit") or "0")
            pnl = d(legs.get("long_pnl" if side == "LONG" else "short_pnl") or "0")
            if pnl <= 0 or pnl < min_profit:
                return False
        else:
            if harvest.get("migrate") or not harvest.get("can_click"):
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
                self._last_cross_key[account.id] = f"{cross.kind}:{cross.time}"
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
