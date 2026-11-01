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

    - 每日：理财利息到账后，换年化 + 划入保证金 + 扫闲置各做一次（不反复扫）
    - 账号开启「自动收利」时：无仓须金叉/死叉与严平稳同时满足才开仓；
      有仓浮盈达标先预扫理财产品，再等交叉+平稳收利
    手动一键入场/收利不受影响。
    """

    def __init__(self, ops: OpsService):
        self.ops = ops
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        # account_id -> 已处理的金叉/死叉 key（多账号不能共用一根）
        self._last_cross_key: dict[int, str] = {}
        # account_id -> UTC 日期：利息到账并完成每日管家后才记完成（落盘）
        self._earn_day_done: dict[int, str] = self._load_earn_day_done()
        # 等利息期间每账号最多半小时强刷一次快照，避免每分钟打爆币安
        self._earn_probe_at: dict[int, float] = {}
        self._earn_probe_gap = 1800.0
        # 本轮按币对各刷一次 MACD（同币对多账号共用，避免重复 force）
        self._tick_crosses: dict[str, object] = {}
        # 浮盈已达标时提前扫好的理财产品：account_id -> {product, detail, at}
        # 年化很少变，缓存拉长到 24h，避免反复打产品列表
        self._harvest_prep: dict[int, dict] = {}
        self._harvest_prep_ttl = 86400.0
        # 账号下次完整检查时间；空闲（无仓/无迁仓/无挂起加仓）拉长，有交叉立刻查
        self._due_at: dict[int, float] = {}
        self._idle_gap = 180.0

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
            extras.append("利息到账后每日换年化一次")
        if bool(getattr(self.ops.base, "auto_fund_margin", True)):
            extras.append("利息到账后每日划入保证金一次")
        if bool(getattr(self.ops.base, "auto_sweep_spot", True)):
            extras.append("利息到账后每日扫闲置一次")
        extras.append("浮盈达标先预扫理财；账户挂起自动加仓")
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

    def _mark_due(self, account_id: int, gap: float) -> None:
        # 错开账号，避免整点一起打币安
        stagger = float((int(account_id) * 11) % 17)
        self._due_at[account_id] = time.monotonic() + max(30.0, float(gap)) + stagger

    def _is_due(self, account_id: int) -> bool:
        return time.monotonic() >= float(self._due_at.get(account_id) or 0)

    def _likely_has_pos(self, account) -> bool:
        hit = self.ops._monitor_cache.get(account.id)
        if not hit:
            return True
        legs = (hit[1] or {}).get("legs") or {}
        return _qty(legs, "long_qty") > 0 or _qty(legs, "short_qty") > 0

    def _is_idle(self, account, *, pending_scale: bool) -> bool:
        if pending_scale or self.ops.migrate_arm(account.id):
            return False
        hit = self.ops._monitor_cache.get(account.id)
        if not hit:
            return False
        live = hit[1] or {}
        legs = live.get("legs") or {}
        if (legs.get("missing") or "") in {"LONG", "SHORT", "IMBALANCE"}:
            return False
        if live.get("other_positions"):
            return False
        return _qty(legs, "long_qty") <= 0 and _qty(legs, "short_qty") <= 0

    def _loop(self) -> None:
        while not self._stop.wait(self._interval()):
            try:
                self._tick()
            except Exception:
                log.exception("自动收利轮询异常")

    def _utc_day(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def _account_symbol(self, account) -> str:
        return str(getattr(account, "hedge_symbol", "") or "").strip().upper()

    def _housekeep(self, account) -> None:
        """利息到账后的每日一次管家（换年化/划入/扫闲置）。"""
        try:
            self._maybe_daily_housekeep(account)
        except Exception:
            log.exception("账号 %s 每日理财管家失败", account.id)

    def _tick(self) -> None:
        url = getattr(self.ops.base, "macd_indicator_url", None) or ""
        accounts = list(self.ops.store.list_accounts())
        # 整轮按币对各强刷一次；失败退避由 macd 模块处理
        symbols = sorted(
            {
                self._account_symbol(a)
                for a in accounts
                if self._account_symbol(a)
            }
        )
        self._tick_crosses = {}
        for sym in symbols:
            try:
                self._tick_crosses[sym] = fetch_macd_cross(url, symbol=sym, force=True)
            except Exception:
                self._tick_crosses[sym] = None
        for account in accounts:
            if self._stop.is_set():
                return
            armed = bool(self.ops.migrate_arm(account.id))
            pending_scale = (
                bool(getattr(account, "auto_scale", False))
                and account.scale_add_qty is not None
                and float(account.scale_add_qty) > 0
            )
            sym = self._account_symbol(account)
            cross_now = bool(sym and self._tick_crosses.get(sym) is not None)
            # 交叉/迁仓窗口：先交易，换年化·划入保证金放到后面，避免拖死窗口
            window_open = cross_now or armed
            if not window_open:
                self._housekeep(account)
            # 已挂起迁仓：每轮都查，别错过交叉+平稳窗口
            if armed:
                try:
                    self._maybe_armed_migrate(account)
                except Exception:
                    log.exception("账号 %s 挂起迁仓检查失败", account.id)
            if not account.auto_harvest and not pending_scale:
                if window_open:
                    self._housekeep(account)
                continue
            # 本币对有交叉 / 有仓 / 挂起加仓 / 迁仓：按正常间隔；纯空仓空闲拉到 ~3 分钟
            urgent = (
                cross_now
                or armed
                or pending_scale
                or self._likely_has_pos(account)
            )
            if not urgent and not self._is_due(account.id):
                if window_open:
                    self._housekeep(account)
                continue
            if account.auto_harvest and not armed:
                try:
                    self._maybe_auto_arm_migrate(account)
                except Exception:
                    log.exception("账号 %s 自动挂起迁仓失败", account.id)
            try:
                self._maybe_act(account)
            except Exception:
                log.exception("账号 %s 自动开仓/收利/加仓失败", account.id)
            if window_open:
                self._housekeep(account)
            idle = self._is_idle(account, pending_scale=pending_scale)
            gap = self._interval() if (urgent or not idle) else self._idle_gap
            self._mark_due(account.id, gap)

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
            f"自动挂起迁仓（等金叉/死叉与平稳）：{tip}，先挂单全平；"
            f"清完后等下次交叉再开 {sym or '新套保'}。"
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

            # 单边或数量不等都立刻救；BOTH 且等量才继续等交叉
            naked = [
                str(r.get("symbol") or "")
                for r in others
                if str(r.get("side") or "").upper() in {"LONG", "SHORT", "IMBALANCE"}
                and r.get("symbol")
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
                        # 单边清净后也不立刻开仓，留给自动开仓等下一根交叉
                        self.ops.clear_migrate_arm(account.id)
                        self._remember_cross(
                            account.id, self._account_symbol(account)
                        )
                        self.ops.store.add_event(
                            account.id,
                            "harvest",
                            True,
                            "旧仓单边已清。新套保等下一根金叉/死叉与入场平稳再开，不立刻重开",
                        )
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
                (("迁仓完成（仅全平，等下次开仓）：" if ok else "迁仓未完成：") + detail)[:500],
            )
            if ok:
                self.ops.clear_migrate_arm(account.id)
                # 吃掉本币对本根交叉，避免同一根马上又自动开新套保
                self._remember_cross(
                    account.id, self._account_symbol(account)
                )
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

    def _maybe_daily_housekeep(self, account) -> None:
        """利息到账后每日一次：换年化 + 划入保证金 + 扫闲置；不等交叉、不反复扫。"""
        day = self._utc_day()
        if self._earn_day_done.get(account.id) == day:
            return
        now = time.monotonic()
        last = float(self._earn_probe_at.get(account.id) or 0)
        if now - last < self._earn_probe_gap:
            return
        if not self.ops.try_begin_action(account.id):
            return
        try:
            self._earn_probe_at[account.id] = now
            cache_key = (account.api_key or "")[:12]
            earn_mod._YDAY_REWARD_CACHE.pop(cache_key, None)
            self.ops.invalidate_snapshot(account.id)
            live = self.ops.snapshot(account, force=True)
            if not live.get("ok"):
                _print(f"每日管家跳过 account={account.id}：{live.get('error') or '快照失败'}")
                return
            yday = live.get("earn_yesterday") or "0"
            src = live.get("earn_yesterday_source") or ""
            reward_day = live.get("earn_yesterday_day") or ""
            earn_total = d(live.get("earn_total") or "0")
            _print(
                f"每日理财收益 account={account.id} 昨日={yday} day={reward_day} source={src}"
            )
            # 有理财仓才等利息；无仓也做一次划入/扫闲置
            if earn_total > 0 and not self._interest_arrived(live):
                _print(
                    f"每日管家等待利息到账 account={account.id} "
                    f"昨日={yday} day={reward_day} source={src}（半小时后再查）"
                )
                return

            notes: list[str] = []
            # 1) 换年化（仅自动收利账号）
            do_switch = bool(account.auto_harvest) and bool(
                getattr(self.ops.base, "auto_switch_earn", True)
            )
            if do_switch:
                switch = live.get("switch") or {}
                if not switch.get("needed"):
                    notes.append("换年化跳过：已是最高或无需换")
                elif not switch.get("can_click"):
                    notes.append(
                        f"换年化跳过：{switch.get('reason') or 'uniMMR/批次不够'}"
                    )
                else:
                    _print(
                        f"每日自动换年化 account={account.id} "
                        f"→ {switch.get('target')} {switch.get('target_apr')}% · "
                        f"{switch.get('reason')}"
                    )
                    result = self.ops.run_action(
                        account,
                        "switch",
                        force=False,
                        mode=None,
                        _locked=True,
                        auto=True,
                        with_snapshot=False,
                    )
                    detail = result.get("reason") or ""
                    steps = result.get("steps") or []
                    if steps:
                        fails = [s for s in steps if not s.get("ok")]
                        pick = fails or steps[-4:]
                        detail = "；".join(
                            f"{s.get('name')}: {s.get('detail')}" for s in pick
                        )
                    notes.append(f"换年化 ok={result.get('ok')} {detail[:200]}")
            else:
                notes.append("换年化跳过：未开自动收利/自动换")

            # 2) 划入保证金一次
            if bool(getattr(self.ops.base, "auto_fund_margin", True)):
                notes.append(self._fund_margin_once(account))
            else:
                notes.append("划入保证金跳过：已关闭")

            # 3) 扫闲置一次
            if bool(getattr(self.ops.base, "auto_sweep_spot", True)):
                notes.append(self._sweep_idle_once(account, live))
            else:
                notes.append("扫闲置跳过：已关闭")

            self._mark_earn_day_done(account.id, day)
            _print(
                f"每日管家完成 account={account.id}："
                + "；".join(notes)[:700]
            )
        finally:
            self.ops.end_action(account.id)

    def _fund_margin_once(self, account) -> str:
        """每日管家内：把待划入的 BFUSD/RWUSD/LDUSDT 划进统一账户。已持锁。"""
        try:
            pipe = pipeline_for(account, self.ops.base, dry_run=True, ops=self.ops)
            if not pipe.needs_fund_to_pm():
                return "划入保证金：无需划入"
        except Exception as exc:
            return f"划入保证金检查失败：{exc}"
        result = self.ops.run_action(
            account,
            "fund",
            force=False,
            mode=None,
            _locked=True,
            auto=True,
            with_snapshot=False,
        )
        detail = result.get("reason") or ""
        steps = result.get("steps") or []
        if steps:
            fails = [s for s in steps if not s.get("ok")]
            pick = fails or steps[-4:]
            detail = "；".join(f"{s.get('name')}: {s.get('detail')}" for s in pick)
        moved = any(
            s.get("ok")
            and (
                "划入统一账户" in str(s.get("name") or "")
                or "转入统一账户" in str(s.get("name") or "")
            )
            for s in steps
        )
        return f"划入保证金 ok={result.get('ok')} moved={moved} {detail[:160]}"

    def _sweep_idle_once(self, account, live: dict) -> str:
        """每日管家内：闲置 U/现货 → 最高年化。已持锁。"""
        legs = live.get("legs") or {}
        has_pos = _qty(legs, "long_qty") > 0 or _qty(legs, "short_qty") > 0
        min_idle = d("10") if has_pos else d("1")
        spot = d(live.get("spot_usdt") or "0")
        if spot < min_idle:
            spot = self.ops._idle_spot(account)
        withdrawable = d(live.get("withdrawable_usdt") or "0")
        if spot < min_idle and withdrawable < min_idle:
            try:
                pipe = pipeline_for(account, self.ops.base, dry_run=True, ops=self.ops)
                from .config import settle_asset_of

                asset = settle_asset_of(str(getattr(pipe.settings, "hedge_symbol", "") or "")) or "USDT"
                withdrawable = d(pipe.futures.max_withdraw(asset) or "0")
            except Exception:
                withdrawable = d("0")
        if spot < min_idle and withdrawable < min_idle:
            return f"扫闲置：无闲置（spot={spot} withdraw={withdrawable}）"
        result = self.ops.run_action(
            account,
            "sweep",
            force=False,
            mode=None,
            _locked=True,
            auto=True,
            with_snapshot=False,
        )
        detail = result.get("reason") or ""
        steps = result.get("steps") or []
        if steps:
            fails = [s for s in steps if not s.get("ok")]
            pick = fails or steps[-4:]
            detail = "；".join(f"{s.get('name')}: {s.get('detail')}" for s in pick)
        return f"扫闲置 ok={result.get('ok')} {detail[:160]}"

    def _macd_side(self, account_id: int, symbol: str | None = None) -> tuple[str | None, str]:
        """仅本币对金叉/死叉那一根才返回腿；否则 None。"""
        base = self.ops.base
        if not bool(getattr(base, "macd_auto_harvest", True)):
            return None, "MACD 过滤已关闭"
        sym = (symbol or "").strip().upper()
        if not sym:
            return None, "无套保币对，无法取 MACD"
        cross = self._tick_crosses.get(sym)
        if cross is None and sym not in self._tick_crosses:
            # 本轮未预刷到该币对时再取缓存（不 force）
            cross = fetch_macd_cross(
                getattr(base, "macd_indicator_url", None), symbol=sym, force=False
            )
        if cross is None:
            return None, f"{sym} 当前不是金叉/死叉那一根"
        key = f"{sym}:{cross.kind}:{cross.time}"
        if self._last_cross_key.get(account_id) == key:
            return None, f"{sym} {cross.label}@{cross.time} 已处理过"
        return (
            cross.harvest_side,
            f"{sym} {cross.label}@{cross.time} → 收{cross.harvest_side}",
        )

    def _remember_cross(self, account_id: int, symbol: str | None = None) -> None:
        sym = (symbol or "").strip().upper()
        if not sym:
            return
        cross = self._tick_crosses.get(sym)
        if cross is None and sym not in self._tick_crosses:
            cross = fetch_macd_cross(
                getattr(self.ops.base, "macd_indicator_url", None),
                symbol=sym,
                force=False,
            )
        if cross:
            self._last_cross_key[account_id] = f"{sym}:{cross.kind}:{cross.time}"

    def _maybe_act(self, account) -> None:
        if not self.ops.try_begin_action(account.id):
            return
        did_trade = False
        try:
            live = self.ops.monitor(account)
            if not live.get("ok"):
                return
            legs = live.get("legs") or {}
            missing = legs.get("missing")
            # 当前币对有缺口：无论是否迁仓待命，都先补齐，禁止带着敞口干等
            if missing in {"LONG", "SHORT", "IMBALANCE"}:
                if self._maybe_repair_hedge(account, live):
                    did_trade = True
                return
            # 旧套保还在：只走迁仓，禁止在新币对上空开/收利/加仓
            others = live.get("other_positions") or []
            if others or (live.get("harvest") or {}).get("migrate"):
                if not self.ops.migrate_arm(account.id):
                    self._maybe_auto_arm_migrate(account)
                return
            flat = _qty(legs, "long_qty") <= 0 and _qty(legs, "short_qty") <= 0
            if flat:
                if account.auto_harvest and self._maybe_enter(account, live):
                    did_trade = True
            else:
                # 浮盈达标：先预扫理财产品，交叉到来时收利不再扫列表
                if account.auto_harvest:
                    self._maybe_prep_harvest(account, live)
                if account.auto_harvest and self._maybe_harvest(account, live):
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

    def _maybe_prep_harvest(self, account, live: dict) -> None:
        """浮盈已达收利门槛：提前扫理财产品，交叉窗口里不再打产品列表。"""
        harvest = live.get("harvest") or {}
        legs = live.get("legs") or {}
        if harvest.get("migrate") or legs.get("missing"):
            return
        if not harvest.get("cooldown_ok", True):
            return
        min_profit = d(harvest.get("min_profit") or "0")
        long_pnl = d(legs.get("long_pnl") or "0")
        short_pnl = d(legs.get("short_pnl") or "0")
        if max(long_pnl, short_pnl) < min_profit or max(long_pnl, short_pnl) <= 0:
            self._harvest_prep.pop(account.id, None)
            return
        hit = self._harvest_prep.get(account.id)
        if hit and (time.monotonic() - float(hit.get("at") or 0)) < self._harvest_prep_ttl:
            return
        try:
            pipe = pipeline_for(account, self.ops.base, dry_run=True, ops=self.ops)
            product, step = pipe.plan_earn_target()
        except Exception as exc:
            log.exception("账号 %s 预扫理财失败", account.id)
            _print(f"收利预扫失败 account={account.id}：{exc}")
            return
        self._harvest_prep[account.id] = {
            "product": product,
            "detail": getattr(step, "detail", "") or "",
            "ok": bool(getattr(step, "ok", True)),
            "at": time.monotonic(),
        }
        _print(
            f"收利预扫完成 account={account.id} "
            f"long={long_pnl} short={short_pnl} min={min_profit} · {step.detail}"
        )

    def _take_harvest_prep(self, account_id: int):
        """取出并消费预扫结果（过期则丢弃）。"""
        hit = self._harvest_prep.pop(account_id, None)
        if not hit:
            return None
        if (time.monotonic() - float(hit.get("at") or 0)) > self._harvest_prep_ttl:
            return None
        if not hit.get("ok"):
            return None
        return hit.get("product")

    def _maybe_scale(self, account, live: dict) -> bool:
        """账户挂起了一次加仓：金叉/死叉与入场级平稳同时满足才加这一次。"""
        if not bool(getattr(account, "auto_scale", False)):
            return False
        raw_qty = getattr(account, "scale_add_qty", None)
        try:
            pending = d(raw_qty) if raw_qty not in (None, "") else d("0")
        except Exception:
            pending = d("0")
        if pending <= 0:
            return False
        if self.ops.migrate_arm(account.id):
            return False
        enter = live.get("enter") or {}
        stability = live.get("stability") or {}
        if not enter.get("scale_ok"):
            return False
        if not enter.get("stable_ok", stability.get("stable")):
            return False
        sym = (live.get("symbol") or self._account_symbol(account) or "").upper()
        macd_note = "未启用 MACD 过滤"
        if bool(getattr(self.ops.base, "macd_auto_harvest", True)):
            side, macd_note = self._macd_side(account.id, sym)
            if not side:
                return False
            # 动手前再确认交叉与平稳仍同时成立
            if not (live.get("stability") or {}).get("stable") and not enter.get("stable_ok"):
                return False
            cross = self._tick_crosses.get(sym)
            if cross is None and sym not in self._tick_crosses:
                cross = fetch_macd_cross(
                    getattr(self.ops.base, "macd_indicator_url", None),
                    symbol=sym,
                    force=False,
                )
            if cross is None:
                return False
        _print(
            f"挂起自动加仓触发 account={account.id} pending={pending} "
            f"room={enter.get('add_qty')} target={enter.get('target_qty')} · {macd_note}"
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
        soft = any(
            (s.get("name") or "") in {"仓位已满", "暂不加仓", "加仓跳过"}
            for s in steps
        )
        if ok and not soft and bool(getattr(self.ops.base, "macd_auto_harvest", True)):
            self._remember_cross(account.id, sym)
        _print(f"挂起自动加仓结束 account={account.id} ok={ok} soft={soft} {detail[:500]}")
        return True

    def _maybe_repair_hedge(self, account, live: dict) -> bool:
        """多空缺口（缺一边或数量不等）：不等平稳/交叉，立刻强行补齐。迁仓待命中也要修当前币对。"""
        missing = (live.get("legs") or {}).get("missing")
        _print(f"自动补齐对冲 account={account.id} missing={missing}")
        result = self.ops.run_action(
            account,
            "enter",
            force=True,
            mode="force",
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
        _print(f"自动补齐对冲结束 account={account.id} ok={ok} {detail[:500]}")
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
        sym = (live.get("symbol") or self._account_symbol(account) or "").upper()
        macd_note = "未启用 MACD 过滤"
        if bool(getattr(self.ops.base, "macd_auto_harvest", True)):
            side, macd_note = self._macd_side(account.id, sym)
            if not side:
                return False
            cross = self._tick_crosses.get(sym)
            if cross is None and sym not in self._tick_crosses:
                cross = fetch_macd_cross(
                    getattr(self.ops.base, "macd_indicator_url", None),
                    symbol=sym,
                    force=False,
                )
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
            self._remember_cross(account.id, sym)
        _print(f"自动开仓结束 account={account.id} ok={ok} {detail[:500]}")
        return True

    def _maybe_harvest(self, account, live: dict) -> bool:
        harvest = live.get("harvest") or {}
        legs = live.get("legs") or {}
        if self.ops.migrate_arm(account.id) or harvest.get("migrate"):
            return False

        sym = (live.get("symbol") or self._account_symbol(account) or "").upper()
        side, macd_note = self._macd_side(account.id, sym)
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

        prep = self._take_harvest_prep(account.id)
        _print(
            f"自动收利触发 account={account.id} side={side or harvest.get('winner')} "
            f"pnl={harvest.get('winner_pnl')} macd={macd_note} "
            f"prep={'已预扫' if prep is not None else '临扫'}"
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
                earn_product=prep,
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
            cross = self._tick_crosses.get(sym)
            if cross is None and sym not in self._tick_crosses:
                cross = fetch_macd_cross(
                    getattr(self.ops.base, "macd_indicator_url", None),
                    symbol=sym,
                    force=False,
                )
            if cross and cross.harvest_side == side:
                self._last_cross_key[account.id] = f"{sym}:{cross.kind}:{cross.time}"
        # 成功或失败都清掉预扫，避免用过期产品
        self._harvest_prep.pop(account.id, None)
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
