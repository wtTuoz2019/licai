#!/usr/bin/env python3
"""本机排查昨日理财收益：直接打币安奖励接口并打印原始返回。"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from licai.config import load_settings, settings_for_account  # noqa: E402
from licai.client import BinanceClient  # noqa: E402
from licai.earn import EarnAPI, _reward_query_window_ms  # noqa: E402
from licai.store import AccountStore  # noqa: E402
from licai import earn as earn_mod  # noqa: E402


def main() -> None:
    account_id = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    store = AccountStore(ROOT / "data" / "accounts.sqlite")
    acc = store.get(account_id)
    base = load_settings()
    s = settings_for_account(
        base,
        api_key=acc.api_key,
        api_secret=acc.api_secret,
        proxy=acc.proxy,
        dry_run=True,
    )
    client = BinanceClient(s.api_key, s.api_secret, proxy=s.proxy)
    try:
        client.sync_time(force=True)
    except Exception as exc:
        print("sync_time failed", exc)
    local_ms = int(__import__("time").time() * 1000)
    binance_ms = client.timestamp()
    print("local_ms", local_ms, datetime.fromtimestamp(local_ms / 1000, tz=timezone.utc))
    print("binance_ms", binance_ms, datetime.fromtimestamp(binance_ms / 1000, tz=timezone.utc))
    print("offset_ms", binance_ms - local_ms)
    start_ms, end_ms = _reward_query_window_ms(binance_ms)
    start30 = end_ms - 30 * 86400 * 1000
    print("account", acc.id, acc.name)
    print(
        "window3d",
        datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc),
        "->",
        datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc),
    )
    print("bfusd_account", client.signed("GET", "/sapi/v1/bfusd/account", {}))
    print(
        "rewards_3d",
        json.dumps(
            client.signed(
                "GET",
                "/sapi/v1/bfusd/history/rewardsHistory",
                {"startTime": start_ms, "endTime": end_ms, "size": 100, "current": 1},
            ),
            ensure_ascii=False,
        ),
    )
    print(
        "rewards_30d",
        json.dumps(
            client.signed(
                "GET",
                "/sapi/v1/bfusd/history/rewardsHistory",
                {"startTime": start30, "endTime": end_ms, "size": 100, "current": 1},
            ),
            ensure_ascii=False,
        ),
    )
    earn_mod._YDAY_REWARD_CACHE.clear()
    print("parsed", EarnAPI(client).yesterday_earn_reward())


if __name__ == "__main__":
    main()
