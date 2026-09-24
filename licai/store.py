from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from .config import ROOT, normalize_hedge_symbol

DATA_DIR = ROOT / "data"
DB_PATH = DATA_DIR / "accounts.sqlite"


@dataclass
class Account:
    id: int
    name: str
    api_key: str
    api_secret: str
    proxy: str | None
    hedge_symbol: str
    created_at: str
    hedge_leverage: int | None = None
    leverage_cap: int | None = None
    leverage_unlock_at: str | None = None
    take_profit_usdt: float | None = None
    take_profit_custom: bool = False
    auto_harvest: bool = False

    def public_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "api_key_tail": _tail(self.api_key),
            "proxy": self.proxy or "",
            "proxy_enabled": bool(self.proxy),
            "hedge_symbol": self.hedge_symbol,
            "hedge_leverage": self.hedge_leverage,
            "created_at": self.created_at,
            "leverage_cap": self.leverage_cap,
            "leverage_unlock_at": self.leverage_unlock_at or "",
            "take_profit_usdt": self.take_profit_usdt if self.take_profit_usdt else None,
            "take_profit_custom": bool(self.take_profit_custom),
            "auto_harvest": bool(self.auto_harvest),
        }


@dataclass
class Event:
    id: int
    account_id: int
    action: str
    ok: bool
    detail: str
    created_at: str


def _tail(value: str, n: int = 4) -> str:
    value = value or ""
    if len(value) <= n:
        return "*" * len(value)
    return "…" + value[-n:]


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


class AccountStore:
    def __init__(self, path: Path | None = None):
        self.path = path or DB_PATH
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS accounts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    api_key TEXT NOT NULL,
                    api_secret TEXT NOT NULL,
                    proxy TEXT,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_id INTEGER NOT NULL,
                    action TEXT NOT NULL,
                    ok INTEGER NOT NULL,
                    detail TEXT,
                    created_at TEXT NOT NULL
                )
                """
            )
            cols = {row[1] for row in conn.execute("PRAGMA table_info(accounts)").fetchall()}
            if "hedge_symbol" not in cols:
                conn.execute("ALTER TABLE accounts ADD COLUMN hedge_symbol TEXT NOT NULL DEFAULT 'ETHUSDT'")
            if "leverage_cap" not in cols:
                conn.execute("ALTER TABLE accounts ADD COLUMN leverage_cap INTEGER")
            if "leverage_unlock_at" not in cols:
                conn.execute("ALTER TABLE accounts ADD COLUMN leverage_unlock_at TEXT")
            if "take_profit_usdt" not in cols:
                conn.execute("ALTER TABLE accounts ADD COLUMN take_profit_usdt TEXT")
            if "take_profit_custom" not in cols:
                conn.execute("ALTER TABLE accounts ADD COLUMN take_profit_custom INTEGER NOT NULL DEFAULT 0")
            if "auto_harvest" not in cols:
                conn.execute("ALTER TABLE accounts ADD COLUMN auto_harvest INTEGER NOT NULL DEFAULT 0")
            if "hedge_leverage" not in cols:
                conn.execute("ALTER TABLE accounts ADD COLUMN hedge_leverage INTEGER")

    def list_accounts(self) -> list[Account]:
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM accounts ORDER BY id").fetchall()
        return [self._account(row) for row in rows]

    def get(self, account_id: int) -> Account:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM accounts WHERE id = ?", (account_id,)).fetchone()
        if row is None:
            raise KeyError(account_id)
        return self._account(row)

    def add(self, name: str, api_key: str, api_secret: str, proxy: str | None, hedge_symbol: str = "ETHUSDT") -> Account:
        created = _now()
        symbol = normalize_hedge_symbol(hedge_symbol)
        with self._connect() as conn:
            cur = conn.execute(
                "INSERT INTO accounts (name, api_key, api_secret, proxy, hedge_symbol, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                (name.strip(), api_key.strip(), api_secret.strip(), (proxy or "").strip() or None, symbol, created),
            )
            account_id = int(cur.lastrowid)
        return self.get(account_id)

    def update(
        self,
        account_id: int,
        *,
        name: str | None = None,
        api_key: str | None = None,
        api_secret: str | None = None,
        proxy: str | None = None,
        hedge_symbol: str | None = None,
        take_profit_usdt: float | None = None,
        take_profit_custom: bool | None = None,
        auto_harvest: bool | None = None,
        hedge_leverage: int | None = None,
        hedge_leverage_set: bool = False,
    ) -> Account:
        account = self.get(account_id)
        next_name = name.strip() if name is not None else account.name
        next_key = api_key.strip() if api_key else account.api_key
        next_secret = api_secret.strip() if api_secret else account.api_secret
        if proxy is None:
            next_proxy = account.proxy
        else:
            next_proxy = proxy.strip() or None
        next_symbol = normalize_hedge_symbol(hedge_symbol) if hedge_symbol else account.hedge_symbol
        next_tp = account.take_profit_usdt
        next_custom = account.take_profit_custom
        if take_profit_custom is False:
            next_custom = False
            next_tp = None
        elif take_profit_usdt is not None:
            if take_profit_usdt <= 0:
                next_custom = False
                next_tp = None
            else:
                next_custom = True
                next_tp = float(take_profit_usdt)
        elif take_profit_custom is True and account.take_profit_usdt:
            next_custom = True
        next_auto = account.auto_harvest if auto_harvest is None else bool(auto_harvest)
        next_lev = account.hedge_leverage
        if hedge_leverage_set:
            next_lev = int(hedge_leverage) if hedge_leverage and int(hedge_leverage) > 0 else None
        with self._connect() as conn:
            conn.execute(
                "UPDATE accounts SET name = ?, api_key = ?, api_secret = ?, proxy = ?, hedge_symbol = ?, take_profit_usdt = ?, take_profit_custom = ?, auto_harvest = ?, hedge_leverage = ? WHERE id = ?",
                (
                    next_name,
                    next_key,
                    next_secret,
                    next_proxy,
                    next_symbol,
                    str(next_tp) if next_tp else None,
                    1 if next_custom else 0,
                    1 if next_auto else 0,
                    next_lev,
                    account_id,
                ),
            )
        return self.get(account_id)

    def set_leverage_cap(self, account_id: int, cap: int | None, unlock_at: str | None) -> Account:
        self.get(account_id)
        with self._connect() as conn:
            conn.execute(
                "UPDATE accounts SET leverage_cap = ?, leverage_unlock_at = ? WHERE id = ?",
                (int(cap) if cap else None, unlock_at, account_id),
            )
        return self.get(account_id)

    def delete(self, account_id: int) -> None:
        self.get(account_id)
        with self._connect() as conn:
            conn.execute("DELETE FROM events WHERE account_id = ?", (account_id,))
            conn.execute("DELETE FROM accounts WHERE id = ?", (account_id,))

    def add_event(self, account_id: int, action: str, ok: bool, detail: str) -> Event:
        created = _now()
        with self._connect() as conn:
            cur = conn.execute(
                "INSERT INTO events (account_id, action, ok, detail, created_at) VALUES (?, ?, ?, ?, ?)",
                (account_id, action, 1 if ok else 0, detail, created),
            )
            event_id = int(cur.lastrowid)
        return Event(event_id, account_id, action, ok, detail, created)

    def recent_events(self, account_id: int, limit: int = 20) -> list[Event]:
        return self.list_events(account_id, limit=limit)

    def list_events(
        self,
        account_id: int,
        *,
        limit: int = 100,
        before_id: int | None = None,
    ) -> list[Event]:
        limit = max(1, min(int(limit or 100), 500))
        with self._connect() as conn:
            if before_id is not None:
                rows = conn.execute(
                    "SELECT * FROM events WHERE account_id = ? AND id < ? ORDER BY id DESC LIMIT ?",
                    (account_id, int(before_id), limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM events WHERE account_id = ? ORDER BY id DESC LIMIT ?",
                    (account_id, limit),
                ).fetchall()
        return [
            Event(
                int(row["id"]),
                int(row["account_id"]),
                row["action"],
                bool(row["ok"]),
                row["detail"] or "",
                row["created_at"],
            )
            for row in rows
        ]

    def last_ok_action_at(self, account_id: int, action: str) -> str | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT created_at FROM events WHERE account_id = ? AND action = ? AND ok = 1 ORDER BY id DESC LIMIT 1",
                (account_id, action),
            ).fetchone()
        return None if row is None else str(row["created_at"])

    @staticmethod
    def _account(row: sqlite3.Row) -> Account:
        keys = set(row.keys())
        cap = row["leverage_cap"] if "leverage_cap" in keys else None
        unlock = row["leverage_unlock_at"] if "leverage_unlock_at" in keys else None
        raw_tp = row["take_profit_usdt"] if "take_profit_usdt" in keys else None
        tp = None
        if raw_tp not in (None, ""):
            try:
                tp = float(raw_tp)
            except (TypeError, ValueError):
                tp = None
            if tp is not None and tp <= 0:
                tp = None
        raw_custom = row["take_profit_custom"] if "take_profit_custom" in keys else None
        custom = bool(int(raw_custom or 0))
        if not custom and tp is not None:
            custom = True
        raw_auto = row["auto_harvest"] if "auto_harvest" in keys else None
        raw_lev = row["hedge_leverage"] if "hedge_leverage" in keys else None
        lev = int(raw_lev) if raw_lev else None
        return Account(
            id=int(row["id"]),
            name=row["name"],
            api_key=row["api_key"],
            api_secret=row["api_secret"],
            proxy=row["proxy"],
            hedge_symbol=str(row["hedge_symbol"] or "ETHUSDT"),
            created_at=row["created_at"],
            hedge_leverage=lev if lev and lev > 0 else None,
            leverage_cap=int(cap) if cap else None,
            leverage_unlock_at=str(unlock) if unlock else None,
            take_profit_usdt=tp,
            take_profit_custom=custom,
            auto_harvest=bool(int(raw_auto or 0)),
        )
