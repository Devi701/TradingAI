from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import aiosqlite

from .models import ChildDNA, MacroRegime, OrderIntent, VirtualFill, utc_now


class Ledger:
    def __init__(self, db_path: str) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    async def init(self) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.executescript(
                """
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    level TEXT NOT NULL,
                    component TEXT NOT NULL,
                    message TEXT NOT NULL,
                    payload TEXT
                );
                CREATE TABLE IF NOT EXISTS state (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_ts TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS equity_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    equity REAL NOT NULL,
                    cash REAL,
                    buying_power REAL,
                    drawdown_pct REAL
                );
                CREATE TABLE IF NOT EXISTS macro_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    score REAL NOT NULL,
                    headline_count INTEGER NOT NULL,
                    summary TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS mother_allocations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    mother_id TEXT NOT NULL,
                    allowance REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS mother_heartbeats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    mother_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    note TEXT
                );
                CREATE TABLE IF NOT EXISTS children (
                    child_id TEXT PRIMARY KEY,
                    mother_id TEXT NOT NULL,
                    dna_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    pnl REAL NOT NULL,
                    position_qty REAL NOT NULL DEFAULT 0,
                    entry_price REAL NOT NULL DEFAULT 0,
                    updated_ts TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS intents (
                    intent_id TEXT PRIMARY KEY,
                    ts TEXT NOT NULL,
                    child_id TEXT NOT NULL,
                    mother_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    qty REAL NOT NULL,
                    observed_price REAL NOT NULL,
                    confidence REAL NOT NULL,
                    reason TEXT,
                    indicators_json TEXT,
                    status TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS orders (
                    order_id TEXT PRIMARY KEY,
                    ts TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    qty REAL NOT NULL,
                    intent_count INTEGER NOT NULL,
                    submitted_price REAL,
                    status TEXT NOT NULL,
                    raw_json TEXT
                );
                CREATE TABLE IF NOT EXISTS fills (
                    fill_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    intent_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    qty REAL NOT NULL,
                    observed_price REAL NOT NULL,
                    effective_price REAL NOT NULL,
                    commission REAL NOT NULL,
                    slippage REAL NOT NULL,
                    latency_ms INTEGER NOT NULL,
                    raw_json TEXT
                );
                """
            )
            await self._migrate(db)
            await db.commit()

    async def _migrate(self, db: aiosqlite.Connection) -> None:
        await _ensure_column(db, "children", "position_qty", "REAL NOT NULL DEFAULT 0")
        await _ensure_column(db, "children", "entry_price", "REAL NOT NULL DEFAULT 0")
        await _ensure_column(db, "intents", "reason", "TEXT")

    async def log_event(self, level: str, component: str, message: str, payload: dict[str, Any] | None = None) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT INTO events(ts, level, component, message, payload) VALUES (?, ?, ?, ?, ?)",
                (utc_now().isoformat(), level, component, message, json.dumps(payload or {}, default=str)),
            )
            await db.commit()

    async def set_state(self, key: str, value: dict[str, Any]) -> None:
        now = utc_now().isoformat()
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO state(key, value, updated_ts) VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_ts=excluded.updated_ts
                """,
                (key, json.dumps(value, default=str), now),
            )
            await db.commit()

    async def get_state(self, key: str) -> dict[str, Any] | None:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT value FROM state WHERE key = ?", (key,))
            row = await cursor.fetchone()
            return None if row is None else json.loads(row[0])

    async def snapshot_equity(self, equity: float, cash: float, buying_power: float, drawdown_pct: float) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT INTO equity_snapshots(ts, equity, cash, buying_power, drawdown_pct) VALUES (?, ?, ?, ?, ?)",
                (utc_now().isoformat(), equity, cash, buying_power, drawdown_pct),
            )
            await db.commit()

    async def snapshot_macro(self, regime: MacroRegime) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT INTO macro_snapshots(ts, mode, score, headline_count, summary) VALUES (?, ?, ?, ?, ?)",
                (regime.fetched_at.isoformat(), regime.mode, regime.score, regime.headline_count, regime.summary),
            )
            await db.commit()

    async def save_mother_allocation(self, mother_id: str, allowance: float) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT INTO mother_allocations(ts, mother_id, allowance) VALUES (?, ?, ?)",
                (utc_now().isoformat(), mother_id, allowance),
            )
            await db.commit()

    async def save_heartbeat(self, mother_id: str, status: str, note: str = "") -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT INTO mother_heartbeats(ts, mother_id, status, note) VALUES (?, ?, ?, ?)",
                (utc_now().isoformat(), mother_id, status, note),
            )
            await db.commit()

    async def upsert_child(
        self,
        child_id: str,
        mother_id: str,
        dna: ChildDNA,
        status: str,
        pnl: float,
        position_qty: float,
        entry_price: float,
    ) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO children(child_id, mother_id, dna_json, status, pnl, position_qty, entry_price, updated_ts)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(child_id)
                DO UPDATE SET dna_json=excluded.dna_json,
                              status=excluded.status,
                              pnl=excluded.pnl,
                              position_qty=excluded.position_qty,
                              entry_price=excluded.entry_price,
                              updated_ts=excluded.updated_ts
                """,
                (
                    child_id,
                    mother_id,
                    json.dumps(dna.to_dict()),
                    status,
                    pnl,
                    position_qty,
                    entry_price,
                    utc_now().isoformat(),
                ),
            )
            await db.commit()

    async def list_children(self, mother_id: str) -> list[tuple[str, dict[str, Any], float, str, float, float]]:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "SELECT child_id, dna_json, pnl, status, position_qty, entry_price FROM children WHERE mother_id = ?",
                (mother_id,),
            )
            rows = await cursor.fetchall()
            return [(r[0], json.loads(r[1]), r[2], r[3], r[4], r[5]) for r in rows]

    async def persist_intent(self, intent: OrderIntent, status: str) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT OR REPLACE INTO intents(intent_id, ts, child_id, mother_id, symbol, side, qty, observed_price, confidence, reason, indicators_json, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    intent.id,
                    intent.created_at.isoformat(),
                    intent.child_id,
                    intent.mother_id,
                    intent.symbol,
                    intent.side,
                    intent.qty,
                    intent.observed_price,
                    intent.confidence,
                    intent.reason,
                    json.dumps(intent.indicators, default=str),
                    status,
                ),
            )
            await db.commit()

    async def update_intent_status(self, intent_id: str, status: str) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE intents SET status = ? WHERE intent_id = ?", (status, intent_id))
            await db.commit()

    async def persist_order(
        self,
        order_id: str,
        symbol: str,
        side: str,
        qty: float,
        intent_count: int,
        submitted_price: float,
        status: str,
        raw: dict[str, Any],
    ) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT OR REPLACE INTO orders(order_id, ts, symbol, side, qty, intent_count, submitted_price, status, raw_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    order_id,
                    utc_now().isoformat(),
                    symbol,
                    side,
                    qty,
                    intent_count,
                    submitted_price,
                    status,
                    json.dumps(raw, default=str),
                ),
            )
            await db.commit()

    async def persist_fill(self, fill: VirtualFill, raw: dict[str, Any] | None = None) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO fills(ts, intent_id, symbol, side, qty, observed_price, effective_price, commission, slippage, latency_ms, raw_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    fill.filled_at.isoformat(),
                    fill.intent_id,
                    fill.symbol,
                    fill.side,
                    fill.qty,
                    fill.observed_price,
                    fill.effective_price,
                    fill.commission,
                    fill.slippage,
                    fill.latency_ms,
                    json.dumps(raw or {}, default=str),
                ),
            )
            await db.commit()

    async def close(self) -> None:
        return None


async def _ensure_column(db: aiosqlite.Connection, table: str, column: str, ddl: str) -> None:
    cursor = await db.execute(f"PRAGMA table_info({table})")
    rows = await cursor.fetchall()
    columns = {row[1] for row in rows}
    if column in columns:
        return
    await db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")
