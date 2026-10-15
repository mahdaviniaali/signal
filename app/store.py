"""SQLite-backed idempotency store for processed orders/events.

UNIQUE(topic, event_id) guarantees that a redelivered Kafka message
(e.g. order 101 after a crash before offset commit) is not applied twice.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any

from app.config import settings


class IdempotencyStore:
    def __init__(self, db_path: str | None = None) -> None:
        self._path = Path(db_path or settings.idempotency_db_path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self._path, timeout=30.0, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            self._local.conn = conn
        return conn

    def _init_db(self) -> None:
        conn = self._connect()
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS processed_events (
                topic TEXT NOT NULL,
                event_id TEXT NOT NULL,
                payload TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                PRIMARY KEY (topic, event_id)
            )
            """
        )
        conn.commit()

    def exists(self, topic: str, event_id: str) -> bool:
        row = self._connect().execute(
            "SELECT 1 FROM processed_events WHERE topic = ? AND event_id = ? LIMIT 1",
            (topic, event_id),
        ).fetchone()
        return row is not None

    def save(self, topic: str, event_id: str, payload: dict[str, Any] | None = None) -> bool:
        """Insert event. Returns True if newly saved, False if already present."""
        conn = self._connect()
        try:
            conn.execute(
                "INSERT INTO processed_events (topic, event_id, payload) VALUES (?, ?, ?)",
                (topic, event_id, json.dumps(payload) if payload is not None else None),
            )
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            conn.rollback()
            return False

    def get(self, topic: str, event_id: str) -> dict[str, Any] | None:
        row = self._connect().execute(
            "SELECT topic, event_id, payload, created_at FROM processed_events "
            "WHERE topic = ? AND event_id = ?",
            (topic, event_id),
        ).fetchone()
        if row is None:
            return None
        return {
            "topic": row["topic"],
            "event_id": row["event_id"],
            "payload": json.loads(row["payload"]) if row["payload"] else None,
            "created_at": row["created_at"],
        }

    def count(self, topic: str | None = None) -> int:
        conn = self._connect()
        if topic is None:
            row = conn.execute("SELECT COUNT(*) AS n FROM processed_events").fetchone()
        else:
            row = conn.execute(
                "SELECT COUNT(*) AS n FROM processed_events WHERE topic = ?",
                (topic,),
            ).fetchone()
        return int(row["n"])


store = IdempotencyStore()
