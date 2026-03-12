from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any


class LifeStorage:
    def __init__(self, db_path: str | Path = "jw_life.db") -> None:
        self.db_path = Path(db_path)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _initialize(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS life_master (
                    id INTEGER PRIMARY KEY,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    category TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    tags TEXT,
                    source_fingerprint TEXT
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_life_master_category ON life_master(category)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_life_master_fingerprint ON life_master(source_fingerprint)")
            conn.commit()

    def ingest(
        self,
        category: str,
        summary: str,
        payload: dict[str, Any],
        tags: list[str],
        source_fingerprint: str | None = None,
    ) -> int:
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO life_master (category, summary, payload, tags, source_fingerprint)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    category.strip(),
                    summary.strip(),
                    json.dumps(payload, ensure_ascii=False),
                    ",".join(tags),
                    source_fingerprint,
                ),
            )
            conn.commit()
            return int(cur.lastrowid)

    def update_record(
        self,
        record_id: int,
        *,
        category: str,
        summary: str,
        payload: dict[str, Any],
        tags: list[str],
        source_fingerprint: str | None = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE life_master
                SET category = ?, summary = ?, payload = ?, tags = ?, source_fingerprint = ?
                WHERE id = ?
                """,
                (
                    category.strip(),
                    summary.strip(),
                    json.dumps(payload, ensure_ascii=False),
                    ",".join(tags),
                    source_fingerprint,
                    record_id,
                ),
            )
            conn.commit()

    def find_by_fingerprint(self, source_fingerprint: str) -> sqlite3.Row | None:
        with self._connect() as conn:
            return conn.execute(
                "SELECT id, timestamp, category, summary, payload, tags, source_fingerprint FROM life_master WHERE source_fingerprint = ? ORDER BY id DESC LIMIT 1",
                (source_fingerprint,),
            ).fetchone()

    def fetch_records(self, category: str | None = None, limit: int | None = None) -> list[sqlite3.Row]:
        query = "SELECT id, timestamp, category, summary, payload, tags, source_fingerprint FROM life_master"
        params: list[Any] = []

        if category:
            query += " WHERE category = ?"
            params.append(category)

        query += " ORDER BY timestamp DESC"

        if limit and limit > 0:
            query += " LIMIT ?"
            params.append(limit)

        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()

        return rows

    def category_stats(self) -> list[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT category, COUNT(*) as count
                FROM life_master
                GROUP BY category
                ORDER BY count DESC, category ASC
                """
            ).fetchall()

    def total_count(self) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) as count FROM life_master").fetchone()
        return int(row["count"]) if row else 0
