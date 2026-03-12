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
                    tags TEXT
                )
                """
            )
            conn.commit()

    def ingest(self, category: str, summary: str, payload: dict[str, Any], tags: list[str]) -> int:
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO life_master (category, summary, payload, tags)
                VALUES (?, ?, ?, ?)
                """,
                (category.strip(), summary.strip(), json.dumps(payload, ensure_ascii=False), ",".join(tags)),
            )
            conn.commit()
            return int(cur.lastrowid)

    def fetch_records(self, category: str | None = None, limit: int | None = None) -> list[sqlite3.Row]:
        query = "SELECT id, timestamp, category, summary, payload, tags FROM life_master"
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
