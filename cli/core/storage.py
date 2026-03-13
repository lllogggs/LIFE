from __future__ import annotations

import json
import sqlite3
import uuid
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any


class SchemaValidationError(ValueError):
    """Raised when payload does not satisfy the registered schema."""


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
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_registry (
                    category TEXT PRIMARY KEY,
                    schema_json TEXT NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS life_master (
                    id INTEGER PRIMARY KEY,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    category TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    tags TEXT,
                    demographic_tag TEXT,
                    source_fingerprint TEXT,
                    synced_at DATETIME
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_life_master_category ON life_master(category)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_life_master_synced ON life_master(synced_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_life_master_demographic ON life_master(demographic_tag)")
            conn.commit()

        self.ensure_client_id()

    def ensure_client_id(self) -> str:
        with self._connect() as conn:
            row = conn.execute("SELECT value FROM metadata WHERE key = 'client_id'").fetchone()
            if row:
                return str(row["value"])

            client_id = str(uuid.uuid4())
            conn.execute("INSERT INTO metadata (key, value) VALUES ('client_id', ?)", (client_id,))
            conn.commit()
            return client_id

    def add_schema(self, category: str, schema: dict[str, Any]) -> None:
        normalized = json.dumps(schema, ensure_ascii=False, sort_keys=True)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO schema_registry (category, schema_json, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(category) DO UPDATE
                SET schema_json = excluded.schema_json,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (category.strip(), normalized),
            )
            conn.commit()

    def list_schemas(self) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT category, schema_json, created_at, updated_at FROM schema_registry ORDER BY category ASC"
            ).fetchall()
        return [
            {
                "category": str(row["category"]),
                "schema": json.loads(str(row["schema_json"])),
                "created_at": str(row["created_at"]),
                "updated_at": str(row["updated_at"]),
            }
            for row in rows
        ]

    def ingest(
        self,
        *,
        category: str,
        summary: str,
        payload: dict[str, Any],
        tags: list[str],
        demographic_tag: str | None = None,
        source_fingerprint: str | None = None,
    ) -> int:
        schema = self._get_schema(category)
        self._validate_payload(category=category, payload=payload, schema=schema)

        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO life_master (category, summary, payload, tags, demographic_tag, source_fingerprint)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    category.strip(),
                    summary.strip(),
                    json.dumps(payload, ensure_ascii=False, sort_keys=True),
                    ",".join(tags),
                    demographic_tag.strip() if demographic_tag else None,
                    source_fingerprint,
                ),
            )
            conn.commit()
            return int(cur.lastrowid)

    def get_records_for_sync(self, limit: int = 500) -> list[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT id, category, payload, demographic_tag
                FROM life_master
                WHERE synced_at IS NULL
                ORDER BY id ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

    def fetch_records(self, category: str | None = None, limit: int | None = None) -> list[sqlite3.Row]:
        query = """
            SELECT id, timestamp, category, summary, payload, tags, demographic_tag, source_fingerprint, synced_at
            FROM life_master
        """
        params: list[Any] = []
        clauses: list[str] = []

        if category:
            clauses.append("category = ?")
            params.append(category.strip())

        if clauses:
            query = f"{query} WHERE {' AND '.join(clauses)}"

        query = f"{query} ORDER BY id ASC"

        if limit is not None:
            query = f"{query} LIMIT ?"
            params.append(limit)

        with self._connect() as conn:
            return conn.execute(query, params).fetchall()

    def mark_synced(self, ids: list[int]) -> None:
        if not ids:
            return
        placeholders = ",".join("?" for _ in ids)
        with self._connect() as conn:
            conn.execute(
                f"UPDATE life_master SET synced_at = CURRENT_TIMESTAMP WHERE id IN ({placeholders})",
                ids,
            )
            conn.commit()

    def _get_schema(self, category: str) -> dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT schema_json FROM schema_registry WHERE category = ?",
                (category.strip(),),
            ).fetchone()
        if not row:
            raise SchemaValidationError(f"schema not found for category '{category}'")
        return dict(json.loads(str(row["schema_json"])))

    def _validate_payload(self, *, category: str, payload: Mapping[str, Any], schema: Mapping[str, Any]) -> None:
        required = schema.get("required", [])
        if not isinstance(required, Sequence) or isinstance(required, (str, bytes)):
            raise SchemaValidationError(f"invalid schema for category '{category}': required must be a list")

        for field in required:
            if field not in payload:
                raise SchemaValidationError(f"missing required field '{field}' for category '{category}'")

        properties = schema.get("properties", {})
        if not isinstance(properties, Mapping):
            raise SchemaValidationError(f"invalid schema for category '{category}': properties must be an object")

        for key, value in payload.items():
            if key not in properties:
                continue
            expected = properties[key]
            expected_type = expected.get("type") if isinstance(expected, Mapping) else None
            if expected_type and not self._is_type_match(value, str(expected_type)):
                actual = type(value).__name__
                raise SchemaValidationError(
                    f"type mismatch for '{key}' in category '{category}': expected {expected_type}, got {actual}"
                )

    @staticmethod
    def _is_type_match(value: Any, expected_type: str) -> bool:
        mapping: dict[str, tuple[type[Any], ...]] = {
            "string": (str,),
            "number": (int, float),
            "integer": (int,),
            "boolean": (bool,),
            "object": (dict,),
            "array": (list,),
        }
        if expected_type == "number" and isinstance(value, bool):
            return False
        return isinstance(value, mapping.get(expected_type, (object,)))
