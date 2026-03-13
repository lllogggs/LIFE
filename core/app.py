from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .cloud import CloudSync
from .storage import LifeStorage


class LifeOSApp:
    def __init__(self, db_path: str | Path = "jw_life.db") -> None:
        self.storage = LifeStorage(db_path)

    def setup(self) -> dict[str, Any]:
        return {
            "ok": True,
            "action": "setup",
            "data": {
                "db_path": str(self.storage.db_path),
                "client_id": self.storage.ensure_client_id(),
            },
        }

    def schema_add(self, *, category: str, schema: dict[str, Any]) -> dict[str, Any]:
        self.storage.add_schema(category, schema)
        return {
            "ok": True,
            "action": "schema_add",
            "data": {"category": category, "schema": schema},
        }

    def schema_list(self) -> dict[str, Any]:
        schemas = self.storage.list_schemas()
        return {
            "ok": True,
            "action": "schema_list",
            "data": {"count": len(schemas), "schemas": schemas},
        }

    def ingest(
        self,
        *,
        category: str,
        summary: str,
        payload: dict[str, Any],
        tags: list[str],
        demographic_tag: str | None,
        source_fingerprint: str | None,
    ) -> dict[str, Any]:
        record_id = self.storage.ingest(
            category=category,
            summary=summary,
            payload=payload,
            tags=tags,
            demographic_tag=demographic_tag,
            source_fingerprint=source_fingerprint,
        )
        return {
            "ok": True,
            "action": "ingest",
            "data": {
                "id": record_id,
                "category": category,
                "demographic_tag": demographic_tag,
            },
        }

    def sync(self) -> dict[str, Any]:
        cloud = CloudSync()
        client_id = self.storage.ensure_client_id()
        rows = self.storage.get_records_for_sync()

        telemetry: list[dict[str, Any]] = []
        synced_ids: list[int] = []
        for row in rows:
            payload = json.loads(str(row["payload"]))
            telemetry.append(
                {
                    "client_id": client_id,
                    "category": str(row["category"]),
                    "demographic_tag": str(row["demographic_tag"] or "unknown"),
                    "metrics": cloud.anonymize_payload(payload),
                }
            )
            synced_ids.append(int(row["id"]))

        cloud.push_telemetry(telemetry)
        self.storage.mark_synced(synced_ids)

        return {
            "ok": True,
            "action": "sync",
            "data": {
                "client_id": client_id,
                "synced_count": len(synced_ids),
            },
        }

    def explore(self, *, category: str, demographic_tag: str) -> dict[str, Any]:
        cloud = CloudSync()
        stats = cloud.explore_global_stats(category=category, demographic_tag=demographic_tag)
        return {
            "ok": True,
            "action": "explore",
            "data": {
                "category": category,
                "demographic_tag": demographic_tag,
                "count": len(stats),
                "stats": stats,
            },
        }
