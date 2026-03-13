from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from .cloud import CloudSync
from .storage import LifeStorage
from .utils import flatten_dict


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

    def extract(self, *, category: str | None, out: str) -> dict[str, Any]:
        rows = self.storage.fetch_records(category=category)

        flattened_rows: list[dict[str, Any]] = []
        fieldnames: list[str] = ["id", "timestamp", "category", "summary", "tags", "demographic_tag", "source_fingerprint", "synced_at"]
        fieldset = set(fieldnames)
        for row in rows:
            payload = json.loads(str(row["payload"]))
            flat_payload = flatten_dict(payload)
            record: dict[str, Any] = {
                "id": row["id"],
                "timestamp": row["timestamp"],
                "category": row["category"],
                "summary": row["summary"],
                "tags": row["tags"],
                "demographic_tag": row["demographic_tag"],
                "source_fingerprint": row["source_fingerprint"],
                "synced_at": row["synced_at"],
            }

            for key, value in flat_payload.items():
                column = f"payload.{key}"
                record[column] = value
                if column not in fieldset:
                    fieldset.add(column)
                    fieldnames.append(column)

            flattened_rows.append(record)

        output_path = Path(out)
        with output_path.open("w", encoding="utf-8", newline="") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            for record in flattened_rows:
                writer.writerow(record)

        return {
            "ok": True,
            "action": "extract",
            "data": {
                "category": category,
                "count": len(flattened_rows),
                "output": str(output_path),
            },
        }
