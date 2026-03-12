from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from .storage import LifeStorage
from .utils import flatten_dict


class LifeOSApp:
    def __init__(self, db_path: str | Path = "jw_life.db") -> None:
        self.storage = LifeStorage(db_path)

    def ingest(self, *, category: str, summary: str, payload: dict[str, Any], tags: list[str]) -> dict[str, Any]:
        record_id = self.storage.ingest(category=category, summary=summary, payload=payload, tags=tags)
        return {
            "ok": True,
            "action": "ingest",
            "data": {
                "id": record_id,
                "category": category,
                "summary": summary,
                "tags": tags,
            },
        }

    def extract(
        self,
        *,
        category: str | None = None,
        limit: int | None = None,
        output: str | None = None,
        output_format: str = "json",
    ) -> dict[str, Any]:
        rows = self.storage.fetch_records(category=category, limit=limit)

        flattened: list[dict[str, Any]] = []
        for row in rows:
            payload = json.loads(row["payload"])
            flattened_payload = flatten_dict(payload)
            flattened.append(
                {
                    "id": row["id"],
                    "timestamp": row["timestamp"],
                    "category": row["category"],
                    "summary": row["summary"],
                    "tags": row["tags"].split(",") if row["tags"] else [],
                    **flattened_payload,
                }
            )

        export_path = None
        if output:
            export_path = self._write_output(flattened, output, output_format)

        return {
            "ok": True,
            "action": "extract",
            "data": {
                "count": len(flattened),
                "records": flattened,
                "exported_to": export_path,
            },
        }

    def status(self) -> dict[str, Any]:
        stats = self.storage.category_stats()
        return {
            "ok": True,
            "action": "status",
            "data": {
                "total_records": self.storage.total_count(),
                "categories": [
                    {
                        "category": row["category"],
                        "count": int(row["count"]),
                        "bar": "#" * int(row["count"]),
                    }
                    for row in stats
                ],
            },
        }

    @staticmethod
    def _write_output(records: list[dict[str, Any]], output: str, output_format: str) -> str:
        output_path = Path(output)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        fmt = output_format.lower().strip()
        if fmt == "json":
            output_path.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")
        elif fmt in {"csv", "excel"}:
            fieldnames = sorted({key for record in records for key in record.keys()})
            with output_path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=fieldnames)
                writer.writeheader()
                for record in records:
                    writer.writerow(record)
        else:
            raise ValueError("output_format must be one of: json, csv, excel")

        return str(output_path)
