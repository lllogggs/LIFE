from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Literal

from .normalizer import normalize_raw_record
from .parsers import parse_excel_like, parse_image_like, parse_text_like
from .storage import LifeStorage
from .utils import flatten_dict

DuplicatePolicy = Literal["skip", "update", "insert"]

from typing import Any

from .storage import LifeStorage
from .utils import flatten_dict


class LifeOSApp:
    def __init__(self, db_path: str | Path = "jw_life.db") -> None:
        self.storage = LifeStorage(db_path)

    def ingest(
        self,
        *,
        category: str,
        summary: str,
        payload: dict[str, Any],
        tags: list[str],
        source_fingerprint: str | None = None,
        on_duplicate: DuplicatePolicy = "insert",
    ) -> dict[str, Any]:
        duplicate = self.storage.find_by_fingerprint(source_fingerprint) if source_fingerprint else None

        dedup_action = "inserted"
        if duplicate and on_duplicate == "skip":
            return {
                "ok": True,
                "action": "ingest",
                "data": {
                    "id": int(duplicate["id"]),
                    "category": str(duplicate["category"]),
                    "summary": str(duplicate["summary"]),
                    "tags": (duplicate["tags"].split(",") if duplicate["tags"] else []),
                    "source_fingerprint": source_fingerprint,
                    "dedup_action": "skipped",
                },
            }

        if duplicate and on_duplicate == "update":
            record_id = int(duplicate["id"])
            self.storage.update_record(
                record_id,
                category=category,
                summary=summary,
                payload=payload,
                tags=tags,
                source_fingerprint=source_fingerprint,
            )
            dedup_action = "updated"
        else:
            record_id = self.storage.ingest(
                category=category,
                summary=summary,
                payload=payload,
                tags=tags,
                source_fingerprint=source_fingerprint,
            )

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
                "source_fingerprint": source_fingerprint,
                "dedup_action": dedup_action,
            },
        }

    def ingest_raw(
        self,
        *,
        source_type: str,
        text: str | None = None,
        file_path: str | None = None,
        on_duplicate: DuplicatePolicy = "insert",
    ) -> dict[str, Any]:
        source = source_type.strip().lower()
        if source == "text":
            parsed = parse_text_like(text or "")
        elif source == "image":
            if not file_path:
                raise ValueError("file_path is required for image source")
            parsed = parse_image_like(file_path)
        elif source in {"excel", "csv", "xlsx"}:
            if not file_path:
                raise ValueError("file_path is required for excel source")
            parsed = parse_excel_like(file_path)
        else:
            raise ValueError("source_type must be one of: text, image, excel")

        canonical = normalize_raw_record(parsed)

        ingested = self.ingest(
            category=str(canonical["category"]),
            summary=str(canonical["summary"]),
            payload=dict(canonical["payload"]),
            tags=list(canonical["tags"]),
            source_fingerprint=str(canonical["source_fingerprint"]),
            on_duplicate=on_duplicate,
        )

        ingested["action"] = "ingest_raw"
        ingested["data"]["normalized"] = {
            "category": canonical["category"],
            "summary": canonical["summary"],
            "tags": canonical["tags"],
        }
        return ingested

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
                    "source_fingerprint": row["source_fingerprint"],
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
