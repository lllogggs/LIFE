from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from core import LifeOSApp
from core.utils import PayloadValidationError, normalize_tags, parse_payload


def emit(response: dict[str, Any], exit_code: int = 0) -> None:
    print(json.dumps(response, ensure_ascii=False))
    raise SystemExit(exit_code)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="jw.py", description="jw-life-os CLI")
    parser.add_argument("--db", default="jw_life.db", help="SQLite database path")

    subparsers = parser.add_subparsers(dest="command", required=True)

    ingest = subparsers.add_parser("ingest", help="Store a life event payload")
    ingest.add_argument("--cat", required=True, help="Category name")
    ingest.add_argument("--sum", required=True, help="One-line summary")
    ingest.add_argument("--data", required=True, help="JSON payload object")
    ingest.add_argument("--tags", default="", help="Comma-separated tags")
    ingest.add_argument(
        "--on-duplicate",
        default="insert",
        choices=["skip", "update", "insert"],
        help="Dedup policy when source fingerprint already exists",
    )
    ingest.add_argument("--fingerprint", default=None, help="Optional source fingerprint")

    ingest_raw = subparsers.add_parser("ingest-raw", help="Ingest unstructured input and normalize automatically")
    ingest_raw.add_argument(
        "--source-type",
        required=True,
        choices=["text", "image", "excel"],
        help="Source type for normalization",
    )
    ingest_raw.add_argument("--text", default=None, help="Raw free-form text input")
    ingest_raw.add_argument("--file", dest="file_path", default=None, help="File path for image/excel sources")
    ingest_raw.add_argument(
        "--on-duplicate",
        default="insert",
        choices=["skip", "update", "insert"],
        help="Dedup policy when normalized fingerprint already exists",
    )

    extract = subparsers.add_parser("extract", help="Extract and flatten records")
    extract.add_argument("--cat", default=None, help="Optional category filter")
    extract.add_argument("--limit", type=int, default=None, help="Optional record limit")
    extract.add_argument("--out", default=None, help="Optional output file path")
    extract.add_argument(
        "--format",
        default="json",
        choices=["json", "csv", "excel"],
        help="Output format (excel writes flattened CSV for compatibility)",
    )

    subparsers.add_parser("status", help="Show category and record statistics")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    app = LifeOSApp(Path(args.db))

    try:
        if args.command == "ingest":
            payload = parse_payload(args.data)
            result = app.ingest(
                category=args.cat,
                summary=args.sum,
                payload=payload,
                tags=normalize_tags(args.tags),
                source_fingerprint=args.fingerprint,
                on_duplicate=args.on_duplicate,
            )
            emit(result)

        if args.command == "ingest-raw":
            result = app.ingest_raw(
                source_type=args.source_type,
                text=args.text,
                file_path=args.file_path,
                on_duplicate=args.on_duplicate,
            )
            emit(result)

        if args.command == "extract":
            result = app.extract(
                category=args.cat,
                limit=args.limit,
                output=args.out,
                output_format=args.format,
            )
            emit(result)

        if args.command == "status":
            emit(app.status())

        emit({"ok": False, "error": "unknown command"}, exit_code=2)
    except PayloadValidationError as exc:
        emit(
            {
                "ok": False,
                "error": {
                    "type": "PayloadValidationError",
                    "message": str(exc),
                },
            },
            exit_code=1,
        )
    except Exception as exc:  # noqa: BLE001
        emit(
            {
                "ok": False,
                "error": {
                    "type": exc.__class__.__name__,
                    "message": str(exc),
                },
            },
            exit_code=1,
        )


if __name__ == "__main__":
    main()
