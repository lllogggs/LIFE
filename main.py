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
