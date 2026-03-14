from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from cli.core import LifeOSApp
from cli.core.cloud import CloudConfigError, CloudRateLimitError
from cli.core.storage import SchemaValidationError
from cli.core.utils import PayloadValidationError, normalize_tags, parse_payload


def emit(response: dict[str, Any], exit_code: int = 0) -> None:
    print(json.dumps(response, ensure_ascii=False))
    raise SystemExit(exit_code)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="jw.py", description="jw-life-os CLI")
    parser.add_argument("--db", default="jw_life.db", help="SQLite database path")

    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("setup", help="Initialize local client identity")

    schema = subparsers.add_parser("schema", help="Schema registry operations")
    schema_sub = schema.add_subparsers(dest="schema_cmd", required=True)

    schema_add = schema_sub.add_parser("add", help="Add or update category schema")
    schema_add.add_argument("--cat", required=True, help="Category name")
    schema_add.add_argument("--schema", required=True, help="Schema JSON object")

    schema_sub.add_parser("list", help="List registered schemas")

    ingest = subparsers.add_parser("ingest", help="Store validated life payload")
    ingest.add_argument("--cat", required=True, help="Category name")
    ingest.add_argument("--sum", required=True, help="One-line summary")
    ingest.add_argument("--data", required=True, help="JSON payload object")
    ingest.add_argument("--tags", default="", help="Comma-separated tags")
    ingest.add_argument("--target", default=None, help="Demographic tag")
    ingest.add_argument("--fingerprint", default=None, help="Optional source fingerprint")

    extract = subparsers.add_parser("extract", help="Extract local records as CSV")
    extract.add_argument("--cat", default=None, help="Category name")
    extract.add_argument("--out", required=True, help="Output CSV path")

    explore = subparsers.add_parser("explore", help="Explore global stats from jw-life server")
    explore.add_argument("--cat", required=True, help="Category name")
    explore.add_argument("--target", required=True, help="Demographic tag")

    subparsers.add_parser("sync", help="Push anonymized local telemetry to jw-life server")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    app = LifeOSApp(Path(args.db))

    try:
        if args.command == "setup":
            emit(app.setup())

        if args.command == "schema":
            if args.schema_cmd == "add":
                emit(app.schema_add(category=args.cat, schema=parse_payload(args.schema)))
            if args.schema_cmd == "list":
                emit(app.schema_list())

        if args.command == "ingest":
            emit(
                app.ingest(
                    category=args.cat,
                    summary=args.sum,
                    payload=parse_payload(args.data),
                    tags=normalize_tags(args.tags),
                    demographic_tag=args.target,
                    source_fingerprint=args.fingerprint,
                )
            )

        if args.command == "sync":
            emit(app.sync())

        if args.command == "explore":
            emit(app.explore(category=args.cat, demographic_tag=args.target))

        if args.command == "extract":
            emit(app.extract(category=args.cat, out=args.out))

        emit({"ok": False, "error": "unknown command"}, exit_code=2)
    except (PayloadValidationError, SchemaValidationError, CloudConfigError) as exc:
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
    except CloudRateLimitError as exc:
        emit(
            {
                "ok": False,
                "error": {
                    "type": "RateLimitExceeded",
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
