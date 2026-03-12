from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core import LifeOSApp

SUPPORTED_IMAGE = {".png", ".jpg", ".jpeg", ".webp"}
SUPPORTED_EXCEL = {".csv", ".xlsx", ".xlsm", ".xltx"}


def detect_source_type(path: Path) -> str | None:
    ext = path.suffix.lower()
    if ext in SUPPORTED_IMAGE:
        return "image"
    if ext in SUPPORTED_EXCEL:
        return "excel"
    if ext in {".txt", ".md"}:
        return "text"
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Auto ingest dropped files into jw-life DB")
    parser.add_argument("--db", default="jw_life.db", help="SQLite db path")
    parser.add_argument("--inbox", default="inbox", help="Input folder")
    parser.add_argument("--archive", default="archive", help="Archive root")
    parser.add_argument("--logs", default="logs", help="Log folder")
    parser.add_argument("--on-duplicate", default="skip", choices=["skip", "update", "insert"])
    args = parser.parse_args()

    app = LifeOSApp(args.db)

    inbox = Path(args.inbox)
    archive_success = Path(args.archive) / "success"
    archive_failed = Path(args.archive) / "failed"
    log_dir = Path(args.logs)

    archive_success.mkdir(parents=True, exist_ok=True)
    archive_failed.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)

    log_path = log_dir / f"ingest_{datetime.now().strftime('%Y%m%d')}.jsonl"

    for path in sorted(inbox.glob("*")):
        if not path.is_file():
            continue

        source_type = detect_source_type(path)
        if not source_type:
            continue

        try:
            if source_type == "text":
                result = app.ingest_raw(
                    source_type="text",
                    text=path.read_text(encoding="utf-8"),
                    on_duplicate=args.on_duplicate,
                )
            else:
                result = app.ingest_raw(
                    source_type=source_type,
                    file_path=str(path),
                    on_duplicate=args.on_duplicate,
                )
            status = "ok"
            path.rename(archive_success / path.name)
        except Exception as exc:  # noqa: BLE001
            result = {"ok": False, "error": {"type": exc.__class__.__name__, "message": str(exc)}}
            status = "error"
            path.rename(archive_failed / path.name)

        event = {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "status": status,
            "source": str(path),
            "result": result,
        }
        with log_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
