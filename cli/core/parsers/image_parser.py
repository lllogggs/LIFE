from __future__ import annotations

from pathlib import Path
from typing import Any

from .text_parser import parse_text_like


def parse_image_like(file_path: str) -> dict[str, Any]:
    path = Path(file_path)
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"image file not found: {file_path}")

    # Lightweight fallback parser:
    # 1) if sidecar .txt exists, treat as OCR output
    # 2) else parse filename tokens only
    sidecar = path.with_suffix(path.suffix + ".txt")
    if sidecar.exists():
        parsed = parse_text_like(sidecar.read_text(encoding="utf-8"))
        parsed["source_meta"]["source_type"] = "image"
        parsed["source_meta"]["image_path"] = str(path)
        parsed["source_meta"]["ocr_sidecar"] = str(sidecar)
        parsed["confidence"] = min(0.9, float(parsed.get("confidence", 0.5)) + 0.1)
        return parsed

    return {
        "raw_fields": {"filename": path.name},
        "raw_text": path.stem.replace("_", " "),
        "confidence": 0.3,
        "source_meta": {"source_type": "image", "image_path": str(path), "ocr_sidecar": None},
    }
