from __future__ import annotations

from typing import Any


def parse_text_like(raw_text: str) -> dict[str, Any]:
    text = raw_text.strip()
    if not text:
        raise ValueError("text input is empty")

    fields: dict[str, str] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if ":" in line:
            key, value = line.split(":", 1)
            fields[key.strip()] = value.strip()

    return {
        "raw_fields": fields,
        "raw_text": text,
        "confidence": 0.65 if fields else 0.45,
        "source_meta": {"source_type": "text", "line_count": len(text.splitlines())},
    }
