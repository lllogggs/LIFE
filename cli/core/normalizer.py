from __future__ import annotations

import hashlib
import json
from typing import Any


def _infer_category(text: str, fields: dict[str, Any]) -> str:
    haystack = f"{text} {' '.join(f'{k}:{v}' for k, v in fields.items())}".lower()
    if any(token in haystack for token in ["주식", "stock", "ticker", "매수", "매도", "투자"]):
        return "Investment"
    if any(token in haystack for token in ["아파트", "부동산", "real estate", "전세", "월세", "house"]):
        return "Real_Estate"
    if any(token in haystack for token in ["디스크", "통증", "병원", "health", "pain"]):
        return "Health"
    if any(token in haystack for token in ["프로젝트", "milestone", "release", "borderwiki", "tripdotdot"]):
        return "Biz_Project"
    if any(token in haystack for token in ["매출", "md", "영업", "career", "실적"]):
        return "Career"
    return "General"


def _infer_tags(category: str, text: str, fields: dict[str, Any]) -> list[str]:
    tags = {category}
    for key in list(fields.keys())[:5]:
        if key:
            tags.add(str(key).strip().replace(" ", "_"))
    for token in text.replace("\n", " ").split(" "):
        cleaned = token.strip(" ,.!?[](){}\"'")
        if 2 <= len(cleaned) <= 20:
            tags.add(cleaned)
        if len(tags) >= 10:
            break
    return sorted(tags)


def _summary(text: str, fields: dict[str, Any], category: str) -> str:
    if fields:
        head = ", ".join(f"{k}={v}" for k, v in list(fields.items())[:3])
        return f"{category}: {head}"[:200]
    compact = " ".join(text.split())
    return f"{category}: {compact}"[:200]


def normalize_raw_record(parsed: dict[str, Any]) -> dict[str, Any]:
    raw_fields = parsed.get("raw_fields") or {}
    if not isinstance(raw_fields, dict):
        raw_fields = {}

    raw_text = str(parsed.get("raw_text") or "")
    category = _infer_category(raw_text, raw_fields)
    tags = _infer_tags(category, raw_text, raw_fields)

    payload = {
        "raw_text": raw_text,
        "extracted_fields": raw_fields,
        "confidence": float(parsed.get("confidence", 0.0)),
        "source_meta": parsed.get("source_meta", {}),
    }

    canonical = {
        "category": category,
        "summary": _summary(raw_text, raw_fields, category),
        "payload": payload,
        "tags": tags,
    }

    canonical_bytes = json.dumps(canonical, ensure_ascii=False, sort_keys=True).encode("utf-8")
    canonical["source_fingerprint"] = hashlib.sha256(canonical_bytes).hexdigest()
    return canonical
