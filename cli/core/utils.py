from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any


class PayloadValidationError(ValueError):
    """Raised when payload JSON is invalid."""


def parse_payload(payload_input: str) -> dict[str, Any]:
    try:
        payload = json.loads(payload_input)
    except json.JSONDecodeError as exc:
        raise PayloadValidationError(f"invalid JSON payload: {exc.msg} at position {exc.pos}") from exc

    if not isinstance(payload, Mapping):
        raise PayloadValidationError("payload must be a JSON object")

    return dict(payload)


def normalize_tags(tags_input: str | None) -> list[str]:
    if not tags_input:
        return []
    return [tag.strip() for tag in tags_input.split(",") if tag.strip()]


def flatten_dict(data: Mapping[str, Any], parent_key: str = "", sep: str = ".") -> dict[str, Any]:
    flat: dict[str, Any] = {}
    for key, value in data.items():
        composite = f"{parent_key}{sep}{key}" if parent_key else str(key)
        if isinstance(value, Mapping):
            flat.update(flatten_dict(value, composite, sep=sep))
        elif isinstance(value, list):
            flat[composite] = json.dumps(value, ensure_ascii=False)
        else:
            flat[composite] = value
    return flat
