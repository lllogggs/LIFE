from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass
class LifeRecord:
    id: int
    timestamp: datetime
    category: str
    summary: str
    payload: dict[str, Any]
    tags: list[str]
    source_fingerprint: str | None = None
