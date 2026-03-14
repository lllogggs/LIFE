from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class TelemetryRecord(BaseModel):
    client_id: str
    category: str
    demographic_tag: str
    metrics: dict[str, float] = Field(default_factory=dict)


class SyncRequest(BaseModel):
    records: list[TelemetryRecord]


class ExploreResponse(BaseModel):
    category: str
    demographic_tag: str
    count: int
    stats: list[dict[str, Any]]


class SchemaUpsertRequest(BaseModel):
    category: str
    schema: dict[str, Any]
    description: str | None = None


class SchemaResponse(BaseModel):
    schemas: list[dict[str, Any]]
