from __future__ import annotations

from typing import Any

from fastapi import Depends, FastAPI, Query
from sqlalchemy import select
from sqlalchemy.orm import Session

from .database import Base, SessionLocal, engine
from .models import GlobalSchema, GlobalTelemetry
from .schemas import ExploreResponse, SchemaResponse, SchemaUpsertRequest, SyncRequest

app = FastAPI(title="jw-life-os global server", version="0.1.0")


@app.on_event("startup")
def on_startup() -> None:
    Base.metadata.create_all(bind=engine)


def get_db() -> Session:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.post("/api/v1/sync")
def sync_telemetry(payload: SyncRequest, db: Session = Depends(get_db)) -> dict[str, Any]:
    for item in payload.records:
        db.add(
            GlobalTelemetry(
                client_id=item.client_id,
                category=item.category,
                demographic_tag=item.demographic_tag,
                telemetry=item.metrics,
            )
        )
    db.commit()
    return {"ok": True, "ingested": len(payload.records)}


@app.get("/api/v1/explore", response_model=ExploreResponse)
def explore(
    category: str = Query(...),
    demographic_tag: str = Query(...),
    db: Session = Depends(get_db),
) -> ExploreResponse:
    rows = db.execute(
        select(GlobalTelemetry)
        .where(GlobalTelemetry.category == category)
        .where(GlobalTelemetry.demographic_tag == demographic_tag)
    ).scalars().all()

    aggregate: dict[str, dict[str, float]] = {}
    for row in rows:
        for key, value in row.telemetry.items():
            metric = aggregate.setdefault(key, {"sum": 0.0, "count": 0.0})
            metric["sum"] += float(value)
            metric["count"] += 1.0

    stats = [
        {"metric": metric, "avg": values["sum"] / values["count"], "samples": int(values["count"])}
        for metric, values in sorted(aggregate.items())
        if values["count"] > 0
    ]

    return ExploreResponse(category=category, demographic_tag=demographic_tag, count=len(rows), stats=stats)


@app.get("/api/v1/schema", response_model=SchemaResponse)
def list_schemas(category: str | None = Query(None), db: Session = Depends(get_db)) -> SchemaResponse:
    stmt = select(GlobalSchema)
    if category:
        stmt = stmt.where(GlobalSchema.category == category)

    rows = db.execute(stmt.order_by(GlobalSchema.category.asc())).scalars().all()
    payload = [
        {
            "category": row.category,
            "schema": row.schema,
            "description": row.description,
            "upvotes": row.upvotes,
            "updated_at": row.updated_at,
        }
        for row in rows
    ]
    return SchemaResponse(schemas=payload)


@app.post("/api/v1/schema")
def upsert_schema(payload: SchemaUpsertRequest, db: Session = Depends(get_db)) -> dict[str, Any]:
    row = db.execute(select(GlobalSchema).where(GlobalSchema.category == payload.category)).scalar_one_or_none()
    if row is None:
        row = GlobalSchema(
            category=payload.category,
            schema=payload.schema,
            description=payload.description,
        )
        db.add(row)
    else:
        row.schema = payload.schema
        row.description = payload.description

    db.commit()
    db.refresh(row)
    return {
        "ok": True,
        "category": row.category,
        "schema": row.schema,
        "description": row.description,
        "upvotes": row.upvotes,
    }
