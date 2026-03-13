from __future__ import annotations

from sqlalchemy import JSON, DateTime, Float, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from .database import Base


class GlobalTelemetry(Base):
    __tablename__ = "global_telemetry"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    client_id: Mapped[str] = mapped_column(String(255), index=True)
    category: Mapped[str] = mapped_column(String(255), index=True)
    demographic_tag: Mapped[str] = mapped_column(String(255), index=True)
    telemetry: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[str] = mapped_column(DateTime(timezone=True), server_default=func.now())


class GlobalSchema(Base):
    __tablename__ = "global_schemas"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    category: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    schema: Mapped[dict] = mapped_column(JSON)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    upvotes: Mapped[float] = mapped_column(Float, default=0)
    updated_at: Mapped[str] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
