from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any


class CloudRateLimitError(RuntimeError):
    """Raised when cloud API rate limit is exceeded."""


class CloudConfigError(RuntimeError):
    """Raised when cloud configuration/dependency is missing or invalid."""


class CloudSync:
    def __init__(self) -> None:
        try:
            from dotenv import load_dotenv
            from supabase import Client, create_client
        except ModuleNotFoundError as exc:  # pragma: no cover - env dependent
            raise CloudConfigError("cloud dependencies missing: install python-dotenv and supabase") from exc

        load_dotenv()
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_KEY")
        if not url or not key:
            raise CloudConfigError("SUPABASE_URL and SUPABASE_KEY must be set in environment")
        self.client: Client = create_client(url, key)

    def push_telemetry(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        try:
            self.client.table("global_telemetry").insert(rows).execute()
        except Exception as exc:  # noqa: BLE001
            if self._is_rate_limited(exc):
                raise CloudRateLimitError(
                    "일일 글로벌 조회 횟수(예: 100회)를 초과했습니다. 내일 다시 시도해주세요."
                ) from exc
            raise

    def explore_global_stats(self, *, category: str, demographic_tag: str) -> list[dict[str, Any]]:
        try:
            response = (
                self.client.table("global_stats")
                .select("*")
                .eq("category", category)
                .eq("demographic_tag", demographic_tag)
                .execute()
            )
            data = response.data if response.data else []
            return [dict(row) for row in data]
        except Exception as exc:  # noqa: BLE001
            if self._is_rate_limited(exc):
                raise CloudRateLimitError(
                    "일일 글로벌 조회 횟수(예: 100회)를 초과했습니다. 내일 다시 시도해주세요."
                ) from exc
            raise

    @staticmethod
    def anonymize_payload(payload: Mapping[str, Any]) -> dict[str, float]:
        metrics: dict[str, float] = {}

        def walk(prefix: str, obj: Any) -> None:
            if isinstance(obj, bool):
                metrics[prefix] = 1.0 if obj else 0.0
            elif isinstance(obj, (int, float)):
                metrics[prefix] = float(obj)
            elif isinstance(obj, Mapping):
                for key, value in obj.items():
                    child = f"{prefix}.{key}" if prefix else str(key)
                    walk(child, value)
            elif isinstance(obj, list):
                numeric_values = [v for v in obj if isinstance(v, (int, float)) and not isinstance(v, bool)]
                if numeric_values:
                    metrics[f"{prefix}.count"] = float(len(numeric_values))
                    metrics[f"{prefix}.avg"] = float(sum(numeric_values) / len(numeric_values))

        walk("", payload)
        return metrics

    @staticmethod
    def _is_rate_limited(exc: Exception) -> bool:
        message = str(exc)
        return "429" in message or "Too Many Requests" in message
