from __future__ import annotations

from collections.abc import Mapping
from typing import Any



class CloudRateLimitError(RuntimeError):
    """Raised when cloud API rate limit is exceeded."""


class CloudConfigError(RuntimeError):
    """Raised when cloud configuration/dependency is missing or invalid."""


class CloudSync:
    def __init__(self, base_url: str = "http://localhost:8000") -> None:
        self.base_url = base_url.rstrip("/")

    def push_telemetry(self, rows: list[dict[str, Any]]) -> None:
        import requests

        if not rows:
            return
        payload = {"records": rows}
        response = requests.post(f"{self.base_url}/api/v1/sync", json=payload, timeout=10)
        self._raise_for_status(response)

    def explore_global_stats(self, *, category: str, demographic_tag: str) -> list[dict[str, Any]]:
        import requests

        response = requests.get(
            f"{self.base_url}/api/v1/explore",
            params={"category": category, "demographic_tag": demographic_tag},
            timeout=10,
        )
        self._raise_for_status(response)
        data = response.json()
        return list(data.get("stats", []))

    def list_global_schemas(self, *, category: str | None = None) -> list[dict[str, Any]]:
        import requests

        response = requests.get(
            f"{self.base_url}/api/v1/schema",
            params={"category": category} if category else None,
            timeout=10,
        )
        self._raise_for_status(response)
        data = response.json()
        return list(data.get("schemas", []))

    def upsert_global_schema(self, *, category: str, schema: dict[str, Any]) -> dict[str, Any]:
        import requests

        response = requests.post(
            f"{self.base_url}/api/v1/schema",
            json={"category": category, "schema": schema},
            timeout=10,
        )
        self._raise_for_status(response)
        return dict(response.json())

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
    def _raise_for_status(response: Any) -> None:
        if response.status_code == 429:
            raise CloudRateLimitError("일일 글로벌 조회 횟수(예: 100회)를 초과했습니다. 내일 다시 시도해주세요.")
        if response.ok:
            return
        raise CloudConfigError(f"cloud API request failed ({response.status_code}): {response.text}")
