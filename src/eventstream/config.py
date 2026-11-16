"""Configuration for the event streaming platform.

All settings are overridable through ``ESP_*`` environment variables so the
same code runs in tests, in the demo, and in a container without edits.
No secrets are involved: the engine is fully offline and in-process.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


@dataclass(frozen=True)
class Settings:
    """Tunable platform defaults."""

    default_partitions: int = 4
    default_retention_ms: int = 24 * 60 * 60 * 1000  # 24 hours
    max_records_per_partition: int = 100_000
    max_delivery_attempts: int = 3
    lag_alert_threshold: int = 100
    p95_latency_target_ms: float = 500.0
    dlq_rate_threshold: float = 0.01  # fraction of consumed records
    quarantine_rate_threshold: float = 0.05  # fraction of produced records
    host: str = "0.0.0.0"
    port: int = 8000
    checkpoint_path: str | None = None

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            default_partitions=_env_int("ESP_DEFAULT_PARTITIONS", 4),
            default_retention_ms=_env_int("ESP_DEFAULT_RETENTION_MS", 24 * 60 * 60 * 1000),
            max_records_per_partition=_env_int("ESP_MAX_RECORDS_PER_PARTITION", 100_000),
            max_delivery_attempts=_env_int("ESP_MAX_DELIVERY_ATTEMPTS", 3),
            lag_alert_threshold=_env_int("ESP_LAG_ALERT_THRESHOLD", 100),
            p95_latency_target_ms=_env_float("ESP_P95_LATENCY_TARGET_MS", 500.0),
            dlq_rate_threshold=_env_float("ESP_DLQ_RATE_THRESHOLD", 0.01),
            quarantine_rate_threshold=_env_float("ESP_QUARANTINE_RATE_THRESHOLD", 0.05),
            host=os.environ.get("ESP_HOST", "0.0.0.0"),
            port=_env_int("ESP_PORT", 8000),
            checkpoint_path=os.environ.get("ESP_CHECKPOINT_PATH"),
        )
