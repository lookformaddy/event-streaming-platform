"""Throughput / latency metrics and SLI evaluation with lag-based alerting.

Metrics are kept in bounded in-memory series so the engine stays dependency
free. Latency is end-to-end: event timestamp (produce time) to consume time.
``SliEvaluator`` turns lag, p95 latency, DLQ rate, and quarantine rate into
a HEALTHY / DEGRADED / UNHEALTHY report with named alerts.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field

from eventstream.config import Settings
from eventstream.models import SliReport


def percentile(values: list[float], p: float) -> float:
    """Nearest-rank percentile; 0.0 for an empty series."""
    if not values:
        return 0.0
    if not 0 <= p <= 100:
        raise ValueError("percentile must be between 0 and 100")
    ordered = sorted(values)
    rank = max(1, int(-(-p * len(ordered) // 100)))  # ceil(p/100 * n)
    return float(ordered[min(rank, len(ordered)) - 1])


@dataclass
class _Series:
    produced_ms: list[int] = field(default_factory=list)
    consumed_ms: list[int] = field(default_factory=list)
    latencies_ms: list[float] = field(default_factory=list)
    failures: int = 0


class Metrics:
    """Per-topic produce/consume counters, latencies, and lag helpers."""

    def __init__(self, clock=None) -> None:
        self._clock = clock
        self._series: dict[str, _Series] = {}

    def _now_ms(self) -> int:
        if self._clock is not None:
            return self._clock.now_ms()
        return int(time.time() * 1000)

    def _topic(self, topic: str) -> _Series:
        return self._series.setdefault(topic, _Series())

    def record_produce(self, topic: str, count: int = 1, timestamp_ms: int | None = None) -> None:
        series = self._topic(topic)
        now = timestamp_ms if timestamp_ms is not None else self._now_ms()
        series.produced_ms.extend([now] * count)

    def record_consume(
        self, topic: str, latency_ms: float, count: int = 1, timestamp_ms: int | None = None
    ) -> None:
        series = self._topic(topic)
        now = timestamp_ms if timestamp_ms is not None else self._now_ms()
        series.consumed_ms.extend([now] * count)
        series.latencies_ms.extend([latency_ms] * count)

    def record_failure(self, topic: str, count: int = 1) -> None:
        self._topic(topic).failures += count

    def _rate(self, series_values: list[int], window_ms: int) -> float:
        if not series_values:
            return 0.0
        now = self._now_ms()
        cutoff = now - window_ms
        recent = sum(1 for ts in series_values if ts >= cutoff)
        return recent / (window_ms / 1000.0)

    def produce_rate(self, topic: str, window_ms: int = 60_000) -> float:
        """Events/sec produced in the trailing window."""
        series = self._series.get(topic)
        return self._rate(series.produced_ms if series else [], window_ms)

    def consume_rate(self, topic: str, window_ms: int = 60_000) -> float:
        """Events/sec consumed in the trailing window."""
        series = self._series.get(topic)
        return self._rate(series.consumed_ms if series else [], window_ms)

    def latency_percentile(self, topic: str, p: float = 95.0) -> float:
        series = self._series.get(topic)
        return percentile(series.latencies_ms if series else [], p)

    def p95_latency(self, topic: str | None = None) -> float:
        if topic is not None:
            return self.latency_percentile(topic, 95.0)
        all_latencies: list[float] = []
        for series in self._series.values():
            all_latencies.extend(series.latencies_ms)
        return percentile(all_latencies, 95.0)

    def failure_count(self, topic: str | None = None) -> int:
        if topic is not None:
            series = self._series.get(topic)
            return series.failures if series else 0
        return sum(s.failures for s in self._series.values())

    def consumer_lag(self, broker, group, topic: str) -> int:
        """Unconsumed records for a group on one topic."""
        return group.lag(topic)

    def snapshot(self) -> dict:
        return {
            topic: {
                "produced": len(series.produced_ms),
                "consumed": len(series.consumed_ms),
                "failures": series.failures,
                "p50_latency_ms": percentile(series.latencies_ms, 50.0),
                "p95_latency_ms": percentile(series.latencies_ms, 95.0),
                "p99_latency_ms": percentile(series.latencies_ms, 99.0),
            }
            for topic, series in sorted(self._series.items())
        }


class SliEvaluator:
    """Evaluate lag, latency, DLQ rate, and quarantine rate against targets."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or Settings()

    def evaluate(
        self,
        *,
        lag: int,
        p95_latency_ms: float,
        dlq_rate: float,
        quarantine_rate: float,
    ) -> SliReport:
        settings = self.settings
        alerts: list[str] = []
        if lag > settings.lag_alert_threshold:
            alerts.append(f"consumer lag {lag} exceeds alert threshold {settings.lag_alert_threshold}")
        elif lag > 0:
            alerts.append(f"consumer lag {lag} is non-zero")
        if p95_latency_ms > settings.p95_latency_target_ms:
            alerts.append(
                f"p95 latency {p95_latency_ms:.1f}ms exceeds target {settings.p95_latency_target_ms:.1f}ms"
            )
        if dlq_rate > settings.dlq_rate_threshold:
            alerts.append(f"DLQ rate {dlq_rate:.2%} exceeds threshold {settings.dlq_rate_threshold:.2%}")
        if quarantine_rate > settings.quarantine_rate_threshold:
            alerts.append(
                f"quarantine rate {quarantine_rate:.2%} exceeds threshold {settings.quarantine_rate_threshold:.2%}"
            )
        if lag > settings.lag_alert_threshold or p95_latency_ms > 2 * settings.p95_latency_target_ms:
            status = "UNHEALTHY"
        elif alerts:
            status = "DEGRADED"
        else:
            status = "HEALTHY"
        return SliReport(
            status=status,
            lag=lag,
            p95_latency_ms=p95_latency_ms,
            dlq_rate=dlq_rate,
            quarantine_rate=quarantine_rate,
            alerts=tuple(alerts),
            targets={
                "lag_alert_threshold": settings.lag_alert_threshold,
                "p95_latency_target_ms": settings.p95_latency_target_ms,
                "dlq_rate_threshold": settings.dlq_rate_threshold,
                "quarantine_rate_threshold": settings.quarantine_rate_threshold,
            },
        )
