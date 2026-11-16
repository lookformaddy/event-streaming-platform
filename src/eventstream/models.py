"""Core data model: schemas, records, topics, and outcome types.

Everything is a plain dataclass so the engine stays serializable, testable,
and free of framework coupling. Timestamps are epoch milliseconds.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from eventstream.errors import FieldViolation


@dataclass(frozen=True)
class FieldDef:
    """One field of a versioned event schema."""

    name: str
    type: str  # "string" | "integer" | "number" | "boolean" | "enum"
    required: bool = True
    minimum: float | None = None
    maximum: float | None = None
    enum: tuple[str, ...] | None = None
    pattern: str | None = None
    min_length: int | None = None
    max_length: int | None = None


@dataclass(frozen=True)
class SchemaVersion:
    """A versioned event schema (one version of one named event type)."""

    name: str
    version: int
    fields: tuple[FieldDef, ...]

    def field_names(self) -> set[str]:
        return {f.name for f in self.fields}


@dataclass(frozen=True)
class TopicConfig:
    """Configuration for a topic, fixed at creation time."""

    name: str
    partitions: int = 4
    retention_ms: int | None = None  # None -> Settings.default_retention_ms
    max_records_per_partition: int | None = None  # None -> Settings default
    compaction: bool = False
    schema_name: str | None = None
    schema_version: int | None = None


@dataclass(frozen=True)
class Record:
    """One immutable event stored in a partition log."""

    topic: str
    partition: int
    offset: int
    key: str | None
    value: dict
    timestamp_ms: int
    producer_id: str = "producer-1"
    dedup_key: str | None = None
    headers: dict = field(default_factory=dict)


@dataclass(frozen=True)
class QuarantinedRecord:
    """A record that failed schema validation at produce time.

    The original payload and every violation are kept, so a bad record is
    diagnosable and recoverable — never silently dropped.
    """

    topic: str
    key: str | None
    value: object
    violations: tuple[FieldViolation, ...]
    timestamp_ms: int
    producer_id: str = "producer-1"


@dataclass
class ProduceOutcome:
    """Result of one produce call."""

    ok: bool
    record: Record | None = None
    quarantined: bool = False
    duplicate: bool = False
    violations: tuple[FieldViolation, ...] = ()


@dataclass(frozen=True)
class FailureEntry:
    """One failed delivery attempt of a record."""

    attempt: int
    error: str
    timestamp_ms: int


@dataclass
class DlqEntry:
    """A record parked in the dead-letter queue with its failure history."""

    topic: str
    partition: int
    offset: int
    key: str | None
    value: dict
    producer_id: str
    dedup_key: str | None
    original_timestamp_ms: int
    failures: list[FailureEntry] = field(default_factory=list)
    first_failed_ms: int = 0
    last_failed_ms: int = 0


@dataclass(frozen=True)
class SliReport:
    """Service-level evaluation of one run: lag, latency, DLQ/quarantine rates."""

    status: str  # HEALTHY | DEGRADED | UNHEALTHY
    lag: int
    p95_latency_ms: float
    dlq_rate: float
    quarantine_rate: float
    alerts: tuple[str, ...] = ()
    targets: dict = field(default_factory=dict)


@dataclass(frozen=True)
class ConsumerRunSummary:
    """Outcome of one ``run_consumer`` pass loop."""

    processed: int
    skipped_duplicates: int
    dlq: int
    failed_attempts: int
    passes: int
    final_lag: int
