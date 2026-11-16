"""The in-process broker: topic registry, partition logs, retention, compaction.

The broker stands in for a real message broker (Kafka-style) so the whole
platform runs anywhere with no external services. Records are immutable and
addressed by ``(topic, partition, offset)``; offsets are stable per
partition and only move forward. Retention (time and size) and compaction are
enforced on produce; schema violations quarantine the record instead of
dropping it; producer dedup keys give idempotent produce.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field

from eventstream.config import Settings
from eventstream.errors import TopicExistsError, TopicNotFoundError
from eventstream.models import ProduceOutcome, QuarantinedRecord, Record, TopicConfig
from eventstream.partitioning import partition_for_key
from eventstream.schemas import SchemaRegistry


class Clock:
    """Wall-clock source for event timestamps (injectable for tests)."""

    def now_ms(self) -> int:
        return int(time.time() * 1000)


class ManualClock(Clock):
    """Deterministic clock for tests: time only moves when advanced."""

    def __init__(self, now_ms: int = 0) -> None:
        self._now_ms = now_ms

    def now_ms(self) -> int:
        return self._now_ms

    def advance(self, delta_ms: int) -> None:
        if delta_ms < 0:
            raise ValueError("cannot move a manual clock backwards")
        self._now_ms += delta_ms

    def set(self, now_ms: int) -> None:
        self._now_ms = now_ms


@dataclass
class _PartitionLog:
    records: list[Record] = field(default_factory=list)
    next_offset: int = 0


@dataclass
class _TopicState:
    config: TopicConfig
    partitions: list[_PartitionLog]
    dedup_keys: set[str] = field(default_factory=set)
    quarantine: list[QuarantinedRecord] = field(default_factory=list)


class Broker:
    """Topic registry + append-only partition logs with retention/compaction."""

    def __init__(
        self,
        settings: Settings | None = None,
        schemas: SchemaRegistry | None = None,
        clock: Clock | None = None,
    ) -> None:
        self.settings = settings or Settings()
        self.schemas = schemas or SchemaRegistry()
        self.clock = clock or Clock()
        self._topics: dict[str, _TopicState] = {}

    # -- topics ---------------------------------------------------------
    def create_topic(self, config: TopicConfig) -> TopicConfig:
        if config.name in self._topics:
            raise TopicExistsError(f"topic {config.name!r} already exists")
        if config.partitions < 1:
            raise ValueError("topic must have at least 1 partition")
        if config.schema_name is not None:
            # Fail fast: a topic bound to an unknown schema is a config error.
            self.schemas.get(config.schema_name, config.schema_version)
        self._topics[config.name] = _TopicState(
            config=config,
            partitions=[_PartitionLog() for _ in range(config.partitions)],
        )
        return config

    def delete_topic(self, name: str) -> None:
        if name not in self._topics:
            raise TopicNotFoundError(f"topic {name!r} not found")
        del self._topics[name]

    def topic(self, name: str) -> TopicConfig:
        return self._state(name).config

    def list_topics(self) -> list[TopicConfig]:
        return [state.config for state in self._topics.values()]

    # -- produce --------------------------------------------------------
    def produce(
        self,
        topic: str,
        key: str | None,
        value: dict,
        *,
        producer_id: str = "producer-1",
        dedup_key: str | None = None,
        timestamp_ms: int | None = None,
        headers: dict | None = None,
    ) -> ProduceOutcome:
        state = self._state(topic)
        now = timestamp_ms if timestamp_ms is not None else self.clock.now_ms()

        if state.config.schema_name is not None:
            schema = self.schemas.get(state.config.schema_name, state.config.schema_version)
            violations = self.schemas.validate(schema, value)
            if violations:
                quarantined = QuarantinedRecord(
                    topic=topic, key=key, value=value,
                    violations=violations, timestamp_ms=now, producer_id=producer_id,
                )
                state.quarantine.append(quarantined)
                return ProduceOutcome(ok=False, quarantined=True, violations=violations)

        if dedup_key is not None and dedup_key in state.dedup_keys:
            return ProduceOutcome(ok=False, duplicate=True)
        if dedup_key is not None:
            state.dedup_keys.add(dedup_key)

        partition = partition_for_key(key, state.config.partitions)
        log = state.partitions[partition]
        record = Record(
            topic=topic, partition=partition, offset=log.next_offset,
            key=key, value=dict(value), timestamp_ms=now,
            producer_id=producer_id, dedup_key=dedup_key,
            headers=dict(headers or {}),
        )
        log.records.append(record)
        log.next_offset += 1
        self._enforce_retention(state)
        return ProduceOutcome(ok=True, record=record)

    # -- consume / inspect ----------------------------------------------
    def fetch(
        self, topic: str, partition: int, offset: int, max_records: int = 100
    ) -> list[Record]:
        log = self._partition(topic, partition)
        return [r for r in log.records if r.offset >= offset][:max_records]

    def earliest_offset(self, topic: str, partition: int) -> int:
        log = self._partition(topic, partition)
        return log.records[0].offset if log.records else log.next_offset

    def latest_offset(self, topic: str, partition: int) -> int:
        """Next offset to be written (one past the last record)."""
        return self._partition(topic, partition).next_offset

    def end_offsets(self, topic: str) -> dict[int, int]:
        state = self._state(topic)
        return {p: log.next_offset for p, log in enumerate(state.partitions)}

    def quarantine(self, topic: str) -> list[QuarantinedRecord]:
        return list(self._state(topic).quarantine)

    def record_count(self, topic: str) -> int:
        state = self._state(topic)
        return sum(len(log.records) for log in state.partitions)

    # -- retention & compaction ------------------------------------------
    def enforce_retention(self, topic: str) -> int:
        """Apply retention now; returns the number of records evicted."""
        return self._enforce_retention(self._state(topic))

    def compact(self, topic: str) -> int:
        """Keep only the latest record per key in each partition.

        Returns the number of records removed. Offsets of surviving records
        are unchanged, so consumers never see a rewritten log.
        """
        state = self._state(topic)
        removed = 0
        for log in state.partitions:
            latest: dict[str | None, Record] = {}
            for record in log.records:
                latest[record.key] = record
            keep = set(id(r) for r in latest.values())
            kept = [r for r in log.records if id(r) in keep]
            removed += len(log.records) - len(kept)
            log.records = kept
        return removed

    # -- internals --------------------------------------------------------
    def _state(self, topic: str) -> _TopicState:
        state = self._topics.get(topic)
        if state is None:
            raise TopicNotFoundError(f"topic {topic!r} not found")
        return state

    def _partition(self, topic: str, partition: int) -> _PartitionLog:
        state = self._state(topic)
        if not 0 <= partition < len(state.partitions):
            raise ValueError(f"partition {partition} out of range for topic {topic!r}")
        return state.partitions[partition]

    def _enforce_retention(self, state: _TopicState) -> int:
        retention_ms = state.config.retention_ms
        if retention_ms is None:
            retention_ms = self.settings.default_retention_ms
        max_records = state.config.max_records_per_partition
        if max_records is None:
            max_records = self.settings.max_records_per_partition
        cutoff = self.clock.now_ms() - retention_ms
        removed = 0
        for log in state.partitions:
            before = len(log.records)
            log.records = [r for r in log.records if r.timestamp_ms >= cutoff]
            while len(log.records) > max_records:
                log.records.pop(0)
            removed += before - len(log.records)
        return removed
