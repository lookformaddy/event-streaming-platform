"""Consumer groups: offset tracking, checkpointing, rebalance, delivery.

A consumer group tracks one committed offset per (topic, partition) — the
next offset to read. Members join/leave and partitions are reassigned with a
deterministic round-robin rebalance. ``run_consumer`` is the processing loop:
at-least-once delivery, idempotent handling, bounded retries, and parking in
the dead-letter queue after ``max_delivery_attempts`` failures. Records are
processed in offset order within each partition, and processing stops at the
first failure in a partition so a failed record is retried — never skipped
by a later commit — until it succeeds or reaches the DLQ.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from eventstream.broker import Broker
from eventstream.config import Settings
from eventstream.dlq import DeadLetterQueue
from eventstream.errors import GroupError
from eventstream.exactly_once import ExactlyOnceProcessor
from eventstream.metrics import Metrics
from eventstream.models import ConsumerRunSummary, FailureEntry, Record
from eventstream.partitioning import assign_partitions


@dataclass
class DeliveryTracker:
    """Counts delivery attempts per (topic, partition, offset) and keeps the
    failure history so the DLQ entry can carry every attempt, not just the
    last one."""

    attempts: dict[tuple[str, int, int], int] = field(default_factory=dict)
    history: dict[tuple[str, int, int], list[FailureEntry]] = field(default_factory=dict)

    def record_attempt(
        self, record: Record, error: str = "", timestamp_ms: int = 0
    ) -> int:
        key = (record.topic, record.partition, record.offset)
        self.attempts[key] = self.attempts.get(key, 0) + 1
        self.history.setdefault(key, []).append(
            FailureEntry(attempt=self.attempts[key], error=error, timestamp_ms=timestamp_ms)
        )
        return self.attempts[key]

    def count(self, record: Record) -> int:
        return self.attempts.get((record.topic, record.partition, record.offset), 0)

    def failures(self, record: Record) -> list[FailureEntry]:
        return list(self.history.get((record.topic, record.partition, record.offset), []))

    def reset(self, record: Record) -> None:
        self.attempts.pop((record.topic, record.partition, record.offset), None)
        self.history.pop((record.topic, record.partition, record.offset), None)


class CheckpointStore:
    """Optional JSON-file persistence for committed offsets.

    Offsets live in memory by default; passing ``path`` makes checkpoints
    survive a process restart (load on group creation, save on commit).
    """

    def __init__(self, path: str | Path | None = None) -> None:
        self.path = Path(path) if path else None

    def load(self) -> dict[str, dict[str, int]]:
        if self.path is None or not self.path.exists():
            return {}
        try:
            data = json.loads(self.path.read_text())
        except (OSError, ValueError):
            return {}
        if not isinstance(data, dict):
            return {}
        out: dict[str, dict[str, int]] = {}
        for topic, offsets in data.items():
            if isinstance(offsets, dict):
                out[topic] = {k: int(v) for k, v in offsets.items()}
        return out

    def save(self, offsets: dict[str, dict[str, int]]) -> None:
        if self.path is None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(offsets, sort_keys=True, indent=2))


class ConsumerGroup:
    """One consumer group: members, subscriptions, committed offsets."""

    def __init__(
        self,
        name: str,
        broker: Broker,
        *,
        settings: Settings | None = None,
        checkpoint_path: str | Path | None = None,
    ) -> None:
        self.name = name
        self.broker = broker
        self.settings = settings or broker.settings
        self._members: set[str] = set()
        self._subscriptions: set[str] = set()
        self._committed: dict[str, dict[int, int]] = {}
        self.rebalance_count = 0
        self._checkpoints = CheckpointStore(checkpoint_path)
        saved = self._checkpoints.load()
        for topic, offsets in saved.items():
            self._committed[topic] = {int(p): int(o) for p, o in offsets.items()}

    # -- membership & subscription --------------------------------------
    def join(self, member_id: str) -> None:
        if member_id in self._members:
            return
        self._members.add(member_id)
        self._rebalance()

    def leave(self, member_id: str) -> None:
        if member_id not in self._members:
            raise GroupError(f"member {member_id!r} is not in group {self.name!r}")
        self._members.discard(member_id)
        self._rebalance()

    def members(self) -> list[str]:
        return sorted(self._members)

    def subscribe(self, topics: list[str]) -> None:
        for topic in topics:
            self.broker.topic(topic)  # fail fast on unknown topics
        self._subscriptions.update(topics)
        self._rebalance()

    def subscriptions(self) -> list[str]:
        return sorted(self._subscriptions)

    def assignment(self, member_id: str) -> dict[str, list[int]]:
        """Partitions assigned to one member, per subscribed topic."""
        if member_id not in self._members:
            raise GroupError(f"member {member_id!r} is not in group {self.name!r}")
        result: dict[str, list[int]] = {}
        for topic in sorted(self._subscriptions):
            partitions = list(range(self.broker.topic(topic).partitions))
            result[topic] = assign_partitions(self.members(), partitions).get(member_id, [])
        return result

    def _rebalance(self) -> None:
        self.rebalance_count += 1

    # -- offsets ----------------------------------------------------------
    def committed(self, topic: str, partition: int) -> int:
        """Next offset this group will read from (0 = from the beginning)."""
        return self._committed.get(topic, {}).get(partition, 0)

    def commit(self, topic: str, partition: int, offset: int) -> None:
        self._committed.setdefault(topic, {})[partition] = offset
        self._checkpoints.save(
            {t: {str(p): o for p, o in offs.items()} for t, offs in self._committed.items()}
        )

    def seek(self, topic: str, partition: int, offset: int) -> None:
        """Reset the read position (used for replay / reprocessing)."""
        if offset < 0:
            raise ValueError("offset must be >= 0")
        self.commit(topic, partition, offset)

    def lag(self, topic: str | None = None) -> int:
        """Unconsumed records across subscribed topics (or one topic)."""
        topics = [topic] if topic else sorted(self._subscriptions)
        total = 0
        for name in topics:
            for partition, latest in self.broker.end_offsets(name).items():
                total += max(0, latest - self.committed(name, partition))
        return total

    # -- polling ----------------------------------------------------------
    def poll(self, member_id: str, max_records: int = 100) -> list[Record]:
        """Fetch up to ``max_records`` from this member's assigned partitions."""
        assignment = self.assignment(member_id)
        out: list[Record] = []
        for topic in sorted(assignment):
            for partition in assignment[topic]:
                if len(out) >= max_records:
                    return out
                start = self.committed(topic, partition)
                out.extend(self.broker.fetch(topic, partition, start, max_records - len(out)))
        return out


def run_consumer(
    broker: Broker,
    group: ConsumerGroup,
    handler: Callable[[Record], object],
    *,
    dlq: DeadLetterQueue | None = None,
    metrics: Metrics | None = None,
    processor: ExactlyOnceProcessor | None = None,
    max_passes: int = 100,
    batch_size: int = 100,
) -> ConsumerRunSummary:
    """Drive a group to zero lag with at-least-once delivery.

    For each record: skip it if its dedup key was already processed
    (exactly-once via idempotent processing); otherwise call ``handler``.
    On failure, retry on the next pass; after ``max_delivery_attempts``
    failures the record is parked in the DLQ and its offset committed so
    one poison record can never stall the group. Within a partition,
    processing stops at the first failure so no later record is committed
    past a failed one.
    """
    dlq = dlq or DeadLetterQueue()
    metrics = metrics or Metrics()
    processor = processor or ExactlyOnceProcessor()
    tracker = DeliveryTracker()
    settings = group.settings
    if not group.members():
        group.join("default-member")
    members = group.members()

    processed = skipped = dlq_count = failed_attempts = 0
    passes = 0
    while passes < max_passes:
        passes += 1
        delivered_any = False
        for member in members:
            batch = group.poll(member, max_records=batch_size)
            if batch:
                delivered_any = True
            # Process in offset order, grouped per partition, stopping at
            # the first failure in each partition.
            by_partition: dict[tuple[str, int], list[Record]] = {}
            for record in batch:
                by_partition.setdefault((record.topic, record.partition), []).append(record)
            for (topic, partition), records in sorted(by_partition.items()):
                for record in records:
                    dedup_key = (
                        record.dedup_key or f"{topic}:{partition}:{record.offset}"
                    )
                    if processor.is_processed(dedup_key):
                        skipped += 1
                        group.commit(topic, partition, record.offset + 1)
                        tracker.reset(record)
                        continue
                    try:
                        result = handler(record)
                    except Exception as exc:  # noqa: BLE001 - handler is user code
                        failed_attempts += 1
                        metrics.record_failure(topic)
                        attempts = tracker.record_attempt(
                            record, str(exc), broker.clock.now_ms()
                        )
                        if attempts >= settings.max_delivery_attempts:
                            dlq.add(
                                record,
                                str(exc),
                                attempts,
                                broker.clock.now_ms(),
                                failures=tracker.failures(record),
                            )
                            dlq_count += 1
                            group.commit(topic, partition, record.offset + 1)
                            tracker.reset(record)
                            continue  # move on to the next record
                        break  # stop this partition: retry on the next pass
                    processor.mark_processed(dedup_key, result)
                    latency_ms = broker.clock.now_ms() - record.timestamp_ms
                    metrics.record_consume(topic, latency_ms)
                    processed += 1
                    group.commit(topic, partition, record.offset + 1)
                    tracker.reset(record)
        if not delivered_any or group.lag() == 0:
            break
    return ConsumerRunSummary(
        processed=processed,
        skipped_duplicates=skipped,
        dlq=dlq_count,
        failed_attempts=failed_attempts,
        passes=passes,
        final_lag=group.lag(),
    )
