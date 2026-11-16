"""Dead-letter queue: poison records, failure history, redrive.

After ``max_delivery_attempts`` failed deliveries a record is parked here
with its original payload and the full failure history (attempt, error,
timestamp). Redrive re-produces entries back to their original topic so a
fixed handler can process them; entries are only removed once the
re-produce succeeds.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from eventstream.models import DlqEntry, FailureEntry, ProduceOutcome, Record


@dataclass
class DeadLetterQueue:
    """Per-topic DLQ keyed by the record's original identity."""

    _entries: dict[str, list[DlqEntry]] = field(default_factory=dict)

    def add(
        self,
        record: Record,
        error: str,
        attempt: int,
        timestamp_ms: int,
        failures: list[FailureEntry] | None = None,
    ) -> DlqEntry:
        entries = self._entries.setdefault(record.topic, [])
        for entry in entries:
            if entry.partition == record.partition and entry.offset == record.offset:
                entry.failures.append(
                    FailureEntry(attempt=attempt, error=error, timestamp_ms=timestamp_ms)
                )
                entry.last_failed_ms = timestamp_ms
                return entry
        history = (
            list(failures)
            if failures
            else [FailureEntry(attempt=attempt, error=error, timestamp_ms=timestamp_ms)]
        )
        entry = DlqEntry(
            topic=record.topic,
            partition=record.partition,
            offset=record.offset,
            key=record.key,
            value=dict(record.value),
            producer_id=record.producer_id,
            dedup_key=record.dedup_key,
            original_timestamp_ms=record.timestamp_ms,
            failures=history,
            first_failed_ms=history[0].timestamp_ms,
            last_failed_ms=timestamp_ms,
        )
        entries.append(entry)
        return entry

    def entries(self, topic: str | None = None) -> list[DlqEntry]:
        if topic is not None:
            return list(self._entries.get(topic, []))
        out: list[DlqEntry] = []
        for entries in self._entries.values():
            out.extend(entries)
        return out

    def count(self, topic: str | None = None) -> int:
        return len(self.entries(topic))

    def redrive(self, broker, topic: str, *, max_entries: int | None = None) -> list[ProduceOutcome]:
        """Re-produce DLQ entries to their original topic.

        A fresh dedup key is used so the broker does not treat the redrive
        as a duplicate produce. Successfully re-produced entries are removed.
        """
        outcomes: list[ProduceOutcome] = []
        remaining: list[DlqEntry] = []
        entries = self._entries.get(topic, [])
        for index, entry in enumerate(entries):
            if max_entries is not None and index >= max_entries:
                remaining.append(entry)
                continue
            outcome = broker.produce(
                topic,
                entry.key,
                entry.value,
                producer_id=entry.producer_id,
                dedup_key=f"redrive:{topic}:{entry.partition}:{entry.offset}",
            )
            outcomes.append(outcome)
            if not outcome.ok and not outcome.duplicate:
                remaining.append(entry)
        self._entries[topic] = remaining
        return outcomes

    def purge(self, topic: str | None = None) -> int:
        if topic is not None:
            removed = len(self._entries.pop(topic, []))
            return removed
        total = sum(len(v) for v in self._entries.values())
        self._entries.clear()
        return total
