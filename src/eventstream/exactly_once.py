"""Exactly-once semantics: idempotent processing + transactional outbox.

Exactly-once delivery is impossible in a distributed system, so the platform
delivers at-least-once and makes processing idempotent:

* producers attach a ``dedup_key``; the broker drops re-produces of a key it
  has already seen (see ``Broker.produce``);
* consumers keep an ``IdempotencyStore`` of processed dedup keys, so a
  redelivered record is skipped instead of re-applied;
* ``TransactionalOutbox`` stages side-effect writes with the offset commit
  and only marks them committed together, so a crash between "handler ran"
  and "offset committed" cannot double-apply a side effect.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from eventstream.models import Record


class IdempotencyStore:
    """In-memory record of processed dedup keys and their results."""

    def __init__(self) -> None:
        self._processed: dict[str, Any] = {}

    def is_processed(self, dedup_key: str) -> bool:
        return dedup_key in self._processed

    def mark_processed(self, dedup_key: str, result: Any = None) -> None:
        self._processed[dedup_key] = result

    def result(self, dedup_key: str) -> Any:
        return self._processed.get(dedup_key)

    def size(self) -> int:
        return len(self._processed)


@dataclass
class OutboxEntry:
    """One staged side-effect write, committed atomically with the offset."""

    entry_id: str
    payload: dict
    created_ms: int
    status: str = "pending"  # pending | committed | discarded


@dataclass
class TransactionalOutbox:
    """Stage-then-commit outbox: writes become visible only on commit."""

    entries: list[OutboxEntry] = field(default_factory=list)

    def stage(self, entry: OutboxEntry) -> OutboxEntry:
        self.entries.append(entry)
        return entry

    def commit_all(self) -> list[OutboxEntry]:
        committed = [e for e in self.entries if e.status == "pending"]
        for entry in committed:
            entry.status = "committed"
        return committed

    def discard_pending(self) -> list[OutboxEntry]:
        discarded = [e for e in self.entries if e.status == "pending"]
        for entry in discarded:
            entry.status = "discarded"
        return discarded

    def pending(self) -> list[OutboxEntry]:
        return [e for e in self.entries if e.status == "pending"]


class ExactlyOnceProcessor:
    """Wrap a handler so a redelivered record is never applied twice."""

    def __init__(self, store: IdempotencyStore | None = None) -> None:
        self.store = store or IdempotencyStore()

    def is_processed(self, dedup_key: str) -> bool:
        return self.store.is_processed(dedup_key)

    def mark_processed(self, dedup_key: str, result: Any = None) -> None:
        self.store.mark_processed(dedup_key, result)

    def process(
        self,
        record: Record,
        handler: Callable[[Record], Any],
        *,
        dedup_key: str | None = None,
    ) -> tuple[bool, Any]:
        """Run ``handler`` once per dedup key.

        Returns ``(executed, result)``; ``executed`` is False when the key
        was already processed, in which case the cached result is returned.
        Handler exceptions propagate so the caller can retry / DLQ.
        """
        key = dedup_key or record.dedup_key or f"{record.topic}:{record.partition}:{record.offset}"
        if self.store.is_processed(key):
            return False, self.store.result(key)
        result = handler(record)
        self.store.mark_processed(key, result)
        return True, result
