"""Replay: seek to offset/timestamp and deterministic time-travel reads.

Because offsets are stable and records are immutable, any slice of a topic
can be re-read exactly as it was produced. ``replay`` returns records in a
deterministic order (partition ascending, then offset ascending) so a replay
reproduces the same output every time.
"""

from __future__ import annotations

from eventstream.broker import Broker
from eventstream.models import Record


def seek_to_timestamp(broker: Broker, topic: str, partition: int, timestamp_ms: int) -> int:
    """Offset of the first record with ``timestamp_ms`` >= the given time.

    Returns the latest offset (end of log) when no record is that recent.
    Records are appended in timestamp order within a partition, so a linear
    scan from the earliest offset is correct.
    """
    for record in broker.fetch(topic, partition, 0, max_records=10_000_000):
        if record.timestamp_ms >= timestamp_ms:
            return record.offset
    return broker.latest_offset(topic, partition)


def replay(
    broker: Broker,
    topic: str,
    *,
    partitions: list[int] | None = None,
    from_offset: int = 0,
    from_timestamp_ms: int | None = None,
    to_timestamp_ms: int | None = None,
    max_records: int | None = None,
) -> list[Record]:
    """Deterministically re-read a slice of a topic.

    ``from_timestamp_ms`` / ``to_timestamp_ms`` bound the slice by event
    time (inclusive); ``from_offset`` is a per-partition lower bound applied
    when no timestamp bound is given.
    """
    config = broker.topic(topic)
    wanted = partitions if partitions is not None else list(range(config.partitions))
    out: list[Record] = []
    for partition in sorted(wanted):
        if from_timestamp_ms is not None:
            start = seek_to_timestamp(broker, topic, partition, from_timestamp_ms)
        else:
            start = from_offset
        for record in broker.fetch(topic, partition, start, max_records=10_000_000):
            if to_timestamp_ms is not None and record.timestamp_ms > to_timestamp_ms:
                continue
            out.append(record)
            if max_records is not None and len(out) >= max_records:
                return out
    return out
