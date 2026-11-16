# Architecture — Event Streaming Platform

## Overview

```
producers ──► Broker ──► partition logs ──► consumer groups ──► handlers
                │  (schema gate)      (offsets, checkpoints)     │
                │                                                  ▼
                ├── quarantine (invalid records + reasons)   idempotency store
                ├── DLQ (poison records + failure history)   transactional outbox
                └── metrics (rates, lag, p95, SLI)           replay (seek/offset/time)
```

The broker is an in-process, deterministic stand-in for a real message
broker (Kafka-style). It owns the durable-feeling parts — topics, partition
logs, offsets — while the transport (network, replication, disk) is
deliberately out of scope. Every semantic decision in this document transfers
directly to a real broker deployment.

## Components

### Broker (`broker.py`)
- **Topic registry**: name → `TopicConfig` (partitions, retention, compaction,
  schema binding). Creating a topic bound to an unknown schema fails fast.
- **Partition logs**: one append-only list per partition. Records are
  immutable dataclasses addressed by `(topic, partition, offset)`; offsets
  are per-partition, sequential, and stable.
- **Produce path**: schema validation → dedup check → partition assignment →
  append → retention enforcement. Validation happens *before* the append, so
  the log never contains a record that violates its topic's schema.
- **Retention**: time-based (drop records older than `retention_ms`) and
  size-based (cap records per partition), enforced on every produce.
  Eviction removes records from the front; surviving offsets never change.
- **Compaction**: keeps only the latest record per key per partition
  (log-compaction semantics for keyed state topics). Offsets of survivors
  are unchanged.
- **Clock**: injectable (`Clock` / `ManualClock`) so retention, latency, and
  replay are deterministic in tests.

### Schemas (`schemas.py`)
- `SchemaRegistry` holds versioned schemas keyed by `(name, version)`.
  `get(name)` returns the latest version; `get(name, version)` pins one.
- Validation is fail-closed and total: missing required fields, wrong types
  (including the Python trap where `bool` is a subclass of `int`), range
  violations, enum violations, regex patterns, string length bounds, and
  unexpected fields are all reported together as `FieldViolation`s with
  field paths and reasons. A record with any violation is quarantined, never
  appended and never dropped silently.

### Partitioning (`partitioning.py`)
- `partition_for_key`: `SHA-256(key) mod N`. Stable across restarts and
  processes, so a key always lands in the same partition and ordering within
  a key is guaranteed by offset order. `None` keys hash as the empty string.
- `assign_partitions`: deterministic round-robin over sorted member ids and
  sorted partition ids. A rebalance is reproducible and every partition is
  owned by exactly one member.

### Consumer groups (`consumer.py`)
- One committed offset per `(topic, partition)` = the *next* offset to read.
  Commits are checkpoints: in-memory by default, optionally persisted to a
  JSON file (`CheckpointStore`) so a restart resumes where it left off.
- `poll` reads from the committed offset across the member's assigned
  partitions. `seek` resets the read position (replay / reprocessing).
- `run_consumer` is the processing loop:
  1. Poll a batch per member.
  2. Process per partition, in offset order. On the first failure in a
     partition, stop that partition for this pass — a later commit must never
     skip past a failed record.
  3. On success: mark the dedup key processed, record end-to-end latency,
     commit `offset + 1`.
  4. On failure: count the attempt; below `max_delivery_attempts`, leave the
     record uncommitted for the next pass; at the limit, park it in the DLQ
     and commit past it so one poison record cannot stall the group.
  5. Repeat until a pass delivers nothing or lag is zero.

### Exactly-once (`exactly_once.py`)
Exactly-once is a composition, not a single mechanism:
- **Producer dedup**: the broker keeps seen dedup keys per topic and drops
  re-produces of a key it has already accepted.
- **Consumer idempotency**: `IdempotencyStore` records processed dedup keys
  with their results; a redelivery returns the cached result without
  re-running the handler.
- **Transactional outbox**: side-effect writes are staged as `pending` and
  only marked `committed` together with the offset commit, so a crash
  between "handler ran" and "offset committed" cannot double-apply a write.
  Pending entries can be discarded on failure.

### Dead-letter queue (`dlq.py`)
- Entries carry the original payload plus a failure history (attempt, error,
  timestamp) and first/last failure times, keyed by the record's original
  `(topic, partition, offset)`.
- `redrive` re-produces entries to their original topic with a fresh dedup
  key (so the broker does not treat the redrive as a duplicate produce).
  Entries are removed only when the re-produce succeeds; a redrive that
  fails validation stays parked.

### Replay (`replay.py`)
- `seek_to_timestamp` finds the first offset with `timestamp >= T` in a
  partition (records are appended in timestamp order).
- `replay` re-reads a slice by offset and/or event-time window, in a
  deterministic order (partition ascending, then offset ascending), so the
  same slice always yields the same output — the property incident
  investigation depends on.

### Metrics and SLI (`metrics.py`)
- Per-topic produce/consume rates over a trailing window, end-to-end
  latency percentiles (nearest-rank), failure counts, and consumer lag
  (latest offset − committed offset, summed over partitions).
- `SliEvaluator` combines lag, p95 latency, DLQ rate, and quarantine rate
  into HEALTHY / DEGRADED / UNHEALTHY with named alerts:
  - HEALTHY: lag 0, p95 under target, rates under thresholds.
  - DEGRADED: any threshold breached but the system is still draining.
  - UNHEALTHY: lag above the alert threshold, or p95 above 2× target.

## Data flow (demo)

```
generate_order_events(seed=42)
  1,000 orders (988 valid, 12 invalid, 5 poison)
        │ produce (schema gate + dedup)
        ▼
orders topic, 4 partitions ──► quarantine: 12 records + reasons
        │ poll (group order-processor, 2 members)
        ▼
handler: poison orders raise ──► 3 attempts ──► DLQ: 5 records
        │ success: 983 processed, lag → 0
        ▼
replay(orders) ──► 988 records == produced set (deterministic)
        ▼
SLI: lag 0, p95 < 500ms, DLQ 0.51% < 1% ──► HEALTHY
```

## Failure modes handled

| Failure | Handling |
|---|---|
| Invalid record produced | Quarantined with payload + reasons; log stays clean |
| Producer retries a produce | Dedup key → second produce is a no-op duplicate |
| Consumer crashes mid-batch | Uncommitted offsets are re-polled; idempotency store skips re-applied work |
| Handler raises transiently | Retried next pass, up to `max_delivery_attempts` |
| Handler raises permanently | Parked in DLQ with full failure history; partition unblocked |
| Consumer group member leaves | Deterministic rebalance reassigns its partitions |
| Incident investigation | Replay reproduces the exact produced set for any offset/time slice |
| Lag grows | Lag-based alert fires; SLI degrades, then goes UNHEALTHY |

## Extensions (documented, not built)

- **Durable broker**: write-ahead log + snapshots on disk; the in-memory
  log is the only part that changes.
- **Real transport**: replace `Broker` calls with a Kafka client; the
  schema, consumer, DLQ, and replay modules are transport-agnostic.
- **Schema evolution**: compatibility checks (BACKWARD / FORWARD / FULL)
  between versions at registration time.
- **Backpressure**: pause polling when lag or handler latency exceeds a
  threshold; the metrics to drive it already exist.
- **Multi-topic transactions**: extend the outbox to commit offsets across
  topics atomically.
