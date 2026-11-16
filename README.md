# Event Streaming Platform

**Kafka-style event streaming, without the broker.** A schema-validated event
streaming platform — topics, partitions, consumer groups, exactly-once
semantics, replay, and dead-letter queues — built as a deterministic
in-process engine that runs anywhere with no external services. It stands in
for a broker so the streaming semantics can be developed, tested, and
demonstrated offline; the same concepts (and the same failure modes) apply
when the transport is swapped for a real broker.

## The enterprise problem

Event-driven systems fail in boring, expensive ways: a producer ships a
record with the wrong field type and a downstream consumer crashes at 2 AM;
a consumer restarts and reprocesses yesterday's orders twice; one poison
message stalls an entire partition; nobody can answer "what did the topic
look like at 10:00?" when an incident is investigated. The usual fixes —
schema registries, idempotent consumers, DLQs, replay — are each a project
of their own, and teams bolt them on after the first incident.

This platform builds them in from the start, as one coherent engine:

- **Schema-validated produce** — every record is validated against a
  versioned schema before it is appended. Invalid records are quarantined
  with the payload and the reason-carrying violations, never silently
  dropped and never allowed to poison the log.
- **Deterministic partitioning** — a stable hash of the record key picks the
  partition, so ordering is guaranteed within a key and replays are
  reproducible.
- **Consumer groups** — offset tracking, checkpointed commits (in-memory or
  JSON file), deterministic rebalance simulation, at-least-once delivery.
- **Exactly-once semantics** — producer dedup keys plus idempotent
  processing (an idempotency store) plus a transactional outbox, so a
  redelivery never double-applies a side effect.
- **Replay** — seek to an offset or a timestamp, time-travel reads, and
  deterministic replay of a topic slice.
- **Dead-letter queue** — after N failed deliveries a record is parked with
  its original payload and full failure history; redrive re-produces it once
  the handler is fixed.
- **Backpressure + metrics** — per-topic produce/consume rates, consumer
  lag, p95 end-to-end latency, and a lag-based SLI report
  (HEALTHY / DEGRADED / UNHEALTHY).

## What it demonstrates

- Fail-closed schema validation with a reason-carrying quarantine table.
- Offset management done correctly: in-order processing per partition, a
  failed record is retried — never skipped by a later commit — until it
  succeeds or reaches the DLQ.
- Exactly-once as a composition (dedup + idempotency + outbox), not a
  checkbox.
- Deterministic replay as an incident-response tool.
- SLI/SLO thinking for streaming: lag, latency, DLQ rate, quarantine rate.

## Quickstart

```bash
make install   # create .venv and install dependencies
make test      # run the test suite
make demo      # 1,000-event end-to-end demo (see below)
make serve     # FastAPI service on :8000
```

Or without make:

```bash
python -m venv .venv && .venv/bin/pip install -r requirements.txt
PYTHONPATH=src python -m pytest -q
PYTHONPATH=src python scripts/demo.py
PYTHONPATH=src python -m uvicorn eventstream.service:app --port 8000
```

The FastAPI service is optional: the core engine is stdlib-only, and the
service tests auto-skip when FastAPI is not installed (they run in CI).

## The demo

```bash
make demo
```

1. Produces **1,000 synthetic order events** across 4 partitions of the
   `orders` topic — 988 valid, 12 with schema violations that are
   quarantined with reasons.
2. Consumer group `order-processor` (2 members) processes the topic with a
   handler that raises for 5 poison orders; each is retried 3 times, then
   parked in the DLQ. The group drains to **lag 0**.
3. Replays the whole topic and confirms the replayed record set matches the
   produced set exactly.
4. Prints the lag / SLI report: lag 0, p95 latency well under the 500 ms
   target, DLQ rate 0.51% (under the 1% threshold) — **SLI HEALTHY**.

## CLI

```bash
PYTHONPATH=src python -m eventstream.cli demo
PYTHONPATH=src python -m eventstream.cli produce --key ORD-000001 \
  --value '{"order_id":"ORD-000001","customer_id":"CUST-1234","total_amount":99.99,"currency":"USD","items_count":2,"status":"placed","created_at":"2026-11-16T09:00:00Z"}'
PYTHONPATH=src python -m eventstream.cli consume --topic orders --group cli-group --limit 5
PYTHONPATH=src python -m eventstream.cli topics
PYTHONPATH=src python -m eventstream.cli replay --topic orders --limit 5
```

## HTTP API (FastAPI)

| Endpoint | Description |
|---|---|
| `GET /health` | Liveness + topic count |
| `POST /topics` / `GET /topics` | Create / list topics |
| `POST /topics/{topic}/produce` | Produce a record (quarantines on schema violation) |
| `GET /topics/{topic}/records` | Fetch by partition + offset |
| `GET /topics/{topic}/quarantine` | Quarantined records with reasons |
| `POST /groups/{name}/join` / `/subscribe` / `/poll` / `/commit` | Consumer-group lifecycle |
| `GET /groups/{name}/lag` | Current consumer lag |
| `POST /replay` | Deterministic topic-slice replay |
| `GET /metrics` / `GET /sli` | Per-topic metrics and SLI status |
| `GET /dlq/{topic}` | Dead-letter queue entries |

## Project layout

```
src/eventstream/
  broker.py        topic registry, partition logs, retention, compaction
  schemas.py       versioned schemas + fail-closed validation
  partitioning.py  stable key hashing + round-robin assignment
  consumer.py      groups, offsets, checkpoints, rebalance, run loop
  exactly_once.py  idempotency store + transactional outbox
  dlq.py           dead-letter queue + redrive
  replay.py        seek-to-timestamp + deterministic replay
  metrics.py       rates, latency percentiles, SLI evaluation
  demo_data.py     deterministic synthetic order generator
  demo.py          the 1,000-event end-to-end demo
  service.py       optional FastAPI surface
  cli.py           command-line interface
tests/             pytest suite (stdlib core + auto-skipping service tests)
docs/architecture.md
```

## Design decisions and trade-offs

- **In-process engine, not a real broker.** The point is the semantics:
  offsets, ordering, dedup, DLQ, replay. A network broker would add
  deployment complexity without changing any of these decisions; the
  transport is the replaceable layer (see `docs/architecture.md`).
- **At-least-once delivery + idempotent processing = effectively-once.**
  True exactly-once delivery is impossible in a distributed system; the
  platform is honest about that and composes dedup, idempotency, and an
  outbox instead of claiming a guarantee it cannot keep.
- **Offsets are stable and append-only.** Retention evicts old records and
  compaction drops superseded ones, but surviving records keep their
  offsets, so consumers never see a rewritten log.
- **Fail-closed everywhere.** Unknown schema, unknown topic, and invalid
  records all fail with a reason instead of a silent default.
- **A poison record blocks its partition, not the group.** Processing stops
  at the first failure within a partition (ordering preserved), other
  partitions keep flowing, and the DLQ bounds the stall to
  `max_delivery_attempts` passes.
- **Deterministic by default.** Seeded synthetic data, a stable key hash,
  and an injectable clock make every run and every test reproducible.
- **No persistence beyond optional JSON checkpoints.** The broker is
  in-memory; durability is a documented extension (WAL + snapshots), not a
  hidden half-feature. Checkpoint files cover the one piece of state a
  restart must not lose: committed offsets.

## Configuration

Copy `.env.example` to `.env` and adjust. All settings are also readable
as `ESP_*` environment variables (see `src/eventstream/config.py`):

| Variable | Default | Meaning |
|---|---|---|
| `ESP_DEFAULT_PARTITIONS` | 4 | Partitions for new topics |
| `ESP_DEFAULT_RETENTION_MS` | 86400000 (24h) | Time-based retention |
| `ESP_MAX_RECORDS_PER_PARTITION` | 100000 | Size-based retention |
| `ESP_MAX_DELIVERY_ATTEMPTS` | 3 | Failures before DLQ |
| `ESP_LAG_ALERT_THRESHOLD` | 100 | Lag that pages |
| `ESP_P95_LATENCY_TARGET_MS` | 500 | p95 latency SLO |
| `ESP_DLQ_RATE_THRESHOLD` | 0.01 | DLQ rate that degrades SLI |
| `ESP_QUARANTINE_RATE_THRESHOLD` | 0.05 | Quarantine rate that degrades SLI |
| `ESP_CHECKPOINT_PATH` | — | JSON file for offset checkpoints |
| `ESP_PORT` | 8000 | Service port |

## Tests

```bash
make test
```

Covers schema validation (including the bool-is-not-an-int trap),
partitioning determinism and spread, broker retention/compaction/dedup,
consumer offsets, rebalance, checkpoints, retry→DLQ, exactly-once
skips, redrive, replay determinism, metrics/SLI thresholds, the CLI, and
the HTTP service (auto-skips without FastAPI; runs in CI).
