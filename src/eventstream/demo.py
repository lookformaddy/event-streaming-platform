"""End-to-end demo: produce 1,000 synthetic orders, consume, replay, report.

Runs the full pipeline on the in-process broker and returns a summary dict:

1. 1,000 synthetic order events produced across 4 partitions; 12 invalid
   events are quarantined with reasons, never silently dropped.
2. A consumer group processes with a handler that raises for 5 poison
   orders; after 3 attempts each lands in the DLQ and the group drains to
   zero lag.
3. A replay of the whole topic reproduces the exact same record set.
4. Lag / SLI report: lag 0, p95 latency under target, DLQ rate under
   threshold — status HEALTHY.
"""

from __future__ import annotations

from eventstream.broker import Broker
from eventstream.config import Settings
from eventstream.consumer import ConsumerGroup, run_consumer
from eventstream.demo_data import ORDER_SCHEMA, generate_order_events
from eventstream.dlq import DeadLetterQueue
from eventstream.metrics import Metrics, SliEvaluator
from eventstream.models import TopicConfig
from eventstream.replay import replay


def run_demo(
    count: int = 1000,
    seed: int = 42,
    invalid_count: int = 12,
    poison_count: int = 5,
    verbose: bool = True,
) -> dict:
    settings = Settings()
    broker = Broker(settings=settings)
    broker.schemas.register(ORDER_SCHEMA)
    broker.create_topic(
        TopicConfig(
            name="orders",
            partitions=4,
            schema_name="order-event",
            schema_version=1,
        )
    )
    metrics = Metrics()
    dlq = DeadLetterQueue()

    events, poison_ids = generate_order_events(
        count=count, seed=seed, invalid_count=invalid_count, poison_count=poison_count
    )

    produced = quarantined = duplicates = 0
    for event in events:
        outcome = broker.produce(
            "orders",
            key=event.get("order_id"),
            value=event,
            producer_id="order-service",
            dedup_key=f"order:{event.get('order_id', 'unknown')}",
        )
        if outcome.ok:
            produced += 1
            metrics.record_produce("orders")
        elif outcome.quarantined:
            quarantined += 1
        elif outcome.duplicate:
            duplicates += 1

    group = ConsumerGroup("order-processor", broker, settings=settings)
    group.join("order-processor-1")
    group.join("order-processor-2")
    group.subscribe(["orders"])

    processed_order_ids: list[str] = []

    def handler(record):
        order_id = record.value["order_id"]
        if order_id in poison_ids:
            raise RuntimeError(f"downstream fulfilment failed for {order_id}")
        processed_order_ids.append(order_id)
        return {"order_id": order_id, "status": "processed"}

    summary = run_consumer(
        broker, group, handler, dlq=dlq, metrics=metrics, max_passes=50, batch_size=100
    )

    # Replay: re-read the whole topic and confirm it reproduces the exact
    # same produced set (processed records plus the poison ones now in the
    # DLQ — replay re-reads what was produced, not what was processed).
    replayed = replay(broker, "orders")
    replayed_ids = sorted(r.value["order_id"] for r in replayed)
    expected_ids = sorted(processed_order_ids + sorted(poison_ids))
    replay_matches = replayed_ids == expected_ids
    replay_count = len(replayed)

    lag = group.lag("orders")
    p95 = metrics.p95_latency("orders")
    consumed = summary.processed + summary.dlq
    dlq_rate = (summary.dlq / consumed) if consumed else 0.0
    quarantine_rate = (quarantined / count) if count else 0.0
    sli = SliEvaluator(settings).evaluate(
        lag=lag, p95_latency_ms=p95, dlq_rate=dlq_rate, quarantine_rate=quarantine_rate
    )

    result = {
        "events_attempted": count,
        "produced": produced,
        "quarantined": quarantined,
        "duplicates": duplicates,
        "partitions": broker.topic("orders").partitions,
        "end_offsets": broker.end_offsets("orders"),
        "processed": summary.processed,
        "skipped_duplicates": summary.skipped_duplicates,
        "failed_attempts": summary.failed_attempts,
        "dlq_count": summary.dlq,
        "dlq_rate": dlq_rate,
        "quarantine_rate": quarantine_rate,
        "replay_count": replay_count,
        "replay_matches_processed": replay_matches,
        "final_lag": lag,
        "p95_latency_ms": p95,
        "sli_status": sli.status,
        "sli_alerts": list(sli.alerts),
        "passes": summary.passes,
    }

    if verbose:
        print("== event-streaming-platform demo ==")
        print(f"produced:          {produced} valid events across 4 partitions")
        print(f"quarantined:       {quarantined} schema violations (payload + reason kept)")
        print(f"processed:         {summary.processed} events by group 'order-processor'")
        print(f"failed attempts:   {summary.failed_attempts} (poison orders retried 3x each)")
        print(f"dead-letter queue: {summary.dlq} records (DLQ rate {dlq_rate:.2%})")
        print(f"replay:            {replay_count} records re-read; matches processed output: {replay_matches}")
        print(f"final lag:         {lag}")
        print(f"p95 latency:       {p95:.2f} ms")
        print(f"SLI status:        {sli.status}")
        for alert in sli.alerts:
            print(f"  alert: {alert}")
    return result
