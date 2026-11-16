from eventstream.dlq import DeadLetterQueue
from eventstream.models import Record, TopicConfig


def _record(order_id="ORD-000001", offset=0):
    return Record(
        topic="orders", partition=0, offset=offset, key=order_id,
        value={
            "order_id": order_id, "customer_id": "CUST-1234", "total_amount": 99.99,
            "currency": "USD", "items_count": 2, "status": "placed",
            "created_at": "2026-11-16T09:00:00Z",
        },
        timestamp_ms=1000,
    )


def test_add_and_failure_history_accumulates():
    dlq = DeadLetterQueue()
    record = _record()
    entry = dlq.add(record, "first failure", attempt=1, timestamp_ms=1000)
    dlq.add(record, "second failure", attempt=2, timestamp_ms=2000)
    assert dlq.count("orders") == 1
    assert len(entry.failures) == 2
    assert entry.failures[0].error == "first failure"
    assert entry.failures[1].attempt == 2
    assert entry.first_failed_ms == 1000
    assert entry.last_failed_ms == 2000


def test_entries_filter_by_topic():
    dlq = DeadLetterQueue()
    dlq.add(_record(), "err", 1, 1000)
    dlq.add(_record("ORD-000002", offset=1), "err", 1, 1000)
    assert dlq.count() == 2
    assert dlq.count("orders") == 2
    assert dlq.count("other") == 0


def test_redrive_reproduces_and_removes(broker):
    dlq = DeadLetterQueue()
    record = _record()
    dlq.add(record, "boom", attempt=3, timestamp_ms=1000)
    outcomes = dlq.redrive(broker, "orders")
    assert len(outcomes) == 1 and outcomes[0].ok
    assert dlq.count("orders") == 0
    assert broker.record_count("orders") == 1
    redriven = next(r for p in range(4) for r in broker.fetch("orders", p, 0))
    assert redriven.value["order_id"] == "ORD-000001"


def test_redrive_of_invalid_record_stays_in_dlq(broker):
    dlq = DeadLetterQueue()
    bad = Record(
        topic="orders", partition=0, offset=0, key="bad",
        value={"total_amount": -1}, timestamp_ms=1000,
    )
    dlq.add(bad, "boom", attempt=3, timestamp_ms=1000)
    outcomes = dlq.redrive(broker, "orders")
    assert not outcomes[0].ok
    assert dlq.count("orders") == 1  # still parked: it cannot be produced


def test_purge():
    dlq = DeadLetterQueue()
    dlq.add(_record(), "err", 1, 1000)
    dlq.add(_record("ORD-000002", offset=1), "err", 1, 1000)
    assert dlq.purge("orders") == 2
    assert dlq.count() == 0


def test_redrive_uses_fresh_dedup_key(broker):
    # The original record was produced with a dedup key; redrive must not be
    # treated as a duplicate produce.
    broker.produce("orders", "ORD-000001", {
        "order_id": "ORD-000001", "customer_id": "CUST-1", "total_amount": 10.0,
        "currency": "USD", "items_count": 1, "status": "placed",
        "created_at": "2026-11-16T09:00:00Z",
    }, dedup_key="order:ORD-000001")
    dlq = DeadLetterQueue()
    original = next(r for p in range(4) for r in broker.fetch("orders", p, 0))
    dlq.add(original, "boom", attempt=3, timestamp_ms=1000)
    outcomes = dlq.redrive(broker, "orders")
    assert outcomes[0].ok
    assert broker.record_count("orders") == 2
