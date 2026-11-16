import pytest

from eventstream.consumer import ConsumerGroup, run_consumer
from eventstream.dlq import DeadLetterQueue
from eventstream.errors import GroupError
from eventstream.exactly_once import ExactlyOnceProcessor
from eventstream.metrics import Metrics


def _produce(broker, orders, key="same-key"):
    for order in orders:
        broker.produce("orders", key, order, dedup_key=f"order:{order['order_id']}")


def test_poll_commit_and_lag(broker, group, valid_orders):
    _produce(broker, valid_orders)
    assert group.lag("orders") == 5
    records = group.poll("member-1")
    assert len(records) == 5
    assert [r.value["order_id"] for r in records] == [o["order_id"] for o in valid_orders]
    for record in records:
        group.commit(record.topic, record.partition, record.offset + 1)
    assert group.lag("orders") == 0
    assert group.poll("member-1") == []


def test_seek_resets_read_position(broker, group, valid_orders):
    _produce(broker, valid_orders)
    records = group.poll("member-1")
    for record in records:
        group.commit(record.topic, record.partition, record.offset + 1)
    assert group.lag("orders") == 0
    group.seek("orders", records[0].partition, 0)
    assert group.lag("orders") == 5
    assert len(group.poll("member-1")) == 5


def test_rebalance_splits_partitions(broker, group):
    assert group.assignment("member-1")["orders"] == [0, 1, 2, 3]
    group.join("member-2")
    assert group.assignment("member-1")["orders"] == [0, 2]
    assert group.assignment("member-2")["orders"] == [1, 3]
    assert group.rebalance_count >= 2
    group.leave("member-2")
    assert group.assignment("member-1")["orders"] == [0, 1, 2, 3]


def test_unknown_member_errors(broker, group):
    with pytest.raises(GroupError):
        group.poll("ghost")
    with pytest.raises(GroupError):
        group.leave("ghost")


def test_checkpoint_save_and_load_round_trip(broker, group, valid_orders, tmp_path):
    path = tmp_path / "checkpoints.json"
    group2 = ConsumerGroup("ckpt-group", broker, checkpoint_path=path)
    group2.join("member-1")
    group2.subscribe(["orders"])
    _produce(broker, valid_orders)
    records = group2.poll("member-1")
    for record in records:
        group2.commit(record.topic, record.partition, record.offset + 1)
    assert path.exists()
    restored = ConsumerGroup("ckpt-group", broker, checkpoint_path=path)
    restored.join("member-1")
    restored.subscribe(["orders"])
    assert restored.lag("orders") == 0


def test_run_consumer_processes_all_and_drains_lag(broker, group, valid_orders):
    _produce(broker, valid_orders)
    seen: list[str] = []

    def handler(record):
        seen.append(record.value["order_id"])

    summary = run_consumer(broker, group, handler)
    assert summary.processed == 5
    assert summary.dlq == 0
    assert summary.final_lag == 0
    assert sorted(seen) == sorted(o["order_id"] for o in valid_orders)


def test_run_consumer_retries_then_dlq(broker, group, valid_orders):
    _produce(broker, valid_orders)
    dlq = DeadLetterQueue()
    poison = valid_orders[1]["order_id"]

    def handler(record):
        if record.value["order_id"] == poison:
            raise RuntimeError("boom")

    summary = run_consumer(broker, group, handler, dlq=dlq, max_passes=10)
    assert summary.processed == 4
    assert summary.dlq == 1
    assert summary.failed_attempts == 3  # max_delivery_attempts
    assert summary.final_lag == 0
    assert dlq.count("orders") == 1
    entry = dlq.entries("orders")[0]
    assert entry.value["order_id"] == poison
    assert len(entry.failures) == 3
    assert all(f.error == "boom" for f in entry.failures)


def test_run_consumer_skips_already_processed(broker, group, valid_orders):
    _produce(broker, valid_orders)
    processor = ExactlyOnceProcessor()
    processor.mark_processed("order:ORD-000000", {"status": "processed"})
    calls: list[str] = []

    def handler(record):
        calls.append(record.value["order_id"])

    summary = run_consumer(broker, group, handler, processor=processor)
    assert summary.processed == 4
    assert summary.skipped_duplicates == 1
    assert "ORD-000000" not in calls


def test_run_consumer_without_members_joins_default(broker, valid_orders):
    _produce(broker, valid_orders)
    group = ConsumerGroup("lonely", broker)
    group.subscribe(["orders"])
    summary = run_consumer(broker, group, lambda r: None)
    assert summary.processed == 5
    assert group.members() == ["default-member"]


def test_metrics_lag_helper(broker, group, valid_orders):
    _produce(broker, valid_orders)
    metrics = Metrics()
    assert metrics.consumer_lag(broker, group, "orders") == 5
