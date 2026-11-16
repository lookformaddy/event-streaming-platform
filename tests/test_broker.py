import pytest

from eventstream.errors import TopicExistsError, TopicNotFoundError
from eventstream.models import TopicConfig
from eventstream.partitioning import partition_for_key


def test_create_and_list_topics(broker):
    assert [t.name for t in broker.list_topics()] == ["orders"]
    assert broker.topic("orders").partitions == 4


def test_duplicate_topic_rejected(broker):
    with pytest.raises(TopicExistsError):
        broker.create_topic(TopicConfig(name="orders", partitions=2))


def test_unknown_topic_errors(broker):
    with pytest.raises(TopicNotFoundError):
        broker.produce("nope", "k", {})
    with pytest.raises(TopicNotFoundError):
        broker.fetch("nope", 0, 0)


def test_produce_assigns_deterministic_partition_and_offsets(broker, valid_order):
    outcome = broker.produce("orders", "ORD-000001", valid_order)
    assert outcome.ok and outcome.record is not None
    assert outcome.record.partition == partition_for_key("ORD-000001", 4)
    assert outcome.record.offset == 0
    second = broker.produce("orders", "ORD-000001", {**valid_order, "order_id": "ORD-000002"})
    assert second.record.offset == 1
    assert second.record.partition == outcome.record.partition


def test_same_key_ordering_within_partition(broker, valid_orders):
    for order in valid_orders:
        broker.produce("orders", "same-key", order)
    partition = partition_for_key("same-key", 4)
    records = broker.fetch("orders", partition, 0)
    assert [r.offset for r in records] == list(range(len(valid_orders)))
    assert [r.value["order_id"] for r in records] == [o["order_id"] for o in valid_orders]


def test_fetch_from_offset(broker, valid_orders):
    for order in valid_orders:
        broker.produce("orders", "same-key", order)
    partition = partition_for_key("same-key", 4)
    assert len(broker.fetch("orders", partition, 2)) == 3
    assert broker.fetch("orders", partition, 2)[0].offset == 2


def test_schema_violation_quarantined_not_dropped(broker, valid_order):
    bad = {**valid_order, "total_amount": -1.0}
    outcome = broker.produce("orders", "ORD-000001", bad)
    assert not outcome.ok and outcome.quarantined
    assert outcome.record is None
    assert broker.record_count("orders") == 0
    quarantined = broker.quarantine("orders")
    assert len(quarantined) == 1
    assert quarantined[0].value == bad
    assert any("total_amount" in str(v) for v in quarantined[0].violations)


def test_producer_dedup_key_is_idempotent(broker, valid_order):
    first = broker.produce("orders", "ORD-000001", valid_order, dedup_key="order:1")
    second = broker.produce("orders", "ORD-000001", valid_order, dedup_key="order:1")
    assert first.ok
    assert not second.ok and second.duplicate
    assert broker.record_count("orders") == 1


def test_size_retention_caps_partition(broker):
    broker.create_topic(TopicConfig(name="small", partitions=1, max_records_per_partition=3))
    for i in range(5):
        broker.produce("small", "k", {"n": i})
    assert broker.record_count("small") == 3
    assert broker.earliest_offset("small", 0) == 2
    assert broker.latest_offset("small", 0) == 5


def test_time_retention_evicts_old_records(broker, clock):
    broker.create_topic(TopicConfig(name="short-lived", partitions=1, retention_ms=1000))
    broker.produce("short-lived", "k", {"n": 1})
    clock.advance(2000)
    broker.produce("short-lived", "k", {"n": 2})
    assert broker.record_count("short-lived") == 1
    assert broker.fetch("short-lived", 0, 0)[0].value == {"n": 2}


def test_compaction_keeps_latest_per_key(broker):
    broker.create_topic(TopicConfig(name="kv", partitions=1, compaction=True))
    broker.produce("kv", "A", {"v": 1})
    broker.produce("kv", "B", {"v": 1})
    broker.produce("kv", "A", {"v": 2})
    removed = broker.compact("kv")
    assert removed == 1
    records = broker.fetch("kv", 0, 0)
    assert len(records) == 2
    by_key = {r.key: r for r in records}
    assert by_key["A"].value == {"v": 2}
    assert by_key["A"].offset == 2  # surviving offsets are unchanged


def test_end_offsets_and_delete(broker, valid_order):
    broker.produce("orders", "ORD-000001", valid_order)
    ends = broker.end_offsets("orders")
    assert sum(ends.values()) == 1
    broker.delete_topic("orders")
    assert broker.list_topics() == []
    with pytest.raises(TopicNotFoundError):
        broker.topic("orders")
