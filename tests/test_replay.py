from eventstream.replay import replay, seek_to_timestamp


def _produce_sequential(broker, clock, orders, key="same-key"):
    for order in orders:
        broker.produce("orders", key, order)
        clock.advance(1000)


def test_seek_to_timestamp(broker, clock, valid_orders):
    _produce_sequential(broker, clock, valid_orders)
    # All records share one key -> one partition; find it from the first record.
    records = []
    for p in range(4):
        records = broker.fetch("orders", p, 0)
        if records:
            break
    assert len(records) == 5
    # Records were produced at 1_000_000, 1_001_000, ... (clock advances after each)
    assert seek_to_timestamp(broker, "orders", records[0].partition, 1_002_000) == 2
    assert seek_to_timestamp(broker, "orders", records[0].partition, 1_000_000) == 0
    assert seek_to_timestamp(broker, "orders", records[0].partition, 9_999_999) == 5


def test_replay_full_topic_is_deterministic(broker, clock, valid_orders):
    _produce_sequential(broker, clock, valid_orders)
    first = replay(broker, "orders")
    second = replay(broker, "orders")
    assert len(first) == 5
    assert [(r.partition, r.offset) for r in first] == [(r.partition, r.offset) for r in second]
    assert [r.value["order_id"] for r in first] == [o["order_id"] for o in valid_orders]


def test_replay_from_offset(broker, clock, valid_orders):
    _produce_sequential(broker, clock, valid_orders)
    replayed = replay(broker, "orders", from_offset=3)
    assert len(replayed) == 2
    assert [r.offset for r in replayed] == [3, 4]


def test_replay_time_window(broker, clock, valid_orders):
    _produce_sequential(broker, clock, valid_orders)
    window = replay(broker, "orders", from_timestamp_ms=1_001_000, to_timestamp_ms=1_002_000)
    assert [r.value["order_id"] for r in window] == ["ORD-000001", "ORD-000002"]


def test_replay_single_partition(broker, clock, valid_orders):
    _produce_sequential(broker, clock, valid_orders)
    partition = next(
        (p for p in range(4) if broker.fetch("orders", p, 0)), 0
    )
    replayed = replay(broker, "orders", partitions=[partition])
    assert len(replayed) == 5
    assert all(r.partition == partition for r in replayed)
