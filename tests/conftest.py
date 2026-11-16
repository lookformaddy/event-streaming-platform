import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import pytest

from eventstream.broker import Broker, ManualClock
from eventstream.config import Settings
from eventstream.consumer import ConsumerGroup
from eventstream.demo_data import ORDER_SCHEMA
from eventstream.models import TopicConfig
from eventstream.schemas import SchemaRegistry


@pytest.fixture()
def settings():
    return Settings()


@pytest.fixture()
def clock():
    return ManualClock(now_ms=1_000_000)


@pytest.fixture()
def registry():
    registry = SchemaRegistry()
    registry.register(ORDER_SCHEMA)
    return registry


@pytest.fixture()
def broker(settings, registry, clock):
    broker = Broker(settings=settings, schemas=registry, clock=clock)
    broker.create_topic(
        TopicConfig(name="orders", partitions=4, schema_name="order-event", schema_version=1)
    )
    return broker


@pytest.fixture()
def group(broker):
    group = ConsumerGroup("test-group", broker)
    group.join("member-1")
    group.subscribe(["orders"])
    return group


@pytest.fixture()
def valid_order():
    return {
        "order_id": "ORD-000001",
        "customer_id": "CUST-1234",
        "total_amount": 99.99,
        "currency": "USD",
        "items_count": 2,
        "status": "placed",
        "created_at": "2026-11-16T09:00:00Z",
    }


@pytest.fixture()
def valid_orders():
    return [
        {
            "order_id": f"ORD-{i:06d}",
            "customer_id": f"CUST-{1000 + i}",
            "total_amount": 10.0 * (i + 1),
            "currency": "USD",
            "items_count": i + 1,
            "status": "placed",
            "created_at": "2026-11-16T09:00:00Z",
        }
        for i in range(5)
    ]
