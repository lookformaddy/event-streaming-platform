import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")

from fastapi.testclient import TestClient

from eventstream.broker import Broker
from eventstream.config import Settings
from eventstream.demo_data import ORDER_SCHEMA
from eventstream.models import TopicConfig
from eventstream.service import create_app


@pytest.fixture()
def client():
    broker = Broker(settings=Settings())
    broker.schemas.register(ORDER_SCHEMA)
    broker.create_topic(
        TopicConfig(name="orders", partitions=4, schema_name="order-event", schema_version=1)
    )
    return TestClient(create_app(broker))


def _valid_order(order_id="ORD-000001"):
    return {
        "order_id": order_id, "customer_id": "CUST-1234", "total_amount": 99.99,
        "currency": "USD", "items_count": 2, "status": "placed",
        "created_at": "2026-11-16T09:00:00Z",
    }


def test_health_and_topic_listing(client):
    assert client.get("/health").json()["status"] == "ok"
    names = [t["name"] for t in client.get("/topics").json()["topics"]]
    assert names == ["orders"]


def test_create_topic_and_duplicate_conflict(client):
    response = client.post("/topics", json={"name": "events", "partitions": 2})
    assert response.status_code == 201
    assert response.json()["partitions"] == 2
    assert client.post("/topics", json={"name": "events"}).status_code == 409


def test_produce_valid_and_quarantine_invalid(client):
    ok = client.post("/topics/orders/produce", json={"key": "ORD-000001", "value": _valid_order()})
    assert ok.status_code == 201
    assert ok.json()["ok"] is True
    assert ok.json()["offset"] == 0

    bad = _valid_order("ORD-000002")
    bad["currency"] = "BTC"
    quarantined = client.post("/topics/orders/produce", json={"key": "ORD-000002", "value": bad})
    assert quarantined.json()["quarantined"] is True
    assert "currency" in quarantined.json()["violations"][0]

    items = client.get("/topics/orders/quarantine").json()["quarantined"]
    assert len(items) == 1


def test_produce_to_unknown_topic_404(client):
    assert client.post("/topics/nope/produce", json={"value": {}}).status_code == 404


def test_records_and_replay_endpoints(client):
    from eventstream.partitioning import partition_for_key

    client.post("/topics/orders/produce", json={"key": "ORD-000001", "value": _valid_order()})
    partition = partition_for_key("ORD-000001", 4)
    records = client.get(f"/topics/orders/records?partition={partition}").json()["records"]
    assert len(records) == 1
    assert records[0]["value"]["order_id"] == "ORD-000001"
    replayed = client.post("/replay", json={"topic": "orders"}).json()["records"]
    assert len(replayed) == 1


def test_group_join_subscribe_poll_commit_lag(client):
    client.post("/topics/orders/produce", json={"key": "ORD-000001", "value": _valid_order()})
    assert client.post("/groups/g1/join", json={"member_id": "m1"}).status_code == 200
    client.post("/groups/g1/subscribe", json={"topics": ["orders"]})
    polled = client.post("/groups/g1/poll", json={"member_id": "m1"}).json()["records"]
    assert len(polled) == 1
    assert client.get("/groups/g1/lag").json()["lag"] == 1
    record = polled[0]
    client.post("/groups/g1/commit", json={
        "topic": "orders", "partition": record["partition"], "offset": record["offset"] + 1,
    })
    assert client.get("/groups/g1/lag").json()["lag"] == 0
    assert client.post("/groups/g1/poll", json={"member_id": "m1"}).json()["records"] == []


def test_unknown_group_404(client):
    assert client.get("/groups/nope/lag").status_code == 404


def test_metrics_and_sli_endpoints(client):
    client.post("/topics/orders/produce", json={"key": "ORD-000001", "value": _valid_order()})
    metrics = client.get("/metrics").json()
    assert metrics["orders"]["produced"] == 1
    assert client.get("/sli").json()["status"] in ("HEALTHY", "DEGRADED")


def test_dlq_endpoint_empty(client):
    assert client.get("/dlq/orders").json()["entries"] == []
