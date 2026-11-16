import json

import pytest

from eventstream.cli import main


def test_demo_exits_zero(capsys):
    assert main(["demo"]) == 0
    out = capsys.readouterr().out
    assert "SLI status:        HEALTHY" in out
    assert "produced:          988 valid events" in out
    assert "quarantined:       12 schema violations" in out
    assert "dead-letter queue: 5 records" in out


def test_produce_valid_record(capsys):
    value = json.dumps({
        "order_id": "ORD-000001", "customer_id": "CUST-1234", "total_amount": 99.99,
        "currency": "USD", "items_count": 2, "status": "placed",
        "created_at": "2026-11-16T09:00:00Z",
    })
    assert main(["produce", "--key", "ORD-000001", "--value", value]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["ok"] is True
    assert out["offset"] == 0


def test_produce_invalid_record_is_quarantined(capsys):
    value = json.dumps({
        "order_id": "ORD-000001", "customer_id": "CUST-1234", "total_amount": -1.0,
        "currency": "USD", "items_count": 2, "status": "placed",
        "created_at": "2026-11-16T09:00:00Z",
    })
    assert main(["produce", "--key", "ORD-000001", "--value", value]) == 1
    out = json.loads(capsys.readouterr().out)
    assert out["quarantined"] is True


def test_produce_bad_json_exits_2(capsys):
    assert main(["produce", "--value", "{not json"]) == 2


def test_topics_lists_seeded_topic(capsys):
    assert main(["topics"]) == 0
    assert "orders" in capsys.readouterr().out


def test_consume_and_replay_on_empty_topic(capsys):
    assert main(["consume", "--topic", "orders", "--limit", "5"]) == 0
    err = capsys.readouterr().err
    assert "polled 0 record(s)" in err
    assert main(["replay", "--topic", "orders"]) == 0


def test_unknown_subcommand_exits_nonzero():
    with pytest.raises(SystemExit) as excinfo:
        main(["nope"])
    assert excinfo.value.code == 2
