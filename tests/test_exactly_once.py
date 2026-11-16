from eventstream.exactly_once import (
    ExactlyOnceProcessor,
    IdempotencyStore,
    OutboxEntry,
    TransactionalOutbox,
)
from eventstream.models import Record


def _record(order_id="ORD-000001"):
    return Record(
        topic="orders", partition=0, offset=0, key=order_id,
        value={"order_id": order_id}, timestamp_ms=1000,
        dedup_key=f"order:{order_id}",
    )


def test_idempotency_store_round_trip():
    store = IdempotencyStore()
    assert not store.is_processed("k1")
    store.mark_processed("k1", {"ok": True})
    assert store.is_processed("k1")
    assert store.result("k1") == {"ok": True}
    assert store.size() == 1


def test_processor_runs_handler_once():
    processor = ExactlyOnceProcessor()
    calls: list[str] = []

    def handler(record):
        calls.append(record.value["order_id"])
        return "done"

    record = _record()
    executed, result = processor.process(record, handler)
    assert executed is True and result == "done"
    executed, result = processor.process(record, handler)
    assert executed is False and result == "done"
    assert calls == ["ORD-000001"]


def test_processor_propagates_handler_errors_for_retry():
    processor = ExactlyOnceProcessor()
    record = _record()

    def handler(_record):
        raise RuntimeError("transient")

    try:
        processor.process(record, handler)
    except RuntimeError:
        pass
    else:  # pragma: no cover
        raise AssertionError("expected the handler error to propagate")
    assert not processor.is_processed(f"order:{record.value['order_id']}")


def test_outbox_commit_and_discard():
    outbox = TransactionalOutbox()
    outbox.stage(OutboxEntry("e1", {"row": 1}, created_ms=100))
    outbox.stage(OutboxEntry("e2", {"row": 2}, created_ms=101))
    assert len(outbox.pending()) == 2
    committed = outbox.commit_all()
    assert [e.entry_id for e in committed] == ["e1", "e2"]
    assert outbox.pending() == []

    outbox.stage(OutboxEntry("e3", {"row": 3}, created_ms=102))
    discarded = outbox.discard_pending()
    assert [e.entry_id for e in discarded] == ["e3"]
    assert outbox.entries[-1].status == "discarded"
