"""Deterministic synthetic order events for the demo and tests.

All data is invented: generic e-commerce orders with no real customers,
products, or company data. A seeded ``random.Random`` makes every run
reproducible. A controlled share of events is invalid (schema violations)
and a controlled share is "poison" (a handler that raises for them), so the
demo exercises quarantine, retries, and the DLQ on purpose.
"""

from __future__ import annotations

import random

from eventstream.models import FieldDef, SchemaVersion

ORDER_SCHEMA = SchemaVersion(
    name="order-event",
    version=1,
    fields=(
        FieldDef("order_id", "string", required=True, pattern=r"ORD-\d{6}"),
        FieldDef("customer_id", "string", required=True, min_length=3, max_length=32),
        FieldDef("total_amount", "number", required=True, minimum=0.01, maximum=100_000),
        FieldDef("currency", "enum", required=True, enum=("USD", "EUR", "GBP")),
        FieldDef("items_count", "integer", required=True, minimum=1, maximum=100),
        FieldDef("status", "enum", required=True, enum=("placed", "paid", "shipped", "cancelled")),
        FieldDef("created_at", "string", required=True),
    ),
)

_CURRENCIES = ("USD", "EUR", "GBP")
_STATUSES = ("placed", "paid", "shipped", "cancelled")

# Defects injected into invalid events, one per event, round-robin.
_DEFECTS = (
    "missing_total",
    "negative_total",
    "wrong_type_items",
    "bad_currency",
    "unknown_field",
    "missing_order_id",
)


def _valid_order(rng: random.Random, index: int) -> dict:
    return {
        "order_id": f"ORD-{index:06d}",
        "customer_id": f"CUST-{rng.randint(1000, 9999)}",
        "total_amount": round(rng.uniform(5.0, 2500.0), 2),
        "currency": rng.choice(_CURRENCIES),
        "items_count": rng.randint(1, 12),
        "status": rng.choice(_STATUSES),
        "created_at": "2026-11-16T09:00:00Z",
    }


def _invalid_order(rng: random.Random, index: int, defect: str) -> dict:
    order = _valid_order(rng, index)
    if defect == "missing_total":
        del order["total_amount"]
    elif defect == "negative_total":
        order["total_amount"] = -42.0
    elif defect == "wrong_type_items":
        order["items_count"] = "three"
    elif defect == "bad_currency":
        order["currency"] = "BTC"
    elif defect == "unknown_field":
        order["internal_cost_cents"] = 1234
    elif defect == "missing_order_id":
        del order["order_id"]
    return order


def generate_order_events(
    count: int = 1000,
    seed: int = 42,
    invalid_count: int = 12,
    poison_count: int = 5,
) -> tuple[list[dict], set[str]]:
    """Generate ``count`` order events: ``invalid_count`` of them invalid.

    Returns ``(events, poison_order_ids)``. Poison ids are valid orders whose
    handler is designed to fail, so they exercise retries and the DLQ.
    Events are shuffled deterministically so invalid/poison records land in
    arbitrary partitions.
    """
    if invalid_count < 0 or poison_count < 0 or invalid_count + poison_count > count:
        raise ValueError("invalid_count + poison_count must be <= count")
    rng = random.Random(seed)
    valid_count = count - invalid_count
    events = [_valid_order(rng, i) for i in range(valid_count)]
    poison_ids = {events[i]["order_id"] for i in rng.sample(range(valid_count), poison_count)}
    for j in range(invalid_count):
        events.append(_invalid_order(rng, valid_count + j, _DEFECTS[j % len(_DEFECTS)]))
    rng.shuffle(events)
    return events, poison_ids
