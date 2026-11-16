import pytest

from eventstream.demo_data import ORDER_SCHEMA
from eventstream.errors import SchemaError
from eventstream.models import FieldDef, SchemaVersion
from eventstream.schemas import SchemaRegistry


def test_valid_order_passes(registry, valid_order):
    assert registry.validate(ORDER_SCHEMA, valid_order) == ()


def test_missing_required_field(registry, valid_order):
    del valid_order["total_amount"]
    violations = registry.validate(ORDER_SCHEMA, valid_order)
    assert len(violations) == 1
    assert violations[0].field == "total_amount"
    assert "missing required" in violations[0].reason


def test_boolean_is_not_an_integer(registry, valid_order):
    valid_order["items_count"] = True
    violations = registry.validate(ORDER_SCHEMA, valid_order)
    assert any(v.field == "items_count" for v in violations)


def test_out_of_range_number(registry, valid_order):
    valid_order["total_amount"] = -5.0
    violations = registry.validate(ORDER_SCHEMA, valid_order)
    assert any("below minimum" in v.reason for v in violations)


def test_bad_enum_value(registry, valid_order):
    valid_order["currency"] = "BTC"
    violations = registry.validate(ORDER_SCHEMA, valid_order)
    assert any(v.field == "currency" for v in violations)


def test_unexpected_field_flagged(registry, valid_order):
    valid_order["internal_cost"] = 123
    violations = registry.validate(ORDER_SCHEMA, valid_order)
    assert any(v.field == "internal_cost" for v in violations)


def test_pattern_mismatch(registry, valid_order):
    valid_order["order_id"] = "not-an-order-id"
    violations = registry.validate(ORDER_SCHEMA, valid_order)
    assert any(v.field == "order_id" for v in violations)


def test_non_object_record(registry):
    violations = registry.validate(ORDER_SCHEMA, ["not", "a", "dict"])
    assert len(violations) == 1
    assert violations[0].field == "<root>"


def test_multiple_violations_reported_together(registry, valid_order):
    del valid_order["total_amount"]
    valid_order["currency"] = "XXX"
    violations = registry.validate(ORDER_SCHEMA, valid_order)
    assert len(violations) == 2


def test_versioning_latest_and_explicit(registry):
    v2 = SchemaVersion(
        name="order-event",
        version=2,
        fields=ORDER_SCHEMA.fields + (FieldDef("discount_code", "string", required=False),),
    )
    registry.register(v2)
    assert registry.latest_version("order-event") == 2
    assert registry.get("order-event", 1).version == 1
    assert registry.get("order-event").version == 2


def test_duplicate_registration_rejected(registry):
    with pytest.raises(SchemaError):
        registry.register(ORDER_SCHEMA)


def test_unknown_schema_lookup(registry):
    with pytest.raises(SchemaError):
        registry.get("no-such-schema")
