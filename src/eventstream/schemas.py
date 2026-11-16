"""Versioned event schemas and fail-closed record validation.

A topic binds to one schema version. Every produced record is validated
before it is appended: a violation quarantines the record (payload + reasons
kept) instead of silently dropping it or letting bad data into the log.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from eventstream.errors import FieldViolation, SchemaError
from eventstream.models import FieldDef, SchemaVersion

_MISSING = object()


def _is_integer(value: object) -> bool:
    # bool is a subclass of int — a boolean is never a valid integer field.
    return isinstance(value, int) and not isinstance(value, bool)


def _is_number(value: object) -> bool:
    return (isinstance(value, (int, float)) and not isinstance(value, bool))


@dataclass
class SchemaRegistry:
    """Registry of versioned event schemas, keyed by (name, version)."""

    _schemas: dict[tuple[str, int], SchemaVersion] = field(default_factory=dict)

    def register(self, schema: SchemaVersion) -> SchemaVersion:
        key = (schema.name, schema.version)
        if key in self._schemas:
            raise SchemaError(f"schema {schema.name} v{schema.version} already registered")
        if schema.version < 1:
            raise SchemaError("schema version must be >= 1")
        self._schemas[key] = schema
        return schema

    def get(self, name: str, version: int | None = None) -> SchemaVersion:
        if version is not None:
            schema = self._schemas.get((name, version))
            if schema is None:
                raise SchemaError(f"schema {name} v{version} not found")
            return schema
        versions = sorted(v for (n, v) in self._schemas if n == name)
        if not versions:
            raise SchemaError(f"schema {name} not found")
        return self._schemas[(name, versions[-1])]

    def latest_version(self, name: str) -> int:
        return self.get(name).version

    def validate(self, schema: SchemaVersion, value: object) -> tuple[FieldViolation, ...]:
        """Validate ``value`` against ``schema``; empty tuple means valid."""
        violations: list[FieldViolation] = []
        if not isinstance(value, dict):
            return (FieldViolation("<root>", f"record must be an object, got {type(value).__name__}"),)
        known = schema.field_names()
        for extra in sorted(set(value) - known):
            violations.append(FieldViolation(extra, "unexpected field (not in schema)"))
        for field_def in schema.fields:
            raw = value.get(field_def.name, _MISSING)
            if raw is _MISSING:
                if field_def.required:
                    violations.append(FieldViolation(field_def.name, "missing required field"))
                continue
            violations.extend(_validate_field(field_def, raw))
        return tuple(violations)


def _validate_field(field_def: FieldDef, raw: object) -> list[FieldViolation]:
    name = field_def.name
    problems: list[FieldViolation] = []
    kind = field_def.type
    if kind == "string":
        if not isinstance(raw, str):
            return [FieldViolation(name, f"expected string, got {type(raw).__name__}")]
        if field_def.min_length is not None and len(raw) < field_def.min_length:
            problems.append(FieldViolation(name, f"shorter than min_length {field_def.min_length}"))
        if field_def.max_length is not None and len(raw) > field_def.max_length:
            problems.append(FieldViolation(name, f"longer than max_length {field_def.max_length}"))
        if field_def.pattern is not None and re.fullmatch(field_def.pattern, raw) is None:
            problems.append(FieldViolation(name, f"does not match pattern {field_def.pattern!r}"))
    elif kind == "integer":
        if not _is_integer(raw):
            return [FieldViolation(name, f"expected integer, got {type(raw).__name__}")]
        _check_range(field_def, raw, problems)
    elif kind == "number":
        if not _is_number(raw):
            return [FieldViolation(name, f"expected number, got {type(raw).__name__}")]
        _check_range(field_def, raw, problems)
    elif kind == "boolean":
        if not isinstance(raw, bool):
            return [FieldViolation(name, f"expected boolean, got {type(raw).__name__}")]
    elif kind == "enum":
        if field_def.enum is None:
            return [FieldViolation(name, "enum field has no allowed values defined")]
        if raw not in field_def.enum:
            return [FieldViolation(name, f"value {raw!r} not in {list(field_def.enum)}")]
    else:
        problems.append(FieldViolation(name, f"unknown field type {kind!r}"))
    return problems


def _check_range(field_def: FieldDef, raw: float | int, problems: list[FieldViolation]) -> None:
    if field_def.minimum is not None and raw < field_def.minimum:
        problems.append(FieldViolation(field_def.name, f"value {raw} below minimum {field_def.minimum}"))
    if field_def.maximum is not None and raw > field_def.maximum:
        problems.append(FieldViolation(field_def.name, f"value {raw} above maximum {field_def.maximum}"))
