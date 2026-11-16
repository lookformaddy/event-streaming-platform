"""Error types for the event streaming platform."""

from __future__ import annotations

from dataclasses import dataclass


class EventStreamError(Exception):
    """Base class for all platform errors."""


class TopicExistsError(EventStreamError):
    """A topic with this name already exists."""


class TopicNotFoundError(EventStreamError):
    """No topic with this name exists."""


class SchemaError(EventStreamError):
    """A schema registration or lookup failed."""


class GroupError(EventStreamError):
    """A consumer-group operation failed."""


@dataclass(frozen=True)
class FieldViolation:
    """One reason a record failed schema validation.

    ``field`` is the dotted path of the offending field (``"<root>"`` when the
    record itself is not an object); ``reason`` is human-readable and is
    carried into the quarantine table so nothing is ever silently dropped.
    """

    field: str
    reason: str

    def __str__(self) -> str:  # pragma: no cover - trivial
        return f"{self.field}: {self.reason}"
