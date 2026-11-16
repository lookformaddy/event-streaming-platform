"""eventstream — a schema-validated event streaming platform.

A deterministic, in-process streaming engine (Kafka-style) that runs anywhere
with no external services: topics, partitions, consumer groups, exactly-once
semantics, replay, and dead-letter queues. All data is synthetic; all code is
original.
"""

from eventstream.broker import Broker
from eventstream.config import Settings
from eventstream.consumer import ConsumerGroup, ConsumerRunSummary, DeliveryTracker, run_consumer
from eventstream.dlq import DeadLetterQueue, DlqEntry, FailureEntry
from eventstream.exactly_once import ExactlyOnceProcessor, IdempotencyStore, TransactionalOutbox
from eventstream.metrics import Metrics, SliEvaluator
from eventstream.models import (
    FieldDef,
    ProduceOutcome,
    QuarantinedRecord,
    Record,
    SchemaVersion,
    SliReport,
    TopicConfig,
)
from eventstream.partitioning import assign_partitions, partition_for_key
from eventstream.replay import replay, seek_to_timestamp
from eventstream.schemas import SchemaRegistry

__version__ = "1.0.0"

__all__ = [
    "Broker",
    "ConsumerGroup",
    "ConsumerRunSummary",
    "DeadLetterQueue",
    "DeliveryTracker",
    "DlqEntry",
    "ExactlyOnceProcessor",
    "FailureEntry",
    "FieldDef",
    "IdempotencyStore",
    "Metrics",
    "ProduceOutcome",
    "QuarantinedRecord",
    "Record",
    "SchemaRegistry",
    "SchemaVersion",
    "Settings",
    "SliEvaluator",
    "SliReport",
    "TopicConfig",
    "TransactionalOutbox",
    "assign_partitions",
    "partition_for_key",
    "replay",
    "run_consumer",
    "seek_to_timestamp",
]
