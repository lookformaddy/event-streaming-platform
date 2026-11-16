"""Command-line interface for the event streaming platform.

Subcommands (each builds a fresh in-process broker; state is per-invocation):

* ``demo`` — run the full 1,000-event end-to-end demo.
* ``produce`` — create a topic (if needed) and produce one JSON record.
* ``consume`` — create a consumer group, subscribe, and poll records.
* ``topics`` — list topics on a fresh broker (empty unless seeded by demo).
* ``replay`` — re-read a topic slice (demo-seeded broker).
* ``metrics`` — print the demo's metrics snapshot.
"""

from __future__ import annotations

import argparse
import json
import sys

from eventstream.broker import Broker
from eventstream.config import Settings
from eventstream.consumer import ConsumerGroup
from eventstream.demo import run_demo
from eventstream.demo_data import ORDER_SCHEMA
from eventstream.models import TopicConfig


def _seeded_broker() -> Broker:
    """Broker with the orders schema registered and an empty orders topic."""
    broker = Broker(settings=Settings())
    broker.schemas.register(ORDER_SCHEMA)
    broker.create_topic(
        TopicConfig(name="orders", partitions=4, schema_name="order-event", schema_version=1)
    )
    return broker


def _cmd_demo(_args: argparse.Namespace) -> int:
    result = run_demo()
    return 0 if result["sli_status"] == "HEALTHY" else 1


def _cmd_produce(args: argparse.Namespace) -> int:
    broker = _seeded_broker()
    try:
        value = json.loads(args.value)
    except json.JSONDecodeError as exc:
        print(f"invalid JSON value: {exc}", file=sys.stderr)
        return 2
    outcome = broker.produce(args.topic, args.key, value, dedup_key=args.dedup_key)
    if outcome.quarantined:
        print(json.dumps({"ok": False, "quarantined": True,
                          "violations": [str(v) for v in outcome.violations]}, indent=2))
        return 1
    if outcome.duplicate:
        print(json.dumps({"ok": False, "duplicate": True}, indent=2))
        return 1
    record = outcome.record
    assert record is not None
    print(json.dumps({
        "ok": True, "topic": record.topic, "partition": record.partition,
        "offset": record.offset, "timestamp_ms": record.timestamp_ms,
    }, indent=2))
    return 0


def _cmd_consume(args: argparse.Namespace) -> int:
    broker = _seeded_broker()
    group = ConsumerGroup(args.group, broker)
    group.join(args.member)
    group.subscribe([args.topic])
    records = group.poll(args.member, max_records=args.limit)
    for record in records:
        print(json.dumps({
            "topic": record.topic, "partition": record.partition,
            "offset": record.offset, "key": record.key, "value": record.value,
        }))
    print(f"# polled {len(records)} record(s); lag {group.lag(args.topic)}", file=sys.stderr)
    return 0


def _cmd_topics(_args: argparse.Namespace) -> int:
    broker = _seeded_broker()
    for config in broker.list_topics():
        print(f"{config.name}\tpartitions={config.partitions}\tschema={config.schema_name} v{config.schema_version}")
    return 0


def _cmd_replay(args: argparse.Namespace) -> int:
    from eventstream.replay import replay

    broker = _seeded_broker()
    records = replay(broker, args.topic, from_offset=args.from_offset, max_records=args.limit)
    for record in records:
        print(json.dumps({
            "partition": record.partition, "offset": record.offset,
            "key": record.key, "value": record.value,
        }))
    print(f"# replayed {len(records)} record(s)", file=sys.stderr)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="event-stream", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    p_demo = sub.add_parser("demo", help="run the full 1,000-event demo")
    p_demo.set_defaults(func=_cmd_demo)

    p_produce = sub.add_parser("produce", help="produce one JSON record")
    p_produce.add_argument("--topic", default="orders")
    p_produce.add_argument("--key", default=None)
    p_produce.add_argument("--value", required=True, help="JSON object")
    p_produce.add_argument("--dedup-key", default=None)
    p_produce.set_defaults(func=_cmd_produce)

    p_consume = sub.add_parser("consume", help="poll a topic as a consumer group")
    p_consume.add_argument("--topic", default="orders")
    p_consume.add_argument("--group", default="cli-group")
    p_consume.add_argument("--member", default="cli-member-1")
    p_consume.add_argument("--limit", type=int, default=10)
    p_consume.set_defaults(func=_cmd_consume)

    p_topics = sub.add_parser("topics", help="list topics")
    p_topics.set_defaults(func=_cmd_topics)

    p_replay = sub.add_parser("replay", help="replay a topic slice")
    p_replay.add_argument("--topic", default="orders")
    p_replay.add_argument("--from-offset", type=int, default=0)
    p_replay.add_argument("--limit", type=int, default=10)
    p_replay.set_defaults(func=_cmd_replay)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
