"""Optional HTTP service surface (FastAPI).

The service is a thin adapter over the in-process broker: topics, produce,
consumer groups, replay, DLQ, and metrics. It is optional — the engine
works without FastAPI, and the service tests auto-skip when FastAPI is not
installed (they run in CI, where requirements.txt is installed).
"""

from __future__ import annotations

try:
    from fastapi import FastAPI, HTTPException
    from pydantic import BaseModel
except ImportError:  # pragma: no cover - exercised only without FastAPI
    FastAPI = None  # type: ignore[assignment]
    HTTPException = None  # type: ignore[assignment]
    BaseModel = object  # type: ignore[assignment]

from eventstream.broker import Broker
from eventstream.config import Settings
from eventstream.consumer import ConsumerGroup
from eventstream.dlq import DeadLetterQueue
from eventstream.metrics import Metrics, SliEvaluator
from eventstream.models import TopicConfig
from eventstream.replay import replay


class TopicCreate(BaseModel):
    name: str
    partitions: int = 4
    retention_ms: int | None = None
    compaction: bool = False
    schema_name: str | None = None
    schema_version: int | None = None


class ProduceRequest(BaseModel):
    key: str | None = None
    value: dict
    producer_id: str = "api-producer"
    dedup_key: str | None = None


class JoinRequest(BaseModel):
    member_id: str


class SubscribeRequest(BaseModel):
    topics: list[str]


class PollRequest(BaseModel):
    member_id: str
    max_records: int = 100


class CommitRequest(BaseModel):
    topic: str
    partition: int
    offset: int


class ReplayRequest(BaseModel):
    topic: str
    from_offset: int = 0
    from_timestamp_ms: int | None = None
    to_timestamp_ms: int | None = None
    max_records: int | None = None


def create_app(broker: Broker | None = None) -> "FastAPI":
    """Build the FastAPI app around a broker (a fresh one by default)."""
    if FastAPI is None:
        raise RuntimeError("FastAPI is not installed; install requirements.txt")
    settings = Settings()
    broker = broker or Broker(settings=settings)
    app = FastAPI(title="event-streaming-platform", version="1.0.0")
    app.state.broker = broker
    app.state.groups: dict[str, ConsumerGroup] = {}
    app.state.dlq = DeadLetterQueue()
    app.state.metrics = Metrics()

    def group_or_404(name: str) -> ConsumerGroup:
        group = app.state.groups.get(name)
        if group is None:
            raise HTTPException(status_code=404, detail=f"group {name!r} not found")
        return group

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok", "topics": len(app.state.broker.list_topics())}

    @app.post("/topics", status_code=201)
    def create_topic(body: TopicCreate) -> dict:
        from eventstream.errors import TopicExistsError

        try:
            config = app.state.broker.create_topic(
                TopicConfig(
                    name=body.name,
                    partitions=body.partitions,
                    retention_ms=body.retention_ms,
                    compaction=body.compaction,
                    schema_name=body.schema_name,
                    schema_version=body.schema_version,
                )
            )
        except TopicExistsError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {"name": config.name, "partitions": config.partitions}

    @app.get("/topics")
    def list_topics() -> dict:
        return {
            "topics": [
                {"name": c.name, "partitions": c.partitions, "compaction": c.compaction}
                for c in app.state.broker.list_topics()
            ]
        }

    @app.post("/topics/{topic}/produce", status_code=201)
    def produce(topic: str, body: ProduceRequest) -> dict:
        from eventstream.errors import TopicNotFoundError

        try:
            outcome = app.state.broker.produce(
                topic,
                body.key,
                body.value,
                producer_id=body.producer_id,
                dedup_key=body.dedup_key,
            )
        except TopicNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        if outcome.quarantined:
            return {
                "ok": False,
                "quarantined": True,
                "violations": [str(v) for v in outcome.violations],
            }
        if outcome.duplicate:
            return {"ok": False, "duplicate": True}
        app.state.metrics.record_produce(topic)
        record = outcome.record
        assert record is not None
        return {
            "ok": True,
            "partition": record.partition,
            "offset": record.offset,
            "timestamp_ms": record.timestamp_ms,
        }

    @app.get("/topics/{topic}/records")
    def records(topic: str, partition: int = 0, offset: int = 0, limit: int = 100) -> dict:
        from eventstream.errors import TopicNotFoundError

        try:
            fetched = app.state.broker.fetch(topic, partition, offset, limit)
        except (TopicNotFoundError, ValueError) as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {
            "records": [
                {
                    "partition": r.partition,
                    "offset": r.offset,
                    "key": r.key,
                    "value": r.value,
                    "timestamp_ms": r.timestamp_ms,
                }
                for r in fetched
            ]
        }

    @app.get("/topics/{topic}/quarantine")
    def quarantine(topic: str) -> dict:
        from eventstream.errors import TopicNotFoundError

        try:
            items = app.state.broker.quarantine(topic)
        except TopicNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {
            "quarantined": [
                {"key": q.key, "value": q.value, "violations": [str(v) for v in q.violations]}
                for q in items
            ]
        }

    @app.post("/groups/{name}/join")
    def join_group(name: str, body: JoinRequest) -> dict:
        group = app.state.groups.setdefault(
            name, ConsumerGroup(name, app.state.broker, settings=settings)
        )
        group.join(body.member_id)
        return {"group": name, "members": group.members()}

    @app.post("/groups/{name}/subscribe")
    def subscribe_group(name: str, body: SubscribeRequest) -> dict:
        group = group_or_404(name)
        group.subscribe(body.topics)
        return {"group": name, "subscriptions": group.subscriptions()}

    @app.post("/groups/{name}/poll")
    def poll_group(name: str, body: PollRequest) -> dict:
        group = group_or_404(name)
        records = group.poll(body.member_id, max_records=body.max_records)
        return {
            "records": [
                {
                    "topic": r.topic,
                    "partition": r.partition,
                    "offset": r.offset,
                    "key": r.key,
                    "value": r.value,
                }
                for r in records
            ]
        }

    @app.post("/groups/{name}/commit")
    def commit_group(name: str, body: CommitRequest) -> dict:
        group = group_or_404(name)
        group.commit(body.topic, body.partition, body.offset)
        return {"group": name, "topic": body.topic, "partition": body.partition, "offset": body.offset}

    @app.get("/groups/{name}/lag")
    def group_lag(name: str) -> dict:
        group = group_or_404(name)
        return {"group": name, "lag": group.lag()}

    @app.get("/metrics")
    def metrics() -> dict:
        return app.state.metrics.snapshot()

    @app.get("/dlq/{topic}")
    def dlq_entries(topic: str) -> dict:
        return {
            "entries": [
                {
                    "partition": e.partition,
                    "offset": e.offset,
                    "key": e.key,
                    "value": e.value,
                    "failures": [
                        {"attempt": f.attempt, "error": f.error} for f in e.failures
                    ],
                }
                for e in app.state.dlq.entries(topic)
            ]
        }

    @app.post("/replay")
    def replay_topic(body: ReplayRequest) -> dict:
        from eventstream.errors import TopicNotFoundError

        try:
            records = replay(
                app.state.broker,
                body.topic,
                from_offset=body.from_offset,
                from_timestamp_ms=body.from_timestamp_ms,
                to_timestamp_ms=body.to_timestamp_ms,
                max_records=body.max_records,
            )
        except (TopicNotFoundError, ValueError) as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {
            "records": [
                {"partition": r.partition, "offset": r.offset, "key": r.key, "value": r.value}
                for r in records
            ]
        }

    @app.get("/sli")
    def sli() -> dict:
        report = SliEvaluator(settings).evaluate(
            lag=sum(g.lag() for g in app.state.groups.values()),
            p95_latency_ms=app.state.metrics.p95_latency(),
            dlq_rate=0.0,
            quarantine_rate=0.0,
        )
        return {"status": report.status, "lag": report.lag, "p95_latency_ms": report.p95_latency_ms}

    return app


app = create_app() if FastAPI is not None else None
