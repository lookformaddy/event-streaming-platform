from eventstream.broker import ManualClock
from eventstream.metrics import Metrics, SliEvaluator, percentile


def test_percentile_nearest_rank():
    values = list(range(1, 101))
    assert percentile(values, 50) == 50.0
    assert percentile(values, 95) == 95.0
    assert percentile(values, 99) == 99.0
    assert percentile(values, 100) == 100.0
    assert percentile([], 95) == 0.0


def test_produce_and_consume_rates_use_separate_series():
    clock = ManualClock(now_ms=0)
    metrics = Metrics(clock=clock)
    metrics.record_produce("orders", count=6)
    metrics.record_consume("orders", latency_ms=10.0, count=3)
    clock.advance(30_000)
    assert metrics.produce_rate("orders", window_ms=60_000) == 6 / 60
    assert metrics.consume_rate("orders", window_ms=60_000) == 3 / 60
    clock.advance(60_000)  # all events fall out of the 60s window
    assert metrics.produce_rate("orders", window_ms=60_000) == 0.0


def test_p95_latency_from_recorded_latencies():
    metrics = Metrics()
    for latency in [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]:
        metrics.record_consume("orders", latency_ms=float(latency))
    assert metrics.p95_latency("orders") == 100.0
    assert metrics.latency_percentile("orders", 50) == 50.0


def test_failure_count_and_snapshot():
    metrics = Metrics()
    metrics.record_produce("orders", count=2)
    metrics.record_failure("orders", count=3)
    assert metrics.failure_count("orders") == 3
    snapshot = metrics.snapshot()
    assert snapshot["orders"]["produced"] == 2
    assert snapshot["orders"]["failures"] == 3


def test_sli_healthy():
    report = SliEvaluator().evaluate(lag=0, p95_latency_ms=12.0, dlq_rate=0.005, quarantine_rate=0.012)
    assert report.status == "HEALTHY"
    assert report.alerts == ()


def test_sli_degraded_on_nonzero_lag():
    report = SliEvaluator().evaluate(lag=5, p95_latency_ms=12.0, dlq_rate=0.0, quarantine_rate=0.0)
    assert report.status == "DEGRADED"
    assert any("lag" in alert for alert in report.alerts)


def test_sli_unhealthy_on_large_lag():
    report = SliEvaluator().evaluate(lag=500, p95_latency_ms=12.0, dlq_rate=0.0, quarantine_rate=0.0)
    assert report.status == "UNHEALTHY"


def test_sli_unhealthy_on_latency_blowout():
    report = SliEvaluator().evaluate(lag=0, p95_latency_ms=5000.0, dlq_rate=0.0, quarantine_rate=0.0)
    assert report.status == "UNHEALTHY"


def test_sli_degraded_on_dlq_rate():
    report = SliEvaluator().evaluate(lag=0, p95_latency_ms=12.0, dlq_rate=0.05, quarantine_rate=0.0)
    assert report.status == "DEGRADED"
    assert any("DLQ" in alert for alert in report.alerts)
