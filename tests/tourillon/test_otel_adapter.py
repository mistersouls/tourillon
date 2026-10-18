# Copyright 2026 Tourillon Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import cast

import pytest
from opentelemetry.trace import Status, StatusCode

from tourillon.core.config import TelemetryConfig
from tourillon.infra.adapter.telemetry import otel_adapter
from tourillon.infra.adapter.telemetry.otel_provider import OpenTelemetryProvider
from tourlib.exceptions import ValidationError


class _FakeInstrument:
    def __init__(self, name: str, description: str, unit: str) -> None:
        self.name = name
        self.description = description
        self.unit = unit
        self.calls: list[tuple[int | float, dict[str, object] | None]] = []

    def add(
        self, amount: int | float, attributes: dict[str, object] | None = None
    ) -> None:
        self.calls.append((amount, attributes))


class _FakeMeter:
    def __init__(self) -> None:
        self.counters: dict[str, _FakeInstrument] = {}
        self.up_down_counters: dict[str, _FakeInstrument] = {}

    def create_counter(
        self, *, name: str, description: str, unit: str
    ) -> _FakeInstrument:
        instrument = _FakeInstrument(name=name, description=description, unit=unit)
        self.counters[name] = instrument
        return instrument

    def create_up_down_counter(
        self, *, name: str, description: str, unit: str
    ) -> _FakeInstrument:
        instrument = _FakeInstrument(name=name, description=description, unit=unit)
        self.up_down_counters[name] = instrument
        return instrument


class _FakeMetricsApi:
    def __init__(self) -> None:
        self.meter_names: list[str] = []
        self.meter = _FakeMeter()

    def get_meter(self, name: str) -> _FakeMeter:
        self.meter_names.append(name)
        return self.meter


class _FakeSpan:
    def __init__(self) -> None:
        self.attributes: dict[str, object] = {}
        self.exceptions: list[BaseException] = []
        self.status: object = None

    def set_attribute(self, key: str, value: object) -> None:
        self.attributes[key] = value

    def record_exception(self, exc: BaseException) -> None:
        self.exceptions.append(exc)

    def set_status(self, status: object) -> None:
        self.status = status


class _FakeSpanContext:
    def __init__(self, span: _FakeSpan) -> None:
        self._span = span
        self.exit_calls: list[tuple[object, object, object]] = []

    def __enter__(self) -> _FakeSpan:
        return self._span

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.exit_calls.append((exc_type, exc, tb))
        return None


class _FakeTracer:
    def __init__(self) -> None:
        self.started: list[tuple[str, dict[str, object], _FakeSpanContext]] = []

    def start_as_current_span(
        self,
        name: str,
        attributes: dict[str, object] | None = None,
    ) -> _FakeSpanContext:
        context = _FakeSpanContext(_FakeSpan())
        self.started.append((name, attributes or {}, context))
        return context


class _FakeTraceApi:
    def __init__(self) -> None:
        self.tracer_names: list[str] = []
        self.tracer = _FakeTracer()

    def get_tracer(self, name: str) -> _FakeTracer:
        self.tracer_names.append(name)
        return self.tracer


@pytest.mark.tourillon
def test_open_telemetry_adapter_records_metrics_and_spans_with_generic_labels() -> None:
    metrics_api = _FakeMetricsApi()
    trace_api = _FakeTraceApi()

    telemetry = otel_adapter.OpenTelemetryTelemetry(
        prefix="demo",
        meter=metrics_api.meter,  # type: ignore[arg-type]
        tracer=trace_api.tracer,  # type: ignore[arg-type]
    )

    protocol_errors = telemetry.protocol_errors_total
    accepted = telemetry.connections_accepted_total
    closed = telemetry.connections_closed_total
    dispatched = telemetry.dispatched_total
    unknown = telemetry.unknown_kind_total
    gauge = telemetry.in_flight_current
    push_attempt = telemetry.gossip_push_attempt_total
    push_error = telemetry.gossip_push_error_total
    ae_cycle = telemetry.gossip_ae_cycle_total
    hot_queue = telemetry.gossip_hot_queue_size
    ae_diverged = telemetry.gossip_ae_diverged
    last_ae_peer = telemetry.gossip_last_ae_peer
    last_ae_at = telemetry.gossip_last_ae_at
    failed_announcements = telemetry.gossip_failed_announcements_total

    bound = accepted.labels(kind="kv.put", endpoint="peer")
    assert bound is accepted.labels(endpoint="peer", kind="kv.put")
    bound.inc(2)
    accepted.add()
    protocol_errors.add(3, attributes={"endpoint": "peer"})
    closed.labels(endpoint="peer").inc()
    dispatched.labels(endpoint="peer", mode=0).inc()
    unknown.labels(endpoint="peer").inc()
    push_attempt.add()
    push_error.add(2)
    ae_cycle.add()
    ae_diverged.add()
    failed_announcements.add()

    gauge.labels(endpoint="peer").set(3)
    gauge.labels(endpoint="peer").set(3)
    gauge.labels(endpoint="peer").set(1)
    hot_queue.set(5)
    hot_queue.set(2)
    last_ae_peer.set(123)
    last_ae_at.set(1700000000)

    with telemetry.start_span(
        "transport.envelope",
        attributes={"endpoint": "peer", "kind": "kv.put"},
    ) as span:
        exc = RuntimeError("boom")
        span.set_attribute("mode", 0)
        span.record_exception(exc)
        span.set_status_error("boom")

    accepted_instrument = metrics_api.meter.counters["demo_connections_accepted_total"]
    assert accepted_instrument.calls == [
        (2, {"endpoint": "peer", "kind": "kv.put"}),
        (1, None),
    ]
    assert metrics_api.meter.counters["demo_protocol_errors_total"].calls == [
        (3, {"endpoint": "peer"}),
    ]
    assert metrics_api.meter.counters["demo_connections_closed_total"].calls == [
        (1, {"endpoint": "peer"}),
    ]
    assert metrics_api.meter.counters["demo_dispatched_total"].calls == [
        (1, {"endpoint": "peer", "mode": 0}),
    ]
    assert metrics_api.meter.counters["demo_unknown_kind_total"].calls == [
        (1, {"endpoint": "peer"}),
    ]
    assert metrics_api.meter.counters["demo_gossip_push_attempt_total"].calls == [
        (1, None),
    ]
    assert metrics_api.meter.counters["demo_gossip_push_error_total"].calls == [
        (2, None),
    ]
    assert metrics_api.meter.counters["demo_gossip_ae_cycle_total"].calls == [
        (1, None),
    ]
    assert metrics_api.meter.counters["demo_gossip_ae_diverged"].calls == [
        (1, None),
    ]
    assert metrics_api.meter.counters[
        "demo_gossip_failed_announcements_total"
    ].calls == [
        (1, None),
    ]
    assert metrics_api.meter.up_down_counters["demo_in_flight_current"].calls == [
        (3.0, {"endpoint": "peer"}),
        (-2.0, {"endpoint": "peer"}),
    ]
    assert metrics_api.meter.up_down_counters["demo_gossip_hot_queue_size"].calls == [
        (5.0, None),
        (-3.0, None),
    ]
    assert metrics_api.meter.up_down_counters["demo_gossip_last_ae_peer"].calls == [
        (123.0, None),
    ]
    assert metrics_api.meter.up_down_counters["demo_gossip_last_ae_at"].calls == [
        (1700000000.0, None),
    ]

    span_name, attributes, context = trace_api.tracer.started[0]
    assert span_name == "transport.envelope"
    assert attributes == {"endpoint": "peer", "kind": "kv.put"}
    assert context.exit_calls == [(None, None, None)]
    assert context._span.attributes == {"mode": 0}
    assert len(context._span.exceptions) == 1
    assert str(context._span.exceptions[0]) == "boom"
    actual_status = context._span.status
    assert isinstance(actual_status, Status)
    assert actual_status.status_code == StatusCode.ERROR
    assert actual_status.description == "boom"


@pytest.mark.tourillon
def test_open_telemetry_adapter_uses_manager_local_metric_cache() -> None:
    first_metrics_api = _FakeMetricsApi()
    first = otel_adapter.OpenTelemetryTelemetry(prefix="demo", meter=first_metrics_api.meter)  # type: ignore[arg-type]
    first_metric = first.connections_accepted_total

    second_metrics_api = _FakeMetricsApi()
    second = otel_adapter.OpenTelemetryTelemetry(prefix="demo", meter=second_metrics_api.meter)  # type: ignore[arg-type]
    second_metric = second.connections_accepted_total

    assert first_metric is first.connections_accepted_total
    assert second_metric is second.connections_accepted_total
    assert first_metric is not second_metric
    assert (
        first_metrics_api.meter.counters["demo_connections_accepted_total"]
        is not second_metrics_api.meter.counters["demo_connections_accepted_total"]
    )


@pytest.mark.tourillon
def test_open_telemetry_adapter_supports_disabled_metrics_and_tracing() -> None:
    telemetry = otel_adapter.OpenTelemetryTelemetry(
        prefix="demo",
    )

    telemetry.connections_accepted_total.labels(endpoint="peer").inc()
    telemetry.in_flight_current.labels(endpoint="peer").set(4)
    with telemetry.start_span("noop", attributes={"endpoint": "peer"}) as span:
        span.record_exception(RuntimeError("ignored"))
        span.set_status_error("ignored")


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_open_telemetry_adapter_close_flushes_and_shuts_down_providers() -> None:
    events: list[str] = []

    async def close_hook() -> None:
        events.extend(
            [
                "tracer.flush",
                "tracer.shutdown",
                "meter.flush",
                "meter.shutdown",
            ]
        )

    telemetry = otel_adapter.OpenTelemetryTelemetry(
        close_hook=close_hook,
    )

    await telemetry.close()

    assert events == [
        "tracer.flush",
        "tracer.shutdown",
        "meter.flush",
        "meter.shutdown",
    ]


@pytest.mark.tourillon
def test_open_telemetry_adapter_span_port_sets_error_status() -> None:
    """_Span.set_status_error must call span.set_status with Status(StatusCode.ERROR, ...)."""
    fake_span = _FakeSpan()
    span_port = otel_adapter._Span(span=fake_span)  # type: ignore[arg-type]

    span_port.set_status_error("something went wrong")

    actual = fake_span.status
    assert isinstance(actual, Status)
    assert actual.status_code == StatusCode.ERROR
    assert actual.description == "something went wrong"


class _FakeTelemetry:
    def __init__(
        self,
        *,
        meter: object | None = None,
        tracer: object | None = None,
        close_hook: object | None = None,
    ) -> None:
        self.meter = meter
        self.tracer = tracer
        self.close_hook = close_hook


@pytest.mark.tourillon
def test_open_telemetry_provider_creates_adapter_when_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "tourillon.infra.adapter.telemetry.otel_provider.OpenTelemetryTelemetry",
        _FakeTelemetry,
    )
    provider = OpenTelemetryProvider()
    meter = _FakeMeter()
    setup_calls = {"metrics": 0}

    def fake_setup_metrics(_resource: object, _options: object) -> object:
        setup_calls["metrics"] += 1

        class _MeterProvider:
            def get_meter(self, _name: str) -> _FakeMeter:
                return meter

        return _MeterProvider()

    monkeypatch.setattr(provider, "_setup_metrics", fake_setup_metrics)

    config = TelemetryConfig(
        metrics_enabled=True,
        tracing_enabled=False,
        service_name="demo",
        metrics_options={"endpoint": "http://localhost:4318/v1/metrics"},
    )

    telemetry = provider.create(config)
    same_telemetry = provider.create(config)

    assert isinstance(telemetry, _FakeTelemetry)
    assert same_telemetry is telemetry
    assert telemetry.meter is meter
    assert telemetry.tracer is None
    assert telemetry.close_hook is not None
    assert setup_calls == {"metrics": 1}


@pytest.mark.tourillon
def test_open_telemetry_provider_falls_back_to_noop_when_disabled() -> None:
    provider = OpenTelemetryProvider()
    config = TelemetryConfig(
        metrics_enabled=False,
        tracing_enabled=False,
        service_name="demo",
    )

    telemetry = provider.create(config)

    telemetry.connections_accepted_total.labels(endpoint="peer").inc()
    with telemetry.start_span("noop") as span:
        span.set_status_error("ignored")
    assert telemetry.__class__.__name__ == "NoopTelemetry"


@pytest.mark.tourillon
def test_open_telemetry_provider_rejects_reinitialization_with_different_config() -> (
    None
):
    provider = OpenTelemetryProvider()
    first = TelemetryConfig(
        metrics_enabled=False,
        tracing_enabled=False,
        service_name="demo",
    )
    second = TelemetryConfig(
        metrics_enabled=True,
        tracing_enabled=False,
        service_name="demo",
        metrics_options={"endpoint": "http://localhost:4318/v1/metrics"},
    )

    provider.create(first)

    with pytest.raises(
        ValidationError, match="already initialized with a different telemetry config"
    ):
        provider.create(second)


@pytest.mark.tourillon
def test_open_telemetry_provider_sets_up_metrics_and_tracing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = OpenTelemetryProvider()
    metric_calls: dict[str, object] = {}
    trace_calls: dict[str, object] = {}

    class _FakeMetricExporter:
        def __init__(self, **kwargs: object) -> None:
            metric_calls["exporter_kwargs"] = kwargs

    class _FakeMetricReader:
        def __init__(self, exporter: object, **kwargs: object) -> None:
            metric_calls["reader_exporter"] = exporter
            metric_calls["reader_kwargs"] = kwargs

    class _FakeMeterProvider:
        def __init__(self, *, resource: object, metric_readers: list[object]) -> None:
            metric_calls["resource"] = resource
            metric_calls["metric_readers"] = metric_readers

    class _FakeSpanExporter:
        def __init__(self, **kwargs: object) -> None:
            trace_calls["exporter_kwargs"] = kwargs

    class _FakeBatchSpanProcessor:
        def __init__(self, exporter: object) -> None:
            trace_calls["processor_exporter"] = exporter

    class _FakeTracerProvider:
        def __init__(self, *, resource: object) -> None:
            self.processors: list[object] = []
            trace_calls["resource"] = resource

        def add_span_processor(self, processor: object) -> None:
            self.processors.append(processor)
            trace_calls["processors"] = list(self.processors)

    monkeypatch.setattr(
        "tourillon.infra.adapter.telemetry.otel_provider.OTLPMetricExporter",
        _FakeMetricExporter,
    )
    monkeypatch.setattr(
        "tourillon.infra.adapter.telemetry.otel_provider.PeriodicExportingMetricReader",
        _FakeMetricReader,
    )
    monkeypatch.setattr(
        "tourillon.infra.adapter.telemetry.otel_provider.MeterProvider",
        _FakeMeterProvider,
    )
    monkeypatch.setattr(
        "tourillon.infra.adapter.telemetry.otel_provider.metrics.set_meter_provider",
        lambda meter_provider: metric_calls.setdefault("set_provider", meter_provider),
    )
    monkeypatch.setattr(
        "tourillon.infra.adapter.telemetry.otel_provider.OTLPSpanExporter",
        _FakeSpanExporter,
    )
    monkeypatch.setattr(
        "tourillon.infra.adapter.telemetry.otel_provider.BatchSpanProcessor",
        _FakeBatchSpanProcessor,
    )
    monkeypatch.setattr(
        "tourillon.infra.adapter.telemetry.otel_provider.TracerProvider",
        _FakeTracerProvider,
    )
    monkeypatch.setattr(
        "tourillon.infra.adapter.telemetry.otel_provider.trace.set_tracer_provider",
        lambda tracer_provider: trace_calls.setdefault("set_provider", tracer_provider),
    )

    metrics_provider = provider._setup_metrics(
        resource=object(),  # type: ignore[arg-type]
        options={
            "endpoint": "http://localhost:4318/v1/metrics",
            "certificate_file": "ca.pem",
            "client_key_file": "client.key",
            "client_certificate_file": "client.pem",
            "timeout": 5,
            "export_interval_millis": 1234,
            "export_timeout_millis": 5678,
        },
    )
    tracing_provider = provider._setup_tracing(
        resource=object(),  # type: ignore[arg-type]
        options={
            "endpoint": "http://localhost:4318/v1/traces",
            "certificate_file": "ca.pem",
            "client_key_file": "client.key",
            "client_certificate_file": "client.pem",
            "timeout": 7,
        },
    )

    assert isinstance(metrics_provider, _FakeMeterProvider)
    assert metric_calls["exporter_kwargs"] == {
        "endpoint": "http://localhost:4318/v1/metrics",
        "certificate_file": "ca.pem",
        "client_key_file": "client.key",
        "client_certificate_file": "client.pem",
        "timeout": 5,
    }
    assert metric_calls["reader_kwargs"] == {
        "export_interval_millis": 1234,
        "export_timeout_millis": 5678,
    }
    assert metric_calls["set_provider"] is metrics_provider

    assert isinstance(tracing_provider, _FakeTracerProvider)
    assert trace_calls["exporter_kwargs"] == {
        "endpoint": "http://localhost:4318/v1/traces",
        "certificate_file": "ca.pem",
        "client_key_file": "client.key",
        "client_certificate_file": "client.pem",
        "timeout": 7,
    }
    assert len(cast(list[object], trace_calls["processors"])) == 1
    assert trace_calls["set_provider"] is tracing_provider


@pytest.mark.tourillon
def test_open_telemetry_provider_requires_endpoint_option() -> None:
    provider = OpenTelemetryProvider()

    with pytest.raises(Exception, match="endpoint is required"):
        provider._require_endpoint({})

    assert provider._require_endpoint({"endpoint": "http://localhost:4318"}) == (
        "http://localhost:4318"
    )
