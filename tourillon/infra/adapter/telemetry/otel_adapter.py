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

import struct
from collections.abc import Awaitable, Callable, Iterator, Mapping
from contextlib import AbstractContextManager, contextmanager, nullcontext
from dataclasses import dataclass

from opentelemetry import context as otel_context
from opentelemetry import trace as otel_trace
from opentelemetry.metrics import Counter, Histogram, Meter, UpDownCounter
from opentelemetry.trace import (
    NonRecordingSpan,
    SpanContext,
    Status,
    StatusCode,
    TraceFlags,
    Tracer,
)
from opentelemetry.trace import Span as _OtelSpan

from tourillon.core.ports.telemetry import (
    Attributes,
    AttributeValue,
    BoundCounter,
    BoundGauge,
    CounterMetric,
    GaugeMetric,
    HistogramMetric,
    Span,
    Telemetry,
)
from tourillon.core.telemetry import build_noop_telemetry

_AttributesKey = tuple[tuple[str, AttributeValue], ...]


def _normalize_attributes(attributes: Attributes | None) -> _AttributesKey:
    if not attributes:
        return ()
    return tuple(sorted(attributes.items()))


def _materialize_attributes(
    attributes: Attributes | None,
) -> dict[str, AttributeValue] | None:
    normalized = _normalize_attributes(attributes)
    if not normalized:
        return None
    return dict(normalized)


@dataclass(slots=True)
class _BoundCounter(BoundCounter):
    metric: "_CounterMetric"
    attributes: dict[str, AttributeValue]

    def inc(self, amount: int | float = 1) -> None:
        self.metric.add(amount, attributes=self.attributes)


@dataclass(slots=True)
class _CounterMetric(CounterMetric):
    instrument: Counter
    _bound_by_attributes: dict[_AttributesKey, _BoundCounter]

    def add(
        self,
        amount: int | float = 1,
        *,
        attributes: Attributes | None = None,
    ) -> None:
        instrument_attributes = _materialize_attributes(attributes)
        if instrument_attributes is None:
            self.instrument.add(amount)
            return
        self.instrument.add(amount, attributes=instrument_attributes)

    def labels(self, **labels: AttributeValue) -> BoundCounter:
        key = _normalize_attributes(labels)
        bound = self._bound_by_attributes.get(key)
        if bound is None:
            bound = _BoundCounter(metric=self, attributes=dict(key))
            self._bound_by_attributes[key] = bound
        return bound


@dataclass(slots=True)
class _BoundGauge(BoundGauge):
    metric: "_GaugeMetric"
    attributes: dict[str, AttributeValue]

    def set(self, value: int | float) -> None:
        self.metric.set(value, attributes=self.attributes)


@dataclass(slots=True)
class _GaugeMetric(GaugeMetric):
    instrument: UpDownCounter
    _bound_by_attributes: dict[_AttributesKey, _BoundGauge]
    _current_values: dict[_AttributesKey, float]

    def set(
        self,
        value: int | float,
        *,
        attributes: Attributes | None = None,
    ) -> None:
        key = _normalize_attributes(attributes)
        current = self._current_values.get(key, 0.0)
        next_value = float(value)
        delta = next_value - current
        if delta == 0:
            return
        self._current_values[key] = next_value
        instrument_attributes = _materialize_attributes(attributes)
        if instrument_attributes is None:
            self.instrument.add(delta)
            return
        self.instrument.add(delta, attributes=instrument_attributes)

    def labels(self, **labels: AttributeValue) -> BoundGauge:
        key = _normalize_attributes(labels)
        bound = self._bound_by_attributes.get(key)
        if bound is None:
            bound = _BoundGauge(metric=self, attributes=dict(key))
            self._bound_by_attributes[key] = bound
        return bound


@dataclass(slots=True)
class _HistogramMetric(HistogramMetric):
    instrument: Histogram

    def record(
        self,
        value: int | float,
        *,
        attributes: Attributes | None = None,
    ) -> None:
        instrument_attributes = _materialize_attributes(attributes)
        if instrument_attributes is None:
            self.instrument.record(value)
            return
        self.instrument.record(value, attributes=instrument_attributes)


@dataclass(slots=True)
class _Span(Span):
    span: _OtelSpan

    def set_attribute(self, key: str, value: AttributeValue) -> None:
        self.span.set_attribute(key, value)

    def record_exception(self, exc: BaseException) -> None:
        self.span.record_exception(exc)

    def set_status_error(self, description: str = "") -> None:
        self.span.set_status(Status(StatusCode.ERROR, description))


class _SpanContext(AbstractContextManager[Span]):
    def __init__(
        self, tracer: Tracer, name: str, attributes: Attributes | None
    ) -> None:
        self._span_cm = tracer.start_as_current_span(
            name,
            attributes=dict(attributes or {}),
        )

    def __enter__(self) -> Span:
        span: _OtelSpan = self._span_cm.__enter__()
        return _Span(span=span)

    def __exit__(self, exc_type, exc, tb) -> bool | None:
        return self._span_cm.__exit__(exc_type, exc, tb)


# Envelope context layout: one format byte, then the W3C trace context
# fields in binary (trace id, parent span id, trace flags).
_TRACE_CONTEXT_V1 = 0x01
_TRACE_CONTEXT = struct.Struct(">B 16s 8s B")


def encode_trace_context() -> bytes:
    """Encode the current sampled span, or return ``b""``."""
    span_context = otel_trace.get_current_span().get_span_context()
    if not span_context.is_valid or not span_context.trace_flags.sampled:
        return b""
    return _TRACE_CONTEXT.pack(
        _TRACE_CONTEXT_V1,
        span_context.trace_id.to_bytes(16, "big"),
        span_context.span_id.to_bytes(8, "big"),
        int(span_context.trace_flags),
    )


def decode_trace_context(data: bytes) -> SpanContext | None:
    """Decode a context written by :func:`encode_trace_context`."""
    if len(data) != _TRACE_CONTEXT.size or data[0] != _TRACE_CONTEXT_V1:
        return None
    _, trace_id, span_id, flags = _TRACE_CONTEXT.unpack(data)
    span_context = SpanContext(
        trace_id=int.from_bytes(trace_id, "big"),
        span_id=int.from_bytes(span_id, "big"),
        is_remote=True,
        trace_flags=TraceFlags(flags),
    )
    return span_context if span_context.is_valid else None


@contextmanager
def _parent(span: _OtelSpan) -> Iterator[None]:
    token = otel_context.attach(otel_trace.set_span_in_context(span))
    try:
        yield
    finally:
        otel_context.detach(token)


class OpenTelemetryTelemetry(Telemetry):
    def __init__(
        self,
        prefix: str = "tourillon",
        *,
        meter: Meter | None = None,
        tracer: Tracer | None = None,
        close_hook: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self._prefix = prefix
        self._meter = meter
        self._tracer = tracer
        self._close_hook = close_hook
        self._metrics: dict[str, _CounterMetric | _GaugeMetric | _HistogramMetric] = {}
        self._noop_telemetry = build_noop_telemetry()

    @property
    def protocol_errors_total(self) -> CounterMetric:
        return self._counter_metric(
            name="protocol_errors_total",
            description="Total number of protocol errors encountered during frame encode/decode.",
        )

    @property
    def connections_accepted_total(self) -> CounterMetric:
        return self._counter_metric(
            name="connections_accepted_total",
            description="Total TCP connections accepted across all servers.",
        )

    @property
    def connections_closed_total(self) -> CounterMetric:
        return self._counter_metric(
            name="connections_closed_total",
            description="Total TCP connections closed across all servers.",
        )

    @property
    def dispatched_total(self) -> CounterMetric:
        return self._counter_metric(
            name="dispatched_total",
            description="Total envelope frames successfully dispatched to a registered handler.",
        )

    @property
    def unknown_kind_total(self) -> CounterMetric:
        return self._counter_metric(
            name="unknown_kind_total",
            description="Total envelope frames rejected because no handler was registered for their kind.",
        )

    @property
    def in_flight_current(self) -> GaugeMetric:
        return self._gauge_metric(
            name="in_flight_current",
            description="Current number of in-flight requests.",
        )

    @property
    def pool_connections_total(self) -> CounterMetric:
        return self._counter_metric(
            name="pool_connections_total",
            description="Total connections created by the client pool.",
        )

    @property
    def pool_evictions_total(self) -> CounterMetric:
        return self._counter_metric(
            name="pool_evictions_total",
            description="Total connections evicted from the client pool.",
        )

    @property
    def pool_promotions_total(self) -> CounterMetric:
        return self._counter_metric(
            name="pool_promotions_total",
            description="Total overflow connections promoted to permanent in the client pool.",
        )

    @property
    def pool_acquire_errors_total(self) -> CounterMetric:
        return self._counter_metric(
            name="pool_acquire_errors_total",
            description="Total failed acquire attempts in the client pool.",
        )

    @property
    def pool_size_permanent(self) -> GaugeMetric:
        return self._gauge_metric(
            name="pool_size_permanent",
            description="Current number of permanent connections in the client pool.",
        )

    @property
    def pool_size_overflow(self) -> GaugeMetric:
        return self._gauge_metric(
            name="pool_size_overflow",
            description="Current number of overflow connections in the client pool.",
        )

    @property
    def gossip_push_attempt_total(self) -> CounterMetric:
        return self._counter_metric(
            name="gossip_push_attempt_total",
            description="Total gossip push attempts.",
        )

    @property
    def gossip_push_error_total(self) -> CounterMetric:
        return self._counter_metric(
            name="gossip_push_error_total",
            description="Total gossip push errors.",
        )

    @property
    def gossip_ae_cycle_total(self) -> CounterMetric:
        return self._counter_metric(
            name="gossip_ae_cycle_total",
            description="Total anti-entropy cycles executed by gossip engine.",
        )

    @property
    def gossip_hot_queue_size(self) -> GaugeMetric:
        return self._gauge_metric(
            name="gossip_hot_queue_size",
            description="Current size of gossip hot queue.",
        )

    @property
    def gossip_ae_diverged(self) -> CounterMetric:
        return self._counter_metric(
            name="gossip_ae_diverged",
            description="Total anti-entropy cycles where local and peer fingerprints diverged.",
        )

    @property
    def gossip_last_ae_at(self) -> GaugeMetric:
        return self._gauge_metric(
            name="gossip_last_ae_at",
            description="Unix timestamp of last completed AE ping/pong.",
        )

    @property
    def gossip_failed_announcements_total(self) -> CounterMetric:
        return self._counter_metric(
            name="gossip_failed_announcements_total",
            description="Total FAILED announcements emitted through gossip hot path.",
        )

    @property
    def kv_requests_total(self) -> CounterMetric:
        return self._counter_metric(
            name="kv_requests_total",
            description="Client operations coordinated by this node, by op and outcome.",
        )

    @property
    def kv_request_duration_seconds(self) -> HistogramMetric:
        return self._histogram_metric(
            name="kv_request_duration_seconds",
            description="Coordinator latency of client operations, by op and outcome.",
            unit="s",
        )

    @property
    def replication_failures_total(self) -> CounterMetric:
        return self._counter_metric(
            name="replication_failures_total",
            description="Replica requests that failed or timed out, by op.",
        )

    @property
    def read_repairs_total(self) -> CounterMetric:
        return self._counter_metric(
            name="read_repairs_total",
            description="Read repairs, by outcome.",
        )

    @property
    def obligations_total(self) -> CounterMetric:
        return self._counter_metric(
            name="obligations_total",
            description="Replication obligations recorded, delivered, dropped or deferred.",
        )

    @property
    def ae_sessions_total(self) -> CounterMetric:
        return self._counter_metric(
            name="ae_sessions_total",
            description="Anti-entropy sessions initiated by this node, by goal and outcome.",
        )

    @property
    def ae_versions_total(self) -> CounterMetric:
        return self._counter_metric(
            name="ae_versions_total",
            description="Versions copied by anti-entropy sessions, by direction.",
        )

    @property
    def ae_bytes_total(self) -> CounterMetric:
        return self._counter_metric(
            name="ae_bytes_total",
            description="Value bytes copied by anti-entropy sessions, by direction.",
        )

    @property
    def ae_sweeps_total(self) -> CounterMetric:
        return self._counter_metric(
            name="ae_sweeps_total",
            description="Anti-entropy sweeps, by outcome.",
        )

    def start_span(
        self,
        name: str,
        *,
        attributes: Mapping[str, AttributeValue] | None = None,
    ) -> AbstractContextManager[Span]:
        if self._tracer is None:
            return self._noop_telemetry.start_span(name, attributes=attributes)
        return _SpanContext(self._tracer, name, attributes=attributes)

    def inject_context(self) -> bytes:
        if self._tracer is None:
            return b""
        return encode_trace_context()

    def extract_context(self, data: bytes) -> AbstractContextManager[None]:
        if self._tracer is None:
            return nullcontext()
        span_context = decode_trace_context(data)
        if span_context is None:
            # No remote parent: start a new trace rather than nesting under
            # whatever span is current.
            return _parent(otel_trace.INVALID_SPAN)
        return _parent(NonRecordingSpan(span_context))

    async def close(self) -> None:
        if self._close_hook is not None:
            await self._close_hook()

    def _counter_metric(self, name: str, description: str) -> CounterMetric:
        if self._meter is None:
            return self._noop_telemetry.protocol_errors_total
        metric_name = f"{self._prefix}_{name}"
        metric = self._metrics.get(metric_name)
        if metric is None:
            instrument = self._meter.create_counter(
                name=metric_name, description=description, unit="1"
            )
            metric = _CounterMetric(instrument=instrument, _bound_by_attributes={})
            self._metrics[metric_name] = metric
        assert isinstance(metric, _CounterMetric)
        return metric

    def _gauge_metric(self, name: str, description: str) -> GaugeMetric:
        if self._meter is None:
            return self._noop_telemetry.in_flight_current
        metric_name = f"{self._prefix}_{name}"
        metric = self._metrics.get(metric_name)
        if metric is None:
            instrument = self._meter.create_up_down_counter(
                name=metric_name,
                description=description,
                unit="1",
            )
            metric = _GaugeMetric(
                instrument=instrument,
                _bound_by_attributes={},
                _current_values={},
            )
            self._metrics[metric_name] = metric
        assert isinstance(metric, _GaugeMetric)
        return metric

    def _histogram_metric(
        self, name: str, description: str, unit: str
    ) -> HistogramMetric:
        if self._meter is None:
            return self._noop_telemetry.kv_request_duration_seconds
        metric_name = f"{self._prefix}_{name}"
        metric = self._metrics.get(metric_name)
        if metric is None:
            instrument = self._meter.create_histogram(
                name=metric_name, description=description, unit=unit
            )
            metric = _HistogramMetric(instrument=instrument)
            self._metrics[metric_name] = metric
        assert isinstance(metric, _HistogramMetric)
        return metric
