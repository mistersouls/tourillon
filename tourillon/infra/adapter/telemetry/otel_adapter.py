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

from collections.abc import Awaitable, Callable, Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass

from opentelemetry.metrics import Counter, Meter, UpDownCounter
from opentelemetry.trace import Span as _OtelSpan
from opentelemetry.trace import Status, StatusCode, Tracer

from tourillon.core.ports.telemetry import (
    Attributes,
    AttributeValue,
    BoundCounter,
    BoundGauge,
    CounterMetric,
    GaugeMetric,
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
        self._metrics: dict[str, _CounterMetric | _GaugeMetric] = {}
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
    def gossip_last_ae_peer(self) -> GaugeMetric:
        return self._gauge_metric(
            name="gossip_last_ae_peer",
            description="Deterministic numeric fingerprint of the last AE peer node id.",
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

    def start_span(
        self,
        name: str,
        *,
        attributes: Mapping[str, AttributeValue] | None = None,
    ) -> AbstractContextManager[Span]:
        if self._tracer is None:
            return self._noop_telemetry.start_span(name, attributes=attributes)
        return _SpanContext(self._tracer, name, attributes=attributes)

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
