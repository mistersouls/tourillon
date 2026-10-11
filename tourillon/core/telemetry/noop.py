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

from contextlib import AbstractContextManager

from tourillon.core.models.config import TelemetryConfig
from tourillon.core.ports.telemetry import (
    Attributes,
    AttributeValue,
    BoundCounter,
    BoundGauge,
    CounterMetric,
    GaugeMetric,
    Span,
    Telemetry,
    TelemetryProvider,
)


class _NoopBoundCounter(BoundCounter):
    def inc(self, amount: int | float = 1) -> None:
        _ = amount


class _NoopBoundGauge(BoundGauge):
    def set(self, value: int | float) -> None:
        _ = value


class _NoopCounterMetric(CounterMetric):
    def add(
        self,
        amount: int | float = 1,
        *,
        attributes: Attributes | None = None,
    ) -> None:
        _ = amount
        _ = attributes

    def labels(self, **labels: AttributeValue) -> BoundCounter:
        _ = labels
        return _NoopBoundCounter()


class _NoopGaugeMetric(GaugeMetric):
    def set(
        self,
        value: int | float,
        *,
        attributes: Attributes | None = None,
    ) -> None:
        _ = value
        _ = attributes

    def labels(self, **labels: AttributeValue) -> BoundGauge:
        _ = labels
        return _NoopBoundGauge()


class _NoopSpan(Span):
    def set_attribute(self, key: str, value: AttributeValue) -> None:
        _ = key
        _ = value

    def record_exception(self, exc: BaseException) -> None:
        _ = exc

    def set_status_error(self, description: str = "") -> None:
        _ = description


class _NoopSpanContext(AbstractContextManager[Span]):
    def __enter__(self) -> Span:
        return _NoopSpan()

    def __exit__(self, exc_type, exc, tb) -> bool | None:
        _ = exc_type
        _ = exc
        _ = tb
        return None


class NoopTelemetry(Telemetry):
    @property
    def protocol_errors_total(self) -> CounterMetric:
        return _NoopCounterMetric()

    @property
    def connections_accepted_total(self) -> CounterMetric:
        return _NoopCounterMetric()

    @property
    def connections_closed_total(self) -> CounterMetric:
        return _NoopCounterMetric()

    @property
    def dispatched_total(self) -> CounterMetric:
        return _NoopCounterMetric()

    @property
    def unknown_kind_total(self) -> CounterMetric:
        return _NoopCounterMetric()

    @property
    def in_flight_current(self) -> GaugeMetric:
        return _NoopGaugeMetric()

    @property
    def pool_connections_total(self) -> CounterMetric:
        return _NoopCounterMetric()

    @property
    def pool_evictions_total(self) -> CounterMetric:
        return _NoopCounterMetric()

    @property
    def pool_promotions_total(self) -> CounterMetric:
        return _NoopCounterMetric()

    @property
    def pool_acquire_errors_total(self) -> CounterMetric:
        return _NoopCounterMetric()

    @property
    def pool_size_permanent(self) -> GaugeMetric:
        return _NoopGaugeMetric()

    @property
    def pool_size_overflow(self) -> GaugeMetric:
        return _NoopGaugeMetric()

    @property
    def gossip_push_attempt_total(self) -> CounterMetric:
        return _NoopCounterMetric()

    @property
    def gossip_push_error_total(self) -> CounterMetric:
        return _NoopCounterMetric()

    @property
    def gossip_ae_cycle_total(self) -> CounterMetric:
        return _NoopCounterMetric()

    @property
    def gossip_hot_queue_size(self) -> GaugeMetric:
        return _NoopGaugeMetric()

    @property
    def gossip_ae_diverged(self) -> CounterMetric:
        return _NoopCounterMetric()

    @property
    def gossip_last_ae_peer(self) -> GaugeMetric:
        return _NoopGaugeMetric()

    @property
    def gossip_last_ae_at(self) -> GaugeMetric:
        return _NoopGaugeMetric()

    @property
    def gossip_failed_announcements_total(self) -> CounterMetric:
        return _NoopCounterMetric()

    def start_span(
        self,
        name: str,
        *,
        attributes: Attributes | None = None,
    ) -> AbstractContextManager[Span]:
        _ = name
        _ = attributes
        return _NoopSpanContext()

    async def close(self) -> None:
        return None


class NoopTelemetryProvider(TelemetryProvider):
    def create(self, config: TelemetryConfig) -> Telemetry:
        _ = config
        return NoopTelemetry()


def build_noop_telemetry() -> Telemetry:
    return NoopTelemetry()


def build_noop_telemetry_provider() -> TelemetryProvider:
    return NoopTelemetryProvider()
