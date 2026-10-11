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

from collections.abc import Mapping
from contextlib import AbstractContextManager
from typing import Protocol

from tourillon.core.models.config import TelemetryConfig

AttributeValue = str | bool | int | float
Attributes = Mapping[str, AttributeValue]


class BoundCounter(Protocol):
    def inc(self, amount: int | float = 1) -> None: ...


class BoundGauge(Protocol):
    def set(self, value: int | float) -> None: ...


class CounterMetric(Protocol):
    def add(
        self,
        amount: int | float = 1,
        *,
        attributes: Attributes | None = None,
    ) -> None: ...

    def labels(self, **labels: AttributeValue) -> BoundCounter: ...


class GaugeMetric(Protocol):
    def set(
        self,
        value: int | float,
        *,
        attributes: Attributes | None = None,
    ) -> None: ...

    def labels(self, **labels: AttributeValue) -> BoundGauge: ...


class Span(Protocol):
    def set_attribute(self, key: str, value: AttributeValue) -> None: ...

    def record_exception(self, exc: BaseException) -> None: ...

    def set_status_error(self, description: str = "") -> None: ...


class Telemetry(Protocol):
    @property
    def protocol_errors_total(self) -> CounterMetric: ...

    @property
    def connections_accepted_total(self) -> CounterMetric: ...

    @property
    def connections_closed_total(self) -> CounterMetric: ...

    @property
    def dispatched_total(self) -> CounterMetric: ...

    @property
    def unknown_kind_total(self) -> CounterMetric: ...

    @property
    def in_flight_current(self) -> GaugeMetric: ...

    @property
    def pool_connections_total(self) -> CounterMetric: ...

    @property
    def pool_evictions_total(self) -> CounterMetric: ...

    @property
    def pool_promotions_total(self) -> CounterMetric: ...

    @property
    def pool_acquire_errors_total(self) -> CounterMetric: ...

    @property
    def pool_size_permanent(self) -> GaugeMetric: ...

    @property
    def pool_size_overflow(self) -> GaugeMetric: ...

    @property
    def gossip_push_attempt_total(self) -> CounterMetric: ...

    @property
    def gossip_push_error_total(self) -> CounterMetric: ...

    @property
    def gossip_ae_cycle_total(self) -> CounterMetric: ...

    @property
    def gossip_hot_queue_size(self) -> GaugeMetric: ...

    @property
    def gossip_ae_diverged(self) -> CounterMetric: ...

    @property
    def gossip_last_ae_peer(self) -> GaugeMetric: ...

    @property
    def gossip_last_ae_at(self) -> GaugeMetric: ...

    @property
    def gossip_failed_announcements_total(self) -> CounterMetric: ...

    def start_span(
        self,
        name: str,
        *,
        attributes: Attributes | None = None,
    ) -> AbstractContextManager[Span]: ...

    async def close(self) -> None: ...


class TelemetryProvider(Protocol):
    def create(self, config: TelemetryConfig) -> Telemetry: ...
