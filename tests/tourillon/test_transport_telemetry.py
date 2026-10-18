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

import asyncio
import ssl
from collections import defaultdict
from collections.abc import AsyncIterator, Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass
from uuid import uuid4

import pytest

from tests.helpers import start_envelope_server
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
from tourlib.constant import TRANSPORT_ERROR_KIND
from tourlib.envelope import Envelope, read_envelope


def _label_key(
    attributes: Mapping[str, AttributeValue] | None = None,
) -> tuple[tuple[str, AttributeValue], ...]:
    if not attributes:
        return ()
    return tuple(sorted(attributes.items()))


@dataclass(slots=True)
class _CounterBound(BoundCounter):
    metric: "_CounterMetricRecorder"
    labels: dict[str, AttributeValue]

    def inc(self, amount: int | float = 1) -> None:
        self.metric.add(amount, attributes=self.labels)


@dataclass(slots=True)
class _GaugeBound(BoundGauge):
    metric: "_GaugeMetricRecorder"
    labels: dict[str, AttributeValue]

    def set(self, value: int | float) -> None:
        self.metric.set(value, attributes=self.labels)


class _CounterMetricRecorder(CounterMetric):
    def __init__(self) -> None:
        self.values: dict[tuple[tuple[str, AttributeValue], ...], float] = defaultdict(
            float
        )

    def add(
        self,
        amount: int | float = 1,
        *,
        attributes: Mapping[str, AttributeValue] | None = None,
    ) -> None:
        self.values[_label_key(attributes)] += float(amount)

    def labels(self, **labels: AttributeValue) -> BoundCounter:
        return _CounterBound(metric=self, labels=dict(labels))


class _GaugeMetricRecorder(GaugeMetric):
    def __init__(self) -> None:
        self.values: dict[tuple[tuple[str, AttributeValue], ...], float] = {}

    def set(
        self,
        value: int | float,
        *,
        attributes: Mapping[str, AttributeValue] | None = None,
    ) -> None:
        self.values[_label_key(attributes)] = float(value)

    def labels(self, **labels: AttributeValue) -> BoundGauge:
        return _GaugeBound(metric=self, labels=dict(labels))


class _SpanRecorder(Span):
    def __init__(self) -> None:
        self.attributes: dict[str, AttributeValue] = {}
        self.exceptions: list[BaseException] = []
        self.error_descriptions: list[str] = []

    def set_attribute(self, key: str, value: AttributeValue) -> None:
        self.attributes[key] = value

    def record_exception(self, exc: BaseException) -> None:
        self.exceptions.append(exc)

    def set_status_error(self, description: str = "") -> None:
        self.error_descriptions.append(description)


class _SpanContextRecorder(AbstractContextManager[Span]):
    def __init__(
        self,
        store: list[tuple[str, dict[str, AttributeValue]]],
        name: str,
        attributes: Attributes | None,
    ) -> None:
        self._store = store
        self._name = name
        self._attributes = dict(attributes or {})

    def __enter__(self) -> Span:
        self._store.append((self._name, self._attributes))
        return _SpanRecorder()

    def __exit__(self, exc_type, exc, tb) -> bool | None:
        _ = exc_type
        _ = exc
        _ = tb
        return None


class _TelemetryRecorder(Telemetry):
    def __init__(self) -> None:
        self._protocol_errors_total = _CounterMetricRecorder()
        self._connections_accepted_total = _CounterMetricRecorder()
        self._connections_closed_total = _CounterMetricRecorder()
        self._dispatched_total = _CounterMetricRecorder()
        self._unknown_kind_total = _CounterMetricRecorder()
        self._in_flight_current = _GaugeMetricRecorder()
        self._pool_connections_total = _CounterMetricRecorder()
        self._pool_evictions_total = _CounterMetricRecorder()
        self._pool_promotions_total = _CounterMetricRecorder()
        self._pool_acquire_errors_total = _CounterMetricRecorder()
        self._pool_size_permanent = _GaugeMetricRecorder()
        self._pool_size_overflow = _GaugeMetricRecorder()
        self.spans: list[tuple[str, dict[str, AttributeValue]]] = []

    @property
    def protocol_errors_total(self) -> CounterMetric:
        return self._protocol_errors_total

    @property
    def connections_accepted_total(self) -> CounterMetric:
        return self._connections_accepted_total

    @property
    def connections_closed_total(self) -> CounterMetric:
        return self._connections_closed_total

    @property
    def dispatched_total(self) -> CounterMetric:
        return self._dispatched_total

    @property
    def unknown_kind_total(self) -> CounterMetric:
        return self._unknown_kind_total

    @property
    def in_flight_current(self) -> GaugeMetric:
        return self._in_flight_current

    @property
    def pool_connections_total(self) -> CounterMetric:
        return self._pool_connections_total

    @property
    def pool_evictions_total(self) -> CounterMetric:
        return self._pool_evictions_total

    @property
    def pool_promotions_total(self) -> CounterMetric:
        return self._pool_promotions_total

    @property
    def pool_acquire_errors_total(self) -> CounterMetric:
        return self._pool_acquire_errors_total

    @property
    def pool_size_permanent(self) -> GaugeMetric:
        return self._pool_size_permanent

    @property
    def pool_size_overflow(self) -> GaugeMetric:
        return self._pool_size_overflow

    def start_span(
        self,
        name: str,
        *,
        attributes: Mapping[str, AttributeValue] | None = None,
    ) -> AbstractContextManager[Span]:
        return _SpanContextRecorder(self.spans, name, attributes)


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_transport_telemetry_records_nominal_metrics_and_spans(
    mtls_contexts: tuple[ssl.SSLContext, ssl.SSLContext],
) -> None:
    server_ssl, client_ssl = mtls_contexts
    telemetry = _TelemetryRecorder()

    async def handler(envelope: Envelope) -> AsyncIterator[Envelope]:
        yield Envelope(
            proto_version=envelope.proto_version,
            schema_id=envelope.schema_id,
            mode=Envelope.MODE_RPC,
            correlation_id=envelope.correlation_id,
            kind="gossip.ping",
            payload=b"pong",
        )

    server = await start_envelope_server(
        {"gossip.ping": handler},
        server_ssl,
        telemetry=telemetry,
    )
    reader, writer = await asyncio.open_connection(
        *server.endpoint,
        ssl=client_ssl,
        server_hostname="localhost",
    )

    request = Envelope(
        proto_version=1,
        schema_id=1,
        mode=Envelope.MODE_RPC,
        correlation_id=uuid4(),
        kind="gossip.ping",
        payload=b"hello",
    )
    writer.write(request.to_bytes())
    await writer.drain()
    await read_envelope(reader)

    writer.close()
    await writer.wait_closed()
    await server.stop()

    endpoint_key = _label_key({"endpoint": "server"})
    assert telemetry._connections_accepted_total.values[endpoint_key] == 1
    assert telemetry._connections_closed_total.values[endpoint_key] == 1
    assert telemetry._dispatched_total.values[endpoint_key] == 1
    assert telemetry._in_flight_current.values[endpoint_key] == 0
    span_names = [name for name, _ in telemetry.spans]
    assert "transport.connection" in span_names
    assert "transport.envelope" in span_names
    assert any(
        name == "transport.connection" and attributes.get("endpoint") == "server"
        for name, attributes in telemetry.spans
    )
    assert any(
        name == "transport.envelope" and attributes.get("endpoint") == "server"
        for name, attributes in telemetry.spans
    )


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_transport_telemetry_records_unknown_kind_metric(
    mtls_contexts: tuple[ssl.SSLContext, ssl.SSLContext],
) -> None:
    server_ssl, client_ssl = mtls_contexts
    telemetry = _TelemetryRecorder()
    server = await start_envelope_server({}, server_ssl, telemetry=telemetry)
    reader, writer = await asyncio.open_connection(
        *server.endpoint,
        ssl=client_ssl,
        server_hostname="localhost",
    )

    request = Envelope(
        proto_version=1,
        schema_id=1,
        mode=Envelope.MODE_RPC,
        correlation_id=uuid4(),
        kind="kv.get",
        payload=b"",
    )
    writer.write(request.to_bytes())
    await writer.drain()

    response = await read_envelope(reader)
    assert response.kind == TRANSPORT_ERROR_KIND

    writer.close()
    await writer.wait_closed()
    await server.stop()

    endpoint_key = _label_key({"endpoint": "server"})
    assert telemetry._unknown_kind_total.values[endpoint_key] == 1


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_transport_telemetry_records_handler_error_metric(
    mtls_contexts: tuple[ssl.SSLContext, ssl.SSLContext],
) -> None:
    server_ssl, client_ssl = mtls_contexts
    telemetry = _TelemetryRecorder()

    async def failing_handler(envelope: Envelope) -> AsyncIterator[Envelope]:
        if envelope.kind == "never":
            yield envelope
        raise RuntimeError("boom")

    server = await start_envelope_server(
        {"kv.put": failing_handler},
        server_ssl,
        telemetry=telemetry,
    )
    reader, writer = await asyncio.open_connection(
        *server.endpoint,
        ssl=client_ssl,
        server_hostname="localhost",
    )

    request = Envelope(
        proto_version=1,
        schema_id=1,
        mode=Envelope.MODE_RPC,
        correlation_id=uuid4(),
        kind="kv.put",
        payload=b"v",
    )
    writer.write(request.to_bytes())
    await writer.drain()

    response = await read_envelope(reader)
    assert response.kind == TRANSPORT_ERROR_KIND
    assert response.payload.decode("utf-8", errors="replace").startswith(
        "internal error"
    )

    writer.close()
    await writer.wait_closed()
    await server.stop()

    endpoint_key = _label_key({"endpoint": "server"})
    assert telemetry._protocol_errors_total.values[endpoint_key] == 1
    span_names = [name for name, _ in telemetry.spans]
    assert "transport.envelope.error" in span_names
