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
import time
from collections import defaultdict
from collections.abc import Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from typing import cast

import pytest

import tourillon.core.peer.pool as pool_module
from tourillon.core.gossip.topology import TopologyManager
from tourillon.core.peer.pool import PeerPool
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
from tourillon.core.probe.detector import FailureDetectorProvider
from tourlib.adapter import MsgPackSerializer

# ---------------------------------------------------------------------------
# Fake EnvelopeClient
# ---------------------------------------------------------------------------


class _FakeClient:
    created: list["_FakeClient"] = []

    def __init__(
        self,
        bind: str,
        tls_ctx: ssl.SSLContext,
        fd: object | None = None,
        logger: object | None = None,
    ) -> None:
        self.bind = bind
        self.tls_ctx = tls_ctx
        self.fd = fd
        self.logger = logger
        self.connect_delay = 0.0
        self.connect_calls = 0
        self.close_calls = 0
        self._last_activity = time.monotonic()
        _FakeClient.created.append(self)

    async def connect(self) -> None:
        self.connect_calls += 1
        if self.connect_delay > 0:
            await asyncio.sleep(self.connect_delay)
        self._last_activity = time.monotonic()

    async def close(self) -> None:
        self.close_calls += 1

    @property
    def inactivity_duration(self) -> float:
        return time.monotonic() - self._last_activity


class _MemberStub:
    def __init__(self, peer_address: str) -> None:
        self.peer_address = peer_address


class _RegistryStub:
    def __init__(self, binds: dict[str, str]) -> None:
        self._binds = binds

    def get(self, node_id: str) -> _MemberStub | None:
        bind = self._binds.get(node_id)
        if bind is None:
            return None
        return _MemberStub(bind)


class _TopologyStub:
    def __init__(self, binds: dict[str, str]) -> None:
        self.registry = _RegistryStub(binds)


class _TopologyManagerStub:
    def __init__(self) -> None:
        self.binds: dict[str, str] = {
            "peer-a": "127.0.0.1:4000",
            "peer-b": "127.0.0.1:5000",
            "peer-c": "127.0.0.1:6000",
        }

    async def snapshot(self) -> _TopologyStub:
        return _TopologyStub(self.binds)


def _as_topologymgr(stub: _TopologyManagerStub) -> TopologyManager:
    return cast(TopologyManager, cast(object, stub))


# ---------------------------------------------------------------------------
# Telemetry recorder
# ---------------------------------------------------------------------------


def _label_key(
    attributes: Mapping[str, AttributeValue] | None = None,
) -> tuple[tuple[str, AttributeValue], ...]:
    if not attributes:
        return ()
    return tuple(sorted(attributes.items()))


@dataclass(slots=True)
class _BoundCounter(BoundCounter):
    metric: "_CounterRecorder"
    labels: dict[str, AttributeValue]

    def inc(self, amount: int | float = 1) -> None:
        self.metric.add(amount, attributes=self.labels)


@dataclass(slots=True)
class _BoundGauge(BoundGauge):
    metric: "_GaugeRecorder"
    labels: dict[str, AttributeValue]

    def set(self, value: int | float) -> None:
        self.metric.set(value, attributes=self.labels)


class _CounterRecorder(CounterMetric):
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
        return _BoundCounter(metric=self, labels=dict(labels))


class _GaugeRecorder(GaugeMetric):
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
        return _BoundGauge(metric=self, labels=dict(labels))


@dataclass
class _SpanRecorder(Span):
    attributes: dict[str, AttributeValue] = field(default_factory=dict)
    exceptions: list[BaseException] = field(default_factory=list)
    error_descriptions: list[str] = field(default_factory=list)

    def set_attribute(self, key: str, value: AttributeValue) -> None:
        self.attributes[key] = value

    def record_exception(self, exc: BaseException) -> None:
        self.exceptions.append(exc)

    def set_status_error(self, description: str = "") -> None:
        self.error_descriptions.append(description)


class _SpanCtx(AbstractContextManager[Span]):
    def __init__(
        self,
        store: list[tuple[str, dict[str, AttributeValue]]],
        name: str,
        attributes: Attributes | None,
    ) -> None:
        self._store = store
        self._name = name
        self._attributes = dict(attributes or {})
        self._span = _SpanRecorder()

    def __enter__(self) -> Span:
        self._store.append((self._name, self._attributes))
        return self._span

    def __exit__(self, exc_type, exc, tb) -> bool | None:
        return None


class _TelemetryRecorder(Telemetry):
    def __init__(self) -> None:
        self._protocol_errors_total = _CounterRecorder()
        self._connections_accepted_total = _CounterRecorder()
        self._connections_closed_total = _CounterRecorder()
        self._dispatched_total = _CounterRecorder()
        self._unknown_kind_total = _CounterRecorder()
        self._in_flight_current = _GaugeRecorder()
        self._pool_connections_total = _CounterRecorder()
        self._pool_evictions_total = _CounterRecorder()
        self._pool_promotions_total = _CounterRecorder()
        self._pool_acquire_errors_total = _CounterRecorder()
        self._pool_size_permanent = _GaugeRecorder()
        self._pool_size_overflow = _GaugeRecorder()
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
        return _SpanCtx(self.spans, name, attributes)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def tls_ctx() -> ssl.SSLContext:
    return ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)


@pytest.fixture(autouse=True)
def fake_client(monkeypatch: pytest.MonkeyPatch) -> None:
    _FakeClient.created.clear()
    monkeypatch.setattr(pool_module, "PeerClient", _FakeClient)


@pytest.fixture
def fd_provider() -> FailureDetectorProvider:
    return FailureDetectorProvider(serializer=MsgPackSerializer())


@pytest.fixture
def topologymgr() -> _TopologyManagerStub:
    return _TopologyManagerStub()


# ---------------------------------------------------------------------------
# Tests – metrics
# ---------------------------------------------------------------------------


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_pool_telemetry_records_new_permanent_connection(
    tls_ctx: ssl.SSLContext,
    fd_provider: FailureDetectorProvider,
    topologymgr: _TopologyManagerStub,
) -> None:
    telemetry = _TelemetryRecorder()
    pool = PeerPool(
        tls_ctx=tls_ctx,
        topologymgr=_as_topologymgr(topologymgr),
        fd_provider=fd_provider,
        telemetry=telemetry,
    )

    await pool.acquire("peer-a")

    key = _label_key({"peer_id": "peer-a", "type": "permanent"})
    assert telemetry._pool_connections_total.values[key] == 1
    assert telemetry._pool_size_permanent.values[()] == 1


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_pool_telemetry_records_new_overflow_connection(
    tls_ctx: ssl.SSLContext,
    fd_provider: FailureDetectorProvider,
    topologymgr: _TopologyManagerStub,
) -> None:
    telemetry = _TelemetryRecorder()
    pool = PeerPool(
        tls_ctx=tls_ctx,
        topologymgr=_as_topologymgr(topologymgr),
        fd_provider=fd_provider,
        max_size=1,
        max_overflow=1,
        telemetry=telemetry,
    )

    await pool.acquire("peer-a")
    await pool.acquire("peer-b")

    key = _label_key({"peer_id": "peer-b", "type": "overflow"})
    assert telemetry._pool_connections_total.values[key] == 1
    assert telemetry._pool_size_overflow.values[()] == 1


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_pool_telemetry_records_lru_eviction(
    tls_ctx: ssl.SSLContext,
    fd_provider: FailureDetectorProvider,
    topologymgr: _TopologyManagerStub,
) -> None:
    telemetry = _TelemetryRecorder()
    pool = PeerPool(
        tls_ctx=tls_ctx,
        topologymgr=_as_topologymgr(topologymgr),
        fd_provider=fd_provider,
        max_size=1,
        max_overflow=0,
        telemetry=telemetry,
    )

    await pool.acquire("peer-a")
    await pool.acquire("peer-b")

    evict_key = _label_key({"peer_id": "peer-a", "reason": "lru_evict"})
    assert telemetry._pool_evictions_total.values[evict_key] == 1


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_pool_telemetry_records_overflow_release_eviction(
    tls_ctx: ssl.SSLContext,
    fd_provider: FailureDetectorProvider,
    topologymgr: _TopologyManagerStub,
) -> None:
    telemetry = _TelemetryRecorder()
    pool = PeerPool(
        tls_ctx=tls_ctx,
        topologymgr=_as_topologymgr(topologymgr),
        fd_provider=fd_provider,
        max_size=1,
        max_overflow=1,
        telemetry=telemetry,
    )

    await pool.acquire("peer-a")
    await pool.acquire("peer-b")
    await pool.release("peer-b")

    evict_key = _label_key({"peer_id": "peer-b", "reason": "overflow_release"})
    assert telemetry._pool_evictions_total.values[evict_key] == 1
    assert telemetry._pool_size_overflow.values[()] == 0


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_pool_telemetry_records_idle_expired_eviction(
    tls_ctx: ssl.SSLContext,
    fd_provider: FailureDetectorProvider,
    topologymgr: _TopologyManagerStub,
) -> None:
    telemetry = _TelemetryRecorder()
    pool = PeerPool(
        tls_ctx=tls_ctx,
        topologymgr=_as_topologymgr(topologymgr),
        fd_provider=fd_provider,
        min_size=0,
        idle_ttl=0.01,
        telemetry=telemetry,
    )

    await pool.acquire("peer-a")
    await pool.release("peer-a")
    await asyncio.sleep(0.02)
    await pool._remove_expired()

    evict_key = _label_key({"peer_id": "peer-a", "reason": "idle_expired"})
    assert telemetry._pool_evictions_total.values[evict_key] == 1
    assert telemetry._pool_size_permanent.values[()] == 0


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_pool_telemetry_records_promotion(
    tls_ctx: ssl.SSLContext,
    fd_provider: FailureDetectorProvider,
    topologymgr: _TopologyManagerStub,
) -> None:
    telemetry = _TelemetryRecorder()
    pool = PeerPool(
        tls_ctx=tls_ctx,
        topologymgr=_as_topologymgr(topologymgr),
        fd_provider=fd_provider,
        min_size=0,
        max_size=2,
        max_overflow=1,
        idle_ttl=0.01,
        telemetry=telemetry,
    )

    # fill permanents
    await pool.acquire("peer-a")
    await pool.acquire("peer-b")
    # add overflow
    await pool.acquire("peer-c")

    # release peer-a and let it expire so a permanent slot opens up
    await pool.release("peer-a")
    await asyncio.sleep(0.02)
    await pool._remove_expired()
    await pool._try_promote_overflow()

    promo_key = _label_key({"peer_id": "peer-c"})
    assert telemetry._pool_promotions_total.values[promo_key] == 1
    assert telemetry._pool_size_overflow.values[()] == 0


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_pool_telemetry_records_acquire_error_on_timeout(
    tls_ctx: ssl.SSLContext,
    fd_provider: FailureDetectorProvider,
    monkeypatch: pytest.MonkeyPatch,
    topologymgr: _TopologyManagerStub,
) -> None:
    telemetry = _TelemetryRecorder()
    pool = PeerPool(
        tls_ctx=tls_ctx,
        topologymgr=_as_topologymgr(topologymgr),
        fd_provider=fd_provider,
        connection_timeout=0.01,
        telemetry=telemetry,
    )

    def slow_client(
        bind: str,
        tls_ctx: ssl.SSLContext,
        fd: object | None = None,
        logger: object | None = None,
    ) -> _FakeClient:
        c = _FakeClient(bind=bind, tls_ctx=tls_ctx, fd=fd, logger=logger)
        c.connect_delay = 1.0
        return c

    monkeypatch.setattr(pool_module, "PeerClient", slow_client)

    with pytest.raises(asyncio.TimeoutError):
        await pool.acquire("peer-a")

    error_key = _label_key({"peer_id": "peer-a"})
    assert telemetry._pool_acquire_errors_total.values[error_key] == 1


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_pool_telemetry_records_refresh_eviction_and_new_connection(
    tls_ctx: ssl.SSLContext,
    fd_provider: FailureDetectorProvider,
    topologymgr: _TopologyManagerStub,
) -> None:
    telemetry = _TelemetryRecorder()
    pool = PeerPool(
        tls_ctx=tls_ctx,
        topologymgr=_as_topologymgr(topologymgr),
        fd_provider=fd_provider,
        telemetry=telemetry,
    )

    await pool.acquire("peer-a")
    topologymgr.binds["peer-a"] = "127.0.0.1:5000"
    await pool.acquire("peer-a")

    evict_key = _label_key({"peer_id": "peer-a", "reason": "refresh"})
    conn_key = _label_key({"peer_id": "peer-a", "type": "permanent"})
    assert telemetry._pool_evictions_total.values[evict_key] == 1
    assert telemetry._pool_connections_total.values[conn_key] == 2


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_pool_telemetry_records_pool_close_evictions(
    tls_ctx: ssl.SSLContext,
    fd_provider: FailureDetectorProvider,
    topologymgr: _TopologyManagerStub,
) -> None:
    telemetry = _TelemetryRecorder()
    pool = PeerPool(
        tls_ctx=tls_ctx,
        topologymgr=_as_topologymgr(topologymgr),
        fd_provider=fd_provider,
        max_size=2,
        max_overflow=1,
        telemetry=telemetry,
    )

    await pool.acquire("peer-a")
    await pool.acquire("peer-b")
    await pool.acquire("peer-c")
    await pool.shutdown()

    for peer_id in ("peer-a", "peer-b", "peer-c"):
        key = _label_key({"peer_id": peer_id, "reason": "pool_close"})
        assert telemetry._pool_evictions_total.values[key] == 1

    assert telemetry._pool_size_permanent.values[()] == 0
    assert telemetry._pool_size_overflow.values[()] == 0


# ---------------------------------------------------------------------------
# Tests – spans
# ---------------------------------------------------------------------------


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_pool_telemetry_acquire_emits_span(
    tls_ctx: ssl.SSLContext,
    fd_provider: FailureDetectorProvider,
    topologymgr: _TopologyManagerStub,
) -> None:
    telemetry = _TelemetryRecorder()
    pool = PeerPool(
        tls_ctx=tls_ctx,
        topologymgr=_as_topologymgr(topologymgr),
        fd_provider=fd_provider,
        telemetry=telemetry,
    )

    await pool.acquire("peer-a")

    span_names = [name for name, _ in telemetry.spans]
    assert "pool.acquire" in span_names
    assert any(
        name == "pool.acquire" and attrs.get("peer_id") == "peer-a"
        for name, attrs in telemetry.spans
    )


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_pool_telemetry_acquire_span_records_error_on_timeout(
    tls_ctx: ssl.SSLContext,
    fd_provider: FailureDetectorProvider,
    monkeypatch: pytest.MonkeyPatch,
    topologymgr: _TopologyManagerStub,
) -> None:
    telemetry = _TelemetryRecorder()
    pool = PeerPool(
        tls_ctx=tls_ctx,
        topologymgr=_as_topologymgr(topologymgr),
        fd_provider=fd_provider,
        connection_timeout=0.01,
        telemetry=telemetry,
    )

    def slow_client(
        bind: str,
        tls_ctx: ssl.SSLContext,
        fd: object | None = None,
        logger: object | None = None,
    ) -> _FakeClient:
        c = _FakeClient(bind=bind, tls_ctx=tls_ctx, fd=fd, logger=logger)
        c.connect_delay = 1.0
        return c

    monkeypatch.setattr(pool_module, "PeerClient", slow_client)

    with pytest.raises(asyncio.TimeoutError):
        await pool.acquire("peer-a")

    # The span should have been opened (name recorded)
    span_names = [name for name, _ in telemetry.spans]
    assert "pool.acquire" in span_names
