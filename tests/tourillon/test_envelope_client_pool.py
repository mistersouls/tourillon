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
from typing import cast

import pytest

from tourillon.core.gossip.topology import TopologyManager
from tourillon.core.peer.pool import PeerPool
from tourillon.core.ports.telemetry import Telemetry
from tourillon.core.probe.detector import FailureDetectorProvider
from tourillon.core.telemetry import build_noop_telemetry
from tourlib.adapter import MsgPackSerializer


class _FakeEnvelopeClient:
    created: list["_FakeEnvelopeClient"] = []

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
        self.is_connected = False
        self.connect_delay = 0.0
        self.connect_calls = 0
        self.close_calls = 0
        self._last_activity = time.monotonic()
        _FakeEnvelopeClient.created.append(self)

    async def connect(self) -> None:
        self.connect_calls += 1
        if self.connect_delay > 0:
            await asyncio.sleep(self.connect_delay)
        self.is_connected = True
        self._last_activity = time.monotonic()

    async def close(self) -> None:
        self.close_calls += 1
        self.is_connected = False

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


class _FakeDetector:
    def __init__(self, peer_id: str) -> None:
        self.peer_id = peer_id
        self.stop_calls = 0

    async def run(self, _client: object) -> None:
        return None

    async def stop(self) -> None:
        self.stop_calls += 1


class _FakeDetectorProvider:
    def __init__(self) -> None:
        self.created: list[_FakeDetector] = []

    def create(self, peer_id: str) -> _FakeDetector:
        detector = _FakeDetector(peer_id)
        self.created.append(detector)
        return detector


def _as_topologymgr(stub: _TopologyManagerStub) -> TopologyManager:
    return cast(TopologyManager, cast(object, stub))


@pytest.fixture
def tls_ctx() -> ssl.SSLContext:
    return ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)


@pytest.fixture
def telemetry() -> Telemetry:
    return build_noop_telemetry()


@pytest.fixture
def fd_provider() -> FailureDetectorProvider:
    return FailureDetectorProvider(serializer=MsgPackSerializer())


@pytest.fixture(autouse=True)
def fake_client(monkeypatch: pytest.MonkeyPatch) -> None:
    _FakeEnvelopeClient.created.clear()
    monkeypatch.setattr("tourillon.core.peer.pool.PeerClient", _FakeEnvelopeClient)


async def _acquire_fake_client(
    pool: PeerPool,
    peer: str,
) -> _FakeEnvelopeClient:
    client = await pool.acquire(peer)
    assert isinstance(client, _FakeEnvelopeClient)
    return client


def _connect_calls(client: _FakeEnvelopeClient) -> int:
    return client.connect_calls


def _close_calls(client: _FakeEnvelopeClient) -> int:
    return client.close_calls


@pytest.mark.tourillon
def test_envelope_client_pool_requires_positive_max_size(
    tls_ctx: ssl.SSLContext,
    telemetry: Telemetry,
    fd_provider: FailureDetectorProvider,
) -> None:
    topologymgr = _TopologyManagerStub()
    with pytest.raises(ValueError, match="max_size must be >= 1"):
        PeerPool(tls_ctx=tls_ctx, topologymgr=_as_topologymgr(topologymgr), fd_provider=fd_provider, max_size=0, telemetry=telemetry)

    with pytest.raises(ValueError, match="idle_ttl must be > 0"):
        PeerPool(tls_ctx=tls_ctx, topologymgr=_as_topologymgr(topologymgr), fd_provider=fd_provider, idle_ttl=0, telemetry=telemetry)


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_acquire_fails_for_unknown_peer(
    tls_ctx: ssl.SSLContext,
    telemetry: Telemetry,
    fd_provider: FailureDetectorProvider,
) -> None:
    topologymgr = _TopologyManagerStub()
    pool = PeerPool(tls_ctx=tls_ctx, topologymgr=_as_topologymgr(topologymgr), fd_provider=fd_provider, telemetry=telemetry)

    with pytest.raises(RuntimeError, match="unknown peer"):
        await pool.acquire("peer-z")


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_acquire_creates_and_reuses_client(
    tls_ctx: ssl.SSLContext,
    telemetry: Telemetry,
    fd_provider: FailureDetectorProvider,
) -> None:
    pool = PeerPool(
        tls_ctx=tls_ctx,
        topologymgr=_as_topologymgr(_TopologyManagerStub()),
        fd_provider=fd_provider,
        telemetry=telemetry,
    )

    first = await _acquire_fake_client(pool, "peer-a")
    second = await _acquire_fake_client(pool, "peer-a")

    assert first is second
    assert _connect_calls(first) == 1


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_acquire_reuses_existing_client_without_reconnecting(
    tls_ctx: ssl.SSLContext,
    telemetry: Telemetry,
    fd_provider: FailureDetectorProvider,
) -> None:
    pool = PeerPool(
        tls_ctx=tls_ctx,
        topologymgr=_as_topologymgr(_TopologyManagerStub()),
        fd_provider=fd_provider,
        telemetry=telemetry,
    )

    client = await _acquire_fake_client(pool, "peer-a")
    client.is_connected = False

    same_client = await _acquire_fake_client(pool, "peer-a")

    assert same_client is client
    assert _connect_calls(client) == 1


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_acquire_recreates_client_when_bind_changes(
    tls_ctx: ssl.SSLContext,
    telemetry: Telemetry,
    fd_provider: FailureDetectorProvider,
) -> None:
    topologymgr = _TopologyManagerStub()
    pool = PeerPool(tls_ctx=tls_ctx, topologymgr=_as_topologymgr(topologymgr), fd_provider=fd_provider, telemetry=telemetry)

    old_client = await _acquire_fake_client(pool, "peer-a")
    topologymgr.binds["peer-a"] = "127.0.0.1:5000"
    new_client = await _acquire_fake_client(pool, "peer-a")

    assert new_client is not old_client
    assert _close_calls(old_client) == 1
    assert new_client.bind == "127.0.0.1:5000"


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_acquire_evicts_lru_when_max_size_is_reached(
    tls_ctx: ssl.SSLContext,
    telemetry: Telemetry,
    fd_provider: FailureDetectorProvider,
) -> None:
    pool = PeerPool(
        tls_ctx=tls_ctx,
        topologymgr=_as_topologymgr(_TopologyManagerStub()),
        fd_provider=fd_provider,
        max_size=1,
        max_overflow=0,
        telemetry=telemetry,
    )

    old_client = await _acquire_fake_client(pool, "peer-a")
    new_client = await _acquire_fake_client(pool, "peer-b")

    assert new_client.bind == "127.0.0.1:5000"
    assert _close_calls(old_client) == 1

    again = await pool.acquire("peer-a")
    assert again.bind == "127.0.0.1:4000"


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_acquire_uses_ephemeral_client_when_pool_is_full_and_all_clients_are_busy(
    tls_ctx: ssl.SSLContext,
    telemetry: Telemetry,
    fd_provider: FailureDetectorProvider,
) -> None:
    pool = PeerPool(
        tls_ctx=tls_ctx,
        topologymgr=_as_topologymgr(_TopologyManagerStub()),
        fd_provider=fd_provider,
        max_size=1,
        max_overflow=1,
        telemetry=telemetry,
    )
    pooled_client = await _acquire_fake_client(pool, "peer-a")

    fallback_client = await _acquire_fake_client(pool, "peer-b")

    assert fallback_client is not pooled_client
    assert "peer-b" in pool._overflows
    assert _connect_calls(fallback_client) == 1


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_acquire_fails_when_pool_is_full_and_busy_without_fallback(
    tls_ctx: ssl.SSLContext,
    telemetry: Telemetry,
    fd_provider: FailureDetectorProvider,
) -> None:
    pool = PeerPool(
        tls_ctx=tls_ctx,
        topologymgr=_as_topologymgr(_TopologyManagerStub()),
        fd_provider=fd_provider,
        max_size=1,
        max_overflow=0,
        telemetry=telemetry,
    )
    await pool.acquire("peer-a")
    old_client = await _acquire_fake_client(pool, "peer-a")

    new_client = await _acquire_fake_client(pool, "peer-b")

    assert _close_calls(old_client) == 1
    assert new_client is not old_client


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_acquire_refreshes_expired_idle_client(
    tls_ctx: ssl.SSLContext,
    telemetry: Telemetry,
    fd_provider: FailureDetectorProvider,
) -> None:
    pool = PeerPool(
        tls_ctx=tls_ctx,
        topologymgr=_as_topologymgr(_TopologyManagerStub()),
        fd_provider=fd_provider,
        min_size=0,
        idle_ttl=0.01,
        telemetry=telemetry,
    )

    old_client = await _acquire_fake_client(pool, "peer-a")
    await pool.release("peer-a")
    await asyncio.sleep(0.02)
    await pool._remove_expired()
    new_client = await _acquire_fake_client(pool, "peer-a")

    assert new_client is not old_client
    assert _close_calls(old_client) == 1


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_acquire_applies_connection_timeout(
    tls_ctx: ssl.SSLContext,
    telemetry: Telemetry,
    fd_provider: FailureDetectorProvider,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool = PeerPool(
        tls_ctx=tls_ctx,
        topologymgr=_as_topologymgr(_TopologyManagerStub()),
        fd_provider=fd_provider,
        connection_timeout=0.01,
        telemetry=telemetry,
    )

    def delayed_client(
        bind: str,
        tls_ctx: ssl.SSLContext,
        fd: object | None = None,
        logger: object | None = None,
    ) -> _FakeEnvelopeClient:
        client = _FakeEnvelopeClient(bind=bind, tls_ctx=tls_ctx, fd=fd, logger=logger)
        client.connect_delay = 0.1
        return client

    monkeypatch.setattr("tourillon.core.peer.pool.PeerClient", delayed_client)

    with pytest.raises(asyncio.TimeoutError):
        await pool.acquire("peer-a")


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_release_and_close_all_close_clients(
    tls_ctx: ssl.SSLContext,
    telemetry: Telemetry,
    fd_provider: FailureDetectorProvider,
) -> None:
    pool = PeerPool(
        tls_ctx=tls_ctx,
        topologymgr=_as_topologymgr(_TopologyManagerStub()),
        fd_provider=fd_provider,
        max_size=1,
        max_overflow=1,
        telemetry=telemetry,
    )

    first = await _acquire_fake_client(pool, "peer-a")
    second = await _acquire_fake_client(pool, "peer-b")

    # overflow is closed on release when no user keeps a reference.
    await pool.release("peer-a")
    await pool.release("peer-b")
    assert _close_calls(first) == 0
    assert _close_calls(second) == 1

    await pool.shutdown()
    assert _close_calls(first) == 1
    assert pool._permanents == {}
    assert pool._overflows == {}


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_lease_acquires_and_releases_client(
    tls_ctx: ssl.SSLContext,
    telemetry: Telemetry,
    fd_provider: FailureDetectorProvider,
) -> None:
    pool = PeerPool(
        tls_ctx=tls_ctx,
        topologymgr=_as_topologymgr(_TopologyManagerStub()),
        fd_provider=fd_provider,
        telemetry=telemetry,
    )

    async with pool.lease("peer-a") as client:
        assert isinstance(client, _FakeEnvelopeClient)
        assert pool._permanents["peer-a"].refcount == 1

    assert pool._permanents["peer-a"].refcount == 0


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_lease_releases_even_when_context_raises(
    tls_ctx: ssl.SSLContext,
    telemetry: Telemetry,
    fd_provider: FailureDetectorProvider,
) -> None:
    pool = PeerPool(
        tls_ctx=tls_ctx,
        topologymgr=_as_topologymgr(_TopologyManagerStub()),
        fd_provider=fd_provider,
        telemetry=telemetry,
    )

    with pytest.raises(RuntimeError, match="boom"):
        async with pool.lease("peer-a"):
            raise RuntimeError("boom")

    assert pool._permanents["peer-a"].refcount == 0


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_acquire_uses_single_detector_instance_per_peer(
    tls_ctx: ssl.SSLContext,
    telemetry: Telemetry,
) -> None:
    provider = _FakeDetectorProvider()
    pool = PeerPool(
        tls_ctx=tls_ctx,
        topologymgr=_as_topologymgr(_TopologyManagerStub()),
        fd_provider=cast(FailureDetectorProvider, cast(object, provider)),
        telemetry=telemetry,
    )

    client = await _acquire_fake_client(pool, "peer-a")
    detector = pool.failure_detector("peer-a")

    assert len(provider.created) == 1
    assert detector is provider.created[0]
    assert client.fd is detector


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_release_stops_detector_when_overflow_peer_is_removed(
    tls_ctx: ssl.SSLContext,
    telemetry: Telemetry,
) -> None:
    provider = _FakeDetectorProvider()
    pool = PeerPool(
        tls_ctx=tls_ctx,
        topologymgr=_as_topologymgr(_TopologyManagerStub()),
        fd_provider=cast(FailureDetectorProvider, cast(object, provider)),
        max_size=1,
        max_overflow=1,
        telemetry=telemetry,
    )

    await _acquire_fake_client(pool, "peer-a")
    await _acquire_fake_client(pool, "peer-b")
    overflow_fd = pool.failure_detector("peer-b")
    assert overflow_fd is not None

    await pool.release("peer-b")

    assert isinstance(overflow_fd, _FakeDetector)
    assert overflow_fd.stop_calls == 1


