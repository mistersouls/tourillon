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

"""Data anti-entropy across real LMDB groups and the real peer handlers.

Each node owns its own storage; requests travel through the msgpack
serializer and the ``antientropy.*`` handlers via a loopback peer pool, so
the wire models, the handlers, the responder and the initiator are all
exercised together.
"""

import asyncio
import contextlib
from collections.abc import AsyncIterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from tourillon.bootstrap.handlers import antientropy as handlers
from tourillon.core.antientropy.service import (
    AntiEntropyService,
    PeerBackoff,
    Unit,
    initiates,
    plan,
)
from tourillon.core.antientropy.sync import (
    BudgetSpentError,
    PeerSync,
    SessionLimits,
    ViewChangedError,
)
from tourillon.core.antientropy.view import ReplicaView, build_view
from tourillon.core.exceptions import AntiEntropyRejectError
from tourillon.core.gossip.topology import TopologyManager
from tourillon.core.models.antientropy import Scope
from tourillon.core.models.config import AntiEntropyConfig
from tourillon.core.models.member import Member, MemberPhase
from tourillon.core.models.retry import BackoffRetry
from tourillon.core.models.storageschema import (
    Key,
    Keyspace,
    Pid,
    Record,
    Tag,
    TagKind,
    Value,
)
from tourillon.core.partition.partitioner import Partitioner
from tourillon.core.spaces.hashspace import HashSpace
from tourillon.core.storage.session import DigestCell, GroupSessionManager
from tourillon.infra.storage.lmdb_session import LmdbConfig, LmdbSessionProvider
from tourlib.adapter import MsgPackSerializer
from tourlib.envelope import Envelope, build_envelope
from tourlib.exceptions import ProtocolError
from tourlib.models.ts import HLCTimestamp
from tourlib.models.version import Address

pytestmark = pytest.mark.tourillon

SERIALIZER = MsgPackSerializer()
# 16 partitions in 2 groups over an 8-bit ring.
PARTITIONER = Partitioner(pshift=4, gshift=1, hashspace=HashSpace(bits=8))
RF = 2
NOW = 20_000.0  # seconds; the grace cutoff is computed from it
OLD_MS = 1_000_000  # bucket 0, well before the cutoff
RECENT_MS = 19_990_000  # within the grace window: never compared
TOKENS = {"node-a": ("10", "70"), "node-b": ("30", "b0"), "node-c": ("50", "e0")}


class _Storage:
    def __init__(self, root: Path) -> None:
        self._root = root
        self._providers: dict[int, LmdbSessionProvider] = {}

    async def session_provider_for(self, group: int) -> LmdbSessionProvider:
        provider = self._providers.get(group)

        if provider is None:
            self._root.mkdir(parents=True, exist_ok=True)
            provider = self._providers[group] = LmdbSessionProvider(
                config=LmdbConfig(path=self._root / f"g{group}", map_size=1 << 24),
                group=group,
            )

        return provider

    async def close(self) -> None:
        for provider in self._providers.values():
            await provider.close()


class _Network:
    """Loopback peer pool: requests run the real handlers of the target node."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.nodes: dict[str, AntiEntropyService] = {}
        self.down: set[str] = set()
        self.requests: list[tuple[str, str]] = []
        self._target: list[Any] = [None]
        monkeypatch.setattr(handlers, "get_manager", lambda: self._target[0])

    def pool(self) -> "_Pool":
        return _Pool(self)

    async def request(self, peer: str, envelope: Envelope) -> Envelope:
        if peer in self.down:
            raise OSError(f"{peer} unreachable")

        self.requests.append((peer, envelope.kind))

        self._target[0] = SimpleNamespace(
            serializer=SERIALIZER, antientropy=self.nodes[peer]
        )

        handler = _HANDLERS[envelope.kind]

        async for response in handler(envelope):
            return response

        raise AssertionError("handler yielded nothing")


_HANDLERS = {
    "antientropy.roots": handlers.roots,
    "antientropy.buckets": handlers.buckets,
    "antientropy.range": handlers.range_digest,
    "antientropy.keys": handlers.keys,
    "antientropy.fetch": handlers.fetch,
    "antientropy.push": handlers.push,
}


class _Client:
    def __init__(self, network: _Network, peer: str) -> None:
        self._network = network
        self._peer = peer

    async def request(self, envelope: Envelope, timeout: float) -> Envelope:
        return await self._network.request(self._peer, envelope)


class _Pool:
    def __init__(self, network: _Network) -> None:
        self._network = network
        self.suspects: set[str] = set()

    def failure_detector(self, peer: str) -> Any:
        if peer not in self.suspects:
            return None
        return SimpleNamespace(is_alive=lambda: False)

    @contextlib.asynccontextmanager
    async def lease(self, peer: str) -> AsyncIterator[_Client]:
        yield _Client(self._network, peer)


def _member(node_id: str, phase: MemberPhase = MemberPhase.READY) -> Member:
    return Member(
        node_id=node_id,
        peer_address="127.0.0.1:1",
        epoch=1,
        phase=phase,
        tokens=TOKENS[node_id],
    )


async def _topology(*members: Member) -> TopologyManager:
    manager = TopologyManager()
    await manager.merge_registry(members)

    return manager


def _record(index: int, wall_ms: int = OLD_MS, node: str = "w") -> Record:
    address = Address(namespace=b"ns", key=f"key-{index}".encode())
    pid = PARTITIONER.pid_for_hash(PARTITIONER.space.hash(address.to_bytes()))

    return Record(
        key=Key(
            pid=Pid(pid),
            address=address,
            ts=HLCTimestamp(wall_ms=wall_ms + index, counter=0, node_id=node),
        ),
        tag=Tag(kind=TagKind.LIVE),
        value=Value(data=f"value-{index}".encode(), quorum_write=1),
    )


def _records(count: int, wall_ms: int = OLD_MS) -> list[Record]:
    return [_record(i, wall_ms) for i in range(count)]


async def _write(service: AntiEntropyService, records: list[Record]) -> None:
    for record in records:
        await service.local.apply(record.key.pid.value, [record])


async def _owned(service: AntiEntropyService, pid: int) -> set[bytes]:
    start = pid.to_bytes(4, "big")
    stop = (pid + 1).to_bytes(4, "big")
    keys, cursor = await service.local.keys(start, stop, 1 << 14)
    assert cursor is None

    return {k.to_bytes(Keyspace.TAGS) for k in keys}


class _Cluster:
    def __init__(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        self._tmp_path = tmp_path
        self.network = _Network(monkeypatch)
        self.storages: list[_Storage] = []

    async def node(
        self,
        node_id: str,
        topology: TopologyManager,
        config: AntiEntropyConfig | None = None,
        *,
        serve: bool = True,
    ) -> AntiEntropyService:
        storage = _Storage(self._tmp_path / node_id)
        self.storages.append(storage)

        service = AntiEntropyService(
            node_id=node_id,
            storage=storage,  # type: ignore[arg-type]
            partitioner=PARTITIONER,
            topologymgr=topology,
            peer_pool=self.network.pool(),  # type: ignore[arg-type]
            serializer=SERIALIZER,
            replication_factor=RF,
            config=config or AntiEntropyConfig(grace=10.0, settle_delay=0.0),
            clock=lambda: NOW,
            backoff=BackoffRetry(initial_delay=0.0, max_delay=0.0, jitter=0.0),
        )

        self.network.nodes[node_id] = service
        service.tracker.start()
        await service.tracker.wait_ready()

        if serve:
            service.responder.start()

        return service

    async def close(self) -> None:
        for service in self.network.nodes.values():
            await service.stop()
        for storage in self.storages:
            await storage.close()


@pytest.fixture
async def cluster(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[_Cluster]:
    cluster = _Cluster(tmp_path, monkeypatch)
    try:
        yield cluster
    finally:
        await cluster.close()


async def _three(
    cluster: _Cluster, config: AntiEntropyConfig | None = None
) -> tuple[AntiEntropyService, AntiEntropyService, AntiEntropyService]:
    members = [_member(n) for n in TOKENS]

    nodes = [
        await cluster.node(m.node_id, await _topology(*members), config)
        for m in members
    ]

    return nodes[0], nodes[1], nodes[2]


def _view(service: AntiEntropyService) -> ReplicaView:
    view = service.tracker.view
    assert view is not None

    return view


def _shared_records(view: ReplicaView, peer: str, count: int = 64) -> list[Record]:
    shared = set(view.shared[peer])
    return [r for r in _records(count) if r.key.pid.value in shared]


# --- replica mode ----------------------------------------------------------


async def test_replica_sync_heals_both_sides_and_skips_recent_versions(
    cluster: _Cluster,
) -> None:
    a, b, _ = await _three(cluster)
    view = _view(a)
    shared = _shared_records(view, "node-b")
    assert len(shared) >= 4
    only_a, only_b = shared[::2], shared[1::2]

    recent = [
        r for r in _records(64, RECENT_MS) if r.key.pid.value in view.shared["node-b"]
    ]

    await _write(a, only_a + recent[:1])
    await _write(b, only_b)

    stale = a.tracker.stale_signal()
    await a.run_unit(Unit("node-b", view.shared["node-b"]), view, stale)

    for pid in {r.key.pid.value for r in shared}:
        assert await _owned(a, pid) - {
            k.key.to_bytes(Keyspace.TAGS) for k in recent
        } == (await _owned(b, pid))

    assert a.stats.pushed == len(only_a)
    assert a.stats.pulled == len(only_b)
    assert a.stats.pids_diverged >= 1

    # The version inside the grace window stays where it was written.
    assert recent[0].key.to_bytes(Keyspace.TAGS) not in await _owned(
        b, recent[0].key.pid.value
    )

    # A second pass finds nothing left to copy.
    cluster.network.requests.clear()
    await a.run_unit(Unit("node-b", view.shared["node-b"]), view, stale)
    assert a.stats.pushed == len(only_a) and a.stats.pulled == len(only_b)


async def test_replica_sync_paginates_keys_and_batches(cluster: _Cluster) -> None:
    config = AntiEntropyConfig(
        grace=10.0, settle_delay=0.0, keys_per_request=1, max_batch_bytes=1
    )

    a, b, _ = await _three(cluster, config)
    view = _view(a)
    shared = _shared_records(view, "node-b", 96)
    await _write(a, shared[::2])
    await _write(b, shared[1::2])

    await a.run_unit(
        Unit("node-b", view.shared["node-b"]), view, a.tracker.stale_signal()
    )

    assert a.stats.pushed + a.stats.pulled == len(shared)
    kinds = [kind for _, kind in cluster.network.requests]
    assert kinds.count("antientropy.fetch") == len(shared[1::2])
    assert kinds.count("antientropy.push") == len(shared[::2])


async def test_hot_bucket_compares_only_its_eligible_part(cluster: _Cluster) -> None:
    # Bucket 4 spans [16_777_216, 20_971_520) ms; the cutoff (NOW - grace) is
    # 19_990_000 ms, inside it.
    a, b, _ = await _three(cluster)
    view = _view(a)
    pid = view.shared["node-b"][0]
    old = [r for r in _records(200, 17_000_000) if r.key.pid.value == pid][:2]
    young = [r for r in _records(200, 19_995_000) if r.key.pid.value == pid][:1]
    assert old and young
    await _write(a, old + young)

    await a.run_unit(Unit("node-b", (pid,)), view, a.tracker.stale_signal())

    copied = await _owned(b, pid)
    assert {r.key.to_bytes(Keyspace.TAGS) for r in old} == copied
    assert ("node-b", "antientropy.range") in cluster.network.requests


async def test_pulled_versions_promote_a_staged_copy(cluster: _Cluster) -> None:
    a, b, _ = await _three(cluster)
    view = _view(a)
    record = _shared_records(view, "node-b")[0]
    pid = record.key.pid.value
    await _write(b, [record])

    manager = GroupSessionManager(
        await a.local._storage.session_provider_for(PARTITIONER.group_for(pid))
    )

    async with manager.session(write=True) as session:
        await session.stage_many([record], "src")

    assert await _owned(a, pid) == set()

    await a.run_unit(Unit("node-b", (pid,)), view, a.tracker.stale_signal())

    assert record.key.to_bytes(Keyspace.TAGS) in await _owned(a, pid)


# --- handoff mode ----------------------------------------------------------


async def test_handoff_pushes_orphans_to_every_replica_once(cluster: _Cluster) -> None:
    a, b, c = await _three(cluster)
    view = _view(c)
    orphan_pids = [p for p in range(16) if not view.is_replica(p)]
    assert orphan_pids
    orphans = [r for r in _records(96) if r.key.pid.value in orphan_pids][:6]
    assert orphans
    await _write(c, orphans)
    stale = c.tracker.stale_signal()

    await c.run_unit(Unit(None, span=(0, 16)), view, stale)

    nodes = {"node-a": a, "node-b": b}

    for record in orphans:
        raw = record.key.to_bytes(Keyspace.TAGS)
        pid = record.key.pid.value

        for replica in view.replicas[pid]:
            assert raw in await _owned(nodes[replica], pid)

        # Nothing is ever deleted locally; the GC owns reclamation.
        assert raw in await _owned(c, pid)

    assert c.stats.handed_off >= len(orphans)

    sessions = c.stats.sessions
    await c.run_unit(Unit(None, span=(0, 16)), view, stale)
    assert c.stats.sessions == sessions  # already handed off: skipped


async def test_handoff_never_pulls_and_retries_unreachable_replicas(
    cluster: _Cluster,
) -> None:
    a, b, c = await _three(cluster)
    view = _view(c)
    orphan = next(r for r in _records(96) if not view.is_replica(r.key.pid.value))
    pid = orphan.key.pid.value
    await _write(c, [orphan])

    replica_only = next(
        r for r in _records(200, OLD_MS + 50_000) if r.key.pid.value == pid
    )

    nodes = {"node-a": a, "node-b": b}

    for replica in view.replicas[pid]:
        await _write(nodes[replica], [replica_only])

    cluster.network.down.add(view.replicas[pid][0])

    await c.run_unit(Unit(None, span=(pid, pid + 1)), view, c.tracker.stale_signal())

    assert replica_only.key.to_bytes(Keyspace.TAGS) not in await _owned(c, pid)
    assert c.stats.errors == 1
    assert pid not in c._handed  # not every replica was reached

    cluster.network.down.clear()
    await c.run_unit(Unit(None, span=(pid, pid + 1)), view, c.tracker.stale_signal())
    assert pid in c._handed


async def test_handoff_retries_orphans_holding_versions_inside_grace(
    cluster: _Cluster,
) -> None:
    a, b, c = await _three(cluster)
    view = _view(c)
    old = next(r for r in _records(96) if not view.is_replica(r.key.pid.value))
    pid = old.key.pid.value
    young = next(r for r in _records(400, RECENT_MS) if r.key.pid.value == pid)
    await _write(c, [old, young])
    nodes = {"node-a": a, "node-b": b}

    await c.run_unit(Unit(None, span=(pid, pid + 1)), view, c.tracker.stale_signal())

    for replica in view.replicas[pid]:
        owned = await _owned(nodes[replica], pid)
        assert old.key.to_bytes(Keyspace.TAGS) in owned
        assert young.key.to_bytes(Keyspace.TAGS) not in owned

    # The root will not change once the young version ages: not marked yet.
    assert pid not in c._handed

    c._clock = lambda: NOW + 3600.0
    await c.run_unit(Unit(None, span=(pid, pid + 1)), view, c.tracker.stale_signal())

    for replica in view.replicas[pid]:
        assert young.key.to_bytes(Keyspace.TAGS) in await _owned(nodes[replica], pid)

    assert pid in c._handed


# --- refusals, backoff and cancellation -------------------------------------


async def test_responder_refusals_defer_the_peer(cluster: _Cluster) -> None:
    members = [_member(n) for n in TOKENS]
    a = await cluster.node("node-a", await _topology(*members))
    await cluster.node("node-b", await _topology(*members), serve=False)
    view = _view(a)
    record = _shared_records(view, "node-b")[0]
    await _write(a, [record])

    await a.run_unit(
        Unit("node-b", view.shared["node-b"]), view, a.tracker.stale_signal()
    )

    assert a.stats.rejects == {AntiEntropyRejectError.NOT_READY: 1}
    # settle_delay is 0 here, so the deferral is an immediate failure backoff.
    assert a.stats.last_failure is not None


async def test_stale_view_is_refused_when_rings_disagree(cluster: _Cluster) -> None:
    a = await cluster.node("node-a", await _topology(*(_member(n) for n in TOKENS)))
    await cluster.node("node-b", await _topology(_member("node-b")))
    view = _view(a)
    await _write(a, _shared_records(view, "node-b")[:1])

    await a.run_unit(
        Unit("node-b", view.shared["node-b"]), view, a.tracker.stale_signal()
    )

    assert a.stats.rejects.get(AntiEntropyRejectError.STALE_VIEW) == 1


async def test_ineligible_peers_are_skipped(cluster: _Cluster) -> None:
    members = [_member(n) for n in TOKENS]
    a = await cluster.node("node-a", await _topology(*members))
    view = _view(a)
    pool = a._pool
    pool.suspects.add("node-b")  # type: ignore[attr-defined]

    await a.run_unit(
        Unit("node-b", view.shared["node-b"]), view, a.tracker.stale_signal()
    )

    await a.run_unit(Unit("node-z", (0,)), view, a.tracker.stale_signal())

    assert a.stats.skipped == 2
    assert cluster.network.requests == []


async def test_session_stops_when_the_view_goes_stale(cluster: _Cluster) -> None:
    a, b, _ = await _three(cluster)
    view = _view(a)
    await _write(a, _shared_records(view, "node-b"))
    stale = asyncio.Event()
    stale.set()

    await a.run_unit(Unit("node-b", view.shared["node-b"]), view, stale)

    assert a.stats.view_changes == 1
    assert cluster.network.requests == []
    assert await a.sweep(view, stale) is False


async def test_budget_stops_the_session(cluster: _Cluster) -> None:
    config = AntiEntropyConfig(grace=10.0, settle_delay=0.0, max_repairs_per_cycle=1)
    a, b, _ = await _three(cluster, config)
    view = _view(a)
    await _write(a, _shared_records(view, "node-b"))

    await a.run_unit(
        Unit("node-b", view.shared["node-b"]), view, a.tracker.stale_signal()
    )

    assert a.stats.budget_stops == 1


async def test_scan_budget_counts_listed_keys(cluster: _Cluster) -> None:
    a, b, _ = await _three(cluster)
    view = _view(a)
    records = _shared_records(view, "node-b")
    await _write(b, records)

    sync = PeerSync(
        a.local,
        a._caller("node-b"),
        view,
        asyncio.Event(),
        SessionLimits(
            keys_per_request=1 << 20,  # clamped to one envelope page
            max_batch_bytes=1 << 40,  # clamped below the envelope payload
            max_repairs=10,
            max_keys_scanned=0,
        ),
        int((NOW - 10) * 1000),
    )

    with pytest.raises(BudgetSpentError):
        await sync.sync_roots(view.shared["node-b"])


async def test_rpc_refuses_once_stale() -> None:
    stale = asyncio.Event()
    stale.set()
    view = ReplicaView("n", "s", ((),))

    sync = PeerSync(
        None,  # type: ignore[arg-type]
        None,  # type: ignore[arg-type]
        view,
        stale,
        SessionLimits(1, 1, 1, 1),
        0,
    )

    with pytest.raises(ViewChangedError):
        await sync._rpc("antientropy.roots", {})


async def test_sweep_walks_every_unit(cluster: _Cluster) -> None:
    config = AntiEntropyConfig(
        grace=10.0, settle_delay=0.0, sweep_period=1.0, min_interval=0.01, jitter=0.0
    )

    a, b, c = await _three(cluster, config)
    view = _view(a)

    assert await a.sweep(view, a.tracker.stale_signal()) is True
    assert a.stats.sweeps == 1


async def test_service_loop_starts_sweeps_and_stops(cluster: _Cluster) -> None:
    config = AntiEntropyConfig(
        grace=10.0, settle_delay=0.0, sweep_period=1.0, min_interval=0.01
    )

    members = [_member(n) for n in TOKENS]
    a = await cluster.node("node-a", await _topology(*members), config)
    await a.start()
    assert a.running
    await a.start()  # idempotent

    for _ in range(200):
        if a.stats.sweeps:
            break
        await asyncio.sleep(0.01)

    assert a.stats.sweeps >= 1
    snapshot = a.to_dict()
    assert snapshot["running"] is True and snapshot["serial"]
    await a.stop()
    assert not a.running
    assert a.to_dict()["serial"] is None


async def test_disabled_service_never_starts(cluster: _Cluster) -> None:
    members = [_member(n) for n in TOKENS]

    a = await cluster.node(
        "node-a", await _topology(*members), AntiEntropyConfig(enabled=False)
    )

    await a.start()
    assert not a.running


async def test_loop_survives_a_failing_sweep(
    cluster: _Cluster, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = AntiEntropyConfig(grace=10.0, settle_delay=0.0, min_interval=0.01)

    a = await cluster.node(
        "node-a", await _topology(*(_member(n) for n in TOKENS)), config
    )

    calls = 0

    async def broken(view: ReplicaView, stale: asyncio.Event) -> bool:
        nonlocal calls
        calls += 1

        raise RuntimeError("boom")

    monkeypatch.setattr(a, "sweep", broken)
    await a.start()

    for _ in range(200):
        if calls >= 2:
            break
        await asyncio.sleep(0.01)

    await a.stop()
    assert calls >= 2
    assert a.stats.errors >= 2 and a.stats.last_failure == "RuntimeError: boom"


# --- tracker ----------------------------------------------------------------


async def test_tracker_invalidates_then_rebuilds_on_ring_change(
    cluster: _Cluster,
) -> None:
    topology = await _topology(_member("node-a"), _member("node-b"))
    a = await cluster.node("node-a", topology)
    first = _view(a)
    stale = a.tracker.stale_signal()
    assert "node-c" not in {n for r in first.replicas for n in r}

    await topology.apply_member(_member("node-c"))
    await asyncio.wait_for(stale.wait(), 2)
    view = await asyncio.wait_for(a.tracker.wait_ready(), 2)

    assert view.serial != first.serial
    assert any("node-c" in r for r in view.replicas)
    assert not a.tracker.stale_signal().is_set()


async def test_build_view_matches_the_replica_walk() -> None:
    topology = await _topology(*(_member(n) for n in TOKENS))
    snapshot = await topology.snapshot()
    view = await build_view(snapshot, "s", PARTITIONER, RF, "node-a")
    assert all(len(r) == RF and len(set(r)) == RF for r in view.replicas)

    for peer, pids in view.shared.items():
        assert all(view.is_replica(p) and view.is_replica(p, peer) for p in pids)

    assert view.covers(range(16)) and not view.covers([16])
    assert view.view_hash([1, 2]) != view.view_hash([2, 1])

    empty = await build_view(
        (await TopologyManager().snapshot()), "s", PARTITIONER, RF, "node-a"
    )

    assert empty.replicas == ((),) * 16 and empty.shared == {}


# --- handlers ----------------------------------------------------------------


async def test_handler_refuses_malformed_payloads(cluster: _Cluster) -> None:
    await cluster.node("node-a", await _topology(*(_member(n) for n in TOKENS)))
    for kind, payload in (
        ("antientropy.roots", b"\xc1"),
        ("antientropy.keys", SERIALIZER.encode([1, 2])),
        ("antientropy.fetch", SERIALIZER.encode({"scope": {}})),
    ):
        envelope = build_envelope(
            kind=kind, payload=payload, schema_id=SERIALIZER.schema_id
        )

        response = await cluster.network.request("node-a", envelope)
        assert response.kind == f"{kind}.reject"
        reject = AntiEntropyRejectError.from_dict(SERIALIZER.decode(response.payload))
        assert reject.code == AntiEntropyRejectError.INVALID


async def test_unexpected_response_kind_is_a_protocol_error(cluster: _Cluster) -> None:
    a = await cluster.node("node-a", await _topology(*(_member(n) for n in TOKENS)))

    response = build_envelope(
        kind="other.kind", payload=SERIALIZER.encode({}), schema_id=1
    )

    with pytest.raises(ProtocolError):
        a._expect(response, "antientropy.roots")

    accepted = build_envelope(
        kind="antientropy.roots.accept", payload=SERIALIZER.encode([1]), schema_id=1
    )

    assert a._expect(accepted, "antientropy.roots") == {}


# --- responder admission ------------------------------------------------------


async def test_responder_refuses_beyond_capacity_and_drains_on_stop(
    cluster: _Cluster,
) -> None:
    config = AntiEntropyConfig(grace=10.0, settle_delay=0.0, concurrency=1)

    a = await cluster.node(
        "node-a", await _topology(*(_member(n) for n in TOKENS)), config
    )

    view = _view(a)
    pid = view.shared["node-b"][0]
    scope = Scope((pid,), view.view_hash((pid,)))
    release = asyncio.Event()

    async def hold() -> int:
        await release.wait()
        return 1

    busy = asyncio.create_task(a.responder.serve(scope, hold))
    await asyncio.sleep(0)

    with pytest.raises(AntiEntropyRejectError) as exc:
        await a.responder.serve(scope, hold)

    assert exc.value.code == AntiEntropyRejectError.BUSY and exc.value.retry_after > 0

    stopping = asyncio.create_task(a.responder.stop())
    await asyncio.sleep(0)
    assert not stopping.done()
    release.set()
    assert await busy == 1
    await stopping


async def test_responder_rejects_unknown_and_foreign_partitions(
    cluster: _Cluster,
) -> None:
    a = await cluster.node("node-a", await _topology(*(_member(n) for n in TOKENS)))
    view = _view(a)

    async def work() -> None:
        return None

    with pytest.raises(AntiEntropyRejectError) as exc:
        await a.responder.serve(Scope((99,), b"\x00" * 16), work)

    assert exc.value.code == AntiEntropyRejectError.INVALID

    foreign = next(p for p in range(16) if not view.is_replica(p))

    with pytest.raises(AntiEntropyRejectError) as exc:
        await a.responder.serve(Scope((foreign,), view.view_hash((foreign,))), work)

    assert exc.value.code == AntiEntropyRejectError.NOT_REPLICA
    assert a.responder.rejected == 2


# --- local store ----------------------------------------------------------------


async def test_local_store_pages_roots_buckets_and_flags_foreign(
    cluster: _Cluster,
) -> None:
    a = await cluster.node("node-a", await _topology(*(_member(n) for n in TOKENS)))
    records = _records(64) + _records(8, 9_000_000)
    await _write(a, records)
    pids = sorted({r.key.pid.value for r in records})

    page, cursor = await a.local.root_page(None, 2)
    assert [pid for pid, _ in page] == pids[:2] and cursor == pids[1]
    collected: list[int] = [pid for pid, _ in page]

    while cursor is not None:
        page, cursor = await a.local.root_page(cursor, 3)
        collected.extend(pid for pid, _ in page)

    assert collected == pids

    pid = records[-1].key.pid.value
    buckets = await a.local.buckets_until(pid, None, None, 1)
    assert 2 in buckets

    assert await a.local.buckets_until(pid, None, 0, 1) == {
        b: c for b, c in buckets.items() if b <= 0
    }

    roots = await a.local.roots([pid])
    total = DigestCell()

    for cell in buckets.values():
        total = total + cell

    assert roots[pid] == total

    record = records[0]
    assert not a.local.foreign(record)

    moved = Record(
        key=Key(
            pid=Pid((record.key.pid.value + 1) % 16),
            address=record.key.address,
            ts=record.key.ts,
        ),
        tag=record.tag,
        value=record.value,
    )

    assert a.local.foreign(moved)


async def test_local_read_bounds_batches_and_always_progresses(
    cluster: _Cluster,
) -> None:
    a = await cluster.node("node-a", await _topology(*(_member(n) for n in TOKENS)))
    pid = _records(64)[0].key.pid.value
    records = [r for r in _records(64) if r.key.pid.value == pid]
    await _write(a, records)
    keys = [r.key for r in records]
    missing = _record(999).key

    found, consumed = await a.local.read(pid, [missing] + keys, 1)
    assert consumed == 2 and found == [records[0]]
    found, consumed = await a.local.read(pid, keys, 1 << 30)
    assert consumed == len(keys) and found == records


# --- scheduling helpers -----------------------------------------------------------


def test_exactly_one_side_of_a_pair_initiates() -> None:
    names = [f"node-{i}" for i in range(20)]

    for a in names:
        for b in names:
            if a != b:
                assert initiates(a, b) != initiates(b, a)

    first = [initiates("node-0", n) for n in names[1:]]
    assert any(first) and not all(first)


def test_plan_cuts_shared_pids_and_the_pid_space() -> None:
    peers = {"p1": (0, 1, 2, 3, 4), "p2": (5,)}
    view = ReplicaView("me", "s", ((),) * 10, peers)
    units = plan(view, 10, 2)
    handoff = [u.span for u in units if u.peer is None]
    assert handoff == [(0, 2), (2, 4), (4, 6), (6, 8), (8, 10)]

    for peer, pids in peers.items():
        mine = [u.pids for u in units if u.peer == peer]
        if initiates("me", peer):
            assert sum(mine, ()) == pids and all(len(p) <= 2 for p in mine)
        else:
            assert mine == []


def test_peer_backoff_honours_hints_and_grows() -> None:
    now = [0.0]

    backoff = PeerBackoff(
        BackoffRetry(initial_delay=1.0, factor=2.0, max_delay=3.0, jitter=0.0),
        lambda: now[0],
    )

    assert not backoff.deferred("p")
    backoff.defer("p", 5.0)
    backoff.defer("p", 1.0)  # never shortens a longer deferral
    now[0] = 4.0
    assert backoff.deferred("p")
    now[0] = 5.0
    assert not backoff.deferred("p")
    backoff.failed("p")
    assert backoff.deferred("p")
    now[0] = 6.0
    assert not backoff.deferred("p")
    backoff.failed("p")
    backoff.failed("p")
    assert backoff._delay["p"] == 3.0
    backoff.succeeded("p")
    assert not backoff.deferred("p") and "p" not in backoff._delay
