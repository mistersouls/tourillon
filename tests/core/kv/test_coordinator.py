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
"""Unit tests for KvCoordinator — write/read/delete paths with in-memory adapters."""

from __future__ import annotations

import asyncio

import pytest

from tourillon.core.kv.coordinator import (
    KvCoordinator,
    KvError,
    _group_versions,
)
from tourillon.core.lifecycle.member import Member, MemberPhase
from tourillon.core.lifecycle.probe import ProbeManager
from tourillon.core.ring.hashspace import HashSpace
from tourillon.core.ring.partitioner import Partitioner
from tourillon.core.ring.placement import PreferenceEntry, SimplePreferenceStrategy
from tourillon.core.ring.topology import TopologyManager
from tourillon.core.structure.clock import HLCTimestamp
from tourillon.core.structure.envelope import Envelope
from tourillon.core.structure.record import KvMetadata, StoreKey, Tombstone, Version
from tourillon.core.testing.mem_storage import InMemoryStorage
from tourillon.infra.serializer.msgpack import MsgpackSerializerAdapter

pytestmark = pytest.mark.kv

_SHIFT = 4  # 16 partitions — keeps tests lean


# ---------------------------------------------------------------------------
# Mock helpers
# ---------------------------------------------------------------------------


class _MockClient:
    """Fake TcpClient that returns a pre-canned response per request kind."""

    def __init__(self, responses: dict[str, tuple[str, bytes]]) -> None:
        # responses: {request_kind → (response_kind, payload_bytes)}
        self._responses = responses
        self.calls: list[str] = []

    async def request(self, env: Envelope, timeout: float = 30.0) -> Envelope:
        self.calls.append(env.kind)
        resp_kind, payload = self._responses[env.kind]
        return Envelope.create(
            payload, kind=resp_kind, correlation_id=env.correlation_id
        )


class _ErrorClient:
    """Fake TcpClient that always raises TimeoutError."""

    async def request(self, env: Envelope, timeout: float = 30.0) -> Envelope:
        raise TimeoutError("simulated timeout")


class _MockPool:
    """Fake PeerClientPool keyed by node_id."""

    def __init__(self, clients: dict[str, object]) -> None:
        self._clients = clients

    async def acquire(self, node_id: str, address: str) -> object:
        return self._clients[node_id]

    async def close_all(self) -> None:
        pass


class _MockStrategy:
    """Strategy stub that returns a fixed preference list."""

    def __init__(self, entries: list[PreferenceEntry]) -> None:
        self._entries = entries

    async def preference_list(
        self, placement, topology, probe_manager
    ) -> list[PreferenceEntry]:
        return self._entries


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


async def _build_1node(
    node_id: str = "node-1",
) -> tuple[KvCoordinator, InMemoryStorage]:
    """Return a KvCoordinator backed by InMemoryStorage with one READY local node."""
    hs = HashSpace()
    partitioner = Partitioner(hs, _SHIFT)
    storage = InMemoryStorage()
    serializer = MsgpackSerializerAdapter()
    topology_mgr = TopologyManager()
    probe_mgr = ProbeManager()

    member = Member(
        node_id=node_id,
        peer_address="127.0.0.1:0",
        generation=1,
        seq=1,
        phase=MemberPhase.READY,
        tokens=(hs.max // 2,),
        partition_shift=_SHIFT,
    )
    await topology_mgr.apply_member(member)

    strategy = SimplePreferenceStrategy(rf=1)
    pool = _MockPool({})

    coord = KvCoordinator(
        node_id=node_id,
        storage=storage,
        partitioner=partitioner,
        topology_manager=topology_mgr,
        probe_manager=probe_mgr,
        placement_strategy=strategy,
        pool=pool,
        serializer=serializer,
        fanout_timeout=0.05,
    )
    return coord, storage


async def _build_with_strategy(
    node_id: str,
    pref_entries: list[PreferenceEntry],
    pool: _MockPool,
) -> tuple[KvCoordinator, InMemoryStorage]:
    """Return a coordinator with a mock strategy and pool for multi-node tests."""
    hs = HashSpace()
    partitioner = Partitioner(hs, _SHIFT)
    storage = InMemoryStorage()
    serializer = MsgpackSerializerAdapter()
    topology_mgr = TopologyManager()
    probe_mgr = ProbeManager()

    # Register local node in ring so placement_for_token works.
    # Also register every remote node_id (primary and handoff) referenced in
    # pref_entries so _peer_address() can look them up by node_id.
    for nid in _all_node_ids(node_id, pref_entries):
        await topology_mgr.apply_member(
            Member(
                node_id=nid,
                peer_address="127.0.0.1:0",
                generation=1,
                seq=1,
                phase=MemberPhase.READY,
                tokens=(hs.max // 2,) if nid == node_id else (),
                partition_shift=_SHIFT,
            )
        )

    strategy = _MockStrategy(pref_entries)

    coord = KvCoordinator(
        node_id=node_id,
        storage=storage,
        partitioner=partitioner,
        topology_manager=topology_mgr,
        probe_manager=probe_mgr,
        placement_strategy=strategy,
        pool=pool,
        serializer=serializer,
        fanout_timeout=0.05,
    )
    return coord, storage


def _all_node_ids(local: str, entries: list[PreferenceEntry]) -> list[str]:
    """Collect every node_id (primary + handoff) from the preference list."""
    ids: list[str] = [local]
    for e in entries:
        if e.node_id not in ids:
            ids.append(e.node_id)
        if e.handoff is not None and e.handoff not in ids:
            ids.append(e.handoff)
    return ids


# ---------------------------------------------------------------------------
# Scenario 1 — kv.put 1 node, W=1, stores COMMITTED version
# ---------------------------------------------------------------------------


@pytest.mark.kv
async def test_1_kvput_1node_rf1_w1_stores_committed_version() -> None:
    """kv.put.ok; replica stores COMMITTED version."""
    coord, storage = await _build_1node()
    key = b"user:001"
    ks = b"default"
    val = b"\x93\xa5Alice"  # msgpack str "Alice"

    result = await coord.put(key, ks, val, quorum_write=1)

    assert result["replicas"] == 1
    assert "hlc" in result

    addr = StoreKey(keyspace=ks, key=key)
    pid = coord._partitioner.pid_for_addr(addr)
    record = await storage.open_partition(pid).get(addr)
    assert record is not None
    assert isinstance(record, Version)
    assert record.value == val


# ---------------------------------------------------------------------------
# Scenario 2 — kv.get returns case-1 confirmed after put
# ---------------------------------------------------------------------------


@pytest.mark.kv
async def test_2_kvget_1node_rf1_r1_confirmed_after_put() -> None:
    """case 1 response, confirmed=true, correct value."""
    coord, storage = await _build_1node()
    key = b"user:001"
    ks = b"default"
    val = b"\xa5Alice"

    await coord.put(key, ks, val, quorum_write=1)
    result = await coord.get(key, ks, quorum_read=1)

    versions = result["versions"]
    assert len(versions) == 1
    v = versions[0]
    assert v["confirmed"] is True
    assert v["value"] == val
    assert v["count"] == 1


# ---------------------------------------------------------------------------
# Scenario 3 — quorum write partial failure → KvError
# ---------------------------------------------------------------------------


@pytest.mark.kv
async def test_3_kvput_quorum_failure_raises_kv_error() -> None:
    """wait fanout_timeout_ms; return kv.error if only 1 ack but W=2."""
    # node-2 always times out
    pool = _MockPool({"node-2": _ErrorClient()})
    pref = [
        PreferenceEntry(node_id="node-1", readable=True, suspect=False, handoff=None),
        PreferenceEntry(node_id="node-2", readable=True, suspect=False, handoff=None),
    ]
    coord, _ = await _build_with_strategy("node-1", pref, pool)

    with pytest.raises(KvError, match="quorum_write_unavailable"):
        await coord.put(b"k", b"default", b"v", quorum_write=2)


# ---------------------------------------------------------------------------
# Scenario 4 — 3-node RF=3 W=2: 2 acks → kv.put.ok
# ---------------------------------------------------------------------------


@pytest.mark.kv
async def test_4_kvput_3node_w2_two_acks_succeeds() -> None:
    """coordinator fans out to 3 replicas; 2 ack → kv.put.ok."""
    serializer = MsgpackSerializerAdapter()
    ok_payload = serializer.encode({})
    ok_client = _MockClient({"kv.replicate": ("kv.replicate.ok", ok_payload)})
    timeout_client = _ErrorClient()
    pool = _MockPool({"node-2": ok_client, "node-3": timeout_client})

    pref = [
        PreferenceEntry(node_id="node-1", readable=True, suspect=False, handoff=None),
        PreferenceEntry(node_id="node-2", readable=True, suspect=False, handoff=None),
        PreferenceEntry(node_id="node-3", readable=True, suspect=False, handoff=None),
    ]
    coord, storage = await _build_with_strategy("node-1", pref, pool)

    result = await coord.put(b"k", b"default", b"v", quorum_write=2)
    assert result["replicas"] == 2  # node-1 + node-2


# ---------------------------------------------------------------------------
# Scenario 5 — suspect replica → hint routed to handoff
# ---------------------------------------------------------------------------


@pytest.mark.kv
async def test_5_kvput_suspect_replica_routes_hint_to_handoff() -> None:
    """fanout sends kv.hint to handoff node; kv.hint.ok counts for quorum."""
    serializer = MsgpackSerializerAdapter()
    hint_ok = serializer.encode({})
    hint_client = _MockClient({"kv.hint": ("kv.hint.ok", hint_ok)})
    pool = _MockPool({"node-3": hint_client})

    pref = [
        PreferenceEntry(node_id="node-1", readable=True, suspect=False, handoff=None),
        PreferenceEntry(
            node_id="node-2", readable=True, suspect=True, handoff="node-3"
        ),
    ]
    coord, storage = await _build_with_strategy("node-1", pref, pool)

    result = await coord.put(b"key", b"default", b"val", quorum_write=2)
    assert result["replicas"] == 2
    assert "kv.hint" in hint_client.calls


# ---------------------------------------------------------------------------
# Scenario 6 — node down, only 1 ack, quorum fail
# ---------------------------------------------------------------------------


@pytest.mark.kv
async def test_6_kvput_1_of_2_acks_quorum_fail() -> None:
    """return kv.error if only 1 ack but W=2."""
    pool = _MockPool({"node-2": _ErrorClient()})
    pref = [
        PreferenceEntry(node_id="node-1", readable=True, suspect=False, handoff=None),
        PreferenceEntry(node_id="node-2", readable=True, suspect=False, handoff=None),
    ]
    coord, _ = await _build_with_strategy("node-1", pref, pool)

    with pytest.raises(KvError, match="quorum_write_unavailable"):
        await coord.put(b"k", b"default", b"v", quorum_write=2)


# ---------------------------------------------------------------------------
# Scenario 7 — read repair triggered toward stale replica
# ---------------------------------------------------------------------------


@pytest.mark.kv
async def test_7_kvget_read_repair_triggered_for_stale_replica() -> None:
    """count(V_latest)=1 >= W=1 → case 1; read repair triggered async toward stale replica."""
    serializer = MsgpackSerializerAdapter()
    # Use quorum_write=1 so each version is "confirmed" with 1 count
    fresh_hlc = HLCTimestamp(wall_ms=2000, counter=0, node_id="node-1")
    stale_hlc = HLCTimestamp(wall_ms=1000, counter=0, node_id="node-2")
    addr = StoreKey(keyspace=b"default", key=b"k7")

    fetch_ok_stale = serializer.encode(
        {
            "found": True,
            "value": b"old",
            "hlc": stale_hlc.to_dict(),
            "quorum_write": 1,
        }
    )
    replicate_ok = serializer.encode({})
    node2_client = _MockClient(
        {
            "kv.fetch": ("kv.fetch.ok", fetch_ok_stale),
            "kv.replicate": ("kv.replicate.ok", replicate_ok),
        }
    )
    pool = _MockPool({"node-2": node2_client})
    pref = [
        PreferenceEntry(node_id="node-1", readable=True, suspect=False, handoff=None),
        PreferenceEntry(node_id="node-2", readable=True, suspect=False, handoff=None),
    ]
    coord, storage = await _build_with_strategy("node-1", pref, pool)

    # Pre-populate local store with fresh version (quorum_write=1)
    pid = coord._partitioner.pid_for_addr(addr)
    await storage.open_partition(pid).put(
        addr, b"v", KvMetadata(hlc=fresh_hlc, quorum_write=1)
    )

    result = await coord.get(b"k7", b"default", quorum_read=2)

    versions = result["versions"]
    assert len(versions) == 1
    assert versions[0]["confirmed"] is True
    assert versions[0]["value"] == b"v"

    # Allow read-repair background task to run
    await asyncio.sleep(0.1)
    assert "kv.replicate" in node2_client.calls


# ---------------------------------------------------------------------------
# Scenario 8 — phantom write: count < quorum_write → case 2
# ---------------------------------------------------------------------------


@pytest.mark.kv
async def test_8_phantom_write_returns_case2_uncertain() -> None:
    """count(V_phantom)=1 < W=2 → case 2, all candidates confirmed=false."""
    serializer = MsgpackSerializerAdapter()
    phantom_hlc = HLCTimestamp(wall_ms=5000, counter=0, node_id="node-2")
    _addr = StoreKey(keyspace=b"default", key=b"k8")

    # node-2 returns a phantom version (only 1 copy, W=2)
    fetch_ok_phantom = serializer.encode(
        {
            "found": True,
            "value": b"phantom",
            "hlc": phantom_hlc.to_dict(),
            "quorum_write": 2,
        }
    )
    node2_client = _MockClient({"kv.fetch": ("kv.fetch.ok", fetch_ok_phantom)})
    # node-1 has no record (None)
    pool = _MockPool({"node-2": node2_client})
    pref = [
        PreferenceEntry(node_id="node-1", readable=True, suspect=False, handoff=None),
        PreferenceEntry(node_id="node-2", readable=True, suspect=False, handoff=None),
    ]
    coord, storage = await _build_with_strategy("node-1", pref, pool)

    result = await coord.get(b"k8", b"default", quorum_read=2)

    # node-1 returns None, node-2 returns phantom
    # phantom has count=1 < quorum_write=2 → not confirmed
    versions = result["versions"]
    assert all(not v["confirmed"] for v in versions)


# ---------------------------------------------------------------------------
# Scenario 18 — coordinator is itself a replica target → no loopback call
# ---------------------------------------------------------------------------


@pytest.mark.kv
async def test_18_coordinator_is_own_replica_writes_directly() -> None:
    """coordinator writes directly to local store; no loopback network call."""
    coord, storage = await _build_1node("node-1")
    never_pool = _MockPool({})  # get() would raise KeyError if called
    coord._pool = never_pool  # type: ignore[assignment]

    await coord.put(b"self-key", b"default", b"self-val", quorum_write=1)

    addr = StoreKey(keyspace=b"default", key=b"self-key")
    pid = coord._partitioner.pid_for_addr(addr)
    record = await storage.open_partition(pid).get(addr)
    assert record is not None
    assert isinstance(record, Version)


# ---------------------------------------------------------------------------
# Scenario 19 — kv.delete W=1, confirmed Tombstone acks
# ---------------------------------------------------------------------------


@pytest.mark.kv
async def test_19_kvdelete_w1_stores_confirmed_tombstone() -> None:
    """kv.delete.ok; confirmed tombstone stored."""
    coord, storage = await _build_1node()

    # First put
    await coord.put(b"user:001", b"default", b"Alice", quorum_write=1)

    result = await coord.delete(b"user:001", b"default", quorum_write=1)
    assert result["replicas"] == 1

    addr = StoreKey(keyspace=b"default", key=b"user:001")
    pid = coord._partitioner.pid_for_addr(addr)
    record = await storage.open_partition(pid).get(addr)
    assert isinstance(record, Tombstone)


# ---------------------------------------------------------------------------
# Scenario 20 — kv.get after delete returns confirmed tombstone
# ---------------------------------------------------------------------------


@pytest.mark.kv
async def test_20_kvget_after_delete_confirmed_tombstone() -> None:
    """case 1, value=null, confirmed=true."""
    coord, storage = await _build_1node()

    await coord.put(b"user:001", b"default", b"Alice", quorum_write=1)
    await coord.delete(b"user:001", b"default", quorum_write=1)

    result = await coord.get(b"user:001", b"default", quorum_read=1)
    versions = result["versions"]
    assert len(versions) == 1
    v = versions[0]
    assert v["confirmed"] is True
    assert v["value"] is None


# ---------------------------------------------------------------------------
# get returns empty versions when key not found
# ---------------------------------------------------------------------------


@pytest.mark.kv
async def test_get_not_found_returns_empty_versions() -> None:
    """All replicas returned absence → versions=[]."""
    coord, _ = await _build_1node()
    result = await coord.get(b"nonexistent", b"default", quorum_read=1)
    assert result["versions"] == []


# ---------------------------------------------------------------------------
# KV convergence — two concurrent writes, highest HLC confirmed
# ---------------------------------------------------------------------------


@pytest.mark.kv
async def test_kv_convergence_highest_hlc_wins_when_confirmed() -> None:
    """Two concurrent writes on the same key converge to the highest-HLC version."""
    hlc_old = HLCTimestamp(wall_ms=1000, counter=0, node_id="n1")
    hlc_new = HLCTimestamp(wall_ms=2000, counter=0, node_id="n2")
    addr = StoreKey(keyspace=b"default", key=b"x")

    rec_old = Version(address=addr, metadata=hlc_old, value=b"old", quorum_write=2)
    rec_new = Version(address=addr, metadata=hlc_new, value=b"new", quorum_write=2)

    # Both confirmed (count=2 each) — highest HLC wins
    candidates = _group_versions([rec_old, rec_new, rec_old, rec_new])
    assert len(candidates) == 1
    assert candidates[0].hlc == hlc_new
    assert candidates[0].confirmed is True


@pytest.mark.kv
async def test_kv_convergence_tombstone_beats_older_version() -> None:
    """A tombstone must beat any older version in confirmed-check."""
    hlc_version = HLCTimestamp(wall_ms=1000, counter=0, node_id="n1")
    hlc_tomb = HLCTimestamp(wall_ms=2000, counter=0, node_id="n1")
    addr = StoreKey(keyspace=b"default", key=b"y")

    old_ver = Version(address=addr, metadata=hlc_version, value=b"v", quorum_write=1)
    tombstone = Tombstone(address=addr, metadata=hlc_tomb, quorum_write=1)

    # Both confirmed individually, but highest HLC (tombstone) wins
    candidates = _group_versions([old_ver, tombstone])
    assert len(candidates) == 1
    assert isinstance(candidates[0].record, Tombstone)


# ---------------------------------------------------------------------------
# quorum_read_unavailable → KvError
# ---------------------------------------------------------------------------


@pytest.mark.kv
async def test_get_quorum_read_unavailable_raises_kv_error() -> None:
    """fewer than quorum_read replicas responded → KvError raised."""
    pool = _MockPool({"node-2": _ErrorClient()})
    pref = [
        PreferenceEntry(node_id="node-2", readable=True, suspect=False, handoff=None),
    ]
    coord, _ = await _build_with_strategy("node-x", pref, pool)

    # 1 target queried → 1 response (None due to timeout); quorum_read=2 requires 2
    with pytest.raises(KvError, match="quorum_read_unavailable"):
        await coord.get(b"k", b"default", quorum_read=2)


# ---------------------------------------------------------------------------
# KvConfig.validate() — scenario 16
# ---------------------------------------------------------------------------


@pytest.mark.kv
def test_16_kv_config_fanout_below_minimum_raises_value_error() -> None:
    """node refuses to start with config error when fanout_timeout_ms < 10."""
    from tourillon.core.structure.config import KvConfig

    with pytest.raises(ValueError, match="fanout_timeout_ms must be"):
        KvConfig(fanout_timeout_ms=5).validate()


@pytest.mark.kv
def test_kv_config_fanout_at_minimum_is_valid() -> None:
    """fanout_timeout_ms == 10 is the boundary — should not raise."""
    from tourillon.core.structure.config import KvConfig

    KvConfig(fanout_timeout_ms=10).validate()  # must not raise
