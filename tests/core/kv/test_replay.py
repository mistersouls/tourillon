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
"""Unit tests for HintReplayManager — stabilisation, replay, phase-guard."""

from __future__ import annotations

import asyncio

import pytest

from tourillon.core.kv.replay import HintReplayManager
from tourillon.core.lifecycle.member import Member, MemberPhase
from tourillon.core.lifecycle.probe import ProbeManager
from tourillon.core.ring.hashspace import HashSpace
from tourillon.core.ring.partitioner import Partitioner
from tourillon.core.ring.topology import TopologyManager
from tourillon.core.structure.clock import HLCTimestamp
from tourillon.core.structure.envelope import Envelope
from tourillon.core.structure.record import KvMetadata, StoreKey
from tourillon.core.testing.mem_storage import InMemoryStorage
from tourillon.infra.serializer.msgpack import MsgpackSerializerAdapter

pytestmark = pytest.mark.kv

_SHIFT = 4  # 16 partitions


# ---------------------------------------------------------------------------
# Mock helpers shared by replay tests
# ---------------------------------------------------------------------------


class _MockReplayClient:
    """Returns kv.replicate.ok for every kv.replicate request."""

    def __init__(self) -> None:
        self.replicate_calls: int = 0

    async def request(self, env: Envelope, timeout: float = 30.0) -> Envelope:
        if env.kind == "kv.replicate":
            self.replicate_calls += 1
            serializer = MsgpackSerializerAdapter()
            payload = serializer.encode({})
            return Envelope.create(
                payload, kind="kv.replicate.ok", correlation_id=env.correlation_id
            )
        raise RuntimeError(f"Unexpected kind: {env.kind}")


class _TimeoutReplayClient:
    """Always times out."""

    async def request(self, env: Envelope, timeout: float = 30.0) -> Envelope:
        raise TimeoutError("simulated timeout")


class _MockPool:
    def __init__(self, client: object) -> None:
        self._client = client

    async def acquire(self, node_id: str, address: str) -> object:
        return self._client

    async def close_all(self) -> None:
        pass


# ---------------------------------------------------------------------------
# Helper — build a replay manager with a pre-populated hint
# ---------------------------------------------------------------------------


def _build_manager(
    node_id: str,
    target_id: str,
    pool: _MockPool,
    stabilization_ms: int = 0,
) -> tuple[HintReplayManager, InMemoryStorage, Partitioner]:
    hs = HashSpace()
    partitioner = Partitioner(hs, _SHIFT)
    storage = InMemoryStorage()
    serializer = MsgpackSerializerAdapter()
    probe_mgr = ProbeManager()
    topology_mgr = TopologyManager()

    mgr = HintReplayManager(
        node_id=node_id,
        storage=storage,
        partitioner=partitioner,
        pool=pool,
        probe_manager=probe_mgr,
        serializer=serializer,
        topology_manager=topology_mgr,
        stabilization_ms=stabilization_ms,
        concurrency=4,
        fanout_timeout=0.05,
    )
    return mgr, storage, partitioner, topology_mgr


async def _put_hint(
    storage: InMemoryStorage,
    partitioner: Partitioner,
    target_id: str,
    addr: StoreKey,
    val: bytes,
    hlc: HLCTimestamp,
) -> None:
    """Pre-populate a hint record for target_id in the in-memory storage."""
    pid = partitioner.pid_for_addr(addr)
    hint_ctx = storage.open_partition(pid).hint(target_id)
    meta = KvMetadata(hlc=hlc, quorum_write=1)
    await hint_ctx.put(addr, val, meta)


async def _register_ready_member(topology: TopologyManager, node_id: str) -> None:
    """Register node_id as READY in topology so replay can look up peer_address."""
    member = Member(
        node_id=node_id,
        peer_address="127.0.0.1:0",
        generation=1,
        seq=1,
        phase=MemberPhase.READY,
        tokens=(0,),
        partition_shift=_SHIFT,
    )
    await topology.apply_member(member)


# ---------------------------------------------------------------------------
# Scenario 9 — hint replay: phase READY → replay sends kv.replicate, marks stale
# ---------------------------------------------------------------------------


@pytest.mark.kv
async def test_9_hint_replay_sends_replicate_and_marks_stale() -> None:
    """HintReplayManager sends kv.replicate to node_id; tag updated \\x02→\\x04."""
    target_id = "node-2"
    addr = StoreKey(keyspace=b"default", key=b"replay-key")
    hlc = HLCTimestamp(wall_ms=1000, counter=0, node_id="node-1")

    replay_client = _MockReplayClient()
    pool = _MockPool(replay_client)
    mgr, storage, partitioner, topology_mgr = _build_manager(
        "node-1", target_id, pool, stabilization_ms=0
    )
    await _register_ready_member(topology_mgr, target_id)
    await _put_hint(storage, partitioner, target_id, addr, b"val", hlc)

    mgr.on_phase_change(target_id, MemberPhase.READY)
    await asyncio.sleep(0.2)  # let stabilisation (0ms) + replay complete

    assert replay_client.replicate_calls >= 1, "Expected kv.replicate to be sent"

    # Verify hint is now stale (mark_stale was called)
    pid = partitioner.pid_for_addr(addr)
    hint_ctx = storage.open_partition(pid).hint(target_id)
    assert hlc in hint_ctx._stale_hlcs


# ---------------------------------------------------------------------------
# Scenario 17 — hints persist across restart
# ---------------------------------------------------------------------------


@pytest.mark.kv
async def test_17_hints_are_rediscovered_after_manager_recreated() -> None:
    """On restart, re-scan DBI_KEYS for \\x02+node_id; pending hints discovered."""
    target_id = "node-3"
    addr = StoreKey(keyspace=b"default", key=b"persist-key")
    hlc = HLCTimestamp(wall_ms=2000, counter=1, node_id="node-1")

    replay_client = _MockReplayClient()
    pool = _MockPool(replay_client)
    mgr, storage, partitioner, _ = _build_manager(
        "node-1", target_id, pool, stabilization_ms=0
    )

    await _put_hint(storage, partitioner, target_id, addr, b"data", hlc)

    # Simulate restart: create a new manager that shares the same storage
    probe_mgr = ProbeManager()
    topology_mgr2 = TopologyManager()
    await _register_ready_member(topology_mgr2, target_id)
    mgr2 = HintReplayManager(
        node_id="node-1",
        storage=storage,
        partitioner=partitioner,
        pool=pool,
        probe_manager=probe_mgr,
        serializer=MsgpackSerializerAdapter(),
        topology_manager=topology_mgr2,
        stabilization_ms=0,
        concurrency=4,
        fanout_timeout=0.05,
    )

    # Trigger replay on new manager
    mgr2.on_phase_change(target_id, MemberPhase.READY)
    await asyncio.sleep(0.2)

    assert replay_client.replicate_calls >= 1


# ---------------------------------------------------------------------------
# Phase guard — non-READY cancels stabilisation
# ---------------------------------------------------------------------------


@pytest.mark.kv
async def test_non_ready_phase_cancels_stabilisation_task() -> None:
    """on_phase_change non-READY cancels any pending session."""
    target_id = "node-4"
    replay_client = _MockReplayClient()
    pool = _MockPool(replay_client)
    mgr, storage, partitioner, _ = _build_manager(
        "node-1", target_id, pool, stabilization_ms=500
    )

    # Start stabilisation with a 500ms delay
    mgr.on_phase_change(target_id, MemberPhase.READY)
    assert target_id in mgr._tasks

    # Cancel before it fires
    mgr.on_phase_change(target_id, MemberPhase.DRAINING)
    assert target_id not in mgr._tasks

    # Give event loop time — no replicate should have been sent
    await asyncio.sleep(0.1)
    assert replay_client.replicate_calls == 0


# ---------------------------------------------------------------------------
# Scenario 10 — node enters DRAINING during replay → session abandoned
# ---------------------------------------------------------------------------


@pytest.mark.kv
async def test_10_draining_during_session_cancels_task() -> None:
    """session abandoned; DRAINING; hints preserved (tag unchanged)."""
    target_id = "node-5"
    addr = StoreKey(keyspace=b"default", key=b"drain-key")
    hlc = HLCTimestamp(wall_ms=3000, counter=0, node_id="node-1")

    replay_client = _MockReplayClient()
    pool = _MockPool(replay_client)
    mgr, storage, partitioner, _ = _build_manager(
        "node-1", target_id, pool, stabilization_ms=1000
    )

    await _put_hint(storage, partitioner, target_id, addr, b"v", hlc)

    # Start stabilisation
    mgr.on_phase_change(target_id, MemberPhase.READY)
    # Immediately cancel (DRAINING)
    mgr.on_phase_change(target_id, MemberPhase.DRAINING)

    await asyncio.sleep(0.1)
    # The hint should still be intact (not marked stale)
    pid = partitioner.pid_for_addr(addr)
    hint_ctx = storage.open_partition(pid).hint(target_id)
    assert hlc not in hint_ctx._stale_hlcs


# ---------------------------------------------------------------------------
# Replay with timeout error → session fails, backoff (concise check)
# ---------------------------------------------------------------------------


@pytest.mark.kv
async def test_replay_timeout_does_not_mark_stale() -> None:
    """Timeout from target → hint remains, not marked stale."""
    target_id = "node-6"
    addr = StoreKey(keyspace=b"default", key=b"to-key")
    hlc = HLCTimestamp(wall_ms=4000, counter=0, node_id="node-1")

    timeout_pool = _MockPool(_TimeoutReplayClient())
    mgr, storage, partitioner, topology_mgr = _build_manager(
        "node-1", target_id, timeout_pool, stabilization_ms=0
    )
    await _register_ready_member(topology_mgr, target_id)

    await _put_hint(storage, partitioner, target_id, addr, b"v", hlc)

    mgr.on_phase_change(target_id, MemberPhase.READY)
    await asyncio.sleep(0.3)  # let it attempt (and fail)

    pid = partitioner.pid_for_addr(addr)
    hint_ctx = storage.open_partition(pid).hint(target_id)
    # Hint not stale — timeout occurred
    assert hlc not in hint_ctx._stale_hlcs
