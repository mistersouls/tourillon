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
"""Test suite for NodeInspectHandler and NodeInspectPeerViewHandler.

Scenarios follow the proposal-node-inspect-05102026-003.md test table.
All tests use in-memory adapters; no real sockets or disk I/O occurs.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import AsyncIterator
from typing import Any

import pytest

from tourillon.core.handlers.inspect import (
    INSPECT_MEMBER_LIMIT,
    NodeInspectHandler,
    NodeInspectPeerViewHandler,
    _partition_ranges_for_tokens,
)
from tourillon.core.lifecycle.member import Member, MemberPhase
from tourillon.core.lifecycle.probe import ProbeManager
from tourillon.core.lifecycle.state import NodeState
from tourillon.core.ports.transport import (
    MAX_PAYLOAD_DEFAULT,
    ResponseTimeoutError,
)
from tourillon.core.ring.hashspace import HashSpace
from tourillon.core.ring.partitioner import Partitioner
from tourillon.core.ring.ring import Ring
from tourillon.core.ring.topology import TopologyManager
from tourillon.core.ring.vnode import VNode
from tourillon.core.structure.envelope import Envelope
from tourillon.core.transport.client import TcpClient
from tourillon.infra.serializer.msgpack import MsgpackSerializerAdapter

pytestmark = pytest.mark.inspect

# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

SERIALIZER = MsgpackSerializerAdapter()
HS = HashSpace(bits=128)
SHIFT = 10
PARTITIONER = Partitioner(HS, SHIFT)
TOTAL_PARTITIONS = PARTITIONER.total_partitions  # 1024


def _make_node_state(
    phase: MemberPhase = MemberPhase.READY,
    generation: int = 1,
    seq: int = 0,
    tokens: tuple[int, ...] = (),
    epoch: int = 1,
) -> NodeState:
    return NodeState(
        phase=phase,
        generation=generation,
        seq=seq,
        tokens=tokens,
        epoch=epoch,
    )


def _make_member(
    node_id: str,
    phase: MemberPhase = MemberPhase.READY,
    tokens: tuple[int, ...] = (),
    seq: int = 0,
) -> Member:
    return Member(
        node_id=node_id,
        peer_address=f"{node_id}:7701",
        generation=1,
        seq=seq,
        phase=phase,
        tokens=tokens,
    )


def _make_tokens(n: int, step_factor: int = 1) -> tuple[int, ...]:
    """Generate n evenly-spaced 128-bit tokens."""
    step = HS.max // n
    return tuple(step * i * step_factor % HS.max for i in range(1, n + 1))


async def _make_topology(members: list[Member]) -> TopologyManager:
    """Create a TopologyManager pre-populated with *members*."""
    tm = TopologyManager()
    for m in members:
        await tm.apply_member(m)
    return tm


async def _call_handler(
    handler: Any,
    payload: dict[str, Any],
    kind: str = "node.inspect",
) -> tuple[str, dict[str, Any] | None]:
    """Invoke *handler* and return (response_kind, decoded_payload)."""
    req_payload = SERIALIZER.encode(payload)
    cid = uuid.uuid4()
    req = Envelope(kind=kind, payload=req_payload, correlation_id=cid)
    responses: list[Envelope] = []

    async def receive() -> Envelope:
        return req

    async def send(env: Envelope) -> None:
        responses.append(env)

    await handler(receive, send)
    assert len(responses) == 1, f"Expected 1 response, got {len(responses)}"
    resp = responses[0]
    assert resp.correlation_id == cid
    if resp.payload:
        return resp.kind, SERIALIZER.decode(resp.payload)
    return resp.kind, None


# ---------------------------------------------------------------------------
# Stub clients for forwarding tests (duck-typed to match TcpClient API)
# ---------------------------------------------------------------------------


class _StubClient:
    """Stub client that returns a pre-built response envelope."""

    def __init__(self, response: Envelope) -> None:
        self._response = response
        self._closed = False

    async def request(
        self,
        env: Envelope,
        timeout: float = 30.0,
    ) -> Envelope:
        # Return response with the inner request's correlation_id.
        return Envelope(
            kind=self._response.kind,
            payload=self._response.payload,
            correlation_id=env.correlation_id,
        )

    async def close(self) -> None:
        self._closed = True

    @property
    def is_connected(self) -> bool:
        return not self._closed

    async def stream(  # noqa: PLR6301
        self,
        env: Envelope,
        timeout: float = 30.0,
    ) -> AsyncIterator[Envelope]:
        raise NotImplementedError


class _FailingClient:
    """Stub client that raises OSError on request."""

    async def request(self, env: Envelope, timeout: float = 30.0) -> Envelope:
        raise OSError("connection refused")

    async def close(self) -> None:
        pass

    @property
    def is_connected(self) -> bool:
        return False


class _TimeoutClient:
    """Stub client that raises ResponseTimeoutError on request."""

    async def request(self, env: Envelope, timeout: float = 30.0) -> Envelope:
        raise ResponseTimeoutError(f"timed out after {timeout}s")

    async def close(self) -> None:
        pass

    @property
    def is_connected(self) -> bool:
        return False


def _make_inspect_handler(
    node_id: str = "node-1",
    state: NodeState | None = None,
    topology_manager: TopologyManager | None = None,
    probe_manager: ProbeManager | None = None,
    partitioner: Partitioner = PARTITIONER,
    client_factory: Any = None,
) -> NodeInspectHandler:
    if state is None:
        tokens = _make_tokens(4)
        state = _make_node_state(tokens=tokens)
    if topology_manager is None:
        topology_manager = TopologyManager()
    if probe_manager is None:
        probe_manager = ProbeManager()
    return NodeInspectHandler(
        node_id=node_id,
        get_state=lambda: state,
        topology_manager=topology_manager,
        probe_manager=probe_manager,
        partitioner=partitioner,
        peer_address=f"{node_id}:7701",
        kv_address=f"{node_id}:7700",
        size="M",
        serializer=SERIALIZER,
        client_factory=client_factory,
    )


# ---------------------------------------------------------------------------
# Scenario 1: self-inspect, single READY node, 4 vnodes
# ---------------------------------------------------------------------------


@pytest.mark.inspect
async def test_1_self_inspect_ready_node_owned_partitions_and_ranges() -> None:
    """node_id='node-1', owned_partitions=1024, len(partition_ranges)==4, forwarded_by=None."""
    tokens = _make_tokens(4)
    state = _make_node_state(tokens=tokens)
    member = _make_member("node-1", tokens=tokens)
    tm = await _make_topology([member])
    handler = _make_inspect_handler(node_id="node-1", state=state, topology_manager=tm)

    kind, data = await _call_handler(handler, {"target_node_id": "node-1"})

    assert kind == "node.inspect.response"
    assert data is not None
    assert data["node_id"] == "node-1"
    assert data["owned_partitions"] == TOTAL_PARTITIONS
    assert len(data["partition_ranges"]) == 4
    assert data["forwarded_by"] is None


# ---------------------------------------------------------------------------
# Scenario 2: forwarding to node-2
# ---------------------------------------------------------------------------


@pytest.mark.inspect
async def test_2_forwarded_inspect_sets_forwarded_by() -> None:
    """node-1 forwards to node-2; receives response with forwarded_by='node-1'."""
    tokens2 = _make_tokens(4)
    member1 = _make_member("node-1", tokens=_make_tokens(4))
    member2 = _make_member("node-2", tokens=tokens2)
    tm = await _make_topology([member1, member2])

    # Build a stub response that node-2 would return.
    node2_resp_dict: dict[str, Any] = {
        "node_id": "node-2",
        "phase": "ready",
        "peer_address": "node-2:7701",
        "kv_address": "node-2:7700",
        "size": "M",
        "generation": 1,
        "seq": 0,
        "epoch": 1,
        "tokens": list(tokens2),
        "total_partitions": TOTAL_PARTITIONS,
        "partition_shift": SHIFT,
        "owned_partitions": 512,
        "partition_ranges": [],
        "members": [],
        "members_truncated": False,
        "members_total": 2,
        "probe_states": [],
        "probe_states_truncated": False,
        "forwarded_by": None,
    }
    stub_env = Envelope(
        kind="node.inspect.response",
        payload=SERIALIZER.encode(node2_resp_dict),
    )
    stub = _StubClient(stub_env)

    async def factory(addr: str) -> TcpClient:  # type: ignore[return]
        return stub  # type: ignore[return-value]

    handler = _make_inspect_handler(
        node_id="node-1",
        topology_manager=tm,
        client_factory=factory,
    )
    kind, data = await _call_handler(handler, {"target_node_id": "node-2"})

    assert kind == "node.inspect.response"
    assert data is not None
    assert data["node_id"] == "node-2"
    assert data["forwarded_by"] == "node-1"


# ---------------------------------------------------------------------------
# Scenario 3: self-inspect is never forwarded
# ---------------------------------------------------------------------------


@pytest.mark.inspect
async def test_3_self_inspect_no_forwarding() -> None:
    """Self-inspect with target==contact returns forwarded_by=None without TcpClient."""
    tokens = _make_tokens(4)
    member = _make_member("node-3", tokens=tokens)
    tm = await _make_topology([member])

    calls: list[str] = []

    async def factory(addr: str) -> TcpClient:
        calls.append(addr)
        raise OSError("should not be called")

    handler = _make_inspect_handler(
        node_id="node-3",
        topology_manager=tm,
        client_factory=factory,
    )
    kind, data = await _call_handler(handler, {"target_node_id": "node-3"})

    assert kind == "node.inspect.response"
    assert data is not None
    assert data["forwarded_by"] is None
    assert calls == [], "No TcpClient connection should occur for self-inspect"


# ---------------------------------------------------------------------------
# Scenario 4: target unknown → error.node_not_found
# ---------------------------------------------------------------------------


@pytest.mark.inspect
async def test_4_unknown_target_returns_node_not_found() -> None:
    """node-1 returns error.node_not_found; no TcpClient connection attempt."""
    calls: list[str] = []

    async def factory(addr: str) -> TcpClient:
        calls.append(addr)
        raise OSError

    handler = _make_inspect_handler(node_id="node-1", client_factory=factory)
    kind, data = await _call_handler(handler, {"target_node_id": "node-99"})

    assert kind == "error.node_not_found"
    assert calls == []


# ---------------------------------------------------------------------------
# Scenario 5: target registered but socket refused → error.node_unreachable
# ---------------------------------------------------------------------------


@pytest.mark.inspect
async def test_5_unreachable_target_returns_node_unreachable() -> None:
    """node-1 returns error.node_unreachable; connection was attempted."""
    member2 = _make_member("node-2")
    tm = await _make_topology([_make_member("node-1"), member2])

    connection_attempted: list[str] = []

    async def factory(addr: str) -> TcpClient:
        connection_attempted.append(addr)
        raise OSError("connection refused")

    handler = _make_inspect_handler(
        node_id="node-1",
        topology_manager=tm,
        client_factory=factory,
    )
    kind, _ = await _call_handler(handler, {"target_node_id": "node-2"})

    assert kind == "error.node_unreachable"
    assert len(connection_attempted) == 1


# ---------------------------------------------------------------------------
# Scenario 6: peer_view with DRAINING member does not contact target
# ---------------------------------------------------------------------------


@pytest.mark.inspect
async def test_6_peer_view_draining_member_no_connection() -> None:
    """Returns node.inspect.peer_view.response; observed_by='node-1'; no connection to node-2."""
    member2 = _make_member("node-2", phase=MemberPhase.DRAINING, tokens=(100,))
    tm = await _make_topology([_make_member("node-1"), member2])
    pm = ProbeManager()

    handler = NodeInspectPeerViewHandler(
        node_id="node-1",
        topology_manager=tm,
        probe_manager=pm,
        serializer=SERIALIZER,
    )
    kind, data = await _call_handler(
        handler,
        {"target_node_id": "node-2"},
        kind="node.inspect.peer_view",
    )

    assert kind == "node.inspect.peer_view.response"
    assert data is not None
    assert data["observed_by"] == "node-1"
    assert data["phase"] == "draining"


# ---------------------------------------------------------------------------
# Scenario 7: peer_view unknown target → error.node_not_found
# ---------------------------------------------------------------------------


@pytest.mark.inspect
async def test_7_peer_view_unknown_target_returns_not_found() -> None:
    """Returns error.node_not_found; no connection attempt."""
    handler = NodeInspectPeerViewHandler(
        node_id="node-1",
        topology_manager=TopologyManager(),
        probe_manager=ProbeManager(),
        serializer=SERIALIZER,
    )
    kind, _ = await _call_handler(
        handler,
        {"target_node_id": "node-99"},
        kind="node.inspect.peer_view",
    )
    assert kind == "error.node_not_found"


# ---------------------------------------------------------------------------
# Scenario 8: IDLE node returns minimal response
# ---------------------------------------------------------------------------


@pytest.mark.inspect
async def test_8_idle_node_returns_minimal_response() -> None:
    """Returns node.inspect.response with phase='idle', empty tokens/ranges/members."""
    state = _make_node_state(phase=MemberPhase.IDLE, tokens=())
    tm = TopologyManager()
    handler = _make_inspect_handler(
        node_id="node-1",
        state=state,
        topology_manager=tm,
    )
    kind, data = await _call_handler(handler, {"target_node_id": "node-1"})

    assert kind == "node.inspect.response"
    assert data is not None
    assert data["phase"] == "idle"
    assert data["tokens"] == []
    assert data["partition_ranges"] == []
    assert data["members"] == []
    assert data["probe_states"] == []


# ---------------------------------------------------------------------------
# Scenario 9: serialisation round-trip of NodeInspectResponse
# ---------------------------------------------------------------------------


@pytest.mark.inspect
async def test_9_serialisation_round_trip_preserves_all_fields() -> None:
    """All fields preserved exactly including nested partition_ranges."""
    tokens = _make_tokens(4)
    state = _make_node_state(tokens=tokens)
    member = _make_member("node-1", tokens=tokens)
    tm = await _make_topology([member])
    handler = _make_inspect_handler(
        node_id="node-1",
        state=state,
        topology_manager=tm,
    )

    kind, data = await _call_handler(handler, {"target_node_id": "node-1"})

    assert kind == "node.inspect.response"
    assert data is not None
    assert data["node_id"] == "node-1"
    assert data["total_partitions"] == TOTAL_PARTITIONS
    assert data["partition_shift"] == SHIFT
    assert isinstance(data["partition_ranges"], list)
    assert all(
        {"start_pid", "end_pid", "count", "token_hex"} <= set(r.keys())
        for r in data["partition_ranges"]
    )
    assert data["members_truncated"] is False
    assert data["probe_states_truncated"] is False


# ---------------------------------------------------------------------------
# Scenario 10: peer_view with SUSPECT probe_state
# ---------------------------------------------------------------------------


@pytest.mark.inspect
async def test_10_peer_view_suspect_probe_state_phi_gt_8() -> None:
    """probe_state='suspect', phi > 8.0 in response."""
    member2 = _make_member("node-2", tokens=(100,))
    tm = await _make_topology([_make_member("node-1"), member2])
    pm = ProbeManager()

    # Record one heartbeat, then wait briefly so phi grows past threshold.
    await pm.record_heartbeat("node-2")
    # Force the detector into suspect state by simulating time passing via phi mock.
    # We directly inject a heartbeat with a very old timestamp via the detector.
    async with pm._lock:  # noqa: SLF001
        det = pm._detectors["node-2"]  # noqa: SLF001
        # Record an artificial interval (very large) to make phi spike.
        det._intervals.append(0.001)  # noqa: SLF001
        det._last_arrival = (
            0.0  # force phi = now / (mean * ln10) → very large  # noqa: SLF001
        )

    phi = await pm.phi_of("node-2")
    assert phi > 8.0, f"Expected phi > 8.0, got {phi}"

    handler = NodeInspectPeerViewHandler(
        node_id="node-1",
        topology_manager=tm,
        probe_manager=pm,
        serializer=SERIALIZER,
    )
    kind, data = await _call_handler(
        handler,
        {"target_node_id": "node-2"},
        kind="node.inspect.peer_view",
    )

    assert kind == "node.inspect.peer_view.response"
    assert data is not None
    assert data["probe_state"] == "suspect"
    assert data["phi"] > 8.0


# ---------------------------------------------------------------------------
# Scenario 11: timeout on forwarding → error.node_unreachable
# ---------------------------------------------------------------------------


@pytest.mark.inspect
async def test_11_forwarding_timeout_returns_node_unreachable() -> None:
    """node-1 client raises ResponseTimeoutError; node-1 returns error.node_unreachable."""
    member2 = _make_member("node-2")
    tm = await _make_topology([_make_member("node-1"), member2])

    async def factory(addr: str) -> TcpClient:  # type: ignore[return]
        return _TimeoutClient()  # type: ignore[return-value]

    handler = _make_inspect_handler(
        node_id="node-1",
        topology_manager=tm,
        client_factory=factory,
    )
    kind, _ = await _call_handler(handler, {"target_node_id": "node-2"})

    assert kind == "error.node_unreachable"


# ---------------------------------------------------------------------------
# Scenario 12: partition_ranges for 8 vnodes, partition_shift=4
# ---------------------------------------------------------------------------


@pytest.mark.inspect
async def test_12_partition_ranges_8_vnodes_shift4_correct_count_and_nonoverlap() -> (
    None
):
    """Returns 8 PartitionRange entries; sum(count)==16; all [start,end] non-overlapping."""
    hs_small = HashSpace(bits=128)
    part = Partitioner(hs_small, partition_shift=4)
    total = part.total_partitions  # 16
    tokens = _make_tokens(8)
    ring = Ring([VNode("node-1", t) for t in tokens])

    ranges = _partition_ranges_for_tokens(tokens, ring, part)

    assert len(ranges) == 8
    assert sum(r["count"] for r in ranges) == total

    # Verify non-overlapping by expanding each range to a pid set.
    all_pids: set[int] = set()
    for r in ranges:
        s, e = r["start_pid"], r["end_pid"]
        if s <= e:
            pids = set(range(s, e + 1))
        else:
            # Wrapping arc.
            pids = set(range(s, total)) | set(range(0, e + 1))
        overlap = all_pids & pids
        assert not overlap, f"Overlapping pids {overlap} in range {r}"
        all_pids |= pids

    assert all_pids == set(range(total)), "All partitions must be covered"


@pytest.mark.inspect
def test_12b_partition_ranges_non_aligned_tokens_exact_total() -> None:
    """sum(count)==total and assignments agree with Partitioner.placement_for_token.

    This regression test uses tokens that do NOT fall on partition boundaries so
    that the off-by-one bug (start_pid = pid_for_hash(pred) instead of +1) is
    detectable.  The canonical ownership is determined by
    Partitioner.placement_for_token for every partition and must agree exactly
    with the ranges returned by _partition_ranges_for_tokens.
    """
    hs = HashSpace(bits=128)
    part = Partitioner(hs, partition_shift=4)
    total = part.total_partitions  # 16
    # Deliberately pick tokens that are NOT multiples of the partition step so
    # they land in the interior of several partitions.
    step = hs.max // 16
    tokens: tuple[int, ...] = (
        step // 3,  # inside partition 0
        step + step // 3,  # inside partition 1
        3 * step + step // 2,  # inside partition 3
        7 * step + step // 7,  # inside partition 7
    )
    ring = Ring([VNode("node-1", t) for t in tokens])

    ranges = _partition_ranges_for_tokens(tokens, ring, part)

    assert (
        sum(r["count"] for r in ranges) == total
    ), f"Expected {total} partitions total, got {sum(r['count'] for r in ranges)}"

    # Build canonical ownership map: pid → owning token.
    canonical: dict[int, int] = {}
    for pid in range(total):
        pid_start = pid * (hs.max >> 4)
        vnode = ring.successor(pid_start)
        canonical[pid] = vnode.token

    # Expand each range and verify each pid is owned by the correct token.
    for r in ranges:
        token_val = int(r["token_hex"], 16)
        s, e = r["start_pid"], r["end_pid"]
        if s <= e:
            pids_in_range = range(s, e + 1)
        else:
            pids_in_range = list(range(s, total)) + list(range(0, e + 1))
        for pid in pids_in_range:
            assert canonical[pid] == token_val, (
                f"pid {pid} should be owned by token {canonical[pid]:#x} "
                f"but range claims {token_val:#x}"
            )


# ---------------------------------------------------------------------------
# Scenario 13: CLI collapses ranges above PARTITION_DISPLAY_THRESHOLD
# ---------------------------------------------------------------------------


@pytest.mark.inspect
async def test_13_cli_collapses_ranges_above_threshold_without_flag() -> None:
    """CLI prints summary line; no individual range lines when above threshold."""
    from io import StringIO

    from rich.console import Console

    from tourctl.core.commands.inspect import (
        PARTITION_DISPLAY_THRESHOLD,
        InspectCommand,
    )

    # Build a response with more than PARTITION_DISPLAY_THRESHOLD ranges.
    many_ranges = [
        {"start_pid": i, "end_pid": i, "count": 1, "token_hex": f"0x{i:032x}"}
        for i in range(PARTITION_DISPLAY_THRESHOLD + 1)
    ]
    n_ranges = len(many_ranges)
    resp_dict = {
        "node_id": "node-1",
        "phase": "ready",
        "peer_address": "node-1:7701",
        "kv_address": "node-1:7700",
        "size": "M",
        "generation": 1,
        "seq": 0,
        "epoch": 1,
        "tokens": [],
        "total_partitions": 1024,
        "partition_shift": 10,
        "owned_partitions": n_ranges,
        "partition_ranges": many_ranges,
        "members": [],
        "members_truncated": False,
        "members_total": 0,
        "probe_states": [],
        "probe_states_truncated": False,
        "forwarded_by": None,
    }

    class _AlwaysRespond:
        """Stub client that always returns the inspect response."""

        async def request(self, env: Envelope, timeout: float = 30.0) -> Envelope:
            return Envelope(
                kind="node.inspect.response",
                payload=SERIALIZER.encode(resp_dict),
                correlation_id=env.correlation_id,
            )

        async def close(self) -> None:
            pass

        @property
        def is_connected(self) -> bool:
            return True

    buf = StringIO()
    con = Console(file=buf, highlight=False)
    err_buf = StringIO()
    err_con = Console(file=err_buf, stderr=True, highlight=False)

    cmd = InspectCommand(
        client=_AlwaysRespond(),  # type: ignore[arg-type]
        serializer=SERIALIZER,
        console=con,
        err_console=err_con,
    )
    exit_code = await cmd.run(
        "node-1",
        show_all_partitions=False,
        json_output=False,
        timeout=5.0,
    )

    assert exit_code == 0
    output = buf.getvalue()
    assert "--partitions" in output, "Should hint about --partitions flag"
    assert "→  pids" not in output, "No individual range lines expected"


# ---------------------------------------------------------------------------
# Scenario 14: --partitions flag lists all range lines
# ---------------------------------------------------------------------------


@pytest.mark.inspect
async def test_14_cli_shows_all_ranges_with_partitions_flag() -> None:
    """CLI prints all individual range lines when show_all_partitions=True."""
    from io import StringIO

    from rich.console import Console

    from tourctl.core.commands.inspect import InspectCommand

    many_ranges = [
        {"start_pid": i, "end_pid": i, "count": 1, "token_hex": f"0x{i:032x}"}
        for i in range(5)
    ]
    resp_dict = {
        "node_id": "node-1",
        "phase": "ready",
        "peer_address": "node-1:7701",
        "kv_address": "node-1:7700",
        "size": "M",
        "generation": 1,
        "seq": 0,
        "epoch": 1,
        "tokens": [],
        "total_partitions": 5,
        "partition_shift": 3,
        "owned_partitions": 5,
        "partition_ranges": many_ranges,
        "members": [],
        "members_truncated": False,
        "members_total": 0,
        "probe_states": [],
        "probe_states_truncated": False,
        "forwarded_by": None,
    }

    class _Respond:
        async def request(self, env: Envelope, timeout: float = 30.0) -> Envelope:
            return Envelope(
                kind="node.inspect.response",
                payload=SERIALIZER.encode(resp_dict),
                correlation_id=env.correlation_id,
            )

        async def close(self) -> None:
            pass

        @property
        def is_connected(self) -> bool:
            return True

    buf = StringIO()
    con = Console(file=buf, highlight=False)
    err_con = Console(file=StringIO(), stderr=True, highlight=False)

    cmd = InspectCommand(
        client=_Respond(),  # type: ignore[arg-type]
        serializer=SERIALIZER,
        console=con,
        err_console=err_con,
    )
    exit_code = await cmd.run(
        "node-1",
        show_all_partitions=True,
        json_output=False,
        timeout=5.0,
    )

    assert exit_code == 0
    output = buf.getvalue()
    assert "→  pids" in output


# ---------------------------------------------------------------------------
# Scenario 15: target returns forwarded_by=None (not set by target)
# ---------------------------------------------------------------------------


@pytest.mark.inspect
async def test_15_target_returns_forwarded_by_none_contact_sets_it() -> None:
    """Target always returns forwarded_by=None; contact stamps its own node_id."""
    tokens2 = _make_tokens(4)
    member1 = _make_member("node-1", tokens=_make_tokens(4))
    member2 = _make_member("node-2", tokens=tokens2)
    tm = await _make_topology([member1, member2])

    # The "target" returns forwarded_by=None (correct behaviour).
    target_resp_dict: dict[str, Any] = {
        "node_id": "node-2",
        "phase": "ready",
        "peer_address": "node-2:7701",
        "kv_address": "node-2:7700",
        "size": "M",
        "generation": 1,
        "seq": 0,
        "epoch": 1,
        "tokens": list(tokens2),
        "total_partitions": TOTAL_PARTITIONS,
        "partition_shift": SHIFT,
        "owned_partitions": 512,
        "partition_ranges": [],
        "members": [],
        "members_truncated": False,
        "members_total": 2,
        "probe_states": [],
        "probe_states_truncated": False,
        "forwarded_by": None,
    }
    stub_env = Envelope(
        kind="node.inspect.response",
        payload=SERIALIZER.encode(target_resp_dict),
    )

    async def factory(addr: str) -> TcpClient:  # type: ignore[return]
        return _StubClient(stub_env)  # type: ignore[return-value]

    handler = _make_inspect_handler(
        node_id="node-1",
        topology_manager=tm,
        client_factory=factory,
    )
    kind, data = await _call_handler(handler, {"target_node_id": "node-2"})

    assert kind == "node.inspect.response"
    assert data is not None
    # Contact must have set forwarded_by to its own ID.
    assert data["forwarded_by"] == "node-1"


# ---------------------------------------------------------------------------
# Scenario 16: target with empty peer_address → error.node_not_found
# ---------------------------------------------------------------------------


@pytest.mark.inspect
async def test_16_empty_peer_address_returns_node_not_found() -> None:
    """Returns error.node_not_found when target's peer_address is empty string."""
    member_empty = Member(
        node_id="node-2",
        peer_address="",  # empty!
        generation=1,
        seq=0,
        phase=MemberPhase.READY,
        tokens=(),
    )
    tm = await _make_topology([_make_member("node-1"), member_empty])

    calls: list[str] = []

    async def factory(addr: str) -> TcpClient:
        calls.append(addr)
        raise OSError

    handler = _make_inspect_handler(
        node_id="node-1",
        topology_manager=tm,
        client_factory=factory,
    )
    kind, _ = await _call_handler(handler, {"target_node_id": "node-2"})

    assert kind == "error.node_not_found"
    assert calls == []


# ---------------------------------------------------------------------------
# Scenario 17: json_output=True emits valid JSON
# ---------------------------------------------------------------------------


@pytest.mark.inspect
async def test_17_json_output_emits_valid_json() -> None:
    """stdout contains valid JSON; members_truncated=false; all numeric fields present."""
    from io import StringIO

    from rich.console import Console

    from tourctl.core.commands.inspect import InspectCommand

    tokens = _make_tokens(4)
    resp_dict: dict[str, Any] = {
        "node_id": "node-1",
        "phase": "ready",
        "peer_address": "node-1:7701",
        "kv_address": "node-1:7700",
        "size": "M",
        "generation": 1,
        "seq": 0,
        "epoch": 1,
        "tokens": list(tokens),
        "total_partitions": TOTAL_PARTITIONS,
        "partition_shift": SHIFT,
        "owned_partitions": TOTAL_PARTITIONS,
        "partition_ranges": [],
        "members": [],
        "members_truncated": False,
        "members_total": 0,
        "probe_states": [],
        "probe_states_truncated": False,
        "forwarded_by": None,
    }

    class _Respond:
        async def request(self, env: Envelope, timeout: float = 30.0) -> Envelope:
            return Envelope(
                kind="node.inspect.response",
                payload=SERIALIZER.encode(resp_dict),
                correlation_id=env.correlation_id,
            )

        async def close(self) -> None:
            pass

        @property
        def is_connected(self) -> bool:
            return True

    buf = StringIO()
    con = Console(file=buf, highlight=False)
    cmd = InspectCommand(
        client=_Respond(),  # type: ignore[arg-type]
        serializer=SERIALIZER,
        console=con,
        err_console=Console(file=StringIO(), stderr=True),
    )
    exit_code = await cmd.run("node-1", json_output=True, timeout=5.0)

    assert exit_code == 0
    parsed = json.loads(buf.getvalue())
    assert parsed["members_truncated"] is False
    assert parsed["probe_states_truncated"] is False
    assert "total_partitions" in parsed
    assert "partition_shift" in parsed


# ---------------------------------------------------------------------------
# Scenario 18: json_output=True with peer_view emits valid JSON
# ---------------------------------------------------------------------------


@pytest.mark.inspect
async def test_18_json_output_peer_view_emits_valid_json() -> None:
    """stdout contains valid JSON with target_node_id, observed_by, probe_state, phi."""
    from io import StringIO

    from rich.console import Console

    from tourctl.core.commands.inspect import InspectCommand

    peer_view_resp: dict[str, Any] = {
        "target_node_id": "node-2",
        "observed_by": "node-1",
        "phase": "ready",
        "generation": 1,
        "seq": 3,
        "peer_address": "node-2:7701",
        "tokens": [100, 200],
        "probe_state": "live",
        "phi": 1.24,
    }

    class _Respond:
        async def request(self, env: Envelope, timeout: float = 30.0) -> Envelope:
            return Envelope(
                kind="node.inspect.peer_view.response",
                payload=SERIALIZER.encode(peer_view_resp),
                correlation_id=env.correlation_id,
            )

        async def close(self) -> None:
            pass

        @property
        def is_connected(self) -> bool:
            return True

    buf = StringIO()
    con = Console(file=buf, highlight=False)
    cmd = InspectCommand(
        client=_Respond(),  # type: ignore[arg-type]
        serializer=SERIALIZER,
        console=con,
        err_console=Console(file=StringIO(), stderr=True),
    )
    exit_code = await cmd.run("node-2", peer_view=True, json_output=True, timeout=5.0)

    assert exit_code == 0
    parsed = json.loads(buf.getvalue())
    assert "target_node_id" in parsed
    assert "observed_by" in parsed
    assert "probe_state" in parsed
    assert "phi" in parsed


# ---------------------------------------------------------------------------
# Scenario 19: INSPECT_MEMBER_LIMIT truncation of members
# ---------------------------------------------------------------------------


@pytest.mark.inspect
async def test_19_members_truncated_at_inspect_member_limit() -> None:
    """members contains exactly INSPECT_MEMBER_LIMIT entries; members_truncated=True."""
    # Build a registry with INSPECT_MEMBER_LIMIT + 1 members.
    members = [_make_member(f"node-{i:06d}") for i in range(INSPECT_MEMBER_LIMIT + 1)]
    local_member = members[0]
    local_node_id = local_member.node_id
    tm = await _make_topology(members)
    state = _make_node_state(tokens=local_member.tokens)

    handler = _make_inspect_handler(
        node_id=local_node_id,
        state=state,
        topology_manager=tm,
    )
    kind, data = await _call_handler(handler, {"target_node_id": local_node_id})

    assert kind == "node.inspect.response"
    assert data is not None
    assert len(data["members"]) == INSPECT_MEMBER_LIMIT
    assert data["members_truncated"] is True
    assert data["members_total"] == INSPECT_MEMBER_LIMIT + 1


# ---------------------------------------------------------------------------
# Scenario 20: INSPECT_MEMBER_LIMIT truncation of probe_states
# ---------------------------------------------------------------------------


@pytest.mark.inspect
async def test_20_probe_states_truncated_at_inspect_member_limit() -> None:
    """probe_states contains exactly INSPECT_MEMBER_LIMIT entries; probe_states_truncated=True."""
    pm = ProbeManager()
    for i in range(INSPECT_MEMBER_LIMIT + 1):
        await pm.record_heartbeat(f"peer-{i:06d}")

    member = _make_member("node-1")
    tm = await _make_topology([member])
    state = _make_node_state(tokens=())

    handler = _make_inspect_handler(
        node_id="node-1",
        state=state,
        topology_manager=tm,
        probe_manager=pm,
    )
    kind, data = await _call_handler(handler, {"target_node_id": "node-1"})

    assert kind == "node.inspect.response"
    assert data is not None
    assert len(data["probe_states"]) == INSPECT_MEMBER_LIMIT
    assert data["probe_states_truncated"] is True


# ---------------------------------------------------------------------------
# Scenario 21: CLI warns when members_truncated=True
# ---------------------------------------------------------------------------


@pytest.mark.inspect
async def test_21_cli_warns_when_members_truncated() -> None:
    """CLI prints warning line when members_truncated=True."""
    from io import StringIO

    from rich.console import Console

    from tourctl.core.commands.inspect import InspectCommand

    resp_dict: dict[str, Any] = {
        "node_id": "node-1",
        "phase": "ready",
        "peer_address": "node-1:7701",
        "kv_address": "node-1:7700",
        "size": "M",
        "generation": 1,
        "seq": 0,
        "epoch": 1,
        "tokens": [],
        "total_partitions": TOTAL_PARTITIONS,
        "partition_shift": SHIFT,
        "owned_partitions": 0,
        "partition_ranges": [],
        "members": [
            {
                "node_id": f"n-{i}",
                "phase": "ready",
                "generation": 1,
                "seq": 0,
                "peer_address": f"n-{i}:7701",
            }
            for i in range(10)
        ],
        "members_truncated": True,
        "members_total": 31427,
        "probe_states": [],
        "probe_states_truncated": False,
        "forwarded_by": None,
    }

    class _Respond:
        async def request(self, env: Envelope, timeout: float = 30.0) -> Envelope:
            return Envelope(
                kind="node.inspect.response",
                payload=SERIALIZER.encode(resp_dict),
                correlation_id=env.correlation_id,
            )

        async def close(self) -> None:
            pass

        @property
        def is_connected(self) -> bool:
            return True

    buf = StringIO()
    con = Console(file=buf, highlight=False)
    cmd = InspectCommand(
        client=_Respond(),  # type: ignore[arg-type]
        serializer=SERIALIZER,
        console=con,
        err_console=Console(file=StringIO(), stderr=True),
    )
    exit_code = await cmd.run("node-1", json_output=False, timeout=5.0)

    assert exit_code == 0
    output = buf.getvalue()
    assert "truncated" in output.lower()
    assert "31427" in output


# ---------------------------------------------------------------------------
# Scenario 22: serialised response with 10 000 members ≤ 4 MiB
# ---------------------------------------------------------------------------


@pytest.mark.inspect
async def test_22_max_realistic_payload_fits_within_budget() -> None:
    """Serialized NodeInspectResponse with INSPECT_MEMBER_LIMIT members ≤ MAX_PAYLOAD_DEFAULT."""
    members_list = [
        {
            "node_id": f"very-long-node-identifier-{i:06d}",
            "phase": "ready",
            "generation": 1,
            "seq": i,
            "peer_address": f"peer-{i:06d}.prod.verylongdomain.example.com:7701",
        }
        for i in range(INSPECT_MEMBER_LIMIT)
    ]
    resp_dict: dict[str, Any] = {
        "node_id": "node-1",
        "phase": "ready",
        "peer_address": "peer.example.com:7701",
        "kv_address": "kv.example.com:7700",
        "size": "M",
        "generation": 1,
        "seq": 0,
        "epoch": 1,
        "tokens": list(range(4)),
        "total_partitions": TOTAL_PARTITIONS,
        "partition_shift": SHIFT,
        "owned_partitions": 256,
        "partition_ranges": [],
        "members": members_list,
        "members_truncated": False,
        "members_total": INSPECT_MEMBER_LIMIT,
        "probe_states": [],
        "probe_states_truncated": False,
        "forwarded_by": None,
    }
    encoded = SERIALIZER.encode(resp_dict)
    assert (
        len(encoded) <= MAX_PAYLOAD_DEFAULT
    ), f"Payload {len(encoded)} bytes exceeds MAX_PAYLOAD_DEFAULT {MAX_PAYLOAD_DEFAULT}"


# ---------------------------------------------------------------------------
# Scenario 23: step-6 fallback clears probe_states when payload too large
# ---------------------------------------------------------------------------


@pytest.mark.inspect
async def test_23_step6_fallback_clears_probe_states_when_oversized() -> None:
    """Handler clears probe_states; final payload ≤ MAX_PAYLOAD_DEFAULT."""
    from tourillon.core.handlers.inspect import _truncate_to_budget

    # Each member entry with 400-char strings → ~800 bytes msgpack.
    # INSPECT_MEMBER_LIMIT × 800 bytes ≈ 8 MiB for members alone → over budget.
    long_str = "x" * 400
    big_member = {
        "node_id": long_str,
        "phase": "ready",
        "generation": 1,
        "seq": 0,
        "peer_address": long_str,
    }
    big_probe = {"node_id": long_str, "state": "live", "phi": 0.0}

    n = INSPECT_MEMBER_LIMIT  # 10 000 entries
    resp: dict[str, Any] = {
        "node_id": "node-1",
        "phase": "ready",
        "peer_address": "n:7701",
        "kv_address": "n:7700",
        "size": "M",
        "generation": 1,
        "seq": 0,
        "epoch": 1,
        "tokens": [],
        "total_partitions": TOTAL_PARTITIONS,
        "partition_shift": SHIFT,
        "owned_partitions": 0,
        "partition_ranges": [],
        "members": [big_member] * n,
        "members_truncated": False,
        "members_total": n,
        "probe_states": [big_probe] * n,
        "probe_states_truncated": False,
        "forwarded_by": None,
    }

    result = _truncate_to_budget(resp, SERIALIZER)
    encoded = SERIALIZER.encode(result)
    assert len(encoded) <= MAX_PAYLOAD_DEFAULT
    assert result["probe_states_truncated"] is True


# ---------------------------------------------------------------------------
# Supplementary: structure dataclasses round-trip
# ---------------------------------------------------------------------------


@pytest.mark.inspect
def test_s1_structure_dataclasses_are_constructable() -> None:
    """PartitionRange, MemberSummary, ProbeSummary, NodeInspectResponse, NodePeerViewResponse."""
    from tourillon.core.structure.inspect import (
        MemberSummary,
        NodeInspectResponse,
        NodePeerViewResponse,
        PartitionRange,
        ProbeSummary,
    )

    pr = PartitionRange(start_pid=0, end_pid=63, count=64, token_hex="0x001a3cb4f200")
    assert pr.start_pid == 0
    assert pr.token_hex == "0x001a3cb4f200"

    ms = MemberSummary(
        node_id="n1",
        phase="ready",
        generation=1,
        seq=5,
        peer_address="n1:7701",
    )
    assert ms.phase == "ready"

    ps = ProbeSummary(node_id="n2", state="live", phi=1.23)
    assert ps.phi == 1.23

    resp = NodeInspectResponse(
        node_id="n1",
        phase="ready",
        peer_address="n1:7701",
        kv_address="n1:7700",
        size="M",
        generation=1,
        seq=0,
        epoch=1,
        tokens=(1, 2, 3, 4),
        total_partitions=1024,
        partition_shift=10,
        owned_partitions=1024,
        partition_ranges=(pr,),
        members=(ms,),
        members_truncated=False,
        members_total=1,
        probe_states=(ps,),
        probe_states_truncated=False,
        forwarded_by=None,
    )
    assert resp.forwarded_by is None
    assert len(resp.partition_ranges) == 1

    pv = NodePeerViewResponse(
        target_node_id="n2",
        observed_by="n1",
        phase="ready",
        generation=1,
        seq=3,
        peer_address="n2:7701",
        tokens=(10, 20),
        probe_state="live",
        phi=0.5,
    )
    assert pv.observed_by == "n1"


# ---------------------------------------------------------------------------
# Supplementary: InspectCommand error path coverage
# ---------------------------------------------------------------------------


@pytest.mark.inspect
async def test_s2_inspect_command_timeout_returns_exit_1() -> None:
    """InspectCommand returns exit code 1 on ResponseTimeoutError."""
    from io import StringIO

    from rich.console import Console

    from tourctl.core.commands.inspect import InspectCommand

    class _TimeoutRespond:
        async def request(self, env: Envelope, timeout: float = 30.0) -> Envelope:
            raise ResponseTimeoutError("timeout")

        async def close(self) -> None:
            pass

        @property
        def is_connected(self) -> bool:
            return True

    err_buf = StringIO()
    cmd = InspectCommand(
        client=_TimeoutRespond(),  # type: ignore[arg-type]
        serializer=SERIALIZER,
        console=Console(file=StringIO()),
        err_console=Console(file=err_buf, stderr=True, highlight=False),
    )
    exit_code = await cmd.run("node-2", timeout=5.0)
    assert exit_code == 1
    assert "timed out" in err_buf.getvalue().lower()


@pytest.mark.inspect
async def test_s3_inspect_command_node_unreachable_error() -> None:
    """InspectCommand prints unreachable message on error.node_unreachable."""
    from io import StringIO

    from rich.console import Console

    from tourctl.core.commands.inspect import InspectCommand

    class _UnreachableRespond:
        async def request(self, env: Envelope, timeout: float = 30.0) -> Envelope:
            return Envelope(
                kind="error.node_unreachable",
                payload=b"",
                correlation_id=env.correlation_id,
            )

        async def close(self) -> None:
            pass

        @property
        def is_connected(self) -> bool:
            return True

    err_buf = StringIO()
    cmd = InspectCommand(
        client=_UnreachableRespond(),  # type: ignore[arg-type]
        serializer=SERIALIZER,
        console=Console(file=StringIO()),
        err_console=Console(file=err_buf, stderr=True, highlight=False),
        contact_node_id="node-1",
    )
    exit_code = await cmd.run("node-2", timeout=5.0)
    assert exit_code == 1
    assert "unreachable" in err_buf.getvalue().lower()


@pytest.mark.inspect
async def test_s4_inspect_command_generic_error_kind() -> None:
    """InspectCommand shows raw error kind for unknown error responses."""
    from io import StringIO

    from rich.console import Console

    from tourctl.core.commands.inspect import InspectCommand

    class _GenericError:
        async def request(self, env: Envelope, timeout: float = 30.0) -> Envelope:
            return Envelope(
                kind="error.some_unknown_error",
                payload=b"",
                correlation_id=env.correlation_id,
            )

        async def close(self) -> None:
            pass

        @property
        def is_connected(self) -> bool:
            return True

    err_buf = StringIO()
    cmd = InspectCommand(
        client=_GenericError(),  # type: ignore[arg-type]
        serializer=SERIALIZER,
        console=Console(file=StringIO()),
        err_console=Console(file=err_buf, stderr=True, highlight=False),
    )
    exit_code = await cmd.run("node-2", timeout=5.0)
    assert exit_code == 1
    assert "error.some_unknown_error" in err_buf.getvalue()


@pytest.mark.inspect
async def test_s5_peer_view_timeout_returns_exit_1() -> None:
    """InspectCommand peer_view returns exit code 1 on timeout."""
    from io import StringIO

    from rich.console import Console

    from tourctl.core.commands.inspect import InspectCommand

    class _TimeoutPV:
        async def request(self, env: Envelope, timeout: float = 30.0) -> Envelope:
            raise ResponseTimeoutError("timeout")

        async def close(self) -> None:
            pass

        @property
        def is_connected(self) -> bool:
            return True

    err_buf = StringIO()
    cmd = InspectCommand(
        client=_TimeoutPV(),  # type: ignore[arg-type]
        serializer=SERIALIZER,
        console=Console(file=StringIO()),
        err_console=Console(file=err_buf, stderr=True, highlight=False),
    )
    exit_code = await cmd.run("node-2", peer_view=True, timeout=5.0)
    assert exit_code == 1


@pytest.mark.inspect
async def test_s6_peer_view_not_found_error() -> None:
    """InspectCommand peer_view shows gossip-record-missing message."""
    from io import StringIO

    from rich.console import Console

    from tourctl.core.commands.inspect import InspectCommand

    class _NotFoundPV:
        async def request(self, env: Envelope, timeout: float = 30.0) -> Envelope:
            return Envelope(
                kind="error.node_not_found",
                payload=b"",
                correlation_id=env.correlation_id,
            )

        async def close(self) -> None:
            pass

        @property
        def is_connected(self) -> bool:
            return True

    err_buf = StringIO()
    cmd = InspectCommand(
        client=_NotFoundPV(),  # type: ignore[arg-type]
        serializer=SERIALIZER,
        console=Console(file=StringIO()),
        err_console=Console(file=err_buf, stderr=True, highlight=False),
        contact_node_id="node-1",
    )
    exit_code = await cmd.run("node-2", peer_view=True, timeout=5.0)
    assert exit_code == 1
    assert "gossip record" in err_buf.getvalue().lower()


@pytest.mark.inspect
async def test_s7_peer_view_unknown_error_kind() -> None:
    """InspectCommand peer_view shows raw error kind for unknown errors."""
    from io import StringIO

    from rich.console import Console

    from tourctl.core.commands.inspect import InspectCommand

    class _UnknownErrPV:
        async def request(self, env: Envelope, timeout: float = 30.0) -> Envelope:
            return Envelope(
                kind="error.unexpected",
                payload=b"",
                correlation_id=env.correlation_id,
            )

        async def close(self) -> None:
            pass

        @property
        def is_connected(self) -> bool:
            return True

    err_buf = StringIO()
    cmd = InspectCommand(
        client=_UnknownErrPV(),  # type: ignore[arg-type]
        serializer=SERIALIZER,
        console=Console(file=StringIO()),
        err_console=Console(file=err_buf, stderr=True, highlight=False),
    )
    exit_code = await cmd.run("node-2", peer_view=True, timeout=5.0)
    assert exit_code == 1
    assert "error.unexpected" in err_buf.getvalue()


@pytest.mark.inspect
async def test_s8_peer_view_rich_output_rendered() -> None:
    """InspectCommand renders peer_view Rich output correctly."""
    from io import StringIO

    from rich.console import Console

    from tourctl.core.commands.inspect import InspectCommand

    pv_resp = {
        "target_node_id": "node-2",
        "observed_by": "node-1",
        "phase": "ready",
        "generation": 1,
        "seq": 5,
        "peer_address": "node-2:7701",
        "tokens": [100, 200],
        "probe_state": "live",
        "phi": 1.24,
    }

    class _PVRespond:
        async def request(self, env: Envelope, timeout: float = 30.0) -> Envelope:
            return Envelope(
                kind="node.inspect.peer_view.response",
                payload=SERIALIZER.encode(pv_resp),
                correlation_id=env.correlation_id,
            )

        async def close(self) -> None:
            pass

        @property
        def is_connected(self) -> bool:
            return True

    buf = StringIO()
    cmd = InspectCommand(
        client=_PVRespond(),  # type: ignore[arg-type]
        serializer=SERIALIZER,
        console=Console(file=buf, highlight=False),
        err_console=Console(file=StringIO(), stderr=True),
    )
    exit_code = await cmd.run("node-2", peer_view=True, json_output=False, timeout=5.0)
    assert exit_code == 0
    output = buf.getvalue()
    assert "node-2" in output
    assert "READY" in output
    assert "LIVE" in output
    assert "1.24" in output


@pytest.mark.inspect
def test_s9_short_token_edge_cases() -> None:
    """_short_token returns expected values for various inputs."""
    from tourctl.core.commands.inspect import _short_token

    # Normal 128-bit hex token (34 chars: 0x + 32).
    full = "0x" + "a" * 32
    assert _short_token(full) == "0xaaaaaaaa\u2026"

    # Short token (no truncation needed).
    short = "0x1234"
    assert _short_token(short) == "0x1234"

    # Without 0x prefix.
    no_prefix = "1234567890abcdef"
    assert _short_token(no_prefix) == "1234567890abcdef"


@pytest.mark.inspect
async def test_s10_inspect_command_bad_payload_returns_1() -> None:
    """InspectCommand returns exit code 1 when response payload cannot be decoded."""
    from io import StringIO

    from rich.console import Console

    from tourctl.core.commands.inspect import InspectCommand

    class _BadPayload:
        async def request(self, env: Envelope, timeout: float = 30.0) -> Envelope:
            return Envelope(
                kind="node.inspect.response",
                payload=b"\xff\xfe\xfd",  # invalid msgpack
                correlation_id=env.correlation_id,
            )

        async def close(self) -> None:
            pass

        @property
        def is_connected(self) -> bool:
            return True

    err_buf = StringIO()
    cmd = InspectCommand(
        client=_BadPayload(),  # type: ignore[arg-type]
        serializer=SERIALIZER,
        console=Console(file=StringIO()),
        err_console=Console(file=err_buf, stderr=True, highlight=False),
    )
    exit_code = await cmd.run("node-2", timeout=5.0)
    assert exit_code == 1


@pytest.mark.inspect
async def test_s11_inspect_handler_connection_closed_error_returns_unreachable() -> (
    None
):
    """ConnectionClosedError during forward returns error.node_unreachable."""
    from tourillon.core.ports.transport import ConnectionClosedError

    member2 = _make_member("node-2")
    tm = await _make_topology([_make_member("node-1"), member2])

    class _ClosedClient:
        async def request(self, env: Envelope, timeout: float = 30.0) -> Envelope:
            raise ConnectionClosedError("node-2:7701")

        async def close(self) -> None:
            pass

        @property
        def is_connected(self) -> bool:
            return False

    async def factory(addr: str) -> TcpClient:  # type: ignore[return]
        return _ClosedClient()  # type: ignore[return-value]

    handler = _make_inspect_handler(
        node_id="node-1",
        topology_manager=tm,
        client_factory=factory,
    )
    kind, _ = await _call_handler(handler, {"target_node_id": "node-2"})
    assert kind == "error.node_unreachable"


@pytest.mark.inspect
async def test_s12_inspect_handler_malformed_payload_closes_connection() -> None:
    """Malformed node.inspect payload logs a warning and sends no response."""
    handler = _make_inspect_handler()
    req = Envelope(
        kind="node.inspect", payload=b"\xff\xfe", correlation_id=uuid.uuid4()
    )
    responses: list[Envelope] = []

    async def receive() -> Envelope:
        return req

    async def send(env: Envelope) -> None:
        responses.append(env)

    await handler(receive, send)
    assert len(responses) == 0


@pytest.mark.inspect
async def test_s13_peer_view_handler_malformed_payload_closes_connection() -> None:
    """Malformed node.inspect.peer_view payload logs a warning and sends no response."""
    tm = await _make_topology([_make_member("node-1")])
    handler = NodeInspectPeerViewHandler(
        node_id="node-1",
        topology_manager=tm,
        probe_manager=ProbeManager(),
        serializer=SERIALIZER,
    )
    req = Envelope(
        kind="node.inspect.peer_view",
        payload=b"\xff\xfe",
        correlation_id=uuid.uuid4(),
    )
    responses: list[Envelope] = []

    async def receive() -> Envelope:
        return req

    async def send(env: Envelope) -> None:
        responses.append(env)

    await handler(receive, send)
    assert len(responses) == 0


@pytest.mark.inspect
async def test_s14_truncate_budget_probe_cleared_fits() -> None:
    """_truncate_to_budget returns after clearing probe_states when that's enough."""
    from tourillon.core.handlers.inspect import _truncate_to_budget

    # Build a response where probe_states alone pushes it over MAX_PAYLOAD_DEFAULT
    # but members + rest fits once probe_states is emptied.
    member_entry = {
        "node_id": "n1",
        "phase": "ready",
        "generation": 1,
        "seq": 0,
        "peer_address": "n1:7701",
    }
    # 3000 member entries × ~50 bytes each ≈ ~150 KB — well under 4 MiB
    members = [member_entry] * 3000
    # Huge probe_states to push total over 4 MiB
    probe_entry = {"node_id": "p", "state": "live", "phi": 1.0, "padding": "x" * 1500}
    probe_states = [probe_entry] * 3000  # ~4.5 MB of probes alone

    resp: dict[str, Any] = {
        "node_id": "n1",
        "phase": "ready",
        "peer_address": "n1:7701",
        "kv_address": "n1:7700",
        "size": "small",
        "generation": 1,
        "seq": 0,
        "epoch": 1,
        "tokens": [],
        "total_partitions": 1024,
        "partition_shift": 10,
        "owned_partitions": 0,
        "partition_ranges": [],
        "members": members,
        "members_truncated": False,
        "members_total": len(members),
        "probe_states": probe_states,
        "probe_states_truncated": False,
        "forwarded_by": None,
    }

    # Verify it starts over budget
    encoded = SERIALIZER.encode(resp)
    assert len(encoded) > MAX_PAYLOAD_DEFAULT

    result = _truncate_to_budget(resp, SERIALIZER)

    # Clearing probe_states should have been enough (members are small)
    assert result["probe_states"] == []
    assert result["probe_states_truncated"] is True
    # members should NOT be truncated (still fits after clearing probes)
    assert result["members_truncated"] is False


@pytest.mark.inspect
async def test_s15_relay_response_decode_failure_returns_unreachable() -> None:
    """A garbled response from the target node returns error.node_unreachable."""
    member2 = _make_member("node-2")
    tm = await _make_topology([_make_member("node-1"), member2])

    garbled = Envelope(
        kind="node.inspect.response",
        payload=b"\xff\xfe\xfd",  # invalid msgpack
        correlation_id=uuid.uuid4(),
    )

    async def factory(addr: str) -> TcpClient:  # type: ignore[return]
        return _StubClient(garbled)  # type: ignore[return-value]

    handler = _make_inspect_handler(
        node_id="node-1",
        topology_manager=tm,
        client_factory=factory,
    )
    kind, _ = await _call_handler(handler, {"target_node_id": "node-2"})
    assert kind == "error.node_unreachable"


@pytest.mark.inspect
def test_s16_msgpack_unknown_ext_type_passthrough() -> None:
    """Unknown ExtType codes are decoded as msgpack.ExtType, not raised."""
    import msgpack

    from tourillon.infra.serializer.msgpack import _ext_hook

    result = _ext_hook(99, b"\x01\x02\x03")
    assert isinstance(result, msgpack.ExtType)
    assert result.code == 99
    assert result.data == b"\x01\x02\x03"
