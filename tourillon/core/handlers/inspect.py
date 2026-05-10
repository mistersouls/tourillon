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
"""ConnectionHandlers for node.inspect and node.inspect.peer_view envelopes.

Both handlers are registered on the peer Dispatcher only. The KV Dispatcher
must not expose these handlers.
"""

from __future__ import annotations

import logging
import uuid
from bisect import bisect_left
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from tourillon.core.lifecycle.probe import ProbeManager
from tourillon.core.lifecycle.state import NodeState
from tourillon.core.ports.transport import (
    MAX_PAYLOAD_DEFAULT,
    RESPONSE_TIMEOUT,
    ConnectionClosedError,
    ResponseTimeoutError,
)
from tourillon.core.ring.partitioner import Partitioner
from tourillon.core.ring.ring import Ring
from tourillon.core.ring.topology import TopologyManager
from tourillon.core.structure.envelope import Envelope
from tourillon.core.transport.client import TcpClient

if TYPE_CHECKING:
    import ssl

    from tourillon.core.ports.serializer import SerializerPort
    from tourillon.core.ports.transport import ReceiveEnvelope, SendEnvelope

logger = logging.getLogger(__name__)

INSPECT_MEMBER_LIMIT: int = 10_000

# Type alias for node-state accessor callable.
type NodeStateService = Callable[[], NodeState]

# Type alias for the factory that creates and connects a TcpClient.
type TcpClientFactory = Callable[[str], Awaitable[TcpClient]]


async def _default_client_factory(
    addr: str,
    tls_ctx: ssl.SSLContext | None,
) -> TcpClient:
    """Create and connect a TcpClient to *addr* using *tls_ctx*."""
    client = TcpClient()
    await client.connect(addr, tls_ctx)
    return client


def _partition_ranges_for_tokens(
    tokens: tuple[int, ...],
    ring: Ring,
    partitioner: Partitioner,
) -> list[dict[str, Any]]:
    """Compute partition ranges for this node's own vnode tokens.

    For each token, determine the predecessor token in the ring and derive the
    range of partition IDs owned by that vnode. The wrapping arc (the vnode
    with the minimum token in the ring) spans [pred_token, ring_max-1] ∪
    [0, token-1]; its count correctly accounts for both sub-ranges. All other
    arcs are non-wrapping and have start_pid <= end_pid.

    Returns a list of dicts sorted by start_pid, ready for msgpack encoding.
    """
    all_ring_tokens = [v.token for v in ring]  # ring iterates in sorted order
    n = len(all_ring_tokens)
    if n == 0:
        return []

    max_pid = partitioner.total_partitions - 1
    ranges: list[dict[str, Any]] = []

    for t in sorted(tokens):
        idx = bisect_left(all_ring_tokens, t)
        pred_idx = (idx - 1) % n
        pred_token = all_ring_tokens[pred_idx]
        wraps = idx == 0

        # Vnode T owns partitions P where P*step > T_prev AND P*step ≤ T,
        # i.e. P ∈ [pid_for_hash(T_prev) + 1, pid_for_hash(T)].
        start_pid = partitioner.pid_for_hash(pred_token) + 1
        end_pid = partitioner.pid_for_hash(t)

        if wraps:
            # High arc [start_pid, max_pid] ∪ low arc [0, end_pid].
            count = (max_pid - start_pid + 1) + (end_pid + 1)
        else:
            count = end_pid - start_pid + 1

        ranges.append(
            {
                "start_pid": start_pid,
                "end_pid": end_pid,
                "count": count,
                "token_hex": f"0x{t:032x}",
            }
        )

    return sorted(ranges, key=lambda r: r["start_pid"])


def _build_member_list(registry: Any) -> list[dict[str, Any]]:
    """Build a sorted-by-node_id list of member summary dicts from *registry*."""
    members = sorted(registry, key=lambda m: m.node_id)
    return [
        {
            "node_id": m.node_id,
            "phase": str(m.phase),
            "generation": m.generation,
            "seq": m.seq,
            "peer_address": m.peer_address,
        }
        for m in members
    ]


def _apply_member_limit(
    member_list: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], bool]:
    """Truncate *member_list* to INSPECT_MEMBER_LIMIT; return (list, truncated)."""
    if len(member_list) > INSPECT_MEMBER_LIMIT:
        return member_list[:INSPECT_MEMBER_LIMIT], True
    return member_list, False


def _truncate_to_budget(
    resp: dict[str, Any],
    serializer: SerializerPort,
) -> dict[str, Any]:
    """Ensure the serialised size of *resp* stays within MAX_PAYLOAD_DEFAULT.

    First attempt: clear probe_states. Second attempt: binary-search the
    largest members prefix that fits with probe_states=[]. Mutates and returns
    the dict in place.
    """
    encoded = serializer.encode(resp)
    if len(encoded) <= MAX_PAYLOAD_DEFAULT:
        return resp

    resp["probe_states"] = []
    resp["probe_states_truncated"] = True
    encoded = serializer.encode(resp)
    if len(encoded) <= MAX_PAYLOAD_DEFAULT:
        return resp

    # Binary-search the largest members prefix that fits.
    lo, hi = 0, len(resp["members"])
    while lo < hi:
        mid = (lo + hi + 1) // 2
        resp["members"] = resp["members"][:mid]
        if len(serializer.encode(resp)) <= MAX_PAYLOAD_DEFAULT:
            lo = mid
        else:
            hi = mid - 1

    resp["members"] = resp["members"][:lo]
    resp["members_truncated"] = True
    return resp


class NodeInspectHandler:
    """ConnectionHandler for node.inspect on the peer endpoint.

    Reads the target_node_id from the request payload. If the target equals
    self.node_id, builds and returns NodeInspectResponse locally. Otherwise,
    looks up the target in the TopologyManager registry, opens a TcpClient
    connection to the target's peer_address, forwards the request, awaits the
    response, sets forwarded_by=self.node_id, and returns the response to the
    caller. The TcpClient connection is closed after the first response.

    If the target is not found in the registry or has an empty peer_address,
    sends error.node_not_found. If the target is unreachable or times out,
    sends error.node_unreachable.

    An IDLE node that receives a self-inspect request returns a minimal
    response (phase, generation, seq, epoch; empty tokens/ranges/members/
    probe_states) rather than an error.
    """

    def __init__(
        self,
        node_id: str,
        get_state: NodeStateService,
        topology_manager: TopologyManager,
        probe_manager: ProbeManager,
        partitioner: Partitioner,
        peer_address: str,
        kv_address: str,
        size: str,
        serializer: SerializerPort,
        tls_ctx: ssl.SSLContext | None = None,
        attempt_timeout: float = RESPONSE_TIMEOUT,
        client_factory: TcpClientFactory | None = None,
    ) -> None:
        self._node_id = node_id
        self._get_state = get_state
        self._topology_manager = topology_manager
        self._probe_manager = probe_manager
        self._partitioner = partitioner
        self._peer_address = peer_address
        self._kv_address = kv_address
        self._size = size
        self._serializer = serializer
        self._tls_ctx = tls_ctx
        self._attempt_timeout = attempt_timeout
        self._client_factory = client_factory

    async def __call__(
        self,
        receive: ReceiveEnvelope,
        send: SendEnvelope,
    ) -> None:
        """Handle one node.inspect request."""
        req = await receive()
        try:
            payload = self._serializer.decode(req.payload)
            target_id: str = payload["target_node_id"]
        except Exception:
            logger.warning("node.inspect: malformed payload; closing connection")
            return

        if target_id == self._node_id:
            resp_dict = await self._build_self_response()
            await self._reply(
                resp_dict, "node.inspect.response", req.correlation_id, send
            )
        else:
            await self._forward(req, target_id, send)

    async def _build_self_response(self) -> dict[str, Any]:
        """Build the full inspect response dict for this node."""
        state = self._get_state()
        topo = await self._topology_manager.snapshot()
        probe_entries = await self._probe_manager.all_states_with_phi()

        my_tokens = tuple(v.token for v in topo.ring if v.node_id == self._node_id)
        partition_ranges = _partition_ranges_for_tokens(
            my_tokens, topo.ring, self._partitioner
        )
        owned = sum(r["count"] for r in partition_ranges)

        members_list = _build_member_list(topo.registry)
        members_total = len(members_list)
        members_list, members_truncated = _apply_member_limit(members_list)

        probe_list = sorted(
            [
                {"node_id": nid, "state": str(state_val), "phi": phi}
                for nid, state_val, phi in probe_entries
            ],
            key=lambda e: e["node_id"],
        )
        probe_list, probe_truncated = _apply_member_limit(probe_list)

        resp: dict[str, Any] = {
            "node_id": self._node_id,
            "phase": str(state.phase),
            "peer_address": self._peer_address,
            "kv_address": self._kv_address,
            "size": self._size,
            "generation": state.generation,
            "seq": state.seq,
            "epoch": state.epoch,
            "tokens": list(my_tokens),
            "total_partitions": self._partitioner.total_partitions,
            "partition_shift": self._partitioner.partition_shift,
            "owned_partitions": owned,
            "partition_ranges": partition_ranges,
            "members": members_list,
            "members_truncated": members_truncated,
            "members_total": members_total,
            "probe_states": probe_list,
            "probe_states_truncated": probe_truncated,
            "forwarded_by": None,
        }
        return _truncate_to_budget(resp, self._serializer)

    async def _forward(
        self,
        req: Envelope,
        target_id: str,
        send: SendEnvelope,
    ) -> None:
        """Forward the inspect request to *target_id* and relay the response."""
        if target_id == self._node_id:
            await _send_error("error.forward_loop", req.correlation_id, send)
            return

        peer_addr = await self._resolve_peer_address(target_id)
        if peer_addr is None:
            await _send_error("error.node_not_found", req.correlation_id, send)
            return

        logger.info("Forwarding node.inspect for %s to %s.", target_id, peer_addr)
        inner_payload = self._serializer.encode({"target_node_id": target_id})
        inner_req = Envelope.create(
            inner_payload,
            kind="node.inspect",
            schema_id=self._serializer.schema_id,
        )
        client = await self._connect(peer_addr, req.correlation_id, send)
        if client is None:
            return

        try:
            target_resp = await client.request(inner_req, timeout=self._attempt_timeout)
        except (ResponseTimeoutError, ConnectionClosedError, OSError):
            await _send_error("error.node_unreachable", req.correlation_id, send)
            return
        finally:
            await client.close()

        await self._relay_response(target_resp, req.correlation_id, send)

    async def _resolve_peer_address(self, target_id: str) -> str | None:
        """Return peer_address for *target_id* from registry, or None if absent/empty."""
        topo = await self._topology_manager.snapshot()
        member = topo.registry.get(target_id)
        if member is None:
            return None
        if not member.peer_address:
            logger.warning(
                "node.inspect: empty peer_address for target %s; refusing forward",
                target_id,
            )
            return None
        return member.peer_address

    async def _connect(
        self,
        peer_addr: str,
        correlation_id: uuid.UUID,
        send: SendEnvelope,
    ) -> TcpClient | None:
        """Connect a TcpClient to *peer_addr*; send error and return None on failure."""
        factory = self._client_factory
        try:
            if factory is not None:
                return await factory(peer_addr)
            return await _default_client_factory(peer_addr, self._tls_ctx)
        except OSError:
            await _send_error("error.node_unreachable", correlation_id, send)
            return None

    async def _relay_response(
        self,
        target_resp: Envelope,
        original_cid: uuid.UUID,
        send: SendEnvelope,
    ) -> None:
        """Decode *target_resp*, stamp forwarded_by, re-encode, and send."""
        try:
            resp_data = self._serializer.decode(target_resp.payload)
            resp_data["forwarded_by"] = self._node_id
            new_payload = self._serializer.encode(resp_data)
        except Exception:
            await _send_error("error.node_unreachable", original_cid, send)
            return

        env = Envelope.create(
            new_payload,
            kind="node.inspect.response",
            correlation_id=original_cid,
            schema_id=self._serializer.schema_id,
        )
        await send(env)

    async def _reply(
        self,
        resp_dict: dict[str, Any],
        kind: str,
        correlation_id: uuid.UUID,
        send: SendEnvelope,
    ) -> None:
        """Encode *resp_dict* and send it as an envelope of *kind*."""
        payload = self._serializer.encode(resp_dict)
        env = Envelope.create(
            payload,
            kind=kind,
            correlation_id=correlation_id,
            schema_id=self._serializer.schema_id,
        )
        await send(env)


class NodeInspectPeerViewHandler:
    """ConnectionHandler for node.inspect.peer_view on the peer endpoint.

    Reads the target_node_id from the payload and returns a
    NodePeerViewResponse built from this node's own MemberRegistry and
    ProbeManager without contacting the target. Sends error.node_not_found
    if the target is absent from the registry.
    """

    def __init__(
        self,
        node_id: str,
        topology_manager: TopologyManager,
        probe_manager: ProbeManager,
        serializer: SerializerPort,
    ) -> None:
        self._node_id = node_id
        self._topology_manager = topology_manager
        self._probe_manager = probe_manager
        self._serializer = serializer

    async def __call__(
        self,
        receive: ReceiveEnvelope,
        send: SendEnvelope,
    ) -> None:
        """Handle one node.inspect.peer_view request."""
        req = await receive()
        try:
            payload = self._serializer.decode(req.payload)
            target_id: str = payload["target_node_id"]
        except Exception:
            logger.warning(
                "node.inspect.peer_view: malformed payload; closing connection"
            )
            return

        topo = await self._topology_manager.snapshot()
        member = topo.registry.get(target_id)
        if member is None:
            await _send_error("error.node_not_found", req.correlation_id, send)
            return

        probe_state_val = await self._probe_manager.state_of(target_id)
        phi = await self._probe_manager.phi_of(target_id)

        resp_dict: dict[str, Any] = {
            "target_node_id": target_id,
            "observed_by": self._node_id,
            "phase": str(member.phase),
            "generation": member.generation,
            "seq": member.seq,
            "peer_address": member.peer_address,
            "tokens": list(member.tokens),
            "probe_state": str(probe_state_val),
            "phi": phi,
        }

        payload_bytes = self._serializer.encode(resp_dict)
        env = Envelope.create(
            payload_bytes,
            kind="node.inspect.peer_view.response",
            correlation_id=req.correlation_id,
            schema_id=self._serializer.schema_id,
        )
        await send(env)


async def _send_error(
    kind: str,
    correlation_id: uuid.UUID,
    send: SendEnvelope,
) -> None:
    """Send an error envelope with empty payload back to the caller."""
    env = Envelope.create(payload=b"", kind=kind, correlation_id=correlation_id)
    await send(env)


# Re-export MemberPhase so callers can guard on IDLE without importing lifecycle.
__all__ = [
    "INSPECT_MEMBER_LIMIT",
    "NodeInspectHandler",
    "NodeInspectPeerViewHandler",
    "NodeStateService",
    "TcpClientFactory",
]
