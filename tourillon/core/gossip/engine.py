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
"""GossipEngine — hot-path push loop and anti-entropy loop."""

import asyncio
import logging
import math
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from tourillon.core.ring.topology import TopologyManager
from tourillon.core.structure.config import GossipConfig
from tourillon.core.structure.member import Member, MemberPhase
from tourillon.core.transport.client import PeerClientPool
from tourlib.envelope import Envelope
from tourlib.exceptions import ConnectionClosedError, ResponseTimeoutError
from tourlib.ports.serializer import Serializer
from tourlib.transport import TcpClient

logger = logging.getLogger(__name__)

# Phases eligible for gossip fan-out (FAILED, PAUSED, IDLE are excluded).
_ELIGIBLE_PHASES = frozenset(
    {MemberPhase.READY, MemberPhase.JOINING, MemberPhase.DRAINING}
)


@dataclass
class GossipStats:
    """Mutable counters tracking gossip activity since engine start."""

    known_members: int = 0
    push_sent_total: int = 0
    push_recv_total: int = 0
    ae_cycles_total: int = 0
    ae_diverged: int = 0
    last_ae_peer: str | None = None
    last_ae_at: str | None = None  # ISO 8601 UTC
    bootstrap_ok_total: int = 0
    bootstrap_err_total: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serialisable dict of all stats."""
        return {
            "known_members": self.known_members,
            "push_sent_total": self.push_sent_total,
            "push_recv_total": self.push_recv_total,
            "ae_cycles_total": self.ae_cycles_total,
            "ae_diverged": self.ae_diverged,
            "last_ae_peer": self.last_ae_peer,
            "last_ae_at": self.last_ae_at,
            "bootstrap_ok_total": self.bootstrap_ok_total,
            "bootstrap_err_total": self.bootstrap_err_total,
        }


class GossipEngine:
    """Anti-entropic gossip engine driving cluster membership convergence.

    Two independent loops run inside an asyncio.TaskGroup:
    - _hot_loop: drains hot_queue and sends gossip.push to K peers.
    - _ae_loop:  fires every anti_entropy_interval seconds; runs ping → pong →
                 digest → delta if divergence detected.

    All member state is delegated to TopologyManager. Internal state is limited
    to hot_queue and the node's own seq counter (from NodeState.seq at startup).

    stop() drains the hot_queue before cancelling both loops, guaranteeing that
    any announce() call made before stop() — including critical FAILED
    announcements — is fully sent before the engine terminates.
    """

    def __init__(
        self,
        node_id: str,
        topology_manager: TopologyManager,
        pool: PeerClientPool,
        config: GossipConfig,
        partition_shift: int,
        serializer: Serializer,
        on_failed: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self._node_id = node_id
        self._topology_manager = topology_manager
        self._pool = pool
        self._config = config
        self._partition_shift = partition_shift
        self._serializer = serializer
        self._on_failed = on_failed
        self._hot_queue: asyncio.Queue[Member] = asyncio.Queue()
        self._stop_event = asyncio.Event()
        self._task_group: asyncio.TaskGroup | None = None
        self._started = False
        self.stats = GossipStats()

    async def start(self) -> None:
        """Start both loops in a TaskGroup. Blocks until stop() is called."""
        self._started = True
        logger.info("GossipEngine started.")
        async with asyncio.TaskGroup() as tg:
            self._task_group = tg
            tg.create_task(self._hot_loop(), name="gossip.hot_loop")
            tg.create_task(self._ae_loop(), name="gossip.ae_loop")

    async def stop(self) -> None:
        """Drain hot_queue then signal both loops to stop cleanly.

        Draining the hot_queue before signalling cancellation guarantees that
        any announce() call made before stop() — including critical FAILED
        announcements — is fully sent to peers before the engine terminates.
        The join is skipped when start() was never called: the hot_loop is not
        running, so task_done() would never be invoked and join() would block
        forever.
        """
        if self._started:
            await self._hot_queue.join()
        self._stop_event.set()

    async def announce(self, member: Member) -> None:
        """Enqueue member for immediate hot-path propagation.

        Called after every local phase transition. seq is already incremented
        and persisted before this call (write-before-announce invariant).
        """
        await self._hot_queue.put(member)

    async def _hot_loop(self) -> None:
        """Drain hot_queue and send gossip.push to K eligible peers."""
        while not self._stop_event.is_set():
            try:
                member = await asyncio.wait_for(self._hot_queue.get(), timeout=1.0)
            except TimeoutError:
                continue
            try:
                await self._push_member(member)
            finally:
                self._hot_queue.task_done()

    async def _ae_loop(self) -> None:
        """Fire anti-entropy cycles every anti_entropy_interval seconds."""
        while not self._stop_event.is_set():
            try:
                async with asyncio.timeout(self._config.anti_entropy_interval):
                    await self._stop_event.wait()
                return  # stop_event set — exit cleanly
            except TimeoutError:
                pass  # interval elapsed — run one AE cycle
            await self._run_ae_cycle()

    async def _push_member(self, member: Member) -> None:
        """Fan-out gossip.push for *member* to K eligible peers."""
        topo = await self._topology_manager.snapshot()
        eligible = [
            m
            for m in topo.registry
            if m.phase in _ELIGIBLE_PHASES and m.node_id != self._node_id
        ]
        if not eligible:
            logger.debug("GossipEngine: no eligible peers for push.")
            return

        n = len(eligible)
        k = min(math.ceil(math.log2(n + 1)) + 1, self._config.max_fan_out)
        targets = random.sample(eligible, min(k, n))

        for target in targets:
            await self._send_push(member, target)

    async def _send_push(self, member: Member, target: Member) -> None:
        """Send gossip.push for *member* to *target*; update stats."""
        if self._serializer is None:
            return
        try:
            client = await self._pool.acquire(target.node_id, target.peer_address)
            payload = self._serializer.encode(  # type: ignore[union-attr]
                {
                    "sender": self._node_id,
                    "ttl": 3,
                    "members": [member.to_dict()],
                }
            )
            env = Envelope.create(
                payload,
                kind="gossip.push",
                schema_id=self._serializer.schema_id,  # type: ignore[union-attr]
            )
            resp = await client.request(env)
            if resp.kind == "gossip.error":
                await self._handle_push_error(resp)
                return
            if resp.kind == "gossip.push.ok":
                data = self._serializer.decode(resp.payload)  # type: ignore[union-attr]
                self.stats.push_sent_total += 1
                if data.get("accepted", 0) == 0:
                    return  # peer already up-to-date
        except (OSError, ResponseTimeoutError, ConnectionClosedError) as exc:
            logger.warning("gossip.push to %s failed: %s.", target.node_id, exc)

    async def _handle_push_error(self, resp: Envelope) -> None:
        """Handle gossip.error received in response to our own gossip.push."""
        try:
            data = self._serializer.decode(resp.payload)  # type: ignore[union-attr]
            code = data.get("code", "")
        except Exception as exc:
            logger.warning("Malformed gossip.push response: %s", exc)
            code = ""

        if code == "partition_shift_mismatch":
            logger.error(
                "Received gossip.error code: partition_shift_mismatch "
                "in response to our own gossip.push. Transitioning to FAILED."
            )
            await self._transition_to_failed()

    async def _transition_to_failed(self) -> None:
        """Execute the write-before-announce → announce → stop FAILED pattern.

        Calls on_failed() when wired in — that callback performs the
        write-before-announce and announce steps (increment seq, persist FAILED
        state, enqueue the FAILED Member). Schedules stop() as a background
        task rather than awaiting it directly to avoid deadlocking the hot_loop
        (which calls this method before its task_done()).
        """
        logger.error(
            "GossipEngine: local partition_shift rejected by peers. Stopping engine."
        )
        if self._on_failed is not None:
            await self._on_failed()
        # Schedule stop() rather than awaiting it — avoids deadlock when this
        # is called from _hot_loop before task_done().  This is a controlled
        # one-shot shutdown task, not a general orphan.
        loop = asyncio.get_running_loop()
        loop.create_task(self.stop(), name="gossip.engine.failed_stop")

    async def _run_ae_cycle(self) -> None:
        """Run one anti-entropy cycle: pick a peer, ping, maybe sync."""
        self.stats.ae_cycles_total += 1
        topo = await self._topology_manager.snapshot()
        eligible = [
            m
            for m in topo.registry
            if m.phase in _ELIGIBLE_PHASES and m.node_id != self._node_id
        ]
        if not eligible:
            logger.debug("GossipEngine AE: no eligible peers.")
            return

        peer = random.choice(eligible)
        await self._ae_ping_pong(peer, topo.epoch)

    async def _ae_ping_pong(self, peer: Member, epoch: int) -> None:
        """Send gossip.ping; if diverged, run digest/delta exchange."""
        if self._serializer is None:
            return
        try:
            fingerprint = await self._topology_manager.member_fingerprint()
            topo = await self._topology_manager.snapshot()
            client = await self._pool.acquire(peer.node_id, peer.peer_address)
            ping_payload = self._serializer.encode(  # type: ignore[union-attr]
                {
                    "sender": self._node_id,
                    "epoch": epoch,
                    "fingerprint": fingerprint,
                    "member_count": len(list(topo.registry)),
                }
            )
            ping_env = Envelope.create(
                ping_payload,
                kind="gossip.ping",
                schema_id=self._serializer.schema_id,  # type: ignore[union-attr]
            )
            pong = await client.request(ping_env)
            if pong.kind != "gossip.pong":
                return

            pong_data = self._serializer.decode(pong.payload)  # type: ignore[union-attr]
            self.stats.last_ae_peer = peer.node_id
            self.stats.last_ae_at = datetime.now(UTC).isoformat()

            same = pong_data.get("same", True)
            logger.debug(
                "AE pong from %s: same=%s epoch=%s members=%s",
                peer.node_id,
                same,
                pong_data.get("epoch"),
                pong_data.get("member_count"),
            )

            if same:
                return  # registries are in sync

            self.stats.ae_diverged += 1
            await self._ae_digest_delta(peer, client)
        except (OSError, ResponseTimeoutError, ConnectionClosedError) as exc:
            logger.warning("gossip.ping to %s failed: %s.", peer.node_id, exc)

    async def _ae_digest_delta(self, peer: Member, client: TcpClient) -> None:
        """Send gossip.digest, merge the received delta, satisfy peer's `wanted`.

        After merging every delta page the initiator inspects each `wanted`
        list returned by the responder and sends a single `gossip.push` back
        carrying every local Member whose node_id appears there. This closes
        the convergence loop symmetrically: each side learns whatever the
        other was missing, in one round-trip per AE cycle.
        """
        try:
            digest_env = await self._build_digest_envelope()
            wanted: set[str] = set()
            async for delta_env in client.stream(digest_env):
                if delta_env.kind == "gossip.error":
                    await self._handle_ae_error(delta_env)
                    return
                if delta_env.kind == "gossip.delta":
                    await self._merge_delta(delta_env, peer.node_id)
                    data = self._serializer.decode(delta_env.payload)  # type: ignore[union-attr]
                    wanted.update(data.get("wanted", []) or [])
                    if not data.get("has_more", False):
                        break
            if wanted:
                await self._satisfy_wanted(peer, client, wanted)
        except (OSError, ResponseTimeoutError, ConnectionClosedError) as exc:
            logger.warning("AE digest/delta with %s failed: %s.", peer.node_id, exc)

    async def _build_digest_envelope(self) -> Envelope:
        """Build the initiator-side gossip.digest envelope from local registry."""
        topo = await self._topology_manager.snapshot()
        digest_members = [
            {"node_id": m.node_id, "generation": m.generation, "seq": m.seq}
            for m in sorted(topo.registry, key=lambda m: m.node_id)
        ]
        payload = self._serializer.encode(  # type: ignore[union-attr]
            {
                "sender": self._node_id,
                "has_more": False,
                "after_node_id": "",
                "members": digest_members,
            }
        )
        return Envelope.create(
            payload,
            kind="gossip.digest",
            schema_id=self._serializer.schema_id,  # type: ignore[union-attr]
        )

    async def _satisfy_wanted(
        self,
        peer: Member,
        client: object,
        wanted: set[str],
    ) -> None:
        """Push to *peer* every local Member whose node_id appears in *wanted*.

        Errors are best-effort: a failure to push back leaves the next AE
        cycle to retry; logged at DEBUG.
        """
        if self._serializer is None:
            return
        from tourillon.core.transport.client import TcpClient

        assert isinstance(client, TcpClient)
        topo = await self._topology_manager.snapshot()
        to_send = [m for m in topo.registry if m.node_id in wanted]
        if not to_send:
            return
        try:
            payload = self._serializer.encode(  # type: ignore[union-attr]
                {
                    "sender": self._node_id,
                    "ttl": 1,
                    "members": [m.to_dict() for m in to_send],
                }
            )
            env = Envelope.create(
                payload,
                kind="gossip.push",
                schema_id=self._serializer.schema_id,  # type: ignore[union-attr]
            )
            resp = await client.request(env)
            if resp.kind == "gossip.push.ok":
                self.stats.push_sent_total += 1
        except (OSError, ResponseTimeoutError, ConnectionClosedError) as exc:
            logger.debug("AE wanted-push to %s failed: %s.", peer.node_id, exc)

    async def _merge_delta(self, delta_env: Envelope, peer_node_id: str) -> None:
        """Merge a gossip.delta envelope into the local registry.

        Validates partition_shift on each member before merging. On mismatch,
        closes the connection and logs WARNING; local node does NOT transition
        to FAILED as initiator (per proposal invariant).
        """

        try:
            data = self._serializer.decode(delta_env.payload)  # type: ignore[union-attr]
            members = []
            for m in data.get("members", []):
                if m["partition_shift"] != self._partition_shift:
                    logger.warning(
                        "AE delta from %s contains member %r with "
                        "partition_shift=%d; local=%d. Closing connection.",
                        peer_node_id,
                        m["node_id"],
                        m["partition_shift"],
                        self._partition_shift,
                    )
                    return  # close implicitly on next pool acquire
                members.append(
                    Member(
                        node_id=m["node_id"],
                        peer_address=m.get("peer_address", ""),
                        generation=m["generation"],
                        seq=m["seq"],
                        phase=MemberPhase(m["phase"]),
                        tokens=tuple(m.get("tokens", [])),
                        partition_shift=m["partition_shift"],
                    )
                )
            await self._topology_manager.merge_registry(members)
        except Exception as exc:
            logger.warning("Failed to merge delta from %s: %s.", peer_node_id, exc)

    @staticmethod
    async def _handle_ae_error(env: Envelope) -> None:
        """Handle gossip.error received during AE cycle."""
        logger.warning("Received gossip.error during AE: %s.", env.kind)

