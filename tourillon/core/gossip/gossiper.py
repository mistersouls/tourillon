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
import contextlib
import hashlib
import math
import random
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import partial
from typing import Any

from tourillon.core.gossip.topology import TopologyManager
from tourillon.core.log import logger
from tourillon.core.models.config import GossipConfig
from tourillon.core.models.member import Member, MemberPhase
from tourillon.core.peer.pool import PeerPool
from tourillon.core.ports.telemetry import Telemetry
from tourlib import Serializer, ValidationError
from tourlib.constant import PROTOCOL_VERSION
from tourlib.envelope import Envelope
from tourlib.exceptions import EnvelopeError
from tourlib.transport import EnvelopeClient


@dataclass
class GossipStats:
    """Mutable counters tracking gossip activity since engine start."""

    known_members: int = 0
    push_sent_total: int = 0
    push_recv_total: int = 0
    ae_cycles_total: int = 0
    ae_diverged: int = 0
    last_ae_peer: str | None = None
    last_ae_at: str | None = None
    bootstrap_ok_total: int = 0
    bootstrap_err_total: int = 0
    last_hot_failure: str | None = None
    last_hot_failure_at: str | None = None
    last_ae_failure: str | None = None
    last_ae_failure_at: str | None = None
    last_stop_failure: str | None = None
    last_stop_failure_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
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
            "last_hot_failure": self.last_hot_failure,
            "last_hot_failure_at": self.last_hot_failure_at,
            "last_ae_failure": self.last_ae_failure,
            "last_ae_failure_at": self.last_ae_failure_at,
            "last_stop_failure": self.last_stop_failure,
            "last_stop_failure_at": self.last_stop_failure_at,
        }


class Gossiper:
    """Anti-entropic gossip engine driving cluster membership convergence.

    Two independent loops run inside an asyncio.TaskGroup:
    - _hot_loop: drains hot_queue and sends gossip.push to K peers.
    - _ae_loop:  fires every anti_entropy_interval seconds; runs ping → pong →
                 digest → delta if divergence detected.

    All member state is delegated to TopologyManager. Internal state is limited
    to hot_queue and the node's own epoch counter (from NodeState.epoch at startup).

    Every push carries a ``hop_limit``. A responder that accepts at least one
    newer member forwards it through :meth:`relay` with ``hop_limit - 1`` to a
    fresh random fan-out that excludes the sender, so an announcement reaches
    nodes several hops away without waiting for the slower AE loop.

    Peer clients are always borrowed through ``PeerPool.lease`` so every
    exchange releases its connection reference, even on failure.

    stop() drains the hot_queue before cancelling both loops, guaranteeing that
    any announce() call made before stop() — including critical FAILED
    announcements — is fully sent before the engine terminates.
    """

    _MAX_PENDING_RELAYS = 64

    def __init__(
        self,
        node_id: str,
        config: GossipConfig,
        stats: GossipStats,
        topologymgr: TopologyManager,
        peer_pool: PeerPool,
        serializer: Serializer,
        telemetry: Telemetry,
    ) -> None:
        self._node_id = node_id
        self._config = config
        self._stats = stats
        self._topologymgr = topologymgr
        self._peer_pool = peer_pool
        self._serializer = serializer
        self._telemetry = telemetry

        self._hot_task: asyncio.Task | None = None
        self._ae_task: asyncio.Task | None = None
        self._hot_queue: asyncio.Queue[Member] = asyncio.Queue()
        self._relay_tasks: set[asyncio.Task[None]] = set()
        self._update_hot_queue_size_metric()

    async def start(self) -> None:
        """Start gossip background loops and return immediately."""
        if self._is_running():
            logger.debug(
                "gossip.start ignored; engine already running node_id=%s",
                self._node_id,
            )
            return

        loop = asyncio.get_running_loop()
        hot_task = loop.create_task(
            self._hot_loop(),
            name=f"gossip-hot-{self._node_id}",
        )
        hot_task.add_done_callback(partial(self._on_loop_done, "hot"))
        ae_task = loop.create_task(
            self._ae_loop(),
            name=f"gossip-ae-{self._node_id}",
        )
        ae_task.add_done_callback(partial(self._on_loop_done, "ae"))
        self._hot_task = hot_task
        self._ae_task = ae_task
        logger.info("gossip.started node_id=%s", self._node_id)

    async def stop(self) -> None:
        """Stop gossip background loops."""
        tasks = [task for task in (self._hot_task, self._ae_task) if task is not None]
        if not tasks:
            logger.debug(
                "gossip.stop ignored; engine not running node_id=%s",
                self._node_id,
            )
            return

        if self._hot_task is not None and not self._hot_task.done():
            timeout_s = self._config.stop_drain_timeout
            try:
                await asyncio.wait_for(
                    self._hot_queue.join(),
                    timeout=timeout_s,
                )
            except TimeoutError:
                self._record_stop_failure(
                    "hot queue drain timed out " f"after {timeout_s}s"
                )
                logger.error(
                    "gossip.stop hot queue drain timed out node_id=%s timeout_s=%s pending=%s",
                    self._node_id,
                    timeout_s,
                    self._hot_queue.qsize(),
                )
        elif not self._hot_queue.empty():
            logger.warning(
                "gossip.stop hot queue not drained; hot loop unavailable node_id=%s pending=%s",
                self._node_id,
                self._hot_queue.qsize(),
            )

        for task in tasks:
            if not task.done():
                task.cancel()

        for task in tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task

        await self._cancel_relays()
        self._hot_task = None
        self._ae_task = None
        self._update_hot_queue_size_metric()
        logger.info("gossip.stopped node_id=%s", self._node_id)

    async def announce(self, member: Member) -> None:
        """Enqueue member for immediate hot-path propagation."""
        await self._hot_queue.put(member)
        self._update_hot_queue_size_metric()
        if member.phase == MemberPhase.FAILED:
            self._telemetry.gossip_failed_announcements_total.add()

    def status(self) -> dict[str, Any]:
        """Return a lightweight health snapshot for introspection endpoints."""
        return {
            "node_id": self._node_id,
            "hot_running": self._hot_task is not None and not self._hot_task.done(),
            "ae_running": self._ae_task is not None and not self._ae_task.done(),
            "hot_queue_size": self._hot_queue.qsize(),
            "last_failures": {
                "hot": {
                    "error": self._stats.last_hot_failure,
                    "at": self._stats.last_hot_failure_at,
                },
                "ae": {
                    "error": self._stats.last_ae_failure,
                    "at": self._stats.last_ae_failure_at,
                },
                "stop": {
                    "error": self._stats.last_stop_failure,
                    "at": self._stats.last_stop_failure_at,
                },
            },
            "stats": self._stats.to_dict(),
        }

    def _is_running(self) -> bool:
        return any(
            task is not None and not task.done()
            for task in (self._hot_task, self._ae_task)
        )

    async def _cancel_relays(self) -> None:
        relays = list(self._relay_tasks)

        for task in relays:
            task.cancel()

        await asyncio.gather(*relays, return_exceptions=True)
        self._relay_tasks.clear()

    def _on_loop_done(self, loop_name: str, task: asyncio.Task) -> None:
        if task.cancelled():
            return
        exception = task.exception()
        if exception is None:
            return

        error = f"{type(exception).__name__}: {exception}"
        when = self._utc_now()
        if loop_name == "hot":
            self._stats.last_hot_failure = error
            self._stats.last_hot_failure_at = when
            return

        self._stats.last_ae_failure = error
        self._stats.last_ae_failure_at = when

    def _record_stop_failure(self, error: str) -> None:
        self._stats.last_stop_failure = error
        self._stats.last_stop_failure_at = self._utc_now()

    @staticmethod
    def _utc_now() -> str:
        return datetime.now(UTC).isoformat()

    @staticmethod
    def _unix_now() -> int:
        return int(datetime.now(UTC).timestamp())

    @staticmethod
    def _stable_peer_id_value(peer_node_id: str) -> int:
        digest = hashlib.blake2b(peer_node_id.encode("utf-8"), digest_size=8).digest()
        return int.from_bytes(digest, byteorder="big", signed=False)

    def _update_hot_queue_size_metric(self) -> None:
        self._telemetry.gossip_hot_queue_size.set(self._hot_queue.qsize())

    def _set_last_ae_metrics(self, peer_node_id: str) -> None:
        self._telemetry.gossip_last_ae_peer.set(
            self._stable_peer_id_value(peer_node_id)
        )
        self._telemetry.gossip_last_ae_at.set(self._unix_now())

    def _build_envelope(
        self, kind: str, payload: Any, mode: int = Envelope.MODE_RPC
    ) -> Envelope:
        return Envelope(
            kind=kind,
            correlation_id=uuid.uuid4(),
            mode=mode,
            proto_version=PROTOCOL_VERSION,
            schema_id=self._serializer.schema_id,
            payload=self._serializer.encode(payload),
        )

    async def _hot_loop(self) -> None:
        while True:
            got = False
            try:
                member = await self._hot_queue.get()
                got = True
                await self._push_member(member)
            finally:
                # Avoid task_done imbalance when cancelled
                # before queue get succeeds.
                if got:
                    self._hot_queue.task_done()
                    self._update_hot_queue_size_metric()

    async def _ae_loop(self) -> None:
        while True:
            try:
                await self._run_ae_cycle()
            except Exception as exc:
                logger.warning(
                    "gossip.ae_cycle failed node_id=%s error=%s", self._node_id, exc
                )
            await asyncio.sleep(self._config.ae_interval)

    async def _eligible_members(self) -> list[Member]:
        topology = await self._topologymgr.snapshot()
        eligible_phases = (MemberPhase.READY, MemberPhase.JOINING, MemberPhase.DRAINING)
        eligible = [
            m
            for m in topology.registry
            if m.phase in eligible_phases and m.node_id != self._node_id
        ]
        return eligible

    def relay(self, members: list[Member], hop_limit: int, sender: str) -> None:
        """Forward freshly accepted members one more gossip hop.

        Called by the ``gossip.push`` responder for the members it actually
        applied. ``hop_limit`` is the budget *remaining* for the forwarded
        push (the caller already decremented it); a budget of zero ends the
        epidemic. Relays run as background tasks so the responder never waits
        on outbound pushes, and the number of in-flight relays is bounded so a
        burst of membership changes cannot fan out without limit. A member is
        only re-forwarded when it was newer than the local view, so duplicate
        pushes die out on their own even before the hop budget is spent.
        """
        if hop_limit < 1 or not members or not self._is_running():
            return

        if len(self._relay_tasks) >= self._MAX_PENDING_RELAYS:
            logger.debug(
                "gossip.relay dropped; too many pending relays node_id=%s",
                self._node_id,
            )
            return

        task = asyncio.get_running_loop().create_task(
            self._relay(list(members), hop_limit, sender),
            name=f"gossip-relay-{self._node_id}",
        )

        self._relay_tasks.add(task)
        task.add_done_callback(self._relay_tasks.discard)

    async def _relay(self, members: list[Member], hop_limit: int, sender: str) -> None:
        excluded = {sender, *(member.node_id for member in members)}
        for member in members:
            await self._push_member(member, hop_limit=hop_limit, excluded=excluded)

    async def _push_member(
        self,
        member: Member,
        *,
        hop_limit: int | None = None,
        excluded: set[str] | None = None,
    ) -> None:
        eligible = [
            peer
            for peer in await self._eligible_members()
            if not excluded or peer.node_id not in excluded
        ]

        if not eligible:
            logger.debug(
                "gossip._push_member no eligible peers node_id=%s member=%s",
                self._node_id,
                member.node_id,
            )
            return

        n = len(eligible)
        k = min(math.ceil(math.log2(n + 1)) + 1, self._config.max_fan_out)
        targets = random.sample(eligible, min(k, n))
        budget = self._config.hop_limit if hop_limit is None else hop_limit

        await asyncio.gather(
            *(self._send_push(member, target, budget) for target in targets),
            return_exceptions=True,
        )

    async def _send_push(
        self, member: Member, target: Member, hop_limit: int | None = None
    ) -> None:
        self._telemetry.gossip_push_attempt_total.add()
        with self._telemetry.start_span(
            "gossip.push",
            attributes={"target": target.node_id, "member": member.node_id},
        ) as span:
            try:
                payload = {
                    "sender": self._node_id,
                    "hop_limit": (
                        self._config.hop_limit if hop_limit is None else hop_limit
                    ),
                    "members": [member.to_dict()],
                }
                envelope = self._build_envelope("gossip.push", payload)

                async with self._peer_pool.lease(target.node_id) as client:
                    resp = await client.request(envelope)

                if resp.kind == "gossip.error":
                    self._telemetry.gossip_push_error_total.add()
                    span.set_attribute("response.kind", resp.kind)
                    await self._handle_push_error(resp, target)
                    return
                if resp.kind == "gossip.push.accept":
                    data: dict[str, Any] = self._serializer.decode(resp.payload)
                    self._stats.push_sent_total += 1
                    if data.get("accepted", 0) == 0:
                        logger.debug(
                            "gossip.push no members accepted node_id=%s target=%s",
                            self._node_id,
                            target.node_id,
                        )
                        return
            except (EnvelopeError, ValidationError) as exc:
                self._telemetry.gossip_push_error_total.add()
                span.record_exception(exc)
                span.set_status_error(str(exc))
                logger.warning(
                    "gossip._send_push failed node_id=%s target=%s error=%s",
                    self._node_id,
                    target.node_id,
                    exc,
                )

    async def _handle_push_error(self, envelope: Envelope, target: Member) -> None:
        try:
            data: dict[str, Any] = self._serializer.decode(envelope.payload)
            error_msg = data.get("error", "unknown error")
            logger.warning(
                "gossip.push rejected by peer node_id=%s target=%s error=%s",
                self._node_id,
                target.node_id,
                error_msg,
            )
            code = data.get("code", "")
        except Exception as exc:
            logger.warning(
                "gossip.push rejected by peer node_id=%s target=%s cid=%.8s "
                "error='failed to decode payload: %s'",
                self._node_id,
                target.node_id,
                envelope.correlation_id,
                exc,
            )
            code = ""

        if code == "todo: critical error should trigger stopping program":
            # not implemented yet
            logger.critical(
                "gossip.push critical error received; stopping "
                "node_id=%s target=%s cid=%.8s",
                self._node_id,
                target.node_id,
                envelope.correlation_id,
            )

    async def _run_ae_cycle(self) -> None:
        self._stats.ae_cycles_total += 1
        self._telemetry.gossip_ae_cycle_total.add()
        eligible = await self._eligible_members()
        if not eligible:
            logger.debug(
                "gossip.ae_cycle no eligible peers node_id=%s",
                self._node_id,
            )
            return

        peer = random.choice(eligible)
        await self._ae_ping_pong(peer)

    async def _ae_ping_pong(self, peer: Member) -> None:
        with self._telemetry.start_span(
            "gossip.ae_ping_pong",
            attributes={"peer": peer.node_id},
        ) as span:
            try:
                async with self._peer_pool.lease(peer.node_id) as client:
                    await self._ae_exchange(peer, client)
            except (EnvelopeError, ValidationError) as exc:
                span.record_exception(exc)
                span.set_status_error(str(exc))
                logger.warning(
                    "gossip.ping failed node_id=%s peer=%s error=%s",
                    self._node_id,
                    peer.node_id,
                    exc,
                )

    async def _ae_exchange(self, peer: Member, client: EnvelopeClient) -> None:
        fingerprint = await self._topologymgr.member_fingerprint()
        topology = await self._topologymgr.snapshot()

        payload = {
            "sender": self._node_id,
            "fingerprint": fingerprint,
            "count": len(topology.registry),
        }

        ping = self._build_envelope("gossip.ping", payload)
        pong = await client.request(ping)

        if pong.kind != "gossip.pong":
            return

        pong_data: dict[str, Any] = self._serializer.decode(pong.payload)
        self._stats.last_ae_peer = peer.node_id
        self._stats.last_ae_at = self._utc_now()
        self._set_last_ae_metrics(peer.node_id)

        same = pong_data.get("same", True)

        logger.debug(
            "gossip.ping result node_id=%s peer=%s same=%s fingerprint=%.8s",
            self._node_id,
            peer.node_id,
            same,
            fingerprint,
        )

        if same:
            return

        self._stats.ae_diverged += 1
        self._telemetry.gossip_ae_diverged.add()
        await self._ae_digest_delta(peer, client)

    async def _ae_digest_delta(self, peer: Member, client: EnvelopeClient) -> None:
        """
        Send gossip.digest, merge the received delta, satisfy peer's `wanted`.

        After merging every delta page the initiator inspects each `wanted`
        list returned by the responder and sends a single `gossip.push` back
        carrying every local Member whose node_id appears there. This closes
        the convergence loop symmetrically: each side learns whatever the
        other was missing, in one round-trip per AE cycle.
        """
        try:
            digest = await self._build_digest()
            wanted: set[str] = set()
            async for delta in client.stream(digest):
                if delta.kind == "gossip.error":
                    await self._handle_ae_error(delta, peer)
                    return
                if delta.kind == "gossip.delta":
                    await self._merge_delta(delta, peer.node_id)
                    data: dict[str, Any] = self._serializer.decode(delta.payload)
                    wanted.update(data.get("wanted", []) or [])
                    if not data.get("has_more", False):
                        break

            if wanted:
                await self._satisfy_wanted(peer, client, wanted)
        except (EnvelopeError, ValidationError) as exc:
            logger.warning(
                "gossip.digest/delta failed node_id=%s peer=%s error=%s",
                self._node_id,
                peer.node_id,
                exc,
            )

    async def _build_digest(self) -> Envelope:
        topology = await self._topologymgr.snapshot()
        digest = [
            {"node_id": m.node_id, "epoch": m.epoch}
            for m in sorted(topology.registry, key=lambda m: m.node_id)
        ]
        payload = {
            "sender": self._node_id,
            "has_more": False,
            "after_node_id": "",
            "digest": digest,
        }
        return self._build_envelope("gossip.digest", payload)

    async def _handle_ae_error(self, envelope: Envelope, peer: Member) -> None:
        try:
            data: dict[str, Any] = self._serializer.decode(envelope.payload)
            logger.warning(
                "Received gossip.error during AE cycle node_id=%s peer=%s error=%s",
                self._node_id,
                peer.node_id,
                data.get("error", "unknown error"),
            )
        except Exception as exc:
            logger.warning(
                "Received gossip.error during AE cycle node_id=%s peer=%s cid=%.8s "
                "error='failed to decode payload: %s'",
                self._node_id,
                peer.node_id,
                envelope.correlation_id,
                exc,
            )

    async def _satisfy_wanted(
        self, peer: Member, client: EnvelopeClient, wanted: set[str]
    ) -> None:
        topology = await self._topologymgr.snapshot()
        to_send = [m.to_dict() for m in topology.registry if m.node_id in wanted]
        if not to_send:
            logger.debug(
                "gossip.satisfy_wanted no members to send node_id=%s peer=%s",
                self._node_id,
                peer.node_id,
            )
            return

        try:
            payload = {
                "sender": self._node_id,
                "hop_limit": 1,
                "members": to_send,
            }
            envelope = self._build_envelope("gossip.push", payload)
            resp = await client.request(envelope)
            if resp.kind == "gossip.push.accept":
                self._stats.push_sent_total += 1
        except (EnvelopeError, ValidationError) as exc:
            logger.warning(
                "gossip.satisfy_wanted failed node_id=%s peer=%s error=%s",
                self._node_id,
                peer.node_id,
                exc,
            )

    async def _merge_delta(self, delta: Envelope, peer_node_id: str) -> None:
        try:
            data: dict[str, Any] = self._serializer.decode(delta.payload)
            members: list[Member] = []
            for m in data.get("members", []):
                # need to check some conditions
                member = Member.from_dict(m)
                members.append(member)
            await self._topologymgr.merge_registry(members)
        except (EnvelopeError, ValidationError) as exc:
            logger.warning(
                "Failed to merge delta from peer node_id=%s error=%s", peer_node_id, exc
            )
