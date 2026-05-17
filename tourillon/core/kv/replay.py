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
"""HintReplayManager — drives hint replay when a target node returns READY.

Triggered by gossip: when phase(node_id) == READY, the manager waits
hint_replay_stabilization_ms before starting a ReplaySession. If the
phase changes before the timer expires, it is cancelled.
"""

from __future__ import annotations

import asyncio
import logging
import random
from typing import TYPE_CHECKING

from tourillon.core.lifecycle.member import MemberPhase
from tourillon.core.ports.transport import ConnectionClosedError, ResponseTimeoutError
from tourillon.core.structure.envelope import Envelope
from tourillon.core.structure.record import KvMetadata, StoreKey, Tombstone, Version

if TYPE_CHECKING:
    from tourillon.core.lifecycle.probe import ProbeManager
    from tourillon.core.ports.serializer import SerializerPort
    from tourillon.core.ports.storage import Storage
    from tourillon.core.ring.partitioner import Partitioner
    from tourillon.core.ring.topology import TopologyManager
    from tourillon.core.transport.pool import PeerClientPool

logger = logging.getLogger(__name__)

_BACKOFF_BASE: float = 1.0
_BACKOFF_MAX: float = 60.0
_JITTER_FRACTION: float = 0.2


class HintReplayManager:
    """Manages hint replay sessions, one per target node_id.

    Listens for phase transitions via ``on_phase_change()``.  When a target
    node enters READY, starts a stabilisation timer.  Launches a replay
    session after the timer expires, unless the phase changes back.
    """

    def __init__(
        self,
        node_id: str,
        storage: Storage,
        partitioner: Partitioner,
        pool: PeerClientPool,
        probe_manager: ProbeManager,
        serializer: SerializerPort,
        topology_manager: TopologyManager,
        stabilization_ms: int = 2000,
        concurrency: int = 16,
        fanout_timeout: float = 0.05,
    ) -> None:
        self._node_id = node_id
        self._storage = storage
        self._partitioner = partitioner
        self._pool = pool
        self._probe_manager = probe_manager
        self._serializer = serializer
        self._topology_manager = topology_manager
        self._stabilization_ms = stabilization_ms
        self._concurrency = concurrency
        self._fanout_timeout = fanout_timeout
        # node_id → current stabilisation/session task
        self._tasks: dict[str, asyncio.Task[None]] = {}

    def on_phase_change(self, target_id: str, new_phase: MemberPhase) -> None:
        """Called by gossip when a member's phase changes.

        Starts or cancels a stabilisation timer based on the new phase.
        Safe to call from any coroutine context.
        """
        if new_phase == MemberPhase.READY:
            self._start_stabilisation(target_id)
        else:
            self._cancel_session(target_id)

    def _start_stabilisation(self, target_id: str) -> None:
        """Cancel any existing task for target_id and start a new stabilisation timer."""
        self._cancel_session(target_id)
        loop = asyncio.get_running_loop()
        task = loop.create_task(
            self._stabilise_then_replay(target_id),
            name=f"hint.stabilise.{target_id}",
        )
        self._tasks[target_id] = task

    def _cancel_session(self, target_id: str) -> None:
        """Cancel any running stabilisation/session task for target_id."""
        task = self._tasks.pop(target_id, None)
        if task is not None and not task.done():
            task.cancel()

    async def _stabilise_then_replay(self, target_id: str) -> None:
        """Wait stabilisation_ms, then run a replay session with backoff."""
        try:
            delay = self._stabilization_ms / 1000.0
            await asyncio.sleep(delay)
            backoff = _BACKOFF_BASE
            while True:
                try:
                    await self._replay_session(target_id)
                    return
                except _SessionAbortedError:
                    return
                except Exception:
                    logger.warning(
                        "Hint replay session for %s failed, retrying.", target_id
                    )
                jitter = random.uniform(
                    -_JITTER_FRACTION, _JITTER_FRACTION
                )  # noqa: S311
                wait = min(backoff * (1 + jitter), _BACKOFF_MAX)
                await asyncio.sleep(wait)
                backoff = min(backoff * 2, _BACKOFF_MAX)
        except asyncio.CancelledError:
            logger.debug("Hint stabilisation for %s cancelled.", target_id)

    async def _replay_session(self, target_id: str) -> None:
        """Replay all pending hints for target_id.

        Raises _SessionAborted when target_id is no longer READY.
        """
        logger.info("Hint replay session started for %s.", target_id)
        sem = asyncio.Semaphore(self._concurrency)

        async def _replay_hint(
            pid: int, addr: StoreKey, record: Version | Tombstone
        ) -> None:
            async with sem:
                value_bytes: bytes | None = (
                    record.value if isinstance(record, Version) else None
                )
                meta = KvMetadata(hlc=record.metadata, quorum_write=record.quorum_write)
                payload = self._serializer.encode(
                    {
                        "key": addr.key,
                        "keyspace": addr.keyspace,
                        "value": value_bytes,
                        "hlc": meta.hlc.to_dict(),
                        "quorum_write": meta.quorum_write,
                    }
                )
                env = Envelope.create(
                    payload, kind="kv.replicate", schema_id=self._serializer.schema_id
                )
                try:
                    topology = await self._topology_manager.snapshot()
                    member = topology.registry.get(target_id)
                    if member is None:
                        raise _SessionAbortedError(f"{target_id} not in registry")
                    client = await self._pool.acquire(target_id, member.peer_address)
                    async with asyncio.timeout(self._fanout_timeout):
                        resp = await client.request(env, timeout=self._fanout_timeout)
                    if resp.kind == "kv.replicate.ok":
                        await self._probe_manager.record_heartbeat(target_id)
                        store = self._storage.open_partition(pid)
                        hint_ctx = store.hint(target_id)
                        await hint_ctx.mark_stale(addr, record.metadata)
                        logger.debug(
                            "Hint replayed: pid=%d key=%r to %s",
                            pid,
                            addr.key,
                            target_id,
                        )
                except (ResponseTimeoutError, ConnectionClosedError, TimeoutError):
                    await self._probe_manager.record_miss(target_id)
                    raise

        tasks: list[asyncio.Task[None]] = []
        async with asyncio.TaskGroup() as tg:
            for pid, records in self._collect_hints(target_id):
                for record in records:
                    tasks.append(
                        tg.create_task(
                            _replay_hint(pid, record.address, record),  # type: ignore[arg-type]
                            name=f"hint.replay.{pid}.{record.metadata}",
                        )
                    )

        logger.info(
            "Hint replay session complete for %s (%d hints).", target_id, len(tasks)
        )

    def _collect_hints(self, target_id: str) -> list[tuple[int, list]]:
        """Collect all pending hint records from all partitions for target_id.

        Note: this is a synchronous helper; callers must be async and should
        iterate pending() in an async context.
        """
        result: list[tuple[int, list]] = []
        for pid in range(self._partitioner.total_partitions):
            store = self._storage.open_partition(pid)
            hint_ctx = store.hint(target_id)
            # InMemoryPartitionHint exposes hint_records() for sync access
            if hasattr(hint_ctx, "hint_records"):
                stale_hlcs = getattr(hint_ctx, "_stale_hlcs", set())
                records = [r for r in hint_ctx.hint_records() if r.metadata not in stale_hlcs]  # type: ignore[union-attr]
                if records:
                    result.append((pid, records))
        return result


class _SessionAbortedError(Exception):
    """Raised internally when a replay session must stop due to phase change."""
