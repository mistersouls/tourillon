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
"""KvCoordinator — write/read/delete fanout with sloppy-quorum logic.

This is pure domain logic: it depends only on port protocols (never on
infra adapters).  The coordinator is instantiated once per node and handles
all client-facing kv.put / kv.get / kv.delete requests.

Design:
  * write path: generate HLC per-partition, build preference list, fan out
                in parallel.
  * read path:  fan out kv.fetch, group by version, determine confirmed
                version, trigger background read-repair.
  * The coordinator contacts itself directly (no loopback network call)
    when it is a target in the preference list.
  * HLC clocks are per-partition (dict[pid, HLCClock]).  Each clock is
    initialised lazily on the first write to that partition: max_hlc() is
    called on the DBI_LOG to seed restore(), guaranteeing monotonicity even
    after a wall-clock regression.  The double-check pattern avoids a lock
    while remaining safe under concurrent first-tick calls on the same pid.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from tourillon.core.ports.transport import ConnectionClosedError, ResponseTimeoutError
from tourillon.core.structure.clock import HLCClock, HLCTimestamp
from tourillon.core.structure.record import (
    KvMetadata,
    Record,
    StoreKey,
    Tombstone,
    Version,
)

if TYPE_CHECKING:
    from tourillon.core.lifecycle.probe import ProbeManager
    from tourillon.core.ports.serializer import SerializerPort
    from tourillon.core.ports.storage import Storage
    from tourillon.core.ring.partitioner import Partitioner
    from tourillon.core.ring.placement import PreferenceEntry, SimplePreferenceStrategy
    from tourillon.core.ring.topology import TopologyManager
    from tourillon.core.transport.pool import PeerClientPool

logger = logging.getLogger(__name__)


@dataclass
class VersionCandidate:
    """One group of replicas returning the same record version."""

    record: Record
    count: int

    @property
    def hlc(self) -> HLCTimestamp:
        """Return the HLC of this candidate."""
        return self.record.metadata

    @property
    def quorum_write(self) -> int:
        """Return the quorum_write stored in this candidate."""
        return self.record.quorum_write

    @property
    def confirmed(self) -> bool:
        """Return True if count >= quorum_write."""
        return self.count >= self.quorum_write

    def to_dict(self) -> dict[str, object]:
        """Return the wire dict for kv.get.ok versions list."""
        value_bytes: bytes | None = (
            self.record.value if isinstance(self.record, Version) else None
        )
        return {
            "confirmed": self.confirmed,
            "value": value_bytes,
            "hlc": self.hlc.to_dict(),
            "count": self.count,
            "quorum_write": self.quorum_write,
        }


class KvCoordinator:
    """Handles kv.put / kv.get / kv.delete on behalf of the local node.

    The coordinator receives a request, determines the preference list for the
    affected key, fans out in parallel, and collects quorum acknowledgements.

    ``node_id`` is the identifier of the local node.  When the local node
    appears in the preference list the coordinator calls the local store
    directly instead of going over the network.
    """

    def __init__(
        self,
        node_id: str,
        storage: Storage,
        partitioner: Partitioner,
        topology_manager: TopologyManager,
        probe_manager: ProbeManager,
        placement_strategy: SimplePreferenceStrategy,
        pool: PeerClientPool,
        serializer: SerializerPort,
        fanout_timeout: float = 0.05,
    ) -> None:
        self._node_id = node_id
        self._storage = storage
        self._partitioner = partitioner
        self._topology_manager = topology_manager
        self._probe_manager = probe_manager
        self._strategy = placement_strategy
        self._pool = pool
        self._serializer = serializer
        self._fanout_timeout = fanout_timeout
        # One HLCClock per partition — initialised lazily on first write.
        # See _tick() for the double-check initialisation pattern.
        self._clocks: dict[int, HLCClock] = {}

    async def _tick(self, pid: int) -> HLCTimestamp:
        """Return the next HLC timestamp for the given partition.

        Lazily initialises the per-partition HLCClock on first call, seeding
        it from the partition's DBI_LOG so that the clock starts strictly
        above any timestamp already written — even after a wall-clock
        regression (NTP, VM snapshot restore, etc.).

        Uses the asyncio double-check pattern to protect the initialisation
        against concurrent first-tick calls on the same pid:

        1. Guard check before the await — fast path for the common case.
        2. await max_hlc() — only I/O yield point; another coroutine may
           initialise this pid while we wait.
        3. Guard check after the await — if the clock was created by another
           coroutine during step 2, we reuse it and skip re-initialisation.
        4. tick() is synchronous with no await, so the return is atomic from
           the event-loop's perspective; no lock is needed.
        """
        if pid not in self._clocks:
            ts = await self._storage.open_partition(pid).max_hlc()
            if pid not in self._clocks:
                clock = HLCClock(self._node_id)
                if ts is not None:
                    clock.restore(ts.wall_ms, ts.counter)
                self._clocks[pid] = clock
        return self._clocks[pid].tick()

    async def _peer_address(self, node_id: str) -> str:
        """Return the peer server address for node_id from the current topology.

        Raises KeyError when node_id is absent from the registry; callers must
        handle this and treat the replica as unreachable. The coordinator uses
        the peer address (not the KV port) because inter-node KV handlers
        (kv.replicate, kv.hint, kv.fetch) are registered on the peer dispatcher.
        """
        topology = await self._topology_manager.snapshot()
        member = topology.registry.get(node_id)
        if member is None:
            raise KeyError(f"node_id {node_id!r} not found in registry")
        return member.peer_address

    # ------------------------------------------------------------------
    # Write path
    # ------------------------------------------------------------------

    async def put(
        self,
        key: bytes,
        keyspace: bytes,
        value: bytes,
        quorum_write: int,
    ) -> dict[str, Any]:
        """Execute a kv.put and return the kv.put.ok payload dict.

        Raise KvError on quorum failure.
        """
        addr = StoreKey(keyspace=keyspace, key=key)
        pid = self._partitioner.pid_for_addr(addr)
        hlc = await self._tick(pid)
        meta = KvMetadata(hlc=hlc, quorum_write=quorum_write)
        return await self._write(addr, value, meta, is_delete=False)

    async def delete(
        self,
        key: bytes,
        keyspace: bytes,
        quorum_write: int,
    ) -> dict[str, Any]:
        """Execute a kv.delete and return the kv.delete.ok payload dict.

        Raise KvError on quorum failure.
        """
        addr = StoreKey(keyspace=keyspace, key=key)
        pid = self._partitioner.pid_for_addr(addr)
        hlc = await self._tick(pid)
        meta = KvMetadata(hlc=hlc, quorum_write=quorum_write)
        return await self._write(addr, None, meta, is_delete=True)

    async def _write(
        self,
        addr: StoreKey,
        value: bytes | None,
        meta: KvMetadata,
        *,
        is_delete: bool,
    ) -> dict[str, Any]:
        """Fan out to all replicas and wait for quorum acks."""
        topology = await self._topology_manager.snapshot()
        placement = self._partitioner.placement_for_addr(addr, topology.ring)
        pref_list = await self._strategy.preference_list(
            placement, topology, self._probe_manager
        )

        tasks: list[asyncio.Task[bool]] = []

        async def _do_replicate(entry: PreferenceEntry) -> bool:
            return await self._replicate_one(entry, addr, value, meta)

        async def _do_hint(entry: PreferenceEntry) -> bool:
            return await self._hint_one(entry, addr, value, meta)

        async with asyncio.TaskGroup() as tg:
            for entry in pref_list:
                if entry.handoff is not None:
                    tasks.append(
                        tg.create_task(_do_hint(entry), name=f"hint.{entry.node_id}")
                    )
                else:
                    tasks.append(
                        tg.create_task(
                            _do_replicate(entry), name=f"repl.{entry.node_id}"
                        )
                    )

        acks = sum(1 for t in tasks if t.result())
        if acks < meta.quorum_write:
            raise KvError(
                f"quorum_write_unavailable ({acks}/{meta.quorum_write} acks, "
                f"timeout after {int(self._fanout_timeout * 1000)} ms)"
            )

        return {"hlc": meta.hlc.to_dict(), "replicas": acks}

    async def _replicate_one(
        self,
        entry: PreferenceEntry,
        addr: StoreKey,
        value: bytes | None,
        meta: KvMetadata,
    ) -> bool:
        """Send kv.replicate to one replica. Returns True on success."""
        if entry.node_id == self._node_id:
            # Local write — no network hop
            try:
                store = self._storage.open_partition(
                    self._partitioner.pid_for_addr(addr)
                )
                if value is None:
                    await store.delete(addr, meta)
                else:
                    await store.put(addr, value, meta)
                return True
            except Exception:
                logger.exception("local replicate failed")
                return False

        payload = self._serializer.encode(
            {
                "key": addr.key,
                "keyspace": addr.keyspace,
                "value": value,
                "hlc": meta.hlc.to_dict(),
                "quorum_write": meta.quorum_write,
            }
        )
        from tourillon.core.structure.envelope import Envelope

        env = Envelope.create(
            payload, kind="kv.replicate", schema_id=self._serializer.schema_id
        )
        try:
            address = await self._peer_address(entry.node_id)
            client = await self._pool.acquire(entry.node_id, address)
            async with asyncio.timeout(self._fanout_timeout):
                resp = await client.request(env, timeout=self._fanout_timeout)
            if resp.kind == "kv.replicate.ok":
                await self._probe_manager.record_heartbeat(entry.node_id)
                return True
            return False
        except (ResponseTimeoutError, ConnectionClosedError, TimeoutError):
            await self._probe_manager.record_miss(entry.node_id)
            return False
        except Exception:
            logger.exception("replicate to %s failed", entry.node_id)
            return False

    async def _hint_one(
        self,
        entry: PreferenceEntry,
        addr: StoreKey,
        value: bytes | None,
        meta: KvMetadata,
    ) -> bool:
        """Send kv.hint to the handoff node. Returns True on success."""
        assert entry.handoff is not None
        handoff_id = entry.handoff

        if handoff_id == self._node_id:
            # Local hint store
            try:
                pid = self._partitioner.pid_for_addr(addr)
                store = self._storage.open_partition(pid)
                hint = store.hint(entry.node_id)
                if value is None:
                    await hint.delete(addr, meta)
                else:
                    await hint.put(addr, value, meta)
                return True
            except Exception:
                logger.exception("local hint store failed")
                return False

        payload = self._serializer.encode(
            {
                "key": addr.key,
                "keyspace": addr.keyspace,
                "value": value,
                "hlc": meta.hlc.to_dict(),
                "quorum_write": meta.quorum_write,
                "for_node": entry.node_id,
            }
        )
        from tourillon.core.structure.envelope import Envelope

        env = Envelope.create(
            payload, kind="kv.hint", schema_id=self._serializer.schema_id
        )
        try:
            address = await self._peer_address(handoff_id)
            client = await self._pool.acquire(handoff_id, address)
            async with asyncio.timeout(self._fanout_timeout):
                resp = await client.request(env, timeout=self._fanout_timeout)
            if resp.kind == "kv.hint.ok":
                await self._probe_manager.record_heartbeat(handoff_id)
                return True
            return False
        except (ResponseTimeoutError, ConnectionClosedError, TimeoutError):
            await self._probe_manager.record_miss(handoff_id)
            return False
        except Exception:
            logger.exception("hint to %s failed", handoff_id)
            return False

    # ------------------------------------------------------------------
    # Read path
    # ------------------------------------------------------------------

    async def get(
        self,
        key: bytes,
        keyspace: bytes,
        quorum_read: int,
    ) -> dict[str, Any]:
        """Execute a kv.get and return the kv.get.ok payload dict.

        Raise KvError on quorum unavailable.
        """
        addr = StoreKey(keyspace=keyspace, key=key)
        topology = await self._topology_manager.snapshot()
        placement = self._partitioner.placement_for_addr(addr, topology.ring)
        pref_list = await self._strategy.preference_list(
            placement, topology, self._probe_manager
        )

        fetch_targets = _build_fetch_targets(pref_list)
        responses = await self._fanout_fetch(addr, fetch_targets)

        if len(responses) < quorum_read:
            raise KvError(
                f"quorum_read_unavailable ({len(responses)}/{quorum_read} replicas responded)"
            )

        candidates = _group_versions([r for r in responses if r is not None])
        self._maybe_trigger_repair(addr, pref_list, responses, candidates)
        return {"versions": [c.to_dict() for c in candidates]}

    async def _fanout_fetch(
        self, addr: StoreKey, fetch_targets: list[str]
    ) -> list[Record | None]:
        """Fan out kv.fetch to all targets and return the result list."""
        tasks: list[asyncio.Task[Record | None]] = []

        async def _fetch(target_id: str) -> Record | None:
            return await self._fetch_one(target_id, addr)

        async with asyncio.TaskGroup() as tg:
            for target_id in fetch_targets:
                tasks.append(
                    tg.create_task(_fetch(target_id), name=f"fetch.{target_id}")
                )
        return [t.result() for t in tasks]

    def _maybe_trigger_repair(
        self,
        addr: StoreKey,
        pref_list: list[PreferenceEntry],
        responses: list[Record | None],
        candidates: list[VersionCandidate],
    ) -> None:
        """Schedule background read repair when a confirmed version exists."""
        confirmed = next((c for c in candidates if c.confirmed), None)
        if confirmed is None:
            return
        repair_targets = _repair_candidates(pref_list, responses, confirmed)
        if not repair_targets:
            return
        asyncio.get_running_loop().create_task(
            self._read_repair(addr, confirmed.record, repair_targets),
            name="kv.read_repair",
        )

    async def _fetch_one(self, target_id: str, addr: StoreKey) -> Record | None:
        """Fetch the local version from one replica. Returns None if not found."""
        if target_id == self._node_id:
            try:
                pid = self._partitioner.pid_for_addr(addr)
                store = self._storage.open_partition(pid)
                return await store.get(addr)
            except Exception:
                logger.exception("local fetch failed")
                return None
        return await self._fetch_remote(target_id, addr)

    async def _fetch_remote(self, target_id: str, addr: StoreKey) -> Record | None:
        """Send kv.fetch to a remote replica and parse the response."""
        payload = self._serializer.encode({"key": addr.key, "keyspace": addr.keyspace})
        from tourillon.core.structure.envelope import Envelope

        env = Envelope.create(
            payload, kind="kv.fetch", schema_id=self._serializer.schema_id
        )
        try:
            address = await self._peer_address(target_id)
            client = await self._pool.acquire(target_id, address)
            async with asyncio.timeout(self._fanout_timeout):
                resp = await client.request(env, timeout=self._fanout_timeout)
            if resp.kind != "kv.fetch.ok":
                return None
            await self._probe_manager.record_heartbeat(target_id)
            return self._parse_fetch_response(resp.payload, addr)
        except (ResponseTimeoutError, ConnectionClosedError, TimeoutError):
            await self._probe_manager.record_miss(target_id)
            return None
        except Exception:
            logger.exception("fetch from %s failed", target_id)
            return None

    def _parse_fetch_response(self, payload: bytes, addr: StoreKey) -> Record | None:
        """Decode a kv.fetch.ok payload and return the Record or None."""
        data = self._serializer.decode(payload)
        if not data.get("found"):
            return None
        hlc_raw = data.get("hlc")
        if hlc_raw is None:
            return None
        hlc = HLCTimestamp.from_dict(hlc_raw)
        qw = int(data.get("quorum_write") or 1)
        value_raw = data.get("value")
        if value_raw is None:
            return Tombstone(address=addr, metadata=hlc, quorum_write=qw)
        val_bytes = bytes(value_raw) if not isinstance(value_raw, bytes) else value_raw
        return Version(address=addr, metadata=hlc, value=val_bytes, quorum_write=qw)

    async def _read_repair(
        self,
        addr: StoreKey,
        v_star: Record,
        repair_targets: list[tuple[str, Record | None]],
    ) -> None:
        """Background best-effort read repair toward stale/phantom replicas."""
        value_bytes: bytes | None = (
            v_star.value if isinstance(v_star, Version) else None
        )
        meta = KvMetadata(hlc=v_star.metadata, quorum_write=v_star.quorum_write)

        async def _repair_one(target_id: str, local_rec: Record | None) -> None:
            if (
                local_rec is not None
                and local_rec.metadata > v_star.metadata
                and target_id == self._node_id
            ):
                # Phantom: local HLC is newer than V*
                pid = self._partitioner.pid_for_addr(addr)
                await self._storage.open_partition(pid).mark_phantom(
                    addr, local_rec.metadata
                )
                return
            await self._repair_send(target_id, addr, value_bytes, meta)

        try:
            async with asyncio.TaskGroup() as tg:
                for target_id, local_rec in repair_targets:
                    tg.create_task(_repair_one(target_id, local_rec))
        except Exception:
            logger.debug("read repair task group failed (best-effort)")

    async def _repair_send(
        self,
        target_id: str,
        addr: StoreKey,
        value_bytes: bytes | None,
        meta: KvMetadata,
    ) -> None:
        """Send V* to target_id (local or remote)."""
        if target_id == self._node_id:
            pid = self._partitioner.pid_for_addr(addr)
            store = self._storage.open_partition(pid)
            if value_bytes is None:
                await store.delete(addr, meta)
            else:
                await store.put(addr, value_bytes, meta)
            return

        from tourillon.core.structure.envelope import Envelope

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
            address = await self._peer_address(target_id)
            client = await self._pool.acquire(target_id, address)
            await client.request(env, timeout=self._fanout_timeout)
        except Exception:
            logger.debug("read repair to %s failed (best-effort)", target_id)


class KvError(Exception):
    """Raised by KvCoordinator when quorum is not reached."""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _hash_key(addr: StoreKey) -> int:
    """Return the ring hash for a StoreKey.

    Uses MD5 (via ``HashSpace.hash``) over the length-prefixed routing bytes
    produced by ``addr.to_routing_bytes()``.  This is kept as a module-level
    helper so that callers outside the coordinator (tests, replay manager) can
    derive a consistent token without holding a ``Partitioner`` reference.
    The hash function and bit-width must stay in sync with ``HashSpace``
    (currently MD5, 128-bit).
    """
    import hashlib  # noqa: PLC0415

    digest = hashlib.md5(
        addr.to_routing_bytes(), usedforsecurity=False
    ).digest()  # noqa: S324
    return int.from_bytes(digest, "big")


def _build_fetch_targets(pref_list: list[PreferenceEntry]) -> list[str]:
    """Return the list of node_ids to send kv.fetch to."""
    targets: list[str] = []
    for entry in pref_list:
        if entry.handoff is not None:
            targets.append(entry.handoff)
        elif entry.readable:
            targets.append(entry.node_id)
    return targets


def _group_versions(records: list[Record]) -> list[VersionCandidate]:
    """Group records by (hlc, type, value) and return VersionCandidate list.

    The list is sorted by HLC descending (newest first). The first confirmed
    version wins; if none is confirmed, all candidates are returned.
    """
    # Key: (wall_ms, counter, node_id, is_tombstone, value_bytes)
    groups: dict[tuple[Any, ...], list[Record]] = {}
    for rec in records:
        k = _record_key(rec)
        if k not in groups:
            groups[k] = []
        groups[k].append(rec)

    candidates = [
        VersionCandidate(record=group[0], count=len(group)) for group in groups.values()
    ]
    candidates.sort(key=lambda c: c.hlc, reverse=True)

    # If there is at least one confirmed candidate, return only the best one
    confirmed = [c for c in candidates if c.confirmed]
    if confirmed:
        return [confirmed[0]]
    return candidates


def _record_key(rec: Record) -> tuple[Any, ...]:
    """Return a hashable key for grouping identical versions."""
    hlc = rec.metadata
    is_tomb = isinstance(rec, Tombstone)
    val = rec.value if isinstance(rec, Version) else b""
    qw = rec.quorum_write
    return (hlc.wall_ms, hlc.counter, hlc.node_id, is_tomb, val, qw)


def _repair_candidates(
    pref_list: list[PreferenceEntry],
    responses: list[Record | None],
    confirmed: VersionCandidate,
) -> list[tuple[str, Record | None]]:
    """Return (node_id, local_record) pairs for replicas that need repair."""
    targets = _build_fetch_targets(pref_list)
    result: list[tuple[str, Record | None]] = []
    for target_id, rec in zip(targets, responses, strict=False):
        if rec is None:
            # Missing — send V*
            if confirmed.record is not None:
                result.append((target_id, None))
        elif _record_key(rec) != _record_key(confirmed.record):
            result.append((target_id, rec))
    return result
