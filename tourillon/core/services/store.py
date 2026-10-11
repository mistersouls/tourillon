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
from collections import Counter
from collections.abc import Callable, Sequence
from typing import Any

from tourillon.core.exceptions import (
    NoReplicaAvailableError,
    QuorumNotMetError,
    StoreOperationError,
)
from tourillon.core.log import logger
from tourillon.core.models.clock import HLCClock
from tourillon.core.models.storageschema import (
    Key,
    PartitionAddress,
    Pid,
    Record,
    Tag,
    TagKind,
    Value,
)
from tourillon.core.partition.partitioner import Partitioner
from tourillon.core.partition.placement import PlacementStrategy, Preference
from tourillon.core.peer.pool import PeerPool
from tourillon.core.ports.storage import Storage
from tourillon.core.storage.group import PartitionGroupStore
from tourlib.constant import ENVELOPE_RESPONSE_TIMEOUT
from tourlib.envelope import build_envelope
from tourlib.models.kv import DeleteRequest, GetRequest, PutRequest
from tourlib.models.version import Address, Meta, Version
from tourlib.port import Serializer

REPLICATION_RESPONSE_DELAY = 0.05


class StoreService:
    def __init__(
        self,
        node_id: str,
        storage: Storage,
        partitioner: Partitioner,
        placement_strategy: PlacementStrategy,
        clock: HLCClock,
        serializer: Serializer,
        peer_pool: PeerPool,
        response_delay: float = REPLICATION_RESPONSE_DELAY,
    ) -> None:
        self._node_id = node_id
        self._storage = storage
        self._partitioner = partitioner
        self._placement_strategy = placement_strategy
        self._clock = clock
        self._serializer = serializer
        self._peer_pool = peer_pool
        self._response_delay = max(0.0, response_delay)
        self._response_timeout = ENVELOPE_RESPONSE_TIMEOUT

    async def get(self, request: GetRequest) -> Version | None:
        """Get the versioned value for the given address."""
        addr = Address(key=request.key, namespace=request.namespace)
        preflist = await self._placement_strategy.preference_list(addr)
        p_address = PartitionAddress(
            pid=Pid(preflist.placement.partition.pid),
            address=addr,
        )
        targets = self._replication_targets(preflist.preferences)
        records, errors = await self._collect_reads(targets, p_address, request.quorum_read)
        if not records and errors:
            raise self._replication_error("read", errors)
        version, version_count = self._select_version(self._visible_versions(records))
        if request.quorum_read is not None:
            # Replicas without any version have no opinion: they only count
            # toward the quorum when no replica holds a version at all.
            votes = version_count if version is not None else len(records)
            self._ensure_quorum(votes, request.quorum_read, "read", errors)
        if self._is_tombstone(version, records):
            return None
        return version

    async def put(self, request: PutRequest) -> Version | None:
        addr = Address(key=request.key, namespace=request.namespace)
        preflist = await self._placement_strategy.preference_list(addr)
        record = self._write_record(addr, request.value, preflist.placement.partition.pid, False)
        records, errors = await self._replicate_write(record, preflist.preferences, request.quorum_write)
        if not records and errors:
            raise self._replication_error("write", errors)
        version, version_count = self._select_version(self._versions_from_records(records))
        if request.quorum_write is not None:
            self._ensure_quorum(version_count, request.quorum_write, "write", errors)
        return version

    async def delete(self, request: DeleteRequest) -> Version | None:
        addr = Address(key=request.key, namespace=request.namespace)
        preflist = await self._placement_strategy.preference_list(addr)
        record = self._write_record(addr, b"", preflist.placement.partition.pid, True)
        records, errors = await self._replicate_write(record, preflist.preferences, request.quorum_write)
        if not records and errors:
            raise self._replication_error("write", errors)
        version, version_count = self._select_version(self._versions_from_records(records))
        if request.quorum_write is not None:
            self._ensure_quorum(version_count, request.quorum_write, "write", errors)
        return version

    async def read(self, addr: PartitionAddress) -> Record | None:
        return await self._local_read(addr)

    async def write(self, record: Record) -> Record:
        # todo(souls): probably should check again if node is in the preference list
        return await self._local_write(record)

    async def _replicate_write(
        self,
        record: Record,
        preferences: Sequence[Preference],
        quorum: int | None,
    ) -> tuple[list[Record], list[Exception]]:
        applied: list[Record] = []
        errors: list[Exception] = []
        targets = self._write_targets(preferences)
        if not targets:
            raise NoReplicaAvailableError("write")
        tasks = {
            asyncio.create_task(
                self._write_to_target(target_node_id, record, hinted_node_id),
                name=target_node_id,
            )
            for target_node_id, hinted_node_id in targets
        }

        def collect(done: set[asyncio.Task[Record]]) -> None:
            for task in done:
                try:
                    applied.append(task.result())
                except Exception as exc:
                    logger.warning("replication.write failed error=%s", exc)
                    errors.append(exc)

        def quorum_met() -> bool:
            if quorum is None:
                return False
            _, version_count = self._select_version(self._versions_from_records(applied))
            return version_count >= quorum

        unanswered = await self._drain_parallel_tasks(tasks, collect, quorum_met if quorum is not None else None)
        errors.extend(self._unanswered(unanswered))
        return applied, errors

    async def _collect_reads(
        self,
        targets: Sequence[str],
        addr: PartitionAddress,
        quorum: int | None,
    ) -> tuple[list[Record | None], list[Exception]]:
        records: list[Record | None] = []
        errors: list[Exception] = []

        tasks = {
            asyncio.create_task(self._read_from_target(target, addr), name=target)
            for target in targets
        }

        def collect(done: set[asyncio.Task[Record | None]]) -> None:
            for task in done:
                try:
                    records.append(task.result())
                except Exception as exc:
                    logger.warning("replication.read failed error=%s", exc)
                    errors.append(exc)

        def quorum_met() -> bool:
            if quorum is None:
                return False
            _, version_count = self._select_version(self._visible_versions(records))
            return version_count >= quorum

        unanswered = await self._drain_parallel_tasks(tasks, collect, quorum_met if quorum is not None else None)
        errors.extend(self._unanswered(unanswered))
        return records, errors

    async def _read_from_target(self, target_node_id: str, addr: PartitionAddress) -> Record | None:
        if target_node_id == self._node_id:
            return await self._local_read(addr)
        return await self._remote_read(target_node_id, addr)

    @staticmethod
    def _write_target(entry: Preference) -> tuple[str, str | None] | None:
        if entry.handoff is not None:
            return entry.handoff, entry.node_id
        if entry.readable:
            return entry.node_id, None
        return None

    async def _write_to_target(
        self,
        target_node_id: str,
        record: Record,
        hinted_node_id: str | None,
    ) -> Record:
        if hinted_node_id is not None:
            record = self._hint_record(record, hinted_node_id)
        if target_node_id == self._node_id:
            return await self._local_write(record)
        return await self._remote_write(target_node_id, record)

    async def _remote_read(self, node_id: str, addr: PartitionAddress) -> Record | None:
        payload = await self._remote_request("replication.read", node_id, {"addr": addr.to_dict()})
        if payload is None:
            return None
        return Record.from_dict(payload)

    async def _remote_write(self, node_id: str, record: Record) -> Record:
        payload = await self._remote_request("replication.write", node_id, {"record": record.to_dict()})
        return Record.from_dict(payload)

    async def _remote_request(self, kind: str, node_id: str, payload: dict[str, Any]) -> Any:
        client = await self._peer_pool.acquire(node_id)
        try:
            envelope = build_envelope(
                kind=kind,
                payload=self._serializer.encode(payload),
                schema_id=self._serializer.schema_id,
            )
            response = await client.request(envelope)
        finally:
            await self._peer_pool.release(node_id)

        expected = f"{kind}.accept"
        if response.kind != expected:
            raise RuntimeError(f"invalid response kind for {kind}: {response.kind}")
        return self._serializer.decode(response.payload)


    @staticmethod
    def _replication_targets(preferences: Sequence[Preference]) -> list[str]:
        targets: list[str] = []
        for entry in preferences:
            if entry.handoff is not None:
                targets.append(entry.handoff)
            elif entry.readable:
                targets.append(entry.node_id)
        if not targets:
            raise NoReplicaAvailableError("read")
        return targets

    @staticmethod
    def _write_targets(preferences: Sequence[Preference]) -> list[tuple[str, str | None]]:
        targets: list[tuple[str, str | None]] = []
        for entry in preferences:
            target = StoreService._write_target(entry)
            if target is not None:
                targets.append(target)
        return targets

    @staticmethod
    def _ensure_quorum(
        success_count: int,
        required: int | None,
        op: str,
        errors: Sequence[Exception] | None = None,
    ) -> None:
        threshold = 1 if required is None else required
        if success_count < threshold:
            raise QuorumNotMetError(
                op,
                required=threshold,
                received=success_count,
                errors=errors or (),
            )

    @staticmethod
    def _replication_error(
        operation: str,
        errors: Sequence[Exception],
        message: str | None = None,
    ) -> StoreOperationError:
        return StoreOperationError(operation=operation, errors=errors, message=message)


    async def _local_read(self, addr: PartitionAddress) -> Record | None:
        group = self._partitioner.group_for(addr.pid.value)
        session_provider = await self._storage.session_provider_for(group)
        store = PartitionGroupStore(session_provider)
        return await store.get(addr)

    async def _local_write(self, record: Record) -> Record:
        group = self._partitioner.group_for(record.key.pid.value)
        session_provider = await self._storage.session_provider_for(group)
        store = PartitionGroupStore(session_provider)
        await store.put(record)
        return record

    @staticmethod
    def _hint_record(record: Record, hinted_node_id: str) -> Record:
        return Record(
            key=record.key,
            value=record.value,
            tag=Tag.hint(record.tag, hinted_node_id),
        )

    async def _drain_parallel_tasks(
        self,
        tasks: set[asyncio.Task[Any]],
        collect: Callable[[set[asyncio.Task[Any]]], None],
        should_stop: Callable[[], bool] | None,
    ) -> set[asyncio.Task[Any]]:
        """Collect results until should_stop() holds or the response deadline expires.

        Return the tasks still running at the deadline: they are cancelled and
        reported by the caller as unanswered replicas.
        """
        stop = should_stop or (lambda: False)
        pending = set(tasks)
        try:
            if pending:
                done, pending = await asyncio.wait(pending, timeout=self._response_delay)
                collect(done)

            loop = asyncio.get_running_loop()
            deadline = loop.time() + self._response_timeout
            while pending and not stop():
                done, pending = await asyncio.wait(
                    pending,
                    timeout=max(deadline - loop.time(), 0),
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if not done:
                    return set(pending)
                collect(done)
            return set()
        finally:
            await self._cancel_pending_tasks(pending)

    def _unanswered(self, tasks: set[asyncio.Task[Any]]) -> list[Exception]:
        """Report replicas that did not answer before the deadline as timeouts."""
        return [
            TimeoutError(f"no response from {name} within {self._response_timeout}s")
            for name in sorted(task.get_name() for task in tasks)
        ]

    @staticmethod
    async def _cancel_pending_tasks(tasks: set[asyncio.Task[Any]]) -> None:
        if not tasks:
            return
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    def _read_key(self, addr: Address, pid: int) -> Key:
        return Key(pid=Pid(pid), address=addr, ts=self._clock.tick())

    def _write_record(self, addr: Address, value: bytes, pid: int, tombstone: bool) -> Record:
        meta = Meta(quorum_write=1, ts=self._clock.tick())
        tag = Tag(kind=TagKind.TOMBSTONE if tombstone else TagKind.LIVE)
        return Record(
            key=Key(pid=Pid(pid), address=addr, ts=meta.ts),
            value=Value(data=value, quorum_write=meta.quorum_write),
            tag=tag,
        )

    @staticmethod
    def _version_from_record(record: Record) -> Version:
        return Version(
            address=record.key.address,
            data=record.value.data,
            meta=Meta(ts=record.key.ts, quorum_write=record.value.quorum_write),
        )

    def _versions_from_records(self, records: Sequence[Record]) -> list[Version]:
        return [self._version_from_record(record) for record in records]

    def _visible_versions(self, records: Sequence[Record | None]) -> list[Version]:
        versions: list[Version] = []
        for record in records:
            if record is None or record.tag.kind not in {TagKind.LIVE, TagKind.TOMBSTONE, TagKind.HINT}:
                continue
            versions.append(self._version_from_record(record))
        return versions

    @staticmethod
    def _is_tombstone(version: Version | None, records: Sequence[Record | None]) -> bool:
        """Return True when the elected version is a delete, hinted or not."""
        if version is None:
            return False
        return any(
            record is not None
            and record.key.ts == version.meta.ts
            and TagKind.TOMBSTONE in (record.tag.kind, record.tag.payload.sub)
            for record in records
        )

    @classmethod
    def _select_version(cls, versions: Sequence[Version]) -> tuple[Version | None, int]:
        if not versions:
            return None, 0

        counts = Counter(versions)
        version, count = max(counts.items(), key=cls._version_rank)
        return version, count

    @staticmethod
    def _version_rank(item: tuple[Version, int]) -> tuple[Any, ...]:
        version, count = item
        return (
            count,
            version.meta.ts,
            version.address.namespace,
            version.address.key,
            version.data,
            version.meta.quorum_write,
        )

