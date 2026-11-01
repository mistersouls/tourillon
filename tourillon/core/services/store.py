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
import random
import time
from collections import Counter
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

from tourillon.core.ae.obligations import ObligationQueue
from tourillon.core.exceptions import (
    InvalidQuorumError,
    InvalidRepairTokenError,
    NodeDrainingError,
    NoReplicaAvailableError,
    OutdatedRepairError,
    QuorumNotMetError,
    StoreOperationError,
    VersionNotFoundError,
)
from tourillon.core.log import describe, logger
from tourillon.core.models.clock import HLCClock
from tourillon.core.models.config import ReadRepairConfig, RepairConfig
from tourillon.core.models.repair import (
    RepairIssue,
    RepairResult,
    VersionLookup,
    decode_token,
    encode_token,
)
from tourillon.core.models.storageschema import (
    Key,
    Keyspace,
    KeyTag,
    ObligationKey,
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
from tourillon.core.ports.telemetry import Telemetry
from tourillon.core.storage.retention import RetentionPolicy
from tourillon.core.storage.session import GroupSessionManager
from tourillon.core.telemetry import build_noop_telemetry
from tourlib.constant import ENVELOPE_RESPONSE_TIMEOUT, MAX_CLOCK_SKEW_MS, MAX_QUORUM
from tourlib.envelope import build_envelope
from tourlib.models.kv import DeleteRequest, GetRequest, PutRequest, RepairRequest
from tourlib.models.version import Address, Meta, Version
from tourlib.port import Serializer

REPLICATION_RESPONSE_DELAY = 0.05

# Tag kinds of a stored version: there is no other way to hold one.
_OWNED_KINDS = frozenset({TagKind.LIVE, TagKind.TOMBSTONE})


@dataclass
class ReplicationStats:
    """Counters for replication anomalies that are tolerated rather than raised."""

    foreign_writes: int = 0
    obligations_recorded: int = 0
    read_repairs_repaired: int = 0
    read_repairs_failed: int = 0
    read_repairs_skipped: int = 0
    repairs_requested: int = 0
    repairs_rejected: int = 0


# Failures caused by the request or the node's own phase rather than by
# replication: expected, so they are not logged as warnings.
_REFUSALS = (
    NodeDrainingError,
    InvalidQuorumError,
    InvalidRepairTokenError,
    VersionNotFoundError,
)


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
        retention: RetentionPolicy,
        response_delay: float = REPLICATION_RESPONSE_DELAY,
        read_repair: ReadRepairConfig | None = None,
        repair: RepairConfig | None = None,
        obligations: ObligationQueue | None = None,
        telemetry: Telemetry | None = None,
    ) -> None:
        self._node_id = node_id
        self._telemetry = telemetry or build_noop_telemetry()
        self._storage = storage
        self._retention = retention
        self._partitioner = partitioner
        self._placement_strategy = placement_strategy
        self._clock = clock
        self._serializer = serializer
        self._peer_pool = peer_pool
        self._response_delay = max(0.0, response_delay)
        self._response_timeout = ENVELOPE_RESPONSE_TIMEOUT
        self._pending_writes: set[asyncio.Task[Any]] = set()
        self._stats = ReplicationStats()
        self._read_repair = read_repair or ReadRepairConfig()
        self._repair_slots = asyncio.Semaphore(self._read_repair.concurrency)
        self._repairs_inflight: set[tuple[bytes, str]] = set()
        self._repair_gate = asyncio.Semaphore((repair or RepairConfig()).concurrency)
        self._accepting_operations = True
        self._accepting_reads = True
        self._accepting_read_repairs = True
        self._mutations: set[asyncio.Future[None]] = set()
        self._obligations = obligations

    def pause(self, *, readable: bool = True) -> None:
        """Refuse writes while draining, retaining reads until IDLE."""
        self._accepting_operations = False
        self._accepting_reads = readable

    def resume(self) -> None:
        self._accepting_operations = True
        self._accepting_reads = True
        self._accepting_read_repairs = True

    async def stop_read_repairs(self) -> None:
        """Stop scheduling repairs and let existing background writes settle."""
        self._accepting_read_repairs = False
        await self.close()

    async def drain(self) -> None:
        """Wait for admitted mutations and a snapshot of background writes."""
        self.pause()
        if self._mutations:
            await asyncio.wait(tuple(self._mutations))
        if self._pending_writes:
            await asyncio.wait(tuple(self._pending_writes))

    @contextmanager
    def _operation(self, operation: str) -> Iterator[None]:
        """Admit, track and measure one store operation.

        Every operation ends in exactly one ``kv_requests_total`` sample whose
        ``outcome`` is ``ok``, the stable error code of a store failure, or
        ``error`` / ``cancelled`` for anything else.
        """
        started = time.perf_counter()
        outcome = "cancelled"
        try:
            self._admit(operation)
            with self._tracked(operation):
                yield
            outcome = "ok"
        except StoreOperationError as exc:
            outcome = exc.code
            self._report_failure(operation, exc)
            raise
        except Exception:
            # Unexpected failures propagate to the transport, which logs them
            # with their traceback; counting them is enough here.
            outcome = "error"
            raise
        finally:
            attributes = {"op": operation, "outcome": outcome}
            self._telemetry.kv_requests_total.add(1, attributes=attributes)
            self._telemetry.kv_request_duration_seconds.record(
                time.perf_counter() - started, attributes=attributes
            )

    def _admit(self, operation: str) -> None:
        accepting = (
            self._accepting_reads if operation == "read" else self._accepting_operations
        )
        if not accepting:
            raise NodeDrainingError(operation)

    @contextmanager
    def _tracked(self, operation: str) -> Iterator[None]:
        """Register a mutation so that ``drain`` can wait for it to settle."""
        if operation == "read":
            yield
            return
        settled = asyncio.get_running_loop().create_future()
        self._mutations.add(settled)
        try:
            yield
        finally:
            settled.set_result(None)
            self._mutations.discard(settled)

    def _report_failure(self, operation: str, exc: StoreOperationError) -> None:
        # Refusals are the caller's business and expected while draining; a
        # replication failure is the node's, and the first thing to look at
        # when clients see errors.
        if isinstance(exc, _REFUSALS):
            logger.debug("store.%s refused node=%s: %s", operation, self._node_id, exc)
            return
        logger.warning("store.%s failed node=%s: %s", operation, self._node_id, exc)

    @property
    def stats(self) -> "ReplicationStats":
        return self._stats

    async def close(self) -> None:
        """Wait for writes detached at the response deadline to settle.

        Shutting down the storage underneath them would turn a slow but
        succeeding replication into a lost write. Read repairs are waited on
        for the same reason: a half-applied repair is harmless, but tearing
        down storage mid-write is not.
        """
        while self._pending_writes:
            await asyncio.wait(tuple(self._pending_writes))

    async def get(self, request: GetRequest) -> Version | None:
        """Get the versioned value for the given address."""
        with self._operation("read"):
            return await self._get(request)

    async def _get(self, request: GetRequest) -> Version | None:
        self._validate_quorum("read", request.quorum_read)
        addr = Address(key=request.key, namespace=request.namespace)
        preflist = await self._placement_strategy.preference_list(addr)
        p_address = PartitionAddress(
            pid=Pid(preflist.placement.partition.pid),
            address=addr,
        )
        targets = self._replication_targets(preflist.preferences)
        answers, errors = await self._collect_reads(
            targets, p_address, request.quorum_read
        )
        records = list(answers.values())
        if not records and errors:
            raise self._replication_error("read", errors)
        version, _ = self._select_version(self._visible_versions(records))
        # A replica answering "I hold nothing" is still an answer: absence is
        # information. A handoff and its owner count as one logical replica.
        self._ensure_quorum(
            self._read_response_count(targets, answers),
            request.quorum_read,
            "read",
            errors,
        )
        self._schedule_read_repair(answers, preflist.preferences, version)
        if self._is_tombstone(version, records):
            return None
        return version

    async def put(self, request: PutRequest) -> Version | None:
        with self._operation("write"):
            return await self._put(request)

    async def _put(self, request: PutRequest) -> Version | None:
        self._validate_quorum("write", request.quorum_write)
        addr = Address(key=request.key, namespace=request.namespace)
        preflist = await self._placement_strategy.preference_list(addr)
        record = self._write_record(
            addr,
            request.value,
            preflist.placement.partition.pid,
            False,
            request.quorum_write,
        )
        records, errors = await self._replicate_write(
            record, preflist.preferences, request.quorum_write, kind="kv.put"
        )
        if not records and errors:
            raise self._replication_error("write", errors)
        version, version_count = self._select_version(
            self._versions_from_records(records)
        )
        self._ensure_quorum(
            version_count,
            request.quorum_write,
            "write",
            errors,
            encode_token(record.key),
        )
        return version

    async def delete(self, request: DeleteRequest) -> Version | None:
        with self._operation("delete"):
            return await self._delete(request)

    async def _delete(self, request: DeleteRequest) -> Version | None:
        self._validate_quorum("write", request.quorum_write)
        addr = Address(key=request.key, namespace=request.namespace)
        preflist = await self._placement_strategy.preference_list(addr)
        record = self._write_record(
            addr, b"", preflist.placement.partition.pid, True, request.quorum_write
        )
        records, errors = await self._replicate_write(
            record, preflist.preferences, request.quorum_write, kind="kv.delete"
        )
        if not records and errors:
            raise self._replication_error("write", errors)
        version, version_count = self._select_version(
            self._versions_from_records(records)
        )
        self._ensure_quorum(
            version_count,
            request.quorum_write,
            "write",
            errors,
            encode_token(record.key),
        )
        return version

    async def read(self, addr: PartitionAddress) -> Record | None:
        return await self._local_read(addr, kind="replication.read")

    async def read_at(self, key: Key) -> Record | None:
        """Answer whether this node holds one exact version."""
        return await self._local_read_at(key, kind="replication.lookup")

    async def lookup(
        self, key: Key, *, kind: str = "replication.lookup"
    ) -> VersionLookup:
        group = self._partitioner.group_for(key.pid.value)
        with self._telemetry.start_span(
            "storage.operation", attributes={"group": group, "kind": kind}
        ):
            return await self._lookup_local(key, group)

    async def _lookup_local(self, key: Key, group: int) -> VersionLookup:
        provider = await self._storage.session_provider_for(group)
        manager = GroupSessionManager(provider)
        async with manager.session() as session:
            return await self._retention.lookup(session, key)

    async def write(self, record: Record) -> Record:
        with self._operation("replication.write"):
            return await self._write(record)

    async def hint(self, record: Record, intent: str) -> Record:
        """Hold a version on behalf of ``intent`` and owe it a copy.

        The version is stored as is (LIVE or TOMBSTONE) and a replication
        obligation towards ``intent`` is persisted in the same transaction;
        anti-entropy delivers it.
        """
        if record.tag.kind not in _OWNED_KINDS:
            raise ValueError("hint requires a LIVE or TOMBSTONE record")
        with self._operation("replication.hint"):
            await self._oblige(record, (intent,), kind="replication.hint")
        return record

    async def _oblige(
        self, record: Record, targets: Iterable[str], *, kind: str
    ) -> None:
        """Store ``record`` and the obligations to copy it to ``targets``.

        Both land in one write transaction, so an obligation never refers to
        a version this node does not hold. The hotqueue is told only once
        they are durable; if it is full, the obligation waits in META.
        """
        obligations = [
            ObligationKey(target=target, key=record.key)
            for target in dict.fromkeys(targets)
            if target != self._node_id
        ]
        group = self._partitioner.group_for(record.key.pid.value)
        with (
            self._telemetry.start_span("storage.write", attributes={"group": group}),
            self._telemetry.start_span(
                "storage.operation",
                attributes={"group": group, "kind": kind},
            ),
            self._telemetry.start_span(
                "storage.transaction", attributes={"group": group}
            ),
        ):
            provider = await self._storage.session_provider_for(group)
            manager = GroupSessionManager(provider)
            async with manager.session(write=True) as session:
                with self._telemetry.start_span("storage.record.put"):
                    await session.put(record)
                if obligations:
                    with self._telemetry.start_span("storage.obligations.put"):
                        await session.put_obligations(obligations)
        self._stats.obligations_recorded += len(obligations)
        self._telemetry.obligations_total.add(
            len(obligations), attributes={"event": "recorded"}
        )
        if self._obligations is not None:
            self._obligations.submit(obligations)

    async def _write(self, record: Record) -> Record:
        """Apply a replicated write received from a peer.

        The partition id is recomputed from the address and compared with the
        one carried by the record. They must agree: a record stored under a
        foreign partition id lands in a group that reads for this address never
        consult, so the write would be acknowledged and never readable again.

        A mismatch is traced and still applied. Refusing it would turn a
        transient topology disagreement into a lost write, and the sender is
        the authority on where it meant to place the data. The full preference
        list is deliberately not re-checked here: that walk snapshots every
        known member on each call, which would multiply the cost of one client
        write by the replication factor.
        """
        expected = self._partitioner.pid_for_hash(
            self._partitioner.space.hash(record.key.address.to_bytes())
        )
        if expected != record.key.pid.value:
            self._stats.foreign_writes += 1
            logger.warning(
                "replication.write.foreign: pid=%s expected=%s namespace=%s",
                record.key.pid.value,
                expected,
                record.key.address.namespace,
            )
        return await self._local_write(record, kind="replication.write")

    async def repair(self, request: RepairRequest) -> RepairResult:
        with self._operation("repair"):
            return await self._repair(request)

    async def _repair(self, request: RepairRequest) -> RepairResult:
        """Spread an already-written version to the replicas that lack it.

        This is the resumable half of a write that did not reach its quorum.
        The version is persisted somewhere — that is why the client holds a
        handle at all — so the work is to copy it, never to create it. Writing
        the same timestamp again is idempotent by construction: replaying it
        a hundred times produces exactly one version, where a replayed `put`
        would produce a hundred.
        """
        self._validate_quorum("repair", request.quorum_write)
        key = self._repair_key(request.token)
        self._stats.repairs_requested += 1
        preflist = await self._placement_strategy.preference_list(key.address)
        fanout = self._repair_fanout(preflist.preferences)
        if not fanout:
            raise NoReplicaAvailableError("repair")

        async with self._repair_gate:
            return await self._run_repair(request, key, fanout)

    async def _run_repair(
        self,
        request: RepairRequest,
        key: Key,
        fanout: Sequence[tuple[str, str, str | None]],
    ) -> RepairResult:
        token = request.token
        lookups, read_errors = await self._collect_repair_reads(key, fanout)
        answers = {owner: result.record for owner, result in lookups.items()}
        source = self._repair_source(answers.values())
        if source is None:
            self._stats.repairs_rejected += 1
            self._check_outdated(request, key, fanout, lookups, 0)
            raise VersionNotFoundError(token)

        holders = self._repair_holders(answers, fanout)
        missing = [
            (target, hinted) for owner, target, hinted in fanout if owner not in holders
        ]
        if not missing:
            self._ensure_repair_quorum(len(holders), request, read_errors, token)
            return RepairResult(RepairIssue.ALREADY_DURABLE, len(holders), token)

        applied, errors = await self._fanout_write(
            source, missing, None, kind="kv.repair"
        )
        durable = len(holders) + len(applied)
        self._ensure_repair_quorum(durable, request, [*read_errors, *errors], token)
        issue = RepairIssue.REPAIRED if applied else RepairIssue.ALREADY_DURABLE
        return RepairResult(issue, durable, token)

    @staticmethod
    def _check_outdated(
        request: RepairRequest,
        key: Key,
        fanout: Sequence[tuple[str, str, str | None]],
        lookups: Mapping[str, VersionLookup],
        durable: int,
    ) -> None:
        if durable >= request.quorum_write:
            return
        direct = {owner for owner, target, _ in fanout if owner == target}
        outside = {
            owner
            for owner, result in lookups.items()
            if owner in direct and result.outside_retention
        }
        if (
            not outside
            or len({owner for owner, _, _ in fanout}) - len(outside)
            >= request.quorum_write
        ):
            return

        copies: dict[bytes, tuple[KeyTag, set[str]]] = {}
        for owner, result in lookups.items():
            for identity in result.retained:
                if owner in direct and identity.key.ts > key.ts:
                    raw = identity.key.to_bytes(Keyspace.TAGS)
                    copies.setdefault(raw, (identity, set()))[1].add(owner)

        newer = [
            {
                "version": item.to_dict(),
                "replicas": len(owners),
                "owners": sorted(owners),
            }
            for item, owners in sorted(
                copies.values(), key=lambda pair: pair[0].key.ts, reverse=True
            )
        ]
        raise OutdatedRepairError(request.token, durable, request.quorum_write, newer)

    @staticmethod
    def _ensure_repair_quorum(
        durable: int,
        request: RepairRequest,
        errors: Sequence[Exception],
        token: str,
    ) -> None:
        """Let the client decide again whether the durability reached is enough."""
        StoreService._ensure_quorum(
            durable, request.quorum_write, "repair", errors, token
        )

    def _repair_key(self, token: str) -> Key:
        """Decode and vet a handle before it costs a single RPC.

        All of these checks are local and O(1). The expensive question — does
        this version exist — cannot be answered without the fan-out that *is*
        the work, so it is not asked twice.
        """
        try:
            key = decode_token(token)
        except ValueError as exc:
            self._stats.repairs_rejected += 1
            raise InvalidRepairTokenError(str(exc)) from exc

        expected = self._partitioner.pid_for_hash(
            self._partitioner.space.hash(key.address.to_bytes())
        )
        if expected != key.pid.value:
            # A record filed under a foreign partition id lands in a group
            # that reads for this address never consult: acknowledged, then
            # invisible. The partition id is recomputed, never trusted.
            self._stats.repairs_rejected += 1
            raise InvalidRepairTokenError(
                f"partition id {key.pid.value} does not match address (expected {expected})"
            )

        horizon = (
            max(int(time.time() * 1000), self._clock.snapshot().wall_ms)
            + MAX_CLOCK_SKEW_MS
        )
        if key.ts.wall_ms > horizon:
            # No clock here could have emitted it, so no write here produced it.
            self._stats.repairs_rejected += 1
            raise InvalidRepairTokenError(
                f"timestamp {key.ts.wall_ms} is beyond the acceptable clock skew"
            )
        return key

    @staticmethod
    def _repair_fanout(
        preferences: Sequence[Preference],
    ) -> list[tuple[str, str, str | None]]:
        """Map each replica to the node that answers for it.

        Returns `(owner, target, hinted)`: `owner` is who must end up holding
        the version, `target` is who is reachable on its behalf. They differ
        while a handoff stands in, and the distinction is what keeps a copy
        held on the owner's behalf from counting as the owner's durability.
        """
        fanout: list[tuple[str, str, str | None]] = []
        for entry in preferences:
            if entry.handoff is not None:
                fanout.append((entry.node_id, entry.handoff, entry.node_id))
            elif entry.readable:
                fanout.append((entry.node_id, entry.node_id, None))
        return fanout

    async def _collect_repair_reads(
        self,
        key: Key,
        fanout: Sequence[tuple[str, str, str | None]],
    ) -> tuple[dict[str, VersionLookup], list[Exception]]:
        """Ask every replica whether it holds this exact version."""
        answers: dict[str, VersionLookup] = {}
        errors: list[Exception] = []
        tasks = {
            asyncio.create_task(self._lookup_target(target, key), name=owner)
            for owner, target, _ in fanout
        }

        def collect(done: set[asyncio.Task[VersionLookup]]) -> None:
            for task in done:
                try:
                    answers[task.get_name()] = task.result()
                except Exception as exc:
                    self._replica_failed("lookup", task.get_name(), exc)
                    errors.append(exc)

        unanswered = await self._drain_parallel_tasks(tasks, collect, None)
        errors.extend(self._unanswered(unanswered))
        return answers, errors

    async def _lookup_target(self, target: str, key: Key) -> VersionLookup:
        if target == self._node_id:
            return await self.lookup(key, kind="kv.repair")
        result = VersionLookup.from_dict(
            await self._remote_request(
                "replication.lookup", target, {"key": key.to_dict()}
            )
        )
        if result.record is not None and result.record.key != key:
            raise ValueError("lookup returned a different exact version")
        if any(
            r.key.address != key.address or r.key.pid != key.pid
            for r in result.retained
        ):
            raise ValueError("lookup returned another address")

        return result

    @staticmethod
    def _repair_holders(
        answers: Mapping[str, Record | None],
        fanout: Sequence[tuple[str, str, str | None]],
    ) -> set[str]:
        """Replicas that hold the version themselves.

        A copy held by a stand-in is a debt, not durability: the owner still
        does not have the version, so it stays a repair target.
        """
        direct = {owner for owner, target, _ in fanout if owner == target}
        return {
            owner
            for owner, record in answers.items()
            if owner in direct
            and record is not None
            and record.tag.kind in _OWNED_KINDS
        }

    @staticmethod
    def _repair_source(records: Iterable[Record | None]) -> Record | None:
        """Pick the record to copy: any stored copy is a faithful one.

        A stand-in's copy is usable too, otherwise a version that only
        survives on a handoff could never be repaired.
        """
        for record in records:
            if record is not None and record.tag.kind in _OWNED_KINDS:
                return record
        return None

    async def _read_at_from_target(
        self, target_node_id: str, key: Key
    ) -> Record | None:
        if target_node_id == self._node_id:
            return await self._local_read_at(key, kind="kv.repair")
        return await self._remote_read_at(target_node_id, key)

    async def _replicate_write(
        self,
        record: Record,
        preferences: Sequence[Preference],
        quorum: int | None,
        *,
        kind: str,
    ) -> tuple[list[Record], list[Exception]]:
        targets = self._write_targets(preferences)
        if not targets:
            raise NoReplicaAvailableError("write")
        return await self._fanout_write(record, targets, quorum, kind=kind)

    async def _fanout_write(
        self,
        record: Record,
        targets: Sequence[tuple[str, str | None]],
        quorum: int | None,
        *,
        kind: str,
    ) -> tuple[list[Record], list[Exception]]:
        applied: list[Record] = []
        errors: list[Exception] = []
        tasks = {
            asyncio.create_task(
                self._write_to_target(
                    target_node_id, record, hinted_node_id, kind=kind
                ),
                name=target_node_id,
            )
            for target_node_id, hinted_node_id in targets
        }

        def collect(done: set[asyncio.Task[Record]]) -> None:
            for task in done:
                try:
                    applied.append(task.result())
                except Exception as exc:
                    self._replica_failed("write", task.get_name(), exc)
                    errors.append(exc)

        def quorum_met() -> bool:
            if quorum is None:
                return False
            _, version_count = self._select_version(
                self._versions_from_records(applied)
            )
            return version_count >= quorum

        unanswered = await self._drain_parallel_tasks(
            tasks,
            collect,
            quorum_met if quorum is not None else None,
            cancel_pending=False,
        )
        errors.extend(self._unanswered(unanswered))
        return applied, errors

    async def _collect_reads(
        self,
        targets: Mapping[str, frozenset[str]],
        addr: PartitionAddress,
        quorum: int | None,
    ) -> tuple[dict[str, Record | None], list[Exception]]:
        """Read every target, keeping which replica returned what.

        The association is what makes read repair possible: a flat list of
        records says a replica is behind but not which one to catch up.
        """
        records: dict[str, Record | None] = {}
        errors: list[Exception] = []

        pending: set[str] = set().union(*targets.values())
        tasks = {
            asyncio.create_task(self._read_from_target(target, addr), name=target)
            for target in sorted(pending)
        }

        def collect(done: set[asyncio.Task[Record | None]]) -> None:
            for task in done:
                pending.discard(task.get_name())
                try:
                    records[task.get_name()] = task.result()
                except Exception as exc:
                    self._replica_failed("read", task.get_name(), exc)
                    errors.append(exc)

        def quorum_met() -> bool:
            if quorum is None:
                return False
            return self._read_response_count(targets, records, pending) >= quorum

        unanswered = await self._drain_parallel_tasks(
            tasks, collect, quorum_met if quorum is not None else None
        )
        errors.extend(self._unanswered(unanswered))
        return records, errors

    @staticmethod
    def _read_response_count(
        targets: Mapping[str, frozenset[str]],
        answers: Mapping[str, Record | None],
        pending: set[str] | frozenset[str] = frozenset(),
    ) -> int:
        """Count owners once, settling both read sources before an early return."""
        return sum(
            bool(sources.intersection(answers)) and not sources.intersection(pending)
            for sources in targets.values()
        )

    async def _read_from_target(
        self, target_node_id: str, addr: PartitionAddress
    ) -> Record | None:
        if target_node_id == self._node_id:
            return await self._local_read(addr, kind="kv.get")
        return await self._remote_read(target_node_id, addr)

    def _schedule_read_repair(
        self,
        answers: Mapping[str, Record | None],
        preferences: Sequence[Preference],
        winner: Version | None,
    ) -> None:
        """Catch up the replicas that answered with less than the elected version.

        Repair runs off the response path: making a read wait for N writes
        would tie read latency to the slowest replica, which is the opposite
        of what repair is for. Failures are counted, never raised — the read
        has already succeeded.

        Tombstones are repaired like any other version. Skipping them would
        let a replica that missed a delete re-propose the old value on every
        subsequent read, turning repair into an endless resurrection loop.
        """
        if winner is None or not self._read_repair.enabled:
            return
        record = self._winning_record(answers.values(), winner)
        if record is None:
            return
        targets = self._repair_targets(answers, preferences, record)
        if not targets:
            return
        holder = self._repair_holder(answers, record)
        if (
            self._read_repair.sample_rate < 1.0
            and random.random() >= self._read_repair.sample_rate
        ):
            # The valve: when a node comes back from a long outage, repairing
            # on every read is exactly when the cluster can least afford it.
            self._read_repairs("skipped", len(targets))
            return
        for node_id in targets:
            self._start_repair(node_id, record, holder)

    def _repair_holder(
        self, answers: Mapping[str, Record | None], winner: Record
    ) -> str:
        """Pick the node that owes the laggards a copy: this one if it can.

        Read repair does not copy anything itself. It records an obligation
        on a node that holds the elected version, and anti-entropy delivers
        it like any other.
        """
        holders = sorted(
            node_id
            for node_id, record in answers.items()
            if record is not None and record.key == winner.key
        )
        return self._node_id if self._node_id in holders else holders[0]

    @staticmethod
    def _winning_record(
        records: Iterable[Record | None], winner: Version
    ) -> Record | None:
        """Return the stored record the elected version came from."""
        for record in records:
            if record is None:
                continue
            if record.key.ts == winner.meta.ts and record.tag.kind in _OWNED_KINDS:
                return record
        return None

    @staticmethod
    def _repair_targets(
        answers: Mapping[str, Record | None],
        preferences: Sequence[Preference],
        winner: Record,
    ) -> list[str]:
        """Select the replicas that own the address and are behind the winner.

        A replica standing in through handoff is skipped: it does not own the
        address, and the obligation it holds makes the real owner whole.
        Replicas that failed or timed out are absent from `answers` entirely,
        and are covered by the obligations of the write path and by
        anti-entropy rather than by repair.
        """
        owners = {
            entry.node_id
            for entry in preferences
            if entry.handoff is None and entry.readable
        }
        targets = [
            node_id
            for node_id, record in answers.items()
            if node_id in owners and (record is None or record.key.ts < winner.key.ts)
        ]
        return sorted(targets)

    def _start_repair(self, node_id: str, record: Record, holder: str) -> None:
        if not self._accepting_read_repairs:
            self._read_repairs("skipped", 1)
            return
        marker = (record.key.to_bytes(Keyspace.LOGS), node_id)
        if marker in self._repairs_inflight:
            # A hot key must not spawn one repair per concurrent read.
            return
        self._repairs_inflight.add(marker)
        task = asyncio.create_task(
            self._repair_target(marker, record, holder), name=node_id
        )
        self._pending_writes.add(task)
        task.add_done_callback(self._settle_detached)

    async def _repair_target(
        self, marker: tuple[bytes, str], record: Record, holder: str
    ) -> None:
        node_id = marker[1]
        try:
            async with self._repair_slots:
                await self._apply_hint(holder, record, node_id, kind="replication.hint")
            self._read_repairs("repaired", 1)
        except Exception as exc:
            self._read_repairs("failed", 1)
            logger.warning(
                "read_repair failed target=%s key=%s error=%s",
                node_id,
                record.key,
                describe(exc),
            )
        finally:
            self._repairs_inflight.discard(marker)

    @staticmethod
    def _write_target(entry: Preference) -> tuple[str, str | None] | None:
        if entry.handoff is not None:
            return entry.handoff, entry.node_id
        if entry.writable:
            return entry.node_id, None
        return None

    async def _write_to_target(
        self,
        target_node_id: str,
        record: Record,
        hinted_node_id: str | None,
        *,
        kind: str,
    ) -> Record:
        if hinted_node_id is not None:
            return await self._apply_hint(
                target_node_id, record, hinted_node_id, kind=kind
            )
        try:
            return await self._apply_write(target_node_id, record, kind=kind)
        except Exception:
            # The placement believed this replica healthy, so nothing else will
            # notice it missed the write. Record the obligation locally and let
            # anti-entropy deliver it instead of dropping it.
            await self._record_obligation(target_node_id, record, kind=kind)
            raise

    async def _apply_write(
        self, target_node_id: str, record: Record, *, kind: str
    ) -> Record:
        if target_node_id == self._node_id:
            return await self._local_write(record, kind=kind)
        return await self._remote_write(target_node_id, record)

    async def _apply_hint(
        self,
        target_node_id: str,
        record: Record,
        intent: str,
        *,
        kind: str,
    ) -> Record:
        if target_node_id == self._node_id:
            await self._oblige(record, (intent,), kind=kind)
            return record
        payload = await self._remote_request(
            "replication.hint",
            target_node_id,
            {"record": record.to_dict(), "intent": intent},
        )
        return Record.from_dict(payload)

    async def _record_obligation(
        self, target_node_id: str, record: Record, *, kind: str
    ) -> None:
        """Owe `target_node_id` this version, never masking the write error.

        There is nothing to record when the failing target is this node: our
        own storage is the thing that just failed. Any error here is swallowed
        on purpose — the caller re-raises the original replication error,
        which is what quorum accounting and the client response are built on.
        """
        if target_node_id == self._node_id:
            return
        try:
            await self._oblige(record, (target_node_id,), kind=kind)
        except Exception as exc:
            logger.warning(
                "obligation.record failed target=%s key=%s error=%s",
                target_node_id,
                record.key,
                describe(exc),
            )

    async def _remote_read(self, node_id: str, addr: PartitionAddress) -> Record | None:
        payload = await self._remote_request(
            "replication.read", node_id, {"addr": addr.to_dict()}
        )
        if payload is None:
            return None
        return Record.from_dict(payload)

    async def _remote_write(self, node_id: str, record: Record) -> Record:
        payload = await self._remote_request(
            "replication.write", node_id, {"record": record.to_dict()}
        )
        return Record.from_dict(payload)

    async def _remote_read_at(self, node_id: str, key: Key) -> Record | None:
        payload = await self._remote_request(
            "replication.lookup", node_id, {"key": key.to_dict()}
        )
        return VersionLookup.from_dict(payload).record

    async def _remote_request(
        self, kind: str, node_id: str, payload: dict[str, Any]
    ) -> Any:
        with self._telemetry.start_span(
            "peer.request",
            attributes={"kind": kind, "peer": node_id},
        ):
            client = await self._peer_pool.acquire(node_id)
            try:
                envelope = build_envelope(
                    kind=kind,
                    payload=self._serializer.encode(payload),
                    schema_id=self._serializer.schema_id,
                )
                response = await client.request(envelope)
            finally:
                await self._peer_pool.release(node_id, client)

            expected = f"{kind}.accept"
            if response.kind == f"{kind}.reject":
                data = self._serializer.decode(response.payload)
                if data.get("errors"):
                    raise StoreOperationError.from_dict(data)
                raise RuntimeError(f"invalid response kind for {kind}: {response.kind}")
            if response.kind != expected:
                raise RuntimeError(f"invalid response kind for {kind}: {response.kind}")
            return self._serializer.decode(response.payload)

    @staticmethod
    def _replication_targets(
        preferences: Sequence[Preference],
    ) -> dict[str, frozenset[str]]:
        targets: dict[str, frozenset[str]] = {}
        for entry in preferences:
            sources: set[str] = set()
            if entry.readable:
                sources.add(entry.node_id)
            if entry.handoff is not None:
                sources.add(entry.handoff)
            if sources:
                targets[entry.node_id] = frozenset(sources)
        if not targets:
            raise NoReplicaAvailableError("read")
        return targets

    @staticmethod
    def _write_targets(
        preferences: Sequence[Preference],
    ) -> list[tuple[str, str | None]]:
        targets: list[tuple[str, str | None]] = []
        for entry in preferences:
            target = StoreService._write_target(entry)
            if target is not None:
                targets.append(target)
        return targets

    @staticmethod
    def _ensure_quorum(
        success_count: int,
        required: int,
        op: str,
        errors: Sequence[Exception] | None = None,
        token: str | None = None,
    ) -> None:
        if success_count < required:
            raise QuorumNotMetError(
                op,
                required=required,
                received=success_count,
                errors=errors or (),
                token=token,
            )

    @staticmethod
    def _replication_error(
        operation: str,
        errors: Sequence[Exception],
        message: str | None = None,
    ) -> StoreOperationError:
        return StoreOperationError(operation=operation, errors=errors, message=message)

    async def _local_read(self, addr: PartitionAddress, *, kind: str) -> Record | None:
        group = self._partitioner.group_for(addr.pid.value)
        with self._telemetry.start_span(
            "storage.operation", attributes={"group": group, "kind": kind}
        ):
            provider = await self._storage.session_provider_for(group)
            manager = GroupSessionManager(provider)
            async with manager.session() as session:
                return await session.get(addr)

    async def _local_read_at(self, key: Key, *, kind: str) -> Record | None:
        group = self._partitioner.group_for(key.pid.value)
        with self._telemetry.start_span(
            "storage.operation", attributes={"group": group, "kind": kind}
        ):
            provider = await self._storage.session_provider_for(group)
            manager = GroupSessionManager(provider)
            async with manager.session() as session:
                return await session.read(key)

    async def _local_write(self, record: Record, *, kind: str) -> Record:
        if record.tag.kind not in _OWNED_KINDS:
            raise ValueError("replicated records must be LIVE or TOMBSTONE")
        group = self._partitioner.group_for(record.key.pid.value)
        with (
            self._telemetry.start_span("storage.write", attributes={"group": group}),
            self._telemetry.start_span(
                "storage.operation",
                attributes={"group": group, "kind": kind},
            ),
            self._telemetry.start_span(
                "storage.transaction", attributes={"group": group}
            ),
        ):
            provider = await self._storage.session_provider_for(group)
            manager = GroupSessionManager(provider)
            async with manager.session(write=True) as session:
                with self._telemetry.start_span("storage.record.put"):
                    await session.put(record)
        return record

    async def _drain_parallel_tasks(
        self,
        tasks: set[asyncio.Task[Any]],
        collect: Callable[[set[asyncio.Task[Any]]], None],
        should_stop: Callable[[], bool] | None,
        cancel_pending: bool = True,
    ) -> set[asyncio.Task[Any]]:
        """Collect results until should_stop() holds or the response deadline expires.

        Return the tasks still running at the deadline, which the caller reports
        as unanswered replicas. What happens to them next differs by operation:
        a late read is worthless, so it is cancelled, but a late write may still
        land, and cancelling it would turn a replica that was merely slow into
        one that permanently missed the write. Writes therefore detach.
        """
        stop = should_stop or (lambda: False)
        pending = set(tasks)
        try:
            if pending:
                done, pending = await asyncio.wait(
                    pending, timeout=self._response_delay
                )
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
            if cancel_pending:
                await self._cancel_pending_tasks(pending)
            else:
                self._detach(pending)

    def _detach(self, tasks: set[asyncio.Task[Any]]) -> None:
        """Let slow writes finish in the background, owned by the service."""
        for task in tasks:
            self._pending_writes.add(task)
            task.add_done_callback(self._settle_detached)

    def _settle_detached(self, task: asyncio.Task[Any]) -> None:
        self._pending_writes.discard(task)
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            self._replica_failed("write", task.get_name(), exc, late=True)

    def _replica_failed(
        self, operation: str, target: str, exc: BaseException, *, late: bool = False
    ) -> None:
        # One replica failing is routine and absorbed by the quorum; WARNING
        # is kept because a replica that fails repeatedly is worth spotting.
        self._telemetry.replication_failures_total.add(1, attributes={"op": operation})
        logger.warning(
            "replication.%s failed%s node=%s target=%s error=%s",
            operation,
            " (late)" if late else "",
            self._node_id,
            target,
            describe(exc),
        )

    def _read_repairs(self, outcome: str, count: int) -> None:
        setattr(
            self._stats,
            f"read_repairs_{outcome}",
            getattr(self._stats, f"read_repairs_{outcome}") + count,
        )
        self._telemetry.read_repairs_total.add(count, attributes={"outcome": outcome})

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

    def _write_record(
        self,
        addr: Address,
        value: bytes,
        pid: int,
        tombstone: bool,
        quorum_write: int,
    ) -> Record:
        """Build the record to replicate, recording the durability asked for.

        The requested quorum is stored with the version rather than a constant:
        it is what tells a later repair how durable the write was meant to be.
        """
        meta = Meta(quorum_write=quorum_write, ts=self._clock.tick())
        tag = Tag(kind=TagKind.TOMBSTONE if tombstone else TagKind.LIVE)
        return Record(
            key=Key(pid=Pid(pid), address=addr, ts=meta.ts),
            value=Value(data=value, quorum_write=meta.quorum_write),
            tag=tag,
        )

    @staticmethod
    def _validate_quorum(operation: str, quorum: int) -> None:
        if (
            not isinstance(quorum, int)
            or isinstance(quorum, bool)
            or not 1 <= quorum <= MAX_QUORUM
        ):
            raise InvalidQuorumError(operation, quorum)

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
            if record is None or record.tag.kind not in _OWNED_KINDS:
                continue
            versions.append(self._version_from_record(record))
        return versions

    @staticmethod
    def _is_tombstone(
        version: Version | None, records: Sequence[Record | None]
    ) -> bool:
        """Return True when the elected version is a delete."""
        if version is None:
            return False
        return any(
            record is not None
            and record.key.ts == version.meta.ts
            and record.tag.kind is TagKind.TOMBSTONE
            for record in records
        )

    @classmethod
    def _select_version(cls, versions: Sequence[Version]) -> tuple[Version | None, int]:
        """Elect the winning version and report how many replicas hold it.

        The timestamp alone elects: agreement between replicas never outranks
        freshness. Returning a plurality to the caller would otherwise make a
        stale value win whenever more replicas missed the latest write than
        received it, and would make freshness *degrade* as the replication
        factor grows. The count is returned for quorum accounting only.
        """
        if not versions:
            return None, 0

        counts = Counter(versions)
        version, count = max(counts.items(), key=cls._version_rank)
        return version, count

    @staticmethod
    def _version_rank(item: tuple[Version, int]) -> tuple[Any, ...]:
        version, _ = item
        return (
            version.meta.ts,
            version.address.namespace,
            version.address.key,
            version.data,
            version.meta.quorum_write,
        )
