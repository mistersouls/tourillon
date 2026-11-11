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
import logging
import time
from contextlib import asynccontextmanager
from time import perf_counter
from typing import Any, cast

import pytest

from tests.helpers.telemetry import RecordingTelemetry
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
from tourillon.core.models.clock import HLCClock
from tourillon.core.models.config import ReadRepairConfig, RetentionConfig
from tourillon.core.models.member import Member, MemberPhase
from tourillon.core.models.repair import (
    RepairIssue,
    VersionLookup,
    decode_token,
    encode_token,
)
from tourillon.core.models.storageschema import (
    Key,
    KeyList,
    Keyspace,
    KeyTag,
    KVList,
    KVPair,
    KVRef,
    MetaKind,
    ObligationKey,
    ObligationPrefix,
    PartitionAddress,
    Pid,
    Record,
    Tag,
    TagKind,
    Value,
)
from tourillon.core.partition.partitioner import Partitioner
from tourillon.core.partition.placement import (
    PlacementStrategy,
    Preference,
    PreferenceList,
)
from tourillon.core.partition.types import LogicalPartition, PartitionPlacement
from tourillon.core.peer.pool import PeerPool
from tourillon.core.ports.storage import Storage
from tourillon.core.services.store import StorageService
from tourillon.core.spaces.vnode import VNode
from tourillon.core.storage.retention import RetentionPolicy
from tourillon.core.storage.store import LocalStore
from tourillon.core.utils.defer import Result
from tourlib.adapter import MsgPackSerializer
from tourlib.constant import MAX_CLOCK_SKEW_MS, MAX_QUORUM
from tourlib.envelope import Envelope, build_envelope
from tourlib.models.kv import DeleteRequest, GetRequest, PutRequest, RepairRequest
from tourlib.models.ts import HLCTimestamp
from tourlib.models.version import Address, Meta, Version
from tourlib.port import Serializer


class _PlacementStub:
    def __init__(self, preferences: tuple[Preference, ...], pid: int = 7) -> None:
        self._preferences = preferences
        self._placement = PartitionPlacement(
            group=0,
            vnode=VNode(
                token=0, node_id=preferences[0].node_id if preferences else "node-a"
            ),
            partition=LogicalPartition(pid=pid, start=0, end=1),
        )

    async def preference_list(self, addr: Address) -> PreferenceList:
        return PreferenceList(placement=self._placement, preferences=self._preferences)

    async def placement_for(self, addr: Address) -> PartitionPlacement:
        return self._placement


class _StoreStub:
    def __init__(self, record: Record | None = None) -> None:
        self.record = record
        self.get_calls: list[PartitionAddress] = []
        self.put_calls: list[Record] = []
        self.written: list[KVPair] = []

    def on_put(self, kv: KVPair) -> None:
        """Hook for the session stub; subclasses raise here to fail a write."""
        self.written.append(kv)

    async def get(self, key: PartitionAddress) -> Record | None:
        self.get_calls.append(key)
        return self.record

    async def put(self, record: Record) -> Record:
        self.put_calls.append(record)
        return record


class _SessionStub:
    def __init__(self, store: _StoreStub) -> None:
        self.store = store
        self.key_pages = 0
        self._pairs: dict[tuple[Keyspace, bytes], KVPair] = {}
        if store.record is not None:
            self._pairs[(Keyspace.LOGS, store.record.key.to_bytes(Keyspace.LOGS))] = (
                KVPair(
                    key=store.record.key.to_bytes(Keyspace.LOGS),
                    value=store.record.value.to_bytes(),
                    keyspace=Keyspace.LOGS,
                )
            )
            self._pairs[(Keyspace.TAGS, store.record.key.to_bytes(Keyspace.TAGS))] = (
                KVPair(
                    key=store.record.key.to_bytes(Keyspace.TAGS),
                    value=store.record.tag.to_bytes(),
                    keyspace=Keyspace.TAGS,
                )
            )

    async def get(self, ref: KVRef) -> KVPair | None:
        return self._pairs.get((ref.keyspace, ref.key))

    async def exists(self, ref: KVRef) -> bool:
        return (ref.keyspace, ref.key) in self._pairs

    async def put(self, kv: KVPair, overwrite: bool = True) -> bool:
        if not overwrite and (kv.keyspace, kv.key) in self._pairs:
            return False
        self.store.on_put(kv)
        self._pairs[(kv.keyspace, kv.key)] = kv
        if kv.keyspace == Keyspace.LOGS:
            key = Key.from_bytes(kv.key, Keyspace.LOGS)
            value = Value.from_bytes(kv.value)
            stored_tag = self._pairs.get((Keyspace.TAGS, key.to_bytes(Keyspace.TAGS)))
            self.store.record = Record(
                key=key,
                value=value,
                tag=(
                    Tag.from_bytes(stored_tag.value)
                    if stored_tag is not None
                    else Tag(kind=TagKind.LIVE)
                ),
            )
        if kv.keyspace == Keyspace.TAGS and self.store.record is not None:
            self.store.record = Record(
                key=self.store.record.key,
                value=self.store.record.value,
                tag=Tag.from_bytes(kv.value),
            )
        return True

    async def last(
        self,
        keyspace: Keyspace,
        prefix: Any | None = None,
        predicate: Any | None = None,
    ) -> KVPair | None:
        prefix_bytes = prefix.to_bytes() if prefix is not None else b""
        pairs = [
            pair
            for (ks, key), pair in self._pairs.items()
            if ks == keyspace and key.startswith(prefix_bytes)
        ]
        if predicate is not None:
            pairs = [pair for pair in pairs if predicate(pair)]
        if not pairs:
            return None
        return max(pairs, key=lambda pair: pair.key)

    async def keys(self, keyspace, prefix=None, start=None, limit=64, reverse=False):
        self.key_pages += 1
        prefix_bytes = b"" if prefix is None else prefix.to_bytes()
        keys = sorted(
            (
                key
                for space, key in self._pairs
                if space == keyspace
                and key.startswith(prefix_bytes)
                and (start is None or (key < start if reverse else key > start))
            ),
            reverse=reverse,
        )
        return [KVRef(key, keyspace) for key in keys[:limit]]

    async def list(self, keyspace, prefix=None, start=None, limit=64, reverse=False):
        refs = await self.keys(keyspace, prefix, start, limit, reverse)
        return [self._pairs[(ref.keyspace, ref.key)] for ref in refs]

    async def get_many(self, refs: list[KeyList]):
        return [
            self._pairs[(ref.keyspace, key)]
            for ref in refs
            for key in ref.items
            if (ref.keyspace, key) in self._pairs
        ]

    async def put_many(self, kvs: list[KVList], overwrite=True):
        pairs = [
            KVPair(key, value, kv.keyspace) for kv in kvs for key, value in kv.items
        ]
        added = 0
        for pair in pairs:
            added += await self.put(pair, overwrite=overwrite)
        return len(pairs), added

    async def delete_many(self, refs: list[KeyList]):
        return [
            self._pairs.pop((ref.keyspace, key), None)
            for ref in refs
            for key in ref.items
        ]


class _TransactionStub:
    def __init__(self, session: _SessionStub) -> None:
        self._session = session
        self._ops = []

    def __getattr__(self, name):
        def enqueue(*args, **kwargs):
            future = asyncio.get_running_loop().create_future()
            self._ops.append((name, args, kwargs, future))
            return Result(self, future)

        return enqueue

    async def results(self):
        return await self.resolve()

    async def resolve(self):
        ops, self._ops = self._ops, []
        results = []
        for name, args, kwargs, future in ops:
            value = await getattr(self._session, name)(*args, **kwargs)
            future.set_result(value)
            results.append(value)
        return results


class _TransactionalStub:
    def __init__(self, group_id: int, session: _SessionStub) -> None:
        self.group_id = group_id
        self._session = session

    @asynccontextmanager
    async def transaction(self, write: bool = False) -> Any:
        _ = write
        txn = _TransactionStub(self._session)
        yield txn
        await txn.results()

    async def close(self) -> None:
        return None


class _StorageStub:
    def __init__(self, store: _StoreStub) -> None:
        self.store = store
        self._session = _SessionStub(store)
        self.open_calls: list[int] = []

    async def transactional_for(self, group: int) -> _TransactionalStub:
        self.open_calls.append(group)
        return _TransactionalStub(group, self._session)

    async def latest_hlc(self, total_groups: int) -> HLCTimestamp:
        _ = total_groups
        return HLCTimestamp(wall_ms=0, counter=0, node_id="node-a")

    async def close(self) -> None:
        return None


class _PartitionerStub:
    def __init__(self, pid: int = 7) -> None:
        self.space = _HashSpaceStub()
        self._pid = pid

    def group_for(self, pid: int) -> int:
        return pid

    def pid_for_hash(self, token: int) -> int:
        _ = token
        return self._pid


class _HashSpaceStub:
    def hash(self, data: bytes) -> int:
        _ = data
        return 0


class _RegistryStub:
    def __init__(self, members: dict[str, Member]) -> None:
        self._members = members

    def get(self, node_id: str) -> Member | None:
        return self._members.get(node_id)


class _TopologyStub:
    def __init__(self, members: dict[str, Member]) -> None:
        self.registry = _RegistryStub(members)


class _TopologyManagerStub:
    def __init__(self, members: dict[str, Member]) -> None:
        self._topology = _TopologyStub(members)

    async def snapshot(self) -> _TopologyStub:
        return self._topology


class _PeerClientStub:
    def __init__(
        self,
        serializer: MsgPackSerializer,
        responses: dict[str, Any | list[Any]],
        delays: dict[str, float | list[float]] | None = None,
    ) -> None:
        self._serializer = serializer
        self._responses = responses
        self._delays = {
            kind: (value.copy() if isinstance(value, list) else value)
            for kind, value in (delays or {}).items()
        }
        self.requests: list[Envelope] = []

    async def request(self, envelope: Envelope) -> Envelope:
        self.requests.append(envelope)
        delay = self._next_delay(envelope.kind)
        if delay:
            await asyncio.sleep(delay)
        response = self._responses[envelope.kind]
        payload_doc = response.pop(0) if isinstance(response, list) else response
        if envelope.kind == "replication.lookup":
            payload_doc = (
                payload_doc.to_dict()
                if isinstance(payload_doc, VersionLookup)
                else {
                    "record": payload_doc,
                    "retained": [],
                    "outside_retention": False,
                }
            )
        payload = self._serializer.encode(payload_doc)
        return build_envelope(
            kind=f"{envelope.kind}.accept",
            payload=payload,
            schema_id=self._serializer.schema_id,
            correlation_id=envelope.correlation_id,
        )

    def _next_delay(self, kind: str) -> float:
        delay_value = self._delays.get(kind, 0.0)
        if isinstance(delay_value, list):
            delay_value = delay_value.pop(0)
        if isinstance(delay_value, (int, float)):
            return float(delay_value)
        return 0.0


class _PeerPoolStub:
    def __init__(self, client: _PeerClientStub, known_peers: set[str]) -> None:
        self._client = client
        self._known_peers = known_peers
        self.acquire_calls: list[str] = []
        self.release_calls: list[str] = []

    async def acquire(self, peer_id: str) -> _PeerClientStub:
        if peer_id not in self._known_peers:
            raise RuntimeError(f"unknown peer: {peer_id}")
        self.acquire_calls.append(peer_id)
        return self._client

    async def release(self, peer_id: str, client: object | None = None) -> None:
        self.release_calls.append(peer_id)


def _member(node_id: str, bind: str) -> Member:
    return Member(
        node_id=node_id,
        peer_address=bind,
        epoch=1,
        phase=MemberPhase.READY,
        tokens=("01",),
    )


def _version(addr: Address, data: bytes = b"value", wall_ms: int = 3) -> Version:
    return Version(
        address=addr,
        data=data,
        meta=Meta(
            ts=HLCTimestamp(wall_ms=wall_ms, counter=0, node_id="node-a"),
            quorum_write=1,
        ),
    )


def _record(
    addr: Address,
    pid: int = 7,
    data: bytes = b"value",
    tombstone: bool = False,
    wall_ms: int = 3,
) -> Record:
    tag = Tag(kind=TagKind.TOMBSTONE if tombstone else TagKind.LIVE)
    return Record(
        key=Key(
            pid=Pid(pid),
            address=addr,
            ts=HLCTimestamp(wall_ms=wall_ms, counter=0, node_id="node-a"),
        ),
        value=Value(data=data, quorum_write=1),
        tag=tag,
    )


@pytest.mark.tourillon
async def test_exact_repair_reports_outdated_without_substituting_a_newer_version() -> (
    None
):
    address = Address(namespace=b"ns", key=b"key")
    old = _record(address, wall_ms=1)
    current = _record(address, wall_ms=3)
    members = {
        node: _member(node, f"127.0.0.1:{port}")
        for node, port in [("node-a", 9001), ("node-b", 9002)]
    }
    store, _, _, _, client = _build_store(
        node_id="node-a",
        preferences=(
            Preference("node-a", True, None, True),
            Preference("node-b", True, None, True),
        ),
        members=members,
        local_store=_StoreStub(current),
        remote_responses={
            "replication.lookup": VersionLookup(
                None, (KeyTag(current.key, current.tag),), True
            )
        },
        retention_versions=1,
    )
    with pytest.raises(OutdatedRepairError) as caught:
        await store.repair(RepairRequest(token=encode_token(old.key), quorum_write=2))
    payload = caught.value.to_dict()
    assert payload["code"] == "outdated"
    assert payload["received"] == 0
    assert payload["required"] == 2
    assert payload["newer"] == [
        {
            "version": KeyTag(current.key, current.tag).to_dict(),
            "replicas": 2,
            "owners": ["node-a", "node-b"],
        }
    ]
    assert all(e.kind == "replication.lookup" for e in client.requests)
    assert await store.read_at(current.key) == current
    assert await store.read_at(old.key) is None


@pytest.mark.tourillon
@pytest.mark.parametrize("hinted", [False, True])
async def test_replication_stores_exact_history_without_retention_admission(
    hinted: bool,
) -> None:
    address = Address(namespace=b"ns", key=b"key")
    current = _record(address, wall_ms=3)
    store, storage, *_ = _build_store(
        node_id="node-a",
        preferences=(Preference("node-a", True, None, True),),
        members={"node-a": _member("node-a", "127.0.0.1:9001")},
        local_store=_StoreStub(current),
        remote_responses={},
        retention_versions=1,
    )
    old = _record(address, wall_ms=1)
    if hinted:
        result = await store.hint(old, "node-b")
    else:
        result = await store.write(old)
    assert result == old
    assert await store.read_at(old.key) == old
    assert await store.read_at(current.key) == current
    assert storage._session.key_pages == 0
    assert (
        any(
            ks == Keyspace.META and key.startswith(MetaKind.OBLIGATION.value)
            for ks, key in storage._session._pairs
        )
        == hinted
    )


@pytest.mark.tourillon
async def test_exact_repair_restores_history_when_another_replica_has_a_source() -> (
    None
):
    address = Address(namespace=b"ns", key=b"key")
    old = _record(address, wall_ms=1)
    current = _record(address, wall_ms=3)
    store, storage, _, _, client = _build_store(
        node_id="node-a",
        preferences=(
            Preference("node-a", True, None, True),
            Preference("node-b", True, None, True),
        ),
        members={
            node: _member(node, f"127.0.0.1:{port}")
            for node, port in [("node-a", 9001), ("node-b", 9002)]
        },
        local_store=_StoreStub(current),
        remote_responses={"replication.lookup": VersionLookup(old)},
        retention_versions=1,
    )
    result = await store.repair(
        RepairRequest(token=encode_token(old.key), quorum_write=2)
    )
    assert result.issue == RepairIssue.REPAIRED
    assert result.replicas == 2
    assert await store.read_at(old.key) == old
    assert await store.read_at(current.key) == current
    assert storage._session.key_pages == 1
    assert all(e.kind == "replication.lookup" for e in client.requests)


@pytest.mark.tourillon
async def test_kv_put_does_not_read_retention_metadata() -> None:
    store, storage, *_ = _build_store(
        node_id="node-a",
        preferences=(Preference("node-a", True, None, True),),
        members={},
        local_store=_StoreStub(),
        remote_responses={},
    )
    for data in [b"first", b"second", b"third"]:
        version = await store.put(PutRequest(namespace=b"ns", key=b"key", value=data))
        assert version is not None
        assert version.data == data
    assert storage._session.key_pages == 0
    assert sum(ks == Keyspace.LOGS for ks, _ in storage._session._pairs) == 3
    await store.close()


def _build_store(
    *,
    node_id: str,
    preferences: tuple[Preference, ...],
    members: dict[str, Member],
    local_store: _StoreStub,
    remote_responses: dict[str, Any],
    remote_delays: dict[str, Any] | None = None,
    response_delay: float = 0.05,
    partitioner_pid: int = 7,
    read_repair: ReadRepairConfig | None = None,
    obligations: ObligationQueue | None = None,
    telemetry: RecordingTelemetry | None = None,
    retention_versions: int = 2,
) -> tuple[
    StorageService, _StorageStub, _PeerPoolStub, MsgPackSerializer, _PeerClientStub
]:
    serializer = MsgPackSerializer()
    client = _PeerClientStub(serializer, remote_responses, remote_delays)
    peer_pool = _PeerPoolStub(client, known_peers=set(members))
    storage = _StorageStub(local_store)
    service = StorageService(
        node_id=node_id,
        store=LocalStore(
            cast(Storage, cast(object, storage)),
            cast(Partitioner, cast(object, _PartitionerStub(partitioner_pid))),
            RetentionPolicy(RetentionConfig(versions=retention_versions)),
            telemetry=None if telemetry is None else telemetry.as_port(),
        ),
        partitioner=cast(Partitioner, cast(object, _PartitionerStub(partitioner_pid))),
        placement_strategy=cast(
            PlacementStrategy, cast(object, _PlacementStub(preferences))
        ),
        clock=HLCClock(node_id),
        serializer=cast(Serializer, serializer),
        peer_pool=cast(PeerPool, cast(object, peer_pool)),
        response_delay=response_delay,
        read_repair=read_repair,
        obligations=obligations,
        telemetry=None if telemetry is None else telemetry.as_port(),
    )
    return service, storage, peer_pool, serializer, client


@pytest.mark.tourillon
@pytest.mark.parametrize("operation", ["get", "put", "delete"])
async def test_omitted_quorum_requires_a_replica(operation: str) -> None:
    store, *_ = _build_store(
        node_id="node-a",
        preferences=(),
        members={},
        local_store=_StoreStub(),
        remote_responses={},
    )
    with pytest.raises(StoreOperationError):
        if operation == "get":
            await store.get(GetRequest(namespace=b"ns", key=b"key"))
        elif operation == "put":
            await store.put(PutRequest(namespace=b"ns", key=b"key", value=b"value"))
        else:
            await store.delete(DeleteRequest(namespace=b"ns", key=b"key"))


@pytest.mark.tourillon
async def test_omitted_quorums_record_one_and_allow_absent_reads() -> None:
    store, *_ = _build_store(
        node_id="node-a",
        preferences=(
            Preference(node_id="node-a", readable=True, handoff=None, writable=True),
        ),
        members={},
        local_store=_StoreStub(),
        remote_responses={},
    )
    assert await store.get(GetRequest(namespace=b"ns", key=b"key")) is None
    version = await store.put(PutRequest(namespace=b"ns", key=b"key", value=b"value"))
    assert version is not None
    assert version.meta.quorum_write == 1
    deleted = await store.delete(DeleteRequest(namespace=b"ns", key=b"key"))
    assert deleted is not None
    assert deleted.meta.quorum_write == 1
    await store.close()


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_get_elects_freshest_version_even_when_outnumbered() -> None:
    """Freshness decides: a plurality of replicas never outvotes a newer write.

    Two replicas still hold the older value and only one received the newer
    one, which is the normal state right after a write that reached a minority.
    Electing by plurality would silently discard that acknowledged write.
    """
    addr = Address(namespace=b"ns", key=b"key")
    stale = _record(addr, data=b"stale", wall_ms=3)
    fresh = _record(addr, data=b"fresh", wall_ms=9)
    store, storage, peer_pool, _, _ = _build_store(
        node_id="node-a",
        preferences=(
            Preference(node_id="node-a", readable=True, handoff=None, writable=True),
            Preference(node_id="node-b", readable=True, handoff=None, writable=True),
            Preference(node_id="node-c", readable=True, handoff=None, writable=True),
        ),
        members={
            "node-b": _member("node-b", "127.0.0.1:9001"),
            "node-c": _member("node-c", "127.0.0.1:9002"),
        },
        local_store=_StoreStub(stale),
        remote_responses={"replication.read": [stale.to_dict(), fresh.to_dict()]},
    )

    version = await store.get(GetRequest(namespace=b"ns", key=b"key"))

    assert version == _version(addr, data=b"fresh", wall_ms=9)
    assert storage.open_calls == [7]
    assert peer_pool.acquire_calls == ["node-b", "node-c"]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_get_coordinates_locally_when_node_in_preference_list() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    expected_record = _record(addr, data=b"local", wall_ms=3)
    store, storage, peer_pool, _, _ = _build_store(
        node_id="node-a",
        preferences=(
            Preference(node_id="node-a", readable=True, handoff=None, writable=True),
            Preference(node_id="node-b", readable=True, handoff=None, writable=True),
        ),
        members={"node-b": _member("node-b", "127.0.0.1:9001")},
        local_store=_StoreStub(expected_record),
        remote_responses={"replication.read": expected_record.to_dict()},
    )

    version = await store.get(GetRequest(namespace=b"ns", key=b"key"))

    assert version == _version(addr, data=b"local")
    assert storage.open_calls == [7]
    assert peer_pool.acquire_calls == ["node-b"]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_get_fans_out_to_all_available_preference_targets() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    remote = _record(addr, data=b"remote")
    store, storage, peer_pool, serializer, client = _build_store(
        node_id="node-a",
        preferences=(
            Preference(node_id="node-b", readable=True, handoff=None, writable=True),
            Preference(node_id="node-c", readable=True, handoff=None, writable=True),
        ),
        members={
            "node-b": _member("node-b", "127.0.0.1:9001"),
            "node-c": _member("node-c", "127.0.0.1:9002"),
        },
        local_store=_StoreStub(),
        remote_responses={"replication.read": [remote.to_dict(), remote.to_dict()]},
    )

    version = await store.get(GetRequest(namespace=b"ns", key=b"key"))

    assert version == _version(addr, data=b"remote")
    assert storage.open_calls == []
    assert peer_pool.acquire_calls == ["node-b", "node-c"]
    assert peer_pool.release_calls == ["node-b", "node-c"]
    assert [request.kind for request in client.requests] == [
        "replication.read",
        "replication.read",
    ]
    payload = serializer.decode(client.requests[1].payload)
    assert payload["addr"]["address"] == {"namespace": b"ns", "key": b"key"}


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_put_coordinates_and_uses_handoff_target() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    local = _record(addr, data=b"local")
    remote = _record(addr, data=b"remote")
    hinted = _record(addr, data=b"hinted")
    store, storage, peer_pool, serializer, client = _build_store(
        node_id="node-a",
        preferences=(
            Preference(node_id="node-a", readable=True, handoff=None, writable=True),
            Preference(node_id="node-b", readable=True, handoff=None, writable=True),
            Preference(
                node_id="node-c", readable=False, handoff="node-d", writable=False
            ),
        ),
        members={
            "node-b": _member("node-b", "127.0.0.1:9001"),
            "node-d": _member("node-d", "127.0.0.1:9003"),
        },
        local_store=_StoreStub(local),
        remote_responses={
            "replication.write": remote.to_dict(),
            "replication.hint": hinted.to_dict(),
        },
    )

    version = await store.put(PutRequest(namespace=b"ns", key=b"key", value=b"value"))

    assert version is not None
    assert version.address == addr
    assert version.data == b"value"
    assert version.meta.quorum_write == 1
    assert storage.open_calls == [7]
    assert [request.kind for request in client.requests] == [
        "replication.write",
        "replication.hint",
    ]
    second_payload = serializer.decode(client.requests[1].payload)
    sent = Record.from_dict(second_payload["record"])
    assert sent.tag == Tag(kind=TagKind.LIVE)
    assert sent.value.data == b"value"
    assert second_payload["intent"] == "node-c"
    assert "intent" not in serializer.decode(client.requests[0].payload)
    assert peer_pool.acquire_calls == ["node-b", "node-d"]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_put_replicates_to_handoff_and_readable_targets() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    hinted = _record(addr, data=b"hinted")
    store, storage, peer_pool, _, client = _build_store(
        node_id="node-a",
        preferences=(
            Preference(
                node_id="node-b", readable=True, handoff="node-z", writable=True
            ),
            Preference(node_id="node-c", readable=True, handoff=None, writable=True),
        ),
        members={
            "node-z": _member("node-z", "127.0.0.1:9009"),
            "node-c": _member("node-c", "127.0.0.1:9002"),
        },
        local_store=_StoreStub(),
        remote_responses={
            "replication.write": hinted.to_dict(),
            "replication.hint": hinted.to_dict(),
        },
    )

    version = await store.put(PutRequest(namespace=b"ns", key=b"key", value=b"value"))

    assert version == _version(addr, data=b"hinted")
    assert storage.open_calls == []
    assert peer_pool.acquire_calls == ["node-z", "node-c"]
    assert [request.kind for request in client.requests] == [
        "replication.hint",
        "replication.write",
    ]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_delete_returns_unique_versions() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    tombstone = _record(addr, data=b"", tombstone=True)
    store, storage, peer_pool, _, client = _build_store(
        node_id="node-a",
        preferences=(
            Preference(node_id="node-a", readable=True, handoff=None, writable=True),
            Preference(node_id="node-b", readable=True, handoff=None, writable=True),
            Preference(
                node_id="node-c", readable=False, handoff="node-d", writable=False
            ),
        ),
        members={
            "node-b": _member("node-b", "127.0.0.1:9001"),
            "node-d": _member("node-d", "127.0.0.1:9003"),
        },
        local_store=_StoreStub(tombstone),
        remote_responses={
            "replication.write": tombstone.to_dict(),
            "replication.hint": tombstone.to_dict(),
        },
    )

    version = await store.delete(DeleteRequest(namespace=b"ns", key=b"key"))

    assert version is not None
    assert version.address == addr
    assert version.data == b""
    assert version.meta.quorum_write == 1
    assert storage.open_calls == [7]
    assert [request.kind for request in client.requests] == [
        "replication.write",
        "replication.hint",
    ]
    assert peer_pool.acquire_calls == ["node-b", "node-d"]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_put_returns_after_quorum_without_waiting_for_slow_replicas() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    record = _record(addr, data=b"quorum")
    store, _, peer_pool, _, client = _build_store(
        node_id="node-a",
        preferences=(
            Preference(node_id="node-b", readable=True, handoff=None, writable=True),
            Preference(node_id="node-c", readable=True, handoff=None, writable=True),
        ),
        members={
            "node-b": _member("node-b", "127.0.0.1:9001"),
            "node-c": _member("node-c", "127.0.0.1:9002"),
        },
        local_store=_StoreStub(),
        remote_responses={"replication.write": [record.to_dict(), record.to_dict()]},
        remote_delays={"replication.write": [0.2, 0.0]},
        response_delay=0.01,
    )

    started = perf_counter()
    version = await store.put(
        PutRequest(namespace=b"ns", key=b"key", value=b"quorum", quorum_write=1)
    )
    elapsed = perf_counter() - started

    assert version == _version(addr, data=b"quorum")
    assert elapsed < 0.1
    assert peer_pool.acquire_calls == ["node-b", "node-c"]
    assert [request.kind for request in client.requests] == [
        "replication.write",
        "replication.write",
    ]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_get_returns_empty_list_when_remote_read_is_missing() -> None:
    store, _, peer_pool, _, _ = _build_store(
        node_id="node-a",
        preferences=(
            Preference(node_id="node-b", readable=True, handoff=None, writable=True),
        ),
        members={"node-b": _member("node-b", "127.0.0.1:9001")},
        local_store=_StoreStub(),
        remote_responses={"replication.read": None},
    )

    version = await store.get(GetRequest(namespace=b"ns", key=b"key"))

    assert version is None
    assert peer_pool.acquire_calls == ["node-b"]
    assert peer_pool.release_calls == ["node-b"]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_get_raises_when_no_replication_targets_available() -> None:
    store, _, _, _, _ = _build_store(
        node_id="node-a",
        preferences=(
            Preference(node_id="node-b", readable=False, handoff=None, writable=False),
        ),
        members={},
        local_store=_StoreStub(),
        remote_responses={"replication.read": None},
    )

    with pytest.raises(
        NoReplicaAvailableError, match="preference list has no replication target"
    ) as exc_info:
        await store.get(GetRequest(namespace=b"ns", key=b"key"))
    assert exc_info.value.to_dict()["code"] == "no_replica_available"
    assert exc_info.value.to_dict()["operation"] == "read"


@pytest.mark.tourillon
@pytest.mark.parametrize("local_is_owner", [True, False])
@pytest.mark.parametrize("remote_kind", ["missing", "newer", "tombstone"])
async def test_get_reads_both_owner_and_handoff_before_counting_preference(
    local_is_owner: bool, remote_kind: str
) -> None:
    addr = Address(namespace=b"ns", key=b"key")
    local = _record(addr, data=b"historical", wall_ms=10)
    remote = (
        None
        if remote_kind == "missing"
        else _record(
            addr,
            data=b"newer",
            wall_ms=20,
            tombstone=remote_kind == "tombstone",
        )
    )
    owner, handoff = ("node-a", "node-b") if local_is_owner else ("node-b", "node-a")
    store, _, pool, _, _ = _build_store(
        node_id="node-a",
        preferences=(Preference(owner, True, handoff, writable=True),),
        members={"node-b": _member("node-b", "127.0.0.1:9001")},
        local_store=_StoreStub(local),
        remote_responses={
            "replication.read": None if remote is None else remote.to_dict()
        },
        remote_delays={"replication.read": 0.02},
        response_delay=0,
        read_repair=ReadRepairConfig(enabled=False),
    )
    store.pause()
    version = await store.get(GetRequest(namespace=b"ns", key=b"key"))

    if remote_kind == "tombstone":
        assert version is None
    else:
        expected = b"historical" if remote is None else b"newer"
        assert version is not None and version.data == expected
    assert pool.acquire_calls == ["node-b"]


@pytest.mark.tourillon
async def test_get_owner_and_handoff_cannot_satisfy_two_replica_quorum() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    record = _record(addr)
    store, _, _, _, _ = _build_store(
        node_id="node-a",
        preferences=(
            Preference("node-a", True, "node-b", writable=True),
            Preference("node-c", True, None, writable=True),
        ),
        members={"node-b": _member("node-b", "127.0.0.1:9001")},
        local_store=_StoreStub(record),
        remote_responses={"replication.read": record.to_dict()},
    )
    with pytest.raises(QuorumNotMetError) as exc_info:
        await store.get(GetRequest(namespace=b"ns", key=b"key", quorum_read=2))
    assert exc_info.value.to_dict()["received"] == 1


@pytest.mark.tourillon
async def test_get_owner_and_handoff_plus_another_owner_meet_two_replica_quorum() -> (
    None
):
    addr = Address(namespace=b"ns", key=b"key")
    record = _record(addr)
    store, _, pool, _, _ = _build_store(
        node_id="node-a",
        preferences=(
            Preference("node-a", True, "node-b", writable=True),
            Preference("node-c", True, None, writable=True),
        ),
        members={
            "node-b": _member("node-b", "127.0.0.1:9001"),
            "node-c": _member("node-c", "127.0.0.1:9002"),
        },
        local_store=_StoreStub(record),
        remote_responses={"replication.read": record.to_dict()},
    )
    version = await store.get(GetRequest(namespace=b"ns", key=b"key", quorum_read=2))
    assert version is not None and version.data == record.value.data
    assert pool.acquire_calls == ["node-b", "node-c"]


@pytest.mark.tourillon
async def test_get_reports_failure_when_neither_owner_nor_handoff_responds() -> None:
    store, _, _, _, _ = _build_store(
        node_id="node-a",
        preferences=(Preference("node-b", True, "node-c", writable=True),),
        members={},
        local_store=_StoreStub(),
        remote_responses={},
    )
    with pytest.raises(StoreOperationError) as exc_info:
        await store.get(GetRequest(namespace=b"ns", key=b"key"))
    assert len(exc_info.value.to_dict()["errors"]) == 2


@pytest.mark.tourillon
@pytest.mark.parametrize("local_is_owner", [True, False])
async def test_get_uses_available_read_source_when_other_source_times_out(
    local_is_owner: bool,
) -> None:
    addr = Address(namespace=b"ns", key=b"key")
    record = _record(addr)
    owner, handoff = ("node-a", "node-b") if local_is_owner else ("node-b", "node-a")
    store, _, _, _, _ = _build_store(
        node_id="node-a",
        preferences=(Preference(owner, True, handoff, writable=True),),
        members={"node-b": _member("node-b", "127.0.0.1:9001")},
        local_store=_StoreStub(record),
        remote_responses={"replication.read": None},
        remote_delays={"replication.read": 60},
        response_delay=0,
    )
    store._response_timeout = 0.01
    version = await store.get(GetRequest(namespace=b"ns", key=b"key"))
    assert version is not None and version.data == record.value.data


@pytest.mark.tourillon
async def test_get_deduplicates_owner_and_handoff_destination() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    store, storage, _, _, _ = _build_store(
        node_id="node-a",
        preferences=(Preference("node-a", True, "node-a", writable=True),),
        members={},
        local_store=_StoreStub(_record(addr)),
        remote_responses={},
    )
    version = await store.get(GetRequest(namespace=b"ns", key=b"key"))
    assert version is not None
    assert storage.open_calls == [7]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_put_skips_unreadable_preference_without_handoff() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    remote = _record(addr, data=b"remote")
    store, storage, peer_pool, _, client = _build_store(
        node_id="node-a",
        preferences=(
            Preference(node_id="node-b", readable=False, handoff=None, writable=False),
            Preference(node_id="node-c", readable=True, handoff=None, writable=True),
        ),
        members={"node-c": _member("node-c", "127.0.0.1:9002")},
        local_store=_StoreStub(),
        remote_responses={"replication.write": remote.to_dict()},
    )

    version = await store.put(PutRequest(namespace=b"ns", key=b"key", value=b"value"))

    assert version == _version(addr, data=b"remote")
    assert storage.open_calls == []
    assert [request.kind for request in client.requests] == ["replication.write"]
    assert peer_pool.acquire_calls == ["node-c"]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_put_raises_when_remote_response_kind_is_invalid() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    record = _record(addr, data=b"remote")

    class _BadPeerClient:
        def __init__(self, serializer: MsgPackSerializer) -> None:
            self._serializer = serializer
            self.requests: list[Envelope] = []

        async def request(self, envelope: Envelope) -> Envelope:
            self.requests.append(envelope)
            return build_envelope(
                kind="replication.write.reject",
                payload=self._serializer.encode(record.to_dict()),
                schema_id=self._serializer.schema_id,
                correlation_id=envelope.correlation_id,
            )

    serializer = MsgPackSerializer()
    client = _BadPeerClient(serializer)
    peer_pool = _PeerPoolStub(
        cast(_PeerClientStub, cast(object, client)), known_peers={"node-b"}
    )
    storage = _StorageStub(_StoreStub())
    service = StorageService(
        node_id="node-a",
        store=LocalStore(
            cast(Storage, cast(object, storage)),
            cast(Partitioner, cast(object, _PartitionerStub())),
            RetentionPolicy(RetentionConfig(versions=2)),
        ),
        partitioner=cast(Partitioner, cast(object, _PartitionerStub())),
        placement_strategy=cast(
            PlacementStrategy,
            cast(
                object,
                _PlacementStub(
                    (
                        Preference(
                            node_id="node-b", readable=True, handoff=None, writable=True
                        ),
                    )
                ),
            ),
        ),
        clock=HLCClock("node-a"),
        serializer=cast(Serializer, serializer),
        peer_pool=cast(PeerPool, cast(object, peer_pool)),
    )

    with pytest.raises(
        StoreOperationError, match="write replication failed"
    ) as exc_info:
        await service.put(PutRequest(namespace=b"ns", key=b"key", value=b"value"))
    assert exc_info.value.to_dict()["errors"][0]["message"] == (
        "invalid response kind for replication.write: replication.write.reject"
    )


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_get_hides_tombstone_record() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    store, _, _, _, _ = _build_store(
        node_id="node-a",
        preferences=(
            Preference(node_id="node-a", readable=True, handoff=None, writable=True),
        ),
        members={},
        local_store=_StoreStub(_record(addr, tombstone=True)),
        remote_responses={"replication.read": None},
    )

    version = await store.get(GetRequest(namespace=b"ns", key=b"key"))

    assert version is None


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_get_forward_raises_when_peer_is_unknown() -> None:
    store, _, _, _, _ = _build_store(
        node_id="node-a",
        preferences=(
            Preference(node_id="node-b", readable=True, handoff=None, writable=True),
        ),
        members={},
        local_store=_StoreStub(),
        remote_responses={"replication.read": None},
    )

    with pytest.raises(
        StoreOperationError, match="read replication failed"
    ) as exc_info:
        await store.get(GetRequest(namespace=b"ns", key=b"key"))
    assert [error["message"] for error in exc_info.value.to_dict()["errors"]] == [
        "unknown peer: node-b"
    ]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_get_raises_when_quorum_read_not_met() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    remote = _record(addr, data=b"remote")
    store, _, _, _, _ = _build_store(
        node_id="node-a",
        preferences=(
            Preference(node_id="node-b", readable=True, handoff=None, writable=True),
            Preference(node_id="node-c", readable=True, handoff=None, writable=True),
        ),
        members={"node-b": _member("node-b", "127.0.0.1:9001")},
        local_store=_StoreStub(),
        remote_responses={"replication.read": remote.to_dict()},
    )

    with pytest.raises(StoreOperationError, match="read quorum not met") as exc_info:
        await store.get(GetRequest(namespace=b"ns", key=b"key", quorum_read=2))
    payload = exc_info.value.to_dict()
    assert payload["operation"] == "read"
    assert payload["errors"][0]["message"] == "unknown peer: node-c"


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_put_raises_when_quorum_write_not_met() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    hinted = _record(addr, data=b"hinted")
    store, _, _, _, _ = _build_store(
        node_id="node-a",
        preferences=(
            Preference(
                node_id="node-b", readable=True, handoff="node-z", writable=True
            ),
            Preference(node_id="node-c", readable=True, handoff=None, writable=True),
        ),
        members={"node-z": _member("node-z", "127.0.0.1:9009")},
        local_store=_StoreStub(),
        remote_responses={"replication.hint": hinted.to_dict()},
    )

    with pytest.raises(StoreOperationError, match="write quorum not met") as exc_info:
        await store.put(
            PutRequest(namespace=b"ns", key=b"key", value=b"value", quorum_write=2)
        )
    payload = exc_info.value.to_dict()
    assert payload["operation"] == "write"
    assert payload["errors"][0]["message"] == "unknown peer: node-c"


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_get_majority_tombstone_hides_live_version() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    deleted = _record(addr, tombstone=True, wall_ms=20)
    stale = _record(addr, data=b"stale", wall_ms=10)
    store, _, _, _, _ = _build_store(
        node_id="node-a",
        preferences=(
            Preference(node_id="node-a", readable=True, handoff=None, writable=True),
            Preference(node_id="node-b", readable=True, handoff=None, writable=True),
            Preference(node_id="node-c", readable=True, handoff=None, writable=True),
        ),
        members={
            "node-b": _member("node-b", "127.0.0.1:9001"),
            "node-c": _member("node-c", "127.0.0.1:9002"),
        },
        local_store=_StoreStub(deleted),
        remote_responses={"replication.read": [deleted.to_dict(), stale.to_dict()]},
    )

    version = await store.get(GetRequest(namespace=b"ns", key=b"key"))

    assert version is None


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_get_newer_handed_off_tombstone_hides_older_live_version() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    stale = _record(addr, data=b"stale", wall_ms=10)
    hinted_delete = _record(addr, data=b"", wall_ms=20, tombstone=True)
    store, _, _, _, _ = _build_store(
        node_id="node-a",
        preferences=(
            Preference(node_id="node-a", readable=True, handoff=None, writable=True),
            Preference(
                node_id="node-c", readable=False, handoff="node-b", writable=False
            ),
        ),
        members={"node-b": _member("node-b", "127.0.0.1:9001")},
        local_store=_StoreStub(stale),
        remote_responses={"replication.read": hinted_delete.to_dict()},
        read_repair=ReadRepairConfig(enabled=False),
    )

    version = await store.get(GetRequest(namespace=b"ns", key=b"key"))

    assert version is None


@pytest.mark.tourillon
async def test_get_returns_a_stand_in_copy_as_a_visible_version() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    held = _record(addr, data=b"held")
    store, _, peer_pool, _, _ = _build_store(
        node_id="node-a",
        preferences=(Preference("node-c", False, "node-b", writable=False),),
        members={"node-b": _member("node-b", "127.0.0.1:9001")},
        local_store=_StoreStub(),
        remote_responses={"replication.read": held.to_dict()},
    )

    version = await store.get(GetRequest(namespace=b"ns", key=b"key"))

    assert version == _version(addr, data=b"held")
    assert peer_pool.acquire_calls == ["node-b"]


@pytest.mark.tourillon
@pytest.mark.parametrize("kind", [TagKind.STALE, TagKind.PHANTOM, TagKind.RESERVED])
async def test_get_ignores_versions_that_are_not_live_or_tombstone(
    kind: TagKind,
) -> None:
    addr = Address(namespace=b"ns", key=b"key")
    plain = _record(addr)
    odd = Record(key=plain.key, value=plain.value, tag=Tag(kind=kind))
    store, *_ = _build_store(
        node_id="node-a",
        preferences=(Preference("node-a", True, None, writable=True),),
        members={},
        local_store=_StoreStub(odd),
        remote_responses={},
    )

    assert await store.get(GetRequest(namespace=b"ns", key=b"key")) is None


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_get_unknown_key_meets_read_quorum_with_empty_responses() -> None:
    store, _, _, _, _ = _build_store(
        node_id="node-a",
        preferences=(
            Preference(node_id="node-a", readable=True, handoff=None, writable=True),
            Preference(node_id="node-b", readable=True, handoff=None, writable=True),
        ),
        members={"node-b": _member("node-b", "127.0.0.1:9001")},
        local_store=_StoreStub(),
        remote_responses={"replication.read": None},
    )

    version = await store.get(GetRequest(namespace=b"ns", key=b"key", quorum_read=2))

    assert version is None


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_put_raises_when_no_write_target_available() -> None:
    store, _, _, _, client = _build_store(
        node_id="node-a",
        preferences=(
            Preference(node_id="node-b", readable=False, handoff=None, writable=False),
        ),
        members={},
        local_store=_StoreStub(),
        remote_responses={},
    )

    with pytest.raises(
        NoReplicaAvailableError, match="preference list has no replication target"
    ) as exc_info:
        await store.put(PutRequest(namespace=b"ns", key=b"key", value=b"value"))
    assert exc_info.value.to_dict()["operation"] == "write"
    assert client.requests == []


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_get_counts_empty_replica_toward_read_quorum() -> None:
    """A replica holding nothing has answered, so it counts toward the quorum.

    This is the steady state of any key written while one replica was down:
    treating "I hold nothing" as a non-answer would make such keys unreadable
    at full quorum even though every replica is reachable.
    """
    addr = Address(namespace=b"ns", key=b"key")
    store, _, _, _, _ = _build_store(
        node_id="node-a",
        preferences=(
            Preference(node_id="node-a", readable=True, handoff=None, writable=True),
            Preference(node_id="node-b", readable=True, handoff=None, writable=True),
        ),
        members={"node-b": _member("node-b", "127.0.0.1:9001")},
        local_store=_StoreStub(),
        remote_responses={
            "replication.read": _record(addr, data=b"v2", wall_ms=9).to_dict()
        },
    )

    version = await store.get(GetRequest(namespace=b"ns", key=b"key", quorum_read=2))

    assert version == _version(addr, data=b"v2", wall_ms=9)


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_get_counts_disagreeing_replicas_toward_read_quorum() -> None:
    """Disagreement is not absence: both replicas answered, so the quorum holds.

    Requiring the replicas to agree would conflate "the replica set is
    reachable" with "the replica set is converged", and would reject reads
    precisely when read repair is needed to converge them.
    """
    addr = Address(namespace=b"ns", key=b"key")
    store, _, _, _, _ = _build_store(
        node_id="node-a",
        preferences=(
            Preference(node_id="node-a", readable=True, handoff=None, writable=True),
            Preference(node_id="node-b", readable=True, handoff=None, writable=True),
        ),
        members={"node-b": _member("node-b", "127.0.0.1:9001")},
        local_store=_StoreStub(_record(addr, data=b"v1", wall_ms=3)),
        remote_responses={
            "replication.read": _record(addr, data=b"v2", wall_ms=9).to_dict()
        },
    )

    version = await store.get(GetRequest(namespace=b"ns", key=b"key", quorum_read=2))

    assert version == _version(addr, data=b"v2", wall_ms=9)


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_get_reports_unanswered_replica_as_timeout_error() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    record = _record(addr, data=b"v1")
    store, _, _, _, _ = _build_store(
        node_id="node-a",
        preferences=(
            Preference(node_id="node-a", readable=True, handoff=None, writable=True),
            Preference(node_id="node-b", readable=True, handoff=None, writable=True),
        ),
        members={"node-b": _member("node-b", "127.0.0.1:9001")},
        local_store=_StoreStub(record),
        remote_responses={"replication.read": record.to_dict()},
        remote_delays={"replication.read": 0.5},
        response_delay=0.01,
    )
    store._response_timeout = 0.05  # noqa: SLF001

    with pytest.raises(QuorumNotMetError, match="read quorum not met") as exc_info:
        await store.get(GetRequest(namespace=b"ns", key=b"key", quorum_read=2))
    errors = exc_info.value.to_dict()["errors"]
    assert errors == [
        {"type": "TimeoutError", "message": "no response from node-b within 0.05s"}
    ]


@pytest.mark.tourillon
@pytest.mark.parametrize("operation", ["put", "delete"])
async def test_writes_refuse_readable_but_unwritable_owner_without_handoff(
    operation: str,
) -> None:
    addr = Address(namespace=b"ns", key=b"key")
    record = _record(addr)
    store, storage, _, _, client = _build_store(
        node_id="node-a",
        preferences=(Preference("node-a", True, None, writable=False),),
        members={},
        local_store=_StoreStub(record),
        remote_responses={},
    )
    with pytest.raises(NoReplicaAvailableError):
        if operation == "put":
            await store.put(PutRequest(namespace=b"ns", key=b"key", value=b"value"))
        else:
            await store.delete(DeleteRequest(namespace=b"ns", key=b"key"))
    assert client.requests == []
    assert storage.open_calls == []


@pytest.mark.tourillon
async def test_put_uses_writable_owner_independently_of_readability() -> None:
    store, _, _, _, _ = _build_store(
        node_id="node-a",
        preferences=(Preference("node-a", False, None, writable=True),),
        members={},
        local_store=_StoreStub(),
        remote_responses={},
    )
    version = await store.put(PutRequest(namespace=b"ns", key=b"key", value=b"written"))
    assert version is not None and version.data == b"written"


@pytest.mark.tourillon
async def test_put_uses_handoff_for_unwritable_owner() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    held = _record(addr)
    store, storage, pool, serializer, client = _build_store(
        node_id="node-a",
        preferences=(Preference("node-a", True, "node-b", writable=False),),
        members={"node-b": _member("node-b", "127.0.0.1:9001")},
        local_store=_StoreStub(),
        remote_responses={"replication.hint": held.to_dict()},
    )
    version = await store.put(PutRequest(namespace=b"ns", key=b"key", value=b"value"))
    assert version is not None
    assert storage.open_calls == []
    assert pool.acquire_calls == ["node-b"]
    payload = serializer.decode(client.requests[0].payload)
    sent = Record.from_dict(payload["record"])
    assert sent.tag == Tag(kind=TagKind.LIVE)
    assert payload["intent"] == "node-a"
    assert client.requests[0].kind == "replication.hint"


@pytest.mark.tourillon
async def test_delete_hands_off_a_plain_tombstone() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    held = _record(addr, data=b"", tombstone=True)
    store, _, _, serializer, client = _build_store(
        node_id="node-a",
        preferences=(Preference("node-c", False, "node-b", writable=False),),
        members={"node-b": _member("node-b", "127.0.0.1:9001")},
        local_store=_StoreStub(),
        remote_responses={"replication.hint": held.to_dict()},
    )
    await store.delete(DeleteRequest(namespace=b"ns", key=b"key"))
    payload = serializer.decode(client.requests[0].payload)
    assert client.requests[0].kind == "replication.hint"
    assert Record.from_dict(payload["record"]).tag == Tag(kind=TagKind.TOMBSTONE)
    assert payload["intent"] == "node-c"


@pytest.mark.tourillon
async def test_explicit_repair_retains_readable_owner_routing() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    record = _record(addr)
    store, _, _, _, client = _build_store(
        node_id="node-a",
        preferences=(
            Preference("node-a", True, None, writable=True),
            Preference("node-b", True, None, writable=False),
        ),
        members={"node-b": _member("node-b", "127.0.0.1:9001")},
        local_store=_StoreStub(record),
        remote_responses={
            "replication.lookup": None,
            "replication.write": record.to_dict(),
        },
    )
    result = await store.repair(
        RepairRequest(token=_repair_token(record), quorum_write=2)
    )
    assert result.issue == RepairIssue.REPAIRED
    assert [request.kind for request in client.requests] == [
        "replication.lookup",
        "replication.write",
    ]


@pytest.mark.tourillon
async def test_read_repair_retains_readable_owner_routing() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    winner = _record(addr, wall_ms=20)
    stale = _record(addr, wall_ms=10)
    store, storage, _, _, client = _build_store(
        node_id="node-a",
        preferences=(
            Preference("node-a", True, None, writable=True),
            Preference("node-b", True, None, writable=False),
        ),
        members={"node-b": _member("node-b", "127.0.0.1:9001")},
        local_store=_StoreStub(winner),
        remote_responses={"replication.read": stale.to_dict()},
    )
    version = await store.get(GetRequest(namespace=b"ns", key=b"key", quorum_read=2))
    await store.close()
    assert version is not None and version.meta.ts == winner.key.ts
    assert [request.kind for request in client.requests] == ["replication.read"]
    assert _obligations(storage) == [ObligationKey("node-b", winner.key)]
    assert store.stats.read_repairs_repaired == 1


class _HintRejectingStore(_StoreStub):
    def on_put(self, kv: KVPair) -> None:
        if kv.keyspace == Keyspace.META and kv.key.startswith(
            MetaKind.OBLIGATION.value
        ):
            raise RuntimeError("no space left on device")
        super().on_put(kv)


class _FailingStore(_StoreStub):
    def on_put(self, kv: KVPair) -> None:
        raise RuntimeError("no space left on device")


def _obligations(storage: _StorageStub) -> list[ObligationKey]:
    prefix = ObligationPrefix().to_bytes()
    return [
        ObligationKey.from_bytes(kv.key)
        for kv in storage.store.written
        if kv.keyspace == Keyspace.META and kv.key.startswith(prefix)
    ]


@pytest.mark.tourillon
@pytest.mark.parametrize("tombstone", [False, True])
async def test_incoming_hint_stores_plain_record_and_distinct_obligations(
    tombstone: bool,
) -> None:
    record = _record(Address(namespace=b"ns", key=b"key"), tombstone=tombstone)
    store, storage, _, _, client = _build_store(
        node_id="keeper",
        preferences=(),
        members={},
        local_store=_StoreStub(),
        remote_responses={},
    )
    assert await store.hint(record, "node-b") == record
    assert await store.hint(record, "node-c") == record
    assert _obligations(storage) == [
        ObligationKey("node-b", record.key),
        ObligationKey("node-c", record.key),
    ]
    metas = [
        kv
        for kv in storage.store.written
        if kv.key.startswith(ObligationPrefix().to_bytes())
    ]
    assert all(kv.value == b"" for kv in metas)
    assert storage.store.record == record
    assert store.stats.obligations_recorded == 2
    assert client.requests == []


@pytest.mark.tourillon
@pytest.mark.parametrize("intent", ["", "x" * 64, "é" * 32])
async def test_incoming_hint_rejects_invalid_intent_without_persisting(
    intent: str,
) -> None:
    store, storage, _, _, _ = _build_store(
        node_id="keeper",
        preferences=(),
        members={},
        local_store=_StoreStub(),
        remote_responses={},
    )
    with pytest.raises(ValueError):
        await store.hint(_record(Address(namespace=b"ns", key=b"key")), intent)
    assert storage.store.written == []
    assert store.stats.obligations_recorded == 0


@pytest.mark.tourillon
async def test_incoming_hint_is_refused_while_draining() -> None:
    store, storage, _, _, _ = _build_store(
        node_id="keeper",
        preferences=(),
        members={},
        local_store=_StoreStub(),
        remote_responses={},
    )
    store.pause()
    with pytest.raises(NodeDrainingError) as exc:
        await store.hint(_record(Address(namespace=b"ns", key=b"key")), "owner")
    assert exc.value.operation == "replication.hint"
    assert storage.store.written == []


@pytest.mark.tourillon
@pytest.mark.parametrize("kind", [TagKind.STALE, TagKind.PHANTOM, TagKind.META])
async def test_hint_and_write_require_live_or_tombstone_records(
    kind: TagKind,
) -> None:
    plain = _record(Address(namespace=b"ns", key=b"key"))
    odd = Record(key=plain.key, value=plain.value, tag=Tag(kind=kind))
    store, storage, _, _, _ = _build_store(
        node_id="keeper",
        preferences=(),
        members={},
        local_store=_StoreStub(),
        remote_responses={},
    )
    with pytest.raises(ValueError, match="LIVE or TOMBSTONE"):
        await store.hint(odd, "owner")
    with pytest.raises(ValueError, match="LIVE or TOMBSTONE"):
        await store.write(odd)
    assert storage.store.written == []
    assert store.stats.obligations_recorded == 0


@pytest.mark.tourillon
async def test_incoming_hint_submits_obligations_to_the_queue() -> None:
    queue = ObligationQueue(8)
    record = _record(Address(namespace=b"ns", key=b"key"))
    store, storage, _, _, _ = _build_store(
        node_id="keeper",
        preferences=(),
        members={},
        local_store=_StoreStub(),
        remote_responses={},
        obligations=queue,
    )
    await store.hint(record, "owner")
    assert queue.to_dict()["queued"] == 1
    assert queue.take(1) == [ObligationKey("owner", record.key)]
    assert _obligations(storage) == [ObligationKey("owner", record.key)]


@pytest.mark.tourillon
async def test_hint_on_behalf_of_this_node_records_no_obligation() -> None:
    queue = ObligationQueue(8)
    record = _record(Address(namespace=b"ns", key=b"key"))
    store, storage, _, _, _ = _build_store(
        node_id="keeper",
        preferences=(),
        members={},
        local_store=_StoreStub(),
        remote_responses={},
        obligations=queue,
    )
    assert await store.hint(record, "keeper") == record
    assert storage.store.record == record
    assert _obligations(storage) == []
    assert store.stats.obligations_recorded == 0
    assert queue.to_dict()["queued"] == 0


@pytest.mark.tourillon
async def test_incoming_hint_storage_failure_records_nothing() -> None:
    queue = ObligationQueue(8)
    store, _, _, _, _ = _build_store(
        node_id="keeper",
        preferences=(),
        members={},
        local_store=_HintRejectingStore(),
        remote_responses={},
        obligations=queue,
    )
    with pytest.raises(RuntimeError, match="no space left"):
        await store.hint(_record(Address(namespace=b"ns", key=b"key")), "owner")
    assert store.stats.obligations_recorded == 0
    assert queue.to_dict()["queued"] == 0


@pytest.mark.tourillon
async def test_local_handoff_persists_obligation_without_network() -> None:
    store, storage, _, _, client = _build_store(
        node_id="keeper",
        preferences=(Preference("owner", False, "keeper", False),),
        members={},
        local_store=_StoreStub(),
        remote_responses={},
    )
    await store.put(PutRequest(namespace=b"ns", key=b"key", value=b"value"))
    obligations = _obligations(storage)
    assert [obligation.target for obligation in obligations] == ["owner"]
    assert obligations[0].key == storage.store.record.key
    assert storage.store.record.tag.kind is TagKind.LIVE
    assert client.requests == []


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_failed_replica_write_records_local_obligation() -> None:
    """A replica believed healthy that misses a write must leave a trace.

    Placement only hands off for replicas already known to be down, so a write
    that fails against a supposedly healthy one is the case nothing else
    catches: without a local obligation the replica stays silently behind
    until the next anti-entropy pass.
    """
    queue = ObligationQueue(8)
    telemetry = RecordingTelemetry()
    store, storage, _, _, _ = _build_store(
        node_id="node-a",
        preferences=(
            Preference(node_id="node-a", readable=True, handoff=None, writable=True),
            Preference(node_id="node-b", readable=True, handoff=None, writable=True),
        ),
        members={},
        local_store=_StoreStub(),
        remote_responses={},
        obligations=queue,
        telemetry=telemetry,
    )

    await store.put(PutRequest(namespace=b"ns", key=b"key", value=b"value"))

    assert storage.store.record.tag.kind is TagKind.LIVE
    assert _obligations(storage) == [ObligationKey("node-b", storage.store.record.key)]
    assert store.stats.obligations_recorded == 1
    assert queue.to_dict()["queued"] == 1
    assert (
        "storage.obligations.put",
        {},
    ) in telemetry.spans


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_local_write_failure_does_not_record_an_obligation_for_this_node() -> (
    None
):
    """There is nobody to owe a copy to when this node is the failing target."""
    store, storage, _, _, _ = _build_store(
        node_id="node-a",
        preferences=(
            Preference(node_id="node-a", readable=True, handoff=None, writable=True),
        ),
        members={},
        local_store=_FailingStore(),
        remote_responses={},
    )

    with pytest.raises(StoreOperationError):
        await store.put(PutRequest(namespace=b"ns", key=b"key", value=b"value"))

    assert _obligations(storage) == []
    assert store.stats.obligations_recorded == 0


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_obligation_write_failure_does_not_mask_the_replication_error() -> None:
    """The client must learn why replication failed, not why the obligation did."""
    store, storage, _, _, _ = _build_store(
        node_id="node-a",
        preferences=(
            Preference(node_id="node-a", readable=True, handoff=None, writable=True),
            Preference(node_id="node-b", readable=True, handoff=None, writable=True),
        ),
        members={},
        local_store=_HintRejectingStore(),
        remote_responses={},
    )

    with pytest.raises(QuorumNotMetError) as exc_info:
        await store.put(
            PutRequest(namespace=b"ns", key=b"key", value=b"value", quorum_write=2)
        )

    messages = [error["message"] for error in exc_info.value.to_dict()["errors"]]
    assert any("unknown peer: node-b" in message for message in messages)
    assert not any("no space left on device" in message for message in messages)
    assert _obligations(storage) == []
    assert store.stats.obligations_recorded == 0


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_slow_replica_write_is_detached_rather_than_cancelled() -> None:
    """A late write may still land; cancelling it would guarantee it never does.

    Reads are cancelled because a late answer is worthless, but a write that is
    merely slow still carries durability. Dropping it would turn a slow replica
    into one that permanently missed the write.
    """
    addr = Address(namespace=b"ns", key=b"key")
    store, storage, _, _, _ = _build_store(
        node_id="node-a",
        preferences=(
            Preference(node_id="node-a", readable=True, handoff=None, writable=True),
            Preference(node_id="node-b", readable=True, handoff=None, writable=True),
        ),
        members={"node-b": _member("node-b", "127.0.0.1:9001")},
        local_store=_StoreStub(),
        remote_responses={"replication.write": _record(addr, data=b"value").to_dict()},
        remote_delays={"replication.write": 0.2},
        response_delay=0.01,
    )
    store._response_timeout = 0.05  # noqa: SLF001

    await store.put(PutRequest(namespace=b"ns", key=b"key", value=b"value"))

    assert len(store._pending_writes) == 1  # noqa: SLF001
    detached = next(iter(store._pending_writes))  # noqa: SLF001

    await store.close()

    assert detached.cancelled() is False
    assert detached.exception() is None
    assert store._pending_writes == set()  # noqa: SLF001
    assert _obligations(storage) == []


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_close_is_a_no_op_without_detached_writes() -> None:
    store, _, _, _, _ = _build_store(
        node_id="node-a",
        preferences=(
            Preference(node_id="node-a", readable=True, handoff=None, writable=True),
        ),
        members={},
        local_store=_StoreStub(),
        remote_responses={},
    )

    await store.close()

    assert store._pending_writes == set()  # noqa: SLF001


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_put_persists_the_requested_write_quorum() -> None:
    """The durability asked for is a property of the version, not a constant.

    It is recorded even when the quorum is not met: the local write is durable
    and a later repair needs to know how durable it was meant to be.
    """
    addr = Address(namespace=b"ns", key=b"key")
    local = _record(addr)
    local_store = _StoreStub(local)
    store, _, _, _, _ = _build_store(
        node_id="node-a",
        preferences=(
            Preference(node_id="node-a", readable=True, handoff=None, writable=True),
        ),
        members={},
        local_store=local_store,
        remote_responses={},
    )

    with pytest.raises(QuorumNotMetError):
        await store.put(
            PutRequest(namespace=b"ns", key=b"key", value=b"value", quorum_write=3)
        )

    logs = [kv for kv in local_store.written if kv.keyspace == Keyspace.LOGS]
    assert [Value.from_bytes(kv.value).quorum_write for kv in logs] == [3]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_delete_persists_the_requested_write_quorum() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    local = _record(addr, data=b"", tombstone=True)
    store, _, _, _, _ = _build_store(
        node_id="node-a",
        preferences=(
            Preference(node_id="node-a", readable=True, handoff=None, writable=True),
        ),
        members={},
        local_store=_StoreStub(local),
        remote_responses={},
    )

    version = await store.delete(
        DeleteRequest(namespace=b"ns", key=b"key", quorum_write=1)
    )

    assert version is not None
    assert version.meta.quorum_write == 1


@pytest.mark.tourillon
@pytest.mark.asyncio
@pytest.mark.parametrize("quorum", [0, -1, MAX_QUORUM + 1])
async def test_put_refuses_a_quorum_the_store_cannot_record(quorum: int) -> None:
    """A quorum outside the storable range is refused before anything is written."""
    addr = Address(namespace=b"ns", key=b"key")
    local_store = _StoreStub(_record(addr))
    store, _, _, _, _ = _build_store(
        node_id="node-a",
        preferences=(
            Preference(node_id="node-a", readable=True, handoff=None, writable=True),
        ),
        members={},
        local_store=local_store,
        remote_responses={},
    )

    with pytest.raises(InvalidQuorumError) as exc_info:
        await store.put(
            PutRequest(namespace=b"ns", key=b"key", value=b"v", quorum_write=quorum)
        )

    assert exc_info.value.to_dict()["code"] == "invalid_quorum"
    assert exc_info.value.to_dict()["quorum"] == quorum
    assert local_store.written == []


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_delete_refuses_a_quorum_the_store_cannot_record() -> None:
    store, _, _, _, _ = _build_store(
        node_id="node-a",
        preferences=(
            Preference(node_id="node-a", readable=True, handoff=None, writable=True),
        ),
        members={},
        local_store=_StoreStub(),
        remote_responses={},
    )

    with pytest.raises(InvalidQuorumError):
        await store.delete(DeleteRequest(namespace=b"ns", key=b"key", quorum_write=0))


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_get_refuses_a_quorum_the_store_cannot_record() -> None:
    store, _, _, _, _ = _build_store(
        node_id="node-a",
        preferences=(
            Preference(node_id="node-a", readable=True, handoff=None, writable=True),
        ),
        members={},
        local_store=_StoreStub(),
        remote_responses={},
    )

    with pytest.raises(InvalidQuorumError):
        await store.get(
            GetRequest(namespace=b"ns", key=b"key", quorum_read=MAX_QUORUM + 1)
        )


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_replicated_write_with_a_consistent_pid_is_not_flagged() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    local_store = _StoreStub()
    store, _, _, _, _ = _build_store(
        node_id="node-a",
        preferences=(
            Preference(node_id="node-a", readable=True, handoff=None, writable=True),
        ),
        members={},
        local_store=local_store,
        remote_responses={},
        partitioner_pid=7,
    )

    applied = await store.write(_record(addr, pid=7))

    assert applied.key.pid.value == 7
    assert store.stats.foreign_writes == 0


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_replicated_write_with_a_foreign_pid_is_traced_but_applied() -> None:
    """A record whose pid disagrees with the local ring lands in a group that
    reads for this address never consult. Refusing it would lose a write the
    sender believes is durable, so it is applied and counted instead.
    """
    addr = Address(namespace=b"ns", key=b"key")
    local_store = _StoreStub()
    store, _, _, _, _ = _build_store(
        node_id="node-a",
        preferences=(
            Preference(node_id="node-a", readable=True, handoff=None, writable=True),
        ),
        members={},
        local_store=local_store,
        remote_responses={},
        partitioner_pid=9,
    )

    applied = await store.write(_record(addr, pid=7))

    assert applied.key.pid.value == 7
    assert store.stats.foreign_writes == 1
    assert local_store.written != []


def _two_owners() -> tuple[Preference, ...]:
    return (
        Preference(node_id="node-a", readable=True, handoff=None, writable=True),
        Preference(node_id="node-b", readable=True, handoff=None, writable=True),
    )


def _sent_record(envelope: Envelope) -> Any:
    return MsgPackSerializer().decode(envelope.payload)["record"]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_read_repair_catches_up_a_stale_replica() -> None:
    """The coordinator holds the winner: it owes the laggard a copy itself."""
    addr = Address(namespace=b"ns", key=b"key")
    fresh = _record(addr, data=b"v2", wall_ms=9)
    stale = _record(addr, data=b"v1", wall_ms=3)
    store, storage, _, _, client = _build_store(
        node_id="node-a",
        preferences=_two_owners(),
        members={"node-b": _member("node-b", "127.0.0.1:9001")},
        local_store=_StoreStub(fresh),
        remote_responses={"replication.read": stale.to_dict()},
    )

    version = await store.get(GetRequest(namespace=b"ns", key=b"key"))
    await store.close()

    assert version == _version(addr, data=b"v2", wall_ms=9)
    assert [request.kind for request in client.requests] == ["replication.read"]
    assert _obligations(storage) == [ObligationKey("node-b", fresh.key)]
    assert store.stats.obligations_recorded == 1
    assert store.stats.read_repairs_repaired == 1


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_read_repair_catches_up_a_replica_holding_nothing() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    fresh = _record(addr, data=b"v2", wall_ms=9)
    store, storage, _, _, client = _build_store(
        node_id="node-a",
        preferences=_two_owners(),
        members={"node-b": _member("node-b", "127.0.0.1:9001")},
        local_store=_StoreStub(fresh),
        remote_responses={"replication.read": None},
    )

    await store.get(GetRequest(namespace=b"ns", key=b"key"))
    await store.close()

    assert [request.kind for request in client.requests] == ["replication.read"]
    assert _obligations(storage) == [ObligationKey("node-b", fresh.key)]
    assert store.stats.read_repairs_repaired == 1


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_read_repair_asks_the_remote_holder_to_owe_the_laggard() -> None:
    """The coordinator lags: the replica holding the winner records the debt."""
    addr = Address(namespace=b"ns", key=b"key")
    fresh = _record(addr, data=b"v2", wall_ms=9)
    stale = _record(addr, data=b"v1", wall_ms=3)
    store, storage, _, serializer, client = _build_store(
        node_id="node-a",
        preferences=_two_owners(),
        members={"node-b": _member("node-b", "127.0.0.1:9001")},
        local_store=_StoreStub(stale),
        remote_responses={
            "replication.read": fresh.to_dict(),
            "replication.hint": fresh.to_dict(),
        },
    )

    version = await store.get(GetRequest(namespace=b"ns", key=b"key"))
    await store.close()

    assert version == _version(addr, data=b"v2", wall_ms=9)
    assert [request.kind for request in client.requests] == [
        "replication.read",
        "replication.hint",
    ]
    payload = serializer.decode(client.requests[1].payload)
    assert payload["intent"] == "node-a"
    assert Record.from_dict(payload["record"]) == fresh
    assert _obligations(storage) == []
    assert storage.store.record == stale
    assert store.stats.read_repairs_repaired == 1
    assert store.stats.read_repairs_failed == 0


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_read_repair_picks_the_first_remote_holder_in_order() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    fresh = _record(addr, data=b"v2", wall_ms=9)
    store, _, pool, serializer, client = _build_store(
        node_id="node-a",
        preferences=(
            Preference("node-a", True, None, writable=True),
            Preference("node-c", True, None, writable=True),
            Preference("node-b", True, None, writable=True),
        ),
        members={
            "node-b": _member("node-b", "127.0.0.1:9001"),
            "node-c": _member("node-c", "127.0.0.1:9002"),
        },
        local_store=_StoreStub(),
        remote_responses={
            "replication.read": [fresh.to_dict(), fresh.to_dict()],
            "replication.hint": fresh.to_dict(),
        },
    )

    await store.get(GetRequest(namespace=b"ns", key=b"key"))
    await store.close()

    hints = [
        request for request in client.requests if request.kind != "replication.read"
    ]
    assert [request.kind for request in hints] == ["replication.hint"]
    assert serializer.decode(hints[0].payload)["intent"] == "node-a"
    assert pool.acquire_calls[-1] == "node-b"


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_read_repair_leaves_an_up_to_date_replica_alone() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    fresh = _record(addr, data=b"v2", wall_ms=9)
    store, storage, _, _, client = _build_store(
        node_id="node-a",
        preferences=_two_owners(),
        members={"node-b": _member("node-b", "127.0.0.1:9001")},
        local_store=_StoreStub(fresh),
        remote_responses={"replication.read": fresh.to_dict()},
    )

    await store.get(GetRequest(namespace=b"ns", key=b"key"))
    await store.close()

    assert [request.kind for request in client.requests] == ["replication.read"]
    assert _obligations(storage) == []
    assert store.stats.read_repairs_repaired == 0


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_read_repair_repairs_a_missed_delete() -> None:
    """A replica that missed the delete must receive the tombstone.

    Leaving it behind would let it re-propose the old value on every later
    read, so repair would resurrect the deleted key forever.
    """
    addr = Address(namespace=b"ns", key=b"key")
    tombstone = _record(addr, data=b"", tombstone=True, wall_ms=9)
    stale = _record(addr, data=b"v1", wall_ms=3)
    store, storage, _, _, client = _build_store(
        node_id="node-a",
        preferences=_two_owners(),
        members={"node-b": _member("node-b", "127.0.0.1:9001")},
        local_store=_StoreStub(tombstone),
        remote_responses={"replication.read": stale.to_dict()},
    )

    version = await store.get(GetRequest(namespace=b"ns", key=b"key"))
    await store.close()

    assert version is None
    assert [request.kind for request in client.requests] == ["replication.read"]
    assert _obligations(storage) == [ObligationKey("node-b", tombstone.key)]
    assert storage.store.record.tag.kind is TagKind.TOMBSTONE
    assert store.stats.read_repairs_repaired == 1


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_read_repair_skips_a_handoff_stand_in() -> None:
    """A stand-in does not own the address; its obligation settles that debt."""
    addr = Address(namespace=b"ns", key=b"key")
    fresh = _record(addr, data=b"v2", wall_ms=9)
    store, _, _, _, client = _build_store(
        node_id="node-a",
        preferences=(
            Preference(node_id="node-a", readable=True, handoff=None, writable=True),
            Preference(
                node_id="node-c", readable=False, handoff="node-b", writable=False
            ),
        ),
        members={"node-b": _member("node-b", "127.0.0.1:9001")},
        local_store=_StoreStub(fresh),
        remote_responses={"replication.read": None},
    )

    await store.get(GetRequest(namespace=b"ns", key=b"key"))
    await store.close()

    assert [request.kind for request in client.requests] == ["replication.read"]
    assert store.stats.read_repairs_repaired == 0


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_read_repair_is_not_scheduled_when_disabled() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    fresh = _record(addr, data=b"v2", wall_ms=9)
    store, _, _, _, client = _build_store(
        node_id="node-a",
        preferences=_two_owners(),
        members={"node-b": _member("node-b", "127.0.0.1:9001")},
        local_store=_StoreStub(fresh),
        remote_responses={"replication.read": None},
        read_repair=ReadRepairConfig(enabled=False),
    )

    await store.get(GetRequest(namespace=b"ns", key=b"key"))
    await store.close()

    assert [request.kind for request in client.requests] == ["replication.read"]
    assert store.stats.read_repairs_repaired == 0


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_read_repair_sampling_counts_what_it_declines_to_repair() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    fresh = _record(addr, data=b"v2", wall_ms=9)
    store, _, _, _, client = _build_store(
        node_id="node-a",
        preferences=_two_owners(),
        members={"node-b": _member("node-b", "127.0.0.1:9001")},
        local_store=_StoreStub(fresh),
        remote_responses={"replication.read": None},
        read_repair=ReadRepairConfig(sample_rate=0.0),
    )

    await store.get(GetRequest(namespace=b"ns", key=b"key"))
    await store.close()

    assert [request.kind for request in client.requests] == ["replication.read"]
    assert store.stats.read_repairs_skipped == 1
    assert store.stats.read_repairs_repaired == 0


@pytest.mark.tourillon
@pytest.mark.asyncio
@pytest.mark.parametrize("draining", [False, True])
async def test_read_repair_failure_is_counted_not_raised(draining: bool) -> None:
    """The read already succeeded; a failed repair must not reach the client."""
    addr = Address(namespace=b"ns", key=b"key")
    fresh = _record(addr, data=b"v2", wall_ms=9)
    store, storage, _, _, client = _build_store(
        node_id="node-a",
        preferences=_two_owners(),
        members={"node-b": _member("node-b", "127.0.0.1:9001")},
        local_store=_StoreStub(),
        remote_responses={"replication.read": fresh.to_dict()},
    )
    if draining:
        store.pause()

    version = await store.get(GetRequest(namespace=b"ns", key=b"key"))
    await store.close()

    assert version == _version(addr, data=b"v2", wall_ms=9)
    assert [request.kind for request in client.requests] == [
        "replication.read",
        "replication.hint",
    ]
    assert _obligations(storage) == []
    assert store.stats.read_repairs_failed == 1
    assert store.stats.read_repairs_repaired == 0


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_read_repair_local_obligation_failure_is_counted_not_raised() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    fresh = _record(addr, data=b"v2", wall_ms=9)
    store, _, _, _, _ = _build_store(
        node_id="node-a",
        preferences=_two_owners(),
        members={"node-b": _member("node-b", "127.0.0.1:9001")},
        local_store=_HintRejectingStore(fresh),
        remote_responses={"replication.read": None},
    )

    version = await store.get(GetRequest(namespace=b"ns", key=b"key"))
    await store.close()

    assert version == _version(addr, data=b"v2", wall_ms=9)
    assert store.stats.read_repairs_failed == 1
    assert store.stats.obligations_recorded == 0


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_concurrent_reads_do_not_stack_repairs_for_the_same_key() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    fresh = _record(addr, data=b"v2", wall_ms=9)
    store, _, _, _, client = _build_store(
        node_id="node-a",
        preferences=_two_owners(),
        members={
            "node-b": _member("node-b", "127.0.0.1:9001"),
            "node-c": _member("node-c", "127.0.0.1:9002"),
        },
        local_store=_StoreStub(fresh),
        remote_responses={
            "replication.read": None,
            "replication.hint": fresh.to_dict(),
        },
        remote_delays={"replication.hint": 0.05},
    )

    store._start_repair("node-b", fresh, "node-c")
    store._start_repair("node-b", fresh, "node-c")
    await store.close()

    assert [request.kind for request in client.requests] == ["replication.hint"]
    assert store.stats.read_repairs_repaired == 1


def _repair_token(record: Record) -> str:
    return encode_token(record.key)


@pytest.mark.tourillon
@pytest.mark.parametrize("operation", ["put", "delete", "repair", "write"])
async def test_draining_rejects_new_work(operation: str) -> None:
    store, _, _, _, client = _build_store(
        node_id="node-a",
        preferences=_two_owners(),
        members={},
        local_store=_StoreStub(),
        remote_responses={},
    )
    record = _record(Address(namespace=b"ns", key=b"key"))
    requests = {
        "get": GetRequest(namespace=b"ns", key=b"key"),
        "put": PutRequest(namespace=b"ns", key=b"key", value=b"value"),
        "delete": DeleteRequest(namespace=b"ns", key=b"key"),
        "repair": RepairRequest(token=_repair_token(record)),
        "write": record,
    }
    store.pause()
    with pytest.raises(NodeDrainingError, match="node is draining"):
        await getattr(store, operation)(requests[operation])
    assert not client.requests


class _BlockedRepairClient(_PeerClientStub):
    def __init__(self, serializer: MsgPackSerializer, fresh: Record) -> None:
        super().__init__(
            serializer,
            {
                "replication.read": None,
                "replication.hint": fresh.to_dict(),
            },
        )
        self.read_started = asyncio.Event()
        self.read_allowed = asyncio.Event()
        self.repair_started = asyncio.Event()
        self.repair_allowed = asyncio.Event()

    async def request(self, envelope: Envelope) -> Envelope:
        if envelope.kind == "replication.read":
            self.read_started.set()
            await self.read_allowed.wait()
        else:
            self.repair_started.set()
            await self.repair_allowed.wait()
        return await super().request(envelope)


class _BlockedWriteClient(_BlockedRepairClient):
    def __init__(
        self, serializer: MsgPackSerializer, record: Record, fail: bool
    ) -> None:
        super().__init__(serializer, record)
        self.fail = fail

    async def request(self, envelope: Envelope) -> Envelope:
        self.repair_started.set()
        await self.repair_allowed.wait()
        if self.fail:
            raise ConnectionError("write unavailable")
        self._responses[envelope.kind] = self._serializer.decode(envelope.payload)[
            "record"
        ]
        return await super().request(envelope)


@pytest.mark.tourillon
async def test_drain_keeps_reads_and_read_repairs_without_waiting_for_reads() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    fresh = _record(addr, data=b"v2", wall_ms=9)
    store, _, pool, serializer, _ = _build_store(
        node_id="node-a",
        preferences=_two_owners(),
        members={
            "node-b": _member("node-b", "127.0.0.1:9001"),
            "node-c": _member("node-c", "127.0.0.1:9002"),
        },
        local_store=_StoreStub(fresh),
        remote_responses={},
    )
    client = _BlockedRepairClient(serializer, fresh)
    pool._client = client
    store._start_repair("node-b", fresh, "node-c")
    await asyncio.wait_for(client.repair_started.wait(), 1)
    read = asyncio.create_task(
        store.get(GetRequest(namespace=b"ns", key=b"key", quorum_read=2))
    )
    await asyncio.wait_for(client.read_started.wait(), 1)
    draining = asyncio.create_task(store.drain())
    try:
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(asyncio.shield(draining), 0.01)
        assert not draining.done()
        client.repair_allowed.set()
        await asyncio.wait_for(draining, 1)
        assert not read.done()
        client.read_allowed.set()
        await asyncio.wait_for(read, 1)
        await store.close()
        assert store.stats.read_repairs_repaired == 2
        assert await store.get(GetRequest(namespace=b"ns", key=b"key")) is not None
        await store.close()
        assert store.stats.read_repairs_repaired == 3
        assert store.stats.read_repairs_skipped == 0
        await store.stop_read_repairs()
        assert await store.get(GetRequest(namespace=b"ns", key=b"key")) is not None
        assert store.stats.read_repairs_repaired == 3
        assert store.stats.read_repairs_skipped == 1
        store.pause(readable=False)
        with pytest.raises(NodeDrainingError):
            await store.get(GetRequest(namespace=b"ns", key=b"key"))
        store.resume()
        assert await store.get(GetRequest(namespace=b"ns", key=b"key")) is not None
        await store.close()
        assert store.stats.read_repairs_repaired == 4
    finally:
        client.read_allowed.set()
        client.repair_allowed.set()
        await asyncio.gather(read, draining)
        await store.close()


@pytest.mark.tourillon
@pytest.mark.parametrize("fail", [False, True])
async def test_drain_waits_for_admitted_write_without_cancelling_it(fail: bool) -> None:
    addr = Address(namespace=b"ns", key=b"key")
    record = _record(addr)
    store, _, pool, serializer, _ = _build_store(
        node_id="node-a",
        preferences=_two_owners(),
        members={"node-b": _member("node-b", "127.0.0.1:9001")},
        local_store=_StoreStub(),
        remote_responses={},
    )
    client = _BlockedWriteClient(serializer, record, fail)
    pool._client = client
    write = asyncio.create_task(
        store.put(
            PutRequest(namespace=b"ns", key=b"key", value=b"value", quorum_write=2)
        )
    )
    await asyncio.wait_for(client.repair_started.wait(), 1)
    draining = asyncio.create_task(store.drain())
    try:
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(asyncio.shield(draining), 0.01)
        draining.cancel()
        with pytest.raises(asyncio.CancelledError):
            await draining
        assert not write.done()
        client.repair_allowed.set()
        if fail:
            with pytest.raises(StoreOperationError):
                await write
        else:
            assert await write is not None
        await asyncio.wait_for(store.drain(), 1)
        assert not store._mutations
    finally:
        client.repair_allowed.set()
        await asyncio.gather(write, draining, return_exceptions=True)
        await store.close()


@pytest.mark.tourillon
async def test_draining_accepts_replica_reads() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    record = _record(addr)
    store, _, _, _, _ = _build_store(
        node_id="node-a",
        preferences=_two_owners(),
        members={},
        local_store=_StoreStub(record),
        remote_responses={},
    )
    store.pause()
    assert await store.read(PartitionAddress(pid=Pid(7), address=addr)) == record
    assert await store.read_at(record.key) == record


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_repair_copies_the_version_to_a_replica_that_lacks_it() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    record = _record(addr, data=b"v1", wall_ms=9)
    store, _, _, _, client = _build_store(
        node_id="node-a",
        preferences=_two_owners(),
        members={"node-b": _member("node-b", "127.0.0.1:9001")},
        local_store=_StoreStub(record),
        remote_responses={
            "replication.lookup": None,
            "replication.write": record.to_dict(),
        },
    )

    result = await store.repair(RepairRequest(token=_repair_token(record)))

    assert result.issue is RepairIssue.REPAIRED
    assert result.replicas == 2
    assert [request.kind for request in client.requests] == [
        "replication.lookup",
        "replication.write",
    ]
    copied = _sent_record(client.requests[1])
    assert copied["key"]["ts"]["wall_ms"] == 9
    assert store.stats.repairs_requested == 1


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_repair_reports_already_durable_without_writing_anything() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    record = _record(addr, data=b"v1", wall_ms=9)
    store, _, _, _, client = _build_store(
        node_id="node-a",
        preferences=_two_owners(),
        members={"node-b": _member("node-b", "127.0.0.1:9001")},
        local_store=_StoreStub(record),
        remote_responses={"replication.lookup": record.to_dict()},
    )

    result = await store.repair(
        RepairRequest(token=_repair_token(record), quorum_write=2)
    )

    assert result.issue is RepairIssue.ALREADY_DURABLE
    assert result.replicas == 2
    assert [request.kind for request in client.requests] == ["replication.lookup"]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_repair_raises_when_no_replica_holds_the_version() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    record = _record(addr, data=b"v1", wall_ms=9)
    store, _, _, _, client = _build_store(
        node_id="node-a",
        preferences=_two_owners(),
        members={"node-b": _member("node-b", "127.0.0.1:9001")},
        local_store=_StoreStub(None),
        remote_responses={"replication.lookup": None},
    )

    with pytest.raises(VersionNotFoundError) as excinfo:
        await store.repair(RepairRequest(token=_repair_token(record)))

    # Repair copies, it never creates: nothing found means nothing written.
    assert [request.kind for request in client.requests] == ["replication.lookup"]
    assert excinfo.value.to_dict()["code"] == "version_not_found"
    assert store.stats.repairs_rejected == 1


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_repair_does_not_count_a_stand_in_copy_as_owner_durability() -> None:
    """A copy held on the owner's behalf is a debt, not durability.

    The version only survives on the stand-in: it is still a faithful source,
    but both owners remain repair targets.
    """
    addr = Address(namespace=b"ns", key=b"key")
    record = _record(addr, data=b"v1", wall_ms=9)
    store, storage, _, serializer, client = _build_store(
        node_id="node-a",
        preferences=(
            Preference("node-a", True, None, writable=True),
            Preference("node-c", False, "node-b", writable=False),
        ),
        members={"node-b": _member("node-b", "127.0.0.1:9001")},
        local_store=_StoreStub(None),
        remote_responses={
            "replication.lookup": record.to_dict(),
            "replication.hint": record.to_dict(),
        },
    )

    result = await store.repair(
        RepairRequest(token=_repair_token(record), quorum_write=2)
    )

    assert result.issue is RepairIssue.REPAIRED
    assert result.replicas == 2
    assert [request.kind for request in client.requests] == [
        "replication.lookup",
        "replication.hint",
    ]
    payload = serializer.decode(client.requests[1].payload)
    assert payload["intent"] == "node-c"
    assert Record.from_dict(payload["record"]) == record
    assert storage.store.record == record


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_repair_with_only_a_stand_in_copy_misses_quorum_when_rehint_fails() -> (
    None
):
    addr = Address(namespace=b"ns", key=b"key")
    record = _record(addr, data=b"v1", wall_ms=9)
    store, _, _, _, _ = _build_store(
        node_id="node-a",
        preferences=(
            Preference("node-a", True, None, writable=True),
            Preference("node-c", False, "node-b", writable=False),
        ),
        members={"node-b": _member("node-b", "127.0.0.1:9001")},
        local_store=_StoreStub(record),
        remote_responses={"replication.lookup": record.to_dict()},
    )

    with pytest.raises(QuorumNotMetError) as exc_info:
        await store.repair(RepairRequest(token=_repair_token(record), quorum_write=2))

    assert exc_info.value.to_dict()["received"] == 1


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_repair_rejects_a_token_that_is_not_a_version_key() -> None:
    store, _, _, _, client = _build_store(
        node_id="node-a",
        preferences=_two_owners(),
        members={"node-b": _member("node-b", "127.0.0.1:9001")},
        local_store=_StoreStub(None),
        remote_responses={},
    )

    with pytest.raises(InvalidRepairTokenError):
        await store.repair(RepairRequest(token="not-a-token"))

    assert client.requests == []
    assert store.stats.repairs_requested == 0


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_repair_rejects_a_token_whose_partition_id_contradicts_its_address() -> (
    None
):
    addr = Address(namespace=b"ns", key=b"key")
    foreign = _record(addr, pid=9, data=b"v1", wall_ms=9)
    store, _, _, _, client = _build_store(
        node_id="node-a",
        preferences=_two_owners(),
        members={"node-b": _member("node-b", "127.0.0.1:9001")},
        local_store=_StoreStub(None),
        remote_responses={},
        partitioner_pid=7,
    )

    with pytest.raises(InvalidRepairTokenError) as excinfo:
        await store.repair(RepairRequest(token=_repair_token(foreign)))

    assert "partition id" in excinfo.value.to_dict()["reason"]
    assert client.requests == []


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_repair_rejects_a_timestamp_no_clock_here_could_have_emitted() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    future = _record(
        addr, data=b"v1", wall_ms=int(time.time() * 1000) + MAX_CLOCK_SKEW_MS * 10
    )
    store, _, _, _, client = _build_store(
        node_id="node-a",
        preferences=_two_owners(),
        members={"node-b": _member("node-b", "127.0.0.1:9001")},
        local_store=_StoreStub(None),
        remote_responses={},
    )

    with pytest.raises(InvalidRepairTokenError) as excinfo:
        await store.repair(RepairRequest(token=_repair_token(future)))

    assert "clock skew" in excinfo.value.to_dict()["reason"]
    assert client.requests == []


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_repair_refuses_a_quorum_the_store_cannot_record() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    record = _record(addr, data=b"v1", wall_ms=9)
    store, _, _, _, client = _build_store(
        node_id="node-a",
        preferences=_two_owners(),
        members={"node-b": _member("node-b", "127.0.0.1:9001")},
        local_store=_StoreStub(record),
        remote_responses={},
    )

    with pytest.raises(InvalidQuorumError):
        await store.repair(RepairRequest(token=_repair_token(record), quorum_write=0))

    assert client.requests == []


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_repair_still_reports_a_quorum_it_could_not_reach() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    record = _record(addr, data=b"v1", wall_ms=9)
    store, _, _, _, _ = _build_store(
        node_id="node-a",
        preferences=_two_owners(),
        members={"node-b": _member("node-b", "127.0.0.1:9001")},
        local_store=_StoreStub(record),
        remote_responses={"replication.lookup": None},
    )

    token = _repair_token(record)
    with pytest.raises(QuorumNotMetError) as excinfo:
        await store.repair(RepairRequest(token=token, quorum_write=2))

    # The client keeps the handle and stays free to try again.
    assert excinfo.value.to_dict()["token"] == token
    assert excinfo.value.received == 1


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_repair_raises_when_no_replica_is_available() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    record = _record(addr, data=b"v1", wall_ms=9)
    store, _, _, _, _ = _build_store(
        node_id="node-a",
        preferences=(
            Preference(node_id="node-b", readable=False, handoff=None, writable=False),
        ),
        members={"node-b": _member("node-b", "127.0.0.1:9001")},
        local_store=_StoreStub(record),
        remote_responses={},
    )

    with pytest.raises(NoReplicaAvailableError):
        await store.repair(RepairRequest(token=_repair_token(record)))


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_put_quorum_failure_hands_back_a_resumable_token() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    store, _, _, _, _ = _build_store(
        node_id="node-a",
        preferences=_two_owners(),
        members={"node-b": _member("node-b", "127.0.0.1:9001")},
        local_store=_StoreStub(None),
        remote_responses={},
    )

    with pytest.raises(QuorumNotMetError) as excinfo:
        await store.put(
            PutRequest(namespace=b"ns", key=b"key", value=b"v1", quorum_write=2)
        )

    token = excinfo.value.token
    assert token is not None
    key = decode_token(token)
    assert key.address == addr
    assert key.pid.value == 7


@pytest.mark.tourillon
async def test_store_measures_a_successful_operation() -> None:
    telemetry = RecordingTelemetry()
    store, *_ = _build_store(
        node_id="node-a",
        preferences=(Preference("node-a", True, None, writable=True),),
        members={},
        local_store=_StoreStub(),
        remote_responses={},
        telemetry=telemetry,
    )

    await store.put(PutRequest(namespace=b"ns", key=b"key", value=b"value"))

    labels = {"op": "write", "outcome": "ok"}
    assert telemetry.calls["kv_requests_total"] == [(1, labels)]
    ((elapsed, duration_labels),) = telemetry.calls["kv_request_duration_seconds"]
    assert elapsed >= 0 and duration_labels == labels
    assert telemetry.calls["replication_failures_total"] == []


@pytest.mark.tourillon
async def test_put_traces_storage_and_peer_requests() -> None:
    telemetry = RecordingTelemetry()
    store, *_ = _build_store(
        node_id="node-a",
        preferences=(
            Preference("node-a", True, None, writable=True),
            Preference("node-b", True, None, writable=True),
        ),
        members={"node-b": _member("node-b", "127.0.0.1:9001")},
        local_store=_StoreStub(),
        remote_responses={
            "replication.write": _record(Address(namespace=b"ns", key=b"key")).to_dict()
        },
        telemetry=telemetry,
    )

    await store.put(PutRequest(namespace=b"ns", key=b"key", value=b"value"))

    assert ("storage.write", {"group": 7}) in telemetry.spans
    assert ("storage.transaction", {"group": 7}) in telemetry.spans
    assert ("storage.record.put", {}) in telemetry.spans
    assert (
        "storage.operation",
        {"group": 7, "kind": "kv.put"},
    ) in telemetry.spans
    assert (
        "peer.request",
        {"kind": "replication.write", "peer": "node-b"},
    ) in telemetry.spans
    assert all(
        "key" not in attributes and "value" not in attributes
        for _, attributes in telemetry.spans
    )


@pytest.mark.tourillon
async def test_put_traces_storage_write_failure() -> None:
    telemetry = RecordingTelemetry()
    store, *_ = _build_store(
        node_id="node-a",
        preferences=(Preference("node-a", True, None, writable=True),),
        members={},
        local_store=_FailingStore(),
        remote_responses={},
        telemetry=telemetry,
    )

    with pytest.raises(StoreOperationError):
        await store.put(PutRequest(namespace=b"ns", key=b"key", value=b"value"))

    assert ("storage.write", {"group": 7}) in telemetry.spans
    assert ("storage.record.put", {}) in telemetry.spans


@pytest.mark.tourillon
async def test_get_delete_and_repair_trace_local_storage_cost() -> None:
    record = _record(Address(namespace=b"ns", key=b"key"))
    telemetry = RecordingTelemetry()
    store, *_ = _build_store(
        node_id="node-a",
        preferences=(Preference("node-a", True, None, writable=True),),
        members={},
        local_store=_StoreStub(record),
        remote_responses={},
        telemetry=telemetry,
    )

    await store.get(GetRequest(namespace=b"ns", key=b"key"))
    await store.delete(DeleteRequest(namespace=b"ns", key=b"key"))
    await store.repair(RepairRequest(token=_repair_token(record)))

    kinds = [
        attributes["kind"]
        for name, attributes in telemetry.spans
        if name == "storage.operation"
    ]
    assert kinds == ["kv.get", "kv.delete", "kv.repair"]


@pytest.mark.tourillon
async def test_store_measures_a_failed_quorum_and_the_failed_replica(
    caplog: pytest.LogCaptureFixture,
) -> None:
    telemetry = RecordingTelemetry()
    store, *_ = _build_store(
        node_id="node-a",
        preferences=(
            Preference("node-a", True, None, writable=True),
            Preference("node-c", True, None, writable=True),
        ),
        members={},
        local_store=_StoreStub(),
        remote_responses={},
        telemetry=telemetry,
    )
    caplog.set_level(logging.WARNING, logger="tourillon.global")

    with pytest.raises(QuorumNotMetError):
        await store.put(
            PutRequest(namespace=b"ns", key=b"key", value=b"value", quorum_write=2)
        )

    assert telemetry.calls["kv_requests_total"] == [
        (1, {"op": "write", "outcome": "quorum_not_met"})
    ]
    assert telemetry.total("replication_failures_total", op="write") == 1
    assert telemetry.total("obligations_total", event="recorded") >= 1
    assert (
        "peer.request",
        {"kind": "replication.write", "peer": "node-c"},
    ) in telemetry.spans
    messages = caplog.messages
    assert any(m.startswith("replication.write failed node=node-a") for m in messages)
    assert any(m.startswith("store.write failed node=node-a") for m in messages)


@pytest.mark.tourillon
async def test_store_counts_a_refusal_without_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    telemetry = RecordingTelemetry()
    store, *_ = _build_store(
        node_id="node-a",
        preferences=(Preference("node-a", True, None, writable=True),),
        members={},
        local_store=_StoreStub(),
        remote_responses={},
        telemetry=telemetry,
    )
    store.pause()
    caplog.set_level(logging.WARNING, logger="tourillon.global")

    with pytest.raises(NodeDrainingError):
        await store.put(PutRequest(namespace=b"ns", key=b"key", value=b"value"))

    assert telemetry.calls["kv_requests_total"] == [
        (1, {"op": "write", "outcome": "node_draining"})
    ]
    assert caplog.records == []
