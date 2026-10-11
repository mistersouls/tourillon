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
from contextlib import asynccontextmanager
from time import perf_counter
from typing import Any, cast

import pytest

from tourillon.core.exceptions import (
    NoReplicaAvailableError,
    QuorumNotMetError,
    StoreOperationError,
)
from tourillon.core.models.clock import HLCClock
from tourillon.core.models.member import Member, MemberPhase
from tourillon.core.models.req import HintRequest
from tourillon.core.models.storageschema import (
    Key,
    Keyspace,
    KVPair,
    KVRef,
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
from tourillon.core.services.store import StoreService
from tourillon.core.spaces.vnode import VNode
from tourlib.adapter import MsgPackSerializer
from tourlib.envelope import Envelope, build_envelope
from tourlib.models.kv import DeleteRequest, GetRequest, PutRequest
from tourlib.models.ts import HLCTimestamp
from tourlib.models.version import Address, Meta, Version
from tourlib.port import Serializer


class _PlacementStub:
    def __init__(self, preferences: tuple[Preference, ...], pid: int = 7) -> None:
        self._preferences = preferences
        self._placement = PartitionPlacement(
            group=0,
            vnode=VNode(token=0, node_id=preferences[0].node_id if preferences else "node-a"),
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

    async def get(self, key: PartitionAddress) -> Record | None:
        self.get_calls.append(key)
        return self.record

    async def put(self, record: Record) -> Record:
        self.put_calls.append(record)
        return record


class _SessionStub:
    def __init__(self, store: _StoreStub) -> None:
        self.store = store
        self._pairs: dict[tuple[Keyspace, bytes], KVPair] = {}
        if store.record is not None:
            self._pairs[(Keyspace.LOGS, store.record.key.to_bytes(Keyspace.LOGS))] = KVPair(
                key=store.record.key.to_bytes(Keyspace.LOGS),
                value=store.record.value.to_bytes(),
                keyspace=Keyspace.LOGS,
            )
            self._pairs[(Keyspace.TAGS, store.record.key.to_bytes(Keyspace.TAGS))] = KVPair(
                key=store.record.key.to_bytes(Keyspace.TAGS),
                value=store.record.tag.to_bytes(),
                keyspace=Keyspace.TAGS,
            )

    async def get(self, ref: KVRef) -> KVPair | None:
        return self._pairs.get((ref.keyspace, ref.key))

    async def put(self, kv: KVPair) -> None:
        self._pairs[(kv.keyspace, kv.key)] = kv
        if kv.keyspace == Keyspace.LOGS:
            key = Key.from_bytes(kv.key, Keyspace.LOGS)
            value = Value.from_bytes(kv.value)
            self.store.record = Record(
                key=key,
                value=value,
                tag=self.store.record.tag if self.store.record is not None else Tag(kind=TagKind.LIVE),
            )
        if kv.keyspace == Keyspace.TAGS and self.store.record is not None:
            self.store.record = Record(
                key=self.store.record.key,
                value=self.store.record.value,
                tag=Tag.from_bytes(kv.value),
            )

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


class _SessionProviderStub:
    def __init__(self, group_id: int, session: _SessionStub) -> None:
        self.group_id = group_id
        self._session = session

    @asynccontextmanager
    async def session(self, write: bool = False) -> Any:
        _ = write
        yield self._session

    async def close(self) -> None:
        return None


class _StorageStub:
    def __init__(self, store: _StoreStub) -> None:
        self.store = store
        self._session = _SessionStub(store)
        self.open_calls: list[int] = []

    async def session_provider_for(self, group: int) -> _SessionProviderStub:
        self.open_calls.append(group)
        return _SessionProviderStub(group, self._session)

    async def latest_hlc(self, total_groups: int) -> HLCTimestamp:
        _ = total_groups
        return HLCTimestamp(wall_ms=0, counter=0, node_id="node-a")

    async def close(self) -> None:
        return None


class _PartitionerStub:
    def group_for(self, pid: int) -> int:
        return pid


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
        self._delays = {kind: (value.copy() if isinstance(value, list) else value) for kind, value in (delays or {}).items()}
        self.requests: list[Envelope] = []

    async def request(self, envelope: Envelope) -> Envelope:
        self.requests.append(envelope)
        delay = self._next_delay(envelope.kind)
        if delay:
            await asyncio.sleep(delay)
        response = self._responses[envelope.kind]
        payload_doc = response.pop(0) if isinstance(response, list) else response
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

    async def release(self, peer_id: str) -> None:
        self.release_calls.append(peer_id)


def _member(node_id: str, bind: str) -> Member:
    return Member(
        node_id=node_id,
        peer_address=bind,
        epoch=1,
        phase=MemberPhase.READY,
        tokens=("01",),
    )


def _version(addr: Address, data: bytes = b"value") -> Version:
    return Version(
        address=addr,
        data=data,
        meta=Meta(ts=HLCTimestamp(wall_ms=3, counter=0, node_id="node-a"), quorum_write=1),
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
        key=Key(pid=Pid(pid), address=addr, ts=HLCTimestamp(wall_ms=wall_ms, counter=0, node_id="node-a")),
        value=Value(data=data, quorum_write=1),
        tag=tag,
    )


def _hint_record(
    addr: Address,
    target_node_id: str,
    *,
    pid: int = 7,
    data: bytes = b"value",
    tombstone: bool = False,
    wall_ms: int = 3,
    original_kind: TagKind = TagKind.LIVE,
) -> Record:
    original = Tag(kind=original_kind)
    return Record(
        key=Key(pid=Pid(pid), address=addr, ts=HLCTimestamp(wall_ms=wall_ms, counter=0, node_id="node-a")),
        value=Value(data=data, quorum_write=1),
        tag=Tag.hint(original, target_node_id),
    )


def _build_store(
    *,
    node_id: str,
    preferences: tuple[Preference, ...],
    members: dict[str, Member],
    local_store: _StoreStub,
    remote_responses: dict[str, Any],
    remote_delays: dict[str, Any] | None = None,
    response_delay: float = 0.05,
) -> tuple[StoreService, _StorageStub, _PeerPoolStub, MsgPackSerializer, _PeerClientStub]:
    serializer = MsgPackSerializer()
    client = _PeerClientStub(serializer, remote_responses, remote_delays)
    peer_pool = _PeerPoolStub(client, known_peers=set(members))
    storage = _StorageStub(local_store)
    service = StoreService(
        node_id=node_id,
        storage=cast(Storage, cast(object, storage)),
        partitioner=cast(Partitioner, cast(object, _PartitionerStub())),
        placement_strategy=cast(PlacementStrategy, cast(object, _PlacementStub(preferences))),
        clock=HLCClock(node_id),
        serializer=cast(Serializer, serializer),
        peer_pool=cast(PeerPool, cast(object, peer_pool)),
        response_delay=response_delay,
    )
    return service, storage, peer_pool, serializer, client


@pytest.mark.tourillon
def test_hint_request_roundtrip() -> None:
    req = HintRequest(
        target_node_id="node-b",
        record=_record(Address(namespace=b"ns", key=b"key"), data=b"value"),
    )

    rebuilt = HintRequest.from_dict(req.to_dict())

    assert rebuilt == req


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_get_returns_version_with_most_exact_matches() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    winning = _record(addr, data=b"winning", wall_ms=3)
    losing = _record(addr, data=b"losing", wall_ms=9)
    store, storage, peer_pool, _, _ = _build_store(
        node_id="node-a",
        preferences=(
            Preference(node_id="node-a", readable=True, alive=True, handoff=None),
            Preference(node_id="node-b", readable=True, alive=True, handoff=None),
            Preference(node_id="node-c", readable=True, alive=True, handoff=None),
        ),
        members={
            "node-b": _member("node-b", "127.0.0.1:9001"),
            "node-c": _member("node-c", "127.0.0.1:9002"),
        },
        local_store=_StoreStub(winning),
        remote_responses={"replication.read": [winning.to_dict(), losing.to_dict()]},
    )

    version = await store.get(GetRequest(namespace=b"ns", key=b"key"))

    assert version == _version(addr, data=b"winning")
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
            Preference(node_id="node-a", readable=True, alive=True, handoff=None),
            Preference(node_id="node-b", readable=True, alive=True, handoff=None),
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
            Preference(node_id="node-b", readable=True, alive=True, handoff=None),
            Preference(node_id="node-c", readable=True, alive=True, handoff=None),
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
    assert [request.kind for request in client.requests] == ["replication.read", "replication.read"]
    payload = serializer.decode(client.requests[1].payload)
    assert payload["addr"]["address"] == {"namespace": b"ns", "key": b"key"}


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_put_coordinates_and_uses_handoff_target() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    local = _record(addr, data=b"local")
    remote = _record(addr, data=b"remote")
    hinted = _hint_record(addr, "node-c", data=b"hinted")
    store, storage, peer_pool, serializer, client = _build_store(
        node_id="node-a",
        preferences=(
            Preference(node_id="node-a", readable=True, alive=True, handoff=None),
            Preference(node_id="node-b", readable=True, alive=True, handoff=None),
            Preference(node_id="node-c", readable=False, alive=False, handoff="node-d"),
        ),
        members={
            "node-b": _member("node-b", "127.0.0.1:9001"),
            "node-d": _member("node-d", "127.0.0.1:9003"),
        },
        local_store=_StoreStub(local),
        remote_responses={"replication.write": [remote.to_dict(), hinted.to_dict()]},
    )

    version = await store.put(PutRequest(namespace=b"ns", key=b"key", value=b"value"))

    assert version is not None
    assert version.address == addr
    assert version.data == b"value"
    assert version.meta.quorum_write == 1
    assert storage.open_calls == [7]
    assert [request.kind for request in client.requests] == ["replication.write", "replication.write"]
    second_payload = serializer.decode(client.requests[1].payload)
    assert second_payload["record"]["tag"]["kind"] == "HINT"
    assert second_payload["record"]["tag"]["payload"]["sub"] == "LIVE"
    hinted_tag = Tag.from_dict(second_payload["record"]["tag"])
    assert Tag.unhint(hinted_tag, "node-c") == Tag(kind=TagKind.LIVE)
    assert peer_pool.acquire_calls == ["node-b", "node-d"]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_put_replicates_to_handoff_and_readable_targets() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    hinted = _hint_record(addr, "node-b", data=b"hinted")
    store, storage, peer_pool, _, client = _build_store(
        node_id="node-a",
        preferences=(
            Preference(node_id="node-b", readable=True, alive=False, handoff="node-z"),
            Preference(node_id="node-c", readable=True, alive=True, handoff=None),
        ),
        members={
            "node-z": _member("node-z", "127.0.0.1:9009"),
            "node-c": _member("node-c", "127.0.0.1:9002"),
        },
        local_store=_StoreStub(),
        remote_responses={"replication.write": [hinted.to_dict(), hinted.to_dict()]},
    )

    version = await store.put(PutRequest(namespace=b"ns", key=b"key", value=b"value"))

    assert version == _version(addr, data=b"hinted")
    assert storage.open_calls == []
    assert peer_pool.acquire_calls == ["node-z", "node-c"]
    assert [request.kind for request in client.requests] == ["replication.write", "replication.write"]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_delete_returns_unique_versions() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    tombstone = _record(addr, data=b"", tombstone=True)
    store, storage, peer_pool, _, client = _build_store(
        node_id="node-a",
        preferences=(
            Preference(node_id="node-a", readable=True, alive=True, handoff=None),
            Preference(node_id="node-b", readable=True, alive=True, handoff=None),
            Preference(node_id="node-c", readable=False, alive=False, handoff="node-d"),
        ),
        members={
            "node-b": _member("node-b", "127.0.0.1:9001"),
            "node-d": _member("node-d", "127.0.0.1:9003"),
        },
        local_store=_StoreStub(tombstone),
        remote_responses={"replication.write": [tombstone.to_dict(), tombstone.to_dict()]},
    )

    version = await store.delete(DeleteRequest(namespace=b"ns", key=b"key"))

    assert version is not None
    assert version.address == addr
    assert version.data == b""
    assert version.meta.quorum_write == 1
    assert storage.open_calls == [7]
    assert [request.kind for request in client.requests] == ["replication.write", "replication.write"]
    assert peer_pool.acquire_calls == ["node-b", "node-d"]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_put_returns_after_quorum_without_waiting_for_slow_replicas() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    record = _record(addr, data=b"quorum")
    store, _, peer_pool, _, client = _build_store(
        node_id="node-a",
        preferences=(
            Preference(node_id="node-b", readable=True, alive=True, handoff=None),
            Preference(node_id="node-c", readable=True, alive=True, handoff=None),
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
    version = await store.put(PutRequest(namespace=b"ns", key=b"key", value=b"quorum", quorum_write=1))
    elapsed = perf_counter() - started

    assert version == _version(addr, data=b"quorum")
    assert elapsed < 0.1
    assert peer_pool.acquire_calls == ["node-b", "node-c"]
    assert [request.kind for request in client.requests] == ["replication.write", "replication.write"]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_get_returns_empty_list_when_remote_read_is_missing() -> None:
    store, _, peer_pool, _, _ = _build_store(
        node_id="node-a",
        preferences=(Preference(node_id="node-b", readable=True, alive=True, handoff=None),),
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
        preferences=(Preference(node_id="node-b", readable=False, alive=True, handoff=None),),
        members={},
        local_store=_StoreStub(),
        remote_responses={"replication.read": None},
    )

    with pytest.raises(NoReplicaAvailableError, match="preference list has no replication target") as exc_info:
        await store.get(GetRequest(namespace=b"ns", key=b"key"))
    assert exc_info.value.to_dict()["code"] == "no_replica_available"
    assert exc_info.value.to_dict()["operation"] == "read"


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_put_skips_unreadable_preference_without_handoff() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    remote = _record(addr, data=b"remote")
    store, storage, peer_pool, _, client = _build_store(
        node_id="node-a",
        preferences=(
            Preference(node_id="node-b", readable=False, alive=False, handoff=None),
            Preference(node_id="node-c", readable=True, alive=True, handoff=None),
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
    peer_pool = _PeerPoolStub(cast(_PeerClientStub, cast(object, client)), known_peers={"node-b"})
    storage = _StorageStub(_StoreStub())
    service = StoreService(
        node_id="node-a",
        storage=cast(Storage, cast(object, storage)),
        partitioner=cast(Partitioner, cast(object, _PartitionerStub())),
        placement_strategy=cast(
            PlacementStrategy,
            cast(object, _PlacementStub((Preference(node_id="node-b", readable=True, alive=True, handoff=None),))),
        ),
        clock=HLCClock("node-a"),
        serializer=cast(Serializer, serializer),
        peer_pool=cast(PeerPool, cast(object, peer_pool)),
    )

    with pytest.raises(StoreOperationError, match="write replication failed") as exc_info:
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
        preferences=(Preference(node_id="node-a", readable=True, alive=True, handoff=None),),
        members={},
        local_store=_StoreStub(_record(addr, tombstone=True)),
        remote_responses={"replication.read": None},
    )

    version = await store.get(GetRequest(namespace=b"ns", key=b"key"))

    assert version is None


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_get_returns_hint_record_as_visible_version() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    hinted = _hint_record(addr, "node-b", data=b"hinted")
    store, _, peer_pool, _, _ = _build_store(
        node_id="node-a",
        preferences=(Preference(node_id="node-b", readable=True, alive=True, handoff=None),),
        members={"node-b": _member("node-b", "127.0.0.1:9001")},
        local_store=_StoreStub(),
        remote_responses={"replication.read": hinted.to_dict()},
    )

    version = await store.get(GetRequest(namespace=b"ns", key=b"key"))

    assert version == _version(addr, data=b"hinted")
    assert peer_pool.acquire_calls == ["node-b"]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_get_forward_raises_when_peer_is_unknown() -> None:
    store, _, _, _, _ = _build_store(
        node_id="node-a",
        preferences=(Preference(node_id="node-b", readable=True, alive=True, handoff=None),),
        members={},
        local_store=_StoreStub(),
        remote_responses={"replication.read": None},
    )

    with pytest.raises(StoreOperationError, match="read replication failed") as exc_info:
        await store.get(GetRequest(namespace=b"ns", key=b"key"))
    assert [error["message"] for error in exc_info.value.to_dict()["errors"]] == ["unknown peer: node-b"]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_get_raises_when_quorum_read_not_met() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    remote = _record(addr, data=b"remote")
    store, _, _, _, _ = _build_store(
        node_id="node-a",
        preferences=(
            Preference(node_id="node-b", readable=True, alive=True, handoff=None),
            Preference(node_id="node-c", readable=True, alive=True, handoff=None),
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
    hinted = _hint_record(addr, "node-b", data=b"hinted")
    store, _, _, _, _ = _build_store(
        node_id="node-a",
        preferences=(
            Preference(node_id="node-b", readable=True, alive=False, handoff="node-z"),
            Preference(node_id="node-c", readable=True, alive=True, handoff=None),
        ),
        members={"node-z": _member("node-z", "127.0.0.1:9009")},
        local_store=_StoreStub(),
        remote_responses={"replication.write": hinted.to_dict()},
    )

    with pytest.raises(StoreOperationError, match="write quorum not met") as exc_info:
        await store.put(PutRequest(namespace=b"ns", key=b"key", value=b"value", quorum_write=2))
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
            Preference(node_id="node-a", readable=True, alive=True, handoff=None),
            Preference(node_id="node-b", readable=True, alive=True, handoff=None),
            Preference(node_id="node-c", readable=True, alive=True, handoff=None),
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
async def test_get_newer_hinted_tombstone_hides_older_live_version() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    stale = _record(addr, data=b"stale", wall_ms=10)
    hinted_delete = _hint_record(
        addr, "node-c", data=b"", wall_ms=20, original_kind=TagKind.TOMBSTONE
    )
    store, _, _, _, _ = _build_store(
        node_id="node-a",
        preferences=(
            Preference(node_id="node-a", readable=True, alive=True, handoff=None),
            Preference(node_id="node-b", readable=True, alive=True, handoff=None),
        ),
        members={"node-b": _member("node-b", "127.0.0.1:9001")},
        local_store=_StoreStub(stale),
        remote_responses={"replication.read": hinted_delete.to_dict()},
    )

    version = await store.get(GetRequest(namespace=b"ns", key=b"key"))

    assert version is None


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_get_unknown_key_meets_read_quorum_with_empty_responses() -> None:
    store, _, _, _, _ = _build_store(
        node_id="node-a",
        preferences=(
            Preference(node_id="node-a", readable=True, alive=True, handoff=None),
            Preference(node_id="node-b", readable=True, alive=True, handoff=None),
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
        preferences=(Preference(node_id="node-b", readable=False, alive=False, handoff=None),),
        members={},
        local_store=_StoreStub(),
        remote_responses={},
    )

    with pytest.raises(NoReplicaAvailableError, match="preference list has no replication target") as exc_info:
        await store.put(PutRequest(namespace=b"ns", key=b"key", value=b"value"))
    assert exc_info.value.to_dict()["operation"] == "write"
    assert client.requests == []


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_get_quorum_not_met_without_replica_error_raises_quorum_error() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    store, _, _, _, _ = _build_store(
        node_id="node-a",
        preferences=(
            Preference(node_id="node-a", readable=True, alive=True, handoff=None),
            Preference(node_id="node-b", readable=True, alive=True, handoff=None),
        ),
        members={"node-b": _member("node-b", "127.0.0.1:9001")},
        local_store=_StoreStub(_record(addr, data=b"v1", wall_ms=3)),
        remote_responses={"replication.read": _record(addr, data=b"v2", wall_ms=9).to_dict()},
    )

    with pytest.raises(QuorumNotMetError, match="read quorum not met") as exc_info:
        await store.get(GetRequest(namespace=b"ns", key=b"key", quorum_read=2))
    payload = exc_info.value.to_dict()
    assert payload["code"] == "quorum_not_met"
    assert payload["required"] == 2
    assert payload["received"] == 1
    assert payload["errors"] == []


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_get_reports_unanswered_replica_as_timeout_error() -> None:
    addr = Address(namespace=b"ns", key=b"key")
    record = _record(addr, data=b"v1")
    store, _, _, _, _ = _build_store(
        node_id="node-a",
        preferences=(
            Preference(node_id="node-a", readable=True, alive=True, handoff=None),
            Preference(node_id="node-b", readable=True, alive=True, handoff=None),
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
    assert errors == [{"type": "TimeoutError", "message": "no response from node-b within 0.05s"}]



