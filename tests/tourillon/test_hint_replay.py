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
import pickle
import time
from collections.abc import Callable
from contextlib import asynccontextmanager, suppress
from types import SimpleNamespace
from typing import cast

import pytest

from tourillon.core.exceptions import StoreOperationError
from tourillon.core.models.retry import BackoffRetry
from tourillon.core.models.storageschema import (
    Hint,
    HintKey,
    HintPrefix,
    HintReference,
    Key,
    Keyspace,
    KVPair,
    KVRef,
    Pid,
    Record,
    Tag,
    TagKind,
    Value,
)
from tourillon.core.partition.placement import Preference, PreferenceList
from tourillon.core.replay.hint import HintReplayer, _TargetBackoff
from tourlib.envelope import Envelope, build_envelope
from tourlib.models.ts import HLCTimestamp
from tourlib.models.version import Address


class _FakeStore:
    def __init__(self) -> None:
        self._items: dict[tuple[Keyspace, bytes], KVPair] = {}
        self.gets: list[tuple[Keyspace, bytes]] = []
        self.batches = 0

    def put(self, kv: KVPair) -> None:
        self._items[(kv.keyspace, kv.key)] = kv

    def delete(self, ref: KVRef) -> None:
        self._items.pop((ref.keyspace, ref.key), None)

    def get(self, ref: KVRef) -> KVPair | None:
        self.gets.append((ref.keyspace, ref.key))
        return self._items.get((ref.keyspace, ref.key))

    def reads(self, keyspace: Keyspace) -> int:
        return sum(1 for ks, _ in self.gets if ks == keyspace)

    def list(
        self,
        keyspace: Keyspace,
        prefix: HintPrefix | None = None,
        start: bytes | None = None,
        limit: int | None = None,
    ) -> list[KVPair]:
        items = [kv for (ks, _), kv in self._items.items() if ks == keyspace]
        prefix_bytes = prefix.to_bytes() if prefix is not None else None
        items = sorted(items, key=lambda kv: kv.key)
        if prefix_bytes is not None:
            items = [kv for kv in items if kv.key.startswith(prefix_bytes)]
        if start is not None:
            items = [kv for kv in items if kv.key > start]
        return items[:limit] if limit is not None else items


class _FakeBatch:
    def __init__(self, store: _FakeStore) -> None:
        self._store = store
        self._ops: list[tuple] = []

    async def __aenter__(self) -> "_FakeBatch":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        return False

    def get(self, ref: KVRef) -> None:
        self._ops.append(("get", ref))

    def exists(self, ref: KVRef) -> None:
        self._ops.append(("exists", ref))

    def put(self, kv: KVPair, overwrite: bool = True) -> None:
        self._ops.append(("put", kv, overwrite))

    def delete(self, ref: KVRef) -> None:
        self._ops.append(("delete", ref))

    def list(
        self,
        keyspace: Keyspace,
        prefix=None,
        start: bytes | None = None,
        limit: int | None = None,
        reverse: bool = False,
    ) -> None:
        del reverse
        self._ops.append(("list", keyspace, prefix, start, limit))

    def last(self, keyspace: Keyspace, prefix=None, predicate=None) -> None:
        self._ops.append(("last", keyspace, prefix, predicate))

    async def result(self):
        self._store.batches += 1
        results = []
        for op in self._ops:
            if op[0] == "get":
                results.append(self._store.get(op[1]))
            elif op[0] == "exists":
                results.append((op[1].keyspace, op[1].key) in self._store._items)
            elif op[0] == "put":
                present = (op[1].keyspace, op[1].key) in self._store._items
                written = op[2] or not present
                if written:
                    self._store.put(op[1])
                results.append(written)
            elif op[0] == "delete":
                self._store.delete(op[1])
                results.append(None)
            elif op[0] == "list":
                _, keyspace, prefix, start, limit = op
                results.append(self._store.list(keyspace, prefix, start, limit))
            else:
                _, keyspace, prefix, predicate = op
                items = self._store.list(keyspace, prefix)
                results.append(items[-1] if items and predicate is None else None)
        return results


class _FakeSession:
    def __init__(self, store: _FakeStore) -> None:
        self._store = store

    async def get(self, ref: KVRef) -> KVPair | None:
        return self._store.get(ref)

    async def exists(self, ref: KVRef) -> bool:
        return self._store.get(ref) is not None

    async def iter(
        self,
        keyspace: Keyspace,
        prefix=None,
        start: bytes | None = None,
        reverse: bool = False,
        batch_size: int = 32,
        limit: int | None = None,
    ):
        del prefix, reverse, batch_size, limit
        for kv in self._store.list(keyspace):
            if start is not None and kv.key <= start:
                continue
            yield kv

    async def put(self, kv: KVPair) -> None:
        self._store.put(kv)

    async def delete(self, ref: KVRef) -> None:
        self._store.delete(ref)

    async def last(self, keyspace: Keyspace, prefix=None, predicate=None):
        items = self._store.list(keyspace, prefix)
        if predicate is not None:
            items = [kv for kv in items if predicate(kv)]
        return items[-1] if items else None

    def batch(self):
        return _FakeBatch(self._store)


class _FakeSessionProvider:
    group_id = 0

    def __init__(self, store: _FakeStore) -> None:
        self._store = store
        self.write_sessions = 0
        self.opened = 0
        self.active = 0
        self.write_error: OSError | None = None

    @asynccontextmanager
    async def session(self, write: bool = False):
        if write:
            self.write_sessions += 1
            if self.write_error is not None:
                raise self.write_error
        self.opened += 1
        self.active += 1
        try:
            yield _FakeSession(self._store)
        finally:
            self.active -= 1

    async def close(self) -> None:
        return None


class _FakeStorage:
    def __init__(self, provider: _FakeSessionProvider) -> None:
        self._provider = provider

    async def session_provider_for(self, group: int) -> _FakeSessionProvider:
        assert group == 0
        return self._provider


class _BlockingWriteProvider(_FakeSessionProvider):
    def __init__(self, store: _FakeStore) -> None:
        super().__init__(store)
        self.writing = asyncio.Event()
        self.release = asyncio.Event()

    @asynccontextmanager
    async def session(self, write: bool = False):
        async with super().session(write=write) as session:
            if write:
                self.writing.set()
                await self.release.wait()
            yield session


class _PickleSerializer:
    schema_id = 1

    def encode(self, document: object) -> bytes:
        return pickle.dumps(document)

    def decode(self, payload: bytes) -> object:
        return pickle.loads(payload)


class _FakeClient:
    def __init__(
        self,
        serializer: _PickleSerializer,
        response_kind: str | None,
        release: asyncio.Event | None = None,
        *,
        owned: object = False,
        on_request: Callable[[Envelope], None] | None = None,
        response_payload: object | None = None,
    ) -> None:
        self._serializer = serializer
        self._response_kind = response_kind
        self._release = release
        self._owned = owned
        self._on_request = on_request
        self._response_payload = response_payload
        self.started = asyncio.Event()
        self.requests: list[Envelope] = []

    async def request(self, envelope: Envelope, timeout: float = 30.0) -> Envelope:
        del timeout
        self.requests.append(envelope)
        self.started.set()
        if self._release is not None:
            await self._release.wait()
        if self._on_request is not None:
            self._on_request(envelope)
        payload = self._response_payload
        if payload is None:
            payload = (
                {"owned": self._owned} if envelope.kind == "replication.exists" else {}
            )
        return build_envelope(
            kind=self._response_kind or f"{envelope.kind}.accept",
            payload=self._serializer.encode(payload),
            schema_id=self._serializer.schema_id,
        )


class _FakePeerPool:
    def __init__(self, client: _FakeClient | dict[str, _FakeClient]) -> None:
        self._client = client
        self.leased: list[str] = []

    @asynccontextmanager
    async def lease(self, peer_id: str):
        self.leased.append(peer_id)
        if isinstance(self._client, dict):
            if peer_id not in self._client:
                raise RuntimeError(f"Peer {peer_id} unavailable")
            yield self._client[peer_id]
        else:
            yield self._client


class _Placement:
    def __init__(self, *preferences: Preference) -> None:
        self.preferences = preferences or (_owner("node-b"),)
        self.addresses: list[Address] = []

    async def preference_list(self, address: Address) -> PreferenceList:
        self.addresses.append(address)
        return SimpleNamespace(preferences=self.preferences)  # type: ignore[return-value]


def _owner(
    node_id: str, *, writable: bool = True, handoff: str | None = None
) -> Preference:
    return Preference(
        node_id=node_id, readable=True, handoff=handoff, writable=writable
    )


def _topology_replayer(
    storage: _FakeStore,
    placement: _Placement,
    pool: _FakePeerPool,
    serializer: _PickleSerializer,
    *,
    provider: _FakeSessionProvider | None = None,
    batch_size: int = 32,
    concurrency: int = 4,
    max_batch_bytes: int = 64 * 1024 * 1024,
) -> HintReplayer:
    return HintReplayer(
        storage=_FakeStorage(provider or _FakeSessionProvider(storage)),  # type: ignore[arg-type]
        peer_pool=pool,  # type: ignore[arg-type]
        serializer=serializer,  # type: ignore[arg-type]
        placement_strategy=placement,  # type: ignore[arg-type]
        node_id="node-a",
        total_groups=1,
        batch_size=batch_size,
        concurrency=concurrency,
        max_batch_bytes=max_batch_bytes,
        backoff=BackoffRetry(initial_delay=30.0, jitter=0.0),
    )


def _record() -> Record:
    key = Key(
        pid=Pid(7),
        address=Address(namespace=b"ns", key=b"k1"),
        ts=HLCTimestamp(wall_ms=107, counter=1, node_id="node-a"),
    )
    value = Value(data=b"v1", quorum_write=1)
    return Record(
        key=key,
        value=value,
        tag=Tag.hint(Tag(kind=TagKind.LIVE)),
    )


def _record_at(idx: int) -> Record:
    key = Key(
        pid=Pid(7),
        address=Address(namespace=b"ns", key=f"k{idx}".encode()),
        ts=HLCTimestamp(wall_ms=107 + idx, counter=idx, node_id="node-a"),
    )
    value = Value(data=f"v{idx}".encode(), quorum_write=1)
    return Record(
        key=key,
        value=value,
        tag=Tag.hint(Tag(kind=TagKind.LIVE)),
    )


def _live_at(idx: int) -> Record:
    key = Key(
        pid=Pid(7),
        address=Address(namespace=b"ns", key=f"live{idx}".encode()),
        ts=HLCTimestamp(wall_ms=200 + idx, counter=idx, node_id="node-a"),
    )
    return Record(
        key=key, value=Value(data=b"x", quorum_write=1), tag=Tag(kind=TagKind.LIVE)
    )


def _seed(store: _FakeStore, record: Record, intent: str = "node-b") -> Hint | None:
    store.put(
        KVPair(
            key=record.key.to_bytes(Keyspace.TAGS),
            value=record.tag.to_bytes(),
            keyspace=Keyspace.TAGS,
        )
    )
    store.put(
        KVPair(
            key=record.key.to_bytes(Keyspace.LOGS),
            value=record.value.to_bytes(),
            keyspace=Keyspace.LOGS,
        )
    )
    if record.tag.kind is TagKind.HINT:
        hint = Hint(
            record=record,
            intent=intent,
            hlc=HLCTimestamp(
                wall_ms=10_000 + record.key.ts.wall_ms,
                counter=record.key.ts.counter,
                node_id="hint",
            ),
        )
        store.put(
            KVPair(
                key=HintKey(hint.intent, hint.hlc).to_bytes(),
                value=HintReference(record.key).to_bytes(),
                keyspace=Keyspace.META,
            )
        )
        return hint
    return None


def _stored_tag(store: _FakeStore, record: Record) -> Tag:
    kv = store.get(
        KVRef(key=record.key.to_bytes(Keyspace.TAGS), keyspace=Keyspace.TAGS)
    )
    assert kv is not None
    return Tag.from_bytes(kv.value)


@pytest.mark.tourillon
async def test_failed_hint_delivery_preserves_hint_tag() -> None:
    store = _FakeStore()
    record = _record()
    _seed(store, record)
    serializer = _PickleSerializer()
    replayer = HintReplayer(
        storage=_FakeStorage(_FakeSessionProvider(store)),  # type: ignore[arg-type]
        peer_pool=_FakePeerPool(_FakeClient(serializer, "unexpected.kind")),  # type: ignore[arg-type]
        serializer=serializer,  # type: ignore[arg-type]
        placement_strategy=_Placement(),  # type: ignore[arg-type]
        node_id="node-a",
        total_groups=1,
    )
    await replayer._replay_once()
    assert _stored_tag(store, record).kind == TagKind.HINT


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_replay_once_deletes_obligation_without_changing_hint_tag() -> None:
    store = _FakeStore()
    record = _record()
    _seed(store, record)
    serializer = _PickleSerializer()
    client = _FakeClient(serializer, "replication.write.accept")
    provider = _FakeSessionProvider(store)
    replayer = HintReplayer(
        storage=cast(object, _FakeStorage(provider)),
        peer_pool=_FakePeerPool(client),  # type: ignore[arg-type]
        serializer=serializer,  # type: ignore[arg-type]
        placement_strategy=_Placement(),  # type: ignore[arg-type]
        node_id="node-a",
        total_groups=1,
    )

    await replayer._replay_once()

    assert client.requests[0].kind == "replication.write"
    original = Record(key=record.key, value=record.value, tag=Tag.unhint(record.tag))
    assert serializer.decode(client.requests[0].payload) == {
        "record": original.to_dict()
    }
    assert _stored_tag(store, record) == record.tag
    assert store.list(Keyspace.META, HintPrefix()) == []
    assert replayer._stats.applied_total == 1
    assert replayer._stats.failed_total == 0
    assert provider.write_sessions == 1


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_replay_once_batches_satisfied_hints_per_group() -> None:
    store = _FakeStore()
    records = [_record_at(1), _record_at(2)]
    for record in records:
        _seed(store, record)
    serializer = _PickleSerializer()
    provider = _FakeSessionProvider(store)
    replayer = HintReplayer(
        storage=cast(object, _FakeStorage(provider)),
        peer_pool=_FakePeerPool(_FakeClient(serializer, "replication.write.accept")),  # type: ignore[arg-type]
        serializer=serializer,  # type: ignore[arg-type]
        placement_strategy=_Placement(),  # type: ignore[arg-type]
        node_id="node-a",
        total_groups=1,
        batch_size=8,
    )

    await replayer._replay_once()

    assert provider.write_sessions == 1
    assert all(_stored_tag(store, record) == record.tag for record in records)
    assert store.list(Keyspace.META, HintPrefix()) == []


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_replay_once_keeps_hint_when_remote_write_is_rejected() -> None:
    store = _FakeStore()
    record = _record()
    _seed(store, record)
    serializer = _PickleSerializer()
    provider = _FakeSessionProvider(store)
    replayer = HintReplayer(
        storage=cast(object, _FakeStorage(provider)),
        peer_pool=_FakePeerPool(_FakeClient(serializer, "replication.write.unexpected")),  # type: ignore[arg-type]
        serializer=serializer,  # type: ignore[arg-type]
        placement_strategy=_Placement(),  # type: ignore[arg-type]
        node_id="node-a",
        total_groups=1,
    )

    await replayer._replay_once()

    assert _stored_tag(store, record).kind == TagKind.HINT
    assert replayer._stats.applied_total == 0
    assert replayer._stats.failed_total == 1
    assert provider.write_sessions == 0


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_replay_does_not_read_logs_for_non_hint_tags() -> None:
    """A replay cycle must cost the number of hints, not the size of the store.

    Tags carry their own kind, so a live entry can be discarded without
    touching LOGS. Without that, every cycle issued one random read per stored
    version, a few times a second, even with nothing to replay.
    """
    store = _FakeStore()
    for idx in range(5):
        _seed(store, _live_at(idx))
    hint = _record()
    _seed(store, hint)
    serializer = _PickleSerializer()
    provider = _FakeSessionProvider(store)
    replayer = HintReplayer(
        storage=cast(object, _FakeStorage(provider)),
        peer_pool=_FakePeerPool(_FakeClient(serializer, "replication.write.accept")),  # type: ignore[arg-type]
        serializer=serializer,  # type: ignore[arg-type]
        placement_strategy=_Placement(),  # type: ignore[arg-type]
        node_id="node-a",
        total_groups=1,
    )

    await replayer._replay_once()

    assert store.reads(Keyspace.LOGS) == 1
    assert _stored_tag(store, hint) == hint.tag
    assert store.list(Keyspace.META, HintPrefix()) == []


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_replay_leases_an_unreachable_target_once_per_cycle() -> None:
    """One unreachable peer must not cost one dial per pending hint."""
    store = _FakeStore()
    hints = [_record_at(1), _record_at(2), _record_at(3)]
    for record in hints:
        _seed(store, record)
    serializer = _PickleSerializer()
    pool = _FakePeerPool(_FakeClient(serializer, "replication.write.unexpected"))
    replayer = HintReplayer(
        storage=cast(object, _FakeStorage(provider := _FakeSessionProvider(store))),
        peer_pool=pool,  # type: ignore[arg-type]
        serializer=serializer,  # type: ignore[arg-type]
        placement_strategy=_Placement(),  # type: ignore[arg-type]
        node_id="node-a",
        total_groups=1,
    )

    await replayer._replay_once()

    assert pool.leased == ["node-b"]
    assert all(_stored_tag(store, record).kind == TagKind.HINT for record in hints)
    assert provider.write_sessions == 0


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_wait_returns_after_the_pending_obligation_is_replayed() -> None:
    store = _FakeStore()
    _seed(store, _record())
    serializer = _PickleSerializer()
    release = asyncio.Event()
    client = _FakeClient(serializer, "replication.write.accept", release)
    provider = _FakeSessionProvider(store)
    replayer = HintReplayer(
        storage=_FakeStorage(provider),  # type: ignore[arg-type]
        peer_pool=_FakePeerPool(client),  # type: ignore[arg-type]
        serializer=serializer,  # type: ignore[arg-type]
        placement_strategy=_Placement(),  # type: ignore[arg-type]
        node_id="node-a",
        total_groups=1,
        poll_interval=0.001,
    )
    await replayer.start()
    waiting = asyncio.create_task(replayer.wait())
    try:
        await asyncio.wait_for(client.started.wait(), timeout=1)
        assert not waiting.done()
        release.set()
        await asyncio.wait_for(waiting, timeout=1)
        assert not await replayer._pending_hints()
    finally:
        release.set()
        waiting.cancel()
        with suppress(asyncio.CancelledError):
            await waiting
        await replayer.stop()


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_wait_stays_pending_after_delivery_failure_and_backoff() -> None:
    store = _FakeStore()
    _seed(store, _record())
    serializer = _PickleSerializer()
    client = _FakeClient(serializer, "replication.write.unexpected")
    provider = _FakeSessionProvider(store)
    replayer = HintReplayer(
        storage=_FakeStorage(provider),  # type: ignore[arg-type]
        peer_pool=_FakePeerPool(client),  # type: ignore[arg-type]
        serializer=serializer,  # type: ignore[arg-type]
        placement_strategy=_Placement(),  # type: ignore[arg-type]
        node_id="node-a",
        total_groups=1,
        poll_interval=0.001,
        backoff=BackoffRetry(initial_delay=30.0, jitter=0.0),
    )
    await replayer.start()
    waiting = asyncio.create_task(replayer.wait())
    try:
        await asyncio.wait_for(client.started.wait(), timeout=1)
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(asyncio.shield(waiting), timeout=0.02)
        assert replayer._stats.failed_total == 1
        assert await replayer._pending_hints()
    finally:
        waiting.cancel()
        with suppress(asyncio.CancelledError):
            await waiting
        await replayer.stop()


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_wait_propagates_cancellation_while_an_obligation_is_pending() -> None:
    store = _FakeStore()
    _seed(store, _record())
    serializer = _PickleSerializer()
    release = asyncio.Event()
    client = _FakeClient(serializer, "replication.write.accept", release)
    replayer = HintReplayer(
        storage=_FakeStorage(_FakeSessionProvider(store)),  # type: ignore[arg-type]
        peer_pool=_FakePeerPool(client),  # type: ignore[arg-type]
        serializer=serializer,  # type: ignore[arg-type]
        placement_strategy=_Placement(),  # type: ignore[arg-type]
        node_id="node-a",
        total_groups=1,
        poll_interval=0.001,
    )
    await replayer.start()
    waiting = asyncio.create_task(replayer.wait())
    try:
        await asyncio.wait_for(client.started.wait(), timeout=1)
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
    finally:
        release.set()
        await replayer.stop()


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_wait_requires_a_running_replayer_only_when_work_is_pending() -> None:
    store = _FakeStore()
    replayer = HintReplayer(
        storage=_FakeStorage(_FakeSessionProvider(store)),  # type: ignore[arg-type]
        peer_pool=_FakePeerPool(_FakeClient(_PickleSerializer(), "replication.write.accept")),  # type: ignore[arg-type]
        serializer=_PickleSerializer(),  # type: ignore[arg-type]
        placement_strategy=_Placement(),  # type: ignore[arg-type]
        node_id="node-a",
        total_groups=1,
    )
    await replayer.wait()

    _seed(store, _record())
    with pytest.raises(RuntimeError, match="running replayer"):
        await replayer.wait()


@pytest.mark.tourillon
async def test_current_intent_is_written_without_existence_or_other_owners() -> None:
    storage = _FakeStore()
    record = _record()
    _seed(storage, record)
    serializer = _PickleSerializer()
    client = _FakeClient(serializer, None, owned=True)
    pool = _FakePeerPool({"node-b": client})
    placement = _Placement(_owner("node-c"), _owner("node-b", handoff="node-d"))
    replayer = _topology_replayer(storage, placement, pool, serializer)

    await replayer._replay_once()

    assert pool.leased == ["node-b"]
    assert [request.kind for request in client.requests] == ["replication.write"]
    assert placement.addresses and all(
        address == record.key.address for address in placement.addresses
    )
    assert storage.list(Keyspace.META, HintPrefix()) == []
    assert _stored_tag(storage, record) == record.tag


@pytest.mark.tourillon
@pytest.mark.parametrize("failure", ["unavailable", "rejected"])
async def test_current_intent_failure_keeps_exact_obligation_and_backs_off(
    failure: str,
) -> None:
    storage = _FakeStore()
    record = _record()
    _seed(storage, record)
    before = storage.list(Keyspace.META, HintPrefix())
    serializer = _PickleSerializer()
    rejection = StoreOperationError("write", [RuntimeError("not writable")])
    client = _FakeClient(
        serializer,
        "replication.write.reject",
        response_payload=rejection.to_dict(),
    )
    pool = _FakePeerPool({} if failure == "unavailable" else {"node-b": client})
    replayer = _topology_replayer(storage, _Placement(), pool, serializer)

    await replayer._replay_once()
    await replayer._replay_once()

    assert pool.leased == ["node-b"]
    assert storage.list(Keyspace.META, HintPrefix()) == before
    assert _stored_tag(storage, record) == record.tag
    assert not client.requests or client.requests[0].kind == "replication.write"


@pytest.mark.tourillon
@pytest.mark.parametrize("owned", [True, False])
async def test_changed_owners_check_presence_and_write_only_missing_versions(
    owned: bool,
) -> None:
    storage = _FakeStore()
    record = _record()
    _seed(storage, record, intent="departed")
    serializer = _PickleSerializer()
    current = _FakeClient(serializer, None, owned=owned)
    other = _FakeClient(serializer, None, owned=True)
    pool = _FakePeerPool({"node-b": current, "node-c": other})
    placement = _Placement(_owner("node-b", handoff="substitute"), _owner("node-c"))
    replayer = _topology_replayer(storage, placement, pool, serializer)

    await replayer._replay_once()

    expected = ["replication.exists"]
    if not owned:
        expected.append("replication.write")
    assert [request.kind for request in current.requests] == expected
    assert [request.kind for request in other.requests] == ["replication.exists"]
    assert set(pool.leased) == {"node-b", "node-c"}
    assert serializer.decode(current.requests[0].payload) == {
        "key": record.key.to_dict()
    }
    if not owned:
        assert serializer.decode(current.requests[1].payload) == {
            "record": Record(
                key=record.key, value=record.value, tag=Tag.unhint(record.tag)
            ).to_dict()
        }
    assert storage.list(Keyspace.META, HintPrefix()) == []
    assert _stored_tag(storage, record) == record.tag


@pytest.mark.tourillon
@pytest.mark.parametrize(
    "failure", ["unavailable", "write_rejected", "exists_rejected"]
)
async def test_changed_owners_keep_debt_until_every_owner_confirms(
    failure: str,
) -> None:
    storage = _FakeStore()
    record = _record()
    _seed(storage, record, intent="departed")
    before = storage.list(Keyspace.META, HintPrefix())
    serializer = _PickleSerializer()
    confirmed = _FakeClient(serializer, None, owned=True)
    rejection = StoreOperationError("write", [RuntimeError("not writable")])
    kind = (
        "replication.exists.reject"
        if failure == "exists_rejected"
        else "replication.write.reject"
    )
    failing = _FakeClient(serializer, kind, response_payload=rejection.to_dict())
    clients = {"node-b": confirmed}
    if failure != "unavailable":
        clients["node-c"] = failing
    pool = _FakePeerPool(clients)
    replayer = _topology_replayer(
        storage, _Placement(_owner("node-b"), _owner("node-c")), pool, serializer
    )
    if failure == "write_rejected":
        failing._response_kind = None

        def reject_write(request: Envelope) -> None:
            failing._response_payload = (
                rejection.to_dict()
                if request.kind == "replication.write"
                else {"owned": False}
            )
            failing._response_kind = (
                "replication.write.reject"
                if request.kind == "replication.write"
                else "replication.exists.accept"
            )

        failing._on_request = reject_write

    await replayer._replay_once()
    attempts = pool.leased.count("node-c")
    await replayer._replay_once()

    assert attempts >= 1
    assert pool.leased.count("node-c") == attempts
    assert storage.list(Keyspace.META, HintPrefix()) == before
    assert _stored_tag(storage, record) == record.tag


@pytest.mark.tourillon
@pytest.mark.parametrize(
    "payload",
    [{}, {"owned": None}, {"owned": 1}, {"owned": "true"}, [], {"present": True}],
)
async def test_malformed_existence_response_never_satisfies_debt(
    payload: object,
) -> None:
    storage = _FakeStore()
    _seed(storage, _record(), intent="departed")
    before = storage.list(Keyspace.META, HintPrefix())
    serializer = _PickleSerializer()
    client = _FakeClient(
        serializer, "replication.exists.accept", response_payload=payload
    )
    replayer = _topology_replayer(
        storage, _Placement(), _FakePeerPool(client), serializer
    )

    await replayer._replay_once()

    assert [request.kind for request in client.requests] == ["replication.exists"]
    assert storage.list(Keyspace.META, HintPrefix()) == before


@pytest.mark.tourillon
async def test_empty_topology_preserves_hint_without_remote_or_local_work() -> None:
    storage = _FakeStore()
    _seed(storage, _record())
    before = storage.list(Keyspace.META, HintPrefix())
    serializer = _PickleSerializer()
    placement = _Placement()
    placement.preferences = ()
    pool = _FakePeerPool({})
    provider = _FakeSessionProvider(storage)
    replayer = _topology_replayer(
        storage, placement, pool, serializer, provider=provider
    )

    await replayer._replay_once()

    assert storage.list(Keyspace.META, HintPrefix()) == before
    assert pool.leased == []
    assert provider.write_sessions == 0


@pytest.mark.tourillon
@pytest.mark.parametrize("kind", [TagKind.LIVE, TagKind.TOMBSTONE])
async def test_local_new_owner_promotes_hint_through_partition_group_store(
    kind: TagKind,
) -> None:
    storage = _FakeStore()
    source = _record()
    record = Record(key=source.key, value=source.value, tag=Tag.hint(Tag(kind=kind)))
    _seed(storage, record, intent="departed")
    serializer = _PickleSerializer()
    provider = _FakeSessionProvider(storage)
    pool = _FakePeerPool({})
    replayer = _topology_replayer(
        storage, _Placement(_owner("node-a")), pool, serializer, provider=provider
    )

    await replayer._replay_once()

    assert provider.write_sessions == 2  # Local promotion, then satisfaction.
    assert replayer._stats.applied_total == 1
    assert _stored_tag(storage, record).kind == kind
    assert storage.list(Keyspace.META, HintPrefix()) == []
    assert pool.leased == []


@pytest.mark.tourillon
async def test_local_current_intent_writes_without_existence_check() -> None:
    storage = _FakeStore()
    record = _record()
    _seed(storage, record, intent="node-a")
    serializer = _PickleSerializer()
    pool = _FakePeerPool({})
    replayer = _topology_replayer(
        storage, _Placement(_owner("node-a")), pool, serializer
    )

    await replayer._replay_once()

    assert replayer._stats.applied_total == 1
    assert _stored_tag(storage, record).kind == TagKind.LIVE
    assert storage.list(Keyspace.META, HintPrefix()) == []
    assert pool.leased == []


@pytest.mark.tourillon
async def test_local_new_owner_already_owns_version_without_another_write() -> None:
    storage = _FakeStore()
    record = _record()
    _seed(storage, record, intent="departed")
    normal = Record(key=record.key, value=record.value, tag=Tag.unhint(record.tag))
    _seed(storage, normal)
    serializer = _PickleSerializer()
    replayer = _topology_replayer(
        storage,
        _Placement(_owner("node-a")),
        _FakePeerPool({}),
        serializer,
    )

    await replayer._replay_once()

    assert replayer._stats.applied_total == 0
    assert storage.list(Keyspace.META, HintPrefix()) == []
    assert _stored_tag(storage, record) == normal.tag


@pytest.mark.tourillon
async def test_local_write_failure_retains_hint_and_backs_off() -> None:
    storage = _FakeStore()
    record = _record()
    _seed(storage, record, intent="departed")
    before = storage.list(Keyspace.META, HintPrefix())
    serializer = _PickleSerializer()
    provider = _FakeSessionProvider(storage)
    provider.write_error = OSError("storage unavailable")
    replayer = _topology_replayer(
        storage,
        _Placement(_owner("node-a")),
        _FakePeerPool({}),
        serializer,
        provider=provider,
    )

    await replayer._replay_once()
    await replayer._replay_once()

    assert provider.write_sessions == 1
    assert replayer._stats.failed_total == 1
    assert storage.list(Keyspace.META, HintPrefix()) == before
    assert _stored_tag(storage, record) == record.tag


@pytest.mark.tourillon
@pytest.mark.parametrize("owned", [False, True])
async def test_paused_local_replay_only_confirms_existing_owned_copy(
    owned: bool,
) -> None:
    storage = _FakeStore()
    record = _record()
    _seed(storage, record, intent="departed")
    if owned:
        original = Record(
            key=record.key, value=record.value, tag=Tag.unhint(record.tag)
        )
        _seed(storage, original)
    before = storage.list(Keyspace.META, HintPrefix())
    provider = _FakeSessionProvider(storage)
    serializer = _PickleSerializer()
    replayer = _topology_replayer(
        storage,
        _Placement(_owner("node-a")),
        _FakePeerPool({}),
        serializer,
        provider=provider,
    )
    replayer.pause_local_writes()

    await replayer._replay_once()
    await replayer.drain_local_writes()

    assert storage.list(Keyspace.META, HintPrefix()) == ([] if owned else before)
    assert provider.write_sessions == int(owned)  # Only obligation satisfaction.
    assert replayer._stats.applied_total == 0


@pytest.mark.tourillon
async def test_resume_local_replay_allows_promotion_again() -> None:
    storage = _FakeStore()
    record = _record()
    _seed(storage, record, intent="departed")
    serializer = _PickleSerializer()
    replayer = _topology_replayer(
        storage, _Placement(_owner("node-a")), _FakePeerPool({}), serializer
    )
    replayer.pause_local_writes()
    replayer.resume_local_writes()

    await replayer._replay_once()

    assert _stored_tag(storage, record).kind is TagKind.LIVE
    assert storage.list(Keyspace.META, HintPrefix()) == []


@pytest.mark.tourillon
async def test_drain_local_writes_waits_for_admitted_promotion() -> None:
    storage = _FakeStore()
    record = _record()
    _seed(storage, record, intent="departed")
    provider = _BlockingWriteProvider(storage)
    serializer = _PickleSerializer()
    replayer = _topology_replayer(
        storage,
        _Placement(_owner("node-a")),
        _FakePeerPool({}),
        serializer,
        provider=provider,
    )
    replay = asyncio.create_task(replayer._replay_once())
    drain_started = asyncio.Event()

    async def drain() -> None:
        drain_started.set()
        await replayer.drain_local_writes()

    try:
        await asyncio.wait_for(provider.writing.wait(), 1)
        replayer.pause_local_writes()
        draining = asyncio.create_task(drain())
        await asyncio.wait_for(drain_started.wait(), 1)
        assert not draining.done()
        provider.release.set()
        await asyncio.wait_for(asyncio.gather(replay, draining), 1)
        assert _stored_tag(storage, record).kind is TagKind.LIVE
        assert provider.active == 0
    finally:
        provider.release.set()
        await replay


@pytest.mark.tourillon
async def test_local_promotion_releases_session_before_remote_delivery() -> None:
    storage = _FakeStore()
    _seed(storage, _record(), intent="departed")
    provider = _FakeSessionProvider(storage)
    serializer = _PickleSerializer()

    def check_no_session(request: Envelope) -> None:
        assert provider.active == 0

    client = _FakeClient(serializer, None, on_request=check_no_session)
    replayer = _topology_replayer(
        storage,
        _Placement(_owner("node-a"), _owner("node-b")),
        _FakePeerPool(client),
        serializer,
        provider=provider,
    )

    await replayer._replay_once()

    assert [request.kind for request in client.requests] == [
        "replication.exists",
        "replication.write",
    ]
    assert storage.list(Keyspace.META, HintPrefix()) == []


@pytest.mark.tourillon
async def test_parallel_destinations_progress_while_one_is_blocked() -> None:
    storage = _FakeStore()
    _seed(storage, _record(), intent="departed")
    provider = _FakeSessionProvider(storage)
    serializer = _PickleSerializer()
    release = asyncio.Event()
    slow = _FakeClient(serializer, None, release)
    healthy = _FakeClient(serializer, None)
    replayer = _topology_replayer(
        storage,
        _Placement(_owner("node-b"), _owner("node-c")),
        _FakePeerPool({"node-b": slow, "node-c": healthy}),
        serializer,
        provider=provider,
        concurrency=2,
    )
    replay = asyncio.create_task(replayer._replay_once())
    try:
        await asyncio.wait_for(healthy.started.wait(), 1)
        assert [item.kind for item in healthy.requests] == [
            "replication.exists",
            "replication.write",
        ]
        assert not replay.done()
        assert provider.active == 0
    finally:
        release.set()
        await asyncio.wait_for(replay, 1)
    assert storage.list(Keyspace.META, HintPrefix()) == []


@pytest.mark.tourillon
async def test_destination_workers_respect_concurrency_limit() -> None:
    storage = _FakeStore()
    _seed(storage, _record(), intent="departed")
    serializer = _PickleSerializer()
    releases = [asyncio.Event() for _ in range(3)]
    clients = [_FakeClient(serializer, None, event) for event in releases]
    pool = _FakePeerPool(
        dict(zip(("node-b", "node-c", "node-d"), clients, strict=True))
    )
    replayer = _topology_replayer(
        storage,
        _Placement(*[_owner(node) for node in ("node-b", "node-c", "node-d")]),
        pool,
        serializer,
        concurrency=2,
    )
    replay = asyncio.create_task(replayer._replay_once())
    try:
        await asyncio.wait_for(
            asyncio.gather(clients[0].started.wait(), clients[1].started.wait()), 1
        )
        assert not clients[2].started.is_set()
        releases[1].set()
        await asyncio.wait_for(clients[2].started.wait(), 1)
        assert not releases[0].is_set()
    finally:
        for event in releases:
            event.set()
        await asyncio.wait_for(replay, 1)
    assert storage.list(Keyspace.META, HintPrefix()) == []


@pytest.mark.tourillon
async def test_same_destination_is_serial_and_failed_attempt_defers_queued_hints() -> (
    None
):
    storage = _FakeStore()
    for index in range(3):
        _seed(storage, _record_at(index))
    serializer = _PickleSerializer()
    release = asyncio.Event()
    client = _FakeClient(serializer, "unexpected.kind", release)
    pool = _FakePeerPool(client)
    replayer = _topology_replayer(storage, _Placement(), pool, serializer)
    replay = asyncio.create_task(replayer._replay_once())
    try:
        await asyncio.wait_for(client.started.wait(), 1)
        assert len(client.requests) == 1
        assert pool.leased == ["node-b"]
    finally:
        release.set()
        await asyncio.wait_for(replay, 1)
    assert len(client.requests) == 1
    assert len(storage.list(Keyspace.META, HintPrefix())) == 3


@pytest.mark.tourillon
async def test_memory_budget_caps_loaded_values_before_network_wait() -> None:
    storage = _FakeStore()
    for index in range(6):
        _seed(storage, _record_at(index))
    serializer = _PickleSerializer()
    release = asyncio.Event()
    client = _FakeClient(serializer, None, release)
    replayer = _topology_replayer(
        storage,
        _Placement(),
        _FakePeerPool(client),
        serializer,
        max_batch_bytes=16 * 1024 * 1024,
    )
    replay = asyncio.create_task(replayer._replay_once())
    try:
        await asyncio.wait_for(client.started.wait(), 1)
        assert storage.reads(Keyspace.LOGS) == 1
    finally:
        release.set()
        await asyncio.wait_for(replay, 1)
    assert len(client.requests) == 6
    assert storage.list(Keyspace.META, HintPrefix()) == []


@pytest.mark.tourillon
async def test_local_window_batches_reads_promotions_and_satisfaction() -> None:
    storage = _FakeStore()
    for index in range(3):
        _seed(storage, _record_at(index), intent="departed")
    provider = _FakeSessionProvider(storage)
    serializer = _PickleSerializer()
    replayer = _topology_replayer(
        storage,
        _Placement(_owner("node-a")),
        _FakePeerPool({}),
        serializer,
        provider=provider,
    )

    await replayer._replay_once()

    assert provider.write_sessions == 2
    assert provider.opened == 4
    # Conditional insertion, tag promotion and digest flush add three batches.
    assert storage.batches == 9
    assert replayer._stats.applied_total == 3
    assert storage.list(Keyspace.META, HintPrefix()) == []


@pytest.mark.tourillon
async def test_stop_cancels_all_destination_workers_without_satisfying_debt() -> None:
    storage = _FakeStore()
    _seed(storage, _record(), intent="departed")
    before = storage.list(Keyspace.META, HintPrefix())
    serializer = _PickleSerializer()
    clients = [_FakeClient(serializer, None, asyncio.Event()) for _ in range(2)]
    replayer = _topology_replayer(
        storage,
        _Placement(_owner("node-b"), _owner("node-c")),
        _FakePeerPool(dict(zip(("node-b", "node-c"), clients, strict=True))),
        serializer,
    )
    try:
        await replayer.start()
        await asyncio.wait_for(
            asyncio.gather(*(client.started.wait() for client in clients)), 1
        )
    finally:
        await replayer.stop()
    assert storage.list(Keyspace.META, HintPrefix()) == before
    assert replayer._task is None


@pytest.mark.tourillon
@pytest.mark.parametrize(
    "options",
    [{"batch_size": 0}, {"concurrency": 0}, {"max_batch_bytes": 1}],
)
def test_replay_rejects_invalid_resource_limits(options: dict[str, int]) -> None:
    serializer = _PickleSerializer()
    with pytest.raises(ValueError):
        _topology_replayer(
            _FakeStore(), _Placement(), _FakePeerPool({}), serializer, **options
        )


@pytest.mark.tourillon
@pytest.mark.parametrize("local_owner", [False, True])
@pytest.mark.parametrize("owned", [False, True])
async def test_unwritable_new_owner_only_satisfies_debt_if_already_owned(
    local_owner: bool, owned: bool
) -> None:
    storage = _FakeStore()
    record = _record()
    _seed(storage, record, intent="departed")
    before = storage.list(Keyspace.META, HintPrefix())
    if local_owner and owned:
        _seed(
            storage,
            Record(key=record.key, value=record.value, tag=Tag.unhint(record.tag)),
        )
    serializer = _PickleSerializer()
    client = _FakeClient(serializer, None, owned=owned)
    pool = _FakePeerPool({"node-b": client})
    node_id = "node-a" if local_owner else "node-b"
    replayer = _topology_replayer(
        storage,
        _Placement(_owner(node_id, writable=False, handoff="substitute")),
        pool,
        serializer,
    )

    await replayer._replay_once()

    assert storage.list(Keyspace.META, HintPrefix()) == ([] if owned else before)
    assert replayer._stats.applied_total == 0
    assert [request.kind for request in client.requests] == (
        [] if local_owner else ["replication.exists"]
    )
    assert "substitute" not in pool.leased


@pytest.mark.tourillon
@pytest.mark.parametrize("local_owner", [False, True])
async def test_unwritable_current_intent_keeps_debt_without_writing(
    local_owner: bool,
) -> None:
    storage = _FakeStore()
    node_id = "node-a" if local_owner else "node-b"
    _seed(storage, _record(), intent=node_id)
    before = storage.list(Keyspace.META, HintPrefix())
    serializer = _PickleSerializer()
    client = _FakeClient(serializer, None, owned=True)
    provider = _FakeSessionProvider(storage)
    replayer = _topology_replayer(
        storage,
        _Placement(_owner(node_id, writable=False, handoff="substitute")),
        _FakePeerPool(client),
        serializer,
        provider=provider,
    )

    await replayer._replay_once()

    assert storage.list(Keyspace.META, HintPrefix()) == before
    assert client.requests == []
    assert provider.write_sessions == 0


@pytest.mark.tourillon
@pytest.mark.parametrize("new_owners", [("node-d",), ("node-b",), ()])
@pytest.mark.parametrize("owned", [False, True])
async def test_owner_set_change_during_fanout_keeps_original_obligation(
    new_owners: tuple[str, ...], owned: bool
) -> None:
    storage = _FakeStore()
    record = _record()
    _seed(storage, record)
    before = storage.list(Keyspace.META, HintPrefix())
    serializer = _PickleSerializer()
    placement = _Placement(_owner("node-c"))

    def change_topology(request: Envelope) -> None:
        if request.kind == ("replication.exists" if owned else "replication.write"):
            placement.preferences = tuple(_owner(node) for node in new_owners)

    current = _FakeClient(serializer, None, owned=owned, on_request=change_topology)
    returning = _FakeClient(serializer, None)
    pool = _FakePeerPool({"node-c": current, "node-b": returning})
    replayer = _topology_replayer(storage, placement, pool, serializer)

    await replayer._replay_once()

    assert storage.list(Keyspace.META, HintPrefix()) == before
    assert _stored_tag(storage, record) == record.tag
    if new_owners == ("node-b",):
        await replayer._replay_once()
        assert [request.kind for request in returning.requests] == ["replication.write"]
        assert storage.list(Keyspace.META, HintPrefix()) == []


@pytest.mark.tourillon
async def test_batch_rechecks_earlier_delivery_before_deleting_obligations() -> None:
    storage = _FakeStore()
    first, second = _record_at(1), _record_at(2)
    first_hint = _seed(storage, first, intent="departed")
    assert first_hint is not None
    _seed(storage, second, intent="departed")
    serializer = _PickleSerializer()
    placement = _Placement(_owner("node-b"))

    def change_after_second_delivery(request: Envelope) -> None:
        payload = serializer.decode(request.payload)
        if request.kind == "replication.write" and payload == {
            "record": Record(
                key=second.key, value=second.value, tag=Tag.unhint(second.tag)
            ).to_dict()
        }:
            placement.preferences = (_owner("node-c"),)

    client = _FakeClient(serializer, None, on_request=change_after_second_delivery)
    replayer = _topology_replayer(
        storage, placement, _FakePeerPool(client), serializer, batch_size=8
    )

    await replayer._replay_once()

    assert (
        storage.get(
            KVRef(HintKey(first_hint.intent, first_hint.hlc).to_bytes(), Keyspace.META)
        )
        is not None
    )
    assert len(storage.list(Keyspace.META, HintPrefix())) == 2


@pytest.mark.tourillon
async def test_replay_deletes_only_the_successful_intent_for_a_shared_version() -> None:
    storage = _FakeStore()
    record = _record()
    _seed(storage, record, intent="node-b")
    _seed(storage, record, intent="node-c")
    remaining = storage.list(Keyspace.META, HintPrefix("node-c"))
    serializer = _PickleSerializer()
    accepted = _FakeClient(serializer, None)
    rejected = _FakeClient(serializer, "unexpected.kind")
    pool = _FakePeerPool({"node-b": accepted, "node-c": rejected})
    replayer = _topology_replayer(
        storage, _Placement(_owner("node-b"), _owner("node-c")), pool, serializer
    )

    await replayer._replay_once()

    assert storage.list(Keyspace.META, HintPrefix()) == remaining
    assert [request.kind for request in accepted.requests] == ["replication.write"]
    assert [request.kind for request in rejected.requests] == ["replication.write"]
    assert _stored_tag(storage, record) == record.tag


@pytest.mark.tourillon
async def test_concurrent_obligation_for_same_intent_is_not_deleted() -> None:
    storage = _FakeStore()
    record = _record()
    _seed(storage, record)
    concurrent = KVPair(
        key=HintKey(
            "node-b", HLCTimestamp(wall_ms=1, counter=0, node_id="new-obligation")
        ).to_bytes(),
        value=HintReference(record.key).to_bytes(),
        keyspace=Keyspace.META,
    )
    serializer = _PickleSerializer()

    def receive_another_hint(request: Envelope) -> None:
        assert request.kind == "replication.write"
        storage.put(concurrent)

    client = _FakeClient(serializer, None, on_request=receive_another_hint)
    replayer = _topology_replayer(
        storage, _Placement(), _FakePeerPool(client), serializer
    )

    await replayer._replay_once()

    assert storage.list(Keyspace.META, HintPrefix()) == [concurrent]
    assert len(client.requests) == 1
    assert _stored_tag(storage, record) == record.tag


@pytest.mark.tourillon
async def test_direct_delivery_keeps_debt_if_original_owner_leaves_during_write() -> (
    None
):
    storage = _FakeStore()
    _seed(storage, _record())
    before = storage.list(Keyspace.META, HintPrefix())
    serializer = _PickleSerializer()
    placement = _Placement()

    def remove_intent(request: Envelope) -> None:
        assert request.kind == "replication.write"
        placement.preferences = (_owner("node-c"),)

    client = _FakeClient(serializer, None, on_request=remove_intent)
    replayer = _topology_replayer(storage, placement, _FakePeerPool(client), serializer)

    await replayer._replay_once()

    assert storage.list(Keyspace.META, HintPrefix()) == before
    assert len(client.requests) == 1


@pytest.mark.tourillon
def test_target_backoff_defers_after_failure_and_resets_after_success() -> None:
    backoff = _TargetBackoff(BackoffRetry(initial_delay=30.0, jitter=0.0))

    assert backoff.deferred("node-b") is False
    backoff.failed("node-b")
    assert backoff.deferred("node-b") is True
    assert backoff.deferred("node-c") is False

    backoff.succeeded("node-b")
    assert backoff.deferred("node-b") is False


@pytest.mark.tourillon
def test_target_backoff_grows_the_delay_while_the_target_stays_down() -> None:
    backoff = _TargetBackoff(
        BackoffRetry(initial_delay=1.0, factor=2.0, max_delay=4.0, jitter=0.0)
    )

    delays = []
    for _ in range(4):
        backoff.failed("node-b")
        delays.append(round(backoff._until["node-b"] - time.monotonic()))

    assert delays == [1, 2, 4, 4]
