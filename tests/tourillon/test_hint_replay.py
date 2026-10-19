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

import pickle
import time
from contextlib import asynccontextmanager
from typing import cast

import pytest

from tourillon.core.models.retry import BackoffRetry
from tourillon.core.models.storageschema import (
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
from tourillon.core.replay.hint import HintReplayer, _TargetBackoff
from tourlib.envelope import Envelope, build_envelope
from tourlib.models.ts import HLCTimestamp
from tourlib.models.version import Address


class _FakeStore:
    def __init__(self) -> None:
        self._items: dict[tuple[Keyspace, bytes], KVPair] = {}
        self.gets: list[tuple[Keyspace, bytes]] = []

    def put(self, kv: KVPair) -> None:
        self._items[(kv.keyspace, kv.key)] = kv

    def get(self, ref: KVRef) -> KVPair | None:
        self.gets.append((ref.keyspace, ref.key))
        return self._items.get((ref.keyspace, ref.key))

    def reads(self, keyspace: Keyspace) -> int:
        return sum(1 for ks, _ in self.gets if ks == keyspace)

    def list(self, keyspace: Keyspace) -> list[KVPair]:
        items = [kv for (ks, _), kv in self._items.items() if ks == keyspace]
        return sorted(items, key=lambda kv: kv.key)


class _FakeBatch:
    def __init__(self, store: _FakeStore) -> None:
        self._store = store
        self._puts: list[KVPair] = []
        self._gets: list[KVRef] = []

    async def __aenter__(self) -> "_FakeBatch":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        return False

    def get(self, ref: KVRef) -> None:
        self._gets.append(ref)

    def put(self, kv: KVPair) -> None:
        self._puts.append(kv)

    async def result(self):
        for kv in self._puts:
            self._store.put(kv)
        return [self._store.get(ref) for ref in self._gets] + [None] * len(self._puts)


class _FakeSession:
    def __init__(self, store: _FakeStore) -> None:
        self._store = store

    async def get(self, ref: KVRef) -> KVPair | None:
        return self._store.get(ref)

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

    def batch(self):
        return _FakeBatch(self._store)


class _FakeSessionProvider:
    group_id = 0

    def __init__(self, store: _FakeStore) -> None:
        self._store = store
        self.write_sessions = 0

    @asynccontextmanager
    async def session(self, write: bool = False):
        if write:
            self.write_sessions += 1
        yield _FakeSession(self._store)

    async def close(self) -> None:
        return None


class _FakeStorage:
    def __init__(self, provider: _FakeSessionProvider) -> None:
        self._provider = provider

    async def session_provider_for(self, group: int) -> _FakeSessionProvider:
        assert group == 0
        return self._provider


class _PickleSerializer:
    schema_id = 1

    def encode(self, document: object) -> bytes:
        return pickle.dumps(document)

    def decode(self, payload: bytes) -> object:
        return pickle.loads(payload)


class _FakeClient:
    def __init__(self, serializer: _PickleSerializer, response_kind: str) -> None:
        self._serializer = serializer
        self._response_kind = response_kind
        self.requests: list[Envelope] = []

    async def request(self, envelope: Envelope, timeout: float = 30.0) -> Envelope:
        del timeout
        self.requests.append(envelope)
        return build_envelope(
            kind=self._response_kind,
            payload=self._serializer.encode({}),
            schema_id=self._serializer.schema_id,
        )


class _FakePeerPool:
    def __init__(self, client: _FakeClient) -> None:
        self._client = client
        self.leased: list[str] = []

    @asynccontextmanager
    async def lease(self, peer_id: str):
        self.leased.append(peer_id)
        yield self._client


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
        tag=Tag.hint(Tag(kind=TagKind.LIVE), "node-b"),
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
        tag=Tag.hint(Tag(kind=TagKind.LIVE), "node-b"),
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


def _seed(store: _FakeStore, record: Record) -> None:
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
        total_groups=1,
    )
    await replayer._replay_once()
    assert _stored_tag(store, record).kind == TagKind.HINT


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_replay_once_restores_original_tag_after_successful_write() -> None:
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
        total_groups=1,
    )

    await replayer._replay_once()

    assert client.requests[0].kind == "replication.write"
    original = Record(key=record.key, value=record.value, tag=Tag.unhint(record.tag))
    assert serializer.decode(client.requests[0].payload) == {
        "record": original.to_dict()
    }
    assert _stored_tag(store, record) == Tag(kind=TagKind.LIVE)
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
        total_groups=1,
        batch_size=8,
    )

    await replayer._replay_once()

    assert provider.write_sessions == 1
    assert all(
        _stored_tag(store, record) == Tag(kind=TagKind.LIVE) for record in records
    )


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
        total_groups=1,
    )

    await replayer._replay_once()

    assert store.reads(Keyspace.LOGS) == 1
    assert _stored_tag(store, hint) == Tag(kind=TagKind.LIVE)


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
        total_groups=1,
    )

    await replayer._replay_once()

    assert pool.leased == ["node-b"]
    assert all(_stored_tag(store, record).kind == TagKind.HINT for record in hints)
    assert provider.write_sessions == 0


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
