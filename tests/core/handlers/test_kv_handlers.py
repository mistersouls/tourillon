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
"""Unit tests for KV envelope handlers (kv.put, kv.get, kv.delete, etc.)."""

from __future__ import annotations

import asyncio
import uuid

import pytest

from tourillon.core.handlers.kv import (
    KvDeleteHandler,
    KvFetchHandler,
    KvGetHandler,
    KvHintHandler,
    KvPutHandler,
    KvReplicateHandler,
)
from tourillon.core.kv.coordinator import KvError
from tourillon.core.ring.hashspace import HashSpace
from tourillon.core.ring.partitioner import Partitioner
from tourillon.core.structure.clock import HLCTimestamp
from tourillon.core.structure.envelope import Envelope
from tourillon.core.structure.record import KvMetadata, StoreKey, Tombstone, Version
from tourillon.core.testing.mem_storage import InMemoryStorage
from tourillon.infra.serializer.msgpack import MsgpackSerializerAdapter

pytestmark = pytest.mark.kv

_SHIFT = 4
_NODE_ID = "node-1"


# ---------------------------------------------------------------------------
# Mock helpers
# ---------------------------------------------------------------------------


def _env(kind: str, payload: bytes, corr: uuid.UUID | None = None) -> Envelope:
    """Build an Envelope with the given kind and payload."""
    return Envelope.create(payload, kind=kind, correlation_id=corr or uuid.uuid4())


def _make_receive(env: Envelope):
    """Return an async receive callable that yields *env* once."""

    async def receive() -> Envelope:
        return env

    return receive


class _Sink:
    """Collect sent envelopes."""

    def __init__(self) -> None:
        self.sent: list[Envelope] = []

    async def __call__(self, env: Envelope) -> None:
        self.sent.append(env)

    @property
    def last(self) -> Envelope:
        return self.sent[-1]


# ---------------------------------------------------------------------------
# Coordinator stub for handler tests (avoids full ring setup)
# ---------------------------------------------------------------------------


class _StubCoordinator:
    """Minimal coordinator stub for KvPut/Get/Delete handler tests."""

    def __init__(
        self,
        put_result: dict | None = None,
        get_result: dict | None = None,
        delete_result: dict | None = None,
        raise_on: str | None = None,
    ) -> None:
        self._put_result = put_result or {
            "hlc": {"wall": 1000, "counter": 0, "node_id": "n1"},
            "replicas": 1,
        }
        self._get_result = get_result or {"versions": []}
        self._delete_result = delete_result or {
            "hlc": {"wall": 2000, "counter": 0, "node_id": "n1"},
            "replicas": 1,
        }
        self._raise_on = raise_on

    async def put(self, key, keyspace, value, quorum_write):
        if self._raise_on == "put":
            raise KvError("quorum_write_unavailable (1/2 acks, timeout after 50 ms)")
        return self._put_result

    async def get(self, key, keyspace, quorum_read):
        if self._raise_on == "get":
            raise KvError("quorum_read_unavailable (1/2 replicas responded)")
        return self._get_result

    async def delete(self, key, keyspace, quorum_write):
        if self._raise_on == "delete":
            raise KvError("quorum_write_unavailable (0/1 acks, timeout after 50 ms)")
        return self._delete_result


# ---------------------------------------------------------------------------
# Storage + partitioner fixture used by replicate/hint/fetch handlers
# ---------------------------------------------------------------------------


def _build_store_infra() -> tuple[InMemoryStorage, Partitioner]:
    hs = HashSpace()
    partitioner = Partitioner(hs, _SHIFT)
    storage = InMemoryStorage()
    return storage, partitioner


def _make_replicate_payload(
    key: bytes,
    ks: bytes,
    val: bytes | None,
    hlc: HLCTimestamp,
    qw: int,
    serializer: MsgpackSerializerAdapter,
) -> bytes:
    return serializer.encode(
        {
            "key": key,
            "keyspace": ks,
            "value": val,
            "hlc": hlc.to_dict(),
            "quorum_write": qw,
        }
    )


# ---------------------------------------------------------------------------
# KvPutHandler — success and error paths
# ---------------------------------------------------------------------------


@pytest.mark.kv
async def test_kv_put_handler_success_sends_put_ok() -> None:
    """KvPutHandler success path → kv.put.ok with matching correlation_id."""
    serializer = MsgpackSerializerAdapter()
    corr = uuid.uuid4()
    payload = serializer.encode({"key": b"k", "keyspace": b"default", "value": b"v"})
    env = _env("kv.put", payload, corr)

    handler = KvPutHandler(_StubCoordinator(), serializer)
    sink = _Sink()
    await handler(_make_receive(env), sink)

    assert sink.last.kind == "kv.put.ok"
    assert sink.last.correlation_id == corr


@pytest.mark.kv
async def test_kv_put_handler_missing_key_sends_error() -> None:
    """KvPutHandler with empty key → kv.error with invalid_key."""
    serializer = MsgpackSerializerAdapter()
    payload = serializer.encode({"key": b"", "keyspace": b"default", "value": b"v"})
    env = _env("kv.put", payload)

    handler = KvPutHandler(_StubCoordinator(), serializer)
    sink = _Sink()
    await handler(_make_receive(env), sink)

    assert sink.last.kind == "kv.error"
    data = serializer.decode(sink.last.payload)
    assert data["reason"] == "invalid_key"


@pytest.mark.kv
async def test_kv_put_handler_invalid_quorum_sends_error() -> None:
    """KvPutHandler with quorum_write=0 → kv.error with invalid_quorum."""
    serializer = MsgpackSerializerAdapter()
    payload = serializer.encode(
        {"key": b"k", "keyspace": b"default", "value": b"v", "quorum_write": 0}
    )
    env = _env("kv.put", payload)

    handler = KvPutHandler(_StubCoordinator(), serializer)
    sink = _Sink()
    await handler(_make_receive(env), sink)

    assert sink.last.kind == "kv.error"
    data = serializer.decode(sink.last.payload)
    assert data["reason"] == "invalid_quorum"


@pytest.mark.kv
async def test_kv_put_handler_coordinator_error_sends_kv_error() -> None:
    """KvPutHandler coordinator raises KvError → kv.error response."""
    serializer = MsgpackSerializerAdapter()
    payload = serializer.encode({"key": b"k", "keyspace": b"default", "value": b"v"})
    env = _env("kv.put", payload)

    handler = KvPutHandler(_StubCoordinator(raise_on="put"), serializer)
    sink = _Sink()
    await handler(_make_receive(env), sink)

    assert sink.last.kind == "kv.error"


# ---------------------------------------------------------------------------
# KvGetHandler — success and error paths
# ---------------------------------------------------------------------------


@pytest.mark.kv
async def test_kv_get_handler_success_sends_get_ok() -> None:
    """KvGetHandler success path → kv.get.ok with matching correlation_id."""
    serializer = MsgpackSerializerAdapter()
    corr = uuid.uuid4()
    versions_result: dict = {
        "versions": [
            {
                "confirmed": True,
                "value": b"val",
                "hlc": {"wall": 1000, "counter": 0, "node_id": "n1"},
                "count": 1,
                "quorum_write": 1,
            }
        ]
    }
    payload = serializer.encode({"key": b"k", "keyspace": b"default"})
    env = _env("kv.get", payload, corr)

    handler = KvGetHandler(_StubCoordinator(get_result=versions_result), serializer)
    sink = _Sink()
    await handler(_make_receive(env), sink)

    assert sink.last.kind == "kv.get.ok"
    assert sink.last.correlation_id == corr
    data = serializer.decode(sink.last.payload)
    assert len(data["versions"]) == 1


@pytest.mark.kv
async def test_kv_get_handler_missing_key_sends_error() -> None:
    """KvGetHandler with no key → kv.error with invalid_key."""
    serializer = MsgpackSerializerAdapter()
    payload = serializer.encode({"key": b"", "keyspace": b"default"})
    env = _env("kv.get", payload)

    handler = KvGetHandler(_StubCoordinator(), serializer)
    sink = _Sink()
    await handler(_make_receive(env), sink)

    assert sink.last.kind == "kv.error"


@pytest.mark.kv
async def test_kv_get_handler_coordinator_error_sends_kv_error() -> None:
    """KvGetHandler coordinator raises KvError → kv.error sent."""
    serializer = MsgpackSerializerAdapter()
    payload = serializer.encode({"key": b"k", "keyspace": b"default"})
    env = _env("kv.get", payload)

    handler = KvGetHandler(_StubCoordinator(raise_on="get"), serializer)
    sink = _Sink()
    await handler(_make_receive(env), sink)

    assert sink.last.kind == "kv.error"


# ---------------------------------------------------------------------------
# KvDeleteHandler — success and error paths
# ---------------------------------------------------------------------------


@pytest.mark.kv
async def test_kv_delete_handler_success_sends_delete_ok() -> None:
    """KvDeleteHandler success path → kv.delete.ok with matching correlation_id."""
    serializer = MsgpackSerializerAdapter()
    corr = uuid.uuid4()
    payload = serializer.encode({"key": b"k", "keyspace": b"default"})
    env = _env("kv.delete", payload, corr)

    handler = KvDeleteHandler(_StubCoordinator(), serializer)
    sink = _Sink()
    await handler(_make_receive(env), sink)

    assert sink.last.kind == "kv.delete.ok"
    assert sink.last.correlation_id == corr


@pytest.mark.kv
async def test_kv_delete_handler_missing_key_sends_error() -> None:
    """KvDeleteHandler with empty key → kv.error."""
    serializer = MsgpackSerializerAdapter()
    payload = serializer.encode({"key": b"", "keyspace": b"default"})
    env = _env("kv.delete", payload)

    handler = KvDeleteHandler(_StubCoordinator(), serializer)
    sink = _Sink()
    await handler(_make_receive(env), sink)

    assert sink.last.kind == "kv.error"


@pytest.mark.kv
async def test_kv_delete_handler_coordinator_error_sends_kv_error() -> None:
    """KvDeleteHandler coordinator raises KvError → kv.error sent."""
    serializer = MsgpackSerializerAdapter()
    payload = serializer.encode({"key": b"k", "keyspace": b"default"})
    env = _env("kv.delete", payload)

    handler = KvDeleteHandler(_StubCoordinator(raise_on="delete"), serializer)
    sink = _Sink()
    await handler(_make_receive(env), sink)

    assert sink.last.kind == "kv.error"


# ---------------------------------------------------------------------------
# KvReplicateHandler — write value and tombstone
# ---------------------------------------------------------------------------


@pytest.mark.kv
async def test_kv_replicate_handler_write_value_stores_and_acks() -> None:
    """kv.replicate write → kv.replicate.ok, Version stored in PartitionStore."""
    serializer = MsgpackSerializerAdapter()
    storage, partitioner = _build_store_infra()
    hlc = HLCTimestamp(wall_ms=100, counter=0, node_id=_NODE_ID)
    addr = StoreKey(keyspace=b"default", key=b"r-key")

    payload = _make_replicate_payload(b"r-key", b"default", b"val", hlc, 1, serializer)
    corr = uuid.uuid4()
    env = _env("kv.replicate", payload, corr)

    handler = KvReplicateHandler(_NODE_ID, storage, partitioner, serializer)
    sink = _Sink()
    await handler(_make_receive(env), sink)

    assert sink.last.kind == "kv.replicate.ok"
    assert sink.last.correlation_id == corr

    pid = partitioner.pid_for_addr(addr)
    record = await storage.open_partition(pid).get(addr)
    assert isinstance(record, Version)
    assert record.value == b"val"


@pytest.mark.kv
async def test_kv_replicate_handler_null_value_writes_tombstone() -> None:
    """kv.replicate with null value → Tombstone stored."""
    serializer = MsgpackSerializerAdapter()
    storage, partitioner = _build_store_infra()
    hlc = HLCTimestamp(wall_ms=200, counter=0, node_id=_NODE_ID)
    addr = StoreKey(keyspace=b"default", key=b"tomb-key")

    payload = _make_replicate_payload(b"tomb-key", b"default", None, hlc, 1, serializer)
    env = _env("kv.replicate", payload)

    handler = KvReplicateHandler(_NODE_ID, storage, partitioner, serializer)
    sink = _Sink()
    await handler(_make_receive(env), sink)

    assert sink.last.kind == "kv.replicate.ok"
    pid = partitioner.pid_for_addr(addr)
    record = await storage.open_partition(pid).get(addr)
    assert isinstance(record, Tombstone)


@pytest.mark.kv
async def test_kv_replicate_handler_missing_fields_sends_no_response() -> None:
    """kv.replicate missing key/hlc → no response sent."""
    serializer = MsgpackSerializerAdapter()
    storage, partitioner = _build_store_infra()
    payload = serializer.encode({"keyspace": b"default"})  # missing key + hlc
    env = _env("kv.replicate", payload)

    handler = KvReplicateHandler(_NODE_ID, storage, partitioner, serializer)
    sink = _Sink()
    await handler(_make_receive(env), sink)

    assert len(sink.sent) == 0


# ---------------------------------------------------------------------------
# KvHintHandler — put hint and delete hint
# ---------------------------------------------------------------------------


@pytest.mark.kv
async def test_kv_hint_handler_stores_hint_and_acks() -> None:
    """kv.hint put → kv.hint.ok, hint Version stored."""
    serializer = MsgpackSerializerAdapter()
    storage, partitioner = _build_store_infra()
    hlc = HLCTimestamp(wall_ms=300, counter=0, node_id=_NODE_ID)
    addr = StoreKey(keyspace=b"default", key=b"hint-key")

    payload = serializer.encode(
        {
            "key": b"hint-key",
            "keyspace": b"default",
            "value": b"hinted",
            "hlc": hlc.to_dict(),
            "quorum_write": 1,
            "for_node": "node-target",
        }
    )
    corr = uuid.uuid4()
    env = _env("kv.hint", payload, corr)

    handler = KvHintHandler(_NODE_ID, storage, partitioner, serializer)
    sink = _Sink()
    await handler(_make_receive(env), sink)

    assert sink.last.kind == "kv.hint.ok"
    assert sink.last.correlation_id == corr

    pid = partitioner.pid_for_addr(addr)
    hint_ctx = storage.open_partition(pid).hint("node-target")
    records = hint_ctx.hint_records()
    assert len(records) == 1
    assert isinstance(records[0], Version)


@pytest.mark.kv
async def test_kv_hint_handler_null_value_stores_tombstone_hint() -> None:
    """kv.hint with null value → hint Tombstone stored."""
    serializer = MsgpackSerializerAdapter()
    storage, partitioner = _build_store_infra()
    hlc = HLCTimestamp(wall_ms=400, counter=0, node_id=_NODE_ID)
    addr = StoreKey(keyspace=b"default", key=b"del-hint")

    payload = serializer.encode(
        {
            "key": b"del-hint",
            "keyspace": b"default",
            "value": None,
            "hlc": hlc.to_dict(),
            "quorum_write": 1,
            "for_node": "node-target",
        }
    )
    env = _env("kv.hint", payload)

    handler = KvHintHandler(_NODE_ID, storage, partitioner, serializer)
    sink = _Sink()
    await handler(_make_receive(env), sink)

    assert sink.last.kind == "kv.hint.ok"
    pid = partitioner.pid_for_addr(addr)
    hint_ctx = storage.open_partition(pid).hint("node-target")
    records = hint_ctx.hint_records()
    assert len(records) == 1
    assert isinstance(records[0], Tombstone)


@pytest.mark.kv
async def test_kv_hint_handler_missing_for_node_sends_no_response() -> None:
    """kv.hint missing for_node → no response sent."""
    serializer = MsgpackSerializerAdapter()
    storage, partitioner = _build_store_infra()
    hlc = HLCTimestamp(wall_ms=500, counter=0, node_id=_NODE_ID)
    payload = serializer.encode(
        {
            "key": b"k",
            "keyspace": b"default",
            "value": b"v",
            "hlc": hlc.to_dict(),
            "quorum_write": 1,
            # for_node missing
        }
    )
    env = _env("kv.hint", payload)

    handler = KvHintHandler(_NODE_ID, storage, partitioner, serializer)
    sink = _Sink()
    await handler(_make_receive(env), sink)

    assert len(sink.sent) == 0


# ---------------------------------------------------------------------------
# KvFetchHandler — found / not found / tombstone / missing key
# ---------------------------------------------------------------------------


@pytest.mark.kv
async def test_kv_fetch_handler_found_version_returns_fetch_ok() -> None:
    """kv.fetch on stored Version → kv.fetch.ok, found=True, value set."""
    serializer = MsgpackSerializerAdapter()
    storage, partitioner = _build_store_infra()
    hlc = HLCTimestamp(wall_ms=600, counter=0, node_id=_NODE_ID)
    addr = StoreKey(keyspace=b"default", key=b"fetch-key")
    meta = KvMetadata(hlc=hlc, quorum_write=1)
    await storage.open_partition(partitioner.pid_for_addr(addr)).put(
        addr, b"fetched", meta
    )

    payload = serializer.encode({"key": b"fetch-key", "keyspace": b"default"})
    corr = uuid.uuid4()
    env = _env("kv.fetch", payload, corr)

    handler = KvFetchHandler(_NODE_ID, storage, partitioner, serializer)
    sink = _Sink()
    await handler(_make_receive(env), sink)

    assert sink.last.kind == "kv.fetch.ok"
    assert sink.last.correlation_id == corr
    data = serializer.decode(sink.last.payload)
    assert data["found"] is True
    assert data["value"] == b"fetched"


@pytest.mark.kv
async def test_kv_fetch_handler_tombstone_returns_null_value() -> None:
    """kv.fetch on stored Tombstone → kv.fetch.ok, found=True, value=None."""
    serializer = MsgpackSerializerAdapter()
    storage, partitioner = _build_store_infra()
    hlc = HLCTimestamp(wall_ms=700, counter=0, node_id=_NODE_ID)
    addr = StoreKey(keyspace=b"default", key=b"tomb-fetch")
    meta = KvMetadata(hlc=hlc, quorum_write=1)
    await storage.open_partition(partitioner.pid_for_addr(addr)).delete(addr, meta)

    payload = serializer.encode({"key": b"tomb-fetch", "keyspace": b"default"})
    env = _env("kv.fetch", payload)

    handler = KvFetchHandler(_NODE_ID, storage, partitioner, serializer)
    sink = _Sink()
    await handler(_make_receive(env), sink)

    assert sink.last.kind == "kv.fetch.ok"
    data = serializer.decode(sink.last.payload)
    assert data["found"] is True
    assert data["value"] is None


@pytest.mark.kv
async def test_kv_fetch_handler_not_found_returns_found_false() -> None:
    """kv.fetch on missing key → kv.fetch.ok, found=False."""
    serializer = MsgpackSerializerAdapter()
    storage, partitioner = _build_store_infra()

    payload = serializer.encode({"key": b"missing", "keyspace": b"default"})
    env = _env("kv.fetch", payload)

    handler = KvFetchHandler(_NODE_ID, storage, partitioner, serializer)
    sink = _Sink()
    await handler(_make_receive(env), sink)

    assert sink.last.kind == "kv.fetch.ok"
    data = serializer.decode(sink.last.payload)
    assert data["found"] is False
    assert data["value"] is None


@pytest.mark.kv
async def test_kv_fetch_handler_missing_key_field_sends_no_response() -> None:
    """kv.fetch with no key field → no response sent."""
    serializer = MsgpackSerializerAdapter()
    storage, partitioner = _build_store_infra()

    payload = serializer.encode({"keyspace": b"default"})  # key absent
    env = _env("kv.fetch", payload)

    handler = KvFetchHandler(_NODE_ID, storage, partitioner, serializer)
    sink = _Sink()
    await handler(_make_receive(env), sink)

    assert len(sink.sent) == 0


# ---------------------------------------------------------------------------
# Multiple concurrent correlation IDs — each response matches its request
# ---------------------------------------------------------------------------


@pytest.mark.kv
async def test_kv_handlers_multiple_correlation_ids_match() -> None:
    """Each handler echoes back the correct correlation_id for concurrent requests."""
    serializer = MsgpackSerializerAdapter()

    corr_ids = [uuid.uuid4() for _ in range(5)]
    put_handler = KvPutHandler(_StubCoordinator(), serializer)

    async def _invoke(corr: uuid.UUID) -> uuid.UUID:
        payload = serializer.encode(
            {"key": b"k", "keyspace": b"default", "value": b"v"}
        )
        env = _env("kv.put", payload, corr)
        sink = _Sink()
        await put_handler(_make_receive(env), sink)
        return sink.last.correlation_id

    results = await asyncio.gather(*(_invoke(c) for c in corr_ids))
    assert sorted(str(r) for r in results) == sorted(str(c) for c in corr_ids)


@pytest.mark.kv
async def test_kv_get_handler_multiple_correlation_ids_match() -> None:
    """KvGetHandler echoes back the correct correlation_id for 5 concurrent requests."""
    serializer = MsgpackSerializerAdapter()

    versions_result: dict = {"versions": []}
    corr_ids = [uuid.uuid4() for _ in range(5)]
    get_handler = KvGetHandler(_StubCoordinator(get_result=versions_result), serializer)

    async def _invoke(corr: uuid.UUID) -> uuid.UUID:
        payload = serializer.encode({"key": b"k", "keyspace": b"default"})
        env = _env("kv.get", payload, corr)
        sink = _Sink()
        await get_handler(_make_receive(env), sink)
        return sink.last.correlation_id

    results = await asyncio.gather(*(_invoke(c) for c in corr_ids))
    assert sorted(str(r) for r in results) == sorted(str(c) for c in corr_ids)
