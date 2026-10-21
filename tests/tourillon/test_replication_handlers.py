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

import pytest

import tourillon.bootstrap.handlers.replication as replication_handler
from tests.helpers import make_envelope
from tourillon.core.exceptions import StoreOperationError
from tourillon.core.models.storageschema import (
    Key,
    PartitionAddress,
    Pid,
    Record,
    Tag,
    TagKind,
    Value,
)
from tourlib.adapter import MsgPackSerializer
from tourlib.models.ts import HLCTimestamp
from tourlib.models.version import Address


class _StoreSpy:
    def __init__(self) -> None:
        self.read_requests: list[PartitionAddress] = []
        self.write_requests: list[Record] = []
        self.hint_requests: list[tuple[Record, str]] = []
        self.read_error: StoreOperationError | None = None
        self.write_error: StoreOperationError | None = None
        self.exists_requests: list[Key] = []
        self.owned = False

    async def exists(self, key: Key) -> bool:
        if self.read_error is not None:
            raise self.read_error
        self.exists_requests.append(key)
        return self.owned

    async def read(self, request: PartitionAddress) -> Record | None:
        if self.read_error is not None:
            raise self.read_error
        self.read_requests.append(request)
        return None

    async def write(self, request: Record) -> Record:
        if self.write_error is not None:
            raise self.write_error
        self.write_requests.append(request)
        return request

    async def hint(self, record: Record, intent: str) -> Record:
        if self.write_error is not None:
            raise self.write_error
        self.hint_requests.append((record, intent))
        return record


class _ManagerStub:
    def __init__(self) -> None:
        self.serializer = MsgPackSerializer()
        self.store = _StoreSpy()


@pytest.mark.tourillon
@pytest.mark.parametrize("owned", [False, True])
async def test_exists_handler_returns_only_presence_and_preserves_envelope(
    monkeypatch: pytest.MonkeyPatch, owned: bool
) -> None:
    manager = _ManagerStub()
    manager.store.owned = owned
    monkeypatch.setattr(replication_handler, "get_manager", lambda: manager)
    key = _record().key
    request = make_envelope(
        kind="replication.exists",
        payload=manager.serializer.encode({"key": key.to_dict()}),
    )
    responses = [response async for response in replication_handler.exists(request)]
    assert len(responses) == 1
    response = responses[0]
    assert response.kind == "replication.exists.accept"
    assert manager.serializer.decode(response.payload) == {"owned": owned}
    assert manager.store.exists_requests == [key]
    assert response.correlation_id == request.correlation_id
    assert response.proto_version == request.proto_version
    assert response.schema_id == request.schema_id
    assert response.mode == request.mode


@pytest.mark.tourillon
async def test_exists_handler_returns_domain_rejection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = _ManagerStub()
    manager.store.read_error = _replication_error("exists")
    monkeypatch.setattr(replication_handler, "get_manager", lambda: manager)
    request = make_envelope(
        kind="replication.exists",
        payload=manager.serializer.encode({"key": _record().key.to_dict()}),
    )
    responses = [response async for response in replication_handler.exists(request)]
    assert responses[0].kind == "replication.exists.reject"
    assert manager.serializer.decode(responses[0].payload)["operation"] == "exists"
    assert manager.store.exists_requests == []


@pytest.mark.tourillon
async def test_exists_handler_rejects_missing_version_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = _ManagerStub()
    monkeypatch.setattr(replication_handler, "get_manager", lambda: manager)
    request = make_envelope(
        kind="replication.exists", payload=manager.serializer.encode({})
    )
    with pytest.raises(KeyError, match="key"):
        _ = [response async for response in replication_handler.exists(request)]
    assert manager.store.exists_requests == []


def _record() -> Record:
    return Record(
        key=Key(
            pid=Pid(7),
            address=Address(namespace=b"ns", key=b"key"),
            ts=HLCTimestamp(wall_ms=3, counter=0, node_id="node-a"),
        ),
        value=Value(data=b"value", quorum_write=1),
        tag=Tag(kind=TagKind.LIVE),
    )


def _replication_error(operation: str) -> StoreOperationError:
    return StoreOperationError(
        operation=operation,
        message=f"{operation} quorum not met",
        errors=[RuntimeError("node-b unavailable"), RuntimeError("node-c unavailable")],
    )


@pytest.mark.tourillon
async def test_hint_handler_passes_required_intent_and_preserves_envelope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = _ManagerStub()
    monkeypatch.setattr(replication_handler, "get_manager", lambda: manager)
    original = _record()
    record = Record(key=original.key, value=original.value, tag=Tag.hint(original.tag))
    request = make_envelope(
        kind="replication.hint",
        payload=manager.serializer.encode(
            {"record": record.to_dict(), "intent": "node-b"}
        ),
    )
    responses = [response async for response in replication_handler.hint(request)]
    assert len(responses) == 1
    response = responses[0]
    assert response.kind == "replication.hint.accept"
    assert response.correlation_id == request.correlation_id
    assert response.schema_id == request.schema_id
    assert response.proto_version == request.proto_version
    assert response.mode == request.mode
    assert manager.serializer.decode(response.payload) == record.to_dict()
    assert manager.store.hint_requests == [(record, "node-b")]
    assert manager.store.write_requests == []


@pytest.mark.tourillon
async def test_hint_handler_rejects_draining_without_acknowledging_delivery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = _ManagerStub()
    manager.store.write_error = _replication_error("replication.hint")
    monkeypatch.setattr(replication_handler, "get_manager", lambda: manager)
    request = make_envelope(
        kind="replication.hint",
        payload=manager.serializer.encode(
            {"record": _record().to_dict(), "intent": "node-b"}
        ),
    )
    responses = [response async for response in replication_handler.hint(request)]
    assert responses[0].kind == "replication.hint.reject"
    assert (
        manager.serializer.decode(responses[0].payload)["operation"]
        == "replication.hint"
    )
    assert manager.store.hint_requests == []


@pytest.mark.tourillon
async def test_hint_handler_requires_intent(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = _ManagerStub()
    monkeypatch.setattr(replication_handler, "get_manager", lambda: manager)
    request = make_envelope(
        kind="replication.hint",
        payload=manager.serializer.encode({"record": _record().to_dict()}),
    )
    with pytest.raises(KeyError, match="intent"):
        _ = [response async for response in replication_handler.hint(request)]
    assert manager.store.hint_requests == []


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_replication_read_handler_decodes_and_returns_accept(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = _ManagerStub()
    monkeypatch.setattr(replication_handler, "get_manager", lambda: manager)

    request = make_envelope(
        kind="replication.read",
        payload=manager.serializer.encode(
            {
                "addr": {
                    "pid": 7,
                    "address": {"namespace": b"ns", "key": b"key"},
                }
            }
        ),
    )

    responses = [response async for response in replication_handler.read(request)]

    assert len(responses) == 1
    assert responses[0].kind == "replication.read.accept"
    assert manager.serializer.decode(responses[0].payload) is None
    assert manager.store.read_requests[0].address == Address(
        namespace=b"ns", key=b"key"
    )


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_replication_write_handler_decodes_and_returns_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = _ManagerStub()
    monkeypatch.setattr(replication_handler, "get_manager", lambda: manager)

    record = _record()
    request = make_envelope(
        kind="replication.write",
        payload=manager.serializer.encode({"record": record.to_dict()}),
    )

    responses = [response async for response in replication_handler.write(request)]
    payload = manager.serializer.decode(responses[0].payload)

    assert responses[0].kind == "replication.write.accept"
    assert payload["value"]["data"] == b"value"
    assert manager.store.write_requests[0].key.address == Address(
        namespace=b"ns", key=b"key"
    )


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_replication_write_handler_rejects_invalid_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = _ManagerStub()
    monkeypatch.setattr(replication_handler, "get_manager", lambda: manager)

    request = make_envelope(
        kind="replication.write",
        payload=manager.serializer.encode({"bad": "payload"}),
    )

    with pytest.raises(KeyError, match="record"):
        _ = [response async for response in replication_handler.write(request)]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_replication_read_handler_returns_reject_for_aggregate_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = _ManagerStub()
    manager.store.read_error = _replication_error("read")
    monkeypatch.setattr(replication_handler, "get_manager", lambda: manager)

    request = make_envelope(
        kind="replication.read",
        payload=manager.serializer.encode(
            {
                "addr": {
                    "pid": 7,
                    "address": {"namespace": b"ns", "key": b"key"},
                }
            }
        ),
    )

    responses = [response async for response in replication_handler.read(request)]
    payload = manager.serializer.decode(responses[0].payload)

    assert responses[0].kind == "replication.read.reject"
    assert payload["operation"] == "read"
    assert payload["errors"][0]["message"] == "node-b unavailable"


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_replication_write_handler_returns_reject_for_aggregate_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = _ManagerStub()
    manager.store.write_error = _replication_error("write")
    monkeypatch.setattr(replication_handler, "get_manager", lambda: manager)

    record = _record()
    request = make_envelope(
        kind="replication.write",
        payload=manager.serializer.encode({"record": record.to_dict()}),
    )

    responses = [response async for response in replication_handler.write(request)]
    payload = manager.serializer.decode(responses[0].payload)

    assert responses[0].kind == "replication.write.reject"
    assert payload["operation"] == "write"
    assert payload["errors"][1]["message"] == "node-c unavailable"


class _LookupSpy(_StoreSpy):
    def __init__(self, record: Record | None) -> None:
        super().__init__()
        self.record = record
        self.lookups: list[Key] = []
        self.lookup_error: StoreOperationError | None = None

    async def read_at(self, key: Key) -> Record | None:
        if self.lookup_error is not None:
            raise self.lookup_error
        self.lookups.append(key)
        return self.record


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_replication_lookup_handler_returns_the_exact_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = _ManagerStub()
    record = _record()
    manager.store = _LookupSpy(record)
    monkeypatch.setattr(replication_handler, "get_manager", lambda: manager)

    request = make_envelope(
        kind="replication.lookup",
        payload=manager.serializer.encode({"key": record.key.to_dict()}),
    )

    responses = [response async for response in replication_handler.lookup(request)]
    payload = manager.serializer.decode(responses[0].payload)

    assert responses[0].kind == "replication.lookup.accept"
    assert Record.from_dict(payload) == record
    assert manager.store.lookups == [record.key]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_replication_lookup_handler_answers_none_when_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = _ManagerStub()
    manager.store = _LookupSpy(None)
    monkeypatch.setattr(replication_handler, "get_manager", lambda: manager)

    request = make_envelope(
        kind="replication.lookup",
        payload=manager.serializer.encode({"key": _record().key.to_dict()}),
    )

    responses = [response async for response in replication_handler.lookup(request)]

    assert responses[0].kind == "replication.lookup.accept"
    assert manager.serializer.decode(responses[0].payload) is None


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_replication_lookup_handler_returns_reject_on_store_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = _ManagerStub()
    spy = _LookupSpy(None)
    spy.lookup_error = _replication_error("read")
    manager.store = spy
    monkeypatch.setattr(replication_handler, "get_manager", lambda: manager)

    request = make_envelope(
        kind="replication.lookup",
        payload=manager.serializer.encode({"key": _record().key.to_dict()}),
    )

    responses = [response async for response in replication_handler.lookup(request)]
    payload = manager.serializer.decode(responses[0].payload)

    assert responses[0].kind == "replication.lookup.reject"
    assert payload["operation"] == "read"
