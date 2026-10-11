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
from tourillon.core.models.storageschema import Key, PartitionAddress, Pid, Record, Tag, TagKind, Value
from tourlib.adapter import MsgPackSerializer
from tourlib.models.ts import HLCTimestamp
from tourlib.models.version import Address


class _StoreSpy:
    def __init__(self) -> None:
        self.read_requests: list[PartitionAddress] = []
        self.write_requests: list[Record] = []
        self.read_error: StoreOperationError | None = None
        self.write_error: StoreOperationError | None = None

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


class _ManagerStub:
    def __init__(self) -> None:
        self.serializer = MsgPackSerializer()
        self.store = _StoreSpy()


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
@pytest.mark.asyncio
async def test_replication_read_handler_decodes_and_returns_accept(monkeypatch: pytest.MonkeyPatch) -> None:
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
    assert manager.store.read_requests[0].address == Address(namespace=b"ns", key=b"key")


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_replication_write_handler_decodes_and_returns_version(monkeypatch: pytest.MonkeyPatch) -> None:
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
    assert manager.store.write_requests[0].key.address == Address(namespace=b"ns", key=b"key")



@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_replication_write_handler_rejects_invalid_payload(monkeypatch: pytest.MonkeyPatch) -> None:
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
async def test_replication_read_handler_returns_reject_for_aggregate_error(monkeypatch: pytest.MonkeyPatch) -> None:
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
async def test_replication_write_handler_returns_reject_for_aggregate_error(monkeypatch: pytest.MonkeyPatch) -> None:
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




