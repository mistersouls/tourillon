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

import tourillon.bootstrap.handlers.kv as kv_handler
from tests.helpers import make_envelope
from tourillon.core.exceptions import StoreOperationError
from tourlib.adapter import MsgPackSerializer
from tourlib.models.version import Version


class _StoreSpy:
    def __init__(self) -> None:
        self.put_error: StoreOperationError | None = None
        self.get_error: StoreOperationError | None = None
        self.delete_error: StoreOperationError | None = None

    async def put(self, request) -> Version | None:
        if self.put_error is not None:
            raise self.put_error
        return None

    async def get(self, request) -> Version | None:
        if self.get_error is not None:
            raise self.get_error
        return None

    async def delete(self, request) -> Version | None:
        if self.delete_error is not None:
            raise self.delete_error
        return None


class _ManagerStub:
    def __init__(self) -> None:
        self.serializer = MsgPackSerializer()
        self.store = _StoreSpy()


def _replication_error(operation: str) -> StoreOperationError:
    return StoreOperationError(
        operation=operation,
        message=f"{operation} quorum not met",
        errors=[RuntimeError("node-b unavailable"), RuntimeError("node-c unavailable")],
    )


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_kv_put_handler_returns_reject_for_aggregate_error(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = _ManagerStub()
    manager.store.put_error = _replication_error("write")
    monkeypatch.setattr(kv_handler, "get_manager", lambda: manager)

    request = make_envelope(
        kind="kv.put",
        payload=manager.serializer.encode(
            {
                "namespace": b"ns",
                "key": b"key",
                "value": b"value",
                "quorum_write": 2,
            }
        ),
    )

    responses = [response async for response in kv_handler.put(request)]
    payload = manager.serializer.decode(responses[0].payload)

    assert responses[0].kind == "kv.put.reject"
    assert payload["operation"] == "write"
    assert payload["errors"][0]["message"] == "node-b unavailable"


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_kv_get_handler_returns_reject_for_aggregate_error(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = _ManagerStub()
    manager.store.get_error = _replication_error("read")
    monkeypatch.setattr(kv_handler, "get_manager", lambda: manager)

    request = make_envelope(
        kind="kv.get",
        payload=manager.serializer.encode(
            {
                "namespace": b"ns",
                "key": b"key",
                "quorum_read": 2,
            }
        ),
    )

    responses = [response async for response in kv_handler.get(request)]
    payload = manager.serializer.decode(responses[0].payload)

    assert responses[0].kind == "kv.get.reject"
    assert payload["operation"] == "read"
    assert payload["errors"][1]["message"] == "node-c unavailable"


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_kv_delete_handler_returns_reject_for_aggregate_error(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = _ManagerStub()
    manager.store.delete_error = _replication_error("write")
    monkeypatch.setattr(kv_handler, "get_manager", lambda: manager)

    request = make_envelope(
        kind="kv.delete",
        payload=manager.serializer.encode(
            {
                "namespace": b"ns",
                "key": b"key",
                "quorum_write": 2,
            }
        ),
    )

    responses = [response async for response in kv_handler.delete(request)]
    payload = manager.serializer.decode(responses[0].payload)

    assert responses[0].kind == "kv.delete.reject"
    assert payload["message"] == "write quorum not met"
    assert len(payload["errors"]) == 2

