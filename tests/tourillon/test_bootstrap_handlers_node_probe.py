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

from collections.abc import AsyncIterator

import pytest

import tourillon.bootstrap.handlers.node as node_handler
import tourillon.bootstrap.handlers.probe as probe_handler
from tourillon.core.exceptions import LeaveError
from tourlib.adapter import MsgPackSerializer
from tourlib.envelope import Envelope, build_envelope
from tourlib.models.inspect import NodeInspectRequest


class _ManagerStub:
    def __init__(self) -> None:
        self.serializer = MsgPackSerializer()
        self.node_id = "node-local"
        self.inspect_requests: list[NodeInspectRequest] = []
        self.join_calls: list[list[str]] = []
        self.leave_calls = 0
        self.leave_error: Exception | None = None

    async def inspect(
        self, request: NodeInspectRequest
    ) -> AsyncIterator[dict[str, object]]:
        self.inspect_requests.append(request)
        yield {"chunk_index": 0, "node_id": "node-local"}

    async def join(self, seeds: list[str]) -> None:
        self.join_calls.append(seeds)

    async def leave(self) -> None:
        self.leave_calls += 1
        if self.leave_error is not None:
            raise self.leave_error


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_node_handler_inspect_decodes_request_and_streams_infos(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = _ManagerStub()
    monkeypatch.setattr(node_handler, "get_manager", lambda: manager)

    request = build_envelope(
        kind="node.inspect",
        payload=manager.serializer.encode({"limit": 1}),
        schema_id=manager.serializer.schema_id,
    )

    responses = [item async for item in node_handler.inspect(request)]

    assert len(responses) == 1
    assert responses[0].kind == "node.info"
    assert manager.inspect_requests[0].limit == 1


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_node_handler_join_triggers_manager_join(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = _ManagerStub()
    monkeypatch.setattr(node_handler, "get_manager", lambda: manager)

    request = build_envelope(
        kind="node.join",
        payload=manager.serializer.encode({"seeds": ["seed-a", "seed-b"]}),
        schema_id=manager.serializer.schema_id,
    )

    responses = [item async for item in node_handler.join(request)]

    assert manager.join_calls == [["seed-a", "seed-b"]]
    assert responses[0].kind == "node.join.accept"
    assert manager.serializer.decode(responses[0].payload)["message"].startswith(
        "Join operation"
    )


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_probe_handler_ping_replies_with_pong(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = _ManagerStub()
    monkeypatch.setattr(probe_handler, "get_manager", lambda: manager)
    monkeypatch.setattr(probe_handler.time, "time", lambda: 1234.567)

    request = build_envelope(
        kind="probe.ping",
        payload=manager.serializer.encode({"timestamp_ms": 1000}),
        schema_id=manager.serializer.schema_id,
    )

    responses = [item async for item in probe_handler.ping(request)]

    assert len(responses) == 1
    response = responses[0]
    assert isinstance(response, Envelope)
    assert response.kind == "probe.pong"
    payload = manager.serializer.decode(response.payload)
    assert payload == {
        "node_id": "node-local",
        "timestamp_ms": 1000,
        "recv_ms": 1234567,
    }


@pytest.mark.tourillon
@pytest.mark.parametrize("rejected", [False, True])
async def test_node_leave_acknowledges_initiation_or_domain_rejection(
    monkeypatch: pytest.MonkeyPatch, rejected: bool
) -> None:
    manager = _ManagerStub()
    if rejected:
        manager.leave_error = LeaveError("Node is not ready")
    monkeypatch.setattr(node_handler, "get_manager", lambda: manager)
    request = build_envelope(
        kind="node.leave",
        payload=manager.serializer.encode({}),
        schema_id=manager.serializer.schema_id,
    )

    responses = [item async for item in node_handler.leave(request)]

    assert manager.leave_calls == 1
    assert len(responses) == 1
    response = responses[0]
    assert response.kind == ("node.leave.reject" if rejected else "node.leave.accept")
    assert response.correlation_id == request.correlation_id
    assert response.schema_id == request.schema_id
    assert response.proto_version == request.proto_version
    assert response.mode == request.mode
    message = manager.serializer.decode(response.payload)["message"]
    assert message == (
        "Node is not ready"
        if rejected
        else "Leave initiated; draining continues asynchronously."
    )


@pytest.mark.tourillon
async def test_node_leave_does_not_hide_unexpected_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = _ManagerStub()
    manager.leave_error = RuntimeError("unexpected")
    monkeypatch.setattr(node_handler, "get_manager", lambda: manager)
    request = build_envelope(
        kind="node.leave",
        payload=manager.serializer.encode({}),
        schema_id=manager.serializer.schema_id,
    )
    with pytest.raises(RuntimeError, match="unexpected"):
        _ = [item async for item in node_handler.leave(request)]


@pytest.mark.tourillon
@pytest.mark.parametrize("document", [None, [], {"force": True}])
async def test_node_leave_validates_request_before_delegating(
    monkeypatch: pytest.MonkeyPatch, document: object
) -> None:
    manager = _ManagerStub()
    monkeypatch.setattr(node_handler, "get_manager", lambda: manager)
    request = build_envelope(
        kind="node.leave",
        payload=manager.serializer.encode(document),
        schema_id=manager.serializer.schema_id,
    )
    with pytest.raises(ValueError, match="empty mapping"):
        _ = [item async for item in node_handler.leave(request)]
    assert manager.leave_calls == 0
