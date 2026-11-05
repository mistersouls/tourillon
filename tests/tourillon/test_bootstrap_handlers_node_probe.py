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

import tourillon.bootstrap.handlers.node as node_handler
import tourillon.bootstrap.handlers.probe as probe_handler
from tourillon.core.exceptions import (
    DrainError,
    JoinError,
    MemberNotFoundError,
    PauseError,
    ResumeError,
)
from tourillon.core.models.member import MemberPhase
from tourillon.core.models.state import NodeState
from tourlib.adapter import MsgPackSerializer
from tourlib.envelope import Envelope, build_envelope
from tourlib.exceptions import ValidationError
from tourlib.models.health import HealthReason, NodeHealth
from tourlib.models.inspect import MembersPage, MembersRequest


class _ManagerStub:
    def __init__(self) -> None:
        self.serializer = MsgPackSerializer()
        self.node_id = "node-local"
        self.members_requests: list[MembersRequest] = []
        self.members_error: Exception | None = None
        self.member_error: Exception | None = None
        self.member_requests: list[str] = []
        self.join_calls: list[list[str]] = []
        self.join_error: Exception | None = None
        self.join_cancel_calls = 0
        self.join_cancel_error: Exception | None = None
        self.drain_calls = 0
        self.drain_error: Exception | None = None
        self.drain_cancel_calls = 0
        self.drain_cancel_error: Exception | None = None
        self.pause_calls = 0
        self.pause_error: Exception | None = None
        self.resume_calls = 0
        self.resume_error: Exception | None = None

    async def inspect(self) -> "_StatusStub":
        return _StatusStub()

    async def health(self) -> NodeHealth:
        return NodeHealth.from_reasons(
            "node-local", [HealthReason("gc_stopped", "GC stopped", critical=True)]
        )

    async def members(self, request: MembersRequest) -> MembersPage:
        self.members_requests.append(request)
        if self.members_error is not None:
            raise self.members_error
        return MembersPage(
            node_id="node-local",
            snapshot_at="2026-11-05T10:00:00+00:00",
            members=1,
            summary=(),
            items=(),
            total=0,
            anomalies_only=request.anomalies_only,
        )

    async def member(self, node_id: str) -> "_MemberReportStub":
        self.member_requests.append(node_id)
        if self.member_error is not None:
            raise self.member_error
        return _MemberReportStub(node_id)

    async def join(self, seeds: list[str]) -> bool:
        self.join_calls.append(seeds)
        if self.join_error is not None:
            raise self.join_error
        return True

    async def cancel_join(self) -> NodeState:
        self.join_cancel_calls += 1
        if self.join_cancel_error is not None:
            raise self.join_cancel_error
        return NodeState("node-local", phase=MemberPhase.IDLE, epoch=4)

    async def drain(self) -> None:
        self.drain_calls += 1
        if self.drain_error is not None:
            raise self.drain_error

    async def cancel_drain(self) -> NodeState:
        self.drain_cancel_calls += 1
        if self.drain_cancel_error is not None:
            raise self.drain_cancel_error
        return NodeState("node-local", phase=MemberPhase.READY, epoch=5)

    async def pause(self) -> None:
        self.pause_calls += 1
        if self.pause_error is not None:
            raise self.pause_error

    async def resume(self) -> None:
        self.resume_calls += 1
        if self.resume_error is not None:
            raise self.resume_error


class _StatusStub:
    @staticmethod
    def to_document() -> dict[str, object]:
        return {"node_id": "node-local"}


class _MemberReportStub:
    def __init__(self, node_id: str) -> None:
        self.node_id = node_id

    def to_document(self) -> dict[str, str]:
        return {"node_id": self.node_id}


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_node_handler_inspect_replies_with_one_status_document(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = _ManagerStub()
    monkeypatch.setattr(node_handler, "get_manager", lambda: manager)

    request = build_envelope(
        kind="node.inspect",
        payload=manager.serializer.encode({}),
        schema_id=manager.serializer.schema_id,
    )

    responses = [item async for item in node_handler.inspect(request)]

    assert len(responses) == 1
    assert responses[0].kind == "node.inspect.accept"
    assert responses[0].correlation_id == request.correlation_id
    assert manager.serializer.decode(responses[0].payload) == {"node_id": "node-local"}


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_node_handler_health_replies_with_the_verdict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = _ManagerStub()
    monkeypatch.setattr(node_handler, "get_manager", lambda: manager)

    request = build_envelope(
        kind="node.health",
        payload=manager.serializer.encode({}),
        schema_id=manager.serializer.schema_id,
    )

    responses = [item async for item in node_handler.health(request)]

    assert [item.kind for item in responses] == ["node.health.accept"]
    assert responses[0].correlation_id == request.correlation_id
    verdict = NodeHealth.from_document(manager.serializer.decode(responses[0].payload))
    assert verdict == await manager.health()


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_node_handler_members_accepts_valid_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = _ManagerStub()
    monkeypatch.setattr(node_handler, "get_manager", lambda: manager)

    request = build_envelope(
        kind="node.members",
        payload=manager.serializer.encode({"phases": ["joining"], "limit": 5}),
        schema_id=manager.serializer.schema_id,
    )

    responses = [item async for item in node_handler.members(request)]

    assert len(responses) == 1
    assert responses[0].kind == "node.members.accept"
    assert responses[0].correlation_id == request.correlation_id
    assert manager.members_requests == [MembersRequest(phases=("JOINING",), limit=5)]
    page = MembersPage.from_document(manager.serializer.decode(responses[0].payload))
    assert page.node_id == "node-local"
    assert page.anomalies_only is False


@pytest.mark.tourillon
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "document,error,message",
    [
        ({"limit": 0}, None, "limit must be an integer"),
        ({"bogus": 1}, None, "unknown fields: bogus"),
        ([], None, "must be a table"),
        ({"cursor": "x"}, ValidationError("invalid cursor"), "invalid cursor"),
        ({}, TypeError("bad key"), "bad key"),
    ],
)
async def test_node_handler_members_rejects_invalid_request(
    monkeypatch: pytest.MonkeyPatch,
    document: object,
    error: Exception | None,
    message: str,
) -> None:
    manager = _ManagerStub()
    manager.members_error = error
    monkeypatch.setattr(node_handler, "get_manager", lambda: manager)

    request = build_envelope(
        kind="node.members",
        payload=manager.serializer.encode(document),
        schema_id=manager.serializer.schema_id,
    )

    responses = [item async for item in node_handler.members(request)]

    assert len(responses) == 1
    assert responses[0].kind == "node.members.reject"
    assert responses[0].correlation_id == request.correlation_id
    payload = manager.serializer.decode(responses[0].payload)
    assert payload["code"] == "invalid_request"
    assert message in payload["message"]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_node_handler_member_accepts_a_known_member(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = _ManagerStub()
    monkeypatch.setattr(node_handler, "get_manager", lambda: manager)
    request = build_envelope(
        kind="node.member",
        payload=manager.serializer.encode({"node_id": "node-2"}),
        schema_id=manager.serializer.schema_id,
    )

    responses = [item async for item in node_handler.member(request)]

    assert responses[0].kind == "node.member.accept"
    assert manager.member_requests == ["node-2"]
    assert manager.serializer.decode(responses[0].payload) == {"node_id": "node-2"}


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_node_handler_member_rejects_unknown_member(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = _ManagerStub()
    manager.member_error = MemberNotFoundError("missing", "node-local")
    monkeypatch.setattr(node_handler, "get_manager", lambda: manager)
    request = build_envelope(
        kind="node.member",
        payload=manager.serializer.encode({"node_id": "missing"}),
        schema_id=manager.serializer.schema_id,
    )

    responses = [item async for item in node_handler.member(request)]

    assert responses[0].kind == "node.member.reject"
    assert manager.serializer.decode(responses[0].payload) == {
        "code": "member_not_found",
        "message": "missing is not a member known to node-local.",
    }


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
    assert manager.serializer.decode(responses[0].payload)["message"] == (
        "Join operation initiated."
    )


@pytest.mark.tourillon
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kind", "handler", "method", "expected_phase", "epoch"),
    [
        ("node.join.cancel", node_handler.cancel_join, "join_cancel_calls", "IDLE", 4),
        (
            "node.drain.cancel",
            node_handler.cancel_drain,
            "drain_cancel_calls",
            "READY",
            5,
        ),
    ],
)
async def test_node_handler_cancels_membership_operation(
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
    handler,
    method: str,
    expected_phase: str,
    epoch: int,
) -> None:
    manager = _ManagerStub()
    monkeypatch.setattr(node_handler, "get_manager", lambda: manager)
    request = build_envelope(
        kind=kind,
        payload=manager.serializer.encode({}),
        schema_id=manager.serializer.schema_id,
    )

    responses = [item async for item in handler(request)]

    assert getattr(manager, method) == 1
    assert responses[0].kind == f"{kind}.accept"
    message = manager.serializer.decode(responses[0].payload)["message"]
    assert expected_phase in message
    assert f"epoch {epoch}" in message


@pytest.mark.tourillon
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kind", "handler", "error_attribute"),
    [
        ("node.join.cancel", node_handler.cancel_join, "join_cancel_error"),
        ("node.drain.cancel", node_handler.cancel_drain, "drain_cancel_error"),
    ],
)
async def test_node_handler_rejects_cancel_without_active_operation(
    monkeypatch: pytest.MonkeyPatch, kind: str, handler, error_attribute: str
) -> None:
    manager = _ManagerStub()
    setattr(manager, error_attribute, JoinError("Node is not joining"))
    if kind == "node.drain.cancel":
        setattr(manager, error_attribute, DrainError("Node is not draining"))
    monkeypatch.setattr(node_handler, "get_manager", lambda: manager)
    request = build_envelope(
        kind=kind,
        payload=manager.serializer.encode({}),
        schema_id=manager.serializer.schema_id,
    )

    responses = [item async for item in handler(request)]

    assert responses[0].kind == f"{kind}.reject"


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
async def test_node_drain_acknowledges_initiation_or_domain_rejection(
    monkeypatch: pytest.MonkeyPatch, rejected: bool
) -> None:
    manager = _ManagerStub()
    if rejected:
        manager.drain_error = DrainError("Node is not ready")
    monkeypatch.setattr(node_handler, "get_manager", lambda: manager)
    request = build_envelope(
        kind="node.drain",
        payload=manager.serializer.encode({}),
        schema_id=manager.serializer.schema_id,
    )

    responses = [item async for item in node_handler.drain(request)]

    assert manager.drain_calls == 1
    assert len(responses) == 1
    response = responses[0]
    assert response.kind == ("node.drain.reject" if rejected else "node.drain.accept")
    assert response.correlation_id == request.correlation_id
    assert response.schema_id == request.schema_id
    assert response.proto_version == request.proto_version
    assert response.mode == request.mode
    message = manager.serializer.decode(response.payload)["message"]
    assert message == (
        "Node is not ready"
        if rejected
        else "Drain initiated; draining continues asynchronously."
    )


@pytest.mark.tourillon
async def test_node_drain_does_not_hide_unexpected_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = _ManagerStub()
    manager.drain_error = RuntimeError("unexpected")
    monkeypatch.setattr(node_handler, "get_manager", lambda: manager)
    request = build_envelope(
        kind="node.drain",
        payload=manager.serializer.encode({}),
        schema_id=manager.serializer.schema_id,
    )
    with pytest.raises(RuntimeError, match="unexpected"):
        _ = [item async for item in node_handler.drain(request)]


@pytest.mark.tourillon
@pytest.mark.parametrize("document", [None, [], {"force": True}])
async def test_node_drain_validates_request_before_delegating(
    monkeypatch: pytest.MonkeyPatch, document: object
) -> None:
    manager = _ManagerStub()
    monkeypatch.setattr(node_handler, "get_manager", lambda: manager)
    request = build_envelope(
        kind="node.drain",
        payload=manager.serializer.encode(document),
        schema_id=manager.serializer.schema_id,
    )
    with pytest.raises(ValueError, match="empty mapping"):
        _ = [item async for item in node_handler.drain(request)]
    assert manager.drain_calls == 0


@pytest.mark.tourillon
@pytest.mark.parametrize(
    ("action", "error", "kind", "message"),
    [
        ("pause", PauseError("No readable peer"), "node.pause", "No readable peer"),
        ("resume", ResumeError("No data source"), "node.resume", "No data source"),
    ],
)
async def test_node_pause_and_resume_report_accept_or_rejection(
    monkeypatch: pytest.MonkeyPatch,
    action: str,
    error: Exception,
    kind: str,
    message: str,
) -> None:
    manager = _ManagerStub()
    setattr(manager, f"{action}_error", error)
    monkeypatch.setattr(node_handler, "get_manager", lambda: manager)
    request = build_envelope(
        kind=kind,
        payload=manager.serializer.encode({}),
        schema_id=manager.serializer.schema_id,
    )

    responses = [item async for item in getattr(node_handler, action)(request)]

    assert len(responses) == 1
    response = responses[0]
    assert response.kind == f"{kind}.reject"
    assert manager.serializer.decode(response.payload) == {"message": message}
    assert getattr(manager, f"{action}_calls") == 1


@pytest.mark.tourillon
@pytest.mark.parametrize(
    ("action", "kind"), [("pause", "node.pause"), ("resume", "node.resume")]
)
async def test_node_pause_and_resume_accept_empty_request(
    monkeypatch: pytest.MonkeyPatch, action: str, kind: str
) -> None:
    manager = _ManagerStub()
    monkeypatch.setattr(node_handler, "get_manager", lambda: manager)
    request = build_envelope(
        kind=kind,
        payload=manager.serializer.encode({}),
        schema_id=manager.serializer.schema_id,
    )

    responses = [item async for item in getattr(node_handler, action)(request)]

    assert len(responses) == 1
    assert responses[0].kind == f"{kind}.accept"
    assert getattr(manager, f"{action}_calls") == 1
