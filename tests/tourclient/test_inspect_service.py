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

import ssl
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import pytest

from tests.helpers import start_envelope_server
from tests.helpers.inspect import make_page, make_status, make_view
from tourclient.core import InspectService, Transport
from tourclient.exception import RequestRejectedError
from tourlib.adapter import MsgPackSerializer
from tourlib.envelope import Envelope, replay_envelope
from tourlib.exceptions import ResponseError
from tourlib.models.health import HealthReason, NodeHealth
from tourlib.models.inspect import MembersRequest
from tourlib.models.member import MemberReport, MemberRequest

pytestmark = pytest.mark.tourctl

SERIALIZER = MsgPackSerializer()


async def _call(
    mtls_contexts: tuple[ssl.SSLContext, ssl.SSLContext],
    kind: str,
    reply_kind: str,
    payload: Any,
    action: Callable[[InspectService], Awaitable[Any]],
    seen: list[Any] | None = None,
) -> Any:
    server_ssl, client_ssl = mtls_contexts

    async def handler(request: Envelope) -> AsyncIterator[Envelope]:
        if seen is not None:
            seen.append(SERIALIZER.decode(request.payload))
        yield replay_envelope(
            kind=reply_kind, request=request, payload=SERIALIZER.encode(payload)
        )

    server = await start_envelope_server({kind: handler}, server_ssl)
    try:
        host, port = server.endpoint
        transport = Transport(
            bind=f"{host}:{port}", ssl_context=client_ssl, serializer=SERIALIZER
        )
        await transport.connect()
        try:
            return await action(InspectService(transport, serializer=SERIALIZER))
        finally:
            await transport.close()
    finally:
        await server.stop()


async def test_status_decodes_the_node_status(mtls_contexts) -> None:
    status = make_status()
    seen: list[Any] = []

    result = await _call(
        mtls_contexts,
        "node.inspect",
        "node.inspect.accept",
        status.to_document(),
        lambda service: service.status(),
        seen,
    )

    assert result == status
    assert seen == [{}]


async def test_health_decodes_the_verdict(mtls_contexts) -> None:
    verdict = NodeHealth.from_reasons(
        "node-3", [HealthReason("phase", "Node is JOINING")]
    )
    seen: list[Any] = []

    result = await _call(
        mtls_contexts,
        "node.health",
        "node.health.accept",
        verdict.to_document(),
        lambda service: service.health(),
        seen,
    )

    assert result == verdict
    assert seen == [{}]


@pytest.mark.parametrize(
    "payload",
    [
        {"node_id": "x", "status": "BAD"},
        {"node_id": "x", "status": "OK", "reasons": "none"},
        {"node_id": "x", "status": "OK", "reasons": [1]},
        {"node_id": "x", "status": "OK", "extra": 1},
    ],
)
async def test_health_rejects_an_invalid_verdict(mtls_contexts, payload) -> None:
    with pytest.raises(ResponseError, match="invalid node.health response"):
        await _call(
            mtls_contexts,
            "node.health",
            "node.health.accept",
            payload,
            lambda service: service.health(),
        )


async def test_members_sends_the_request_and_decodes_the_page(mtls_contexts) -> None:
    page = make_page(make_view("node-2", status="DEAD"), cursor="next")
    request = MembersRequest(phases=("joining",), limit=10, cursor="abc")
    seen: list[Any] = []

    result = await _call(
        mtls_contexts,
        "node.members",
        "node.members.accept",
        page.to_document(),
        lambda service: service.members(request),
        seen,
    )

    assert result == page
    assert seen == [request.to_document()]


async def test_members_defaults_to_the_anomalies_request(mtls_contexts) -> None:
    seen: list[Any] = []

    await _call(
        mtls_contexts,
        "node.members",
        "node.members.accept",
        make_page().to_document(),
        lambda service: service.members(),
        seen,
    )

    assert seen == [MembersRequest().to_document()]


async def test_member_sends_id_and_decodes_report(mtls_contexts) -> None:
    report = MemberReport(
        node_id="node-2",
        view_from="node-1",
        address="node-2:7701",
        phase="READY",
        epoch=2,
        status="ALIVE",
        since_s=2,
        last_heard_s=1,
        ring_share_percent=25,
        partitions=64,
        total_partitions=256,
        vnodes=4,
        answered=0,
        neighbors=0,
        owed_by=(),
        unanswered=(),
        stand_ins=(),
    )
    request = MemberRequest("node-2")
    seen: list[Any] = []

    result = await _call(
        mtls_contexts,
        "node.member",
        "node.member.accept",
        report.to_document(),
        lambda service: service.member(request),
        seen,
    )

    assert result == report
    assert seen == [request.to_document()]


async def test_member_rejection_preserves_the_error_code(mtls_contexts) -> None:
    with pytest.raises(RequestRejectedError, match="unknown member") as info:
        await _call(
            mtls_contexts,
            "node.member",
            "node.member.reject",
            {"code": "member_not_found", "message": "unknown member"},
            lambda service: service.member(MemberRequest("missing")),
        )

    assert info.value.code == "member_not_found"


@pytest.mark.parametrize(
    "payload,code,message",
    [
        ({"code": "invalid_request", "message": "bad limit"}, "invalid_request", "bad"),
        (["not", "a", "table"], None, "node.members rejected"),
    ],
)
async def test_members_reject_raises_request_rejected(
    mtls_contexts, payload, code, message
) -> None:
    with pytest.raises(RequestRejectedError, match=message) as info:
        await _call(
            mtls_contexts,
            "node.members",
            "node.members.reject",
            payload,
            lambda service: service.members(),
        )

    assert info.value.code == code
    assert info.value.request_kind == "node.members"


@pytest.mark.parametrize(
    "kind,reply_kind,payload,match",
    [
        ("node.inspect", "node.info", {}, "unexpected node.inspect response kind"),
        ("node.inspect", "node.inspect.accept", [1], "invalid node.inspect response"),
        ("node.inspect", "node.inspect.accept", {}, "invalid node.inspect response"),
        (
            "node.members",
            "node.members.accept",
            {"node_id": "x"},
            "invalid node.members response",
        ),
        (
            "node.members",
            "node.members.accept",
            "page",
            "invalid node.members response payload",
        ),
    ],
)
async def test_unexpected_responses_raise_response_error(
    mtls_contexts, kind, reply_kind, payload, match
) -> None:
    async def action(service: InspectService) -> Any:
        if kind == "node.inspect":
            return await service.status()
        return await service.members()

    with pytest.raises(ResponseError, match=match):
        await _call(mtls_contexts, kind, reply_kind, payload, action)
