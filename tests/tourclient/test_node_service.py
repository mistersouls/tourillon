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
from collections.abc import AsyncIterator

import pytest

from tests.helpers import start_envelope_server
from tourclient.core.services.node import NodeService
from tourclient.core.services.registry import get_service, validate_service_endpoint
from tourclient.core.transport import Transport
from tourclient.exception import RequestRejectedError
from tourlib.adapter import MsgPackSerializer
from tourlib.envelope import Envelope, replay_envelope
from tourlib.exceptions import ResponseError
from tourlib.models.node import LeaveResponse

pytestmark = pytest.mark.tourctl


@pytest.mark.parametrize(
    ("kind", "document", "error"),
    [
        ("node.leave.accept", {"message": "Draining initiated"}, None),
        ("node.leave.reject", {"message": "Not ready"}, RequestRejectedError),
        ("node.leave.other", None, ResponseError),
        ("node.leave.accept", None, ResponseError),
        ("node.leave.accept", {"message": 1}, ResponseError),
        ("node.leave.accept", {"message": ""}, ResponseError),
        ("node.leave.accept", {"message": "ok", "unknown": True}, ResponseError),
        ("node.leave.reject", [], ResponseError),
    ],
)
async def test_leave_over_real_mtls_transport(
    mtls_contexts: tuple[ssl.SSLContext, ssl.SSLContext],
    kind: str,
    document: object,
    error: type[Exception] | None,
) -> None:
    server_ssl, client_ssl = mtls_contexts
    serializer = MsgPackSerializer()
    calls: list[object] = []

    async def handler(request: Envelope) -> AsyncIterator[Envelope]:
        calls.append(serializer.decode(request.payload))
        yield replay_envelope(
            kind=kind, request=request, payload=serializer.encode(document)
        )

    server = await start_envelope_server({"node.leave": handler}, server_ssl)
    host, port = server.endpoint
    transport = Transport(
        bind=f"{host}:{port}", ssl_context=client_ssl, serializer=serializer
    )
    try:
        await transport.connect()
        service = get_service(
            "node", "peer", transport=transport, serializer=serializer
        )
        assert isinstance(service, NodeService)
        if error is None:
            assert await service.leave() == LeaveResponse("Draining initiated")
        else:
            with pytest.raises(error) as caught:
                await service.leave()
            if isinstance(caught.value, RequestRejectedError):
                assert caught.value.request_kind == "node.leave"
                assert str(caught.value) == "Not ready"
        assert calls == [{}]
    finally:
        await transport.close()
        await server.stop()


def test_node_leave_service_is_peer_only() -> None:
    validate_service_endpoint("node", "peer")
    with pytest.raises(ValueError, match="not available on endpoint 'client'"):
        validate_service_endpoint("node", "client")
