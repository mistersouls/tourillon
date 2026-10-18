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
from tourclient.core import InspectService, Transport
from tourlib.adapter import MsgPackSerializer
from tourlib.envelope import Envelope, replay_envelope
from tourlib.exceptions import ResponseError
from tourlib.models.inspect import NodeInspectRequest


def _node_info_document(
    *,
    chunk_index: int,
    has_more: bool,
    next_cursor: str | None,
    members: list[dict[str, object]],
    gossip_stats: dict[str, object],
    self_member: dict[str, object] | None = None,
) -> dict[str, object]:
    payload = {
        "node_id": "node-3",
        "snapshot_at": "2026-08-16T09:15:20Z",
        "chunk_index": chunk_index,
        "has_more": has_more,
        "next_cursor": next_cursor,
        "ring_position": {
            "partition_shift": 10,
            "total_partitions": 1024,
            "owned_partitions": 256,
            "owned_percent": 25.0,
            "vnode_ranges": [
                {
                    "token": "0x7b",
                    "arc": [1023, 63],
                    "partitions": 64,
                }
            ],
        },
        "members": members,
        "gossip_stats": gossip_stats,
    }
    payload["self_member"] = self_member or {
        "node_id": "node-3",
        "phase": "READY",
        "status": "-",
        "epoch": 3,
    }
    return payload


@pytest.mark.asyncio
async def test_inspect_service_collects_multiple_pages(
    mtls_contexts: tuple[ssl.SSLContext, ssl.SSLContext],
) -> None:
    server_ssl, client_ssl = mtls_contexts
    serializer = MsgPackSerializer()
    seen_cursors: list[str | None] = []

    async def inspect_handler(request: Envelope) -> AsyncIterator[Envelope]:
        request_data = serializer.decode(request.payload)
        cursor = request_data.get("cursor")
        seen_cursors.append(cursor)

        if cursor is None:
            payload = _node_info_document(
                chunk_index=0,
                has_more=True,
                next_cursor="node-1",
                members=[
                    {
                        "node_id": "node-1",
                        "phase": "READY",
                        "status": "LIVE",
                        "epoch": 3,
                    }
                ],
                gossip_stats={"known_members": 3},
                self_member={
                    "node_id": "node-3",
                    "phase": "READY",
                    "status": "-",
                    "epoch": 3,
                },
            )
        else:
            payload = _node_info_document(
                chunk_index=1,
                has_more=False,
                next_cursor=None,
                members=[
                    {"node_id": "node-3", "phase": "READY", "status": "-", "epoch": 3}
                ],
                gossip_stats={},
            )

        yield replay_envelope(
            kind="node.info",
            request=request,
            payload=serializer.encode(payload),
        )

    server = await start_envelope_server({"node.inspect": inspect_handler}, server_ssl)

    try:
        host, port = server.endpoint
        transport = Transport(
            bind=f"{host}:{port}",
            ssl_context=client_ssl,
            serializer=serializer,
        )
        await transport.connect()
        try:
            service = InspectService(transport, serializer=serializer)
            chunks = await service.inspect(NodeInspectRequest(limit=1))
        finally:
            await transport.close()
    finally:
        await server.stop()

    assert seen_cursors == [None, "node-1"]
    assert len(chunks) == 2
    assert chunks[0].has_more is True
    assert chunks[0].next_cursor == "node-1"
    assert chunks[1].has_more is False
    assert chunks[0].self_member is not None
    assert chunks[0].self_member.node_id == "node-3"
    assert [member.node_id for member in chunks[0].members] == ["node-1"]
    assert [member.node_id for member in chunks[1].members] == ["node-3"]


@pytest.mark.asyncio
async def test_inspect_service_accepts_hex_token_values(
    mtls_contexts: tuple[ssl.SSLContext, ssl.SSLContext],
) -> None:
    server_ssl, client_ssl = mtls_contexts
    serializer = MsgPackSerializer()
    token_hex = "0x1234567890abcdef1234567890abcdef"

    async def inspect_handler(request: Envelope) -> AsyncIterator[Envelope]:
        payload = _node_info_document(
            chunk_index=0,
            has_more=False,
            next_cursor=None,
            members=[
                {"node_id": "node-3", "phase": "READY", "status": "-", "epoch": 3}
            ],
            gossip_stats={},
        )
        ring_position = payload["ring_position"]
        assert isinstance(ring_position, dict)
        vnode_ranges = ring_position["vnode_ranges"]
        assert isinstance(vnode_ranges, list)
        vnode_ranges[0]["token"] = token_hex
        yield replay_envelope(
            kind="node.info",
            request=request,
            payload=serializer.encode(payload),
        )

    server = await start_envelope_server({"node.inspect": inspect_handler}, server_ssl)

    try:
        host, port = server.endpoint
        transport = Transport(
            bind=f"{host}:{port}",
            ssl_context=client_ssl,
            serializer=serializer,
        )
        await transport.connect()
        try:
            service = InspectService(transport, serializer=serializer)
            chunks = await service.inspect()
        finally:
            await transport.close()
    finally:
        await server.stop()

    assert len(chunks) == 1
    assert chunks[0].ring_position.vnode_ranges[0].token == token_hex
    assert chunks[0].ring_position.vnode_ranges[0].arc == (1023, 63)


@pytest.mark.asyncio
async def test_inspect_service_fails_on_unexpected_response_kind(
    mtls_contexts: tuple[ssl.SSLContext, ssl.SSLContext],
) -> None:
    server_ssl, client_ssl = mtls_contexts
    serializer = MsgPackSerializer()

    async def bad_inspect_handler(request: Envelope) -> AsyncIterator[Envelope]:
        yield replay_envelope(
            kind="node.status",
            request=request,
            payload=serializer.encode({"status": "ok"}),
        )

    server = await start_envelope_server(
        {"node.inspect": bad_inspect_handler}, server_ssl
    )

    try:
        host, port = server.endpoint
        transport = Transport(
            bind=f"{host}:{port}",
            ssl_context=client_ssl,
            serializer=serializer,
        )
        await transport.connect()
        try:
            service = InspectService(transport, serializer=serializer)
            with pytest.raises(ResponseError, match="unexpected inspect response kind"):
                await service.inspect()
        finally:
            await transport.close()
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_inspect_service_rejects_missing_self_member_payload(
    mtls_contexts: tuple[ssl.SSLContext, ssl.SSLContext],
) -> None:
    server_ssl, client_ssl = mtls_contexts
    serializer = MsgPackSerializer()

    async def inspect_handler(request: Envelope) -> AsyncIterator[Envelope]:
        payload = _node_info_document(
            chunk_index=0,
            has_more=False,
            next_cursor=None,
            members=[],
            gossip_stats={},
        )
        payload.pop("self_member", None)
        yield replay_envelope(
            kind="node.info",
            request=request,
            payload=serializer.encode(payload),
        )

    server = await start_envelope_server({"node.inspect": inspect_handler}, server_ssl)

    try:
        host, port = server.endpoint
        transport = Transport(
            bind=f"{host}:{port}",
            ssl_context=client_ssl,
            serializer=serializer,
        )
        await transport.connect()
        try:
            service = InspectService(transport, serializer=serializer)
            with pytest.raises(ResponseError, match="self_member"):
                await service.inspect()
        finally:
            await transport.close()
    finally:
        await server.stop()
