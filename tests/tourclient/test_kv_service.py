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
from typing import Any

import pytest

from tests.helpers import start_envelope_server
from tourclient.core import KvService, Transport
from tourclient.exception import RequestRejectedError
from tourlib.adapter import MsgPackSerializer
from tourlib.envelope import Envelope, replay_envelope
from tourlib.exceptions import ResponseError
from tourlib.models.version import Version


def _version_document(namespace: Any, key: Any, value: Any) -> dict[str, Any]:
    return {
        "address": {
            "namespace": namespace,
            "key": key,
        },
        "data": value,
        "meta": {
            "ts": {
                "wall_ms": 1,
                "counter": 0,
                "node_id": "n1",
            },
            "quorum_write": 1,
        },
    }


@pytest.mark.asyncio
async def test_kv_service_put_get_delete_roundtrip(
    mtls_contexts: tuple[ssl.SSLContext, ssl.SSLContext],
) -> None:
    server_ssl, client_ssl = mtls_contexts
    serializer = MsgPackSerializer()
    store: dict[tuple[Any, Any], Any] = {}

    async def put_handler(request: Envelope) -> AsyncIterator[Envelope]:
        document = serializer.decode(request.payload)
        key = (document["namespace"], document["key"])
        assert document["quorum_write"] is None
        store[key] = document["value"]
        yield replay_envelope(
            kind="kv.put.accept",
            request=request,
            payload=serializer.encode(_version_document(*key, store[key])),
        )

    async def get_handler(request: Envelope) -> AsyncIterator[Envelope]:
        document = serializer.decode(request.payload)
        key = (document["namespace"], document["key"])
        assert document["quorum_read"] is None
        value = store.get(key)
        payload = None if value is None else _version_document(*key, value)
        yield replay_envelope(
            kind="kv.get.accept",
            request=request,
            payload=serializer.encode(payload),
        )

    async def delete_handler(request: Envelope) -> AsyncIterator[Envelope]:
        document = serializer.decode(request.payload)
        key = (document["namespace"], document["key"])
        assert document["quorum_write"] is None
        store.pop(key, None)
        yield replay_envelope(
            kind="kv.delete.accept",
            request=request,
            payload=serializer.encode(_version_document(*key, b"")),
        )

    server = await start_envelope_server(
        {
            "kv.put": put_handler,
            "kv.get": get_handler,
            "kv.delete": delete_handler,
        },
        server_ssl,
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
            service = KvService(transport, serializer)
            put_result = await service.put(namespace="ns", key="k", value="v1")
            get_result = await service.get(namespace="ns", key="k")
            delete_result = await service.delete(namespace="ns", key="k")
            get_missing = await service.get(namespace="ns", key="k")
        finally:
            await transport.close()
    finally:
        await server.stop()

    assert isinstance(put_result, Version)
    assert serializer.decode(put_result.data) == "v1"
    assert isinstance(get_result, Version)
    assert serializer.decode(get_result.address.key) == "k"
    assert isinstance(delete_result, Version)
    assert serializer.decode(delete_result.address.namespace) == "ns"
    assert get_missing is None


@pytest.mark.asyncio
async def test_kv_service_rejects_unexpected_response_kind(
    mtls_contexts: tuple[ssl.SSLContext, ssl.SSLContext],
) -> None:
    server_ssl, client_ssl = mtls_contexts
    serializer = MsgPackSerializer()

    async def bad_put_handler(request: Envelope) -> AsyncIterator[Envelope]:
        yield replay_envelope(
            kind="kv.put.other",
            request=request,
            payload=serializer.encode({"message": "no"}),
        )

    server = await start_envelope_server({"kv.put": bad_put_handler}, server_ssl)

    try:
        host, port = server.endpoint
        transport = Transport(
            bind=f"{host}:{port}",
            ssl_context=client_ssl,
            serializer=serializer,
        )
        await transport.connect()
        try:
            service = KvService(transport, serializer)
            with pytest.raises(ResponseError, match="unexpected kv.put response kind"):
                await service.put(namespace="default", key="alpha", value="beta")
        finally:
            await transport.close()
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_kv_service_rejects_invalid_version_payload(
    mtls_contexts: tuple[ssl.SSLContext, ssl.SSLContext],
) -> None:
    server_ssl, client_ssl = mtls_contexts
    serializer = MsgPackSerializer()

    async def bad_get_handler(request: Envelope) -> AsyncIterator[Envelope]:
        yield replay_envelope(
            kind="kv.get.accept",
            request=request,
            payload=serializer.encode({"address": {"namespace": b"ns"}}),
        )

    server = await start_envelope_server({"kv.get": bad_get_handler}, server_ssl)

    try:
        host, port = server.endpoint
        transport = Transport(
            bind=f"{host}:{port}",
            ssl_context=client_ssl,
            serializer=serializer,
        )
        await transport.connect()
        try:
            service = KvService(transport, serializer)
            with pytest.raises(ResponseError, match="invalid kv.get response payload"):
                await service.get(namespace="default", key="alpha")
        finally:
            await transport.close()
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_kv_service_raises_rejected_error_with_server_details(
    mtls_contexts: tuple[ssl.SSLContext, ssl.SSLContext],
) -> None:
    server_ssl, client_ssl = mtls_contexts
    serializer = MsgPackSerializer()

    async def rejecting_get_handler(request: Envelope) -> AsyncIterator[Envelope]:
        yield replay_envelope(
            kind="kv.get.reject",
            request=request,
            payload=serializer.encode(
                {
                    "operation": "read",
                    "code": "quorum_not_met",
                    "message": "read quorum not met: successes=1, required=2",
                    "errors": [
                        {
                            "type": "TimeoutError",
                            "message": "no response from node-b within 30.0s",
                        }
                    ],
                    "required": 2,
                    "received": 1,
                }
            ),
        )

    server = await start_envelope_server({"kv.get": rejecting_get_handler}, server_ssl)

    try:
        host, port = server.endpoint
        transport = Transport(
            bind=f"{host}:{port}",
            ssl_context=client_ssl,
            serializer=serializer,
        )
        await transport.connect()
        try:
            service = KvService(transport, serializer)
            with pytest.raises(RequestRejectedError) as exc_info:
                await service.get(namespace="default", key="alpha", quorum_read=2)
        finally:
            await transport.close()
    finally:
        await server.stop()

    error = exc_info.value
    assert error.request_kind == "kv.get"
    assert error.code == "quorum_not_met"
    assert error.operation == "read"
    assert error.errors == ("no response from node-b within 30.0s",)
    assert str(error) == (
        "read quorum not met: successes=1, required=2 (no response from node-b within 30.0s)"
    )
