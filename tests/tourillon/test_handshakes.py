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

import asyncio
import ssl

import pytest

from tourillon.core.transports.handshakes import WINDOW_S, HandshakeMonitor

pytestmark = pytest.mark.tourillon


class _Clock:
    def __init__(self) -> None:
        self.now = 10_000.0

    def __call__(self) -> float:
        return self.now


async def _serve(context: ssl.SSLContext) -> tuple[asyncio.Server, int]:
    async def handle(reader, writer) -> None:
        writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0, ssl=context)
    return server, server.sockets[0].getsockname()[1]


async def _settle(monitor: HandshakeMonitor, count: int) -> None:
    for _ in range(100):
        if monitor.snapshot().count >= count:
            return
        await asyncio.sleep(0.02)


async def test_a_client_without_certificate_is_counted_as_rejected(
    mtls_contexts,
) -> None:
    server_ctx, _ = mtls_contexts
    monitor = HandshakeMonitor()
    monitor.install(server_ctx)
    server, port = await _serve(server_ctx)
    anonymous = ssl.create_default_context()
    anonymous.check_hostname = False
    anonymous.verify_mode = ssl.CERT_NONE
    try:
        with pytest.raises(
            (ssl.SSLError, ConnectionError, asyncio.IncompleteReadError)
        ):
            reader, writer = await asyncio.open_connection(
                "127.0.0.1", port, ssl=anonymous
            )
            await reader.readexactly(1)
        await _settle(monitor, 1)
    finally:
        server.close()
        await server.wait_closed()

    rejections = monitor.snapshot()
    assert rejections.count == 1
    assert rejections.last_reason
    assert rejections.last_ago_s is not None


async def test_a_valid_client_is_not_counted(mtls_contexts) -> None:
    server_ctx, client_ctx = mtls_contexts
    monitor = HandshakeMonitor()
    monitor.install(server_ctx)
    server, port = await _serve(server_ctx)
    try:
        reader, writer = await asyncio.open_connection(
            "127.0.0.1", port, ssl=client_ctx
        )
        await reader.read()
        writer.close()
    finally:
        server.close()
        await server.wait_closed()

    assert monitor.snapshot().count == 0


def test_rejections_age_out_after_the_window() -> None:
    clock = _Clock()
    monitor = HandshakeMonitor(clock=clock)
    assert monitor.snapshot().count == 0

    monitor.record("CERTIFICATE_VERIFY_FAILED")
    monitor.record("CERTIFICATE_VERIFY_FAILED")
    clock.now += 600
    monitor.record("WRONG_VERSION_NUMBER")
    snapshot = monitor.snapshot()
    assert snapshot.count == 3
    assert snapshot.last_reason == "WRONG_VERSION_NUMBER"
    assert snapshot.last_ago_s == 0

    clock.now += WINDOW_S - 300
    assert monitor.snapshot().count == 1

    clock.now += WINDOW_S
    assert monitor.snapshot().count == 0
    assert monitor.snapshot().last_reason is None
