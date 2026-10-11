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
from collections.abc import Mapping

from tourillon.core.ports import Telemetry
from tourillon.core.telemetry import build_noop_telemetry
from tourillon.core.transports.dispatcher import Dispatcher, EnvelopeHandler
from tourillon.core.transports.server import EnvelopeServer


async def start_envelope_server(
    handlers: Mapping[str, EnvelopeHandler],
    server_ssl: ssl.SSLContext,
    *,
    host: str = "127.0.0.1",
    port: int = 0,
    name: str = "server",
    max_in_flight: int = 32,
    telemetry: Telemetry | None = None,
) -> EnvelopeServer:
    """Start an envelope server with defaults used in integration tests."""
    server = EnvelopeServer(
        Dispatcher(handlers),
        server_ssl,
        name=name,
        max_in_flight=max_in_flight,
        telemetry=telemetry if telemetry is not None else build_noop_telemetry(),
    )
    await server.start(host, port)
    return server
