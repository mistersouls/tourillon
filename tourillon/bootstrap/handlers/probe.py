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

import time
from collections.abc import AsyncIterator

from tourillon.bootstrap.deps import client_dispatcher, get_manager, peer_dispatcher
from tourlib.envelope import Envelope, replay_envelope
from tourlib.models.probe import ProbePong

c_dispatcher = client_dispatcher()
p_dispatcher = peer_dispatcher()


@c_dispatcher.on("probe.ping")
@p_dispatcher.on("probe.ping")
async def ping(request: Envelope) -> AsyncIterator[Envelope]:
    """Handle probe ping request.

    Decodes ProbePing from request, creates ProbePong response with current timestamp.
    """
    manager = get_manager()
    serializer = manager.serializer
    node_id = manager.node_id
    ping_data = serializer.decode(request.payload)
    recv_ms = int(time.time() * 1000)
    probe_pong = ProbePong(
        node_id=node_id,
        timestamp_ms=ping_data["timestamp_ms"],
        recv_ms=recv_ms,
    )

    yield replay_envelope(
        kind="probe.pong",
        request=request,
        payload=serializer.encode(probe_pong),
    )
