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

from time import time

from tourclient.core.transport import Transport
from tourlib import Serializer
from tourlib.exceptions import ResponseError
from tourlib.models.probe import ProbePong


class ProbeService:
    """Application service for probe operations."""

    def __init__(
        self,
        transport: Transport,
        serializer: Serializer,
    ) -> None:
        self._transport = transport
        self._serializer = serializer

    async def ping(self) -> ProbePong:
        response = await self._transport.request(
            kind="probe.ping",
            document={
                "node_id": "client",
                "timestamp_ms": int(time() * 1000),
            },
        )

        if response.kind != "probe.pong":
            raise ResponseError(f"unexpected ping response kind: {response.kind}")

        pong_data = self._serializer.decode(response.payload)
        return ProbePong(
            node_id=pong_data["node_id"],
            timestamp_ms=pong_data["timestamp_ms"],
            recv_ms=pong_data["recv_ms"],
        )
