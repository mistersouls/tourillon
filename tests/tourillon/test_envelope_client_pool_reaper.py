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
from typing import cast

import pytest

from tourillon.core.gossip.topology import TopologyManager
from tourillon.core.peer.pool import PeerConnection, PeerPool
from tourillon.core.probe.detector import FailureDetectorProvider
from tourillon.core.telemetry import build_noop_telemetry
from tourlib.adapter import MsgPackSerializer


class _IdleClient:
    def __init__(self, inactivity_duration: float) -> None:
        self.inactivity_duration = inactivity_duration
        self.close_calls = 0

    async def close(self) -> None:
        self.close_calls += 1


class _DetectorStub:
    def __init__(self) -> None:
        self.stop_calls = 0

    async def stop(self) -> None:
        self.stop_calls += 1


class _MemberStub:
    def __init__(self, peer_address: str) -> None:
        self.peer_address = peer_address


class _RegistryStub:
    @staticmethod
    def get(node_id: str) -> _MemberStub | None:
        _ = node_id
        return _MemberStub("127.0.0.1:4000")


class _TopologyStub:
    registry = _RegistryStub()


class _TopologyManagerStub:
    @staticmethod
    async def snapshot() -> _TopologyStub:
        return _TopologyStub()


def _as_topologymgr(stub: _TopologyManagerStub) -> TopologyManager:
    return cast(TopologyManager, cast(object, stub))


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_remove_expired_removes_idle_overflow_peer() -> None:
    fd_provider = FailureDetectorProvider(serializer=MsgPackSerializer())
    pool = PeerPool(
        tls_ctx=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT),
        topologymgr=_as_topologymgr(_TopologyManagerStub()),
        fd_provider=fd_provider,
        min_size=1,
        idle_ttl=10.0,
        telemetry=build_noop_telemetry(),
    )
    overflow_client = _IdleClient(inactivity_duration=11.0)
    detector = _DetectorStub()
    pool._overflows["peer-overflow"] = PeerConnection(
        peer_id="peer-overflow",
        client=overflow_client,  # type: ignore[arg-type]
        fd=detector,  # type: ignore[arg-type]
        refcount=0,
        ephemeral=True,
    )

    await pool._remove_expired()

    assert overflow_client.close_calls == 1
    assert detector.stop_calls == 1
    assert "peer-overflow" not in pool._overflows

