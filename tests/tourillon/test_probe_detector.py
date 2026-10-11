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
import types
from typing import cast

import pytest

from tourillon.core.probe.detector import (
    FailureDetectorProvider,
    PeerFailureDetector,
    PeerStatus,
)
from tourlib.adapter import MsgPackSerializer
from tourlib.transport import EnvelopeClient


class _ClientStub:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.requests = 0

    async def request(self, envelope) -> None:
        _ = envelope
        self.requests += 1
        if self.fail:
            raise RuntimeError("boom")


@pytest.mark.tourillon
def test_failure_detector_provider_creates_detector() -> None:
    provider = FailureDetectorProvider(serializer=MsgPackSerializer(), threshold=3.0, probe_interval=1.0, probe_jitter=0.0)

    detector = provider.create("peer-a")

    assert isinstance(detector, PeerFailureDetector)
    assert detector.is_running is False


@pytest.mark.tourillon
def test_failure_detector_status_branches() -> None:
    detector = PeerFailureDetector(peer_id="peer-a", serializer=MsgPackSerializer(), threshold=3.0)

    detector._active = True
    assert detector.status() == PeerStatus.SUSPECT

    detector._active = False
    detector._phi_accrual.phi = lambda: 0.5  # type: ignore[method-assign]
    assert detector.status() == PeerStatus.ALIVE

    detector._phi_accrual.phi = lambda: 2.5  # type: ignore[method-assign]
    assert detector.status() == PeerStatus.SUSPECT

    detector._phi_accrual.phi = lambda: 4.0  # type: ignore[method-assign]
    assert detector.status() == PeerStatus.DEAD
    assert detector.is_dead() is True


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_failure_detector_run_and_stop_manage_probe_task() -> None:
    detector = PeerFailureDetector(peer_id="peer-a", serializer=MsgPackSerializer())
    parked = asyncio.Event()

    async def parked_probe(self: PeerFailureDetector) -> None:
        await parked.wait()

    detector._probe_peer = types.MethodType(parked_probe, detector)  # type: ignore[method-assign]

    await detector.run(cast(EnvelopeClient, _ClientStub()))
    assert detector.is_running is True

    await detector.stop()
    assert detector.is_running is False


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_failure_detector_probe_raises_without_client() -> None:
    detector = PeerFailureDetector(peer_id="peer-a", serializer=MsgPackSerializer())

    with pytest.raises(RuntimeError, match="Cannot probe peer without an EnvelopeClient"):
        await detector._probe_peer()


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_failure_detector_probe_records_success_and_exits() -> None:
    detector = PeerFailureDetector(peer_id="peer-a", serializer=MsgPackSerializer(), probe_interval=0.01)
    detector._active = True
    detector._client = cast(EnvelopeClient, _ClientStub())

    await detector._probe_peer()

    assert detector._client.requests >= 1
    assert detector._active is False
