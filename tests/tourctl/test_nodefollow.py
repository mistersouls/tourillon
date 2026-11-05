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
from typing import cast

import pytest

from tests.helpers.inspect import make_status
from tourclient.core.services.node import NodeService
from tourclient.core.transport import Transport
from tourctl.core.error import CliError
from tourctl.core.nodefollow import follow_phase, render_follow
from tourlib.adapter import MsgPackSerializer
from tourlib.envelope import Envelope, build_envelope
from tourlib.models.inspect import ProgressInfo

pytestmark = pytest.mark.tourctl


class _StatusTransport:
    def __init__(self, statuses: list[object]) -> None:
        self._statuses = statuses

    async def request(self, kind: str, document: object) -> Envelope:
        assert kind == "node.inspect"
        assert document == {}
        status = self._statuses.pop(0)
        serializer = MsgPackSerializer()
        return build_envelope(
            kind="node.inspect.accept",
            payload=serializer.encode(status.to_document()),
            schema_id=serializer.schema_id,
        )


def _service(*statuses: object) -> NodeService:
    return NodeService(
        cast(Transport, _StatusTransport(list(statuses))),
        MsgPackSerializer(),
    )


@pytest.mark.asyncio
async def test_follow_reports_progress_until_expected_phase(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("tourctl.core.nodefollow.POLL_INTERVAL", 0.01)
    initial = make_status(
        phase="JOINING",
        progress=ProgressInfo(goal="join", total=2, done=0),
    )
    progressed = make_status(
        phase="JOINING",
        progress=ProgressInfo(goal="join", total=2, done=2),
    )
    service = _service(initial, progressed, make_status(phase="READY"))

    result = await follow_phase(
        service,
        expected_phase="READY",
        timeout=1,
        address="node:7001",
    )

    output = render_follow(result, as_json=False)
    assert "0 of 2 partitions synced" in output
    assert "2 of 2 partitions synced" in output
    assert output.endswith("node-3 is READY.")


@pytest.mark.asyncio
async def test_follow_timeout_returns_exit_code_five(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("tourctl.core.nodefollow.POLL_INTERVAL", 0.01)
    status = make_status(phase="DRAINING")
    service = _service(status)

    with pytest.raises(CliError) as caught:
        await follow_phase(
            service,
            expected_phase="IDLE",
            timeout=0.001,
            address="node:7001",
        )

    assert caught.value.code == 5
    assert "Follow with: tourctl node status node:7001" in str(caught.value)


@pytest.mark.asyncio
async def test_follow_cancellation_stops_tracking_without_cancelling_operation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("tourctl.core.nodefollow.POLL_INTERVAL", 10.0)
    status = make_status(phase="DRAINING")
    service = _service(status)
    task = asyncio.create_task(
        follow_phase(
            service,
            expected_phase="IDLE",
            timeout=30,
            address="node:7001",
        )
    )
    await asyncio.sleep(0)
    task.cancel()

    result = await task

    assert result.stopped is True
    assert result.status is not None
    assert result.status.phase == "DRAINING"
    assert "Stopped following; node-3 is still DRAINING." in render_follow(
        result, as_json=False
    )
