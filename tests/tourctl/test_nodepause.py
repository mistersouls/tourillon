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

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest

from tests.helpers.inspect import make_status
from tourclient.core.services.node import NodeService
from tourclient.exception import RequestRejectedError
from tourctl.core.error import EXIT_TIMEOUT, CliError
from tourctl.core.nodepause import pause
from tourctl.core.noderesume import resume
from tourlib.adapter import MsgPackSerializer
from tourlib.envelope import Envelope, build_envelope
from tourlib.exceptions import ResponseError

pytestmark = pytest.mark.tourctl


class _NodeTransport:
    def __init__(self, phases: tuple[str, ...] = ("READY",)) -> None:
        self.serializer = MsgPackSerializer()
        self.phases = phases
        self.phase_index = 0
        self.calls: list[str] = []
        self.pause_response: Envelope | None = None

    async def request(self, kind: str, document: object) -> Envelope:
        self.calls.append(kind)
        if kind == "node.pause":
            response = self.pause_response or build_envelope(
                kind="node.pause.accept",
                payload=self.serializer.encode({"message": "Node is paused."}),
                schema_id=self.serializer.schema_id,
            )
            return response
        if kind == "node.resume":
            return build_envelope(
                kind="node.resume.accept",
                payload=self.serializer.encode({"message": "Resume initiated."}),
                schema_id=self.serializer.schema_id,
            )
        if kind == "node.inspect":
            phase = self.phases[min(self.phase_index, len(self.phases) - 1)]
            self.phase_index += 1
            return build_envelope(
                kind="node.inspect.accept",
                payload=self.serializer.encode(make_status(phase=phase).to_document()),
                schema_id=self.serializer.schema_id,
            )
        raise AssertionError(f"unexpected request: {kind}, {document}")


def _service(transport: _NodeTransport) -> NodeService:
    return NodeService(transport, transport.serializer)


@pytest.mark.asyncio
async def test_pause_uses_direct_peer_address_and_renders_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = _NodeTransport()
    service = _service(transport)
    calls = []

    @asynccontextmanager
    async def client_context(*args, **kwargs) -> AsyncIterator[NodeService]:
        calls.append((args, kwargs))
        yield service

    monkeypatch.setattr("tourctl.core.nodepause.client", client_context)
    output = await pause(
        "127.0.0.1:7701",
        as_json=True,
        tourconf="tourconf.toml",
        context_name="prod",
    )

    assert '"message": "Node is paused."' in output
    assert calls == [
        (
            ("node",),
            {
                "endpoint": "peer",
                "bind": "127.0.0.1:7701",
                "tourconf": "tourconf.toml",
                "context_name": "prod",
            },
        )
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "message"),
    [
        (RequestRejectedError("node.pause", {"message": "unsafe"}), "unsafe"),
        (
            ConnectionRefusedError("offline"),
            "Tourillon node refused the pause connection",
        ),
        (OSError("unreachable"), "Unable to reach the Tourillon node for pause"),
        (ResponseError("invalid"), "Tourillon node returned an invalid pause response"),
    ],
)
async def test_pause_renders_rejections_and_connectivity_failures(
    monkeypatch: pytest.MonkeyPatch, error: Exception, message: str
) -> None:
    @asynccontextmanager
    async def client_context(*args, **kwargs) -> AsyncIterator[NodeService]:
        raise error
        yield

    monkeypatch.setattr("tourctl.core.nodepause.client", client_context)
    with pytest.raises(SystemExit, match=message):
        await pause(
            "127.0.0.1:7701",
            as_json=False,
            tourconf=None,
            context_name=None,
        )


@pytest.mark.asyncio
async def test_resume_waits_for_ready_using_one_peer_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = _NodeTransport(("PAUSED", "READY"))
    service = _service(transport)
    connections = []

    @asynccontextmanager
    async def client_context(*args, **kwargs) -> AsyncIterator[NodeService]:
        connections.append((args, kwargs))
        yield service

    monkeypatch.setattr("tourctl.core.noderesume.client", client_context)
    monkeypatch.setattr("tourctl.core.noderesume._POLL_INTERVAL", 0)
    output = await resume(
        "127.0.0.1:7701",
        wait=True,
        timeout=1,
        as_json=False,
        tourconf=None,
        context_name=None,
    )

    assert output == "node_id=node-3\nphase=READY"
    assert transport.calls == ["node.resume", "node.inspect", "node.inspect"]
    assert len(connections) == 1


@pytest.mark.asyncio
async def test_resume_wait_timeout_reports_current_phase(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = _NodeTransport(("PAUSED",))
    service = _service(transport)

    @asynccontextmanager
    async def client_context(*args, **kwargs) -> AsyncIterator[NodeService]:
        yield service

    monkeypatch.setattr("tourctl.core.noderesume.client", client_context)
    with pytest.raises(CliError) as caught:
        await resume(
            "127.0.0.1:7701",
            wait=True,
            timeout=0.001,
            as_json=False,
            tourconf=None,
            context_name=None,
        )

    assert caught.value.code == EXIT_TIMEOUT
    assert "node remains PAUSED" in str(caught.value)
