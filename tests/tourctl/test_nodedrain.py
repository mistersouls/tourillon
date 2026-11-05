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
from pathlib import Path
from typing import cast

import pytest

from tests.helpers.inspect import make_status
from tourclient.core.services.node import NodeService
from tourclient.core.transport import Transport
from tourclient.exception import RequestRejectedError
from tourctl.bootstrap.usecases.common import GlobalOptions
from tourctl.bootstrap.usecases.node import node_drain
from tourctl.core.nodedrain import drain
from tourlib.adapter import MsgPackSerializer
from tourlib.envelope import Envelope, build_envelope
from tourlib.exceptions import ResponseError
from tourlib.models.node import DrainRequest

pytestmark = pytest.mark.tourctl


class _TransportStub:
    async def request(self, kind: str, document: object) -> Envelope:
        assert kind == "node.drain"
        assert document == DrainRequest()
        serializer = MsgPackSerializer()
        return build_envelope(
            kind="node.drain.accept",
            payload=serializer.encode(
                {"message": "Drain initiated; draining continues asynchronously."}
            ),
            schema_id=serializer.schema_id,
        )


async def test_node_drain_command_uses_peer_endpoint_and_shared_options(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = []
    service = NodeService(cast(Transport, _TransportStub()), MsgPackSerializer())

    @asynccontextmanager
    async def client_context(*args, **kwargs) -> AsyncIterator[NodeService]:
        calls.append((args, kwargs))
        yield service

    monkeypatch.setattr("tourctl.core.nodedrain.client", client_context)
    message = await node_drain(
        "127.0.0.1:7701",
        opts=GlobalOptions(tourconf=Path("tourconf.toml"), context="prod"),
    )
    assert message == "Drain initiated; draining continues asynchronously."
    assert calls == [
        (
            ("node",),
            {
                "endpoint": "peer",
                "bind": "127.0.0.1:7701",
                "tourconf": Path("tourconf.toml"),
                "context_name": "prod",
            },
        )
    ]


@pytest.mark.parametrize(
    ("error", "message"),
    [
        (RequestRejectedError("node.drain", {"message": "Not ready"}), "Not ready"),
        (
            ConnectionRefusedError("offline"),
            "Tourillon node refused the drain connection",
        ),
        (OSError("unreachable"), "Unable to reach the Tourillon node for drain"),
        (ResponseError("invalid"), "Tourillon node returned an invalid drain response"),
        (KeyError("prod"), "Unknown client context 'prod'"),
    ],
)
async def test_node_drain_command_renders_actionable_failures(
    monkeypatch: pytest.MonkeyPatch, error: Exception, message: str
) -> None:
    @asynccontextmanager
    async def client_context(*args, **kwargs) -> AsyncIterator[NodeService]:
        raise error
        yield

    monkeypatch.setattr("tourctl.core.nodedrain.client", client_context)
    with pytest.raises(SystemExit, match=message):
        await drain("127.0.0.1:7701", tourconf=None, context_name="prod")


async def test_node_drain_rejects_wrong_service(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    @asynccontextmanager
    async def client_context(*args, **kwargs) -> AsyncIterator[object]:
        yield object()

    monkeypatch.setattr("tourctl.core.nodedrain.client", client_context)
    with pytest.raises(SystemExit, match="Unknown service: node"):
        await drain("127.0.0.1:7701", tourconf=None, context_name=None)


async def test_drain_cancel_wait_uses_the_same_peer_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    serializer = MsgPackSerializer()

    class _WaitTransport:
        def __init__(self) -> None:
            self.calls: list[str] = []

        async def request(self, kind: str, document: object) -> Envelope:
            self.calls.append(kind)
            payload = (
                {"message": "node-a: DRAINING -> READY"}
                if kind == "node.drain.cancel"
                else make_status(phase="READY").to_document()
            )
            response_kind = (
                "node.drain.cancel.accept"
                if kind == "node.drain.cancel"
                else "node.inspect.accept"
            )
            return build_envelope(
                kind=response_kind,
                payload=serializer.encode(payload),
                schema_id=serializer.schema_id,
            )

    transport = _WaitTransport()
    service = NodeService(cast(Transport, transport), serializer)
    contexts = []

    @asynccontextmanager
    async def client_context(*args, **kwargs) -> AsyncIterator[NodeService]:
        contexts.append((args, kwargs))
        yield service

    monkeypatch.setattr("tourctl.core.nodedrain.client", client_context)
    output = await drain(
        "127.0.0.1:7701",
        wait=True,
        cancel=True,
        tourconf=None,
        context_name=None,
    )

    assert "node-3 is READY." in output
    assert transport.calls == ["node.drain.cancel", "node.inspect"]
    assert len(contexts) == 1
