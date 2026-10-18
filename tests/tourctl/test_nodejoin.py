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
from typing import cast

import pytest

from tourclient.core.services.node import NodeService
from tourclient.core.transport import Transport
from tourclient.exception import UnknownServiceError
from tourctl.core.nodejoin import _run_join, join
from tourlib.adapter import MsgPackSerializer
from tourlib.envelope import Envelope, build_envelope
from tourlib.models.node import JoinRequest, JoinResponse


class _TransportStub:
    def __init__(self, response: Envelope) -> None:
        self._response = response
        self.calls: list[tuple[str, dict[str, object]]] = []

    async def request(self, kind: str, document: dict[str, object]) -> Envelope:
        self.calls.append((kind, document))
        return self._response


def _service_with_response(response_kind: str, message: str) -> NodeService:
    serializer = MsgPackSerializer()
    response = build_envelope(
        kind=response_kind,
        payload=serializer.encode({"message": message}),
        schema_id=serializer.schema_id,
    )
    return NodeService(
        transport=cast(Transport, cast(object, _TransportStub(response))),
        serializer=serializer,
    )


@asynccontextmanager
async def _service_ctx(service: object) -> AsyncIterator[object]:
    yield service


@pytest.mark.tourctl
@pytest.mark.asyncio
async def test_run_join_forwards_seeds_and_returns_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = _service_with_response("node.join.accept", "ok")

    monkeypatch.setattr(
        "tourctl.core.nodejoin.client", lambda *args, **kwargs: _service_ctx(service)
    )

    result = await _run_join(
        "127.0.0.1:7701",
        seeds={"seed-b", "seed-a"},
        tourconf=None,
        context_name=None,
    )

    assert result.success is True
    assert result.message == "ok"
    transport = cast(_TransportStub, cast(object, service._transport))
    assert transport.calls == [("node.join", JoinRequest(seeds=("seed-a", "seed-b")))]


@pytest.mark.tourctl
@pytest.mark.asyncio
async def test_join_raises_system_exit_when_join_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_run_join(*args, **kwargs) -> JoinResponse:
        return JoinResponse(success=False, message="rejected")

    monkeypatch.setattr("tourctl.core.nodejoin._run_join", fake_run_join)

    with pytest.raises(SystemExit, match="rejected"):
        await join("127.0.0.1:7701", seeds=None, tourconf=None, context_name=None)


@pytest.mark.tourctl
@pytest.mark.asyncio
async def test_run_join_raises_when_client_returns_wrong_service(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "tourctl.core.nodejoin.client", lambda *args, **kwargs: _service_ctx(object())
    )

    with pytest.raises(UnknownServiceError, match="Unknown service: node"):
        await _run_join("127.0.0.1:7701", seeds=None, tourconf=None, context_name=None)


@pytest.mark.tourctl
@pytest.mark.asyncio
async def test_join_returns_message_when_successful(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_run_join(*args, **kwargs) -> JoinResponse:
        return JoinResponse(success=True, message="joined")

    monkeypatch.setattr("tourctl.core.nodejoin._run_join", fake_run_join)

    message = await join("127.0.0.1:7701", seeds=None, tourconf=None, context_name=None)

    assert message == "joined"


@pytest.mark.tourctl
@pytest.mark.asyncio
async def test_join_wraps_internal_exception_as_system_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_run_join(*args, **kwargs) -> JoinResponse:
        raise RuntimeError("boom")

    monkeypatch.setattr("tourctl.core.nodejoin._run_join", fake_run_join)

    with pytest.raises(SystemExit, match="Node join failed: boom"):
        await join("127.0.0.1:7701", seeds=None, tourconf=None, context_name=None)


@pytest.mark.tourctl
@pytest.mark.asyncio
async def test_join_formats_missing_context_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_run_join(*args, **kwargs) -> JoinResponse:
        raise KeyError("prod")

    monkeypatch.setattr("tourctl.core.nodejoin._run_join", fake_run_join)

    with pytest.raises(SystemExit, match="Unknown client context 'prod'"):
        await join("127.0.0.1:7701", seeds=None, tourconf=None, context_name="prod")
