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

from types import SimpleNamespace
from typing import Any

import pytest

from tests.helpers import make_envelope
from tourillon.bootstrap.handlers import transfer as transfer_handlers
from tourillon.core.exceptions import ProcessError, TransferError
from tourillon.core.models.transfer import (
    RangeTransfer,
    TransferChunk,
    TransferClaim,
    TransferCommit,
    TransferInit,
    TransferInitAccept,
    TransferReject,
)
from tourlib.adapter import MsgPackSerializer

SERIALIZER = MsgPackSerializer()
RANGE = RangeTransfer(segment=(0, 3), src="src", dst="dst")
REQUESTS = [
    (
        transfer_handlers.init,
        "transfer.init",
        TransferInit(serial="s", range=RANGE).to_dict(),
    ),
    (
        transfer_handlers.claim,
        "transfer.claim",
        TransferClaim(serial="s", resumes={}, range=RANGE).to_dict(),
    ),
    (
        transfer_handlers.chunk,
        "transfer.chunk",
        TransferChunk(range=RANGE, raw=b"x", seq=4, session="s-1").to_dict(),
    ),
    (
        transfer_handlers.commit,
        "transfer.commit",
        TransferCommit(range=RANGE, session="s-1").to_dict(),
    ),
]


class _TransferSpy:
    def __init__(self, error: Exception | None = None) -> None:
        self._error = error
        self.requests: list[Any] = []

    async def init(self, request: TransferInit) -> TransferInitAccept:
        self.requests.append(request)
        self._fail()
        return TransferInitAccept(resumes={}, session="s-1")

    async def claim(self, request: TransferClaim):
        self.requests.append(request)
        yield TransferChunk(range=request.range, raw=b"a")
        self._fail()
        yield TransferChunk(range=request.range, raw=b"", seq=1, last=True)

    async def chunk(self, request: TransferChunk) -> None:
        self.requests.append(request)
        self._fail()

    async def commit(self, request: TransferCommit) -> int:
        self.requests.append(request)
        self._fail()
        return 3

    def _fail(self) -> None:
        if self._error is not None:
            raise self._error


def _install(monkeypatch: pytest.MonkeyPatch, spy: _TransferSpy) -> None:
    manager = SimpleNamespace(serializer=SERIALIZER, transfer=spy)
    monkeypatch.setattr(transfer_handlers, "get_manager", lambda: manager)


async def _call(handler, kind: str, payload: dict[str, Any]) -> list[tuple[str, Any]]:
    request = make_envelope(kind=kind, payload=SERIALIZER.encode(payload))
    responses = [response async for response in handler(request)]
    assert all(
        response.correlation_id == request.correlation_id for response in responses
    )
    return [
        (response.kind, SERIALIZER.decode(response.payload)) for response in responses
    ]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_transfer_handlers_answer_with_accepts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spy = _TransferSpy()
    _install(monkeypatch, spy)

    init, claim, chunk, commit = [await _call(*request) for request in REQUESTS]

    assert init == [("transfer.init.accept", {"resumes": {}, "session": "s-1"})]
    assert [kind for kind, _ in claim] == ["transfer.chunk", "transfer.chunk"]
    assert TransferChunk.from_dict(claim[-1][1]).last is True
    assert chunk == [("transfer.chunk.accept", {"seq": 4})]
    assert commit == [("transfer.commit.accept", {"activated": 3})]
    assert [type(request) for request in spy.requests] == [
        TransferInit,
        TransferClaim,
        TransferChunk,
        TransferCommit,
    ]


@pytest.mark.tourillon
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "fatal"),
    [(TransferError("retry me"), False), (ProcessError("give up"), True)],
)
async def test_transfer_handlers_turn_transfer_errors_into_typed_rejects(
    monkeypatch: pytest.MonkeyPatch,
    error: Exception,
    fatal: bool,
) -> None:
    _install(monkeypatch, _TransferSpy(error))

    for handler, kind, payload in REQUESTS:
        responses = await _call(handler, kind, payload)
        reject_kind, data = responses[-1]
        assert reject_kind == f"{kind}.reject"
        assert TransferReject.from_dict(data) == TransferReject(
            reason=str(error), fatal=fatal
        )


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_transfer_handlers_reject_malformed_payloads_as_fatal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spy = _TransferSpy()
    _install(monkeypatch, spy)

    for handler, kind, _ in REQUESTS:
        [(reject_kind, data)] = await _call(handler, kind, {"unexpected": True})
        reject = TransferReject.from_dict(data)
        assert reject_kind == f"{kind}.reject"
        assert reject.fatal
        assert f"malformed {kind} payload" in reject.reason

    assert spy.requests == []
