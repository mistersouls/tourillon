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

import pytest

import tourillon.bootstrap.handlers.ae as ae_handler
from tourlib.adapter import MsgPackSerializer
from tourlib.envelope import build_envelope
from tourlib.exceptions import ValidationError
from tourlib.models.ae import AeCounters, AeStatus, ObligationsPage

pytestmark = pytest.mark.tourillon


class _ManagerStub:
    def __init__(self) -> None:
        self.serializer = MsgPackSerializer()
        self.requests = []
        self.error = None

    async def ae_status(self, request):
        self.requests.append(request)
        if self.error:
            raise self.error
        return AeStatus(
            "node-local", "READY", True, 1, 2, 60.0, AeCounters(), (), 0, 0, 0
        )

    async def ae_obligations(self, request):
        self.requests.append(request)
        if self.error:
            raise self.error
        return ObligationsPage("node-local", 2, request.target, (), 0)


async def _call(monkeypatch, handler, kind, document, error=None):
    manager = _ManagerStub()
    manager.error = error
    monkeypatch.setattr(ae_handler, "get_manager", lambda: manager)
    request = build_envelope(
        kind=kind,
        payload=manager.serializer.encode(document),
        schema_id=manager.serializer.schema_id,
    )
    responses = [item async for item in handler(request)]
    assert len(responses) == 1
    assert responses[0].correlation_id == request.correlation_id
    return manager, responses[0], manager.serializer.decode(responses[0].payload)


async def test_ae_status_answers_with_the_node_snapshot(monkeypatch):
    manager, response, payload = await _call(
        monkeypatch, ae_handler.status, "ae.status", {"limit": 5}
    )
    assert response.kind == "ae.status.accept"
    assert AeStatus.from_document(payload).node_id == "node-local"
    assert manager.requests[0].limit == 5


async def test_ae_obligations_forwards_the_target_filter(monkeypatch):
    manager, response, payload = await _call(
        monkeypatch, ae_handler.obligations, "ae.obligations", {"target": "node-2"}
    )
    assert response.kind == "ae.obligations.accept"
    assert ObligationsPage.from_document(payload).target == "node-2"
    assert manager.requests[0].target == "node-2"


@pytest.mark.parametrize(
    "handler,kind,document,error,message",
    [
        (ae_handler.status, "ae.status", {"force": True}, None, "unknown"),
        (ae_handler.status, "ae.status", {"limit": 0}, None, "limit"),
        (
            ae_handler.obligations,
            "ae.obligations",
            {},
            ValidationError("invalid cursor"),
            "invalid cursor",
        ),
    ],
)
async def test_ae_handlers_reject_invalid_requests(
    monkeypatch, handler, kind, document, error, message
):
    _, response, payload = await _call(monkeypatch, handler, kind, document, error)
    assert response.kind == f"{kind}.reject"
    assert payload["code"] == "invalid_request"
    assert message in payload["message"]
