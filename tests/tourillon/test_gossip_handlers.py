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

from tests.helpers import make_envelope
from tourillon.bootstrap.handlers import gossip
from tourillon.core.models.gossip import (
    GossipDelta,
    GossipDigest,
    GossipPong,
    GossipPush,
    GossipPushAccept,
)
from tourillon.core.models.member import Member, MemberPhase
from tourlib.adapter import MsgPackSerializer
from tourlib.exceptions import ValidationError


class _GossipSpy:
    def __init__(self) -> None:
        self.accept_requests = []
        self.ping_requests = []
        self.digest_requests = []

    async def accept(self, request):
        self.accept_requests.append(request)
        return GossipPushAccept(sender="node-a", accepted=1, ignored=0)

    async def pong(self, request):
        self.ping_requests.append(request)
        return GossipPong(sender="node-a", same=True)

    async def delta(self, request):
        self.digest_requests.append(request)
        yield GossipDelta(
            sender="node-a",
            members=[
                Member(
                    node_id="node-b",
                    peer_address="127.0.0.1:4001",
                    epoch=2,
                    phase=MemberPhase.READY,
                    tokens=("01", "02"),
                )
            ],
            has_more=True,
            after_node_id="node-b",
            wanted=["node-c"],
        )

        yield GossipDelta(
            sender="node-a",
            members=[
                Member(
                    node_id="node-c",
                    peer_address="127.0.0.1:4002",
                    epoch=3,
                    phase=MemberPhase.READY,
                    tokens=("03", "04"),
                )
            ],
            has_more=False,
            after_node_id="",
            wanted=[],
        )


class _ManagerStub:
    def __init__(self, gossip_service: _GossipSpy) -> None:
        self.serializer = MsgPackSerializer()
        self.gossip = gossip_service


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_gossip_push_handler_decodes_member_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spy = _GossipSpy()
    manager = _ManagerStub(spy)
    monkeypatch.setattr(gossip, "get_manager", lambda: manager)

    request = make_envelope(
        kind="gossip.push",
        payload=manager.serializer.encode(
            {
                "sender": "node-b",
                "hop_limit": 1,
                "members": [
                    {
                        "node_id": "node-b",
                        "peer_address": "127.0.0.1:4001",
                        "phase": "ready",
                        "epoch": 2,
                        "tokens": ["01", "02"],
                    }
                ],
            }
        ),
    )

    responses = [response async for response in gossip.push(request)]

    assert len(responses) == 1
    assert responses[0].kind == "gossip.push.accept"
    assert spy.accept_requests[0].members[0].node_id == "node-b"


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_gossip_ping_handler_returns_pong(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spy = _GossipSpy()
    manager = _ManagerStub(spy)
    monkeypatch.setattr(gossip, "get_manager", lambda: manager)

    request = make_envelope(
        kind="gossip.ping",
        payload=manager.serializer.encode(
            {
                "sender": "node-b",
                "fingerprint": "abcd",
                "count": 2,
            }
        ),
    )

    responses = [response async for response in gossip.ping(request)]
    payload = manager.serializer.decode(responses[0].payload)

    assert responses[0].kind == "gossip.pong"
    assert payload["same"] is True
    assert spy.ping_requests[0].fingerprint == "abcd"


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_gossip_digest_handler_streams_all_delta_pages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spy = _GossipSpy()
    manager = _ManagerStub(spy)
    monkeypatch.setattr(gossip, "get_manager", lambda: manager)

    request = make_envelope(
        kind="gossip.digest",
        payload=manager.serializer.encode(
            {
                "sender": "node-b",
                "has_more": False,
                "after_node_id": "",
                "digest": [{"node_id": "node-a", "epoch": 1}],
            }
        ),
    )

    responses = [response async for response in gossip.digest(request)]
    first_payload = manager.serializer.decode(responses[0].payload)
    second_payload = manager.serializer.decode(responses[1].payload)

    assert [response.kind for response in responses] == ["gossip.delta", "gossip.delta"]
    assert first_payload["has_more"] is True
    assert second_payload["has_more"] is False
    assert len(spy.digest_requests) == 1
    assert spy.digest_requests[0].after_node_id == ""


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_gossip_digest_handler_rejects_invalid_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spy = _GossipSpy()
    manager = _ManagerStub(spy)
    monkeypatch.setattr(gossip, "get_manager", lambda: manager)

    request = make_envelope(
        kind="gossip.digest",
        payload=manager.serializer.encode(
            {
                "sender": "",
                "has_more": False,
                "after_node_id": "",
                "digest": [],
            }
        ),
    )

    with pytest.raises(ValidationError, match="sender must be a non-empty string"):
        _ = [response async for response in gossip.digest(request)]


@pytest.mark.tourillon
def test_gossip_push_from_document_rejects_non_list_members() -> None:
    with pytest.raises(ValidationError, match="members must be a list"):
        GossipPush.from_document({"sender": "node-a", "hop_limit": 1, "members": "bad"})


@pytest.mark.tourillon
def test_gossip_push_from_document_rejects_non_mapping_member() -> None:
    with pytest.raises(ValidationError, match=r"members\[0\] must be a table"):
        GossipPush.from_document(
            {"sender": "node-a", "hop_limit": 1, "members": ["not-a-dict"]}
        )


@pytest.mark.tourillon
def test_gossip_push_as_int_rejects_non_integer() -> None:
    with pytest.raises(ValidationError, match="hop_limit must be an integer"):
        GossipPush.from_document(
            {"sender": "node-a", "hop_limit": "one", "members": []}
        )


@pytest.mark.tourillon
def test_gossip_push_as_int_rejects_below_min_value() -> None:
    with pytest.raises(ValidationError, match="hop_limit must be >= 0"):
        GossipPush.from_document({"sender": "node-a", "hop_limit": -1, "members": []})


@pytest.mark.tourillon
def test_gossip_digest_from_document_rejects_non_list_digest() -> None:
    with pytest.raises(ValidationError, match="digest must be a list"):
        GossipDigest.from_document(
            {
                "sender": "node-a",
                "has_more": False,
                "after_node_id": "",
                "digest": "bad",
            }
        )


@pytest.mark.tourillon
def test_gossip_digest_from_document_rejects_non_string_after_node_id() -> None:
    with pytest.raises(ValidationError, match="after_node_id must be a string"):
        GossipDigest.from_document(
            {"sender": "node-a", "has_more": False, "after_node_id": 42, "digest": []}
        )


@pytest.mark.tourillon
def test_gossip_digest_as_bool_rejects_non_bool() -> None:
    with pytest.raises(ValidationError, match="has_more must be a boolean"):
        GossipDigest.from_document(
            {"sender": "node-a", "has_more": 1, "after_node_id": "", "digest": []}
        )


@pytest.mark.tourillon
def test_gossip_digest_parse_node_digest_rejects_non_mapping() -> None:
    with pytest.raises(ValidationError, match=r"digest\[0\] must be a table"):
        GossipDigest.from_document(
            {
                "sender": "node-a",
                "has_more": False,
                "after_node_id": "",
                "digest": ["not-a-dict"],
            }
        )
