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

from tourillon.core.gossip.topology import TopologyManager
from tourillon.core.models.gossip import (
    GossipDigest,
    GossipNodeDigest,
    GossipPing,
    GossipPush,
)
from tourillon.core.models.member import Member, MemberPhase
from tourillon.core.services.gossip import GossipService


def _member(node_id: str, *, epoch: int) -> Member:
    return Member(
        node_id=node_id,
        peer_address=f"127.0.0.1:{4000 + epoch}",
        epoch=epoch,
        phase=MemberPhase.READY,
        tokens=(f"{epoch:02x}", f"{epoch + 100:02x}"),
    )


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_gossip_service_accept_returns_accepted_and_ignored_count() -> None:
    topologymgr = TopologyManager()
    await topologymgr.apply_member(_member("node-a", epoch=5))
    service = GossipService(node_id="node-a", topologymgr=topologymgr)

    push = GossipPush(
        sender="node-b",
        hop_limit=1,
        members=[
            _member("node-b", epoch=3),  # new → accepted
            _member("node-a", epoch=1),  # stale → ignored
        ],
    )
    result = await service.accept(push)

    assert result.sender == "node-a"
    assert result.accepted == 1
    assert result.ignored == 1


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_gossip_service_pong_same_when_fingerprint_and_count_match() -> None:
    topologymgr = TopologyManager()
    await topologymgr.apply_member(_member("node-a", epoch=1))
    service = GossipService(node_id="node-a", topologymgr=topologymgr)

    fingerprint = await topologymgr.member_fingerprint()
    request = GossipPing(sender="node-b", fingerprint=fingerprint, count=1)
    result = await service.pong(request)

    assert result.sender == "node-a"
    assert result.same is True


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_gossip_service_pong_not_same_when_fingerprint_differs() -> None:
    topologymgr = TopologyManager()
    await topologymgr.apply_member(_member("node-a", epoch=1))
    service = GossipService(node_id="node-a", topologymgr=topologymgr)

    request = GossipPing(sender="node-b", fingerprint="wrong-fp", count=1)
    result = await service.pong(request)

    assert result.same is False


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_gossip_service_delta_returns_missing_and_newer_members_and_remote_only_wanted_nodes() -> (
    None
):
    topologymgr = TopologyManager()
    await topologymgr.merge_registry(
        (
            _member("node-b", epoch=2),
            _member("node-c", epoch=3),
        )
    )
    service = GossipService(node_id="node-b", topologymgr=topologymgr)

    request = GossipDigest(
        sender="node-a",
        has_more=False,
        after_node_id="",
        digest=[
            GossipNodeDigest(node_id="node-a", epoch=1),
            GossipNodeDigest(node_id="node-b", epoch=1),
        ],
    )

    responses = [response async for response in service.delta(request)]
    response = responses[0]

    assert response.sender == "node-b"
    assert response.has_more is False
    assert response.after_node_id == ""
    assert [member.node_id for member in response.members] == ["node-b", "node-c"]
    assert response.wanted == ["node-a"]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_gossip_service_delta_paginates_with_after_node_id() -> None:
    topologymgr = TopologyManager()
    await topologymgr.merge_registry(
        (
            _member("node-a", epoch=5),
            _member("node-b", epoch=5),
            _member("node-c", epoch=5),
            _member("node-d", epoch=5),
        )
    )

    class _PagedGossipService(GossipService):
        GOSSIP_DELTA_SIZE = 2

    service = _PagedGossipService(node_id="node-a", topologymgr=topologymgr)

    first_request = GossipDigest(
        sender="node-b",
        has_more=False,
        after_node_id="",
        digest=[
            GossipNodeDigest(node_id="node-a", epoch=1),
            GossipNodeDigest(node_id="node-b", epoch=1),
            GossipNodeDigest(node_id="node-c", epoch=1),
            GossipNodeDigest(node_id="node-d", epoch=1),
        ],
    )

    pages = [page async for page in service.delta(first_request)]
    first = pages[0]
    second = pages[1]

    assert [member.node_id for member in first.members] == ["node-a", "node-b"]
    assert first.has_more is True
    assert first.after_node_id == "node-b"

    assert [member.node_id for member in second.members] == ["node-c", "node-d"]
    assert second.has_more is False
    assert second.after_node_id == ""


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_gossip_service_delta_returns_empty_page_for_unknown_after_node_id() -> (
    None
):
    topologymgr = TopologyManager()
    await topologymgr.merge_registry(
        (_member("node-a", epoch=2), _member("node-b", epoch=2))
    )
    service = GossipService(node_id="node-a", topologymgr=topologymgr)

    request = GossipDigest(
        sender="node-b",
        has_more=True,
        after_node_id="node-z",
        digest=[
            GossipNodeDigest(node_id="node-a", epoch=1),
            GossipNodeDigest(node_id="node-b", epoch=1),
        ],
    )

    responses = [response async for response in service.delta(request)]
    response = responses[0]

    assert response.members == []
    assert response.has_more is False
    assert response.after_node_id == ""
