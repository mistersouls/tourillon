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
from collections.abc import AsyncIterator
from typing import cast

import pytest

from tourillon.core.gossip.bootstrapper import GossipBootstrapper
from tourillon.core.gossip.topology import TopologyManager
from tourillon.core.models.member import Member
from tourillon.core.models.retry import BackoffRetry
from tourlib.adapter import MsgPackSerializer
from tourlib.envelope import Envelope, build_envelope
from tourlib.transport import EnvelopeClient


class _StreamClientStub:
    def __init__(self, responses: list[Envelope]) -> None:
        self._responses = responses
        self.requests: list[Envelope] = []

    async def stream(self, envelope: Envelope) -> AsyncIterator[Envelope]:
        self.requests.append(envelope)
        for response in self._responses:
            yield response


def _build_bootstrapper() -> GossipBootstrapper:
    return GossipBootstrapper(
        node_id="node-a",
        serializer=MsgPackSerializer(),
        topologymgr=TopologyManager(),
        backoff=BackoffRetry(),
        tls_client=ssl.create_default_context(),
    )

def _member(node_id: str, *, epoch: int) -> dict[str, object]:
    return {
        "node_id": node_id,
        "peer_address": f"127.0.0.1:{4000 + epoch}",
        "phase": "ready",
        "epoch": epoch,
        "tokens": [epoch, epoch + 100],
    }


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_bootstrapper_exchange_digest_reads_gossip_delta_pages() -> None:
    bootstrapper = _build_bootstrapper()
    serializer = MsgPackSerializer()
    responses = [
        build_envelope(
            kind="gossip.delta",
            payload=serializer.encode({"members": [_member("node-b", epoch=2)], "has_more": True}),
            schema_id=serializer.schema_id,
        ),
        build_envelope(
            kind="gossip.delta",
            payload=serializer.encode({"members": [_member("node-c", epoch=3)], "has_more": False}),
            schema_id=serializer.schema_id,
        ),
    ]
    client = _StreamClientStub(responses)

    members = await bootstrapper._exchange_digest(cast(EnvelopeClient, cast(object, client)), "127.0.0.1:4000")

    assert [member.node_id for member in members] == ["node-b", "node-c"]
    assert client.requests[0].kind == "gossip.digest"


@pytest.mark.tourillon
def test_bootstrapper_decode_delta_skips_invalid_members() -> None:
    members = GossipBootstrapper._decode_delta(
        [
            _member("node-b", epoch=2),
            {"node_id": "bad-member"},
        ]
    )

    assert [member.node_id for member in members] == ["node-b"]
    assert all(isinstance(member, Member) for member in members)

