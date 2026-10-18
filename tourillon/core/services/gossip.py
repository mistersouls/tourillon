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

from tourillon.core.gossip.topology import TopologyManager
from tourillon.core.models.gossip import (
    GossipDelta,
    GossipDigest,
    GossipPing,
    GossipPong,
    GossipPush,
    GossipPushAccept,
)
from tourillon.core.models.member import Member


class GossipService:
    GOSSIP_DELTA_SIZE = 1024

    def __init__(
        self,
        node_id: str,
        topologymgr: TopologyManager,
    ) -> None:
        self._node_id = node_id
        self._topologymgr = topologymgr

    async def accept(self, request: GossipPush) -> GossipPushAccept:
        accepted = 0
        ignored = 0

        for member in request.members:
            # check partition shift
            if await self._topologymgr.apply_member(member):
                accepted += 1
            else:
                ignored += 1

        return GossipPushAccept(
            sender=self._node_id, accepted=accepted, ignored=ignored
        )

    async def delta(self, request: GossipDigest) -> AsyncIterator[GossipDelta]:
        topology = await self._topologymgr.snapshot()
        remote_epochs: dict[str, int] = {
            item.node_id: item.epoch for item in request.digest
        }
        delta_members: list[Member] = []
        local_node_ids: set[str] = set()

        for member in sorted(topology.registry, key=lambda item: item.node_id):
            remote_epoch = remote_epochs.get(member.node_id)
            local_node_ids.add(member.node_id)
            if remote_epoch is None:
                delta_members.append(member)
                continue

            if member.epoch > remote_epoch:
                delta_members.append(member)

        wanted = sorted(set(remote_epochs) - local_node_ids)
        cursor = request.after_node_id
        if cursor:
            start = next(
                (
                    idx
                    for idx, member in enumerate(delta_members)
                    if member.node_id > cursor
                ),
                len(delta_members),
            )
        else:
            start = 0

        while True:
            end = min(start + self.GOSSIP_DELTA_SIZE, len(delta_members))
            members = delta_members[start:end]
            has_more = end < len(delta_members)
            after_node_id = members[-1].node_id if has_more and members else ""

            yield GossipDelta(
                sender=self._node_id,
                members=members,
                has_more=has_more,
                after_node_id=after_node_id,
                wanted=wanted,
            )

            if not has_more:
                return

            start = end

    async def pong(self, request: GossipPing) -> GossipPong:
        topology = await self._topologymgr.snapshot()
        fingerprint = await self._topologymgr.member_fingerprint()
        member_count = len(topology.registry)
        same = request.fingerprint == fingerprint and request.count == member_count
        return GossipPong(
            sender=self._node_id,
            same=same,
        )
