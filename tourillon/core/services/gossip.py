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

from collections.abc import AsyncIterator, Callable

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

GossipRelay = Callable[[list[Member], int, str], None]


class GossipService:
    """Responder side of the membership gossip protocol.

    ``accept`` applies pushed members and, when a relay hook is wired and the
    push still has hop budget left, hands the members that were actually newer
    to the hook with ``hop_limit - 1`` so they travel one more epidemic hop.
    ``delta`` answers an anti-entropy digest with the members the initiator
    lacks or holds stale, and lists in ``wanted`` the members for which the
    initiator is ahead (unknown locally or carrying a newer epoch).
    """

    GOSSIP_DELTA_SIZE = 1024

    def __init__(
        self,
        node_id: str,
        topologymgr: TopologyManager,
        relay: GossipRelay | None = None,
    ) -> None:
        self._node_id = node_id
        self._topologymgr = topologymgr
        self._relay = relay

    async def accept(self, request: GossipPush) -> GossipPushAccept:
        accepted: list[Member] = []
        ignored = 0

        for member in request.members:
            # check partition shift
            if await self._topologymgr.apply_member(member):
                accepted.append(member)
            else:
                ignored += 1

        if accepted and request.hop_limit > 1 and self._relay is not None:
            self._relay(accepted, request.hop_limit - 1, request.sender)

        return GossipPushAccept(
            sender=self._node_id, accepted=len(accepted), ignored=ignored
        )

    @staticmethod
    def _wanted(remote_epochs: dict[str, int], local: dict[str, int]) -> list[str]:
        return sorted(
            node_id
            for node_id, epoch in remote_epochs.items()
            if node_id not in local or epoch > local[node_id]
        )

    async def delta(self, request: GossipDigest) -> AsyncIterator[GossipDelta]:
        topology = await self._topologymgr.snapshot()
        remote_epochs: dict[str, int] = {
            item.node_id: item.epoch for item in request.digest
        }
        delta_members: list[Member] = []
        local_epochs: dict[str, int] = {}

        for member in sorted(topology.registry, key=lambda item: item.node_id):
            remote_epoch = remote_epochs.get(member.node_id)
            local_epochs[member.node_id] = member.epoch

            if remote_epoch is None:
                delta_members.append(member)
                continue

            if member.epoch > remote_epoch:
                delta_members.append(member)

        wanted = self._wanted(remote_epochs, local_epochs)
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
