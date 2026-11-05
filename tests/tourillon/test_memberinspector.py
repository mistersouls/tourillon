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

import asyncio
from contextlib import asynccontextmanager

import pytest

from tourillon.core.exceptions import MemberNotFoundError
from tourillon.core.gossip.topology import TopologyManager
from tourillon.core.models.member import Member, MemberPhase
from tourillon.core.partition.partitioner import Partitioner
from tourillon.core.services.memberinspector import MemberInspector
from tourillon.core.spaces.hashspace import HashSpace
from tourlib.adapter import MsgPackSerializer
from tourlib.envelope import Envelope, replay_envelope
from tourlib.models.ae import AeCounters, AeStatus, AeTarget
from tourlib.models.inspect import MemberView

pytestmark = pytest.mark.tourillon

_WALL_MS = 1_800_000_000_000


class _Members:
    def __init__(
        self, phases: dict[str, MemberPhase], statuses: dict[str, str]
    ) -> None:
        self._phases = phases
        self._statuses = statuses

    async def member_view(self, node_id: str) -> MemberView | None:
        phase = self._phases.get(node_id)
        if phase is None:
            return None
        return MemberView(
            node_id=node_id,
            address=f"{node_id}:7701",
            phase=phase.value.upper(),
            status=self.status_of(node_id, len(self._phases), 10),
            epoch=1,
            last_heard_s=840,
            since_s=840,
        )

    def status_of(self, node_id: str, count: int, now: float) -> str:
        return self._statuses.get(node_id, "ALIVE")


class _PeerPool:
    def __init__(
        self,
        serializer: MsgPackSerializer,
        debts: dict[str, AeTarget] | None = None,
        failures: set[str] | None = None,
        block: bool = False,
    ) -> None:
        self.serializer = serializer
        self.debts = debts or {}
        self.failures = failures or set()
        self.block = block
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.active = 0
        self.peak = 0

    @asynccontextmanager
    async def lease(self, peer: str):
        yield _Client(self, peer)

    async def request(self, peer: str, envelope: Envelope) -> Envelope:
        assert envelope.kind == "ae.status"
        assert self.serializer.decode(envelope.payload) == {
            "limit": 1,
            "cursor": None,
            "target": "node-0",
        }
        self.active += 1
        self.peak = max(self.peak, self.active)

        try:
            if self.block:
                if self.active == 16:
                    self.entered.set()
                await self.release.wait()
            if peer in self.failures:
                raise OSError("peer unavailable")
        finally:
            self.active -= 1

        item = self.debts.get(peer)
        items = () if item is None else (item,)
        result = AeStatus(
            node_id=peer,
            phase="READY",
            running=True,
            started_at_ms=None,
            snapshot_at_ms=_WALL_MS,
            stuck_after_s=60,
            counters=AeCounters(),
            items=items,
            targets=len(items),
            versions=0 if item is None else item.versions,
            stuck=0,
        )
        return replay_envelope(
            kind="ae.status.accept",
            request=envelope,
            payload=self.serializer.encode(result.to_document()),
        )


class _Client:
    def __init__(self, pool: _PeerPool, peer: str) -> None:
        self._pool = pool
        self._peer = peer

    async def request(self, envelope: Envelope, timeout: float) -> Envelope:
        assert timeout == 2.0
        return await self._pool.request(self._peer, envelope)


async def _inspector(
    count: int = 3,
    *,
    replication_factor: int = 2,
    failures: set[str] | None = None,
    block: bool = False,
) -> tuple[MemberInspector, _PeerPool]:
    partitioner = Partitioner(pshift=8, gshift=2, hashspace=HashSpace(bits=128))
    members = [
        Member(
            node_id=f"node-{index}",
            peer_address=f"node-{index}:7701",
            epoch=1,
            phase=MemberPhase.READY,
            tokens=(f"{(index + 1) * (partitioner.space.max // count):032x}",),
        )
        for index in range(count)
    ]
    topology = TopologyManager()
    await topology.merge_registry(members)
    phases = {member.node_id: member.phase for member in members}
    statuses = {"node-0": "DEAD"}
    serializer = MsgPackSerializer()
    debts = {
        "node-1": AeTarget(
            target="node-0",
            versions=3,
            oldest_wall_ms=_WALL_MS - 60_000,
            phase="READY",
            status="ALIVE",
        )
    }
    pool = _PeerPool(serializer, debts, failures, block)
    inspector = MemberInspector(
        node_id="node-viewer",
        replication_factor=replication_factor,
        partitioner=partitioner,
        topologymgr=topology,
        members=_Members(phases, statuses),  # type: ignore[arg-type]
        peer_pool=pool,  # type: ignore[arg-type]
        serializer=serializer,
    )
    return inspector, pool


async def test_member_report_shows_ring_share_debt_and_stand_ins() -> None:
    inspector, _ = await _inspector()

    report = await inspector.inspect("node-0")

    assert report.status == "DEAD"
    assert report.partitions > 0
    assert report.total_partitions == 256
    assert report.vnodes == 1
    assert report.neighbors == report.answered == 2
    assert report.owed_by[0].holder == "node-1"
    assert report.owed_by[0].versions == 3
    assert report.stand_ins


async def test_member_report_marks_a_neighbour_that_does_not_answer() -> None:
    inspector, _ = await _inspector(failures={"node-2"})

    report = await inspector.inspect("node-0")

    assert report.answered == 1
    assert report.neighbors == 2
    assert report.unanswered[0].node_id == "node-2"


async def test_member_report_rejects_an_unknown_member() -> None:
    inspector, _ = await _inspector()

    with pytest.raises(MemberNotFoundError, match="missing is not a member"):
        await inspector.inspect("missing")


async def test_member_report_bounds_peer_queries_to_sixteen() -> None:
    inspector, pool = await _inspector(count=20, replication_factor=20, block=True)
    task = asyncio.create_task(inspector.inspect("node-0"))

    await asyncio.wait_for(pool.entered.wait(), timeout=1)
    pool.release.set()
    report = await task

    assert pool.peak == 16
    assert report.neighbors == 19
