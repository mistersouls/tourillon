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
from tourillon.core.models.member import Member, MemberPhase


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_apply_member_updates_ring_and_records_span() -> None:
    manager = TopologyManager()

    accepted = await manager.apply_member(
        Member(
            node_id="node-a",
            peer_address="127.0.0.1:7001",
            epoch=1,
            phase=MemberPhase.READY,
            tokens=("b", "16"),
        )
    )

    topology = await manager.snapshot()
    assert accepted is True
    assert len(topology.ring) == 2

    assert topology.registry.get("node-a") is not None


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_apply_member_rejects_stale_record() -> None:
    manager = TopologyManager()

    await manager.apply_member(
        Member(
            node_id="node-a",
            peer_address="127.0.0.1:7001",
            epoch=2,
            phase=MemberPhase.READY,
            tokens=("b",),
        )
    )

    accepted = await manager.apply_member(
        Member(
            node_id="node-a",
            peer_address="127.0.0.1:7001",
            epoch=1,
            phase=MemberPhase.DRAINING,
            tokens=("b",),
        )
    )

    topology = await manager.snapshot()
    assert accepted is False
    assert topology.registry.get("node-a").phase == MemberPhase.READY  # type: ignore[union-attr]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_merge_registry_returns_accepted_count() -> None:
    manager = TopologyManager()

    accepted = await manager.merge_registry(
        [
            Member(
                node_id="node-a",
                peer_address="127.0.0.1:7001",
                epoch=1,
                phase=MemberPhase.JOINING,
                tokens=("a", "14"),
            ),
            Member(
                node_id="node-a",
                peer_address="127.0.0.1:7001",
                epoch=2,
                phase=MemberPhase.READY,
                tokens=("a", "14"),
            ),
            Member(
                node_id="node-a",
                peer_address="127.0.0.1:7001",
                epoch=1,
                phase=MemberPhase.READY,
                tokens=("a", "14"),
            ),
        ]
    )

    topology = await manager.snapshot()
    assert accepted == 1
    assert len(topology.ring) == 2

    assert topology.registry.get("node-a").epoch == 2  # type: ignore[union-attr]


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_member_fingerprint_cache_and_invalidation() -> None:
    manager = TopologyManager()

    await manager.apply_member(
        Member(
            node_id="node-b",
            peer_address="127.0.0.1:7002",
            epoch=1,
            phase=MemberPhase.JOINING,
            tokens=("1e",),
        )
    )

    fp1 = await manager.member_fingerprint()
    fp2 = await manager.member_fingerprint()
    assert fp1 == fp2

    await manager.apply_member(
        Member(
            node_id="node-b",
            peer_address="127.0.0.1:7002",
            epoch=2,
            phase=MemberPhase.READY,
            tokens=("1e",),
        )
    )
    fp3 = await manager.member_fingerprint()
    assert fp3 != fp1


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_draining_to_idle_removes_node_vnodes_from_ring() -> None:
    manager = TopologyManager()

    await manager.apply_member(
        Member(
            node_id="node-c",
            peer_address="127.0.0.1:7003",
            epoch=1,
            phase=MemberPhase.READY,
            tokens=("65", "ca"),
        )
    )
    assert len((await manager.snapshot()).ring) == 2

    await manager.apply_member(
        Member(
            node_id="node-c",
            peer_address="127.0.0.1:7003",
            epoch=2,
            phase=MemberPhase.DRAINING,
            tokens=("65", "ca"),
        )
    )
    assert len((await manager.snapshot()).ring) == 2

    await manager.apply_member(
        Member(
            node_id="node-c",
            peer_address="127.0.0.1:7003",
            epoch=3,
            phase=MemberPhase.IDLE,
            tokens=("65", "ca"),
        )
    )
    assert len((await manager.snapshot()).ring) == 0


def _node_b(
    epoch: int, phase: MemberPhase, tokens: tuple[str, ...] = ("10", "80")
) -> Member:
    return Member(
        node_id="node-b",
        peer_address="127.0.0.1:7002",
        epoch=epoch,
        phase=phase,
        tokens=tokens,
    )


@pytest.mark.tourillon
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("seen", "expected_tokens"),
    [
        # node-b drained then left; this node never received the DRAINING update.
        ([_node_b(1, MemberPhase.READY), _node_b(3, MemberPhase.IDLE)], ()),
        # First contact with node-b while it drains: it still owns its data.
        ([_node_b(5, MemberPhase.DRAINING)], ("10", "80")),
        # First contact with node-b while it is paused: it still owns its data.
        ([_node_b(5, MemberPhase.PAUSED)], ("10", "80")),
        # node-b left and re-joined with new tokens; IDLE/JOINING were missed.
        (
            [
                _node_b(1, MemberPhase.READY),
                _node_b(9, MemberPhase.READY, tokens=("20", "90")),
            ],
            ("20", "90"),
        ),
    ],
    ids=["missed-draining", "first-seen-draining", "first-seen-paused", "new-tokens"],
)
async def test_ring_reflects_latest_member_state(
    seen: list[Member], expected_tokens: tuple[str, ...]
) -> None:
    """Gossip only guarantees the latest member state, not every transition.

    The ring must therefore be a function of that latest state: vnodes of
    READY, PAUSED and DRAINING members.
    """
    manager = TopologyManager()
    for member in seen:
        await manager.apply_member(member)

    ring = (await manager.snapshot()).ring

    assert [vnode.token for vnode in ring] == [
        int(token, 16) for token in expected_tokens
    ]
