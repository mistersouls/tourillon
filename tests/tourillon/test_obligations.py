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

import pytest

from tourillon.core.ae.obligations import ObligationQueue
from tourillon.core.models.storageschema import Key, ObligationKey, Pid
from tourlib.models.ts import HLCTimestamp
from tourlib.models.version import Address

pytestmark = pytest.mark.tourillon


def _obligation(index: int, target: str = "node-b") -> ObligationKey:
    key = Key(
        pid=Pid(1),
        address=Address(namespace=b"ns", key=f"k{index}".encode()),
        ts=HLCTimestamp(wall_ms=index, counter=0, node_id="node-a"),
    )
    return ObligationKey(target, key)


def test_capacity_must_be_positive() -> None:
    with pytest.raises(ValueError, match="capacity"):
        ObligationQueue(0)


def test_submit_deduplicates_and_keeps_submission_order() -> None:
    queue = ObligationQueue(8)
    first, second = _obligation(1), _obligation(2)

    assert queue.submit([first, second, first]) == 2
    assert queue.submit([second]) == 0
    assert len(queue) == 2 and not queue.idle

    assert queue.take(1) == [first]
    assert queue.to_dict()["inflight"] == 1
    assert queue.take(5) == [second]
    assert len(queue) == 0 and not queue.idle

    queue.settle(2)
    assert queue.idle
    assert queue.to_dict() == {
        "queued": 0,
        "inflight": 0,
        "capacity": 8,
        "backlog": True,
        "submitted": 2,
        "overflowed": 0,
    }


def test_overflow_raises_the_backlog_instead_of_dropping() -> None:
    queue = ObligationQueue(2)
    queue.backlog = False

    assert queue.submit([_obligation(i) for i in range(5)]) == 2
    assert queue.backlog
    assert queue.overflowed == 3
    assert queue.capacity == 2
    assert len(queue) == 2


async def test_wait_wakes_on_submit_and_honours_timeout() -> None:
    queue = ObligationQueue(4)

    await asyncio.wait_for(queue.wait(0.01), 1)  # times out quietly

    waiter = asyncio.create_task(queue.wait())
    await asyncio.sleep(0)
    assert not waiter.done()
    queue.submit([_obligation(1)])
    await asyncio.wait_for(waiter, 1)

    await asyncio.wait_for(queue.wait(), 1)  # already queued: returns at once


async def test_wait_idle_blocks_until_delivery_settles() -> None:
    queue = ObligationQueue(4)
    await asyncio.wait_for(queue.wait_idle(), 1)

    queue.submit([_obligation(1)])
    idle = asyncio.create_task(queue.wait_idle())
    batch = queue.take(4)
    await asyncio.sleep(0)
    assert not idle.done()

    queue.settle(len(batch))
    await asyncio.wait_for(idle, 1)
