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

from tourillon.core.utils.waitgroup import WaitGroup


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_waitgroup_wait_returns_immediately_when_no_tasks_are_registered() -> (
    None
):
    group = WaitGroup[str]()

    assert group.is_done() is False
    await group.add(0)
    success, failed = await group.wait()

    assert success == []
    assert failed == []
    assert group.is_done() is True


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_waitgroup_records_results_and_resets_on_new_cycle() -> None:
    group = WaitGroup[str]()

    await group.add(2)
    assert group.is_done() is False

    await group.done("alpha", True)
    assert group.is_done() is False

    await group.done("beta", False)
    success, failed = await group.wait()
    assert success == ["alpha"]
    assert failed == ["beta"]

    await group.add(1)
    await group.done()
    success, failed = await group.wait()
    assert success == []
    assert failed == []


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_waitgroup_rejects_negative_additions() -> None:
    group = WaitGroup[str]()

    with pytest.raises(ValueError, match="negative"):
        await group.add(-1)


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_waitgroup_rejects_extra_done_calls() -> None:
    group = WaitGroup[str]()

    await group.add(1)
    await group.done("alpha", True)

    with pytest.raises(RuntimeError, match="Too many calls to done"):
        await group.done("beta", False)
