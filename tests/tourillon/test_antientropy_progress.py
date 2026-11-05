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
"""Join and drain progress published by the anti-entropy goals."""

from types import SimpleNamespace

import pytest

from tourillon.core.services.antientropy import (
    Exchange,
    GoalProgress,
    _join_progress,
    _tally_drain,
)

pytestmark = pytest.mark.tourillon


def test_join_progress_counts_pids_copied_by_any_session() -> None:
    primaries = {"node-a": [0, 1, 2], "node-b": [3, 4]}
    first = Exchange(reached={0: 1, 3: 1, 4: 0})
    fallback = Exchange(reached={1: 1})

    progress = _join_progress(primaries, first, fallback)

    assert progress == GoalProgress(
        "join", total=5, done=3, waiting={"node-a": 1, "node-b": 1}
    )


def test_join_progress_of_a_complete_pass_waits_on_nobody() -> None:
    progress = _join_progress({"node-a": [0]}, Exchange(reached={0: 1}))

    assert progress == GoalProgress("join", total=1, done=1)


def test_drain_tally_blames_the_unfinished_targets_page_by_page() -> None:
    view = SimpleNamespace(
        replicas={0: ("node-a", "node-b"), 1: ("node-a", "node-c"), 2: ("node-c",)}
    )
    progress = GoalProgress("drain")

    _tally_drain(
        progress,
        view,
        [0, 1],
        Exchange(reached={0: 2, 1: 1}, failed={"node-c"}),
    )
    _tally_drain(progress, view, [2], Exchange(skipped={"node-c"}))

    assert progress == GoalProgress("drain", total=3, done=1, waiting={"node-c": 2})
