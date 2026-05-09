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
"""FileStateAdapter tests — scenarios 16, 17, 18, 20."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from tourillon.core.lifecycle.member import MemberPhase
from tourillon.core.lifecycle.state import NodeState
from tourillon.core.ports.state import StateError
from tourillon.infra.store.state import FileStateAdapter

pytestmark = pytest.mark.ring


@pytest.fixture()
def state_path(tmp_path: Path) -> Path:
    return tmp_path / "state.toml"


@pytest.mark.ring
async def test_16_load_returns_none_when_no_file(state_path: Path) -> None:
    """Returns None if the file does not exist."""
    adapter = FileStateAdapter(state_path)
    result = await adapter.load()
    assert result is None


@pytest.mark.ring
async def test_17_load_parses_valid_state_toml(state_path: Path) -> None:
    """Returns NodeState(phase=READY, generation=1, seq=1, tokens=(14,87), epoch=1)."""
    state_path.write_text(
        '[node]\nphase = "ready"\ngeneration = 1\nseq = 1\ntokens = [14, 87]\n\n'
        "[topology]\nepoch = 1\n",
        encoding="utf-8",
    )
    adapter = FileStateAdapter(state_path)
    result = await adapter.load()

    assert result is not None
    assert result.phase == MemberPhase.READY
    assert result.generation == 1
    assert result.seq == 1
    assert result.tokens == (14, 87)
    assert result.epoch == 1


@pytest.mark.ring
async def test_18_save_then_load_round_trips_state(state_path: Path) -> None:
    """Round-trip: loaded state equals saved state."""
    adapter = FileStateAdapter(state_path)
    original = NodeState(
        phase=MemberPhase.READY,
        generation=2,
        seq=3,
        tokens=(100, 200, 50, 150),
        epoch=5,
    )
    await adapter.save(original)
    loaded = await adapter.load()

    assert loaded == original


@pytest.mark.ring
async def test_20_load_ignores_tmp_file_left_by_crashed_write(
    tmp_path: Path,
) -> None:
    """Old state returned; temp file is ignored; no StateError."""
    state_path = tmp_path / "state.toml"
    old_state = NodeState(
        phase=MemberPhase.IDLE,
        generation=0,
        seq=0,
        tokens=(),
        epoch=0,
    )

    # Write the original state
    adapter = FileStateAdapter(state_path)
    await adapter.save(old_state)

    # Simulate a crash: write a bad tmp file without calling os.replace
    tmp_path_file = state_path.with_suffix(".tmp")
    tmp_path_file.write_text("corrupted content", encoding="utf-8")

    # Fresh adapter should load the original state, not the tmp file
    adapter2 = FileStateAdapter(state_path)
    loaded = await adapter2.load()

    assert loaded == old_state


@pytest.mark.ring
async def test_load_raises_state_error_on_corrupt_toml(state_path: Path) -> None:
    """load() raises StateError when state.toml contains invalid TOML."""
    state_path.write_text("not valid [[[ toml !!!", encoding="utf-8")
    adapter = FileStateAdapter(state_path)
    with pytest.raises(StateError, match="Cannot read"):
        await adapter.load()


@pytest.mark.ring
async def test_load_raises_state_error_on_malformed_structure(state_path: Path) -> None:
    """load() raises StateError when state.toml is valid TOML but missing required keys."""
    state_path.write_text("[topology]\nepoch = 1\n", encoding="utf-8")
    adapter = FileStateAdapter(state_path)
    with pytest.raises(StateError, match="Malformed"):
        await adapter.load()


@pytest.mark.ring
async def test_save_raises_state_error_on_write_failure(state_path: Path) -> None:
    """save() raises StateError when writing the temp file fails."""
    adapter = FileStateAdapter(state_path)
    state = NodeState(
        phase=MemberPhase.READY,
        generation=1,
        seq=0,
        tokens=(10, 20),
        epoch=1,
    )
    with (
        patch.object(Path, "write_text", side_effect=OSError(13, "permission denied")),
        pytest.raises(StateError, match="Cannot write"),
    ):
        await adapter.save(state)


@pytest.mark.ring
async def test_fsync_dir_swallows_os_error(tmp_path: Path) -> None:
    """_fsync_dir does not raise even when os.fsync raises OSError."""
    from tourillon.infra.store.state import _fsync_dir  # noqa: PLC2701

    with patch("os.fsync", side_effect=OSError("not supported")):
        _fsync_dir(tmp_path)  # must not raise


@pytest.mark.ring
def test_fsync_dir_covers_inner_try_finally(tmp_path: Path) -> None:
    """_fsync_dir covers inner try/finally when os.open succeeds then os.fsync fails."""
    from tourillon.infra.store.state import _fsync_dir  # noqa: PLC2701

    fake_fd = 999
    with (
        patch("tourillon.infra.store.state.os.open", return_value=fake_fd),
        patch(
            "tourillon.infra.store.state.os.fsync", side_effect=OSError("unsupported")
        ),
        patch("tourillon.infra.store.state.os.close"),
    ):
        _fsync_dir(tmp_path)  # OSError must be swallowed
