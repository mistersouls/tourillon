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

from pathlib import Path

import pytest

from tourlib.adapter import TomlPersistence


@pytest.mark.tourlib
def test_atomic_write_cleans_up_temp_file_on_replace_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "config.toml"
    temp_files_before = set(tmp_path.iterdir())

    def fail_replace(src: str, dst: str) -> None:
        raise OSError("replace failed")

    monkeypatch.setattr("tourlib.adapter.toml_persistence.os.replace", fail_replace)

    with pytest.raises(OSError, match="replace failed"):
        TomlPersistence._atomic_write(path, "answer = 42\n")

    assert not path.exists()
    assert set(tmp_path.iterdir()) == temp_files_before

