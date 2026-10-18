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

import shutil
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def journey_executables(pytestconfig: pytest.Config) -> dict[str, str]:
    executables = {}
    for name in ("tourillon", "tourctl"):
        supplied = pytestconfig.getoption(f"--{name}-bin")
        if supplied is not None:
            path = Path(supplied).expanduser().resolve()
            if not path.is_file():
                pytest.fail(f"--{name}-bin is not an executable file: {path}")
            executables[name] = str(path)
        else:
            resolved = shutil.which(name)
            if resolved is None:
                pytest.fail(f"{name} missing from PATH; supply --{name}-bin")
            executables[name] = resolved
    return executables
