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

import ssl
from pathlib import Path

import pytest

from tests.helpers import create_mtls_contexts


def pytest_addoption(parser: pytest.Parser) -> None:
    """Declare end-to-end options here so they are accepted by any invocation.

    pytest only collects `pytest_addoption` from the *initial* conftest files;
    declaring them in `tests/e2e/conftest.py` made them usable only when
    `tests/e2e` was passed explicitly on the command line.
    """
    group = parser.getgroup("general-e2e")
    group.addoption(
        "--journey-artifacts",
        default=None,
        help="Export user-journey reports and logs to this directory.",
    )
    for name in ("tourillon", "tourctl"):
        group.addoption(
            f"--{name}-bin",
            default=None,
            help=(
                f"Test this standalone {name} executable without Python environment "
                "variables or PATH; otherwise resolve it on PATH."
            ),
        )


@pytest.fixture
def mtls_contexts(tmp_path: Path) -> tuple[ssl.SSLContext, ssl.SSLContext]:
    """Shared mTLS contexts for transport tests."""
    return create_mtls_contexts(tmp_path)
