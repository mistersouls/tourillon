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
"""Check that every Python source file contains the Apache 2.0 license header.

Usage (called automatically by pre-commit)::

    python scripts/check_license_header.py <file1> [file2 ...]

Exit code 1 if any file is missing the header; 0 otherwise.
"""

from __future__ import annotations

import sys

# These two markers must both appear somewhere in the first 20 lines.
_REQUIRED = (
    "# Copyright",
    "# Licensed under the Apache License, Version 2.0",
)


def _has_header(path: str) -> bool:
    """Return True when *path* contains all required header markers."""
    try:
        with open(path, encoding="utf-8") as fh:
            head = "".join(fh.readline() for _ in range(20))
    except OSError as exc:
        print(f"ERROR reading {path}: {exc}", file=sys.stderr)
        return False

    missing = [marker for marker in _REQUIRED if marker not in head]
    if missing:
        for marker in missing:
            print(f"{path}: missing license header marker: {marker!r}")
        return False
    return True


def main(argv: list[str] | None = None) -> int:
    """Entry-point – returns 0 on success, 1 on failure."""
    files = argv if argv is not None else sys.argv[1:]
    failed = [f for f in files if not _has_header(f)]
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
