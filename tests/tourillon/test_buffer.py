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

from tourlib.utils import BytesWalker


@pytest.mark.tourillon
def test_read_remaining_returns_unread_suffix() -> None:
    walker = BytesWalker(b"abcdef")

    assert walker.read(2, "prefix") == b"ab"
    assert walker.read_remaining() == b"cdef"


@pytest.mark.tourillon
def test_read_remaining_from_end_returns_empty_bytes() -> None:
    walker = BytesWalker(b"abc")

    _ = walker.read_remaining()

    assert walker.read_remaining() == b""


@pytest.mark.tourillon
def test_read_after_read_remaining_raises_short_buffer_error() -> None:
    walker = BytesWalker(b"abc")

    _ = walker.read_remaining()

    with pytest.raises(ValueError, match="Buffer too short"):
        walker.read(1, "tail")

