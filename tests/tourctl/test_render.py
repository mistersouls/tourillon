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

from tourctl.core.render import (
    block,
    fields,
    format_duration,
    format_since,
    format_size,
    messages,
    table,
)


@pytest.mark.tourctl
def test_fields_align_values_on_the_longest_label() -> None:
    assert fields([("Key", "a"), ("Version", "b"), ("Empty", "")]) == (
        "Key:     a\nVersion: b\nEmpty:"
    )


@pytest.mark.tourctl
def test_block_and_messages_indent_by_two_spaces() -> None:
    assert block("Value", "a\n\nb") == "Value:\n  a\n\n  b"
    assert messages(["one", "two"]) == "Messages:\n  one\n  two"


@pytest.mark.tourctl
def test_table_pads_columns_and_marks_missing_values() -> None:
    assert table(
        ("NAME", "PHASE", "NOTE"), [("node-1", "READY", None), ("n2", "", 3)]
    ) == ("NAME     PHASE   NOTE\n" "node-1   READY   -\n" "n2       -       3")


@pytest.mark.tourctl
def test_table_with_no_rows_prints_only_headers() -> None:
    assert table(("A", "B"), []) == "A   B"


@pytest.mark.tourctl
@pytest.mark.parametrize(
    ("seconds", "text"),
    [
        (-3, "0s"),
        (45, "45s"),
        (240, "4m"),
        (130, "2m10s"),
        (11100, "3h5m"),
        (187200, "2d4h"),
    ],
)
def test_format_duration_keeps_two_units(seconds: float, text: str) -> None:
    assert format_duration(seconds) == text


@pytest.mark.tourctl
def test_format_since_reads_past_and_future() -> None:
    assert format_since(1_000, now_ms=61_000) == "1m ago"
    assert format_since(61_000, now_ms=1_000) == "in 1m"


@pytest.mark.tourctl
@pytest.mark.parametrize(
    ("size", "text"),
    [
        (5, "5 B"),
        (1023, "1023 B"),
        (2458, "2.4 KiB"),
        (5 * 1024**2, "5.0 MiB"),
        (3 * 1024**5, "3072.0 TiB"),
    ],
)
def test_format_size_uses_binary_units(size: int, text: str) -> None:
    assert format_size(size) == text
