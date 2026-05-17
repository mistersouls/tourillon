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
"""Unit tests for tourillon.core.kv.encoding — inference and type-flag rules."""

from __future__ import annotations

import base64
from pathlib import Path

import pytest

from tourillon.core.kv.encoding import EncodingError, resolve_arg

pytestmark = pytest.mark.kv

# ---------------------------------------------------------------------------
# Inference tests (no explicit type flag)
# ---------------------------------------------------------------------------


@pytest.mark.kv
def test_inference_plain_string_returns_str() -> None:
    """Plain non-JSON string falls back to msgpack str."""
    result = resolve_arg("Alice")
    assert result == "Alice"
    assert isinstance(result, str)


@pytest.mark.kv
def test_inference_integer_string_returns_int() -> None:
    """value encoded as msgpack int 42."""
    result = resolve_arg("42")
    assert result == 42
    assert isinstance(result, int)


@pytest.mark.kv
def test_inference_float_string_returns_float() -> None:
    """JSON-parseable float returns Python float."""
    result = resolve_arg("3.14")
    assert abs(result - 3.14) < 1e-9
    assert isinstance(result, float)


@pytest.mark.kv
def test_inference_true_string_returns_bool() -> None:
    """JSON 'true' inference returns Python True."""
    result = resolve_arg("true")
    assert result is True


@pytest.mark.kv
def test_inference_false_string_returns_bool() -> None:
    """JSON 'false' inference returns Python False."""
    result = resolve_arg("false")
    assert result is False


@pytest.mark.kv
def test_inference_null_string_returns_none() -> None:
    """JSON 'null' inference returns Python None."""
    result = resolve_arg("null")
    assert result is None


@pytest.mark.kv
def test_inference_json_list_returns_list() -> None:
    """JSON array string returns Python list."""
    result = resolve_arg("[1,2,3]")
    assert result == [1, 2, 3]


@pytest.mark.kv
def test_inference_json_dict_returns_dict() -> None:
    """JSON object string returns Python dict."""
    result = resolve_arg('{"a":1}')
    assert result == {"a": 1}


# ---------------------------------------------------------------------------
# Explicit type flag tests (scenario 14)
# ---------------------------------------------------------------------------


@pytest.mark.kv
def test_explicit_str_bypasses_inference() -> None:
    """value encoded as msgpack str '42'."""
    result = resolve_arg("42", "str")
    assert result == "42"
    assert isinstance(result, str)


@pytest.mark.kv
def test_explicit_int_converts_string() -> None:
    """Explicit int flag converts '100' to integer 100."""
    result = resolve_arg("100", "int")
    assert result == 100
    assert isinstance(result, int)


@pytest.mark.kv
def test_explicit_float_converts_string() -> None:
    """Explicit float flag converts '2.718' to float."""
    result = resolve_arg("2.718", "float")
    assert abs(result - 2.718) < 1e-9
    assert isinstance(result, float)


@pytest.mark.kv
def test_explicit_bool_true_variants() -> None:
    """'true', '1', 'yes' with bool flag all return True."""
    for raw in ("true", "1", "yes"):
        assert resolve_arg(raw, "bool") is True, f"Expected True for {raw!r}"


@pytest.mark.kv
def test_explicit_bool_false_variants() -> None:
    """'false', '0', 'no' with bool flag all return False."""
    for raw in ("false", "0", "no"):
        assert resolve_arg(raw, "bool") is False, f"Expected False for {raw!r}"


@pytest.mark.kv
def test_explicit_json_parses_json() -> None:
    """Explicit json flag forces json.loads even if inference would also work."""
    result = resolve_arg('{"host":"prod"}', "json")
    assert result == {"host": "prod"}


@pytest.mark.kv
def test_explicit_bytes_decodes_base64() -> None:
    """Explicit bytes flag decodes base64 to raw bytes."""
    # "hello" in base64
    b64 = base64.b64encode(b"hello").decode()
    result = resolve_arg(b64, "bytes")
    assert result == b"hello"


@pytest.mark.kv
def test_explicit_null_returns_none_regardless_of_value() -> None:
    """Explicit null flag returns None; value argument is ignored."""
    result = resolve_arg("anything-at-all", "null")
    assert result is None


# ---------------------------------------------------------------------------
# @@ escape (scenario 15)
# ---------------------------------------------------------------------------


@pytest.mark.kv
def test_double_at_escape_produces_literal_at_string() -> None:
    """value literal @alice, encoded as msgpack str."""
    result = resolve_arg("@@alice")
    # After stripping @@, raw="@alice" — fails json.loads → kept as str
    assert result == "@alice"
    assert isinstance(result, str)


@pytest.mark.kv
def test_double_at_with_explicit_type_applied_after_strip() -> None:
    """@@ with int type: @@42 → raw=@42, which -t int would reject."""
    with pytest.raises(EncodingError, match="not a valid integer"):
        resolve_arg("@@42abc", "int")


# ---------------------------------------------------------------------------
# @ prefix — file read
# ---------------------------------------------------------------------------


@pytest.mark.kv
def test_at_prefix_reads_file_as_raw_bytes(tmp_path: Path) -> None:
    """@ prefix reads file bytes raw when no type flag is given."""
    content = b"binary\x00data"
    f = tmp_path / "blob.bin"
    f.write_bytes(content)
    result = resolve_arg(f"@{f}")
    assert result == content


@pytest.mark.kv
def test_at_prefix_with_json_type_parses_file_content(tmp_path: Path) -> None:
    """@ prefix with -t json reads file and parses as JSON."""
    f = tmp_path / "data.json"
    f.write_text('{"x": 99}')
    result = resolve_arg(f"@{f}", "json")
    assert result == {"x": 99}


@pytest.mark.kv
def test_at_prefix_with_str_type_decodes_utf8(tmp_path: Path) -> None:
    """@ prefix with -t str decodes file bytes as UTF-8 string."""
    f = tmp_path / "name.txt"
    f.write_text("Alice")
    result = resolve_arg(f"@{f}", "str")
    assert result == "Alice"


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------


@pytest.mark.kv
def test_explicit_int_with_non_integer_raises_encoding_error() -> None:
    """@@id -t int raises EncodingError: '@id' is not a valid integer."""
    with pytest.raises(EncodingError, match="not a valid integer"):
        resolve_arg("not-an-int", "int")


@pytest.mark.kv
def test_explicit_float_with_non_float_raises_encoding_error() -> None:
    """Non-numeric string with -t float raises EncodingError."""
    with pytest.raises(EncodingError, match="not a valid float"):
        resolve_arg("abc", "float")


@pytest.mark.kv
def test_explicit_bool_with_invalid_value_raises_encoding_error() -> None:
    """Invalid bool value raises EncodingError."""
    with pytest.raises(EncodingError, match="not a valid bool"):
        resolve_arg("maybe", "bool")


@pytest.mark.kv
def test_explicit_json_with_invalid_json_raises_encoding_error() -> None:
    """Invalid JSON with -t json raises EncodingError."""
    with pytest.raises(EncodingError, match="not valid JSON"):
        resolve_arg("{not json}", "json")


@pytest.mark.kv
def test_explicit_bytes_with_invalid_base64_raises_encoding_error() -> None:
    """Invalid base64 with -t bytes raises EncodingError."""
    with pytest.raises(EncodingError, match="not valid base64"):
        resolve_arg("!!!bad_base64!!!", "bytes")


@pytest.mark.kv
def test_unknown_type_flag_raises_encoding_error() -> None:
    """Unrecognised type flag raises EncodingError."""
    with pytest.raises(EncodingError, match="unknown type flag"):
        resolve_arg("hello", "xmlstring")
