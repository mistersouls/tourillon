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
"""Value encoding helpers — inference, explicit type flags, and @ file prefix.

This module is imported by tourctl CLI commands to encode key/value/keyspace
arguments into msgpack bytes before sending them over the wire.  It does not
import any infra-layer packages; msgpack is used here because the CLI is
allowed to depend on third-party libraries (tourctl/ is not restricted to
core/).

The encoding rules are defined in proposal-kv-05162026-006, section
"Value encoding — inference and explicit types".
"""

from __future__ import annotations

import base64
import json
from pathlib import Path
from typing import Any


class EncodingError(ValueError):
    """Raised when an explicit type conversion fails."""


def _infer_value(raw: str) -> Any:  # noqa: ANN401
    """Apply the inference rule: try json.loads first, fall back to str."""
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, ValueError):
        return raw


def _to_int(raw: str) -> int:
    """Convert raw string to int or raise EncodingError."""
    try:
        return int(raw)
    except ValueError as exc:
        raise EncodingError(f"{raw!r} is not a valid integer") from exc


def _to_float(raw: str) -> float:
    """Convert raw string to float or raise EncodingError."""
    try:
        return float(raw)
    except ValueError as exc:
        raise EncodingError(f"{raw!r} is not a valid float") from exc


def _to_bool(raw: str) -> bool:
    """Convert raw string to bool or raise EncodingError."""
    if raw.lower() in ("true", "1", "yes"):
        return True
    if raw.lower() in ("false", "0", "no"):
        return False
    raise EncodingError(f"{raw!r} is not a valid bool; use true/false/1/0/yes/no")


def _to_json(raw: str) -> Any:  # noqa: ANN401
    """Parse raw as JSON or raise EncodingError."""
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, ValueError) as exc:
        raise EncodingError(f"{raw!r} is not valid JSON: {exc}") from exc


def _to_bytes(raw: str) -> bytes:
    """Decode raw as base64 or raise EncodingError."""
    try:
        return base64.b64decode(raw)
    except Exception as exc:
        raise EncodingError(f"{raw!r} is not valid base64: {exc}") from exc


def _apply_explicit_type(raw: str, type_flag: str) -> Any:  # noqa: ANN401
    """Convert raw to the type requested by an explicit -t / --kt / --ks flag.

    Raise EncodingError with a descriptive message on conversion failure.
    """
    _handlers = {
        "str": lambda r: r,
        "int": _to_int,
        "float": _to_float,
        "bool": _to_bool,
        "json": _to_json,
        "bytes": _to_bytes,
        "null": lambda _: None,
    }
    handler = _handlers.get(type_flag)
    if handler is None:
        raise EncodingError(f"unknown type flag: {type_flag!r}")
    return handler(raw)


def resolve_arg(raw: str, type_flag: str | None = None) -> str | bytes:
    """Resolve a CLI argument to a str/bytes Python object.

    Handles:
    - ``@@...`` escape → literal ``@...``
    - ``@path``        → read file bytes; apply type_flag if given
    - type_flag        → explicit conversion via _apply_explicit_type
    - no type_flag     → inference via _infer_value
    """
    if raw.startswith("@@"):
        # Escaped literal @
        raw = raw[1:]
        if type_flag is not None:
            return _apply_explicit_type(raw, type_flag)  # type: ignore[return-value]
        return _infer_value(raw)  # type: ignore[return-value]

    if raw.startswith("@"):
        # File read
        path = Path(raw[1:])
        data: str | bytes = path.read_bytes()
        if type_flag is not None:
            decoded = data.decode("utf-8", errors="replace")
            return _apply_explicit_type(decoded, type_flag)  # type: ignore[return-value]
        return data  # raw bytes

    if type_flag is not None:
        return _apply_explicit_type(raw, type_flag)  # type: ignore[return-value]

    return _infer_value(raw)  # type: ignore[return-value]
