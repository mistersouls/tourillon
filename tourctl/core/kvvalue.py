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

"""How tourctl reads typed values from the command line and describes them."""

import base64
import binascii
import json
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO, Literal

from tourctl.core.render import format_size

TypeName = Literal["auto", "str", "int", "float", "bool", "json", "bytes"]

INLINE_LIMIT = 80
TEXT_LIMIT = 4096
_LIST_INLINE_LIMIT = 60
_BOOL_TRUE = {"1", "true", "yes", "on"}
_BOOL_FALSE = {"0", "false", "no", "off"}
_SCALARS = ("bool", "int", "float")


def _expected(type_name: str, text: str) -> ValueError:
    return ValueError(f"expected {type_name}, got {text!r}")


def _parse_int(text: str) -> int:
    try:
        return int(text, 10)
    except ValueError:
        raise _expected("int", text) from None


def _parse_float(text: str) -> float:
    try:
        return float(text)
    except ValueError:
        raise _expected("float", text) from None


def _parse_bool(text: str) -> bool:
    lowered = text.strip().lower()
    if lowered in _BOOL_TRUE:
        return True
    if lowered in _BOOL_FALSE:
        return False
    raise _expected("bool", text)


def _parse_json(text: str) -> Any:
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"expected JSON (line {exc.lineno} column {exc.colno})"
        ) from None


def _parse_bytes(text: str) -> bytes:
    try:
        return base64.b64decode(text, validate=True)
    except binascii.Error:
        raise _expected("base64", text) from None


def _looks_like_number(text: str) -> bool:
    stripped = text.strip().lower()
    return bool(stripped) and stripped.lstrip("+-")[:1].isdigit()


def _infer(text: str) -> Any:
    if text.strip().lower() in _BOOL_TRUE | _BOOL_FALSE:
        return _parse_bool(text)
    for parse in (_parse_int, _parse_float):
        if _looks_like_number(text):
            try:
                return parse(text)
            except ValueError:
                continue
    if text.strip().startswith(("{", "[", '"')):
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return text
    return text


_PARSERS: dict[str, Callable[[str], Any]] = {
    "auto": _infer,
    "str": str,
    "int": _parse_int,
    "float": _parse_float,
    "bool": _parse_bool,
    "json": _parse_json,
    "bytes": _parse_bytes,
}


def parse_typed(text: str, type_name: TypeName) -> Any:
    """Decode ``text`` as ``type_name``; ``ValueError`` explains a mismatch."""
    return _PARSERS[type_name](text)


def _from_payload(payload: bytes, type_name: TypeName | None) -> Any:
    """Files and stdin are bytes unless a type says how to read them."""
    if type_name in (None, "auto", "bytes"):
        return payload
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError:
        raise ValueError("content is not valid UTF-8") from None
    return parse_typed(text, type_name)


def _read_file(name: str) -> bytes:
    try:
        return Path(name).read_bytes()
    except FileNotFoundError:
        raise ValueError(f"file not found: {name}") from None
    except OSError as exc:
        raise ValueError(f"cannot read {name}: {exc.strerror or exc}") from None


def read_value(
    text: str, type_name: TypeName | None, stdin: BinaryIO | None = None
) -> Any:
    """Resolve ``-`` (stdin), ``@file`` and ``@@`` (a literal ``@``)."""
    if text == "-":
        return _from_payload((stdin or sys.stdin.buffer).read(), type_name)
    if text.startswith("@@"):
        return parse_typed(text[1:], type_name or "auto")
    if text.startswith("@"):
        return _from_payload(_read_file(text[1:]), type_name)
    return parse_typed(text, type_name or "auto")


def json_safe(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"base64": base64.b64encode(value).decode("ascii")}
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [json_safe(item) for item in value]
    return value


def display(value: Any) -> str:
    """One-line text for a namespace or a key."""
    if isinstance(value, str):
        return value
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("ascii")
    return json.dumps(json_safe(value), ensure_ascii=False)


def type_of(value: Any) -> str:
    for kind, name in ((bool, "bool"), (int, "int"), (float, "float")):
        if isinstance(value, kind):
            return name
    if isinstance(value, str):
        return "str"
    return "bytes" if isinstance(value, bytes) else "json"


def pretty_json(value: Any, depth: int = 0) -> str:
    """Indented JSON that keeps short lists of scalars on one line."""
    compact = json.dumps(value, ensure_ascii=False)
    if not isinstance(value, dict | list) or not value:
        return compact
    flat = isinstance(value, list) and not any(
        isinstance(item, dict | list) for item in value
    )
    if flat and len(compact) <= _LIST_INLINE_LIMIT:
        return compact
    pad = "  " * (depth + 1)
    if isinstance(value, dict):
        items = [
            f"{pad}{json.dumps(key, ensure_ascii=False)}: {pretty_json(item, depth + 1)}"
            for key, item in value.items()
        ]
        opening, closing = "{", "}"
    else:
        items = [f"{pad}{pretty_json(item, depth + 1)}" for item in value]
        opening, closing = "[", "]"
    return f"{opening}\n" + ",\n".join(items) + f"\n{'  ' * depth}{closing}"


@dataclass(frozen=True)
class ValueView:
    """A stored value with the type and size tourctl reports for it."""

    data: Any
    type: str

    @classmethod
    def of(cls, data: Any) -> "ValueView":
        if isinstance(data, dict | list | tuple):
            data = json_safe(data)
        return cls(data=data, type=type_of(data))

    @property
    def text(self) -> str:
        if self.type == "bool":
            return "true" if self.data else "false"
        if self.type == "json":
            return pretty_json(self.data)
        if self.type == "bytes":
            return ""
        return str(self.data)

    @property
    def encoded(self) -> bytes:
        """The bytes written by ``--output``."""
        if isinstance(self.data, bytes):
            return self.data
        return self.text.encode("utf-8")

    @property
    def size(self) -> int:
        if self.type == "json":
            return len(json.dumps(self.data, ensure_ascii=False).encode("utf-8"))
        return len(self.encoded)

    @property
    def summary(self) -> str:
        return f"{self.type}, {format_size(self.size)}"

    def inline(self) -> str | None:
        """The value on its label line, or None when it belongs in a block."""
        if self.type in _SCALARS:
            return self.text
        text = self.text
        if self.type == "str" and len(text) <= INLINE_LIMIT and "\n" not in text:
            return text
        return None

    def raw(self) -> bytes:
        """The bytes written by ``--raw``: text ends with a newline, bytes do not."""
        if isinstance(self.data, bytes):
            return self.data
        if self.type == "json":
            compact = json.dumps(self.data, ensure_ascii=False, separators=(",", ":"))
            return f"{compact}\n".encode()
        return f"{self.text}\n".encode()

    def to_json(self, *, with_data: bool) -> dict[str, Any]:
        document: dict[str, Any] = {"size": self.size, "type": self.type}
        if not with_data:
            return document
        if isinstance(self.data, bytes):
            document["base64"] = base64.b64encode(self.data).decode("ascii")
        else:
            document["data"] = self.data
        return document
