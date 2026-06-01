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
"""MessagePack implementation of SerializerPort (schema_id = 1)."""

from __future__ import annotations

from typing import Any

import msgpack

# Extension type code for 128-bit unsigned integers (stored as 16-byte big-endian).
_EXT_UINT128: int = 1


def _default(obj: Any) -> Any:
    """Pack Python integers that exceed msgpack's uint64 range as ExtType 1."""
    if isinstance(obj, int) and obj >= 0 and obj.bit_length() > 63:
        return msgpack.ExtType(_EXT_UINT128, obj.to_bytes(16, "big"))
    raise TypeError(f"Unknown type: {type(obj)!r}")


def _ext_hook(code: int, data: bytes) -> Any:
    """Decode ExtType 1 back into a Python integer."""
    if code == _EXT_UINT128:
        return int.from_bytes(data, "big")
    return msgpack.ExtType(code, data)


class MsgpackSerializerAdapter:
    """Serialize and deserialize Envelope payloads using MessagePack.

    schema_id = 1 is written into every outgoing Envelope header and must
    match the value stored here. The core layer never imports this class
    directly; it depends only on SerializerPort and receives instances via
    constructor injection at startup.

    Integers larger than uint64 (e.g. 128-bit ring tokens) are transparently
    encoded as extension type 1 (16-byte big-endian) and decoded back to
    Python int on the receiving side.
    """

    schema_id: int = 1

    def encode(self, obj: Any) -> bytes:
        """Encode *obj* to MessagePack bytes."""
        return msgpack.packb(obj, use_bin_type=True, default=_default)  # type: ignore[no-any-return]

    def decode(self, data: bytes) -> Any:
        """Decode MessagePack bytes to a Python object."""
        return msgpack.unpackb(data, raw=False, ext_hook=_ext_hook)
