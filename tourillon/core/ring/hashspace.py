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
"""Hash space helper used by the ring."""

from __future__ import annotations

import hashlib


class HashSpace:
    """Circular integer domain [0, 2**bits)."""

    def __init__(self, bits: int = 128) -> None:
        if bits < 1:
            raise ValueError(f"bits must be >= 1, got {bits}")
        self._bits = bits
        self._max = 1 << bits

    @property
    def bits(self) -> int:
        return self._bits

    @property
    def max(self) -> int:
        return self._max

    def hash(self, value: bytes) -> int:
        digest = hashlib.md5(value, usedforsecurity=False).digest()  # noqa: S324
        full = int.from_bytes(digest, "big")
        return full >> (128 - self._bits) if self._bits < 128 else full
