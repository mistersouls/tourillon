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
"""HashSpace — circular integer domain for consistent hashing."""

from __future__ import annotations

import hashlib


class HashSpace:
    """Circular integer domain [0, 2**bits) for consistent hashing.

    In production, bits=128 gives 2^128 ≈ 3.4×10^38 positions. In tests,
    bits=8 preserves every structural invariant while keeping property-based
    tests tractable. Mixing instances with different bits values in the same
    cluster is a configuration error caught at startup.

    HashSpace is never a global singleton. Instantiate once at cluster
    bootstrap with a fixed bits value and inject it into every component.
    """

    def __init__(self, bits: int = 128) -> None:
        """Raise ValueError when bits < 1."""
        if bits < 1:
            raise ValueError(f"bits must be >= 1, got {bits}")
        self._bits = bits
        self._max = 1 << bits

    @property
    def bits(self) -> int:
        """Return the number of significant bits in this hash space."""
        return self._bits

    @property
    def max(self) -> int:
        """Return 2**bits — one past the last valid position."""
        return self._max

    def hash(self, value: bytes) -> int:
        """Return the MD5 hash of value truncated to bits significant bits.

        MD5 is used exclusively for its 128-bit deterministic output width;
        it is not a cryptographic primitive — mTLS provides all security.
        Output is right-shifted to bits significant bits when bits < 128.
        """
        digest = hashlib.md5(value, usedforsecurity=False).digest()  # noqa: S324
        full = int.from_bytes(digest, "big")
        if self._bits < 128:
            return full >> (128 - self._bits)
        return full
