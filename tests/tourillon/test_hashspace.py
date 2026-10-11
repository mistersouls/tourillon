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

import hashlib

import pytest

from tourillon.core.spaces.hashspace import HashSpace


@pytest.mark.tourillon
def test_hashspace_rejects_invalid_bits() -> None:
    with pytest.raises(ValueError, match="bits must be >= 1"):
        HashSpace(bits=0)


@pytest.mark.tourillon
def test_hashspace_properties_expose_bits_and_max() -> None:
    space = HashSpace(bits=8)

    assert space.bits == 8
    assert space.max == 256


@pytest.mark.tourillon
def test_hashspace_hash_is_deterministic_and_within_range_for_truncated_space() -> None:
    space = HashSpace(bits=8)

    v1 = space.hash(b"namespace:key")
    v2 = space.hash(b"namespace:key")

    assert v1 == v2
    assert 0 <= v1 < space.max


@pytest.mark.tourillon
def test_hashspace_128_bits_matches_full_md5_integer() -> None:
    space = HashSpace(bits=128)
    digest = hashlib.md5(b"abc", usedforsecurity=False).digest()
    expected = int.from_bytes(digest, "big")

    assert space.hash(b"abc") == expected


@pytest.mark.tourillon
def test_hashspace_truncates_md5_to_requested_bit_width() -> None:
    full_digest = hashlib.md5(b"abc", usedforsecurity=False).digest()
    full_int = int.from_bytes(full_digest, "big")

    space = HashSpace(bits=12)

    assert space.hash(b"abc") == full_int >> (128 - 12)

