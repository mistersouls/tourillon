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

from tourlib.constant import MAX_KEY_LEN, MAX_NAMESPACE_LEN
from tourlib.models.ts import HLCTimestamp
from tourlib.models.version import Address, Meta, Version


@pytest.mark.tourlib
def test_address_from_bytes_validates_minimum_and_component_lengths() -> None:
    with pytest.raises(ValueError, match="too short to contain Address"):
        Address.from_bytes(b"\x01")

    with pytest.raises(ValueError, match="too short for namespace length"):
        Address.from_bytes(b"\x03ab")

    with pytest.raises(ValueError, match="too short for key length"):
        Address.from_bytes(b"\x02ns\x04ab")


@pytest.mark.tourlib
def test_address_to_bytes_rejects_namespace_or_key_overflow() -> None:
    with pytest.raises(OverflowError, match="Namespace length"):
        Address(namespace=b"n" * (MAX_NAMESPACE_LEN + 1), key=b"k").to_bytes()

    with pytest.raises(OverflowError, match="Key length"):
        Address(namespace=b"ns", key=b"k" * (MAX_KEY_LEN + 1)).to_bytes()


@pytest.mark.tourlib
def test_address_meta_and_version_round_trip() -> None:
    address = Address(namespace=b"ns", key=b"key")
    ts = HLCTimestamp(wall_ms=10, counter=2, node_id="node-a")
    meta = Meta(ts=ts, quorum_write=2)
    version = Version(address=address, data=b"value", meta=meta)

    assert Address.from_dict(address.to_dict()) == address
    assert Address.from_bytes(address.to_bytes()) == address
    assert Meta.from_dict(meta.to_dict()) == meta
    assert Version.from_dict(version.to_dict()) == version


@pytest.mark.tourlib
def test_hlc_timestamp_from_bytes_rejects_truncated_payload() -> None:
    with pytest.raises(ValueError, match="Buffer too short"):
        HLCTimestamp.from_bytes(b"\x00" * 9)


@pytest.mark.tourlib
def test_hlc_timestamp_walk_rejects_invalid_node_id_encoding() -> None:
    data = (
        (10).to_bytes(8, "big")
        + (1).to_bytes(2, "big")
        + (1).to_bytes(2, "big")
        + b"\xff"
    )

    with pytest.raises(UnicodeDecodeError):
        HLCTimestamp.from_bytes(data)
