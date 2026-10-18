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

from dataclasses import asdict, is_dataclass
from enum import Enum
from typing import Any

import msgpack

from tourlib.constant import MSGPACK_SCHEMA_ID


def _default(obj: Any) -> Any:
    if isinstance(obj, Enum):
        return obj.value
    if is_dataclass(obj):
        return {k: _default(v) for k, v in asdict(obj).items()}
    if isinstance(obj, (list, tuple, set)):
        return [_default(x) for x in obj]
    if isinstance(obj, dict):
        return {k: _default(v) for k, v in obj.items()}
    return obj


class MsgPackSerializer:
    """MsgPack serializer adapter used for envelope payload documents."""

    schema_id = MSGPACK_SCHEMA_ID

    @staticmethod
    def encode(document: Any) -> bytes:
        try:
            return msgpack.packb(document, default=_default, use_bin_type=True)
        except (TypeError, ValueError) as exc:
            raise ValueError("failed to encode document with msgpack") from exc

    @staticmethod
    def decode(payload: bytes) -> Any:
        try:
            return msgpack.unpackb(payload, raw=False)
        except (
            TypeError,
            ValueError,
            msgpack.ExtraData,
            msgpack.FormatError,
            msgpack.StackError,
        ) as exc:
            raise ValueError("failed to decode msgpack payload") from exc
