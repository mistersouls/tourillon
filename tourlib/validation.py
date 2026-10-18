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

from collections.abc import Callable, Mapping
from typing import Any

from tourlib.constant import MAX_NODE_ID_LEN_BYTES
from tourlib.exceptions import ValidationError


def validate_node_id_size(node_id: str) -> None:
    node_id_size = len(node_id.encode("utf-8"))
    if node_id_size > MAX_NODE_ID_LEN_BYTES:
        raise ValueError(f"node_id must be <= {MAX_NODE_ID_LEN_BYTES} bytes (utf-8)")


def require_named_table(
    document: Mapping[str, Any],
    key: str,
    factory: Callable[[Mapping[str, Any]], Any],
) -> dict[str, Any]:
    value = require_mapping(document, key)
    result: dict[str, Any] = {}
    for name, item in value.items():
        if not isinstance(name, str) or not name:
            raise ValidationError(f"{key} keys must be non-empty strings")
        if not isinstance(item, Mapping):
            raise ValidationError(f"{key}.{name} must be a table")
        result[name] = factory(item)
    return result


def require_mapping(document: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    value = document.get(key)
    if not isinstance(value, Mapping):
        raise ValidationError(f"{key} must be a table")
    return value


def require_str(document: Mapping[str, Any], key: str) -> str:
    value = document.get(key)
    if not isinstance(value, str) or not value:
        raise ValidationError(f"{key} must be a non-empty string")
    return value
