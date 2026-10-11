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

import re

from tourlib.constant import MAX_NODE_ID_LEN_BYTES
from tourlib.validation import (
    ValidationError,
    validate_node_id_size,
)

_ALLOWED_LOG_LEVELS = frozenset({"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"})
_BINARY_SIZE_UNITS: tuple[tuple[str, int], ...] = (
    ("Yi", 1 << 80),
    ("Zi", 1 << 70),
    ("Ei", 1 << 60),
    ("Pi", 1 << 50),
    ("Ti", 1 << 40),
    ("Gi", 1 << 30),
    ("Mi", 1 << 20),
    ("Ki", 1 << 10),
)
_GROUP_SIZE_PATTERN = re.compile(r"^(?P<bytes>\d+)(?P<unit>B|Ki|Mi|Gi|Ti|Pi|Ei|Zi|Yi)?$")


def validate_log_level(value: str, field_name: str) -> str:
    normalized = value.strip().upper()
    if not normalized:
        raise ValidationError(f"{field_name} must be a non-empty string")
    if normalized not in _ALLOWED_LOG_LEVELS:
        levels = ", ".join(sorted(_ALLOWED_LOG_LEVELS))
        raise ValidationError(f"{field_name} must be one of: {levels}")
    return normalized


def validate_group_size(value: str, field_name: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValidationError(f"{field_name} must be a non-empty size string")
    if _GROUP_SIZE_PATTERN.fullmatch(normalized) is None:
        allowed = ", ".join(["B", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi", "Yi"])
        raise ValidationError(f"{field_name} must be a positive size string using one of: {allowed}")
    quantity_bytes(normalized, field_name)
    return normalized


def quantity_bytes(value: str, field_name: str) -> int:
    match = _GROUP_SIZE_PATTERN.fullmatch(value.strip())
    if match is None:
        raise ValidationError(f"{field_name} must be a positive size string")
    amount = int(match.group("bytes"))
    if amount <= 0:
        raise ValidationError(f"{field_name} must be a positive size string")
    unit = match.group("unit") or "B"
    multiplier = dict(_BINARY_SIZE_UNITS).get(unit, 1)
    return amount * multiplier


def validate_node_id(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValidationError(f"{field_name} must be a non-empty string")
    try:
        validate_node_id_size(value)
    except ValueError as exc:
        raise ValidationError(f"{field_name} must be <= {MAX_NODE_ID_LEN_BYTES} bytes (utf-8)") from exc
    return value


