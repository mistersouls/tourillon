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
"""Wire model of ``node.health``: a local verdict for supervision probes.

The node computes the verdict itself so a probe transfers a status and a few
reasons, never the whole node status.
"""

from collections.abc import Mapping
from dataclasses import asdict, dataclass
from typing import Any

from tourlib.exceptions import ValidationError

OK = "OK"
WARNING = "WARNING"
CRITICAL = "CRITICAL"
HEALTH_STATUSES = (OK, WARNING, CRITICAL)

# Thresholds shared by node.health and the messages of node status.
STORAGE_WARNING = 0.80
STORAGE_CRITICAL = 0.95
CERTIFICATE_WARNING_S = 14 * 86400.0
PENDING_WARNING_S = 600.0


@dataclass(frozen=True)
class HealthReason:
    code: str
    message: str
    critical: bool = False


@dataclass(frozen=True)
class NodeHealth:
    node_id: str
    status: str
    reasons: tuple[HealthReason, ...] = ()

    def __post_init__(self) -> None:
        if self.status not in HEALTH_STATUSES:
            raise ValidationError(
                f"health status must be among {', '.join(HEALTH_STATUSES)}"
            )

    @classmethod
    def from_reasons(cls, node_id: str, reasons: list[HealthReason]) -> "NodeHealth":
        """Critical reasons first; the status is the worst reason."""
        ordered = sorted(reasons, key=lambda reason: not reason.critical)
        if any(reason.critical for reason in ordered):
            status = CRITICAL
        else:
            status = WARNING if ordered else OK
        return cls(node_id=node_id, status=status, reasons=tuple(ordered))

    def to_document(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_document(cls, document: Any) -> "NodeHealth":
        if not isinstance(document, Mapping):
            raise ValidationError("node.health response must be a table")
        data = dict(document)
        reasons = data.get("reasons", ())
        if not isinstance(reasons, list | tuple):
            raise ValidationError("health reasons must be a list")
        data["reasons"] = tuple(
            HealthReason(**_mapping(item, "health reason")) for item in reasons
        )
        return cls(**data)


def _mapping(document: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(document, Mapping):
        raise ValidationError(f"{name} must be a table")
    return document
