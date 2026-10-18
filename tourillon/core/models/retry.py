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

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from tourlib.validation import ValidationError

_DELAY_FIELDS = ("initial_delay", "max_delay", "factor", "jitter")


@dataclass
class BackoffRetry:
    initial_delay: float = 0.5
    max_delay: float = 10.0
    max_retries: int = 10
    factor: float = 2.0
    jitter: float = 0.1

    def to_document(self) -> dict[str, Any]:
        return {
            "initial_delay": self.initial_delay,
            "max_delay": self.max_delay,
            "max_retries": self.max_retries,
            "factor": self.factor,
            "jitter": self.jitter,
        }

    @classmethod
    def from_document(
        cls, document: Mapping[str, Any], prefix: str = ""
    ) -> "BackoffRetry":
        delays: dict[str, float] = {}
        for name in _DELAY_FIELDS:
            value = document.get(name, getattr(cls, name))
            if not isinstance(value, int | float) or value < 0:
                raise ValidationError(f"{prefix}{name} must be a number >= 0")
            delays[name] = float(value)

        max_retries = document.get("max_retries", cls.max_retries)
        if not isinstance(max_retries, int) or max_retries < 1:
            raise ValidationError(f"{prefix}max_retries must be an integer >= 1")

        return cls(max_retries=max_retries, **delays)
