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

"""Telemetry fake that records every metric update by metric name."""

from collections import defaultdict
from collections.abc import Mapping
from contextlib import AbstractContextManager
from typing import cast

from tourillon.core.ports.telemetry import AttributeValue, Span, Telemetry
from tourillon.core.telemetry import build_noop_telemetry


class _RecordedMetric:
    def __init__(self, calls: list[tuple[float, dict[str, AttributeValue]]]) -> None:
        self._calls = calls

    def add(
        self,
        amount: int | float = 1,
        *,
        attributes: Mapping[str, AttributeValue] | None = None,
    ) -> None:
        self._calls.append((amount, dict(attributes or {})))

    record = add
    set = add


class RecordingTelemetry:
    """Record metric calls as ``calls[name] == [(amount, attributes), ...]``."""

    def __init__(self) -> None:
        self.calls: dict[str, list[tuple[float, dict[str, AttributeValue]]]] = (
            defaultdict(list)
        )
        self._noop = build_noop_telemetry()

    def __getattr__(self, name: str) -> _RecordedMetric:
        if name.startswith("_"):
            raise AttributeError(name)
        return _RecordedMetric(self.calls[name])

    def start_span(
        self,
        name: str,
        *,
        attributes: Mapping[str, AttributeValue] | None = None,
    ) -> AbstractContextManager[Span]:
        return self._noop.start_span(name, attributes=attributes)

    def inject_context(self) -> bytes:
        return b""

    def extract_context(self, data: bytes) -> AbstractContextManager[None]:
        return self._noop.extract_context(data)

    def total(self, name: str, **attributes: AttributeValue) -> float:
        """Sum the amounts recorded on ``name`` whose labels include ``attributes``."""
        return sum(
            amount
            for amount, labels in self.calls[name]
            if all(labels.get(k) == v for k, v in attributes.items())
        )

    def as_port(self) -> Telemetry:
        return cast(Telemetry, cast(object, self))
