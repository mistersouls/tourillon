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
from typing import Any


class UnknownServiceError(Exception):
    """Raise error for unknown service."""


class RequestRejectedError(Exception):
    """Raised when the node rejects a request with a ``<kind>.reject`` envelope.

    Carries the rejection details sent by the node: error code, operation,
    message and the per-replica error messages.
    """

    def __init__(self, request_kind: str, payload: Mapping[str, Any]) -> None:
        self.request_kind = request_kind
        self.code = payload.get("code")
        self.operation = payload.get("operation")
        self.errors = self._error_messages(payload.get("errors"))
        self.details = dict(payload)
        message = payload.get("message") or f"{request_kind} rejected"
        super().__init__(self._describe(str(message), self.errors))

    @staticmethod
    def _error_messages(raw: Any) -> tuple[str, ...]:
        if not isinstance(raw, list):
            return ()
        return tuple(
            str(item.get("message", "")) if isinstance(item, Mapping) else str(item)
            for item in raw
        )

    @staticmethod
    def _describe(message: str, errors: tuple[str, ...]) -> str:
        if not errors:
            return message
        return f"{message} ({'; '.join(errors)})"
