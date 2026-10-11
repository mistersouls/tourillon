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

from collections.abc import AsyncIterator, Callable, Mapping

from tourillon.core.log import logger
from tourlib.envelope import Envelope

EnvelopeHandler = Callable[[Envelope], AsyncIterator[Envelope]]


class Dispatcher:
    """Kind-based handler registry for envelope transport."""

    def __init__(self, handlers: Mapping[str, EnvelopeHandler] | None = None) -> None:
        self._handlers: dict[str, EnvelopeHandler] = {}
        if handlers:
            for kind, handler in handlers.items():
                self.register(kind, handler)

    def register(self, kind: str, handler: EnvelopeHandler) -> None:
        if kind in self._handlers:
            logger.error("Handler already registered for kind '%s'", kind)
            raise ValueError(f"handler already registered for kind: {kind}")
        logger.debug("Registering handler for kind '%s'", kind)
        self._handlers[kind] = handler

    def lookup(self, kind: str) -> EnvelopeHandler | None:
        return self._handlers.get(kind)

    def on(self, kind: str) -> Callable[[EnvelopeHandler], EnvelopeHandler]:
        def _decorator(handler: EnvelopeHandler) -> EnvelopeHandler:
            self.register(kind, handler)
            return handler

        return _decorator
