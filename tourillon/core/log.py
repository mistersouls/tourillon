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

import logging
from typing import Any

from tourillon.core.models.config import LoggingConfig


class Logger:
    """Global logger facade; transport logger is accessed via transport()."""

    def __init__(self) -> None:
        self._global_logger = logging.getLogger("tourillon.global")
        self._transport_logger = logging.getLogger("tourillon.transport")
        self._storage_logger = logging.getLogger("tourillon.storage")

    def get(self) -> logging.Logger:
        return self._global_logger

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._global_logger.debug(msg, *args, **kwargs)

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._global_logger.info(msg, *args, **kwargs)

    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._global_logger.warning(msg, *args, **kwargs)

    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._global_logger.error(msg, *args, **kwargs)

    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._global_logger.exception(msg, *args, **kwargs)

    def transport(self) -> logging.Logger:
        return self._transport_logger

    def storage(self) -> logging.Logger:
        return self._storage_logger


logger = Logger()


def _resolve_level(name: str) -> int:
    level = logging.getLevelName(name)
    if isinstance(level, int):
        return level
    raise ValueError(f"Unsupported log level: {name}")


def init_loggers(config: LoggingConfig) -> None:
    """Apply logging settings from node configuration."""

    logging.basicConfig(
        level=_resolve_level(config.level),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        force=True,
    )

    known_loggers = {
        "global": logger.get(),
        "transport": logger.transport(),
        "storage": logger.storage(),
    }
    for name, logger_config in config.loggers.items():
        config_logger = known_loggers.get(name, logging.getLogger(f"tourillon.{name}"))
        config_logger.setLevel(_resolve_level(logger_config.level))
        config_logger.disabled = not logger_config.enabled

    enabled = [
        name for name, logger_config in config.loggers.items() if logger_config.enabled
    ]
    logger.debug(
        "Loggers initialized (root_level=%s, enabled=%s)",
        config.level,
        enabled,
    )
