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
"""Logging configuration for the tourillon daemon process.

Call setup_logging() as the very first statement in every Typer command so
that all subsequent code — including config loading and error handling — emits
through the configured root logger. The tourillon daemon produces zero terminal
output outside of logging; Console.print() and print() are forbidden.
"""

from __future__ import annotations

import logging
import sys

_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"

# DEBUG keeps the function name so developers can trace execution paths.
# INFO and above omit it: operators care about the module and the message.
_DEBUG_FORMAT = "%(asctime)s %(levelname)-8s [%(name)s:%(funcName)s] %(message)s"
_DEFAULT_FORMAT = "%(asctime)s %(levelname)-8s [%(name)s] %(message)s"

_VALID_LEVELS = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})


class _LevelAwareFormatter(logging.Formatter):
    """Format DEBUG records with function name; INFO and above without."""

    def __init__(self) -> None:
        super().__init__(datefmt=_DATE_FORMAT)
        self._debug = logging.Formatter(_DEBUG_FORMAT, datefmt=_DATE_FORMAT)
        self._default = logging.Formatter(_DEFAULT_FORMAT, datefmt=_DATE_FORMAT)

    def format(self, record: logging.LogRecord) -> str:
        """Delegate to the formatter appropriate for *record*'s level."""
        if record.levelno <= logging.DEBUG:
            return self._debug.format(record)
        return self._default.format(record)


def setup_logging(level: str = "INFO") -> None:
    """Configure the root logger for the tourillon daemon.

    Must be the first call in every Typer entry-point command, before config
    loading, lock acquisition, or any other initialisation step. This ensures
    that errors occurring during startup are routed through the log handler and
    not lost to an unconfigured logger.

    Raise ValueError when level is not one of DEBUG, INFO, WARNING, ERROR,
    CRITICAL so that misconfiguration is surfaced immediately.
    """
    upper = level.upper()
    if upper not in _VALID_LEVELS:
        raise ValueError(
            f"Invalid log level {level!r}. Valid values: {', '.join(sorted(_VALID_LEVELS))}"
        )
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(_LevelAwareFormatter())
    root = logging.getLogger()
    root.setLevel(upper)
    # Remove only stream handlers pointing at stderr/stdout (ours), leaving
    # any handlers added by test frameworks (e.g. pytest caplog) untouched.
    for existing in root.handlers[:]:
        if isinstance(existing, logging.StreamHandler) and existing.stream in (
            sys.stderr,
            sys.stdout,
        ):
            root.removeHandler(existing)
    root.addHandler(handler)
