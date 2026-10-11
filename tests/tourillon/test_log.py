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

import pytest

from tourillon.core.log import init_loggers, logger
from tourillon.core.models.config import LoggerConfig, LoggingConfig


@pytest.mark.tourillon
def test_init_loggers_applies_named_logger_configuration() -> None:
    global_logger = logging.getLogger("tourillon.global")
    transport_logger = logging.getLogger("tourillon.transport")
    previous_global_level = global_logger.level
    previous_global_disabled = global_logger.disabled
    previous_transport_level = transport_logger.level
    previous_transport_disabled = transport_logger.disabled

    try:
        init_loggers(
            LoggingConfig(
                level="INFO",
                loggers={
                    "global": LoggerConfig(level="INFO", enabled=True),
                    "transport": LoggerConfig(level="DEBUG", enabled=False),
                },
            )
        )

        assert global_logger.level == logging.INFO
        assert global_logger.disabled is False
        assert transport_logger.level == logging.DEBUG
        assert transport_logger.disabled is True
    finally:
        global_logger.setLevel(previous_global_level)
        global_logger.disabled = previous_global_disabled
        transport_logger.setLevel(previous_transport_level)
        transport_logger.disabled = previous_transport_disabled


@pytest.mark.tourillon
def test_init_loggers_rejects_unknown_logger_level() -> None:
    with pytest.raises(ValueError, match="Unsupported log level: TRACE"):
        init_loggers(
            LoggingConfig(
                level="INFO",
                loggers={
                    "global": LoggerConfig(level="TRACE", enabled=True),
                },
            )
        )


@pytest.mark.tourillon
def test_log_info_defaults_to_global_scope(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, logger="tourillon.global")

    logger.info("hello global")

    assert any(
        record.name == "tourillon.global" and record.getMessage() == "hello global"
        for record in caplog.records
    )


@pytest.mark.tourillon
def test_log_info_supports_transport_scope(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, logger="tourillon.transport")

    logger.transport().info("hello transport")

    assert any(
        record.name == "tourillon.transport" and record.getMessage() == "hello transport"
        for record in caplog.records
    )


@pytest.mark.tourillon
def test_log_info_supports_global_format_arguments(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, logger="tourillon.global")

    logger.info("hello %s", "world")

    assert any(
        record.name == "tourillon.global" and record.getMessage() == "hello world"
        for record in caplog.records
    )


@pytest.mark.tourillon
def test_log_info_supports_scoped_format_arguments(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, logger="tourillon.transport")

    logger.transport().info("hello %s", "wire")

    assert any(
        record.name == "tourillon.transport" and record.getMessage() == "hello wire"
        for record in caplog.records
    )


