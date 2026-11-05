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

import json
import ssl
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from tourclient.exception import RequestRejectedError, UnknownServiceError
from tourlib import ValidationError
from tourlib.exceptions import ProtocolError, ResponseError

EXIT_FAILED = 1
EXIT_INVALID = 2
EXIT_UNREACHABLE = 3
EXIT_NOT_FOUND = 4
EXIT_TIMEOUT = 5


class CliError(SystemExit):
    """A user-facing failure carrying its exit code and a machine-readable code.

    It subclasses ``SystemExit`` so a command that raises it never prints a
    traceback; the entry point renders it on stderr, as text or JSON.
    """

    def __init__(
        self,
        message: str,
        code: int = EXIT_FAILED,
        *,
        error: str = "request_failed",
        details: Mapping[str, Any] | None = None,
        prefix: bool = True,
    ) -> None:
        super().__init__(code)
        self.message = message
        self.error = error
        self.details = dict(details or {})
        self.prefix = prefix

    def __str__(self) -> str:
        return self.message

    def render_text(self) -> str:
        return f"Error: {self.message}" if self.prefix else self.message

    def render_json(self) -> str:
        document = {
            "error": {
                "code": self.error,
                "message": self.message.split("\n", 1)[0],
                "details": self.details,
            }
        }
        return json.dumps(
            document, ensure_ascii=False, indent=2, sort_keys=True, default=str
        )


@dataclass(frozen=True)
class ErrorProfile:
    action_failed: str
    tls_failed: str
    refused: str
    unreachable: str
    invalid_response: str


GENERIC_ERROR_PROFILE = ErrorProfile(
    action_failed="Request failed",
    tls_failed="TLS setup or handshake failed while contacting the Tourillon node",
    refused="Tourillon node refused the connection",
    unreachable="Unable to reach the Tourillon node",
    invalid_response="Tourillon node returned an invalid response",
)

PROBE_ERROR_PROFILE = ErrorProfile(
    action_failed="Probe ping failed",
    tls_failed="TLS setup or handshake failed during probe ping",
    refused="Tourillon node refused the probe connection",
    unreachable="Unable to reach the Tourillon node for probe ping",
    invalid_response="Tourillon node returned an invalid probe response",
)

INSPECT_ERROR_PROFILE = ErrorProfile(
    action_failed="Status request failed",
    tls_failed="TLS setup or handshake failed during the status request",
    refused="Tourillon node refused the status connection",
    unreachable="Unable to reach the Tourillon node for status",
    invalid_response="Tourillon node returned an invalid status response",
)

KV_ERROR_PROFILE = ErrorProfile(
    action_failed="KV request failed",
    tls_failed="TLS setup or handshake failed during KV request",
    refused="Tourillon node refused the KV connection",
    unreachable="Unable to reach the Tourillon node for KV request",
    invalid_response="Tourillon node returned an invalid KV response",
)

NODE_JOIN_ERROR_PROFILE = ErrorProfile(
    action_failed="Node join failed",
    tls_failed="TLS setup or handshake failed during node join",
    refused="Tourillon node refused the join connection",
    unreachable="Unable to reach the Tourillon node for join",
    invalid_response="Tourillon node returned an invalid join response",
)

NODE_DRAIN_ERROR_PROFILE = ErrorProfile(
    action_failed="Node drain failed",
    tls_failed="TLS setup or handshake failed during node drain",
    refused="Tourillon node refused the drain connection",
    unreachable="Unable to reach the Tourillon node for drain",
    invalid_response="Tourillon node returned an invalid drain response",
)

NODE_PAUSE_ERROR_PROFILE = ErrorProfile(
    action_failed="Node pause failed",
    tls_failed="TLS setup or handshake failed during node pause",
    refused="Tourillon node refused the pause connection",
    unreachable="Unable to reach the Tourillon node for pause",
    invalid_response="Tourillon node returned an invalid pause response",
)

NODE_RESUME_ERROR_PROFILE = ErrorProfile(
    action_failed="Node resume failed",
    tls_failed="TLS setup or handshake failed during node resume",
    refused="Tourillon node refused the resume connection",
    unreachable="Unable to reach the Tourillon node for resume",
    invalid_response="Tourillon node returned an invalid resume response",
)

AE_ERROR_PROFILE = ErrorProfile(
    action_failed="Anti-entropy request failed",
    tls_failed="TLS setup or handshake failed during the anti-entropy request",
    refused="Tourillon node refused the anti-entropy connection",
    unreachable="Unable to reach the Tourillon node for anti-entropy",
    invalid_response="Tourillon node returned an invalid anti-entropy response",
)

GC_ERROR_PROFILE = ErrorProfile(
    action_failed="GC request failed",
    tls_failed="TLS setup or handshake failed during the GC request",
    refused="Tourillon node refused the GC connection",
    unreachable="Unable to reach the Tourillon node for GC",
    invalid_response="Tourillon node returned an invalid GC response",
)


def _config_error_message(exc: Exception, context_name: str | None) -> str | None:
    if isinstance(exc, FileNotFoundError):
        return f"Unable to load the Tourillon client config: {exc}"
    if isinstance(exc, KeyError):
        if context_name is not None:
            return f"Unknown client context '{context_name}' in the Tourillon client config."
        return f"Missing client configuration entry: {exc}"
    if isinstance(exc, (ValidationError, ValueError)):
        return f"Invalid Tourillon client configuration: {exc}"
    return None


def _transport_error_message(exc: Exception, profile: ErrorProfile) -> str | None:
    if isinstance(exc, ssl.SSLError):
        return f"{profile.tls_failed}: {exc}"
    if isinstance(exc, ConnectionRefusedError):
        return f"{profile.refused}: {exc}"
    if isinstance(exc, OSError):
        message = exc.strerror or str(exc)
        return f"{profile.unreachable}: {message}"
    if isinstance(exc, (ProtocolError, ResponseError)):
        return f"{profile.invalid_response}: {exc}"
    if isinstance(exc, UnknownServiceError):
        return str(exc)
    return None


def handle_error(
    exc: Exception,
    context_name: str | None,
    profile: ErrorProfile = GENERIC_ERROR_PROFILE,
) -> str:
    config_error = _config_error_message(exc, context_name)
    if config_error is not None:
        return config_error
    transport_error = _transport_error_message(exc, profile)
    if transport_error is not None:
        return transport_error
    return f"{profile.action_failed}: {exc}"


def _error_code(exc: Exception) -> tuple[int, str]:
    if isinstance(exc, (FileNotFoundError, KeyError, ValidationError, ValueError)):
        return EXIT_INVALID, "invalid_config"
    if isinstance(exc, UnknownServiceError):
        return EXIT_INVALID, "unknown_service"
    if isinstance(exc, ssl.SSLError):
        return EXIT_UNREACHABLE, "tls_failed"
    if isinstance(exc, OSError):
        return EXIT_UNREACHABLE, "unreachable"
    if isinstance(exc, (ProtocolError, ResponseError)):
        return EXIT_FAILED, "invalid_response"
    return EXIT_FAILED, "request_failed"


def cli_error(
    exc: Exception,
    context_name: str | None,
    profile: ErrorProfile = GENERIC_ERROR_PROFILE,
) -> CliError:
    """Classify ``exc`` into the exit codes every tourctl command shares."""
    if isinstance(exc, CliError):
        return exc
    if isinstance(exc, RequestRejectedError):
        return CliError(
            f"{profile.action_failed}: {exc}",
            EXIT_FAILED,
            error=str(exc.code or "rejected"),
            details=exc.details,
        )
    code, error = _error_code(exc)
    return CliError(handle_error(exc, context_name, profile), code, error=error)
