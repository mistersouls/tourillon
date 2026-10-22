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

import ssl
from dataclasses import dataclass

from tourclient.exception import UnknownServiceError
from tourlib import ValidationError
from tourlib.exceptions import ProtocolError, ResponseError


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
    action_failed="Node inspect failed",
    tls_failed="TLS setup or handshake failed during node inspect",
    refused="Tourillon node refused the inspect connection",
    unreachable="Unable to reach the Tourillon node for inspect",
    invalid_response="Tourillon node returned an invalid inspect response",
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
