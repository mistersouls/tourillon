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

import pytest

from tourclient.exception import UnknownServiceError
from tourctl.core.error import handle_error
from tourlib import ValidationError
from tourlib.exceptions import ProtocolError, ResponseError


@pytest.mark.tourctl
@pytest.mark.parametrize(
    ("exc", "context_name", "expected"),
    [
        (FileNotFoundError("missing"), None, "Unable to load the Tourillon client config"),
        (KeyError("clusters.default"), None, "Missing client configuration entry"),
        (KeyError("prod"), "prod", "Unknown client context 'prod'"),
        (ValidationError("bad"), None, "Invalid Tourillon client configuration"),
        (ValueError("bad"), None, "Invalid Tourillon client configuration"),
        (ssl.SSLError("bad tls"), None, "TLS setup or handshake failed while contacting the Tourillon node"),
        (ConnectionRefusedError("nope"), None, "Tourillon node refused the connection"),
        (OSError("network down"), None, "Unable to reach the Tourillon node"),
        (ProtocolError("bad frame"), None, "returned an invalid response"),
        (ResponseError("bad response"), None, "returned an invalid response"),
        (UnknownServiceError("Unknown service: probe"), None, "Unknown service: probe"),
        (RuntimeError("boom"), None, "Request failed: boom"),
    ],
)
def test_handle_error_returns_expected_message(
    exc: Exception,
    context_name: str | None,
    expected: str,
) -> None:
    assert expected in handle_error(exc, context_name)

