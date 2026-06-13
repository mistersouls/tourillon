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
"""PKI port interfaces, request dataclasses, and PkiError.

The core layer depends only on these Protocol definitions. The concrete
adapter (CryptographyCaAdapter, CryptographyCertIssuerAdapter) lives in
tourillon/infra/pki/ and is the sole importer of the cryptography library.
"""

from __future__ import annotations

from typing import Protocol

from tourillon.core.structure.cert import CaRequest, CertRequest


class PkiError(Exception):
    """Raised by PKI adapters for any certificate generation or I/O failure.

    This exception is the single surface that CLI commands catch. It wraps
    lower-level errors from the cryptography library, OSError, and permission
    failures so that callers never need to import cryptography internals.
    """


class X509CertificateIssuer(Protocol):
    """..."""

    def generate_ca(self, request: CaRequest) -> None:
        """Generate a self-signed CA certificate and private key to disk."""

    def issue_cert(self, request: CertRequest) -> None:
        """Issue a certificate signed by the CA described in the request."""
