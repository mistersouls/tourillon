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

import dataclasses
from pathlib import Path
from typing import Protocol


class PkiError(Exception):
    """Raised by PKI adapters for any certificate generation or I/O failure.

    This exception is the single surface that CLI commands catch. It wraps
    lower-level errors from the cryptography library, OSError, and permission
    failures so that callers never need to import cryptography internals.
    """


@dataclasses.dataclass(frozen=True)
class CaRequest:
    """Parameters required to generate a self-signed Certificate Authority.

    The CA produced from this request is the trust root for all mTLS
    connections in the cluster. The caller is responsible for choosing an
    appropriate validity window: cluster CAs typically use a multi-year
    validity while leaf certs use a shorter window.
    """

    common_name: str
    valid_days: int
    key_size: int
    out_cert: Path
    out_key: Path


@dataclasses.dataclass(frozen=True)
class CertRequest:
    """Parameters required to issue a leaf certificate signed by an existing CA.

    Both san_dns and san_ip may be empty tuples for client certificates.
    For server certificates at least one SAN entry is required; the CLI
    layer enforces this constraint before constructing a CertRequest.

    The CA certificate and private key are read ephemerally during signing
    and must not be stored on any cluster node after this operation completes.
    """

    common_name: str
    san_dns: tuple[str, ...]
    san_ip: tuple[str, ...]
    valid_days: int
    ca_cert: Path
    ca_key: Path
    out_cert: Path
    out_key: Path
    key_size: int = 2048


class CertificateAuthorityPort(Protocol):
    """Contract for generating a self-signed Certificate Authority key pair.

    Implementors must write both out_cert and out_key to disk with the
    private key file restricted to mode 0600 (owner read/write only). The
    operation must be effectively atomic: on failure, no partial file is left
    on disk. Raise PkiError for any error condition.
    """

    def generate_ca(self, request: CaRequest) -> None:
        """Generate a self-signed CA certificate and private key to disk."""


class CertificateIssuerPort(Protocol):
    """Contract for issuing a leaf certificate signed by an existing CA.

    The CA private key is consumed ephemerally. The issued certificate and
    its private key are written to disk. Private key mode must be 0600.
    Raise PkiError for cryptographic, I/O, or permission errors.
    """

    def issue_cert(self, request: CertRequest) -> None:
        """Issue a certificate signed by the CA described in the request."""
