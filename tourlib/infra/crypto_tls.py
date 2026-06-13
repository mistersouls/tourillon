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
"""TLS context builders and validation."""

import base64
import ssl
from datetime import UTC, datetime

import cryptography.x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

from tourlib.exceptions import TlsValidationError


class CryptographyTlsContext:
    @staticmethod
    def validate_cert_not_expired(b64_cert_pem: str) -> None:
        """Validate that the certificate has not expired.

        Raise TlsValidationError if the certificate is expired.
        """
        try:
            cert_pem = base64.b64decode(b64_cert_pem)
            cert = cryptography.x509.load_pem_x509_certificate(
                cert_pem, backend=default_backend()
            )
            now = datetime.now(UTC)
            if cert.not_valid_after_utc < now:
                expired_date = cert.not_valid_after_utc.isoformat()
                raise TlsValidationError(
                    f"Certificate expired on {expired_date}. Generate a new CA first."
                )
        except TlsValidationError:
            raise
        except Exception as e:
            raise TlsValidationError(f"Failed to validate certificate: {e}") from e

    @staticmethod
    def validate_cert_key_match(b64_cert_pem: str, b64_key_pem: str) -> None:
        """Validate that the certificate and private key match.

        Raise TlsValidationError if they do not match or cannot be parsed.
        """
        try:
            cert_pem = base64.b64decode(b64_cert_pem)
            key_pem = base64.b64decode(b64_key_pem)

            cert = cryptography.x509.load_pem_x509_certificate(
                cert_pem, backend=default_backend()
            )
            private_key = serialization.load_pem_private_key(
                key_pem, password=None, backend=default_backend()
            )

            # Get public key from certificate and compare with private key's public key
            cert_public_key = cert.public_key()
            private_public_key = private_key.public_key()

            # For RSA keys, compare the public numbers
            cert_numbers = cert_public_key.public_numbers()
            private_numbers = private_public_key.public_numbers()

            if (
                cert_numbers.e != private_numbers.e
                or cert_numbers.n != private_numbers.n
            ):
                raise TlsValidationError(
                    "Certificate public key does not match private key's public key."
                )
        except TlsValidationError:
            raise
        except Exception as e:
            raise TlsValidationError(
                "CA private key does not match CA certificate public key."
            ) from e

    @staticmethod
    def build_server_ssl_context(
        b64_cert_pem: str, b64_key_pem: str, b64_ca_pem: str
    ) -> ssl.SSLContext:
        """Build an mTLS server SSL context.

        Enforces CERT_REQUIRED and validates client certificates against the CA.
        """
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_REQUIRED

        # Load server certificate and private key from base64 PEM
        cert_pem = base64.b64decode(b64_cert_pem)
        key_pem = base64.b64decode(b64_key_pem)

        # Write temporarily to load (or use load_cert_chain with file-like objects)
        # For simplicity, we'll use Python's ssl module's load_cert_chain with bytes workaround
        import tempfile

        with tempfile.NamedTemporaryFile(
            mode="wb", delete=False, suffix=".pem"
        ) as cert_file:
            cert_file.write(cert_pem)
            cert_file_path = cert_file.name

        with tempfile.NamedTemporaryFile(
            mode="wb", delete=False, suffix=".pem"
        ) as key_file:
            key_file.write(key_pem)
            key_file_path = key_file.name

        with tempfile.NamedTemporaryFile(
            mode="wb", delete=False, suffix=".pem"
        ) as ca_file:
            ca_pem = base64.b64decode(b64_ca_pem)
            ca_file.write(ca_pem)
            ca_file_path = ca_file.name

        try:
            ctx.load_cert_chain(cert_file_path, key_file_path)
            ctx.load_verify_locations(ca_file_path)
        finally:
            import os

            os.unlink(cert_file_path)
            os.unlink(key_file_path)
            os.unlink(ca_file_path)

        return ctx

    @staticmethod
    def build_client_ssl_context(
        b64_cert_pem: str, b64_key_pem: str, b64_ca_pem: str
    ) -> ssl.SSLContext:
        """Build an mTLS client SSL context.

        Enforces CERT_REQUIRED and validates server certificates against the CA.
        """
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_REQUIRED

        # Load client certificate and private key from base64 PEM
        cert_pem = base64.b64decode(b64_cert_pem)
        key_pem = base64.b64decode(b64_key_pem)

        # Load CA certificate for server verification
        import tempfile

        with tempfile.NamedTemporaryFile(
            mode="wb", delete=False, suffix=".pem"
        ) as cert_file:
            cert_file.write(cert_pem)
            cert_file_path = cert_file.name

        with tempfile.NamedTemporaryFile(
            mode="wb", delete=False, suffix=".pem"
        ) as key_file:
            key_file.write(key_pem)
            key_file_path = key_file.name

        with tempfile.NamedTemporaryFile(
            mode="wb", delete=False, suffix=".pem"
        ) as ca_file:
            ca_pem = base64.b64decode(b64_ca_pem)
            ca_file.write(ca_pem)
            ca_file_path = ca_file.name

        try:
            ctx.load_cert_chain(cert_file_path, key_file_path)
            ctx.load_verify_locations(ca_file_path)
        finally:
            import os

            os.unlink(cert_file_path)
            os.unlink(key_file_path)
            os.unlink(ca_file_path)

        return ctx
