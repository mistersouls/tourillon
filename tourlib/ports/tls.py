import ssl
from typing import Protocol


class TlsContext(Protocol):
    def validate_cert_key_match(self, b64_cert_pem: str, b64_key_pem: str) -> None:
        """Validate that the certificate matches the key data."""

    def validate_cert_not_expired(self, b64_cert_pem: str) -> None:
        """Validate that the certificate does not match the key data."""

    def build_server_ssl_context(
        self,
        b64_cert_pem: str,
        b64_key_pem: str,
        b64_ca_pem: str
    ) -> ssl.SSLContext:
        """Build a SSL context for a TCP server."""

    def build_client_ssl_context(
        self,
        b64_cert_pem: str,
        b64_key_pem: str,
        b64_ca_pem: str
    ) -> ssl.SSLContext:
        """Build a SSL context for a TCP client."""
