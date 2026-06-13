from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
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


@dataclass(frozen=True)
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
