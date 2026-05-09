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
"""tourillon config subcommands — config.toml and contexts.toml generation."""

from __future__ import annotations

import base64
import logging
import os
import stat
import tempfile
from pathlib import Path
from typing import Annotated

import tomli_w
import typer

from tourillon.bootstrap.log import setup_logging
from tourillon.core.ports.pki import CertRequest, PkiError
from tourillon.core.structure.config import NodeSize
from tourillon.core.structure.contexts import (
    ClusterRef,
    ContextEntry,
    CredentialsConfig,
    EndpointsConfig,
)
from tourillon.infra.contexts import load_contexts, save_contexts
from tourillon.infra.pki.x509 import CryptographyCertIssuerAdapter

logger = logging.getLogger(__name__)

config_app = typer.Typer(no_args_is_help=True)


@config_app.command("generate")
def config_generate(
    ca_cert: Annotated[Path, typer.Option("--ca-cert", help="CA certificate path")],
    ca_key: Annotated[Path, typer.Option("--ca-key", help="CA private key path")],
    node_id: Annotated[str, typer.Option("--node-id", help="Node identifier")],
    size: Annotated[
        str, typer.Option("--size", help="Node size (XS S M L XL XXL)")
    ] = "M",
    kv_bind: Annotated[str, typer.Option("--kv-bind")] = "0.0.0.0:7700",
    peer_bind: Annotated[str, typer.Option("--peer-bind")] = "0.0.0.0:7701",
    out: Annotated[Path, typer.Option("--out")] = Path("config.toml"),
    valid_days: Annotated[int, typer.Option("--valid-days")] = 365,
    log_level: Annotated[str, typer.Option("--log-level")] = "INFO",
) -> None:
    """Issue a server certificate and write a fully-populated config.toml."""
    setup_logging(log_level)
    try:
        NodeSize(size)
    except ValueError:
        valid = " ".join(s.value for s in NodeSize)
        logger.error("Invalid node size '%s'; accepted values are: %s.", size, valid)
        raise typer.Exit(1) from None

    cert_b64, key_b64, ca_b64 = _issue_leaf_cert(ca_cert, ca_key, node_id, valid_days)

    config_dict = {
        "schema_version": 1,
        "node": {"id": node_id, "size": size, "data_dir": "./node-data"},
        "tls": {"cert_data": cert_b64, "key_data": key_b64, "ca_data": ca_b64},
        "servers": {
            "kv": {"bind": kv_bind, "advertise": ""},
            "peer": {"bind": peer_bind, "advertise": ""},
        },
        "cluster": {"seeds": [], "rf": 3, "partition_shift": 10},
        "join": {
            "max_retries": -1,
            "attempt_timeout": "10s",
            "deadline": "120s",
            "backoff_base": "2s",
            "backoff_max": "30s",
            "max_concurrent": 4,
        },
        "drain": {
            "max_retries": -1,
            "attempt_timeout": "30s",
            "deadline": "300s",
            "backoff_base": "5s",
            "backoff_max": "60s",
            "max_concurrent": 4,
            "bandwidth_fraction": 1.0,
        },
    }

    try:
        out.write_bytes(tomli_w.dumps(config_dict).encode())
        os.chmod(out, stat.S_IRUSR | stat.S_IWUSR)
    except OSError as exc:
        logger.error("Failed to write config to %s: %s.", out, exc.strerror.lower())
        raise typer.Exit(1) from exc

    logger.info("Server certificate issued for node '%s'.", node_id)
    logger.info("Configuration written to %s (mode 0600).", out)


@config_app.command("generate-context")
def config_generate_context(
    name: Annotated[str, typer.Argument(help="Context name")],
    ca_cert: Annotated[Path, typer.Option("--ca-cert", help="CA certificate path")],
    ca_key: Annotated[Path, typer.Option("--ca-key", help="CA private key path")],
    out: Annotated[
        Path, typer.Option("--out", help="Path to contexts.toml to create or update")
    ],
    kv: Annotated[
        str | None, typer.Option("--kv", help="KV endpoint (host:port)")
    ] = None,
    peer: Annotated[
        str | None, typer.Option("--peer", help="Peer endpoint (host:port)")
    ] = None,
    use: Annotated[bool, typer.Option("--use/--no-use")] = False,
    valid_days: Annotated[int, typer.Option("--valid-days")] = 365,
    log_level: Annotated[str, typer.Option("--log-level")] = "INFO",
) -> None:
    """Issue a client certificate and write a named context to contexts.toml."""
    setup_logging(log_level)

    if not kv and not peer:
        logger.error("At least one of --kv or --peer must be specified.")
        raise typer.Exit(1)

    cert_b64, key_b64, ca_b64 = _issue_leaf_cert(
        ca_cert, ca_key, f"tourctl-{name}", valid_days, san_dns=()
    )

    contexts_file = load_contexts(out)
    logger.debug(
        "Loaded %d existing context(s) from %s.",
        len(contexts_file.contexts),
        out,
    )
    entry = ContextEntry(
        name=name,
        cluster=ClusterRef(name=name, ca_data=ca_b64),
        endpoints=EndpointsConfig(kv=kv, peer=peer),
        credentials=CredentialsConfig(cert_data=cert_b64, key_data=key_b64),
    )
    contexts_file.upsert(entry)

    if use or contexts_file.current_context is None:
        contexts_file.current_context = name
        logger.debug("Active context set to '%s'.", name)

    try:
        save_contexts(out, contexts_file)
    except OSError as exc:
        logger.error(
            "Failed to write contexts file to %s: %s.", out, exc.strerror.lower()
        )
        raise typer.Exit(1) from exc

    logger.info("Client certificate issued for context '%s'.", name)
    logger.info("Context '%s' written to %s.", name, out)


def _issue_leaf_cert(
    ca_cert: Path,
    ca_key: Path,
    common_name: str,
    valid_days: int,
    *,
    san_dns: tuple[str, ...] | None = None,
) -> tuple[str, str, str]:
    """Issue a leaf certificate and return (cert_b64, key_b64, ca_b64).

    When san_dns is None the common_name is used as the single SAN DNS entry.
    Pass an empty tuple to issue a certificate with no DNS SANs (client certs).
    Raise typer.Exit(1) on any PkiError.
    """
    sans: tuple[str, ...] = (common_name,) if san_dns is None else san_dns
    issuer = CryptographyCertIssuerAdapter()
    logger.debug(
        "Issuing leaf certificate for '%s' (valid %d days, SAN DNS: %s).",
        common_name,
        valid_days,
        sans or "none",
    )

    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)
        leaf_cert = td_path / "leaf.crt"
        leaf_key = td_path / "leaf.key"
        req = CertRequest(
            common_name=common_name,
            san_dns=sans,
            san_ip=(),
            valid_days=valid_days,
            ca_cert=ca_cert,
            ca_key=ca_key,
            out_cert=leaf_cert,
            out_key=leaf_key,
        )
        try:
            issuer.issue_cert(req)
        except PkiError as exc:
            logger.error(
                "Failed to issue leaf certificate for '%s': %s.", common_name, exc
            )
            raise typer.Exit(1) from exc

        cert_b64 = base64.b64encode(leaf_cert.read_bytes()).decode()
        key_b64 = base64.b64encode(leaf_key.read_bytes()).decode()
        ca_b64 = base64.b64encode(ca_cert.read_bytes()).decode()

    return cert_b64, key_b64, ca_b64
