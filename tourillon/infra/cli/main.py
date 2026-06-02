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
"""tourillon CLI entry point for proposal 001 bootstrap commands."""

from __future__ import annotations

import base64
import logging
import os
import stat
import tempfile
import uuid
from asyncio import run as asyncio_run
from pathlib import Path

import tomli_w
import typer

from tourillon.bootstrap.log import setup_logging
from tourillon.bootstrap.node_start import NodeStartError, run_node_start
from tourillon.core.ports.pki import CaRequest, CertRequest, PkiError
from tourillon.core.structure.config import NodeSize
from tourillon.core.structure.contexts import (
    ClusterRef,
    ContextEntry,
    CredentialsConfig,
    EndpointsConfig,
)
from tourillon.infra.contexts import load_contexts, save_contexts
from tourillon.infra.pki.x509 import (
    CryptographyCaAdapter,
    CryptographyCertIssuerAdapter,
)

app = typer.Typer(no_args_is_help=True)
pki_app = typer.Typer(no_args_is_help=True)
config_app = typer.Typer(no_args_is_help=True)
node_app = typer.Typer(no_args_is_help=True)
app.add_typer(pki_app, name="pki")
app.add_typer(config_app, name="config")
app.add_typer(node_app, name="node")

logger = logging.getLogger("tourillon.bootstrap.node")


class _CliServices:
    """Infrastructure service instances used by CLI commands."""

    ca = CryptographyCaAdapter()
    issuer = CryptographyCertIssuerAdapter()


def _b64(path: Path) -> str:
    return base64.b64encode(path.read_bytes()).decode("ascii")


def _write_config(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(tomli_w.dumps(payload), encoding="utf-8")
    os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)


def _handle_pki_error(exc: PkiError) -> None:
    message = str(exc)
    code = (
        1
        if "cannot read" in message.lower() or "cannot write" in message.lower()
        else 2
    )
    typer.echo(f"✗ {message}", err=True)
    raise typer.Exit(code=code)


@pki_app.command("ca")
def pki_ca(
    out_cert: Path = typer.Option(Path("./ca.crt")),
    out_key: Path = typer.Option(Path("./ca.key")),
    name: str = typer.Option("tourillon-ca"),
    days: int = typer.Option(3650),
    key_size: int = typer.Option(2048),
) -> None:
    """Generate a self-signed CA certificate and private key."""
    try:
        _CliServices.ca.generate_ca(
            CaRequest(
                common_name=name,
                valid_days=days,
                key_size=key_size,
                out_cert=out_cert,
                out_key=out_key,
            )
        )
    except PkiError as exc:
        _handle_pki_error(exc)
    typer.echo(f"✓ CA certificate written to {out_cert}")
    typer.echo(f"✓ CA private key  written to {out_key}  (mode 0600)")


@pki_app.command("issue")
def pki_issue(
    ca_cert: Path = typer.Option(...),
    ca_key: Path = typer.Option(...),
    name: str = typer.Option(...),
    san_dns: list[str] = typer.Option(None),
    san_ip: list[str] = typer.Option(None),
    out_cert: Path | None = typer.Option(None),
    out_key: Path | None = typer.Option(None),
    days: int = typer.Option(365),
    key_size: int = typer.Option(2048),
) -> None:
    """Issue a leaf certificate signed by the supplied CA."""
    leaf_cert = out_cert or Path(f"./{name}.pem")
    leaf_key = out_key or Path(f"./{name}-key.pem")
    try:
        _CliServices.issuer.issue_cert(
            CertRequest(
                common_name=name,
                san_dns=tuple(san_dns or []),
                san_ip=tuple(san_ip or []),
                valid_days=days,
                ca_cert=ca_cert,
                ca_key=ca_key,
                out_cert=leaf_cert,
                out_key=leaf_key,
                key_size=key_size,
            )
        )
    except PkiError as exc:
        _handle_pki_error(exc)
    typer.echo(f"✓ Certificate issued for {name}")
    typer.echo(f"✓ Certificate written to {leaf_cert}")
    typer.echo(f"✓ Private key  written to {leaf_key}  (mode 0600)")


@config_app.command("generate")
def config_generate(
    ca_cert: Path = typer.Option(...),
    ca_key: Path = typer.Option(...),
    node_id: str | None = typer.Option(None),
    size: NodeSize = typer.Option(NodeSize.M),
    data_dir: Path = typer.Option(Path("./data")),
    kv_bind: str = typer.Option("0.0.0.0:7000"),
    peer_bind: str = typer.Option("0.0.0.0:7001"),
    peer_advertise: str | None = typer.Option(None),
    seed: list[str] = typer.Option(None),
    rf: int = typer.Option(3),
    partition_shift: int = typer.Option(10),
    days: int = typer.Option(365),
    out: Path = typer.Option(Path("./config.toml")),
) -> None:
    """Issue a node cert and write a self-contained config.toml."""
    resolved_node_id = node_id or f"node-{uuid.uuid4().hex[:8]}"
    cert_fd, cert_path = tempfile.mkstemp(suffix=".crt")
    key_fd, key_path = tempfile.mkstemp(suffix=".key")
    os.close(cert_fd)
    os.close(key_fd)
    leaf_cert = Path(cert_path)
    leaf_key = Path(key_path)
    try:
        _CliServices.issuer.issue_cert(
            CertRequest(
                common_name=resolved_node_id,
                san_dns=tuple(),
                san_ip=tuple(),
                valid_days=days,
                ca_cert=ca_cert,
                ca_key=ca_key,
                out_cert=leaf_cert,
                out_key=leaf_key,
            )
        )
        payload: dict[str, object] = {
            "schema_version": 1,
            "node": {
                "id": resolved_node_id,
                "size": size.value,
                "data_dir": str(data_dir),
                "rf": rf,
                "partition_shift": partition_shift,
                "seeds": seed or [],
            },
            "servers": {
                "kv": {"bind": kv_bind},
                "peer": {
                    "bind": peer_bind,
                    "advertise": peer_advertise or "127.0.0.1:7001",
                },
            },
            "tls": {
                "cert_data": _b64(leaf_cert),
                "key_data": _b64(leaf_key),
                "ca_data": _b64(ca_cert),
            },
            "join": {
                "max_retries": -1,
                "attempt_timeout": "10s",
                "deadline": "2m",
                "backoff_base": "2s",
                "backoff_max": "30s",
                "max_concurrent": 4,
            },
            "drain": {
                "max_retries": -1,
                "attempt_timeout": "30s",
                "deadline": "5m",
                "backoff_base": "5s",
                "backoff_max": "60s",
                "max_concurrent": 4,
                "bandwidth_fraction": 1.0,
            },
            "rebalance": {"max_concurrent_transfers": 4, "max_chunk_bytes": "1Mi"},
        }
        _write_config(out, payload)
    except PkiError as exc:
        _handle_pki_error(exc)
    finally:
        leaf_cert.unlink(missing_ok=True)
        leaf_key.unlink(missing_ok=True)
    typer.echo(f"✓ Certificate issued for {resolved_node_id}")
    typer.echo(f"✓ Config written to {out}  (mode 0600)")


@config_app.command("generate-context")
def generate_context(
    name: str,
    ca_cert: Path = typer.Option(...),
    ca_key: Path = typer.Option(...),
    kv: str | None = typer.Option(None),
    peer: str | None = typer.Option(None),
    out: Path = typer.Option(Path.home() / ".tourillon" / "contexts.toml"),
    days: int = typer.Option(365),
    set_current: bool = typer.Option(False),
) -> None:
    """Issue a client cert and upsert one context in contexts.toml."""
    if kv is None and peer is None:
        typer.echo("✗ --kv or --peer (or both) must be supplied.", err=True)
        raise typer.Exit(code=1)

    cert_fd, cert_tmp = tempfile.mkstemp(suffix=".crt")
    key_fd, key_tmp = tempfile.mkstemp(suffix=".key")
    os.close(cert_fd)
    os.close(key_fd)
    cert_path = Path(cert_tmp)
    key_path = Path(key_tmp)
    try:
        _CliServices.issuer.issue_cert(
            CertRequest(
                common_name=f"tourctl-{name}",
                san_dns=tuple(),
                san_ip=tuple(),
                valid_days=days,
                ca_cert=ca_cert,
                ca_key=ca_key,
                out_cert=cert_path,
                out_key=key_path,
            )
        )
        contexts_file = load_contexts(out)
        contexts_file.upsert(
            ContextEntry(
                name=name,
                cluster=ClusterRef(name=name, ca_data=_b64(ca_cert)),
                endpoints=EndpointsConfig(kv=kv, peer=peer),
                credentials=CredentialsConfig(
                    cert_data=_b64(cert_path),
                    key_data=_b64(key_path),
                ),
            )
        )
        if set_current:
            contexts_file.current_context = name
        save_contexts(out, contexts_file)
    except PkiError as exc:
        _handle_pki_error(exc)
    finally:
        cert_path.unlink(missing_ok=True)
        key_path.unlink(missing_ok=True)
    typer.echo("✓ Client certificate issued")
    typer.echo(f'✓ Context "{name}" written to {out}')


@node_app.command("start")
def node_start(
    config: Path = typer.Option(Path("./config.toml")),
    log_level: str = typer.Option("INFO", "--log-level"),
) -> None:
    """Start this node as the first node of a cluster."""
    setup_logging(log_level)
    try:
        asyncio_run(run_node_start(config))
    except NodeStartError as exc:
        raise typer.Exit(code=exc.exit_code) from exc
    except KeyboardInterrupt:
        logger.info("Shutdown requested.")
