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
import os
import tempfile
from pathlib import Path

import typer

from tourillon.bootstrap.cli.utils import handle_pki_error
from tourillon.bootstrap.deps import get_core
from tourillon.core.helpers.utils import b64
from tourillon.core.machinery.config import NodeSize
from tourillon.core.ports.pki import PkiError
from tourillon.core.structure.cert import CertRequest
from tourillon.core.structure.config import ConfigRequest
from tourlib.models import ClusterRef, ContextEntry, CredentialsConfig, EndpointsConfig

app = typer.Typer(no_args_is_help=True)


@app.command("generate")
def generate(
    ca_cert: Path = typer.Option(...),
    ca_key: Path = typer.Option(...),
    node_id: str = typer.Option(...),
    size: NodeSize = typer.Option(NodeSize.M),
    data_dir: Path = typer.Option(...),
    kv_bind: str = typer.Option("127.0.0.1:7000"),
    peer_bind: str = typer.Option("127.0.0.1:7001"),
    peer_advertise: str | None = typer.Option(None),
    seeds: list[str] = typer.Option(None),
    rf: int = typer.Option(3),
    partition_shift: int = typer.Option(17),
    days: int = typer.Option(365),
    out: Path = typer.Option(...),
) -> None:
    """Issue a node cert and write a self-contained config.toml."""
    core = get_core()
    cert_fd, cert_path = tempfile.mkstemp(suffix=".crt")
    key_fd, key_path = tempfile.mkstemp(suffix=".key")
    os.close(cert_fd)
    os.close(key_fd)
    leaf_cert = Path(cert_path)
    leaf_key = Path(key_path)
    try:
        req = ConfigRequest(
            node_id=node_id,
            ca_cert=ca_cert,
            ca_key=ca_key,
            out_cert=leaf_cert,
            out_key=leaf_key,
            size=size,
            data_dir=data_dir,
            kv_bind=kv_bind,
            peer_bind=peer_bind,
            peer_advertise=peer_advertise or peer_bind,
            replication_factor=rf,
            partition_shift=partition_shift,
            seeds=seeds or [],
            cert_valid_days=days,
            out=out,
        )
        core._config.generate_node_config(req)
    except PkiError as exc:
        handle_pki_error(exc)
    finally:
        leaf_cert.unlink(missing_ok=True)
        leaf_key.unlink(missing_ok=True)

    typer.echo(f"✓ Certificate issued for {node_id}")
    typer.echo(f"✓ Config written to {out}  (mode 0600)")


@app.command("generate-context")
def generate_context(
    sub: str,
    name: str = typer.Option(...),
    ca_cert: Path = typer.Option(...),
    ca_key: Path = typer.Option(...),
    kv: str = typer.Option(...),
    peer: str = typer.Option(...),
    out: Path = typer.Option(...),
    days: int = typer.Option(365),
) -> None:
    """Issue a client cert and upsert one context in contexts.toml."""
    core = get_core()
    cert_fd, cert_tmp = tempfile.mkstemp(suffix=".crt")
    key_fd, key_tmp = tempfile.mkstemp(suffix=".key")
    os.close(cert_fd)
    os.close(key_fd)
    cert_path = Path(cert_tmp)
    key_path = Path(key_tmp)
    try:
        core._config.issue_cert(
            CertRequest(
                common_name=sub,
                san_dns=tuple(),
                san_ip=tuple(),
                valid_days=days,
                ca_cert=ca_cert,
                ca_key=ca_key,
                out_cert=cert_path,
                out_key=key_path,
            )
        )
        contexts_file = core._config.load_contexts(out)
        contexts_file.upsert(
            ContextEntry(
                name=name,
                cluster=ClusterRef(name=name, ca_data=b64(ca_cert)),
                endpoints=EndpointsConfig(kv=kv, peer=peer),
                credentials=CredentialsConfig(
                    cert_data=b64(cert_path),
                    key_data=b64(key_path),
                ),
            )
        )
        core._config.save_contexts(out, contexts_file)
    except PkiError as exc:
        handle_pki_error(exc)
    finally:
        cert_path.unlink(missing_ok=True)
        key_path.unlink(missing_ok=True)
    typer.echo("✓ Client certificate issued")
    typer.echo(f'✓ Context "{name}" written to {out}')
