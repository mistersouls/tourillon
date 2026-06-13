from pathlib import Path

import typer

from tourillon.bootstrap.cli.utils import handle_pki_error
from tourillon.bootstrap.deps import get_core
from tourillon.core.ports.pki import PkiError
from tourillon.core.structure.cert import CaRequest, CertRequest

app = typer.Typer(no_args_is_help=True)


@app.command("ca")
def ca(
    out_cert: Path = typer.Option(...),
    out_key: Path = typer.Option(...),
    name: str = typer.Option("tourillon-ca"),
    days: int = typer.Option(3650),
    key_size: int = typer.Option(2048),
) -> None:
    """Generate a self-signed CA certificate and private key."""
    core = get_core()

    try:
        core._config.generate_ca(
            CaRequest(
                common_name=name,
                valid_days=days,
                key_size=key_size,
                out_cert=out_cert,
                out_key=out_key,
            )
        )
    except PkiError as exc:
        handle_pki_error(exc)
    typer.echo(f"✓ CA certificate written to {out_cert}")
    typer.echo(f"✓ CA private key  written to {out_key}  (mode 0600)")


@app.command("issue")
def issue(
    ca_cert: Path = typer.Option(...),
    ca_key: Path = typer.Option(...),
    name: str = typer.Option(...),
    san_dns: list[str] = typer.Option(None),
    san_ip: list[str] = typer.Option(None),
    out_cert: Path = typer.Option(...),
    out_key: Path = typer.Option(...),
    days: int = typer.Option(365),
    key_size: int = typer.Option(2048),
) -> None:
    """Issue a leaf certificate signed by the supplied CA."""
    core = get_core()
    leaf_cert = out_cert or Path(f"./{name}.pem")
    leaf_key = out_key or Path(f"./{name}-key.pem")
    try:
        core._config.issue_cert(
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
        handle_pki_error(exc)
    typer.echo(f"✓ Certificate issued for {name}")
    typer.echo(f"✓ Certificate written to {leaf_cert}")
    typer.echo(f"✓ Private key  written to {leaf_key}  (mode 0600)")
