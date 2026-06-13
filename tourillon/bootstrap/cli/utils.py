import typer

from tourillon.core.ports.pki import PkiError


def handle_pki_error(exc: PkiError) -> None:
    message = str(exc)
    code = (
        1
        if "cannot read" in message.lower() or "cannot write" in message.lower()
        else 2
    )
    typer.echo(f"✗ {message}", err=True)
    raise typer.Exit(code=code)
