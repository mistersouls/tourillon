import asyncio
from pathlib import Path

import typer

from tourctl.bootstrap.deps import get_node_client

app = typer.Typer(no_args_is_help=True)


@app.command("join")
def node_join(
    address: str = typer.Argument(..., metavar="ADDRESS"),
    seeds: str | None = typer.Option(None, "--seeds"),
    context: str | None = typer.Option(None, "--context"),
    contexts_file: Path = typer.Option(Path.home() / ".tourillon" / "contexts.toml"),
    timeout: float = typer.Option(30.0, "--timeout"),
) -> None:
    node = get_node_client()

    async def _run() -> int:
        code_, message, is_error = await node.join(
            address=address,
            seeds=seeds,
            context=context,
            contexts_file=contexts_file,
            timeout=timeout,
        )
        typer.echo(message, err=is_error)
        return code_

    code = asyncio.run(_run())
    if code != 0:
        raise typer.Exit(code=code)
