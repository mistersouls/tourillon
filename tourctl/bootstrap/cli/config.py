from pathlib import Path

import typer

from tourctl.bootstrap.deps import get_config_rw
from tourlib.contexts import ContextConfigurer

app = typer.Typer(no_args_is_help=True)


@app.command("use-context")
def use_context(
    name: str,
    contexts_file: Path = typer.Option(Path.home() / ".tourillon" / "contexts.toml"),
) -> None:
    """Set the active context in contexts.toml."""
    if not contexts_file.exists():
        typer.echo(f"✗ Contexts file not found: {contexts_file}", err=True)
        raise typer.Exit(code=1)

    configurer = ContextConfigurer(get_config_rw())
    contexts = configurer.load_contexts(contexts_file)
    if contexts.get(name) is None:
        typer.echo(f'✗ Context "{name}" not found in {contexts_file}', err=True)
        raise typer.Exit(code=1)

    contexts.current_context = name
    configurer.save_contexts(contexts_file, contexts)
    typer.echo(f'✓ Active context set to "{name}".')
