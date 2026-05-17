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
"""tourctl CLI entry point — config subcommands."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from tourctl.core.commands.config import ContextsError, load_contexts, save_contexts
from tourctl.infra.cli.kv import kv_app
from tourctl.infra.cli.node import node_app
from tourctl.infra.cli.rebalance import rebalance_app

app = typer.Typer(name="tourctl", add_completion=False, no_args_is_help=True)
config_app = typer.Typer(no_args_is_help=True)
app.add_typer(config_app, name="config")
app.add_typer(node_app, name="node")
app.add_typer(rebalance_app, name="rebalance")
app.add_typer(kv_app, name="kv")

_console = Console()
_err_console = Console(stderr=True)

_DEFAULT_CONTEXTS_PATH = Path.home() / ".config" / "tourillon" / "contexts.toml"


@config_app.command("use-context")
def config_use_context(
    name: Annotated[str, typer.Argument(help="Context name to activate")],
    contexts_path: Annotated[Path, typer.Option("--contexts")] = _DEFAULT_CONTEXTS_PATH,
) -> None:
    """Set the active context."""
    try:
        contexts_file = load_contexts(contexts_path)
    except ContextsError as exc:
        _err_console.print(f"✗ {exc}")
        raise typer.Exit(1) from exc

    if not contexts_file.get(name):
        _err_console.print(f'✗ Context "{name}" not found.')
        raise typer.Exit(1)

    contexts_file.current_context = name
    save_contexts(contexts_path, contexts_file)
    _console.print(f'✓ Active context set to "{name}"')


@config_app.command("list")
def config_list(
    contexts_path: Annotated[Path, typer.Option("--contexts")] = _DEFAULT_CONTEXTS_PATH,
) -> None:
    """List all contexts and highlight the active one."""
    try:
        contexts_file = load_contexts(contexts_path)
    except ContextsError as exc:
        _err_console.print(f"✗ {exc}")
        raise typer.Exit(1) from exc

    table = Table(show_header=True)
    table.add_column("CONTEXT")
    table.add_column("KV ENDPOINT")
    table.add_column("PEER ENDPOINT")

    for ctx in contexts_file.contexts:
        marker = "* " if ctx.name == contexts_file.current_context else "  "
        table.add_row(
            f"{marker}{ctx.name}",
            ctx.endpoints.kv or "—",
            ctx.endpoints.peer or "—",
        )
    _console.print(table)
