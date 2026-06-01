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
"""tourctl CLI root application."""

from __future__ import annotations

from pathlib import Path

import typer

from tourillon.infra.contexts import load_contexts, save_contexts

app = typer.Typer(no_args_is_help=True)
config_app = typer.Typer(no_args_is_help=True)
app.add_typer(config_app, name="config")


@config_app.command("use-context")
def use_context(
    name: str,
    contexts_file: Path = typer.Option(Path.home() / ".tourillon" / "contexts.toml"),
) -> None:
    """Set the active context in contexts.toml."""
    if not contexts_file.exists():
        typer.echo(f"✗ Contexts file not found: {contexts_file}", err=True)
        raise typer.Exit(code=1)

    contexts = load_contexts(contexts_file)
    if contexts.get(name) is None:
        typer.echo(f'✗ Context "{name}" not found in {contexts_file}', err=True)
        raise typer.Exit(code=1)

    contexts.current_context = name
    save_contexts(contexts_file, contexts)
    typer.echo(f'✓ Active context set to "{name}".')
