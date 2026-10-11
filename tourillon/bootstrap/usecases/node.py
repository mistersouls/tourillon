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

from pathlib import Path

from cyclopts import App

from tourillon.bootstrap.deps import get_app
from tourillon.core.log import init_loggers, logger
from tourillon.core.manager import Manager
from tourlib.helpers import scan

node_app = App(
    name="node",
    help="Node lifecycle and management commands."
)


@node_app.command(name="start")
@scan("tourillon.bootstrap.handlers")
async def node_start(
    config: Path,
    seeds: list[str] | None = None,
) -> None:
    """Start a Tourillon node.

    :param config: Path to the node configuration TOML file.
    :param seeds: Optional list of seed peer addresses for joining an existing cluster.
    """
    # Build app and load config through centralized adapters.
    app = get_app()
    tourillon_config = await app.load_config(config)
    init_loggers(tourillon_config.logging)
    logger.info("Node start requested from config '%s'", config)
    manager = await app.create_manager(tourillon_config)
    app.ctx.set(Manager, manager)
    await manager.serve_forever(seeds=seeds)
