import asyncio
import logging
from pathlib import Path

import typer

from tourillon.bootstrap.deps import get_core, get_state, get_storage, setup_logging
from tourillon.core.exceptions import (
    BootstrapError,
    ConfigError,
    NodeIdMismatchError,
    StateError,
)
from tourillon.core.helpers.utils import scan
from tourillon.core.ring.hashspace import HashSpace
from tourillon.core.ring.partitioner import Partitioner

app = typer.Typer(no_args_is_help=True)

logger = logging.getLogger(__name__)


@app.command("start")
@scan("tourillon.bootstrap.handlers")
def node_start(
    config: Path = typer.Option(Path("./config.toml")),
    seeds: list[str] | None = typer.Option(None, "--seed"),
    log_level: str = typer.Option("INFO", "--log-level"),
) -> None:
    """Start this node as the first node of a cluster."""
    setup_logging(log_level)
    try:
        core = get_core()
        cfg = core.config.load_config(config)
        state = get_state(cfg)
        space = HashSpace(bits=128)
        partitioner = Partitioner(space, cfg.partition_shift, cfg.segment_shift)
        storage = get_storage(cfg.data_dir, partitioner)
        core.setup(cfg=cfg, state=state, storage=storage, partitioner=partitioner)
        asyncio.run(core.node.start(seeds=seeds))
    except (BootstrapError, ConfigError, NodeIdMismatchError, StateError) as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    except RuntimeError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    except KeyboardInterrupt:
        logger.info("Shutdown requested.")
