import typer

from tourctl.bootstrap.cli.config import app as config
from tourctl.bootstrap.cli.node import app as node


def main() -> None:
    app = typer.Typer(no_args_is_help=True)
    app.add_typer(config, name="config")
    app.add_typer(node, name="node")
    app()


if __name__ == "__main__":
    main()
