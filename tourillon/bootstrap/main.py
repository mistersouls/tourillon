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
import typer

from tourillon.bootstrap.cli.config import app as config
from tourillon.bootstrap.cli.node import app as node
from tourillon.bootstrap.cli.pki import app as pki


def main() -> None:
    app = typer.Typer(no_args_is_help=True)
    app.add_typer(config, name="config")
    app.add_typer(pki, name="pki")
    app.add_typer(node, name="node")
    app()


if __name__ == "__main__":
    main()
