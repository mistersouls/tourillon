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

import sys
from collections.abc import Sequence

from cyclopts import App, CycloptsError

from tourctl.bootstrap.usecases.ae import ae_app
from tourctl.bootstrap.usecases.cluster import cluster_app
from tourctl.bootstrap.usecases.config import config_app
from tourctl.bootstrap.usecases.gc import gc_app
from tourctl.bootstrap.usecases.kv import delete, get, put, repair
from tourctl.bootstrap.usecases.node import node_app
from tourctl.bootstrap.usecases.probe import probe_app
from tourctl.core.error import EXIT_INVALID, CliError

app = App(
    name="tourctl",
    help="Tourillon operator and client commands.",
)
app.command(probe_app, name="probe")
app.command(node_app, name="node")
app.command(cluster_app, name="cluster")
app.command(gc_app, name="gc")
app.command(ae_app, name="ae")
app.command(config_app, name="config")
app.command(put, name="put")
app.command(get, name="get")
app.command(delete, name="delete")
app.command(repair, name="repair")


def main(tokens: Sequence[str] | None = None) -> None:
    """Run tourctl; failures go to stderr with the exit codes of the spec."""
    argv = list(sys.argv[1:] if tokens is None else tokens)
    try:
        app(argv, exit_on_error=False)
    except CliError as exc:
        rendered = exc.render_json() if "--json" in argv else exc.render_text()
        print(rendered, file=sys.stderr)
        sys.exit(exc.code)
    except CycloptsError:
        sys.exit(EXIT_INVALID)


if __name__ == "__main__":
    main()
