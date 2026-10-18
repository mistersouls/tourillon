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

from cyclopts import App
from tourctl.bootstrap.usecases.kv import delete, get, put, repair
from tourctl.bootstrap.usecases.node import node_app
from tourctl.bootstrap.usecases.probe import probe_app


app = App(
    name="tourctl",
    help="Tourillon operator and client commands.",
)
app.command(probe_app, name="probe")
app.command(node_app, name="node")
app.command(put, name="put")
app.command(get, name="get")
app.command(delete, name="delete")
app.command(repair, name="repair")


if __name__ == "__main__":
    app()
