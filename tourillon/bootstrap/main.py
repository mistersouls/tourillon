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

from tourillon.bootstrap.usecases import config_app, node_app

app = App(
    name="tourillon",
    help=(
        "Leaderless distributed key-value database for massive scale, high "
        "availability, and multi-tenant namespaces."
    ),
)
app.command(config_app, name="config")
app.command(node_app, name="node")


if __name__ == "__main__":
    app()
