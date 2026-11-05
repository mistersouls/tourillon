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

from tourctl.core.config import config_use_context, config_view

config_app = App(name="config", help="View and switch tourctl client contexts.")


@config_app.command(name="view")
async def view(tourconf: str | None = None, json: bool = False) -> None:
    """Show the contexts of the client config. Certificates are never shown."""
    print(await config_view(tourconf=tourconf, as_json=json))


@config_app.command(name="use-context")
async def use_context(name: str, tourconf: str | None = None) -> None:
    """Make NAME the current context."""
    print(await config_use_context(name, tourconf=tourconf))
