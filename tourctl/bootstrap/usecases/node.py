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

from tourctl.bootstrap.usecases.common import GlobalOptions
from tourctl.core import inspect_node
from tourctl.core.nodejoin import join

node_app = App(name="node", help="Node lifecycle and inspection commands.")


@node_app.command(name="inspect")
async def node_inspect(
    address: str,
    json: bool = False,
    phases: list[str] | None = None,
    probe_statuses: list[str] | None = None,
    node_id_prefix: str | None = None,
    only_anomalies: bool = False,
    limit: int = 500,
    cursor: str | None = None,
    opts: GlobalOptions | None = None,
) -> str:
    """Inspect a node over the peer endpoint and render text or JSON output."""
    opts = opts or GlobalOptions()
    return await inspect_node(
        address=address,
        as_json=json,
        phases=tuple(phases or ()),
        probe_statuses=tuple(probe_statuses or ()),
        node_id_prefix=node_id_prefix,
        only_anomalies=only_anomalies,
        limit=limit,
        cursor=cursor,
        tourconf=opts.tourconf,
        context_name=opts.context,
    )


@node_app.command(name="join")
async def node_join(
    address: str,
    seeds: set[str] | None = None,
    opts: GlobalOptions | None = None,
) -> str:
    """Trigger node to join a cluster with given seeds or configured seeds."""
    opts = opts or GlobalOptions()
    return await join(
        address=address,
        seeds=seeds,
        tourconf=opts.tourconf,
        context_name=opts.context,
    )
