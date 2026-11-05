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
from typing import Annotated

from cyclopts import App, Parameter

from tourctl.bootstrap.usecases.common import GlobalOptions
from tourctl.core.cluster import (
    Target,
    build_request,
    cluster_member,
    cluster_status,
    watch_cluster_status,
)
from tourlib.models.inspect import DEFAULT_LIMIT

cluster_app = App(name="cluster", help="Cluster-wide views, from one node's gossip.")


@cluster_app.command(name="member")
async def member(
    node_id: str,
    address: Annotated[
        str | None,
        Parameter(
            name=("--address", "-a"),
            help="Node to ask. Defaults to the peer endpoint of the context.",
        ),
    ] = None,
    json: bool = False,
    opts: GlobalOptions | None = None,
) -> str:
    """Show one member's placement, reachability, debt and stand-ins.

    :param node_id: Member id to inspect.
    :param address: Node to query, or the peer endpoint from the context.
    :param json: Emit the report as JSON.
    :param opts: Shared Tourillon client options.
    """
    opts = opts or GlobalOptions()
    target = Target(address=address, tourconf=opts.tourconf, context_name=opts.context)
    return await cluster_member(node_id, target, as_json=json)


@cluster_app.command(name="status")
async def status(
    address: Annotated[
        str | None,
        Parameter(
            name=("--address", "-a"),
            help="Node to ask. Defaults to the peer endpoint of the context.",
        ),
    ] = None,
    all_members: Annotated[
        bool, Parameter(name="--all", help="List every member, not only anomalies.")
    ] = False,
    phase: Annotated[
        list[str] | None,
        Parameter(name="--phase", help="Only members in this phase. Repeatable."),
    ] = None,
    status: Annotated[
        list[str] | None,
        Parameter(
            name="--status",
            help="Only members with this status (ALIVE, SUSPECT, DEAD). Repeatable.",
        ),
    ] = None,
    prefix: Annotated[
        str | None, Parameter(help="Only node ids starting with this prefix.")
    ] = None,
    limit: Annotated[int, Parameter(help="Members per page, 1 to 1000.")] = (
        DEFAULT_LIMIT
    ),
    cursor: Annotated[
        str | None, Parameter(help="Page to show, as printed by the previous one.")
    ] = None,
    watch: Annotated[
        bool, Parameter(help="Refresh the summary and the first page.")
    ] = False,
    interval: Annotated[
        float, Parameter(help="Seconds between refreshes, at least 1.")
    ] = 2.0,
    json: bool = False,
    opts: GlobalOptions | None = None,
) -> None:
    """Show the members of the cluster as one node sees them.

    By default only anomalies are listed: members that are not both READY
    and ALIVE. The summary always counts every member.
    """
    opts = opts or GlobalOptions()
    request = build_request(
        list_all=all_members,
        phases=phase or (),
        statuses=status or (),
        prefix=prefix,
        limit=limit,
        cursor=cursor,
    )
    target = Target(address=address, tourconf=opts.tourconf, context_name=opts.context)
    if watch:
        await watch_cluster_status(request, target, interval=interval, as_json=json)
        return
    print(await cluster_status(request, target, as_json=json))
