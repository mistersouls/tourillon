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
from tourctl.core.nodedrain import drain
from tourctl.core.nodegc import collect
from tourctl.core.nodejoin import join

node_app = App(name="node", help="Node lifecycle and inspection commands.")


@node_app.command(name="gc")
async def node_gc(
    address: str,
    *,
    dry_run: bool = False,
    partition: int | None = None,
    wait: bool = False,
    timeout: float = 60.0,
    json: bool = False,
    opts: GlobalOptions | None = None,
) -> str:
    """Submit local historical GC; never purge current state or compact files.

    :param address: Target node's peer endpoint.
    :param dry_run: Estimate without modifying storage.
    :param partition: Restrict the pass to this partition; default is all local partitions.
    :param wait: Wait for this job's completion, without cancelling on timeout.
    :param timeout: Maximum wait in seconds when --wait is used.
    :param json: Return exact structured counts instead of human-readable units.
    :param opts: Client configuration and context options.
    """
    opts = opts or GlobalOptions()
    return await collect(
        address,
        dry_run=dry_run,
        partition=partition,
        wait=wait,
        timeout=timeout,
        as_json=json,
        tourconf=opts.tourconf,
        context_name=opts.context,
    )


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


@node_app.command(name="drain")
async def node_drain(
    address: str,
    opts: GlobalOptions | None = None,
) -> str:
    """Initiate asynchronous draining; acknowledgement does not mean completion.

    :param address: The peer endpoint of the node to drain.
    :param opts: Client configuration and context options.
    """
    opts = opts or GlobalOptions()
    return await drain(
        address=address,
        tourconf=opts.tourconf,
        context_name=opts.context,
    )
