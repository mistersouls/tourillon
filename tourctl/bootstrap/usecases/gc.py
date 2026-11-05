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
from tourctl.core.gc import gc_status, run_gc

gc_app = App(name="gc", help="Local historical garbage collection of one node.")


@gc_app.command(name="run")
async def gc_run_cmd(
    address: str,
    *,
    dry_run: bool = False,
    partition: int | None = None,
    wait: bool = False,
    timeout: float = 60.0,
    json: bool = False,
    opts: GlobalOptions | None = None,
) -> None:
    """Submit one GC pass; it never purges current values or shrinks files.

    :param address: Direct peer address of the node, never a load balancer.
    :param dry_run: Estimate what a pass would collect, without writing.
    :param partition: Restrict the pass to this partition; default is all local partitions.
    :param wait: Wait for the pass to finish; a timeout never cancels it.
    :param timeout: Maximum wait in seconds when --wait is used.
    :param json: Print the pass as JSON with exact counts.
    :param opts: Client configuration and context options.
    """
    opts = opts or GlobalOptions()
    output = await run_gc(
        address,
        dry_run=dry_run,
        partition=partition,
        wait=wait,
        timeout=timeout,
        as_json=json,
        tourconf=opts.tourconf,
        context_name=opts.context,
    )
    print(output)


@gc_app.command(name="status")
async def gc_status_cmd(
    address: str,
    *,
    job: str | None = None,
    all: bool = False,
    json: bool = False,
    opts: GlobalOptions | None = None,
) -> None:
    """Show the GC worker and its recent passes, or one pass in detail.

    :param address: Direct peer address of the node, never a load balancer.
    :param job: Show one pass, named by a unique prefix of its identifier.
    :param all: List every retained pass instead of the 5 most recent.
    :param json: Print the worker and passes as JSON.
    :param opts: Client configuration and context options.
    """
    opts = opts or GlobalOptions()
    output = await gc_status(
        address,
        job=job,
        show_all=all,
        as_json=json,
        tourconf=opts.tourconf,
        context_name=opts.context,
    )
    print(output)
