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
from tourctl.core import node_status
from tourctl.core.nodedrain import drain
from tourctl.core.nodehealth import node_health
from tourctl.core.nodejoin import join
from tourctl.core.nodepause import pause
from tourctl.core.noderesume import resume

node_app = App(name="node", help="Node lifecycle and inspection commands.")


@node_app.command(name="status")
async def node_status_cmd(
    address: str,
    vnodes: bool = False,
    json: bool = False,
    opts: GlobalOptions | None = None,
) -> None:
    """Summarise one node: phase, ring, anti-entropy, GC, storage and TLS.

    :param address: Direct peer address of the node, never a load balancer.
    :param vnodes: Add the table of vnodes and the partitions they own.
    :param json: Print the full status as JSON.
    :param opts: Client configuration and context options.
    """
    opts = opts or GlobalOptions()
    output = await node_status(
        address=address,
        vnodes=vnodes,
        as_json=json,
        tourconf=opts.tourconf,
        context_name=opts.context,
    )
    print(output)


@node_app.command(name="health")
async def node_health_cmd(
    address: str,
    json: bool = False,
    opts: GlobalOptions | None = None,
) -> None:
    """Local verdict for supervision: exit 0 OK, 1 WARNING, 2 CRITICAL, 3 UNKNOWN.

    :param address: Direct peer address of the node, never a load balancer.
    :param json: Print the verdict and its reasons as JSON.
    :param opts: Client configuration and context options.
    """
    opts = opts or GlobalOptions()
    output = await node_health(
        address=address,
        as_json=json,
        tourconf=opts.tourconf,
        context_name=opts.context,
    )
    print(output.text)
    if output.exit_code:
        raise SystemExit(output.exit_code)


@node_app.command(name="join")
async def node_join(
    address: str,
    seeds: set[str] | None = None,
    wait: bool = False,
    timeout: float = 600.0,
    cancel: bool = False,
    json: bool = False,
    opts: GlobalOptions | None = None,
) -> str:
    """Join a cluster, cancel joining, or wait for the requested transition.

    :param address: Direct peer address of the node to join.
    :param seeds: Optional comma-separated seed addresses.
    :param wait: Wait until READY or IDLE after cancellation.
    :param timeout: Maximum wait duration in seconds.
    :param cancel: Cancel an in-progress join and return the node to IDLE.
    :param json: Print structured output.
    :param opts: Client configuration and context options.
    """
    opts = opts or GlobalOptions()
    return await join(
        address=address,
        seeds=seeds,
        wait=wait,
        timeout=timeout,
        cancel=cancel,
        as_json=json,
        tourconf=opts.tourconf,
        context_name=opts.context,
    )


@node_app.command(name="drain")
async def node_drain(
    address: str,
    wait: bool = False,
    timeout: float = 600.0,
    cancel: bool = False,
    json: bool = False,
    opts: GlobalOptions | None = None,
) -> str:
    """Drain a node, cancel draining, or wait for the requested transition.

    :param address: Direct peer address of the node to drain.
    :param wait: Wait until IDLE or READY after cancellation.
    :param timeout: Maximum wait duration in seconds.
    :param cancel: Cancel an in-progress drain and return the node to READY.
    :param json: Print structured output.
    :param opts: Client configuration and context options.
    """
    opts = opts or GlobalOptions()
    return await drain(
        address=address,
        wait=wait,
        timeout=timeout,
        cancel=cancel,
        as_json=json,
        tourconf=opts.tourconf,
        context_name=opts.context,
    )


@node_app.command(name="pause")
async def node_pause(
    address: str,
    json: bool = False,
    opts: GlobalOptions | None = None,
) -> str:
    """Pause user operations while keeping the node on the ring.

    :param address: Direct peer address of the node to pause.
    :param json: Print the acknowledgement as JSON.
    :param opts: Client configuration and context options.
    """
    opts = opts or GlobalOptions()
    return await pause(
        address=address,
        as_json=json,
        tourconf=opts.tourconf,
        context_name=opts.context,
    )


@node_app.command(name="resume")
async def node_resume(
    address: str,
    wait: bool = False,
    timeout: float = 300.0,
    json: bool = False,
    opts: GlobalOptions | None = None,
) -> str:
    """Resume a paused node; optionally wait until anti-entropy reaches READY.

    :param address: Direct peer address of the node to resume.
    :param wait: Wait for synchronization and the READY phase.
    :param timeout: Maximum wait duration in seconds.
    :param json: Print the result as JSON.
    :param opts: Client configuration and context options.
    """
    opts = opts or GlobalOptions()
    return await resume(
        address=address,
        wait=wait,
        timeout=timeout,
        as_json=json,
        tourconf=opts.tourconf,
        context_name=opts.context,
    )
