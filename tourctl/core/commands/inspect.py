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
"""InspectCommand — sends node.inspect or node.inspect.peer_view and renders output."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from tourctl.core.ports.console import ConsolePort
from tourillon.core.ports.transport import RESPONSE_TIMEOUT, ResponseTimeoutError
from tourillon.core.structure.envelope import Envelope
from tourillon.core.transport.client import TcpClient

if TYPE_CHECKING:

    from tourillon.core.ports.serializer import SerializerPort

PARTITION_DISPLAY_THRESHOLD: int = 64


class InspectCommand:
    """Sends node.inspect (or node.inspect.peer_view) and renders the result.

    Connects to the peer endpoint in the active context, sends the request,
    awaits NodeInspectResponse (or NodePeerViewResponse), and renders the
    structured output. The standard format for every vnode partition range
    line is:

        token 0x<8 hex digits>…  →  pids  <start>–<end>  (<count> partitions)

    This format is used unconditionally whenever partition ranges are rendered,
    whether from a self-inspect, a forwarded inspect, or --partitions. When
    json_output=True the raw response is serialised to JSON and written to
    stdout (token_hex in the JSON carries the full untruncated hex string);
    otherwise Rich is used for human-readable output. CLI rendering collapses
    partition_ranges to a hint line when len(partition_ranges) >
    PARTITION_DISPLAY_THRESHOLD unless show_all_partitions=True. A truncation
    warning is printed when members_truncated or probe_states_truncated is True.
    """

    def __init__(
        self,
        client: TcpClient,
        serializer: SerializerPort,
        console: ConsolePort,
        err_console: ConsolePort,
        contact_node_id: str = "",
    ) -> None:
        self._client = client
        self._serializer = serializer
        self._console = console
        self._err_console = err_console
        self._contact_node_id = contact_node_id

    async def run(
        self,
        target_node_id: str,
        *,
        peer_view: bool = False,
        show_all_partitions: bool = False,
        json_output: bool = False,
        timeout: float = RESPONSE_TIMEOUT,
    ) -> int:
        """Execute the inspect request and render/print results.

        Return 0 on success, 1 on any error. Errors are always written to
        stderr regardless of json_output.
        """
        if peer_view:
            return await self._run_peer_view(target_node_id, json_output, timeout)
        return await self._run_inspect(
            target_node_id, show_all_partitions, json_output, timeout
        )

    async def _run_inspect(
        self,
        target_node_id: str,
        show_all_partitions: bool,
        json_output: bool,
        timeout: float,
    ) -> int:
        """Send node.inspect and render NodeInspectResponse."""
        req_payload = self._serializer.encode({"target_node_id": target_node_id})
        req = Envelope.create(
            req_payload,
            kind="node.inspect",
            schema_id=self._serializer.schema_id,
        )

        try:
            resp = await self._client.request(req, timeout=timeout)
        except ResponseTimeoutError:
            self._err_console.print(f"✗ Inspect timed out after {timeout:.0f}s.")
            return 1

        if resp.kind.startswith("error."):
            return self._handle_error(resp.kind, target_node_id)

        try:
            data = self._serializer.decode(resp.payload)
        except Exception:
            self._err_console.print("✗ Failed to decode response payload.")
            return 1

        if json_output:
            self._console.print(json.dumps(data, indent=2))
            return 0

        return self._render_inspect(data, target_node_id, show_all_partitions)

    async def _run_peer_view(
        self,
        target_node_id: str,
        json_output: bool,
        timeout: float,
    ) -> int:
        """Send node.inspect.peer_view and render NodePeerViewResponse."""
        req_payload = self._serializer.encode({"target_node_id": target_node_id})
        req = Envelope.create(
            req_payload,
            kind="node.inspect.peer_view",
            schema_id=self._serializer.schema_id,
        )

        try:
            resp = await self._client.request(req, timeout=timeout)
        except ResponseTimeoutError:
            self._err_console.print(f"✗ Inspect timed out after {timeout:.0f}s.")
            return 1

        if resp.kind.startswith("error."):
            return self._handle_peer_view_error(resp.kind, target_node_id)

        try:
            data = self._serializer.decode(resp.payload)
        except Exception:
            self._err_console.print("✗ Failed to decode response payload.")
            return 1

        if json_output:
            self._console.print(json.dumps(data, indent=2))
            return 0

        return self._render_peer_view(data, target_node_id)

    def _handle_error(self, error_kind: str, target_node_id: str) -> int:
        """Print the appropriate error and return exit code 1."""
        contact = self._contact_node_id or "contact"
        if error_kind == "error.node_not_found":
            self._err_console.print(
                f"✗ {contact} has no routing entry for {target_node_id}."
            )
        elif error_kind == "error.node_unreachable":
            self._err_console.print(
                f"✗ {target_node_id} is unreachable. "
                f"Try --peer-view to query {contact}'s gossip record."
            )
        else:
            self._err_console.print(f"✗ Received error response: {error_kind}")
        return 1

    def _handle_peer_view_error(self, error_kind: str, target_node_id: str) -> int:
        """Print the appropriate peer-view error and return exit code 1."""
        contact = self._contact_node_id or "contact"
        if error_kind == "error.node_not_found":
            self._err_console.print(
                f"✗ {contact} has no gossip record for {target_node_id}."
            )
        else:
            self._err_console.print(f"✗ Received error response: {error_kind}")
        return 1

    def _render_inspect(
        self,
        data: dict[str, Any],
        target_node_id: str,
        show_all_partitions: bool,
    ) -> int:
        """Render NodeInspectResponse as Rich-formatted output."""
        forwarded_by = data.get("forwarded_by")
        via = f" (via {forwarded_by})" if forwarded_by else ""
        self._console.print(f"Inspecting {target_node_id}{via} ...\n")

        phase = data.get("phase", "").upper()
        size = data.get("size", "")
        token_count = len(data.get("tokens", []))
        self._console.print("  [bold]Identity[/bold]")
        self._console.print(f"    Node ID:      {data.get('node_id', '')}")
        self._console.print(f"    Phase:        {phase}")
        self._console.print(f"    Size:         {size}  ({token_count} vnodes)")
        self._console.print(f"    Generation:   {data.get('generation', 0)}")
        self._console.print(f"    Seq:          {data.get('seq', 0)}")
        self._console.print(f"    Epoch:        {data.get('epoch', 0)}\n")

        self._console.print("  [bold]Addresses[/bold]")
        self._console.print(f"    Peer:   {data.get('peer_address', '')}")
        self._console.print(f"    KV:     {data.get('kv_address', '')}\n")

        self._render_ring_section(data, show_all_partitions)
        self._render_members_section(data, target_node_id)
        self._render_probe_section(data, target_node_id)

        if data.get("members_truncated"):
            shown = len(data.get("members", []))
            total = data.get("members_total", shown)
            self._console.print(
                f"\n  [yellow]⚠ Membership list truncated: "
                f"showing {shown} of {total} members.[/yellow]"
            )
        return 0

    def _render_ring_section(
        self,
        data: dict[str, Any],
        show_all_partitions: bool,
    ) -> None:
        """Render the Ring position section."""
        ps = data.get("partition_shift", 0)
        total = data.get("total_partitions", 0)
        owned = data.get("owned_partitions", 0)
        pct = (owned / total * 100) if total else 0.0
        ranges: list[dict[str, Any]] = data.get("partition_ranges", [])
        n_ranges = len(ranges)

        self._console.print(
            f"  [bold]Ring position[/bold]  "
            f"(partition_shift={ps}, {total:,} total partitions)"
        )
        self._console.print(f"    Owned partitions:  {owned}  ({pct:.1f} % of total)")

        if n_ranges > PARTITION_DISPLAY_THRESHOLD and not show_all_partitions:
            self._console.print(
                f"    Vnode ranges:      {n_ranges}  "
                f"(use --partitions to list all ranges)"
            )
        else:
            self._console.print(f"    Vnode ranges:      {n_ranges}")
            for r in ranges:
                token_short = _short_token(r.get("token_hex", ""))
                start = r.get("start_pid", 0)
                end = r.get("end_pid", 0)
                count = r.get("count", 0)
                self._console.print(
                    f"      token {token_short}  →  "
                    f"pids  {start}–{end}  ({count} partitions)"
                )
        self._console.print()

    def _render_members_section(
        self,
        data: dict[str, Any],
        target_node_id: str,
    ) -> None:
        """Render the Cluster membership section."""
        members: list[dict[str, Any]] = data.get("members", [])
        total = data.get("members_total", len(members))
        self._console.print(
            f"  [bold]Cluster membership[/bold]  (as seen by {target_node_id})"
        )
        self._console.print(f"    Members:  {total} total")
        for m in members:
            nid = m.get("node_id", "")
            phase = m.get("phase", "").upper()
            gen = m.get("generation", 0)
            seq = m.get("seq", 0)
            marker = "   ← this node" if nid == target_node_id else ""
            self._console.print(
                f"      {nid:<12} {phase:<12} gen={gen}  seq={seq}{marker}"
            )
        self._console.print()

    def _render_probe_section(
        self,
        data: dict[str, Any],
        target_node_id: str,
    ) -> None:
        """Render the Probe state section."""
        probes: list[dict[str, Any]] = data.get("probe_states", [])
        self._console.print(
            f"  [bold]Probe state[/bold]  ({target_node_id}'s local failure detector)"
        )
        for p in probes:
            nid = p.get("node_id", "")
            state = p.get("state", "").upper()
            self._console.print(f"    {nid}   {state}")

    def _render_peer_view(
        self,
        data: dict[str, Any],
        target_node_id: str,
    ) -> int:
        """Render NodePeerViewResponse as Rich-formatted output."""
        observed_by = data.get("observed_by", "contact")
        self._console.print(
            f"Gossip view of {target_node_id}  (observed by {observed_by}) ...\n"
        )
        self._console.print(
            f"  [yellow]⚠ This is {observed_by}'s cached gossip record, "
            f"not a live response from {target_node_id}.[/yellow]\n"
        )

        phase = data.get("phase", "").upper()
        probe_state = data.get("probe_state", "").upper()
        phi_val = data.get("phi", 0.0)
        token_count = len(data.get("tokens", []))

        self._console.print(f"  Phase:       {phase}")
        self._console.print(f"  Generation:  {data.get('generation', 0)}")
        self._console.print(f"  Seq:         {data.get('seq', 0)}")
        self._console.print(f"  Peer:        {data.get('peer_address', '')}")
        self._console.print(f"  Tokens:      {token_count} tokens")
        self._console.print(f"  Probe state: {probe_state}  (φ = {phi_val:.2f})")
        return 0


def _short_token(token_hex: str) -> str:
    """Return the first 8 hex digits of *token_hex* followed by '…'.

    Expects a string of the form '0x<hex>'. Returns '0x<8chars>…' for display.
    Falls back to the full string if it is too short.
    """
    if token_hex.startswith("0x") and len(token_hex) > 10:
        return token_hex[:10] + "…"
    return token_hex
