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
"""GossipBootstrapper — full-resync bootstrap with exponential backoff."""

import asyncio
import logging
import random
import ssl

from tourillon.core.exceptions import (
    BootstrapAttemptError,
    BootstrapError,
    BootstrapPartitionShiftError,
)
from tourillon.core.ring.topology import TopologyManager
from tourillon.core.structure.config import GossipBootstrapConfig
from tourillon.core.structure.member import Member, MemberPhase
from tourlib.envelope import Envelope
from tourlib.exceptions import ConnectionClosedError, ResponseTimeoutError
from tourlib.ports.serializer import Serializer
from tourlib.transport import TcpClient

logger = logging.getLogger(__name__)


class GossipBootstrapper:
    """Encapsulates the gossip bootstrap retry loop.

    Attempts full-resync from each seed address concurrently. A single
    bootstrap attempt contacts all seeds in parallel and accepts partial
    success (seeds_ok > 0). Retries on total failure using
    GossipBootstrapConfig parameters. Raises BootstrapError after max_retries.

    BootstrapPartitionShiftError propagates immediately — it is never retried.
    """

    def __init__(
        self,
        topology_manager: TopologyManager,
        config: GossipBootstrapConfig,
        partition_shift: int,
        ssl_ctx: ssl.SSLContext,
        serializer: Serializer,
    ) -> None:
        self._topology_manager = topology_manager
        self._config = config
        self._partition_shift = partition_shift
        self._ssl_ctx = ssl_ctx
        self._serializer = serializer

    async def run(self, seeds: list[str]) -> int:
        """Run the bootstrap retry loop.

        Return seeds_ok count on success. Raise BootstrapError when
        max_retries is exhausted without any seed responding.
        Raise BootstrapPartitionShiftError immediately on cluster incompatibility.
        """
        delay = self._config.initial_delay_s
        attempt = 0
        while True:
            attempt += 1
            try:
                seeds_ok = await self._attempt(seeds)
                logger.info(
                    "Gossip bootstrap complete. seeds_ok=%d seeds_err=%d",
                    seeds_ok,
                    len(seeds) - seeds_ok,
                )
                return seeds_ok
            except BootstrapPartitionShiftError:
                raise  # never retried — incompatible cluster, exit immediately
            except BootstrapAttemptError as err:
                max_r = self._config.max_retries
                if 0 < max_r <= attempt:
                    raise BootstrapError(
                        f"no seed responded after {attempt} attempts"
                    ) from err
                jittered = delay * (
                    1 + random.uniform(-self._config.jitter, self._config.jitter)
                )
                logger.warning(
                    "Gossip bootstrap attempt %d/%s failed; retrying in %.1fs.",
                    attempt,
                    max_r or "∞",
                    jittered,
                )
                await asyncio.sleep(jittered)
                delay = min(delay * self._config.multiplier, self._config.max_delay_s)

    async def _attempt(self, seeds: list[str]) -> int:
        """Run one concurrent pass across all seeds; return seeds_ok count.

        Raise BootstrapAttemptError when no seed responds.
        Raise BootstrapPartitionShiftError immediately on mismatch.
        """
        results: list[bool] = [False] * len(seeds)

        async with asyncio.TaskGroup() as tg:
            for idx, seed in enumerate(seeds):
                tg.create_task(
                    self._bootstrap_from_seed(seed, results, idx),
                    name=f"bootstrap-{seed}",
                )

        seeds_ok = sum(1 for r in results if r)
        if seeds_ok == 0:
            raise BootstrapAttemptError("no seed responded")
        return seeds_ok

    async def _bootstrap_from_seed(
        self, seed_address: str, results: list[bool], idx: int
    ) -> None:
        """Contact one seed and merge its delta into the local registry.

        Validates partition_shift on each delta member before merge_registry.
        Sets results[idx] = True on success.
        """

        client = TcpClient()
        try:
            async with asyncio.timeout(self._config.connect_timeout):
                await client.connect(seed_address, self._ssl_ctx)

            members = await self._exchange_digest(client, seed_address)
            # Validate partition_shift before writing any member to the registry.
            for m in members:
                if m.partition_shift != self._partition_shift:
                    logger.error(
                        "Bootstrap seed %s returned member %r with "
                        "partition_shift=%d; local partition_shift=%d. "
                        "Cluster is incompatible with this node's config.",
                        seed_address,
                        m.node_id,
                        m.partition_shift,
                        self._partition_shift,
                    )
                    raise BootstrapPartitionShiftError(
                        seed_address, m.partition_shift, self._partition_shift
                    )
            await self._topology_manager.merge_registry(members)
            results[idx] = True
        except BootstrapPartitionShiftError:
            raise  # propagate without logging again
        except (
            OSError,
            TimeoutError,
            ConnectionClosedError,
            ResponseTimeoutError,
        ) as exc:
            logger.warning("Seed %s unreachable: %s.", seed_address, exc)
        finally:
            await client.close()

    async def _exchange_digest(
        self, client: TcpClient, seed_address: str
    ) -> list[Member]:
        """Send an empty gossip.digest and collect all delta pages.

        Returns a flat list of raw member dicts (or deserialized Member objects
        when a serializer is provided). In tests, the fake client returns Member
        objects directly via a special stream protocol.
        """
        # Build the gossip.digest envelope (empty members list = full resync).
        payload = self._serializer.encode(
            {"sender": "", "has_more": False, "after_node_id": "", "members": []}
        )
        schema_id = self._serializer.schema_id

        env = Envelope.create(payload, kind="gossip.digest", schema_id=schema_id)
        members: list[Member] = []

        async for delta_env in client.stream(env):
            if delta_env.kind == "gossip.error":
                logger.warning(
                    "Seed %s returned gossip.error for digest.", seed_address
                )
                break
            if delta_env.kind == "gossip.delta":
                delta_members = self._decode_delta(delta_env)
                members.extend(delta_members)
                data = self._serializer.decode(delta_env.payload)  # type: ignore[union-attr]
                if not data.get("has_more", False):
                    break
            else:
                # Unexpected kind — treat as terminal.
                break

        return members

    def _decode_delta(self, env: Envelope) -> list[Member]:
        """Decode a gossip.delta envelope into Member objects when serializer exists."""
        try:
            data = self._serializer.decode(env.payload)  # type: ignore[union-attr]
            results = []
            for m in data.get("members", []):
                results.append(
                    Member(
                        node_id=m["node_id"],
                        peer_address=m.get("peer_address", ""),
                        generation=m["generation"],
                        seq=m["seq"],
                        phase=MemberPhase(m["phase"]),
                        tokens=tuple(m.get("tokens", [])),
                        partition_shift=m["partition_shift"],
                    )
                )
            return results
        except Exception as exc:
            logger.warning("Failed to decode delta payload: %s", exc)
            return []
