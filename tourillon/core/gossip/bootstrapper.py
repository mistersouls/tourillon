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

import asyncio
import random
import ssl
from typing import Any

from tourillon.core.exceptions import (
    BootstrapAttemptError,
    BootstrapError,
    BootstrapPartitionShiftError,
)
from tourillon.core.gossip.topology import TopologyManager
from tourillon.core.log import logger
from tourillon.core.models.member import Member
from tourillon.core.models.retry import BackoffRetry
from tourlib import Serializer
from tourlib.constant import ENVELOPE_CONNECTION_TIMEOUT
from tourlib.envelope import build_envelope
from tourlib.exceptions import ResponseTimeoutError
from tourlib.transport import EnvelopeClient


class GossipBootstrapper:
    def __init__(
        self,
        node_id: str,
        serializer: Serializer,
        topologymgr: TopologyManager,
        backoff: BackoffRetry,
        tls_client: ssl.SSLContext,
        connection_timeout: float = ENVELOPE_CONNECTION_TIMEOUT,
    ) -> None:
        self._node_id = node_id
        self._serializer = serializer
        self._topologymgr = topologymgr
        self._backoff = backoff
        self._tls_client = tls_client
        self._connection_timeout = connection_timeout

    async def run(self, seeds: tuple[str, ...]) -> int:
        if not seeds:
            raise RuntimeError("Cannot run bootstrap without seeds")

        delay = self._backoff.initial_delay
        attempt = 0
        while True:
            attempt += 1
            try:
                # Attempt to join the cluster using the provided seeds
                seeds_ok = await self._attempt(seeds)
                logger.info(
                    "Gossip bootstrap completed successfully "
                    "after %d attempt(s). seeds_ok=%d seeds_err=%d",
                    attempt,
                    seeds_ok,
                    len(seeds) - seeds_ok
                )
                return seeds_ok
            except BootstrapPartitionShiftError:
                raise
            except BootstrapAttemptError as exc:
                max_retries = self._backoff.max_retries
                if 0 < max_retries <= attempt:
                    logger.error(
                        "Gossip bootstrap failed after %d attempt(s): %s",
                        attempt,
                        str(exc)
                    )
                    raise BootstrapError(
                        f"No seed responded after {attempt} attempt(s)"
                    ) from exc

                jittered = delay * (1 + random.uniform(-self._backoff.jitter, self._backoff.jitter))
                logger.warning(
                    "Gossip bootstrap attempt %d/%s failed: %s, retrying in %.1fs.",
                    attempt, max_retries or "∞", str(exc), jittered
                )
                await asyncio.sleep(jittered)
                delay = min(delay * self._backoff.factor, self._backoff.max_delay)

    async def _attempt(self, seeds: tuple[str, ...]) -> int:
        results: list[bool] = [False] * len(seeds)

        async with asyncio.TaskGroup() as tg:
            for idx, seed in enumerate(seeds):
                tg.create_task(
                    self._attempt_seed(seed, results, idx),
                    name=f"gossip-bootstrap-seed-{seed}"
                )

        seeds_ok = sum(1 for result in results if result)
        if seeds_ok == 0:
            raise BootstrapAttemptError("No seed responded successfully")
        return seeds_ok

    async def _attempt_seed(
        self,
        seed_address: str,
        results: list[bool],
        idx: int
    ) -> None:
        client = EnvelopeClient(seed_address, tls_ctx=self._tls_client, logger=logger.transport())

        try:
            async with asyncio.timeout(self._connection_timeout):
                await client.connect()

            members = await self._exchange_digest(client, seed_address)
            await self._topologymgr.merge_registry(members)
            results[idx] = True
        except BootstrapPartitionShiftError:
            raise
        except (OSError, TimeoutError, ConnectionError, ResponseTimeoutError) as exc:
            logger.warning("Seed %s is unreachable: %s", seed_address, str(exc))
        finally:
            await client.close()

    async def _exchange_digest(
        self,
        client: EnvelopeClient,
        seed_address: str
    ) -> list[Member]:
        payload = {
            "sender": self._node_id,
            "has_more": False,
            "after_node_id": "",
            "digest": [],
        }
        digest = build_envelope(
            kind="gossip.digest",
            payload=self._serializer.encode(payload),
            schema_id=self._serializer.schema_id,
        )
        members: list[Member] = []

        async for delta in client.stream(digest):
            if delta.kind == "gossip.error":
                logger.warning("Seed %s returned gossip.error for digest.", seed_address)
                break
            if delta.kind == "gossip.delta":
                data = self._serializer.decode(delta.payload)
                delta_members = self._decode_delta(data.get("members", []))
                members.extend(delta_members)
                if not data.get("has_more", False):
                    break
            else:
                logger.warning("Seed %s returned unexpected envelope kind: %s", seed_address, delta.kind)
                break

        return members

    @staticmethod
    def _decode_delta(members_data: list[dict[str, Any]]) -> list[Member]:
        members: list[Member] = []
        for member_data in members_data:
            try:
                member = Member.from_dict(member_data)
                members.append(member)
            except (KeyError, TypeError, ValueError) as exc:
                logger.warning("Failed to decode member data: %s, error: %s", member_data, str(exc))
        return members
