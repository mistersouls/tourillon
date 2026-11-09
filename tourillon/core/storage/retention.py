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

"""Shared version retention, evaluated inside the storage transaction."""

from collections.abc import Sequence
from typing import Literal

from tourillon.core.models.config import RetentionConfig
from tourillon.core.models.repair import VersionLookup
from tourillon.core.models.storageschema import Key, KeyTag, PartitionAddress, Record

WriteDisposition = Literal["applied", "already_present", "ignored_retention"]


class RetentionPolicy:
    """Explicit retention operations inside a caller-owned group transaction."""

    def __init__(self, policy: RetentionConfig) -> None:
        self._policy = policy

    @property
    def versions(self) -> int:
        return self._policy.versions

    async def _newest(
        self, session: GroupSession, keys: Sequence[Key]
    ) -> dict[PartitionAddress, list[KeyTag]]:
        addresses = [PartitionAddress(k.pid, k.address) for k in keys]
        return await session.latest_identities_many(addresses, self._policy.versions)

    def _outside(self, key: Key, newest: dict[PartitionAddress, list[KeyTag]]) -> bool:
        identities = newest[PartitionAddress(key.pid, key.address)]
        return sum(item.key.ts > key.ts for item in identities) >= self._policy.versions

    async def apply(
        self, session: GroupSession, records: Sequence[Record]
    ) -> list[WriteDisposition]:
        """Report applied/already_present/ignored_retention for every input."""
        newest = await self._newest(session, [r.key for r in records])
        accepted = [not self._outside(r.key, newest) for r in records]
        selected = [r for r, ok in zip(records, accepted, strict=True) if ok]
        inserted = iter(await session.put_many(selected))
        return [
            (
                ("applied" if next(inserted) else "already_present")
                if ok
                else "ignored_retention"
            )
            for ok in accepted
        ]

    async def lookup(self, session: GroupSession, key: Key) -> VersionLookup:
        newest = await self._newest(session, [key])
        exact = await session.read(key)
        identities = newest[PartitionAddress(key.pid, key.address)]
        return VersionLookup(exact, tuple(identities), self._outside(key, newest))

    async def collect(
        self, session: GroupSession, keys: Sequence[Key], *, dry_run: bool = False
    ) -> tuple[int, int]:
        newest = await self._newest(session, keys)
        candidates = [key for key in dict.fromkeys(keys) if self._outside(key, newest)]
        return await session.remove(candidates, dry_run=dry_run)
