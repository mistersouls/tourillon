from typing import Any

from tourillon.core.exceptions import GossipError
from tourillon.core.ring.topology import TopologyManager
from tourillon.core.structure.member import Member
from tourlib.ports.serializer import Serializer


class Gossiper:
    def __init__(
        self,
        node_id: str,
        partition_shift: int,
        max_digest_entries: int,
        topology_manager: TopologyManager,
        serializer: Serializer,
    ) -> None:
        self._node_id = node_id
        self._partition_shift = partition_shift
        self._max_digest_entries = max_digest_entries
        self._topology_manager = topology_manager
        self._serializer = serializer

    async def diff(self, data: dict[str, Any]) -> dict[str, Any]:
        digest_entries: list[dict[str, Any]] = data.get("members", [])
        if len(digest_entries) > self._max_digest_entries:
            raise self._gossip_error(
                "gossip.digest",
                "payload_too_large",
                f"digest count {len(digest_entries)} exceeds limit {self._max_digest_entries}",
                self._node_id,
            )

        # Build version-vector lookup from digest
        peer_versions: dict[str, tuple[int, int]] = {}
        for entry in digest_entries:
            peer_versions[entry["node_id"]] = (
                int(entry["generation"]),
                int(entry["seq"]),
            )

        topo = await self._topology_manager.snapshot()
        local_ids = {m.node_id for m in topo.registry}
        ahead: list[dict[str, Any]] = []
        for member in sorted(topo.registry, key=lambda m: m.node_id):
            peer_ver = peer_versions.get(member.node_id)
            if peer_ver is None or (member.generation, member.seq) > peer_ver:
                ahead.append(member.to_dict())

        # Symmetric AE: tell the initiator which node_ids it knows about but
        # we do not, so it can send them back via gossip.push. This closes
        # the convergence loop for a peer that lost its registry on restart.
        wanted = sorted(nid for nid in peer_versions if nid not in local_ids)

        # Send a single delta page (pagination left for future extension).
        delta_payload = {
                "sender": self._node_id,
                "has_more": False,
                "members": ahead,
                "wanted": wanted,
        }
        return delta_payload

    async def get_memberships_state(self, data: dict[str, Any]) -> dict[str, Any]:
        topo = await self._topology_manager.snapshot()
        fingerprint = await self._topology_manager.member_fingerprint()
        member_count = len(list(topo.registry))
        same = (
            data.get("epoch") == topo.epoch
            and data.get("fingerprint") == fingerprint
            and data.get("member_count") == member_count
        )
        pong_payload = {
            "sender": self._node_id,
            "epoch": topo.epoch,
            "fingerprint": fingerprint,
            "member_count": member_count,
            "same": same,
        }
        return pong_payload

    async def update_memberships(self, data: dict[str, Any]) -> dict[str, Any]:
        members_raw: list[dict[str, Any]] = data.get("members", [])
        if len(members_raw) > 4096:
            raise self._gossip_error(
                "gossip.push",
                "payload_too_large",
                f"members count {len(members_raw)} exceeds limit 4096",
                self._node_id,
            )

        accepted = 0
        ignored = 0
        for raw in members_raw:
            try:
                member = Member.from_dict(raw)
            except (ValueError, KeyError) as exc:
                raise self._gossip_error(
                    "gossip.push",
                    "invalid_member",
                    str(exc),
                    self._node_id
                ) from exc

            if member.partition_shift != self._partition_shift:
                raise self._gossip_error(
                    "gossip.push",
                    "partition_shift_mismatch",
                    f"member {member.node_id!r} has partition_shift="
                    f"{member.partition_shift}; local={self._partition_shift}",
                    self._node_id,
                )

            if await self._topology_manager.apply_member(member):
                accepted += 1
            else:
                ignored += 1

        ok_payload = {
            "sender": self._node_id,
            "accepted": accepted,
            "ignored": ignored
        }

        return ok_payload

    @staticmethod
    def _gossip_error(
        kind: str,
        code: str,
        message: str,
        sender: str,
    ) -> GossipError:
        """Build a gossip.error envelope."""
        payload = {
            "sender": sender,
            "rejected_kind": kind,
            "code": code,
            "message": message,
        }

        return GossipError(kind="gossip.error", payload=payload)
