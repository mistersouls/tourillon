from typing import Any

from tourillon.core.exceptions import AntiEntropyRejectError
from tourillon.core.peer import PeerPool
from tourlib import Serializer
from tourlib.envelope import build_envelope
from tourlib.exceptions import ProtocolError
from tourlib.handle import DictPayload


class Caller:
    def __init__(self, peer_pool: PeerPool, serializer: Serializer) -> None:
        self._pool = peer_pool
        self._serializer = serializer


    async def rpc(
        self,
        peer: str,
        kind: str,
        payload: DictPayload
    ) -> dict[str, Any]:
        request = build_envelope(
            kind=kind,
            payload=self._serializer.encode(payload.to_dict()),
            schema_id=self._serializer.schema_id,
        )
        async with self._pool.lease(peer) as client:
            resp = await client.request(request)

        if resp.kind == f"{kind}.accept":
            data = self._serializer.decode(resp.payload)
            return data if isinstance(data, dict) else {}

        if resp.kind == f"{kind}.reject":
            raise AntiEntropyRejectError.from_dict(
                self._serializer.decode(resp.payload)
            )

        raise ProtocolError(
            f"unexpected response kind '{resp.kind}' for {kind!r} request"
        )
