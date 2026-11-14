from typing import AsyncIterator

from tourillon.bootstrap.deps import peer_dispatcher, get_manager
from tourillon.core.exceptions import AntiEntropyRejectError
from tourillon.core.models.ae import RootsRequest
from tourlib.envelope import Envelope
from tourlib.handle import DictReplier, MalformedError

dispatcher = peer_dispatcher()


@dispatcher.on("ae.roots")
async def init(request: Envelope) -> AsyncIterator[Envelope]:
    manager = get_manager()
    replier = DictReplier(manager.serializer, request)

    try:
        response = await manager.ae.roots(replier.decode(RootsRequest))
        yield replier.reply("ae.roots.accept", response)
    except MalformedError:
        ...
    except AntiEntropyRejectError as exc:
        yield replier.reply("ae.roots.reject", exc)
