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

import pytest

from tests.helpers import make_envelope
from tourillon.core.transports.dispatcher import Dispatcher
from tourlib.envelope import Envelope


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_dispatcher_register_and_lookup() -> None:
    dispatcher = Dispatcher()

    async def handler(envelope: Envelope):
        yield Envelope(
            proto_version=envelope.proto_version,
            schema_id=envelope.schema_id,
            mode=Envelope.MODE_RPC,
            correlation_id=envelope.correlation_id,
            kind="kv.get",
            payload=b"ok",
        )

    dispatcher.register("kv.get", handler)

    resolved = dispatcher.lookup("kv.get")
    assert resolved is not None
    responses = [response async for response in resolved(make_envelope())]
    assert responses[0].payload == b"ok"


@pytest.mark.tourillon
@pytest.mark.asyncio
async def test_dispatcher_on_decorator_registers_handler() -> None:
    dispatcher = Dispatcher()

    @dispatcher.on("gossip.ping")
    async def handler(envelope: Envelope):
        yield Envelope(
            proto_version=envelope.proto_version,
            schema_id=envelope.schema_id,
            mode=Envelope.MODE_RPC,
            correlation_id=envelope.correlation_id,
            kind="gossip.ping",
            payload=b"pong",
        )

    resolved = dispatcher.lookup("gossip.ping")
    assert resolved is handler
    responses = [response async for response in resolved(make_envelope())]
    assert responses[0].payload == b"pong"


@pytest.mark.tourillon
def test_dispatcher_rejects_duplicate_registration() -> None:
    dispatcher = Dispatcher()

    async def first(envelope: Envelope):
        yield Envelope(
            proto_version=envelope.proto_version,
            schema_id=envelope.schema_id,
            mode=Envelope.MODE_RPC,
            correlation_id=envelope.correlation_id,
            kind="kv.put",
            payload=b"first",
        )

    async def second(envelope: Envelope):
        yield Envelope(
            proto_version=envelope.proto_version,
            schema_id=envelope.schema_id,
            mode=Envelope.MODE_RPC,
            correlation_id=envelope.correlation_id,
            kind="kv.put",
            payload=b"second",
        )

    dispatcher.register("kv.put", first)

    with pytest.raises(ValueError, match="already registered"):
        dispatcher.register("kv.put", second)


