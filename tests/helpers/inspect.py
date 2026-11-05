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
"""Factories for the operator view models (``node.inspect``, ``node.members``)."""

import ssl
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path
from typing import Any

from tests.helpers.config import generate_ca
from tests.helpers.server import start_envelope_server
from tourillon.core.config import ClientConfigGenerator
from tourlib.adapter import CryptographyMtlsAdapter, MsgPackSerializer, TomlPersistence
from tourlib.envelope import Envelope, replay_envelope
from tourlib.models.gc import GcInfo, GcStatus
from tourlib.models.inspect import (
    AntiEntropyInfo,
    MembersPage,
    MembersSummary,
    MemberView,
    NodeStatus,
    PhaseCount,
    RingPositionInspect,
    VnodeRangeInspect,
)


def make_antientropy(**overrides: Any) -> AntiEntropyInfo:
    info = AntiEntropyInfo(
        queued=0,
        inflight=0,
        backlog=False,
        sweeps=2,
        last_sweep_at=None,
        last_sweep_diverged=0,
        delivered=0,
        errors=0,
    )
    return replace(info, **overrides)


def make_gc(*, running: bool = True, interval: float = 600, **status: Any) -> GcInfo:
    return GcInfo(running, 2, interval, 0, GcStatus(**status))


def make_status(**overrides: Any) -> NodeStatus:
    status = NodeStatus(
        node_id="node-3",
        address="10.0.0.3:7701",
        version="1.2.3",
        snapshot_at="2026-11-05T10:00:00+00:00",
        phase="READY",
        epoch=3,
        since_s=125.0,
        ring_position=RingPositionInspect(
            partition_shift=10,
            total_partitions=1024,
            owned_partitions=256,
            owned_percent=25.0,
            vnode_ranges=(
                VnodeRangeInspect(token="0x1a", arc=(900, 120), partitions=244),
                VnodeRangeInspect(token="0x2b", arc=(120, 132), partitions=12),
            ),
        ),
        members=MembersSummary(total=3, ready_alive=3),
        antientropy=make_antientropy(),
        gc=make_gc(),
        gossip_stats={"known_members": 3},
    )
    return replace(status, **overrides)


def make_view(node_id: str, phase: str = "READY", status: str = "ALIVE") -> MemberView:
    return MemberView(
        node_id=node_id,
        address=f"{node_id}:7701",
        phase=phase,
        status=status,
        epoch=1,
        last_heard_s=4.0,
        since_s=90.0,
    )


def make_page(
    *items: MemberView,
    summary: tuple[PhaseCount, ...] = (PhaseCount("READY", alive=3),),
    total: int | None = None,
    cursor: str | None = None,
    anomalies_only: bool = True,
) -> MembersPage:
    return MembersPage(
        node_id="node-1",
        snapshot_at="2026-11-05T10:00:00+00:00",
        members=sum(row.total for row in summary),
        summary=summary,
        items=items,
        total=len(items) if total is None else total,
        cursor=cursor,
        anomalies_only=anomalies_only,
    )


WIRE = MsgPackSerializer()


def reply(kind: str, payload: Any, seen: list[Any] | None = None):
    """Envelope handler answering every request with ``kind`` and ``payload``."""

    async def handler(request: Envelope) -> AsyncIterator[Envelope]:
        if seen is not None:
            seen.append(WIRE.decode(request.payload))
        yield replay_envelope(kind=kind, request=request, payload=WIRE.encode(payload))

    return handler


def _server_ssl(tmp_path: Path, ca_cert: Path, ca_key: Path) -> ssl.SSLContext:
    cert_pem, key_pem = CryptographyMtlsAdapter().generate_cert(
        common_name="tourillon-server",
        ca_certificate_pem=ca_cert.read_bytes(),
        ca_private_key_pem=ca_key.read_bytes(),
        validity_days=30,
        key_size=2048,
        client_only=False,
    )
    cert_path = tmp_path / "server-cert.pem"
    key_path = tmp_path / "server-key.pem"
    cert_path.write_bytes(cert_pem)
    key_path.write_bytes(key_pem)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(str(cert_path), str(key_path))
    context.load_verify_locations(cafile=str(ca_cert))
    context.verify_mode = ssl.CERT_REQUIRED
    return context


async def make_tourconf(tmp_path: Path) -> tuple[Path, ssl.SSLContext]:
    """A client config and a server context signed by the same test CA."""
    ca_cert, ca_key = generate_ca(tmp_path)
    result = await ClientConfigGenerator(
        CryptographyMtlsAdapter(), TomlPersistence()
    ).generate(
        output_file=tmp_path / "tourconf.toml",
        common_name="tourctl-inspect",
        ca_certificate_path=ca_cert,
        ca_private_key_path=ca_key,
        client_address="127.0.0.1:9",
        validity_days=30,
        key_size=2048,
        overwrite=False,
    )
    return result.config_path, _server_ssl(tmp_path, ca_cert, ca_key)


@asynccontextmanager
async def inspect_server(
    tmp_path: Path, handlers: Mapping[str, Any]
) -> AsyncIterator[tuple[str, Path]]:
    """Serve ``handlers`` over mTLS; yield the address and a matching tourconf."""
    tourconf, server_ssl = await make_tourconf(tmp_path)
    server = await start_envelope_server(handlers, server_ssl)
    try:
        host, port = server.endpoint
        yield f"{host}:{port}", tourconf
    finally:
        await server.stop()
