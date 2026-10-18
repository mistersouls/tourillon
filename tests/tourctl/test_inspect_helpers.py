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

import ssl

import pytest

from tourclient.exception import UnknownServiceError
from tourctl.core.inspect import (
    _format_pid_label,
    _gossip_state,
    _inspect_error_message,
    _merge_members,
    _render_text,
)
from tourlib.exceptions import ProtocolError, ResponseError, ValidationError
from tourlib.models.inspect import (
    MemberInspect,
    NodeInfo,
    RingPositionInspect,
    VnodeRangeInspect,
)


def _node_info(*, members: tuple[MemberInspect, ...]) -> NodeInfo:
    return NodeInfo(
        node_id="node-1",
        snapshot_at="2026-09-01T00:00:00Z",
        chunk_index=0,
        has_more=False,
        next_cursor=None,
        self_member=MemberInspect(node_id="node-1", phase="READY", status="-", epoch=3),
        members=members,
        ring_position=RingPositionInspect(
            partition_shift=10,
            total_partitions=1024,
            owned_partitions=12,
            owned_percent=1.2,
            vnode_ranges=(
                VnodeRangeInspect(
                    token="0x123456789abcdef", arc=(900, 120), partitions=244
                ),
            ),
        ),
        gossip_stats={
            "known_members": 3,
            "ae_cycles_total": 7,
            "ae_diverged": 0,
            "last_ae_peer": None,
            "last_ae_at": None,
            "bootstrap_ok": 1,
            "bootstrap_err": 0,
            "last_hot_failure": None,
            "last_ae_failure": None,
            "last_stop_failure": None,
        },
    )


@pytest.mark.tourctl
@pytest.mark.parametrize(
    ("exc", "context_name", "expected"),
    [
        (
            FileNotFoundError("missing"),
            None,
            "Unable to load the Tourillon client config",
        ),
        (KeyError("clusters.default"), None, "Missing client configuration entry"),
        (KeyError("prod"), "prod", "Unknown client context 'prod'"),
        (ValidationError("bad"), None, "Invalid Tourillon client configuration"),
        (
            ssl.SSLError("bad tls"),
            None,
            "TLS setup or handshake failed during node inspect",
        ),
        (
            OSError("network down"),
            None,
            "Unable to reach the Tourillon node for inspect",
        ),
        (ProtocolError("bad frame"), None, "invalid inspect response"),
        (ResponseError("bad response"), None, "invalid inspect response"),
        (
            UnknownServiceError("Unknown service: inspect"),
            None,
            "Unknown service: inspect",
        ),
        (RuntimeError("boom"), None, "Node inspect failed: boom"),
    ],
)
def test_inspect_error_message_branches(
    exc: Exception, context_name: str | None, expected: str
) -> None:
    assert expected in _inspect_error_message(exc, context_name)


@pytest.mark.tourctl
def test_render_text_handles_empty_chunks() -> None:
    output = _render_text("127.0.0.1:7701", [])

    assert "No data received" in output


@pytest.mark.tourctl
def test_render_text_merges_members_and_formats_wraparound() -> None:
    first = _node_info(
        members=(
            MemberInspect(node_id="node-2", phase="READY", status="UNKNOWN", epoch=1),
            MemberInspect(node_id="node-3", phase="JOINING", status="SUSPECT", epoch=2),
        )
    )
    second = _node_info(
        members=(
            MemberInspect(node_id="node-2", phase="READY", status="DEAD", epoch=4),
        )
    )

    output = _render_text("127.0.0.1:7701", [first, second])

    assert "Inspecting 127.0.0.1:7701 (node-1)" in output
    assert "gossip=stable" in output
    assert "901-120(w)" in output
    assert "node-2" in output
    assert "DEAD" in output


@pytest.mark.tourctl
def test_merge_members_keeps_latest_entry_per_node() -> None:
    first = _node_info(
        members=(
            MemberInspect(node_id="node-2", phase="READY", status="UNKNOWN", epoch=1),
        )
    )
    second = _node_info(
        members=(
            MemberInspect(node_id="node-2", phase="READY", status="DEAD", epoch=2),
        )
    )

    merged = _merge_members([first, second])

    assert merged["node-2"].status == "DEAD"


@pytest.mark.tourctl
def test_format_pid_label_variants() -> None:
    assert _format_pid_label(None, 1024) == "-"
    assert _format_pid_label((9, 12), 0) == "9-12"
    assert _format_pid_label((9, 12), 1024) == "10-12"
    assert _format_pid_label((900, 120), 1024) == "901-120(w)"


@pytest.mark.tourctl
def test_gossip_state_variants() -> None:
    assert _gossip_state({"last_hot_failure": "boom"}) == "degraded"
    assert _gossip_state({"last_ae_failure": "boom"}) == "degraded"
    assert _gossip_state({"ae_diverged": 1}) == "active"
    assert _gossip_state({"ae_diverged": 0}) == "stable"
