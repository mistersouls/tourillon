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
import json
import socket
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pytest

import tourctl.core.cluster as cluster_module
from tests.helpers.inspect import (
    inspect_server,
    make_page,
    make_tourconf,
    make_view,
    reply,
)
from tourclient.exception import RequestRejectedError
from tourctl.bootstrap.main import main
from tourctl.core.cluster import (
    Target,
    build_request,
    capped,
    cluster_member,
    cluster_status,
    member_messages,
    next_page_command,
    page_document,
    page_messages,
    render_member,
    render_page,
    watch_cluster_status,
)
from tourctl.core.error import (
    EXIT_FAILED,
    EXIT_INVALID,
    EXIT_NOT_FOUND,
    EXIT_UNREACHABLE,
    CliError,
)
from tourlib.models.inspect import MembersRequest, PhaseCount
from tourlib.models.member import (
    MemberDebt,
    MemberReport,
    StandIn,
    UnansweredNeighbor,
)

pytestmark = pytest.mark.tourctl

TARGET = Target(address="10.0.0.1:7701", tourconf=None, context_name="prod")


def _member_report() -> MemberReport:
    return MemberReport(
        node_id="node-2",
        view_from="node-1",
        address="10.0.0.2:7701",
        phase="READY",
        epoch=3,
        status="DEAD",
        since_s=840,
        last_heard_s=840,
        ring_share_percent=0.0098,
        partitions=12,
        total_partitions=131_072,
        vnodes=4,
        answered=1,
        neighbors=2,
        owed_by=(
            MemberDebt(
                holder="node-3",
                versions=270,
                oldest_wall_ms=int(time.time() * 1000) - 840_000,
                phase="READY",
                status="ALIVE",
                retry_in_s=0,
                last_error=None,
                stuck=False,
            ),
        ),
        unanswered=(UnansweredNeighbor("node-4", "SUSPECT"),),
        stand_ins=(StandIn("node-3", 12),),
    )


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _run_ok(tokens: list[str]) -> None:
    try:
        main(tokens)
    except SystemExit as exc:
        assert exc.code in (0, None)


def _request(**kwargs: Any) -> MembersRequest:
    options = {
        "list_all": False,
        "phases": (),
        "statuses": (),
        "prefix": None,
        "limit": 50,
        "cursor": None,
    }
    return build_request(**{**options, **kwargs})


def test_healthy_cluster_shows_the_summary_only() -> None:
    page = make_page(summary=(PhaseCount("READY", alive=1200),))

    text = render_page(page, 0.4, _request(), TARGET)

    assert text == (
        "View from: node-1 (gossip, <1ms old)\n"
        "Members:   1,200\n"
        "\n"
        "PHASE   ALIVE   SUSPECT   DEAD\n"
        "READY   1,200   0         0\n"
        "\n"
        "Messages:\n"
        "  All members are READY and ALIVE."
    )


def test_anomalies_are_listed_with_their_ages() -> None:
    page = make_page(
        make_view("node-7", status="DEAD"),
        make_view("node-9", phase="JOINING"),
        summary=(PhaseCount("READY", alive=3, dead=1), PhaseCount("JOINING", 1)),
    )

    text = render_page(page, 12.4, _request(), TARGET)

    assert "View from: node-1 (gossip, 12ms old)" in text
    assert (
        "ANOMALIES (2)\n"
        "NODE     ADDRESS       PHASE     STATUS   LAST HEARD   SINCE\n"
        "node-7   node-7:7701   READY     DEAD     4s           1m30s\n"
        "node-9   node-9:7701   JOINING   ALIVE    4s           1m30s"
    ) in text
    assert "Showing" not in text
    assert text.endswith(
        "Messages:\n"
        "  1 node is DEAD; their replicas are served by stand-ins.\n"
        "  1 node is JOINING; anti-entropy is filling their partitions."
    )


def test_missing_ages_render_as_dashes() -> None:
    view = make_view("node-2", status="SUSPECT")
    object.__setattr__(view, "last_heard_s", None)
    object.__setattr__(view, "since_s", None)

    text = render_page(make_page(view), 1, _request(), TARGET)

    assert "node-2   node-2:7701   READY   SUSPECT   -            -" in text


def test_filtered_listing_uses_the_members_heading() -> None:
    page = make_page(make_view("node-1"), anomalies_only=False)

    text = render_page(page, 1, _request(list_all=True), TARGET)

    assert "MEMBERS (1)\nNODE" in text


def test_filters_without_match_say_so() -> None:
    page = make_page(anomalies_only=False)

    text = render_page(page, 1, _request(phases=["IDLE"]), TARGET)

    assert "MEMBERS (0)\nNo member matches the filters." in text


def test_first_page_footer_repeats_every_option() -> None:
    request = _request(
        list_all=True,
        phases=["ready", "joining"],
        statuses=["dead"],
        prefix="node-eu",
        limit=2,
    )
    page = make_page(
        make_view("node-eu-1"),
        make_view("node-eu-2"),
        total=1500,
        cursor="CUR",
        anomalies_only=False,
    )

    text = render_page(page, 1, request, TARGET)

    assert text.split("\n\n")[2].splitlines()[-1] == (
        "Showing 2 of 1,500. Next page: tourctl cluster status -a 10.0.0.1:7701 "
        "--context prod --all --phase READY --phase JOINING --status DEAD "
        "--prefix node-eu --limit 2 --cursor CUR"
    )


def test_last_page_footer() -> None:
    page = make_page(make_view("node-3", status="DEAD"), total=51)

    text = render_page(page, 1, _request(cursor="PREV"), TARGET)

    assert "Showing 1 of 51. This is the last page." in text


def test_next_page_command_keeps_only_what_was_given() -> None:
    target = Target(address=None, tourconf=None, context_name=None)

    assert (
        next_page_command(_request(), target, "C")
        == "tourctl cluster status --cursor C"
    )


def test_page_messages_count_every_abnormal_state() -> None:
    page = make_page(
        summary=(
            PhaseCount("READY", alive=10, suspect=2, dead=3),
            PhaseCount("DRAINING", alive=1),
            PhaseCount("PAUSED", alive=2),
            PhaseCount("IDLE", dead=1),
            PhaseCount("FAILED", alive=1),
        )
    )

    assert page_messages(page) == [
        "4 nodes are DEAD; their replicas are served by stand-ins.",
        "2 nodes are SUSPECT; requests fail over while it lasts.",
        "1 node is DRAINING; anti-entropy is moving their partitions away.",
        "2 nodes are PAUSED; reads and writes for their partitions go to stand-ins.",
        "1 node is IDLE; they own no partitions.",
        "1 node is FAILED; they own no partitions.",
    ]


def test_capped_keeps_five_messages_and_counts_the_rest() -> None:
    lines = [f"m{i}" for i in range(8)]

    assert capped(lines) == [
        "m0",
        "m1",
        "m2",
        "m3",
        "m4",
        "... and 3 more (use --json for the full list)",
    ]
    assert capped(lines[:5]) == lines[:5]


def test_rendered_messages_are_capped() -> None:
    page = make_page(
        summary=(
            PhaseCount("READY", alive=1, suspect=1, dead=1),
            PhaseCount("JOINING", 1),
            PhaseCount("DRAINING", 1),
            PhaseCount("PAUSED", 1),
            PhaseCount("IDLE", 1),
        )
    )

    text = render_page(page, 1, _request(), TARGET)

    assert "  ... and 1 more (use --json for the full list)" in text


def test_page_document_adds_age_and_messages() -> None:
    page = make_page(make_view("node-2", status="DEAD"))

    document = page_document(page, 3.14159)

    assert document["view_age_ms"] == 3.142
    assert document["messages"] == ["All members are READY and ALIVE."]
    assert document["items"][0]["node_id"] == "node-2"


def test_ten_thousand_members_render_fast_and_short() -> None:
    anomalies = [make_view(f"node-{i:05}", status="DEAD") for i in range(5)] + [
        make_view(f"node-{i:05}", phase="JOINING") for i in range(5, 10)
    ]
    page = make_page(
        *anomalies,
        summary=(PhaseCount("READY", alive=9990, dead=5), PhaseCount("JOINING", 5)),
    )
    assert page.members == 10_000

    started = time.perf_counter()
    text = render_page(page, 2, _request(), TARGET)
    elapsed = time.perf_counter() - started

    assert elapsed < 0.1
    assert len(text.splitlines()) < 40
    assert "Members:   10,000" in text


@pytest.mark.parametrize(
    "options,match",
    [
        ({"limit": 0}, "limit must be an integer from 1 to 1000"),
        ({"phases": ["SLEEPING"]}, "phases must be among"),
        ({"statuses": ["LIVE"]}, "statuses must be among"),
        ({"prefix": ""}, "prefix must be a non-empty string"),
    ],
)
def test_build_request_rejects_invalid_options(options: dict, match: str) -> None:
    with pytest.raises(CliError, match=match) as info:
        _request(**options)

    assert info.value.code == EXIT_INVALID
    assert info.value.error == "invalid_option"


async def test_cluster_status_renders_text_and_json(tmp_path: Path) -> None:
    page = make_page(make_view("node-2", status="DEAD"), cursor="NEXT", total=9)
    seen: list[Any] = []
    handler = reply("node.members.accept", page.to_document(), seen)
    request = _request(limit=1)

    async with inspect_server(tmp_path, {"node.members": handler}) as (
        address,
        tourconf,
    ):
        target = Target(address=address, tourconf=tourconf, context_name=None)
        text = await cluster_status(request, target)
        document = json.loads(await cluster_status(request, target, as_json=True))

    assert seen == [request.to_document(), request.to_document()]
    assert "ANOMALIES (9)" in text
    assert (
        f"Showing 1 of 9. Next page: tourctl cluster status -a {address} "
        "--limit 1 --cursor NEXT"
    ) in text
    assert document["cursor"] == "NEXT"
    assert document["messages"] == ["All members are READY and ALIVE."]
    assert document["view_age_ms"] >= 0


async def test_cluster_member_renders_report_and_json(tmp_path: Path) -> None:
    report = _member_report()
    seen: list[Any] = []
    handler = reply("node.member.accept", report.to_document(), seen)

    async with inspect_server(tmp_path, {"node.member": handler}) as (
        address,
        tourconf,
    ):
        target = Target(address=address, tourconf=tourconf, context_name=None)
        text = await cluster_member("node-2", target)
        document = json.loads(await cluster_member("node-2", target, as_json=True))

    assert seen == [{"node_id": "node-2"}, {"node_id": "node-2"}]
    assert "Node:       node-2" in text
    assert "Status:     DEAD for 14m (view from node-1)" in text
    assert "Owed to it (answered 1 of 2 neighbours)" in text
    assert "270 versions are delivered when node-2 is ALIVE." in text
    assert "node-4 (SUSPECT)" in text
    assert document["partitions"] == 12
    assert document["messages"] == member_messages(report)


async def test_cluster_member_maps_unknown_member_to_exit_4(tmp_path: Path) -> None:
    handler = reply(
        "node.member.reject",
        {
            "code": "member_not_found",
            "message": "node-missing is not a member known to node-1.",
        },
    )

    async with inspect_server(tmp_path, {"node.member": handler}) as (
        address,
        tourconf,
    ):
        target = Target(address=address, tourconf=tourconf, context_name=None)
        with pytest.raises(CliError, match="node-missing is not a member") as info:
            await cluster_member("node-missing", target)

    assert info.value.code == EXIT_NOT_FOUND
    assert info.value.error == "member_not_found"


async def test_cluster_member_maps_an_unreachable_node_to_exit_3(
    tmp_path: Path,
) -> None:
    tourconf, _ = await make_tourconf(tmp_path)
    target = Target(f"127.0.0.1:{_free_port()}", tourconf, None)

    with pytest.raises(CliError) as info:
        await cluster_member("node-2", target)

    assert info.value.code == EXIT_UNREACHABLE


def test_member_renderer_lists_stand_ins_and_unanswered_members() -> None:
    report = _member_report()

    text = render_member(report)

    assert "Stand-ins:\n  12 partitions are served by node-3." in text
    assert "1 neighbour did not answer within 2s: node-4 (SUSPECT)." in text


@pytest.mark.parametrize(
    "code,exit_code",
    [("invalid_request", EXIT_INVALID), ("overloaded", EXIT_FAILED)],
)
async def test_cluster_status_maps_rejections(
    tmp_path: Path, code: str, exit_code: int
) -> None:
    handler = reply("node.members.reject", {"code": code, "message": "bad cursor"})

    async with inspect_server(tmp_path, {"node.members": handler}) as (
        address,
        tourconf,
    ):
        target = Target(address=address, tourconf=tourconf, context_name=None)
        with pytest.raises(CliError, match="bad cursor") as info:
            await cluster_status(_request(cursor="zz"), target)

    assert info.value.code == exit_code
    assert info.value.error == code


async def test_cluster_status_maps_an_unreachable_node_to_exit_3(
    tmp_path: Path,
) -> None:
    tourconf, _ = await make_tourconf(tmp_path)
    target = Target(f"127.0.0.1:{_free_port()}", tourconf, None)

    with pytest.raises(CliError) as info:
        await cluster_status(_request(), target)

    assert info.value.code == EXIT_UNREACHABLE


def _wrong_service(monkeypatch: pytest.MonkeyPatch) -> None:
    @asynccontextmanager
    async def fake_client(*_args: Any, **_kwargs: Any):
        yield object()

    monkeypatch.setattr(cluster_module, "client", fake_client)


async def test_cluster_status_rejects_a_service_of_the_wrong_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _wrong_service(monkeypatch)

    with pytest.raises(CliError) as info:
        await cluster_status(_request(), TARGET)

    assert info.value.error == "unknown_service"


async def test_watch_rejects_a_cursor_and_a_short_interval() -> None:
    with pytest.raises(CliError, match="--watch shows the first page only.") as info:
        await watch_cluster_status(_request(cursor="C"), TARGET, interval=2)
    assert info.value.code == EXIT_INVALID

    with pytest.raises(CliError, match="--interval must be at least 1") as info:
        await watch_cluster_status(_request(), TARGET, interval=0.5)
    assert info.value.code == EXIT_INVALID


async def test_watch_rejects_a_service_of_the_wrong_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _wrong_service(monkeypatch)

    with pytest.raises(CliError) as info:
        await watch_cluster_status(_request(), TARGET, interval=1)

    assert info.value.error == "unknown_service"


async def test_watch_maps_an_unreachable_node_to_exit_3(tmp_path: Path) -> None:
    tourconf, _ = await make_tourconf(tmp_path)
    target = Target(f"127.0.0.1:{_free_port()}", tourconf, None)

    with pytest.raises(CliError) as info:
        await watch_cluster_status(_request(), target, interval=1)

    assert info.value.code == EXIT_UNREACHABLE


@pytest.mark.parametrize("as_json", [False, True])
async def test_watch_refreshes_until_cancelled(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], as_json: bool
) -> None:
    page = make_page(make_view("node-2", status="DEAD"))
    seen: list[Any] = []
    handler = reply("node.members.accept", page.to_document(), seen)

    async with inspect_server(tmp_path, {"node.members": handler}) as (
        address,
        tourconf,
    ):
        target = Target(address=address, tourconf=tourconf, context_name=None)
        task = asyncio.create_task(
            watch_cluster_status(_request(), target, interval=1, as_json=as_json)
        )
        for _ in range(200):
            if seen:
                break
            await asyncio.sleep(0.05)
        await asyncio.sleep(0.1)
        task.cancel()
        assert await task is None

    out = capsys.readouterr().out
    assert len(seen) >= 1
    if as_json:
        assert json.loads(out.splitlines()[0])["items"][0]["node_id"] == "node-2"
    else:
        assert out.startswith("Every 1s: tourctl cluster status   ")
        assert "ANOMALIES (1)" in out


def test_watch_redraw_clears_a_terminal(monkeypatch, capsys) -> None:
    monkeypatch.setattr(cluster_module.sys.stdout, "isatty", lambda: True)

    cluster_module._redraw("body", 2)

    out = capsys.readouterr().out
    assert out.startswith("\x1b[H\x1b[2JEvery 2s: tourctl cluster status")
    assert out.endswith("\n\nbody\n\n")


def test_cli_forwards_cluster_status_options(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    captured: dict[str, Any] = {}

    async def fake_cluster_status(request, target, *, as_json=False) -> str:
        captured.update(request=request, target=target, as_json=as_json)
        return "rendered"

    monkeypatch.setattr(
        "tourctl.bootstrap.usecases.cluster.cluster_status", fake_cluster_status
    )

    _run_ok(
        [
            "cluster",
            "status",
            "-a",
            "h:1",
            "--all",
            "--phase",
            "joining",
            "--phase",
            "ready",
            "--status",
            "dead",
            "--prefix",
            "node-",
            "--limit",
            "7",
            "--cursor",
            "C",
            "--json",
            "--tourconf",
            "conf.toml",
            "--context",
            "prod",
        ]
    )

    assert capsys.readouterr().out == "rendered\n"
    assert captured == {
        "request": MembersRequest(
            phases=("JOINING", "READY"),
            statuses=("DEAD",),
            prefix="node-",
            list_all=True,
            limit=7,
            cursor="C",
        ),
        "target": Target(address="h:1", tourconf="conf.toml", context_name="prod"),
        "as_json": True,
    }


def test_cli_forwards_cluster_member_options(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    captured: dict[str, Any] = {}

    async def fake_cluster_member(node_id, target, *, as_json=False) -> str:
        captured.update(node_id=node_id, target=target, as_json=as_json)
        return "member report"

    monkeypatch.setattr(
        "tourctl.bootstrap.usecases.cluster.cluster_member", fake_cluster_member
    )

    _run_ok(
        [
            "cluster",
            "member",
            "node-2",
            "-a",
            "h:1",
            "--json",
            "--tourconf",
            "conf.toml",
            "--context",
            "prod",
        ]
    )

    assert capsys.readouterr().out == "member report\n"
    assert captured == {
        "node_id": "node-2",
        "target": Target(address="h:1", tourconf="conf.toml", context_name="prod"),
        "as_json": True,
    }


def test_cli_cluster_status_watch_uses_the_watch_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    async def fake_watch(request, target, *, interval, as_json=False) -> None:
        captured.update(request=request, interval=interval, as_json=as_json)

    monkeypatch.setattr(
        "tourctl.bootstrap.usecases.cluster.watch_cluster_status", fake_watch
    )

    _run_ok(["cluster", "status", "--watch", "--interval", "5"])

    assert captured == {"request": MembersRequest(), "interval": 5.0, "as_json": False}


def test_cli_cluster_status_rejects_an_invalid_limit(capsys) -> None:
    with pytest.raises(SystemExit) as info:
        main(["cluster", "status", "--limit", "0", "--json"])

    assert info.value.code == EXIT_INVALID
    error = json.loads(capsys.readouterr().err)["error"]
    assert error["code"] == "invalid_option"


async def test_rejected_error_with_another_code_keeps_the_failure_exit() -> None:
    exc = RequestRejectedError("node.members", {"code": "busy", "message": "later"})

    error = cluster_module._cluster_error(exc, None)

    assert error.code == EXIT_FAILED
    assert error.error == "busy"
