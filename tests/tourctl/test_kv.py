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

import io
import json
import ssl
from collections.abc import AsyncIterator, Callable
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from tests.helpers import generate_ca, start_envelope_server
from tourclient.exception import RequestRejectedError
from tourctl.bootstrap.main import app, main
from tourctl.core.error import CliError
from tourctl.core.kv import KeyArgs, kv_delete, kv_get, kv_put, kv_repair
from tourillon.core.config import ClientConfigGenerator
from tourlib.adapter import CryptographyMtlsAdapter, MsgPackSerializer, TomlPersistence
from tourlib.envelope import Envelope, replay_envelope
from tourlib.models.kv import ReadOutcome, WriteOutcome
from tourlib.models.ts import HLCTimestamp
from tourlib.models.version import Address, Meta, Version


async def _build_tourconf(
    tmp_path: Path,
    *,
    client_address: str,
    ca_cert_path: Path,
    ca_key_path: Path,
) -> Path:
    generator = ClientConfigGenerator(
        CryptographyMtlsAdapter(),
        TomlPersistence(),
    )
    result = await generator.generate(
        output_file=tmp_path / "tourconf.toml",
        common_name="tourctl-kv",
        ca_certificate_path=ca_cert_path,
        ca_private_key_path=ca_key_path,
        client_address=client_address,
        validity_days=30,
        key_size=2048,
        overwrite=False,
    )
    return result.config_path


def _build_server_ssl_from_ca(
    tmp_path: Path,
    ca_cert_path: Path,
    ca_key_path: Path,
) -> ssl.SSLContext:
    crypto = CryptographyMtlsAdapter()
    server_cert_pem, server_key_pem = crypto.generate_cert(
        common_name="tourillon-server",
        ca_certificate_pem=ca_cert_path.read_bytes(),
        ca_private_key_pem=ca_key_path.read_bytes(),
        validity_days=30,
        key_size=2048,
        client_only=False,
    )

    server_cert_path = tmp_path / "server-cert.pem"
    server_key_path = tmp_path / "server-key.pem"
    server_cert_path.write_bytes(server_cert_pem)
    server_key_path.write_bytes(server_key_pem)

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(str(server_cert_path), str(server_key_path))
    context.load_verify_locations(cafile=str(ca_cert_path))
    context.verify_mode = ssl.CERT_REQUIRED
    return context


_WIRE = MsgPackSerializer()


def _wall(text: str) -> int:
    return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp() * 1000)


SPEC_TS = HLCTimestamp(
    wall_ms=_wall("2026-11-04T20:07:09.246Z"), counter=0, node_id="node-1"
)
SPEC_VERSION = "2026-11-04T20:07:09.246Z c=0 node-1"


def _version(value: Any, ts: HLCTimestamp = SPEC_TS) -> Version:
    return Version(
        address=Address(key=b"k", namespace=b"n"),
        data=_WIRE.encode(value),
        meta=Meta(ts=ts, quorum_write=1),
    )


def _write(
    value: Any = "hello", acknowledged: int = 1, required: int = 1, replicas: int = 3
) -> WriteOutcome:
    return WriteOutcome(
        version=_version(value),
        acknowledged=acknowledged,
        required=required,
        replicas=replicas,
    )


def _read(
    value: Any = "hello",
    answered: int = 1,
    required: int = 1,
    replicas: int = 3,
    tombstone: HLCTimestamp | None = None,
    absent: bool = False,
) -> ReadOutcome:
    return ReadOutcome(
        version=None if absent else _version(value),
        tombstone=tombstone,
        answered=answered,
        required=required,
        replicas=replicas,
    )


def _rejection(kind: str, **details: Any) -> RequestRejectedError:
    return RequestRejectedError(kind, details)


class FakeKv:
    """Stands in for the kv client service; records every call."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.result: Any = None
        self.error: Exception | None = None

    async def __call__(
        self, method: str, tourconf: Any, context_name: Any, **kwargs: Any
    ) -> Any:
        self.calls.append((method, kwargs))
        if self.error is not None:
            raise self.error
        return self.result


@pytest.fixture
def fake_kv(monkeypatch: pytest.MonkeyPatch) -> FakeKv:
    fake = FakeKv()
    monkeypatch.setattr("tourctl.core.kv._call", fake)
    return fake


@pytest.fixture
def fixed_since(monkeypatch: pytest.MonkeyPatch) -> Callable[[str], None]:
    def install(text: str) -> None:
        monkeypatch.setattr("tourctl.core.kv.format_since", lambda wall_ms: text)

    return install


async def _fails(call: Any) -> CliError:
    with pytest.raises(CliError) as exc_info:
        await call
    return exc_info.value


# ---------------------------------------------------------------- integration


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_kv_commands_round_trip_through_a_real_mtls_server(
    tmp_path: Path,
) -> None:
    ca_cert_path, ca_key_path = generate_ca(tmp_path)
    server_ssl = _build_server_ssl_from_ca(tmp_path, ca_cert_path, ca_key_path)
    serializer = MsgPackSerializer()
    store: dict[tuple[Any, Any], Version] = {}
    tombstones: dict[tuple[Any, Any], HLCTimestamp] = {}

    async def put_handler(request: Envelope) -> AsyncIterator[Envelope]:
        document = serializer.decode(request.payload)
        key = (document["namespace"], document["key"])
        store[key] = Version(
            address=Address(key=document["key"], namespace=document["namespace"]),
            data=document["value"],
            meta=Meta(ts=SPEC_TS, quorum_write=document["quorum_write"]),
        )
        outcome = WriteOutcome(store[key], acknowledged=1, required=1, replicas=1)
        yield replay_envelope(
            kind="kv.put.accept",
            request=request,
            payload=serializer.encode(outcome.to_dict()),
        )

    async def get_handler(request: Envelope) -> AsyncIterator[Envelope]:
        document = serializer.decode(request.payload)
        key = (document["namespace"], document["key"])
        outcome = ReadOutcome(
            version=store.get(key),
            tombstone=tombstones.get(key),
            answered=1,
            required=document["quorum_read"],
            replicas=1,
        )
        yield replay_envelope(
            kind="kv.get.accept",
            request=request,
            payload=serializer.encode(outcome.to_dict()),
        )

    async def delete_handler(request: Envelope) -> AsyncIterator[Envelope]:
        document = serializer.decode(request.payload)
        key = (document["namespace"], document["key"])
        version = store.pop(key)
        tombstones[key] = SPEC_TS
        outcome = WriteOutcome(version, acknowledged=1, required=1, replicas=1)
        yield replay_envelope(
            kind="kv.delete.accept",
            request=request,
            payload=serializer.encode(outcome.to_dict()),
        )

    server = await start_envelope_server(
        {"kv.put": put_handler, "kv.get": get_handler, "kv.delete": delete_handler},
        server_ssl,
    )
    try:
        host, port = server.endpoint
        tourconf = await _build_tourconf(
            tmp_path,
            client_address=f"{host}:{port}",
            ca_cert_path=ca_cert_path,
            ca_key_path=ca_key_path,
        )
        payload_file = tmp_path / "blob.bin"
        payload_file.write_bytes(b"\x00\x01\xff")
        typed = {"key": "7", "key_type": "int", "tourconf": tourconf}

        put_output = await kv_put(value=f"@{payload_file}", as_json=True, **typed)
        get_output = await kv_get(as_json=True, **typed)
        raw_output = await kv_get(raw=True, **typed)
        delete_output = await kv_delete(as_json=True, **typed)
        error = await _fails(kv_get(**typed))
    finally:
        await server.stop()

    put_json = json.loads(put_output)
    assert put_json["address"] == {"key": 7, "namespace": "default"}
    assert put_json["value"] == {"size": 3, "type": "bytes"}
    assert json.loads(get_output)["value"] == {
        "base64": "AAH/",
        "size": 3,
        "type": "bytes",
    }
    assert raw_output == b"\x00\x01\xff"
    assert json.loads(delete_output)["version"]["node_id"] == "node-1"
    assert error.code == 4
    assert error.error == "not_found"
    assert error.message.startswith("get default/7: deleted ")


# ------------------------------------------------------------------------ put


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_put_renders_the_stored_version(fake_kv: FakeKv) -> None:
    fake_kv.result = _write("hello", acknowledged=2, required=2, replicas=3)

    output = await kv_put(key="souls", value="hello", namespace="toto", quorum_write=2)

    assert output == (
        "Stored toto/souls.\n"
        f"Version: {SPEC_VERSION}\n"
        "Value:   hello (str, 5 B)\n"
        "Quorum:  acknowledged by 2 of 3 replicas (2 required)"
    )
    assert fake_kv.calls == [
        (
            "put",
            {"namespace": "toto", "key": "souls", "value": "hello", "quorum_write": 2},
        )
    ]


@pytest.mark.asyncio
@pytest.mark.tourctl
@pytest.mark.parametrize(
    ("value", "value_type", "expected"),
    [
        ("hello", None, "hello"),
        ("42", None, 42),
        ("42", "str", "42"),
        ("1.5", None, 1.5),
        ("1e3", None, 1000.0),
        ("true", None, True),
        ("yes", None, True),
        ("1", None, True),
        ("1", "int", 1),
        ('{"a": 1}', None, {"a": 1}),
        ("[1, 2]", None, [1, 2]),
        ('{"a": 1', None, '{"a": 1'),
        ("aGVsbG8=", "bytes", b"hello"),
        ("@@hello", None, "@hello"),
    ],
)
async def test_put_infers_or_applies_the_value_type(
    fake_kv: FakeKv, value: str, value_type: Any, expected: Any
) -> None:
    fake_kv.result = _write()

    await kv_put(key="k", value=value, namespace="n", value_type=value_type)

    sent = fake_kv.calls[0][1]["value"]
    assert sent == expected
    assert type(sent) is type(expected)


@pytest.mark.asyncio
@pytest.mark.tourctl
@pytest.mark.parametrize(
    ("value", "value_type", "message"),
    [
        ('{"a": 1', "json", "Invalid value: expected JSON (line 1 column "),
        ("abc", "int", "Invalid value: expected int, got 'abc'"),
        ("abc", "float", "Invalid value: expected float, got 'abc'"),
        ("maybe", "bool", "Invalid value: expected bool, got 'maybe'"),
        ("%%%", "bytes", "Invalid value: expected base64, got '%%%'"),
        ("@absent.txt", None, "Invalid value: file not found: absent.txt"),
    ],
)
async def test_put_rejects_a_value_that_does_not_match_its_type(
    fake_kv: FakeKv, value: str, value_type: Any, message: str
) -> None:
    error = await _fails(
        kv_put(key="k", value=value, namespace="n", value_type=value_type)
    )

    assert error.code == 2
    assert error.error == "invalid_input"
    assert error.message.startswith(message)
    assert fake_kv.calls == []


@pytest.mark.asyncio
@pytest.mark.tourctl
@pytest.mark.parametrize(
    ("value_type", "expected"),
    [
        (None, b'{"port": 7001}'),
        ("auto", b'{"port": 7001}'),
        ("str", '{"port": 7001}'),
        ("json", {"port": 7001}),
    ],
)
async def test_put_reads_a_file_as_bytes_unless_a_type_is_given(
    fake_kv: FakeKv, tmp_path: Path, value_type: Any, expected: Any
) -> None:
    source = tmp_path / "conf.json"
    source.write_bytes(b'{"port": 7001}')
    fake_kv.result = _write()

    await kv_put(key="k", value=f"@{source}", namespace="n", value_type=value_type)

    assert fake_kv.calls[0][1]["value"] == expected


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_put_refuses_a_file_that_is_not_utf8_for_a_text_type(
    fake_kv: FakeKv, tmp_path: Path
) -> None:
    source = tmp_path / "blob.bin"
    source.write_bytes(b"\xff\xfe")

    error = await _fails(
        kv_put(key="k", value=f"@{source}", namespace="n", value_type="str")
    )

    assert error.message == "Invalid value: content is not valid UTF-8"


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_put_reports_an_unreadable_file(fake_kv: FakeKv, tmp_path: Path) -> None:
    error = await _fails(kv_put(key="k", value=f"@{tmp_path}", namespace="n"))

    assert error.code == 2
    assert error.message.startswith(f"Invalid value: cannot read {tmp_path}")


@pytest.mark.asyncio
@pytest.mark.tourctl
@pytest.mark.parametrize(
    ("value_type", "expected"),
    [(None, b'{"a": 1}'), ("json", {"a": 1})],
)
async def test_put_reads_stdin_for_a_dash(
    fake_kv: FakeKv, value_type: Any, expected: Any
) -> None:
    fake_kv.result = _write()

    await kv_put(
        key="k",
        value="-",
        namespace="n",
        value_type=value_type,
        stdin=io.BytesIO(b'{"a": 1}'),
    )

    assert fake_kv.calls[0][1]["value"] == expected


@pytest.mark.asyncio
@pytest.mark.tourctl
@pytest.mark.parametrize(
    ("value", "line"),
    [
        (42, "Value:   42 (int)"),
        (True, "Value:   true (bool)"),
        ({"a": 1}, "Value:   json, 8 B"),
        ("x" * 100, "Value:   str, 100 B"),
        ("two\nlines", "Value:   str, 9 B"),
        (b"hello", "Value:   bytes, 5 B"),
    ],
)
async def test_put_summarises_values_that_do_not_fit_inline(
    fake_kv: FakeKv, value: Any, line: str
) -> None:
    fake_kv.result = _write(value)

    output = await kv_put(key="k", value="ignored", namespace="n")

    assert output.splitlines()[2] == line


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_put_uses_the_singular_for_one_replica(fake_kv: FakeKv) -> None:
    fake_kv.result = _write(acknowledged=1, required=1, replicas=1)

    output = await kv_put(key="k", value="v", namespace="n")

    assert output.endswith("Quorum:  acknowledged by 1 of 1 replica (1 required)")


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_put_renders_json(fake_kv: FakeKv) -> None:
    fake_kv.result = _write("hello", acknowledged=2, required=2, replicas=3)

    output = await kv_put(
        key="7", value="hello", namespace="toto", key_type="int", as_json=True
    )

    assert json.loads(output) == {
        "address": {"key": 7, "namespace": "toto"},
        "quorum": {"acknowledged": 2, "replicas": 3, "required": 2},
        "value": {"size": 5, "type": "str"},
        "version": {
            "counter": 0,
            "node_id": "node-1",
            "wall": "2026-11-04T20:07:09.246Z",
            "wall_ms": SPEC_TS.wall_ms,
        },
    }


@pytest.mark.asyncio
@pytest.mark.tourctl
@pytest.mark.parametrize("quorum", [0, 256])
async def test_put_refuses_a_quorum_out_of_range(fake_kv: FakeKv, quorum: int) -> None:
    error = await _fails(
        kv_put(key="souls", value="v", namespace="toto", quorum_write=quorum)
    )

    assert error.code == 2
    assert error.error == "invalid_quorum"
    assert error.message == f"put toto/souls: quorum {quorum} is out of range [1, 255]."
    assert fake_kv.calls == []


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_put_explains_a_quorum_that_can_never_be_met(fake_kv: FakeKv) -> None:
    fake_kv.error = _rejection(
        "kv.put", code="quorum_unreachable", quorum=4, replication_factor=3
    )

    error = await _fails(
        kv_put(key="souls", value="v", namespace="toto", quorum_write=4)
    )

    assert error.code == 1
    assert error.error == "quorum_unreachable"
    assert error.message == (
        "put toto/souls: quorum 4 can never be met, replication factor is 3."
    )


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_put_offers_a_repair_when_the_write_quorum_is_missed(
    fake_kv: FakeKv,
) -> None:
    fake_kv.error = _rejection(
        "kv.put",
        code="quorum_not_met",
        operation="write",
        received=1,
        required=2,
        token="TOK",
    )

    error = await _fails(
        kv_put(key="souls", value="v", namespace="toto", quorum_write=2)
    )

    assert error.code == 1
    assert error.error == "quorum_not_met"
    assert error.details["token"] == "TOK"
    assert error.message == (
        "put toto/souls: quorum not met, 1 of 2 required replicas acknowledged.\n"
        "The version is stored on 1 replica and is not lost; anti-entropy spreads it.\n"
        "Finish the put with:\n"
        "  tourctl repair TOK -w 2\n"
        "See where it landed with:\n"
        "  tourctl explain souls -n toto"
    )
    assert json.loads(error.render_json())["error"]["message"] == (
        "put toto/souls: quorum not met, 1 of 2 required replicas acknowledged."
    )


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_put_without_any_acknowledgement_offers_no_repair(
    fake_kv: FakeKv,
) -> None:
    fake_kv.error = _rejection(
        "kv.put", code="quorum_not_met", operation="write", received=0, required=2
    )

    error = await _fails(
        kv_put(key="souls", value="v", namespace="toto", quorum_write=2)
    )

    assert error.message == (
        "put toto/souls: quorum not met, 0 of 2 required replicas acknowledged.\n"
        "See where it landed with:\n"
        "  tourctl explain souls -n toto"
    )


@pytest.mark.asyncio
@pytest.mark.tourctl
@pytest.mark.parametrize(
    ("details", "message", "code"),
    [
        (
            {"code": "invalid_quorum", "quorum": 300},
            "put toto/souls: quorum 300 is out of range [1, 255].",
            2,
        ),
        (
            {"code": "node_draining", "operation": "write", "node_id": "node-2"},
            "put toto/souls: node-2 is draining and accepts no new writes.\n"
            "Use another node's client endpoint.",
            1,
        ),
        (
            {"code": "node_draining", "operation": "write"},
            "put toto/souls: the node is draining and accepts no new writes.\n"
            "Use another node's client endpoint.",
            1,
        ),
        (
            {"code": "no_replica_available"},
            "put toto/souls: no replica can take this key; every replica is "
            "unavailable.\nSee: tourctl explain souls -n toto",
            1,
        ),
        (
            {"code": "boom", "message": "it broke."},
            "put toto/souls: it broke.",
            1,
        ),
    ],
)
async def test_put_explains_each_rejection(
    fake_kv: FakeKv, details: dict[str, Any], message: str, code: int
) -> None:
    fake_kv.error = _rejection("kv.put", **details)

    error = await _fails(kv_put(key="souls", value="v", namespace="toto"))

    assert error.message == message
    assert error.code == code
    assert error.error == details["code"]


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_put_names_an_uncoded_rejection(fake_kv: FakeKv) -> None:
    fake_kv.error = _rejection("kv.put")

    error = await _fails(kv_put(key="souls", value="v", namespace="toto"))

    assert error.error == "rejected"
    assert error.message == "put toto/souls: kv.put rejected."


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_put_reports_an_unreachable_node(fake_kv: FakeKv) -> None:
    fake_kv.error = ConnectionRefusedError("refused")

    error = await _fails(kv_put(key="souls", value="v", namespace="toto"))

    assert error.code == 3
    assert "refused the KV connection" in error.message


# --------------------------------------------------------------------- delete


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_delete_renders_the_tombstone(fake_kv: FakeKv) -> None:
    fake_kv.result = _write(b"", acknowledged=3, required=1, replicas=3)

    output = await kv_delete(key="souls", namespace="toto")

    assert output == (
        "Deleted toto/souls.\n"
        f"Version: {SPEC_VERSION} (tombstone)\n"
        "Quorum:  acknowledged by 3 of 3 replicas (1 required)\n"
        "\n"
        "Messages:\n"
        "  The tombstone hides older versions; garbage collection removes them later."
    )
    assert fake_kv.calls == [
        ("delete", {"namespace": "toto", "key": "souls", "quorum_write": 1})
    ]


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_delete_renders_json(fake_kv: FakeKv) -> None:
    fake_kv.result = _write(b"", acknowledged=1, required=1, replicas=1)

    output = await kv_delete(key="souls", namespace="toto", as_json=True)

    document = json.loads(output)
    assert set(document) == {"address", "quorum", "version"}
    assert document["quorum"] == {"acknowledged": 1, "replicas": 1, "required": 1}


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_delete_offers_a_repair_when_the_quorum_is_missed(
    fake_kv: FakeKv,
) -> None:
    fake_kv.error = _rejection(
        "kv.delete",
        code="quorum_not_met",
        operation="write",
        received=2,
        required=3,
        token="TOK",
    )

    error = await _fails(kv_delete(key="souls", namespace="toto", quorum_write=3))

    assert "Finish the delete with:\n  tourctl repair TOK -w 3" in error.message
    assert "stored on 2 replicas" in error.message


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_delete_refuses_a_quorum_out_of_range(fake_kv: FakeKv) -> None:
    error = await _fails(kv_delete(key="k", namespace="n", quorum_write=0))

    assert error.code == 2
    assert fake_kv.calls == []


# ------------------------------------------------------------------------ get


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_get_renders_a_short_value_inline(
    fake_kv: FakeKv, fixed_since: Callable[[str], None]
) -> None:
    fixed_since("4m ago")
    fake_kv.result = _read("hello", answered=1, required=1, replicas=3)

    output = await kv_get(key="souls", namespace="toto")

    assert output == (
        "Key:     toto/souls\n"
        "Value:   hello\n"
        "Type:    str, 5 B\n"
        f"Version: {SPEC_VERSION} (4m ago)\n"
        "Read:    1 of 3 replicas answered (1 required)"
    )
    assert fake_kv.calls == [
        ("get", {"namespace": "toto", "key": "souls", "quorum_read": 1})
    ]


@pytest.mark.asyncio
@pytest.mark.tourctl
@pytest.mark.parametrize(
    ("value", "line"),
    [(42, "Value:   42"), (1.5, "Value:   1.5"), (False, "Value:   false")],
)
async def test_get_renders_scalars_inline(
    fake_kv: FakeKv, value: Any, line: str
) -> None:
    fake_kv.result = _read(value)

    output = await kv_get(key="k", namespace="n")

    assert output.splitlines()[1] == line


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_get_renders_json_as_a_block_at_the_end(
    fake_kv: FakeKv, fixed_since: Callable[[str], None]
) -> None:
    fixed_since("2m ago")
    value = {"port": 7001, "tags": ["a", "b"]}
    fake_kv.result = _read(value)

    output = await kv_get(key="config", namespace="default")

    size = len(json.dumps(value))
    assert output == (
        "Key:     default/config\n"
        f"Type:    json, {size} B\n"
        f"Version: {SPEC_VERSION} (2m ago)\n"
        "Read:    1 of 3 replicas answered (1 required)\n"
        "Value:\n"
        "  {\n"
        '    "port": 7001,\n'
        '    "tags": ["a", "b"]\n'
        "  }"
    )


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_get_renders_multiline_text_as_a_block(fake_kv: FakeKv) -> None:
    fake_kv.result = _read("first\nsecond")

    output = await kv_get(key="k", namespace="n")

    assert output.endswith("Value:\n  first\n  second")


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_get_truncates_text_over_four_kib(fake_kv: FakeKv) -> None:
    fake_kv.result = _read("a" * 5000)

    output = await kv_get(key="notes", namespace="n")

    block, note = output.split("\n\nMessages:\n")
    assert block.endswith("\n  " + "a" * 4096)
    assert note == (
        "  Value truncated (4.9 KiB). Show all with: tourctl get notes -n n --raw"
    )


@pytest.mark.asyncio
@pytest.mark.tourctl
@pytest.mark.parametrize(
    ("key", "file_name"),
    [("avatar", "avatar.bin"), ("my key/1", "my_key_1.bin"), ("...", "value.bin")],
)
async def test_get_hides_binary_values_and_suggests_saving_them(
    fake_kv: FakeKv, fixed_since: Callable[[str], None], key: str, file_name: str
) -> None:
    fixed_since("1m ago")
    fake_kv.result = _read(b"\x00" * 2458)

    output = await kv_get(key=key, namespace="default")

    lines = output.splitlines()
    assert lines[1] == "Type:    bytes, 2.4 KiB"
    assert lines[4] == "Value:   not shown (binary)"
    assert lines[-2:] == [
        "Messages:",
        f"  Save it with: tourctl get {KeyArgs(key, 'default').hint()} "
        f"--output {file_name}",
    ]


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_get_renders_json_with_the_value(fake_kv: FakeKv) -> None:
    fake_kv.result = _read("hello", answered=2, required=1, replicas=3)

    output = await kv_get(key="souls", namespace="toto", as_json=True)

    assert json.loads(output) == {
        "address": {"key": "souls", "namespace": "toto"},
        "read": {"answered": 2, "replicas": 3, "required": 1},
        "value": {"data": "hello", "size": 5, "type": "str"},
        "version": {
            "counter": 0,
            "node_id": "node-1",
            "wall": "2026-11-04T20:07:09.246Z",
            "wall_ms": SPEC_TS.wall_ms,
        },
    }


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_get_renders_bytes_as_base64_in_json(fake_kv: FakeKv) -> None:
    fake_kv.result = _read(b"hello")

    output = await kv_get(key="k", namespace="n", as_json=True)

    assert json.loads(output)["value"] == {
        "base64": "aGVsbG8=",
        "size": 5,
        "type": "bytes",
    }


@pytest.mark.asyncio
@pytest.mark.tourctl
@pytest.mark.parametrize(
    ("value", "raw"),
    [
        ("hello", b"hello\n"),
        (42, b"42\n"),
        (True, b"true\n"),
        ({"a": [1, 2]}, b'{"a":[1,2]}\n'),
        (b"\x00\x01", b"\x00\x01"),
    ],
)
async def test_get_raw_prints_only_the_value(
    fake_kv: FakeKv, value: Any, raw: bytes
) -> None:
    fake_kv.result = _read(value)

    assert await kv_get(key="k", namespace="n", raw=True) == raw


@pytest.mark.asyncio
@pytest.mark.tourctl
@pytest.mark.parametrize(
    ("value", "content"),
    [
        ("héllo", "héllo".encode()),
        ({"port": 7001}, b'{\n  "port": 7001\n}'),
        (b"\x00\xff", b"\x00\xff"),
    ],
)
async def test_get_output_writes_the_value_to_a_file(
    fake_kv: FakeKv, tmp_path: Path, value: Any, content: bytes
) -> None:
    fake_kv.result = _read(value)
    target = tmp_path / "value.out"

    output = await kv_get(key="souls", namespace="toto", output=str(target))

    assert target.read_bytes() == content
    assert output == f"Saved toto/souls to {target} ({len(content)} B)."


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_get_json_with_output_leaves_the_value_out_of_stdout(
    fake_kv: FakeKv, tmp_path: Path
) -> None:
    fake_kv.result = _read("hello")
    target = tmp_path / "value.out"

    output = await kv_get(key="k", namespace="n", as_json=True, output=str(target))

    assert json.loads(output)["value"] == {"size": 5, "type": "str"}
    assert target.read_bytes() == b"hello"


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_get_output_reports_an_unwritable_path(
    fake_kv: FakeKv, tmp_path: Path
) -> None:
    fake_kv.result = _read("hello")

    error = await _fails(kv_get(key="k", namespace="n", output=str(tmp_path)))

    assert error.code == 2
    assert error.error == "invalid_output"
    assert error.message.startswith(f"get n/k: cannot write {tmp_path}: ")


@pytest.mark.asyncio
@pytest.mark.tourctl
@pytest.mark.parametrize(
    ("options", "message"),
    [
        ({"as_json": True, "raw": True}, "--json and --raw cannot be combined."),
        ({"raw": True, "output": "f"}, "--raw and --output cannot be combined."),
    ],
)
async def test_get_refuses_conflicting_output_modes(
    fake_kv: FakeKv, options: dict[str, Any], message: str
) -> None:
    error = await _fails(kv_get(key="k", namespace="n", **options))

    assert error.code == 2
    assert error.message == message
    assert fake_kv.calls == []


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_get_reports_an_absent_key_as_not_found(fake_kv: FakeKv) -> None:
    fake_kv.result = _read(absent=True, answered=1, required=1, replicas=3)

    error = await _fails(kv_get(key="nothing", namespace="default"))

    assert error.code == 4
    assert error.error == "not_found"
    assert error.render_text() == (
        "Error: get default/nothing: not found (1 of 3 replicas answered)."
    )
    assert error.details == {
        "answered": 1,
        "replicas": 3,
        "required": 1,
        "tombstone": None,
    }


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_get_reports_a_deleted_key_with_its_tombstone(
    fake_kv: FakeKv, fixed_since: Callable[[str], None]
) -> None:
    fixed_since("3m ago")
    tombstone = HLCTimestamp(
        wall_ms=_wall("2026-11-04T20:12:00.004Z"), counter=0, node_id="node-1"
    )
    fake_kv.result = _read(absent=True, tombstone=tombstone)

    error = await _fails(kv_get(key="souls", namespace="toto"))

    assert error.code == 4
    assert error.message == (
        "get toto/souls: deleted 3m ago (2026-11-04T20:12:00.004Z c=0 node-1)."
    )
    assert error.details["tombstone"]["node_id"] == "node-1"


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_get_points_to_explain_when_the_read_quorum_is_missed(
    fake_kv: FakeKv,
) -> None:
    fake_kv.error = _rejection(
        "kv.get", code="quorum_not_met", operation="read", received=1, required=2
    )

    error = await _fails(kv_get(key="souls", namespace="toto", quorum_read=2))

    assert error.code == 1
    assert error.message == (
        "get toto/souls: quorum not met, 1 of 2 required replicas answered.\n"
        "See which replicas are unavailable with:\n"
        "  tourctl explain souls -n toto"
    )


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_get_reports_a_draining_node(fake_kv: FakeKv) -> None:
    fake_kv.error = _rejection(
        "kv.get", code="node_draining", operation="read", node_id="node-1"
    )

    error = await _fails(kv_get(key="k", namespace="n"))

    assert error.message.startswith(
        "get n/k: node-1 is draining and accepts no new reads."
    )


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_get_refuses_a_quorum_out_of_range(fake_kv: FakeKv) -> None:
    error = await _fails(kv_get(key="k", namespace="n", quorum_read=0))

    assert error.message == "get n/k: quorum 0 is out of range [1, 255]."


# ---------------------------------------------------------------- addressing


@pytest.mark.asyncio
@pytest.mark.tourctl
@pytest.mark.parametrize(
    ("options", "message"),
    [
        ({"key": "abc", "key_type": "int"}, "Invalid key: expected int, got 'abc'"),
        (
            {"key": "k", "namespace": "x", "namespace_type": "int"},
            "Invalid namespace: expected int, got 'x'",
        ),
    ],
)
async def test_an_address_that_does_not_match_its_type_is_refused(
    fake_kv: FakeKv, options: dict[str, Any], message: str
) -> None:
    options.setdefault("namespace", "n")

    error = await _fails(kv_get(**options))

    assert error.code == 2
    assert error.message == message
    assert fake_kv.calls == []


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_a_missing_tourconf_is_reported_as_invalid_config(
    fake_kv: FakeKv, tmp_path: Path
) -> None:
    error = await _fails(kv_get(key="k", tourconf=tmp_path / "absent.toml"))

    assert error.code == 2
    assert error.error == "invalid_config"


@pytest.mark.tourctl
@pytest.mark.parametrize(
    ("args", "hint"),
    [
        (KeyArgs("souls"), "souls"),
        (KeyArgs("souls", "toto"), "souls -n toto"),
        (
            KeyArgs("my key", "ns 1", "int", "int"),
            "'my key' -n 'ns 1' --namespace-type int --key-type int",
        ),
    ],
)
def test_key_hint_names_the_key_as_typed(args: KeyArgs, hint: str) -> None:
    assert args.hint() == hint


# --------------------------------------------------------------------- repair


@pytest.mark.asyncio
@pytest.mark.tourctl
@pytest.mark.parametrize(
    ("result", "quorum", "text"),
    [
        (
            {"issue": "repaired", "replicas": 3},
            2,
            "Repaired: the version now has 3 copies (2 required).",
        ),
        (
            {"issue": "repaired", "replicas": 1},
            1,
            "Repaired: the version now has 1 copy (1 required).",
        ),
        (
            {"issue": "already_durable", "replicas": 2},
            2,
            "Already durable: 2 replicas hold the version; nothing was copied.",
        ),
        (
            {"issue": "already_durable", "replicas": 1},
            1,
            "Already durable: 1 replica holds the version; nothing was copied.",
        ),
    ],
)
async def test_repair_renders_the_outcome(
    fake_kv: FakeKv, result: dict[str, Any], quorum: int, text: str
) -> None:
    fake_kv.result = result

    output = await kv_repair(token="  tok  ", quorum_write=quorum)

    assert output == text
    assert fake_kv.calls == [("repair", {"token": "tok", "quorum_write": quorum})]


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_repair_renders_json(fake_kv: FakeKv) -> None:
    fake_kv.result = {"issue": "already_durable", "replicas": 1}

    output = await kv_repair(token="tok", quorum_write=2, as_json=True)

    assert json.loads(output) == {
        "issue": "already_durable",
        "replicas": 1,
        "required": 2,
        "token": "tok",
    }


@pytest.mark.asyncio
@pytest.mark.tourctl
@pytest.mark.parametrize(
    ("options", "message"),
    [
        ({"token": "   "}, "repair: token must not be empty."),
        (
            {"token": "tok", "quorum_write": 0},
            "repair: quorum 0 is out of range [1, 255].",
        ),
    ],
)
async def test_repair_refuses_invalid_input(
    fake_kv: FakeKv, options: dict[str, Any], message: str
) -> None:
    error = await _fails(kv_repair(**options))

    assert error.code == 2
    assert error.message == message
    assert fake_kv.calls == []


def _newer(wall: str, replicas: int) -> dict[str, Any]:
    ts = HLCTimestamp(wall_ms=_wall(wall), counter=0, node_id="node-2")
    return {"version": {"key": {"ts": ts.to_dict()}}, "replicas": replicas}


@pytest.mark.asyncio
@pytest.mark.tourctl
@pytest.mark.parametrize(
    ("details", "message", "code"),
    [
        (
            {"code": "invalid_repair_token", "reason": "bad padding"},
            "repair: invalid token (bad padding). "
            "Copy it exactly as printed by the failed write.",
            2,
        ),
        (
            {"code": "version_not_found"},
            "repair: no replica holds this version; nothing to copy. "
            "Write the value again with tourctl put.",
            1,
        ),
        (
            {"code": "quorum_not_met", "received": 1, "required": 2},
            "repair: 1 of 2 required replicas hold the version.\n"
            "Retry later with the same command.",
            1,
        ),
        (
            {"code": "outdated", "newer": [_newer("2026-11-04T20:10:00.000Z", 1)]},
            "repair: a newer version exists; repairing an older one has no effect.\n"
            "Newer: 2026-11-04T20:10:00.000Z c=0 node-2 on 1 replica",
            1,
        ),
        (
            {"code": "outdated", "newer": [{"version": None}]},
            "repair: a newer version exists; repairing an older one has no effect.\n"
            "Newer: unknown version",
            1,
        ),
    ],
)
async def test_repair_explains_each_rejection(
    fake_kv: FakeKv, details: dict[str, Any], message: str, code: int
) -> None:
    fake_kv.error = _rejection("kv.repair", **details)

    error = await _fails(kv_repair(token="tok", quorum_write=2))

    assert error.message == message
    assert error.code == code


@pytest.mark.asyncio
@pytest.mark.tourctl
async def test_repair_lists_at_most_five_newer_versions(fake_kv: FakeKv) -> None:
    newer = [_newer(f"2026-11-04T20:1{i}:00.000Z", 2) for i in range(7)]
    fake_kv.error = _rejection("kv.repair", code="outdated", newer=newer)

    error = await _fails(kv_repair(token="tok"))

    lines = error.message.splitlines()
    assert len(lines) == 7
    assert lines[1] == "Newer: 2026-11-04T20:10:00.000Z c=0 node-2 on 2 replicas"
    assert lines[-1] == "... and 2 more"


# ------------------------------------------------------------------------ CLI


def _run(argv: list[str]) -> Any:
    return app(
        argv,
        exit_on_error=False,
        print_error=False,
        help_on_error=False,
        result_action="return_value",
    )


@pytest.mark.tourctl
def test_cli_forwards_get_and_delete_options(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    captured: dict[str, Any] = {}

    async def fake_kv_get(**kwargs: Any) -> str:
        captured["get"] = kwargs
        return "ok-get"

    async def fake_kv_delete(**kwargs: Any) -> str:
        captured["delete"] = kwargs
        return "ok-delete"

    monkeypatch.setattr("tourctl.bootstrap.usecases.kv.kv_get", fake_kv_get)
    monkeypatch.setattr("tourctl.bootstrap.usecases.kv.kv_delete", fake_kv_delete)

    _run(["get", "k", "-n", "ns", "--key-type", "int", "-r", "2", "--output", "f"])
    _run(["delete", "k", "-n", "ns", "-w", "3", "--json"])

    assert capsys.readouterr().out == "ok-get\nok-delete\n"
    assert captured["get"] | {"tourconf": None, "context_name": None} == {
        "key": "k",
        "namespace": "ns",
        "key_type": "int",
        "namespace_type": "str",
        "quorum_read": 2,
        "as_json": False,
        "raw": False,
        "output": "f",
        "tourconf": None,
        "context_name": None,
    }
    assert captured["delete"]["quorum_write"] == 3
    assert captured["delete"]["as_json"] is True


@pytest.mark.tourctl
def test_cli_accepts_a_dash_or_a_negative_number_as_the_value(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    values: list[str] = []

    async def fake_kv_put(**kwargs: Any) -> str:
        values.append(kwargs["value"])
        return "ok"

    monkeypatch.setattr("tourctl.bootstrap.usecases.kv.kv_put", fake_kv_put)

    _run(["put", "k", "-", "-w", "2"])
    _run(["put", "k", "-5"])

    assert values == ["-", "-5"]


@pytest.mark.tourctl
def test_cli_writes_raw_bytes_to_stdout(
    monkeypatch: pytest.MonkeyPatch, capsysbinary: pytest.CaptureFixture[bytes]
) -> None:
    async def fake_kv_get(**kwargs: Any) -> bytes:
        return b"\x00\xff"

    monkeypatch.setattr("tourctl.bootstrap.usecases.kv.kv_get", fake_kv_get)

    _run(["get", "k", "--raw"])

    assert capsysbinary.readouterr().out == b"\x00\xff"


@pytest.mark.tourctl
def test_cli_forwards_the_repair_token_and_quorum(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    captured: dict[str, Any] = {}

    async def fake_kv_repair(**kwargs: Any) -> str:
        captured.update(kwargs)
        return "ok-repair"

    monkeypatch.setattr("tourctl.bootstrap.usecases.kv.kv_repair", fake_kv_repair)

    _run(["repair", "tok-123", "-w", "2", "--json"])

    assert capsys.readouterr().out == "ok-repair\n"
    assert captured["token"] == "tok-123"
    assert captured["quorum_write"] == 2
    assert captured["as_json"] is True


@pytest.mark.tourctl
def test_main_renders_a_missing_key_as_json_on_stderr(
    fake_kv: FakeKv, capsys: pytest.CaptureFixture[str]
) -> None:
    fake_kv.result = _read(absent=True, answered=1, required=1, replicas=1)

    with pytest.raises(SystemExit) as exc_info:
        main(["get", "k", "-n", "n", "--json"])

    captured = capsys.readouterr()
    assert exc_info.value.code == 4
    assert captured.out == ""
    assert json.loads(captured.err)["error"]["code"] == "not_found"


@pytest.mark.tourctl
def test_main_renders_a_missing_key_as_text_on_stderr(
    fake_kv: FakeKv, capsys: pytest.CaptureFixture[str]
) -> None:
    fake_kv.result = _read(absent=True, answered=1, required=1, replicas=1)

    with pytest.raises(SystemExit) as exc_info:
        main(["get", "k", "-n", "n"])

    assert exc_info.value.code == 4
    assert capsys.readouterr().err == (
        "Error: get n/k: not found (1 of 1 replica answered).\n"
    )
