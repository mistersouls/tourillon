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

import base64
import json
import textwrap
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, NoReturn

from tourclient import KvService, client
from tourclient.exception import RequestRejectedError, UnknownServiceError
from tourctl.core.error import KV_ERROR_PROFILE, handle_error
from tourlib.adapter import MsgPackSerializer
from tourlib.models.version import Version

TypeName = Literal["auto", "str", "int", "float", "bool", "json", "bytes"]

_BOOL_TRUE = {"1", "true", "yes", "on"}
_BOOL_FALSE = {"0", "false", "no", "off"}
_PRETTY_WIDTH = 96
_LABEL_WIDTH = 13
_WIRE_SERIALIZER = MsgPackSerializer()


def _raise_user_error(message: str) -> NoReturn:
    raise SystemExit(message)


def _kv_error_message(exc: Exception, context_name: str | None) -> str:
    if isinstance(exc, UnknownServiceError):
        return str(exc)
    message = handle_error(exc, context_name, KV_ERROR_PROFILE)
    return f"{message}{_resume_hint(exc)}"


def _resume_hint(exc: Exception) -> str:
    """Surface the handle that makes a partial write resumable.

    A quorum failure means the version is written somewhere and stays there.
    Hiding the handle would leave the operator with a failure they cannot act
    on, and tempt them into a second write under a new timestamp.
    """
    if not isinstance(exc, RequestRejectedError):
        return ""
    token = exc.details.get("token")
    if not isinstance(token, str) or not token:
        return ""
    return f"\nThe version is written but under-replicated. Resume it with:\n  tourctl repair {token}"


def _parse_bool(value: str) -> bool:
    lowered = value.strip().lower()
    if lowered in _BOOL_TRUE:
        return True
    if lowered in _BOOL_FALSE:
        return False
    raise ValueError(f"invalid bool value: {value!r}")


def _infer_type(value: str) -> Any:
    lowered = value.strip().lower()
    if lowered in _BOOL_TRUE | _BOOL_FALSE:
        return _parse_bool(value)

    if _looks_like_int(value):
        return int(value, 10)
    if _looks_like_float(value):
        return float(value)

    if _looks_like_json(value):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value

    return value


def _looks_like_int(value: str) -> bool:
    stripped = value.strip()
    if not stripped:
        return False
    if stripped[0] in "+-":
        return stripped[1:].isdigit()
    return stripped.isdigit()


def _looks_like_float(value: str) -> bool:
    stripped = value.strip().lower()
    if not stripped:
        return False
    if _looks_like_int(stripped):
        return False
    if "." in stripped or "e" in stripped:
        try:
            float(stripped)
        except ValueError:
            return False
        return True
    return False


def _looks_like_json(value: str) -> bool:
    stripped = value.strip()
    return stripped.startswith(("{", "[", '"')) or stripped.lower() in {
        "true",
        "false",
        "null",
    }


def _parse_typed_value(value: str, type_name: TypeName) -> Any:
    if type_name == "auto":
        return _infer_type(value)
    if type_name == "str":
        return value
    if type_name == "int":
        return int(value, 10)
    if type_name == "float":
        return float(value)
    if type_name == "bool":
        return _parse_bool(value)
    if type_name == "json":
        return json.loads(value)
    return base64.b64decode(value, validate=True)


def _parse_file_content(payload: bytes, type_name: TypeName) -> Any:
    if type_name == "bytes":
        return payload
    if type_name == "auto":
        return payload
    text = payload.decode("utf-8")
    return _parse_typed_value(text, type_name)


def _parse_value(value: str, type_name: TypeName | None) -> Any:
    if value.startswith("@"):
        payload = Path(value[1:]).read_bytes()
        if type_name is None:
            return payload
        return _parse_file_content(payload, type_name)

    if type_name is None:
        return _parse_typed_value(value, "auto")
    return _parse_typed_value(value, type_name)


def _json_safe(value: Any) -> Any:
    if isinstance(value, bytes):
        return {
            "type": "bytes",
            "base64": base64.b64encode(value).decode("ascii"),
        }
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [_json_safe(item) for item in value]
    return value


def _decode_if_wire_encoded(value: Any) -> Any:
    if not isinstance(value, bytes):
        return value
    try:
        return _WIRE_SERIALIZER.decode(value)
    except ValueError:
        return value


def _version_to_render_dict(version: Version) -> dict[str, Any]:
    payload = version.to_dict()
    address = payload["address"]
    return {
        "address": {
            "key": _decode_if_wire_encoded(address["key"]),
            "namespace": _decode_if_wire_encoded(address["namespace"]),
        },
        "data": _decode_if_wire_encoded(payload["data"]),
        "meta": payload["meta"],
    }


def _stringify_pretty_value(value: Any) -> str:
    if isinstance(value, bytes):
        b64 = base64.b64encode(value).decode("ascii")
        return f"{b64} ({len(value):,} bytes total)"
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float, bool)) or value is None:
        return str(value)
    return json.dumps(_json_safe(value), ensure_ascii=False, indent=2, sort_keys=True)


def _format_labeled_block(label: str, value: Any) -> str:
    payload = _stringify_pretty_value(value)
    lines = payload.splitlines() or [""]

    first = f"{label:<{_LABEL_WIDTH}}{lines[0]}"
    if len(lines) > 1:
        rest = [" " * _LABEL_WIDTH + line for line in lines[1:]]
        return "\n".join([first, *rest])

    if len(first) <= _PRETTY_WIDTH:
        return first

    wrapped = textwrap.wrap(
        lines[0],
        width=max(20, _PRETTY_WIDTH - _LABEL_WIDTH),
        break_long_words=False,
        break_on_hyphens=False,
    )
    out = [f"{label:<{_LABEL_WIDTH}}{wrapped[0]}"]
    out.extend(" " * _LABEL_WIDTH + part for part in wrapped[1:])
    return "\n".join(out)


def _format_hlc_wall_clock(wall_ms: Any) -> str | None:
    if not isinstance(wall_ms, int):
        return None
    dt = datetime.fromtimestamp(wall_ms / 1000, tz=UTC)
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _render_metadata_lines(meta: Any) -> list[str]:
    if not isinstance(meta, Mapping):
        return []

    ts = meta.get("ts")
    quorum_write = meta.get("quorum_write")
    if not isinstance(ts, Mapping) or quorum_write is None:
        return []

    wall_clock = _format_hlc_wall_clock(ts.get("wall_ms"))
    counter = ts.get("counter")
    node_id = ts.get("node_id")
    if wall_clock is None or counter is None or node_id is None:
        return []

    return [
        "Metadata",
        f"    HLC:         {wall_clock}  (wall clock)",
        f"                 counter={counter}",
        f"                 node_id={node_id}",
        f"    Quorum W:    {quorum_write}",
    ]


def _render_pretty(value: Any) -> str:
    if value is None:
        return "Not found."

    if not isinstance(value, Mapping):
        return _format_labeled_block("Value:", value)

    address = value.get("address")
    if not isinstance(address, Mapping):
        return _format_labeled_block("Value:", value)

    key = address.get("key")
    namespace = address.get("namespace")
    payload = value.get("data")
    metadata_lines = _render_metadata_lines(value.get("meta"))

    lines = [
        _format_labeled_block("Key:", key),
        _format_labeled_block("Namespace:", namespace),
        _format_labeled_block("Value:", payload),
    ]
    lines.extend(metadata_lines)
    return "\n".join(lines)


def _render(value: Any, as_json: bool) -> str:
    display_value = _json_safe(value)
    if as_json:
        return json.dumps(display_value, ensure_ascii=False, indent=2, sort_keys=True)
    return _render_pretty(value)


async def _run_put(
    *,
    key: Any,
    value: Any,
    namespace: Any,
    quorum_write: int,
    tourconf: str | Path | None,
    context_name: str | None,
) -> dict[str, Any] | None:
    async with client(
        "kv", endpoint="client", tourconf=tourconf, context_name=context_name
    ) as kv_service:
        if not isinstance(kv_service, KvService):
            raise UnknownServiceError("Unknown service: kv")
        version = await kv_service.put(
            namespace=namespace, key=key, value=value, quorum_write=quorum_write
        )
        return None if version is None else _version_to_render_dict(version)


async def _run_get(
    *,
    key: Any,
    namespace: Any,
    quorum_read: int,
    tourconf: str | Path | None,
    context_name: str | None,
) -> dict[str, Any] | None:
    async with client(
        "kv", endpoint="client", tourconf=tourconf, context_name=context_name
    ) as kv_service:
        if not isinstance(kv_service, KvService):
            raise UnknownServiceError("Unknown service: kv")
        response = await kv_service.get(
            namespace=namespace, key=key, quorum_read=quorum_read
        )
        return None if response is None else _version_to_render_dict(response)


async def _run_delete(
    *,
    key: Any,
    namespace: Any,
    quorum_write: int,
    tourconf: str | Path | None,
    context_name: str | None,
) -> dict[str, Any] | None:
    async with client(
        "kv", endpoint="client", tourconf=tourconf, context_name=context_name
    ) as kv_service:
        if not isinstance(kv_service, KvService):
            raise UnknownServiceError("Unknown service: kv")
        version = await kv_service.delete(
            namespace=namespace, key=key, quorum_write=quorum_write
        )
        return None if version is None else _version_to_render_dict(version)


async def _run_repair(
    *,
    token: str,
    quorum_write: int,
    tourconf: str | Path | None,
    context_name: str | None,
) -> dict[str, Any]:
    async with client(
        "kv", endpoint="client", tourconf=tourconf, context_name=context_name
    ) as kv_service:
        if not isinstance(kv_service, KvService):
            raise UnknownServiceError("Unknown service: kv")
        result = await kv_service.repair(token=token, quorum_write=quorum_write)
        return dict(result)


async def kv_put(
    *,
    key: str,
    value: str,
    namespace: str,
    key_type: TypeName = "str",
    value_type: TypeName | None = None,
    namespace_type: TypeName = "auto",
    quorum_write: int = 1,
    as_json: bool = False,
    tourconf: str | Path | None = None,
    context_name: str | None = None,
) -> str:
    try:
        typed_key = _parse_typed_value(key, key_type)
        typed_namespace = _parse_typed_value(namespace, namespace_type)
        typed_value = _parse_value(value, value_type)
    except (
        FileNotFoundError,
        UnicodeDecodeError,
        ValueError,
        json.JSONDecodeError,
    ) as exc:
        _raise_user_error(f"Invalid KV input: {exc}")

    try:
        version = await _run_put(
            key=typed_key,
            value=typed_value,
            namespace=typed_namespace,
            quorum_write=quorum_write,
            tourconf=tourconf,
            context_name=context_name,
        )
    except Exception as exc:
        _raise_user_error(_kv_error_message(exc, context_name))

    return _render(version, as_json)


async def kv_get(
    *,
    key: str,
    namespace: str,
    key_type: TypeName = "str",
    namespace_type: TypeName = "auto",
    quorum_read: int = 1,
    as_json: bool = False,
    tourconf: str | Path | None = None,
    context_name: str | None = None,
) -> str:
    try:
        typed_key = _parse_typed_value(key, key_type)
        typed_namespace = _parse_typed_value(namespace, namespace_type)
    except (ValueError, json.JSONDecodeError) as exc:
        _raise_user_error(f"Invalid KV input: {exc}")

    try:
        version = await _run_get(
            key=typed_key,
            namespace=typed_namespace,
            quorum_read=quorum_read,
            tourconf=tourconf,
            context_name=context_name,
        )
    except Exception as exc:
        _raise_user_error(_kv_error_message(exc, context_name))

    return _render(version, as_json)


async def kv_delete(
    *,
    key: str,
    namespace: str,
    key_type: TypeName = "str",
    namespace_type: TypeName = "auto",
    quorum_write: int = 1,
    as_json: bool = False,
    tourconf: str | Path | None = None,
    context_name: str | None = None,
) -> str:
    try:
        typed_key = _parse_typed_value(key, key_type)
        typed_namespace = _parse_typed_value(namespace, namespace_type)
    except (ValueError, json.JSONDecodeError) as exc:
        _raise_user_error(f"Invalid KV input: {exc}")

    try:
        version = await _run_delete(
            key=typed_key,
            namespace=typed_namespace,
            quorum_write=quorum_write,
            tourconf=tourconf,
            context_name=context_name,
        )
    except Exception as exc:
        _raise_user_error(_kv_error_message(exc, context_name))

    return _render(version, as_json)


async def kv_repair(
    *,
    token: str,
    quorum_write: int = 1,
    as_json: bool = False,
    tourconf: str | Path | None = None,
    context_name: str | None = None,
) -> str:
    if not token.strip():
        _raise_user_error("Invalid KV input: repair token must not be empty")

    try:
        result = await _run_repair(
            token=token.strip(),
            quorum_write=quorum_write,
            tourconf=tourconf,
            context_name=context_name,
        )
    except Exception as exc:
        _raise_user_error(_kv_error_message(exc, context_name))

    if as_json:
        return json.dumps(
            _json_safe(result), ensure_ascii=False, indent=2, sort_keys=True
        )
    return _render_repair(result)


def _render_repair(result: Mapping[str, Any]) -> str:
    issue = result.get("issue")
    replicas = result.get("replicas")
    summary = {
        "repaired": "Repaired.",
        "already_durable": "Already durable, nothing to copy.",
    }.get(str(issue), str(issue))
    return "\n".join(
        [
            _format_labeled_block("Issue:", summary),
            _format_labeled_block("Replicas:", replicas),
            _format_labeled_block("Token:", result.get("token")),
        ]
    )
