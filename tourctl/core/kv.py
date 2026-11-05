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

import re
import shlex
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO, NoReturn

from tourclient import KvService, client
from tourclient.exception import RequestRejectedError, UnknownServiceError
from tourctl.core.context import resolve_namespace
from tourctl.core.error import (
    EXIT_FAILED,
    EXIT_INVALID,
    EXIT_NOT_FOUND,
    KV_ERROR_PROFILE,
    CliError,
    cli_error,
)
from tourctl.core.kvvalue import (
    TEXT_LIMIT,
    TypeName,
    ValueView,
    display,
    json_safe,
    parse_typed,
    read_value,
)
from tourctl.core.render import (
    block,
    fields,
    format_since,
    format_size,
    format_ts,
    messages,
    to_json,
    ts_to_json,
)
from tourlib.adapter import MsgPackSerializer
from tourlib.constant import MAX_QUORUM
from tourlib.models.kv import ReadOutcome, WriteOutcome
from tourlib.models.ts import HLCTimestamp

_WIRE = MsgPackSerializer()
_MAX_NEWER = 5
_TOMBSTONE_NOTE = (
    "The tombstone hides older versions; garbage collection removes them later."
)


def _invalid(message: str) -> NoReturn:
    raise CliError(message, EXIT_INVALID, error="invalid_input")


def _count(n: int, singular: str, plural: str | None = None) -> str:
    return f"{n} {singular if n == 1 else plural or singular + 's'}"


@dataclass(frozen=True)
class KeyArgs:
    """A key as typed on the command line, to name it again in hints."""

    key: str
    namespace: str | None = None
    key_type: TypeName = "str"
    namespace_type: TypeName = "str"

    def hint(self) -> str:
        parts = [shlex.quote(self.key)]
        if self.namespace is not None:
            parts += ["-n", shlex.quote(self.namespace)]
        if self.namespace_type != "str":
            parts += ["--namespace-type", self.namespace_type]
        if self.key_type != "str":
            parts += ["--key-type", self.key_type]
        return " ".join(parts)


@dataclass(frozen=True)
class KeyRef:
    """A resolved address together with the words that name it."""

    namespace: Any
    key: Any
    verb: str
    hint: str

    @property
    def target(self) -> str:
        return f"{display(self.namespace)}/{display(self.key)}"

    @property
    def subject(self) -> str:
        return f"{self.verb} {self.target}"

    def address(self) -> dict[str, Any]:
        return {"key": json_safe(self.key), "namespace": json_safe(self.namespace)}


async def _resolve(
    verb: str, args: KeyArgs, tourconf: str | Path | None, context_name: str | None
) -> KeyRef:
    try:
        key = parse_typed(args.key, args.key_type)
    except ValueError as exc:
        _invalid(f"Invalid key: {exc}")
    try:
        raw = await resolve_namespace(
            args.namespace, tourconf=tourconf, context_name=context_name
        )
    except (OSError, KeyError, ValueError) as exc:
        raise cli_error(exc, context_name, KV_ERROR_PROFILE) from None
    try:
        namespace = parse_typed(raw, args.namespace_type)
    except ValueError as exc:
        _invalid(f"Invalid namespace: {exc}")
    return KeyRef(namespace=namespace, key=key, verb=verb, hint=args.hint())


def _check_quorum(subject: str, quorum: int) -> None:
    if not 1 <= quorum <= MAX_QUORUM:
        raise CliError(
            f"{subject}: quorum {quorum} is out of range [1, {MAX_QUORUM}].",
            EXIT_INVALID,
            error="invalid_quorum",
        )


# Rejections: each server refusal code becomes a message that says what
# happened, what is already done, and which command comes next.

Rejection = Callable[[str, Mapping[str, Any], "KeyRef | None"], CliError]


def _failed(
    lines: list[str], details: Mapping[str, Any], code: int = EXIT_FAILED
) -> CliError:
    return CliError(
        "\n".join(lines), code, error=str(details.get("code")), details=details
    )


def _write_quorum(
    subject: str, details: Mapping[str, Any], ref: KeyRef | None
) -> CliError:
    received, required = details.get("received", 0), details.get("required", 0)
    token = details.get("token")
    lines = [
        f"{subject}: quorum not met, {received} of {required} required replicas acknowledged."
    ]
    if received:
        lines.append(
            f"The version is stored on {_count(received, 'replica')} and is not lost; "
            "anti-entropy spreads it."
        )
    if token:
        lines += [
            f"Finish the {subject.split()[0]} with:",
            f"  tourctl repair {token} -w {required}",
        ]
    if ref is not None:
        lines += ["See where it landed with:", f"  tourctl explain {ref.hint}"]
    return _failed(lines, details)


def _read_quorum(
    subject: str, details: Mapping[str, Any], ref: KeyRef | None
) -> CliError:
    received, required = details.get("received", 0), details.get("required", 0)
    lines = [
        f"{subject}: quorum not met, {received} of {required} required replicas answered."
    ]
    if ref is not None:
        lines += [
            "See which replicas are unavailable with:",
            f"  tourctl explain {ref.hint}",
        ]
    return _failed(lines, details)


def _quorum_not_met(
    subject: str, details: Mapping[str, Any], ref: KeyRef | None
) -> CliError:
    if details.get("operation") == "read":
        return _read_quorum(subject, details, ref)
    return _write_quorum(subject, details, ref)


def _quorum_unreachable(
    subject: str, details: Mapping[str, Any], ref: KeyRef | None
) -> CliError:
    quorum, factor = details.get("quorum"), details.get("replication_factor")
    return _failed(
        [
            f"{subject}: quorum {quorum} can never be met, replication factor is {factor}."
        ],
        details,
    )


def _invalid_quorum(
    subject: str, details: Mapping[str, Any], ref: KeyRef | None
) -> CliError:
    quorum = details.get("quorum")
    return _failed(
        [f"{subject}: quorum {quorum} is out of range [1, {MAX_QUORUM}]."],
        details,
        EXIT_INVALID,
    )


def _draining(subject: str, details: Mapping[str, Any], ref: KeyRef | None) -> CliError:
    work = "reads" if details.get("operation") == "read" else "writes"
    node = details.get("node_id") or "the node"
    return _failed(
        [
            f"{subject}: {node} is draining and accepts no new {work}.",
            "Use another node's client endpoint.",
        ],
        details,
    )


def _no_replica(
    subject: str, details: Mapping[str, Any], ref: KeyRef | None
) -> CliError:
    lines = [f"{subject}: no replica can take this key; every replica is unavailable."]
    if ref is not None:
        lines.append(f"See: tourctl explain {ref.hint}")
    return _failed(lines, details)


def _bad_token(
    subject: str, details: Mapping[str, Any], ref: KeyRef | None
) -> CliError:
    reason = details.get("reason", "unreadable")
    return _failed(
        [
            f"{subject}: invalid token ({reason}). "
            "Copy it exactly as printed by the failed write."
        ],
        details,
        EXIT_INVALID,
    )


def _version_missing(
    subject: str, details: Mapping[str, Any], ref: KeyRef | None
) -> CliError:
    return _failed(
        [
            f"{subject}: no replica holds this version; nothing to copy. "
            "Write the value again with tourctl put."
        ],
        details,
    )


def _newer_line(item: Any) -> str:
    try:
        ts = HLCTimestamp.from_dict(item["version"]["key"]["ts"])
        replicas = int(item.get("replicas", 0))
    except (KeyError, TypeError, ValueError, AttributeError):
        return "Newer: unknown version"
    return f"Newer: {format_ts(ts)} on {_count(replicas, 'replica')}"


def _outdated(subject: str, details: Mapping[str, Any], ref: KeyRef | None) -> CliError:
    newer = details.get("newer")
    items = newer if isinstance(newer, list) else []
    lines = [
        f"{subject}: a newer version exists; repairing an older one has no effect."
    ]
    lines += [_newer_line(item) for item in items[:_MAX_NEWER]]
    if len(items) > _MAX_NEWER:
        lines.append(f"... and {len(items) - _MAX_NEWER} more")
    return _failed(lines, details)


def _repair_quorum(
    subject: str, details: Mapping[str, Any], ref: KeyRef | None
) -> CliError:
    received, required = details.get("received", 0), details.get("required", 0)
    return _failed(
        [
            f"{subject}: {received} of {required} required replicas hold the version.",
            "Retry later with the same command.",
        ],
        details,
    )


_KV_REJECTIONS: dict[str, Rejection] = {
    "quorum_not_met": _quorum_not_met,
    "quorum_unreachable": _quorum_unreachable,
    "invalid_quorum": _invalid_quorum,
    "node_draining": _draining,
    "no_replica_available": _no_replica,
}

_REPAIR_REJECTIONS: dict[str, Rejection] = {
    "quorum_not_met": _repair_quorum,
    "invalid_quorum": _invalid_quorum,
    "invalid_repair_token": _bad_token,
    "version_not_found": _version_missing,
    "outdated": _outdated,
    "node_draining": _draining,
}


def _rejected(
    exc: RequestRejectedError,
    subject: str,
    ref: KeyRef | None,
    table: Mapping[str, Rejection],
) -> CliError:
    details = exc.details
    handler = table.get(str(exc.code))
    if handler is not None:
        return handler(subject, details, ref)
    message = str(details.get("message") or exc).rstrip(".")
    return _failed(
        [f"{subject}: {message}."], {**details, "code": exc.code or "rejected"}
    )


async def _call(
    method: str, tourconf: str | Path | None, context_name: str | None, **kwargs: Any
) -> Any:
    async with client(
        "kv", endpoint="client", tourconf=tourconf, context_name=context_name
    ) as service:
        if not isinstance(service, KvService):
            raise UnknownServiceError("Unknown service: kv")
        return await getattr(service, method)(**kwargs)


async def _request(
    method: str,
    subject: str,
    ref: KeyRef | None,
    table: Mapping[str, Rejection],
    tourconf: str | Path | None,
    context_name: str | None,
    **kwargs: Any,
) -> Any:
    try:
        return await _call(method, tourconf, context_name, **kwargs)
    except RequestRejectedError as exc:
        raise _rejected(exc, subject, ref, table) from None
    except Exception as exc:
        raise cli_error(exc, context_name, KV_ERROR_PROFILE) from None


def _decode(data: bytes) -> Any:
    try:
        return _WIRE.decode(data)
    except ValueError:
        return data


def _quorum_json(outcome: WriteOutcome) -> dict[str, int]:
    return {
        "acknowledged": outcome.acknowledged,
        "replicas": outcome.replicas,
        "required": outcome.required,
    }


def _acknowledged(outcome: WriteOutcome) -> str:
    return (
        f"acknowledged by {outcome.acknowledged} of "
        f"{_count(outcome.replicas, 'replica')} ({outcome.required} required)"
    )


def _put_value(view: ValueView) -> str:
    inline = view.inline()
    if inline is None:
        return view.summary
    if view.type == "str":
        return f"{inline} ({view.summary})"
    return f"{inline} ({view.type})"


def _render_put(ref: KeyRef, outcome: WriteOutcome, as_json: bool) -> str:
    view = ValueView.of(_decode(outcome.version.data))
    ts = outcome.version.meta.ts
    if as_json:
        return to_json(
            {
                "address": ref.address(),
                "quorum": _quorum_json(outcome),
                "value": view.to_json(with_data=False),
                "version": ts_to_json(ts),
            }
        )
    rows = [
        ("Version", format_ts(ts)),
        ("Value", _put_value(view)),
        ("Quorum", _acknowledged(outcome)),
    ]
    return f"Stored {ref.target}.\n{fields(rows)}"


def _render_delete(ref: KeyRef, outcome: WriteOutcome, as_json: bool) -> str:
    ts = outcome.version.meta.ts
    if as_json:
        return to_json(
            {
                "address": ref.address(),
                "quorum": _quorum_json(outcome),
                "version": ts_to_json(ts),
            }
        )
    rows = [
        ("Version", f"{format_ts(ts)} (tombstone)"),
        ("Quorum", _acknowledged(outcome)),
    ]
    return f"Deleted {ref.target}.\n{fields(rows)}\n\n{messages([_TOMBSTONE_NOTE])}"


def _absent(ref: KeyRef, outcome: ReadOutcome) -> CliError:
    tombstone = outcome.tombstone
    details = {
        "answered": outcome.answered,
        "replicas": outcome.replicas,
        "required": outcome.required,
        "tombstone": None if tombstone is None else ts_to_json(tombstone),
    }
    if tombstone is not None:
        message = (
            f"{ref.subject}: deleted {format_since(tombstone.wall_ms)} "
            f"({format_ts(tombstone)})."
        )
    else:
        message = (
            f"{ref.subject}: not found ({outcome.answered} of "
            f"{_count(outcome.replicas, 'replica')} answered)."
        )
    return CliError(message, EXIT_NOT_FOUND, error="not_found", details=details)


@dataclass(frozen=True)
class _Read:
    ref: KeyRef
    outcome: ReadOutcome
    view: ValueView

    @property
    def ts(self) -> HLCTimestamp:
        assert self.outcome.version is not None
        return self.outcome.version.meta.ts

    def to_json(self, *, with_data: bool) -> str:
        return to_json(
            {
                "address": self.ref.address(),
                "read": {
                    "answered": self.outcome.answered,
                    "replicas": self.outcome.replicas,
                    "required": self.outcome.required,
                },
                "value": self.view.to_json(with_data=with_data),
                "version": ts_to_json(self.ts),
            }
        )

    def rows(self, inline: str | None) -> list[tuple[str, str]]:
        outcome = self.outcome
        rows = [("Key", self.ref.target)]
        if inline is not None:
            rows.append(("Value", inline))
        return rows + [
            ("Type", self.view.summary),
            ("Version", f"{format_ts(self.ts)} ({format_since(self.ts.wall_ms)})"),
            (
                "Read",
                f"{outcome.answered} of {_count(outcome.replicas, 'replica')} "
                f"answered ({outcome.required} required)",
            ),
        ]


def _file_name(key: Any) -> str:
    stem = re.sub(r"[^\w.-]", "_", display(key)).strip("._")
    return f"{stem or 'value'}.bin"


def _long_value(read: _Read) -> str:
    head = fields(read.rows(None))
    data = read.view.text.encode("utf-8")
    if len(data) <= TEXT_LIMIT:
        return f"{head}\n{block('Value', read.view.text)}"
    shown = data[:TEXT_LIMIT].decode("utf-8", errors="ignore")
    note = (
        f"Value truncated ({format_size(len(data))}). "
        f"Show all with: tourctl get {read.ref.hint} --raw"
    )
    return f"{head}\n{block('Value', shown)}\n\n{messages([note])}"


def _render_get_text(read: _Read) -> str:
    if read.view.type == "bytes":
        rows = read.rows(None) + [("Value", "not shown (binary)")]
        note = (
            f"Save it with: tourctl get {read.ref.hint} "
            f"--output {_file_name(read.ref.key)}"
        )
        return f"{fields(rows)}\n\n{messages([note])}"
    inline = read.view.inline()
    if inline is None:
        return _long_value(read)
    return fields(read.rows(inline))


def _save(read: _Read, output: str) -> int:
    data = read.view.encoded
    try:
        Path(output).write_bytes(data)
    except OSError as exc:
        raise CliError(
            f"{read.ref.subject}: cannot write {output}: {exc.strerror or exc}.",
            EXIT_INVALID,
            error="invalid_output",
        ) from None
    return len(data)


def _render_get(
    read: _Read, as_json: bool, raw: bool, output: str | None
) -> str | bytes:
    if raw:
        return read.view.raw()
    if output is not None:
        size = _save(read, output)
        if as_json:
            return read.to_json(with_data=False)
        return f"Saved {read.ref.target} to {output} ({format_size(size)})."
    if as_json:
        return read.to_json(with_data=True)
    return _render_get_text(read)


def _check_get_modes(as_json: bool, raw: bool, output: str | None) -> None:
    if as_json and raw:
        _invalid("--json and --raw cannot be combined.")
    if raw and output is not None:
        _invalid("--raw and --output cannot be combined.")


async def kv_put(
    *,
    key: str,
    value: str,
    namespace: str | None = None,
    key_type: TypeName = "str",
    value_type: TypeName | None = None,
    namespace_type: TypeName = "str",
    quorum_write: int = 1,
    as_json: bool = False,
    tourconf: str | Path | None = None,
    context_name: str | None = None,
    stdin: BinaryIO | None = None,
) -> str:
    try:
        typed_value = read_value(value, value_type, stdin)
    except ValueError as exc:
        _invalid(f"Invalid value: {exc}")
    args = KeyArgs(key, namespace, key_type, namespace_type)
    ref = await _resolve("put", args, tourconf, context_name)
    _check_quorum(ref.subject, quorum_write)
    outcome = await _request(
        "put",
        ref.subject,
        ref,
        _KV_REJECTIONS,
        tourconf,
        context_name,
        namespace=ref.namespace,
        key=ref.key,
        value=typed_value,
        quorum_write=quorum_write,
    )
    return _render_put(ref, outcome, as_json)


async def kv_get(
    *,
    key: str,
    namespace: str | None = None,
    key_type: TypeName = "str",
    namespace_type: TypeName = "str",
    quorum_read: int = 1,
    as_json: bool = False,
    raw: bool = False,
    output: str | None = None,
    tourconf: str | Path | None = None,
    context_name: str | None = None,
) -> str | bytes:
    _check_get_modes(as_json, raw, output)
    args = KeyArgs(key, namespace, key_type, namespace_type)
    ref = await _resolve("get", args, tourconf, context_name)
    _check_quorum(ref.subject, quorum_read)
    outcome: ReadOutcome = await _request(
        "get",
        ref.subject,
        ref,
        _KV_REJECTIONS,
        tourconf,
        context_name,
        namespace=ref.namespace,
        key=ref.key,
        quorum_read=quorum_read,
    )
    if outcome.version is None:
        raise _absent(ref, outcome)
    read = _Read(ref, outcome, ValueView.of(_decode(outcome.version.data)))
    return _render_get(read, as_json, raw, output)


async def kv_delete(
    *,
    key: str,
    namespace: str | None = None,
    key_type: TypeName = "str",
    namespace_type: TypeName = "str",
    quorum_write: int = 1,
    as_json: bool = False,
    tourconf: str | Path | None = None,
    context_name: str | None = None,
) -> str:
    args = KeyArgs(key, namespace, key_type, namespace_type)
    ref = await _resolve("delete", args, tourconf, context_name)
    _check_quorum(ref.subject, quorum_write)
    outcome = await _request(
        "delete",
        ref.subject,
        ref,
        _KV_REJECTIONS,
        tourconf,
        context_name,
        namespace=ref.namespace,
        key=ref.key,
        quorum_write=quorum_write,
    )
    return _render_delete(ref, outcome, as_json)


def _render_repair(result: Mapping[str, Any], required: int) -> str:
    replicas = int(result.get("replicas", 0))
    if result.get("issue") == "repaired":
        copies = _count(replicas, "copy", "copies")
        return f"Repaired: the version now has {copies} ({required} required)."
    verb = "holds" if replicas == 1 else "hold"
    return (
        f"Already durable: {_count(replicas, 'replica')} {verb} the version; "
        "nothing was copied."
    )


async def kv_repair(
    *,
    token: str,
    quorum_write: int = 1,
    as_json: bool = False,
    tourconf: str | Path | None = None,
    context_name: str | None = None,
) -> str:
    token = token.strip()
    if not token:
        _invalid("repair: token must not be empty.")
    _check_quorum("repair", quorum_write)
    result = await _request(
        "repair",
        "repair",
        None,
        _REPAIR_REJECTIONS,
        tourconf,
        context_name,
        token=token,
        quorum_write=quorum_write,
    )
    if as_json:
        return to_json(
            {
                "issue": result.get("issue"),
                "replicas": result.get("replicas"),
                "required": quorum_write,
                "token": result.get("token", token),
            }
        )
    return _render_repair(result, quorum_write)
