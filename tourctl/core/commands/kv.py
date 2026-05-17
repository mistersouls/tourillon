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
"""KvCommand — client-side logic for kv.put, kv.get, kv.delete.

Sends the appropriate envelope over the mTLS connection, waits for the
response, and renders or forwards the result to the caller.
"""

from __future__ import annotations

import base64
import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import msgpack

from tourctl.core.ports.console import ConsolePort
from tourillon.core.ports.transport import RESPONSE_TIMEOUT, ResponseTimeoutError
from tourillon.core.structure.envelope import Envelope

if TYPE_CHECKING:
    from tourillon.core.ports.serializer import SerializerPort
    from tourillon.core.transport.client import TcpClient


def _fmt_hlc(hlc: dict[str, Any]) -> str:
    """Format an HLC dict as a human-readable string."""
    wall_ms = int(hlc.get("wall_ms") or hlc.get("wall", 0))
    counter = int(hlc.get("counter", 0))
    node_id = str(hlc.get("node_id", "?"))
    dt = datetime.fromtimestamp(wall_ms / 1000.0, tz=UTC)
    ts = dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"
    return f"{ts} \u00b7 #{counter:04d} \u00b7 {node_id}"


def _decode_value(raw: bytes) -> Any:  # noqa: ANN401
    """Decode msgpack bytes to a Python value for display."""
    try:
        return msgpack.unpackb(raw, raw=False)
    except Exception:
        return raw


def _json_safe(val: Any) -> Any:  # noqa: ANN401
    """Convert a Python value to a JSON-serialisable form."""
    if isinstance(val, bytes):
        return {"_type": "bytes", "_value": base64.b64encode(val).decode()}
    if isinstance(val, dict):
        return {k: _json_safe(v) for k, v in val.items()}
    if isinstance(val, list):
        return [_json_safe(v) for v in val]
    return val


class KvPutCommand:
    """Send kv.put and render the response."""

    def __init__(
        self,
        client: TcpClient,
        serializer: SerializerPort,
        console: ConsolePort,
        err_console: ConsolePort,
    ) -> None:
        self._client = client
        self._serializer = serializer
        self._console = console
        self._err_console = err_console

    async def run(
        self,
        key: bytes,
        keyspace: bytes,
        value: bytes,
        *,
        quorum_write: int | None = None,
        json_output: bool = False,
        timeout: float = RESPONSE_TIMEOUT,
    ) -> int:
        """Send kv.put and print result. Return exit code."""
        body: dict[str, Any] = {"key": key, "keyspace": keyspace, "value": value}
        if quorum_write is not None:
            body["quorum_write"] = quorum_write

        payload = self._serializer.encode(body)
        env = Envelope.create(
            payload, kind="kv.put", schema_id=self._serializer.schema_id
        )
        try:
            resp = await self._client.request(env, timeout=timeout)
        except ResponseTimeoutError:
            self._err_console.print("✗  write failed: request timeout")
            return 1

        if resp.kind == "kv.error":
            data = self._serializer.decode(resp.payload)
            reason = data.get("reason", "unknown")
            self._err_console.print(f"✗  write failed: {reason}")
            return 1

        data = self._serializer.decode(resp.payload)
        hlc = data.get("hlc", {})
        replicas = data.get("replicas", 0)

        if json_output:
            out = {"hlc": hlc, "replicas": replicas}
            self._console.print(json.dumps(out))
            return 0

        key_str = str(_decode_value(key))
        hlc_str = _fmt_hlc(hlc)
        self._console.print(
            f"\u2713  {key_str}  written  [hlc {hlc_str}, acks {replicas}]",
            markup=False,
        )
        return 0


class KvGetCommand:
    """Send kv.get and render the response."""

    def __init__(
        self,
        client: TcpClient,
        serializer: SerializerPort,
        console: ConsolePort,
        err_console: ConsolePort,
    ) -> None:
        self._client = client
        self._serializer = serializer
        self._console = console
        self._err_console = err_console

    async def run(
        self,
        key: bytes,
        keyspace: bytes,
        *,
        quorum_read: int | None = None,
        json_output: bool = False,
        timeout: float = RESPONSE_TIMEOUT,
    ) -> int:
        """Send kv.get and print result. Return exit code."""
        body: dict[str, Any] = {"key": key, "keyspace": keyspace}
        if quorum_read is not None:
            body["quorum_read"] = quorum_read

        payload = self._serializer.encode(body)
        env = Envelope.create(
            payload, kind="kv.get", schema_id=self._serializer.schema_id
        )
        try:
            resp = await self._client.request(env, timeout=timeout)
        except ResponseTimeoutError:
            self._err_console.print("✗  read failed: request timeout")
            return 1

        if resp.kind == "kv.error":
            data = self._serializer.decode(resp.payload)
            reason = data.get("reason", "unknown")
            self._err_console.print(f"✗  read failed: {reason}")
            return 1

        data = self._serializer.decode(resp.payload)
        versions = data.get("versions", [])

        if json_output:
            out: dict[str, Any] = {"versions": versions}
            self._console.print(json.dumps(out, default=_json_safe))
            return 0

        return self._render_versions(key, keyspace, versions)

    def _render_versions(
        self, key: bytes, keyspace: bytes, versions: list[dict[str, Any]]
    ) -> int:
        """Render version list to the console. Return exit code."""
        key_str = str(_decode_value(key))
        ks_str = str(_decode_value(keyspace))

        if not versions:
            self._err_console.print("✗  not found")
            return 1

        confirmed = next((v for v in versions if v.get("confirmed")), None)
        if confirmed is not None:
            hlc = confirmed.get("hlc") or {}
            count = confirmed.get("count", 0)
            qw = confirmed.get("quorum_write", 1)
            hlc_str = _fmt_hlc(hlc)
            val_raw = confirmed.get("value")
            if val_raw is None:
                self._console.print("✓  confirmed  (deleted)")
                self._console.print(f"   key       {key_str}")
                self._console.print(f"   keyspace  {ks_str}")
                self._console.print(f"   hlc       {hlc_str}")
                self._console.print(f"   replicas  {count}/{qw}")
            else:
                decoded = _decode_value(bytes(val_raw))
                type_name = type(decoded).__name__
                self._console.print("✓  confirmed")
                self._console.print(f"   key       {key_str}")
                self._console.print(f"   keyspace  {ks_str}")
                self._console.print(f"   value     {decoded}")
                self._console.print(f"   type      {type_name}")
                self._console.print(f"   hlc       {hlc_str}")
                self._console.print(f"   replicas  {count}/{qw}")
            return 0

        # Case 2 — uncertainty
        n = len(versions)
        self._console.print(
            f"⚠  uncertain — {n} candidates, none confirmed  (quorum not reached at read time)"
        )
        self._console.print("   #  value  type  hlc                           replicas")
        for i, v in enumerate(versions, 1):
            hlc = v.get("hlc") or {}
            count = v.get("count", 0)
            qw = v.get("quorum_write", 1)
            val_raw = v.get("value")
            if val_raw is None:
                val_str, type_str = "(deleted)", "tombstone"
            else:
                decoded = _decode_value(bytes(val_raw))
                val_str = str(decoded)[:12]
                type_str = type(decoded).__name__
            hlc_str = _fmt_hlc(hlc)
            self._console.print(
                f"   {i}  {val_str}  {type_str}  {hlc_str}   {count}/{qw}"
            )
        return 0


class KvDeleteCommand:
    """Send kv.delete and render the response."""

    def __init__(
        self,
        client: TcpClient,
        serializer: SerializerPort,
        console: ConsolePort,
        err_console: ConsolePort,
    ) -> None:
        self._client = client
        self._serializer = serializer
        self._console = console
        self._err_console = err_console

    async def run(
        self,
        key: bytes,
        keyspace: bytes,
        *,
        quorum_write: int | None = None,
        json_output: bool = False,
        timeout: float = RESPONSE_TIMEOUT,
    ) -> int:
        """Send kv.delete and print result. Return exit code."""
        body: dict[str, Any] = {"key": key, "keyspace": keyspace}
        if quorum_write is not None:
            body["quorum_write"] = quorum_write

        payload = self._serializer.encode(body)
        env = Envelope.create(
            payload, kind="kv.delete", schema_id=self._serializer.schema_id
        )
        try:
            resp = await self._client.request(env, timeout=timeout)
        except ResponseTimeoutError:
            self._err_console.print("✗  delete failed: request timeout")
            return 1

        if resp.kind == "kv.error":
            data = self._serializer.decode(resp.payload)
            reason = data.get("reason", "unknown")
            self._err_console.print(f"✗  delete failed: {reason}")
            return 1

        data = self._serializer.decode(resp.payload)
        hlc = data.get("hlc", {})
        replicas = data.get("replicas", 0)

        if json_output:
            out = {"hlc": hlc, "replicas": replicas}
            self._console.print(json.dumps(out))
            return 0

        key_str = str(_decode_value(key))
        hlc_str = _fmt_hlc(hlc)
        self._console.print(
            f"\u2713  {key_str}  deleted  [hlc {hlc_str}, acks {replicas}]",
            markup=False,
        )
        return 0
