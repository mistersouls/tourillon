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

import json
import os
import socket
import subprocess
import time
from collections.abc import Callable
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from typing import Any

COMMAND_TIMEOUT = 15
CONVERGENCE_TIMEOUT = 60
SCENARIO_TIMEOUT = 240
_WAIT_DEADLINE: ContextVar[float | None] = ContextVar("wait_deadline", default=None)


def reserve_addresses() -> tuple[str, str]:
    # Keep both sockets reserved until both distinct ports have been selected.
    with socket.socket() as client, socket.socket() as peer:
        client.bind(("127.0.0.1", 0))
        peer.bind(("127.0.0.1", 0))
        return (
            f"127.0.0.1:{client.getsockname()[1]}",
            f"127.0.0.1:{peer.getsockname()[1]}",
        )


def wait_for(
    condition: Callable[[], Any], description: str, timeout: float = CONVERGENCE_TIMEOUT
) -> Any:
    deadline = time.monotonic() + timeout
    token = _WAIT_DEADLINE.set(deadline)
    last: object = None
    try:
        while time.monotonic() < deadline:
            try:
                last = condition()
                if last:
                    return last
            except AssertionError as exc:
                last = exc
            time.sleep(min(0.2, max(0.0, deadline - time.monotonic())))
    finally:
        _WAIT_DEADLINE.reset(token)
    raise AssertionError(
        f"Timed out waiting for {description}; last observation: {last}"
    )


@dataclass
class Node:
    name: str
    config: Path
    client_config: Path
    client: str
    peer: str
    process: subprocess.Popen | None = None


def command_environment(standalone: bool) -> dict[str, str]:
    if not standalone:
        return {**os.environ, "PYTHONIOENCODING": "utf-8", "NO_COLOR": "1"}
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.upper().startswith(
            ("PYTHON", "UV_", "VIRTUAL_ENV", "CONDA", "LD_", "DYLD_")
        )
    }
    environment.update(PATH="", NO_COLOR="1", LC_ALL="C")
    return environment


class Journey:
    def __init__(
        self, root: Path, executables: dict[str, str], standalone_tools: set[str]
    ) -> None:
        self.root = root
        self.executables = executables
        self.standalone_tools = standalone_tools
        self.nodes: dict[str, Node] = {}
        self.commands = 0
        self.observations: dict[str, Any] = {}
        self.references: dict[str, Any] = {}
        self.durations: list[tuple[float, str]] = []
        self.ca = root / "ca"
        self.deadline = time.monotonic() + SCENARIO_TIMEOUT

    def command_timeout(self) -> float:
        # Commands inside a wait share its deadline instead of extending it.
        limits = [COMMAND_TIMEOUT, self.deadline - time.monotonic()]
        wait_deadline = _WAIT_DEADLINE.get()
        if wait_deadline is not None:
            limits.append(wait_deadline - time.monotonic())
        remaining = min(limits)
        if remaining <= 0:
            raise AssertionError(f"E2E time budget exhausted; logs: {self.root}")
        return remaining

    def run(self, tool: str, *args: str) -> subprocess.CompletedProcess[str]:
        self.commands += 1
        timeout = self.command_timeout()
        log = self.root / f"command-{self.commands:04d}.log"
        started = time.monotonic()
        try:
            result = subprocess.run(
                [self.executables[tool], *args],
                capture_output=True,
                text=True,
                encoding="utf-8",
                cwd=self.root,
                env=command_environment(tool in self.standalone_tools),
                timeout=timeout,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            log.write_text(
                f"{tool} {' '.join(args)}\ntimeout={timeout:.1f}s\n", encoding="utf-8"
            )
            raise AssertionError(f"{tool} {args[:3]} exceeded {timeout:.1f}s") from exc
        self.durations.append(
            (time.monotonic() - started, f"{tool} {' '.join(args[:3])}")
        )
        log.write_text(
            f"{tool} {' '.join(args)}\nexit={result.returncode}\n"
            f"{result.stdout}\n{result.stderr}",
            encoding="utf-8",
        )
        return result

    def generate(self, *args: str) -> None:
        result = self.run("tourillon", "config", *args)
        assert result.returncode == 0, result.stdout + result.stderr

    def generate_ca(self, directory: Path) -> None:
        directory.mkdir()
        self.generate(
            "ca",
            "--output-dir",
            str(directory),
            "--key-size",
            "2048",
            "--validity-days",
            "2",
        )

    def client_config(self, node: Node, name: str, ca: Path) -> Path:
        path = self.root / f"{name}.toml"
        self.generate(
            "client",
            name,
            "--ca-cert",
            str(ca / "ca-cert.pem"),
            "--ca-key",
            str(ca / "ca-key.pem"),
            "--output-file",
            str(path),
            "--client-address",
            node.client,
            "--peer-address",
            node.peer,
            "--validity-days",
            "2",
        )
        return path

    def configure(self, name: str, seed: str | None = None) -> Node:
        client, peer = reserve_addresses()
        node = Node(
            name,
            self.root / f"{name}.toml",
            self.root / f"client-{name}.toml",
            client,
            peer,
        )
        args = [
            "node",
            name,
            str(self.root / f"data-{name}"),
            "8Mi",
            str(self.ca / "ca-cert.pem"),
            str(self.ca / "ca-key.pem"),
            "--output-file",
            str(node.config),
            "--client-bind",
            client,
            "--peer-bind",
            peer,
            "--peer-advertise",
            peer,
            "--validity-days",
            "2",
            "--key-size",
            "2048",
            "--node-size",
            "XXL",
        ]
        if seed:
            args.extend(["--seeds", seed])
        self.generate(*args)
        node.client_config = self.client_config(node, f"client-{name}", self.ca)
        self.nodes[name] = node
        return node

    def start(self, node: Node) -> None:
        number = self.commands
        with (
            (self.root / f"{node.name}-{number}-stdout.log").open("ab") as stdout,
            (self.root / f"{node.name}-{number}-stderr.log").open("ab") as stderr,
        ):
            node.process = subprocess.Popen(
                [self.executables["tourillon"], "node", "start", str(node.config)],
                stdout=stdout,
                stderr=stderr,
                cwd=self.root,
                env=command_environment("tourillon" in self.standalone_tools),
            )

    def stop(self, node: Node) -> None:
        process = node.process
        if process is None or process.poll() is not None:
            return
        if os.name == "nt":
            # Windows console-script launchers may own a child interpreter.
            subprocess.run(
                ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                capture_output=True,
                timeout=10,
                check=False,
            )
        else:
            process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)

    def close(self) -> None:
        errors = []
        for node in reversed(list(self.nodes.values())):
            try:
                self.stop(node)
            except Exception as exc:
                errors.append(exc)
        assert all(
            n.process is None or n.process.poll() is not None
            for n in self.nodes.values()
        ), f"Node cleanup failed: {errors}"
        assert not errors, errors

    def ctl_result(
        self, node: Node, *args: str, config: Path | None = None
    ) -> subprocess.CompletedProcess[str]:
        return self.run(
            "tourctl", *args, "--tourconf", str(config or node.client_config)
        )

    def ctl(self, node: Node, *args: str) -> Any:
        result = self.ctl_result(node, *args)
        assert result.returncode == 0, result.stdout + result.stderr
        return json.loads(result.stdout)

    def inspect(self, node: Node) -> dict[str, Any]:
        chunks = self.ctl(node, "node", "inspect", node.peer, "--json")
        assert len(chunks) == 1, chunks
        info = chunks[0]
        assert info["node_id"] == node.name
        assert info["has_more"] is False
        return info

    def probe(self, node: Node, endpoint: str) -> dict[str, Any]:
        result = self.ctl(node, "probe", "ping", endpoint)
        assert result["node_id"] == node.name
        assert type(result["timestamp_ms"]) is int
        assert type(result["recv_ms"]) is int
        return result

    def ready(self, node: Node) -> bool:
        assert node.process is not None
        assert node.process.poll() is None, f"{node.name} exited; logs: {self.root}"
        self.probe(node, "client")
        self.probe(node, "peer")
        assert self.inspect(node)["self_member"]["phase"] == "READY"
        return True

    def kv(
        self,
        node: Node,
        operation: str,
        key: str,
        *args: str,
        namespace: str = "journey",
    ) -> Any:
        return self.ctl(
            node,
            operation,
            key,
            *args,
            "--namespace",
            namespace,
            "--namespace-type",
            "str",
            "--json",
        )

    def put(
        self,
        node: Node,
        key: str,
        value: str,
        *,
        namespace: str = "journey",
        kind: str = "str",
        expected: Any = None,
    ) -> Any:
        version = self.kv(
            node, "put", key, value, "--value-type", kind, namespace=namespace
        )
        assert_version(version, key, value if expected is None else expected, namespace)
        return version

    def check(
        self, node: Node, key: str, expected: Any, namespace: str = "journey"
    ) -> Any:
        version = self.kv(node, "get", key, namespace=namespace)
        if expected is None:
            assert version is None, version
        else:
            assert_version(version, key, expected, namespace)
        return version


def assert_version(version: Any, key: str, expected: Any, namespace: str) -> None:
    assert set(version) == {"address", "data", "meta"}
    assert version["address"] == {"key": key, "namespace": namespace}
    assert type(version["data"]) is type(expected)
    try:
        assert version["data"] == expected
    except AssertionError:
        print(f"Assertion failed for version: {version['data']}!={expected}")
        raise
    assert type(version["meta"]["quorum_write"]) is int
    assert version["meta"]["quorum_write"] > 0
    assert isinstance(version["meta"]["ts"]["node_id"], str)
