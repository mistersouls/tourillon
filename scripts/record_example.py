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
#
# /// script
# requires-python = ">=3.14"
# dependencies = ["pillow", "pyyaml", "pyte"]
# ///

"""Record the real Docker Compose example as an animated terminal GIF."""

import argparse
import json
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

import pyte
import yaml
from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "examples" / "docker"


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output", type=Path, default=ROOT / "docs/assets/tourillon.gif"
    )
    parser.add_argument("--font", type=Path, help="Path to a monospace TrueType font.")
    parser.add_argument(
        "--wsl",
        default="Ubuntu" if sys.platform == "win32" else None,
        help="WSL distribution containing Docker (Windows default: Ubuntu).",
    )
    parser.add_argument("--compose", choices=["v1", "v2"], default="v2")
    return parser.parse_args()


def load_font(path: Path | None) -> ImageFont.FreeTypeFont:
    if path is not None:
        return ImageFont.truetype(str(path), 18)
    candidates = [
        Path("C:/Windows/Fonts/consola.ttf"),
        Path("/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf"),
        Path("/System/Library/Fonts/Menlo.ttc"),
    ]
    for candidate in candidates:
        if candidate.is_file():
            return ImageFont.truetype(str(candidate), 18)
    raise RuntimeError("No monospace font found; pass --font PATH.")


class Docker:
    def __init__(self, args: argparse.Namespace) -> None:
        self.prefix = (
            ["wsl", "-d", args.wsl, "--cd", str(EXAMPLE), "--"] if args.wsl else []
        )
        self.compose = (
            ["docker-compose"] if args.compose == "v1" else ["docker", "compose"]
        )

    def run(self, *args: str) -> str:
        result = subprocess.run(
            [*self.prefix, *self.compose, *args],
            cwd=EXAMPLE,
            capture_output=True,
            encoding="utf-8",
            errors="replace",
        )
        output = result.stdout + result.stderr
        if result.returncode:
            raise RuntimeError(
                f"Compose command failed ({result.returncode}):\n{output}"
            )
        return output

    def start(self, *args: str) -> subprocess.Popen:
        process = subprocess.Popen(
            [*self.prefix, *self.compose, *args],
            cwd=EXAMPLE,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )
        assert process.stdin is not None
        process.stdin.reconfigure(newline="\n")
        return process

    def progress(self, recording: "Recording", *args: str) -> None:
        with self.start(*args) as process:
            assert process.stdout is not None
            for line in process.stdout:
                recording.output(line)
            if process.wait():
                raise RuntimeError("Compose startup failed; see recorded output.")

    def config_path(self, path: Path) -> str:
        if not self.prefix:
            return str(path)
        result = subprocess.run(
            [*self.prefix, "wslpath", "-u", path.as_posix()],
            capture_output=True,
            encoding="utf-8",
            check=True,
        )
        return result.stdout.strip()


class Recording:
    def __init__(self, font: ImageFont.FreeTypeFont) -> None:
        self.font = font
        self.frames: list[Image.Image] = []
        self.durations: list[int] = []
        self.screen = pyte.Screen(96, 21)
        self.stream = pyte.Stream(self.screen)

    def frame(self, duration: int) -> None:
        image = Image.new("RGB", (1120, 650), "#111827")
        draw = ImageDraw.Draw(image)
        draw.rounded_rectangle((16, 16, 1104, 634), radius=12, fill="#0b1020")
        draw.text(
            (36, 32),
            "Tourillon | Docker Compose example",
            font=self.font,
            fill="#e2e8f0",
        )
        draw.line((36, 68, 1084, 68), fill="#334155")
        for index, line in enumerate(self.screen.display):
            color = "#67e8f9" if line.startswith(("$ ", "# ")) else "#e2e8f0"
            draw.text((36, 86 + index * 23), line, font=self.font, fill=color)
        cursor = self.screen.cursor
        draw.rectangle(
            (
                36 + cursor.x * self.font.getlength(" "),
                86 + cursor.y * 23 + 18,
                45 + cursor.x * self.font.getlength(" "),
                106 + cursor.y * 23,
            ),
            fill="#67e8f9",
        )
        draw.text(
            (36, 600),
            "Single node | Real CLI output | Waiting times shortened",
            font=self.font,
            fill="#94a3b8",
        )
        self.frames.append(image)
        self.durations.append(duration)

    def type_command(self, command: str, prompt: str = "$ ") -> None:
        self.stream.feed(prompt)
        self.frame(300)
        for character in command:
            self.stream.feed(character)
            self.frame(40)
        self.frame(350)
        self.stream.feed("\r\n")

    def output(self, output: str) -> None:
        for line in output.splitlines(keepends=True):
            self.stream.feed(line.replace("\n", "\r\n"))
            self.frame(120)

    def pause(self, duration: int) -> None:
        self.frame(duration)

    def clear(self) -> None:
        self.stream.feed("\x1b[2J\x1b[H")

    def save(self, destination: Path) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        self.durations[-1] = 5000
        self.frames[0].save(
            destination,
            format="GIF",
            save_all=True,
            append_images=self.frames[1:],
            duration=self.durations,
            loop=0,
            optimize=True,
        )
        with Image.open(destination) as gif:
            if not gif.is_animated or gif.n_frames < 4:
                raise RuntimeError("Generated GIF is not a valid animation.")
        print(f"Saved {destination} ({destination.stat().st_size:,} bytes)")


def wait_for_node(docker: Docker, options: list[str]) -> None:
    for _ in range(30):
        if "Client listener started" in docker.run(
            *options, "logs", "--no-color", "tourillon"
        ):
            return
        time.sleep(1)
    raise RuntimeError("Node did not become ready within 30 seconds.")


def capture(docker: Docker, options: list[str], recording: Recording) -> None:
    command = " ".join(docker.compose)
    recording.type_command(f"{command} up -d")
    docker.progress(recording, *options, "up", "-d")
    wait_for_node(docker, options)
    recording.pause(1500)
    recording.type_command(f"{command} exec -T tourctl sh")
    with docker.start(*options, "exec", "-T", "tourctl", "sh") as shell:
        try:
            capture_shell(shell, recording)
        finally:
            if shell.poll() is None:
                shell.terminate()
            shell.wait()


def shell_command(shell: subprocess.Popen, command: str, recording: Recording) -> str:
    assert shell.stdin is not None and shell.stdout is not None
    marker = f"__tourillon_{uuid.uuid4().hex}__"
    shell.stdin.write(command + f"\nprintf '\\n{marker}%s\\n' \"$?\"\n")
    shell.stdin.flush()
    output = []
    for line in shell.stdout:
        if line.startswith(marker):
            if line.strip() != marker + "0":
                raise RuntimeError(f"Client command failed:\n{''.join(output)}")
            return "".join(output)
        output.append(line)
        recording.output(line)
    raise RuntimeError("Container shell exited before completing the command.")


def capture_shell(shell: subprocess.Popen, recording: Recording) -> None:
    recording.pause(600)
    recording.clear()
    for action in ("put", "get"):
        command = f"tourctl {action} greeting"
        if action == "put":
            command += ' "Hello, Tourillon!"'
        command += " --tourconf ./tourconf.toml"
        recording.type_command(command, "# ")
        output = shell_command(shell, command, recording)
        if "Hello, Tourillon!" not in output:
            raise RuntimeError(f"Unexpected {action} output:\n{output}")
        recording.pause(1800)
    recording.pause(3000)
    recording.type_command("exit", "# ")
    assert shell.stdin is not None
    shell.stdin.write("exit\n")
    shell.stdin.flush()
    if shell.wait(timeout=10):
        raise RuntimeError("Container shell exited with an error.")


def main() -> None:
    args = arguments()
    recording = Recording(load_font(args.font))
    docker = Docker(args)
    project_name = EXAMPLE.name
    docker.run("-p", project_name, "down", "--volumes")
    docker.run("build", "--quiet")
    config = yaml.safe_load(docker.run("-p", project_name, "config"))
    for service in config["services"].values():
        service.pop("ports", None)
        service.pop("build", None)
    with tempfile.TemporaryDirectory(prefix="tourillon-gif-") as directory:
        config_file = Path(directory) / "compose.json"
        config_file.write_text(json.dumps(config), encoding="utf-8")
        options = [
            "-f",
            docker.config_path(config_file),
            "-p",
            project_name,
        ]
        try:
            capture(docker, options, recording)
            recording.save(args.output)
        finally:
            print(docker.run(*options, "down", "--volumes"))


if __name__ == "__main__":
    main()
