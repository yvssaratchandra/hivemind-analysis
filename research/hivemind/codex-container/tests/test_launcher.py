"""Integration tests for run.sh using temporary trees and fake host utilities.

The Docker socket fixture is an ordinary temporary file. The fake stat
utility reports that path as a socket to exercise launcher control flow; the
fixture is not a Unix socket and no Docker or Podman daemon is contacted.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


RUN_SH = Path(__file__).resolve().parents[1] / "run.sh"
ISOLATED_IMAGE = "localhost/hivemind-codex:latest"
DOCKER_IMAGE = "localhost/hivemind-codex-docker:latest"


class LauncherIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="codex launcher tests ")
        self.addCleanup(self.temp.cleanup)
        self.temp_root = Path(self.temp.name)

        # A path with spaces checks that the launcher keeps bind sources and
        # command arguments as distinct argv entries.
        self.repo = self.temp_root / "repository checkout with spaces"
        self.container_dir = (
            self.repo / "research" / "hivemind" / "codex-container"
        )
        self.container_dir.mkdir(parents=True)
        self.launcher = self.container_dir / "run.sh"
        shutil.copy2(RUN_SH, self.launcher)
        (self.container_dir / "Containerfile").write_text(
            "# Fake build context for launcher argv tests.\n", encoding="utf-8"
        )

        self.fake_bin = self.temp_root / "fake host utilities"
        self.fake_bin.mkdir()
        self.podman_log = self.temp_root / "podman.jsonl"
        self.stat_log = self.temp_root / "stat.jsonl"
        self.socket_marker = self.temp_root / "docker socket marker"
        self.socket_marker.write_text(
            "ordinary file reported as a socket by fake stat\n", encoding="utf-8"
        )
        self.home = self.temp_root / "fake home"
        self.home.mkdir()

        python = sys.executable
        self.write_utility(
            "dirname",
            f"""#!{python}
import os
import sys

path = sys.argv[-1]
print(os.path.dirname(os.path.normpath(path)) or ".")
""",
        )
        self.write_utility(
            "id",
            f"""#!{python}
import sys

if sys.argv[1:] == ["-u"]:
    print("1000")
else:
    print("1000")
""",
        )
        self.write_utility(
            "podman",
            f"""#!{python}
import json
import os
import sys

args = sys.argv[1:]
record = {{
    "argv": args,
    "docker_host": os.environ.get("DOCKER_HOST"),
}}
with open(os.environ["PODMAN_LOG"], "a", encoding="utf-8") as output:
    output.write(json.dumps(record) + "\\n")

if args[:2] == ["image", "exists"]:
    raise SystemExit(0 if os.environ.get("PODMAN_IMAGE_EXISTS", "1") == "1" else 1)
""",
        )
        self.write_utility(
            "stat",
            f"""#!{python}
import json
import os
import sys

args = sys.argv[1:]
record = {{"argv": args}}
with open(os.environ["STAT_LOG"], "a", encoding="utf-8") as output:
    output.write(json.dumps(record) + "\\n")

socket_path = os.environ.get("MOCK_SOCKET_PATH")
target = args[-1] if args else ""
kind = os.environ.get("MOCK_STAT_TYPE", "socket")
if target != socket_path or kind == "missing":
    raise SystemExit(1)
print(kind)
""",
        )
        self.install_crun()

    def write_utility(self, name: str, contents: str) -> Path:
        path = self.fake_bin / name
        path.write_text(contents, encoding="utf-8")
        path.chmod(0o755)
        return path

    def install_crun(self) -> None:
        self.write_utility(
            "crun",
            f"""#!{sys.executable}
raise SystemExit(0)
""",
        )

    def clear_capture(self) -> None:
        self.podman_log.write_text("", encoding="utf-8")
        self.stat_log.write_text("", encoding="utf-8")

    def run_launcher(
        self,
        *args: str,
        docker_socket: str | None = None,
        stat_type: str = "socket",
        with_crun: bool = True,
        podman_image_exists: bool = True,
        launcher_path: Path | None = None,
        repo_path: Path | None = None,
    ) -> subprocess.CompletedProcess[str]:
        launch_path = launcher_path or self.launcher
        working_repo = repo_path or self.repo
        env = {
            "PATH": str(self.fake_bin),
            "HOME": str(self.home),
            "PODMAN_LOG": str(self.podman_log),
            "PODMAN_IMAGE_EXISTS": "1" if podman_image_exists else "0",
            "STAT_LOG": str(self.stat_log),
            "MOCK_SOCKET_PATH": docker_socket or str(self.socket_marker),
            "MOCK_STAT_TYPE": stat_type,
            "CODEX_DOCKER_SOCKET": docker_socket or str(self.socket_marker),
        }
        if not with_crun:
            crun = self.fake_bin / "crun"
            if crun.exists():
                crun.unlink()

        return subprocess.run(
            ["/bin/bash", str(launch_path), *args],
            cwd=working_repo,
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )

    def podman_calls(self) -> list[dict[str, object]]:
        if not self.podman_log.exists():
            return []
        content = self.podman_log.read_text(encoding="utf-8")
        return [json.loads(line) for line in content.splitlines() if line]

    def stat_calls(self) -> list[dict[str, object]]:
        if not self.stat_log.exists():
            return []
        content = self.stat_log.read_text(encoding="utf-8")
        return [json.loads(line) for line in content.splitlines() if line]

    @staticmethod
    def run_call(calls: list[dict[str, object]]) -> list[str]:
        run_calls = [
            call["argv"]
            for call in calls
            if isinstance(call["argv"], list) and call["argv"][:1] == ["run"]
        ]
        if len(run_calls) != 1:
            raise AssertionError(f"expected one Podman run call, got {run_calls!r}")
        return run_calls[0]

    @staticmethod
    def option_values(argv: list[str], option: str) -> list[str]:
        values = []
        for index, arg in enumerate(argv):
            if arg == option and index + 1 < len(argv):
                values.append(argv[index + 1])
            elif arg.startswith(option + "="):
                values.append(arg[len(option) + 1 :])
        return values

    def test_isolated_command_keeps_original_image_mounts_and_security(self) -> None:
        command = [
            "printf",
            "%s\\n",
            "hello with spaces",
            "$literal",
            chr(96) + "ticks" + chr(96),
        ]
        result = self.run_launcher("command", *command)
        self.assertEqual(result.returncode, 0, result.stderr)

        calls = self.podman_calls()
        self.assertEqual(
            calls[0]["argv"][:2],
            ["image", "exists"],
        )
        self.assertEqual(calls[1]["argv"][:1], ["run"])
        argv = self.run_call(calls)
        self.assertIn(ISOLATED_IMAGE, argv)
        image_index = argv.index(ISOLATED_IMAGE)
        self.assertEqual(argv[image_index + 1 :], command)
        self.assertEqual(self.option_values(argv, "--workdir"), ["/workspace"])
        self.assertEqual(
            self.option_values(argv, "--volume"),
            [
                f"{self.repo.resolve()}:/workspace:rw,Z",
                "hivemind-codex-home:/home/node:U",
            ],
        )
        self.assertIn("--cap-drop=all", argv)
        self.assertIn("--security-opt=no-new-privileges", argv)
        self.assertIn("--userns=keep-id:uid=1000,gid=1000", argv)
        self.assertIn("--user=1000:1000", argv)
        self.assertNotIn("--runtime", argv)
        self.assertNotIn("--group-add", argv)
        self.assertFalse(any(arg.startswith("DOCKER_HOST=") for arg in argv))
        self.assertEqual(self.stat_calls(), [])
        self.assertFalse(any(arg.startswith("--network=host") for arg in argv))
        self.assertNotIn("--privileged", argv)
        self.assertNotIn("--pid=host", argv)

    def test_legacy_login_shell_and_default_codex_arguments_are_preserved(self) -> None:
        cases = [
            (
                ("login", "--device", "literal $HOME"),
                ["codex", "login", "--device-auth", "--device", "literal $HOME"],
            ),
            (
                ("shell", "-l", "-c", "printf '%s'"),
                ["bash", "-l", "-c", "printf '%s'"],
            ),
            (
                ("--model", "example", "argument with spaces"),
                [
                    "codex",
                    "--no-daemon",
                    "--sandbox",
                    "danger-full-access",
                    "--ask-for-approval",
                    "never",
                    "--model",
                    "example",
                    "argument with spaces",
                ],
            ),
        ]

        for launcher_args, expected_command in cases:
            with self.subTest(launcher_args=launcher_args):
                self.clear_capture()
                result = self.run_launcher(*launcher_args)
                self.assertEqual(result.returncode, 0, result.stderr)
                argv = self.run_call(self.podman_calls())
                image_index = argv.index(ISOLATED_IMAGE)
                self.assertEqual(argv[image_index + 1 :], expected_command)

    def test_docker_command_mounts_socket_and_repository_and_wraps_command(self) -> None:
        command = [
            "docker",
            "compose",
            "-f",
            str(self.repo / "compose file.yaml"),
            "run",
            "worker",
            "$literal",
            chr(96) + "ticks" + chr(96),
        ]
        result = self.run_launcher("--docker", "command", *command)
        self.assertEqual(result.returncode, 0, result.stderr)

        calls = self.podman_calls()
        argv = self.run_call(calls)
        self.assertIn(DOCKER_IMAGE, argv)
        image_index = argv.index(DOCKER_IMAGE)
        wrapped = argv[image_index + 1 :]
        self.assertEqual(wrapped[:2], ["bash", "-c"])
        self.assertIn("docker info --format", wrapped[2])
        self.assertIn('exec "$@"', wrapped[2])
        self.assertEqual(wrapped[3], "docker-check")
        self.assertEqual(wrapped[4:], command)

        self.assertEqual(self.option_values(argv, "--runtime"), ["crun"])
        self.assertEqual(self.option_values(argv, "--group-add"), ["keep-groups"])
        self.assertEqual(
            self.option_values(argv, "--env"),
            ["HOME=/home/node", "DOCKER_HOST=unix:///var/run/docker.sock"],
        )
        self.assertEqual(
            self.option_values(argv, "--workdir"), [str(self.repo.resolve())]
        )
        self.assertEqual(
            self.option_values(argv, "--volume"),
            [
                f"{self.repo.resolve()}:/workspace:rw,Z",
                "hivemind-codex-home:/home/node:U",
                f"{self.socket_marker}:/var/run/docker.sock:rw",
                f"{self.repo.resolve()}:{self.repo.resolve()}:rw,Z",
            ],
        )
        self.assertIn("--cap-drop=all", argv)
        self.assertIn("--security-opt=no-new-privileges", argv)
        self.assertNotIn("--privileged", argv)
        self.assertNotIn("--network=host", argv)
        self.assertNotIn("--pid=host", argv)
        self.assertEqual(self.stat_calls()[0]["argv"][-1], str(self.socket_marker))

    def test_docker_checkout_nested_under_workspace_uses_one_repository_mount(self) -> None:
        # This exercises the special case where /workspace is the parent of
        # the checkout. Podman only records argv, so no host paths are mounted.
        if not Path("/workspace").is_dir() or not os.access("/workspace", os.W_OK):
            self.skipTest("nested-path case requires a writable /workspace")
        with tempfile.TemporaryDirectory(
            prefix="codex launcher nested checkout ", dir="/workspace"
        ) as temp_dir:
            repo = Path(temp_dir) / "nested checkout"
            container_dir = repo / "research" / "hivemind" / "codex-container"
            container_dir.mkdir(parents=True)
            launcher = container_dir / "run.sh"
            shutil.copy2(RUN_SH, launcher)
            (container_dir / "Containerfile").write_text(
                "# Fake nested build context.\n", encoding="utf-8"
            )

            result = self.run_launcher(
                "--docker",
                "command",
                "docker",
                "compose",
                "config",
                docker_socket=str(self.socket_marker),
                launcher_path=launcher,
                repo_path=repo,
            )
            self.assertEqual(result.returncode, 0, result.stderr)

            argv = self.run_call(self.podman_calls())
            volumes = self.option_values(argv, "--volume")
            canonical_repo = str(repo.resolve())
            self.assertEqual(
                [volume for volume in volumes if volume.startswith(canonical_repo + ":")],
                [f"{canonical_repo}:{canonical_repo}:rw,Z"],
            )
            self.assertEqual(self.option_values(argv, "--workdir"), [canonical_repo])

    def test_docker_rejects_relative_socket_path_before_podman(self) -> None:
        result = self.run_launcher(
            "--docker", "command", "docker", "ps", docker_socket="relative/docker.sock"
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("absolute", result.stderr.lower())
        self.assertEqual(self.podman_calls(), [])
        self.assertEqual(self.stat_calls(), [])

    def test_docker_rejects_missing_and_non_socket_paths(self) -> None:
        missing_socket = str(self.temp_root / "missing docker socket")
        cases = [
            (missing_socket, "missing"),
            (str(self.socket_marker), "regular file"),
        ]
        for socket_path, stat_type in cases:
            with self.subTest(socket_path=socket_path, stat_type=stat_type):
                self.clear_capture()
                result = self.run_launcher(
                    "--docker",
                    "command",
                    "docker",
                    "ps",
                    docker_socket=socket_path,
                    stat_type=stat_type,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("docker socket", result.stderr.lower())
                self.assertFalse(
                    any(call["argv"][:1] == ["run"] for call in self.podman_calls())
                )

    def test_docker_rejects_socket_marker_without_write_permission(self) -> None:
        original_mode = self.socket_marker.stat().st_mode & 0o777
        try:
            self.socket_marker.chmod(0o400)
            if os.access(self.socket_marker, os.W_OK):
                self.skipTest("test process can still write a read-only marker")

            result = self.run_launcher(
                "--docker",
                "command",
                "docker",
                "ps",
                docker_socket=str(self.socket_marker),
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("inaccessible", result.stderr.lower())
            self.assertEqual(self.podman_calls(), [])
        finally:
            self.socket_marker.chmod(original_mode)

    def test_docker_requires_crun(self) -> None:
        result = self.run_launcher(
            "--docker",
            "command",
            "docker",
            "ps",
            with_crun=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("crun", result.stderr)
        self.assertEqual(self.podman_calls(), [])

    def test_docker_build_selects_client_image_without_checking_socket(self) -> None:
        result = self.run_launcher(
            "--docker",
            "build",
            docker_socket="relative path proves the build skips socket validation",
            podman_image_exists=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = self.podman_calls()
        self.assertEqual(len(calls), 1)
        build_argv = calls[0]["argv"]
        self.assertEqual(build_argv[:2], ["build", "--tag"])
        self.assertEqual(build_argv[2], DOCKER_IMAGE)
        self.assertIn("--file", build_argv)
        self.assertEqual(build_argv[-1], str(self.container_dir.resolve()))
        self.assertEqual(self.stat_calls(), [])

    def test_command_without_program_shows_usage_without_starting_podman(self) -> None:
        result = self.run_launcher("--docker", "command")
        self.assertEqual(result.returncode, 2)
        self.assertIn("Usage:", result.stderr)
        self.assertEqual(self.podman_calls(), [])
        self.assertEqual(self.stat_calls(), [])


if __name__ == "__main__":
    unittest.main()
