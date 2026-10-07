#!/usr/bin/env python3
"""Prepare/run a private, model-free Exo audit; requires only stdlib on host."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
import uuid

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
SOURCE = ROOT / "exo"
RESULTS = HERE.parent / "results"
SERVICES = ("honest-a", "honest-b")
ARMS = ("baseline", "self", "forged", "stale-session", "replay")


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_json(path: Path, value: object) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def checked(command: list[str], *, env: dict[str, str] | None = None,
            timeout: float = 30) -> str:
    result = subprocess.run(command, env=env, text=True, capture_output=True,
                            timeout=timeout, check=False)
    if result.returncode:
        raise RuntimeError(f"Command failed ({result.returncode}): {command!r}\n"
                           f"{result.stderr[-3000:]}")
    return result.stdout


def source_revision() -> str:
    revision = checked(["git", "-C", str(SOURCE), "rev-parse", "HEAD"]).strip()
    dirty = checked(["git", "-C", str(SOURCE), "status", "--porcelain"])
    if dirty:
        raise RuntimeError("Exo source is modified. Use a clean pinned checkout for this audit.")
    return revision


def available_memory() -> int | None:
    try:
        info = Path("/proc/meminfo").read_text()
        match = re.search(r"^MemAvailable:\s+(\d+) kB$", info, re.M)
        return int(match[1]) * 1024 if match else None
    except OSError:
        return None


def preflight() -> dict:
    blockers = []
    try:
        revision = source_revision()
    except (RuntimeError, OSError, subprocess.SubprocessError) as error:
        revision = None
        blockers.append(str(error))
    docker = shutil.which("docker")
    engine = None
    compose = None
    if not docker:
        blockers.append("Docker executable is unavailable in this session.")
    else:
        try:
            compose = checked([docker, "compose", "version", "--short"]).strip()
            engine = checked([docker, "info", "--format", "{{json .ServerVersion}}"],
                             timeout=15).strip()
        except (RuntimeError, OSError, subprocess.SubprocessError) as error:
            blockers.append(str(error))
    memory = available_memory()
    cgroup = {}
    for name in ("memory.max", "memory.current"):
        try:
            cgroup[name] = (Path("/sys/fs/cgroup") / name).read_text().strip()
        except OSError:
            pass
    return {
        "checked_at": now(), "source_revision": revision,
        "docker": docker, "compose_version": compose, "engine_version": engine,
        "available_host_memory_bytes": memory,
        "initial_available_memory_budget_bytes": 8 * 1024**3,
        "memory_budget_is_unmeasured": True,
        "memory_note": "Budget build and peer runtime separately. Caps are not reservations."
                       " /proc/meminfo may describe the host rather than a session's cgroup.",
        "free_disk_bytes": shutil.disk_usage(ROOT).free,
        "host_cpu_count": os.cpu_count(), "blockers": blockers,
        "session_cgroup_memory": cgroup,
        "live_runtime_available": not blockers,
    }


def create_output(label: str) -> Path:
    if not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,39}", label):
        raise ValueError("Label must be 1–40 lowercase letters/digits/underscores/hyphens.")
    output = RESULTS / label
    output.mkdir(parents=True, exist_ok=False)
    return output


def environment(revision: str, label: str, output: Path, arm: str,
                seed: int, probe_timeout: int) -> dict[str, str]:
    env = os.environ.copy()
    env.update({
        "EXO_REVISION": revision, "EXO_AUDIT_IMAGE": f"exo-audit:{revision}",
        "EXO_NAMESPACE": f"{label}-{arm}", "EXO_AUDIT_ARM": arm,
        "EXO_AUDIT_SEED": str(seed), "EXO_AUDIT_EVIDENCE": str(output),
        "EXO_PROBE_TIMEOUT": str(probe_timeout),
    })
    return env


def validate_readiness(ready: dict, namespace: str, arm: str) -> str:
    from probe import valid_zid
    identity = ready.get("publisher_node_id")
    if (not isinstance(identity, str) or not valid_zid(identity)
            or ready.get("namespace") != namespace or ready.get("arm") != arm
            or ready.get("role") != "router-only"):
        raise RuntimeError("Publisher readiness does not match this arm and namespace.")
    return identity


def summarize(results: list[dict], arms: list[str]) -> dict:
    self_result = next((item for item in results if item["arm"] == "self"), None)
    control_ok = bool(self_result and self_result.get("status") == "observed"
                      and self_result.get("observations", {}).get("outcome") == "accepted")
    interpretable = {"baseline": {"no_send_control"}, "self": {"accepted"},
                     "forged": {"accepted", "not_observed"},
                     "stale-session": {"accepted", "not_observed"},
                     "replay": {"indexed_once", "reindexed"}}
    observed = len(results) == len(arms) and all(
        item["status"] == "observed" and not item.get("capture_error")
        and not item.get("cleanup_error")
        and item.get("observations", {}).get("outcome") in interpretable[item["arm"]]
        for item in results)
    return {"results": results, "positive_control_established": control_ok,
            "run_complete": observed and (arms == ["baseline"] or control_ok),
            "security_vulnerability_validated": False,
            "note": "Metadata acceptance is an observation, not proof of inference impact or a security boundary violation."}


class Compose:
    def __init__(self, project: str, env: dict[str, str], output: Path):
        self.project, self.env, self.output = project, env, output

    def call(self, *arguments: str, timeout: float = 30, check: bool = True) -> str:
        command = ["docker", "compose", "-f", str(HERE / "compose.yaml"),
                   "-p", self.project, *arguments]
        started = time.monotonic()
        result = subprocess.run(command, env=self.env, text=True,
                                capture_output=True, timeout=timeout, check=False)
        record = {"at": now(), "argv": command, "returncode": result.returncode,
                  "duration_s": time.monotonic() - started, "stderr": result.stderr}
        with (self.output / "commands.jsonl").open("a") as stream:
            stream.write(json.dumps(record) + "\n")
        if check and result.returncode:
            raise RuntimeError(f"Compose command failed: {arguments!r}\n{result.stderr[-3000:]}")
        return result.stdout

    def logs(self) -> dict[str, str]:
        return {service: self.call("logs", "--no-color", "--timestamps", service)
                for service in SERVICES}

    def snapshots(self) -> dict[str, dict]:
        snapshots = {}
        for service in SERVICES:
            raw = self.call("exec", "-T", service, "/opt/exo/.venv/bin/python",
                            "/audit/observe.py", "--snapshot", timeout=10)
            snapshots[service] = json.loads(raw)
        return snapshots

    def sample_resources(self) -> None:
        ids = self.call("ps", "-q").split()
        if not ids:
            return
        result = subprocess.run(["docker", "stats", "--no-stream", "--format",
                                 "{{json .}}", *ids], env=self.env, text=True,
                                capture_output=True, timeout=10, check=False)
        with (self.output / "docker-stats.jsonl").open("a") as stream:
            stream.write(json.dumps({"at": now(), "returncode": result.returncode,
                                     "raw": result.stdout, "stderr": result.stderr}) + "\n")

    def capture(self) -> None:
        for service in (*SERVICES, "attacker"):
            raw = self.call("logs", "--no-color", "--timestamps", service, check=False)
            (self.output / f"{service}.log").write_text(raw)
        raw = self.call("ps", "-a", "--format", "json", check=False)
        (self.output / "containers.json").write_text(raw)
        ids = self.call("ps", "-aq", check=False).split()
        if ids:
            inspected = checked(["docker", "inspect", *ids], env=self.env)
            (self.output / "container-inspect.json").write_text(inspected)
        # Linux cgroup peaks complement the sampled Docker statistics when supported.
        for service in (*SERVICES, "attacker"):
            raw = self.call("exec", "-T", service, "/opt/exo/.venv/bin/python", "-c",
                           "from pathlib import Path; import json; "
                           "print(json.dumps({n:(Path('/sys/fs/cgroup')/n).read_text().strip() "
                           "for n in ['memory.current','memory.peak','memory.events'] "
                           "if (Path('/sys/fs/cgroup')/n).exists()}))", check=False)
            (self.output / f"{service}-cgroup.json").write_text(raw)


def wait_consensus(compose: Compose, timeout: float) -> dict:
    from observe import session_consensus
    deadline = time.monotonic() + timeout
    stable, previous = 0, None
    last = None
    while time.monotonic() < deadline:
        logs = compose.logs()
        report = session_consensus(logs)
        last = report
        with (compose.output / "convergence.jsonl").open("a") as stream:
            stream.write(json.dumps({"at": now(), **report}) + "\n")
        key = json.dumps([report.get("node_ids"), report.get("session")], sort_keys=True)
        if report.get("converged"):
            stable = stable + 1 if key == previous else 1
            if stable >= 3:
                try:
                    snapshots = compose.snapshots()
                    if all(snapshots[name]["node_id"] == report["node_ids"][name]
                           for name in SERVICES):
                        return report
                except (RuntimeError, ValueError, KeyError, OSError,
                        subprocess.SubprocessError) as error:
                    # An API can start after election logs; retry within the same deadline.
                    with (compose.output / "convergence.jsonl").open("a") as stream:
                        stream.write(json.dumps({"at": now(), "snapshot_error": str(error)}) + "\n")
        else:
            stable = 0
        previous = key
        compose.sample_resources()
        time.sleep(1)
    raise RuntimeError(f"Honest peers did not establish stable consensus: {last}")


def wait_file(path: Path, compose: Compose, timeout: float) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return json.loads(path.read_text())
        compose.sample_resources()
        time.sleep(0.5)
    raise RuntimeError(f"Timed out waiting for {path.name}; inspect preserved peer logs.")


def manifest(revision: str, options: argparse.Namespace) -> dict:
    files = [HERE / "compose.yaml", HERE / "run.py", HERE / "observe.py", HERE / "probe.py",
             HERE.parent / "two-peer/Dockerfile", HERE.parent / "two-peer/Dockerfile.dockerignore"]
    return {"created_at": now(), "source_revision": revision,
            "settings": vars(options), "lab_file_sha256": {
                str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
                for path in files},
            "scope": "Private Docker bridge, two honest coordinator nodes, one bounded Router publisher.",
            "models_loaded": False, "synthetic_metadata_only": True,
            "security_vulnerability_validated": False}


def run_arm(output: Path, revision: str, label: str, arm: str, seed: int,
            startup_timeout: int) -> dict:
    from observe import assess_probe
    output.mkdir()
    env = environment(revision, label, output, arm, seed, startup_timeout + 180)
    compose = Compose(f"{label}-{arm}", env, output)
    result = {"arm": arm, "status": "incomplete"}
    started = False
    try:
        (output / "compose-resolved.yaml").write_text(compose.call("config"))
        # Refuse to reuse an existing named project or its persistent peer state.
        if compose.call("ps", "-aq").strip():
            raise RuntimeError("Project already contains containers; choose a fresh label.")
        volumes = checked(["docker", "volume", "ls", "-q", "--filter",
                           f"label=com.docker.compose.project={compose.project}"], env=env)
        if volumes.strip():
            raise RuntimeError("Project already contains state volumes; choose a fresh label.")
        started = True  # Also clean up a partially successful 'up'.
        compose.call("up", "-d", "--no-build", *SERVICES, timeout=60)
        initial = wait_consensus(compose, startup_timeout)
        write_json(output / "initial-consensus.json", initial)
        compose.call("up", "-d", "--no-build", "attacker", timeout=60)
        ready = wait_file(output / "attacker-ready.json", compose, startup_timeout)
        publisher_id = validate_readiness(ready, env["EXO_NAMESPACE"], arm)
        consensus = wait_consensus(compose, startup_timeout)
        if publisher_id in consensus["node_ids"].values():
            raise RuntimeError("Attacker and honest identities collide.")
        before = compose.snapshots()
        if any(before[name]["node_id"] != consensus["node_ids"][name] for name in SERVICES):
            raise RuntimeError("Honest identities changed before publication.")
        write_json(output / "before.json", before)
        target = (consensus["node_ids"]["honest-b"] if arm == "forged"
                  else publisher_id)
        control = {
            "arm": arm, "seed": seed, "session": consensus["session"],
            "node_ids": consensus["node_ids"], "target_node_id": target,
            "available_bytes": 128 * 1024**2 + (seed % 4096) * 1024,
            "origin": str(uuid.uuid5(uuid.NAMESPACE_URL, f"exo-audit:{label}:{arm}:{seed}")),
            "origin_idx": 0,
        }
        write_json(output / "control.json", control)
        done = wait_file(output / "probe-done.json", compose, 30)
        if done.get("type") != "done" or done.get("status") == "error":
            raise RuntimeError(f"Publisher failed: {done}")
        expected_count = 0 if arm == "baseline" else 2 if arm == "replay" else 1
        if done.get("publications") != expected_count:
            raise RuntimeError(f"Expected {expected_count} publications; observed {done.get('publications')}.")
        after = compose.snapshots()
        write_json(output / "after.json", after)
        final_consensus = wait_consensus(compose, 15)
        write_json(output / "final-consensus.json", final_consensus)
        records = [json.loads(line) for line in (output / "probe.jsonl").read_text().splitlines()]
        before_states = {name: item["state"] for name, item in before.items()}
        after_states = {name: item["state"] for name, item in after.items()}
        observations = assess_probe(arm, records, before_states, after_states)
        same_session = final_consensus["session"] == consensus["session"]
        same_ids = final_consensus["node_ids"] == consensus["node_ids"] and all(
            after[name]["node_id"] == consensus["node_ids"][name] for name in SERVICES)
        result.update({"status": "observed" if same_session and same_ids else "inconclusive",
                       "observations": observations, "probe_done": done,
                       "session_unchanged": same_session, "honest_identities_unchanged": same_ids,
                       "security_vulnerability_validated": False})
    except (RuntimeError, ValueError, KeyError, OSError, subprocess.SubprocessError) as error:
        result.update({"status": "incomplete", "error": str(error)})
    finally:
        if started:
            try:
                compose.sample_resources()
                compose.capture()
            except (RuntimeError, OSError, subprocess.SubprocessError) as error:
                result["capture_error"] = str(error)
            try:
                compose.call("down", "--timeout", "10", timeout=45)
            except (RuntimeError, OSError, subprocess.SubprocessError) as error:
                result["cleanup_error"] = str(error)
        write_json(output / "result.json", result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("preflight")
    build = commands.add_parser("build")
    build.add_argument("--label", required=True)
    run = commands.add_parser("run")
    run.add_argument("--label", required=True)
    run.add_argument("--arms", default=",".join(ARMS))
    run.add_argument("--seed", type=int, default=1901)
    run.add_argument("--startup-timeout", type=int, default=90)
    options = parser.parse_args()
    if options.command == "run":
        arms = options.arms.split(",")
        if not arms or len(set(arms)) != len(arms) or any(arm not in ARMS for arm in arms):
            parser.error(f"Choose distinct arms from {ARMS}.")
        if arms != ["baseline"] and "self" not in arms:
            parser.error("Include the self positive control in any probe run.")
        if options.startup_timeout < 10 or options.startup_timeout > 300:
            parser.error("Startup timeout must be 10–300 seconds.")
        if not -(2**31) <= options.seed < 2**31:
            parser.error("Seed must fit a signed 32-bit integer.")
    report = preflight()
    if options.command == "preflight":
        print(json.dumps(report, indent=2))
        return 0 if report["live_runtime_available"] else 2
    output = create_output(options.label)
    write_json(output / "preflight.json", report)
    if report["blockers"]:
        print(json.dumps(report, indent=2))
        print(f"Preflight failed; saved {output / 'preflight.json'}", file=sys.stderr)
        return 2
    revision = report["source_revision"]
    run_manifest = manifest(revision, options)
    write_json(output / "manifest.json", run_manifest)
    for relative in run_manifest["lab_file_sha256"]:
        destination = output / "lab-source" / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / relative, destination)
    if options.command == "build":
        env = environment(revision, options.label, output, "baseline", 1901, 180)
        command = ["docker", "compose", "-f", str(HERE / "compose.yaml"), "-p",
                   f"{options.label}-build", "build", "honest-a"]
        print(f"Building exo-audit:{revision}; log: {output / 'build.log'}", flush=True)
        with (output / "build.log").open("w") as stream:
            result = subprocess.run(command, env=env, stdout=stream, stderr=subprocess.STDOUT,
                                    check=False)
        write_json(output / "build-result.json", {"returncode": result.returncode, "at": now()})
        return result.returncode
    image = checked(["docker", "image", "inspect", f"exo-audit:{revision}"])
    (output / "image.json").write_text(image)
    image_data = json.loads(image)[0]
    image_revision = (image_data["Config"].get("Labels") or {}).get("org.opencontainers.image.revision")
    if image_revision != revision:
        raise RuntimeError("Image revision does not match source; build with this runner first.")
    results = []
    for arm in arms:
        print(f"Running {arm}; evidence: {output / arm}", flush=True)
        result = run_arm(output / arm, revision, options.label, arm, options.seed,
                         options.startup_timeout)
        results.append(result)
        print(json.dumps(result), flush=True)
        if result["status"] == "incomplete":
            break
    summary = summarize(results, arms)
    write_json(output / "summary.json", summary)
    return 0 if summary["run_complete"] else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, RuntimeError, OSError, subprocess.SubprocessError) as error:
        print(f"Audit setup error: {error}", file=sys.stderr)
        raise SystemExit(2)
