#!/usr/bin/env python3
"""Run model-free LocalAI federation checks in a private Docker network.

Python standard library only. Never connects to a public P2P pool.
"""
import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import statistics
import subprocess
import time
import uuid

ROOT = Path(__file__).resolve().parent
HOSTS = [f"localai-peer-{n}" for n in (1, 2, 3)]


def utc():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def command(argv, timeout=30):
    try:
        p = subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
        return {"argv": argv, "exit_code": p.returncode,
                "stdout": p.stdout, "stderr": p.stderr}
    except (OSError, subprocess.TimeoutExpired) as error:
        return {"argv": argv, "exit_code": -1, "stdout": "", "stderr": str(error)}


def resources():
    memory = {}
    for line in Path("/proc/meminfo").read_text().splitlines():
        name, value = line.split(":", 1)
        if name in ("MemTotal", "MemAvailable", "SwapTotal", "SwapFree"):
            memory[name] = int(value.split()[0]) * 1024
    limits = {}
    for name in ("memory.max", "memory.current", "memory.swap.max", "cpu.max"):
        path = Path("/sys/fs/cgroup") / name
        if path.exists():
            limits[name] = path.read_text().strip()
    return {"time": utc(), "memory_bytes": memory, "visible_cgroup": limits,
            "logical_cpus": os.cpu_count(), "workspace_free_bytes": shutil.disk_usage(ROOT).free,
            "tools": {tool: shutil.which(tool) for tool in ("docker", "go", "local-ai")}}


def fresh_hosts(payload, now=None):
    """Separate ledger freshness from presence in the cached API node list."""
    now = now or dt.datetime.now(dt.timezone.utc)
    found = set()
    for node in payload.get("federated_nodes") or []:
        try:
            seen = dt.datetime.fromisoformat(node["LastSeen"].replace("Z", "+00:00"))
            age = (now - seen).total_seconds()
        except (KeyError, TypeError, ValueError):
            continue
        if 0 <= age < 40:
            for hostname in HOSTS:
                if node.get("ID", "").startswith(hostname + "-"):
                    found.add(hostname)
    return sorted(found)


def memory_bytes(value):
    match = re.fullmatch(r"\s*([\d.]+)\s*(B|KiB|MiB|GiB|kB|MB|GB)\s*", value)
    if not match:
        raise ValueError(f"Unknown memory unit: {value}")
    scale = {"B": 1, "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3,
             "kB": 1000, "MB": 1000**2, "GB": 1000**3}
    return int(float(match[1]) * scale[match[2]])


def peer_args(name, hostname, network, image, mib, token=""):
    env = {
        "LOCALAI_P2P_DISABLE_DHT": "true",
        "LOCALAI_P2P_NETWORK_ID": network,
        "LOCALAI_AUTOLOAD_GALLERIES": "false",
        "LOCALAI_AUTOLOAD_BACKEND_GALLERIES": "false",
        "LOCALAI_DISABLE_AGENTS": "true",
        "LOCALAI_DISABLE_MCP": "true",
        "LOCALAI_DISABLE_ASSISTANT": "true",
        "LOCALAI_DISABLE_WEBUI": "true",
        "LOCALAI_DISABLE_RUNTIME_SETTINGS": "true",
        "LOCALAI_MODELS_PATH": "/models",
        "LOCALAI_ADDRESS": ":8080",
    }
    if token:
        env["LOCALAI_P2P_TOKEN"] = token
    args = ["run", "-d", "--name", name, "--hostname", hostname,
            "--network", network, "--label", f"localai-research={network}",
            "--memory", f"{mib}m", "--memory-swap", f"{mib}m", "--cpus", "1",
            "--no-healthcheck"]
    for key, value in env.items():
        args.extend(["-e", f"{key}={value}"])
    return args + [image, "run", "--p2p", "--federated"]


class Baseline:
    def __init__(self, args):
        self.args = args
        self.out = ROOT / "results" / args.label
        self.out.mkdir(parents=True, exist_ok=False)
        self.network = f"localai-lab-{uuid.uuid4().hex[:12]}"
        self.names = [f"{self.network}-p{n}" for n in (1, 2, 3)]
        self.created = []
        self.network_created = False
        self.token = ""
        self.samples = []
        self.summary = {"started": utc(), "status": "preflight", "stages": [],
                        "label": args.label, "network": self.network,
                        "randomness": "LocalAI creates random identities and token; no seed flag",
                        "configuration": vars(args), "resources_before": resources()}

    def save(self, name, value):
        content = json.dumps(value, indent=2) + "\n"
        if self.token:
            content = content.replace(self.token, "<local-token-redacted>")
        (self.out / name).write_text(content)

    def docker(self, args, timeout=30, required=True, secret=False):
        result = command(["docker"] + args, timeout)
        safe = json.loads(json.dumps(result))
        if secret:
            safe["stdout"] = "<token-response-redacted>"
        if self.token:
            safe = json.loads(json.dumps(safe).replace(self.token, "<local-token-redacted>"))
        with (self.out / "commands.jsonl").open("a") as f:
            f.write(json.dumps({"time": utc(), **safe}) + "\n")
        if required and result["exit_code"] != 0:
            raise RuntimeError(result["stderr"].strip() or result["stdout"].strip())
        return result

    def http(self, peer, path, secret=False):
        return self.docker(["exec", self.names[peer], "curl", "--noproxy", "*",
                            "-fsS", "--max-time", "3", "http://127.0.0.1:8080" + path],
                           timeout=10, required=False, secret=secret)

    def start(self, peer, image):
        self.docker(peer_args(self.names[peer], HOSTS[peer], self.network,
                              image, self.args.memory_mib, self.token))
        self.created.append(peer)
        deadline = time.monotonic() + self.args.discovery_timeout
        while time.monotonic() < deadline:
            if self.http(peer, "/readyz")["exit_code"] == 0:
                models = json.loads(self.http(peer, "/v1/models")["stdout"])
                system = json.loads(self.http(peer, "/system")["stdout"])
                self.save(f"p{peer + 1}-model-control.json", {"models": models, "system": system})
                if "data" not in models or "loaded_models" not in system:
                    raise RuntimeError("Cannot verify the empty-model control: unexpected API schema")
                if models["data"] or system["loaded_models"]:
                    raise RuntimeError("Model-free control failed: an installed or loaded model was found")
                return
            state = self.docker(["inspect", "--format", "{{json .State}}", self.names[peer]])
            if not json.loads(state["stdout"])["Running"]:
                raise RuntimeError(f"{HOSTS[peer]} exited before readiness")
            time.sleep(2)
        raise RuntimeError(f"{HOSTS[peer]} not ready within startup timeout")

    def sample(self, phase, peers):
        sample = {"time": utc(), "phase": phase, "peers": {}}
        for peer in peers:
            p = {}
            membership = self.http(peer, "/api/p2p")
            p["membership_probe"] = membership
            if membership["exit_code"] == 0:
                p["membership"] = json.loads(membership["stdout"])
                if "federated_nodes" not in p["membership"]:
                    raise RuntimeError("Unexpected P2P API schema")
                p["fresh_hosts"] = fresh_hosts(p["membership"])
            stats = self.docker(["stats", "--no-stream", "--format", "{{json .}}", self.names[peer]],
                                required=False)
            p["stats_probe"] = stats
            if stats["exit_code"] == 0:
                p["stats"] = json.loads(stats["stdout"])
                p["docker_memory_bytes"] = memory_bytes(p["stats"]["MemUsage"].split("/")[0])
            p["cgroup"] = self.docker(["exec", self.names[peer], "sh", "-c",
                'for f in memory.current memory.peak memory.events; do '
                'echo "$f"; cat "/sys/fs/cgroup/$f"; done'], required=False)
            sample["peers"][HOSTS[peer]] = p
        self.samples.append(sample)
        with (self.out / "samples.jsonl").open("a") as f:
            f.write(json.dumps(sample) + "\n")
        return sample

    def stage(self, name, peers, seconds, converge=True):
        print(f"Stage: {name}", flush=True)
        started = time.monotonic()
        expected = sorted(HOSTS[p] for p in peers)
        first_match = None
        success = False
        duration = self.args.discovery_timeout + seconds if converge else seconds
        deadline = started + duration
        last = None
        while time.monotonic() < deadline:
            last = self.sample(name, peers)
            matched = all(p.get("fresh_hosts") == expected for p in last["peers"].values())
            if matched and first_match is None:
                first_match = time.monotonic() - started
            if matched and (not converge or time.monotonic() - started >= (first_match or 0) + seconds):
                success = True
                if converge:
                    break
            time.sleep(self.args.interval)
        final_match = bool(last) and all(p.get("fresh_hosts") == expected
                                        for p in last["peers"].values())
        success = success and final_match
        self.summary["stages"].append({"name": name, "expected_fresh_hosts": expected,
            "first_match_seconds": first_match, "elapsed_seconds": time.monotonic() - started,
            "final_membership_match": final_match, "passed": success})
        if converge and not success:
            raise RuntimeError(f"Discovery baseline did not converge in stage {name}; inspect logs/mDNS")

    def run(self):
        exit_code = 1
        try:
            source = command(["git", "-C", str(ROOT / "upstream"), "rev-parse", "HEAD"])
            if source["exit_code"]:
                raise RuntimeError("LocalAI source checkout is missing")
            revision = source["stdout"].strip()
            self.summary["source_revision"] = revision
            self.summary["source_worktree"] = command([
                "git", "-C", str(ROOT / "upstream"), "status", "--porcelain"])
            if self.summary["source_worktree"]["stdout"].strip():
                raise RuntimeError("Upstream checkout has local changes; use a clean pinned source control")
            healthcheck = command([
                "bash", str(ROOT / "upstream/scripts/build/healthcheck_test.sh")], timeout=30)
            self.save("source-healthcheck-test.json", healthcheck)
            if healthcheck["exit_code"]:
                raise RuntimeError("Upstream healthcheck regression test failed; inspect its raw output")
            self.summary["docker_preflight"] = self.docker([
                "info", "--format", "{{json .ServerVersion}}"], required=False)
            if self.summary["docker_preflight"]["exit_code"]:
                self.summary["status"] = "blocked_docker_access"
                raise RuntimeError(self.summary["docker_preflight"]["stderr"].strip())
            image = self.args.image or f"localai/localai:sha-{revision[:7]}"
            self.summary["requested_image"] = image
            inspected = self.docker(["image", "inspect", image], required=False)
            if inspected["exit_code"]:
                print(f"Fetching source-matched image: {image}", flush=True)
                self.docker(["pull", image], timeout=600)
                inspected = self.docker(["image", "inspect", image])
            image_info = json.loads(inspected["stdout"])[0]
            self.save("image.json", image_info)
            actual_revision = (image_info["Config"].get("Labels") or {}).get(
                "org.opencontainers.image.revision")
            if actual_revision != revision:
                raise RuntimeError(f"Image revision {actual_revision!r} does not match cloned source {revision}")
            self.summary["image_id"] = image_info["Id"]
            self.docker(["network", "create", "--internal", "--driver", "bridge",
                         "--label", f"localai-research={self.network}", self.network])
            self.network_created = True
            self.save("network.json", json.loads(self.docker([
                "network", "inspect", self.network])["stdout"]))
            self.summary["status"] = "running"
            self.start(0, image_info["Id"])
            token_result = self.http(0, "/api/p2p/token", secret=True)
            if token_result["exit_code"] or not token_result["stdout"].strip():
                raise RuntimeError("First peer did not return its generated network token")
            self.token = token_result["stdout"].strip()
            self.summary["token_sha256"] = hashlib.sha256(self.token.encode()).hexdigest()
            for n in (1, 2, 3):
                if n > 1:
                    self.start(n - 1, image_info["Id"])
                self.stage(f"idle-{n}-peer", list(range(n)), self.args.seconds)
            self.save("p3-before-kill.json", json.loads(self.docker([
                "inspect", "--format", "{{json .State}}", self.names[2]])["stdout"]))
            self.docker(["kill", "--signal", "KILL", self.names[2]])
            self.stage("peer3-crashed", [0, 1], self.args.failure_seconds, converge=False)
            self.save("p3-after-kill.json", json.loads(self.docker([
                "inspect", "--format", "{{json .State}}", self.names[2]])["stdout"]))
            self.docker(["start", self.names[2]])
            self.stage("peer3-rejoined", [0, 1, 2], self.args.seconds)
            self.summary["status"] = "passed" if all(s["passed"] for s in self.summary["stages"]) else "failed"
            exit_code = 0 if self.summary["status"] == "passed" else 1
        except (RuntimeError, ValueError, KeyError, KeyboardInterrupt) as error:
            self.summary["error"] = str(error).replace(self.token, "<token>") if self.token else str(error)
            if self.summary["status"] != "blocked_docker_access":
                self.summary["status"] = "failed"
        finally:
            self.summary["container_final_states"] = {}
            cleanup = []
            for peer in self.created:
                name = self.names[peer]
                logs = self.docker(["logs", "--timestamps", name], required=False)
                self.save(f"p{peer + 1}-logs.json", logs)
                state = self.docker(["inspect", "--format", "{{json .State}}", name], required=False)
                self.summary["container_final_states"][HOSTS[peer]] = state
                cleanup.append(self.docker(["rm", "-f", "-v", name], required=False))
            if self.network_created:
                cleanup.append(self.docker(["network", "rm", self.network], required=False))
            self.summary["cleanup"] = cleanup
            if any(item["exit_code"] for item in cleanup):
                self.summary["status"] = "cleanup_failed"
                exit_code = 1
            self.summary["finished"] = utc()
            self.summary["resources_after"] = resources()
            self.save("summary.json", self.summary)
            self.report()
            print(f"{self.summary['status']}: {self.out}", flush=True)
        return exit_code

    def report(self):
        lines = ["# LocalAI preliminary baseline", "", f"Status: **{self.summary['status']}**.", "",
                 f"Source: `{self.summary.get('source_revision', 'unknown')}`.", "",
                 "No models, inference, WAN delay, DHT discovery or adversarial injection are tested.", ""]
        if self.summary.get("error"):
            lines.extend(["Blocker/error: " + self.summary["error"], ""])
        if self.samples:
            lines += ["| Stage | Peer | Docker memory median MiB | Sampled max MiB | CPU median % |",
                      "| --- | --- | ---: | ---: | ---: |"]
            for phase in dict.fromkeys(s["phase"] for s in self.samples):
                for peer in HOSTS:
                    rows = [s["peers"][peer] for s in self.samples
                            if s["phase"] == phase and peer in s["peers"]
                            and "docker_memory_bytes" in s["peers"][peer]]
                    if rows:
                        memory = [r["docker_memory_bytes"] / 1024**2 for r in rows]
                        cpu = [float(r["stats"]["CPUPerc"].rstrip("%")) for r in rows]
                        lines.append(f"| {phase} | {peer} | {statistics.median(memory):.1f} | "
                                     f"{max(memory):.1f} | {statistics.median(cpu):.2f} |")
            lines += ["", "Memory above is Docker's displayed container usage, not process RSS. Sampling includes startup/discovery; inspect timestamps to select a settled interval. Sampling probes briefly add curl/shell processes. Kernel memory.current, memory.peak and memory.events are retained in samples.jsonl where cgroup v2 is available.", ""]
        else:
            lines += ["**No LocalAI peer started; per-node RAM is unmeasured.**", ""]
        lines += ["Raw evidence: summary.json, commands.jsonl, source-healthcheck-test.json, and (when peers run) samples.jsonl, image.json, network.json and peer logs.", ""]
        (self.out / "REPORT.md").write_text("\n".join(lines))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--label", default="baseline-" + dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S"))
    p.add_argument("--image", help="Override image reference; OCI revision must match the source checkout")
    p.add_argument("--memory-mib", type=int, default=1024)
    p.add_argument("--seconds", type=int, default=60, help="Observation time after initial discovery convergence")
    p.add_argument("--failure-seconds", type=int, default=90)
    p.add_argument("--discovery-timeout", type=int, default=180)
    p.add_argument("--interval", type=int, default=5)
    args = p.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,79}", args.label):
        p.error("label must be a simple unique directory name")
    if min(args.memory_mib, args.seconds, args.failure_seconds, args.discovery_timeout, args.interval) <= 0:
        p.error("resource limits and durations must be positive")
    raise SystemExit(Baseline(args).run())


if __name__ == "__main__":
    main()
