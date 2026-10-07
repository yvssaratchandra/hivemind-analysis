#!/usr/bin/env python3
"""Capture model-free LocalAI control messages in a private Docker LAN.

Uses an endpoint observer for decrypted EdgeVPN envelopes, not a public swarm.
No host mounts, published ports, host network, model download or inference.
"""
import argparse
import codecs
from concurrent.futures import ThreadPoolExecutor
import datetime as dt
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import shutil
import socket
import struct
import threading
import time
import uuid
from urllib.parse import quote, urlencode

from run_baseline import command, memory_bytes, peer_args, resources, utc

ROOT = Path(__file__).resolve().parent
MIB = 1024 ** 2


def controller_pids():
    """Controller cgroup limits; Docker's sibling containers are separate."""
    values = {}
    for filename in ("pids.max", "pids.current", "pids.events"):
        path = Path("/sys/fs/cgroup") / filename
        if path.exists():
            values[filename] = path.read_text().strip()
    return values


def docker_log_frames(response):
    """Docker non-TTY stdout/stderr multiplex frames, after HTTP dechunking."""
    while True:
        header = response.read(8)
        if not header:
            return
        if len(header) != 8:
            raise ValueError("Truncated Docker log frame header")
        channel, length = header[0], struct.unpack(">I", header[4:])[0]
        if channel not in (1, 2) or header[1:4] != b"\0\0\0" or length > 64 * MIB:
            raise ValueError("Invalid or oversized Docker log frame")
        data = response.read(length)
        if len(data) != length:
            raise ValueError("Truncated Docker log frame payload")
        yield channel, data


def docker_log_since(timestamp):
    """The logs REST API takes Unix seconds; the CLI converts RFC3339 itself."""
    return f"{dt.datetime.fromisoformat(timestamp.replace('Z', '+00:00')).timestamp():.6f}"


class UnixDockerConnection(http.client.HTTPConnection):
    def __init__(self, path):
        super().__init__("localhost", timeout=15)
        self.path = path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self.path)


def choose_peer_count(available, limit_mib, requested=0, reserve_mib=4096):
    """Use bounded container ceilings, including gateway and observer overhead."""
    budget = available - (reserve_mib + 768) * MIB
    maximum = min(64, max(0, budget // (limit_mib * MIB)))
    if requested:
        maximum = min(maximum, requested)
    if maximum < 2:
        raise RuntimeError("Insufficient free RAM for two capped peers and the reserve")
    return int(maximum)


def fresh_hostnames(payload, hosts, now=None):
    now = now or dt.datetime.now(dt.timezone.utc)
    fresh = set()
    for item in payload.get("federated_nodes") or []:
        try:
            seen = dt.datetime.fromisoformat(item["LastSeen"].replace("Z", "+00:00"))
            age = (now - seen).total_seconds()
        except (KeyError, TypeError, ValueError):
            continue
        if 0 <= age < 40:
            for host in hosts:
                if item.get("ID", "").startswith(host + "-"):
                    fresh.add(host)
                    break
    return sorted(fresh)


def control_peer_args(name, hostname, network, image, mib, token, cpus):
    args = peer_args(name, hostname, network, image, mib, token)
    args[args.index("--cpus") + 1] = str(cpus)
    position = args.index(image)
    args[position:position] = ["-e", "LOCALAI_P2P_LOGLEVEL=debug",
                              "-e", "LOCALAI_P2P_LIB_LOGLEVEL=info",
                              "--log-opt", "max-size=20m", "--log-opt", "max-file=2"]
    return args + ["--debug"]


def gateway_args(name, network, image, mib, token):
    args = peer_args(name, "control-gateway", network, image, mib, token)
    args[-3:] = ["federated"]
    position = args.index(image)
    args[position:position] = ["-e", f"TOKEN={token}"]
    return args


class ControlCapture:
    def __init__(self, args):
        self.args = args
        self.out = ROOT / "results" / args.label
        self.out.mkdir(parents=True, exist_ok=False)
        self.network = "localai-control-" + uuid.uuid4().hex[:12]
        self.hosts = [f"control-peer-{n:02}" for n in range(1, 65)]
        self.names = [f"{self.network}-p{n:02}" for n in range(1, 65)]
        self.observer = self.network + "-observer"
        self.gateway = self.network + "-gateway"
        self.token = ""
        self.created = []
        self.streams = []
        self.capture_errors = []
        self.lock = threading.Lock()
        self.phase = "preflight"
        self.network_created = False
        self.observer_started = False
        self.gateway_started = False
        self.summary = {
            "started": utc(), "status": "preflight", "label": args.label,
            "network": self.network, "configuration": vars(args),
            "resources_before": resources(), "stages": [],
            "controller_pids_before": controller_pids(),
            "randomness": "Fresh token and random peer/service identities; no seed interface",
            "scope": "Model-free LocalAI federation, private LAN mDNS, diagnostic observer",
            "observer_effect": "One extra libp2p peer; ledger sync enabled, no service or Alive announcement",
        }

    def redact(self, value):
        return value.replace(self.token, "<local-token-redacted>") if self.token else value

    def append(self, name, record):
        with self.lock, (self.out / name).open("a") as f:
            f.write(self.redact(json.dumps(record)) + "\n")

    def save(self, name, record):
        (self.out / name).write_text(self.redact(json.dumps(record, indent=2)) + "\n")

    def event(self, phase, action, **extra):
        self.append("phases.jsonl", {"timestamp": utc(), "phase": phase,
                                     "action": action, **extra})

    def docker(self, args, timeout=40, required=True, secret=False):
        result = command(["docker"] + args, timeout)
        safe = dict(result)
        if secret:
            safe["stdout"] = "<token-response-redacted>"
        self.append("commands.jsonl", {"timestamp": utc(), **safe})
        if required and result["exit_code"]:
            raise RuntimeError(self.redact(result["stderr"] or result["stdout"]))
        return result

    def http(self, name, path, secret=False, timeout=6):
        return self.docker(["exec", name, "curl", "--noproxy", "*", "-fsS",
                            "--max-time", str(timeout), "http://127.0.0.1:8080" + path],
                           timeout=timeout + 10, required=False, secret=secret)

    def follow(self, name, filename, structured=False, since=None):
        """Stream the Docker socket directly: one thread, no CLI per peer."""
        query = {"follow": "true", "stdout": "true", "stderr": "true",
                 "timestamps": "false" if structured else "true"}
        if since:
            query["since"] = docker_log_since(since)
        connection = UnixDockerConnection(self.docker_socket)
        connection.request("GET", f"/containers/{quote(name, safe='')}/logs?{urlencode(query)}")
        wire_socket = connection.sock
        response = connection.getresponse()
        if response.status != 200:
            error = response.read(512).decode(errors="replace")
            connection.close()
            raise RuntimeError(f"Docker log stream failed ({response.status}): {error}")
        wire_socket.settimeout(None)
        stopping = threading.Event()

        def collect():
            destinations = {1: (self.out / filename).open("a")}
            destinations[2] = (self.out / "observer.stderr.log").open("a") if structured else destinations[1]
            decoders = {n: codecs.getincrementaldecoder("utf-8")("replace") for n in (1, 2)}
            pending = {1: "", 2: ""}
            try:
                for channel, data in docker_log_frames(response):
                    pending[channel] += decoders[channel].decode(data)
                    while "\n" in pending[channel]:
                        line, _, pending[channel] = pending[channel].partition("\n")
                        destinations[channel].write(self.redact(line + "\n"))
                    destinations[channel].flush()
            except Exception as error:
                if not stopping.is_set():
                    detail = {"container": name, "error": str(error)}
                    self.capture_errors.append(detail)
                    self.event(self.phase, "log_capture_error", **detail)
            finally:
                for channel in (1, 2):
                    tail = pending[channel] + decoders[channel].decode(b"", final=True)
                    if tail:
                        destinations[channel].write(self.redact(tail))
                for destination in set(destinations.values()):
                    destination.close()
                response.close()
                connection.close()

        thread = threading.Thread(target=collect, daemon=True)
        thread.start()
        self.streams.append((name, wire_socket, thread, stopping))
        self.event(self.phase, "log_capture_started", container=name, file=filename,
                   transport="Docker Unix socket HTTP logs; no extra CLI process")

    def wait_ready(self, name):
        deadline = time.monotonic() + self.args.discovery_timeout
        while time.monotonic() < deadline:
            if self.http(name, "/readyz")["exit_code"] == 0:
                return
            state = self.docker(["inspect", "--format", "{{json .State}}", name])
            data = json.loads(state["stdout"])
            if not data["Running"] or data.get("OOMKilled"):
                raise RuntimeError(f"{name} exited or OOM-killed before readiness")
            time.sleep(1)
        raise RuntimeError(f"{name} did not become ready")

    def empty_models(self, index):
        name = self.names[index]
        model_r = self.http(name, "/v1/models")
        system_r = self.http(name, "/system")
        if model_r["exit_code"] or system_r["exit_code"]:
            raise RuntimeError("Could not verify the model-free control")
        models, system = json.loads(model_r["stdout"]), json.loads(system_r["stdout"])
        self.append("model-controls.jsonl", {"timestamp": utc(), "phase": self.phase,
                                            "host": self.hosts[index],
                                            "models": models, "system": system})
        if "data" not in models or "loaded_models" not in system:
            raise RuntimeError("Unexpected empty-model API schema")
        if models["data"] or system["loaded_models"]:
            raise RuntimeError("A peer has configured/loaded models")

    def start_peer(self, index, image):
        name = self.names[index]
        # Retain decrypted discovery state in LocalAI's own debug logs as a
        # second vantage. The observer is the per-envelope message capture.
        args = control_peer_args(name, self.hosts[index], self.network, image,
                                 self.args.memory_mib, self.token, self.args.peer_cpus)
        self.docker(args)
        with self.lock:
            self.created.append(index)
        self.event(self.phase, "peer_started", host=self.hosts[index], container=name)
        self.wait_ready(name)
        if index == 0:
            response = self.http(name, "/api/p2p/token", secret=True)
            if response["exit_code"] or not response["stdout"].strip():
                raise RuntimeError("First peer did not produce a network token")
            self.token = response["stdout"].strip()
            self.summary["token_sha256"] = hashlib.sha256(self.token.encode()).hexdigest()
        self.follow(name, f"peer-{index + 1:02}.log")
        self.empty_models(index)

    def start_observer(self, image):
        self.docker(["run", "-d", "--name", self.observer, "--hostname", "control-observer",
                     "--network", self.network, "--label", f"localai-research={self.network}",
                     "--memory", "512m", "--memory-swap", "512m", "--cpus", "1",
                     "--log-opt", "max-size=20m", "--log-opt", "max-file=2",
                     "-e", f"LOCALAI_P2P_TOKEN={self.token}", image])
        self.observer_started = True
        self.follow(self.observer, "observer.jsonl", structured=True)
        self.event(self.phase, "observer_started", container=self.observer)

    def sample(self, indices, membership_all=False):
        names = [self.names[i] for i in indices]
        if self.observer_started:
            names.append(self.observer)
        if self.gateway_started:
            names.append(self.gateway)
        r = self.docker(["stats", "--no-stream", "--format", "{{json .}}", *names])
        stats = [json.loads(line) for line in r["stdout"].splitlines() if line.strip()]
        witnesses = indices if membership_all else sorted(set([indices[0], indices[len(indices)//2], indices[-1]]))

        def probe(i):
            result = self.http(self.names[i], "/api/p2p")
            return i, result

        membership, errors = {}, {}
        with ThreadPoolExecutor(max_workers=8) as pool:
            for i, result in pool.map(probe, witnesses):
                if result["exit_code"]:
                    errors[self.hosts[i]] = result
                else:
                    membership[self.hosts[i]] = json.loads(result["stdout"])
        row = {"timestamp": utc(), "phase": self.phase, "stats": stats,
               "membership": membership, "probe_errors": errors, "resources": resources(),
               "controller_pids": controller_pids()}
        row["fresh_hosts"] = {host: fresh_hostnames(payload, self.hosts)
                              for host, payload in membership.items()}
        self.append("samples.jsonl", row)
        if self.capture_errors:
            raise RuntimeError("Log capture failed; inspect phases.jsonl")
        for stat in stats:
            usage, ceiling = map(memory_bytes, stat["MemUsage"].split("/"))
            if usage > ceiling * 0.92:
                raise RuntimeError(f"Memory safety stop: {stat['Name']} exceeds 92% of its ceiling")
        if row["resources"]["memory_bytes"]["MemAvailable"] < 3072 * MIB:
            raise RuntimeError("Memory safety stop: less than 3 GiB host MemAvailable")
        if errors:
            raise RuntimeError("Membership witness failed; inspect its state and logs")
        return row

    def stage(self, phase, indices, seconds, converge=True):
        self.phase = phase
        self.event(phase, "begin", hosts=[self.hosts[i] for i in indices], seconds=seconds)
        print(f"Phase {phase}: {len(indices)} service peers, observe {seconds}s", flush=True)
        expected = sorted(self.hosts[i] for i in indices)
        began = time.monotonic()
        first = None
        deadline = began + seconds + (self.args.discovery_timeout if converge else 0)
        matched = False
        while time.monotonic() < deadline:
            row = self.sample(indices)
            matched = bool(row["fresh_hosts"]) and all(h == expected for h in row["fresh_hosts"].values())
            if matched and first is None:
                first = time.monotonic() - began
                self.event(phase, "witness_membership_match", expected_count=len(expected), seconds=first)
            if converge and first is not None and matched and time.monotonic() - began >= first + seconds:
                break
            time.sleep(self.args.interval)
        if converge and (first is None or not matched):
            raise RuntimeError(f"Membership witnesses did not converge in {phase}")
        result = {"phase": phase, "service_peer_count": len(indices),
                  "first_witness_match_seconds": first, "elapsed_seconds": time.monotonic() - began,
                  "final_witness_match": matched, "witnesses_are_not_all_peers": True}
        if converge:
            complete = self.sample(indices, membership_all=True)
            all_match = len(complete["fresh_hosts"]) == len(indices) and all(
                h == expected for h in complete["fresh_hosts"].values())
            result["all_peer_membership_match"] = all_match
            if not all_match:
                raise RuntimeError(f"Full-peer membership control failed in {phase}")
        self.summary["stages"].append(result)
        self.event(phase, "end", **{k: v for k, v in result.items() if k != "phase"})
        self.save("summary.json", self.summary)
        return result

    def service_probe(self, image):
        self.phase = "service-routing"
        self.event(self.phase, "begin")
        args = gateway_args(self.gateway, self.network, image,
                            self.args.memory_mib, self.token)
        self.docker(args)
        self.gateway_started = True
        self.follow(self.gateway, "gateway.log")
        # Its root is proxied; allow ledger discovery before probing.
        deadline = time.monotonic() + self.args.discovery_timeout
        attempts = []
        success = False
        while time.monotonic() < deadline:
            attempt = self.http(self.gateway, "/v1/models", timeout=10)
            attempts.append({"timestamp": utc(), **attempt})
            if not attempt["exit_code"]:
                parsed = json.loads(attempt["stdout"])
                if parsed.get("data") == []:
                    success = True
                    break
            time.sleep(5)
        self.save("service-probe.json", {"attempts": attempts, "passed": success,
                                        "scope": "read-only HTTP service routing, no inference"})
        self.event(self.phase, "end", passed=success)
        if not success:
            raise RuntimeError("Model-free federation service routing did not succeed")

    def run(self):
        code = 1
        try:
            self.summary["docker_preflight"] = self.docker(["info", "--format", "{{.ServerVersion}}"])
            endpoint = os.environ.get("DOCKER_HOST")
            if not endpoint:
                endpoint = json.loads(self.docker(["context", "inspect", "--format",
                    "{{json .Endpoints.docker.Host}}"])["stdout"])
            if not endpoint.startswith("unix://"):
                raise RuntimeError("This bounded log collector requires a local Docker Unix socket")
            self.docker_socket = endpoint[len("unix://"):]
            self.summary["log_capture_transport"] = "Docker Unix socket HTTP logs; no per-peer CLI process"
            source = command(["git", "-C", str(ROOT / "upstream"), "rev-parse", "HEAD"])
            if source["exit_code"]:
                raise RuntimeError("Missing LocalAI source clone")
            revision = source["stdout"].strip()
            self.summary["source_revision"] = revision
            self.save("message_protocol_reference.json", json.loads(
                (ROOT / "message_protocol_reference.json").read_text()))
            self.summary["instrumentation_sha256"] = {
                "run_control_messages.py": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                "observer_go_sources": {
                    p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                    for p in sorted((ROOT / "control-observer").glob("*.go"))},
                "observer_go_mod": hashlib.sha256((ROOT / "control-observer/go.mod").read_bytes()).hexdigest(),
                "observer_go_sum": hashlib.sha256((ROOT / "control-observer/go.sum").read_bytes()).hexdigest(),
            }
            archive = self.out / "instrumentation"
            (archive / "control-observer").mkdir(parents=True)
            for path in (Path(__file__), ROOT / "run_baseline.py"):
                shutil.copyfile(path, archive / path.name)
            for path in (ROOT / "control-observer").iterdir():
                if path.is_file() and (path.suffix == ".go" or path.name in ("go.mod", "go.sum", "Dockerfile")):
                    shutil.copyfile(path, archive / "control-observer" / path.name)
            self.summary["instrumentation_archive"] = "instrumentation/"
            dirty = command(["git", "-C", str(ROOT / "upstream"), "status", "--porcelain"])
            if dirty["exit_code"] or dirty["stdout"].strip():
                raise RuntimeError("LocalAI upstream clone must be clean")
            image = self.args.image or "localai/localai:sha-" + revision[:7]
            info = json.loads(self.docker(["image", "inspect", image])["stdout"])[0]
            if (info["Config"].get("Labels") or {}).get("org.opencontainers.image.revision") != revision:
                raise RuntimeError("Image/source revision mismatch")
            observer_info = json.loads(self.docker(["image", "inspect", self.args.observer_image])["stdout"])[0]
            self.save("image.json", info)
            self.save("observer-image.json", observer_info)
            image, observer_image = info["Id"], observer_info["Id"]
            self.summary["image_id"] = image
            self.summary["observer_image_id"] = observer_image
            available = resources()["memory_bytes"]["MemAvailable"]
            count = choose_peer_count(available, self.args.memory_mib, self.args.peers, self.args.reserve_mib)
            self.summary["target_service_peers"] = count
            self.summary["memory_budget"] = {"mem_available_bytes": available,
                "reserve_mib": self.args.reserve_mib, "per_peer_ceiling_mib": self.args.memory_mib,
                "total_service_peer_ceiling_mib": count * self.args.memory_mib}
            self.docker(["network", "create", "--internal", "--driver", "bridge",
                         "--label", f"localai-research={self.network}", self.network])
            self.network_created = True
            self.save("network.json", json.loads(self.docker(["network", "inspect", self.network])["stdout"]))
            self.summary["status"] = "running"
            self.phase = "bootstrap"
            self.event(self.phase, "begin")
            self.start_peer(0, image)
            self.start_observer(observer_image)
            self.stage("bootstrap", [0], self.args.stage_seconds)
            started = 1
            milestones = sorted(set([min(n, count) for n in (2, 8, 16, 32, 64)]))
            for n in milestones:
                if n <= started:
                    continue
                self.phase = f"join-{n}"
                self.event(self.phase, "begin", new_peer_count=n-started)
                with ThreadPoolExecutor(max_workers=4) as pool:
                    list(pool.map(lambda i: self.start_peer(i, image), range(started, n)))
                started = n
                self.stage(self.phase, list(range(n)), self.args.stage_seconds)
                self.summary["actual_service_peers"] = n
            self.summary["actual_service_peers"] = count
            self.stage("steady-maintenance", list(range(count)), self.args.steady_seconds)
            self.service_probe(image)
            last = count - 1
            self.phase = "peer-departure"
            self.event(self.phase, "begin", host=self.hosts[last])
            self.event(self.phase, "peer_kill", host=self.hosts[last], signal="SIGKILL")
            self.docker(["kill", "--signal", "KILL", self.names[last]])
            self.save("departed-peer-state.json", json.loads(self.docker([
                "inspect", "--format", "{{json .State}}", self.names[last]])["stdout"]))
            self.stage(self.phase, list(range(last)), self.args.departure_seconds, converge=False)
            self.phase = "peer-rejoin"
            self.event(self.phase, "begin", host=self.hosts[last])
            self.event(self.phase, "peer_restart", host=self.hosts[last])
            restarted_at = utc()
            self.docker(["start", self.names[last]])
            self.wait_ready(self.names[last])
            # docker logs -f can exit at a container stop; resume with only new lines.
            self.follow(self.names[last], f"peer-{last+1:02}-rejoin.log", since=restarted_at)
            self.empty_models(last)
            self.stage(self.phase, list(range(count)), self.args.stage_seconds)
            self.summary["status"] = "passed"
            code = 0
        except (Exception, KeyboardInterrupt) as error:
            self.summary["status"] = "failed"
            self.summary["error"] = self.redact(str(error))
            self.summary["error_type"] = type(error).__name__
            print("Capture stopped: " + self.summary["error"], flush=True)
        finally:
            self.cleanup()
            self.summary["finished"] = utc()
            self.summary["resources_after"] = resources()
            self.summary["controller_pids_after"] = controller_pids()
            self.summary["log_capture_errors"] = self.capture_errors
            if self.capture_errors and self.summary["status"] == "passed":
                self.summary["status"] = "failed"
                self.summary["error"] = "Log capture failed; inspect phases.jsonl"
            self.save("summary.json", self.summary)
            print(f"{self.summary['status']}: {self.out}", flush=True)
        return code if self.summary["status"] == "passed" else 1

    def cleanup(self):
        self.phase = "shutdown"
        self.event(self.phase, "begin")
        final_states = {}
        cleanup = []
        names = [self.names[i] for i in sorted(self.created)]
        if self.gateway_started:
            names.append(self.gateway)
        # Keep the observer alive briefly while the other peers stop.
        def stop(name):
            result = self.docker(["inspect", "--format", "{{json .State}}", name], required=False)
            self.docker(["stop", "--time", "3", name], timeout=12, required=False)
            return name, result
        with ThreadPoolExecutor(max_workers=8) as pool:
            for name, result in pool.map(stop, names):
                final_states[name] = result
        if self.observer_started:
            time.sleep(5)
            result = self.docker(["inspect", "--format", "{{json .State}}", self.observer], required=False)
            final_states[self.observer] = result
            self.docker(["stop", "--time", "3", self.observer], timeout=12, required=False)
            names.append(self.observer)
        for name, wire_socket, thread, stopping in self.streams:
            thread.join(timeout=5)
            if thread.is_alive():
                stopping.set()
                try:
                    wire_socket.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                thread.join(timeout=3)
        for name in names:
            cleanup.append(self.docker(["rm", "-f", "-v", name], required=False))
        if self.network_created:
            cleanup.append(self.docker(["network", "rm", self.network], required=False))
        self.summary["container_final_states"] = final_states
        self.summary["cleanup"] = cleanup
        if any(r["exit_code"] for r in cleanup):
            self.summary["status"] = "cleanup_failed"
        self.event(self.phase, "end")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--label", default="control-messages-" + dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S"))
    p.add_argument("--image")
    p.add_argument("--observer-image", default="localai-control-observer:edgevpn034-a3d5556")
    p.add_argument("--peers", type=int, default=0, help="0 selects up to 64 peers within the RAM budget")
    p.add_argument("--memory-mib", type=int, default=256)
    p.add_argument("--reserve-mib", type=int, default=4096)
    p.add_argument("--peer-cpus", type=float, default=0.25)
    p.add_argument("--stage-seconds", type=int, default=25)
    p.add_argument("--steady-seconds", type=int, default=960)
    p.add_argument("--departure-seconds", type=int, default=90)
    p.add_argument("--discovery-timeout", type=int, default=240)
    p.add_argument("--interval", type=int, default=10)
    args = p.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,79}", args.label):
        p.error("label must be a simple unique directory name")
    if args.peers < 0 or args.peers > 64 or min(args.memory_mib, args.reserve_mib, args.peer_cpus,
        args.stage_seconds, args.steady_seconds, args.departure_seconds, args.discovery_timeout, args.interval) <= 0:
        p.error("peer count must be 0..64; resource limits/durations must be positive")
    raise SystemExit(ControlCapture(args).run())


if __name__ == "__main__":
    main()
