#!/usr/bin/env python3
"""Short, model-free LocalAI DHT capture in an internal Docker network."""
import argparse
import datetime as dt
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import shutil
import time

from run_baseline import command, resources, utc
from run_control_messages import (ControlCapture, ROOT, choose_peer_count,
                                  control_peer_args, controller_pids)


def dht_peer_args(name, hostname, network, image, mib, token, cpus, bootstrap):
    """Enable DHT with an explicit bridge-reachable bootstrap, never defaults."""
    match = re.fullmatch(r"/ip4/([^/]+)/tcp/(\d+)/p2p/([^/]+)", bootstrap)
    if not match or not ipaddress.ip_address(match[1]).is_private or not 0 < int(match[2]) < 65536:
        raise ValueError("Expected a private IPv4 TCP bootstrap peer multiaddr")
    if ipaddress.ip_address(match[1]).is_loopback:
        raise ValueError("A container bootstrap must not use loopback")
    args = control_peer_args(name, hostname, network, image, mib, token, cpus)
    args[args.index("LOCALAI_P2P_DISABLE_DHT=true")] = "LOCALAI_P2P_DISABLE_DHT=false"
    position = args.index(image)
    args[position:position] = ["-e", f"LOCALAI_P2P_BOOTSTRAP_PEERS_MADDRS={bootstrap}",
                              "-e", "LOCALAI_P2P_LISTEN_MADDRS=/ip4/0.0.0.0/tcp/4001"]
    return args


class DHTCapture(ControlCapture):
    def __init__(self, args):
        super().__init__(args)
        self.token_helper = self.network + "-token"
        self.token_helper_started = False
        self.summary.update({
            "scope": "Two model-free LocalAI peers, private DHT bootstrap; mDNS also enabled",
            "observer_effect": "Diagnostic KAD server plus EdgeVPN ledger participant; no LocalAI service/heartbeat",
            "dht_capture_coverage": "Inbound DHT requests via decoded protobuf hook; no responses or observer outbound requests",
            "dht_bytes": "Decoded protobuf re-encoding; no exact length-delimited frames or encrypted packets",
            "dht_discovery_attribution": "mDNS and GossipSub direct bootstrap peers coexist; convergence is not DHT-only proof",
        })

    def offline_token(self, image):
        # LocalAI hardcodes mDNS and exits without a multicast-capable interface.
        # This fresh internal bridge supplies one while keeping WAN access off.
        args = control_peer_args(self.token_helper, "dht-token-helper", self.network, image,
                                 self.args.memory_mib, "", self.args.peer_cpus)
        self.docker(args)
        self.token_helper_started = True
        try:
            self.wait_ready(self.token_helper)
        except Exception:
            logs = self.docker(["logs", self.token_helper], required=False)
            self.save("token-helper-failure-logs.json", logs)
            state = self.docker(["inspect", "--format", "{{json .State}}", self.token_helper], required=False)
            self.save("token-helper-failure-state.json", state)
            raise
        response = self.http(self.token_helper, "/api/p2p/token", secret=True)
        if response["exit_code"] or not response["stdout"].strip():
            raise RuntimeError("Isolated helper did not produce a token")
        self.token = response["stdout"].strip()
        self.summary["token_sha256"] = hashlib.sha256(self.token.encode()).hexdigest()
        self.summary["token_helper_cleanup"] = self.docker(["rm", "-f", "-v", self.token_helper])
        self.token_helper_started = False
        self.event("preflight", "fresh_token_generated", network=self.network,
                   dht_enabled=False, helper_removed_before_capture=True)

    def start_observer(self, image):
        self.docker(["run", "-d", "--name", self.observer, "--hostname", "dht-observer",
                     "--network", self.network, "--label", f"localai-research={self.network}",
                     "--memory", "512m", "--memory-swap", "512m", "--cpus", "1",
                     "--log-opt", "max-size=20m", "--log-opt", "max-file=2",
                     "-e", f"LOCALAI_P2P_TOKEN={self.token}",
                     "-e", "LOCALAI_P2P_LISTEN_MADDRS=/ip4/0.0.0.0/tcp/4001",
                     image, "--dht=true", "--mdns=false", "--dht-mode=server"])
        self.observer_started = True
        self.follow(self.observer, "observer.jsonl", structured=True)
        deadline = time.monotonic() + self.args.discovery_timeout
        while time.monotonic() < deadline:
            path = self.out / "observer.jsonl"
            events = []
            if path.exists():
                for line in path.read_text().splitlines():
                    try:
                        events.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass  # A reader can see the final line before its newline arrives.
            started = next((e for e in events if e.get("event") == "observer_started"), None)
            if started:
                address = self.docker(["inspect", "--format", "{{json .NetworkSettings.Networks}}", self.observer])
                ip = json.loads(address["stdout"])[self.network]["IPAddress"]
                self.bootstrap = f"/ip4/{ip}/tcp/4001/p2p/{started['observer_peer_id']}"
                self.summary["bootstrap_multiaddr"] = self.bootstrap
                self.summary["observer_configuration"] = started
                self.event(self.phase, "private_dht_server_ready", multiaddr=self.bootstrap)
                return
            state = json.loads(self.docker(["inspect", "--format", "{{json .State}}", self.observer])["stdout"])
            if not state["Running"]:
                raise RuntimeError("DHT observer exited during startup")
            time.sleep(0.5)
        raise RuntimeError("DHT observer startup timed out")

    def start_peer(self, index, image):
        name = self.names[index]
        self.docker(dht_peer_args(name, self.hosts[index], self.network, image,
                                 self.args.memory_mib, self.token, self.args.peer_cpus, self.bootstrap))
        self.created.append(index)
        self.event(self.phase, "peer_started", host=self.hosts[index], container=name,
                   dht_enabled=True, mdns_enabled=True, bootstrap=self.bootstrap)
        self.follow(name, f"peer-{index + 1:02}.log")
        self.wait_ready(name)
        self.empty_models(index)

    def cleanup(self):
        if self.token_helper_started:
            self.summary["token_helper_cleanup"] = self.docker(["rm", "-f", "-v", self.token_helper], required=False)
        super().cleanup()

    def run(self):
        try:
            self.summary["docker_preflight"] = self.docker(["info", "--format", "{{.ServerVersion}}"])
            endpoint = os.environ.get("DOCKER_HOST")
            if not endpoint:
                endpoint = json.loads(self.docker(["context", "inspect", "--format", "{{json .Endpoints.docker.Host}}"])["stdout"])
            if not endpoint.startswith("unix://"):
                raise RuntimeError("This collector requires a local Docker Unix socket")
            self.docker_socket = endpoint[len("unix://"):]
            revision = command(["git", "-C", str(ROOT / "upstream"), "rev-parse", "HEAD"])["stdout"].strip()
            dirty = command(["git", "-C", str(ROOT / "upstream"), "status", "--porcelain"])
            if not revision or dirty["exit_code"] or dirty["stdout"].strip():
                raise RuntimeError("A clean LocalAI source clone is required")
            self.summary["source_revision"] = revision
            image_info = json.loads(self.docker(["image", "inspect", self.args.image])["stdout"])[0]
            observer_info = json.loads(self.docker(["image", "inspect", self.args.observer_image])["stdout"])[0]
            if (image_info["Config"].get("Labels") or {}).get("org.opencontainers.image.revision") != revision:
                raise RuntimeError("LocalAI image/source revision mismatch")
            image, observer_image = image_info["Id"], observer_info["Id"]
            self.summary.update({"image_id": image, "observer_image_id": observer_image})
            self.save("image.json", image_info)
            self.save("observer-image.json", observer_info)
            choose_peer_count(resources()["memory_bytes"]["MemAvailable"], self.args.memory_mib, 2, 4096)
            archive = self.out / "instrumentation"
            (archive / "control-observer").mkdir(parents=True)
            source_files = [Path(__file__), ROOT / "analyze_dht_messages.py",
                            ROOT / "run_control_messages.py", ROOT / "run_baseline.py"]
            source_files += [p for p in (ROOT / "control-observer").iterdir()
                             if p.is_file() and (p.suffix == ".go" or p.name in ("go.mod", "go.sum", "Dockerfile"))]
            self.summary["instrumentation_sha256"] = {}
            for path in source_files:
                relative = path.relative_to(ROOT)
                shutil.copyfile(path, archive / relative)
                self.summary["instrumentation_sha256"][str(relative)] = hashlib.sha256(path.read_bytes()).hexdigest()
            self.summary["instrumentation_archive"] = "instrumentation/"
            self.docker(["network", "create", "--internal", "--driver", "bridge",
                         "--label", f"localai-research={self.network}", self.network])
            self.network_created = True
            self.save("network.json", json.loads(self.docker(["network", "inspect", self.network])["stdout"]))
            self.offline_token(image)
            self.summary["status"] = "running"
            self.phase = "bootstrap"
            self.event(self.phase, "begin")
            self.start_observer(observer_image)
            self.start_peer(0, image)
            self.stage("bootstrap", [0], self.args.stage_seconds)
            self.phase = "join-advertisement"
            self.event(self.phase, "begin")
            self.start_peer(1, image)
            self.stage(self.phase, [0, 1], self.args.stage_seconds)
            self.stage("steady-maintenance", [0, 1], self.args.steady_seconds)
            self.phase = "peer-departure"
            self.event(self.phase, "begin", host=self.hosts[1])
            self.expect_log_end(self.names[1])
            self.docker(["kill", "--signal", "KILL", self.names[1]])
            self.stage(self.phase, [0], self.args.departure_seconds, converge=False)
            self.phase = "peer-rejoin"
            self.event(self.phase, "begin", host=self.hosts[1])
            restarted_at = utc()
            self.docker(["start", self.names[1]])
            self.follow(self.names[1], "peer-02-rejoin.log", since=restarted_at)
            self.wait_ready(self.names[1])
            self.empty_models(1)
            self.stage(self.phase, [0, 1], self.args.stage_seconds)
            self.summary["actual_service_peers"] = 2
            self.summary["status"] = "passed"
        except (Exception, KeyboardInterrupt) as error:
            self.summary.update({"status": "failed", "error": self.redact(str(error)),
                                 "error_type": type(error).__name__})
            print("Capture stopped: " + self.summary["error"], flush=True)
        finally:
            self.cleanup()
            self.summary.update({"finished": utc(), "resources_after": resources(),
                                 "controller_pids_after": controller_pids(),
                                 "log_capture_errors": self.capture_errors})
            if self.capture_errors and self.summary["status"] == "passed":
                self.summary.update({"status": "failed", "error": "Log capture was incomplete"})
            self.save("summary.json", self.summary)
            print(f"{self.summary['status']}: {self.out}", flush=True)
        return 0 if self.summary["status"] == "passed" else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--label", default="dht-messages-" + dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S"))
    parser.add_argument("--image", default="localai/localai:sha-a3d5556")
    parser.add_argument("--observer-image", default="localai-control-observer:dht-edgevpn034-a3d5556")
    parser.add_argument("--memory-mib", type=int, default=256)
    parser.add_argument("--peer-cpus", type=float, default=0.5)
    parser.add_argument("--stage-seconds", type=int, default=10)
    parser.add_argument("--steady-seconds", type=int, default=50)
    parser.add_argument("--departure-seconds", type=int, default=15)
    parser.add_argument("--discovery-timeout", type=int, default=90)
    parser.add_argument("--interval", type=float, default=3)
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", args.label):
        parser.error("label must be a plain directory name")
    if min(args.stage_seconds, args.steady_seconds, args.departure_seconds, args.discovery_timeout, args.interval) <= 0:
        parser.error("durations must be positive")
    raise SystemExit(DHTCapture(args).run())


if __name__ == "__main__":
    main()
