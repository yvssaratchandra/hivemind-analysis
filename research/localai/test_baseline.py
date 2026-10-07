"""Checks for the research runner; these are not LocalAI integration tests."""
import argparse
import datetime as dt
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import run_baseline as lab


class BaselineChecks(unittest.TestCase):
    def test_memory_units(self):
        self.assertEqual(lab.memory_bytes("1.5GiB"), 1610612736)
        self.assertEqual(lab.memory_bytes("123MiB"), 123 * 1024**2)
        self.assertEqual(lab.memory_bytes("12.5MB"), 12500000)
        with self.assertRaises(ValueError):
            lab.memory_bytes("missing")

    def test_cached_expired_and_future_records_are_not_counted_live(self):
        now = dt.datetime(2026, 10, 7, tzinfo=dt.timezone.utc)
        records = []
        for host, age in zip(lab.HOSTS, (20, 40, -60)):
            records.append({"ID": host + "-random", "LastSeen": (now - dt.timedelta(seconds=age)).isoformat()})
        self.assertEqual(lab.fresh_hosts({"federated_nodes": records}, now), [lab.HOSTS[0]])
        self.assertEqual(len(records), 3)  # Presence in the API is a separate observation.

    def test_null_bad_timestamp_and_duplicate_ids(self):
        self.assertEqual(lab.fresh_hosts({"federated_nodes": None}), [])
        now = dt.datetime.now(dt.timezone.utc)
        node = {"ID": lab.HOSTS[0] + "-random", "LastSeen": now.isoformat()}
        self.assertEqual(lab.fresh_hosts({"federated_nodes": [node, node, {"LastSeen": "bad"}]}, now), [lab.HOSTS[0]])

    def test_real_peers_have_caps_private_discovery_and_no_host_mounts(self):
        argv = lab.peer_args("test-peer", lab.HOSTS[0], "private-lab", "sha256:image", 1024, "secret")
        for forbidden in ("--privileged", "--gpus", "--device", "-v", "--volume", "--mount", "-p", "--publish"):
            self.assertNotIn(forbidden, argv)
        self.assertEqual(argv[argv.index("--network") + 1], "private-lab")
        self.assertEqual(argv[argv.index("--memory") + 1], "1024m")
        self.assertEqual(argv[argv.index("--memory-swap") + 1], "1024m")
        self.assertIn("LOCALAI_P2P_DISABLE_DHT=true", argv)
        self.assertIn("LOCALAI_P2P_TOKEN=secret", argv)
        self.assertEqual(argv[-3:], ["run", "--p2p", "--federated"])

    def test_denied_docker_stops_before_any_container_or_image_mutation(self):
        with tempfile.TemporaryDirectory() as folder:
            args = argparse.Namespace(label="denied", image=None, memory_mib=1024,
                                      seconds=1, failure_seconds=1, discovery_timeout=1, interval=1)
            called = []

            def fake(argv, timeout=30):
                called.append(argv)
                result = {"argv": argv, "exit_code": 0, "stdout": "", "stderr": ""}
                if "rev-parse" in argv:
                    result["stdout"] = "a" * 40
                if argv[0] == "docker":
                    result.update(exit_code=1, stderr="permission denied")
                return result

            with patch.object(lab, "ROOT", Path(folder)), patch.object(lab, "command", side_effect=fake):
                runner = lab.Baseline(args)
                self.assertEqual(runner.run(), 1)
                self.assertEqual([c[1] for c in called if c[0] == "docker"], ["info"])
                summary = json.loads((runner.out / "summary.json").read_text())
                self.assertEqual(summary["status"], "blocked_docker_access")
                self.assertEqual(summary["container_final_states"], {})
                self.assertIn("per-node RAM is unmeasured", (runner.out / "REPORT.md").read_text())
                with self.assertRaises(FileExistsError):
                    lab.Baseline(args)

    def test_token_is_redacted_from_recorded_command_and_output(self):
        with tempfile.TemporaryDirectory() as folder, patch.object(lab, "ROOT", Path(folder)):
            args = argparse.Namespace(label="redaction")
            runner = lab.Baseline(args)
            runner.token = "supersecret"
            output = {"argv": ["docker", "run", "-e", "TOKEN=supersecret"],
                      "exit_code": 0, "stdout": "supersecret", "stderr": ""}
            with patch.object(lab, "command", return_value=output):
                runner.docker(["run", "-e", "TOKEN=supersecret"])
            self.assertNotIn("supersecret", (runner.out / "commands.jsonl").read_text())
            runner.save("logs.json", {"logs": "supersecret"})
            self.assertNotIn("supersecret", (runner.out / "logs.json").read_text())


if __name__ == "__main__":
    unittest.main()
