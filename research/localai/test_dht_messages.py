"""Focused isolation checks for the DHT-enabled runner."""
import base64
import hashlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

from analyze_dht_messages import analyze
from run_dht_messages import DHTCapture, dht_peer_args


class DHTArguments(unittest.TestCase):
    def args(self, bootstrap="/ip4/172.23.0.2/tcp/4001/p2p/test-peer"):
        return dht_peer_args("test", "test", "internal-lab", "image:pin", 256, "token", 0.5, bootstrap)

    def test_private_bootstrap_is_explicit_before_image(self):
        args = self.args()
        image = args.index("image:pin")
        self.assertIn("LOCALAI_P2P_DISABLE_DHT=false", args[:image])
        self.assertNotIn("LOCALAI_P2P_DISABLE_DHT=true", args)
        self.assertIn("LOCALAI_P2P_BOOTSTRAP_PEERS_MADDRS=/ip4/172.23.0.2/tcp/4001/p2p/test-peer", args[:image])
        self.assertEqual(args[image + 1:], ["run", "--p2p", "--federated", "--debug"])

    def test_network_is_internal_lab_without_host_exposure(self):
        args = self.args()
        self.assertEqual(args[args.index("--network") + 1], "internal-lab")
        self.assertTrue({"--privileged", "-p", "--publish", "-v", "--volume", "--mount"}.isdisjoint(args))

    def test_public_bootstrap_is_rejected(self):
        with self.assertRaises(ValueError):
            self.args("/ip4/8.8.8.8/tcp/4001/p2p/test-peer")

    def test_missing_or_loopback_bootstrap_is_rejected(self):
        for addr in ("", "/ip4/127.0.0.1/tcp/4001/p2p/test-peer", "/dns/public.example/tcp/4001/p2p/test-peer"):
            with self.subTest(addr=addr), self.assertRaises(ValueError):
                self.args(addr)

    def test_invalid_port_is_rejected(self):
        for port in (0, 65536):
            with self.subTest(port=port), self.assertRaises(ValueError):
                self.args(f"/ip4/172.23.0.2/tcp/{port}/p2p/test-peer")

    def test_temporary_token_helper_uses_bridge_with_dht_disabled(self):
        capture = DHTCapture.__new__(DHTCapture)
        capture.args = SimpleNamespace(memory_mib=256, peer_cpus=0.5)
        capture.token_helper, capture.network = "temporary-helper", "private-internal-lab"
        capture.summary = {}
        capture.docker = Mock(return_value={"exit_code": 0})
        capture.wait_ready = Mock()
        capture.http = Mock(return_value={"exit_code": 0, "stdout": "fresh-token\n"})
        capture.event = Mock()
        capture.offline_token("image:pin")
        args = capture.docker.call_args_list[0].args[0]
        self.assertEqual(args[args.index("--network") + 1], "private-internal-lab")
        self.assertIn("LOCALAI_P2P_DISABLE_DHT=true", args)
        self.assertFalse(capture.token_helper_started)
        self.assertEqual(capture.docker.call_args_list[-1].args[0], ["rm", "-f", "-v", "temporary-helper"])


class DHTAnalysis(unittest.TestCase):
    def fixture(self, directory, corrupt=False):
        (directory / "summary.json").write_text(json.dumps({"status": "passed", "label": "fixture"}))
        (directory / "phases.jsonl").write_text(json.dumps({"timestamp": "2026-10-07T10:00:00Z",
            "phase": "join", "action": "begin"}) + "\n")
        body = b"\x08\x04"
        e = {"timestamp_utc": "2026-10-07T10:00:01Z", "event": "dht_message",
             "direction": "inbound_request", "message_type": "GET_PROVIDERS",
             "capture_fidelity": "decoded_protobuf_message_reencoded_by_hook",
             "from_peer_id": "peer-1", "to_peer_id": "observer"}
        for prefix, data in (("protobuf_body", body),):
            e[prefix + "_base64"] = base64.b64encode(data).decode()
            e[prefix + "_sha256"] = "bad" if corrupt else hashlib.sha256(data).hexdigest()
        (directory / "observer.jsonl").write_text(json.dumps(e) + "\n")

    def test_partial_inventory_does_not_claim_all_discovery_operations(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            self.fixture(directory)
            result = analyze(directory)
            self.assertFalse(result["all_expected_discovery_requests_observed"])
            self.assertEqual(result["dht_directions_and_types"], {"inbound_request:GET_PROVIDERS": 1})
            self.assertEqual(result["phase_dht"]["join"], {"inbound_request:GET_PROVIDERS": 1})
            self.assertEqual(result["dht_remote_peers"], ["peer-1"])
            self.assertEqual(result["integrity_errors"], [])

    def test_corrupt_raw_message_is_reported(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            self.fixture(directory, corrupt=True)
            result = analyze(directory)
            self.assertEqual(len(result["integrity_errors"]), 1)
            self.assertIn("SHA-256 mismatch", result["integrity_errors"][0]["error"])


if __name__ == "__main__":
    unittest.main()
