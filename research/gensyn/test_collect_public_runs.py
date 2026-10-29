"""Offline tests for the bounded Gensyn public-evidence collector."""

from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


SCRIPT = Path(__file__).with_name("collect_public_runs.py")
SPEC = importlib.util.spec_from_file_location("collect_public_runs", SCRIPT)
assert SPEC and SPEC.loader
collector_module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(collector_module)


class CollectorIntegrityTests(unittest.TestCase):
    def test_verified_success_is_reused_without_network_access(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / "snapshot"
            collector = collector_module.Collector(output, 1024, False)
            body = b"verified"
            relative = Path("raw/item.json")
            (output / relative).write_bytes(body)
            collector.manifest["entries"].append({
                "url": "https://example.test/api",
                "path": str(relative),
                "status": 200,
                "bytes": len(body),
                "sha256": collector_module.hashlib.sha256(body).hexdigest(),
            })
            collector.save_manifest()
            resumed = collector_module.Collector(output, 1024, True)
            with patch.object(collector_module.urllib.request, "urlopen", side_effect=AssertionError("network request")):
                saved_body, saved_entry = resumed.get("https://example.test/api")
            self.assertEqual(saved_body, body)
            self.assertEqual(saved_entry["status"], 200)

    def test_resume_rejects_modified_saved_response(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / "snapshot"
            collector = collector_module.Collector(output, 1024, False)
            body = b"{\"public\":true}"
            relative = Path("raw/item.json")
            (output / relative).write_bytes(body)
            collector.manifest["entries"].append({
                "url": "https://example.test/api",
                "path": str(relative),
                "status": 200,
                "bytes": len(body),
                "sha256": collector_module.hashlib.sha256(body).hexdigest(),
            })
            collector.save_manifest()

            (output / relative).write_bytes(b"modified")
            with self.assertRaisesRegex(ValueError, "changed|hash"):
                collector_module.Collector(output, 1024, True)

    def test_existing_success_never_refetches_corrupted_body(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / "snapshot"
            collector = collector_module.Collector(output, 1024, False)
            relative = Path("raw/item.json")
            (output / "raw").mkdir(exist_ok=True)
            (output / relative).write_bytes(b"corrupt")
            collector.manifest["entries"].append({
                "url": "https://example.test/api",
                "path": str(relative),
                "status": 200,
                "bytes": 10,
                "sha256": "bad-hash",
            })
            with self.assertRaisesRegex(ValueError, "corrupt"):
                collector._existing_success("https://example.test/api")

    def test_analysis_prefers_successful_retry_without_duplicate_rows(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / "snapshot"
            output.mkdir()
            url = "https://dashboard-math.gensyn.ai/api/v1/leaderboard"
            failed = {"url": url, "status": 503, "path": None, "bytes": 0}
            payload = {"entries": [{
                "peerId": "peer-a",
                "peerName": "peer",
                "score": 23,
                "reward": 12,
                "online": True,
            }]}
            body = json.dumps(payload).encode()
            relative = Path("raw/leaderboard.json")
            (output / "raw").mkdir()
            (output / relative).write_bytes(body)
            success = {
                "url": url,
                "status": 200,
                "path": str(relative),
                "bytes": len(body),
                "sha256": collector_module.hashlib.sha256(body).hexdigest(),
            }
            rewards_url = "https://dashboard-math.gensyn.ai/api/v1/top-rewards"
            rewards_payload = {"entries": [{"id": "peer-a", "name": "peer", "reward": 12}]}
            rewards_body = json.dumps(rewards_payload).encode()
            rewards_path = Path("raw/rewards.json")
            (output / rewards_path).write_bytes(rewards_body)
            rewards_entry = {
                "url": rewards_url,
                "status": 200,
                "path": str(rewards_path),
                "bytes": len(rewards_body),
                "sha256": collector_module.hashlib.sha256(rewards_body).hexdigest(),
            }
            (output / "manifest.json").write_text(json.dumps({"entries": [failed, success, rewards_entry]}))
            with patch.object(collector_module, "write_inventory"):
                summary = collector_module.analyze(output)
            with (output / "normalized/nodes.csv").open(encoding="utf-8") as handle:
                rows = list(collector_module.csv.DictReader(handle))
            self.assertEqual(summary["requested_responses"], 3)
            self.assertEqual(summary["nodes_normalized"], 1)
            self.assertEqual(summary["top_reward_rows"], 1)
            self.assertEqual(rows[0]["score_raw"], "23")
            self.assertEqual(rows[0]["participation_score"], "23")
            self.assertEqual(rows[0]["training_reward"], "12")


if __name__ == "__main__":
    unittest.main()
