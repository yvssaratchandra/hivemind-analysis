import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

MODULE_PATH = Path(__file__).with_name("collect_public_runs.py")
SPEC = importlib.util.spec_from_file_location("psyche_collector", MODULE_PATH)
collector = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = collector
SPEC.loader.exec_module(collector)


class CollectorTests(unittest.TestCase):
    def test_raw_budget_is_bounded(self):
        self.assertEqual(collector.raw_budget(3_000_000_000), 1_000_000_000)
        self.assertEqual(collector.raw_budget(80), 40)
        with self.assertRaises(ValueError):
            collector.raw_budget(0)

    def test_model_card_metadata_only_skips_nonmetadata_and_failures(self):
        self.assertEqual(collector.model_metadata(Path("."), [
            {"label": "consilience-40b-model-metadata", "status": 404},
            {"label": "not-model-metadata", "status": 200, "path": "x"},
        ]), [])

    def test_deployed_api_classifier_requires_official_host_and_known_route(self):
        self.assertTrue(collector.is_official_api_response("https://mainnet.psyche.network/api/runs"))
        self.assertFalse(collector.is_official_api_response("https://analytics.example/api/runs"))
        self.assertFalse(collector.is_official_api_response("https://psyche.network/api/private"))
        self.assertFalse(collector.is_official_api_response("https://psyche.network/api/runs-extra"))

    def test_analyze_only_requires_and_verifies_hash_manifests(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self.assertRaises(FileNotFoundError):
                collector.verify_existing(root, require_manifest=True)
            snap = root / "dashboard_x"
            snap.mkdir()
            body = b"captured public response"
            (snap / "body.bin").write_bytes(body)
            import hashlib
            (snap / "manifest.json").write_text(json.dumps({"entries": [{
                "path": "body.bin", "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}]}))
            self.assertEqual(collector.verify_existing(root, require_manifest=True), 1)
            (snap / "body.bin").write_bytes(b"tampered")
            with self.assertRaises(ValueError):
                collector.verify_existing(root, require_manifest=True)


if __name__ == "__main__":
    unittest.main()
