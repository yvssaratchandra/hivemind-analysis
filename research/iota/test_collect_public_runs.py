import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

MODULE_PATH = Path(__file__).with_name("collect_public_runs.py")
SPEC = importlib.util.spec_from_file_location("iota_collector", MODULE_PATH)
collector = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = collector
SPEC.loader.exec_module(collector)


class CollectorTests(unittest.TestCase):
    def test_raw_budget_is_half_project_cap_and_never_over_one_gib(self):
        self.assertEqual(collector.raw_budget(3_000_000_000), 1_000_000_000)
        self.assertEqual(collector.raw_budget(1000), 500)
        with self.assertRaises(ValueError):
            collector.raw_budget(3_000_000_001)

    def test_heatmap_normalization_preserves_node_layer_epoch_and_count(self):
        payload = {"data": [{"name": "activations", "x": ["E1"],
                             "y": ["L02 · hotkey-abc"], "z": [[0.5]],
                             "customdata": [[12]]}]}
        row, = list(collector.source_rows("run-a", "mn_activations", payload, "https://example/chart"))
        self.assertEqual((row["run"], row["layer"], row["hotkey"], row["x"]),
                         ("run-a", "L02", "hotkey-abc", "E1"))
        self.assertEqual((row["z"], json.loads(row["customdata"])), (0.5, 12))

    def test_leaderboard_keeps_raw_display_units(self):
        payload = {"format": "table", "columns": ["#", "HOTKEY", "TOKENS", "TOK/SEC"],
                   "rows": [["1", "key", "4.2B", "2,327.87"]]}
        row, = list(collector.source_rows("run-a", "mn_leaderboard", payload, "https://example/table"))
        self.assertEqual(row["hotkey"], "key")
        self.assertEqual(json.loads(row["metric_value"])["TOKENS"], "4.2B")

    def test_loss_ndjson_preserves_both_axes_metadata_and_chunk_indices(self):
        payload = [
            {"format": "loss", "traces": [{"i": 0, "name": "LOSS (MA)"},
                                             {"i": 1, "name": "LOSS (RAW)"}]},
            {"i": 1, "x_time": [101], "x_tokens": [20], "y": [7.0]},
            {"i": 0, "x_time": [102], "x_tokens": [30], "y": [6.0]},
            {"i": 0, "x_time": [103], "x_tokens": [40], "y": [5.0]},
        ]
        rows = list(collector.source_rows("run-a", "ov_loss_ma", payload, "https://example/loss"))
        self.assertEqual(rows[0]["trace"], "LOSS (RAW)")
        self.assertEqual((rows[0]["x_time"], rows[0]["x_tokens"], rows[0]["y"]), (101, 20, 7.0))
        self.assertEqual([r["chunk_index"] for r in rows], [0, 0, 1])

    def test_analyze_only_requires_and_verifies_hash_manifests(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self.assertRaises(FileNotFoundError):
                collector.verify_existing(root, require_manifest=True)
            snap = root / "dashboard_x"
            snap.mkdir()
            body = b"captured public response"
            (snap / "body.bin").write_bytes(body)
            (snap / "manifest.json").write_text(json.dumps({"entries": [{
                "path": "body.bin", "bytes": len(body),
                "sha256": __import__("hashlib").sha256(body).hexdigest()}]}))
            self.assertEqual(collector.verify_existing(root, require_manifest=True), 1)
            (snap / "body.bin").write_bytes(b"tampered")
            with self.assertRaises(ValueError):
                collector.verify_existing(root, require_manifest=True)


if __name__ == "__main__":
    unittest.main()
