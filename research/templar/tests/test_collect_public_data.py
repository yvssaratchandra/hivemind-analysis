import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from research.templar.collect_public_data import (
    bpb_rows,
    benchmark_rows,
    bounded_write,
    normalize_snapshots,
    _run_context,
)


class TemplarNormalizationTests(unittest.TestCase):
    def test_bpb_series_keeps_outer_steps_and_numeric_values(self):
        payload = {
            "chart": {"series": [{"name": "bpb", "data": [
                {"x": 10, "y": "2.0091"},
                {"x": "20", "y": 0.9},
                {"x": 30, "y": "not-a-number"},
                {"x": float("nan"), "y": 0.8},
            ]}]}
        }
        rows = bpb_rows(payload, snapshot="capture", source_url="https://www.tplr.ai/api/model-training")
        self.assertEqual([(row["x"], row["y"]) for row in rows], [(10, 2.0091), (20, 0.9)])
        self.assertTrue(all(row["metric"] == "bits_per_byte" for row in rows))
        self.assertTrue(all(row["run_label"] == "Covenant 72B" for row in rows))

    def test_benchmark_rows_keep_step_context(self):
        payload = {"benchmarks": [
            {"task": "arc_easy", "score": 0.809, "global_step": 6410, "window": 0, "timestamp": "2026-10-29T00:00:00Z"},
            {"task": "bad", "score": "invalid", "global_step": 10},
        ]}
        rows = benchmark_rows(payload)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["global_step"], 6410)
        self.assertEqual(rows[0]["score"], 0.809)

    def test_bounded_write_enforces_complete_output_cap(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            bounded_write(root, "raw/a", b"123", 6)
            bounded_write(root, "normalized/b", b"45", 6)
            with self.assertRaisesRegex(ValueError, "budget"):
                bounded_write(root, "normalized/c", b"67", 6)

    def test_open_selector_option_does_not_import_72b_model_training_series(self):
        with tempfile.TemporaryDirectory() as temporary:
            snapshot = Path(temporary) / "selector_open"
            raw = snapshot / "raw"
            artifacts = snapshot / "artifacts"
            raw.mkdir(parents=True)
            artifacts.mkdir()
            url = "https://www.tplr.ai/api/model-training"
            payload = json.dumps({"chart": {"series": [{"name": "bpb", "data": [{"x": 10, "y": 0.7}]}]}}).encode()
            body_path = raw / "response.json"
            body_path.write_bytes(payload)
            (artifacts / "page_summary.json").write_text(json.dumps({"visible_text": "RUN QWEN30_MOE_003 Covenant 72B COMPLETED"}))
            (snapshot / "manifest.json").write_text(json.dumps({"entries": [{
                "kind": "http_response", "url": url, "path": "raw/response.json", "bytes": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(), "content_type": "application/json",
            }]}))
            rows, summary = normalize_snapshots([snapshot])
            self.assertEqual(rows["series"], [])
            self.assertEqual(summary["bpb_rows"], 0)

    def test_selected_72b_panel_is_labeled_from_rendered_model_size(self):
        label, run_id = _run_context(
            Path("templar_covenant72b_selected"),
            "Covenant 72B MODEL SIZE: 72B COMPLETED",
            "https://www.tplr.ai/api/model-training",
        )
        self.assertEqual(label, "Covenant 72B")
        self.assertEqual(run_id, "")

    def test_repeated_qwen_point_is_not_counted_twice_and_keeps_run_id(self):
        with tempfile.TemporaryDirectory() as temporary:
            snapshots = []
            url = "https://www.tplr.ai/api/pipeline/loss?run_id=qwen-run-123"
            payload = json.dumps({"series": [{"name": "global", "data": [{"x": 1, "y": 2.5}]}]}).encode()
            for name in ("capture_a", "capture_b"):
                snapshot = Path(temporary) / name
                (snapshot / "raw").mkdir(parents=True)
                (snapshot / "artifacts").mkdir()
                (snapshot / "raw/response.json").write_bytes(payload)
                (snapshot / "artifacts/page_summary.json").write_text(json.dumps({"visible_text": "Covenant 72B"}))
                (snapshot / "manifest.json").write_text(json.dumps({"entries": [{
                    "kind": "http_response", "url": url, "path": "raw/response.json", "bytes": len(payload),
                    "sha256": hashlib.sha256(payload).hexdigest(), "content_type": "application/json",
                }]}))
                snapshots.append(snapshot)
            rows, summary = normalize_snapshots(snapshots)
            self.assertEqual(len(rows["series"]), 1)
            self.assertEqual(rows["series"][0]["run_label"], "QWEN30_MOE_003")
            self.assertEqual(rows["series"][0]["run_id"], "qwen-run-123")
            self.assertEqual(summary["duplicate_rows_collapsed_across_captures"], 1)


if __name__ == "__main__":
    unittest.main()
