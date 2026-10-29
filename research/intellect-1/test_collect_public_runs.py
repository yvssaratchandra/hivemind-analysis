import importlib.util
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("collect_public_runs.py")
SPEC = importlib.util.spec_from_file_location("intellect1_collector", SCRIPT)
collector = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(collector)


class CollectorNormalizationTests(unittest.TestCase):
    def test_failed_source_has_explicit_empty_evidence_fields(self):
        [row] = collector.normalized_entries({"entries": [{"url": "https://example.invalid", "error": "unavailable"}]})
        self.assertIsNone(row["path"])
        self.assertEqual(row["bytes"], 0)
        self.assertIsNone(row["sha256"])
        self.assertIsNone(row["status"])

    def test_successful_source_fields_are_preserved(self):
        item = {"url": "https://example.invalid", "status": 200, "path": "raw/a", "bytes": 3, "sha256": "abc"}
        [row] = collector.normalized_entries({"entries": [item]})
        self.assertEqual({key: row[key] for key in ("status", "path", "bytes", "sha256")},
                         {key: item[key] for key in ("status", "path", "bytes", "sha256")})

    def test_raw_caps_never_exceed_half_total_or_one_decimal_gigabyte(self):
        raw, http, per_capture = collector.raw_budget_parts(3_000_000_000)
        self.assertEqual(raw, 1_000_000_000)
        self.assertEqual(http + 2 * per_capture, raw)
        self.assertLessEqual(raw, 3_000_000_000 // 2)

    def test_raw_caps_scale_down_for_small_budget(self):
        raw, http, per_capture = collector.raw_budget_parts(200_000_000)
        self.assertEqual(raw, 100_000_000)
        self.assertEqual(http + 2 * per_capture, raw)

    def test_file_budget_measurement_counts_nested_output(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            nested = root / "captures" / "raw.bin"
            nested.parent.mkdir()
            nested.write_bytes(b"12345")
            self.assertEqual(collector.file_bytes(root), 5)

    def test_total_budget_rejects_overflow(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "raw.bin").write_bytes(b"123456")
            with self.assertRaisesRegex(ValueError, "Total output exceeded"):
                collector.enforce_budget(root, 5, 0)

    def test_raw_budget_rejects_overflow(self):
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaisesRegex(ValueError, "Raw evidence exceeded"):
                collector.enforce_budget(Path(temporary), 10, 6)

    def test_corrupt_browser_capture_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            capture = root / "captures" / "dashboard"
            (capture / "artifacts").mkdir(parents=True)
            body = b"original"
            evidence = capture / "artifacts" / "page.html"
            evidence.write_bytes(body)
            manifest = {"entries": [{"path": "artifacts/page.html", "bytes": len(body),
                                     "sha256": hashlib.sha256(body).hexdigest()}]}
            (capture / "manifest.json").write_text(json.dumps(manifest))
            evidence.write_bytes(b"corrupt!")
            with self.assertRaisesRegex(ValueError, "Browser evidence hash/size mismatch"):
                collector.verify_captures(root)

    def test_capture_summary_preserves_initial_document_status(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "captures"
            capture = root / "dashboard"
            capture.mkdir(parents=True)
            (capture / "manifest.json").write_text(json.dumps({
                "url": "https://example.invalid", "capture_status": "complete_with_errors",
                "entries": [{"path": "artifacts/initial.html", "status": 404}],
            }))
            [summary] = collector.read_capture_manifests(Path(temporary))
            self.assertEqual(summary["document_status"], 404)

    def test_analyze_rejects_missing_http_manifest(self):
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaisesRegex(FileNotFoundError, "Expected HTTP source manifest"):
                collector.analyze(Path(temporary), 1_000_000)


if __name__ == "__main__":
    unittest.main()
