import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock
import http.client
import gzip
import urllib.error


MODULE_PATH = Path(__file__).with_name("collect_public_runs.py")
SPEC = importlib.util.spec_from_file_location("node0_collector", MODULE_PATH)
collector = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(collector)


class NormalizationTests(unittest.TestCase):
    def test_peer_rows_preserve_source_fields_without_claiming_contribution(self):
        payload = [{"name": "worker-a", "username": "alice", "stage_raw": "body1", "active_time": 12.5, "device": "GPU"}]
        rows = collector._peer_rows(payload, "node0-7-5b", "raw/peers.body")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["name"], "worker-a")
        self.assertEqual(rows[0]["active_time"], 12.5)
        self.assertIsNone(rows[0]["peer_id"])
        self.assertEqual(rows[0]["identity_kind"], "name")
        self.assertEqual(rows[0]["raw_ref"], "raw/peers.body")
        self.assertEqual(rows[0]["fields_json"], payload[0])
        self.assertNotIn("contribution", rows[0])

    def test_training_and_worker_telemetry_preserve_time_worker_and_metric(self):
        training = {"metrics": {"train_loss": [{"step": 4, "timestamp": 10.0, "y": 2.5}]}, "progress": {"tokens": 123}}
        telemetry = {"operations": {"worker_rate": {"timestamps": [10.0, 20.0], "workers": ["worker-a"], "values": [[1.5, None]]}}}
        rows = collector._series_rows(training, "node0-7-5b", "raw/training.body", "training")
        rows += collector._series_rows(telemetry, "node0-7-5b", "raw/telemetry.body", "telemetry_workers")
        loss = next(row for row in rows if row["metric"] == "train_loss")
        worker = next(row for row in rows if row["metric"] == "worker_rate")
        tokens = next(row for row in rows if row["metric"] == "progress.tokens")
        self.assertEqual((loss["step"], loss["timestamp"], loss["value"]), (4, 10.0, 2.5))
        no_step_training = collector._series_rows({"metrics": {"metric": [{"timestamp": 3.0, "y": 1.0}]}}, "run", "raw", "training")[0]
        self.assertIsNone(no_step_training["step"])
        self.assertEqual(no_step_training["sample_index"], 0)
        self.assertEqual((worker["worker"], worker["timestamp"], worker["value"]), ("worker-a", 10.0, 1.5))
        self.assertIsNone(worker["step"])
        self.assertEqual(worker["sample_index"], 0)
        self.assertEqual(tokens["value"], 123)
        self.assertIsNone(tokens["timestamp"])
        self.assertTrue(all(row["unit"] == "dashboard-defined; not inferred" for row in rows))
        self.assertEqual(len([row for row in rows if row["metric"] == "worker_rate"]), 1)

    def test_leaderboard_rows_keep_unknown_fields(self):
        payload = {"leaderboard": [{"rank": 1, "name": "worker-a", "score": 99.0, "tasks": 5}]}
        rows = collector._leaderboard_rows(payload, "node0-7-5b", "raw/leaderboard.body")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["rank"], 1)
        self.assertEqual(rows[0]["score"], 99.0)
        self.assertEqual(rows[0]["active_time"], None)
        self.assertEqual(rows[0]["fields_json"]["score"], 99.0)
        self.assertEqual(rows[0]["raw_ref"], "raw/leaderboard.body")

    def test_node0_registry_selection_is_exact_before_fallback(self):
        registry = {"runs": [
            {"id": "pluralis-8b", "label": "Pluralis-8B"},
            {"id": "node0-7-5b", "label": "Node0-7.5B"},
        ]}
        self.assertEqual(collector.find_node0_run(registry)["id"], "node0-7-5b")
        self.assertIsNone(collector.find_node0_run({"runs": []}))


class FetcherTests(unittest.TestCase):
    class Response:
        status = 200
        headers = {"Content-Type": "application/json"}

        def __init__(self, payload):
            self.payload = payload
            self.offset = 0

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def read(self, amount=-1):
            if amount < 0:
                amount = len(self.payload)
            part = self.payload[self.offset:self.offset + amount]
            self.offset += len(part)
            return part

    def test_success_writes_hash_and_manifest_entry(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp)
            manifest = collector.read_manifest(output, 100)
            client = collector.PublicFetcher(output, manifest, 100, resume=False)
            body = b'{"ok":true}'
            with mock.patch.object(collector.urllib.request, "urlopen", return_value=self.Response(body)):
                saved = client.get("fixture", "https://example.test/data")
            self.assertEqual(saved, body)
            entry = manifest["entries"][0]
            self.assertEqual(entry["status"], 200)
            self.assertEqual(entry["bytes"], len(body))
            self.assertEqual(entry["sha256"], collector.hashlib.sha256(body).hexdigest())
            self.assertEqual((output / entry["path"]).read_bytes(), body)
            self.assertTrue((output / "manifest.json").is_file())

    def test_403_is_recorded_without_retry_or_response_body(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp)
            manifest = collector.read_manifest(output, 100)
            client = collector.PublicFetcher(output, manifest, 100, resume=False)
            error = urllib.error.HTTPError("https://example.test/data", 403, "Forbidden", {}, None)
            with mock.patch.object(collector.urllib.request, "urlopen", side_effect=error) as mocked:
                saved = client.get("fixture", "https://example.test/data")
            self.assertIsNone(saved)
            self.assertEqual(mocked.call_count, 1)
            self.assertEqual(manifest["entries"][0]["status"], 403)
            self.assertIsNone(manifest["entries"][0]["path"])
            self.assertEqual(json.loads((output / "manifest.json").read_text())["entries"][0]["error"], "HTTP 403")

    def test_budget_limit_records_skip_and_removes_partial_file(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp)
            manifest = collector.read_manifest(output, 3)
            client = collector.PublicFetcher(output, manifest, 3, resume=False)
            with mock.patch.object(collector.urllib.request, "urlopen", return_value=self.Response(b"1234")):
                saved = client.get("fixture", "https://example.test/data")
            self.assertIsNone(saved)
            self.assertEqual(manifest["entries"][0]["status"], "skipped_budget")
            self.assertFalse(list((output / "raw").glob("*.body")))

    def test_resume_fails_closed_on_missing_or_corrupt_successful_body(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp)
            raw = output / "raw" / "good.body"
            raw.parent.mkdir()
            raw.write_bytes(b"original")
            manifest = {
                "schema_version": 1,
                "created_at": "now",
                "max_bytes": 100,
                "entries": [{
                    "key": "fixture",
                    "url": "https://example.test/data",
                    "path": "raw/good.body",
                    "status": 200,
                    "bytes": 8,
                    "sha256": collector.hashlib.sha256(b"original").hexdigest(),
                }],
            }
            client = collector.PublicFetcher(output, manifest, 100, resume=True)
            raw.write_bytes(b"changed")
            with self.assertRaisesRegex(ValueError, "size/hash verification"):
                client.get("fixture", "https://example.test/data")
            with self.assertRaisesRegex(ValueError, "size/hash verification"):
                collector.verify_successful_raw(output, manifest)
            raw.unlink()
            with self.assertRaisesRegex(ValueError, "missing"):
                collector.verify_successful_raw(output, manifest)

    def test_retry_after_beyond_bound_is_deferred_without_early_retry(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp)
            manifest = collector.read_manifest(output, 100)
            client = collector.PublicFetcher(output, manifest, 100, resume=False)
            error = urllib.error.HTTPError("https://example.test/data", 429, "Too Many Requests", {"Retry-After": "120"}, None)
            with mock.patch.object(collector.urllib.request, "urlopen", side_effect=error) as mocked, mock.patch.object(collector.time, "sleep") as sleep:
                saved = client.get("fixture", "https://example.test/data")
            self.assertIsNone(saved)
            self.assertEqual(mocked.call_count, 1)
            sleep.assert_not_called()
            self.assertIn("deferred without early retry", manifest["entries"][0]["error"])

    def test_misaligned_worker_matrix_is_rejected(self):
        payload = {"operations": {"metric": {"timestamps": [1, 2], "workers": ["a"], "values": [[3]]}}}
        with self.assertRaisesRegex(ValueError, "Misaligned timestamp/value series"):
            collector._series_rows(payload, "run", "raw", "telemetry_workers")

    def test_compressed_csv_is_deterministic_and_readable(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "metrics.csv.gz"
            fields = ["metric", "value"]
            rows = [{"metric": "loss", "value": 1.25}]
            collector._write_csv(path, fields, rows)
            first = path.read_bytes()
            collector._write_csv(path, fields, rows)
            second = path.read_bytes()
            self.assertEqual(first, second)
            with gzip.open(path, "rt", encoding="utf-8", newline="") as stream:
                self.assertEqual(stream.read().splitlines(), ["metric,value", "loss,1.25"])

    def test_incomplete_read_cleans_partial_and_retries_boundedly(self):
        class IncompleteResponse(self.Response):
            def __init__(self):
                super().__init__(b"")
                self.calls = 0

            def read(self, amount=-1):
                self.calls += 1
                if self.calls == 1:
                    return b"part"
                raise http.client.IncompleteRead(b"part", 5)

        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp)
            manifest = collector.read_manifest(output, 100)
            client = collector.PublicFetcher(output, manifest, 100, resume=False)
            with mock.patch.object(collector.urllib.request, "urlopen", side_effect=[IncompleteResponse(), self.Response(b'{"ok":true}')]):
                saved = client.get("fixture", "https://example.test/data")
            self.assertEqual(saved, b'{"ok":true}')
            self.assertFalse(list((output / "raw").glob("*.partial")))
            self.assertEqual(manifest["entries"][0]["attempts"], 2)


if __name__ == "__main__":
    unittest.main()
