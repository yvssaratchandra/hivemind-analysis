"""Focused offline tests for bounded dashboard capture helpers."""

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

import capture_dashboard as capture


class SnapshotWriterTests(unittest.TestCase):
    def test_saved_file_has_manifest_hash_and_size_and_budget_is_enforced(self):
        with tempfile.TemporaryDirectory() as temp:
            writer = capture.SnapshotWriter(Path(temp) / "snapshot", max_bytes=3)
            entry = writer.save(
                relative="raw/one.body",
                body=b"abc",
                kind="http_response",
                url="https://example.test/api",
                status=200,
            )
            self.assertEqual(entry["bytes"], 3)
            self.assertEqual(entry["sha256"], hashlib.sha256(b"abc").hexdigest())
            self.assertEqual((writer.output / entry["path"]).read_bytes(), b"abc")
            skipped = writer.save(
                relative="raw/two.body",
                body=b"d",
                kind="http_response",
                url="https://example.test/other",
                status=200,
            )
            self.assertIsNone(skipped["path"])
            self.assertEqual(skipped["application_status"], "skipped")
            self.assertFalse((writer.output / "raw/two.body").exists())

    def test_existing_output_is_never_overwritten(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / "snapshot"
            output.mkdir()
            marker = output / "keep.txt"
            marker.write_text("preserve")
            with self.assertRaises(FileExistsError):
                capture.SnapshotWriter(output, max_bytes=10)
            self.assertEqual(marker.read_text(), "preserve")

    def test_evidence_paths_cannot_escape_output(self):
        with tempfile.TemporaryDirectory() as temp:
            writer = capture.SnapshotWriter(Path(temp) / "snapshot", max_bytes=10)
            for path in ("../escape", "/absolute", "raw/../../escape"):
                with self.subTest(path=path), self.assertRaises(ValueError):
                    writer.save(relative=path, body=b"x", kind="test", url=None)

    def test_manifest_is_parseable_and_lists_every_saved_entry(self):
        with tempfile.TemporaryDirectory() as temp:
            writer = capture.SnapshotWriter(Path(temp) / "snapshot", max_bytes=10)
            writer.save(relative="artifacts/dom.html", body=b"<html/>", kind="rendered_dom", url="https://example.test")
            writer.flush_manifest()
            manifest = json.loads((writer.output / "manifest.json").read_text())
            self.assertEqual(len(manifest["entries"]), 1)
            self.assertEqual(manifest["entries"][0]["sha256"], hashlib.sha256(b"<html/>").hexdigest())

    def test_empty_generated_artifact_is_marked_skipped(self):
        with tempfile.TemporaryDirectory() as temp:
            dashboard = capture.DashboardCapture.__new__(capture.DashboardCapture)
            dashboard.writer = capture.SnapshotWriter(Path(temp) / "snapshot", max_bytes=10)
            dashboard.args = type("Args", (), {"max_bytes": 10})()
            entry = dashboard._save_artifact("page.pdf", b"", "page_pdf", "https://example.test", "application/pdf")
            self.assertEqual(entry["application_status"], "skipped")
            self.assertIn("empty", entry["reason"])
            self.assertFalse((dashboard.writer.output / "artifacts/page.pdf").exists())


class PolicyHelperTests(unittest.TestCase):
    def test_host_normalization_and_url_validation(self):
        self.assertEqual(capture.normalized_host("WWW.Example.test."), "www.example.test")
        self.assertEqual(capture.validate_url("https://www.example.test/app?x=1"), ("www.example.test", "https"))
        with self.assertRaises(ValueError):
            capture.validate_url("file:///tmp/page.html")
        with self.assertRaises(ValueError):
            capture.normalized_host("example.test/private/path")

    def test_analytics_and_refresh_paths_are_blocked_but_metrics_are_allowed(self):
        self.assertEqual(capture.blocked_path_reason("https://example.test/api/analytics/events"), "analytics route blocked")
        self.assertEqual(capture.blocked_path_reason("https://example.test/api/v1/refresh"), "refresh route blocked")
        self.assertIsNone(capture.blocked_path_reason("https://example.test/api/v1/metrics"))
        self.assertIsNone(capture.blocked_path_reason("https://example.test/api/telemetry"))

    def test_http_route_allows_four_gets_per_url_and_blocks_the_fifth(self):
        dashboard = capture.DashboardCapture.__new__(capture.DashboardCapture)
        dashboard.allowed_hosts = {"example.test"}
        dashboard.get_url_counts = {}
        dashboard.blocked_requests = []
        dashboard.console_bytes = 0
        dashboard.dropped_log_entries = 0
        route = Mock()
        route.request.method = "GET"
        route.request.url = "https://example.test/api/mech1/logo"

        for _ in range(4):
            dashboard._route_request(route)

        self.assertEqual(route.continue_.call_count, 4)
        self.assertEqual(route.abort.call_count, 0)
        self.assertEqual(dashboard.get_url_counts[route.request.url], 4)

        dashboard._route_request(route)

        self.assertEqual(route.continue_.call_count, 4)
        route.abort.assert_called_once_with("blockedbyclient")
        self.assertIn("repeat cap (4)", dashboard.blocked_requests[0]["reason"])

    def test_websocket_capture_drops_frames_over_limit_before_encoding(self):
        dashboard = capture.DashboardCapture.__new__(capture.DashboardCapture)
        dashboard.args = type("Args", (), {"max_response_bytes": 8, "max_bytes": 100})()
        dashboard.websocket_lines = []
        dashboard.websocket_bytes = 0
        dashboard.websocket_dropped = 0
        dashboard._capture_websocket("wss://example.test/ws", b"x" * 9)
        self.assertEqual(dashboard.websocket_dropped, 1)
        self.assertEqual(dashboard.websocket_lines, [])

    def test_websocket_proxy_leaves_close_propagation_to_playwright(self):
        dashboard = capture.DashboardCapture.__new__(capture.DashboardCapture)
        dashboard.allowed_ws_hosts = {"example.test"}
        dashboard._capture_websocket = Mock()
        route = Mock()
        route.url = "wss://example.test/metrics"
        server = Mock()
        route.connect_to_server.return_value = server
        dashboard._route_websocket(route)
        route.on_close.assert_not_called()
        server.on_close.assert_not_called()


if __name__ == "__main__":
    unittest.main()
