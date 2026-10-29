import csv
import email.message
import gzip
import hashlib
import io
import json
import sys
import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import collect_public_runs as collector


class CollectorUnitTests(unittest.TestCase):
    def test_non_scalar_history_is_allowed_but_sensitive_key_is_rejected(self):
        safe_doc = {
            "data": {
                "project": {
                    "runs": {
                        "edges": [
                            {
                                "node": {
                                    "name": "run1",
                                    "history": [json.dumps({"loss": 0.2, "tag": "baseline", "nested": {"x": 1}})],
                                }
                            }
                        ]
                    }
                }
            }
        }
        self.assertEqual(collector.validate_wandb_safe(safe_doc), (True, None, []))
        unsafe_doc = json.loads(json.dumps(safe_doc))
        unsafe_doc["data"]["project"]["runs"]["edges"][0]["node"]["history"] = [
            json.dumps({"api_token": "redacted-test-value"})
        ]
        valid, reason, names = collector.validate_wandb_safe(unsafe_doc)
        self.assertFalse(valid)
        self.assertIn("credential-like", reason)
        self.assertIn("api_token", reason)
        self.assertNotIn("redacted-test-value", reason)
        self.assertEqual(names, ["run1"])
        unknown_history = {"data": {"project": {"runs": {"edges": [{"node": {"name": "r", "history": None}}]}}}}
        self.assertEqual(collector.validate_wandb_safe(unknown_history), (True, None, []))
        full_window = {"data": {"project": {"run": {"h000": [json.dumps({"_step": 1, "api_token": "omitted"})]}}}}
        valid, reason, _ = collector.validate_wandb_safe(full_window)
        self.assertFalse(valid)
        self.assertIn("credential-like", reason)

    def make_snapshot(self, root: Path):
        entries = []

        def save(relative, body, request_id):
            path = root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            data = json.dumps(body, sort_keys=True).encode()
            path.write_bytes(data)
            entries.append(
                {
                    "id": request_id,
                    "path": relative,
                    "status": 200,
                    "bytes": len(data),
                    "sha256": hashlib.sha256(data).hexdigest(),
                }
            )

        def run(name, count, history):
            return {
                "node": {
                    "name": name,
                    "displayName": name,
                    "state": "finished",
                    "createdAt": "2026-01-01T00:00:00Z",
                    "heartbeatAt": "2026-01-01T00:01:00Z",
                    "historyLineCount": count,
                    "history": history,
                }
            }

        doc = {
            "data": {
                "project": {
                    "runCount": 3,
                    "runs": {
                        "edges": [
                            run("r1", 0, []),
                            run("r2", None, []),
                            run("r3", 2, [json.dumps({"loss": 1.0}), json.dumps({"accuracy": 0.5})]),
                        ],
                        "pageInfo": {"hasNextPage": False, "endCursor": "cursor3"},
                    },
                }
            }
        }
        save("raw/wandb/kmfoda/distributed_training_miners/page-0000.json", doc, "wandb-test")

        index = {"current_block": 9, "strategies": [{"uid": "u1", "hotkey": "h1", "score": 4}]}
        block = {"strategies": [{"uid": "u1", "hotkey": "h1", "score": 4}]}
        save("raw/mech1/strategies-index.json", index, "strategy-index")
        save("raw/mech1/strategies-block-9.json", block, "strategy-block")
        root.joinpath("manifest.json").write_text(json.dumps({"entries": entries}))

    def test_normalize_marks_zero_and_unknown_counts_without_false_completeness(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.make_snapshot(root)
            summary = collector.normalize(root)
            self.assertEqual(summary["wandb_by_project"]["distributed_training_miners"]["zero_reported"], 1)
            self.assertEqual(summary["wandb_by_project"]["distributed_training_miners"]["count_unknown"], 1)
            self.assertEqual(summary["wandb_by_project"]["distributed_training_miners"]["count_matches_reported"], 1)
            with (root / "normalized/wandb_runs.csv").open() as handle:
                rows = {row["run_id"]: row for row in csv.DictReader(handle)}
            self.assertEqual(rows["r1"]["history_coverage"], "zero_reported")
            self.assertEqual(rows["r2"]["history_coverage"], "count_unknown")
            self.assertEqual(rows["r3"]["history_coverage"], "count_matches_reported")
            self.assertNotIn("history_complete", rows["r3"])
            self.assertTrue(summary["wandb_project_coverage"]["distributed_training_miners"]["coverage_complete_by_public_run_count"])
            with collector.gzip.open(root / "normalized/wandb_history_sampled.jsonl.gz", "rt") as handle:
                sampled = [json.loads(line) for line in handle]
            self.assertEqual(len(sampled), 2)
            self.assertEqual(sampled[0]["metrics"]["loss"], 1.0)
            self.assertNotIn("loss", sampled[0])
            self.assertEqual(sampled[1]["metrics"]["accuracy"], 0.5)
            self.assertNotIn("loss", sampled[1]["metrics"])

    def test_full_history_summary_uses_run_rows_when_plan_files_are_absent(self):
        run_rows = [
            {"project": "p", "run_id": "matched", "history_coverage": "returned_count_differs_from_reported", "history_line_count": 2},
            {"project": "p", "run_id": "partial", "history_coverage": "count_unknown", "history_line_count": None},
            {"project": "p", "run_id": "unscanned", "history_coverage": "returned_count_differs_from_reported", "history_line_count": 4},
        ]
        coverage = {
            "p/matched": {
                "project": "p", "run_id": "matched", "last_step": 1,
                "expected_step_ranges": 2, "step_ranges_returned": 2,
                "all_expected_step_ranges_returned": True,
                "unique_rows_match_reported_line_count": True,
                "all_ranges_and_row_count_match": True, "unique_row_fingerprints": 2,
            },
            "p/partial": {
                "project": "p", "run_id": "partial", "last_step": 2,
                "expected_step_ranges": 1, "step_ranges_returned": 0,
                "all_expected_step_ranges_returned": False,
                "unique_rows_match_reported_line_count": False,
                "all_ranges_and_row_count_match": False, "unique_row_fingerprints": 0,
            },
        }
        entries = [
            {"history_scan": {"project": "p", "run_id": "matched", "windows": [
                {"min_step": 0, "max_step": 1}, {"min_step": 1, "max_step": 2},
            ]}},
            {"history_scan": {"project": "p", "run_id": "partial", "windows": [
                {"min_step": 0, "max_step": 3},
            ]}},
        ]
        summary = collector.summarize_full_history(run_rows, coverage, entries)
        self.assertEqual(summary["wandb_full_history_candidate_runs"], 3)
        self.assertEqual(summary["wandb_full_history_scanned_candidates"], 2)
        self.assertEqual(summary["wandb_full_history_requested_ranges"], 3)
        self.assertEqual(summary["wandb_full_history_ranges_and_count_match"], 1)
        self.assertEqual(summary["wandb_full_history_unresolved_runs"], 2)
        unscanned = next(row for row in summary["wandb_full_history_unresolved_plan_runs"] if row["run_id"] == "unscanned")
        self.assertEqual(unscanned["scan_skip_reason"], "no archived full-history scan")

    def test_validator_history_page_falls_back_after_response_cap(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = collector.Collector(Path(tmp) / "snapshot", 1_000_000, False)
            requests = []

            def fake_fetch(self, request_id, url, relative, **kwargs):
                payload = kwargs["payload"]
                query = json.loads(payload)["query"]
                samples = int(__import__("re").search(r"history\(samples:(\d+)\)", query).group(1))
                requests.append((request_id, samples, query))
                if "page-0000" in request_id and samples == 5000:
                    failed = self._record_failure(
                        request_id, url, kwargs["method"], relative, None,
                        "RuntimeError: decompressed response exceeded 134217728-byte safety cap", payload
                    )
                    return failed, None
                page = 1 if "page-0001" in request_id else 0
                body = json.dumps({
                    "data": {"project": {
                        "runCount": 2,
                        "runs": {
                            "edges": [{"cursor": f"v{page + 1}", "node": {
                                "name": f"v{page + 1}", "historyLineCount": 5100,
                                "history": [json.dumps({"_step": 0})],
                            }}],
                            "pageInfo": {
                                "hasNextPage": page == 0,
                                "endCursor": "cursor-A" if page == 0 else "cursor-B",
                            },
                        },
                    }}
                }).encode()
                target = self.output / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(body)
                entry = {
                    "id": request_id, "url": url, "method": "POST",
                    "request_body": payload.decode(), "path": relative, "status": 200,
                    "sha256": hashlib.sha256(body).hexdigest(), "bytes": len(body),
                    "retrieved_at": collector.utc_now(),
                }
                self._record(entry)
                return entry, body

            with patch.object(collector, "WANDB_PROJECTS", ("distributed_training_validators",)):
                with patch.object(collector.Collector, "fetch", fake_fetch):
                    collector.collect_wandb(instance)
            self.assertEqual([samples for _, samples, _ in requests], [5000, 100, 100])
            self.assertIn('after:"cursor-A"', requests[-1][2])
            failed = instance.entry_by_id["wandb-distributed_training_validators-page-0000"]
            saved = instance.entry_by_id["wandb-distributed_training_validators-page-0000-samples-0100"]
            self.assertIn("decompressed response exceeded", failed["error"])
            self.assertEqual(saved["status"], 200)
            self.assertIn("history(samples:100)", saved["request_body"])
            self.assertEqual(
                instance.entry_by_id["wandb-distributed_training_validators-page-0001-samples-0100"]["status"],
                200,
            )

    def test_validator_resume_skips_previously_capped_samples(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "snapshot"
            root.mkdir(parents=True)
            base_id = "wandb-distributed_training_validators-page-0000"
            failure_body = collector.graphql_body("distributed_training_validators", None, 1, 5000).decode()
            (root / "manifest.json").write_text(json.dumps({"entries": [{
                "id": base_id,
                "url": collector.WANDB_GRAPHQL,
                "method": "POST",
                "request_body": failure_body,
                "path": None,
                "status": None,
                "sha256": None,
                "bytes": 0,
                "retrieved_at": collector.utc_now(),
                "error": "RuntimeError: decompressed response exceeded 134217728-byte safety cap",
            }]}))
            instance = collector.Collector(root, 1_000_000, True)
            requested_samples = []

            def fake_fetch(self, request_id, url, relative, **kwargs):
                payload = kwargs["payload"]
                query = json.loads(payload)["query"]
                samples = int(__import__("re").search(r"history\(samples:(\d+)\)", query).group(1))
                requested_samples.append(samples)
                body = json.dumps({"data": {"project": {"runCount": 1, "runs": {
                    "edges": [{"cursor": "v1", "node": {"name": "v1", "historyLineCount": 0, "history": []}}],
                    "pageInfo": {"hasNextPage": False, "endCursor": "v1"},
                }}}}).encode()
                target = self.output / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(body)
                entry = {
                    "id": request_id, "url": url, "method": "POST",
                    "request_body": payload.decode(), "path": relative, "status": 200,
                    "sha256": hashlib.sha256(body).hexdigest(), "bytes": len(body),
                    "retrieved_at": collector.utc_now(),
                }
                self._record(entry)
                return entry, body

            with patch.object(collector, "WANDB_PROJECTS", ("distributed_training_validators",)):
                with patch.object(collector.Collector, "fetch", fake_fetch):
                    collector.collect_wandb(instance)
            self.assertEqual(requested_samples, [100])

    def test_excluded_history_page_retries_metadata_only_and_preserves_cursor(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = collector.Collector(Path(tmp) / "snapshot", 1_000_000, False)
            request_log = []

            def fake_fetch(self, request_id, url, relative, **kwargs):
                payload = kwargs["payload"]
                query = json.loads(payload)["query"]
                request_log.append((request_id, query))
                if "metadata-only" not in request_id and "page-0000" in request_id:
                    unsafe = json.dumps({"data": {"project": {"runs": {"edges": [
                        {"node": {"name": "v1", "history": [json.dumps({"api_token": "never-save-this"})]}}
                    ]}}}}).encode()
                    failed = self._record_failure(
                        request_id, url, kwargs["method"], relative, 200,
                        "credential-like key-name screen matched; response not archived", payload
                    )
                    failed["application_status"] = "excluded"
                    self.save_manifest()
                    return failed, unsafe
                page = 1 if "page-0001" in request_id else 0
                body_doc = {"data": {"project": {"runCount": 2, "runs": {
                    "edges": [{"cursor": f"v{page + 1}", "node": {
                        "name": f"v{page + 1}", "historyLineCount": 0,
                        **({} if "metadata-only" in request_id else {"history": []}),
                    }}],
                    "pageInfo": {"hasNextPage": page == 0, "endCursor": "cursor-A" if page == 0 else "cursor-B"},
                }}}}
                body = json.dumps(body_doc).encode()
                target = self.output / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(body)
                entry = {
                    "id": request_id, "url": url, "method": "POST",
                    "request_body": payload.decode(), "path": relative, "status": 200,
                    "sha256": hashlib.sha256(body).hexdigest(), "bytes": len(body),
                    "retrieved_at": collector.utc_now(),
                }
                self._record(entry)
                return entry, body

            with patch.object(collector, "WANDB_PROJECTS", ("distributed_training_validators",)):
                with patch.object(collector.Collector, "fetch", fake_fetch):
                    collector.collect_wandb(instance)
            self.assertEqual(len(request_log), 3)
            excluded_id, excluded_query = request_log[0]
            metadata_id, metadata_query = request_log[1]
            next_id, next_query = request_log[2]
            self.assertIn("history(samples:", excluded_query)
            self.assertIn("-metadata-only", metadata_id)
            self.assertNotIn("history(", metadata_query)
            self.assertIn('after:"cursor-A"', next_query)
            self.assertIn("page-0001", next_id)
            excluded = instance.entry_by_id[excluded_id]
            self.assertEqual(excluded["application_status"], "excluded")
            self.assertIsNone(excluded["path"])
            self.assertNotIn("never-save-this", json.dumps(instance.entries))
            metadata_path = instance.entry_by_id[metadata_id]["path"]
            self.assertFalse((instance.output / metadata_path).read_text().find("api_token") >= 0)

    def test_full_history_coverage_uses_recorded_validator_window_size(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            relative = "raw/window.json"
            target = root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(json.dumps({"data": {"project": {"run": {"h000": [], "h001": []}}}}))
            keys_relative = "raw/wandb/kmfoda/distributed_training_validators/history-keys-r1.json"
            keys_payload, _ = collector.history_keys_body("distributed_training_validators", ["v1"])
            keys_body = json.dumps({"data": {"project": {"r000": {"historyKeys": {"lastStep": 199}}}}}).encode()
            keys_target = root / keys_relative
            keys_target.parent.mkdir(parents=True, exist_ok=True)
            keys_target.write_bytes(keys_body)
            keys_entry = {
                "id": "wandb-distributed_training_validators-history-keys-run-v1",
                "request_body": keys_payload.decode(),
                "status": 200,
                "path": keys_relative,
                "bytes": len(keys_body),
                "sha256": hashlib.sha256(keys_body).hexdigest(),
            }
            entry = {
                "id": "window-response",
                "status": 200,
                "path": relative,
                "history_scan": {
                    "project": "distributed_training_validators",
                    "run_id": "v1",
                    "history_line_count": None,
                    "last_step": 199,
                    "window_size": 100,
                    "windows": [
                        {"alias": "h000", "min_step": 0, "max_step": 100},
                        {"alias": "h001", "min_step": 100, "max_step": 200},
                    ],
                },
            }
            coverage = collector.full_history_coverage(root, [entry, keys_entry])["distributed_training_validators/v1"]
            self.assertEqual(coverage["expected_step_ranges"], 2)
            self.assertEqual(coverage["window_size"], 100)
            self.assertTrue(coverage["last_step_verified"])
            self.assertTrue(coverage["all_expected_step_ranges_returned"])

    def test_normalize_deduplicates_index_strategies_and_verifies_raw_hashes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.make_snapshot(root)
            summary = collector.normalize(root)
            self.assertEqual(summary["mech1_strategy_rows"], 1)
            raw = root / "raw/mech1/strategies-block-9.json"
            raw.write_text("tampered")
            with self.assertRaisesRegex(RuntimeError, "hash verification"):
                collector.normalize(root)

    def test_normalize_reports_withheld_history_and_recovered_safe_identity(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.make_snapshot(root)
            manifest_path = root / "manifest.json"
            manifest = json.loads(manifest_path.read_text())
            relative = "raw/wandb/kmfoda/distributed_training_validators/page-0003-metadata-only.json"
            body = json.dumps({"data": {"project": {"runCount": 1, "runs": {
                "edges": [{"node": {"name": "validator-run", "historyLineCount": 3}}],
                "pageInfo": {"hasNextPage": False, "endCursor": "validator-run"},
            }}}}).encode()
            target = root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(body)
            manifest["entries"].extend([
                {
                    "id": "wandb-distributed_training_validators-page-0003-samples-0100",
                    "url": collector.WANDB_GRAPHQL,
                    "status": 200,
                    "application_status": "excluded",
                    "path": None,
                    "error": "credential-like key-name screen matched; matched key paths (no values retained): $.api_token (key `api_token`)",
                    "bytes": 0,
                },
                {
                    "id": "wandb-distributed_training_validators-page-0003-metadata-only",
                    "url": collector.WANDB_GRAPHQL,
                    "status": 200,
                    "path": relative,
                    "bytes": len(body),
                    "sha256": hashlib.sha256(body).hexdigest(),
                    "request_body": collector.graphql_body(
                        "distributed_training_validators", None, 1, include_history=False
                    ).decode(),
                },
            ])
            manifest_path.write_text(json.dumps(manifest))
            summary = collector.normalize(root)
            withheld = summary["wandb_excluded_history_responses"][0]
            self.assertEqual(withheld["project"], "distributed_training_validators")
            self.assertEqual(withheld["run_ids"], ["validator-run"])
            self.assertTrue(withheld["metadata_only_recovered"])
            self.assertNotIn("token-value", json.dumps(summary))

    def test_resume_refuses_missing_or_corrupt_success_body(self):
        for write_body in (False, True):
            with self.subTest(write_body=write_body), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp) / "snapshot"
                (root / "raw").mkdir(parents=True)
                body = b"saved response"
                entry = {
                    "id": "saved",
                    "path": "raw/saved.json",
                    "status": 200,
                    "bytes": len(body),
                    "sha256": hashlib.sha256(body).hexdigest(),
                }
                (root / "manifest.json").write_text(json.dumps({"entries": [entry]}))
                target = root / entry["path"]
                if write_body:
                    target.write_bytes(b"changed")
                instance = collector.Collector(root, 1000, True)
                with self.assertRaisesRegex(RuntimeError, "saved response"):
                    instance.fetch("saved", "https://example.invalid", entry["path"])

    def test_retry_after_over_30_seconds_is_deferred_without_early_retry(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = collector.Collector(Path(tmp) / "snapshot", 1000, False)
            headers = email.message.Message()
            headers["Retry-After"] = "60"
            response = urllib.error.HTTPError("https://example.invalid", 429, "Too Many", headers, None)
            with patch.object(collector.urllib.request, "urlopen", side_effect=response):
                entry, body = instance.fetch("rate-limited", "https://example.invalid", "raw/x")
            self.assertIsNone(body)
            self.assertIn("retry deferred", entry["error"])

    def test_byte_budget_rejects_oversized_response(self):
        with self.assertRaisesRegex(RuntimeError, "budget exhausted"):
            collector.Collector._read_limited(io.BytesIO(b"12345"), 4)
        with patch.object(collector, "MAX_RESPONSE_BYTES", 4):
            with self.assertRaisesRegex(RuntimeError, "single response exceeded"):
                collector.Collector._read_limited(io.BytesIO(b"12345"), 100)

    def test_gzip_response_is_archived_compressed_and_decoded_for_readers(self):
        class FakeResponse(io.BytesIO):
            status = 200
            headers = {"Content-Encoding": "gzip"}

            def __enter__(self):
                return self

            def __exit__(self, *args):
                self.close()

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "snapshot"
            instance = collector.Collector(root, 1_000_000, False)
            document = {"data": {"project": {"runCount": 1}}}
            decoded = json.dumps(document).encode()
            compressed = gzip.compress(decoded, mtime=0)
            response = FakeResponse(compressed)
            with patch.object(collector.urllib.request, "urlopen", return_value=response) as open_request:
                entry, body = instance.fetch(
                    "gzip-test",
                    collector.WANDB_GRAPHQL,
                    "raw/wandb/gzip-test.json",
                    method="POST",
                    payload=b'{"query":"safe"}',
                    validate_wandb=True,
                )
            self.assertEqual(open_request.call_args.args[0].get_header("Accept-encoding"), "gzip")
            self.assertEqual(body, compressed)
            self.assertEqual((root / entry["path"]).read_bytes(), compressed)
            self.assertEqual(entry["content_encoding"], "gzip")
            self.assertEqual(entry["bytes"], len(compressed))
            self.assertEqual(entry["decoded_bytes"], len(decoded))
            self.assertEqual(collector.parse_json(body), document)
            self.assertEqual(collector.raw_doc(root, entry["path"]), document)

    def test_gzip_decompression_has_an_output_safety_cap(self):
        compressed = gzip.compress(b"x" * 11, mtime=0)
        with patch.object(collector, "MAX_RESPONSE_BYTES", 10):
            with self.assertRaisesRegex(RuntimeError, "decompressed response exceeded"):
                collector.decode_response_body(compressed, "gzip")

    def test_resume_skips_a_known_decompressed_cap_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "snapshot"
            root.mkdir(parents=True)
            request_id = "known-cap"
            (root / "manifest.json").write_text(json.dumps({"entries": [{
                "id": request_id,
                "url": collector.WANDB_GRAPHQL,
                "method": "POST",
                "request_body": '{"query":"safe"}',
                "path": None,
                "status": None,
                "bytes": 0,
                "sha256": None,
                "error": "RuntimeError: decompressed response exceeded 134217728-byte safety cap",
            }]}))
            instance = collector.Collector(root, 1_000_000, True)
            with patch.object(collector.urllib.request, "urlopen", side_effect=AssertionError("must not retry")):
                entry, body = instance.fetch(
                    request_id, collector.WANDB_GRAPHQL, "raw/cap.json", method="POST", payload=b'{"query":"safe"}'
                )
            self.assertTrue(collector.is_response_cap_error(entry["error"]))
            self.assertIsNone(body)

    def test_full_history_cap_falls_back_to_smaller_batches_and_preserves_last_step(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "snapshot"
            instance = collector.Collector(root, 10_000_000, False)
            page_rel = "raw/wandb/kmfoda/distributed_training_validators/page-0000-samples-0100.json"
            page_doc = {"data": {"project": {"runs": {"edges": [{"node": {
                "name": "v1", "historyLineCount": 510, "history": [json.dumps({"_step": 0})],
            }}]}}}}
            page_body = json.dumps(page_doc).encode()
            page_path = root / page_rel
            page_path.parent.mkdir(parents=True, exist_ok=True)
            page_path.write_bytes(page_body)
            page_entry = {
                "id": "wandb-distributed_training_validators-page-0000-samples-0100",
                "path": page_rel,
                "status": 200,
                "bytes": len(page_body),
                "sha256": hashlib.sha256(page_body).hexdigest(),
            }
            instance.entries.append(page_entry)
            instance.entry_by_id[page_entry["id"]] = page_entry
            instance.save_manifest()
            window_alias_counts = []

            def fake_fetch(self, request_id, url, relative, **kwargs):
                payload = kwargs["payload"]
                query = json.loads(payload)["query"]
                if "history-keys" in request_id:
                    response_doc = {"data": {"project": {
                        "r000": {"historyKeys": json.dumps({"lastStep": 499})}
                    }}}
                else:
                    alias_count = len(__import__("re").findall(r"h\d{3}: history\(", query))
                    window_alias_counts.append(alias_count)
                    if alias_count == 5:
                        failure = self._record_failure(
                            request_id, url, "POST", relative, None,
                            "RuntimeError: decompressed response exceeded 134217728-byte safety cap", payload
                        )
                        return failure, None
                    response_doc = {"data": {"project": {"run": {
                        f"h{index:03d}": [] for index in range(alias_count)
                    }}}}
                body = json.dumps(response_doc).encode()
                target = self.output / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(body)
                entry = {
                    "id": request_id,
                    "url": url,
                    "method": "POST",
                    "request_body": payload.decode(),
                    "path": relative,
                    "status": 200,
                    "sha256": hashlib.sha256(body).hexdigest(),
                    "bytes": len(body),
                    "retrieved_at": collector.utc_now(),
                }
                self._record(entry)
                return entry, body

            with patch.object(collector, "WANDB_FULL_HISTORY_PROJECTS", ("distributed_training_validators",)):
                with patch.object(collector.Collector, "fetch", fake_fetch):
                    collector.collect_wandb_full_history(instance)
            self.assertEqual(window_alias_counts, [5, 2, 3])
            full_entries = [entry for entry in instance.entries if entry.get("history_scan")]
            self.assertEqual({entry["history_scan"]["last_step"] for entry in full_entries}, {499})
            coverage = collector.full_history_coverage(root, instance.entries)[
                "distributed_training_validators/v1"
            ]
            self.assertEqual(coverage["last_step"], 499)
            self.assertEqual(coverage["expected_step_ranges"], 5)
            self.assertEqual(coverage["step_ranges_returned"], 5)
            self.assertTrue(coverage["all_expected_step_ranges_returned"])

    def test_full_history_reuses_legacy_batch_named_window(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "snapshot"
            instance = collector.Collector(root, 10_000_000, False)
            page_rel = "raw/wandb/kmfoda/distributed_training_validators/page-0000-samples-0100.json"
            page_body = json.dumps({"data": {"project": {"runs": {"edges": [{"node": {
                "name": "v1", "historyLineCount": 200, "history": [],
            }}]}}}}).encode()
            page_path = root / page_rel
            page_path.parent.mkdir(parents=True, exist_ok=True)
            page_path.write_bytes(page_body)
            page_entry = {
                "id": "page",
                "path": page_rel,
                "status": 200,
                "bytes": len(page_body),
                "sha256": hashlib.sha256(page_body).hexdigest(),
            }
            legacy_rel = "raw/wandb/kmfoda/distributed_training_validators/history-windows/v1-batch-0000.json"
            legacy_body = json.dumps({"data": {"project": {"run": {"h000": []}}}}).encode()
            legacy_path = root / legacy_rel
            legacy_path.parent.mkdir(parents=True, exist_ok=True)
            legacy_path.write_bytes(legacy_body)
            legacy_entry = {
                "id": "wandb-distributed_training_validators-fullhist-v1-batch-0000",
                "path": legacy_rel,
                "status": 200,
                "bytes": len(legacy_body),
                "sha256": hashlib.sha256(legacy_body).hexdigest(),
                "history_scan": {
                    "project": "distributed_training_validators",
                    "run_id": "v1",
                    "history_line_count": 200,
                    "last_step": 100,
                    "window_size": 100,
                    "windows": [{
                        "alias": "h000", "min_step": 0, "max_step": 100,
                        "alias_returned": True, "returned_rows": 0,
                    }],
                },
            }
            for item in (page_entry, legacy_entry):
                instance.entries.append(item)
                instance.entry_by_id[item["id"]] = item
            instance.save_manifest()
            full_queries = []

            def fake_fetch(self, request_id, url, relative, **kwargs):
                payload = kwargs["payload"]
                if "history-keys" in request_id:
                    response_doc = {"data": {"project": {
                        "r000": {"historyKeys": json.dumps({"lastStep": 199})}
                    }}}
                else:
                    query = json.loads(payload)["query"]
                    full_queries.append(query)
                    response_doc = {"data": {"project": {"run": {"h000": []}}}}
                body = json.dumps(response_doc).encode()
                target = self.output / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(body)
                entry = {
                    "id": request_id, "url": url, "method": "POST",
                    "request_body": payload.decode(), "path": relative, "status": 200,
                    "sha256": hashlib.sha256(body).hexdigest(), "bytes": len(body),
                }
                self._record(entry)
                return entry, body

            with patch.object(collector, "WANDB_FULL_HISTORY_PROJECTS", ("distributed_training_validators",)):
                with patch.object(collector.Collector, "fetch", fake_fetch):
                    collector.collect_wandb_full_history(instance)
            self.assertEqual(len(full_queries), 1)
            self.assertIn("history(minStep:100,maxStep:200,samples:100)", full_queries[0])
            coverage = collector.full_history_coverage(root, instance.entries)[
                "distributed_training_validators/v1"
            ]
            self.assertEqual(coverage["last_step"], 199)
            self.assertEqual(coverage["step_ranges_returned"], 2)
            self.assertTrue(coverage["all_expected_step_ranges_returned"])

    def test_full_history_missing_alias_differs_from_empty_window(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)

            def coverage_for(response):
                relative = "raw/window.json"
                path = root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(json.dumps({"data": {"project": {"run": response}}}))
                entry = {
                    "id": "window-response",
                    "status": 200,
                    "path": relative,
                    "history_scan": {
                        "project": "distributed_training_miners",
                        "run_id": "r1",
                        "history_line_count": 1,
                        "last_step": 0,
                        "windows": [{"alias": "h000", "min_step": 0, "max_step": 1, "returned_rows": 0}],
                    },
                }
                keys_relative = "raw/wandb/kmfoda/distributed_training_miners/history-keys-r1.json"
                keys_payload, _ = collector.history_keys_body("distributed_training_miners", ["r1"])
                keys_body = json.dumps({"data": {"project": {"r000": {"historyKeys": {"lastStep": 0}}}}}).encode()
                keys_path = root / keys_relative
                keys_path.parent.mkdir(parents=True, exist_ok=True)
                keys_path.write_bytes(keys_body)
                keys_entry = {
                    "id": "wandb-distributed_training_miners-history-keys-run-r1",
                    "request_body": keys_payload.decode(),
                    "status": 200,
                    "path": keys_relative,
                    "bytes": len(keys_body),
                    "sha256": hashlib.sha256(keys_body).hexdigest(),
                }
                return collector.full_history_coverage(root, [entry, keys_entry])["distributed_training_miners/r1"]

            missing = coverage_for({})
            empty = coverage_for({"h000": []})
            self.assertEqual(missing["step_ranges_returned"], 0)
            self.assertFalse(missing["all_expected_step_ranges_returned"])
            self.assertEqual(empty["step_ranges_returned"], 1)
            self.assertTrue(empty["all_expected_step_ranges_returned"])
            self.assertFalse(empty["all_ranges_and_row_count_match"])

    def test_full_history_does_not_trust_unarchived_last_step(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            relative = "raw/window.json"
            target = root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(json.dumps({"data": {"project": {"run": {"h000": []}}}}))
            scan = {
                "id": "full-window",
                "status": 200,
                "path": relative,
                "history_scan": {
                    "project": "distributed_training_miners",
                    "run_id": "r1",
                    "history_line_count": 100,
                    "last_step": 99,
                    "source_last_step": 99,
                    "window_size": 100,
                    "history_keys_raw_ref": "raw/wandb/kmfoda/distributed_training_miners/history-keys-excluded.json",
                    "windows": [{"alias": "h000", "min_step": 0, "max_step": 100}],
                },
            }
            excluded = {
                "id": "wandb-distributed_training_miners-history-keys-page-0000",
                "status": 200,
                "application_status": "excluded",
                "path": None,
                "history_key_runs": [{"project": "distributed_training_miners", "run_id": "r1", "last_step": 99}],
            }
            coverage = collector.full_history_coverage(root, [scan, excluded])["distributed_training_miners/r1"]
            self.assertIsNone(coverage["last_step"])
            self.assertFalse(coverage["last_step_verified"])
            self.assertEqual(coverage["expected_step_ranges"], 0)
            self.assertFalse(coverage["all_expected_step_ranges_returned"])

    def test_excluded_history_keys_batch_is_split_and_excluded_body_is_never_used(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "snapshot"
            instance = collector.Collector(root, 1_000_000, False)
            page_rel = "raw/wandb/kmfoda/distributed_training_miners/page-0000.json"
            page_doc = {"data": {"project": {"runs": {"edges": [
                {"node": {"name": "r-safe", "historyLineCount": 1, "history": []}},
                {"node": {"name": "r-unsafe", "historyLineCount": 1, "history": []}},
            ]}}}}
            page_body = json.dumps(page_doc).encode()
            page_path = root / page_rel
            page_path.parent.mkdir(parents=True, exist_ok=True)
            page_path.write_bytes(page_body)
            page_entry = {
                "id": "seed-page", "path": page_rel, "status": 200,
                "bytes": len(page_body), "sha256": hashlib.sha256(page_body).hexdigest(),
            }
            instance._record(page_entry)
            requests = []

            def save_response(self, request_id, url, relative, payload, doc):
                body = json.dumps(doc).encode()
                target = self.output / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(body)
                entry = {
                    "id": request_id, "url": url, "method": "POST",
                    "request_body": payload.decode(), "path": relative, "status": 200,
                    "sha256": hashlib.sha256(body).hexdigest(), "bytes": len(body),
                    "retrieved_at": collector.utc_now(),
                }
                self._record(entry)
                return entry, body

            def fake_fetch(self, request_id, url, relative, **kwargs):
                payload = kwargs["payload"]
                requests.append(request_id)
                if "history-keys-page" in request_id:
                    failed = self._record_failure(
                        request_id, url, "POST", relative, 200,
                        "credential-like key-name screen matched; response not archived", payload,
                    )
                    failed["application_status"] = "excluded"
                    self.save_manifest()
                    # This body must never be parsed or retained by the scanner.
                    return failed, json.dumps({"data": {"project": {
                        "r000": {"historyKeys": {"lastStep": 9999, "api_token": "do-not-use"}},
                        "r001": {"historyKeys": {"lastStep": 9999}},
                    }}}).encode()
                if "history-keys-run-r-safe" in request_id:
                    return save_response(self, request_id, url, relative, payload, {
                        "data": {"project": {"r000": {"historyKeys": {"lastStep": 0}}}}
                    })
                if "history-keys-run-r-unsafe" in request_id:
                    failed = self._record_failure(
                        request_id, url, "POST", relative, 200,
                        "credential-like key-name screen matched; response not archived", payload,
                    )
                    failed["application_status"] = "excluded"
                    self.save_manifest()
                    return failed, json.dumps({"data": {"project": {"r000": {"historyKeys": {
                        "lastStep": 0, "api_token": "do-not-use"
                    }}}}}).encode()
                return save_response(self, request_id, url, relative, payload, {
                    "data": {"project": {"run": {"h000": []}}}
                })

            with patch.object(collector, "WANDB_FULL_HISTORY_PROJECTS", ("distributed_training_miners",)):
                with patch.object(collector.Collector, "fetch", new=fake_fetch):
                    collector.collect_wandb_full_history(instance)
            self.assertIn("wandb-distributed_training_miners-history-keys-run-r-safe", requests)
            self.assertIn("wandb-distributed_training_miners-history-keys-run-r-unsafe", requests)
            self.assertNotIn("api_token", json.dumps(instance.entries))
            batch_entry = instance.entry_by_id["wandb-distributed_training_miners-history-keys-page-0000"]
            self.assertNotIn("history_key_runs", batch_entry)
            self.assertEqual(batch_entry["resolved_run_ids"], ["r-safe"])
            self.assertEqual(batch_entry["excluded_run_ids"], ["r-unsafe"])
            plan = json.loads((root / "wandb-full-history-plan-distributed_training_miners.json").read_text())
            per_run = {row["run_id"]: row for row in plan}
            self.assertEqual(per_run["r-safe"]["last_step"], 0)
            self.assertIsNone(per_run["r-unsafe"]["last_step"])
            self.assertIn("excluded", per_run["r-unsafe"]["scan_skip_reason"])

    def test_resume_skips_previously_excluded_response(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "snapshot"
            root.mkdir(parents=True)
            request_id = "unsafe-response"
            (root / "manifest.json").write_text(json.dumps({"entries": [{
                "id": request_id, "url": "https://example.invalid", "method": "POST",
                "request_body": "{}", "path": None, "status": 200,
                "application_status": "excluded", "bytes": 0,
            }]}))
            instance = collector.Collector(root, 1_000_000, True)
            with patch("urllib.request.urlopen", side_effect=AssertionError("must not refetch excluded body")):
                entry, body = instance.fetch(request_id, "https://example.invalid", "raw/unsafe.json")
            self.assertEqual(entry["application_status"], "excluded")
            self.assertIsNone(body)

    def test_unknown_history_count_is_scanned_and_empty_window_does_not_stop_later_window(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            instance = collector.Collector(root, 1_000_000, False)
            page_rel = "raw/wandb/kmfoda/distributed_training_miners/page-0000.json"
            page_body = json.dumps(
                {
                    "data": {
                        "project": {
                            "runs": {
                                "edges": [
                                    {"node": {"name": "unknown-count", "historyLineCount": None, "history": []}}
                                ]
                            }
                        }
                    }
                }
            ).encode()
            page_path = root / page_rel
            page_path.parent.mkdir(parents=True, exist_ok=True)
            page_path.write_bytes(page_body)
            seed = {
                "id": "seed-page",
                "path": page_rel,
                "status": 200,
                "bytes": len(page_body),
                "sha256": hashlib.sha256(page_body).hexdigest(),
            }
            instance.entries.append(seed)
            instance.entry_by_id[seed["id"]] = seed
            instance.save_manifest()
            requested = []

            def fake_fetch(self, request_id, url, relative, **kwargs):
                payload = kwargs.get("payload") or b""
                requested.append(request_id)
                if "history-keys" in request_id:
                    response = {"data": {"project": {"r000": {"historyKeys": json.dumps({"lastStep": 1000})}}}}
                else:
                    response = {"data": {"project": {"run": {"h000": [], "h001": [json.dumps({"_step": 1000})]}}}}
                body = json.dumps(response).encode()
                target = root / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(body)
                entry = {
                    "id": request_id,
                    "url": url,
                    "method": "POST",
                    "request_body": payload.decode("utf-8"),
                    "path": relative,
                    "status": 200,
                    "sha256": hashlib.sha256(body).hexdigest(),
                    "bytes": len(body),
                    "retrieved_at": collector.utc_now(),
                }
                self._record(entry)
                return entry, body

            with patch.object(collector.Collector, "fetch", new=fake_fetch):
                collector.collect_wandb_full_history(instance)
            self.assertEqual(len(requested), 2)  # one historyKeys query plus one batched windows query
            window_requests = [request_id for request_id in requested if "fullhist" in request_id]
            self.assertEqual(len(window_requests), 1)
            plan_path = root / "wandb-full-history-plan-distributed_training_miners.json"
            plan = json.loads(plan_path.read_text())[0]
            self.assertIsNone(plan["history_line_count"])
            self.assertEqual(plan["last_step"], 1000)
            self.assertEqual([row["returned_rows"] for row in plan["window_request_refs"]], [0, 1])
            self.assertIn("history(minStep:0,maxStep:1000,samples:1000)",
                          instance.entry_by_id[window_requests[0]]["request_body"])

    def test_repeated_wandb_cursor_stops_pagination(self):
        def response(cursor):
            doc = {
                "data": {
                    "project": {
                        "runCount": 1,
                        "runs": {
                            "edges": [],
                            "pageInfo": {"hasNextPage": True, "endCursor": cursor},
                        },
                    }
                }
            }
            return json.dumps(doc).encode()

        class FakeCollector:
            def __init__(self):
                self.entry_by_id = {}
                self.requests = 0

            def fetch(self, request_id, url, relative, **kwargs):
                self.requests += 1
                entry = {"id": request_id, "status": 200}
                self.entry_by_id[request_id] = entry
                return entry, response("cursor-A")

            def save_manifest(self, **kwargs):
                pass

        fake = FakeCollector()
        with patch.object(collector, "WANDB_PROJECTS", ("test_project",)):
            collector.collect_wandb(fake, batch_size=1)
        self.assertEqual(fake.requests, 2)
        self.assertEqual(
            fake.entry_by_id["wandb-test_project-page-0001"]["application_status"],
            "repeated_pagination_cursor",
        )


if __name__ == "__main__":
    unittest.main()
