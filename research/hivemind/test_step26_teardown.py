"""Offline tests for Step 26 measurement-scope teardown auditing and recovery."""
from __future__ import annotations

import json
import sys
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import Mock, patch

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

import resume_step26_audit as resume
import step26_joint_sweep as frozen
import step26_teardown_audit as teardown


def write_json(path: Path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")


def make_failed_suite(suite: Path):
    suite.mkdir(parents=True, exist_ok=True)
    jobs = frozen.make_jobs("pilot")
    failed = next(job for job in jobs if frozen.trial_name(job) == resume.FAILED_TRIAL)
    write_json(suite / "suite.json", {"experiment": "step26", "phase": "pilot", "jobs": jobs})
    write_json(suite / "provenance.json", {"fixture": True})
    write_json(suite / "halt.json", {"trial": resume.FAILED_TRIAL, "all_peers_shutdown": True})
    trial = suite / resume.FAILED_TRIAL
    write_json(trial / "config.json", {"fixture": "original"})
    (trial / "events.jsonl").write_text('{"kind":"trace_closed"}\n', encoding="utf-8")
    write_json(trial / "summary.json", {"pending": [], "error": None})
    write_json(trial / "analysis.json", {"fixture": "native-analysis"})
    write_json(trial / "joint_analysis.json", {"fixture": "original-integrity-failure"})
    return jobs, failed, trial


def failed_state():
    jobs = frozen.make_jobs("pilot")
    rows = []
    for job in jobs:
        status = "blocked" if frozen.trial_name(job) == resume.FAILED_TRIAL else "completed"
        rows.append({"job": job, "trial_dir": frozen.trial_name(job), "status": status,
                     "file_sha256": {}})
    return {"valid": False, "jobs": rows,
            "counts": {"completed": len(jobs) - 1, "pending": 0, "interrupted": 0, "blocked": 1}}


def trace_event(kind, event_number, monotonic, *, peer="X", detail=None, pid=41):
    return {"kind": kind, "pid": pid, "event_number": event_number,
            "monotonic": monotonic, "wall_time": 100.0 + monotonic,
            "peer": peer, "other": "", "detail": detail or {}}


def complete_activity_trace():
    return [
        trace_event("step_start", 1, 1.0, detail={"name": "X", "sequence": 1}),
        trace_event("step_result", 2, 2.0, detail={"name": "X", "sequence": 1}),
        trace_event("search_span_start", 3, 3.0, detail={"search_id": "search-1"}),
        trace_event("search_span_end", 4, 4.0, detail={"search_id": "search-1"}),
        trace_event("ad_read_start", 5, 5.0, detail={"read_id": "read-1"}),
        trace_event("ad_read_end", 6, 6.0, detail={"read_id": "read-1"}),
        trace_event("join_attempt_start", 7, 7.0, detail={"join_id": "join-1"}),
        trace_event("join_attempt_end", 8, 8.0, detail={"join_id": "join-1"}),
        trace_event("ad_finished", 9, 9.0, detail={"active": True}),
        trace_event("trace_closed", 10, 100.0, detail={"error": None, "pending": []}),
    ]


def rpc_start(event_number, monotonic, rpc_id, rpc_type, *, pid=41, **changes):
    detail = {"rpc_id": rpc_id, "rpc_type": rpc_type, "delay_seconds": 0.15,
              "key_hashes": ["abc123"]}
    detail.update(changes)
    return trace_event("control_rpc_start", event_number, monotonic,
                       detail=detail, pid=pid)


def valid_native():
    return {"control_plane_pass": True, "trace_integrity_pass": True}


class TailClassificationTests(unittest.TestCase):
    def classify(self, events, native=None, configured_delay=0.15):
        return teardown.classify_tail(events, native or valid_native(), configured_delay)

    def test_post_activity_find_and_store_starts_are_narrowly_excluded(self):
        events = complete_activity_trace() + [
            rpc_start(11, 10.0, "rpc-find", "find"),
            rpc_start(12, 11.0, "rpc-store", "store", pid=42),
        ]
        result = self.classify(events)
        self.assertEqual({row["rpc_id"] for row in result["excluded"]}, {"rpc-find", "rpc-store"})
        self.assertEqual(result["unexcluded_start_only"], [])
        self.assertEqual(result["activity_cutoff_monotonic"], 9.0)
        self.assertEqual(result["trace_closed"]["monotonic"], 100.0)
        self.assertTrue(all("no completion" in row["reason"] for row in result["excluded"]))

    def test_measurement_boundary_and_post_activity_join_are_not_excluded(self):
        events = complete_activity_trace() + [
            rpc_start(11, 7.5, "inside-join", "find"),
            rpc_start(12, 10.0, "tail-join", "join", pid=42),
        ]
        result = self.classify(events)
        self.assertEqual(result["excluded"], [])
        self.assertEqual({row["rpc_id"] for row in result["unexcluded_start_only"]},
                         {"inside-join", "tail-join"})
        self.assertTrue(any("at or before" in row["reason"]
                            for row in result["unexcluded_start_only"]))
        self.assertTrue(any("only start-only find/store" in row["reason"]
                            for row in result["unexcluded_start_only"]))

    def test_partial_handler_and_response_streams_are_never_excluded(self):
        events = complete_activity_trace() + [
            rpc_start(11, 10.0, "partial-handler", "find"),
            trace_event("control_rpc_handler_enter", 12, 10.1,
                        detail={"rpc_id": "partial-handler"}),
            rpc_start(13, 11.0, "partial-response", "store", pid=42),
            trace_event("control_rpc_response", 14, 11.1, pid=42,
                        detail={"rpc_id": "partial-response", "response_index": 0}),
        ]
        result = self.classify(events)
        self.assertEqual(result["excluded"], [])

    def test_missing_native_pass_missing_close_and_incomplete_spans_block_exclusion(self):
        base = complete_activity_trace()
        candidate = rpc_start(11, 10.0, "candidate", "find")
        scenarios = (
            (base + [candidate], {"control_plane_pass": False, "trace_integrity_pass": True},
             "native control_plane_pass"),
            ([event for event in base if event["kind"] != "trace_closed"] + [candidate],
             valid_native(), "trace_closed"),
            ([event for event in base if event["kind"] != "ad_read_end"] + [candidate],
             valid_native(), "incomplete or malformed"),
        )
        for events, native, expected_gate in scenarios:
            with self.subTest(expected_gate=expected_gate):
                result = self.classify(events, native)
                self.assertEqual(result["excluded"], [])
                self.assertTrue(result["unexcluded_start_only"])
                self.assertTrue(any(expected_gate in gate for gate in result["eligibility_gates"]))
        incomplete = self.classify(scenarios[-1][0], scenarios[-1][1])
        self.assertFalse(incomplete["activity_spans_complete"])

    def test_malformed_or_case_mismatched_start_metadata_is_not_excluded(self):
        base = complete_activity_trace()
        scenarios = (
            rpc_start(11, 10.0, "bad-keys", "find", key_hashes=["ok", 3]),
            rpc_start(11, 10.0, "bad-delay", "find", delay_seconds=0.2),
        )
        for candidate, message in zip(scenarios, ("malformed key_hashes", "configured case delay")):
            with self.subTest(message=message):
                result = self.classify(base + [candidate])
                self.assertEqual(result["excluded"], [])
                self.assertEqual(len(result["unexcluded_start_only"]), 1)
                self.assertIn(message, result["unexcluded_start_only"][0]["reason"])

    def test_wrapper_removes_only_eligible_start_and_retains_partial_streams(self):
        eligible = rpc_start(11, 10.0, "tail-find", "find")
        partial = rpc_start(12, 11.0, "partial-store", "store", pid=42)
        partial_handler = trace_event("control_rpc_handler_enter", 13, 11.1, pid=42,
                                      detail={"rpc_id": "partial-store"})
        events = complete_activity_trace() + [eligible, partial, partial_handler]
        observed = {}

        def original(case, config, actual_events, by_kind, by_ref, native):
            observed["events"] = actual_events
            return {"pass_": True, "errors": [], "summary": {}}

        analyzer = types.SimpleNamespace(_rpc_integrity=original)
        teardown.install(analyzer)
        result = analyzer._rpc_integrity({"one_way_delay_seconds": 0.15}, {}, events, {}, {}, valid_native())
        refs = {f"{event['pid']}:{event['event_number']}" for event in observed["events"]}
        self.assertNotIn("41:11", refs)
        self.assertIn("42:12", refs)
        self.assertIn("42:13", refs)
        self.assertEqual(result["summary"]["teardown_tail_excluded_stream_count"], 1)


class ResumeAuditTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="step26_teardown_test_")
        self.addCleanup(self.temp.cleanup)
        self.suite = Path(self.temp.name) / "suite"
        self.jobs, self.failed_job, self.trial = make_failed_suite(self.suite)

    def test_dry_run_validates_without_writing_or_starting_peers(self):
        paths = [self.suite / "suite.json", self.suite / "provenance.json", self.suite / "halt.json",
                 *(self.trial / name for name in (*frozen.RAW_FILES, "analysis.json", "joint_analysis.json"))]
        before = {path: path.read_bytes() for path in paths}
        initial, pending = failed_state(), {"counts": {"pending": 4}}
        with patch.object(frozen, "inspect_suite", side_effect=[initial, pending]), \
             patch.object(frozen, "validate_sources"), \
             patch.object(resume, "corrected_result", return_value={"integrity_pass": True}) as correct, \
             patch.object(frozen, "run_suite") as run_suite:
            result = resume.prepare(self.suite, dry_run=True)

        self.assertTrue(result["valid"])
        self.assertTrue(result["correction_verified"])
        self.assertTrue(result["starts_no_peers"])
        self.assertEqual(result["pending"], 4)
        correct.assert_called_once_with(self.suite)
        run_suite.assert_not_called()
        self.assertFalse((self.suite / "audit_provenance.json").exists())
        self.assertFalse((self.suite / "audit_harness").exists())
        self.assertFalse((self.trial / "completion.json").exists())
        self.assertEqual({path: path.read_bytes() for path in paths}, before)

    def test_offline_correction_preserves_original_failure_and_all_raw_bytes(self):
        original_halt = (self.suite / "halt.json").read_bytes()
        original_joint = (self.trial / "joint_analysis.json").read_bytes()
        original_events = (self.trial / "events.jsonl").read_bytes()
        initial, completed = failed_state(), {"valid": True, "jobs": [],
                                              "counts": {"completed": len(self.jobs), "pending": 0,
                                                         "interrupted": 0, "blocked": 0}}
        corrected = {"integrity_pass": True, "summary_metrics": {"x_led_rate": 0.5}}
        with patch.object(frozen, "inspect_suite", side_effect=[initial, completed]), \
             patch.object(frozen, "validate_sources"), \
             patch.object(resume, "corrected_result", return_value=corrected):
            result = resume.prepare(self.suite, dry_run=False)
            provenance = frozen.read_json(self.suite / "audit_provenance.json")
            resume.validate_audit(self.suite, provenance)

        self.assertTrue(result["valid"])
        self.assertEqual((self.suite / "halt_original_failure.json").read_bytes(), original_halt)
        self.assertEqual((self.trial / resume.ORIGINAL).read_bytes(), original_joint)
        self.assertEqual((self.trial / "events.jsonl").read_bytes(), original_events)
        self.assertEqual(frozen.read_json(self.trial / "joint_analysis.json"), corrected)
        self.assertTrue(frozen.read_json(self.trial / "completion.json")["all_peers_shutdown"])
        for name in resume.AUDIT_FILES:
            self.assertEqual((self.suite / "audit_harness" / name).read_bytes(),
                             (frozen.ROOT / name).read_bytes())

    def test_corrected_result_rejects_a_changed_non_rpc_metric(self):
        suite = self.suite
        trial = self.trial
        write_json(suite / "suite.json", {"jobs": [self.failed_job]})
        write_json(trial / "config.json", {"fixture": True})
        (trial / "events.jsonl").write_text('{"kind":"trace_closed"}\n', encoding="utf-8")
        write_json(trial / "summary.json", {"pending": [], "error": None})
        original = {
            "case": self.failed_job["case"], "seed": self.failed_job["seed"],
            "duration": self.failed_job["duration"], "native_control_plane_pass": True,
            "trace_integrity_pass": True, "policy_integrity": {"pass_": True},
            "summary_metrics": {"x_led_rate": 0.5, "rpc_counts": {"find": 4}},
        }
        write_json(trial / resume.ORIGINAL, original)
        changed = dict(original, integrity_pass=True,
                       summary_metrics={"x_led_rate": 0.6, "rpc_counts": {"find": 5}})
        analyzer = types.ModuleType("analyze_step26_joint")
        analyzer.analyse_trial = lambda path: changed
        correction = types.ModuleType("step26_teardown_audit")
        correction.install = lambda analyzer_module: None

        with patch.object(frozen, "validate_trial_config"), \
             patch.object(frozen, "validate_trace_closed"), \
             patch.dict(sys.modules, {"analyze_step26_joint": analyzer,
                                      "step26_teardown_audit": correction}):
            with self.assertRaisesRegex(ValueError, "leadership/choice metric changed: x_led_rate"):
                resume.corrected_result(suite)


if __name__ == "__main__":
    unittest.main()
