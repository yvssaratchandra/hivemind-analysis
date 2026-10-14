"""Regression tests for the versioned Step 30 live-trace analyzer fixes."""
from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

import analyze_step30_harm_v2 as analyzer


SUITE = HERE / "results" / "step30_smoke_20261012_v2_a"
TRIALS = ("stock_2s_s3000", "renew_1_2_s3000")


def event(kind, event_number, monotonic, peer="", other="", **detail):
    return dict(kind=kind, peer=peer, other=other, pid=event_number + 100,
                event_number=event_number, monotonic=monotonic,
                wall_time=monotonic + 1000.0, detail=detail)


class NativeNoResponseWitnessTests(unittest.TestCase):
    def _lifecycle(self, exception="TimeoutError"):
        caller, destination = "peer-caller", "peer-server"
        join_id, step_id, expiration = "200:join1", "H0:1", 1200.0
        rows = [
            event("join_attempt_start", 1, 10.0, caller, destination,
                  join_id=join_id, step_id=step_id),
            event("native_client_wait", 2, 10.01, caller, destination,
                  join_id=join_id, step_id=step_id, phase="handshake",
                  request_expiration=expiration, timeout=3.0),
            event("control_rpc_start", 3, 10.02, destination, caller,
                  rpc_id="300:rpc1", caller_step_id=step_id,
                  request_expiration=expiration),
            event("native_client_exception", 4, 13.01, caller, destination,
                  join_id=join_id, step_id=step_id, phase="handshake",
                  request_expiration=expiration, error=exception),
            event("join_attempt_end", 5, 13.02, caller, destination,
                  join_id=join_id, step_id=step_id),
            event("control_rpc_end", 6, 13.021, destination, caller,
                  rpc_id="300:rpc1", cancelled=False, error=None),
        ]
        return rows

    def test_exact_client_timeout_and_server_end_are_correlated(self):
        rows = self._lifecycle()
        witness = analyzer._native_client_no_response_witness(
            rows, rows[2], rows[5], [])
        self.assertIsNotNone(witness)
        self.assertEqual(witness["join_id"], "200:join1")
        self.assertEqual(witness["closure_kind"], "native_client_timeout")

    def test_exact_native_cancellation_is_correlated_without_timeout_duration(self):
        rows = self._lifecycle("CancelledError")
        rows[3]["monotonic"] = 11.0
        rows[4]["monotonic"] = 11.01
        rows[5]["monotonic"] = 11.02
        witness = analyzer._native_client_no_response_witness(
            rows, rows[2], rows[5], [])
        self.assertIsNotNone(witness)
        self.assertEqual(witness["closure_kind"], "native_client_cancelled")

    def test_no_response_without_exact_caller_closure_is_rejected(self):
        rows = self._lifecycle()
        rows[:] = [row for row in rows if row["kind"] != "native_client_exception"]
        self.assertIsNone(analyzer._native_client_no_response_witness(
            rows, rows[2], rows[-1], []))

    def test_mismatched_request_expiration_is_rejected(self):
        rows = self._lifecycle()
        rows[3]["detail"]["request_expiration"] += 1.0
        self.assertIsNone(analyzer._native_client_no_response_witness(
            rows, rows[2], rows[5], []))


class PreservedSmokeReplayTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not SUITE.is_dir():
            raise unittest.SkipTest("preserved Step 30 v2 smoke suite is unavailable")
        cls.results = {}
        for trial_name in TRIALS:
            trial_dir = SUITE / trial_name
            if not (trial_dir / "events.jsonl").is_file():
                raise unittest.SkipTest(f"preserved raw trial missing: {trial_name}")
            cls.results[trial_name] = analyzer.analyse_trial(trial_dir)

    def test_both_smoke_traces_pass_revised_integrity(self):
        for name, result in self.results.items():
            with self.subTest(trial=name):
                self.assertTrue(result["integrity_pass"], result["integrity_errors"])
                self.assertEqual(result["integrity_errors"], [])
                self.assertTrue(result["publication_integrity"]["pass_"])
        self.assertEqual(self.results["stock_2s_s3000"]["delay_integrity"]["summary"].get(
            "no_response_native_client_closure_count"), 0)
        self.assertEqual(self.results["renew_1_2_s3000"]["delay_integrity"]["summary"].get(
            "no_response_native_client_closure_count"), 2)

    def test_repeated_expiration_is_allowed_only_at_actual_search_deadline(self):
        renew = self.results["renew_1_2_s3000"]
        by_ref = {row["publication_event_ref"]: row for row in renew["publications"]}
        for ref in ("2782832:441", "2782832:668"):
            row = by_ref[ref]
            self.assertTrue(row["terminal_deadline_clamp"])
            self.assertEqual(row["expiration"], row["expected_expiration"])
            self.assertLess(row["publication_wall"], row["expiration"])
        self.assertTrue(all(error == "" for error in renew["integrity_errors"]))

    def test_replayed_smoke_analyses_export_as_a_complete_suite(self):
        with tempfile.TemporaryDirectory(prefix="step30_analyzer_v2_export_") as tmp:
            suite = Path(tmp)
            (suite / "suite.json").write_bytes((SUITE / "suite.json").read_bytes())
            for trial_name, result in self.results.items():
                trial_dir = suite / trial_name
                trial_dir.mkdir()
                (trial_dir / "control_analysis.json").write_text(
                    json.dumps(result, sort_keys=True, allow_nan=False) + "\n",
                    encoding="utf-8")
            exported = analyzer.export_suite(suite)
            self.assertTrue(exported["complete"])
            self.assertTrue(exported["integrity_pass"], exported["integrity_errors"])
            self.assertTrue(exported["smoke_gate_pass"], exported["smoke_gate_errors"])
            self.assertEqual(exported["analyzed_trial_count"], 2)


if __name__ == "__main__":
    unittest.main()
