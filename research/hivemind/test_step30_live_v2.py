"""Replay regressions for the versioned Step 30 raw verifier."""
from __future__ import annotations

import json
from pathlib import Path
import shutil
import sys
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

import analyze_step30_harm
import verify_step30_harm_v2 as verifier

SUITE = HERE / "results" / "step30_smoke_20261012_a"
TRIAL = SUITE / "stock_2s_s3000"
COMPLETION_FILES = ("config.json", "events.jsonl", "summary.json", "analysis.json",
                    "control_analysis.json", "raw_verification.json")


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def write_json(path, value):
    Path(path).write_text(json.dumps(value, sort_keys=True, allow_nan=False) + "\n",
                          encoding="utf-8")


def upgrade_provenance_v2(suite):
    suite = Path(suite)
    provenance = read_json(suite / "provenance.json")
    for name in verifier.V2_HARNESS:
        shutil.copyfile(HERE / name, suite / "harness" / name)
        provenance["harness_sha256"][name] = verifier._sha256(HERE / name)
    controller = HERE / "run_step30_harm_v2.py"
    provenance.update(verification_policy_version=2,
                      controller_entrypoint=str(controller.resolve()),
                      controller_entrypoint_sha256=verifier._sha256(controller))
    write_json(suite / "provenance.json", provenance)


class LiveSmokeReplayTests(unittest.TestCase):
    def test_preserved_v1_smoke_replays_against_raw_evidence(self):
        self.assertTrue(TRIAL.is_dir(), "preserved Step 30 smoke trace is missing")
        control = read_json(TRIAL / "control_analysis.json")
        raw = verifier.raw_trial(TRIAL)
        self.assertEqual(raw["errors"], [])
        checked = verifier.verify_trial(TRIAL, control)
        self.assertTrue(checked["pass"], checked["errors"])
        self.assertEqual(checked["schema"], "step30-raw-v2")
        self.assertEqual(len(checked["raw_step_starts"]), len(control["attempts"]))
        self.assertEqual(raw["summary_metrics"]["honest_submitted_attempts"], 26)
        self.assertEqual(raw["summary_metrics"]["honest_group_successes"], 25)
        self.assertEqual(raw["summary_metrics"]["x_client_accept_join_count"], 25)

    def test_original_v1_snapshot_remains_provenance_valid(self):
        self.assertEqual(verifier._provenance_errors(SUITE), [])

    def test_v2_provenance_checks_only_its_additional_frozen_sources(self):
        with tempfile.TemporaryDirectory(prefix="step30_v2_provenance_") as tmp:
            suite = Path(tmp) / "suite"
            shutil.copytree(SUITE, suite)
            upgrade_provenance_v2(suite)
            self.assertEqual(verifier._provenance_errors(suite), [])
            with (suite / "harness" / "test_step30_live_v2.py").open("a", encoding="utf-8") as stream:
                stream.write("# post-freeze mutation\n")
            errors = verifier._provenance_errors(suite)
            self.assertTrue(any("version 2 verification source hash mismatch" in error
                                for error in errors), errors)

    def test_export_certificate_checks_all_peer_attempt_rows(self):
        with tempfile.TemporaryDirectory(prefix="step30_v2_exports_") as tmp:
            suite = Path(tmp) / "suite"
            shutil.copytree(SUITE, suite)
            trial = suite / TRIAL.name
            result = verifier.verify_trial(trial, read_json(trial / "control_analysis.json"))
            self.assertTrue(result["pass"], result["errors"])
            write_json(trial / "raw_verification.json", result)
            write_json(trial / "completion.json", {
                "all_peers_shutdown": True,
                "file_sha256": {name: verifier._sha256(trial / name)
                                for name in COMPLETION_FILES},
            })
            upgrade_provenance_v2(suite)

            exported = analyze_step30_harm.export_suite(suite)
            self.assertEqual(exported["analyzed_trial_count"], 1)
            checked = verifier.verify_exports(suite)
            self.assertTrue(checked["pass"], checked["errors"])
            with (suite / "attempts.csv").open(newline="", encoding="utf-8") as stream:
                import csv
                attempts = list(csv.DictReader(stream))
            self.assertEqual(len(attempts), len(result["raw_step_starts"]))
            x_rows = [row for row in attempts if row.get("peer_name") == "X"]
            self.assertEqual(len(x_rows), 2)
            self.assertTrue(all(row["membership_success"].lower() == "false" for row in x_rows))
            self.assertTrue(all(float(row["capped_latency_seconds"]) == 12.0 for row in x_rows))


if __name__ == "__main__":
    unittest.main()
