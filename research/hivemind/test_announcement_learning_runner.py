"""Focused contracts for the E4/E5 announcement-learning suite controller."""
from __future__ import annotations

import copy
import csv
import dataclasses
import gzip
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

import announcement_learning as workload
import run_announcement_learning as runner

import numpy as np


def read_trace(path: Path):
    with gzip.open(path, "rt", encoding="utf-8") as stream:
        return [json.loads(line) for line in stream if line.strip()]


def write_trace(path: Path, rows):
    with gzip.open(path, "wt", encoding="utf-8", newline="\n") as stream:
        for row in rows:
            stream.write(json.dumps(row, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n")


def make_reference(root: Path, name: str):
    directory = root / name
    directory.mkdir(parents=True)
    (directory / "raw.json").write_text('{"checked":true}\n', encoding="utf-8")
    import announcement_artifacts

    inventory = announcement_artifacts.inventory(directory)
    inventory_path = directory / "artifact_inventory.json"
    inventory_path.write_text(json.dumps(inventory, indent=2) + "\n", encoding="utf-8")
    return {
        "path": directory.relative_to(ROOT).as_posix(),
        "artifact_inventory_sha256": hashlib.sha256(inventory_path.read_bytes()).hexdigest(),
    }


class RunnerTrajectoryTests(unittest.TestCase):
    def test_run_trace_is_deterministic_replayable_and_has_per_node_metrics(self):
        problem = workload.make_problem(201, "distinctive")
        schedule = [
            {"round": index, "condition": "honest", "target": None}
            for index in range(1, 4)
        ]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            summary_a = runner.run_case(
                problem, "baseline", schedule,
                trace_path=root / "a.jsonl.gz", metrics_path=root / "a.csv",
            )
            summary_b = runner.run_case(
                problem, "baseline", schedule,
                trace_path=root / "b.jsonl.gz", metrics_path=root / "b.csv",
            )
            self.assertEqual(summary_a, summary_b)
            rows_a, rows_b = read_trace(root / "a.jsonl.gz"), read_trace(root / "b.jsonl.gz")
            self.assertEqual(rows_a, rows_b)
            replay = runner.verify_trace(problem, root / "a.jsonl.gz", expected_rounds=3)
            self.assertTrue(replay["valid"])
            self.assertEqual(replay["gradient_evaluations_reconstructed"], 12)
            self.assertEqual(replay["summary"], summary_a)
            with (root / "a.csv").open(newline="", encoding="utf-8") as stream:
                metrics = list(csv.DictReader(stream))
            self.assertEqual(len(metrics), 12)
            self.assertEqual({row["node"] for row in metrics}, set(workload.NODES))
            self.assertIn("exclusive_block_loss", metrics[0])
            self.assertEqual(summary_a["gradient_evaluations"], 12)

    def test_benign_and_membership_omission_leave_target_on_its_own_gradient(self):
        problem = workload.make_problem(201, "iid")
        benign = [{"round": 1, "condition": "benign_omission", "target": "D"}]
        membership = [{"round": 1, "condition": "membership_omission", "target": "A"}]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            runner.run_case(problem, "random_omission_25", benign, trace_path=root / "benign.jsonl.gz")
            runner.run_case(problem, "membership_25", membership, trace_path=root / "membership.jsonl.gz")
            benign_row = read_trace(root / "benign.jsonl.gz")[0]
            membership_row = read_trace(root / "membership.jsonl.gz")[0]
        self.assertEqual(benign_row["active_members"], ["B", "A", "C"])
        self.assertEqual(set(benign_row["numerical_result"]["views"]), {"B", "A", "C"})
        np.testing.assert_array_equal(
            benign_row["numerical_result"]["values"]["D"],
            benign_row["input_gradients"]["D"],
        )
        self.assertEqual(membership_row["numerical_result"]["views"]["B"], ["B", "D", "C"])
        self.assertEqual(membership_row["numerical_result"]["views"]["A"], ["B", "D", "A", "C"])
        np.testing.assert_array_equal(
            membership_row["numerical_result"]["values"]["A"],
            membership_row["input_gradients"]["A"],
        )

    def test_ordering_policy_changes_victim_result_and_trace_replay_checks_it(self):
        problem = workload.make_problem(201, "iid")
        honest = [{"round": 1, "condition": "honest", "target": None}]
        ordering = [{"round": 1, "condition": "ordering_corruption", "target": "A"}]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            runner.run_case(problem, "baseline", honest, trace_path=root / "honest.jsonl.gz")
            runner.run_case(problem, "ordering_25", ordering, trace_path=root / "ordering.jsonl.gz")
            honest_row = read_trace(root / "honest.jsonl.gz")[0]
            order_rows = read_trace(root / "ordering.jsonl.gz")
            self.assertTrue(runner.verify_trace(problem, root / "ordering.jsonl.gz")["valid"])
            order_rows[0]["weights_after"]["A"][0] += 1.0
            write_trace(root / "corrupted.jsonl.gz", order_rows)
            with self.assertRaisesRegex(ValueError, "post-state mismatch|update mismatch"):
                runner.verify_trace(problem, root / "corrupted.jsonl.gz")
        self.assertEqual(order_rows[0]["numerical_result"]["views"]["A"], ["A", "D", "B", "C"])
        self.assertNotEqual(
            honest_row["numerical_result"]["values"]["A"],
            order_rows[0]["numerical_result"]["values"]["A"],
        )

    def test_nonfinite_gradient_is_retained_as_json_strings_and_censors_only_that_arm(self):
        problem = workload.make_problem(202, "iid")
        huge = np.full((workload.TRAINING_EXAMPLES_PER_NODE, workload.DIMENSIONS), 1.0e20, dtype=np.float32)
        train_x = {node: huge.copy() for node in workload.NODES}
        train_y = {node: np.asarray(train_x[node] @ problem.target, dtype=np.float32)
                   for node in workload.NODES}
        divergent_problem = dataclasses.replace(problem, train_x=train_x, train_y=train_y)
        schedule = [
            {"round": index, "condition": "honest", "target": None}
            for index in range(1, 4)
        ]
        with tempfile.TemporaryDirectory() as temporary:
            trace = Path(temporary) / "diverged.jsonl.gz"
            metrics_path = Path(temporary) / "diverged.csv"
            summary = runner.run_case(
                divergent_problem, "baseline", schedule, trace_path=trace, metrics_path=metrics_path,
            )
            rows = read_trace(trace)
            replay = runner.verify_trace(divergent_problem, trace, expected_rounds=3)
            with metrics_path.open(newline="", encoding="utf-8") as stream:
                metric_rows = list(csv.DictReader(stream))
        self.assertEqual(summary["trajectory_status"], "diverged_nonfinite")
        self.assertTrue(summary["stopped_early"])
        self.assertEqual(summary["rounds"], 1)
        self.assertEqual(summary["divergence"]["stage"], "gradient")
        self.assertEqual(summary["divergence"]["round"], 1)
        self.assertEqual(len(rows), 1)
        self.assertTrue(any(isinstance(value, str) and value in ("Infinity", "-Infinity", "NaN")
                            for value in rows[0]["input_gradients"]["B"]))
        self.assertTrue(replay["valid"])
        self.assertTrue(replay["diverged_nonfinite"])
        self.assertEqual(len(metric_rows), 4)
        self.assertTrue(all(row["updated"] == "False" for row in metric_rows))

    def test_nonfinite_metrics_are_null_with_field_reasons(self):
        problem = workload.make_problem(202, "iid")
        weights = {node: problem.initial_weights for node in workload.NODES}
        weights["A"] = np.full(workload.DIMENSIONS, np.inf, dtype=np.float32)
        metrics = runner._round_metrics(problem, weights)
        self.assertIsNone(metrics["A"]["test_loss"])
        self.assertIsNone(metrics["A"]["exclusive_block_loss"])
        self.assertIn("A.test_loss", metrics["nonfinite_metric_fields"])
        self.assertEqual(runner._json_safe(metrics)["A"]["test_loss"], None)

    def test_large_but_finite_loss_runs_the_full_supplied_schedule(self):
        problem = workload.make_problem(203, "iid")
        scale = np.float32(1.0e15)
        large_problem = dataclasses.replace(
            problem,
            train_y={node: np.asarray(problem.train_y[node] * scale, dtype=np.float32)
                     for node in workload.NODES},
        )
        schedule = [
            {"round": index, "condition": "honest", "target": None}
            for index in range(1, 4)
        ]
        with tempfile.TemporaryDirectory() as temporary:
            trace = Path(temporary) / "large-finite.jsonl.gz"
            summary = runner.run_case(large_problem, "baseline", schedule, trace_path=trace)
            replay = runner.verify_trace(large_problem, trace, expected_rounds=3)
        self.assertEqual(summary["rounds"], 3)
        self.assertEqual(summary["trajectory_status"], "completed")
        self.assertTrue(np.isfinite(summary["final_honest_mean_test_loss"]))
        self.assertGreater(summary["final_honest_mean_test_loss"], 1.0e20)
        self.assertTrue(replay["valid"])


class ScheduleAndSummaryTests(unittest.TestCase):
    def test_200_round_matrix_has_paired_25_percent_and_recovery_windows(self):
        schedules = workload.make_schedules(201, 200)
        self.assertEqual(tuple(schedules), workload.SCHEDULE_ARMS)
        self.assertTrue(all(len(rows) == 200 for rows in schedules.values()))
        affected = {
            arm: {row["round"] for row in schedules[arm] if row["condition"] != "honest"}
            for arm in ("random_omission_25", "membership_25", "ordering_25")
        }
        self.assertEqual(affected["random_omission_25"], affected["membership_25"])
        self.assertEqual(affected["membership_25"], affected["ordering_25"])
        self.assertEqual(len(affected["membership_25"]), 50)
        self.assertTrue(all(row["condition"] != "honest" for row in schedules["membership_100"]))
        self.assertTrue(all(row["condition"] != "honest" for row in schedules["ordering_100"]))
        self.assertEqual(schedules["membership_recovery"][99]["condition"], "membership_omission")
        self.assertEqual(schedules["membership_recovery"][100]["condition"], "honest")
        self.assertEqual(schedules["ordering_recovery"][99]["condition"], "ordering_corruption")
        self.assertEqual(schedules["ordering_recovery"][100]["condition"], "honest")

    def test_convergence_requires_ten_consecutive_rounds_and_reports_censoring(self):
        problem = workload.make_problem(201, "iid")
        base = runner._round_metrics(problem, {node: problem.initial_weights for node in workload.NODES})
        threshold = base["honest_mean_test_loss"] * runner.CONVERGENCE_FRACTION
        losses = [threshold / 2] * 5 + [threshold * 2] + [threshold / 2] * 10
        records = []
        for index, loss in enumerate(losses, start=1):
            metrics = copy.deepcopy(base)
            metrics["honest_mean_test_loss"] = loss
            records.append({"round": index, "metrics": metrics, "divergence": None})
        summary = runner._summary(problem, records, scheduled_rounds=16)
        self.assertEqual(summary["first_round_sustained_below_threshold"], 7)
        self.assertEqual(summary["round_when_sustained_threshold_confirmed"], 16)
        self.assertFalse(summary["convergence_censored"])
        self.assertEqual(summary["scheduled_rounds"], 16)

        after_convergence = copy.deepcopy(records)
        after_convergence.append({
            "round": 17,
            "metrics": {**copy.deepcopy(base), "honest_mean_test_loss": None},
            "divergence": {"round": 17, "stage": "metric", "field": "A.test_loss"},
        })
        diverged = runner._summary(problem, after_convergence, scheduled_rounds=17)
        self.assertEqual(diverged["round_when_sustained_threshold_confirmed"], 16)
        self.assertFalse(diverged["convergence_censored"])
        self.assertIsNone(diverged["censor_reason"])
        self.assertEqual(diverged["trajectory_status"], "diverged_nonfinite")
        self.assertEqual(diverged["divergence"]["round"], 17)

        no_hit = []
        for index in range(1, 201):
            metrics = copy.deepcopy(base)
            metrics["honest_mean_test_loss"] = threshold * 2
            no_hit.append({"round": index, "metrics": metrics, "divergence": None})
        censored = runner._summary(problem, no_hit, scheduled_rounds=200)
        self.assertEqual(censored["rounds"], 200)
        self.assertTrue(censored["convergence_censored"])
        self.assertEqual(censored["censor_reason"], "horizon_reached")


class GateTests(unittest.TestCase):
    def _validation(self, root: Path):
        references = [make_reference(root, f"native_ref_{index}") for index in range(9)]
        path = root / "validation.json"
        path.write_text(json.dumps({
            "schema": runner.VALIDATION_SCHEMA,
            "valid": True,
            "source_hashes": runner.source_hashes(),
            "native_references": references,
            "checks": ["marker cases", "training cases"],
            "errors": [],
        }, sort_keys=True) + "\n", encoding="utf-8")
        return path, references

    def test_validation_requires_matching_sources_and_all_intact_native_inventories(self):
        with tempfile.TemporaryDirectory(dir=ROOT, prefix=".announcement-gate-test-") as temporary:
            root = Path(temporary)
            path, refs = self._validation(root)
            accepted = runner._verify_validation(path)
            self.assertEqual(accepted["source_hashes"], runner.source_hashes())
            self.assertEqual(len(accepted["native_references"]), 9)

            validation = json.loads(path.read_text())
            validation["native_references"] = []
            path.write_text(json.dumps(validation), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "9 native"):
                runner._verify_validation(path)

            validation["native_references"] = refs
            validation["source_hashes"][runner.CORE_SOURCE_PATHS[0]] = "0" * 64
            path.write_text(json.dumps(validation), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "source hashes"):
                runner._verify_validation(path)

            validation["source_hashes"] = runner.source_hashes()
            validation["native_references"][0]["artifact_inventory_sha256"] = "0" * 64
            path.write_text(json.dumps(validation), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "inventory hash mismatch"):
                runner._verify_validation(path)

            validation["native_references"] = refs
            validation["native_references"][0]["path"] = "research/does-not-exist"
            path.write_text(json.dumps(validation), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "missing or outside"):
                runner._verify_validation(path)

    def test_confirmation_requires_complete_reviewed_pilot_and_matching_metadata(self):
        with tempfile.TemporaryDirectory(dir=ROOT, prefix=".announcement-pilot-gate-test-") as temporary:
            root = Path(temporary)
            validation_path, _ = self._validation(root)
            validation = runner._verify_validation(validation_path)
            pilot_root = root / "pilot"
            pilot_root.mkdir()
            data_artifacts = {}
            for distribution in ("iid", "distinctive"):
                data_path = pilot_root / "problems" / f"seed201_{distribution}"
                problem = workload.make_problem(201, distribution)
                problem.save(data_path)
                data_artifacts[f"seed201_{distribution}"] = {
                    "directory": data_path.relative_to(pilot_root).as_posix(),
                    "manifest_sha256": runner._sha256(data_path / "learning_manifest.json"),
                    "hashes": problem.hashes(),
                }
            runs = []
            for distribution in ("iid", "distinctive"):
                for arm in workload.SCHEDULE_ARMS:
                    run_path = pilot_root / "runs" / "seed201" / distribution / arm
                    relative_path = run_path.relative_to(pilot_root).as_posix()
                    run_path.mkdir(parents=True)
                    (run_path / "trace.jsonl.gz").write_bytes(b"synthetic trace fixture\n")
                    (run_path / "metrics.csv").write_text("synthetic metrics fixture\n", encoding="utf-8")
                    summary = {
                        "fixture": relative_path,
                        "rounds": 200,
                        "scheduled_rounds": 200,
                        "gradient_evaluations": 800,
                        "trajectory_status": "completed",
                        "divergence": None,
                        "initial_honest_mean_test_loss": 1.0,
                        "final_honest_mean_test_loss": 0.5,
                    }
                    runner._write_json(run_path / "summary.json", summary)
                    runner._write_json(run_path / "verification.json", {
                        "valid": True,
                        "rounds": 200,
                        "scheduled_rounds": 200,
                        "diverged_nonfinite": False,
                        "summary": summary,
                    })
                    runs.append({
                        "seed": 201,
                        "distribution": distribution,
                        "arm": arm,
                        "path": relative_path,
                        "status": "complete",
                        "rounds": 200,
                        "gradient_evaluations": 800,
                        "divergence": None,
                    })
            frozen_hashes = {
                **runner.source_hashes(),
                runner.RUNNER_SOURCE_PATH: runner._sha256(ROOT / runner.RUNNER_SOURCE_PATH),
            }
            runner._freeze_sources(pilot_root, frozen_hashes)
            inventory = runner._artifact_inventory(pilot_root)
            manifest = {
                "schema": runner.SUITE_SCHEMA,
                "mode": "pilot",
                "status": "complete",
                "run_count": 18,
                "runs": runs,
                "source_hashes": runner.source_hashes(),
                "runner_sha256": frozen_hashes[runner.RUNNER_SOURCE_PATH],
                "validation_sha256": validation["sha256"],
                "workload_contract_sha256": runner._contract_sha256(workload.make_problem(201, "iid")),
                "arms": list(workload.SCHEDULE_ARMS),
                "distributions": ["iid", "distinctive"],
                "seeds": [201],
                "rounds": 200,
                "data_artifacts": data_artifacts,
                "artifact_inventory_sha256": hashlib.sha256(runner._json_bytes(inventory)).hexdigest(),
            }
            runner._write_json(pilot_root / "manifest.json", manifest)
            accepted = runner._verify_reviewed_pilot(
                pilot_root, validation=validation, problem=workload.make_problem(202, "iid"))
            self.assertEqual(accepted["path"], str(pilot_root.resolve()))

            manifest["runner_sha256"] = "0" * 64
            runner._write_json(pilot_root / "manifest.json", manifest)
            with self.assertRaisesRegex(ValueError, "runner source"):
                runner._verify_reviewed_pilot(
                    pilot_root, validation=validation, problem=workload.make_problem(202, "iid"))
            manifest["runner_sha256"] = frozen_hashes[runner.RUNNER_SOURCE_PATH]
            runner._write_json(pilot_root / "manifest.json", manifest)

            # The outer inventory and all nested artifacts are independently frozen.
            (pilot_root / "runs" / "seed201" / "iid" / "baseline" / "metrics.csv").write_text(
                "tampered\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "artifact changed"):
                runner._verify_reviewed_pilot(
                    pilot_root, validation=validation, problem=workload.make_problem(202, "iid"))

            manifest["run_count"] = 17
            runner._write_json(pilot_root / "manifest.json", manifest)
            with self.assertRaisesRegex(ValueError, "18 completed"):
                runner._verify_reviewed_pilot(
                    pilot_root, validation=validation, problem=workload.make_problem(202, "iid"))

    def test_artifact_freeze_and_inventory_detect_runner_or_data_changes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            frozen_sources = root / "suite"
            frozen_sources.mkdir()
            hashes = {**runner.source_hashes(), runner.RUNNER_SOURCE_PATH: runner._sha256(ROOT / runner.RUNNER_SOURCE_PATH)}
            runner._freeze_sources(frozen_sources, hashes)
            copied = frozen_sources / "harness" / runner.RUNNER_SOURCE_PATH
            self.assertEqual(runner._sha256(copied), hashes[runner.RUNNER_SOURCE_PATH])
            (frozen_sources / "raw.txt").write_text("first\n", encoding="utf-8")
            inventory = runner._artifact_inventory(frozen_sources)
            original = inventory["raw.txt"]
            (frozen_sources / "raw.txt").write_text("changed\n", encoding="utf-8")
            changed = runner._artifact_inventory(frozen_sources)
            self.assertNotEqual(original, changed["raw.txt"])


if __name__ == "__main__":
    unittest.main()
