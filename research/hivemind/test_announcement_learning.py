"""Determinism and numerical contracts for announcement_learning."""
from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

import announcement_learning as workload
import numpy as np


class LearningProblemTests(unittest.TestCase):
    def test_problem_is_deterministic_and_distribution_pairing_is_preserved(self):
        iid = workload.make_problem(201, "iid")
        iid_again = workload.make_problem(201, "iid")
        distinctive = workload.make_problem(201, "distinctive")

        np.testing.assert_array_equal(iid.target, iid_again.target)
        np.testing.assert_array_equal(iid.eval_x, iid_again.eval_x)
        for node in workload.NODES:
            np.testing.assert_array_equal(iid.train_x[node], iid_again.train_x[node])
            np.testing.assert_array_equal(iid.train_y[node], iid_again.train_y[node])
            np.testing.assert_array_equal(iid.train_x[node][:, :36], distinctive.train_x[node][:, :36])
        np.testing.assert_array_equal(iid.target, distinctive.target)
        np.testing.assert_array_equal(iid.eval_x, distinctive.eval_x)
        np.testing.assert_array_equal(iid.eval_y, distinctive.eval_y)
        for node in ("B", "D", "C"):
            self.assertTrue(np.all(distinctive.train_x[node][:, 36:] == 0.0))
        self.assertTrue(np.any(distinctive.train_x["A"][:, 36:] != 0.0))
        self.assertEqual(iid.target.dtype, np.float32)
        self.assertAlmostEqual(float(np.linalg.norm(iid.target.astype(np.float64))), 1.0, places=6)
        self.assertEqual(iid.train_x["B"].shape, (512, 48))
        self.assertEqual(iid.train_y["B"].shape, (512,))
        self.assertEqual(iid.eval_x.shape, (4096, 48))

    def test_full_batch_gradient_and_update_match_the_frozen_contract(self):
        problem = workload.make_problem(201, "iid")
        weights = np.linspace(-0.15, 0.2, workload.DIMENSIONS, dtype=np.float32)
        gradient = problem.gradient("A", weights)
        residual = problem.train_x["A"] @ weights - problem.train_y["A"]
        expected = problem.train_x["A"].T @ residual / np.float32(512)
        np.testing.assert_allclose(gradient, expected, rtol=1e-6, atol=1e-6)
        self.assertEqual(gradient.dtype, np.float32)

        updated = problem.update(weights, gradient)
        np.testing.assert_array_equal(
            updated, weights - np.float32(0.1) * gradient
        )
        self.assertEqual(updated.dtype, np.float32)
        self.assertEqual(problem.initial_weights.shape, (48,))
        self.assertEqual(problem.initial_weights.dtype, np.float32)
        self.assertTrue(np.all(problem.initial_weights == 0.0))

    def test_gradient_is_the_derivative_of_half_mean_squared_error(self):
        problem = workload.make_problem(99, "iid")
        weights = np.zeros(48, dtype=np.float32)
        gradient = problem.gradient("D", weights)
        h = np.float32(0.01)

        def train_loss(vector):
            residual = problem.train_x["D"] @ vector - problem.train_y["D"]
            return float(np.mean(np.square(residual, dtype=np.float32)) / 2.0)

        for coordinate in (0, 17, 47):
            plus = weights.copy()
            minus = weights.copy()
            plus[coordinate] += h
            minus[coordinate] -= h
            numerical = (train_loss(plus) - train_loss(minus)) / (2.0 * float(h))
            self.assertAlmostEqual(float(gradient[coordinate]), numerical, delta=2e-4)

    def test_input_validation_and_unknown_participant_errors_are_clear(self):
        with self.assertRaisesRegex(ValueError, "distribution"):
            workload.make_problem(1, "non_iid")
        with self.assertRaisesRegex(ValueError, "seed"):
            workload.make_problem(-1)
        problem = workload.make_problem(1)
        with self.assertRaisesRegex(KeyError, "unknown participant"):
            problem.gradient("X", np.zeros(48, dtype=np.float32))
        with self.assertRaisesRegex(ValueError, "weights must have shape"):
            problem.gradient("A", np.zeros(47, dtype=np.float32))
        with self.assertRaisesRegex(ValueError, "gradient must have shape"):
            problem.update(np.zeros(48), np.zeros(47))

    def test_saved_artifacts_include_config_and_array_hashes(self):
        problem = workload.make_problem(201, "distinctive")
        with tempfile.TemporaryDirectory() as tmp:
            manifest = problem.save(tmp)
            path = Path(tmp)
            self.assertEqual(json.loads((path / "learning_config.json").read_text()), problem.config())
            disk_manifest = json.loads((path / "learning_manifest.json").read_text())
            self.assertEqual(manifest, disk_manifest)
            self.assertEqual(manifest["config_sha256"], problem.hashes()["config_sha256"])
            self.assertEqual(len(manifest["data_file_sha256"]), 64)
            with np.load(path / "learning_data.npz") as saved:
                np.testing.assert_array_equal(saved["train_x_A"], problem.train_x["A"])
                np.testing.assert_array_equal(saved["eval_y"], problem.eval_y)
            restored = workload.load_problem(path)
            self.assertEqual(restored.config(), problem.config())
            self.assertEqual(restored.hashes(), problem.hashes())
            np.testing.assert_array_equal(restored.train_x["C"], problem.train_x["C"])

    def test_saved_artifact_loader_rejects_corrupted_config_or_data(self):
        problem = workload.make_problem(202, "iid")
        with tempfile.TemporaryDirectory() as tmp:
            problem.save(tmp)
            data_path = Path(tmp) / "learning_data.npz"
            original = data_path.read_bytes()
            data_path.write_bytes(original[:-8] + bytes([original[-8] ^ 1]) + original[-7:])
            with self.assertRaisesRegex(ValueError, "data file hash"):
                workload.load_problem(tmp)


class ScheduleTests(unittest.TestCase):
    def test_schedules_are_deterministic_paired_and_have_expected_boundaries(self):
        schedules = workload.make_schedules(201)
        schedules_again = workload.make_schedules(201)
        self.assertEqual(schedules, schedules_again)
        self.assertEqual(tuple(schedules), workload.SCHEDULE_ARMS)
        self.assertTrue(all(len(rows) == 200 for rows in schedules.values()))
        self.assertTrue(all(row == {"round": index, "condition": "honest", "target": None}
                            for index, row in enumerate(schedules["baseline"], start=1)))

        affected = {
            arm: {row["round"] for row in schedules[arm] if row["condition"] != "honest"}
            for arm in ("random_omission_25", "membership_25", "ordering_25")
        }
        self.assertEqual(affected["random_omission_25"], affected["membership_25"])
        self.assertEqual(affected["membership_25"], affected["ordering_25"])
        self.assertEqual(len(affected["membership_25"]), 50)
        self.assertTrue(all(row["target"] in ("A", "D", "C")
                            for row in schedules["random_omission_25"]
                            if row["condition"] == "benign_omission"))
        for arm, condition in (("membership_25", "membership_omission"),
                               ("ordering_25", "ordering_corruption")):
            self.assertTrue(all(row["target"] == "A" for row in schedules[arm]
                                if row["condition"] == condition))

        for arm, condition in (("random_omission_100", "benign_omission"),
                               ("membership_100", "membership_omission"),
                               ("ordering_100", "ordering_corruption")):
            self.assertTrue(all(row["condition"] == condition for row in schedules[arm]))
        self.assertEqual(schedules["membership_recovery"][99]["condition"], "membership_omission")
        self.assertEqual(schedules["membership_recovery"][99]["target"], "A")
        self.assertEqual(schedules["membership_recovery"][100], {
            "round": 101, "condition": "honest", "target": None})
        self.assertEqual(schedules["ordering_recovery"][99]["condition"], "ordering_corruption")
        self.assertEqual(schedules["ordering_recovery"][100]["condition"], "honest")

    def test_schedule_validation_and_small_horizons(self):
        with self.assertRaisesRegex(ValueError, "rounds"):
            workload.make_schedules(1, 0)
        with self.assertRaisesRegex(ValueError, "seed"):
            workload.make_schedules(-1)
        short = workload.make_schedules(2, rounds=8)
        self.assertEqual(len(short["membership_25"]), 8)
        self.assertEqual(sum(row["condition"] != "honest" for row in short["membership_25"]), 2)
        self.assertTrue(all(row["condition"] == "membership_omission"
                            for row in short["membership_recovery"]))


if __name__ == "__main__":
    unittest.main()
