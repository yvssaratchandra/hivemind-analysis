"""Focused checks for the deterministic compute workload helpers."""
from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import unittest

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

import compute_workload as workload

if importlib.util.find_spec("numpy") is not None:
    import numpy as np
else:  # Keep pure summary and module-import checks usable without the optional dependency.
    np = None


@unittest.skipUnless(np is not None, "NumPy is absent from this interpreter")
class WorkloadTests(unittest.TestCase):
    def test_problem_constants_and_target_are_stable_unit_float32(self):
        self.assertEqual(workload.NODES, ("B", "D", "A", "C"))
        self.assertEqual(workload.CONDITIONS, ("baseline", "exclusion", "absent"))
        self.assertEqual(workload.SHARE70_SAMPLES_PER_SECOND,
                         {"B": 128, "D": 128, "A": 896, "C": 128})
        target = workload.make_target(41)
        np.testing.assert_array_equal(target, workload.make_target(41))
        self.assertEqual(target.shape, (48,))
        self.assertEqual(target.dtype, np.float32)
        self.assertAlmostEqual(float(np.linalg.norm(target)), 1.0, places=6)
        self.assertFalse(np.array_equal(target, workload.make_target(42)))
        np.testing.assert_array_equal(workload.initial_weights(), np.zeros(48, np.float32))
        self.assertEqual(workload.LEARNING_RATE, 0.1)

    def test_batch_pairing_stream_keys_sample_ids_and_hashes(self):
        x, y, metadata = workload.batch(41, "D", 3)
        x_again, y_again, metadata_again = workload.batch(41, "D", 3)
        np.testing.assert_array_equal(x, x_again)
        np.testing.assert_array_equal(y, y_again)
        self.assertEqual(metadata, metadata_again)
        self.assertEqual(x.shape, (32, 48))
        self.assertEqual(y.shape, (32,))
        self.assertEqual(x.dtype, np.float32)
        self.assertEqual(y.dtype, np.float32)
        np.testing.assert_array_equal(y, x @ workload.make_target(41))
        self.assertEqual(len(metadata["sample_ids"]), 32)
        self.assertEqual(len(set(metadata["sample_ids"])), 32)
        for key in ("x_sha256", "y_sha256", "batch_sha256"):
            self.assertEqual(len(metadata[key]), 64)
        for other in (workload.batch(41, "C", 3), workload.batch(41, "D", 4),
                      workload.batch(42, "D", 3)):
            self.assertNotEqual(metadata["batch_sha256"], other[2]["batch_sha256"])
        eval_x, eval_y = workload.evaluation(41)
        self.assertEqual(eval_x.shape, (4096, 48))
        self.assertEqual(eval_y.shape, (4096,))
        np.testing.assert_array_equal(eval_y, eval_x @ workload.make_target(41))
        self.assertFalse(np.array_equal(x, eval_x[:32]))

    def test_invalid_batch_keys_are_rejected(self):
        for args in ((-1, "B", 0), (1, "X", 0), (1, "B", -1), (1, "B", 0, 0)):
            with self.subTest(args=args), self.assertRaises(ValueError):
                workload.batch(*args)

    def test_pacing_sleeps_only_the_current_batches_remaining_interval(self):
        self.assertAlmostEqual(workload.next_batch_delay(10.0, 10.4, 1.0), 0.6)
        self.assertEqual(workload.next_batch_delay(10.0, 11.1, 1.0), 0.0)
        # Starting from a fresh post-optimizer timestamp yields a fresh interval;
        # there is no prior deadline from which to accumulate catch-up time.
        self.assertAlmostEqual(workload.next_batch_delay(20.0, 20.1, 1.0), 0.9)
        with self.assertRaises(ValueError):
            workload.next_batch_delay(2.0, 1.0, 1.0)


@unittest.skipUnless(np is not None, "NumPy is absent from this interpreter")
class MetricTests(unittest.TestCase):
    def test_half_mse_dc_aggregates_and_a_are_computed_separately(self):
        x = np.zeros((4, 48), dtype=np.float32)
        x[:, 0] = 1.0
        y = np.ones(4, dtype=np.float32)
        models = {node: np.zeros(48, np.float32) for node in workload.NODES}
        models["D"][0] = 0.0  # half-MSE 0.5
        models["C"][0] = 0.5  # half-MSE 0.125
        models["A"][0] = 1.0  # half-MSE 0
        result = workload.evaluate_models(models, x, y)
        self.assertAlmostEqual(result["per_node"]["D"], 0.5)
        self.assertAlmostEqual(result["per_node"]["C"], 0.125)
        self.assertAlmostEqual(result["DC_mean"], 0.3125)
        self.assertAlmostEqual(result["DC_worst"], 0.5)
        self.assertAlmostEqual(result["A"], 0.0)
        self.assertEqual(result["node_status"], {node: "ok" for node in workload.NODES})

    def test_missing_and_nonfinite_models_are_explicit_in_metrics(self):
        x = np.ones((2, 48), dtype=np.float32)
        y = np.zeros(2, dtype=np.float32)
        models = {node: np.zeros(48, np.float32) for node in workload.NODES}
        del models["C"]
        models["D"][0] = np.nan
        result = workload.evaluate_models(models, x, y)
        self.assertIsNone(result["per_node"]["C"])
        self.assertEqual(result["node_status"]["C"], "missing")
        self.assertIsNone(result["per_node"]["D"])
        self.assertEqual(result["node_status"]["D"], "nonfinite_weights")
        self.assertIsNone(result["DC_mean"])
        self.assertIsNone(result["DC_worst"])


class ThresholdTests(unittest.TestCase):
    def test_thresholds_use_each_participant_initial_loss(self):
        initial = {"per_node": {"D": 0.4, "C": 0.2}}
        thresholds = workload.thresholds_from_initial_metrics(initial)
        self.assertAlmostEqual(thresholds["D"], 0.04)
        self.assertAlmostEqual(thresholds["C"], 0.02)


def snapshot(when, d=0.05, c=0.05, a=0.0, **overrides):
    values = {"B": 0.2, "D": d, "A": a, "C": c}
    values.update(overrides)
    return {
        "time": float(when),
        "thresholds": {"D": 0.1, "C": 0.1},
        "metrics": {
            "per_node": values,
            "node_status": {node: ("missing" if value is None else "ok")
                            for node, value in values.items()},
        },
    }


class EvaluationSummaryTests(unittest.TestCase):
    def test_first_sustained_crossing_is_reported_and_full_horizon_is_used(self):
        rows = [snapshot(i) for i in range(10)]
        rows.append(snapshot(300, d=0.01, c=0.02, a=0.03))
        result = workload.summarize_evaluations(rows, horizon=300)
        self.assertEqual(result["crossing_time"], 0.0)
        self.assertEqual(result["confirmed_time"], 9.0)
        self.assertFalse(result["censored"])
        self.assertIsNone(result["censor_reason"])
        self.assertEqual(result["evaluations_used"], 11)
        self.assertAlmostEqual(result["final_metrics"]["A"], 0.03)

    def test_above_threshold_and_large_gap_break_a_streak(self):
        rows = [snapshot(i) for i in range(8)]
        rows.append(snapshot(8, d=0.1))  # equality is not below threshold
        rows.extend(snapshot(9 + i) for i in range(9))
        # The two-second gap resets that nine-evaluation run.
        rows.extend(snapshot(20 + i) for i in range(10))
        result = workload.summarize_evaluations(rows, horizon=40)
        self.assertEqual(result["crossing_time"], 20.0)
        self.assertEqual(result["confirmed_time"], 29.0)
        self.assertEqual(result["gap_resets"], 1)
        self.assertFalse(result["censored"])

    def test_missing_node_and_nonfinite_metric_censor_unconfirmed_event(self):
        missing = snapshot(1, c=None)
        result = workload.summarize_evaluations([snapshot(0), missing], horizon=10)
        self.assertTrue(result["censored"])
        self.assertEqual(result["censor_reason"], "missing_node_C")
        self.assertEqual(result["censor_time"], 1.0)

        invalid = snapshot(2)
        invalid["metrics"]["per_node"]["D"] = float("nan")
        result = workload.summarize_evaluations([invalid], horizon=10)
        self.assertTrue(result["censored"])
        self.assertEqual(result["censor_reason"], "invalid_metric_D_nonfinite")

    def test_unconfirmed_valid_run_is_horizon_censored(self):
        rows = [snapshot(i, d=0.1) for i in range(5)]
        result = workload.summarize_evaluations(rows, horizon=12)
        self.assertTrue(result["censored"])
        self.assertEqual(result["censor_reason"], "horizon")
        self.assertEqual(result["censor_time"], 12.0)
        self.assertIsNone(result["crossing_time"])
        self.assertIsNotNone(result["final_metrics"])

    def test_missing_thresholds_and_invalid_timestamps_are_explicit(self):
        missing_threshold = snapshot(0)
        del missing_threshold["thresholds"]
        result = workload.summarize_evaluations([missing_threshold], horizon=5)
        self.assertTrue(result["censored"])
        self.assertEqual(result["censor_reason"], "missing_thresholds")
        self.assertIsNotNone(result["final_metrics"])

        invalid_time = snapshot(float("nan"))
        result = workload.summarize_evaluations([invalid_time], horizon=5)
        self.assertTrue(result["censored"])
        self.assertEqual(result["censor_reason"], "invalid_timestamp")


if __name__ == "__main__":
    unittest.main()
