import tempfile
import unittest
from pathlib import Path

import report_announcement_learning as report


class ReportArithmeticTests(unittest.TestCase):
    def test_pair_reports_absolute_deltas_for_tiny_losses(self):
        baseline = [{"seed": 202, "trajectory_status": "completed", "loss": 1.0e-13}]
        arm = [{"seed": 202, "trajectory_status": "completed", "loss": 2.5e-13}]

        result = report._pair_metric(arm, baseline, lambda case: case["loss"])

        self.assertAlmostEqual(result["pairs"][0]["difference_arm_minus_baseline"], 1.5e-13)
        self.assertAlmostEqual(result["difference_arm_minus_baseline"]["mean"], 1.5e-13)
        self.assertNotIn("difference_percent_of_baseline", result)
        self.assertNotIn("difference_percent_of_baseline", result["pairs"][0])

    def test_finite_stats_reports_nonfinite_values_explicitly(self):
        result = report._finite_stats([0.0, None, 2.0])
        self.assertEqual(result["finite_count"], 2)
        self.assertEqual(result["nonfinite_count"], 1)
        self.assertEqual(result["mean"], 1.0)
        self.assertEqual(result["status"], "mixed_finite_nonfinite")


class RecoveryTests(unittest.TestCase):
    @staticmethod
    def case(losses):
        return {
            "seed": 204,
            "arm": "membership_recovery",
            "scheduled_rounds": report.ROUNDS,
            "rounds": max(losses),
            "final_honest_mean_test_loss": losses.get(report.ROUNDS),
            "convergence": {"streak_rounds": 10, "threshold": 0.1},
            "round_metrics": {
                round_index: {
                    report.NODES[0]: {"honest_mean_test_loss": loss}
                }
                for round_index, loss in losses.items()
            },
        }

    def test_recovery_is_exactly_round_100_to_200_and_strictly_below_threshold(self):
        losses = {100: 0.3, 200: 0.05}
        losses.update({round_index: 0.09 for round_index in range(101, 110)})
        losses[110] = 0.1  # Equality does not count as below the runner threshold.
        losses.update({round_index: 0.09 for round_index in range(111, 120)})

        result = report._recovery_record(self.case(losses))

        self.assertEqual(result["seed"], 204)
        self.assertEqual(result["loss_at_attack_end"], 0.3)
        self.assertEqual(result["final_loss"], 0.05)
        self.assertAlmostEqual(result["loss_reduction_round_100_to_200"], 0.25)
        self.assertIsNone(result["post_attack_convergence_start_round"])
        self.assertIsNone(result["post_attack_convergence_confirmed_round"])

    def test_recovery_start_only_appears_after_streak_is_confirmed(self):
        losses = {100: 0.3, 200: 0.08}
        losses.update({round_index: 0.08 for round_index in range(101, 111)})

        result = report._recovery_record(self.case(losses))

        self.assertEqual(result["post_attack_convergence_start_round"], 101)
        self.assertEqual(result["post_attack_convergence_confirmed_round"], 110)

    def test_missing_round_200_is_explicitly_unavailable(self):
        result = report._recovery_record(self.case({100: 0.3, 101: 0.08}))
        self.assertIsNone(result["final_loss"])
        self.assertIsNone(result["loss_reduction_round_100_to_200"])
        self.assertEqual(result["status"], "nonfinite_or_unavailable")


class InventoryTests(unittest.TestCase):
    def test_inventory_hashes_artifacts_and_rejects_mutation(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            artifact = root / "summary.json"
            artifact.write_text('{"ok": true}\n', encoding="utf-8")
            inventory = {"summary.json": report._sha256(artifact)}
            (root / "artifact_inventory.json").write_bytes(report._json_bytes(inventory))
            manifest = {"artifact_inventory_sha256": report._sha256(root / "artifact_inventory.json")}

            self.assertEqual(report._verify_inventory(root, manifest), inventory)

            artifact.write_text('{"ok": false}\n', encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "artifact hash mismatch"):
                report._verify_inventory(root, manifest)


if __name__ == "__main__":
    unittest.main()
