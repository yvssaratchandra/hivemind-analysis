import json
import tempfile
import unittest
from pathlib import Path

from compute_calibration import calibrate_case


def event(kind, node, monotonic=None, **fields):
    row = {"kind": kind, "node": node}
    if monotonic is not None:
        row["monotonic"] = monotonic
    row.update(fields)
    return row


class ComputeCalibrationTests(unittest.TestCase):
    def make_case(self, root, condition="baseline"):
        suite = root / "pilot"
        case = suite / "baseline_s301"
        case.mkdir(parents=True)
        (suite / "status.json").write_text(json.dumps({"mode": "pilot", "status": "complete"}))
        (case / "config.json").write_text(json.dumps({
            "condition": condition, "seed": 301, "duration": 10,
            "rates": {"A": 896, "B": 128, "D": 128, "C": 128},
            "microbatch_size": 32,
        }))
        (case / "release.json").write_text(json.dumps({"monotonic": 100.0}))
        (case / "outcome.json").write_text(json.dumps({"status": "completed", "measured_horizon": 10.0}))
        (case / "analysis.json").write_text(json.dumps({
            "valid": True,
            "learning": {"crossing_time": 5.0, "confirmed_time": 14.0,
                         "censored": False, "final_metrics": {
                             "DC_mean": 0.08, "DC_worst": 0.09, "A": 0.11}},
        }))

        rows = [
            # A's ordinary optimizer.step samples: index 1 is an update and is excluded.
            event("microbatch", "A", 100.10, batch_index=0, batch_size=32),
            event("step_complete", "A", 100.30, batch_index=0, seconds=0.05,
                  global_progress={"num_peers": 0}),
            event("microbatch", "A", 100.60, batch_index=1, batch_size=32),
            event("update_begin", "A", 100.61, batch_index=1),
            event("update_end", "A", 100.80, batch_index=1),
            event("step_complete", "A", 100.85, batch_index=1, seconds=0.07,
                  global_progress={"num_peers": 2}),
            event("microbatch", "A", 101.40, batch_index=2, batch_size=32),
            event("step_complete", "A", 101.45, batch_index=2, seconds=0.05,
                  global_progress={"num_peers": 2}),
            event("update_begin", "A", 101.30, batch_index=3),
            event("update_end", "A", 101.45, batch_index=3),
            event("step_complete", "A", "NaN", batch_index=9, seconds=-1.0,
                  global_progress={"num_peers": 4}),
            # A second node's update overlaps the state service interval.
            event("update_begin", "B", 102.20, batch_index=0),
            event("update_end", "B", 102.60, batch_index=0),
            # Max input is 102, max output is 103, so state service is 1s;
            # earliest participant wait is deliberately excluded.
            event("aggregate_input", "A", 101.00, prefix="state_averager",
                  group_id="state-1", channel="state", members=["A", "B"]),
            event("aggregate_output", "A", 102.50, prefix="state_averager",
                  group_id="state-1", channel="state", members=["A", "B"]),
            event("aggregate_input", "B", 102.00, prefix="state_averager",
                  group_id="state-1", channel="state", members=["A", "B"]),
            event("aggregate_output", "B", 103.00, prefix="state_averager",
                  group_id="state-1", channel="state", members=["A", "B"]),
            # Logical gradient service overlaps state by 0.6s.
            event("aggregate_input", "A", 102.40, prefix="run_grad_averager",
                  group_id="gradient-fast", channel="gradient", members=["A", "B"]),
            event("aggregate_output", "A", 103.20, prefix="run_grad_averager",
                  group_id="gradient-fast", channel="gradient", members=["A", "B"]),
            event("aggregate_input", "B", 102.30, prefix="run_grad_averager",
                  group_id="gradient-fast", channel="gradient", members=["A", "B"]),
            event("aggregate_output", "B", 103.50, prefix="run_grad_averager",
                  group_id="gradient-fast", channel="gradient", members=["A", "B"]),
            # Any fallback excludes the entire logical group from fast service.
            event("aggregate_input", "A", 106.00, prefix="run_grad_averager",
                  group_id="gradient-fallback", channel="gradient", members=["A", "B"]),
            event("aggregate_output", "A", 108.00, prefix="run_grad_averager",
                  group_id="gradient-fallback", channel="gradient", members=["A", "B"]),
            event("aggregate_input", "B", 106.10, prefix="run_grad_averager",
                  group_id="gradient-fallback", channel="gradient", members=["A", "B"]),
            event("fallback", "B", 107.00, prefix="run_grad_averager",
                  group_id="gradient-fallback", channel="gradient"),
            event("aggregate_output", "B", 108.50, prefix="run_grad_averager",
                  group_id="gradient-fallback", channel="gradient", members=["A", "B"]),
            # Native shutdown-only zero-weight groups are excluded from timing.
            event("training_terminal", "A", 109.00),
            event("training_terminal", "B", 109.00),
            event("allocation", "A", 111.00, prefix="run_grad_averager",
                  group_id="shutdown", weight=0, members=["A", "B"]),
            event("allocation", "B", 111.00, prefix="run_grad_averager",
                  group_id="shutdown", weight=0, members=["A", "B"]),
            event("aggregate_input", "A", 112.00, prefix="run_grad_averager",
                  group_id="shutdown", channel="gradient", members=["A", "B"]),
            event("aggregate_output", "A", 113.00, prefix="run_grad_averager",
                  group_id="shutdown", channel="gradient", members=["A", "B"]),
            event("aggregate_input", "B", 112.10, prefix="run_grad_averager",
                  group_id="shutdown", channel="gradient", members=["A", "B"]),
            event("aggregate_output", "B", 113.10, prefix="run_grad_averager",
                  group_id="shutdown", channel="gradient", members=["A", "B"]),
            # Partial, missing and invalid-time state groups are accounted for safely.
            event("aggregate_input", "A", 110.00, prefix="state_averager",
                  group_id="state-partial", channel="state", members=["A", "C"]),
            event("aggregate_output", "A", 111.00, prefix="state_averager",
                  group_id="state-partial", channel="state", members=["A", "C"]),
            event("group_started", "C", 112.00, prefix="state_averager",
                  group_id="state-missing", channel="state", members=None),
            event("aggregate_input", "A", 120.00, prefix="state_averager",
                  group_id="state-negative", channel="state", members=["A"]),
            event("aggregate_output", "A", 119.00, prefix="state_averager",
                  group_id="state-negative", channel="state", members=["A"]),
            event("state_recovery_fetch", "C", 113.0, available=True),
            event("state_recovery_application", "C", 114.0, phase="before"),
            # Invalid timestamps are ignored rather than leaking NaN into output.
            event("microbatch", "C", "NaN", batch_index=0, batch_size=32),
        ]
        (case / "events_case.jsonl").write_text(
            "".join(json.dumps(row, allow_nan=True) + "\n" for row in rows))
        return case

    def test_component_statistics_group_filtering_and_overlap(self):
        with tempfile.TemporaryDirectory() as temp:
            result = calibrate_case(self.make_case(Path(temp)))

        self.assertTrue(result["pilot_calibration_eligible"])
        self.assertEqual(result["suite_mode"], "pilot")
        self.assertEqual(result["nodes"]["A"]["ordinary_optimizer_step_seconds"]["n"], 2)
        self.assertAlmostEqual(result["nodes"]["A"]["ordinary_optimizer_step_seconds"]["median_seconds"], 0.05)
        expected_interbatch = ((100.60 - 100.30 - 32 / 896)
                               + (101.40 - 100.85 - 32 / 896)) / 2
        self.assertAlmostEqual(result["nodes"]["A"]["inter_batch_overhead_seconds"]["median_seconds"],
                               expected_interbatch)
        self.assertAlmostEqual(result["nodes"]["A"]["first_peer_progress_visible"]["lag_upper_bound_seconds"], .85)

        self.assertEqual(result["gradient_group_service_latency_seconds"]["n"], 1)
        self.assertAlmostEqual(result["gradient_group_service_latency_seconds"]["median_seconds"], 1.1)
        self.assertEqual(result["group_completeness"]["gradient"]["fallback_groups_excluded_from_fast_service"], 1)
        self.assertEqual(result["group_completeness"]["gradient"]["post_terminal_shutdown_groups_excluded"], 1)
        self.assertEqual(result["gradient_fallback_view_latency_seconds"]["B"]["n"], 1)
        self.assertAlmostEqual(result["gradient_fallback_view_latency_seconds"]["B"]["median_seconds"], 2.4)
        self.assertEqual(result["state_service_latency_seconds"]["n"], 1)
        self.assertAlmostEqual(result["state_service_latency_seconds"]["median_seconds"], 1.0)
        self.assertEqual(result["group_completeness"]["state"]["partial_groups"], 1)
        self.assertEqual(result["group_completeness"]["state"]["missing_groups"], 1)
        self.assertEqual(result["group_completeness"]["state"]["invalid_timing_groups"], 1)
        self.assertEqual(result["state_overlap"]["state_gradient_service_overlap_pair_count"], 1)
        self.assertAlmostEqual(result["state_overlap"]["state_gradient_service_overlap_pair_seconds"], .6)
        self.assertAlmostEqual(result["state_overlap"]["state_optimizer_update_overlap_pair_seconds"], .4)
        self.assertEqual(result["recoveries"]["fetch_event_count"], 1)
        self.assertEqual(result["recoveries"]["application_attempt_count"], 1)
        self.assertIsNone(result["recoveries"]["duration_seconds"])
        self.assertEqual(result["native_target"]["crossing_time_seconds"], 5.0)
        json.dumps(result, allow_nan=False)

    def test_exclusion_condition_does_not_supply_fast_service_parameter(self):
        with tempfile.TemporaryDirectory() as temp:
            result = calibrate_case(self.make_case(Path(temp), "exclusion"))
        self.assertEqual(result["gradient_group_service_latency_seconds"]["n"], 0)
        self.assertEqual(result["group_completeness"]["gradient"]["condition_groups_excluded_from_fast_service"], 1)

    def test_heldout_case_is_not_pilot_eligible(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            case = self.make_case(root)
            (case.parent / "status.json").write_text(json.dumps({"mode": "confirmation", "status": "complete"}))
            result = calibrate_case(case)
        self.assertFalse(result["pilot_calibration_eligible"])
        self.assertEqual(result["suite_mode"], "confirmation")
        json.dumps(result, allow_nan=False)


if __name__ == "__main__":
    unittest.main()
