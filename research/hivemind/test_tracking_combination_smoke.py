import importlib.util
import multiprocessing as mp
import os
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest


MODULE_PATH = Path(__file__).with_name("tracking_combination_smoke.py")
SPEC = importlib.util.spec_from_file_location("tracking_combination_smoke", MODULE_PATH)
probe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(probe)


class TrackingCombinationSmokeTests(unittest.TestCase):
    def test_filter_roster_keeps_gathered_payloads_aligned(self):
        peers = ("A", "X", "H2")
        gathered = (b"opaque-A", b"opaque-X", b"opaque-H2")
        ids, metadata, removed = probe.filter_roster(peers, gathered, "A", str)

        self.assertTrue(removed)
        self.assertEqual(ids, ("X", "H2"))
        self.assertEqual(metadata, (b"opaque-X", b"opaque-H2"))

    def test_summary_separates_capture_tracking_and_attacker_local_omission(self):
        events = [
            {"kind": "aggregate_view", "arm": "capture_exclusion", "channel": "gradient_probe",
             "round": 0, "peer": "X", "leader": "X", "native_members": ["A", "X"],
             "group_id": "g0",
             "effective_members": ["X"], "target_coefficient_after": 0.0,
             "predicted_victim_next_bits": "1"},
            {"kind": "aggregate_output", "arm": "capture_exclusion", "channel": "gradient_probe",
             "round": 0, "peer": "X", "group_id": "g0", "target_coefficient_after": 0.0},
            {"kind": "aggregate_view", "arm": "capture_exclusion", "channel": "gradient_probe",
             "round": 0, "peer": "A", "leader": "X", "group_id": "g0",
             "native_members": ["A", "X"], "effective_members": ["A", "X"]},
            {"kind": "bucket_advertisement", "arm": "capture_exclusion", "channel": "gradient_probe",
             "round": 1, "peer": "A", "bucket": "1", "looking_for_group": True},
            {"kind": "bucket_advertisement", "arm": "capture_exclusion", "channel": "gradient_probe",
             "round": 1, "peer": "X", "bucket": "1", "looking_for_group": True},
            {"kind": "step_result", "arm": "capture_exclusion", "channel": "gradient_probe",
             "round": 0, "peer": "H2", "status": "no_group"},
        ]

        result = probe.summarize_events(events, "capture_exclusion", "gradient_probe", rounds=2)

        self.assertEqual(result["captured_rounds_with_attacker_as_leader"], [0])
        self.assertEqual(result["successful_victim_next_bucket_predictions"], 1)
        self.assertEqual(result["attacker_next_bucket_matches"], 1)
        self.assertEqual(result["attacker_local_omission_rounds"], [0])
        self.assertEqual(result["attacker_local_omission_target_coefficients"], [0.0])
        self.assertFalse(result["honest_follower_received_rewritten_roster_tested"])
        self.assertEqual(result["step_attempt_count"], 1)

    def test_control_arm_does_not_report_local_omission(self):
        events = [{"kind": "aggregate_view", "arm": "capture_control", "channel": "state_probe",
                   "round": 0, "peer": "X", "leader": "X", "native_members": ["A", "X"],
                   "effective_members": ["A", "X"], "target_coefficient_after": 0.5}]

        result = probe.summarize_events(events, "capture_control", "state_probe", rounds=1)

        self.assertEqual(result["attacker_local_omission_rounds"], [])
        self.assertEqual(result["captured_rounds_with_attacker_as_leader"], [0])

    def test_native_averager_helpers_do_not_shadow_process_fields(self):
        package_root = probe.ROOT / "research/simulator/.native-runtime/root_snapshot_a"
        dependencies = probe.ROOT / "research/agora/.runtime/venv/lib/python3.11/site-packages"
        if not package_root.exists() or not dependencies.exists():
            self.skipTest("prepared native runtime is unavailable")

        original_cwd = Path.cwd()
        original_path = list(sys.path)
        try:
            probe.configure_native(SimpleNamespace(package_root=package_root,
                                                   dependencies=dependencies))
            averager_class = probe.make_averager_class()
            from hivemind.averaging.control import AveragingStage

            # Process initializes these fields on each instance. A same-named
            # subclass method would be replaced by the field, as happened with
            # the former _name helper.
            process_fields = set(vars(mp.Process(target=lambda: None)))
            subclass_methods = {
                name for name, member in averager_class.__dict__.items()
                if callable(member) and not name.startswith("__")
            }
            self.assertIn("_name", process_fields)
            self.assertEqual(sorted(process_fields & subclass_methods), [])
            self.assertTrue(callable(averager_class.__dict__["_peer_label"]))
            self.assertEqual(probe._stage_label(AveragingStage.IDLE), "IDLE")
        finally:
            os.chdir(original_cwd)
            sys.path[:] = original_path


if __name__ == "__main__":
    unittest.main()
