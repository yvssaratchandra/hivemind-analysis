import json
import tempfile
import unittest
from pathlib import Path

from compute_timing import summarize_case


class ComputeTimingTests(unittest.TestCase):
    def make_case(self, root):
        case = Path(root)
        for name, value in {
            "config.json": {"condition": "exclusion", "mode": "training", "duration": 8},
            "outcome.json": {"status": "native_noncompletion", "completed": False,
                             "stuck_nodes": ["A"], "measured_horizon": 2},
            "release.json": {"monotonic": 100, "ready": ["A", "B", "C", "D"]},
        }.items():
            (case / name).write_text(json.dumps(value))

        events = []

        def add(node, kind, at, **fields):
            events.append({"node": node, "kind": kind, "monotonic": 100 + at, **fields})

        for i in range(4):
            add("B", "microbatch", i + .1, batch_index=i, batch_size=32,
                compute_seconds=.02, pacing_seconds=.1)
        add("B", "accumulation", .3, batch_index=0, accepted=True, sample_count=32)
        add("B", "accumulation", 1.3, batch_index=1, accepted=False, sample_count=32)
        for i in (0, 1, 2):
            add("B", "step_complete", i + .5, batch_index=i, seconds=.2)
        for index, at in ((0, 1), (2, 4)):
            add("B", "update_begin", at, batch_index=index)
            add("B", "update_end", at + .5, batch_index=index)

        members = ["B", "D", "A", "C"]
        prefix, gid = "run_grad_averager", "g1"
        weights = {"B": 32, "D": 32, "A": 224, "C": 32}
        for node in members:
            add(node, "local_roster", 2, prefix=prefix, group_id=gid,
                native_members=members, members=["B", "D", "C"] if node == "B" else members)
            add(node, "allocation", 2.1, prefix=prefix, group_id=gid,
                members=members, weight=weights[node])
            add(node, "aggregate_input", 2.2, prefix=prefix, group_id=gid, channel="gradient")
            add(node, "aggregate_output", 2.5, prefix=prefix, group_id=gid, channel="gradient")
            add(node, "training_terminal", 5, batches=4)

        # Native shutdown can leave a prescheduled zero-weight group after each
        # worker's terminal event; its timings and weights are excluded.
        shutdown_prefix, shutdown_gid = "run_grad_averager", "shutdown"
        for node in members:
            add(node, "allocation", 6, prefix=shutdown_prefix, group_id=shutdown_gid,
                members=members, weight=0)
            add(node, "aggregate_input", 6.1, prefix=shutdown_prefix,
                group_id=shutdown_gid, channel="gradient")
            add(node, "aggregate_output", 6.2, prefix=shutdown_prefix,
                group_id=shutdown_gid, channel="gradient")
        add("B", "state_application", 3, prefix="run_state_averager", phase="after")
        add("B", "state_recovery_fetch", 3.1, prefix="run_state_averager", available=True)
        add("B", "state_recovery_application", 3.2, prefix="run_state_averager", phase="after")
        add("B", "state_download_chunk", 3.3, sender="A", recipient="B", prefix="run_state_averager")
        add("A", "state_download_chunk", 3.4, sender="B", recipient="A", prefix="run_state_averager")
        (case / "events_B_1.jsonl").write_text("".join(json.dumps(e) + "\n" for e in events))
        return case

    def test_noncompletion_horizon_counts_pacing_and_update_pairs(self):
        with tempfile.TemporaryDirectory() as tmp:
            case = self.make_case(tmp)
            before = (case / "events_B_1.jsonl").read_bytes()
            result = summarize_case(case)
            self.assertEqual(result["outcome_status"], "native_noncompletion")
            self.assertEqual(result["measured_horizon_seconds"], 2)
            self.assertEqual(result["nodes"]["B"]["submitted_samples"], 128)
            self.assertEqual(result["nodes"]["B"]["submitted_samples_per_second"], 64)
            self.assertIn("already-computed microbatch", result["sample_counting_note"])
            self.assertEqual(result["untraced_boundary_examples_upper_bound_per_node"], 32)
            self.assertEqual(result["nodes"]["B"]["accepted_samples"], 32)
            self.assertEqual(result["nodes"]["B"]["discarded_samples"], 64)
            self.assertEqual(result["nodes"]["B"]["pending_samples"], 32)
            self.assertEqual(result["nodes"]["B"]["completed_updates"], 2)
            self.assertEqual(result["nodes"]["B"]["median_update_interval_seconds"], 3)
            self.assertEqual(before, (case / "events_B_1.jsonl").read_bytes())

    def test_unique_weights_roster_omission_and_shutdown_exclusion(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = summarize_case(self.make_case(tmp))
            groups = {row["group_id"]: row for row in result["unique_group_weight_totals"]}
            self.assertEqual(groups["g1"]["unique_origin_weight_total"], 320)
            self.assertEqual(groups["g1"]["A_weight_share"], .7)
            self.assertNotIn("shutdown", groups)
            roster = result["roster_contribution_summary"]
            self.assertEqual(roster["A_proposed_view_count"], 4)
            self.assertEqual(roster["A_omitted_view_count"], 1)
            self.assertEqual(roster["A_weight_in_groups_with_an_omitted_view"], 224)
            self.assertEqual(result["post_terminal_shutdown_groups_excluded"], 1)
            self.assertEqual(result["post_terminal_shutdown_group_views_excluded"], 4)
            self.assertEqual(result["state"]["A_paths"], {"A->B": 1, "B->A": 1})


if __name__ == "__main__":
    unittest.main()
