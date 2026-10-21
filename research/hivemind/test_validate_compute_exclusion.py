"""Synthetic native-event proofs for the compute numerical replay validator."""
from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import unittest


HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "simulator" / "src"))

from trainsim.protocols.hivemind.numerical import aggregate
from validate_compute_exclusion import replay_case


NODES = ("A", "B", "C")


def _events_for_group(*, prefix="case_grad_averager", group_id="g1", views=None,
                      weights=None, chunk_elements=2):
    import numpy as np

    views = views or {node: NODES for node in NODES}
    weights = weights or {"A": 1.0, "B": 2.0, "C": 3.0}
    inputs = {
        "A": np.arange(12, dtype=np.float32),
        "B": np.arange(12, dtype=np.float32) + 10,
        "C": np.arange(12, dtype=np.float32) + 30,
    }
    result = aggregate(inputs, views, chunk_elements=chunk_elements, weights=weights)
    events = []

    def emit(kind, node, **fields):
        events.append({"kind": kind, "node": node, "worker": node,
                       "channel": "gradient" if "grad" in prefix else "state",
                       "prefix": prefix, "traceprefix": prefix,
                       "group_id": group_id, "monotonic": 100.0,
                       **fields})

    for node in NODES:
        roster = list(views[node])
        native_parts = {
            peer: [{"tensor_index": 0, "start": start, "end": end,
                    "elements": end - start}
                   for start, end in result.allocations[node][peer]]
            for peer in roster
        }
        emit("aggregate_input", node, peer=node, members=roster,
             values=[inputs[node].tolist()])
        emit("allocation", node, peer=node, members=roster,
             peer_fractions=[1.0] * len(roster), weight=weights[node],
             parts=[len(native_parts[peer]) for peer in roster],
             part_ranges=native_parts, total_elements=inputs[node].size)
        emit("aggregate_output", node, peer=node, members=roster,
             values=[result.values[node].tolist()])
        emit("terminal", node, peer=node, outcome="complete")
        failed_size = sum(end - start for start, end in result.fallback_ranges[node])
        emit("finalize", node, failed_size=failed_size, total_elements=inputs[node].size)

    for node in NODES:
        trace_rows = [row for row in result.trace
                      if row.get("kind") == "processed_chunk" and row["node"] == node]
        for row in trace_rows:
            emit("processed_chunk", node, reducer=row["reducer"],
                 part_index=row["part_index"], values=row["delta"])
        fallback_by_reducer = {}
        for row in trace_rows:
            if row.get("fallback"):
                fallback_by_reducer.setdefault(row["reducer"], []).append(row["part_index"])
        for reducer, indices in fallback_by_reducer.items():
            emit("fallback", node, reducer=reducer, registered_parts=min(indices),
                 part_count=len(result.allocations[node][reducer]))
    return events, result


def _write_case(root: Path, events):
    root.mkdir(parents=True, exist_ok=True)
    with (root / "events_A_1.jsonl").open("w") as stream:
        for row in events:
            stream.write(json.dumps(row, allow_nan=False) + "\n")


class ComputeReplayTests(unittest.TestCase):
    def test_replays_full_weighted_native_group_and_state_group(self):
        gradient, _ = _events_for_group()
        state, _ = _events_for_group(prefix="case_state_averager", group_id="s1",
                                     weights={node: 1.0 for node in NODES})
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _write_case(root, gradient + state)
            report = replay_case(root)
        self.assertTrue(report["valid"], report["failures"])
        self.assertFalse(report["simulation_gate_eligible"])
        self.assertEqual(report["checks"]["groups_seen"], 2)
        self.assertEqual(report["checks"]["groups_replayed"], 2)
        self.assertEqual(report["checks"]["outputs_checked"], 6)
        self.assertGreater(report["checks"]["native_chunks_checked"], 0)

    def test_replays_native_fallback_ranges(self):
        views = {"A": ("A", "B"), "B": ("B", "C"), "C": ("C", "A")}
        events, result = _events_for_group(views=views)
        self.assertTrue(any(result.fallback_ranges.values()))
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _write_case(root, events)
            report = replay_case(root)
        self.assertTrue(report["valid"], report["failures"])
        self.assertEqual(report["checks"]["fallback_nodes_checked"], 3)

    def test_output_and_allocation_tampering_fail_closed(self):
        for tamper in ("output", "allocation"):
            with self.subTest(tamper=tamper), tempfile.TemporaryDirectory() as temporary:
                events, _ = _events_for_group()
                if tamper == "output":
                    row = next(row for row in events
                               if row["kind"] == "aggregate_output" and row["node"] == "B")
                    row["values"][0][0] += 1.0
                else:
                    row = next(row for row in events
                               if row["kind"] == "allocation" and row["node"] == "A")
                    row["part_ranges"]["A"][0]["end"] -= 1
                    row["part_ranges"]["A"][0]["elements"] -= 1
                root = Path(temporary)
                _write_case(root, events)
                report = replay_case(root)
                self.assertFalse(report["valid"])
                self.assertTrue(report["failures"])

    def test_partial_or_incomplete_timing_group_is_unmodeled(self):
        events, _ = _events_for_group()
        events = [row for row in events if not (
            row["kind"] == "aggregate_output" and row["node"] == "C")]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _write_case(root, events)
            report = replay_case(root)
        self.assertFalse(report["valid"])
        self.assertEqual(report["groups"][0]["status"], "unmodeled")
        self.assertTrue(any("aggregate_output records" in message for message in report["failures"]))

    def test_shutdown_only_zero_weight_group_is_explicitly_excluded(self):
        events, _ = _events_for_group()
        for row in events:
            if row["kind"] == "allocation":
                row["weight"] = 0.0
                row["monotonic"] = 20.0
            elif row["kind"] == "aggregate_input":
                row["monotonic"] = 20.0
            elif row["kind"] in {"aggregate_output", "processed_chunk"}:
                row["values"] = [["nan"] * 12] if row["kind"] == "aggregate_output" else ["nan"] * 2
                row["monotonic"] = 21.0
            elif row["kind"] == "terminal":
                row["monotonic"] = 22.0
            elif row["kind"] == "finalize":
                row["monotonic"] = 22.0
        for node in NODES:
            events.append({"kind": "gradient_loaded", "node": node, "worker": node,
                           "channel": "gradient", "prefix": "case_grad_averager",
                           "traceprefix": "case_grad_averager",
                           "group_id": None, "monotonic": 10.0})
            events.append({"kind": "training_terminal", "node": node, "worker": node,
                           "monotonic": 19.0})
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _write_case(root, events)
            report = replay_case(root)
        self.assertTrue(report["valid"], report["failures"])
        self.assertEqual(report["checks"]["groups_replayed"], 0)
        self.assertEqual(report["checks"]["groups_excluded_shutdown"], 1)
        self.assertEqual(report["groups"][0]["status"], "excluded_shutdown")
        self.assertEqual(len(report["excluded_shutdown_groups"]), 1)

    def test_zero_weight_group_after_training_activity_is_not_excluded(self):
        events, _ = _events_for_group()
        for row in events:
            if row["kind"] == "allocation":
                row["weight"] = 0.0
            elif row["kind"] in {"aggregate_output", "processed_chunk"}:
                row["values"] = [["nan"] * 12] if row["kind"] == "aggregate_output" else ["nan"] * 2
        for node in NODES:
            events.append({"kind": "training_terminal", "node": node,
                           "worker": node, "monotonic": 10.0})
            events.append({"kind": "gradient_loaded", "node": node,
                           "worker": node, "monotonic": 11.0})
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _write_case(root, events)
            report = replay_case(root)
        self.assertFalse(report["valid"])
        self.assertEqual(report["excluded_shutdown_groups"], [])
        self.assertEqual(report["groups"][0]["status"], "unmodeled")


if __name__ == "__main__":
    unittest.main()
