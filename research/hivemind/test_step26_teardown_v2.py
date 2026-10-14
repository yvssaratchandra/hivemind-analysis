"""Synthetic checks for the Step 26 v2 measurement-tail audit.

These tests are offline: they exercise the post-control-prefix classifier and
its wrapper around the frozen RPC analyzer without starting peers or changing
raw traces.
"""
from __future__ import annotations

from collections import defaultdict
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

import analyze_step26_joint as analyzer
import step26_teardown_audit_v2 as teardown


def event(kind, number, when, *, detail=None, pid=41, peer="X", other=""):
    return {"kind": kind, "pid": pid, "event_number": number,
            "monotonic": when, "wall_time": 1000.0 + when,
            "peer": peer, "other": other, "detail": detail or {}}


def activity_trace(prefixes=(), *, final_status="completed"):
    """Control work ends at 9s, while the final training step ends at 12s."""
    events = [
        event("step_start", 1, 1.0, detail={"name": "X", "sequence": 1}),
        event("search_span_start", 2, 2.0, detail={"search_id": "s1"}),
        event("search_span_end", 3, 3.0, detail={"search_id": "s1"}),
        event("ad_read_start", 4, 4.0, detail={"read_id": "r1"}),
        event("ad_read_end", 5, 5.0, detail={"read_id": "r1"}),
        event("join_attempt_start", 6, 6.0, detail={"join_id": "j1"}),
        event("join_attempt_end", 7, 7.0, detail={"join_id": "j1"}),
        event("ad_finished", 8, 9.0, detail={"active": True}),
    ]
    events.extend(prefixes)
    events.extend([
        event("step_result", 24, 12.0,
              detail={"name": "X", "sequence": 1, "status": final_status}),
        event("trace_closed", 25, 100.0, detail={"error": None, "pending": []}),
    ])
    return events


def trial11_prefixes():
    return [
        event("control_rpc_start", 20, 10.0,
              detail={"rpc_id": "find-after-control", "rpc_type": "find",
                      "delay_seconds": .15, "key_hashes": ["key1"], "step_id": None}),
        event("control_rpc_start", 21, 10.2, pid=42,
              detail={"rpc_id": "store-after-control", "rpc_type": "store",
                      "delay_seconds": .15, "key_hashes": ["key2"], "step_id": None}),
        event("control_rpc_delay", 22, 10.4, pid=42,
              detail={"rpc_id": "store-after-control", "leg": "request",
                      "requested_delay": .15, "actual_delay": .15, "cancelled": False}),
        event("control_rpc_handler_enter", 23, 10.6, pid=42,
              detail={"rpc_id": "store-after-control"}),
    ]


def native_pass():
    return {"control_plane_pass": True, "trace_integrity_pass": True, "reads": []}


class Step26TeardownV2Tests(unittest.TestCase):
    def classify(self, events, native=None, delay=.15):
        return teardown.classify_tail(events, native or native_pass(), configured_delay=delay)

    def test_uses_control_cutoff_and_accepts_both_trial11_prefix_shapes(self):
        result = self.classify(activity_trace(trial11_prefixes()))

        self.assertEqual(result["control_activity_cutoff_monotonic"], 9.0)
        self.assertEqual({row["rpc_id"] for row in result["excluded"]},
                         {"find-after-control", "store-after-control"})
        by_id = {row["rpc_id"]: row for row in result["excluded"]}
        self.assertEqual(by_id["find-after-control"]["prefix_shape"], ["control_rpc_start"])
        self.assertEqual(by_id["store-after-control"]["prefix_shape"],
                         ["control_rpc_start", "control_rpc_delay", "control_rpc_handler_enter"])
        self.assertTrue(all("completion, or outcome is observed or inferred" in row["reason"]
                            for row in result["excluded"]))
        # The start-only and handler-only evidence ends before the final step result at 12s.
        self.assertLess(by_id["store-after-control"]["prefix_events"][-1]["monotonic"], 12.0)
        self.assertEqual(result["unexcluded_incomplete_streams"], [])

    def test_failed_step_result_is_diagnostic_and_late_assembly_moves_cutoff(self):
        failed = self.classify(activity_trace(trial11_prefixes(), final_status="failed"))
        self.assertEqual(len(failed["excluded"]), 2)

        late_assembly = activity_trace([
            event("assembled", 19, 10.2, detail={"step_id": "H1:1"}),
            event("control_rpc_start", 20, 10.0,
                  detail={"rpc_id": "before-assembly", "rpc_type": "find",
                          "delay_seconds": .15, "key_hashes": ["key"], "step_id": None}),
        ])
        result = self.classify(late_assembly)
        self.assertEqual(result["control_activity_cutoff_monotonic"], 10.2)
        self.assertEqual(result["excluded"], [])

    def test_boundary_join_completed_or_malformed_prefixes_and_incomplete_gates_are_rejected(self):
        base = activity_trace()

        def rpc_start(number, when, rpc_id, rpc_type="find", **extra):
            detail = {"rpc_id": rpc_id, "rpc_type": rpc_type,
                      "delay_seconds": .15, "key_hashes": ["key"]}
            detail.update(extra)
            return event("control_rpc_start", number, when, detail=detail)

        scenarios = {
            "before cutoff": (activity_trace([rpc_start(30, 8.5, "before")]), native_pass()),
            "at cutoff": (activity_trace([rpc_start(30, 9.0, "equal")]), native_pass()),
            "join": (activity_trace([rpc_start(30, 10.0, "join", "join")]), native_pass()),
            "response observed": (activity_trace([rpc_start(30, 10.0, "response"),
                event("control_rpc_response", 31, 10.1,
                      detail={"rpc_id": "response", "response_index": 0})]), native_pass()),
            "wrong order": (activity_trace([rpc_start(30, 10.0, "order"),
                event("control_rpc_handler_enter", 31, 10.1, detail={"rpc_id": "order"}),
                event("control_rpc_delay", 32, 10.2, detail={"rpc_id": "order", "leg": "request",
                      "requested_delay": .15, "actual_delay": .15, "cancelled": False})]), native_pass()),
            "cancelled": (activity_trace([rpc_start(30, 10.0, "cancelled"),
                event("control_rpc_delay", 31, 10.2, detail={"rpc_id": "cancelled", "leg": "request",
                      "requested_delay": .15, "actual_delay": .15, "cancelled": True}),
                event("control_rpc_handler_enter", 32, 10.3, detail={"rpc_id": "cancelled"})]), native_pass()),
            "short delay": (activity_trace([rpc_start(30, 10.0, "short"),
                event("control_rpc_delay", 31, 10.2, detail={"rpc_id": "short", "leg": "request",
                      "requested_delay": .15, "actual_delay": .05, "cancelled": False}),
                event("control_rpc_handler_enter", 32, 10.3, detail={"rpc_id": "short"})]), native_pass()),
            "bad metadata": (activity_trace([rpc_start(30, 10.0, "bad", key_hashes=[1])]), native_pass()),
            "step id present": (activity_trace([rpc_start(30, 10.0, "step-id", step_id="H1:1")]), native_pass()),
            "handler peer mismatch": (activity_trace([
                rpc_start(30, 10.0, "peer"),
                event("control_rpc_delay", 31, 10.2, detail={"rpc_id": "peer", "leg": "request",
                      "requested_delay": .15, "actual_delay": .15, "cancelled": False}),
                event("control_rpc_handler_enter", 32, 10.3, peer="Y", detail={"rpc_id": "peer"})]), native_pass()),
            "missing native pass": (activity_trace([rpc_start(30, 10.0, "native")]),
                {"control_plane_pass": True, "trace_integrity_pass": False}),
            "incomplete spans": ([row for row in base if row["kind"] != "ad_read_end"] +
                [rpc_start(30, 10.0, "span")], native_pass()),
        }
        for label, (events, native) in scenarios.items():
            with self.subTest(label=label):
                self.assertEqual(self.classify(events, native)["excluded"], [])

    def test_real_rpc_analyzer_wrapper_filters_only_eligible_prefixes(self):
        # A completed response stream remains in the original analyzer's input.
        events = activity_trace([
            event("control_rpc_start", 20, 10.0,
                  detail={"rpc_id": "eligible-find", "rpc_type": "find",
                          "delay_seconds": .15, "key_hashes": ["key"], "step_id": None}),
            event("control_rpc_start", 30, 10.0, pid=43,
                  detail={"rpc_id": "keep-response", "rpc_type": "store",
                          "delay_seconds": .15, "key_hashes": ["key"]}),
            event("control_rpc_response", 31, 10.1, pid=43,
                  detail={"rpc_id": "keep-response", "response_index": 0}),
        ])
        adapter = SimpleNamespace(_rpc_integrity=analyzer._rpc_integrity)
        teardown.install(adapter)
        by_kind, by_ref = defaultdict(list), {}
        for row in events:
            by_kind[row["kind"]].append(row)
            by_ref[f"{row['pid']}:{row['event_number']}"] = row

        result = adapter._rpc_integrity({"one_way_delay_seconds": .15}, {"peer_ids": {}},
                                        events, by_kind, by_ref, native_pass())
        self.assertEqual(result["summary"]["teardown_v2_excluded_stream_count"], 1)
        self.assertEqual(result["summary"]["rpc_counts"]["find"], 0)
        self.assertEqual(result["summary"]["rpc_counts"]["store"], 1)
        self.assertFalse(result["pass_"])
        self.assertTrue(any("keep-response" in error for error in result["errors"]))


if __name__ == "__main__":
    unittest.main()
