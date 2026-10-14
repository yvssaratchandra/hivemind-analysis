"""Offline design and measurement contracts for the Step 30 final comparison."""
from __future__ import annotations

import asyncio
import builtins
import csv
import contextlib
import io
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import types
import unittest
from types import SimpleNamespace
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

import step30_harm as sweep


def write_json(path: Path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")


def make_suite_fixture(path: Path):
    jobs = sweep.make_jobs("smoke")
    path.mkdir(parents=True, exist_ok=True)
    write_json(path / "suite.json", {
        "experiment": "step30", "phase": "smoke", "jobs": jobs,
        "training_delay": sweep.TRAINING, "delay_profile": sweep.PROFILE,
        "decision_rule": sweep.DECISION,
        "outcome": "grouping", "control_only": True,
    })
    write_json(path / "provenance.json", {})
    return jobs


def make_completed_trial(suite: Path, job):
    trial = suite / f"{job['case']['name']}_s{job['seed']}"
    trial.mkdir(parents=True)
    write_json(trial / "config.json", {
        "case": job["case"], "seed": job["seed"], "duration": job["duration"],
    })
    (trial / "events.jsonl").write_text('{"kind":"trace_closed"}\n', encoding="utf-8")
    write_json(trial / "summary.json", {"pending": [], "error": None})
    write_json(trial / "analysis.json", {"control_only": True})
    write_json(trial / "control_analysis.json", {"integrity_pass": True})
    write_json(trial / "raw_verification.json", {"pass": True, "errors": []})
    write_json(trial / "completion.json", {
        "all_peers_shutdown": True,
        "file_sha256": {name: sweep.sha256(trial / name) for name in sweep.EVIDENCE_FILES},
    })
    return trial


class DesignTests(unittest.TestCase):
    def test_only_the_two_unreliable_x_publication_policies_are_compared(self):
        cases = sweep.make_cases()
        self.assertEqual([case["name"] for case in cases], ["stock_2s", "renew_1_2"])
        self.assertEqual({case["publication_policy"] for case in cases}, {"native", "renew"})
        for case in cases:
            self.assertEqual(case["behavior"], "recruit_only")
            self.assertEqual(case["mode"], "passive")
            self.assertEqual(case["lifetime"], 2.0)
            self.assertEqual((case["minimum"], case["target"]), (2, 4))
            self.assertEqual((case["response"], case["hold_seconds"]), ("unreliable", .5))
            self.assertEqual(case["one_way_delay_seconds"], .15)
        renewed = cases[1]
        self.assertEqual((renewed["frequency"], renewed["nominal_lower"],
                          renewed["nominal_upper"]), (1.0, 1.0, 2.0))
        self.assertEqual(sweep.PROFILE,
                         {"distribution": "uniform", "lower": .1, "upper": .2})
        self.assertEqual(sweep.DECISION["wait_increase_seconds"], .5)
        self.assertEqual(sweep.DECISION["failure_increase_fraction"], .05)
        self.assertEqual(sweep.DECISION["group_rate_decrease_fraction"], .15)
        self.assertEqual(sweep.DECISION["minimum_threshold_seeds"], 2)
        self.assertEqual(sweep.DECISION["minimum_late_windows"], 3)
        self.assertEqual(sweep.DECISION["honest_starts_per_trial"], 100)
        self.assertEqual(sweep.DECISION["x_client_acceptances_per_trial"], 20)
        self.assertEqual(sweep.DECISION["x_multi_publication_searches"], 10)

    def test_smoke_and_confirm_jobs_are_paired_and_exact(self):
        smoke = sweep.make_jobs("smoke")
        self.assertEqual([(j["case"]["name"], j["seed"], j["duration"]) for j in smoke], [
            ("stock_2s", 3000, 20.0), ("renew_1_2", 3000, 20.0),
        ])
        confirm = sweep.make_jobs("confirm")
        self.assertEqual(len(confirm), 6)
        self.assertEqual([j["seed"] for j in confirm], [3001, 3001, 3002, 3002, 3003, 3003])
        self.assertEqual([j["case"]["name"] for j in confirm], [
            "stock_2s", "renew_1_2", "renew_1_2", "stock_2s", "stock_2s", "renew_1_2",
        ])
        self.assertTrue(all(j["duration"] == 300.0 for j in confirm))
        self.assertEqual(confirm, sweep.make_jobs("confirm"))
        with self.assertRaises(ValueError):
            sweep.make_jobs("pilot")

    def test_native_expiration_and_serialized_renewal_cadence_boundaries(self):
        # Native expiration is max(scheduled, now + lifetime), capped at this search's deadline.
        self.assertEqual(sweep.native_expiration(100., 99., 2., 110.), 102.)
        self.assertEqual(sweep.native_expiration(100., 104., 2., 110.), 104.)
        self.assertEqual(sweep.native_expiration(100., 99., 2., 101.), 101.)
        self.assertEqual(sweep.native_expiration(100., 99., 2., 100.), 100.)
        # A one-second interval is measured from completion of the serialized write.
        self.assertAlmostEqual(sweep.renewal_sleep(110., 100., .2), .8)
        self.assertAlmostEqual(sweep.renewal_sleep(100.25, 100., .1), .25)
        self.assertEqual(sweep.renewal_sleep(110., 100., 1.2), 0.)

    def test_delay_is_reproducible_per_logical_rpc_leg_and_not_process_identity(self):
        args = (3001, "H2", "X", "find", "request", 4)
        first = sweep.deterministic_delay(*args)
        self.assertEqual(first, sweep.deterministic_delay(*args))
        self.assertGreaterEqual(first, .1)
        self.assertLessEqual(first, .2)
        variants = {
            sweep.deterministic_delay(3002, *args[1:]),
            sweep.deterministic_delay(3001, "H3", *args[2:]),
            sweep.deterministic_delay(3001, *args[1:5], 5),
            sweep.deterministic_delay(3001, *args[1:5], 4, 1),
        }
        self.assertTrue(all(.1 <= value <= .2 for value in variants))
        self.assertGreater(len(variants), 1)
        with patch.object(sweep.os, "getpid", return_value=111):
            from_pid_one = sweep.deterministic_delay(*args)
        with patch.object(sweep.os, "getpid", return_value=222):
            from_pid_two = sweep.deterministic_delay(*args)
        self.assertEqual(from_pid_one, from_pid_two)

    def test_clock_discontinuity_checks_wall_and_suspend_aware_boot_offsets(self):
        stable = [
            {"clock_wall": 10.0, "clock_monotonic": 1.0, "clock_boottime": 11.0},
            {"clock_wall": 11.0, "clock_monotonic": 2.0, "clock_boottime": 12.0},
        ]
        self.assertEqual(sweep.check_clock(stable), [])
        wall_jump = [dict(stable[0]), dict(stable[1], clock_wall=11.5)]
        self.assertTrue(any("clock_wall" in error for error in sweep.check_clock(wall_jump)))
        suspend_jump = [dict(stable[0]), dict(stable[1], clock_boottime=12.5)]
        self.assertTrue(any("clock_boottime" in error
                            for error in sweep.check_clock(suspend_jump)))


class DelayInstrumentationTests(unittest.IsolatedAsyncioTestCase):
    async def test_rpc_legs_log_seeded_draws_and_increment_stream_response_indexes(self):
        calls = []
        old_obs, old_network = sweep.OBS, sweep.NETWORK
        old_counts, old_info = sweep.RPC_COUNTS, sweep.RPC_INFO

        class Serializer:
            @staticmethod
            def loads(value):
                if isinstance(value, bytes) and value.startswith(b"outer:"):
                    return (None, None, value[len(b"outer:"):])
                if isinstance(value, bytes):
                    return json.loads(value.decode())
                return value

        sweep.OBS = SimpleNamespace(
            sweep=SimpleNamespace(MSGPackSerializer=Serializer),
            emit=lambda kind, peer="", other="", **detail:
            calls.append((kind, peer, other, detail)))
        identities = {"H0": "peer-h0", "X": "peer-x"}
        try:
            sweep.configure_network(3001, identities)
            server = SimpleNamespace(peer_id="peer-x")
            context = SimpleNamespace(remote_id="peer-h0")
            gather = b"outer:" + json.dumps({"step_id": "H0:3"}).encode()
            request = SimpleNamespace(keys=[b"group-key"], gather=gather,
                                      expiration=1791815058.5, group_key=b"group-key")
            rpc_id, logged_mean, peer = sweep.rpc_start(server, request, context, "join")
            self.assertEqual((logged_mean, peer), (.15, "peer-x"))
            with patch.object(sweep.asyncio, "sleep", new=unittest.mock.AsyncMock()):
                await sweep.injected_sleep(rpc_id, "request", logged_mean)
                await sweep.injected_sleep(rpc_id, "response", logged_mean)
                await sweep.injected_sleep(rpc_id, "response", logged_mean)
        finally:
            sweep.OBS, sweep.NETWORK = old_obs, old_network
            sweep.RPC_COUNTS, sweep.RPC_INFO = old_counts, old_info

        starts = [row for row in calls if row[0] == "control_rpc_start"]
        self.assertEqual(len(starts), 1)
        self.assertEqual(starts[0][3]["delay_profile"], sweep.PROFILE)
        self.assertEqual(starts[0][3]["delay_seconds_semantics"],
                         "profile mean; actual per-leg draws recorded separately")
        self.assertEqual(starts[0][3]["request_expiration"], 1791815058.5)
        self.assertEqual(starts[0][3]["caller_step_id"], "H0:3")
        self.assertEqual(starts[0][3]["request_group_key"], b"group-key")
        legs = [row[3] for row in calls if row[0] == "control_rpc_delay"]
        self.assertEqual([(row["leg"], row["response_index"]) for row in legs], [
            ("request", 0), ("response", 0), ("response", 1),
        ])
        for row in legs:
            self.assertGreaterEqual(row["requested_delay"], .1)
            self.assertLessEqual(row["requested_delay"], .2)


class AnalyzerSemanticsTests(unittest.TestCase):
    @staticmethod
    def event(kind, peer, other, when, number, **detail):
        return {"kind": kind, "peer": peer, "other": other, "pid": 1,
                "event_number": number, "monotonic": when, "detail": detail}

    def test_roster_claim_is_not_follower_success_until_that_peer_assembles(self):
        import analyze_step30_harm as analysis

        ids = {"X": "peer-x", "H0": "peer-h0"}
        members = {"peer-x": {"step_id": "X:0"}, "peer-h0": {"step_id": "H0:0"}}
        groups = [{"group_id": "g1", "leader_peer_id": "peer-x",
                   "roster": ["peer-x", "peer-h0"], "assembly_monotonic": 2.0}]
        attempts = [
            {"peer_id": "peer-x", "peer_name": "X", "step_id": "X:0",
             "started_monotonic": 1.0},
            {"peer_id": "peer-h0", "peer_name": "H0", "step_id": "H0:0",
             "started_monotonic": 1.0},
        ]
        leader_assembly = self.event(
            "assembled", "peer-x", "", 2.0, 1, group_id="g1", step_id="X:0",
            roster=["peer-x", "peer-h0"], members=members)
        errors = []
        analysis._actual_membership([leader_assembly], attempts, groups, ids, errors)

        self.assertTrue(attempts[0]["membership_success"])
        self.assertFalse(attempts[1]["membership_success"])
        self.assertEqual(attempts[1]["capped_latency_seconds"], 12.0)
        self.assertFalse(groups[0]["follower_corroborated"])
        self.assertFalse(errors)

        follower_assembly = self.event(
            "follower_assembled", "peer-h0", "peer-x", 3.0, 2, group_id="g1",
            step_id="H0:0", roster=["peer-x", "peer-h0"],
            user={"step_id": "H0:0"}, members=members)
        analysis._actual_membership([leader_assembly, follower_assembly], attempts,
                                    groups, ids, errors)
        self.assertTrue(attempts[1]["membership_success"])
        self.assertEqual(attempts[1]["first_assembly_monotonic"], 3.0)
        self.assertEqual(attempts[1]["first_assembly_latency_seconds"], 2.0)
        self.assertTrue(groups[0]["follower_corroborated"])

    def test_retry_requires_both_expirations_in_the_same_native_search(self):
        import analyze_step30_harm as analysis

        ids = {"X": "peer-x", "H0": "peer-h0"}

        def rows(split_x_searches):
            events = []

            def add(kind, peer, other, when, number, **detail):
                event = self.event(kind, peer, other, when, number, **detail)
                events.append(event)
                return f"{event['pid']}:{event['event_number']}"

            x_searches = []
            if split_x_searches:
                spans = [("x-search-old", 1., 3., 10), ("x-search-new", 4., 9., 12)]
            else:
                spans = [("x-search-one", 1., 9., 10)]
            for search_id, start, end, number in spans:
                start_ref = add("search_span_start", "peer-x", "", start, number,
                                step_id="X:0", search_id=search_id)
                end_ref = add("search_span_end", "peer-x", "", end, number + 1,
                              step_id="X:0", search_id=search_id)
                x_searches.append({
                    "peer_id": "peer-x", "step_id": "X:0", "search_id": search_id,
                    "start_event_ref": start_ref, "end_event_ref": end_ref,
                    "start_monotonic": start, "end_monotonic": end,
                })
            h_start = add("search_span_start", "peer-h0", "", 1., 1,
                          step_id="H0:0", search_id="h-search-one")
            h_end = add("search_span_end", "peer-h0", "", 9., 2,
                        step_id="H0:0", search_id="h-search-one")
            honest_search = {"peer_id": "peer-h0", "step_id": "H0:0",
                             "search_id": "h-search-one", "start_event_ref": h_start,
                             "end_event_ref": h_end, "start_monotonic": 1.,
                             "end_monotonic": 9.}
            add("ad_started", "peer-x", "", 2., 20, active=True, expiration=100.)
            add("ad_started", "peer-x", "", 4., 21, active=True, expiration=101.)
            selected_ref = add("native_pop_selected", "peer-h0", "peer-x", 5., 30,
                               step_id="H0:0", search_id="h-search-one",
                               selected_expiration=101., own_expiration=99.,
                               selection_top_wall=90., queue=[],
                               past_attempts=[{"peer": "peer-x", "expiration": 100.}])
            selection = {"event_ref": selected_ref, "peer_id": "peer-h0",
                         "leader_peer_id": "peer-x", "step_id": "H0:0",
                         "search_id": "h-search-one", "selection_valid": True,
                         "selected_expiration": 101., "selection_top_wall": 90.,
                         "past_attempts":[{"peer": "peer-x", "expiration": 100.}],
                         "queue": [], "own_expiration": 99.}
            native = {"searches": [*x_searches, honest_search],
                      "selections": [selection]}
            by_ref = {f"{event['pid']}:{event['event_number']}": event for event in events}
            return native, events, by_ref

        same_native, same_events, same_refs = rows(False)
        same_search = analysis._retry_rows(
            same_native, same_events, same_refs, ids, [], [])[0]
        self.assertEqual(same_search["x_search_id"], "x-search-one")
        self.assertEqual((same_search["old_pair_expiration"],
                          same_search["new_pair_expiration"]), (100., 101.))

        split_native, split_events, split_refs = rows(True)
        self.assertEqual(analysis._retry_rows(
            split_native, split_events, split_refs, ids, [], []), [])

    def test_cutoff_attempts_remain_in_failure_denominator_and_drain_groups_are_separate(self):
        import analyze_step30_harm as analysis

        attempts = [
            {"peer_name": "H0", "peer_id": "peer-h0", "step_id": "H0:0",
             "started_monotonic": .1, "membership_success": False,
             "capped_latency_seconds": 12.0},
            {"peer_name": "H1", "peer_id": "peer-h1", "step_id": "H1:0",
             "started_monotonic": 299.0, "membership_success": True,
             "capped_latency_seconds": 1.0, "first_assembly_latency_seconds": 1.0},
            {"peer_name": "H2", "peer_id": "peer-h2", "step_id": "H2:0",
             "started_monotonic": 300.0, "membership_success": False,
             "capped_latency_seconds": 12.0,
             "late_start_after_submission_cutoff": True},
        ]
        groups = [
            {"group_id": "g-in", "assembly_monotonic": 299.9,
             "leader_name": "H3", "leader_peer_id": "peer-h3"},
            {"group_id": "g-drain", "assembly_monotonic": 300.1,
             "leader_name": "H4", "leader_peer_id": "peer-h4"},
        ]
        config = {"case": {"name": "stock_2s"}, "seed": 3001}
        windows, drain = analysis._window_metrics(attempts, groups, config, 0.0, 300.0)
        metrics = analysis._summary_metrics(
            attempts, groups, [], [], {}, windows, drain,
            {"summary": {"rpc_counts": {}}}, config, 0.0, 300.0, {})

        self.assertEqual(metrics["honest_submitted_attempts"], 3)
        self.assertEqual(metrics["honest_group_failures"], 2)
        self.assertAlmostEqual(metrics["honest_group_failure_fraction"], 2 / 3)
        self.assertAlmostEqual(metrics["honest_mean_capped_wait_seconds"], 25 / 3)
        self.assertEqual(metrics["total_groups"], 2)
        self.assertEqual(metrics["observation_groups"], 1)
        self.assertEqual(metrics["drain_group_count"], 1)
        self.assertEqual(metrics["honest_group_rate_per_minute"], .2)
        self.assertEqual(sum(row["honest_submitted_attempts"] for row in windows), 2)
        self.assertEqual([row["group_event_refs"] for row in windows if row["total_groups"]],
                         [[None]])

    def test_metadata_completion_requires_roster_observations_and_rejects_tensor_events(self):
        import analyze_step30_harm as analysis

        ids = {"X": "peer-x", "H0": "peer-h0"}
        members = {"peer-x": {"step_id": "X:0"}, "peer-h0": {"step_id": "H0:0"}}
        leader = self.event("assembled", "peer-x", "", 2.0, 1, group_id="g1",
                            step_id="X:0", roster=["peer-x", "peer-h0"], members=members)
        x_complete = self.event("control_round_complete", "peer-x", "", 3.0, 2,
                                group_id="g1", step_id="X:0", hook="metadata_only",
                                members=members)
        h_complete = self.event("control_round_complete", "peer-h0", "peer-x", 3.1, 3,
                                group_id="g1", step_id="H0:0", hook="metadata_only",
                                members=members)
        x_result = self.event("step_result", "peer-x", "", 4.0, 4,
                              step_id="X:0", status="completed", members=members,
                              control_only=True, tensor_averaging_performed=False)
        h_result = self.event("step_result", "peer-h0", "", 4.1, 5,
                              step_id="H0:0", status="completed", members=members,
                              control_only=True, tensor_averaging_performed=False)
        config = {"peer_ids": ids}
        native = {"groups": [{"group_id": "g1", "leader_peer_id": "peer-x",
                              "roster": ["peer-x", "peer-h0"]}]}
        errors = []
        result = analysis._completion_integrity(
            [leader, x_complete, h_complete, x_result, h_result], config, native, errors)
        self.assertFalse(errors)
        self.assertEqual(result["g1"]["metadata_completion_count"], 2)

        forbidden = self.event("forbidden_tensor_rpc", "peer-x", "peer-h0", 3.2, 4)
        errors = []
        analysis._completion_integrity(
            [leader, x_complete, h_complete, x_result, h_result, forbidden],
            config, native, errors)
        self.assertTrue(any("forbidden tensor RPC" in error for error in errors))

        aggregate = self.event("allreduce_start", "peer-x", "", 3.2, 5)
        errors = []
        analysis._completion_integrity(
            [leader, x_complete, h_complete, x_result, h_result, aggregate],
            config, native, errors)
        self.assertTrue(any("tensor/aggregate event" in error for error in errors))

    def test_join_deadlines_are_linked_to_the_exact_request_and_leader_step(self):
        import analyze_step30_harm as analysis

        ids = {"X": "peer-x", "H0": "peer-h0"}
        events = []

        def add(kind, peer, other, when, number, **detail):
            event = self.event(kind, peer, other, when, number, **detail)
            events.append(event)
            return event

        selection = add("native_pop_selected", "peer-h0", "peer-x", 9.9, 1,
                        step_id="H0:7", search_id="H0-search-7",
                        selected_expiration=201.0, own_expiration=100.25)
        selection_ref = f"{selection['pid']}:{selection['event_number']}"
        add("join_attempt_start", "peer-h0", "peer-x", 10.0, 2,
            join_id="join-7", step_id="H0:7", search_id="H0-search-7",
            origin="queue", selection_event_ref=selection_ref)
        add("native_client_wait", "peer-h0", "peer-x", 10.05, 3,
            join_id="join-7", step_id="H0:7", phase="handshake", request_expiration=100.75)
        add("native_client_wait", "peer-h0", "peer-x", 10.08, 4,
            join_id="join-7", step_id="H0:7", phase="waiting_for_group",
            request_expiration=100.75)
        add("control_rpc_start", "peer-x", "peer-h0", 10.1, 5,
            rpc_id="rpc-join-7", rpc_type="join", caller_step_id="H0:7",
            request_expiration=100.75, request_group_key=b"task-key")
        add("control_join_admission_state", "peer-x", "peer-h0", 10.11, 6,
            rpc_id="rpc-join-7", request_expiration=100.75,
            leader_expiration=200.75, admission_state="accepted")
        add("unreliable_accept", "peer-x", "peer-h0", 10.12, 7,
            rpc_id="rpc-join-7")
        add("native_leader_wait", "peer-x", "peer-h0", 10.13, 8,
            step_id="X:14", captured_expiration=200.75)
        add("native_client_reply", "peer-h0", "peer-x", 10.2, 9,
            join_id="join-7", step_id="H0:7", code="ACCEPTED")
        add("join_attempt_end", "peer-h0", "peer-x", 10.3, 10,
            join_id="join-7", step_id="H0:7", search_id="H0-search-7",
            origin="queue", selection_event_ref=selection_ref)

        errors = []
        rows = analysis._link_joins(events, {"peer_ids": ids}, {}, errors)
        self.assertFalse(errors)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["server_rpc_link_status"], "unique")
        self.assertEqual(row["native_client_request_expiration"], 100.75)
        self.assertEqual(row["selected_leader_expiration"], 201.0)
        self.assertEqual(row["caller_advertised_expiration"], 100.25)
        self.assertEqual(row["admission_request_expiration"], 100.75)
        self.assertEqual(row["admission_leader_expiration"], 200.75)
        self.assertEqual(row["local_leader_wait_captured_expirations"], [200.75])

        # A second remote RPC with the same caller, step, interval, and exact
        # expiration makes the raw join-to-server association ambiguous.
        duplicate = self.event("control_rpc_start", "peer-x", "peer-h0", 10.15, 11,
                               rpc_id="rpc-duplicate", rpc_type="join",
                               caller_step_id="H0:7", request_expiration=100.75,
                               request_group_key=b"task-key")
        ambiguous = analysis._link_joins([*events, duplicate], {"peer_ids": ids}, {}, [])
        self.assertEqual(ambiguous[0]["server_rpc_link_status"], "ambiguous")
        self.assertIsNone(ambiguous[0]["linked_server_rpc_id"])


class IndependentRawVerifierTests(unittest.TestCase):
    @staticmethod
    def add_event(events, kind, peer, other, when, pid, number, **detail):
        events.append({"kind": kind, "peer": peer, "other": other, "pid": pid,
                       "event_number": number, "monotonic": when, "wall_time": when + 1000,
                       "detail": detail})

    def make_raw_trial(self, root):
        import verify_step30_harm as verifier

        path = root / "stock_2s_s3000"
        path.mkdir()
        ids = {"X": "peer-x", **{f"H{i}": f"peer-h{i}" for i in range(7)}}
        config = {"experiment": "step30", "outcome": "grouping", "case": sweep.make_cases()[0],
                  "seed": 3000, "duration": 20.0, "started_monotonic": 100.0,
                  "network_seed": 3000, "delay_profile": sweep.PROFILE,
                  "control_only": True, "tensor_transfer_enabled": False, "peer_ids": ids}
        write_json(path / "config.json", config)
        events = []
        self.add_event(events, "network_config_installed", "", "", 99.9, 30, 1,
                       network_seed=3000, delay_profile=sweep.PROFILE, identities=ids)
        self.add_event(events, "step_start", "peer-h0", "", 100.0, 10, 5,
                       name="H0", sequence=0, step_id="H0:0")
        self.add_event(events, "step_start", "peer-h1", "", 100.2, 11, 8,
                       name="H1", sequence=0, step_id="H1:0")
        self.add_event(events, "step_start", "peer-h3", "", 100.4, 13, 4,
                       name="H3", sequence=0, step_id="H3:0")
        roster = [ids["H3"], ids["H0"], ids["H1"]]
        members = {ids[name]: {"step_id": f"{name}:0"} for name in ("H3", "H0", "H1")}
        self.add_event(events, "assembled", ids["H3"], "", 101.2, 13, 5,
                       group_id="g1", step_id="H3:0", roster=roster, members=members)
        self.add_event(events, "follower_assembled", ids["H0"], ids["H3"], 102.0, 10, 6,
                       group_id="g1", step_id="H0:0", roster=roster, members=members,
                       user={"step_id": "H0:0"})
        # H1 is named in the leader's roster but never reports a local native
        # assembly, so its attempt must remain a capped failure.
        self.add_event(events, "step_start", "peer-h2", "", 120.0, 12, 2,
                       name="H2", sequence=0, step_id="H2:0")
        self.add_event(events, "assembled", ids["H2"], "", 120.1, 12, 3,
                       group_id="g2", step_id="H2:0", roster=[ids["H2"]],
                       members={ids["H2"]: {"step_id": "H2:0"}})
        self.add_event(events, "trace_closed", "", "", 120.2, 30, 2,
                       participants=["X", *[f"H{i}" for i in range(7)]], pending=[], error=None)
        (path / "events.jsonl").write_text(
            "".join(json.dumps(event, sort_keys=True) + "\n" for event in events),
            encoding="utf-8")
        write_json(path / "summary.json", {"pending": [], "error": None})
        write_json(path / "analysis.json", {"control_only": True})
        raw = verifier.raw_trial(path)
        return path, raw

    def test_raw_verifier_uses_local_follower_receipts_and_keeps_cutoff_and_drain_separate(self):
        import verify_step30_harm as verifier

        with tempfile.TemporaryDirectory(prefix="step30_raw_verify_") as tmp:
            path, raw = self.make_raw_trial(Path(tmp))
            self.assertEqual(raw["errors"], [])
            metrics = raw["summary_metrics"]
            self.assertEqual(metrics["honest_submitted_attempts"], 4)
            self.assertEqual(metrics["honest_group_successes"], 3)
            self.assertEqual(metrics["honest_group_failures"], 1)
            self.assertAlmostEqual(metrics["honest_group_failure_fraction"], .25)
            self.assertEqual(metrics["honest_first_outgoing_choice_counts"],
                             {"X": 0, "other": 0, "none": 4})
            self.assertAlmostEqual(metrics["honest_mean_capped_wait_seconds"], 3.725)
            self.assertEqual(metrics["total_groups"], 2)
            self.assertEqual(metrics["observation_groups"], 1)
            self.assertEqual(metrics["drain_group_count"], 1)
            self.assertEqual(metrics["follower_corroborated_groups"], 1)
            self.assertEqual(raw["windows"][0]["honest_submitted_attempts"], 3)
            self.assertEqual(raw["windows"][0]["total_groups"], 1)
            self.assertFalse(next(row for row in raw["attempts"] if row["peer_name"] == "H1")
                             ["membership_success"])

            matching = dict(experiment="step30", trial_dir=path.name,
                            case=sweep.make_cases()[0], seed=3000, duration=20.0,
                            integrity_pass=True, summary_metrics=metrics,
                            attempts=raw["attempts"], groups=raw["groups"], choices=raw["choices"],
                            joins=raw["joins"],
                            retries=raw["retries"], publications=raw["publications"],
                            windows=raw["windows"])
            self.assertTrue(verifier.verify_trial(path, matching)["pass"])
            matching["summary_metrics"] = dict(metrics, honest_group_successes=4)
            checked = verifier.verify_trial(path, matching)
            self.assertFalse(checked["pass"])
            self.assertTrue(any("honest_group_successes" in error for error in checked["errors"]))
            matching["summary_metrics"] = metrics
            matching["choices"] = [dict(row) for row in raw["choices"]]
            matching["choices"][0]["first_outgoing_choice"] = "X"
            checked = verifier.verify_trial(path, matching)
            self.assertFalse(checked["pass"])
            self.assertTrue(any("choice" in error and "first_outgoing_choice" in error
                                for error in checked["errors"]))

    def test_seeded_delay_audit_rejects_tampered_draw(self):
        import verify_step30_harm as verifier

        ids = {"X": "peer-x", **{f"H{i}": f"peer-h{i}" for i in range(7)}}
        config = {"network_seed": 3000}
        events = []
        self.add_event(events, "network_config_installed", "", "", .9, 30, 1,
                       network_seed=3000, delay_profile=verifier.PROFILE, identities=ids)
        self.add_event(events, "control_rpc_start", ids["H1"], ids["H0"], 1.0, 40, 9,
                       rpc_id="40:rpc9", rpc_type="find", measured=True,
                       source_name="H0", destination_name="H1", occurrence=1,
                       network_seed=3000, delay_seconds=.15)
        expected = verifier._delay_draw(3000, "H0", "H1", "find", "request", 1, 0)
        self.add_event(events, "control_rpc_delay", ids["H1"], ids["H0"], 1.01, 40, 10,
                       rpc_id="40:rpc9", leg="request", response_index=0,
                       requested_delay=expected, actual_delay=expected, cancelled=False)
        errors = []
        verifier._delay_integrity(events, config, ids, errors)
        self.assertEqual(errors, [])

        delay_event = next(event for event in events if event["kind"] == "control_rpc_delay")
        delay_event["detail"]["requested_delay"] += .001
        errors = []
        verifier._delay_integrity(events, config, ids, errors)
        self.assertTrue(any("seeded delay draw differs" in error for error in errors))

    def test_frozen_harness_provenance_detects_a_changed_snapshot(self):
        import verify_step30_harm as verifier

        with tempfile.TemporaryDirectory(prefix="step30_source_verify_") as tmp:
            suite = Path(tmp) / "suite"
            harness = suite / "harness"
            harness.mkdir(parents=True)
            hashes = {}
            for name in verifier.REQUIRED_HARNESS:
                source = HERE / name
                shutil.copyfile(source, harness / name)
                hashes[name] = sweep.sha256(source)
            write_json(suite / "provenance.json", {
                "harness_sha256": hashes,
                "entrypoint_sha256": hashes["step30_harm.py"],
                "script_sha256": hashes["step30_harm.py"],
                "source_sha256": {}, "native_source_snapshot_sha256": {},
                "runtime_files_sha256": {},
            })
            self.assertEqual(verifier._provenance_errors(suite), [])
            with (harness / "step30_harm.py").open("a", encoding="utf-8") as stream:
                stream.write("# changed after freeze\n")
            self.assertTrue(any("frozen source hash mismatch" in error
                                for error in verifier._provenance_errors(suite)))


class OfflineExecutionTests(unittest.TestCase):
    def test_script_entrypoint_dry_run_uses_the_exact_confirm_matrix(self):
        completed = subprocess.run(
            [sys.executable, str(HERE / "step30_harm.py"), "--phase", "confirm", "--dry-run"],
            cwd=Path("/tmp"), check=True, capture_output=True, text=True, timeout=15,
        )
        result = json.loads(completed.stdout)
        self.assertEqual(result["jobs"], sweep.make_jobs("confirm"))
        self.assertEqual(len(result["jobs"]), 6)

    def test_dry_run_does_not_install_runtime_open_sockets_or_create_suite(self):
        original_import = builtins.__import__

        def guarded_import(name, *args, **kwargs):
            if name.split(".", 1)[0] in {"hivemind", "socket", "step30_workload"}:
                raise AssertionError(f"dry-run attempted runtime import: {name}")
            return original_import(name, *args, **kwargs)

        with tempfile.TemporaryDirectory(prefix="step30_dry_run_") as tmp:
            before = set(Path(sweep.ROOT, "results").glob("step30_*"))
            stdout = io.StringIO()
            with patch("builtins.__import__", side_effect=guarded_import), \
                    patch.object(sweep, "install_runtime",
                                 side_effect=AssertionError("runtime install")), \
                    contextlib.redirect_stdout(stdout):
                sweep.main(["--phase", "confirm", "--dry-run"])
            result = json.loads(stdout.getvalue())
            self.assertEqual(result["jobs"], sweep.make_jobs("confirm"))
            self.assertEqual(set(Path(sweep.ROOT, "results").glob("step30_*")), before)
            self.assertEqual(list(Path(tmp).iterdir()), [])

    def test_resume_dry_run_classifies_jobs_and_rejects_evidence_tampering(self):
        with tempfile.TemporaryDirectory(prefix="step30_resume_test_") as tmp:
            suite = Path(tmp) / "suite"
            jobs = make_suite_fixture(suite)
            completed = make_completed_trial(suite, jobs[0])
            interrupted = suite / f"{jobs[1]['case']['name']}_s{jobs[1]['seed']}"
            interrupted.mkdir()
            (interrupted / "events.jsonl").write_text('{"kind":"step_start"}\n', encoding="utf-8")

            output = io.StringIO()
            with patch.object(sweep, "validate_sources"), \
                    patch.object(sweep, "install_runtime",
                                 side_effect=AssertionError("dry-run installed runtime")), \
                    contextlib.redirect_stdout(output):
                sweep.main(["--resume", str(suite), "--dry-run"])
            state = json.loads(output.getvalue())
            self.assertEqual(state["counts"], {"completed": 1, "pending": 0, "interrupted": 1})
            self.assertEqual([row["status"] for row in state["jobs"]], ["completed", "interrupted"])

            with (completed / "events.jsonl").open("a", encoding="utf-8") as stream:
                stream.write('{"kind":"tampered"}\n')
            with patch.object(sweep, "validate_sources"), self.assertRaisesRegex(
                    ValueError, "completed evidence changed"):
                sweep.inspect_suite(suite)

    def test_resume_rejects_drift_from_the_frozen_decision_rule(self):
        with tempfile.TemporaryDirectory(prefix="step30_manifest_test_") as tmp:
            suite = Path(tmp) / "suite"
            make_suite_fixture(suite)
            manifest_path = suite / "suite.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["decision_rule"]["wait_increase_seconds"] += .1
            write_json(manifest_path, manifest)
            with self.assertRaisesRegex(ValueError, "manifest differs from exact Step30 design"):
                sweep.inspect_suite(suite)

    def test_resume_archives_and_reruns_only_the_interrupted_trial(self):
        with tempfile.TemporaryDirectory(prefix="step30_resume_run_") as tmp:
            suite = Path(tmp) / "suite"
            jobs = make_suite_fixture(suite)
            interrupted = suite / f"{jobs[0]['case']['name']}_s{jobs[0]['seed']}"
            interrupted.mkdir()
            (interrupted / "events.jsonl").write_text('{"kind":"step_start"}\n', encoding="utf-8")
            completed = make_completed_trial(suite, jobs[1])
            run_calls = []

            def run_trial(case, seed, path, duration, outcome):
                run_calls.append((case["name"], seed, duration, outcome))
                path.mkdir()
                write_json(path / "config.json", {
                    "case": case, "seed": seed, "duration": duration,
                })
                (path / "events.jsonl").write_text('{"kind":"trace_closed"}\n', encoding="utf-8")
                write_json(path / "summary.json", {"pending": [], "error": None})
                write_json(path / "analysis.json", {"control_only": True})
                return {"error": None}

            analysis_result = {
                "integrity_pass": True, "integrity_errors": [],
                "summary_metrics": {"honest_mean_capped_wait_seconds": 1.0, "total_groups": 0},
            }
            analyzer = types.ModuleType("analyze_step30_harm")
            analyzer.analyse_trial = lambda path: analysis_result
            analyzer.export_suite = lambda path: {"exported": True}
            verifier = types.ModuleType("verify_step30_harm")
            verifier.verify_trial = lambda path, result: {"pass": True, "errors": []}
            verifier.verify_exports = lambda path: {"pass": True, "errors": []}
            observation = SimpleNamespace(sweep=SimpleNamespace(run_trial=run_trial))
            output = io.StringIO()
            with patch.object(sweep, "validate_sources"), \
                    patch.object(sweep, "install_runtime", return_value=observation), \
                    patch.object(sweep.ancestor, "verify_trial_shutdown"), \
                    patch.object(sweep.ancestor, "stop_requested", return_value=False), \
                    patch("multiprocessing.active_children", return_value=[]), \
                    patch.dict(sys.modules, {"analyze_step30_harm": analyzer,
                                             "verify_step30_harm": verifier}), \
                    contextlib.redirect_stdout(output):
                result = sweep.run_suite(suite)

            self.assertEqual(result, {"exported": True})
            self.assertEqual(run_calls, [(jobs[0]["case"]["name"], jobs[0]["seed"],
                                          jobs[0]["duration"], "grouping")])
            self.assertTrue(completed.exists())
            self.assertTrue((interrupted / "completion.json").exists())
            archives = list(suite.glob("archive_*.json"))
            self.assertEqual(len(archives), 1)
            archive = json.loads(archives[0].read_text())
            self.assertTrue(Path(archive["path"]).is_dir())

    def test_smoke_export_merges_metadata_without_duplicate_seed_keyword(self):
        import analyze_step30_harm as analysis

        with tempfile.TemporaryDirectory(prefix="step30_export_test_") as tmp:
            suite = Path(tmp) / "suite"
            jobs = make_suite_fixture(suite)
            for job in jobs:
                trial_dir = f"{job['case']['name']}_s{job['seed']}"
                trial = suite / trial_dir
                trial.mkdir()
                write_json(trial / "control_analysis.json", {
                    "trial_dir": trial_dir, "case": job["case"],
                    "seed": job["seed"], "duration": job["duration"],
                    "integrity_pass": True, "integrity_errors": [],
                    "summary_metrics": {
                        "case_name": job["case"]["name"], "seed": job["seed"],
                        "honest_submitted_attempts": 100,
                        "honest_group_successes": 100,
                        "honest_group_failures": 0,
                        "honest_mean_capped_wait_seconds": 1.0,
                        "total_groups": 20,
                    },
                    "attempts": [], "groups": [], "choices": [], "joins": [],
                    "retries": [], "holds": [], "publications": [], "windows": [], "states": [],
                })
            report = analysis.export_suite(suite)
            self.assertTrue(report["complete"])
            self.assertTrue(report["integrity_pass"])
            with (suite / "trial_summary.csv").open(newline="", encoding="utf-8") as stream:
                rows = list(csv.DictReader(stream))
            self.assertEqual(len(rows), 2)
            self.assertEqual({row["seed"] for row in rows}, {"3000"})
            self.assertEqual({row["publication_policy"] for row in rows}, {"native", "renew"})


class MetadataOnlyCompletionTests(unittest.IsolatedAsyncioTestCase):
    async def test_renewal_loop_serializes_slow_writes_and_recomputes_the_expiration(self):
        clock = {"wall": 100.0, "monotonic": 50.0}
        calls = []

        class Running:
            def __init__(self):
                self.waits = 0

            async def wait(self):
                self.waits += 1
                if self.waits > 2:
                    raise asyncio.CancelledError()

            def is_set(self):
                return True

        class Signal:
            def set(self):
                calls.append(("declared_expiration_set",))

        class KeyManager:
            current_key = "group-key"

            async def declare_averager(self, key, peer, expiration_time,
                                       looking_for_group=True):
                calls.append(("declare_start", expiration_time, looking_for_group,
                              clock["wall"], clock["monotonic"]))
                if looking_for_group:
                    # Simulate a DHT publication occupying 0.2 seconds of the
                    # one-second active-publication interval.
                    clock["wall"] += .2
                    clock["monotonic"] += .2
                    calls.append(("declare_end", expiration_time, clock["wall"],
                                  clock["monotonic"]))

            async def update_key_on_not_enough_peers(self):
                calls.append(("update_key",))

        async def simulated_sleep(delay):
            calls.append(("sleep", delay))
            clock["wall"] += delay
            clock["monotonic"] += delay

        observation = SimpleNamespace(base=SimpleNamespace(
            get_dht_time=lambda: clock["wall"]))
        old_obs = sweep.OBS
        sweep.OBS = observation
        obj = SimpleNamespace(
            lock_declare=asyncio.Lock(), running=Running(), min_matchmaking_time=2.,
            search_end_time=110., declared_group_key=None,
            declared_expiration_time=float("inf"), declared_expiration=Signal(),
            leader_queue=[], max_assured_time=float("-inf"), peer_id="X",
        )
        old_time = sweep.time
        sweep.time = SimpleNamespace(monotonic=lambda: clock["monotonic"])
        try:
            with patch.object(sweep.asyncio, "sleep", side_effect=simulated_sleep):
                with self.assertRaises(asyncio.CancelledError):
                    await sweep.renewed_native_publisher(
                        obj, SimpleNamespace(scheduled_time=100.), KeyManager())
        finally:
            sweep.OBS, sweep.time = old_obs, old_time

        starts = [row for row in calls if row[0] == "declare_start" and row[2]]
        self.assertEqual([row[1] for row in starts], [102., 103.])
        self.assertAlmostEqual(starts[1][3] - starts[0][3], 1.)
        self.assertAlmostEqual(starts[1][4] - starts[0][4], 1.)
        sleeps = [row[1] for row in calls if row[0] == "sleep"]
        self.assertEqual(len(sleeps), 2)
        for delay in sleeps:
            self.assertAlmostEqual(delay, .8)
        # Each publication fully completes before its following cadence wait.
        starts_idx = [i for i, row in enumerate(calls)
                      if row[0] == "declare_start" and row[2]]
        ends_idx = [i for i, row in enumerate(calls) if row[0] == "declare_end"]
        sleep_idx = [i for i, row in enumerate(calls) if row[0] == "sleep"]
        self.assertLess(ends_idx[0], sleep_idx[0])
        self.assertLess(sleep_idx[0], starts_idx[1])
        self.assertLess(ends_idx[1], sleep_idx[1])

    async def test_native_gathered_member_metadata_completes_without_tensor_work(self):
        calls = []
        data = {
            "X": {"step_id": "x-step"},
            "H0": {"step_id": "h-step"},
        }

        class FakeSerializer:
            def loads(self, value):
                calls.append(("loads", value))
                # The native gathered payload is outer-serialized and its third
                # tuple field contains the member metadata serialization.
                if isinstance(value, bytes):
                    if value.startswith(b"outer:"):
                        return (None, None, value[len(b"outer:"):])
                    return json.loads(value.decode())
                return value

        class Future:
            def __init__(self):
                self.result = "pending"

            def done(self):
                return self.result != "pending"

            def set_result(self, value):
                self.result = value

        group_id = b"g1"
        future = Future()
        observation = SimpleNamespace(
            base=None,
            sweep=SimpleNamespace(user_data=lambda step: {"step_id": step}),
            emit=lambda kind, peer, **detail: calls.append((kind, peer, detail)),
        )
        old_obs = sweep.OBS
        sweep.OBS = observation
        try:
            obj = SimpleNamespace(
                peer_id="X", serializer=FakeSerializer(),
                _running_groups={group_id: future},
                _matchmaking=SimpleNamespace(step_control="x-step"),
            )
            gathered = [
                b"outer:" + json.dumps(data["X"]).encode(),
                b"outer:" + json.dumps(data["H0"]).encode(),
            ]
            info = SimpleNamespace(peer_ids=["X", "H0"], gathered=gathered, group_id=group_id)
            result = await sweep.control_round_completion(obj, info)
        finally:
            sweep.OBS = old_obs

        self.assertEqual(result, data)
        self.assertEqual(future.result, None)
        event = next(row for row in calls if isinstance(row, tuple) and row[0] ==
                     "control_round_complete")
        self.assertEqual(event[2]["hook"], "metadata_only")
        self.assertEqual(event[2]["members"], data)
        self.assertEqual(len([row for row in calls if row[0] == "loads"]), 4)

    async def test_metadata_completion_falls_back_to_member_step_and_tolerates_cleaned_future(self):
        calls = []

        class Serializer:
            def loads(self, value):
                if isinstance(value, bytes) and value.startswith(b"outer:"):
                    return (None, None, value[len(b"outer:"):])
                if isinstance(value, bytes):
                    return json.loads(value.decode())
                return value

        observation = SimpleNamespace(
            base=None, sweep=SimpleNamespace(user_data=lambda step: {"step_id": step}),
            emit=lambda kind, peer, **detail: calls.append((kind, peer, detail)),
        )
        old_obs = sweep.OBS
        sweep.OBS = observation
        try:
            obj = SimpleNamespace(
                peer_id="H0", serializer=Serializer(), _running_groups={},
                _matchmaking=SimpleNamespace(step_control=None),
            )
            members = {"H0": {"step_id": "from-member"}}
            info = SimpleNamespace(
                peer_ids=["H0"],
                gathered=[b"outer:" + json.dumps(members["H0"]).encode()],
                group_id=b"g2",
            )
            await sweep.control_round_completion(obj, info)
        finally:
            sweep.OBS = old_obs
        event = next(row for row in calls if row[0] == "control_round_complete")
        self.assertEqual(event[2]["step_id"], "from-member")


if __name__ == "__main__":
    unittest.main()
