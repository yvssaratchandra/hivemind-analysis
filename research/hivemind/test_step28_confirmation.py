"""Offline contracts for the Step 28 confirmation controller and analysis."""
from __future__ import annotations

import builtins
import contextlib
import io
import json
import random
from pathlib import Path
import shutil
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

import step28_confirmation as sweep
import analyze_step28_confirmation as analysis
import verify_step28_confirmation as verifier


def write_json(path: Path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")


def valid_config(job):
    rng = random.Random(job["seed"])
    honest = [rng.uniform(0.0, 3.0) for _ in range(7)]
    offsets = dict(X=rng.uniform(0.0, 3.0), **{f"H{i}": value for i, value in enumerate(honest)})
    delay = job["case"]["one_way_delay_seconds"]
    return dict(
        case=job["case"], seed=job["seed"], duration=job["duration"],
        experiment="step28", outcome="grouping", instrumentation="detailed",
        timeout=12, request_timeout=3, tensor_size=128, honest_min=2, target=4,
        honest_ad_lifetime=5, allreduce_timeout=6, store_delay=0, group_bits="",
        allow_state_sharing=False, scheduled_time="native default", training_delay=sweep.TRAINING,
        network="eight-peer loopback", attempt_unit="step_start", event_transport="batched",
        trace_batch_size=32, duplicate_read_views=False, cached_reads_delayed=False,
        tensor_transfer_delayed=False, one_way_delay_seconds=delay,
        network_delay_injected=delay > 0,
        peer_ids={name: f"peer-{i}" for i, name in enumerate(("X", *(f"H{i}" for i in range(7))))},
        initial_offsets=offsets,
        response_policy=job["case"]["response"],
        leadership_latency_origin="step_start", harm_timeout_cap=12,
        once_unit="one active publication per native SEARCH; withdrawals separate",
        clock_monitor=True, clock_jump_limit_seconds=sweep.CLOCK_JUMP_LIMIT,
        submission_cutoff_checked_per_peer=True,
        started_monotonic=100.0, started_wall=1100.0,
    )


def write_completed_trial(suite: Path, job, *, all_peers_shutdown=True):
    trial = suite / sweep.trial_name(job)
    trial.mkdir(parents=True, exist_ok=True)
    write_json(trial / "config.json", valid_config(job))
    (trial / "events.jsonl").write_text('{"kind":"trace_closed"}\n', encoding="utf-8")
    write_json(trial / "summary.json", {"pending": [], "error": None})
    write_json(trial / "analysis.json", {"ok": True})
    write_json(trial / "control_analysis.json", {"integrity_pass": True})
    tracked = (*sweep.RAW_FILES, "analysis.json", "control_analysis.json")
    write_json(trial / "completion.json", {
        "file_sha256": {name: sweep.sha256(trial / name) for name in tracked},
        "all_peers_shutdown": all_peers_shutdown,
    })
    return trial


def make_suite(path: Path, phase="smoke"):
    jobs = sweep.make_jobs(phase)
    write_json(path / "suite.json", {
        "experiment": "step28", "phase": phase, "jobs": jobs,
        "training_delay": sweep.TRAINING, "order_seed": 280028,
        "outcome": "grouping", "instrumentation": "detailed", "preliminary": True,
    })
    write_json(path / "provenance.json", {})
    return jobs


class DesignTests(unittest.TestCase):
    def test_case_matrix_keeps_the_two_reliable_publication_policies(self):
        cases = sweep.make_cases()
        self.assertEqual([case["name"] for case in cases], ["reliable_single", "reliable_renewed"])
        self.assertEqual({(case["mode"], case["response"]) for case in cases},
                         {("once", "reliable"), ("refresh", "reliable")})
        self.assertTrue(all(case["minimum"] == case["target"] == 4 for case in cases))
        self.assertTrue(all(case["one_way_delay_seconds"] == 0.15 for case in cases))
        self.assertEqual(cases[0]["lifetime"], 2.0)
        self.assertEqual((cases[1]["frequency"], cases[1]["nominal_lower"],
                          cases[1]["nominal_upper"]), (2.0, 1.5, 2.0))

    def test_smoke_and_confirmation_matrices_have_exact_seeds_durations_and_order(self):
        smoke = sweep.make_jobs("smoke")
        self.assertEqual([(job["seed"], job["duration"], job["case"]["name"]) for job in smoke], [
            (2800, 20, "reliable_single"), (2800, 20, "reliable_renewed"),
        ])
        confirm = sweep.make_jobs("confirm")
        self.assertEqual([(job["seed"], job["duration"], job["case"]["name"]) for job in confirm], [
            (2801, 120, "reliable_single"), (2801, 120, "reliable_renewed"),
            (2802, 120, "reliable_renewed"), (2802, 120, "reliable_single"),
            (2803, 120, "reliable_single"), (2803, 120, "reliable_renewed"),
        ])
        self.assertEqual(confirm, sweep.make_jobs("confirm"))
        self.assertEqual(analysis.canonical_jobs("smoke"), smoke)
        self.assertEqual(analysis.canonical_jobs("confirm"), confirm)

    def test_old_once_publication_unit_and_clock_monitor_are_locked_in_trial_config(self):
        job = sweep.make_jobs("smoke")[0]
        config = valid_config(job)
        sweep.validate_trial_config(config, job)
        changed = dict(config, once_unit="republish on expiry")
        with self.assertRaisesRegex(ValueError, "once_unit"):
            sweep.validate_trial_config(changed, job)

    def test_controller_clock_audit_accepts_stable_offsets_and_detects_either_clock_jump(self):
        stable = [
            {"clock_wall": 1000.0, "clock_monotonic": 100.0, "clock_boottime": 200.0},
            {"clock_wall": 1000.1, "clock_monotonic": 100.1, "clock_boottime": 200.1},
        ]
        self.assertEqual(sweep.check_clock(stable), [])
        wall_jump = [*stable, {"clock_wall": 1000.5, "clock_monotonic": 100.2,
                              "clock_boottime": 200.2}]
        self.assertTrue(any("clock_wall" in error for error in sweep.check_clock(wall_jump)))
        boot_jump = [*stable, {"clock_wall": 1000.2, "clock_monotonic": 100.2,
                              "clock_boottime": 200.6}]
        self.assertTrue(any("clock_boottime" in error for error in sweep.check_clock(boot_jump)))


class OfflineControllerTests(unittest.TestCase):
    def test_fresh_dry_run_does_not_import_runtime_or_open_network(self):
        original_import = builtins.__import__

        def guarded_import(name, *args, **kwargs):
            if name.split(".", 1)[0] in {
                "hivemind", "socket", "step25_choice_measurement", "step28_workload",
            }:
                raise AssertionError(f"offline dry-run attempted runtime import: {name}")
            return original_import(name, *args, **kwargs)

        stdout = io.StringIO()
        with patch("builtins.__import__", side_effect=guarded_import), \
             patch.object(sweep, "install_runtime", side_effect=AssertionError("runtime install")), \
             contextlib.redirect_stdout(stdout):
            sweep.main(["--phase", "confirm", "--dry-run"])
        result = json.loads(stdout.getvalue())
        self.assertEqual(result["experiment"], "step28")
        self.assertEqual(result["jobs"], sweep.make_jobs("confirm"))

    def test_resume_dry_run_reports_completed_trials_without_installing_runtime(self):
        with tempfile.TemporaryDirectory(prefix="step28_resume_test_") as directory:
            suite = Path(directory) / "suite"
            suite.mkdir()
            jobs = make_suite(suite)
            for job in jobs:
                write_completed_trial(suite, job)
            original_import = builtins.__import__

            def guarded_import(name, *args, **kwargs):
                if name.split(".", 1)[0] in {
                    "hivemind", "socket", "step25_choice_measurement", "step28_workload",
                }:
                    raise AssertionError(f"resume dry-run attempted runtime import: {name}")
                return original_import(name, *args, **kwargs)

            with patch("builtins.__import__", side_effect=guarded_import), \
                 patch.object(sweep, "validate_sources"), \
                 patch.object(sweep, "install_runtime", side_effect=AssertionError("runtime install")), \
                 contextlib.redirect_stdout(io.StringIO()) as stdout:
                sweep.main(["--resume", str(suite), "--dry-run"])
            result = json.loads(stdout.getvalue())
            self.assertEqual(result["counts"]["completed"], len(jobs))
            self.assertEqual(result["counts"]["blocked"], 0)

    def test_completed_resume_blocks_corrupted_raw_hash(self):
        with tempfile.TemporaryDirectory(prefix="step28_hash_test_") as directory:
            suite = Path(directory) / "suite"
            suite.mkdir()
            jobs = make_suite(suite)
            trial = write_completed_trial(suite, jobs[0])
            state = sweep.inspect_suite(suite, verify_sources=False)
            write_json(suite / "checkpoint.json", {
                "suite_sha256": state["suite_sha256"],
                "provenance_sha256": state["provenance_sha256"],
                "jobs": state["jobs"],
            })
            with (trial / "events.jsonl").open("a", encoding="utf-8") as stream:
                stream.write('{"kind":"tampered"}\n')
            with self.assertRaisesRegex(ValueError, "completed evidence changed"):
                sweep.inspect_suite(suite, verify_sources=False)

    def test_interrupted_archive_preserves_hashes_and_resume_detects_archive_drift(self):
        with tempfile.TemporaryDirectory(prefix="step28_archive_test_") as directory:
            root = Path(directory)
            suite = root / "suite"
            suite.mkdir()
            jobs = make_suite(suite)
            trial = suite / sweep.trial_name(jobs[0])
            trial.mkdir()
            (trial / "events.jsonl").write_text('{"kind":"partial"}\n', encoding="utf-8")
            write_json(trial / "summary.json", {
                "pending": ["H0"],
                "error": "ClockDiscontinuity: clock discontinuity; archive and rerun this trial on resume",
            })
            write_json(suite / "halt.json", {
                "trial": trial.name, "interrupted": True, "all_peers_shutdown": True,
                "error": "ClockDiscontinuity: clock discontinuity; archive and rerun this trial on resume",
            })
            state = sweep.inspect_suite(suite, verify_sources=False)
            row = next(row for row in state["jobs"] if row["trial_dir"] == trial.name)
            self.assertEqual(row["status"], "interrupted")
            archive = sweep.archive_interrupted(suite, row["trial_dir"])
            archived = Path(archive["path"])
            self.assertFalse(trial.exists())
            for name, digest in archive["file_sha256"].items():
                self.assertEqual(sweep.sha256(archived / name), digest)
            row["archives"] = [archive]
            write_json(suite / "checkpoint.json", {
                "suite_sha256": state["suite_sha256"],
                "provenance_sha256": state["provenance_sha256"],
                "jobs": state["jobs"],
            })
            (archived / "events.jsonl").write_text("changed\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "interrupted archive drift"):
                sweep.inspect_suite(suite, verify_sources=False)


class AnalyzerAttemptTests(unittest.TestCase):
    PEER_IDS = {name: f"peer-{index}" for index, name in
                enumerate(("X", *(f"H{i}" for i in range(7))))}

    @classmethod
    def attempt(cls, step_id, name, started, outcome="failed"):
        return dict(step_id=step_id, peer_id=cls.PEER_IDS[name], peer_name=name,
                    started_monotonic=started, started_wall=started + 1000.0,
                    start_event_ref=f"{name}:{step_id}", assembled_group_ids=[],
                    eventual_outcome=outcome, search_count=1, first_search_event_ref=None)

    @classmethod
    def group(cls, group_id, leader_name, leader_step, member_steps, assembled):
        roster = [cls.PEER_IDS[name] for name in member_steps]
        return dict(group_id=group_id, leader_name=leader_name,
                    leader_peer_id=cls.PEER_IDS[leader_name], leader_step_id=leader_step,
                    roster=roster,
                    member_step_ids={cls.PEER_IDS[name]: step_id
                                     for name, step_id in member_steps.items()},
                    assembly_event_ref=f"event:{group_id}",
                    assembly_event_refs=[f"event:{group_id}"],
                    assembly_monotonic=assembled, assembly_wall=assembled + 1000.0,
                    roster_size=len(roster), x_in_roster=cls.PEER_IDS["X"] in roster,
                    within_submission_window=True,
                    honest_membership_count=sum(name.startswith("H") for name in member_steps),
                    honest_peer_names=[name for name in member_steps if name.startswith("H")])

    def test_lead_cdf_uses_all_attempts_as_denominator_but_excludes_late_or_missing(self):
        attempts = [
            self.attempt("x-2", "X", 100.0), self.attempt("x-3", "X", 100.0),
            self.attempt("x-5", "X", 100.0), self.attempt("x-late", "X", 100.0),
            self.attempt("x-missing", "X", 100.0),
        ]
        groups = [
            self.group("g2", "X", "x-2", {"X": "x-2"}, 101.5),
            self.group("g2-extra", "X", "x-2", {"X": "x-2"}, 101.6),
            self.group("g3", "X", "x-3", {"X": "x-3"}, 102.5),
            self.group("g5", "X", "x-5", {"X": "x-5"}, 104.0),
            self.group("g-late", "X", "x-late", {"X": "x-late"}, 113.0),
        ]
        config = {"started_monotonic": 100.0, "duration": 120.0}
        errors = []
        rows = analysis._attempt_rows(
            {"attempts": attempts, "joins": []}, groups, config, self.PEER_IDS, [], {}, errors)
        self.assertEqual(errors, [])
        by_step = {row["step_id"]: row for row in rows}
        self.assertEqual(by_step["x-late"]["lead_latency_seconds"], 13.0)
        self.assertEqual(by_step["x-late"]["lead_capped_latency_seconds"], 12.0)
        self.assertIsNone(by_step["x-missing"]["lead_latency_seconds"])
        self.assertEqual(by_step["x-missing"]["lead_capped_latency_seconds"], 12.0)

        metrics = analysis._trial_metrics(
            {"attempts": attempts, "joins": [], "selections": []}, rows, groups, [], [],
            config, self.PEER_IDS)
        self.assertEqual(metrics["x_submitted_attempts"], 5)
        self.assertEqual(metrics["x_led_attempts"], 4)
        self.assertEqual(metrics["x_led_by_2s_count"], 1)
        self.assertEqual(metrics["x_led_by_3s_count"], 2)
        self.assertEqual(metrics["x_led_by_5s_count"], 3)
        self.assertEqual(metrics["x_led_by_12s_count"], 3)
        self.assertEqual([metrics[f"x_led_by_{n}s_rate"] for n in (2, 3, 5, 12)],
                         [0.2, 0.4, 0.6, 0.6])
        self.assertEqual(metrics["x_lead_latency_cdf_denominator"], 5)
        self.assertAlmostEqual(metrics["x_mean_capped_latency_seconds"], 6.4)

    def test_first_choice_redirect_retries_and_first_assembly_are_scoped_to_one_step(self):
        native_attempts = [
            self.attempt("x-1", "X", 100.0),
            self.attempt("h0-old", "H0", 101.0),
            self.attempt("h0-current", "H0", 102.0),
            self.attempt("h1-current", "H1", 102.5),
            self.attempt("h2-current", "H2", 103.0),
            self.attempt("h3-old", "H3", 103.5),
            self.attempt("h3-current", "H3", 104.0),
            self.attempt("h4-current", "H4", 104.5),
        ]
        groups = [
            self.group("g-x", "X", "x-1", {"X": "x-1", "H0": "h0-old"}, 105.0),
            self.group("g-h1", "H1", "h1-current",
                       {"H1": "h1-current", "H0": "h0-current"}, 106.0),
            self.group("g-h4", "H4", "h4-current",
                       {"H4": "h4-current", "H3": "h3-old"}, 107.0),
        ]

        raw_events = []
        joins = []

        def add_join(join_id, caller, leader, step, start, origin, accepted=False):
            event_refs = []
            if accepted:
                ref = f"30:{len(raw_events) + 1}"
                raw_events.append(dict(
                    pid=30, event_number=len(raw_events) + 1, kind="native_client_reply",
                    peer=self.PEER_IDS[caller], other=self.PEER_IDS[leader], monotonic=start + 0.02,
                    detail=dict(join_id=join_id, step_id=step, code="ACCEPTED",
                                request_expiration=500.0 + len(raw_events))))
                event_refs.append(ref)
            joins.append(dict(
                join_id=join_id, peer_id=self.PEER_IDS[caller], peer_name=caller,
                leader_peer_id=self.PEER_IDS[leader], leader_name=leader, step_id=step,
                search_id=f"search-{step}", origin=origin,
                selection_event_ref=None, start_event_ref=f"31:{len(joins) + 1}",
                end_event_ref=None, start_monotonic=start, selected_expiration=900.0,
                native_client_records=[dict(event_ref=ref) for ref in event_refs],
            ))

        add_join("h0-j1", "H0", "X", "h0-old", 101.1, "queue", accepted=True)
        # A redirect and retry belong to the same H0 step, so neither creates another attempt.
        add_join("h0-j2", "H0", "H1", "h0-old", 101.2, "redirect", accepted=True)
        add_join("h0-j3", "H0", "H1", "h0-old", 101.3, "queue", accepted=False)
        add_join("h0-current-j1", "H0", "H1", "h0-current", 102.1, "queue", accepted=True)
        add_join("h3-current-j1", "H3", "H2", "h3-current", 104.1, "redirect", accepted=False)

        native = {"attempts": native_attempts, "joins": joins, "selections": []}
        config = {"started_monotonic": 100.0, "duration": 120.0}
        errors = []
        attempts = analysis._attempt_rows(native, groups, config, self.PEER_IDS, [], {}, errors)
        by_step = {row["step_id"]: row for row in attempts}
        exported_joins = analysis._annotate_attempt_choices(
            attempts, native, groups, self.PEER_IDS,
            {f"{event['pid']}:{event['event_number']}": event for event in raw_events})

        self.assertEqual(errors, [])
        self.assertEqual(len(attempts), len(native_attempts))
        self.assertEqual(len(exported_joins), len(joins))
        self.assertEqual(by_step["h0-old"]["first_outgoing_join_id"], "h0-j1")
        self.assertEqual(by_step["h0-old"]["first_outgoing_join_destination"], "X")
        self.assertTrue(by_step["h0-old"]["ever_tried_x"])
        self.assertEqual(by_step["h0-old"]["accepted_x_reply_count"], 1)
        self.assertEqual(by_step["h0-old"]["accepted_other_reply_count"], 1)
        self.assertEqual(by_step["h3-current"]["first_outgoing_join_destination"], "other")
        self.assertEqual(by_step["h3-current"]["first_outgoing_join_origin"], "redirect")
        self.assertEqual(by_step["h1-current"]["first_outgoing_join_destination"], "none")
        self.assertEqual(by_step["h0-old"]["first_group_leader_category"], "X")
        self.assertEqual(by_step["h0-current"]["first_group_leader_category"], "other")
        self.assertEqual(by_step["h1-current"]["first_group_leader_category"], "other")
        self.assertEqual(by_step["h2-current"]["first_group_leader_category"], "no_group")
        self.assertEqual(by_step["h3-old"]["first_group_leader_category"], "other")
        self.assertEqual(by_step["h3-current"]["first_group_leader_category"], "no_group")
        self.assertEqual(by_step["h4-current"]["first_group_leader_category"], "other")

        metrics = analysis._trial_metrics(native, attempts, groups, [], [], config, self.PEER_IDS)
        self.assertEqual(metrics["honest_first_group_x_led_attempts"], 1)
        self.assertEqual(metrics["honest_first_group_other_led_attempts"], 4)
        self.assertEqual(metrics["honest_no_group_attempts"], 2)
        self.assertEqual(metrics["honest_first_choice_x_attempts"], 1)
        self.assertEqual(metrics["honest_first_choice_other_attempts"], 2)
        self.assertEqual(metrics["honest_no_outgoing_choice_attempts"], 4)
        self.assertEqual(metrics["honest_ever_tried_x_attempts"], 1)
        self.assertEqual(metrics["honest_accepted_x_reply_count"], 1)
        self.assertEqual(metrics["honest_accepted_other_reply_count"], 2)


class ReceiptLinkageTests(unittest.TestCase):
    def test_disband_receipt_uses_request_expiration_and_exact_join_id(self):
        ids = {name: f"peer-{i}" for i, name in enumerate(("X", *(f"H{i}" for i in range(7))))}
        visit_detail = dict(visit_id="v-1", rpc_id="rpc-1", leader_step_id="x-step",
                            leader_search_id="x-search", request_expiration=500.0, delta=0.5)
        events = [
            dict(pid=1, event_number=1, kind="unreliable_accept", peer=ids["X"],
                 other=ids["H0"], monotonic=1.0, detail=dict(visit_detail)),
            dict(pid=1, event_number=2, kind="unreliable_hold_start", peer=ids["X"],
                 other=ids["H0"], monotonic=1.1, detail=dict(visit_detail)),
            dict(pid=2, event_number=1, kind="native_client_reply", peer=ids["H0"],
                 other=ids["X"], monotonic=1.2,
                 detail=dict(join_id="join-7", step_id="h0-step", code="ACCEPTED",
                             request_expiration=500.0)),
            dict(pid=1, event_number=3, kind="unreliable_disband", peer=ids["X"],
                 other=ids["H0"], monotonic=1.7, detail=dict(visit_detail)),
            dict(pid=2, event_number=2, kind="native_client_reply", peer=ids["H0"],
                 other=ids["X"], monotonic=1.8,
                 detail=dict(join_id="join-7", step_id="h0-step", code="GROUP_DISBANDED",
                             request_expiration=500.0)),
            dict(pid=1, event_number=4, kind="unreliable_release", peer=ids["X"],
                 other=ids["H0"], monotonic=1.9, detail=dict(visit_detail)),
        ]
        native = {"joins": [dict(
            join_id="join-7", peer_id=ids["H0"], leader_peer_id=ids["X"],
            start_monotonic=1.1, end_monotonic=1.9,
            selected_expiration=900.0,
            native_client_records=[dict(event_ref="2:1"), dict(event_ref="2:2")],
        )]}
        by_ref = {f"{event['pid']}:{event['event_number']}": event for event in events}
        errors = []
        holds = analysis._hold_rows(
            events, ids, {"response": "unreliable", "hold_seconds": 0.5},
            errors, native=native, by_ref=by_ref)

        self.assertEqual(errors, [])
        self.assertEqual(len(holds), 1)
        self.assertTrue(holds[0]["disband_received_by_native_client"])
        self.assertEqual(holds[0]["disband_native_client_join_id"], "join-7")
        self.assertEqual(holds[0]["disband_native_client_request_expiration"], 500.0)
        self.assertEqual(holds[0]["disband_native_client_accepted_reply_event_ref"], "2:1")
        self.assertEqual(holds[0]["disband_native_client_reply_event_ref"], "2:2")


class IndependentVerifierTests(unittest.TestCase):
    def test_raw_verifier_reconstructs_failure_inclusive_cdf_and_honest_choice(self):
        with tempfile.TemporaryDirectory(prefix="step28_raw_verify_test_") as directory:
            trial = Path(directory) / "trial"
            trial.mkdir()
            peer_ids = {name: f"peer-{i}" for i, name in
                        enumerate(("X", *(f"H{i}" for i in range(7))))}
            config = dict(
                case=dict(name="reliable_single", mode="once", response="reliable",
                          lifetime=2.0, hold_seconds=0.0, minimum=4, target=4,
                          one_way_delay_seconds=0.15),
                seed=2801, duration=120, started_monotonic=100.0,
                peer_ids=peer_ids,
            )
            write_json(trial / "config.json", config)
            events = []

            def add(kind, monotonic, peer="", other="", **detail):
                event = dict(kind=kind, peer=peer, other=other, pid=7,
                             event_number=len(events) + 1, monotonic=monotonic,
                             wall_time=1000.0 + monotonic, detail=detail)
                events.append(event)
                return event

            starts = {}

            def start(name, step_id, sequence, at, value):
                details = dict(name=name, sequence=sequence, step_id=step_id, value=value)
                starts[(name, step_id)] = details
                add("step_start", at, peer=peer_ids[name], **details)

            def result(name, step_id, sequence, at, value):
                add("step_result", at, peer=peer_ids[name], name=name, sequence=sequence,
                    step_id=step_id, finished=at, status="ok", value=value)

            start("H0", "h0-1", 1, 99.5, 0.0)
            add("join_attempt_start", 100.0, peer=peer_ids["H0"], other=peer_ids["X"],
                step_id="h0-1", search_id="h0-search", join_id="h0-j1", origin="redirect",
                selection_event_ref=None)
            add("native_client_reply", 100.1, peer=peer_ids["H0"], other=peer_ids["X"],
                step_id="h0-1", search_id="h0-search", join_id="h0-j1", code="ACCEPTED",
                request_expiration=500.0)
            add("join_attempt_end", 100.2, peer=peer_ids["H0"], other=peer_ids["X"],
                step_id="h0-1", search_id="h0-search", join_id="h0-j1", origin="redirect",
                selection_event_ref=None, error=None)
            start("X", "x-1", 1, 100.5, 7.0)
            start("H1", "h1-1", 1, 101.0, 1.0)
            start("X", "x-2", 2, 102.0, 8.0)
            start("X", "x-3", 3, 103.0, 9.0)

            members_g1 = {peer_ids["X"]: starts[("X", "x-1")],
                          peer_ids["H0"]: starts[("H0", "h0-1")]}
            add("assembled", 104.5, peer=peer_ids["X"], group_id="g-x1", step_id="x-1",
                roster=list(members_g1), members=members_g1)
            members_g2 = {peer_ids["X"]: starts[("X", "x-3")],
                          peer_ids["H1"]: starts[("H1", "h1-1")]}
            add("assembled", 116.0, peer=peer_ids["X"], group_id="g-x3", step_id="x-3",
                roster=list(members_g2), members=members_g2)

            result("H0", "h0-1", 1, 117.0, 0.0)
            result("X", "x-1", 1, 117.1, 7.0)
            result("H1", "h1-1", 1, 117.2, 1.0)
            result("X", "x-2", 2, 117.3, 8.0)
            result("X", "x-3", 3, 117.4, 9.0)
            add("trace_closed", 118.0, participants=list(peer_ids), pending=[], error=None)
            (trial / "events.jsonl").write_text(
                "".join(json.dumps(event, allow_nan=False) + "\n" for event in events),
                encoding="utf-8")

            raw = verifier.raw_trial(trial)
            self.assertEqual(raw["errors"], [])
            metrics = raw["metrics"]
            self.assertEqual(metrics["x_submitted_attempts"], 3)
            self.assertEqual(metrics["x_led_by_2s_count"], 0)
            self.assertEqual(metrics["x_led_by_3s_count"], 0)
            self.assertEqual(metrics["x_led_by_5s_count"], 1)
            self.assertEqual(metrics["x_led_by_12s_count"], 1)
            self.assertEqual(raw["attempt_choice"][(peer_ids["H0"], "h0-1")]["destination"], "X")
            self.assertTrue(raw["attempt_choice"][(peer_ids["H0"], "h0-1")]["ever_tried_x"])
            self.assertEqual(metrics["honest_first_choice_x_attempts"], 1)
            self.assertEqual(metrics["honest_accepted_x_reply_count"], 1)


class ExportSignalTests(unittest.TestCase):
    @staticmethod
    def run_mocked_confirm_export(root: Path, x_attempts: int):
        suite = root / "suite"
        suite.mkdir(parents=True)
        jobs = sweep.make_jobs("confirm")
        write_json(suite / "suite.json", {
            "experiment": "step28", "phase": "confirm", "jobs": jobs,
            "training_delay": sweep.TRAINING, "order_seed": 280028,
            "outcome": "grouping", "instrumentation": "detailed", "preliminary": True,
        })
        trial_rows = {}
        for job in jobs:
            trial_dir = sweep.trial_name(job)
            trial_path = suite / trial_dir
            trial_path.mkdir()
            (trial_path / "config.json").write_text("{}\n", encoding="utf-8")
            (trial_path / "events.jsonl").write_text("{}\n", encoding="utf-8")
            name = job["case"]["name"]
            renewed = name == "reliable_renewed"
            led_count = 4 if renewed else 2
            early_count = 1 if renewed else 2
            metrics = dict(
                x_submitted_attempts=x_attempts,
                x_led_attempts=led_count,
                x_led_rate=led_count / x_attempts,
                x_led_by_2s_count=early_count,
                x_led_by_2s_rate=early_count / x_attempts,
                x_mean_capped_latency_seconds=5.0,
                honest_submitted_attempts=40,
                honest_first_group_success_fraction=0.7,
                honest_first_group_x_led_attempts=8,
                honest_first_group_x_led_rate=0.2,
                honest_group_memberships_in_submission_window=80,
                honest_mean_capped_latency_seconds=4.0,
                honest_success_only_mean_first_group_latency_seconds=3.5,
                honest_first_choice_x_attempts=4,
                honest_ever_tried_x_attempts=7,
                honest_accepted_x_reply_count=5,
            )
            trial_rows[trial_dir] = dict(
                experiment="step28", trial_dir=trial_dir, case=job["case"],
                seed=job["seed"], duration=job["duration"], integrity_pass=True,
                integrity_errors=[], summary_metrics=metrics, attempts=[], groups=[],
                choices=[], joins=[], retries=[], holds=[], policy_integrity={"pass_": True},
                delay_integrity={"pass_": True, "errors": [], "summary": {"coverage": {}}},
            )

        def fake_analyse(path):
            return trial_rows[Path(path).name]

        with patch.object(analysis, "analyse_trial", side_effect=fake_analyse):
            result = analysis.export_suite(suite)
        return result, sweep.read_json(suite / "summary.json")

    def test_confirm_signal_uses_eventual_leadership_and_attempt_floor_not_two_second_speed(self):
        with tempfile.TemporaryDirectory(prefix="step28_signal_test_") as directory:
            root = Path(directory)
            strong, strong_summary = self.run_mocked_confirm_export(root / "strong", 10)
            self.assertTrue(strong["complete"])
            self.assertTrue(strong["integrity_pass"])
            self.assertTrue(strong["descriptive_signal"]["pass_"])
            self.assertTrue(strong["descriptive_signal"][
                "each_arm_each_seed_has_at_least_10_x_attempts"])
            self.assertGreaterEqual(strong["descriptive_signal"]["mean_x_led_rate_delta"], 0.10)
            by_seed_case = {(row["seed"], row["case"]["name"]): row["summary_metrics"]
                            for row in strong_summary["trials"]}
            for seed in (2801, 2802, 2803):
                single = by_seed_case[(seed, "reliable_single")]
                renewed = by_seed_case[(seed, "reliable_renewed")]
                self.assertGreater(renewed["x_led_rate"], single["x_led_rate"])
                self.assertLess(renewed["x_led_by_2s_rate"], single["x_led_by_2s_rate"])

            low_floor, low_floor_summary = self.run_mocked_confirm_export(root / "low_floor", 9)
            self.assertTrue(low_floor["complete"])
            self.assertTrue(low_floor["integrity_pass"])
            self.assertFalse(low_floor["descriptive_signal"][
                "each_arm_each_seed_has_at_least_10_x_attempts"])
            self.assertFalse(low_floor["descriptive_signal"]["pass_"])
            self.assertTrue(low_floor_summary["integrity_pass"])


if __name__ == "__main__":
    unittest.main()
