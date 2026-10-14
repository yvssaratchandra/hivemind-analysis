"""Offline contracts for the Step 29 publication-policy sweep."""
from __future__ import annotations

import asyncio
import contextlib
import io
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
from types import SimpleNamespace
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

import step29_policies as sweep


def write_json(path: Path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")


class DesignTests(unittest.TestCase):
    def test_native_expiration_uses_scheduled_or_minimum_time_then_search_cap(self):
        self.assertEqual(sweep.native_expiration(100.0, 99.0, 2.0, 110.0), 102.0)
        self.assertEqual(sweep.native_expiration(100.0, 104.0, 2.0, 110.0), 104.0)
        self.assertEqual(sweep.native_expiration(100.0, 99.0, 2.0, 101.0), 101.0)

    def test_renewal_sleep_subtracts_serialized_write_and_respects_expiry(self):
        # The one-second cadence starts after the serialized DHT write.
        self.assertAlmostEqual(sweep.renewal_sleep(110.0, 100.0, 0.1), 0.9)
        # An imminent expiry shortens the next wait even when the cadence has time left.
        self.assertAlmostEqual(sweep.renewal_sleep(100.25, 100.0, 0.1), 0.25)
        self.assertEqual(sweep.renewal_sleep(110.0, 100.0, 1.2), 0.0)

    def test_cases_keep_native_controls_and_recruit_only_is_passive(self):
        normal = sweep.make_cases("normal")
        both = sweep.make_cases("both")
        expected_suffixes = ("native_5s", "native_1s", "native_2s", "renew_1_2")

        self.assertEqual([case["name"] for case in normal],
                         [f"normal_{suffix}" for suffix in expected_suffixes])
        self.assertEqual(len(both), 8)
        self.assertEqual([case["name"] for case in both],
                         [f"{behavior}_{suffix}"
                          for behavior in ("normal", "recruit_only")
                          for suffix in expected_suffixes])
        self.assertEqual({(case["publication_policy"], case["lifetime"]) for case in normal},
                         {("native", 5.0), ("native", 1.0), ("native", 2.0), ("renew", 2.0)})
        for case in both:
            self.assertEqual((case["minimum"], case["target"]), (2, 4))
            self.assertEqual(case["response"], "reliable")
            self.assertEqual(case["one_way_delay_seconds"], 0.0)
            self.assertEqual(case["hold_seconds"], 0.0)
            expected_mode = "stock" if case["behavior"] == "normal" else "passive"
            self.assertEqual(case["mode"], expected_mode)
        renew = next(case for case in normal if case["publication_policy"] == "renew")
        self.assertEqual((renew["frequency"], renew["nominal_lower"], renew["nominal_upper"]),
                         (1.0, 1.0, 2.0))

    def test_pilot_is_exactly_balanced_and_reproducibly_shuffled(self):
        jobs = sweep.make_jobs("pilot", "both")
        expected_order = [
            "recruit_only_native_2s", "normal_native_5s", "recruit_only_native_1s",
            "normal_native_2s", "recruit_only_renew_1_2", "recruit_only_native_5s",
            "normal_renew_1_2", "normal_native_1s",
        ]
        self.assertEqual([job["case"]["name"] for job in jobs], expected_order)
        self.assertEqual(jobs, sweep.make_jobs("pilot", "both"))
        self.assertEqual({job["seed"] for job in jobs}, {2901})
        self.assertEqual({job["duration"] for job in jobs}, {45.0})
        self.assertEqual(len({job["case"]["name"] for job in jobs}), 8)
        self.assertEqual(
            {behavior: sum(job["case"]["behavior"] == behavior for job in jobs)
             for behavior in ("normal", "recruit_only")},
            {"normal": 4, "recruit_only": 4},
        )
        # The fixed shuffle changes the input order and keeps every policy once per behavior.
        self.assertNotEqual(expected_order, [case["name"] for case in sweep.make_cases("both")])

    def test_smoke_is_the_two_specific_normal_cases(self):
        jobs = sweep.make_jobs("smoke", "both")
        self.assertEqual([(job["case"]["name"], job["seed"], job["duration"]) for job in jobs], [
            ("normal_native_1s", 2900, 10.0),
            ("normal_renew_1_2", 2900, 10.0),
        ])


class TimedStorage:
    """Minimal stand-in for the queue cleared by the publisher's native teardown."""
    def __len__(self):
        return 0


class RuntimePublisherTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.previous_obs = sweep.OBS
        sweep.OBS = None

    def tearDown(self):
        sweep.OBS = self.previous_obs

    @staticmethod
    def install_fake_runtime(case, clock, iteration_limit=2):
        calls = []

        class Running:
            def __init__(self):
                self.waits = 0

            async def wait(self):
                self.waits += 1
                if self.waits > iteration_limit:
                    raise asyncio.CancelledError()

            def is_set(self):
                return True

        class Signal:
            def set(self):
                calls.append(("expiration_set",))

        class PotentialLeaders:
            def __init__(self):
                pass

        class Matchmaking:
            async def _request_join_potential_leaders(self, step):
                calls.append(("native_matchmaking", base.CASE["mode"]))
                return "native-matchmaking-result"

        async def original_declare(self, step, key_manager):
            calls.append(("original_native_publisher",))
            return "native-publisher-result"

        class KeyManager:
            current_key = "group-key"

            async def declare_averager(self, key, peer_id, expiration_time, looking_for_group=True):
                calls.append(("declare", expiration_time, looking_for_group))
                if looking_for_group:
                    # Model the serialized declaration RPC consuming 0.1 seconds.
                    clock["wall"] += 0.1
                    clock["monotonic"] += 0.1

            async def update_key_on_not_enough_peers(self):
                calls.append(("update_key",))

        base = SimpleNamespace(
            PotentialLeaders=PotentialLeaders,
            Matchmaking=Matchmaking,
            ORIGINAL_DECLARE=original_declare,
            CASE=case,
            ATTACKER=True,
            get_dht_time=lambda: clock["wall"],
            time=SimpleNamespace(monotonic=lambda: clock["monotonic"]),
        )
        observation = SimpleNamespace(
            base=base,
            sweep=SimpleNamespace(
                run_trial=None,
                write_json=None,
                user_data=lambda step: {"step_id": step},
            ),
            emit=lambda *args, **kwargs: calls.append(("emit", args[0], kwargs)),
        )
        fake_workload = types.ModuleType("step29_workload")
        fake_workload.run_trial = lambda *args, **kwargs: None
        return observation, base, calls, Running, Signal, KeyManager, fake_workload

    async def test_renewed_publisher_repeats_after_write_and_caps_sleep_at_expiry(self):
        case = next(c for c in sweep.make_cases() if c["publication_policy"] == "renew")
        clock = {"wall": 100.0, "monotonic": 50.0}
        obs, base, calls, Running, Signal, KeyManager, fake_workload = self.install_fake_runtime(
            case, clock, iteration_limit=2)
        base.ATTACKER = True
        base.CASE = case
        sleeps = []

        async def simulated_sleep(delay):
            sleeps.append(delay)
            calls.append(("sleep", delay))
            clock["wall"] += delay
            clock["monotonic"] += delay

        with patch.object(sweep.ancestor, "install_runtime", return_value=obs), \
             patch.dict(sys.modules, {"step29_workload": fake_workload}), \
             patch.object(sweep.asyncio, "sleep", side_effect=simulated_sleep):
            sweep.install_runtime()
            leaders = base.PotentialLeaders()
            leaders.lock_declare = asyncio.Lock()
            leaders.running = Running()
            leaders.min_matchmaking_time = 2.0
            leaders.search_end_time = 101.5
            leaders.declared_group_key = None
            leaders.declared_expiration_time = float("inf")
            leaders.declared_expiration = Signal()
            leaders.leader_queue = []
            leaders.max_assured_time = float("-inf")
            leaders.peer_id = "peer-X"
            manager = KeyManager()
            step = SimpleNamespace(scheduled_time=100.0)

            with self.assertRaises(asyncio.CancelledError):
                await base.PotentialLeaders._declare_averager_periodically(leaders, step, manager)

        active_expirations = [row[1] for row in calls if row[0] == "declare" and row[2] is True]
        self.assertEqual(active_expirations, [101.5, 101.5])
        self.assertAlmostEqual(sleeps[0], 0.9)
        self.assertAlmostEqual(sleeps[1], 0.4)
        self.assertEqual([row[0] for row in calls].count("update_key"), 2)
        self.assertEqual(calls[-1], ("declare", 101.5, False))

    async def test_native_publication_and_passive_recruitment_delegate_to_frozen_paths(self):
        native_case = next(c for c in sweep.make_cases() if c["name"] == "normal_native_5s")
        clock = {"wall": 100.0, "monotonic": 50.0}
        obs, base, calls, Running, Signal, KeyManager, fake_workload = self.install_fake_runtime(
            native_case, clock, iteration_limit=1)
        with patch.object(sweep.ancestor, "install_runtime", return_value=obs), \
             patch.dict(sys.modules, {"step29_workload": fake_workload}):
            sweep.install_runtime()

        leaders = base.PotentialLeaders()
        self.assertEqual(await base.PotentialLeaders._declare_averager_periodically(
            leaders, object(), KeyManager()), "native-publisher-result")
        self.assertEqual(calls.count(("original_native_publisher",)), 1)

        passive_case = next(c for c in sweep.make_cases("both")
                            if c["name"] == "recruit_only_renew_1_2")
        base.CASE = passive_case
        matchmaking = base.Matchmaking()
        matchmaking.peer_id = "peer-X"
        matchmaking.potential_leaders = SimpleNamespace(min_matchmaking_time=2.0)
        matchmaking.min_group_size = 2
        matchmaking.target_group_size = 4
        matchmaking.request_timeout = 3.0
        result = await base.Matchmaking._request_join_potential_leaders(matchmaking, "step-1")
        self.assertEqual(result, "native-matchmaking-result")
        self.assertIn(("native_matchmaking", "passive"), calls)
        self.assertEqual(sum(row[0] == "native_matchmaking" for row in calls), 1)


def make_completed_trial(suite: Path, job):
    trial = suite / sweep.ancestor.trial_name(job)
    trial.mkdir(parents=True, exist_ok=True)
    write_json(trial / "config.json", {
        "case": job["case"], "seed": job["seed"], "duration": job["duration"],
    })
    (trial / "events.jsonl").write_text('{"kind":"trace_closed"}\n', encoding="utf-8")
    write_json(trial / "summary.json", {"pending": [], "error": None})
    write_json(trial / "analysis.json", {"ok": True})
    write_json(trial / "control_analysis.json", {"integrity_pass": True})
    write_json(trial / "raw_verification.json", {"pass": True, "errors": []})
    tracked = (*sweep.RAW_FILES, "analysis.json", "control_analysis.json", "raw_verification.json")
    write_json(trial / "completion.json", {
        "file_sha256": {name: sweep.sha256(trial / name) for name in tracked},
        "all_peers_shutdown": True,
    })
    return trial


def make_suite(path: Path):
    jobs = sweep.make_jobs("smoke", "normal")
    path.mkdir(parents=True, exist_ok=True)
    write_json(path / "suite.json", {
        "experiment": "step29", "phase": "smoke", "behaviors": "normal", "jobs": jobs,
    })
    write_json(path / "provenance.json", {})
    return jobs


class ControllerTests(unittest.TestCase):
    def test_interrupted_trial_is_archived_and_run_again_on_resume(self):
        with tempfile.TemporaryDirectory(prefix="step29_resume_test_") as directory:
            suite = Path(directory) / "suite"
            jobs = make_suite(suite)
            interrupted = suite / sweep.ancestor.trial_name(jobs[0])
            interrupted.mkdir()
            (interrupted / "events.jsonl").write_text('{"kind":"step_start"}\n', encoding="utf-8")
            make_completed_trial(suite, jobs[1])

            def write_rerun(case, seed, path, duration, outcome):
                self.assertEqual((case, seed, duration, outcome),
                                 (jobs[0]["case"], jobs[0]["seed"], jobs[0]["duration"], "grouping"))
                path.mkdir()
                write_json(path / "config.json", {"case": case, "seed": seed, "duration": duration})
                (path / "events.jsonl").write_text('{"kind":"trace_closed"}\n', encoding="utf-8")
                write_json(path / "summary.json", {"pending": [], "error": None})
                write_json(path / "analysis.json", {"ok": True})
                return {"native": True}

            result = {"integrity_pass": True, "integrity_errors": [],
                      "summary_metrics": {
                          "total_groups": 0, "x_led_groups": 0, "x_submitted_attempts": 0,
                          "x_led_attempts": 0, "x_led_rate": 0.0,
                          "x_mean_capped_latency_seconds": 0.0,
                          "x_success_only_mean_first_lead_latency_seconds": 0.0,
                          "x_success_only_median_first_lead_latency_seconds": 0.0,
                          "honest_submitted_attempts": 0, "honest_first_choice_x_attempts": 0,
                          "honest_first_choice_other_attempts": 0,
                          "honest_no_outgoing_choice_attempts": 0,
                          "honest_first_group_x_led_attempts": 0,
                          "honest_first_group_other_led_attempts": 0,
                          "honest_no_group_attempts": 0,
                          "honest_accepted_x_reply_count": 0,
                          "honest_accepted_other_reply_count": 0,
                      }}
            analyzer = types.ModuleType("analyze_step29_policies")
            analyzer.analyse_trial = lambda path: result
            analyzer.export_suite = lambda path: {"exported": True}
            independent = types.ModuleType("verify_step28_exports_v2")
            independent.raw_trial = lambda path: {"errors": [], "metrics": result["summary_metrics"]}
            independent._same_value = lambda left, right: left == right
            fake_sweep = SimpleNamespace(run_trial=write_rerun, trial_should_halt=lambda *_: False)
            observation = SimpleNamespace(sweep=fake_sweep)

            with patch.object(sweep, "validate_sources"), \
                 patch.object(sweep, "install_runtime", return_value=observation), \
                 patch.object(sweep.ancestor, "verify_trial_shutdown"), \
                 patch.object(sweep.ancestor, "stop_requested", return_value=False), \
                 patch("multiprocessing.active_children", return_value=[]), \
                 patch.dict(sys.modules, {
                     "analyze_step29_policies": analyzer,
                     "verify_step28_exports_v2": independent,
                 }), \
                 contextlib.redirect_stdout(io.StringIO()):
                report = sweep.run_suite(suite)

            self.assertEqual(report, {"exported": True})
            self.assertTrue((interrupted / "completion.json").exists())
            self.assertTrue(list(suite.glob("archive_*.json")))
            with patch.object(sweep, "validate_sources"):
                state = sweep.inspect_suite(suite)
            self.assertEqual(state["counts"]["completed"], 2)
            self.assertEqual(state["counts"]["interrupted"], 0)

    def test_completed_raw_evidence_mutation_is_rejected(self):
        with tempfile.TemporaryDirectory(prefix="step29_tamper_test_") as directory:
            suite = Path(directory) / "suite"
            jobs = make_suite(suite)
            trial = make_completed_trial(suite, jobs[0])
            with patch.object(sweep, "validate_sources"):
                initial = sweep.inspect_suite(suite)
                self.assertEqual(initial["counts"]["completed"], 1)
                with (trial / "events.jsonl").open("a", encoding="utf-8") as stream:
                    stream.write('{"kind":"tampered"}\n')
                with self.assertRaisesRegex(ValueError, "completed evidence changed"):
                    sweep.inspect_suite(suite)


if __name__ == "__main__":
    unittest.main()
