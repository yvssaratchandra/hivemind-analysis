"""Offline contracts for the Step 27 control-plane screen."""
from __future__ import annotations

import asyncio
import builtins
import contextlib
import io
import json
import math
import random
import shutil
from pathlib import Path
import sys
import tempfile
import types
import unittest
from types import SimpleNamespace
from typing import get_type_hints
from unittest.mock import AsyncMock, Mock, patch

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

import step27_control_plane as sweep
import analyze_step27_control_plane as analysis
import analyze_step25_choices as step25_analysis
import verify_step27_raw as raw_verifier


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
        experiment="step27", outcome="grouping", instrumentation="detailed",
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
        control_delay_model="remote find/store request+response; join request+each response",
        once_unit="one active publication per native SEARCH; withdrawals separate",
        leadership_latency_origin="step_start", harm_timeout_cap=12,
        started_monotonic=100.0,
    )


def write_completed_trial(suite: Path, job, *, all_peers_shutdown=True, integrity=True):
    trial = suite / sweep.trial_name(job)
    trial.mkdir(parents=True, exist_ok=True)
    write_json(trial / "config.json", valid_config(job))
    (trial / "events.jsonl").write_text('{"kind":"trace_closed"}\n', encoding="utf-8")
    write_json(trial / "summary.json", {"pending": [], "error": None})
    write_json(trial / "analysis.json", {"control_plane_pass": True})
    write_json(trial / "control_analysis.json", {"integrity_pass": integrity})
    tracked = (*sweep.RAW_FILES, "analysis.json", "control_analysis.json")
    write_json(trial / "completion.json", {
        "file_sha256": {name: sweep.sha256(trial / name) for name in tracked},
        "all_peers_shutdown": all_peers_shutdown,
    })
    return trial


def make_suite(path: Path, phase="smoke"):
    jobs = sweep.make_jobs(phase)
    write_json(path / "suite.json", {
        "experiment": "step27", "phase": phase, "jobs": jobs,
        "training_delay": sweep.TRAINING, "order_seed": 270027,
        "outcome": "grouping", "instrumentation": "detailed",
    })
    write_json(path / "provenance.json", {})
    return jobs


class DesignTests(unittest.TestCase):
    def test_five_cases_hold_response_policy_and_delay_controls_fixed(self):
        cases = sweep.make_cases()
        self.assertEqual(len(cases), 5)
        self.assertEqual(cases, sweep.make_cases())
        self.assertEqual({case["name"] for case in cases}, {
            "native_baseline", "reliable_single", "reliable_renewed",
            "unreliable_single", "unreliable_renewed",
        })
        self.assertEqual(
            {(case["mode"], case["response"]) for case in cases},
            {("passive", "reliable"), ("once", "reliable"), ("refresh", "reliable"),
             ("once", "unreliable"), ("refresh", "unreliable")},
        )
        self.assertTrue(all(case["minimum"] == case["target"] == 4 for case in cases))
        self.assertTrue(all(case["one_way_delay_seconds"] == 0.15 for case in cases))
        self.assertEqual({case["lifetime"] for case in cases if case["mode"] != "passive"}, {2.0})
        self.assertEqual({case["hold_seconds"] for case in cases if case["response"] == "unreliable"}, {0.5})
        self.assertEqual(cases[0]["hold_seconds"], 0.0)
        self.assertEqual(
            {(case["frequency"], case["nominal_lower"], case["nominal_upper"])
             for case in cases if case["mode"] == "refresh"},
            {(2.0, 1.5, 2.0)},
        )

    def test_pilot_has_two_independent_seeds_and_five_cases_each(self):
        jobs = sweep.make_jobs("pilot")
        self.assertEqual(len(jobs), 10)
        self.assertEqual(jobs, sweep.make_jobs("pilot"))
        self.assertEqual({job["seed"] for job in jobs}, {2701, 2702})
        self.assertEqual({job["duration"] for job in jobs}, {60})
        self.assertEqual({job["case"]["name"] for job in jobs if job["seed"] == 2701},
                         {case["name"] for case in sweep.make_cases()})
        self.assertEqual({job["case"]["name"] for job in jobs if job["seed"] == 2702},
                         {case["name"] for case in sweep.make_cases()})
        expected = [dict(case=case, seed=seed, duration=60)
                    for seed in (2701, 2702) for case in sweep.make_cases()]
        random.Random(270027).shuffle(expected)
        self.assertEqual(jobs, expected)

    def test_smoke_is_the_two_renewed_response_policies(self):
        jobs = sweep.make_jobs("smoke")
        self.assertEqual(len(jobs), 2)
        self.assertEqual({job["seed"] for job in jobs}, {2700})
        self.assertEqual({job["duration"] for job in jobs}, {10})
        self.assertEqual({(job["case"]["mode"], job["case"]["response"]) for job in jobs},
                         {("refresh", "reliable"), ("refresh", "unreliable")})
        self.assertEqual({job["case"]["frequency"] for job in jobs}, {2.0})


class OfflineCliTests(unittest.TestCase):
    def test_dry_run_does_not_import_hivemind_or_socket_or_install_runtime(self):
        original_import = builtins.__import__

        def guarded_import(name, *args, **kwargs):
            if name.split(".", 1)[0] in {"hivemind", "socket", "step25_choice_measurement"}:
                raise AssertionError(f"offline dry-run attempted runtime import: {name}")
            return original_import(name, *args, **kwargs)

        stdout = io.StringIO()
        with patch("builtins.__import__", side_effect=guarded_import), \
             patch.object(sweep, "install_runtime", side_effect=AssertionError("runtime install")), \
             contextlib.redirect_stdout(stdout):
            sweep.main(["--phase", "smoke", "--dry-run"])

        result = json.loads(stdout.getvalue())
        self.assertEqual(result["experiment"], "step27")
        self.assertEqual(result["jobs"], sweep.make_jobs("smoke"))

    def test_resume_dry_run_checks_frozen_source_config_and_raw_hashes(self):
        with tempfile.TemporaryDirectory(prefix="step27_resume_test_") as directory:
            root = Path(directory)
            suite = root / "suite"
            suite.mkdir()
            source = root / "source"
            source.mkdir()
            jobs = make_suite(suite)
            for name in sweep.FROZEN_FILES:
                (suite / "harness").mkdir(exist_ok=True)
                shutil.copyfile(HERE / name, suite / "harness" / name)
            write_json(suite / "provenance.json", {
                "source": str(source), "source_sha256": {}, "runtime_files_sha256": {},
                "harness_sha256": {name: sweep.sha256(HERE / name) for name in sweep.FROZEN_FILES},
            })
            write_completed_trial(suite, jobs[0])

            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                sweep.main(["--resume", str(suite), "--dry-run"])
            result = json.loads(stdout.getvalue())
            row = next(row for row in result["jobs"] if row["trial_dir"] == sweep.trial_name(jobs[0]))
            self.assertEqual(row["status"], "completed")

            snapshot = suite / "harness" / "step27_control_plane.py"
            snapshot.write_text("source drift\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "frozen source drift"):
                sweep.main(["--resume", str(suite), "--dry-run"])
            shutil.copyfile(HERE / "step27_control_plane.py", snapshot)

            with (suite / sweep.trial_name(jobs[0]) / "events.jsonl").open("a", encoding="utf-8") as stream:
                stream.write('{"kind":"tampered"}\n')
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                sweep.main(["--resume", str(suite), "--dry-run"])
            drift = json.loads(stdout.getvalue())
            changed = next(row for row in drift["jobs"]
                           if row["trial_dir"] == sweep.trial_name(jobs[0]))
            self.assertEqual(changed["status"], "blocked")
            self.assertIn("trial evidence changed", changed["error"])


class RuntimeHandlerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.events = []
        self.event = asyncio.Event()
        self.sleep_calls = []
        self.request = SimpleNamespace(expiration=12.0)
        self.context = SimpleNamespace(remote_id="follower")
        self.response_codes = SimpleNamespace(ACCEPTED=1, GROUP_DISBANDED=2, REJECTED=3)

    def fake_base(self, *, attacker=True, response="unreliable"):
        def message(code):
            return SimpleNamespace(code=code)

        return SimpleNamespace(
            ATTACKER=attacker,
            CASE={"response": response, "hold_seconds": 0.5,
                  "one_way_delay_seconds": 0.15, "lifetime": 2.0},
            averaging_pb2=SimpleNamespace(
                MessageFromLeader=message, ACCEPTED=1, GROUP_DISBANDED=2,
                MessageCode=SimpleNamespace(Name=lambda code: str(code)),
            ),
        )

    def leader(self, rejection=None):
        calls = []

        def check(request, context):
            calls.append((request, context))
            return rejection

        mm = SimpleNamespace(
            lock_request_join_group=asyncio.Lock(),
            _check_reasons_to_reject=check,
            current_followers={},
            follower_was_discarded=self.event,
            potential_leaders=SimpleNamespace(_s25_step_id="step-1", _s25_search_id="search-1",
                                              declared_expiration_time=8.0,
                                              declared_group_key="leader-key"),
            is_looking_for_group=True,
            current_leader=None,
        )
        return SimpleNamespace(peer_id="X", _matchmaking=mm), mm, calls

    async def test_unreliable_join_runs_native_admission_accepts_holds_and_disbands(self):
        peer, matchmaking, admission_calls = self.leader()
        sleep_started = asyncio.Event()

        async def controlled_sleep(seconds):
            self.sleep_calls.append(seconds)
            if seconds == 0.5:
                sleep_started.set()

        self.addCleanup(lambda: setattr(sweep, "VISIT_NUMBER", 0))
        with patch.object(sweep, "BASE", self.fake_base()), \
             patch.object(sweep, "OBS", SimpleNamespace(emit=lambda kind, *args, **details:
                                                          self.events.append((kind, details)))), \
             patch.object(sweep, "VISIT_NUMBER", 0), \
             patch.object(sweep.asyncio, "sleep", side_effect=controlled_sleep):
            stream = sweep.unreliable_join(peer, self.request, self.context)
            accepted = await anext(stream)
            self.assertEqual(accepted.code, self.response_codes.ACCEPTED)
            self.assertEqual(matchmaking.current_followers["follower"], self.request)
            self.assertEqual(admission_calls, [(self.request, self.context)])

            next_response = asyncio.create_task(anext(stream))
            await sleep_started.wait()
            disbanded = await next_response
            self.assertEqual(disbanded.code, self.response_codes.GROUP_DISBANDED)
            with self.assertRaises(StopAsyncIteration):
                await anext(stream)

        self.assertEqual(self.sleep_calls, [0.5])
        self.assertEqual(matchmaking.current_followers, {})
        self.assertTrue(self.event.is_set())
        self.assertEqual([kind for kind, _ in self.events], [
            "unreliable_accept", "unreliable_hold_start", "unreliable_disband", "unreliable_release",
        ])

    async def test_native_admission_rejection_is_forwarded_without_hold(self):
        reject = SimpleNamespace(code=self.response_codes.REJECTED)
        peer, matchmaking, admission_calls = self.leader(rejection=reject)
        with patch.object(sweep, "BASE", self.fake_base()), \
             patch.object(sweep, "OBS", SimpleNamespace(emit=lambda kind, *args, **details:
                                                          self.events.append((kind, details)))), \
             patch.object(sweep, "VISIT_NUMBER", 0), \
             patch.object(sweep.asyncio, "sleep", new_callable=AsyncMock) as sleep:
            responses = [response async for response in sweep.unreliable_join(peer, self.request, self.context)]

        self.assertEqual(responses, [reject])
        self.assertEqual(admission_calls, [(self.request, self.context)])
        self.assertEqual(matchmaking.current_followers, {})
        sleep.assert_not_awaited()
        self.assertEqual([kind for kind, _ in self.events], ["unreliable_reject"])

    async def test_cancelling_join_after_acceptance_releases_follower(self):
        peer, matchmaking, _ = self.leader()
        sleep_entered = asyncio.Event()

        async def hold(seconds):
            self.sleep_calls.append(seconds)
            sleep_entered.set()
            await asyncio.Future()

        with patch.object(sweep, "BASE", self.fake_base()), \
             patch.object(sweep, "OBS", SimpleNamespace(emit=lambda kind, *args, **details:
                                                          self.events.append((kind, details)))), \
             patch.object(sweep, "VISIT_NUMBER", 0), \
             patch.object(sweep.asyncio, "sleep", side_effect=hold):
            stream = sweep.unreliable_join(peer, self.request, self.context)
            accepted = await anext(stream)
            self.assertEqual(accepted.code, self.response_codes.ACCEPTED)
            next_response = asyncio.create_task(anext(stream))
            await sleep_entered.wait()
            next_response.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await next_response
            with self.assertRaises(StopAsyncIteration):
                await anext(stream)

        self.assertEqual(matchmaking.current_followers, {})
        self.assertTrue(self.event.is_set())
        self.assertIn("unreliable_cancel", [kind for kind, _ in self.events])
        self.assertIn("unreliable_release", [kind for kind, _ in self.events])

    async def test_installed_runtime_rejects_assembly_by_unreliable_x(self):
        async def native_declare(self, *args, **kwargs):
            return None

        async def native_rpc(self, *args, **kwargs):
            return None

        async def native_join(self, *args, **kwargs):
            if False:
                yield None

        native_join.__annotations__ = {"request": bytes, "context": object, "return": bytes}

        async def native_assemble(self):
            return "assembled"

        class PotentialLeaders:
            _declare_averager_periodically = native_declare

        class DHTProtocol:
            rpc_find = native_rpc
            rpc_store = native_rpc

        class RefreshAverager:
            rpc_join_group = native_join

        self.assertEqual(get_type_hints(RefreshAverager.rpc_join_group), {
            "request": bytes, "context": object, "return": bytes,
        })

        class Matchmaking:
            leader_assemble_group = native_assemble

        base = SimpleNamespace(
            PotentialLeaders=PotentialLeaders, DHTProtocol=DHTProtocol,
            RefreshAverager=RefreshAverager, Matchmaking=Matchmaking,
            ATTACKER=True, CASE={"response": "unreliable", "one_way_delay_seconds": 0.15,
                                 "hold_seconds": 0.5},
            averaging_pb2=SimpleNamespace(
                MessageFromLeader=lambda code: SimpleNamespace(code=code),
                ACCEPTED=1, GROUP_DISBANDED=2,
                MessageCode=SimpleNamespace(Name=lambda code: str(code)),
            ),
            torch=SimpleNamespace(set_num_threads=Mock()),
        )
        observation = types.ModuleType("step25_choice_measurement")
        observation.base = base
        observation.BATCH_SIZE = 32
        observation.sweep = SimpleNamespace(write_json=None, snapshot=Mock(),
                                            user_data=lambda _control: {"step_id": "step-1"})
        observation.emit = lambda kind, *args, **details: self.events.append((kind, details))
        observation.OBS = None
        with patch.object(sweep, "BASE", None), patch.object(sweep, "OBS", None), \
             patch.object(sweep, "ORIGINAL_JOIN_RESPONSE", None), \
             patch.dict(sys.modules, {"step25_choice_measurement": observation}), \
             patch("multiprocessing.set_start_method"):
            installed = sweep.install_runtime()
            leader = SimpleNamespace(peer_id="X", step_control=object(),
                                     potential_leaders=SimpleNamespace(declared_expiration_time=8.0))
            with self.assertRaisesRegex(AssertionError, "must never assemble"):
                await installed.base.Matchmaking.leader_assemble_group(leader)
            self.assertEqual(sweep.unreliable_join.__annotations__, {
                "request": bytes, "context": object, "return": bytes,
            })
            annotations = get_type_hints(installed.base.RefreshAverager.rpc_join_group)
            self.assertEqual(annotations, {"request": bytes, "context": object, "return": bytes})

            # Exercise the registered outer wrapper: the server RPC ID must
            # flow through the accepted/hold/disband/release events.
            join_peer = installed.base.RefreshAverager()
            join_peer.peer_id = "X"
            join_peer._matchmaking = self.leader()[1]
            with patch.object(sweep, "injected_sleep", new_callable=AsyncMock), \
                 patch.object(sweep.asyncio, "sleep", new_callable=AsyncMock):
                stream = installed.base.RefreshAverager.rpc_join_group(
                    join_peer, self.request, self.context)
                self.assertEqual((await anext(stream)).code, 1)
                self.assertEqual((await anext(stream)).code, 2)
                with self.assertRaises(StopAsyncIteration):
                    await anext(stream)

            rpc_start = next(details for kind, details in self.events
                             if kind == "control_rpc_start" and details.get("rpc_type") == "join")
            linked = [details for kind, details in self.events if kind in {
                "unreliable_accept", "unreliable_hold_start", "unreliable_disband",
                "unreliable_release",
            }]
            self.assertEqual(len(linked), 4)
            self.assertEqual({details.get("rpc_id") for details in linked},
                             {rpc_start["rpc_id"]})

        self.assertIn("assembly_deadline_state", [kind for kind, _ in self.events])

    async def test_reliable_policy_delegates_to_native_join_response(self):
        native_response = SimpleNamespace(code=9)
        calls = []

        async def native(self, request, context):
            calls.append((self, request, context))
            yield native_response

        peer = SimpleNamespace(peer_id="X")
        with patch.object(sweep, "BASE", self.fake_base(response="reliable")), \
             patch.object(sweep, "ORIGINAL_JOIN_RESPONSE", native), \
             patch.object(sweep, "OBS", SimpleNamespace(emit=Mock())):
            responses = [response async for response in
                         sweep.unreliable_join(peer, self.request, self.context)]

        self.assertEqual(responses, [native_response])
        self.assertEqual(calls, [(peer, self.request, self.context)])


class AnalyzerAttemptTests(unittest.TestCase):
    PEER_IDS = {"X": "peer-x", **{f"H{i}": f"peer-h{i}" for i in range(7)}}
    STARTED = 100.0
    CUTOFF = 110.0

    @staticmethod
    def attempt(step_id, peer_name, started, groups=(), outcome="failed"):
        peer_id = AnalyzerAttemptTests.PEER_IDS[peer_name]
        return dict(step_id=step_id, peer_id=peer_id, peer_name=peer_name,
                    started_monotonic=started, started_wall=started + 1000.0,
                    start_event_ref=f"{peer_name}:{step_id}",
                    assembled_group_ids=list(groups), eventual_outcome=outcome,
                    search_count=1, first_search_event_ref=None)

    @staticmethod
    def group(group_id, leader_name, leader_step, roster_names, assembled, *,
              member_steps=None, in_submission=True):
        peer = AnalyzerAttemptTests.PEER_IDS
        member_steps = member_steps or {}
        return dict(group_id=group_id, leader_name=leader_name,
                    leader_peer_id=peer[leader_name], leader_step_id=leader_step,
                    roster=[peer[name] for name in roster_names],
                    member_step_ids={peer[name]: step_id for name, step_id in member_steps.items()},
                    assembly_event_ref=f"event:{group_id}",
                    assembly_event_refs=[f"event:{group_id}"],
                    assembly_monotonic=assembled, assembly_wall=assembled + 1000.0,
                    roster_size=len(roster_names), x_in_roster=peer["X"] in
                    [peer[name] for name in roster_names],
                    within_submission_window=in_submission,
                    honest_membership_count=sum(name.startswith("H") for name in roster_names))

    def config(self):
        return {"started_monotonic": self.STARTED, "duration": 10}

    def test_step_start_latency_keeps_failed_and_drain_attempts_without_multiplying_attempts(self):
        native = {"attempts": [
            self.attempt("x-1", "X", 100.0, ("g-x-first", "g-x-second"), "led"),
            self.attempt("x-2", "X", 110.0),  # Starts exactly at cutoff, so it is drain-only.
            self.attempt("h0-old", "H0", 101.0),
            self.attempt("h0-current", "H0", 103.0, ("g-h0",), "followed"),
            self.attempt("h1-failed", "H1", 102.0),
            self.attempt("h1-current", "H1", 108.0, ("g-h2-later",), "followed"),
            self.attempt("h2-current", "H2", 104.0, ("g-h0",), "followed"),
            self.attempt("h2-success", "H2", 109.0, ("g-h2-later",), "followed"),
        ], "selections": []}
        groups = [
            self.group("g-x-first", "X", "x-1", ("X", "H0"), 108.0,
                       member_steps={"X": "x-1", "H0": "h0-current"}),
            self.group("g-x-second", "X", "x-1", ("X", "H0"), 109.0,
                       member_steps={"X": "x-1", "H0": "h0-current"}),
            self.group("g-h0", "H2", "h2-current", ("H0", "H2"), 107.0,
                       member_steps={"H0": "h0-current", "H2": "h2-current"}),
            # H1 appears in a later roster, but this is H2's successful attempt.
            self.group("g-h2-later", "H2", "h2-success", ("H1", "H2"), 111.0,
                       member_steps={"H1": "h1-current", "H2": "h2-success"},
                       in_submission=False),
        ]
        # A leader starts the collective itself. It has an allreduce_start
        # readiness witness and correctly has no client BEGIN receipt.
        ready = dict(pid=9, event_number=1, peer=self.PEER_IDS["X"], other="",
                     kind="allreduce_start", monotonic=108.2, wall_time=1108.2,
                     detail={"step_id": "x-1", "group_id": "g-x-first"})
        events = [ready]
        by_ref = {"9:1": ready}
        errors = []
        attempts = analysis._attempt_rows(
            native, groups, self.config(), self.PEER_IDS, events, by_ref, errors)
        by_step = {row["step_id"]: row for row in attempts}

        self.assertEqual(errors, [])
        self.assertEqual(len(attempts), len(native["attempts"]))
        self.assertEqual(by_step["x-1"]["first_lead_group_id"], "g-x-first")
        self.assertEqual(by_step["x-1"]["lead_latency_seconds"], 8.0)
        self.assertTrue(by_step["x-1"]["allreduce_started"])
        self.assertFalse(by_step["x-1"]["begin_allreduce_received"])
        self.assertEqual(by_step["x-1"]["ready_evidence_kind"], "allreduce_start")
        self.assertFalse(by_step["x-1"]["assembly_roster_vs_ready_evidence_diverges"])
        self.assertEqual(by_step["x-2"]["lead_capped_latency_seconds"], 12.0)
        self.assertFalse(by_step["x-2"]["submission_started"])
        self.assertTrue(by_step["x-2"]["late_start_after_submission_cutoff"])
        self.assertIsNone(by_step["h0-old"]["first_assembly_group_id"])
        self.assertEqual(by_step["h0-current"]["first_assembly_group_id"], "g-h0")
        self.assertIsNone(by_step["h1-failed"]["first_assembly_group_id"])
        self.assertEqual(by_step["h1-failed"]["capped_latency_seconds"], 12.0)
        # A pre-cutoff attempt can assemble during drain; latency still starts
        # at its own step_start while submission-window membership excludes it.
        self.assertEqual(by_step["h1-current"]["first_assembly_group_id"], "g-h2-later")
        self.assertEqual(by_step["h1-current"]["capped_latency_seconds"], 3.0)
        self.assertEqual(by_step["h2-success"]["capped_latency_seconds"], 2.0)

        metrics = analysis._trial_metrics(native, attempts, groups, [], [], self.config(), self.PEER_IDS)
        self.assertEqual(metrics["x_submitted_attempts"], 2)
        self.assertEqual(metrics["x_late_start_attempts"], 1)
        self.assertEqual(metrics["x_led_attempts"], 1)
        self.assertEqual(metrics["honest_submitted_attempts"], 6)
        self.assertEqual(metrics["honest_mean_capped_latency_seconds"], 6.0)
        # g-h2-later is assembled during drain and contributes no submission-window membership.
        self.assertEqual(metrics["honest_group_memberships_in_submission_window"], 4)
        self.assertAlmostEqual(metrics["honest_peer_minutes"], 7 * 10 / 60)
        self.assertAlmostEqual(metrics["honest_group_memberships_per_honest_peer_minute"], 24 / 7)

    def test_native_duplicate_group_id_is_counted_once_and_reported(self):
        events = [
            dict(pid=7, event_number=1, peer="peer-x", other="", kind="assembled",
                 monotonic=108.0, wall_time=1108.0,
                 detail={"group_id": "g1", "roster": ["peer-h0", "peer-x"], "step_id": "x-1"}),
            dict(pid=7, event_number=2, peer="peer-x", other="", kind="assembled",
                 monotonic=108.1, wall_time=1108.1,
                 detail={"group_id": "g1", "roster": ["peer-h0", "peer-x"], "step_id": "x-1"}),
        ]
        errors = []
        names = {peer: name for name, peer in self.PEER_IDS.items()}
        groups = step25_analysis._build_groups(events, self.PEER_IDS, names, errors)

        self.assertEqual(len(groups), 1)
        self.assertEqual(groups[0]["group_id"], "g1")
        self.assertEqual(groups[0]["assembly_event_refs"], ["7:1", "7:2"])
        self.assertTrue(any("duplicate assembled group_id g1" in error for error in errors))


class AnalyzerRetryTests(unittest.TestCase):
    PEER_IDS = AnalyzerAttemptTests.PEER_IDS

    @staticmethod
    def event(pid, number, kind, monotonic, peer="peer-x", other="", **detail):
        return dict(pid=pid, event_number=number, kind=kind, monotonic=monotonic,
                    wall_time=1000.0 + monotonic, peer=peer, other=other, detail=detail)

    @staticmethod
    def group(group_id, assembled):
        return dict(group_id=group_id, leader_peer_id="peer-x", leader_step_id="x-step",
                    assembly_monotonic=assembled)

    def test_retries_require_x_as_actual_selection_and_count_one_per_selection_event(self):
        events = [
            self.event(1, 1, "search_span_start", 1.0),
            self.event(1, 2, "ad_started", 2.0, active=True, expiration=101.0),
            self.event(1, 3, "ad_started", 3.0, active=True, expiration=102.0),
            self.event(1, 4, "ad_started", 4.5, active=True, expiration=108.0),
            self.event(1, 5, "assembled", 6.0, detail={"group_id": "gx", "roster": ["peer-x"]}),
            self.event(1, 6, "ad_started", 6.5, active=True, expiration=109.0),
            self.event(1, 7, "search_span_end", 12.0),
            self.event(2, 1, "selection", 4.0, peer="peer-h0", other="peer-h1"),
            self.event(2, 2, "selection", 5.0, peer="peer-h0", other="peer-x"),
            self.event(2, 3, "selection", 7.0, peer="peer-h0", other="peer-x"),
        ]
        by_ref = {f"{row['pid']}:{row['event_number']}": row for row in events}
        native = {
            "searches": [dict(peer_id="peer-x", search_id="x-search", step_id="x-step",
                              start_event_ref="1:1", end_event_ref="1:7",
                              start_monotonic=1.0, end_monotonic=12.0),
                          dict(peer_id="peer-h0", search_id="h0-search", step_id="h0-step",
                               start_event_ref="2:1", end_event_ref="2:3",
                               start_monotonic=1.0, end_monotonic=12.0)],
            "selections": [
                dict(peer_id="peer-h0", search_id="h0-search", step_id="h0-step",
                     event_ref="2:1", leader_peer_id="peer-h1", selected_expiration=103.0,
                     selection_top_wall=100.0, selection_valid=True,
                     past_attempts=[{"peer": "peer-x", "expiration": 101.0}]),
                dict(peer_id="peer-h0", search_id="h0-search", step_id="h0-step",
                     event_ref="2:2", leader_peer_id="peer-x", selected_expiration=108.0,
                     selection_top_wall=100.0, selection_valid=True,
                     past_attempts=[{"peer": "peer-x", "expiration": 101.0},
                                    {"peer": "peer-x", "expiration": 102.0}]),
                dict(peer_id="peer-h0", search_id="h0-search", step_id="h0-step",
                     event_ref="2:3", leader_peer_id="peer-x", selected_expiration=109.0,
                     selection_top_wall=100.0, selection_valid=True,
                     past_attempts=[{"peer": "peer-x", "expiration": 101.0},
                                    {"peer": "peer-x", "expiration": 102.0}]),
            ],
        }
        rows = analysis._retry_rows(native, events, by_ref, self.PEER_IDS,
                                    [self.group("gx", 6.0)])

        self.assertEqual(len(rows), 2)
        by_selection = {row["selection_event_ref"]: row for row in rows}
        self.assertEqual(set(by_selection), {"2:2", "2:3"})
        self.assertEqual(by_selection["2:2"]["x_assembly_relative_to_selection"], "after")
        self.assertEqual(by_selection["2:3"]["x_assembly_relative_to_selection"], "before")
        self.assertTrue(all(row["x_search_id"] == "x-search" for row in rows))

    def test_old_and_new_x_publications_must_be_in_same_open_search(self):
        events = [
            self.event(1, 1, "search_span_start", 1.0),
            self.event(1, 2, "ad_started", 2.0, active=True, expiration=101.0),
            self.event(1, 3, "search_span_end", 3.0),
            self.event(2, 1, "search_span_start", 4.0),
            self.event(2, 2, "ad_started", 5.0, active=True, expiration=105.0),
            self.event(2, 3, "selection", 6.0, peer="peer-h0", other="peer-x"),
            self.event(2, 4, "search_span_end", 7.0),
        ]
        by_ref = {f"{row['pid']}:{row['event_number']}": row for row in events}
        native = {
            "searches": [
                dict(peer_id="peer-x", search_id="x-old", step_id="x-step-old",
                     start_event_ref="1:1", end_event_ref="1:3", start_monotonic=1.0,
                     end_monotonic=3.0),
                dict(peer_id="peer-x", search_id="x-new", step_id="x-step-new",
                     start_event_ref="2:1", end_event_ref="2:4", start_monotonic=4.0,
                     end_monotonic=7.0),
                dict(peer_id="peer-h0", search_id="h0-search", step_id="h0-step",
                     start_event_ref="2:1", end_event_ref="2:4", start_monotonic=4.0,
                     end_monotonic=7.0),
            ],
            "selections": [dict(
                peer_id="peer-h0", search_id="h0-search", step_id="h0-step",
                event_ref="2:3", leader_peer_id="peer-x", selected_expiration=105.0,
                selection_top_wall=100.0, selection_valid=True,
                past_attempts=[{"peer": "peer-x", "expiration": 101.0}],
            )],
        }

        rows = analysis._retry_rows(native, events, by_ref, self.PEER_IDS, [])
        self.assertEqual(rows, [])

    def test_old_pair_expiration_requires_exact_float_match(self):
        almost_old = math.nextafter(101.0, math.inf)
        events = [
            self.event(1, 1, "search_span_start", 1.0),
            self.event(1, 2, "ad_started", 2.0, active=True, expiration=almost_old),
            self.event(1, 3, "ad_started", 4.0, active=True, expiration=105.0),
            self.event(1, 4, "search_span_end", 10.0),
            self.event(2, 1, "selection", 5.0, peer="peer-h0", other="peer-x"),
        ]
        by_ref = {f"{row['pid']}:{row['event_number']}": row for row in events}
        native = {
            "searches": [dict(peer_id="peer-x", search_id="x-search", step_id="x-step",
                              start_event_ref="1:1", end_event_ref="1:4",
                              start_monotonic=1.0, end_monotonic=10.0),
                          dict(peer_id="peer-h0", search_id="h0-search", step_id="h0-step",
                               start_event_ref="2:1", end_event_ref="2:1",
                               start_monotonic=1.0, end_monotonic=10.0)],
            "selections": [dict(peer_id="peer-h0", search_id="h0-search", step_id="h0-step",
                                 event_ref="2:1", leader_peer_id="peer-x", selected_expiration=105.0,
                                 selection_top_wall=100.0, selection_valid=True,
                                 past_attempts=[{"peer": "peer-x", "expiration": 101.0}])],
        }

        self.assertEqual(analysis._retry_rows(native, events, by_ref, self.PEER_IDS, []), [])


class AnalyzerContrastTests(unittest.TestCase):
    @staticmethod
    def arms(**updates):
        defaults = {
            "native_baseline": dict(x_led_by_2s_rate=0.0, x_led_rate=0.2,
                                     x_submitted_attempts=10, honest_submitted_attempts=30,
                                     honest_mean_capped_latency_seconds=4.0,
                                     same_search_x_retry_count=0),
            "reliable_single": dict(x_led_by_2s_rate=0.2, x_led_rate=0.3,
                                     x_submitted_attempts=10, honest_submitted_attempts=30,
                                     honest_mean_capped_latency_seconds=4.0,
                                     same_search_x_retry_count=0),
            "reliable_renewed": dict(x_led_by_2s_rate=0.2, x_led_rate=0.3,
                                     x_submitted_attempts=10, honest_submitted_attempts=30,
                                     honest_mean_capped_latency_seconds=4.0,
                                     same_search_x_retry_count=0),
            "unreliable_single": dict(x_led_by_2s_rate=0.1, x_led_rate=0.2,
                                      x_submitted_attempts=10, honest_submitted_attempts=30,
                                      honest_mean_capped_latency_seconds=4.0,
                                      same_search_x_retry_count=0),
            "unreliable_renewed": dict(x_led_by_2s_rate=0.1, x_led_rate=0.2,
                                       x_submitted_attempts=10, honest_submitted_attempts=30,
                                       honest_mean_capped_latency_seconds=5.0,
                                       same_search_x_retry_count=1),
        }
        for case, metrics in updates.items():
            defaults[case].update(metrics)
        return {name: {"summary_metrics": metrics} for name, metrics in defaults.items()}

    def test_renewal_specific_leadership_gain_compares_reliable_renewed_to_single(self):
        contrast = analysis._contrast_for_seed(2701, self.arms())
        self.assertTrue(contrast["delta_x_led_by_2s_rate_reliable_renewed_vs_native"] > 0)
        self.assertEqual(
            contrast["delta_x_led_by_2s_rate_reliable_renewed_vs_reliable_single"], 0.0)
        self.assertFalse(contrast["screen_flags"]["leadership_gain"])
        self.assertTrue(contrast["screen_flags"]["renewal_leadership_gain_vs_native"])

    def test_unreliable_renewed_retry_can_witness_harm_with_no_reliable_renewal_retry(self):
        arms = self.arms(
            reliable_single={"honest_mean_capped_latency_seconds": 4.0,
                             "same_search_x_retry_count": 0},
            reliable_renewed={"honest_mean_capped_latency_seconds": 4.0,
                              "same_search_x_retry_count": 0},
            unreliable_single={"honest_mean_capped_latency_seconds": 4.0},
            unreliable_renewed={"honest_mean_capped_latency_seconds": 5.0,
                                "same_search_x_retry_count": 1},
        )
        contrast = analysis._contrast_for_seed(2702, arms)
        self.assertEqual(contrast["harm_difference_in_differences_seconds"], 1.0)
        self.assertEqual(contrast["unreliable_renewed_same_search_x_retries"], 1)
        self.assertTrue(contrast["screen_flags"]["harm_did_at_least_1s"])

    def test_overall_paired_flag_requires_both_seed_contrasts(self):
        good = analysis._contrast_for_seed(2701, self.arms(
            reliable_renewed={"x_led_by_2s_rate": 0.6},
        ))
        bad = analysis._contrast_for_seed(2702, self.arms(
            reliable_renewed={"x_led_by_2s_rate": 0.1,
                              "honest_mean_capped_latency_seconds": 2.0},
            unreliable_renewed={"honest_mean_capped_latency_seconds": 2.0},
        ))
        pooled_rr = (good["x_led_by_2s_rate_reliable_renewed"] +
                     bad["x_led_by_2s_rate_reliable_renewed"]) / 2
        pooled_rs = (good["x_led_by_2s_rate_reliable_single"] +
                     bad["x_led_by_2s_rate_reliable_single"]) / 2
        self.assertGreater(pooled_rr, pooled_rs)
        overall = analysis._paired_screen_flags([good, bad])

        self.assertTrue(good["screen_flags"]["leadership_gain"])
        self.assertFalse(bad["screen_flags"]["leadership_gain"])
        self.assertEqual(set(overall), {
            "leadership_gain", "native_policy_leadership_gain",
            "renewal_amplified_harm", "renewal_alone_harm",
        })
        self.assertFalse(overall["leadership_gain"])

    def test_pilot_export_publishes_paired_flags_and_rejects_one_seed_worse(self):
        jobs = analysis.canonical_jobs("pilot")
        arms_by_seed = {
            2701: self.arms(reliable_renewed={"x_led_by_2s_rate": 0.6}),
            2702: self.arms(reliable_renewed={
                "x_led_by_2s_rate": 0.1,
                "honest_mean_capped_latency_seconds": 2.0,
            }, unreliable_renewed={"honest_mean_capped_latency_seconds": 5.0}),
        }
        with tempfile.TemporaryDirectory(prefix="step27_paired_export_") as directory:
            suite = Path(directory)
            write_json(suite / "suite.json", {
                "experiment": "step27", "phase": "pilot", "jobs": jobs,
                "outcome": "grouping", "instrumentation": "detailed",
            })
            by_trial = {}
            for job in jobs:
                case, seed = job["case"], job["seed"]
                name = f"{case['name']}_s{seed}"
                trial_path = suite / name
                trial_path.mkdir()
                (trial_path / "config.json").write_text("{}\n", encoding="utf-8")
                (trial_path / "events.jsonl").write_text("", encoding="utf-8")
                by_trial[name] = dict(
                    trial_dir=name, case=case, seed=seed, duration=job["duration"],
                    integrity_pass=True, integrity_errors=[],
                    summary_metrics=arms_by_seed[seed][case["name"]]["summary_metrics"],
                    policy_integrity={},
                    delay_integrity={"summary": {kind: {"correlated_delayed": 1}
                                                  for kind in ("find", "store", "join")}},
                    attempts=[], groups=[], retries=[], holds=[],
                )

            with patch.object(analysis, "analyse_trial",
                              side_effect=lambda path: by_trial[Path(path).name]):
                exported = analysis.export_suite(suite)

            summary = json.loads((suite / "summary.json").read_text())
            self.assertTrue(summary["complete"])
            self.assertEqual(summary["screen_flags"], exported["screen_flags"])
            self.assertFalse(summary["screen_flags"]["leadership_gain"])
            self.assertTrue(summary["screen_flags"]["renewal_amplified_harm"])
            self.assertEqual(summary["any_follow_up_signal"],
                             any(summary["screen_flags"].values()))
            by_seed = {row["seed"]: row for row in summary["per_seed_contrasts"]}
            self.assertTrue(by_seed[2701]["screen_flags"]["leadership_gain"])
            self.assertFalse(by_seed[2702]["screen_flags"]["leadership_gain"])
            self.assertEqual(summary["screen_status"]["leadership_gain"], "no screen flag")

    def test_smoke_gate_allows_valid_unreliable_disband_without_groups(self):
        jobs = analysis.canonical_jobs("smoke")
        with tempfile.TemporaryDirectory(prefix="step27_smoke_gate_") as directory:
            suite = Path(directory)
            write_json(suite / "suite.json", {
                "experiment": "step27", "phase": "smoke", "jobs": jobs,
                "outcome": "grouping", "instrumentation": "detailed",
            })
            by_trial = {}
            for job in jobs:
                case, seed = job["case"], job["seed"]
                name = f"{case['name']}_s{seed}"
                trial_path = suite / name
                trial_path.mkdir()
                (trial_path / "config.json").write_text("{}\n", encoding="utf-8")
                (trial_path / "events.jsonl").write_text("", encoding="utf-8")
                metrics = dict(
                    total_groups=(1 if case["response"] == "reliable" else 0),
                    unreliable_accept_count=(1 if case["response"] == "unreliable" else 0),
                    unreliable_hold_start_count=(1 if case["response"] == "unreliable" else 0),
                    unreliable_disband_emitted_count=(1 if case["response"] == "unreliable" else 0),
                )
                by_trial[name] = dict(
                    trial_dir=name, case=case, seed=seed, duration=job["duration"],
                    integrity_pass=True, integrity_errors=[], summary_metrics=metrics,
                    policy_integrity={},
                    delay_integrity={"summary": {kind: {"correlated_delayed": 1}
                                                  for kind in ("find", "store", "join")}},
                    attempts=[], groups=[], retries=[], holds=[],
                )

            with patch.object(analysis, "analyse_trial",
                              side_effect=lambda path: by_trial[Path(path).name]):
                exported = analysis.export_suite(suite)

            summary = json.loads((suite / "summary.json").read_text())
            unreliable = next(row for row in summary["trials"]
                              if row["case"]["response"] == "unreliable")
            self.assertEqual(unreliable["summary_metrics"]["total_groups"], 0)
            self.assertTrue(summary["smoke_gate_pass"], summary["integrity_errors"])
            self.assertTrue(exported["smoke_gate_pass"])


class IndependentRawVerificationTests(unittest.TestCase):
    PEER_IDS = AnalyzerAttemptTests.PEER_IDS

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="step27_raw_test_")
        self.addCleanup(self.temp.cleanup)
        self.trial = Path(self.temp.name) / "trial"
        self.trial.mkdir()

    @staticmethod
    def event(pid, number, kind, monotonic, peer, other="", **detail):
        return dict(pid=pid, event_number=number, kind=kind, monotonic=monotonic,
                    wall_time=1000.0 + monotonic, peer=peer, other=other, detail=detail)

    def write_trial(self, events, *, duration=10):
        write_json(self.trial / "config.json", {
            "peer_ids": self.PEER_IDS, "started_monotonic": 100.0, "duration": duration,
            "case": {"response": "reliable"},
        })
        (self.trial / "events.jsonl").write_text(
            "".join(json.dumps(event, sort_keys=True) + "\n" for event in events), encoding="utf-8")

    def test_raw_trial_deduplicates_groups_and_counts_actual_drain_starts(self):
        start_details = {}
        events = []
        specs = [
            ("X", "x-1", 100.0),
            ("X", "x-drain", 110.0),
            ("H0", "h0-1", 101.0),
            ("H1", "h1-failed", 102.0),
            ("H1", "h1-current", 103.0),
            ("H2", "h2-success", 109.0),
        ]
        for number, (name, step_id, monotonic) in enumerate(specs, 1):
            peer_id = self.PEER_IDS[name]
            detail = dict(name=name, sequence=number, step_id=step_id, value=number)
            start_details[(peer_id, step_id)] = detail
            events.append(self.event(1, number, "step_start", monotonic, peer_id, **detail))
            events.append(self.event(1, 100 + number, "step_result", monotonic + 1.0, peer_id,
                                     step_id=step_id, status="failed" if "failed" in step_id else "ok"))

        g1_members = {
            self.PEER_IDS["X"]: start_details[(self.PEER_IDS["X"], "x-1")],
            self.PEER_IDS["H0"]: start_details[(self.PEER_IDS["H0"], "h0-1")],
        }
        g1_detail = dict(group_id="g1", step_id="x-1", roster=list(g1_members), members=g1_members)
        events.extend([
            self.event(1, 201, "assembled", 108.0, self.PEER_IDS["X"], **g1_detail),
            self.event(1, 202, "assembled", 108.1, self.PEER_IDS["X"], **g1_detail),
        ])
        g2_members = {
            self.PEER_IDS["H1"]: start_details[(self.PEER_IDS["H1"], "h1-current")],
            self.PEER_IDS["H2"]: start_details[(self.PEER_IDS["H2"], "h2-success")],
        }
        events.append(self.event(1, 203, "assembled", 111.0, self.PEER_IDS["H2"],
                                 group_id="g2", step_id="h2-success",
                                 roster=list(g2_members), members=g2_members))
        events.append(self.event(1, 204, "trace_closed", 112.0, "", "",
                                 participants=list(self.PEER_IDS), pending=[], error=None))
        events.sort(key=lambda event: event["event_number"])
        self.write_trial(events)

        result = raw_verifier.raw_trial(self.trial)

        self.assertEqual(result["errors"], [])
        self.assertEqual(result["metrics"]["total_groups"], 2)
        self.assertEqual(result["metrics"]["x_submitted_attempts"], 2)
        self.assertEqual(result["metrics"]["x_led_attempts"], 1)
        self.assertEqual(result["attempts"][(self.PEER_IDS["X"], "x-drain")]
                         ["capped_latency_seconds"], 12.0)
        self.assertEqual(result["metrics"]["honest_submitted_attempts"], 4)
        self.assertEqual(result["metrics"]["honest_group_memberships_in_submission_window"], 1)
        self.assertAlmostEqual(
            result["metrics"]["honest_group_memberships_per_honest_peer_minute"], 6 / 7)
        self.assertEqual(result["attempts"][(self.PEER_IDS["H1"], "h1-failed")]
                         ["first_assembly_group_id"], None)
        self.assertEqual(result["attempts"][(self.PEER_IDS["H1"], "h1-current")]
                         ["first_assembly_group_id"], "g2")

    def test_raw_retry_requires_new_x_selection_and_exact_pair_in_same_open_search(self):
        x = self.PEER_IDS["X"]
        h0 = self.PEER_IDS["H0"]
        almost_old = math.nextafter(101.0, math.inf)
        events = [
            self.event(1, 1, "search_span_start", 1.0, x, search_id="x-search"),
            self.event(1, 2, "ad_started", 2.0, x, active=True, expiration=101.0),
            self.event(1, 3, "ad_started", 3.0, x, active=True, expiration=102.0),
            self.event(1, 4, "ad_started", 4.0, x, active=True, expiration=108.0),
            self.event(1, 5, "ad_started", 5.0, x, active=True, expiration=109.0),
            self.event(1, 6, "search_span_end", 12.0, x, search_id="x-search"),
            self.event(2, 1, "native_pop_selected", 5.5, h0, self.PEER_IDS["H1"],
                       selected_expiration=103.0,
                       past_attempts=[{"peer": x, "expiration": 101.0}]),
            self.event(2, 2, "native_pop_selected", 6.0, h0, x,
                       selected_expiration=108.0,
                       past_attempts=[{"peer": x, "expiration": 101.0},
                                      {"peer": x, "expiration": 102.0}]),
            self.event(2, 3, "native_pop_selected", 7.0, h0, x,
                       selected_expiration=109.0,
                       past_attempts=[{"peer": x, "expiration": 101.0},
                                      {"peer": x, "expiration": 102.0}]),
            self.event(2, 4, "native_pop_selected", 8.0, h0, x,
                       selected_expiration=110.0,
                       past_attempts=[{"peer": x, "expiration": almost_old}]),
        ]
        result = raw_verifier.raw_retries(events, {"peer_ids": self.PEER_IDS})

        self.assertEqual(set(result), {"2:2", "2:3"})
        self.assertEqual(result["2:2"]["old_values"], {101.0, 102.0})
        self.assertEqual(result["2:2"]["x_search_id"], "x-search")

    def test_raw_retry_rejects_old_and_new_pairs_split_across_searches(self):
        x = self.PEER_IDS["X"]
        h0 = self.PEER_IDS["H0"]
        events = [
            self.event(1, 1, "search_span_start", 1.0, x, search_id="x-old"),
            self.event(1, 2, "ad_started", 2.0, x, active=True, expiration=101.0),
            self.event(1, 3, "search_span_end", 3.0, x, search_id="x-old"),
            self.event(1, 4, "search_span_start", 4.0, x, search_id="x-new"),
            self.event(1, 5, "ad_started", 5.0, x, active=True, expiration=108.0),
            self.event(1, 6, "search_span_end", 10.0, x, search_id="x-new"),
            self.event(2, 1, "native_pop_selected", 6.0, h0, x,
                       selected_expiration=108.0,
                       past_attempts=[{"peer": x, "expiration": 101.0}]),
        ]

        self.assertEqual(raw_verifier.raw_retries(events, {"peer_ids": self.PEER_IDS}), {})


class CheckpointTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="step27_control_test_")
        self.addCleanup(self.temp.cleanup)
        self.suite = Path(self.temp.name) / "suite"
        self.suite.mkdir()
        self.jobs = make_suite(self.suite)

    def inspect(self):
        return sweep.inspect_suite(self.suite, verify_sources=False)

    def test_completed_trial_requires_post_shutdown_evidence(self):
        self.assertEqual(self.inspect()["counts"], {
            "completed": 0, "pending": 2, "interrupted": 0, "blocked": 0,
        })
        trial = write_completed_trial(self.suite, self.jobs[0], all_peers_shutdown=False)
        state = self.inspect()
        row = next(row for row in state["jobs"] if row["trial_dir"] == trial.name)
        self.assertEqual(row["status"], "blocked")
        self.assertIn("post-shutdown", row["error"])

    def test_completed_checkpoint_detects_mutated_trial_evidence(self):
        trial = write_completed_trial(self.suite, self.jobs[0])
        state = self.inspect()
        row = next(row for row in state["jobs"] if row["trial_dir"] == trial.name)
        self.assertEqual(row["status"], "completed")
        write_json(self.suite / "checkpoint.json", {
            "suite_sha256": state["suite_sha256"],
            "provenance_sha256": state["provenance_sha256"],
            "jobs": state["jobs"],
        })
        with (trial / "events.jsonl").open("a", encoding="utf-8") as stream:
            stream.write('{"kind":"tampered"}\n')
        with self.assertRaisesRegex(ValueError, "completed evidence changed"):
            self.inspect()

    def test_completed_trial_rejects_config_drift_after_file_hash_is_recomputed(self):
        trial = write_completed_trial(self.suite, self.jobs[0])
        config = json.loads((trial / "config.json").read_text())
        config["response_policy"] = (
            "unreliable" if self.jobs[0]["case"]["response"] == "reliable" else "reliable"
        )
        write_json(trial / "config.json", config)
        completion = json.loads((trial / "completion.json").read_text())
        completion["file_sha256"]["config.json"] = sweep.sha256(trial / "config.json")
        write_json(trial / "completion.json", completion)

        state = self.inspect()
        row = next(row for row in state["jobs"] if row["trial_dir"] == trial.name)
        self.assertEqual(row["status"], "blocked")
        self.assertIn("Step 27 outcome control differs: response_policy", row["error"])

    def test_interrupted_partial_analysis_is_archived_and_rerun_on_resume(self):
        interrupted_job, completed_job = self.jobs
        interrupted = self.suite / sweep.trial_name(interrupted_job)
        interrupted.mkdir()
        (interrupted / "events.jsonl").write_text('{"kind":"partial"}\n', encoding="utf-8")
        write_json(interrupted / "control_analysis.json", {"integrity_pass": False})
        write_json(interrupted / "summary.json", {"pending": ["X"], "error": "interrupted"})
        write_json(self.suite / "halt.json", {
            "trial": interrupted.name, "interrupted": True, "all_peers_shutdown": False,
        })
        completed = write_completed_trial(self.suite, completed_job)

        state = self.inspect()
        row = next(row for row in state["jobs"] if row["trial_dir"] == interrupted.name)
        self.assertEqual(row["status"], "interrupted")
        self.assertEqual(next(row for row in state["jobs"] if row["trial_dir"] == completed.name)["status"],
                         "completed")

        def run_trial(case, seed, path, duration, *, outcome):
            job = {"case": case, "seed": seed, "duration": duration}
            path.mkdir(parents=True, exist_ok=True)
            write_json(path / "config.json", valid_config(job))
            (path / "events.jsonl").write_text('{"kind":"trace_closed"}\n', encoding="utf-8")
            write_json(path / "summary.json", {"pending": [], "error": None})
            write_json(path / "analysis.json", {"control_plane_pass": True})
            return {"ok": True}

        runner = Mock(side_effect=run_trial)
        observation = SimpleNamespace(sweep=SimpleNamespace(
            run_trial=runner, trial_should_halt=lambda *_args: False,
        ))
        analyzer = types.ModuleType("analyze_step27_control_plane")
        analyzer.analyse_trial = lambda _path: {
            "integrity_pass": True, "integrity_errors": [],
            "summary_metrics": {
                "total_groups": 0, "x_led_groups": 0, "x_led_attempts": 0,
                "x_submitted_attempts": 0, "x_led_by_2s_count": 0,
                "honest_mean_capped_latency_seconds": 0.0,
            },
        }
        analyzer.export_suite = Mock(return_value={"complete": True, "integrity_pass": True})
        verifier = types.ModuleType("verify_step27_raw")
        verifier.verify_suite = Mock(return_value={"pass": True})
        with patch.object(sweep, "validate_sources"), \
             patch.object(sweep, "install_runtime", return_value=observation), \
             patch.object(sweep, "verify_trial_shutdown"), \
             patch.dict(sys.modules, {
                 "analyze_step27_control_plane": analyzer,
                 "verify_step27_raw": verifier,
             }), contextlib.redirect_stdout(io.StringIO()):
            sweep.run_suite(self.suite)

        runner.assert_called_once()
        verifier.verify_suite.assert_called_once_with(self.suite)
        state_after = json.loads((self.suite / "checkpoint.json").read_text())
        rerun = next(row for row in state_after["jobs"] if row["trial_dir"] == interrupted.name)
        self.assertEqual(rerun["status"], "completed")
        self.assertEqual(len(rerun["archives"]), 1)
        self.assertTrue(Path(rerun["archives"][0]["path"]).is_dir())
        self.assertTrue((self.suite / "raw_verification.json").exists())

    def test_interrupted_trial_is_archived_with_preserved_hashes(self):
        trial = self.suite / sweep.trial_name(self.jobs[0])
        trial.mkdir()
        raw = trial / "events.jsonl"
        raw.write_text('{"kind":"partial"}\n', encoding="utf-8")
        state = self.inspect()
        row = next(row for row in state["jobs"] if row["trial_dir"] == trial.name)
        self.assertEqual(row["status"], "interrupted")
        archive = sweep.archive_interrupted(self.suite, row["trial_dir"])
        archived = Path(archive["path"])
        self.assertFalse(trial.exists())
        self.assertEqual(sweep.sha256(archived / "events.jsonl"), archive["file_sha256"]["events.jsonl"])

    def test_stop_marker_returns_before_installing_runtime(self):
        (self.suite / "STOP_AFTER_CURRENT").touch()
        with patch.object(sweep, "inspect_suite", return_value={
                "valid": True, "jobs": [], "counts": {}}), \
             patch.object(sweep, "install_runtime", side_effect=AssertionError("runtime must stay offline")):
            sweep.run_suite(self.suite)
        self.assertTrue((self.suite / "checkpoint.json").exists())


if __name__ == "__main__":
    unittest.main()
