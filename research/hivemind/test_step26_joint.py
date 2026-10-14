"""Offline checks for the Step 26 joint sweep and factorial contrasts."""
from __future__ import annotations

import asyncio
from collections import defaultdict
import json
import multiprocessing
import random
from pathlib import Path
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

import analyze_step26_joint as analysis
import step26_joint_sweep as sweep


def write_json(path: Path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")


def valid_config(job):
    rng = random.Random(job["seed"])
    honest = [rng.uniform(0.0, 3.0) for _ in range(7)]
    offsets = dict(X=rng.uniform(0.0, 3.0), **{f"H{i}": value for i, value in enumerate(honest)})
    config = dict(
        case=job["case"], seed=job["seed"], duration=job["duration"],
        experiment="step26", outcome="grouping", instrumentation="detailed",
        timeout=12, request_timeout=3, tensor_size=128, honest_min=2, target=4,
        honest_ad_lifetime=5, allreduce_timeout=6, store_delay=0, group_bits="",
        allow_state_sharing=False, scheduled_time="native default", training_delay=sweep.TRAINING,
        network="eight-peer loopback", attempt_unit="step_start", event_transport="batched",
        trace_batch_size=32, duplicate_read_views=False, cached_reads_delayed=False,
        tensor_transfer_delayed=False,
        one_way_delay_seconds=job["case"]["one_way_delay_seconds"],
        network_delay_injected=job["case"]["one_way_delay_seconds"] > 0,
        peer_ids={name: f"peer-{i}" for i, name in enumerate(("X", *(f"H{i}" for i in range(7))))},
        initial_offsets=offsets,
    )
    return config


def write_completed_trial(suite: Path, job, *, all_peers_shutdown=True, integrity=True):
    trial = suite / sweep.trial_name(job)
    trial.mkdir(parents=True, exist_ok=True)
    write_json(trial / "config.json", valid_config(job))
    (trial / "events.jsonl").write_text('{"kind":"trace_closed"}\n', encoding="utf-8")
    write_json(trial / "summary.json", {"pending": [], "error": None})
    write_json(trial / "analysis.json", {"control_plane_pass": True})
    write_json(trial / "joint_analysis.json", {"integrity_pass": integrity})
    tracked = (*sweep.RAW_FILES, "analysis.json", "joint_analysis.json")
    write_json(trial / "completion.json", {
        "file_sha256": {name: sweep.sha256(trial / name) for name in tracked},
        "all_peers_shutdown": all_peers_shutdown,
    })
    return trial


def make_suite(path: Path, phase="smoke"):
    jobs = sweep.make_jobs(phase)
    write_json(path / "suite.json", {
        "experiment": "step26", "phase": phase, "jobs": jobs,
        "training_delay": sweep.TRAINING, "order_seed": 260026,
        "outcome": "grouping", "instrumentation": "detailed",
    })
    write_json(path / "provenance.json", {})
    return jobs


class DesignTests(unittest.TestCase):
    def test_pilot_is_exact_14_cell_factorial_plus_matched_controls(self):
        jobs = sweep.make_jobs("pilot")
        self.assertEqual(len(jobs), 14)
        self.assertEqual(jobs, sweep.make_jobs("pilot"))
        self.assertEqual({job["seed"] for job in jobs}, {2601})
        self.assertEqual({job["duration"] for job in jobs}, {30})

        cases = [job["case"] for job in jobs]
        refresh = [c for c in cases if c["mode"] == "refresh"]
        once = [c for c in cases if c["mode"] == "once"]
        passive = [c for c in cases if c["mode"] == "passive"]
        self.assertEqual(len(refresh), 8)
        self.assertEqual(len(once), 4)
        self.assertEqual(len(passive), 2)
        self.assertEqual(
            {(c["lifetime"], c["frequency"], c["one_way_delay_seconds"]) for c in refresh},
            {(u, f, d) for u in (1.0, 3.0) for f in (0.5, 2.0) for d in (0.0, 0.15)},
        )
        self.assertEqual(
            {(c["lifetime"], c["one_way_delay_seconds"]) for c in once},
            {(u, d) for u in (1.0, 3.0) for d in (0.0, 0.15)},
        )
        self.assertEqual({c["one_way_delay_seconds"] for c in passive}, {0.0, 0.15})
        self.assertTrue(all(c["target"] == 4 and c["minimum"] == 4 for c in cases))

    def test_smoke_contains_only_the_two_requested_gate_conditions(self):
        jobs = sweep.make_jobs("smoke")
        self.assertEqual(len(jobs), 2)
        self.assertEqual({job["seed"] for job in jobs}, {2600})
        self.assertEqual({job["duration"] for job in jobs}, {5})
        self.assertEqual(
            {(job["case"]["lifetime"], job["case"]["frequency"],
              job["case"]["one_way_delay_seconds"]) for job in jobs},
            {(1.0, 2.0, 0.0), (1.0, 2.0, 0.15)},
        )


class DelayWrapperTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.events = []
        self.observer = SimpleNamespace(emit=lambda kind, *args, **kwargs:
                                        self.events.append((kind, args, kwargs)))
        self.peer = SimpleNamespace(peer_id="receiver")
        self.context = SimpleNamespace(remote_id="sender")

    async def test_zero_delay_calls_no_sleep_and_preserves_unary_result(self):
        request = SimpleNamespace(keys=[b"target"])
        result = object()
        calls = []

        async def native(peer, actual_request, actual_context):
            calls.append((peer, actual_request, actual_context))
            return result

        with patch.object(sweep, "BASE", SimpleNamespace(CASE={"one_way_delay_seconds": 0.0})), \
             patch.object(sweep, "OBS", self.observer), \
             patch.object(sweep, "RPC_NUMBER", 0), \
             patch.object(sweep.asyncio, "sleep", new_callable=AsyncMock) as sleep:
            actual = await sweep.wrap_unary(native, "find")(
                self.peer, request, self.context
            )

        self.assertIs(actual, result)
        self.assertEqual(calls, [(self.peer, request, self.context)])
        sleep.assert_not_awaited()
        kinds = [kind for kind, _, _ in self.events]
        self.assertEqual(kinds.count("control_rpc_delay"), 2)
        self.assertEqual(kinds[-1], "control_rpc_end")
        delay_legs = [detail["leg"] for kind, _, detail in self.events if kind == "control_rpc_delay"]
        self.assertEqual(delay_legs, ["request", "response"])

    async def test_nonzero_unary_delay_brackets_original_handler(self):
        order = []
        request = SimpleNamespace(keys=[])

        async def fake_sleep(seconds):
            order.append(("sleep", seconds))

        async def native(peer, actual_request, actual_context):
            order.append(("native", None))
            return "ok"

        with patch.object(sweep, "BASE", SimpleNamespace(CASE={"one_way_delay_seconds": 0.15})), \
             patch.object(sweep, "OBS", self.observer), \
             patch.object(sweep, "RPC_NUMBER", 0), \
             patch.object(sweep.asyncio, "sleep", side_effect=fake_sleep):
            self.assertEqual(await sweep.wrap_unary(native, "store")(
                self.peer, request, self.context
            ), "ok")

        self.assertEqual(order, [("sleep", 0.15), ("native", None), ("sleep", 0.15)])
        delay_events = [detail for kind, _, detail in self.events if kind == "control_rpc_delay"]
        self.assertEqual([event["leg"] for event in delay_events], ["request", "response"])
        self.assertTrue(all(event["requested_delay"] == 0.15 for event in delay_events))

    async def test_injected_sleep_records_requested_delay_and_known_clock_duration(self):
        with patch.object(sweep, "OBS", self.observer), \
             patch.object(sweep.time, "monotonic", side_effect=[100.0, 100.15]), \
             patch.object(sweep.asyncio, "sleep", new_callable=AsyncMock) as sleep:
            await sweep.injected_sleep("rpc-known-clock", "response", 0.15)

        sleep.assert_awaited_once_with(0.15)
        kind, args, detail = self.events[0]
        self.assertEqual(kind, "control_rpc_delay")
        self.assertEqual(args, ("", ""))  # delay records have no peer identity
        self.assertEqual(detail["rpc_id"], "rpc-known-clock")
        self.assertEqual(detail["leg"], "response")
        self.assertEqual(detail["requested_delay"], 0.15)
        self.assertAlmostEqual(detail["actual_delay"], 0.15, places=12)
        self.assertFalse(detail["cancelled"])

    async def test_unary_cancellation_records_paired_end_without_response_delay(self):
        async def native(peer, request, context):
            raise asyncio.CancelledError()

        with patch.object(sweep, "BASE", SimpleNamespace(CASE={"one_way_delay_seconds": 0.0})), \
             patch.object(sweep, "OBS", self.observer), \
             patch.object(sweep, "RPC_NUMBER", 0):
            with self.assertRaises(asyncio.CancelledError):
                await sweep.wrap_unary(native, "find")(self.peer, object(), self.context)

        kinds = [kind for kind, _, _ in self.events]
        self.assertIn("control_rpc_handler_enter", kinds)
        self.assertNotIn("control_rpc_response", kinds)
        self.assertEqual(kinds[-1], "control_rpc_end")
        self.assertTrue(self.events[-1][2]["cancelled"])

    async def test_join_delays_both_responses_in_order(self):
        order = []
        response_a, response_b = SimpleNamespace(code=1), SimpleNamespace(code=2)

        class MessageCode:
            @staticmethod
            def Name(code):
                return f"CODE_{code}"

        async def fake_sleep(seconds):
            order.append(("sleep", seconds))

        async def native(peer, request, context):
            order.append(("native", "start"))
            try:
                yield response_a
                order.append(("native", "second"))
                yield response_b
            finally:
                order.append(("native", "closed"))

        with patch.object(sweep, "BASE", SimpleNamespace(
                CASE={"one_way_delay_seconds": 0.15}, averaging_pb2=SimpleNamespace(MessageCode=MessageCode))), \
             patch.object(sweep, "OBS", self.observer), \
             patch.object(sweep, "RPC_NUMBER", 0), \
             patch.object(sweep.asyncio, "sleep", side_effect=fake_sleep):
            stream = sweep.wrap_join(native)(self.peer, object(), self.context)
            actual = [response async for response in stream]

        self.assertEqual(actual, [response_a, response_b])
        self.assertIn(("native", "closed"), order)
        self.assertIn(("native", "second"), order)
        self.assertEqual([event["leg"] for kind, _, event in self.events
                          if kind == "control_rpc_delay"], ["request", "response", "response"])
        self.assertEqual([detail["response_index"] for kind, _, detail in self.events
                          if kind == "control_rpc_response"], [0, 1])
        self.assertFalse(self.events[-1][2]["cancelled"])

    async def test_join_consumer_cancellation_closes_native_stream(self):
        order = []
        response = SimpleNamespace(code=1)

        class MessageCode:
            @staticmethod
            def Name(code):
                return f"CODE_{code}"

        async def native(peer, request, context):
            try:
                yield response
                await asyncio.Future()
            finally:
                order.append("native closed")

        with patch.object(sweep, "BASE", SimpleNamespace(
                CASE={"one_way_delay_seconds": 0.0}, averaging_pb2=SimpleNamespace(MessageCode=MessageCode))), \
             patch.object(sweep, "OBS", self.observer), \
             patch.object(sweep, "RPC_NUMBER", 0):
            stream = sweep.wrap_join(native)(self.peer, object(), self.context)
            self.assertIs(await anext(stream), response)
            await stream.aclose()

        self.assertEqual(order, ["native closed"])
        self.assertEqual(self.events[-1][0], "control_rpc_end")
        self.assertTrue(self.events[-1][2]["cancelled"])


class PublishOnceTests(unittest.IsolatedAsyncioTestCase):
    async def test_active_ad_is_published_once_then_withdrawn_at_search_cancellation(self):
        events, calls = [], []
        published = asyncio.Event()

        class KeyManager:
            current_key = "pool.key"

            async def declare_averager(self, *args, **kwargs):
                calls.append((args, kwargs))
                if not kwargs:
                    published.set()

        class Expiration:
            def set(self):
                events.append(("expiration_set",))

        leader = SimpleNamespace(
            lock_declare=asyncio.Lock(), running=asyncio.Event(), search_end_time=200.0,
            declared_group_key=None, declared_expiration_time=float("inf"),
            declared_expiration=Expiration(), peer_id="X", _s25_search_id="X:search1",
        )
        leader.running.set()
        key_manager = KeyManager()

        logical_clock = [100.0]
        with patch.object(sweep, "BASE", SimpleNamespace(
                CASE={"lifetime": 1.0}, get_dht_time=lambda: logical_clock[0])), \
             patch.object(sweep, "emit", lambda kind, *args, **kwargs:
                          events.append((kind, args, kwargs))):
            task = asyncio.create_task(sweep.publish_once(leader, object(), key_manager))
            await asyncio.wait_for(published.wait(), timeout=1)
            logical_clock[0] = 105.0  # the advertised expiration is now in the past
            leader.running.clear()    # model a native pause and resume within this search
            await asyncio.sleep(0)
            leader.running.set()
            await asyncio.sleep(0)
            self.assertEqual(len(calls), 1)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0], (("pool.key", "X", 101.0), {}))
        self.assertEqual(calls[1], (("pool.key", "X", 101.0), {"looking_for_group": False}))
        publications = [event for event in events if event[0] == "once_publication"]
        self.assertEqual(len(publications), 1)
        self.assertIsNone(leader.declared_group_key)
        self.assertEqual(leader.declared_expiration_time, float("inf"))


class FakeChild:
    def __init__(self, pid, alive, *, shutdown_stops=True, terminate_stops=True, kill_stops=True):
        self.pid = pid
        self.alive = alive
        self.shutdown_stops = shutdown_stops
        self.terminate_stops = terminate_stops
        self.kill_stops = kill_stops
        self.calls = []

    def join(self, timeout=None):
        self.calls.append(("join", timeout))

    def is_alive(self):
        return self.alive

    def shutdown(self):
        self.calls.append(("shutdown",))
        if self.shutdown_stops:
            self.alive = False

    def terminate(self):
        self.calls.append(("terminate",))
        if self.terminate_stops:
            self.alive = False

    def kill(self):
        self.calls.append(("kill",))
        if self.kill_stops:
            self.alive = False


class ShutdownTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="step26_shutdown_test_")
        self.addCleanup(self.temp.cleanup)

    def test_reaper_ignores_preexisting_child_and_joins_exited_trial_child(self):
        preexisting = FakeChild(101, True)
        exited_trial = FakeChild(202, False)
        with patch.object(multiprocessing, "active_children", return_value=[preexisting, exited_trial]):
            sweep.verify_trial_shutdown({preexisting.pid})

        self.assertEqual(preexisting.calls, [])
        self.assertEqual(exited_trial.calls, [("join", 1.0)])

    def test_reaper_cleans_lingering_child_then_raises_an_integrity_stop(self):
        preexisting = FakeChild(111, True)
        lingering = FakeChild(222, True)
        with patch.object(multiprocessing, "active_children", return_value=[preexisting, lingering]):
            with self.assertRaisesRegex(RuntimeError, "additional shutdown cleanup"):
                sweep.verify_trial_shutdown({preexisting.pid})

        self.assertEqual(preexisting.calls, [])
        self.assertEqual(lingering.calls, [("join", 1.0), ("shutdown",), ("join", 1.0)])
        self.assertFalse(lingering.alive)

    def run_suite_with_fake_child(self, *, child_stops):
        suite = Path(self.temp.name) / ("stopped" if child_stops else "still_alive")
        suite.mkdir()
        jobs = make_suite(suite)
        job = jobs[0]
        state = {
            "valid": True,
            "suite": str(suite),
            "suite_sha256": "suite-hash",
            "provenance_sha256": "provenance-hash",
            "phase": "smoke",
            "jobs": [{"job": job, "trial_dir": sweep.trial_name(job), "status": "pending"}],
            "counts": {"completed": 0, "pending": 1, "interrupted": 0, "blocked": 0},
        }
        preexisting = FakeChild(301, True)
        trial_child = FakeChild(302, True, shutdown_stops=child_stops,
                                terminate_stops=child_stops, kill_stops=child_stops)
        runner = Mock(side_effect=RuntimeError("synthetic trial failure"))
        observation = SimpleNamespace(sweep=SimpleNamespace(run_trial=runner))

        # run_suite snapshots existing children, the reaper sees a new trial
        # child, and its error handler reads the live list once more for halt.json.
        child_snapshots = ([preexisting], [preexisting, trial_child],
                           [preexisting] if child_stops else [preexisting, trial_child])
        with patch.object(sweep, "inspect_suite", return_value=state), \
             patch.object(sweep, "validate_sources"), \
             patch.object(sweep, "install_runtime", return_value=observation), \
             patch.object(multiprocessing, "active_children", side_effect=child_snapshots):
            with self.assertRaises(RuntimeError):
                sweep.run_suite(suite)
        halt = json.loads((suite / "halt.json").read_text())
        return halt, preexisting, trial_child, runner

    def test_halt_marks_shutdown_true_after_cleanup_and_preserves_preexisting_child(self):
        halt, preexisting, trial_child, runner = self.run_suite_with_fake_child(child_stops=True)
        self.assertTrue(halt["all_peers_shutdown"])
        self.assertEqual(preexisting.calls, [])
        self.assertFalse(trial_child.alive)
        runner.assert_called_once()

    def test_halt_marks_shutdown_false_when_trial_child_survives_cleanup(self):
        halt, preexisting, trial_child, runner = self.run_suite_with_fake_child(child_stops=False)
        self.assertFalse(halt["all_peers_shutdown"])
        self.assertEqual(preexisting.calls, [])
        self.assertTrue(trial_child.alive)
        self.assertEqual([call[0] for call in trial_child.calls],
                         ["join", "shutdown", "join", "terminate", "join", "kill", "join"])
        runner.assert_called_once()


class CheckpointTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="step26_joint_test_")
        self.addCleanup(self.temp.cleanup)
        self.suite = Path(self.temp.name) / "suite"
        self.suite.mkdir()
        self.jobs = make_suite(self.suite)

    def inspect(self):
        return sweep.inspect_suite(self.suite, verify_sources=False)

    def test_empty_suite_marks_every_job_pending_and_completed_requires_shutdown(self):
        state = self.inspect()
        self.assertEqual(state["counts"], {"completed": 0, "pending": 2, "interrupted": 0, "blocked": 0})
        self.assertTrue(state["valid"])

        trial = write_completed_trial(self.suite, self.jobs[0], all_peers_shutdown=False)
        state = self.inspect()
        row = next(row for row in state["jobs"] if row["trial_dir"] == trial.name)
        self.assertEqual(row["status"], "blocked")
        self.assertIn("post-shutdown", row["error"])

    def test_valid_completed_trial_is_revalidated_and_mutation_is_rejected(self):
        trial = write_completed_trial(self.suite, self.jobs[0])
        state = self.inspect()
        row = next(row for row in state["jobs"] if row["trial_dir"] == trial.name)
        self.assertEqual(row["status"], "completed")
        self.assertIn("completion.json", row["file_sha256"])

        write_json(self.suite / "checkpoint.json", {
            "suite_sha256": state["suite_sha256"],
            "provenance_sha256": state["provenance_sha256"],
            "jobs": state["jobs"],
        })
        with (trial / "events.jsonl").open("a", encoding="utf-8") as stream:
            stream.write('{"kind":"tampered"}\n')
        with self.assertRaisesRegex(ValueError, "completed evidence changed"):
            self.inspect()

    def test_completed_stamp_without_closed_trace_is_blocked(self):
        trial = write_completed_trial(self.suite, self.jobs[0])
        (trial / "events.jsonl").write_text('{"kind":"step_start"}\n', encoding="utf-8")
        completion = json.loads((trial / "completion.json").read_text())
        completion["file_sha256"]["events.jsonl"] = sweep.sha256(trial / "events.jsonl")
        write_json(trial / "completion.json", completion)

        state = self.inspect()
        row = next(row for row in state["jobs"] if row["trial_dir"] == trial.name)
        self.assertEqual(row["status"], "blocked")
        self.assertIn("trace_closed", row["error"])

    def test_interrupted_trials_are_classified_and_archived_before_rerun(self):
        trial = self.suite / sweep.trial_name(self.jobs[0])
        trial.mkdir()
        (trial / "events.jsonl").write_text('{"kind":"partial"}\n', encoding="utf-8")
        state = self.inspect()
        row = next(row for row in state["jobs"] if row["trial_dir"] == trial.name)
        self.assertEqual(row["status"], "interrupted")

        archive = sweep.archive_interrupted(self.suite, row["trial_dir"])
        archived_path = Path(archive["path"])
        self.assertFalse(trial.exists())
        self.assertTrue(archived_path.is_dir())
        self.assertIn("events.jsonl", archive["file_sha256"])
        self.assertEqual(sweep.sha256(archived_path / "events.jsonl"),
                         archive["file_sha256"]["events.jsonl"])

    def test_closed_integrity_failure_is_blocked_instead_of_retried(self):
        trial = self.suite / sweep.trial_name(self.jobs[0])
        trial.mkdir()
        write_json(trial / "joint_analysis.json", {"integrity_pass": False})
        state = self.inspect()
        row = next(row for row in state["jobs"] if row["trial_dir"] == trial.name)
        self.assertEqual(row["status"], "blocked")

    def test_stop_marker_is_detected_without_starting_jobs(self):
        self.assertFalse(sweep.stop_requested(self.suite))
        (self.suite / "STOP_AFTER_CURRENT").touch()
        self.assertTrue(sweep.stop_requested(self.suite))

    def test_stop_marker_returns_before_runtime_install_or_trial_start(self):
        (self.suite / "STOP_AFTER_CURRENT").touch()
        state = {"valid": True, "jobs": [], "counts": {}}
        with patch.object(sweep, "inspect_suite", return_value=state), \
             patch.object(sweep, "install_runtime", side_effect=AssertionError("runtime must stay offline")):
            sweep.run_suite(self.suite)
        self.assertTrue((self.suite / "checkpoint.json").exists())

    def test_completed_jobs_are_skipped_without_invoking_trial_runner(self):
        jobs = self.jobs
        rows = [{"job": job, "trial_dir": sweep.trial_name(job), "status": "completed",
                 "file_sha256": {"events.jsonl": "fixture-hash"}} for job in jobs]
        state = {"valid": True, "experiment": "step26", "suite": str(self.suite),
                 "phase": "smoke", "jobs": rows,
                 "counts": {"completed": len(rows), "pending": 0, "interrupted": 0, "blocked": 0},
                 "suite_sha256": "suite-hash", "provenance_sha256": "provenance-hash"}
        trial_runner = Mock(side_effect=AssertionError("completed job reran"))
        observer = SimpleNamespace(sweep=SimpleNamespace(run_trial=trial_runner))
        with patch.object(sweep, "inspect_suite", return_value=state), \
             patch.object(sweep, "install_runtime", return_value=observer), \
             patch.object(analysis, "export_suite", return_value={"complete": True,
                                                                   "integrity_pass": True}):
            sweep.run_suite(self.suite)
        trial_runner.assert_not_called()

    def test_atomic_json_roundtrips_and_removes_temporary_files(self):
        target = self.suite / "atomic.json"
        sweep.atomic_json(target, {"nested": [1, 2], "ok": True})
        self.assertEqual(json.loads(target.read_text()), {"nested": [1, 2], "ok": True})
        self.assertEqual(list(self.suite.glob("*.tmp")), [])
        self.assertEqual(sweep.sha256(target), sweep.sha256(target))

    def test_source_validation_detects_live_or_snapshot_drift(self):
        source_root = Path(self.temp.name) / "source"
        snapshot_root = self.suite / "harness"
        source_root.mkdir()
        snapshot_root.mkdir()
        live = source_root / "fixture.py"
        snap = snapshot_root / "fixture.py"
        live.write_text("same source\n", encoding="utf-8")
        snap.write_text("same source\n", encoding="utf-8")
        digest = sweep.sha256(live)
        provenance = {"harness_sha256": {"fixture.py": digest}, "source": str(source_root),
                      "source_sha256": {}, "runtime_files_sha256": {}}
        with patch.object(sweep, "FROZEN_FILES", ("fixture.py",)), \
             patch.object(sweep, "ROOT", source_root):
            sweep.validate_sources(self.suite, provenance)
            live.write_text("drift\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "frozen source drift"):
                sweep.validate_sources(self.suite, provenance)
            live.write_text("same source\n", encoding="utf-8")
            snap.write_text("snapshot drift\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "frozen source drift"):
                sweep.validate_sources(self.suite, provenance)

    def test_checkpoint_metadata_hash_drift_is_rejected(self):
        state = self.inspect()
        write_json(self.suite / "checkpoint.json", {
            "suite_sha256": state["suite_sha256"],
            "provenance_sha256": state["provenance_sha256"],
            "jobs": state["jobs"],
        })
        write_json(self.suite / "provenance.json", {"changed": True})
        with self.assertRaisesRegex(ValueError, "checkpoint metadata drift"):
            self.inspect()


class InteractionTests(unittest.TestCase):
    @staticmethod
    def cells(x_rate_fn):
        cells = {}
        for u in (1.0, 3.0):
            for f in (0.5, 2.0):
                for d in (0.0, 0.15):
                    value = x_rate_fn(u, f, d)
                    cells[(u, f, d)] = {
                        "x_led_rate": {"rate": value, "denominator": 10},
                        "x_led_by_2s_rate": {"rate": value, "denominator": 10},
                        "first_eligible_choice_rate": {"rate": value, "denominator": 10},
                    }
        return cells

    def test_three_way_interaction_has_expected_difference_in_differences(self):
        # Refreshing faster helps only for short lifetime under injected delay.
        def effect(u, f, d):
            return 0.1 + (0.2 if (u == 1.0 and f == 2.0 and d == 0.15) else 0.0)

        rows = analysis.calculate_interactions(self.cells(effect))
        three_way = [row for row in rows if row.get("kind") == "u_x_f_x_delay"]
        self.assertTrue(three_way)
        self.assertTrue(all(abs(row["estimate"] - 0.2) < 1e-12 for row in three_way
                            if row["metric"] == "x_led_rate"))

    def test_missing_cells_and_small_denominators_are_reported_as_unavailable(self):
        cells = self.cells(lambda _u, _f, _d: 0.5)
        del cells[(3.0, 0.5, 0.15)]
        rows = analysis.calculate_interactions(cells)
        missing = [row for row in rows if row.get("status") == "missing_cells"]
        self.assertTrue(missing)
        self.assertTrue(all(row["estimate"] is None for row in missing))

        low_n = self.cells(lambda u, f, d: 0.1 +
                           (0.2 if (u == 1.0 and f == 2.0 and d == 0.15) else 0.0))
        low_n[(1.0, 2.0, 0.15)]["x_led_rate"]["denominator"] = 7
        gated = [row for row in analysis.calculate_interactions(low_n)
                 if row.get("metric") == "x_led_rate" and row.get("minimum_denominator", 0) < 8]
        self.assertTrue(any(row["estimate"] is not None and abs(row["estimate"]) >= .15
                            for row in gated))
        self.assertTrue(all(not row["flag"] for row in gated))


class AnalyzerIntegrityTests(unittest.TestCase):
    @staticmethod
    def rpc_events(request_leg="request"):
        common = dict(pid=7, peer="destination", other="source", wall_time=100.0)
        rpc_id = "7:rpc1"
        return [
            dict(common, kind="control_rpc_start", event_number=1, monotonic=10.0,
                 detail={"rpc_id": rpc_id, "rpc_type": "find", "delay_seconds": 0.15,
                         "key_hashes": ["ab12"]}),
            dict(common, kind="control_rpc_delay", event_number=2, monotonic=10.15,
                 detail={"rpc_id": rpc_id, "leg": request_leg, "requested_delay": 0.15,
                         "actual_delay": 0.15, "cancelled": False}),
            dict(common, kind="control_rpc_handler_enter", event_number=3, monotonic=10.16,
                 detail={"rpc_id": rpc_id}),
            dict(common, kind="control_rpc_delay", event_number=4, monotonic=10.30,
                 detail={"rpc_id": rpc_id, "leg": "response", "requested_delay": 0.15,
                         "actual_delay": 0.15, "cancelled": False}),
            dict(common, kind="control_rpc_response", event_number=5, monotonic=10.31,
                 detail={"rpc_id": rpc_id, "response_index": 0}),
            dict(common, kind="control_rpc_end", event_number=6, monotonic=10.32,
                 detail={"rpc_id": rpc_id, "cancelled": False, "error": None}),
        ]

    @staticmethod
    def grouped(events):
        by_kind, by_ref = defaultdict(list), {}
        for event in events:
            by_kind[event["kind"]].append(event)
            by_ref[f"{event['pid']}:{event['event_number']}"] = event
        return by_kind, by_ref

    def test_delay_trace_with_wrong_leg_fails_integrity(self):
        events = self.rpc_events(request_leg="outbound")
        by_kind, by_ref = self.grouped(events)
        result = analysis._rpc_integrity(
            {"one_way_delay_seconds": 0.15}, {}, events, by_kind, by_ref, {"reads": []}
        )
        self.assertFalse(result["pass_"])
        self.assertTrue(any("invalid delay leg" in error for error in result["errors"]))

    def test_once_policy_accepts_one_ad_and_flags_duplicate_publication(self):
        def fixture(duplicate=False):
            events = [
                dict(pid=8, event_number=1, peer="X", other="", kind="search_span_start",
                     monotonic=10.0, wall_time=100.0, detail={}),
                dict(pid=8, event_number=2, peer="X", other="", kind="once_publication",
                     monotonic=11.0, wall_time=100.0,
                     detail={"search_id": "X:search1", "expiration": 101.0}),
                dict(pid=8, event_number=3, peer="X", other="", kind="ad_started",
                     monotonic=11.0, wall_time=100.0,
                     detail={"active": True, "expiration": 101.0}),
                dict(pid=8, event_number=4, peer="X", other="", kind="search_span_end",
                     monotonic=20.0, wall_time=109.0, detail={}),
            ]
            if duplicate:
                events[2]["monotonic"] = 12.0
                events.insert(3, dict(pid=8, event_number=5, peer="X", other="", kind="once_publication",
                                      monotonic=12.0, wall_time=100.25,
                                      detail={"search_id": "X:search1", "expiration": 101.0}))
                events.insert(4, dict(pid=8, event_number=6, peer="X", other="", kind="ad_started",
                                      monotonic=12.0, wall_time=100.25,
                                      detail={"active": True, "expiration": 101.0}))
            by_kind, by_ref = AnalyzerIntegrityTests.grouped(events)
            native = {
                "per_peer": {"X": {"peer_id": "peer-x"}},
                "searches": [{"peer_id": "peer-x", "search_id": "X:search1", "step_id": "step-x",
                              "start_event_ref": "8:1", "end_event_ref": "8:4",
                              "start_monotonic": 10.0, "end_monotonic": 20.0}],
            }
            # The observed peer id must match the native reconstructed identity.
            for event in events:
                if event["kind"] in ("search_span_start", "search_span_end", "once_publication", "ad_started"):
                    event["peer"] = "peer-x"
            return events, by_kind, by_ref, native

        case = {"mode": "once", "lifetime": 1.0}
        events, by_kind, by_ref, native = fixture()
        passing = analysis._policy_integrity(case, {}, events, by_kind, by_ref, native)
        self.assertTrue(passing["pass_"], passing["errors"])

        events, by_kind, by_ref, native = fixture(duplicate=True)
        failing = analysis._policy_integrity(case, {}, events, by_kind, by_ref, native)
        self.assertFalse(failing["pass_"])
        self.assertTrue(any("exactly one active advertisement" in error or "exactly one once_publication" in error
                            for error in failing["errors"]))


if __name__ == "__main__":
    unittest.main()
