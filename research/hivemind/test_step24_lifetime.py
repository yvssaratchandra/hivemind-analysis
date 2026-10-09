"""Focused corruption checks for the Step 24 lifetime trace analyzer."""

import copy
import hashlib
import json
from pathlib import Path
import random
import shutil
import tempfile
import unittest

from analyze_step24_lifetime import analyse_suite, analyse_trial, _assembly_analysis


NAMES = ["X"] + [f"H{i}" for i in range(7)]
IDS = {name: name.lower() for name in NAMES}
GROUP_NAMES = NAMES[:4]
SEED = 2400


def training_parameters():
    import math
    sigma = math.sqrt(math.log(1.36))
    return dict(mu=math.log(1.5) - sigma * sigma / 2, sigma=sigma, lower=0.1, upper=4.0)


def user_row(name, sequence=1, value=None):
    if value is None:
        value = float((7 if name == "X" else int(name[1:])) + 10 * sequence)
    return dict(name=name, sequence=sequence, step_id=f"{name}:{sequence}", value=float(value))


def make_trial(seed=SEED, training=False):
    config = dict(
        case=dict(name="range_1_2", mode="refresh", minimum=4, target=4,
                  lifetime=2.0, frequency=1.0),
        seed=seed,
        duration=10,
        peer_ids=IDS.copy(),
        tensor_size=128,
        timeout=12,
        training_delay=training_parameters(),
        initial_offsets={name: 0.0 for name in NAMES},
        started_monotonic=1000.0,
        started_wall=5000.0,
    )
    events, outcomes, starts_by_step, finishes = [], [], {}, {}

    def add(kind, peer="", other="", monotonic=None, wall_time=None, **detail):
        tick = len(events) + 1
        if monotonic is None:
            monotonic = 1000.0 + tick / 1000
        if wall_time is None:
            wall_time = 5000.0 + monotonic - 1000.0
        events.append(dict(kind=kind, peer=peer, other=other, pid=1, event_number=tick,
                           monotonic=monotonic, wall_time=wall_time, detail=detail))
        return events[-1]

    for index, name in enumerate(NAMES):
        row = user_row(name)
        starts_by_step[row["step_id"]] = row
        add("step_start", IDS[name], monotonic=1000.1 + index * .01, **row)

    member_map = {IDS[name]: copy.deepcopy(starts_by_step[f"{name}:1"]) for name in GROUP_NAMES}
    expected = sum(data["value"] for data in member_map.values()) / len(member_map)
    group_id = "g1"
    add("assembled", IDS["X"], monotonic=1000.3, step_id="X:1", group_id=group_id,
        roster=[IDS[name] for name in GROUP_NAMES], user=member_map[IDS["X"]], members=member_map)
    for index, name in enumerate(GROUP_NAMES):
        add("allreduce_start", IDS[name], monotonic=1000.4 + index * .001,
            step_id=f"{name}:1", group_id=group_id, members=member_map,
            actual_input=[member_map[IDS[name]]["value"]] * 128)
    for index, name in enumerate(GROUP_NAMES):
        add("allreduce_end", IDS[name], monotonic=1000.5 + index * .001,
            step_id=f"{name}:1", group_id=group_id, error=None,
            actual_output=[expected] * 128)

    for index, name in enumerate(NAMES):
        step_id = f"{name}:1"
        finished = 1001.0 + index * .01
        if name in GROUP_NAMES:
            row = dict(name=name, sequence=1, step_id=step_id, finished=finished,
                       status="completed", members=copy.deepcopy(member_map), expected=expected,
                       actual=[expected] * 128, correct=True)
        else:
            row = dict(name=name, sequence=1, step_id=step_id, finished=finished,
                       status="no_group")
        outcomes.append(row)
        event = add("step_result", IDS[name], monotonic=finished + .0001, **row)
        finishes[step_id] = (finished, event)

    add("ad_started", IDS["X"], other="logical-group-key", monotonic=1000.2,
        active=True, expiration=5005.0)
    add("ad_finished", IDS["X"], other="logical-group-key", monotonic=1000.21,
        active=True, accepted=True, expiration=5005.0)
    add("holder_store", IDS["H0"], IDS["X"], monotonic=1000.22,
        wall_time=5000.22, subkey=IDS["X"], key="deadbeef", active=True,
        expiration=5005.0, received=5000.22, accepted=True)
    add("native_leader_wait", IDS["X"], IDS["H0"], monotonic=1000.23,
        wall_time=5000.23, step_id="X:1", captured_expiration=5003.0,
        timer_started_wall=5001.0, timeout=2.0, followers=[IDS["H0"]])

    if training:
        params = config["training_delay"]
        delay_rng = random.Random(seed * 100)
        delay = min(4.0, max(.1, delay_rng.lognormvariate(params["mu"], params["sigma"])))
        finished = finishes["X:1"][0]
        ready = finished + delay
        add("training_scheduled", IDS["X"], monotonic=finished + .0002,
            name="X", sequence=1, step_id="X:1", finished=finished, delay=delay, ready=ready)
        next_start = max(ready, finished + .001) + .01
        next_row = user_row("X", sequence=2)
        add("step_start", IDS["X"], monotonic=next_start, **next_row)
        starts_by_step[next_row["step_id"]] = next_row
        config["started_monotonic"] = 1000.0
        pending = ["X:2"]
    else:
        pending = []
    add("trace_closed", monotonic=1010.0, participants=NAMES, pending=pending, error=None)
    summary = dict(outcomes=outcomes, pending=pending, duration=config["duration"], error=None)
    return config, summary, events


def write_trial(path, data):
    config, summary, events = data
    path.mkdir(parents=True, exist_ok=True)
    (path / "config.json").write_text(json.dumps(config))
    (path / "summary.json").write_text(json.dumps(summary))
    (path / "events.jsonl").write_text("".join(json.dumps(event) + "\n" for event in events))


def rewrite_event(events, predicate, mutate):
    for event in events:
        if predicate(event):
            mutate(event)
            return
    raise AssertionError("fixture event not found")


class Step24AnalyzerTests(unittest.TestCase):
    def analyse_data(self, data):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)
            write_trial(path, data)
            return analyse_trial(path)

    def test_reconstructs_verified_groups_and_leadership(self):
        result = self.analyse_data(make_trial())
        self.assertEqual(result["total_assembled_groups"], 1)
        self.assertEqual(result["x_assembled_leads"], 1)
        self.assertEqual(result["total_verified_groups"], 1)
        self.assertEqual(result["x_verified_leads"], 1)
        self.assertEqual(result["x_leadership_share"], 1.0)
        self.assertEqual(result["mean_x_verified_group_size"], 4)
        self.assertEqual((result["x_ad_attempts"], result["x_ad_writes"]), (1, 1))
        self.assertEqual(result["timing"]["native_leader_waits"][0]["timeout"], 2.0)

    def test_training_delay_seed_and_next_start_are_checked(self):
        result = self.analyse_data(make_trial(training=True))
        self.assertEqual(result["timing"]["training"]["delay_count"], 1)
        self.assertEqual(result["pending"], ["X:2"])

        data = make_trial(training=True)
        rewrite_event(data[2], lambda event: event["kind"] == "training_scheduled",
                      lambda event: event["detail"].update(delay=event["detail"]["delay"] + .25,
                                                            ready=event["detail"]["ready"] + .25))
        with self.assertRaisesRegex(ValueError, "seeded training delay"):
            self.analyse_data(data)

    def test_member_input_conflict_is_returned_as_integrity_error(self):
        data = make_trial()
        def change(event):
            event["detail"]["members"][IDS["H0"]]["value"] += 1
        rewrite_event(data[2], lambda event: event["kind"] == "allreduce_start" and event["peer"] == IDS["H0"], change)
        result = self.analyse_data(data)
        self.assertTrue(any("member input conflicts" in error for error in result["integrity_errors"]))

    def test_incorrect_tensor_is_reported_without_raising(self):
        data = make_trial()
        def corrupt(event):
            event["detail"].update(actual=[999.0] * 128, correct=False)
        rewrite_event(data[2], lambda event: event["kind"] == "step_result" and event["detail"]["step_id"] == "X:1",
                      corrupt)
        rewrite_event(data[2], lambda event: event["kind"] == "allreduce_end" and event["peer"] == IDS["X"],
                      lambda event: event["detail"].update(actual_output=[999.0] * 128))
        data[1]["outcomes"][0].update(actual=[999.0] * 128, correct=False)
        result = self.analyse_data(data)
        self.assertIn("X:1", result["incorrect_steps"])
        self.assertFalse(result["groups"][0]["verified"])

    def test_grouping_outcome_keeps_assembly_counts_after_tensor_failure(self):
        data = make_trial()
        data[0]["outcome"] = "grouping"
        rewrite_event(data[2], lambda event: event["kind"] == "step_result" and event["detail"]["step_id"] == "X:1",
                      lambda event: event["detail"].update(actual=[999.0] * 128, correct=False))
        rewrite_event(data[2], lambda event: event["kind"] == "allreduce_end" and event["peer"] == IDS["X"],
                      lambda event: event["detail"].update(actual_output=[999.0] * 128))
        data[1]["outcomes"][0].update(actual=[999.0] * 128, correct=False)
        result = self.analyse_data(data)
        self.assertEqual(result["outcome"], "grouping")
        self.assertEqual((result["total_assembled_groups"], result["x_assembled_leads"]), (1, 1))
        self.assertTrue(result["control_plane_pass"])
        self.assertFalse(result["averaging_pass"])

    def test_grouping_control_roster_and_leader_spoofs_fail(self):
        for mutate in (
            lambda event: event["detail"].update(roster=event["detail"]["roster"][1:]),
            lambda event: event.update(peer=IDS["H0"]),
        ):
            data = make_trial()
            data[0]["outcome"] = "grouping"
            rewrite_event(data[2], lambda event: event["kind"] == "assembled", mutate)
            result = self.analyse_data(data)
            self.assertFalse(result["control_plane_pass"])
            self.assertTrue(result["control_integrity_errors"])

    def test_summary_tamper_and_missing_trace_closure_raise(self):
        data = make_trial()
        data[1]["outcomes"][0]["finished"] += 1
        with self.assertRaisesRegex(ValueError, "summary outcomes"):
            self.analyse_data(data)
        data = make_trial()
        data[2][-1]["kind"] = "other"
        with self.assertRaisesRegex(ValueError, "trace is not closed"):
            self.analyse_data(data)
        data = make_trial()
        rewrite_event(data[2], lambda event: event["kind"] == "step_result" and event["detail"]["step_id"] == "X:1",
                      lambda event: event["detail"].update(actual=[12.5] * 127))
        data[1]["outcomes"][0]["actual"] = [12.5] * 127
        with self.assertRaisesRegex(ValueError, "wrong tensor size"):
            self.analyse_data(data)

    def test_overlapping_allreduces_for_same_step_are_integrity_error(self):
        data = make_trial()
        config, summary, events = data
        closure = events.pop()
        second_map = {IDS[name]: user_row(name)
                      for name in ("X", "H0", "H1", "H3")}
        mean = sum(row["value"] for row in second_map.values()) / 4
        next_number = max(event["event_number"] for event in events) + 1
        def add(kind, peer, monotonic, **detail):
            nonlocal next_number
            event = dict(kind=kind, peer=peer, other="", pid=1, event_number=next_number,
                         monotonic=monotonic, wall_time=5000 + monotonic - 1000, detail=detail)
            next_number += 1
            events.append(event)
        add("assembled", IDS["X"], 1000.34, step_id="X:1", group_id="g2",
            roster=[IDS[name] for name in ("X", "H0", "H1", "H3")],
            user=second_map[IDS["X"]], members=second_map)
        add("allreduce_start", IDS["X"], 1000.35, step_id="X:1", group_id="g2",
            members=second_map, actual_input=[17.0] * 128)
        add("allreduce_end", IDS["X"], 1000.45, step_id="X:1", group_id="g2",
            error=None, actual_output=[mean] * 128)
        events.append(closure)
        for number, event in enumerate(events, 1):
            event["event_number"] = number
        result = self.analyse_data(data)
        self.assertTrue(any("simultaneous allreduces" in error for error in result["integrity_errors"]))

    def test_sequential_failed_group_retry_is_not_overlap(self):
        data = make_trial()
        config, summary, events = data
        closure = events.pop()
        second_map = {IDS[name]: user_row(name)
                      for name in ("X", "H0", "H1", "H3")}
        next_number = max(event["event_number"] for event in events) + 1
        def add(kind, peer, monotonic, **detail):
            nonlocal next_number
            events.append(dict(kind=kind, peer=peer, other="", pid=1, event_number=next_number,
                               monotonic=monotonic, wall_time=5000 + monotonic - 1000, detail=detail))
            next_number += 1
        add("assembled", IDS["X"], 1000.6, step_id="X:1", group_id="failed-retry",
            roster=[IDS[name] for name in ("X", "H0", "H1", "H3")],
            user=second_map[IDS["X"]], members=second_map)
        add("allreduce_start", IDS["X"], 1000.61, step_id="X:1", group_id="failed-retry",
            members=second_map, actual_input=[17.0] * 128)
        add("allreduce_end", IDS["X"], 1000.7, step_id="X:1", group_id="failed-retry",
            error="TimeoutError", actual_output=[12.75] * 128)
        events.append(closure)
        for number, event in enumerate(events, 1):
            event["event_number"] = number
        result = self.analyse_data(data)
        self.assertEqual((result["total_assembled_groups"], result["total_verified_groups"]), (2, 1))
        self.assertEqual(result["integrity_errors"], [])

    def test_same_roster_in_later_round_with_new_steps_is_valid(self):
        config, summary, events = make_trial()
        ids = IDS.copy()
        names_by_peer = {peer: name for name, peer in ids.items()}
        starts = {}
        results = {}
        result_events = {}
        for event in events:
            if event["kind"] == "step_start":
                d = event["detail"]
                starts[(d["name"], d["sequence"])] = event
            if event["kind"] == "step_result":
                d = event["detail"]
                results[d["step_id"]] = d
                result_events[d["step_id"]] = dict(event=event, detail=d)
        group_members = ["X", "H0", "H1", "H2"]
        second_map = {}
        for name in group_members:
            row = user_row(name, sequence=2)
            starts[(name, 2)] = dict(detail=row)
            second_map[IDS[name]] = row
        mean = sum(row["value"] for row in second_map.values()) / 4
        for name in group_members:
            step_id = f"{name}:2"
            row = dict(name=name, sequence=2, step_id=step_id, status="completed",
                       members=second_map, expected=mean, actual=[mean] * 128, correct=True)
            results[step_id] = row
        second_events = [dict(kind="assembled", peer=IDS["X"], monotonic=1002.0,
                              detail=dict(group_id="g2", step_id="X:2", roster=[IDS[n] for n in group_members],
                                          user=second_map[IDS["X"]], members=second_map))]
        for index, name in enumerate(group_members):
            second_events.append(dict(kind="allreduce_start", peer=IDS[name], monotonic=1002.1 + index*.01,
                                      detail=dict(group_id="g2", step_id=f"{name}:2", members=second_map,
                                                  actual_input=[second_map[IDS[name]]["value"]] * 128)))
            second_events.append(dict(kind="allreduce_end", peer=IDS[name], monotonic=1002.2 + index*.01,
                                      detail=dict(group_id="g2", step_id=f"{name}:2", error=None,
                                                  actual_output=[mean] * 128)))
        groups, incorrect, integrity = _assembly_analysis(events + second_events, ids, names_by_peer,
                                                           starts, results, result_events, config["tensor_size"])
        self.assertEqual(sum(group["verified"] for group in groups), 2)
        self.assertEqual(incorrect, [])
        self.assertEqual(integrity, [])

    def make_suite(self, root, outcome=None):
        (root / "harness").mkdir()
        snapshot = b"immutable harness snapshot"
        (root / "harness" / "sweep.py").write_bytes(snapshot)
        (root / "provenance.json").write_text(json.dumps(dict(harness_sha256={
            "sweep.py": hashlib.sha256(snapshot).hexdigest()})))
        jobs = []
        for seed in (2400, 2401):
            data = make_trial(seed)
            if outcome is not None:
                data[0]["outcome"] = outcome
            case, _, _ = data
            jobs.append(dict(case=case["case"], seed=seed, duration=case["duration"]))
            write_trial(root / f"range_1_2_s{seed}", data)
        suite = dict(phase="smoke", jobs=jobs)
        if outcome is not None:
            suite["outcome"] = outcome
        (root / "suite.json").write_text(json.dumps(suite))

    def test_suite_passes_and_reports_policy_aggregate(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            self.make_suite(root)
            result = analyse_suite(root)
            self.assertTrue(result["pass"])
            self.assertEqual(result["trial_count"], 2)
            self.assertEqual(result["aggregate"][0]["total_verified_groups"], 2)

    def test_partial_suite_is_gated_and_provenance_is_checked(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            self.make_suite(root)
            shutil.rmtree(root / "range_1_2_s2401")
            with self.assertRaisesRegex(ValueError, "incomplete suite"):
                analyse_suite(root)
            partial = analyse_suite(root, require_complete=False)
            self.assertFalse(partial["complete"])
            self.assertFalse(partial["pass"])

        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            self.make_suite(root)
            (root / "harness" / "sweep.py").write_bytes(b"changed snapshot")
            with self.assertRaisesRegex(ValueError, "snapshot mismatch"):
                analyse_suite(root)

        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            self.make_suite(root)
            (root / "halt.json").write_text(json.dumps(dict(reason="infrastructure failure")))
            result = analyse_suite(root)
            self.assertTrue(result["complete"])
            self.assertFalse(result["pass"])
            self.assertTrue(result["halted"])

    def test_grouping_suite_passes_control_gate_despite_tensor_failure(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            self.make_suite(root, outcome="grouping")
            trial_path = root / "range_1_2_s2400"
            data = (json.loads((trial_path / "config.json").read_text()),
                    json.loads((trial_path / "summary.json").read_text()),
                    [json.loads(line) for line in (trial_path / "events.jsonl").read_text().splitlines()])
            rewrite_event(data[2], lambda event: event["kind"] == "step_result" and event["detail"]["step_id"] == "X:1",
                          lambda event: event["detail"].update(actual=[999.0] * 128, correct=False))
            rewrite_event(data[2], lambda event: event["kind"] == "allreduce_end" and event["peer"] == IDS["X"],
                          lambda event: event["detail"].update(actual_output=[999.0] * 128))
            data[1]["outcomes"][0].update(actual=[999.0] * 128, correct=False)
            write_trial(trial_path, data)
            result = analyse_suite(root)
            self.assertTrue(result["control_plane_pass"])
            self.assertFalse(result["averaging_pass"])
            self.assertTrue(result["pass"])

    def test_grouping_suite_requires_manifest_and_trial_scope_match(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            self.make_suite(root, outcome="grouping")
            config_path = root / "range_1_2_s2400" / "config.json"
            config = json.loads(config_path.read_text())
            del config["outcome"]
            config_path.write_text(json.dumps(config))
            with self.assertRaisesRegex(ValueError, "trial outcome differs"):
                analyse_suite(root)


if __name__ == "__main__":
    unittest.main()
