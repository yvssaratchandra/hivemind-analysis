"""Evidence-corruption checks for the Step 23 independent analyzer."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from analyze_step23 import analyse_suite, analyse_trial, contrasts, interval


def fixture(mode="advance", second_search=False):
    config = dict(case=dict(name=mode, mode=mode, response="reject", phase="mechanism", guard="native"),
                  peer_ids=dict(X="x", H0="h"), tensor_size=128, correct_output_horizon=18,
                  seed=2301, pattern="mechanism")
    events = []

    def add(kind, peer="", other="", **detail):
        tick = len(events)+1
        detail.setdefault("search_id", None)
        events.append(dict(kind=kind, peer=peer, other=other, pid=1, event_number=tick,
                           monotonic=100+tick/100, wall_time=100+tick/100, detail=detail))

    add("step_start", "x", name="X", step_id="X:1", value=7)
    add("search_start", "x", search_id="X:1:search1", step_id="X:1")
    add("ad_finished", "x", expiration=102, active=True, accepted=True)
    add("observer_ready", expiration=102)
    add("step_start", "h", name="H0", step_id="H0:1", value=0)
    search = "H0:1:search1"
    add("search_start", "h", search_id=search, step_id="H0:1")
    add("ad_view", "h", search_id=search, entries=[dict(peer="x", expiration=102)])
    add("leader_selected", "h", "x", search_id=search, selected_expiration=102)
    add("join_start", "h", "x", search_id=search, attempt_id="H0:1:attempt1",
        selected_expiration=102, request_expiration=105)
    add("join_end", "h", "x", search_id=search, attempt_id="H0:1:attempt1")
    new_expiration = 102.25 if mode == "advance" else 102
    add("ad_finished", "x", expiration=new_expiration, active=True, accepted=mode == "advance")
    if mode == "advance" or second_search:
        if second_search:
            add("search_end", "h", search_id=search, step_id="H0:1")
            search = "H0:1:search2"
            add("search_start", "h", search_id=search, step_id="H0:1")
        add("ad_view", "h", search_id=search, entries=[dict(peer="x", expiration=new_expiration)])
        add("leader_selected", "h", "x", search_id=search, selected_expiration=new_expiration)
        add("join_start", "h", "x", search_id=search, attempt_id="H0:1:attempt2",
            selected_expiration=new_expiration, request_expiration=105)
        add("join_end", "h", "x", search_id=search, attempt_id="H0:1:attempt2")
    add("search_end", "h", search_id=search, step_id="H0:1")
    add("search_end", "x", search_id="X:1:search1", step_id="X:1")
    outcomes = dict(H0=dict(name="H0", step_id="H0:1", elapsed=6, status="failed"),
                    X=dict(name="X", step_id="X:1", elapsed=6, status="cancelled"))
    for name, row in outcomes.items():
        add("step_result", config["peer_ids"][name], **row)
    add("trace_closed", participants=["X", "H0"])
    return config, dict(outcomes=outcomes, expected_x_cancel=True), events


class EvidenceTests(unittest.TestCase):
    def run_fixture(self, data):
        config, summary, events = data
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)
            (path / "config.json").write_text(json.dumps(config))
            (path / "summary.json").write_text(json.dumps(summary))
            (path / "events.jsonl").write_text("".join(json.dumps(event)+"\n" for event in events))
            return analyse_trial(path)

    def test_changed_expiration_is_retry(self):
        result = self.run_fixture(fixture())
        self.assertEqual((result["x_retries"], result["renewal_retries"]), (1, 1))
        self.assertTrue(result["mechanism_retry_before_original_expiry"])

    def test_unchanged_expiration_is_excluded(self):
        self.assertEqual(self.run_fixture(fixture("same"))["x_retries"], 0)

    def test_new_search_is_not_same_search_retry(self):
        result = self.run_fixture(fixture("same", second_search=True))
        self.assertEqual(result["x_attempts"], 2)
        self.assertEqual(result["x_retries"], 0)

    def test_missing_or_reordered_events_rejected(self):
        for index in (2, 8, -1):
            data = fixture()
            data[2].pop(index)
            with self.assertRaises(ValueError):
                self.run_fixture(data)

    def test_unclosed_stream_rejected(self):
        data = fixture()
        event = next(e for e in data[2] if e["kind"] == "join_end")
        event["kind"] = "unrelated"
        with self.assertRaisesRegex(ValueError, "unfinished join"):
            self.run_fixture(data)

    def test_unobserved_selected_expiration_rejected(self):
        data = fixture()
        for event in data[2]:
            if event["kind"] == "ad_view":
                event["detail"]["entries"] = []
        with self.assertRaisesRegex(ValueError, "native DHT views"):
            self.run_fixture(data)

    def test_summary_tampering_rejected(self):
        data = fixture()
        data[1]["outcomes"]["H0"]["elapsed"] = 1
        # The fixture deliberately shares row objects; detach the raw evidence first.
        fresh = fixture()
        with self.assertRaisesRegex(ValueError, "summary"):
            self.run_fixture((data[0], data[1], fresh[2]))

    def test_peerid_guard_must_prevent_retry(self):
        data = fixture()
        data[0]["case"]["guard"] = "peer_id"
        with self.assertRaisesRegex(ValueError, "PeerID guard"):
            self.run_fixture(data)

    def test_identical_pair_retry_rejected(self):
        data = fixture()
        for event in data[2]:
            if event["detail"].get("selected_expiration") == 102.25:
                event["detail"]["selected_expiration"] = 102
        with self.assertRaisesRegex(ValueError, "selected twice"):
            self.run_fixture(data)

    def test_failed_output_is_penalized(self):
        self.assertEqual(self.run_fixture(fixture())["penalized_seconds"], 18)

    def test_completed_tensors_verified_against_inputs(self):
        config, summary, events = fixture("same")
        config["case"].update(phase="smoke", response="reliable")
        config["peer_ids"]["H1"] = "h1"
        members = {"h": dict(name="H0", step_id="H0:1", value=0.0),
                   "h1": dict(name="H1", step_id="H1:1", value=1.0)}
        events.insert(1, dict(kind="step_start", peer="h1", other="", pid=2, event_number=1,
                              monotonic=100.015, wall_time=100.015,
                              detail=dict(name="H1", step_id="H1:1", value=1, search_id=None)))
        row = dict(name="H1", step_id="H1:1", elapsed=2, status="completed", members=members, actual=[0.5]*128)
        summary["outcomes"]["H1"] = copy.deepcopy(row)
        events.insert(-1, dict(kind="step_result", peer="h1", other="", pid=2, event_number=3,
                               monotonic=100.9, wall_time=100.9, detail=copy.deepcopy(row)))
        events.insert(-2, dict(kind="assembled", peer="h", other="", pid=2, event_number=2,
                               monotonic=100.2, wall_time=100.2,
                               detail=dict(group_id="g", roster=["h", "h1"])))
        h0 = dict(row, name="H0", step_id="H0:1")
        summary["outcomes"]["H0"] = copy.deepcopy(h0)
        for event in events:
            if event["kind"] == "step_result" and event["peer"] == "h":
                event["detail"] = copy.deepcopy(h0)
        self.assertEqual(self.run_fixture((config, summary, events))["honest_correct"], 2)
        for event in events:
            if event["kind"] == "step_result" and event["peer"] == "h":
                event["detail"]["actual"][0] = 9
        summary["outcomes"]["H0"]["actual"][0] = 9
        result = self.run_fixture((config, summary, events))
        self.assertEqual(result["incorrect_steps"], ["H0"])
        self.assertEqual(result["honest_correct"], 0)

    def test_bootstrap_is_reproducible(self):
        self.assertEqual(interval([1, 2, 3]), interval([1, 2, 3]))

    def test_reliable_control_adjusts_publication_effect(self):
        rows = []
        for pattern in ("staggered", "bursty"):
            for response in ("reliable", "unreliable"):
                for mode in ("expire", "refresh"):
                    value = 1 + (response == "unreliable") + 2*(mode == "refresh")
                    rows.append(dict(seed=1, pattern=pattern, response=response, mode=mode, guard="native",
                                     penalized_seconds=value, honest_correct=7, x_retries=0, x_wait_seconds=0))
        result = contrasts(rows)["penalized_seconds"]
        self.assertEqual(result["primary"]["mean"], 2)
        self.assertEqual(result["interaction"]["mean"], 0)

    def make_suite(self, path):
        jobs = []
        (path / "harness").mkdir()
        (path / "harness" / "snapshot.py").write_bytes(b"preserved source")
        digest = hashlib.sha256(b"preserved source").hexdigest()
        (path / "provenance.json").write_text(json.dumps(dict(harness_sha256={"snapshot.py": digest})))
        for seed in range(2301, 2306):
            for mode in ("same", "advance"):
                config, summary, events = fixture(mode)
                config["seed"] = seed
                trial = path / f"{mode}_{seed}"
                trial.mkdir()
                (trial / "config.json").write_text(json.dumps(config))
                (trial / "summary.json").write_text(json.dumps(summary))
                (trial / "events.jsonl").write_text("".join(json.dumps(e)+"\n" for e in events))
                jobs.append(dict(case=config["case"], seed=seed, pattern=config["pattern"]))
        (path / "suite.json").write_text(json.dumps(dict(phase="mechanism", jobs=jobs)))

    def test_complete_paired_mechanism_suite_passes(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)
            self.make_suite(path)
            result = analyse_suite(path)
            self.assertTrue(result["mechanism_pass"])
            self.assertEqual(result["mechanism_pairs_demonstrated"], 5)

    def test_partial_suite_cannot_pass_gate(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)
            self.make_suite(path)
            (path / "advance_2301" / "summary.json").unlink()
            with self.assertRaisesRegex(ValueError, "incomplete"):
                analyse_suite(path)

    def test_snapshot_tampering_cannot_pass_gate(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)
            self.make_suite(path)
            (path / "harness" / "snapshot.py").write_bytes(b"changed source")
            with self.assertRaisesRegex(ValueError, "snapshot mismatch"):
                analyse_suite(path)

    def test_policy_drift_cannot_pass_gate(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)
            self.make_suite(path)
            p = path / "same_2301" / "config.json"
            data = json.loads(p.read_text())
            data["case"]["unexpected_parameter"] = 1
            p.write_text(json.dumps(data))
            with self.assertRaisesRegex(ValueError, "policy differs"):
                analyse_suite(path)


if __name__ == "__main__":
    unittest.main()
