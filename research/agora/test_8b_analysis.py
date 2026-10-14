"""Focused fixture tests for the local Agora eight-peer baseline analyzer."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from typing import Any

from analyze_8b_baseline import analyze_trial


NAMES = ["R0", "R1", "R2", "R3", "C0", "C1", "C2", "C3"]
PEER_IDS = [f"p{index}" for index in range(8)]
INDICES = [1, 3, 5, 7, 9]


def _snapshot_values(peer_index: int, round_number: int, phase: str) -> list[float]:
    if phase == "smoke":
        return [float(peer_index)] * 5
    return [peer_index + round_number * 0.5 + coordinate * 0.25 for coordinate in range(5)]


def _fixture_events(round_number: int, phase: str = "smoke") -> dict[str, list[dict[str, Any]]]:
    weights = [1.0] * 8 if phase == "smoke" else [float(index + 1) for index in range(8)]
    values = [_snapshot_values(index, round_number, phase) for index in range(8)]
    total_weight = sum(weights)
    mean = [
        sum(values[index][coordinate] * weights[index] for index in range(8)) / total_weight
        for coordinate in range(5)
    ]
    # Simulates the native ordering by binary PeerID; its string form need not
    # be lexicographically sorted.
    roster = list(reversed(PEER_IDS))
    result: dict[str, list[dict[str, Any]]] = {}
    for peer_index, name in enumerate(NAMES):
        before = [values[peer_index][coordinate] + 100.0 + peer_index for coordinate in range(5)]
        delta = [mean[coordinate] - values[peer_index][coordinate] for coordinate in range(5)]
        after = [before[coordinate] + delta[coordinate] for coordinate in range(5)]
        role = "reducer" if name.startswith("R") else "contributor"
        result[name] = [
            {"kind": "identity", "peer": name, "peer_id": PEER_IDS[peer_index], "role": role},
            {"kind": "native_roster_log", "peer": name, "group_size": 8},
            {"kind": "training_initial", "peer": name, "loss": 2.0},
            {
                "kind": "round_snapshot",
                "peer": name,
                "round": round_number,
                "epoch": round_number * 20,
                "indices": list(INDICES),
                "values": values[peer_index],
                "weight": weights[peer_index],
            },
            {
                "kind": "roster",
                "peer": name,
                "round": round_number,
                "epoch": round_number * 20,
                "members": list(roster),
                "native_group_id": "c0ffee",
                "ready_to_roster_s": 0.2 + peer_index / 100,
                "native_roster_s": 0.4 + peer_index / 100,
            },
            {
                "kind": "round_result",
                "peer": name,
                "round": round_number,
                "epoch": round_number * 20,
                "indices": list(INDICES),
                "delta": delta,
                "before_apply": before,
                "after_apply": after,
                "failed_size": 0,
                "failed_peer_ids": {},
                "roster": list(roster),
                "status": "completed",
            },
        ]
    return result


def _write_trial(
    root: Path,
    *,
    phase: str = "smoke",
    rounds: int = 1,
    final_loss: float = 1.5,
    events: dict[str, list[dict[str, Any]]] | None = None,
    shutdown: dict[str, Any] | None = None,
) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    config = {
        "seed": 23,
        "phase": phase,
        "rounds": rounds,
        "node_names": list(NAMES),
        "profiles": {
            name: {"role": "reducer" if name.startswith("R") else "contributor"}
            for name in NAMES
        },
        "tensor_values": 100,
        "averaging_every_steps": 20,
        "tolerance": 1e-5,
    }
    (root / "config.json").write_text(json.dumps(config), encoding="utf-8")
    if events is None:
        events = {name: [] for name in NAMES}
        for round_number in range(1, rounds + 1):
            generated = _fixture_events(round_number, phase)
            for name in NAMES:
                if round_number == 1:
                    events[name].extend(generated[name])
                else:
                    events[name].extend(
                        event for event in generated[name] if event["kind"] in ("round_snapshot", "roster", "round_result")
                    )
    for peer_index, name in enumerate(NAMES):
        peer_events = list(events.get(name, []))
        peer_events.extend(
            [
                {
                    "kind": "training_final",
                    "peer": name,
                    "loss": final_loss,
                    "step": 0 if phase == "smoke" else 100,
                    "finite": True,
                },
                {"kind": "trace_closed", "peer": name, "clean": True},
            ]
        )
        path = root / f"events_{name}.jsonl"
        path.write_text("".join(json.dumps(event, allow_nan=True) + "\n" for event in peer_events), encoding="utf-8")
    if shutdown is not None:
        (root / "shutdown.json").write_text(json.dumps(shutdown), encoding="utf-8")
    return root


class AnalyzeEightPeerBaselineTests(unittest.TestCase):
    def test_successful_smoke_checks_async_correction_at_apply_time(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            trial = _write_trial(Path(temp_dir))
            report = analyze_trial(trial)
            events = [json.loads(line) for line in (trial / "events_R0.jsonl").read_text(encoding="utf-8").splitlines()]
            snapshot = next(event for event in events if event["kind"] == "round_snapshot")
            result = next(event for event in events if event["kind"] == "round_result")
            async_state_is_distinct = result["before_apply"] != snapshot["values"]

        self.assertTrue(report["pass"], report["errors"])
        self.assertTrue(report["valid"])
        self.assertEqual(report["metrics"]["peer_round_results"], 8)
        self.assertEqual(report["metrics"]["agreed_rounds"], 1)
        self.assertTrue(report["rounds"][0]["numerical_pass"])
        self.assertEqual(report["rounds"][0]["group_size"], 8)
        # Fixture before_apply values have progressed far beyond their immutable
        # snapshots. A stale-state comparison would reject this valid async update.
        self.assertTrue(async_state_is_distinct)
        self.assertLessEqual(report["rounds"][0]["max_abs_error"], 1e-5)

    def test_wrong_delta_fails_even_when_after_apply_uses_that_delta(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            events = _fixture_events(1)
            target = next(event for event in events["R0"] if event["kind"] == "round_result")
            target["delta"][0] += 0.25
            target["after_apply"][0] += 0.25
            report = analyze_trial(_write_trial(Path(temp_dir), events=events))

        self.assertFalse(report["pass"])
        self.assertTrue(report["valid"])
        self.assertFalse(report["rounds"][0]["numerical_pass"])
        self.assertTrue(any("weighted snapshot correction" in error for error in report["errors"]))

    def test_mismatched_roster_fails_group_agreement(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            events = _fixture_events(1)
            for event in events["C3"]:
                if event["kind"] in ("roster", "round_result"):
                    event["members" if event["kind"] == "roster" else "roster"].pop()
            report = analyze_trial(_write_trial(Path(temp_dir), events=events))

        self.assertFalse(report["pass"])
        self.assertFalse(report["rounds"][0]["roster_agreement"])
        self.assertEqual(report["rounds"][0]["group_size"], 7)

    def test_smoke_snapshot_must_remain_the_actor_index(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            events = _fixture_events(1)
            snapshot = next(event for event in events["C0"] if event["kind"] == "round_snapshot")
            snapshot["values"] = [7.0] * 5
            report = analyze_trial(_write_trial(Path(temp_dir), events=events))

        self.assertFalse(report["pass"])
        self.assertTrue(any("constant actor index 4" in error for error in report["errors"]))

    def test_duplicate_and_missing_round_events_are_invalid(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            events = _fixture_events(1)
            duplicate = dict(next(event for event in events["R0"] if event["kind"] == "round_snapshot"))
            events["R0"].append(duplicate)
            events["R1"] = [event for event in events["R1"] if event["kind"] != "round_snapshot"]
            report = analyze_trial(_write_trial(Path(temp_dir), events=events))

        self.assertFalse(report["pass"])
        self.assertFalse(report["valid"])
        self.assertTrue(any("expected one round_snapshot" in error for error in report["errors"]))
        self.assertTrue(any("missing round_snapshot" in error for error in report["errors"]))

    def test_partial_averaging_failure_is_invalid(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            events = _fixture_events(1)
            result = next(event for event in events["R2"] if event["kind"] == "round_result")
            result.update(status="failed", failed_size=1, failed_peer_ids={"p7": 1}, error="peer left")
            report = analyze_trial(_write_trial(Path(temp_dir), events=events))

        self.assertFalse(report["pass"])
        self.assertFalse(report["valid"])
        self.assertTrue(any("failed_size must be zero" in error for error in report["errors"]))

    def test_nonfinite_numeric_value_is_invalid(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            events = _fixture_events(1)
            snapshot = next(event for event in events["C0"] if event["kind"] == "round_snapshot")
            snapshot["values"][2] = float("nan")
            report = analyze_trial(_write_trial(Path(temp_dir), events=events))

        self.assertFalse(report["pass"])
        self.assertFalse(report["valid"])
        self.assertTrue(any("non-finite" in error for error in report["errors"]))

    def test_missing_trace_closure_is_invalid(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            trial = _write_trial(Path(temp_dir))
            path = trial / "events_C2.jsonl"
            lines = [line for line in path.read_text(encoding="utf-8").splitlines() if '"trace_closed"' not in line]
            path.write_text("\n".join(lines) + "\n", encoding="utf-8")
            report = analyze_trial(trial)

        self.assertFalse(report["pass"])
        self.assertFalse(report["valid"])
        self.assertTrue(any("missing trace_closed" in error for error in report["errors"]))

    def test_missing_loss_check_is_invalid(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            events = _fixture_events(1)
            events["R1"] = [event for event in events["R1"] if event["kind"] != "training_initial"]
            report = analyze_trial(_write_trial(Path(temp_dir), events=events))

        self.assertFalse(report["pass"])
        self.assertFalse(report["valid"])
        self.assertTrue(any("missing training_initial" in error for error in report["errors"]))

    def test_pilot_requires_decreased_mean_loss(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            report = analyze_trial(_write_trial(Path(temp_dir), phase="pilot", rounds=5, final_loss=2.0))

        self.assertFalse(report["pass"])
        self.assertTrue(report["valid"])
        self.assertEqual(report["metrics"]["agreed_rounds"], 5)
        self.assertTrue(all(round_report["numerical_pass"] for round_report in report["rounds"]))
        self.assertTrue(any("must be below mean initial loss" in error for error in report["errors"]))

    def test_pilot_success_requires_100_final_steps(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            report = analyze_trial(_write_trial(Path(temp_dir), phase="pilot", rounds=5, final_loss=1.0))

        self.assertTrue(report["pass"], report["errors"])
        self.assertTrue(all(round_report["numerical_pass"] for round_report in report["rounds"]))

    def test_worker_clock_and_skipped_round_events_reject_trial(self) -> None:
        for kind in ("worker_error", "clock_discontinuity", "round_skipped"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as temp_dir:
                events = _fixture_events(1)
                events["R0"].append({"kind": kind, "peer": "R0", "round": 1, "error": "fixture"})
                report = analyze_trial(_write_trial(Path(temp_dir), events=events))

            self.assertFalse(report["pass"])
            self.assertFalse(report["valid"])
            self.assertTrue(any(kind in error for error in report["errors"]))

    def test_epochs_must_match_round_cadence(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            events = _fixture_events(1)
            roster = next(event for event in events["R0"] if event["kind"] == "roster")
            roster["epoch"] = 19
            report = analyze_trial(_write_trial(Path(temp_dir), events=events))

        self.assertFalse(report["pass"])
        self.assertFalse(report["valid"])
        self.assertTrue(any("roster epoch must be 20" in error for error in report["errors"]))

    def test_indices_must_be_unique_and_inside_tensor(self) -> None:
        observed_errors = []
        for indices in ([1, 1, 5, 7, 9], [1, 3, 5, 7, 100]):
            with self.subTest(indices=indices), tempfile.TemporaryDirectory() as temp_dir:
                events = _fixture_events(1)
                snapshot = next(event for event in events["R0"] if event["kind"] == "round_snapshot")
                snapshot["indices"] = list(indices)
                report = analyze_trial(_write_trial(Path(temp_dir), events=events))
                observed_errors.append(report["errors"])

            self.assertFalse(report["pass"])
            self.assertFalse(report["valid"])
        self.assertTrue(any("unique indices" in error for error in observed_errors[0]))
        self.assertTrue(any("within [0, 100)" in error for error in observed_errors[1]))

    def test_final_step_count_is_phase_specific(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            trial = _write_trial(Path(temp_dir))
            path = trial / "events_R0.jsonl"
            events = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
            next(event for event in events if event["kind"] == "training_final")["step"] = 20
            path.write_text("".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")
            report = analyze_trial(trial)

        self.assertFalse(report["pass"])
        self.assertFalse(report["valid"])
        self.assertTrue(any("smoke training_final step must be 0" in error for error in report["errors"]))

    def test_configured_profile_roles_are_checked(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            trial = _write_trial(Path(temp_dir))
            config_path = trial / "config.json"
            config = json.loads(config_path.read_text(encoding="utf-8"))
            config["profiles"]["R0"]["role"] = "contributor"
            config_path.write_text(json.dumps(config), encoding="utf-8")
            report = analyze_trial(trial)

        self.assertFalse(report["pass"])
        self.assertFalse(report["valid"])
        self.assertTrue(any("does not match configured profile role" in error for error in report["errors"]))

    def test_optional_shutdown_evidence_must_be_safe_with_zero_exits(self) -> None:
        valid_shutdown = {
            "safe": True,
            "survivors": [],
            "worker_exitcodes": {name: 0 for name in NAMES},
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            report = analyze_trial(_write_trial(Path(temp_dir), shutdown=valid_shutdown))
        self.assertTrue(report["pass"], report["errors"])
        self.assertTrue(report["shutdown"]["present"])

        invalid_shutdowns = (
            dict(valid_shutdown, safe=False),
            dict(valid_shutdown, worker_exitcodes=dict(valid_shutdown["worker_exitcodes"], R0=1)),
        )
        for invalid_shutdown, expected_error in zip(invalid_shutdowns, ("shutdown.json safe must be true", "exit code 0")):
            with self.subTest(shutdown=invalid_shutdown), tempfile.TemporaryDirectory() as temp_dir:
                report = analyze_trial(_write_trial(Path(temp_dir), shutdown=invalid_shutdown))
            self.assertFalse(report["pass"])
            self.assertFalse(report["valid"])
            self.assertTrue(any(expected_error in error for error in report["errors"]))


if __name__ == "__main__":
    unittest.main()
