"""Focused tests for the native announcement-training controller."""
from __future__ import annotations

import json
import importlib.util
import signal
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

from announcement_learning import NODES, make_problem
from run_announcement_training import (
    _build_round_record,
    _safe_label,
    _run_native_worker,
    _stop_process_group,
    _verify_native_reference,
    _worker_command,
    verify_trajectory,
)


def _write_native_case(case_path: Path, outputs: dict) -> None:
    (case_path / "outputs.json").write_text(json.dumps(outputs), encoding="utf-8")
    (case_path / "reference_validation.json").write_text(
        json.dumps({"available": True, "valid": True, "errors": []}), encoding="utf-8"
    )


class AnnouncementTrainingTests(unittest.TestCase):
    def test_safe_labels_reject_paths_and_unexpected_characters(self):
        self.assertEqual(_safe_label("native_s201_iid_baseline"), "native_s201_iid_baseline")
        for label in ("", "../escape", "a/b", "with space", "semi;colon"):
            with self.subTest(label=label), self.assertRaises(ValueError):
                _safe_label(label)

    def test_worker_command_uses_absolute_inputs_and_sixteen_byte_chunks(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            command = _worker_command(
                "membership", 201, root / "round_001", root / "inputs.json",
                root / "package", root / "dependencies",
            )
        self.assertIn("--inputs-json", command)
        input_index = command.index("--inputs-json") + 1
        self.assertTrue(Path(command[input_index]).is_absolute())
        chunk_index = command.index("--chunk-bytes") + 1
        self.assertEqual(command[chunk_index], "16")
        self.assertIn("membership", command)

    def test_native_reference_validator_is_required_and_saved(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            case_path = root / "round_001"
            case_path.mkdir()
            (root / "announcement_reference.py").write_text("# fixture\n", encoding="utf-8")
            expected = {"valid": True, "errors": [], "streams": {}, "processed": {}}
            module = types.SimpleNamespace(verify_native_round=lambda path: expected)
            with patch("run_announcement_training.HERE", root), \
                    patch("importlib.import_module", return_value=module):
                checked = _verify_native_reference(case_path)
            self.assertTrue(checked["available"])
            self.assertTrue(checked["valid"])
            self.assertEqual(json.loads((case_path / "reference_validation.json").read_text()),
                             {"available": True, **expected})

        with tempfile.TemporaryDirectory() as temporary:
            with patch("run_announcement_training.HERE", Path(temporary)):
                with self.assertRaisesRegex(RuntimeError, "required native tensor validator"):
                    _verify_native_reference(Path(temporary) / "round_001")

    def test_worker_exit_is_followed_by_owned_process_audit(self):
        events = []
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            case_path = root / "round_001"
            inputs_path = root / "requested.json"
            outputs = {
                node: {"initial": [0.0] * 48, "final": [0.0] * 48,
                       "status": "success", "failed_size": 0,
                       "failed_peer_ids": [], "banned_senders": []}
                for node in NODES
            }

            class FakeProcess:
                pid = 9317
                returncode = 0

                def __init__(self, command, **kwargs):
                    events.append("spawn")
                    case_path.mkdir()
                    (case_path / "shutdown.json").write_text("{}", encoding="utf-8")
                    (case_path / "outputs.json").write_text(json.dumps(outputs), encoding="utf-8")
                    self.command = command
                    self.kwargs = kwargs

                def wait(self, timeout):
                    events.append("exit")
                    return 0

            def audit(path, exit_code, dependencies):
                events.append("audit")
                self.assertEqual(path, case_path)
                self.assertEqual(exit_code, 0)
                self.assertEqual(dependencies, root / "dependencies")
                return {"safe": True}

            with patch("run_announcement_training.subprocess.Popen", FakeProcess), \
                    patch("run_announcement_training.audit_shutdown", side_effect=audit):
                result = _run_native_worker(
                    condition="baseline4", seed=201, case_path=case_path,
                    inputs_path=inputs_path, inputs={node: [0.0] * 48 for node in NODES},
                    log_path=root / "worker.log", package_root=root / "package",
                    dependencies=root / "dependencies",
                )
        self.assertEqual(events, ["spawn", "exit", "audit"])
        self.assertEqual(set(result), set(NODES))

    def test_interruption_stops_worker_group_before_propagating(self):
        class InterruptedWorker:
            pid = 9320
            returncode = None

            def wait(self, timeout):
                raise KeyboardInterrupt

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch("run_announcement_training.subprocess.Popen", return_value=InterruptedWorker()), \
                    patch("run_announcement_training._stop_process_group") as stop_group:
                with self.assertRaises(KeyboardInterrupt):
                    _run_native_worker(
                        condition="baseline4", seed=201, case_path=root / "round_001",
                        inputs_path=root / "requested.json",
                        inputs={node: [0.0] * 48 for node in NODES},
                        log_path=root / "worker.log", package_root=root / "package",
                        dependencies=root / "dependencies",
                    )
        stop_group.assert_called_once()

    @unittest.skipUnless(importlib.util.find_spec("numpy"), "NumPy is absent from this interpreter")
    def test_reconstructs_gradients_and_applies_successful_fallback_update(self):
        import numpy as np

        problem = make_problem(201, "distinctive")
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            problem.save(root)
            case_path = root / "round_001_native"
            case_path.mkdir()
            (case_path / "events_A.jsonl").write_text(
                json.dumps({"kind": "missing_part_fallback", "node": "A", "registered_parts": 0}) + "\n",
                encoding="utf-8",
            )
            before = {node: problem.initial_weights for node in NODES}
            gradients = {node: problem.gradient(node, before[node]) for node in NODES}
            outputs = {
                node: {
                    "initial": gradients[node].tolist(),
                    "final": gradients[node].tolist(),
                    "status": "success",
                    "failed_size": 192 if node == "A" else 0,
                    "failed_peer_ids": ["B", "D", "C"] if node == "A" else [],
                }
                for node in NODES
            }
            _write_native_case(case_path, outputs)
            record, after = _build_round_record(
                round_index=1,
                condition="membership",
                case_path=case_path,
                weights_before=before,
                input_gradients=gradients,
                outputs=outputs,
            )
            record["native_reference_validation"] = {
                "available": True, "valid": True, "errors": []
            }
            self.assertTrue(record["fallback"]["A"]["observed"])
            self.assertTrue(record["updates_applied"]["A"])
            self.assertFalse(np.array_equal(after["A"], before["A"]))
            trajectory = {"initial_weights": {node: before[node].tolist() for node in NODES}, "rounds": [record]}
            verification = verify_trajectory(root / "learning_data.npz", trajectory)
        self.assertTrue(verification["valid"])
        self.assertEqual(verification["participant_gradients_reconstructed"], 4)
        self.assertEqual(verification["fallback_updates_reconstructed"], 1)

    @unittest.skipUnless(importlib.util.find_spec("numpy"), "NumPy is absent from this interpreter")
    def test_verifier_rejects_corrupted_saved_post_update_model(self):
        problem = make_problem(201, "iid")
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            problem.save(root)
            case_path = root / "round_001_native"
            case_path.mkdir()
            before = {node: problem.initial_weights for node in NODES}
            gradients = {node: problem.gradient(node, before[node]) for node in NODES}
            outputs = {
                node: {"initial": gradients[node].tolist(), "final": gradients[node].tolist(),
                       "status": "success", "failed_size": 0, "failed_peer_ids": []}
                for node in NODES
            }
            _write_native_case(case_path, outputs)
            record, _ = _build_round_record(
                round_index=1, condition="baseline4", case_path=case_path,
                weights_before=before, input_gradients=gradients, outputs=outputs,
            )
            record["native_reference_validation"] = {
                "available": True, "valid": True, "errors": []
            }
            record["weights_after"]["B"][0] += 1.0
            trajectory = {"initial_weights": {node: before[node].tolist() for node in NODES}, "rounds": [record]}
            with self.assertRaisesRegex(ValueError, "post-update model mismatch"):
                verify_trajectory(root / "learning_data.npz", trajectory)

    @unittest.skipUnless(importlib.util.find_spec("numpy"), "NumPy is absent from this interpreter")
    def test_terminal_error_does_not_apply_local_update(self):
        problem = make_problem(201, "iid")
        with tempfile.TemporaryDirectory() as temporary:
            case_path = Path(temporary)
            before = {node: problem.initial_weights for node in NODES}
            gradients = {node: problem.gradient(node, before[node]) for node in NODES}
            outputs = {
                node: {"initial": gradients[node].tolist(), "final": gradients[node].tolist(),
                       "status": "success", "failed_size": 0, "failed_peer_ids": []}
                for node in NODES
            }
            outputs["D"]["status"] = "error"
            record, after = _build_round_record(
                round_index=1, condition="ordering", case_path=case_path,
                weights_before=before, input_gradients=gradients, outputs=outputs,
            )
        self.assertFalse(record["updates_applied"]["D"])
        self.assertEqual(after["D"].tolist(), before["D"].tolist())

    def test_timeout_cleanup_sends_term_then_kill_to_owned_group(self):
        class FakeProcess:
            pid = 8123
            returncode = None

            def wait(self, timeout):
                return -signal.SIGKILL

        process = FakeProcess()
        group_alive = True

        def signal_group(pid, sig):
            nonlocal group_alive
            if sig == 0:
                if group_alive:
                    return
                raise ProcessLookupError
            if sig == signal.SIGKILL:
                group_alive = False

        with patch("run_announcement_training.SHUTDOWN_GRACE_SECONDS", 0), \
                patch("run_announcement_training.os.killpg", side_effect=signal_group) as killpg:
            _stop_process_group(process)
        sent = [args for args, _ in killpg.call_args_list if args[1] != 0]
        self.assertEqual(sent, [
            (8123, signal.SIGTERM),
            (8123, signal.SIGKILL),
        ])
        self.assertFalse(group_alive)

    def test_cleanup_kills_children_after_worker_leader_has_exited(self):
        class ExitedLeader:
            pid = 9318
            returncode = 0

            def poll(self):
                return 0

            def wait(self, timeout):
                return 0

        group_alive = True

        def signal_group(pid, sig):
            nonlocal group_alive
            if sig == 0:
                if group_alive:
                    return
                raise ProcessLookupError
            if sig == signal.SIGKILL:
                group_alive = False

        with patch("run_announcement_training.SHUTDOWN_GRACE_SECONDS", 0), \
                patch("run_announcement_training.os.killpg", side_effect=signal_group) as killpg:
            _stop_process_group(ExitedLeader())
        sent = [args for args, _ in killpg.call_args_list if args[1] != 0]
        self.assertEqual(sent, [
            (9318, signal.SIGTERM),
            (9318, signal.SIGKILL),
        ])
        self.assertFalse(group_alive)


if __name__ == "__main__":
    unittest.main()
