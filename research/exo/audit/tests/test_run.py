"""Runner regression checks; no Docker daemon or Exo runtime needed."""
from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import run as runner


class RunnerTests(unittest.TestCase):
    def test_fresh_label_required_and_path_traversal_refused(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(runner, "RESULTS", Path(directory)):
            runner.create_output("local-001")
            with self.assertRaises(FileExistsError):
                runner.create_output("local-001")
            for label in ("../outside", "a/b", "UPPER", "", "a" * 41):
                with self.assertRaises(ValueError):
                    runner.create_output(label)

    def test_no_docker_is_reported_as_runtime_blocker(self):
        with patch.object(runner, "source_revision", return_value="a" * 40), \
                patch.object(runner.shutil, "which", return_value=None):
            report = runner.preflight()
        self.assertFalse(report["live_runtime_available"])
        self.assertTrue(report["memory_budget_is_unmeasured"])
        self.assertIn("Docker executable", report["blockers"][0])

    def test_readiness_cannot_cross_namespaces_or_arms(self):
        ready = {"publisher_node_id": "abc", "namespace": "local-001-self",
                 "arm": "self", "role": "router-only"}
        self.assertEqual(runner.validate_readiness(ready, "local-001-self", "self"), "abc")
        for change in ({"namespace": "other"}, {"arm": "forged"},
                       {"role": "node"}, {"publisher_node_id": "../bad"}):
            with self.assertRaises(RuntimeError):
                runner.validate_readiness(ready | change, "local-001-self", "self")

    def test_api_startup_error_is_retried_within_consensus_deadline(self):
        with tempfile.TemporaryDirectory() as directory:
            compose = MagicMock(output=Path(directory))
            logs = {
                "honest-a": "Starting node a1\nCurrent session: master_node_id='a1' election_clock=2",
                "honest-b": "Starting node b1\nCurrent session: master_node_id='a1' election_clock=2",
            }
            compose.logs.return_value = logs
            compose.snapshots.side_effect = [RuntimeError("API not yet ready"),
                {"honest-a": {"node_id": "a1"}, "honest-b": {"node_id": "b1"}}]
            with patch.object(runner.time, "sleep"), \
                    patch.object(runner.time, "monotonic", side_effect=range(100)):
                result = runner.wait_consensus(compose, 20)
            self.assertTrue(result["converged"])
            self.assertEqual(compose.snapshots.call_count, 2)
            evidence = (Path(directory) / "convergence.jsonl").read_text()
            self.assertIn("API not yet ready", evidence)

    def test_existing_project_is_neither_started_nor_cleaned(self):
        for existing_containers in (True, False):
            with self.subTest(containers=existing_containers), tempfile.TemporaryDirectory() as directory:
                compose = MagicMock()
                compose.project = "local-001-self"
                compose.call.side_effect = lambda *args, **kw: (
                    "existing-id" if args == ("ps", "-aq") and existing_containers else "")
                with patch.object(runner, "Compose", return_value=compose), \
                        patch.object(runner, "checked", return_value="existing-volume"):
                    result = runner.run_arm(Path(directory) / "self", "a" * 40,
                                            "local-001", "self", 1901, 90)
                self.assertEqual(result["status"], "incomplete")
                self.assertFalse(any(call.args[0] in {"up", "down"}
                                     for call in compose.call.call_args_list))

    def test_partial_start_failure_is_captured_and_scoped_cleanup_preserves_volumes(self):
        with tempfile.TemporaryDirectory() as directory:
            compose = MagicMock()
            compose.project = "local-001-self"
            def respond(*args, **kwargs):
                if args[0] == "up":
                    raise RuntimeError("second container failed")
                return ""
            compose.call.side_effect = respond
            with patch.object(runner, "Compose", return_value=compose), \
                    patch.object(runner, "checked", return_value=""):
                result = runner.run_arm(Path(directory) / "self", "a" * 40,
                                        "local-001", "self", 1901, 90)
            self.assertEqual(result["status"], "incomplete")
            compose.capture.assert_called_once()
            compose.call.assert_any_call("down", "--timeout", "10", timeout=45)
            self.assertFalse(any("-v" in call.args or "--volumes" in call.args
                                 for call in compose.call.call_args_list))
            self.assertEqual(json.loads((Path(directory) / "self/result.json").read_text()), result)

    def test_success_requires_positive_control_and_complete_evidence(self):
        def observation(arm, outcome="accepted", **extra):
            return {"arm": arm, "status": "observed", "observations": {"outcome": outcome}, **extra}
        self.assertFalse(runner.summarize([observation("forged")], ["forged"])["run_complete"])
        self.assertFalse(runner.summarize([observation("self", "inconclusive")], ["self"])["run_complete"])
        self.assertTrue(runner.summarize([observation("self"), observation("forged")],
                                         ["self", "forged"])["run_complete"])
        self.assertTrue(runner.summarize([observation("baseline", "no_send_control")],
                                         ["baseline"])["run_complete"])
        self.assertFalse(runner.summarize([observation("baseline", "unexpected_send_observed")],
                                          ["baseline"])["run_complete"])
        self.assertFalse(runner.summarize([observation("self", capture_error="missing logs")],
                                          ["self"])["run_complete"])


if __name__ == "__main__":
    unittest.main()
