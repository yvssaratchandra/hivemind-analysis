"""Focused checks for the raw verifier's non-participant closure contract."""
import json
from pathlib import Path
import tempfile
import unittest

import verify_step26_closure as verifier


class ClosureTests(unittest.TestCase):
    def marker(self):
        return dict(kind="trace_closed", pid=1, event_number=2, peer="", other="",
                    detail=dict(participants=list(verifier.frozen.PEER_NAMES), pending=[], error=None))

    def check(self, events):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.jsonl"
            path.write_text("".join(json.dumps(event) + "\n" for event in events))
            return verifier.closure_event(path)

    def test_nonparticipant_closure_is_valid(self):
        self.assertEqual(self.check([dict(kind="step_result"), self.marker()]), self.marker())

    def test_malformed_or_nonfinal_closure_is_rejected(self):
        for events in ([self.marker(), dict(kind="other")], [self.marker(), self.marker()], []):
            with self.subTest(events=events), self.assertRaises(ValueError):
                self.check(events)
        for key, value in (("pending", ["X"]), ("error", "failure"), ("participants", ["X"])):
            event = self.marker()
            event["detail"][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.check([event])

    def test_only_closure_identity_error_is_corrected(self):
        marker = self.marker()
        result = verifier.apply_closure_check({"errors": ["event 1:2 has unknown peer identity",
                                                            "event 3:4 has unknown peer identity"],
                                                "pass": False}, marker)
        self.assertEqual(result["errors"], ["event 3:4 has unknown peer identity"])
        self.assertFalse(result["pass"])
        self.assertTrue(result["closure_marker"]["valid"])


if __name__ == "__main__":
    unittest.main()
