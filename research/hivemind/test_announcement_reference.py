"""Focused regression tests for native-round evidence and numerical references."""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import announcement_reference as reference
from trainsim.protocols.partitioning import allocate_elements, assign_chunks
from trainsim.protocols.hivemind.numerical import aggregate


NODES = ("B", "D", "A", "C")
NATIVE_GROUP = "a1b2c3"


def _roster(condition: str, node: str) -> list[str]:
    if condition == "membership" and node != "A":
        return ["B", "D", "C"]
    if condition == "ordering" and node == "A":
        return ["A", "D", "B", "C"]
    return list(NODES)


def _write_case(root: Path, *, condition: str = "baseline4") -> tuple[Path, list[dict]]:
    """Create the outer artifact schema; numerical stream proofs are mocked per test."""
    case = root / "round_001"
    case.mkdir(parents=True)
    (case / "config.json").write_text(json.dumps({
        "condition": condition, "tensor_shape": [48], "chunk_bytes": 16,
    }))
    outputs = {}
    events: list[dict] = [{"kind": "leader_assembled", "node": "B",
                           "native_group_id": NATIVE_GROUP}]
    for node in NODES:
        members = _roster(condition, node)
        counts = allocate_elements(48, [None] * len(members))
        chunk_indices = assign_chunks(48, 4, counts)
        ranges = {
            peer: [[4 * index, 4 * index + 4] for index in chunks]
            for peer, chunks in zip(members, chunk_indices)
        }
        parts = {peer: len(chunks) for peer, chunks in zip(members, chunk_indices)}
        common = {"node": node, "native_group_id": NATIVE_GROUP}
        events.extend([
            {"kind": "local_roster", **common, "members": members,
             "gathered_nodes": members, "bandwidths": [None] * len(members),
             "modes": [0] * len(members)},
            {"kind": "allocation", **common, "members": members,
             "modes": [0] * len(members), "allocations": list(counts),
             "parts": parts, "part_ranges": ranges, "weight": 1,
             "compression": "NoCompression"},
            {"kind": "averaging_terminal", **common, "status": "success",
             "failed_size": 0, "failed_peer_ids": [], "banned_senders": []},
            {"kind": "runner_finalized", **common, "failed_size": 0,
             "failed_peer_ids": [], "banned_senders": []},
        ])
        outputs[node] = {
            "initial": [float(i) for i in range(48)],
            "final": [float(i) for i in range(48)],
            "status": "success", "failed_size": 0,
            "failed_peer_ids": [], "banned_senders": [],
        }

    for node in ("D", "A", "C"):
        members = _roster(condition, node)
        fields = {"payload_hex": "00", "group_id": NATIVE_GROUP,
                  "members": members, "gathered_nodes": members}
        events.append({"kind": "announcement_sent", "node": "B",
                       "recipient": node, **fields})
        events.append({"kind": "announcement_received", "node": node,
                       "sender": "B", **fields})

    (case / "outputs.json").write_text(json.dumps(outputs))
    (case / "events.jsonl").write_text("".join(json.dumps(event) + "\n" for event in events))
    return case, events


def _proof_mocks():
    streams = {node: {"valid": True, "evidence_complete": True, "issues": [],
                      "evidence_issues": []} for node in NODES}
    processed = {node: {"valid": True, "complete": True, "issues": []}
                 for node in NODES}
    return {
        "_shutdown_check": lambda _path: (True, None),
        "_identity_check": lambda _path: ({"valid": True}, []),
        "_source_provenance_check": lambda _path: ({"valid": True}, []),
        "_stream_proof": lambda *_args, **_kwargs: streams,
        "_processed_output_proof": lambda *_args, **_kwargs: processed,
    }


class NativeReferenceGateTests(unittest.TestCase):
    def _verify(self, case: Path) -> dict:
        with patch.multiple(reference, **_proof_mocks()):
            return reference.verify_native_round(case)

    def test_accepts_well_formed_isolated_reference_fixture(self):
        with tempfile.TemporaryDirectory() as temporary:
            case, _ = _write_case(Path(temporary))
            result = self._verify(case)
        self.assertTrue(result["valid"], result["errors"])

    def test_rejects_nonleader_announcement_sender(self):
        with tempfile.TemporaryDirectory() as temporary:
            case, events = _write_case(Path(temporary))
            sent = next(event for event in events
                        if event["kind"] == "announcement_sent" and event["recipient"] == "D")
            sent["node"] = "C"
            (case / "events.jsonl").write_text(
                "".join(json.dumps(event) + "\n" for event in events))
            result = self._verify(case)
        self.assertFalse(result["valid"], "the recipient payload must be attributable to leader B")

    def test_rejects_received_announcement_from_nonleader(self):
        with tempfile.TemporaryDirectory() as temporary:
            case, events = _write_case(Path(temporary))
            received = next(event for event in events
                            if event["kind"] == "announcement_received" and event["node"] == "D")
            received["sender"] = "C"
            (case / "events.jsonl").write_text(
                "".join(json.dumps(event) + "\n" for event in events))
            result = self._verify(case)
        self.assertFalse(result["valid"], "received announcement must name leader B")

    def test_rejects_duplicate_announcement_records(self):
        with tempfile.TemporaryDirectory() as temporary:
            case, events = _write_case(Path(temporary))
            sent = next(event for event in events
                        if event["kind"] == "announcement_sent" and event["recipient"] == "D")
            events.append(dict(sent))
            (case / "events.jsonl").write_text(
                "".join(json.dumps(event) + "\n" for event in events))
            result = self._verify(case)
        self.assertFalse(result["valid"], "each recipient must have exactly one sent announcement")

    def test_rejects_announcement_bound_to_different_native_group(self):
        with tempfile.TemporaryDirectory() as temporary:
            case, events = _write_case(Path(temporary))
            for event in events:
                if event.get("kind") in ("announcement_sent", "announcement_received"):
                    event["group_id"] = "different-group"
            (case / "events.jsonl").write_text(
                "".join(json.dumps(event) + "\n" for event in events))
            result = self._verify(case)
        self.assertFalse(result["valid"], "announcement group ID must match the native group")

    def test_rejects_misaligned_gathered_metadata(self):
        with tempfile.TemporaryDirectory() as temporary:
            case, events = _write_case(Path(temporary))
            for event in events:
                if event.get("kind") in ("announcement_sent", "announcement_received") \
                        and event.get("recipient", event.get("node")) == "D":
                    event["gathered_nodes"] = ["D", "B", "A", "C"]
            (case / "events.jsonl").write_text(
                "".join(json.dumps(event) + "\n" for event in events))
            result = self._verify(case)
        self.assertFalse(result["valid"], "gathered metadata order must match the announced roster")

    def test_rejects_malformed_announcement_payload_hex(self):
        with tempfile.TemporaryDirectory() as temporary:
            case, events = _write_case(Path(temporary))
            for event in events:
                if event.get("kind") in ("announcement_sent", "announcement_received") \
                        and event.get("recipient", event.get("node")) == "D":
                    event["payload_hex"] = "not-hex"
            (case / "events.jsonl").write_text(
                "".join(json.dumps(event) + "\n" for event in events))
            result = self._verify(case)
        self.assertFalse(result["valid"], "serialized announcements must be valid hexadecimal bytes")

    def test_rejects_nested_tensor_when_config_declares_flat_vector(self):
        with tempfile.TemporaryDirectory() as temporary:
            case, _ = _write_case(Path(temporary))
            outputs = json.loads((case / "outputs.json").read_text())
            outputs["B"]["initial"] = [outputs["B"]["initial"][i:i + 12]
                                         for i in range(0, 48, 12)]
            (case / "outputs.json").write_text(json.dumps(outputs))
            result = self._verify(case)
        self.assertFalse(result["valid"], "tensor_shape=[48] requires a flat vector artifact")

    def test_rejects_unexpected_output_participant(self):
        with tempfile.TemporaryDirectory() as temporary:
            case, _ = _write_case(Path(temporary))
            outputs = json.loads((case / "outputs.json").read_text())
            outputs["X"] = dict(outputs["B"])
            (case / "outputs.json").write_text(json.dumps(outputs))
            result = self._verify(case)
        self.assertFalse(result["valid"], "native reference outputs must contain exactly the four nodes")

    def test_missing_required_artifact_fails_closed(self):
        with tempfile.TemporaryDirectory() as temporary:
            case, _ = _write_case(Path(temporary))
            (case / "outputs.json").unlink()
            result = self._verify(case)
        self.assertFalse(result["valid"])
        self.assertTrue(result["errors"])


class NumericalBackendGateTests(unittest.TestCase):
    def test_empty_reference_views_are_rejected(self):
        with self.assertRaises(ValueError):
            aggregate({"A": [1.0, 2.0]}, {}, chunk_elements=1)


if __name__ == "__main__":
    unittest.main()
