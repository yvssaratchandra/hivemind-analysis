"""Focused tests for offline announcement-probe analysis."""

from __future__ import annotations

import json
import hashlib
import io
import sys
import tempfile
import tarfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

from analyze_announcement_probe import (
    NODES,
    analyze_condition,
    analyze_suite,
    _allocation_errors,
    _evidence_checks,
    _mean_from_saved_inputs,
    _output_analysis,
)
import announcement_probe as native_probe


def _input_tensor(node: str) -> list[list[float]]:
    node_index = NODES.index(node)
    result = []
    for row_index in range(48):
        row = [0.0] * 9
        row[0] = 100 * node_index + row_index + 1
        row[1 + node_index] = 1.0
        row[5 + node_index] = row_index + 1
        result.append(row)
    return result


_UNEQUAL_COUNTS = {
    "B,D,A,C": [0, 94, 150, 188],
    "A,D,B,C": [150, 94, 0, 188],
    "B,D,C": [0, 190, 242],
}
_UNEQUAL_RANGES = {
    "B,D,A,C": {
        "B": [],
        "D": [[0, 36], [36, 72], [72, 108]],
        "A": [[108, 144], [144, 180], [180, 216], [216, 252]],
        "C": [[252, 288], [288, 324], [324, 360], [360, 396], [396, 432]],
    },
    "A,D,B,C": {
        "A": [[0, 36], [36, 72], [72, 108], [108, 144]],
        "D": [[144, 180], [180, 216], [216, 252]],
        "B": [],
        "C": [[252, 288], [288, 324], [324, 360], [360, 396], [396, 432]],
    },
    "B,D,C": {
        "B": [],
        "D": [[0, 36], [36, 72], [72, 108], [108, 144], [144, 180]],
        "C": [[180, 216], [216, 252], [252, 288], [288, 324], [324, 360], [360, 396], [396, 432]],
    },
}


def _unequal_config() -> dict:
    return {
        "bandwidth_profile": "unequal",
        "bandwidths": {"B": 1.0, "D": 2.0, "A": 3.0, "C": 4.0},
        "allocation_reference": _UNEQUAL_COUNTS,
        "tensor_shape": [48, 9],
        "chunk_bytes": 144,
    }


def _unequal_allocation_event(node: str, members: list[str]) -> dict:
    key = ",".join(members)
    ranges = _UNEQUAL_RANGES[key]
    return {
        "kind": "allocation",
        "node": node,
        "members": members,
        "allocations": _UNEQUAL_COUNTS[key],
        "modes": [1 if count == 0 else 0 for count in _UNEQUAL_COUNTS[key]],
        "parts": {peer: len(ranges[peer]) for peer in members},
        "part_ranges": ranges,
    }


def _condition_artifacts(path: Path, condition: str, *, fallback_b: bool = False) -> None:
    path.mkdir(parents=True, exist_ok=True)
    run_token = f"{path.parent.name}-{path.name}"
    identities = {node: f"test-peer-{run_token}-{node}" for node in NODES}
    (path / "identities.json").write_text(json.dumps(identities), encoding="utf-8")
    harness_bytes = b"synthetic announcement probe harness for offline analyzer tests\n"
    native_bytes = b"synthetic pinned native source for offline analyzer tests\n"
    native_hash = hashlib.sha256(native_bytes).hexdigest()
    harness_hash = hashlib.sha256(harness_bytes).hexdigest()
    provenance = {
        "package": "/synthetic/native/hivemind",
        "python": sys.version,
        "torch": "test-fixture",
        "hivemind": "test-fixture",
        "p2pd_sha256": "a" * 64,
        "hashes": {"averaging/test_fixture.py": native_hash},
    }
    (path / "provenance.json").write_text(json.dumps(provenance), encoding="utf-8")
    (path.parent / "harness_hashes.json").write_text(json.dumps({"announcement_probe.py": harness_hash}), encoding="utf-8")
    with tarfile.open(path / "sources.tar.gz", "w:gz") as archive:
        for name, contents in (("announcement_probe.py", harness_bytes),
                               ("hivemind/averaging/test_fixture.py", native_bytes)):
            info = tarfile.TarInfo(name)
            info.size = len(contents)
            archive.addfile(info, io.BytesIO(contents))
    active = ["B", "D", "C"] if condition == "control3" else list(NODES)
    config = {"condition": condition, "seed": 1, "nodes": list(NODES), "active": active,
              "prefix": f"test-prefix-{run_token}"}
    (path / "config.json").write_text(json.dumps(config), encoding="utf-8")
    local_rosters = {
        "baseline4": {node: list(NODES) for node in NODES},
        "control3": {node: ["B", "D", "C"] for node in ("B", "D", "C")},
        "ordering": {node: (["A", "D", "B", "C"] if node == "A" else list(NODES)) for node in NODES},
        "membership": {node: (list(NODES) if node == "A" else ["B", "D", "C"]) for node in NODES},
    }[condition]
    assembled = ["B", "D", "C"] if condition == "control3" else list(NODES)
    group_id = f"group-{run_token}"
    events = [
        {"kind": "step_started", "node": "B", "time": 0.4, "pid": 100},
        {"kind": "leader_advertised", "node": "B", "time": 0.5, "pid": 100},
        {"kind": "leader_assembled", "node": "B", "time": 1.0, "pid": 100,
         "members": assembled, "native_group_id": group_id},
    ]
    for follower_index, follower in enumerate(node for node in active if node != "B"):
        events.append({"kind": "step_started", "node": follower, "time": 0.7 + follower_index / 100,
                       "pid": 101 + follower_index})
        events.append({"kind": "join_accepted", "node": "B", "time": 0.8 + follower_index / 100,
                       "pid": 100, "follower": follower})

    allocation_by_node = {}
    initial_by_node = {node: _input_tensor(node) for node in NODES}
    for node in active:
        members = local_rosters[node]
        expected_dense = 432 // len(members)
        chunks_per_peer = 3 if len(members) == 4 else 4
        ranges = {
            peer: [[index * expected_dense + chunk * 36, index * expected_dense + (chunk + 1) * 36]
                   for chunk in range(chunks_per_peer)]
            for index, peer in enumerate(members)
        }
        events.extend([
            {"kind": "local_roster", "node": node, "time": 1.1, "pid": 100 + NODES.index(node),
             "members": members, "gathered_nodes": members, "native_group_id": group_id,
             "modes": [0] * len(members), "bandwidths": [None] * len(members)},
            {"kind": "allocation", "node": node, "time": 1.2, "pid": 100 + NODES.index(node),
             "members": members, "allocations": [expected_dense] * len(members),
             "modes": [0] * len(members), "weight": 1, "compression": "NoCompression",
             "parts": {member: chunks_per_peer for member in members},
             "part_ranges": ranges, "native_group_id": group_id},
        ])
        allocation_by_node[node] = events[-1]

    recipients = [node for node in active if node != "B"]
    for recipient in recipients:
        members = local_rosters[recipient]
        payload = f"{len(members):02x}{NODES.index(recipient):02x}"
        events.extend([
            {"kind": "announcement_sent", "node": "B", "time": 1.01, "pid": 100,
             "recipient": recipient, "members": members, "gathered_nodes": members,
             "payload_hex": payload, "group_id": group_id},
            {"kind": "announcement_received", "node": recipient, "time": 1.05, "pid": 100 + NODES.index(recipient),
             "sender": "B", "members": members, "gathered_nodes": members,
             "payload_hex": payload, "group_id": group_id},
        ])

    input_chunks = {}
    for receiver in active:
        for sender in local_rosters[receiver]:
            if sender == receiver or sender not in active or receiver not in local_rosters[sender]:
                continue
            source_allocation = allocation_by_node[sender]
            ranges = source_allocation["part_ranges"][receiver]
            source_flat = [value for row in initial_by_node[sender] for value in row]
            stream_id = f"{group_id}:{sender}->{receiver}"
            events.append({"kind": "stream_started", "node": receiver, "time": 1.25, "pid": 100 + NODES.index(receiver),
                           "sender": sender, "receiver": receiver, "stream_id": stream_id,
                           "native_group_id": group_id})
            input_chunks[(receiver, sender)] = []
            for part_index, (start, end) in enumerate(ranges):
                values = source_flat[start:end]
                event = {"kind": "input_chunk", "node": receiver, "time": 1.3 + part_index / 1000,
                         "pid": 100 + NODES.index(receiver), "sender": sender, "receiver": receiver,
                         "stream_id": stream_id, "native_group_id": group_id, "part_index": part_index,
                         "values": values, "code": 4 if part_index == 0 else 0,
                         "input_group_id": group_id if part_index == 0 else "",
                         "weight": 1, "compression": 0}
                events.append(event)
                input_chunks[(receiver, sender)].append(event)
            events.append({"kind": "stream_closed", "node": receiver, "time": 1.5, "pid": 100 + NODES.index(receiver),
                           "sender": sender, "receiver": receiver, "stream_id": stream_id,
                           "native_group_id": group_id})

    def reducer_delta(reducer: str, recipient: str, part_index: int) -> list[float]:
        reducer_ranges = allocation_by_node[reducer]["part_ranges"][reducer]
        start, end = reducer_ranges[part_index]
        local_flat = [value for row in initial_by_node[reducer] for value in row]
        local_values = local_flat[start:end]
        remote_values = []
        for source in local_rosters[reducer]:
            if source == reducer:
                continue
            remote_values.append(input_chunks[(reducer, source)][part_index]["values"])
        mean = [(local_values[index] + sum(values[index] for values in remote_values)) / len(local_rosters[reducer])
                for index in range(len(local_values))]
        if recipient == reducer:
            original = local_values
        else:
            original = input_chunks[(reducer, recipient)][part_index]["values"]
        return [averaged - value for averaged, value in zip(mean, original)]

    result_chunks = {}
    for reducer in active:
        for recipient in local_rosters[reducer]:
            if recipient == reducer or recipient not in active or reducer not in local_rosters[recipient]:
                continue
            part_count = allocation_by_node[reducer]["parts"][reducer]
            stream_id = f"{group_id}:{recipient}->{reducer}"
            result_chunks[(recipient, reducer)] = []
            for part_index in range(part_count):
                values = reducer_delta(reducer, recipient, part_index)
                if fallback_b and reducer == "D" and recipient == "B":
                    continue
                event = {"kind": "result_chunk", "node": reducer, "time": 1.6 + part_index / 1000,
                         "pid": 100 + NODES.index(reducer), "sender": reducer, "receiver": recipient,
                         "stream_id": stream_id, "native_group_id": group_id, "part_index": part_index,
                         "values": values}
                events.append(event)
                result_chunks[(recipient, reducer)].append(event)

    outputs = {}
    for node in NODES:
        initial = initial_by_node[node]
        if node == "A" and condition == "control3":
            outputs[node] = {"status": "idle", "initial": initial, "final": initial, "elapsed_seconds": 0.0}
            continue
        if node == "A" and condition == "membership":
            outputs[node] = {"status": "error", "initial": initial, "final": initial,
                             "error": "native result streams did not complete", "elapsed_seconds": 2.0}
            continue
        flat = [value for row in initial for value in row]
        for peer in local_rosters[node]:
            ranges = allocation_by_node[node]["part_ranges"][peer]
            for part_index, (start, end) in enumerate(ranges):
                if fallback_b and node == "B" and peer == "D":
                    delta = [0.0] * (end - start)
                elif peer == node:
                    delta = reducer_delta(node, node, part_index)
                else:
                    delta_events = result_chunks.get((node, peer), [])
                    match = next((event for event in delta_events if event["part_index"] == part_index), None)
                    delta = match["values"] if match else [0.0] * (end - start)
                for offset, value in enumerate(delta, start):
                    flat[offset] += value
        final = [flat[row * 9:(row + 1) * 9] for row in range(48)]
        outputs[node] = {"status": "success", "initial": initial, "final": final,
                         "elapsed_seconds": 1.0, "error": None}

    for node in active:
        failed_size = 144 if fallback_b and node == "B" else 0
        failed_peers = ["D"] if failed_size else []
        if failed_size:
            events.append({"kind": "missing_part_fallback", "node": node, "time": 1.8,
                           "pid": 100 + NODES.index(node), "reducer": "D",
                           "registered_parts": 0, "native_group_id": group_id})
        events.append({"kind": "runner_finalized", "node": node, "time": 1.9,
                       "pid": 100 + NODES.index(node), "failed_size": failed_size,
                       "failed_peer_ids": failed_peers, "banned_senders": [],
                       "native_group_id": group_id})
        outputs[node].update({"failed_size": failed_size, "failed_peer_ids": failed_peers, "banned_senders": []})
        events.append({"kind": "averaging_terminal", "node": node, "time": 2.0,
                       "pid": 200 + NODES.index(node), "status": outputs[node]["status"],
                       "failed_size": failed_size, "failed_peer_ids": failed_peers, "banned_senders": []})

    for node in active:
        if outputs[node]["status"] != "success":
            continue
        initial_flat = [value for row in outputs[node]["initial"] for value in row]
        final_flat = [value for row in outputs[node]["final"] for value in row]
        for peer in local_rosters[node]:
            for part_index, (start, end) in enumerate(allocation_by_node[node]["part_ranges"][peer]):
                events.append({"kind": "processed_chunk", "node": node, "time": 1.7,
                               "pid": 100 + NODES.index(node), "reducer": peer, "part_index": part_index,
                               "native_group_id": group_id,
                               "values": [final_flat[offset] - initial_flat[offset] for offset in range(start, end)]})

    (path / "events_0.jsonl").write_text("".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")
    (path / "outputs.json").write_text(json.dumps(outputs), encoding="utf-8")
    (path / "shutdown.json").write_text(json.dumps({"safe": True, "surviving_pids": []}), encoding="utf-8")


def _enable_native_shape_victim_fallback(path: Path) -> None:
    """Turn the membership fixture's failed A operation into the recorded native fallback shape."""
    outputs_path = path / "outputs.json"
    outputs = json.loads(outputs_path.read_text(encoding="utf-8"))
    outputs["A"].update(
        status="success",
        error=None,
        final=outputs["A"]["initial"],
        elapsed_seconds=6.0,
        failed_size=324,
        failed_peer_ids=["D", "C", "B"],
        banned_senders=["B", "D", "C"],
    )
    outputs_path.write_text(json.dumps(outputs), encoding="utf-8")

    events_path = path / "events_0.jsonl"
    events = [json.loads(line) for line in events_path.read_text(encoding="utf-8").splitlines()]
    group_id = next(event["native_group_id"] for event in events if event.get("kind") == "allocation" and event.get("node") == "A")
    rewritten = []
    for event in events:
        if event.get("node") == "A" and event.get("kind") == "runner_finalized":
            event = {
                **event,
                "time": 2.0,
                "failed_size": 324,
                "failed_peer_ids": ["D", "C", "B"],
                "banned_senders": ["B", "D", "C"],
            }
        elif event.get("node") == "A" and event.get("kind") == "averaging_terminal":
            event = {
                **event,
                "time": 2.1,
                "status": "success",
                "failed_size": 324,
                "failed_peer_ids": ["D", "C", "B"],
                "banned_senders": ["B", "D", "C"],
            }
        rewritten.append(event)

    # Remote participants reject A's stream because their recipient rosters omit
    # it. Keep the actual direction and lifecycle so the fallback is traceable.
    for reducer in ("B", "D", "C"):
        stream_id = f"{group_id}:A->{reducer}"
        for kind, timestamp, extra in (
            ("stream_started", 0.90, {}),
            ("stream_error", 0.91, {"error": "rejected sender is not in reducer roster"}),
            ("stream_closed", 0.92, {}),
        ):
            rewritten.append({
                "kind": kind,
                "node": reducer,
                "time": timestamp,
                "pid": 101 + NODES.index(reducer),
                "sender": "A",
                "receiver": reducer,
                "stream_id": stream_id,
                "native_group_id": group_id,
                **extra,
            })

    # Each remote reducer's three 36-element output parts fall back to zero.
    # After the three senders are banned, A's own three parts are processed as a
    # local-only mean, also yielding zero correction.
    for reducer in ("D", "C", "B"):
        rewritten.append({
            "kind": "missing_part_fallback", "node": "A", "time": 1.70,
            "pid": 102, "reducer": reducer, "registered_parts": 0,
            "native_group_id": group_id,
        })
        for part_index in range(3):
            rewritten.append({
                "kind": "processed_chunk", "node": "A", "time": 1.71 + part_index / 1000,
                "pid": 102, "reducer": reducer, "part_index": part_index,
                "values": [0.0] * 36, "native_group_id": group_id,
            })
    for sender in ("B", "D", "C"):
        rewritten.append({
            "kind": "sender_banned", "node": "A", "time": 1.85,
            "pid": 102, "sender": sender, "native_group_id": group_id,
        })
    for part_index in range(3):
        rewritten.append({
            "kind": "processed_chunk", "node": "A", "time": 1.90 + part_index / 1000,
            "pid": 102, "reducer": "A", "part_index": part_index,
            "values": [0.0] * 36, "native_group_id": group_id,
        })
    events_path.write_text("".join(json.dumps(event) + "\n" for event in rewritten), encoding="utf-8")


class AnnouncementProbeAnalyzerTests(unittest.TestCase):
    def test_output_mean_and_provenance_use_saved_inputs(self) -> None:
        initial = {node: _input_tensor(node) for node in NODES}
        for node_index, node in enumerate(NODES):
            for row in range(48):
                initial[node][row][0] += (node_index + 1) * 13.0
        saved_mean = _mean_from_saved_inputs(initial, NODES)
        self.assertIsNotNone(saved_mean)
        outputs = {
            node: {"status": "success", "initial": initial[node], "final": saved_mean}
            for node in NODES
        }
        events = [
            {"kind": "local_roster", "node": node, "members": list(NODES)}
            for node in NODES
        ]

        analyzed = _output_analysis(outputs, events)

        self.assertTrue(analyzed["B"]["matches_expected_control"])
        self.assertTrue(analyzed["B"]["matches_mean_implied_by_origin_markers"])
        self.assertFalse(analyzed["B"]["A_cleanly_excluded_from_completed_BDC_mean"])
        self.assertAlmostEqual(analyzed["B"]["control_mean_max_abs_error"], 0.0)

    def test_four_and_three_member_controls_match_independent_means(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            _condition_artifacts(root / "baseline4", "baseline4")
            _condition_artifacts(root / "control3", "control3")
            baseline = analyze_condition(root / "baseline4")
            control = analyze_condition(root / "control3")
            self.assertTrue(baseline["valid_evidence"], baseline["evidence"]["errors"])
            self.assertTrue(baseline["control_mean_check"]["active_outputs_match"])
            self.assertTrue(control["valid_evidence"], control["evidence"]["errors"])
            self.assertTrue(control["control_mean_check"]["active_outputs_match"])
            self.assertEqual(control["evidence"]["active_nodes"], ["B", "D", "C"])
            self.assertTrue(control["interpretation"]["clean_exclusion_consistent_across_BDC"])
            self.assertFalse(control["interpretation"]["membership_inconsistency_observed"])
            self.assertFalse(control["interpretation"]["attack_hypothesis_confirmed"])
            self.assertAlmostEqual(baseline["outputs"]["B"]["announcement_to_terminal_seconds"], 0.9)
            for node in ("D", "A", "C"):
                self.assertAlmostEqual(baseline["outputs"][node]["announcement_to_terminal_seconds"], 0.95)
            self.assertTrue(all(pair["agree_within_tolerance"] for pair in baseline["participant_agreement"]["pairs"]))

    def test_membership_exclusion_requires_completed_nonfallback_bdc_mean(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "membership"
            _condition_artifacts(path, "membership")
            result = analyze_condition(path)
            self.assertTrue(result["valid_evidence"], result["evidence"]["errors"])
            self.assertEqual(result["interpretation"]["nodes_with_clean_A_exclusion"], ["B", "D", "C"])
            self.assertTrue(result["interpretation"]["attack_hypothesis_confirmed"])
            self.assertTrue(result["interpretation"]["membership_inconsistency_observed"])
            self.assertEqual(result["outputs"]["B"]["origin_nodes_observed"], ["B", "D", "C"])
            self.assertEqual(result["evidence"]["leader_assembled_roster"], ["B", "D", "A", "C"])
            self.assertEqual(result["evidence"]["local_aggregation_rosters"]["B"], ["B", "D", "C"])

    def test_membership_victim_local_fallback_is_a_valid_confirmed_finding(self) -> None:
        # The native fallback still has to reconstruct A's saved output, and the
        # local-only mean is accepted only after matching sender-ban events.
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "membership"
            _condition_artifacts(path, "membership")
            _enable_native_shape_victim_fallback(path)
            result = analyze_condition(path)
            self.assertTrue(result["valid_evidence"], result["evidence"]["errors"])
            self.assertTrue(result["interpretation"]["attack_hypothesis_confirmed"])
            self.assertEqual(result["interpretation"]["nodes_with_clean_A_exclusion"], ["B", "D", "C"])
            self.assertTrue(result["outputs"]["A"]["fallback_indicated"])
            self.assertFalse(result["outputs"]["A"]["stream_proof_complete"])
            self.assertTrue(result["outputs"]["A"]["stream_evidence_complete"])
            self.assertTrue(result["outputs"]["A"]["processed_output_valid"])
            self.assertTrue(result["processed_output_proof"]["A"]["reconstructs_saved_final"])
            self.assertEqual(result["processed_output_proof"]["A"]["processed_chunks"], 12)
            self.assertFalse(result["outputs"]["A"]["A_cleanly_excluded_from_completed_BDC_mean"])

    def test_native_shape_fallback_rejects_missing_or_corrupted_evidence(self) -> None:
        mutations = (
            "remove_chunk",
            "corrupt_fallback_delta",
            "corrupt_final",
            "remove_timed_ban",
            "corrupt_failed_size",
        )
        for mutation in mutations:
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temp:
                path = Path(temp) / "membership"
                _condition_artifacts(path, "membership")
                _enable_native_shape_victim_fallback(path)
                events_path = path / "events_0.jsonl"
                events = [json.loads(line) for line in events_path.read_text(encoding="utf-8").splitlines()]
                outputs_path = path / "outputs.json"
                outputs = json.loads(outputs_path.read_text(encoding="utf-8"))
                if mutation == "remove_chunk":
                    events = [event for event in events if not (
                        event.get("kind") == "processed_chunk" and event.get("node") == "A"
                        and event.get("reducer") == "D" and event.get("part_index") == 1
                    )]
                elif mutation == "corrupt_fallback_delta":
                    event = next(event for event in events if event.get("kind") == "processed_chunk"
                                 and event.get("node") == "A" and event.get("reducer") == "D")
                    event["values"][0] = 1.0
                elif mutation == "corrupt_final":
                    outputs["A"]["final"][0][0] += 1000.0
                    outputs_path.write_text(json.dumps(outputs), encoding="utf-8")
                elif mutation == "remove_timed_ban":
                    events = [event for event in events if not (
                        event.get("kind") == "sender_banned" and event.get("node") == "A"
                        and event.get("sender") == "B"
                    )]
                    outputs["A"]["banned_senders"] = ["D", "C"]
                    outputs_path.write_text(json.dumps(outputs), encoding="utf-8")
                    finalized = next(event for event in events if event.get("kind") == "runner_finalized" and event.get("node") == "A")
                    finalized["banned_senders"] = ["D", "C"]
                    terminal = next(event for event in events if event.get("kind") == "averaging_terminal" and event.get("node") == "A")
                    terminal["banned_senders"] = ["D", "C"]
                elif mutation == "corrupt_failed_size":
                    outputs["A"]["failed_size"] = 288
                    outputs_path.write_text(json.dumps(outputs), encoding="utf-8")
                    finalized = next(event for event in events if event.get("kind") == "runner_finalized" and event.get("node") == "A")
                    finalized["failed_size"] = 288
                    terminal = next(event for event in events if event.get("kind") == "averaging_terminal" and event.get("node") == "A")
                    terminal["failed_size"] = 288
                events_path.write_text("".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")
                result = analyze_condition(path)
                self.assertFalse(result["valid_evidence"], result["evidence"]["errors"])
                self.assertFalse(result["processed_output_proof"]["A"]["valid"])

    def test_stream_error_with_contradictory_direction_is_invalid(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "membership"
            _condition_artifacts(path, "membership")
            _enable_native_shape_victim_fallback(path)
            events_path = path / "events_0.jsonl"
            events = [json.loads(line) for line in events_path.read_text(encoding="utf-8").splitlines()]
            event = next(event for event in events if (
                event.get("kind") == "stream_error" and event.get("node") == "B"
                and event.get("sender") == "A"
            ))
            event["receiver"] = "C"
            events_path.write_text("".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")
            result = analyze_condition(path)
            self.assertFalse(result["valid_evidence"])
            self.assertFalse(result["outputs"]["B"]["stream_evidence_complete"])
            self.assertTrue(any("stream_error node/sender/receiver identity is inconsistent" in issue
                                for issue in result["outputs"]["B"]["stream_evidence_issues"]))

    def test_membership_victim_success_without_fallback_still_invalidates(self) -> None:
        # The fallback exemption is narrow: a success whose streams are incomplete but
        # that records no fallback at all is genuinely missing evidence and must fail.
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "membership"
            _condition_artifacts(path, "membership")
            outputs = json.loads((path / "outputs.json").read_text(encoding="utf-8"))
            outputs["A"].update(status="success", error=None, final=outputs["A"]["initial"],
                                failed_size=0, failed_peer_ids=[], banned_senders=[])
            (path / "outputs.json").write_text(json.dumps(outputs), encoding="utf-8")
            result = analyze_condition(path)
            self.assertFalse(result["valid_evidence"])
            self.assertTrue(any("lacks complete native stream proof" in error
                                for error in result["evidence"]["errors"]))

    def test_ordering_misalignment_is_reported_without_invalidating_stream_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "ordering"
            _condition_artifacts(path, "ordering")
            result = analyze_condition(path)
            self.assertTrue(result["valid_evidence"], result["evidence"]["errors"])
            self.assertTrue(result["stream_proof"]["A"]["valid"], result["stream_proof"]["A"]["issues"])
            self.assertFalse(result["stream_proof"]["A"]["input_coordinate_alignment"]["B"])
            self.assertFalse(result["stream_proof"]["A"]["result_coordinate_alignment"]["B"])
            self.assertFalse(result["outputs"]["A"]["coordinate_markers_correct"])

    def test_returned_delta_is_recomputed_from_actual_input_chunks(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "baseline4"
            _condition_artifacts(path, "baseline4")
            events_path = path / "events_0.jsonl"
            events = [json.loads(line) for line in events_path.read_text(encoding="utf-8").splitlines()]
            result_chunk = next(event for event in events if event["kind"] == "result_chunk" and
                                event["node"] == "D" and event["receiver"] == "B")
            result_chunk["values"][0] += 1.0
            events_path.write_text("".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")
            result = analyze_condition(path)
            self.assertFalse(result["stream_proof"]["B"]["valid"])
            self.assertTrue(any("mean-minus-input correction" in issue for issue in result["stream_proof"]["B"]["issues"]))
            self.assertFalse(result["valid_evidence"])

    def test_local_processed_delta_is_recomputed_from_received_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "baseline4"
            _condition_artifacts(path, "baseline4")
            events_path = path / "events_0.jsonl"
            events = [json.loads(line) for line in events_path.read_text(encoding="utf-8").splitlines()]
            own_delta = next(event for event in events if event["kind"] == "processed_chunk" and
                             event["node"] == "B" and event["reducer"] == "B" and event["part_index"] == 0)
            own_delta["values"][0] += 1.0
            events_path.write_text("".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")
            outputs_path = path / "outputs.json"
            outputs = json.loads(outputs_path.read_text(encoding="utf-8"))
            outputs["B"]["final"][0][0] += 1.0
            outputs_path.write_text(json.dumps(outputs), encoding="utf-8")
            result = analyze_condition(path)
            self.assertTrue(any("applied local delta differs from native mean-minus-input correction" in issue
                                for issue in result["processed_output_proof"]["B"]["issues"]))
            self.assertFalse(result["valid_evidence"])

    def test_missing_leader_acceptance_invalidates_matchmaking_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "baseline4"
            _condition_artifacts(path, "baseline4")
            events_path = path / "events_0.jsonl"
            events = [json.loads(line) for line in events_path.read_text(encoding="utf-8").splitlines()]
            events = [event for event in events if not (
                event["kind"] == "join_accepted" and event["follower"] == "A"
            )]
            events_path.write_text("".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")
            result = analyze_condition(path)
            self.assertFalse(result["valid_evidence"])
            self.assertTrue(any("join_accepted event for A" in issue for issue in result["evidence"]["errors"]))

    def test_unexpected_leader_invalidates_the_condition(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "baseline4"
            _condition_artifacts(path, "baseline4")
            events_path = path / "events_0.jsonl"
            events = [json.loads(line) for line in events_path.read_text(encoding="utf-8").splitlines()]
            events.append({"kind": "unexpected_leader", "node": "D", "time": 0.9, "pid": 101})
            events_path.write_text("".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")
            result = analyze_condition(path)
            self.assertFalse(result["valid_evidence"])
            self.assertTrue(any("unexpected leader election" in issue for issue in result["evidence"]["errors"]))

    def test_follower_search_must_start_after_leader_advertisement(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "baseline4"
            _condition_artifacts(path, "baseline4")
            events_path = path / "events_0.jsonl"
            events = [json.loads(line) for line in events_path.read_text(encoding="utf-8").splitlines()]
            follower_start = next(event for event in events if event["kind"] == "step_started" and event["node"] == "A")
            follower_start["time"] = 0.4
            events_path.write_text("".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")
            result = analyze_condition(path)
            self.assertFalse(result["valid_evidence"])
            self.assertTrue(any("search did not start after B's successful advertisement" in issue
                                for issue in result["evidence"]["errors"]))

    def test_leader_search_must_start_before_its_advertisement(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "baseline4"
            _condition_artifacts(path, "baseline4")
            events_path = path / "events_0.jsonl"
            events = [json.loads(line) for line in events_path.read_text(encoding="utf-8").splitlines()]
            leader_start = next(event for event in events if event["kind"] == "step_started" and event["node"] == "B")
            leader_start["time"] = 0.6
            events_path.write_text("".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")
            result = analyze_condition(path)
            self.assertFalse(result["valid_evidence"])
            self.assertTrue(any("B: leader search did not start before" in issue
                                for issue in result["evidence"]["errors"]))

    def test_native_timeout_is_reported_as_deadline(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "membership"
            _condition_artifacts(path, "membership")
            outputs_path = path / "outputs.json"
            outputs = json.loads(outputs_path.read_text(encoding="utf-8"))
            outputs["A"]["error"] = "TimeoutError('native all-reduce deadline')"
            outputs_path.write_text(json.dumps(outputs), encoding="utf-8")
            result = analyze_condition(path)
            self.assertEqual(result["outputs"]["A"]["operation_status"], "deadline")

    def test_three_member_control_rejects_any_A_averaging_attempt(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "control3"
            _condition_artifacts(path, "control3")
            events_path = path / "events_0.jsonl"
            events = [json.loads(line) for line in events_path.read_text(encoding="utf-8").splitlines()]
            events.append({"kind": "local_roster", "node": "A", "time": 1.1, "pid": 404,
                           "members": ["B", "D", "C"], "gathered_nodes": ["B", "D", "C"],
                           "native_group_id": "unexpected", "modes": [0, 0, 0], "bandwidths": [None, None, None]})
            events_path.write_text("".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")
            result = analyze_condition(path)
            self.assertFalse(result["valid_evidence"])
            self.assertTrue(any("inactive participant emitted averaging evidence" in issue for issue in result["evidence"]["errors"]))

    def test_coordinate_marker_mismatch_is_reported(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "ordering"
            _condition_artifacts(path, "ordering")
            outputs = json.loads((path / "outputs.json").read_text(encoding="utf-8"))
            outputs["B"]["final"][7][5] += 1.0
            (path / "outputs.json").write_text(json.dumps(outputs), encoding="utf-8")
            result = analyze_condition(path)
            self.assertFalse(result["outputs"]["B"]["coordinate_markers_correct"])
            self.assertIn("B", result["interpretation"]["nodes_with_coordinate_marker_mismatch"])
            self.assertFalse(result["outputs"]["B"]["matches_expected_control"])

    def test_fallback_is_not_counted_as_successful_exclusion(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "membership"
            _condition_artifacts(path, "membership", fallback_b=True)
            result = analyze_condition(path)
            self.assertTrue(result["outputs"]["B"]["fallback_indicated"])
            self.assertEqual(result["processed_output_proof"]["B"]["fallback_elements"], 144)
            self.assertFalse(result["outputs"]["B"]["matches_expected_control"])
            self.assertFalse(result["outputs"]["B"]["A_cleanly_excluded_from_completed_BDC_mean"])
            self.assertFalse(result["interpretation"]["attack_hypothesis_confirmed"])

    def test_fallback_indicators_handle_lists_and_mappings(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "membership"
            _condition_artifacts(path, "membership")
            events_path = path / "events_0.jsonl"
            events = [json.loads(line) for line in events_path.read_text(encoding="utf-8").splitlines()]
            terminal = next(event for event in events if event["kind"] == "averaging_terminal" and event["node"] == "B")
            terminal["failed_size"] = {"failed_bytes": 0}
            terminal["failed_peer_ids"] = {"peer-D": "timeout"}
            events_path.write_text("".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")
            result = analyze_condition(path)
            self.assertTrue(result["outputs"]["B"]["fallback_indicated"])

            terminal["failed_peer_ids"] = {}
            events_path.write_text("".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")
            result = analyze_condition(path)
            self.assertFalse(result["outputs"]["B"]["fallback_indicated"])
            self.assertIn("B", result["interpretation"]["nodes_with_clean_A_exclusion"])

    def test_missing_result_stream_prevents_exclusion_claim(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "membership"
            _condition_artifacts(path, "membership")
            events_path = path / "events_0.jsonl"
            events = [json.loads(line) for line in events_path.read_text(encoding="utf-8").splitlines()]
            events = [event for event in events if not (
                event["kind"] == "result_chunk" and event["sender"] == "D" and event["receiver"] == "B"
            )]
            events_path.write_text("".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")
            result = analyze_condition(path)
            self.assertFalse(result["outputs"]["B"]["stream_proof_complete"])
            self.assertFalse(result["outputs"]["B"]["A_cleanly_excluded_from_completed_BDC_mean"])
            self.assertFalse(result["interpretation"]["attack_hypothesis_confirmed"])

    def test_missing_evidence_invalidates_condition(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "baseline4"
            _condition_artifacts(path, "baseline4")
            events_path = path / "events_0.jsonl"
            events = [json.loads(line) for line in events_path.read_text(encoding="utf-8").splitlines()]
            events = [event for event in events if not (event["kind"] == "announcement_received" and event["node"] == "A")]
            events_path.write_text("".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")
            result = analyze_condition(path)
            self.assertFalse(result["valid_evidence"])
            self.assertTrue(any("A: missing announcement_received" in error for error in result["evidence"]["errors"]))

    def test_bad_allocation_or_chunk_ranges_invalidates_condition(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "baseline4"
            _condition_artifacts(path, "baseline4")
            events_path = path / "events_0.jsonl"
            events = [json.loads(line) for line in events_path.read_text(encoding="utf-8").splitlines()]
            allocation = next(event for event in events if event["kind"] == "allocation" and event["node"] == "B")
            allocation["allocations"][0] = 107
            allocation["part_ranges"]["B"][0] = [36, 72]
            events_path.write_text("".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")
            result = analyze_condition(path)
            self.assertFalse(result["valid_evidence"])
            errors = "\n".join(result["evidence"]["errors"])
            self.assertIn("expected equal dense allocations", errors)
            self.assertIn("gap or overlap", errors)
            self.assertFalse(result["evidence"]["allocation_checks"]["B"]["valid"])

    def test_unequal_profile_checks_roster_metadata_and_native_allocation_reference(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "baseline4"
            _condition_artifacts(path, "baseline4")
            events = [json.loads(line) for line in (path / "events_0.jsonl").read_text().splitlines()]
            outputs = json.loads((path / "outputs.json").read_text())
            config = _unequal_config()
            for event in events:
                if event["kind"] == "local_roster":
                    event["bandwidths"] = [config["bandwidths"][member] for member in event["members"]]
                elif event["kind"] == "allocation":
                    expected = _unequal_allocation_event(event["node"], event["members"])
                    event.update({key: expected[key] for key in ("allocations", "modes", "parts", "part_ranges")})

            evidence = _evidence_checks("baseline4", events, outputs, True, config)
            self.assertTrue(evidence["valid"], evidence["errors"])
            self.assertTrue(all(check["valid"] for check in evidence["allocation_checks"].values()))
            self.assertEqual(evidence["allocation_checks"]["B"]["allocations"], [0, 94, 150, 188])
            self.assertEqual(evidence["allocation_checks"]["B"]["parts"], {"B": 0, "D": 3, "A": 4, "C": 5})
            allocation = next(event for event in events if event["kind"] == "allocation" and event["node"] == "B")
            self.assertEqual(allocation["modes"], [1, 0, 0, 0])
            (path / "config.json").write_text(json.dumps(config), encoding="utf-8")
            (path / "events_0.jsonl").write_text(
                "".join(json.dumps(event) + "\n" for event in events), encoding="utf-8"
            )
            analyzed = analyze_condition(path)
            self.assertTrue(all(check["valid"] for check in analyzed["evidence"]["allocation_checks"].values()))

            bad_metadata = [dict(event) for event in events]
            roster = next(event for event in bad_metadata if event["kind"] == "local_roster" and event["node"] == "B")
            roster["bandwidths"] = [1.0, 2.0, 2.5, 4.0]
            rejected = _evidence_checks("baseline4", bad_metadata, outputs, True, config)
            self.assertFalse(rejected["valid"])
            self.assertTrue(any("native bandwidths" in error for error in rejected["errors"]))

            bad_mode_events = [dict(event) for event in events]
            zero_fraction_allocation = next(
                event for event in bad_mode_events
                if event["kind"] == "allocation" and event["node"] == "B"
            )
            zero_fraction_allocation["modes"] = [0, 0, 0, 0]
            rejected_mode = _evidence_checks("baseline4", bad_mode_events, outputs, True, config)
            self.assertFalse(rejected_mode["valid"])
            self.assertTrue(any("mode" in error.lower() for error in rejected_mode["errors"]))
            wrong_config = _unequal_config()
            wrong_config["bandwidths"]["A"] = 2.5
            rejected_config = _evidence_checks("baseline4", events, outputs, True, wrong_config)
            self.assertFalse(rejected_config["valid"])
            self.assertTrue(any("config bandwidths differ" in error for error in rejected_config["errors"]))

    def test_unequal_profile_rejects_wrong_dense_counts_and_chunk_ownership(self) -> None:
        config = _unequal_config()
        for members in (list(NODES), ["A", "D", "B", "C"], ["B", "D", "C"]):
            with self.subTest(roster=members):
                event = _unequal_allocation_event("B", members)
                self.assertEqual(_allocation_errors(event, "B", members, config), [])

                wrong_counts = dict(event)
                wrong_counts["allocations"] = list(event["allocations"])
                wrong_counts["allocations"][0] += 1
                errors = _allocation_errors(wrong_counts, "B", members, config)
                self.assertTrue(any("expected configured dense allocations" in error for error in errors), errors)

                wrong_ranges = dict(event)
                wrong_ranges["part_ranges"] = {peer: [list(interval) for interval in ranges]
                                               for peer, ranges in event["part_ranges"].items()}
                owner = next(peer for peer in members if wrong_ranges["part_ranges"][peer])
                wrong_ranges["part_ranges"][owner][0][0] += 36
                errors = _allocation_errors(wrong_ranges, "B", members, config)
                self.assertTrue(any("part ranges differ from dense-allocation chunk ownership" in error
                                    for error in errors), errors)

    def test_native_allocation_mode_weight_and_compression_are_checked(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "baseline4"
            _condition_artifacts(path, "baseline4")
            events_path = path / "events_0.jsonl"
            events = [json.loads(line) for line in events_path.read_text(encoding="utf-8").splitlines()]
            allocation = next(event for event in events if event["kind"] == "allocation" and event["node"] == "B")
            allocation["modes"][0] = "CLIENT"
            allocation["weight"] = 0.5
            allocation["compression"] = "SomeCompression"
            events_path.write_text("".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")
            result = analyze_condition(path)
            self.assertFalse(result["valid_evidence"])
            errors = "\n".join(result["evidence"]["errors"])
            self.assertIn("expected modes", errors)
            self.assertIn("weight is missing or differs from 1", errors)
            self.assertIn("did not record NoCompression", errors)
            self.assertFalse(result["evidence"]["allocation_checks"]["B"]["valid"])

    def test_identity_marker_and_announcement_bytes_are_required(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "baseline4"
            _condition_artifacts(path, "baseline4")
            outputs_path = path / "outputs.json"
            outputs = json.loads(outputs_path.read_text(encoding="utf-8"))
            outputs["A"]["initial"][0][3] = 0.0
            outputs_path.write_text(json.dumps(outputs), encoding="utf-8")
            events_path = path / "events_0.jsonl"
            events = [json.loads(line) for line in events_path.read_text(encoding="utf-8").splitlines()]
            received = next(event for event in events if event["kind"] == "announcement_received" and event["node"] == "D")
            received["payload_hex"] = "ffff"
            events_path.write_text("".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")
            result = analyze_condition(path)
            self.assertFalse(result["valid_evidence"])
            errors = "\n".join(result["evidence"]["errors"])
            self.assertIn("initial tensor differs from the planned identity-marker input", errors)
            self.assertIn("received announcement payload_hex differs", errors)

    def test_suite_writes_machine_and_human_reports(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "suite"
            for condition in ("baseline4", "control3", "ordering", "membership"):
                _condition_artifacts(root / condition, condition)
            membership_events = root / "membership" / "events_0.jsonl"
            event_rows = [json.loads(line) for line in membership_events.read_text(encoding="utf-8").splitlines()]
            event_rows.extend([
                {"kind": "averager_rpc_started", "node": "B", "time": 1.55, "pid": 100,
                 "sender": "A", "receiver": "B", "stream_id": "rejected-A"},
                {"kind": "averager_rpc_code", "node": "B", "time": 1.56, "pid": 100,
                 "sender": "A", "receiver": "B", "stream_id": "rejected-A", "code": 14,
                 "code_name": "PROTOCOL_VIOLATION"},
                {"kind": "averager_rpc_closed", "node": "B", "time": 1.57, "pid": 100,
                 "sender": "A", "receiver": "B", "stream_id": "rejected-A"},
            ])
            membership_events.write_text("".join(json.dumps(event) + "\n" for event in event_rows), encoding="utf-8")
            result = analyze_suite(root)
            self.assertTrue(result["all_evidence_valid"])
            self.assertEqual(result["suite_evidence_errors"], [])
            membership_result = next(item for item in result["results"] if item["condition"] == "membership")
            dispatcher = membership_result["dispatcher_events"]
            self.assertEqual(len(dispatcher), 3)
            self.assertTrue(any(item["code_name"] == "PROTOCOL_VIOLATION" for item in dispatcher))
            self.assertTrue((root / "analysis.json").is_file())
            self.assertTrue((root / "REPORT.md").is_file())
            report = (root / "REPORT.md").read_text(encoding="utf-8")
            self.assertIn("B assembled", report)
            self.assertNotIn("**input_chunk**", report)
            self.assertNotIn("**result_chunk**", report)
            dashboard = (root / "DASHBOARD.html").read_text(encoding="utf-8")
            self.assertIn("membership", dashboard)
            self.assertIn("B aggregation", dashboard)
            self.assertIn("Coordination timeline", dashboard)
            self.assertIn("PROTOCOL_VIOLATION", report)
            self.assertIn("averager_rpc_code", dashboard)

    def test_suite_runner_failure_cannot_be_reported_as_valid(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "suite"
            for condition in ("baseline4", "control3", "ordering", "membership"):
                _condition_artifacts(root / condition, condition)
            (root / "status.json").write_text(json.dumps({"status": "infrastructure_error", "failed": "baseline4"}), encoding="utf-8")
            result = analyze_suite(root)
            self.assertFalse(result["all_evidence_valid"])
            self.assertIn("infrastructure_error", result["run_status_error"])
            self.assertIn("Suite run status", (root / "REPORT.md").read_text(encoding="utf-8"))

    def test_suite_rejects_reused_group_identity_and_private_prefix(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "suite"
            for condition in ("baseline4", "control3", "ordering", "membership"):
                _condition_artifacts(root / condition, condition)
            baseline = root / "baseline4"
            control = root / "control3"
            baseline_config = json.loads((baseline / "config.json").read_text(encoding="utf-8"))
            control_config = json.loads((control / "config.json").read_text(encoding="utf-8"))
            control_config["prefix"] = baseline_config["prefix"]
            (control / "config.json").write_text(json.dumps(control_config), encoding="utf-8")
            baseline_ids = json.loads((baseline / "identities.json").read_text(encoding="utf-8"))
            control_ids = json.loads((control / "identities.json").read_text(encoding="utf-8"))
            control_ids["B"] = baseline_ids["B"]
            (control / "identities.json").write_text(json.dumps(control_ids), encoding="utf-8")
            baseline_events = [json.loads(line) for line in (baseline / "events_0.jsonl").read_text(encoding="utf-8").splitlines()]
            control_events = [json.loads(line) for line in (control / "events_0.jsonl").read_text(encoding="utf-8").splitlines()]
            baseline_group = next(event["native_group_id"] for event in baseline_events if event["kind"] == "allocation")
            control_group = next(event["native_group_id"] for event in control_events if event["kind"] == "allocation")
            for event in control_events:
                for field in ("native_group_id", "group_id", "input_group_id"):
                    if event.get(field) == control_group:
                        event[field] = baseline_group
                if isinstance(event.get("stream_id"), str):
                    event["stream_id"] = event["stream_id"].replace(control_group, baseline_group)
            (control / "events_0.jsonl").write_text("".join(json.dumps(event) + "\n" for event in control_events), encoding="utf-8")
            result = analyze_suite(root)
            self.assertFalse(result["all_evidence_valid"])
            self.assertTrue(any("reuses peer identity" in issue for issue in result["suite_evidence_errors"]))
            self.assertTrue(any("reuses native group ID" in issue for issue in result["suite_evidence_errors"]))
            self.assertTrue(any("reuses private prefix" in issue for issue in result["suite_evidence_errors"]))


class AnnouncementProbeHelperTests(unittest.TestCase):
    def test_roster_for_matches_all_four_condition_views(self) -> None:
        expected = {
            "baseline4": {node: ["B", "D", "A", "C"] for node in NODES},
            "control3": {node: ["B", "D", "C"] for node in NODES},
            "ordering": {node: (["A", "D", "B", "C"] if node == "A" else ["B", "D", "A", "C"]) for node in NODES},
            "membership": {node: (["B", "D", "A", "C"] if node == "A" else ["B", "D", "C"]) for node in NODES},
        }
        for condition, per_node in expected.items():
            for node, roster in per_node.items():
                with self.subTest(condition=condition, node=node):
                    self.assertEqual(native_probe.roster_for(condition, node), roster)

    def test_reorder_keeps_opaque_gathered_metadata_with_each_peer(self) -> None:
        hivemind_pkg = types.ModuleType("hivemind")
        hivemind_pkg.__path__ = []
        averaging_pkg = types.ModuleType("hivemind.averaging")
        averaging_pkg.__path__ = []
        group_info_module = types.ModuleType("hivemind.averaging.group_info")

        class FakeGroupInfo:
            def __init__(self, group_id, peer_ids, gathered):
                self.group_id = group_id
                self.peer_ids = peer_ids
                self.gathered = gathered

        group_info_module.GroupInfo = FakeGroupInfo
        group = FakeGroupInfo(b"group", ("peer-b", "peer-d", "peer-a", "peer-c"),
                              (b"opaque-b", b"opaque-d", b"opaque-a", b"opaque-c"))
        old_identities = native_probe.IDENTITIES
        native_probe.IDENTITIES = {"peer-b": "B", "peer-d": "D", "peer-a": "A", "peer-c": "C"}
        try:
            with patch.dict(sys.modules, {
                "hivemind": hivemind_pkg,
                "hivemind.averaging": averaging_pkg,
                "hivemind.averaging.group_info": group_info_module,
            }):
                reordered = native_probe.reorder(group, ["A", "D", "B", "C"])
        finally:
            native_probe.IDENTITIES = old_identities
        self.assertEqual(reordered.peer_ids, ("peer-a", "peer-d", "peer-b", "peer-c"))
        self.assertEqual(reordered.gathered, (b"opaque-a", b"opaque-d", b"opaque-b", b"opaque-c"))
        self.assertEqual(reordered.group_id, b"group")

    def test_reorder_rejects_missing_or_unknown_peer(self) -> None:
        hivemind_pkg = types.ModuleType("hivemind")
        hivemind_pkg.__path__ = []
        averaging_pkg = types.ModuleType("hivemind.averaging")
        averaging_pkg.__path__ = []
        group_info_module = types.ModuleType("hivemind.averaging.group_info")

        class FakeGroupInfo:
            def __init__(self, group_id, peer_ids, gathered):
                self.group_id = group_id
                self.peer_ids = peer_ids
                self.gathered = gathered

        group_info_module.GroupInfo = FakeGroupInfo
        group = FakeGroupInfo(b"group", ("peer-b", "peer-d"), (b"opaque-b", b"opaque-d"))
        old_identities = native_probe.IDENTITIES
        native_probe.IDENTITIES = {"peer-b": "B", "peer-d": "D"}
        try:
            with patch.dict(sys.modules, {
                "hivemind": hivemind_pkg,
                "hivemind.averaging": averaging_pkg,
                "hivemind.averaging.group_info": group_info_module,
            }):
                with self.assertRaises(ValueError):
                    native_probe.reorder(group, ["B", "A"])
        finally:
            native_probe.IDENTITIES = old_identities


if __name__ == "__main__":
    unittest.main()
