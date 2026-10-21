"""Synthetic raw-evidence tests for the compute exclusion analyzer."""
from __future__ import annotations

import json
from pathlib import Path
import struct
import sys
import tempfile
import unittest

import numpy as np

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from analyze_compute_exclusion import analyze_case
from compute_workload import (
    BATCH_SIZE, DIMENSIONS, NODES, SHARE70_SAMPLES_PER_SECOND,
    evaluate_models, evaluation,
)
from run_compute_exclusion import marker_samples


def _write(path: Path, name: str, data) -> None:
    (path / name).write_text(json.dumps(data, allow_nan=False) + "\n")


def _event(node, kind, *, mono=101.0, time=1.0, channel=None, **fields):
    event = dict(kind=kind, node=node, worker=node, pid=1000 + NODES.index(node),
                 monotonic=mono, time=time, **fields)
    if channel is not None:
        event["channel"] = channel
        event["prefix"] = "fixture_grad_averager"
        event["traceprefix"] = "fixture_grad_averager"
    return event


def _protobuf_chunk(group_id: bytes, weight: float, code: int = 4) -> str:
    body = bytearray()
    if code:
        body.extend((8, code))
    body.extend((18, len(group_id)))
    body.extend(group_id)
    body.append(41)  # protobuf field 5, fixed64 weight
    body.extend(struct.pack("<d", weight))
    return body.hex()


def _base_case(path: Path, *, mode="markers", condition="baseline", seed=11,
               duration=1, outcome=None):
    active = [node for node in NODES if node != "A" or condition != "absent"]
    config = dict(schema="compute-native-case-v1", mode=mode, condition=condition,
                  seed=seed, duration=duration, active=active, run_id="fixture",
                  target_batch_size=4096, rates=SHARE70_SAMPLES_PER_SECOND,
                  microbatch_size=BATCH_SIZE, state_sharing=True, dim=DIMENSIONS,
                  learning_rate=.1)
    _write(path, "config.json", config)
    _write(path, "identities.json", {f"peer-{node}": node for node in active})
    _write(path, "release.json", {"monotonic": 100.0, "ready": sorted(active)})
    _write(path, "outcome.json", outcome or {
        "status": "completed", "completed": True, "observed_seconds": duration + .1,
    })
    _write(path, "shutdown.json", {"safe": True, "surviving_pids": []})
    rows = {node: [] for node in active}
    for node in active:
        rows[node].append(_event(node, "dht_started", mono=99.0, time=None,
                                 addresses=["/ip4/127.0.0.1/tcp/12345"]))
        if node == "B":
            rows[node].append(_event(node, "leader_advertisement", mono=99.2, time=None,
                                     channel="gradient", peer="B", declared=True, mode="NODE"))
        else:
            rows[node].append(_event(node, "leader_advertisement_suppressed", mono=99.2,
                                     time=None, channel="gradient", peer=node, mode="NODE"))
    return active, rows


def _marker_case(path: Path):
    active, rows = _base_case(path)
    seed = 11
    weights = {node: BATCH_SIZE * (7 if node == "A" else 1) for node in active}
    inputs = {}
    for node in active:
        count = 7 if node == "A" else 1
        gradients = []
        for index in range(count):
            samples = marker_samples(seed, node, index)
            gradient = samples.mean(axis=0)
            gradients.append(gradient)
            rows[node].append(_event(
                node, "marker_batch", mono=101.0 + index * .01, time=1.0 + index * .01,
                batch_index=index, sample_count=BATCH_SIZE,
                sample_ids=[f"{seed}:{node}:{index}:{i}" for i in range(BATCH_SIZE)],
                gradient=gradient.tolist(),
            ))
        inputs[node] = np.mean(gradients, axis=0)
        rows[node].append(_event(node, "marker_accumulated", initial=inputs[node].tolist(),
                                 weight=weights[node], batches=count))

    ordered = list(NODES)
    peer_ids = [f"{i + 1:04x}" for i in range(len(ordered))]
    gathered = [f"aa{i + 1:02x}" for i in range(len(ordered))]
    ranges = {node: [{"tensor_index": 0, "start": 12 * i,
                      "end": 12 * (i + 1), "elements": 12}]
              for i, node in enumerate(ordered)}
    denominator = sum(weights.values())
    expected = sum(inputs[node] * weights[node] for node in active) / denominator
    group_id = bytes.fromhex("01020304")
    group_hex = group_id.hex()

    for node in active:
        rows[node].extend([
            _event(node, "local_roster", channel="gradient", group_id=group_hex,
                   peer=node, members=ordered, peer_ids_hex=peer_ids, gathered_hex=gathered,
                   native_members=ordered, native_peer_ids_hex=peer_ids,
                   native_gathered_hex=gathered, excluded=[]),
            _event(node, "aggregate_input", channel="gradient", group_id=group_hex,
                   peer=node, members=ordered, peer_ids_hex=peer_ids, gathered_hex=gathered,
                   values=[inputs[node].tolist()]),
            _event(node, "allocation", channel="gradient", group_id=group_hex,
                   peer=node, members=ordered, peer_ids_hex=peer_ids,
                   peer_fractions=[1.0] * len(ordered), weight=weights[node],
                   parts=[1] * len(ordered), part_ranges=ranges, total_elements=DIMENSIONS),
        ])
        for reducer, reducer_range in ranges.items():
            start, end = reducer_range[0]["start"], reducer_range[0]["end"]
            rows[node].append(_event(
                node, "processed_chunk", channel="gradient", group_id=group_hex,
                reducer=reducer, part_index=0,
                values=(expected[start:end] - inputs[node][start:end]).tolist(),
            ))
        rows[node].append(_event(node, "aggregate_output", channel="gradient",
                                 group_id=group_hex, peer=node, members=ordered,
                                 peer_ids_hex=peer_ids, gathered_hex=gathered,
                                 values=[expected.tolist()]))

    # Each peer sends the recipient's owned coordinate slice once. The wire
    # form stays byte-identical at both ends, including opaque group and weight.
    for sender in active:
        for receiver in active:
            if sender == receiver:
                continue
            part = ranges[receiver][0]
            start, end = part["start"], part["end"]
            fields = dict(channel="gradient", group_id=group_hex, input_group_id=group_hex,
                          sender=sender, receiver=receiver, part_index=0, weight=weights[sender],
                          code=4, payload_hex=_protobuf_chunk(group_id, weights[sender]),
                          values=inputs[sender][start:end].tolist())
            rows[sender].append(_event(sender, "input_chunk", direction="sent", **fields))
            rows[receiver].append(_event(receiver, "input_chunk", direction="received", **fields))

    for node in active:
        count = 7 if node == "A" else 1
        final = expected.tolist()
        initial = inputs[node].tolist()
        rows[node].append(_event(node, "marker_terminal", initial=initial, final=final,
                                 weight=weights[node], status="success"))
        _write(path, f"output_{node}.json", dict(status="success", weight=weights[node],
                batches=count, initial=initial, final=final))
        event_file = path / f"events_{node}_{1000 + NODES.index(node)}.jsonl"
        event_file.write_text("".join(json.dumps(row, allow_nan=False) + "\n" for row in rows[node]))


def _training_case(path: Path, *, native_noncompletion=False):
    duration = 2 if native_noncompletion else 1
    outcome = ({"status": "native_noncompletion", "completed": False,
                "stuck_nodes": ["D"], "observed_seconds": duration + .2,
                "measured_horizon": duration} if native_noncompletion else None)
    active, rows = _base_case(path, mode="smoke", duration=duration, outcome=outcome)
    x, y = evaluation(11)
    np.savez_compressed(path / "evaluation_data.npz", x=x, y=y)
    models = {node: np.zeros(DIMENSIONS, dtype=np.float32) for node in active}
    initial = evaluate_models(models, x, y)
    thresholds = {node: .1 * initial["per_node"][node] for node in ("D", "C")}
    trace = []
    for tick in range(duration + 1):
        current = dict(models)
        if native_noncompletion:
            current.pop("D")
        metrics = evaluate_models(current, x, y)
        trace.append(dict(time=float(tick), scheduled_time=tick, capture_lag=0.0,
                          weights={node: value.tolist() for node, value in current.items()},
                          metrics=metrics, thresholds=thresholds))
    (path / "evaluations.jsonl").write_text(
        "".join(json.dumps(row, allow_nan=False) + "\n" for row in trace))
    for node in active:
        if not native_noncompletion or node != "D":
            rows[node].append(_event(node, "training_terminal", mono=100 + duration + .1,
                                     time=duration + .1, batches=0, epoch=0,
                                     parameters=np.zeros(DIMENSIONS).tolist()))
        event_file = path / f"events_{node}_{1000 + NODES.index(node)}.jsonl"
        event_file.write_text("".join(json.dumps(row, allow_nan=False) + "\n" for row in rows[node]))


class AnalyzeComputeExclusionTests(unittest.TestCase):
    def test_marker_evidence_recomputes_native_weighted_outputs(self):
        with tempfile.TemporaryDirectory() as directory:
            case = Path(directory)
            _marker_case(case)
            report = analyze_case(case)
            self.assertTrue(report["valid"], report["errors"])
            self.assertEqual(report["reducer_evidence"]["groups_reconstructed"], 4)
            self.assertEqual(report["reducer_evidence"]["weighted_coordinates_checked"], DIMENSIONS)
            self.assertEqual(report["marker"]["native_expected_group"], "all")

    def test_fallback_preserves_local_coordinates_and_rejects_nonzero_delta(self):
        from analyze_compute_exclusion import _verify_group_outputs
        vector = np.arange(48, dtype=float).tolist()
        fields = dict(node='A', channel='gradient', prefix='fixture_grad_averager', group_id='abcd', monotonic=1.)
        ranges = {n: [dict(tensor_index=0, start=i*24, end=(i+1)*24, elements=24)] for i,n in enumerate(('A','B'))}
        events = [dict(fields, kind='allocation', members=['A','B'], weight=224., part_ranges=ranges),
                  dict(fields, kind='local_roster', members=['A','B']),
                  dict(fields, kind='aggregate_input', values=[vector]),
                  dict(fields, kind='aggregate_output', values=[vector]),
                  dict(fields, kind='processed_chunk', reducer='A', part_index=0, values=[0.]*24),
                  dict(fields, kind='processed_chunk', reducer='B', part_index=0, values=[0.]*24),
                  dict(fields, kind='fallback', reducer='B', registered_parts=0, part_count=1),
                  dict(fields, kind='sender_banned', sender='B')]
        errors = []
        report = _verify_group_outputs(events, errors)
        self.assertEqual(errors, [])
        self.assertEqual(report['fallbacks'][0]['coordinates'], list(range(24,48)))
        next(e for e in events if e['kind']=='processed_chunk' and e['reducer']=='B')['values'][0] = 1.
        errors = []
        _verify_group_outputs(events, errors)
        self.assertTrue(any('fallback chunk' in error for error in errors))

    def test_marker_sample_tampering_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            case = Path(directory)
            _marker_case(case)
            event_path = case / "events_B_1000.jsonl"
            events = [json.loads(line) for line in event_path.read_text().splitlines()]
            target = next(event for event in events if event["kind"] == "marker_batch")
            target["gradient"][0] += .25
            event_path.write_text("".join(json.dumps(event) + "\n" for event in events))
            report = analyze_case(case)
            self.assertFalse(report["valid"])
            self.assertTrue(any("marker gradient" in error for error in report["errors"]))

    def test_training_metrics_are_recomputed_and_native_censor_is_reported(self):
        with tempfile.TemporaryDirectory() as directory:
            case = Path(directory)
            _training_case(case)
            report = analyze_case(case)
            self.assertTrue(report["valid"], report["errors"])
            self.assertTrue(report["learning"]["censored"])
            self.assertEqual(report["learning"]["censor_reason"], "horizon")

        with tempfile.TemporaryDirectory() as directory:
            case = Path(directory)
            _training_case(case, native_noncompletion=True)
            report = analyze_case(case)
            self.assertTrue(report["valid"], report["errors"])
            self.assertFalse(report["completed"])
            self.assertFalse(report["protocol_complete"])
            self.assertTrue(report["learning"]["censored"])
            self.assertEqual(report["learning"]["censor_reason"], "missing_node_D")

    def test_training_metric_tampering_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            case = Path(directory)
            _training_case(case)
            rows = [json.loads(line) for line in (case / "evaluations.jsonl").read_text().splitlines()]
            rows[1]["metrics"]["per_node"]["C"] += .5
            (case / "evaluations.jsonl").write_text(
                "".join(json.dumps(row) + "\n" for row in rows))
            report = analyze_case(case)
            self.assertFalse(report["valid"])
            self.assertTrue(any("reported losses differ" in error for error in report["errors"]))


if __name__ == "__main__":
    unittest.main()
