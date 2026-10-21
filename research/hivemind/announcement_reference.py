"""Independent evidence checks for a native numerical averaging operation."""
from __future__ import annotations

import json
import math
from pathlib import Path
import sys

from analyze_announcement_probe import (
    _read_events, _shutdown_check, _identity_check, _source_provenance_check,
    _stream_proof, _processed_output_proof,
)

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "research/simulator/src"))
from trainsim.protocols.partitioning import allocate_elements, assign_chunks

NODES = ("B", "D", "A", "C")


def numeric_matrix(value):
    if not isinstance(value, list) or not value:
        return None
    flat = []
    def visit(item):
        if isinstance(item, list):
            return bool(item) and all(visit(child) for child in item)
        if isinstance(item, bool) or not isinstance(item, (int, float)) or not math.isfinite(item):
            return False
        flat.append(float(item))
        return True
    return [flat] if visit(value) else None


def verify_native_round(path: str | Path) -> dict:
    try:
        return _verify_native_round(path)
    except (OSError, ValueError, KeyError, TypeError) as exc:
        return {"valid": False, "errors": [f"Malformed or missing native evidence: {exc}"],
                "condition": None, "native_source": {}, "streams": {}, "processed": {}}


def _verify_native_round(path: str | Path) -> dict:
    path = Path(path)
    errors = []
    config = json.loads((path / "config.json").read_text())
    outputs = json.loads((path / "outputs.json").read_text())
    if set(outputs) != set(NODES):
        errors.append("Native outputs must contain exactly B, D, A, C")
    events, read_errors = _read_events(path)
    errors.extend(read_errors)
    safe, shutdown_error = _shutdown_check(path)
    if not safe:
        errors.append(shutdown_error or "Shutdown did not pass")
    _, identity_errors = _identity_check(path)
    errors.extend(identity_errors)
    source, source_errors = _source_provenance_check(path)
    errors.extend(source_errors)
    condition = config["condition"]
    if condition not in ("baseline4", "membership", "ordering"):
        errors.append(f"Unsupported numerical reference condition {condition}")
    if config.get("tensor_shape") != [48] or config.get("chunk_bytes") != 16:
        errors.append("Native training requires a 48-element vector with 16-byte chunks")
    rosters, allocation = {}, {}
    for node in NODES:
        expected = list(NODES)
        if condition == "membership" and node != "A":
            expected = ["B", "D", "C"]
        elif condition == "ordering" and node == "A":
            expected = ["A", "D", "B", "C"]
        local = [e for e in events if e.get("kind") == "local_roster" and e.get("node") == node]
        layouts = [e for e in events if e.get("kind") == "allocation" and e.get("node") == node]
        terminals = [e for e in events if e.get("kind") == "averaging_terminal" and e.get("node") == node]
        if len(local) != 1 or len(layouts) != 1 or len(terminals) != 1:
            errors.append(f"{node}: expected one local roster, allocation and terminal event")
            continue
        rosters[node] = local[0]["members"]
        allocation[node] = layouts[0]
        if rosters[node] != expected or local[0].get("gathered_nodes") != expected:
            errors.append(f"{node}: incorrect roster or metadata alignment")
        counts = allocate_elements(48, [None] * len(expected))
        parts = assign_chunks(48, 4, counts)
        ranges = {peer: [[4 * i, 4 * i + 4] for i in chunks] for peer, chunks in zip(expected, parts)}
        recorded = layouts[0]
        if (recorded.get("allocations") != list(counts) or recorded.get("part_ranges") != ranges
                or recorded.get("parts") != {peer: len(chunks) for peer, chunks in zip(expected, parts)}):
            errors.append(f"{node}: allocation differs from independent equal partition")
        if (recorded.get("weight") != 1 or recorded.get("compression") != "NoCompression"
                or local[0].get("bandwidths") != [None] * len(expected)
                or local[0].get("modes") != [0] * len(expected)
                or recorded.get("modes") != [0] * len(expected)
                or recorded.get("members") != expected):
            errors.append(f"{node}: unexpected numerical averaging settings")
        record = outputs.get(node, {})
        if record.get("status") != "success" or terminals[0].get("status") != "success":
            errors.append(f"{node}: native training operation did not succeed")
        for key in ("initial", "final"):
            matrix = numeric_matrix(record.get(key))
            if (matrix is None or len(matrix[0]) != 48
                    or len(record[key]) != 48 or any(isinstance(v, list) for v in record[key])):
                errors.append(f"{node}: invalid {key} gradient")
        for key in ("failed_size", "failed_peer_ids", "banned_senders"):
            if record.get(key) != terminals[0].get(key):
                errors.append(f"{node}: output {key} differs from terminal evidence")
    sent_events = [e for e in events if e.get("kind") == "announcement_sent"]
    received_events = [e for e in events if e.get("kind") == "announcement_received"]
    if (len(sent_events) != 3 or len(received_events) != 3
            or sorted(e.get("recipient", "") for e in sent_events) != ["A", "C", "D"]
            or sorted(e.get("node", "") for e in received_events) != ["A", "C", "D"]):
        errors.append("Expected exactly one sent and received announcement per follower")
    sent = {e.get("recipient"): e for e in sent_events}
    received = {e.get("node"): e for e in received_events}
    for node in ("D", "A", "C"):
        s, r = sent.get(node), received.get(node)
        if s is None or r is None:
            errors.append(f"{node}: missing announcement")
        elif s.get("node") != "B" or r.get("sender") != "B" or not s.get("payload_hex"):
            errors.append(f"{node}: invalid announcement origin or missing bytes")
        elif any(s.get(k) != r.get(k) for k in ("payload_hex", "group_id", "members", "gathered_nodes")):
            errors.append(f"{node}: announcement bytes or decoded metadata differ")
        elif r.get("members") != rosters.get(node):
            errors.append(f"{node}: aggregation differs from announced roster")
        elif r.get("gathered_nodes") != rosters.get(node):
            errors.append(f"{node}: announcement gathered metadata is not aligned")
        if s is not None:
            try:
                payload = s.get("payload_hex")
                if not isinstance(payload, str) or not bytes.fromhex(payload):
                    raise ValueError("empty payload")
            except ValueError:
                errors.append(f"{node}: announcement payload is not hexadecimal bytes")
    group_ids = {e.get("native_group_id") for e in events
                 if e.get("kind") in ("leader_assembled", "local_roster", "allocation", "runner_finalized")}
    if len(group_ids) != 1 or None in group_ids:
        errors.append("Inconsistent or missing native group identity")
    elif any(e.get("group_id") not in group_ids for e in sent_events + received_events):
        errors.append("Announcement group identity differs from native aggregation")
    if any(e.get("kind") in ("unexpected_leader", "infrastructure_error") for e in events):
        errors.append("Unexpected leader or native infrastructure error")
    streams = _stream_proof(events, list(NODES), outputs, matrix_parser=numeric_matrix)
    processed = _processed_output_proof(events, list(NODES), outputs, matrix_parser=numeric_matrix)
    for node in NODES:
        if not streams[node].get("evidence_complete", streams[node]["valid"]):
            errors.extend(f"{node}: {issue}" for issue in streams[node].get("evidence_issues", streams[node]["issues"]))
        if not processed[node]["valid"] or not processed[node]["complete"]:
            errors.extend(f"{node}: {issue}" for issue in processed[node]["issues"])
    return {"valid": not errors, "errors": errors, "condition": condition,
            "native_source": source, "streams": streams, "processed": processed}
