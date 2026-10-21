"""Validate raw evidence from the private native compute exclusion experiment.

This analyzer checks instrumentation and numerical evidence. It reports the
observed treatment effects separately; it does not treat exclusion succeeding
or failing to change training loss as an evidence-integrity failure.
"""
from __future__ import annotations

import json
import math
from pathlib import Path
import re
import sys
from typing import Any, Mapping

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

import numpy as np

from compute_workload import (
    BATCH_SIZE, CONDITIONS, DIMENSIONS, LEARNING_RATE, NODES,
    SHARE70_SAMPLES_PER_SECOND, batch, evaluate_models, evaluation,
    summarize_evaluations,
)
from run_compute_exclusion import marker_samples


SCHEMA = "compute-native-analysis-v1"
ATOL = 1e-5
RTOL = 1e-4


def _finite(value: Any) -> bool:
    if isinstance(value, bool) or value is None or isinstance(value, (str, int)):
        return True
    if isinstance(value, float):
        return math.isfinite(value)
    if isinstance(value, Mapping):
        return all(_finite(key) and _finite(item) for key, item in value.items())
    if isinstance(value, (list, tuple)):
        return all(_finite(item) for item in value)
    return True


def _read_json(path: Path, errors: list[str], required: bool = True):
    try:
        return json.loads(path.read_text())
    except FileNotFoundError:
        if required:
            errors.append(f"missing {path.name}")
    except (OSError, json.JSONDecodeError) as exc:
        errors.append(f"cannot read {path.name}: {exc}")
    return None


def _read_jsonl(path: Path, errors: list[str]) -> list[dict[str, Any]]:
    rows = []
    try:
        with path.open() as stream:
            for line_no, line in enumerate(stream, 1):
                try:
                    row = json.loads(line)
                except json.JSONDecodeError as exc:
                    errors.append(f"{path.name}:{line_no}: invalid JSON: {exc}")
                    continue
                if not isinstance(row, dict):
                    errors.append(f"{path.name}:{line_no}: event must be an object")
                    continue
                if not _finite(row):
                    errors.append(f"{path.name}:{line_no}: non-finite event value")
                row["_source"] = path.name
                row["_line"] = line_no
                rows.append(row)
    except OSError as exc:
        errors.append(f"cannot read {path.name}: {exc}")
    return rows


def _vec(value: Any, length: int = DIMENSIONS) -> np.ndarray | None:
    try:
        vector = np.asarray(value, dtype=np.float64)
    except (TypeError, ValueError):
        return None
    if vector.shape != (length,) or not np.isfinite(vector).all():
        return None
    return vector


def _same(left: Any, right: Any, *, atol: float = ATOL, rtol: float = RTOL) -> bool:
    try:
        return bool(np.allclose(np.asarray(left, dtype=np.float64),
                                np.asarray(right, dtype=np.float64), atol=atol, rtol=rtol,
                                equal_nan=False))
    except (TypeError, ValueError):
        return False


def _parts_vector(values: Any) -> np.ndarray | None:
    """Flatten the one-tensor vectors emitted by the workload's hooks."""
    if isinstance(values, list) and len(values) == 1 and isinstance(values[0], list):
        values = values[0]
    return _vec(values)


def _flat_vector(values: Any) -> np.ndarray | None:
    """Decode any finite nested JSON numeric array, including a tensor chunk."""
    if isinstance(values, Mapping):
        return None
    try:
        vector = np.asarray(values, dtype=np.float64).reshape(-1)
    except (TypeError, ValueError):
        return None
    return vector if vector.size and np.isfinite(vector).all() else None


def _same_metric(left: Any, right: Any) -> bool:
    if left is None or right is None:
        return left is None and right is None
    if isinstance(left, bool) or isinstance(right, bool):
        return False
    try:
        return math.isclose(float(left), float(right), rel_tol=RTOL, abs_tol=ATOL)
    except (TypeError, ValueError):
        return False


def _varint(data: bytes, offset: int) -> tuple[int, int]:
    result = shift = 0
    while offset < len(data):
        byte = data[offset]
        offset += 1
        result |= (byte & 0x7f) << shift
        if byte < 0x80:
            return result, offset
        shift += 7
        if shift > 70:
            break
    raise ValueError("invalid protobuf varint")


def _protobuf_fields(payload_hex: Any) -> dict[int, list[Any]]:
    """Read protobuf wire fields without importing the native Hivemind runtime."""
    if not isinstance(payload_hex, str) or len(payload_hex) % 2:
        raise ValueError("payload is not even-length hex")
    try:
        data = bytes.fromhex(payload_hex)
    except ValueError as exc:
        raise ValueError("payload is not valid hex") from exc
    fields: dict[int, list[Any]] = {}
    offset = 0
    while offset < len(data):
        tag, offset = _varint(data, offset)
        number, wire = tag >> 3, tag & 7
        if number == 0:
            raise ValueError("protobuf field number is zero")
        if wire == 0:
            value, offset = _varint(data, offset)
        elif wire == 1:
            if offset + 8 > len(data):
                raise ValueError("truncated fixed64 protobuf field")
            value, offset = data[offset:offset + 8], offset + 8
        elif wire == 2:
            size, offset = _varint(data, offset)
            if offset + size > len(data):
                raise ValueError("truncated length-delimited protobuf field")
            value, offset = data[offset:offset + size], offset + size
        elif wire == 5:
            if offset + 4 > len(data):
                raise ValueError("truncated fixed32 protobuf field")
            value, offset = data[offset:offset + 4], offset + 4
        else:
            raise ValueError(f"unsupported protobuf wire type {wire}")
        fields.setdefault(number, []).append(value)
    return fields


def _expect(condition: bool, errors: list[str], message: str) -> None:
    if not condition:
        errors.append(message)


def _roster_fields(event: Mapping[str, Any], errors: list[str], *, native: bool = False):
    suffix = "native_" if native else ""
    members = event.get(suffix + "members")
    peer_ids = event.get(suffix + "peer_ids_hex")
    gathered = event.get(suffix + "gathered_hex")
    if not all(isinstance(value, list) for value in (members, peer_ids, gathered)):
        errors.append(f"{event.get('kind')} lacks aligned {suffix}roster arrays")
        return None
    if not (len(members) == len(peer_ids) == len(gathered)):
        errors.append(f"{event.get('kind')} {suffix}roster arrays have different lengths")
        return None
    if len(set(members)) != len(members):
        errors.append(f"{event.get('kind')} {suffix}roster repeats a member")
    try:
        [bytes.fromhex(item) for item in peer_ids]
        [bytes.fromhex(item) for item in gathered]
    except (TypeError, ValueError):
        errors.append(f"{event.get('kind')} {suffix}roster contains invalid opaque hex")
        return None
    return list(zip(members, peer_ids, gathered))


def _validate_message_payload(event: Mapping[str, Any], errors: list[str], *, native: bool = False) -> None:
    suffix = "native_" if native else ""
    payload_key = suffix + "payload_hex"
    if payload_key not in event:
        return
    try:
        fields = _protobuf_fields(event[payload_key])
    except ValueError as exc:
        errors.append(f"{event.get('kind')} has invalid {payload_key}: {exc}")
        return
    group = event.get("group_id")
    if group is not None:
        try:
            expected_group = bytes.fromhex(group)
        except (TypeError, ValueError):
            errors.append(f"{event.get('kind')} group_id is not hex")
            return
        if fields.get(2, [None])[0] != expected_group:
            errors.append(f"{event.get('kind')} protobuf group id disagrees with event")
    roster = _roster_fields(event, errors, native=native)
    if roster is not None:
        ids = [bytes.fromhex(peer_id) for _, peer_id, _ in roster]
        metadata = [bytes.fromhex(gathered) for _, _, gathered in roster]
        if fields.get(4, []) != ids or fields.get(5, []) != metadata:
            errors.append(f"{event.get('kind')} protobuf payload disagrees with {suffix}roster")


def _validate_event_channels(events: list[dict[str, Any]], config: Mapping[str, Any],
                             active: list[str], errors: list[str]) -> dict[str, Any]:
    gradient_prefix = f"{config.get('run_id', '')}_grad_averager"
    channel_counts = {"gradient": 0, "state": 0}
    exclusions = []
    advertisements = {node: 0 for node in active}
    for event in events:
        if "channel" not in event:
            continue
        channel = event.get("channel")
        prefix = event.get("prefix")
        expected_channel = "gradient" if prefix == gradient_prefix else "state"
        _expect(channel == expected_channel, errors,
                f"{event.get('kind')} channel does not match exact prefix")
        _expect(event.get("traceprefix") == prefix, errors,
                f"{event.get('kind')} traceprefix disagrees with prefix")
        _expect(event.get("worker") == event.get("node"), errors,
                f"{event.get('kind')} worker disagrees with event node")
        if channel in channel_counts:
            channel_counts[channel] += 1
        kind = event.get("kind")
        if kind in {"announcement_sent", "announcement_received", "local_roster"}:
            current = _roster_fields(event, errors)
            native = _roster_fields(event, errors, native=True) if kind == "announcement_sent" or kind == "local_roster" else None
            _validate_message_payload(event, errors)
            if kind == "announcement_sent":
                _validate_message_payload(event, errors, native=True)
                excluded = event.get("excluded", [])
                if not isinstance(excluded, list):
                    errors.append("announcement_sent excluded field is not a list")
                    excluded = []
                if channel == "state" and (excluded or current != native):
                    errors.append("state announcement was altered by exclusion instrumentation")
                if channel == "gradient" and config.get("condition") != "exclusion" and (excluded or current != native):
                    errors.append("non-exclusion gradient announcement was altered")
                if channel == "gradient" and config.get("condition") == "exclusion":
                    recipient = event.get("recipient")
                    should_remove = recipient != "A" and native is not None and any(row[0] == "A" for row in native)
                    wanted = [row for row in native if row[0] != "A"] if should_remove else native
                    if excluded != (["A"] if should_remove else []) or current != wanted:
                        errors.append("gradient announcement exclusion did not preserve the expected opaque roster")
            elif kind == "local_roster":
                excluded = event.get("excluded", [])
                if channel == "state" and (excluded or current != native):
                    errors.append("state local roster was altered by exclusion instrumentation")
                if channel == "gradient" and config.get("condition") != "exclusion" and (excluded or current != native):
                    errors.append("non-exclusion gradient local roster was altered")
                if channel == "gradient" and config.get("condition") == "exclusion":
                    expected_remove = event.get("peer") == "B" and native is not None and any(row[0] == "A" for row in native)
                    wanted = [row for row in native if row[0] != "A"] if expected_remove else native
                    if excluded != (["A"] if expected_remove else []) or current != wanted:
                        errors.append("gradient local exclusion changed the wrong opaque roster")
                    if expected_remove:
                        exclusions.append({"node": event.get("node"), "group_id": event.get("group_id"),
                                           "native_members": [r[0] for r in native],
                                           "members": [r[0] for r in current]})
        if kind == "leader_advertisement" and channel == "gradient":
            peer = event.get("peer")
            if peer in advertisements:
                advertisements[peer] += 1
            if peer != "B" or event.get("declared") is not True:
                errors.append("a non-B peer advertised itself as a gradient leader")
        if kind == "leader_advertisement_suppressed" and channel == "gradient":
            if event.get("peer") == "B" or event.get("mode") != "NODE":
                errors.append("gradient leader suppression changed B's NODE role or mode")
    if not advertisements.get("B"):
        errors.append("no successful B gradient leader advertisement was observed")
    for node in active:
        if node != "B" and not advertisements[node]:
            suppressed = any(e.get("kind") == "leader_advertisement_suppressed"
                             and e.get("channel") == "gradient" and e.get("peer") == node for e in events)
            if not suppressed:
                errors.append(f"no B-only leader evidence for active gradient peer {node}")
    sent_messages = {}
    received_messages = {}
    for event in events:
        kind = event.get("kind")
        if kind not in {"announcement_sent", "announcement_received"}:
            continue
        sender = event.get("sender") if kind == "announcement_received" else event.get("sender")
        receiver = event.get("node") if kind == "announcement_received" else event.get("recipient")
        key = (sender, receiver, event.get("group_id"))
        target = sent_messages if kind == "announcement_sent" else received_messages
        target.setdefault(key, []).append(event)
    matched_messages = 0
    for key, receives in received_messages.items():
        sends = sent_messages.get(key, [])
        if len(sends) != len(receives):
            # A native timeout can leave unmatched announcements. Compare every
            # observed pair and keep coverage visible rather than inventing a
            # transport failure from an unpaired event.
            matched_messages += min(len(sends), len(receives))
        for sent, received in zip(sends, receives):
            if sent.get("payload_hex") != received.get("payload_hex"):
                errors.append("sent/received opaque announcement payload changed")
            if any(sent.get(field) != received.get(field)
                   for field in ("members", "peer_ids_hex", "gathered_hex")):
                errors.append("sent/received opaque announcement roster changed")
    treatment = {"gradient_leader_advertisements": advertisements,
                 "gradient_exclusions": exclusions,
                 "channel_event_counts": channel_counts,
                 "announcement_pairs_checked": matched_messages,
                 "unpaired_announcements": abs(sum(map(len, sent_messages.values())) -
                                                 sum(map(len, received_messages.values())))}
    return treatment


def _validate_opaque_chunk_pairs(events: list[dict[str, Any]], errors: list[str]) -> dict[str, Any]:
    sent, received = {}, {}
    for event in events:
        if event.get("kind") != "input_chunk" or event.get("channel") not in {"gradient", "state"}:
            continue
        direction = event.get("direction")
        sender, receiver = event.get("sender"), event.get("receiver")
        group_hex = event.get("input_group_id")
        try:
            group_bytes = bytes.fromhex(group_hex or "")
        except (TypeError, ValueError):
            errors.append("input_chunk opaque group id is invalid hex")
            continue
        if group_hex and group_hex != event.get("group_id"):
            errors.append("input_chunk opaque group id differs from its native group")
        key = (event.get("prefix"), event.get("group_id"), sender, receiver, event.get("part_index"))
        try:
            payload = _protobuf_fields(event.get("payload_hex"))
        except ValueError as exc:
            errors.append(f"input_chunk has invalid protobuf payload: {exc}")
            continue
        payload_group = payload.get(2, [b""])[0]
        if payload_group != group_bytes:
            errors.append("input_chunk protobuf group id disagrees with opaque group id")
        if payload.get(1, [0])[0] != event.get("code"):
            errors.append("input_chunk protobuf code disagrees with event")
        weight_fields = payload.get(5, [])
        if not weight_fields:
            wire_weight = 0.0
        elif len(weight_fields) == 1 and isinstance(weight_fields[0], bytes) and len(weight_fields[0]) == 8:
            import struct
            wire_weight = struct.unpack("<d", weight_fields[0])[0]
        else:
            errors.append("input_chunk protobuf has malformed fixed64 weight")
            wire_weight = None
        if (wire_weight is not None and isinstance(event.get("weight"), (int, float))
                and not math.isclose(wire_weight, float(event["weight"]), rel_tol=1e-12, abs_tol=1e-12)):
            errors.append("input_chunk protobuf weight disagrees with event")
        chunk_values = _flat_vector(event.get("values"))
        _expect(chunk_values is not None,
                errors, "input_chunk decoded values are malformed/non-finite")
        target = sent if direction == "sent" else received if direction == "received" else None
        if target is None:
            errors.append("input_chunk direction is invalid")
        else:
            target.setdefault(key, []).append(event)
    paired = missing = 0
    for key, send_rows in sent.items():
        recv_rows = received.get(key, [])
        if not recv_rows:
            missing += len(send_rows)
            continue  # a logged disconnect may be followed by an explicit fallback
        paired += min(len(send_rows), len(recv_rows))
        if len(send_rows) != len(recv_rows):
            errors.append("sent/received input chunk counts disagree")
        for left, right in zip(send_rows, recv_rows):
            if left.get("payload_hex") != right.get("payload_hex"):
                errors.append("sent/received opaque tensor payload changed in transit")
            if left.get("values") != right.get("values") or left.get("weight") != right.get("weight"):
                errors.append("sent/received decoded tensor values or weight changed")
            if left.get("code") != right.get("code"):
                errors.append("sent/received input chunk code changed")
    return {"sent_chunks": sum(map(len, sent.values())),
            "received_chunks": sum(map(len, received.values())),
            "paired_chunks": paired, "sent_without_receive": missing}


def _group_key(event: Mapping[str, Any]):
    return (event.get("node"), event.get("prefix"), event.get("group_id"))


def _ranges(allocation: Mapping[str, Any], errors: list[str]) -> dict[str, list[tuple[int, int]]]:
    ranges = allocation.get("part_ranges")
    members = allocation.get("members")
    result: dict[str, list[tuple[int, int]]] = {}
    if not isinstance(ranges, Mapping) or not isinstance(members, list):
        errors.append("allocation lacks members or part_ranges")
        return result
    if set(ranges) != set(members):
        errors.append("allocation part_ranges keys differ from members")
    coverage = np.zeros(DIMENSIONS, dtype=np.int32)
    for peer in members:
        rows = ranges.get(peer, [])
        if not isinstance(rows, list):
            errors.append("allocation peer ranges are not lists")
            continue
        peer_ranges = []
        for row in rows:
            if not isinstance(row, Mapping):
                errors.append("allocation part range is not an object")
                continue
            start, end = row.get("start"), row.get("end")
            if (not isinstance(start, int) or not isinstance(end, int) or
                    start < 0 or end <= start or end > DIMENSIONS or
                    row.get("elements") != end - start or row.get("tensor_index") != 0):
                errors.append("allocation has an invalid coordinate range")
                continue
            coverage[start:end] += 1
            peer_ranges.append((start, end))
        result[peer] = peer_ranges
    if not np.all(coverage == 1):
        errors.append("allocation ranges do not cover each model coordinate exactly once")
    return result


def _verify_group_outputs(events: list[dict[str, Any]], errors: list[str],
                         *, allow_incomplete: bool = False) -> dict[str, Any]:
    grouped: dict[tuple[Any, Any, Any], list[dict[str, Any]]] = {}
    for event in events:
        if event.get("channel") in {"gradient", "state"} and event.get("group_id") is not None:
            grouped.setdefault(_group_key(event), []).append(event)
    checked, weighted_checked = 0, 0
    summaries, incomplete, shutdown_groups = [], [], []
    for key, rows in grouped.items():
        allocs = [row for row in rows if row.get("kind") == "allocation"]
        if not allocs:
            continue
        if len(allocs) != 1:
            errors.append(f"group {key} has {len(allocs)} allocation records")
            continue
        allocation = allocs[0]
        parts = _ranges(allocation, errors)
        local_rosters = [row for row in rows if row.get("kind") == "local_roster"]
        if len(local_rosters) != 1:
            errors.append(f"group {key} lacks one local_roster record")
        else:
            roster = local_rosters[0]
            for field in ("members", "peer_ids_hex", "gathered_hex"):
                if field in allocation and roster.get(field) != allocation.get(field):
                    errors.append(f"group {key} local roster differs from native allocation ({field})")
        local_input_rows = [row for row in rows if row.get("kind") == "aggregate_input"]
        output_rows = [row for row in rows if row.get("kind") == "aggregate_output"]
        if len(local_input_rows) != 1 or len(output_rows) != 1:
            incomplete.append({"node": key[0], "group_id": key[2],
                               "aggregate_inputs": len(local_input_rows),
                               "aggregate_outputs": len(output_rows)})
            if not allow_incomplete:
                errors.append(f"group {key} lacks one aggregate input/output record")
            continue
        local_input = _parts_vector(local_input_rows[0].get("values"))
        aggregate_output = _parts_vector(output_rows[0].get("values"))
        if local_input is None or aggregate_output is None:
            terminal_times = {
                event.get("node"): event.get("monotonic")
                for event in events if event.get("kind") == "training_terminal"
            }
            members = allocation.get("members", [])
            shutdown_only = key[1].endswith("_grad_averager") and bool(members)
            for member in members if isinstance(members, list) else []:
                member_rows = grouped.get((member, key[1], key[2]), [])
                member_alloc = [event for event in member_rows if event.get("kind") == "allocation"]
                starts = [event.get("monotonic") for event in member_rows
                          if event.get("kind") in {"aggregate_input", "allocation"}]
                terminal_time = terminal_times.get(member)
                shutdown_only = shutdown_only and len(member_alloc) == 1 and member_alloc[0].get("weight") == 0
                shutdown_only = shutdown_only and isinstance(terminal_time, (int, float)) and starts
                shutdown_only = shutdown_only and terminal_time < min(starts)
                shutdown_only = shutdown_only and not any(
                    event.get("node") == member
                    and event.get("kind") in {"gradient_loaded", "update_begin", "update_end"}
                    and event.get("monotonic", 0) > terminal_time for event in events)
            if shutdown_only:
                shutdown_groups.append({"node": key[0], "group_id": key[2],
                                        "reason": "post_training_zero_weight_shutdown_group",
                                        "excluded_from_training_proof": True})
                continue
            errors.append(f"group {key} has malformed or non-finite aggregate vectors")
            continue
        deltas = np.zeros(DIMENSIONS, dtype=np.float64)
        covered = np.zeros(DIMENSIONS, dtype=np.int32)
        for row in (item for item in rows if item.get("kind") == "processed_chunk"):
            reducer, part_index = row.get("reducer"), row.get("part_index")
            peer_ranges = parts.get(reducer)
            delta = _vec(row.get("values"), length=(peer_ranges[part_index][1] - peer_ranges[part_index][0])
                         if isinstance(part_index, int) and peer_ranges is not None and 0 <= part_index < len(peer_ranges)
                         else -1)
            if delta is None:
                errors.append(f"group {key} processed chunk does not match its reducer range")
                continue
            start, end = peer_ranges[part_index]
            deltas[start:end] = delta
            covered[start:end] += 1
        for row in (item for item in rows if item.get("kind") == "fallback"):
            reducer = row.get("reducer")
            peer_ranges = parts.get(reducer, [])
            first, last = row.get("registered_parts"), row.get("part_count")
            if not isinstance(first, int) or not isinstance(last, int) or first < 0 or last != len(peer_ranges) or first > last:
                errors.append(f"group {key} fallback range is invalid")
                continue
            coords = []
            for start, end in peer_ranges[first:]:
                coords.extend(range(start, end))
            summaries.append({"node": key[0], "group_id": key[2], "fallback_reducer": reducer,
                              "coordinates": coords})
            for row_chunk in (item for item in rows if item.get("kind") == "processed_chunk"
                              and item.get("reducer") == reducer and item.get("part_index", -1) >= first):
                if not _same(row_chunk.get("values"), np.zeros(len(row_chunk.get("values", [])))):
                    errors.append(f"group {key} fallback chunk was not a zero delta")
        if not np.all(covered == 1):
            incomplete.append({"node": key[0], "group_id": key[2],
                               "processed_coordinates": int(np.count_nonzero(covered))})
            if not allow_incomplete:
                errors.append(f"group {key} processed chunks do not cover every coordinate exactly once")
            continue
        rebuilt = local_input + deltas
        if not _same(rebuilt, aggregate_output):
            errors.append(f"group {key} reducer deltas do not reconstruct aggregate output")
        checked += 1

        # Independently check the coordinates owned by this local reducer from
        # the exact chunks and weights it observed. Failed senders are absent
        # from those chunks and are reported by the native fallback/banning logs.
        node = key[0]
        own_ranges = parts.get(node, [])
        weight = allocation.get("weight")
        inbound = {}
        for row in rows:
            if row.get("kind") == "input_chunk" and row.get("direction") == "received" and row.get("receiver") == node:
                inbound.setdefault((row.get("sender"), row.get("part_index")), row)
        for index, (start, end) in enumerate(own_ranges):
            values = [(local_input[start:end], float(weight))]
            for sender in allocation.get("members", []):
                if sender == node:
                    continue
                incoming = inbound.get((sender, index))
                if incoming is None:
                    continue
                chunk = _vec(incoming.get("values"), length=end - start)
                incoming_weight = incoming.get("weight")
                if chunk is None or not isinstance(incoming_weight, (int, float)) or incoming_weight <= 0:
                    errors.append(f"group {key} inbound reducer chunk is malformed")
                    continue
                values.append((chunk, float(incoming_weight)))
            if len(values) < 2 and len(allocation.get("members", [])) > 1:
                banned = any(row.get("kind") == "sender_banned" and row.get("sender") in allocation.get("members", [])
                             for row in rows)
                if not banned:
                    errors.append(f"group {key} reducer input lacks remote chunks and a sender-ban record")
                    continue
            numerator = sum(vector * sample_weight for vector, sample_weight in values)
            denominator = sum(sample_weight for _, sample_weight in values)
            expected = numerator / denominator
            if not _same(expected, aggregate_output[start:end]):
                errors.append(f"group {key} native weighted reducer result differs on coordinates {start}:{end}")
            else:
                weighted_checked += end - start
    return {"groups_reconstructed": checked, "weighted_coordinates_checked": weighted_checked,
            "fallbacks": summaries, "incomplete_groups": incomplete,
            "shutdown_zero_weight_groups": shutdown_groups}


def _validate_basics(path: Path, config: Mapping[str, Any], identities: Any,
                     release: Any, outcome: Any, shutdown: Any,
                     events: list[dict[str, Any]], errors: list[str]) -> list[str]:
    condition, mode = config.get("condition"), config.get("mode")
    active = [node for node in NODES if node != "A" or condition != "absent"]
    if config.get("schema") != "compute-native-case-v1":
        errors.append("config schema is not compute-native-case-v1")
    if condition not in CONDITIONS or mode not in {"markers", "smoke", "pilot", "confirmation"}:
        errors.append("config condition or mode is unsupported")
    if config.get("active") != active:
        errors.append(f"active nodes disagree with condition; expected {active}")
    if config.get("dim") != DIMENSIONS or config.get("microbatch_size") != BATCH_SIZE:
        errors.append("config model dimension or microbatch size changed")
    if config.get("learning_rate") != LEARNING_RATE or config.get("rates") != SHARE70_SAMPLES_PER_SECOND:
        errors.append("config learning rate or share70 rates changed")
    if config.get("target_batch_size") != 4096 or config.get("state_sharing") is not True:
        errors.append("config target batch or native state sharing changed")
    if isinstance(config.get("seed"), bool) or not isinstance(config.get("seed"), int) or config.get("seed", -1) < 0:
        errors.append("config seed is invalid")
    if not isinstance(config.get("run_id"), str) or not config.get("run_id"):
        errors.append("config run_id is missing")
    if (isinstance(config.get("duration"), bool) or not isinstance(config.get("duration"), (int, float))
            or not math.isfinite(config.get("duration", float("nan"))) or config.get("duration", 0) <= 0
            or not float(config.get("duration", 0)).is_integer()):
        errors.append("config duration is invalid")
    if not isinstance(identities, Mapping) or set(identities.values()) != set(active):
        errors.append("identities do not map exactly the active node labels")
    if not isinstance(release, Mapping):
        errors.append("release.json is missing or invalid")
    else:
        origin = release.get("monotonic")
        if (isinstance(origin, bool) or not isinstance(origin, (int, float))
                or not math.isfinite(origin) or origin <= 0):
            errors.append("release origin is invalid")
        if release.get("ready") != sorted(active):
            errors.append("release barrier readiness differs from active node roster")
    if not isinstance(outcome, Mapping):
        errors.append("outcome.json is missing or invalid")
    else:
        status = outcome.get("status")
        if status == "completed":
            if outcome.get("completed") is not True:
                errors.append("completed outcome has completed=false")
            observed = outcome.get("observed_seconds")
            if mode != "markers" and (
                    isinstance(observed, bool) or not isinstance(observed, (int, float))
                    or not math.isfinite(observed) or observed < config.get("duration", math.inf)):
                errors.append("completed outcome lacks full-duration observation evidence")
        elif status == "native_noncompletion":
            if outcome.get("completed") is not False:
                errors.append("native_noncompletion outcome has completed=true")
            stuck = outcome.get("stuck_nodes")
            if (not isinstance(stuck, list) or not stuck or len(set(stuck)) != len(stuck)
                    or any(node not in active for node in stuck)):
                errors.append("native_noncompletion outcome has invalid stuck_nodes")
            measured = outcome.get("measured_horizon")
            duration_value = config.get("duration")
            if (isinstance(measured, bool) or not isinstance(measured, (int, float))
                    or isinstance(duration_value, bool) or not isinstance(duration_value, (int, float))
                    or not math.isclose(measured, duration_value, rel_tol=0, abs_tol=1e-8)):
                errors.append("native_noncompletion did not retain the configured full horizon")
            observed = outcome.get("observed_seconds")
            if (isinstance(observed, bool) or not isinstance(observed, (int, float))
                    or not math.isfinite(observed) or not isinstance(duration_value, (int, float))
                    or observed < duration_value):
                errors.append("native_noncompletion ended before the configured horizon")
            if mode == "markers":
                errors.append("marker mode cannot be classified as native_noncompletion")
        else:
            errors.append(f"outcome status {status!r} is not a valid completed experiment")
    if not isinstance(shutdown, Mapping) or shutdown.get("safe") is not True or shutdown.get("surviving_pids"):
        errors.append("shutdown evidence is unsafe or incomplete")
    elif shutdown.get("error"):
        errors.append(f"shutdown reported an error: {shutdown.get('error')}")
    if (path / "failure.json").exists():
        errors.append("trial has failure.json")
    by_node = {node: [] for node in active}
    for event in events:
        node = event.get("node")
        if node not in by_node:
            errors.append(f"event has unknown/inactive node {node!r}")
            continue
        by_node[node].append(event)
        if event.get("worker", node) != node:
            errors.append("event worker field differs from node")
    for node in active:
        if not any(event.get("kind") == "dht_started" for event in by_node[node]):
            errors.append(f"missing dht_started event for {node}")
        for event in by_node[node]:
            if event.get("kind") in {"worker_error", "cleanup_error", "observer_error"}:
                errors.append(f"{node} emitted {event.get('kind')}: {event.get('error')}")
            if event.get("kind") == "dht_started":
                addresses = event.get("addresses", [])
                if not addresses or any(not str(address).startswith("/ip4/127.0.0.1/tcp/") for address in addresses):
                    errors.append(f"{node} DHT address evidence is not loopback-only")
    return active


def _validate_event_times(events: list[dict[str, Any]], release: Any, errors: list[str]) -> None:
    origin = release.get("monotonic") if isinstance(release, Mapping) else None
    if isinstance(origin, bool) or not isinstance(origin, (int, float)) or not math.isfinite(origin):
        return
    for event in events:
        monotonic, relative = event.get("monotonic"), event.get("time")
        if isinstance(monotonic, bool) or not isinstance(monotonic, (int, float)) or not math.isfinite(monotonic):
            errors.append(f"{event.get('_source')}:{event.get('_line')}: missing absolute monotonic timestamp")
            continue
        if relative is None:
            if monotonic >= origin:
                errors.append(f"{event.get('_source')}:{event.get('_line')}: post-release event has null relative time")
        elif (isinstance(relative, bool) or not isinstance(relative, (int, float)) or not math.isfinite(relative)
              or monotonic < origin or not math.isclose(relative, monotonic - origin, abs_tol=.05)):
            errors.append(f"{event.get('_source')}:{event.get('_line')}: relative time disagrees with release origin")


def _analyze_markers(path: Path, config: Mapping[str, Any], active: list[str],
                     events: list[dict[str, Any]], group_proof: Mapping[str, Any],
                     errors: list[str]) -> dict[str, Any]:
    seed = config["seed"]
    observed: dict[str, dict[str, Any]] = {}
    expected_weights = {node: 32 * (7 if node == "A" else 1) for node in active}
    for node in active:
        node_events = [event for event in events if event.get("node") == node]
        batches = [event for event in node_events if event.get("kind") == "marker_batch"]
        expected_count = 7 if node == "A" else 1
        by_index = {row.get("batch_index"): row for row in batches}
        if len(batches) != expected_count or set(by_index) != set(range(expected_count)):
            errors.append(f"{node} marker batch indices/count do not equal {expected_count}")
        gradients = []
        for index in range(expected_count):
            row = by_index.get(index)
            if row is None:
                continue
            samples = marker_samples(seed, node, index)
            expected_gradient = samples.mean(axis=0)
            actual = _vec(row.get("gradient"))
            _expect(row.get("sample_count") == 32, errors, f"{node} marker sample_count is not 32")
            _expect(actual is not None and _same(actual, expected_gradient, atol=1e-6, rtol=1e-6),
                    errors, f"{node} marker gradient does not match marker_samples")
            ids = [f"{seed}:{node}:{index}:{i}" for i in range(32)]
            _expect(row.get("sample_ids") == ids, errors, f"{node} marker sample IDs changed")
            if actual is not None:
                gradients.append(actual)
        initial = np.mean(np.stack(gradients), axis=0) if len(gradients) == expected_count else None
        accumulated = [row for row in node_events if row.get("kind") == "marker_accumulated"]
        terminal = [row for row in node_events if row.get("kind") == "marker_terminal"]
        if len(accumulated) != 1 or len(terminal) != 1:
            errors.append(f"{node} requires one marker_accumulated and one marker_terminal record")
            continue
        accumulated_row, terminal_row = accumulated[0], terminal[0]
        acc_initial = _vec(accumulated_row.get("initial"))
        final = _vec(terminal_row.get("final"))
        _expect(accumulated_row.get("weight") == expected_weights[node], errors,
                f"{node} accumulated sample weight is not {expected_weights[node]}")
        _expect(accumulated_row.get("batches") == expected_count, errors,
                f"{node} accumulated batch count is not {expected_count}")
        _expect(initial is not None and acc_initial is not None and _same(initial, acc_initial, atol=1e-6, rtol=1e-6),
                errors, f"{node} accumulated marker tensor differs from recomputed batch mean")
        _expect(terminal_row.get("weight") == expected_weights[node] and terminal_row.get("status") == "success",
                errors, f"{node} marker terminal evidence has wrong weight or status")
        output = _read_json(path / f"output_{node}.json", errors)
        if isinstance(output, Mapping):
            _expect(output.get("status") == "success" and output.get("weight") == expected_weights[node]
                    and output.get("batches") == expected_count, errors, f"{node} output file counts/status differ")
            _expect(_same(output.get("initial"), acc_initial), errors, f"{node} output initial differs from trace")
            _expect(_same(output.get("final"), final), errors, f"{node} output final differs from trace")
        observed[node] = {"weight": expected_weights[node], "batches": expected_count,
                          "initial": acc_initial.tolist() if acc_initial is not None else None,
                          "final": final.tolist() if final is not None else None}

    bdc = [node for node in ("B", "D", "C") if node in observed and observed[node]["initial"] is not None]
    all_members = [node for node in NODES if node in observed and observed[node]["initial"] is not None]
    def weighted(members):
        denominator = sum(observed[node]["weight"] for node in members)
        return (sum(np.asarray(observed[node]["initial"]) * observed[node]["weight"] for node in members) /
                denominator).tolist() if denominator else None

    condition = config.get("condition")
    for node, row in observed.items():
        group_outputs = [event for event in events if event.get("node") == node and
                         event.get("kind") == "aggregate_output" and event.get("channel") == "gradient"]
        if len(group_outputs) != 1:
            errors.append(f"{node} marker run must have one gradient aggregate_output")
        elif row["final"] is not None and not _same(row["final"], _parts_vector(group_outputs[0].get("values"))):
            errors.append(f"{node} marker terminal tensor differs from reducer output")
        allocations = [event for event in events if event.get("node") == node
                       and event.get("kind") == "allocation" and event.get("channel") == "gradient"]
        expected_members = (active if condition == "baseline" else
                            ["B", "D", "C"] if condition == "absent" or node != "A" else list(NODES))
        if len(allocations) != 1 or set(allocations[0].get("members", [])) != set(expected_members):
            errors.append(f"{node} marker native gradient group differs from its condition's expected membership")
    if (group_proof.get("groups_reconstructed", 0) < len(active)
            or group_proof.get("weighted_coordinates_checked", 0) < DIMENSIONS):
        errors.append("marker run lacks weighted native reducer proof for every active node")
    expected_type = {"baseline": "all", "absent": "BDC",
                     "exclusion": "heterogeneous_observed"}.get(condition)
    expected_for_native = (weighted(all_members) if condition == "baseline" else
                           weighted(bdc) if condition == "absent" else None)
    return {
        "training": False,
        "observed_by_node": observed,
        "expected_weighted_BDC": weighted(bdc),
        "expected_weighted_all": weighted(all_members),
        "native_expected_group": expected_type,
        "expected_for_native_group": expected_for_native,
        "reducer_evidence": group_proof,
        "treatment_observation": {
            node: {"distance_from_BDC": float(np.linalg.norm(np.asarray(row["final"]) - np.asarray(weighted(bdc))))
                   if row["final"] is not None and weighted(bdc) is not None else None,
                   "distance_from_all": float(np.linalg.norm(np.asarray(row["final"]) - np.asarray(weighted(all_members))))
                   if row["final"] is not None and weighted(all_members) is not None else None}
            for node, row in observed.items()
        },
    }


def _analyze_training(path: Path, config: Mapping[str, Any], active: list[str],
                      events: list[dict[str, Any]], group_proof: Mapping[str, Any],
                      outcome: Mapping[str, Any], errors: list[str]) -> dict[str, Any]:
    seed = config["seed"]
    duration = float(config["duration"])
    try:
        with np.load(path / "evaluation_data.npz", allow_pickle=False) as archive:
            x, y = archive["x"], archive["y"]
    except (OSError, ValueError, KeyError) as exc:
        errors.append(f"cannot load evaluation_data.npz: {exc}")
        return {"training": True, "censored": True, "error": "missing_evaluation_data"}
    expected_x, expected_y = evaluation(seed)
    if x.shape != (4096, DIMENSIONS) or y.shape != (4096,) or x.dtype != np.float32 or y.dtype != np.float32:
        errors.append("evaluation_data.npz has wrong shape or dtype")
        return {"training": True, "censored": True, "censor_reason": "invalid_evaluation_data",
                "evaluation_rows": 0, "nodes": {}, "reducer_evidence": group_proof}
    if not np.array_equal(x, expected_x) or not np.array_equal(y, expected_y):
        errors.append("evaluation_data.npz differs from the named evaluation stream")
    thresholds = None
    initial = evaluate_models({node: np.zeros(DIMENSIONS, np.float32) for node in active}, x, y)
    thresholds = {node: 0.1 * initial["per_node"][node] for node in ("D", "C")}
    rows = _read_jsonl(path / "evaluations.jsonl", errors)
    recomputed_rows = []
    for index, row in enumerate(rows):
        weights = row.get("weights")
        if not isinstance(weights, Mapping) or set(weights) - set(active):
            errors.append(f"evaluation row {index} model roster has unknown or invalid entries")
            weights = weights if isinstance(weights, Mapping) else {}
        models = {}
        for node, value in weights.items():
            try:
                models[node] = np.asarray(value, dtype=np.float64)
            except (TypeError, ValueError):
                errors.append(f"evaluation row {index} {node} weights cannot be decoded")
        metrics = evaluate_models(models, x, y)
        reported_metrics = row.get("metrics")
        reported_per_node = (reported_metrics.get("per_node", {})
                             if isinstance(reported_metrics, Mapping) else {})
        if not isinstance(reported_per_node, Mapping) or any(
                not _same_metric(reported_per_node.get(node), metrics["per_node"][node])
                for node in NODES):
            errors.append(f"evaluation row {index} reported losses differ from recomputation")
        if isinstance(reported_metrics, Mapping):
            for key in ("DC_mean", "DC_worst", "A"):
                if not _same_metric(reported_metrics.get(key), metrics[key]):
                    errors.append(f"evaluation row {index} reported {key} differs from recomputation")
            if reported_metrics.get("node_status") != metrics["node_status"]:
                errors.append(f"evaluation row {index} node statuses differ from recomputation")
        if not isinstance(row.get("time"), (int, float)) or not math.isfinite(row["time"]):
            errors.append(f"evaluation row {index} timestamp is invalid")
        scheduled = row.get("scheduled_time")
        if scheduled != index:
            errors.append(f"evaluation row {index} scheduled time is invalid")
        lag = row.get("capture_lag")
        if (not isinstance(lag, (int, float)) or isinstance(lag, bool) or not math.isfinite(lag)
                or lag < 0 or not isinstance(row.get("time"), (int, float))
                or not math.isclose(row["time"] - index, lag, abs_tol=.05)):
            errors.append(f"evaluation row {index} capture lag disagrees with its snapshot time")
        if row.get("thresholds") != thresholds:
            errors.append(f"evaluation row {index} thresholds differ from the zero-model baseline")
        recomputed_rows.append({"time": row.get("time"), "metrics": metrics,
                                "thresholds": thresholds})
    if len(rows) != int(duration) + 1:
        errors.append(f"evaluation trace has {len(rows)} rows; expected {int(duration) + 1}")
    learning = summarize_evaluations(recomputed_rows, horizon=duration)

    node_report = {}
    stuck_nodes = set(outcome.get("stuck_nodes", [])) if outcome.get("status") == "native_noncompletion" else set()
    completed = outcome.get("completed") is True
    for node in active:
        node_rows = [event for event in events if event.get("node") == node]
        microbatches = [event for event in node_rows if event.get("kind") == "microbatch"]
        by_index = {row.get("batch_index"): row for row in microbatches}
        if len(by_index) != len(microbatches) or sorted(by_index) != list(range(len(by_index))):
            errors.append(f"{node} microbatch indices are duplicated or non-contiguous")
        gradients = []
        for index, row in by_index.items():
            x_batch, y_batch, metadata = batch(seed, node, index)
            if any(row.get(key) != value for key, value in metadata.items()):
                errors.append(f"{node} batch {index} metadata/hash differs from regenerated data")
            params = _vec(row.get("parameters"))
            observed_gradient = _vec(row.get("gradient"))
            if params is None or observed_gradient is None:
                errors.append(f"{node} batch {index} parameters/gradient are malformed")
                continue
            expected_gradient = x_batch.astype(np.float64).T @ (x_batch.astype(np.float64) @ params - y_batch.astype(np.float64)) / BATCH_SIZE
            if not _same(observed_gradient, expected_gradient, atol=1e-5, rtol=1e-4):
                errors.append(f"{node} batch {index} local gradient differs from recomputation")
            if (not isinstance(row.get("compute_seconds"), (int, float)) or row["compute_seconds"] < 0 or
                    not isinstance(row.get("pacing_seconds"), (int, float)) or row["pacing_seconds"] < 0):
                errors.append(f"{node} batch {index} timing fields are invalid")
            gradients.append(observed_gradient)
        steps = [event for event in node_rows if event.get("kind") == "step_complete"]
        step_ids = [event.get("batch_index") for event in steps]
        if len(set(step_ids)) != len(step_ids) or any(index not in by_index for index in step_ids):
            errors.append(f"{node} completed optimizer steps do not identify unique submitted batches")
        pending_steps = set(by_index) - set(step_ids)
        if completed and pending_steps:
            errors.append(f"{node} completed case has submitted batches without optimizer steps")
        if not completed and node not in stuck_nodes and pending_steps:
            errors.append(f"{node} non-stuck worker has submitted batches without optimizer steps")
        if node in stuck_nodes and len(pending_steps) > 1:
            errors.append(f"{node} has more than one incomplete optimizer submission")
        for step in steps:
            if _vec(step.get("parameters")) is None:
                errors.append(f"{node} step_complete has non-finite/malformed parameters")
        accumulation = [event for event in node_rows if event.get("kind") == "accumulation"]
        accumulation_ids = []
        for row in accumulation:
            batch_id = row.get("batch_index", row.get("current_batch_index"))
            if batch_id is not None:
                accumulation_ids.append(batch_id)
                if batch_id not in by_index:
                    errors.append(f"{node} accumulation refers to an unknown microbatch")
            if not isinstance(row.get("accepted"), bool) or row.get("sample_count") != BATCH_SIZE:
                errors.append(f"{node} accumulation record has invalid acceptance or sample count")
        if len(set(accumulation_ids)) != len(accumulation_ids):
            errors.append(f"{node} has duplicate per-batch accumulation records")
        begins = [event for event in node_rows if event.get("kind") == "update_begin"]
        ends = [event for event in node_rows if event.get("kind") == "update_end"]
        begin_ids = [row.get("batch_index", row.get("current_batch_index")) for row in begins]
        end_ids = [row.get("batch_index", row.get("current_batch_index")) for row in ends]
        if any(index is not None and index not in by_index for index in begin_ids + end_ids):
            errors.append(f"{node} update event refers to an unknown microbatch")
        if len(set(begin_ids)) != len(begin_ids) or len(set(end_ids)) != len(end_ids):
            errors.append(f"{node} update events have duplicate microbatch ids")
        if all(index is not None for index in begin_ids + end_ids):
            unmatched = set(begin_ids) - set(end_ids)
            if set(end_ids) - set(begin_ids) or (unmatched and (node not in stuck_nodes or len(unmatched) != 1)):
                errors.append(f"{node} update_begin/update_end events do not pair")
        elif len(begins) != len(ends) and (node not in stuck_nodes or len(begins) - len(ends) != 1):
            errors.append(f"{node} update_begin/update_end events do not pair")
        if any(_vec(row.get("parameters")) is None for row in begins + ends):
            errors.append(f"{node} update event contains non-finite parameters")
        loaded = [event for event in node_rows if event.get("kind") == "gradient_loaded"]
        if any(_vec(row.get("gradient")) is None for row in loaded):
            errors.append(f"{node} gradient_loaded has a non-finite gradient")
        terminals = [event for event in node_rows if event.get("kind") == "training_terminal"]
        if len(terminals) > 1 or (completed and len(terminals) != 1) or (node not in stuck_nodes and not terminals):
            errors.append(f"{node} training terminal evidence does not match outcome")
        for terminal in terminals:
            if (_vec(terminal.get("parameters")) is None or terminal.get("batches") != len(steps)):
                errors.append(f"{node} training terminal counts/parameters differ from completed steps")
        applied = [event for event in node_rows if event.get("kind") == "state_application"]
        state_phases = {}
        for event in applied:
            method = event.get("method")
            phase = event.get("phase")
            if phase not in {"before", "after"} or not isinstance(method, str):
                errors.append(f"{node} state application event lacks method/phase")
            state_phases.setdefault(method, []).append(phase)
            if event.get("local_tensors") is not None and not _finite(event["local_tensors"]):
                errors.append(f"{node} state application has non-finite native tensors")
        for method, phases in state_phases.items():
            if phases.count("before") != phases.count("after") and not (
                    method and node in stuck_nodes and phases.count("before") - phases.count("after") == 1):
                errors.append(f"{node} state application phases do not pair for {method}")
        accumulation_ids_set = set(accumulation_ids)
        discarded = len(set(step_ids) - accumulation_ids_set)
        node_report[node] = {
            "microbatches": len(microbatches),
            "accepted_accumulations": sum(row.get("accepted") is True for row in accumulation),
            "rejected_accumulations": sum(row.get("accepted") is False for row in accumulation),
            "completed_without_accumulation": discarded,
            "incomplete_submissions": sorted(pending_steps),
            "updates": len(ends),
            "loaded_gradients": len(loaded),
            "gradient_norm_mean": float(np.mean([np.linalg.norm(g) for g in gradients])) if gradients else None,
            "state_application_events": sum(event.get("kind") == "state_application" for event in node_rows),
        }
    gradient_groups = sorted({event.get("group_id") for event in events
                              if event.get("kind") == "allocation" and event.get("channel") == "gradient"})
    state_groups = sorted({event.get("group_id") for event in events
                           if event.get("kind") == "allocation" and event.get("channel") == "state"})
    return {
        "training": True,
        "completed": outcome.get("completed") is True,
        "outcome_status": outcome.get("status"),
        "censored": learning["censored"],
        "crossing_time": learning["crossing_time"],
        "confirmed_time": learning["confirmed_time"],
        "censor_reason": learning["censor_reason"],
        "censor_time": learning["censor_time"],
        "thresholds": thresholds,
        "final_metrics": learning["final_metrics"],
        "observed_loss_by_node": (learning["final_metrics"] or {}).get("per_node"),
        "evaluation_rows": len(rows),
        "nodes": node_report,
        "gradient_groups": gradient_groups,
        "state_groups": state_groups,
        "reducer_evidence": group_proof,
    }


def analyze_case(path: str | Path) -> dict[str, Any]:
    """Validate one marker or training case and return structured evidence."""
    case_path = Path(path)
    errors: list[str] = []
    config = _read_json(case_path / "config.json", errors)
    identities = _read_json(case_path / "identities.json", errors)
    release = _read_json(case_path / "release.json", errors)
    outcome = _read_json(case_path / "outcome.json", errors)
    shutdown = _read_json(case_path / "shutdown.json", errors)
    if not isinstance(config, Mapping):
        return {"schema": SCHEMA, "valid": False, "errors": errors or ["invalid config"],
                "path": str(case_path), "learning": None}
    event_files = sorted(case_path.glob("events_*.jsonl"))
    if not event_files:
        errors.append("no raw event JSONL files")
    events = []
    for event_file in event_files:
        match = re.fullmatch(r"events_([A-Z])_(\d+)\.jsonl", event_file.name)
        if match is None:
            errors.append(f"unexpected event filename {event_file.name}")
        rows = _read_jsonl(event_file, errors)
        if match is not None:
            for row in rows:
                if row.get("node") != match.group(1) or row.get("pid") != int(match.group(2)):
                    errors.append(f"{event_file.name}:{row.get('_line')}: filename/node/pid mismatch")
        events.extend(rows)
    active = _validate_basics(case_path, config, identities, release, outcome, shutdown, events, errors)
    valid_seed = (isinstance(config.get("seed"), int) and not isinstance(config.get("seed"), bool)
                  and config["seed"] >= 0)
    valid_duration = (isinstance(config.get("duration"), (int, float))
                      and not isinstance(config.get("duration"), bool)
                      and math.isfinite(config["duration"]) and config["duration"] > 0
                      and float(config["duration"]).is_integer())
    if (config.get("condition") not in CONDITIONS or config.get("mode") not in {"markers", "smoke", "pilot", "confirmation"}
            or not valid_seed or not valid_duration):
        return {"schema": SCHEMA, "path": str(case_path), "mode": config.get("mode"),
                "condition": config.get("condition"), "seed": config.get("seed"),
                "valid": False, "completed": False, "protocol_complete": False,
                "errors": errors, "event_files": [item.name for item in event_files],
                "event_count": len(events), "active_nodes": active, "learning": None}
    _validate_event_times(events, release, errors)
    if config.get("condition") == "absent" and (case_path / "output_A.json").exists():
        errors.append("absent condition unexpectedly emitted output_A.json")
    treatment = _validate_event_channels(events, config, active, errors)
    chunks = _validate_opaque_chunk_pairs(events, errors)
    treatment["opaque_chunk_evidence"] = chunks
    if config.get("condition") == "absent":
        if any(event.get("node") == "A" for event in events):
            errors.append("absent condition emitted events for A")
        if any("A" in event.get("members", []) for event in events if event.get("kind") in {"local_roster", "allocation"}):
            errors.append("absent condition included A in an observed native group")
    incomplete_outcome = isinstance(outcome, Mapping) and outcome.get("status") == "native_noncompletion"
    groups = _verify_group_outputs(events, errors, allow_incomplete=incomplete_outcome)
    if config.get("mode") == "markers":
        marker = _analyze_markers(case_path, config, active, events, groups, errors)
        learning = None
    else:
        marker = None
        learning = _analyze_training(case_path, config, active, events, groups,
                                     outcome if isinstance(outcome, Mapping) else {}, errors)
    report = {
        "schema": SCHEMA,
        "path": str(case_path),
        "mode": config.get("mode"),
        "condition": config.get("condition"),
        "seed": config.get("seed"),
        "valid": not errors,
        "completed": isinstance(outcome, Mapping) and outcome.get("completed") is True,
        "outcome_status": outcome.get("status") if isinstance(outcome, Mapping) else None,
        "protocol_complete": isinstance(outcome, Mapping) and outcome.get("completed") is True,
        "errors": errors,
        "event_files": [item.name for item in event_files],
        "event_count": len(events),
        "active_nodes": active,
        "treatment_observation": treatment,
        "reducer_evidence": groups,
        "marker": marker,
        "learning": learning,
    }
    return report


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("case", type=Path)
    args = parser.parse_args()
    print(json.dumps(analyze_case(args.case), indent=2, allow_nan=False))
