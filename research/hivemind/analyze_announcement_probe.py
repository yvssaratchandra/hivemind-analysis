#!/usr/bin/env python3
"""Analyze native Hivemind inconsistent-announcement probe artifacts.

The analyzer intentionally works from saved JSON artifacts and does not import
Hivemind. It checks the evidence needed to interpret each condition, then
classifies tensor outputs using the embedded origin and coordinate markers.
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import math
from pathlib import Path
import tarfile
from typing import Any, Iterable


NODES = ("B", "D", "A", "C")
NODE_INDEX = {node: index for index, node in enumerate(NODES)}
ROWS = 48
LANES = 9
ABS_TOL = 1e-4
REL_TOL = 0.0
CONDITIONS = ("baseline4", "control3", "ordering", "membership")


def _load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as stream:
        return json.load(stream)


def _read_events(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    events: list[dict[str, Any]] = []
    errors: list[str] = []
    files = sorted(path.glob("events_*.jsonl"))
    if not files and (path / "events.jsonl").is_file():
        files = [path / "events.jsonl"]
    if not files:
        return events, ["no events_*.jsonl or events.jsonl found"]
    for event_file in files:
        try:
            with event_file.open("r", encoding="utf-8") as stream:
                for line_number, line in enumerate(stream, 1):
                    if not line.strip():
                        continue
                    try:
                        event = json.loads(line)
                    except json.JSONDecodeError as exc:
                        errors.append(f"{event_file.name}:{line_number}: invalid JSON ({exc})")
                        continue
                    if not isinstance(event, dict):
                        errors.append(f"{event_file.name}:{line_number}: event is not an object")
                        continue
                    event.setdefault("_source_file", event_file.name)
                    event.setdefault("_line", line_number)
                    events.append(event)
        except OSError as exc:
            errors.append(f"cannot read {event_file.name}: {exc}")
    return events, errors


def _event_value(event: dict[str, Any], key: str, default: Any = None) -> Any:
    """Read common flat fields, accepting an optional nested `fields` object."""
    if key in event:
        return event[key]
    fields = event.get("fields")
    if isinstance(fields, dict):
        return fields.get(key, default)
    return default


def _event_kind(event: dict[str, Any]) -> str:
    return str(event.get("kind", event.get("event", "")))


def _event_node(event: dict[str, Any]) -> str | None:
    node = event.get("node", event.get("actor"))
    return str(node) if node is not None else None


def _event_time(event: dict[str, Any]) -> float | None:
    value = event.get("time", event.get("t"))
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _members(value: Any) -> list[str] | None:
    if not isinstance(value, (list, tuple)):
        return None
    return [str(item) for item in value]


def _canonical_id(value: Any) -> str:
    if isinstance(value, list) and all(isinstance(item, int) and 0 <= item <= 255 for item in value):
        return bytes(value).hex()
    return str(value).strip().lower().removeprefix("0x")


def _close(left: float, right: float, *, atol: float = ABS_TOL) -> bool:
    return math.isclose(left, right, rel_tol=REL_TOL, abs_tol=atol)


def _is_nonempty(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, dict):
        return any(_is_nonempty(key) or _is_nonempty(item) for key, item in value.items())
    if isinstance(value, (list, tuple, set)):
        return any(_is_nonempty(item) for item in value)
    if isinstance(value, (str, bytes)):
        return bool(value)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value != 0
    return bool(value)


def _numeric_values(value: Any) -> list[float] | None:
    if not isinstance(value, list):
        return None
    try:
        values = [float(item) for item in value]
    except (TypeError, ValueError):
        return None
    return values if all(math.isfinite(item) for item in values) else None


def _mode_name(value: Any) -> str | None:
    """Normalize native enum strings and AveragingMode.NODE's numeric value."""
    if isinstance(value, str):
        return value.upper().rsplit(".", 1)[-1]
    if isinstance(value, int) and not isinstance(value, bool):
        return {0: "NODE", 1: "CLIENT", 2: "AUX"}.get(value)
    return None


def _roster_metadata_errors(
    event: dict[str, Any], node: str, members: list[str], *, check_bandwidths: bool = True
) -> list[str]:
    errors = []
    modes = _event_value(event, "modes")
    if not isinstance(modes, list) or len(modes) != len(members):
        errors.append(f"{node}: native modes must be aligned with the local roster")
    elif [_mode_name(value) for value in modes] != ["NODE"] * len(members):
        errors.append(f"{node}: expected NODE mode for every roster member, got {modes!r}")
    if check_bandwidths:
        bandwidths = _event_value(event, "bandwidths")
        if not isinstance(bandwidths, list) or len(bandwidths) != len(members):
            errors.append(f"{node}: native bandwidths must be aligned with the local roster")
        elif any(value is not None for value in bandwidths):
            errors.append(f"{node}: expected unspecified native bandwidths, got {bandwidths!r}")
    return errors


def _positive_size(value: Any) -> bool:
    if isinstance(value, dict):
        return any(_positive_size(item) for item in value.values())
    if isinstance(value, (list, tuple, set)):
        return any(_positive_size(item) for item in value)
    if isinstance(value, bool) or value is None:
        return False
    try:
        return float(value) > 0
    except (TypeError, ValueError):
        return False


def _operation_status(record: dict[str, Any]) -> str:
    status = str(record.get("status", "missing")).lower()
    error = str(record.get("error", "")).lower()
    if status in {"deadline", "timeout"} or any(token in error for token in ("timeouterror", "deadline", "timed out")):
        return "deadline"
    if status in {"error", "exception"}:
        return "exception"
    return status


def _flatten_matrix(value: Any) -> list[list[float]] | None:
    if not isinstance(value, list) or len(value) != ROWS:
        return None
    matrix: list[list[float]] = []
    try:
        for row in value:
            if not isinstance(row, list) or len(row) != LANES:
                return None
            converted = [float(item) for item in row]
            if not all(math.isfinite(item) for item in converted):
                return None
            matrix.append(converted)
    except (TypeError, ValueError):
        return None
    return matrix


def expected_input_tensor(node: str) -> list[list[float]]:
    """Reconstruct one participant's deterministic main and marker values."""
    index = NODE_INDEX[node]
    tensor: list[list[float]] = []
    for row_index in range(ROWS):
        row = [0.0] * LANES
        row[0] = 100 * index + row_index + 1
        row[1 + index] = 1.0
        row[5 + index] = row_index + 1
        tensor.append(row)
    return tensor


def _mean_from_saved_inputs(
    initial_by_node: dict[str, list[list[float]]], members: Iterable[str] | None
) -> list[list[float]] | None:
    """Compute an equal-weight mean from the saved input tensors, not labels."""
    if members is None:
        return None
    member_list = list(members)
    if not member_list or len(member_list) != len(set(member_list)):
        return None
    matrices = [initial_by_node.get(member) for member in member_list]
    if any(matrix is None for matrix in matrices):
        return None
    assert all(matrix is not None for matrix in matrices)
    count = float(len(matrices))
    return [
        [sum(matrix[row][lane] for matrix in matrices) / count for lane in range(LANES)]
        for row in range(ROWS)
    ]


def _compare_matrix(actual: list[list[float]] | None, expected: list[list[float]]) -> dict[str, Any]:
    if actual is None:
        return {"matches": False, "max_abs_error": None, "shape_valid": False}
    errors = [abs(a - e) for arow, erow in zip(actual, expected) for a, e in zip(arow, erow)]
    return {
        "matches": all(error <= ABS_TOL for error in errors),
        "max_abs_error": max(errors, default=0.0),
        "shape_valid": True,
    }


def _infer_condition(path: Path, config: dict[str, Any]) -> str:
    name = config.get("condition") or config.get("name") or path.name
    normalized = str(name).lower().replace("-", "_")
    aliases = {
        "baseline": "baseline4",
        "baseline_4": "baseline4",
        "four_member_baseline": "baseline4",
        "control": "control3",
        "three_member_control": "control3",
        "inconsistent_ordering": "ordering",
        "inconsistent_membership": "membership",
    }
    normalized = aliases.get(normalized, normalized)
    if normalized not in CONDITIONS:
        raise ValueError(f"unknown condition {name!r}; expected one of {', '.join(CONDITIONS)}")
    return normalized


def _active_nodes(condition: str) -> list[str]:
    return ["B", "D", "C"] if condition == "control3" else list(NODES)


def _expected_local_roster(condition: str, node: str) -> list[str] | None:
    if condition == "baseline4":
        return list(NODES)
    if condition == "control3":
        return ["B", "D", "C"] if node in ("B", "D", "C") else None
    if condition == "ordering":
        return ["A", "D", "B", "C"] if node == "A" else list(NODES)
    if condition == "membership":
        return list(NODES) if node == "A" else ["B", "D", "C"]
    return None


def _shutdown_check(path: Path) -> tuple[bool, str | None]:
    shutdown_path = path / "shutdown.json"
    if not shutdown_path.is_file():
        return False, "shutdown.json is missing"
    try:
        data = _load_json(shutdown_path)
    except (OSError, json.JSONDecodeError) as exc:
        return False, f"shutdown.json is unreadable: {exc}"
    if not isinstance(data, dict):
        return False, "shutdown.json must contain an object"
    explicit = data.get("safe")
    if explicit is None:
        explicit = data.get("clean")
    survivors = data.get("surviving_pids", data.get("survivors"))
    if explicit is False:
        return False, "shutdown.json reports unsafe shutdown"
    if explicit is True and isinstance(survivors, list) and not survivors:
        return True, None
    if explicit is True and survivors is None and data.get("survivors_checked") is True:
        return True, None
    if isinstance(survivors, list) and survivors:
        return False, f"shutdown.json lists surviving processes: {survivors}"
    return False, "shutdown.json lacks explicit safe=true and an empty survivor list/check"


def _identity_check(path: Path) -> tuple[dict[str, Any], list[str]]:
    identity_path = path / "identities.json"
    errors: list[str] = []
    if not identity_path.is_file():
        return {"valid": False, "peer_ids": {}}, ["identities.json is missing"]
    try:
        identities = _load_json(identity_path)
    except (OSError, json.JSONDecodeError) as exc:
        return {"valid": False, "peer_ids": {}}, [f"identities.json is unreadable: {exc}"]
    if not isinstance(identities, dict):
        return {"valid": False, "peer_ids": {}}, ["identities.json must contain a node-to-peer-ID object"]
    if set(identities) != set(NODES):
        errors.append(f"identities.json must contain exactly {list(NODES)}")
    peer_ids = {str(node): value for node, value in identities.items()}
    if any(not isinstance(value, str) or not value.strip() for value in peer_ids.values()):
        errors.append("identities.json contains an empty or non-string peer ID")
    values = [value for value in peer_ids.values() if isinstance(value, str)]
    if len(values) != len(set(values)):
        errors.append("identities.json assigns one peer ID to multiple node labels")
    return {"valid": not errors, "peer_ids": peer_ids}, errors


def _source_provenance_check(path: Path) -> tuple[dict[str, Any], list[str]]:
    provenance_path = path / "provenance.json"
    archive_path = path / "sources.tar.gz"
    errors: list[str] = []
    if not provenance_path.is_file():
        return {"valid": False, "native_files_verified": 0}, ["provenance.json is missing"]
    try:
        provenance = _load_json(provenance_path)
    except (OSError, json.JSONDecodeError) as exc:
        return {"valid": False, "native_files_verified": 0}, [f"provenance.json is unreadable: {exc}"]
    if not isinstance(provenance, dict):
        return {"valid": False, "native_files_verified": 0}, ["provenance.json must contain an object"]
    hashes = provenance.get("hashes")
    if not isinstance(provenance.get("package"), str) or not provenance.get("package"):
        errors.append("provenance.json has no native package path")
    for field in ("python", "torch", "hivemind", "p2pd_sha256"):
        if not provenance.get(field):
            errors.append(f"provenance.json lacks {field}")
    if not isinstance(hashes, dict) or not hashes:
        errors.append("provenance.json has no native source hashes")
        hashes = {}
    for relative, digest in hashes.items():
        if not isinstance(relative, str) or not isinstance(digest, str) or len(digest) != 64:
            errors.append(f"invalid source hash entry: {relative!r}")
    if not archive_path.is_file():
        errors.append("sources.tar.gz is missing")
        return {"valid": False, "native_files_verified": 0, "native_hashes": hashes}, errors
    verified = 0
    try:
        with tarfile.open(archive_path, "r:gz") as archive:
            members = set(archive.getnames())
            if "announcement_probe.py" not in members:
                errors.append("sources.tar.gz does not contain the experiment harness")
            for relative, digest in hashes.items():
                member_name = f"hivemind/{relative}"
                try:
                    source_file = archive.extractfile(member_name)
                except (KeyError, OSError, tarfile.TarError):
                    source_file = None
                if source_file is None:
                    errors.append(f"sources.tar.gz is missing {member_name}")
                    continue
                actual = hashlib.sha256(source_file.read()).hexdigest()
                if actual != digest:
                    errors.append(f"archived native source hash mismatch for {relative}")
                else:
                    verified += 1
    except (OSError, tarfile.TarError) as exc:
        errors.append(f"sources.tar.gz is unreadable: {exc}")
    harness_manifest_path = path.parent / "harness_hashes.json"
    harness_digest = None
    if harness_manifest_path.is_file():
        try:
            harness_manifest = _load_json(harness_manifest_path)
            harness_digest = harness_manifest.get("announcement_probe.py") if isinstance(harness_manifest, dict) else None
            if not isinstance(harness_digest, str) or len(harness_digest) != 64:
                errors.append("harness_hashes.json has no announcement_probe.py hash")
            else:
                with tarfile.open(archive_path, "r:gz") as archive:
                    harness_file = archive.extractfile("announcement_probe.py")
                    archived_harness_digest = hashlib.sha256(harness_file.read()).hexdigest() if harness_file else None
                if archived_harness_digest != harness_digest:
                    errors.append("archived experiment harness does not match harness_hashes.json")
        except (OSError, json.JSONDecodeError, tarfile.TarError, KeyError) as exc:
            errors.append(f"cannot verify archived harness hash: {exc}")
    else:
        errors.append("suite harness_hashes.json is missing")
    return {
        "valid": not errors,
        "native_files_verified": verified,
        "native_file_count": len(hashes),
        "harness_sha256": harness_digest,
        "runtime_versions": {field: provenance.get(field) for field in ("python", "torch", "hivemind")},
    }, errors


def _allocation_errors(event: dict[str, Any], node: str, expected_members: list[str]) -> list[str]:
    """Check dense allocation, part count, and complete non-overlapping ranges."""
    errors: list[str] = []
    members = _members(_event_value(event, "members"))
    allocations = _event_value(event, "allocations")
    parts = _event_value(event, "parts")
    part_ranges = _event_value(event, "part_ranges")
    if members != expected_members:
        errors.append(f"{node}: allocation roster {members} differs from expected {expected_members}")
        return errors
    member_count = len(members)
    expected_dense = 108 if member_count == 4 else 144 if member_count == 3 else None
    expected_parts = 3 if member_count == 4 else 4 if member_count == 3 else None
    if expected_dense is None or expected_parts is None:
        errors.append(f"{node}: unsupported allocation roster size {member_count}")
        return errors

    if not isinstance(allocations, list) or len(allocations) != member_count:
        errors.append(f"{node}: allocations must be a {member_count}-entry list aligned with members")
    else:
        if any(isinstance(value, bool) or not isinstance(value, int) for value in allocations):
            errors.append(f"{node}: allocations must contain integer dense-element counts")
        elif allocations != [expected_dense] * member_count:
            errors.append(f"{node}: expected equal dense allocations {[expected_dense] * member_count}, got {allocations}")

    if not isinstance(parts, dict) or set(parts) != set(members):
        errors.append(f"{node}: parts must map exactly the local roster members to counts")
    else:
        for peer in members:
            value = parts.get(peer)
            if isinstance(value, bool) or not isinstance(value, int) or value != expected_parts:
                errors.append(f"{node}: {peer} should own {expected_parts} chunks, got {value!r}")

    if not isinstance(part_ranges, dict) or set(part_ranges) != set(members):
        errors.append(f"{node}: part_ranges must map exactly the local roster members to ranges")
        return errors
    ranges: list[tuple[int, int, str]] = []
    for peer in members:
        peer_ranges = part_ranges.get(peer)
        if not isinstance(peer_ranges, list) or len(peer_ranges) != expected_parts:
            errors.append(f"{node}: {peer} should have {expected_parts} part ranges")
            continue
        for interval in peer_ranges:
            if not isinstance(interval, list) or len(interval) != 2:
                errors.append(f"{node}: invalid part range for {peer}: {interval!r}")
                continue
            if any(isinstance(value, bool) or not isinstance(value, int) for value in interval):
                errors.append(f"{node}: non-integer part range for {peer}: {interval!r}")
                continue
            start, end = interval
            if end - start != 36 or start < 0 or end > 432 or start % 36 or end % 36:
                errors.append(f"{node}: range {interval!r} for {peer} is not one aligned 36-float chunk")
            ranges.append((start, end, peer))
    if len(ranges) != 12:
        errors.append(f"{node}: found {len(ranges)} ranges, expected exactly 12 chunks")
    else:
        ranges.sort(key=lambda item: (item[0], item[1], item[2]))
        cursor = 0
        for start, end, peer in ranges:
            if start != cursor:
                errors.append(f"{node}: part ranges have a gap or overlap at flat offset {cursor} (next {start})")
                break
            cursor = end
        if cursor != 432:
            errors.append(f"{node}: part ranges cover {cursor} floats, expected 432")
    return errors


def _same_json(left: Any, right: Any) -> bool:
    return left == right


def _evidence_checks(
    condition: str,
    events: list[dict[str, Any]],
    outputs: dict[str, Any],
    shutdown_ok: bool,
) -> dict[str, Any]:
    errors: list[str] = []
    active = _active_nodes(condition)
    kinds = [_event_kind(event) for event in events]
    allocation_checks: dict[str, Any] = {}
    initial_input_checks: dict[str, bool] = {}
    by_kind_node: dict[tuple[str, str | None], list[dict[str, Any]]] = {}
    for event in events:
        node = _event_node(event)
        if node not in (None, "") and node not in NODES:
            errors.append(f"event {_event_kind(event)} uses unknown node identity {node}")
        if node not in (None, ""):
            pid = event.get("pid")
            if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
                errors.append(f"{node}: event {_event_kind(event)} has no valid process ID")
        by_kind_node.setdefault((_event_kind(event), node), []).append(event)

    for node in active:
        for required_kind in ("local_roster", "allocation", "runner_finalized", "averaging_terminal"):
            count = len(by_kind_node.get((required_kind, node), []))
            if count != 1:
                errors.append(f"{node}: expected exactly one {required_kind} event, found {count}")
        terminal_events = by_kind_node.get(("averaging_terminal", node), [])
        if terminal_events:
            terminal = terminal_events[-1]
            if _event_value(terminal, "status") is None:
                errors.append(f"{node}: averaging_terminal has no status")
        finalized_events = by_kind_node.get(("runner_finalized", node), [])
        if finalized_events:
            finalized = finalized_events[-1]
            for field in ("failed_size", "failed_peer_ids", "banned_senders"):
                if _event_value(finalized, field) is None:
                    errors.append(f"{node}: runner_finalized has no {field}")
            if _event_value(finalized, "native_group_id") is None:
                errors.append(f"{node}: runner_finalized has no native_group_id")
            banned_list = _event_value(finalized, "banned_senders", [])
            if not isinstance(banned_list, list):
                errors.append(f"{node}: runner_finalized banned_senders is not a list")
                banned_list = []
            banned_events = {
                str(_event_value(event, "sender")) for event in events
                if _event_kind(event) == "sender_banned" and _event_node(event) == node
            }
            if banned_events != {str(peer) for peer in banned_list}:
                errors.append(f"{node}: sender_banned events disagree with runner_finalized banned_senders")
            fallback_peers = {
                str(_event_value(event, "reducer")) for event in events
                if _event_kind(event) == "missing_part_fallback" and _event_node(event) == node
            }
            failed_peers = _event_value(finalized, "failed_peer_ids", [])
            if not isinstance(failed_peers, list):
                errors.append(f"{node}: runner_finalized failed_peer_ids is not a list")
                failed_peers = []
            if not fallback_peers.issubset({str(peer) for peer in failed_peers}):
                errors.append(f"{node}: fallback events disagree with runner_finalized failed_peer_ids")
        node_output = outputs.get(node)
        if not isinstance(node_output, dict):
            errors.append(f"{node}: outputs.json entry is missing")
        else:
            if _flatten_matrix(node_output.get("initial")) is None:
                errors.append(f"{node}: initial tensor is missing or has the wrong shape")
            else:
                initial_check = _compare_matrix(_flatten_matrix(node_output.get("initial")), expected_input_tensor(node))
                initial_input_checks[node] = initial_check["matches"]
                if not initial_check["matches"]:
                    errors.append(f"{node}: initial tensor differs from the planned identity-marker input")
            if _flatten_matrix(node_output.get("final")) is None:
                errors.append(f"{node}: final tensor is missing or has the wrong shape")
            if node_output.get("status") not in {"success", "error", "exception", "deadline", "timeout", "idle"}:
                errors.append(f"{node}: outputs.json status is missing or unrecognized")
            if terminal_events and node_output.get("status") != _event_value(terminal_events[-1], "status"):
                errors.append(f"{node}: outputs.json status disagrees with averaging_terminal")
            if terminal_events and finalized_events:
                terminal = terminal_events[-1]
                finalized = finalized_events[-1]
                for field in ("failed_size", "failed_peer_ids", "banned_senders"):
                    if _event_value(node_output, field) != _event_value(finalized, field):
                        errors.append(f"{node}: outputs.json {field} disagrees with runner_finalized")
                    if _event_value(terminal, field) != _event_value(finalized, field):
                        errors.append(f"{node}: averaging_terminal {field} disagrees with runner_finalized")

    # The three-member control keeps A idle, but its starting and final state
    # must still be recorded for a complete comparison.
    for node in NODES:
        if node in active:
            continue
        participation_kinds = {
            "local_roster", "allocation", "runner_finalized", "averaging_terminal",
            "announcement_received", "stream_started", "input_chunk", "result_chunk", "processed_chunk",
        }
        unexpected = [event for event in events if _event_node(event) == node and _event_kind(event) in participation_kinds]
        if unexpected:
            errors.append(f"{node}: inactive participant emitted averaging evidence")
        node_output = outputs.get(node)
        if not isinstance(node_output, dict):
            errors.append(f"{node}: outputs.json entry is missing")
            continue
        if _flatten_matrix(node_output.get("initial")) is None:
            errors.append(f"{node}: initial tensor is missing or has the wrong shape")
        else:
            initial_check = _compare_matrix(_flatten_matrix(node_output.get("initial")), expected_input_tensor(node))
            initial_input_checks[node] = initial_check["matches"]
            if not initial_check["matches"]:
                errors.append(f"{node}: initial tensor differs from the planned identity-marker input")
        if _flatten_matrix(node_output.get("final")) is None:
            errors.append(f"{node}: final tensor is missing or has the wrong shape")
        if node_output.get("status") != "idle":
            errors.append(f"{node}: expected idle status in this condition")

    leader_assembled_count = len(by_kind_node.get(("leader_assembled", "B"), []))
    if leader_assembled_count != 1:
        errors.append(f"B: expected exactly one leader_assembled event, found {leader_assembled_count}")

    unexpected_leaders = [event for event in events if _event_kind(event) == "unexpected_leader"]
    if unexpected_leaders:
        names = sorted({_event_node(event) or "?" for event in unexpected_leaders})
        errors.append(f"unexpected leader election observed at {names}")

    # Require evidence for the intended matchmaking sequence, rather than
    # inferring leadership from a plausible final tensor.
    advertised = [event for event in events if _event_kind(event) == "leader_advertised" and _event_node(event) == "B"]
    if len(advertised) != 1:
        errors.append(f"B: expected exactly one successful leader_advertised event, found {len(advertised)}")
    advertised_time = _event_time(advertised[0]) if len(advertised) == 1 else None
    start_kinds = {"follower_search_started", "follower_started", "search_started", "step_started"}
    leader_starts = [
        event for event in events
        if _event_kind(event) in start_kinds and _event_node(event) == "B"
    ]
    if len(leader_starts) != 1:
        errors.append(f"B: expected exactly one leader search-start event, found {len(leader_starts)}")
    elif advertised_time is not None:
        leader_start_time = _event_time(leader_starts[0])
        if leader_start_time is None or leader_start_time >= advertised_time:
            errors.append("B: leader search did not start before its successful advertisement")
    follower_starts = [
        event for event in events
        if _event_kind(event) in start_kinds and _event_node(event) in active and _event_node(event) != "B"
    ]
    accepted = [event for event in events if _event_kind(event) == "join_accepted" and _event_node(event) == "B"]
    followers = [node for node in active if node != "B"]
    starts_by_node = {_event_node(event): event for event in follower_starts}
    for follower in followers:
        accepted_count = sum(str(_event_value(event, "follower")) == follower for event in accepted)
        if accepted_count != 1:
            errors.append(f"B: expected exactly one join_accepted event for {follower}, found {accepted_count}")
        start = starts_by_node.get(follower)
        if start is None:
            errors.append(f"{follower}: missing follower search-start event")
        elif advertised_time is None or _event_time(start) is None or _event_time(start) <= advertised_time:
            errors.append(f"{follower}: search did not start after B's successful advertisement")
        accepted_event = next((event for event in accepted if str(_event_value(event, "follower")) == follower), None)
        if start is not None and accepted_event is not None:
            start_time = _event_time(start)
            accepted_time = _event_time(accepted_event)
            if start_time is None or accepted_time is None or accepted_time < start_time:
                errors.append(f"B: accepted {follower} before its search-start event")
    for node in active:
        if node == "B":
            continue
        if sum(_event_node(event) == node for event in follower_starts) > 1:
            errors.append(f"{node}: duplicate follower search-start event")
    for event in events:
        if _event_kind(event) in start_kinds and _event_node(event) not in active:
            errors.append(f"inactive node {_event_node(event)} unexpectedly started a matchmaking step")
    for event in accepted:
        follower = str(_event_value(event, "follower"))
        if follower not in followers:
            errors.append(f"B: unexpected join_accepted follower {follower}")
        if advertised_time is not None and (_event_time(event) is None or _event_time(event) < advertised_time):
            errors.append(f"B: accepted {follower} before the leader advertisement was observed")

    for node in active:
        terminal = next((event for event in by_kind_node.get(("averaging_terminal", node), [])), None)
        announcement = next((event for event in by_kind_node.get(("announcement_received", node), [])), None)
        if node == "B":
            announcement = next((event for event in by_kind_node.get(("local_roster", node), [])), None)
        if terminal is not None and announcement is not None:
            start_time, terminal_time = _event_time(announcement), _event_time(terminal)
            if start_time is None or terminal_time is None or terminal_time < start_time:
                errors.append(f"{node}: terminal outcome precedes announcement/aggregation start")

    assembled_events = by_kind_node.get(("leader_assembled", "B"), [])
    assembled_roster = _members(_event_value(assembled_events[0], "members")) if assembled_events else None
    local_rosters = {
        node: _members(_event_value(by_kind_node[("local_roster", node)][0], "members"))
        for node in active if by_kind_node.get(("local_roster", node))
    }

    # Check each advertised payload independently, including gathered metadata.
    sent = [event for event in events if _event_kind(event) == "announcement_sent" and _event_node(event) == "B"]
    received = [event for event in events if _event_kind(event) == "announcement_received"]
    expected_recipients = [node for node in active if node != "B"]
    sent_by_recipient = {str(_event_value(event, "recipient")): event for event in sent}
    received_by_node = {_event_node(event): event for event in received}
    announcement_payload_checks: dict[str, Any] = {}
    for recipient in expected_recipients:
        if sum(str(_event_value(event, "recipient")) == recipient for event in sent) != 1:
            errors.append(f"B: expected exactly one announcement_sent event for {recipient}")
        if sum(_event_node(event) == recipient for event in received) != 1:
            errors.append(f"{recipient}: expected exactly one announcement_received event")
        sent_event = sent_by_recipient.get(recipient)
        recv_event = received_by_node.get(recipient)
        if sent_event is None:
            errors.append(f"B: missing announcement_sent for {recipient}")
            continue
        if recv_event is None:
            errors.append(f"{recipient}: missing announcement_received")
            continue
        for field in ("payload_hex", "group_id", "members", "gathered_nodes"):
            if _event_value(sent_event, field) is None or _event_value(recv_event, field) is None:
                errors.append(f"{recipient}: sent/received announcement lacks {field}")
            elif not _same_json(_event_value(sent_event, field), _event_value(recv_event, field)):
                errors.append(f"{recipient}: received announcement {field} differs from B's sent payload")
        payload_hex = _event_value(sent_event, "payload_hex")
        payload_digest = None
        if isinstance(payload_hex, str):
            try:
                payload_bytes = bytes.fromhex(payload_hex)
                payload_digest = hashlib.sha256(payload_bytes).hexdigest()
            except ValueError:
                errors.append(f"B: announcement to {recipient} has invalid payload_hex")
        sent_members = _members(_event_value(sent_event, "members"))
        sent_gathered = _members(_event_value(sent_event, "gathered_nodes"))
        expected_message_members = _expected_local_roster(condition, recipient)
        if sent_members is not None and expected_message_members is not None and sent_members != expected_message_members:
            errors.append(f"B: announcement to {recipient} has roster {sent_members}, expected {expected_message_members}")
        if sent_members is not None and sent_gathered != sent_members:
            errors.append(f"B: announcement to {recipient} gathered metadata is not aligned with the roster")
        if str(_event_value(recv_event, "sender", "B")) != "B":
            errors.append(f"{recipient}: announcement sender is not B")
        announcement_payload_checks[recipient] = {
            "payload_sha256": payload_digest,
            "sent_equals_received": all(
                _event_value(sent_event, field) == _event_value(recv_event, field)
                for field in ("payload_hex", "group_id", "members", "gathered_nodes")
            ),
            "members": sent_members,
            "gathered_nodes_aligned": sent_members is not None and sent_gathered == sent_members,
            "group_id": _event_value(sent_event, "group_id"),
        }

    for event in sent:
        recipient = str(_event_value(event, "recipient"))
        if recipient not in expected_recipients:
            errors.append(f"B: unexpected announcement recipient {recipient}")
    for event in received:
        recipient = _event_node(event)
        if recipient not in expected_recipients:
            errors.append(f"unexpected announcement_received event at {recipient}")

    for event in events:
        kind = _event_kind(event)
        node = _event_node(event)
        if kind == "local_roster" and node in active:
            expected = _expected_local_roster(condition, node)
            observed = _members(_event_value(event, "members"))
            if observed is None:
                errors.append(f"{node}: local_roster has no members list")
            elif expected is not None and observed != expected:
                errors.append(f"{node}: local roster {observed} differs from planned {expected}")
            if _event_value(event, "native_group_id") is None:
                errors.append(f"{node}: local_roster has no native_group_id")
            gathered_nodes = _members(_event_value(event, "gathered_nodes"))
            if observed is not None and gathered_nodes != observed:
                errors.append(f"{node}: local gathered metadata is missing or misaligned with its roster")
            if observed is not None:
                errors.extend(_roster_metadata_errors(event, node, observed))
        if kind == "allocation" and node in active:
            expected = _expected_local_roster(condition, node)
            if expected is None:
                errors.append(f"{node}: no expected local roster for allocation validation")
            else:
                check_errors = _allocation_errors(event, node, expected)
                weight = _event_value(event, "weight")
                if isinstance(weight, bool) or not isinstance(weight, (int, float)) or not _close(float(weight), 1.0):
                    check_errors.append(f"{node}: native averaging weight is missing or differs from 1")
                if _event_value(event, "compression") not in {"NoCompression", "NoCompression()"}:
                    check_errors.append(f"{node}: native allocation did not record NoCompression")
                check_errors.extend(_roster_metadata_errors(event, node, expected, check_bandwidths=False))
                if _event_value(event, "native_group_id") is None:
                    check_errors.append(f"{node}: allocation has no native_group_id")
                errors.extend(check_errors)
                allocation_checks[node] = {
                    "valid": not check_errors,
                    "errors": check_errors,
                    "members": _members(_event_value(event, "members")),
                    "allocations": _event_value(event, "allocations"),
                    "parts": _event_value(event, "parts"),
                    "part_ranges": _event_value(event, "part_ranges"),
                }
        if kind == "leader_assembled" and node == "B":
            observed = _members(_event_value(event, "members"))
            # Matchmaking assembles the four-member group before the membership
            # intervention changes B's local aggregation view.
            expected = ["B", "D", "C"] if condition == "control3" else list(NODES)
            if observed is None:
                errors.append("B: leader_assembled has no members list")
            elif expected is not None and observed != expected:
                errors.append(f"B: assembled roster {observed} differs from planned {expected}")

    native_group_ids = {
        _canonical_id(_event_value(event, "native_group_id"))
        for event in events
        if _event_kind(event) in {"leader_assembled", "local_roster", "allocation", "runner_finalized"} and _event_node(event) in active
        and _event_value(event, "native_group_id") is not None
    }
    announcement_group_ids = {
        _canonical_id(_event_value(event, "group_id"))
        for event in sent
        if _event_value(event, "group_id") is not None
    }
    if len(native_group_ids) > 1:
        errors.append(f"participants report inconsistent native group IDs: {sorted(native_group_ids)}")
    if len(announcement_group_ids) > 1:
        errors.append(f"B sent announcements with inconsistent group IDs: {sorted(announcement_group_ids)}")
    if native_group_ids and announcement_group_ids and native_group_ids != announcement_group_ids:
        errors.append(
            f"local aggregation and leader announcements use different group IDs: "
            f"{sorted(native_group_ids)} vs {sorted(announcement_group_ids)}"
        )

    if not shutdown_ok:
        errors.append("shutdown evidence does not verify clean process exit")
    return {
        "valid": not errors,
        "errors": errors,
        "active_nodes": active,
        "event_count": len(events),
        "event_kinds": sorted(set(kinds)),
        "initial_input_identity_checks": initial_input_checks,
        "allocation_checks": allocation_checks,
        "announcement_payload_checks": announcement_payload_checks,
        "native_group_ids": sorted(native_group_ids),
        "announcement_group_ids": sorted(announcement_group_ids),
        "leader_assembled_roster": assembled_roster,
        "local_aggregation_rosters": local_rosters,
        "join_accepted_followers": sorted(
            str(_event_value(event, "follower")) for event in accepted
            if _event_value(event, "follower") is not None
        ),
        "leader_advertised_at": advertised_time,
        "clean_shutdown_verified": shutdown_ok,
    }


def _output_analysis(outputs: dict[str, Any], events: list[dict[str, Any]]) -> dict[str, Any]:
    saved_inputs = {
        node: matrix
        for node, record in outputs.items()
        if node in NODES and isinstance(record, dict)
        if (matrix := _flatten_matrix(record.get("initial"))) is not None
    }
    local_rosters = {
        _event_node(event): _members(_event_value(event, "members"))
        for event in events
        if _event_kind(event) == "local_roster" and _event_node(event) in NODES
    }
    terminal_by_node = {
        _event_node(event): event
        for event in events
        if _event_kind(event) == "averaging_terminal"
    }
    finalized_by_node = {
        _event_node(event): event
        for event in events
        if _event_kind(event) == "runner_finalized"
    }
    receipt_by_node = {
        _event_node(event): event
        for event in events
        if _event_kind(event) == "announcement_received"
    }
    leader_event = next((event for event in events if _event_kind(event) == "leader_assembled" and _event_node(event) == "B"), None)
    results: dict[str, Any] = {}
    for node in NODES:
        record = outputs.get(node)
        if not isinstance(record, dict):
            results[node] = {"status": "missing", "shape_valid": False, "matches_control_mean": False}
            continue
        status = str(record.get("status", "missing"))
        matrix = _flatten_matrix(record.get("final"))
        roster = local_rosters.get(node)
        roster_mean = _mean_from_saved_inputs(saved_inputs, roster)
        comparison = (
            _compare_matrix(matrix, roster_mean)
            if roster_mean is not None
            else {"matches": False, "max_abs_error": None, "shape_valid": matrix is not None}
        )
        origin_union: set[int] = set()
        coordinate_errors: list[float] = []
        coordinate_ok = matrix is not None
        inferred_expected_ok = matrix is not None
        excluded_rows = 0
        contributing_by_row: list[list[str]] = []
        origin_rows = [0, 0, 0, 0]
        if matrix is not None:
            for row_index, row in enumerate(matrix):
                weights = row[1:5]
                active_indices = [i for i, weight in enumerate(weights) if abs(weight) > ABS_TOL]
                origin_union.update(active_indices)
                contributing_by_row.append([NODES[i] for i in active_indices])
                for index in active_indices:
                    origin_rows[index] += 1
                if 2 not in active_indices:
                    excluded_rows += 1
                expected_weight = 1.0 / len(active_indices) if active_indices else 0.0
                row_sources = [saved_inputs.get(NODES[index]) for index in active_indices]
                expected_main = (
                    sum(source[row_index][0] for source in row_sources) / len(row_sources)
                    if row_sources and all(source is not None for source in row_sources) else None
                )
                markers_are_equal_mean = (
                    bool(active_indices)
                    and all(_close(weights[index], expected_weight) for index in active_indices)
                    and all(_close(weights[index], 0.0) for index in range(4) if index not in active_indices)
                )
                if expected_main is None or not markers_are_equal_mean or not _close(row[0], expected_main):
                    inferred_expected_ok = False
                for i, weight in enumerate(weights):
                    expected_coordinate = weight * (row_index + 1)
                    error = abs(row[5 + i] - expected_coordinate)
                    coordinate_errors.append(error)
                    if error > ABS_TOL:
                        coordinate_ok = False
            if any(not row for row in contributing_by_row):
                inferred_expected_ok = False
            if not coordinate_ok:
                inferred_expected_ok = False
        terminal = terminal_by_node.get(node)
        finalized = finalized_by_node.get(node)
        failed_size = _event_value(terminal, "failed_size", 0) if terminal else None
        failed_peers = _event_value(terminal, "failed_peer_ids", []) if terminal else None
        failed_size_positive = _positive_size(failed_size)
        finalized_failed_size = _event_value(finalized, "failed_size", 0) if finalized else None
        finalized_failed_size_positive = _positive_size(finalized_failed_size)
        finalized_failed_peers = _event_value(finalized, "failed_peer_ids", []) if finalized else []
        banned_senders = _event_value(finalized, "banned_senders", []) if finalized else []
        terminal = terminal_by_node.get(node)
        local_start = next((event for event in events if _event_kind(event) == "local_roster" and _event_node(event) == node), None)
        receipt = receipt_by_node.get(node) or (local_start if node == "B" else None) or (leader_event if node == "B" else None)
        receipt_time = _event_time(receipt) if receipt else None
        terminal_time = _event_time(terminal) if terminal else None
        announcement_to_terminal = (
            max(0.0, terminal_time - receipt_time)
            if receipt_time is not None and terminal_time is not None else None
        )
        fallback = bool(
            failed_size_positive
            or _is_nonempty(failed_peers)
            or finalized_failed_size_positive
            or _is_nonempty(finalized_failed_peers)
            or any(_event_kind(event) in {"fallback", "missing_part_fallback"} and _event_node(event) == node for event in events)
        )
        target_absent = 2 not in origin_union
        bdc_mean = _mean_from_saved_inputs(saved_inputs, ("B", "D", "C"))
        excluded_cleanly = bool(
            status == "success"
            and not fallback
            and target_absent
            and origin_union == {NODE_INDEX["B"], NODE_INDEX["D"], NODE_INDEX["C"]}
            and inferred_expected_ok
            and coordinate_ok
            and matrix is not None
            and bdc_mean is not None
            and _compare_matrix(matrix, bdc_mean)["matches"]
        )
        results[node] = {
            "status": status,
            "operation_status": _operation_status(record),
            "shape_valid": matrix is not None,
            "matches_control_mean": comparison["matches"],
            "control_mean_max_abs_error": comparison["max_abs_error"],
            "local_roster_members": roster,
            "matches_local_roster_mean": comparison["matches"],
            "local_roster_mean_max_abs_error": comparison["max_abs_error"],
            "origin_nodes_observed": [NODES[i] for i in range(4) if i in origin_union],
            "origin_row_coverage": {NODES[i]: origin_rows[i] for i in range(4)},
            "target_A_absent_from_all_rows": target_absent if matrix is not None else None,
            "rows_without_A_origin": excluded_rows if matrix is not None else None,
            "coordinate_markers_correct": coordinate_ok,
            "coordinate_marker_max_abs_error": max(coordinate_errors, default=None),
            "matches_mean_implied_by_origin_markers": inferred_expected_ok,
            "fallback_indicated": fallback,
            "failed_size": failed_size if failed_size is not None else finalized_failed_size,
            "failed_peer_ids": failed_peers if _is_nonempty(failed_peers) else finalized_failed_peers,
            "banned_senders": banned_senders,
            "A_cleanly_excluded_from_completed_BDC_mean": excluded_cleanly,
            "per_row_origin_nodes": contributing_by_row if matrix is not None else None,
            "elapsed_seconds": record.get("elapsed_seconds"),
            "announcement_to_terminal_seconds": announcement_to_terminal,
            "error": record.get("error"),
        }
    active = [node for node in NODES if node in local_rosters]
    for node in active:
        result = results[node]
        result["matches_expected_control"] = result.get("matches_local_roster_mean", False)
    return results


def _participant_agreement(outputs: dict[str, Any], active: list[str]) -> dict[str, Any]:
    matrices = {
        node: _flatten_matrix(outputs.get(node, {}).get("final")) if isinstance(outputs.get(node), dict) else None
        for node in active
    }
    by_node: dict[str, Any] = {}
    pairs: list[dict[str, Any]] = []
    for left_index, left in enumerate(active):
        differences: dict[str, float | None] = {}
        agrees_with = []
        for right in active:
            if left == right:
                continue
            left_matrix, right_matrix = matrices[left], matrices[right]
            if left_matrix is None or right_matrix is None:
                maximum = None
            else:
                maximum = max(
                    abs(a - b)
                    for left_row, right_row in zip(left_matrix, right_matrix)
                    for a, b in zip(left_row, right_row)
                )
                if maximum <= ABS_TOL:
                    agrees_with.append(right)
            differences[right] = maximum
            if active.index(right) > left_index:
                pairs.append({"left": left, "right": right, "max_abs_difference": maximum,
                              "agree_within_tolerance": maximum is not None and maximum <= ABS_TOL})
        by_node[left] = {"agrees_with": agrees_with, "max_abs_difference": differences}
    return {"absolute_tolerance": ABS_TOL, "by_node": by_node, "pairs": pairs}


def _stream_proof(
    events: list[dict[str, Any]],
    active: list[str],
    outputs: dict[str, Any],
    *,
    matrix_parser=_flatten_matrix,
) -> dict[str, Any]:
    """Validate saved inputs, native stream payloads, and returned corrections.

    Incoming chunks are checked against the sender's saved input and its
    allocation for that reducer. The reducer averages by native part index;
    its response is the actual mean minus the sender's input chunk. Coordinate
    alignment is reported separately because misalignment is the ordering
    intervention's outcome, not an instrumentation defect.
    """
    rosters: dict[str, list[str]] = {}
    allocations: dict[str, dict[str, Any]] = {}
    saved_inputs: dict[str, list[float]] = {}
    for event in events:
        node = _event_node(event)
        if _event_kind(event) == "local_roster" and node in active:
            members = _members(_event_value(event, "members"))
            if members is not None:
                rosters[node] = members
        elif _event_kind(event) == "allocation" and node in active:
            allocations[node] = event
    for node, record in outputs.items():
        if node in active and isinstance(record, dict):
            matrix = matrix_parser(record.get("initial"))
            if matrix is not None:
                saved_inputs[node] = [value for row in matrix for value in row]

    inputs: dict[tuple[str, str], list[dict[str, Any]]] = {}
    results: dict[tuple[str, str], list[dict[str, Any]]] = {}
    stream_events: dict[tuple[str, str], dict[str, list[dict[str, Any]]]] = {}
    sender_bans: dict[tuple[str, str], list[dict[str, Any]]] = {}
    fallbacks: dict[tuple[str, str], list[dict[str, Any]]] = {}
    local_processed: dict[str, list[dict[str, Any]]] = {node: [] for node in active}
    malformed: dict[str, list[str]] = {node: [] for node in active}
    for event in events:
        kind = _event_kind(event)
        node = _event_node(event)
        if node in active and kind == "sender_banned":
            sender = _event_value(event, "sender")
            if isinstance(sender, str):
                sender_bans.setdefault((node, sender), []).append(event)
        elif node in active and kind == "missing_part_fallback":
            reducer = _event_value(event, "reducer")
            if isinstance(reducer, str):
                fallbacks.setdefault((node, reducer), []).append(event)
        elif node in active and kind == "processed_chunk" and _event_value(event, "reducer", _event_value(event, "peer")) == node:
            local_processed[node].append(event)
        if kind not in {"input_chunk", "result_chunk", "stream_started", "stream_closed", "stream_error", "stream_code"}:
            continue
        sender = _event_value(event, "sender")
        receiver = _event_value(event, "receiver")
        stream_id = _event_value(event, "stream_id")
        if node not in active:
            continue
        if not isinstance(stream_id, str) or not stream_id:
            malformed[node].append(f"{kind} has no stream_id")
        if kind == "input_chunk":
            if node != receiver or sender is None:
                malformed[node].append("input_chunk node/sender/receiver identity is inconsistent")
                continue
            inputs.setdefault((node, str(sender)), []).append(event)
            stream_events.setdefault((node, str(sender)), {}).setdefault(kind, []).append(event)
        elif kind == "result_chunk":
            if node != sender or receiver is None:
                malformed[node].append("result_chunk node/sender/receiver identity is inconsistent")
                continue
            results.setdefault((node, str(receiver)), []).append(event)
            stream_events.setdefault((node, str(receiver)), {}).setdefault(kind, []).append(event)
        elif kind == "stream_started":
            if node != receiver or sender is None:
                malformed[node].append("stream_started node/sender/receiver identity is inconsistent")
                continue
            stream_events.setdefault((node, str(sender)), {}).setdefault(kind, []).append(event)
        elif kind == "stream_closed":
            if node != receiver or sender is None:
                malformed[node].append("stream_closed node/sender/receiver identity is inconsistent")
                continue
            stream_events.setdefault((node, str(sender)), {}).setdefault(kind, []).append(event)
        elif kind == "stream_error":
            if node != receiver or sender is None:
                malformed[node].append("stream_error node/sender/receiver identity is inconsistent")
                continue
            stream_events.setdefault((node, str(sender)), {}).setdefault(kind, []).append(event)
        elif kind == "stream_code":
            if receiver is None or (sender is not None and sender != node):
                malformed[node].append("stream_code node/receiver identity is inconsistent")
                continue
            stream_events.setdefault((node, str(receiver)), {}).setdefault(kind, []).append(event)
        else:
            malformed[node].append(f"unexpected native stream event kind {kind}")

    proof: dict[str, Any] = {}
    for node in active:
        issues = list(malformed.get(node, []))
        evidence_issues = list(malformed.get(node, []))
        expected_fallback_gaps: list[str] = []
        roster = rosters.get(node, [])
        allocation = allocations.get(node)
        ranges_by_peer = _event_value(allocation, "part_ranges") if allocation else None
        parts_by_peer = _event_value(allocation, "parts") if allocation else None
        if allocation is None:
            issues.append("participant has no native allocation event")
        input_checked = 0
        result_checked = 0
        input_alignment: dict[str, bool] = {}
        result_alignment: dict[str, bool] = {}

        def valid_ban_before(peer: str, at_time: float | None) -> bool:
            events_for_peer = sender_bans.get((node, peer), [])
            if len(events_for_peer) != 1 or at_time is None:
                return False
            ban = events_for_peer[0]
            ban_time = _event_time(ban)
            group_id = _canonical_id(_event_value(allocation, "native_group_id")) if allocation else None
            return (
                ban_time is not None
                and ban_time <= at_time
                and _canonical_id(_event_value(ban, "native_group_id")) == group_id
            )

        first_local_process_time = min(
            (_event_time(event) for event in local_processed[node] if _event_time(event) is not None),
            default=None,
        )

        # This node is the reducer. Verify every remote contribution against
        # the sending participant's own saved tensor and recipient partition.
        for sender in roster:
            if sender == node:
                continue
            source_allocation = allocations.get(sender)
            source_ranges_by_peer = _event_value(source_allocation, "part_ranges") if source_allocation else None
            source_parts = _event_value(source_allocation, "parts") if source_allocation else None
            expected_ranges = source_ranges_by_peer.get(node) if isinstance(source_ranges_by_peer, dict) else None
            expected_count = source_parts.get(node) if isinstance(source_parts, dict) else None
            local_ranges = ranges_by_peer.get(node) if isinstance(ranges_by_peer, dict) else None
            input_alignment[sender] = expected_ranges == local_ranges
            chunks = sorted(inputs.get((node, sender), []), key=lambda event: _event_value(event, "part_index", -1))
            state = stream_events.get((node, sender), {})
            if isinstance(expected_count, int) and not isinstance(expected_count, bool) and expected_count == 0:
                if chunks or state:
                    message = f"{sender}->{node}: stream evidence exists for a zero-part allocation"
                    issues.append(message)
                    evidence_issues.append(message)
                continue
            if not isinstance(expected_ranges, list) or not isinstance(expected_count, int) or isinstance(expected_count, bool):
                # A sender whose own announcement excludes this reducer has no
                # recipient partition to stream. This is an explained gap only
                # when this reducer later records a matching, timely ban.
                if (
                    not expected_ranges
                    and not chunks
                    and node not in (rosters.get(sender) or [])
                    and valid_ban_before(sender, first_local_process_time)
                ):
                    message = f"{sender}->{node}: inbound contribution is absent after a timed sender ban"
                    issues.append(message)
                    expected_fallback_gaps.append(message)
                    continue
                message = f"{sender}->{node}: sender allocation lacks recipient ranges/count"
                issues.append(message)
                evidence_issues.append(message)
                continue
            if len(expected_ranges) != expected_count:
                message = f"{sender}->{node}: sender allocation ranges/count disagree"
                issues.append(message)
                evidence_issues.append(message)
            if expected_count == 0:
                if chunks or state:
                    message = f"{sender}->{node}: stream evidence exists for a zero-part allocation"
                    issues.append(message)
                    evidence_issues.append(message)
                continue
            if len(state.get("stream_started", [])) != 1 or len(state.get("stream_closed", [])) != 1:
                message = f"{sender}->{node}: expected one started and closed native stream"
                issues.append(message)
                evidence_issues.append(message)
            has_stream_failure = bool(state.get("stream_error") or state.get("stream_code"))
            if has_stream_failure:
                message = f"{sender}->{node}: native stream recorded an error or non-success code"
                issues.append(message)
                evidence_issues.append(message)
            stream_ids = {
                str(_event_value(item, "stream_id"))
                for items in state.values() for item in items
                if _event_value(item, "stream_id") is not None
            }
            if len(stream_ids) != 1:
                message = f"{sender}->{node}: native stream events do not share one stream_id"
                issues.append(message)
                evidence_issues.append(message)
            observed_indices = [_event_value(event, "part_index") for event in chunks]
            complete_input = len(chunks) == expected_count and observed_indices == list(range(expected_count))
            if not complete_input:
                supported_partial = (
                    observed_indices == list(range(len(observed_indices)))
                    and has_stream_failure
                    and valid_ban_before(sender, first_local_process_time)
                )
                message = f"{sender}->{node}: observed {len(chunks)} input chunks, expected {expected_count}"
                issues.append(message)
                if supported_partial:
                    expected_fallback_gaps.append(message)
                else:
                    evidence_issues.append(message)
                if observed_indices != list(range(len(observed_indices))):
                    message = f"{sender}->{node}: input part indices are missing, duplicated, or reordered"
                    issues.append(message)
                    evidence_issues.append(message)
            saved = saved_inputs.get(sender)
            if saved is None:
                issues.append(f"{sender}: saved initial tensor is missing")
            for chunk in chunks:
                index = _event_value(chunk, "part_index")
                values = _numeric_values(_event_value(chunk, "values"))
                if not isinstance(index, int) or isinstance(index, bool) or not 0 <= index < len(expected_ranges):
                    continue
                interval = expected_ranges[index]
                if not isinstance(interval, list) or len(interval) != 2 or values is None:
                    issues.append(f"{sender}->{node}: malformed input chunk {index}")
                    continue
                start, end = interval
                expected = saved[start:end] if saved is not None and 0 <= start <= end <= len(saved) else []
                if len(values) != len(expected) or any(not _close(actual, target) for actual, target in zip(values, expected)):
                    issues.append(f"{sender}->{node}: input chunk {index} differs from saved sender input/range")
                group_id = _canonical_id(_event_value(chunk, "native_group_id"))
                expected_group_id = _canonical_id(_event_value(allocation, "native_group_id")) if allocation else None
                if group_id != expected_group_id:
                    issues.append(f"{sender}->{node}: input chunk uses an unexpected native group ID")
                if index == 0:
                    if _canonical_id(_event_value(chunk, "input_group_id")) != expected_group_id:
                        issues.append(f"{sender}->{node}: first input chunk lacks the native group ID")
                    if _event_value(chunk, "code") != 4:
                        issues.append(f"{sender}->{node}: first input chunk is not PART_FOR_AVERAGING")
                weight = _event_value(chunk, "weight")
                if isinstance(weight, bool) or not isinstance(weight, (int, float)) or not _close(float(weight), 1.0):
                    issues.append(f"{sender}->{node}: input chunk weight is missing or differs from 1")
                compression = _event_value(chunk, "compression")
                if compression not in (0, "0", "NONE"):
                    issues.append(f"{sender}->{node}: input chunk is not uncompressed")
            input_checked += len(chunks)

        # Validate this participant's returned corrections. A result generated
        # by reducer R uses R's local shard for its mean; the recipient applies
        # it to its own allocation for R. The two ranges may differ under the
        # ordering intervention, so keep alignment as a separate result.
        for reducer in roster:
            if reducer == node:
                continue
            reducer_allocation = allocations.get(reducer)
            reducer_ranges_by_peer = _event_value(reducer_allocation, "part_ranges") if reducer_allocation else None
            reducer_parts = _event_value(reducer_allocation, "parts") if reducer_allocation else None
            reducer_roster = rosters.get(reducer, [])
            reducer_self_ranges = reducer_ranges_by_peer.get(reducer) if isinstance(reducer_ranges_by_peer, dict) else None
            recipient_ranges = ranges_by_peer.get(reducer) if isinstance(ranges_by_peer, dict) else None
            reducer_expected_count = reducer_parts.get(reducer) if isinstance(reducer_parts, dict) else None
            recipient_expected_count = parts_by_peer.get(reducer) if isinstance(parts_by_peer, dict) else None
            result_alignment[reducer] = reducer_self_ranges == recipient_ranges
            chunks = sorted(results.get((reducer, node), []), key=lambda event: _event_value(event, "part_index", -1))
            state = stream_events.get((reducer, node), {})
            fallback_records = fallbacks.get((node, reducer), [])

            if isinstance(recipient_expected_count, int) and not isinstance(recipient_expected_count, bool) and recipient_expected_count == 0:
                if chunks or state or fallback_records:
                    message = f"{reducer}->{node}: evidence exists for a zero-part result allocation"
                    issues.append(message)
                    evidence_issues.append(message)
                continue

            if fallback_records:
                registered = _event_value(fallback_records[0], "registered_parts") if len(fallback_records) == 1 else None
                expected_group = _canonical_id(_event_value(allocation, "native_group_id")) if allocation else None
                fallback_group = _canonical_id(_event_value(fallback_records[0], "native_group_id")) if len(fallback_records) == 1 else None
                observed_indices = [_event_value(event, "part_index") for event in chunks]
                fallback_is_well_formed = (
                    len(fallback_records) == 1
                    and isinstance(registered, int)
                    and not isinstance(registered, bool)
                    and isinstance(recipient_expected_count, int)
                    and not isinstance(recipient_expected_count, bool)
                    and 0 <= registered < recipient_expected_count
                    and fallback_group == expected_group
                    and observed_indices == list(range(registered))
                    and bool(state.get("stream_error") or state.get("stream_code"))
                    and len(state.get("stream_started", [])) == 1
                    and len(state.get("stream_closed", [])) == 1
                )
                if node not in reducer_roster:
                    fallback_is_well_formed = fallback_is_well_formed and registered == 0 and not chunks
                if not fallback_is_well_formed:
                    message = f"{reducer}->{node}: fallback does not explain the observed result stream"
                    issues.append(message)
                    evidence_issues.append(message)
                else:
                    message = f"{reducer}->{node}: result stream is incomplete and completed with recorded fallback"
                    issues.append(message)
                    expected_fallback_gaps.append(message)
                # Any chunk received before a fallback still has to be a
                # correctly shaped, numerically supported native correction.
                # A peer excluded by the reducer's roster cannot have returned
                # even a prefix of results.
                if node not in reducer_roster:
                    continue
            elif node not in reducer_roster:
                message = f"{reducer}->{node}: reducer's local roster excludes this recipient without fallback evidence"
                issues.append(message)
                evidence_issues.append(message)
                continue

            if node not in reducer_roster:
                message = f"{reducer}->{node}: reducer's local roster excludes this recipient"
                issues.append(message)
                evidence_issues.append(message)
            if not isinstance(reducer_self_ranges, list) or not isinstance(reducer_expected_count, int):
                message = f"{reducer}->{node}: reducer allocation lacks its own result ranges/count"
                issues.append(message)
                evidence_issues.append(message)
                continue
            if not isinstance(recipient_ranges, list) or not isinstance(recipient_expected_count, int):
                message = f"{reducer}->{node}: recipient allocation lacks output ranges/count"
                issues.append(message)
                evidence_issues.append(message)
                continue
            if reducer_expected_count != recipient_expected_count:
                message = (f"{reducer}->{node}: reducer and recipient expect different result part counts "
                           f"({reducer_expected_count} vs {recipient_expected_count})")
                issues.append(message)
                evidence_issues.append(message)
            expected_results = reducer_expected_count
            fallback_is_well_formed = bool(
                fallback_records
                and len(fallback_records) == 1
                and isinstance(_event_value(fallback_records[0], "registered_parts"), int)
                and not isinstance(_event_value(fallback_records[0], "registered_parts"), bool)
                and 0 <= _event_value(fallback_records[0], "registered_parts") < recipient_expected_count
                and _canonical_id(_event_value(fallback_records[0], "native_group_id"))
                == (_canonical_id(_event_value(allocation, "native_group_id")) if allocation else None)
                and [_event_value(event, "part_index") for event in chunks]
                == list(range(_event_value(fallback_records[0], "registered_parts")))
                and bool(state.get("stream_error") or state.get("stream_code"))
                and len(state.get("stream_started", [])) == 1
                and len(state.get("stream_closed", [])) == 1
            )
            if fallback_is_well_formed:
                expected_results = _event_value(fallback_records[0], "registered_parts")
            if len(state.get("stream_started", [])) != 1 or len(state.get("stream_closed", [])) != 1:
                message = f"{reducer}->{node}: expected one started and closed native result stream"
                issues.append(message)
                if not fallback_is_well_formed:
                    evidence_issues.append(message)
            if state.get("stream_error") or state.get("stream_code"):
                message = f"{reducer}->{node}: native result stream recorded an error or non-success code"
                issues.append(message)
                if fallback_is_well_formed:
                    expected_fallback_gaps.append(message)
                else:
                    evidence_issues.append(message)
            stream_ids = {
                str(_event_value(item, "stream_id"))
                for items in state.values() for item in items
                if _event_value(item, "stream_id") is not None
            }
            if len(stream_ids) != 1:
                message = f"{reducer}->{node}: input/result events do not share one stream_id"
                issues.append(message)
                evidence_issues.append(message)
            if len(chunks) != expected_results or len(reducer_self_ranges) < expected_results:
                message = f"{reducer}->{node}: observed {len(chunks)} result chunks, expected reducer-owned count {expected_results}"
                issues.append(message)
                if not fallback_is_well_formed:
                    evidence_issues.append(message)
            if [_event_value(event, "part_index") for event in chunks] != list(range(expected_results)):
                message = f"{reducer}->{node}: result part indices are missing, duplicated, or reordered"
                issues.append(message)
                evidence_issues.append(message)
            local = saved_inputs.get(reducer)
            reducer_inputs: dict[str, list[dict[str, Any]]] = {}
            for source in reducer_roster:
                if source != reducer:
                    reducer_inputs[source] = sorted(inputs.get((reducer, source), []), key=lambda event: _event_value(event, "part_index", -1))
            recipient_chunks = sorted(inputs.get((reducer, node), []), key=lambda event: _event_value(event, "part_index", -1))
            for chunk in chunks:
                index = _event_value(chunk, "part_index")
                values = _numeric_values(_event_value(chunk, "values"))
                if not isinstance(index, int) or isinstance(index, bool) or not 0 <= index < len(recipient_ranges):
                    continue
                recipient_interval = recipient_ranges[index]
                if not isinstance(recipient_interval, list) or len(recipient_interval) != 2 or values is None:
                    issues.append(f"{reducer}->{node}: malformed result chunk {index}")
                    continue
                if len(values) != recipient_interval[1] - recipient_interval[0]:
                    issues.append(f"{reducer}->{node}: result chunk {index} length differs from recipient allocation")
                reducer_interval = reducer_self_ranges[index] if isinstance(reducer_self_ranges, list) and index < len(reducer_self_ranges) else None
                if not isinstance(reducer_interval, list) or len(reducer_interval) != 2 or local is None:
                    issues.append(f"{reducer}->{node}: reducer local shard evidence is missing for result {index}")
                    continue
                if len(values) != reducer_interval[1] - reducer_interval[0]:
                    issues.append(f"{reducer}->{node}: result chunk {index} length differs from reducer-owned range")
                start, end = reducer_interval
                local_values = local[start:end]
                source_values: list[list[float]] = []
                complete = True
                for source, chunks_for_source in reducer_inputs.items():
                    source_chunk = next((item for item in chunks_for_source if _event_value(item, "part_index") == index), None)
                    parsed = _numeric_values(_event_value(source_chunk, "values")) if source_chunk else None
                    if parsed is None:
                        complete = False
                        break
                    source_values.append(parsed)
                origin_chunk = next((item for item in recipient_chunks if _event_value(item, "part_index") == index), None)
                original_values = _numeric_values(_event_value(origin_chunk, "values")) if origin_chunk else None
                if not complete or original_values is None:
                    issues.append(f"{reducer}->{node}: cannot recompute result {index} from complete native inputs")
                    continue
                if any(len(part) != len(local_values) for part in source_values) or len(local_values) != len(original_values) or len(values) != len(local_values):
                    issues.append(f"{reducer}->{node}: result {index} and accumulated input shapes differ")
                    continue
                divisor = len(reducer_roster)
                expected_delta = [
                    (local_values[offset] + sum(part[offset] for part in source_values)) / divisor - original_values[offset]
                    for offset in range(len(local_values))
                ]
                if any(not _close(actual, expected) for actual, expected in zip(values, expected_delta)):
                    issues.append(f"{reducer}->{node}: result {index} differs from native mean-minus-input correction")
                if _canonical_id(_event_value(chunk, "native_group_id")) != _canonical_id(_event_value(reducer_allocation, "native_group_id")):
                    issues.append(f"{reducer}->{node}: result chunk uses an unexpected native group ID")
            result_checked += len(chunks)

        for (reducer, sender), chunks in inputs.items():
            if reducer == node and sender not in roster:
                issues.append(f"unexpected input chunks from {sender}")
        for (reducer, recipient), chunks in results.items():
            if recipient == node and reducer not in roster:
                issues.append(f"unexpected result chunks from {reducer}")
        for (receiver, sender), state in stream_events.items():
            if receiver != node or sender in roster:
                continue
            rejected_stream_is_explained = (
                not inputs.get((receiver, sender))
                and len(state.get("stream_started", [])) == 1
                and len(state.get("stream_closed", [])) == 1
                and bool(state.get("stream_error") or state.get("stream_code"))
                and len({
                    str(_event_value(event, "stream_id"))
                    for events_for_kind in state.values()
                    for event in events_for_kind
                    if _event_value(event, "stream_id") is not None
                }) == 1
            )
            if not rejected_stream_is_explained:
                message = f"unexpected stream evidence from excluded sender {sender}"
                issues.append(message)
                evidence_issues.append(message)
        evidence_issues = [issue for issue in issues if issue not in expected_fallback_gaps]
        proof[node] = {
            "valid": not issues,
            "evidence_complete": not evidence_issues,
            "issues": issues,
            "evidence_issues": evidence_issues,
            "fallback_gaps": expected_fallback_gaps,
            "inbound_chunks_checked": input_checked,
            "result_chunks_checked": result_checked,
            "expected_members": roster,
            "input_coordinate_alignment": input_alignment,
            "result_coordinate_alignment": result_alignment,
            "stream_ids": sorted({
                str(_event_value(event, "stream_id")) for event in events
                if _event_node(event) == node and _event_value(event, "stream_id") is not None
            }),
        }
    return proof


def _processed_output_proof(
    events: list[dict[str, Any]],
    active: list[str],
    outputs: dict[str, Any],
    *,
    matrix_parser=_flatten_matrix,
) -> dict[str, Any]:
    """Prove that recorded native output deltas account for each saved result."""
    roster_by_node: dict[str, list[str]] = {}
    allocation_by_node: dict[str, dict[str, Any]] = {}
    chunks_by_node: dict[str, list[dict[str, Any]]] = {node: [] for node in active}
    returned: dict[tuple[str, str, int], dict[str, Any]] = {}
    inbound: dict[tuple[str, str], list[dict[str, Any]]] = {}
    fallback_events: dict[tuple[str, str], list[dict[str, Any]]] = {}
    sender_bans: dict[tuple[str, str], list[dict[str, Any]]] = {}
    finalized_by_node: dict[str, dict[str, Any]] = {}
    for event in events:
        node = _event_node(event)
        if node not in active:
            continue
        if _event_kind(event) == "local_roster":
            members = _members(_event_value(event, "members"))
            if members is not None:
                roster_by_node[node] = members
        elif _event_kind(event) == "allocation":
            allocation_by_node[node] = event
        elif _event_kind(event) == "processed_chunk":
            chunks_by_node[node].append(event)
        elif _event_kind(event) == "runner_finalized":
            finalized_by_node[node] = event
        elif _event_kind(event) == "sender_banned":
            sender = _event_value(event, "sender")
            if isinstance(sender, str):
                sender_bans.setdefault((node, sender), []).append(event)
        elif _event_kind(event) == "input_chunk":
            sender = _event_value(event, "sender")
            if isinstance(sender, str):
                inbound.setdefault((node, sender), []).append(event)
        if _event_kind(event) == "result_chunk":
            reducer = _event_node(event)
            recipient = _event_value(event, "receiver")
            part_index = _event_value(event, "part_index")
            if reducer is not None and recipient is not None and isinstance(part_index, int):
                returned[(str(recipient), str(reducer), part_index)] = event
        elif _event_kind(event) == "missing_part_fallback":
            owner = _event_node(event)
            reducer = _event_value(event, "reducer")
            if owner is not None and isinstance(reducer, str):
                fallback_events.setdefault((owner, reducer), []).append(event)

    proof: dict[str, Any] = {}
    for node in active:
        issues: list[str] = []
        record = outputs.get(node)
        initial = matrix_parser(record.get("initial")) if isinstance(record, dict) else None
        final = matrix_parser(record.get("final")) if isinstance(record, dict) else None
        status = str(record.get("status", "missing")) if isinstance(record, dict) else "missing"
        allocation = allocation_by_node.get(node)
        roster = roster_by_node.get(node, [])
        ranges_by_peer = _event_value(allocation, "part_ranges") if allocation else None
        parts_by_peer = _event_value(allocation, "parts") if allocation else None
        if initial is None or final is None:
            issues.append("saved initial/final tensors are unavailable")
        by_part: dict[tuple[str, int], dict[str, Any]] = {}
        arrival_order: dict[str, list[int]] = {}
        for event in chunks_by_node[node]:
            peer = _event_value(event, "reducer", _event_value(event, "peer"))
            part_index = _event_value(event, "part_index")
            if not isinstance(peer, str) or not isinstance(part_index, int) or isinstance(part_index, bool):
                issues.append("processed_chunk lacks a valid reducer/part_index")
                continue
            key = (peer, part_index)
            arrival_order.setdefault(peer, []).append(part_index)
            if key in by_part:
                issues.append(f"duplicate processed chunk for {peer} part {part_index}")
                continue
            by_part[key] = event
        expected_keys: set[tuple[str, int]] = set()
        flat_initial = [value for row in initial for value in row] if initial is not None else []
        flat_final = [value for row in final for value in row] if final is not None else []
        reconstructed = list(flat_initial)
        fallback_elements = 0
        unchanged_ranges: list[dict[str, Any]] = []
        fallback_thresholds: dict[str, int] = {}
        expected_failed_peers: set[str] = set()
        if not isinstance(ranges_by_peer, dict) or not isinstance(parts_by_peer, dict):
            issues.append("native allocation map is missing")
        else:
            for peer in roster:
                ranges = ranges_by_peer.get(peer)
                count = parts_by_peer.get(peer)
                if not isinstance(count, int) or isinstance(count, bool) or not isinstance(ranges, list) or len(ranges) != count:
                    issues.append(f"{peer}: allocation lacks part ranges/count for processed output")
                    continue
                peer_fallbacks = fallback_events.get((node, peer), [])
                if len(peer_fallbacks) > 1:
                    issues.append(f"{peer}: duplicate missing_part_fallback events")
                elif peer_fallbacks:
                    fallback_event = peer_fallbacks[0]
                    registered = _event_value(fallback_event, "registered_parts")
                    expected_group = _canonical_id(_event_value(allocation, "native_group_id")) if allocation else None
                    if (
                        not isinstance(registered, int)
                        or isinstance(registered, bool)
                        or registered < 0
                        or registered >= count
                    ):
                        issues.append(f"{peer}: fallback registered_parts is outside the failed range")
                    elif _canonical_id(_event_value(fallback_event, "native_group_id")) != expected_group:
                        issues.append(f"{peer}: fallback event has the wrong native group ID")
                    else:
                        fallback_thresholds[peer] = registered
                        expected_failed_peers.add(peer)
                elif peer == node and (node, peer) in fallback_events:
                    issues.append(f"{peer}: local reducer unexpectedly has a missing-part fallback")
                for part_index, interval in enumerate(ranges):
                    expected_keys.add((peer, part_index))
                    event = by_part.get((peer, part_index))
                    if event is None:
                        continue
                    if not isinstance(interval, list) or len(interval) != 2:
                        issues.append(f"{peer} part {part_index}: malformed allocation interval")
                        continue
                    start, end = interval
                    values = _numeric_values(_event_value(event, "values"))
                    if values is None or len(values) != end - start:
                        issues.append(f"{peer} part {part_index}: malformed processed delta values")
                        continue
                    if _canonical_id(_event_value(event, "native_group_id")) != _canonical_id(_event_value(allocation, "native_group_id")):
                        issues.append(f"{peer} part {part_index}: processed delta has the wrong group ID")
                    if initial is not None and final is not None:
                        expected_delta = [flat_final[offset] - flat_initial[offset] for offset in range(start, end)]
                        if any(not _close(actual, expected) for actual, expected in zip(values, expected_delta)):
                            issues.append(f"{peer} part {part_index}: processed delta differs from saved final minus initial")
                        for offset, value in enumerate(values, start):
                            reconstructed[offset] += value
                        if all(abs(value) <= ABS_TOL for value in values):
                            unchanged_ranges.append({"peer": peer, "part_index": part_index, "range": interval})
                    if peer == node:
                        if initial is None or not isinstance(ranges_by_peer, dict):
                            issues.append(f"{peer} part {part_index}: local shard inputs are unavailable")
                        else:
                            local_values = flat_initial[start:end]
                            remote_values: list[list[float]] = []
                            processed_time = _event_time(event)
                            for source in roster:
                                if source == node:
                                    continue
                                candidates = inbound.get((node, source), [])
                                input_event = next((
                                    item for item in candidates
                                    if _event_value(item, "part_index") == part_index
                                ), None)
                                parsed = _numeric_values(_event_value(input_event, "values")) if input_event else None
                                ban_events = sender_bans.get((node, source), [])
                                ban_event = ban_events[0] if len(ban_events) == 1 else None
                                ban_time = _event_time(ban_event) if ban_event else None
                                ban_group_matches = (
                                    ban_event is not None
                                    and _canonical_id(_event_value(ban_event, "native_group_id"))
                                    == _canonical_id(_event_value(allocation, "native_group_id"))
                                )
                                if parsed is None:
                                    if ban_time is None or processed_time is None or ban_time > processed_time or not ban_group_matches:
                                        issues.append(
                                            f"{peer} part {part_index}: missing {source} input has no matching timed ban before local reduction"
                                        )
                                    continue
                                input_time = _event_time(input_event)
                                if input_time is None or processed_time is None or input_time > processed_time:
                                    issues.append(f"{peer} part {part_index}: {source} input arrived after local reduction")
                                    continue
                                if ban_time is not None and ban_time <= input_time:
                                    issues.append(f"{peer} part {part_index}: {source} input arrived after sender was banned")
                                    continue
                                remote_values.append(parsed)
                            if any(len(part) != len(local_values) for part in remote_values) or len(values) != len(local_values):
                                issues.append(f"{peer} part {part_index}: local correction and accumulated input shapes differ")
                            else:
                                divisor = 1 + len(remote_values)
                                expected_local_delta = [
                                    (local_values[offset] + sum(part[offset] for part in remote_values)) / divisor
                                    - local_values[offset]
                                    for offset in range(len(local_values))
                                ]
                                if any(not _close(actual, expected) for actual, expected in zip(values, expected_local_delta)):
                                    issues.append(f"{peer} part {part_index}: applied local delta differs from native mean-minus-input correction")
                    else:
                        remote_result = returned.get((node, peer, part_index))
                        threshold = fallback_thresholds.get(peer)
                        if remote_result is not None:
                            returned_values = _numeric_values(_event_value(remote_result, "values"))
                            if threshold is not None and part_index >= threshold:
                                issues.append(f"{peer} part {part_index}: result arrived for a chunk recorded as fallback")
                            elif returned_values is None or values is None or len(returned_values) != len(values) or any(
                                not _close(actual, expected) for actual, expected in zip(values, returned_values)
                            ):
                                issues.append(f"{peer} part {part_index}: applied delta differs from returned native correction")
                        elif threshold is None or part_index < threshold:
                            issues.append(f"{peer} part {part_index}: processed delta has no matching returned native correction")
                        elif values is not None and any(abs(value) > ABS_TOL for value in values):
                            issues.append(f"{peer} part {part_index}: native fallback delta is not zero")
                        else:
                            fallback_elements += len(values or [])
                observed_order = arrival_order.get(peer, [])
                if observed_order != list(range(len(observed_order))):
                    issues.append(f"{peer}: processed chunk indices are missing, duplicated, or out of order")
            extras = set(by_part) - expected_keys
            if extras:
                issues.append(f"processed chunks reference unexpected roster parts: {sorted(extras)}")
        unexpected_fallback_peers = sorted(peer for owner, peer in fallback_events if owner == node and peer not in roster)
        if unexpected_fallback_peers:
            issues.append(f"fallback events reference peers outside the local roster: {unexpected_fallback_peers}")
        missing = expected_keys - set(by_part)
        if missing and status == "success":
            issues.append(f"successful output is missing processed chunks: {sorted(missing)}")
        reconstructs_saved_final = False
        if final is not None and initial is not None and not missing and len(reconstructed) == len(flat_final):
            reconstructs_saved_final = all(
                _close(actual, expected) for actual, expected in zip(reconstructed, flat_final)
            )
            if not reconstructs_saved_final:
                issues.append("reconstructed tensor from processed deltas differs from saved final tensor")
        finalized = finalized_by_node.get(node)
        if finalized is not None:
            actual_failed = _event_value(finalized, "failed_size")
            if (
                isinstance(actual_failed, bool)
                or not isinstance(actual_failed, (int, float))
                or not _close(float(actual_failed), float(fallback_elements), atol=0.0)
            ):
                issues.append(f"runner_finalized failed_size {actual_failed} differs from processed fallback elements {fallback_elements}")
            actual_failed_peers = _event_value(finalized, "failed_peer_ids")
            if not isinstance(actual_failed_peers, list) or {str(peer) for peer in actual_failed_peers} != expected_failed_peers:
                issues.append(
                    f"runner_finalized failed_peer_ids {actual_failed_peers} differs from recorded fallback peers "
                    f"{sorted(expected_failed_peers)}"
                )
        elif status == "success":
            issues.append("successful output has no runner_finalized event for fallback accounting")
        proof[node] = {
            "valid": not issues,
            "complete": not missing,
            "issues": issues,
            "processed_chunks": len(chunks_by_node[node]),
            "expected_chunks": len(expected_keys),
            "missing_chunks": [list(key) for key in sorted(missing)],
            "reconstructs_saved_final": reconstructs_saved_final,
            "fallback_elements": fallback_elements,
            "unchanged_ranges": unchanged_ranges,
        }
    return proof


def _stream_timeline(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Compact stream lifecycle summary; raw chunks stay in events_*.jsonl."""
    grouped: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for event in events:
        kind = _event_kind(event)
        if kind not in {"stream_started", "stream_closed", "stream_error", "stream_code", "input_chunk", "result_chunk"}:
            continue
        node = _event_node(event) or "?"
        sender = str(_event_value(event, "sender", "?"))
        receiver = str(_event_value(event, "receiver", "?"))
        stream_id = str(_event_value(event, "stream_id", f"{sender}->{receiver}"))
        record = grouped.setdefault((stream_id, node, sender, receiver), {
            "stream_id": stream_id, "node": node, "sender": sender, "receiver": receiver,
            "started_at": None, "ended_at": None, "input_chunks": 0, "result_chunks": 0,
            "errors": [], "codes": [], "native_group_id": _event_value(event, "native_group_id"),
        })
        timestamp = _event_time(event)
        if kind == "stream_started":
            record["started_at"] = timestamp
        elif kind in {"stream_closed", "stream_error", "stream_code"}:
            record["ended_at"] = timestamp
            if kind == "stream_error":
                record["errors"].append(_event_value(event, "error", "unknown stream error"))
            if kind == "stream_code":
                record["codes"].append(_event_value(event, "code"))
        elif kind == "input_chunk":
            record["input_chunks"] += 1
        elif kind == "result_chunk":
            record["result_chunks"] += 1
    result = list(grouped.values())
    result.sort(key=lambda item: (float("inf") if item["started_at"] is None else item["started_at"], item["node"], item["stream_id"]))
    return result


def _dispatcher_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep dispatcher events that may explain streams without a runner."""
    result = []
    for event in events:
        kind = _event_kind(event)
        if not kind.startswith("averager_rpc_"):
            continue
        result.append({
            "time": _event_time(event),
            "kind": kind,
            "node": _event_node(event),
            "sender": _event_value(event, "sender"),
            "receiver": _event_value(event, "receiver"),
            "remote_id": _event_value(event, "remote_id"),
            "group_id": _event_value(event, "native_group_id", _event_value(event, "group_id")),
            "stream_id": _event_value(event, "stream_id"),
            "code": _event_value(event, "code"),
            "code_name": _event_value(event, "code_name"),
            "status": _event_value(event, "status"),
            "error": _event_value(event, "error"),
        })
    result.sort(key=lambda item: (float("inf") if item["time"] is None else item["time"], item["node"] or ""))
    return result


def _timeline(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    relevant = {
        "leader_advertised",
        "step_started",
        "join_accepted",
        "unexpected_leader",
        "leader_assembled",
        "announcement_sent",
        "announcement_received",
        "local_roster",
        "allocation",
        "averager_rpc_started",
        "averager_rpc_code",
        "averager_rpc_closed",
        "stream_started",
        "stream_closed",
        "stream_error",
        "stream_code",
        "runner_finalized",
        "averaging_terminal",
    }
    timeline = []
    for event in events:
        kind = _event_kind(event)
        if kind not in relevant:
            continue
        timestamp = _event_time(event)
        timeline.append({
            "time": timestamp,
            "kind": kind,
            "node": _event_node(event),
            "recipient": _event_value(
                event,
                "recipient",
                _event_value(event, "remote_id", _event_value(event, "receiver")),
            ),
            "members": _event_value(event, "members"),
            "status": _event_value(event, "status"),
            "follower": _event_value(event, "follower"),
            "stream_id": _event_value(event, "stream_id"),
            "sender": _event_value(event, "sender"),
            "receiver": _event_value(event, "receiver"),
            "code": _event_value(event, "code"),
            "code_name": _event_value(event, "code_name"),
            "error": _event_value(event, "error"),
        })
    timeline.sort(key=lambda item: (float("inf") if item["time"] is None else item["time"], item["kind"], item["node"] or ""))
    return timeline


def analyze_condition(path: str | Path) -> dict[str, Any]:
    """Analyze one condition directory and write its analysis.json."""
    condition_path = Path(path)
    config_path = condition_path / "config.json"
    outputs_path = condition_path / "outputs.json"
    try:
        config = _load_json(config_path)
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read {config_path}: {exc}") from exc
    if not isinstance(config, dict):
        raise ValueError(f"{config_path} must contain an object")
    condition = _infer_condition(condition_path, config)
    try:
        outputs = _load_json(outputs_path)
    except (OSError, json.JSONDecodeError) as exc:
        outputs = {}
        output_load_error = f"cannot read outputs.json: {exc}"
    else:
        output_load_error = None
    if not isinstance(outputs, dict):
        outputs = {}
        output_load_error = output_load_error or "outputs.json must contain an object"
    events, event_errors = _read_events(condition_path)
    shutdown_ok, shutdown_error = _shutdown_check(condition_path)
    identity_check, identity_errors = _identity_check(condition_path)
    provenance_check, provenance_errors = _source_provenance_check(condition_path)
    evidence = _evidence_checks(condition, events, outputs, shutdown_ok)
    evidence["errors"].extend(identity_errors)
    evidence["errors"].extend(provenance_errors)
    evidence["identity_check"] = identity_check
    evidence["source_provenance_check"] = provenance_check
    if identity_errors or provenance_errors:
        evidence["valid"] = False
    if output_load_error:
        evidence["errors"].append(output_load_error)
        evidence["valid"] = False
    evidence["errors"].extend(event_errors)
    if event_errors:
        evidence["valid"] = False
    if shutdown_error:
        evidence["shutdown_error"] = shutdown_error
    output_results = _output_analysis(outputs, events)
    active = _active_nodes(condition)
    stream_proof = _stream_proof(events, active, outputs)
    processed_proof = _processed_output_proof(events, active, outputs)
    for node in active:
        output_results[node]["stream_proof_complete"] = stream_proof[node]["valid"]
        output_results[node]["stream_proof_issues"] = stream_proof[node]["issues"]
        output_results[node]["stream_evidence_complete"] = stream_proof[node]["evidence_complete"]
        output_results[node]["stream_evidence_issues"] = stream_proof[node]["evidence_issues"]
        output_results[node]["processed_output_complete"] = processed_proof[node]["complete"]
        output_results[node]["processed_output_valid"] = processed_proof[node]["valid"]
        output_results[node]["processed_output_issues"] = processed_proof[node]["issues"]
        output_results[node]["A_cleanly_excluded_from_completed_BDC_mean"] = bool(
            output_results[node]["A_cleanly_excluded_from_completed_BDC_mean"]
            and stream_proof[node]["valid"]
            and processed_proof[node]["valid"]
            and processed_proof[node]["complete"]
        )
        if outputs.get(node, {}).get("status") == "success":
            if not stream_proof[node]["evidence_complete"]:
                evidence["errors"].append(f"{node}: successful output lacks complete native stream proof or supported fallback evidence")
            if not processed_proof[node]["valid"] or not processed_proof[node]["complete"]:
                evidence["errors"].append(f"{node}: successful output lacks complete processed-delta proof")
    if condition in ("baseline4", "control3"):
        for node in active:
            if not stream_proof[node]["valid"]:
                evidence["errors"].append(f"{node}: baseline/control stream proof is incomplete")
            if not processed_proof[node]["valid"] or not processed_proof[node]["complete"]:
                evidence["errors"].append(f"{node}: baseline/control processed-delta proof is incomplete")
            if not output_results[node].get("matches_expected_control", False):
                evidence["errors"].append(f"{node}: control output differs from independently computed mean")
        if any(output_results[node].get("fallback_indicated") for node in active):
            evidence["errors"].append("baseline/control has fallback evidence")
        evidence["valid"] = not evidence["errors"]
    agreement = _participant_agreement(outputs, active)
    for node in active:
        output_results[node]["agrees_with"] = agreement["by_node"].get(node, {}).get("agrees_with", [])
        output_results[node]["max_abs_difference_by_participant"] = agreement["by_node"].get(node, {}).get("max_abs_difference", {})
    evidence["valid"] = not evidence["errors"]
    local_means = {
        node: {
            "members": output_results[node].get("local_roster_members"),
            "matches": output_results[node].get("matches_local_roster_mean", False),
            "max_abs_error": output_results[node].get("local_roster_mean_max_abs_error"),
        }
        for node in active
    }
    summary = {
        "condition": condition,
        "path": str(condition_path),
        "valid_evidence": evidence["valid"],
        "evidence": evidence,
        "control_mean_check": {
            "members_by_node": {node: data["members"] for node, data in local_means.items()},
            "absolute_tolerance": ABS_TOL,
            "relative_tolerance": REL_TOL,
            "expected_tensor_shape": [ROWS, LANES],
            "mean_by_node": local_means,
            "active_outputs_match": all(output_results[node].get("matches_expected_control", False) for node in active),
        },
        "outputs": output_results,
        "stream_proof": stream_proof,
        "processed_output_proof": processed_proof,
        "participant_agreement": agreement,
        "timeline": _timeline(events),
        "stream_timeline": _stream_timeline(events),
        "dispatcher_events": _dispatcher_events(events),
        "interpretation": _interpretation(condition, output_results, active, evidence),
    }
    (condition_path / "analysis.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return summary


def _interpretation(
    condition: str, outputs: dict[str, Any], active: list[str], evidence: dict[str, Any]
) -> dict[str, Any]:
    clean_exclusions = [node for node in active if outputs.get(node, {}).get("A_cleanly_excluded_from_completed_BDC_mean")]
    coordinate_mixing = [
        node for node in active
        if outputs.get(node, {}).get("shape_valid") and not outputs[node].get("coordinate_markers_correct")
    ]
    fallback_nodes = [node for node in active if outputs.get(node, {}).get("fallback_indicated")]
    bdc_nodes = [node for node in ("B", "D", "C") if node in active]
    consistent_membership_exclusion = len(bdc_nodes) == 3 and all(
        outputs.get(node, {}).get("A_cleanly_excluded_from_completed_BDC_mean") for node in bdc_nodes
    )
    assembled = evidence.get("leader_assembled_roster")
    local_rosters = evidence.get("local_aggregation_rosters", {})
    payloads = evidence.get("announcement_payload_checks", {})
    announced_to_a = payloads.get("A", {}).get("members") if isinstance(payloads, dict) else None
    membership_inconsistency_observed = bool(
        isinstance(assembled, list)
        and "A" in assembled
        and isinstance(local_rosters.get("B"), list)
        and "A" not in local_rosters["B"]
        and isinstance(announced_to_a, list)
        and "A" in announced_to_a
    )
    return {
        "condition": condition,
        "nodes_with_clean_A_exclusion": clean_exclusions,
        "nodes_with_coordinate_marker_mismatch": coordinate_mixing,
        "nodes_with_fallback_evidence": fallback_nodes,
        "membership_inconsistency_observed": membership_inconsistency_observed,
        "clean_exclusion_consistent_across_BDC": consistent_membership_exclusion,
        "attack_hypothesis_confirmed": membership_inconsistency_observed and consistent_membership_exclusion,
        "note": "Clean exclusion requires complete native stream and applied-delta evidence, correct output provenance and coordinate markers, no fallback, and a completed B/D/C mean.",
    }


def _condition_rows(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for result in results:
        active = result["evidence"]["active_nodes"]
        outputs = result["outputs"]
        announcements = result["evidence"].get("announcement_payload_checks", {})
        local_rosters = result["evidence"].get("local_aggregation_rosters", {})
        roster_cells = {}
        for node in ("D", "A", "C"):
            sent = announcements.get(node, {}).get("members")
            local = local_rosters.get(node)
            if sent is None and local is None:
                roster_cells[node] = "no request / idle"
            else:
                sent_text = ",".join(sent) if sent else "—"
                local_text = ",".join(local) if local else "—"
                roster_cells[node] = f"announce [{sent_text}] / aggregate [{local_text}]"
        rows.append({
            "condition": result["condition"],
            "valid": result["valid_evidence"],
            "active_nodes": ", ".join(active),
            "controls_match": result["control_mean_check"]["active_outputs_match"],
            "streams_complete": all(result["stream_proof"].get(node, {}).get("valid", False) for node in active),
            "A_excluded": ", ".join(result["interpretation"]["nodes_with_clean_A_exclusion"]) or "—",
            "coordinate_mismatch": ", ".join(result["interpretation"]["nodes_with_coordinate_marker_mismatch"]) or "—",
            "fallback": ", ".join(result["interpretation"]["nodes_with_fallback_evidence"]) or "—",
            "evidence_errors": len(result["evidence"]["errors"]),
            "assembled_roster": ",".join(result["evidence"].get("leader_assembled_roster") or []) or "—",
            "B_local_roster": ",".join(local_rosters.get("B") or []) or "—",
            "D_roster_view": roster_cells["D"],
            "A_roster_view": roster_cells["A"],
            "C_roster_view": roster_cells["C"],
        })
    return rows


def _markdown_report(results: list[dict[str, Any]]) -> str:
    rows = _condition_rows(results)
    lines = [
        "# Hivemind inconsistent-announcement probe",
        "",
        "This report analyzes local native Hivemind runs. It distinguishes a completed, numerically supported exclusion from an error, missing result, or local fallback.",
        "",
        "## Matchmaking and aggregation rosters",
        "",
        "B's assembled view is shown separately from its local aggregation view because the membership condition changes B's roster after assembly.",
        "",
        "| Condition | B assembled | B aggregate | D announced / aggregate | A announced / aggregate | C announced / aggregate |",
        "|---|---|---|---|---|---|",
    ]
    for row in rows:
        lines.append(f"| {row['condition']} | {row['assembled_roster']} | {row['B_local_roster']} | {row['D_roster_view']} | {row['A_roster_view']} | {row['C_roster_view']} |")
    lines.extend([
        "",
        "| Condition | Evidence valid | Active nodes | Local-roster means match | Streams complete | A cleanly excluded | Coordinate mismatch | Fallback evidence | Evidence errors |",
        "|---|---:|---|---:|---:|---|---|---|---:|",
    ])
    for row in rows:
        lines.append(
            f"| {row['condition']} | {row['valid']} | {row['active_nodes']} | {row['controls_match']} | {row['streams_complete']} | {row['A_excluded']} | {row['coordinate_mismatch']} | {row['fallback']} | {row['evidence_errors']} |"
        )
    for result in results:
        lines.extend(["", f"## {result['condition']}", ""])
        if result["evidence"]["errors"]:
            lines.append("Evidence gaps:")
            lines.extend(f"- {error}" for error in result["evidence"]["errors"])
            lines.append("")
        lines.append("| Node | Operation status | Local-roster mean | Stream proof | Applied deltas | Origins observed | Row coverage | Coordinates correct | Fallback | A cleanly excluded | Announcement/aggregation to terminal (s) | Agrees with | Max mean error |")
        lines.append("|---|---|---:|---:|---:|---|---|---:|---:|---:|---:|---|---:|")
        for node in NODES:
            output = result["outputs"][node]
            error = output.get("control_mean_max_abs_error")
            error_text = "—" if error is None else f"{error:.6g}"
            elapsed = output.get("announcement_to_terminal_seconds")
            elapsed_text = "—" if elapsed is None else f"{elapsed:.3f}"
            coverage = ", ".join(f"{origin}:{count}" for origin, count in output.get("origin_row_coverage", {}).items()) or "—"
            lines.append(
                f"| {node} | {output.get('operation_status', output['status'])} | {output.get('matches_expected_control', output.get('matches_control_mean', False))} | "
                f"{output.get('stream_proof_complete', False)} | {output.get('processed_output_complete', False)} | {', '.join(output.get('origin_nodes_observed') or []) or '—'} | {coverage} | {output.get('coordinate_markers_correct', False)} | "
                f"{output.get('fallback_indicated', False)} | {output.get('A_cleanly_excluded_from_completed_BDC_mean', False)} | {elapsed_text} | {', '.join(output.get('agrees_with') or []) or '—'} | {error_text} |"
            )
        lines.extend(["", "Timeline (seconds are the recorded event times):", ""])
        report_timeline_kinds = {"leader_advertised", "step_started", "join_accepted", "leader_assembled", "announcement_sent", "announcement_received", "local_roster", "averager_rpc_started", "averager_rpc_code", "averager_rpc_closed", "stream_error", "stream_code", "averaging_terminal"}
        for item in result["timeline"]:
            if item["kind"] not in report_timeline_kinds:
                continue
            time_text = "?" if item["time"] is None else f"{item['time']:.6f}"
            has_direction = item.get("sender") and item.get("receiver") and (
                item["kind"].startswith("stream_") or item["kind"].startswith("averager_rpc_")
            )
            actor = str(item["sender"]) if has_direction else item["node"] or "?"
            recipient = f" → {item['receiver']}" if has_direction else (f" → {item['recipient']}" if item.get("recipient") else "")
            if item.get("follower") and not has_direction:
                recipient = f" follower={item['follower']}"
            detail = f" members={item['members']}" if item.get("members") is not None else ""
            status = f" status={item['status']}" if item.get("status") is not None else ""
            code_value = item.get("code_name") or item.get("code")
            code = f" code={code_value}" if code_value is not None else ""
            error = f" error={item['error']}" if item.get("error") else ""
            lines.append(f"- `{time_text}s` **{item['kind']}** {actor}{recipient}{detail}{status}{code}{error}")
    lines.extend([
        "",
        "## Interpretation limits",
        "",
        "The report covers the exact local process runs and tensor values saved in these artifacts. It does not estimate attack probability, Internet performance, or model-convergence impact. A failed or partial aggregation is not evidence that a participant was cleanly excluded.",
        "",
    ])
    return "\n".join(lines)


def _html_dashboard(results: list[dict[str, Any]]) -> str:
    rows = _condition_rows(results)
    body_rows = []
    for row in rows:
        body_rows.append(
            "<tr>" + "".join(f"<td>{html.escape(str(row[key]))}</td>" for key in (
        "condition", "valid", "active_nodes", "controls_match", "streams_complete", "A_excluded", "coordinate_mismatch", "fallback", "evidence_errors"
            )) + "</tr>"
        )
    roster_rows = []
    for row in rows:
        roster_rows.append(
            "<tr>" + "".join(f"<td>{html.escape(str(row[key]))}</td>" for key in (
                "condition", "assembled_roster", "B_local_roster", "D_roster_view", "A_roster_view", "C_roster_view"
            )) + "</tr>"
        )
    details = []
    for result in results:
        nodes = []
        for node in NODES:
            output = result["outputs"][node]
            nodes.append(
                "<tr>" + "".join(f"<td>{html.escape(str(value))}</td>" for value in (
                    node,
                    output.get("operation_status", output.get("status", "missing")),
                    output.get("matches_expected_control", output.get("matches_control_mean", False)),
                    output.get("stream_proof_complete", False),
                    output.get("processed_output_complete", False),
                    ", ".join(output.get("origin_nodes_observed") or []) or "—",
                    output.get("origin_row_coverage", {}),
                    output.get("coordinate_markers_correct", False),
                    output.get("fallback_indicated", False),
                    output.get("A_cleanly_excluded_from_completed_BDC_mean", False),
                    output.get("announcement_to_terminal_seconds"),
                    output.get("agrees_with", []),
                )) + "</tr>"
            )
        timeline = []
        report_timeline_kinds = {"leader_advertised", "step_started", "join_accepted", "leader_assembled", "announcement_sent", "announcement_received", "local_roster", "averager_rpc_started", "averager_rpc_code", "averager_rpc_closed", "stream_error", "stream_code", "averaging_terminal"}
        for event in result["timeline"]:
            if event["kind"] not in report_timeline_kinds:
                continue
            when = "?" if event["time"] is None else f"{event['time']:.3f}s"
            has_direction = event.get("sender") and event.get("receiver") and (
                event["kind"].startswith("stream_") or event["kind"].startswith("averager_rpc_")
            )
            actor = str(event["sender"]) if has_direction else event["node"] or "?"
            target = event.get("receiver") if has_direction else (event.get("recipient") or event.get("follower") or "")
            route = f"{html.escape(str(actor))} → {html.escape(str(target))}" if has_direction else (
                f"{html.escape(str(actor))} {html.escape(str(target))}"
            )
            members = event.get("members") or ""
            state = event.get("status") or ""
            code = event.get("code_name") or event.get("code")
            error = event.get("error")
            native_result = f" code={code}" if code is not None else ""
            native_error = f" {error}" if error else ""
            timeline.append(f"<li><code>{html.escape(when)}</code> <b>{html.escape(event['kind'])}</b> {route} {html.escape(str(members))} {html.escape(str(state))}{html.escape(native_result)}{html.escape(native_error)}</li>")
        gaps = "".join(f"<li>{html.escape(error)}</li>" for error in result["evidence"]["errors"])
        details.append(
            f"<section><h2>{html.escape(result['condition'])}</h2>"
        f"<p>Evidence valid: <b>{result['valid_evidence']}</b>. Active outputs matching the arithmetic mean of their saved local-roster inputs: <b>{result['control_mean_check']['active_outputs_match']}</b>.</p>"
            f"<table><thead><tr><th>Node</th><th>Status</th><th>Local-roster mean</th><th>Stream proof</th><th>Applied deltas</th><th>Origins observed</th><th>Origin row coverage</th><th>Coordinates correct</th><th>Fallback</th><th>A cleanly excluded</th><th>Announcement/aggregation to terminal (s)</th><th>Agrees with</th></tr></thead><tbody>{''.join(nodes)}</tbody></table>"
            f"<h3>Evidence gaps</h3><ul>{gaps or '<li>None detected</li>'}</ul>"
            f"<h3>Coordination timeline</h3><ol>{''.join(timeline)}</ol></section>"
        )
    document = """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Hivemind announcement probe</title><style>
body{font:15px system-ui,sans-serif;max-width:1200px;margin:2rem auto;padding:0 1rem;color:#18202b;background:#f5f7fa}
h1,h2{color:#173b63}section,header{background:white;border:1px solid #d7dee8;border-radius:8px;padding:1rem;margin:1rem 0;box-shadow:0 2px 6px #13243a10}
table{border-collapse:collapse;width:100%;font-size:.9rem;margin:.6rem 0 1rem}th,td{border:1px solid #d7dee8;padding:.42rem;text-align:left;vertical-align:top}th{background:#eaf0f7}
ol{max-height:24rem;overflow:auto;background:#f8fafc;padding:1rem 1rem 1rem 2.5rem}li{margin:.2rem 0}code{color:#4a3570}.note{color:#4b5868}
</style></head><body><header><h1>Hivemind inconsistent-announcement probe</h1>
<p>Local native process experiment. Clean exclusion requires a completed result whose origin markers and arithmetic support the remaining contributors; an error or fallback is not a successful exclusion.</p>
<table><thead><tr><th>Condition</th><th>B assembled</th><th>B aggregation</th><th>D announcement / aggregation</th><th>A announcement / aggregation</th><th>C announcement / aggregation</th></tr></thead>
<tbody>""" + "".join(roster_rows) + "</tbody></table>\n"
    document += "<table><thead><tr><th>Condition</th><th>Evidence valid</th><th>Active nodes</th><th>Local-roster means match</th><th>Streams complete</th><th>A cleanly excluded</th><th>Coordinate mismatch</th><th>Fallback</th><th>Evidence gaps</th></tr></thead>\n"
    document += "<tbody>" + "".join(body_rows) + "</tbody></table></header>" + "".join(details)
    document += "<p class='note'>Results apply only to these recorded local runs. They do not estimate public-network attack rates or training convergence.</p></body></html>\n"
    return document


def analyze_suite(path: str | Path) -> dict[str, Any]:
    """Analyze every condition directory and write suite reports."""
    suite = Path(path)
    condition_dirs: list[Path]
    if (suite / "config.json").is_file():
        condition_dirs = [suite]
    else:
        condition_dirs = sorted(
            child for child in suite.iterdir()
            if child.is_dir() and (child / "config.json").is_file()
        ) if suite.is_dir() else []
    results = [analyze_condition(condition_path) for condition_path in condition_dirs]
    by_name = {result["condition"]: result for result in results}
    missing_conditions = [name for name in CONDITIONS if name not in by_name]
    suite_evidence_errors: list[str] = []
    seen_peer_ids: dict[str, str] = {}
    seen_group_ids: dict[str, str] = {}
    seen_prefixes: dict[str, str] = {}
    for result in results:
        condition = result["condition"]
        peer_ids = result["evidence"].get("identity_check", {}).get("peer_ids", {})
        for node, peer_id in peer_ids.items():
            if peer_id in seen_peer_ids:
                suite_evidence_errors.append(
                    f"{condition}/{node} reuses peer identity from {seen_peer_ids[peer_id]}"
                )
            else:
                seen_peer_ids[peer_id] = f"{condition}/{node}"
        for group_id in result["evidence"].get("native_group_ids", []):
            if group_id in seen_group_ids:
                suite_evidence_errors.append(
                    f"{condition} reuses native group ID from {seen_group_ids[group_id]}"
                )
            else:
                seen_group_ids[group_id] = condition
        try:
            config = _load_json(Path(result["path"]) / "config.json")
        except (OSError, json.JSONDecodeError):
            config = {}
        prefix = config.get("prefix") if isinstance(config, dict) else None
        if not isinstance(prefix, str) or not prefix:
            suite_evidence_errors.append(f"{condition} config has no private prefix")
        elif prefix in seen_prefixes:
            suite_evidence_errors.append(
                f"{condition} reuses private prefix from {seen_prefixes[prefix]}"
            )
        else:
            seen_prefixes[prefix] = condition
    run_status = None
    run_status_error = None
    status_path = suite / "status.json"
    if status_path.is_file():
        try:
            run_status = _load_json(status_path)
        except (OSError, json.JSONDecodeError) as exc:
            run_status_error = f"status.json is unreadable: {exc}"
        else:
            if not isinstance(run_status, dict) or run_status.get("status") != "completed":
                run_status_error = f"suite status is not completed: {run_status!r}"
    suite_run_completed = run_status is None or run_status_error is None
    suite_summary = {
        "suite": str(suite),
        "run_status": run_status,
        "run_status_error": run_status_error,
        "conditions": [result["condition"] for result in results],
        "missing_conditions": missing_conditions,
        "suite_evidence_errors": suite_evidence_errors,
        "all_evidence_valid": suite_run_completed and bool(results) and not missing_conditions and not suite_evidence_errors and all(result["valid_evidence"] for result in results),
        "results": results,
    }
    suite.mkdir(parents=True, exist_ok=True)
    (suite / "analysis.json").write_text(json.dumps(suite_summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown = _markdown_report(results)
    if suite_evidence_errors:
        markdown = "**Suite evidence errors:** " + "; ".join(suite_evidence_errors) + "\n\n" + markdown
    if run_status_error:
        markdown = f"**Suite run status:** {run_status_error}\n\n" + markdown
    dashboard = _html_dashboard(results)
    if suite_evidence_errors:
        dashboard = dashboard.replace(
            "<h1>Hivemind inconsistent-announcement probe</h1>",
            "<h1>Hivemind inconsistent-announcement probe</h1><p><b>Suite evidence errors:</b> " +
            html.escape("; ".join(suite_evidence_errors)) + "</p>",
            1,
        )
    if run_status_error:
        dashboard = dashboard.replace(
            "<h1>Hivemind inconsistent-announcement probe</h1>",
            f"<h1>Hivemind inconsistent-announcement probe</h1><p><b>Suite run status:</b> {html.escape(run_status_error)}</p>",
            1,
        )
    (suite / "REPORT.md").write_text(markdown, encoding="utf-8")
    (suite / "DASHBOARD.html").write_text(dashboard, encoding="utf-8")
    return suite_summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", type=Path, help="suite directory, or a single condition directory")
    args = parser.parse_args()
    summary = analyze_suite(args.path)
    print(json.dumps({
        "suite": summary["suite"],
        "conditions": summary["conditions"],
        "missing_conditions": summary["missing_conditions"],
        "all_evidence_valid": summary["all_evidence_valid"],
        "report": str(args.path / "REPORT.md") if (args.path / "config.json").is_file() else str(args.path / "REPORT.md"),
    }, indent=2))
    return 0 if summary["all_evidence_valid"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
