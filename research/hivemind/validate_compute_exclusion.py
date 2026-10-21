"""Replay native compute-exclusion averaging groups with the simulator backend.

This validator covers numerical aggregation and native partition assignment.
It does not model network timing, optimizer progress, or gate eligibility. A
partial native group is reported as unsupported instead of being omitted.
"""
from __future__ import annotations

import json
import math
from pathlib import Path
import sys
from typing import Any, Mapping


HERE = Path(__file__).resolve().parent
SIMULATOR_SRC = HERE.parent / "simulator" / "src"
ATOL = 1e-5
RTOL = 1e-4

_NUMERICAL_EVENTS = {
    "aggregate_input", "allocation", "aggregate_output", "fallback",
    "processed_chunk", "terminal", "finalize", "input_chunk",
    "result_chunk", "stream_terminal", "sender_banned",
}


def _read_events(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    failures: list[str] = []
    if path.is_file():
        sources = [path]
    elif path.is_dir():
        sources = sorted(path.rglob("events_*.jsonl"))
    else:
        return [], [f"case path does not exist: {path}"]
    if not sources:
        return [], [f"no events_*.jsonl files under {path}"]

    events = []
    for source in sources:
        try:
            with source.open() as stream:
                for line_no, line in enumerate(stream, 1):
                    try:
                        row = json.loads(line)
                    except json.JSONDecodeError as exc:
                        failures.append(f"{source}:{line_no}: invalid JSON: {exc}")
                        continue
                    if not isinstance(row, dict):
                        failures.append(f"{source}:{line_no}: event is not an object")
                        continue
                    row["_source"] = source.name
                    row["_line"] = line_no
                    events.append(row)
        except OSError as exc:
            failures.append(f"cannot read {source}: {exc}")
    return events, failures


def _number(value: Any) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    try:
        return math.isfinite(float(value))
    except OverflowError:
        return False


def _vector(value: Any):
    """Decode the hook's one-or-more flattened tensor lists as float32."""
    import numpy as np

    def valid_tree(item: Any) -> bool:
        if isinstance(item, (list, tuple)):
            return all(valid_tree(child) for child in item)
        return _number(item)

    if not isinstance(value, (list, tuple)) or not valid_tree(value):
        return None
    try:
        result = np.asarray(value, dtype=np.float32).reshape(-1)
    except (TypeError, ValueError, OverflowError):
        return None
    if not result.size or not np.isfinite(result).all():
        return None
    return result


def _same(left: Any, right: Any) -> bool:
    import numpy as np

    try:
        return bool(np.allclose(left, right, atol=ATOL, rtol=RTOL, equal_nan=False))
    except (TypeError, ValueError):
        return False


def _raw_vector_size(value: Any) -> int | None:
    """Count saved tensor elements while permitting the hook's string NaNs."""
    if isinstance(value, (list, tuple)):
        sizes = [_raw_vector_size(item) for item in value]
        return None if any(size is None for size in sizes) else sum(sizes)
    if isinstance(value, Mapping) or isinstance(value, bool) or value is None:
        return None
    if isinstance(value, str):
        return 1 if value.lower() in {"nan", "inf", "+inf", "-inf"} else None
    return 1 if isinstance(value, (int, float)) else None


def _load_aggregate():
    if str(SIMULATOR_SRC) not in sys.path:
        sys.path.insert(0, str(SIMULATOR_SRC))
    from trainsim.protocols.hivemind.numerical import aggregate

    return aggregate


def _group_label(key: tuple[str, str]) -> str:
    return f"prefix={key[0]!r}, group_id={key[1]!r}"


def _shutdown_only_evidence(key: tuple[str, str], rows: list[dict[str, Any]],
                            all_events: list[dict[str, Any]]):
    """Recognize optimizer-shutdown groups that cannot affect training."""
    if not rows or any(row.get("channel") != "gradient" for row in rows):
        return None
    by_node: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for row in rows:
        if (row.get("node") is None or row.get("worker") != row.get("node") or
                row.get("traceprefix") != row.get("prefix") or
                (row.get("peer") is not None and row.get("peer") != row.get("worker"))):
            return None
        node = _node(row)
        if node is None:
            return None
        by_node.setdefault(node, {}).setdefault(row.get("kind", ""), []).append(row)
    participants = set(by_node)
    expected: set[str] = set()
    zero_weights = {}
    terminal_times = {}
    inputs = {}
    allocations = {}
    for node, kinds in by_node.items():
        if any(len(kinds.get(kind, [])) != 1 for kind in
               ("aggregate_input", "allocation", "aggregate_output", "terminal", "finalize")):
            return None
        input_row = kinds["aggregate_input"][0]
        allocation = kinds["allocation"][0]
        output_row = kinds["aggregate_output"][0]
        roster = input_row.get("members")
        if (not isinstance(roster, list) or any(not isinstance(member, str) for member in roster) or
                node not in roster or len(set(roster)) != len(roster) or
                allocation.get("members") != roster):
            return None
        expected.update(roster)
        input_vector = _vector(input_row.get("values"))
        if input_vector is None:
            return None
        inputs[node] = input_vector
        if _raw_vector_size(output_row.get("values")) != input_vector.size:
            return None
        weight = allocation.get("weight")
        if not _number(weight) or float(weight) != 0.0:
            return None
        zero_weights[node] = 0.0
        allocation_errors: list[str] = []
        ranges = _allocation_ranges(allocation, int(input_vector.size), allocation_errors,
                                    f"{_group_label(key)}, node={node}")
        if ranges is None or allocation_errors:
            return None
        allocations[node] = ranges
        native_terminal = kinds["terminal"][0]
        if native_terminal.get("outcome") != "complete":
            return None
        training_terminals = [event for event in all_events
                              if event.get("kind") == "training_terminal" and event.get("node") == node]
        if len(training_terminals) != 1:
            return None
        training_terminal_time = training_terminals[0].get("monotonic")
        if not _number(training_terminal_time):
            return None
        terminal_times[node] = float(training_terminal_time)
        input_time, allocation_time = input_row.get("monotonic"), allocation.get("monotonic")
        if (not _number(input_time) or not _number(allocation_time) or
                float(training_terminal_time) >= min(float(input_time), float(allocation_time))):
            return None
        final = kinds["finalize"][0]
        failed_size, total_elements = final.get("failed_size"), final.get("total_elements")
        if (not isinstance(failed_size, int) or isinstance(failed_size, bool) or
                not isinstance(total_elements, int) or isinstance(total_elements, bool) or
                total_elements != input_vector.size or not 0 <= failed_size <= total_elements):
            return None
    if expected != participants:
        return None

    for node, kinds in by_node.items():
        expected_chunks = {
            (node, reducer, part_index)
            for reducer, part_ranges in allocations[node].items()
            for part_index in range(len(part_ranges))
        }
        observed_chunks = set()
        for row in kinds.get("processed_chunk", []):
            reducer, part_index = row.get("reducer"), row.get("part_index")
            if (not isinstance(reducer, str) or not isinstance(part_index, int) or
                    isinstance(part_index, bool) or part_index < 0):
                return None
            identity = (node, reducer, part_index)
            if identity in observed_chunks or identity not in expected_chunks:
                return None
            observed_chunks.add(identity)
        if observed_chunks != expected_chunks:
            return None
        for row in kinds.get("fallback", []):
            reducer = row.get("reducer")
            part_ranges = allocations[node].get(reducer)
            first, count = row.get("registered_parts"), row.get("part_count")
            if (part_ranges is None or not isinstance(first, int) or isinstance(first, bool) or
                    not isinstance(count, int) or isinstance(count, bool) or
                    first < 0 or count != len(part_ranges) or first > count):
                return None

    for node, terminal_time in terminal_times.items():
        for event in all_events:
            if event.get("node") != node:
                continue
            if event.get("kind") not in {"gradient_loaded", "update_begin", "update_end"}:
                continue
            event_time = event.get("monotonic")
            if not _number(event_time) or float(event_time) >= terminal_time:
                return None

    return {
        "prefix": key[0], "group_id": key[1], "participants": sorted(participants),
        "status": "excluded_shutdown", "classification": "shutdown_only_zero_weight",
        "weights": zero_weights,
        "checks": {"native_allocation_nodes_checked": len(participants),
                   "training_shutdown_proven": True,
                   "simulator_allocation_checked": False},
        "reason": "all weights are zero; every participant's training_terminal preceded its group, "
                  "with no later gradient load or optimizer update",
    }


def _node(row: Mapping[str, Any]) -> str | None:
    # Hook events use `peer`; the event writer uses `node` for the process.
    peer = row.get("peer")
    if isinstance(peer, str) and peer:
        return peer
    worker = row.get("worker")
    return worker if isinstance(worker, str) and worker else None


def _allocation_ranges(row: Mapping[str, Any], size: int, failures: list[str], label: str):
    members = row.get("members")
    ranges = row.get("part_ranges")
    if (not isinstance(members, list) or not members or
            any(not isinstance(member, str) or not member for member in members) or
            len(set(members)) != len(members)):
        failures.append(f"{label}: allocation has an invalid member list")
        return None
    if not isinstance(ranges, Mapping) or set(ranges) != set(members):
        failures.append(f"{label}: allocation part_ranges keys do not match members")
        return None

    decoded: dict[str, list[tuple[int, int]]] = {}
    coverage = [0] * size
    for member in members:
        rows = ranges.get(member)
        if not isinstance(rows, list):
            failures.append(f"{label}: allocation ranges for {member} are not a list")
            return None
        member_ranges = []
        for part in rows:
            if not isinstance(part, Mapping):
                failures.append(f"{label}: allocation range for {member} is not an object")
                return None
            start, end = part.get("start"), part.get("end")
            if (not isinstance(start, int) or isinstance(start, bool) or
                    not isinstance(end, int) or isinstance(end, bool) or
                    part.get("tensor_index") != 0 or start < 0 or end <= start or end > size or
                    part.get("elements") != end - start):
                failures.append(
                    f"{label}: allocation for {member} uses malformed or unsupported coordinates"
                )
                return None
            member_ranges.append((start, end))
            for coordinate in range(start, end):
                coverage[coordinate] += 1
        if member_ranges != sorted(member_ranges):
            failures.append(f"{label}: allocation ranges for {member} are out of coordinate order")
        decoded[member] = member_ranges

    if any(count != 1 for count in coverage):
        failures.append(f"{label}: native allocation does not cover each coordinate exactly once")
        return None
    parts = row.get("parts")
    if (not isinstance(parts, list) or len(parts) != len(members) or
            any(not isinstance(count, int) or isinstance(count, bool) for count in parts) or
            parts != [len(decoded[member]) for member in members]):
        failures.append(f"{label}: allocation part counts disagree with part_ranges")
    if row.get("total_elements") != size:
        failures.append(f"{label}: native total_elements disagrees with captured input length")
    return decoded


def _capture_group(key: tuple[str, str], rows: list[dict[str, Any]], failures: list[str]):
    label = _group_label(key)
    group_failures: list[str] = []

    def fail(message: str) -> None:
        text = f"{label}: {message}"
        failures.append(text)
        group_failures.append(text)

    channels = {row.get("channel") for row in rows}
    if len(channels) != 1 or not channels.issubset({"gradient", "state"}):
        fail(f"group has missing or inconsistent native channels: {sorted(map(str, channels))}")

    by_node: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for row in rows:
        if row.get("kind") not in _NUMERICAL_EVENTS:
            continue
        node = _node(row)
        if node is None:
            fail(f"{row.get('kind')} event lacks a peer identity")
            continue
        if not isinstance(row.get("node"), str) or not isinstance(row.get("worker"), str):
            fail(f"{row.get('kind')} event lacks native node/worker identity")
        elif row["node"] != row["worker"]:
            fail(f"{row.get('kind')} event node and worker disagree")
        if row.get("traceprefix") != row.get("prefix"):
            fail(f"{row.get('kind')} event traceprefix disagrees with prefix")
        if (row.get("peer") is not None and row.get("worker") is not None and
                row["peer"] != row["worker"]):
            fail(f"{row.get('kind')} event peer and worker disagree")
        by_node.setdefault(node, {}).setdefault(row["kind"], []).append(row)

    inputs: dict[str, Any] = {}
    views: dict[str, tuple[str, ...]] = {}
    weights: dict[str, float] = {}
    outputs: dict[str, Any] = {}
    native_allocations: dict[str, dict[str, list[tuple[int, int]]]] = {}
    expected_members: set[str] = set()
    participant_nodes = set(by_node)

    for node, kinds in by_node.items():
        input_rows = kinds.get("aggregate_input", [])
        allocation_rows = kinds.get("allocation", [])
        output_rows = kinds.get("aggregate_output", [])
        if len(input_rows) != 1:
            fail(f"{node} has {len(input_rows)} aggregate_input records; exactly one is required")
        if len(allocation_rows) != 1:
            fail(f"{node} has {len(allocation_rows)} allocation records; exactly one is required")
        if len(output_rows) != 1:
            fail(f"{node} has {len(output_rows)} aggregate_output records; exactly one is required")

        input_row = input_rows[0] if len(input_rows) == 1 else None
        allocation = allocation_rows[0] if len(allocation_rows) == 1 else None
        output_row = output_rows[0] if len(output_rows) == 1 else None
        if input_row is not None:
            input_members = input_row.get("members")
            value = _vector(input_row.get("values"))
            if (not isinstance(input_members, list) or not input_members or
                    any(not isinstance(member, str) for member in input_members) or
                    len(set(input_members)) != len(input_members) or node not in input_members):
                fail(f"{node} aggregate_input has an invalid local roster")
            else:
                views[node] = tuple(input_members)
                expected_members.update(input_members)
            if value is None:
                fail(f"{node} aggregate_input has malformed, empty, or non-finite tensor values")
            else:
                inputs[node] = value

        if allocation is not None:
            alloc_members = allocation.get("members")
            if isinstance(alloc_members, list):
                expected_members.update(alloc_members)
            native_weight = allocation.get("weight")
            peer_fractions = allocation.get("peer_fractions")
            if not _number(native_weight) or float(native_weight) < 0:
                fail(f"{node} allocation weight must be finite and nonnegative")
            else:
                weights[node] = float(native_weight)
            if (not isinstance(peer_fractions, list) or not alloc_members or
                    len(peer_fractions) != len(alloc_members) or
                    any(not _number(item) or float(item) < 0 for item in peer_fractions) or
                    sum(float(item) for item in peer_fractions) <= 0):
                fail(f"{node} allocation peer_fractions are malformed or have zero total")
            if input_row is not None and alloc_members != input_row.get("members"):
                fail(f"{node} allocation roster differs from its aggregate_input roster")
            if node in inputs:
                native = _allocation_ranges(allocation, int(inputs[node].size), group_failures,
                                           f"{label}, node={node}")
                if native is not None:
                    native_allocations[node] = native
        if output_row is not None:
            value = _vector(output_row.get("values"))
            if value is None:
                fail(f"{node} aggregate_output has malformed, empty, or non-finite tensor values")
            else:
                outputs[node] = value

        terminal_rows = kinds.get("terminal", [])
        if not terminal_rows:
            fail(f"{node} has no native terminal record; timing/completion is unmodeled")
        elif any(row.get("outcome") != "complete" for row in terminal_rows):
            outcomes = [row.get("outcome") for row in terminal_rows]
            fail(f"{node} native terminal outcomes are not all complete: {outcomes}")
        if not kinds.get("finalize"):
            fail(f"{node} has no native tensor-container finalize record")

    if expected_members != participant_nodes:
        fail(f"native rosters imply participants {sorted(expected_members)}, but captured nodes are "
             f"{sorted(participant_nodes)}; group is partial")
    if set(inputs) != participant_nodes or set(views) != participant_nodes or set(weights) != participant_nodes:
        fail("not every captured participant has an input, roster, and allocation weight")

    if not inputs or len({int(value.size) for value in inputs.values()}) != 1:
        fail("participant inputs do not have one common nonempty shape")
    size = int(next(iter(inputs.values())).size) if inputs else 0
    if size and any(value.size != size for value in outputs.values()):
        fail("native output lengths differ from captured input length")

    # Native part boundaries are explicit. This backend can reproduce one flat
    # tensor with a common part size; it rejects multiple tensor coordinate spaces.
    chunk_sizes = [end - start for alloc in native_allocations.values()
                   for peer_ranges in alloc.values() for start, end in peer_ranges]
    if not chunk_sizes or any(part_size <= 0 for part_size in chunk_sizes):
        fail("native allocation has no supported part boundaries")
        chunk_elements = None
    else:
        first_allocation = next(iter(native_allocations.values()))
        first_ranges = sorted((start, end) for peer_ranges in first_allocation.values()
                              for start, end in peer_ranges)
        chunk_elements = max(end - start for start, end in first_ranges)
        for node, allocation_ranges in native_allocations.items():
            ordered = sorted((start, end) for peer_ranges in allocation_ranges.values()
                             for start, end in peer_ranges)
            if (not ordered or any(end - start != chunk_elements for start, end in ordered[:-1]) or
                    not 0 < ordered[-1][1] - ordered[-1][0] <= chunk_elements):
                fail(f"{node} native part sizes cannot be represented by one flat chunk size")

    model_result = None
    if not group_failures and inputs and chunk_elements is not None:
        try:
            model_result = _load_aggregate()(inputs, views, chunk_elements=chunk_elements, weights=weights)
        except (ImportError, TypeError, ValueError) as exc:
            fail(f"simulator cannot model this native group: {exc}")

    group_checks = {"allocation_nodes_checked": 0, "outputs_checked": 0,
                    "native_chunks_checked": 0, "fallback_nodes_checked": 0}
    if model_result is not None:
        for node in participant_nodes:
            observed = native_allocations.get(node)
            predicted = model_result.allocations.get(node)
            if observed is None or predicted is None:
                fail(f"{node} native/simulator allocation is unavailable")
                continue
            if observed != predicted:
                fail(f"{node} native part_ranges differ from simulator allocation")
            else:
                group_checks["allocation_nodes_checked"] += 1

            expected = model_result.values.get(node)
            actual = outputs.get(node)
            if expected is None or actual is None or not _same(expected, actual):
                fail(f"{node} native aggregate_output differs from weighted simulator output")
            else:
                group_checks["outputs_checked"] += 1

        observed_chunks: dict[tuple[str, str, int], Any] = {}
        for node, kinds in by_node.items():
            for row in kinds.get("processed_chunk", []):
                reducer, part_index = row.get("reducer"), row.get("part_index")
                if (not isinstance(reducer, str) or not isinstance(part_index, int) or
                        isinstance(part_index, bool) or part_index < 0):
                    fail(f"{node} has a malformed processed_chunk identity")
                    continue
                identity = (node, reducer, part_index)
                if identity in observed_chunks:
                    fail(f"duplicate native processed_chunk record for {identity}")
                observed_chunks[identity] = _vector(row.get("values"))

        expected_chunks: dict[tuple[str, str, int], Any] = {}
        for row in model_result.trace:
            if row.get("kind") != "processed_chunk":
                continue
            identity = (row["node"], row["reducer"], int(row["part_index"]))
            if identity in expected_chunks:
                fail(f"simulator produced duplicate processed_chunk identity {identity}")
            expected_chunks[identity] = _vector(row.get("delta"))
        if set(observed_chunks) != set(expected_chunks):
            fail("native processed_chunk identities differ from simulator; partial/timing behavior is unmodeled")
        for identity in sorted(set(observed_chunks) & set(expected_chunks)):
            if observed_chunks[identity] is None or not _same(expected_chunks[identity], observed_chunks[identity]):
                fail(f"native processed_chunk delta differs from simulator for {identity}")
            else:
                group_checks["native_chunks_checked"] += 1

        # The native fallback event records the unregistered suffix of one
        # reducer's output parts. Compare those coordinates to the simulator's
        # recipient/reducer fallback ranges, not just the resulting zero delta.
        observed_fallback: dict[str, list[tuple[int, int]]] = {node: [] for node in participant_nodes}
        for node, kinds in by_node.items():
            for row in kinds.get("fallback", []):
                reducer = row.get("reducer")
                native = native_allocations.get(node, {}).get(reducer)
                first, count = row.get("registered_parts"), row.get("part_count")
                if (native is None or not isinstance(first, int) or isinstance(first, bool) or
                        not isinstance(count, int) or isinstance(count, bool) or
                        first < 0 or count != len(native) or first > count):
                    fail(f"{node} has malformed or unmodeled fallback metadata")
                    continue
                observed_fallback[node].extend(native[first:])
        for node in participant_nodes:
            expected = sorted(model_result.fallback_ranges.get(node, []))
            observed = sorted(observed_fallback.get(node, []))
            if expected != observed:
                fail(f"{node} native fallback ranges differ from simulator; timing/partial fallback is unmodeled")
            else:
                group_checks["fallback_nodes_checked"] += 1

        for node, kinds in by_node.items():
            finalize_rows = kinds.get("finalize", [])
            if len(finalize_rows) != 1:
                fail(f"{node} has {len(finalize_rows)} tensor-container finalize records")
                continue
            expected_failed = sum(end - start for start, end in model_result.fallback_ranges.get(node, []))
            final = finalize_rows[0]
            if final.get("total_elements") != size or final.get("failed_size") != expected_failed:
                fail(f"{node} finalize sizes disagree with simulator fallback coverage")

    for message in group_failures:
        if message not in failures:
            failures.append(message)
    return {
        "prefix": key[0], "group_id": key[1], "participants": sorted(participant_nodes),
        "chunk_elements": chunk_elements, "status": "replayed" if not group_failures else "unmodeled",
        "checks": group_checks, "failures": group_failures,
    }


def replay_case(path: str | Path) -> dict[str, Any]:
    """Replay all complete native groups found at ``path``.

    ``valid`` means the captured numerical groups are complete and agree with
    the native allocation, weighted outputs, deltas, and fallback ranges. It
    does not mean that timing was modeled or that a simulation gate passed.
    """
    case_path = Path(path)
    events, failures = _read_events(case_path)
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in events:
        kind = row.get("kind")
        if kind not in _NUMERICAL_EVENTS:
            continue
        prefix, group_id = row.get("prefix"), row.get("group_id")
        if not isinstance(prefix, str) or not isinstance(group_id, str) or not group_id:
            failures.append(
                f"{row.get('_source')}:{row.get('_line')}: {kind} lacks prefix or group_id"
            )
            continue
        grouped.setdefault((prefix, group_id), []).append(row)

    groups = []
    checks = {"groups_seen": len(grouped), "groups_replayed": 0,
              "groups_excluded_shutdown": 0,
              "allocation_nodes_checked": 0, "outputs_checked": 0,
              "native_chunks_checked": 0, "fallback_nodes_checked": 0}
    excluded_shutdown_groups = []
    for key, rows in sorted(grouped.items()):
        shutdown = _shutdown_only_evidence(key, rows, events)
        if shutdown is not None:
            groups.append(shutdown)
            excluded_shutdown_groups.append(shutdown)
            checks["groups_excluded_shutdown"] += 1
            continue
        group = _capture_group(key, rows, failures)
        groups.append(group)
        if group["status"] == "replayed":
            checks["groups_replayed"] += 1
        for name in ("allocation_nodes_checked", "outputs_checked",
                     "native_chunks_checked", "fallback_nodes_checked"):
            checks[name] += group["checks"][name]
    if not grouped:
        failures.append("no native numerical groups found")

    return {
        "schema": "compute-exclusion-numerical-replay-v1",
        "case_path": str(case_path),
        "scope": "numerical replay only; native timing and simulation gate eligibility are not assessed",
        "simulation_gate_eligible": False,
        "valid": not failures,
        "checks": checks,
        "groups": groups,
        "excluded_shutdown_groups": excluded_shutdown_groups,
        "failures": failures,
    }
