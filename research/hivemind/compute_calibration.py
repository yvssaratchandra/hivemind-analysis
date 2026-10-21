"""Extract descriptive, pilot-only calibration measurements from native traces."""
from __future__ import annotations

import json
import math
from pathlib import Path
from statistics import median
from typing import Any

from compute_timing import summarize_case


DEFAULT_RATES = {"A": 896.0, "B": 128.0, "D": 128.0, "C": 128.0}
DEFAULT_BATCH_SIZE = 32


def _num(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        return float(value) if math.isfinite(float(value)) else None
    except (OverflowError, ValueError):
        return None


def _scalar(value: Any):
    if isinstance(value, (str, bool, int)) and not isinstance(value, float):
        return value
    return _num(value)


def _stats(values: list[float]) -> dict[str, Any]:
    clean = sorted(value for item in values if (value := _num(item)) is not None)
    if not clean:
        return {"n": 0, "mean_seconds": None, "median_seconds": None,
                "p95_seconds": None, "sum_seconds": None}
    return {"n": len(clean), "mean_seconds": sum(clean) / len(clean),
            "median_seconds": float(median(clean)),
            "p95_seconds": clean[math.ceil(.95 * len(clean)) - 1],
            "sum_seconds": sum(clean)}


def _read_json(path: Path, errors: list[str]):
    try:
        return json.loads(path.read_text())
    except FileNotFoundError:
        return None
    except (OSError, json.JSONDecodeError) as exc:
        errors.append(f"{path.name}: {exc}")
        return None


def _read_events(case: Path, errors: list[str]) -> list[dict[str, Any]]:
    events = []
    for source in sorted(case.glob("events_*.jsonl")):
        try:
            lines = source.read_text().splitlines()
        except OSError as exc:
            errors.append(f"{source.name}: {exc}")
            continue
        for line_no, line in enumerate(lines, 1):
            try:
                event = json.loads(line)
            except json.JSONDecodeError as exc:
                errors.append(f"{source.name}:{line_no}: {exc}")
                continue
            if not isinstance(event, dict):
                errors.append(f"{source.name}:{line_no}: event is not an object")
                continue
            event["_source"] = source.name
            event["_line"] = line_no
            events.append(event)
    return events


def _suite_role(case: Path, errors: list[str]) -> tuple[str | None, str | None]:
    for parent in (case, *case.parents):
        status_path = parent / "status.json"
        if not status_path.is_file():
            continue
        status = _read_json(status_path, errors)
        if isinstance(status, dict):
            mode, state = status.get("mode"), status.get("status")
            return (mode if isinstance(mode, str) else None,
                    state if isinstance(state, str) else None)
    return None, None


def _event_node(event: dict[str, Any]) -> str | None:
    value = event.get("node", event.get("worker"))
    return value if isinstance(value, str) and value else None


def _group_channel(prefix: str, rows: list[dict[str, Any]]) -> str:
    channel = next((row.get("channel") for row in rows
                    if row.get("channel") in {"gradient", "state"}), None)
    if channel:
        return channel
    return "state" if prefix.endswith("_state_averager") else "gradient"


def _group_views(events: list[dict[str, Any]]):
    views: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for event in events:
        prefix, group_id, node = event.get("prefix"), event.get("group_id"), _event_node(event)
        if isinstance(prefix, str) and isinstance(group_id, str) and group_id and node:
            views.setdefault((prefix, group_id, node), []).append(event)
    logical: dict[tuple[str, str], dict[str, list[dict[str, Any]]]] = {}
    for (prefix, group_id, node), rows in views.items():
        logical.setdefault((prefix, group_id), {})[node] = rows
    return views, logical


def _view_times(rows: list[dict[str, Any]], kind: str) -> list[float]:
    return [value for event in rows if event.get("kind") == kind
            and (value := _num(event.get("monotonic"))) is not None]


def _shutdown_group(prefix, group_id, by_node, all_events_by_node, terminal_times):
    """Recognize native zero-weight groups created only during shutdown."""
    if not str(prefix).endswith("_grad_averager"):
        return False
    allocations = [row for rows in by_node.values() for row in rows
                   if row.get("kind") == "allocation"]
    if not allocations or any(row.get("weight") != 0 for row in allocations):
        return False
    members = allocations[0].get("members", [])
    if not isinstance(members, list) or not members:
        return False
    for member in members:
        terminal = terminal_times.get(member)
        own = [row for row in all_events_by_node.get(member, [])
               if row.get("prefix") == prefix and row.get("group_id") == group_id]
        starts = [_num(row.get("monotonic")) for row in own
                  if row.get("kind") in {"aggregate_input", "allocation"}]
        member_allocations = [row for row in own if row.get("kind") == "allocation"]
        if terminal is None or not starts or not member_allocations:
            return False
        if any(row.get("weight") != 0 for row in member_allocations):
            return False
        if not all(start is not None and terminal < start for start in starts):
            return False
        if any(row.get("kind") in {"gradient_loaded", "update_begin", "update_end"}
               and (_num(row.get("monotonic")) or 0) > terminal
               for row in all_events_by_node.get(member, [])):
            return False
    return True


def _group_analysis(logical, condition, all_events_by_node, terminal_times):
    counts = {channel: {"observed_groups": 0, "complete_groups": 0,
                        "partial_groups": 0, "missing_groups": 0,
                        "missing_participant_views": 0, "invalid_timing_groups": 0,
                        "fallback_groups_excluded_from_fast_service": 0,
                        "condition_groups_excluded_from_fast_service": 0,
                        "post_terminal_shutdown_groups_excluded": 0}
              for channel in ("gradient", "state")}
    service = {"gradient": [], "state": []}
    intervals = {"gradient": [], "state": []}
    node_fast: dict[str, list[float]] = {}
    node_fallback: dict[str, list[float]] = {}
    for (prefix, _group_id), by_node in logical.items():
        rows = [event for items in by_node.values() for event in items]
        channel = _group_channel(prefix, rows)
        has_fallback = any(event.get("kind") == "fallback" for event in rows)
        summary = counts[channel]
        if _shutdown_group(prefix, _group_id, by_node, all_events_by_node, terminal_times):
            summary["post_terminal_shutdown_groups_excluded"] += 1
            continue
        summary["observed_groups"] += 1
        expected = {member for event in rows
                    for member in (event.get("members") if isinstance(event.get("members"), list) else [])
                    if isinstance(member, str)}
        if not expected:
            expected = set(by_node)
        complete_nodes = set()
        group_inputs, group_outputs = [], []
        for node, node_rows in by_node.items():
            inputs = _view_times(node_rows, "aggregate_input")
            outputs = _view_times(node_rows, "aggregate_output")
            group_inputs.extend(inputs)
            group_outputs.extend(outputs)
            if inputs and outputs:
                complete_nodes.add(node)
                start, end = max(inputs), max(outputs)
                duration = _num(end - start)
                if duration is not None and duration >= 0 and channel == "gradient":
                    target = node_fallback if any(r.get("kind") == "fallback" for r in node_rows) else node_fast
                    target.setdefault(node, []).append(duration)
        missing = expected - complete_nodes
        summary["missing_participant_views"] += len(missing)
        if not group_inputs and not group_outputs:
            summary["missing_groups"] += 1
            continue
        if missing:
            summary["partial_groups"] += 1
            continue
        summary["complete_groups"] += 1
        start, end = max(group_inputs), max(group_outputs)
        duration = _num(end - start)
        if duration is None or duration < 0:
            summary["invalid_timing_groups"] += 1
            continue
        intervals[channel].append((start, end))
        if channel == "state":
            service[channel].append(duration)
        elif has_fallback:
            summary["fallback_groups_excluded_from_fast_service"] += 1
        elif condition not in {"baseline", "absent"}:
            summary["condition_groups_excluded_from_fast_service"] += 1
        else:
            service[channel].append(duration)
    return counts, service, intervals, node_fast, node_fallback


def _overlap(left: tuple[float, float], right: tuple[float, float]) -> float:
    return max(0.0, min(left[1], right[1]) - max(left[0], right[0]))


def _learning_target(analysis, errors):
    if not isinstance(analysis, dict) or not isinstance(analysis.get("learning"), dict):
        return {"available": False}
    learning = analysis["learning"]
    metrics = learning.get("final_metrics")
    metrics = metrics if isinstance(metrics, dict) else {}
    target = {"available": True, "analysis_valid": analysis.get("valid") is True,
              "crossing_time_seconds": _num(learning.get("crossing_time")),
              "confirmed_time_seconds": _num(learning.get("confirmed_time")),
              "censored": learning.get("censored") if isinstance(learning.get("censored"), bool) else None,
              "censor_reason": learning.get("censor_reason") if isinstance(learning.get("censor_reason"), str) else None,
              "censor_time_seconds": _num(learning.get("censor_time")),
              "final_DC_mean": _num(metrics.get("DC_mean")),
              "final_DC_worst": _num(metrics.get("DC_worst")),
              "final_A": _num(metrics.get("A"))}
    if analysis.get("valid") is not True:
        errors.append("analysis.json does not mark this native case valid")
    return target


def calibrate_case(path: str | Path) -> dict[str, Any]:
    """Return raw-derived calibration summaries; use only cases in pilot suites.

    No outcome fitting is performed. Progress visibility is measured at completed
    steps and is therefore an upper bound, not exact publication latency.
    """
    case = Path(path)
    errors: list[str] = []
    config = _read_json(case / "config.json", errors)
    outcome = _read_json(case / "outcome.json", errors)
    release = _read_json(case / "release.json", errors)
    analysis = _read_json(case / "analysis.json", errors)
    events = _read_events(case, errors)
    origin = _num(release.get("monotonic")) if isinstance(release, dict) else None
    horizon = (_num(outcome.get("measured_horizon")) if isinstance(outcome, dict) else None)
    if horizon is None and isinstance(config, dict):
        horizon = _num(config.get("duration"))
    suite_mode, suite_status = _suite_role(case, errors)
    timing = summarize_case(case)
    if timing.get("errors"):
        errors.extend(f"timing: {message}" for message in timing["errors"])
    rate_config = config.get("rates") if isinstance(config, dict) else None
    batch_size = _num(config.get("microbatch_size")) if isinstance(config, dict) else None
    batch_size = int(batch_size) if batch_size is not None and batch_size > 0 else DEFAULT_BATCH_SIZE
    rates = {}
    for node in sorted({_event_node(event) for event in events} - {None}):
        rate = _num(rate_config.get(node)) if isinstance(rate_config, dict) else None
        rates[node] = rate if rate is not None and rate > 0 else DEFAULT_RATES.get(node)

    node_events: dict[str, list[dict[str, Any]]] = {}
    for event in events:
        node = _event_node(event)
        if node:
            node_events.setdefault(node, []).append(event)
    terminal_times = {}
    for event in events:
        node = _event_node(event)
        stamp = _num(event.get("monotonic"))
        if node and event.get("kind") == "training_terminal" and stamp is not None:
            terminal_times[node] = stamp
    _, logical = _group_views(events)
    condition = config.get("condition") if isinstance(config, dict) and isinstance(config.get("condition"), str) else None
    seed = _scalar(config.get("seed")) if isinstance(config, dict) else None
    group_counts, group_service, group_intervals, fast_latency, fallback_latency = _group_analysis(
        logical, condition, node_events, terminal_times)
    nodes = {}
    update_intervals = {}
    update_windows = []
    first_progress = {}
    for node, rows in node_events.items():
        micros = {}
        steps = {}
        update_begin_ids = set()
        begins, ends = {}, {}
        for row in rows:
            index = row.get("batch_index", row.get("current_batch_index"))
            if row.get("kind") == "microbatch" and isinstance(index, int) and not isinstance(index, bool):
                micros.setdefault(index, row)
            elif row.get("kind") == "step_complete" and isinstance(index, int) and not isinstance(index, bool):
                steps.setdefault(index, row)
            elif row.get("kind") == "update_begin":
                if isinstance(index, int) and not isinstance(index, bool):
                    update_begin_ids.add(index)
                    begins.setdefault(index, row)
            elif row.get("kind") == "update_end" and isinstance(index, int) and not isinstance(index, bool):
                ends.setdefault(index, row)
        ordinary_steps = [_num(row.get("seconds")) for index, row in steps.items()
                          if index not in update_begin_ids]
        ordinary_steps = [value for value in ordinary_steps if value is not None and value >= 0]
        interbatch = []
        nominal_rate = rates.get(node)
        if nominal_rate is not None and nominal_rate > 0:
            interval = batch_size / nominal_rate
            for index, micro in micros.items():
                previous = steps.get(index - 1)
                now = _num(micro.get("monotonic"))
                prev = _num(previous.get("monotonic")) if previous else None
                if now is not None and prev is not None and now >= prev:
                    interbatch.append(max(0.0, now - prev - interval))
        else:
            errors.append(f"{node}: no nominal rate available for inter-batch overhead")
        paired_updates = []
        for index in begins.keys() & ends.keys():
            start, end = _num(begins[index].get("monotonic")), _num(ends[index].get("monotonic"))
            if start is not None and end is not None and end >= start:
                paired_updates.append((index, start, end))
                update_windows.append((node, start, end))
        timing_node = timing.get("nodes", {}).get(node, {})
        update_intervals[node] = _num(timing_node.get("median_update_interval_seconds"))
        progress_rows = []
        for index, row in steps.items():
            progress = row.get("global_progress")
            peers = _num(progress.get("num_peers")) if isinstance(progress, dict) else None
            stamp = _num(row.get("monotonic"))
            if peers is not None and peers > 0 and stamp is not None and origin is not None and stamp >= origin:
                progress_rows.append((stamp, index, int(peers)))
        first = min(progress_rows, default=None)
        first_progress[node] = {
            "observations_with_peer_progress": len(progress_rows),
            "lag_upper_bound_seconds": _num(first[0] - origin) if first and origin is not None else None,
            "batch_index": first[1] if first else None,
            "visible_peer_count": first[2] if first else None,
            "interpretation": "First completed-step observation with global_progress.num_peers > 0; upper-bounds when peers became visible.",
        }
        nodes[node] = {
            "nominal_samples_per_second": nominal_rate,
            "microbatch_size": batch_size,
            "step_complete_count": len(steps),
            "steps_excluded_for_update_begin_index_count": sum(index in update_begin_ids for index in steps),
            "ordinary_optimizer_step_seconds": _stats(ordinary_steps),
            "inter_batch_overhead_seconds": _stats(interbatch),
            "gradient_fast_view_latency_seconds": _stats(fast_latency.get(node, [])),
            "gradient_fallback_view_latency_seconds": _stats(fallback_latency.get(node, [])),
            "median_update_interval_seconds": update_intervals[node],
            "paired_update_window_count": len(paired_updates),
            "first_peer_progress_visible": first_progress[node],
        }

    state_intervals = group_intervals["state"]
    gradient_intervals = group_intervals["gradient"]
    state_gradient_overlaps = [_overlap(state, gradient) for state in state_intervals
                               for gradient in gradient_intervals]
    state_update_overlaps = [_overlap(state, (start, end)) for state in state_intervals
                             for _node, start, end in update_windows]
    state_gradient_overlaps = [value for value in state_gradient_overlaps if value > 0]
    state_update_overlaps = [value for value in state_update_overlaps if value > 0]
    fetches = [event for event in events if event.get("kind") == "state_recovery_fetch"]
    recovery_before = [event for event in events if event.get("kind") == "state_recovery_application"
                       and event.get("phase") == "before"]
    recovery_after = [event for event in events if event.get("kind") == "state_recovery_application"
                      and event.get("phase") == "after"]
    return {
        "schema": "compute-pilot-calibration-v1", "case_path": str(case),
        "suite_mode": suite_mode, "suite_status": suite_status,
        "pilot_calibration_eligible": suite_mode == "pilot" and suite_status == "complete",
        "calibration_policy": "Descriptive pilot inputs only; no held-out fitting is performed.",
        "condition": condition,
        "seed": seed,
        "measured_horizon_seconds": horizon, "nominal_rates": rates,
        "native_target": _learning_target(analysis, errors),
        "timing_median_update_intervals_seconds": update_intervals,
        "nodes": nodes,
        "gradient_fast_view_latency_seconds": {node: _stats(values) for node, values in fast_latency.items()},
        "gradient_fallback_view_latency_seconds": {node: _stats(values) for node, values in fallback_latency.items()},
        "group_completeness": group_counts,
        "state_service_latency_seconds": _stats(group_service["state"]),
        "gradient_group_service_latency_seconds": _stats(group_service["gradient"]),
        "state_overlap": {
            "complete_state_service_groups": len(state_intervals),
            "state_gradient_service_overlap_pair_count": len(state_gradient_overlaps),
            "state_gradient_service_overlap_pair_seconds": sum(state_gradient_overlaps),
            "state_optimizer_update_overlap_pair_count": len(state_update_overlaps),
            "state_optimizer_update_overlap_pair_seconds": sum(state_update_overlaps),
            "overlap_definition": "Pairwise interval intersections using each complete group's max-input to max-output interval; seconds sum across pairs, not unique wall time.",
        },
        "recoveries": {
            "fetch_event_count": len(fetches),
            "available_fetch_count": sum(event.get("available") is True for event in fetches),
            "application_attempt_count": len(recovery_before),
            "application_after_event_count": len(recovery_after),
            "duration_seconds": None,
            "duration_note": "Recovery duration is unobserved; no duration is inferred from missing events.",
        },
        "observability_limits": [
            "Aggregate view latency is aggregate_input to aggregate_output and can include local scheduling or rendezvous; it is not reducer-handler-only time.",
            "Fast/fallback gradient latencies classify each node view by whether a fallback event occurred in that group.",
            "State service time uses max(output timestamps) minus max(input timestamps) for a complete logical group, excluding earliest-participant wait.",
            "Peer progress lag is observed only at step_complete and upper-bounds visibility; exact DHT publication latency is not observed.",
            "No recovery duration is assigned unless a future source provides direct recovery timing; absent recovery events mean unobserved, not zero.",
        ],
        "measurement_parameters": {
            "microbatch_size": batch_size,
            "nominal_rates_samples_per_second": rates,
            "fast_gradient_service_conditions": ["baseline", "absent"],
            "fast_gradient_service_excludes_any_logical_group_with_fallback": True,
            "fallback_view_latency_is_measured_wrapper_latency": True,
            "fallback_configured_delay_seconds": 2.0,
        },
        "errors": errors,
    }
