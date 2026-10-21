"""Summarize timing and contribution traces from a compute case."""
from __future__ import annotations
import json
import math
from pathlib import Path
from statistics import median
from typing import Any

def _number(value: Any) -> float | None:
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) else None
def _stats(values: list[float]) -> dict[str, Any]:
    values = sorted(value for value in values if math.isfinite(value))
    if not values:
        return {"count": 0, "sum": 0.0, "mean": None, "median": None, "p95": None}
    return {"count": len(values), "sum": sum(values), "mean": sum(values) / len(values),
            "median": float(median(values)), "p95": values[math.ceil(.95 * len(values)) - 1]}
def _events(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    rows, errors = [], []
    for source in sorted(path.glob("events_*.jsonl")):
        try:
            for line_no, line in enumerate(source.read_text().splitlines(), 1):
                try:
                    row = json.loads(line)
                except json.JSONDecodeError as exc:
                    errors.append(f"{source.name}:{line_no}: {exc}")
                    continue
                if isinstance(row, dict):
                    rows.append(row)
                else:
                    errors.append(f"{source.name}:{line_no}: event is not an object")
        except OSError as exc:
            errors.append(f"{source.name}: {exc}")
    return rows, errors
_batch_id = lambda event: event.get("batch_index", event.get("current_batch_index"))
def _shutdown_group(prefix, rows_by_node, terminals, all_events_by_node) -> bool:
    if not str(prefix).endswith("_grad_averager"):
        return False
    allocations = [row for rows in rows_by_node.values() for row in rows
                   if row.get("kind") == "allocation"]
    if not allocations or any(row.get("weight") != 0 for row in allocations): return False
    members = allocations[0].get("members", [])
    if not members: return False
    for member in members:
        terminal = terminals.get(member)
        own = rows_by_node.get(member, [])
        start = [row.get("monotonic") for row in own
                 if row.get("kind") in {"aggregate_input", "allocation"}]
        member_alloc = [row for row in own if row.get("kind") == "allocation"]
        if terminal is None or not start or not member_alloc or any(a.get("weight") != 0 for a in member_alloc):
            return False
        if not all(_number(t) is not None and terminal < t for t in start):
            return False
        if any(row.get("kind") in {"gradient_loaded", "update_begin", "update_end"}
               and (_number(row.get("monotonic")) or 0) > terminal
               for row in all_events_by_node.get(member, [])):
            return False
    return True

def summarize_case(path: str | Path) -> dict[str, Any]:
    """Return JSON-safe timing and contribution statistics for one case."""
    case = Path(path)
    errors = []
    try:
        config = json.loads((case / "config.json").read_text())
        outcome = json.loads((case / "outcome.json").read_text())
        release = json.loads((case / "release.json").read_text())
    except (OSError, json.JSONDecodeError) as exc:
        return {"path": str(case), "errors": [f"case metadata: {exc}"], "nodes": {}}
    events, read_errors = _events(case)
    errors.extend(read_errors)
    origin = _number(release.get("monotonic")) if isinstance(release, dict) else None
    if origin is None:
        errors.append("release monotonic origin is missing or invalid")
    duration = _number(outcome.get("measured_horizon")) if isinstance(outcome, dict) else None
    if duration is None:
        duration = _number(config.get("duration")) if isinstance(config, dict) else None
    if duration is None or duration <= 0:
        duration = max((max(0.0, (_number(row.get("monotonic")) or 0) - (origin or 0))
                        for row in events), default=0.0)
        errors.append("measured horizon missing; event span used as fallback")
    nodes = sorted(set(row.get("node") for row in events if isinstance(row.get("node"), str)))
    all_events_by_node = {node: [row for row in events if row.get("node") == node] for node in nodes}
    terminal_times = {row.get("node"): value for row in events
                      if row.get("kind") == "training_terminal"
                      and (value := _number(row.get("monotonic"))) is not None}
    node_report = {}
    for node in nodes:
        own = all_events_by_node[node]
        micro = { _batch_id(row): row for row in own if row.get("kind") == "microbatch"
                  and _batch_id(row) is not None }
        steps = {_batch_id(row) for row in own if row.get("kind") == "step_complete"
                 and _batch_id(row) is not None}
        accum = {}
        for row in own:
            if row.get("kind") == "accumulation" and _batch_id(row) is not None:
                accum.setdefault(_batch_id(row), row)
        submitted = {str(index): int(row.get("batch_size", row.get("sample_count", 0)))
                     for index, row in micro.items()
                     if isinstance(row.get("batch_size", row.get("sample_count", 0)), int)
                     and not isinstance(row.get("batch_size", row.get("sample_count", 0)), bool)}
        accepted_by, discarded_by, pending_by = {}, {}, {}
        for index, row in micro.items():
            count = submitted.get(str(index), 0)
            event = accum.get(index)
            if event is not None and event.get("accepted") is True:
                accepted_by[str(index)] = int(event.get("sample_count", count))
            elif event is not None and event.get("accepted") is False:
                discarded_by[str(index)] = int(event.get("sample_count", count))
            elif index in steps:
                discarded_by[str(index)] = count
            else:
                pending_by[str(index)] = count
        computes = [_number(row.get("compute_seconds")) for row in micro.values()]
        paces = [_number(row.get("pacing_seconds")) for row in micro.values()]
        event_times = [_number(row.get("monotonic")) for row in own
                       if row.get("kind") in {"microbatch", "step_complete", "training_terminal"}]
        event_times = [t for t in event_times if t is not None]
        blocks = [_number(row.get("seconds")) for row in own if row.get("kind") == "step_complete"]
        begins, ends = {}, {}
        for row in own:
            if row.get("kind") == "update_begin" and _batch_id(row) is not None:
                begins.setdefault(_batch_id(row), row)
            elif row.get("kind") == "update_end" and _batch_id(row) is not None:
                ends.setdefault(_batch_id(row), row)
        pairs = [(begins[i], ends[i]) for i in begins.keys() & ends.keys()]
        update_times = sorted(_number(start.get("monotonic")) for start, _ in pairs
                              if _number(start.get("monotonic")) is not None)
        update_durations = [end - start for begin, finish in pairs
                            if (start := _number(begin.get("monotonic"))) is not None
                            and (end := _number(finish.get("monotonic"))) is not None and end >= start]
        intervals = [b - a for a, b in zip(update_times, update_times[1:]) if b >= a]
        submitted_count = sum(submitted.values())
        node_report[node] = {
            "submitted_samples": submitted_count,
            "accepted_samples": sum(accepted_by.values()),
            "discarded_samples": sum(discarded_by.values()),
            "pending_samples": sum(pending_by.values()),
            "submitted_samples_per_second": submitted_count / duration,
            "relative_event_time_range_seconds": [min(event_times) - origin, max(event_times) - origin]
            if event_times and origin is not None else None,
            "submitted_samples_by_batch": submitted,
            "accepted_samples_by_batch": accepted_by,
            "discarded_samples_by_batch": discarded_by,
            "pending_samples_by_batch": pending_by,
            "compute_seconds": _stats([v for v in computes if v is not None]),
            "pacing_seconds": _stats([v for v in paces if v is not None]),
            "optimizer_block_seconds": _stats([v for v in blocks if v is not None]),
            "completed_updates": len(pairs),
            "update_duration_seconds": _stats(update_durations),
            "median_update_interval_seconds": float(median(intervals)) if intervals else None,
        }
    grouped = {}
    for row in events:
        if row.get("group_id") is not None and row.get("prefix") is not None:
            grouped.setdefault((row.get("node"), row.get("prefix"), row.get("group_id")), []).append(row)
    logical_groups = {}
    for (node, prefix, gid), rows in grouped.items():
        logical_groups.setdefault((prefix, gid), {})[node] = rows
    shutdown_logical = {key for key, by_node in logical_groups.items()
                        if _shutdown_group(key[0], by_node, terminal_times, all_events_by_node)}
    shutdown_groups = {(node, prefix, gid) for prefix, gid in shutdown_logical
                       for node in logical_groups[(prefix, gid)]}
    group_latencies = {}
    for key, rows in grouped.items():
        if key in shutdown_groups:
            continue
        inputs = [row for row in rows if row.get("kind") == "aggregate_input"]
        outputs = [row for row in rows if row.get("kind") == "aggregate_output"]
        if not inputs and not outputs:
            continue
        channel = next((row.get("channel") for row in rows if row.get("channel")),
                       "state" if str(key[1]).endswith("_state_averager") else "gradient")
        latencies = []
        for start, finish in zip(inputs, outputs):
            a, b = _number(start.get("monotonic")), _number(finish.get("monotonic"))
            if a is not None and b is not None and b >= a:
                latencies.append(b - a)
        if latencies:
            group_latencies.setdefault(channel, []).extend(latencies)
    allocations = {}
    for row in events:
        if row.get("kind") == "allocation" and row.get("group_id") is not None:
            key = (row.get("prefix"), row.get("group_id"))
            if (row.get("node"), key[0], key[1]) not in shutdown_groups:
                allocations.setdefault(key, {}).setdefault(row.get("node"), row)
    group_weights = []
    for (prefix, gid), origins in allocations.items():
        weights = {node: _number(row.get("weight")) for node, row in origins.items()}
        weights = {node: value for node, value in weights.items() if value is not None and value >= 0}
        total = sum(weights.values())
        group_weights.append({"prefix": prefix, "group_id": gid, "channel":
                              "state" if str(prefix).endswith("_state_averager") else "gradient",
                              "unique_origin_weight_total": total, "weight_by_node": weights,
                              "A_weight_share": weights.get("A", 0.0) / total if total else None})
    roster_views = {}
    for row in events:
        if row.get("kind") == "local_roster" and row.get("group_id") is not None:
            key = (row.get("prefix"), row.get("group_id"), row.get("node"))
            roster_views.setdefault(key, row)
    a_native_groups, a_omitted_groups = set(), set()
    a_proposed_views = a_present_views = a_omitted_views = 0
    for (prefix, gid, node), row in roster_views.items():
        if str(prefix).endswith("_state_averager"):
            continue
        proposed = row.get("native_members", row.get("members", []))
        effective = row.get("members", [])
        if "A" in proposed:
            a_proposed_views += 1
            a_native_groups.add((prefix, gid))
            if "A" in effective:
                a_present_views += 1
            else:
                a_omitted_views += 1
                a_omitted_groups.add((prefix, gid))
    a_weight_by_group = {tuple((g["prefix"], g["group_id"])): g["weight_by_node"].get("A", 0.0)
                         for g in group_weights if g["channel"] == "gradient"}
    omitted_weight = sum(a_weight_by_group.get(key, 0.0) for key in a_omitted_groups)
    state_kinds = ("state_application", "state_recovery_fetch", "state_recovery_application",
                   "state_download_chunk", "state_download_terminal")
    state = {kind: sum(row.get("kind") == kind for row in events) for kind in state_kinds}
    state["by_node"] = {node: {kind: sum(row.get("node") == node and row.get("kind") == kind
                                         for row in events) for kind in state_kinds} for node in nodes}
    state["downloads_by_path"] = {}
    for row in events:
        if row.get("kind") == "state_download_chunk":
            edge = f"{row.get('sender')}->{row.get('recipient')}"
            state["downloads_by_path"][edge] = state["downloads_by_path"].get(edge, 0) + 1
    state["A_paths"] = {edge: count for edge, count in state["downloads_by_path"].items()
                         if "A" in edge.split("->")}
    return {"path": str(case), "condition": config.get("condition"), "mode": config.get("mode"),
            "outcome_status": outcome.get("status"), "completed": outcome.get("completed") is True,
            "measured_horizon_seconds": duration, "errors": errors, "nodes": node_report,
            "untraced_boundary_examples_upper_bound_per_node": config.get("microbatch_size", 32),
            "sample_counting_note": "Counts include logged batches submitted after pacing. If the horizon hits during pacing, up to one already-computed microbatch per node is unlogged.",
            "group_timing_seconds": {channel: _stats(values) for channel, values in group_latencies.items()},
            "post_terminal_shutdown_groups_excluded": len(shutdown_logical),
            "post_terminal_shutdown_group_views_excluded": len(shutdown_groups),
            "unique_group_weight_totals": group_weights,
            "roster_contribution_summary": {
                "A_native_present_group_count": len(a_native_groups),
                "A_proposed_view_count": a_proposed_views,
                "A_effective_present_view_count": a_present_views,
                "A_omitted_view_count": a_omitted_views,
                "A_omitted_unique_group_count": len(a_omitted_groups),
                "A_weight_in_groups_with_an_omitted_view": omitted_weight,
                "interpretation": "A weight is counted once per logical group; roster omission alone does not imply zero influence elsewhere.",
            }, "state": state}
