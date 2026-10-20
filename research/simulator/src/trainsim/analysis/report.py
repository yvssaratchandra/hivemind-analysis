"""Trace replay and compact human-readable reporting."""

from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Any, Mapping

from ..observer import Observer


def _open_trace(path: str | Path):
    trace_path = Path(path)
    if trace_path.suffix == ".gz":
        return gzip.open(trace_path, "rt", encoding="utf-8")
    return trace_path.open("r", encoding="utf-8")


def analyze_trace(
    path: str | Path,
    labels: Mapping[str, set[str] | list[str]] | None = None,
    target_work: float | None = None,
) -> dict[str, Any]:
    """Rebuild accounting and group tables by replaying a saved JSONL trace.

    The source ``work_accepted`` rows are derived records. Replay recomputes them
    from aggregate results and skips those rows, so retries and duplicates cannot
    double-count work.
    """
    observer = Observer(path=None, labels=labels)
    observer.target_work = float(target_work) if target_work is not None else None
    try:
        with _open_trace(path) as stream:
            for line_number, line in enumerate(stream, 1):
                if not line.strip():
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError as error:
                    raise ValueError(f"Invalid trace JSON at line {line_number}: {error}") from error
                if not isinstance(event, dict):
                    raise ValueError(f"Trace line {line_number} must contain an object")
                kind = str(event.get("kind", ""))
                if kind == "work_accepted":
                    continue
                data = event.get("data", {})
                if not isinstance(data, dict):
                    raise ValueError(f"Trace line {line_number} has non-object event data")
                observer.emit(
                    float(event.get("time", 0.0)),
                    kind,
                    str(event.get("actor", "")),
                    **data,
                )
    finally:
        observer.close()

    online_shape = observer.summary()
    summary = {
        key: value for key, value in online_shape.items()
        if key not in {"per_node", "groups", "effective_sets", "phase_durations"}
    }
    return {
        "summary": summary,
        "per_node": online_shape["per_node"],
        "groups": online_shape["groups"],
        "effective_sets": online_shape["effective_sets"],
        "phase_durations": online_shape["phase_durations"],
    }


def format_report(data: Mapping[str, Any]) -> str:
    """Render key replay metrics without implying model convergence."""
    summary = data.get("summary", {})
    lines = [
        "# Simulated coordination workload",
        "",
        f"Accepted work: {summary.get('accepted_work', 0)}.",
        f"Accepted work at observation boundary: {summary.get('accepted_work_at_observation')}.",
        f"First accepted contribution: {summary.get('first_accept_time')} s.",
        f"Target time: {summary.get('target_time')} s; last event: {summary.get('last_time')} s.",
        f"Groups: {summary.get('group_count', 0)}; effective chunk sets: {summary.get('effective_set_count', 0)}.",
        f"Zero-participation nodes: {summary.get('zero_participation_nodes', 0)}.",
        "",
        "Work units are synthetic independent batch contributions. These measurements do not establish model convergence.",
    ]
    return "\n".join(lines) + "\n"
