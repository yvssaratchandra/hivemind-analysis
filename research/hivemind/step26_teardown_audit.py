"""Bounded Step 26 teardown-tail audit for start-only control RPCs.

This add-on wraps the frozen Step 26 analyzer at runtime. It only removes an
RPC start from the RPC-integrity input when the RPC has no linked records and
started strictly after all observed experiment work, with a closed raw trace
and passing native integrity checks. It does not infer that the RPC completed
or alter native grouping, choice, or policy reconstruction.
"""
from __future__ import annotations

from collections import defaultdict
from functools import wraps
import math


MARKER_KINDS = (
    "step_result",
    "search_span_end",
    "ad_read_end",
    "join_attempt_end",
)
OPTIONAL_MARKER_KINDS = ("ad_finished",)
RPC_KINDS = frozenset((
    "control_rpc_start",
    "control_rpc_delay",
    "control_rpc_handler_enter",
    "control_rpc_response",
    "control_rpc_end",
    "control_join_admission_state",
))


def _event_ref(event):
    return f"{event.get('pid')}:{event.get('event_number')}"


def _finite_monotonic(event):
    value = event.get("monotonic")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    value = float(value)
    return value if math.isfinite(value) else None


def _rpc_key(event):
    detail = event.get("detail")
    rpc_id = detail.get("rpc_id") if isinstance(detail, dict) else None
    if not isinstance(rpc_id, str) or not rpc_id:
        return None
    return event.get("pid"), rpc_id


def _activity_spans_complete(events):
    """Check experiment start/end pairs needed to justify a post-activity tail."""
    pairs = (
        ("step_start", "step_result", lambda e: (e.get("detail", {}).get("name"),
                                                   e.get("detail", {}).get("sequence"))),
        ("search_span_start", "search_span_end", lambda e: (e.get("peer"),
                                                             e.get("detail", {}).get("search_id"))),
        ("ad_read_start", "ad_read_end", lambda e: (e.get("peer"),
                                                     e.get("detail", {}).get("read_id"))),
        ("join_attempt_start", "join_attempt_end", lambda e: (e.get("peer"),
                                                               e.get("detail", {}).get("join_id"))),
    )
    for start_kind, end_kind, identity in pairs:
        starts, ends = defaultdict(list), defaultdict(list)
        for event in events:
            if event.get("kind") == start_kind:
                starts[identity(event)].append(event)
            elif event.get("kind") == end_kind:
                ends[identity(event)].append(event)
        if any(not key[0] or not key[1] or len(rows) != 1 for key, rows in starts.items()):
            return False
        if any(not key[0] or not key[1] or len(rows) != 1 for key, rows in ends.items()):
            return False
        if set(starts) != set(ends):
            return False
        for key, start_rows in starts.items():
            if _finite_monotonic(start_rows[0]) is None or _finite_monotonic(ends[key][0]) is None:
                return False
            if _finite_monotonic(ends[key][0]) < _finite_monotonic(start_rows[0]):
                return False
    return True


def _start_metadata_error(event, configured_delay=None):
    detail = event.get("detail")
    if not isinstance(detail, dict):
        return "RPC start has malformed detail"
    rpc_id = detail.get("rpc_id")
    if not isinstance(rpc_id, str) or not rpc_id:
        return "RPC start has missing/invalid rpc_id"
    rpc_type = detail.get("rpc_type")
    if rpc_type not in ("find", "store", "join"):
        return "RPC start has unknown rpc_type"
    delay = detail.get("delay_seconds")
    if isinstance(delay, bool) or not isinstance(delay, (int, float)) or not math.isfinite(float(delay)):
        return "RPC start has missing/non-finite delay_seconds"
    if delay < 0:
        return "RPC start has negative delay_seconds"
    key_hashes = detail.get("key_hashes", [])
    if not isinstance(key_hashes, list) or any(not isinstance(value, str) for value in key_hashes):
        return "RPC start has malformed key_hashes"
    if configured_delay is not None:
        if (isinstance(configured_delay, bool) or
                not isinstance(configured_delay, (int, float)) or
                not math.isfinite(float(configured_delay)) or
                not math.isclose(float(delay), float(configured_delay), rel_tol=0.0, abs_tol=1e-12)):
            return "RPC start delay_seconds does not match the configured case delay"
    return None


def classify_tail(events, native, configured_delay=None):
    """Classify eligible teardown-tail RPC starts without inferring completion.

    `events` is the analyzer's parsed raw-event list and `native` is the output
    from the frozen native grouping/choice reconstruction. Returned event
    references are `{pid}:{event_number}` strings suitable for audit reports.
    """
    events = list(events)
    by_kind = defaultdict(list)
    streams = defaultdict(list)
    for event in events:
        kind = event.get("kind")
        by_kind[kind].append(event)
        if kind in RPC_KINDS:
            key = _rpc_key(event)
            if key is not None:
                streams[key].append(event)

    marker_records = []
    marker_kinds = list(MARKER_KINDS)
    marker_kinds.extend(kind for kind in OPTIONAL_MARKER_KINDS if by_kind.get(kind))
    marker_values = []
    missing_markers = []
    malformed_markers = []
    for kind in marker_kinds:
        rows = by_kind.get(kind, [])
        if not rows:
            missing_markers.append(kind)
            continue
        valid = [(value, event) for event in rows
                 if (value := _finite_monotonic(event)) is not None]
        if len(valid) != len(rows):
            malformed_markers.append(kind)
            continue
        value, event = max(valid, key=lambda pair: pair[0])
        record = dict(kind=kind, event_ref=_event_ref(event), monotonic=value)
        marker_records.append(record)
        marker_values.append((value, record))
    cutoff = max((value for value, _ in marker_values), default=None)
    cutoff_record = next((record for value, record in marker_values if value == cutoff), None)

    closures = by_kind.get("trace_closed", [])
    close_event = closures[0] if len(closures) == 1 else None
    close_time = _finite_monotonic(close_event) if close_event else None
    close_detail = close_event.get("detail", {}) if close_event else {}
    closure_valid = bool(
        close_event is not None and close_time is not None and
        close_detail.get("error") is None and close_detail.get("pending") == []
    )
    close_record = (dict(event_ref=_event_ref(close_event), monotonic=close_time)
                    if closure_valid else None)
    native_pass = native.get("control_plane_pass") is True and native.get("trace_integrity_pass") is True
    spans_complete = _activity_spans_complete(events)

    gates = []
    if missing_markers:
        gates.append("missing activity markers: " + ", ".join(missing_markers))
    if malformed_markers:
        gates.append("activity marker has a non-finite timestamp: " + ", ".join(malformed_markers))
    if cutoff is None:
        gates.append("no valid activity cutoff")
    if not closure_valid:
        gates.append("raw trace_closed is absent, ambiguous, malformed, or reports pending work/error")
    if not native_pass:
        gates.append("native control_plane_pass and trace_integrity_pass are not both true")
    if not spans_complete:
        gates.append("one or more native activity spans are incomplete or malformed")
    if closure_valid and cutoff is not None and close_time < cutoff:
        gates.append("trace_closed precedes the final activity marker")

    excluded = []
    unexcluded_start_only = []
    for key, rows in streams.items():
        starts = [event for event in rows if event.get("kind") == "control_rpc_start"]
        if len(starts) != 1 or len(rows) != 1:
            continue
        start = starts[0]
        detail = start.get("detail", {})
        rpc_type = detail.get("rpc_type")
        start_time = _finite_monotonic(start)
        reason = None
        if rpc_type not in ("find", "store"):
            reason = "only start-only find/store RPCs qualify; this RPC type remains audited"
        elif (metadata_error := _start_metadata_error(start, configured_delay)):
            reason = metadata_error
        elif gates:
            reason = "; ".join(gates)
        elif start_time is None:
            reason = "RPC start has a non-finite timestamp"
        elif start_time <= cutoff:
            reason = "RPC start is at or before the final activity cutoff"
        elif start_time >= close_time:
            reason = "RPC start is not strictly before trace_closed"

        row = dict(pid=start.get("pid"), rpc_id=key[1], rpc_type=rpc_type,
                   start_event_ref=_event_ref(start), start_monotonic=start_time,
                   activity_cutoff_monotonic=cutoff,
                   activity_cutoff_event_ref=(cutoff_record["event_ref"] if cutoff_record else None),
                   trace_closed_event_ref=(close_record["event_ref"] if close_record else None),
                   trace_closed_monotonic=(close_record["monotonic"] if close_record else None))
        if reason is None:
            row["reason"] = (
                "Start-only find/store RPC begins strictly after measured activity and before a valid "
                "trace close. Excluded from the RPC instrumentation audit only; no completion or outcome "
                "is observed or inferred."
            )
            excluded.append(row)
        else:
            row["reason"] = reason
            unexcluded_start_only.append(row)

    return dict(
        activity_cutoff_monotonic=cutoff,
        activity_cutoff_event=cutoff_record,
        activity_cutoff_events=marker_records,
        missing_activity_markers=missing_markers,
        malformed_activity_markers=malformed_markers,
        trace_closed=close_record,
        native_checks_pass=native_pass,
        activity_spans_complete=spans_complete,
        excluded=excluded,
        unexcluded_start_only=unexcluded_start_only,
        eligibility_gates=gates,
    )


def install(adapter):
    """Install this audit around `adapter._rpc_integrity` once per module."""
    current = getattr(adapter, "_rpc_integrity", None)
    if not callable(current):
        raise TypeError("analyzer adapter must expose callable _rpc_integrity")
    if getattr(adapter, "_step26_teardown_audit_installed", False):
        return adapter

    original = current

    @wraps(original)
    def audited_rpc_integrity(case, config, events, by_kind, by_ref, native):
        configured_delay = case.get("one_way_delay_seconds") if isinstance(case, dict) else None
        classification = classify_tail(events, native, configured_delay=configured_delay)
        excluded_refs = {row["start_event_ref"] for row in classification["excluded"]}
        if not excluded_refs:
            result = original(case, config, events, by_kind, by_ref, native)
        else:
            filtered_events = [event for event in events if _event_ref(event) not in excluded_refs]
            filtered_by_kind = defaultdict(list)
            filtered_by_ref = {}
            for event in filtered_events:
                filtered_by_kind[event["kind"]].append(event)
                filtered_by_ref[_event_ref(event)] = event
            result = original(case, config, filtered_events, filtered_by_kind,
                              filtered_by_ref, native)
        summary = result.setdefault("summary", {})
        summary.update(
            teardown_tail_excluded_stream_count=len(classification["excluded"]),
            teardown_tail_excluded_streams=classification["excluded"],
            teardown_tail_activity_cutoff_monotonic=classification["activity_cutoff_monotonic"],
            teardown_tail_activity_cutoff_event=classification["activity_cutoff_event"],
            teardown_tail_activity_cutoff_events=classification["activity_cutoff_events"],
            teardown_tail_trace_closed=classification["trace_closed"],
            teardown_tail_unexcluded_start_only_count=len(classification["unexcluded_start_only"]),
            teardown_tail_unexcluded_start_only=classification["unexcluded_start_only"],
            teardown_tail_native_checks_pass=classification["native_checks_pass"],
            teardown_tail_activity_spans_complete=classification["activity_spans_complete"],
            teardown_tail_eligibility_gates=classification["eligibility_gates"],
        )
        return result

    audited_rpc_integrity._step26_teardown_audit_wrapper = True
    audited_rpc_integrity._step26_teardown_original = original
    adapter._rpc_integrity = audited_rpc_integrity
    adapter._step26_teardown_audit_installed = True
    return adapter
