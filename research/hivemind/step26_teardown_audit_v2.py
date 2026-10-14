"""Step 26 bounded audit correction for incomplete teardown RPC prefixes.

This runtime wrapper is for the already-closed exploratory trial traces. It
removes only narrowly recognized, incomplete unary RPC prefixes that begin
after control-plane activity ended. Raw events remain unchanged, and neither
RPC completion nor a response outcome is inferred.
"""
from __future__ import annotations

from collections import defaultdict
from functools import wraps
import math

try:
    from . import step26_teardown_audit as _base
except ImportError:  # direct script/import layout used by the sweep tools
    import step26_teardown_audit as _base


RPC_KINDS = frozenset((
    "control_rpc_start",
    "control_rpc_delay",
    "control_rpc_handler_enter",
    "control_rpc_response",
    "control_rpc_end",
    "control_join_admission_state",
))
CONTROL_CUTOFF_KINDS = (
    "search_span_end",
    "ad_read_end",
    "join_attempt_end",
)


def _finite(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        number = float(value)
    except (OverflowError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _time(event):
    return _finite(event.get("monotonic"))


def _ref(event):
    return f"{event.get('pid')}:{event.get('event_number')}"


def _rpc_key(event):
    detail = event.get("detail")
    rpc_id = detail.get("rpc_id") if isinstance(detail, dict) else None
    if not isinstance(rpc_id, str) or not rpc_id:
        return None
    return event.get("pid"), rpc_id


def _control_cutoff(events):
    by_kind = defaultdict(list)
    for event in events:
        by_kind[event.get("kind")].append(event)
    marker_kinds = list(CONTROL_CUTOFF_KINDS)
    if by_kind.get("assembled"):
        marker_kinds.append("assembled")
    if by_kind.get("ad_finished"):
        marker_kinds.append("ad_finished")
    records, missing, malformed = [], [], []
    for kind in marker_kinds:
        rows = by_kind.get(kind, [])
        if not rows:
            missing.append(kind)
            continue
        timed = [(value, event) for event in rows if (value := _time(event)) is not None]
        if len(timed) != len(rows):
            malformed.append(kind)
            continue
        value, event = max(timed, key=lambda item: item[0])
        records.append(dict(kind=kind, event_ref=_ref(event), monotonic=value))
    cutoff_record = max(records, key=lambda row: row["monotonic"], default=None)
    return (cutoff_record["monotonic"] if cutoff_record else None,
            cutoff_record, records, missing, malformed)


def _valid_delay_event(event, configured_delay, start_time):
    detail = event.get("detail")
    if not isinstance(detail, dict) or detail.get("leg") != "request":
        return "incomplete RPC prefix delay is not the request leg"
    requested = _finite(detail.get("requested_delay"))
    actual = _finite(detail.get("actual_delay"))
    configured = _finite(configured_delay)
    event_time = _time(event)
    if requested is None or actual is None or configured is None or event_time is None:
        return "request delay record has non-finite or missing timing metadata"
    if not math.isclose(requested, configured, rel_tol=0.0, abs_tol=1e-12):
        return "request delay does not match the configured case delay"
    if actual < 0 or (detail.get("cancelled") is not False):
        return "request delay is negative or marked cancelled/ambiguous"
    if configured > 0 and actual + 0.005 < requested:
        return "observed request delay is materially shorter than configured"
    if event_time < start_time:
        return "request delay record precedes RPC start"
    if actual > event_time - start_time + 0.005:
        return "request delay duration exceeds observed start-to-delay interval"
    return None


def _prefix_record(event):
    detail = event.get("detail", {})
    selected = {}
    if isinstance(detail, dict):
        for name in ("rpc_id", "rpc_type", "leg", "requested_delay", "actual_delay",
                     "cancelled", "delay_seconds", "key_hashes"):
            if name in detail:
                selected[name] = detail[name]
    return dict(kind=event.get("kind"), event_ref=_ref(event),
                monotonic=_time(event), detail=selected)


def classify_tail(events, native, configured_delay=None):
    """Classify safe-to-exclude incomplete unary RPC prefixes.

    Eligible event shapes are exactly a lone `control_rpc_start`, or one start,
    one uncancelled request-delay event, and one handler-entry event. All
    observed prefix events must lie strictly after control-plane work and
    strictly before valid trace closure.
    """
    events = list(events)
    base = _base.classify_tail(events, native, configured_delay=configured_delay)
    cutoff, cutoff_event, cutoff_events, missing, malformed = _control_cutoff(events)
    trace_close = base.get("trace_closed")
    base_gates = list(base.get("eligibility_gates", []))
    if base.get("native_checks_pass") is not True:
        base_gates.append("native control-plane/trace integrity checks did not pass")
    if base.get("activity_spans_complete") is not True:
        base_gates.append("native activity spans are incomplete")
    if missing:
        base_gates.append("missing control-plane activity markers: " + ", ".join(missing))
    if malformed:
        base_gates.append("malformed control-plane activity markers: " + ", ".join(malformed))
    if cutoff is None:
        base_gates.append("no valid control-plane activity cutoff")

    streams = defaultdict(list)
    for event in events:
        if event.get("kind") in RPC_KINDS:
            key = _rpc_key(event)
            if key is not None:
                streams[key].append(event)

    excluded, unexcluded = [], []
    close_time = trace_close.get("monotonic") if trace_close else None
    for key, rows in streams.items():
        starts = [row for row in rows if row.get("kind") == "control_rpc_start"]
        # Complete RPCs remain under the frozen analyzer; only one-start,
        # missing-end streams are candidates for this narrowly scoped audit.
        if len(starts) != 1 or any(row.get("kind") == "control_rpc_end" for row in rows):
            continue
        start = starts[0]
        rpc_type = start.get("detail", {}).get("rpc_type")
        ordered = sorted(rows, key=lambda row: (_time(row) if _time(row) is not None else float("inf"),
                                                 row.get("event_number", -1)))
        kinds = [row.get("kind") for row in ordered]
        reason = None
        if rpc_type not in ("find", "store"):
            reason = "only unary find/store RPCs qualify"
        elif (error := _base._start_metadata_error(start, configured_delay)):
            reason = error
        elif start.get("detail", {}).get("step_id") is not None:
            reason = "RPC start is tied to a training step, not teardown-only work"
        elif base_gates:
            reason = "; ".join(base_gates)
        elif kinds not in (["control_rpc_start"],
                           ["control_rpc_start", "control_rpc_delay", "control_rpc_handler_enter"]):
            reason = "incomplete RPC event stream is not an allowed start/request-delay/handler prefix"
        start_time = _time(start)
        if reason is None and (start_time is None or start_time <= cutoff):
            reason = "RPC prefix starts at or before the final control-plane activity cutoff"
        if reason is None and (close_time is None or any(_time(row) is None or _time(row) >= close_time
                                                        for row in ordered)):
            reason = "not every observed RPC-prefix event is strictly before valid trace closure"
        if reason is None and len(ordered) == 3:
            delay_error = _valid_delay_event(ordered[1], configured_delay, start_time)
            handler_time = _time(ordered[2])
            if delay_error:
                reason = delay_error
            elif (ordered[2].get("peer") != start.get("peer") or
                  ordered[2].get("other") != start.get("other")):
                reason = "handler destination/source peers differ from RPC start"
            elif handler_time is None or handler_time < _time(ordered[1]):
                reason = "handler entry precedes or has invalid time relative to request delay"
        row = dict(
            pid=start.get("pid"), rpc_id=key[1], rpc_type=rpc_type,
            prefix_events=[_prefix_record(event) for event in ordered],
            prefix_shape=kinds,
            control_activity_cutoff_monotonic=cutoff,
            control_activity_cutoff_event=cutoff_event,
            control_activity_cutoff_events=cutoff_events,
            trace_closed=trace_close,
        )
        if reason is None:
            row["reason"] = (
                "Incomplete unary RPC prefix is strictly after control-plane activity and before a valid "
                "trace close. Excluded from RPC integrity auditing only; no end, response, completion, or "
                "outcome is observed or inferred. Raw prefix records remain in the trace."
            )
            excluded.append(row)
        else:
            row["reason"] = reason
            unexcluded.append(row)

    return dict(
        control_activity_cutoff_monotonic=cutoff,
        control_activity_cutoff_event=cutoff_event,
        control_activity_cutoff_events=cutoff_events,
        missing_control_activity_markers=missing,
        malformed_control_activity_markers=malformed,
        trace_closed=trace_close,
        native_checks_pass=base.get("native_checks_pass") is True,
        activity_spans_complete=base.get("activity_spans_complete") is True,
        excluded=excluded,
        unexcluded_incomplete_streams=unexcluded,
        eligibility_gates=base_gates,
    )


def install(adapter):
    """Install the v2 wrapper once, independently of the v1 wrapper flag."""
    current = getattr(adapter, "_rpc_integrity", None)
    if not callable(current):
        raise TypeError("analyzer adapter must expose callable _rpc_integrity")
    if getattr(adapter, "_step26_teardown_v2_audit_installed", False):
        return adapter
    original = current

    @wraps(original)
    def audited_rpc_integrity(case, config, events, by_kind, by_ref, native):
        configured_delay = case.get("one_way_delay_seconds") if isinstance(case, dict) else None
        classification = classify_tail(events, native, configured_delay=configured_delay)
        excluded_refs = {event["event_ref"] for stream in classification["excluded"]
                         for event in stream["prefix_events"]}
        if excluded_refs:
            filtered = [event for event in events if _ref(event) not in excluded_refs]
            filtered_by_kind = defaultdict(list)
            filtered_by_ref = {}
            for event in filtered:
                filtered_by_kind[event["kind"]].append(event)
                filtered_by_ref[_ref(event)] = event
            result = original(case, config, filtered, filtered_by_kind, filtered_by_ref, native)
        else:
            result = original(case, config, events, by_kind, by_ref, native)
        summary = result.setdefault("summary", {})
        summary.update(
            teardown_v2_excluded_stream_count=len(classification["excluded"]),
            teardown_v2_excluded_streams=classification["excluded"],
            teardown_v2_unexcluded_incomplete_stream_count=len(classification["unexcluded_incomplete_streams"]),
            teardown_v2_unexcluded_incomplete_streams=classification["unexcluded_incomplete_streams"],
            teardown_v2_control_activity_cutoff_monotonic=classification["control_activity_cutoff_monotonic"],
            teardown_v2_control_activity_cutoff_event=classification["control_activity_cutoff_event"],
            teardown_v2_control_activity_cutoff_events=classification["control_activity_cutoff_events"],
            teardown_v2_trace_closed=classification["trace_closed"],
            teardown_v2_native_checks_pass=classification["native_checks_pass"],
            teardown_v2_activity_spans_complete=classification["activity_spans_complete"],
            teardown_v2_eligibility_gates=classification["eligibility_gates"],
        )
        return result

    audited_rpc_integrity._step26_teardown_v2_wrapper = True
    audited_rpc_integrity._step26_teardown_v2_original = original
    adapter._rpc_integrity = audited_rpc_integrity
    adapter._step26_teardown_v2_audit_installed = True
    return adapter
