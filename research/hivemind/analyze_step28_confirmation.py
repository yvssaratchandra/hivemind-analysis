"""Step 28 analysis for reliable single versus renewed advertisements.

This analysis preserves the Step 27 native grouping reconstruction while
adding attempt-level outgoing-choice, acceptance, participation, clock, and
capped-latency summaries. Raw traces remain the source of truth; exports keep
event references so rows can be checked against them.
"""
from __future__ import annotations

import csv
from collections import Counter, defaultdict
import json
import math
from pathlib import Path
import random
import statistics
from types import SimpleNamespace

try:
    from .analyze_step24_lifetime import finite_number, read_json
    from .analyze_step25_choices import analyse_trial_choices, _event_stream
    from . import analyze_step26_joint as _step26
    from . import step26_teardown_audit_v2 as _teardown_v2
except ImportError:  # direct import layout used by the experiment runner
    from analyze_step24_lifetime import finite_number, read_json
    from analyze_step25_choices import analyse_trial_choices, _event_stream
    import analyze_step26_joint as _step26
    import step26_teardown_audit_v2 as _teardown_v2


X_NAME = "X"
HONEST_NAMES = tuple(f"H{i}" for i in range(7))
CAP_SECONDS = 12.0
CSV_NAMES = ("leadership_summary.csv", "attempts.csv", "groups.csv", "retries.csv",
             "holds.csv", "choices.csv", "joins.csv",
             "per_seed_contrasts.csv")
_STEP28_CLOCK_JUMP_LIMIT = 0.25
_rpc_adapter = SimpleNamespace(_rpc_integrity=_step26._rpc_integrity)
_teardown_v2.install(_rpc_adapter)
_rpc_integrity = _rpc_adapter._rpc_integrity


def _ref(event):
    return f"{event['pid']}:{event['event_number']}"


def _event_order(event):
    return (float(event.get("monotonic", 0.0)), event.get("pid", -1),
            event.get("event_number", -1))


def _median(values):
    values = [float(value) for value in values if value is not None]
    return statistics.median(values) if values else None


def _percentile(values, fraction):
    values = sorted(float(value) for value in values if value is not None)
    if not values:
        return None
    position = (len(values) - 1) * fraction
    lower, upper = math.floor(position), math.ceil(position)
    if lower == upper:
        return values[lower]
    weight = position - lower
    return values[lower] * (1.0 - weight) + values[upper] * weight


def _mean(values):
    values = [float(value) for value in values if value is not None]
    return statistics.mean(values) if values else None


def _event_groups(events):
    by_kind, by_ref = defaultdict(list), {}
    for event in events:
        by_kind[event["kind"]].append(event)
        ref = _ref(event)
        if ref in by_ref:
            raise ValueError(f"duplicate event reference {ref}")
        by_ref[ref] = event
    return by_kind, by_ref


def _clock_integrity(events, errors, required):
    """Check raw controller clock samples for discontinuous wall/boot offsets."""
    clock_events = [event for event in events if event.get("kind") == "controller_clock"]
    if not required:
        return dict(required=False, samples=len(clock_events), wall_offset_spread_seconds=None,
                    boottime_offset_spread_seconds=None)
    if not clock_events:
        errors.append("Step 28 raw trace has no controller_clock samples")
    offsets = {"clock_wall": [], "clock_boottime": []}
    valid_sample_count = 0
    for event in clock_events:
        detail = event.get("detail", {})
        monotonic = detail.get("clock_monotonic", event.get("monotonic"))
        valid_sample = True
        for field in offsets:
            value = detail.get(field)
            try:
                monotonic_number = finite_number(monotonic, "controller clock monotonic")
                value_number = finite_number(value, f"controller {field}")
            except (TypeError, ValueError) as exc:
                errors.append(f"controller_clock {_ref(event)}: {exc}")
                valid_sample = False
                continue
            offsets[field].append(value_number - monotonic_number)
        if valid_sample:
            valid_sample_count += 1
    if valid_sample_count < 2:
        errors.append("Step 28 requires at least two valid controller_clock samples")
    spreads = {}
    for field, rows in offsets.items():
        spread = max(rows) - min(rows) if rows else None
        spreads[field] = spread
        if spread is None:
            errors.append(f"Step 28 has no valid {field} controller clock offsets")
        elif spread > _STEP28_CLOCK_JUMP_LIMIT:
            errors.append(f"controller clock {field}-monotonic offset spread exceeds "
                          f"{_STEP28_CLOCK_JUMP_LIMIT}s")
    return dict(required=True, samples=len(clock_events), valid_samples=valid_sample_count,
                wall_offset_spread_seconds=spreads["clock_wall"],
                boottime_offset_spread_seconds=spreads["clock_boottime"])


def _guard_new_control_tail(rpc, events, errors):
    """Refuse Step 26 tail exemptions that overlap Step 27 control activity."""
    summary = rpc.get("summary", {})
    excluded = summary.get("teardown_v2_excluded_streams", [])
    markers = [event for event in events
               if event.get("kind") == "assembly_deadline_state" or
               (isinstance(event.get("kind"), str) and
                event["kind"].startswith("unreliable_"))]
    latest_activity = max((event.get("monotonic") for event in markers
                           if isinstance(event.get("monotonic"), (int, float))), default=None)
    if latest_activity is None:
        return
    for stream in excluded:
        prefix = stream.get("prefix_events", [])
        start = next((row.get("monotonic") for row in prefix
                      if row.get("kind") == "control_rpc_start"), None)
        if start is not None and start <= latest_activity:
            errors.append(
                "teardown-v2 excluded unary RPC prefix overlaps Step 27 unreliable/assembly control activity "
                f"(rpc_id={stream.get('rpc_id')}, start={start}, latest_step27_activity={latest_activity})")


def _canonical_cases():
    cases = [dict(name="native_baseline", mode="passive", response="reliable",
                  lifetime=5.0, hold_seconds=0.0)]
    for response in ("reliable", "unreliable"):
        for mode, suffix in (("once", "single"), ("refresh", "renewed")):
            case = dict(name=f"{response}_{suffix}", mode=mode, response=response,
                        lifetime=2.0, hold_seconds=0.5 if response == "unreliable" else 0.0)
            if mode == "refresh":
                case.update(frequency=2.0, nominal_lower=1.5, nominal_upper=2.0)
            cases.append(case)
    for case in cases:
        case.update(minimum=4, target=4, one_way_delay_seconds=0.15)
    return cases


def canonical_jobs(phase):
    """Return the fixed Step 28 smoke or confirm matrix in launch order."""
    if phase not in ("smoke", "confirm"):
        raise ValueError("phase must be smoke or confirm")
    cases = [dict(row) for row in _canonical_cases()
             if row["name"] in ("reliable_single", "reliable_renewed")]
    if phase == "smoke":
        return [dict(case=case, seed=2800, duration=20) for case in cases]
    jobs = []
    for seed in (2801, 2802, 2803):
        order = list(reversed(cases)) if seed == 2802 else cases
        jobs.extend(dict(case=case, seed=seed, duration=120) for case in order)
    return jobs


def _training_delay():
    sigma = math.sqrt(math.log(1.0 + 0.6 ** 2))
    return dict(mu=math.log(1.5) - sigma ** 2 / 2.0, sigma=sigma,
                lower=0.1, upper=4.0)


_FIXED_CONTROLS = {
    "experiment": "step27", "outcome": "grouping", "instrumentation": "detailed",
    "timeout": 12, "request_timeout": 3, "tensor_size": 128,
    "honest_min": 2, "target": 4, "honest_ad_lifetime": 5,
    "allreduce_timeout": 6, "store_delay": 0, "group_bits": "",
    "allow_state_sharing": False, "scheduled_time": "native default",
    "training_delay": _training_delay(), "network": "eight-peer loopback",
    "attempt_unit": "step_start", "event_transport": "batched",
    "trace_batch_size": 32, "duplicate_read_views": False,
    "cached_reads_delayed": False, "tensor_transfer_delayed": False,
    "request_timeout": 3,
    "control_delay_model": "remote find/store request+response; join request+each response",
    "once_unit": "one active publication per native SEARCH; withdrawals separate",
    "leadership_latency_origin": "step_start", "harm_timeout_cap": 12,
}


def _validate_config(config, path, errors):
    if not isinstance(config, dict):
        errors.append("config.json must contain an object")
        return {}, {}, None, None, None
    case = config.get("case")
    if not isinstance(case, dict):
        errors.append("config.case must be an object")
        case = {}
    experiment = config.get("experiment")
    allowed_cases = (_canonical_cases() if experiment == "step27" else
                     [row for row in _canonical_cases()
                      if row["name"] in ("reliable_single", "reliable_renewed")]
                     if experiment == "step28" else [])
    allowed = {row["name"]: row for row in allowed_cases}
    expected_case = allowed.get(case.get("name"))
    if expected_case is None:
        errors.append(f"unknown {experiment or 'research'} case: {case.get('name')!r}")
    elif case != expected_case:
        errors.append(f"config.case differs from the frozen {experiment} arm")
    fixed = dict(_FIXED_CONTROLS)
    if experiment == "step28":
        fixed.update(experiment="step28", clock_monitor=True,
                     clock_jump_limit_seconds=_STEP28_CLOCK_JUMP_LIMIT,
                     submission_cutoff_checked_per_peer=True)
    for key, expected in fixed.items():
        if config.get(key) != expected:
            errors.append(f"{experiment} control {key} differs from the frozen design")
    try:
        seed = int(config.get("seed"))
        if isinstance(config.get("seed"), bool):
            raise ValueError("seed must be an integer")
        duration = finite_number(config.get("duration"), "duration")
        started = finite_number(config.get("started_monotonic"), "started_monotonic")
        expected_durations = (10.0, 60.0) if experiment == "step27" else (20.0, 120.0)
        if duration not in expected_durations:
            errors.append(f"duration must be one of {expected_durations} for {experiment}")
        if experiment == "step28" and (
                (duration == 20.0 and seed != 2800) or
                (duration == 120.0 and seed not in (2801, 2802, 2803))):
            errors.append("Step 28 seed does not match the smoke/confirm duration")
    except (TypeError, ValueError) as exc:
        errors.append(str(exc))
        seed, duration, started = None, None, None
    delay = case.get("one_way_delay_seconds")
    if delay != 0.15 or config.get("one_way_delay_seconds") != 0.15:
        errors.append(f"{experiment} requires a symmetric one-way delay setting of 0.15 seconds")
    if config.get("network_delay_injected") is not True:
        errors.append(f"network_delay_injected must be true for {experiment}")
    if config.get("response_policy") != case.get("response"):
        errors.append("response_policy differs from config.case.response")
    peer_ids = config.get("peer_ids")
    expected_names = {X_NAME, *HONEST_NAMES}
    if not isinstance(peer_ids, dict) or set(peer_ids) != expected_names:
        errors.append("peer_ids must map X and H0..H6")
        peer_ids = {}
    elif (any(not isinstance(value, str) or not value for value in peer_ids.values()) or
          len(set(peer_ids.values())) != 8):
        errors.append("peer_ids must contain eight distinct nonempty identities")
    if seed is not None:
        rng = random.Random(seed)
        honest_offsets = [rng.uniform(0.0, 3.0) for _ in range(7)]
        expected_offsets = dict(X=rng.uniform(0.0, 3.0),
                                **{f"H{i}": value for i, value in enumerate(honest_offsets)})
        if config.get("initial_offsets") != expected_offsets:
            errors.append("initial_offsets differ from the frozen seeded launch schedule")
    return case, peer_ids, seed, duration, started


def _x_publications(native, events, by_ref, x_peer):
    publications = [event for event in events if event["kind"] == "ad_started" and
                    event["peer"] == x_peer and event.get("detail", {}).get("active") is True]
    publications.sort(key=_event_order)
    searches = []
    for row in native.get("searches", []):
        if row.get("peer_id") != x_peer:
            continue
        start_event = by_ref.get(row.get("start_event_ref"))
        if not start_event:
            continue
        searches.append(dict(search_id=row.get("search_id"), step_id=row.get("step_id"),
                             pid=start_event["pid"], start=row.get("start_monotonic"),
                             end=row.get("end_monotonic"), start_ref=row.get("start_event_ref"),
                             end_ref=row.get("end_event_ref")))
    return publications, searches


def _deadline_for(group, events, by_ref, errors):
    assembly_event = by_ref.get(group.get("assembly_event_ref"))
    if assembly_event is None:
        errors.append(f"group {group.get('group_id')} has no raw assembly event")
        return None
    candidates = [event for event in events
                  if event["kind"] == "assembly_deadline_state" and
                  event.get("peer") == assembly_event.get("peer") and
                  event.get("pid") == assembly_event.get("pid") and
                  event.get("detail", {}).get("step_id") == group.get("leader_step_id") and
                  event.get("event_number", -1) < assembly_event.get("event_number", -1)]
    if not candidates:
        errors.append(f"group {group.get('group_id')} has no preceding assembly_deadline_state")
        return None
    candidates.sort(key=lambda event: event["event_number"])
    return candidates[-1]


def _group_rows(native, events, by_ref, config, peer_ids, errors):
    x_peer = peer_ids.get(X_NAME)
    names_by_peer = {peer: name for name, peer in peer_ids.items()}
    started = config.get("started_monotonic")
    duration = config.get("duration")
    cutoff = (float(started) + float(duration)
              if isinstance(started, (int, float)) and isinstance(duration, (int, float)) else None)
    x_publications, x_searches = _x_publications(native, events, by_ref, x_peer)
    x_attempts = [attempt for attempt in native.get("attempts", [])
                  if attempt.get("peer_id") == x_peer]
    native_groups = native.get("groups", [])
    output = []
    for group in native.get("groups", []):
        deadline = _deadline_for(group, events, by_ref, errors)
        assembly_event = by_ref.get(group.get("assembly_event_ref"))
        assembly_time = group.get("assembly_monotonic")
        published = [event for event in x_publications if event["monotonic"] <= assembly_time]
        latest = published[-1] if published else None
        detail = deadline.get("detail", {}) if deadline else {}
        deadline_expiration = detail.get("leader_expiration")
        native_expiration = group.get("leader_expiration")
        honest_peers = sorted(peer for peer in group.get("roster", [])
                              if names_by_peer.get(peer) in HONEST_NAMES)
        raw_members = ((assembly_event or {}).get("detail", {}).get("members", {}))
        member_step_ids = {peer: member.get("step_id")
                           for peer, member in raw_members.items() if isinstance(member, dict)}
        leader_waits = [event for event in events
                        if event["kind"] == "native_leader_wait" and
                        event.get("peer") == group.get("leader_peer_id") and
                        event.get("detail", {}).get("step_id") == group.get("leader_step_id") and
                        event.get("monotonic", float("inf")) <= assembly_time]
        leader_wait = max(leader_waits, key=_event_order, default=None)
        latest_pub_searches = [search for search in x_searches if latest and
                               search["pid"] == latest["pid"] and
                               search["start"] <= latest["monotonic"] <= search["end"]]
        x_searches_at_assembly = [search for search in x_searches
                                  if search["start"] <= assembly_time <= search["end"]]
        x_peer_name = names_by_peer.get(group.get("leader_peer_id"))
        prior_x_steps = [attempt for attempt in x_attempts
                         if attempt.get("started_monotonic") is not None and
                         attempt["started_monotonic"] <= assembly_time]
        current_x_step = max(prior_x_steps, key=lambda attempt: attempt["started_monotonic"], default=None)
        current_x_step_id = current_x_step.get("step_id") if current_x_step else None
        current_x_searches = [search for search in x_searches
                              if search.get("step_id") == current_x_step_id and
                              search["start"] <= assembly_time <= search["end"]]
        x_led_current_step = any(
            native_group.get("leader_peer_id") == x_peer and
            native_group.get("leader_step_id") == current_x_step_id and
            native_group.get("assembly_monotonic") is not None and
            native_group["assembly_monotonic"] <= assembly_time
            for native_group in native_groups)
        step_result_time = current_x_step.get("step_result_monotonic") if current_x_step else None
        if current_x_step is None:
            x_state = "not_started"
        elif step_result_time is not None and step_result_time < assembly_time:
            x_state = "between_rounds"
        elif x_led_current_step:
            x_state = "leading"
        elif current_x_searches:
            x_state = "searching"
        else:
            x_state = "between_searches"
        output.append(dict(
            group_id=group.get("group_id"), leader_name=group.get("leader_name"),
            leader_peer_id=group.get("leader_peer_id"), leader_step_id=group.get("leader_step_id"),
            roster=group.get("roster", []), roster_size=group.get("roster_size"),
            x_in_roster=group.get("x_in_roster"), assembly_event_ref=group.get("assembly_event_ref"),
            assembly_event_refs=group.get("assembly_event_refs", []),
            assembly_monotonic=assembly_time, assembly_wall=group.get("assembly_wall"),
            within_submission_window=(cutoff is not None and started <= assembly_time < cutoff),
            honest_peer_ids=honest_peers,
            honest_peer_names=[names_by_peer[peer] for peer in honest_peers],
            member_step_ids=member_step_ids,
            honest_membership_count=len(honest_peers),
            leader_ad_started_event_ref=group.get("leader_ad_started_event_ref"),
            leader_ad_active=group.get("leader_ad_active"),
            leader_expiration=group.get("leader_expiration"),
            leader_remaining_at_assembly=group.get("leader_remaining_at_assembly"),
            assembly_deadline_event_ref=_ref(deadline) if deadline else None,
            assembly_deadline_state_is_preassembly_snapshot=True,
            leader_expiration_pre_assembly_snapshot=deadline_expiration,
            leader_expiration_at_assembly_deadline=deadline_expiration,
            leader_expiration_changed_since_preassembly_snapshot=(
                deadline_expiration != native_expiration
                if deadline_expiration is not None and native_expiration is not None else None),
            native_leader_wait_event_ref=_ref(leader_wait) if leader_wait else None,
            native_leader_wait_captured_expiration=(leader_wait.get("detail", {}).get("captured_expiration")
                                                    if leader_wait else None),
            x_latest_publication_event_ref=_ref(latest) if latest else None,
            x_latest_publication_expiration=(latest["detail"].get("expiration") if latest else None),
            x_latest_publication_pid=(latest.get("pid") if latest else None),
            x_latest_publication_search_id=(latest_pub_searches[-1]["search_id"]
                                            if latest_pub_searches else None),
            x_step25_group_expiration=group.get("x_expiration"),
            x_step25_group_ad_started_event_ref=group.get("x_ad_started_event_ref"),
            x_step25_group_ad_active=group.get("x_ad_active"),
            x_remaining_at_assembly=group.get("x_remaining_at_assembly"),
            x_publication_search_ids=[row["search_id"] for row in latest_pub_searches],
            x_search_ids_at_assembly=[row["search_id"] for row in x_searches_at_assembly],
            x_current_step_id=current_x_step_id,
            x_current_search_ids=[row["search_id"] for row in current_x_searches],
            x_state_at_assembly=x_state,
            x_is_leader=(x_peer_name == X_NAME),
        ))
    return output


def _cap_latency(value, errors, label):
    if value is None:
        return CAP_SECONDS
    try:
        number = float(value)
    except (TypeError, ValueError):
        errors.append(f"{label} latency is not numeric")
        return CAP_SECONDS
    if number < -1e-9:
        errors.append(f"{label} assembly precedes step_start")
        return CAP_SECONDS
    return min(max(0.0, number), CAP_SECONDS)


def _attempt_rows(native, groups, config, peer_ids, events, by_ref, errors):
    names_by_peer = {peer: name for name, peer in peer_ids.items()}
    group_by_id = {group["group_id"]: group for group in groups}
    started = config.get("started_monotonic")
    duration = config.get("duration")
    cutoff = (float(started) + float(duration)
              if isinstance(started, (int, float)) and isinstance(duration, (int, float)) else None)
    output = []
    for attempt in native.get("attempts", []):
        peer_id, peer_name = attempt.get("peer_id"), attempt.get("peer_name")
        step_id, step_started = attempt.get("step_id"), attempt.get("started_monotonic")
        if step_started is None:
            errors.append(f"attempt {step_id} has no native step_start time")
            continue
        membership_groups = [group for group in groups
                             if peer_id in group.get("roster", []) and
                             group.get("member_step_ids", {}).get(peer_id) == step_id and
                             group.get("assembly_monotonic") is not None and
                             group["assembly_monotonic"] >= step_started]
        membership_groups.sort(key=lambda row: (row["assembly_monotonic"], row["group_id"]))
        first_group = membership_groups[0] if membership_groups else None
        lead_groups = [group for group in groups
                       if group.get("leader_peer_id") == peer_id and
                       group.get("leader_step_id") == step_id and
                       group.get("member_step_ids", {}).get(peer_id) == step_id and
                       group.get("assembly_monotonic") is not None and
                       group["assembly_monotonic"] >= step_started]
        lead_groups.sort(key=lambda row: (row["assembly_monotonic"], row["group_id"]))
        first_lead = lead_groups[0] if lead_groups else None
        first_delta = (first_group["assembly_monotonic"] - step_started if first_group else None)
        lead_delta = (first_lead["assembly_monotonic"] - step_started if first_lead else None)
        submission = (cutoff is not None and started is not None and
                      float(started) <= step_started < cutoff)
        receipt_events = []
        for join in native.get("joins", []):
            if join.get("peer_id") != peer_id or join.get("step_id") != step_id:
                continue
            for reply_ref in join.get("begin_allreduce_reply_refs", []):
                reply = by_ref.get(reply_ref)
                if reply is not None and reply.get("monotonic", -math.inf) >= step_started:
                    receipt_events.append(reply)
        receipt_events.sort(key=_event_order)
        receipt = receipt_events[0] if receipt_events else None
        receipt_group_id = receipt.get("detail", {}).get("group_id") if receipt else None
        receipt_delta = float(receipt["monotonic"] - step_started) if receipt else None
        collective_starts = [event for event in events if event.get("kind") == "allreduce_start" and
                             event.get("peer") == peer_id and
                             event.get("detail", {}).get("step_id") == step_id and
                             event.get("monotonic", -math.inf) >= step_started]
        collective_starts.sort(key=_event_order)
        collective_start = collective_starts[0] if collective_starts else None
        participation_evidence = sorted([*receipt_events, *collective_starts], key=_event_order)
        participation = participation_evidence[0] if participation_evidence else None
        participation_group_id = participation.get("detail", {}).get("group_id") if participation else None
        participation_delta = (float(participation["monotonic"] - step_started)
                               if participation else None)
        begin_diverges = ((first_group is not None) != (receipt is not None) or
                          (first_group is not None and receipt is not None and
                           first_group.get("group_id") != receipt_group_id))
        ready_diverges = ((first_group is not None) != (participation is not None) or
                          (first_group is not None and participation is not None and
                           first_group.get("group_id") != participation_group_id))
        output.append(dict(
            step_id=step_id, peer_id=peer_id, peer_name=peer_name,
            submission_started=submission, started_monotonic=step_started,
            late_start_after_submission_cutoff=(cutoff is not None and step_started >= cutoff),
            started_wall=attempt.get("started_wall"), start_event_ref=attempt.get("start_event_ref"),
            first_assembly_group_id=first_group.get("group_id") if first_group else None,
            first_assembly_event_ref=first_group.get("assembly_event_ref") if first_group else None,
            first_assembly_monotonic=first_group.get("assembly_monotonic") if first_group else None,
            first_assembly_latency_seconds=first_delta,
            capped_latency_seconds=_cap_latency(first_delta, errors, f"{peer_name} {step_id}"),
            first_lead_group_id=first_lead.get("group_id") if first_lead else None,
            first_lead_assembly_event_ref=first_lead.get("assembly_event_ref") if first_lead else None,
            first_lead_assembly_monotonic=first_lead.get("assembly_monotonic") if first_lead else None,
            lead_latency_seconds=lead_delta,
            lead_capped_latency_seconds=_cap_latency(lead_delta, errors,
                                                      f"{peer_name} lead {step_id}"),
            begin_allreduce_received=receipt is not None,
            first_begin_allreduce_group_id=receipt_group_id,
            first_begin_allreduce_reply_event_ref=_ref(receipt) if receipt else None,
            first_begin_allreduce_reply_monotonic=receipt.get("monotonic") if receipt else None,
            begin_receipt_latency_seconds=receipt_delta,
            begin_receipt_capped_latency_seconds=_cap_latency(
                receipt_delta, errors, f"{peer_name} BEGIN receipt {step_id}"),
            participation_evidence_kind=(participation.get("kind") if participation else None),
            participation_evidence_event_ref=_ref(participation) if participation else None,
            participation_evidence_group_id=participation_group_id,
            ready_evidence_kind=(participation.get("kind") if participation else None),
            ready_evidence_event_ref=_ref(participation) if participation else None,
            ready_evidence_group_id=participation_group_id,
            ready_evidence_monotonic=participation.get("monotonic") if participation else None,
            ready_evidence_latency_seconds=participation_delta,
            participation_evidence_latency_seconds=participation_delta,
            participation_evidence_capped_latency_seconds=_cap_latency(
                participation_delta, errors, f"{peer_name} participation evidence {step_id}"),
            allreduce_started=collective_start is not None,
            first_allreduce_start_event_ref=_ref(collective_start) if collective_start else None,
            first_allreduce_start_monotonic=collective_start.get("monotonic") if collective_start else None,
            assembly_roster_vs_begin_receipt_diverges=begin_diverges,
            assembly_roster_vs_ready_evidence_diverges=ready_diverges,
            led_success=first_lead is not None,
            membership_success=first_group is not None,
            native_eventual_outcome=attempt.get("eventual_outcome"),
            search_count=attempt.get("search_count"),
            first_search_event_ref=attempt.get("first_search_event_ref"),
            x_attempt=(peer_name == X_NAME),
        ))
    output.sort(key=lambda row: (row["started_monotonic"], row["peer_id"], row["step_id"]))
    return output


def _annotate_attempt_choices(attempts, native, groups, peer_ids, by_ref):
    """Attach outgoing-join and first-group outcomes to each exact peer/step."""
    x_peer = peer_ids.get(X_NAME)
    joins_by_attempt = defaultdict(list)
    for join in native.get("joins", []):
        joins_by_attempt[(join.get("peer_id"), join.get("step_id"))].append(join)
    for rows in joins_by_attempt.values():
        rows.sort(key=lambda row: (float(row.get("start_monotonic", math.inf)),
                                   str(row.get("start_event_ref") or ""),
                                   str(row.get("join_id") or "")))
    groups_by_id = {row.get("group_id"): row for row in groups}
    joins_output = []

    for attempt in attempts:
        peer_id, step_id = attempt.get("peer_id"), attempt.get("step_id")
        attempt_joins = joins_by_attempt.get((peer_id, step_id), [])
        first = attempt_joins[0] if attempt_joins else None
        join_reply_events = []
        for join in attempt_joins:
            for record in join.get("native_client_records", []):
                event = by_ref.get(record.get("event_ref"))
                if (event is not None and event.get("kind") == "native_client_reply" and
                        event.get("peer") == peer_id and
                        event.get("other") == join.get("leader_peer_id") and
                        event.get("detail", {}).get("join_id") == join.get("join_id") and
                        event.get("detail", {}).get("step_id") == step_id):
                    join_reply_events.append((join, event))
        join_reply_events.sort(key=lambda item: _event_order(item[1]))
        accepted_x = [_ref(event) for join, event in join_reply_events
                      if event.get("detail", {}).get("code") == "ACCEPTED" and
                      join.get("leader_peer_id") == x_peer]
        accepted_other = [_ref(event) for join, event in join_reply_events
                          if event.get("detail", {}).get("code") == "ACCEPTED" and
                          join.get("leader_peer_id") != x_peer]
        first_group = groups_by_id.get(attempt.get("first_assembly_group_id"))
        first_group_leader_id = first_group.get("leader_peer_id") if first_group else None
        first_group_leader_name = first_group.get("leader_name") if first_group else None
        if first_group is None:
            first_group_category = "no_group"
        elif first_group_leader_id == x_peer:
            first_group_category = X_NAME
        else:
            first_group_category = "other"
        destination = (X_NAME if first and first.get("leader_peer_id") == x_peer else
                       "other" if first else "none")
        attempt["first_outgoing_join_id"] = first.get("join_id") if first else None
        attempt["first_outgoing_join_origin"] = first.get("origin") if first else None
        attempt["first_outgoing_join_event_ref"] = first.get("start_event_ref") if first else None
        attempt["first_outgoing_join_selection_event_ref"] = (
            first.get("selection_event_ref") if first else None)
        attempt["first_outgoing_join_peer_id"] = first.get("leader_peer_id") if first else None
        attempt["first_outgoing_join_peer_name"] = first.get("leader_name") if first else None
        attempt["first_outgoing_join_destination"] = destination
        attempt["first_outgoing_choice"] = destination
        attempt["ever_tried_x"] = any(join.get("leader_peer_id") == x_peer
                                      for join in attempt_joins)
        attempt["x_join_attempt_count"] = sum(join.get("leader_peer_id") == x_peer
                                              for join in attempt_joins)
        attempt["accepted_x_reply_count"] = len(accepted_x)
        attempt["accepted_other_reply_count"] = len(accepted_other)
        attempt["accepted_x_reply_event_refs"] = accepted_x
        attempt["accepted_other_reply_event_refs"] = accepted_other
        attempt["first_group_leader_peer_id"] = first_group_leader_id
        attempt["first_group_leader_name"] = first_group_leader_name
        attempt["first_group_leader_category"] = first_group_category
        attempt["first_group_event_ref"] = (first_group.get("assembly_event_ref")
                                             if first_group else None)
        attempt["first_group_latency_seconds"] = attempt.get("first_assembly_latency_seconds")

        for join in attempt_joins:
            records = join.get("native_client_records", [])
            reply_events = []
            request_expirations = []
            accepted_refs, disbanded_refs = [], []
            for record in records:
                event = by_ref.get(record.get("event_ref"))
                if (event is None or event.get("kind") != "native_client_reply" or
                        event.get("peer") != peer_id or
                        event.get("other") != join.get("leader_peer_id") or
                        event.get("detail", {}).get("join_id") != join.get("join_id") or
                        event.get("detail", {}).get("step_id") != step_id):
                    continue
                detail = event.get("detail", {})
                reply_events.append(event)
                if "request_expiration" in detail:
                    request_expirations.append(detail.get("request_expiration"))
                if detail.get("code") == "ACCEPTED":
                    accepted_refs.append(_ref(event))
                if detail.get("code") == "GROUP_DISBANDED":
                    disbanded_refs.append(_ref(event))
            reply_events.sort(key=_event_order)
            start_event_ref = join.get("start_event_ref")
            joins_output.append(dict(
                join_id=join.get("join_id"), peer_id=peer_id,
                peer_name=join.get("peer_name"), leader_peer_id=join.get("leader_peer_id"),
                leader_name=join.get("leader_name"), step_id=step_id,
                search_id=join.get("search_id"), origin=join.get("origin"),
                selection_event_ref=join.get("selection_event_ref"),
                start_event_ref=start_event_ref, join_attempt_start_event_ref=start_event_ref,
                end_event_ref=join.get("end_event_ref"),
                native_client_event_refs=[_ref(event) for event in reply_events],
                accepted_reply_event_refs=accepted_refs,
                group_disbanded_reply_event_refs=disbanded_refs,
                reply_codes=[event.get("detail", {}).get("code") for event in reply_events],
                request_expiration=(request_expirations[0] if request_expirations else None),
                request_expirations=request_expirations,
                selected_leader_expiration=join.get("selected_expiration"),
                first_outgoing_join_for_step=(join is first),
                first_outgoing_join_destination=(destination if join is first else None),
                ever_tried_x_for_step=any(row.get("leader_peer_id") == x_peer
                                          for row in attempt_joins),
            ))
    joins_output.sort(key=lambda row: (row.get("step_id") or "", row.get("peer_id") or "",
                                       row.get("start_event_ref") or ""))
    return joins_output


def _priority_alternatives(selection, peer_ids):
    own = selection.get("own_expiration")
    own_bytes_hex = selection.get("own_peer_bytes_hex")
    try:
        own_exp = math.inf if own is None else float(own)
        own_bytes = bytes.fromhex(own_bytes_hex) if own_bytes_hex else b""
        top_wall = float(selection.get("selection_top_wall"))
    except (TypeError, ValueError):
        return []
    attempted = set()
    for item in selection.get("past_attempts", []):
        if isinstance(item, dict):
            try:
                attempted.add((item.get("peer"), float(item.get("expiration"))))
            except (TypeError, ValueError):
                pass
    eligible = set()
    for item in selection.get("queue", []):
        if not isinstance(item, dict):
            continue
        try:
            peer = item["peer"]
            expiration = float(item["expiration"])
            peer_bytes = bytes.fromhex(item["peer_bytes_hex"])
        except (KeyError, TypeError, ValueError):
            continue
        if (peer != selection.get("peer_id") and expiration > top_wall and
                (peer, expiration) not in attempted and
                (expiration, peer_bytes) <= (own_exp, own_bytes)):
            eligible.add(peer)
    return sorted(eligible)


def _retry_rows(native, events, by_ref, peer_ids, groups):
    x_peer = peer_ids.get(X_NAME)
    x_ads, x_searches = _x_publications(native, events, by_ref, x_peer)
    x_publications_with_search = []
    for publication in x_ads:
        for x_search in x_searches:
            if (x_search["pid"] == publication["pid"] and
                    x_search["start"] <= publication["monotonic"] <= x_search["end"]):
                x_publications_with_search.append((publication, x_search))
    output = []
    for selection in native.get("selections", []):
        peer_id, search_id = selection.get("peer_id"), selection.get("search_id")
        event = by_ref.get(selection.get("event_ref"))
        selected_expiration = selection.get("selected_expiration")
        if (event is None or selection.get("leader_peer_id") != x_peer or
                not selection.get("selection_valid") or selected_expiration is None):
            continue
        try:
            selected_expiration = float(selected_expiration)
            selection_time = float(event["monotonic"])
            top_wall = float(selection.get("selection_top_wall"))
        except (KeyError, TypeError, ValueError):
            continue
        if selected_expiration <= top_wall:
            continue
        prior_pairs = set()
        for item in selection.get("past_attempts", []):
            if isinstance(item, dict) and item.get("peer") == x_peer:
                try:
                    prior_pairs.add((x_peer, float(item.get("expiration"))))
                except (TypeError, ValueError):
                    pass
        if (x_peer, selected_expiration) in prior_pairs:
            continue
        honest_search = next((row for row in native.get("searches", [])
                              if row.get("peer_id") == peer_id and row.get("search_id") == search_id
                              and row.get("start_monotonic") <= selection_time <= row.get("end_monotonic")), None)
        if honest_search is None:
            continue
        past_x = []
        for item in selection.get("past_attempts", []):
            if not isinstance(item, dict) or item.get("peer") != x_peer:
                continue
            try:
                old_expiration = float(item.get("expiration"))
            except (TypeError, ValueError):
                continue
            if old_expiration < selected_expiration:
                past_x.append(old_expiration)
        if not past_x:
            continue
        # A true same-search retry is an actual selection of a newer X tuple,
        # with its old tuple already in past_attempts and both publications in
        # one X SEARCH that remains open at this honest selection.
        candidates = []
        for old_expiration in past_x:
            old_rows = [(pub, search) for pub, search in x_publications_with_search
                        if pub["detail"].get("expiration") == old_expiration and
                        pub["monotonic"] <= selection_time and
                        search["start"] <= selection_time <= search["end"]]
            new_rows = [(pub, search) for pub, search in x_publications_with_search
                        if pub["detail"].get("expiration") == selected_expiration and
                        pub["monotonic"] <= selection_time and
                        search["start"] <= selection_time <= search["end"]]
            for old_pub, x_search in old_rows:
                for new_pub, new_search in new_rows:
                    if (x_search["search_id"] == new_search["search_id"] and
                            x_search["pid"] == new_search["pid"]):
                        candidates.append((old_expiration, old_pub, new_pub, x_search))
        if not candidates:
            continue
        old_expiration, old_publication, new_publication, x_search = max(
            candidates, key=lambda item: (item[0], item[1]["monotonic"]))
        current_pub = next((ad for ad in reversed(x_ads)
                            if ad["monotonic"] <= selection_time), None)
        alternatives = _priority_alternatives(selection, peer_ids)
        x_assemblies = [group for group in groups
                        if group.get("leader_peer_id") == x_peer and
                        group.get("leader_step_id") == x_search.get("step_id")]
        before = any(group.get("assembly_monotonic") <= selection_time for group in x_assemblies)
        after = any(group.get("assembly_monotonic") > selection_time for group in x_assemblies)
        output.append(dict(
            selection_event_ref=selection.get("event_ref"),
            read_event_ref=selection.get("read_event_ref"), peer_id=peer_id,
            search_id=search_id, step_id=selection.get("step_id"),
            honest_search_start_event_ref=honest_search.get("start_event_ref"),
            honest_search_end_event_ref=honest_search.get("end_event_ref"),
            old_pair_expiration=old_expiration,
            old_pair_was_live_at_selection=(old_expiration > top_wall),
            old_pair_publication_event_ref=_ref(old_publication),
            new_pair_expiration=selected_expiration,
            new_pair_publication_event_ref=_ref(new_publication),
            x_search_id=x_search.get("search_id"),
            x_search_step_id=x_search.get("step_id"),
            x_search_start_event_ref=x_search.get("start_ref"),
            x_search_end_event_ref=x_search.get("end_ref"),
            selection_observed_x_expiration=selection.get("x_expiration"),
            selection_read_x_expiration=selection.get("read_x_expiration"),
            latest_published_x_expiration=(current_pub["detail"].get("expiration")
                                           if current_pub else None),
            latest_published_x_event_ref=_ref(current_pub) if current_pub else None,
            alternative_eligible_queued_peers=alternatives,
            alternative_eligible_queue_is_observation=True,
            x_assembly_before_selection=before,
            x_assembly_after_selection=after,
            x_assembly_relative_to_selection=(
                "before_and_after" if before and after else
                "before" if before else "after" if after else "none"),
        ))
    output.sort(key=lambda row: row["selection_event_ref"])
    return output


def _hold_rows(events, peer_ids, case, errors, native=None, by_ref=None):
    x_peer = peer_ids.get(X_NAME)
    event_kinds = ("unreliable_accept", "unreliable_reject", "unreliable_hold_start",
                   "unreliable_disband", "unreliable_release", "unreliable_cancel")
    visits = defaultdict(lambda: defaultdict(list))
    disband_responses = [event for event in events
                         if event.get("kind") == "control_rpc_response" and
                         event.get("detail", {}).get("code") == "GROUP_DISBANDED"]
    linked_response_refs = set()
    linked_native_client_reply_refs = set()
    for event in events:
        if event["kind"] not in event_kinds:
            continue
        detail = event.get("detail", {})
        visit_id = detail.get("visit_id")
        if not isinstance(visit_id, str) or not visit_id:
            errors.append(f"{event['kind']} missing visit_id at {_ref(event)}")
            continue
        if event.get("peer") != x_peer:
            errors.append(f"{event['kind']} emitted by a peer other than X at {_ref(event)}")
        visits[visit_id][event["kind"]].append(event)
    if case.get("response") == "reliable" and visits:
        errors.append("unreliable response events appear in a reliable arm")

    output = []
    for visit_id, kinds in sorted(visits.items()):
        for kind, rows in kinds.items():
            if len(rows) != 1:
                errors.append(f"visit {visit_id} has {len(rows)} {kind} records")
        accepted = kinds.get("unreliable_accept", [])
        rejected = kinds.get("unreliable_reject", [])
        hold = kinds.get("unreliable_hold_start", [])
        disband = kinds.get("unreliable_disband", [])
        release = kinds.get("unreliable_release", [])
        cancelled = kinds.get("unreliable_cancel", [])
        if accepted and rejected:
            errors.append(f"visit {visit_id} has both accept and reject records")
        if accepted:
            if len(release) != 1:
                errors.append(f"accepted visit {visit_id} must have one unreliable_release")
            if len(disband) != 1 and not cancelled:
                errors.append(f"accepted visit {visit_id} must emit disband or cancellation")
            if len(hold) != 1 and not cancelled:
                errors.append(f"accepted visit {visit_id} has no hold start")
        elif hold or disband or release:
            errors.append(f"unaccepted visit {visit_id} has hold/disband/release records")
        common = next((rows[0] for kind, rows in kinds.items() if rows), None)
        detail = common.get("detail", {}) if common else {}
        expected_delta = case.get("hold_seconds", 0.0)
        if case.get("response") == "unreliable":
            try:
                observed_delta = finite_number(detail.get("delta"), "unreliable hold delta")
                if not math.isclose(observed_delta, float(expected_delta), rel_tol=0.0, abs_tol=1e-9):
                    errors.append(f"visit {visit_id} hold delta differs from configured .5 seconds")
            except (TypeError, ValueError) as exc:
                errors.append(f"visit {visit_id}: {exc}")
        hold_start = hold[0] if hold else None
        if disband and cancelled and disband[0]["monotonic"] > cancelled[0]["monotonic"]:
            errors.append(f"visit {visit_id} cancellation precedes emitted disband")
        cancellation_or_disband = [*(disband[:1]), *(cancelled[:1])]
        if release and cancellation_or_disband and release[0]["monotonic"] < max(
                event["monotonic"] for event in cancellation_or_disband):
            errors.append(f"visit {visit_id} release precedes disband/cancellation cleanup")
        terminal_event = max(cancellation_or_disband, key=_event_order, default=None)
        if terminal_event is None:
            terminal_event = next(iter(release or accepted or rejected), None)
        actual_hold = None
        if hold_start and (disband or cancelled):
            end = disband[0] if disband else cancelled[0]
            actual_hold = float(end["monotonic"] - hold_start["monotonic"])
            if end["monotonic"] < hold_start["monotonic"]:
                errors.append(f"visit {visit_id} terminal event precedes hold start")
            if disband and actual_hold + 0.005 < float(expected_delta):
                errors.append(f"visit {visit_id} disband precedes configured hold duration")
        release_duration = (float(release[0]["monotonic"] - hold_start["monotonic"])
                            if hold_start and release else None)
        linked_disband_response = None
        response_link_method = None
        if disband:
            rpc_id = disband[0].get("detail", {}).get("rpc_id")
            if isinstance(rpc_id, str) and rpc_id:
                candidates = [event for event in disband_responses
                              if _ref(event) not in linked_response_refs and
                              event.get("pid") == disband[0].get("pid") and
                              event.get("detail", {}).get("rpc_id") == rpc_id and
                              event.get("peer") == disband[0].get("peer") and
                              event.get("other") == disband[0].get("other") and
                              event.get("monotonic", -math.inf) >= disband[0]["monotonic"]]
                response_link_method = "exact pid and server join rpc_id"
            else:
                next_same_caller_accept = min((event["monotonic"] for event in events
                                                if event.get("kind") == "unreliable_accept" and
                                                event.get("peer") == disband[0].get("peer") and
                                                event.get("other") == disband[0].get("other") and
                                                event.get("monotonic", -math.inf) > disband[0]["monotonic"]),
                                               default=math.inf)
                candidates = [event for event in disband_responses
                              if _ref(event) not in linked_response_refs and
                              event.get("pid") == disband[0].get("pid") and
                              event.get("peer") == disband[0].get("peer") and
                              event.get("other") == disband[0].get("other") and
                              disband[0]["monotonic"] <= event.get("monotonic", -math.inf) <
                              next_same_caller_accept]
                response_link_method = "bounded same-caller fallback; rpc_id absent"
            if candidates:
                linked_disband_response = min(candidates, key=_event_order)
                linked_response_refs.add(_ref(linked_disband_response))
        native_client_receipt = None
        native_client_accept = None
        native_client_join_id = None
        if disband and native is not None:
            caller = common.get("other") if common else None
            leader = common.get("peer") if common else None
            request_expiration = detail.get("request_expiration")
            accepted_time = accepted[0].get("monotonic", -math.inf) if accepted else -math.inf
            next_accept = min((event.get("monotonic", math.inf) for event in events
                               if event.get("kind") == "unreliable_accept" and
                               event.get("peer") == leader and event.get("other") == caller and
                               event.get("monotonic", -math.inf) > accepted_time),
                              default=math.inf)
            matching_pairs = []
            for join in native.get("joins", []):
                if (join.get("peer_id") != caller or join.get("leader_peer_id") != leader or
                        join.get("start_monotonic", math.inf) > disband[0]["monotonic"] or
                        not join.get("join_id")):
                    continue
                replies = []
                for record in join.get("native_client_records", []):
                    reply = (by_ref or {}).get(record.get("event_ref"))
                    reply_detail = reply.get("detail", {}) if reply else {}
                    if (reply is None or reply.get("kind") != "native_client_reply" or
                            reply.get("peer") != caller or reply.get("other") != leader or
                            reply_detail.get("join_id") != join.get("join_id") or
                            reply_detail.get("request_expiration") != request_expiration):
                        continue
                    replies.append(reply)
                accepted_replies = [reply for reply in replies
                                    if reply.get("detail", {}).get("code") == "ACCEPTED" and
                                    accepted_time <= reply.get("monotonic", -math.inf) <=
                                    disband[0].get("monotonic", math.inf)]
                disbanded_replies = [reply for reply in replies
                                     if reply.get("detail", {}).get("code") == "GROUP_DISBANDED" and
                                     disband[0].get("monotonic", math.inf) <=
                                     reply.get("monotonic", -math.inf) < next_accept]
                for accepted_reply in accepted_replies:
                    for disbanded_reply in disbanded_replies:
                        if (accepted_reply.get("monotonic", math.inf) <=
                                disbanded_reply.get("monotonic", -math.inf) and
                                _ref(disbanded_reply) not in linked_native_client_reply_refs):
                            matching_pairs.append((join, accepted_reply, disbanded_reply))
            if len(matching_pairs) > 1:
                errors.append(f"visit {visit_id} has ambiguous native client ACCEPTED/DISBANDED join receipt")
            if matching_pairs:
                join, native_client_accept, native_client_receipt = min(
                    matching_pairs, key=lambda row: _event_order(row[2]))
                native_client_join_id = join.get("join_id")
                linked_native_client_reply_refs.add(_ref(native_client_receipt))
        output.append(dict(
            visit_id=visit_id, leader_peer_id=common.get("peer") if common else None,
            caller_peer_id=common.get("other") if common else None,
            leader_step_id=detail.get("leader_step_id"), leader_search_id=detail.get("leader_search_id"),
            request_expiration=detail.get("request_expiration"), configured_hold_seconds=expected_delta,
            accepted_event_ref=_ref(accepted[0]) if accepted else None,
            rejected_event_ref=_ref(rejected[0]) if rejected else None,
            hold_start_event_ref=_ref(hold_start) if hold_start else None,
            disband_event_ref=_ref(disband[0]) if disband else None,
            disband_control_response_event_ref=(_ref(linked_disband_response)
                                                if linked_disband_response else None),
            disband_control_response_emitted=linked_disband_response is not None,
            disband_control_response_rpc_id=(linked_disband_response.get("detail", {}).get("rpc_id")
                                             if linked_disband_response else None),
            disband_control_response_link_method=response_link_method,
            disband_native_client_reply_event_ref=(_ref(native_client_receipt)
                                                   if native_client_receipt else None),
            disband_received_by_native_client=native_client_receipt is not None,
            disband_native_client_join_id=native_client_join_id,
            disband_native_client_accepted_reply_event_ref=(
                _ref(native_client_accept) if native_client_accept else None),
            disband_native_client_request_expiration=(
                native_client_receipt.get("detail", {}).get("request_expiration")
                if native_client_receipt else None),
            disband_native_client_link_method=(
                "same caller/leader/client join_id and raw request_expiration with temporal ACCEPTED/DISBANDED"
                if native_client_receipt else None),
            cancellation_event_ref=_ref(cancelled[0]) if cancelled else None,
            release_event_ref=_ref(release[0]) if release else None,
            actual_hold_duration_seconds=actual_hold,
            hold_completed=bool(disband),
            hold_completed_at_disband_emission=bool(disband),
            registered_duration_to_release_seconds=release_duration,
            terminal_event_ref=_ref(terminal_event) if terminal_event else None,
            terminal_kind=(terminal_event["kind"] if terminal_event else None),
        ))
    output.sort(key=lambda row: (row["leader_step_id"] or "", row["visit_id"]))
    return output


def _trial_metrics(native, attempts, groups, retries, holds, config, peer_ids):
    # Every actual step_start is an attempt. Starts just beyond the configured
    # cutoff stay in the denominator and are counted separately as boundary slips.
    x_attempts = [row for row in attempts if row["peer_name"] == X_NAME]
    honest_attempts = [row for row in attempts if row["peer_name"] in HONEST_NAMES]
    x_latency = [row["lead_latency_seconds"] for row in x_attempts
                 if row["lead_latency_seconds"] is not None]
    x_capped = [row["lead_capped_latency_seconds"] for row in x_attempts]
    x_led = [row for row in x_attempts if row["led_success"]]
    x_success_latencies = [row["lead_latency_seconds"] for row in x_led
                           if row.get("lead_latency_seconds") is not None]
    honest_capped = [row["capped_latency_seconds"] for row in honest_attempts]
    honest_success_latencies = [row["first_group_latency_seconds"] for row in honest_attempts
                                if row.get("membership_success") and
                                row.get("first_group_latency_seconds") is not None]
    honest_begin_capped = [row["begin_receipt_capped_latency_seconds"] for row in honest_attempts]
    honest_ready_capped = [row["participation_evidence_capped_latency_seconds"]
                           for row in honest_attempts]
    honest_groups = [row for row in groups if row["within_submission_window"]]
    memberships = sum(row["honest_membership_count"] for row in honest_groups)
    duration = float(config.get("duration", 0.0))
    peer_minutes_per_peer = duration / 60.0
    total_honest_peer_minutes = len(HONEST_NAMES) * peer_minutes_per_peer
    x_led_groups = [row for row in groups if row.get("leader_name") == X_NAME]
    x_led_groups_window = [row for row in x_led_groups if row["within_submission_window"]]
    eventual = Counter(row.get("native_eventual_outcome") for row in x_attempts)
    native_selections = native.get("selections", [])
    first_eligible = [row for row in native_selections
                      if row.get("first_eligible") and row.get("peer_name") in HONEST_NAMES]
    x_chosen = sum(row.get("chose_X") is True for row in first_eligible)
    memberships_by_peer = {
        name: sum(name in row.get("honest_peer_names", []) for row in honest_groups)
        for name in HONEST_NAMES
    }
    honest_group_x = sum(row.get("first_group_leader_category") == X_NAME
                         for row in honest_attempts)
    honest_group_other = sum(row.get("first_group_leader_category") == "other"
                             for row in honest_attempts)
    honest_no_group = sum(row.get("first_group_leader_category") == "no_group"
                          for row in honest_attempts)
    honest_first_choice_x = sum(row.get("first_outgoing_join_destination") == X_NAME
                                for row in honest_attempts)
    honest_first_choice_other = sum(row.get("first_outgoing_join_destination") == "other"
                                    for row in honest_attempts)
    honest_no_outgoing_choice = sum(row.get("first_outgoing_join_destination") == "none"
                                    for row in honest_attempts)
    honest_ever_tried_x = sum(row.get("ever_tried_x") is True for row in honest_attempts)
    honest_accepted_x_replies = sum(row.get("accepted_x_reply_count", 0)
                                    for row in honest_attempts)
    honest_accepted_other_replies = sum(row.get("accepted_other_reply_count", 0)
                                        for row in honest_attempts)
    honest_per_peer = {}
    joins = native.get("joins", [])
    for name in HONEST_NAMES:
        peer_id = peer_ids.get(name)
        peer_attempts = [row for row in honest_attempts if row.get("peer_name") == name]
        peer_capped = [row["capped_latency_seconds"] for row in peer_attempts]
        peer_begin_capped = [row["begin_receipt_capped_latency_seconds"] for row in peer_attempts]
        peer_ready_capped = [row["participation_evidence_capped_latency_seconds"]
                             for row in peer_attempts]
        peer_retries = [row for row in retries if row.get("peer_id") == peer_id]
        peer_x_visits = [row for row in joins if row.get("peer_id") == peer_id and
                         row.get("leader_peer_id") == peer_ids.get(X_NAME)]
        peer_no_group = sum(not row["membership_success"] for row in peer_attempts)
        receipt_count = sum(row["begin_allreduce_received"] for row in peer_attempts)
        peer_outcomes = Counter(row.get("native_eventual_outcome") for row in peer_attempts)
        honest_per_peer[name] = dict(
            peer_id=peer_id, attempts=len(peer_attempts),
            mean_capped_latency_seconds=_mean(peer_capped),
            median_capped_latency_seconds=_median(peer_capped),
            p90_capped_latency_seconds=_percentile(peer_capped, 0.9),
            mean_begin_receipt_capped_latency_seconds=_mean(peer_begin_capped),
            mean_ready_evidence_capped_latency_seconds=_mean(peer_ready_capped),
            no_group_attempts=peer_no_group,
            no_group_fraction=(peer_no_group / len(peer_attempts) if peer_attempts else None),
            native_outcome_counts=dict(peer_outcomes),
            native_step_attempts=len(peer_attempts), x_visits=len(peer_x_visits),
            same_search_x_retries=len(peer_retries),
            submission_window_attempts=sum(row["submission_started"] for row in peer_attempts),
            group_memberships_in_submission_window=memberships_by_peer[name],
            memberships_per_peer_minute=(memberships_by_peer[name] / peer_minutes_per_peer
                                         if peer_minutes_per_peer else None),
            begin_allreduce_receipts=receipt_count,
            receipt_participation_rate=(receipt_count / len(peer_attempts) if peer_attempts else None),
            roster_vs_begin_receipt_divergences=sum(
                row["assembly_roster_vs_begin_receipt_diverges"] for row in peer_attempts),
            roster_vs_ready_evidence_divergences=sum(
                row["assembly_roster_vs_ready_evidence_diverges"] for row in peer_attempts),
        )
    return dict(
        x_submitted_attempts=len(x_attempts), x_led_attempts=len(x_led),
        x_submission_window_attempts=sum(row["submission_started"] for row in x_attempts),
        x_late_start_attempts=sum(row["late_start_after_submission_cutoff"] for row in x_attempts),
        x_attempt_denominator_includes_all_raw_step_starts=True,
        all_late_start_attempts=sum(row["late_start_after_submission_cutoff"] for row in attempts),
        x_followed_attempts=eventual["followed"], x_no_group_attempts=eventual["no_group"],
        x_failed_attempts=eventual["failed"],
        x_led_rate=(len(x_led) / len(x_attempts) if x_attempts else None),
        **{f"x_led_by_{threshold}s_count": sum(
            row["lead_latency_seconds"] is not None and
            row["lead_latency_seconds"] <= float(threshold) for row in x_attempts)
           for threshold in (2, 3, 5, 12)},
        **{f"x_led_by_{threshold}s_rate": (
            sum(row["lead_latency_seconds"] is not None and
                row["lead_latency_seconds"] <= float(threshold) for row in x_attempts) /
            len(x_attempts) if x_attempts else None)
           for threshold in (2, 3, 5, 12)},
        x_late_first_assembly_after_12s_count=sum(
            row["lead_latency_seconds"] is not None and row["lead_latency_seconds"] > 12.0
            for row in x_attempts),
        x_no_first_assembly_count=sum(row["lead_latency_seconds"] is None for row in x_attempts),
        x_lead_latency_cdf_denominator=len(x_attempts),
        x_lead_latency_cdf_definition=(
            "observed raw own-step X-led assembly latency within each threshold divided by all raw X "
            "step_start attempts; missing leadership remains in the denominator and is not counted "
            "as an observed success"),
        x_success_only_lead_latency_count=len(x_success_latencies),
        x_success_only_mean_first_lead_latency_seconds=_mean(x_success_latencies),
        x_success_only_median_first_lead_latency_seconds=_median(x_success_latencies),
        x_success_only_p90_first_lead_latency_seconds=_percentile(x_success_latencies, 0.9),
        x_led_latency_successes=len(x_latency),
        x_mean_first_lead_latency_seconds=_mean(x_latency),
        x_median_first_lead_latency_seconds=_median(x_latency),
        x_led_latency_median_seconds=_median(x_latency),
        x_led_latency_p90_seconds=_percentile(x_latency, 0.9),
        x_mean_capped_latency_seconds=_mean(x_capped),
        x_median_capped_latency_seconds=_median(x_capped),
        x_p90_capped_latency_seconds=_percentile(x_capped, 0.9),
        x_capped_latency_definition="all raw X step_start attempts; min(first own-led assembly monotonic - step_start monotonic, 12); absent assembly is 12",
        honest_submitted_attempts=len(honest_attempts),
        honest_submission_window_attempts=sum(row["submission_started"] for row in honest_attempts),
        honest_assembled_attempts=sum(row["membership_success"] for row in honest_attempts),
        honest_no_group_attempts=sum(not row["membership_success"] for row in honest_attempts),
        honest_first_group_x_led_attempts=honest_group_x,
        honest_first_group_other_led_attempts=honest_group_other,
        honest_first_group_x_led_rate=(honest_group_x / len(honest_attempts)
                                       if honest_attempts else None),
        honest_first_group_other_led_rate=(honest_group_other / len(honest_attempts)
                                           if honest_attempts else None),
        honest_first_group_success_fraction=(
            sum(row["membership_success"] for row in honest_attempts) / len(honest_attempts)
            if honest_attempts else None),
        honest_first_choice_x_attempts=honest_first_choice_x,
        honest_first_choice_other_attempts=honest_first_choice_other,
        honest_no_outgoing_choice_attempts=honest_no_outgoing_choice,
        honest_ever_tried_x_attempts=honest_ever_tried_x,
        honest_accepted_x_reply_count=honest_accepted_x_replies,
        honest_accepted_other_reply_count=honest_accepted_other_replies,
        honest_first_choice_x_rate=(honest_first_choice_x / len(honest_attempts)
                                    if honest_attempts else None),
        honest_first_choice_other_rate=(honest_first_choice_other / len(honest_attempts)
                                        if honest_attempts else None),
        honest_no_outgoing_choice_rate=(honest_no_outgoing_choice / len(honest_attempts)
                                        if honest_attempts else None),
        honest_ever_tried_x_rate=(honest_ever_tried_x / len(honest_attempts)
                                  if honest_attempts else None),
        honest_success_only_first_group_latency_count=len(honest_success_latencies),
        honest_success_only_mean_first_group_latency_seconds=_mean(honest_success_latencies),
        honest_success_only_median_first_group_latency_seconds=_median(honest_success_latencies),
        honest_success_only_p90_first_group_latency_seconds=_percentile(
            honest_success_latencies, 0.9),
        honest_no_group_fraction=(sum(not row["membership_success"] for row in honest_attempts) /
                                  len(honest_attempts) if honest_attempts else None),
        honest_mean_capped_latency_seconds=_mean(honest_capped),
        honest_median_capped_latency_seconds=_median(honest_capped),
        honest_p90_capped_latency_seconds=_percentile(honest_capped, 0.9),
        honest_capped_latency_definition="min(first assembled group containing the peer - step_start, 12); absent assembly is 12",
        honest_mean_begin_receipt_capped_latency_seconds=_mean(honest_begin_capped),
        honest_begin_receipt_median_capped_latency_seconds=_median(honest_begin_capped),
        honest_begin_receipt_p90_capped_latency_seconds=_percentile(honest_begin_capped, 0.9),
        honest_mean_ready_capped_latency_seconds=_mean(honest_ready_capped),
        honest_ready_median_capped_latency_seconds=_median(honest_ready_capped),
        honest_ready_p90_capped_latency_seconds=_percentile(honest_ready_capped, 0.9),
        honest_participation_evidence_attempts=sum(row["participation_evidence_event_ref"] is not None
                                                   for row in honest_attempts),
        honest_begin_allreduce_receipt_attempts=sum(row["begin_allreduce_received"]
                                                    for row in honest_attempts),
        honest_assembly_roster_vs_begin_receipt_divergence_count=sum(
            row["assembly_roster_vs_begin_receipt_diverges"] for row in honest_attempts),
        honest_assembly_roster_vs_ready_evidence_divergence_count=sum(
            row["assembly_roster_vs_ready_evidence_diverges"] for row in honest_attempts),
        honest_roster_only_attempts=sum(row["membership_success"] and not row["begin_allreduce_received"]
                                        for row in honest_attempts),
        honest_begin_without_roster_attempts=sum(row["begin_allreduce_received"] and
                                                 not row["membership_success"]
                                                 for row in honest_attempts),
        total_groups=len(groups), x_led_groups=len(x_led_groups),
        x_led_group_share=(len(x_led_groups) / len(groups)
                           if groups else None),
        x_led_groups_in_submission_window=len(x_led_groups_window),
        x_led_groups_per_submission_minute=(len(x_led_groups_window) / peer_minutes_per_peer
                                            if peer_minutes_per_peer else None),
        cohort_groups_per_submission_minute_including_drain=(len(groups) / peer_minutes_per_peer
                                                             if peer_minutes_per_peer else None),
        honest_submission_window_groups=len(honest_groups),
        honest_group_memberships_in_submission_window=memberships,
        honest_peer_minutes=total_honest_peer_minutes,
        honest_peer_minutes_per_peer=peer_minutes_per_peer,
        honest_group_memberships_per_honest_peer_minute=(memberships / total_honest_peer_minutes
                                                         if total_honest_peer_minutes else None),
        honest_group_memberships_by_peer=memberships_by_peer,
        honest_group_memberships_per_peer_minute_by_peer={
            name: (count / (float(config.get("duration", 0.0)) / 60.0)
                   if float(config.get("duration", 0.0)) > 0 else None)
            for name, count in memberships_by_peer.items()},
        honest_per_peer=honest_per_peer,
        first_eligible_honest_choices=len(first_eligible),
        first_eligible_honest_chose_x=x_chosen,
        first_eligible_honest_chose_x_rate=(x_chosen / len(first_eligible) if first_eligible else None),
        same_search_x_retry_count=len(retries),
        same_search_x_retry_old_pairs=len({(row["peer_id"], row["search_id"], row["old_pair_expiration"])
                                           for row in retries}),
        unreliable_accept_count=sum(bool(row["accepted_event_ref"]) for row in holds),
        unreliable_reject_count=sum(bool(row["rejected_event_ref"]) for row in holds),
        unreliable_hold_start_count=sum(bool(row["hold_start_event_ref"]) for row in holds),
        unreliable_disband_count=sum(bool(row["disband_event_ref"]) for row in holds),
        unreliable_disband_emitted_count=sum(bool(row["disband_event_ref"]) for row in holds),
        unreliable_disband_count_definition="emitted unreliable_disband events; does not imply client receipt",
        unreliable_disband_control_response_emitted_count=sum(
            bool(row["disband_control_response_event_ref"]) for row in holds),
        unreliable_disband_native_client_receipt_count=sum(
            bool(row["disband_native_client_reply_event_ref"]) for row in holds),
        unreliable_release_count=sum(bool(row["release_event_ref"]) for row in holds),
        unreliable_cancel_count=sum(bool(row["cancellation_event_ref"]) for row in holds),
        unreliable_completed_hold_durations_seconds=[row["actual_hold_duration_seconds"] for row in holds
                                                      if row.get("hold_completed")],
        unreliable_completed_hold_median_seconds=_median([row["actual_hold_duration_seconds"] for row in holds
                                                          if row.get("hold_completed")]),
        delay_activity_coverage={},
    )


def analyse_trial(path) -> dict:
    """Analyze one Step 28 trial or replay an existing Step 27 raw trial."""
    path = Path(path)
    config = read_json(path / "config.json")
    errors = []
    case, peer_ids, seed, duration, started = _validate_config(config, path, errors)
    native = analyse_trial_choices(path)
    events = _event_stream(path)
    by_kind, by_ref = _event_groups(events)
    clock = _clock_integrity(events, errors, config.get("experiment") == "step28")
    x_peer = peer_ids.get(X_NAME)

    rpc = _rpc_integrity(case, config, events, by_kind, by_ref, native)
    policy = _step26._policy_integrity(case, config, events, by_kind, by_ref, native)
    errors.extend(native.get("trace_integrity_errors", []))
    errors.extend(native.get("step24_control_integrity_errors", []))
    errors.extend(f"rpc: {error}" for error in rpc.get("errors", []))
    errors.extend(f"policy: {error}" for error in policy.get("errors", []))
    _guard_new_control_tail(rpc, events, errors)
    trial_summary = read_json(path / "summary.json") if (path / "summary.json").is_file() else {}
    if trial_summary.get("error") is not None or trial_summary.get("pending"):
        errors.append("trial summary reports an error or pending peers")
    trace_closed = [event for event in events if event.get("kind") == "trace_closed"]
    if len(trace_closed) != 1 or (events and events[-1].get("kind") != "trace_closed"):
        errors.append("raw event trace must end with one trace_closed event")

    # The attacker must not issue outbound joins. Server-side join RPCs retain
    # their normal source/destination identity and are checked as well.
    x_join_starts = [event for event in by_kind.get("join_attempt_start", [])
                     if event.get("peer") == x_peer]
    if x_join_starts:
        errors.append(f"X made {len(x_join_starts)} outbound native join attempts")
    x_join_rpcs = [row for row in rpc.get("summary", {}).get("rpc_rows", [])
                   if row.get("rpc_type") == "join" and row.get("source_peer") == x_peer]
    if x_join_rpcs:
        errors.append(f"X initiated {len(x_join_rpcs)} outbound control join RPCs")

    groups = _group_rows(native, events, by_ref, config, peer_ids, errors)
    attempts = _attempt_rows(native, groups, config, peer_ids, events, by_ref, errors)
    joins = _annotate_attempt_choices(attempts, native, groups, peer_ids, by_ref)
    retries = _retry_rows(native, events, by_ref, peer_ids, groups)
    holds = _hold_rows(events, peer_ids, case, errors, native, by_ref)
    metrics = _trial_metrics(native, attempts, groups, retries, holds, config, peer_ids)
    metrics["clock_integrity"] = clock
    metrics["conditional_first_eligible_choice_count"] = len(first_eligible := [
        row for row in native.get("selections", [])
        if row.get("first_eligible") and row.get("peer_name") in HONEST_NAMES])
    metrics["conditional_first_eligible_chose_x_count"] = sum(
        row.get("chose_X") is True for row in first_eligible)
    metrics["conditional_first_eligible_chose_x_rate"] = (
        metrics["conditional_first_eligible_chose_x_count"] /
        len(first_eligible) if first_eligible else None)
    if config.get("experiment") == "step28":
        # Keep the conditional-per-search selection view explicitly secondary.
        for key in ("first_eligible_honest_choices", "first_eligible_honest_chose_x",
                    "first_eligible_honest_chose_x_rate"):
            metrics.pop(key, None)
    metrics.update(_step26._compact_cadence(native))
    metrics.update(policy.get("summary", {}))
    policy_summary = policy.get("summary", {})
    metrics["advertisement_write_cost"] = {
        key: policy_summary.get(key) for key in (
            "active_publication_attempts", "accepted_active_declaration_calls",
            "cancelled_declaration_calls", "withdrawal_calls",
            "accepted_active_holder_stores", "active_holder_store_records")
    }
    x_publications, x_searches = _x_publications(native, events, by_ref, x_peer)
    publications_by_pid = defaultdict(list)
    for event in x_publications:
        publications_by_pid[event["pid"]].append(event)
    publication_periods, search_publication_cadence = [], []
    for search in x_searches:
        ads = [event for event in publications_by_pid[search["pid"]]
               if search["start"] <= event["monotonic"] <= search["end"]]
        intervals = [right["monotonic"] - left["monotonic"]
                     for left, right in zip(ads, ads[1:])]
        publication_periods.extend(intervals)
        median_interval = _median(intervals)
        search_publication_cadence.append(dict(
            search_id=search["search_id"], step_id=search["step_id"],
            start_event_ref=search["start_ref"], end_event_ref=search["end_ref"],
            active_publication_count=len(ads),
            active_publication_event_refs=[_ref(event) for event in ads],
            active_publication_interval_count=len(intervals),
            active_publication_interval_median_seconds=median_interval,
            active_publication_interval_p90_seconds=_percentile(intervals, 0.9),
            achieved_publication_frequency_per_second=(1.0 / median_interval
                                                       if median_interval and median_interval > 0 else None)))
    overall_period = _median(publication_periods)
    metrics["requested_refresh_frequency_per_second"] = case.get("frequency")
    metrics["achieved_active_publication_frequency_per_second"] = (
        1.0 / overall_period if overall_period and overall_period > 0 else None)
    metrics["active_publication_interval_median_seconds"] = overall_period
    metrics["active_publication_interval_p90_seconds"] = _percentile(publication_periods, 0.9)
    metrics["active_publication_interval_count"] = len(publication_periods)
    metrics["x_active_publications_by_search"] = search_publication_cadence
    metrics["x_time_state"] = _step26._x_state(native, config, events)
    coverage = rpc.get("summary", {}).get("coverage", {})
    metrics["delay_activity_coverage"] = coverage
    metrics["rpc_counts"] = rpc.get("summary", {}).get("rpc_counts", {})
    metrics["delayed_completed_rpc_legs"] = rpc.get("summary", {}).get("delayed_completed_legs", 0)
    metrics["unlinked_find_rpc_count"] = rpc.get("summary", {}).get("unlinked_find_rpc_count", 0)
    metrics["configured_one_way_delay_seconds"] = case.get("one_way_delay_seconds")
    metrics["response_policy"] = case.get("response")
    metrics["mode"] = case.get("mode")
    metrics["lifetime_seconds"] = case.get("lifetime")
    metrics["refresh_frequency_per_second"] = case.get("frequency")
    integrity_errors = sorted(set(errors))
    rpc_summary = {key: value for key, value in rpc.get("summary", {}).items()
                   if key != "rpc_rows"}
    return dict(
        experiment=config.get("experiment"), trial_dir=path.name, case=case, seed=seed, duration=duration,
        integrity_pass=bool(native.get("control_plane_pass") and not integrity_errors),
        integrity_errors=integrity_errors,
        native_control_plane_pass=native.get("control_plane_pass"),
        trace_integrity_pass=native.get("trace_integrity_pass"),
        policy_integrity=dict(pass_=policy.get("pass_") is True, errors=policy.get("errors", [])),
        delay_integrity=dict(pass_=rpc.get("pass_") is True, errors=rpc.get("errors", []),
                             summary=rpc_summary),
        summary_metrics=metrics, leadership_summary=metrics, attempts=attempts, groups=groups,
        retries=retries, holds=holds, choices=native.get("selections", []), joins=joins)


def _json_csv_value(value):
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return value


def _write_csv(path, rows, preferred=()):
    rows = list(rows)
    fields, seen = list(preferred), set(preferred)
    for row in rows:
        for key in row:
            if key not in seen:
                fields.append(key)
                seen.add(key)
    if not fields:
        fields = list(preferred)
    with Path(path).open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _json_csv_value(row.get(key)) for key in fields})


def _difference(left, right):
    if left is None or right is None:
        return None
    return float(left) - float(right)


def _flatten_trial(trial):
    row = dict(trial_dir=trial.get("trial_dir"), case=trial.get("case", {}).get("name"),
               seed=trial.get("seed"), mode=trial.get("case", {}).get("mode"),
               response=trial.get("case", {}).get("response"),
               lifetime=trial.get("case", {}).get("lifetime"),
               frequency=trial.get("case", {}).get("frequency"),
               duration=trial.get("duration"), integrity_pass=trial.get("integrity_pass"))
    for key, value in trial.get("summary_metrics", {}).items():
        if not isinstance(value, (dict, list)):
            row[key] = value
    return row


def _contrast_for_seed(seed, by_case):
    single = by_case.get("reliable_single")
    renewed = by_case.get("reliable_renewed")
    if single is None or renewed is None:
        return dict(seed=seed, complete=False)
    sm = single["summary_metrics"]
    rm = renewed["summary_metrics"]
    x_attempt_delta = _difference(rm.get("x_led_attempts"), sm.get("x_led_attempts"))
    x_rate_delta = _difference(rm.get("x_led_rate"), sm.get("x_led_rate"))
    honest_success_delta = _difference(rm.get("honest_first_group_success_fraction"),
                                       sm.get("honest_first_group_success_fraction"))
    honest_x_led_count_delta = _difference(rm.get("honest_first_group_x_led_attempts"),
                                           sm.get("honest_first_group_x_led_attempts"))
    honest_x_lead_delta = _difference(rm.get("honest_first_group_x_led_rate"),
                                      sm.get("honest_first_group_x_led_rate"))
    honest_memberships_delta = _difference(rm.get("honest_group_memberships_in_submission_window"),
                                           sm.get("honest_group_memberships_in_submission_window"))
    honest_capped_delta = _difference(rm.get("honest_mean_capped_latency_seconds"),
                                      sm.get("honest_mean_capped_latency_seconds"))
    honest_success_time_delta = _difference(
        rm.get("honest_success_only_mean_first_group_latency_seconds"),
        sm.get("honest_success_only_mean_first_group_latency_seconds"))
    x_capped_delta = _difference(rm.get("x_mean_capped_latency_seconds"),
                                 sm.get("x_mean_capped_latency_seconds"))
    return dict(
        seed=seed, complete=True,
        reliable_single_x_attempts=sm.get("x_submitted_attempts", 0),
        reliable_renewed_x_attempts=rm.get("x_submitted_attempts", 0),
        reliable_single_x_led_attempts=sm.get("x_led_attempts", 0),
        reliable_renewed_x_led_attempts=rm.get("x_led_attempts", 0),
        reliable_single_x_led_rate=sm.get("x_led_rate"),
        reliable_renewed_x_led_rate=rm.get("x_led_rate"),
        delta_x_led_attempts_renewed_minus_single=x_attempt_delta,
        delta_x_led_rate_renewed_minus_single=x_rate_delta,
        reliable_single_honest_attempts=sm.get("honest_submitted_attempts", 0),
        reliable_renewed_honest_attempts=rm.get("honest_submitted_attempts", 0),
        reliable_single_honest_first_group_success_fraction=(
            sm.get("honest_first_group_success_fraction")),
        reliable_renewed_honest_first_group_success_fraction=(
            rm.get("honest_first_group_success_fraction")),
        delta_honest_first_group_success_fraction_renewed_minus_single=honest_success_delta,
        reliable_single_honest_first_group_x_led_rate=(sm.get("honest_first_group_x_led_rate")),
        reliable_renewed_honest_first_group_x_led_rate=(rm.get("honest_first_group_x_led_rate")),
        delta_honest_first_group_x_led_attempts_renewed_minus_single=honest_x_led_count_delta,
        delta_honest_first_group_x_led_rate_renewed_minus_single=honest_x_lead_delta,
        reliable_single_honest_group_memberships_in_submission_window=(
            sm.get("honest_group_memberships_in_submission_window", 0)),
        reliable_renewed_honest_group_memberships_in_submission_window=(
            rm.get("honest_group_memberships_in_submission_window", 0)),
        delta_honest_group_memberships_renewed_minus_single=honest_memberships_delta,
        reliable_single_honest_mean_capped_latency_seconds=(
            sm.get("honest_mean_capped_latency_seconds")),
        reliable_renewed_honest_mean_capped_latency_seconds=(
            rm.get("honest_mean_capped_latency_seconds")),
        delta_honest_mean_capped_latency_seconds_renewed_minus_single=honest_capped_delta,
        delta_honest_success_only_mean_first_group_latency_seconds_renewed_minus_single=(
            honest_success_time_delta),
        reliable_single_x_mean_capped_latency_seconds=sm.get("x_mean_capped_latency_seconds"),
        reliable_renewed_x_mean_capped_latency_seconds=rm.get("x_mean_capped_latency_seconds"),
        delta_x_mean_capped_latency_seconds_renewed_minus_single=x_capped_delta,
        delta_honest_first_choice_x_attempts_renewed_minus_single=_difference(
            rm.get("honest_first_choice_x_attempts"), sm.get("honest_first_choice_x_attempts")),
        delta_honest_ever_tried_x_attempts_renewed_minus_single=_difference(
            rm.get("honest_ever_tried_x_attempts"), sm.get("honest_ever_tried_x_attempts")),
        delta_honest_accepted_x_reply_count_renewed_minus_single=_difference(
            rm.get("honest_accepted_x_reply_count"), sm.get("honest_accepted_x_reply_count")),
        descriptive_leadership_signal=(x_rate_delta is not None and x_rate_delta > 0.0),
    )


def _pooled_arm_metrics(trials):
    attempts = [row for trial in trials for row in trial.get("attempts", [])]
    groups = [row for trial in trials for row in trial.get("groups", [])]
    x_attempts = [row for row in attempts if row.get("peer_name") == X_NAME]
    honest_attempts = [row for row in attempts if row.get("peer_name") in HONEST_NAMES]
    x_led = [row for row in x_attempts if row.get("led_success")]
    honest_success = [row for row in honest_attempts if row.get("membership_success")]
    honest_group_x = [row for row in honest_attempts
                      if row.get("first_group_leader_category") == X_NAME]
    honest_group_other = [row for row in honest_attempts
                          if row.get("first_group_leader_category") == "other"]
    honest_no_group = [row for row in honest_attempts
                       if row.get("first_group_leader_category") == "no_group"]
    x_capped = [row.get("lead_capped_latency_seconds") for row in x_attempts]
    x_observed_latencies = [row.get("lead_latency_seconds") for row in x_attempts]
    x_success_latencies = [value for value in x_observed_latencies if value is not None]
    honest_capped = [row.get("capped_latency_seconds") for row in honest_attempts]
    window_groups = [row for row in groups if row.get("within_submission_window")]
    return dict(
        trial_count=len(trials), x_attempts=len(x_attempts), x_led_attempts=len(x_led),
        x_led_rate=(len(x_led) / len(x_attempts) if x_attempts else None),
        x_led_by_2s_count=sum(value is not None and value <= 2 for value in x_observed_latencies),
        x_led_by_3s_count=sum(value is not None and value <= 3 for value in x_observed_latencies),
        x_led_by_5s_count=sum(value is not None and value <= 5 for value in x_observed_latencies),
        x_led_by_12s_count=sum(value is not None and value <= 12 for value in x_observed_latencies),
        x_mean_capped_latency_seconds=_mean(x_capped),
        x_success_only_mean_first_lead_latency_seconds=_mean(x_success_latencies),
        x_success_only_median_first_lead_latency_seconds=_median(x_success_latencies),
        x_success_only_p90_first_lead_latency_seconds=_percentile(x_success_latencies, 0.9),
        honest_attempts=len(honest_attempts),
        honest_first_group_success_attempts=len(honest_success),
        honest_first_group_success_fraction=(len(honest_success) / len(honest_attempts)
                                             if honest_attempts else None),
        honest_first_group_x_led_attempts=len(honest_group_x),
        honest_first_group_other_led_attempts=len(honest_group_other),
        honest_no_group_attempts=len(honest_no_group),
        honest_first_group_x_led_rate=(len(honest_group_x) / len(honest_attempts)
                                       if honest_attempts else None),
        honest_first_choice_x_attempts=sum(
            row.get("first_outgoing_join_destination") == X_NAME for row in honest_attempts),
        honest_first_choice_other_attempts=sum(
            row.get("first_outgoing_join_destination") == "other" for row in honest_attempts),
        honest_no_outgoing_choice_attempts=sum(
            row.get("first_outgoing_join_destination") == "none" for row in honest_attempts),
        honest_ever_tried_x_attempts=sum(row.get("ever_tried_x") is True for row in honest_attempts),
        honest_accepted_x_reply_count=sum(row.get("accepted_x_reply_count", 0)
                                          for row in honest_attempts),
        honest_accepted_other_reply_count=sum(row.get("accepted_other_reply_count", 0)
                                              for row in honest_attempts),
        honest_group_memberships_in_submission_window=sum(
            row.get("honest_membership_count", 0) for row in window_groups),
        honest_mean_capped_latency_seconds=_mean(honest_capped),
        honest_success_only_mean_first_group_latency_seconds=_mean(
            [row.get("first_group_latency_seconds") for row in honest_success
             if row.get("first_group_latency_seconds") is not None]),
        total_groups=len(groups), x_led_groups=sum(row.get("leader_name") == X_NAME
                                                   for row in groups),
    )


def _write_report(summary):
    def f(value, digits=3):
        return "—" if value is None else f"{float(value):.{digits}f}"

    lines = ["# Step 28 reliable advertisement comparison", "",
             f"Phase: **{summary.get('phase')}**. Trials: **{summary.get('trial_count')}/"
             f"{summary.get('expected_trials')}**. Integrity: **{summary.get('integrity_pass')}**.", "",
             "Local loopback experiment with 0.15-second one-way delay on find/store/join control RPC legs. "
             "Leadership and honest attempt denominators use every raw `step_start`, including starts "
             "after the submission cutoff. X leadership is tied to the same X peer and step. Honest first "
             "group classification is tied to the same honest peer and step. Capped means assign 12 seconds "
             "to attempts without an observed assembly; the lead-time CDF counts only observed raw lead "
             "latencies at or below each threshold, with all raw X starts in its denominator.", "",
             "Per-seed differences and the confirm signal are descriptive. No speed threshold is used.", "",
             "## Pooled attempt outcomes", "",
             "Counts pool all trials in each arm. Rates use all raw starts of the named peer cohort.", "",
             "| Arm | X led / X attempts | X-led rate | X capped mean | X success-only mean / median / p90 | "
             "Honest capped mean | "
             "Honest X-led first group / honest attempts | Honest X-led rate | "
             "First outgoing X / other / none (counts, all honest starts) | Any X tried / honest starts | "
             "Accepted replies X / other |",
             "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    pooled = summary.get("pooled_counts_by_case", {})
    for case_name in ("reliable_single", "reliable_renewed"):
        row = pooled.get(case_name, {})
        lines.append(
            f"| {case_name} | {row.get('x_led_attempts', 0)} / {row.get('x_attempts', 0)} | "
            f"{f(row.get('x_led_rate'))} | {f(row.get('x_mean_capped_latency_seconds'))} | "
            f"{f(row.get('x_success_only_mean_first_lead_latency_seconds'))} / "
            f"{f(row.get('x_success_only_median_first_lead_latency_seconds'))} / "
            f"{f(row.get('x_success_only_p90_first_lead_latency_seconds'))} | "
            f"{f(row.get('honest_mean_capped_latency_seconds'))} | "
            f"{row.get('honest_first_group_x_led_attempts', 0)} / {row.get('honest_attempts', 0)} | "
            f"{f(row.get('honest_first_group_x_led_rate'))} | "
            f"{row.get('honest_first_choice_x_attempts', 0)} / "
            f"{row.get('honest_first_choice_other_attempts', 0)} / "
            f"{row.get('honest_no_outgoing_choice_attempts', 0)} "
            f"(n={row.get('honest_attempts', 0)}) | "
            f"{row.get('honest_ever_tried_x_attempts', 0)} / {row.get('honest_attempts', 0)} | "
            f"{row.get('honest_accepted_x_reply_count', 0)} / "
            f"{row.get('honest_accepted_other_reply_count', 0)} |")
    lines.extend(["", "## X lead-time CDF", "",
                  "A threshold count is the number of raw X attempts with observed own-step leadership "
                  "latency at or below the threshold. Missing and later-than-threshold successes do not "
                  "count; all raw X attempts remain in the denominator.", "",
                  "| Arm | ≤2 s | ≤3 s | ≤5 s | ≤12 s |", "|---|---:|---:|---:|---:|"])
    for case_name in ("reliable_single", "reliable_renewed"):
        row = pooled.get(case_name, {})
        n = row.get("x_attempts", 0)
        values = [row.get(f"x_led_by_{threshold}s_count", 0) for threshold in (2, 3, 5, 12)]
        lines.append(f"| {case_name} | " + " | ".join(f"{value}/{n}" for value in values) + " |")
    lines.extend(["", "## Trial outcomes and publication cadence", "",
                  "Requested refresh frequency is blank for the single-publication arm. Achieved frequency "
                  "uses the median interval between actual active publications within X searches.", "",
                  "| Arm | Seed | Groups | X-led groups | X led / attempts | X-led rate | "
                  "Honest X-led first group / honest starts | Honest capped mean | "
                  "Requested / achieved publications per second |",
                  "|---|---:|---:|---:|---:|---:|---:|---:|---:|"])
    for trial in summary.get("trials", []):
        metrics = trial.get("summary_metrics", {})
        lines.append(
            f"| {trial.get('case', {}).get('name')} | {trial.get('seed')} | "
            f"{metrics.get('total_groups', 0)} | {metrics.get('x_led_groups', 0)} | "
            f"{metrics.get('x_led_attempts', 0)} / {metrics.get('x_submitted_attempts', 0)} | "
            f"{f(metrics.get('x_led_rate'))} | "
            f"{metrics.get('honest_first_group_x_led_attempts', 0)} / "
            f"{metrics.get('honest_submitted_attempts', 0)} | "
            f"{f(metrics.get('honest_mean_capped_latency_seconds'))} | "
            f"{f(metrics.get('requested_refresh_frequency_per_second'))} / "
            f"{f(metrics.get('achieved_active_publication_frequency_per_second'))} |")
    lines.extend(["", "## Per-seed renewed minus single differences", "",
                  "The participation delta is the rate of first assembled groups led by X among all honest "
                  "starts. The additional honest success-fraction delta counts first-group membership led "
                  "by any peer.", "",
                  "| Seed | Δ X-led rate | Δ honest first-group X-led rate | Δ honest first-group "
                  "success fraction | Δ honest capped mean (s) | Δ X capped mean (s) |",
                  "|---:|---:|---:|---:|---:|---:|"])
    for row in summary.get("per_seed_contrasts", []):
        lines.append(
            f"| {row.get('seed')} | {f(row.get('delta_x_led_rate_renewed_minus_single'))} | "
            f"{f(row.get('delta_honest_first_group_x_led_rate_renewed_minus_single'))} | "
            f"{f(row.get('delta_honest_first_group_success_fraction_renewed_minus_single'))} | "
            f"{f(row.get('delta_honest_mean_capped_latency_seconds_renewed_minus_single'))} | "
            f"{f(row.get('delta_x_mean_capped_latency_seconds_renewed_minus_single'))} |")
    if summary.get("per_seed_contrasts"):
        means = summary["unweighted_mean_differences_across_seeds"]
        lines.append(
            f"| Unweighted mean | {f(means.get('delta_x_led_rate_renewed_minus_single'))} | "
            f"{f(means.get('delta_honest_first_group_x_led_rate_renewed_minus_single'))} | "
            f"{f(means.get('delta_honest_first_group_success_fraction_renewed_minus_single'))} | "
            f"{f(means.get('delta_honest_mean_capped_latency_seconds_renewed_minus_single'))} | "
            f"{f(means.get('delta_x_mean_capped_latency_seconds_renewed_minus_single'))} |")
    lines.extend(["", "## Smoke coverage", "",
                  f"Smoke gate: **{summary.get('smoke_gate_pass')}**. Delayed find/store/join counts: `"
                  f"{json.dumps(summary.get('delayed_active_work_coverage', {}).get('counts', {}), sort_keys=True)}`.",
                  f"Checks: `{json.dumps(summary.get('smoke_checks'), sort_keys=True)}`.", "",
                  "## Descriptive confirm signal", "",
                  f"Signal: `{json.dumps(summary.get('descriptive_signal'), sort_keys=True)}`.", ""])
    if summary.get("integrity_errors"):
        lines.extend(["## Integrity errors", ""])
        lines.extend(f"- {error}" for error in summary["integrity_errors"])
        lines.append("")
    return "\n".join(lines)


def export_suite(suite_dir) -> dict:
    """Analyze the Step 28 manifest, writing event-linked tables and a summary."""
    suite_dir = Path(suite_dir)
    manifest = read_json(suite_dir / "suite.json")
    phase = manifest.get("phase")
    expected_jobs = canonical_jobs(phase)
    errors, missing, trials = [], [], []
    if manifest.get("experiment") != "step28" or manifest.get("jobs") != expected_jobs:
        errors.append("suite manifest differs from the frozen Step 28 job matrix")
    if manifest.get("outcome") != "grouping" or manifest.get("instrumentation") != "detailed":
        errors.append("Step 28 suite requires detailed grouping-only instrumentation")
    attempts, groups, choices, joins, retries, holds = [], [], [], [], [], []
    for job in manifest.get("jobs", []):
        case, seed = job.get("case", {}), job.get("seed")
        trial_dir = f"{case.get('name')}_s{seed}"
        trial_path = suite_dir / trial_dir
        if not (trial_path / "config.json").is_file() or not (trial_path / "events.jsonl").is_file():
            missing.append(trial_dir)
            continue
        try:
            trial = analyse_trial(trial_path)
            prefix = dict(trial_dir=trial_dir, case=case.get("name"), seed=seed)
            trials.append(trial)
            if not trial.get("integrity_pass"):
                errors.extend(f"{trial_dir}: {error}" for error in trial.get("integrity_errors", []))
            attempts.extend(dict(prefix, **row) for row in trial.get("attempts", []))
            groups.extend(dict(prefix, **row) for row in trial.get("groups", []))
            choices.extend(dict(prefix, **row) for row in trial.get("choices", []))
            joins.extend(dict(prefix, **row) for row in trial.get("joins", []))
            retries.extend(dict(prefix, **row) for row in trial.get("retries", []))
            holds.extend(dict(prefix, **row) for row in trial.get("holds", []))
        except (OSError, ValueError, KeyError, TypeError) as exc:
            errors.append(f"{trial_dir}: {type(exc).__name__}: {exc}")
    complete = len(trials) == len(expected_jobs) and not missing
    all_integrity = all(row.get("integrity_pass") is True for row in trials)
    def rpc_coverage(row):
        rpc_summary = row.get("delay_integrity", {}).get("summary", {})
        coverage = rpc_summary.get("coverage", {})
        return coverage.get("coverage", coverage)
    coverage_counts = {kind: sum(rpc_coverage(row).get(kind, {}).get("correlated_delayed", 0)
                                 for row in trials)
                       for kind in ("find", "store", "join")}
    coverage_required = phase == "smoke" and complete
    coverage_pass = (all(value > 0 for value in coverage_counts.values())
                     if coverage_required else True)
    if coverage_required and not coverage_pass:
        errors.append(f"smoke delayed find/store/join coverage incomplete: {coverage_counts}")

    smoke_gate_pass = None
    smoke_checks = None
    if phase == "smoke":
        by_case = {row.get("case", {}).get("name"): row for row in trials}
        single = by_case.get("reliable_single")
        renewed = by_case.get("reliable_renewed")
        native_groups_pass = bool(complete and single and renewed and all(
            row.get("summary_metrics", {}).get("total_groups", 0) > 0
            for row in (single, renewed)))
        single_search_counts = [search.get("active_publication_count", 0)
                                for search in (single or {}).get("summary_metrics", {}).get(
                                    "x_active_publications_by_search", [])]
        single_publication_pass = bool(single_search_counts and
                                       all(count == 1 for count in single_search_counts))
        renewed_search_counts = [search.get("active_publication_count", 0)
                                 for search in (renewed or {}).get("summary_metrics", {}).get(
                                     "x_active_publications_by_search", [])]
        renewed_multi_publication_searches = sum(count > 1 for count in renewed_search_counts)
        renewed_refresh_pass = renewed_multi_publication_searches > 0
        smoke_gate_pass = bool(complete and all_integrity and coverage_pass and native_groups_pass and
                               single_publication_pass and renewed_refresh_pass)
        smoke_checks = dict(native_group_coverage_pass=native_groups_pass,
                            reliable_single_one_active_publication_per_search_pass=single_publication_pass,
                            reliable_renewed_searches_with_multiple_active_publications=(
                                renewed_multi_publication_searches),
                            reliable_renewed_refresh_observed_pass=renewed_refresh_pass)
        if complete and not native_groups_pass:
            errors.append("both Step 28 smoke arms must contain native assembled groups")
        if complete and not single_publication_pass:
            errors.append("reliable_single smoke must have exactly one active publication per X search")
        if complete and not renewed_refresh_pass:
            errors.append("reliable_renewed smoke has no X search with multiple active publications")

    by_seed = defaultdict(dict)
    for row in trials:
        by_seed[row.get("seed")][row.get("case", {}).get("name")] = row
    contrasts = ([ _contrast_for_seed(seed, case_rows)
                   for seed, case_rows in sorted(by_seed.items())]
                 if phase == "confirm" else [])
    pooled_by_case = {
        name: _pooled_arm_metrics([row for row in trials
                                   if row.get("case", {}).get("name") == name])
        for name in ("reliable_single", "reliable_renewed")
    }
    pooled_count_keys = ("x_attempts", "x_led_attempts", "honest_attempts",
                         "honest_first_group_success_attempts", "honest_first_group_x_led_attempts",
                         "honest_first_group_other_led_attempts", "honest_no_group_attempts",
                         "honest_first_choice_x_attempts", "honest_first_choice_other_attempts",
                         "honest_no_outgoing_choice_attempts", "honest_ever_tried_x_attempts",
                         "honest_accepted_x_reply_count", "honest_accepted_other_reply_count",
                         "honest_group_memberships_in_submission_window", "total_groups", "x_led_groups")
    pooled_count_differences = {
        key: int(pooled_by_case["reliable_renewed"].get(key, 0)) -
        int(pooled_by_case["reliable_single"].get(key, 0)) for key in pooled_count_keys}
    mean_delta_keys = ("delta_x_led_attempts_renewed_minus_single",
                       "delta_x_led_rate_renewed_minus_single",
                       "delta_honest_first_group_x_led_attempts_renewed_minus_single",
                       "delta_honest_first_group_success_fraction_renewed_minus_single",
                       "delta_honest_first_group_x_led_rate_renewed_minus_single",
                       "delta_honest_group_memberships_renewed_minus_single",
                       "delta_honest_mean_capped_latency_seconds_renewed_minus_single",
                       "delta_honest_success_only_mean_first_group_latency_seconds_renewed_minus_single",
                       "delta_x_mean_capped_latency_seconds_renewed_minus_single")
    unweighted_mean_differences = {key: _mean([row.get(key) for row in contrasts])
                                   for key in mean_delta_keys}
    descriptive_signal = None
    if phase == "confirm":
        per_seed_positive = (len(contrasts) == 3 and all(
            row.get("delta_x_led_rate_renewed_minus_single") is not None and
            row["delta_x_led_rate_renewed_minus_single"] > 0 for row in contrasts))
        mean_lead_delta = unweighted_mean_differences.get("delta_x_led_rate_renewed_minus_single")
        attempt_floor = bool(len(contrasts) == 3 and all(
            row.get("reliable_single_x_attempts", 0) >= 10 and
            row.get("reliable_renewed_x_attempts", 0) >= 10 for row in contrasts))
        descriptive_signal = dict(
            all_three_seed_x_led_rate_deltas_positive=per_seed_positive,
            mean_x_led_rate_delta=mean_lead_delta,
            mean_x_led_rate_delta_at_least_0p10=(
                mean_lead_delta is not None and mean_lead_delta >= 0.10),
            each_arm_each_seed_has_at_least_10_x_attempts=attempt_floor,
            pass_=(per_seed_positive and mean_lead_delta is not None and
                   mean_lead_delta >= 0.10 and attempt_floor),
            interpretation="Descriptive signal only; not a speed gate, significance test, or deployment rate.")
    valid = complete and all_integrity and not errors and smoke_gate_pass is not False
    summary = dict(
        experiment="step28", phase=phase, complete=complete,
        expected_trials=len(expected_jobs), trial_count=len(trials), missing_trials=missing,
        integrity_pass=valid, integrity_errors=sorted(set(errors)),
        smoke_gate_pass=smoke_gate_pass, smoke_checks=smoke_checks,
        delayed_active_work_coverage=dict(counts=coverage_counts, required=coverage_required,
                                          pass_=coverage_pass),
        trials=[dict(trial_dir=row["trial_dir"], experiment=row["experiment"],
                     case=row["case"], seed=row["seed"], duration=row["duration"],
                     integrity_pass=row["integrity_pass"], integrity_errors=row["integrity_errors"],
                     summary_metrics=row["summary_metrics"], policy_integrity=row["policy_integrity"],
                     delay_integrity=row["delay_integrity"])
                for row in trials],
        per_seed_contrasts=contrasts, pooled_counts_by_case=pooled_by_case,
        pooled_count_differences_renewed_minus_single=pooled_count_differences,
        unweighted_mean_differences_across_seeds=unweighted_mean_differences,
        descriptive_signal=descriptive_signal, confirm_signal=descriptive_signal,
        screening_note=("Step 28 is a local descriptive confirmation. The three-seed signal is not a "
                        "significance test, deployment success rate, or speed gate."))
    (suite_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")
    (suite_dir / "REPORT.md").write_text(_write_report(summary), encoding="utf-8")
    _write_csv(suite_dir / "leadership_summary.csv", [_flatten_trial(row) for row in trials],
               preferred=("trial_dir", "case", "seed", "duration", "integrity_pass",
                          "x_submitted_attempts", "x_led_attempts", "x_led_rate",
                          "x_led_by_2s_count", "x_led_by_2s_rate", "x_led_by_3s_count",
                          "x_led_by_3s_rate", "x_led_by_5s_count", "x_led_by_5s_rate",
                          "x_led_by_12s_count", "x_led_by_12s_rate",
                          "x_success_only_mean_first_lead_latency_seconds",
                          "x_success_only_median_first_lead_latency_seconds",
                          "x_success_only_p90_first_lead_latency_seconds",
                          "honest_submitted_attempts", "honest_first_group_success_fraction",
                          "honest_first_group_x_led_attempts", "honest_first_group_other_led_attempts",
                          "honest_first_group_x_led_rate", "honest_no_group_attempts",
                          "honest_mean_capped_latency_seconds",
                          "honest_success_only_mean_first_group_latency_seconds",
                          "honest_first_choice_x_attempts", "honest_first_choice_other_attempts",
                          "honest_no_outgoing_choice_attempts", "honest_ever_tried_x_attempts",
                          "honest_accepted_x_reply_count", "honest_accepted_other_reply_count",
                          "total_groups", "x_led_groups", "honest_group_memberships_in_submission_window"))
    _write_csv(suite_dir / "attempts.csv", attempts,
               preferred=("trial_dir", "case", "seed", "step_id", "peer_id", "peer_name",
                          "submission_started", "late_start_after_submission_cutoff",
                          "started_monotonic", "started_wall", "start_event_ref",
                          "first_assembly_group_id", "first_assembly_event_ref",
                          "first_assembly_monotonic", "first_assembly_latency_seconds",
                          "capped_latency_seconds", "first_lead_group_id",
                          "first_lead_assembly_event_ref", "lead_latency_seconds",
                          "lead_capped_latency_seconds", "first_outgoing_join_id",
                          "first_outgoing_join_origin", "first_outgoing_join_event_ref",
                          "first_outgoing_join_selection_event_ref", "first_outgoing_join_peer_id",
                          "first_outgoing_join_peer_name", "first_outgoing_join_destination",
                          "ever_tried_x", "accepted_x_reply_count", "accepted_x_reply_event_refs",
                          "accepted_other_reply_count", "accepted_other_reply_event_refs",
                          "first_group_leader_peer_id", "first_group_leader_name",
                          "first_group_leader_category", "first_group_event_ref",
                          "first_group_latency_seconds", "led_success", "membership_success"))
    _write_csv(suite_dir / "groups.csv", groups,
               preferred=("trial_dir", "case", "seed", "group_id", "leader_name", "leader_peer_id",
                          "leader_step_id", "roster_size", "roster", "assembly_event_ref",
                          "assembly_event_refs", "assembly_monotonic", "within_submission_window",
                          "honest_peer_names", "member_step_ids", "honest_membership_count",
                          "leader_expiration", "assembly_deadline_event_ref",
                          "leader_expiration_pre_assembly_snapshot", "x_latest_publication_event_ref",
                          "x_latest_publication_expiration"))
    _write_csv(suite_dir / "joins.csv", joins,
               preferred=("trial_dir", "case", "seed", "join_id", "peer_id", "peer_name",
                          "leader_peer_id", "leader_name", "step_id", "search_id", "origin",
                          "selection_event_ref", "start_event_ref", "end_event_ref",
                          "native_client_event_refs", "accepted_reply_event_refs",
                          "group_disbanded_reply_event_refs", "reply_codes", "request_expiration",
                          "request_expirations", "selected_leader_expiration",
                          "first_outgoing_join_for_step", "first_outgoing_join_destination",
                          "ever_tried_x_for_step"))
    _write_csv(suite_dir / "choices.csv", choices,
               preferred=("trial_dir", "case", "seed", "event_ref", "read_event_ref", "peer_id",
                          "peer_name", "step_id", "search_id", "leader_peer_id", "leader_name",
                          "selected_expiration", "selection_top_wall", "selection_valid",
                          "past_attempts", "queue", "x_expiration", "x_eligible",
                          "first_eligible", "chose_X"))
    _write_csv(suite_dir / "retries.csv", retries,
               preferred=("trial_dir", "case", "seed", "selection_event_ref", "read_event_ref",
                          "peer_id", "search_id", "step_id", "old_pair_expiration",
                          "old_pair_publication_event_ref", "new_pair_expiration",
                          "new_pair_publication_event_ref", "x_search_id",
                          "alternative_eligible_queued_peers"))
    _write_csv(suite_dir / "holds.csv", holds,
               preferred=("trial_dir", "case", "seed", "visit_id", "leader_peer_id",
                          "caller_peer_id", "request_expiration", "accepted_event_ref",
                          "hold_start_event_ref", "disband_event_ref",
                          "disband_control_response_event_ref", "disband_native_client_join_id",
                          "disband_native_client_accepted_reply_event_ref",
                          "disband_native_client_reply_event_ref",
                          "disband_native_client_request_expiration",
                          "disband_received_by_native_client", "release_event_ref"))
    _write_csv(suite_dir / "per_seed_contrasts.csv", contrasts,
               preferred=("seed", "complete", "reliable_single_x_attempts",
                          "reliable_renewed_x_attempts", "reliable_single_x_led_rate",
                          "reliable_renewed_x_led_rate", "delta_x_led_rate_renewed_minus_single",
                          "delta_honest_first_group_x_led_attempts_renewed_minus_single",
                          "delta_honest_first_group_success_fraction_renewed_minus_single",
                          "delta_honest_first_group_x_led_rate_renewed_minus_single",
                          "delta_honest_mean_capped_latency_seconds_renewed_minus_single",
                          "delta_honest_success_only_mean_first_group_latency_seconds_renewed_minus_single",
                          "delta_honest_group_memberships_renewed_minus_single",
                          "descriptive_leadership_signal"))
    return dict(experiment="step28", phase=phase, integrity_pass=valid, complete=complete,
                trial_count=len(trials), missing_trials=missing, output_dir=str(suite_dir),
                smoke_gate_pass=smoke_gate_pass, smoke_checks=smoke_checks,
                descriptive_signal=descriptive_signal,
                confirm_signal=descriptive_signal,
                pooled_counts_by_case=pooled_by_case,
                pooled_count_differences_renewed_minus_single=pooled_count_differences,
                unweighted_mean_differences_across_seeds=unweighted_mean_differences,
                delayed_active_work_coverage=coverage_counts,
                integrity_errors=summary["integrity_errors"])


__all__ = ["analyse_trial", "canonical_jobs", "export_suite"]
