"""Step 27 analysis for renewal benefit and unreliable-response harm.

The native election reconstruction stays frozen in Step 25. This module adds
Step 27 configuration checks, response-hold linkage, event-referenced exports,
and bounded, descriptive contrasts. Raw event records are kept in the trial
directories; suite exports contain compact rows and event references.
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
CSV_NAMES = ("leadership_summary.csv", "per_peer.csv", "attempts.csv", "groups.csv",
             "retries.csv", "holds.csv", "choices.csv", "rpc_admissions.csv",
             "per_seed_contrasts.csv")
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
    if phase not in ("smoke", "pilot"):
        raise ValueError("phase must be smoke or pilot")
    cases = _canonical_cases()
    if phase == "smoke":
        cases = [case for case in cases if case["mode"] == "refresh"]
    seeds = (2700,) if phase == "smoke" else (2701, 2702)
    duration = 10 if phase == "smoke" else 60
    jobs = [dict(case=case, seed=seed, duration=duration)
            for seed in seeds for case in cases]
    random.Random(270027).shuffle(jobs)
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
    allowed = {row["name"]: row for row in _canonical_cases()}
    expected_case = allowed.get(case.get("name"))
    if expected_case is None:
        errors.append(f"unknown Step 27 case: {case.get('name')!r}")
    elif case != expected_case:
        errors.append("config.case differs from the frozen Step 27 arm")
    for key, expected in _FIXED_CONTROLS.items():
        if config.get(key) != expected:
            errors.append(f"Step 27 control {key} differs from the frozen design")
    try:
        seed = int(config.get("seed"))
        if isinstance(config.get("seed"), bool):
            raise ValueError("seed must be an integer")
        duration = finite_number(config.get("duration"), "duration")
        started = finite_number(config.get("started_monotonic"), "started_monotonic")
        if duration not in (10.0, 60.0):
            errors.append("duration must be 10 seconds for smoke or 60 seconds for pilot")
    except (TypeError, ValueError) as exc:
        errors.append(str(exc))
        seed, duration, started = None, None, None
    delay = case.get("one_way_delay_seconds")
    if delay != 0.15 or config.get("one_way_delay_seconds") != 0.15:
        errors.append("Step 27 requires a symmetric one-way delay setting of 0.15 seconds")
    if config.get("network_delay_injected") is not True:
        errors.append("network_delay_injected must be true for Step 27")
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
        if disband and native is not None:
            caller = common.get("other") if common else None
            leader = common.get("peer") if common else None
            request_expiration = detail.get("request_expiration")
            matching_replies = []
            for join in native.get("joins", []):
                if (join.get("peer_id") != caller or join.get("leader_peer_id") != leader or
                        join.get("start_monotonic", math.inf) > disband[0]["monotonic"] or
                        (join.get("end_monotonic") is not None and
                         join["end_monotonic"] < disband[0]["monotonic"])):
                    continue
                selected = join.get("selected_expiration")
                if selected is not None and request_expiration is not None:
                    try:
                        if float(selected) != float(request_expiration):
                            continue
                    except (TypeError, ValueError):
                        continue
                for record in join.get("native_client_records", []):
                    reply = (by_ref or {}).get(record.get("event_ref"))
                    if (record.get("code") == "GROUP_DISBANDED" and reply is not None and
                            reply.get("kind") == "native_client_reply" and
                            reply.get("peer") == caller and reply.get("other") == leader and
                            reply.get("monotonic", -math.inf) >= disband[0]["monotonic"]):
                        matching_replies.append(reply)
            native_client_receipt = min(matching_replies, key=_event_order, default=None)
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
    x_led_by_2s = [row for row in x_attempts
                   if row["lead_latency_seconds"] is not None and row["lead_latency_seconds"] <= 2.0]
    honest_capped = [row["capped_latency_seconds"] for row in honest_attempts]
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
        x_led_by_2s_count=len(x_led_by_2s),
        x_led_by_2s_rate=(len(x_led_by_2s) / len(x_attempts) if x_attempts else None),
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
    """Analyze one raw Step 27 trial, retaining compact event-reference rows."""
    path = Path(path)
    config = read_json(path / "config.json")
    errors = []
    case, peer_ids, seed, duration, started = _validate_config(config, path, errors)
    native = analyse_trial_choices(path)
    events = _event_stream(path)
    by_kind, by_ref = _event_groups(events)
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
    retries = _retry_rows(native, events, by_ref, peer_ids, groups)
    holds = _hold_rows(events, peer_ids, case, errors, native, by_ref)
    metrics = _trial_metrics(native, attempts, groups, retries, holds, config, peer_ids)
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
        experiment="step27", trial_dir=path.name, case=case, seed=seed, duration=duration,
        integrity_pass=bool(native.get("control_plane_pass") and not integrity_errors),
        integrity_errors=integrity_errors,
        native_control_plane_pass=native.get("control_plane_pass"),
        trace_integrity_pass=native.get("trace_integrity_pass"),
        policy_integrity=dict(pass_=policy.get("pass_") is True, errors=policy.get("errors", [])),
        delay_integrity=dict(pass_=rpc.get("pass_") is True, errors=rpc.get("errors", []),
                             summary=rpc_summary),
        summary_metrics=metrics, attempts=attempts, groups=groups, retries=retries, holds=holds,
        choices=native.get("selections", []))


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


def _contrast_for_seed(seed, by_case):
    names = ("native_baseline", "reliable_single", "reliable_renewed",
             "unreliable_single", "unreliable_renewed")
    if any(name not in by_case for name in names):
        return dict(seed=seed, complete=False, screen_flags={})
    trial = {name: by_case[name] for name in names}
    m = {name: row["summary_metrics"] for name, row in trial.items()}
    rr, rs = m["reliable_renewed"], m["reliable_single"]
    ur, us = m["unreliable_renewed"], m["unreliable_single"]
    baseline = m["native_baseline"]
    delta_lead_2s_vs_single = _difference(rr.get("x_led_by_2s_rate"), rs.get("x_led_by_2s_rate"))
    delta_lead_2s_vs_native = _difference(rr.get("x_led_by_2s_rate"), baseline.get("x_led_by_2s_rate"))
    eventual_change_vs_single = _difference(rr.get("x_led_rate"), rs.get("x_led_rate"))
    eventual_change_vs_native = _difference(rr.get("x_led_rate"), baseline.get("x_led_rate"))
    renewal_alone_delta_l = _difference(rr.get("honest_mean_capped_latency_seconds"),
                                        rs.get("honest_mean_capped_latency_seconds"))
    unreliable_renewal_delta_l = _difference(ur.get("honest_mean_capped_latency_seconds"),
                                              us.get("honest_mean_capped_latency_seconds"))
    reliable_renewal_delta_l = renewal_alone_delta_l
    harm_did = (_difference(unreliable_renewal_delta_l, reliable_renewal_delta_l)
                if unreliable_renewal_delta_l is not None and reliable_renewal_delta_l is not None else None)
    n_x_by_arm = {name: m[name].get("x_submitted_attempts", 0)
                  for name in ("native_baseline", "reliable_single", "reliable_renewed")}
    n_x_required = min(n_x_by_arm["reliable_single"], n_x_by_arm["reliable_renewed"])
    n_x_native_required = min(n_x_by_arm["native_baseline"], n_x_by_arm["reliable_renewed"])
    n_honest_required = min(m[name].get("honest_submitted_attempts", 0)
                            for name in ("reliable_single", "reliable_renewed",
                                         "unreliable_single", "unreliable_renewed"))
    retry_count = ur.get("same_search_x_retry_count", 0)
    flags = dict(
        renewal_leadership_gain_vs_reliable_single=(delta_lead_2s_vs_single is not None and
                                                    delta_lead_2s_vs_single >= 0.15),
        renewal_leadership_gain_vs_native=(delta_lead_2s_vs_native is not None and
                                           delta_lead_2s_vs_native >= 0.15),
        leadership_gain=(delta_lead_2s_vs_single is not None and delta_lead_2s_vs_single >= 0.15),
        native_policy_leadership_gain=(delta_lead_2s_vs_native is not None and
                                       delta_lead_2s_vs_native >= 0.15),
        eventual_drop_within_0p10_vs_reliable_single=(eventual_change_vs_single is not None and
                                                       eventual_change_vs_single >= -0.10),
        eventual_drop_within_0p10_vs_native=(eventual_change_vs_native is not None and
                                              eventual_change_vs_native >= -0.10),
        eventual_drop_within_0p10=(eventual_change_vs_single is not None and
                                   eventual_change_vs_single >= -0.10),
        x_attempt_floor=(n_x_required >= 8),
        native_x_attempt_floor=(n_x_native_required >= 8),
        harm_did_at_least_1s=(harm_did is not None and harm_did >= 1.0),
        renewal_amplified_harm=(harm_did is not None and harm_did >= 1.0 and retry_count >= 1),
        honest_attempt_floor=(n_honest_required >= 20),
        unreliable_renewal_retry_observed=(retry_count >= 1),
        renewal_alone_delta_l_at_least_1s=(renewal_alone_delta_l is not None and
                                           renewal_alone_delta_l >= 1.0),
        renewal_alone_harm=(renewal_alone_delta_l is not None and
                            renewal_alone_delta_l >= 1.0),
    )
    return dict(seed=seed, complete=True,
                x_led_by_2s_rate_native=baseline.get("x_led_by_2s_rate"),
        x_led_by_2s_rate_reliable_single=rs.get("x_led_by_2s_rate"),
        x_led_by_2s_rate_reliable_renewed=rr.get("x_led_by_2s_rate"),
        delta_x_led_by_2s_rate_reliable_renewed_vs_reliable_single=delta_lead_2s_vs_single,
        delta_x_led_by_2s_rate_reliable_renewed_vs_native=delta_lead_2s_vs_native,
        x_led_rate_native=baseline.get("x_led_rate"),
        x_led_rate_reliable_single=rs.get("x_led_rate"),
        x_led_rate_reliable_renewed=rr.get("x_led_rate"),
        reliable_renewed_x_led_rate_change_vs_reliable_single=eventual_change_vs_single,
        reliable_renewed_x_led_rate_change_vs_native=eventual_change_vs_native,
                honest_mean_capped_latency_reliable_single=rs.get("honest_mean_capped_latency_seconds"),
                honest_mean_capped_latency_reliable_renewed=rr.get("honest_mean_capped_latency_seconds"),
                honest_mean_capped_latency_unreliable_single=us.get("honest_mean_capped_latency_seconds"),
                honest_mean_capped_latency_unreliable_renewed=ur.get("honest_mean_capped_latency_seconds"),
                renewal_alone_delta_l_seconds=renewal_alone_delta_l,
                unreliable_renewal_delta_l_seconds=unreliable_renewal_delta_l,
                harm_difference_in_differences_seconds=harm_did,
                n_x_by_arm=n_x_by_arm,
                n_x_reliable_renewed=rr.get("x_submitted_attempts", 0),
                n_x_reliable_single=rs.get("x_submitted_attempts", 0),
                n_x_native_baseline=baseline.get("x_submitted_attempts", 0),
                n_x_gate_minimum=n_x_required,
                n_x_native_gate_minimum=n_x_native_required,
                n_honest_by_arm={name: m[name].get("honest_submitted_attempts", 0)
                                 for name in ("reliable_single", "reliable_renewed",
                                              "unreliable_single", "unreliable_renewed")},
                n_honest_gate_minimum=n_honest_required,
                unreliable_renewed_same_search_x_retries=retry_count,
                unreliable_renewed_retry_witness=(retry_count >= 1),
                screen_flags=flags,
                interpretation="Preliminary per-seed screen only; these flags are not significance tests or deployment rates.")


def _paired_screen_flags(contrasts):
    """Require complete observations and thresholds in both planned pilot seeds."""
    by_seed = {row.get("seed"): row for row in contrasts}
    complete_pair = (set(by_seed) == {2701, 2702} and
                     all(by_seed[seed].get("complete") is True for seed in (2701, 2702)))
    checks = {
        "leadership_gain": ("leadership_gain", "eventual_drop_within_0p10", "x_attempt_floor"),
        "native_policy_leadership_gain": ("native_policy_leadership_gain",
                                          "eventual_drop_within_0p10_vs_native", "native_x_attempt_floor"),
        "renewal_amplified_harm": ("renewal_amplified_harm", "honest_attempt_floor",
                                   "unreliable_renewal_retry_observed"),
        "renewal_alone_harm": ("renewal_alone_harm", "honest_attempt_floor"),
    }
    return {name: bool(complete_pair and all(
        all(by_seed[seed].get("screen_flags", {}).get(flag) is True for flag in required)
        for seed in (2701, 2702))) for name, required in checks.items()}


def _paired_screen_status(contrasts, flags):
    by_seed = {row.get("seed"): row for row in contrasts}
    complete_pair = (set(by_seed) == {2701, 2702} and
                     all(by_seed[seed].get("complete") is True for seed in (2701, 2702)))
    floor_by_gate = {
        "leadership_gain": ("x_attempt_floor",),
        "native_policy_leadership_gain": ("native_x_attempt_floor",),
        "renewal_amplified_harm": ("honest_attempt_floor", "unreliable_renewal_retry_observed"),
        "renewal_alone_harm": ("honest_attempt_floor",),
    }
    return {name: ("screen flag" if value else
                   "insufficient observations" if (not complete_pair or any(
                       not all(by_seed[seed].get("screen_flags", {}).get(flag) is True
                               for flag in floor_by_gate[name])
                       for seed in (2701, 2702))) else "no screen flag")
            for name, value in flags.items()}


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


def _report_text(summary):
    lines = ["# Step 27 control-plane screen", "",
             f"Phase: **{summary.get('phase')}**. Trials: **{summary.get('trial_count')}/"
             f"{summary.get('expected_trials')}**. Integrity: **{summary.get('integrity_pass')}**.", "",
             "This is a bounded local screening experiment with 0.15-second one-way control RPC delay. "
             "The capped latency uses native `step_start` through first relevant native assembly; missing "
             "assembly is assigned 12 seconds. Honest group memberships count deduplicated native groups "
             "assembled during each submission window.", "",
             "Per-seed flags are preliminary descriptive checks. They are not significance tests, "
             "deployment success rates, or deployment claims. The report separates disband emission, "
             "the server-side response event after its delay, and the native client's reply record; "
             "server-side response evidence alone does not establish client receipt.", "",
             "## Trial metrics", "",
             "| Case | Seed | X led / submitted | X led by 2s | Honest capped latency | Honest memberships/minute | Groups | Retries | Holds | Integrity |",
             "|---|---:|---:|---:|---:|---:|---:|---:|---:|---|"]
    for row in summary.get("trials", []):
        m = row.get("summary_metrics", {})
        latency = m.get("honest_mean_capped_latency_seconds")
        rate = m.get("honest_group_memberships_per_honest_peer_minute")
        lines.append(f"| {row.get('case', {}).get('name')} | {row.get('seed')} | "
                     f"{m.get('x_led_attempts')}/{m.get('x_submitted_attempts')} | "
                     f"{m.get('x_led_by_2s_count')}/{m.get('x_submitted_attempts')} | "
                     f"{latency if latency is not None else 'n/a'} | {rate if rate is not None else 'n/a'} | "
                     f"{m.get('total_groups')} | {m.get('same_search_x_retry_count')} | "
                     f"{m.get('unreliable_hold_start_count')} | {row.get('integrity_pass')} |")
    lines.extend(["", "## Paired-seed screening flags", "",
                  f"Suite flags: `{json.dumps(summary.get('screen_flags'), sort_keys=True)}`. Status: "
                  f"`{json.dumps(summary.get('screen_status'), sort_keys=True)}`.", "",
                  "| Seed | Δ X-led-by-2s rate, renewed vs once | Δ X-led-by-2s rate, renewed vs native | "
                  "Renewal-alone Δ honest capped latency | "
                  "Harm difference-in-differences | Flags |",
                  "|---:|---:|---:|---:|---:|---|"])
    for row in summary.get("per_seed_contrasts", []):
        flags = row.get("screen_flags", {})
        passed = [name for name, value in flags.items() if value]
        lines.append(f"| {row.get('seed')} | "
                     f"{row.get('delta_x_led_by_2s_rate_reliable_renewed_vs_reliable_single')} | "
                     f"{row.get('delta_x_led_by_2s_rate_reliable_renewed_vs_native')} | "
                     f"{row.get('renewal_alone_delta_l_seconds')} | "
                     f"{row.get('harm_difference_in_differences_seconds')} | "
                     f"{', '.join(passed) if passed else 'none'} |")
    lines.extend(["", "## Integrity and coverage", "",
                  f"Smoke gate: **{summary.get('smoke_gate_pass')}**.", "",
                  f"Delayed correlated find/store/join RPC counts: `{json.dumps(summary.get('delayed_active_work_coverage', {}).get('counts', {}), sort_keys=True)}`.", "",
                  "All detailed rows are event-referenced in the CSV exports. `selection_observed_x_expiration` "
                  "comes from the selection queue snapshot; `latest_published_x_expiration` comes from X's "
                  "latest active publication event. Queue alternatives are observed in the local native queue "
                  "and do not establish global DHT availability.", ""])
    if summary.get("integrity_errors"):
        lines.extend(["## Integrity errors", ""])
        lines.extend(f"- {error}" for error in summary["integrity_errors"])
        lines.append("")
    return "\n".join(lines)


def export_suite(suite_dir) -> dict:
    """Analyze manifest trials and write summary, report, and event-referenced CSVs."""
    suite_dir = Path(suite_dir)
    manifest = read_json(suite_dir / "suite.json")
    phase = manifest.get("phase")
    expected_jobs = canonical_jobs(phase)
    errors, missing, trials = [], [], []
    if manifest.get("experiment") != "step27" or manifest.get("jobs") != expected_jobs:
        errors.append("suite manifest differs from the frozen Step 27 job matrix")
    if manifest.get("outcome") != "grouping" or manifest.get("instrumentation") != "detailed":
        errors.append("Step 27 suite requires detailed grouping-only instrumentation")
    attempts, groups, retries, holds, choices, rpc_admissions, per_peer_rows = [], [], [], [], [], [], []
    for job in manifest.get("jobs", []):
        case, seed = job.get("case", {}), job.get("seed")
        trial_dir = f"{case.get('name')}_s{seed}"
        trial_path = suite_dir / trial_dir
        if not (trial_path / "config.json").is_file() or not (trial_path / "events.jsonl").is_file():
            missing.append(trial_dir)
            continue
        try:
            trial = analyse_trial(trial_path)
            if not trial["integrity_pass"]:
                errors.extend(f"{trial_dir}: {error}" for error in trial["integrity_errors"])
            prefix = dict(trial_dir=trial_dir, case=case.get("name"), seed=seed)
            trials.append(trial)
            attempts.extend(dict(prefix, **row) for row in trial["attempts"])
            groups.extend(dict(prefix, **row) for row in trial["groups"])
            retries.extend(dict(prefix, **row) for row in trial["retries"])
            holds.extend(dict(prefix, **row) for row in trial["holds"])
            choices.extend(dict(prefix, **row) for row in trial.get("choices", []))
            rpc_admissions.extend(_step26._rpc_admission_rows(trial_path, trial))
            for peer_name, peer_metrics in trial["summary_metrics"].get("honest_per_peer", {}).items():
                per_peer_rows.append(dict(prefix, peer_name=peer_name, **peer_metrics))
        except (OSError, ValueError, KeyError, TypeError) as exc:
            errors.append(f"{trial_dir}: {type(exc).__name__}: {exc}")
    complete = len(trials) == len(expected_jobs) and not missing
    all_integrity = all(row.get("integrity_pass") is True for row in trials)
    def rpc_coverage(row):
        rpc_summary = row.get("delay_integrity", {}).get("summary", {})
        return rpc_summary.get("coverage", rpc_summary)

    coverage_counts = {kind: sum(rpc_coverage(row).get(kind, {}).get("correlated_delayed", 0)
                                 for row in trials)
                       for kind in ("find", "store", "join")}
    coverage_required = phase == "smoke" and complete
    coverage_pass = all(value > 0 for value in coverage_counts.values()) if coverage_required else True
    if coverage_required and not coverage_pass:
        errors.append(f"smoke delayed active-work coverage incomplete: {coverage_counts}")
    smoke_gate_pass = None
    if phase == "smoke":
        by_case = {row["case"]["name"]: row for row in trials}
        reliable_renewed = by_case.get("reliable_renewed")
        unreliable = by_case.get("unreliable_renewed")
        normal_groups_pass = bool(complete and reliable_renewed and
                                  reliable_renewed.get("summary_metrics", {}).get("total_groups", 0) > 0)
        hold_pass = bool(unreliable and
                         unreliable["summary_metrics"].get("unreliable_accept_count", 0) > 0 and
                         unreliable["summary_metrics"].get("unreliable_hold_start_count", 0) > 0 and
                         unreliable["summary_metrics"].get("unreliable_disband_emitted_count", 0) > 0)
        smoke_gate_pass = bool(complete and all_integrity and coverage_pass and
                               normal_groups_pass and hold_pass)
        if complete and not normal_groups_pass:
            errors.append("reliable renewed smoke trial lacks active native group coverage")
        if complete and not hold_pass:
            errors.append("unreliable renewed smoke trial lacks an accept/hold/disband sequence")
    contrasts = []
    by_seed = defaultdict(dict)
    for row in trials:
        by_seed[row.get("seed")][row.get("case", {}).get("name")] = row
    if phase == "pilot":
        contrasts = [_contrast_for_seed(seed, by_case)
                     for seed, by_case in sorted(by_seed.items())]
    screen_flags = _paired_screen_flags(contrasts) if phase == "pilot" else None
    screen_status = (_paired_screen_status(contrasts, screen_flags)
                     if phase == "pilot" else None)
    any_follow_up_signal = (any(screen_flags.values()) if screen_flags is not None else None)
    valid = complete and all_integrity and not errors and smoke_gate_pass is not False
    summary = dict(
        experiment="step27", phase=phase, complete=complete,
        expected_trials=len(expected_jobs), trial_count=len(trials), missing_trials=missing,
        integrity_pass=valid, integrity_errors=sorted(set(errors)),
        smoke_gate_pass=smoke_gate_pass,
        delayed_active_work_coverage=dict(counts=coverage_counts, required=coverage_required,
                                          pass_=coverage_pass),
        trials=[dict(trial_dir=row["trial_dir"], case=row["case"], seed=row["seed"],
                     duration=row["duration"], integrity_pass=row["integrity_pass"],
                     integrity_errors=row["integrity_errors"], summary_metrics=row["summary_metrics"],
                     policy_integrity=row["policy_integrity"], delay_integrity=row["delay_integrity"])
                for row in trials],
        per_seed_contrasts=contrasts,
        screen_flags=screen_flags, screen_status=screen_status,
        any_follow_up_signal=any_follow_up_signal,
        screening_note=("Pilot contrasts are per-seed preliminary flags. They are not significance tests, "
                        "deployment success rates, or deployment claims."),
    )
    (suite_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")
    (suite_dir / "REPORT.md").write_text(_report_text(summary), encoding="utf-8")
    _write_csv(suite_dir / "leadership_summary.csv", [_flatten_trial(row) for row in trials],
               preferred=("trial_dir", "case", "seed", "mode", "response", "duration", "integrity_pass",
                          "x_submitted_attempts", "x_led_attempts", "x_led_rate", "x_led_by_2s_count",
                          "x_led_by_2s_rate", "x_mean_first_lead_latency_seconds",
                          "x_mean_capped_latency_seconds", "honest_submitted_attempts",
                          "honest_mean_capped_latency_seconds", "honest_no_group_attempts",
                          "honest_no_group_fraction", "total_groups", "x_led_groups",
                          "x_led_groups_in_submission_window",
                          "honest_group_memberships_in_submission_window",
                          "honest_group_memberships_per_honest_peer_minute",
                          "same_search_x_retry_count", "unreliable_hold_start_count",
                          "unreliable_disband_count"))
    _write_csv(suite_dir / "per_peer.csv", per_peer_rows,
               preferred=("trial_dir", "case", "seed", "peer_name", "peer_id", "attempts",
                          "mean_capped_latency_seconds", "median_capped_latency_seconds",
                          "p90_capped_latency_seconds", "no_group_attempts", "no_group_fraction",
                          "native_step_attempts", "x_visits", "same_search_x_retries",
                          "group_memberships_in_submission_window", "memberships_per_peer_minute",
                          "begin_allreduce_receipts", "receipt_participation_rate"))
    _write_csv(suite_dir / "attempts.csv", attempts,
               preferred=("trial_dir", "case", "seed", "step_id", "peer_id", "peer_name",
                          "submission_started", "late_start_after_submission_cutoff",
                          "started_monotonic", "started_wall", "start_event_ref",
                          "first_assembly_group_id", "first_assembly_event_ref", "first_assembly_monotonic",
                          "first_assembly_latency_seconds", "capped_latency_seconds",
                          "first_lead_group_id", "first_lead_assembly_event_ref",
                          "first_lead_assembly_monotonic", "lead_latency_seconds",
                          "lead_capped_latency_seconds", "first_begin_allreduce_reply_event_ref",
                          "begin_receipt_latency_seconds", "first_allreduce_start_event_ref",
                          "ready_evidence_event_ref", "assembly_roster_vs_begin_receipt_diverges",
                          "assembly_roster_vs_ready_evidence_diverges", "led_success", "membership_success"))
    _write_csv(suite_dir / "groups.csv", groups,
               preferred=("trial_dir", "case", "seed", "group_id", "leader_name", "leader_peer_id",
                          "leader_step_id", "roster_size", "roster", "assembly_event_ref",
                          "assembly_event_refs", "assembly_monotonic", "within_submission_window",
                          "honest_peer_names", "member_step_ids", "honest_membership_count", "leader_expiration",
                          "assembly_deadline_event_ref", "assembly_deadline_state_is_preassembly_snapshot",
                          "leader_expiration_pre_assembly_snapshot",
                          "leader_expiration_at_assembly_deadline",
                          "leader_expiration_changed_since_preassembly_snapshot",
                          "x_latest_publication_event_ref", "x_latest_publication_expiration",
                          "x_step25_group_expiration", "x_step25_group_ad_started_event_ref",
                          "x_step25_group_ad_active", "x_publication_search_ids"))
    _write_csv(suite_dir / "retries.csv", retries,
               preferred=("trial_dir", "case", "seed", "selection_event_ref", "read_event_ref",
                          "peer_id", "search_id", "step_id", "old_pair_expiration",
                          "old_pair_was_live_at_selection", "old_pair_publication_event_ref",
                          "new_pair_expiration", "new_pair_publication_event_ref",
                          "x_search_id", "x_search_step_id", "selection_observed_x_expiration",
                          "selection_read_x_expiration", "latest_published_x_expiration",
                          "latest_published_x_event_ref", "alternative_eligible_queued_peers",
                          "alternative_eligible_queue_is_observation", "x_assembly_relative_to_selection"))
    _write_csv(suite_dir / "holds.csv", holds,
               preferred=("trial_dir", "case", "seed", "visit_id", "leader_peer_id", "caller_peer_id",
                          "leader_step_id", "leader_search_id", "request_expiration",
                          "configured_hold_seconds", "accepted_event_ref", "rejected_event_ref",
                          "hold_start_event_ref", "disband_event_ref", "cancellation_event_ref",
                          "disband_control_response_event_ref", "disband_control_response_emitted",
                          "disband_control_response_rpc_id", "disband_control_response_link_method",
                          "disband_native_client_reply_event_ref",
                          "disband_received_by_native_client",
                          "release_event_ref", "actual_hold_duration_seconds", "hold_completed",
                          "registered_duration_to_release_seconds", "terminal_kind"))
    _write_csv(suite_dir / "choices.csv", choices,
               preferred=("trial_dir", "case", "seed", "event_ref", "read_event_ref", "peer_id",
                          "peer_name", "step_id", "search_id", "leader_peer_id", "leader_name",
                          "selected_expiration", "selection_top_wall", "selection_valid",
                          "past_attempts", "queue", "x_expiration", "x_eligible",
                          "first_eligible", "chose_X"))
    _write_csv(suite_dir / "rpc_admissions.csv", rpc_admissions,
               preferred=("trial_dir", "case", "seed", "rpc_id", "rpc_start_event_ref",
                          "admission_state_event_ref", "response_event_refs", "caller_peer",
                          "leader_peer", "state_observed", "leader_expiration", "request_expiration",
                          "request_minus_leader_expiration_gap_seconds", "response_codes", "rpc_cancelled"))
    _write_csv(suite_dir / "per_seed_contrasts.csv", contrasts,
               preferred=("seed", "complete", "delta_x_led_by_2s_rate_reliable_renewed_vs_native",
                          "reliable_renewed_x_led_rate_change_vs_native",
                          "renewal_alone_delta_l_seconds", "unreliable_renewal_delta_l_seconds",
                          "harm_difference_in_differences_seconds", "n_x_gate_minimum",
                          "n_honest_gate_minimum", "reliable_renewed_same_search_x_retries", "screen_flags"))
    return dict(experiment="step27", phase=phase, integrity_pass=valid, complete=complete,
                trial_count=len(trials), missing_trials=missing, output_dir=str(suite_dir),
                smoke_gate_pass=smoke_gate_pass, screen_flags=screen_flags,
                screen_status=screen_status, any_follow_up_signal=any_follow_up_signal,
                delayed_active_work_coverage=coverage_counts,
                integrity_errors=summary["integrity_errors"])


__all__ = ["analyse_trial", "canonical_jobs", "export_suite"]
