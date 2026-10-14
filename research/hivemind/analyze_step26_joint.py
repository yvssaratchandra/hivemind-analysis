"""Compact analysis and event-referenced exports for the Step 26 joint sweep.

This module reuses Step 25's native election reconstruction, then adds checks
for Step 26's own-ad publication policy and injected RPC delays. Raw events are
never copied into the summary; the CSV detail rows retain source event refs.
Only the Python standard library and the frozen Step 24/25 analyzers are used.
"""
from __future__ import annotations

import csv
from collections import Counter, defaultdict
import json
import math
from pathlib import Path
import statistics

try:
    from .analyze_step24_lifetime import finite_number, read_json
    from .analyze_step25_choices import analyse_trial_choices, _event_stream
except ImportError:  # direct script/import layout used by the experiment runner
    from analyze_step24_lifetime import finite_number, read_json
    from analyze_step25_choices import analyse_trial_choices, _event_stream


RATE_METRICS = ("x_led_rate", "x_led_by_2s_rate", "first_eligible_choice_rate")
INTERACTION_CSV = "interactions.csv"


def _ref(event):
    return f"{event['pid']}:{event['event_number']}"


def _pct(values, fraction):
    values = sorted(float(x) for x in values if x is not None)
    if not values:
        return None
    pos = (len(values) - 1) * fraction
    lo, hi = math.floor(pos), math.ceil(pos)
    return values[lo] if lo == hi else values[lo] * (hi - pos) + values[hi] * (pos - lo)


def _median(values):
    values = [float(x) for x in values if x is not None]
    return statistics.median(values) if values else None


def _case_config(config, errors):
    case = config.get("case")
    if not isinstance(case, dict):
        errors.append("config.case must be an object")
        return {}
    if config.get("experiment") != "step26":
        errors.append("config experiment must be step26")
    if config.get("outcome") != "grouping" or config.get("instrumentation") != "detailed":
        errors.append("Step 26 requires detailed grouping-only instrumentation")
    mode = case.get("mode")
    if mode not in ("refresh", "once", "passive"):
        errors.append(f"unknown Step 26 mode: {mode!r}")
    try:
        delay = finite_number(case.get("one_way_delay_seconds"), "case one-way delay")
        lifetime = finite_number(case.get("lifetime"), "case lifetime")
        if delay < 0 or lifetime <= 0:
            raise ValueError("delay must be nonnegative and lifetime positive")
    except ValueError as exc:
        errors.append(str(exc))
        delay, lifetime = 0.0, 0.0
    if "one_way_delay_seconds" not in config:
        errors.append("config is missing the top-level one-way delay")
    else:
        try:
            if not math.isclose(finite_number(config["one_way_delay_seconds"], "configured delay"),
                                delay, abs_tol=1e-12):
                errors.append("top-level one-way delay differs from case")
        except ValueError as exc:
            errors.append(str(exc))
    expected_delay_flag = delay > 0
    if config.get("network_delay_injected") is not expected_delay_flag:
        errors.append("network_delay_injected does not match configured delay")
    if config.get("cached_reads_delayed") is not False or config.get("tensor_transfer_delayed") is not False:
        errors.append("cached reads and tensor transfers must remain undelayed")
    if case.get("mode") == "refresh":
        try:
            frequency = finite_number(case.get("frequency"), "refresh frequency")
            if frequency <= 0:
                errors.append("refresh frequency must be positive")
        except ValueError as exc:
            errors.append(str(exc))
    return case


def _event_groups(events):
    by_kind = defaultdict(list)
    by_ref = {}
    for event in events:
        by_kind[event["kind"]].append(event)
        by_ref[_ref(event)] = event
    return by_kind, by_ref


def _assign_x_searches(native, events, by_ref, errors):
    """Attach X's publication calls to the exact same-process native SEARCH."""
    x_peer = native.get("per_peer", {}).get("X", {}).get("peer_id")
    searches = []
    for row in native.get("searches", []):
        if row.get("peer_id") != x_peer:
            continue
        start_event = by_ref.get(row.get("start_event_ref"))
        end_event = by_ref.get(row.get("end_event_ref"))
        if not start_event or not end_event:
            continue
        searches.append(dict(search_id=row["search_id"], step_id=row["step_id"],
                             peer_id=x_peer, pid=start_event["pid"],
                             start=row["start_monotonic"], end=row["end_monotonic"],
                             start_ref=row["start_event_ref"], end_ref=row["end_event_ref"]))
    searches.sort(key=lambda r: (r["start"], r["search_id"]))

    def find_search(event):
        matches = [s for s in searches if s["pid"] == event["pid"] and
                   s["peer_id"] == event["peer"] and s["start"] <= event["monotonic"] <= s["end"]]
        if len(matches) > 1:
            errors.append(f"publication event {_ref(event)} overlaps multiple X searches")
        return matches[0] if matches else None

    publications = []
    for event in events:
        if event["kind"] not in ("ad_started", "ad_finished", "ad_cancelled", "once_publication"):
            continue
        if event["peer"] != x_peer:
            continue
        if event["kind"] == "once_publication":
            search_id = event["detail"].get("search_id")
            search = next((s for s in searches if s["search_id"] == search_id and
                           s["pid"] == event["pid"]), None)
        else:
            search = find_search(event)
        publications.append((event, search))
    return searches, publications


def _rpc_integrity(case, config, events, by_kind, by_ref, native):
    errors = []
    try:
        configured_delay = finite_number(case.get("one_way_delay_seconds"), "case one-way delay")
    except ValueError as exc:
        configured_delay = 0.0
        errors.append(str(exc))
    starts = {}
    delays = defaultdict(list)
    handlers = defaultdict(list)
    responses = defaultdict(list)
    ends = defaultdict(list)
    admissions = defaultdict(list)
    for event in events:
        kind, detail = event["kind"], event["detail"]
        if kind not in ("control_rpc_start", "control_rpc_delay", "control_rpc_handler_enter",
                        "control_rpc_response", "control_rpc_end", "control_join_admission_state"):
            continue
        rpc_id = detail.get("rpc_id")
        if not isinstance(rpc_id, str) or not rpc_id:
            errors.append(f"{kind} missing rpc_id at {_ref(event)}")
            continue
        key = (event["pid"], rpc_id)
        if kind == "control_rpc_start":
            if key in starts:
                errors.append(f"duplicate control_rpc_start for {rpc_id}")
            starts[key] = event
            if detail.get("rpc_type") not in ("find", "store", "join"):
                errors.append(f"unknown RPC type for {rpc_id}")
            try:
                observed = finite_number(detail.get("delay_seconds"), "RPC start delay")
                if not math.isclose(observed, configured_delay, abs_tol=1e-12):
                    errors.append(f"RPC {rpc_id} delay differs from case")
            except ValueError as exc:
                errors.append(f"RPC {rpc_id}: {exc}")
            key_hashes = detail.get("key_hashes", [])
            if not isinstance(key_hashes, list) or any(not isinstance(x, str) for x in key_hashes):
                errors.append(f"RPC {rpc_id} key_hashes must be a string list")
        elif kind == "control_rpc_delay":
            leg = detail.get("leg")
            if leg not in ("request", "response"):
                errors.append(f"RPC {rpc_id} has invalid delay leg")
            try:
                requested = finite_number(detail.get("requested_delay"), "requested RPC delay")
                actual = finite_number(detail.get("actual_delay"), "actual RPC delay")
                if actual < 0 or not math.isclose(requested, configured_delay, abs_tol=1e-12):
                    errors.append(f"RPC {rpc_id} has delay inconsistent with case")
                if detail.get("cancelled") is not True and configured_delay > 0 and actual + .005 < requested:
                    errors.append(f"RPC {rpc_id} completed a delay leg materially early")
            except ValueError as exc:
                errors.append(f"RPC {rpc_id}: {exc}")
            delays[key].append(event)
        elif kind == "control_rpc_handler_enter":
            handlers[key].append(event)
        elif kind == "control_rpc_response":
            responses[key].append(event)
        elif kind == "control_join_admission_state":
            admissions[key].append(event)
        elif kind == "control_rpc_end":
            ends[key].append(event)

    for key, event in starts.items():
        rpc_id = key[1]
        detail = event["detail"]
        if len(ends.get(key, ())) != 1:
            errors.append(f"RPC {rpc_id} must have exactly one end record")
        if len(delays.get(key, ())) < 1:
            errors.append(f"RPC {rpc_id} has no request delay record")
        legs = Counter(row["detail"].get("leg") for row in delays.get(key, ()))
        if legs["request"] != 1:
            errors.append(f"RPC {rpc_id} must have exactly one request delay record")
        end = ends.get(key, [None])[0]
        ended_cancelled = bool(end and end["detail"].get("cancelled"))
        ended_error = bool(end and end["detail"].get("error"))
        request_cancelled = any(row["detail"].get("leg") == "request" and
                                row["detail"].get("cancelled") is True
                                for row in delays.get(key, ()))
        if not (ended_cancelled or request_cancelled):
            if len(handlers.get(key, ())) != 1:
                errors.append(f"RPC {rpc_id} lacks one handler-enter record")
        if not ended_cancelled and not request_cancelled and not ended_error and legs["response"] != len(responses.get(key, ())):
            errors.append(f"RPC {rpc_id} response event/delay counts differ")
        if detail.get("rpc_type") in ("find", "store") and not ended_cancelled and not ended_error:
            if len(responses.get(key, ())) != 1 or legs["response"] != 1:
                errors.append(f"unary RPC {rpc_id} must have one response")
        ordered_responses = sorted(responses.get(key, ()), key=lambda e: e["monotonic"])
        indices = [row["detail"].get("response_index") for row in ordered_responses]
        if indices != list(range(len(indices))):
            errors.append(f"RPC {rpc_id} response indexes are not contiguous from zero")
        if detail.get("rpc_type") == "join" and any(not row["detail"].get("code") for row in ordered_responses):
            errors.append(f"join RPC {rpc_id} response lacks a code")
        if detail.get("rpc_type") != "join" and admissions.get(key):
            errors.append(f"non-join RPC {rpc_id} has a join admission-state record")
        if detail.get("rpc_type") == "join" and len(admissions.get(key, ())) > 1:
            errors.append(f"join RPC {rpc_id} has duplicate admission-state records")
        for admission in admissions.get(key, ()):
            if handlers.get(key) and admission["monotonic"] < handlers[key][0]["monotonic"]:
                errors.append(f"join RPC {rpc_id} admission state precedes handler entry")
    for key in (set(delays) | set(handlers) | set(responses) | set(ends) | set(admissions)) - set(starts):
        errors.append(f"RPC event references unknown start {key[1]}")

    # Link remote RPCs to the specific control-plane work they served. Reads
    # wholly satisfied from local cache intentionally have no find RPC.
    read_intervals = [(r["peer_id"], r.get("start_monotonic"), r.get("end_monotonic"))
                      for r in native.get("reads", []) if r.get("success") and
                      r.get("start_monotonic") is not None and r.get("end_monotonic") is not None]
    peer_ids = config.get("peer_ids", {})
    known_peers = set(peer_ids.values()) if isinstance(peer_ids, dict) else set()
    ad_keys = {str(e["detail"].get("key", "")).lower().removeprefix("0x")
               for e in by_kind.get("holder_store", [])
               if e["detail"].get("subkey") in known_peers and e["detail"].get("active") is True
               and e["detail"].get("key")}
    join_intervals = []
    for start in by_kind.get("join_attempt_start", []):
        end = next((e for e in by_kind.get("join_attempt_end", [])
                    if e["pid"] == start["pid"] and e["detail"].get("join_id") == start["detail"].get("join_id") and
                    e["monotonic"] >= start["monotonic"]), None)
        join_intervals.append((start, end["monotonic"] if end else start["monotonic"] + 1.0))

    coverage = {kind: dict(total=0, correlated=0, correlated_delayed=0)
                for kind in ("find", "store", "join")}
    rpc_rows = []
    reads_with_find = set()
    for key, start in starts.items():
        detail, rpc_type = start["detail"], start["detail"].get("rpc_type")
        rpc_keys = {str(value).lower().removeprefix("0x")
                    for value in detail.get("key_hashes", []) if isinstance(value, str)}
        end = ends.get(key, [None])[0]
        delay_rows = delays.get(key, [])
        requested_legs = [row for row in delay_rows if row["detail"].get("requested_delay", 0) > 0 and
                          row["detail"].get("cancelled") is not True]
        correlated = False
        if rpc_type == "find":
            caller = start["other"]
            relevant_key = bool(rpc_keys & ad_keys)
            correlated_reads = [(peer, lo, hi) for peer, lo, hi in read_intervals
                                if peer == caller and lo <= start["monotonic"] <= hi]
            correlated = relevant_key and bool(correlated_reads)
            if correlated:
                reads_with_find.update(correlated_reads)
        elif rpc_type == "store":
            caller = start["other"]
            # holder_store identifies the advertisement owner (subkey) after
            # the request delay and before the RPC handler returns.
            handler_time = next((e["monotonic"] for e in handlers.get(key, ())), start["monotonic"])
            end_time = end["monotonic"] if end else float("inf")
            correlated = bool(rpc_keys & ad_keys) and any(
                             e["peer"] == start["peer"] and e["other"] == caller and
                             e["detail"].get("subkey") in known_peers and
                             e["detail"].get("active") is True and
                             handler_time <= e["monotonic"] <= end_time
                             for e in by_kind.get("holder_store", []))
        elif rpc_type == "join":
            correlated = any(start["peer"] == row["other"] and start["other"] == row["peer"] and
                             lo <= start["monotonic"] <= hi
                             for row, hi in join_intervals for lo in (row["monotonic"],))
        coverage[rpc_type]["total"] += 1
        if correlated:
            coverage[rpc_type]["correlated"] += 1
            if requested_legs:
                coverage[rpc_type]["correlated_delayed"] += 1
        rpc_rows.append(dict(rpc_id=detail.get("rpc_id"), rpc_type=rpc_type,
                             start_event_ref=_ref(start), source_peer=start["other"],
                             destination_peer=start["peer"], requested_delay=configured_delay,
                             response_count=len(responses.get(key, ())),
                             cancelled=bool(end and end["detail"].get("cancelled")),
                             error=end["detail"].get("error") if end else "missing_end",
                             correlated_to_target_activity=correlated,
                             completed_delay_legs=len(requested_legs)))
    counts = Counter(row["rpc_type"] for row in rpc_rows)
    summary = dict(configured_one_way_seconds=configured_delay,
                   rpc_counts={kind: counts.get(kind, 0) for kind in ("find", "store", "join")},
                   delayed_completed_legs=sum(r["completed_delay_legs"] for r in rpc_rows),
                   join_admission_state_count=sum(len(v) for v in admissions.values()),
                   coverage=coverage,
                   logical_read_count=len(read_intervals),
                   logical_reads_with_remote_find=len(reads_with_find),
                   unlinked_find_rpc_count=sum(row["rpc_type"] == "find" and
                                               not row["correlated_to_target_activity"] for row in rpc_rows),
                   rpc_rows=rpc_rows)
    # Event instrumentation integrity is separate from the requirement that a
    # particular short trial happen to exercise every RPC class.
    return dict(pass_=not errors, errors=sorted(set(errors)), summary=summary)


def _policy_integrity(case, config, events, by_kind, by_ref, native):
    errors = []
    searches, x_publication_events = _assign_x_searches(native, events, by_ref, errors)
    x_peer = native.get("per_peer", {}).get("X", {}).get("peer_id")
    mode = case.get("mode")
    lifetime = float(case.get("lifetime", 0) or 0)
    active = [e for e, search in x_publication_events if e["kind"] == "ad_started" and
              e["detail"].get("active") is True]
    active_by_search = defaultdict(list)
    once_by_search = defaultdict(list)
    for event, search in x_publication_events:
        if search and event["kind"] == "ad_started" and event["detail"].get("active") is True:
            active_by_search[search["search_id"]].append(event)
        if event["kind"] == "once_publication":
            if search is None:
                errors.append(f"once publication {_ref(event)} has no matching X search")
            else:
                once_by_search[search["search_id"]].append(event)
    for search in searches:
        ads = sorted(active_by_search.get(search["search_id"], []), key=lambda e: e["monotonic"])
        if mode in ("refresh", "once", "passive") and not ads:
            errors.append(f"X search {search['search_id']} has no active advertisement publication")
        previous_expiration = None
        for event in ads:
            detail = event["detail"]
            try:
                expiration = finite_number(detail.get("expiration"), "active ad expiration")
                remaining = expiration - float(event["wall_time"])
                if remaining <= 0:
                    errors.append(f"active advertisement {_ref(event)} was already expired when sent")
                if lifetime and remaining > lifetime + .05:
                    errors.append(f"active advertisement {_ref(event)} exceeds configured lifetime")
                if previous_expiration is not None and expiration <= previous_expiration:
                    errors.append(f"X expiration did not increase within SEARCH {search['search_id']}")
                previous_expiration = expiration
            except (ValueError, TypeError) as exc:
                errors.append(f"active advertisement {_ref(event)}: {exc}")
        if mode == "once":
            if len(ads) != 1:
                errors.append(f"once mode must send exactly one active advertisement in {search['search_id']}")
            if len(once_by_search.get(search["search_id"], [])) != 1:
                errors.append(f"once mode must log exactly one once_publication in {search['search_id']}")
        elif once_by_search.get(search["search_id"]):
            errors.append(f"once_publication appears in {mode} mode for {search['search_id']}")

    forbidden = [e for e in events if e["kind"] in ("forbidden_attacker_read", "forbidden_attacker_join")]
    if forbidden:
        errors.append(f"X made {len(forbidden)} forbidden competitor-read/outbound-join calls")

    x_ads = [e for e in by_kind.get("ad_started", []) if e["peer"] == x_peer]
    withdrawals = [e for e in x_ads if e["detail"].get("active") is False]
    ad_finished = [e for e in by_kind.get("ad_finished", []) if e["peer"] == x_peer]
    active_finished = [e for e in ad_finished if e["detail"].get("active") is True]
    accepted = sum(e["detail"].get("accepted") is True for e in active_finished)
    cancelled = sum(e["kind"] == "ad_cancelled" and e["peer"] == x_peer for e in events)
    holder_writes = [e for e in by_kind.get("holder_store", [])
                     if e["detail"].get("subkey") == x_peer and e["detail"].get("active") is True]
    holder_accepted = sum(e["detail"].get("accepted") is True for e in holder_writes)
    gaps, intervals = [], []
    for search in searches:
        ads = sorted(active_by_search.get(search["search_id"], []), key=lambda e: e["monotonic"])
        for prev, nxt in zip(ads, ads[1:]):
            interval = float(nxt["monotonic"] - prev["monotonic"])
            intervals.append(interval)
            deadline = float(prev["detail"].get("expiration", 0))
            send_wall = float(nxt["wall_time"])
            gaps.append(max(0.0, send_wall - deadline))
    publication_counts = [len(active_by_search.get(search["search_id"], [])) for search in searches]
    return dict(pass_=not errors, errors=sorted(set(errors)),
                summary=dict(mode=mode, x_searches=len(searches),
                             active_publication_attempts=len(active),
                             active_publications_per_search_min=min(publication_counts, default=None),
                             active_publications_per_search_max=max(publication_counts, default=None),
                             once_publications=sum(len(v) for v in once_by_search.values()),
                             accepted_active_declaration_calls=accepted,
                             cancelled_declaration_calls=cancelled,
                             withdrawal_calls=len(withdrawals),
                             accepted_active_holder_stores=holder_accepted,
                             active_holder_store_records=len(holder_writes),
                             publication_interval_median_seconds=_median(intervals),
                             publication_interval_p90_seconds=_pct(intervals, .9),
                             local_deadline_gap_count=sum(g > 0 for g in gaps),
                             local_deadline_gap_max_seconds=max(gaps, default=0.0)))


def _attempt_summary(native):
    rows = [row for row in native["attempts"] if row.get("peer_name") == "X" and
            row.get("submission_started")]
    denominator = len(rows)
    led = sum(row["led_success"] for row in rows)
    interior = [row for row in rows if row.get("interior_attempt")]
    interior_denominator = len(interior)
    interior_led = sum(row["led_success"] for row in interior)
    interior_by_2 = sum(bool(row["success_by_2s"]) for row in interior)
    eventual = Counter(row["eventual_outcome"] for row in rows)
    early = {}
    for s in (1, 2, 5):
        count = sum(bool(row[f"success_by_{s}s"]) for row in rows)
        early[f"x_led_by_{s}s_count"] = count
        early[f"x_led_by_{s}s_rate"] = count / denominator if denominator else None
    latencies = [row["assembly_latency_seconds"] for row in rows
                 if row.get("led_success") and row.get("assembly_latency_seconds") is not None]
    groups = native["groups"]
    x_groups = [g for g in groups if g.get("leader_name") == "X"]
    eligible = [r for r in native["selections"] if r.get("first_eligible")]
    chose_x = sum(row.get("chose_X") is True for row in eligible)
    return dict(x_submitted_attempts=denominator, x_led_attempts=led,
                x_followed_attempts=eventual["followed"], x_no_group_attempts=eventual["no_group"],
                x_failed_attempts=eventual["failed"], x_led_rate=led / denominator if denominator else None,
                x_interior_attempts=interior_denominator, x_interior_led_attempts=interior_led,
                x_interior_led_rate=(interior_led / interior_denominator if interior_denominator else None),
                x_interior_led_by_2s_count=interior_by_2,
                x_interior_led_by_2s_rate=(interior_by_2 / interior_denominator if interior_denominator else None),
                **early, x_led_latency_successes=len(latencies),
                x_led_latency_median_seconds=_median(latencies),
                x_led_latency_p90_seconds=_pct(latencies, .9),
                total_groups=len(groups), x_led_groups=len(x_groups),
                x_led_group_share=len(x_groups) / len(groups) if groups else None,
                x_led_group_mean_roster_size=(statistics.mean(g["roster_size"] for g in x_groups)
                                              if x_groups else None),
                first_eligible_choice_count=len(eligible), first_eligible_chose_x=chose_x,
                first_eligible_choice_rate=chose_x / len(eligible) if eligible else None,
                selections_total=len(native["selections"]))


def _compact_cadence(native):
    cadence = native["metrics"]["cadence"]
    honest = cadence.get("honest_by_phase", {}).get("submission", {})
    return dict(honest_logical_read_starts=cadence.get("honest_read_start_count", 0),
                honest_enabled_seconds=cadence.get("honest_enabled_seconds", 0.0),
                honest_logical_reads_per_enabled_second=cadence.get("honest_read_start_rate_per_enabled_second"),
                honest_logical_read_interval_median_seconds=cadence.get("honest_read_start_interval_median_seconds"),
                honest_logical_read_interval_p90_seconds=cadence.get("honest_read_start_interval_p90_seconds"),
                honest_submission_read_starts=honest.get("read_start_count", 0),
                honest_submission_enabled_seconds=honest.get("enabled_seconds", 0.0),
                honest_submission_read_rate=honest.get("read_start_rate_per_enabled_second"),
                all_logical_read_starts=cadence.get("read_start_count", 0),
                successful_logical_read_completions=cadence.get("successful_completed_reads", 0),
                reads_in_flight_across_pause=cadence.get("reads_in_flight_across_pause", 0))


def _x_state(native, config, events):
    xpeer = native.get("per_peer", {}).get("X", {}).get("peer_id")
    search_spans = [row for row in native.get("searches", []) if row.get("peer_id") == xpeer]
    search_seconds = sum(max(0.0, float(row.get("duration_seconds") or 0)) for row in search_spans)
    enabled_spans = [row for row in native.get("enabled_spans", []) if row.get("peer_id") == xpeer]
    read_enabled_seconds = sum(max(0.0, float(row.get("duration_seconds") or 0)) for row in enabled_spans)
    training = [e for e in events if e["kind"] == "training_scheduled" and e["peer"] == xpeer]
    durations = []
    for event in training:
        try:
            durations.append(finite_number(event["detail"].get("delay"), "X training delay"))
        except ValueError:
            continue
    # Scheduled delay is the controlled between-round wait. Scheduler lateness
    # remains in the raw trace and is not mislabeled as training delay.
    return dict(search_spans=len(search_spans), searching_seconds=search_seconds,
                training_intervals=len(durations), between_round_training_seconds=sum(durations),
                mean_training_delay_seconds=(statistics.mean(durations) if durations else None),
                read_enabled_seconds=read_enabled_seconds)


def _holder_record_summary(path):
    """Compact Step 24's accepted-holder diagnostics without copying its arrays."""
    native_file = Path(path) / "analysis.json"
    if not native_file.exists():
        return dict(available=False, accepted_holder_intervals=0, accepted_holder_gap_count=0,
                    accepted_holder_gap_total_seconds=0.0, accepted_holder_gap_max_seconds=None)
    try:
        analysis = read_json(native_file)
        records = analysis.get("timing", {}).get("holder_records", {})
        gaps = records.get("gaps", [])
        intervals = records.get("intervals", [])
        durations = [finite_number(row.get("duration"), "holder gap duration") for row in gaps]
    except (ValueError, TypeError, AttributeError):
        return dict(available=False, accepted_holder_intervals=0, accepted_holder_gap_count=0,
                    accepted_holder_gap_total_seconds=0.0, accepted_holder_gap_max_seconds=None)
    return dict(available=True, accepted_holder_intervals=len(intervals),
                accepted_holder_gap_count=len(durations),
                accepted_holder_gap_total_seconds=sum(durations),
                accepted_holder_gap_max_seconds=max(durations, default=None),
                interpretation=records.get("interpretation"))


def analyse_trial(path) -> dict:
    """Return compact Step 26 metrics and per-trial integrity diagnostics."""
    path = Path(path)
    config = read_json(path / "config.json")
    config_errors = []
    case = _case_config(config, config_errors)
    native = analyse_trial_choices(path)
    events = _event_stream(path)
    by_kind, by_ref = _event_groups(events)
    rpc = _rpc_integrity(case, config, events, by_kind, by_ref, native)
    policy = _policy_integrity(case, config, events, by_kind, by_ref, native)
    metrics = _attempt_summary(native)
    metrics.update(_compact_cadence(native))
    metrics.update(policy["summary"])
    metrics.update(_holder_record_summary(path))
    x_peer = native.get("per_peer", {}).get("X", {}).get("peer_id")
    pubs_by_pid = defaultdict(list)
    for event in by_kind.get("ad_started", []):
        if event["peer"] == x_peer and event["detail"].get("active") is True:
            pubs_by_pid[event["pid"]].append(event)
    for rows in pubs_by_pid.values():
        rows.sort(key=lambda event: event["monotonic"])
    observed_periods = []
    for search in native.get("searches", []):
        if search.get("peer_id") != x_peer:
            continue
        start_event = by_ref.get(search.get("start_event_ref"))
        if start_event is None:
            continue
        ads = [event for event in pubs_by_pid[start_event["pid"]]
               if search["start_monotonic"] <= event["monotonic"] <= search["end_monotonic"]]
        observed_periods.extend(b["monotonic"] - a["monotonic"] for a, b in zip(ads, ads[1:]))
    case_frequency = case.get("frequency") if case.get("mode") == "refresh" else None
    observed_interval = _median(observed_periods)
    metrics.update(requested_refresh_frequency_per_second=case_frequency,
                   achieved_active_publication_frequency_per_second=(1.0 / observed_interval
                                                                      if observed_interval and observed_interval > 0 else None))
    metrics.update(dict(rpc_counts=rpc["summary"]["rpc_counts"],
                        delayed_completed_rpc_legs=rpc["summary"]["delayed_completed_legs"],
                        delay_activity_coverage=rpc["summary"]["coverage"],
                        logical_read_count_for_delay=rpc["summary"]["logical_read_count"],
                        logical_reads_with_remote_find=rpc["summary"]["logical_reads_with_remote_find"],
                        unlinked_find_rpc_count=rpc["summary"]["unlinked_find_rpc_count"]))
    metrics["x_time_state"] = _x_state(native, config, events)
    errors = list(config_errors)
    errors.extend(native.get("trace_integrity_errors", []))
    errors.extend(native.get("step24_control_integrity_errors", []))
    errors.extend(f"delay: {error}" for error in rpc["errors"])
    errors.extend(f"policy: {error}" for error in policy["errors"])
    errors = sorted(set(errors))
    return dict(experiment="step26", trial_dir=path.name,
                case=case, seed=config.get("seed"), duration=config.get("duration"),
                integrity_pass=bool(native.get("control_plane_pass") and not errors),
                integrity_errors=errors,
                native_control_plane_pass=native.get("control_plane_pass"),
                trace_integrity_pass=native.get("trace_integrity_pass"),
                policy_integrity=dict(pass_=policy["pass_"], errors=policy["errors"]),
                delay_integrity=dict(pass_=rpc["pass_"], errors=rpc["errors"],
                                     summary={k: v for k, v in rpc["summary"].items() if k != "rpc_rows"}),
                summary_metrics=metrics)


def calculate_interactions(cells):
    """Calculate complete-factorial contrasts from cell metric records.

    `cells` maps `(lifetime, frequency, one_way_delay)` to metric records. A
    rate record is `{rate, denominator}`; latency is `{median_seconds,
    successes}`. Missing cells/metrics are returned explicitly as NA rows.
        """
    metrics = (*RATE_METRICS, "x_led_latency")
    rows = []

    def rate(key, metric):
        cell = cells.get(key)
        value = cell.get(metric) if isinstance(cell, dict) else None
        if not isinstance(value, dict):
            return None, 0
        return value.get("rate"), int(value.get("denominator", 0) or 0)

    def latency(key):
        cell = cells.get(key)
        value = cell.get("x_led_latency") if isinstance(cell, dict) else None
        if not isinstance(value, dict):
            return None, 0
        return value.get("median_seconds"), int(value.get("successes", 0) or 0)

    def emit(kind, metric, component_keys, coefficients, contrast_label):
        present, denominators, successes = [], [], []
        for key, coefficient in zip(component_keys, coefficients):
            if metric == "x_led_latency":
                value, n = latency(key)
                successes.append(n)
            else:
                value, n = rate(key, metric)
                denominators.append(n)
            if value is None or not isinstance(value, (int, float)) or not math.isfinite(value):
                present = []
                break
            present.append(float(value) * coefficient)
        missing = [list(key) for key in component_keys
                   if key not in cells or not isinstance(cells.get(key), dict) or
                   (metric == "x_led_latency" and not isinstance(cells[key].get(metric), dict)) or
                   (metric != "x_led_latency" and not isinstance(cells[key].get(metric), dict))]
        enough = (min(denominators, default=0) >= 8 if metric != "x_led_latency" else
                  min(successes, default=0) >= 5)
        estimate = sum(present) if len(present) == len(component_keys) else None
        if metric == "x_led_latency" and estimate is not None:
            # For latency, report relative difference from the lower-order arm
            # represented by coefficient -1 at the first component.
            baseline = None
            for key, coefficient in zip(component_keys, coefficients):
                if coefficient < 0:
                    baseline, _ = latency(key)
                    break
            if baseline and baseline > 0:
                estimate /= baseline
            else:
                estimate = None
        flag = bool(estimate is not None and enough and abs(estimate) >= (0.30 if metric == "x_led_latency" else .15))
        status = ("missing_cells" if estimate is None else
                  "ok" if enough else "insufficient_observations")
        rows.append(dict(kind=kind, metric=metric, contrast=contrast_label,
                         component_cells=[list(k) for k in component_keys],
                         component_coefficients=list(coefficients), estimate=estimate,
                         minimum_denominator=min(denominators) if denominators else None,
                         minimum_successes=min(successes) if successes else None,
                         estimate_unit="relative_latency_change" if metric == "x_led_latency" else "rate_difference",
                         status=status,
                         flag=flag, threshold=(.30 if metric == "x_led_latency" else .15)))

    lifetimes, frequencies, delays = (1.0, 3.0), (.5, 2.0), (0.0, .15)
    # Difference in differences for each pair, holding the third factor fixed.
    for metric in metrics:
        for d in delays:
            keys = [(1., .5, d), (1., 2., d), (3., .5, d), (3., 2., d)]
            emit("u_x_f_at_delay", metric, keys, [-1, 1, 1, -1], f"U×f at d={d:g}s")
        for f in frequencies:
            keys = [(1., f, 0.), (1., f, .15), (3., f, 0.), (3., f, .15)]
            emit("u_x_d_at_frequency", metric, keys, [-1, 1, 1, -1], f"U×d at f={f:g}/s")
        for u in lifetimes:
            keys = [(u, .5, 0.), (u, 2., 0.), (u, .5, .15), (u, 2., .15)]
            emit("f_x_d_at_lifetime", metric, keys, [-1, 1, 1, -1], f"f×d at U={u:g}s")
        keys = [(1., .5, 0.), (1., 2., 0.), (3., .5, 0.), (3., 2., 0.),
                (1., .5, .15), (1., 2., .15), (3., .5, .15), (3., 2., .15)]
        emit("u_x_f_x_delay", metric, keys, [1, -1, -1, 1, -1, 1, 1, -1], "U×f×d")
    return rows


def _matched_contrasts(trials):
    by_cell = {}
    controls_once, controls_passive = {}, {}
    for trial in trials:
        case = trial["case"]
        mode, u, d = case.get("mode"), float(case.get("lifetime")), float(case.get("one_way_delay_seconds"))
        if mode == "refresh":
            by_cell[(u, float(case["frequency"]), d)] = trial
        elif mode == "once":
            controls_once[(u, d)] = trial
        elif mode == "passive":
            controls_passive[d] = trial
    rows = []
    metric_fields = (("x_led_rate", "x_led_rate", "x_submitted_attempts"),
                     ("x_led_by_2s_rate", "x_led_by_2s_rate", "x_submitted_attempts"),
                     ("first_eligible_choice_rate", "first_eligible_choice_rate", "first_eligible_choice_count"))
    for key, treatment in sorted(by_cell.items()):
        u, f, d = key
        controls = (("refresh_vs_once", controls_once.get((u, d))),
                    ("refresh_vs_passive", controls_passive.get(d)))
        for kind, control in controls:
            for label, field, denom_field in metric_fields:
                tv, cv = treatment["summary_metrics"].get(field), control["summary_metrics"].get(field) if control else None
                tn = treatment["summary_metrics"].get(denom_field, 0)
                cn = control["summary_metrics"].get(denom_field, 0) if control else 0
                estimate = tv - cv if tv is not None and cv is not None else None
                rows.append(dict(kind=kind, metric=label, treatment_case=treatment["case"]["name"],
                                 control_case=control["case"]["name"] if control else None,
                                 treatment_cell=[u, f, d], estimate=estimate,
                                 treatment_denominator=tn, control_denominator=cn,
                                 minimum_denominator=min(tn, cn) if control else None,
                                 status=("missing_control_or_metric" if estimate is None else
                                         "ok" if min(tn, cn) >= 8 else "insufficient_observations"),
                                 flag=bool(estimate is not None and min(tn, cn) >= 8 and abs(estimate) >= .15),
                                 threshold=.15))
            tl = treatment["summary_metrics"].get("x_led_latency_median_seconds")
            cl = control["summary_metrics"].get("x_led_latency_median_seconds") if control else None
            ts = treatment["summary_metrics"].get("x_led_latency_successes", 0)
            cs = control["summary_metrics"].get("x_led_latency_successes", 0) if control else 0
            relative = (tl / cl - 1.0) if tl is not None and cl is not None and cl > 0 else None
            rows.append(dict(kind=kind, metric="x_led_latency_relative", treatment_case=treatment["case"]["name"],
                             control_case=control["case"]["name"] if control else None,
                             treatment_cell=[u, f, d], estimate=relative,
                             treatment_successes=ts, control_successes=cs,
                             minimum_successes=min(ts, cs) if control else None,
                             status=("missing_control_or_metric" if relative is None else
                                     "ok" if min(ts, cs) >= 5 else "insufficient_observations"),
                             estimate_unit="relative_latency_change",
                             flag=bool(relative is not None and min(ts, cs) >= 5 and abs(relative) >= .30),
                             threshold=.30))
    return rows


def _cell_records(trials):
    cells = {}
    for trial in trials:
        case, m = trial["case"], trial["summary_metrics"]
        if case.get("mode") != "refresh":
            continue
        key = (float(case["lifetime"]), float(case["frequency"]),
               float(case["one_way_delay_seconds"]))
        cells[key] = {
            "x_led_rate": dict(rate=m.get("x_led_rate"), denominator=m.get("x_submitted_attempts", 0)),
            "x_led_by_2s_rate": dict(rate=m.get("x_led_by_2s_rate"), denominator=m.get("x_submitted_attempts", 0)),
            "first_eligible_choice_rate": dict(rate=m.get("first_eligible_choice_rate"),
                                                 denominator=m.get("first_eligible_choice_count", 0)),
            "x_led_latency": dict(median_seconds=m.get("x_led_latency_median_seconds"),
                                   successes=m.get("x_led_latency_successes", 0))}
    return cells


def _flatten_summary(trial):
    row = dict(trial_dir=trial["trial_dir"], case=trial["case"].get("name"),
               seed=trial.get("seed"), mode=trial["case"].get("mode"),
               lifetime=trial["case"].get("lifetime"), frequency=trial["case"].get("frequency"),
               one_way_delay_seconds=trial["case"].get("one_way_delay_seconds"),
               integrity_pass=trial["integrity_pass"])
    for key, value in trial["summary_metrics"].items():
        if not isinstance(value, (dict, list)):
            row[key] = value
        elif key not in ("x_time_state",):
            row[key] = value
    state = trial["summary_metrics"].get("x_time_state", {})
    for key, value in state.items():
        row[f"x_{key}"] = value
    return row


def _csv_value(value):
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return value


def _write_csv(path, rows, preferred=()):
    rows = list(rows)
    fields = list(preferred)
    seen = set(fields)
    for row in rows:
        for key in row:
            if key not in seen:
                fields.append(key)
                seen.add(key)
    if not fields:
        fields = list(preferred)
    with Path(path).open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _csv_value(row.get(key)) for key in fields})


def _export_detail_rows(path, trial):
    """Return compact event-referenced detail rows, never full event traces."""
    native = analyse_trial_choices(path)
    prefix = dict(trial_dir=path.name, case=trial["case"].get("name"), seed=trial.get("seed"))
    groups = []
    for row in native["groups"]:
        x_searches = [s for s in native.get("searches", []) if s.get("peer_name") == "X" and
                      s.get("start_monotonic") is not None and s.get("end_monotonic") is not None and
                      s["start_monotonic"] <= row["assembly_monotonic"] <= s["end_monotonic"]]
        groups.append(dict(prefix, group_id=row["group_id"], leader_name=row["leader_name"],
                           leader_peer_id=row["leader_peer_id"], leader_step_id=row.get("leader_step_id"),
                           roster_size=row["roster_size"], roster=row["roster"],
                           x_in_roster=row["x_in_roster"], assembly_event_ref=row["assembly_event_ref"],
                           leader_ad_started_event_ref=row.get("leader_ad_started_event_ref"),
                           leader_ad_active=row.get("leader_ad_active"),
                           leader_expiration=row.get("leader_expiration"),
                           leader_remaining_at_assembly=row.get("leader_remaining_at_assembly"),
                           x_ad_started_event_ref=row.get("x_ad_started_event_ref"),
                           x_ad_active=row.get("x_ad_active"), x_expiration=row.get("x_expiration"),
                           x_remaining_at_assembly=row.get("x_remaining_at_assembly"),
                           x_searching_at_assembly=bool(x_searches),
                           x_search_ids_at_assembly=[s["search_id"] for s in x_searches],
                           x_minus_leader_expiration_gap=row.get("x_minus_leader_expiration_gap")))
    attempts = []
    for row in native["attempts"]:
        attempts.append(dict(prefix, step_id=row["step_id"], peer_id=row["peer_id"],
                             peer_name=row["peer_name"], submission_started=row["submission_started"],
                             started_monotonic=row["started_monotonic"], started_wall=row["started_wall"],
                             start_event_ref=row["start_event_ref"], first_search_event_ref=row.get("first_search_event_ref"),
                             search_count=row["search_count"], led_success=row["led_success"],
                             followed_success=row["followed_success"], assembled_success=row["assembled_success"],
                             eventual_outcome=row["eventual_outcome"],
                             first_led_group_id=row.get("first_led_group_id"),
                             first_led_assembly_event_ref=row.get("first_led_assembly_event_ref"),
                             assembly_latency_seconds=row.get("assembly_latency_seconds"),
                             success_by_1s=row["success_by_1s"], success_by_2s=row["success_by_2s"],
                             success_by_5s=row["success_by_5s"]))
    choices = []
    for row in native["selections"]:
        if not row.get("first_eligible"):
            continue
        choices.append(dict(prefix, event_ref=row["event_ref"], read_event_ref=row.get("read_event_ref"),
                            peer_name=row["peer_name"], peer_id=row["peer_id"], step_id=row["step_id"],
                            search_id=row["search_id"], group_key=row["group_key"],
                            leader_name=row["leader_name"], leader_peer_id=row["leader_peer_id"],
                            selected_expiration=row["selected_expiration"],
                            own_expiration=row["own_expiration"], selection_top_wall=row["selection_top_wall"],
                            x_expiration=row["x_expiration"], x_eligible=row["x_eligible"],
                            chose_X=row["chose_X"], queue_matches_read_view=row["queue_matches_read_view"],
                            selection_valid=row["selection_valid"], queue=row["queue"],
                            past_attempts=row["past_attempts"]))
    return groups, attempts, choices


def _rpc_admission_rows(path, trial):
    """Link join admission-time expiration state to response codes by RPC id."""
    events = _event_stream(path)
    starts, states, responses, ends = {}, defaultdict(list), defaultdict(list), {}
    for event in events:
        detail, kind = event["detail"], event["kind"]
        if kind not in ("control_rpc_start", "control_join_admission_state",
                        "control_rpc_response", "control_rpc_end"):
            continue
        rpc_id = detail.get("rpc_id")
        if not isinstance(rpc_id, str):
            continue
        key = (event["pid"], rpc_id)
        if kind == "control_rpc_start":
            starts[key] = event
        elif kind == "control_join_admission_state":
            states[key].append(event)
        elif kind == "control_rpc_response":
            responses[key].append(event)
        else:
            ends[key] = event
    prefix = dict(trial_dir=path.name, case=trial["case"].get("name"), seed=trial.get("seed"))
    rows = []
    for key, start in starts.items():
        if start["detail"].get("rpc_type") != "join":
            continue
        detail = start["detail"]
        state_rows = states.get(key) or [None]
        ordered_responses = sorted(responses.get(key, []), key=lambda e: e["detail"].get("response_index", 0))
        response_codes = [row["detail"].get("code") for row in ordered_responses]
        end = ends.get(key)
        for state in state_rows:
            sd = state["detail"] if state else {}
            try:
                signed_gap = (finite_number(sd.get("request_expiration"), "join request expiration") -
                              finite_number(sd.get("leader_expiration"), "admission leader expiration"))
            except ValueError:
                signed_gap = None
            rows.append(dict(prefix, rpc_id=detail["rpc_id"], rpc_start_event_ref=_ref(start),
                             admission_state_event_ref=_ref(state) if state else None,
                             response_event_refs=[_ref(row) for row in ordered_responses],
                             caller_peer=start["other"], leader_peer=start["peer"], state_observed=state is not None,
                             leader_expiration=sd.get("leader_expiration"),
                             request_expiration=sd.get("request_expiration"),
                             request_minus_leader_expiration_gap_seconds=signed_gap,
                             is_looking_for_group=sd.get("is_looking_for_group"),
                             declared_group_key=sd.get("declared_group_key"),
                             leader_step_id=sd.get("leader_step_id"), current_leader=sd.get("current_leader"),
                             followers=sd.get("followers"), response_codes=response_codes,
                             rpc_cancelled=bool(end and end["detail"].get("cancelled")),
                             rpc_error=end["detail"].get("error") if end else "missing_end"))
    return rows


def export_suite(suite_dir) -> dict:
    """Write compact suite metrics and event-referenced CSVs to suite root."""
    suite_dir = Path(suite_dir)
    manifest = read_json(suite_dir / "suite.json")
    jobs = manifest.get("jobs")
    if not isinstance(jobs, list):
        raise ValueError("suite manifest jobs must be a list")
    trials, missing, errors = [], [], []
    groups, attempts, choices, admissions = [], [], [], []
    for job in jobs:
        case, seed = job.get("case"), job.get("seed")
        trial_dir = f"{case.get('name')}_s{seed}"
        path = suite_dir / trial_dir
        if not (path / "config.json").exists() or not (path / "events.jsonl").exists():
            missing.append(trial_dir)
            continue
        joint_path = path / "joint_analysis.json"
        try:
            trial = read_json(joint_path) if joint_path.exists() else analyse_trial(path)
            if not trial.get("integrity_pass"):
                errors.extend(f"{trial_dir}: {e}" for e in trial.get("integrity_errors", ["integrity failed"]))
            trials.append(trial)
            g, a, c = _export_detail_rows(path, trial)
            groups.extend(g)
            attempts.extend(a)
            choices.extend(c)
            admissions.extend(_rpc_admission_rows(path, trial))
        except (OSError, ValueError, KeyError, TypeError) as exc:
            errors.append(f"{trial_dir}: {type(exc).__name__}: {exc}")

    cells = _cell_records(trials)
    interactions = calculate_interactions(cells)
    matched = _matched_contrasts(trials)
    phase = manifest.get("phase")
    # A delayed-traffic claim requires actual, active-work coverage. Cached
    # reads legitimately create no find RPC, so only suite-level existence is
    # required for each RPC class, linked to ad reads, ad stores, or join calls.
    delayed_coverage = {kind: sum(row["delay_integrity"]["summary"]["coverage"][kind]["correlated_delayed"]
                                 for row in trials if row["case"].get("one_way_delay_seconds", 0) > 0)
                       for kind in ("find", "store", "join")}
    delayed_trials = [row for row in trials if row["case"].get("one_way_delay_seconds", 0) > 0]
    complete = len(trials) == len(jobs) and not missing
    coverage_required = phase in ("smoke", "pilot") and complete and bool(delayed_trials)
    coverage_pass = all(value > 0 for value in delayed_coverage.values()) if coverage_required else True
    if coverage_required and not coverage_pass:
        errors.append(f"delayed active-work coverage incomplete: {delayed_coverage}")
    valid = all(row.get("integrity_pass") for row in trials) and not errors
    summary = dict(experiment="step26", phase=phase, complete=complete,
                   expected_trials=len(jobs), trial_count=len(trials), missing_trials=missing,
                   integrity_pass=valid, integrity_errors=sorted(set(errors)),
                   delayed_active_work_coverage=dict(counts=delayed_coverage,
                                                     required=coverage_required,
                                                     pass_=coverage_pass),
                   trials=[dict(trial_dir=row["trial_dir"], case=row["case"], seed=row.get("seed"),
                                integrity_pass=row["integrity_pass"],
                                integrity_errors=row.get("integrity_errors", []),
                                summary_metrics=row["summary_metrics"],
                                delay_integrity=row["delay_integrity"],
                                policy_integrity=row["policy_integrity"])
                           for row in trials],
                   matched_control_contrasts=matched,
                   factorial_interactions=interactions,
                   note="One-seed preliminary local result; screening contrasts are exploratory and are not deployment rates.")
    (suite_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True, allow_nan=False) + "\n")
    _write_csv(suite_dir / "leadership_summary.csv", [_flatten_summary(row) for row in trials],
               preferred=("trial_dir", "case", "seed", "mode", "lifetime", "frequency",
                          "one_way_delay_seconds", "integrity_pass", "x_submitted_attempts",
                          "x_led_attempts", "x_followed_attempts", "x_no_group_attempts", "x_failed_attempts",
                          "x_led_rate", "x_led_by_1s_count", "x_led_by_1s_rate",
                          "x_led_by_2s_count", "x_led_by_2s_rate", "x_led_by_5s_count", "x_led_by_5s_rate",
                          "x_led_latency_successes", "x_led_latency_median_seconds", "x_led_latency_p90_seconds",
                          "total_groups", "x_led_groups", "x_led_group_share", "x_led_group_mean_roster_size",
                          "first_eligible_choice_count", "first_eligible_chose_x", "first_eligible_choice_rate"))
    _write_csv(suite_dir / INTERACTION_CSV, [*matched, *interactions])
    _write_csv(suite_dir / "groups.csv", groups,
               preferred=("trial_dir", "case", "seed", "group_id", "leader_name", "leader_peer_id",
                          "leader_step_id", "roster_size", "roster", "x_in_roster", "assembly_event_ref",
                          "leader_ad_started_event_ref", "leader_ad_active", "leader_expiration",
                          "leader_remaining_at_assembly", "x_ad_started_event_ref", "x_ad_active",
                          "x_expiration", "x_remaining_at_assembly", "x_searching_at_assembly",
                          "x_search_ids_at_assembly", "x_minus_leader_expiration_gap"))
    _write_csv(suite_dir / "attempts.csv", attempts,
               preferred=("trial_dir", "case", "seed", "step_id", "peer_id", "peer_name",
                          "submission_started", "started_monotonic", "started_wall", "start_event_ref",
                          "first_search_event_ref", "search_count", "led_success", "followed_success",
                          "assembled_success", "eventual_outcome", "first_led_group_id",
                          "first_led_assembly_event_ref", "assembly_latency_seconds", "success_by_1s",
                          "success_by_2s", "success_by_5s"))
    _write_csv(suite_dir / "choices.csv", choices,
               preferred=("trial_dir", "case", "seed", "event_ref", "read_event_ref", "peer_name", "peer_id",
                          "step_id", "search_id", "leader_name", "leader_peer_id", "selected_expiration",
                          "own_expiration", "selection_top_wall", "x_expiration", "x_eligible", "chose_X",
                          "queue_matches_read_view", "selection_valid", "queue", "past_attempts"))
    _write_csv(suite_dir / "rpc_admissions.csv", admissions,
               preferred=("trial_dir", "case", "seed", "rpc_id", "rpc_start_event_ref",
                          "admission_state_event_ref", "caller_peer", "leader_peer", "state_observed",
                          "leader_expiration", "request_expiration",
                          "request_minus_leader_expiration_gap_seconds", "is_looking_for_group",
                          "declared_group_key", "leader_step_id", "current_leader", "followers",
                          "response_codes", "response_event_refs", "rpc_cancelled", "rpc_error"))
    return dict(experiment="step26", phase=phase, integrity_pass=valid,
                complete=summary["complete"], trial_count=len(trials), missing_trials=missing,
                output_dir=str(suite_dir), delayed_active_work_coverage=delayed_coverage,
                integrity_errors=summary["integrity_errors"])


__all__ = ["analyse_trial", "calculate_interactions", "export_suite"]
