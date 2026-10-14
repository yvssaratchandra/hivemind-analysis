"""Audit and summarize the local Step 29 advertisement-policy pilot.

The analyzer composes the frozen Step 25 choice reconstruction and Step 28
trace/RPC helpers. All per-step denominators include every raw ``step_start``;
the capped lead score is explicitly a score, not elapsed time for a peer that
followed another leader.
"""
from __future__ import annotations

import csv
from collections import Counter, defaultdict
import json
import math
from pathlib import Path
import random
import statistics

try:
    from .analyze_step24_lifetime import finite_number, read_json
    from .analyze_step25_choices import analyse_trial_choices, _event_stream
    from . import analyze_step26_joint as _step26
    from . import analyze_step28_confirmation as _step28
except ImportError:  # direct import layout used by the local experiment runner
    from analyze_step24_lifetime import finite_number, read_json
    from analyze_step25_choices import analyse_trial_choices, _event_stream
    import analyze_step26_joint as _step26
    import analyze_step28_confirmation as _step28


X_NAME = "X"
HONEST_NAMES = tuple(f"H{i}" for i in range(7))
CAP_SECONDS = 12.0
CSV_NAMES = ("attempts.csv", "groups.csv", "choices.csv", "joins.csv",
             "leadership_summary.csv")
_CLOCK_JUMP_LIMIT = 0.25
_AD_FORMULA_TOLERANCE = 0.10
_CADENCE_TOLERANCE = 0.35


def _ref(event):
    return f"{event['pid']}:{event['event_number']}"


def _median(values):
    values = [float(value) for value in values if value is not None]
    return statistics.median(values) if values else None


def _mean(values):
    values = [float(value) for value in values if value is not None]
    return statistics.mean(values) if values else None


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


def _canonical_cases(behaviors="both"):
    if behaviors not in ("normal", "both"):
        raise ValueError("behaviors must be normal or both")
    rows = []
    behavior_names = ("normal", "recruit_only") if behaviors == "both" else ("normal",)
    for behavior in behavior_names:
        for policy, lifetime in (("native", 5.0), ("native", 1.0),
                                 ("native", 2.0), ("renew", 2.0)):
            suffix = f"native_{int(lifetime)}s" if policy == "native" else "renew_1_2"
            case = dict(name=f"{behavior}_{suffix}", behavior=behavior,
                        mode="stock" if behavior == "normal" else "passive",
                        publication_policy=policy, lifetime=lifetime, minimum=2,
                        target=4, one_way_delay_seconds=0.0, response="reliable",
                        hold_seconds=0.0)
            if policy == "renew":
                case.update(frequency=1.0, nominal_lower=1.0, nominal_upper=2.0)
            rows.append(case)
    return rows


def _canonical_jobs(phase, behaviors="both"):
    cases = _canonical_cases(behaviors)
    if phase == "smoke":
        return [dict(case=case, seed=2900, duration=10.0) for case in cases
                if case["behavior"] == "normal" and
                (case["publication_policy"] == "renew" or case["lifetime"] == 1.0)]
    if phase != "pilot":
        raise ValueError("phase must be smoke or pilot")
    random.Random(290029).shuffle(cases)
    return [dict(case=case, seed=2901, duration=45.0) for case in cases]


def _expect(value, expected, label, errors, tolerance=1e-9):
    try:
        actual = finite_number(value, label)
    except (TypeError, ValueError) as exc:
        errors.append(str(exc))
        return
    if not math.isclose(actual, float(expected), rel_tol=0.0, abs_tol=tolerance):
        errors.append(f"{label} is {actual!r}, expected {expected!r}")


def _validate_config(config, errors):
    if not isinstance(config, dict):
        errors.append("config.json must contain an object")
        return {}, {}, None, None, None
    case = config.get("case")
    if not isinstance(case, dict):
        errors.append("config.case must be an object")
        case = {}
    allowed = {row["name"]: row for row in _canonical_cases("both")}
    expected_case = allowed.get(case.get("name"))
    if expected_case is None:
        errors.append(f"unknown Step 29 case: {case.get('name')!r}")
    elif case != expected_case:
        errors.append("config.case differs from the frozen Step 29 arm")

    fixed = {
        "experiment": "step29", "outcome": "grouping", "instrumentation": "detailed",
        "attempt_unit": "step_start", "timeout": 12, "request_timeout": 3,
        "tensor_size": 128, "honest_min": 2, "target": 4,
        "honest_ad_lifetime": 5, "allreduce_timeout": 6, "store_delay": 0,
        "delay_location": "after observed FINISHED, before next step()",
        "group_bits": "", "scheduled_time": "native default",
        "network": "eight-peer loopback", "network_delay_injected": False,
        "one_way_delay_seconds": 0.0, "allow_state_sharing": False,
        "event_transport": "batched", "trace_batch_size": 32,
        "duplicate_read_views": False, "cached_reads_delayed": False,
        "tensor_transfer_delayed": False, "response_policy": "reliable",
        "leadership_latency_origin": "step_start", "harm_timeout_cap": 12,
        "clock_monitor": True, "clock_jump_limit_seconds": _CLOCK_JUMP_LIMIT,
        "submission_cutoff_checked_per_peer": True,
        "control_delay_model": "remote find/store request+response; join request+each response",
        "x_min_group_size": 2,
    }
    try:
        expected_training = _step28._training_delay()
    except Exception:
        expected_training = None
    if expected_training is not None:
        fixed["training_delay"] = expected_training
    for key, expected in fixed.items():
        if config.get(key) != expected:
            errors.append(f"Step 29 control {key} differs from the frozen design")
    if expected_case is not None:
        _expect(config.get("x_min_matchmaking_time"), expected_case["lifetime"],
                "x_min_matchmaking_time", errors)

    try:
        seed = int(config.get("seed"))
        if isinstance(config.get("seed"), bool):
            raise ValueError("seed must be an integer")
        duration = finite_number(config.get("duration"), "duration")
        started = finite_number(config.get("started_monotonic"), "started_monotonic")
        if (duration, seed) not in ((10.0, 2900), (45.0, 2901)):
            errors.append("Step 29 seed/duration must be smoke 2900/10s or pilot 2901/45s")
    except (TypeError, ValueError) as exc:
        errors.append(str(exc))
        seed, duration, started = None, None, None

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
        offsets = dict(X=rng.uniform(0.0, 3.0),
                       **{f"H{i}": value for i, value in enumerate(honest_offsets)})
        if config.get("initial_offsets") != offsets:
            errors.append("initial_offsets differ from the frozen seeded launch schedule")
    return case, peer_ids, seed, duration, started


def _expected_peer_runtime(name, case):
    if name == X_NAME:
        return dict(min_matchmaking_time=float(case.get("lifetime", 0.0)), min_group_size=2,
                    target_group_size=4, request_timeout=3.0,
                    behavior=case.get("behavior"),
                    publication_policy=case.get("publication_policy"))
    return dict(min_matchmaking_time=5.0, min_group_size=2,
                target_group_size=4, request_timeout=3.0,
                behavior="normal", publication_policy="native")


def _runtime_integrity(config, events, native, case, peer_ids, errors):
    """Compare configured values with each peer's traced live Matchmaking fields."""
    names_by_peer = {peer: name for name, peer in peer_ids.items()}
    runtime = config.get("runtime_matchmaking_config")
    if not isinstance(runtime, dict) or set(runtime) != set(names_by_peer):
        errors.append("runtime_matchmaking_config must be keyed by all eight peer IDs")
        runtime = runtime if isinstance(runtime, dict) else {}
    for peer, name in names_by_peer.items():
        row = runtime.get(peer)
        if not isinstance(row, dict):
            errors.append(f"runtime_matchmaking_config missing {name}")
            continue
        expected = _expected_peer_runtime(name, case)
        for key, value in expected.items():
            actual = row.get(key)
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                _expect(actual, value, f"configured {name}.{key}", errors)
            elif actual != value:
                errors.append(f"configured {name}.{key} is {actual!r}, expected {value!r}")

    rows_by_peer = defaultdict(list)
    for event in events:
        if event.get("kind") == "runtime_matchmaking_config":
            rows_by_peer[event.get("peer")].append(event)
    searches_by_peer = defaultdict(list)
    for search in native.get("searches", []):
        searches_by_peer[search.get("peer_id")].append(search)
    for peer, name in names_by_peer.items():
        rows = rows_by_peer.get(peer, [])
        if len(rows) != 1:
            errors.append(f"expected one runtime_matchmaking_config event for {name}, found {len(rows)}")
            continue
        event = rows[0]
        detail = event.get("detail", {})
        expected = _expected_peer_runtime(name, case)
        for key, value in expected.items():
            actual = detail.get(key)
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                _expect(actual, value, f"runtime event {name}.{key}", errors)
            elif actual != value:
                errors.append(f"runtime event {name}.{key} is {actual!r}, expected {value!r}")
        first_search = min(searches_by_peer.get(peer, []),
                           key=lambda row: row.get("start_monotonic", math.inf), default=None)
        if first_search is not None and event.get("monotonic", math.inf) > first_search.get("start_monotonic", -math.inf):
            errors.append(f"runtime matchmaking configuration for {name} was logged after its first search")
        if first_search is not None and detail.get("step_id") != first_search.get("step_id"):
            errors.append(f"runtime matchmaking configuration for {name} is linked to a different first step")
    return runtime


def _search_start_context(search, events, by_ref, errors):
    span_start = by_ref.get(search.get("start_ref") or search.get("start_event_ref"))
    if span_start is None:
        errors.append(f"X search {search.get('search_id')} has no raw span-start event")
        return None
    candidates = [event for event in events
                  if event.get("kind") == "search_start" and
                  event.get("pid") == span_start.get("pid") and
                  event.get("detail", {}).get("step_id") == search.get("step_id") and
                  event.get("event_number", math.inf) <= span_start.get("event_number", -math.inf)]
    if not candidates:
        errors.append(f"X search {search.get('search_id')} has no native search_start context")
        return None
    candidates.sort(key=lambda event: event.get("event_number", -1))
    start = candidates[-1]
    try:
        scheduled = finite_number(start.get("detail", {}).get("scheduled_time"), "search scheduled_time")
        deadline = finite_number(start.get("detail", {}).get("deadline"), "search deadline")
    except (TypeError, ValueError) as exc:
        errors.append(f"X search {search.get('search_id')}: {exc}")
        return None
    return dict(event=start, scheduled_time=scheduled, deadline=deadline)


def _publication_integrity(events, by_ref, native, case, peer_ids, errors):
    """Audit X publication expirations and observed native/renew cadence."""
    searches, assigned = _step26._assign_x_searches(native, events, by_ref, errors)
    x_peer = peer_ids.get(X_NAME)
    x_searches = [row for row in searches if row.get("peer_id") == x_peer]
    active_by_search = defaultdict(list)
    active_rows = []
    for event, search in assigned:
        if event.get("kind") != "ad_started" or event.get("detail", {}).get("active") is not True:
            continue
        active_rows.append(event)
        if search is None:
            errors.append(f"active X publication {_ref(event)} has no same-process native search")
            continue
        active_by_search[search["search_id"]].append(event)

    contexts = {search["search_id"]: _search_start_context(search, events, by_ref, errors)
                for search in x_searches}
    case_lifetime = float(case.get("lifetime", 0.0))
    cadence_rows = []
    ad_finish_by_start = {}
    for event in active_rows:
        detail = event.get("detail", {})
        expiration = None
        try:
            expiration = finite_number(detail.get("expiration"), "active X ad expiration")
            remaining = expiration - finite_number(event.get("wall_time"), "active X ad wall time")
            if remaining <= 0:
                errors.append(f"active X publication {_ref(event)} is expired when sent")
        except (TypeError, ValueError) as exc:
            errors.append(f"active X publication {_ref(event)}: {exc}")
            continue
        search = next((row for row in x_searches
                       if row.get("pid") == event.get("pid") and
                       row.get("start", math.inf) <= event.get("monotonic", -math.inf) <=
                       row.get("end", -math.inf)), None)
        context = contexts.get(search.get("search_id")) if search else None
        if context is not None:
            try:
                expected = min(max(context["scheduled_time"],
                                   finite_number(event.get("wall_time"), "ad wall time") + case_lifetime),
                               context["deadline"])
                if abs(expiration - expected) > _AD_FORMULA_TOLERANCE:
                    errors.append(f"X publication {_ref(event)} expiration differs from the native formula")
            except (TypeError, ValueError) as exc:
                errors.append(f"X publication {_ref(event)} formula audit: {exc}")
        finishes = [row for row in events
                    if row.get("pid") == event.get("pid") and
                    row.get("kind") in ("ad_finished", "ad_cancelled") and
                    row.get("detail", {}).get("active") is True and
                    row.get("detail", {}).get("expiration") == detail.get("expiration") and
                    row.get("event_number", -1) > event.get("event_number", math.inf)]
        finishes.sort(key=lambda row: row.get("event_number", math.inf))
        if finishes:
            ad_finish_by_start[_ref(event)] = finishes[0]
        else:
            errors.append(f"active X publication {_ref(event)} has no finish/cancel event")

    pause_spans = defaultdict(list)
    active_pauses = {}
    for event in sorted(events, key=lambda row: (row.get("pid", -1),
                                                  row.get("event_number", -1))):
        if event.get("kind") not in ("read_paused", "read_resumed"):
            continue
        detail = event.get("detail", {})
        key = (event.get("pid"), detail.get("search_id"))
        if event["kind"] == "read_paused":
            active_pauses[key] = event
        else:
            start_event = active_pauses.pop(key, None)
            if start_event is not None:
                pause_spans[key].append((start_event.get("monotonic"), event.get("monotonic")))

    by_search_rows = []
    for search in x_searches:
        ads = sorted(active_by_search.get(search["search_id"], []),
                     key=lambda event: (event.get("monotonic", 0), event.get("event_number", 0)))
        for previous, following in zip(ads, ads[1:]):
            detail = previous.get("detail", {})
            finish = ad_finish_by_start.get(_ref(previous))
            try:
                remaining = finite_number(detail.get("expiration"), "previous expiration") - \
                    finite_number(previous.get("wall_time"), "previous publication wall time")
                write_duration = (finite_number(finish.get("detail", {}).get("latency"), "publication write duration")
                                  if finish and finish.get("kind") == "ad_finished" else 0.0)
                if case.get("publication_policy") == "renew":
                    expected_gap = max(write_duration, min(remaining, 1.0))
                else:
                    expected_gap = max(write_duration, remaining)
                gap = finite_number(following.get("monotonic"), "next publication monotonic") - \
                    finite_number(previous.get("monotonic"), "previous publication monotonic")
                if gap + _CADENCE_TOLERANCE < expected_gap:
                    errors.append(f"X publication interval in SEARCH {search['search_id']} is earlier than "
                                  f"{case.get('publication_policy')} cadence: {gap:.3f}s vs minimum "
                                  f"{expected_gap:.3f}s")
                interval_start = finite_number(previous.get("monotonic"), "previous publication monotonic")
                interval_end = interval_start + expected_gap
                spans = pause_spans.get((search.get("pid"), search.get("search_id")), [])
                pause_overlap = sum(max(0.0, min(interval_end, float(end)) -
                                        max(interval_start, float(start)))
                                    for start, end in spans if start is not None and end is not None)
                paused_at_expected_send = any(
                    start is not None and end is not None and
                    float(start) <= interval_end <= float(end) for start, end in spans)
                lateness = max(0.0, gap - expected_gap)
                cadence_rows.append(dict(search_id=search["search_id"],
                                         previous_publication_event_ref=_ref(previous),
                                         next_publication_event_ref=_ref(following),
                                         observed_interval_seconds=gap,
                                         expected_interval_seconds=expected_gap,
                                         scheduler_lateness_seconds=lateness,
                                         read_pause_overlap_seconds=pause_overlap,
                                         read_paused_at_expected_send=paused_at_expected_send,
                                         policy=case.get("publication_policy")))
            except (TypeError, ValueError) as exc:
                errors.append(f"X publication cadence audit in SEARCH {search['search_id']}: {exc}")
        context = contexts.get(search["search_id"])
        by_search_rows.append(dict(
            search_id=search["search_id"], step_id=search.get("step_id"),
            search_start_event_ref=search.get("start_ref"), search_end_event_ref=search.get("end_ref"),
            active_publication_count=len(ads),
            active_publication_event_refs=[_ref(event) for event in ads],
            expiration_values=[event.get("detail", {}).get("expiration") for event in ads],
            lifetime_seconds=case_lifetime,
            scheduled_time=context.get("scheduled_time") if context else None,
            search_deadline=context.get("deadline") if context else None))
    return dict(pass_=not errors, errors=errors,
                summary=dict(publication_policy=case.get("publication_policy"),
                             x_search_count=len(x_searches), active_publication_count=len(active_rows),
                             active_publication_count_by_search=by_search_rows,
                             observed_cadence=cadence_rows,
                             lifetime_formula="min(max(scheduled_time, publication_wall_time + min_matchmaking_time), search_deadline)"))


def _conditional_join_integrity(case, peer_ids, events, rpc, errors):
    if case.get("behavior") != "recruit_only":
        return dict(required=False, outbound_join_count=None)
    x_peer = peer_ids.get(X_NAME)
    starts = [event for event in events if event.get("kind") == "join_attempt_start" and
              event.get("peer") == x_peer]
    rows = [row for row in rpc.get("summary", {}).get("rpc_rows", [])
            if row.get("rpc_type") == "join" and row.get("source_peer") == x_peer]
    if starts:
        errors.append(f"recruit_only X made {len(starts)} outbound native join attempts")
    if rows:
        errors.append(f"recruit_only X initiated {len(rows)} outbound control join RPCs")
    return dict(required=True, outbound_join_count=len(starts), outbound_join_rpc_count=len(rows))


def _choice_rows(attempts):
    output = []
    for attempt in attempts:
        if attempt.get("peer_name") not in HONEST_NAMES:
            continue
        output.append({key: attempt.get(key) for key in (
            "peer_name", "peer_id", "step_id", "start_event_ref", "started_monotonic",
            "submission_started", "first_outgoing_choice", "first_outgoing_join_id",
            "first_outgoing_join_event_ref", "first_outgoing_join_peer_id",
            "first_outgoing_join_peer_name", "accepted_x_reply_count",
            "accepted_other_reply_count", "accepted_x_reply_event_refs",
            "accepted_other_reply_event_refs", "first_group_leader_category",
            "first_group_leader_peer_id", "first_group_leader_name", "first_group_event_ref",
            "first_assembly_group_id", "first_assembly_event_ref",
            "first_assembly_latency_seconds", "membership_success")})
    return output


def _policy_metrics(attempts, groups, config):
    x_attempts = [row for row in attempts if row.get("peer_name") == X_NAME]
    honest_attempts = [row for row in attempts if row.get("peer_name") in HONEST_NAMES]
    x_led = [row for row in x_attempts if row.get("led_success")]
    x_success_lead = [row.get("lead_latency_seconds") for row in x_led
                      if row.get("lead_latency_seconds") is not None]
    x_any_group = [row.get("first_assembly_latency_seconds") for row in x_attempts
                   if row.get("membership_success") and row.get("first_assembly_latency_seconds") is not None]
    x_outcomes = Counter(row.get("native_eventual_outcome") for row in x_attempts)
    honest_first_choice = Counter(row.get("first_outgoing_choice") for row in honest_attempts)
    honest_first_group = Counter(row.get("first_group_leader_category") for row in honest_attempts)
    x_led_groups = [row for row in groups if row.get("leader_name") == X_NAME]
    honest_group_sizes = [row.get("roster_size") for row in groups
                          if row.get("honest_membership_count")]
    metrics = dict(
        attempt_count=len(attempts), x_submitted_attempts=len(x_attempts),
        honest_submitted_attempts=len(honest_attempts), total_groups=len(groups),
        x_led_attempts=len(x_led), x_led_rate=(len(x_led) / len(x_attempts) if x_attempts else None),
        x_followed_attempts=x_outcomes.get("followed", 0),
        x_no_group_attempts=x_outcomes.get("no_group", 0),
        x_failed_attempts=x_outcomes.get("failed", 0),
        x_outcome_counts={name: x_outcomes.get(name, 0)
                          for name in ("led", "followed", "no_group", "failed")},
        x_success_only_lead_latency_count=len(x_success_lead),
        x_success_only_mean_lead_latency_seconds=_mean(x_success_lead),
        x_success_only_median_lead_latency_seconds=_median(x_success_lead),
        x_success_only_p90_lead_latency_seconds=_percentile(x_success_lead, 0.9),
        x_capped_lead_score_mean_seconds=_mean(
            [row.get("lead_capped_latency_seconds") for row in x_attempts]),
        x_capped_lead_score_median_seconds=_median(
            [row.get("lead_capped_latency_seconds") for row in x_attempts]),
        x_capped_lead_score_definition=(
            "min(observed first own-led assembly latency, 12s); 12 for an X step_start without an "
            "own-led assembly, including followed, no_group, and failed outcomes; this is a score, "
            "not actual elapsed wait for followed attempts"),
        x_any_group_success_count=len(x_any_group),
        x_success_only_mean_any_group_latency_seconds=_mean(x_any_group),
        x_success_only_median_any_group_latency_seconds=_median(x_any_group),
        x_capped_any_group_latency_mean_seconds=_mean(
            [row.get("capped_latency_seconds") for row in x_attempts]),
        x_capped_any_group_latency_median_seconds=_median(
            [row.get("capped_latency_seconds") for row in x_attempts]),
        x_any_group_latency_definition=(
            "first assembled group containing X in its exact step; successful timing excludes no-group "
            "attempts, while capped mean assigns 12s to attempts without an assembly"),
        x_led_group_count=len(x_led_groups),
        x_led_group_mean_roster_size=_mean([row.get("roster_size") for row in x_led_groups]),
        x_led_group_median_roster_size=_median([row.get("roster_size") for row in x_led_groups]),
        group_roster_size_counts=dict(sorted(Counter(row.get("roster_size") for row in groups).items(),
                                             key=lambda item: str(item[0]))),
        honest_group_roster_size_mean=_mean(honest_group_sizes),
        honest_first_outgoing_choice_denominator=len(honest_attempts),
        honest_first_outgoing_choice_x=honest_first_choice.get("X", 0),
        honest_first_outgoing_choice_other=honest_first_choice.get("other", 0),
        honest_first_outgoing_choice_none=honest_first_choice.get("none", 0),
        honest_first_outgoing_choice_counts={key: honest_first_choice.get(key, 0)
                                             for key in ("X", "other", "none")},
        honest_first_native_group_denominator=len(honest_attempts),
        honest_first_native_group_x=honest_first_group.get("X", 0),
        honest_first_native_group_other=honest_first_group.get("other", 0),
        honest_first_native_group_no_group=honest_first_group.get("no_group", 0),
        honest_first_native_group_counts={key: honest_first_group.get(key, 0)
                                          for key in ("X", "other", "no_group")},
        honest_accepted_x_replies=sum(row.get("accepted_x_reply_count", 0)
                                      for row in honest_attempts),
        honest_accepted_other_replies=sum(row.get("accepted_other_reply_count", 0)
                                          for row in honest_attempts),
        honest_accepted_x_reply_event_refs=[ref for row in honest_attempts
                                             for ref in row.get("accepted_x_reply_event_refs", [])],
        honest_accepted_other_reply_event_refs=[ref for row in honest_attempts
                                                 for ref in row.get("accepted_other_reply_event_refs", [])],
        prior_default_behavior_highlead_control="prior confounded control; excluded from current policy comparison",
        submission_duration_seconds=config.get("duration"),
        all_attempt_denominators_include_every_raw_step_start=True,
    )
    for threshold in (2, 3, 5):
        count = sum(row.get("lead_latency_seconds") is not None and
                    row["lead_latency_seconds"] <= threshold for row in x_attempts)
        metrics[f"x_led_by_{threshold}s_count"] = count
        metrics[f"x_led_by_{threshold}s_rate"] = count / len(x_attempts) if x_attempts else None
    return metrics


def analyse_trial(path) -> dict:
    """Analyze a completed Step 29 raw trial and preserve all event references."""
    path = Path(path)
    config = read_json(path / "config.json")
    errors = []
    case, peer_ids, seed, duration, started = _validate_config(config, errors)
    native = analyse_trial_choices(path)
    events = _event_stream(path)
    by_kind, by_ref = _step28._event_groups(events)
    clock = _step28._clock_integrity(events, errors, config.get("experiment") == "step29")
    runtime = _runtime_integrity(config, events, native, case, peer_ids, errors)
    rpc = _step28._rpc_integrity(case, config, events, by_kind, by_ref, native)
    errors.extend(native.get("trace_integrity_errors", []))
    errors.extend(native.get("step24_control_integrity_errors", []))
    errors.extend(f"rpc: {error}" for error in rpc.get("errors", []))
    _step28._guard_new_control_tail(rpc, events, errors)
    summary = read_json(path / "summary.json") if (path / "summary.json").is_file() else {}
    if summary.get("error") is not None or summary.get("pending"):
        errors.append("trial summary reports an error or pending peers")
    trace_closed = [event for event in events if event.get("kind") == "trace_closed"]
    if len(trace_closed) != 1 or (events and events[-1].get("kind") != "trace_closed"):
        errors.append("raw event trace must end with one trace_closed event")

    x_peer = peer_ids.get(X_NAME)
    groups = _step28._group_rows(native, events, by_ref, config, peer_ids, errors)
    attempts = _step28._attempt_rows(native, groups, config, peer_ids, events, by_ref, errors)
    joins = _step28._annotate_attempt_choices(attempts, native, groups, peer_ids, by_ref)
    retries = _step28._retry_rows(native, events, by_ref, peer_ids, groups)
    publication_errors = []
    publications = _publication_integrity(events, by_ref, native, case, peer_ids, publication_errors)
    errors.extend(f"publication: {error}" for error in publication_errors)
    conditional_joins = _conditional_join_integrity(case, peer_ids, events, rpc, errors)
    unexpected_forbidden = [event for event in events
                            if event.get("kind") in ("forbidden_attacker_read", "forbidden_attacker_join")]
    if case.get("behavior") == "recruit_only" and unexpected_forbidden:
        errors.append(f"recruit_only X emitted {len(unexpected_forbidden)} forbidden read/join markers")

    metrics = _step28._trial_metrics(native, attempts, groups, retries, [], config, peer_ids)
    metrics.update(_policy_metrics(attempts, groups, config))
    metrics.update(
        clock_integrity=clock,
        runtime_matchmaking_config=runtime,
        publication_integrity_pass=publications.get("pass_") is True,
        publication_policy_summary=publications.get("summary", {}),
        conditional_x_outbound_join_audit=conditional_joins,
        configured_one_way_delay_seconds=case.get("one_way_delay_seconds"),
        behavior=case.get("behavior"),
        publication_policy=case.get("publication_policy"),
        lifetime_seconds=case.get("lifetime"),
        refresh_frequency_per_second=case.get("frequency"),
    )
    # Selection-read choices remain available as a distinct observation. The
    # primary choice table below is one row per honest native step_start.
    choices = _choice_rows(attempts)
    integrity_errors = sorted(set(errors))
    rpc_summary = {key: value for key, value in rpc.get("summary", {}).items()
                   if key != "rpc_rows"}
    return dict(
        experiment="step29", trial_dir=path.name, case=case, seed=seed, duration=duration,
        integrity_pass=bool(native.get("control_plane_pass") and not integrity_errors),
        integrity_errors=integrity_errors,
        native_control_plane_pass=native.get("control_plane_pass"),
        trace_integrity_pass=native.get("trace_integrity_pass"),
        clock_integrity=clock,
        runtime_matchmaking_integrity_pass=not any("runtime" in error for error in errors),
        publication_integrity=dict(pass_=publications.get("pass_") is True,
                                   errors=publication_errors,
                                   summary=publications.get("summary", {})),
        delay_integrity=dict(pass_=rpc.get("pass_") is True, errors=rpc.get("errors", []),
                             summary=rpc_summary),
        conditional_join_integrity=conditional_joins,
        summary_metrics=metrics, leadership_summary=metrics,
        attempts=attempts, groups=groups, choices=choices, joins=joins,
        retries=retries,
    )


def _csv_value(value):
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
    with Path(path).open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _csv_value(row.get(key)) for key in fields})


def _pooled_case_metrics(trials):
    attempts = [row for trial in trials for row in trial.get("attempts", [])]
    groups = [row for trial in trials for row in trial.get("groups", [])]
    metrics = _policy_metrics(attempts, groups, {"duration": sum(
        float(trial.get("duration") or 0.0) for trial in trials)})
    # This is descriptive pooling. The one-seed pilot does not estimate a
    # population effect or provide a significance test.
    metrics["trial_count"] = len(trials)
    metrics["all_trials_integrity_pass"] = all(row.get("integrity_pass") is True for row in trials)
    metrics["seed_count"] = len({row.get("seed") for row in trials})
    return metrics


def _summary_trial_row(trial, phase):
    case = trial.get("case", {})
    metrics = trial.get("summary_metrics", {})
    return dict(trial_dir=trial.get("trial_dir"), phase=phase,
                case=case.get("name"), behavior=case.get("behavior"),
                publication_policy=case.get("publication_policy"), lifetime=case.get("lifetime"),
                seed=trial.get("seed"), duration=trial.get("duration"),
                integrity_pass=trial.get("integrity_pass"), **{
                    key: value for key, value in metrics.items()
                    if not isinstance(value, (dict, list))})


def export_suite(suite) -> dict:
    """Export pooled Step 29 summaries from the suite manifest and trial analyses."""
    suite = Path(suite)
    manifest = read_json(suite / "suite.json")
    phase = manifest.get("phase")
    behaviors = manifest.get("behaviors", "both")
    errors, missing, trials = [], [], []
    try:
        expected_jobs = _canonical_jobs(phase, behaviors)
    except ValueError as exc:
        expected_jobs = []
        errors.append(str(exc))
    if manifest.get("experiment") != "step29" or manifest.get("jobs") != expected_jobs:
        errors.append("suite manifest differs from the frozen Step 29 job matrix")
    if manifest.get("outcome") != "grouping" or manifest.get("instrumentation") != "detailed":
        errors.append("Step 29 suite requires detailed grouping-only instrumentation")

    attempts, groups, choices, joins = [], [], [], []
    for job in manifest.get("jobs", []):
        case, seed = job.get("case", {}), job.get("seed")
        trial_dir = f"{case.get('name')}_s{seed}"
        trial_path = suite / trial_dir
        control_path = trial_path / "control_analysis.json"
        if not control_path.is_file():
            missing.append(trial_dir)
            continue
        try:
            trial = read_json(control_path)
            if trial.get("trial_dir") != trial_dir or trial.get("case") != case or \
                    trial.get("seed") != seed or trial.get("duration") != job.get("duration"):
                errors.append(f"{trial_dir}: control_analysis metadata differs from suite job")
            trials.append(trial)
            if not trial.get("integrity_pass"):
                errors.extend(f"{trial_dir}: {error}" for error in trial.get("integrity_errors", []))
            prefix = dict(trial_dir=trial_dir, case=case.get("name"),
                          behavior=case.get("behavior"), publication_policy=case.get("publication_policy"),
                          lifetime=case.get("lifetime"), seed=seed)
            attempts.extend(dict(prefix, **row) for row in trial.get("attempts", []))
            groups.extend(dict(prefix, **row) for row in trial.get("groups", []))
            choices.extend(dict(prefix, **row) for row in trial.get("choices", []))
            joins.extend(dict(prefix, **row) for row in trial.get("joins", []))
        except (OSError, ValueError, TypeError, KeyError) as exc:
            errors.append(f"{trial_dir}: {type(exc).__name__}: {exc}")
    complete = len(trials) == len(expected_jobs) and not missing
    integrity_pass = complete and not errors and all(row.get("integrity_pass") is True for row in trials)
    by_case = defaultdict(list)
    for trial in trials:
        by_case[trial.get("case", {}).get("name")].append(trial)
    pooled = {name: _pooled_case_metrics(rows) for name, rows in sorted(by_case.items())}
    leadership_rows = [_summary_trial_row(trial, phase) for trial in trials]
    for case_name, metrics in sorted(pooled.items()):
        case = by_case[case_name][0].get("case", {})
        leadership_rows.append(dict(trial_dir="POOLED", phase=phase, case=case_name,
                                    behavior=case.get("behavior"),
                                    publication_policy=case.get("publication_policy"),
                                    lifetime=case.get("lifetime"), seed="pooled",
                                    duration=sum(float(row.get("duration") or 0.0)
                                                 for row in by_case[case_name]),
                                    **metrics))

    for filename, rows in (("attempts.csv", attempts), ("groups.csv", groups),
                           ("choices.csv", choices), ("joins.csv", joins),
                           ("leadership_summary.csv", leadership_rows)):
        _write_csv(suite / filename, rows)

    summary = dict(experiment="step29", phase=phase, behaviors=behaviors,
                   complete=complete, integrity_pass=integrity_pass,
                   job_count=len(expected_jobs), analyzed_trial_count=len(trials),
                   missing_trials=missing, integrity_errors=sorted(set(errors)),
                   pooled_by_case=pooled,
                   trial_summaries=[_summary_trial_row(trial, phase) for trial in trials],
                   interpretation=("Descriptive local loopback pilot with one pilot seed; no significance "
                                   "claim. Normal behavior allows X to follow. recruit_only forbids X's "
                                   "outbound joins, and that audit is conditional on that behavior."))
    (suite / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True,
                                                   allow_nan=False) + "\n", encoding="utf-8")
    _write_report(suite / "REPORT.md", summary, leadership_rows)
    return summary


def _write_report(path, summary, rows):
    lines = ["# Step 29 local advertisement-policy pilot", "",
             f"Phase: `{summary.get('phase')}`; behavior set: `{summary.get('behaviors')}`.", "",
             "This is a local eight-peer loopback comparison with zero injected network delay. "
             "The pilot uses one seed and is descriptive; it does not support a significance claim.", "",
             "Normal behavior allows X to follow another peer. `recruit_only` prohibits X outbound joins; "
             "the no-outbound-join integrity check applies only to those cases.", "",
             "Each row in `choices.csv` is an honest native `step_start`. Its first outgoing destination "
             "(X, another peer, or none), accepted native replies, and first assembled roster are reported "
             "as separate outcomes.", "",
             "The X capped lead score assigns 12 seconds to any X step without an own-led assembly, "
             "including followed steps. It is a score, not an observed wait for followed attempts. "
             "Successful own-led times are summarized separately. The prior default-behavior/high-lead "
             "control is a confounded prior control and is excluded from this comparison.", "",
             "| Case | X-led / starts (%) | Followed / no group / failed | Honest first choice X / other / none | "
             "Honest first roster X / other / none | X-led groups (mean roster) | Successful own-led median (s) | "
             "Capped lead score mean (s) | All groups |",
             "|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for row in rows:
        if row.get("trial_dir") == "POOLED":
            choices = row.get("honest_first_outgoing_choice_counts", {})
            rosters = row.get("honest_first_native_group_counts", {})
            choice_text = "/".join(str(choices.get(key, 0)) for key in ("X", "other", "none"))
            roster_text = "/".join(str(rosters.get(key, 0)) for key in ("X", "other", "no_group"))
            lines.append("| {case} (pooled) | {led}/{starts} ({rate}) | {followed}/{nogroup}/{failed} | "
                         "{choices} | {rosters} | {xgroups} ({roster_mean}) | {median} | {score} | {groups} |".format(
                             case=row.get("case"), starts=row.get("x_submitted_attempts", 0),
                             led=row.get("x_led_attempts", 0), rate=_percent(row.get("x_led_rate")),
                             followed=row.get("x_followed_attempts", 0),
                             nogroup=row.get("x_no_group_attempts", 0),
                             failed=row.get("x_failed_attempts", 0), choices=choice_text,
                             rosters=roster_text, xgroups=row.get("x_led_group_count", 0),
                             roster_mean=_display(row.get("x_led_group_mean_roster_size")),
                             median=_display(row.get("x_success_only_median_lead_latency_seconds")),
                             score=_display(row.get("x_capped_lead_score_mean_seconds")),
                             groups=row.get("total_groups", 0)))
    lines.extend(["", "The native expiration audit checks each X publication against "
                  "`min(max(scheduled_time, publication_time + configured minimum), search_deadline)`. "
                  "Renewal cadence is checked against the 1-second pre-expiry publication loop. See "
                  "`groups.csv`, `attempts.csv`, `choices.csv`, and `joins.csv` for raw event references.", "",
                  f"Suite integrity pass: `{summary.get('integrity_pass')}`."])
    if summary.get("integrity_errors"):
        lines.extend(["", "Integrity errors:", ""])
        lines.extend(f"- {error}" for error in summary["integrity_errors"])
    Path(path).write_text("\n".join(lines) + "\n", encoding="utf-8")


def _display(value):
    return "" if value is None else f"{float(value):.3f}"


def _percent(value):
    return "" if value is None else f"{100.0 * float(value):.1f}%"


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("suite", type=Path)
    parser.add_argument("--trial", type=Path)
    args = parser.parse_args()
    result = analyse_trial(args.trial) if args.trial else export_suite(args.suite)
    print(json.dumps(result, indent=2, sort_keys=True, allow_nan=False))
