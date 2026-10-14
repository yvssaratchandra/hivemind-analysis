"""Independent raw-trace checks for the Step 30 grouping comparison.

This module does not import the Step 30 analyzer. It rebuilds the primary
membership, wait, group-rate, receipt, retry, publication, and delay results
from frozen inputs, then checks that the analyzer and its tables preserve
those observations.
"""
from __future__ import annotations

import csv
import hashlib
import json
import math
from pathlib import Path
import statistics


ROOT = Path(__file__).resolve().parent
X_NAME = "X"
HONEST_NAMES = tuple(f"H{i}" for i in range(7))
CAP_SECONDS = 12.0
PROFILE = {"distribution": "uniform", "lower": 0.1, "upper": 0.2}
TRAINING_SIGMA = math.sqrt(math.log(1.0 + 0.6 ** 2))
TRAINING = {"mu": math.log(1.5) - TRAINING_SIGMA ** 2 / 2,
            "sigma": TRAINING_SIGMA, "lower": 0.1, "upper": 4.0}
DECISION_RULE = {
    "wait_increase_seconds": 0.5, "failure_increase_fraction": 0.05,
    "group_rate_decrease_fraction": 0.15, "minimum_threshold_seeds": 2,
    "require_third_same_direction": True, "minimum_late_windows": 3,
    "minimum_persistent_seeds": 2, "honest_starts_per_trial": 100,
    "x_client_acceptances_per_trial": 20, "x_multi_publication_searches": 10,
    "maximum_renewal_to_stock_median_interval": 0.75,
}
CASE_ROWS = {
    "stock_2s": {"name": "stock_2s", "mode": "passive", "behavior": "recruit_only",
                 "publication_policy": "native", "lifetime": 2.0, "minimum": 2,
                 "target": 4, "response": "unreliable", "hold_seconds": 0.5,
                 "one_way_delay_seconds": 0.15},
    "renew_1_2": {"name": "renew_1_2", "mode": "passive", "behavior": "recruit_only",
                  "publication_policy": "renew", "lifetime": 2.0, "minimum": 2,
                  "target": 4, "response": "unreliable", "hold_seconds": 0.5,
                  "one_way_delay_seconds": 0.15, "frequency": 1.0,
                  "nominal_lower": 1.0, "nominal_upper": 2.0},
}
BASE_HARNESS = (
    "step28_confirmation.py", "step28_workload.py", "analyze_step28_confirmation.py",
    "test_step28_confirmation.py", "verify_step28_confirmation.py",
    "analyze_step27_control_plane.py", "verify_step27_raw.py", "audit_step27_receipts.py",
    "step26_joint_sweep.py", "analyze_step26_joint.py", "step26_teardown_audit.py",
    "step26_teardown_audit_v2.py", "step25_choice_measurement.py", "step24_lifetime_sweep.py",
    "step22_refresh_leadership.py", "analyze_step24_lifetime.py", "analyze_step25_choices.py",
    "test_step25_harness.py", "test_step25_choices.py",
)
REQUIRED_HARNESS = frozenset(BASE_HARNESS + (
    "step29_policies.py", "step29_workload.py", "analyze_step29_policies.py",
    "test_step29_policies.py", "verify_step28_exports_v2.py", "step30_harm.py",
    "step30_workload.py", "analyze_step30_harm.py", "verify_step30_harm.py",
    "test_step30_harm.py",
))
CSV_TABLES = ("attempts", "groups", "choices", "joins", "retries", "publications", "windows",
              "states", "trial_summary")


def _canonical_jobs(phase):
    cases = [CASE_ROWS["stock_2s"], CASE_ROWS["renew_1_2"]]
    if phase == "smoke":
        return [dict(case=case, seed=3000, duration=20.0) for case in cases]
    if phase == "confirm":
        return [dict(case=case, seed=seed, duration=300.0)
                for seed in (3001, 3002, 3003)
                for case in (list(reversed(cases)) if seed == 3002 else cases)]
    return []


def _read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _ref(event):
    return f"{event['pid']}:{event['event_number']}"


def _finite(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _close(actual, expected, tolerance=1e-8):
    if expected is None:
        return actual is None or actual in ("", "None", "null")
    try:
        return math.isclose(float(actual), float(expected), rel_tol=0.0, abs_tol=tolerance)
    except (TypeError, ValueError):
        return False


def _csv_value(value):
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return value


def _same_cell(actual, expected):
    actual = _csv_value(actual)
    if expected is None:
        return actual is None or actual in ("", "None", "null")
    if isinstance(expected, bool):
        return actual is expected or str(actual).lower() == str(expected).lower()
    if isinstance(expected, (int, float)) and not isinstance(expected, bool):
        return _close(actual, expected)
    if isinstance(expected, (dict, list)):
        return actual == expected
    return actual == str(expected)


def _read_events(path, errors):
    events = []
    trace = Path(path) / "events.jsonl"
    try:
        with trace.open(encoding="utf-8") as stream:
            for line_number, line in enumerate(stream, 1):
                if not line.strip():
                    errors.append(f"blank event trace line {line_number}")
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError as exc:
                    errors.append(f"malformed event JSON at line {line_number}: {exc}")
                    continue
                if not isinstance(event, dict) or not isinstance(event.get("detail"), dict):
                    errors.append(f"event line {line_number} is not a record with detail")
                    continue
                events.append(event)
    except OSError as exc:
        errors.append(f"cannot read raw event trace: {exc}")
    return events


def _check_event_closure(events, config, errors):
    refs = set()
    by_pid = {}
    for event in events:
        try:
            pid, number = int(event["pid"]), int(event["event_number"])
            ref = (pid, number)
        except (KeyError, TypeError, ValueError):
            errors.append("raw event has invalid pid/event_number")
            continue
        if ref in refs:
            errors.append(f"duplicate raw event ref {pid}:{number}")
        refs.add(ref)
        if not _finite(event.get("monotonic")):
            errors.append(f"nonfinite monotonic time at {pid}:{number}")
        if not _finite(event.get("wall_time")):
            errors.append(f"nonfinite wall time at {pid}:{number}")
        by_pid.setdefault(pid, []).append(event)
        peer_ids = config.get("peer_ids", {})
        if event.get("peer") not in ("", *peer_ids.values()) or event.get("other") not in ("", *peer_ids.values()):
            errors.append(f"unknown peer identity at {pid}:{number}")

    # A worker can have emitted events before the first captured record. Check
    # the captured sequence is continuous without assuming it begins at one.
    for pid, rows in by_pid.items():
        ordered = sorted(rows, key=lambda row: int(row["event_number"]))
        numbers = [int(row["event_number"]) for row in ordered]
        if any(right != left + 1 for left, right in zip(numbers, numbers[1:])):
            errors.append(f"event numbers are not contiguous for pid {pid}")
        if any(_finite(left.get("monotonic")) and _finite(right.get("monotonic")) and
               right["monotonic"] < left["monotonic"] or
               _finite(left.get("wall_time")) and _finite(right.get("wall_time")) and
               right["wall_time"] < left["wall_time"]
               for left, right in zip(ordered, ordered[1:])):
            errors.append(f"event clock decreases for pid {pid}")

    closures = [event for event in events if event.get("kind") == "trace_closed"]
    participants = {X_NAME, *HONEST_NAMES}
    if len(closures) != 1 or not events or events[-1].get("kind") != "trace_closed":
        errors.append("trace must end with exactly one trace_closed marker")
    elif (set(closures[0]["detail"].get("participants", [])) != participants or
          closures[0]["detail"].get("pending") != [] or closures[0]["detail"].get("error") is not None):
        errors.append("trace_closed does not show all peers stopped cleanly")


def _config_errors(config, errors):
    case = config.get("case") if isinstance(config, dict) else None
    if not isinstance(case, dict) or CASE_ROWS.get(case.get("name")) != case:
        errors.append("config case differs from the frozen Step 30 arms")
    if config.get("experiment") != "step30" or config.get("outcome") != "grouping":
        errors.append("config does not identify the Step 30 grouping trial")
    if config.get("control_only") is not True or config.get("tensor_transfer_enabled") is not False:
        errors.append("config does not enforce control-only execution")
    seed, duration, start = config.get("seed"), config.get("duration"), config.get("started_monotonic")
    if (seed, duration) not in ((3000, 20.0), (3001, 300.0), (3002, 300.0), (3003, 300.0)):
        errors.append("seed/duration differs from Step 30 smoke/confirm jobs")
    if not _finite(start):
        errors.append("config lacks finite started_monotonic")
    ids = config.get("peer_ids")
    if (not isinstance(ids, dict) or set(ids) != {X_NAME, *HONEST_NAMES} or
            any(not isinstance(value, str) or not value for value in ids.values()) or
            len(set(ids.values())) != 8):
        errors.append("config peer_ids must map X and H0..H6 to distinct IDs")
        ids = {}
    if config.get("network_seed") != seed:
        errors.append("network_seed differs from trial seed")
    if config.get("delay_profile") != PROFILE:
        errors.append("config delay profile differs from the seeded 100–200ms policy")
    return ids


def _primary_rows(events, config, peer_ids, errors):
    names = {peer: name for name, peer in peer_ids.items()}
    by_ref = {}
    for event in events:
        try:
            by_ref[_ref(event)] = event
        except (KeyError, TypeError):
            pass

    starts = {}
    attempts = []
    for event in events:
        if event.get("kind") != "step_start" or event.get("peer") not in names:
            continue
        detail = event["detail"]
        name, step_id = names[event["peer"]], detail.get("step_id")
        key = (event["peer"], step_id)
        if not isinstance(step_id, str) or key in starts:
            errors.append(f"duplicate or missing raw step_start identity at {_ref(event)}")
            continue
        starts[key] = event
        if name in HONEST_NAMES:
            attempts.append(dict(peer_id=event["peer"], peer_name=name, step_id=step_id,
                                 start_event_ref=_ref(event), started_monotonic=event["monotonic"]))

    groups_by_id = {}
    for event in events:
        if event.get("kind") != "assembled":
            continue
        detail = event["detail"]
        group_id, step_id = detail.get("group_id"), detail.get("step_id")
        if not isinstance(group_id, str) or not group_id:
            errors.append(f"leader assembly has no group id at {_ref(event)}")
            continue
        if group_id in groups_by_id:
            errors.append(f"duplicate leader assembly for group {group_id}")
            continue
        roster = detail.get("roster", [])
        if not isinstance(roster, list) or event.get("peer") not in roster:
            errors.append(f"leader assembly roster invalid for group {group_id}")
            roster = roster if isinstance(roster, list) else []
        groups_by_id[group_id] = dict(
            group_id=group_id, leader_peer_id=event.get("peer"),
            leader_name=names.get(event.get("peer")), leader_step_id=step_id,
            roster=roster, roster_size=len(roster), assembly_event_ref=_ref(event),
            assembly_monotonic=event.get("monotonic"), follower_assembly_event_refs=[],
            follower_corroborated=False)

    memberships = {}
    for event in events:
        kind = event.get("kind")
        if kind not in ("assembled", "follower_assembled"):
            continue
        detail = event["detail"]
        peer = event.get("peer")
        step_id = detail.get("step_id")
        group_id = detail.get("group_id")
        if kind == "follower_assembled":
            group = groups_by_id.get(group_id)
            if (group is None or event.get("other") != group.get("leader_peer_id") or
                    peer not in detail.get("roster", []) or peer not in group.get("roster", [])):
                errors.append(f"follower assembly does not corroborate leader roster for {group_id}")
                continue
            group["follower_assembly_event_refs"].append(_ref(event))
            group["follower_corroborated"] = True
        key = (peer, step_id)
        attempt = next((row for row in attempts if (row["peer_id"], row["step_id"]) == key), None)
        if attempt is None:
            continue
        if event.get("monotonic", -math.inf) < attempt["started_monotonic"]:
            errors.append(f"native group membership predates step_start {step_id}")
            continue
        membership = dict(group_id=group_id, event_ref=_ref(event),
                          assembly_monotonic=event["monotonic"], kind=kind)
        current = memberships.get(key)
        if current is None or (membership["assembly_monotonic"], membership["event_ref"]) < \
                (current["assembly_monotonic"], current["event_ref"]):
            memberships[key] = membership

    for attempt in attempts:
        key = (attempt["peer_id"], attempt["step_id"])
        membership = memberships.get(key)
        wait = (membership["assembly_monotonic"] - attempt["started_monotonic"]
                if membership else CAP_SECONDS)
        attempt.update(membership_success=membership is not None,
                       first_group_id=membership.get("group_id") if membership else None,
                       first_assembly_event_ref=membership.get("event_ref") if membership else None,
                       first_assembly_monotonic=membership.get("assembly_monotonic") if membership else None,
                       first_assembly_latency_seconds=wait if membership else None,
                       capped_latency_seconds=min(CAP_SECONDS, max(0.0, wait)))

    joins = _raw_joins(events, starts, peer_ids, errors)
    retries = _raw_retries(events, peer_ids, errors)
    publications = _raw_publications(events, config, peer_ids, errors)
    choices = _raw_choice_rows(attempts, list(groups_by_id.values()), joins, peer_ids, config)
    windows, metrics = _window_summary(attempts, list(groups_by_id.values()), config)
    x_peer = peer_ids.get(X_NAME)
    x_accept_ids = sorted({row["join_id"] for row in joins
                           if row["leader_peer_id"] == x_peer and "ACCEPTED" in row["reply_codes"]})
    x_disband_ids = sorted({row["join_id"] for row in joins
                            if row["leader_peer_id"] == x_peer and "GROUP_DISBANDED" in row["reply_codes"]})
    successful = [row for row in attempts if row["membership_success"]]
    capped = [row["capped_latency_seconds"] for row in attempts]
    observation_groups = [row for row in groups_by_id.values()
                          if config["started_monotonic"] <= row["assembly_monotonic"] <
                          config["started_monotonic"] + config["duration"]]
    drain = [row for row in groups_by_id.values()
             if row["assembly_monotonic"] >= config["started_monotonic"] + config["duration"]]
    metrics.update(
        honest_submitted_attempts=len(attempts), honest_group_successes=len(successful),
        honest_group_failures=len(attempts) - len(successful),
        honest_group_failure_fraction=((len(attempts)-len(successful))/len(attempts) if attempts else None),
        honest_group_success_fraction=(len(successful)/len(attempts) if attempts else None),
        honest_success_wait_count=len(successful),
        honest_mean_success_wait_seconds=(statistics.fmean(row["first_assembly_latency_seconds"]
                                                          for row in successful) if successful else None),
        honest_mean_capped_wait_seconds=(statistics.fmean(capped) if capped else None),
        total_groups=len(groups_by_id), observation_groups=len(observation_groups),
        drain_group_count=len(drain),
        follower_corroborated_groups=sum(row["follower_corroborated"] for row in groups_by_id.values()),
        uncorroborated_groups=sum(not row["follower_corroborated"] for row in groups_by_id.values()),
        honest_group_rate_per_minute=(len(observation_groups)/(config["duration"]/60)
                                      if config["duration"] else None),
        x_assembled_leader_groups=sum(row["leader_peer_id"] == x_peer for row in groups_by_id.values()),
        x_client_accept_join_count=len(x_accept_ids), x_client_disband_join_count=len(x_disband_ids),
        same_search_x_retry_count=len(retries),
        x_active_publication_count=len(publications),
        x_accepted_active_publication_count=sum(row["accepted"] is True for row in publications),
        x_search_count=len({row["search_id"] for row in publications if row.get("search_id")}),
        x_searches_with_multiple_accepted_publications=sum(
            len([row for row in publications if row.get("search_id") == search_id and row["accepted"] is True]) >= 2
            for search_id in {row["search_id"] for row in publications if row.get("search_id")}),
        x_publication_interval_median_seconds=statistics.median(
            gap for row in publications for gap in row.get("next_intervals", []))
            if any(row.get("next_intervals") for row in publications) else None,
        honest_first_outgoing_choice_counts=_choice_counts(choices),
        honest_ever_tried_x_attempts=sum(row["ever_tried_x"] for row in choices),
    )
    return dict(errors=errors, events=events, config=config, attempts=attempts,
                groups=list(groups_by_id.values()), choices=choices, joins=joins, retries=retries,
                publications=publications, windows=windows, summary_metrics=metrics,
                x_client_accept_join_ids=x_accept_ids,
                x_client_disband_join_ids=x_disband_ids,
                event_by_ref=by_ref)


def _raw_joins(events, starts, peer_ids, errors):
    names = {peer: name for name, peer in peer_ids.items()}
    join_starts, join_ends, client = {}, {}, {}
    for event in events:
        kind, detail = event.get("kind"), event.get("detail", {})
        if kind in ("join_attempt_start", "join_attempt_end"):
            join_id = detail.get("join_id")
            target = join_starts if kind == "join_attempt_start" else join_ends
            if not isinstance(join_id, str) or not join_id or join_id in target:
                errors.append(f"invalid or duplicate {kind} event")
                continue
            target[join_id] = event
        elif kind in ("native_client_wait", "native_client_reply", "native_client_exception"):
            join_id = detail.get("join_id")
            if not isinstance(join_id, str) or not join_id:
                errors.append(f"{kind} lacks join_id at {_ref(event)}")
                continue
            client.setdefault(join_id, []).append(event)
    rows = []
    for join_id, start in join_starts.items():
        detail = start["detail"]
        peer, leader, step_id = start.get("peer"), start.get("other"), detail.get("step_id")
        if names.get(peer) not in HONEST_NAMES or names.get(leader) is None:
            errors.append(f"join {join_id} has invalid honest caller or leader")
        if (peer, step_id) not in starts:
            errors.append(f"join {join_id} has no matching native step_start")
        end = join_ends.get(join_id)
        if end is not None and (end.get("peer") != peer or end.get("other") != leader or
                                end["detail"].get("step_id") != step_id or
                                end.get("monotonic", -math.inf) < start.get("monotonic", math.inf)):
            errors.append(f"join {join_id} end differs from its start")
        records = sorted(client.get(join_id, []), key=lambda row: (row.get("monotonic", 0),
                                                                    row.get("pid", -1),
                                                                    row.get("event_number", -1)))
        for record in records:
            if (record.get("peer") != peer or record.get("other") != leader or
                    record["detail"].get("step_id") != step_id or
                    record.get("monotonic", -math.inf) < start.get("monotonic", math.inf) or
                    (end and record.get("monotonic", math.inf) > end.get("monotonic", -math.inf))):
                errors.append(f"native client receipt identity/time mismatch for {join_id}")
        replies = [row for row in records if row.get("kind") == "native_client_reply"]
        waits = [row for row in records if row.get("kind") == "native_client_wait"]
        expires = {row["detail"].get("request_expiration") for row in waits
                   if _finite(row["detail"].get("request_expiration"))}
        if len(expires) > 1:
            errors.append(f"native client request expiration changes during join {join_id}")
        req_exp = next(iter(expires)) if len(expires) == 1 else None
        rpc_matches = [row for row in events if row.get("kind") == "control_rpc_start" and
                       row.get("detail", {}).get("rpc_type") == "join" and
                       row.get("other") == peer and row.get("peer") == leader and
                       row.get("detail", {}).get("caller_step_id") == step_id and
                       row.get("detail", {}).get("request_expiration") == req_exp and req_exp is not None and
                       start.get("monotonic", -math.inf) <= row.get("monotonic", math.inf) <=
                       (end.get("monotonic", math.inf) if end else math.inf)]
        if replies and len(rpc_matches) != 1:
            errors.append(f"join {join_id} server RPC linkage is not unique")
        linked_rpc = rpc_matches[0] if len(rpc_matches) == 1 else None
        rpc_id = linked_rpc.get("detail", {}).get("rpc_id") if linked_rpc else None
        server = [row for row in events if row.get("kind") == "control_join_admission_state" and
                  row.get("detail", {}).get("rpc_id") == rpc_id] if rpc_id else []
        local_leader_waits = [row for row in events if row.get("kind") == "native_leader_wait" and
                              row.get("peer") == leader and row.get("other") == peer and
                              start.get("monotonic", -math.inf) <= row.get("monotonic", math.inf) <=
                              (end.get("monotonic", math.inf) if end else math.inf)]
        selection_ref = detail.get("selection_event_ref")
        selected = next((row for row in events if _ref(row) == selection_ref), None) if selection_ref else None
        if detail.get("origin") == "queue" and (selected is None or
                selected.get("kind") != "native_pop_selected" or selected.get("peer") != peer or
                selected.get("other") != leader or selected["detail"].get("step_id") != step_id):
            errors.append(f"join {join_id} has invalid native queue selection reference")
        if detail.get("origin") not in ("queue", "redirect"):
            errors.append(f"join {join_id} has invalid origin")
        row = dict(join_id=join_id, peer_id=peer, peer_name=names.get(peer),
                   leader_peer_id=leader, leader_name=names.get(leader), step_id=step_id,
                   search_id=detail.get("search_id"), origin=detail.get("origin"),
                   selection_event_ref=selection_ref, start_event_ref=_ref(start),
                   end_event_ref=_ref(end) if end else None,
                   start_monotonic=start.get("monotonic"),
                   reply_codes=[reply["detail"].get("code") for reply in replies],
                   reply_event_refs=[_ref(reply) for reply in replies],
                   native_client_request_expiration=req_exp,
                   selected_leader_expiration=(selected or {}).get("detail", {}).get("selected_expiration"),
                   caller_advertised_expiration=(selected or {}).get("detail", {}).get("own_expiration"),
                   admission_request_expiration=(server[0].get("detail", {}).get("request_expiration")
                                                 if server else None),
                   admission_leader_expiration=(server[0].get("detail", {}).get("leader_expiration")
                                                if server else None),
                   local_leader_wait_captured_expirations=[row.get("detail", {}).get("captured_expiration")
                                                           for row in local_leader_waits],
                   linked_server_rpc_id=rpc_id,
                   server_rpc_link_status=("unique" if len(rpc_matches) == 1 else
                                           "ambiguous" if len(rpc_matches) > 1 else "unmatched"))
        rows.append(row)
    for join_id in join_ends.keys() - join_starts.keys():
        errors.append(f"join end without start: {join_id}")
    for join_id in client.keys() - join_starts.keys():
        errors.append(f"native client event without join start: {join_id}")
    return sorted(rows, key=lambda row: (row["start_event_ref"], row["join_id"]))


def _raw_retries(events, peer_ids, errors):
    x_peer = peer_ids.get(X_NAME)
    searches = [event for event in events if event.get("kind") == "search_span_start" and
                event.get("peer") == x_peer]
    search_ends = [event for event in events if event.get("kind") == "search_span_end" and
                   event.get("peer") == x_peer]
    intervals = []
    for start in searches:
        candidates = [end for end in search_ends if end.get("pid") == start.get("pid") and
                      end.get("detail", {}).get("search_id") == start.get("detail", {}).get("search_id")]
        if len(candidates) != 1:
            errors.append(f"X search {start['detail'].get('search_id')} lacks one end marker")
            continue
        end = candidates[0]
        intervals.append(dict(pid=start["pid"], search_id=start["detail"].get("search_id"),
                              step_id=start["detail"].get("step_id"), start=start.get("monotonic"),
                              end=end.get("monotonic")))
    ads = [event for event in events if event.get("kind") == "ad_started" and
           event.get("peer") == x_peer and event.get("detail", {}).get("active") is True]
    retries = []
    for selection in events:
        if selection.get("kind") != "native_pop_selected" or selection.get("other") != x_peer:
            continue
        detail = selection.get("detail", {})
        new_exp = detail.get("selected_expiration")
        top_wall = detail.get("selection_top_wall")
        if not (_finite(new_exp) and _finite(top_wall) and new_exp > top_wall):
            continue
        old_exps = sorted({float(item["expiration"]) for item in detail.get("past_attempts", [])
                           if isinstance(item, dict) and item.get("peer") == x_peer and
                           _finite(item.get("expiration")) and item["expiration"] < new_exp})
        honest_search = selection.get("detail", {}).get("search_id")
        honest_search_start = next((row for row in events
                                    if row.get("kind") == "search_span_start" and
                                    row.get("peer") == selection.get("peer") and
                                    row.get("detail", {}).get("search_id") == honest_search and
                                    row.get("detail", {}).get("step_id") == detail.get("step_id") and
                                    row.get("monotonic", math.inf) <= selection.get("monotonic", -math.inf)),
                                   None)
        honest_search_end = next((row for row in events
                                  if row.get("kind") == "search_span_end" and honest_search_start and
                                  row.get("pid") == honest_search_start.get("pid") and
                                  row.get("detail", {}).get("search_id") == honest_search), None)
        if (honest_search_start is None or honest_search_end is None or
                selection.get("monotonic", -math.inf) > honest_search_end.get("monotonic", math.inf)):
            continue
        candidates = []
        for old_exp in old_exps:
            for old in ads:
                if old["detail"].get("expiration") != old_exp or old.get("monotonic", math.inf) > selection["monotonic"]:
                    continue
                for new in ads:
                    if new["detail"].get("expiration") != new_exp or new.get("monotonic", math.inf) > selection["monotonic"]:
                        continue
                    common = [span for span in intervals if span["pid"] == old.get("pid") == new.get("pid") and
                              span["start"] <= selection["monotonic"] <= span["end"] and
                              span["start"] <= old["monotonic"] <= span["end"] and
                              span["start"] <= new["monotonic"] <= span["end"]]
                    for span in common:
                        candidates.append((old_exp, old, new, span))
        if candidates:
            old_exp, old, new, span = max(
                candidates, key=lambda row: (row[0], row[1]["monotonic"], row[1]["event_number"]))
            retries.append(dict(selection_event_ref=_ref(selection), peer_id=selection.get("peer"),
                                step_id=detail.get("step_id"), search_id=honest_search,
                                old_pair_expiration=old_exp, new_pair_expiration=float(new_exp),
                                old_pair_publication_event_ref=_ref(old),
                                new_pair_publication_event_ref=_ref(new),
                                x_search_id=span["search_id"]))
    unique = {}
    for row in retries:
        unique.setdefault(row["selection_event_ref"], row)
    return sorted(unique.values(), key=lambda row: row["selection_event_ref"])


def _raw_publications(events, config, peer_ids, errors):
    x_peer = peer_ids.get(X_NAME)
    search_starts = [event for event in events if event.get("kind") == "search_span_start" and
                     event.get("peer") == x_peer]
    search_ends = [event for event in events if event.get("kind") == "search_span_end" and
                   event.get("peer") == x_peer]
    searches = []
    for start in search_starts:
        ends = [end for end in search_ends if end.get("pid") == start.get("pid") and
                end["detail"].get("search_id") == start["detail"].get("search_id")]
        if len(ends) == 1:
            searches.append(dict(pid=start["pid"], search_id=start["detail"].get("search_id"),
                                 step_id=start["detail"].get("step_id"),
                                 start=start["monotonic"], end=ends[0]["monotonic"],
                                 start_event=start))
    active = sorted((event for event in events if event.get("kind") == "ad_started" and
                     event.get("peer") == x_peer and event.get("detail", {}).get("active") is True),
                    key=lambda row: (row.get("monotonic", math.inf), row.get("pid", -1),
                                     row.get("event_number", -1)))
    finish_events = [event for event in events if event.get("kind") in ("ad_finished", "ad_cancelled") and
                     event.get("peer") == x_peer and event.get("detail", {}).get("active") is True]
    rows = []
    grouped = {}
    for index, ad in enumerate(active):
        matches = [search for search in searches if search["pid"] == ad.get("pid") and
                   search["step_id"] == ad["detail"].get("step_id", search["step_id"]) and
                   search["start"] <= ad["monotonic"] <= search["end"]]
        if not matches:
            # The declaration wrapper does not repeat step_id on every version;
            # same-process open search is the native context when exactly one exists.
            matches = [search for search in searches if search["pid"] == ad.get("pid") and
                       search["start"] <= ad["monotonic"] <= search["end"]]
        if len(matches) != 1:
            errors.append(f"active X publication {_ref(ad)} does not belong to one native search")
            continue
        search = matches[0]
        next_ad_number = min((row.get("event_number", math.inf) for row in active
                              if row.get("pid") == ad.get("pid") and row.get("event_number", -1) > ad.get("event_number", -1)),
                             default=math.inf)
        finishes = [row for row in finish_events if row.get("pid") == ad.get("pid") and
                    row.get("event_number", -1) > ad.get("event_number", -1) and
                    row.get("event_number", math.inf) < next_ad_number and
                    row.get("detail", {}).get("expiration") == ad["detail"].get("expiration")]
        if len(finishes) != 1:
            errors.append(f"active X publication {_ref(ad)} lacks one matching finish/cancel record")
            finish = None
        else:
            finish = finishes[0]
        expected = None
        start_context = [event for event in events if event.get("kind") == "search_start" and
                         event.get("peer") == x_peer and event.get("pid") == search["pid"] and
                         event.get("detail", {}).get("step_id") == search["step_id"] and
                         event.get("monotonic", -math.inf) <= ad["monotonic"]]
        if start_context:
            context = max(start_context, key=lambda row: row.get("event_number", -1))["detail"]
            try:
                expected = min(max(float(context["scheduled_time"]),
                                   float(ad["wall_time"]) + float(config["case"]["lifetime"])),
                               float(context["deadline"]))
                if not _close(ad["detail"].get("expiration"), expected, 0.10):
                    errors.append(f"active X expiration formula differs at {_ref(ad)}")
            except (KeyError, TypeError, ValueError):
                errors.append(f"X search context lacks expiration inputs for {_ref(ad)}")
        row = dict(publication_event_ref=_ref(ad), publication_monotonic=ad.get("monotonic"),
                   publication_wall=ad.get("wall_time"), step_id=search["step_id"],
                   search_id=search["search_id"], pid=ad.get("pid"),
                   expiration=ad["detail"].get("expiration"),
                   finish_event_ref=_ref(finish) if finish else None,
                   declaration_accepted=(finish["detail"].get("accepted") is True
                                         if finish and finish.get("kind") == "ad_finished" else False),
                   accepted=bool(finish and finish.get("kind") == "ad_finished" and
                                 finish["detail"].get("accepted") is True), next_intervals=[])
        rows.append(row)
        grouped.setdefault(search["search_id"], []).append(row)
    policy = config.get("case", {}).get("publication_policy")
    for search_id, ads in grouped.items():
        ads.sort(key=lambda row: row["publication_monotonic"])
        for previous, following in zip(ads, ads[1:]):
            gap = following["publication_monotonic"] - previous["publication_monotonic"]
            previous["next_intervals"].append(gap)
            finish = next((event for event in finish_events
                           if _ref(event) == previous.get("finish_event_ref")), None)
            remaining = max(0.0, float(previous["expiration"]) - float(previous["publication_wall"]))
            latency = (finish or {}).get("detail", {}).get("latency", 0.0)
            minimum = max(float(latency or 0.0), min(remaining, 1.0) if policy == "renew" else remaining)
            if gap + 0.35 < minimum:
                errors.append(f"X publication cadence is early within search {search_id}")
    return rows


def _window_summary(attempts, groups, config):
    start, duration = config.get("started_monotonic"), config.get("duration")
    if not (_finite(start) and _finite(duration) and duration > 0):
        return [], {}
    windows = []
    begin = 0.0
    while begin < duration:
        end = min(begin + 60.0, duration)
        window_start, window_end = start + begin, start + end
        rows = [attempt for attempt in attempts if window_start <= attempt["started_monotonic"] < window_end]
        successes = [attempt for attempt in rows if attempt["membership_success"]]
        groups_here = [group for group in groups if window_start <= group["assembly_monotonic"] < window_end]
        failure_count = len(rows) - len(successes)
        windows.append(dict(window_index=int(begin // 60), start_offset_seconds=begin,
                            end_offset_seconds=end, honest_submitted_attempts=len(rows),
                            honest_group_successes=len(successes), honest_group_failures=failure_count,
                            honest_group_failure_fraction=failure_count / len(rows) if rows else None,
                            honest_mean_capped_wait_seconds=(statistics.fmean(
                                row["capped_latency_seconds"] for row in rows) if rows else None),
                            total_groups=len(groups_here),
                            honest_group_rate_per_minute=len(groups_here)/((end-begin)/60.0),
                            group_event_refs=[group["assembly_event_ref"] for group in groups_here]))
        begin = end
    return windows, {}


def _raw_choice_rows(attempts, groups, joins, peer_ids, config):
    """Rebuild the inspector choice rows from native join and membership events."""
    names = {peer: name for name, peer in peer_ids.items()}
    x_peer = peer_ids.get(X_NAME)
    first_group_by_id = {row.get("group_id"): row for row in groups}
    joins_by_attempt = {}
    for join in joins:
        joins_by_attempt.setdefault((join["peer_id"], join["step_id"]), []).append(join)
    for rows in joins_by_attempt.values():
        rows.sort(key=lambda row: (row.get("start_monotonic", math.inf),
                                   row.get("start_event_ref", ""), row.get("join_id", "")))
    started, duration = config.get("started_monotonic"), config.get("duration")
    out = []
    for attempt in attempts:
        attempt_joins = joins_by_attempt.get((attempt["peer_id"], attempt["step_id"]), [])
        first_join = attempt_joins[0] if attempt_joins else None
        accepted_x, accepted_other = [], []
        for join in attempt_joins:
            for code, event_ref in zip(join["reply_codes"], join["reply_event_refs"]):
                if code != "ACCEPTED":
                    continue
                (accepted_x if join["leader_peer_id"] == x_peer else accepted_other).append(event_ref)
        first_group = first_group_by_id.get(attempt.get("first_group_id"))
        if first_group is None:
            group_category = "no_group"
        elif first_group.get("leader_peer_id") == x_peer:
            group_category = X_NAME
        else:
            group_category = "other"
        destination = (X_NAME if first_join and first_join["leader_peer_id"] == x_peer else
                       "other" if first_join else "none")
        out.append(dict(
            peer_name=attempt["peer_name"], peer_id=attempt["peer_id"], step_id=attempt["step_id"],
            start_event_ref=attempt["start_event_ref"], started_monotonic=attempt["started_monotonic"],
            submission_started=(started is not None and duration is not None and
                                started <= attempt["started_monotonic"] < started + duration),
            first_outgoing_choice=destination,
            first_outgoing_join_id=first_join["join_id"] if first_join else None,
            first_outgoing_join_event_ref=first_join["start_event_ref"] if first_join else None,
            first_outgoing_join_peer_id=first_join["leader_peer_id"] if first_join else None,
            first_outgoing_join_peer_name=first_join["leader_name"] if first_join else None,
            accepted_x_reply_count=len(accepted_x), accepted_other_reply_count=len(accepted_other),
            accepted_x_reply_event_refs=accepted_x, accepted_other_reply_event_refs=accepted_other,
            ever_tried_x=any(join["leader_peer_id"] == x_peer for join in attempt_joins),
            first_group_leader_category=group_category,
            first_group_leader_peer_id=first_group.get("leader_peer_id") if first_group else None,
            first_group_leader_name=first_group.get("leader_name") if first_group else None,
            first_group_event_ref=first_group.get("assembly_event_ref") if first_group else None,
            first_assembly_group_id=attempt.get("first_group_id"),
            first_assembly_event_ref=attempt.get("first_assembly_event_ref"),
            first_assembly_latency_seconds=attempt.get("first_assembly_latency_seconds"),
            membership_success=attempt["membership_success"],
        ))
    return out


def _choice_counts(choices):
    counts = {"X": 0, "other": 0, "none": 0}
    for choice in choices:
        counts[choice["first_outgoing_choice"]] += 1
    return counts


def _delay_draw(seed, source, destination, rpc_type, leg, occurrence, response_index):
    payload = json.dumps([int(seed), source, destination, rpc_type, leg,
                          int(occurrence), int(response_index)], separators=(",", ":"))
    uniform = int.from_bytes(hashlib.sha256(payload.encode("utf-8")).digest()[:8], "big") / 2**64
    return PROFILE["lower"] + (PROFILE["upper"] - PROFILE["lower"]) * uniform


def _delay_integrity(events, config, peer_ids, errors):
    names = {peer: name for name, peer in peer_ids.items()}
    network_events = [event for event in events if event.get("kind") == "network_config_installed"]
    if not network_events:
        errors.append("network_config_installed evidence missing")
    for event in network_events:
        detail = event["detail"]
        if detail.get("network_seed") != config.get("network_seed") or \
                detail.get("delay_profile") != PROFILE or detail.get("identities") != peer_ids:
            errors.append(f"network config differs from trial metadata at {_ref(event)}")
    rpcs, starts = {}, {}
    for event in events:
        if event.get("kind") not in ("control_rpc_start", "control_rpc_delay", "control_rpc_response",
                                      "control_rpc_end", "control_rpc_handler_enter",
                                      "control_join_admission_state"):
            continue
        detail = event.get("detail", {})
        rpc_id = detail.get("rpc_id")
        if not isinstance(rpc_id, str) or not rpc_id:
            errors.append(f"{event.get('kind')} lacks rpc_id at {_ref(event)}")
            continue
        rpcs.setdefault((event.get("pid"), rpc_id), []).append(event)
        if event.get("kind") == "control_rpc_start":
            key = (event.get("pid"), rpc_id)
            if key in starts:
                errors.append(f"duplicate control_rpc_start for {rpc_id}")
            starts[key] = event
    occurrence_rows = {}
    for key, records in rpcs.items():
        start_event = starts.get(key)
        if start_event is None:
            errors.append(f"orphan RPC observations for {key[1]}")
            continue
        detail = start_event["detail"]
        measured = detail.get("measured") is True
        rpc_type = detail.get("rpc_type")
        source, destination = detail.get("source_name"), detail.get("destination_name")
        if measured:
            if (names.get(start_event.get("other")) != source or
                    names.get(start_event.get("peer")) != destination or
                    detail.get("network_seed") != config.get("network_seed")):
                errors.append(f"measured RPC caller labels/seed disagree for {key[1]}")
            occurrence = detail.get("occurrence")
            if not isinstance(occurrence, int) or isinstance(occurrence, bool) or occurrence < 1:
                errors.append(f"measured RPC has invalid occurrence for {key[1]}")
            else:
                occurrence_rows.setdefault((start_event.get("pid"), source, destination, rpc_type), []).append(occurrence)
        expected_mean = 0.15 if measured else 0.0
        if not _close(detail.get("delay_seconds"), expected_mean, 1e-12):
            errors.append(f"RPC compatibility delay differs for {key[1]}")
        delays = [row for row in records if row.get("kind") == "control_rpc_delay"]
        responses = [row for row in records if row.get("kind") == "control_rpc_response"]
        seen_response = []
        seen_request = 0
        for delay in delays:
            d = delay["detail"]
            leg, index = d.get("leg"), d.get("response_index")
            if leg not in ("request", "response") or not isinstance(index, int):
                errors.append(f"invalid RPC delay leg/index for {key[1]}")
                continue
            expected = _delay_draw(config["network_seed"], source, destination, rpc_type, leg,
                                   detail.get("occurrence", 0), index) if measured else 0.0
            if not _close(d.get("requested_delay"), expected, 1e-12):
                errors.append(f"RPC seeded delay draw differs for {key[1]} {leg}/{index}")
            if d.get("cancelled") is not True and _finite(d.get("actual_delay")) and \
                    d["actual_delay"] + 0.005 < d.get("requested_delay", 0.0):
                errors.append(f"RPC actual delay shorter than request for {key[1]} {leg}/{index}")
            if leg == "request":
                seen_request += 1
            else:
                seen_response.append(index)
        if seen_request > 1:
            errors.append(f"RPC has multiple request delay legs for {key[1]}")
        response_indexes = [row.get("detail", {}).get("response_index") for row in responses]
        if response_indexes != list(range(len(response_indexes))) or \
                sorted(seen_response) != list(range(len(seen_response))):
            errors.append(f"RPC response indexes are not contiguous for {key[1]}")
        if responses and len(seen_response) != len(responses):
            errors.append(f"RPC response events and delay legs differ for {key[1]}")
    for key, occurrences in occurrence_rows.items():
        ordered = sorted(occurrences)
        if ordered != list(range(1, len(ordered) + 1)):
            errors.append(f"measured RPC occurrences are not contiguous for {key}")


def raw_trial(path):
    """Reconstruct primary Step 30 measurements from a single raw event file."""
    path = Path(path)
    errors = []
    try:
        config = _read_json(path / "config.json")
    except (OSError, ValueError, TypeError) as exc:
        return dict(errors=[f"cannot read config.json: {exc}"], attempts=[], groups=[], joins=[],
                    choices=[], retries=[], publications=[], windows=[], summary_metrics={})
    peer_ids = _config_errors(config, errors)
    events = _read_events(path, errors)
    _check_event_closure(events, config, errors)
    _delay_integrity(events, config, peer_ids, errors)
    if (not _finite(config.get("started_monotonic")) or not _finite(config.get("duration")) or
            config.get("duration", 0) <= 0 or set(peer_ids) != {X_NAME, *HONEST_NAMES}):
        return dict(errors=errors, events=events, config=config, attempts=[], groups=[], joins=[],
                    choices=[], retries=[], publications=[], windows=[], summary_metrics={},
                    x_client_accept_join_ids=[], x_client_disband_join_ids=[], event_by_ref={})
    return _primary_rows(events, config, peer_ids, errors)


def _compare_metrics(raw, result, errors):
    expected = raw["summary_metrics"]
    actual = result.get("summary_metrics", {}) if isinstance(result, dict) else {}
    keys = (
        "honest_submitted_attempts", "honest_group_successes", "honest_group_failures",
        "honest_group_failure_fraction", "honest_group_success_fraction",
        "honest_success_wait_count", "honest_mean_success_wait_seconds",
        "honest_mean_capped_wait_seconds", "total_groups", "observation_groups",
        "drain_group_count", "follower_corroborated_groups", "uncorroborated_groups",
        "honest_group_rate_per_minute", "x_assembled_leader_groups",
        "x_client_accept_join_count", "x_client_disband_join_count",
        "same_search_x_retry_count", "x_active_publication_count",
        "x_accepted_active_publication_count", "x_searches_with_multiple_accepted_publications",
        "x_publication_interval_median_seconds", "x_search_count",
        "honest_first_outgoing_choice_counts", "honest_ever_tried_x_attempts",
    )
    for key in keys:
        if key not in actual:
            errors.append(f"analyzer summary omits independently checked {key}")
        elif not _same_cell(actual.get(key), expected.get(key)):
            errors.append(f"analyzer summary {key} differs from raw reconstruction")


def _compare_rows(raw, result, errors):
    def indexed(rows, key, label):
        out = {}
        for row in rows if isinstance(rows, list) else []:
            ident = row.get(key)
            if ident in out:
                errors.append(f"duplicate analyzer {label} key {ident}")
            out[ident] = row
        return out

    raw_attempts = indexed(raw["attempts"], "start_event_ref", "attempt")
    actual_attempts = indexed(result.get("attempts", []), "start_event_ref", "attempt")
    if set(actual_attempts) != set(raw_attempts):
        errors.append("analyzer attempt raw references differ from step_start events")
    for ref, expected in raw_attempts.items():
        row = actual_attempts.get(ref, {})
        for key in ("peer_id", "step_id", "membership_success", "first_assembly_event_ref",
                    "first_assembly_latency_seconds", "capped_latency_seconds"):
            if not _same_cell(row.get(key), expected.get(key)):
                errors.append(f"analyzer attempt {ref} {key} differs from raw membership")

    raw_choices = indexed(raw.get("choices", []), "start_event_ref", "choice")
    actual_choices = indexed(result.get("choices", []), "start_event_ref", "choice")
    if set(actual_choices) != set(raw_choices):
        errors.append("analyzer choice raw references differ from honest step_start events")
    choice_fields = ("peer_id", "step_id", "submission_started", "first_outgoing_choice",
                     "first_outgoing_join_id", "first_outgoing_join_event_ref",
                     "first_outgoing_join_peer_id", "first_outgoing_join_peer_name",
                     "accepted_x_reply_count", "accepted_other_reply_count",
                     "accepted_x_reply_event_refs", "accepted_other_reply_event_refs", "ever_tried_x",
                     "first_group_leader_category", "first_group_leader_peer_id",
                     "first_group_leader_name", "first_group_event_ref", "first_assembly_group_id",
                     "first_assembly_event_ref", "first_assembly_latency_seconds", "membership_success")
    for ref, expected in raw_choices.items():
        row = actual_choices.get(ref, {})
        for key in choice_fields:
            if not _same_cell(row.get(key), expected.get(key)):
                errors.append(f"analyzer choice {ref} {key} differs from raw observations")

    raw_groups = indexed(raw["groups"], "assembly_event_ref", "group")
    actual_groups = indexed(result.get("groups", []), "assembly_event_ref", "group")
    if set(actual_groups) != set(raw_groups):
        errors.append("analyzer leader-group raw references differ from assembled events")
    for ref, expected in raw_groups.items():
        row = actual_groups.get(ref, {})
        for key in ("group_id", "leader_peer_id", "leader_step_id", "roster", "follower_corroborated"):
            if not _same_cell(row.get(key), expected.get(key)):
                errors.append(f"analyzer group {ref} {key} differs from raw assembly evidence")

    raw_joins = indexed(raw["joins"], "join_id", "join")
    actual_joins = indexed(result.get("joins", []), "join_id", "join")
    if set(actual_joins) != set(raw_joins):
        errors.append("analyzer join IDs differ from raw join_attempt_start events")
    for join_id, expected in raw_joins.items():
        row = actual_joins.get(join_id, {})
        for key in ("start_event_ref", "end_event_ref", "peer_id", "leader_peer_id", "step_id",
                    "reply_codes", "native_client_request_expiration", "selected_leader_expiration",
                    "caller_advertised_expiration", "admission_request_expiration",
                    "admission_leader_expiration", "server_rpc_link_status",
                    "local_leader_wait_captured_expirations"):
            if not _same_cell(row.get(key), expected.get(key)):
                errors.append(f"analyzer join {join_id} {key} differs from raw request/receipt")

    raw_retries = indexed(raw["retries"], "selection_event_ref", "retry")
    actual_retries = indexed(result.get("retries", []), "selection_event_ref", "retry")
    if set(actual_retries) != set(raw_retries):
        errors.append("analyzer retry selection refs differ from same-search native pair evidence")
    for ref, expected in raw_retries.items():
        row = actual_retries.get(ref, {})
        for key in ("old_pair_expiration", "new_pair_expiration", "x_search_id"):
            if not _same_cell(row.get(key), expected.get(key)):
                errors.append(f"analyzer retry {ref} {key} differs from raw pair evidence")

    raw_pubs = indexed(raw["publications"], "publication_event_ref", "publication")
    actual_pubs = indexed(result.get("publications", []), "publication_event_ref", "publication")
    if set(actual_pubs) != set(raw_pubs):
        errors.append("analyzer publication references differ from raw active X ads")
    for ref, expected in raw_pubs.items():
        row = actual_pubs.get(ref, {})
        for key in ("search_id", "expiration", "declaration_accepted"):
            if not _same_cell(row.get(key), expected.get(key)):
                errors.append(f"analyzer publication {ref} {key} differs from raw advertisement")

    actual_windows = indexed(result.get("windows", []), "window_index", "window")
    raw_windows = indexed(raw["windows"], "window_index", "window")
    if set(actual_windows) != set(raw_windows):
        errors.append("analyzer window indexes differ from fixed 60-second raw windows")
    for index, expected in raw_windows.items():
        row = actual_windows.get(index, {})
        for key in ("honest_submitted_attempts", "honest_group_successes", "honest_group_failures",
                    "honest_group_failure_fraction", "honest_mean_capped_wait_seconds", "total_groups",
                    "honest_group_rate_per_minute", "group_event_refs"):
            if not _same_cell(row.get(key), expected.get(key)):
                errors.append(f"analyzer window {index} {key} differs from raw events")


def verify_trial(path, result):
    """Compare analyzer output to independently reconstructed primary evidence."""
    raw = raw_trial(path)
    errors = list(raw.get("errors", []))
    if not isinstance(result, dict):
        errors.append("analyzer result is not an object")
    else:
        _compare_metrics(raw, result, errors)
        _compare_rows(raw, result, errors)
        if result.get("integrity_pass") is not True:
            errors.append("analyzer integrity_pass is not true")
    trial_path = Path(path)
    completion_file = trial_path / "completion.json"
    if completion_file.exists():
        try:
            done = _read_json(completion_file)
            hashes = done.get("file_sha256", {})
            for name in ("config.json", "events.jsonl", "summary.json", "analysis.json", "control_analysis.json"):
                if name not in hashes or _sha256(trial_path / name) != hashes.get(name):
                    errors.append(f"completed trial evidence hash differs for {name}")
            if done.get("all_peers_shutdown") is not True:
                errors.append("completion evidence does not confirm peer shutdown")
        except (OSError, ValueError, TypeError) as exc:
            errors.append(f"invalid completion evidence: {exc}")
    passed = not errors
    return {"pass": passed, "pass_": passed, "errors": sorted(set(errors)),
            "schema": "step30-raw-v1", "trial_dir": trial_path.name,
            "case": raw.get("config", {}).get("case"),
            "seed": raw.get("config", {}).get("seed"),
            "duration": raw.get("config", {}).get("duration"),
            "raw_events_sha256": _sha256(trial_path / "events.jsonl")
            if (trial_path / "events.jsonl").is_file() else None,
            "x_client_accept_join_ids": raw.get("x_client_accept_join_ids", []),
            "x_client_disband_join_ids": raw.get("x_client_disband_join_ids", []),
            "raw_summary_metrics": raw.get("summary_metrics", {}),
            "primary_rows": {key: raw.get(key, []) for key in
                             ("attempts", "groups", "choices", "joins", "retries", "publications", "windows")}}


def _sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _provenance_errors(suite):
    suite = Path(suite)
    errors = []
    try:
        provenance = _read_json(suite / "provenance.json")
    except (OSError, ValueError, TypeError) as exc:
        return [f"cannot read provenance.json: {exc}"]
    hashes = provenance.get("harness_sha256", {})
    if not REQUIRED_HARNESS.issubset(hashes):
        errors.append("frozen harness provenance omits required source files")
    for name in REQUIRED_HARNESS:
        expected = hashes.get(name)
        for path in (ROOT / name, suite / "harness" / name):
            try:
                if not isinstance(expected, str) or _sha256(path) != expected:
                    errors.append(f"frozen source hash mismatch: {path}")
            except OSError:
                errors.append(f"frozen source missing: {path}")
    entrypoint = hashes.get("step30_harm.py")
    if provenance.get("entrypoint_sha256") != entrypoint or provenance.get("script_sha256") != entrypoint:
        errors.append("Step 30 entrypoint hash is not tied to its frozen harness hash")

    native = provenance.get("native_source_snapshot_sha256", {})
    snapshot_root = suite / "native_source"
    saved = {str(path.relative_to(snapshot_root)) for path in snapshot_root.rglob("*")
             if path.is_file()} if snapshot_root.exists() else set()
    if saved != set(native):
        errors.append("native source snapshot files differ from provenance index")
    for relative, expected in native.items():
        try:
            if _sha256(snapshot_root / relative) != expected:
                errors.append(f"native source snapshot hash mismatch: {relative}")
        except OSError:
            errors.append(f"native source snapshot missing: {relative}")
    originals = provenance.get("source_sha256", {})
    for relative, expected in originals.items():
        if native.get(relative) != expected:
            errors.append(f"native source provenance does not match captured source: {relative}")
    for path, expected in provenance.get("runtime_files_sha256", {}).items():
        try:
            runtime_path = Path(path)
            if runtime_path.exists() and _sha256(runtime_path) != expected:
                errors.append(f"installed native runtime hash mismatch: {path}")
        except OSError:
            errors.append(f"cannot verify installed native runtime hash: {path}")
    return errors


def _read_csv(path, errors):
    try:
        with Path(path).open(newline="", encoding="utf-8") as stream:
            return list(csv.DictReader(stream))
    except OSError as exc:
        errors.append(f"missing export {path.name}: {exc}")
        return []


def _verify_csv_rows(suite, trial_rows, raw_by_trial, errors):
    expected_refs = {
        "attempts": ("attempts", "start_event_ref"),
        "groups": ("groups", "assembly_event_ref"),
        "choices": ("choices", "start_event_ref"),
        "joins": ("joins", "join_id"),
        "retries": ("retries", "selection_event_ref"),
        "publications": ("publications", "publication_event_ref"),
        "windows": ("windows", "window_index"),
    }
    tables = {}
    for table in CSV_TABLES:
        table_path = suite / f"{table}.csv"
        if not table_path.exists():
            if trial_rows:
                errors.append(f"export table missing: {table}.csv")
            continue
        rows = _read_csv(table_path, errors)
        tables[table] = rows

    for table, (raw_key, id_key) in expected_refs.items():
        if table not in tables:
            continue
        actual = {}
        for row in tables[table]:
            trial_dir = row.get("trial_dir")
            ident = row.get(id_key)
            key = (trial_dir, str(ident))
            if key in actual:
                errors.append(f"duplicate {table}.csv row {key}")
            actual[key] = row
        expected = {}
        for trial_dir, raw in raw_by_trial.items():
            for item in raw.get(raw_key, []):
                expected[(trial_dir, str(item.get(id_key)))] = item
        if set(actual) != set(expected):
            errors.append(f"{table}.csv raw reference set differs from raw trace")
        for key, item in expected.items():
            row = actual.get(key, {})
            checks = {
                "attempts": ("membership_success", "first_assembly_event_ref", "capped_latency_seconds"),
                "groups": ("group_id", "leader_peer_id", "follower_corroborated"),
                "choices": ("peer_id", "step_id", "submission_started", "first_outgoing_choice",
                            "first_outgoing_join_id", "first_outgoing_join_event_ref",
                            "accepted_x_reply_count", "accepted_other_reply_count",
                            "accepted_x_reply_event_refs", "accepted_other_reply_event_refs",
                            "ever_tried_x", "first_group_leader_category", "first_group_leader_peer_id",
                            "first_group_leader_name", "first_group_event_ref",
                            "first_assembly_group_id", "first_assembly_event_ref",
                            "first_assembly_latency_seconds", "membership_success"),
                "joins": ("start_event_ref", "end_event_ref", "reply_codes",
                          "native_client_request_expiration", "selected_leader_expiration",
                          "admission_leader_expiration", "admission_request_expiration"),
                "retries": ("old_pair_expiration", "new_pair_expiration", "x_search_id"),
                "publications": ("expiration", "search_id", "declaration_accepted"),
                "windows": ("honest_submitted_attempts", "honest_group_successes", "total_groups",
                            "honest_group_rate_per_minute", "group_event_refs"),
            }[table]
            for field in checks:
                if not _same_cell(row.get(field), item.get(field)):
                    errors.append(f"{table}.csv {key} field {field} differs from raw trace")

    if "trial_summary" in tables:
        actual = {}
        for row in tables["trial_summary"]:
            key = row.get("trial_dir")
            if key in actual:
                errors.append(f"duplicate trial_summary.csv row for {key}")
            actual[key] = row
        expected_names = set(trial_rows)
        if set(actual) != expected_names:
            errors.append("trial_summary.csv trial set differs from analyzed trials")
        for trial_dir, raw in raw_by_trial.items():
            row = actual.get(trial_dir, {})
            for field in ("honest_submitted_attempts", "honest_group_successes",
                          "honest_group_failures", "honest_group_failure_fraction",
                          "honest_mean_capped_wait_seconds", "total_groups", "observation_groups",
                          "drain_group_count", "honest_group_rate_per_minute",
                          "x_client_accept_join_count", "x_client_disband_join_count",
                          "same_search_x_retry_count", "x_accepted_active_publication_count",
                          "honest_first_outgoing_choice_counts", "honest_ever_tried_x_attempts"):
                if not _same_cell(row.get(field), raw["summary_metrics"].get(field)):
                    errors.append(f"trial_summary.csv {trial_dir} field {field} differs from raw trace")


def verify_exports(suite):
    """Check frozen inputs and exported primary rows against each raw trial."""
    suite = Path(suite)
    errors = _provenance_errors(suite)
    try:
        manifest = _read_json(suite / "suite.json")
    except (OSError, ValueError, TypeError) as exc:
        return dict(pass_=False, errors=[*errors, f"cannot read suite.json: {exc}"])
    phase = manifest.get("phase")
    if manifest.get("experiment") != "step30" or phase not in ("smoke", "confirm"):
        errors.append("suite manifest is not Step 30 smoke/confirm")
    jobs = manifest.get("jobs", [])
    if jobs != _canonical_jobs(phase):
        errors.append("suite manifest jobs differ from the frozen paired matrix")
    if (manifest.get("training_delay") != TRAINING or manifest.get("delay_profile") != PROFILE or
            manifest.get("decision_rule") != DECISION_RULE or manifest.get("outcome") != "grouping" or
            manifest.get("control_only") is not True):
        errors.append("suite manifest differs from frozen Step 30 design metadata")
    seen_jobs, trials, raw_by_trial = set(), {}, {}
    for job in jobs:
        case, seed, duration = job.get("case", {}), job.get("seed"), job.get("duration")
        trial_dir = f"{case.get('name')}_s{seed}"
        if (case != CASE_ROWS.get(case.get("name")) or
                (seed, duration) not in ((3000, 20.0), (3001, 300.0),
                                         (3002, 300.0), (3003, 300.0))):
            errors.append(f"invalid Step 30 job in manifest: {trial_dir}")
        key = (case.get("name"), seed)
        if key in seen_jobs:
            errors.append(f"duplicate Step 30 job {key}")
        seen_jobs.add(key)
        path = suite / trial_dir
        control_path = path / "control_analysis.json"
        if not control_path.is_file():
            continue
        verification = {}
        try:
            result = _read_json(control_path)
        except (OSError, ValueError, TypeError) as exc:
            errors.append(f"{trial_dir}: invalid control analysis: {exc}")
            continue
        if (result.get("trial_dir") != trial_dir or result.get("case") != case or
                result.get("seed") != seed or result.get("duration") != duration):
            errors.append(f"{trial_dir}: control analysis metadata differs from manifest")
        try:
            verification = _read_json(path / "raw_verification.json")
            completion = _read_json(path / "completion.json")
            hashes = completion.get("file_sha256", {})
            expected_files = {"config.json", "events.jsonl", "summary.json", "analysis.json",
                              "control_analysis.json", "raw_verification.json"}
            if (completion.get("all_peers_shutdown") is not True or set(hashes) != expected_files):
                errors.append(f"{trial_dir}: completion record is incomplete")
            for filename, digest in hashes.items():
                if not (path / filename).is_file() or _sha256(path / filename) != digest:
                    errors.append(f"{trial_dir}: completed evidence changed: {filename}")
            if verification.get("pass") is not True:
                errors.append(f"{trial_dir}: persisted raw_verification.json failed")
            if (verification.get("schema") != "step30-raw-v1" or
                    verification.get("trial_dir") != trial_dir or verification.get("case") != case or
                    verification.get("seed") != seed or verification.get("duration") != duration):
                errors.append(f"{trial_dir}: raw verifier certificate metadata differs")
            if verification.get("raw_events_sha256") != hashes.get("events.jsonl"):
                errors.append(f"{trial_dir}: raw verifier certificate is not bound to raw events")
        except (OSError, ValueError, TypeError) as exc:
            errors.append(f"{trial_dir}: cannot read raw verification: {exc}")
        # Trust only the independent reconstruction saved at trial completion,
        # and only after its file hashes and raw trace digest agree. This keeps
        # each incremental export check from parsing a long raw trace again.
        if "verification" in locals() and verification.get("schema") == "step30-raw-v1":
            cert_rows = verification.get("primary_rows", {})
            raw = {"summary_metrics": verification.get("raw_summary_metrics", {}),
                     **{key: cert_rows.get(key, []) for key in
                      ("attempts", "groups", "choices", "joins", "retries", "publications", "windows")}}
            x_peer = next((row.get("leader_peer_id") for row in raw["joins"]
                           if row.get("leader_name") == X_NAME), None)
            checked = dict(errors=[],
                           x_client_accept_join_ids=sorted({row["join_id"] for row in raw["joins"]
                                                           if row.get("leader_name") == X_NAME and
                                                           "ACCEPTED" in row.get("reply_codes", [])}),
                           x_client_disband_join_ids=sorted({row["join_id"] for row in raw["joins"]
                                                            if row.get("leader_name") == X_NAME and
                                                            "GROUP_DISBANDED" in row.get("reply_codes", [])}))
            errors_for_compare = []
            _compare_metrics(raw, result, errors_for_compare)
            _compare_rows(raw, result, errors_for_compare)
            errors.extend(f"{trial_dir}: {error}" for error in errors_for_compare)
        else:
            checked = verify_trial(path, result)
            errors.extend(f"{trial_dir}: {error}" for error in checked["errors"])
            raw = raw_trial(path)
        persisted = verification if verification else checked
        if (sorted(persisted.get("x_client_accept_join_ids", [])) != checked["x_client_accept_join_ids"] or
                sorted(persisted.get("x_client_disband_join_ids", [])) != checked["x_client_disband_join_ids"]):
            errors.append(f"{trial_dir}: persisted X receipt join IDs differ")
        if result.get("integrity_pass") is not True:
            errors.append(f"{trial_dir}: analyzer integrity_pass is not true")
        trials[trial_dir] = result
        raw_by_trial[trial_dir] = raw
    _verify_csv_rows(suite, trials, raw_by_trial, errors)
    passed = not errors
    return {"pass": passed, "pass_": passed, "errors": sorted(set(errors)),
            "analyzed_trial_count": len(trials), "expected_job_count": len(jobs)}
