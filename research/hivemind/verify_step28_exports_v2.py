"""Independent raw checks for the Step 28 confirmation suite.

This module reads event traces and exports directly. It does not import the
Step 28 analyzer. Assembly and attempt integrity checks reuse the frozen raw
reader from Step 27, then derive Step 28 choice, acceptance, and latency
metrics from the raw event references.

Usage: python verify_step28_confirmation.py --suite PATH [--preservation PATH]
The only output written by the CLI is PATH/raw_verification.json.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import importlib.util
import json
import math
from pathlib import Path
import statistics
import sys


ROOT = Path(__file__).resolve().parent
STEP27_RAW_PATH = Path(__file__).with_name("verify_step27_raw.py")
LEAD_CAP_SECONDS = 12.0
CLOCK_OFFSET_LIMIT_SECONDS = 0.25


def sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_json(path: str | Path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def event_ref(event: dict) -> str:
    return f"{event['pid']}:{event['event_number']}"


def _step27_raw_module():
    """Load only the frozen raw verifier helper, never the Step 27 analyzer."""
    module_name = "_hivemind_step27_raw_for_step28_verification"
    module = sys.modules.get(module_name)
    if module is not None:
        return module
    spec = importlib.util.spec_from_file_location(module_name, STEP27_RAW_PATH)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load frozen raw verifier at {STEP27_RAW_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _percentile(values: list[float], fraction: float) -> float | None:
    ordered = sorted(values)
    if not ordered:
        return None
    position = (len(ordered) - 1) * fraction
    lower, upper = math.floor(position), math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def _capped_latency(value: float | None) -> float:
    if value is None:
        return LEAD_CAP_SECONDS
    return min(max(float(value), 0.0), LEAD_CAP_SECONDS)


def _ordered(events: list[dict]) -> list[dict]:
    return sorted(events, key=lambda event: (event["monotonic"], event["pid"], event["event_number"]))


def _same_number(left, right, tolerance: float = 1e-9) -> bool:
    if left is None or right in (None, "", "None", "null"):
        return left is None and right in (None, "", "None", "null")
    try:
        return math.isclose(float(left), float(right), rel_tol=0.0, abs_tol=tolerance)
    except (TypeError, ValueError):
        return False


def _json_cell(value):
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return value
    return value


def _same_value(actual, expected) -> bool:
    actual = _json_cell(actual)
    if expected is None:
        return actual in (None, "", "None", "null")
    if isinstance(expected, bool):
        if isinstance(actual, bool):
            return actual is expected
        return str(actual).strip().lower() == str(expected).lower()
    if isinstance(expected, (int, float)) and not isinstance(expected, bool):
        return _same_number(actual, expected)
    if isinstance(expected, (dict, list, tuple)):
        return actual == expected or (isinstance(expected, tuple) and actual == list(expected))
    return actual == str(expected)


def _csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as stream:
        return list(csv.DictReader(stream))


def _peer_name(config: dict, peer: str | None) -> str | None:
    if peer is None:
        return None
    return {value: name for name, value in config.get("peer_ids", {}).items()}.get(peer)


def _join_raw(events: list[dict], attempts: dict, config: dict, errors: list[str]) -> tuple[list[dict], dict]:
    """Link each join start, end, and native client record by join_id."""
    starts, ends, client_records = {}, {}, {}
    refs = {event_ref(event): event for event in events}
    for event in events:
        kind, detail = event["kind"], event["detail"]
        if kind in ("join_attempt_start", "join_attempt_end"):
            join_id = detail.get("join_id")
            if not isinstance(join_id, str) or not join_id:
                errors.append(f"{kind} missing join_id at {event_ref(event)}")
                continue
            target = starts if kind == "join_attempt_start" else ends
            if join_id in target:
                errors.append(f"duplicate {kind} for {join_id}")
            target[join_id] = event
        elif kind in ("native_client_wait", "native_client_reply", "native_client_exception"):
            join_id = detail.get("join_id")
            if not isinstance(join_id, str) or not join_id:
                errors.append(f"{kind} missing join_id at {event_ref(event)}")
                continue
            client_records.setdefault(join_id, []).append(event)

    rows = []
    joins_by_attempt: dict[tuple[str, str], list[dict]] = {}
    for join_id, start in starts.items():
        detail = start["detail"]
        peer, leader, step_id = start.get("peer"), start.get("other"), detail.get("step_id")
        end = ends.get(join_id)
        key = peer, step_id
        if key not in attempts:
            errors.append(f"join {join_id} does not belong to a raw step_start: {key}")
        if _peer_name(config, peer) is None or _peer_name(config, leader) is None:
            errors.append(f"join {join_id} has unknown caller or leader identity")
        if detail.get("origin") not in ("queue", "redirect"):
            errors.append(f"join {join_id} has invalid origin")
        if end is not None:
            ed = end["detail"]
            if (end.get("peer") != peer or end.get("other") != leader or
                    ed.get("step_id") != step_id or ed.get("search_id") != detail.get("search_id")):
                errors.append(f"join {join_id} start/end identity differs")
            if end["monotonic"] < start["monotonic"]:
                errors.append(f"join {join_id} ends before it starts")

        selection_ref = detail.get("selection_event_ref")
        selected_expiration = None
        if detail.get("origin") == "queue":
            selected = refs.get(selection_ref)
            if (selected is None or selected["kind"] != "native_pop_selected" or
                    selected.get("peer") != peer or selected.get("other") != leader or
                    selected["detail"].get("step_id") != step_id or
                    selected["detail"].get("search_id") != detail.get("search_id")):
                errors.append(f"queue join {join_id} has invalid selection reference")
            else:
                selected_expiration = selected["detail"].get("selected_expiration")
        elif selection_ref not in (None, ""):
            errors.append(f"redirect join {join_id} unexpectedly has a queue selection reference")

        records = _ordered(client_records.get(join_id, []))
        for record in records:
            rd = record["detail"]
            if (record.get("peer") != peer or record.get("other") != leader or
                    rd.get("step_id") != step_id):
                errors.append(f"native client record identity differs for join {join_id}")
            if (record["monotonic"] < start["monotonic"] or
                    (end is not None and record["monotonic"] > end["monotonic"])):
                errors.append(f"native client record falls outside join {join_id} interval")
        replies = [event for event in records if event["kind"] == "native_client_reply"]
        accepted = [event for event in replies if event["detail"].get("code") == "ACCEPTED"]
        disbanded = [event for event in replies if event["detail"].get("code") == "GROUP_DISBANDED"]
        request_expirations = [event["detail"].get("request_expiration") for event in replies
                               if "request_expiration" in event["detail"]]
        joins_by_attempt.setdefault(key, []).append(dict(
            join_id=join_id, start=start, end=end, leader_peer_id=leader,
            leader_name=_peer_name(config, leader), origin=detail.get("origin"),
            search_id=detail.get("search_id"), selection_event_ref=selection_ref,
            selected_leader_expiration=selected_expiration, records=records,
            replies=replies, accepted=accepted, request_expirations=request_expirations))
        rows.append(dict(
            join_id=join_id, peer_id=peer, peer_name=_peer_name(config, peer),
            leader_peer_id=leader, leader_name=_peer_name(config, leader), step_id=step_id,
            search_id=detail.get("search_id"), origin=detail.get("origin"),
            selection_event_ref=selection_ref, start_event_ref=event_ref(start),
            end_event_ref=event_ref(end) if end else None,
            native_client_event_refs=[event_ref(event) for event in replies],
            accepted_reply_event_refs=[event_ref(event) for event in accepted],
            group_disbanded_reply_event_refs=[event_ref(event) for event in disbanded],
            reply_codes=[event["detail"].get("code") for event in replies],
            request_expiration=request_expirations[0] if request_expirations else None,
            request_expirations=request_expirations,
            selected_leader_expiration=selected_expiration,
        ))

    for join_id in ends.keys() - starts.keys():
        errors.append(f"join_attempt_end has no start: {join_id}")
    for join_id in client_records.keys() - starts.keys():
        errors.append(f"native client event has no join_attempt_start: {join_id}")
    rows.sort(key=lambda row: (row["start_event_ref"], row["join_id"]))
    for joins in joins_by_attempt.values():
        joins.sort(key=lambda row: (row["start"]["monotonic"], row["start"]["pid"],
                                    row["start"]["event_number"]))
    return rows, joins_by_attempt


def _clock_audit(events: list[dict], config: dict, errors: list[str]) -> dict:
    samples = [event for event in events if event.get("kind") == "controller_clock"]
    wall_offsets, boot_offsets = [], []
    for event in samples:
        detail = event.get("detail", {})
        try:
            wall = float(detail["clock_wall"])
            monotonic = float(detail.get("clock_monotonic", event["monotonic"]))
            boottime = float(detail["clock_boottime"])
        except (KeyError, TypeError, ValueError):
            errors.append(f"malformed controller_clock sample at {event_ref(event)}")
            continue
        if not all(math.isfinite(value) for value in (wall, monotonic, boottime)):
            errors.append(f"nonfinite controller_clock sample at {event_ref(event)}")
            continue
        wall_offsets.append(wall - monotonic)
        boot_offsets.append(boottime - monotonic)
    wall_span = max(wall_offsets) - min(wall_offsets) if wall_offsets else None
    boot_span = max(boot_offsets) - min(boot_offsets) if boot_offsets else None
    if config.get("clock_monitor") is True and len(wall_offsets) < 2:
        errors.append("clock monitoring requires at least two valid controller_clock samples")
    if wall_span is not None and wall_span > CLOCK_OFFSET_LIMIT_SECONDS:
        errors.append(f"controller clock_wall-clock_monotonic offset span exceeds {CLOCK_OFFSET_LIMIT_SECONDS}s")
    if boot_span is not None and boot_span > CLOCK_OFFSET_LIMIT_SECONDS:
        errors.append(f"controller clock_boottime-clock_monotonic offset span exceeds {CLOCK_OFFSET_LIMIT_SECONDS}s")
    return dict(sample_count=len(samples), valid_sample_count=len(wall_offsets), clock_wall_minus_monotonic_span_seconds=wall_span,
                clock_boottime_minus_monotonic_span_seconds=boot_span,
                jump_limit_seconds=CLOCK_OFFSET_LIMIT_SECONDS)


def raw_trial(path: str | Path) -> dict:
    """Reconstruct Step 28 attempt, grouping, first-choice, and ACCEPTED metrics."""
    path = Path(path)
    raw = _step27_raw_module().raw_trial(path)
    errors = list(raw["errors"])
    events = list(raw["events_by_ref"].values())
    config = raw["config"]
    names = {peer: name for name, peer in config.get("peer_ids", {}).items()}
    starts = [event for event in events if event["kind"] == "step_start"]
    starts_by_attempt = {(event["peer"], event["detail"].get("step_id")): event
                         for event in starts}
    groups = raw["groups"]
    attempts = raw["attempts"]
    x_peer = config.get("peer_ids", {}).get("X")

    joins, joins_by_attempt = _join_raw(events, attempts, config, errors)
    first_choice_x = first_choice_other = no_outgoing = ever_x = 0
    accepted_x = accepted_other = 0
    attempt_choice = {}
    attempt_accepts = {}
    for event in starts:
        key = event["peer"], event["detail"].get("step_id")
        if key not in attempts:
            continue
        peer_name = names.get(key[0])
        outgoing = joins_by_attempt.get(key, [])
        first = outgoing[0] if outgoing else None
        destinations = [join for join in outgoing if join["leader_peer_id"] == x_peer]
        destination = "none" if first is None else (
            "X" if first["leader_peer_id"] == x_peer else "other")
        attempt_choice[key] = dict(first=first, destination=destination, ever_tried_x=bool(destinations))
        if peer_name != "X":
            if first is None:
                no_outgoing += 1
            elif destination == "X":
                first_choice_x += 1
            else:
                first_choice_other += 1
            if destinations:
                ever_x += 1

        peer_accepted = {"X": [], "other": []}
        for join in outgoing:
            category = "X" if join["leader_peer_id"] == x_peer else "other"
            for reply in join["accepted"]:
                peer_accepted[category].append(reply)
        if peer_name != "X":
            accepted_x += len(peer_accepted["X"])
            accepted_other += len(peer_accepted["other"])
        attempt_accepts[key] = peer_accepted

    honest = [row for row in attempts.values() if row["peer_name"] != "X"]
    x_attempts = [row for row in attempts.values() if row["peer_name"] == "X"]
    first_x = first_other = 0
    honest_success_latencies = []
    for attempt in honest:
        group = groups.get(attempt["first_assembly_group_id"])
        if group is None:
            continue
        detail = group["detail"]
        member = detail.get("members", {}).get(attempt["peer_id"])
        if member is None or member.get("step_id") != attempt["step_id"]:
            errors.append(f"first group does not contain exact honest peer-step: {attempt['peer_id']}/{attempt['step_id']}")
            continue
        leader_name = names.get(group["peer"])
        if leader_name == "X":
            first_x += 1
        else:
            first_other += 1
        start = starts_by_attempt.get((attempt["peer_id"], attempt["step_id"]))
        if start is not None:
            honest_success_latencies.append(max(0.0, group["monotonic"] - start["monotonic"]))

    x_lead_latencies = [row["lead_latency_seconds"] for row in x_attempts]
    x_lead_capped = [_capped_latency(value) for value in x_lead_latencies]
    x_cdf_counts = {
        threshold: sum(value is not None and float(value) <= threshold for value in x_lead_latencies)
        for threshold in (2, 3, 5, 12)
    }
    x_success_latencies = [float(value) for value in x_lead_latencies if value is not None]
    honest_capped = [float(row["capped_latency_seconds"]) for row in honest]
    honest_no_group = sum(not row["membership_success"] for row in honest)
    honest_count = len(honest)
    honest_success = honest_count - honest_no_group
    cutoff_start = config.get("started_monotonic")
    cutoff_end = (float(cutoff_start) + float(config["duration"])
                  if cutoff_start is not None else None)

    def started_in_window(attempt):
        event = starts_by_attempt[(attempt["peer_id"], attempt["step_id"])]
        return cutoff_start is not None and cutoff_end is not None and (
            float(cutoff_start) <= event["monotonic"] < cutoff_end)

    submission_x = [row for row in x_attempts if started_in_window(row)]
    submission_honest = [row for row in honest if started_in_window(row)]
    late_starts = sum(cutoff_end is not None and
                      starts_by_attempt[(row["peer_id"], row["step_id"])]
                      ["monotonic"] >= cutoff_end for row in attempts.values())
    metrics = dict(raw["metrics"])
    metrics.update(
        x_led_rate=(sum(row["led_success"] for row in x_attempts) / len(x_attempts)
                    if x_attempts else None),
        **{f"x_led_by_{threshold}s_count": x_cdf_counts[threshold]
           for threshold in (2, 3, 5, 12)},
        **{f"x_led_by_{threshold}s_rate": (x_cdf_counts[threshold] / len(x_attempts)
                                             if x_attempts else None)
           for threshold in (2, 3, 5, 12)},
        x_lead_latency_cdf_denominator=len(x_attempts),
        x_lead_latency_cdf_definition=(
            "observed raw own-step X-led assembly latency within each threshold divided by all raw X "
            "step_start attempts; missing leadership remains in the denominator and is not counted "
            "as an observed success"),
        x_no_first_assembly_count=sum(value is None for value in x_lead_latencies),
        x_success_only_lead_latency_count=len(x_success_latencies),
        x_success_only_mean_first_lead_latency_seconds=(
            statistics.mean(x_success_latencies) if x_success_latencies else None),
        x_success_only_median_first_lead_latency_seconds=(
            statistics.median(x_success_latencies) if x_success_latencies else None),
        x_success_only_p90_first_lead_latency_seconds=(
            _percentile(x_success_latencies, 0.9) if x_success_latencies else None),
        x_late_first_assembly_after_12s_count=sum(
            value is not None and float(value) > LEAD_CAP_SECONDS for value in x_lead_latencies),
        x_led_latency_successes=len(x_success_latencies),
        x_mean_first_lead_latency_seconds=(statistics.mean(x_success_latencies)
                                           if x_success_latencies else None),
        x_median_first_lead_latency_seconds=(statistics.median(x_success_latencies)
                                             if x_success_latencies else None),
        x_led_latency_median_seconds=(statistics.median(x_success_latencies)
                                      if x_success_latencies else None),
        x_led_latency_p90_seconds=(_percentile(x_success_latencies, 0.9)
                                   if x_success_latencies else None),
        x_mean_capped_latency_seconds=(statistics.mean(x_lead_capped) if x_lead_capped else None),
        x_median_capped_latency_seconds=(statistics.median(x_lead_capped) if x_lead_capped else None),
        x_p90_capped_latency_seconds=(_percentile(x_lead_capped, 0.9) if x_lead_capped else None),
        x_capped_latency_definition=(
            "all raw X step_start attempts; min(first own-led assembly monotonic - step_start monotonic, 12); "
            "absent assembly is 12"),
        x_attempt_denominator_includes_all_raw_step_starts=True,
        x_submission_window_attempts=len(submission_x),
        x_late_start_attempts=len(x_attempts) - len(submission_x),
        all_late_start_attempts=late_starts,
        honest_submission_window_attempts=len(submission_honest),
        honest_assembled_attempts=honest_success,
        honest_first_group_x_led_attempts=first_x,
        honest_first_group_other_led_attempts=first_other,
        honest_no_group_attempts=honest_no_group,
        honest_first_group_x_led_rate=(first_x / honest_count if honest_count else None),
        honest_first_group_other_led_rate=(first_other / honest_count if honest_count else None),
        honest_first_group_success_fraction=(honest_success / honest_count if honest_count else None),
        honest_first_choice_x_rate=(first_choice_x / honest_count if honest_count else None),
        honest_first_choice_other_rate=(first_choice_other / honest_count if honest_count else None),
        honest_no_outgoing_choice_rate=(no_outgoing / honest_count if honest_count else None),
        honest_ever_tried_x_rate=(ever_x / honest_count if honest_count else None),
        honest_success_only_first_group_latency_count=len(honest_success_latencies),
        honest_success_only_mean_first_group_latency_seconds=(
            statistics.mean(honest_success_latencies) if honest_success_latencies else None),
        honest_success_only_median_first_group_latency_seconds=(
            statistics.median(honest_success_latencies) if honest_success_latencies else None),
        honest_success_only_p90_first_group_latency_seconds=(
            _percentile(honest_success_latencies, 0.9) if honest_success_latencies else None),
        honest_median_capped_latency_seconds=(statistics.median(honest_capped)
                                              if honest_capped else None),
        honest_p90_capped_latency_seconds=(_percentile(honest_capped, 0.9)
                                           if honest_capped else None),
        honest_capped_latency_definition=(
            "min(first assembled group containing the peer - step_start, 12); absent assembly is 12"),
        honest_first_choice_x_attempts=first_choice_x,
        honest_first_choice_other_attempts=first_choice_other,
        honest_no_outgoing_choice_attempts=no_outgoing,
        honest_ever_tried_x_attempts=ever_x,
        honest_accepted_x_reply_count=accepted_x,
        honest_accepted_other_reply_count=accepted_other,
    )
    clock = _clock_audit(events, config, errors)
    metrics["clock_integrity"] = dict(
        required=config.get("clock_monitor") is True, samples=clock["sample_count"], valid_samples=clock["valid_sample_count"],
        wall_offset_spread_seconds=clock["clock_wall_minus_monotonic_span_seconds"],
        boottime_offset_spread_seconds=clock["clock_boottime_minus_monotonic_span_seconds"])
    return dict(config=config, errors=errors, metrics=metrics, attempts=attempts,
                groups=groups, retries=raw["retries"], joins=joins,
                attempt_choice=attempt_choice, attempt_accepts=attempt_accepts,
                clock_audit=clock, events_by_ref=raw["events_by_ref"], events=events,
                starts_by_attempt=starts_by_attempt, joins_by_attempt=joins_by_attempt)


def _load_harness():
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    import step28_confirmation as harness  # noqa: PLC0415
    return harness


def _compare_summary_metrics(row_errors: list[str], expected: dict, actual: dict, label: str):
    if not isinstance(actual, dict):
        row_errors.append(f"{label} lacks summary_metrics")
        return
    for key, value in expected.items():
        if key not in actual:
            row_errors.append(f"{label} missing raw-derived metric: {key}")
        elif not _same_value(actual.get(key), value):
            row_errors.append(f"{label} metric mismatch: {key}, raw={value!r}, exported={actual.get(key)!r}")


def _index_rows(rows: list[dict[str, str]], trial_name: str, key_fields: tuple[str, ...],
                row_errors: list[str], label: str) -> dict[tuple[str, ...], dict[str, str]]:
    result = {}
    for row in rows:
        if row.get("trial_dir") != trial_name:
            continue
        key = tuple(row.get(field, "") for field in key_fields)
        if key in result:
            row_errors.append(f"{label} duplicate exported row: {key}")
        result[key] = row
    return result


def _check_attempts(trial_name: str, raw: dict, rows: list[dict[str, str]], errors: list[str]):
    actual = _index_rows(rows, trial_name, ("peer_id", "step_id"), errors, "attempts.csv")
    expected = raw["attempts"]
    expected_keys = {(peer, step) for peer, step in expected}
    if set(actual) != expected_keys:
        errors.append("attempts.csv raw/export peer-step key set differs from every raw step_start")
    required_columns = ("start_event_ref", "first_assembly_event_ref", "first_lead_assembly_event_ref",
                        "first_outgoing_join_id", "first_outgoing_join_event_ref",
                        "first_outgoing_join_selection_event_ref", "first_outgoing_join_peer_id",
                        "first_outgoing_join_destination", "first_outgoing_choice", "ever_tried_x",
                        "accepted_x_reply_count", "accepted_x_reply_event_refs",
                        "accepted_other_reply_count", "accepted_other_reply_event_refs",
                        "first_group_event_ref",
                        "first_group_leader_peer_id", "first_group_leader_name",
                        "first_group_leader_category")
    if actual:
        missing_columns = [column for column in required_columns
                           if column not in next(iter(actual.values()))]
        if missing_columns:
            errors.append(f"attempts.csv lacks required raw-link columns: {missing_columns}")
    for key in expected_keys & set(actual):
        peer, step_id = key
        expected_attempt = expected[key]
        row = actual[key]
        choice = raw["attempt_choice"].get(key, dict(first=None, destination="none", ever_tried_x=False))
        first_join = choice["first"]
        group_id = expected_attempt["first_assembly_group_id"]
        group = raw["groups"].get(group_id)
        start = raw["starts_by_attempt"].get(key)
        leader_name = _peer_name(raw["config"], group["peer"]) if group else None
        leader_category = "no_group" if group is None else ("X" if leader_name == "X" else "other")
        expected_values = dict(
            peer_name=expected_attempt["peer_name"],
            start_event_ref=event_ref(start) if start else None,
            started_monotonic=start["monotonic"] if start else None,
            started_wall=start["wall_time"] if start else None,
            first_assembly_group_id=group_id,
            first_assembly_event_ref=event_ref(group) if group else None,
            first_assembly_monotonic=group["monotonic"] if group else None,
            first_assembly_latency_seconds=(group["monotonic"] - start["monotonic"] if group and start else None),
            first_group_event_ref=event_ref(group) if group else None,
            first_group_latency_seconds=(group["monotonic"] - start["monotonic"] if group and start else None),
            capped_latency_seconds=expected_attempt["capped_latency_seconds"],
            first_lead_group_id=expected_attempt["first_lead_group_id"],
            first_lead_assembly_event_ref=(event_ref(raw["groups"][expected_attempt["first_lead_group_id"]])
                                           if expected_attempt["first_lead_group_id"] else None),
            lead_latency_seconds=expected_attempt["lead_latency_seconds"],
            led_success=expected_attempt["led_success"],
            membership_success=expected_attempt["membership_success"],
            first_outgoing_join_id=first_join["join_id"] if first_join else None,
            first_outgoing_join_origin=first_join["origin"] if first_join else None,
            first_outgoing_join_event_ref=event_ref(first_join["start"]) if first_join else None,
            first_outgoing_join_selection_event_ref=first_join["selection_event_ref"] if first_join else None,
            first_outgoing_join_peer_id=first_join["leader_peer_id"] if first_join else None,
            first_outgoing_join_peer_name=first_join["leader_name"] if first_join else None,
            first_outgoing_join_destination=choice["destination"],
            first_outgoing_choice=choice["destination"],
            ever_tried_x=choice["ever_tried_x"],
            x_join_attempt_count=sum(join["leader_peer_id"] == raw["config"]["peer_ids"]["X"]
                                     for join in raw["joins_by_attempt"].get(key, [])),
            first_group_leader_peer_id=group["peer"] if group else None,
            first_group_leader_name=leader_name,
            first_group_leader_category=leader_category,
        )
        accepts = raw["attempt_accepts"].get(key, {"X": [], "other": []})
        expected_values["accepted_x_reply_count"] = len(accepts["X"])
        expected_values["accepted_other_reply_count"] = len(accepts["other"])
        expected_values["accepted_x_reply_event_refs"] = [event_ref(event) for event in accepts["X"]]
        expected_values["accepted_other_reply_event_refs"] = [event_ref(event) for event in accepts["other"]]
        for field, value in expected_values.items():
            if field not in row:
                # Some rich timing columns were not part of the Step 28 export
                # contract. Raw identity and decision fields above are required.
                if field in required_columns:
                    errors.append(f"attempt {key}: missing {field}")
                continue
            if not _same_value(row[field], value):
                errors.append(f"attempt {key}: {field} differs: raw={value!r}, csv={row[field]!r}")


def _check_groups(trial_name: str, raw: dict, rows: list[dict[str, str]], errors: list[str]):
    actual = _index_rows(rows, trial_name, ("group_id",), errors, "groups.csv")
    expected_ids = set(raw["groups"])
    if set(actual) != {(group_id,) for group_id in expected_ids}:
        errors.append("groups.csv raw/export unique group IDs differ")
    if actual:
        required = ("leader_peer_id", "leader_step_id", "roster", "assembly_event_ref",
                    "assembly_event_refs", "member_step_ids")
        missing = [field for field in required if field not in next(iter(actual.values()))]
        if missing:
            errors.append(f"groups.csv lacks exact raw assembly linkage columns: {missing}")
    events_by_group = {}
    for event in raw["events"]:
        if event["kind"] == "assembled":
            events_by_group.setdefault(event["detail"].get("group_id"), []).append(event)
    for (group_id,), row in actual.items():
        event = raw["groups"].get(group_id)
        if event is None:
            continue
        detail = event["detail"]
        members = detail["members"]
        expected_refs = [event_ref(item) for item in _ordered(events_by_group.get(group_id, []))]
        expected_steps = {peer: member.get("step_id") for peer, member in members.items()}
        values = dict(
            leader_peer_id=event["peer"], leader_step_id=detail.get("step_id"),
            roster=sorted(detail.get("roster", [])), assembly_event_ref=event_ref(event),
            assembly_event_refs=expected_refs, member_step_ids=expected_steps,
            assembled_member_step_ids=expected_steps,
        )
        for field, value in values.items():
            if field in row and not _same_value(row[field], value):
                errors.append(f"group {group_id}: {field} differs from raw assembly")
        if not any(field in row for field in ("member_step_ids", "assembled_member_step_ids")):
            errors.append(f"group {group_id}: export omits exact assembled member peer-step IDs")
        for ref in expected_refs:
            referenced = raw["events_by_ref"].get(ref)
            if (referenced is None or referenced["kind"] != "assembled" or
                    referenced["detail"].get("group_id") != group_id):
                errors.append(f"group {group_id}: invalid raw assembly event reference {ref}")


def _check_joins(trial_name: str, raw: dict, rows: list[dict[str, str]], errors: list[str]):
    actual = {key[0]: row for key, row in _index_rows(rows, trial_name, ("join_id",), errors, "joins.csv").items()}
    expected = {row["join_id"]: row for row in raw["joins"]}
    if set(actual) != set(expected):
        errors.append("joins.csv raw/export join ID set differs from join_attempt_start")
    required_columns = ("start_event_ref", "end_event_ref", "native_client_event_refs",
                        "accepted_reply_event_refs", "group_disbanded_reply_event_refs",
                        "request_expiration", "request_expirations", "selected_leader_expiration",
                        "first_outgoing_join_for_step", "first_outgoing_join_destination",
                        "ever_tried_x_for_step")
    if actual:
        missing_columns = [field for field in required_columns if field not in next(iter(actual.values()))]
        if missing_columns:
            errors.append(f"joins.csv lacks raw linkage/expiration columns: {missing_columns}")
    for join_id in set(actual) & set(expected):
        row, raw_row = actual[join_id], expected[join_id]
        key = raw_row["peer_id"], raw_row["step_id"]
        choice = raw["attempt_choice"].get(key, {})
        same_step_joins = raw["joins_by_attempt"].get(key, [])
        expected_first = bool(choice.get("first") and choice["first"]["join_id"] == join_id)
        expected_ever_x = any(item["leader_peer_id"] == raw["config"]["peer_ids"]["X"]
                              for item in same_step_joins)
        expected_destination = choice.get("destination", "none")
        expected_values = dict(
            peer_id=raw_row["peer_id"], peer_name=raw_row["peer_name"],
            leader_peer_id=raw_row["leader_peer_id"], leader_name=raw_row["leader_name"],
            step_id=raw_row["step_id"], search_id=raw_row["search_id"], origin=raw_row["origin"],
            selection_event_ref=raw_row["selection_event_ref"],
            start_event_ref=raw_row["start_event_ref"], end_event_ref=raw_row["end_event_ref"],
            native_client_event_refs=raw_row["native_client_event_refs"],
            accepted_reply_event_refs=raw_row["accepted_reply_event_refs"],
            group_disbanded_reply_event_refs=raw_row["group_disbanded_reply_event_refs"],
            reply_codes=raw_row["reply_codes"], request_expiration=raw_row["request_expiration"],
            request_expirations=raw_row["request_expirations"],
            selected_leader_expiration=raw_row["selected_leader_expiration"],
            first_outgoing_join_for_step=expected_first,
            first_outgoing_join_destination=expected_destination if expected_first else None,
            ever_tried_x_for_step=expected_ever_x,
        )
        for field, value in expected_values.items():
            if field not in row:
                if field in required_columns:
                    errors.append(f"join {join_id}: missing {field}")
                continue
            if not _same_value(row[field], value):
                errors.append(f"join {join_id}: {field} differs: raw={value!r}, csv={row[field]!r}")


def _compare_trial(trial_path: Path, job: dict, trial_name: str, suite_rows: dict,
                   exports: dict[str, list[dict[str, str]]]) -> dict:
    row_errors = []
    metrics = {}
    try:
        raw = raw_trial(trial_path)
        row_errors.extend(raw["errors"])
        harness = _load_harness()
        harness.validate_trial_config(raw["config"], job)
        completion = read_json(trial_path / "completion.json")
        if completion.get("all_peers_shutdown") is not True:
            row_errors.append("completion lacks all-peer shutdown evidence")
        file_hashes = completion.get("file_sha256")
        if not isinstance(file_hashes, dict) or not file_hashes:
            row_errors.append("completion lacks file_sha256 evidence")
            file_hashes = {}
        for filename, digest in file_hashes.items():
            file_path = trial_path / filename
            if not file_path.is_file() or sha256(file_path) != digest:
                row_errors.append(f"completion hash mismatch: {filename}")
        expected_completion_files = {"config.json", "events.jsonl", "summary.json",
                                     "analysis.json", "control_analysis.json"}
        if set(file_hashes) != expected_completion_files:
            row_errors.append("completion file hash set differs from frozen Step 28 contract")

        analysis_files = [trial_path / name for name in ("control_analysis.json", "analysis.json")
                          if (trial_path / name).is_file()]
        if not (trial_path / "control_analysis.json").is_file():
            row_errors.append("missing per-trial control_analysis.json")
        for analysis_path in analysis_files:
            stored = read_json(analysis_path)
            if analysis_path.name == "control_analysis.json":
                if stored.get("integrity_pass") is not True:
                    row_errors.append("control_analysis.json is not marked integrity_pass")
            if "summary_metrics" in stored:
                _compare_summary_metrics(row_errors, raw["metrics"], stored.get("summary_metrics"),
                                         analysis_path.name)
        _compare_summary_metrics(row_errors, raw["metrics"], suite_rows.get("summary_metrics"),
                                 "suite summary.json trial")
        _check_attempts(trial_name, raw, exports.get("attempts.csv", []), row_errors)
        _check_groups(trial_name, raw, exports.get("groups.csv", []), row_errors)
        _check_joins(trial_name, raw, exports.get("joins.csv", []), row_errors)
        _check_retry_export(trial_name, raw, exports.get("retries.csv", []), row_errors)
        metrics = raw["metrics"]
        metrics["clock_audit"] = raw["clock_audit"]
    except (OSError, ValueError, KeyError, TypeError, ImportError, csv.Error) as exc:
        row_errors.append(f"{type(exc).__name__}: {exc}")
    return dict(trial_dir=trial_name, **{"pass": not row_errors}, errors=row_errors, metrics=metrics)


def _check_retry_export(trial_name: str, raw: dict, rows: list[dict[str, str]], errors: list[str]):
    actual = _index_rows(rows, trial_name, ("selection_event_ref",), errors, "retries.csv")
    expected = raw["retries"]
    if set(actual) != {(ref,) for ref in expected}:
        errors.append("retries.csv exported renewal witnesses differ from raw selections")
    for (selection_ref,), row in actual.items():
        witness = expected.get(selection_ref)
        if witness is None:
            continue
        for field, value in (("old_pair_expiration", witness["old_values"]),
                             ("x_search_id", witness["x_search_id"])):
            if field not in row:
                errors.append(f"retry {selection_ref}: missing {field}")
            elif field == "old_pair_expiration":
                try:
                    if float(row[field]) not in value:
                        errors.append(f"retry {selection_ref}: old expiration is not a raw witness")
                except (TypeError, ValueError):
                    errors.append(f"retry {selection_ref}: invalid old expiration")
            elif not _same_value(row[field], value):
                errors.append(f"retry {selection_ref}: {field} differs from raw witness")


def _confirm_signal(jobs: list[dict], trial_reports: dict[str, dict], harness) -> tuple[dict, list[dict], float | None]:
    """Independently compute the descriptive eventual-X leadership signal."""
    if not jobs or jobs[0].get("duration") != 120:
        return dict(applicable=False), [], None
    contrasts = []
    for seed in (2801, 2802, 2803):
        pair = {}
        for job in jobs:
            if job.get("seed") == seed:
                pair[job["case"]["name"]] = trial_reports.get(harness.trial_name(job))
        single = (pair.get("reliable_single") or {}).get("metrics", {})
        renewed = (pair.get("reliable_renewed") or {}).get("metrics", {})
        single_n, renewed_n = single.get("x_submitted_attempts"), renewed.get("x_submitted_attempts")
        single_led, renewed_led = single.get("x_led_attempts"), renewed.get("x_led_attempts")
        single_rate = single.get("x_led_rate")
        renewed_rate = renewed.get("x_led_rate")
        delta = (float(renewed_rate) - float(single_rate)
                 if single_rate is not None and renewed_rate is not None else None)
        contrasts.append(dict(
            seed=seed, complete=("x_submitted_attempts" in single and
                                 "x_submitted_attempts" in renewed),
            reliable_single_x_attempts=single_n or 0,
            reliable_renewed_x_attempts=renewed_n or 0,
            reliable_single_x_led_attempts=single_led or 0,
            reliable_renewed_x_led_attempts=renewed_led or 0,
            reliable_single_x_led_rate=single_rate,
            reliable_renewed_x_led_rate=renewed_rate,
            delta_x_led_attempts_renewed_minus_single=(
                (renewed_led - single_led) if single_led is not None and renewed_led is not None else None),
            delta_x_led_rate_renewed_minus_single=delta,
            descriptive_leadership_signal=(delta is not None and delta > 0.0),
        ))
    deltas = [row["delta_x_led_rate_renewed_minus_single"] for row in contrasts]
    mean_delta = statistics.mean(deltas) if len(deltas) == 3 and all(v is not None for v in deltas) else None
    per_seed_positive = len(deltas) == 3 and all(value is not None and value > 0 for value in deltas)
    attempt_floor = len(contrasts) == 3 and all(
        row["reliable_single_x_attempts"] >= 10 and row["reliable_renewed_x_attempts"] >= 10
        for row in contrasts)
    passed = per_seed_positive and mean_delta is not None and mean_delta >= 0.10 and attempt_floor
    signal = dict(
        applicable=True,
        rate_definition="eventual own-step X-led assemblies divided by every raw X step_start",
        contrast_definition="reliable_renewed minus reliable_single within each seed",
        all_three_seed_x_led_rate_deltas_positive=per_seed_positive,
        unweighted_mean_x_led_rate_delta=mean_delta,
        mean_x_led_rate_delta_at_least_0p10=mean_delta is not None and mean_delta >= 0.10,
        each_arm_each_seed_has_at_least_10_x_attempts=attempt_floor,
        pass_=passed,
    )
    return signal, contrasts, mean_delta


def _compare_confirm_exports(summary: dict, contrast_rows: list[dict[str, str]],
                             contrasts: list[dict], mean_delta: float | None,
                             errors: list[str]):
    if not contrasts:
        return
    actual_summary = summary.get("per_seed_contrasts")
    by_seed_summary = {row.get("seed"): row for row in actual_summary or [] if isinstance(row, dict)}
    by_seed_csv = {}
    for row in contrast_rows:
        try:
            seed = int(row.get("seed", ""))
        except (ValueError, TypeError):
            errors.append("per_seed_contrasts.csv has invalid seed")
            continue
        if seed in by_seed_csv:
            errors.append(f"per_seed_contrasts.csv duplicate seed {seed}")
        by_seed_csv[seed] = row
    expected_seeds = {2801, 2802, 2803}
    if set(by_seed_summary) != expected_seeds:
        errors.append("summary per_seed_contrasts does not contain exactly seeds 2801, 2802, 2803")
    if set(by_seed_csv) != expected_seeds:
        errors.append("per_seed_contrasts.csv does not contain exactly seeds 2801, 2802, 2803")
    fields = (
        "complete", "reliable_single_x_attempts", "reliable_renewed_x_attempts",
        "reliable_single_x_led_attempts", "reliable_renewed_x_led_attempts",
        "reliable_single_x_led_rate", "reliable_renewed_x_led_rate",
        "delta_x_led_attempts_renewed_minus_single", "delta_x_led_rate_renewed_minus_single",
        "descriptive_leadership_signal",
    )
    for expected in contrasts:
        seed = expected["seed"]
        for label, rows in (("summary", by_seed_summary), ("CSV", by_seed_csv)):
            actual = rows.get(seed)
            if actual is None:
                continue
            for field in fields:
                if field not in actual:
                    errors.append(f"per-seed {label} row {seed} missing {field}")
                elif not _same_value(actual[field], expected.get(field)):
                    errors.append(f"per-seed {label} row {seed}: {field} differs from raw")
    means = summary.get("unweighted_mean_differences_across_seeds", {})
    if "delta_x_led_rate_renewed_minus_single" not in means or not _same_value(
            means.get("delta_x_led_rate_renewed_minus_single"), mean_delta):
        errors.append("summary unweighted per-seed eventual X-led rate delta differs from raw")
    expected_signal = dict(
        all_three_seed_x_led_rate_deltas_positive=(
            len(contrasts) == 3 and all(
                row["delta_x_led_rate_renewed_minus_single"] is not None and
                row["delta_x_led_rate_renewed_minus_single"] > 0 for row in contrasts)),
        mean_x_led_rate_delta=mean_delta,
        mean_x_led_rate_delta_at_least_0p10=mean_delta is not None and mean_delta >= 0.10,
        each_arm_each_seed_has_at_least_10_x_attempts=(
            len(contrasts) == 3 and all(
                row["reliable_single_x_attempts"] >= 10 and
                row["reliable_renewed_x_attempts"] >= 10 for row in contrasts)),
        pass_=(mean_delta is not None and mean_delta >= 0.10 and
               all(row["delta_x_led_rate_renewed_minus_single"] is not None and
                   row["delta_x_led_rate_renewed_minus_single"] > 0 and
                   row["reliable_single_x_attempts"] >= 10 and
                   row["reliable_renewed_x_attempts"] >= 10 for row in contrasts)),
    )
    for signal_name in ("descriptive_signal", "confirm_signal"):
        signal = summary.get(signal_name)
        if not isinstance(signal, dict):
            errors.append(f"summary lacks {signal_name}")
            continue
        for field, value in expected_signal.items():
            if field not in signal or not _same_value(signal.get(field), value):
                errors.append(f"summary {signal_name} {field} differs from raw")


def verify_suite(suite, preservation=None) -> dict:
    suite = Path(suite).resolve()
    report = dict(experiment="step28", suite=str(suite), errors=[], trials=[], totals={})
    errors = report["errors"]
    harness = None
    try:
        manifest = read_json(suite / "suite.json")
        provenance = read_json(suite / "provenance.json")
        harness = _load_harness()
        if manifest.get("experiment") != "step28" or manifest.get("jobs") != harness.make_jobs(manifest.get("phase")):
            errors.append("manifest differs from exact Step 28 job matrix")
        harness.validate_sources(suite, provenance)
    except (OSError, ValueError, KeyError, TypeError, ImportError) as exc:
        errors.append(f"suite provenance or design validation failed: {type(exc).__name__}: {exc}")
        manifest = {}

    exports = {}
    for filename in ("attempts.csv", "groups.csv", "joins.csv", "retries.csv",
                     "per_seed_contrasts.csv"):
        path = suite / filename
        try:
            if not path.is_file():
                errors.append(f"missing suite export: {filename}")
                exports[filename] = []
            else:
                exports[filename] = _csv_rows(path)
        except (OSError, csv.Error) as exc:
            errors.append(f"cannot read {filename}: {exc}")
            exports[filename] = []

    suite_summary = {}
    try:
        suite_summary = read_json(suite / "summary.json")
        summary_trials = {row["trial_dir"]: row for row in suite_summary.get("trials", [])
                          if isinstance(row, dict) and isinstance(row.get("trial_dir"), str)}
        if len(summary_trials) != len(suite_summary.get("trials", [])):
            errors.append("suite summary has missing or duplicate trial_dir rows")
        if suite_summary.get("experiment") != "step28":
            errors.append("suite summary is not labeled experiment=step28")
    except (OSError, ValueError, KeyError, TypeError) as exc:
        errors.append(f"cannot read suite summary: {type(exc).__name__}: {exc}")
        summary_trials = {}

    jobs = manifest.get("jobs", []) if isinstance(manifest, dict) else []
    try:
        if manifest and harness and len(jobs) != len(harness.make_jobs(manifest.get("phase"))):
            errors.append("manifest job count differs from canonical Step 28 matrix")
    except (ValueError, NameError):
        pass
    trial_names = set()
    for job in jobs:
        try:
            name = harness.trial_name(job)
            if name in trial_names:
                errors.append(f"duplicate canonical trial name: {name}")
            trial_names.add(name)
            path = suite / name
            if not path.is_dir():
                errors.append(f"missing trial directory: {name}")
                report["trials"].append(dict(trial_dir=name, **{"pass": False},
                                             errors=["missing trial directory"], metrics={}))
                continue
            summary_row = summary_trials.get(name)
            if summary_row is None:
                errors.append(f"suite summary missing trial: {name}")
                summary_row = {}
            trial_report = _compare_trial(path, job, name, summary_row, exports)
            errors.extend(f"{name}: {error}" for error in trial_report["errors"])
            report["trials"].append(trial_report)
        except (ValueError, KeyError, TypeError, NameError) as exc:
            errors.append(f"invalid job in Step 28 manifest: {type(exc).__name__}: {exc}")
    extra_summary = set(summary_trials) - trial_names
    if extra_summary:
        errors.append(f"suite summary contains noncanonical trials: {sorted(extra_summary)}")

    metric_keys = (
        "total_groups", "x_led_groups", "x_submitted_attempts", "x_led_attempts",
        "x_led_by_2s_count", "x_led_by_3s_count", "x_led_by_5s_count", "x_led_by_12s_count",
        "honest_submitted_attempts", "honest_no_group_attempts",
        "honest_first_group_x_led_attempts", "honest_first_group_other_led_attempts",
        "honest_first_choice_x_attempts", "honest_first_choice_other_attempts",
        "honest_no_outgoing_choice_attempts", "honest_ever_tried_x_attempts",
        "honest_accepted_x_reply_count", "honest_accepted_other_reply_count",
    )
    for key in metric_keys:
        report["totals"][key] = sum(row.get("metrics", {}).get(key, 0)
                                     for row in report["trials"])
    raw_trial_reports = {row["trial_dir"]: row for row in report["trials"]}
    report["totals"]["pooled_x_led_rate"] = (
        report["totals"].get("x_led_attempts", 0) /
        report["totals"].get("x_submitted_attempts", 0)
        if report["totals"].get("x_submitted_attempts", 0) else None)
    if harness is not None:
        signal, raw_contrasts, mean_delta = _confirm_signal(jobs, raw_trial_reports, harness)
    else:
        signal, raw_contrasts, mean_delta = dict(applicable=False), [], None
    report["confirm_signal"] = signal
    if signal.get("applicable"):
        _compare_confirm_exports(suite_summary, exports.get("per_seed_contrasts.csv", []),
                                 raw_contrasts, mean_delta, errors)
    if preservation is not None:
        try:
            baseline = read_json(preservation)
            files = baseline["files"]
            changed, invalid_paths = [], []
            if not isinstance(files, dict):
                raise ValueError("baseline files must be an absolute-path-to-digest mapping")
            for file, digest in files.items():
                file_path = Path(file)
                if not file_path.is_absolute():
                    invalid_paths.append(file)
                    continue
                if not file_path.is_file() or sha256(file_path) != digest:
                    changed.append(file)
            preservation_pass = not changed and not invalid_paths and len(files) == baseline.get("count", len(files))
            report["preservation"] = dict(count=len(files), baseline_count=baseline.get("count", len(files)),
                                          changed=changed, invalid_paths=invalid_paths,
                                          **{"pass": preservation_pass})
            if not preservation_pass:
                errors.append("prior source/raw evidence preservation failed")
        except (OSError, ValueError, KeyError, TypeError) as exc:
            errors.append(f"invalid preservation baseline: {type(exc).__name__}: {exc}")

    report["pass"] = not errors
    report.pop("pass_", None)
    report["verifier_sha256"] = sha256(__file__)
    report["exports_sha256"] = {path.name: sha256(path) for path in suite.glob("*.csv")}
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", required=True, type=Path)
    parser.add_argument("--preservation", type=Path)
    args = parser.parse_args()
    suite = Path(args.suite).resolve()
    try:
        manifest = read_json(suite / "suite.json")
        harness = _load_harness()
        is_step28_suite = (manifest.get("experiment") == "step28" and
                           manifest.get("jobs") == harness.make_jobs(manifest.get("phase")))
    except (OSError, ValueError, KeyError, TypeError, ImportError):
        is_step28_suite = False
    if not suite.is_dir() or not is_step28_suite:
        raise SystemExit("refusing to write a report outside the canonical Step 28 suite")
    report = verify_suite(suite, args.preservation)
    target = suite / "raw_verification_v2.json"
    target.write_text(json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n",
                      encoding="utf-8")
    print(json.dumps(dict(pass_=report["pass"], totals=report["totals"], report=str(target))))
    raise SystemExit(0 if report["pass"] else 1)


if __name__ == "__main__":
    main()
