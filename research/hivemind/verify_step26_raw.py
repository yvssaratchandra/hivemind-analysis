"""Independent, offline checks of a completed Step 26 pilot suite.

This verifier uses only the Python standard library. It reconstructs trial
attempts, group assemblies, winner expirations, and native selection event
references directly from the raw JSONL/JSON, then compares the exported CSVs.

Usage: python verify_step26_raw.py --suite PATH [--preservation PATH]
The report is written to ``PATH/raw_verification.json``. A preservation input
is read-only; the report records current hashes beside its baseline hashes.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import random
import statistics
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
X_NAME = "X"
PEER_NAMES = ("X", *(f"H{i}" for i in range(7)))
RATE_FIELDS = (1, 2, 5)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_json(path: Path) -> Any:
    with Path(path).open(encoding="utf-8") as stream:
        return json.load(stream)


def event_ref(event: dict[str, Any]) -> str:
    return f"{event['pid']}:{event['event_number']}"


def canonical_jobs() -> list[dict[str, Any]]:
    """Recreate the frozen 14-cell matrix without importing the experiment."""
    cases = []
    for delay in (0.0, 0.15):
        suffix = "d0" if delay == 0 else "d150"
        for lifetime in (1.0, 3.0):
            for frequency in (0.5, 2.0):
                cases.append(dict(
                    name=f"refresh_u{int(lifetime)}_f{'0p5' if frequency == .5 else '2'}_{suffix}",
                    mode="refresh", minimum=4, target=4, lifetime=lifetime,
                    frequency=frequency, nominal_lower=lifetime - 1 / frequency,
                    nominal_upper=lifetime, one_way_delay_seconds=delay))
            cases.append(dict(name=f"once_u{int(lifetime)}_{suffix}", mode="once",
                              minimum=4, target=4, lifetime=lifetime,
                              one_way_delay_seconds=delay))
        cases.append(dict(name=f"passive_stock_{suffix}", mode="passive", minimum=4,
                          target=4, lifetime=5.0, one_way_delay_seconds=delay))
    jobs = [dict(case=case, seed=2601, duration=30) for case in cases]
    random.Random(260026).shuffle(jobs)
    return jobs


def _same_number(left: Any, right: Any) -> bool:
    try:
        a, b = float(left), float(right)
    except (TypeError, ValueError):
        return False
    return math.isclose(a, b, rel_tol=1e-12, abs_tol=1e-12)


def _same_csv_value(actual: str | None, expected: Any) -> bool:
    if expected is None:
        return actual in (None, "", "None", "null")
    if isinstance(expected, bool):
        return actual is not None and actual.strip().lower() == str(expected).lower()
    if isinstance(expected, (int, float)) and not isinstance(expected, bool):
        return actual is not None and _same_number(actual, expected)
    if isinstance(expected, (dict, list)):
        if actual is None:
            return False
        try:
            parsed = json.loads(actual)
        except (json.JSONDecodeError, TypeError):
            return False
        return parsed == expected
    return actual == str(expected)


def _csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as stream:
        return list(csv.DictReader(stream))


def _compare_rows(path: Path, trial_dir: str, expected_rows: list[dict[str, Any]],
                  key_fields: tuple[str, ...], fields: tuple[str, ...], errors: list[str]) -> int:
    """Compare all selected raw-derived fields and enforce per-trial row counts."""
    if not path.is_file():
        errors.append(f"missing export: {path.name}")
        return 0
    try:
        rows = [row for row in _csv_rows(path) if row.get("trial_dir") == trial_dir]
    except (OSError, csv.Error) as exc:
        errors.append(f"cannot read {path.name}: {exc}")
        return 0
    if len(rows) != len(expected_rows):
        errors.append(f"{path.name}: {len(rows)} rows for {trial_dir}, raw gives {len(expected_rows)}")
    expected_by_key = {tuple(str(row.get(key)) for key in key_fields): row
                       for row in expected_rows}
    actual_by_key: dict[tuple[str, ...], dict[str, str]] = {}
    for row in rows:
        key = tuple(row.get(key, "") for key in key_fields)
        if key in actual_by_key:
            errors.append(f"{path.name}: duplicate key {key}")
        actual_by_key[key] = row
    if expected_by_key.keys() != actual_by_key.keys():
        errors.append(f"{path.name}: raw/export key set differs for {trial_dir}")
    for key in expected_by_key.keys() & actual_by_key.keys():
        expected, actual = expected_by_key[key], actual_by_key[key]
        for field in fields:
            if field not in expected:
                continue
            if field not in actual:
                errors.append(f"{path.name}: missing column {field}")
                continue
            if not _same_csv_value(actual[field], expected[field]):
                errors.append(f"{path.name}: {field} differs for {key}: "
                              f"csv={actual[field]!r}, raw={expected[field]!r}")
    return len(rows)


def _latest_same_process_ad(events_by_pid: dict[int, list[dict[str, Any]]],
                            assembly: dict[str, Any]) -> dict[str, Any] | None:
    candidates = [event for event in events_by_pid.get(assembly["pid"], [])
                  if event["kind"] == "ad_started" and event["peer"] == assembly["peer"]
                  and event["event_number"] < assembly["event_number"]
                  and event["monotonic"] <= assembly["monotonic"]]
    return candidates[-1] if candidates else None


def _latest_x_ad(x_ads: list[dict[str, Any]], assembly: dict[str, Any]) -> dict[str, Any] | None:
    candidates = [event for event in x_ads if event["monotonic"] <= assembly["monotonic"]]
    return candidates[-1] if candidates else None


def _percentile(values: list[float], fraction: float) -> float | None:
    values = sorted(values)
    if not values:
        return None
    position = (len(values) - 1) * fraction
    lower, upper = math.floor(position), math.ceil(position)
    if lower == upper:
        return values[lower]
    weight = position - lower
    return values[lower] * (1 - weight) + values[upper] * weight


def _raw_selection_state(detail: dict[str, Any], selected_peer: str,
                         x_peer: str) -> tuple[float | None, bool, bool]:
    """Recheck direct queue/expiration predicates without inferring read eligibility."""
    queue = detail.get("queue", [])
    parsed = []
    for item in queue:
        if not isinstance(item, dict):
            continue
        try:
            parsed.append((item["peer"], float(item["expiration"]),
                           bytes.fromhex(item["peer_bytes_hex"])))
        except (KeyError, TypeError, ValueError):
            continue
    x_candidates = [item for item in parsed if item[0] == x_peer]
    x_candidate = min(x_candidates, key=lambda item: (item[1], item[2])) if x_candidates else None
    x_expiration = x_candidate[1] if x_candidate else None
    try:
        top_wall = float(detail["selection_top_wall"])
    except (KeyError, TypeError, ValueError):
        return x_expiration, False, False
    past = set()
    for item in detail.get("past_attempts", []):
        if isinstance(item, dict):
            try:
                past.add((item["peer"], float(item["expiration"])))
            except (KeyError, TypeError, ValueError):
                pass
    try:
        own_expiration = float(detail["own_expiration"])
        own_bytes = bytes.fromhex(detail["own_peer_bytes_hex"])
    except (KeyError, TypeError, ValueError):
        own_expiration, own_bytes = None, None
    x_eligible = bool(x_candidate and x_candidate[1] > top_wall and
                      (x_candidate[0], x_candidate[1]) not in past and
                      own_expiration is not None and own_bytes is not None and
                      (x_candidate[1], x_candidate[2]) <= (own_expiration, own_bytes))
    try:
        selected_expiration = float(detail["selected_expiration"])
        selected_bytes = bytes.fromhex(detail["selected_peer_bytes_hex"])
    except (KeyError, TypeError, ValueError):
        return x_expiration, x_eligible, False
    selected_in_queue = any(peer == selected_peer and expiration == selected_expiration and
                            peer_bytes == selected_bytes
                            for peer, expiration, peer_bytes in parsed)
    selected_live = selected_expiration > top_wall
    selected_le_own = bool(own_expiration is not None and own_bytes is not None and
                           (selected_expiration, selected_bytes) <= (own_expiration, own_bytes))
    live_queue = [item for item in parsed if item[1] > top_wall]
    native_top = min(live_queue, key=lambda item: (item[1], item[0])) if live_queue else None
    selected_is_top = bool(native_top and native_top[0] == selected_peer and
                           native_top[1] == selected_expiration and native_top[2] == selected_bytes)
    return x_expiration, x_eligible, bool(selected_in_queue and selected_live and
                                           selected_le_own and selected_is_top)


def verify_trial(suite: Path, trial_dir: str, job: dict[str, Any],
                 exports: dict[str, Path]) -> dict[str, Any]:
    errors: list[str] = []
    trial = suite / trial_dir
    config_path, events_path = trial / "config.json", trial / "events.jsonl"
    summary_path, complete_path = trial / "summary.json", trial / "completion.json"
    result: dict[str, Any] = dict(trial_dir=trial_dir, case=job["case"]["name"],
                                 seed=job["seed"], errors=errors)
    try:
        config = read_json(config_path)
        summary = read_json(summary_path)
        completion = read_json(complete_path)
    except (OSError, json.JSONDecodeError) as exc:
        result["pass"] = False
        result["error"] = f"missing/invalid trial JSON: {exc}"
        return result

    if config.get("experiment") != "step26":
        errors.append("config is not labeled experiment=step26")
    for key, expected in (("case", job["case"]), ("seed", job["seed"]),
                          ("duration", job["duration"]), ("outcome", "grouping"),
                          ("instrumentation", "detailed"), ("attempt_unit", "step_start")):
        if config.get(key) != expected:
            errors.append(f"config {key} differs from canonical job")
    if config.get("one_way_delay_seconds") != job["case"]["one_way_delay_seconds"]:
        errors.append("config one-way delay differs from canonical job")
    if config.get("network_delay_injected") != (job["case"]["one_way_delay_seconds"] > 0):
        errors.append("config network_delay_injected does not match delay setting")
    peer_ids = config.get("peer_ids", {})
    if set(peer_ids) != set(PEER_NAMES) or len(set(peer_ids.values())) != 8:
        errors.append("config does not contain eight uniquely identified peers")
    x_peer = peer_ids.get(X_NAME)
    names_by_peer = {peer: name for name, peer in peer_ids.items()}
    if summary.get("error") is not None or summary.get("pending"):
        errors.append("summary reports controller error or pending peers")
    if completion.get("all_peers_shutdown") is not True:
        errors.append("completion does not confirm all peers shut down")
    for filename, expected_hash in completion.get("file_sha256", {}).items():
        file_path = trial / filename
        if not file_path.is_file() or sha256(file_path) != expected_hash:
            errors.append(f"completion hash mismatch for {filename}")
    required_completion_files = {"config.json", "events.jsonl", "summary.json",
                                 "analysis.json", "joint_analysis.json"}
    if set(completion.get("file_sha256", {})) != required_completion_files:
        errors.append("completion file set differs from frozen Step 26 contract")

    try:
        events = [json.loads(line) for line in events_path.open(encoding="utf-8") if line.strip()]
    except (OSError, json.JSONDecodeError) as exc:
        errors.append(f"cannot parse events.jsonl: {exc}")
        events = []
    refs: dict[str, dict[str, Any]] = {}
    events_by_pid: dict[int, list[dict[str, Any]]] = {}
    for event in events:
        try:
            ref = event_ref(event)
            if ref in refs:
                errors.append(f"duplicate event reference {ref}")
            refs[ref] = event
            events_by_pid.setdefault(event["pid"], []).append(event)
            # Sleep-leg instrumentation is emitted outside the RPC handler and
            # is linked by rpc_id rather than by a peer identity.
            if event.get("peer") not in names_by_peer and not (
                    event.get("kind") == "control_rpc_delay" and event.get("peer") == ""):
                errors.append(f"event {ref} has unknown peer identity")
            if not isinstance(event.get("detail"), dict):
                errors.append(f"event {ref} has malformed detail")
        except (KeyError, TypeError):
            errors.append("event has malformed reference fields")
    for rows in events_by_pid.values():
        rows.sort(key=lambda event: (event["event_number"], event["monotonic"]))

    step_starts: dict[tuple[str, str], dict[str, Any]] = {}
    step_results: dict[tuple[str, str], dict[str, Any]] = {}
    searches: dict[tuple[str, str], list[dict[str, Any]]] = {}
    groups_by_id: dict[str, dict[str, Any]] = {}
    selection_events = {}
    read_ends: dict[str, dict[str, Any]] = {}
    followed_group_ids: dict[tuple[str, str], set[str]] = {}
    x_ads = [event for event in events if event["kind"] == "ad_started" and event["peer"] == x_peer]
    x_ads.sort(key=lambda event: (event["monotonic"], event["pid"], event["event_number"]))
    for event in events:
        kind, detail, peer = event["kind"], event["detail"], event["peer"]
        step_id = detail.get("step_id")
        if kind in ("step_start", "step_result"):
            if not isinstance(step_id, str) or not step_id:
                errors.append(f"{kind} missing step_id at {event_ref(event)}")
                continue
            index = (peer, step_id)
            target = step_starts if kind == "step_start" else step_results
            if index in target:
                errors.append(f"duplicate {kind} for {index}")
            else:
                target[index] = event
        elif kind == "search_span_start":
            if not isinstance(step_id, str) or not step_id:
                errors.append(f"search span missing step_id at {event_ref(event)}")
            else:
                searches.setdefault((peer, step_id), []).append(event)
        elif kind == "ad_read_end":
            read_id = detail.get("read_id")
            if isinstance(read_id, str):
                read_ends[read_id] = event
        elif kind == "native_pop_selected":
            selection_events[event_ref(event)] = event
        elif (kind == "native_client_reply" and
              detail.get("code") == "BEGIN_ALLREDUCE" and
              isinstance(detail.get("group_id"), str)):
            followed_group_ids.setdefault((peer, step_id), set()).add(detail["group_id"])
        elif kind == "assembled":
            group_id = detail.get("group_id")
            if not isinstance(group_id, str) or not group_id:
                errors.append(f"assembly missing group_id at {event_ref(event)}")
                continue
            if group_id in groups_by_id:
                previous = groups_by_id[group_id]
                if previous["peer"] != peer or previous["detail"].get("roster") != detail.get("roster"):
                    errors.append(f"conflicting duplicate assembly ID {group_id}")
                else:
                    errors.append(f"duplicate assembly event for group {group_id}")
            else:
                groups_by_id[group_id] = event

    if step_starts.keys() != step_results.keys():
        errors.append("raw step_start and step_result keys differ")
    if len(summary.get("outcomes", [])) != len(step_results):
        errors.append("summary outcome count differs from raw step_result count")

    start_time = config.get("started_monotonic")
    duration = config.get("duration")
    if not isinstance(start_time, (int, float)) or not isinstance(duration, (int, float)):
        errors.append("config has no numeric trial start/duration")
        cutoff = float("inf")
    else:
        cutoff = start_time + duration

    assembly_records = []
    for group_id, event in groups_by_id.items():
        detail = event["detail"]
        roster = detail.get("roster")
        leader_peer = event["peer"]
        user = detail.get("user") or {}
        leader_step = detail.get("step_id") or user.get("step_id")
        if not isinstance(roster, list) or len(roster) != len(set(roster)):
            errors.append(f"group {group_id} has invalid or duplicate roster")
            roster = roster if isinstance(roster, list) else []
        if leader_peer not in roster:
            errors.append(f"group {group_id} leader is missing from its roster")
        if user and user.get("step_id") != leader_step:
            errors.append(f"group {group_id} leader user/step IDs differ")
        leader_ad = _latest_same_process_ad(events_by_pid, event)
        x_ad = _latest_x_ad(x_ads, event)
        leader_exp = leader_ad["detail"].get("expiration") if leader_ad else None
        x_exp = x_ad["detail"].get("expiration") if x_ad else None
        assembly_wall = event.get("wall_time")
        gap = x_exp - leader_exp if x_exp is not None and leader_exp is not None else None
        record = dict(
            trial_dir=trial_dir, case=job["case"]["name"], seed=job["seed"],
            group_id=group_id, leader_name=names_by_peer.get(leader_peer),
            leader_peer_id=leader_peer, leader_step_id=leader_step,
            roster_size=len(roster), roster=sorted(roster), x_in_roster=x_peer in roster,
            assembly_event_ref=event_ref(event),
            leader_ad_started_event_ref=event_ref(leader_ad) if leader_ad else None,
            leader_expiration=leader_exp,
            x_ad_started_event_ref=event_ref(x_ad) if x_ad else None,
            x_expiration=x_exp, x_minus_leader_expiration_gap=gap,
            assembly_monotonic=event["monotonic"], assembly_wall=assembly_wall)
        assembly_records.append(record)
    assembly_records.sort(key=lambda row: (row["assembly_monotonic"], row["group_id"]))

    led_groups_by_attempt: dict[tuple[str, str], list[dict[str, Any]]] = {}
    all_groups_by_attempt: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for record in assembly_records:
        key = (record["leader_peer_id"], record["leader_step_id"])
        if None not in key:
            all_groups_by_attempt.setdefault(key, []).append(record)
            led_groups_by_attempt.setdefault(key, []).append(record)
    group_by_id = {row["group_id"]: row for row in assembly_records}
    for attempt_key, group_ids in followed_group_ids.items():
        for group_id in group_ids:
            group = group_by_id.get(group_id)
            if group and attempt_key[0] in group["roster"]:
                rows = all_groups_by_attempt.setdefault(attempt_key, [])
                if all(existing["group_id"] != group_id for existing in rows):
                    rows.append(group)

    attempt_records = []
    for (peer, step_id), start_event in step_starts.items():
        start = start_event["monotonic"]
        submitted = start < cutoff
        search_rows = sorted(searches.get((peer, step_id), []),
                             key=lambda e: (e["monotonic"], e["pid"], e["event_number"]))
        assembly_rows = sorted(all_groups_by_attempt.get((peer, step_id), []),
                               key=lambda row: row["assembly_monotonic"])
        led_rows = sorted(led_groups_by_attempt.get((peer, step_id), []),
                          key=lambda row: row["assembly_monotonic"])
        first_led = led_rows[0] if led_rows else None
        first_assembly = assembly_rows[0] if assembly_rows else None
        first_search = search_rows[0] if search_rows else None
        latency = (first_led["assembly_monotonic"] - first_search["monotonic"]
                   if first_led and first_search else None)
        if latency is not None and latency < 0:
            errors.append(f"X leadership precedes first search for {step_id}")
        followed = any(row["leader_peer_id"] != peer for row in assembly_rows)
        result_event = step_results.get((peer, step_id))
        status = (result_event["detail"].get("status") if result_event else None)
        if led_rows:
            eventual = "led"
        elif followed:
            eventual = "followed"
        elif status == "no_group":
            eventual = "no_group"
        else:
            eventual = "failed"
        rec = dict(
            trial_dir=trial_dir, case=job["case"]["name"], seed=job["seed"], step_id=step_id,
            peer_id=peer, peer_name=names_by_peer.get(peer), submission_started=submitted,
            started_monotonic=start, started_wall=start_event.get("wall_time"),
            start_event_ref=event_ref(start_event),
            first_search_event_ref=event_ref(first_search) if first_search else None,
            search_count=len(search_rows), led_success=bool(led_rows),
            followed_success=followed, assembled_success=bool(assembly_rows),
            eventual_outcome=eventual,
            first_led_group_id=first_led["group_id"] if first_led else None,
            first_led_assembly_event_ref=first_led["assembly_event_ref"] if first_led else None,
            assembly_latency_seconds=latency,
            success_by_1s=bool(led_rows and latency is not None and latency <= 1),
            success_by_2s=bool(led_rows and latency is not None and latency <= 2),
            success_by_5s=bool(led_rows and latency is not None and latency <= 5))
        attempt_records.append(rec)
    attempt_records.sort(key=lambda row: (row["started_monotonic"], row["peer_id"], row["step_id"]))

    x_attempts = [row for row in attempt_records if row["peer_name"] == X_NAME and row["submission_started"]]
    x_led_attempts = [row for row in x_attempts if row["led_success"]]
    x_groups = [row for row in assembly_records if row["leader_peer_id"] == x_peer]
    x_latencies = [row["assembly_latency_seconds"] for row in x_led_attempts
                   if row["assembly_latency_seconds"] is not None]
    x_early = {str(seconds): sum(row[f"success_by_{seconds}s"] for row in x_attempts)
               for seconds in RATE_FIELDS}

    # Native selection opportunities are taken from the compact CSV; each row
    # must point to a raw pop event and its linked raw read. We deliberately do
    # not recompute the analyzer's eligibility denominator from native queues.
    if exports["choices"].is_file():
        try:
            choice_rows = [r for r in _csv_rows(exports["choices"]) if r.get("trial_dir") == trial_dir]
        except (OSError, csv.Error) as exc:
            errors.append(f"cannot read choices.csv: {exc}")
            choice_rows = []
    else:
        errors.append("missing export: choices.csv")
        choice_rows = []
    choice_refs = set()
    chose_x = 0
    for row in choice_rows:
        ref = row.get("event_ref", "")
        if not ref or ref in choice_refs:
            errors.append(f"choices.csv has missing/duplicate native event reference {ref!r}")
        choice_refs.add(ref)
        event = selection_events.get(ref)
        if event is None:
            errors.append(f"choices.csv event_ref {ref!r} is not a raw native_pop_selected event")
            continue
        detail = event["detail"]
        for field, expected in (("peer_id", event["peer"]), ("step_id", detail.get("step_id")),
                                ("search_id", detail.get("search_id")),
                                ("leader_peer_id", event.get("other")),
                                ("leader_name", names_by_peer.get(event.get("other"))),
                                ("selected_expiration", detail.get("selected_expiration")),
                                ("own_expiration", detail.get("own_expiration")),
                                ("selection_top_wall", detail.get("selection_top_wall"))):
            if field in row and not _same_csv_value(row[field], expected):
                errors.append(f"choices.csv {field} disagrees with raw event {ref}")
        read_id = detail.get("read_id")
        read_event = read_ends.get(read_id)
        expected_read_ref = event_ref(read_event) if read_event else None
        if not read_event:
            errors.append(f"native selection {ref} has no matching raw ad_read_end for {read_id}")
        elif read_event["peer"] != event["peer"] or read_event["detail"].get("search_id") != detail.get("search_id"):
            errors.append(f"native selection {ref} read reference identity differs")
        if row.get("read_event_ref") != (expected_read_ref or ""):
            errors.append(f"choices.csv read_event_ref differs from raw read for {ref}")
        x_expiration, x_eligible, selection_valid = _raw_selection_state(detail, event.get("other"), x_peer)
        for field, expected in (("x_expiration", x_expiration), ("x_eligible", x_eligible),
                                ("selection_valid", selection_valid),
                                ("chose_X", event.get("other") == x_peer)):
            if field in row and not _same_csv_value(row[field], expected):
                errors.append(f"choices.csv {field} disagrees with raw event {ref}")
        if event.get("other") == x_peer:
            chose_x += 1
    choice_count = len(choice_rows)

    # Verify exported rows against independently reconstructed raw records.
    group_fields = ("case", "seed", "group_id", "leader_name", "leader_peer_id", "leader_step_id",
                    "roster_size", "roster", "x_in_roster", "assembly_event_ref",
                    "leader_ad_started_event_ref", "leader_expiration", "x_ad_started_event_ref",
                    "x_expiration", "x_minus_leader_expiration_gap")
    _compare_rows(exports["groups"], trial_dir, assembly_records, ("trial_dir", "group_id"),
                  group_fields, errors)
    attempt_fields = ("case", "seed", "step_id", "peer_id", "peer_name", "submission_started",
                      "started_monotonic", "started_wall", "start_event_ref", "first_search_event_ref", "search_count",
                      "led_success", "followed_success", "assembled_success", "eventual_outcome",
                      "first_led_group_id", "first_led_assembly_event_ref", "assembly_latency_seconds",
                      "success_by_1s", "success_by_2s", "success_by_5s")
    _compare_rows(exports["attempts"], trial_dir, attempt_records,
                  ("trial_dir", "step_id", "peer_id"), attempt_fields, errors)

    # The first-eligible export is a filtered view. Check exact row count and
    # raw fields above; eligibility remains the analyzer's explicitly scoped
    # inference, rather than duplicating its read-causality reconstruction.
    raw_selection_records = []
    for row in choice_rows:
        raw_event = selection_events.get(row.get("event_ref", ""))
        if not raw_event:
            continue
        detail = raw_event["detail"]
        x_expiration, x_eligible, selection_valid = _raw_selection_state(
            detail, raw_event.get("other"), x_peer)
        raw_selection_records.append(dict(
            trial_dir=trial_dir, case=job["case"]["name"], seed=job["seed"],
            peer_name=names_by_peer.get(raw_event["peer"]), peer_id=raw_event["peer"],
            step_id=detail.get("step_id"), search_id=detail.get("search_id"),
            group_key=detail.get("group_key"),
            event_ref=event_ref(raw_event), read_event_ref=(event_ref(read_ends[detail["read_id"]])
                if detail.get("read_id") in read_ends else None),
            leader_name=names_by_peer.get(raw_event.get("other")),
            leader_peer_id=raw_event.get("other"),
            selected_expiration=detail.get("selected_expiration"),
            own_expiration=detail.get("own_expiration"),
            selection_top_wall=detail.get("selection_top_wall"),
            x_expiration=x_expiration, x_eligible=x_eligible,
            selection_valid=selection_valid,
            chose_X=raw_event.get("other") == x_peer,
            queue=detail.get("queue"), past_attempts=detail.get("past_attempts")))
    choice_fields = ("case", "seed", "peer_name", "peer_id", "step_id", "search_id", "event_ref",
                     "read_event_ref", "group_key", "leader_name", "leader_peer_id", "selected_expiration",
                     "own_expiration", "selection_top_wall", "x_expiration", "x_eligible", "chose_X",
                     "selection_valid", "queue", "past_attempts")
    _compare_rows(exports["choices"], trial_dir, raw_selection_records,
                  ("trial_dir", "event_ref"), choice_fields, errors)

    led_size_mean = (statistics.mean(len(row["roster"]) for row in x_groups) if x_groups else None)
    # One row per trial is the scalar projection; count and rate fields are
    # checked against raw attempts/assemblies plus the first-eligible CSV rows.
    summary_rows = []
    if exports["leadership_summary"].is_file():
        try:
            summary_rows = [r for r in _csv_rows(exports["leadership_summary"])
                            if r.get("trial_dir") == trial_dir]
        except (OSError, csv.Error) as exc:
            errors.append(f"cannot read leadership_summary.csv: {exc}")
    else:
        errors.append("missing export: leadership_summary.csv")
    if len(summary_rows) != 1:
        errors.append(f"leadership_summary.csv has {len(summary_rows)} rows for trial; expected 1")
    else:
        row = summary_rows[0]
        denominator, numerator = len(x_attempts), len(x_led_attempts)
        group_values = len(groups_by_id)
        summary_expected: dict[str, Any] = {
            "case": job["case"]["name"], "seed": job["seed"],
            "one_way_delay_seconds": job["case"]["one_way_delay_seconds"],
            "lifetime": job["case"]["lifetime"],
            "mode": job["case"]["mode"],
            "x_submitted_attempts": denominator, "x_led_attempts": numerator,
            "x_followed_attempts": sum(row["eventual_outcome"] == "followed" for row in x_attempts),
            "x_no_group_attempts": sum(row["eventual_outcome"] == "no_group" for row in x_attempts),
            "x_failed_attempts": sum(row["eventual_outcome"] == "failed" for row in x_attempts),
            "x_led_groups": len(x_groups), "total_groups": group_values,
            "x_led_group_mean_roster_size": led_size_mean,
            "first_eligible_choice_count": choice_count,
            "first_eligible_chose_x": chose_x,
            "first_eligible_choice_rate": chose_x / choice_count if choice_count else None,
            "x_led_group_share": len(x_groups) / group_values if group_values else None,
            "x_led_latency_successes": len(x_latencies),
            "x_led_latency_median_seconds": statistics.median(x_latencies) if x_latencies else None,
            "x_led_latency_p90_seconds": _percentile(x_latencies, .9),
            "integrity_pass": True,
        }
        if "frequency" in job["case"]:
            summary_expected["frequency"] = job["case"]["frequency"]
        summary_expected["x_led_rate"] = numerator / denominator if denominator else None
        for seconds in RATE_FIELDS:
            summary_expected[f"x_led_by_{seconds}s_count"] = x_early[str(seconds)]
            summary_expected[f"x_led_by_{seconds}s_rate"] = (
                x_early[str(seconds)] / denominator if denominator else None)
        for field, expected in summary_expected.items():
            if field not in row:
                errors.append(f"leadership_summary.csv missing required field {field}")
            elif not _same_csv_value(row[field], expected):
                errors.append(f"leadership_summary.csv {field} differs: csv={row[field]!r}, raw={expected!r}")

    # Verify delay instrumentation is present in the correct cells. Delayed
    # traffic can be bootstrap-only, so this records traffic classes rather
    # than asserting every trial must produce a leader join.
    rpc_starts = [event for event in events if event["kind"] == "control_rpc_start"]
    delay_events = [event for event in events if event["kind"] == "control_rpc_delay"]
    expected_delay = job["case"]["one_way_delay_seconds"]
    if expected_delay > 0 and not delay_events:
        errors.append("nonzero-delay trial contains no raw delay instrumentation")
    if any(not _same_number(event["detail"].get("requested_delay"), expected_delay)
           for event in delay_events):
        errors.append("a raw delay leg differs from configured one-way delay")
    rpc_types = sorted({event["detail"].get("rpc_type") for event in rpc_starts})

    result.update(
        total_groups=len(groups_by_id), x_led_groups=len(x_groups),
        submitted_x_attempts=len(x_attempts), x_led_attempts=len(x_led_attempts),
        x_followed_attempts=sum(row["eventual_outcome"] == "followed" for row in x_attempts),
        x_no_group_attempts=sum(row["eventual_outcome"] == "no_group" for row in x_attempts),
        x_failed_attempts=sum(row["eventual_outcome"] == "failed" for row in x_attempts),
        x_success_by_seconds=x_early, mean_x_led_group_size=led_size_mean,
        first_eligible_csv_rows=choice_count, first_eligible_chose_x=chose_x,
        control_rpc_count=len(rpc_starts), delay_leg_count=len(delay_events),
        observed_rpc_types=rpc_types,
        raw_events_sha256=sha256(events_path),
        completed_files={name: digest for name, digest in completion.get("file_sha256", {}).items()},
    )
    result["pass"] = not errors
    return result


def verify_preservation(path: Path) -> dict[str, Any]:
    """Read a pre-experiment before-hash list and record current state."""
    baseline = read_json(path)
    before = baseline.get("before_sha256")
    if not isinstance(before, dict) or not before:
        raise ValueError("preservation file must contain a nonempty before_sha256 map")
    rows = []
    for relative, expected in sorted(before.items()):
        target = ROOT / relative
        actual = sha256(target) if target.is_file() else None
        rows.append(dict(path=relative, before_sha256=expected, after_sha256=actual,
                         matches=actual == expected))
    return dict(path=str(path.resolve()), input_sha256=sha256(path),
                entries=len(rows), matches=sum(row["matches"] for row in rows),
                changed=[row for row in rows if not row["matches"]],
                after_sha256={row["path"]: row["after_sha256"] for row in rows})


def verify_suite(suite: Path, preservation: Path | None = None) -> dict[str, Any]:
    suite = suite.resolve()
    errors: list[str] = []
    provenance: dict[str, Any] = {}
    manifest: dict[str, Any] = {}
    checkpoint: dict[str, Any] = {}
    for filename, target in (("suite.json", manifest), ("provenance.json", provenance),
                             ("checkpoint.json", checkpoint)):
        try:
            target.update(read_json(suite / filename))
        except (OSError, json.JSONDecodeError) as exc:
            errors.append(f"missing/invalid {filename}: {exc}")
    if manifest.get("experiment") != "step26" or manifest.get("phase") != "pilot":
        errors.append("suite is not a Step 26 pilot suite")
    expected_jobs = canonical_jobs()
    if manifest.get("jobs") != expected_jobs:
        errors.append("suite jobs differ from the canonical 14-job randomized matrix")
    if checkpoint.get("suite_sha256") != sha256(suite / "suite.json"):
        errors.append("checkpoint suite hash differs")
    if checkpoint.get("provenance_sha256") != sha256(suite / "provenance.json"):
        errors.append("checkpoint provenance hash differs")
    checkpoint_jobs = {row.get("trial_dir"): row for row in checkpoint.get("jobs", [])}
    expected_names = {f"{job['case']['name']}_s{job['seed']}" for job in expected_jobs}
    if set(checkpoint_jobs) != expected_names:
        errors.append("checkpoint trial set differs from canonical 14 jobs")
    if any(row.get("status") != "completed" for row in checkpoint_jobs.values()):
        errors.append("checkpoint has jobs not marked completed")

    # The verifier is frozen into provenance alongside the harness/analyzer.
    this_sha = sha256(Path(__file__))
    frozen = provenance.get("harness_sha256", {})
    frozen_digest = frozen.get(Path(__file__).name)
    snapshot = suite / "harness" / Path(__file__).name
    if frozen_digest != this_sha:
        errors.append("verifier source hash is absent or differs from suite provenance")
    if not snapshot.is_file() or sha256(snapshot) != this_sha:
        errors.append("frozen verifier snapshot is missing or differs")
    for name, expected_hash in frozen.items():
        frozen_copy = suite / "harness" / name
        if not frozen_copy.is_file() or sha256(frozen_copy) != expected_hash:
            errors.append(f"frozen harness snapshot mismatch: {name}")
    source_root = Path(str(provenance.get("source", "")))
    core_root = ROOT / "hivemind"
    for relative, expected_hash in provenance.get("source_sha256", {}).items():
        candidates = (source_root / relative, core_root / relative)
        if not any(candidate.is_file() and sha256(candidate) == expected_hash
                   for candidate in candidates):
            errors.append(f"native source provenance mismatch: {relative}")
    for filename, expected_hash in provenance.get("runtime_files_sha256", {}).items():
        candidate = Path(filename)
        if not candidate.is_file() or sha256(candidate) != expected_hash:
            errors.append(f"runtime file provenance mismatch: {filename}")

    exports = {"leadership_summary": suite / "leadership_summary.csv",
               "groups": suite / "groups.csv", "attempts": suite / "attempts.csv",
               "choices": suite / "choices.csv"}
    trials = []
    for job in expected_jobs:
        trial_dir = f"{job['case']['name']}_s{job['seed']}"
        row = verify_trial(suite, trial_dir, job, exports)
        trials.append(row)
        checkpoint_row = checkpoint_jobs.get(trial_dir, {})
        checkpoint_hashes = checkpoint_row.get("file_sha256", {})
        for filename, expected_hash in checkpoint_hashes.items():
            target = suite / trial_dir / filename
            if not target.is_file() or sha256(target) != expected_hash:
                row["pass"] = False
                row.setdefault("errors", []).append(f"checkpoint file hash mismatch: {filename}")
        if not checkpoint_hashes:
            row["pass"] = False
            row.setdefault("errors", []).append("checkpoint has no completed file hashes")
    totals = {
        "trials": len(trials),
        "passed_trials": sum(row.get("pass") is True for row in trials),
        "total_groups": sum(row.get("total_groups", 0) for row in trials),
        "x_led_groups": sum(row.get("x_led_groups", 0) for row in trials),
        "submitted_x_attempts": sum(row.get("submitted_x_attempts", 0) for row in trials),
        "x_led_attempts": sum(row.get("x_led_attempts", 0) for row in trials),
        "first_eligible_csv_rows": sum(row.get("first_eligible_csv_rows", 0) for row in trials),
        "first_eligible_chose_x": sum(row.get("first_eligible_chose_x", 0) for row in trials),
    }
    preservation_result = None
    if preservation is not None:
        try:
            preservation_result = verify_preservation(preservation)
            if preservation_result["changed"]:
                errors.append(f"{len(preservation_result['changed'])} baseline preservation hashes changed")
        except (OSError, json.JSONDecodeError, ValueError) as exc:
            errors.append(f"preservation verification failed: {exc}")
    result = dict(
        experiment="step26", phase=manifest.get("phase"), suite=str(suite),
        **{"pass": not errors and all(row.get("pass") is True for row in trials)},
        errors=errors, trials=trials, totals=totals,
        provenance=dict(suite_sha256=sha256(suite / "suite.json") if (suite / "suite.json").is_file() else None,
                        provenance_sha256=sha256(suite / "provenance.json") if (suite / "provenance.json").is_file() else None,
                        checkpoint_sha256=sha256(suite / "checkpoint.json") if (suite / "checkpoint.json").is_file() else None,
                        verifier_sha256=this_sha,
                        source_sha256=provenance.get("source_sha256", {}),
                        harness_sha256=frozen),
        preservation=preservation_result,
        scope=("Raw group, attempt, and winner-expiration counts are reconstructed from events. "
               "Choice rows are verified against referenced native pop/read events, but the analyzer's "
               "first-eligible denominator is not independently recomputed from its queue/read-causality inference. "
               "A one-seed loopback pilot is exploratory and is not a deployment success-rate estimate."))
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", type=Path, required=True)
    parser.add_argument("--preservation", type=Path)
    args = parser.parse_args(argv)
    report = verify_suite(args.suite, args.preservation)
    output = args.suite.resolve() / "raw_verification.json"
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"Step 26 independent raw verification: {'PASS' if report['pass'] else 'FAIL'}; "
          f"{report['totals']['passed_trials']}/{report['totals']['trials']} trials; "
          f"{report['totals']['total_groups']} groups; "
          f"{report['totals']['x_led_groups']} X-led groups; report={output}")
    return 0 if report["pass"] else 1


if __name__ == "__main__":
    sys.exit(main())
