"""Independent validation and aggregation for the Step 24 lifetime sweep.

Only the Python standard library is required.  The analyzer works from the
saved config, raw JSONL trace, summary, suite manifest, and source snapshots.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
import hashlib
import json
import math
from pathlib import Path
import random
import statistics
import sys


NAMES = ["X"] + [f"H{i}" for i in range(7)]
TOLERANCE = 1e-6
TENSOR_ATOL = 1e-4
TENSOR_RTOL = 1e-5


def require(condition, message):
    if not condition:
        raise ValueError(message)


def read_json(path):
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read {path}: {exc}") from exc


def finite_number(value, label):
    require(isinstance(value, (int, float)) and not isinstance(value, bool)
            and math.isfinite(value), f"invalid {label}")
    return float(value)


def close(a, b, *, atol=TOLERANCE, rtol=0.0):
    return math.isclose(float(a), float(b), rel_tol=rtol, abs_tol=atol)


def _event_stream(path):
    try:
        lines = (path / "events.jsonl").read_text().splitlines()
    except OSError as exc:
        raise ValueError(f"cannot read {path / 'events.jsonl'}: {exc}") from exc
    require(bool(lines), "empty trace")
    events = []
    for number, line in enumerate(lines, 1):
        try:
            event = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid JSON on trace line {number}") from exc
        require(isinstance(event, dict), f"invalid event on trace line {number}")
        for field in ("kind", "peer", "other", "pid", "event_number", "monotonic", "wall_time", "detail"):
            require(field in event, f"event missing {field} on trace line {number}")
        require(isinstance(event["detail"], dict), f"invalid detail on trace line {number}")
        finite_number(event["monotonic"], "monotonic clock")
        finite_number(event["wall_time"], "wall clock")
        require(isinstance(event["event_number"], int), "invalid event number")
        events.append(event)
    require(events[-1]["kind"] == "trace_closed", "trace is not closed")
    require(sum(event["kind"] == "trace_closed" for event in events) == 1,
            "trace must contain exactly one closure event")

    per_process = defaultdict(list)
    for event in events:
        per_process[event["pid"]].append(event["event_number"])
    for sequence in per_process.values():
        require(all(right == left + 1 for left, right in zip(sequence, sequence[1:])),
                "missing, duplicate, or reordered per-process event number")
    return sorted(events, key=lambda e: (e["monotonic"], e["pid"], e["event_number"]))


def _member_key(data):
    name, sequence = data.get("name"), data.get("sequence")
    if not isinstance(name, str) or not isinstance(sequence, int) or isinstance(sequence, bool):
        return None
    return name, sequence


def _clean_detail(detail):
    # A few Hivemind tracing hooks attach search_id as transport context rather
    # than as part of the step outcome.  It is not copied into summary.json.
    return {key: value for key, value in detail.items() if key != "search_id"}


def _tensor_correct(actual, expected, tensor_size=None):
    if not isinstance(actual, list) or not actual or (tensor_size is not None and len(actual) != tensor_size):
        return False
    try:
        target = float(expected)
        return all(math.isfinite(float(value)) and
                   abs(float(value) - target) <= TENSOR_ATOL + TENSOR_RTOL * abs(target)
                   for value in actual)
    except (TypeError, ValueError, OverflowError):
        return False


def _normalise_outcomes(raw, summary):
    require(isinstance(summary, dict) and "outcomes" in summary, "summary missing outcomes")
    summary_rows = summary["outcomes"]
    require(isinstance(summary_rows, (list, dict)), "invalid summary outcomes")

    def keyed(rows, label):
        output = {}
        iterator = rows.items() if isinstance(rows, dict) else enumerate(rows)
        for outer_key, value in iterator:
            require(isinstance(value, dict), f"invalid {label} outcome")
            step_id = value.get("step_id")
            require(isinstance(step_id, str) and step_id, f"{label} outcome missing step_id")
            require(step_id not in output, f"duplicate {label} outcome {step_id}")
            if isinstance(rows, dict):
                require(outer_key in (step_id, value.get("name")),
                        f"{label} outcome key mismatch")
            output[step_id] = value
        return output

    raw_by_step = keyed(raw, "raw")
    summary_by_step = keyed(summary_rows, "summary")
    require(raw_by_step == summary_by_step, "summary outcomes do not exactly match raw step results")
    return raw_by_step


def _validate_training(config, events, starts, results, summary):
    training = config.get("training_delay")
    require(isinstance(training, dict), "missing training delay configuration")
    sigma = math.sqrt(math.log(1.36))
    mu = math.log(1.5) - sigma * sigma / 2
    expected_parameters = dict(mu=mu, sigma=sigma, lower=0.1, upper=4.0)
    for key, expected in expected_parameters.items():
        actual = finite_number(training.get(key), f"training_delay.{key}")
        require(close(actual, expected, atol=1e-12), f"training delay parameter mismatch: {key}")

    offsets = config.get("initial_offsets")
    require(isinstance(offsets, dict), "missing initial offsets")
    started = finite_number(config.get("started_monotonic"), "started_monotonic")
    summary_error = summary.get("error")
    for name in NAMES:
        require(name in offsets, f"missing initial offset for {name}")
        first = starts.get((name, 1))
        if first is None and summary_error:
            continue
        require(first is not None, f"missing initial step start for {name}")
        threshold = started + finite_number(offsets[name], f"initial offset for {name}")
        require(first["monotonic"] >= threshold - TOLERANCE,
                f"initial start precedes configured offset for {name}")

    schedules = {}
    rngs = {name: random.Random(config["seed"] * 100 + index)
            for index, name in enumerate(NAMES)}
    draw_counts = defaultdict(int)
    delay_rows = []
    for event in events:
        if event["kind"] != "training_scheduled":
            continue
        detail = event["detail"]
        name, sequence = detail.get("name"), detail.get("sequence")
        require(name in NAMES and isinstance(sequence, int) and not isinstance(sequence, bool)
                and sequence > 0, "invalid training schedule identity")
        step_id = f"{name}:{sequence}"
        require(detail.get("step_id") == step_id, "training schedule step id mismatch")
        require(step_id in results, "training scheduled before its step result")
        result_event = results[step_id]["event"]
        finished = finite_number(detail.get("finished"), "training finished time")
        delay = finite_number(detail.get("delay"), "training delay")
        ready = finite_number(detail.get("ready"), "training ready time")
        require(event["monotonic"] >= result_event["monotonic"] - TOLERANCE,
                "training scheduled before prior step result")
        require(close(finished, result_event["detail"].get("finished"), atol=1e-9),
                "training finished time differs from step result")
        require(close(ready, finished + delay, atol=1e-9),
                "training ready time does not equal finished plus delay")
        expected_delay = min(4.0, max(0.1, rngs[name].lognormvariate(mu, sigma)))
        require(close(delay, expected_delay, atol=1e-9),
                f"seeded training delay mismatch for {step_id}")
        require(step_id not in schedules, f"duplicate training schedule for {step_id}")
        schedules[step_id] = dict(name=name, sequence=sequence, finished=finished,
                                  delay=delay, ready=ready, event_monotonic=event["monotonic"])
        draw_counts[name] += 1
        delay_rows.append(dict(step_id=step_id, delay=delay, expected_delay=expected_delay,
                               ready=ready, finished=finished))

    scheduler_samples = []
    for (name, sequence), event in starts.items():
        if sequence == 1:
            continue
        preceding_id = f"{name}:{sequence - 1}"
        schedule = schedules.get(preceding_id)
        require(schedule is not None, f"next step lacks training schedule after {preceding_id}")
        require(event["monotonic"] >= schedule["ready"] - TOLERANCE,
                f"{name}:{sequence} started before training delay elapsed")
        prior_result = results.get(preceding_id)
        require(prior_result is not None, f"next step started before {preceding_id} result")
        scheduler_samples.append(dict(step_id=f"{name}:{sequence}", previous_step=preceding_id,
                                      finished=schedule["finished"], delay=schedule["delay"],
                                      ready=schedule["ready"], actual_start=event["monotonic"],
                                      scheduler_lateness=event["monotonic"]-schedule["ready"]))
    return dict(schedules=delay_rows, delay_count=len(delay_rows),
                mean_delay=statistics.mean(row["delay"] for row in delay_rows) if delay_rows else None,
                per_peer_schedule_counts=dict(draw_counts), scheduler_samples=scheduler_samples,
                mean_scheduler_lateness=(statistics.mean(row["scheduler_lateness"] for row in scheduler_samples)
                                         if scheduler_samples else None))


def _validate_step_events(config, events, summary, outcome="averaging"):
    ids = config.get("peer_ids")
    require(isinstance(ids, dict) and set(ids) == set(NAMES), "peer_ids must map X and H0..H6")
    require(len(set(ids.values())) == len(NAMES), "peer ids are not unique")
    names_by_peer = {peer: name for name, peer in ids.items()}
    starts, results, result_events = {}, {}, {}
    raw_rows = {}
    pending = summary.get("pending", [])
    require(isinstance(pending, list), "summary pending must be a list")
    pending_ids = set(pending)
    require(len(pending_ids) == len(pending) and all(isinstance(row, str) for row in pending),
            "invalid or duplicate pending step ids")

    for event in events:
        kind, detail = event["kind"], event["detail"]
        if kind == "step_start":
            name, sequence, step_id = detail.get("name"), detail.get("sequence"), detail.get("step_id")
            require(name in NAMES and event["peer"] == ids[name], "step start identity mismatch")
            require(isinstance(sequence, int) and not isinstance(sequence, bool) and sequence > 0,
                    "invalid step sequence")
            require(step_id == f"{name}:{sequence}", "invalid step start id")
            key = (name, sequence)
            require(key not in starts, f"duplicate step start {step_id}")
            require(isinstance(detail.get("value"), (int, float)) and math.isfinite(detail["value"]),
                    f"invalid input value for {step_id}")
            previous = max((seq for peer_name, seq in starts if peer_name == name), default=0)
            require(sequence == previous + 1, f"nonconsecutive step sequence for {name}")
            if sequence > 1:
                previous_id = f"{name}:{sequence - 1}"
                require(previous_id in results, f"step started before prior result {previous_id}")
            starts[key] = event
        elif kind == "step_result":
            name, sequence, step_id = detail.get("name"), detail.get("sequence"), detail.get("step_id")
            require(name in NAMES and event["peer"] == ids[name], "step result identity mismatch")
            require(isinstance(sequence, int) and not isinstance(sequence, bool) and sequence > 0,
                    "invalid result sequence")
            require(step_id == f"{name}:{sequence}", "step result id mismatch")
            require((name, sequence) in starts and step_id not in results,
                    f"result without unique matching start: {step_id}")
            require(event["monotonic"] >= starts[(name, sequence)]["monotonic"],
                    f"step result precedes start: {step_id}")
            require(detail.get("status") in ("completed", "failed", "cancelled", "no_group"),
                    f"invalid outcome status for {step_id}")
            if detail["status"] == "completed" and outcome == "averaging":
                require(isinstance(detail.get("members"), dict), f"completed step missing members: {step_id}")
                require("expected" in detail and isinstance(detail.get("actual"), list),
                        f"completed step missing tensor evidence: {step_id}")
                require(len(detail["actual"]) == config.get("tensor_size"),
                        f"wrong tensor size for {step_id}")
                require(isinstance(detail.get("correct"), bool), f"completed step missing correct flag: {step_id}")
            results[step_id] = detail
            result_events[step_id] = dict(event=event, detail=detail)
            raw_rows[step_id] = _clean_detail(detail)

    for step_id in pending_ids:
        require(step_id.count(":") == 1, f"invalid pending step id: {step_id}")
        name, seq_text = step_id.split(":", 1)
        require(name in NAMES and seq_text.isdigit() and (name, int(seq_text)) in starts,
                f"pending step has no start: {step_id}")
        require(step_id not in results, f"pending step already has a result: {step_id}")
    missing = {event["detail"]["step_id"] for (name, seq), event in starts.items()
               if f"{name}:{seq}" not in results and f"{name}:{seq}" not in pending_ids}
    require(not missing, f"started steps have neither result nor pending record: {sorted(missing)}")

    outcome_rows = _normalise_outcomes(raw_rows, summary)
    return ids, names_by_peer, starts, results, result_events, pending_ids, outcome_rows


def _assembly_analysis(events, ids, names_by_peer, starts, results, result_events, tensor_size):
    integrity = []
    assembly_rows = defaultdict(list)
    allreduce_starts = defaultdict(list)
    allreduce_ends = defaultdict(list)
    for event in events:
        if event["kind"] == "assembled":
            detail = event["detail"]
            group_id = detail.get("group_id")
            if not isinstance(group_id, str) or not group_id:
                integrity.append("assembled event missing group_id")
                continue
            assembly_rows[group_id].append(event)
        elif event["kind"] == "allreduce_start":
            group_id = event["detail"].get("group_id")
            step_id = event["detail"].get("step_id")
            if not isinstance(group_id, str) or not isinstance(step_id, str):
                integrity.append("allreduce_start missing group or step id")
                continue
            allreduce_starts[(group_id, step_id)].append(event)
        elif event["kind"] == "allreduce_end":
            group_id = event["detail"].get("group_id")
            step_id = event["detail"].get("step_id")
            if not isinstance(group_id, str) or not isinstance(step_id, str):
                integrity.append("allreduce_end missing group or step id")
                continue
            allreduce_ends[(group_id, step_id)].append(event)

    groups = []
    membership_groups = defaultdict(list)
    group_collective_success = {}
    incorrect_steps = []
    for group_id, events_for_id in sorted(assembly_rows.items()):
        leader_peer = events_for_id[0]["peer"]
        first_detail = events_for_id[0]["detail"]
        first_signature = (leader_peer, tuple(sorted(first_detail.get("roster", []))),
                           first_detail.get("user"), first_detail.get("step_id"))
        for event in events_for_id[1:]:
            detail = event["detail"]
            signature = (event["peer"], tuple(sorted(detail.get("roster", []))), detail.get("user"))
            signature += (detail.get("step_id"),)
            if signature != first_signature:
                integrity.append(f"conflicting duplicate assembly id {group_id}")
        leader_name = names_by_peer.get(leader_peer)
        roster = first_detail.get("roster")
        user = first_detail.get("user")
        if leader_name is None:
            integrity.append(f"assembly {group_id} has unknown leader")
        if not isinstance(roster, list) or len(set(roster)) != len(roster) or not roster:
            integrity.append(f"assembly {group_id} has invalid roster")
            roster = []
        unknown_peers = set(roster) - set(names_by_peer)
        if unknown_peers:
            integrity.append(f"assembly {group_id} roster has unknown peers")
        if leader_peer not in roster:
            integrity.append(f"assembly {group_id} leader is absent from roster")
        if not isinstance(user, dict):
            integrity.append(f"assembly {group_id} missing leader user data")
            user = {}

        user_key = _member_key(user)
        user_start = starts.get(user_key) if user_key else None
        if user_start is None:
            integrity.append(f"assembly {group_id} user has no matching logged step start")
        else:
            expected_user = user_start["detail"]
            if (event_value := user.get("value")) != expected_user.get("value"):
                integrity.append(f"assembly {group_id} leader input conflicts with step start")
            if user.get("step_id") != expected_user.get("step_id") or user.get("name") != leader_name:
                integrity.append(f"assembly {group_id} leader identity conflicts with step start")

        roster_set = set(roster)

        # Native retries can assemble a candidate roster, fail its allreduce,
        # and assemble another roster for the same step.  Prefer the explicit
        # allreduce-start record for this group; the leader's final result is a
        # fallback for older/minimal traces without allreduce instrumentation.
        group_starts = [event for (candidate_group, _), rows in allreduce_starts.items()
                        if candidate_group == group_id for event in rows]
        member_map = None
        assembled_member_map = first_detail.get("members")
        if assembled_member_map is not None and not isinstance(assembled_member_map, dict):
            integrity.append(f"assembly {group_id} has malformed gathered member map")
            assembled_member_map = None
        if group_starts:
            maps = [event["detail"].get("members") for event in group_starts]
            valid_maps = [value for value in maps if isinstance(value, dict)]
            if len(valid_maps) != len(group_starts):
                integrity.append(f"allreduce group {group_id} has malformed member map")
            if valid_maps:
                member_map = valid_maps[0]
                if any(value != member_map for value in valid_maps[1:]):
                    integrity.append(f"allreduce group {group_id} has conflicting member maps")
            if assembled_member_map is not None and assembled_member_map != member_map:
                integrity.append(f"assembly {group_id} gathered map conflicts with allreduce inputs")
            if set(member_map or {}) != roster_set:
                integrity.append(f"assembly {group_id} roster conflicts with allreduce member map")
        else:
            assembled_members = first_detail.get("members")
            if isinstance(assembled_members, dict):
                member_map = assembled_members
            else:
                leader_step = user.get("step_id")
                leader_result = results.get(leader_step) if isinstance(leader_step, str) else None
                if leader_result and leader_result.get("status") == "completed" and isinstance(leader_result.get("members"), dict):
                    leader_members = leader_result["members"]
                    if set(leader_members) == roster_set:
                        member_map = leader_members
            if member_map is not None and set(member_map) != roster_set:
                integrity.append(f"assembly {group_id} roster conflicts with assembled member map")

        normalized_members = {}
        if member_map is not None:
            for peer, data in member_map.items():
                expected_name = names_by_peer.get(peer)
                key = _member_key(data) if isinstance(data, dict) else None
                if expected_name is None or key is None:
                    integrity.append(f"assembly {group_id} contains malformed member identity")
                    continue
                name, sequence = key
                member_id = f"{name}:{sequence}"
                expected_start = starts.get((name, sequence))
                if name != expected_name or expected_start is None:
                    integrity.append(f"assembly {group_id} member identity conflicts with step start")
                    continue
                expected_data = expected_start["detail"]
                if data.get("step_id") != expected_data.get("step_id") or data.get("value") != expected_data.get("value"):
                    integrity.append(f"assembly {group_id} member input conflicts with step start")
                if member_id in normalized_members.values():
                    integrity.append(f"assembly {group_id} repeats a member step")
                normalized_members[peer] = member_id

        group_member_ids = set(normalized_members.values())
        for member_id in group_member_ids:
            membership_groups[member_id].append(group_id)

        values = []
        all_outputs_good = member_map is not None and set(normalized_members) == roster_set
        if member_map is not None:
            values = [float(data["value"]) for data in member_map.values()
                      if isinstance(data, dict) and isinstance(data.get("value"), (int, float))]
        expected_mean = statistics.mean(values) if values and len(values) == len(member_map or {}) else None
        member_step_ids = set(normalized_members.values())

        # Validate each native allreduce crosscheck against the exact local
        # step input and output.  Candidate groups with a recorded collective
        # error remain assembled but cannot be verified.
        group_allreduce_good = True
        for (candidate_group, step_id), start_events in allreduce_starts.items():
            if candidate_group != group_id:
                continue
            for start_event in start_events:
                start_detail = start_event["detail"]
                peer_name = names_by_peer.get(start_event["peer"])
                member = (member_map or {}).get(start_event["peer"])
                if member is None or not isinstance(member, dict):
                    integrity.append(f"allreduce input peer absent from group map for {group_id}")
                    group_allreduce_good = False
                    continue
                member_key = _member_key(member)
                if member_key is None or step_id != member.get("step_id"):
                    integrity.append(f"allreduce step identity conflicts with member map for {group_id}")
                    group_allreduce_good = False
                    continue
                own_start = starts.get(member_key)
                if peer_name != member.get("name") or own_start is None:
                    integrity.append(f"allreduce peer identity conflicts with step start for {step_id}")
                    group_allreduce_good = False
                    continue
                actual_input = start_detail.get("actual_input")
                if not _tensor_correct(actual_input, own_start["detail"].get("value"), tensor_size):
                    integrity.append(f"allreduce input tensor conflicts with logged value for {step_id}")
                    group_allreduce_good = False
                end_events = allreduce_ends.get((group_id, step_id), [])
                matching_ends = [end for end in end_events if end["peer"] == start_event["peer"]]
                if len(matching_ends) != 1:
                    group_allreduce_good = False
                    continue
                end_event = matching_ends[0]
                if end_event["monotonic"] < start_event["monotonic"]:
                    integrity.append(f"allreduce end precedes start for {step_id}")
                    group_allreduce_good = False
                    continue
                end_detail = end_event["detail"]
                if end_detail.get("error") is not None:
                    group_allreduce_good = False
                    continue
                if expected_mean is not None and not _tensor_correct(
                        end_detail.get("actual_output"), expected_mean, tensor_size):
                    incorrect_steps.append(step_id)
                    group_allreduce_good = False

        if group_starts:
            participants = [event["peer"] for event in group_starts]
            if len(participants) != len(set(participants)):
                integrity.append(f"allreduce group {group_id} has duplicate peer starts")
                group_allreduce_good = False
            if set(participants) != roster_set:
                group_allreduce_good = False
        else:
            # Backward-compatible evidence mode: result rows can verify a
            # roster when this trace predates allreduce crosscheck events.
            group_allreduce_good = True
        group_collective_success[group_id] = bool(group_starts and group_allreduce_good)

        for peer, step_id in normalized_members.items():
            outcome = results.get(step_id)
            if outcome is None or outcome.get("status") != "completed":
                all_outputs_good = False
                continue
            if outcome.get("members") != member_map:
                # A prior candidate group may have failed and this same step
                # may have completed later under a different group.  Its final
                # tensor belongs to that later roster and cannot verify or
                # invalidate this candidate.
                all_outputs_good = False
                continue
            computed_correct = (expected_mean is not None and
                                _tensor_correct(outcome.get("actual"), expected_mean, tensor_size))
            if not computed_correct:
                incorrect_steps.append(step_id)
                all_outputs_good = False
            if outcome.get("correct") is not computed_correct:
                integrity.append(f"correct flag conflicts with tensor for {step_id}")
                all_outputs_good = False
            if expected_mean is not None:
                try:
                    if not close(outcome.get("expected"), expected_mean, atol=TENSOR_ATOL, rtol=TENSOR_RTOL):
                        integrity.append(f"expected mean conflicts with member inputs for {step_id}")
                        all_outputs_good = False
                except (TypeError, ValueError):
                    integrity.append(f"invalid expected mean for {step_id}")
                    all_outputs_good = False

        # Check matching outcome rows against exact contributor identities.
        # The same peer roster can appear in later rounds with new step IDs.
        for step_id, outcome in results.items():
            members = outcome.get("members")
            if outcome.get("status") == "completed" and isinstance(members, dict) and set(members) == roster_set:
                result_member_steps = {
                    peer: data.get("step_id") for peer, data in members.items()
                    if isinstance(data, dict)
                }
                group_member_steps = {
                    peer: data.get("step_id") for peer, data in (member_map or {}).items()
                    if isinstance(data, dict)
                }
                if result_member_steps == group_member_steps and members != member_map:
                    integrity.append(f"same-roster result {step_id} conflicts with assembly {group_id}")
                    all_outputs_good = False

        verified = bool(member_map is not None and roster_set and group_allreduce_good and
                        set(member_map) == roster_set and len(member_step_ids) == len(roster_set)
                        and all_outputs_good and expected_mean is not None)
        groups.append(dict(group_id=group_id, leader=leader_name, leader_peer=leader_peer,
                           roster=sorted(roster), size=len(roster), verified=verified,
                           allreduce_pass=group_collective_success[group_id],
                           member_steps=sorted(member_step_ids), expected_mean=expected_mean))

    # A repeated step membership is expected when a failed group is followed by
    # a sequential retry.  Flag overlap only when two native collectives for the
    # same step were simultaneously active, or both groups demonstrably
    # completed correctly using that step.
    by_member = defaultdict(set)
    for member_id, group_ids in membership_groups.items():
        by_member[member_id].update(group_ids)
    group_lookup = {row["group_id"]: row for row in groups}
    for member_id, group_ids in by_member.items():
        if len(group_ids) < 2:
            continue
        successful = [group_id for group_id in group_ids if group_lookup.get(group_id, {}).get("verified")]
        if len(successful) > 1:
            integrity.append(f"step {member_id} appears in multiple verified groups {sorted(successful)}")
        successful_collectives = [group_id for group_id in group_ids if group_collective_success.get(group_id)]
        if len(successful_collectives) > 1:
            integrity.append(f"step {member_id} appears in multiple successful allreduces {sorted(successful_collectives)}")
        intervals = []
        for group_id in group_ids:
            for (candidate_group, step_id), rows in allreduce_starts.items():
                if candidate_group != group_id or step_id != member_id:
                    continue
                ends = allreduce_ends.get((group_id, step_id), [])
                for start_event in rows:
                    end = next((event for event in ends if event["peer"] == start_event["peer"]), None)
                    if end is not None:
                        intervals.append((start_event["monotonic"], end["monotonic"], group_id))
        intervals.sort()
        for left, right in zip(intervals, intervals[1:]):
            if left[2] != right[2] and right[0] < left[1] - TOLERANCE:
                integrity.append(f"step {member_id} had simultaneous allreduces in groups {left[2]} and {right[2]}")

    # Completed results with tensor claims still count as incorrect even when
    # their group could not be connected to a surviving assembly event.
    for step_id, outcome in results.items():
        if outcome.get("status") != "completed":
            continue
        members = outcome.get("members", {})
        values = [data.get("value") for data in members.values()
                  if isinstance(data, dict) and isinstance(data.get("value"), (int, float))]
        expected_mean = statistics.mean(values) if values and len(values) == len(members) else outcome.get("expected")
        if not _tensor_correct(outcome.get("actual"), expected_mean, tensor_size):
            incorrect_steps.append(step_id)
        if outcome.get("correct") is True and not _tensor_correct(
                outcome.get("actual"), expected_mean, tensor_size):
            integrity.append(f"false correct flag for {step_id}")
    return groups, sorted(set(incorrect_steps)), sorted(set(integrity))


def _control_assembly_analysis(events, ids, names_by_peer, starts, config):
    """Count unique native assembly IDs and validate control-plane identity."""
    rows_by_id = defaultdict(list)
    errors = []
    for event in events:
        if event["kind"] != "assembled":
            continue
        group_id = event["detail"].get("group_id")
        if not isinstance(group_id, str) or not group_id:
            errors.append("assembled event missing group_id")
            continue
        rows_by_id[group_id].append(event)

    case = config.get("case", {})
    minimum = case.get("minimum", 4) if isinstance(case, dict) else 4
    target = case.get("target", 4) if isinstance(case, dict) else 4
    if (not isinstance(minimum, int) or isinstance(minimum, bool) or minimum < 1 or
            not isinstance(target, int) or isinstance(target, bool) or target < minimum):
        errors.append("invalid configured group size bounds")
        minimum, target = 4, 4

    groups = []
    for group_id, rows in sorted(rows_by_id.items()):
        first = rows[0]
        detail = first["detail"]
        leader_peer = first["peer"]
        leader = names_by_peer.get(leader_peer)
        for duplicate in rows[1:]:
            if duplicate["peer"] != leader_peer or duplicate["detail"] != detail:
                errors.append(f"conflicting duplicate assembly id {group_id}")

        roster = detail.get("roster")
        valid_roster = isinstance(roster, list) and all(isinstance(peer, str) for peer in roster)
        if not valid_roster:
            errors.append(f"assembly {group_id} has invalid roster")
            roster = []
        else:
            if len(set(roster)) != len(roster):
                errors.append(f"assembly {group_id} has duplicate roster peers")
            if set(roster) - set(names_by_peer):
                errors.append(f"assembly {group_id} roster has unknown peers")
        if leader is None:
            errors.append(f"assembly {group_id} has unknown leader")
        if leader_peer not in roster:
            errors.append(f"assembly {group_id} leader is absent from roster")

        user = detail.get("user")
        if not isinstance(user, dict):
            errors.append(f"assembly {group_id} missing leader user data")
            user = {}
        user_key = _member_key(user)
        own_start = starts.get(user_key) if user_key else None
        if own_start is None:
            errors.append(f"assembly {group_id} user has no matching logged step start")
        else:
            expected = own_start["detail"]
            if (leader is None or user.get("name") != leader or
                    user.get("step_id") != expected.get("step_id") or
                    detail.get("step_id") != expected.get("step_id") or
                    user.get("value") != expected.get("value")):
                errors.append(f"assembly {group_id} leader identity conflicts with step start")
        if leader == "X" and (len(roster) < minimum or len(roster) > target):
            errors.append(f"X-led assembly {group_id} size {len(roster)} outside {minimum}..{target}")
        groups.append(dict(group_id=group_id, leader=leader, leader_peer=leader_peer,
                           roster=sorted(roster), size=len(roster)))
    return groups, sorted(set(errors))


def _forbidden_action_errors(events, ids, config):
    """The non-stock arms keep X from following competitor leaders."""
    case = config.get("case", {})
    mode = case.get("mode") if isinstance(case, dict) else None
    if mode == "stock":
        return []
    x_id = ids["X"]
    errors = []
    for event in events:
        if ((event["kind"] == "native_client_wait" and event["peer"] == x_id) or
                (event["kind"] == "join_received" and event["other"] == x_id)):
            errors.append(f"forbidden outbound join by X in {mode or 'unknown'} mode")
    return sorted(set(errors))


def _holder_timing(config, events, ids, names_by_peer):
    x_id = ids["X"]
    started_wall = finite_number(config.get("started_wall"), "started_wall")
    result_walls = [event["wall_time"] for event in events if event["kind"] == "step_result"]
    end_wall = max(result_walls) if result_walls else started_wall

    received = defaultdict(list)
    for event in events:
        if event["kind"] != "holder_store":
            continue
        detail = event["detail"]
        if detail.get("subkey") != x_id:
            continue
        key = str(detail.get("key", "")).lower().removeprefix("0x")
        if detail.get("accepted") is not True:
            continue
        time_received = finite_number(detail.get("received"), "holder received time")
        expiration = finite_number(detail.get("expiration"), "holder expiration")
        holder = event["peer"]
        active = detail.get("active") is True
        received[(holder, key)].append(dict(received=time_received, expiration=expiration,
                                             active=active, step_id=detail.get("step_id")))

    intervals = []
    gaps = []
    by_holder = []
    for (holder, key), rows in sorted(received.items()):
        rows.sort(key=lambda row: (row["received"], row["expiration"]))
        raw_intervals = []
        for row in rows:
            if row["active"]:
                raw_intervals.append([row["received"], row["expiration"]])
            else:
                for interval in raw_intervals:
                    if interval[0] <= row["received"] < interval[1]:
                        interval[1] = row["received"]
        accepted = [(max(started_wall, lo), min(end_wall, hi))
                    for lo, hi in raw_intervals
                    if min(end_wall, hi) > max(started_wall, lo)]
        merged = []
        for lo, hi in sorted(accepted):
            if merged and lo <= merged[-1][1]:
                merged[-1] = (merged[-1][0], max(merged[-1][1], hi))
            else:
                merged.append((lo, hi))
        local_gaps = []
        for (_, previous_end), (next_start, _) in zip(merged, merged[1:]):
            if next_start > previous_end:
                row = dict(holder=holder, key=key, start=previous_end, end=next_start,
                           duration=next_start-previous_end,
                           interpretation="gap between logged accepted holder records; not observed DHT visibility")
                local_gaps.append(row)
                gaps.append(row)
        intervals.extend(dict(holder=holder, key=key, start=lo, end=hi, duration=hi-lo)
                         for lo, hi in merged)
        by_holder.append(dict(holder=holder, key=key, logged_accepted_intervals=len(merged),
                              intervals=[dict(start=lo, end=hi) for lo, hi in merged],
                              logged_gaps=local_gaps))
    return dict(source="accepted holder_store records; not observed DHT visibility",
                clip_window=dict(start=started_wall, end=end_wall),
                interpretation="logged receipt intervals and their internal gaps are diagnostics, not DHT visibility or contention measurements",
                intervals=intervals, gaps=gaps, by_holder=by_holder)


def analyse_trial(path) -> dict:
    """Validate a Step 24 trial and return reconstructed group and timing data."""
    path = Path(path)
    require(not (path / "invalid.json").exists(), f"invalid trial marker: {path}")
    config, summary = read_json(path / "config.json"), read_json(path / "summary.json")
    require(isinstance(config, dict) and isinstance(summary, dict), "invalid trial config or summary")
    outcome = config.get("outcome", "averaging")
    require(outcome in ("averaging", "grouping"), f"invalid trial outcome: {outcome}")
    events = _event_stream(path)
    ids, names_by_peer, starts, results, result_events, pending, _ = _validate_step_events(
        config, events, summary, outcome=outcome)
    training_timing = _validate_training(config, events, starts, result_events, summary)

    for event in events:
        if event["kind"] != "native_leader_wait":
            continue
        detail = event["detail"]
        captured = finite_number(detail.get("captured_expiration"), "captured leader expiration")
        timeout = finite_number(detail.get("timeout"), "native leader timeout")
        timer_started = finite_number(detail.get("timer_started_wall"), "leader timer start")
        expected = max(0.0, captured - timer_started)
        require(close(timeout, expected, atol=0.005), "native leader timeout differs from captured expiration")

    closed_detail = events[-1]["detail"]
    summary_pending = summary.get("pending", [])
    summary_error = summary.get("error")
    require(closed_detail.get("pending", []) == summary_pending,
            "trace closure pending list differs from summary")
    require(closed_detail.get("error") == summary_error,
            "trace closure error differs from summary")
    if not summary_error:
        require(sorted(closed_detail.get("participants", [])) == sorted(NAMES),
                "trace closure participants differ from configured peers")

    groups, incorrect_steps, integrity_errors = _assembly_analysis(
        events, ids, names_by_peer, starts, results, result_events, config["tensor_size"])
    control_groups, control_errors = _control_assembly_analysis(events, ids, names_by_peer, starts, config)
    control_errors.extend(_forbidden_action_errors(events, ids, config))
    control_errors = sorted(set(control_errors))
    assembled_count = len(control_groups)
    x_assembled = sum(group["leader"] == "X" for group in control_groups)
    verified = [group for group in groups if group["verified"]]
    x_verified = [group for group in verified if group["leader"] == "X"]
    total_verified = len(verified)
    x_share = len(x_verified) / total_verified if total_verified else 0.0

    failed_steps = sorted(step_id for step_id, row in results.items()
                          if row.get("status") != "completed")
    x_id = ids["X"]
    active_ad_attempts = sum(event["kind"] == "ad_started" and event["peer"] == x_id
                             and event["detail"].get("active") is True for event in events)
    x_ad_writes = sum(event["kind"] == "ad_finished" and event["peer"] == x_id
                      and event["detail"].get("active") is True
                      and event["detail"].get("accepted") is True for event in events)
    disband_replies = sum(event["kind"] == "join_reply" and event["peer"] == x_id
                          and event["detail"].get("code") == "GROUP_DISBANDED" for event in events)
    timer_samples = []
    for event in events:
        if event["kind"] != "native_leader_wait":
            continue
        detail = event["detail"]
        timer_samples.append(dict(step_id=detail.get("step_id"), leader=names_by_peer.get(event["peer"]),
                                  follower=names_by_peer.get(event["other"]), timeout=detail["timeout"],
                                  captured_expiration=detail["captured_expiration"],
                                  timer_started_wall=detail["timer_started_wall"],
                                  followers=len(detail.get("followers", []))))
    joins = [dict(leader=names_by_peer.get(event["peer"]), follower=names_by_peer.get(event["other"]),
                  step_id=event["detail"].get("step_id"), monotonic=event["monotonic"],
                  wall_time=event["wall_time"], request_expiration=event["detail"].get("expiration"))
             for event in events if event["kind"] == "join_received"]

    averaging_pass = not (incorrect_steps or integrity_errors or pending or summary.get("error"))
    control_plane_pass = not (control_errors or pending or summary.get("error"))
    assembled_x = [group for group in control_groups if group["leader"] == "X"]
    assembled_x_size = (statistics.mean(group["size"] for group in assembled_x)
                        if assembled_x else None)
    return dict(case=config["case"]["name"], policy=config["case"], seed=config["seed"],
                duration=config["duration"], source=str(path), outcome=outcome,
                total_assembled_groups=assembled_count, x_assembled_leads=x_assembled,
                assembled_x_leadership_share=x_assembled / assembled_count if assembled_count else 0.0,
                mean_x_assembled_group_size=assembled_x_size,
                total_verified_groups=total_verified, x_verified_leads=len(x_verified),
                x_leadership_share=x_share,
                mean_x_verified_group_size=(statistics.mean(group["size"] for group in x_verified)
                                            if x_verified else None),
                x_ad_writes=x_ad_writes, x_ad_attempts=active_ad_attempts,
                x_ad_write_attempts=active_ad_attempts,
                disband_replies=disband_replies, incorrect_steps=incorrect_steps,
                failed_steps=failed_steps, pending=sorted(pending), integrity_errors=integrity_errors,
                control_integrity_errors=control_errors, averaging_pass=averaging_pass,
                control_plane_pass=control_plane_pass,
                error=summary.get("error"),
                groups=groups, control_groups=control_groups,
                timing=dict(training=training_timing,
                            native_leader_waits=timer_samples,
                            join_arrivals=joins,
                            holder_records=_holder_timing(config, events, ids, names_by_peer)))


def _job_key(case, seed, duration):
    return json.dumps(dict(case=case, seed=seed, duration=duration), sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


def _discover_trial_dirs(path):
    output = []
    for config_path in path.rglob("config.json"):
        parent = config_path.parent
        if parent == path or "harness" in parent.relative_to(path).parts:
            continue
        output.append(parent)
    return sorted(output)


def _aggregate_trials(trials):
    grouped = defaultdict(list)
    for trial in trials:
        grouped[trial["case"]].append(trial)
    aggregate = []
    for case, rows in sorted(grouped.items()):
        total_verified = sum(row["total_verified_groups"] for row in rows)
        x_verified = sum(row["x_verified_leads"] for row in rows)
        total_assembled = sum(row["total_assembled_groups"] for row in rows)
        x_assembled = sum(row["x_assembled_leads"] for row in rows)
        x_groups = [group for row in rows for group in row["groups"]
                    if group["verified"] and group["leader"] == "X"]
        assembled_x_groups = [group for row in rows for group in row.get("control_groups", [])
                              if group["leader"] == "X"]
        diagnostics = [dict(seed=row["seed"], failed_steps=list(row["failed_steps"]),
                            failed_steps_count=len(row["failed_steps"]),
                            incorrect_steps=list(row["incorrect_steps"]),
                            incorrect_steps_count=len(row["incorrect_steps"]),
                            integrity_errors=list(row["integrity_errors"]),
                            integrity_error_count=len(row["integrity_errors"]),
                            control_integrity_errors=list(row.get("control_integrity_errors", [])),
                            control_integrity_error_count=len(row.get("control_integrity_errors", [])),
                            pending=list(row["pending"]), pending_count=len(row["pending"]))
                           for row in rows]
        aggregate.append(dict(
            case=case,
            trials=len(rows),
            total_assembled_groups=total_assembled,
            x_assembled_leads=x_assembled,
            assembled_x_leadership_share=x_assembled / total_assembled if total_assembled else 0.0,
            total_verified_groups=total_verified,
            x_verified_leads=x_verified,
            x_leadership_share=x_verified / total_verified if total_verified else 0.0,
            verified_x_leadership_share=x_verified / total_verified if total_verified else 0.0,
            mean_x_assembled_group_size=(statistics.mean(group["size"] for group in assembled_x_groups)
                                         if assembled_x_groups else None),
            mean_x_verified_group_size=(statistics.mean(group["size"] for group in x_groups)
                                        if x_groups else None),
            x_ad_writes=sum(row["x_ad_writes"] for row in rows),
            x_ad_attempts=sum(row["x_ad_attempts"] for row in rows),
            disband_replies=sum(row["disband_replies"] for row in rows),
            incorrect_steps=sorted({step for row in rows for step in row["incorrect_steps"]}),
            integrity_errors=sorted({error for row in rows for error in row["integrity_errors"]}),
            control_integrity_errors=sorted({error for row in rows for error in row.get("control_integrity_errors", [])}),
            pending=sum(len(row["pending"]) for row in rows),
            diagnostics_per_seed=diagnostics,
        ))
    return aggregate


def analyse_suite(path, require_complete=True) -> dict:
    """Validate a sweep suite, optionally accepting missing trial directories."""
    path = Path(path)
    suite = read_json(path / "suite.json")
    outcome = suite.get("outcome", "averaging")
    require(outcome in ("averaging", "grouping"), f"invalid suite outcome: {outcome}")
    provenance_path = path / "provenance.json"
    provenance = read_json(provenance_path) if provenance_path.exists() else suite.get("provenance")
    require(isinstance(provenance, dict) and isinstance(provenance.get("harness_sha256"), dict)
            and bool(provenance["harness_sha256"]),
            "suite missing harness provenance")
    for name, digest in provenance["harness_sha256"].items():
        snapshot = path / "harness" / name
        try:
            actual = hashlib.sha256(snapshot.read_bytes()).hexdigest()
        except OSError as exc:
            raise ValueError(f"missing harness snapshot {name}") from exc
        require(actual == digest, f"harness snapshot mismatch: {name}")

    jobs = suite.get("jobs")
    require(isinstance(jobs, list), "suite jobs must be a list")
    expected = {}
    for job in jobs:
        require(isinstance(job, dict) and isinstance(job.get("case"), dict), "invalid suite job")
        if "outcome" in job:
            require(job["outcome"] == outcome, "suite job outcome differs from suite outcome")
        key = _job_key(job["case"], job.get("seed"), job.get("duration"))
        require(key not in expected, "duplicate suite job")
        expected[key] = job

    discovered = _discover_trial_dirs(path)
    trials, discovered_keys, incomplete_dirs = [], set(), []
    for trial_path in discovered:
        config_path = trial_path / "config.json"
        config = read_json(config_path)
        trial_outcome = config.get("outcome", "averaging")
        require(trial_outcome == outcome,
                f"trial outcome differs from suite outcome: {trial_path}")
        key = _job_key(config.get("case"), config.get("seed"), config.get("duration"))
        require(key in expected, f"unexpected trial configuration: {trial_path}")
        require(key not in discovered_keys, f"duplicate trial for suite job: {trial_path}")
        discovered_keys.add(key)
        missing = [filename for filename in ("events.jsonl", "summary.json")
                   if not (trial_path / filename).exists()]
        if missing:
            incomplete_dirs.append(dict(path=str(trial_path), missing=missing))
            continue
        trial = analyse_trial(trial_path)
        require(trial["policy"] == expected[key]["case"],
                f"trial policy differs from suite: {trial_path}")
        trials.append(trial)

    missing_jobs = sorted(set(expected) - discovered_keys)
    incomplete = bool(missing_jobs or incomplete_dirs or len(trials) != len(expected))
    if require_complete and incomplete:
        raise ValueError(f"incomplete suite: {len(trials)}/{len(expected)} complete trials")

    has_trial_errors = any(row["incorrect_steps"] or row["integrity_errors"] or row["pending"] or row["error"]
                           for row in trials)
    halt_path = path / "halt.json"
    halt = read_json(halt_path) if halt_path.exists() else None
    averaging_pass = not incomplete and not has_trial_errors and halt is None
    control_plane_pass = (not incomplete and halt is None and
                          all(row["control_plane_pass"] for row in trials))
    passed = control_plane_pass if outcome == "grouping" else averaging_pass
    aggregate = _aggregate_trials(trials)
    expected_policy_count = len({json.dumps(job["case"], sort_keys=True) for job in jobs})
    observed_policy_count = len({row["case"] for row in trials})
    return dict(phase=suite.get("phase"), complete=not incomplete, outcome=outcome,
                **{"pass": passed}, control_plane_pass=control_plane_pass,
                averaging_pass=averaging_pass, halted=halt is not None, halt=halt,
                expected_trials=len(expected), trial_count=len(trials),
                missing_jobs=missing_jobs, incomplete_trials=incomplete_dirs,
                trials=trials, aggregate=aggregate,
                policy_count=dict(expected=expected_policy_count, observed=observed_policy_count),
                incorrect_steps=sorted({step for row in trials for step in row["incorrect_steps"]}),
                integrity_errors=sorted({error for row in trials for error in row["integrity_errors"]}),
                control_integrity_errors=sorted({error for row in trials for error in row["control_integrity_errors"]}),
                pending=sum(len(row["pending"]) for row in trials))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("suite", type=Path, help="Step 24 suite result directory")
    parser.add_argument("--output", type=Path, help="write analysis JSON to this file")
    args = parser.parse_args(argv)
    result = analyse_suite(args.suite)
    rendered = json.dumps(result, indent=2, allow_nan=False) + "\n"
    if args.output:
        args.output.write_text(rendered)
    print(rendered, end="")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ValueError as exc:
        print(f"analysis failed: {exc}", file=sys.stderr)
        raise SystemExit(2)
