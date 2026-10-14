#!/usr/bin/env python3
"""Validate an eight-peer local Agora baseline trace.

The analyzer treats each round as one group observation.  Per-peer trace cells
are used to reconstruct that group, but are not counted as independent rounds.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from pathlib import Path
from typing import Any


VECTOR_SIZE = 5
EXPECTED_PEER_COUNT = 8
PHASE_ROUNDS = {"smoke": 1, "pilot": 5}
ROUND_EVENT_KINDS = ("round_snapshot", "roster", "round_result")


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _finite_number(value: Any) -> bool:
    return _is_number(value) and math.isfinite(float(value))


def _vector(value: Any, field: str, errors: list[str], invalid: list[str]) -> list[float] | None:
    if not isinstance(value, list) or len(value) != VECTOR_SIZE:
        message = f"{field} must contain exactly {VECTOR_SIZE} numbers"
        errors.append(message)
        invalid.append(message)
        return None
    if not all(_finite_number(item) for item in value):
        message = f"{field} contains a non-finite or non-numeric value"
        errors.append(message)
        invalid.append(message)
        return None
    return [float(item) for item in value]


def _summary(values: list[float]) -> dict[str, float | int | None]:
    if not values:
        return {"count": 0, "mean": None, "min": None, "max": None}
    return {
        "count": len(values),
        "mean": sum(values) / len(values),
        "min": min(values),
        "max": max(values),
    }


def _read_trial(trial_path: Path, errors: list[str], invalid: list[str]) -> tuple[dict[str, Any], dict[str, list[dict[str, Any]]]]:
    config_path = trial_path / "config.json"
    config: dict[str, Any] = {}
    try:
        with config_path.open("r", encoding="utf-8") as stream:
            loaded = json.load(stream)
        if not isinstance(loaded, dict):
            raise ValueError("config root must be an object")
        config = loaded
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        message = f"cannot read config.json: {exc}"
        errors.append(message)
        invalid.append(message)
        return {}, {}

    node_names = config.get("node_names")
    if (
        not isinstance(node_names, list)
        or len(node_names) != EXPECTED_PEER_COUNT
        or any(not isinstance(name, str) or not name for name in node_names)
        or len(set(node_names)) != len(node_names)
    ):
        message = f"config node_names must contain {EXPECTED_PEER_COUNT} unique non-empty names"
        errors.append(message)
        invalid.append(message)
        return config, {}

    event_paths = {path.name[len("events_") : -len(".jsonl")]: path for path in trial_path.glob("events_*.jsonl")}
    expected_names = set(node_names)
    for name in sorted(expected_names - set(event_paths)):
        message = f"missing event file for peer {name}"
        errors.append(message)
        invalid.append(message)
    for name in sorted(set(event_paths) - expected_names):
        message = f"unexpected event file for peer {name}"
        errors.append(message)
        invalid.append(message)

    events_by_peer: dict[str, list[dict[str, Any]]] = {}
    for name in node_names:
        path = event_paths.get(name)
        if path is None:
            continue
        peer_events: list[dict[str, Any]] = []
        try:
            with path.open("r", encoding="utf-8") as stream:
                for line_number, line in enumerate(stream, start=1):
                    if not line.strip():
                        continue
                    try:
                        event = json.loads(line)
                    except json.JSONDecodeError as exc:
                        message = f"{path.name}:{line_number}: invalid JSON: {exc.msg}"
                        errors.append(message)
                        invalid.append(message)
                        continue
                    if not isinstance(event, dict):
                        message = f"{path.name}:{line_number}: event must be an object"
                        errors.append(message)
                        invalid.append(message)
                        continue
                    if not isinstance(event.get("kind"), str) or not event.get("kind"):
                        message = f"{path.name}:{line_number}: event is missing kind"
                        errors.append(message)
                        invalid.append(message)
                        continue
                    if event.get("peer") != name:
                        message = f"{path.name}:{line_number}: event peer {event.get('peer')!r} does not match file peer {name!r}"
                        errors.append(message)
                        invalid.append(message)
                    peer_events.append(event)
        except OSError as exc:
            message = f"cannot read {path.name}: {exc}"
            errors.append(message)
            invalid.append(message)
        events_by_peer[name] = peer_events
    return config, events_by_peer


def analyze_trial(path: str | Path) -> dict[str, Any]:
    """Analyze one trial directory and return a JSON-serializable report."""

    trial_path = Path(path)
    errors: list[str] = []
    invalid: list[str] = []
    config, events_by_peer = _read_trial(trial_path, errors, invalid)

    phase = config.get("phase")
    expected_rounds = config.get("rounds")
    if not isinstance(phase, str) or phase not in PHASE_ROUNDS:
        message = f"config phase must be one of {sorted(PHASE_ROUNDS)}"
        errors.append(message)
        invalid.append(message)
    if not isinstance(expected_rounds, int) or isinstance(expected_rounds, bool) or expected_rounds not in (1, 5):
        message = "config rounds must be 1 (smoke) or 5 (pilot)"
        errors.append(message)
        invalid.append(message)
    elif isinstance(phase, str) and phase in PHASE_ROUNDS and expected_rounds != PHASE_ROUNDS[phase]:
        message = f"config phase {phase!r} requires {PHASE_ROUNDS[phase]} rounds, got {expected_rounds}"
        errors.append(message)
        invalid.append(message)

    tolerance = config.get("tolerance", config.get("numerical_tolerance", 1e-5))
    if not _finite_number(tolerance) or float(tolerance) < 0:
        message = "config tolerance must be a finite non-negative number"
        errors.append(message)
        invalid.append(message)
        tolerance = 1e-5
    tolerance = float(tolerance)

    tensor_values = config.get("tensor_values", 100)
    if not isinstance(tensor_values, int) or isinstance(tensor_values, bool) or tensor_values <= 0:
        message = "config tensor_values must be a positive integer"
        errors.append(message)
        invalid.append(message)
        tensor_values = 100
    averaging_every_steps = config.get("averaging_every_steps", 20)
    if not isinstance(averaging_every_steps, int) or isinstance(averaging_every_steps, bool) or averaging_every_steps <= 0:
        message = "config averaging_every_steps must be a positive integer"
        errors.append(message)
        invalid.append(message)
        averaging_every_steps = 20

    node_names = config.get("node_names") if isinstance(config.get("node_names"), list) else []
    valid_names = [name for name in node_names if isinstance(name, str) and name]
    peer_events: dict[str, list[dict[str, Any]]] = {name: events_by_peer.get(name, []) for name in valid_names}

    profile_roles: dict[str, str] | None = None
    if "profiles" in config:
        profiles = config.get("profiles")
        if not isinstance(profiles, dict):
            message = "config profiles must be an object keyed by peer name"
            errors.append(message)
            invalid.append(message)
        else:
            profile_roles = {}
            for name in valid_names:
                profile = profiles.get(name)
                role = profile.get("role") if isinstance(profile, dict) else None
                if not isinstance(role, str) or not role:
                    message = f"config profile for {name} must include a non-empty role"
                    errors.append(message)
                    invalid.append(message)
                else:
                    profile_roles[name] = role

    shutdown_report: dict[str, Any] = {"present": False}
    shutdown_path = trial_path / "shutdown.json"
    if shutdown_path.exists():
        shutdown_report = {"present": True}
        try:
            loaded_shutdown = json.loads(shutdown_path.read_text(encoding="utf-8"))
            if not isinstance(loaded_shutdown, dict):
                raise ValueError("shutdown root must be an object")
            shutdown_report.update(loaded_shutdown)
            if loaded_shutdown.get("safe") is not True:
                message = "shutdown.json safe must be true"
                errors.append(message)
                invalid.append(message)
            exitcodes = loaded_shutdown.get("worker_exitcodes")
            if not isinstance(exitcodes, dict) or set(exitcodes) != set(valid_names) or any(
                not isinstance(exitcodes.get(name), int)
                or isinstance(exitcodes.get(name), bool)
                or exitcodes.get(name) != 0
                for name in valid_names
            ):
                message = "shutdown.json must report exit code 0 for all eight configured workers"
                errors.append(message)
                invalid.append(message)
        except (OSError, json.JSONDecodeError, ValueError) as exc:
            message = f"cannot read shutdown.json: {exc}"
            errors.append(message)
            invalid.append(message)
            shutdown_report["error"] = str(exc)

    def record(message: str, *, is_invalid: bool = False) -> None:
        errors.append(message)
        if is_invalid:
            invalid.append(message)

    def one_event(name: str, kind: str) -> dict[str, Any] | None:
        found = [event for event in peer_events.get(name, []) if event.get("kind") == kind]
        if len(found) != 1:
            if not found:
                record(f"peer {name}: missing {kind} event", is_invalid=True)
            else:
                record(f"peer {name}: expected one {kind} event, found {len(found)}", is_invalid=True)
            return None
        return found[0]

    def parse_indices(value: Any, field: str) -> tuple[int, ...] | None:
        if (
            not isinstance(value, list)
            or len(value) != VECTOR_SIZE
            or any(not isinstance(index, int) or isinstance(index, bool) for index in value)
        ):
            record(f"{field} must contain exactly {VECTOR_SIZE} integers", is_invalid=True)
            return None
        if len(set(value)) != VECTOR_SIZE:
            record(f"{field} must contain {VECTOR_SIZE} unique indices", is_invalid=True)
            return None
        if any(index < 0 or index >= tensor_values for index in value):
            record(f"{field} indices must be within [0, {tensor_values})", is_invalid=True)
            return None
        return tuple(value)

    # Event buckets retain duplicates so they can be reported instead of silently
    # selecting the first or last record.
    round_events: dict[str, dict[int, dict[str, list[dict[str, Any]]]]] = {
        name: {round_number: {kind: [] for kind in ROUND_EVENT_KINDS} for round_number in range(1, 6)}
        for name in valid_names
    }
    for name, events in peer_events.items():
        for event in events:
            kind = event.get("kind")
            if kind in ("worker_error", "clock_discontinuity", "round_skipped"):
                detail = f": {event.get('error')}" if event.get("error") else ""
                record(f"peer {name}: {kind}{detail}", is_invalid=True)
            elif kind in ROUND_EVENT_KINDS:
                round_number = event.get("round")
                if not isinstance(round_number, int) or isinstance(round_number, bool) or round_number not in range(1, 6):
                    record(f"peer {name}: {kind} has invalid round {round_number!r}", is_invalid=True)
                    continue
                if isinstance(expected_rounds, int) and not isinstance(expected_rounds, bool) and round_number > expected_rounds:
                    record(f"peer {name}: unexpected {kind} event for round {round_number}", is_invalid=True)
                round_events[name][round_number][kind].append(event)

    identities: dict[str, dict[str, str]] = {}
    peer_id_to_name: dict[str, str] = {}
    initial_losses: list[float] = []
    final_losses: list[float] = []
    ready_times: list[float] = []
    native_times: list[float] = []
    for name in valid_names:
        identity = one_event(name, "identity")
        if identity is not None:
            peer_id = identity.get("peer_id")
            role = identity.get("role")
            if not isinstance(peer_id, str) or not peer_id or not isinstance(role, str) or not role:
                record(f"peer {name}: identity needs non-empty peer_id and role strings", is_invalid=True)
            else:
                if profile_roles is not None and profile_roles.get(name) != role:
                    record(
                        f"peer {name}: identity role {role!r} does not match configured profile role {profile_roles.get(name)!r}",
                        is_invalid=True,
                    )
                identities[name] = {"peer_id": peer_id, "role": role}
                if peer_id in peer_id_to_name:
                    record(f"duplicate peer_id {peer_id!r} in identity events", is_invalid=True)
                else:
                    peer_id_to_name[peer_id] = name

        initial = one_event(name, "training_initial")
        if initial is not None:
            loss = initial.get("loss")
            if not _finite_number(loss):
                record(f"peer {name}: training_initial loss must be finite", is_invalid=True)
            else:
                initial_losses.append(float(loss))

        final = one_event(name, "training_final")
        if final is not None:
            loss = final.get("loss")
            if not _finite_number(loss):
                record(f"peer {name}: training_final loss must be finite", is_invalid=True)
            else:
                final_losses.append(float(loss))
            if final.get("finite") is not True:
                record(f"peer {name}: training_final finite must be true", is_invalid=True)
            step = final.get("step")
            if not isinstance(step, int) or isinstance(step, bool) or step < 0:
                record(f"peer {name}: training_final step must be a non-negative integer", is_invalid=True)
            elif phase in ("smoke", "pilot"):
                expected_step = 0 if phase == "smoke" else 100
                if step != expected_step:
                    record(
                        f"peer {name}: {phase} training_final step must be {expected_step}, got {step}",
                        is_invalid=True,
                    )

        closures = [event for event in peer_events.get(name, []) if event.get("kind") == "trace_closed"]
        if len(closures) != 1:
            if not closures:
                record(f"peer {name}: missing trace_closed event", is_invalid=True)
            else:
                record(f"peer {name}: expected one trace_closed event, found {len(closures)}", is_invalid=True)
        elif closures[0].get("clean") is not True:
            record(f"peer {name}: trace_closed clean must be true", is_invalid=True)

    # Per-round analysis compares all peers' immutable round snapshots.  The
    # before_apply vectors may include asynchronous local training progress.
    round_reports: list[dict[str, Any]] = []
    agreed_rounds = 0
    peer_round_results = 0
    round_numbers = range(1, expected_rounds + 1) if isinstance(expected_rounds, int) and not isinstance(expected_rounds, bool) and 0 < expected_rounds <= 5 else ()
    for round_number in round_numbers:
        selected: dict[str, dict[str, dict[str, Any]]] = {}
        for name in valid_names:
            selected[name] = {}
            for kind in ROUND_EVENT_KINDS:
                found = round_events.get(name, {}).get(round_number, {}).get(kind, [])
                if len(found) != 1:
                    if not found:
                        record(f"peer {name} round {round_number}: missing {kind}", is_invalid=True)
                    else:
                        record(f"peer {name} round {round_number}: expected one {kind}, found {len(found)}", is_invalid=True)
                    continue
                selected[name][kind] = found[0]
                if kind == "round_result":
                    peer_round_results += 1

        roster_records = [selected[name]["roster"] for name in valid_names if "roster" in selected[name]]
        snapshot_records = [selected[name]["round_snapshot"] for name in valid_names if "round_snapshot" in selected[name]]
        result_records = [selected[name]["round_result"] for name in valid_names if "round_result" in selected[name]]

        parsed_rosters: list[list[str]] = []
        roster_epochs: list[int] = []
        native_group_ids: list[str] = []
        for name, event in ((name, selected[name].get("roster")) for name in valid_names):
            if event is None:
                continue
            members = event.get("members")
            if not isinstance(members, list) or any(not isinstance(member, str) or not member for member in members):
                record(f"peer {name} round {round_number}: roster members must be strings", is_invalid=True)
            else:
                parsed_rosters.append(members)
                if len(members) != EXPECTED_PEER_COUNT or len(set(members)) != EXPECTED_PEER_COUNT:
                    record(f"peer {name} round {round_number}: roster must contain {EXPECTED_PEER_COUNT} unique members")
                if set(members) != set(peer_id_to_name):
                    record(f"peer {name} round {round_number}: roster does not match the eight observed identities")
            epoch = event.get("epoch")
            if not isinstance(epoch, int) or isinstance(epoch, bool) or epoch < 0:
                record(f"peer {name} round {round_number}: roster epoch must be a non-negative integer", is_invalid=True)
            else:
                roster_epochs.append(epoch)
                expected_epoch = averaging_every_steps * round_number
                if epoch != expected_epoch:
                    record(
                        f"peer {name} round {round_number}: roster epoch must be {expected_epoch}, got {epoch}",
                        is_invalid=True,
                    )
            group_id = event.get("native_group_id")
            if not isinstance(group_id, str) or not re.fullmatch(r"(?:0x)?[0-9a-fA-F]+", group_id):
                record(f"peer {name} round {round_number}: native_group_id must be hexadecimal", is_invalid=True)
            else:
                native_group_ids.append(group_id.lower())
            for field, destination in (("ready_to_roster_s", ready_times), ("native_roster_s", native_times)):
                value = event.get(field)
                if not _finite_number(value) or float(value) < 0:
                    record(f"peer {name} round {round_number}: {field} must be finite and non-negative", is_invalid=True)
                else:
                    destination.append(float(value))

        unique_rosters = {tuple(members) for members in parsed_rosters}
        roster_agreement = (
            len(roster_records) == EXPECTED_PEER_COUNT
            and len(parsed_rosters) == EXPECTED_PEER_COUNT
            and len(unique_rosters) == 1
            and all(len(members) == EXPECTED_PEER_COUNT and set(members) == set(peer_id_to_name) for members in parsed_rosters)
        )
        epochs_agree = len(roster_epochs) == EXPECTED_PEER_COUNT and len(set(roster_epochs)) == 1
        group_ids_agree = len(native_group_ids) == EXPECTED_PEER_COUNT and len(set(native_group_ids)) == 1

        parsed_snapshots: dict[str, dict[str, Any]] = {}
        parsed_results: dict[str, dict[str, Any]] = {}
        snapshot_epochs: list[int] = []
        result_epochs: list[int] = []
        all_indices: list[tuple[int, ...]] = []
        max_abs_error: float | None = None
        numerical_pass = True
        round_has_complete_events = all(len(selected[name]) == len(ROUND_EVENT_KINDS) for name in valid_names)

        for name in valid_names:
            snapshot = selected[name].get("round_snapshot")
            if snapshot is not None:
                epoch = snapshot.get("epoch")
                if not isinstance(epoch, int) or isinstance(epoch, bool) or epoch < 0:
                    record(f"peer {name} round {round_number}: snapshot epoch must be a non-negative integer", is_invalid=True)
                else:
                    snapshot_epochs.append(epoch)
                    expected_epoch = averaging_every_steps * round_number
                    if epoch != expected_epoch:
                        record(
                            f"peer {name} round {round_number}: snapshot epoch must be {expected_epoch}, got {epoch}",
                            is_invalid=True,
                        )
                indices_tuple = parse_indices(
                    snapshot.get("indices"), f"peer {name} round {round_number} snapshot indices"
                )
                if indices_tuple is not None:
                    all_indices.append(indices_tuple)
                values = _vector(snapshot.get("values"), f"peer {name} round {round_number} snapshot values", errors, invalid)
                if phase == "smoke" and values is not None:
                    actor_index = valid_names.index(name)
                    if any(abs(value - actor_index) > tolerance for value in values):
                        record(
                            f"peer {name} round {round_number}: smoke snapshots must contain the constant actor index {actor_index}"
                        )
                weight = snapshot.get("weight")
                if not _finite_number(weight) or float(weight) <= 0:
                    record(f"peer {name} round {round_number}: snapshot weight must be finite and positive", is_invalid=True)
                    weight_value = None
                else:
                    weight_value = float(weight)
                if indices_tuple is not None and values is not None and weight_value is not None:
                    parsed_snapshots[name] = {"epoch": epoch, "indices": indices_tuple, "values": values, "weight": weight_value}

            result = selected[name].get("round_result")
            if result is not None:
                epoch = result.get("epoch")
                if not isinstance(epoch, int) or isinstance(epoch, bool) or epoch < 0:
                    record(f"peer {name} round {round_number}: result epoch must be a non-negative integer", is_invalid=True)
                else:
                    result_epochs.append(epoch)
                    expected_epoch = averaging_every_steps * round_number
                    if epoch != expected_epoch:
                        record(
                            f"peer {name} round {round_number}: result epoch must be {expected_epoch}, got {epoch}",
                            is_invalid=True,
                        )
                indices_tuple = parse_indices(
                    result.get("indices"), f"peer {name} round {round_number} result indices"
                )
                if indices_tuple is not None:
                    all_indices.append(indices_tuple)
                delta = _vector(result.get("delta"), f"peer {name} round {round_number} delta", errors, invalid)
                before = _vector(result.get("before_apply"), f"peer {name} round {round_number} before_apply", errors, invalid)
                after = _vector(result.get("after_apply"), f"peer {name} round {round_number} after_apply", errors, invalid)
                roster = result.get("roster")
                if not isinstance(roster, list) or any(not isinstance(member, str) for member in roster):
                    record(f"peer {name} round {round_number}: result roster must be a list of peer ids", is_invalid=True)
                    result_roster = None
                else:
                    result_roster = roster
                failed_size = result.get("failed_size")
                failed_peer_ids = result.get("failed_peer_ids")
                if not isinstance(failed_size, int) or isinstance(failed_size, bool) or failed_size != 0:
                    record(f"peer {name} round {round_number}: failed_size must be zero", is_invalid=True)
                if not isinstance(failed_peer_ids, (list, dict)) or failed_peer_ids:
                    record(f"peer {name} round {round_number}: failed_peer_ids must be an empty list or object", is_invalid=True)
                status = result.get("status")
                if status != "completed":
                    detail = f": {result.get('error')}" if result.get("error") else ""
                    record(f"peer {name} round {round_number}: result status is {status!r}, expected 'completed'{detail}", is_invalid=True)
                if indices_tuple is not None and delta is not None and before is not None and after is not None and result_roster is not None:
                    parsed_results[name] = {
                        "epoch": epoch,
                        "indices": indices_tuple,
                        "delta": delta,
                        "before_apply": before,
                        "after_apply": after,
                        "roster": result_roster,
                    }

        index_sets = set(all_indices)
        indices_agree = len(all_indices) == EXPECTED_PEER_COUNT * 2 and len(index_sets) == 1
        snapshots_and_results_same_epoch = (
            len(snapshot_epochs) == EXPECTED_PEER_COUNT
            and len(result_epochs) == EXPECTED_PEER_COUNT
            and len(set(snapshot_epochs)) == 1
            and len(set(result_epochs)) == 1
            and len(set(snapshot_epochs + result_epochs + roster_epochs)) == 1
        )
        agreed = roster_agreement and epochs_agree and group_ids_agree and indices_agree and snapshots_and_results_same_epoch
        if agreed:
            agreed_rounds += 1

        if len(parsed_snapshots) == EXPECTED_PEER_COUNT and roster_agreement:
            common_roster = parsed_rosters[0]
            for name in valid_names:
                snapshot = parsed_snapshots.get(name)
                result = parsed_results.get(name)
                if snapshot is None or result is None:
                    numerical_pass = False
                    continue
                if (
                    len(result["roster"]) != EXPECTED_PEER_COUNT
                    or len(set(result["roster"])) != EXPECTED_PEER_COUNT
                    or result["roster"] != common_roster
                ):
                    record(f"peer {name} round {round_number}: result roster differs from roster event")
                    numerical_pass = False
                if result["indices"] != snapshot["indices"]:
                    record(f"peer {name} round {round_number}: result indices differ from its snapshot")
                    numerical_pass = False
                if result["epoch"] != snapshot["epoch"]:
                    record(f"peer {name} round {round_number}: result epoch differs from its snapshot")
                    numerical_pass = False

                member_names = [peer_id_to_name.get(peer_id) for peer_id in common_roster]
                if any(member_name is None or member_name not in parsed_snapshots for member_name in member_names):
                    record(f"peer {name} round {round_number}: cannot resolve every roster member snapshot")
                    numerical_pass = False
                    continue
                members = [parsed_snapshots[member_name] for member_name in member_names if member_name is not None]
                total_weight = sum(member["weight"] for member in members)
                # The trace's snapshot weight is the aggregation contribution.
                # Agora's reducer fractions assign tensor chunks to reducers;
                # they do not exclude CLIENT-mode peers from the all-reduce.
                expected_delta = [
                    sum(member["values"][coordinate] * member["weight"] for member in members) / total_weight
                    - snapshot["values"][coordinate]
                    for coordinate in range(VECTOR_SIZE)
                ]
                for coordinate, (actual, expected) in enumerate(zip(result["delta"], expected_delta)):
                    difference = abs(actual - expected)
                    max_abs_error = difference if max_abs_error is None else max(max_abs_error, difference)
                    if difference > tolerance:
                        record(
                            f"peer {name} round {round_number}: delta[{coordinate}] differs from weighted snapshot correction by {difference:.9g} (tolerance {tolerance:.9g})"
                        )
                        numerical_pass = False
                for coordinate, (before, delta, after) in enumerate(zip(result["before_apply"], result["delta"], result["after_apply"])):
                    difference = abs((before + delta) - after)
                    max_abs_error = difference if max_abs_error is None else max(max_abs_error, difference)
                    if difference > tolerance:
                        record(
                            f"peer {name} round {round_number}: before_apply[{coordinate}] + delta does not match after_apply (error {difference:.9g}, tolerance {tolerance:.9g})"
                        )
                        numerical_pass = False
        else:
            numerical_pass = False
            if not roster_agreement:
                record(f"round {round_number}: eight peers do not have one identical eight-member roster")
            if not snapshots_and_results_same_epoch:
                record(f"round {round_number}: snapshots, roster, and results do not share one epoch")
            if not indices_agree:
                record(f"round {round_number}: snapshots and results do not share one indices vector")
            if not group_ids_agree:
                record(f"round {round_number}: native_group_id differs across peer roster events")

        # Preserve a diagnostic row for every configured round, including when
        # required events are absent or malformed.
        roster_sizes = [len(members) for members in parsed_rosters]
        round_reports.append(
            {
                "round": round_number,
                "roster_agreement": roster_agreement,
                "group_size": min(roster_sizes) if roster_sizes else 0,
                "epochs_agree": epochs_agree and snapshots_and_results_same_epoch,
                "indices_agree": indices_agree,
                "numerical_pass": numerical_pass and round_has_complete_events,
                "max_abs_error": max_abs_error,
                "agreed": agreed,
            }
        )

    mean_initial_loss = sum(initial_losses) / len(initial_losses) if len(initial_losses) == EXPECTED_PEER_COUNT else None
    mean_final_loss = sum(final_losses) / len(final_losses) if len(final_losses) == EXPECTED_PEER_COUNT else None
    if phase == "pilot" and mean_initial_loss is not None and mean_final_loss is not None and not mean_final_loss < mean_initial_loss:
        record(f"pilot mean final loss {mean_final_loss:.9g} must be below mean initial loss {mean_initial_loss:.9g}")

    valid = not invalid
    passed = valid and not errors and len(round_reports) == (expected_rounds if isinstance(expected_rounds, int) and not isinstance(expected_rounds, bool) else -1) and all(
        report["agreed"] and report["numerical_pass"] for report in round_reports
    )
    return {
        "pass": passed,
        "valid": valid,
        "errors": errors,
        "phase": phase,
        "seed": config.get("seed"),
        "shutdown": shutdown_report,
        "expected_peers": EXPECTED_PEER_COUNT,
        "expected_rounds": expected_rounds,
        "rounds": round_reports,
        "metrics": {
            "peer_round_results": peer_round_results,
            "agreed_rounds": agreed_rounds,
            "ready_to_roster_s": _summary(ready_times),
            "native_roster_s": _summary(native_times),
            "mean_initial_loss": mean_initial_loss,
            "mean_final_loss": mean_final_loss,
        },
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", type=Path, help="trial directory containing config.json and per-peer event logs")
    parser.add_argument("--output", type=Path, help="write the JSON report to this file instead of stdout")
    args = parser.parse_args(argv)

    report = analyze_trial(args.path)
    rendered = json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n"
    if args.output:
        args.output.write_text(rendered, encoding="utf-8")
    else:
        sys.stdout.write(rendered)
    return 0 if report["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
