"""Raw-safe analysis and reporting for the Step 30 control-plane comparison.

The trial intentionally stops before tensor averaging.  This module therefore
uses Step 25's pure event reconstruction and Step 28's grouping/receipt helpers,
never its tensor-validating trial entry point.  Results are local loopback
measurements with seeded RPC jitter.
"""
from __future__ import annotations

from collections import Counter, defaultdict
import csv
import hashlib
import json
import math
from pathlib import Path
import random
import statistics

try:
    from .analyze_step24_lifetime import finite_number, read_json
    from . import analyze_step25_choices as _step25
    from . import analyze_step26_joint as _step26
    from . import analyze_step28_confirmation as _step28
    from . import analyze_step29_policies as _step29
except ImportError:  # runner imports research/hivemind from its working directory
    from analyze_step24_lifetime import finite_number, read_json
    import analyze_step25_choices as _step25
    import analyze_step26_joint as _step26
    import analyze_step28_confirmation as _step28
    import analyze_step29_policies as _step29


ROOT = Path(__file__).resolve().parent
X_NAME = "X"
HONEST_NAMES = tuple(f"H{i}" for i in range(7))
CAP_SECONDS = 12.0
PROFILE = {"distribution": "uniform", "lower": 0.1, "upper": 0.2}
CLOCK_LIMIT = 0.25
HARM_THRESHOLDS = {
    "honest_mean_capped_wait_seconds": 0.5,
    "honest_group_failure_fraction": 0.05,
    "honest_group_rate_reduction_fraction": 0.15,
}
DECISION_RULE = {
    "wait_increase_seconds": 0.5, "failure_increase_fraction": 0.05,
    "group_rate_decrease_fraction": 0.15, "minimum_threshold_seeds": 2,
    "require_third_same_direction": True, "minimum_late_windows": 3,
    "minimum_persistent_seeds": 2, "honest_starts_per_trial": 100,
    "x_client_acceptances_per_trial": 20, "x_multi_publication_searches": 10,
    "maximum_renewal_to_stock_median_interval": 0.75,
}
CSV_NAMES = ("attempts.csv", "groups.csv", "choices.csv", "joins.csv",
             "retries.csv", "holds.csv", "publications.csv", "windows.csv",
             "states.csv", "trial_summary.csv", "paired_contrasts.csv")


def _ref(event):
    return f"{event['pid']}:{event['event_number']}"


def _event_order(event):
    return (float(event.get("monotonic", 0.0)), event.get("pid", -1),
            event.get("event_number", -1))


def _mean(values):
    rows = [float(value) for value in values if value is not None]
    return statistics.mean(rows) if rows else None


def _median(values):
    rows = [float(value) for value in values if value is not None]
    return statistics.median(rows) if rows else None


def _canonical_cases():
    rows = []
    for name, policy in (("stock_2s", "native"), ("renew_1_2", "renew")):
        row = dict(name=name, mode="passive", behavior="recruit_only",
                   publication_policy=policy, lifetime=2.0, minimum=2, target=4,
                   response="unreliable", hold_seconds=0.5,
                   one_way_delay_seconds=0.15)
        if policy == "renew":
            row.update(frequency=1.0, nominal_lower=1.0, nominal_upper=2.0)
        rows.append(row)
    return rows


def canonical_jobs(phase):
    cases = _canonical_cases()
    if phase == "smoke":
        return [dict(case=case, seed=3000, duration=20.0) for case in cases]
    if phase != "confirm":
        raise ValueError("phase must be smoke or confirm")
    jobs = []
    for seed in (3001, 3002, 3003):
        ordered = list(reversed(cases)) if seed == 3002 else cases
        jobs.extend(dict(case=case, seed=seed, duration=300.0) for case in ordered)
    return jobs


def _read_events(path):
    events = []
    with (Path(path) / "events.jsonl").open(encoding="utf-8") as stream:
        for lineno, line in enumerate(stream, 1):
            if not line.strip():
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"events.jsonl line {lineno}: {exc}") from exc
            required = ("kind", "peer", "other", "pid", "event_number", "monotonic",
                        "wall_time", "detail")
            if not isinstance(event, dict) or any(key not in event for key in required):
                raise ValueError(f"events.jsonl line {lineno}: missing required event fields")
            if (not isinstance(event.get("kind"), str) or not event["kind"] or
                    not isinstance(event.get("peer"), str) or
                    not isinstance(event.get("other"), str) or
                    not isinstance(event.get("pid"), int) or isinstance(event.get("pid"), bool) or
                    not isinstance(event.get("event_number"), int) or
                    isinstance(event.get("event_number"), bool) or
                    not isinstance(event.get("detail"), dict)):
                raise ValueError(f"events.jsonl line {lineno}: invalid event field types")
            try:
                finite_number(event.get("monotonic"), "event monotonic")
                finite_number(event.get("wall_time"), "event wall_time")
            except ValueError as exc:
                raise ValueError(f"events.jsonl line {lineno}: {exc}") from exc
            events.append(event)
    if not events:
        return events
    per_pid = defaultdict(list)
    for event in events:
        per_pid[event["pid"]].append(event)
    for pid, rows in per_pid.items():
        for previous, current in zip(rows, rows[1:]):
            if current["event_number"] != previous["event_number"] + 1:
                raise ValueError(f"pid {pid} has missing, duplicate, or reordered buffered events")
            if current["monotonic"] < previous["monotonic"]:
                raise ValueError(f"pid {pid} has decreasing monotonic observation time")
            if current["wall_time"] < previous["wall_time"]:
                raise ValueError(f"pid {pid} has decreasing wall observation time")
    return events


def _validate_config(config, errors):
    if not isinstance(config, dict):
        errors.append("config.json must contain an object")
        return {}, {}, None, None, None
    case = config.get("case")
    expected_by_name = {row["name"]: row for row in _canonical_cases()}
    expected_case = expected_by_name.get(case.get("name")) if isinstance(case, dict) else None
    if expected_case is None:
        errors.append(f"unknown Step 30 case: {case.get('name') if isinstance(case, dict) else None!r}")
        case = case if isinstance(case, dict) else {}
    elif case != expected_case:
        errors.append("config.case differs from the frozen Step 30 arm")

    fixed = {
        "experiment": "step30", "outcome": "grouping", "instrumentation": "detailed",
        "attempt_unit": "step_start", "timeout": 12, "request_timeout": 3,
        "tensor_size": 128, "honest_min": 2, "target": 4,
        "honest_ad_lifetime": 5, "x_min_matchmaking_time": 2.0,
        "x_min_group_size": 2, "allreduce_timeout": 6, "store_delay": 0,
        "delay_location": "after observed FINISHED, before next step()",
        "group_bits": "", "scheduled_time": "native default",
        "network": "eight-peer loopback", "network_delay_injected": True,
        "one_way_delay_seconds": 0.15, "delay_profile": PROFILE,
        "control_only": True, "tensor_transfer_enabled": False,
        "response_policy": "unreliable", "allow_state_sharing": False,
        "event_transport": "batched", "trace_batch_size": 32,
        "duplicate_read_views": False, "cached_reads_delayed": False,
        "tensor_transfer_delayed": False,
        "leadership_latency_origin": "step_start", "harm_timeout_cap": 12,
        "clock_monitor": True, "clock_jump_limit_seconds": CLOCK_LIMIT,
        "submission_cutoff_checked_per_peer": True,
        "control_delay_model": "remote find/store request+response; join request+each response",
    }
    try:
        seed = int(config.get("seed"))
        if isinstance(config.get("seed"), bool):
            raise ValueError("seed must be an integer")
        duration = finite_number(config.get("duration"), "duration")
        started = finite_number(config.get("started_monotonic"), "started_monotonic")
        if (seed, duration) not in {(3000, 20.0), (3001, 300.0), (3002, 300.0),
                                    (3003, 300.0)}:
            errors.append("seed/duration must match smoke 3000/20s or confirm 3001-3003/300s")
    except (TypeError, ValueError) as exc:
        errors.append(str(exc))
        seed, duration, started = None, None, None
    if seed is not None:
        fixed["network_seed"] = seed
    try:
        fixed["training_delay"] = _step28._training_delay()
    except Exception:
        pass
    for key, expected in fixed.items():
        if config.get(key) != expected:
            errors.append(f"Step 30 control {key} differs from the frozen design")

    peer_ids = config.get("peer_ids")
    names = {X_NAME, *HONEST_NAMES}
    if not isinstance(peer_ids, dict) or set(peer_ids) != names:
        errors.append("peer_ids must map X and H0..H6")
        peer_ids = {}
    elif (any(not isinstance(value, str) or not value for value in peer_ids.values()) or
          len(set(peer_ids.values())) != 8):
        errors.append("peer_ids must contain eight distinct nonempty identities")
    if seed is not None:
        rng = random.Random(seed)
        honest = [rng.uniform(0.0, 3.0) for _ in range(7)]
        offsets = dict(X=rng.uniform(0.0, 3.0),
                       **{f"H{i}": value for i, value in enumerate(honest)})
        if config.get("initial_offsets") != offsets:
            errors.append("initial_offsets differ from seeded staggered launch schedule")
    return case, peer_ids, seed, duration, started


def _runtime_integrity(config, events, case, peer_ids, errors):
    names_by_peer = {peer: name for name, peer in peer_ids.items()}
    runtime = config.get("runtime_matchmaking_config")
    if not isinstance(runtime, dict) or set(runtime) != set(names_by_peer):
        errors.append("runtime_matchmaking_config must cover all eight peer IDs")
        runtime = runtime if isinstance(runtime, dict) else {}
    rows_by_peer = defaultdict(list)
    for event in events:
        if event.get("kind") == "runtime_matchmaking_config":
            rows_by_peer[event.get("peer")].append(event)
    for peer, name in names_by_peer.items():
        expected = dict(min_matchmaking_time=2.0 if name == X_NAME else 5.0,
                        min_group_size=2, target_group_size=4, request_timeout=3,
                        behavior="recruit_only" if name == X_NAME else "normal",
                        publication_policy=(case.get("publication_policy") if name == X_NAME
                                            else "native"),
                        response_policy="unreliable" if name == X_NAME else "native")
        config_row = runtime.get(peer)
        if not isinstance(config_row, dict):
            errors.append(f"runtime matchmaking config missing {name}")
        elif any(config_row.get(key) != value for key, value in expected.items()):
            errors.append(f"runtime matchmaking config differs from expected values for {name}")
        rows = rows_by_peer.get(peer, [])
        if len(rows) != 1:
            errors.append(f"expected one runtime_matchmaking_config event for {name}, found {len(rows)}")
            continue
        detail = rows[0].get("detail", {})
        if any(detail.get(key) != value for key, value in expected.items()):
            errors.append(f"runtime matchmaking event differs from expected values for {name}")
        starts = [event for event in events if event.get("kind") == "step_start" and
                  event.get("peer") == peer]
        first_start = min(starts, key=_event_order, default=None)
        if first_start:
            first_detail = first_start.get("detail", {})
            if rows[0].get("detail", {}).get("step_id") != first_detail.get("step_id"):
                errors.append(f"runtime matchmaking event for {name} is not linked to its first step")
            next_native = [event for event in events if event.get("peer") == peer and
                           event.get("kind") in ("search_start", "ad_started", "ad_read_start") and
                           event.get("detail", {}).get("step_id") == first_detail.get("step_id")]
            if next_native and rows[0].get("monotonic", math.inf) > min(
                    event.get("monotonic", math.inf) for event in next_native):
                errors.append(f"runtime matchmaking event for {name} occurred after native search work")

    publication_rows = defaultdict(list)
    for event in events:
        if event.get("kind") == "runtime_publication_policy":
            publication_rows[event.get("peer")].append(event)
    for peer, name in names_by_peer.items():
        rows = publication_rows.get(peer, [])
        expected_policy = case.get("publication_policy") if name == X_NAME else "native"
        if len(rows) != 1:
            errors.append(f"expected one runtime_publication_policy event for {name}, found {len(rows)}")
            continue
        detail = rows[0].get("detail", {})
        if (detail.get("publication_policy") != expected_policy or
                detail.get("native_original") is not (expected_policy == "native") or
                detail.get("expiration_formula") != "native" or
                detail.get("period_seconds") != (1.0 if expected_policy == "renew" else None)):
            errors.append(f"runtime publication policy attestation differs for {name}")
        if (not isinstance(detail.get("original_sha256"), str) or
                len(detail["original_sha256"]) != 64):
            errors.append(f"runtime publication policy for {name} lacks original source hash")
    source_attestations = {(row.get("detail", {}).get("original_source"),
                            row.get("detail", {}).get("original_sha256"))
                           for rows in publication_rows.values() for row in rows}
    if len(source_attestations) != 1 or any(not source or not digest for source, digest in source_attestations):
        errors.append("runtime publication-policy source attestations disagree across peers")

    network_rows = [event for event in events if event.get("kind") == "network_config_installed"]
    if not network_rows:
        errors.append("network_config_installed event missing")
    for event in network_rows:
        detail = event.get("detail", {})
        if detail.get("network_seed") != config.get("network_seed"):
            errors.append(f"network configuration seed differs at {_ref(event)}")
        if detail.get("delay_profile") != PROFILE:
            errors.append(f"network delay profile differs at {_ref(event)}")
        identities = detail.get("identities")
        if not isinstance(identities, dict) or identities != peer_ids:
            errors.append(f"network identities differ from config at {_ref(event)}")
    return runtime, publication_rows, network_rows


def _deterministic_delay(seed, source, destination, rpc_type, leg, occurrence, response_index):
    payload = json.dumps([int(seed), source, destination, rpc_type, leg,
                          int(occurrence), int(response_index)], separators=(",", ":"))
    uniform = int.from_bytes(hashlib.sha256(payload.encode()).digest()[:8], "big") / 2**64
    return PROFILE["lower"] + (PROFILE["upper"] - PROFILE["lower"]) * uniform


def _native_client_no_response_witness(events, start, end, responses):
    """Link a clean server stream-end to one caller-side handshake closure.

    The native join handler may swallow ``CancelledError`` while waiting for
    its matchmaking lock. Its RPC wrapper can consequently record a clean end
    with no response. That is only a valid no-response outcome when the raw
    trace independently proves the exact caller's matching handshake timed
    out or was cancelled and then closed.
    """
    if end is None or responses:
        return None
    sd = start.get("detail", {})
    caller, destination = start.get("other"), start.get("peer")
    caller_step = sd.get("caller_step_id")
    request_expiration = sd.get("request_expiration")
    if (not caller or not destination or not isinstance(caller_step, str) or
            request_expiration is None or end.get("detail", {}).get("cancelled") is not False or
            end.get("detail", {}).get("error") is not None):
        return None

    matching_attempts = []
    for attempt in events:
        ad = attempt.get("detail", {})
        if (attempt.get("kind") != "join_attempt_start" or
                attempt.get("peer") != caller or attempt.get("other") != destination or
                ad.get("step_id") != caller_step or
                not isinstance(ad.get("join_id"), str) or
                not (start.get("monotonic", math.inf) - 0.25 <=
                     attempt.get("monotonic", -math.inf) <= start.get("monotonic", math.inf))):
            continue
        join_id = ad["join_id"]
        client_rows = [row for row in events if row.get("detail", {}).get("join_id") == join_id and
                       row.get("kind") in ("native_client_wait", "native_client_reply",
                                            "native_client_exception")]
        waits = [row for row in client_rows if row.get("kind") == "native_client_wait" and
                 row.get("detail", {}).get("phase") == "handshake" and
                 row.get("detail", {}).get("step_id") == caller_step and
                 row.get("detail", {}).get("request_expiration") == request_expiration and
                 row.get("peer") == caller and row.get("other") == destination and
                 0 <= start.get("monotonic", -math.inf) - row.get("monotonic", math.inf) <= 0.25]
        exceptions = [row for row in client_rows if row.get("kind") == "native_client_exception" and
                      row.get("detail", {}).get("phase") == "handshake" and
                      row.get("detail", {}).get("step_id") == caller_step and
                      row.get("detail", {}).get("request_expiration") == request_expiration and
                      row.get("peer") == caller and row.get("other") == destination and
                      row.get("detail", {}).get("error") in ("TimeoutError", "CancelledError",
                                                                "asyncio.CancelledError") and
                      start.get("monotonic", -math.inf) <= row.get("monotonic", math.inf) <=
                      end.get("monotonic", -math.inf)]
        replies = [row for row in client_rows if row.get("kind") == "native_client_reply" and
                   row.get("detail", {}).get("phase") == "handshake"]
        if len(waits) != 1 or len(exceptions) != 1 or replies:
            continue
        wait, exception = waits[0], exceptions[0]
        error_name = exception.get("detail", {}).get("error")
        try:
            timeout = finite_number(wait.get("detail", {}).get("timeout"), "native handshake timeout")
        except (TypeError, ValueError):
            continue
        if timeout <= 0:
            continue
        timeout_elapsed = exception.get("monotonic", -math.inf) - wait.get("monotonic", math.inf)
        if error_name == "TimeoutError" and abs(timeout_elapsed - timeout) > 0.25:
            continue
        if error_name != "TimeoutError" and timeout_elapsed > timeout + 0.25:
            continue
        closes = [row for row in events if row.get("kind") == "join_attempt_end" and
                  row.get("detail", {}).get("join_id") == join_id and
                  row.get("peer") == caller and row.get("other") == destination and
                  row.get("detail", {}).get("step_id") == caller_step and
                  exception.get("monotonic", -math.inf) <= row.get("monotonic", math.inf) <=
                  exception.get("monotonic", -math.inf) + 0.25]
        if len(closes) != 1:
            continue
        client_end = closes[0]
        server_end_delta = end.get("monotonic", math.inf) - exception.get("monotonic", -math.inf)
        if not -0.05 <= server_end_delta <= 0.25:
            continue
        matching_attempts.append(dict(
            join_id=join_id, caller_peer_id=caller, destination_peer_id=destination,
            caller_step_id=caller_step, request_expiration=request_expiration,
            closure_kind="native_client_timeout" if error_name == "TimeoutError"
                         else "native_client_cancelled",
            wait_event_ref=_ref(wait), exception_event_ref=_ref(exception),
            join_start_event_ref=_ref(attempt), join_end_event_ref=_ref(client_end),
            server_start_event_ref=_ref(start), server_end_event_ref=_ref(end),
            timeout_seconds=timeout, timeout_elapsed_seconds=timeout_elapsed,
            server_end_after_client_exception_seconds=server_end_delta))
    return matching_attempts[0] if len(matching_attempts) == 1 else None


def _rpc_integrity(config, events, peer_ids, native, errors):
    """Audit seeded per-leg jitter without applying Step 28's fixed-delay rule."""
    names_by_peer = {peer: name for name, peer in peer_ids.items()}
    groups = defaultdict(lambda: defaultdict(list))
    start_rows = {}
    rpc_kinds = {"control_rpc_start", "control_rpc_delay", "control_rpc_handler_enter",
                 "control_rpc_response", "control_rpc_end", "control_join_admission_state"}
    for event in events:
        kind = event.get("kind")
        if kind not in rpc_kinds:
            continue
        detail = event.get("detail", {})
        rpc_id = detail.get("rpc_id")
        if not isinstance(rpc_id, str) or not rpc_id:
            errors.append(f"{kind} lacks rpc_id at {_ref(event)}")
            continue
        key = (event.get("pid"), rpc_id)
        groups[key][kind].append(event)
        if kind == "control_rpc_start":
            if key in start_rows:
                errors.append(f"duplicate control_rpc_start {rpc_id}")
            start_rows[key] = event
    delay_rows = []
    coverage = {kind: dict(total=0, correlated=0, delayed_legs=0, correlated_delayed=0)
                for kind in ("find", "store", "join")}
    exclusions = []
    client_timeout_exclusions = []
    observed_activity_kinds = {"search_span_end", "ad_read_end", "join_attempt_end",
                               "assembled", "ad_finished", "control_round_complete"}
    activity = [event for event in events if event.get("kind") in observed_activity_kinds]
    activity_cutoff = max((float(event["monotonic"]) for event in activity), default=None)
    close = next((event for event in events if event.get("kind") == "trace_closed"), None)
    for key, records in groups.items():
        start = start_rows.get(key)
        if start is None:
            errors.append(f"RPC event has no start for {key[1]}")
            continue
        detail = start.get("detail", {})
        rpc_id = detail.get("rpc_id")
        rpc_type = detail.get("rpc_type")
        if rpc_type not in coverage:
            errors.append(f"unknown RPC type for {rpc_id}: {rpc_type!r}")
            continue
        if not isinstance(detail.get("key_hashes", []), list):
            errors.append(f"RPC {rpc_id} key_hashes is not a list")
        measured = detail.get("measured") is True
        source_name, destination_name = detail.get("source_name"), detail.get("destination_name")
        if measured:
            if (source_name not in names_by_peer.values() or destination_name not in names_by_peer.values() or
                    names_by_peer.get(start.get("other")) != source_name or
                    names_by_peer.get(start.get("peer")) != destination_name):
                errors.append(f"measured RPC {rpc_id} source/destination labels do not match peer IDs")
            if detail.get("network_seed") != config.get("network_seed"):
                errors.append(f"measured RPC {rpc_id} has wrong network seed")
            try:
                occurrence = int(detail.get("occurrence"))
                if occurrence < 1:
                    raise ValueError
            except (TypeError, ValueError):
                errors.append(f"measured RPC {rpc_id} has invalid occurrence")
                occurrence = 1
        else:
            occurrence = int(detail.get("occurrence", 0) or 0)
            if detail.get("network_seed") is not None:
                errors.append(f"unmeasured RPC {rpc_id} unexpectedly has a network seed")
        try:
            start_delay = finite_number(detail.get("delay_seconds"), "RPC compatibility delay")
            expected_mean = 0.15 if measured else 0.0
            if not math.isclose(start_delay, expected_mean, rel_tol=0.0, abs_tol=1e-12):
                errors.append(f"RPC {rpc_id} compatibility delay metadata differs from profile mean")
        except (TypeError, ValueError) as exc:
            errors.append(f"RPC {rpc_id}: {exc}")

        delays = sorted(records.get("control_rpc_delay", []), key=_event_order)
        legs = Counter(row.get("detail", {}).get("leg") for row in delays)
        responses = sorted(records.get("control_rpc_response", []), key=_event_order)
        ends = records.get("control_rpc_end", [])
        handlers = records.get("control_rpc_handler_enter", [])
        admissions = records.get("control_join_admission_state", [])
        if len(ends) > 1:
            errors.append(f"RPC {rpc_id} has duplicate end records")
        if len(admissions) > 1:
            errors.append(f"join RPC {rpc_id} has duplicate admission-state records")
        for delay in delays:
            dd = delay.get("detail", {})
            leg = dd.get("leg")
            if leg not in ("request", "response"):
                errors.append(f"RPC {rpc_id} has invalid delay leg")
                continue
            try:
                requested = finite_number(dd.get("requested_delay"), "requested delay")
                actual = finite_number(dd.get("actual_delay"), "actual delay")
                response_index = int(dd.get("response_index", 0))
                if actual < 0:
                    errors.append(f"RPC {rpc_id} actual delay is negative")
                if measured:
                    expected = _deterministic_delay(config["network_seed"], source_name,
                                                    destination_name, rpc_type, leg,
                                                    occurrence, response_index)
                else:
                    expected = 0.0
                if not math.isclose(requested, expected, rel_tol=0.0, abs_tol=1e-12):
                    errors.append(f"RPC {rpc_id} {leg}/{response_index} requested delay differs from seed")
                if dd.get("cancelled") is not True and actual + .005 < requested:
                    errors.append(f"RPC {rpc_id} {leg}/{response_index} completed materially early")
                if dd.get("cancelled") is not True:
                    delay_rows.append(dict(rpc_id=rpc_id, rpc_type=rpc_type, leg=leg,
                                           response_index=response_index, requested_delay=requested,
                                           actual_delay=actual, cancelled=False,
                                           event_ref=_ref(delay), source_name=source_name,
                                           destination_name=destination_name))
            except (TypeError, ValueError, KeyError) as exc:
                errors.append(f"RPC {rpc_id} delay metadata: {exc}")

        end = ends[0] if ends else None
        ended_cancelled = bool(end and end.get("detail", {}).get("cancelled"))
        ended_error = bool(end and end.get("detail", {}).get("error"))
        request_cancelled = any(row.get("detail", {}).get("leg") == "request" and
                                row.get("detail", {}).get("cancelled") is True for row in delays)
        response_indices = [row.get("detail", {}).get("response_index") for row in responses]
        if response_indices != list(range(len(response_indices))):
            errors.append(f"RPC {rpc_id} response indexes are not contiguous")
        if rpc_type == "join" and any(not row.get("detail", {}).get("code") for row in responses):
            errors.append(f"join RPC {rpc_id} response lacks a code")

        teardown_prefix = False
        if end is None:
            ordered = sorted((start, *delays, *handlers, *responses, *admissions), key=_event_order)
            kinds = [row.get("kind") for row in ordered]
            request_rows = [row for row in delays if row.get("detail", {}).get("leg") == "request"]
            shape_ok = (kinds == ["control_rpc_start"] or
                        (len(request_rows) == 1 and
                         kinds == ["control_rpc_start", "control_rpc_delay"] and
                         request_rows[0] is ordered[1]) or
                        (len(request_rows) == 1 and len(handlers) == 1 and
                         kinds == ["control_rpc_start", "control_rpc_delay",
                                   "control_rpc_handler_enter"] and
                         request_rows[0] is ordered[1] and handlers[0] is ordered[2]))
            late = activity_cutoff is not None and start.get("monotonic", -math.inf) > activity_cutoff
            before_close = close is not None and all(row.get("monotonic", math.inf) < close.get("monotonic", -math.inf)
                                                     for row in ordered)
            if (rpc_type in ("find", "store") and detail.get("step_id") is None and
                    shape_ok and late and before_close):
                teardown_prefix = True
                exclusions.append(dict(pid=key[0], rpc_id=rpc_id, rpc_type=rpc_type,
                                       event_refs=[_ref(row) for row in ordered],
                                       reason="incomplete unary RPC prefix strictly after native control activity; raw events retained"))
            else:
                errors.append(f"RPC {rpc_id} has no end and is not a narrow teardown-only unary prefix")
        elif not isinstance(end.get("detail", {}).get("cancelled"), bool):
            errors.append(f"RPC {rpc_id} end has no boolean cancelled field")

        if not teardown_prefix:
            request_rows = [row for row in delays if row.get("detail", {}).get("leg") == "request"]
            if len(request_rows) != 1:
                errors.append(f"RPC {rpc_id} must have exactly one request-delay record")
            if not (ended_cancelled or request_cancelled) and len(handlers) != 1:
                errors.append(f"RPC {rpc_id} must have one handler-enter event")
            if not ended_cancelled and not ended_error and not request_cancelled:
                if legs["response"] != len(responses):
                    errors.append(f"RPC {rpc_id} response events/delay records differ")
                if rpc_type in ("find", "store") and (len(responses) != 1 or legs["response"] != 1):
                    errors.append(f"unary RPC {rpc_id} must have one response and delay")
                if rpc_type == "join" and not responses:
                    witness = _native_client_no_response_witness(events, start, end, responses)
                    admission_records = records.get("control_join_admission_state", [])
                    if (witness is not None and len(handlers) == 1 and len(admission_records) == 1 and
                            len(request_rows) == 1 and request_rows[0].get("detail", {}).get("cancelled") is not True):
                        client_timeout_exclusions.append(dict(
                            pid=key[0], rpc_id=rpc_id, event_refs=[_ref(start), _ref(end)],
                            reason="no server reply; exact native caller handshake timeout/cancellation and join closure are recorded",
                            client_lifecycle=witness))
                    else:
                        errors.append(f"join RPC {rpc_id} has no client response record and no unique corroborated native caller timeout/cancellation")

        coverage[rpc_type]["total"] += 1
        if measured:
            request_legs = [row for row in delays if row.get("detail", {}).get("leg") == "request"]
            if any(row.get("detail", {}).get("cancelled") is not True and
                   row.get("detail", {}).get("requested_delay", 0) >= PROFILE["lower"]
                   for row in request_legs):
                coverage[rpc_type]["delayed_legs"] += 1

    for key in set(groups) - set(start_rows):
        errors.append(f"orphan RPC records for {key[1]}")

    # Coverage links use caller/leader IDs and time intervals. Cached reads may
    # legitimately have no find RPC; an observed RPC is correlated only when
    # its native work interval identifies the same activity.
    known = set(peer_ids.values())
    reads = [row for row in native.get("reads", []) if row.get("success") and
             row.get("start_monotonic") is not None and row.get("end_monotonic") is not None]
    join_starts = [event for event in events if event.get("kind") == "join_attempt_start"]
    join_ends = {(event.get("pid"), event.get("detail", {}).get("join_id")): event
                 for event in events if event.get("kind") == "join_attempt_end"}
    holders = [event for event in events if event.get("kind") == "holder_store" and
               event.get("detail", {}).get("subkey") in known and
               event.get("detail", {}).get("active") is True]
    advertisement_keys = {str(event.get("detail", {}).get("key", "")).lower().removeprefix("0x")
                          for event in holders}
    publication_finishes = defaultdict(list)
    for event in events:
        if event.get("kind") in ("ad_finished", "ad_cancelled"):
            detail = event.get("detail", {})
            publication_finishes[(event.get("pid"), detail.get("expiration"), detail.get("active"))].append(event)
    publication_intervals = []
    for declaration in events:
        if declaration.get("kind") != "ad_started":
            continue
        signature = (declaration.get("pid"), declaration.get("detail", {}).get("expiration"),
                     declaration.get("detail", {}).get("active"))
        ending = min((event for event in publication_finishes.get(signature, [])
                      if event.get("event_number", -1) > declaration.get("event_number", math.inf)),
                     key=_event_order, default=None)
        if ending is not None:
            publication_intervals.append((declaration.get("peer"), declaration["monotonic"], ending["monotonic"]))
    for key, records in groups.items():
        start = start_rows.get(key)
        if start is None or start.get("detail", {}).get("measured") is not True:
            continue
        rpc_type = start["detail"].get("rpc_type")
        source_peer, destination_peer = start.get("other"), start.get("peer")
        row = coverage[rpc_type]
        correlated = False
        if rpc_type == "find":
            keys = {str(value).lower().removeprefix("0x") for value in start["detail"].get("key_hashes", [])}
            correlated = bool(keys & advertisement_keys) and (
                any(read.get("peer_id") == source_peer and
                    read["start_monotonic"] <= start["monotonic"] <= read["end_monotonic"] for read in reads)
                or any(peer == source_peer and lower <= start["monotonic"] <= upper
                       for peer, lower, upper in publication_intervals))
        elif rpc_type == "join":
            for attempt in join_starts:
                join_id = attempt.get("detail", {}).get("join_id")
                end = join_ends.get((attempt.get("pid"), join_id))
                if (attempt.get("peer") == source_peer and attempt.get("other") == destination_peer and
                        attempt.get("monotonic", math.inf) <= start.get("monotonic", -math.inf) <=
                        (end.get("monotonic", -math.inf) if end else attempt.get("monotonic", -math.inf) + 1.0)):
                    correlated = True
                    break
        elif rpc_type == "store":
            handler_time = min((event["monotonic"] for event in records.get("control_rpc_handler_enter", [])),
                               default=start["monotonic"])
            end = min(records.get("control_rpc_end", []), key=_event_order, default=None)
            end_time = end.get("monotonic", math.inf) if end else math.inf
            keys = {str(value).lower().removeprefix("0x") for value in
                    start.get("detail", {}).get("key_hashes", []) if isinstance(value, str)}
            correlated = any(event.get("peer") == destination_peer and
                             event.get("other") == source_peer and
                             event.get("detail", {}).get("accepted") in (True, False) and
                             str(event.get("detail", {}).get("key", "")).lower().removeprefix("0x") in keys and
                             handler_time <= event["monotonic"] <= end_time for event in holders)
        if correlated:
            row["correlated"] += 1
            request_delayed = any(
                delay.get("detail", {}).get("leg") == "request" and
                delay.get("detail", {}).get("cancelled") is not True and
                delay.get("detail", {}).get("requested_delay", 0) >= PROFILE["lower"]
                for delay in records.get("control_rpc_delay", []))
            if request_delayed:
                row["correlated_delayed"] += 1

    summary = dict(rpc_counts={kind: value["total"] for kind, value in coverage.items()},
                   rpc_coverage=dict(coverage), requested_delayed_rpc_legs=len(delay_rows),
                   request_delay_median_seconds=_median([row["requested_delay"] for row in delay_rows
                                                         if row["leg"] == "request"]),
                   actual_delay_median_seconds=_median([row["actual_delay"] for row in delay_rows]),
                   delayed_rpc_leg_coverage={key: value["delayed_legs"]
                                              for key, value in coverage.items()},
                   delayed_activity_coverage={key: value["correlated_delayed"]
                                              for key, value in coverage.items()},
                   excluded_teardown_prefix_count=len(exclusions),
                   excluded_teardown_prefixes=exclusions,
                   no_response_native_client_closure_count=len(client_timeout_exclusions),
                   no_response_native_client_closures=client_timeout_exclusions,
                   deterministic_delay_formula="sha256(compact-json([seed,source,destination,rpc_type,leg,occurrence,response_index]))[:8] / 2**64 mapped linearly to [0.1,0.2]")
    return dict(pass_=not errors, errors=errors, summary=summary, delay_rows=delay_rows)


def _completion_integrity(events, config, native, errors):
    peer_ids = config.get("peer_ids", {})
    names_by_peer = {peer: name for name, peer in peer_ids.items()}
    groups_by_id = {row.get("group_id"): row for row in native.get("groups", [])}
    completions = [event for event in events if event.get("kind") == "control_round_complete"]
    by_group = defaultdict(list)
    by_step = defaultdict(list)
    results = {(event.get("peer"), event.get("detail", {}).get("step_id")): event
               for event in events if event.get("kind") == "step_result"}
    for event in completions:
        detail = event.get("detail", {})
        gid = detail.get("group_id")
        by_group[gid].append(event)
        by_step[(event.get("peer"), detail.get("step_id"))].append(event)
        if detail.get("hook") != "metadata_only":
            errors.append(f"control completion has unexpected hook at {_ref(event)}")
        if gid not in groups_by_id:
            errors.append(f"control completion refers to unknown group {gid!r}")
        if event.get("peer") not in peer_ids.values():
            errors.append(f"control completion has unknown participant at {_ref(event)}")
        if not isinstance(detail.get("members"), dict):
            errors.append(f"control completion lacks member metadata at {_ref(event)}")
        result = results.get((event.get("peer"), detail.get("step_id")))
        if result is None or result.get("detail", {}).get("status") != "completed":
            errors.append(f"control completion has no matching completed step result at {_ref(event)}")
        members = detail.get("members", {})
        if event.get("peer") not in members:
            errors.append(f"control completion omits its local member at {_ref(event)}")
        elif (not isinstance(members.get(event.get("peer")), dict) or
              members.get(event.get("peer"), {}).get("step_id") != detail.get("step_id")):
            errors.append(f"control completion local member step differs at {_ref(event)}")
        group = groups_by_id.get(gid)
        if group and event.get("peer") not in group.get("roster", []):
            errors.append(f"control completion peer is outside assembled roster at {_ref(event)}")

    for key, result in results.items():
        if result.get("detail", {}).get("status") == "completed" and len(by_step.get(key, [])) != 1:
            errors.append(f"completed step {key[1]} must have exactly one metadata completion")
        if result.get("detail", {}).get("status") != "completed" and by_step.get(key):
            errors.append(f"noncompleted step {key[1]} has metadata completion")
    group_rows = {}
    for group in groups_by_id.values():
        gid = group.get("group_id")
        assembled = next((event for event in events if event.get("kind") == "assembled" and
                          event.get("detail", {}).get("group_id") == gid), None)
        members = (assembled or {}).get("detail", {}).get("members", {})
        rows = by_group.get(gid, [])
        seen = [row.get("peer") for row in rows]
        if len(set(seen)) != len(seen):
            errors.append(f"group {gid} has duplicate participant metadata completion")
        for row in rows:
            detail = row.get("detail", {})
            if detail.get("members") != members:
                errors.append(f"group {gid} metadata differs from assembled roster at {_ref(row)}")
            if row.get("peer") not in set(group.get("roster", [])):
                errors.append(f"group {gid} metadata completion is not from a roster member")
            if row.get("monotonic", -math.inf) < (assembled or {}).get("monotonic", math.inf):
                errors.append(f"group {gid} metadata completion precedes native assembly")
        group_rows[gid] = dict(metadata_completion_count=len(rows),
                               metadata_completion_event_refs=[_ref(row) for row in rows])

    violations = [event for event in events if event.get("kind") == "forbidden_tensor_rpc"]
    if violations:
        errors.append(f"observed {len(violations)} forbidden tensor RPC event(s)")
    tensor_events = [event for event in events if event.get("kind") in
                     ("allreduce_start", "allreduce_result", "tensor_rpc_start", "tensor_rpc_end")]
    if tensor_events:
        errors.append(f"control-only trial emitted {len(tensor_events)} tensor/aggregate event(s)")
    summary = read_json(Path(config.get("_trial_path", ".")) / "summary.json") if config.get("_trial_path") else {}
    for outcome in summary.get("outcomes", []):
        if outcome.get("control_only") is not True or outcome.get("tensor_averaging_performed") is not False:
            errors.append(f"step outcome {outcome.get('step_id')} lacks metadata-only attestation")
        if any(key in outcome for key in ("actual", "expected", "correct")):
            errors.append(f"step outcome {outcome.get('step_id')} contains tensor correctness claims")
    return group_rows


def _actual_membership(events, attempts, groups, peer_ids, errors):
    """Use each participant's successful native assembly observation.

    A leader roster can claim a follower that never received/assembled the
    group. Only the leader's `assembled` event or the follower's own
    `follower_assembled` event establishes that participant's success.
    """
    names_by_peer = {peer: name for name, peer in peer_ids.items()}
    group_by_id = {row.get("group_id"): row for row in groups}
    starts_by_key = {(row.get("peer_id"), row.get("step_id")): row for row in attempts}
    assembled_events = [event for event in events if event.get("kind") == "assembled"]
    follower_events = [event for event in events if event.get("kind") == "follower_assembled"]
    local_events = defaultdict(list)
    follower_refs = defaultdict(list)
    leader_signatures = {}
    for event in assembled_events:
        detail = event.get("detail", {})
        gid = detail.get("group_id")
        group = group_by_id.get(gid)
        if group is None:
            errors.append(f"leader assembled event has no unique group row: {gid!r}")
            continue
        if event.get("peer") != group.get("leader_peer_id"):
            errors.append(f"assembled peer is not recorded group leader for {gid}")
        signature = (tuple(detail.get("roster", [])), detail.get("members", {}),
                     detail.get("step_id"))
        if gid in leader_signatures and leader_signatures[gid] != signature:
            errors.append(f"conflicting repeated leader assembly metadata for {gid}")
        leader_signatures[gid] = signature
        leader_key = (event.get("peer"), detail.get("step_id"))
        start = starts_by_key.get(leader_key)
        if start is None:
            errors.append(f"leader assembly {gid} has no exact leader step_start")
        elif event.get("monotonic", -math.inf) < start.get("started_monotonic", math.inf):
            errors.append(f"leader assembly {gid} precedes its step_start")
        local_events[leader_key].append(event)
    for event in follower_events:
        detail = event.get("detail", {})
        gid = detail.get("group_id")
        group = group_by_id.get(gid)
        peer, step_id = event.get("peer"), detail.get("step_id")
        if group is None:
            errors.append(f"follower_assembled event has no leader group {gid!r}")
            continue
        if peer == group.get("leader_peer_id") or peer not in group.get("roster", []):
            errors.append(f"follower_assembled peer is not a follower in group {gid}")
        leader = next((row for row in assembled_events
                       if row.get("detail", {}).get("group_id") == gid), None)
        expected_members = (leader or {}).get("detail", {}).get("members")
        if expected_members is not None and detail.get("members") != expected_members:
            errors.append(f"follower assembly metadata differs from leader roster for {gid}")
        member = detail.get("members", {}).get(peer)
        if not isinstance(member, dict) or member.get("step_id") != step_id:
            errors.append(f"follower assembly {gid} does not identify the follower's own step")
        start = starts_by_key.get((peer, step_id))
        if start is None:
            errors.append(f"follower assembly {gid} has no exact local step_start")
        elif event.get("monotonic", -math.inf) < start.get("started_monotonic", math.inf):
            errors.append(f"follower assembly {gid} precedes local step_start")
        local_events[(peer, step_id)].append(event)
        follower_refs[gid].append(_ref(event))

    for group in groups:
        gid = group.get("group_id")
        group["follower_assembled_event_refs"] = sorted(follower_refs.get(gid, []))
        group["follower_assembled_peer_ids"] = [
            row.get("peer") for row in follower_events
            if row.get("detail", {}).get("group_id") == gid]
        group["follower_corroborated"] = bool(follower_refs.get(gid))
        group["follower_corroborated_event_refs"] = sorted(follower_refs.get(gid, []))

    for attempt in attempts:
        key = (attempt.get("peer_id"), attempt.get("step_id"))
        start_time = attempt.get("started_monotonic")
        observations = sorted((event for event in local_events.get(key, [])
                              if event.get("monotonic", -math.inf) >= start_time), key=_event_order)
        first = observations[0] if observations else None
        gid = first.get("detail", {}).get("group_id") if first else None
        group = group_by_id.get(gid)
        leader_peer = (group or {}).get("leader_peer_id")
        leader_name = names_by_peer.get(leader_peer)
        latency = (float(first["monotonic"] - start_time) if first else None)
        attempt.update(
            first_assembly_group_id=gid,
            first_assembly_event_ref=_ref(first) if first else None,
            first_assembly_monotonic=first.get("monotonic") if first else None,
            first_assembly_latency_seconds=latency,
            first_group_event_ref=_ref(first) if first else None,
            first_group_latency_seconds=latency,
            first_group_leader_peer_id=leader_peer,
            first_group_leader_name=leader_name,
            first_group_leader_category=("no_group" if first is None else
                                         X_NAME if leader_name == X_NAME else "other"),
            first_local_assembly_kind=first.get("kind") if first else None,
            first_local_assembly_observation_count=len(observations),
            capped_latency_seconds=min(CAP_SECONDS, max(0.0, latency)) if latency is not None else CAP_SECONDS,
            membership_success=first is not None,
            led_success=bool(first and first.get("kind") == "assembled" and
                             first.get("peer") == attempt.get("peer_id")),
        )
    return group_by_id


def _link_joins(events, config, native, errors):
    peer_ids = config.get("peer_ids", {})
    names_by_peer = {peer: name for name, peer in peer_ids.items()}
    refs = {_ref(event): event for event in events}
    starts, ends, client = {}, {}, defaultdict(list)
    rpc_starts = []
    linked_server_kinds = {"control_join_admission_state", "unreliable_accept",
                           "unreliable_reject", "unreliable_hold_start",
                           "unreliable_disband", "unreliable_cancel", "unreliable_release"}
    for event in events:
        kind, detail = event.get("kind"), event.get("detail", {})
        if kind in ("join_attempt_start", "join_attempt_end"):
            join_id = detail.get("join_id")
            if not isinstance(join_id, str) or not join_id:
                errors.append(f"{kind} missing join_id at {_ref(event)}")
                continue
            target = starts if kind == "join_attempt_start" else ends
            if join_id in target:
                errors.append(f"duplicate {kind} for {join_id}")
            target[join_id] = event
        elif kind in ("native_client_wait", "native_client_reply", "native_client_exception"):
            join_id = detail.get("join_id")
            if not isinstance(join_id, str) or not join_id:
                errors.append(f"{kind} missing join_id at {_ref(event)}")
                continue
            client[join_id].append(event)
        elif kind == "control_rpc_start" and detail.get("rpc_type") == "join":
            rpc_starts.append(event)

    joins = []
    for join_id, start in starts.items():
        detail = start.get("detail", {})
        peer, leader, step_id = start.get("peer"), start.get("other"), detail.get("step_id")
        end = ends.get(join_id)
        if (peer not in names_by_peer or leader not in names_by_peer or peer == leader or
                names_by_peer.get(peer) == X_NAME):
            errors.append(f"unexpected join initiator/destination in this recruit-only experiment: {join_id}")
        if end and (end.get("peer") != peer or end.get("other") != leader or
                    end.get("detail", {}).get("step_id") != step_id or
                    end.get("detail", {}).get("search_id") != detail.get("search_id") or
                    end.get("detail", {}).get("origin") != detail.get("origin") or
                    end.get("monotonic", -math.inf) < start.get("monotonic", math.inf)):
            errors.append(f"join {join_id} start/end identity or time mismatch")
        selection_ref = detail.get("selection_event_ref")
        selection = refs.get(selection_ref) if selection_ref else None
        if detail.get("origin") == "queue" and (selection is None or
                selection.get("kind") != "native_pop_selected" or selection.get("peer") != peer or
                selection.get("other") != leader or selection.get("detail", {}).get("step_id") != step_id or
                selection.get("detail", {}).get("search_id") != detail.get("search_id")):
            errors.append(f"join {join_id} lacks a matching exact native selection reference")
        if detail.get("origin") not in ("queue", "redirect"):
            errors.append(f"join {join_id} has invalid origin")
        if detail.get("origin") == "redirect" and selection_ref not in (None, ""):
            errors.append(f"redirect join {join_id} unexpectedly has a selection reference")
        records = sorted(client.get(join_id, []), key=_event_order)
        for record in records:
            rd = record.get("detail", {})
            if (record.get("peer") != peer or record.get("other") != leader or
                    rd.get("step_id") != step_id or
                    record.get("monotonic", -math.inf) < start.get("monotonic", math.inf) or
                    (end is not None and record.get("monotonic", math.inf) > end.get("monotonic", -math.inf))):
                errors.append(f"native client record identity/time mismatch for {join_id}")
        replies = [row for row in records if row.get("kind") == "native_client_reply"]
        client_waits = [row for row in records if row.get("kind") == "native_client_wait"]
        request_expirations = [row.get("detail", {}).get("request_expiration") for row in client_waits
                               if row.get("detail", {}).get("request_expiration") is not None]
        unique_request_expirations = list(dict.fromkeys(request_expirations))
        if len(unique_request_expirations) > 1:
            errors.append(f"native client wait phases disagree on request expiration for {join_id}")
        # Link to the destination's logged server RPC using the actual caller
        # deadline and caller step. The selected leader expiry is a separate
        # local queue view and is never substituted for the request expiry.
        lower = start.get("monotonic", -math.inf)
        upper = end.get("monotonic", math.inf) if end else math.inf
        expected_request_expiration = (unique_request_expirations[0]
                                       if len(unique_request_expirations) == 1 else None)
        matching_rpcs = [row for row in rpc_starts
                         if row.get("other") == peer and row.get("peer") == leader and
                         row.get("detail", {}).get("caller_step_id") == step_id and
                         lower <= row.get("monotonic", math.inf) <= upper and
                         expected_request_expiration is not None and
                         row.get("detail", {}).get("request_expiration") == expected_request_expiration]
        linked_rpc = matching_rpcs[0] if len(matching_rpcs) == 1 else None
        linked_rpc_id = (linked_rpc or {}).get("detail", {}).get("rpc_id")
        server_records = [row for row in events if row.get("kind") in linked_server_kinds and
                          row.get("detail", {}).get("rpc_id") == linked_rpc_id] if linked_rpc_id else []
        admission = next((row for row in server_records
                          if row.get("kind") == "control_join_admission_state"), None)
        unreliable_visits = [dict(event_ref=_ref(row), kind=row.get("kind"),
                                  visit_id=row.get("detail", {}).get("visit_id"),
                                  rpc_id=row.get("detail", {}).get("rpc_id"),
                                  request_expiration=row.get("detail", {}).get("request_expiration"),
                                  leader_step_id=row.get("detail", {}).get("leader_step_id"),
                                  leader_search_id=row.get("detail", {}).get("leader_search_id"),
                                  hold_seconds=row.get("detail", {}).get("delta"))
                            for row in server_records if row.get("kind", "").startswith("unreliable_")]
        selected_expiration = (selection or {}).get("detail", {}).get("selected_expiration")
        caller_ad_expiration = (selection or {}).get("detail", {}).get("own_expiration")
        leader_waits = [row for row in events if row.get("kind") == "native_leader_wait" and
                        row.get("peer") == leader and row.get("other") == peer and
                        start.get("monotonic", -math.inf) <= row.get("monotonic", math.inf) <=
                        (end.get("monotonic", math.inf) if end else math.inf)]
        leader_expirations = [row.get("detail", {}).get("captured_expiration") for row in leader_waits]
        joins.append(dict(
            join_id=join_id, peer_id=peer, peer_name=names_by_peer.get(peer),
            leader_peer_id=leader, leader_name=names_by_peer.get(leader), step_id=step_id,
            search_id=detail.get("search_id"), origin=detail.get("origin"),
            start_event_ref=_ref(start), end_event_ref=_ref(end) if end else None,
            selection_event_ref=selection_ref,
            native_client_event_refs=[_ref(row) for row in records],
            native_client_reply_event_refs=[_ref(row) for row in replies],
            reply_codes=[row.get("detail", {}).get("code") for row in replies],
            accepted=any(row.get("detail", {}).get("code") == "ACCEPTED" for row in replies),
            disbanded=any(row.get("detail", {}).get("code") == "GROUP_DISBANDED" for row in replies),
            native_client_request_expiration=(expected_request_expiration if
                                              expected_request_expiration is not None else None),
            caller_request_expiration_values=request_expirations,
            native_client_wait_event_refs=[_ref(row) for row in client_waits],
            native_client_wait_phases=[row.get("detail", {}).get("phase") for row in client_waits],
            selected_leader_expiration=selected_expiration,
            caller_advertised_expiration=caller_ad_expiration,
            admission_leader_expiration=(admission or {}).get("detail", {}).get("leader_expiration"),
            admission_request_expiration=(admission or {}).get("detail", {}).get("request_expiration"),
            admission_state_event_ref=_ref(admission) if admission else None,
            local_leader_wait_captured_expirations=leader_expirations,
            linked_server_rpc_id=linked_rpc_id,
            linked_server_rpc_start_event_ref=_ref(linked_rpc) if linked_rpc else None,
            server_rpc_link_status=("unique" if len(matching_rpcs) == 1 else
                                    "ambiguous" if len(matching_rpcs) > 1 else
                                    "no_request_expiration" if expected_request_expiration is None
                                    else "unmatched"),
            linked_server_event_refs=[_ref(row) for row in server_records],
            unreliable_event_kinds=[row.get("kind") for row in server_records
                                    if row.get("kind", "").startswith("unreliable_")],
            unreliable_visit_records=unreliable_visits,
            local_leader_wait_event_refs=[_ref(row) for row in leader_waits],
        ))
    for join_id in ends.keys() - starts.keys():
        errors.append(f"join end without start: {join_id}")
    for join_id in client.keys() - starts.keys():
        errors.append(f"native client event without join start: {join_id}")
    joins.sort(key=lambda row: (row["start_event_ref"], row["join_id"]))
    return joins


def _publication_rows(events, config, native, peer_ids, errors):
    x_peer = peer_ids.get(X_NAME)
    active = [event for event in events if event.get("kind") == "ad_started" and
              event.get("peer") == x_peer and event.get("detail", {}).get("active") is True]
    finishes = [event for event in events if event.get("kind") in ("ad_finished", "ad_cancelled") and
                event.get("peer") == x_peer and event.get("detail", {}).get("active") is True]
    holders = [event for event in events if event.get("kind") == "holder_store" and
               event.get("detail", {}).get("subkey") == x_peer and
               event.get("detail", {}).get("active") is True]
    rows = []
    search_rows, assigned_publications = _step26._assign_x_searches(
        native, events, {_ref(e): e for e in events}, errors)
    search_by_id = {row.get("search_id"): row for row in search_rows
                    if row.get("peer_id") == x_peer}
    contexts = {search_id: _step29._search_start_context(
        search, events, {_ref(e): e for e in events}, errors)
        for search_id, search in search_by_id.items()}
    used_finish_refs = set()
    for event, search in assigned_publications:
        if event.get("kind") != "ad_started" or event.get("peer") != x_peer or \
                event.get("detail", {}).get("active") is not True:
            continue
        detail = event.get("detail", {})
        expiration = detail.get("expiration")
        next_ad_number = min((row.get("event_number", math.inf) for row in active
                              if row.get("pid") == event.get("pid") and
                              row.get("event_number", -1) > event.get("event_number", math.inf)),
                             default=math.inf)
        candidates = [row for row in finishes if row.get("pid") == event.get("pid") and
                      row.get("event_number", -1) > event.get("event_number", math.inf) and
                      row.get("event_number", math.inf) < next_ad_number and
                      row.get("detail", {}).get("expiration") == expiration and
                      row.get("detail", {}).get("step_id") == detail.get("step_id") and
                      _ref(row) not in used_finish_refs]
        finish = min(candidates, key=lambda row: row["event_number"], default=None)
        if finish is not None:
            used_finish_refs.add(_ref(finish))
        if finish is None:
            errors.append(f"active X publication {_ref(event)} has no finish/cancel record")
        if search is None:
            errors.append(f"active X publication {_ref(event)} lacks an X search")
        matching_stores = [row for row in holders if row.get("other") == x_peer and
                           row.get("detail", {}).get("expiration") == expiration and
                           row.get("monotonic", -math.inf) >= event.get("monotonic", math.inf) and
                           (finish is None or row.get("monotonic", math.inf) <= finish.get("monotonic", -math.inf))]
        rows.append(dict(
            publication_event_ref=_ref(event), publication_monotonic=event.get("monotonic"),
            publication_wall=event.get("wall_time"), pid=event.get("pid"),
            step_id=detail.get("step_id"), search_id=search.get("search_id") if search else None,
            expiration=expiration, active=True,
            finish_event_ref=_ref(finish) if finish else None,
            finish_kind=finish.get("kind") if finish else None,
            declaration_accepted=(finish or {}).get("detail", {}).get("accepted"),
            holder_store_event_refs=[_ref(row) for row in matching_stores],
            accepted_holder_store_count=sum(row.get("detail", {}).get("accepted") is True
                                             for row in matching_stores)))
    rows.sort(key=lambda row: (row.get("publication_monotonic", math.inf), row["publication_event_ref"]))
    all_searches = [row for row in native.get("searches", []) if row.get("peer_id") == x_peer]
    grouped = defaultdict(list)
    for row in rows:
        grouped[row.get("search_id")].append(row)
    for search in all_searches:
        if search.get("search_id") not in grouped:
            errors.append(f"X search {search.get('search_id')} has no active publication")

    case = config.get("case", {})
    publication_policy = case.get("publication_policy")
    policy_search_rows = []
    for search_id, search in search_by_id.items():
        ads = sorted(grouped.get(search_id, []),
                     key=lambda row: row.get("publication_monotonic", math.inf))
        context = contexts.get(search_id)
        audit_errors, intervals = [], []
        previous_expiration = None
        previous_was_deadline_clamped = False
        for row in ads:
            try:
                expiration = finite_number(row.get("expiration"), "active X expiration")
                wall = finite_number(row.get("publication_wall"), "active X publication wall time")
                if context is None:
                    audit_errors.append(f"{row['publication_event_ref']}: missing native search context")
                else:
                    expected = min(max(context["scheduled_time"], wall + float(case["lifetime"])),
                                   context["deadline"])
                    if abs(expiration - expected) > 0.10:
                        audit_errors.append(f"{row['publication_event_ref']}: expiration differs from native formula")
                    deadline = context["deadline"]
                    deadline_epsilon = 1e-6
                    formula_clamped = (expected >= deadline - deadline_epsilon and
                                       abs(expiration - deadline) <= deadline_epsilon)
                    if expiration <= wall and not (formula_clamped and
                                                   wall >= deadline - deadline_epsilon):
                        audit_errors.append(f"{row['publication_event_ref']}: expired send outside terminal deadline clamp")
                    if (previous_expiration is not None and expiration <= previous_expiration and
                            not (formula_clamped and previous_was_deadline_clamped)):
                        audit_errors.append(f"{row['publication_event_ref']}: expiration did not advance before terminal deadline clamp")
                    row["expected_expiration"] = expected
                    row["terminal_deadline_clamp"] = formula_clamped
                    previous_expiration = expiration
                    previous_was_deadline_clamped = formula_clamped
                if row.get("finish_event_ref") is None:
                    audit_errors.append(f"{row['publication_event_ref']}: missing matching finish/cancel")
                elif row.get("finish_kind") == "ad_finished" and not isinstance(
                        row.get("declaration_accepted"), bool):
                    audit_errors.append(f"{row['publication_event_ref']}: ad_finished lacks boolean accepted result")
            except (TypeError, ValueError, KeyError) as exc:
                audit_errors.append(f"{row.get('publication_event_ref')}: {exc}")
        for previous, following in zip(ads, ads[1:]):
            finish = next((event for event in finishes
                           if _ref(event) == previous.get("finish_event_ref")), None)
            try:
                remaining = max(0.0, float(previous["expiration"]) - float(previous["publication_wall"]))
                latency = ((finish or {}).get("detail", {}).get("latency")
                           if finish and finish.get("kind") == "ad_finished" else 0.0)
                write_duration = finite_number(latency or 0.0, "publication write latency")
                expected_gap = max(write_duration, min(remaining, 1.0)
                                   if publication_policy == "renew" else remaining)
                gap = float(following["publication_monotonic"]) - float(previous["publication_monotonic"])
                if gap + 0.35 < expected_gap:
                    audit_errors.append(f"{search_id}: publication cadence early ({gap:.3f}s < {expected_gap:.3f}s)")
                intervals.append(dict(previous_publication_event_ref=previous["publication_event_ref"],
                                      next_publication_event_ref=following["publication_event_ref"],
                                      observed_interval_seconds=gap,
                                      expected_interval_seconds=expected_gap))
            except (TypeError, ValueError, KeyError) as exc:
                audit_errors.append(f"{search_id}: invalid cadence metadata: {exc}")
        errors.extend(f"publication: {error}" for error in audit_errors)
        policy_search_rows.append(dict(search_id=search_id, step_id=search.get("step_id"),
                                       active_publication_count=len(ads),
                                       accepted_active_publication_count=sum(
                                           row.get("declaration_accepted") is True for row in ads),
                                       median_interval_seconds=_median(
                                           row["observed_interval_seconds"] for row in intervals),
                                       intervals=intervals, errors=audit_errors,
                                       search_deadline=context.get("deadline") if context else None))

    intervals_by_search = defaultdict(list)
    for search_id, ads in grouped.items():
        ordered = sorted(ads, key=lambda row: row.get("publication_monotonic", math.inf))
        for left, right in zip(ordered, ordered[1:]):
            intervals_by_search[search_id].append(
                right["publication_monotonic"] - left["publication_monotonic"])
    intervals = [value for values in intervals_by_search.values() for value in values]
    x_searches = [row for row in native.get("searches", []) if row.get("peer_id") == x_peer]
    accepted_multi = []
    cadence_rows = []
    for search in x_searches:
        ads = grouped.get(search.get("search_id"), [])
        accepted = [row for row in ads if row.get("declaration_accepted") is True]
        if len(accepted) >= 2:
            accepted_multi.append(search)
        gaps = intervals_by_search.get(search.get("search_id"), [])
        cadence_rows.append(dict(search_id=search.get("search_id"), step_id=search.get("step_id"),
                                 start_event_ref=search.get("start_event_ref"),
                                 end_event_ref=search.get("end_event_ref"),
                                 active_publication_count=len(ads),
                                 accepted_active_publication_count=len(accepted),
                                 publication_event_refs=[row["publication_event_ref"] for row in ads],
                                 publication_intervals_seconds=gaps,
                                 median_interval_seconds=_median(gaps)))
    summary = dict(x_search_count=len(x_searches), active_publication_count=len(rows),
                   accepted_active_publication_count=sum(row.get("declaration_accepted") is True
                                                        for row in rows),
                   accepted_holder_store_count=sum(row.get("accepted_holder_store_count", 0)
                                                   for row in rows),
                   active_holder_store_count=sum(len(row.get("holder_store_event_refs", []))
                                                 for row in rows),
                   searches_with_multiple_accepted_publications=len(accepted_multi),
                   publication_interval_median_seconds=_median(intervals),
                   publication_interval_count=len(intervals),
                   by_search=cadence_rows,
                   policy_audit=dict(publication_policy=publication_policy,
                                     lifetime_formula="min(max(scheduled_time, publication_wall_time + minimum), search_deadline)",
                                     cadence_rule=("max(write_latency, min(remaining_lifetime, 1s))"
                                                   if publication_policy == "renew"
                                                   else "max(write_latency, remaining_lifetime)"),
                                     searches=policy_search_rows),
                   holder_store_records=[_ref(event) for event in holders])
    return rows, summary


def _group_by_kind(events):
    out = defaultdict(list)
    for event in events:
        out[event.get("kind")].append(event)
    return out


def _retry_rows(native, events, by_ref, peer_ids, groups, attempts):
    rows = _step28._retry_rows(native, events, by_ref, peer_ids, groups)
    attempt_by_key = {(row.get("peer_id"), row.get("step_id")): row for row in attempts}
    for row in rows:
        attempt = attempt_by_key.get((row.get("peer_id"), row.get("step_id")))
        first = attempt.get("first_assembly_monotonic") if attempt else None
        selection = by_ref.get(row.get("selection_event_ref"))
        when = selection.get("monotonic") if selection else None
        row["same_search_exact_pair_retry"] = True
        row["selection_monotonic"] = when
        row["first_assembly_monotonic"] = first
        row["relative_to_first_assembly"] = (
            "before_first_assembly" if first is None or when < first else
            "after_first_assembly" if when > first else "at_first_assembly")
    return rows


def _step_outcomes(events, config, errors):
    starts, results = {}, {}
    for event in events:
        detail = event.get("detail", {})
        if event.get("kind") in ("step_start", "step_result"):
            key = (event.get("peer"), detail.get("step_id"))
            target = starts if event["kind"] == "step_start" else results
            if key in target or not key[0] or not key[1]:
                errors.append(f"duplicate or invalid {event['kind']} {key}")
            target[key] = event
    if starts.keys() != results.keys():
        missing_results = sorted(starts.keys() - results.keys())
        extra_results = sorted(results.keys() - starts.keys())
        errors.append(f"step_start/step_result mismatch (missing={missing_results[:3]}, extra={extra_results[:3]})")
    summary = read_json(Path(config.get("_trial_path", ".")) / "summary.json") if config.get("_trial_path") else {}
    if summary.get("error") is not None or summary.get("pending"):
        errors.append("trial summary reports error or pending controls")
    outcome_by_step = {row.get("step_id"): row for row in summary.get("outcomes", [])}
    if len(outcome_by_step) != len(summary.get("outcomes", [])):
        errors.append("summary.json has duplicate outcome step IDs")
    raw_step_ids = {step_id for peer, step_id in starts}
    if set(outcome_by_step) != raw_step_ids:
        errors.append("summary.json outcome IDs do not exactly match raw step starts")
    for key, event in starts.items():
        detail = event.get("detail", {})
        name = detail.get("name")
        sequence = detail.get("sequence")
        if name != next((n for n, p in config.get("peer_ids", {}).items() if p == event.get("peer")), None):
            errors.append(f"step {key[1]} participant name differs from peer identity")
        if not isinstance(sequence, int) or detail.get("step_id") != f"{name}:{sequence}":
            errors.append(f"step identity/sequence differs for {key[1]}")
        expected_value = float((7 if name == X_NAME else int(name[1:])) + 10 * sequence) if name else None
        if detail.get("value") != expected_value:
            errors.append(f"step {key[1]} value differs from deterministic workload")
        result = results.get(key)
        outcome = outcome_by_step.get(detail.get("step_id"))
        if result:
            rd = result.get("detail", {})
            if rd.get("control_only") is not True or rd.get("tensor_averaging_performed") is not False:
                errors.append(f"step_result {key[1]} lacks metadata-only completion markers")
            if any(field in rd for field in ("actual", "expected", "correct")):
                errors.append(f"step_result {key[1]} includes tensor correctness fields")
            if outcome is not None:
                if outcome.get("status") != rd.get("status"):
                    errors.append(f"summary outcome {key[1]} status differs from raw step_result")
                if outcome.get("members") != rd.get("members"):
                    errors.append(f"summary outcome {key[1]} members differ from raw step_result")
        if detail.get("step_id") not in outcome_by_step:
            errors.append(f"summary.json omits step outcome {key[1]}")
    return starts, results, summary


def _validate_training_schedule(events, starts, results, config, errors):
    seed = config.get("seed")
    if not isinstance(seed, int):
        return
    name_index = {"X": 0, **{f"H{i}": i + 1 for i in range(7)}}
    by_peer = defaultdict(list)
    for event in events:
        if event.get("kind") == "training_scheduled":
            by_peer[event.get("peer")].append(event)
    names_by_peer = {peer: name for name, peer in config.get("peer_ids", {}).items()}
    sigma = float(_step28._training_delay()["sigma"])
    mu = float(_step28._training_delay()["mu"])
    for peer, name in names_by_peer.items():
        scheduled = sorted(by_peer.get(peer, []), key=_event_order)
        rng = random.Random(seed * 100 + name_index[name])
        prior_sequence = 0
        for event in scheduled:
            d = event.get("detail", {})
            step_id = d.get("step_id")
            try:
                sequence = int(d.get("sequence"))
                delay = finite_number(d.get("delay"), "training delay")
                ready = finite_number(d.get("ready"), "training ready")
                finished = finite_number(d.get("finished"), "training finished")
            except (TypeError, ValueError) as exc:
                errors.append(f"training schedule {step_id}: {exc}")
                continue
            expected = min(4.0, max(0.1, rng.lognormvariate(mu, sigma)))
            if sequence != prior_sequence + 1 or step_id != f"{name}:{sequence}":
                errors.append(f"training schedule sequence is not consecutive for {name}: {step_id}")
            if not math.isclose(delay, expected, rel_tol=0.0, abs_tol=1e-9):
                errors.append(f"training delay draw differs from seed for {name}:{sequence}")
            if not math.isclose(ready, finished + delay, rel_tol=0.0, abs_tol=1e-8):
                errors.append(f"training ready time differs from finish+delay for {name}:{sequence}")
            next_step_id = f"{name}:{sequence + 1}"
            next_start = starts.get((peer, next_step_id))
            if next_start and next_start.get("monotonic", -math.inf) + 1e-9 < ready:
                errors.append(f"step {next_step_id} began before its training delay ended")
            prior_sequence = sequence
    # Every step after the first must be explained by exactly one seeded schedule.
    scheduled_ids = {event.get("detail", {}).get("step_id") for rows in by_peer.values() for event in rows}
    for (peer, step_id), event in starts.items():
        detail = event.get("detail", {})
        sequence = detail.get("sequence", 0)
        previous_id = f"{detail.get('name')}:{sequence - 1}"
        if sequence > 1 and previous_id not in scheduled_ids:
            errors.append(f"noninitial step {step_id} has no seeded prior-step training schedule")


def _x_states(events, native, attempts, config, peer_ids):
    x_peer = peer_ids.get(X_NAME)
    searches = [row for row in native.get("searches", []) if row.get("peer_id") == x_peer]
    attempts_by_id = {row.get("step_id"): row for row in attempts if row.get("peer_name") == X_NAME}
    by_step = defaultdict(list)
    for search in searches:
        duration = (search.get("end_monotonic") - search.get("start_monotonic")
                   if search.get("end_monotonic") is not None else None)
        by_step[search.get("step_id")].append(dict(
            peer_name=X_NAME, step_id=search.get("step_id"), search_id=search.get("search_id"),
            start_event_ref=search.get("start_event_ref"), end_event_ref=search.get("end_event_ref"),
            search_start_monotonic=search.get("start_monotonic"),
            search_end_monotonic=search.get("end_monotonic"), searching_seconds=duration))
    step_starts = [row for row in attempts if row.get("peer_name") == X_NAME]
    step_starts.sort(key=lambda row: row.get("started_monotonic", math.inf))
    scheduled = {event.get("detail", {}).get("step_id"): event for event in events
                 if event.get("kind") == "training_scheduled" and event.get("peer") == x_peer}
    state_rows = []
    for index, attempt in enumerate(step_starts):
        step_id = attempt.get("step_id")
        next_attempt = step_starts[index + 1] if index + 1 < len(step_starts) else None
        result_time = attempt.get("step_result_monotonic")
        gap = (next_attempt["started_monotonic"] - result_time
               if next_attempt and result_time is not None else None)
        training = scheduled.get(step_id)
        state_rows.append(dict(
            peer_name=X_NAME, step_id=step_id, step_start_event_ref=attempt.get("start_event_ref"),
            step_start_monotonic=attempt.get("started_monotonic"),
            step_result_monotonic=result_time,
            training_delay_seconds=(training or {}).get("detail", {}).get("delay"),
            training_ready_monotonic=(training or {}).get("detail", {}).get("ready"),
            next_step_id=next_attempt.get("step_id") if next_attempt else None,
            between_rounds_seconds=gap,
            search_count=len(by_step.get(step_id, [])),
            searching_seconds=sum(row.get("searching_seconds") or 0.0 for row in by_step.get(step_id, [])),
            searches=by_step.get(step_id, []),
            x_led_group=attempt.get("first_lead_group_id")))
    return state_rows


def _window_metrics(attempts, groups, config, started, duration):
    cutoff = started + duration
    windows = []
    begin = 0.0
    while begin < duration:
        end = min(begin + 60.0, duration)
        absolute_start, absolute_end = started + begin, started + end
        rows = [row for row in attempts if row.get("peer_name") in HONEST_NAMES and
                absolute_start <= row.get("started_monotonic", math.inf) < absolute_end]
        successes = [row for row in rows if row.get("membership_success")]
        local_assemblies = [row for row in attempts if row.get("peer_name") in HONEST_NAMES and
                            row.get("membership_success") and
                            absolute_start <= row.get("first_assembly_monotonic", math.inf) < absolute_end]
        groups_in = [row for row in groups if absolute_start <= row.get("assembly_monotonic", math.inf) < absolute_end]
        honest_leader_groups_in = [row for row in groups_in if row.get("leader_name") in HONEST_NAMES]
        failures = len(rows) - len(successes)
        windows.append(dict(
            window_index=int(begin // 60), start_offset_seconds=begin, end_offset_seconds=end,
            start_monotonic=absolute_start, end_monotonic=absolute_end,
            honest_submitted_attempts=len(rows), honest_group_successes=len(successes),
            honest_group_failures=failures,
            honest_group_failure_fraction=failures / len(rows) if rows else None,
            honest_mean_success_wait_seconds=_mean(row.get("first_assembly_latency_seconds")
                                                   for row in successes),
            honest_mean_capped_wait_seconds=_mean(row.get("capped_latency_seconds") for row in rows),
            total_groups=len(groups_in),
            honest_group_rate_per_minute=(len(groups_in) / ((end - begin) / 60.0)
                                          if end > begin else None),
            honest_first_local_assemblies=len(local_assemblies),
            honest_first_local_assemblies_per_minute=(len(local_assemblies) / ((end - begin) / 60.0)
                                                       if end > begin else None),
            honest_leader_group_count=len(honest_leader_groups_in),
            honest_leader_mean_roster_size=_mean(
                row.get("roster_size") or len(row.get("roster", []))
                for row in honest_leader_groups_in),
            group_event_refs=[row.get("assembly_event_ref") for row in groups_in]))
        begin = end
    drain = [row for row in groups if row.get("assembly_monotonic", math.inf) >= cutoff]
    return windows, drain


def _summary_metrics(attempts, groups, joins, retries, publications, windows,
                     drain_groups, rpc, config, started, duration, group_completion):
    honest = [row for row in attempts if row.get("peer_name") in HONEST_NAMES]
    successes = [row for row in honest if row.get("membership_success")]
    cutoff = started + duration
    observation_groups = [row for row in groups if started <= row.get("assembly_monotonic", -math.inf) < cutoff]
    honest_leader_groups = [row for row in observation_groups if row.get("leader_name") in HONEST_NAMES]
    observation_local_assemblies = [row for row in successes if
                                    started <= row.get("first_assembly_monotonic", -math.inf) < cutoff]
    accepts = [row for row in joins if row.get("leader_name") == X_NAME and row.get("accepted")]
    disbands = [row for row in joins if row.get("leader_name") == X_NAME and row.get("disbanded")]
    x_attempts = [row for row in attempts if row.get("peer_name") == X_NAME]
    failures = len(honest) - len(successes)
    cap_vals = [min(CAP_SECONDS, max(0.0, float(row.get("capped_latency_seconds", CAP_SECONDS))))
                for row in honest]
    actual_wait = [row.get("first_assembly_latency_seconds") for row in successes]
    multi_searches = publications.get("searches_with_multiple_accepted_publications", 0)
    x_search_count = publications.get("x_search_count", 0)
    return dict(
        case_name=config.get("case", {}).get("name"), seed=config.get("seed"),
        duration_seconds=duration, observation_window_start_monotonic=started,
        observation_window_end_monotonic=cutoff,
        attempt_denominator="every raw step_start; cutoff starts are retained",
        honest_submitted_attempts=len(honest), honest_group_successes=len(successes),
        honest_group_failures=failures,
        honest_group_failure_fraction=failures / len(honest) if honest else None,
        honest_group_success_fraction=len(successes) / len(honest) if honest else None,
        honest_mean_success_wait_seconds=_mean(actual_wait),
        honest_success_wait_count=len(actual_wait),
        honest_mean_capped_wait_seconds=_mean(cap_vals),
        honest_capped_wait_cap_seconds=CAP_SECONDS,
        honest_capped_wait_definition="time from each raw step_start to the first successful local assembled/follower_assembled observation for that exact peer/step; missing group scores 12s",
        total_groups=len(groups), observation_groups=len(observation_groups),
        drain_group_count=len(drain_groups),
        follower_corroborated_groups=sum(row.get("follower_corroborated") is True for row in groups),
        uncorroborated_groups=sum(row.get("follower_corroborated") is not True for row in groups),
        honest_leader_group_count=len(honest_leader_groups),
        honest_leader_assembled_mean_roster_size=_mean(
            row.get("member_count") or len(row.get("roster", []))
            for row in honest_leader_groups),
        metadata_completion_count=sum(row.get("metadata_completion_count", 0) for row in groups),
        honest_first_local_assemblies=len(observation_local_assemblies),
        honest_first_local_assemblies_per_minute=(len(observation_local_assemblies) /
                                                  (duration / 60.0) if duration else None),
        honest_group_rate_per_minute=(len(observation_groups) / (duration / 60.0)
                                      if duration else None),
        groups_per_minute_definition="unique native leader assemblies in [trial start, submission cutoff), divided by fixed observation minutes; roster size is reported separately",
        x_submitted_attempts=len(x_attempts),
        x_assembled_leader_groups=sum(row.get("leader_name") == X_NAME for row in groups),
        x_outbound_join_attempts=sum(row.get("peer_name") == X_NAME for row in joins),
        x_search_count=x_search_count,
        x_searches_with_multiple_accepted_publications=multi_searches,
        x_client_accept_join_count=len({row["join_id"] for row in accepts}),
        x_client_disband_join_count=len({row["join_id"] for row in disbands}),
        x_client_accept_reply_count=sum(row.get("leader_name") == X_NAME and
                                        "ACCEPTED" in row.get("reply_codes", []) for row in joins),
        x_client_disband_reply_count=sum(row.get("leader_name") == X_NAME and
                                         "GROUP_DISBANDED" in row.get("reply_codes", []) for row in joins),
        honest_first_outgoing_choice_counts=dict(
            X=sum(row.get("first_outgoing_choice") == "X" for row in honest),
            other=sum(row.get("first_outgoing_choice") == "other" for row in honest),
            none=sum(row.get("first_outgoing_choice") == "none" for row in honest)),
        honest_ever_tried_x_attempts=sum(row.get("ever_tried_x") is True for row in honest),
        same_search_x_retry_count=len(retries),
        retry_relative_to_assembly_counts=dict(Counter(row.get("relative_to_first_assembly")
                                                        for row in retries)),
        x_active_publication_count=publications.get("active_publication_count", 0),
        x_accepted_active_publication_count=publications.get("accepted_active_publication_count", 0),
        x_accepted_holder_store_count=publications.get("accepted_holder_store_count", 0),
        x_publication_interval_median_seconds=publications.get("publication_interval_median_seconds"),
        rpc_counts=rpc.get("summary", {}).get("rpc_counts", {}),
        delayed_rpc_coverage=rpc.get("summary", {}).get("rpc_coverage", {}),
        clock_integrity=config.get("clock_integrity"),
    )


def _coverage_summary(metrics, publications, config, duration):
    if duration < 60:
        return dict(required=False, adequate=None, reason="smoke exposure; confirmation thresholds do not apply",
                    checks={})
    checks = dict(honest_starts_at_least_100=metrics.get("honest_submitted_attempts", 0) >= 100,
                  x_client_accepts_at_least_20=metrics.get("x_client_accept_join_count", 0) >= 20,
                  x_searches_with_two_accepted_publications_at_least_10=
                  publications.get("searches_with_multiple_accepted_publications", 0) >= 10)
    return dict(required=True, adequate=all(checks.values()), checks=checks)


def analyse_trial(path) -> dict:
    path = Path(path)
    errors = []
    config = read_json(path / "config.json")
    config_for_helpers = dict(config)
    config_for_helpers["_trial_path"] = str(path)
    case, peer_ids, seed, duration, started = _validate_config(config, errors)
    events = _read_events(path)
    if not events:
        errors.append("raw event trace is empty")
    by_ref = {}
    for event in events:
        try:
            ref = _ref(event)
            if ref in by_ref:
                errors.append(f"duplicate raw event ref {ref}")
            by_ref[ref] = event
            if not isinstance(event.get("monotonic"), (int, float)) or not math.isfinite(event["monotonic"]):
                errors.append(f"invalid event monotonic at {ref}")
            if event.get("peer") not in ("", *peer_ids.values()):
                errors.append(f"unknown event peer at {ref}")
        except (KeyError, TypeError) as exc:
            errors.append(f"malformed raw event: {exc}")
    closed = [event for event in events if event.get("kind") == "trace_closed"]
    if (len(closed) != 1 or not events or events[-1].get("kind") != "trace_closed" or
            closed and (closed[0].get("peer") != "" or closed[0].get("other") != "" or
                        set(closed[0].get("detail", {}).get("participants", [])) !=
                        {X_NAME, *HONEST_NAMES} or closed[0].get("detail", {}).get("pending") != [] or
                        closed[0].get("detail", {}).get("error") is not None)):
        errors.append("trace must end with one clean trace_closed marker for all peers")

    # Pure raw-event reconstruction intentionally skips Step 24 tensor checks.
    try:
        native = _step25.reconstruct_trial(config, events)
    except Exception as exc:
        native = dict(trace_integrity_errors=[f"Step 25 raw reconstruction: {type(exc).__name__}: {exc}"],
                      attempts=[], groups=[], searches=[], selections=[], joins=[], reads=[])
    errors.extend(native.get("trace_integrity_errors", []))
    _, publication_runtime, _ = _runtime_integrity(config, events, case, peer_ids, errors)
    clock = _step28._clock_integrity(events, errors, required=True)

    groups = _step28._group_rows(native, events, by_ref, config, peer_ids, errors)
    attempts = _step28._attempt_rows(native, groups, config, peer_ids, events, by_ref, errors)
    for row in attempts:
        row["step_result_monotonic"] = native_attempt_result(native, row)
        row["submission_started"] = (started is not None and duration is not None and
                                     started <= row.get("started_monotonic", -math.inf) < started + duration)
        row["late_start_after_submission_cutoff"] = (started is not None and duration is not None and
                                                     row.get("started_monotonic", -math.inf) >= started + duration)
    _actual_membership(events, attempts, groups, peer_ids, errors)
    _step28._annotate_attempt_choices(attempts, native, groups, peer_ids, by_ref)
    # Keep successful completion separate from membership: X is intentionally
    # unreliable, so accepted followers are released without X assembling.
    groups.sort(key=lambda row: (row.get("assembly_monotonic", math.inf), row.get("group_id", "")))
    attempts.sort(key=lambda row: (row.get("started_monotonic", math.inf), row.get("peer_id", ""),
                                   row.get("step_id", "")))
    choice_rows = _choice_rows(attempts)
    joins = _link_joins(events, config, native, errors)
    retries = _retry_rows(native, events, by_ref, peer_ids, groups, attempts)
    hold_errors = []
    holds = _step28._hold_rows(events, peer_ids, case, hold_errors, native, by_ref)
    errors.extend(f"hold: {error}" for error in hold_errors)
    publications, pub_summary = _publication_rows(events, config, native, peer_ids, errors)

    starts, results, trial_summary = _step_outcomes(events, config_for_helpers, errors)
    _validate_training_schedule(events, starts, results, config, errors)
    group_completion = _completion_integrity(events, config_for_helpers, native, errors)
    for group in groups:
        group.update(group_completion.get(group.get("group_id"), {}))
    rpc = _rpc_integrity(config, events, peer_ids, native, errors)
    _validate_actor_behavior(events, case, peer_ids, errors)
    windows, drain_groups = _window_metrics(attempts, groups, config, started, duration)
    metrics = _summary_metrics(attempts, groups, joins, retries, pub_summary,
                               windows, drain_groups, rpc, config, started, duration,
                               group_completion)
    metrics["clock_integrity"] = clock
    metrics["publication_integrity"] = pub_summary
    metrics["rpc_integrity"] = rpc.get("summary", {})
    metrics["control_only_integrity"] = dict(
        metadata_completion_count=len([e for e in events if e.get("kind") == "control_round_complete"]),
        tensor_transfer_enabled=config.get("tensor_transfer_enabled"),
        tensor_rpc_count=len([e for e in events if e.get("kind") == "forbidden_tensor_rpc"]),
        follower_corroborated_groups=metrics.get("follower_corroborated_groups"))
    coverage = _coverage_summary(metrics, pub_summary, config, duration)
    metrics["exposure_coverage"] = coverage

    integrity_errors = sorted(set(errors))
    return dict(
        experiment="step30", trial_dir=path.name, case=case, seed=seed, duration=duration,
        integrity_pass=not integrity_errors, integrity_errors=integrity_errors,
        trace_integrity_pass=not native.get("trace_integrity_errors"),
        clock_integrity=clock, runtime_publication_integrity_pass=not any(
            "runtime publication" in error for error in errors),
        delay_integrity=dict(pass_=rpc.get("pass_") is True,
                             summary=rpc.get("summary", {})),
        publication_integrity=dict(pass_=not any("publication" in error for error in errors),
                                   summary=pub_summary),
        summary_metrics=metrics, attempts=attempts, groups=groups, choices=choice_rows,
        joins=joins, retries=retries, holds=holds, publications=publications,
        windows=windows, states=_x_states(events, native, attempts, config, peer_ids),
    )


def native_attempt_result(native, attempt):
    for row in native.get("attempts", []):
        if (row.get("peer_id") == attempt.get("peer_id") and
                row.get("step_id") == attempt.get("step_id")):
            return row.get("step_result_monotonic")
    return None


def _validate_actor_behavior(events, case, peer_ids, errors):
    x_peer = peer_ids.get(X_NAME)
    for kind in ("forbidden_attacker_read", "forbidden_attacker_join"):
        hits = [event for event in events if event.get("kind") == kind]
        if hits:
            errors.append(f"X emitted {len(hits)} forbidden {kind} marker(s)")
    x_reads = [event for event in events if event.get("kind") == "ad_read_start" and
               event.get("peer") == x_peer]
    if x_reads:
        errors.append(f"recruit-only X performed {len(x_reads)} advertisement reads")
    x_joins = [event for event in events if event.get("kind") == "join_attempt_start" and
               event.get("peer") == x_peer]
    if x_joins:
        errors.append(f"recruit-only X made {len(x_joins)} outbound joins")
    x_assemblies = [event for event in events if event.get("kind") == "assembled" and
                    event.get("peer") == x_peer]
    if x_assemblies:
        errors.append(f"unreliable X assembled {len(x_assemblies)} groups")
    x_pops = [event for event in events if event.get("kind") == "native_pop_selected" and
              event.get("peer") == x_peer]
    if x_pops:
        errors.append(f"recruit-only X selected {len(x_pops)} competitor advertisements")


def _choice_rows(attempts):
    fields = ("peer_name", "peer_id", "step_id", "start_event_ref", "started_monotonic",
              "submission_started", "first_outgoing_choice", "first_outgoing_join_id",
              "first_outgoing_join_event_ref", "first_outgoing_join_peer_id",
              "first_outgoing_join_peer_name", "accepted_x_reply_count",
              "accepted_other_reply_count", "accepted_x_reply_event_refs",
              "accepted_other_reply_event_refs", "ever_tried_x",
              "first_group_leader_category", "first_group_leader_peer_id",
              "first_group_leader_name", "first_group_event_ref",
              "first_assembly_group_id", "first_assembly_event_ref",
              "first_assembly_latency_seconds", "membership_success")
    return [{key: row.get(key) for key in fields} for row in attempts
            if row.get("peer_name") in HONEST_NAMES]


def _summary_trial_row(trial, phase):
    case = trial.get("case", {})
    metrics = dict(trial.get("summary_metrics", {}))
    for key, value in (("seed", trial.get("seed")),
                       ("case_name", case.get("name")),
                       ("duration_seconds", trial.get("duration"))):
        if key in metrics and metrics[key] != value:
            raise ValueError(f"trial summary metric {key} conflicts with trial metadata")
    row = dict(trial_dir=trial.get("trial_dir"), phase=phase, case=case.get("name"),
               publication_policy=case.get("publication_policy"), seed=trial.get("seed"),
               duration=trial.get("duration"), integrity_pass=trial.get("integrity_pass"))
    row.update(metrics)
    return row


def _csv_cell(value):
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return value


def _write_csv(path, rows):
    rows = list(rows)
    fields, seen = [], set()
    for row in rows:
        for key in row:
            if key not in seen:
                fields.append(key)
                seen.add(key)
    with Path(path).open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _csv_cell(row.get(key)) for key in fields})


def _prefix_rows(trial, phase, name, rows):
    prefix = dict(trial_dir=trial["trial_dir"], phase=phase, case=name,
                  publication_policy=trial.get("case", {}).get("publication_policy"),
                  seed=trial.get("seed"))
    # Prefix wins on collisions so raw rows cannot spoof their suite identity.
    return [{**row, **prefix} for row in rows]


def _paired_contrasts(trials):
    by_seed = defaultdict(dict)
    for trial in trials:
        by_seed[trial.get("seed")][trial.get("case", {}).get("name")] = trial
    outcomes = list(HARM_THRESHOLDS)
    rows = []
    for seed, cases in sorted(by_seed.items()):
        stock, renew = cases.get("stock_2s"), cases.get("renew_1_2")
        if not stock or not renew:
            continue
        a, b = stock["summary_metrics"], renew["summary_metrics"]
        stock_rate = a.get("honest_group_rate_per_minute")
        renew_rate = b.get("honest_group_rate_per_minute")
        reduction = (1.0 - renew_rate / stock_rate) if stock_rate else None
        rows.extend([
            dict(seed=seed, outcome="honest_mean_capped_wait_seconds",
                 stock_value=a.get("honest_mean_capped_wait_seconds"),
                 renew_value=b.get("honest_mean_capped_wait_seconds"),
                 renew_minus_stock=(b.get("honest_mean_capped_wait_seconds") -
                                    a.get("honest_mean_capped_wait_seconds"))
                 if a.get("honest_mean_capped_wait_seconds") is not None and
                 b.get("honest_mean_capped_wait_seconds") is not None else None,
                 harmful_threshold=HARM_THRESHOLDS["honest_mean_capped_wait_seconds"]),
            dict(seed=seed, outcome="honest_group_failure_fraction",
                 stock_value=a.get("honest_group_failure_fraction"),
                 renew_value=b.get("honest_group_failure_fraction"),
                 renew_minus_stock=(b.get("honest_group_failure_fraction") -
                                    a.get("honest_group_failure_fraction"))
                 if a.get("honest_group_failure_fraction") is not None and
                 b.get("honest_group_failure_fraction") is not None else None,
                 harmful_threshold=HARM_THRESHOLDS["honest_group_failure_fraction"]),
            dict(seed=seed, outcome="honest_group_rate_reduction_fraction",
                 stock_value=stock_rate, renew_value=renew_rate,
                 renew_minus_stock=(renew_rate - stock_rate) if stock_rate is not None and
                 renew_rate is not None else None,
                 renew_reduction_fraction=reduction,
                 harmful_threshold=HARM_THRESHOLDS["honest_group_rate_reduction_fraction"]),
        ])
    return rows


def _decision(trials, paired_rows):
    confirm_trials = [trial for trial in trials if trial.get("duration") == 300.0]
    seeds = sorted({trial.get("seed") for trial in confirm_trials})
    result = {}
    mapping = {
        "honest_mean_capped_wait_seconds": ("renew_minus_stock", "positive"),
        "honest_group_failure_fraction": ("renew_minus_stock", "positive"),
        "honest_group_rate_reduction_fraction": ("renew_reduction_fraction", "positive"),
    }
    for outcome, (field, _) in mapping.items():
        threshold = HARM_THRESHOLDS[outcome]
        rows_by_seed = {row["seed"]: row for row in paired_rows if row.get("outcome") == outcome}
        effects = [rows_by_seed[seed].get(field) for seed in seeds if seed in rows_by_seed and
                   rows_by_seed[seed].get(field) is not None]
        harmful = [value >= threshold for value in effects]
        directional = [value > 0 for value in effects]
        result[outcome] = dict(threshold=threshold, paired_seed_effects=effects,
                               seeds_meeting_threshold=sum(harmful),
                               seeds_same_harm_direction=sum(directional),
                               equally_weighted_mean_effect=_mean(effects),
                               equally_weighted_mean_meets_threshold=(
                                   _mean(effects) is not None and _mean(effects) >= threshold),
                               material_signal=(sum(harmful) >= 2 and len(directional) == 3 and
                                                all(directional) and _mean(effects) is not None and
                                                _mean(effects) >= threshold))
    expected_pairs = {(seed, name) for seed in (3001, 3002, 3003)
                      for name in ("stock_2s", "renew_1_2")}
    observed_pairs = [(trial.get("seed"), trial.get("case", {}).get("name"))
                      for trial in confirm_trials]
    cadence_rows = []
    by_seed = defaultdict(dict)
    for trial in confirm_trials:
        by_seed[trial.get("seed")][trial.get("case", {}).get("name")] = trial
    for seed in (3001, 3002, 3003):
        stock = by_seed.get(seed, {}).get("stock_2s", {})
        renew = by_seed.get(seed, {}).get("renew_1_2", {})
        stock_interval = stock.get("summary_metrics", {}).get("x_publication_interval_median_seconds")
        renew_interval = renew.get("summary_metrics", {}).get("x_publication_interval_median_seconds")
        ratio = (renew_interval / stock_interval if stock_interval and renew_interval is not None else None)
        cadence_rows.append(dict(seed=seed, stock_median_interval_seconds=stock_interval,
                                 renewal_median_interval_seconds=renew_interval,
                                 renewal_to_stock_ratio=ratio,
                                 within_75_percent=(ratio is not None and ratio <= .75)))
    cadence_covered = len(cadence_rows) == 3 and all(row["within_75_percent"] for row in cadence_rows)
    enough_trials = (set(observed_pairs) == expected_pairs and len(observed_pairs) == 6 and
                     len(seeds) == 3 and all(
        trial.get("integrity_pass") is True and
        trial.get("summary_metrics", {}).get("exposure_coverage", {}).get("adequate") is True
        for trial in confirm_trials) and cadence_covered)
    result["coverage_sufficient"] = enough_trials
    result["confirm_pair_count"] = len(observed_pairs)
    result["expected_confirm_pair_count"] = 6
    result["publication_cadence_coverage"] = dict(rule="renewal median interval <= 0.75 * stock median interval in every paired seed",
                                                   covered=cadence_covered, by_seed=cadence_rows)
    if not enough_trials:
        result["conclusion"] = "insufficient exposure or incomplete paired trials; no negative claim"
    elif any(result[key]["material_signal"] for key in mapping):
        result["conclusion"] = "predeclared material signal observed; review before further experiments"
    else:
        result["conclusion"] = "no material renewal advantage observed under the tested configuration"
    result["persistence"] = _persistence(trials)
    return result


def _persistence(trials):
    by_seed_case = defaultdict(dict)
    for trial in trials:
        if trial.get("duration") == 300.0:
            by_seed_case[trial.get("seed")][trial.get("case", {}).get("name")] = trial
    outcomes = ("honest_mean_capped_wait_seconds", "honest_group_failure_fraction",
                "honest_group_rate_per_minute")
    result = {}
    for outcome in outcomes:
        by_seed_counts = {}
        for seed, cases in by_seed_case.items():
            if not {"stock_2s", "renew_1_2"}.issubset(cases):
                continue
            a = {row["window_index"]: row for row in cases["stock_2s"].get("windows", [])}
            b = {row["window_index"]: row for row in cases["renew_1_2"].get("windows", [])}
            same = 0
            for index in (1, 2, 3, 4):
                if index not in a or index not in b:
                    continue
                av, bv = a[index].get(outcome), b[index].get(outcome)
                if av is None or bv is None:
                    continue
                if (bv > av if outcome != "honest_group_rate_per_minute" else bv < av):
                    same += 1
            by_seed_counts[str(seed)] = same
        result[outcome] = dict(seed_later_window_same_direction_counts=by_seed_counts,
                               seeds_persistent=sum(value >= 3 for value in by_seed_counts.values()),
                               persistent=(sum(value >= 3 for value in by_seed_counts.values()) >= 2))
    return result


def _smoke_gate(trials):
    if len(trials) != 2 or {trial.get("case", {}).get("name") for trial in trials} != \
            {"stock_2s", "renew_1_2"}:
        return dict(pass_=False, errors=["smoke gate requires exactly both smoke arms"])
    errors = []
    for trial in trials:
        name = trial.get("case", {}).get("name")
        m = trial.get("summary_metrics", {})
        pub = trial.get("publications", [])
        if not trial.get("integrity_pass"):
            errors.append(f"{name}: analyzer integrity failed")
        if not pub or not any(row.get("declaration_accepted") is True for row in pub):
            errors.append(f"{name}: no accepted active X publication")
        if m.get("x_searches_with_multiple_accepted_publications", 0) < 1:
            errors.append(f"{name}: no X search has two accepted active publications")
        if not any(row.get("accepted") for row in trial.get("joins", [])) or \
                not any(row.get("disbanded") for row in trial.get("joins", [])):
            errors.append(f"{name}: smoke did not capture both X acceptance and disband receipt")
        if m.get("honest_group_successes", 0) <= 0 or m.get("follower_corroborated_groups", 0) <= 0:
            errors.append(f"{name}: no honest group with follower metadata corroboration")
        coverage = m.get("delayed_rpc_coverage", {})
        if any(coverage.get(kind, {}).get("correlated_delayed", 0) <= 0
               for kind in ("find", "store", "join")):
            errors.append(f"{name}: missing delayed find/store/join RPC leg")
        if m.get("x_assembled_leader_groups") != 0:
            errors.append(f"{name}: unreliable X assembled a group")
        if m.get("control_only_integrity", {}).get("tensor_rpc_count") != 0:
            errors.append(f"{name}: tensor RPC observed")
        if trial.get("clock_integrity", {}).get("required") is not True:
            errors.append(f"{name}: clock integrity was not required")
    return dict(pass_=not errors, errors=errors)


def export_suite(suite) -> dict:
    """Export inspection tables and paired findings without rewriting trial evidence."""
    suite = Path(suite)
    manifest = read_json(suite / "suite.json")
    phase = manifest.get("phase")
    errors, missing, trials = [], [], []
    try:
        jobs = canonical_jobs(phase)
    except ValueError as exc:
        jobs = []
        errors.append(str(exc))
    if (manifest.get("experiment") != "step30" or manifest.get("jobs") != jobs or
            manifest.get("decision_rule") != DECISION_RULE):
        errors.append("suite manifest differs from the exact Step 30 job matrix")
    if manifest.get("control_only") is not True or manifest.get("delay_profile") != PROFILE:
        errors.append("suite manifest lacks Step 30 control-only or jitter settings")

    tables = {name[:-4]: [] for name in CSV_NAMES if name.endswith(".csv")}
    for job in jobs:
        trial_dir = f"{job['case']['name']}_s{job['seed']}"
        control = suite / trial_dir / "control_analysis.json"
        if not control.is_file():
            missing.append(trial_dir)
            continue
        try:
            trial = read_json(control)
            if (trial.get("trial_dir") != trial_dir or trial.get("case") != job["case"] or
                    trial.get("seed") != job["seed"] or trial.get("duration") != job["duration"]):
                errors.append(f"{trial_dir}: control analysis metadata differs from manifest")
            if trial.get("integrity_pass") is not True:
                errors.extend(f"{trial_dir}: {error}" for error in trial.get("integrity_errors", []))
            trials.append(trial)
            for name, key in (("attempts", "attempts"), ("groups", "groups"),
                              ("choices", "choices"), ("joins", "joins"),
                              ("retries", "retries"), ("holds", "holds"),
                              ("publications", "publications"), ("windows", "windows"),
                              ("states", "states")):
                tables[name].extend(_prefix_rows(trial, phase, job["case"]["name"], trial.get(key, [])))
        except (OSError, ValueError, TypeError, KeyError) as exc:
            errors.append(f"{trial_dir}: {type(exc).__name__}: {exc}")

    complete = len(trials) == len(jobs) and not missing
    trial_rows = [_summary_trial_row(row, phase) for row in trials]
    paired = _paired_contrasts(trials) if phase == "confirm" else []
    decision = _decision(trials, paired) if phase == "confirm" else None
    smoke = _smoke_gate(trials) if phase == "smoke" and complete else dict(pass_=False, errors=[])
    tables["trial_summary"] = trial_rows
    tables["paired_contrasts"] = paired
    for table_name, rows in tables.items():
        _write_csv(suite / f"{table_name}.csv", rows)
    summary = dict(experiment="step30", phase=phase, complete=complete,
                   integrity_pass=complete and not errors and all(t.get("integrity_pass") is True for t in trials),
                   job_count=len(jobs), analyzed_trial_count=len(trials), missing_trials=missing,
                   integrity_errors=sorted(set(errors)), trial_summaries=trial_rows,
                   smoke_gate_pass=smoke.get("pass_"), smoke_gate_errors=smoke.get("errors", []),
                   paired_contrasts=paired, decision=decision,
                   interpretation=("Descriptive local eight-peer loopback result with independently seeded 100–200ms control-leg jitter. "
                                   "No tensor averaging, training quality, WAN, or public-swarm impact claim is made."))
    (suite / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True,
                                                   allow_nan=False) + "\n", encoding="utf-8")
    _write_report(suite / "REPORT.md", summary, trial_rows)
    return summary


def _write_report(path, summary, trials):
    lines = ["# Step 30: stock 2s versus `(1,2]` renewal", "",
             f"Phase: `{summary.get('phase')}`; suite complete: `{summary.get('complete')}`; "
             f"integrity pass: `{summary.get('integrity_pass')}`.", "",
             "Eight peers ran on private loopback. X recruited followers, accepted them, held for 0.5s, "
             "and disbanded. Each remote find, store, join request, and response leg received an "
             "independently seeded uniform 100–200ms delay. The experiment records grouping and "
             "control-plane harm; metadata-only completion does not test tensor averaging or training quality.", "",
             "Every raw `step_start` is in the attempt denominator. A missing honest group receives a "
             "12s capped score; successful waits are also shown separately. Submission-window groups "
             "are counted by assembly time; drain assemblies are listed separately. Group rate is paired "
             "with honest-led mean roster size and actual honest local assemblies per minute, since the "
             "number of groups can change when roster sizes change.", "",
             "| Case | Honest attempts | Groups | Groups/min | Honest-led mean roster | Honest local assemblies/min | Honest failure | Mean successful wait (s) | Mean capped wait (s) | X ACCEPTED joins | X DISBANDED joins | X multi-publication searches |",
             "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for trial in trials:
        m = trial
        lines.append("| {case} / {seed} | {attempts} | {groups} | {rate} | {roster} | {local} | {fail} | {success} | {capped} | {accepted} | {disbanded} | {multi} |".format(
            case=m.get("case"), seed=m.get("seed"), attempts=m.get("honest_submitted_attempts", 0),
            groups=m.get("observation_groups", 0), rate=_fmt(m.get("honest_group_rate_per_minute")),
            roster=_fmt(m.get("honest_leader_assembled_mean_roster_size")),
            local=_fmt(m.get("honest_first_local_assemblies_per_minute")),
            fail=_fmt(m.get("honest_group_failure_fraction")),
            success=_fmt(m.get("honest_mean_success_wait_seconds")),
            capped=_fmt(m.get("honest_mean_capped_wait_seconds")),
            accepted=m.get("x_client_accept_join_count", 0),
            disbanded=m.get("x_client_disband_join_count", 0),
            multi=m.get("x_searches_with_multiple_accepted_publications", 0)))
    if summary.get("paired_contrasts"):
        lines.extend(["", "## Paired effects", "",
                      "Positive wait/failure differences and a negative group-rate difference are harmful to honest peers. "
                      "A material signal requires the predeclared threshold in at least two paired seeds, "
                      "the third seed in the same direction, and the equally weighted mean effect to meet its threshold.", ""])
        decision = summary.get("decision", {})
        for outcome, row in decision.items():
            if isinstance(row, dict) and "material_signal" in row:
                lines.append(f"- `{outcome}`: seed effects {row.get('paired_seed_effects')}; "
                             f"threshold {row.get('threshold')}; material signal `{row.get('material_signal')}`.")
        lines.append(f"- Coverage: `{decision.get('coverage_sufficient')}`; conclusion: {decision.get('conclusion')}.")
        lines.append(f"- Persistence: `{json.dumps(decision.get('persistence', {}), sort_keys=True)}`.")
    if summary.get("smoke_gate_errors"):
        lines.extend(["", "## Smoke gate", ""])
        lines.extend(f"- {error}" for error in summary["smoke_gate_errors"])
    if summary.get("integrity_errors"):
        lines.extend(["", "## Integrity errors", ""])
        lines.extend(f"- {error}" for error in summary["integrity_errors"])
    lines.extend(["", summary.get("interpretation", "")])
    Path(path).write_text("\n".join(lines) + "\n", encoding="utf-8")


def _fmt(value):
    return "" if value is None else f"{float(value):.3f}"
