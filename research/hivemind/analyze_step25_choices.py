"""Reconstruct Step 25 native matchmaking choices and ad-read cadence.

The analyzer is stdlib-only. It validates each trial with the Step 24 native
grouping analyzer, then joins Step 25's detailed observations to submission,
read, selection, join, and assembly records. It never treats a queue view as a
native decision unless the selection has a same-peer, same-search read link.
"""

from __future__ import annotations

import argparse
import csv
from collections import defaultdict
import hashlib
import json
import math
from pathlib import Path
import statistics
import sys

try:  # Supports both `python analyze_step25_choices.py` and package imports.
    from .analyze_step24_lifetime import analyse_trial, _event_stream, read_json, require, finite_number
except ImportError:  # pragma: no cover - exercised by the CLI import layout
    from analyze_step24_lifetime import analyse_trial, _event_stream, read_json, require, finite_number


X_NAME = "X"
HONEST_NAMES = tuple(f"H{i}" for i in range(7))
CSV_NAMES = ("policy_table.csv", "attempts.csv", "selections.csv", "reads.csv",
             "groups.csv", "joins.csv", "per_peer.csv",
             "per_seed.csv",
             "paired_differences_vs_passive_stock.csv")


def _ref(event):
    return f"{event['pid']}:{event['event_number']}"


def _event_order(event):
    return (float(event["monotonic"]), event["pid"], event["event_number"])


def _percentile(values, fraction):
    values = sorted(float(value) for value in values)
    if not values:
        return None
    position = (len(values) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return values[lower]
    weight = position - lower
    return values[lower] * (1.0 - weight) + values[upper] * weight


def _json_key(case, seed, duration):
    return json.dumps(dict(case=case, seed=seed, duration=duration), sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


def _name_maps(config, errors):
    peer_ids = config.get("peer_ids")
    if not isinstance(peer_ids, dict) or set(peer_ids) != {X_NAME, *HONEST_NAMES}:
        errors.append("peer_ids must map X and H0..H6")
        return {}, {}
    if any(not isinstance(peer, str) or not peer for peer in peer_ids.values()):
        errors.append("peer_ids contains an invalid peer id")
        return {}, {}
    if len(set(peer_ids.values())) != len(peer_ids):
        errors.append("peer_ids are not unique")
    return peer_ids, {peer: name for name, peer in peer_ids.items()}


def _step_identity(event, names_by_peer, errors, kind):
    detail = event.get("detail", {})
    step_id = detail.get("step_id")
    if not isinstance(step_id, str) or not step_id:
        errors.append(f"{kind} missing step_id at {_ref(event)}")
        return None
    if event.get("peer") not in names_by_peer:
        errors.append(f"{kind} has unknown peer at {_ref(event)}")
    return step_id


def _cutoff(config, events, errors):
    try:
        started = finite_number(config.get("started_monotonic"), "started_monotonic")
        duration = finite_number(config.get("duration"), "duration")
        if duration < 0:
            raise ValueError("duration must be nonnegative")
        return started, started + duration
    except (TypeError, ValueError) as exc:
        errors.append(str(exc))
        return None, None


def _build_searches(events, names_by_peer, errors, minimal=False):
    starts, ends = {}, {}
    for event in events:
        kind, detail = event["kind"], event["detail"]
        if kind not in ("search_span_start", "search_span_end"):
            continue
        search_id = detail.get("search_id")
        step_id = _step_identity(event, names_by_peer, errors, kind)
        if not isinstance(search_id, str) or not search_id:
            errors.append(f"{kind} missing search_id at {_ref(event)}")
            continue
        key = (event["peer"], search_id)
        target = starts if kind == "search_span_start" else ends
        if key in target:
            errors.append(f"duplicate {kind} for {search_id}")
            continue
        target[key] = event
        if kind == "search_span_start":
            if detail.get("step_id") != step_id:
                errors.append(f"search step identity mismatch for {search_id}")

    searches = []
    by_id = {}
    for key, start in starts.items():
        end = ends.get(key)
        detail = start["detail"]
        search_id = detail["search_id"]
        row = dict(peer_id=start["peer"], peer_name=names_by_peer.get(start["peer"]),
                   step_id=detail.get("step_id"), search_id=search_id,
                   start_event_ref=_ref(start), end_event_ref=_ref(end) if end else None,
                   start_monotonic=start["monotonic"], end_monotonic=end["monotonic"] if end else None,
                   start_wall=start["wall_time"], end_wall=end["wall_time"] if end else None,
                   group_key=detail.get("group_key"), detailed=True,
                   duration_seconds=(end["monotonic"] - start["monotonic"] if end else None))
        if end is None:
            errors.append(f"search {search_id} has no search_span_end")
        elif end["monotonic"] < start["monotonic"]:
            errors.append(f"search {search_id} ends before it starts")
        elif (end["detail"].get("step_id") != row["step_id"] or
              end["detail"].get("search_id") != search_id):
            errors.append(f"search {search_id} start/end identity mismatch")
        searches.append(row)
        by_id[key] = row

    for key in ends.keys() - starts.keys():
        errors.append(f"search {key[1]} has an end without a start")

    # Old minimal Step 24 traces have no search IDs. Keep this fallback limited
    # to configs explicitly marked minimal.
    if minimal:
        legacy_by_step = defaultdict(list)
        for event in events:
            if event["kind"] == "search_start":
                step_id = event["detail"].get("step_id")
                if isinstance(step_id, str):
                    legacy_by_step[(event["peer"], step_id)].append(event)
        for (peer, step_id), rows in legacy_by_step.items():
            for ordinal, start in enumerate(rows, 1):
                end = next((event for event in events
                            if event["kind"] == "search_end" and event["peer"] == peer
                            and event["event_number"] > start["event_number"]
                            and (ordinal == len(rows) or event["event_number"] < rows[ordinal]["event_number"])), None)
                search_id = f"legacy:{peer}:{step_id}:{ordinal}"
                row = dict(peer_id=peer, peer_name=names_by_peer.get(peer), step_id=step_id,
                           search_id=search_id, start_event_ref=_ref(start),
                           end_event_ref=_ref(end) if end else None,
                           start_monotonic=start["monotonic"],
                           end_monotonic=end["monotonic"] if end else None,
                           start_wall=start["wall_time"], end_wall=end["wall_time"] if end else None,
                           group_key=start["detail"].get("group_key"), detailed=False,
                           duration_seconds=(end["monotonic"] - start["monotonic"] if end else None))
                searches.append(row)
                by_id[(peer, search_id)] = row
    searches.sort(key=lambda row: (row["start_monotonic"], row["peer_id"], row["search_id"]))
    return searches, by_id


def _build_read_states(events, names_by_peer, search_by_id, errors):
    spans = []
    active = {}
    pauses = defaultdict(list)
    for event in events:
        kind = event["kind"]
        if kind not in ("read_enabled", "read_resumed", "read_paused", "read_disabled"):
            continue
        detail = event["detail"]
        peer, search_id = event["peer"], detail.get("search_id")
        step_id = _step_identity(event, names_by_peer, errors, kind)
        search = search_by_id.get((peer, search_id))
        if search is None:
            errors.append(f"{kind} references unknown same-peer search {search_id} at {_ref(event)}")
        elif search["step_id"] != step_id:
            errors.append(f"{kind} step differs from search {search_id}")
        key = (peer, search_id)
        if kind in ("read_enabled", "read_resumed"):
            if key in active:
                errors.append(f"{kind} while reads already enabled for {search_id}")
                continue
            row = dict(peer_id=peer, peer_name=names_by_peer.get(peer), step_id=step_id,
                       search_id=search_id, span_index=sum(1 for span in spans
                                                         if span["peer_id"] == peer and
                                                         span["search_id"] == search_id) + 1,
                       start_event_ref=_ref(event), end_event_ref=None,
                       start_monotonic=event["monotonic"], end_monotonic=None,
                       start_wall=event["wall_time"], end_wall=None,
                       start_kind=kind, end_kind=None, duration_seconds=None)
            spans.append(row)
            active[key] = row
        else:
            if kind == "read_paused":
                pauses[key].append(event)
            row = active.pop(key, None)
            if row is None:
                # begin_search may finish while already paused. It correctly
                # emits no disabled event in that state.
                if kind == "read_disabled":
                    errors.append(f"read_disabled without enabled span for {search_id}")
                continue
            row.update(end_event_ref=_ref(event), end_monotonic=event["monotonic"],
                       end_wall=event["wall_time"], end_kind=kind,
                       duration_seconds=max(0.0, event["monotonic"] - row["start_monotonic"]))
            if event["monotonic"] < row["start_monotonic"]:
                errors.append(f"read state span ends before it starts for {search_id}")

    for key, row in active.items():
        row["end_kind"] = "trace_end"
        errors.append(f"read-enabled span for {key[1]} has no pause/disable event")
    return spans, pauses


def _build_reads(events, names_by_peer, search_by_id, spans, pauses, x_peer, errors):
    starts, ends = {}, {}
    for event in events:
        if event["kind"] not in ("ad_read_start", "ad_read_end"):
            continue
        detail = event["detail"]
        read_id = detail.get("read_id")
        step_id = _step_identity(event, names_by_peer, errors, event["kind"])
        if not isinstance(read_id, str) or not read_id:
            errors.append(f"{event['kind']} missing read_id at {_ref(event)}")
            continue
        key = (event["peer"], read_id)
        target = starts if event["kind"] == "ad_read_start" else ends
        if key in target:
            errors.append(f"duplicate {event['kind']} for {read_id}")
            continue
        target[key] = event
        search_id = detail.get("search_id")
        search = search_by_id.get((event["peer"], search_id))
        if search is None:
            errors.append(f"{event['kind']} references unknown same-peer search {search_id}")
        elif search["step_id"] != step_id:
            errors.append(f"{event['kind']} step differs from search {search_id}")

    spans_by_search = defaultdict(list)
    for span in spans:
        spans_by_search[(span["peer_id"], span["search_id"])].append(span)

    rows = []
    for key, start in starts.items():
        end = ends.get(key)
        detail = start["detail"]
        search_id, step_id = detail.get("search_id"), detail.get("step_id")
        span = next((candidate for candidate in spans_by_search[(start["peer"], search_id)]
                     if candidate["start_monotonic"] <= start["monotonic"] and
                     (candidate["end_monotonic"] is None or
                      start["monotonic"] <= candidate["end_monotonic"])), None)
        entries = end["detail"].get("entries") if end else None
        successful = bool(end and end["detail"].get("error") in (None, "") and
                          end["detail"].get("cancelled") is not True)
        x_entries = [entry for entry in entries or []
                     if isinstance(entry, dict) and entry.get("peer") == x_peer]
        x_expiration = None
        x_visible = None
        if end and successful:
            if len(x_entries) > 1:
                errors.append(f"read {key[1]} repeats X in entries")
            if x_entries:
                try:
                    x_expiration = finite_number(x_entries[0].get("expiration"), "X read expiration")
                    x_visible = x_expiration > end["wall_time"]
                except ValueError as exc:
                    errors.append(f"read {key[1]} has invalid X expiration: {exc}")
            else:
                x_visible = False
        if end:
            if end["detail"].get("search_id") != search_id or end["detail"].get("step_id") != step_id:
                errors.append(f"read {key[1]} start/end identity mismatch")
            if not isinstance(entries, list):
                errors.append(f"read {key[1]} end entries must be a list")
            if not successful and entries != []:
                errors.append(f"failed or cancelled read {key[1]} must have empty entries")
            if end["monotonic"] < start["monotonic"]:
                errors.append(f"read {key[1]} ends before it starts")
        else:
            errors.append(f"read {key[1]} has no ad_read_end")
        latency = None
        native_await_latency = None
        if end:
            try:
                latency = finite_number(end["detail"].get("latency"), "ad read latency")
                if latency < 0:
                    raise ValueError("ad read latency must be nonnegative")
            except ValueError as exc:
                errors.append(f"read {key[1]} has invalid latency: {exc}")
                latency = None
            if "native_await_latency" in end["detail"]:
                try:
                    native_await_latency = finite_number(
                        end["detail"].get("native_await_latency"), "native ad await latency")
                    if native_await_latency < 0:
                        raise ValueError("native ad await latency must be nonnegative")
                except ValueError as exc:
                    errors.append(f"read {key[1]} has invalid native await latency: {exc}")
                    native_await_latency = None
        crossed_pause = False
        if end:
            crossed_pause = any(start["monotonic"] < pause["monotonic"] <= end["monotonic"]
                                for pause in pauses[(start["peer"], search_id)])
        rows.append(dict(
            peer_id=start["peer"], peer_name=names_by_peer.get(start["peer"]),
            step_id=step_id, search_id=search_id, read_id=key[1],
            start_event_ref=_ref(start), end_event_ref=_ref(end) if end else None,
            start_monotonic=start["monotonic"], end_monotonic=end["monotonic"] if end else None,
            start_wall=start["wall_time"], completion_wall=end["wall_time"] if end else None,
            latency_seconds=latency,
            native_await_latency_seconds=native_await_latency,
            event_span_seconds=(end["monotonic"] - start["monotonic"] if end else None),
            latency_definition=("elapsed monotonic wrapper interval around the logged ad read; includes local trace-hook time and is not packet/RPC latency"),
            native_await_latency_definition=("elapsed monotonic time awaiting the original native read, excluding surrounding local trace hooks; not packet/RPC latency"),
            success=successful, error=end["detail"].get("error") if end else "missing_end",
            cancelled=(end["detail"].get("cancelled") is True if end else False),
            entries=entries if isinstance(entries, list) else [],
            x_visible_at_completion=x_visible, x_expiration=x_expiration,
            x_remaining_at_completion=(x_expiration - end["wall_time"]
                                       if x_expiration is not None and end else None),
            enabled_span_id=(f"{span['peer_id']}:{span['search_id']}:{span['span_index']}"
                             if span else None),
            enabled_at_start=span is not None,
            start_phase=None, completion_phase=None,
            in_flight_across_pause=crossed_pause,
            group_key=detail.get("group_key")))

    for key, end in ends.items():
        if key not in starts:
            errors.append(f"read {key[1]} has ad_read_end without start")
            rows.append(dict(peer_id=end["peer"], peer_name=names_by_peer.get(end["peer"]),
                             step_id=end["detail"].get("step_id"),
                             search_id=end["detail"].get("search_id"), read_id=key[1],
                             start_event_ref=None, end_event_ref=_ref(end),
                             start_monotonic=None, end_monotonic=end["monotonic"],
                             start_wall=None, completion_wall=end["wall_time"],
                             latency_seconds=end["detail"].get("latency"),
                             native_await_latency_seconds=end["detail"].get("native_await_latency"),
                             event_span_seconds=None,
                             latency_definition=("elapsed monotonic wrapper interval around the logged ad read; includes local trace-hook time and is not packet/RPC latency"),
                             native_await_latency_definition=("elapsed monotonic time awaiting the original native read, excluding surrounding local trace hooks; not packet/RPC latency"),
                             success=False, error="orphan_end", cancelled=False,
                             entries=end["detail"].get("entries", []),
                             x_visible_at_completion=None, x_expiration=None,
                             x_remaining_at_completion=None, enabled_span_id=None,
                             enabled_at_start=False, start_phase=None, completion_phase=None,
                             in_flight_across_pause=False, group_key=end["detail"].get("group_key")))
    rows.sort(key=lambda row: (float("inf") if row["start_monotonic"] is None else row["start_monotonic"],
                               row["peer_id"], row["read_id"]))
    return rows


def _bytes_hex(value, label, errors):
    if not isinstance(value, str):
        errors.append(f"invalid {label}: expected hex string")
        return None
    try:
        return bytes.fromhex(value)
    except ValueError:
        errors.append(f"invalid {label}: malformed hex string")
        return None


def _expiration(value, label, errors, allow_infinite=False):
    if value is None and allow_infinite:
        return math.inf
    try:
        return finite_number(value, label)
    except ValueError as exc:
        errors.append(str(exc))
        return None


def _build_selections(events, names_by_peer, peer_ids, search_by_id, read_rows,
                      x_peer, cutoff, errors):
    reads_by_id = {(row.get("peer_id"), row.get("read_id")): row for row in read_rows
                   if row.get("start_event_ref") is not None}
    completed_by_search = defaultdict(list)
    for row in read_rows:
        if row.get("success") and row.get("end_monotonic") is not None:
            completed_by_search[(row["peer_id"], row["search_id"])].append(row)
    for rows in completed_by_search.values():
        rows.sort(key=lambda row: row["end_monotonic"])

    output = []
    first_eligible_by_search = set()
    for event in events:
        if event["kind"] != "native_pop_selected":
            continue
        detail = event["detail"]
        peer, search_id = event["peer"], detail.get("search_id")
        step_id = _step_identity(event, names_by_peer, errors, "native_pop_selected")
        search = search_by_id.get((peer, search_id))
        if search is None:
            errors.append(f"native_pop_selected references unknown same-peer search {search_id}")
        elif search["step_id"] != step_id:
            errors.append(f"selection step differs from search {search_id}")
        try:
            top_wall = finite_number(detail.get("selection_top_wall"), "selection_top_wall")
        except ValueError as exc:
            errors.append(str(exc))
            top_wall = None
        own_expiration = _expiration(detail.get("own_expiration"), "own_expiration", errors,
                                     allow_infinite=True)
        own_bytes = _bytes_hex(detail.get("own_peer_bytes_hex"), "own_peer_bytes_hex", errors)
        selected_expiration = _expiration(detail.get("selected_expiration"),
                                          "selected_expiration", errors)
        selected_bytes = _bytes_hex(detail.get("selected_peer_bytes_hex"),
                                    "selected_peer_bytes_hex", errors)
        queue = detail.get("queue")
        past = detail.get("past_attempts")
        if not isinstance(queue, list):
            errors.append(f"selection {_ref(event)} queue must be a list")
            queue = []
        if not isinstance(past, list):
            errors.append(f"selection {_ref(event)} past_attempts must be a list")
            past = []

        parsed_queue = []
        for item in queue:
            if not isinstance(item, dict) or not isinstance(item.get("peer"), str):
                errors.append(f"selection {_ref(event)} contains malformed queue entry")
                continue
            expiration = _expiration(item.get("expiration"), "queue expiration", errors)
            peer_bytes = _bytes_hex(item.get("peer_bytes_hex"), "queue peer_bytes_hex", errors)
            if expiration is not None and peer_bytes is not None:
                parsed_queue.append(dict(peer=item["peer"], expiration=expiration,
                                         peer_bytes=peer_bytes, original=item))
        parsed_past = []
        for item in past:
            if not isinstance(item, dict) or not isinstance(item.get("peer"), str):
                errors.append(f"selection {_ref(event)} contains malformed past_attempt")
                continue
            expiration = _expiration(item.get("expiration"), "past attempt expiration", errors)
            if expiration is not None:
                parsed_past.append((item["peer"], expiration))

        def attempted(item):
            return (item["peer"], item["expiration"]) in parsed_past

        for item in parsed_queue:
            if attempted(item):
                errors.append(f"selection {_ref(event)} queue includes past-attempted exact pair")
                break

        selected_peer = event.get("other")
        selected_matches = [item for item in parsed_queue
                            if item["peer"] == selected_peer and
                            item["expiration"] == selected_expiration and
                            item["peer_bytes"] == selected_bytes]
        selected_in_queue = bool(selected_matches)
        if not selected_in_queue:
            errors.append(f"selection {_ref(event)} selected tuple absent from queue snapshot")
        selected_live = (selected_expiration is not None and top_wall is not None and
                         selected_expiration > top_wall)
        selected_le_own = (selected_expiration is not None and own_expiration is not None and
                           own_bytes is not None and selected_bytes is not None and
                           (selected_expiration, selected_bytes) <= (own_expiration, own_bytes))
        live_queue = [item for item in parsed_queue
                      if top_wall is not None and item["expiration"] > top_wall]
        # TimedStorage's heap compares PeerID keys using their base58 string
        # ordering. The separate native priority guard compares peer bytes.
        native_top = min(live_queue, key=lambda item: (item["expiration"], item["peer"])) if live_queue else None
        selected_is_top = bool(native_top and native_top["peer"] == selected_peer and
                               native_top["expiration"] == selected_expiration and
                               native_top["peer_bytes"] == selected_bytes)
        selection_valid = bool(selected_in_queue and selected_live and selected_le_own and selected_is_top)
        if selected_in_queue and not selected_live:
            errors.append(f"selection {_ref(event)} selected an expired queue entry at captured top wall")
        if selected_in_queue and not selected_le_own:
            errors.append(f"selection {_ref(event)} violates the native priority check")
        if selected_in_queue and selected_live and not selected_is_top:
            errors.append(f"selection {_ref(event)} is not the live native queue top")

        x_candidates = [item for item in parsed_queue if item["peer"] == x_peer]
        x_candidate = min(x_candidates, key=lambda item: (item["expiration"], item["peer_bytes"])) if x_candidates else None
        x_exp = x_candidate["expiration"] if x_candidate else None
        x_live = bool(x_candidate and top_wall is not None and x_candidate["expiration"] > top_wall)
        x_not_attempted = bool(x_candidate and not attempted(x_candidate))
        x_le_own = bool(x_candidate and own_expiration is not None and own_bytes is not None and
                        (x_candidate["expiration"], x_candidate["peer_bytes"]) <=
                        (own_expiration, own_bytes))
        x_eligible = bool(x_candidate and x_live and x_not_attempted and x_le_own)

        read_id = detail.get("read_id")
        read = reads_by_id.get((peer, read_id)) if isinstance(read_id, str) else None
        if read_id is None:
            errors.append(f"selection {_ref(event)} is missing its completed read_id")
        latest_success = next((row for row in reversed(completed_by_search[(peer, search_id)])
                               if row["end_monotonic"] <= event["monotonic"]), None)
        read_causality_valid = bool(read and read.get("success") and
                                    read.get("peer_id") == peer and
                                    read.get("search_id") == search_id and
                                    read.get("step_id") == step_id and
                                    read.get("end_monotonic") is not None and
                                    read["end_monotonic"] <= event["monotonic"] and
                                    latest_success is not None and latest_success["read_id"] == read_id)
        if read_id is not None and not read_causality_valid:
            errors.append(f"selection {_ref(event)} read_id is not latest successful same-peer/search read")
        read_entry_pairs = set()
        if read_causality_valid:
            for entry in read.get("entries", []):
                if not isinstance(entry, dict) or not isinstance(entry.get("peer"), str):
                    errors.append(f"selection {_ref(event)} references a read with malformed entries")
                    continue
                try:
                    read_entry_pairs.add((entry["peer"], finite_number(entry.get("expiration"),
                                                                        "read entry expiration")))
                except ValueError as exc:
                    errors.append(f"selection {_ref(event)} read has invalid entry expiration: {exc}")
        filtered_queue_pairs = {
            (item["peer"], item["expiration"]) for item in parsed_queue
            if item["peer"] != peer and top_wall is not None and item["expiration"] > top_wall
            and not attempted(item)
        }
        queue_matches_read_view = bool(read_causality_valid and
                                       filtered_queue_pairs.issubset(read_entry_pairs))
        selected_matches_read_view = bool(read_causality_valid and
                                          (selected_peer, selected_expiration) in read_entry_pairs)
        if read_causality_valid and not queue_matches_read_view:
            errors.append(f"selection {_ref(event)} queue contains an unobserved live entry")
        if read_causality_valid and selected_in_queue and not selected_matches_read_view:
            errors.append(f"selection {_ref(event)} selected tuple differs from linked read view")
        first_key = (peer, search_id)
        opportunity = (names_by_peer.get(peer) in HONEST_NAMES and x_eligible and
                       selection_valid and read_causality_valid and queue_matches_read_view and
                       selected_matches_read_view)
        first_eligible = bool(opportunity and first_key not in first_eligible_by_search)
        if first_eligible:
            first_eligible_by_search.add(first_key)

        read_x_entry = next((entry for entry in (read or {}).get("entries", [])
                             if isinstance(entry, dict) and entry.get("peer") == x_peer), None)
        phase = "submission" if cutoff is not None and event["monotonic"] < cutoff else "drain"
        output.append(dict(
            event_ref=_ref(event), peer_id=peer, peer_name=names_by_peer.get(peer),
            step_id=step_id, search_id=search_id, group_key=detail.get("group_key"),
            leader_peer_id=selected_peer, leader_name=names_by_peer.get(selected_peer),
            selected_expiration=selected_expiration,
            selected_peer_bytes_hex=detail.get("selected_peer_bytes_hex"),
            own_expiration=detail.get("own_expiration"),
            own_priority_is_infinite=detail.get("own_expiration") is None,
            own_peer_bytes_hex=detail.get("own_peer_bytes_hex"),
            selection_top_wall=top_wall, queue=queue, past_attempts=past,
            selected_in_queue=selected_in_queue, selected_unexpired=selected_live,
            selected_priority_le_own=selected_le_own, selected_is_native_top=selected_is_top,
            selection_valid=selection_valid,
            read_id=read_id, read_event_ref=read.get("end_event_ref") if read else None,
            read_causality_valid=read_causality_valid,
            queue_matches_read_view=queue_matches_read_view,
            selected_matches_read_view=selected_matches_read_view,
            read_x_expiration=(read_x_entry.get("expiration") if read_x_entry else None),
            read_x_visible_at_completion=(read.get("x_visible_at_completion") if read else None),
            x_in_queue=x_candidate is not None, x_expiration=x_exp,
            x_priority_le_own=x_le_own, x_unexpired=x_live,
            x_not_past_attempt=x_not_attempted, x_eligible=x_eligible,
            eligible_choice_opportunity=opportunity, first_eligible=first_eligible,
            chose_X=(selected_peer == x_peer), decision_phase=phase))

    output.sort(key=lambda row: (next(event["monotonic"] for event in events
                                      if _ref(event) == row["event_ref"]),
                                 row["peer_id"], row["event_ref"]))
    return output


def _publisher_before(events_by_pid, assembly):
    rows = [event for event in events_by_pid.get(assembly["pid"], [])
            if event["event_number"] < assembly["event_number"] and
            event["monotonic"] <= assembly["monotonic"] and
            event["kind"] == "ad_started" and event["peer"] == assembly["peer"]]
    return rows[-1] if rows else None


def _build_groups(events, peer_ids, names_by_peer, errors):
    by_pid = defaultdict(list)
    for event in events:
        by_pid[event["pid"]].append(event)
    for rows in by_pid.values():
        rows.sort(key=lambda event: event["event_number"])
    x_peer = peer_ids.get("X")
    x_declarations = [event for event in events
                      if event["kind"] == "ad_started" and event["peer"] == x_peer]
    x_declarations.sort(key=_event_order)
    assemblies = defaultdict(list)
    for event in events:
        if event["kind"] != "assembled":
            continue
        group_id = event["detail"].get("group_id")
        if not isinstance(group_id, str) or not group_id:
            errors.append(f"assembled event missing group_id at {_ref(event)}")
            continue
        assemblies[group_id].append(event)

    groups = []
    for group_id, rows in sorted(assemblies.items()):
        if len(rows) > 1:
            errors.append(f"duplicate assembled group_id {group_id}")
        first = min(rows, key=_event_order)
        detail = first["detail"]
        leader_peer = first["peer"]
        roster = detail.get("roster")
        if not isinstance(roster, list) or any(not isinstance(peer, str) for peer in roster):
            errors.append(f"assembly {group_id} has invalid roster")
            roster = []
        if len(set(roster)) != len(roster):
            errors.append(f"assembly {group_id} has duplicate roster peers")
        if leader_peer not in roster:
            errors.append(f"assembly {group_id} leader absent from roster")
        for duplicate in rows:
            if duplicate["peer"] != leader_peer or duplicate["detail"].get("roster") != detail.get("roster"):
                errors.append(f"conflicting duplicate assembly id {group_id}")

        leader_declaration = _publisher_before(by_pid, first)
        if leader_declaration:
            leader_expiration = leader_declaration["detail"].get("expiration")
            leader_active = leader_declaration["detail"].get("active")
            try:
                leader_expiration = (finite_number(leader_expiration, "leader ad expiration")
                                     if leader_expiration is not None else None)
            except ValueError as exc:
                errors.append(f"assembly {group_id} has invalid leader expiration: {exc}")
                leader_expiration = None
        else:
            leader_expiration, leader_active = None, None
        prior_x = [event for event in x_declarations if event["monotonic"] <= first["monotonic"]]
        x_declaration = prior_x[-1] if prior_x else None
        x_expiration = None
        x_active = None
        if x_declaration:
            x_active = x_declaration["detail"].get("active")
            try:
                raw_exp = x_declaration["detail"].get("expiration")
                x_expiration = finite_number(raw_exp, "X ad expiration") if raw_exp is not None else None
            except ValueError as exc:
                errors.append(f"assembly {group_id} has invalid X expiration: {exc}")
        wall = first["wall_time"]
        gap = (x_expiration - leader_expiration
               if x_expiration is not None and leader_expiration is not None else None)
        groups.append(dict(
            group_id=group_id, leader_name=names_by_peer.get(leader_peer),
            leader_peer_id=leader_peer, leader_step_id=detail.get("step_id"),
            roster=sorted(roster), roster_size=len(roster),
            assembly_event_ref=_ref(first), assembly_event_refs=[_ref(event) for event in rows],
            assembly_monotonic=first["monotonic"], assembly_wall=wall,
            leader_ad_started_event_ref=_ref(leader_declaration) if leader_declaration else None,
            leader_ad_active=leader_active, leader_expiration=leader_expiration,
            leader_group_key=(leader_declaration["other"] or None) if leader_declaration else None,
            x_ad_started_event_ref=_ref(x_declaration) if x_declaration else None,
            x_ad_active=x_active, x_expiration=x_expiration,
            x_group_key=(x_declaration["other"] or None) if x_declaration else None,
            x_minus_leader_expiration_gap=gap,
            leader_remaining_at_assembly=(leader_expiration - wall
                                          if leader_expiration is not None else None),
            x_remaining_at_assembly=(x_expiration - wall if x_expiration is not None else None),
            x_in_roster=x_peer in roster))
    return groups


def _build_joins(events, names_by_peer, search_by_id, selections, groups, errors):
    starts, ends = {}, {}
    client_events = defaultdict(list)
    selections_by_ref = {row["event_ref"]: row for row in selections}
    for event in events:
        kind, detail = event["kind"], event["detail"]
        if kind not in ("join_attempt_start", "join_attempt_end"):
            if kind in ("native_client_wait", "native_client_reply", "native_client_exception"):
                join_id = detail.get("join_id")
                if isinstance(join_id, str) and join_id:
                    client_events[join_id].append(event)
                else:
                    errors.append(f"{kind} missing join_id at {_ref(event)}")
            continue
        join_id = detail.get("join_id")
        if not isinstance(join_id, str) or not join_id:
            errors.append(f"{kind} missing join_id at {_ref(event)}")
            continue
        target = starts if kind == "join_attempt_start" else ends
        if join_id in target:
            errors.append(f"duplicate {kind} for {join_id}")
            continue
        target[join_id] = event
        if event["peer"] not in names_by_peer:
            errors.append(f"{kind} has unknown peer for {join_id}")
        search_id = detail.get("search_id")
        search = search_by_id.get((event["peer"], search_id))
        if search is None:
            errors.append(f"{kind} references unknown same-peer search {search_id}")
        elif search["step_id"] != detail.get("step_id"):
            errors.append(f"{kind} step differs from search {search_id}")
        if kind == "join_attempt_start":
            origin = detail.get("origin")
            if origin not in ("queue", "redirect"):
                errors.append(f"join {join_id} has invalid origin")
            selection_ref = detail.get("selection_event_ref")
            if origin == "queue":
                selection = selections_by_ref.get(selection_ref)
                if selection is None:
                    errors.append(f"queue join {join_id} has no matching selection reference")
                elif (selection["peer_id"] != event["peer"] or
                      selection["search_id"] != search_id or
                      selection["step_id"] != detail.get("step_id") or
                      selection["leader_peer_id"] != event["other"]):
                    errors.append(f"queue join {join_id} selection reference identity mismatch")
            elif selection_ref is not None:
                errors.append(f"redirect join {join_id} unexpectedly references a queue selection")

    group_by_id = {row["group_id"]: row for row in groups}
    joins = []
    for join_id, start in starts.items():
        detail = start["detail"]
        end = ends.get(join_id)
        replies = sorted(client_events.get(join_id, []), key=_event_order)
        for event in replies:
            if event["peer"] != start["peer"] or event["other"] != start["other"]:
                errors.append(f"native client event identity differs from join {join_id}")
            if event["detail"].get("step_id") != detail.get("step_id"):
                errors.append(f"native client step differs from join {join_id}")
        begin_replies = [event for event in replies
                         if event["kind"] == "native_client_reply" and
                         event["detail"].get("code") == "BEGIN_ALLREDUCE"]
        group_ids = sorted({event["detail"].get("group_id") for event in begin_replies
                            if isinstance(event["detail"].get("group_id"), str)})
        for group_id in group_ids:
            group = group_by_id.get(group_id)
            if group is None:
                errors.append(f"join {join_id} begins allreduce for unknown assembled group {group_id}")
        if end is None:
            errors.append(f"join {join_id} has no join_attempt_end")
        elif (end["peer"] != start["peer"] or end["other"] != start["other"] or
              end["detail"].get("step_id") != detail.get("step_id") or
              end["detail"].get("search_id") != detail.get("search_id")):
            errors.append(f"join {join_id} start/end identity mismatch")
        selection = selections_by_ref.get(detail.get("selection_event_ref"))
        native_records = [dict(event_ref=_ref(event), kind=event["kind"],
                               phase=event["detail"].get("phase"),
                               request_expiration=event["detail"].get("request_expiration"),
                               timeout=event["detail"].get("timeout"),
                               code=event["detail"].get("code"),
                               error=event["detail"].get("error"),
                               group_id=event["detail"].get("group_id"),
                               error_message=event["detail"].get("error_message"))
                          for event in replies]
        joins.append(dict(join_id=join_id, peer_id=start["peer"],
                          peer_name=names_by_peer.get(start["peer"]),
                          leader_peer_id=start["other"],
                          leader_name=names_by_peer.get(start["other"]),
                          step_id=detail.get("step_id"), search_id=detail.get("search_id"),
                          origin=detail.get("origin"),
                          selection_event_ref=detail.get("selection_event_ref"),
                          start_event_ref=_ref(start), end_event_ref=_ref(end) if end else None,
                          start_monotonic=start["monotonic"], end_monotonic=end["monotonic"] if end else None,
                          error=end["detail"].get("error") if end else "missing_end",
                          selected_expiration=selection.get("selected_expiration") if selection else None,
                          native_client_event_refs=[_ref(event) for event in replies],
                          native_client_records=native_records,
                          reply_codes=[event["detail"].get("code") for event in replies
                                       if event["kind"] == "native_client_reply"],
                          exception_types=[event["detail"].get("error") for event in replies
                                           if event["kind"] == "native_client_exception"],
                          begin_allreduce_group_ids=group_ids,
                          begin_allreduce_reply_refs=[_ref(event) for event in begin_replies]))
    for join_id in ends.keys() - starts.keys():
        errors.append(f"join {join_id} has join_attempt_end without start")
    for join_id in client_events.keys() - starts.keys():
        errors.append(f"native client events reference unknown join {join_id}")
    joins.sort(key=lambda row: (row["start_monotonic"], row["peer_id"], row["join_id"]))
    return joins


def _summarize_attempts(events, peer_ids, names_by_peer, searches, groups, joins, cutoff,
                        interior_cutoff, trace_end, errors, detailed=True):
    starts, results = {}, {}
    for event in events:
        if event["kind"] == "step_start":
            step_id = event["detail"].get("step_id")
            if not isinstance(step_id, str) or not step_id:
                errors.append(f"step_start missing step_id at {_ref(event)}")
                continue
            if step_id in starts:
                errors.append(f"duplicate step_start for {step_id}")
            else:
                starts[step_id] = event
        elif event["kind"] == "step_result":
            step_id = event["detail"].get("step_id")
            if isinstance(step_id, str):
                if step_id in results:
                    errors.append(f"duplicate step_result for {step_id}")
                else:
                    results[step_id] = event

    led_groups_by_step = defaultdict(list)
    followed_groups_by_step = defaultdict(list)
    assemblies_by_step = defaultdict(list)
    group_by_id = {group["group_id"]: group for group in groups}
    for group in groups:
        step_id = group.get("leader_step_id")
        if isinstance(step_id, str):
            led_groups_by_step[(group["leader_peer_id"], step_id)].append(group)
            assemblies_by_step[(group["leader_peer_id"], step_id)].append(group)
    for join in joins:
        if join["peer_id"] not in peer_ids.values():
            continue
        for group_id in join["begin_allreduce_group_ids"]:
            group = group_by_id.get(group_id)
            if group and join["peer_id"] in group["roster"]:
                followed_groups_by_step[(join["peer_id"], join["step_id"])].append(group)
                assemblies_by_step[(join["peer_id"], join["step_id"])].append(group)

    # Search entries can be legacy fallbacks in a minimal trace. That fallback
    # is never enabled for detailed Step 25 configs.
    searches_by_step = defaultdict(list)
    for search in searches:
        searches_by_step[(search["peer_id"], search["step_id"])].append(search)
    attempts = []
    for step_id, start in starts.items():
        peer_id = start["peer"]
        peer_name = names_by_peer.get(peer_id)
        result = results.get(step_id)
        step_searches = sorted(searches_by_step[(peer_id, step_id)],
                               key=lambda row: row["start_monotonic"])
        first_search = step_searches[0] if step_searches else None
        candidates = {row["group_id"]: row
                      for row in assemblies_by_step.get((peer_id, step_id), [])}
        assembled_groups = sorted(candidates.values(), key=lambda row: row["assembly_monotonic"])
        led_groups = sorted({row["group_id"]: row for row in
                             led_groups_by_step.get((peer_id, step_id), [])}.values(),
                            key=lambda row: row["assembly_monotonic"])
        followed_groups = sorted({row["group_id"]: row for row in
                                  followed_groups_by_step.get((peer_id, step_id), [])}.values(),
                                 key=lambda row: row["assembly_monotonic"])
        first_group = assembled_groups[0] if assembled_groups else None
        first_led_group = led_groups[0] if led_groups else None
        first_followed_group = followed_groups[0] if followed_groups else None
        latency = (first_led_group["assembly_monotonic"] - first_search["start_monotonic"]
                   if first_led_group and first_search else None)
        if latency is not None and latency < 0:
            errors.append(f"first X-led assembly precedes first search for {step_id}")
        any_success = bool(assembled_groups)
        led_success = bool(led_groups)
        status = result["detail"].get("status") if result else None
        if led_success:
            eventual = "led"
            if detailed and first_search is None:
                errors.append(f"X-led attempt {step_id} has no detailed search span")
        elif first_followed_group is not None:
            eventual = "followed"
        elif status == "no_group":
            eventual = "no_group"
        else:
            # This includes explicit failed/cancelled outcomes and attempts
            # still unresolved when the bounded drain closed.
            eventual = "failed"
        submission_started = cutoff is not None and start["monotonic"] < cutoff
        interior_attempt = (submission_started and interior_cutoff is not None and
                            start["monotonic"] <= interior_cutoff)
        result_phase = ("submission" if result and cutoff is not None and
                        result["monotonic"] < cutoff else "drain" if result else None)
        row = dict(peer_id=peer_id, peer_name=peer_name, step_id=step_id,
                   sequence=start["detail"].get("sequence"),
                   start_event_ref=_ref(start), started_monotonic=start["monotonic"],
                   started_wall=start["wall_time"], submission_started=submission_started,
                   interior_attempt=interior_attempt,
                   attempt_phase=("submission" if submission_started else "drain"),
                   step_result_event_ref=_ref(result) if result else None,
                   step_result_monotonic=result["monotonic"] if result else None,
                   step_result_status=status, result_phase=result_phase,
                   drain_outcome=(result is None or (cutoff is not None and result["monotonic"] > cutoff)),
                   search_ids=[search["search_id"] for search in step_searches],
                   search_count=len(step_searches),
                   first_search_event_ref=first_search["start_event_ref"] if first_search else None,
                   first_search_monotonic=first_search["start_monotonic"] if first_search else None,
                   assembled_success=any_success, led_success=led_success,
                   followed_success=bool(first_followed_group),
                   first_assembled_group_id=first_group["group_id"] if first_group else None,
                   first_assembly_event_ref=first_group["assembly_event_ref"] if first_group else None,
                   first_assembly_monotonic=first_group["assembly_monotonic"] if first_group else None,
                   first_led_group_id=first_led_group["group_id"] if first_led_group else None,
                   first_led_assembly_event_ref=first_led_group["assembly_event_ref"] if first_led_group else None,
                   first_led_assembly_monotonic=first_led_group["assembly_monotonic"] if first_led_group else None,
                   first_followed_group_id=first_followed_group["group_id"] if first_followed_group else None,
                   first_followed_assembly_event_ref=(first_followed_group["assembly_event_ref"]
                                                     if first_followed_group else None),
                   assembly_latency_seconds=latency,
                   assembly_latency_definition=("first own-led native assembled-event emission minus first search_span_start; observed wrapper return time"
                                                if first_led_group and first_search else None),
                   any_assembly_latency_seconds=(first_group["assembly_monotonic"] - first_search["start_monotonic"]
                                                 if first_group and first_search else None),
                   assembled_group_ids=[group["group_id"] for group in assembled_groups],
                   assembly_event_refs=[group["assembly_event_ref"] for group in assembled_groups],
                   led_group_ids=[group["group_id"] for group in led_groups],
                   followed_group_ids=[group["group_id"] for group in followed_groups],
                   eventual_outcome=eventual,
                   success_by_1s=bool(led_success and latency is not None and latency <= 1.0),
                   success_by_2s=bool(led_success and latency is not None and latency <= 2.0),
                   success_by_5s=bool(led_success and latency is not None and latency <= 5.0))
        attempts.append(row)
    for step_id in results.keys() - starts.keys():
        errors.append(f"step_result without step_start for {step_id}")
    attempts.sort(key=lambda row: (row["started_monotonic"], row["peer_id"], row["step_id"]))
    return attempts


def _enabled_duration(span, started, cutoff, drain_end):
    lo = span["start_monotonic"]
    hi = span["end_monotonic"] if span["end_monotonic"] is not None else drain_end
    if started is not None:
        lo = max(lo, started)
    if drain_end is not None:
        hi = min(hi, drain_end)
    if hi <= lo:
        return 0.0, 0.0
    submission_hi = min(hi, cutoff) if cutoff is not None else hi
    submission = max(0.0, submission_hi - lo)
    drain_lo = max(lo, cutoff) if cutoff is not None else hi
    drain = max(0.0, hi - drain_lo)
    return submission, drain


def _metrics(attempts, selections, reads, spans, peer_ids, cutoff, started,
             drain_end, trace_closed):
    x_attempts = [row for row in attempts if row["peer_name"] == X_NAME and row["submission_started"]]
    x_interior = [row for row in x_attempts if row["interior_attempt"]]
    denominator = len(x_attempts)
    latency_samples = [row["assembly_latency_seconds"] for row in x_attempts
                       if row["led_success"] and row["assembly_latency_seconds"] is not None]
    eventual_counts = {name: sum(row["eventual_outcome"] == name for row in x_attempts)
                       for name in ("led", "followed", "no_group", "failed")}
    success_counts = {}
    for seconds in (1, 2, 5):
        count = sum(row[f"success_by_{seconds}s"] for row in x_attempts)
        success_counts[f"success_by_{seconds}s"] = count
        success_counts[f"success_by_{seconds}s_rate"] = count / denominator if denominator else None
    interior_counts = {}
    for seconds in (1, 2, 5):
        count = sum(row[f"success_by_{seconds}s"] for row in x_interior)
        interior_counts[f"interior_success_by_{seconds}s"] = count
        interior_counts[f"interior_success_by_{seconds}s_rate"] = count / len(x_interior) if x_interior else None
    interior_outcomes = {name: sum(row["eventual_outcome"] == name for row in x_interior)
                         for name in ("led", "followed", "no_group", "failed")}

    choice_rows = [row for row in selections if row["first_eligible"]]
    choice_denominator = len(choice_rows)
    chose_x = sum(row["chose_X"] for row in choice_rows)
    eligible_by_phase = {phase: sum(row["decision_phase"] == phase for row in choice_rows)
                         for phase in ("submission", "drain")}
    chose_by_phase = {phase: sum(row["decision_phase"] == phase and row["chose_X"]
                                 for row in choice_rows)
                      for phase in ("submission", "drain")}

    span_time = defaultdict(lambda: [0.0, 0.0])
    spans_by_id = {}
    for span in spans:
        duration_parts = _enabled_duration(span, started, cutoff, drain_end)
        span_id = f"{span['peer_id']}:{span['search_id']}:{span['span_index']}"
        spans_by_id[span_id] = span
        span_time[span["peer_id"]][0] += duration_parts[0]
        span_time[span["peer_id"]][1] += duration_parts[1]

    for read in reads:
        start = read.get("start_monotonic")
        end = read.get("end_monotonic")
        read["start_phase"] = ("before_start" if start is not None and started is not None and start < started
                               else "submission" if cutoff is not None and start is not None and start < cutoff
                               else "drain" if start is not None and
                               (drain_end is None or start <= drain_end) else "after_drain" if start is not None else None)
        read["completion_phase"] = ("before_start" if end is not None and started is not None and end < started
                                    else "submission" if cutoff is not None and end is not None and end < cutoff
                                    else "drain" if end is not None and
                                    (drain_end is None or end <= drain_end) else "after_drain" if end is not None else None)
        span_id = read.get("enabled_span_id")
        read["enabled_seconds_at_start"] = bool(span_id)
        read["enabled_in_observation_window"] = bool(span_id and read["start_phase"] in ("submission", "drain"))

    successful_reads = [row for row in reads if row["success"] and row["end_event_ref"]]
    latency_reads = [row["latency_seconds"] for row in successful_reads
                     if row["latency_seconds"] is not None]
    visible = sum(row["x_visible_at_completion"] is True for row in successful_reads)
    read_starts_by_span = defaultdict(list)
    read_count_by_peer = defaultdict(int)
    all_read_count_by_peer = defaultdict(int)
    for read in reads:
        if read.get("start_monotonic") is None:
            continue
        all_read_count_by_peer[read["peer_id"]] += 1
        span_id = read.get("enabled_span_id")
        if span_id and read["start_phase"] in ("submission", "drain"):
            read_count_by_peer[read["peer_id"]] += 1
            read_starts_by_span[span_id].append(read)
    intervals = []
    honest_intervals = []
    phase_intervals = {phase: [] for phase in ("submission", "drain")}
    honest_phase_intervals = {phase: [] for phase in ("submission", "drain")}
    honest_ids = {peer_ids[name] for name in HONEST_NAMES}
    for span_id, rows in read_starts_by_span.items():
        rows.sort(key=lambda row: row["start_monotonic"])
        previous = None
        previous_by_phase = {}
        for row in rows:
            row["interval_since_previous_in_enabled_span"] = (
                row["start_monotonic"] - previous if previous is not None else None)
            if previous is not None:
                intervals.append(row["interval_since_previous_in_enabled_span"])
                if row["peer_id"] in honest_ids:
                    honest_intervals.append(row["interval_since_previous_in_enabled_span"])
            phase = row["start_phase"]
            previous_phase = previous_by_phase.get(phase)
            row["interval_since_previous_in_enabled_span_same_phase"] = (
                row["start_monotonic"] - previous_phase if previous_phase is not None else None)
            if previous_phase is not None:
                interval = row["interval_since_previous_in_enabled_span_same_phase"]
                phase_intervals[phase].append(interval)
                if row["peer_id"] in honest_ids:
                    honest_phase_intervals[phase].append(interval)
            previous_by_phase[phase] = row["start_monotonic"]
            previous = row["start_monotonic"]

    peers = {}
    selections_by_peer = defaultdict(list)
    for selection in selections:
        selections_by_peer[selection["peer_id"]].append(selection)
    for name, peer in peer_ids.items():
        p_attempts = [row for row in attempts if row["peer_id"] == peer and row["submission_started"]]
        p_reads = [row for row in reads if row["peer_id"] == peer]
        p_success_reads = [row for row in p_reads if row["success"] and row["end_event_ref"]]
        p_choices = [row for row in selections_by_peer[peer] if row["first_eligible"]]
        submission_seconds, drain_seconds = span_time[peer]
        p_read_starts = read_count_by_peer[peer]
        p_phase_seconds = dict(submission=submission_seconds, drain=drain_seconds)
        p_phase_starts = {phase: sum(row["enabled_in_observation_window"] and
                                     row["peer_id"] == peer and row["start_phase"] == phase
                                     for row in reads)
                          for phase in ("submission", "drain")}
        peers[name] = dict(peer_id=peer, attempts=len(p_attempts),
                           eventual_counts={outcome: sum(row["eventual_outcome"] == outcome for row in p_attempts)
                                            for outcome in ("led", "followed", "no_group", "failed")},
                           searches=sum(row["search_count"] for row in p_attempts),
                           reads=len(p_reads), successful_reads=len(p_success_reads),
                           out_of_span_read_start_count=(all_read_count_by_peer[peer] - p_read_starts),
                           enabled_seconds=submission_seconds + drain_seconds,
                           submission_enabled_seconds=submission_seconds,
                           drain_enabled_seconds=drain_seconds,
                           read_start_count=p_read_starts,
                           read_start_rate_per_enabled_second=(p_read_starts / (submission_seconds + drain_seconds)
                                                               if submission_seconds + drain_seconds else None),
                           read_start_count_by_phase=p_phase_starts,
                           read_start_rate_per_enabled_second_by_phase={
                               phase: (p_phase_starts[phase] / p_phase_seconds[phase]
                                       if p_phase_seconds[phase] else None)
                               for phase in ("submission", "drain")},
                           successful_read_latency_seconds=[row["latency_seconds"] for row in p_success_reads
                                                            if row["latency_seconds"] is not None],
                           x_visible_successful_reads=sum(row["x_visible_at_completion"] is True
                                                         for row in p_success_reads),
                           x_visible_fraction=(sum(row["x_visible_at_completion"] is True for row in p_success_reads)
                                               / len(p_success_reads) if p_success_reads else None),
                           choice_opportunities=len(p_choices),
                           chose_X=sum(row["chose_X"] for row in p_choices),
                           choice_opportunities_by_phase={phase: sum(row["decision_phase"] == phase
                                                                      for row in p_choices)
                                                          for phase in ("submission", "drain")},
                           chose_X_by_phase={phase: sum(row["decision_phase"] == phase and row["chose_X"]
                                                       for row in p_choices)
                                             for phase in ("submission", "drain")})
    honest_enabled = sum(span_time[peer][0] + span_time[peer][1] for peer in honest_ids)
    honest_read_starts = sum(read_count_by_peer[peer] for peer in honest_ids)
    honest_successful_reads = [row for row in successful_reads if row["peer_name"] in HONEST_NAMES]
    honest_read_latencies = [row["latency_seconds"] for row in honest_successful_reads
                             if row["latency_seconds"] is not None]
    honest_x_visible = sum(row["x_visible_at_completion"] is True for row in honest_successful_reads)
    honest_by_phase = {}
    for phase, phase_index in (("submission", 0), ("drain", 1)):
        enabled_seconds = sum(span_time[peer][phase_index] for peer in honest_ids)
        phase_reads_started = [row for row in reads if row["peer_name"] in HONEST_NAMES and
                               row["enabled_in_observation_window"] and row["start_phase"] == phase]
        phase_successful = [row for row in reads if row["peer_name"] in HONEST_NAMES and row["success"] and
                            row["end_event_ref"] and row["completion_phase"] == phase]
        phase_latencies = [row["latency_seconds"] for row in phase_successful
                           if row["latency_seconds"] is not None]
        phase_visible = sum(row["x_visible_at_completion"] is True for row in phase_successful)
        phase_intervals = honest_phase_intervals[phase]
        start_count = len(phase_reads_started)
        honest_by_phase[phase] = dict(
            enabled_seconds=enabled_seconds, read_start_count=start_count,
            read_start_rate_per_enabled_second=(start_count / enabled_seconds if enabled_seconds else None),
            read_start_intervals_seconds=phase_intervals,
            read_start_interval_median_seconds=(statistics.median(phase_intervals) if phase_intervals else None),
            read_start_interval_p90_seconds=_percentile(phase_intervals, 0.9),
            successful_completed_reads=len(phase_successful),
            successful_read_latency_seconds=phase_latencies,
            successful_read_latency_median_seconds=(statistics.median(phase_latencies) if phase_latencies else None),
            successful_read_latency_p90_seconds=_percentile(phase_latencies, 0.9),
            x_visible_successful_reads=phase_visible,
            x_visible_fraction=(phase_visible / len(phase_successful) if phase_successful else None))
    all_enabled = sum(v[0] + v[1] for v in span_time.values())
    all_starts = sum(read_count_by_peer.values())
    return dict(
        submission_cutoff_monotonic=cutoff,
        trial_started_monotonic=started,
        drain_end_monotonic=drain_end,
        trace_closed_monotonic=trace_closed,
        x_attempts=dict(denominator=denominator,
                        success_count=sum(row["led_success"] for row in x_attempts),
                        any_group_attempts=sum(row["assembled_success"] for row in x_attempts),
                        **success_counts, eventual_counts=eventual_counts,
                        interior_denominator=len(x_interior),
                        interior_assembled_attempts=sum(row["led_success"] for row in x_interior),
                        interior_led_attempts=interior_outcomes["led"],
                        interior_eventual_counts=interior_outcomes,
                        **interior_counts,
                        successful_assembly_latency_seconds=latency_samples,
                        successful_assembly_latency_median_seconds=statistics.median(latency_samples)
                        if latency_samples else None,
                        successful_assembly_latency_p90_seconds=_percentile(latency_samples, 0.9)),
        choice_opportunity=dict(eligible_first=choice_denominator, chose_X=chose_x,
                                fraction=chose_x / choice_denominator if choice_denominator else None,
                                eligible_first_by_phase=eligible_by_phase,
                                chose_X_by_phase=chose_by_phase,
                                fraction_by_phase={phase: (chose_by_phase[phase] / eligible_by_phase[phase]
                                                          if eligible_by_phase[phase] else None)
                                                   for phase in ("submission", "drain")}),
        cadence=dict(enabled_seconds=all_enabled,
                     submission_enabled_seconds=sum(v[0] for v in span_time.values()),
                     drain_enabled_seconds=sum(v[1] for v in span_time.values()),
                     read_start_count=all_starts,
                     out_of_span_read_start_count=sum(all_read_count_by_peer.values()) - all_starts,
                     read_start_rate_per_enabled_second=all_starts / all_enabled if all_enabled else None,
                     read_start_intervals_seconds=intervals,
                     successful_read_latency_seconds=latency_reads,
                     x_visible_successful_reads=visible,
                     successful_completed_reads=len(successful_reads),
                     x_visible_fraction=visible / len(successful_reads) if successful_reads else None,
                     reads_in_flight_across_pause=sum(row["in_flight_across_pause"] for row in reads),
                     honest_enabled_seconds=honest_enabled,
                     honest_read_start_count=honest_read_starts,
                     honest_read_start_rate_per_enabled_second=(honest_read_starts / honest_enabled
                                                                 if honest_enabled else None),
                     honest_read_start_intervals_seconds=honest_intervals,
                     honest_read_start_interval_median_seconds=(statistics.median(honest_intervals)
                                                                if honest_intervals else None),
                     honest_read_start_interval_p90_seconds=_percentile(honest_intervals, 0.9),
                     honest_successful_read_latency_seconds=honest_read_latencies,
                     honest_successful_read_latency_median_seconds=(statistics.median(honest_read_latencies)
                                                                    if honest_read_latencies else None),
                     honest_successful_read_latency_p90_seconds=_percentile(honest_read_latencies, 0.9),
                     honest_x_visible_successful_reads=honest_x_visible,
                     honest_successful_completed_reads=len(honest_successful_reads),
                     honest_x_visible_fraction=(honest_x_visible / len(honest_successful_reads)
                                                if honest_successful_reads else None),
                     honest_by_phase=honest_by_phase,
                     honest_read_start_count_by_phase={phase: honest_by_phase[phase]["read_start_count"]
                                                       for phase in ("submission", "drain")},
                     honest_read_start_rate_per_enabled_second_by_phase={
                         phase: honest_by_phase[phase]["read_start_rate_per_enabled_second"]
                         for phase in ("submission", "drain")},
                     honest_phase_definitions=dict(
                         enabled_seconds="read_enabled/read_resumed intervals clipped to [trial start, drain end], split at submission cutoff",
                         read_start_count="starts in the phase that fall inside a linked enabled span",
                         read_start_intervals_seconds="intervals between consecutive starts only when both starts are in the same enabled span and same phase",
                         successful_completed_reads="successful ad_read_end events with completion in the phase, regardless of start phase",
                         successful_read_latency_seconds="observed logical wrapper latency recorded at successful completion; includes local trace-hook overhead and is not packet/RPC latency",
                         x_visible_fraction="X-visible successful completions divided by all successful completions in the phase")),
        per_peer=peers)


def reconstruct_trial(config, events) -> dict:
    """Purely reconstruct Step 25 choices from config and validated events."""
    if not isinstance(config, dict):
        raise ValueError("trial config must be a dictionary")
    if not isinstance(events, list) or not events:
        raise ValueError("events must be a nonempty list")
    ordered = sorted(events, key=_event_order)
    errors = []
    peer_ids, names_by_peer = _name_maps(config, errors)
    started, cutoff = _cutoff(config, ordered, errors)
    trace_closed_event = next((event for event in reversed(ordered)
                               if event["kind"] == "trace_closed"), None)
    trace_closed = trace_closed_event["monotonic"] if trace_closed_event else None
    trace_end = trace_closed if trace_closed is not None else (ordered[-1]["monotonic"]
                                                               if ordered else None)
    if trace_closed_event is None:
        errors.append("trace has no trace_closed event")
    result_times = [event["monotonic"] for event in ordered if event["kind"] == "step_result"]
    last_step_result = max(result_times) if result_times else None
    drain_candidates = [value for value in (cutoff, last_step_result) if value is not None]
    drain_end = max(drain_candidates) if drain_candidates else trace_end
    minimal = config.get("instrumentation") == "minimal"
    searches, search_by_id = _build_searches(ordered, names_by_peer, errors, minimal=minimal)
    spans, pauses = _build_read_states(ordered, names_by_peer, search_by_id, errors)
    x_peer = peer_ids.get(X_NAME)
    reads = _build_reads(ordered, names_by_peer, search_by_id, spans, pauses, x_peer, errors)
    selections = _build_selections(ordered, names_by_peer, peer_ids, search_by_id, reads,
                                   x_peer, cutoff, errors)
    groups = _build_groups(ordered, peer_ids, names_by_peer, errors)
    joins = _build_joins(ordered, names_by_peer, search_by_id, selections, groups, errors)
    try:
        timeout = finite_number(config.get("timeout"), "timeout")
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        interior_cutoff = cutoff - timeout if cutoff is not None else None
    except ValueError as exc:
        errors.append(str(exc))
        interior_cutoff = None
    attempts = _summarize_attempts(ordered, peer_ids, names_by_peer, searches, groups,
                                   joins, cutoff, interior_cutoff, trace_end, errors,
                                   detailed=config.get("instrumentation") != "minimal")
    metrics = _metrics(attempts, selections, reads, spans, peer_ids, cutoff, started,
                       drain_end, trace_closed)
    # Rebuild per-peer interval arrays after metric calculation assigned them.
    errors = sorted(set(errors))
    case = config.get("case", {})
    return dict(case=case.get("name") if isinstance(case, dict) else None,
                policy=case if isinstance(case, dict) else {}, seed=config.get("seed"),
                duration=config.get("duration"), instrumentation=config.get("instrumentation"),
                experiment=config.get("experiment"),
                submission_cutoff_monotonic=cutoff,
                interior_attempt_cutoff_monotonic=interior_cutoff,
                drain_end_monotonic=drain_end,
                last_step_result_monotonic=last_step_result,
                trace_closed_monotonic=trace_closed,
                trace_end_monotonic=trace_end,
                trace_integrity_errors=errors, trace_integrity_pass=not errors,
                attempts=attempts, searches=searches, enabled_spans=spans,
                reads=reads, selections=selections, groups=groups, joins=joins,
                admission_observations=[dict(kind=event["kind"], event_ref=_ref(event),
                                             peer_id=event["peer"], peer_name=names_by_peer.get(event["peer"]),
                                             other_peer_id=event["other"],
                                             other_name=names_by_peer.get(event["other"]),
                                             monotonic=event["monotonic"], wall_time=event["wall_time"],
                                             detail=event["detail"])
                                        for event in ordered
                                        if event["kind"] in ("join_received", "native_leader_wait")],
                per_peer=metrics.pop("per_peer"), metrics=metrics)


def analyse_trial_choices(path) -> dict:
    """Validate Step 24 grouping integrity and reconstruct detailed choices."""
    path = Path(path)
    native = analyse_trial(path)
    config = read_json(path / "config.json")
    require(isinstance(config, dict), "invalid trial config")
    require(config.get("outcome") == "grouping", "Step 25 choice analysis requires grouping outcome")
    require(config.get("instrumentation") == "detailed",
            "Step 25 choice analysis requires detailed instrumentation")
    events = _event_stream(path)
    measured = reconstruct_trial(config, events)
    measured.update(dict(source=str(path), trial_dir=path.name,
                         step24_control_plane_pass=native["control_plane_pass"],
                         step24_control_integrity_errors=native["control_integrity_errors"],
                         control_plane_pass=bool(native["control_plane_pass"] and
                                                 measured["trace_integrity_pass"]),
                         groups=measured["groups"], step24_control_groups=native["control_groups"],
                         x_assembled_leads=native["x_assembled_leads"],
                         total_assembled_groups=native["total_assembled_groups"],
                         error=native.get("error")))
    return measured


def _honest_phase_aggregate(trials, phase):
    """Pool honest-peer cadence samples and counts for one observation phase."""
    rows = [trial["metrics"]["cadence"]["honest_by_phase"][phase] for trial in trials]
    intervals = [value for row in rows for value in row["read_start_intervals_seconds"]]
    latencies = [value for row in rows for value in row["successful_read_latency_seconds"]]
    enabled_seconds = sum(row["enabled_seconds"] for row in rows)
    starts = sum(row["read_start_count"] for row in rows)
    successful = sum(row["successful_completed_reads"] for row in rows)
    visible = sum(row["x_visible_successful_reads"] for row in rows)
    return dict(
        enabled_seconds=enabled_seconds,
        read_start_count=starts,
        read_start_rate_per_enabled_second=(starts / enabled_seconds if enabled_seconds else None),
        read_start_intervals_seconds=intervals,
        read_start_interval_median_seconds=statistics.median(intervals) if intervals else None,
        read_start_interval_p90_seconds=_percentile(intervals, 0.9),
        successful_completed_reads=successful,
        successful_read_latency_seconds=latencies,
        successful_read_latency_median_seconds=statistics.median(latencies) if latencies else None,
        successful_read_latency_p90_seconds=_percentile(latencies, 0.9),
        x_visible_successful_reads=visible,
        x_visible_fraction=visible / successful if successful else None)


def _honest_phase_columns(by_phase):
    return {f"honest_{phase}_{key}": value
            for phase, summary in by_phase.items() for key, value in summary.items()}


def _aggregate_trials(trials):
    by_case = defaultdict(list)
    for trial in trials:
        by_case[trial["case"]].append(trial)
    result = []
    for case, rows in sorted(by_case.items()):
        submitted = [row for trial in rows for row in trial["attempts"]
                     if row["peer_name"] == X_NAME and row["submission_started"]]
        successful_latencies = [row["assembly_latency_seconds"] for row in submitted
                                 if row["led_success"] and row["assembly_latency_seconds"] is not None]
        interior = [row for row in submitted if row["interior_attempt"]]
        denomin = len(submitted)
        outcomes = {name: sum(row["eventual_outcome"] == name for row in submitted)
                    for name in ("led", "followed", "no_group", "failed")}
        choices = [selection for trial in rows for selection in trial["selections"]
                   if selection["first_eligible"]]
        choices_by_phase = {phase: [row for row in choices if row["decision_phase"] == phase]
                            for phase in ("submission", "drain")}
        honest_reads = [read for trial in rows for read in trial["reads"]
                        if read["peer_name"] in HONEST_NAMES and read["success"] and
                        read["end_event_ref"]]
        honest_read_latencies = [read["latency_seconds"] for read in honest_reads
                                 if read["latency_seconds"] is not None]
        honest_read_intervals = [read["interval_since_previous_in_enabled_span"]
                                 for trial in rows for read in trial["reads"]
                                 if read["peer_name"] in HONEST_NAMES and
                                 read.get("interval_since_previous_in_enabled_span") is not None]
        honest_visible = sum(read["x_visible_at_completion"] is True for read in honest_reads)
        honest_enabled_seconds = sum(trial["metrics"]["cadence"]["honest_enabled_seconds"]
                                     for trial in rows)
        honest_read_start_count = sum(trial["metrics"]["cadence"]["honest_read_start_count"]
                                      for trial in rows)
        all_trial_groups = sum(trial["total_assembled_groups"] for trial in rows)
        x_lead_groups = sum(trial["x_assembled_leads"] for trial in rows)
        honest_cadence_by_phase = {
            phase: _honest_phase_aggregate(rows, phase) for phase in ("submission", "drain")}
        counts = {f"success_by_{n}s": sum(row[f"success_by_{n}s"] for row in submitted)
                  for n in (1, 2, 5)}
        choice_count = sum(row["chose_X"] for row in choices)
        per_seed = []
        for trial in sorted(rows, key=lambda row: row["seed"]):
            attempts = [row for row in trial["attempts"]
                        if row["peer_name"] == X_NAME and row["submission_started"]]
            n = len(attempts)
            interior_attempts = [row for row in attempts if row["interior_attempt"]]
            interior_n = len(interior_attempts)
            seed_choices = [row for row in trial["selections"] if row["first_eligible"]]
            seed_honest_reads = [read for read in trial["reads"]
                                 if read["peer_name"] in HONEST_NAMES and read["success"] and
                                 read["end_event_ref"]]
            seed_read_latencies = [read["latency_seconds"] for read in seed_honest_reads
                                   if read["latency_seconds"] is not None]
            seed_read_intervals = [read["interval_since_previous_in_enabled_span"]
                                   for read in trial["reads"]
                                   if read["peer_name"] in HONEST_NAMES and
                                   read.get("interval_since_previous_in_enabled_span") is not None]
            seed_visible = sum(read["x_visible_at_completion"] is True for read in seed_honest_reads)
            seed_cadence = trial["metrics"]["cadence"]
            seed_honest_cadence_by_phase = seed_cadence["honest_by_phase"]
            per_seed.append(dict(seed=trial["seed"], case=case,
                                 total_assembled_groups=trial["total_assembled_groups"],
                                 x_assembled_leads=trial["x_assembled_leads"],
                                 x_attempt_denominator=n,
                                 success_by_1s=sum(row["success_by_1s"] for row in attempts),
                                 success_by_2s=sum(row["success_by_2s"] for row in attempts),
                                 success_by_5s=sum(row["success_by_5s"] for row in attempts),
                                 success_by_1s_rate=(sum(row["success_by_1s"] for row in attempts) / n if n else None),
                                 success_by_2s_rate=(sum(row["success_by_2s"] for row in attempts) / n if n else None),
                                 success_by_5s_rate=(sum(row["success_by_5s"] for row in attempts) / n if n else None),
                                 interior_attempt_denominator=interior_n,
                                 interior_led_attempts=sum(row["led_success"] for row in interior_attempts),
                                 interior_led_rate=(sum(row["led_success"] for row in interior_attempts)
                                                    / interior_n if interior_n else None),
                                 interior_success_by_1s=sum(row["success_by_1s"] for row in interior_attempts),
                                 interior_success_by_2s=sum(row["success_by_2s"] for row in interior_attempts),
                                 interior_success_by_5s=sum(row["success_by_5s"] for row in interior_attempts),
                                 interior_success_by_1s_rate=(sum(row["success_by_1s"] for row in interior_attempts)
                                                              / interior_n if interior_n else None),
                                 interior_success_by_2s_rate=(sum(row["success_by_2s"] for row in interior_attempts)
                                                              / interior_n if interior_n else None),
                                 interior_success_by_5s_rate=(sum(row["success_by_5s"] for row in interior_attempts)
                                                              / interior_n if interior_n else None),
                                 eventual_counts={name: sum(row["eventual_outcome"] == name for row in attempts)
                                                  for name in ("led", "followed", "no_group", "failed")},
                                 eventual_led_rate=(sum(row["eventual_outcome"] == "led" for row in attempts)
                                                    / n if n else None),
                                 eventual_followed_rate=(sum(row["eventual_outcome"] == "followed" for row in attempts)
                                                        / n if n else None),
                                 choice_opportunities=len(seed_choices),
                                 chose_X=sum(row["chose_X"] for row in seed_choices),
                                 choice_fraction=(sum(row["chose_X"] for row in seed_choices) / len(seed_choices)
                                                  if seed_choices else None),
                                 eligible_first_submission=sum(row["decision_phase"] == "submission"
                                                               for row in seed_choices),
                                 chose_X_submission=sum(row["decision_phase"] == "submission" and row["chose_X"]
                                                        for row in seed_choices),
                                 eligible_first_drain=sum(row["decision_phase"] == "drain"
                                                          for row in seed_choices),
                                 chose_X_drain=sum(row["decision_phase"] == "drain" and row["chose_X"]
                                                   for row in seed_choices),
                                 choice_fraction_submission=(
                                     sum(row["decision_phase"] == "submission" and row["chose_X"]
                                         for row in seed_choices)
                                     / sum(row["decision_phase"] == "submission" for row in seed_choices)
                                     if any(row["decision_phase"] == "submission" for row in seed_choices) else None),
                                 choice_fraction_drain=(
                                     sum(row["decision_phase"] == "drain" and row["chose_X"]
                                         for row in seed_choices)
                                     / sum(row["decision_phase"] == "drain" for row in seed_choices)
                                     if any(row["decision_phase"] == "drain" for row in seed_choices) else None),
                                 honest_enabled_seconds=seed_cadence["honest_enabled_seconds"],
                                 honest_read_start_count=seed_cadence["honest_read_start_count"],
                                 honest_read_start_rate_per_enabled_second=(
                                     seed_cadence["honest_read_start_rate_per_enabled_second"]),
                                 honest_read_start_intervals_seconds=seed_read_intervals,
                                 honest_read_start_interval_median_seconds=(statistics.median(seed_read_intervals)
                                                                            if seed_read_intervals else None),
                                 honest_read_start_interval_p90_seconds=_percentile(seed_read_intervals, 0.9),
                                 honest_successful_read_latency_seconds=seed_read_latencies,
                                 honest_successful_read_latency_median_seconds=(statistics.median(seed_read_latencies)
                                                                                if seed_read_latencies else None),
                                 honest_successful_read_latency_p90_seconds=_percentile(seed_read_latencies, 0.9),
                                 honest_x_visible_successful_reads=seed_visible,
                                 honest_successful_completed_reads=len(seed_honest_reads),
                                 honest_x_visible_fraction=(seed_visible / len(seed_honest_reads)
                                                            if seed_honest_reads else None),
                                 honest_cadence_by_phase=seed_honest_cadence_by_phase,
                                 **_honest_phase_columns(seed_honest_cadence_by_phase),
                                 successful_assembly_latency_seconds=[row["assembly_latency_seconds"]
                                                                      for row in attempts
                                                                      if row["led_success"] and
                                                                      row["assembly_latency_seconds"] is not None]))
        result.append(dict(
            case=case, policy=rows[0]["policy"], trials=len(rows),
            total_assembled_groups=all_trial_groups,
            x_assembled_leads=x_lead_groups,
            x_attempt_denominator=denomin,
            interior_attempt_denominator=len(interior),
            interior_led_attempts=sum(row["led_success"] for row in interior),
            interior_led_rate=(sum(row["led_success"] for row in interior) / len(interior)
                               if interior else None),
            interior_success_by_1s=sum(row["success_by_1s"] for row in interior),
            interior_success_by_2s=sum(row["success_by_2s"] for row in interior),
            interior_success_by_5s=sum(row["success_by_5s"] for row in interior),
            interior_success_by_1s_rate=(sum(row["success_by_1s"] for row in interior) / len(interior)
                                         if interior else None),
            interior_success_by_2s_rate=(sum(row["success_by_2s"] for row in interior) / len(interior)
                                         if interior else None),
            interior_success_by_5s_rate=(sum(row["success_by_5s"] for row in interior) / len(interior)
                                         if interior else None),
            x_assembled_attempts=sum(row["assembled_success"] for row in submitted),
            x_led_attempts=sum(row["led_success"] for row in submitted),
            **counts,
            **{f"{key}_rate": value / denomin if denomin else None for key, value in counts.items()},
            eventual_counts=outcomes,
            eventual_led_rate=outcomes["led"] / denomin if denomin else None,
            eventual_followed_rate=outcomes["followed"] / denomin if denomin else None,
            eventual_no_group_rate=outcomes["no_group"] / denomin if denomin else None,
            eventual_failed_rate=outcomes["failed"] / denomin if denomin else None,
            eligible_first=len(choices), chose_X=choice_count,
            choice_fraction=choice_count / len(choices) if choices else None,
            eligible_first_submission=len(choices_by_phase["submission"]),
            chose_X_submission=sum(row["chose_X"] for row in choices_by_phase["submission"]),
            choice_fraction_submission=(
                sum(row["chose_X"] for row in choices_by_phase["submission"])
                / len(choices_by_phase["submission"]) if choices_by_phase["submission"] else None),
            eligible_first_drain=len(choices_by_phase["drain"]),
            chose_X_drain=sum(row["chose_X"] for row in choices_by_phase["drain"]),
            choice_fraction_drain=(
                sum(row["chose_X"] for row in choices_by_phase["drain"])
                / len(choices_by_phase["drain"]) if choices_by_phase["drain"] else None),
            honest_enabled_seconds=honest_enabled_seconds,
            honest_read_start_count=honest_read_start_count,
            honest_read_start_rate_per_enabled_second=(honest_read_start_count / honest_enabled_seconds
                                                        if honest_enabled_seconds else None),
            honest_read_start_intervals_seconds=honest_read_intervals,
            honest_read_start_interval_median_seconds=(statistics.median(honest_read_intervals)
                                                       if honest_read_intervals else None),
            honest_read_start_interval_p90_seconds=_percentile(honest_read_intervals, 0.9),
            honest_successful_read_latency_seconds=honest_read_latencies,
            honest_successful_read_latency_median_seconds=(statistics.median(honest_read_latencies)
                                                           if honest_read_latencies else None),
            honest_successful_read_latency_p90_seconds=_percentile(honest_read_latencies, 0.9),
            honest_x_visible_successful_reads=honest_visible,
            honest_successful_completed_reads=len(honest_reads),
            honest_x_visible_fraction=(honest_visible / len(honest_reads) if honest_reads else None),
            honest_cadence_by_phase=honest_cadence_by_phase,
            **_honest_phase_columns(honest_cadence_by_phase),
            successful_assembly_latency_seconds=successful_latencies,
            successful_assembly_latency_median_seconds=statistics.median(successful_latencies)
            if successful_latencies else None,
            successful_assembly_latency_p90_seconds=_percentile(successful_latencies, 0.9),
            per_seed=per_seed))
    return result


def _paired_differences(policy_rows):
    by_seed = defaultdict(dict)
    for policy in policy_rows:
        for row in policy["per_seed"]:
            by_seed[row["seed"]][policy["case"]] = row
    output = []
    for seed, rows in sorted(by_seed.items()):
        baseline = rows.get("passive_stock")
        if baseline is None:
            for case, row in sorted(rows.items()):
                if case != "passive_stock":
                    output.append(dict(seed=seed, case=case, baseline_case="passive_stock",
                                       status="missing_passive_stock", baseline=None, treatment=row,
                                       differences=None))
            continue
        base_rates = {
            "success_by_1s_rate": baseline["success_by_1s_rate"],
            "success_by_2s_rate": baseline["success_by_2s_rate"],
            "success_by_5s_rate": baseline["success_by_5s_rate"],
            "interior_led_rate": baseline["interior_led_rate"],
            "interior_success_by_1s_rate": baseline["interior_success_by_1s_rate"],
            "interior_success_by_2s_rate": baseline["interior_success_by_2s_rate"],
            "interior_success_by_5s_rate": baseline["interior_success_by_5s_rate"],
            "eventual_led_rate": baseline["eventual_led_rate"],
            "eventual_followed_rate": baseline["eventual_followed_rate"],
            "choice_fraction": baseline["choice_fraction"],
            "choice_fraction_submission": baseline["choice_fraction_submission"],
            "choice_fraction_drain": baseline["choice_fraction_drain"],
            "total_assembled_groups": baseline["total_assembled_groups"],
            "x_assembled_leads": baseline["x_assembled_leads"],
            "honest_read_start_rate_per_enabled_second": baseline["honest_read_start_rate_per_enabled_second"],
            "honest_read_start_interval_median_seconds": baseline["honest_read_start_interval_median_seconds"],
            "honest_successful_read_latency_median_seconds": baseline["honest_successful_read_latency_median_seconds"],
            "honest_x_visible_fraction": baseline["honest_x_visible_fraction"],
        }
        for phase in ("submission", "drain"):
            for metric in ("read_start_rate_per_enabled_second",
                           "read_start_interval_median_seconds",
                           "successful_read_latency_median_seconds", "x_visible_fraction"):
                key = f"honest_{phase}_{metric}"
                base_rates[key] = baseline[key]
        for case, row in sorted(rows.items()):
            if case == "passive_stock":
                continue
            treatment_rates = {
                "success_by_1s_rate": row["success_by_1s_rate"],
                "success_by_2s_rate": row["success_by_2s_rate"],
                "success_by_5s_rate": row["success_by_5s_rate"],
                "interior_led_rate": row["interior_led_rate"],
                "interior_success_by_1s_rate": row["interior_success_by_1s_rate"],
                "interior_success_by_2s_rate": row["interior_success_by_2s_rate"],
                "interior_success_by_5s_rate": row["interior_success_by_5s_rate"],
                "eventual_led_rate": row["eventual_led_rate"],
                "eventual_followed_rate": row["eventual_followed_rate"],
                "choice_fraction": row["choice_fraction"],
                "choice_fraction_submission": row["choice_fraction_submission"],
                "choice_fraction_drain": row["choice_fraction_drain"],
                "total_assembled_groups": row["total_assembled_groups"],
                "x_assembled_leads": row["x_assembled_leads"],
                "honest_read_start_rate_per_enabled_second": row["honest_read_start_rate_per_enabled_second"],
                "honest_read_start_interval_median_seconds": row["honest_read_start_interval_median_seconds"],
                "honest_successful_read_latency_median_seconds": row["honest_successful_read_latency_median_seconds"],
                "honest_x_visible_fraction": row["honest_x_visible_fraction"],
            }
            for phase in ("submission", "drain"):
                for metric in ("read_start_rate_per_enabled_second",
                               "read_start_interval_median_seconds",
                               "successful_read_latency_median_seconds", "x_visible_fraction"):
                    key = f"honest_{phase}_{metric}"
                    treatment_rates[key] = row[key]
            output.append(dict(seed=seed, case=case, baseline_case="passive_stock", status="paired",
                               baseline=baseline, treatment=row,
                               differences={key: (treatment_rates[key] - base_rates[key]
                                                  if treatment_rates[key] is not None and base_rates[key] is not None
                                                  else None)
                                            for key in base_rates}))
    return output


def _discover_trial_dirs(path):
    dirs = []
    for config_path in path.rglob("config.json"):
        trial = config_path.parent
        relative_parts = trial.relative_to(path).parts
        if trial == path or "harness" in relative_parts:
            continue
        dirs.append(trial)
    return sorted(set(dirs))


def analyse_suite_choices(path, require_complete=True) -> dict:
    """Check a manifest-driven Step 25 detailed grouping suite."""
    path = Path(path)
    suite = read_json(path / "suite.json")
    require(isinstance(suite, dict), "invalid Step 25 suite manifest")
    require(suite.get("outcome") == "grouping", "Step 25 choices require grouping outcome")
    require(suite.get("instrumentation") == "detailed",
            "Step 25 choices require detailed instrumentation")
    provenance_path = path / "provenance.json"
    provenance = read_json(provenance_path) if provenance_path.exists() else suite.get("provenance")
    require(isinstance(provenance, dict) and isinstance(provenance.get("harness_sha256"), dict)
            and bool(provenance["harness_sha256"]), "suite missing Step 25 harness provenance")
    snapshot_digests = {}
    for name, digest in provenance["harness_sha256"].items():
        snapshot = path / "harness" / name
        try:
            actual = hashlib.sha256(snapshot.read_bytes()).hexdigest()
        except OSError as exc:
            raise ValueError(f"missing harness snapshot {name}") from exc
        require(actual == digest, f"harness snapshot mismatch: {name}")
        snapshot_digests[name] = actual

    jobs = suite.get("jobs")
    require(isinstance(jobs, list) and bool(jobs), "suite jobs must be a nonempty list")
    expected = {}
    for job in jobs:
        require(isinstance(job, dict) and isinstance(job.get("case"), dict), "invalid suite job")
        require(job.get("outcome", "grouping") == "grouping", "suite job outcome differs from grouping")
        key = _json_key(job["case"], job.get("seed"), job.get("duration"))
        require(key not in expected, "duplicate suite job")
        expected[key] = job

    trials, discovered_keys, incomplete_trials = [], set(), []
    input_hashes = {}
    for trial_path in _discover_trial_dirs(path):
        config = read_json(trial_path / "config.json")
        require(isinstance(config, dict), f"invalid trial config: {trial_path}")
        require(config.get("outcome") == "grouping", f"trial is not grouping-only: {trial_path}")
        require(config.get("instrumentation") == "detailed", f"trial lacks detailed instrumentation: {trial_path}")
        key = _json_key(config.get("case"), config.get("seed"), config.get("duration"))
        require(key in expected, f"unexpected trial configuration: {trial_path}")
        require(key not in discovered_keys, f"duplicate trial for manifest job: {trial_path}")
        discovered_keys.add(key)
        if config.get("case") != expected[key]["case"]:
            raise ValueError(f"trial case differs from suite manifest: {trial_path}")
        missing = [filename for filename in ("events.jsonl", "summary.json")
                   if not (trial_path / filename).is_file()]
        if missing:
            incomplete_trials.append(dict(path=str(trial_path), missing=missing))
            continue
        trial = analyse_trial_choices(trial_path)
        trials.append(trial)
        for filename in ("config.json", "events.jsonl", "summary.json"):
            file_path = trial_path / filename
            input_hashes[str(file_path.relative_to(path))] = hashlib.sha256(file_path.read_bytes()).hexdigest()

    missing_jobs = [job for key, job in expected.items() if key not in discovered_keys]
    incomplete = bool(missing_jobs or incomplete_trials or len(trials) != len(expected))
    if require_complete and incomplete:
        raise ValueError(f"incomplete Step 25 suite: {len(trials)}/{len(expected)} complete trials")
    halt_path = path / "halt.json"
    halt = read_json(halt_path) if halt_path.exists() else None
    step24_pass = not incomplete and halt is None and all(row["step24_control_plane_pass"] for row in trials)
    trace_pass = not incomplete and all(row["trace_integrity_pass"] for row in trials)
    control_plane_pass = step24_pass and trace_pass
    policy_table = _aggregate_trials(trials)
    paired = _paired_differences(policy_table)
    return dict(phase=suite.get("phase"), run_kind=suite.get("run_kind"), outcome="grouping",
                instrumentation="detailed", complete=not incomplete,
                expected_trials=len(expected), trial_count=len(trials),
                missing_jobs=missing_jobs, incomplete_trials=incomplete_trials,
                halted=halt is not None, halt=halt,
                step24_control_plane_pass=step24_pass,
                trace_integrity_pass=trace_pass,
                control_plane_pass=control_plane_pass, **{"pass": control_plane_pass},
                trials=trials, aggregate=policy_table, policy_table=policy_table,
                paired_differences_vs_passive_stock=paired,
                provenance=dict(suite_path=str(path),
                                suite_sha256=hashlib.sha256((path / "suite.json").read_bytes()).hexdigest(),
                                suite_provenance_sha256=(hashlib.sha256(provenance_path.read_bytes()).hexdigest()
                                                         if provenance_path.exists() else None),
                                raw_input_sha256=input_hashes,
                                harness_snapshot_sha256=snapshot_digests,
                                analyzer_snapshot_sha256=snapshot_digests.get(Path(__file__).name),
                                source_sha256=provenance.get("source_sha256", {}),
                                analyzer_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                                analyzer_matches_snapshot=(
                                    snapshot_digests.get(Path(__file__).name) ==
                                    hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
                                    if Path(__file__).name in snapshot_digests else None)),
                trace_integrity_errors=sorted({error for row in trials
                                               for error in row["trace_integrity_errors"]}),
                step24_control_integrity_errors=sorted({error for row in trials
                                                        for error in row["step24_control_integrity_errors"]}))


def _csv_value(value):
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return value


def _write_csv(path, rows, preferred_fields=()):
    rows = list(rows)
    all_fields = set()
    for row in rows:
        all_fields.update(row)
    fields = list(preferred_fields) + sorted(all_fields - set(preferred_fields))
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _csv_value(value) for key, value in row.items()})


def _policy_csv_rows(policy_table):
    rows = []
    for policy in policy_table:
        rows.append({key: value for key, value in policy.items() if key != "per_seed"})
    return rows


def _paired_csv_rows(paired):
    rows = []
    for pair in paired:
        row = dict(seed=pair["seed"], case=pair["case"],
                   baseline_case=pair["baseline_case"], status=pair["status"])
        baseline, treatment, differences = pair.get("baseline"), pair.get("treatment"), pair.get("differences")
        for prefix, source in (("baseline", baseline), ("treatment", treatment)):
            if not isinstance(source, dict):
                continue
            for key in ("x_attempt_denominator", "interior_attempt_denominator",
                        "success_by_1s_rate", "success_by_2s_rate", "success_by_5s_rate",
                        "interior_led_rate", "interior_success_by_1s_rate",
                        "interior_success_by_2s_rate", "interior_success_by_5s_rate",
                        "total_assembled_groups", "x_assembled_leads",
                        "honest_enabled_seconds", "honest_read_start_count",
                        "honest_read_start_rate_per_enabled_second",
                        "honest_read_start_interval_median_seconds",
                        "honest_successful_read_latency_median_seconds",
                        "honest_x_visible_fraction",
                        "eligible_first_submission", "chose_X_submission",
                        "eligible_first_drain", "chose_X_drain",
                        "choice_fraction_submission", "choice_fraction_drain",
                        "choice_opportunities", "choice_fraction"):
                if key in source:
                    row[f"{prefix}_{key}"] = source[key]
            if "choice_opportunities" not in source and "eligible_first" in source:
                row[f"{prefix}_choice_opportunities"] = source["eligible_first"]
            if "choice_fraction" not in source and "choice_fraction" in source:
                row[f"{prefix}_choice_fraction"] = source["choice_fraction"]
        for key, value in (differences or {}).items():
            row[f"difference_{key}"] = value
        rows.append(row)
    return rows


def _flatten_trial_rows(suite, field):
    output = []
    for trial in suite["trials"]:
        rows = trial.get(field, [])
        iterator = rows.items() if isinstance(rows, dict) else enumerate(rows)
        for key, row in iterator:
            if isinstance(row, dict):
                metadata = dict(case=trial["case"], policy=trial["policy"], seed=trial["seed"],
                                trial_dir=trial["trial_dir"])
                if field == "per_peer":
                    metadata["peer_name"] = key
                output.append({**metadata, **row})
    return output


def write_outputs(suite, output_dir) -> dict:
    """Write a complete analysis bundle to a new directory outside its suite."""
    suite_path = Path(suite).resolve()
    output_dir = Path(output_dir).resolve()
    require(suite_path.is_dir(), f"suite directory does not exist: {suite_path}")
    require(not output_dir.exists(), f"output directory already exists: {output_dir}")
    require(output_dir != suite_path and suite_path not in output_dir.parents and
            output_dir not in suite_path.parents,
            "output directory must be external to and separate from the suite")
    result = analyse_suite_choices(suite_path, require_complete=True)
    output_dir.mkdir(parents=True)
    (output_dir / "analysis.json").write_text(json.dumps(result, indent=2, allow_nan=False) + "\n",
                                               encoding="utf-8")
    (output_dir / "trial_summaries.json").write_text(
        json.dumps(result["trials"], indent=2, allow_nan=False) + "\n", encoding="utf-8")
    (output_dir / "provenance.json").write_text(
        json.dumps(result["provenance"], indent=2, allow_nan=False) + "\n", encoding="utf-8")
    _write_csv(output_dir / "policy_table.csv", _policy_csv_rows(result["policy_table"]))
    _write_csv(output_dir / "paired_differences_vs_passive_stock.csv",
               _paired_csv_rows(result["paired_differences_vs_passive_stock"]))
    _write_csv(output_dir / "per_seed.csv",
               [{**seed, "case": policy["case"]}
                for policy in result["policy_table"] for seed in policy["per_seed"]])
    for filename, field in (("attempts.csv", "attempts"),
                            ("selections.csv", "selections"),
                            ("reads.csv", "reads"),
                            ("groups.csv", "groups"),
                            ("joins.csv", "joins"),
                            ("per_peer.csv", "per_peer")):
        _write_csv(output_dir / filename, _flatten_trial_rows(result, field))
    return dict(output_dir=str(output_dir), analysis_json=str(output_dir / "analysis.json"),
                trial_summaries_json=str(output_dir / "trial_summaries.json"),
                provenance_json=str(output_dir / "provenance.json"),
                csv_files={name: str(output_dir / name) for name in CSV_NAMES},
                control_plane_pass=result["control_plane_pass"],
                suite_result=result)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("suite", type=Path, help="Step 25 detailed suite result directory")
    parser.add_argument("--label", required=True,
                        help="write fresh outputs to results/step25_analysis_LABEL")
    parser.add_argument("--output-dir", type=Path,
                        help="explicit fresh external output directory")
    args = parser.parse_args(argv)
    if not args.label or any(ch not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
                             for ch in args.label):
        parser.error("label must contain letters, numbers, underscores or hyphens")
    output = args.output_dir or (Path(__file__).resolve().parent / "results" /
                                 f"step25_analysis_{args.label}")
    result = write_outputs(args.suite, output)
    print(f"Saved Step 25 choice analysis to {result['output_dir']}")
    print(f"Control-plane pass: {result['control_plane_pass']}")
    return 0 if result["control_plane_pass"] else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ValueError as exc:
        print(f"analysis failed: {exc}", file=sys.stderr)
        raise SystemExit(2)
