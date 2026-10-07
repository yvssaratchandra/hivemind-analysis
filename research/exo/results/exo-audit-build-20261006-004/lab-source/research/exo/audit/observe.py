"""Read-only helpers for observing local Exo audit runs.

This module deliberately has no Exo imports. It parses captured logs and JSON
snapshots, and its ``--snapshot`` mode only reads the local Exo HTTP API.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Iterable, Mapping, Sequence
from typing import Any
from urllib.request import urlopen


_NODE_ID = re.compile(r"\bStarting node\s+([0-9a-f]{1,32})(?=\s|$)", re.IGNORECASE)
_MASTER_ID = re.compile(
    r"\bmaster_node_id\s*=\s*(?:NodeId\()?\s*['\"]?([0-9a-f]{1,32})['\"]?\s*\)?",
    re.IGNORECASE,
)
_ELECTION_CLOCK = re.compile(r"\belection_clock\s*=\s*(-?\d+)\b")
_CURRENT_SESSION = "Current session:"
_ELECTION_SESSION = "Election finished, new SessionId("
_CAMPAIGN_LAUNCH = re.compile(
    r"\bStarting (?:initial|new) campaign\b|\bCampaign started\b"
)
_ELECTION_STARTED = re.compile(r"\bElection (\d+) started\b")
_ELECTION_ENDED = re.compile(r"\bElection (\d+) (?:cancelled|finally)\b")


def _session_from_text(value: str) -> dict[str, Any] | None:
    """Parse only the two SessionId fields used for consensus comparison."""
    master_match = _MASTER_ID.search(value)
    clock_match = _ELECTION_CLOCK.search(value)
    if master_match is None or clock_match is None:
        return None

    master_node_id = master_match.group(1).lower()
    election_clock = int(clock_match.group(1))
    if election_clock < 0:
        return None
    return {
        "master_node_id": master_node_id,
        "election_clock": election_clock,
    }


def _latest_node_id_and_start_line(log: str) -> tuple[str | None, int | None]:
    latest_id: str | None = None
    latest_line: int | None = None
    for line_number, line in enumerate(log.splitlines()):
        match = _NODE_ID.search(line)
        if match is not None:
            latest_id = match.group(1).lower()
            latest_line = line_number
    return latest_id, latest_line


def _latest_committed_session(
    log: str, start_line: int | None
) -> tuple[dict[str, Any] | None, bool, int | None]:
    """Return the latest committed SessionId after this process's startup.

    Election candidate queues are intentionally excluded: for an election
    completion line, only the SessionId before ``with queue`` is inspected.
    """
    if start_line is None:
        return None, False, None

    latest_payload: str | None = None
    latest_line: int | None = None
    lines = log.splitlines()
    for line_number, line in enumerate(lines):
        if line_number < start_line:
            continue
        current_index = line.find(_CURRENT_SESSION)
        election_index = line.find(_ELECTION_SESSION)
        if current_index >= 0:
            latest_payload = line[current_index + len(_CURRENT_SESSION) :]
            latest_line = line_number
        elif election_index >= 0:
            payload = line[election_index + len(_ELECTION_SESSION) :]
            latest_payload = payload.split(") with queue", 1)[0]
            latest_line = line_number

    if latest_payload is None:
        return None, False, None
    parsed = _session_from_text(latest_payload)
    return parsed, parsed is None, latest_line


def _campaign_in_flight_after(log: str, committed_line: int | None) -> bool:
    """Detect a campaign launched after the latest committed session.

    These markers match Election.run/_connection_receiver/_election_receiver
    in the pinned source. A scheduled campaign stays pending until its task
    starts; started campaigns clear only on their matching cancel/finally line.
    """
    if committed_line is None:
        return False

    pending_campaign = False
    active_clocks: set[str] = set()
    for line_number, line in enumerate(log.splitlines()):
        if line_number <= committed_line:
            continue
        if _CAMPAIGN_LAUNCH.search(line):
            pending_campaign = True
        started = _ELECTION_STARTED.search(line)
        if started is not None:
            active_clocks.add(started.group(1))
            pending_campaign = False
        ended = _ELECTION_ENDED.search(line)
        if ended is not None:
            active_clocks.discard(ended.group(1))
    return pending_campaign or bool(active_clocks)


def session_consensus(logs: dict[str, str]) -> dict[str, Any]:
    """Compare the latest logged committed Exo session across services.

    The function ignores election-candidate text unless it is inside the
    explicitly selected SessionId on an ``Election finished`` line.
    """
    reasons: list[str] = []
    node_ids: dict[str, str] = {}
    sessions: dict[str, dict[str, Any] | None] = {}
    campaign_in_flight = False

    if not logs:
        reasons.append("no service logs supplied")
    if len(logs) != 2:
        reasons.append("exactly two peer logs are required")

    for service, raw_log in logs.items():
        log = raw_log if isinstance(raw_log, str) else ""
        node_id, start_line = _latest_node_id_and_start_line(log)
        if node_id is None:
            reasons.append(f"{service}: no Starting node identity found")
        else:
            node_ids[service] = node_id

        session, invalid_latest, committed_line = _latest_committed_session(
            log, start_line
        )
        sessions[service] = session
        if session is None:
            if invalid_latest:
                reasons.append(f"{service}: latest committed session is malformed")
            else:
                reasons.append(f"{service}: no committed session after startup")
        if _campaign_in_flight_after(log, committed_line):
            campaign_in_flight = True
            reasons.append(
                f"{service}: election campaign remains in flight after latest committed session"
            )

    if len(node_ids) != len(logs) or len(set(node_ids.values())) != len(node_ids):
        reasons.append("current node IDs are missing or not distinct")

    parsed_sessions = list(sessions.values())
    common_session: dict[str, Any] | None = None
    if parsed_sessions and all(session is not None for session in parsed_sessions):
        first_session = parsed_sessions[0]
        if all(session == first_session for session in parsed_sessions):
            common_session = first_session
        else:
            reasons.append("latest committed sessions do not match across services")

    if common_session is not None and common_session["master_node_id"] not in set(
        node_ids.values()
    ):
        reasons.append("common session master is not one of the current node IDs")

    converged = (
        len(logs) == 2
        and len(node_ids) == len(logs)
        and len(set(node_ids.values())) == len(node_ids)
        and not campaign_in_flight
        and common_session is not None
        and common_session["master_node_id"] in set(node_ids.values())
    )
    return {
        "converged": converged,
        "node_ids": node_ids,
        "session": common_session,
        "reasons": list(dict.fromkeys(reasons)),
    }


def _mapping_value(mapping: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        value = mapping.get(key)
        if value is not None:
            return value
    return None


def _byte_count(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def memory_available(state: dict[str, Any], node_id: str) -> int | None:
    """Read one node's available RAM bytes from either API naming style."""
    if not isinstance(state, Mapping):
        return None
    node_memory = _mapping_value(state, "nodeMemory", "node_memory")
    if not isinstance(node_memory, Mapping):
        return None
    node = node_memory.get(node_id)
    if not isinstance(node, Mapping):
        return None
    ram_available = _mapping_value(node, "ramAvailable", "ram_available")
    if isinstance(ram_available, Mapping):
        ram_available = _mapping_value(ram_available, "inBytes", "in_bytes")
    return _byte_count(ram_available)


def _record_kind(record: Mapping[str, Any]) -> str:
    raw = _mapping_value(record, "type", "kind", "record_type", "event_type")
    if not isinstance(raw, str):
        return ""
    kind = raw.strip().lower().replace("-", "_")
    aliases = {
        "publisher_ready": "ready",
        "ready": "ready",
        "publish": "published",
        "published": "published",
        "event_published": "published",
        "receive": "received",
        "received": "received",
        "event_received": "received",
        "indexed": "received",
    }
    return aliases.get(kind, kind)


def _record_service(record: Mapping[str, Any]) -> str | None:
    value = _mapping_value(
        record, "observer", "service", "receiver", "source_service"
    )
    return value if isinstance(value, str) and value else None


def _iter_records(records: Any) -> Iterable[Mapping[str, Any]]:
    """Accept a record list, an envelope with ``records``, or service buckets."""
    if isinstance(records, Mapping):
        if any(
            key in records
            for key in ("type", "kind", "record_type", "event_type")
        ):
            yield records
            return
        nested = records.get("records")
        if isinstance(nested, Sequence) and not isinstance(nested, (str, bytes)):
            for item in nested:
                if isinstance(item, Mapping):
                    yield item
            return
        for service, bucket in records.items():
            if isinstance(bucket, Sequence) and not isinstance(bucket, (str, bytes)):
                for item in bucket:
                    if isinstance(item, Mapping):
                        if _record_service(item) is None:
                            copied = dict(item)
                            copied["service"] = str(service)
                            yield copied
                        else:
                            yield item
        return

    if isinstance(records, Sequence) and not isinstance(records, (str, bytes)):
        for item in records:
            if isinstance(item, Mapping):
                yield item


def _arm_name(arm: str) -> str:
    normalized = arm.strip().lower().replace("_", "-").split("/", 1)[0]
    if normalized in {"no-send", "no-sends", "baseline"}:
        return "baseline"
    return normalized


def _global_index(record: Mapping[str, Any]) -> int | None:
    value = record.get("origin_idx")
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _session_key(value: Any) -> Any:
    if isinstance(value, Mapping):
        master = _mapping_value(value, "master_node_id", "masterNodeId")
        clock = _mapping_value(value, "election_clock", "electionClock")
        if isinstance(master, str) and isinstance(clock, int) and not isinstance(
            clock, bool
        ):
            return (master.lower(), clock)
    return value


def _payload_origin(payload: Any) -> str | None:
    """Find the origin on a serialized or decoded GlobalForwarderEvent."""
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except (TypeError, ValueError):
            return None
    if not isinstance(payload, Mapping):
        return None

    for key in ("origin", "origin_node_id", "originNodeId"):
        value = payload.get(key)
        if isinstance(value, str) and value:
            return value.lower()
    for value in payload.values():
        found = _payload_origin(value)
        if found is not None:
            return found
    return None


def _payload_node_gathered_info(payload: Any) -> Mapping[str, Any] | None:
    """Find the tagged NodeGatheredInfo inside a serialized global event."""
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except (TypeError, ValueError):
            return None
    if not isinstance(payload, Mapping):
        return None

    for tag in ("NodeGatheredInfo", "node_gathered_info"):
        value = payload.get(tag)
        if isinstance(value, Mapping):
            return value
    event_type = _mapping_value(payload, "type", "event_type", "eventType")
    if event_type == "NodeGatheredInfo" and isinstance(
        _mapping_value(payload, "info"), Mapping
    ):
        return payload
    if (
        isinstance(_mapping_value(payload, "node_id", "nodeId"), str)
        and isinstance(_mapping_value(payload, "info"), Mapping)
        and _mapping_value(payload, "event_id", "eventId") is not None
    ):
        return payload
    for value in payload.values():
        found = _payload_node_gathered_info(value)
        if found is not None:
            return found
    return None


def _memory_usage_available(info: Any) -> int | None:
    if not isinstance(info, Mapping):
        return None
    tagged = _mapping_value(info, "MemoryUsage", "memory_usage")
    if isinstance(tagged, Mapping):
        info = tagged
    ram_available = _mapping_value(info, "ramAvailable", "ram_available")
    if isinstance(ram_available, Mapping):
        memory = _mapping_value(ram_available, "Memory", "memory")
        if isinstance(memory, Mapping):
            ram_available = memory
        ram_available = _mapping_value(ram_available, "inBytes", "in_bytes")
    return _byte_count(ram_available)


def _payload_matches_marker(
    payload: Any,
    *,
    event_id: str,
    target_node_id: str | None,
    expected_bytes: int | None,
) -> bool:
    event = _payload_node_gathered_info(payload)
    if event is None:
        return False
    payload_event_id = _mapping_value(event, "event_id", "eventId")
    payload_node_id = _mapping_value(event, "node_id", "nodeId")
    if (
        payload_event_id != event_id
        or not isinstance(target_node_id, str)
        or not isinstance(payload_node_id, str)
        or payload_node_id.lower() != target_node_id.lower()
    ):
        return False
    info = _mapping_value(event, "info")
    return (
        expected_bytes is not None
        and _memory_usage_available(info) == expected_bytes
    )


def _replay_envelope(record: Mapping[str, Any]) -> tuple[Any, ...]:
    fields = (
        "event_id",
        "node_id",
        "origin_idx",
        "session",
        "available_bytes",
        "payload",
        "publisher_node_id",
    )
    return tuple(_session_key(record.get(field)) for field in fields)


def assess_probe(
    arm: str,
    records: Any,
    before: dict[str, dict[str, Any]],
    after: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Summarize observations for one local probe arm without a verdict.

    ``received`` records are acknowledgements from Exo's indexed global event
    stream. An accepted observation requires such an acknowledgement and the
    claimed node's available memory value on both honest post-run snapshots.
    A stale-session arm with no acknowledgement is only ``not_observed``;
    absence does not establish rejection.
    """
    normalized_arm = _arm_name(arm)
    all_records = list(_iter_records(records))
    ready_records = [
        record for record in all_records if _record_kind(record) == "ready"
    ]
    published = [
        record for record in all_records if _record_kind(record) == "published"
    ]
    received = [record for record in all_records if _record_kind(record) == "received"]
    reasons: list[str] = []

    if normalized_arm == "baseline":
        outcome = "no_send_control" if not published else "unexpected_send_observed"
        if published:
            reasons.append("baseline arm contains published records")
        return {
            "outcome": outcome,
            "event_id": None,
            "per_service_marker_matches": {},
            "indexed_occurrences": [],
            "reasons": reasons,
        }

    event_ids = {
        record.get("event_id")
        for record in published
        if isinstance(record.get("event_id"), str) and record.get("event_id")
    }
    event_ids.discard(None)
    event_id = next(iter(event_ids)) if len(event_ids) == 1 else None
    if not ready_records:
        reasons.append("publisher readiness record is missing")
    if not published:
        reasons.append("published marker record is missing")
    if len(event_ids) > 1:
        reasons.append("published records contain more than one marker event ID")
    if event_id is None:
        return {
            "outcome": "inconclusive",
            "event_id": None,
            "per_service_marker_matches": {},
            "indexed_occurrences": [],
            "reasons": reasons or ["marker event ID is unavailable"],
        }

    marker_publishes = [
        record for record in published if record.get("event_id") == event_id
    ]
    if len(marker_publishes) != len(published):
        reasons.append("some published records have no marker event ID")
    if normalized_arm == "replay":
        if len(marker_publishes) != 2:
            reasons.append("replay arm must publish the same envelope exactly twice")
        elif _replay_envelope(marker_publishes[0]) != _replay_envelope(
            marker_publishes[1]
        ):
            reasons.append("replay publications do not contain the same envelope")
    elif len(marker_publishes) != 1:
        reasons.append("non-replay arm must publish one marker envelope")

    publisher_ids = {
        record.get("publisher_node_id")
        for record in marker_publishes
        if isinstance(record.get("publisher_node_id"), str)
    }
    ready_ids = {
        record.get("publisher_node_id")
        for record in ready_records
        if isinstance(record.get("publisher_node_id"), str)
    }
    if len(publisher_ids) != 1 or not ready_ids:
        reasons.append("publisher node identity is missing or ambiguous")
    elif publisher_ids != ready_ids:
        reasons.append("published publisher identity does not match readiness")

    marker_publish = marker_publishes[0] if marker_publishes else {}
    target_node_id = marker_publish.get("node_id")
    publisher_node_id = marker_publish.get("publisher_node_id")
    if not isinstance(target_node_id, str) or not target_node_id:
        reasons.append("claimed node identity is missing")
        target_node_id = None
    if normalized_arm == "self" and target_node_id != publisher_node_id:
        reasons.append("self arm does not claim the publisher's node identity")
    if normalized_arm == "forged" and target_node_id == publisher_node_id:
        reasons.append("forged arm does not claim a different node identity")

    published_session = _session_key(marker_publish.get("session"))
    session_master = marker_publish.get("session")
    if isinstance(session_master, Mapping):
        session_master = _mapping_value(
            session_master, "master_node_id", "masterNodeId"
        )
    else:
        session_master = None
    if isinstance(session_master, str):
        session_master = session_master.lower()

    expected_bytes = _byte_count(marker_publish.get("available_bytes"))
    if expected_bytes is None:
        reasons.append("published available_bytes is missing or invalid")

    valid_received: list[Mapping[str, Any]] = []
    for record in received:
        if record.get("event_id") != event_id:
            continue
        if _session_key(record.get("session")) != published_session:
            reasons.append("received record session does not match publication")
            continue
        if not isinstance(session_master, str) or _payload_origin(
            record.get("payload")
        ) != session_master:
            reasons.append("received payload origin does not match session master")
            continue
        if not _payload_matches_marker(
            record.get("payload"),
            event_id=event_id,
            target_node_id=target_node_id,
            expected_bytes=expected_bytes,
        ):
            reasons.append("received NodeGatheredInfo does not match published marker")
            continue
        if _record_service(record) != "attacker":
            reasons.append("indexed acknowledgement is not from the attacker listener")
            continue
        observer_node_id = record.get("observer_node_id")
        if (
            not isinstance(publisher_node_id, str)
            or not isinstance(observer_node_id, str)
            or observer_node_id.lower() != publisher_node_id.lower()
        ):
            reasons.append(
                "indexed acknowledgement observer identity is missing or mismatched"
            )
            continue
        valid_received.append(record)

    indexed_by_service: dict[str, set[int]] = {"attacker": set()}
    indexed_occurrences: set[int] = set()
    for record in valid_received:
        index = _global_index(record)
        if index is None:
            reasons.append("matching received record has no valid global event index")
            continue
        indexed_occurrences.add(index)
        service = _record_service(record) or "unattributed"
        indexed_by_service.setdefault(service, set()).add(index)

    honest_services = sorted(set(before) & set(after))
    per_service_matches: dict[str, list[int]] = {
        service: sorted(indexed_by_service.get(service, set()))
        for service in honest_services
    }
    for service, indices in indexed_by_service.items():
        if service not in per_service_matches:
            per_service_matches[service] = sorted(indices)

    state_observations: dict[str, dict[str, Any]] = {}
    all_snapshots_present = len(honest_services) == 2
    if len(honest_services) != 2:
        reasons.append("before and after must contain both honest peer snapshots")
    for service in honest_services:
        before_state = before.get(service)
        after_state = after.get(service)
        before_value = (
            memory_available(before_state, target_node_id)
            if isinstance(target_node_id, str) and isinstance(before_state, dict)
            else None
        )
        after_value = (
            memory_available(after_state, target_node_id)
            if isinstance(target_node_id, str) and isinstance(after_state, dict)
            else None
        )
        state_observations[service] = {
            "before_bytes": before_value,
            "after_bytes": after_value,
            "matches_after": expected_bytes is not None
            and after_value == expected_bytes,
        }
        if not isinstance(before_state, dict) or not isinstance(after_state, dict):
            all_snapshots_present = False

    all_after_match = (
        expected_bytes is not None
        and len(state_observations) == 2
        and all(item["matches_after"] for item in state_observations.values())
    )
    has_ack = bool(indexed_occurrences)

    structural_problem = any(
        reason
        in {
            "publisher readiness record is missing",
            "published marker record is missing",
            "published records contain more than one marker event ID",
            "some published records have no marker event ID",
            "replay arm must publish the same envelope exactly twice",
            "replay publications do not contain the same envelope",
            "non-replay arm must publish one marker envelope",
            "publisher node identity is missing or ambiguous",
            "published publisher identity does not match readiness",
            "claimed node identity is missing",
            "self arm does not claim the publisher's node identity",
            "forged arm does not claim a different node identity",
        }
        for reason in reasons
    )

    if structural_problem or not all_snapshots_present:
        outcome = "inconclusive"
        if not all_snapshots_present:
            reasons.append("complete before/after memory snapshots are unavailable")
    elif normalized_arm == "stale-session" and not has_ack:
        outcome = "not_observed"
        reasons.append("stale-session marker was not observed during this run")
    elif not has_ack:
        outcome = "inconclusive"
        reasons.append("matching indexed acknowledgement is missing")
    elif not all_after_match:
        outcome = "inconclusive"
        reasons.append("marker value is not present in both honest post snapshots")
    elif normalized_arm == "replay":
        occurrence_count = len(indexed_occurrences)
        if occurrence_count == 1:
            outcome = "indexed_once"
        elif occurrence_count > 1:
            outcome = "reindexed"
        else:
            outcome = "inconclusive"
            reasons.append("matching acknowledgement has no indexed occurrence")
    else:
        outcome = "accepted"

    return {
        "outcome": outcome,
        "marker_is_synthetic": True,
        "event_id": event_id,
        "target_node_id": target_node_id,
        "publisher_node_id": publisher_node_id,
        "expected_available_bytes": expected_bytes,
        "per_service_marker_matches": per_service_matches,
        "indexed_occurrences": sorted(indexed_occurrences),
        "state_observations": state_observations,
        "reasons": list(dict.fromkeys(reasons)),
    }


def snapshot_payload(timeout: float = 3.0) -> dict[str, Any]:
    """Fetch this peer's node ID and state from its local Exo API."""

    def read_json(url: str) -> Any:
        with urlopen(url, timeout=timeout) as response:
            return json.load(response)

    node_id = read_json("http://localhost:52415/node_id")
    state = read_json("http://localhost:52415/state")
    if not isinstance(node_id, str):
        raise ValueError("/node_id did not return a JSON string")
    if not isinstance(state, dict):
        raise ValueError("/state did not return a JSON object")
    return {"node_id": node_id, "state": state}


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--snapshot",
        action="store_true",
        help="print this peer's localhost /node_id and /state as JSON",
    )
    args = parser.parse_args(argv)
    if not args.snapshot:
        parser.error("--snapshot is required")

    try:
        print(json.dumps(snapshot_payload(), sort_keys=True))
    except Exception as exc:
        print(f"snapshot failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
