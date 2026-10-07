#!/usr/bin/env python3
"""Analyze LocalAI/EdgeVPN control-plane observer traces.

The observer records decrypted, compressed EdgeVPN ledger snapshots. A snapshot
is a complete ledger state carried in a gossip envelope, not one RPC or one
operation per record. This analyzer keeps wire receptions separate from
deduplicated block snapshots and reports state differences as observations.
It never launches containers or contacts peers.
"""
from __future__ import annotations

import argparse
import base64
import datetime as dt
import gzip
import hashlib
import json
from pathlib import Path
import binascii
import re
import statistics
from typing import Any, Iterable


UTC = dt.timezone.utc
DEFAULT_ONLINE_SECONDS = 40
DEFAULT_HEALTHCHECK_SECONDS = 900
DEFAULT_EXAMPLES_PER_GROUP = 4
MAX_EVENT_EXAMPLES_PER_CATEGORY = 8

LIFECYCLE = [
    {
        "id": "discovery_and_connections",
        "order": 1,
        "description": "Peer discovery and transport connection events (mDNS is visible only if captured; DHT traffic may be outside this observer).",
    },
    {
        "id": "membership_and_authorization",
        "order": 2,
        "description": "User/peer registration records used by EdgeVPN admission checks.",
    },
    {
        "id": "service_registration_and_worker_discovery",
        "order": 3,
        "description": "EdgeVPN services and LocalAI worker advertisement records.",
    },
    {
        "id": "health_and_keepalive",
        "order": 4,
        "description": "Periodic health heartbeats and refreshed LastSeen fields.",
    },
    {
        "id": "shared_model_state",
        "order": 5,
        "description": "Federated application state such as synchronized model-name lists, when enabled.",
    },
    {
        "id": "ledger_replication",
        "order": 6,
        "description": "Encrypted gossip carries a compressed full-block ledger snapshot; the envelope can contain several record buckets.",
    },
    {
        "id": "churn_and_expiry",
        "order": 7,
        "description": "Observed state disappearance, branch/resync differences, stale/future records, and peer disconnects; none is an explicit leave message by itself.",
    },
    {
        "id": "unknown_or_unclassified",
        "order": 8,
        "description": "Unknown events or buckets retained for inspection rather than silently discarded.",
    },
]
LIFECYCLE_BY_ID = {row["id"]: row for row in LIFECYCLE}

DEFAULT_BUCKETS: dict[str, dict[str, str]] = {
    "users": {
        "category": "membership_and_authorization",
        "meaning": "PeerID-keyed registration record {PeerID, Timestamp}; source checks this bucket for stream authorization.",
    },
    "services": {
        "category": "service_registration_and_worker_discovery",
        "meaning": "EdgeVPN service record {PeerID, Name}.",
    },
    "healthcheck": {
        "category": "health_and_keepalive",
        "meaning": "PeerID-keyed RFC3339 heartbeat timestamp.",
    },
    "shared_state": {
        "category": "shared_model_state",
        "meaning": "Federated application shared state; the models key can carry model names.",
    },
}


def parse_time(value: Any) -> dt.datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    candidate = value.strip()
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"
    try:
        parsed = dt.datetime.fromisoformat(candidate)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def jsonl_rows(path: Path) -> Iterable[tuple[int, dict[str, Any] | None, str | None]]:
    """Stream JSONL records as (1-based line, object, error)."""
    try:
        stream = path.open(encoding="utf-8")
    except FileNotFoundError:
        return
    with stream:
        for line_number, line in enumerate(stream, 1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                yield line_number, None, f"invalid_json: {exc.msg if hasattr(exc, 'msg') else str(exc)}"
                continue
            if not isinstance(value, dict):
                yield line_number, None, "json_value_is_not_object"
                continue
            yield line_number, value, None


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def parse_data_value(value: Any) -> Any:
    """Decode EdgeVPN blockchain.Data: JSON string containing JSON, or native JSON."""
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return value


def normalize_block(raw: Any) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None
    # The observer's parsed Go block uses exported Go field names. Tolerate
    # lowercase names for fixtures and hand-authored traces.
    def get(name: str, default: Any = None) -> Any:
        return raw.get(name, raw.get(name.lower(), default))

    storage = get("Storage", {})
    if not isinstance(storage, dict):
        return None
    cleaned: dict[str, dict[str, Any]] = {}
    for bucket, records in storage.items():
        if not isinstance(bucket, str):
            continue
        if not isinstance(records, dict):
            # Preserve an unexpected bucket shape as a sentinel record.
            cleaned[bucket] = {"<bucket-value>": records}
            continue
        cleaned[bucket] = {str(key): parse_data_value(value) for key, value in records.items()}
    try:
        index = int(get("Index", -1))
    except (ValueError, TypeError):
        index = -1
    return {
        "index": index,
        "timestamp": get("Timestamp"),
        "hash": str(get("Hash", "")),
        "prev_hash": str(get("PrevHash", "")),
        "storage": cleaned,
    }


def load_protocol_reference(directory: Path) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    path = directory / "message_protocol_reference.json"
    if not path.exists():
        return dict(DEFAULT_BUCKETS), {"available": False, "file": path.name}
    try:
        reference = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return dict(DEFAULT_BUCKETS), {"available": False, "file": path.name, "error": str(exc)}
    buckets = dict(DEFAULT_BUCKETS)
    raw_buckets = reference.get("buckets", {}) if isinstance(reference, dict) else {}
    if isinstance(raw_buckets, dict):
        for name, description in raw_buckets.items():
            if isinstance(description, dict):
                buckets[str(name)] = dict(description)
            elif isinstance(description, str):
                buckets[str(name)] = {"meaning": description}
    elif isinstance(raw_buckets, list):
        for description in raw_buckets:
            if isinstance(description, dict) and description.get("name"):
                buckets[str(description["name"])] = dict(description)
    if isinstance(reference, dict):
        # The pinned source catalogue uses lifecycle_categories with nested
        # records_or_messages, rather than a flat buckets map. Flatten only
        # records that actually name a ledger bucket; keep transport messages
        # and local decisions in the source reference summary.
        for lifecycle in reference.get("lifecycle_categories", []) or []:
            if not isinstance(lifecycle, dict):
                continue
            for record in lifecycle.get("records_or_messages", []) or []:
                if not isinstance(record, dict) or not isinstance(record.get("bucket"), str):
                    continue
                bucket_text = record["bucket"]
                names = [candidate.strip() for candidate in bucket_text.split(",") if candidate.strip()]
                for name in names:
                    if name.startswith("<") and ">" not in name:
                        continue
                    bucket_key = name
                    standard = buckets.get(bucket_key, {})
                    record_name = record.get("name")
                    record_kind = record.get("kind")
                    meaning = f"{record_name} ({record_kind})" if record_name and record_kind else record_name or record_kind or "Source-derived ledger record"
                    buckets[bucket_key] = {
                        **standard,
                        "category": standard.get("category") or default_bucket_category(bucket_key),
                        "meaning": meaning,
                        "record_name": record.get("name"),
                        "record_kind": record.get("kind"),
                        "bucket": bucket_text,
                        "value_fields": record.get("value_fields", record.get("value_fields_written_by_ExposeService", [])),
                        "nominal_schedule": record.get("nominal_schedule"),
                        "source_notes": record.get("notes"),
                        "source_reference": record.get("source"),
                        "reference_lifecycle_category": lifecycle.get("category"),
                    }
    return buckets, {"available": True, "file": path.name, "source": reference}


def matches_reference_bucket(template: str, observed: str) -> bool:
    if template == observed:
        return True
    if not template.startswith("<"):
        return False
    # Some entries contain human-readable variants in a single `bucket` value
    # instead of a glob. Match the concrete LocalAI P2P service suffixes.
    for suffix in re.findall(r"<P2PNetworkID>_([A-Za-z0-9_-]+)", template):
        if suffix in ("federated", "worker", "mlx_worker") and (observed == suffix or observed.endswith("_" + suffix)):
            return True
    if "<service ID>" in template and observed.endswith(("_federated", "_worker", "_mlx_worker", "federated", "worker", "mlx_worker")):
        return True
    suffix = template.split(">", 1)[1].split(";", 1)[0].split(",", 1)[0].strip()
    return bool(suffix and (observed.endswith(suffix) or observed == suffix.lstrip("_")))


def bucket_metadata(name: str, reference_buckets: dict[str, dict[str, Any]]) -> dict[str, Any]:
    known = reference_buckets.get(name)
    if not known:
        for pattern, metadata in reference_buckets.items():
            if matches_reference_bucket(pattern, name):
                known = metadata
                break
    if known:
        category = known.get("lifecycle_category", known.get("category"))
        if category in LIFECYCLE_BY_ID:
            return {**known, "category": category, "known": True}
        if category:
            # Retain reference-defined categories in the data and place them in
            # the standard unknown bucket until the category taxonomy is known.
            return {**known, "category": "unknown_or_unclassified", "reference_category": category, "known": True}
        return {**known, "category": default_bucket_category(name), "known": True}
    category = default_bucket_category(name)
    return {"category": category, "meaning": "Not present in protocol reference; retained as an observed bucket.", "known": False}


def default_bucket_category(name: str) -> str:
    if name in DEFAULT_BUCKETS:
        return DEFAULT_BUCKETS[name]["category"]
    lower = name.casefold()
    if lower.endswith("_federated") or lower.endswith("_worker") or lower.endswith("_mlx_worker") or lower.endswith("_services") or lower.endswith("_workers"):
        return "service_registration_and_worker_discovery"
    if lower in ("federated", "worker", "mlx_worker"):
        return "service_registration_and_worker_discovery"
    if any(part in lower for part in ("model", "shared_state")):
        return "shared_model_state"
    return "unknown_or_unclassified"


def event_timestamp(row: dict[str, Any]) -> dt.datetime | None:
    for key in ("timestamp_utc", "timestamp", "time", "observed_at"):
        parsed = parse_time(row.get(key))
        if parsed:
            return parsed
    return None


def load_phases(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    events: list[dict[str, Any]] = []
    errors: list[str] = []
    for line_number, row, error in jsonl_rows(path):
        if error:
            errors.append(f"{path.name}:{line_number}: {error}")
            continue
        assert row is not None
        timestamp = event_timestamp(row)
        phase = row.get("phase")
        if timestamp is None or not isinstance(phase, str):
            errors.append(f"{path.name}:{line_number}: phase event lacks timestamp or phase")
            continue
        events.append({"timestamp": timestamp, "phase": phase, "action": str(row.get("action", "action")), "line": line_number})
    events.sort(key=lambda item: (item["timestamp"], item["line"]))
    return events, errors


def phase_at(timestamp: dt.datetime | None, phase_events: list[dict[str, Any]]) -> str:
    if timestamp is None:
        return "unknown"
    active = "before_recorded_phases"
    for event in phase_events:
        if event["timestamp"] > timestamp:
            break
        if event["action"] == "begin":
            active = event["phase"]
        elif event["action"] == "end" and active == event["phase"]:
            active = f"after_{event['phase']}"
        elif event["action"] == "action":
            active = event["phase"]
    return active


def value_summary(value: Any) -> dict[str, Any]:
    """Return a small, non-address-bearing preview for useful ledger values."""
    if not isinstance(value, dict):
        if isinstance(value, list):
            return {"type": "list", "item_count": len(value), "sample": value[:3]}
        if isinstance(value, str):
            return {"type": "string", "value": value[:120]}
        return {"type": type(value).__name__, "value": value}
    allowed = ("PeerID", "Name", "ID", "ServiceID", "LastSeen", "Timestamp", "models")
    selected = {key: value[key] for key in allowed if key in value}
    return {"type": "object", "fields": selected, "other_field_names": sorted(str(key) for key in value if key not in allowed)}


def record_freshness(bucket: str, value: Any, at: dt.datetime, online_seconds: int, health_seconds: int, future_skew_seconds: int) -> dict[str, Any] | None:
    timestamp = None
    field = None
    threshold = online_seconds
    if isinstance(value, dict):
        for key in ("LastSeen", "last_seen"):
            if key in value:
                timestamp = parse_time(value.get(key))
                field = key
                break
    elif bucket == "healthcheck":
        timestamp = parse_time(value)
        field = "value"
        threshold = health_seconds
    if field is None:
        return None
    if timestamp is None:
        return {"status": "unparseable_timestamp", "field": field, "threshold_seconds": threshold}
    age = (at - timestamp).total_seconds()
    if age < -future_skew_seconds:
        status = "future"
    elif age < 0:
        status = "slightly_future"
    elif age > threshold:
        status = "stale"
    else:
        status = "fresh"
    return {"status": status, "field": field, "age_seconds": round(age, 3), "threshold_seconds": threshold}


def stable_peer_id(value: Any, key: str) -> str:
    if isinstance(value, dict):
        return str(value.get("PeerID") or value.get("ID") or key)
    return key


def safe_int(value: Any, fallback: int) -> int:
    try:
        return int(value)
    except (ValueError, TypeError):
        return fallback


def parse_size_bytes(value: Any) -> int | None:
    if not isinstance(value, str):
        return None
    match = re.fullmatch(r"\s*([\d.]+)\s*(B|KiB|MiB|GiB|kB|MB|GB)\s*", value)
    if not match:
        return None
    scale = {"B": 1, "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3,
             "kB": 1000, "MB": 1000**2, "GB": 1000**3}
    return int(float(match[1]) * scale[match[2]])


def number_summary(values: list[float | int], divisor: float = 1.0) -> dict[str, Any] | None:
    if not values:
        return None
    ordered = sorted(values)
    p95 = ordered[max(0, (95 * len(ordered) + 99) // 100 - 1)]
    return {"sample_count": len(ordered), "min": min(ordered) / divisor,
            "median": statistics.median(ordered) / divisor,
            "p95": p95 / divisor, "max": max(ordered) / divisor}


class Analyzer:
    def __init__(self, directory: Path):
        self.directory = directory
        self.reference_buckets, self.reference_info = load_protocol_reference(directory)
        self.phases, self.phase_errors = load_phases(directory / "phases.jsonl")
        self.errors: list[str] = list(self.phase_errors)
        self.summary = self._read_json("summary.json")
        self.online_seconds = DEFAULT_ONLINE_SECONDS
        self.health_seconds = DEFAULT_HEALTHCHECK_SECONDS
        self.future_skew_seconds = 5
        source = self.reference_info.get("source", {})
        if isinstance(source, dict):
            timing = source.get("timing", source.get("timings", {}))
            if isinstance(timing, dict):
                self.online_seconds = safe_int(timing.get("node_online_seconds", timing.get("online_seconds")), self.online_seconds)
                self.health_seconds = safe_int(timing.get("healthcheck_max_age_seconds", timing.get("health_max_age_seconds")), self.health_seconds)
                self.future_skew_seconds = safe_int(timing.get("future_skew_seconds"), self.future_skew_seconds)
        self.events: list[dict[str, Any]] = []  # bounded examples only; counts are accumulated separately
        self.observer_ids: set[str] = set()
        self.change_groups: dict[tuple[str, str, str], dict[str, Any]] = {}
        self.bucket_absences: dict[str, dict[str, Any]] = {}
        self.category_phase_counts: dict[str, dict[str, dict[str, int]]] = {}
        self.phase_counts: dict[str, dict[str, int]] = {}
        self.unique_block_hashes: set[str] = set()
        self.observed_bucket_names: set[str] = set()
        self.unique_body_hashes: set[str] = set()
        self.seen_observer_blocks: dict[str, set[str]] = {}
        self.previous_state: dict[str, dict[str, dict[str, Any]]] = {}
        self.previous_hash: dict[str, str] = {}
        self.previous_index: dict[str, int] = {}
        self.latest_state_by_observer: dict[str, dict[str, dict[str, Any]]] = {}
        self.latest_observer_time: dict[str, dt.datetime] = {}
        self.latest_state_pointer: dict[str, dict[str, Any]] = {}
        self.connection_counts: dict[str, int] = {}
        self.event_counts: dict[str, int] = {}
        self.malformed_events: list[dict[str, Any]] = []
        self.capture_metadata_counts: dict[str, int] = {}
        self.capture_metadata_examples: list[dict[str, Any]] = []
        self.mdns_query_response_counts = {"query": 0, "response": 0}
        self.mdns_ttl_zero_count = 0
        self.mdns_ttl_zero_examples: list[dict[str, Any]] = []
        self.category_direct_event_counts: dict[str, dict[str, int]] = {}
        self.freshness_counts: dict[tuple[str, str], int] = {}
        self.freshness_examples: dict[tuple[str, str], list[dict[str, Any]]] = {}
        self.gossip_received = 0
        self.gossip_duplicates = 0
        self.gossip_unique_blocks = 0
        self.hash_mismatches = 0
        self.body_decode_failures = 0
        self.events_without_timestamp = 0
        self.parsed_observer_event_count = 0
        self.max_block_index = -1
        self.min_block_index: int | None = None

    def _read_json(self, filename: str) -> dict[str, Any]:
        path = self.directory / filename
        if not path.exists():
            return {}
        try:
            result = json.loads(path.read_text(encoding="utf-8"))
            return result if isinstance(result, dict) else {}
        except (OSError, json.JSONDecodeError) as exc:
            self.errors.append(f"{filename}: {exc}")
            return {}

    def _phase_counter(self, phase: str) -> dict[str, int]:
        return self.phase_counts.setdefault(phase, {"observer_events": 0, "gossip_receptions": 0, "unique_block_snapshots": 0, "record_additions": 0, "record_changes": 0, "record_removals": 0, "peer_connections": 0, "peer_disconnections": 0, "mdns_packets": 0, "unknown_events": 0})

    def _category_phase_counter(self, category: str, phase: str) -> dict[str, int]:
        by_phase = self.category_phase_counts.setdefault(category, {})
        return by_phase.setdefault(phase, {"observer_events": 0, "record_additions": 0, "record_changes": 0, "record_removals": 0, "record_initially_present": 0})

    def _count_direct_event(self, category: str, event_kind: str) -> None:
        counts = self.category_direct_event_counts.setdefault(category, {})
        counts[event_kind] = counts.get(event_kind, 0) + 1

    def _append_event_example(self, event: dict[str, Any]) -> None:
        category = str(event.get("lifecycle_category", "unknown_or_unclassified"))
        if sum(1 for existing in self.events if existing.get("lifecycle_category") == category) < MAX_EVENT_EXAMPLES_PER_CATEGORY:
            self.events.append(event)

    def _change(self, bucket: str, action: str, category: str, phase: str, pointer: dict[str, Any], key: str, value: Any, freshness: dict[str, Any] | None = None, metadata: dict[str, Any] | None = None) -> None:
        group_key = (category, bucket, action)
        group = self.change_groups.setdefault(group_key, {"lifecycle_category": category, "bucket": bucket, "action": action, "bucket_metadata": metadata or {}, "count": 0, "snapshot_lineage_counts": {}, "examples": []})
        group["count"] += 1
        lineage = str(pointer.get("chain_relation", "unknown"))
        group["snapshot_lineage_counts"][lineage] = group["snapshot_lineage_counts"].get(lineage, 0) + 1
        category_phase = self._category_phase_counter(category, phase)
        category_phase_key = {"added": "record_additions", "changed": "record_changes", "removed": "record_removals", "present_at_first_observation": "record_initially_present"}.get(action)
        if category_phase_key:
            category_phase[category_phase_key] += 1
        count_field = {"added": "record_additions", "changed": "record_changes", "removed": "record_removals"}.get(action)
        if count_field:
            self._phase_counter(phase)[count_field] += 1
        if len(group["examples"]) < DEFAULT_EXAMPLES_PER_GROUP:
            example = {**pointer, "phase": phase, "key": key}
            if action != "removed":
                example["value_summary"] = value_summary(value)
            if freshness:
                example["freshness"] = freshness
            group["examples"].append(example)

    def _record_freshness(self, bucket: str, key: str, value: Any, at: dt.datetime, pointer: dict[str, Any], phase: str) -> dict[str, Any] | None:
        status = record_freshness(bucket, value, at, self.online_seconds, self.health_seconds, self.future_skew_seconds)
        if not status:
            return None
        pair = (bucket, status["status"])
        self.freshness_counts[pair] = self.freshness_counts.get(pair, 0) + 1
        examples = self.freshness_examples.setdefault(pair, [])
        if len(examples) < DEFAULT_EXAMPLES_PER_GROUP:
            examples.append({**pointer, "phase": phase, "key": key, **status})
        return status

    def _validate_wire_body(self, row: dict[str, Any], parsed_block: dict[str, Any] | None, pointer: dict[str, Any]) -> None:
        encoded = row.get("body_base64", row.get("message_base64"))
        if not isinstance(encoded, str):
            return
        try:
            body = base64.b64decode(encoded, validate=True)
            actual = hashlib.sha256(body).hexdigest()
            expected = row.get("body_sha256", row.get("message_sha256"))
            if expected and actual.lower() != str(expected).lower():
                self.hash_mismatches += 1
                self.errors.append(f"{pointer['file']}:{pointer['line']}: body_sha256 mismatch")
            try:
                payload = gzip.decompress(body)
                decompressed = json.loads(payload)
            except (OSError, EOFError, gzip.BadGzipFile, json.JSONDecodeError):
                self.body_decode_failures += 1
                self.errors.append(f"{pointer['file']}:{pointer['line']}: observer body is not valid gzip JSON")
                return
            embedded = normalize_block(decompressed)
            if parsed_block and embedded and parsed_block["hash"] and embedded["hash"] != parsed_block["hash"]:
                self.errors.append(f"{pointer['file']}:{pointer['line']}: parsed block disagrees with compressed body")
        except (ValueError, binascii.Error):
            self.body_decode_failures += 1
            self.errors.append(f"{pointer['file']}:{pointer['line']}: invalid body_base64")

    def _flatten(self, storage: dict[str, dict[str, Any]]) -> dict[tuple[str, str], Any]:
        return {(bucket, key): value for bucket, records in storage.items() for key, value in records.items()}

    def _process_gossip(self, row: dict[str, Any], line_number: int) -> None:
        self.gossip_received += 1
        observer = str(row.get("observer_id", row.get("observer_peer_id", "unknown-observer")))
        if observer and observer != "unknown-observer":
            self.observer_ids.add(observer)
        timestamp = event_timestamp(row)
        phase = phase_at(timestamp, self.phases)
        counter = self._phase_counter(phase)
        counter["observer_events"] += 1
        counter["gossip_receptions"] += 1
        self.event_counts["gossip_message"] = self.event_counts.get("gossip_message", 0) + 1
        self._count_direct_event("ledger_replication", "gossip_receptions")
        if timestamp is None:
            self.events_without_timestamp += 1
        block_raw = row.get("block", row.get("block_payload"))
        if not isinstance(block_raw, dict) and isinstance(row.get("block_storage"), dict):
            block_raw = {"Index": row.get("block_index", -1), "Hash": row.get("block_hash", ""),
                         "PrevHash": row.get("block_prev_hash", ""), "Storage": row["block_storage"]}
        block = normalize_block(block_raw)
        message_hash = str(row.get("body_sha256") or row.get("message_body_sha256") or row.get("message_sha256") or "")
        pointer = {
            "file": "observer.jsonl",
            "line": line_number,
            "observer_id": observer,
            "message_sha256": message_hash or None,
            "received_from_peer_id": row.get("received_from_peer_id"),
        }
        self._validate_wire_body(row, block, pointer)
        if message_hash:
            self.unique_body_hashes.add(message_hash)
        if not block:
            self.malformed_events.append({**pointer, "event": "gossip_message", "error": "missing_or_invalid_block"})
            return
        block_hash = block["hash"] or f"observer-line-{line_number}"
        self.observed_bucket_names.update(block["storage"])
        pointer["block_hash"] = block_hash
        pointer["block_index"] = block["index"]
        category_phase = self._category_phase_counter("ledger_replication", phase)
        category_phase["observer_events"] += 1
        block_seen = self.seen_observer_blocks.setdefault(observer, set())
        unique_for_observer = block_hash not in block_seen
        if unique_for_observer:
            block_seen.add(block_hash)
            self.unique_block_hashes.add(block_hash)
            self.gossip_unique_blocks += 1
            counter["unique_block_snapshots"] += 1
            self.max_block_index = max(self.max_block_index, block["index"])
            self.min_block_index = block["index"] if self.min_block_index is None else min(self.min_block_index, block["index"])
        else:
            self.gossip_duplicates += 1
        self.latest_observer_time[observer] = timestamp or self.latest_observer_time.get(observer, dt.datetime.min.replace(tzinfo=UTC))
        self.latest_state_by_observer[observer] = block["storage"]
        self.latest_state_pointer[observer] = pointer

        snapshot_event = {
            "event": "ledger_snapshot_observed",
            "lifecycle_category": "ledger_replication",
            "phase": phase,
            "timestamp": timestamp.isoformat() if timestamp else None,
            "observer_id": observer,
            "block_index": block["index"],
            "block_hash": block_hash,
            "prev_hash": block["prev_hash"],
            "message_sha256": message_hash or None,
            "pointer": pointer,
            "duplicate_snapshot": not unique_for_observer,
            "diff_basis": "previous_observed_snapshot" if self.previous_state.get(observer) else "none",
            "chain_relation": "pending",
            "previous_observed_block_index": self.previous_index.get(observer),
            "bucket_names": sorted(block["storage"]),
            "record_count": sum(len(records) for records in block["storage"].values()),
        }
        # Whole snapshots may be retransmitted many times. Only compare the
        # first reception of a block per observer; never count each contained
        # record as an independent wire message.
        if not unique_for_observer:
            snapshot_event["chain_relation"] = "duplicate_snapshot"
            self._append_event_example(snapshot_event)
            return
        previous = self.previous_state.get(observer, {})
        previous_hash = self.previous_hash.get(observer)
        previous_index = self.previous_index.get(observer, -1)
        chain_relation = "not_direct_child_of_previous_observed_block" if previous_hash and block["prev_hash"] and block["prev_hash"] != previous_hash else "direct_child_or_initial"
        pointer["diff_basis"] = "previous_observed_snapshot" if previous else "none"
        pointer["previous_observed_block_hash"] = previous_hash if previous else None
        pointer["chain_relation"] = chain_relation
        if timestamp:
            # Snapshot states can be retransmitted many times. Assess embedded
            # ages once per unique block so unchanged records can become stale
            # during the run without being mistaken for a new wire operation.
            for bucket_name, records in block["storage"].items():
                for record_key, value in records.items():
                    self._record_freshness(bucket_name, record_key, value, timestamp, pointer, phase)
        if previous:
            prior_buckets = set(previous)
            current_buckets = set(block["storage"])
            for vanished_bucket in sorted(prior_buckets - current_buckets):
                transition = self.bucket_absences.setdefault(vanished_bucket, {"bucket": vanished_bucket, "action": "absent_from_observed_snapshot", "count": 0, "removed_record_count": 0, "lifecycle_category": "churn_and_expiry", "examples": []})
                transition["count"] += 1
                transition["removed_record_count"] += len(previous.get(vanished_bucket, {}))
                transition.setdefault("snapshot_lineage_counts", {})[chain_relation] = transition.setdefault("snapshot_lineage_counts", {}).get(chain_relation, 0) + 1
                if len(transition["examples"]) < DEFAULT_EXAMPLES_PER_GROUP:
                    transition["examples"].append({**pointer, "previous_block_hash": previous_hash, "previous_block_index": previous_index, "previous_record_count": len(previous.get(vanished_bucket, {})), "phase": phase})
            current_flat = self._flatten(block["storage"])
            previous_flat = self._flatten(previous)
            keys = set(current_flat) | set(previous_flat)
            for bucket, key in sorted(keys):
                before_present = (bucket, key) in previous_flat
                after_present = (bucket, key) in current_flat
                if before_present and after_present and canonical(previous_flat[(bucket, key)]) == canonical(current_flat[(bucket, key)]):
                    continue
                if not before_present:
                    action = "added"
                    value = current_flat[(bucket, key)]
                elif not after_present:
                    action = "removed"
                    value = previous_flat[(bucket, key)]
                else:
                    action = "changed"
                    value = current_flat[(bucket, key)]
                meta = bucket_metadata(bucket, self.reference_buckets)
                category = str(meta.get("category", "unknown_or_unclassified"))
                # Missing keys/buckets describe a state transition, not an
                # explicit leave packet. Separate churn from the record's
                # ordinary registration/heartbeat category.
                if action == "removed":
                    category = "churn_and_expiry"
                self._change(bucket, action, category, phase, pointer, key, value, metadata=meta)
        elif block["index"] > 0 and block["storage"]:
            # First observer snapshot may already contain multiple records.
            # Count them as observed state, with an explicit unknown baseline.
            for bucket, records in sorted(block["storage"].items()):
                for key, value in sorted(records.items()):
                    meta = bucket_metadata(bucket, self.reference_buckets)
                    self._change(bucket, "present_at_first_observation", str(meta.get("category", "unknown_or_unclassified")), phase, pointer, key, value, metadata=meta)
        snapshot_event["chain_relation"] = chain_relation
        snapshot_event["diff_basis"] = "previous_observed_snapshot" if previous else "none"
        snapshot_event["previous_observed_block_index"] = previous_index if previous else None
        self._append_event_example(snapshot_event)
        if unique_for_observer:
            self.previous_state[observer] = block["storage"]
            self.previous_hash[observer] = block_hash
            self.previous_index[observer] = block["index"]

    def _process_lifecycle_event(self, row: dict[str, Any], line_number: int) -> None:
        event = str(row.get("event", "unknown"))
        if event.casefold() == "gossip_message":
            self._process_gossip(row, line_number)
            return
        timestamp = event_timestamp(row)
        phase = phase_at(timestamp, self.phases)
        counter = self._phase_counter(phase)
        counter["observer_events"] += 1
        self.event_counts[event] = self.event_counts.get(event, 0) + 1
        observer_id = row.get("observer_id", row.get("observer_peer_id"))
        if observer_id:
            self.observer_ids.add(str(observer_id))
        pointer = {"file": "observer.jsonl", "line": line_number, "observer_id": observer_id, "timestamp": timestamp.isoformat() if timestamp else None}
        if timestamp is None:
            self.events_without_timestamp += 1
        if event in ("observer_started", "observer_stopping", "packet_capture_started", "packet_capture_error", "packet_capture_disabled"):
            self.capture_metadata_counts[event] = self.capture_metadata_counts.get(event, 0) + 1
            if len(self.capture_metadata_examples) < 12:
                self.capture_metadata_examples.append({"file": "observer.jsonl", "line": line_number, "timestamp": timestamp.isoformat() if timestamp else None, "event": event, "observer_peer_id": row.get("observer_peer_id"), "detail": {key: row.get(key) for key in ("role", "dht_enabled", "mdns_enabled", "localai_records_added", "interface", "filter", "error", "hint", "reason", "stream_events_available") if row.get(key) is not None}})
            return
        normalized_event = event.casefold()
        category = "unknown_or_unclassified"
        if normalized_event in ("peer_connection", "connection"):
            action = str(row.get("action", row.get("state", "unknown"))).casefold()
            if action in ("connect", "connected", "listen", "listening", "disconnect", "disconnected", "already_connected_at_observer_registration"):
                category = "discovery_and_connections"
                if action in ("connect", "connected"):
                    counter["peer_connections"] += 1
                    self.connection_counts["connect"] = self.connection_counts.get("connect", 0) + 1
                elif action == "already_connected_at_observer_registration":
                    self.connection_counts["already_connected_at_registration"] = self.connection_counts.get("already_connected_at_registration", 0) + 1
                elif action in ("disconnect", "disconnected"):
                    category = "churn_and_expiry"
                    counter["peer_disconnections"] += 1
                    self.connection_counts["disconnect"] = self.connection_counts.get("disconnect", 0) + 1
                else:
                    self.connection_counts["listen"] = self.connection_counts.get("listen", 0) + 1
            else:
                counter["unknown_events"] += 1
        elif normalized_event == "listen_address":
            state = str(row.get("state", "")).casefold()
            category = "churn_and_expiry" if state == "closed" else "discovery_and_connections"
            if state not in ("opened", "closed", "active_at_observer_registration"):
                counter["unknown_events"] += 1
        elif normalized_event in ("mdns_packet", "packet_capture_started", "packet_capture_error", "packet_capture_disabled"):
            category = "discovery_and_connections"
            if normalized_event == "mdns_packet":
                counter["mdns_packets"] += 1
                dns_packet = row.get("dns")
                if isinstance(dns_packet, dict):
                    direction = "response" if dns_packet.get("response") else "query"
                    self.mdns_query_response_counts[direction] += 1
                    for section in ("answers", "authority", "additional"):
                        for record in dns_packet.get(section) or []:
                            if not isinstance(record, dict):
                                continue
                            ttl = record.get("TTL", record.get("ttl"))
                            if str(ttl).strip() != "0":
                                continue
                            self.mdns_ttl_zero_count += 1
                            if len(self.mdns_ttl_zero_examples) < MAX_EVENT_EXAMPLES_PER_CATEGORY:
                                self.mdns_ttl_zero_examples.append({
                                    "file": "observer.jsonl", "line": line_number, "phase": phase,
                                    "section": section,
                                    "resource_record": {key: record.get(key) for key in ("name", "type", "class", "TTL", "ttl", "data", "target") if record.get(key) is not None},
                                })
        else:
            counter["unknown_events"] += 1
        if category == "unknown_or_unclassified":
            self.malformed_events.append({**pointer, "event": event, "error": "unknown_event_type_or_action", "fields": sorted(row)})
        if normalized_event in ("peer_connection", "connection"):
            action = str(row.get("action", row.get("state", "unknown"))).casefold()
            direct_kind = {
                "connect": "peer_connections", "connected": "peer_connections",
                "disconnect": "peer_disconnections", "disconnected": "peer_disconnections",
                "already_connected_at_observer_registration": "existing_connections",
                "listen": "peer_listen_events", "listening": "peer_listen_events",
            }.get(action, f"peer_connection_{action}")
            self._count_direct_event(category, direct_kind)
        elif normalized_event == "listen_address":
            state = str(row.get("state", "unknown")).casefold()
            self._count_direct_event(category, f"listen_address_{state}")
        elif normalized_event == "mdns_packet":
            dns_packet = row.get("dns")
            direction = "responses" if isinstance(dns_packet, dict) and dns_packet.get("response") else "queries"
            self._count_direct_event(category, f"mdns_{direction}")
        else:
            self._count_direct_event(category, f"events_{normalized_event}")
        self._category_phase_counter(category, phase)["observer_events"] += 1
        dns = row.get("dns")
        dns_summary = None
        if isinstance(dns, dict):
            dns_summary = {key: len(dns.get(section) or []) for key, section in (("questions", "questions"), ("answers", "answers"), ("authority", "authority"), ("additional", "additional"))}
            dns_summary["response"] = bool(dns.get("response"))
            dns_summary["question_examples"] = [{key: question.get(key) for key in ("name", "type") if question.get(key) is not None} for question in (dns.get("questions") or [])[:3] if isinstance(question, dict)]
            dns_summary["answer_examples"] = [{key: answer.get(key) for key in ("name", "type", "data") if answer.get(key) is not None} for answer in (dns.get("answers") or [])[:3] if isinstance(answer, dict)]
        self._append_event_example({**pointer, "event": event, "action": row.get("action", row.get("state")), "lifecycle_category": category, "peer_id": row.get("peer_id", row.get("remote_peer_id")), "remote_peer_id": row.get("remote_peer_id"), "detail": row.get("detail", {key: row.get(key) for key in ("direction", "connection_id", "local_multiaddr", "remote_multiaddr", "category", "multiaddr", "dns_payload_sha256", "dns_parse_error", "interface", "filter") if row.get(key) is not None}), "dns_summary": dns_summary})

    def process_observer(self) -> None:
        path = self.directory / "observer.jsonl"
        if not path.exists():
            self.errors.append("observer.jsonl is missing")
            return
        for line_number, row, error in jsonl_rows(path):
            if error:
                self.malformed_events.append({"file": path.name, "line": line_number, "error": error})
                continue
            assert row is not None
            self.parsed_observer_event_count += 1
            self._process_lifecycle_event(row, line_number)

    def process_model_controls(self) -> dict[str, Any]:
        path = self.directory / "model-controls.jsonl"
        counts: dict[str, int] = {}
        examples: list[dict[str, Any]] = []
        checks: list[dict[str, Any]] = []
        malformed = 0
        for line_number, row, error in jsonl_rows(path):
            if error:
                malformed += 1
                self.malformed_events.append({"file": path.name, "line": line_number, "error": error})
                continue
            assert row is not None
            kind = str(row.get("event", row.get("action", "empty_model_probe")))
            counts[kind] = counts.get(kind, 0) + 1
            if len(examples) < 8:
                examples.append({"file": path.name, "line": line_number, "event": kind, "timestamp": row.get("timestamp_utc", row.get("timestamp")), "phase": row.get("phase"), "result": row.get("result"), "status": row.get("status")})
            models = row.get("models", {})
            system = row.get("system", {})
            data = models.get("data") if isinstance(models, dict) else None
            loaded = system.get("loaded_models") if isinstance(system, dict) else None
            checks.append({"line": line_number, "phase": row.get("phase"), "host": row.get("host"), "configured_model_count": len(data) if isinstance(data, list) else None, "loaded_model_count": len(loaded) if isinstance(loaded, list) else None, "verified_empty": data == [] and loaded == []})
        return {"file": path.name, "present": path.exists(), "counts": counts, "malformed_lines": malformed, "examples": examples, "empty_model_checks": checks, "all_checks_empty": bool(checks) and all(check["verified_empty"] for check in checks), "interpretation": "LocalAPI/model-control observations are separate from peer-to-peer ledger messages; a routed HTTP request is not counted as a gossip control message."}

    def process_service_probe(self) -> dict[str, Any]:
        path = self.directory / "service-probe.json"
        if not path.exists():
            return {"file": path.name, "present": False}
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            return {"file": path.name, "present": True, "error": str(exc)}
        attempts = data.get("attempts", []) if isinstance(data, dict) else []
        statuses = [row.get("exit_code") for row in attempts if isinstance(row, dict)] if isinstance(attempts, list) else []
        return {"file": path.name, "present": True, "passed": data.get("passed"), "scope": data.get("scope"), "attempt_count": len(attempts) if isinstance(attempts, list) else 0, "exit_codes": statuses[:16], "interpretation": "The probe is a model-free gateway /v1/models request; it can verify service access without exercising model inference."}

    def process_completion_audit(self) -> dict[str, Any]:
        path = self.directory / "pilot-completion-audit.json"
        if not path.exists():
            return {"file": path.name, "present": False}
        try:
            audit = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            return {"file": path.name, "present": True, "error": str(exc)}
        return {"file": path.name, "present": True, "process_exit_code": audit.get("process_exit_code"), "outcome": audit.get("outcome"), "error": audit.get("error"), "original_summary_status": audit.get("original_summary_status"), "capture": audit.get("capture"), "note": audit.get("note")}

    def process_samples(self) -> dict[str, Any]:
        path = self.directory / "samples.jsonl"
        phases: dict[str, dict[str, Any]] = {}
        containers: dict[tuple[str, str], dict[str, list[float]]] = {}
        malformed = 0
        for line_number, row, error in jsonl_rows(path):
            if error:
                malformed += 1
                self.errors.append(f"{path.name}:{line_number}: {error}")
                continue
            assert row is not None
            phase = str(row.get("phase", "unknown"))
            item = phases.setdefault(phase, {"sample_count": 0, "timestamps": [], "fresh_membership_counts": [], "listed_federated_node_counts": [], "witness_hosts": set(), "fresh_host_sets": [], "host_mem_available_bytes": [], "containers": {}})
            item["sample_count"] += 1
            timestamp = parse_time(row.get("timestamp_utc", row.get("timestamp")))
            if timestamp:
                item["timestamps"].append(timestamp.isoformat())
            fresh_hosts = row.get("fresh_hosts", {})
            membership = row.get("membership", {})
            if isinstance(fresh_hosts, dict):
                for witness, hosts in fresh_hosts.items():
                    if isinstance(hosts, list):
                        item["witness_hosts"].add(str(witness))
                        item["fresh_membership_counts"].append(len(hosts))
                        item["fresh_host_sets"].append(tuple(sorted(str(host) for host in hosts)))
            if isinstance(membership, dict):
                for witness, payload in membership.items():
                    if not isinstance(payload, dict):
                        continue
                    all_nodes: list[Any] = []
                    for key in ("federated_nodes", "llama_cpp_nodes", "mlx_nodes"):
                        listed = payload.get(key)
                        if isinstance(listed, list):
                            all_nodes.extend(listed)
                    ids = {str(node.get("ID")) for node in all_nodes if isinstance(node, dict) and node.get("ID")}
                    item["listed_federated_node_counts"].append(len(ids))
                    item["witness_hosts"].add(str(witness))
            resources = row.get("resources", {})
            memory = resources.get("memory_bytes", {}) if isinstance(resources, dict) else {}
            available = memory.get("MemAvailable") if isinstance(memory, dict) else None
            if isinstance(available, (int, float)):
                item["host_mem_available_bytes"].append(int(available))
            stats = row.get("stats", [])
            if isinstance(stats, list):
                for stat in stats:
                    if not isinstance(stat, dict):
                        continue
                    name = str(stat.get("Name", "unknown-container"))
                    container = containers.setdefault((phase, name), {"memory_bytes": [], "cpu_percent": []})
                    mem_text = stat.get("MemUsage")
                    usage_text = mem_text.split("/", 1)[0] if isinstance(mem_text, str) else None
                    mem_value = parse_size_bytes(usage_text)
                    if mem_value is not None:
                        container["memory_bytes"].append(mem_value)
                    cpu_text = stat.get("CPUPerc")
                    try:
                        if isinstance(cpu_text, str):
                            container["cpu_percent"].append(float(cpu_text.strip().rstrip("%")))
                    except ValueError:
                        pass
        result_phases = {}
        stages_by_phase = {str(stage.get("phase")): stage for stage in self.summary.get("stages", []) if isinstance(stage, dict) and stage.get("phase")}
        for phase, item in sorted(phases.items()):
            fresh_sets = sorted(set(item["fresh_host_sets"]))
            stage = stages_by_phase.get(phase, {})
            stage_check = stage.get("all_peer_membership_match", stage.get("final_witness_match"))
            result_phases[phase] = {
                "sample_count": item["sample_count"],
                "first_timestamp": min(item["timestamps"]) if item["timestamps"] else None,
                "last_timestamp": max(item["timestamps"]) if item["timestamps"] else None,
                "membership_witnesses": sorted(item["witness_hosts"]),
                "fresh_membership_count": number_summary(item["fresh_membership_counts"]),
                "listed_api_node_count": number_summary(item["listed_federated_node_counts"]),
                "fresh_host_set_examples": [list(hosts) for hosts in fresh_sets[:4]],
                "host_mem_available_bytes": number_summary(item["host_mem_available_bytes"]),
                "stage_membership_check": "pass" if stage_check is True else "fail" if stage_check is False else "not_recorded",
            }
        container_result = {}
        for (phase, name), values in sorted(containers.items()):
            container_result.setdefault(phase, {})[name] = {
                "memory_mib": number_summary(values["memory_bytes"], 1024**2),
                "cpu_percent": number_summary(values["cpu_percent"]),
            }
        return {
            "file": path.name,
            "present": path.exists(),
            "malformed_lines": malformed,
            "phase_summaries": result_phases,
            "container_resource_samples": container_result,
            "stages_from_summary": self.summary.get("stages", []),
            "target_service_peers": self.summary.get("target_service_peers"),
            "actual_service_peers": self.summary.get("actual_service_peers"),
            "interpretation": "Docker stats are sampled container measurements; listed API nodes and LastSeen-filtered fresh hosts are reported separately because cached/stale IDs can remain listed.",
        }

    def _freshness_output(self) -> list[dict[str, Any]]:
        rows = []
        for (bucket, status), count in sorted(self.freshness_counts.items()):
            rows.append({"bucket": bucket, "status": status, "count": count, "examples": self.freshness_examples[(bucket, status)]})
        return rows

    def _protocol_reference_summary(self) -> dict[str, Any]:
        source = self.reference_info.get("source", {})
        if not isinstance(source, dict):
            source = {}
        categories = []
        for lifecycle in source.get("lifecycle_categories", []) or []:
            if not isinstance(lifecycle, dict):
                continue
            records = []
            for record in lifecycle.get("records_or_messages", []) or []:
                if not isinstance(record, dict):
                    continue
                summarized = {key: record.get(key) for key in ("name", "bucket", "kind", "nominal_schedule", "stream_protocol_id", "notes", "source") if record.get(key) is not None}
                bucket = record.get("bucket")
                runtime_evidence = "not directly visible in ledger capture"
                if isinstance(bucket, str) and bucket:
                    candidates = [candidate.strip() for candidate in bucket.split(",") if candidate.strip()]
                    matches = []
                    for observed in self.observed_bucket_names:
                        for candidate in candidates:
                            if matches_reference_bucket(candidate, observed):
                                matches.append(observed)
                    runtime_evidence = "bucket present in observed snapshots: " + ", ".join(sorted(set(matches))) if matches else "no matching bucket observed"
                elif record.get("kind") == "absent":
                    runtime_evidence = "source says no explicit record exists"
                elif "mDNS" in str(record.get("name", "")):
                    runtime_evidence = "mDNS packets observed" if self.mdns_query_response_counts["query"] + self.mdns_query_response_counts["response"] else "mDNS packets not observed"
                elif "ledger" in str(record.get("name", "")).casefold() and self.gossip_received:
                    runtime_evidence = f"{self.gossip_received} gossip envelope receptions observed"
                summarized["runtime_evidence"] = runtime_evidence
                records.append(summarized)
            categories.append({"category": lifecycle.get("category"), "records_or_messages": records})
        return {
            "available": self.reference_info.get("available", False),
            "file": self.reference_info.get("file"),
            "error": self.reference_info.get("error"),
            "source_pin": source.get("source_pin"),
            "evidence_status": source.get("evidence_status"),
            "transport_and_envelopes": source.get("transport_and_envelopes"),
            "source_lifecycle_categories": categories,
            "out_of_scope_for_model_free_run": source.get("out_of_scope_for_model_free_run", []),
            "source_links": source.get("source_links", {}),
        }

    def build_result(self) -> dict[str, Any]:
        self.process_observer()
        model_controls = self.process_model_controls()
        samples = self.process_samples()
        service_probe = self.process_service_probe()
        completion_audit = self.process_completion_audit()
        categories = []
        for category in LIFECYCLE:
            matching = [group for group in self.change_groups.values() if group["lifecycle_category"] == category["id"]]
            matching.sort(key=lambda group: (group["bucket"], group["action"]))
            phase_counts = self.category_phase_counts.get(category["id"], {})
            direct_events = sum(count["observer_events"] for count in phase_counts.values())
            categories.append({**category, "record_changes": matching, "event_count": direct_events, "direct_event_counts": dict(sorted(self.category_direct_event_counts.get(category["id"], {}).items())), "phase_counts": dict(sorted(phase_counts.items()))})
        all_changes = sorted(self.change_groups.values(), key=lambda group: (LIFECYCLE_BY_ID.get(group["lifecycle_category"], LIFECYCLE[-1])["order"], group["bucket"], group["action"]))
        return {
            "schema_version": 1,
            "run": {
                "directory": str(self.directory),
                "label": self.summary.get("label", self.summary.get("run_label")),
                "status": "failed" if completion_audit.get("process_exit_code", 0) not in (None, 0) else self.summary.get("status"),
                "summary_status_as_written": self.summary.get("status"),
                "runner_error": self.summary.get("error"),
                "completion_outcome": completion_audit.get("outcome"),
                "source_revision": self.summary.get("source_revision"),
                "observer_count": len(self.observer_ids),
                "started_at": self.summary.get("started_at", self.summary.get("started")),
                "ended_at": self.summary.get("ended_at", self.summary.get("finished")),
            },
            "protocol_reference": self._protocol_reference_summary(),
            "input_files": {name: (self.directory / name).exists() for name in ("summary.json", "phases.jsonl", "observer.jsonl", "message_protocol_reference.json", "model-controls.jsonl", "samples.jsonl", "peer-logs.jsonl", "service-probe.json", "pilot-completion-audit.json")},
            "capture_scope": {
                "observer_file": "observer.jsonl",
                "ledger_snapshot_semantics": "Each gossip_message contains a complete compressed EdgeVPN ledger block. Multiple buckets and records can coexist in one envelope.",
                "received_from_peer_id_semantics": "Immediate relay peer observed by the tap; it is not necessarily the peer that originally published the ledger state.",
                "diff_semantics": "Record additions/changes/removals are differences between unique snapshots observed by this tap, compared to the prior unique snapshot per observer. They are not counts of application operations or independently transmitted records.",
                "phase_semantics": "Phase labels are assigned from the latest phases.jsonl transition at the event timestamp. A phase label is separate from the semantic lifecycle category.",
            },
            "counts": {
                "observer_event_types": dict(sorted(self.event_counts.items())),
                "parsed_observer_event_count": self.parsed_observer_event_count,
                "gossip_envelope_receptions": self.gossip_received,
                "unique_ledger_block_hashes_per_observer": self.gossip_unique_blocks,
                "unique_block_hash_values_global": len(self.unique_block_hashes),
                "duplicate_block_snapshot_receptions": self.gossip_duplicates,
                "distinct_raw_body_sha256_values": len(self.unique_body_hashes),
                "body_sha256_mismatches": self.hash_mismatches,
                "body_gzip_json_decode_failures": self.body_decode_failures,
                "peer_connections": self.connection_counts.get("connect", 0),
                "peer_disconnections": self.connection_counts.get("disconnect", 0),
                "connections_already_open_at_observer_start": self.connection_counts.get("already_connected_at_registration", 0),
                "peer_listen_events": self.connection_counts.get("listen", 0),
                "mdns_queries": self.mdns_query_response_counts["query"],
                "mdns_responses": self.mdns_query_response_counts["response"],
                "mdns_ttl_zero_resource_records": self.mdns_ttl_zero_count,
                "events_without_timestamp": self.events_without_timestamp,
                "malformed_or_unknown_events": len(self.malformed_events),
                "block_index_min": self.min_block_index,
                "block_index_max": self.max_block_index if self.max_block_index >= 0 else None,
            },
            "phase_counts": dict(sorted(self.phase_counts.items())),
            "lifecycle_categories": categories,
            "record_change_catalogue": all_changes,
            "observed_bucket_absences": [self.bucket_absences[name] for name in sorted(self.bucket_absences)],
            "record_freshness": {
                "thresholds": {"node_last_seen_seconds": self.online_seconds, "healthcheck_max_age_seconds": self.health_seconds, "future_clock_skew_seconds": self.future_skew_seconds},
                "observations": self._freshness_output(),
                "interpretation": "A stale/future label compares a timestamp embedded in a captured record with the observer timestamp. Staleness alone does not establish that the application accepted, rejected, or routed traffic using that record.",
            },
            "model_control_observations": model_controls,
            "service_routing_probe": service_probe,
            "completion_audit": completion_audit,
            "membership_and_resources": samples,
            "capture_metadata": {"counts": dict(sorted(self.capture_metadata_counts.items())), "examples": self.capture_metadata_examples, "interpretation": "Observer startup/stop and packet-capture diagnostics describe the measurement setup; they are not LocalAI peer-to-peer control messages."},
            "mdns_ttl_zero_observations": {
                "count": self.mdns_ttl_zero_count,
                "examples": self.mdns_ttl_zero_examples,
                "interpretation": "DNS resource records with TTL 0 can signal DNS-level withdrawal/goodbye; they do not prove a LocalAI ledger leave or authoritative membership removal.",
            },
            "snapshot_and_event_examples": self.events,
            "snapshot_and_event_examples_cap_per_category": MAX_EVENT_EXAMPLES_PER_CATEGORY,
            "malformed_or_unknown_events": self.malformed_events,
            "analysis_errors": self.errors,
            "limitations": [
                "One observer's local network vantage; relay IDs do not identify the original ledger publisher.",
                "The lab disables DHT; no WAN behavior is measured. If mDNS capture is absent, discovery traffic may be outside this trace.",
                "The run uses model-free peers. The gateway /v1/models probe is not model inference and no sharded inference workload is measured.",
                "Observed removals are ledger state differences, not explicit leave messages. Peer disconnect, service loss, and eventual record expiry are reported as separate evidence.",
                "A record or bucket removal means it was absent from a later snapshot received by this observer. That block may be from a stale, forked, or otherwise nonaccepted branch; block index and parent-hash lineage do not prove canonical acceptance. This trace cannot establish authoritative deletion, explicit departure, or prove that a leader-driven heartbeat-bucket scrub did or did not occur.",
                "Repeated full snapshots can contain unchanged records. Counts of changed records are observed state differences, not record-level packet counts.",
            ],
        }

    def write_outputs(self) -> dict[str, Any]:
        result = self.build_result()
        out_path = self.directory / "analysis.json"
        out_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        (self.directory / "LIFECYCLE_REPORT.md").write_text(render_report(result), encoding="utf-8")
        return result


def render_report(result: dict[str, Any]) -> str:
    counts = result["counts"]
    lines = [
        "# LocalAI control-message lifecycle observations",
        "",
        f"Run: `{result['run'].get('label') or result['run'].get('directory')}`  ",
        f"Source revision: `{result['run'].get('source_revision') or 'not recorded'}`",
        f"Run status: **{result['run'].get('status') or 'not recorded'}**.",
        "",
        "## Capture totals",
        "",
        f"- Gossip envelope receptions: **{counts['gossip_envelope_receptions']}**.",
        f"- Unique block snapshots seen by observers: **{counts['unique_ledger_block_hashes_per_observer']}**; repeated snapshot receptions: **{counts['duplicate_block_snapshot_receptions']}**.",
        f"- Peer connection/disconnection events: **{counts['peer_connections']} / {counts['peer_disconnections']}**.",
        f"- Captured mDNS packets: **{counts['mdns_queries']} queries / {counts['mdns_responses']} responses**.",
        f"- Empty-model controls: **{'passed' if result['model_control_observations'].get('all_checks_empty') else 'not verified'}** across {len(result['model_control_observations'].get('empty_model_checks', []))} probes; gateway service probe: **{'passed' if result['service_routing_probe'].get('passed') else 'not completed or failed'}**.",
        "- One envelope carries a whole ledger block; the records listed below are state differences between observed snapshots, not separate packets.",
        "",
        "## Lifecycle catalogue",
        "",
        "| Order | Lifecycle category | Observed records/events | What the trace can show |",
        "|---:|---|---|---|",
    ]
    if result['run'].get('status') not in (None, 'passed'):
        lines[6:6] = ["", "**Partial capture:** the lifecycle experiment did not complete successfully. "
                      + str(result['run'].get('runner_error') or result['run'].get('completion_outcome') or '')]
    for category in result["lifecycle_categories"]:
        records = category["record_changes"]
        summaries = [f"`{row['bucket']}` {row['action']}: {row['count']}" for row in records]
        event_labels = {
            "gossip_receptions": "gossip receptions",
            "mdns_queries": "mDNS queries",
            "mdns_responses": "mDNS responses",
            "peer_connections": "peer connections",
            "peer_disconnections": "peer disconnections",
            "existing_connections": "connections already open at observer registration",
            "peer_listen_events": "peer listen events",
        }
        direct_events = [f"{event_labels.get(kind, kind.replace('_', ' '))}: {count}" for kind, count in category.get("direct_event_counts", {}).items()]
        observed = summaries + direct_events
        lines.append(f"| {category['order']} | **{category['id']}** | {'; '.join(observed) if observed else 'none recorded'} | {category['description']} |")
    source_categories = result["protocol_reference"].get("source_lifecycle_categories", [])
    if source_categories:
        lines.extend(["", "## Source-derived message catalogue", "", "These entries describe code paths and expected message types; their runtime status is shown separately.", "", "| Source lifecycle stage | Message / record | Bucket | Runtime evidence |", "|---|---|---|---|"])
        for lifecycle in source_categories:
            for record in lifecycle.get("records_or_messages", []):
                name = str(record.get("name", "unnamed"))
                bucket = str(record.get("bucket", "—")).replace("|", "\\|")
                evidence = str(record.get("runtime_evidence", "not directly visible in ledger capture")).replace("|", "\\|")
                lines.append(f"| {lifecycle.get('category', 'unknown')} | {name} | {bucket} | {evidence} |")
    lines.extend(["", "## Record examples", ""])
    if not result["record_change_catalogue"]:
        lines.append("No ledger state differences were observed in the captured trace.")
    else:
        lines.extend(["| Category | Bucket / action | Meaning and snapshot lineage | Count | Example raw pointer |", "|---|---|---|---:|---|"])
        for group in result["record_change_catalogue"]:
            example = group["examples"][0] if group["examples"] else {}
            pointer = f"`observer.jsonl:{example.get('line', '?')}`"
            if example.get("message_sha256"):
                pointer += f" (`{example['message_sha256'][:12]}…` )"
            if example.get("block_index") is not None:
                pointer += f" [block index {example['block_index']}; {example.get('chain_relation', 'lineage unknown')}]"
            meaning = group.get("bucket_metadata", {}).get("meaning", group.get("bucket_metadata", {}).get("description", "Observed bucket state"))
            meaning = str(meaning).replace("|", "\\|").replace("\n", " ")
            lineage = ", ".join(f"{key}: {value}" for key, value in sorted(group.get("snapshot_lineage_counts", {}).items()))
            lines.append(f"| {group['lifecycle_category']} | `{group['bucket']}` / {group['action']} | {meaning}; lineage counts: {lineage or 'not recorded'} | {group['count']} | {pointer} |")
    transitions = result.get("observed_bucket_absences", [])
    lines.extend(["", "## Bucket-level state transitions", ""])
    if transitions:
        lines.extend(["| Bucket | Observation | Count | Records removed from observed state | Snapshot lineage | Example raw pointer |", "|---|---|---:|---:|---|---|"])
        for item in transitions:
            example = item["examples"][0] if item["examples"] else {}
            lineage = ", ".join(f"{key}: {value}" for key, value in sorted(item.get("snapshot_lineage_counts", {}).items())) or "not recorded"
            lines.append(f"| `{item['bucket']}` | absent from a later snapshot | {item['count']} | {item['removed_record_count']} | {lineage} | `observer.jsonl:{example.get('line', '?')}` |")
        lines.append("A whole-bucket disappearance is inferred from consecutive received snapshots; it is not a distinct DELETE packet or proof of an explicit peer leave.")
    else:
        lines.append("No complete bucket disappearance was visible between the unique snapshots received by this observer.")
    ttl_zero = result.get("mdns_ttl_zero_observations", {})
    lines.extend(["", "## mDNS TTL-zero resource records", ""])
    if ttl_zero.get("count", 0):
        lines.append(f"Observed **{ttl_zero['count']}** DNS resource records with TTL 0. These are DNS-level withdrawal/goodbye signals, not evidence of a LocalAI ledger leave.")
        for example in ttl_zero.get("examples", []):
            rr = example.get("resource_record", {})
            rr_text = ", ".join(f"{key}={value}" for key, value in rr.items())
            lines.append(f"- `observer.jsonl:{example.get('line', '?')}`; phase `{example.get('phase', 'unknown')}`; {example.get('section', 'unknown')} record ({rr_text}).")
    else:
        lines.append("No TTL-zero DNS resource records were observed; the capture cannot infer a LocalAI leave from this absence.")
    freshness = result["record_freshness"]["observations"]
    lines.extend(["", "## Freshness checks", ""])
    if freshness:
        lines.append("| Bucket | Status | Count | Example |")
        lines.append("|---|---|---:|---|")
        for row in freshness:
            example = row["examples"][0] if row["examples"] else {}
            lines.append(f"| `{row['bucket']}` | {row['status']} | {row['count']} | `observer.jsonl:{example.get('line', '?')}` |")
    else:
        lines.append("No parseable heartbeat or LastSeen timestamps were present in observed records.")
    samples = result["membership_and_resources"]
    lines.extend(["", "## Membership and resource samples", ""])
    if samples["phase_summaries"]:
        lines.append(f"Target service peers: **{samples.get('target_service_peers') or 'not recorded'}**; actual peers: **{samples.get('actual_service_peers') or 'not recorded'}**.")
        lines.extend(["", "| Phase | Samples | Fresh nodes per witness (median; range) | Listed API nodes (median; range) | Final stage check | Lowest host MemAvailable |", "|---|---:|---|---|---|---:|"])
        for phase, item in samples["phase_summaries"].items():
            fresh = item["fresh_membership_count"]
            listed = item["listed_api_node_count"]
            mem = item["host_mem_available_bytes"]
            fresh_text = f"{fresh['median']:.1f} ({fresh['min']:.0f}–{fresh['max']:.0f})" if fresh else "not sampled"
            listed_text = f"{listed['median']:.1f} ({listed['min']:.0f}–{listed['max']:.0f})" if listed else "not sampled"
            mem_text = f"{mem['min'] / 1024**3:.2f} GiB" if mem else "not sampled"
            lines.append(f"| {phase} | {item['sample_count']} | {fresh_text} | {listed_text} | {item['stage_membership_check']} | {mem_text} |")
        container_rows = []
        for phase, by_name in samples["container_resource_samples"].items():
            for name, metrics in by_name.items():
                memory = metrics.get("memory_mib")
                if memory:
                    container_rows.append((memory["max"], phase, name, memory["median"], metrics.get("cpu_percent")))
        if container_rows:
            lines.extend(["", "Highest sampled container memory peaks:"])
            for peak, phase, name, median, cpu in sorted(container_rows, reverse=True)[:5]:
                cpu_text = f"; median CPU {cpu['median']:.2f}%" if cpu else ""
                lines.append(f"- `{phase}` / `{name}`: median {median:.1f} MiB, sampled maximum {peak:.1f} MiB{cpu_text}.")
    else:
        lines.append("No samples.jsonl observations were available.")
    lines.extend(["", "## Interpretation limits", ""])
    audit = result.get("completion_audit", {})
    if audit.get("present") and audit.get("process_exit_code") not in (None, 0):
        lines.append(f"- **Partial/failed run:** process exit code {audit['process_exit_code']} (`{audit.get('outcome') or 'outcome not recorded'}`); lifecycle observations below are from the captured prefix, not a completed experiment.")
    for limitation in result["limitations"]:
        lines.append(f"- {limitation}")
    if result["analysis_errors"]:
        lines.extend(["", "## Trace warnings", ""])
        lines.extend(f"- {warning}" for warning in result["analysis_errors"][:20])
    lines.append("")
    return "\n".join(lines)


def analyze(directory: Path) -> dict[str, Any]:
    return Analyzer(directory).write_outputs()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path, help="experiment result directory containing observer.jsonl")
    args = parser.parse_args()
    result = analyze(args.directory)
    print(json.dumps({"analysis": str(args.directory / 'analysis.json'), "report": str(args.directory / 'LIFECYCLE_REPORT.md'), "counts": result["counts"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
