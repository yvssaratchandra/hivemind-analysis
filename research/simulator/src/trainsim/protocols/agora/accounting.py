"""Replayable accounting for the Agora simulation trace.

Agora accounting uses a different unit from the Hivemind simulator: one
accepted sparse snapshot contribution is one unique
``(origin, epoch, layout, partition)`` fully incorporated by one remote
recipient.  This module deliberately consumes raw protocol events only.  A
``work_accepted`` or other derived row in a trace has no accounting effect.
"""

from __future__ import annotations

import gzip
import json
import math
from collections import Counter, defaultdict
from pathlib import Path
from statistics import median
from typing import Any, Mapping


def _event_data(event: Mapping[str, Any]) -> dict[str, Any]:
    """Accept both Engine-style flattened events and persisted JSONL rows."""
    raw = event.get("data", {})
    data = dict(raw) if isinstance(raw, Mapping) else {}
    for key, value in event.items():
        if key not in {"data", "time", "kind", "actor", "sequence", "parent"}:
            data.setdefault(key, value)
    return data


def _first(data: Mapping[str, Any], *names: str, default: Any = None) -> Any:
    for name in names:
        if name in data and data[name] is not None:
            return data[name]
    return default


def _text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    if isinstance(value, bytes):
        return value.hex()
    return str(value)


def _number(value: Any, default: float | None = None) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def _identity(value: Any) -> str:
    """Turn peer IDs, including Observer's byte wrapper, into stable text."""
    if isinstance(value, Mapping) and "__bytes_hex__" in value:
        return str(value["__bytes_hex__"])
    return _text(value)


def _component_map(value: Any) -> dict[str, float] | None:
    """Normalize component cardinalities while retaining one entry per ID."""
    if not isinstance(value, Mapping):
        return None
    result: dict[str, float] = {}
    for component, count in value.items():
        numeric = _number(count)
        if numeric is None or numeric < 0:
            return None
        key = _text(component)
        if key in result:
            return None
        result[key] = numeric
    return result or None


def _id_list(value: Any) -> tuple[list[str] | None, bool]:
    if not isinstance(value, (list, tuple, set)):
        return None, False
    ids = [_text(item) for item in value]
    valid = bool(ids) and all(ids) and len(ids) == len(set(ids))
    return (sorted(ids) if valid else None), valid


def _roster(data: Mapping[str, Any]) -> list[str]:
    peers = _first(data, "ordered_node_ids", "ordered_peer_ids", "roster", "peers",
                   "members", "observed_peers", "node_ids", "peer_ids", default=[])
    if isinstance(peers, Mapping):
        peers = list(peers)
    if not isinstance(peers, (list, tuple, set)):
        return []
    result = []
    for peer in peers:
        if isinstance(peer, Mapping):
            peer = _first(peer, "node_id", "node", "id", "peer_id", default=peer)
        result.append(_identity(peer))
    return result


def _normal_role(value: Any) -> str:
    role = _text(value).upper()
    if role in {"NODE", "REDUCER", "REDUCE"}:
        return "reducer"
    if role in {"CLIENT", "CONTRIBUTOR", "CONTRIBUTE"}:
        return "contributor"
    return role.lower()


class Accounting:
    """Incremental Agora-specific trace accounting with independent replay.

    ``consume`` accepts a persisted row of the form
    ``{time, kind, actor, data}``, or the flattened equivalent.  For credit,
    the preferred raw event is ``agora_sparse_application`` with ``origin``,
    ``recipient``, ``epoch``, ``layout``, ``partition``,
    ``selected_cardinality``, ``applied_cardinality``, ``status`` and
    ``fallback``.  Optional tensor/component receipts can be joined at one
    recipient when they carry a common snapshot cardinality.
    """

    def __init__(self, config: Mapping[str, Any] | None = None):
        self.config = dict(config or {})
        self.measurement_start: float | None = None
        self.last_time = 0.0
        self.observation_boundary: float | None = None
        self.target_time: float | None = None
        target = _number(_first(self.config, "target_accepted_contributions", "target_work"))
        if target is not None and not target.is_integer():
            raise ValueError("target_accepted_contributions must be an integer count")
        self._target = int(target) if target is not None else None
        self._observation_seconds = _number(self.config.get("observation_seconds"))
        self._events = Counter()
        self._roles: dict[str, str] = {}
        self._node_stats: dict[str, dict[str, Any]] = defaultdict(self._new_node_stats)
        self._advertisements: dict[tuple[str, str], float] = {}
        self._snapshots: dict[tuple[str, str, str, str], dict[str, Any]] = {}
        self._rosters: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self._expected: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self._lookups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self._applications: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
        self._credited: dict[tuple[str, str, str, str], dict[str, Any]] = {}
        self._complete_applications: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
        self._statuses = Counter()
        self._averaging_outcomes = Counter()
        self._protocol_failures = Counter()
        self._timeouts = Counter()
        self._timeline: dict[str, list[dict[str, Any]]] = defaultdict(list)
        population = self.config.get("population")
        if isinstance(population, int) and population >= 0:
            for index in range(population):
                node = f"N{index:04d}"
                self._node_stats[node]

    @staticmethod
    def _new_node_stats() -> dict[str, Any]:
        return {
            "role": "unknown",
            "advertisements": 0,
            "rosters_observed": 0,
            "snapshots_offered": 0,
            "accepted_sparse_snapshot_contributions": 0,
            "partial": 0,
            "failed": 0,
            "averaging_partial": 0,
            "averaging_failed": 0,
            "protocol_failures": 0,
            "timeouts": 0,
            "skipped": 0,
            "reducer_weight_sum": 0.0,
            "reducer_weight_observations": 0,
        }

    @staticmethod
    def _epoch(data: Mapping[str, Any]) -> str:
        return _text(_first(data, "epoch", "averaging_epoch", "global_step", "step", default="?"))

    def consume(self, event: Mapping[str, Any]) -> None:
        """Consume one raw event; derived credit records are intentionally ignored."""
        if not isinstance(event, Mapping):
            raise TypeError("Agora accounting events must be mappings")
        time_value = _number(event.get("time"), 0.0) or 0.0
        kind = _text(event.get("kind"))
        actor = _identity(event.get("actor", ""))
        data = _event_data(event)
        self.last_time = max(self.last_time, time_value)
        self._events[kind] += 1

        if kind == "measurement_started":
            self.measurement_start = time_value
            self._refresh_target_time()
            return
        if kind == "observation_boundary":
            self.observation_boundary = time_value
            return
        if kind in {"work_accepted", "accepted_sparse_snapshot_contribution", "agora_credit"}:
            return

        role_value = _first(data, "role", "mode", "node_role")
        if role_value is not None:
            node = _identity(_first(data, "node", "node_id", "actor_node", default=actor))
            role = _normal_role(role_value)
            self._roles[node] = role
            self._node_stats[node]["role"] = role
            if role == "reducer" and kind.lower() == "agora_role":
                weight = _number(_first(data, "weight", "averaging_weight", "reduction_weight"))
                if weight is not None:
                    self._node_stats[node]["reducer_weight_sum"] += weight
                    self._node_stats[node]["reducer_weight_observations"] += 1

        if self._is_advertisement(kind, data):
            self._consume_advertisement(time_value, actor, data)
        if self._is_roster(kind, data):
            self._consume_roster(time_value, actor, data)
        if self._is_expected(kind, data):
            self._consume_expected(time_value, actor, data)
        if self._is_lookup(kind, data):
            self._consume_lookup(time_value, actor, data)
        if self._is_snapshot_offer(kind):
            self._consume_snapshot_offer(time_value, actor, data)
        if self._is_application(kind, data):
            self._consume_application(time_value, actor, kind, data)

        low = kind.lower()
        if "skip" in low:
            node = _identity(_first(data, "node", "node_id", "origin", default=actor))
            self._node_stats[node]["skipped"] += 1
            self._statuses["skipped"] += 1
        elif low.endswith("aggregate_finished"):
            outcome = _text(_first(data, "status", "outcome", default="unknown")).lower() or "unknown"
            self._averaging_outcomes[outcome] += 1
            node = _identity(_first(data, "node", "node_id", default=actor))
            if outcome in {"partial", "incomplete"}:
                self._node_stats[node]["averaging_partial"] += 1
            elif outcome in {"failed", "error", "timeout", "rejected"}:
                self._node_stats[node]["averaging_failed"] += 1
        elif "timeout" in low:
            node = _identity(_first(data, "node", "node_id", "origin", default=actor))
            self._node_stats[node]["timeouts"] += 1
            self._timeouts["events"] += 1
        elif "fail" in low:
            node = _identity(_first(data, "node", "node_id", "origin", default=actor))
            self._node_stats[node]["protocol_failures"] += 1
            self._protocol_failures["events"] += 1

    @staticmethod
    def _has_epoch(data: Mapping[str, Any]) -> bool:
        return _first(data, "epoch", "averaging_epoch", "global_step") is not None

    @staticmethod
    def _is_advertisement(kind: str, data: Mapping[str, Any]) -> bool:
        low = kind.lower()
        return Accounting._has_epoch(data) and low == "agora_step_advertised"

    @staticmethod
    def _is_roster(kind: str, data: Mapping[str, Any]) -> bool:
        low = kind.lower()
        return Accounting._has_epoch(data) and low == "agora_local_roster"

    @staticmethod
    def _is_expected(kind: str, data: Mapping[str, Any]) -> bool:
        low = kind.lower()
        return Accounting._has_epoch(data) and low == "agora_expected_count"

    @staticmethod
    def _is_lookup(kind: str, data: Mapping[str, Any]) -> bool:
        low = kind.lower()
        return Accounting._has_epoch(data) and low == "agora_lookup"

    @staticmethod
    def _is_snapshot_offer(kind: str) -> bool:
        # This is the sole authoritative declaration. In particular,
        # aggregate_started is not allowed to replace a source snapshot.
        return kind.lower() == "agora_snapshot_created"

    @staticmethod
    def _is_application(kind: str, data: Mapping[str, Any]) -> bool:
        low = kind.lower()
        if low in {"agora_sparse_application", "agora_partition_applied", "sparse_application", "partition_application"}:
            return True
        return ("sparse" in low or "partition" in low) and any(
            token in low for token in ("applied", "application", "receipt", "incorporat")
        ) and _first(data, "origin", "contributor", "source_node") is not None

    def _consume_advertisement(self, time_value: float, actor: str, data: Mapping[str, Any]) -> None:
        node = _identity(_first(data, "node", "node_id", "actor_node", default=actor))
        epoch = self._epoch(data)
        timestamp = _number(_first(data, "published_at", "advertised_at", "time"))
        if timestamp is None:
            timestamp = time_value
        self._advertisements.setdefault((node, epoch), timestamp)
        self._node_stats[node]["advertisements"] += 1
        self._timeline[epoch].append({"time": time_value, "type": "advertisement", "node": node,
                                      "key": _text(_first(data, "key", "step_key", default=""))})

    def _consume_snapshot_offer(self, time_value: float, actor: str, data: Mapping[str, Any]) -> None:
        origin = _identity(_first(data, "origin", "node", "node_id", "contributor", default=actor))
        epoch = self._epoch(data)
        layout = _text(_first(data, "layout", "tensor_layout", "layout_id", "tensor_layout_id", default="?"))
        partition = _text(_first(data, "partition", "partition_id", "sparse_partition", default="?"))
        selected = _number(_first(data, "selected_cardinality", "selected_snapshot_cardinality",
                                  "selected_elements", "selected_bytes"))
        expected_counts = _component_map(_first(data, "expected_component_cardinalities", "selected_component_cardinalities"))
        expected_ids_raw = _first(data, "expected_component_ids", "expected_part_ids", "selected_part_ids")
        expected_ids, expected_ids_valid = _id_list(expected_ids_raw)
        if expected_ids_raw is None and expected_counts is not None:
            expected_ids = sorted(expected_counts)
            expected_ids_valid = True
        if expected_counts is not None and expected_ids is not None \
                and set(expected_counts) != set(expected_ids):
            expected_ids_valid = False
        if expected_counts is not None and selected is not None \
                and not math.isclose(sum(expected_counts.values()), selected, rel_tol=0, abs_tol=1e-9):
            expected_ids_valid = False
        if epoch != "?" and layout != "?" and partition != "?":
            snapshot_key = (origin, epoch, layout, partition)
            if snapshot_key not in self._snapshots:
                self._node_stats[origin]["snapshots_offered"] += 1
                self._snapshots[snapshot_key] = {"origin": origin, "epoch": epoch, "layout": layout,
                                                 "partition": partition, "selected_cardinality": selected,
                                                 "expected_component_ids": expected_ids,
                                                 "expected_component_ids_valid": expected_ids_valid,
                                                 "expected_component_cardinalities": expected_counts,
                                                 "time": time_value}
            self._timeline[epoch].append({"time": time_value, "type": "snapshot", "origin": origin,
                                          "layout": layout, "partition": partition,
                                          "selected_cardinality": selected,
                                          "expected_component_ids": expected_ids,
                                          "expected_component_ids_valid": expected_ids_valid})

    def _consume_expected(self, time_value: float, actor: str, data: Mapping[str, Any]) -> None:
        epoch = self._epoch(data)
        expected = _number(_first(data, "expected_count", "expected_peers", "count", "population"))
        if expected is None:
            return
        self._expected[epoch].append({"time": time_value, "actor": actor, "expected": int(expected)})
        self._timeline[epoch].append({"time": time_value, "type": "expected_count", "actor": actor,
                                      "expected": int(expected)})

    def _consume_lookup(self, time_value: float, actor: str, data: Mapping[str, Any]) -> None:
        epoch = self._epoch(data)
        peers = _roster(data)
        row = {"time": time_value, "actor": actor, "observed": len(peers), "peers": peers,
               "key": _text(_first(data, "key", "step_key", default=""))}
        self._lookups[epoch].append(row)
        self._timeline[epoch].append({"time": time_value, "type": "lookup", **row})

    def _consume_roster(self, time_value: float, actor: str, data: Mapping[str, Any]) -> None:
        epoch = self._epoch(data)
        node = _identity(_first(data, "node", "node_id", "observer", "peer", default=actor))
        peers = _roster(data)
        if not peers and data.get("roster_fingerprint"):
            # Keep the fingerprint for comparisons; empty peer lists remain
            # honest about what was observable in the trace.
            peers = []
        expected = _number(_first(data, "expected", "expected_count", "expected_peers"))
        observed = _number(_first(data, "observed", "observed_count", "peer_count"), float(len(peers)))
        form_time = _number(_first(data, "formation_time", "formed_at", "roster_time"))
        if form_time is None:
            form_time = time_value
        ad_time = _number(_first(data, "advertised_at", "ad_time", "publish_time"))
        if ad_time is None:
            ad_time = self._advertisements.get((node, epoch))
        wait = _number(_first(data, "ad_to_roster_wait", "roster_wait_seconds", "wait_seconds"))
        if wait is None and ad_time is not None:
            wait = max(0.0, form_time - ad_time)
        fingerprint = _text(_first(data, "roster_fingerprint", "fingerprint", default=""))
        if not fingerprint:
            fingerprint = ",".join(peers)
        row = {"time": form_time, "actor": actor, "node": node, "epoch": epoch,
               "peers": peers, "fingerprint": fingerprint, "expected": int(expected) if expected is not None else None,
               "observed": int(observed) if observed is not None else len(peers), "wait_seconds": wait,
               "reason": _text(_first(data, "reason", "completion_reason", default=""))}
        self._rosters[epoch].append(row)
        self._node_stats[node]["rosters_observed"] += 1
        if wait is not None:
            self._timeline[epoch].append({"time": form_time, "type": "roster", **row})
        else:
            self._timeline[epoch].append({"time": form_time, "type": "roster", **row})
        # Roster evidence can also provide an advertisement/expected snapshot
        # when the source emits a compact combined event.
        if row["expected"] is not None:
            self._expected[epoch].append({"time": form_time, "actor": node, "expected": row["expected"]})

    def _consume_application(self, time_value: float, actor: str, kind: str, data: Mapping[str, Any]) -> None:
        origin = _identity(_first(data, "origin", "contributor", "source_node", "sender", default=""))
        recipient = _identity(_first(data, "recipient", "receiver", "applier", default=actor))
        epoch = self._epoch(data)
        layout = _text(_first(data, "layout", "tensor_layout", "layout_id", "tensor_layout_id", default="?"))
        partition = _text(_first(data, "partition", "partition_id", "sparse_partition", default="?"))
        if not origin or not recipient or epoch == "?" or layout == "?" or partition == "?":
            return

        unit = (origin, epoch, layout, partition)
        selected = _number(_first(data, "selected_cardinality", "expected_cardinality",
                                  "selected_elements", "expected_elements", "selected_bytes",
                                  "expected_bytes"))
        applied = _number(_first(data, "applied_cardinality", "incorporated_cardinality", "applied_elements",
                                 "incorporated_elements", "applied_bytes", "incorporated_bytes"))
        status = _text(_first(data, "status", "outcome", "application_status", default="")).lower()
        fallback = bool(_first(data, "fallback", "used_fallback", default=False))
        key = (*unit, recipient)
        state = self._applications.setdefault(key, {"origin": origin, "recipient": recipient, "epoch": epoch,
                                                     "layout": layout, "partition": partition,
                                                     "time": time_value, "status": status,
                                                     "fallback": fallback, "selected": None, "applied": 0.0,
                                                     "components": {}, "expected_components": None,
                                                     "snapshot_selected": None, "expected_component_ids": None,
                                                     "expected_component_ids_valid": False,
                                                     "expected_component_cardinalities": None,
                                                     "applied_component_ids": None,
                                                     "applied_component_ids_valid": False,
                                                     "applied_component_cardinalities": None,
                                                     "component_conflicts": set(), "events": 0})
        state["events"] += 1
        state["time"] = max(state["time"], time_value)
        state["status"] = status or state["status"]
        state["fallback"] = state["fallback"] or fallback
        state["expected_components"] = _first(data, "expected_components", "component_count", "tensor_count",
                                               default=state["expected_components"])
        explicit_snapshot = _number(_first(data, "snapshot_selected_cardinality", "selected_snapshot_cardinality",
                                           "full_selected_cardinality", "selected_total_cardinality"))
        if explicit_snapshot is not None:
            state["snapshot_selected"] = explicit_snapshot

        component = _first(data, "component_id", "tensor_index", "tensor_id", "part_index")
        component_mode = component is not None
        if component_mode:
            component_key = _text(component)
            component_row = {"selected": selected, "applied": applied, "status": status,
                             "fallback": fallback}
            prior = state["components"].get(component_key)
            if prior is not None and prior != component_row:
                state["component_conflicts"].add(component_key)
            elif prior is None:
                state["components"][component_key] = component_row
        else:
            # One raw terminal receipt describes a whole sparse partition.
            # Keep the strongest latest terminal outcome without combining
            # multiple recipients, attempts, or epochs.
            if selected is not None:
                state["selected"] = selected
            if applied is not None:
                state["applied"] = applied

        source_snapshot = self._snapshots.get(unit)
        if source_snapshot:
            state["expected_component_ids"] = source_snapshot.get("expected_component_ids")
            state["expected_component_ids_valid"] = source_snapshot.get("expected_component_ids_valid", False)
            state["expected_component_cardinalities"] = source_snapshot.get("expected_component_cardinalities")
        applied_counts = _component_map(_first(data, "applied_component_cardinalities", "received_component_cardinalities"))
        applied_ids_raw = _first(data, "applied_component_ids", "received_component_ids", "applied_part_ids")
        applied_ids, applied_ids_valid = _id_list(applied_ids_raw)
        if applied_ids_raw is None and applied_counts is not None:
            applied_ids = sorted(applied_counts)
            applied_ids_valid = True
        if applied_ids is not None or applied_ids_raw is not None:
            state["applied_component_ids"] = applied_ids
            state["applied_component_ids_valid"] = applied_ids_valid
        if applied_counts is not None:
            state["applied_component_cardinalities"] = applied_counts
        elif component_mode and state["components"]:
            component_applied = {
                component_id: float(row["applied"])
                for component_id, row in state["components"].items()
                if row.get("applied") is not None
            }
            if len(component_applied) == len(state["components"]):
                state["applied_component_cardinalities"] = component_applied
        snapshot_matches = bool(source_snapshot and source_snapshot["layout"] == layout
                                and source_snapshot["partition"] == partition
                                and source_snapshot.get("selected_cardinality") is not None
                                and source_snapshot.get("expected_component_ids")
                                and source_snapshot.get("expected_component_ids_valid")
                                and source_snapshot.get("expected_component_cardinalities") is not None)
        if snapshot_matches:
            receipt_selected = self._selected_total(state) if component_mode else (
                explicit_snapshot if explicit_snapshot is not None else selected
            )
            snapshot_matches = receipt_selected is not None \
                and math.isclose(float(receipt_selected), float(source_snapshot["selected_cardinality"]),
                                 rel_tol=0, abs_tol=1e-9)
            expected_ids = set(source_snapshot["expected_component_ids"])
            if component_mode:
                snapshot_matches = snapshot_matches and set(state["components"]) == expected_ids \
                    and not state["component_conflicts"]
            else:
                snapshot_matches = snapshot_matches and state["applied_component_ids_valid"] \
                    and state["applied_component_ids"] is not None \
                    and set(state["applied_component_ids"]) == expected_ids
            expected_counts = source_snapshot.get("expected_component_cardinalities")
            applied_counts = state.get("applied_component_cardinalities")
            snapshot_matches = snapshot_matches and applied_counts is not None \
                and applied_counts == expected_counts \
                and set(applied_counts) == expected_ids
        state["snapshot_matches"] = snapshot_matches
        self._timeline[epoch].append({"time": time_value, "type": "application", "origin": origin,
                                      "recipient": recipient, "layout": layout, "partition": partition,
                                      "status": status, "fallback": fallback,
                                      "selected_cardinality": selected, "applied_cardinality": applied,
                                      "snapshot_matches": snapshot_matches})
        complete = self._application_complete(state, data, component_mode)
        if complete:
            self._complete_applications[key] = {"origin": origin, "recipient": recipient, "epoch": epoch,
                                                "layout": layout, "partition": partition, "time": time_value}
            credit = self._credited.get(unit)
            if credit is None:
                credit_row = {"origin": origin, "recipient": recipient, "epoch": epoch,
                              "layout": layout, "partition": partition, "time": time_value,
                              "selected_cardinality": self._selected_total(state),
                              "applied_cardinality": self._applied_total(state), "status": "complete"}
                self._credited[unit] = credit_row
                self._node_stats[origin]["accepted_sparse_snapshot_contributions"] += 1
                self._statuses["accepted"] += 1
                self._timeline[epoch].append({"time": time_value, "type": "incorporation", **credit_row})
                self._refresh_target_time()
        elif status in {"partial", "incomplete"} or (applied is not None and selected is not None and applied < selected):
            self._statuses["partial"] += 1
            self._node_stats[origin]["partial"] += 1
        elif status in {"failed", "error", "timeout", "rejected"}:
            self._statuses["failed"] += 1
            self._node_stats[origin]["failed"] += 1

    def _refresh_target_time(self) -> None:
        if self._target is None or self.measurement_start is None:
            return
        start = self.measurement_start
        measured = sorted(row["time"] for row in self._credited.values() if row["time"] >= start)
        if self._target <= 0:
            self.target_time = 0.0
        elif len(measured) >= self._target:
            self.target_time = max(0.0, measured[int(self._target) - 1] - start)
        else:
            self.target_time = None

    def _selected_total(self, state: Mapping[str, Any]) -> float:
        if state.get("snapshot_selected") is not None:
            return float(state["snapshot_selected"])
        if state.get("components"):
            return sum(float(item.get("selected") or 0.0) for item in state["components"].values())
        return float(state.get("selected") or 0.0)

    def _applied_total(self, state: Mapping[str, Any]) -> float:
        if state.get("components"):
            return sum(float(item.get("applied") or 0.0) for item in state["components"].values())
        return float(state.get("applied") or 0.0)

    def _application_complete(self, state: Mapping[str, Any], event_data: Mapping[str, Any], component_mode: bool) -> bool:
        if state["origin"] == state["recipient"] or state["fallback"] or not state.get("snapshot_matches", False):
            return False
        status = state.get("status", "")
        if status not in {"complete", "applied", "incorporated", "success", "ok"}:
            return False
        if component_mode:
            components = list(state["components"].values())
            if not components or any(item["fallback"] or item["status"] not in
                                     {"complete", "applied", "incorporated", "success", "ok"}
                                     for item in components):
                return False
            expected_ids = state.get("expected_component_ids")
            if expected_ids is None or not state.get("expected_component_ids_valid") \
                    or set(state["components"]) != set(expected_ids) or state["component_conflicts"]:
                # A subset of components must never be mistaken for the full
                # sparse partition. The selected part IDs come from the
                # origin's snapshot declaration, not from the receipt.
                return False
            selected = self._selected_total(state)
            snapshot_selected = state.get("snapshot_selected")
            applied = self._applied_total(state)
            expected = float(snapshot_selected) if snapshot_selected is not None else selected
            return expected > 0 and math.isclose(applied, expected, rel_tol=0, abs_tol=1e-9)
        expected_ids = state.get("expected_component_ids")
        if expected_ids is None or not state.get("expected_component_ids_valid") \
                or not state.get("applied_component_ids_valid") or state.get("applied_component_ids") is None \
                or set(state["applied_component_ids"]) != set(expected_ids):
            return False
        selected = state.get("selected")
        applied = state.get("applied")
        # For one terminal receipt, cardinalities must explicitly establish
        # full application. Status alone never proves complete coverage.
        return selected is not None and applied is not None and selected > 0 \
            and math.isclose(float(applied), float(selected), rel_tol=0, abs_tol=1e-9)

    def summary(self) -> dict[str, Any]:
        """Return scalar results and inspectable node/epoch tables."""
        origin = self.measurement_start or 0.0
        boundary = self.observation_boundary
        if boundary is None and self._observation_seconds is not None and self.measurement_start is not None:
            boundary = self.measurement_start + self._observation_seconds
        accepted_rows = sorted(self._credited.values(), key=lambda row: (row["time"], row["origin"], row["epoch"], row["partition"]))
        accepted_at_boundary = None
        if boundary is not None:
            accepted_at_boundary = sum(origin <= row["time"] < boundary for row in accepted_rows)
        nodes = []
        for node in sorted(set(self._node_stats) | set(self._roles)):
            data = dict(self._node_stats[node])
            data["node"] = node
            if data["reducer_weight_observations"]:
                data["mean_reducer_weight"] = data["reducer_weight_sum"] / data["reducer_weight_observations"]
            else:
                data["mean_reducer_weight"] = None
            nodes.append(data)

        epochs: dict[str, Any] = {}
        all_waits: list[float] = []
        for epoch in sorted(set(self._rosters) | set(self._expected) | set(self._lookups) | set(self._timeline),
                            key=_epoch_sort):
            roster_rows = self._rosters.get(epoch, [])
            variants: dict[str, dict[str, Any]] = {}
            for row in roster_rows:
                fingerprint = row["fingerprint"]
                variant = variants.setdefault(fingerprint, {"fingerprint": fingerprint, "roster": row["peers"],
                                                              "observers": []})
                variant["observers"].append(row["node"])
                if row["wait_seconds"] is not None:
                    all_waits.append(float(row["wait_seconds"]))
            expected_rows = self._expected.get(epoch, [])
            lookup_rows = self._lookups.get(epoch, [])
            obs = [row["observed"] for row in lookup_rows]
            if not obs:
                obs = [row["observed"] for row in roster_rows]
            epochs[epoch] = {
                "advertisements": sum(1 for (node, ep) in self._advertisements if ep == epoch),
                "expected_observations": expected_rows,
                "expected_min": min((row["expected"] for row in expected_rows), default=None),
                "expected_max": max((row["expected"] for row in expected_rows), default=None),
                "observed_min": min(obs, default=None),
                "observed_max": max(obs, default=None),
                "roster_observations": roster_rows,
                "roster_variants": list(variants.values()),
                "divergent_roster_count": len(variants),
                "lookup_count": len(lookup_rows),
                "incorporations": [row for row in accepted_rows if row["epoch"] == epoch],
                "timeline": sorted(self._timeline.get(epoch, []), key=lambda row: row["time"]),
            }

        measured_rows = [row for row in accepted_rows if row["time"] >= origin]
        start_credit = next((max(0.0, row["time"] - origin) for row in measured_rows), None)
        target_relative = self.target_time
        if target_relative is None and self._target is not None and self.measurement_start is not None:
            if self._target <= 0:
                target_relative = 0.0
            elif len(measured_rows) >= self._target:
                target_relative = max(0.0, measured_rows[int(self._target) - 1]["time"] - origin)
        rel_last = max(0.0, self.last_time - origin)
        accepted_curve = [{"time": max(0.0, row["time"] - origin),
                           "accepted_sparse_snapshot_contributions": index}
                          for index, row in enumerate(measured_rows, 1)]
        return {
            "accepted_work": len(accepted_rows),
            "accepted_sparse_snapshot_contributions": len(accepted_rows),
            "accepted_work_unit": "accepted sparse snapshot contribution",
            "accepted_work_at_observation": accepted_at_boundary,
            "target_time": target_relative,
            "first_accept_time": start_credit,
            "measurement_start": self.measurement_start,
            "observation_boundary": boundary,
            "last_time": rel_last,
            "absolute_last_time": self.last_time,
            "population": self.config.get("population", len(nodes)),
            "node_count": len(nodes),
            "roles": dict(sorted(self._roles.items())),
            "role_counts": dict(Counter(self._roles.values())),
            "nodes": nodes,
            "epochs": epochs,
            "accepted_contributions": accepted_rows,
            "accepted_curve": accepted_curve,
            "ad_to_roster_wait_seconds": {
                "count": len(all_waits),
                "mean": sum(all_waits) / len(all_waits) if all_waits else None,
                "median": median(all_waits) if all_waits else None,
                "max": max(all_waits, default=None),
            },
            "outcomes": dict(self._statuses),
            "averaging_outcomes": dict(self._averaging_outcomes),
            "protocol_failures": dict(self._protocol_failures),
            "timeouts": dict(self._timeouts),
            "event_counts": dict(sorted(self._events.items())),
            "participant_counts": {
                "advertised_nodes": len({node for node, _ in self._advertisements}),
                "rostered_nodes": len({peer for rows in self._rosters.values() for row in rows for peer in row["peers"]}),
                "roster_observer_nodes": len({row["node"] for rows in self._rosters.values() for row in rows}),
                "contributing_origins": len({row["origin"] for row in accepted_rows}),
                "incorporating_recipients": len({row["recipient"] for row in self._complete_applications.values()}),
                "fully_applied_snapshot_receipts": len(self._complete_applications),
            },
        }


def _epoch_sort(value: str) -> tuple[int, str]:
    try:
        return int(value), value
    except ValueError:
        return 2**63 - 1, value


def replay_trace(path: str | Path, config: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Build a fresh accounting instance from a JSONL or gzip JSONL trace."""
    trace = Path(path)
    opener = gzip.open if trace.suffix == ".gz" else open
    accounting = Accounting(config)
    with opener(trace, "rt", encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, 1):
            if not line.strip():
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid Agora trace JSON at line {line_number}: {exc}") from exc
            if not isinstance(event, dict):
                raise ValueError(f"Agora trace line {line_number} must contain an object")
            accounting.consume(event)
    return accounting.summary()
