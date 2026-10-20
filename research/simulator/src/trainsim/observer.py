"""Streaming event observer and work-credit accounting."""

from __future__ import annotations

import gzip
import json
import math
from collections import defaultdict
from dataclasses import asdict, is_dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Mapping


def _value(value: Any) -> Any:
    """Convert event values to stable JSON-compatible values."""
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value):
        return _value(asdict(value))
    if isinstance(value, bytes):
        return {"__bytes_hex__": value.hex()}
    if isinstance(value, Mapping):
        return {str(key): _value(item) for key, item in value.items()}
    if isinstance(value, (set, frozenset)):
        return [_value(item) for item in sorted(value, key=repr)]
    if isinstance(value, (tuple, list)):
        return [_value(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return repr(value)


def _text(value: Any) -> str:
    return str(value.value if isinstance(value, Enum) else value)


def _dict(value: Any) -> dict[str, Any]:
    if is_dataclass(value):
        value = asdict(value)
    if isinstance(value, Mapping):
        return dict(value)
    return {}


class Observer:
    """Record events incrementally and account for unique fully incorporated work."""

    def __init__(self, path: str | Path | None = None, labels: Mapping[str, set[str]] | None = None):
        self.path = Path(path) if path is not None else None
        self.labels = {str(name): {str(node) for node in nodes} for name, nodes in (labels or {}).items()}
        self.target_work: float | None = None
        self.target_time: float | None = None
        self.accepted_work = 0.0
        self.last_time = 0.0
        self._observation_seconds: float | None = None

        self._sequence = 0
        self._events_since_flush = 0
        self._stream: Any = None
        if self.path is not None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            if self.path.suffix == ".gz":
                self._stream = gzip.open(self.path, "wt", encoding="utf-8", newline="\n")
            else:
                self._stream = self.path.open("w", encoding="utf-8", newline="\n")

        self._work: dict[str, dict[str, Any]] = {}
        self._work_by_origin: dict[str, list[str]] = defaultdict(list)
        self._credited: set[str] = set()
        self._groups: list[dict[str, Any]] = []
        self._group_work_by_node: dict[str, dict[str, str]] = {}
        self._effective_set_groups: dict[tuple[Any, ...], dict[str, Any]] = {}
        self._node: dict[str, dict[str, Any]] = {}
        self._phase_current: dict[str, tuple[str, float]] = {}
        self._phase_durations: dict[str, float] = defaultdict(float)
        self._outcomes: dict[str, int] = defaultdict(int)
        self._aggregate_results = 0
        self._partial_occurrences = 0
        self._partial_work_ids: set[str] = set()
        self._events_by_kind: dict[str, int] = defaultdict(int)

        for name, nodes in self.labels.items():
            for node_id in nodes:
                self._node_for(node_id)

    def emit(self, time: float, kind: str, actor: str = "", **data: Any) -> None:
        """Persist one event and update observer-owned metrics."""
        event_time = float(time)
        event_kind = _text(kind)
        event_actor = _text(actor) if actor else ""
        self.last_time = max(self.last_time, event_time)
        self._events_by_kind[event_kind] += 1

        serial_data = _value(data)
        parent = serial_data.pop("parent", None) if isinstance(serial_data, dict) else None
        self._sequence += 1
        if self._stream is not None:
            row = {
                "sequence": self._sequence,
                "time": event_time,
                "kind": event_kind,
                "actor": event_actor,
                "parent": parent,
                "data": serial_data,
            }
            self._stream.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
            self._events_since_flush += 1
            # Bound the amount of buffered trace lost if a long run is interrupted.
            if self._events_since_flush >= 128:
                self._stream.flush()
                self._events_since_flush = 0

        if event_kind == "work_ready":
            self._on_work_ready(event_time, _dict(data.get("work")))
        elif event_kind == "group_formed":
            self._on_group_formed(event_time, _dict(data.get("roster")))
        elif event_kind == "aggregate_result":
            self._on_aggregate_result(event_time, _dict(data.get("result")))
        elif event_kind == "run_started":
            self._on_run_started(_dict(data))
        elif event_kind == "node_phase":
            self._on_node_phase(event_time, event_actor, _dict(data))
        elif event_kind == "work_terminal":
            self._on_work_terminal(_dict(data))

    def summary(self) -> dict[str, Any]:
        """Return a JSON-compatible snapshot of scalar metrics and analysis tables."""
        return self._build_summary()

    def close(self) -> None:
        """Flush and close the optional trace sink."""
        if self._stream is not None:
            self._stream.flush()
            self._stream.close()
            self._stream = None

    def _node_for(self, node_id: str) -> dict[str, Any]:
        node = self._node.get(node_id)
        if node is None:
            node = {
                "node_id": node_id,
                "work_ready": 0,
                "work_ready_amount": 0.0,
                "accepted_work": 0.0,
                "direct_weight_sum": 0.0,
                "direct_weight_observations": 0,
                "accepted_count": 0,
                "announced_group_count": 0,
                "effective_chunk_count": 0,
                "ready_wait_seconds_total": 0.0,
                "ready_wait_count": 0,
                "first_ready_time": None,
                "acceptance_times": [],
                "partners": set(),
                "labels": sorted(name for name, nodes in self.labels.items() if node_id in nodes),
                "partial_occurrences": 0,
            }
            self._node[node_id] = node
        return node

    def _on_work_ready(self, time: float, work: dict[str, Any]) -> None:
        work_id = str(work.get("work_id", ""))
        origin = str(work.get("origin", ""))
        if not work_id:
            return
        # Preserve the first observation so replayed duplicates cannot reset accounting.
        if work_id in self._work:
            return
        amount = float(work.get("amount", 1.0))
        amount = amount if math.isfinite(amount) and amount >= 0 else 0.0
        row = {
            "work_id": work_id,
            "origin": origin,
            "sequence": work.get("sequence"),
            "amount": amount,
            "payload_bytes": int(work.get("payload_bytes", 0) or 0),
            "ready_time": time,
            "compute_start": work.get("compute_start"),
            "compute_finish": work.get("compute_finish"),
            "group_time": None,
            "accepted_time": None,
            "partial_occurrences": 0,
        }
        self._work[work_id] = row
        self._work_by_origin[origin].append(work_id)
        node = self._node_for(origin)
        node["work_ready"] += 1
        node["work_ready_amount"] += amount
        first_ready = node["first_ready_time"]
        node["first_ready_time"] = time if first_ready is None else min(first_ready, time)

    def _on_run_started(self, data: dict[str, Any]) -> None:
        """Recover target, labels and known population from a trace header event."""
        if self.target_work is None and data.get("target_work") is not None:
            self.target_work = float(data["target_work"])
        if not self.labels and isinstance(data.get("labels"), Mapping):
            self.labels = {
                str(name): {str(node_id) for node_id in node_ids}
                for name, node_ids in data["labels"].items()
            }
            for nodes in self.labels.values():
                for node_id in nodes:
                    self._node_for(node_id)
        config = _dict(data.get("config"))
        if config.get("observation_seconds") is not None:
            self._observation_seconds = float(config["observation_seconds"])
        population = config.get("population")
        if isinstance(population, int) and population >= 0:
            for index in range(population):
                node = self._node_for(f"N{index:04d}")
                if node["first_ready_time"] is None:
                    node["first_ready_time"] = 0.0

    def _on_group_formed(self, time: float, roster: dict[str, Any]) -> None:
        group_id = str(roster.get("group_id", ""))
        members = [str(node) for node in (roster.get("members") or [])]
        work_ids = [str(work_id) for work_id in (roster.get("work_ids") or [])]
        node_work = {node: work_id for node, work_id in zip(members, work_ids)}
        if group_id:
            self._group_work_by_node[group_id] = node_work
        for work_id in work_ids:
            work = self._work.get(work_id)
            if work is not None and work["group_time"] is None:
                work["group_time"] = time
                node = self._node_for(work["origin"])
                node["ready_wait_seconds_total"] += max(0.0, time - work["ready_time"])
                node["ready_wait_count"] += 1

        member_set = set(members)
        for member in members:
            self._node_for(member)["announced_group_count"] += 1
        for member in member_set:
            self._node_for(member)["partners"].update(member_set - {member})
        label_counts = {name: sum(member in nodes for member in member_set) for name, nodes in self.labels.items()}
        size = len(member_set)
        row = {
            "group_id": group_id,
            "time": time,
            "leader": str(roster.get("leader", "")),
            "members": sorted(member_set),
            "group_size": size,
            "controlled_counts": label_counts,
            "mixing": self._mixing(label_counts, size),
            "krum_count_condition_holds": {
                name: size > 2 * count + 2 for name, count in label_counts.items()
            },
        }
        self._groups.append(row)

    def _resolve_work_id(self, contributor: Any, group_id: str) -> str | None:
        candidate = str(contributor)
        if candidate in self._work:
            return candidate
        group_map = self._group_work_by_node.get(group_id, {})
        if candidate in group_map:
            work_id = group_map[candidate]
            return work_id if work_id in self._work else None
        return None

    def _on_aggregate_result(self, time: float, result: dict[str, Any]) -> None:
        self._aggregate_results += 1
        group_id = str(result.get("group_id", ""))
        recipient = str(result.get("recipient", ""))
        status = _text(result.get("status", ""))
        expected_bytes = int(result.get("expected_bytes", 0) or 0)
        receipts = [_dict(receipt) for receipt in (result.get("receipts") or [])]
        resolved_receipts: list[dict[str, Any]] = []
        for receipt in receipts:
            raw_contributors = [str(item) for item in (receipt.get("contributors") or [])]
            contributor_work_ids = sorted({
                work_id for item in raw_contributors
                if (work_id := self._resolve_work_id(item, group_id)) is not None
            })
            contributor_nodes = sorted({self._work[work_id]["origin"] for work_id in contributor_work_ids})
            raw_weights = receipt.get("normalized_weights") or []
            weight_by_work_id: dict[str, float] = {}
            for contributor, weight in zip(raw_contributors, raw_weights):
                work_id = self._resolve_work_id(contributor, group_id)
                try:
                    numeric_weight = float(weight)
                except (TypeError, ValueError):
                    numeric_weight = math.nan
                if work_id is not None and math.isfinite(numeric_weight):
                    weight_by_work_id[work_id] = numeric_weight
            fallback = bool(receipt.get("fallback", False))
            receipt_group_id = str(receipt.get("group_id", group_id))
            receipt_recipient = str(receipt.get("recipient", recipient))
            numeric_weights = []
            for weight in raw_weights:
                try:
                    numeric_weights.append(float(weight))
                except (TypeError, ValueError):
                    numeric_weights.append(math.nan)
            weights_valid = not fallback and (
                len(numeric_weights) == len(raw_contributors)
                and len(set(raw_contributors)) == len(raw_contributors)
                and all(math.isfinite(weight) and weight >= 0 for weight in numeric_weights)
                and math.isclose(sum(numeric_weights), 1.0, rel_tol=1e-6, abs_tol=1e-8)
            )
            if weights_valid:
                for work_id, numeric_weight in weight_by_work_id.items():
                    origin = self._work[work_id]["origin"]
                    node = self._node_for(origin)
                    node["direct_weight_sum"] += numeric_weight
                    node["direct_weight_observations"] += 1
            size_bytes = int(receipt.get("size_bytes", 0) or 0)
            part = {
                "group_id": group_id,
                "recipient": recipient,
                "partition": int(receipt.get("partition", -1)),
                "time": float(result.get("finish_time", time)),
                "size_bytes": size_bytes,
                "contributors_work_ids": contributor_work_ids,
                "contributors": contributor_nodes,
                "normalized_weights_by_work_id": weight_by_work_id,
                "normalized_weights_valid": weights_valid,
                "fallback": fallback,
                "receipt_group_matches": receipt_group_id == group_id,
                "receipt_recipient_matches": receipt_recipient == recipient,
                "status": status,
                "controlled_counts": {
                    name: sum(node in nodes for node in contributor_nodes)
                    for name, nodes in self.labels.items()
                },
            }
            part["mixing"] = self._mixing(part["controlled_counts"], len(contributor_nodes))
            part["krum_count_condition_holds"] = {
                name: len(contributor_nodes) > 2 * count + 2
                for name, count in part["controlled_counts"].items()
            }
            resolved_receipts.append(part)
            signature = (
                group_id, recipient, tuple(contributor_work_ids), fallback,
                weights_valid, receipt_group_id == group_id,
                receipt_recipient == recipient, status,
            )
            combined = self._effective_set_groups.get(signature)
            if combined is None:
                combined = {
                    "group_id": group_id,
                    "recipient": recipient,
                    "contributors_work_ids": contributor_work_ids,
                    "contributors": contributor_nodes,
                    "fallback": fallback,
                    "normalized_weights_valid": weights_valid,
                    "receipt_group_matches": receipt_group_id == group_id,
                    "receipt_recipient_matches": receipt_recipient == recipient,
                    "status": status,
                    "controlled_counts": part["controlled_counts"],
                    "mixing": part["mixing"],
                    "krum_count_condition_holds": part["krum_count_condition_holds"],
                    "partition_count": 0,
                    "size_bytes": 0,
                    "partition_index_min": part["partition"],
                    "partition_index_max": part["partition"],
                    "first_time": part["time"],
                    "last_time": part["time"],
                    "weight_sums": defaultdict(float),
                    "weight_observations": defaultdict(int),
                }
                self._effective_set_groups[signature] = combined
            combined["partition_count"] += 1
            combined["size_bytes"] += size_bytes
            combined["partition_index_min"] = min(combined["partition_index_min"], part["partition"])
            combined["partition_index_max"] = max(combined["partition_index_max"], part["partition"])
            combined["first_time"] = min(combined["first_time"], part["time"])
            combined["last_time"] = max(combined["last_time"], part["time"])
            for work_id, weight in weight_by_work_id.items():
                combined["weight_sums"][work_id] += weight
                combined["weight_observations"][work_id] += 1
            for node_id in contributor_nodes:
                self._node_for(node_id)["effective_chunk_count"] += 1
            for node_id in contributor_nodes:
                self._node_for(node_id)["partners"].update(set(contributor_nodes) - {node_id})

        full_result = self._is_full_result(status, expected_bytes, resolved_receipts)
        per_work_presence: dict[str, int] = defaultdict(int)
        per_work_fallback: dict[str, bool] = defaultdict(bool)
        for receipt in resolved_receipts:
            for work_id in receipt["contributors_work_ids"]:
                per_work_presence[work_id] += 1
                per_work_fallback[work_id] = per_work_fallback[work_id] or receipt["fallback"]

        credit_candidates: set[str] = set()
        for work_id, presence in per_work_presence.items():
            work = self._work.get(work_id)
            if work is None or work["origin"] == recipient:
                continue
            if full_result and presence == len(resolved_receipts) and not per_work_fallback[work_id]:
                credit_candidates.add(work_id)
            else:
                self._partial_occurrences += 1
                self._partial_work_ids.add(work_id)
                work["partial_occurrences"] += 1
                self._node_for(work["origin"])["partial_occurrences"] += 1

        if status == "complete":
            self._outcomes["complete"] += 1
        elif status == "partial":
            self._outcomes["partial"] += 1
        elif status:
            self._outcomes[status] += 1

        for work_id in sorted(credit_candidates):
            if work_id in self._credited:
                continue
            work = self._work[work_id]
            self._credited.add(work_id)
            self.accepted_work += work["amount"]
            work["accepted_time"] = float(result.get("finish_time", time))
            node = self._node_for(work["origin"])
            node["accepted_work"] += work["amount"]
            node["accepted_count"] += 1
            node["acceptance_times"].append(work["accepted_time"])
            self.emit(
                work["accepted_time"],
                "work_accepted",
                work["origin"],
                work_id=work_id,
                recipient=recipient,
                group_id=group_id,
                amount=work["amount"],
            )
            if self.target_time is None and self.target_work is not None and self.accepted_work >= self.target_work:
                self.target_time = work["accepted_time"]

    @staticmethod
    def _is_full_result(status: str, expected_bytes: int, receipts: list[dict[str, Any]]) -> bool:
        if status != "complete" or expected_bytes <= 0 or not receipts:
            return False
        partitions = [item["partition"] for item in receipts]
        if len(set(partitions)) != len(partitions) or any(part < 0 for part in partitions):
            return False
        if sorted(partitions) != list(range(len(partitions))):
            return False
        if any(item["fallback"] or item["size_bytes"] <= 0 for item in receipts):
            return False
        if any(not item["normalized_weights_valid"] for item in receipts):
            return False
        if any(not item["receipt_group_matches"] or not item["receipt_recipient_matches"] for item in receipts):
            return False
        return sum(item["size_bytes"] for item in receipts) == expected_bytes

    def _on_node_phase(self, time: float, actor: str, data: dict[str, Any]) -> None:
        if not actor:
            return
        phase = str(data.get("phase", data.get("to", "")))
        if not phase:
            return
        previous = self._phase_current.get(actor)
        if previous is not None:
            old_phase, old_time = previous
            self._phase_durations[old_phase] += max(0.0, time - old_time)
        self._phase_current[actor] = (phase, time)
        self._node_for(actor)

    def _on_work_terminal(self, data: dict[str, Any]) -> None:
        outcome = _text(data.get("outcome", data.get("status", "unknown")))
        self._outcomes[f"terminal_{outcome}"] += 1

    @staticmethod
    def _mixing(counts: Mapping[str, int], size: int) -> dict[str, dict[str, Any]]:
        result = {}
        for name, controlled in counts.items():
            result[name] = {
                "mixed": controlled > 0 and controlled < size,
                "controlled_only": controlled > 0 and controlled == size,
            }
        return result

    def _build_summary(self) -> dict[str, Any]:
        final_time = self.last_time
        per_node = []
        for node_id in sorted(self._node):
            node = self._node[node_id]
            accepts = sorted(node["acceptance_times"])
            start = node["first_ready_time"]
            gaps = []
            if start is not None:
                cursor = start
                for accepted_at in accepts:
                    if accepted_at >= cursor:
                        gaps.append(accepted_at - cursor)
                        cursor = accepted_at
                gaps.append(max(0.0, final_time - cursor))
            per_node.append({
                "node_id": node_id,
                "labels": list(node["labels"]),
                "work_ready": node["work_ready"],
                "work_ready_amount": node["work_ready_amount"],
                "accepted_work": node["accepted_work"],
                "first_accept_time": accepts[0] if accepts else None,
                "direct_aggregation_weight_mean": (
                    node["direct_weight_sum"] / node["direct_weight_observations"]
                    if node["direct_weight_observations"] else None
                ),
                "direct_aggregation_weight_observations": node["direct_weight_observations"],
                "accepted_count": node["accepted_count"],
                "zero_participation": node["accepted_count"] == 0,
                "announced_group_count": node["announced_group_count"],
                "effective_chunk_count": node["effective_chunk_count"],
                "unique_partners": len(node["partners"]),
                "ready_wait_seconds_total": node["ready_wait_seconds_total"],
                "ready_wait_count": node["ready_wait_count"],
                "ready_wait_seconds_mean": (
                    node["ready_wait_seconds_total"] / node["ready_wait_count"]
                    if node["ready_wait_count"] else None
                ),
                "longest_no_acceptance_seconds": max(gaps) if gaps else None,
                "partial_occurrences": node["partial_occurrences"],
            })
        partial_ids_without_credit = self._partial_work_ids - self._credited
        first_accept_time = min(
            (work["accepted_time"] for work_id, work in self._work.items() if work_id in self._credited),
            default=None,
        )
        if self._observation_seconds is not None:
            accepted_at_observation = sum(
                work["amount"] for work_id, work in self._work.items()
                if work_id in self._credited
                and work["accepted_time"] is not None
                and work["accepted_time"] < self._observation_seconds
            )
        else:
            accepted_at_observation = None
        effective_sets = []
        for combined in self._effective_set_groups.values():
            observations = combined["weight_observations"]
            effective_sets.append({
                key: value for key, value in combined.items()
                if key not in {"weight_sums", "weight_observations"}
            } | {
                "normalized_weights_by_work_id": {
                    work_id: combined["weight_sums"][work_id] / observations[work_id]
                    for work_id in sorted(observations)
                    if observations[work_id]
                },
            })
        effective_sets.sort(key=lambda row: (
            row["group_id"], row["recipient"], row["contributors_work_ids"], row["fallback"], row["status"]
        ))
        scalar = {
            "accepted_work": self.accepted_work,
            "target_work": self.target_work,
            "target_time": self.target_time,
            "first_accept_time": first_accept_time,
            "accepted_work_at_observation": accepted_at_observation,
            "last_time": final_time,
            "event_count": self._sequence,
            "work_ready_count": len(self._work),
            "accepted_work_items": len(self._credited),
            "partial_occurrences": self._partial_occurrences,
            "unique_partially_incorporated_work_items": len(self._partial_work_ids),
            "partial_only_work_items": len(partial_ids_without_credit),
            "aggregate_result_count": self._aggregate_results,
            "group_count": len(self._groups),
            "effective_set_count": len(effective_sets),
            "effective_partition_count": sum(row["partition_count"] for row in effective_sets),
            "zero_participation_nodes": sum(row["zero_participation"] for row in per_node),
            "outcomes": dict(sorted(self._outcomes.items())),
            "events_by_kind": dict(sorted(self._events_by_kind.items())),
        }
        phase_durations = dict(self._phase_durations)
        for node_id, (phase, start) in self._phase_current.items():
            phase_durations[phase] = phase_durations.get(phase, 0.0) + max(0.0, final_time - start)
        return {
            **scalar,
            "per_node": per_node,
            "groups": list(self._groups),
            "effective_sets": effective_sets,
            "phase_durations": dict(sorted(phase_durations.items())),
        }
