"""Numerical local-roster model for fixed-group learning experiments.

This backend models reliable delivery before native deadlines. Missing senders
caused by incompatible membership expire before the local reducer completes.
It predicts vectors and fallback ranges, not network timing or leader exposure.
NumPy is optional and imported only when this backend is used.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import math
from typing import Any

from ...policies.base import PolicyObservation
from ..partitioning import allocate_elements, assign_chunks


@dataclass(frozen=True)
class NumericalResult:
    values: dict[str, Any]
    views: dict[str, tuple[str, ...]]
    allocations: dict[str, dict[str, list[tuple[int, int]]]]
    fallback_ranges: dict[str, list[tuple[int, int]]]
    banned_senders: dict[str, list[str]]
    trace: list[dict]


def announced_views(members, leader, policy, group_id="numerical"):
    """Assemble independent recipient views using only the leader's policy."""
    members = tuple(members)
    if len(set(members)) != len(members) or leader not in members:
        raise ValueError("Group requires unique members including its leader")
    proposed = {"members": list(members), "work_ids": [f"{group_id}:{n}" for n in members]}
    views = {}
    for recipient in members:
        hook = "local_aggregation" if recipient == leader else "group_announcement"
        observation = PolicyObservation(leader, hook, {"leader": leader, "recipient": recipient})
        action = policy.decide(observation, proposed)
        order = tuple(action["members"])
        if (recipient not in order or len(set(order)) != len(order)
                or not set(order).issubset(members)):
            raise ValueError("Unsupported announcement: invalid recipient membership")
        expected_ids = [f"{group_id}:{node}" for node in order]
        if action["work_ids"] != expected_ids:
            raise ValueError("Announcement metadata is not aligned")
        views[recipient] = order
    return views


def aggregate(inputs, views, chunk_elements=4, arrival_order=None, weights=None):
    """Apply native mean-minus-input corrections using each peer's own coordinates.

    No attack labels enter this function. A sender addresses the reducers in
    its own view. A reducer accepts only senders in its own view. Absent remote
    results preserve the recipient's input. Unequal stream lengths are rejected
    as outside the validated numerical backend rather than guessed. Optional
    ``weights`` maps every input node to its scalar contribution weight; omitted
    weights preserve the native default of one per peer.
    """
    import numpy as np

    arrays = {node: np.asarray(value, dtype=np.float32).reshape(-1) for node, value in inputs.items()}
    if not arrays or len({value.size for value in arrays.values()}) != 1:
        raise ValueError("Inputs must have one common nonempty shape")
    size = next(iter(arrays.values())).size
    if size == 0 or any(not np.isfinite(value).all() for value in arrays.values()):
        raise ValueError("Inputs must be finite and nonempty")
    if not views or not set(views).issubset(arrays):
        raise ValueError("Every active participant needs an input")
    if weights is None:
        peer_weights = {node: 1.0 for node in arrays}
    else:
        if not isinstance(weights, Mapping) or set(weights) != set(arrays):
            raise ValueError("Weights must map exactly the input nodes")
        peer_weights = {}
        for node, weight in weights.items():
            if isinstance(weight, (str, bytes, bool, np.bool_)) or not np.isscalar(weight):
                raise ValueError("Weights must be finite, nonnegative numeric scalars")
            try:
                numeric_weight = float(weight)
            except (TypeError, ValueError, OverflowError) as exc:
                raise ValueError("Weights must be finite, nonnegative numeric scalars") from exc
            if not math.isfinite(numeric_weight) or numeric_weight < 0:
                raise ValueError("Weights must be finite, nonnegative numeric scalars")
            peer_weights[node] = numeric_weight
    layout = {}
    for node, roster in views.items():
        if node not in roster or len(set(roster)) != len(roster) or not set(roster).issubset(views):
            raise ValueError("Invalid local roster")
        counts = allocate_elements(size, [None] * len(roster))
        chunks = assign_chunks(size, chunk_elements, counts)
        layout[node] = {
            peer: [(i * chunk_elements, min((i + 1) * chunk_elements, size)) for i in indices]
            for peer, indices in zip(roster, chunks)
        }
    output = {node: value.copy() for node, value in arrays.items()}
    fallback = {node: [] for node in arrays}
    banned = {node: [] for node in arrays}
    trace = []
    order = list(arrival_order) if arrival_order is not None else list(views)
    if set(order) != set(views) or len(order) != len(views):
        raise ValueError("Arrival order must contain each active node exactly once")
    for reducer in order:
        roster = views[reducer]
        own_ranges = layout[reducer][reducer]
        senders = [source for source in order if source in roster and reducer in views[source]]
        absent = [source for source in roster if reducer not in views[source]]
        banned[reducer] = absent
        for source in absent:
            trace.append({"kind": "sender_timeout", "node": reducer, "sender": source})
        for source in senders:
            if len(layout[source][reducer]) != len(own_ranges):
                raise ValueError("Unequal stream lengths require a separately validated backend")
        for index, (start, end) in enumerate(own_ranges):
            pieces = {}
            for source in senders:
                left, right = layout[source][reducer][index]
                pieces[source] = arrays[source][left:right]
                if pieces[source].size != end - start:
                    raise ValueError("Incompatible chunk shapes")
            # Native reducer uses float32 accumulated weighted sums.
            mean = np.zeros(end - start, dtype=np.float32)
            total_weight = 0.0
            for source in senders:
                weight = peer_weights[source]
                if weight == 1.0:
                    mean += pieces[source]
                elif weight != 0.0:
                    mean += pieces[source] * np.float32(weight)
                total_weight += weight
            if total_weight == 0.0:
                raise ValueError("Accepted sender weights must have a positive total")
            mean /= total_weight
            for recipient in senders:
                left, right = layout[recipient][reducer][index]
                delta = np.asarray(mean - pieces[recipient], dtype=np.float32)
                output[recipient][left:right] += delta
                trace.append({"kind": "processed_chunk", "node": recipient, "reducer": reducer,
                              "part_index": index, "range": [left, right],
                              "sources": senders, "source_ranges": {
                                  source: list(layout[source][reducer][index]) for source in senders},
                              "delta": delta.tolist(), "fallback": False})
    for recipient, roster in views.items():
        for reducer in roster:
            if recipient in views[reducer]:
                continue
            for index, interval in enumerate(layout[recipient][reducer]):
                fallback[recipient].append(interval)
                trace.append({"kind": "processed_chunk", "node": recipient, "reducer": reducer,
                              "part_index": index, "range": list(interval), "sources": [recipient],
                              "delta": [0.] * (interval[1] - interval[0]), "fallback": True})
    return NumericalResult(output, {n: tuple(v) for n, v in views.items()},
                           layout, fallback, banned, trace)
