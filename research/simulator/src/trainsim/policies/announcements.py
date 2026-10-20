"""Recipient-specific leader actions, independent of numerical protocol behavior."""
from __future__ import annotations

from .base import PolicyObservation


class AnnouncementPolicy:
    def __init__(self, kind: str, target: str = "A"):
        if kind not in ("honest", "membership", "ordering"):
            raise ValueError(f"Unknown announcement policy: {kind}")
        self.kind, self.target = kind, target

    def decide(self, observation: PolicyObservation, proposed: dict) -> dict:
        result = dict(proposed)
        if (observation.hook not in ("group_announcement", "local_aggregation")
                or observation.node_id != observation.local.get("leader")):
            return result
        members = list(proposed["members"])
        if self.target not in members or self.target == observation.node_id:
            return result
        recipient = observation.local.get("recipient", observation.node_id)
        if self.kind == "membership" and recipient != self.target:
            order = [peer for peer in members if peer != self.target]
        elif self.kind == "ordering" and recipient == self.target:
            order = list(members)
            a, b = order.index(self.target), order.index(observation.node_id)
            order[a], order[b] = order[b], order[a]
        else:
            return result
        metadata = dict(zip(members, proposed["work_ids"]))
        result.update(members=order, work_ids=[metadata[peer] for peer in order])
        return result
