"""Deterministic policy fixtures for exercising protocol attack hooks."""

from __future__ import annotations

from .base import PolicyObservation


class FixturePolicy:
    """Select only actions exposed by the current node's local protocol hook.

    ``reject_join_from`` contains peers whose requests this node rejects while
    acting as an eligible leader. ``prefer_candidates`` can only select peers
    present in the node's current lookup response. Delays affect this node's
    own outgoing transfer or announcement.
    """

    def __init__(
        self,
        *,
        delay_transfer_seconds: float = 0.0,
        reject_join_from: frozenset[str] = frozenset(),
        prefer_candidates: frozenset[str] = frozenset(),
        delay_announcement_seconds: float = 0.0,
    ) -> None:
        for name, value in (
            ("delay_transfer_seconds", delay_transfer_seconds),
            ("delay_announcement_seconds", delay_announcement_seconds),
        ):
            if value < 0:
                raise ValueError(f"{name} must be non-negative")
        self.delay_transfer_seconds = float(delay_transfer_seconds)
        self.reject_join_from = frozenset(reject_join_from)
        self.prefer_candidates = frozenset(prefer_candidates)
        self.delay_announcement_seconds = float(delay_announcement_seconds)

    def decide(self, observation: PolicyObservation, proposed: dict) -> dict:
        result = dict(proposed)
        if observation.hook == "candidate_selection":
            candidates = list(observation.local.get("candidates", ()))
            preferred = [peer for peer in candidates if peer in self.prefer_candidates]
            if preferred:
                # Candidate order is already protocol priority order. Preserve
                # that order among the locally visible controlled peers.
                result["peer_id"] = preferred[0]
        elif observation.hook == "incoming_join":
            peer = observation.local.get("peer_id")
            if peer in self.reject_join_from:
                result.update(accept=False, reason="fixture_selective_rejection")
        elif observation.hook in ("input_transfer", "result_transfer"):
            result["delay_seconds"] = self.delay_transfer_seconds
        elif observation.hook == "group_announcement":
            result["delay_seconds"] = self.delay_announcement_seconds
        return result
