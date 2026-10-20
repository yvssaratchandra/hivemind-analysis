"""Local, capability-limited participant behaviour policies."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True)
class PolicyObservation:
    """Information available to one node at a single protocol hook."""

    node_id: str
    hook: str
    local: dict[str, Any]


class BehaviourPolicy(Protocol):
    """Choose among actions available to the node at the current hook."""

    def decide(
        self,
        observation: PolicyObservation,
        proposed: dict[str, Any],
    ) -> dict[str, Any]: ...


class HonestPolicy:
    """Return the protocol's proposed action unchanged."""

    def decide(self, observation: PolicyObservation, proposed: dict[str, Any]) -> dict[str, Any]:
        return dict(proposed)


def policy_from_config(config: dict[str, Any] | None) -> BehaviourPolicy:
    """Build a deterministic policy from the JSON-safe scenario settings."""
    if not config or config.get("kind", "honest") == "honest":
        return HonestPolicy()
    from .fixtures import FixturePolicy

    kind = config.get("kind")
    if kind != "fixture":
        raise ValueError(f"Unknown policy kind: {kind!r}")
    return FixturePolicy(
        delay_transfer_seconds=float(config.get("delay_transfer_seconds", 0.0)),
        reject_join_from=frozenset(config.get("reject_join_from", [])),
        prefer_candidates=frozenset(config.get("prefer_candidates", [])),
        delay_announcement_seconds=float(config.get("delay_announcement_seconds", 0.0)),
    )
