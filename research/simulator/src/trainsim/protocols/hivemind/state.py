"""Node-local Hivemind matchmaking state used by the adapter."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ...types import AttemptState, NodeProfile, TimerHandle, WorkItem


@dataclass
class JoinRequest:
    peer_id: str
    work_id: str
    expiration: float
    group_key: str
    attempt_id: str


@dataclass
class PendingJoin:
    peer_id: str
    peer_expiration: float
    request_expiration: float
    attempt_id: str
    generation: int


@dataclass
class NodeState:
    profile: NodeProfile
    group_bits: str
    work: WorkItem | None = None
    attempt: AttemptState | None = None
    retry_sequence: int = 0
    generation: int = 0
    active: bool = True
    phase: str = "idle"
    declared_key: str | None = None
    declared_expiration: float = float("inf")
    current_leader: str | None = None
    current_followers: dict[str, JoinRequest] = field(default_factory=dict)
    pending_join: PendingJoin | None = None
    attempted_candidates: set[tuple[str, float]] = field(default_factory=set)
    lookup_inflight: bool = False
    timers: dict[str, TimerHandle] = field(default_factory=dict)
    message_sequence: int = 0
    last_terminal_work_id: str | None = None
    group_id: str | None = None
    group_run: Any = None

    @property
    def node_id(self) -> str:
        return self.profile.node_id

    @property
    def peer_id_bytes(self) -> bytes:
        return self.profile.peer_id

    def cancel_timer(self, name: str) -> None:
        timer = self.timers.pop(name, None)
        if timer is not None:
            timer.cancel()

    def cancel_all_timers(self) -> None:
        for timer in self.timers.values():
            timer.cancel()
        self.timers.clear()
