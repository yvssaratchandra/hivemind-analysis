"""Shared immutable records; no protocol or simulation dependencies."""
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable


class Outcome(str, Enum):
    COMPLETE = "complete"
    PARTIAL = "partial"
    FAILED = "failed"
    CENSORED = "censored"


@dataclass(frozen=True)
class NodeProfile:
    node_id: str
    peer_id: bytes
    compute_seconds: float
    upload_bps: float
    download_bps: float
    latency_seconds: float
    initial_group_bits: str = ""


@dataclass(frozen=True)
class WorkItem:
    work_id: str
    origin: str
    sequence: int
    compute_start: float
    compute_finish: float
    amount: float
    payload_bytes: int


@dataclass
class AttemptState:
    attempt_id: str
    work_id: str
    scheduled_time: float
    search_deadline: float
    phase: str = "searching"


@dataclass(frozen=True)
class Advertisement:
    key: str
    peer_id: str
    active: bool
    expiry: float


@dataclass(frozen=True)
class Message:
    message_id: str
    sender: str
    recipient: str
    kind: str
    body: dict[str, Any]
    wire_bytes: int
    attempt_id: str = ""
    group_id: str = ""


@dataclass(frozen=True)
class GroupRoster:
    group_id: str
    leader: str
    members: tuple[str, ...]
    work_ids: tuple[str, ...]
    formation_time: float


@dataclass(frozen=True)
class PartitionReceipt:
    group_id: str
    recipient: str
    partition: int
    size_bytes: int
    contributors: tuple[str, ...]
    normalized_weights: tuple[float, ...]
    fallback: bool = False


@dataclass(frozen=True)
class LocalAggregateResult:
    recipient: str
    group_id: str
    status: Outcome
    receipts: tuple[PartitionReceipt, ...]
    expected_bytes: int
    finish_time: float


@dataclass(frozen=True)
class TraceEvent:
    sequence: int
    time: float
    kind: str
    actor: str
    parent: int | None
    data: dict[str, Any]


@dataclass(frozen=True)
class RunResult:
    status: str
    target_time: float | None
    final_time: float
    artifacts: dict[str, str] = field(default_factory=dict)


@dataclass
class TimerHandle:
    """Cancellation token; cancelled callbacks must have no effects."""
    cancelled: bool = False

    def cancel(self) -> None:
        self.cancelled = True


@dataclass
class TransferHandle:
    transfer_id: int
    cancelled: bool = False


EventSink = Callable[..., None]
