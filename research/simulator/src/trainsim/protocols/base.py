"""Minimal protocol adapter contract shared by workload and runner."""

from __future__ import annotations

from typing import Protocol

from ..types import Message, WorkItem


class ProtocolAdapter(Protocol):
    """Expose node lifecycle and message callbacks to the simulation runner."""

    def start(self, node_id: str) -> None: ...

    def on_work_ready(self, node_id: str, work: WorkItem) -> None: ...

    def on_message(self, node_id: str, message: Message) -> None: ...

    def stop(self, node_id: str) -> None: ...
