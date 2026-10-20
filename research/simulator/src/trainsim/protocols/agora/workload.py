"""Continuous independent local steps and sparse averaging opportunities."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from typing import Any

from ...randomness import rng
from ...types import NodeProfile
from .partitioning import sparse_cardinalities


def layout_id(tensor_elements: list[int], partitions: int) -> str:
    """Stable identity for the compact sparse tensor layout."""
    payload = json.dumps(
        {"tensor_elements": tensor_elements, "partitions": partitions, "element_bytes": 4},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    return hashlib.sha256(payload).hexdigest()


def component_cardinalities(tensor_cardinalities: tuple[int, ...], chunk_bytes: int) -> dict[str, int]:
    """Return symbolic per-tensor chunk sizes for the compact sparse snapshot."""
    chunk_elements = max(1, int(chunk_bytes) // 4)
    return {
        f"t{tensor_index}:c{chunk_index}": min(chunk_elements, size - start)
        for tensor_index, size in enumerate(tensor_cardinalities)
        for chunk_index, start in enumerate(range(0, size, chunk_elements))
    }


@dataclass(frozen=True)
class SparseSnapshot:
    node_id: str
    epoch: int
    global_step: int
    layout: str
    partition: int
    tensor_cardinalities: tuple[int, ...]
    selected_cardinality: int
    snapshot_id: str
    role: str
    weight: float = 1.0
    advertised_bandwidth: None = None

    def as_gather(self) -> dict[str, Any]:
        return {
            "node_id": self.node_id,
            "role": self.role,
            "weight": self.weight,
            "advertised_bandwidth": self.advertised_bandwidth,
            "epoch": self.epoch,
            "global_step": self.global_step,
            "layout": self.layout,
            "partition": self.partition,
            "tensor_cardinalities": list(self.tensor_cardinalities),
            "selected_cardinality": self.selected_cardinality,
            "snapshot_id": self.snapshot_id,
        }


class AgoraWorkload:
    """Keep local training steps moving while averaging runs independently."""

    def __init__(self, engine, profiles: dict[str, NodeProfile], config: dict[str, Any], matcher, aggregator):
        self.engine, self.profiles, self.config = engine, dict(profiles), config
        self.matcher, self.aggregator = matcher, aggregator
        self.steps = {node: 0 for node in profiles}
        self.busy: dict[str, int | None] = {node: None for node in profiles}
        self.active = True
        self.stage = str(config["stage"])
        self.layout = layout_id(list(config["tensor_elements"]), int(config["partitions"]))
        self.roles = {
            node: ("NODE" if index < int(config["reducers"]) else "CLIENT")
            for index, node in enumerate(sorted(profiles))
        }
        self._active_match_epoch: dict[str, int | None] = {node: None for node in profiles}

    def start(self) -> None:
        """Begin seeded initial compute offsets after the setup barrier."""
        for node in self.profiles:
            self.engine.emit(
                "agora_role",
                node,
                stage=self.stage,
                role=self.roles[node],
                weight=1.0,
                advertised_bandwidth=None,
                upload_bps=self.profiles[node].upload_bps,
                download_bps=self.profiles[node].download_bps,
            )
            offset = rng(self.engine.seed, "agora-initial-readiness", node).uniform(
                0.0, float(self.config["initial_offset_seconds"])
            )
            self.engine.emit(
                "agora_initial_compute_offset",
                node,
                offset_seconds=offset,
                seed=self.engine.seed,
            )
            self.engine.schedule(offset, self._begin_compute, node)

    def _begin_compute(self, node_id: str) -> None:
        if not self.active:
            return
        started = self.engine.now
        self.engine.emit(
            "agora_compute_started",
            node_id,
            local_step=self.steps[node_id] + 1,
            compute_start=started,
            compute_seconds=float(self.profiles[node_id].compute_seconds),
        )
        self.engine.schedule(
            float(self.profiles[node_id].compute_seconds), self._finish_compute, node_id, started
        )

    def _finish_compute(self, node_id: str, started: float) -> None:
        if not self.active:
            return
        step = self.steps[node_id] + 1
        self.steps[node_id] = step
        self.engine.emit(
            "agora_local_step",
            node_id,
            local_step=step,
            compute_start=started,
            compute_finish=self.engine.now,
        )
        interval = int(self.config["average_every"])
        if step % interval == 0:
            # Match the native harness, whose first averaging round is 1.
            epoch = step // interval
            active_epoch = self.busy[node_id]
            if active_epoch is not None:
                self.engine.emit(
                    "round_skipped",
                    node_id,
                    stage=self.stage,
                    epoch=epoch,
                    local_step=step,
                    active_epoch=active_epoch,
                    reason="averaging_busy",
                )
            else:
                self._start_average(node_id, epoch, step)
        # The previous step just finished; begin the next one immediately.
        # Thus completed local steps remain separated by compute_seconds.
        self.engine.schedule(0.0, self._begin_compute, node_id)

    def _start_average(self, node_id: str, epoch: int, global_step: int) -> None:
        partition = epoch % int(self.config["partitions"])
        tensor_cardinalities = sparse_cardinalities(
            self.config["tensor_elements"], int(self.config["partitions"]), partition
        )
        snapshot = SparseSnapshot(
            node_id=node_id,
            epoch=epoch,
            global_step=global_step,
            layout=self.layout,
            partition=partition,
            tensor_cardinalities=tensor_cardinalities,
            selected_cardinality=sum(tensor_cardinalities),
            snapshot_id=f"{node_id}:e{epoch}:{self.layout[:12]}:p{partition}",
            role=self.roles[node_id],
        )
        self.busy[node_id] = epoch
        self._active_match_epoch[node_id] = epoch
        gather = snapshot.as_gather()
        expected_components = component_cardinalities(
            tensor_cardinalities, int(self.config["chunk_bytes"])
        )
        self.engine.emit(
            "agora_snapshot_created",
            node_id,
            stage=self.stage,
            epoch=epoch,
            global_step=global_step,
            layout=snapshot.layout,
            partition=partition,
            selected_cardinality=snapshot.selected_cardinality,
            tensor_cardinalities=list(tensor_cardinalities),
            snapshot_id=snapshot.snapshot_id,
            expected_component_ids=list(expected_components),
            expected_component_cardinalities=expected_components,
        )
        self.engine.emit(
            "agora_aggregate_started",
            node_id,
            stage=self.stage,
            epoch=epoch,
            global_step=global_step,
            role=snapshot.role,
            weight=snapshot.weight,
            layout=snapshot.layout,
            partition=partition,
            selected_cardinality=snapshot.selected_cardinality,
            tensor_cardinalities=list(tensor_cardinalities),
            snapshot_id=snapshot.snapshot_id,
            expected_component_ids=list(expected_components),
            expected_component_cardinalities=expected_components,
        )

        def on_roster(_node: str, _epoch: int, roster: dict[str, Any] | None) -> None:
            if not self.active or self.busy.get(node_id) != epoch:
                return
            self._active_match_epoch[node_id] = None
            if roster is None:
                self.engine.emit(
                    "agora_aggregate_finished",
                    node_id,
                    stage=self.stage,
                    epoch=epoch,
                    status="failed",
                    reason="empty_roster",
                    elapsed_seconds=self.engine.now - snapshot_time,
                    expected_component_ids=[],
                    received_component_ids=[],
                    fallback_component_ids=[],
                )
                self.busy[node_id] = None
                return
            self.aggregator.start(node_id, snapshot, roster, self._on_aggregate_done)

        snapshot_time = self.engine.now
        self.matcher.begin(node_id, epoch, global_step, gather, on_roster)

    def _on_aggregate_done(self, node_id: str, epoch: int, result: dict[str, Any]) -> None:
        if self.busy.get(node_id) != epoch:
            return
        self.busy[node_id] = None
        self._active_match_epoch[node_id] = None

    def stop(self) -> None:
        self.active = False
