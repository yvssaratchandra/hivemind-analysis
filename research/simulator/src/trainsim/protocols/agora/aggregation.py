"""Network-driven sparse all-reduce with peer-local rosters.

This module models the pinned stream's group-ID and sender-membership checks,
part ordering, timeout behavior, and reducer fallback. The transport uses one
simulated request per tensor part and a three-part sender window. That window
represents bounded streaming; it is not an assertion about gRPC's exact wire
buffering. Numerical tensor values and compression CPU cost are not simulated.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ...network import Network
from ...types import Message, TimerHandle
from .partitioning import assign_tensor_parts, reducer_allocations, reducer_fractions
from .workload import SparseSnapshot


# Literal group ID used by pinned Agora Matchmaking. Epoch is not part of it.
NATIVE_GROUP_ID = bytes.fromhex("4f5b9a55cf25f028904e71df218b8529260ce972").hex()
PART_HEADER_BYTES = 64
STREAM_WINDOW_PARTS = 3


@dataclass(frozen=True)
class TensorPart:
    tensor_index: int
    chunk_index: int
    owner_index: int
    elements: int

    @property
    def component_id(self) -> str:
        return f"t{self.tensor_index}:c{self.chunk_index}"

    @property
    def wire_bytes(self) -> int:
        return self.elements * 4 + PART_HEADER_BYTES


@dataclass
class _PartOutput:
    contributors: dict[str, dict[str, Any]]
    fallback: bool
    reducer_node: str
    reducer_epoch: int
    reducer_component_id: str
    receipt_id: str = ""


@dataclass
class _Incoming:
    source_run: "_AverageRun"
    stream: "_OutboundStream"
    source_part: TensorPart
    stream_index: int
    destination_part: TensorPart


@dataclass
class _OutboundStream:
    stream_id: str
    source_run: "_AverageRun"
    destination: str
    parts: list[TensorPart]
    next_to_send: int = 0
    in_flight: int = 0
    failed: bool = False
    closed: bool = False
    sent_count: int = 0
    received_count: int = 0
    delay_results: bool = False
    pending_results: list[tuple[_Incoming, _PartOutput]] = field(default_factory=list)
    timer: TimerHandle | None = None
    timeout_phase: str = "sender_timeout"
    inputs_closed: bool = False
    results_received: int = 0


@dataclass
class _AverageRun:
    node_id: str
    snapshot: SparseSnapshot
    roster: dict[str, Any]
    node_ids: tuple[str, ...]
    peer_ids: tuple[str, ...]
    roles: tuple[str, ...]
    weights: tuple[float, ...]
    parts: tuple[TensorPart, ...]
    parts_by_owner: tuple[tuple[TensorPart, ...], ...]
    self_index: int
    expected_component_ids: tuple[str, ...]
    started_at: float
    on_done: Any = None
    outputs: dict[str, _PartOutput] = field(default_factory=dict)
    inputs: dict[str, dict[str, _Incoming]] = field(default_factory=dict)
    streams: dict[str, _OutboundStream] = field(default_factory=dict)
    delayed_input_streams: set[str] = field(default_factory=set)
    sender_timers: dict[str, TimerHandle] = field(default_factory=dict)
    banned_senders: set[str] = field(default_factory=set)
    sent_part_count: dict[str, int] = field(default_factory=dict)
    active: bool = True
    allreduce_timer: TimerHandle | None = None

    @property
    def own_parts(self) -> tuple[TensorPart, ...]:
        return self.parts_by_owner[self.self_index]


@dataclass
class _PendingInput:
    message: Message
    stream: _OutboundStream
    source_part: TensorPart
    stream_index: int
    expires_at: float
    timer: TimerHandle


class AgoraAggregator:
    """Run independent local stream schedules against each receiver's active run."""

    def __init__(self, engine, network: Network, profiles: dict, config: dict[str, Any]) -> None:
        self.engine, self.network, self.profiles, self.config = engine, network, profiles, config
        self.active: dict[str, _AverageRun] = {}
        self._runs: dict[tuple[str, int], _AverageRun] = {}
        self._pending: dict[str, list[_PendingInput]] = {}
        self._stream_counter = 0
        self._stopped = False

    def start(self, node_id: str, snapshot: SparseSnapshot, roster: dict[str, Any], on_done) -> None:
        """Start one local average using only this node's gathered roster."""
        node_ids = tuple(roster["ordered_node_ids"])
        peer_ids = tuple(roster["ordered_peer_ids"])
        gathered = tuple(roster["gathered"])
        if len(node_ids) != len(peer_ids) or len(node_ids) != len(gathered):
            raise ValueError("local roster peer IDs, node IDs, and gathered metadata differ in length")
        roles = tuple(str(record.get("role", "CLIENT")) for record in gathered)
        weights = tuple(float(record.get("weight", 1.0)) for record in gathered)
        if node_id not in node_ids:
            self._emit_local_failure(node_id, snapshot, roster, "self_missing_from_local_roster", ())
            on_done(node_id, snapshot.epoch, {"status": "failed"})
            return
        self_index = node_ids.index(node_id)
        if roles.count("NODE") == 0:
            self._emit_local_failure(node_id, snapshot, roster, "no_reducer_in_local_roster", ())
            on_done(node_id, snapshot.epoch, {"status": "failed"})
            return
        dense_total = sum(self.config.get("tensor_elements", snapshot.tensor_cardinalities))
        allocations = reducer_allocations(dense_total, roles)
        # Native load-balance counts the dense parameter vector first. The
        # compact sparse tensors are then partitioned using those proportions.
        fractions = tuple(value / dense_total for value in allocations)
        raw_parts = assign_tensor_parts(
            snapshot.tensor_cardinalities,
            allocations,
            int(self.config["chunk_bytes"]),
            element_bytes=4,
        )
        parts = tuple(TensorPart(*item) for item in raw_parts)
        parts_by_owner = tuple(
            tuple(part for part in parts if part.owner_index == index)
            for index in range(len(node_ids))
        )
        expected = tuple(part.component_id for part in parts)
        run = _AverageRun(
            node_id=node_id,
            snapshot=snapshot,
            roster=roster,
            node_ids=node_ids,
            peer_ids=peer_ids,
            roles=roles,
            weights=weights,
            parts=parts,
            parts_by_owner=parts_by_owner,
            self_index=self_index,
            expected_component_ids=expected,
            started_at=self.engine.now,
            on_done=on_done,
        )
        run.inputs = {part.component_id: {} for part in run.own_parts}
        self._runs[(node_id, snapshot.epoch)] = run
        # The wire group ID is constant in pinned Agora. A second active run
        # replaces the local dispatch slot, matching its group-ID keyed table.
        self.active[node_id] = run
        run.allreduce_timer = self.engine.schedule(
            float(self.config["allreduce_timeout"]), self._on_allreduce_timeout, run
        )
        if run.own_parts:
            for sender in run.node_ids:
                if sender != node_id:
                    self._reset_sender_timer(run, sender)

        self.engine.emit(
            "agora_local_assignment",
            node_id,
            stage=roster["stage"],
            epoch=snapshot.epoch,
            roster_fingerprint=roster["roster_fingerprint"],
            ordered_node_ids=list(node_ids),
            roles=list(roles),
            reducer_fractions=list(fractions),
            reducer_allocations=list(allocations),
            expected_component_ids=list(expected),
            owned_component_ids=[part.component_id for part in run.own_parts],
        )
        self.engine.emit(
            "agora_aggregate_started",
            node_id,
            stage=roster["stage"],
            epoch=snapshot.epoch,
            global_step=snapshot.global_step,
            role=snapshot.role,
            weight=snapshot.weight,
            layout=snapshot.layout,
            partition=snapshot.partition,
            selected_cardinality=snapshot.selected_cardinality,
            snapshot_id=snapshot.snapshot_id,
            expected_component_ids=list(expected),
            expected_component_cardinalities=self._component_cardinalities(parts),
            local_roster_fingerprint=roster["roster_fingerprint"],
        )

        # A NODE contributes its local input to its own assigned parts without
        # a network transfer, matching AllReduceRunner's self communication.
        if roles[self_index] == "NODE":
            for part in run.own_parts:
                incoming = _Incoming(run, _OutboundStream("local", run, node_id, [part]), part, 0, part)
                run.inputs[part.component_id][node_id] = incoming
                run.sent_part_count[node_id] = run.sent_part_count.get(node_id, 0) + 1

        # Process messages that arrived before this local group was registered.
        pending = self._pending.pop(node_id, [])
        for item in pending:
            item.timer.cancel()
            if item.expires_at > self.engine.now:
                self._accept_input(item.message, item.stream, item.source_part, item.stream_index)
            else:
                self._fail_stream(item.stream, "sender_timeout")

        # Each sender opens independent streams only to peers owning its parts.
        for owner_index, destination in enumerate(node_ids):
            if destination == node_id:
                continue
            owned = list(parts_by_owner[owner_index])
            if not owned:
                continue
            self._stream_counter += 1
            stream = _OutboundStream(
                stream_id=f"agora-stream-{self._stream_counter}",
                source_run=run,
                destination=destination,
                parts=owned,
                delay_results=roles[self_index] == "CLIENT",
            )
            run.streams[destination] = stream
            self._pump_stream(stream)

        # The local reducer's part zero may already be complete when this is a
        # one-node group; otherwise inputs/timeout callbacks advance it.
        self._try_complete_owned_parts(run)
        self._try_finish(run, on_done)

    def _pump_stream(self, stream: _OutboundStream) -> None:
        if self._stopped or stream.failed or stream.closed:
            return
        window = int(self.config.get("stream_window_parts", STREAM_WINDOW_PARTS))
        while stream.in_flight < window and stream.next_to_send < len(stream.parts):
            index = stream.next_to_send
            part = stream.parts[index]
            stream.next_to_send += 1
            stream.sent_count += 1
            stream.in_flight += 1
            body = {
                "first_part": index == 0,
                "tensor_index": part.tensor_index,
                "chunk_index": part.chunk_index,
                "elements": part.elements,
                "stream_index": index,
            }
            message = Message(
                message_id=f"{stream.stream_id}:part:{index}",
                sender=stream.source_run.node_id,
                recipient=stream.destination,
                kind="agora_reduce_input",
                body=body,
                wire_bytes=part.wire_bytes,
                group_id=NATIVE_GROUP_ID,
            )
            self.engine.emit(
                "agora_stream_part_sent",
                stream.source_run.node_id,
                stream_id=stream.stream_id,
                destination=stream.destination,
                snapshot_id=stream.source_run.snapshot.snapshot_id,
                epoch=stream.source_run.snapshot.epoch,
                component_id=part.component_id,
                stream_index=index,
                tensor_index=part.tensor_index,
                chunk_index=part.chunk_index,
                elements=part.elements,
                wire_bytes=part.wire_bytes,
                window_parts=window,
            )
            if not stream.delay_results:
                self._reset_stream_timer(stream, "reducer_timeout")
            self.network.send(
                message,
                lambda delivered, stream=stream, part=part, index=index: self._receive_input(
                    delivered, stream, part, index
                ),
            )

        if stream.next_to_send == len(stream.parts) and stream.in_flight == 0:
            self._close_stream(stream)

    def _receive_input(
        self, message: Message, stream: _OutboundStream, source_part: TensorPart, stream_index: int
    ) -> None:
        if stream.failed or not stream.source_run.active:
            return
        destination_run = self.active.get(message.recipient)
        if destination_run is None or not destination_run.active:
            expires_at = self.engine.now + float(self.config["sender_timeout"])
            timer = self.engine.call_at(expires_at, self._expire_pending, stream)
            self._pending.setdefault(message.recipient, []).append(
                _PendingInput(message, stream, source_part, stream_index, expires_at, timer)
            )
            return
        self._accept_input(message, stream, source_part, stream_index)

    def _accept_input(
        self, message: Message, stream: _OutboundStream, source_part: TensorPart, stream_index: int
    ) -> None:
        destination_run = self.active.get(message.recipient)
        if destination_run is None or not destination_run.active or stream.failed:
            return
        sender = message.sender
        # Native dispatch uses its constant group ID and the receiver's current
        # local sender roster. Epoch is audit-only and is deliberately ignored.
        if stream_index == 0 and message.group_id != NATIVE_GROUP_ID:
            self._fail_stream(stream, "bad_group_id")
            return
        if sender not in destination_run.node_ids:
            self.engine.emit(
                "agora_protocol_rejection",
                message.recipient,
                sender=sender,
                receiver_epoch=destination_run.snapshot.epoch,
                source_epoch=stream.source_run.snapshot.epoch,
                reason="sender_not_in_local_roster",
                group_id_checked=stream_index == 0,
            )
            self._ban_sender(destination_run, sender, "protocol_violation")
            self._fail_stream(stream, "protocol_violation")
            return
        if sender in destination_run.banned_senders:
            self._fail_stream(stream, "sender_already_banned")
            return
        owned = destination_run.own_parts
        if stream_index >= len(owned):
            self.engine.emit(
                "agora_protocol_rejection",
                message.recipient,
                sender=sender,
                receiver_epoch=destination_run.snapshot.epoch,
                source_epoch=stream.source_run.snapshot.epoch,
                reason="too_many_parts_for_local_reducer",
                stream_index=stream_index,
            )
            self._ban_sender(destination_run, sender, "part_count_mismatch")
            self._fail_stream(stream, "part_count_mismatch")
            return
        destination_part = owned[stream_index]
        # Native accumulation fails on incompatible tensor part shapes. It does
        # not check the epoch or sparse partition ID on the wire.
        if source_part.elements != destination_part.elements:
            self.engine.emit(
                "agora_protocol_rejection",
                message.recipient,
                sender=sender,
                receiver_epoch=destination_run.snapshot.epoch,
                source_epoch=stream.source_run.snapshot.epoch,
                reason="part_shape_mismatch",
                source_elements=source_part.elements,
                receiver_elements=destination_part.elements,
                source_tensor=source_part.tensor_index,
                receiver_tensor=destination_part.tensor_index,
            )
            self._ban_sender(destination_run, sender, "part_shape_mismatch")
            self._fail_stream(stream, "part_shape_mismatch")
            return

        part_id = destination_part.component_id
        incoming = _Incoming(stream.source_run, stream, source_part, stream_index, destination_part)
        by_origin = destination_run.inputs.setdefault(part_id, {})
        if sender not in by_origin:
            by_origin[sender] = incoming
            destination_run.sent_part_count[sender] = destination_run.sent_part_count.get(sender, 0) + 1
            if stream.delay_results:
                destination_run.delayed_input_streams.add(stream.stream_id)
            self._reset_sender_timer(destination_run, sender)
        self.engine.emit(
            "agora_part_received",
            message.recipient,
            sender=sender,
            stream_id=stream.stream_id,
            source_snapshot_id=stream.source_run.snapshot.snapshot_id,
            source_epoch=stream.source_run.snapshot.epoch,
            receiver_epoch=destination_run.snapshot.epoch,
            component_id=part_id,
            source_component_id=source_part.component_id,
            stream_index=stream_index,
            elements=source_part.elements,
            weight=stream.source_run.snapshot.weight,
        )
        if stream.delay_results:
            # Native waits until a CLIENT has delivered its full input stream
            # before returning averaged chunks; input delivery frees this slot.
            stream.received_count += 1
            stream.in_flight = max(0, stream.in_flight - 1)
            self._pump_stream(stream)
        self._try_complete_owned_parts(destination_run)

    def _try_complete_owned_parts(self, run: _AverageRun) -> None:
        if not run.active or not run.own_parts:
            return
        expected_senders = set(run.node_ids)
        for part in run.own_parts:
            part_id = part.component_id
            by_origin = run.inputs.setdefault(part_id, {})
            if part_id in run.outputs:
                continue
            if not expected_senders.issubset(set(by_origin) | run.banned_senders):
                continue
            contributors = {
                origin: self._snapshot_evidence(
                    incoming.source_run.snapshot,
                    incoming.source_part.component_id,
                    incoming.source_part.elements,
                )
                for origin, incoming in by_origin.items()
            }
            receipt_id = (
                f"{run.node_id}:e{run.snapshot.epoch}:{run.snapshot.layout[:12]}:"
                f"p{run.snapshot.partition}:{part_id}"
            )
            output = _PartOutput(
                contributors=contributors,
                fallback=False,
                reducer_node=run.node_id,
                reducer_epoch=run.snapshot.epoch,
                reducer_component_id=part_id,
                receipt_id=receipt_id,
            )
            self._record_output(run, part, output)
            self.engine.emit(
                "agora_sparse_part_receipt",
                run.node_id,
                stage=run.roster["stage"],
                epoch=run.snapshot.epoch,
                layout=run.snapshot.layout,
                partition=run.snapshot.partition,
                selected_cardinality=run.snapshot.selected_cardinality,
                component_id=part_id,
                receipt_id=receipt_id,
                tensor_index=part.tensor_index,
                chunk_index=part.chunk_index,
                part_cardinality=part.elements,
                expected_senders=len(expected_senders),
                contributors=list(contributors.values()),
                contributor_ids=sorted(contributors),
                missing_senders=sorted(expected_senders - set(by_origin)),
                contributor_weights={key: value["weight"] for key, value in contributors.items()},
                status="complete" if len(by_origin) == len(expected_senders) else "partial",
                fallback=False,
                reducer_node=run.node_id,
            )
            for incoming in list(by_origin.values()):
                if incoming.source_run.node_id == run.node_id:
                    continue
                self._send_result(incoming, output)
        self._try_finish(run)

    def _send_result(self, incoming: _Incoming, output: _PartOutput) -> None:
        stream = incoming.stream
        if not incoming.source_run.active:
            return
        if stream.delay_results and not stream.inputs_closed:
            stream.pending_results.append((incoming, output))
            return
        result = Message(
            message_id=f"{stream.stream_id}:result:{incoming.stream_index}",
            sender=output.reducer_node,
            recipient=incoming.source_run.node_id,
            kind="agora_reduce_result",
            body={
                "stream_index": incoming.stream_index,
                "elements": incoming.source_part.elements,
                "success": True,
            },
            wire_bytes=incoming.source_part.wire_bytes,
            group_id=NATIVE_GROUP_ID,
        )
        self.network.send(
            result,
            lambda delivered, incoming=incoming, output=output: self._receive_result(
                delivered, incoming, output
            ),
        )

    def _receive_result(self, message: Message, incoming: _Incoming, output: _PartOutput) -> None:
        stream = incoming.stream
        if not incoming.source_run.active:
            return
        run = incoming.source_run
        source_part = incoming.source_part
        existing = run.outputs.get(source_part.component_id)
        if existing is not None and not existing.fallback:
            return
        run.outputs[source_part.component_id] = output
        stream.in_flight = max(0, stream.in_flight - 1)
        stream.results_received += 1
        stream.received_count = stream.results_received
        self.engine.emit(
            "agora_part_result_received",
            run.node_id,
            stream_id=stream.stream_id,
            reducer=message.sender,
            snapshot_id=run.snapshot.snapshot_id,
            recipient_epoch=run.snapshot.epoch,
            reducer_epoch=output.reducer_epoch,
            component_id=source_part.component_id,
            source_component_id=source_part.component_id,
            reducer_component_id=output.reducer_component_id,
            receipt_id=output.receipt_id,
            elements=source_part.elements,
            contributor_ids=sorted(output.contributors),
        )
        if stream.results_received >= len(stream.parts):
            if stream.timer is not None:
                stream.timer.cancel()
                stream.timer = None
        else:
            self._reset_stream_timer(stream, "reducer_timeout")
        self._pump_stream(stream)
        self._try_finish(run)

    def _close_stream(self, stream: _OutboundStream) -> None:
        if stream.closed or stream.failed:
            return
        stream.closed = True
        if stream.delay_results:
            self._reset_stream_timer(stream, "reducer_timeout")
        elif stream.timer is not None:
            stream.timer.cancel()
            stream.timer = None
        message = Message(
            message_id=f"{stream.stream_id}:close",
            sender=stream.source_run.node_id,
            recipient=stream.destination,
            kind="agora_reduce_close",
            body={"sent_parts": stream.sent_count},
            wire_bytes=32,
            group_id=NATIVE_GROUP_ID,
        )
        self.network.send(message, lambda delivered, stream=stream: self._receive_close(delivered, stream))

    def _receive_close(self, message: Message, stream: _OutboundStream) -> None:
        run = self.active.get(message.recipient)
        stream.inputs_closed = True
        pending, stream.pending_results = stream.pending_results, []
        for incoming, output in pending:
            self._send_result(incoming, output)
        if run is not None:
            run.delayed_input_streams.discard(stream.stream_id)
        if run is None or not run.active:
            return
        sender = message.sender
        self._cancel_sender_timer(run, sender)
        expected = len(run.own_parts)
        received = run.sent_part_count.get(sender, 0)
        if received < expected:
            self._ban_sender(run, sender, "stream_closed_early")
        self._try_complete_owned_parts(run)

    def _expire_pending(self, stream: _OutboundStream) -> None:
        if stream.failed or not stream.source_run.active:
            return
        if self.active.get(stream.destination) is not None:
            return
        self._fail_stream(stream, "no_active_group_timeout")

    def _ban_sender(self, run: _AverageRun, sender: str, reason: str) -> None:
        if sender in run.banned_senders:
            return
        run.banned_senders.add(sender)
        self._cancel_sender_timer(run, sender)
        delayed_streams = {
            incoming.stream.stream_id: incoming.stream
            for by_origin in run.inputs.values()
            for origin, incoming in by_origin.items()
            if origin == sender and incoming.stream.delay_results
        }
        for stream in delayed_streams.values():
            # A failed CLIENT cannot send the close marker that normally frees
            # its buffered responses. Release already accepted parts now, then
            # ignore any later inputs from this banned stream.
            run.delayed_input_streams.discard(stream.stream_id)
            stream.inputs_closed = True
            pending, stream.pending_results = stream.pending_results, []
            for incoming, output in pending:
                self._send_result(incoming, output)
        self.engine.emit(
            "agora_sender_banned",
            run.node_id,
            sender=sender,
            epoch=run.snapshot.epoch,
            reason=reason,
            parts_received=run.sent_part_count.get(sender, 0),
        )
        self._try_complete_owned_parts(run)

    def _fail_stream(self, stream: _OutboundStream, reason: str) -> None:
        if stream.failed:
            return
        stream.failed = True
        if stream.timer is not None:
            stream.timer.cancel()
            stream.timer = None
        run = stream.source_run
        for part_index, part in enumerate(stream.parts):
            # Already received results are retained. Unsent or in-flight parts
            # without a completed result fall back to a zero delta.
            if part_index >= stream.results_received and part.component_id not in run.outputs:
                run.outputs[part.component_id] = _PartOutput(
                    contributors={},
                    fallback=True,
                    reducer_node=stream.destination,
                    reducer_epoch=-1,
                    reducer_component_id=part.component_id,
                )
                self.engine.emit(
                    "agora_reducer_fallback",
                    run.node_id,
                    stage=run.roster["stage"],
                    epoch=run.snapshot.epoch,
                    snapshot_id=run.snapshot.snapshot_id,
                    reducer=stream.destination,
                    component_id=part.component_id,
                    part_cardinality=part.elements,
                    reason=reason,
                    fallback="local_delta_zero",
                )
        destination_run = self.active.get(stream.destination)
        if destination_run is not None and destination_run.active:
            destination_run.delayed_input_streams.discard(stream.stream_id)
            self._ban_sender(destination_run, run.node_id, reason)
        self._try_finish(run)

    def _reset_stream_timer(self, stream: _OutboundStream, phase: str) -> None:
        if stream.timer is not None:
            stream.timer.cancel()
        stream.timeout_phase = phase
        duration = float(self.config[phase])
        stream.timer = self.engine.schedule(duration, self._on_stream_timeout, stream, phase)

    def _reset_sender_timer(self, run: _AverageRun, sender: str) -> None:
        self._cancel_sender_timer(run, sender)
        expected = len(run.own_parts)
        if expected <= 0 or run.sent_part_count.get(sender, 0) >= expected:
            return
        run.sender_timers[sender] = self.engine.schedule(
            float(self.config["sender_timeout"]), self._on_missing_sender, run, sender
        )

    @staticmethod
    def _cancel_sender_timer(run: _AverageRun, sender: str) -> None:
        timer = run.sender_timers.pop(sender, None)
        if timer is not None:
            timer.cancel()

    def _on_missing_sender(self, run: _AverageRun, sender: str) -> None:
        run.sender_timers.pop(sender, None)
        if run.active and run.sent_part_count.get(sender, 0) < len(run.own_parts):
            self._ban_sender(run, sender, "sender_timeout")

    def _on_stream_timeout(self, stream: _OutboundStream, phase: str) -> None:
        if not stream.source_run.active or stream.failed:
            return
        # A CLIENT can finish its input stream yet still wait for reducer
        # results; the timeout phase tracks which leg made no progress.
        if stream.timeout_phase != phase:
            return
        self._fail_stream(stream, phase)

    def _on_allreduce_timeout(self, run: _AverageRun) -> None:
        if not run.active:
            return
        self._fill_fallbacks(run, "allreduce_timeout")
        self._try_finish(run)

    def _fill_fallbacks(self, run: _AverageRun, reason: str) -> None:
        for part in run.parts:
            if part.component_id in run.outputs:
                continue
            output = _PartOutput({}, True, "", -1, part.component_id)
            run.outputs[part.component_id] = output
            self.engine.emit(
                "agora_reducer_fallback",
                run.node_id,
                stage=run.roster["stage"],
                epoch=run.snapshot.epoch,
                snapshot_id=run.snapshot.snapshot_id,
                reducer="",
                component_id=part.component_id,
                part_cardinality=part.elements,
                reason=reason,
                fallback="local_delta_zero",
            )

    def _record_output(self, run: _AverageRun, part: TensorPart, output: _PartOutput) -> None:
        """Record the reducer's local result for its own recipient."""
        run.outputs[part.component_id] = output

    def _emit_local_failure(
        self,
        node_id: str,
        snapshot: SparseSnapshot,
        roster: dict[str, Any],
        reason: str,
        expected: tuple[str, ...],
    ) -> None:
        self.engine.emit(
            "agora_aggregate_finished",
            node_id,
            stage=roster.get("stage", self.config.get("stage", "")),
            epoch=snapshot.epoch,
            status="failed",
            reason=reason,
            elapsed_seconds=0.0,
            expected_component_ids=list(expected),
            received_component_ids=[],
            fallback_component_ids=list(expected),
        )

    @staticmethod
    def _component_cardinalities(parts: tuple[TensorPart, ...]) -> dict[str, int]:
        return {part.component_id: part.elements for part in parts}

    def _try_finish(self, run: _AverageRun, on_done=None) -> None:
        if not run.active:
            return
        if not set(run.expected_component_ids).issubset(run.outputs):
            return
        if run.delayed_input_streams:
            # Hold the reducer service slot until every delayed CLIENT input
            # stream has closed and its buffered outputs have been released.
            return
        run.active = False
        if self.active.get(run.node_id) is run:
            self.active.pop(run.node_id, None)
        if run.allreduce_timer is not None:
            run.allreduce_timer.cancel()
        for timer in run.sender_timers.values():
            timer.cancel()
        run.sender_timers.clear()
        for stream in run.streams.values():
            if stream.timer is not None:
                stream.timer.cancel()
                stream.timer = None
        received = sorted(
            component for component, output in run.outputs.items() if not output.fallback
        )
        fallbacks = sorted(
            component for component, output in run.outputs.items() if output.fallback
        )
        status = "complete" if not fallbacks else ("partial" if received else "failed")
        self._complete_applications(run, status=status)
        self.engine.emit(
            "agora_aggregate_finished",
            run.node_id,
            stage=run.roster["stage"],
            epoch=run.snapshot.epoch,
            status=status,
            elapsed_seconds=self.engine.now - run.started_at,
            expected_component_ids=list(run.expected_component_ids),
            received_component_ids=received,
            fallback_component_ids=fallbacks,
        )
        callback = on_done or run.on_done
        if callback is not None:
            callback(run.node_id, run.snapshot.epoch, {"status": status})

    def _complete_applications(self, run: _AverageRun, *, status: str) -> None:
        origin_data: dict[str, dict[str, Any]] = {}
        for node_id, record in zip(run.node_ids, run.roster["gathered"]):
            origin_data[node_id] = dict(record)
        origin_data.setdefault(run.node_id, run.snapshot.as_gather())

        # Include any sender evidence actually incorporated by a reducer. This
        # preserves asymmetric views without inventing a canonical roster.
        for output in run.outputs.values():
            for origin, evidence in output.contributors.items():
                origin_data.setdefault(origin, dict(evidence))

        received_ids = sorted(
            component for component, output in run.outputs.items() if not output.fallback
        )
        fallback_ids = sorted(
            component for component, output in run.outputs.items() if output.fallback
        )
        part_by_id = {part.component_id: part for part in run.parts}
        expected_cardinalities = {
            component: part.elements for component, part in part_by_id.items()
        }
        for origin, record in sorted(origin_data.items()):
            snapshot_id = str(record.get("snapshot_id", f"{origin}:unknown"))
            origin_epoch = int(record.get("epoch", run.snapshot.epoch))
            layout = str(record.get("layout", run.snapshot.layout))
            partition = int(record.get("partition", run.snapshot.partition))
            selected = int(record.get("selected_cardinality", run.snapshot.selected_cardinality))
            weight = float(record.get("weight", 1.0))
            applied_components = []
            applied_component_cardinalities: dict[str, int] = {}
            component_evidence = []
            for component_id in run.expected_component_ids:
                output = run.outputs.get(component_id)
                contributor = output.contributors.get(origin) if output and not output.fallback else None
                intended = origin_data.get(origin, {})
                identity_matches = bool(
                    contributor
                    and str(contributor.get("snapshot_id", "")) == str(intended.get("snapshot_id", ""))
                    and int(contributor.get("epoch", -1)) == int(intended.get("epoch", -2))
                    and str(contributor.get("layout", "")) == str(intended.get("layout", ""))
                    and int(contributor.get("partition", -1)) == int(intended.get("partition", -2))
                    and int(contributor.get("epoch", -1)) == run.snapshot.epoch
                    and str(contributor.get("layout", "")) == run.snapshot.layout
                    and int(contributor.get("partition", -1)) == run.snapshot.partition
                    and contributor.get("source_component_id") == component_id
                    and int(contributor.get("source_component_cardinality", -1))
                    == expected_cardinalities.get(component_id, -2)
                    and output.reducer_component_id == component_id
                )
                included = bool(identity_matches)
                if included:
                    applied_components.append(component_id)
                    applied_component_cardinalities[component_id] = expected_cardinalities[component_id]
                component_evidence.append({
                    "component_id": component_id,
                    "applied": included,
                    "fallback": bool(output is None or output.fallback),
                    "reducer": output.reducer_node if output else "",
                    "reducer_epoch": output.reducer_epoch if output else -1,
                    "reducer_component_id": output.reducer_component_id if output else component_id,
                    "receipt_id": output.receipt_id if output else "",
                    "source_component_id": (
                        contributor.get("source_component_id", "") if contributor else ""
                    ),
                    "contributor_snapshot_id": (
                        contributor.get("snapshot_id", "") if contributor else ""
                    ),
                    "contributor_epoch": contributor.get("epoch", -1) if contributor else -1,
                    "contributor_layout": contributor.get("layout", "") if contributor else "",
                    "contributor_partition": contributor.get("partition", -1) if contributor else -1,
                    "contributor_cardinality": (
                        contributor.get("source_component_cardinality", 0) if contributor else 0
                    ),
                })
            applied_cardinality = sum(applied_component_cardinalities.values())
            if (
                set(applied_components) == set(run.expected_component_ids)
                and applied_cardinality == selected
            ):
                app_status = "complete"
            elif applied_components:
                app_status = "partial"
            else:
                app_status = "failed"
            self.engine.emit(
                "agora_sparse_application",
                run.node_id,
                stage=run.roster["stage"],
                origin=origin,
                recipient=run.node_id,
                snapshot_id=snapshot_id,
                epoch=origin_epoch,
                recipient_epoch=run.snapshot.epoch,
                layout=layout,
                partition=partition,
                selected_cardinality=selected,
                applied_cardinality=applied_cardinality,
                status=app_status,
                fallback=bool(fallback_ids),
                weight=weight,
                expected_component_ids=list(run.expected_component_ids),
                expected_component_cardinalities=expected_cardinalities,
                received_component_ids=received_ids,
                applied_component_ids=applied_components,
                applied_component_cardinalities=applied_component_cardinalities,
                fallback_component_ids=fallback_ids,
                component_evidence=component_evidence,
                aggregation_status=status,
            )

    @staticmethod
    def _snapshot_evidence(
        snapshot: SparseSnapshot, component_id: str = "", cardinality: int = 0
    ) -> dict[str, Any]:
        return {
            "node_id": snapshot.node_id,
            "snapshot_id": snapshot.snapshot_id,
            "epoch": snapshot.epoch,
            "layout": snapshot.layout,
            "partition": snapshot.partition,
            "selected_cardinality": snapshot.selected_cardinality,
            "weight": snapshot.weight,
            "source_component_id": component_id,
            "source_component_cardinality": cardinality,
        }

    def stop(self) -> None:
        self._stopped = True
        for run in self._runs.values():
            run.active = False
            if run.allreduce_timer is not None:
                run.allreduce_timer.cancel()
            for timer in run.sender_timers.values():
                timer.cancel()
            run.sender_timers.clear()
            for stream in run.streams.values():
                if stream.timer is not None:
                    stream.timer.cancel()
