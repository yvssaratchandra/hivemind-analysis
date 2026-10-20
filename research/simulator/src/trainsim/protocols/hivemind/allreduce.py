"""Chunk-stream model of Hivemind's partitioned all-reduce.

Tensor chunks travel individually from each contributor to their assigned
reducer, then from that reducer to each recipient. Each sender/reducer and
reducer/recipient stream has an inactivity timeout which is reset by every
successfully delivered chunk. The model keeps payloads abstract; only bytes,
timing, provenance, and the resulting per-chunk average are simulated.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from ...types import LocalAggregateResult, Message, Outcome, PartitionReceipt
from ..partitioning import allocate_elements, assign_chunks, chunk_sizes


@dataclass
class _InputStream:
    """One contributor's ordered input stream into one reducer."""

    next_index: int = 0
    deadline: float = 0.0
    timer: object | None = None
    failed: bool = False
    complete: bool = False
    inflight: bool = False


@dataclass
class _Reducer:
    index: int
    chunks: tuple[int, ...]
    started: bool = False
    stopped: bool = False
    streams: dict[str, _InputStream] = field(default_factory=dict)
    pending_inputs: list[Message] = field(default_factory=list)
    received: dict[int, dict[str, str]] = field(default_factory=dict)
    finalized: dict[int, tuple[tuple[str, ...], tuple[float, ...]]] = field(default_factory=dict)
    unavailable: set[int] = field(default_factory=set)


@dataclass
class _Member:
    node_id: str
    work_id: str
    started: bool = False
    started_at: float | None = None
    done: bool = False
    receipts: dict[int, PartitionReceipt] = field(default_factory=dict)
    pending_results: list[Message] = field(default_factory=list)
    output_next: dict[int, int] = field(default_factory=dict)
    output_inflight: set[int] = field(default_factory=set)
    reducer_timers: dict[int, object] = field(default_factory=dict)
    reducer_deadlines: dict[int, float] = field(default_factory=dict)
    allreduce_timer: object | None = None


class GroupAllReduce:
    """Run a group's chunk input, reduction, and output streams."""

    def __init__(self, adapter, roster, attempt_ids=None) -> None:
        self.adapter = adapter
        self.engine = adapter.engine
        self.roster = roster
        self.group_id = roster.group_id
        self.members = tuple(roster.members)
        self.work_by_member = dict(zip(roster.members, roster.work_ids))
        self.attempt_ids = dict(attempt_ids or {})
        self.payload_bytes = int(adapter.config["payload_bytes"])
        self.chunk_bytes = int(adapter.config["chunk_bytes"])
        if self.payload_bytes % 4 or self.chunk_bytes % 4:
            raise ValueError("float32 payload and chunk sizes must be divisible by four")
        if self.chunk_bytes <= 0:
            raise ValueError("chunk_bytes must be positive")
        self.total_elements = self.payload_bytes // 4
        self.chunk_elements = self.chunk_bytes // 4
        self.chunk_size_elements = chunk_sizes(self.total_elements, self.chunk_elements)
        self.chunk_size_bytes = tuple(size * 4 for size in self.chunk_size_elements)
        # Baseline workers report bandwidth=None. Hivemind therefore gives
        # each NODE an equal fraction, independently of actual link capacity.
        fractions = allocate_elements(self.total_elements, [None] * len(self.members))
        chunks_by_reducer = assign_chunks(self.total_elements, self.chunk_elements, fractions)
        self.reducers = {
            node: _Reducer(index, tuple(chunks_by_reducer[index]))
            for index, node in enumerate(self.members)
        }
        self.members_state = {
            node: _Member(node, self.work_by_member[node],
                          output_next={index: 0 for index in range(len(self.members))})
            for node in self.members
        }
        self.closed = False
        self._seen_messages: set[str] = set()

    def start_member(self, node_id: str) -> None:
        """Start local sending, reduction, and recipient streams once."""
        member = self.members_state.get(node_id)
        if member is None or member.started or member.done or self.closed:
            return
        member.started = True
        member.started_at = self.engine.now
        member.allreduce_timer = self.engine.schedule(
            float(self.adapter.config["allreduce_timeout"]), self._allreduce_timeout, node_id
        )
        for reducer_node, reducer in self.reducers.items():
            if not reducer.chunks:
                continue
            index = reducer.index
            self._reset_recipient_timer(member, index)

        # A participant operates its own reducer. Other participants may not
        # have processed their group announcement yet; their streams begin
        # when they start and are bounded by the reducer's sender timeout.
        own_reducer = self.reducers[node_id]
        if own_reducer.chunks:
            self._start_reducer(node_id)

        # Start this contributor's stream at every reducer that is already
        # online. Later reducers will start the same streams from their owner.
        for reducer_node in self.reducers:
            self._send_next_input(node_id, reducer_node)

        pending = list(member.pending_results)
        member.pending_results.clear()
        for message in pending:
            self.result_received(message)
        for reducer_node in self.reducers:
            self._send_next_output(reducer_node, node_id)
        self._maybe_finish_member(member)

    def member_unavailable(self, node_id: str) -> None:
        """Retire a local group member whose work ended before normal output."""
        member = self.members_state.get(node_id)
        if member is None or member.done:
            return
        member.done = True
        self._cancel_member_timers(member)
        reducer = self.reducers.get(node_id)
        if reducer is not None:
            self._stop_reducer(reducer)
        self._maybe_close()

    def input_received(self, message: Message) -> None:
        """Accept one completed input chunk and advance that sender stream."""
        if self.closed or message.group_id != self.group_id:
            return
        if message.message_id in self._seen_messages:
            return
        reducer = self.reducers.get(message.recipient)
        sender = message.sender
        stream = reducer.streams.get(sender) if reducer is not None else None
        if reducer is None or reducer.stopped:
            return
        if not reducer.started:
            if not any(pending.message_id == message.message_id for pending in reducer.pending_inputs):
                reducer.pending_inputs.append(message)
            return
        if stream is None or stream.failed:
            return
        pos = message.body.get("position")
        global_chunk = message.body.get("chunk")
        if (not isinstance(pos, int) or pos != stream.next_index
                or pos < 0 or pos >= len(reducer.chunks)
                or global_chunk != reducer.chunks[pos]
                or message.body.get("work_id") != self.work_by_member.get(sender)
                or message.wire_bytes != self.chunk_size_bytes[global_chunk] + 128):
            self.engine.emit("protocol_rejection", message.recipient,
                             reason="malformed_reduce_input", sender=sender,
                             group_id=self.group_id)
            return
        if self.engine.now >= stream.deadline:
            self._input_timeout(message.recipient, sender)
            return

        self._seen_messages.add(message.message_id)
        reducer.received.setdefault(pos, {})[sender] = self.work_by_member[sender]
        stream.inflight = False
        stream.next_index += 1
        self._cancel_timer(stream.timer)
        stream.timer = None
        if stream.next_index >= len(reducer.chunks):
            stream.complete = True
        else:
            self._reset_input_timer(message.recipient, sender, stream)

        self.engine.emit("reduce_input_chunk", message.recipient,
                         group_id=self.group_id, sender=sender,
                         partition=reducer.index, position=pos,
                         chunk=global_chunk, size_bytes=self.chunk_size_bytes[global_chunk])
        self._advance_reducer(message.recipient)

    def result_received(self, message: Message) -> None:
        """Accept one reducer output chunk at its destination."""
        if self.closed or message.group_id != self.group_id:
            return
        member = self.members_state.get(message.recipient)
        reducer = self.reducers.get(message.sender)
        if member is None or reducer is None or member.done or not reducer.chunks:
            return
        if not member.started:
            member.pending_results.append(message)
            return
        if message.message_id in self._seen_messages:
            return
        self._seen_messages.add(message.message_id)
        index = reducer.index
        if index not in member.output_inflight:
            # Only accept a chunk that this reducer actually sent on the
            # recipient's currently open stream.
            self.engine.emit("protocol_rejection", member.node_id,
                             reason="unsolicited_reduce_result", sender=message.sender,
                             group_id=self.group_id)
            return
        pos = member.output_next[index]
        if pos >= len(reducer.chunks):
            return
        if self.engine.now >= member.reducer_deadlines.get(index, self.engine.now):
            self._recipient_timeout(member.node_id, index)
            return
        expected_chunk = reducer.chunks[pos]
        if (message.body.get("partition") != index
                or message.body.get("position") != pos
                or message.body.get("chunk") != expected_chunk
                or message.wire_bytes != self.chunk_size_bytes[expected_chunk] + 128):
            self.engine.emit("protocol_rejection", member.node_id,
                             reason="malformed_reduce_result", sender=message.sender,
                             group_id=self.group_id)
            return

        contributors = message.body.get("contributors")
        weights = message.body.get("normalized_weights")
        if not isinstance(contributors, list) or not isinstance(weights, list):
            self.engine.emit("protocol_rejection", member.node_id,
                             reason="malformed_reduce_provenance", sender=message.sender,
                             group_id=self.group_id)
            return
        valid_work_ids = set(self.work_by_member.values())
        if (not contributors or any(work_id not in valid_work_ids for work_id in contributors)
                or len(set(contributors)) != len(contributors)
                or len(weights) != len(contributors)
                or any(not isinstance(weight, (int, float)) or weight < 0 for weight in weights)):
            self.engine.emit("protocol_rejection", member.node_id,
                             reason="invalid_reduce_provenance", sender=message.sender,
                             group_id=self.group_id)
            return
        ordered = tuple(work_id for peer in self.members
                        for work_id in (self.work_by_member[peer],)
                        if work_id in contributors)
        if tuple(contributors) != ordered or abs(sum(weights) - 1.0) > 1e-9:
            self.engine.emit("protocol_rejection", member.node_id,
                             reason="invalid_reduce_weights", sender=message.sender,
                             group_id=self.group_id)
            return

        self._cancel_timer(member.reducer_timers.get(index))
        member.receipts[expected_chunk] = PartitionReceipt(
            self.group_id, member.node_id, expected_chunk,
            self.chunk_size_bytes[expected_chunk], ordered,
            tuple(float(weight) for weight in weights), False,
        )
        member.output_next[index] += 1
        member.output_inflight.discard(index)
        self.engine.emit("reduce_result_chunk", member.node_id,
                         group_id=self.group_id, reducer=message.sender,
                         partition=index, position=pos, chunk=expected_chunk,
                         contributors=list(ordered), size_bytes=self.chunk_size_bytes[expected_chunk])

        if member.output_next[index] >= len(reducer.chunks):
            member.reducer_timers.pop(index, None)
            member.reducer_deadlines.pop(index, None)
        else:
            self._reset_recipient_timer(member, index)
            self._send_next_output(message.sender, member.node_id)
        self._maybe_finish_member(member)

    def _start_reducer(self, reducer_node: str) -> None:
        reducer = self.reducers[reducer_node]
        if reducer.started or reducer.stopped or not reducer.chunks:
            return
        reducer.started = True
        for sender in self.members:
            stream = reducer.streams.setdefault(sender, _InputStream())
            if not stream.failed and not stream.complete and stream.timer is None:
                # Native reducer startup waits sender_timeout for missing
                # initial streams, then applies the same inactivity timeout
                # between subsequent chunks.
                self._reset_input_timer(reducer_node, sender, stream)
        pending = list(reducer.pending_inputs)
        reducer.pending_inputs.clear()
        for message in pending:
            self.input_received(message)
        self._advance_reducer(reducer_node)

    def _send_next_input(self, sender: str, reducer_node: str) -> None:
        reducer = self.reducers.get(reducer_node)
        if reducer is None or reducer.stopped or not reducer.chunks:
            return
        stream = reducer.streams.setdefault(sender, _InputStream())
        member = self.members_state.get(sender)
        if (stream is None or stream.failed or stream.complete or stream.inflight
                or member is None or not member.started or member.done):
            return
        pos = stream.next_index
        if pos >= len(reducer.chunks):
            stream.complete = True
            self._cancel_timer(stream.timer)
            stream.timer = None
            self._advance_reducer(reducer_node)
            return
        # Keep at most one chunk of input ahead of the reducer's current
        # ordered position. This bounds buffering without forcing all senders
        # to stop at every chunk boundary.
        cursor = self._reducer_cursor(reducer)
        if pos > cursor + 1:
            return
        chunk = reducer.chunks[pos]
        size_bytes = self.chunk_size_bytes[chunk]
        message = self.adapter.build_message(
            sender, reducer_node, "reduce_input",
            {"work_id": member.work_id, "partition": reducer.index,
             "position": pos, "chunk": chunk, "payload_bytes": size_bytes},
            size_bytes + 128, attempt_id=self._attempt_id(sender), group_id=self.group_id,
        )
        stream.inflight = True
        self.adapter.send_group_message(sender, message)

    def _advance_reducer(self, reducer_node: str) -> None:
        reducer = self.reducers[reducer_node]
        if not reducer.started or reducer.stopped:
            return
        for pos, _chunk in enumerate(reducer.chunks):
            if pos in reducer.finalized or pos in reducer.unavailable:
                continue
            inputs = reducer.received.get(pos, {})
            unresolved = False
            for sender in self.members:
                if sender in inputs:
                    continue
                stream = reducer.streams[sender]
                if not (stream.failed or stream.complete):
                    unresolved = True
                    break
            if unresolved:
                # Ordered input streams mean later chunks cannot overtake this
                # one for a given sender. Continue checking other reducers but
                # keep this partition's output in order.
                break
            contributors = tuple(work_id for peer in self.members
                                 for work_id in (self.work_by_member[peer],)
                                 if inputs.get(peer) == work_id)
            if not contributors:
                # Hivemind cannot normalize an empty reducer input set. No
                # fabricated result is sent; recipient timeout yields local
                # fallback for this and later chunks in the reducer stream.
                self.engine.emit("reducer_chunk_unavailable", reducer_node,
                                 group_id=self.group_id, partition=reducer.index,
                                 position=pos, chunk=reducer.chunks[pos])
                reducer.unavailable.add(pos)
                continue
            weights = tuple(1.0 / len(contributors) for _ in contributors)
            reducer.finalized[pos] = (contributors, weights)
            self.engine.emit("reducer_chunk_finalized", reducer_node,
                             group_id=self.group_id, partition=reducer.index,
                             position=pos, chunk=reducer.chunks[pos],
                             contributor_work_ids=list(contributors))
            for recipient in self.members:
                self._send_next_output(reducer_node, recipient)
        for sender in self.members:
            if self.members_state[sender].started:
                self._send_next_input(sender, reducer_node)

    @staticmethod
    def _reducer_cursor(reducer: _Reducer) -> int:
        for pos in range(len(reducer.chunks)):
            if pos not in reducer.finalized and pos not in reducer.unavailable:
                return pos
        return len(reducer.chunks)

    def _send_next_output(self, reducer_node: str, recipient: str) -> None:
        reducer = self.reducers[reducer_node]
        member = self.members_state[recipient]
        if (self.closed or reducer.stopped or member.done or not member.started
                or not reducer.started or not reducer.chunks):
            return
        index = reducer.index
        if index in member.output_inflight:
            return
        pos = member.output_next[index]
        if pos >= len(reducer.chunks) or pos not in reducer.finalized:
            return
        chunk = reducer.chunks[pos]
        contributors, weights = reducer.finalized[pos]
        size_bytes = self.chunk_size_bytes[chunk]
        message = self.adapter.build_message(
            reducer_node, recipient, "reduce_result",
            {"partition": index, "position": pos, "chunk": chunk,
             "contributors": list(contributors), "normalized_weights": list(weights),
             "payload_bytes": size_bytes},
            size_bytes + 128, attempt_id=self._attempt_id(reducer_node), group_id=self.group_id,
        )
        member.output_inflight.add(index)
        self.adapter.send_group_message(reducer_node, message)

    def _input_timeout(self, reducer_node: str, sender: str) -> None:
        reducer = self.reducers[reducer_node]
        stream = reducer.streams.get(sender)
        if stream is None or stream.failed or stream.complete or reducer.stopped:
            return
        if self.engine.now < stream.deadline:
            return
        stream.failed = True
        stream.inflight = False
        stream.timer = None
        self.engine.emit("sender_stream_timeout", reducer_node, group_id=self.group_id,
                         sender=sender, partition=reducer.index,
                         next_position=stream.next_index)
        self._advance_reducer(reducer_node)

    def _reset_input_timer(self, reducer_node: str, sender: str, stream: _InputStream) -> None:
        self._cancel_timer(stream.timer)
        stream.deadline = self.engine.now + float(self.adapter.config["sender_timeout"])
        stream.timer = self.engine.schedule(
            float(self.adapter.config["sender_timeout"]),
            self._input_timeout, reducer_node, sender,
        )

    def _recipient_timeout(self, node_id: str, reducer_index: int) -> None:
        member = self.members_state.get(node_id)
        if member is None or member.done or not member.started:
            return
        reducer_node = self.members[reducer_index]
        reducer = self.reducers[reducer_node]
        pos = member.output_next[reducer_index]
        if pos >= len(reducer.chunks):
            return
        deadline = member.reducer_deadlines.get(reducer_index, self.engine.now)
        if self.engine.now < deadline:
            return
        self._cancel_timer(member.reducer_timers.pop(reducer_index, None))
        member.reducer_deadlines.pop(reducer_index, None)
        member.output_inflight.discard(reducer_index)
        own_work = self.work_by_member[node_id]
        for remaining_pos in range(pos, len(reducer.chunks)):
            chunk = reducer.chunks[remaining_pos]
            member.receipts.setdefault(chunk, PartitionReceipt(
                self.group_id, node_id, chunk, self.chunk_size_bytes[chunk],
                (own_work,), (1.0,), True,
            ))
        member.output_next[reducer_index] = len(reducer.chunks)
        self.engine.emit("reducer_fallback", node_id, group_id=self.group_id,
                         reducer=reducer_node,
                         chunks=list(reducer.chunks[pos:]))
        self._maybe_finish_member(member)

    def _reset_recipient_timer(self, member: _Member, reducer_index: int) -> None:
        self._cancel_timer(member.reducer_timers.get(reducer_index))
        timeout = float(self.adapter.config["reducer_timeout"])
        member.reducer_deadlines[reducer_index] = self.engine.now + timeout
        member.reducer_timers[reducer_index] = self.engine.schedule(
            timeout, self._recipient_timeout, member.node_id, reducer_index,
        )

    def _allreduce_timeout(self, node_id: str) -> None:
        member = self.members_state.get(node_id)
        if member is None or member.done:
            return
        receipts = tuple(member.receipts[index] for index in sorted(member.receipts))
        result = LocalAggregateResult(node_id, self.group_id, Outcome.FAILED,
                                      receipts, self.payload_bytes, self.engine.now)
        member.done = True
        self._cancel_member_timers(member)
        self._stop_reducer(self.reducers[node_id])
        self.adapter._group_member_failed(node_id, result)
        self._maybe_close()

    def _maybe_finish_member(self, member: _Member) -> None:
        if member.done or len(member.receipts) != len(self.chunk_size_bytes):
            return
        member.done = True
        self._cancel_member_timers(member)
        receipts = tuple(member.receipts[index] for index in range(len(self.chunk_size_bytes)))
        outcome = Outcome.PARTIAL if any(receipt.fallback for receipt in receipts) else Outcome.COMPLETE
        result = LocalAggregateResult(member.node_id, self.group_id, outcome,
                                      receipts, self.payload_bytes, self.engine.now)
        self.engine.emit("allreduce_member_complete", member.node_id, group_id=self.group_id,
                         status=outcome.value, receipt_chunks=len(receipts))
        self.adapter._group_member_complete(member.node_id, outcome, result)
        self._maybe_close()

    def _cancel_member_timers(self, member: _Member) -> None:
        self._cancel_timer(member.allreduce_timer)
        member.allreduce_timer = None
        for timer in member.reducer_timers.values():
            self._cancel_timer(timer)
        member.reducer_timers.clear()
        member.reducer_deadlines.clear()

    def _stop_reducer(self, reducer: _Reducer) -> None:
        if reducer.stopped:
            return
        reducer.stopped = True
        for stream in reducer.streams.values():
            self._cancel_timer(stream.timer)
            stream.timer = None

    def _attempt_id(self, node_id: str) -> str:
        return self.attempt_ids.get(node_id, "")

    def _maybe_close(self) -> None:
        if all(member.done for member in self.members_state.values()):
            self.closed = True
            self.adapter.groups.pop(self.group_id, None)

    @staticmethod
    def _cancel_timer(timer) -> None:
        if timer is not None:
            timer.cancel()
