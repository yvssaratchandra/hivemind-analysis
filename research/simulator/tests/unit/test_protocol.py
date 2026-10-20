from types import SimpleNamespace

from trainsim.engine import Engine
from trainsim.network import Network
from trainsim.protocols.hivemind.allreduce import GroupAllReduce
from trainsim.types import GroupRoster, Message, NodeProfile, Outcome


class _AllReduceFixture:
    """Small network-backed protocol harness for chunk-stream tests."""

    def __init__(self, *, sender_timeout=0.5, reducer_timeout=1.0,
                 input_delays=None, result_delays=None):
        self.engine = Engine(seed=7)
        self.profiles = {
            node: NodeProfile(node, node.encode() * 32, 0.0,
                              1e9, 1e9, 0.0, "")
            for node in ("A", "B")
        }
        self.network = Network(self.engine, self.profiles)
        self.config = {
            "payload_bytes": 16,
            "chunk_bytes": 4,
            "allreduce_timeout": 5.0,
            "sender_timeout": sender_timeout,
            "reducer_timeout": reducer_timeout,
        }
        self.states = {
            node: SimpleNamespace(
                work=SimpleNamespace(work_id=f"work-{node}", payload_bytes=16),
                attempt=SimpleNamespace(attempt_id=f"attempt-{node}"),
            )
            for node in ("A", "B")
        }
        self.groups = {}
        self.outcomes = {}
        self.messages = []
        self.sequence = {node: 0 for node in self.profiles}
        self.input_delays = input_delays or {}
        self.result_delays = result_delays or {}

    def build_message(self, sender, recipient, kind, body, wire_bytes,
                      *, attempt_id="", group_id=""):
        self.sequence[sender] += 1
        return Message(f"{sender}:{self.sequence[sender]}", sender, recipient,
                       kind, body, wire_bytes, attempt_id, group_id)

    def send_group_message(self, node_id, message, *, policy_hook=None):
        self.messages.append(message)
        delays = self.input_delays if message.kind == "reduce_input" else self.result_delays
        delay = delays.get((message.sender, message.recipient,
                            message.body.get("position")), 0.0)
        if delay:
            self.engine.schedule(delay, self._send_network, message)
        else:
            self._send_network(message)

    def _send_network(self, message):
        def deliver(delivered):
            group = self.groups.get(delivered.group_id)
            if group is None:
                return
            if delivered.kind == "reduce_input":
                group.input_received(delivered)
            elif delivered.kind == "reduce_result":
                group.result_received(delivered)

        if message.sender == message.recipient:
            self.engine.schedule(0, deliver, message)
        else:
            self.network.send(message, deliver)

    def _group_member_complete(self, node_id, outcome, result):
        self.outcomes[node_id] = (outcome, result)

    def _group_member_failed(self, node_id, result):
        self.outcomes[node_id] = (Outcome.FAILED, result)


def _make_group(*, sender_timeout=0.5, reducer_timeout=1.0,
                input_delays=None, result_delays=None):
    adapter = _AllReduceFixture(sender_timeout=sender_timeout,
                                reducer_timeout=reducer_timeout,
                                input_delays=input_delays,
                                result_delays=result_delays)
    roster = GroupRoster("group-1", "A", ("A", "B"),
                         ("work-A", "work-B"), 0.0)
    group = GroupAllReduce(adapter, roster)
    adapter.groups[group.group_id] = group
    return adapter, group


def test_chunk_streams_complete_with_global_chunk_receipts_and_work_provenance():
    adapter, group = _make_group()
    group.start_member("A")
    group.start_member("B")
    adapter.engine.run(0.01)

    assert set(adapter.outcomes) == {"A", "B"}
    for outcome, result in adapter.outcomes.values():
        assert outcome is Outcome.COMPLETE
        assert [receipt.partition for receipt in result.receipts] == [0, 1, 2, 3]
        assert sum(receipt.size_bytes for receipt in result.receipts) == 16
        assert all(receipt.contributors == ("work-A", "work-B")
                   for receipt in result.receipts)
    assert sum(message.kind == "reduce_input" for message in adapter.messages) == 8
    assert sum(message.kind == "reduce_result" for message in adapter.messages) == 8


def test_sender_stream_timeout_keeps_earlier_chunks_and_excludes_late_chunk():
    adapter, group = _make_group(
        sender_timeout=0.5,
        input_delays={("B", "A", 0): 0.75},
    )
    group.start_member("A")
    group.start_member("B")
    adapter.engine.run(0.6)

    # With A's own contribution still present, A's reducer completes its
    # chunks after B's stream inactivity expires. The delayed B chunk arrives
    # after the stream is closed and cannot alter the recorded receipt.
    a_result = adapter.outcomes["A"][1]
    receipt_by_chunk = {receipt.partition: receipt for receipt in a_result.receipts}
    assert receipt_by_chunk[0].contributors == ("work-A",)
    assert receipt_by_chunk[1].contributors == ("work-A",)
    assert not any(receipt.fallback for receipt in a_result.receipts)


def test_recipient_timeout_falls_back_only_for_that_reducer_stream():
    adapter, group = _make_group(
        sender_timeout=0.5,
        reducer_timeout=1.0,
        result_delays={("A", "B", 0): 2.0},
    )
    group.start_member("A")
    group.start_member("B")
    adapter.engine.run(1.5)

    outcome, result = adapter.outcomes["B"]
    assert outcome is Outcome.PARTIAL
    by_chunk = {receipt.partition: receipt for receipt in result.receipts}
    assert by_chunk[0].fallback and by_chunk[1].fallback
    assert by_chunk[0].contributors == ("work-B",)
    assert not by_chunk[2].fallback and not by_chunk[3].fallback


def test_sender_sends_before_remote_reducer_starts_and_receiver_buffers_chunk():
    adapter, group = _make_group()
    group.start_member("A")
    adapter.engine.run(0.001)

    assert not group.members_state["B"].started
    assert group.reducers["B"].pending_inputs
    assert any(message.kind == "reduce_input" and message.sender == "A"
               and message.recipient == "B" for message in adapter.messages)

    group.start_member("B")
    adapter.engine.run(0.01)
    assert set(adapter.outcomes) == {"A", "B"}


def test_missing_first_sender_expires_from_reducer_start_before_late_announcement():
    adapter, group = _make_group(sender_timeout=0.5, reducer_timeout=1.0)
    group.start_member("A")
    adapter.engine.run(0.6)

    # A's local reducer has already expired B's missing sender stream. B's
    # later announcement cannot restart that stream or submit an input chunk.
    assert group.reducers["A"].streams["B"].failed
    group.start_member("B")
    adapter.engine.run(2.0)

    assert not any(message.kind == "reduce_input"
                   and message.sender == "B" and message.recipient == "A"
                   for message in adapter.messages)
    assert set(adapter.outcomes) == {"A", "B"}
    a_receipts = {receipt.partition: receipt for receipt in adapter.outcomes["A"][1].receipts}
    assert a_receipts[0].contributors == ("work-A",)
    assert a_receipts[1].contributors == ("work-A",)


def test_sender_failure_retains_provenance_of_previously_received_chunk():
    adapter, group = _make_group(input_delays={("B", "A", 1): .75})
    group.start_member("A")
    group.start_member("B")
    adapter.engine.run(.6)
    receipts = {r.partition: r for r in adapter.outcomes["A"][1].receipts}
    assert receipts[0].contributors == ("work-A", "work-B")
    assert receipts[1].contributors == ("work-A",)
