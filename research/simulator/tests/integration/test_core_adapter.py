"""Exercise core resources/accounting without importing a production adapter."""
from dataclasses import asdict

from trainsim.config import make_profiles, resolve
from trainsim.engine import Engine
from trainsim.network import Network
from trainsim.observer import Observer
from trainsim.types import GroupRoster, LocalAggregateResult, Message, Outcome, PartitionReceipt
from trainsim.workload import Workload


class PairAdapter:
    """Test-only protocol: pair successive ready nodes and exchange one chunk."""
    def __init__(self, engine, network, terminal):
        self.engine, self.network, self.terminal = engine, network, terminal
        self.waiting = []
        self.groups = 0

    def start(self, node):
        pass

    def stop(self, node):
        pass

    def on_work_ready(self, node, work):
        self.waiting.append((node, work))
        if len(self.waiting) < 2:
            return
        pair, self.waiting = self.waiting[:2], self.waiting[2:]
        group = f"pair{self.groups}"
        self.groups += 1
        self.engine.emit("group_formed", pair[0][0], roster=asdict(GroupRoster(
            group, pair[0][0], tuple(n for n, w in pair), tuple(w.work_id for n, w in pair), self.engine.now)))
        for (sender, work), (recipient, other) in (pair, pair[::-1]):
            message = Message(group+sender, sender, recipient, "pair_update", {}, work.payload_bytes)
            def delivered(message, own=sender, peer=recipient, work=work, other=other):
                receipt = PartitionReceipt(group, peer, 0, work.payload_bytes,
                                           (work.work_id, other.work_id), (.5, .5))
                result = LocalAggregateResult(peer, group, Outcome.COMPLETE, (receipt,), work.payload_bytes, self.engine.now)
                self.engine.emit("aggregate_result", peer, result=asdict(result))
                self.terminal(own, Outcome.COMPLETE)
            self.network.send(message, delivered)


def test_core_accepts_an_unrelated_protocol():
    c = resolve(dict(population=2, initial_offset_seconds=0, payload_bytes=128, chunk_bytes=128,
                     horizon_seconds=50, observation_seconds=0, initial_bits=0))
    profiles = make_profiles(c, 1)
    observer = Observer()
    engine = Engine(1, observer.emit)
    network = Network(engine, profiles)
    workload = None
    adapter = PairAdapter(engine, network, lambda n, o: workload.terminal(n, o))
    workload = Workload(engine, profiles, c, adapter)
    workload.start()
    engine.run(11)
    workload.stop()
    assert observer.accepted_work == 2
    assert observer.summary()["group_count"] == 1
