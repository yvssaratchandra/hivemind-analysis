from trainsim.engine import Engine
from trainsim.network import Network
from trainsim.types import Message, NodeProfile


def test_tiny_positive_residual_advances_time_and_delivers_once():
    profiles = {name: NodeProfile(name, name.encode(), 1., 12_500_000.,
                                  12_500_000., 0.) for name in ("A", "B")}
    engine = Engine()
    network = Network(engine, profiles)
    received = []
    engine.run(130.8432447158627)
    handle = network.send(Message("rounding", "A", "B", "test", {}, 128),
                          lambda message: received.append((message.message_id, engine.now)))
    transfer = network._active[handle.transfer_id]
    transfer.remaining_bytes = 1.5e-7
    transfer.updated_at = engine.now
    transfer.completion_timer.cancel()
    transfer.completion_timer = None
    network._schedule_completion(transfer)
    # A bounded callback loop makes a recurrence fail without hanging the suite.
    for _ in range(20):
        if received:
            break
        engine.env.step()
    assert received == [("rounding", received[0][1])]
    assert received[0][1] > 130.8432447158627
    assert not network._active


def test_self_transfer_uses_no_time_or_shared_capacity():
    p = {name: NodeProfile(name, name.encode(), 1., 100., 100., .5) for name in ("A", "B")}
    e = Engine()
    network = Network(e, p)
    arrivals = []
    network.send(Message("self", "A", "A", "test", {}, 100000), lambda m: arrivals.append((m.message_id, e.now)))
    network.send(Message("remote", "A", "B", "test", {}, 100), lambda m: arrivals.append((m.message_id, e.now)))
    e.run(3)
    assert arrivals == [("self", 0.), ("remote", 2.)]


def test_unlimited_endpoints_have_only_propagation_delay():
    p = {name: NodeProfile(name, name.encode(), 1., float("inf"), float("inf"), .5) for name in ("A", "B")}
    engine = Engine()
    network = Network(engine, p)
    received = []
    network.send(Message("unlimited", "A", "B", "test", {}, 100), lambda m: received.append(engine.now))
    engine.run(2.)
    assert received == [1.]
