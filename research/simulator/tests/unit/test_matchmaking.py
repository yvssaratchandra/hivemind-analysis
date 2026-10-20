"""End-to-end local message tests for grouping and policy capabilities."""
from trainsim.config import make_profiles, resolve
from trainsim.discovery import Discovery
from trainsim.engine import Engine
from trainsim.network import Network
from trainsim.protocols.hivemind.adapter import HivemindAdapter
from trainsim.types import WorkItem


def setup_pair():
    c = resolve(dict(population=2, initial_bits=0, target_group_size=2,
                     registry_latency_seconds=0, latency_seconds=.001,
                     payload_bytes=8, chunk_bytes=4))
    p = make_profiles(c, 11)
    events = []
    engine = Engine(11, lambda t, k, a, **d: events.append((t, k, a, d)))
    network = Network(engine, p, registry_latency_seconds=0)
    discovery = Discovery(engine, network)
    return c, p, events, engine, network, discovery


def test_equal_deadlines_choose_public_peer_id_order():
    c, p, events, engine, network, discovery = setup_pair()
    outcomes = []
    adapter = HivemindAdapter(engine, network, discovery, p, c,
                             lambda n, o: outcomes.append((n, o)))
    for node in p:
        adapter.start(node)
        adapter.on_work_ready(node, WorkItem(f"{node}:0", node, 0, 0., 0., 1., 8))
    engine.run(.1)
    rosters = [d["roster"] for t, k, a, d in events if k == "group_formed"]
    assert len(rosters) == 1
    assert rosters[0]["leader"] == min(p, key=lambda n: p[n].peer_id)
    assert len(outcomes) == 2


def test_selective_rejection_requires_actual_leadership_opportunity():
    c, p, events, engine, network, discovery = setup_pair()
    leader = min(p, key=lambda n: p[n].peer_id)
    follower = next(n for n in p if n != leader)
    c["policies"] = {leader: dict(kind="fixture", reject_join_from=[follower])}
    adapter = HivemindAdapter(engine, network, discovery, p, c, lambda n, o: None)
    for node in p:
        adapter.start(node)
        adapter.on_work_ready(node, WorkItem(f"{node}:0", node, 0, 0., 0., 1., 8))
    engine.run(1)
    assert not any(k == "group_formed" for t, k, a, d in events)
    actions = [(a, d) for t, k, a, d in events if k == "policy_action"]
    assert any(a == leader and d["hook"] == "incoming_join" and not d["selected"]["accept"]
               for a, d in actions)
