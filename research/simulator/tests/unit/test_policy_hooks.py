"""Policies can influence only their own exposed protocol actions."""
from trainsim.config import make_profiles, resolve
from trainsim.discovery import Discovery
from trainsim.engine import Engine
from trainsim.network import Network
from trainsim.protocols.hivemind.adapter import HivemindAdapter
from trainsim.types import WorkItem


def test_advertisement_hook_delays_own_publication():
    class DelayOwnAd:
        def decide(self, observation, proposed):
            if observation.hook == "advertisement" and observation.local["active"]:
                assert observation.local["peer_id"] == observation.node_id
                return dict(delay_seconds=1.)
            return proposed
    c = resolve(dict(population=1, registry_latency_seconds=0, latency_seconds=0,
                     payload_bytes=128, chunk_bytes=128))
    profiles = make_profiles(c, 1)
    trace = []
    engine = Engine(1, lambda t, k, a, **d: trace.append((t, k, a, d)))
    network = Network(engine, profiles, registry_latency_seconds=0)
    discovery = Discovery(engine, network)
    adapter = HivemindAdapter(engine, network, discovery, profiles, c,
                             lambda n, o: None, policies={"N0000": DelayOwnAd()})
    adapter.start("N0000")
    adapter.on_work_ready("N0000", WorkItem("N0000:0", "N0000", 0, 0., 0., 1., 128))
    views = []
    key = adapter.states["N0000"].declared_key
    engine.schedule(.2, discovery.lookup, "N0000", key, views.append)
    engine.schedule(1.2, discovery.lookup, "N0000", key, views.append)
    engine.run(1.5)
    assert views[0] == []
    assert [a.peer_id for a in views[1]] == ["N0000"]
    assert any(k == "policy_action" and d["hook"] == "advertisement" for t, k, a, d in trace)
