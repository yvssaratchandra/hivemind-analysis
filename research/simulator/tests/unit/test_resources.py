from __future__ import annotations

import math
import random

import pytest

from trainsim.discovery import Discovery
from trainsim.engine import Engine
from trainsim.network import Network
from trainsim.randomness import rng
from trainsim.types import Advertisement, Message, NodeProfile


def _profiles(
    names: tuple[str, ...],
    *,
    upload: float = 1_000.0,
    download: float = 1_000.0,
    latency: float = 0.0,
) -> dict[str, NodeProfile]:
    return {
        name: NodeProfile(
            node_id=name,
            peer_id=name.encode(),
            compute_seconds=1.0,
            upload_bps=upload,
            download_bps=download,
            latency_seconds=latency,
        )
        for name in names
    }


def _message(sender: str, recipient: str, wire_bytes: int, suffix: str = "m") -> Message:
    return Message(
        message_id=suffix,
        sender=sender,
        recipient=recipient,
        kind="test",
        body={},
        wire_bytes=wire_bytes,
    )


def _slow_max_min_rates(network: Network, transfers) -> dict[int, float]:
    """Reference allocator retained here to differentially check optimization."""
    rates = {transfer.handle.transfer_id: 0.0 for transfer in transfers}
    capacities = {}
    uses = {}
    for transfer in transfers:
        upload, _, _ = network._endpoint(transfer.message.sender)
        _, download, _ = network._endpoint(transfer.message.recipient)
        resources = []
        if math.isfinite(upload):
            resource = ("upload", transfer.message.sender)
            capacities[resource] = upload
            resources.append(resource)
        if math.isfinite(download):
            resource = ("download", transfer.message.recipient)
            capacities[resource] = download
            resources.append(resource)
        uses[transfer.handle.transfer_id] = tuple(resources)

    if not capacities:
        return {transfer_id: math.inf for transfer_id in rates}

    unfrozen = set(rates)
    while unfrozen:
        candidates = []
        residuals = {}
        consumers = {}
        for resource, capacity in capacities.items():
            users = [transfer_id for transfer_id in unfrozen if resource in uses[transfer_id]]
            if not users:
                continue
            used = sum(
                rates[transfer_id]
                for transfer_id, flow_resources in uses.items()
                if resource in flow_resources
            )
            residuals[resource] = max(0.0, capacity - used)
            consumers[resource] = users
            candidates.append((residuals[resource] / len(users), resource))
        if not candidates:
            for transfer_id in unfrozen:
                rates[transfer_id] = math.inf
            break

        increment = min(value for value, _ in candidates)
        for transfer_id in unfrozen:
            rates[transfer_id] += increment
        saturated = set()
        for resource, users in consumers.items():
            amount = increment * len(users)
            tolerance = max(1e-9, capacities[resource] * 1e-12)
            if residuals[resource] - amount <= tolerance:
                saturated.add(resource)
        if not saturated:
            saturated.add(min(candidates, key=lambda item: item[0])[1])
        newly_frozen = {
            transfer_id for transfer_id in unfrozen
            if any(resource in uses[transfer_id] for resource in saturated)
        }
        assert newly_frozen
        unfrozen.difference_update(newly_frozen)
    return rates


def test_engine_scheduling_is_repeatable_and_horizon_is_exclusive():
    def replay() -> tuple[list[tuple[float, str]], float]:
        seen: list[tuple[float, str]] = []
        engine = Engine(seed=11, sink=lambda t, kind, actor="", **data: seen.append((t, kind)))
        engine.schedule(1.0, engine.emit, "at_boundary")
        engine.schedule(0.25, engine.emit, "before_boundary")
        engine.run(1.0)
        assert [kind for _, kind in seen] == ["before_boundary"]
        engine.run(1.1)
        return seen, engine.now

    first = replay()
    second = replay()
    assert first == second
    assert first[0] == [(0.25, "before_boundary"), (1.0, "at_boundary")]
    assert first[1] == pytest.approx(1.1)


def test_rng_streams_are_repeatable_and_scoped():
    left = rng(23, "workload", "node-a", 4)
    same = rng(23, "workload", "node-a", 4)
    separate = rng(23, "workload", "node-b", 4)
    assert [left.random() for _ in range(8)] == [same.random() for _ in range(8)]
    assert rng(23, "workload", "node-a", 4).random() != separate.random()


def test_max_min_sharing_reallocates_capacity_when_shorter_transfer_finishes():
    profiles = _profiles(("source", "left", "right"), upload=100.0, download=1_000.0)
    engine = Engine()
    network = Network(engine, profiles)
    delivered: list[tuple[str, float]] = []
    network.send(
        _message("source", "left", 100, "short"),
        lambda message: delivered.append((message.message_id, engine.now)),
    )
    network.send(
        _message("source", "right", 200, "long"),
        lambda message: delivered.append((message.message_id, engine.now)),
    )

    engine.run(5.0)
    assert delivered == [("short", pytest.approx(2.0)), ("long", pytest.approx(3.0))]


def test_incidence_allocator_matches_reference_on_random_endpoint_graphs():
    randomizer = random.Random(20261019)
    capacities = (0.0, 1.0, 3.0, 10.0, 80.0, math.inf)
    endpoints = ("n0", "n1", "n2", "n3", "n4", "__registry__")
    for trial in range(40):
        profiles = {}
        for node in endpoints[:-1]:
            profiles[node] = NodeProfile(
                node_id=node,
                peer_id=node.encode(),
                compute_seconds=1.0,
                upload_bps=randomizer.choice(capacities),
                download_bps=randomizer.choice(capacities),
                latency_seconds=0.0,
            )
        network = Network(Engine(), profiles, registry_latency_seconds=0.0)
        possible_pairs = [(src, dst) for src in endpoints for dst in endpoints if src != dst]
        randomizer.shuffle(possible_pairs)
        for index, (src, dst) in enumerate(possible_pairs[:randomizer.randint(1, 24)]):
            network.send(_message(src, dst, randomizer.randint(1, 500), f"{trial}-{index}"), lambda _: None)

        transfers = tuple(network._active.values())
        expected = _slow_max_min_rates(network, transfers)
        actual = network._max_min_rates(transfers)
        assert actual.keys() == expected.keys()
        for transfer_id, expected_rate in expected.items():
            actual_rate = actual[transfer_id]
            if math.isinf(expected_rate):
                assert actual_rate == expected_rate
            else:
                assert actual_rate == pytest.approx(expected_rate, rel=1e-9, abs=1e-9)


def test_download_capacity_is_shared_across_different_senders():
    profiles = _profiles(("source-a", "source-b", "target"), upload=1_000.0)
    profiles["target"] = NodeProfile(
        node_id="target",
        peer_id=b"target",
        compute_seconds=1.0,
        upload_bps=1_000.0,
        download_bps=100.0,
        latency_seconds=0.0,
    )
    engine = Engine()
    network = Network(engine, profiles)
    delivered: list[tuple[str, float]] = []
    network.send(
        _message("source-a", "target", 100, "short"),
        lambda message: delivered.append((message.message_id, engine.now)),
    )
    network.send(
        _message("source-b", "target", 200, "long"),
        lambda message: delivered.append((message.message_id, engine.now)),
    )

    engine.run(5.0)
    assert delivered == [("short", pytest.approx(2.0)), ("long", pytest.approx(3.0))]


def test_unrelated_and_registry_flows_keep_independent_completion_timers():
    profiles = _profiles(("node-a", "node-b", "node-c", "node-d", "node-e", "node-f"),
                         upload=100.0, download=100.0)
    engine = Engine()
    network = Network(engine, profiles, registry_latency_seconds=0.0)
    delivered: list[tuple[str, float]] = []
    network.send(_message("node-a", "node-b", 100, "pair"),
                 lambda message: delivered.append((message.message_id, engine.now)))
    first = network._active[1]
    first_timer = first.completion_timer
    network.send(_message("node-c", "node-d", 100, "other-pair"),
                 lambda message: delivered.append((message.message_id, engine.now)))
    assert first.completion_timer is first_timer

    # The registry is infinite-capacity, so it does not join distinct client
    # upload components into one artificial global bottleneck.
    request_a = network.send(_message("node-e", "__registry__", 100, "registry-a"), lambda _: None)
    request_c = network.send(_message("node-f", "__registry__", 100, "registry-c"), lambda _: None)
    assert network._connected_component((request_a.transfer_id,)) == {request_a.transfer_id}
    assert network._connected_component((request_c.transfer_id,)) == {request_c.transfer_id}

    engine.run(2.0)
    assert delivered == [("pair", pytest.approx(1.0)), ("other-pair", pytest.approx(1.0))]


def test_unchanged_rate_inside_connected_component_keeps_completion_timer():
    profiles = _profiles(("source", "limited", "short"), upload=100.0, download=100.0)
    profiles["limited"] = NodeProfile(
        node_id="limited", peer_id=b"limited", compute_seconds=1.0,
        upload_bps=100.0, download_bps=10.0, latency_seconds=0.0,
    )
    profiles["short"] = NodeProfile(
        node_id="short", peer_id=b"short", compute_seconds=1.0,
        upload_bps=100.0, download_bps=1.0, latency_seconds=0.0,
    )
    engine = Engine()
    network = Network(engine, profiles)
    delivered: list[tuple[str, float]] = []

    network.send(_message("source", "limited", 100, "stable-rate"),
                 lambda message: delivered.append((message.message_id, engine.now)))
    stable = network._active[1]
    original_timer = stable.completion_timer
    assert stable.rate_bytes_per_second == pytest.approx(10.0)

    # The second flow shares source upload but its destination caps it at 1 B/s.
    # The first flow remains capped by `limited` at exactly 10 B/s.
    network.send(_message("source", "short", 10, "short-flow"),
                 lambda message: delivered.append((message.message_id, engine.now)))
    assert network._connected_component((stable.handle.transfer_id,)) == {1, 2}
    assert stable.rate_bytes_per_second == pytest.approx(10.0)
    assert stable.completion_timer is original_timer
    assert not original_timer.cancelled

    engine.run(10.1)
    assert delivered == [
        ("stable-rate", pytest.approx(10.0)),
        ("short-flow", pytest.approx(10.0)),
    ]


def test_cancelled_transfer_releases_capacity_and_never_delivers():
    profiles = _profiles(("source", "left", "right"), upload=100.0, download=1_000.0)
    engine = Engine()
    network = Network(engine, profiles)
    delivered: list[tuple[str, float]] = []
    cancelled = network.send(
        _message("source", "left", 100, "cancelled"),
        lambda message: delivered.append((message.message_id, engine.now)),
    )
    network.send(
        _message("source", "right", 100, "survivor"),
        lambda message: delivered.append((message.message_id, engine.now)),
    )
    engine.call_at(1.0, network.cancel, cancelled)

    engine.run(3.0)
    assert cancelled.cancelled
    assert delivered == [("survivor", pytest.approx(1.5))]


def test_cancellation_during_propagation_prevents_delivery():
    profiles = _profiles(("source", "target"), latency=0.2)
    engine = Engine()
    network = Network(engine, profiles)
    delivered: list[Message] = []
    handle = network.send(
        _message("source", "target", 0), delivered.append
    )
    engine.call_at(0.1, network.cancel, handle)

    engine.run(1.0)
    assert handle.cancelled
    assert delivered == []


def test_discovery_commits_at_registry_arrival_and_withdrawal_wins_by_expiry():
    profiles = _profiles(("node-a", "node-b"), upload=10_000_000.0, download=10_000_000.0)
    engine = Engine()
    events: list[tuple[float, str, dict]] = []
    engine.sink = lambda t, kind, actor="", **data: events.append((t, kind, data))
    network = Network(engine, profiles)
    discovery = Discovery(engine, network)
    publish_results: list[bool] = []
    lookup_results: list[list[Advertisement]] = []

    active = Advertisement(key="bucket", peer_id="peer-a", active=True, expiry=1.0)
    discovery.publish("node-a", active, publish_results.append)
    engine.run(0.1)
    assert publish_results == [True]
    assert any(kind == "discovery_publish" and data["accepted"] for _, kind, data in events)

    # A withdrawal uses the next float above the active lease, so an active
    # record with the same original expiry cannot overwrite the tombstone.
    discovery.publish(
        "node-a",
        Advertisement(key="bucket", peer_id="peer-a", active=False, expiry=1.0),
        publish_results.append,
    )
    engine.run(0.2)
    assert publish_results == [True, True]
    discovery.publish(
        "node-b",
        Advertisement(key="bucket", peer_id="peer-a", active=True, expiry=1.0),
        publish_results.append,
    )
    engine.run(0.3)
    assert publish_results == [True, True, False]

    discovery.lookup("node-b", "bucket", lookup_results.append)
    engine.run(0.5)
    assert len(lookup_results) == 1
    assert lookup_results[0] == [
        Advertisement(
            key="bucket", peer_id="peer-a", active=False, expiry=math.nextafter(1.0, math.inf)
        )
    ]


def test_discovery_filters_a_record_again_if_it_expires_in_transit():
    profiles = _profiles(("writer", "reader"), upload=10_000_000.0, download=10_000_000.0)
    engine = Engine()
    network = Network(engine, profiles)
    discovery = Discovery(engine, network)
    results: list[list[Advertisement]] = []

    discovery.publish(
        "writer",
        Advertisement(key="short-lease", peer_id="p", active=True, expiry=0.2),
    )
    engine.run(0.1)
    engine.call_at(0.15, discovery.lookup, "reader", "short-lease", results.append)
    engine.run(0.3)
    assert results == [[]]


def test_expired_publish_is_rejected_at_registry_arrival():
    profiles = _profiles(("writer",), upload=10_000_000.0, download=10_000_000.0)
    engine = Engine()
    network = Network(engine, profiles)
    discovery = Discovery(engine, network)
    statuses: list[bool] = []

    # Request reaches the registry at about 25 ms, after its stated expiry.
    discovery.publish(
        "writer",
        Advertisement(key="expired", peer_id="p", active=True, expiry=0.01),
        statuses.append,
    )
    engine.run(0.1)
    assert statuses == [False]


def test_registry_latency_can_be_disabled_for_native_validation():
    profiles = _profiles(("writer",), upload=10_000_000.0, download=10_000_000.0)
    engine = Engine()
    events: list[tuple[float, str]] = []
    engine.sink = lambda t, kind, actor="", **data: events.append((t, kind))
    network = Network(engine, profiles, registry_latency_seconds=0.0)
    discovery = Discovery(engine, network)
    statuses: list[bool] = []

    discovery.publish(
        "writer",
        Advertisement(key="native", peer_id="p", active=True, expiry=1.0),
        statuses.append,
    )
    engine.run(0.001)
    assert statuses == [True]
    publish_time = next(time for time, kind in events if kind == "discovery_publish")
    assert publish_time == pytest.approx(256 / 10_000_000)
