"""Focused checks for Agora advert timing and node-local lookup behavior."""

from __future__ import annotations

import math
from typing import Callable

import pytest

from trainsim.engine import Engine
from trainsim.network import Network
from trainsim.protocols.agora.matchmaking import (
    AgoraMatcher,
    RegistryRecord,
    TimedRegistry,
)
from trainsim.protocols.agora.partitioning import stage_namespace, step_key
from trainsim.types import NodeProfile


def _peer_id(number: int) -> bytes:
    return number.to_bytes(20, "big")


def _profile(node_id: str, number: int, latency: float = 0.0) -> NodeProfile:
    return NodeProfile(
        node_id=node_id,
        peer_id=_peer_id(number),
        compute_seconds=1.0,
        upload_bps=math.inf,
        download_bps=math.inf,
        latency_seconds=latency,
    )


def _record(
    number: int,
    key: str,
    *,
    expiry: float = math.inf,
    value: dict | None = None,
) -> RegistryRecord:
    return RegistryRecord(
        key=key,
        peer_id=_peer_id(number).hex(),
        value=value or {"role": "NODE", "snapshot_id": f"node{number}:step"},
        expiry=expiry,
    )


def _gather(node_id: str) -> dict:
    return {
        "role": "NODE",
        "layout": "layout-a",
        "partition": 0,
        "selected_cardinality": 1,
        "tensor_cardinalities": [1],
        "snapshot_id": f"{node_id}:step0",
        "weight": 1.0,
    }


def _capture(events: list[dict]) -> Callable:
    def sink(time: float, kind: str, actor: str, **data) -> None:
        events.append({"time": time, "kind": kind, "actor": actor, **data})

    return sink


class ScheduledRegistry:
    """A deterministic callback stub; protocol scheduling remains on Engine."""

    def __init__(
        self,
        engine: Engine,
        *,
        ack_delay: float = 0.0,
        stage_replies: dict[str, list[tuple[list[RegistryRecord], float]]] | None = None,
        step_replies: dict[str, list[tuple[list[RegistryRecord], float]]] | None = None,
    ) -> None:
        self.engine = engine
        self.ack_delay = ack_delay
        self.stage_replies = stage_replies or {}
        self.step_replies = step_replies or {}
        self.publish_calls: list[dict] = []
        self.lookup_calls: list[dict] = []

    def publish(
        self,
        node_id: str,
        key: str,
        peer_id: str,
        value: dict,
        expiry: float,
        callback=None,
        *,
        active: bool = True,
    ) -> None:
        self.publish_calls.append(
            {
                "time": self.engine.now,
                "node_id": node_id,
                "key": key,
                "peer_id": peer_id,
                "value": value,
                "expiry": expiry,
                "active": active,
            }
        )
        if callback is not None:
            self.engine.schedule(self.ack_delay, callback, True)

    def lookup(self, node_id: str, key: str, callback, *, prefix: bool = False) -> None:
        self.lookup_calls.append(
            {"time": self.engine.now, "node_id": node_id, "key": key, "prefix": prefix}
        )
        table = self.stage_replies if prefix else self.step_replies
        replies = table.get(node_id, [])
        records, delay = replies.pop(0) if replies else ([], 0.0)
        self.engine.schedule(delay, callback, records)


def _matcher(
    engine: Engine,
    registry,
    profiles: dict[str, NodeProfile],
    *,
    matchmaking_seconds: float = 10.0,
    poll_seconds: float = 1.0,
    on_roster=None,
) -> AgoraMatcher:
    return AgoraMatcher(
        engine=engine,
        registry=registry,
        profiles=profiles,
        config={
            "matchmaking_seconds": matchmaking_seconds,
            "poll_seconds": poll_seconds,
        },
        stage="agora",
        step_prefix="agora_avg",
        on_roster=on_roster or (lambda *_: None),
    )


def test_ack_delay_keeps_ad_expiry_at_40_and_starts_collection_at_ack():
    events: list[dict] = []
    engine = Engine(seed=1, sink=_capture(events))
    profiles = {"A": _profile("A", 1), "B": _profile("B", 2)}
    stage_key = stage_namespace("agora")
    advert_key = step_key("agora_avg", 20)
    registry = ScheduledRegistry(
        engine,
        ack_delay=2.0,
        stage_replies={
            "A": [
                (
                    [_record(1, f"{stage_key}0"), _record(2, f"{stage_key}1")],
                    0.1,
                )
            ]
        },
        step_replies={
            "A": [
                ([_record(2, advert_key), _record(1, advert_key)], 0.1)
            ]
        },
    )
    completed: list[tuple[str, int, dict | None]] = []
    matcher = _matcher(
        engine,
        registry,
        profiles,
        matchmaking_seconds=40.0,
        on_roster=lambda *args: completed.append(args),
    )

    matcher.begin("A", 0, 20, _gather("A"))
    engine.run(until=3.0)

    assert registry.publish_calls[0]["time"] == 0.0
    assert registry.publish_calls[0]["expiry"] == 40.0
    ack = next(event for event in events if event["kind"] == "agora_step_publish_ack")
    assert ack["publish_ack_time"] == pytest.approx(2.0)
    assert ack["collection_deadline"] == pytest.approx(42.0)
    assert len(completed) == 1
    roster = completed[0][2]
    assert roster is not None
    assert roster["reason"] == "expected_count"
    assert roster["formed_at"] < ack["collection_deadline"]
    assert roster["collection_wait_seconds"] == pytest.approx(0.2)
    assert roster["ad_to_roster_wait"] == pytest.approx(2.2)
    assert roster["ordered_peer_ids"] == [_peer_id(1).hex(), _peer_id(2).hex()]
    assert roster["ordered_node_ids"] == ["A", "B"]


def test_stale_expected_count_refresh_waits_one_poll_before_next_step_lookup():
    events: list[dict] = []
    engine = Engine(seed=2, sink=_capture(events))
    profiles = {"A": _profile("A", 1), "B": _profile("B", 2)}
    stage_key = stage_namespace("agora")
    advert_key = step_key("agora_avg", 40)
    registry = ScheduledRegistry(
        engine,
        ack_delay=0.1,
        stage_replies={
            "A": [
                ([_record(1, f"{stage_key}0")], 0.1),
                (
                    [_record(1, f"{stage_key}0"), _record(2, f"{stage_key}1")],
                    0.1,
                ),
            ]
        },
        step_replies={
            "A": [
                ([_record(1, advert_key), _record(2, advert_key)], 0.1),
                ([_record(1, advert_key), _record(2, advert_key)], 0.1),
            ]
        },
    )
    completed: list[tuple[str, int, dict | None]] = []
    matcher = _matcher(
        engine,
        registry,
        profiles,
        matchmaking_seconds=8.0,
        on_roster=lambda *args: completed.append(args),
    )

    matcher.begin("A", 0, 40, _gather("A"))
    engine.run(until=2.0)

    stage_lookups = [call for call in registry.lookup_calls if call["prefix"]]
    step_lookups = [call for call in registry.lookup_calls if not call["prefix"]]
    expected_events = [event for event in events if event["kind"] == "agora_expected_count"]
    assert [event["expected_count"] for event in expected_events] == [1, 2]
    assert len(stage_lookups) == 2
    assert len(step_lookups) == 2
    assert step_lookups[1]["time"] - expected_events[1]["lookup_time"] == pytest.approx(1.0)
    assert completed[0][2]["reason"] == "expected_count"


def test_previous_step_records_survive_empty_lookups_and_expiry_to_partial_deadline():
    events: list[dict] = []
    engine = Engine(seed=3, sink=_capture(events))
    profiles = {f"N{i}": _profile(f"N{i}", i + 1) for i in range(3)}
    stage_key = stage_namespace("agora")
    advert_key = step_key("agora_avg", 60)
    first_records = [
        _record(i + 1, advert_key, expiry=0.9, value={"role": "NODE", "snapshot_id": f"N{i}:s"})
        for i in range(3)
    ]
    registry = ScheduledRegistry(
        engine,
        ack_delay=0.1,
        stage_replies={"N0": [([_record(i + 1, f"{stage_key}{i}") for i in range(4)], 0.1)]},
        step_replies={"N0": [(first_records, 0.1), ([], 0.1)]},
    )
    completed: list[tuple[str, int, dict | None]] = []
    matcher = _matcher(
        engine,
        registry,
        profiles,
        matchmaking_seconds=4.0,
        on_roster=lambda *args: completed.append(args),
    )

    matcher.begin("N0", 0, 60, _gather("N0"))
    engine.run(until=6.0)

    assert len(completed) == 1
    roster = completed[0][2]
    assert roster is not None
    assert roster["reason"] == "deadline"
    assert roster["expected_count"] == 4
    assert roster["observed_count"] == 3
    assert roster["formed_at"] > 0.9
    assert roster["ordered_peer_ids"] == [_peer_id(i).hex() for i in (1, 2, 3)]
    later_lookups = [event for event in events if event["kind"] == "agora_lookup"]
    assert later_lookups[-1]["observed_count"] == 3
    assert later_lookups[-1]["record_expiries"] == {}


def test_empty_step_lookups_fail_at_collection_deadline():
    events: list[dict] = []
    engine = Engine(seed=4, sink=_capture(events))
    profiles = {"A": _profile("A", 1)}
    stage_key = stage_namespace("agora")
    registry = ScheduledRegistry(
        engine,
        ack_delay=0.1,
        stage_replies={"A": [([_record(1, f"{stage_key}0")], 0.1)]},
        step_replies={"A": [([], 0.1)]},
    )
    completed: list[tuple[str, int, dict | None]] = []
    matcher = _matcher(
        engine,
        registry,
        profiles,
        matchmaking_seconds=2.0,
        on_roster=lambda *args: completed.append(args),
    )

    matcher.begin("A", 0, 80, _gather("A"))
    engine.run(until=4.0)

    assert completed == [("A", 0, None)]
    failure = next(event for event in events if event["kind"] == "agora_matchmaking_failed")
    assert failure["reason"] == "empty_roster"
    assert failure["expected_count"] == 1


def test_matchers_keep_divergent_node_local_roster_views():
    engine = Engine(seed=5)
    profiles = {f"N{i}": _profile(f"N{i}", i + 1) for i in range(3)}
    stage_key = stage_namespace("agora")
    advert_key = step_key("agora_avg", 100)
    stage_records = [_record(1, f"{stage_key}0"), _record(2, f"{stage_key}1")]
    registry = ScheduledRegistry(
        engine,
        ack_delay=0.05,
        stage_replies={
            "N0": [(stage_records, 0.05)],
            "N1": [(stage_records, 0.05)],
        },
        step_replies={
            "N0": [([_record(1, advert_key), _record(2, advert_key)], 0.05)],
            "N1": [([_record(1, advert_key), _record(3, advert_key)], 0.05)],
        },
    )
    views: dict[str, dict | None] = {}
    matcher = _matcher(
        engine,
        registry,
        profiles,
        on_roster=lambda node, epoch, roster: views.__setitem__(node, roster),
    )

    matcher.begin("N0", 0, 100, _gather("N0"))
    matcher.begin("N1", 0, 100, _gather("N1"))
    engine.run(until=1.0)

    assert views["N0"]["ordered_node_ids"] == ["N0", "N1"]
    assert views["N1"]["ordered_node_ids"] == ["N0", "N2"]
    assert views["N0"]["roster_fingerprint"] != views["N1"]["roster_fingerprint"]


def test_real_registry_filters_record_that_expires_during_response_transit():
    events: list[dict] = []
    engine = Engine(seed=6, sink=_capture(events))
    profiles = {"A": _profile("A", 1, latency=0.1)}
    network = Network(engine, profiles, registry_latency_seconds=0.1)
    registry = TimedRegistry(engine, network)
    key = step_key("agora_avg", 120)
    expiry = 0.75
    ack_times: list[tuple[float, bool]] = []
    lookup_results: list[tuple[float, list[RegistryRecord]]] = []

    def on_publish(accepted: bool) -> None:
        ack_times.append((engine.now, accepted))
        registry.lookup(
            "A",
            key,
            lambda records: lookup_results.append((engine.now, records)),
        )

    registry.publish(
        "A",
        key,
        _peer_id(1).hex(),
        {"role": "NODE", "snapshot_id": "A:120"},
        expiry,
        on_publish,
    )
    engine.run(until=1.0)

    assert ack_times == [(pytest.approx(0.4), True)]
    assert len(lookup_results) == 1
    response_time, records = lookup_results[0]
    assert response_time > expiry
    assert records == []
    snapshot = next(event for event in events if event["kind"] == "agora_registry_lookup_snapshot")
    response = next(event for event in events if event["kind"] == "agora_registry_lookup_response")
    assert snapshot["count"] == 1
    assert response["count"] == 0
