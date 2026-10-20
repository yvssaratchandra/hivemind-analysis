"""Focused tests for Agora's local roster, streaming, and timeout behavior."""

from trainsim.engine import Engine
from trainsim.network import Network
from trainsim.protocols.agora.aggregation import AgoraAggregator
from trainsim.protocols.agora.partitioning import reducer_allocations
from trainsim.protocols.agora.workload import AgoraWorkload, SparseSnapshot
from trainsim.types import NodeProfile


def _fixture(*, include_missing=False, tensor_elements=8, upload_overrides=None):
    node_names = ["R", "C1", "C2"] if include_missing else ["R", "C1"]
    profiles = {
        name: NodeProfile(
            node_id=name,
            peer_id=bytes([index + 1]) * 20,
            compute_seconds=1.0,
            upload_bps=(upload_overrides or {}).get(name, 1_000_000.0),
            download_bps=1_000_000.0,
            latency_seconds=0.001,
        )
        for index, name in enumerate(node_names)
    }
    order = sorted(node_names, key=lambda name: profiles[name].peer_id)
    snapshots = {
        name: SparseSnapshot(
            node_id=name,
            epoch=1,
            global_step=2,
            layout="layout-1",
            partition=1,
            tensor_cardinalities=(tensor_elements,),
            selected_cardinality=tensor_elements,
            snapshot_id=f"{name}:e1",
            role="NODE" if name == "R" else "CLIENT",
        )
        for name in node_names
    }
    roster = {
        "stage": "stage",
        "epoch": 1,
        "ordered_node_ids": order,
        "ordered_peer_ids": [profiles[name].peer_id.hex() for name in order],
        "gathered": [dict(snapshots[name].as_gather(), role=snapshots[name].role) for name in order],
        "roster_fingerprint": "test-roster",
    }
    config = {
        "stage": "stage",
        "chunk_bytes": 16,
        "stream_window_parts": 3,
        "sender_timeout": 30.0,
        "reducer_timeout": 60.0,
        "allreduce_timeout": 120.0,
    }
    events = []
    engine = Engine(seed=1, sink=lambda time, kind, actor, **data: events.append(
        dict(time=time, kind=kind, actor=actor, **data)
    ))
    network = Network(engine, profiles, registry_latency_seconds=0.001)
    aggregator = AgoraAggregator(engine, network, profiles, config)
    callbacks = []
    return engine, aggregator, roster, snapshots, events, callbacks


def test_client_stream_over_window_sends_all_input_before_results_and_signals_completion():
    engine, aggregator, roster, snapshots, events, callbacks = _fixture(tensor_elements=32)
    for node in ("R", "C1"):
        engine.schedule(
            0,
            aggregator.start,
            node,
            snapshots[node],
            roster,
            lambda peer, epoch, result: callbacks.append((peer, epoch, result, engine.now)),
        )

    engine.run(10)

    assert {row[0] for row in callbacks} == {"R", "C1"}
    client_stream = next(
        row["stream_id"]
        for row in events
        if row["kind"] == "agora_stream_part_sent" and row["actor"] == "C1"
    )
    client_inputs = [
        row["time"] for row in events
        if row["kind"] == "agora_part_received" and row["stream_id"] == client_stream
    ]
    client_results = [
        row["time"] for row in events
        if row["kind"] == "agora_part_result_received" and row["stream_id"] == client_stream
    ]
    assert len(client_inputs) == len(client_results) == 8
    assert min(client_results) >= max(client_inputs)

    client_credit = next(
        row for row in events
        if row["kind"] == "agora_sparse_application"
        and row["origin"] == "C1" and row["recipient"] == "R"
    )
    assert client_credit["status"] == "complete"
    assert client_credit["expected_component_ids"] == [f"t0:c{index}" for index in range(8)]
    assert client_credit["applied_component_cardinalities"] == {
        f"t0:c{index}": 4 for index in range(8)
    }
    assert all(item["receipt_id"] for item in client_credit["component_evidence"])


def test_inactive_roster_peer_is_banned_after_sender_timeout_not_round_deadline():
    engine, aggregator, roster, snapshots, events, callbacks = _fixture(include_missing=True)
    for node in ("R", "C1"):
        engine.schedule(
            0,
            aggregator.start,
            node,
            snapshots[node],
            roster,
            lambda peer, epoch, result: callbacks.append((peer, epoch, result, engine.now)),
        )

    engine.run(35)

    assert {row[0] for row in callbacks} == {"R", "C1"}
    assert all(29.9 <= row[3] < 31.0 for row in callbacks)
    assert any(
        row["kind"] == "agora_sender_banned" and row["actor"] == "R"
        and row["sender"] == "C2" and row["reason"] == "sender_timeout"
        for row in events
    )
    # Missing senders are excluded; outputs still use the remaining inputs.
    assert not any(
        row["kind"] == "agora_reducer_fallback" and row["actor"] == "R"
        for row in events
    )


def test_sender_timeout_resets_on_continuous_incoming_progress():
    engine, aggregator, roster, snapshots, events, callbacks = _fixture(
        tensor_elements=16, upload_overrides={"C1": 1_500.0}
    )
    aggregator.config["stream_window_parts"] = 1
    aggregator.config["sender_timeout"] = 0.1
    aggregator.config["reducer_timeout"] = 1.0
    aggregator.config["allreduce_timeout"] = 2.0
    for node in ("R", "C1"):
        engine.schedule(
            0,
            aggregator.start,
            node,
            snapshots[node],
            roster,
            lambda peer, epoch, result: callbacks.append((peer, epoch, result, engine.now)),
        )

    engine.run(1.5)

    assert {row[0] for row in callbacks} == {"R", "C1"}
    assert not any(
        row["kind"] == "agora_sender_banned" and row["sender"] == "C1"
        for row in events
    )


def test_roster_role_divergence_fails_locally_without_false_complete_application():
    engine, aggregator, roster, snapshots, events, callbacks = _fixture(tensor_elements=8)
    source_view = {
        **roster,
        "gathered": [dict(row) for row in roster["gathered"]],
    }
    source_view["gathered"][1]["role"] = "CLIENT"
    reducer_view = {
        **roster,
        "gathered": [dict(row) for row in roster["gathered"]],
    }
    reducer_view["gathered"][1]["role"] = "NODE"
    aggregator.config["sender_timeout"] = 1.0
    aggregator.config["reducer_timeout"] = 2.0
    aggregator.config["allreduce_timeout"] = 4.0
    engine.schedule(0, aggregator.start, "R", snapshots["R"], reducer_view,
                    lambda peer, epoch, result: callbacks.append((peer, epoch, result, engine.now)))
    engine.schedule(0, aggregator.start, "C1", snapshots["C1"], source_view,
                    lambda peer, epoch, result: callbacks.append((peer, epoch, result, engine.now)))

    engine.run(6)

    assert {row[0] for row in callbacks} == {"R", "C1"}
    assert any(row["kind"] == "agora_protocol_rejection" for row in events)
    assert any(row["kind"] == "agora_reducer_fallback" for row in events)
    assert not any(
        row["kind"] == "agora_sparse_application" and row["status"] == "complete"
        for row in events
    )


def test_failed_client_stream_releases_reducer_close_barrier_after_partial_input():
    interrupted = {"done": False}
    engine = None
    aggregator = None
    events = []

    def sink(time, kind, actor, **data):
        events.append(dict(time=time, kind=kind, actor=actor, **data))
        if kind == "agora_part_received" and actor == "R" and not interrupted["done"]:
            interrupted["done"] = True
            stream = aggregator.active["C1"].streams["R"]
            engine.schedule(0, aggregator._fail_stream, stream, "sender_timeout")

    node_names = ["R", "C1"]
    profiles = {
        name: NodeProfile(name, bytes([index + 1]) * 20, 1.0, 1_000_000.0,
                          1_000_000.0, 0.001)
        for index, name in enumerate(node_names)
    }
    snapshots = {
        name: SparseSnapshot(name, 1, 2, "layout-1", 1, (64,), 64,
                              f"{name}:e1", "NODE" if name == "R" else "CLIENT")
        for name in node_names
    }
    roster = {
        "stage": "stage", "epoch": 1,
        "ordered_node_ids": node_names,
        "ordered_peer_ids": [profiles[name].peer_id.hex() for name in node_names],
        "gathered": [snapshots[name].as_gather() for name in node_names],
        "roster_fingerprint": "test-roster",
    }
    config = {"stage": "stage", "chunk_bytes": 16, "stream_window_parts": 1,
              "sender_timeout": 30.0, "reducer_timeout": 60.0, "allreduce_timeout": 120.0}
    engine = Engine(seed=1, sink=sink)
    aggregator = AgoraAggregator(engine, Network(engine, profiles, 0.001), profiles, config)
    callbacks = []
    for node in node_names:
        engine.schedule(0, aggregator.start, node, snapshots[node], roster,
                        lambda peer, epoch, result: callbacks.append((peer, result, engine.now)))

    engine.run(10)

    assert interrupted["done"]
    assert {row[0] for row in callbacks} == set(node_names)
    assert all(row[2] < 10 for row in callbacks)
    assert not aggregator.active


def test_workload_skips_new_average_while_previous_average_is_busy():
    events = []
    engine = Engine(seed=7, sink=lambda time, kind, actor, **data:
                    events.append(dict(time=time, kind=kind, actor=actor, **data)))
    profile = NodeProfile("N", b"N" * 20, 0.5, 1_000_000, 1_000_000, 0.001)

    class Matcher:
        def begin(self, node, epoch, step, gather, on_roster):
            on_roster(node, epoch, {"stage": "stage"})

    class BusyAggregator:
        def __init__(self):
            self.epochs = []

        def start(self, node, snapshot, roster, on_done):
            self.epochs.append(snapshot.epoch)

    aggregator = BusyAggregator()
    workload = AgoraWorkload(
        engine, {"N": profile},
        {"stage": "stage", "reducers": 1, "tensor_elements": [8], "partitions": 2,
         "chunk_bytes": 16, "initial_offset_seconds": 0.0, "average_every": 1},
        Matcher(), aggregator,
    )
    workload.start()
    engine.run(1.6)

    steps = [row["time"] for row in events if row["kind"] == "agora_local_step"]
    skipped = [row["epoch"] for row in events if row["kind"] == "round_skipped"]
    assert steps == [0.5, 1.0, 1.5]
    assert aggregator.epochs == [1]
    assert skipped == [2, 3]


def test_nondivisible_reducer_work_uses_native_hagenbach_bischoff_counts():
    roles = ["NODE", "NODE", "NODE", "CLIENT", "CLIENT"]
    assert reducer_allocations(10, roles) == (4, 3, 3, 0, 0)
    assert reducer_allocations(11, roles) == (4, 4, 3, 0, 0)
