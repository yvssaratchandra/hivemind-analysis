"""Short synthetic checks for the bounded compute-share candidate."""
import pytest

np = pytest.importorskip("numpy")

from trainsim.policies.announcements import AnnouncementPolicy
from trainsim.protocols.hivemind.compute import ComputeConfig, run_compute


def _run(config, policy, gradient):
    snapshots = {}

    def evaluate(time, models):
        snapshots[time] = {node: value.copy() for node, value in models.items()}
        return {"time": time}

    result = run_compute(config, policy, gradient, evaluate)
    return result, snapshots


def _constant_gradient(value_by_node):
    def gradient(seed, node, batch_index, weights):
        return np.full(weights.shape, value_by_node[node], dtype=np.float32)
    return gradient


def test_microbatches_wait_for_interbatch_and_ordinary_step_overhead():
    nodes = ("B", "D", "C")
    config = ComputeConfig(
        seed=1,
        horizon_seconds=3.5,
        rates={node: 32 for node in nodes},
        members=nodes,
        target_batch_size=1024,
        dimension=4,
        gradient_latency_seconds=0.1,
        state_latency_seconds=0.1,
        step_overhead_seconds={node: 0.2 for node in nodes},
        interbatch_overhead_seconds={node: 0.1 for node in nodes},
    )

    result, _ = _run(config, AnnouncementPolicy("honest"),
                     _constant_gradient({node: 0 for node in nodes}))

    starts = [event["time"] for event in result["trace"]
              if event["kind"] == "microbatch_computed" and event["actor"] == "B"]
    assert starts == pytest.approx([0.0, 1.3, 2.6])
    assert result["sample_totals"]["B"] == 64
    assert result["update_times"]["B"] == []
    assert result["evaluations"][0]["time"] == 0
    assert result["evaluations"][-1]["time"] == pytest.approx(3.5)
    assert not result["unsupported"]


def test_membership_policy_uses_only_accepted_sample_weighted_gradients():
    nodes = ("B", "D", "A", "C")
    config = ComputeConfig(
        seed=2,
        horizon_seconds=5.9,
        rates={"B": 32, "D": 32, "A": 8, "C": 32},
        members=nodes,
        target_batch_size=128,
        dimension=4,
        gradient_latency_seconds=0.05,
        state_latency_seconds=0.1,
    )

    result, snapshots = _run(config, AnnouncementPolicy("membership"),
                             _constant_gradient({"B": 1, "D": 2, "A": 99, "C": 3}))

    group = next(event["data"] for event in result["trace"]
                 if event["kind"] == "gradient_group_started")
    assert group["weights"] == {"B": 96, "D": 96, "A": 32, "C": 96}
    assert group["fallback_nodes"] == ["A"]
    for node in ("B", "D", "C"):
        np.testing.assert_allclose(snapshots[5.0][node], np.full(4, -0.2), atol=1e-6)
        assert result["update_times"][node] == pytest.approx([4.05])
    np.testing.assert_array_equal(snapshots[5.0]["A"], np.zeros(4))
    assert result["update_times"]["A"] == []


def test_completed_state_future_applies_on_optimizer_call_after_batch_compute():
    nodes = ("B", "D", "C")
    config = ComputeConfig(
        seed=3,
        horizon_seconds=4.1,
        rates={node: 32 for node in nodes},
        members=nodes,
        target_batch_size=128,
        dimension=4,
        gradient_latency_seconds=0.05,
        state_latency_seconds=0.1,
    )

    result, _ = _run(config, AnnouncementPolicy("honest"),
                     _constant_gradient({node: 0 for node in nodes}))

    node_events = [(event["time"], event["kind"], event["data"])
                   for event in result["trace"] if event["actor"] == "B"]
    computed = next(i for i, item in enumerate(node_events)
                    if item[1] == "microbatch_computed" and item[2]["batch_index"] == 3)
    optimizer = next(i for i, item in enumerate(node_events)
                     if item[1] == "optimizer_call" and item[2]["batch_index"] == 3)
    applied = next(i for i, item in enumerate(node_events)
                   if item[1] == "state_applied")
    assert node_events[computed][0] < node_events[optimizer][0]
    assert node_events[optimizer][0] == node_events[applied][0]
    assert optimizer < applied
    assert node_events[applied][2]["occasion"] == "optimizer_call"


def test_progress_polls_pause_during_blocked_update_and_resume_after_commit():
    nodes = ("B", "D", "C")
    config = ComputeConfig(
        seed=31,
        horizon_seconds=4.2,
        rates={node: 32 for node in nodes},
        members=nodes,
        target_batch_size=128,
        dimension=4,
        gradient_latency_seconds=1.0,
        state_latency_seconds=0.1,
    )

    result, _ = _run(config, AnnouncementPolicy("honest"),
                     _constant_gradient({node: 0 for node in nodes}))

    deferred = next(event for event in result["trace"]
                    if event["kind"] == "progress_poll_deferred" and event["actor"] == "B")
    resumed = next(event for event in result["trace"]
                   if event["kind"] == "progress_poll_resumed" and event["actor"] == "B")
    assert deferred["time"] == pytest.approx(3.5)
    assert resumed["time"] == pytest.approx(4.0)
    assert not any(event["kind"] == "progress_polled" and event["actor"] == "B"
                   and 3.5 <= event["time"] < 4.0 for event in result["trace"])


def test_incomplete_prior_state_round_at_update_boundary_fails_closed():
    nodes = ("B", "D", "C")
    config = ComputeConfig(
        seed=32,
        horizon_seconds=4.2,
        rates={node: 32 for node in nodes},
        members=nodes,
        target_batch_size=32,
        dimension=4,
        gradient_latency_seconds=0.0,
        state_latency_seconds=5.0,
    )

    result, _ = _run(config, AnnouncementPolicy("honest"),
                     _constant_gradient({node: 0 for node in nodes}))

    assert result["unsupported"]
    assert result["unsupported_reasons"] == [
        "prior_state_round_in_progress_at_optimizer_boundary"
    ]
    unsupported = next(event for event in result["trace"]
                       if event["kind"] == "unsupported_behavior")
    assert unsupported["data"]["state_rendezvous_started"] is True


def test_overlap_with_unfinished_gradient_round_stops_as_unsupported():
    nodes = ("B", "D", "A", "C")
    config = ComputeConfig(
        seed=4,
        horizon_seconds=6.0,
        rates={"B": 32, "D": 32, "A": 8, "C": 32},
        members=nodes,
        target_batch_size=32,
        dimension=4,
        gradient_latency_seconds=0.0,
        state_latency_seconds=0.1,
    )

    result, _ = _run(config, AnnouncementPolicy("membership"),
                     _constant_gradient({node: 1 for node in nodes}))

    assert result["unsupported"]
    assert "overlapping_gradient_rounds" in result["unsupported_reasons"]
    unsupported = [event for event in result["trace"]
                   if event["kind"] == "unsupported_behavior"]
    assert unsupported[0]["data"]["reason"] == "overlapping_gradient_rounds"
    assert result["evaluations"][-1]["time"] < config.horizon_seconds
