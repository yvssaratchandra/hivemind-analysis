from trainsim.analysis.report import analyze_trace
from trainsim.analysis.progress import bootstrap_summary
from trainsim.observer import Observer


def _work(work_id, origin, amount=1.0):
    return {
        "work_id": work_id,
        "origin": origin,
        "sequence": int(work_id.rsplit(":", 1)[1]),
        "compute_start": 0.0,
        "compute_finish": 1.0,
        "amount": amount,
        "payload_bytes": 16,
    }


def _roster():
    return {
        "group_id": "group-1",
        "leader": "N0000",
        "members": ["N0000", "N0001", "N0002", "N0003"],
        "work_ids": ["N0000:0", "N0001:0", "N0002:0", "N0003:0"],
        "formation_time": 2.0,
    }


def _receipt(partition, contributors, fallback=False):
    return {
        "group_id": "group-1",
        "recipient": "N0001",
        "partition": partition,
        "size_bytes": 4,
        "contributors": contributors,
        "normalized_weights": [1.0 / len(contributors)] * len(contributors),
        "fallback": fallback,
    }


def _result(receipts, status="complete", recipient="N0001"):
    return {
        "recipient": recipient,
        "group_id": "group-1",
        "status": status,
        "receipts": receipts,
        "expected_bytes": 16,
        "finish_time": 10.0,
    }


def _setup(observer):
    for index in range(4):
        node = f"N{index:04d}"
        observer.emit(1.0, "work_ready", node, work=_work(f"{node}:0", node))
    observer.emit(2.0, "group_formed", "N0000", roster=_roster())


def test_full_work_credit_requires_all_coordinates_at_one_remote_recipient():
    observer = Observer(labels={"5pct": {"N0002"}})
    observer.target_work = 1.0
    _setup(observer)

    # Two recipients each see only half the tensor. Their partial coverage cannot
    # be stitched together into a full contribution.
    observer.emit(10.0, "aggregate_result", "N0001", result=_result([
        _receipt(0, ["N0000:0"]), _receipt(1, ["N0000:0"]),
    ], status="partial", recipient="N0001"))
    observer.emit(11.0, "aggregate_result", "N0002", result=_result([
        _receipt(2, ["N0000:0"]), _receipt(3, ["N0000:0"]),
    ], status="partial", recipient="N0002"))
    assert observer.accepted_work == 0
    assert observer.target_time is None

    complete = _result([_receipt(i, ["N0000:0", "N0002:0"]) for i in range(4)])
    observer.emit(12.0, "aggregate_result", "N0001", result=complete)
    observer.emit(13.0, "aggregate_result", "N0001", result=complete)  # duplicate
    summary = observer.summary()
    assert summary["accepted_work"] == 2.0
    assert summary["accepted_work_items"] == 2
    assert summary["target_time"] == 10.0
    assert summary["partial_occurrences"] == 2
    assert summary["per_node"][0]["accepted_work"] == 1.0


def test_fallback_and_local_contribution_never_get_full_remote_credit():
    observer = Observer()
    _setup(observer)
    receipts = [_receipt(i, ["N0000:0", "N0001:0"], fallback=(i == 3)) for i in range(4)]
    observer.emit(10.0, "aggregate_result", "N0001", result=_result(receipts))
    summary = observer.summary()
    assert summary["accepted_work"] == 0
    assert summary["partial_occurrences"] == 1
    assert summary["per_node"][0]["direct_aggregation_weight_mean"] == 0.5


def test_varying_chunk_membership_is_preserved_and_only_full_contributors_get_credit():
    observer = Observer()
    _setup(observer)
    receipts = [
        _receipt(0, ["N0000:0", "N0002:0"]),
        _receipt(1, ["N0000:0", "N0002:0"]),
        _receipt(2, ["N0000:0"]),
        _receipt(3, ["N0000:0"]),
    ]
    observer.emit(10.0, "aggregate_result", "N0001", result=_result(receipts))
    summary = observer.summary()
    assert summary["accepted_work"] == 1.0
    assert summary["partial_occurrences"] == 1
    assert summary["effective_partition_count"] == 4
    assert summary["effective_set_count"] == 2
    assert sorted(row["partition_count"] for row in summary["effective_sets"]) == [2, 2]


def test_full_credit_rejects_sparse_chunk_indices_and_invalid_weights():
    observer = Observer()
    _setup(observer)
    sparse = [_receipt(i, ["N0000:0"]) for i in (0, 1, 2, 9)]
    observer.emit(10.0, "aggregate_result", "N0001", result=_result(sparse))

    invalid_weights = [_receipt(i, ["N0000:0"]) for i in range(4)]
    invalid_weights[2]["normalized_weights"] = [1.1]
    observer.emit(11.0, "aggregate_result", "N0001", result=_result(invalid_weights))

    for offset, weights in enumerate(([], [float("nan")], [-0.1]), 12):
        malformed = [_receipt(i, ["N0000:0"]) for i in range(4)]
        malformed[1]["normalized_weights"] = weights
        observer.emit(float(offset), "aggregate_result", "N0001", result=_result(malformed))

    incorrect_bytes = [_receipt(i, ["N0000:0"]) for i in range(4)]
    incorrect_bytes[-1]["size_bytes"] = 3
    observer.emit(15.0, "aggregate_result", "N0001", result=_result(incorrect_bytes))
    assert observer.accepted_work == 0


def test_observation_window_and_starvation_tail_are_reported():
    observer = Observer()
    observer.target_work = 1
    observer.emit(0.0, "run_started", "", config={"population": 4, "observation_seconds": 5},
                  target_work=1)
    _setup(observer)
    observer.emit(5.0, "observation_boundary")
    observer.emit(10.0, "aggregate_result", "N0001", result=_result([
        _receipt(i, ["N0000:0"]) for i in range(4)
    ]))
    observer.emit(12.0, "run_finished", status="completed")
    summary = observer.summary()
    assert summary["accepted_work_at_observation"] == 0
    assert summary["first_accept_time"] == 10.0
    assert summary["last_time"] == 12.0
    assert summary["per_node"][0]["longest_no_acceptance_seconds"] == 10.0
    assert summary["per_node"][1]["longest_no_acceptance_seconds"] == 12.0


def test_observation_window_excludes_boundary_credit_independent_of_event_order(tmp_path):
    results = []
    for index, marker_first in enumerate((False, True)):
        trace = tmp_path / f"boundary-{index}.jsonl.gz"
        observer = Observer(trace)
        observer.emit(0.0, "run_started", "", config={"population": 4, "observation_seconds": 5},
                      target_work=1.0)
        _setup(observer)
        complete = _result([_receipt(i, ["N0000:0"]) for i in range(4)])
        if marker_first:
            observer.emit(5.0, "observation_boundary")
            observer.emit(5.0, "aggregate_result", "N0001", result=complete)
        else:
            observer.emit(5.0, "aggregate_result", "N0001", result=complete)
            observer.emit(5.0, "observation_boundary")
        observer.emit(6.0, "run_finished", status="completed")
        summary = observer.summary()
        observer.close()
        replay = analyze_trace(trace)["summary"]
        assert summary["accepted_work"] == 1.0
        assert summary["accepted_work_at_observation"] == 0.0
        assert replay["accepted_work"] == summary["accepted_work"]
        assert replay["accepted_work_at_observation"] == 0.0
        results.append(summary["accepted_work_at_observation"])

    assert results == [0.0, 0.0]


def test_group_concentration_exclusion_and_krum_count_diagnostic():
    observer = Observer(labels={"5pct": {"N0007"}, "10pct": {"N0005", "N0006", "N0007"}})
    observer.emit(0.0, "run_started", "", config={"population": 8}, target_work=100,
                  labels={"5pct": ["N0007"], "10pct": ["N0005", "N0006", "N0007"]})
    observer.emit(2.0, "group_formed", "N0000", roster={
        "group_id": "group-8", "leader": "N0000",
        "members": [f"N{i:04d}" for i in range(8)],
        "work_ids": [f"N{i:04d}:0" for i in range(8)],
    })
    summary = observer.summary()
    group = summary["groups"][0]
    assert group["controlled_counts"] == {"5pct": 1, "10pct": 3}
    assert group["mixing"]["5pct"]["mixed"] is True
    assert group["mixing"]["10pct"]["controlled_only"] is False
    assert group["krum_count_condition_holds"] == {"5pct": True, "10pct": False}
    assert summary["zero_participation_nodes"] == 8


def test_jsonl_gzip_replay_matches_online_unique_work_summary(tmp_path):
    trace = tmp_path / "events.jsonl.gz"
    labels = {"5pct": {"N0002"}}
    observer = Observer(trace, labels=labels)
    observer.target_work = 1.0
    observer.emit(0.0, "run_started", "", config={"population": 4, "observation_seconds": 5}, target_work=1.0,
                  labels={"5pct": ["N0002"]})
    _setup(observer)
    observer.emit(5.0, "observation_boundary")
    observer.emit(10.0, "aggregate_result", "N0001", result=_result([
        _receipt(i, ["N0000:0"]) for i in range(4)
    ]))
    online = observer.summary()
    observer.close()

    replay = analyze_trace(trace, labels={"5pct": ["N0002"]}, target_work=1.0)
    assert replay["summary"]["accepted_work"] == online["accepted_work"] == 1.0
    assert replay["summary"]["target_time"] == online["target_time"] == 10.0
    assert replay["summary"]["event_count"] == online["event_count"]
    assert replay["summary"]["accepted_work_at_observation"] == online["accepted_work_at_observation"] == 0
    assert len(replay["per_node"]) == 4
    assert replay["groups"][0]["group_id"] == "group-1"


def test_bootstrap_summarizes_seed_level_metrics_deterministically():
    runs = [
        {"target_time": 10.0, "accepted_work": 8.0},
        {"target_time": 20.0, "accepted_work": 9.0},
        {"target_time": None, "accepted_work": 10.0},
    ]
    first = bootstrap_summary(runs, metrics=["target_time", "accepted_work"], resamples=2000, seed=7)
    second = bootstrap_summary(runs, metrics=["target_time", "accepted_work"], resamples=2000, seed=7)
    assert first == second
    assert first["target_time"]["n"] == 2
    assert first["target_time"]["median"] == 15.0
    assert first["target_time"]["iqr"] == [12.5, 17.5]
    assert first["accepted_work"]["n"] == 3
