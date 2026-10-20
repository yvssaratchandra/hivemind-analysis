"""Deterministic mechanics checked against pinned Agora source and fixture."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import subprocess

import pytest

from trainsim.protocols.agora.partitioning import (
    assign_tensor_parts,
    reducer_fractions,
    sparse_cardinalities,
    sparse_partition_cardinality,
    stage_namespace,
    stage_record_key,
    step_key,
)


ROOT = Path(__file__).resolve().parents[3]
FIXTURE_PATH = ROOT / "research/simulator/fixtures/agora_native.json"
FIXTURE = json.loads(FIXTURE_PATH.read_text())


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def test_fixture_sources_match_pinned_native_checkout():
    provenance = FIXTURE["provenance"]
    agora_checkout = ROOT / "research/agora/upstream"
    runtime_lock = ROOT / "research/agora/.runtime/prepared.json"
    if not agora_checkout.is_dir() or not runtime_lock.is_file():
        pytest.skip("local pinned Agora checkout/runtime is needed to recheck fixture provenance")

    assert subprocess.check_output(
        ["git", "-C", str(agora_checkout), "rev-parse", "HEAD"],
        text=True,
    ).strip() == provenance["agora_commit"]

    prepared = json.loads(
        (ROOT / "research/agora/.runtime/prepared.json").read_text()
    )
    assert prepared["hivemind_commit"] == provenance["hivemind_commit"]
    assert prepared["lock_sha256"] == provenance["prepared_lock_sha256"]

    native_paths = [ROOT / relative_path for relative_path in provenance["source_sha256"]]
    if any(not path.is_file() for path in native_paths):
        pytest.skip("one or more local pinned native source files are unavailable")
    for relative_path, expected_hash in provenance["source_sha256"].items():
        assert sha256_bytes((ROOT / relative_path).read_bytes()) == expected_hash

    pinned_hivemind_file = subprocess.check_output(
        [
            "git",
            "show",
            f"{provenance['hivemind_commit']}:hivemind/averaging/load_balancing.py",
        ],
        cwd=ROOT,
    )
    assert sha256_bytes(pinned_hivemind_file) == provenance["source_sha256"][
        "research/agora/.runtime/hivemind-source/hivemind/averaging/load_balancing.py"
    ]


def test_stage_and_step_key_helpers_match_pinned_agora_fixture():
    keys = FIXTURE["matchmaking_keys"]
    assert stage_namespace(keys["stage_prefix"]) == keys["stage_population_key"]
    assert stage_record_key(keys["stage_prefix"], 4) == "agorasim.0.4"
    assert step_key(
        keys["averager_prefix"], 41
    ) == keys["step_key_global_step_41"]
    first_average = keys["first_training_average"]
    assert first_average["round_number"] == 1
    assert first_average["sparse_partition"] == 1
    assert step_key(
        keys["averager_prefix"], first_average["matchmaking_global_step"]
    ) == first_average["matchmaking_step_key"]


def test_sparse_cardinalities_and_epoch_rotation_match_native_selector():
    selector = FIXTURE["sparse_selector"]
    requested_partitions = int(1 / selector["fraction"])
    for scenario_name in ("small_uneven", "small_capped", "pilot"):
        scenario = selector[scenario_name]
        sizes = scenario["tensor_elements"]
        for tensor_index, num_partitions in enumerate(
            scenario["num_partitions_by_tensor"]
        ):
            counts = scenario["selected_elements_by_epoch"][tensor_index]
            partition_ids = scenario["partition_index_by_epoch"][tensor_index]
            assert scenario["count_sums_by_tensor"][tensor_index] == sizes[
                tensor_index
            ]
            assert len(counts) == num_partitions + 2
            for epoch, native_count in enumerate(counts):
                assert partition_ids[epoch] == epoch % num_partitions
                assert native_count == sparse_partition_cardinality(
                    sizes[tensor_index],
                    requested_partitions,
                    epoch,
                )
                assert sparse_cardinalities(
                    sizes,
                    requested_partitions,
                    epoch,
                )[tensor_index] == native_count


def test_role_fractions_and_sparse_chunks_match_native_container_for_8_and_100():
    fixture = FIXTURE["tensor_assignment"]
    selector = FIXTURE["sparse_selector"]["pilot"]
    selected_epoch_zero = [
        counts[0] for counts in selector["selected_elements_by_epoch"]
    ]

    for scenario_name in (
        "8_peers_4_reducers_4_contributors",
        "100_peers_4_reducers_96_contributors",
    ):
        scenario = fixture["scenarios"][scenario_name]
        roster = scenario["roster_order"]
        assert len(roster) == scenario["population"]
        roles = [peer["mode"] for peer in roster]
        expected_fractions = reducer_fractions(roles)
        assert all(peer["weight"] == 1.0 for peer in roster)

        native_fractions = tuple(
            scenario["peer_fractions_input"][peer["logical_id"]]
            for peer in roster
        )
        fraction_sum = sum(native_fractions)
        assert fraction_sum > 0
        normalized_native_fractions = tuple(
            value / fraction_sum for value in native_fractions
        )
        assert normalized_native_fractions == pytest.approx(expected_fractions)

        assert scenario["compact_sparse_tensor_elements"] == selected_epoch_zero
        expected_parts = assign_tensor_parts(
            scenario["compact_sparse_tensor_elements"],
            expected_fractions,
            scenario["part_size_bytes"],
            scenario["element_bytes"],
        )
        native_fraction_parts = assign_tensor_parts(
            scenario["compact_sparse_tensor_elements"],
            native_fractions,
            scenario["part_size_bytes"],
            scenario["element_bytes"],
        )
        actual_parts = tuple(
            (
                row["tensor_index"],
                row["part_index"],
                row["peer_index"],
                row["part_elements"],
            )
            for row in scenario["assignments"]
        )
        assert actual_parts == expected_parts == native_fraction_parts
        assert scenario["native_total_parts"] == len(expected_parts) == 16
        assert all(
            count == 0
            for logical_id, count in scenario["parts_by_peer"].items()
            if next(peer["mode"] for peer in roster if peer["logical_id"] == logical_id)
            == "CLIENT"
        )
        reducer_part_counts = [
            count
            for logical_id, count in scenario["parts_by_peer"].items()
            if next(peer["mode"] for peer in roster if peer["logical_id"] == logical_id)
            == "NODE"
        ]
        assert sorted(reducer_part_counts) == [4, 4, 4, 4]


def test_native_harness_five_scalar_tensors_for_eight_peers():
    scenario = FIXTURE["tensor_assignment"]["scenarios"][
        "native_harness_8_peers_five_scalar_tensors"
    ]
    roster = scenario["roster_order"]
    roles = [peer["mode"] for peer in roster]
    expected_fractions = reducer_fractions(roles)
    native_fractions = tuple(
        scenario["peer_fractions_input"][peer["logical_id"]] for peer in roster
    )
    assert [peer["mode"] for peer in roster] == [
        "CLIENT", "NODE", "NODE", "CLIENT", "NODE", "CLIENT", "CLIENT", "NODE"
    ]
    assert native_fractions == (0, 0.25, 0.25, 0, 0.25, 0, 0, 0.25)
    assert scenario["fixture_basis"] == (
        "research/agora/test_8b_baseline.py::"
        "HarnessTests.test_scalar_parts_exercise_four_reducers_in_interleaved_order"
    )
    expected = assign_tensor_parts(
        [1] * 5,
        expected_fractions,
        scenario["part_size_bytes"],
        scenario["element_bytes"],
    )
    native_fraction_assignment = assign_tensor_parts(
        [1] * 5,
        native_fractions,
        scenario["part_size_bytes"],
        scenario["element_bytes"],
    )
    actual = tuple(
        (row["tensor_index"], row["part_index"], row["peer_index"], row["part_elements"])
        for row in scenario["assignments"]
    )
    assert scenario["population"] == 8
    assert scenario["reducer_count"] == 4
    assert scenario["compact_sparse_tensor_elements"] == [1] * 5
    assert actual == expected == native_fraction_assignment
    reducer_parts = [
        count
        for logical_id, count in scenario["parts_by_peer"].items()
        if next(peer["mode"] for peer in roster if peer["logical_id"] == logical_id) == "NODE"
    ]
    assert sorted(reducer_parts) == [1, 1, 1, 2]


def test_native_uneven_multichunk_boundaries_and_tie_breaking():
    scenario = FIXTURE["tensor_assignment"]["scenarios"][
        "native_load_balance_uneven_multichunk_tie"
    ]
    roster = scenario["roster_order"]
    roles = [peer["mode"] for peer in roster]
    native_fractions = tuple(
        scenario["peer_fractions_input"][peer["logical_id"]] for peer in roster
    )
    expected = assign_tensor_parts(
        [5, 1, 4],
        reducer_fractions(roles),
        scenario["part_size_bytes"],
        scenario["element_bytes"],
    )
    native_fraction_assignment = assign_tensor_parts(
        [5, 1, 4], native_fractions, scenario["part_size_bytes"], scenario["element_bytes"]
    )
    actual = tuple(
        (row["tensor_index"], row["part_index"], row["peer_index"], row["part_elements"])
        for row in scenario["assignments"]
    )
    assert scenario["dense_total_elements"] == 20
    assert scenario["compact_sparse_tensor_elements"] == [5, 1, 4]
    assert scenario["part_size_bytes"] == 8
    assert actual == expected == native_fraction_assignment
    assert actual == (
        (0, 0, 1, 2),
        (0, 1, 2, 2),
        (0, 2, 2, 1),
        (1, 0, 5, 1),  # each tensor begins a new chunk sequence
        (2, 0, 5, 2),  # equal pivot overlap; native numpy.argmax keeps roster index 5
        (2, 1, 7, 2),
    )
