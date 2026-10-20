#!/usr/bin/env python3
"""Generate a deterministic fixture from the pinned local Agora implementation.

This imports the installed pinned code and runs pure CPU operations only. It does
not start DHT nodes, open sockets, or contact a public network. The local Agora
runtime's Python symlink may be unavailable after temporary interpreter cleanup;
the current simulator Python works when its ABI is CPython 3.11, because the
fixture generator adds the pinned runtime's site-packages explicitly.
"""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[2]
AGORA_ROOT = ROOT / "research/agora/upstream"
HIVEMIND_ROOT = ROOT / "research/agora/.runtime/hivemind-source"
NATIVE_SITE = ROOT / "research/agora/.runtime/venv/lib/python3.11/site-packages"
AGORA_SOURCE = AGORA_ROOT / "agora_server/src"
PREPARED = ROOT / "research/agora/.runtime/prepared.json"
OUTPUT = ROOT / "research/simulator/fixtures/agora_native.json"

EXPECTED_AGORA_COMMIT = "37d29a966528675635161007d7b43388012a98b2"
EXPECTED_HIVEMIND_COMMIT = "4d5c41495be082490ea44cce4e9dd58f9926bb4e"
PART_SIZE_BYTES = 2 * 2**20
ELEMENT_BYTES = 4
TENSOR_ELEMENTS = [1_048_576] * 16
SPARSE_FRACTION = 0.05
SPARSE_SEED = 1337
ROSTER_SEED = 20261020

AGORA_FILES = (
    "agora_server/src/agora_server/core/all_reduce/matchmaking.py",
    "agora_server/src/agora_server/core/all_reduce/partition.py",
    "agora_server/src/agora_server/core/all_reduce/ar_runner.py",
    "agora_server/src/agora_server/core/averaging/averager.py",
    "agora_server/src/agora_server/core/averaging/state_averager_sparta.py",
)
HIVEMIND_FILES = ("hivemind/averaging/load_balancing.py",)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def git_head(path: Path) -> str:
    return subprocess.check_output(
        ["git", "-C", str(path), "rev-parse", "HEAD"], text=True
    ).strip()


def git_file(commit: str, rel: str) -> bytes:
    return subprocess.check_output(["git", "show", f"{commit}:{rel}"], cwd=ROOT)


def native_imports():
    """Import native packages without allowing the research fork to shadow them."""
    if not NATIVE_SITE.is_dir() or not AGORA_SOURCE.is_dir():
        raise RuntimeError("Pinned Agora runtime/source tree is missing")

    # This script can be launched from the repository root. Remove the root from
    # import resolution and load only the installed pinned packages below.
    os.chdir("/tmp")
    sys.path[:] = [
        entry
        for entry in sys.path
        if entry and Path(entry).resolve() != ROOT.resolve()
    ]
    sys.path.insert(0, str(AGORA_SOURCE))
    sys.path.insert(0, str(NATIVE_SITE))

    import torch
    from agora_server.core.all_reduce.ar_runner import AveragingMode
    from agora_server.core.all_reduce.matchmaking import GroupKeyManager
    from agora_server.core.all_reduce.partition import TensorPartContainer
    from agora_server.core.averaging.state_averager_sparta import PartitionedIndexSelector
    from hivemind.averaging.load_balancing import load_balance_peers
    from hivemind.compression import NoCompression
    from hivemind.p2p import PeerID

    return (
        torch,
        AveragingMode,
        GroupKeyManager,
        TensorPartContainer,
        PartitionedIndexSelector,
        load_balance_peers,
        NoCompression,
        PeerID,
    )


def source_provenance() -> tuple[dict, dict[str, str]]:
    agora_commit = git_head(AGORA_ROOT)
    prepared = json.loads(PREPARED.read_text())
    # The Hivemind runtime source is a `git archive` snapshot, so it has no
    # nested .git directory. Its commit is pinned in prepared.json and is
    # verified against the parent repository's retained upstream Git object.
    hivemind_commit = prepared.get("hivemind_commit")
    if agora_commit != EXPECTED_AGORA_COMMIT:
        raise RuntimeError(f"Unexpected Agora commit: {agora_commit}")
    if hivemind_commit != EXPECTED_HIVEMIND_COMMIT:
        raise RuntimeError(f"Unexpected Hivemind commit: {hivemind_commit}")
    if prepared.get("agora_commit") != agora_commit:
        raise RuntimeError("Agora commit does not match .runtime/prepared.json")
    subprocess.run(
        ["git", "cat-file", "-e", f"{hivemind_commit}^{{commit}}"],
        cwd=ROOT,
        check=True,
        stdout=subprocess.DEVNULL,
    )

    files: dict[str, str] = {}
    for rel in AGORA_FILES:
        files[f"research/agora/upstream/{rel}"] = sha256(AGORA_ROOT / rel)
    for rel in HIVEMIND_FILES:
        snapshot = HIVEMIND_ROOT / rel
        pinned = git_file(hivemind_commit, rel)
        if snapshot.read_bytes() != pinned:
            raise RuntimeError(f"Hivemind runtime source does not match pinned commit: {rel}")
        files[f"research/agora/.runtime/hivemind-source/{rel}"] = sha256(snapshot)

    # The runnable environment contains installed copies of these files. Refuse
    # to generate a fixture if it differs from the pinned source checkout.
    for rel, source_root in (
        *((rel, AGORA_ROOT) for rel in AGORA_FILES),
        *((rel, HIVEMIND_ROOT) for rel in HIVEMIND_FILES),
    ):
        source = source_root / rel
        installed = NATIVE_SITE / rel.removeprefix("agora_server/src/")
        if rel.startswith("hivemind/"):
            installed = NATIVE_SITE / rel
        if not installed.is_file() or sha256(source) != sha256(installed):
            raise RuntimeError(f"Installed native file does not match pinned source: {rel}")

    return {
        "agora_commit": agora_commit,
        "hivemind_commit": hivemind_commit,
        "prepared_lock_sha256": prepared.get("lock_sha256"),
        "source_sha256": files,
    }, files


def selector_fixture(torch, PartitionedIndexSelector) -> dict:
    def capture_sizes(sizes: list[int], seed: int) -> dict:
        torch.manual_seed(seed)
        partitions_by_tensor = []
        counts_by_tensor = []
        for tensor_elements in sizes:
            parameter = torch.zeros(tensor_elements, dtype=torch.float32)
            selector = PartitionedIndexSelector(SPARSE_FRACTION, parameter)
            num_partitions = selector.state[parameter]["num_partitions"]
            epochs = list(range(num_partitions + 2))
            counts = [
                int(selector.get_indices(parameter, epoch).sum().item())
                for epoch in epochs
            ]
            partitions_by_tensor.append(num_partitions)
            counts_by_tensor.append(counts)
            del selector, parameter
        return {
            "tensor_elements": sizes,
            "num_partitions_by_tensor": partitions_by_tensor,
            "epochs_by_tensor": [list(range(count + 2)) for count in partitions_by_tensor],
            "partition_index_by_epoch": [
                [epoch % count for epoch in range(count + 2)]
                for count in partitions_by_tensor
            ],
            "selected_elements_by_epoch": counts_by_tensor,
            "count_sums_by_tensor": [sum(row[:parts]) for row, parts in zip(counts_by_tensor, partitions_by_tensor)],
            "seed": seed,
        }

    return {
        "fraction": SPARSE_FRACTION,
        "partition_rule": "min(ceil(1/fraction), tensor.numel())",
        "order_rule": "partition = epoch % num_partitions",
        "size_rule": "Torch random permutation labels elements modulo num_partitions; each partition has floor/ceil cardinality",
        "small_uneven": capture_sizes([1037], SPARSE_SEED),
        "small_capped": capture_sizes([7], SPARSE_SEED),
        "pilot": capture_sizes(TENSOR_ELEMENTS, SPARSE_SEED),
    }


def tensor_assignment_fixture(
    torch,
    AveragingMode,
    TensorPartContainer,
    load_balance_peers,
    NoCompression,
    PeerID,
    tensor_cardinalities: list[int],
    population: int,
    reducer_count: int,
    *,
    dense_tensor_elements: list[int] | None = None,
    fraction_policy: str = "native_load_balance",
    part_size_bytes: int = PART_SIZE_BYTES,
    role_order: list[str] | None = None,
    ordered_peer_ids: bool = False,
    fixture_basis: str | None = None,
) -> dict:
    if population < reducer_count or reducer_count <= 0:
        raise ValueError("Invalid role counts")
    assigned_roles = role_order or [
        "reducer" if index < reducer_count else "contributor"
        for index in range(population)
    ]
    if len(assigned_roles) != population or assigned_roles.count("reducer") != reducer_count:
        raise ValueError("role_order must match population and reducer_count")

    logical_peers = []
    for index in range(population):
        peer_bytes = (
            index.to_bytes(20, "big")
            if ordered_peer_ids
            else hashlib.sha256(
                f"agora-fixture:{ROSTER_SEED}:{population}:{index}".encode()
            ).digest()
        )
        logical_peers.append(
            {
                "logical_id": f"N{index:04d}",
                "role": assigned_roles[index],
                "peer_id_hex": peer_bytes.hex(),
                "peer": PeerID(peer_bytes),
            }
        )

    # Match native Matchmaking's raw-byte ordering for its independently formed
    # local roster. The synthetic IDs are only fixture identities.
    roster = sorted(logical_peers, key=lambda item: bytes.fromhex(item["peer_id_hex"]))
    roles = [item["role"] for item in roster]
    modes = [
        AveragingMode.NODE if role == "reducer" else AveragingMode.CLIENT
        for role in roles
    ]
    dense_tensor_elements = list(
        TENSOR_ELEMENTS if dense_tensor_elements is None else dense_tensor_elements
    )
    dense_total_elements = sum(dense_tensor_elements)
    bandwidths = [None if role == "reducer" else 0.0 for role in roles]
    # Sparta computes the reducer fractions against the dense model size, then
    # passes compact sparse tensors to TensorPartContainer. The fractions are
    # ratios, so TPC normalizes them against the sparse value count.
    if fraction_policy == "native_load_balance":
        fractions = tuple(load_balance_peers(dense_total_elements, bandwidths, 0))
    elif fraction_policy == "harness_equal_reducers":
        fractions = tuple(1 / reducer_count if role == "reducer" else 0.0 for role in roles)
    else:
        raise ValueError(f"Unknown fraction policy: {fraction_policy}")
    tensor_values = [
        torch.zeros(size, dtype=torch.float32) for size in tensor_cardinalities
    ]
    peers = [item["peer"] for item in roster]
    container = TensorPartContainer(
        tensor_values,
        fractions,
        peers,
        compression=NoCompression(),
        part_size_bytes=part_size_bytes,
    )

    assignments = []
    for peer_index, peer_parts in enumerate(container._input_parts_by_peer):
        for part, compression_info in peer_parts:
            assignments.append(
                {
                    "tensor_index": int(compression_info.key),
                    "part_index": int(compression_info.part_index),
                    "peer_index": peer_index,
                    "peer_logical_id": roster[peer_index]["logical_id"],
                    "peer_role": roles[peer_index],
                    "part_elements": int(part.numel()),
                }
            )
    assignments.sort(key=lambda item: (item["tensor_index"], item["part_index"]))

    modes_by_peer = {
        roster[index]["logical_id"]: modes[index].name for index in range(population)
    }
    weights_by_peer = {item["logical_id"]: 1.0 for item in roster}
    parts_by_peer = {
        roster[index]["logical_id"]: int(count)
        for index, count in enumerate(container.num_parts_by_peer)
    }
    result = {
        "population": population,
        "reducer_count": reducer_count,
        "contributor_count": population - reducer_count,
        "roster_order": [
            {
                "peer_id_hex": item["peer_id_hex"],
                "logical_id": item["logical_id"],
                "role": item["role"],
                "mode": modes_by_peer[item["logical_id"]],
                "weight": weights_by_peer[item["logical_id"]],
            }
            for item in roster
        ],
        "fraction_policy": fraction_policy,
        "fixture_basis": fixture_basis,
        "dense_tensor_elements": dense_tensor_elements,
        "dense_total_elements": dense_total_elements,
        "compact_sparse_tensor_elements": tensor_cardinalities,
        "compact_sparse_total_elements": sum(tensor_cardinalities),
        "element_bytes": ELEMENT_BYTES,
        "part_size_bytes": part_size_bytes,
        "part_elements": part_size_bytes // ELEMENT_BYTES,
        "peer_fractions_input": {
            roster[index]["logical_id"]: fractions[index]
            for index in range(population)
        },
        "parts_by_peer": parts_by_peer,
        "assignments": assignments,
        "native_total_parts": sum(container.num_parts_by_tensor),
    }
    container.finalize()
    return result


def main() -> None:
    provenance, _ = source_provenance()
    (
        torch,
        AveragingMode,
        GroupKeyManager,
        TensorPartContainer,
        PartitionedIndexSelector,
        load_balance_peers,
        NoCompression,
        PeerID,
    ) = native_imports()

    prefix = "agorasim_avg"
    manager = GroupKeyManager.__new__(GroupKeyManager)
    manager._key_prefix = prefix
    stage_key = prefix.split("_")[0] + ".0."
    step_key = GroupKeyManager.key_for_step(manager, 41)

    torch.set_num_threads(1)
    sparse = selector_fixture(torch, PartitionedIndexSelector)
    selected_cardinalities = [
        row[0] for row in sparse["pilot"]["selected_elements_by_epoch"]
    ]
    if len(selected_cardinalities) != len(TENSOR_ELEMENTS):
        raise AssertionError("Native sparse selector fixture tensor count changed")
    document = {
        "schema_version": 1,
        "provenance": provenance,
        "native_versions": {
            "python": sys.version.split()[0],
            "torch": torch.__version__,
            "numpy": importlib.metadata.version("numpy"),
            "hivemind": importlib.metadata.version("hivemind"),
            "agora_server": importlib.metadata.version("agora-server"),
        },
        "scope": {
            "description": "Deterministic CPU-only mechanics from the pinned local Agora/Hivemind code. No DHT or network process is started.",
            "representative_harness": "research/agora/baseline_8b.py",
            "sparse_fraction": SPARSE_FRACTION,
            "tensor_elements": TENSOR_ELEMENTS,
            "dtype": "float32",
            "part_size_bytes": PART_SIZE_BYTES,
            "synthetic_peer_id_seed": ROSTER_SEED,
        },
        "matchmaking_keys": {
            "stage_prefix": "agorasim",
            "averager_prefix": prefix,
            "stage_population_key": stage_key,
            "step_key_global_step_41": step_key,
            "first_training_average": {
                "completed_local_step": 20,
                "round_number": 1,
                "sparse_partition": 1,
                "matchmaking_global_step": 20,
                "matchmaking_step_key": GroupKeyManager.key_for_step(manager, 20),
                "source_rule": "baseline_8b.py calls average(step//20, step); its sparse selector uses round_number while matchmaking uses epoch as global_step",
            },
        },
        "sparse_selector": sparse,
        "tensor_assignment": {
            "native_allocation_rule": "load_balance_peers(dense_total_elements, bandwidths) with None for NODE, 0 for CLIENT; with no positive specified bandwidth only NODE receives equal score",
            "native_sparse_path": "SPARTA compacts selected coordinates into one tensor per source tensor before TensorPartContainer chunks them; the dense-derived fractions are normalized over the compact sparse total",
            "client_rule": "CLIENT fraction is zero, but NODE and CLIENT weights are both one and both are senders",
            "scenarios": {
                "8_peers_4_reducers_4_contributors": tensor_assignment_fixture(
                    torch, AveragingMode, TensorPartContainer, load_balance_peers,
                    NoCompression, PeerID, selected_cardinalities, 8, 4,
                ),
                "100_peers_4_reducers_96_contributors": tensor_assignment_fixture(
                    torch, AveragingMode, TensorPartContainer, load_balance_peers,
                    NoCompression, PeerID, selected_cardinalities, 100, 4,
                ),
                "native_harness_8_peers_five_scalar_tensors": tensor_assignment_fixture(
                    torch, AveragingMode, TensorPartContainer, load_balance_peers,
                    NoCompression, PeerID, [1] * 5, 8, 4,
                    dense_tensor_elements=[1] * 5,
                    fraction_policy="harness_equal_reducers",
                    role_order=[
                        "contributor", "reducer", "reducer", "contributor",
                        "reducer", "contributor", "contributor", "reducer",
                    ],
                    ordered_peer_ids=True,
                    fixture_basis=(
                        "research/agora/test_8b_baseline.py::"
                        "HarnessTests.test_scalar_parts_exercise_four_reducers_in_interleaved_order"
                    ),
                ),
                "native_load_balance_uneven_multichunk_tie": tensor_assignment_fixture(
                    torch, AveragingMode, TensorPartContainer, load_balance_peers,
                    NoCompression, PeerID, [5, 1, 4], 8, 4,
                    # Keep dense total divisible by the four equal NODE shares;
                    # compact sizes are deliberately uneven to exercise global
                    # pivots across tensor-local chunk boundaries.
                    dense_tensor_elements=[8, 4, 8],
                    part_size_bytes=2 * ELEMENT_BYTES,
                ),
            },
        },
    }
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")
    print(f"Wrote {OUTPUT.relative_to(ROOT)}")
    print(f"Agora {provenance['agora_commit']} / Hivemind {provenance['hivemind_commit']}")


if __name__ == "__main__":
    main()
