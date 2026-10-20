"""Pure helpers for Agora stage keys, sparse selection, and tensor-part ownership.

The sparse selector in the pinned server uses a seeded Torch permutation of
the coordinate indices, then assigns each shuffled index by modulo.  This
module models exact partition cardinalities and epoch rotation while leaving
coordinate identities symbolic; the coordinate permutation does not affect
the number of values in each partition.
"""

from __future__ import annotations

import math
from typing import Sequence


def stage_namespace(stage: str) -> str:
    """Return the DHT prefix queried by Agora's stage population count."""
    if not stage or "_" in stage:
        raise ValueError("stage must be a nonempty name without underscores")
    return f"{stage}.0."


def stage_record_key(stage: str, index: int) -> str:
    """Return one per-node stage registration key."""
    if type(index) is not int or index < 0:
        raise ValueError("stage registration index must be a nonnegative integer")
    return f"{stage_namespace(stage)}{index}"


def step_key(prefix: str, global_step: int) -> str:
    """Match the pinned Agora ``GroupKeyManager.key_for_step`` format."""
    if not prefix:
        raise ValueError("step prefix must be nonempty")
    if type(global_step) is not int or global_step < 0:
        raise ValueError("global_step must be a nonnegative integer")
    return f"{prefix}_s{global_step}.0b"


def sparse_partition_cardinality(elements: int, partitions: int, partition: int) -> int:
    """Return a deterministic floor/ceil-sized symbolic sparse partition.

    Native labels are ``permutation % count``.  Since the permutation contains
    every integer from zero through ``elements - 1`` exactly once, the first
    ``elements % count`` labels receive one extra element regardless of the
    permutation.  Coordinate identities remain unmodeled.
    """
    if type(elements) is not int or elements < 0:
        raise ValueError("elements must be a nonnegative integer")
    if type(partitions) is not int or partitions <= 0:
        raise ValueError("partitions must be a positive integer")
    if type(partition) is not int or partition < 0:
        raise ValueError("partition must be a nonnegative integer")
    count = min(partitions, elements)
    if count == 0:
        if partition != 0:
            raise ValueError("empty tensors have only partition 0")
        return 0
    selected = partition % count
    quotient, remainder = divmod(elements, count)
    return quotient + int(selected < remainder)


def sparse_cardinalities(
    tensor_elements: Sequence[int], partitions: int, epoch: int
) -> tuple[int, ...]:
    """Return one selected sparse cardinality per tensor for an epoch."""
    if type(epoch) is not int or epoch < 0:
        raise ValueError("epoch must be a nonnegative integer")
    if not tensor_elements:
        raise ValueError("at least one tensor is required")
    return tuple(
        sparse_partition_cardinality(size, partitions, epoch,)
        for size in tensor_elements
    )


def reducer_fractions(roles: Sequence[str]) -> tuple[float, ...]:
    """Allocate equal reducer fractions; CLIENT peers contribute with weight 1.

    The pinned native baseline harness explicitly uses equal shares across NODE
    reducers and zero partition share for CLIENT peers.  That role-specific
    policy is retained here; it does not infer reducer quality from bandwidth.
    """
    if not roles or any(role not in ("NODE", "CLIENT") for role in roles):
        raise ValueError("roles must be a nonempty sequence of NODE/CLIENT values")
    reducers = sum(role == "NODE" for role in roles)
    if reducers == 0:
        raise ValueError("Agora all-reduce requires at least one NODE reducer")
    share = 1.0 / reducers
    return tuple(share if role == "NODE" else 0.0 for role in roles)


def reducer_allocations(total_elements: int, roles: Sequence[str]) -> tuple[int, ...]:
    """Mirror native ``load_balance_peers`` plus H-B integerization.

    In the baseline, NODEs advertise unspecified download bandwidth and
    CLIENTs advertise zero. The pinned loader therefore assigns equal scores
    to NODEs, zero scores to CLIENTs, then applies the H-B apportionment. This
    returns exact element counts before the tensor container computes pivots.
    """
    if type(total_elements) is not int or total_elements < 0:
        raise ValueError("total_elements must be a nonnegative integer")
    if not roles or any(role not in ("NODE", "CLIENT") for role in roles):
        raise ValueError("roles must be a nonempty sequence of NODE/CLIENT values")
    scores = tuple(1.0 if role == "NODE" else 0.0 for role in roles)
    total_score = sum(scores)
    if total_score == 0:
        raise ValueError("Agora all-reduce requires at least one NODE reducer")
    allocation = [int(total_elements * score / total_score) for score in scores]
    while sum(allocation) < total_elements:
        quotients = [score / (allocation[index] + 1) for index, score in enumerate(scores)]
        allocation[quotients.index(max(quotients))] += 1
    return tuple(allocation)


def assign_tensor_parts(
    tensor_cardinalities: Sequence[int],
    peer_fractions: Sequence[float],
    chunk_bytes: int,
    element_bytes: int = 4,
) -> tuple[tuple[int, int, int, int], ...]:
    """Assign whole per-tensor chunks to peers by their largest pivot overlap.

    This mirrors Agora's vendored ``TensorPartContainer``: peer pivots are
    computed over the concatenated tensor elements, but chunks never cross a
    tensor boundary. A chunk crossing a peer pivot is assigned to the peer with
    the largest overlap; ties go to the earlier roster position, like
    ``numpy.argmax``. Returned rows are ``(tensor_index, chunk_index,
    peer_index, elements)`` in tensor/chunk order.
    """
    if not tensor_cardinalities or any(type(n) is not int or n < 0 for n in tensor_cardinalities):
        raise ValueError("tensor_cardinalities must contain nonnegative integers")
    if not peer_fractions or any(
        isinstance(x, bool) or not isinstance(x, (float, int)) or not math.isfinite(x) or x < 0
        for x in peer_fractions
    ):
        raise ValueError("peer_fractions must be nonempty finite nonnegative values")
    if type(chunk_bytes) is not int or chunk_bytes <= 0:
        raise ValueError("chunk_bytes must be a positive integer")
    if type(element_bytes) is not int or element_bytes <= 0:
        raise ValueError("element_bytes must be a positive integer")
    if any(n % 1 for n in tensor_cardinalities):
        raise ValueError("tensor cardinalities must be integral")
    total = sum(tensor_cardinalities)
    fraction_sum = float(sum(peer_fractions))
    if total and fraction_sum <= 0:
        raise ValueError("nonempty tensors require a positive peer fraction")
    if total == 0:
        return ()

    max_chunk_elements = max(1, chunk_bytes // element_bytes)
    pivots: list[int] = []
    if (
        all(type(value) is int for value in peer_fractions)
        and sum(peer_fractions) == total
    ):
        cumulative_allocation = 0
        for allocation in peer_fractions:
            cumulative_allocation += allocation
            pivots.append(cumulative_allocation)
    else:
        cumulative_fraction = 0.0
        for fraction in peer_fractions:
            cumulative_fraction += float(fraction)
            pivots.append(int(cumulative_fraction / fraction_sum * total))
    pivots[-1] = total

    output: list[tuple[int, int, int, int]] = []
    tensor_start = 0
    for tensor_index, tensor_size in enumerate(tensor_cardinalities):
        for chunk_index, start_in_tensor in enumerate(range(0, tensor_size, max_chunk_elements)):
            length = min(max_chunk_elements, tensor_size - start_in_tensor)
            global_start = tensor_start + start_in_tensor
            global_end = global_start + length
            left = 0
            intersections = []
            for right in pivots:
                intersections.append(max(0, min(global_end, right) - max(global_start, left)))
                left = right
            owner = max(range(len(intersections)), key=intersections.__getitem__)
            output.append((tensor_index, chunk_index, owner, length))
        tensor_start += tensor_size
    return tuple(output)
