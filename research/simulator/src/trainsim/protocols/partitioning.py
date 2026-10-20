"""Pure-Python port of Hivemind's equal-share partition and chunk assignment.

The simulator's baseline reports bandwidth as ``None`` for every NODE, which
selects Hivemind's equal-score branch.  The optional numeric path below uses a
small water-filling solver for the same minimax objective; it avoids importing
NumPy or SciPy into the simulator runtime.
"""

from __future__ import annotations

import math
from typing import Sequence


def hagenbach_bischoff(total: int, scores: Sequence[float]) -> tuple[int, ...]:
    """Split an integer total using Hivemind's largest-quotient remainder rule."""
    if total < 0:
        raise ValueError("total must be non-negative")
    if not scores or any(score < 0 or not math.isfinite(score) for score in scores):
        raise ValueError("scores must be a non-empty sequence of finite non-negative values")
    score_sum = sum(scores)
    if score_sum <= 0:
        raise ValueError("at least one score must be positive")
    allocated = [int(total * score / score_sum) for score in scores]
    while sum(allocated) < total:
        quotients = [score / (allocated[index] + 1) for index, score in enumerate(scores)]
        allocated[quotients.index(max(quotients))] += 1
    return tuple(allocated)


def _bandwidth_scores(
    total_elements: int, bandwidths: Sequence[float | None], min_elements: int
) -> tuple[float, ...]:
    """Approximate Hivemind's minimax LP scores with deterministic water filling."""
    if total_elements <= 0:
        return tuple(0.0 for _ in bandwidths)
    if not bandwidths:
        raise ValueError("at least one peer is required")
    if any(value is not None and (not math.isfinite(value) or value < 0) for value in bandwidths):
        raise ValueError("bandwidths must be non-negative finite numbers or None")

    specified = [value for value in bandwidths if value is not None and value > 0]
    if not specified:
        scores = tuple(1.0 if value is None else 0.0 for value in bandwidths)
        if not any(scores):
            raise ValueError("at least one peer must have positive or unspecified bandwidth")
        return scores

    default = sum(specified) / len(specified)
    rates = tuple(default if value is None else value for value in bandwidths)
    active = [index for index, rate in enumerate(rates) if rate > 0]
    if len(active) == 1:
        result = [0.0] * len(rates)
        result[active[0]] = 1.0
        return tuple(result)

    # Hivemind minimizes max_i (1 + (n - 2) * fraction_i) / bandwidth_i.
    # Solve sum(max(0, (rate_i * bound - 1) / (n - 2))) == 1 by bisection.
    # For a two-peer group the objective is independent of fractions; use equal
    # scores, a deterministic valid minimizer for the simulator.
    if len(rates) <= 2:
        scores = [1.0 if index in active else 0.0 for index in range(len(rates))]
    else:
        lo = min(1.0 / rates[index] for index in active)
        hi = max(1.0 / rates[index] for index in active) + (len(rates) - 2) / min(
            rates[index] for index in active
        )
        for _ in range(100):
            bound = (lo + hi) / 2.0
            weight = sum(max(0.0, (rates[index] * bound - 1.0) / (len(rates) - 2)) for index in active)
            if weight < 1.0:
                lo = bound
            else:
                hi = bound
        bound = (lo + hi) / 2.0
        scores = [max(0.0, (rate * bound - 1.0) / (len(rates) - 2)) if rate > 0 else 0.0 for rate in rates]

    # Match Hivemind's minimum partition threshold: only discard small shares
    # when at least one peer can meet the threshold.
    threshold = min_elements / float(total_elements)
    if max(scores, default=0.0) >= threshold:
        scores = [score if score >= threshold else 0.0 for score in scores]
    scores = [round(score, 9) for score in scores]
    if not any(scores):
        # This is only reachable for pathological thresholds; preserve all work.
        scores = [1.0 if rate > 0 else 0.0 for rate in rates]
    return tuple(scores)


def allocate_elements(
    total_elements: int,
    bandwidths: Sequence[float | None],
    min_elements: int = 0,
) -> tuple[int, ...]:
    """Return per-peer element counts using Hivemind's partition scoring rule.

    ``None`` means unspecified bandwidth. When all peers are unspecified, all
    receive equal scores; zero bandwidth peers receive no partition.
    """
    if total_elements < 0 or min_elements < 0:
        raise ValueError("element counts must be non-negative")
    scores = _bandwidth_scores(total_elements, bandwidths, min_elements)
    if total_elements == 0:
        return tuple(0 for _ in bandwidths)
    return hagenbach_bischoff(total_elements, scores)


def assign_chunks(
    total_elements: int,
    chunk_elements: int,
    peer_fractions: Sequence[int],
) -> tuple[tuple[int, ...], ...]:
    """Assign each tensor chunk to the peer with the largest boundary overlap.

    This follows ``TensorPartContainer``: first make tensor-sized chunks, form
    cumulative pivots from peer fractions, then assign each whole chunk to the
    peer receiving its largest element intersection. Ties go to the earlier
    peer, matching ``numpy.argmax``.
    """
    if total_elements < 0 or chunk_elements <= 0:
        raise ValueError("total_elements must be non-negative and chunk_elements positive")
    if not peer_fractions or any(value < 0 for value in peer_fractions):
        raise ValueError("peer_fractions must be a non-empty sequence of non-negative integers")
    fraction_sum = sum(peer_fractions)
    if total_elements and fraction_sum <= 0:
        raise ValueError("peer fractions must have a positive sum for non-empty tensors")
    if not total_elements:
        return tuple(() for _ in peer_fractions)

    pivots: list[int] = []
    cumulative = 0
    for fraction in peer_fractions:
        cumulative += fraction
        pivots.append(int(cumulative / fraction_sum * total_elements))
    pivots[-1] = total_elements

    by_peer: list[list[int]] = [[] for _ in peer_fractions]
    start = 0
    chunk_index = 0
    while start < total_elements:
        end = min(start + chunk_elements, total_elements)
        left = 0
        intersections: list[int] = []
        for right in pivots:
            intersections.append(max(0, min(end, right) - max(start, left)))
            left = right
        best = max(range(len(intersections)), key=intersections.__getitem__)
        by_peer[best].append(chunk_index)
        start = end
        chunk_index += 1
    return tuple(tuple(indices) for indices in by_peer)


def chunk_sizes(total_elements: int, chunk_elements: int) -> tuple[int, ...]:
    """Return the element count of each tensor chunk, including a short tail."""
    if total_elements < 0 or chunk_elements <= 0:
        raise ValueError("total_elements must be non-negative and chunk_elements positive")
    return tuple(min(chunk_elements, total_elements - offset) for offset in range(0, total_elements, chunk_elements))
