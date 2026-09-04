"""Aggregation rules: Hivemind's current one, and Byzantine-robust alternatives.

Hivemind's all-reduce reduces to three lines in ``TensorPartReducer.accumulate_part``
(``hivemind/averaging/partition.py:242-260``)::

    self.accumulator.add_(tensor_part, alpha=weight)
    self.denominator += weight
    ...
    self.current_part_future.set_result(self.accumulator.div_(self.denominator))

That is an **unbounded weighted arithmetic mean**, and the ``weight`` arrives on the
wire from the sender (``allreduce.py:339-347``) with no validation -- no
non-negativity check, no finiteness check, no cap. A whole-repo search for
``krum|byzantine|median|trimmed|robust`` returns nothing, so there is no alternative
rule to compare against; :func:`weighted_mean` reproduces the current behaviour
faithfully and the rest are candidates this project introduces.

Robust rules deliberately ignore the sender-supplied weight: it is exactly the field
an attacker controls, so a rule that honoured it would hand back the leverage it is
meant to remove.
"""

from __future__ import annotations

from typing import Callable, Optional

import torch

__all__ = ["weighted_mean", "krum", "multi_krum", "coordinate_median", "trimmed_mean", "AGGREGATORS"]


def weighted_mean(vectors: torch.Tensor, weights: Optional[torch.Tensor] = None) -> torch.Tensor:
    """Hivemind's current rule, reproduced exactly -- including its lack of guards.

    :param vectors: ``(n, d)`` stack of per-peer contributions
    :param weights: ``(n,)`` sender-supplied weights; ``None`` means uniform
    """
    if weights is None:
        weights = torch.ones(vectors.shape[0], dtype=vectors.dtype)
    # add_(x, alpha=w) summed over senders, then div_(sum of weights)
    accumulator = (vectors * weights.unsqueeze(1)).sum(dim=0)
    denominator = weights.sum()
    return accumulator / denominator


def _pairwise_sq_distances(vectors: torch.Tensor) -> torch.Tensor:
    """``(n, n)`` matrix of squared euclidean distances between contributions."""
    return torch.cdist(vectors, vectors, p=2) ** 2


def krum(
    vectors: torch.Tensor,
    num_byzantine: int,
    weights: Optional[torch.Tensor] = None,  # noqa: ARG001 - intentionally ignored, see module docstring
) -> torch.Tensor:
    """Krum (Blanchard et al., NeurIPS 2017): return the single most 'agreed-with' contribution.

    Each candidate scores the sum of squared distances to its ``n - f - 2`` nearest
    peers; the lowest score wins. Because the winner is an actual submitted vector,
    a lone outlier can never be selected regardless of how extreme it is -- the
    property plain averaging lacks.

    Tolerance requires ``n > 2f + 2``. Above that the guarantee lapses, which is the
    boundary Track C measures rather than assumes.
    """
    num_peers = vectors.shape[0]
    if num_peers == 1:
        return vectors[0].clone()

    num_neighbors = max(num_peers - num_byzantine - 2, 1)
    distances = _pairwise_sq_distances(vectors)
    distances.fill_diagonal_(float("inf"))  # never score a vector against itself

    # NaN/inf contributions must not win by making every distance undefined
    distances = torch.nan_to_num(distances, nan=float("inf"), posinf=float("inf"))
    closest, _ = torch.topk(distances, k=num_neighbors, dim=1, largest=False)
    scores = closest.sum(dim=1)
    return vectors[int(torch.argmin(scores))].clone()


def multi_krum(
    vectors: torch.Tensor,
    num_byzantine: int,
    weights: Optional[torch.Tensor] = None,  # noqa: ARG001
    num_selected: Optional[int] = None,
) -> torch.Tensor:
    """Multi-Krum: average the ``num_selected`` best-scoring contributions.

    Trades a little robustness for the variance reduction that makes averaging
    useful in the first place -- selecting only one vector (plain Krum) throws away
    most of the honest signal.
    """
    num_peers = vectors.shape[0]
    if num_selected is None:
        num_selected = max(num_peers - 2 * num_byzantine, 1)
    num_selected = min(num_selected, num_peers)

    num_neighbors = max(num_peers - num_byzantine - 2, 1)
    distances = _pairwise_sq_distances(vectors)
    distances.fill_diagonal_(float("inf"))
    distances = torch.nan_to_num(distances, nan=float("inf"), posinf=float("inf"))
    closest, _ = torch.topk(distances, k=min(num_neighbors, num_peers - 1), dim=1, largest=False)
    scores = closest.sum(dim=1)

    chosen = torch.topk(scores, k=num_selected, largest=False).indices
    return vectors[chosen].mean(dim=0)


def coordinate_median(
    vectors: torch.Tensor,
    num_byzantine: int = 0,  # noqa: ARG001 - signature parity with the other rules
    weights: Optional[torch.Tensor] = None,  # noqa: ARG001
) -> torch.Tensor:
    """Per-coordinate median. Cheap, and unmoved by a minority of extreme values."""
    return torch.nan_to_num(vectors, nan=0.0).median(dim=0).values


def trimmed_mean(
    vectors: torch.Tensor,
    num_byzantine: int = 1,
    weights: Optional[torch.Tensor] = None,  # noqa: ARG001
) -> torch.Tensor:
    """Coordinate-wise mean after dropping the ``num_byzantine`` largest and smallest values."""
    num_peers = vectors.shape[0]
    trim = min(num_byzantine, (num_peers - 1) // 2)
    if trim == 0:
        return torch.nan_to_num(vectors, nan=0.0).mean(dim=0)
    ordered, _ = torch.sort(torch.nan_to_num(vectors, nan=0.0), dim=0)
    return ordered[trim : num_peers - trim].mean(dim=0)


# Name -> rule. ``weighted_mean`` is the status quo; the rest are the candidates.
AGGREGATORS: dict[str, Callable[..., torch.Tensor]] = {
    "weighted_mean (Hivemind today)": weighted_mean,
    "krum": krum,
    "multi_krum": multi_krum,
    "coordinate_median": coordinate_median,
    "trimmed_mean": trimmed_mean,
}


def aggregate(
    name: str,
    vectors: torch.Tensor,
    *,
    weights: Optional[torch.Tensor] = None,
    num_byzantine: int = 0,
) -> torch.Tensor:
    """Dispatch to a named rule with a uniform signature."""
    rule = AGGREGATORS[name]
    if rule is weighted_mean:
        return rule(vectors, weights)
    return rule(vectors, num_byzantine, weights)


def estimate_num_byzantine(num_peers: int, assumed_fraction: float = 0.25) -> int:
    """A defender's guess at ``f``, since the true count is never known.

    Robust rules need an ``f`` up front. Track C uses this deliberately conservative
    estimate rather than the true value, so the defense is not handed information a
    real deployment would not have.
    """
    return max(int(num_peers * assumed_fraction), 1)


def contribution_norms(vectors: torch.Tensor) -> torch.Tensor:
    """L2 norm per contribution -- used to quantify how 'stealthy' an attack is."""
    return torch.nan_to_num(vectors, nan=float("inf")).norm(dim=1)
