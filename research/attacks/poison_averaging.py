"""Track C -- turning control of a victim's group into damage to what it is learning.

Tracks A and B produce a *local malicious fraction* ``f_l``: the share of a victim's
averaging group that the attacker controls. This module asks what that share is worth.

The learning task is deliberately tiny and fully known: a distributed least-squares
problem with an optimum ``x*`` we fix in advance. No neural network, no dataset, no
GPU -- but real convergence, and an exact ground truth to measure displacement
against. Peers run local gradient steps and average parameters through the same rule
Hivemind uses (:mod:`research.defenses.robust_agg`).

Two attacker styles, because they fail differently:

* **untargeted** -- wreck convergence outright (sign flips, huge norms, non-finite
  values, or simply an inflated ``weight``, which Hivemind applies unchecked).
* **targeted, stealthy** -- leave convergence looking healthy while steering a chosen
  subset of coordinates. This models poisoning one class or subgroup, and it is the
  case norm-based sanity checks miss.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Sequence

import torch

__all__ = ["LeastSquaresTask", "ATTACKS", "AttackConfig", "run_training"]


@dataclass
class LeastSquaresTask:
    """A convex problem with a known optimum, sharded across peers.

    Each peer holds rows ``(A_i, b_i)`` with ``b_i = A_i x* + noise``. Averaging the
    peers' parameters is what a real Hivemind run does; here the honest fixed point is
    ``x*``, so any displacement is attributable to the attack rather than to noise in a
    model we cannot inspect.
    """

    dim: int = 64
    num_peers: int = 16
    rows_per_peer: int = 128
    noise_std: float = 0.01
    seed: int = 0

    optimum: torch.Tensor = field(init=False)
    matrices: List[torch.Tensor] = field(init=False)
    targets: List[torch.Tensor] = field(init=False)

    def __post_init__(self) -> None:
        generator = torch.Generator().manual_seed(self.seed)
        self.optimum = torch.randn(self.dim, generator=generator)
        self.matrices, self.targets = [], []
        for _ in range(self.num_peers):
            # unit-scale rows keep (A^T A / rows) ~ I, so the objective is well conditioned
            # and a fixed learning rate converges in tens of rounds
            matrix = torch.randn(self.rows_per_peer, self.dim, generator=generator)
            noise = torch.randn(self.rows_per_peer, generator=generator) * self.noise_std
            self.matrices.append(matrix)
            self.targets.append(matrix @ self.optimum + noise)

    def local_gradient(self, peer_index: int, parameters: torch.Tensor) -> torch.Tensor:
        matrix, target = self.matrices[peer_index], self.targets[peer_index]
        residual = matrix @ parameters - target
        return 2.0 * (matrix.T @ residual) / matrix.shape[0]

    def loss(self, parameters: torch.Tensor) -> float:
        """Mean squared error over every peer's data -- the global objective."""
        total = 0.0
        for matrix, target in zip(self.matrices, self.targets):
            total += float(((matrix @ parameters - target) ** 2).mean())
        return total / len(self.matrices)

    def distance_to_optimum(self, parameters: torch.Tensor) -> float:
        return float(torch.norm(torch.nan_to_num(parameters, nan=float("inf")) - self.optimum))

    def targeted_error(self, parameters: torch.Tensor, coordinates: Sequence[int]) -> float:
        """Displacement restricted to the attacker's chosen coordinates."""
        index = torch.tensor(list(coordinates), dtype=torch.long)
        clean = torch.nan_to_num(parameters, nan=float("inf"))
        return float(torch.norm(clean[index] - self.optimum[index]))


# --- attacker strategies -------------------------------------------------------
# Each returns the vector a malicious peer submits, given what the honest peers are
# doing this round (the attacker can see its groupmates' contributions, and in
# Hivemind it genuinely can -- it is a full participant in the all-reduce).


def attack_sign_flip(honest: torch.Tensor, current: torch.Tensor, task: LeastSquaresTask, **_: object) -> torch.Tensor:
    """Push directly against the honest consensus."""
    return -5.0 * honest.mean(dim=0)


def attack_large_norm(
    honest: torch.Tensor, current: torch.Tensor, task: LeastSquaresTask, **_: object
) -> torch.Tensor:
    """Brute force: a contribution large enough to dominate the mean."""
    return torch.full_like(current, 1e3)


def attack_non_finite(
    honest: torch.Tensor, current: torch.Tensor, task: LeastSquaresTask, **_: object
) -> torch.Tensor:
    """NaN poisoning -- a single NaN makes the plain mean NaN for every peer."""
    poisoned = current.clone()
    poisoned[0] = float("nan")
    return poisoned


def attack_targeted_stealth(
    honest: torch.Tensor,
    current: torch.Tensor,
    task: LeastSquaresTask,
    *,
    coordinates: Sequence[int] = (),
    strength: float = 0.35,
    **_: object,
) -> torch.Tensor:
    """Look like an ordinary peer, but drag a few coordinates away from the optimum.

    The submission starts from the honest mean -- so its norm and direction sit inside
    the honest distribution -- and adds a small offset on the target coordinates only.
    Convergence keeps looking fine while those coordinates are steered.
    """
    poisoned = honest.mean(dim=0).clone()
    if len(coordinates):
        index = torch.tensor(list(coordinates), dtype=torch.long)
        # push away from the optimum, scaled to stay comparable to honest updates
        direction = torch.sign(poisoned[index] - task.optimum[index])
        direction[direction == 0] = 1.0
        poisoned[index] = poisoned[index] + strength * direction * (1.0 + poisoned[index].abs())
    return poisoned


ATTACKS: Dict[str, Callable[..., torch.Tensor]] = {
    "none": lambda honest, current, task, **kw: honest.mean(dim=0),
    "sign_flip": attack_sign_flip,
    "large_norm": attack_large_norm,
    "non_finite": attack_non_finite,
    "targeted_stealth": attack_targeted_stealth,
}


@dataclass
class AttackConfig:
    """How the attacker behaves in a run."""

    strategy: str = "none"
    local_fraction: float = 0.0  # f_l: share of the victim's group that is malicious
    weight: float = 1.0  # what malicious peers claim; Hivemind applies this unchecked
    target_coordinates: Sequence[int] = ()
    strength: float = 0.35


@dataclass
class TrainingResult:
    rounds: int
    final_distance: float
    final_loss: float
    final_targeted_error: float
    baseline_targeted_error: float
    distance_history: List[float]
    targeted_history: List[float]
    diverged: bool
    mean_attacker_norm: float
    mean_honest_norm: float

    @property
    def stealth_ratio(self) -> float:
        """Attacker's contribution norm relative to honest peers'.

        Near 1.0 means a norm-based filter would not flag the attacker -- the reason
        the targeted attack is the dangerous one.
        """
        if self.mean_honest_norm == 0:
            return float("inf")
        return self.mean_attacker_norm / self.mean_honest_norm


def run_training(
    task: LeastSquaresTask,
    *,
    aggregator: str,
    attack: AttackConfig,
    rounds: int = 60,
    learning_rate: float = 0.25,
    assumed_byzantine_fraction: float = 0.25,
) -> TrainingResult:
    """Run the distributed task to completion under one (aggregator, attack) pair.

    Every peer keeps its own parameters, takes a local gradient step, then replaces
    them with the aggregate -- the structure of a Hivemind round. Only the aggregation
    rule and the attacker's submission change between conditions.
    """
    from research.defenses.robust_agg import aggregate, contribution_norms, estimate_num_byzantine

    num_peers = task.num_peers
    num_malicious = int(round(attack.local_fraction * num_peers))
    num_honest = num_peers - num_malicious
    attack_fn = ATTACKS[attack.strategy]

    parameters = torch.zeros(task.dim)
    distance_history, targeted_history = [], []
    attacker_norms, honest_norms = [], []
    assumed_byzantine = estimate_num_byzantine(num_peers, assumed_byzantine_fraction)

    for _ in range(rounds):
        # honest peers each take a local gradient step from the shared parameters
        honest_updates = torch.stack(
            [parameters - learning_rate * task.local_gradient(i, parameters) for i in range(num_honest)]
        )

        if num_malicious > 0 and attack.strategy != "none":
            malicious = attack_fn(
                honest_updates,
                parameters,
                task,
                coordinates=attack.target_coordinates,
                strength=attack.strength,
            )
            contributions = torch.cat([honest_updates, malicious.unsqueeze(0).repeat(num_malicious, 1)])
            weights = torch.cat([torch.ones(num_honest), torch.full((num_malicious,), float(attack.weight))])
            norms = contribution_norms(contributions)
            attacker_norms.append(float(norms[num_honest:].mean()))
            honest_norms.append(float(norms[:num_honest].mean()))
        else:
            contributions = honest_updates
            weights = torch.ones(num_honest)
            honest_norms.append(float(contribution_norms(contributions).mean()))

        parameters = aggregate(aggregator, contributions, weights=weights, num_byzantine=assumed_byzantine)

        distance_history.append(task.distance_to_optimum(parameters))
        targeted_history.append(
            task.targeted_error(parameters, attack.target_coordinates) if len(attack.target_coordinates) else 0.0
        )

    final_distance = distance_history[-1]
    baseline_targeted = (
        task.targeted_error(torch.zeros(task.dim), attack.target_coordinates)
        if len(attack.target_coordinates)
        else 0.0
    )
    return TrainingResult(
        rounds=rounds,
        final_distance=final_distance,
        final_loss=task.loss(parameters) if torch.isfinite(parameters).all() else float("inf"),
        final_targeted_error=targeted_history[-1],
        baseline_targeted_error=baseline_targeted,
        distance_history=distance_history,
        targeted_history=targeted_history,
        diverged=not torch.isfinite(torch.tensor(final_distance)) or final_distance > 1e3,
        mean_attacker_norm=sum(attacker_norms) / len(attacker_norms) if attacker_norms else 0.0,
        mean_honest_norm=sum(honest_norms) / len(honest_norms) if honest_norms else 0.0,
    )
