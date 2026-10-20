"""Small deterministic learning workload for announcement experiments.

NumPy is imported only when a numeric problem is requested, so importing this
module does not make it a requirement for control-plane-only simulator runs.
The workload is deliberately protocol agnostic: callers own participant models
and decide how returned gradients are combined.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
from pathlib import Path
import random
from typing import Any


NODES = ("B", "D", "A", "C")
DIMENSIONS = 48
TRAINING_EXAMPLES_PER_NODE = 512
EVALUATION_EXAMPLES = 4096
LEARNING_RATE = 0.1
ROUNDS = 200
RECOVERY_ATTACK_ROUNDS = 100
DISTINCTIVE_FEATURES = tuple(range(36, 48))
SCHEDULE_ARMS = (
    "baseline",
    "random_omission_25",
    "random_omission_100",
    "membership_25",
    "membership_100",
    "ordering_25",
    "ordering_100",
    "membership_recovery",
    "ordering_recovery",
)


def _numpy():
    try:
        import numpy as np
    except ImportError as exc:  # pragma: no cover - exercised without optional dep
        raise RuntimeError(
            "the announcement learning workload requires the optional numpy package"
        ) from exc
    return np


def _derive_seed(seed: int, stream: str) -> int:
    """Derive stable named RNG seeds without Python's process-randomized hash()."""
    if isinstance(seed, bool) or not isinstance(seed, int) or seed < 0:
        raise ValueError("seed must be a non-negative integer")
    material = f"announcement-learning-v1\0{seed}\0{stream}".encode("utf-8")
    return int.from_bytes(hashlib.sha256(material).digest()[:16], "big")


def _array_hash(array: Any) -> str:
    np = _numpy()
    contiguous = np.ascontiguousarray(array)
    header = f"{contiguous.dtype.str}:{contiguous.shape}:".encode("ascii")
    return hashlib.sha256(header + contiguous.tobytes(order="C")).hexdigest()


def _source_hash() -> str:
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def _canonical_json(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"),
                       ensure_ascii=True, allow_nan=False) + "\n").encode("utf-8")


@dataclass(frozen=True)
class LearningProblem:
    """Fixed full-batch linear-regression problem shared by four participants.

    ``train_x[node]`` and ``train_y[node]`` are float32 arrays. ``target`` is
    the shared seeded unit-norm coefficient vector. ``evaluate(weights)``
    returns half mean-squared error over the fixed isotropic test set.
    """

    seed: int
    distribution: str
    target: Any
    train_x: dict[str, Any]
    train_y: dict[str, Any]
    eval_x: Any
    eval_y: Any
    source_sha256: str

    @property
    def initial_weights(self):
        """Return a fresh zero-initialized float32 model vector."""
        return _numpy().zeros((DIMENSIONS,), dtype=_numpy().float32)

    def gradient(self, node: str, weights):
        """Compute that participant's full-batch gradient of half-MSE."""
        if node not in self.train_x:
            raise KeyError(f"unknown participant {node!r}; expected one of {NODES}")
        np = _numpy()
        vector = np.asarray(weights, dtype=np.float32)
        if vector.shape != (DIMENSIONS,):
            raise ValueError(f"weights must have shape ({DIMENSIONS},), got {vector.shape}")
        residual = self.train_x[node] @ vector - self.train_y[node]
        result = self.train_x[node].T @ residual / np.float32(TRAINING_EXAMPLES_PER_NODE)
        return np.asarray(result, dtype=np.float32)

    def evaluate(self, weights) -> float:
        """Return fixed-set half-MSE as a Python float."""
        np = _numpy()
        vector = np.asarray(weights, dtype=np.float32)
        if vector.shape != (DIMENSIONS,):
            raise ValueError(f"weights must have shape ({DIMENSIONS},), got {vector.shape}")
        residual = self.eval_x @ vector - self.eval_y
        return float(np.mean(np.square(residual, dtype=np.float32), dtype=np.float32) / 2.0)

    @staticmethod
    def update(weights, gradient):
        """Apply one float32 SGD update using the frozen learning rate."""
        np = _numpy()
        vector = np.asarray(weights, dtype=np.float32)
        grad = np.asarray(gradient, dtype=np.float32)
        expected = (DIMENSIONS,)
        if vector.shape != expected:
            raise ValueError(f"weights must have shape {expected}, got {vector.shape}")
        if grad.shape != expected:
            raise ValueError(f"gradient must have shape {expected}, got {grad.shape}")
        return np.asarray(vector - np.float32(LEARNING_RATE) * grad, dtype=np.float32)

    def config(self) -> dict[str, Any]:
        """Return the complete frozen numerical and data-generation contract."""
        return {
            "schema": "announcement-learning-problem-v1",
            "seed": self.seed,
            "distribution": self.distribution,
            "nodes": list(NODES),
            "dimensions": DIMENSIONS,
            "training_examples_per_node": TRAINING_EXAMPLES_PER_NODE,
            "evaluation_examples": EVALUATION_EXAMPLES,
            "distinctive_features": list(DISTINCTIVE_FEATURES),
            "target": "shared seeded unit-norm vector",
            "initialization": "zero float32 vector",
            "targets": "noiseless X @ target",
            "loss": "half mean squared error",
            "gradient": "full-batch local gradient",
            "optimizer": "SGD",
            "learning_rate": LEARNING_RATE,
            "dtype": "float32",
            "rng": "named SHA-256-derived streams with NumPy default_rng",
        }

    def hashes(self) -> dict[str, Any]:
        """Return stable hashes that let a runner identify every frozen array."""
        config = self.config()
        return {
            "source_sha256": self.source_sha256,
            "config_sha256": hashlib.sha256(_canonical_json(config)).hexdigest(),
            "target_sha256": _array_hash(self.target),
            "training": {
                node: {
                    "x_sha256": _array_hash(self.train_x[node]),
                    "y_sha256": _array_hash(self.train_y[node]),
                }
                for node in NODES
            },
            "evaluation": {
                "x_sha256": _array_hash(self.eval_x),
                "y_sha256": _array_hash(self.eval_y),
            },
        }

    def save(self, directory: str | Path) -> dict[str, Any]:
        """Persist arrays and a hash manifest; return the manifest written.

        A caller should choose a fresh experiment directory and retain its raw
        event trace beside these files. Existing names are overwritten so a
        runner can atomically regenerate an incomplete local artifact set.
        """
        np = _numpy()
        path = Path(directory)
        path.mkdir(parents=True, exist_ok=True)
        data_path = path / "learning_data.npz"
        arrays = {"target": self.target, "eval_x": self.eval_x, "eval_y": self.eval_y}
        for node in NODES:
            arrays[f"train_x_{node}"] = self.train_x[node]
            arrays[f"train_y_{node}"] = self.train_y[node]
        np.savez_compressed(data_path, **arrays)
        config_bytes = _canonical_json(self.config())
        (path / "learning_config.json").write_bytes(config_bytes)
        manifest = {
            **self.hashes(),
            "data_file": data_path.name,
            "data_file_sha256": hashlib.sha256(data_path.read_bytes()).hexdigest(),
            "config_file": "learning_config.json",
            "config_file_sha256": hashlib.sha256(config_bytes).hexdigest(),
        }
        (path / "learning_manifest.json").write_bytes(_canonical_json(manifest))
        return manifest


def make_problem(seed: int, distribution: str = "iid") -> LearningProblem:
    """Generate paired deterministic IID or distinctive-feature data.

    ``iid`` gives each participant 512 independent isotropic Gaussian examples.
    ``distinctive`` zeros the last twelve input features at B, D, and C while A
    retains its full Gaussian data. In both cases labels follow the same seeded
    target vector with no added noise. Evaluation data is independent and
    isotropic for both distributions.
    """
    if distribution not in ("iid", "distinctive"):
        raise ValueError("distribution must be 'iid' or 'distinctive'")
    _derive_seed(seed, "validation")  # validate seed before importing/allocating
    np = _numpy()

    target_rng = np.random.default_rng(_derive_seed(seed, "problem/target"))
    target = target_rng.standard_normal(DIMENSIONS).astype(np.float32)
    norm = float(np.linalg.norm(target.astype(np.float64)))
    if not math.isfinite(norm) or norm == 0.0:  # defensive; practically impossible
        raise RuntimeError("failed to generate a finite nonzero target vector")
    target = np.asarray(target / np.float32(norm), dtype=np.float32)

    train_x: dict[str, Any] = {}
    train_y: dict[str, Any] = {}
    for node in NODES:
        rng = np.random.default_rng(_derive_seed(seed, f"problem/train/{node}"))
        features = rng.standard_normal((TRAINING_EXAMPLES_PER_NODE, DIMENSIONS)).astype(np.float32)
        if distribution == "distinctive" and node != "A":
            features[:, DISTINCTIVE_FEATURES] = np.float32(0.0)
        train_x[node] = features
        train_y[node] = np.asarray(features @ target, dtype=np.float32)

    eval_rng = np.random.default_rng(_derive_seed(seed, "problem/evaluation"))
    eval_x = eval_rng.standard_normal((EVALUATION_EXAMPLES, DIMENSIONS)).astype(np.float32)
    eval_y = np.asarray(eval_x @ target, dtype=np.float32)
    return LearningProblem(
        seed=seed,
        distribution=distribution,
        target=target,
        train_x=train_x,
        train_y=train_y,
        eval_x=eval_x,
        eval_y=eval_y,
        source_sha256=_source_hash(),
    )


def load_problem(directory: str | Path) -> LearningProblem:
    """Load persisted problem arrays and verify their source/config/data hashes."""
    np = _numpy()
    path = Path(directory)
    manifest = json.loads((path / "learning_manifest.json").read_text(encoding="utf-8"))
    config_path = path / manifest["config_file"]
    config_bytes = config_path.read_bytes()
    if hashlib.sha256(config_bytes).hexdigest() != manifest["config_file_sha256"]:
        raise ValueError("saved learning config file hash mismatch")
    config = json.loads(config_bytes)
    if hashlib.sha256(_canonical_json(config)).hexdigest() != manifest["config_sha256"]:
        raise ValueError("saved learning config hash mismatch")
    if config.get("schema") != "announcement-learning-problem-v1":
        raise ValueError("unsupported saved learning config schema")
    data_path = path / manifest["data_file"]
    if hashlib.sha256(data_path.read_bytes()).hexdigest() != manifest["data_file_sha256"]:
        raise ValueError("saved learning data file hash mismatch")
    with np.load(data_path, allow_pickle=False) as saved:
        target = np.array(saved["target"], dtype=np.float32, copy=True)
        train_x = {node: np.array(saved[f"train_x_{node}"], dtype=np.float32, copy=True)
                   for node in NODES}
        train_y = {node: np.array(saved[f"train_y_{node}"], dtype=np.float32, copy=True)
                   for node in NODES}
        eval_x = np.array(saved["eval_x"], dtype=np.float32, copy=True)
        eval_y = np.array(saved["eval_y"], dtype=np.float32, copy=True)
    problem = LearningProblem(
        seed=int(config["seed"]),
        distribution=str(config["distribution"]),
        target=target,
        train_x=train_x,
        train_y=train_y,
        eval_x=eval_x,
        eval_y=eval_y,
        source_sha256=str(manifest["source_sha256"]),
    )
    if problem.hashes() != {
        key: value for key, value in manifest.items()
        if key in ("source_sha256", "config_sha256", "target_sha256", "training", "evaluation")
    }:
        raise ValueError("saved learning array hashes do not match the manifest")
    if problem.config() != config:
        raise ValueError("saved learning config does not match the loaded arrays")
    return problem


def make_schedules(seed: int, rounds: int = ROUNDS) -> dict[str, tuple[dict[str, Any], ...]]:
    """Build paired 200-round experiment schedules with named RNG streams.

    Entries are one-indexed dictionaries with ``round``, ``condition`` and
    ``target``. Honest rounds have a null target. All 25% arms share exactly
    the same uniformly sampled affected-round set. Benign omission chooses a
    participant from A, D, or C independently on each affected round. Recovery
    arms attack rounds 1 through 100 and then run honestly.
    """
    if isinstance(rounds, bool) or not isinstance(rounds, int) or rounds < 1:
        raise ValueError("rounds must be a positive integer")
    affected_count = int(rounds * 0.25)
    schedule_rng = random.Random(_derive_seed(seed, "schedule/affected-25"))
    affected_25 = set(schedule_rng.sample(range(1, rounds + 1), affected_count))
    omission_rng = random.Random(_derive_seed(seed, "schedule/random-omission-targets"))
    omission_targets = {
        round_index: omission_rng.choice(("A", "D", "C"))
        for round_index in sorted(affected_25)
    }
    omission_100_rng = random.Random(_derive_seed(seed, "schedule/random-omission-100-targets"))
    omission_100_targets = {
        round_index: omission_100_rng.choice(("A", "D", "C"))
        for round_index in range(1, rounds + 1)
    }

    def build(arm: str) -> tuple[dict[str, Any], ...]:
        rows = []
        for round_index in range(1, rounds + 1):
            condition, target = "honest", None
            if arm == "random_omission_25" and round_index in affected_25:
                condition, target = "benign_omission", omission_targets[round_index]
            elif arm == "random_omission_100":
                condition = "benign_omission"
                target = omission_100_targets[round_index]
            elif arm == "membership_25" and round_index in affected_25:
                condition, target = "membership_omission", "A"
            elif arm == "membership_100":
                condition, target = "membership_omission", "A"
            elif arm == "ordering_25" and round_index in affected_25:
                condition, target = "ordering_corruption", "A"
            elif arm == "ordering_100":
                condition, target = "ordering_corruption", "A"
            elif arm == "membership_recovery" and round_index <= min(RECOVERY_ATTACK_ROUNDS, rounds):
                condition, target = "membership_omission", "A"
            elif arm == "ordering_recovery" and round_index <= min(RECOVERY_ATTACK_ROUNDS, rounds):
                condition, target = "ordering_corruption", "A"
            rows.append({"round": round_index, "condition": condition, "target": target})
        return tuple(rows)

    return {arm: build(arm) for arm in SCHEDULE_ARMS}
