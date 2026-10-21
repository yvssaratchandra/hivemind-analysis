"""Deterministic synthetic SGD workload and evaluation helpers.

The module keeps NumPy optional until a numeric helper is called. Data streams
are keyed by explicit names and SHA-256-derived seeds, so workers can request
batches in any order without sharing mutable RNG state.
"""
from __future__ import annotations

import hashlib
import json
import math
from typing import Any, Mapping


NODES = ("B", "D", "A", "C")
CONDITIONS = ("baseline", "exclusion", "absent")
DIMENSIONS = 48
BATCH_SIZE = 32
EVALUATION_EXAMPLES = 4096
LEARNING_RATE = 0.1
SHARE70_SAMPLES_PER_SECOND = {"B": 128, "D": 128, "A": 896, "C": 128}
SAMPLES_PER_SECOND = {"share70": SHARE70_SAMPLES_PER_SECOND}
CONFIRMATION_EVALUATIONS = 10
MAX_EVALUATION_GAP_SECONDS = 1.5
THRESHOLD_FRACTION = 0.1
RNG_SCHEMA = "hivemind-compute-workload-v1"


def _numpy():
    try:
        import numpy as np
    except ImportError as exc:  # pragma: no cover - exercised without optional dep
        raise RuntimeError("the compute workload requires the optional numpy package") from exc
    return np


def _validate_seed(seed: int) -> int:
    if isinstance(seed, bool) or not isinstance(seed, int) or seed < 0:
        raise ValueError("seed must be a non-negative integer")
    return seed


def _derive_seed(seed: int, stream: str) -> int:
    """Derive a stable 128-bit seed from a named stream and experiment seed."""
    seed = _validate_seed(seed)
    material = f"{RNG_SCHEMA}\0{seed}\0{stream}".encode("utf-8")
    return int.from_bytes(hashlib.sha256(material).digest()[:16], "big")


def _rng(seed: int, stream: str):
    return _numpy().random.default_rng(_derive_seed(seed, stream))


def _array_hash(array: Any) -> str:
    np = _numpy()
    contiguous = np.ascontiguousarray(array)
    header = f"{contiguous.dtype.str}:{contiguous.shape}:".encode("ascii")
    return hashlib.sha256(header + contiguous.tobytes(order="C")).hexdigest()


def make_target(seed: int):
    """Return the shared seeded unit-norm float32 coefficient vector."""
    np = _numpy()
    raw = _rng(seed, "target-vector").standard_normal(DIMENSIONS)
    target64 = raw / np.linalg.norm(raw)
    return np.asarray(target64, dtype=np.float32)


def batch(seed: int, node: str, index: int, batch_size: int = BATCH_SIZE):
    """Return an independently keyed Gaussian batch, noiseless labels, metadata."""
    np = _numpy()
    seed = _validate_seed(seed)
    if node not in NODES:
        raise ValueError(f"node must be one of {NODES}, got {node!r}")
    if isinstance(index, bool) or not isinstance(index, int) or index < 0:
        raise ValueError("index must be a non-negative integer")
    if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")

    x = _rng(seed, f"train-features:{node}:batch:{index}").standard_normal(
        (batch_size, DIMENSIONS)
    ).astype(np.float32)
    y = np.asarray(x @ make_target(seed), dtype=np.float32)
    sample_ids = [f"{RNG_SCHEMA}:{seed}:{node}:{index}:{row}" for row in range(batch_size)]
    x_hash, y_hash = _array_hash(x), _array_hash(y)
    digest = hashlib.sha256()
    digest.update(f"{RNG_SCHEMA}\0{seed}\0{node}\0{index}\0".encode("utf-8"))
    digest.update(x.tobytes(order="C"))
    digest.update(y.tobytes(order="C"))
    metadata = {
        "schema": RNG_SCHEMA,
        "seed": seed,
        "node": node,
        "batch_index": index,
        "batch_size": batch_size,
        "rng_stream": f"train-features:{node}:batch:{index}",
        "rng": "SHA-256-derived seed with NumPy default_rng",
        "sample_ids": sample_ids,
        "x_sha256": x_hash,
        "y_sha256": y_hash,
        "batch_sha256": digest.hexdigest(),
    }
    return x, y, metadata


def evaluation(seed: int):
    """Return an independent fixed 4096-example float32 evaluation set."""
    np = _numpy()
    seed = _validate_seed(seed)
    x = _rng(seed, "evaluation-features").standard_normal(
        (EVALUATION_EXAMPLES, DIMENSIONS)
    ).astype(np.float32)
    y = np.asarray(x @ make_target(seed), dtype=np.float32)
    return x, y


def initial_weights():
    """Return a fresh zero-initialized float32 model vector."""
    return _numpy().zeros((DIMENSIONS,), dtype=_numpy().float32)


def thresholds_from_initial_metrics(initial_metrics: Mapping[str, Any],
                                    fraction: float = THRESHOLD_FRACTION) -> dict[str, float]:
    """Return each D/C threshold as ``fraction`` of its initial half-MSE."""
    if isinstance(fraction, bool) or not isinstance(fraction, (int, float)) or not math.isfinite(fraction) or fraction <= 0:
        raise ValueError("fraction must be a finite positive number")
    if not isinstance(initial_metrics, Mapping):
        raise TypeError("initial_metrics must be a metrics mapping")
    values = _per_node_values(initial_metrics)
    thresholds = {}
    for node in ("D", "C"):
        value = values.get(node)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
            raise ValueError(f"initial half-MSE for {node} must be finite and positive")
        thresholds[node] = float(value) * float(fraction)
    return thresholds


def next_batch_delay(batch_started_at: float, compute_finished_at: float,
                     interval_seconds: float) -> float:
    """Return the remaining interval after one batch, without deadline carryover.

    Callers should take ``batch_started_at`` after the preceding optimizer step
    finishes. A slow step therefore makes this batch late without shifting a
    future deadline or creating accumulated catch-up work.
    """
    values = (batch_started_at, compute_finished_at, interval_seconds)
    if any(isinstance(value, bool) or not isinstance(value, (int, float))
           or not math.isfinite(value) for value in values):
        raise ValueError("pacing times and interval must be finite numbers")
    if interval_seconds < 0:
        raise ValueError("interval_seconds must be non-negative")
    elapsed = compute_finished_at - batch_started_at
    if elapsed < 0:
        raise ValueError("compute_finished_at must not precede batch_started_at")
    return max(0.0, interval_seconds - elapsed)


def _as_numpy(value: Any):
    np = _numpy()
    if hasattr(value, "detach"):
        value = value.detach()
    if hasattr(value, "cpu"):
        value = value.cpu()
    if hasattr(value, "numpy"):
        value = value.numpy()
    return np.asarray(value)


def evaluate_models(models: Mapping[str, Any], x: Any, y: Any) -> dict[str, Any]:
    """Compute fixed-set half-MSE for each node and the D/C aggregates.

    Model values may be NumPy-compatible vectors or tensor objects exposing
    ``detach().cpu().numpy()``. Missing, malformed, and non-finite models are
    represented explicitly in ``node_status`` and as ``None`` in ``per_node``.
    """
    np = _numpy()
    if not isinstance(models, Mapping):
        raise TypeError("models must be a mapping from node name to weight vector")
    features = np.asarray(_as_numpy(x), dtype=np.float64)
    labels = np.asarray(_as_numpy(y), dtype=np.float64)
    if features.ndim != 2 or features.shape[1:] != (DIMENSIONS,):
        raise ValueError(f"x must have shape (n, {DIMENSIONS})")
    if labels.shape != (features.shape[0],) or features.shape[0] == 0:
        raise ValueError("y must have shape (n,) matching non-empty x")

    per_node: dict[str, float | None] = {}
    status: dict[str, str] = {}
    for node in NODES:
        if node not in models:
            per_node[node], status[node] = None, "missing"
            continue
        try:
            weights = np.asarray(_as_numpy(models[node]), dtype=np.float64)
        except (TypeError, ValueError, RuntimeError):
            per_node[node], status[node] = None, "invalid_model"
            continue
        if weights.shape != (DIMENSIONS,):
            per_node[node], status[node] = None, "invalid_shape"
            continue
        if not np.isfinite(weights).all():
            per_node[node], status[node] = None, "nonfinite_weights"
            continue
        if not np.isfinite(features).all() or not np.isfinite(labels).all():
            per_node[node], status[node] = None, "nonfinite_evaluation_data"
            continue
        with np.errstate(over="ignore", invalid="ignore"):
            residual = features @ weights - labels
            loss = float(0.5 * np.mean(np.square(residual)))
        if not math.isfinite(loss):
            per_node[node], status[node] = None, "nonfinite_loss"
        else:
            per_node[node], status[node] = loss, "ok"

    d_and_c = [per_node[node] for node in ("D", "C")]
    if all(value is not None for value in d_and_c):
        dc_mean = float(sum(d_and_c) / 2.0)
        dc_worst = float(max(d_and_c))
    else:
        dc_mean = dc_worst = None
    return {
        "per_node": per_node,
        "node_status": status,
        "DC_mean": dc_mean,
        "DC_worst": dc_worst,
        "A": per_node["A"],
    }


def _snapshot_metrics(row: Mapping[str, Any]) -> Mapping[str, Any]:
    metrics = row.get("metrics", row)
    return metrics if isinstance(metrics, Mapping) else {}


def _per_node_values(metrics: Mapping[str, Any]) -> Mapping[str, Any]:
    values = metrics.get("per_node", metrics.get("per_node_half_mse"))
    if isinstance(values, Mapping):
        return values
    return metrics


def _threshold_pair(rows: list[Any]) -> tuple[dict[str, float] | None, str | None]:
    source = next((row.get("thresholds") for row in rows
                   if isinstance(row, Mapping) and "thresholds" in row), None)
    if not isinstance(source, Mapping):
        return None, "missing_thresholds"
    if isinstance(source.get("per_node"), Mapping):
        source = source["per_node"]
    result = {}
    for node in ("D", "C"):
        value = source.get(node)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
            return None, f"invalid_threshold_{node}"
        result[node] = float(value)
    return result, None


def _read_node_metric(metrics: Mapping[str, Any], node: str) -> tuple[float | None, str | None]:
    values = _per_node_values(metrics)
    statuses = metrics.get("node_status", {})
    status = statuses.get(node) if isinstance(statuses, Mapping) else None
    if node not in values or values.get(node) is None:
        if isinstance(status, str) and status not in ("missing", "ok"):
            return None, f"invalid_metric_{node}_{status}"
        return None, f"missing_node_{node}"
    value = values[node]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None, f"invalid_metric_{node}_type"
    if not math.isfinite(value):
        return None, f"invalid_metric_{node}_nonfinite"
    if value < 0:
        return None, f"invalid_metric_{node}_negative"
    if isinstance(status, str) and status != "ok":
        return None, f"invalid_metric_{node}_{status}"
    return float(value), None


def _normalized_final_metrics(metrics: Mapping[str, Any]) -> dict[str, Any]:
    per_node, node_status = {}, {}
    for node in NODES:
        value, error = _read_node_metric(metrics, node)
        per_node[node] = value
        if error is None:
            node_status[node] = "ok"
        elif error.startswith(f"missing_node_{node}"):
            node_status[node] = "missing"
        else:
            node_status[node] = error.removeprefix(f"invalid_metric_{node}_")
    dc = [per_node[node] for node in ("D", "C")]
    return {
        "per_node": per_node,
        "node_status": node_status,
        "DC_mean": float(sum(dc) / 2.0) if all(value is not None for value in dc) else None,
        "DC_worst": float(max(dc)) if all(value is not None for value in dc) else None,
        "A": per_node["A"],
    }


def summarize_evaluations(rows, horizon: float = 300.0) -> dict[str, Any]:
    """Summarize the sustained D/C threshold event and final fixed-set metrics.

    Each snapshot has ``time`` (seconds from trial start), ``metrics`` (the
    result of :func:`evaluate_models`), and ``thresholds`` on at least one row.
    Thresholds are absolute half-MSE limits for D and C. Ten consecutive
    evaluations below both limits confirm an event; gaps above 1.5 seconds
    reset the streak. Missing or invalid D/C metrics right-censor an event that
    has not yet been confirmed. Reaching the horizon without confirmation is
    administrative censoring.
    """
    if isinstance(horizon, bool) or not isinstance(horizon, (int, float)) or not math.isfinite(horizon) or horizon < 0:
        raise ValueError("horizon must be a finite non-negative number")
    try:
        snapshots = list(rows)
    except TypeError as exc:
        raise TypeError("rows must be an iterable of snapshot mappings") from exc
    thresholds, threshold_error = _threshold_pair(snapshots)
    result: dict[str, Any] = {
        "horizon": float(horizon),
        "crossing_time": None,
        "confirmed_time": None,
        "censored": False,
        "censor_reason": None,
        "censor_time": None,
        "final_metrics": None,
        "thresholds": thresholds,
        "evaluations_used": 0,
        "gap_resets": 0,
    }
    if threshold_error:
        result.update(censored=True, censor_reason=threshold_error, censor_time=0.0)

    streak = 0
    streak_start = None
    last_time = None
    for row in snapshots:
        if not isinstance(row, Mapping):
            if result["confirmed_time"] is None:
                result.update(censored=True, censor_reason="invalid_snapshot", censor_time=last_time)
            break
        raw_time = row.get("time", row.get("time_seconds", row.get("capture_seconds")))
        if isinstance(raw_time, bool) or not isinstance(raw_time, (int, float)) or not math.isfinite(raw_time):
            if result["confirmed_time"] is None:
                result.update(censored=True, censor_reason="invalid_timestamp", censor_time=last_time)
            break
        timestamp = float(raw_time)
        if timestamp < 0 or (last_time is not None and timestamp <= last_time):
            if result["confirmed_time"] is None:
                result.update(censored=True, censor_reason="invalid_timestamp", censor_time=last_time)
            break
        if timestamp > horizon:
            break

        metrics = _snapshot_metrics(row)
        result["final_metrics"] = _normalized_final_metrics(metrics)
        result["evaluations_used"] += 1
        if threshold_error:
            last_time = timestamp
            continue
        if result["confirmed_time"] is not None:
            last_time = timestamp
            continue

        if last_time is not None and timestamp - last_time > MAX_EVALUATION_GAP_SECONDS:
            streak = 0
            streak_start = None
            result["gap_resets"] += 1
        d_loss, d_error = _read_node_metric(metrics, "D")
        c_loss, c_error = _read_node_metric(metrics, "C")
        if d_error or c_error:
            result.update(
                censored=True,
                censor_reason=d_error or c_error,
                censor_time=timestamp,
            )
            break
        if d_loss < thresholds["D"] and c_loss < thresholds["C"]:
            if streak == 0:
                streak_start = timestamp
            streak += 1
            if streak >= CONFIRMATION_EVALUATIONS:
                result["crossing_time"] = streak_start
                result["confirmed_time"] = timestamp
                result["censored"] = False
                result["censor_reason"] = None
                result["censor_time"] = None
        else:
            streak = 0
            streak_start = None
        last_time = timestamp

    if result["confirmed_time"] is None and not result["censored"]:
        result.update(censored=True, censor_reason="horizon", censor_time=float(horizon))
    return result
