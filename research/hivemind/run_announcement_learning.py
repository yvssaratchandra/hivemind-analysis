#!/usr/bin/env python3
"""Run the E4/E5 numerical announcement-learning simulation suite.

The runner keeps one model per participant, computes local full-batch
gradients, passes those gradients through the validated Hivemind numerical
backend, and applies one local SGD step. It does not model transport timing or
leader exposure. Large experiment runs require a matching E3 validation gate.
"""
from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import math
from pathlib import Path
import shutil
import sys
from typing import Any, Iterable, TextIO

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
SIMULATOR_SRC = ROOT / "research/simulator/src"
if str(SIMULATOR_SRC) not in sys.path:
    sys.path.insert(0, str(SIMULATOR_SRC))

from announcement_learning import (  # noqa: E402
    DISTINCTIVE_FEATURES,
    NODES,
    SCHEDULE_ARMS,
    LearningProblem,
    load_problem,
    make_problem,
    make_schedules,
)
from trainsim.policies.announcements import AnnouncementPolicy  # noqa: E402
from trainsim.protocols.hivemind.numerical import aggregate, announced_views  # noqa: E402


DEFAULT_OUTPUT_ROOT = HERE / "results"
HONEST_NODES = ("A", "D", "C")
VALIDATION_SCHEMA = "announcement-numerical-validation-v1"
SUITE_SCHEMA = "announcement-learning-suite-v1"
ROUNDS = 200
PILOT_SEEDS = (201,)
CONFIRMATION_SEEDS = (202, 203, 204, 205, 206)
CHUNK_ELEMENTS = 4  # 16 bytes at float32
CONVERGENCE_FRACTION = 0.1
CONVERGENCE_STREAK = 10
CORE_SOURCE_PATHS = (
    "research/simulator/src/trainsim/protocols/hivemind/numerical.py",
    "research/simulator/src/trainsim/policies/announcements.py",
    "research/simulator/src/trainsim/protocols/partitioning.py",
    "research/hivemind/announcement_learning.py",
)
RUNNER_SOURCE_PATH = "research/hivemind/run_announcement_learning.py"
METRIC_COLUMNS = (
    "seed", "distribution", "arm", "round", "node", "condition", "target",
    "updated", "test_loss", "exclusive_block_loss", "distance_to_optimum",
    "disagreement_to_honest_mean", "honest_mean_test_loss",
    "honest_worst_test_loss", "honest_mean_disagreement", "nonfinite_metric_fields",
)


def _numpy():
    import numpy as np

    return np


def _json_bytes(value: Any) -> bytes:
    return (json.dumps(_json_safe(value), sort_keys=True, indent=2, allow_nan=False) + "\n").encode("utf-8")


def _json_safe(value: Any) -> Any:
    """Encode non-finite numeric outcomes as explicit strings in raw artifacts."""
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"
        return value
    if hasattr(value, "item"):
        try:
            return _json_safe(value.item())
        except (ValueError, TypeError):
            pass
    return value


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_bytes(_json_bytes(value))
    temporary.replace(path)


def _safe_label(label: str) -> str:
    if not label or Path(label).name != label or label in (".", ".."):
        raise ValueError("label must be one safe path component")
    if any(character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-" for character in label):
        raise ValueError("label may contain only letters, digits, dot, underscore, and hyphen")
    return label


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def source_hashes() -> dict[str, str]:
    """Hash the numerical model, leader policy, partition model, and workload."""
    return {name: _sha256(ROOT / name) for name in CORE_SOURCE_PATHS}


def _verify_validation(path: str | Path) -> dict[str, Any]:
    validation_path = Path(path).resolve()
    validation = json.loads(validation_path.read_text(encoding="utf-8"))
    if validation.get("schema") != VALIDATION_SCHEMA:
        raise ValueError(f"validation schema must be {VALIDATION_SCHEMA!r}")
    if validation.get("valid") is not True:
        raise ValueError("E3 numerical validation is not marked valid")
    if validation.get("errors"):
        raise ValueError("E3 validation contains errors")
    references = validation.get("native_references")
    if not isinstance(references, list) or not references:
        raise ValueError("E3 validation must cite at least one native reference")
    for reference in references:
        if not isinstance(reference, dict) or not reference.get("path"):
            raise ValueError("E3 native reference entries need a path")
        digest = reference.get("artifact_inventory_sha256")
        if not isinstance(digest, str) or len(digest) != 64:
            raise ValueError("E3 native references need a SHA256 artifact inventory")
    expected = source_hashes()
    observed = validation.get("source_hashes")
    if not isinstance(observed, dict):
        raise ValueError("E3 validation is missing source_hashes")
    mismatches = {
        name: {"expected": digest, "observed": observed.get(name)}
        for name, digest in expected.items()
        if observed.get(name) != digest
    }
    if mismatches:
        raise ValueError(f"E3 source hashes do not match the current model: {mismatches}")
    return {
        "path": str(validation_path),
        "sha256": _sha256(validation_path),
        "source_hashes": expected,
        "checks": validation.get("checks", []),
        "native_references": references,
    }


def _problem_contract(problem: LearningProblem) -> dict[str, Any]:
    config = problem.config()
    return {key: value for key, value in config.items() if key not in ("seed", "distribution")}


def _contract_sha256(problem: LearningProblem) -> str:
    encoded = json.dumps(_problem_contract(problem), sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256((encoded + "\n").encode("utf-8")).hexdigest()


def _verify_reviewed_pilot(path: str | Path, *, validation: dict[str, Any], problem: LearningProblem) -> dict[str, Any]:
    pilot_root = Path(path).resolve()
    manifest_path = pilot_root / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("schema") != SUITE_SCHEMA or manifest.get("mode") != "pilot":
        raise ValueError("reviewed pilot must be an announcement-learning pilot suite")
    if manifest.get("status") != "complete" or manifest.get("run_count") != 18:
        raise ValueError("reviewed pilot must contain all 18 completed pilot runs")
    runs = manifest.get("runs")
    if (not isinstance(runs, list) or len(runs) != 18
            or any(row.get("status") not in ("complete", "verified_divergence") for row in runs)):
        raise ValueError("reviewed pilot run catalog is incomplete or contains failed runs")
    if manifest.get("source_hashes") != source_hashes():
        raise ValueError("reviewed pilot source hashes differ from the current model")
    if manifest.get("validation_sha256") != validation["sha256"]:
        raise ValueError("reviewed pilot used a different E3 validation artifact")
    if manifest.get("workload_contract_sha256") != _contract_sha256(problem):
        raise ValueError("reviewed pilot workload contract differs from the current workload")
    if manifest.get("arms") != list(SCHEDULE_ARMS) or manifest.get("distributions") != ["iid", "distinctive"]:
        raise ValueError("reviewed pilot arm or distribution matrix differs")
    if manifest.get("seeds") != list(PILOT_SEEDS) or manifest.get("rounds") != ROUNDS:
        raise ValueError("reviewed pilot seed or round count differs")
    data_entries = manifest.get("data_artifacts")
    if not isinstance(data_entries, dict) or set(data_entries) != {"seed201_iid", "seed201_distinctive"}:
        raise ValueError("reviewed pilot data artifact catalog is incomplete")
    for key, row in data_entries.items():
        directory = pilot_root / row["directory"]
        if _sha256(directory / "learning_manifest.json") != row["manifest_sha256"]:
            raise ValueError(f"reviewed pilot data manifest changed: {key}")
        loaded = load_problem(directory)
        if loaded.hashes() != row["hashes"]:
            raise ValueError(f"reviewed pilot frozen data changed: {key}")
    inventory_path = pilot_root / "artifact_inventory.json"
    if _sha256(inventory_path) != manifest.get("artifact_inventory_sha256"):
        raise ValueError("reviewed pilot artifact inventory fingerprint changed")
    inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
    for relative, digest in inventory.items():
        artifact = (pilot_root / relative).resolve()
        if pilot_root not in artifact.parents or not artifact.is_file() or _sha256(artifact) != digest:
            raise ValueError(f"reviewed pilot artifact changed or escaped its suite: {relative}")
    for row in runs:
        verification_path = pilot_root / row["path"] / "verification.json"
        verification = json.loads(verification_path.read_text(encoding="utf-8"))
        if verification.get("valid") is not True:
            raise ValueError(f"reviewed pilot run failed trace replay: {row['path']}")
    return {"path": str(pilot_root), "manifest_sha256": _sha256(manifest_path)}


def _make_round_inputs(problem: LearningProblem, weights: dict[str, Any]) -> dict[str, Any]:
    return {node: problem.gradient(node, weights[node]) for node in NODES}


def _view_spec(schedule_row: dict[str, Any]) -> tuple[tuple[str, ...], str, str | None]:
    condition, target = schedule_row.get("condition"), schedule_row.get("target")
    if condition == "honest":
        if target is not None:
            raise ValueError("honest schedule rows must have a null target")
        return NODES, "honest", None
    if condition == "benign_omission":
        if target not in ("A", "D", "C"):
            raise ValueError("benign omission target must be A, D, or C")
        return tuple(node for node in NODES if node != target), "honest", target
    if condition == "membership_omission":
        if target != "A":
            raise ValueError("membership omission is defined for participant A")
        return NODES, "membership", target
    if condition == "ordering_corruption":
        if target != "A":
            raise ValueError("ordering corruption is defined for participant A")
        return NODES, "ordering", target
    raise ValueError(f"unsupported schedule condition: {condition!r}")


def _serializable_result(result) -> dict[str, Any]:
    return {
        "values": {node: result.values[node].tolist() for node in NODES},
        "views": {node: list(result.views[node]) for node in result.views},
        "allocations": {
            node: {peer: [list(interval) for interval in intervals]
                   for peer, intervals in peers.items()}
            for node, peers in result.allocations.items()
        },
        "fallback_ranges": {
            node: [list(interval) for interval in intervals]
            for node, intervals in result.fallback_ranges.items()
        },
        "banned_senders": result.banned_senders,
        "trace": result.trace,
    }


def _round_metrics(problem: LearningProblem, weights: dict[str, Any]) -> dict[str, Any]:
    np = _numpy()
    with np.errstate(over="ignore", invalid="ignore"):
        center = np.mean(np.stack([weights[node] for node in HONEST_NODES]), axis=0, dtype=np.float32)
    nonfinite_fields: list[str] = []

    def finite_metric(value: float, label: str):
        value = float(value)
        if not math.isfinite(value):
            nonfinite_fields.append(label)
            return None
        return value

    by_node = {}
    for node in NODES:
        with np.errstate(over="ignore", invalid="ignore"):
            error = np.asarray(weights[node] - problem.target, dtype=np.float32)
            exclusive = error[list(DISTINCTIVE_FEATURES)]
            by_node[node] = {
                "test_loss": finite_metric(problem.evaluate(weights[node]), f"{node}.test_loss"),
                "exclusive_block_loss": finite_metric(
                    np.sum(np.square(exclusive, dtype=np.float32), dtype=np.float32) / 2.0,
                    f"{node}.exclusive_block_loss",
                ),
                "distance_to_optimum": finite_metric(
                    np.linalg.norm(error.astype(np.float64)), f"{node}.distance_to_optimum"),
                "disagreement_to_honest_mean": finite_metric(
                    np.linalg.norm((weights[node] - center).astype(np.float64)),
                    f"{node}.disagreement_to_honest_mean",
                ),
            }
    honest_losses = [by_node[node]["test_loss"] for node in HONEST_NODES]
    by_node["honest_mean_test_loss"] = finite_metric(
        sum(honest_losses) / len(HONEST_NODES) if all(value is not None for value in honest_losses) else math.nan,
        "honest_mean_test_loss",
    )
    by_node["honest_worst_test_loss"] = finite_metric(
        max(honest_losses) if all(value is not None for value in honest_losses) else math.nan,
        "honest_worst_test_loss",
    )
    disagreements = [by_node[node]["disagreement_to_honest_mean"] for node in HONEST_NODES]
    by_node["honest_mean_disagreement"] = finite_metric(
        sum(disagreements) / len(HONEST_NODES) if all(value is not None for value in disagreements) else math.nan,
        "honest_mean_disagreement",
    )
    by_node["nonfinite_metric_fields"] = sorted(set(nonfinite_fields))
    return by_node


def _first_nonfinite(vectors: dict[str, Any]) -> str | None:
    np = _numpy()
    for node in NODES:
        if not bool(np.isfinite(vectors[node]).all()):
            return node
    return None


def _divergence_reason(gradients, numerical, weights_after, metrics) -> dict[str, Any] | None:
    node = _first_nonfinite(gradients)
    if node is not None:
        return {"stage": "gradient", "node": node}
    if numerical is not None:
        node = _first_nonfinite(numerical.values)
        if node is not None:
            return {"stage": "aggregate", "node": node}
    node = _first_nonfinite(weights_after)
    if node is not None:
        return {"stage": "model", "node": node}
    fields = metrics.get("nonfinite_metric_fields", [])
    if fields:
        return {"stage": "metric", "field": fields[0], "fields": fields}
    return None


def _summary(problem: LearningProblem, rounds: list[dict[str, Any]], scheduled_rounds: int) -> dict[str, Any]:
    initial = _round_metrics(problem, {node: problem.initial_weights for node in NODES})
    initial_mean = initial["honest_mean_test_loss"]
    if initial_mean is None:
        raise ValueError("initial honest model evaluation is nonfinite")
    threshold = initial_mean * CONVERGENCE_FRACTION
    streak_start: int | None = None
    streak_length = 0
    convergence_start: int | None = None
    convergence_confirmed: int | None = None
    divergence = None
    for row in rounds:
        round_index = row["round"]
        loss = row["metrics"]["honest_mean_test_loss"]
        if row.get("divergence") is not None or loss is None:
            divergence = row.get("divergence") or {"round": round_index, "stage": "metric"}
            break
        if loss <= threshold:
            if streak_start is None:
                streak_start = round_index
                streak_length = 0
            streak_length += 1
            if streak_length >= CONVERGENCE_STREAK:
                convergence_start = streak_start
                convergence_confirmed = round_index
                break
        else:
            streak_start = None
            streak_length = 0
    final_metrics = rounds[-1]["metrics"] if rounds else initial
    per_node = {}
    for node in NODES:
        values = [row["metrics"][node] for row in rounds
                  if row["metrics"][node]["test_loss"] is not None]
        per_node[node] = {
            "initial_test_loss": initial[node]["test_loss"],
            "final_test_loss": final_metrics[node]["test_loss"],
            "trajectory_mean_test_loss": (
                float(sum(row["metrics"][node]["test_loss"] for row in rounds) / len(rounds))
                if rounds and all(row["metrics"][node]["test_loss"] is not None for row in rounds)
                else (float(sum(value["test_loss"] for value in values) / len(values))
                      if values else None)
            ),
            "final_exclusive_block_loss": final_metrics[node]["exclusive_block_loss"],
            "final_distance_to_optimum": final_metrics[node]["distance_to_optimum"],
            "final_disagreement_to_honest_mean": final_metrics[node]["disagreement_to_honest_mean"],
        }
    return {
        "rounds": len(rounds),
        "scheduled_rounds": scheduled_rounds,
        "gradient_evaluations": len(rounds) * len(NODES),
        "initial_honest_mean_test_loss": initial_mean,
        "convergence_threshold": threshold,
        "convergence_streak_rounds": CONVERGENCE_STREAK,
        "first_round_sustained_below_threshold": convergence_start,
        "round_when_sustained_threshold_confirmed": convergence_confirmed,
        "convergence_censored": convergence_start is None,
        "censor_reason": (divergence["stage"] if divergence is not None
                          else ("horizon_reached" if convergence_start is None else None)),
        "trajectory_status": "diverged_nonfinite" if divergence is not None else "completed",
        "stopped_early": divergence is not None and len(rounds) < scheduled_rounds,
        "divergence": divergence,
        "final_honest_mean_test_loss": final_metrics["honest_mean_test_loss"],
        "final_honest_worst_test_loss": final_metrics["honest_worst_test_loss"],
        "trajectory_mean_honest_test_loss": (
            float(sum(row["metrics"]["honest_mean_test_loss"] for row in rounds
                      if row["metrics"]["honest_mean_test_loss"] is not None)
                      / sum(row["metrics"]["honest_mean_test_loss"] is not None for row in rounds))
            if any(row["metrics"]["honest_mean_test_loss"] is not None for row in rounds)
            else initial_mean
        ),
        "final_honest_mean_disagreement": final_metrics["honest_mean_disagreement"],
        "final_honest_mean_exclusive_block_loss": float(
            sum(final_metrics[node]["exclusive_block_loss"] for node in HONEST_NODES) / len(HONEST_NODES)
        ),
        "participants": per_node,
    }


def _open_trace(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        raise FileExistsError(f"trace output already exists: {path}")
    if path.suffix == ".gz":
        return gzip.open(path, "wt", encoding="utf-8", newline="\n")
    return path.open("w", encoding="utf-8", newline="\n")


def _write_metric_rows(writer: csv.DictWriter | None, *, problem: LearningProblem, arm: str,
                       schedule_row: dict[str, Any], metrics: dict[str, Any]) -> None:
    if writer is None:
        return
    for node in NODES:
        values = metrics[node]
        writer.writerow({
            "seed": problem.seed,
            "distribution": problem.distribution,
            "arm": arm,
            "round": schedule_row["round"],
            "node": node,
            "condition": schedule_row["condition"],
            "target": schedule_row["target"] or "",
            "updated": True,
            **values,
            "honest_mean_test_loss": metrics["honest_mean_test_loss"],
            "honest_worst_test_loss": metrics["honest_worst_test_loss"],
            "honest_mean_disagreement": metrics["honest_mean_disagreement"],
            "nonfinite_metric_fields": ";".join(metrics.get("nonfinite_metric_fields", [])),
        })


def run_case(
    problem: LearningProblem,
    arm: str,
    schedule: Iterable[dict[str, Any]] | None = None,
    *,
    trace_path: str | Path | None = None,
    metrics_path: str | Path | None = None,
) -> dict[str, Any]:
    """Run one schedule; useful for tiny tests and full 200-round suite cases."""
    if arm not in SCHEDULE_ARMS:
        raise ValueError(f"arm must be one of {SCHEDULE_ARMS}")
    rows = tuple(schedule if schedule is not None else make_schedules(problem.seed, ROUNDS)[arm])
    if not rows:
        raise ValueError("schedule must include at least one round")
    if trace_path is None and metrics_path is not None:
        raise ValueError("metrics_path requires a trace_path")
    destination = Path(trace_path) if trace_path is not None else None
    metrics_destination = Path(metrics_path) if metrics_path is not None else None
    if metrics_destination is not None and metrics_destination.exists():
        raise FileExistsError(f"metrics output already exists: {metrics_destination}")
    weights = {node: problem.initial_weights for node in NODES}
    metric_rounds: list[dict[str, Any]] = []
    metrics_file: TextIO | None = None
    metric_writer: csv.DictWriter | None = None
    if metrics_destination is not None:
        metrics_destination.parent.mkdir(parents=True, exist_ok=True)
        metrics_file = metrics_destination.open("w", encoding="utf-8", newline="")
        metric_writer = csv.DictWriter(metrics_file, fieldnames=METRIC_COLUMNS)
        metric_writer.writeheader()
    trace_file = _open_trace(destination) if destination is not None else None
    try:
        for expected_round, schedule_row in enumerate(rows, start=1):
            if schedule_row.get("round") != expected_round:
                raise ValueError("schedule rounds must be contiguous and one-indexed")
            active_members, policy_kind, target = _view_spec(schedule_row)
            before = {node: value.copy() for node, value in weights.items()}
            policy = AnnouncementPolicy(policy_kind, target or "A")
            group_id = f"seed{problem.seed}:{problem.distribution}:{arm}:round{expected_round}"
            views = announced_views(active_members, "B", policy, group_id=group_id)
            np = _numpy()
            with np.errstate(over="ignore", invalid="ignore"):
                inputs = _make_round_inputs(problem, before)
            if _first_nonfinite(inputs) is not None:
                numerical = None
                weights = before
                metrics = _round_metrics(problem, weights)
            else:
                with np.errstate(over="ignore", invalid="ignore"):
                    numerical = aggregate(inputs, views, chunk_elements=CHUNK_ELEMENTS)
                    weights = {node: problem.update(before[node], numerical.values[node]) for node in NODES}
                    metrics = _round_metrics(problem, weights)
            divergence = _divergence_reason(inputs, numerical, weights, metrics)
            _write_metric_rows(metric_writer, problem=problem, arm=arm,
                               schedule_row=schedule_row, metrics=metrics)
            record = {
                "schema": "announcement-learning-round-v1",
                "seed": problem.seed,
                "distribution": problem.distribution,
                "arm": arm,
                "round": expected_round,
                "scheduled_rounds": len(rows),
                "schedule": schedule_row,
                "active_members": list(active_members),
                "local_only_target": target if policy_kind == "honest" and len(active_members) == 3 else None,
                "weights_before": {node: before[node].tolist() for node in NODES},
                "input_gradients": {node: inputs[node].tolist() for node in NODES},
                "numerical_result": _serializable_result(numerical) if numerical is not None else None,
                "weights_after": {node: weights[node].tolist() for node in NODES},
                "metrics": metrics,
                "divergence": divergence,
                "work": {"gradient_evaluations": len(NODES), "round": expected_round},
            }
            if trace_file is not None:
                trace_file.write(json.dumps(_json_safe(record), sort_keys=True,
                                           separators=(",", ":"), allow_nan=False) + "\n")
                trace_file.flush()
            metric_rounds.append(record)
            if divergence is not None:
                break
    finally:
        if trace_file is not None:
            trace_file.close()
        if metrics_file is not None:
            metrics_file.close()
    return _summary(problem, metric_rounds, len(rows))


def _assert_vector_equal(expected: Any, observed: Any, *, label: str) -> None:
    np = _numpy()
    expected_array = np.asarray(expected, dtype=np.float32)
    observed_array = np.asarray(observed, dtype=np.float32)
    if expected_array.shape != observed_array.shape or not np.array_equal(expected_array, observed_array, equal_nan=True):
        raise ValueError(f"trace replay {label} mismatch")


def _normalized_result(result) -> dict[str, Any]:
    return _serializable_result(result)


def verify_trace(problem: LearningProblem, trace_path: str | Path, *, expected_rounds: int | None = None) -> dict[str, Any]:
    """Independently recompute gradients, rosters, reductions, and SGD updates."""
    path = Path(trace_path)
    opener = gzip.open if path.suffix == ".gz" else open
    weights = {node: problem.initial_weights for node in NODES}
    metric_rounds: list[dict[str, Any]] = []
    count = 0
    scheduled_rounds = None
    diverged = False
    with opener(path, "rt", encoding="utf-8") as stream:
        for line in stream:
            if not line.strip():
                continue
            if diverged:
                raise ValueError("trace continues after the first nonfinite event")
            row = json.loads(line)
            count += 1
            if row.get("schema") != "announcement-learning-round-v1":
                raise ValueError("trace replay found an unsupported row schema")
            if row.get("seed") != problem.seed or row.get("distribution") != problem.distribution:
                raise ValueError("trace replay problem seed/distribution mismatch")
            if row.get("round") != count:
                raise ValueError("trace replay rounds are not contiguous")
            current_scheduled_rounds = row.get("scheduled_rounds")
            if not isinstance(current_scheduled_rounds, int) or current_scheduled_rounds < count:
                raise ValueError("trace replay has invalid scheduled-round metadata")
            if scheduled_rounds is None:
                scheduled_rounds = current_scheduled_rounds
            elif scheduled_rounds != current_scheduled_rounds:
                raise ValueError("trace replay scheduled-round metadata changed")
            arm = row.get("arm")
            if arm not in SCHEDULE_ARMS:
                raise ValueError("trace replay found an unknown experiment arm")
            schedule = row.get("schedule")
            active_members, policy_kind, target = _view_spec(schedule)
            if row.get("active_members") != list(active_members):
                raise ValueError("trace replay active membership mismatch")
            for node in NODES:
                _assert_vector_equal(weights[node], row["weights_before"][node], label=f"round {count} {node} pre-state")
            gradients = _make_round_inputs(problem, weights)
            for node in NODES:
                _assert_vector_equal(gradients[node], row["input_gradients"][node], label=f"round {count} {node} gradient")
            if _first_nonfinite(gradients) is not None:
                numerical = None
                after = {node: value.copy() for node, value in weights.items()}
                metrics = _round_metrics(problem, after)
            else:
                policy = AnnouncementPolicy(policy_kind, target or "A")
                group_id = f"seed{problem.seed}:{problem.distribution}:{arm}:round{count}"
                views = announced_views(active_members, "B", policy, group_id=group_id)
                with _numpy().errstate(over="ignore", invalid="ignore"):
                    numerical = aggregate(gradients, views, chunk_elements=CHUNK_ELEMENTS)
                    after = {node: problem.update(weights[node], numerical.values[node]) for node in NODES}
                    metrics = _round_metrics(problem, after)
            expected_numerical = _json_safe(_normalized_result(numerical)) if numerical is not None else None
            if row.get("numerical_result") != expected_numerical:
                raise ValueError(f"trace replay numerical aggregation mismatch at round {count}")
            divergence = _divergence_reason(gradients, numerical, after, metrics)
            if _json_safe(divergence) != row.get("divergence"):
                raise ValueError(f"trace replay divergence marker mismatch at round {count}")
            for node in NODES:
                _assert_vector_equal(after[node], row["weights_after"][node], label=f"round {count} {node} update")
            if _json_safe(metrics) != row.get("metrics"):
                raise ValueError(f"trace replay metrics mismatch at round {count}")
            metric_rounds.append({"round": count, "metrics": metrics, "divergence": divergence})
            weights = after
            diverged = divergence is not None
    if count == 0:
        raise ValueError("trace contains no rounds")
    if expected_rounds is not None and scheduled_rounds != expected_rounds:
        raise ValueError(f"trace schedules {scheduled_rounds} rounds; expected {expected_rounds}")
    if count != scheduled_rounds and not diverged:
        raise ValueError(f"trace stopped after {count}/{scheduled_rounds} rounds without a nonfinite event")
    return {
        "valid": True,
        "rounds": count,
        "scheduled_rounds": scheduled_rounds,
        "diverged_nonfinite": diverged,
        "gradient_evaluations_reconstructed": count * len(NODES),
        "summary": _summary(problem, metric_rounds, scheduled_rounds),
    }


def _freeze_sources(destination: Path, hashes: dict[str, str]) -> None:
    frozen = destination / "harness"
    for name in (*CORE_SOURCE_PATHS, RUNNER_SOURCE_PATH):
        source = ROOT / name
        data = source.read_bytes()
        if hashlib.sha256(data).hexdigest() != (hashes.get(name) if name in hashes else _sha256(source)):
            raise ValueError(f"source changed while suite was being frozen: {name}")
        target = frozen / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)


def _artifact_inventory(destination: Path) -> dict[str, str]:
    inventory = {}
    for path in sorted(destination.rglob("*")):
        if path.is_file() and path.name not in ("artifact_inventory.json", "manifest.json"):
            inventory[path.relative_to(destination).as_posix()] = _sha256(path)
    _write_json(destination / "artifact_inventory.json", inventory)
    return inventory


def run_suite(
    *,
    label: str,
    mode: str,
    validation_path: str | Path,
    reviewed_pilot: str | Path | None = None,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
) -> Path:
    """Run pilot or confirmation after the matching E3 gate has passed."""
    label = _safe_label(label)
    if mode not in ("pilot", "confirmation"):
        raise ValueError("mode must be pilot or confirmation")
    if mode == "confirmation" and reviewed_pilot is None:
        raise ValueError("confirmation mode requires --reviewed-pilot")
    validation = _verify_validation(validation_path)
    seeds = PILOT_SEEDS if mode == "pilot" else CONFIRMATION_SEEDS
    probe = make_problem(seeds[0], "iid")
    pilot = (_verify_reviewed_pilot(reviewed_pilot, validation=validation, problem=probe)
             if mode == "confirmation" and reviewed_pilot is not None else None)

    destination = Path(output_root) / f"announcement_learning_{label}"
    if destination.exists():
        raise FileExistsError(f"suite output already exists: {destination}")
    destination.mkdir(parents=True)
    hashes = source_hashes()
    runner_hash = _sha256(ROOT / RUNNER_SOURCE_PATH)
    _freeze_sources(destination, {**hashes, RUNNER_SOURCE_PATH: runner_hash})
    manifest: dict[str, Any] = {
        "schema": SUITE_SCHEMA,
        "label": label,
        "mode": mode,
        "status": "running",
        "seeds": list(seeds),
        "rounds": ROUNDS,
        "arms": list(SCHEDULE_ARMS),
        "distributions": ["iid", "distinctive"],
        "leader": "B",
        "honest_metrics_nodes": list(HONEST_NODES),
        "source_hashes": hashes,
        "runner_sha256": runner_hash,
        "validation_path": validation["path"],
        "validation_sha256": validation["sha256"],
        "workload_contract_sha256": _contract_sha256(probe),
        "validation_source_hashes": validation["source_hashes"],
        "reviewed_pilot": pilot,
        "data_artifacts": {},
        "runs": [],
        "run_count": 0,
        "attack_exposure": "conditional; leader B is designated",
        "timing_claims": "none; fixed rounds and gradient evaluations only",
        "parameter_synchronization": False,
        "recovery_interpretation": (
            "honest regrouping averages gradients but does not synchronize participant parameters; "
            "existing model differences may persist"
        ),
        "workload_contract": _problem_contract(probe),
    }
    _write_json(destination / "manifest.json", manifest)
    try:
        for seed in seeds:
            schedules = make_schedules(seed, ROUNDS)
            _write_json(destination / "schedules" / f"seed{seed}.json", schedules)
            for distribution in ("iid", "distinctive"):
                problem = make_problem(seed, distribution)
                data_dir = destination / "problems" / f"seed{seed}_{distribution}"
                problem.save(data_dir)
                data_key = f"seed{seed}_{distribution}"
                manifest["data_artifacts"][data_key] = {
                    "directory": data_dir.relative_to(destination).as_posix(),
                    "manifest_sha256": _sha256(data_dir / "learning_manifest.json"),
                    "hashes": problem.hashes(),
                }
                for arm in SCHEDULE_ARMS:
                    run_path = destination / "runs" / f"seed{seed}" / distribution / arm
                    run_path.mkdir(parents=True)
                    entry = {
                        "seed": seed,
                        "distribution": distribution,
                        "arm": arm,
                        "path": run_path.relative_to(destination).as_posix(),
                        "status": "running",
                    }
                    manifest["runs"].append(entry)
                    _write_json(destination / "manifest.json", manifest)
                    trace_path = run_path / "trace.jsonl.gz"
                    metrics_path = run_path / "metrics.csv"
                    try:
                        summary = run_case(problem, arm, schedules[arm],
                                           trace_path=trace_path, metrics_path=metrics_path)
                        verification = verify_trace(problem, trace_path, expected_rounds=ROUNDS)
                        if not verification["valid"] or verification["summary"] != summary:
                            raise ValueError("independent replay summary differs from the run summary")
                        _write_json(run_path / "summary.json", summary)
                        _write_json(run_path / "verification.json", verification)
                        if arm == "baseline":
                            final_loss = summary["final_honest_mean_test_loss"]
                            initial_loss = summary["initial_honest_mean_test_loss"]
                            if (summary["trajectory_status"] != "completed" or final_loss is None
                                    or not math.isfinite(final_loss) or final_loss >= initial_loss):
                                entry.update(status="baseline_learning_gate_failed", rounds=summary["rounds"],
                                             summary="baseline must finish finite with lower honest mean loss")
                                _write_json(destination / "manifest.json", manifest)
                                raise RuntimeError("baseline failed the finite learning and loss-reduction gate")
                        outcome = ("verified_divergence" if summary["trajectory_status"] != "completed"
                                   else "complete")
                        entry.update(status=outcome, rounds=summary["rounds"],
                                     gradient_evaluations=summary["gradient_evaluations"])
                        if summary["divergence"] is not None:
                            entry["divergence"] = summary["divergence"]
                        manifest["run_count"] += 1
                        manifest["verified_divergence_count"] = manifest.get("verified_divergence_count", 0) + int(
                            summary["trajectory_status"] != "completed")
                        _write_json(destination / "manifest.json", manifest)
                    except Exception as exc:
                        if entry.get("status") == "running":
                            entry.update(status="failed")
                        entry.update(error=type(exc).__name__, message=str(exc))
                        manifest["status"] = "failed"
                        _write_json(destination / "manifest.json", manifest)
                        raise
        manifest["status"] = "complete"
        manifest["source_hashes_after_run"] = source_hashes()
        if manifest["source_hashes_after_run"] != manifest["source_hashes"]:
            raise ValueError("validated source changed while the suite was running")
        _write_json(destination / "manifest.json", manifest)
        inventory = _artifact_inventory(destination)
        manifest["artifact_inventory_sha256"] = hashlib.sha256(_json_bytes(inventory)).hexdigest()
        _write_json(destination / "manifest.json", manifest)
        _artifact_inventory(destination)
        return destination
    except Exception as exc:
        manifest["status"] = "failed"
        manifest["failure"] = {"type": type(exc).__name__, "message": str(exc)}
        manifest["source_hashes_after_failure"] = source_hashes()
        _write_json(destination / "manifest.json", manifest)
        _artifact_inventory(destination)
        raise


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--label", required=True)
    parser.add_argument("--mode", choices=("pilot", "confirmation"), required=True)
    parser.add_argument("--reviewed-pilot", type=Path)
    parser.add_argument("--validation", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    try:
        path = run_suite(label=args.label, mode=args.mode, validation_path=args.validation,
                         reviewed_pilot=args.reviewed_pilot, output_root=args.output_root)
    except (ValueError, FileExistsError, OSError, RuntimeError) as exc:
        parser.error(str(exc))
    print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
