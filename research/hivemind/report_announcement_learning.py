#!/usr/bin/env python3
"""Summarize inventory-verified E4/E5 announcement-learning suites.

This report describes fixed-round simulation outcomes only. It makes no timing,
deployment, or population-generalization claims.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from pathlib import Path
import statistics
from typing import Any


SUITE_SCHEMA = "announcement-learning-suite-v1"
REPORT_SCHEMA = "announcement-learning-report-v1"
ARM_ORDER = (
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
DISTRIBUTIONS = ("iid", "distinctive")
PILOT_SEEDS = (201,)
CONFIRMATION_SEEDS = (202, 203, 204, 205, 206)
NODES = ("B", "D", "A", "C")
HONEST_NODES = ("A", "D", "C")
ROUNDS = 200
RECOVERY_ATTACK_ROUNDS = 100
CONVERGENCE_STREAK = 10
INVENTORY_EXCLUSIONS = {"manifest.json", "artifact_inventory.json"}
METRIC_FIELDS = (
    "test_loss",
    "exclusive_block_loss",
    "distance_to_optimum",
    "disagreement_to_honest_mean",
    "honest_mean_test_loss",
    "honest_worst_test_loss",
    "honest_mean_disagreement",
)
METRIC_COLUMNS = (
    "seed", "distribution", "arm", "round", "node", "condition", "target",
    "updated", *METRIC_FIELDS, "nonfinite_metric_fields",
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _json_bytes(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + "\n").encode("utf-8")


def _reject_constant(value: str):
    raise ValueError(f"non-standard JSON numeric constant {value}")


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"), parse_constant=_reject_constant)


def _safe_relative(value: Any, *, label: str) -> Path:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{label} must be a nonempty relative path")
    relative = Path(value)
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError(f"{label} must remain inside its suite: {value!r}")
    return relative


def _inside(path: Path, root: Path) -> bool:
    return path == root or root in path.parents


def _verify_inventory(root: Path, manifest: dict[str, Any]) -> dict[str, str]:
    inventory_path = root / "artifact_inventory.json"
    if not inventory_path.is_file():
        raise ValueError(f"suite inventory is missing: {root}")
    inventory = _read_json(inventory_path)
    if not isinstance(inventory, dict):
        raise ValueError(f"suite inventory must be a path-to-SHA256 object: {root}")
    expected_inventory_hash = manifest.get("artifact_inventory_sha256")
    observed_inventory_hash = hashlib.sha256(_json_bytes(inventory)).hexdigest()
    if expected_inventory_hash != observed_inventory_hash:
        raise ValueError(f"suite inventory fingerprint does not match manifest: {root}")

    for relative, digest in inventory.items():
        path = _safe_relative(relative, label="inventory path")
        if relative in INVENTORY_EXCLUSIONS:
            raise ValueError(f"suite inventory must not list {relative}")
        if not isinstance(digest, str) or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
            raise ValueError(f"invalid SHA256 in suite inventory for {relative}")
        artifact = root / path
        if not artifact.is_file() or not _inside(artifact.resolve(), root):
            raise ValueError(f"inventoried suite artifact is missing or escapes its directory: {relative}")
        if _sha256(artifact) != digest:
            raise ValueError(f"suite artifact hash mismatch: {relative}")

    actual: dict[str, str] = {}
    for artifact in sorted(root.rglob("*")):
        if not artifact.is_file() or artifact.name in INVENTORY_EXCLUSIONS or "__pycache__" in artifact.parts:
            continue
        if not _inside(artifact.resolve(), root):
            raise ValueError(f"suite artifact escapes its directory: {artifact}")
        relative = artifact.relative_to(root).as_posix()
        actual[relative] = _sha256(artifact)
    if actual != inventory:
        missing = sorted(set(actual) - set(inventory))
        unlisted = sorted(set(inventory) - set(actual))
        raise ValueError(f"suite inventory file set differs (unlisted={missing}, missing={unlisted})")
    return inventory


def _number(value: Any, *, label: str, nullable: bool = False) -> float | None:
    if value is None and nullable:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
        raise ValueError(f"{label} must be a finite number" + (" or null" if nullable else ""))
    return float(value)


def _csv_number(value: str, *, label: str, nonfinite_fields: set[str], field: str) -> float | None:
    if value == "":
        if field not in nonfinite_fields:
            raise ValueError(f"{label} is blank without a matching nonfinite_metric_fields entry")
        return None
    try:
        parsed = float(value)
    except ValueError as exc:
        raise ValueError(f"{label} is not numeric: {value!r}") from exc
    if not math.isfinite(parsed):
        raise ValueError(f"{label} contains an unmarked nonfinite value")
    return parsed


def _same_metric(left: float | None, right: Any, *, label: str) -> bool:
    expected = _number(right, label=label, nullable=True)
    if left is None or expected is None:
        return left is None and expected is None
    return math.isclose(left, expected, rel_tol=1e-12, abs_tol=1e-12)


def _read_metrics(path: Path, *, seed: int, distribution: str, arm: str,
                  expected_rounds: int) -> dict[int, dict[str, dict[str, Any]]]:
    result: dict[int, dict[str, dict[str, Any]]] = {}
    with path.open("r", encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        if tuple(reader.fieldnames or ()) != METRIC_COLUMNS:
            raise ValueError(f"unsupported metrics.csv columns: {path}")
        for row in reader:
            if int(row["seed"]) != seed or row["distribution"] != distribution or row["arm"] != arm:
                raise ValueError(f"metrics.csv case identity differs from manifest: {path}")
            round_index = int(row["round"])
            if not 1 <= round_index <= expected_rounds:
                raise ValueError(f"metrics.csv has an out-of-range round in {path}")
            node = row["node"]
            if node not in NODES:
                raise ValueError(f"metrics.csv has an unknown node {node!r}: {path}")
            round_rows = result.setdefault(round_index, {})
            if node in round_rows:
                raise ValueError(f"metrics.csv has duplicate {node} round {round_index}: {path}")
            raw_nonfinite = row.get("nonfinite_metric_fields", "")
            nonfinite = {item for item in raw_nonfinite.split(";") if item}
            unknown = nonfinite - {
                *(f"{peer}.{field}" for peer in NODES for field in METRIC_FIELDS[:4]),
                *METRIC_FIELDS[4:],
            }
            if unknown:
                raise ValueError(f"metrics.csv has unknown nonfinite field names {sorted(unknown)}: {path}")
            parsed = dict(row)
            parsed["round"] = round_index
            parsed["nonfinite_set"] = nonfinite
            for field in METRIC_FIELDS:
                nonfinite_label = f"{node}.{field}" if field in METRIC_FIELDS[:4] else field
                parsed[field] = _csv_number(
                    row[field], label=f"{path}:{round_index}:{node}:{field}",
                    nonfinite_fields=nonfinite, field=nonfinite_label,
                )
            round_rows[node] = parsed
    if sorted(result) != list(range(1, expected_rounds + 1)):
        raise ValueError(f"metrics.csv round catalog is incomplete: {path}")
    for round_index, rows in result.items():
        if set(rows) != set(NODES):
            raise ValueError(f"metrics.csv must have one row per node at round {round_index}: {path}")
        nonfinite_sets = {tuple(sorted(row["nonfinite_set"])) for row in rows.values()}
        if len(nonfinite_sets) != 1:
            raise ValueError(f"metrics.csv nonfinite field markers disagree within round {round_index}: {path}")
        for field in METRIC_FIELDS[4:]:
            values = [row[field] for row in rows.values()]
            if any(value != values[0] for value in values[1:]):
                raise ValueError(f"metrics.csv repeated {field} differs across nodes at round {round_index}: {path}")
    return result


def _check_summary_against_metrics(summary: dict[str, Any], metrics: dict[int, dict[str, dict[str, Any]]], *, path: Path) -> None:
    final = metrics[max(metrics)]
    participants = summary.get("participants")
    if not isinstance(participants, dict):
        raise ValueError(f"summary has no per-participant metrics: {path}")
    mapping = {
        "test_loss": "final_test_loss",
        "exclusive_block_loss": "final_exclusive_block_loss",
        "distance_to_optimum": "final_distance_to_optimum",
        "disagreement_to_honest_mean": "final_disagreement_to_honest_mean",
    }
    for node in NODES:
        record = participants.get(node)
        if not isinstance(record, dict):
            raise ValueError(f"summary lacks participant {node}: {path}")
        for column, key in mapping.items():
            if not _same_metric(final[node][column], record.get(key), label=f"{path}:{node}:{key}"):
                raise ValueError(f"summary and metrics.csv differ for {node}.{key}: {path}")
    for column, key in (
        ("honest_mean_test_loss", "final_honest_mean_test_loss"),
        ("honest_worst_test_loss", "final_honest_worst_test_loss"),
        ("honest_mean_disagreement", "final_honest_mean_disagreement"),
    ):
        if not _same_metric(final[NODES[0]][column], summary.get(key), label=f"{path}:{key}"):
            raise ValueError(f"summary and metrics.csv differ for {key}: {path}")


def _verify_suite(path: str | Path, expected_mode: str) -> dict[str, Any]:
    root = Path(path).resolve()
    if not root.is_dir():
        raise ValueError(f"{expected_mode} suite directory is missing: {root}")
    manifest_path = root / "manifest.json"
    manifest = _read_json(manifest_path)
    if not isinstance(manifest, dict) or manifest.get("schema") != SUITE_SCHEMA:
        raise ValueError(f"suite manifest schema must be {SUITE_SCHEMA!r}: {root}")
    if manifest.get("mode") != expected_mode or manifest.get("status") != "complete":
        raise ValueError(f"{expected_mode} suite must be marked complete: {root}")
    inventory = _verify_inventory(root, manifest)
    expected_seeds = PILOT_SEEDS if expected_mode == "pilot" else CONFIRMATION_SEEDS
    if manifest.get("seeds") != list(expected_seeds):
        raise ValueError(f"{expected_mode} seed list differs from the frozen suite design")
    if manifest.get("arms") != list(ARM_ORDER) or manifest.get("distributions") != list(DISTRIBUTIONS):
        raise ValueError(f"{expected_mode} arm or distribution matrix differs from the frozen design")
    if manifest.get("rounds") != ROUNDS:
        raise ValueError(f"{expected_mode} scheduled-round count differs from {ROUNDS}")
    expected_keys = {
        (seed, distribution, arm)
        for seed in expected_seeds
        for distribution in DISTRIBUTIONS
        for arm in ARM_ORDER
    }
    runs = manifest.get("runs")
    if not isinstance(runs, list) or len(runs) != len(expected_keys) or manifest.get("run_count") != len(expected_keys):
        raise ValueError(f"{expected_mode} manifest does not contain the complete run matrix")
    cases: dict[tuple[str, int, str], dict[str, Any]] = {}
    run_paths: set[str] = set()
    observed_keys: set[tuple[int, str, str]] = set()
    divergence_count = 0
    for row in runs:
        if not isinstance(row, dict):
            raise ValueError(f"{expected_mode} run catalog contains a malformed entry")
        seed, distribution, arm = row.get("seed"), row.get("distribution"), row.get("arm")
        key = (seed, distribution, arm)
        status = row.get("status")
        relative = row.get("path")
        if key not in expected_keys or key in observed_keys:
            raise ValueError(f"{expected_mode} run catalog has an unknown or duplicate case: {key}")
        if status not in ("complete", "verified_divergence"):
            raise ValueError(f"{expected_mode} contains a non-completed run: {key} ({status})")
        relative_path = _safe_relative(relative, label="run path")
        run_directory = (root / relative_path).resolve()
        if not _inside(run_directory, root) or not run_directory.is_dir() or relative in run_paths:
            raise ValueError(f"{expected_mode} run path is missing, duplicated, or outside its suite: {relative}")
        observed_keys.add(key)
        run_paths.add(relative)
        for filename in ("trace.jsonl.gz", "metrics.csv", "summary.json", "verification.json"):
            if not (run_directory / filename).is_file():
                raise ValueError(f"{expected_mode} run artifact is missing: {relative}/{filename}")
        summary_path = run_directory / "summary.json"
        verification_path = run_directory / "verification.json"
        summary = _read_json(summary_path)
        verification = _read_json(verification_path)
        trajectory_status = summary.get("trajectory_status")
        diverged = trajectory_status == "diverged_nonfinite"
        if trajectory_status not in ("completed", "diverged_nonfinite"):
            raise ValueError(f"run has an unsupported trajectory status: {relative}")
        expected_status = "verified_divergence" if diverged else "complete"
        if status != expected_status:
            raise ValueError(f"manifest status does not match verified trajectory outcome: {relative}")
        if (
            verification.get("valid") is not True
            or verification.get("summary") != summary
            or verification.get("scheduled_rounds") != ROUNDS
            or verification.get("rounds") != summary.get("rounds")
            or verification.get("diverged_nonfinite") is not diverged
        ):
            raise ValueError(f"run has missing or inconsistent independent trace verification: {relative}")
        rounds = summary.get("rounds")
        if isinstance(rounds, bool) or not isinstance(rounds, int) or not 1 <= rounds <= ROUNDS:
            raise ValueError(f"run summary has invalid completed round count: {relative}")
        if summary.get("scheduled_rounds") != ROUNDS:
            raise ValueError(f"run summary scheduled horizon differs: {relative}")
        if row.get("rounds") != rounds or row.get("gradient_evaluations") != rounds * len(NODES):
            raise ValueError(f"manifest run counters differ from its summary: {relative}")
        if diverged:
            divergence_count += 1
            if not isinstance(summary.get("divergence"), dict):
                raise ValueError(f"verified divergence lacks a divergence marker: {relative}")
        elif summary.get("divergence") is not None:
            raise ValueError(f"completed finite trajectory unexpectedly has a divergence marker: {relative}")
        for metric in (
            "initial_honest_mean_test_loss", "final_honest_mean_test_loss", "final_honest_worst_test_loss",
            "trajectory_mean_honest_test_loss", "final_honest_mean_disagreement",
            "final_honest_mean_exclusive_block_loss", "convergence_threshold",
        ):
            _number(summary.get(metric), label=f"{relative}:{metric}", nullable=True)
        metrics = _read_metrics(
            run_directory / "metrics.csv", seed=seed, distribution=distribution, arm=arm,
            expected_rounds=rounds,
        )
        _check_summary_against_metrics(summary, metrics, path=summary_path)
        case = _case_record(expected_mode, seed, distribution, arm, status, summary, metrics)
        cases[(distribution, seed, arm)] = case
    if observed_keys != expected_keys:
        raise ValueError(f"{expected_mode} run matrix is missing cases")
    if manifest.get("verified_divergence_count") != divergence_count:
        raise ValueError(f"{expected_mode} verified-divergence count differs from run catalog")
    return {
        "mode": expected_mode,
        "path": str(root),
        "manifest_sha256": _sha256(manifest_path),
        "inventory_sha256": manifest["artifact_inventory_sha256"],
        "source_hashes": manifest.get("source_hashes"),
        "runner_sha256": manifest.get("runner_sha256"),
        "validation_sha256": manifest.get("validation_sha256"),
        "workload_contract_sha256": manifest.get("workload_contract_sha256"),
        "manifest": manifest,
        "inventory": inventory,
        "cases": cases,
        "verified_divergence_count": divergence_count,
    }


def _case_record(suite: str, seed: int, distribution: str, arm: str, status: str,
                 summary: dict[str, Any], metrics: dict[int, dict[str, dict[str, Any]]]) -> dict[str, Any]:
    nonfinite_fields = sorted({
        field for rows in metrics.values() for row in rows.values() for field in row["nonfinite_set"]
    })
    per_node = {}
    for node in NODES:
        values = summary["participants"][node]
        per_node[node] = {
            "initial_test_loss": values.get("initial_test_loss"),
            "final_test_loss": values.get("final_test_loss"),
            "trajectory_mean_test_loss": values.get("trajectory_mean_test_loss"),
            "final_exclusive_block_loss": values.get("final_exclusive_block_loss"),
            "final_distance_to_optimum": values.get("final_distance_to_optimum"),
            "final_disagreement_to_honest_mean": values.get("final_disagreement_to_honest_mean"),
        }
    return {
        "suite": suite,
        "seed": seed,
        "distribution": distribution,
        "arm": arm,
        "status": status,
        "trajectory_status": summary["trajectory_status"],
        "rounds": summary["rounds"],
        "scheduled_rounds": summary["scheduled_rounds"],
        "gradient_evaluations": summary["gradient_evaluations"],
        "divergence": summary.get("divergence"),
        "nonfinite_metric_fields": nonfinite_fields,
        "initial_honest_mean_test_loss": summary.get("initial_honest_mean_test_loss"),
        "final_honest_mean_test_loss": summary.get("final_honest_mean_test_loss"),
        "final_honest_worst_test_loss": summary.get("final_honest_worst_test_loss"),
        "trajectory_mean_honest_test_loss": summary.get("trajectory_mean_honest_test_loss"),
        "final_honest_mean_disagreement": summary.get("final_honest_mean_disagreement"),
        "final_honest_mean_exclusive_block_loss": summary.get("final_honest_mean_exclusive_block_loss"),
        "convergence": {
            "threshold": summary.get("convergence_threshold"),
            "first_round_sustained_below_threshold": summary.get("first_round_sustained_below_threshold"),
            "round_when_sustained_threshold_confirmed": summary.get("round_when_sustained_threshold_confirmed"),
            "censored": summary.get("convergence_censored"),
            "censor_reason": summary.get("censor_reason"),
            "streak_rounds": summary.get("convergence_streak_rounds"),
        },
        "participants": per_node,
        "round_metrics": {
            index: {
                node: {field: metrics[index][node][field] for field in METRIC_FIELDS}
                for node in NODES
            }
            for index in metrics
        },
    }


def _finite_stats(values: list[float | None]) -> dict[str, Any]:
    finite = [float(value) for value in values if value is not None and math.isfinite(float(value))]
    nonfinite_count = len(values) - len(finite)
    return {
        "count": len(values),
        "finite_count": len(finite),
        "nonfinite_count": nonfinite_count,
        "mean": statistics.fmean(finite) if finite else None,
        "median": statistics.median(finite) if finite else None,
        "minimum": min(finite) if finite else None,
        "maximum": max(finite) if finite else None,
        "status": "finite" if finite and not nonfinite_count else "mixed_finite_nonfinite" if finite else "nonfinite_or_unavailable",
    }


def _pair_metric(arm_cases: list[dict[str, Any]], baseline_cases: list[dict[str, Any]], getter) -> dict[str, Any]:
    baseline_by_seed = {case["seed"]: case for case in baseline_cases}
    rows = []
    differences: list[float | None] = []
    for case in sorted(arm_cases, key=lambda item: item["seed"]):
        baseline = baseline_by_seed.get(case["seed"])
        if baseline is None:
            raise ValueError(f"paired baseline is missing seed {case['seed']}")
        baseline_value = getter(baseline)
        arm_value = getter(case)
        base = _number(baseline_value, label="baseline paired metric", nullable=True)
        value = _number(arm_value, label="arm paired metric", nullable=True)
        difference = value - base if value is not None and base is not None else None
        pair_status = "finite" if difference is not None else "nonfinite_or_unavailable"
        rows.append({
            "seed": case["seed"],
            "baseline_value": base,
            "arm_value": value,
            "difference_arm_minus_baseline": difference,
            "status": pair_status,
            "baseline_trajectory_status": baseline["trajectory_status"],
            "arm_trajectory_status": case["trajectory_status"],
        })
        differences.append(difference)
    return {
        "pairs": rows,
        "difference_arm_minus_baseline": _finite_stats(differences),
    }


def _recovery_record(case: dict[str, Any]) -> dict[str, Any] | None:
    if case["arm"] not in ("membership_recovery", "ordering_recovery"):
        return None
    by_round = {
        int(index): rows[NODES[0]]["honest_mean_test_loss"]
        for index, rows in case["round_metrics"].items()
    }
    attack_end = RECOVERY_ATTACK_ROUNDS
    attack_loss = by_round.get(attack_end)
    final_round = ROUNDS
    final_loss = by_round.get(final_round)
    finite = attack_loss is not None and final_loss is not None
    reduction = attack_loss - final_loss if finite else None
    streak = case["convergence"].get("streak_rounds") or CONVERGENCE_STREAK
    threshold = case["convergence"].get("threshold")
    recovery_confirmed = None
    recovery_start_candidate = None
    recovery_start = None
    if threshold is not None:
        run_length = 0
        for round_index in range(attack_end + 1, case["rounds"] + 1):
            loss = by_round.get(round_index)
            if loss is not None and loss < threshold:
                run_length += 1
                if run_length == 1:
                    recovery_start_candidate = round_index
                if run_length >= streak:
                    recovery_start = recovery_start_candidate
                    recovery_confirmed = round_index
                    break
            else:
                run_length = 0
                recovery_start_candidate = None
    return {
        "seed": case["seed"],
        "attack_rounds": attack_end,
        "final_round": final_round,
        "loss_at_attack_end": attack_loss,
        "final_loss": final_loss,
        "loss_reduction_round_100_to_200": reduction,
        "post_attack_convergence_start_round": recovery_start,
        "post_attack_convergence_confirmed_round": recovery_confirmed,
        "status": "finite" if finite else "nonfinite_or_unavailable",
    }


def _suite_summary(suite: dict[str, Any]) -> dict[str, Any]:
    cases = suite["cases"]
    groups: dict[str, Any] = {}
    for distribution in DISTRIBUTIONS:
        arms: dict[str, Any] = {}
        baseline_cases = [cases[(distribution, seed, "baseline")] for seed in suite["manifest"]["seeds"]]
        for arm in ARM_ORDER:
            arm_cases = [cases[(distribution, seed, arm)] for seed in suite["manifest"]["seeds"]]
            by_node = {}
            for node in NODES:
                by_node[node] = _pair_metric(
                    arm_cases,
                    baseline_cases,
                    lambda case, peer=node: case["participants"][peer]["final_test_loss"],
                )
            recovery = [_recovery_record(case) for case in arm_cases]
            recovery = [row for row in recovery if row is not None]
            arms[arm] = {
                "run_count": len(arm_cases),
                "finite_trajectory_count": sum(case["trajectory_status"] == "completed" for case in arm_cases),
                "verified_divergence_count": sum(case["trajectory_status"] == "diverged_nonfinite" for case in arm_cases),
                "runs": [
                    {key: value for key, value in case.items() if key != "round_metrics"}
                    for case in arm_cases
                ],
                "paired_final_honest_mean_test_loss_vs_baseline": _pair_metric(
                    arm_cases, baseline_cases, lambda case: case["final_honest_mean_test_loss"]
                ),
                "paired_final_honest_worst_test_loss_vs_baseline": _pair_metric(
                    arm_cases, baseline_cases, lambda case: case["final_honest_worst_test_loss"]
                ),
                "paired_trajectory_mean_honest_test_loss_vs_baseline": _pair_metric(
                    arm_cases, baseline_cases, lambda case: case["trajectory_mean_honest_test_loss"]
                ),
                "paired_final_honest_exclusive_block_loss_vs_baseline": _pair_metric(
                    arm_cases, baseline_cases, lambda case: case["final_honest_mean_exclusive_block_loss"]
                ),
                "per_node_final_test_loss_vs_baseline": by_node,
                "honest_worst_final_test_loss": _finite_stats([
                    case["final_honest_worst_test_loss"] for case in arm_cases
                ]),
                "convergence": {
                    "by_seed": [{"seed": case["seed"], **case["convergence"]} for case in arm_cases],
                    "converged_count": sum(not case["convergence"]["censored"] for case in arm_cases),
                    "censored_count": sum(bool(case["convergence"]["censored"]) for case in arm_cases),
                    "median_confirmed_round": statistics.median([
                        case["convergence"]["round_when_sustained_threshold_confirmed"]
                        for case in arm_cases
                        if case["convergence"]["round_when_sustained_threshold_confirmed"] is not None
                    ]) if any(case["convergence"]["round_when_sustained_threshold_confirmed"] is not None
                             for case in arm_cases) else None,
                },
                "recovery": {
                    "by_seed": recovery,
                    "loss_reduction_round_100_to_200": _finite_stats([
                        row["loss_reduction_round_100_to_200"] for row in recovery
                    ]),
                    "post_attack_convergence_count": sum(
                        row["post_attack_convergence_confirmed_round"] is not None for row in recovery
                    ),
                } if recovery else None,
            }
        groups[distribution] = arms
    return groups


def _paired_rows(suites: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for suite_name, suite in suites.items():
        for distribution, arms in suite["analysis"].items():
            baseline_cases = {run["seed"]: run for run in arms["baseline"]["runs"]}
            for arm, aggregate in arms.items():
                paired = aggregate["paired_final_honest_mean_test_loss_vs_baseline"]["pairs"]
                for pair in paired:
                    seed = pair["seed"]
                    baseline = baseline_cases[seed]
                    case = next(run for run in aggregate["runs"] if run["seed"] == seed)
                    row = {
                        "suite": suite_name,
                        "distribution": distribution,
                        "seed": seed,
                        "arm": arm,
                        "baseline_trajectory_status": baseline["trajectory_status"],
                        "arm_trajectory_status": case["trajectory_status"],
                        "baseline_final_honest_mean_test_loss": pair["baseline_value"],
                        "arm_final_honest_mean_test_loss": pair["arm_value"],
                        "difference_arm_minus_baseline": pair["difference_arm_minus_baseline"],
                        "pair_metric_status": pair["status"],
                        "baseline_honest_worst_test_loss": baseline["final_honest_worst_test_loss"],
                        "arm_honest_worst_test_loss": case["final_honest_worst_test_loss"],
                    }
                    for node in NODES:
                        base_loss = baseline["participants"][node]["final_test_loss"]
                        arm_loss = case["participants"][node]["final_test_loss"]
                        row[f"baseline_{node}_final_test_loss"] = base_loss
                        row[f"arm_{node}_final_test_loss"] = arm_loss
                        row[f"{node}_difference_arm_minus_baseline"] = (
                            arm_loss - base_loss if arm_loss is not None and base_loss is not None else None
                        )
                    rows.append(row)
    return rows


def _case_csv_rows(suites: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    paired_index = {}
    for row in _paired_rows(suites):
        paired_index[(row["suite"], row["distribution"], row["seed"], row["arm"])] = row
    rows = []
    for suite_name, suite in suites.items():
        for case in suite["cases"].values():
            paired = paired_index[(suite_name, case["distribution"], case["seed"], case["arm"])]
            row = {
                "suite": suite_name,
                "distribution": case["distribution"],
                "seed": case["seed"],
                "arm": case["arm"],
                "run_status": case["status"],
                "trajectory_status": case["trajectory_status"],
                "rounds": case["rounds"],
                "scheduled_rounds": case["scheduled_rounds"],
                "gradient_evaluations": case["gradient_evaluations"],
                "divergence": json.dumps(case["divergence"], sort_keys=True) if case["divergence"] else "",
                "nonfinite_metric_fields": ";".join(case["nonfinite_metric_fields"]),
                "initial_honest_mean_test_loss": case["initial_honest_mean_test_loss"],
                "final_honest_mean_test_loss": case["final_honest_mean_test_loss"],
                "final_honest_worst_test_loss": case["final_honest_worst_test_loss"],
                "trajectory_mean_honest_test_loss": case["trajectory_mean_honest_test_loss"],
                "paired_final_honest_mean_difference_vs_baseline": paired["difference_arm_minus_baseline"],
                "convergence_threshold": case["convergence"]["threshold"],
                "first_round_sustained_below_threshold": case["convergence"]["first_round_sustained_below_threshold"],
                "convergence_confirmed_round": case["convergence"]["round_when_sustained_threshold_confirmed"],
                "convergence_censored": case["convergence"]["censored"],
                "convergence_censor_reason": case["convergence"]["censor_reason"] or "",
                "final_honest_mean_disagreement": case["final_honest_mean_disagreement"],
                "final_honest_mean_exclusive_block_loss": case["final_honest_mean_exclusive_block_loss"],
            }
            recovery = _recovery_record(case)
            if recovery:
                row.update({
                    "recovery_round_100_loss": recovery["loss_at_attack_end"],
                    "recovery_round_200_loss": recovery["final_loss"],
                    "recovery_loss_reduction_round_100_to_200": recovery["loss_reduction_round_100_to_200"],
                    "recovery_post_attack_convergence_confirmed_round": recovery["post_attack_convergence_confirmed_round"],
                })
            else:
                row.update({
                    "recovery_round_100_loss": None,
                    "recovery_round_200_loss": None,
                    "recovery_loss_reduction_round_100_to_200": None,
                    "recovery_post_attack_convergence_confirmed_round": None,
                })
            for node in NODES:
                for metric, value in case["participants"][node].items():
                    row[f"{node}_{metric}"] = value
            rows.append(row)
    return sorted(rows, key=lambda row: (row["suite"], row["distribution"], row["seed"], ARM_ORDER.index(row["arm"])))


def _display(value: Any, *, nonfinite: bool = False) -> str:
    if value is None:
        return "nonfinite" if nonfinite else "n/a"
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, (int, float)):
        if not math.isfinite(float(value)):
            return "nonfinite"
        return f"{value:.6g}"
    return str(value)


def _stats_display(stats: dict[str, Any]) -> str:
    if stats["finite_count"] == 0:
        return f"nonfinite/unavailable (0/{stats['count']} finite)"
    value = _display(stats["mean"])
    if stats["nonfinite_count"]:
        value += f" ({stats['finite_count']}/{stats['count']} finite)"
    return value


def _markdown_report(suites: dict[str, dict[str, Any]]) -> str:
    lines = [
        "# Announcement-learning simulation results",
        "",
        "This report includes only completed runs whose suite inventory and independent trace verification passed. "
        "It summarizes fixed-round simulation outcomes and makes no timing, deployment, or population-generalization claims. "
        "A negative paired difference means the arm's loss is below that seed's baseline loss. Null metrics are shown as nonfinite or unavailable; divergence remains visible even when some final metrics are finite.",
        "",
        "The model is local to each participant; regrouping does not synchronize model parameters. Recovery measurements describe the scheduled honest rounds after the 100-round attack phase.",
        "",
        "## Verified suite inputs",
        "",
        "| Suite | Seeds | Runs | Verified divergences | Manifest SHA256 | Inventory SHA256 |",
        "|---|---:|---:|---:|---|---|",
    ]
    for name, suite in suites.items():
        lines.append(
            f"| {name} | {', '.join(map(str, suite['manifest']['seeds']))} | {suite['manifest']['run_count']} "
            f"| {suite['verified_divergence_count']} | `{suite['manifest_sha256']}` | `{suite['inventory_sha256']}` |"
        )
    for name, suite in suites.items():
        lines.extend(["", f"## {name.title()}", ""])
        for distribution in DISTRIBUTIONS:
            lines.extend([
                f"### {distribution}",
                "",
                "| Arm | Finite / runs | Final honest mean loss | Paired Δ vs baseline | Honest worst loss | Converged / censored | B / D / A / C final loss | Recovery Δ loss (100→200) |",
                "|---|---:|---:|---:|---:|---:|---|---:|",
            ])
            for arm in ARM_ORDER:
                record = suite["analysis"][distribution][arm]
                cases = record["runs"]
                mean_loss = _finite_stats([case["final_honest_mean_test_loss"] for case in cases])
                paired = record["paired_final_honest_mean_test_loss_vs_baseline"]["difference_arm_minus_baseline"]
                worst = record["honest_worst_final_test_loss"]
                convergence = record["convergence"]
                node_losses = []
                for node in NODES:
                    node_values = [case["participants"][node]["final_test_loss"] for case in cases]
                    node_stats = _finite_stats(node_values)
                    node_losses.append(f"{node}={_stats_display(node_stats)}")
                recovery = record["recovery"]["loss_reduction_round_100_to_200"] if record["recovery"] else None
                recovery_display = _stats_display(recovery) if recovery else "n/a"
                lines.append(
                    f"| `{arm}` | {record['finite_trajectory_count']} / {record['run_count']} "
                    f"(diverged {record['verified_divergence_count']}) | {_stats_display(mean_loss)} "
                    f"| {_stats_display(paired)} | {_stats_display(worst)} "
                    f"| {convergence['converged_count']} / {convergence['censored_count']} "
                    f"| {'; '.join(node_losses)} | {recovery_display} |"
                )
            lines.append("")
    lines.extend([
        "## Reading the metrics",
        "",
        "The final honest mean is computed within each run across honest nodes A, D, and C; the table then reports the arithmetic mean of those run-level means across the listed seeds. Per-node final losses are separately averaged across seeds. Paired differences use the same seed and data distribution, with `arm − baseline` as the sign convention. Honest worst loss is the final maximum across A, D, and C. Convergence uses the runner's fixed threshold (10% of initial honest mean loss) and ten consecutive rounds; censored runs did not meet it within the recorded horizon or diverged first. Recovery loss change is measured from round 100 to round 200; a positive reduction means loss fell after the attack stopped. If a verified-divergence run did not reach either round, the recovery value is unavailable.",
        "",
        "Per-node final and trajectory metrics, seed-paired rows, nonfinite markers, and recovery details are available in `report.json`, `case_metrics.csv`, and `paired_losses.csv`.",
        "",
    ])
    return "\n".join(lines)


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0])
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fieldnames, extrasaction="raise")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _csv_value(value) for key, value in row.items()})


def _csv_value(value: Any) -> Any:
    if value is None:
        return "NONFINITE_OR_UNAVAILABLE"
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return value


def generate_report(pilot_path: str | Path, confirmation_path: str | Path,
                    output_path: str | Path) -> dict[str, Any]:
    pilot_root = Path(pilot_path).resolve()
    confirmation_root = Path(confirmation_path).resolve()
    output_root = Path(output_path).resolve()
    if pilot_root == confirmation_root or _inside(pilot_root, confirmation_root) or _inside(confirmation_root, pilot_root):
        raise ValueError("pilot and confirmation suites must be distinct, non-nested directories")
    if output_root.exists():
        raise FileExistsError(f"report output directory already exists: {output_root}")
    if any(_inside(output_root, root) or _inside(root, output_root) for root in (pilot_root, confirmation_root)):
        raise ValueError("report output directory must be separate from both inventoried suites")

    pilot = _verify_suite(pilot_root, "pilot")
    confirmation = _verify_suite(confirmation_root, "confirmation")
    confirmation_link = confirmation["manifest"].get("reviewed_pilot")
    if (
        not isinstance(confirmation_link, dict)
        or confirmation_link.get("manifest_sha256") != pilot["manifest_sha256"]
        or Path(str(confirmation_link.get("path", ""))).resolve() != pilot_root
    ):
        raise ValueError("confirmation suite does not cite the supplied reviewed pilot manifest")
    for field in ("source_hashes", "runner_sha256", "validation_sha256", "workload_contract_sha256"):
        if pilot.get(field) != confirmation.get(field):
            raise ValueError(f"pilot and confirmation suites differ in {field}")
    for field in ("arms", "distributions", "rounds", "chunk_elements"):
        if pilot["manifest"].get(field) != confirmation["manifest"].get(field):
            raise ValueError(f"pilot and confirmation manifests differ in {field}")

    suites = {"pilot": pilot, "confirmation": confirmation}
    for suite in suites.values():
        suite["analysis"] = _suite_summary(suite)
    report = {
        "schema": REPORT_SCHEMA,
        "scope": {
            "claim_type": "fixed-round numerical simulation results",
            "timing_claims": "none",
            "deployment_claims": "none",
            "population_generalization": "none",
            "parameter_synchronization": False,
            "paired_difference_sign": "arm minus same-seed baseline; negative means lower arm loss",
            "recovery_attack_rounds": RECOVERY_ATTACK_ROUNDS,
            "convergence_fraction_of_initial_honest_mean_loss": 0.1,
            "convergence_streak_rounds": CONVERGENCE_STREAK,
        },
        "suites": {
            name: {
                "path": suite["path"],
                "mode": suite["mode"],
                "manifest_sha256": suite["manifest_sha256"],
                "inventory_sha256": suite["inventory_sha256"],
                "seeds": suite["manifest"]["seeds"],
                "run_count": suite["manifest"]["run_count"],
                "verified_divergence_count": suite["verified_divergence_count"],
                "distributions": suite["analysis"],
            }
            for name, suite in suites.items()
        },
    }
    case_rows = _case_csv_rows(suites)
    paired_rows = _paired_rows(suites)
    markdown = _markdown_report(suites)

    output_root.mkdir(parents=True, exist_ok=False)
    (output_root / "REPORT.md").write_text(markdown, encoding="utf-8")
    (output_root / "report.json").write_bytes(_json_bytes(report))
    _write_csv(output_root / "case_metrics.csv", case_rows)
    _write_csv(output_root / "paired_losses.csv", paired_rows)
    return {
        "output": str(output_root),
        "report": str(output_root / "REPORT.md"),
        "json": str(output_root / "report.json"),
        "case_metrics_csv": str(output_root / "case_metrics.csv"),
        "paired_losses_csv": str(output_root / "paired_losses.csv"),
        "suites": {name: {"runs": suite["manifest"]["run_count"],
                          "verified_divergences": suite["verified_divergence_count"]}
                   for name, suite in suites.items()},
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pilot", type=Path, required=True, help="completed pilot suite directory")
    parser.add_argument("--confirmation", type=Path, required=True, help="completed confirmation suite directory")
    parser.add_argument("--output", type=Path, required=True, help="new report directory outside both suites")
    args = parser.parse_args(argv)
    try:
        result = generate_report(args.pilot, args.confirmation, args.output)
    except (OSError, ValueError, KeyError, TypeError, FileExistsError) as exc:
        parser.error(str(exc))
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
