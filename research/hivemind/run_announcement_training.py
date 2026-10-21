#!/usr/bin/env python3
"""Run one native Hivemind gradient-SGD announcement trajectory.

Each round starts a fresh private four-node loopback averaging group. Participant
models remain in this controller process, so the only state that crosses a
round boundary is the recorded model vector. This isolates the native
averaging behavior while preserving the learning state affected by it.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from typing import Any

from announcement_learning import (
    DIMENSIONS,
    LEARNING_RATE,
    NODES,
    TRAINING_EXAMPLES_PER_NODE,
    make_problem,
)
from announcement_probe import audit_shutdown


ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
DEFAULT_OUTPUT_ROOT = HERE / "results"
DEFAULT_PACKAGE_ROOT = ROOT / "research/simulator/.native-runtime/root_snapshot_a"
DEFAULT_DEPENDENCIES = ROOT / "research/agora/.runtime/venv/lib/python3.11/site-packages"
CONDITIONS = ("baseline4", "membership", "ordering")
TIMEOUT_SECONDS = 120
SHUTDOWN_GRACE_SECONDS = 5
SOURCE_FILES = (
    "announcement_probe.py",
    "analyze_announcement_probe.py",
    "announcement_learning.py",
    "announcement_reference.py",
    "run_announcement_training.py",
)


def _json_bytes(value: Any) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n").encode("utf-8")


def _write_json(path: Path, value: Any) -> None:
    """Write a JSON artifact atomically so interrupted runs remain readable."""
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


def _freeze_sources(destination: Path) -> dict[str, str]:
    frozen = destination / "harness"
    frozen.mkdir()
    hashes: dict[str, str] = {}
    for filename in SOURCE_FILES:
        source = HERE / filename
        contents = source.read_bytes()
        (frozen / filename).write_bytes(contents)
        hashes[filename] = hashlib.sha256(contents).hexdigest()
    partition_source = ROOT / "research/simulator/src/trainsim/protocols/partitioning.py"
    partition_bytes = partition_source.read_bytes()
    (frozen / "partitioning.py").write_bytes(partition_bytes)
    hashes["partitioning.py"] = hashlib.sha256(partition_bytes).hexdigest()
    _write_json(destination / "harness_hashes.json", {
        "announcement_probe.py": hashes["announcement_probe.py"],
    })
    _write_json(destination / "frozen_sources.json", {
        "schema": "announcement-training-sources-v1",
        "files": hashes,
    })
    return hashes


def _verify_native_reference(case_path: Path) -> dict[str, Any]:
    """Run the independent E0 tensor validator; missing validation fails closed."""
    validator_path = HERE / "announcement_reference.py"
    if not validator_path.is_file():
        raise RuntimeError(f"required native tensor validator is missing: {validator_path}")
    import importlib

    module = importlib.import_module("announcement_reference")
    verify = getattr(module, "verify_native_round", None)
    if verify is None:
        raise RuntimeError("announcement_reference.py does not expose verify_native_round(case_path)")
    result = verify(case_path)
    if not isinstance(result, dict) or "valid" not in result:
        raise RuntimeError("verify_native_round must return a dictionary with a valid field")
    checked = {"available": True, **result}
    _write_json(case_path / "reference_validation.json", checked)
    return checked


def _worker_command(
    condition: str,
    seed: int,
    case_path: Path,
    inputs_path: Path,
    package_root: Path,
    dependencies: Path,
) -> list[str]:
    if condition not in CONDITIONS:
        raise ValueError(f"condition must be one of {CONDITIONS}")
    return [
        sys.executable,
        str((HERE / "announcement_probe.py").resolve()),
        "--condition", condition,
        "--case-path", str(case_path.resolve()),
        "--inputs-json", str(inputs_path.resolve()),
        "--chunk-bytes", "16",
        "--seed", str(seed),
        "--package-root", str(package_root.resolve()),
        "--dependencies", str(dependencies.resolve()),
    ]


def _stop_process_group(process: subprocess.Popen[Any]) -> None:
    """Stop and reap a timed-out worker's complete private process group."""
    process_group = process.pid

    def group_exists() -> bool:
        try:
            os.killpg(process_group, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            return True

    try:
        os.killpg(process_group, signal.SIGTERM)
    except ProcessLookupError:
        pass
    deadline = time.monotonic() + SHUTDOWN_GRACE_SECONDS
    while group_exists() and time.monotonic() < deadline:
        try:
            process.wait(timeout=min(0.05, max(0.001, deadline - time.monotonic())))
        except subprocess.TimeoutExpired:
            pass
        time.sleep(0.01)
    if group_exists():
        try:
            os.killpg(process_group, signal.SIGKILL)
        except ProcessLookupError:
            pass
    try:
        process.wait(timeout=SHUTDOWN_GRACE_SECONDS)
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"native worker process group {process_group} survived SIGKILL")
    deadline = time.monotonic() + SHUTDOWN_GRACE_SECONDS
    while group_exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    if group_exists():
        raise RuntimeError(f"native worker process group {process_group} survived SIGKILL")


def _run_native_worker(
    *,
    condition: str,
    seed: int,
    case_path: Path,
    inputs_path: Path,
    inputs: dict[str, list[float]],
    log_path: Path,
    package_root: Path,
    dependencies: Path,
    timeout: int = TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Run one native round, audit worker-owned helpers, and read its outputs."""
    if case_path.exists():
        raise FileExistsError(f"native round directory already exists: {case_path}")
    if set(inputs) != set(NODES):
        raise ValueError(f"inputs must contain exactly {NODES}")
    for node, values in inputs.items():
        if len(values) != DIMENSIONS or any(not math.isfinite(float(value)) for value in values):
            raise ValueError(f"{node} input must contain {DIMENSIONS} finite values")
    _write_json(inputs_path, inputs)
    command = _worker_command(condition, seed, case_path, inputs_path, package_root, dependencies)
    process: subprocess.Popen[Any] | None = None
    timed_out = False
    exit_code: int | None = None
    with log_path.open("w", encoding="utf-8") as log:
        process = subprocess.Popen(
            command,
            stdout=log,
            stderr=subprocess.STDOUT,
            cwd="/tmp",
            start_new_session=True,
        )
        try:
            exit_code = process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            _stop_process_group(process)
            exit_code = process.returncode
        except BaseException:
            _stop_process_group(process)
            shutdown_path = case_path / "shutdown.json"
            if shutdown_path.exists():
                audit_shutdown(case_path, process.returncode, dependencies)
            raise

    shutdown_path = case_path / "shutdown.json"
    shutdown: dict[str, Any] | None = None
    if shutdown_path.exists():
        shutdown = audit_shutdown(case_path, exit_code, dependencies)
    if timed_out:
        if shutdown is None:
            case_path.mkdir(parents=True, exist_ok=True)
            _write_json(case_path / "controller_timeout.json", {
                "timeout_seconds": timeout,
                "worker_exitcode": exit_code,
                "process_group_cleanup": "SIGTERM then SIGKILL if needed",
                "shutdown_evidence_available": False,
            })
        raise TimeoutError(f"native worker exceeded {timeout}s; see {log_path}")
    if shutdown is None:
        raise RuntimeError(f"native worker omitted shutdown.json; see {log_path}")
    if exit_code != 0:
        raise RuntimeError(f"native worker exited {exit_code}; see {log_path}")
    if not shutdown.get("safe"):
        raise RuntimeError(f"native worker left surviving processes: {shutdown.get('surviving_pids')}")
    outputs_path = case_path / "outputs.json"
    if not outputs_path.exists():
        raise RuntimeError(f"native worker omitted outputs.json; see {log_path}")
    outputs = json.loads(outputs_path.read_text(encoding="utf-8"))
    if set(outputs) != set(NODES):
        raise RuntimeError(f"native worker returned nodes {sorted(outputs)}, expected {NODES}")
    return outputs


def _native_fallback_evidence(case_path: Path, node: str, output: dict[str, Any]) -> dict[str, Any]:
    events_path = case_path / f"events_{node}.jsonl"
    missing_parts = []
    if events_path.exists():
        for line in events_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            event = json.loads(line)
            if event.get("kind") == "missing_part_fallback":
                missing_parts.append(event)
    failed_size = int(output.get("failed_size", 0) or 0)
    failed_peers = list(output.get("failed_peer_ids", []) or [])
    return {
        "observed": failed_size > 0 or bool(failed_peers) or bool(missing_parts),
        "failed_size": failed_size,
        "failed_peer_ids": failed_peers,
        "missing_part_events": missing_parts,
    }


def _vector(values: Any, *, label: str):
    import numpy as np

    result = np.asarray(values, dtype=np.float32)
    if result.shape != (DIMENSIONS,):
        raise ValueError(f"{label} must have shape ({DIMENSIONS},), got {result.shape}")
    if not np.isfinite(result).all():
        raise ValueError(f"{label} contains non-finite values")
    return result


def _build_round_record(
    *,
    round_index: int,
    condition: str,
    case_path: Path,
    weights_before: dict[str, Any],
    input_gradients: dict[str, Any],
    outputs: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Capture one operation and apply each successful returned gradient."""
    import numpy as np

    if set(outputs) != set(NODES):
        raise ValueError(f"outputs must contain exactly {NODES}")
    record: dict[str, Any] = {
        "round": round_index,
        "condition": condition,
        "native_case": case_path.name,
        "weights_before": {},
        "input_gradients": {},
        "returned_gradients": {},
        "statuses": {},
        "fallback": {},
        "updates_applied": {},
        "weights_after": {},
    }
    weights_after: dict[str, Any] = {}
    for node in NODES:
        before = _vector(weights_before[node], label=f"{node} weights_before")
        requested = _vector(input_gradients[node], label=f"{node} input_gradient")
        output = outputs[node]
        native_initial = _vector(output.get("initial"), label=f"{node} native initial")
        returned = _vector(output.get("final"), label=f"{node} returned gradient")
        if not np.array_equal(native_initial, requested):
            raise ValueError(f"{node} native initial tensor differs from requested gradient")
        status = str(output.get("status", "missing"))
        applied = status == "success"
        after = np.asarray(before - np.float32(LEARNING_RATE) * returned, dtype=np.float32) if applied else before.copy()
        fallback = _native_fallback_evidence(case_path, node, output)
        record["weights_before"][node] = before.tolist()
        record["input_gradients"][node] = requested.tolist()
        record["returned_gradients"][node] = returned.tolist()
        record["statuses"][node] = {
            "status": status,
            "error": output.get("error"),
            "failed_size": int(output.get("failed_size", 0) or 0),
            "failed_peer_ids": list(output.get("failed_peer_ids", []) or []),
        }
        record["fallback"][node] = fallback
        record["updates_applied"][node] = applied
        record["weights_after"][node] = after.tolist()
        weights_after[node] = after
    return record, weights_after


def _metrics(weights: dict[str, Any], problem) -> dict[str, Any]:
    import numpy as np

    by_node: dict[str, Any] = {}
    block_by_node: dict[str, float] = {}
    distances: dict[str, float] = {}
    target = np.asarray(problem.target, dtype=np.float32)
    eval_x = np.asarray(problem.eval_x, dtype=np.float32)
    eval_y = np.asarray(problem.eval_y, dtype=np.float32)
    for node in NODES:
        vector = _vector(weights[node], label=f"{node} evaluation weights")
        residual = np.asarray(eval_x @ vector - eval_y, dtype=np.float32)
        by_node[node] = float(np.mean(np.square(residual, dtype=np.float32), dtype=np.float32) / 2.0)
        exclusive_residual = np.asarray(
            eval_x[:, 36:48] @ (vector[36:48] - target[36:48]), dtype=np.float32
        )
        block_by_node[node] = float(
            np.mean(np.square(exclusive_residual, dtype=np.float32), dtype=np.float32) / 2.0
        )
        distances[node] = float(np.linalg.norm((vector - target).astype(np.float64)))
    pairwise = []
    for i, left in enumerate(NODES):
        for right in NODES[i + 1:]:
            pairwise.append(float(np.linalg.norm(
                (_vector(weights[left], label=f"{left} weights") -
                 _vector(weights[right], label=f"{right} weights")).astype(np.float64)
            )))
    ordered = sorted(by_node.values())
    return {
        "test_half_mse_by_node": by_node,
        "test_half_mse_mean": float(sum(ordered) / len(ordered)),
        "test_half_mse_worst": float(max(ordered)),
        "victim_exclusive_half_mse_by_node": block_by_node,
        "distance_to_target_by_node": distances,
        "model_disagreement_mean_l2": float(sum(pairwise) / len(pairwise)),
        "model_disagreement_max_l2": float(max(pairwise)),
    }


def verify_trajectory(data_file: Path, trajectory: dict[str, Any]) -> dict[str, Any]:
    """Recompute saved local gradients and SGD updates from persisted X/y arrays."""
    import numpy as np

    with np.load(data_file, allow_pickle=False) as archive:
        arrays = {key: archive[key] for key in archive.files}
    if set(trajectory.get("initial_weights", {})) != set(NODES):
        raise ValueError("trajectory initial_weights must contain all participants")
    previous = {
        node: _vector(trajectory["initial_weights"][node], label=f"{node} initial weight")
        for node in NODES
    }
    checked_gradients = 0
    checked_updates = 0
    fallback_updates = 0
    round_rows = trajectory.get("rounds", [])
    for expected_round, row in enumerate(round_rows, start=1):
        if int(row.get("round", -1)) != expected_round:
            raise ValueError(f"expected round {expected_round}, got {row.get('round')}")
        case_name = row.get("native_case")
        if not isinstance(case_name, str) or Path(case_name).name != case_name:
            raise ValueError(f"round {expected_round} has an invalid native_case path")
        case_path = data_file.parent / case_name
        reference = row.get("native_reference_validation", {})
        reference_file = case_path / "reference_validation.json"
        if not reference.get("available") or not reference.get("valid") or not reference_file.is_file():
            raise ValueError(f"round {expected_round} lacks valid independent native evidence")
        saved_reference = json.loads(reference_file.read_text(encoding="utf-8"))
        if not saved_reference.get("valid"):
            raise ValueError(f"round {expected_round} native reference evidence is invalid")
        outputs_path = case_path / "outputs.json"
        if not outputs_path.is_file():
            raise ValueError(f"round {expected_round} lacks native outputs.json")
        native_outputs = json.loads(outputs_path.read_text(encoding="utf-8"))
        if set(native_outputs) != set(NODES):
            raise ValueError(f"round {expected_round} native outputs do not contain all participants")
        for node in NODES:
            before = _vector(row["weights_before"][node], label=f"round {expected_round} {node} before")
            if not np.array_equal(before, previous[node]):
                raise ValueError(f"round {expected_round} {node} pre-update model does not chain")
            train_x = arrays[f"train_x_{node}"]
            train_y = arrays[f"train_y_{node}"]
            direct_gradient = np.asarray(
                train_x.T @ (train_x @ before - train_y) /
                np.float32(TRAINING_EXAMPLES_PER_NODE),
                dtype=np.float32,
            )
            observed_gradient = _vector(
                row["input_gradients"][node], label=f"round {expected_round} {node} input gradient"
            )
            if not np.allclose(direct_gradient, observed_gradient, rtol=1e-6, atol=1e-7):
                difference = float(np.max(np.abs(direct_gradient - observed_gradient)))
                raise ValueError(f"round {expected_round} {node} gradient mismatch, max abs {difference}")
            native_output = native_outputs[node]
            native_input = _vector(native_output.get("initial"), label=f"round {expected_round} {node} native input")
            if not np.array_equal(native_input, observed_gradient):
                raise ValueError(f"round {expected_round} {node} saved input differs from native output")
            checked_gradients += 1
            returned = _vector(
                row["returned_gradients"][node], label=f"round {expected_round} {node} returned gradient"
            )
            native_returned = _vector(
                native_output.get("final"), label=f"round {expected_round} {node} native output"
            )
            if not np.array_equal(native_returned, returned):
                raise ValueError(f"round {expected_round} {node} saved output differs from native output")
            status = row["statuses"][node]["status"]
            if status != native_output.get("status"):
                raise ValueError(f"round {expected_round} {node} terminal status differs from native output")
            status_details = row["statuses"][node]
            if (status_details.get("error") != native_output.get("error")
                    or int(status_details.get("failed_size", 0) or 0) != int(native_output.get("failed_size", 0) or 0)
                    or list(status_details.get("failed_peer_ids", []) or []) !=
                    list(native_output.get("failed_peer_ids", []) or [])):
                raise ValueError(f"round {expected_round} {node} saved terminal details differ from native output")
            if row["fallback"][node] != _native_fallback_evidence(case_path, node, native_output):
                raise ValueError(f"round {expected_round} {node} saved fallback evidence differs from native artifacts")
            applied = bool(row["updates_applied"][node])
            expected_applied = status == "success"
            if applied != expected_applied:
                raise ValueError(f"round {expected_round} {node} update flag contradicts native status")
            if applied:
                expected_after = np.asarray(before - np.float32(LEARNING_RATE) * returned, dtype=np.float32)
                checked_updates += 1
                if row["fallback"][node]["observed"]:
                    fallback_updates += 1
                    raw_step = np.float32(LEARNING_RATE) * float(np.max(np.abs(returned)))
                    precision_floor = np.finfo(np.float32).eps * max(
                        1.0, float(np.max(np.abs(before)))
                    )
                    if raw_step > precision_floor and np.array_equal(expected_after, before):
                        raise ValueError(f"round {expected_round} {node} model froze despite a nonzero fallback gradient")
            else:
                expected_after = before.copy()
            observed_after = _vector(
                row["weights_after"][node], label=f"round {expected_round} {node} after"
            )
            if not np.array_equal(expected_after, observed_after):
                raise ValueError(f"round {expected_round} {node} post-update model mismatch")
            previous[node] = observed_after
        if set(row.get("statuses", {})) != set(NODES):
            raise ValueError(f"round {expected_round} is missing terminal status")
    return {
        "valid": True,
        "rounds": len(round_rows),
        "participant_gradients_reconstructed": checked_gradients,
        "successful_sgd_updates_reconstructed": checked_updates,
        "fallback_updates_reconstructed": fallback_updates,
        "method": "recompute float32 full-batch X.T @ (X @ w - y) / 512 and local SGD from learning_data.npz",
    }


def _trajectory_payload(
    *,
    label: str,
    seed: int,
    rounds: int,
    distribution: str,
    condition: str,
    initial_weights: dict[str, Any],
    initial_metrics: dict[str, Any],
    round_records: list[dict[str, Any]],
    latest_metrics: dict[str, Any],
    status: str,
) -> dict[str, Any]:
    return {
        "schema": "announcement-native-training-trajectory-v1",
        "label": label,
        "seed": seed,
        "round_limit": rounds,
        "rounds_completed": len(round_records),
        "distribution": distribution,
        "condition": condition,
        "condition_scope": "one averaging operation per round; fresh native group, persistent local model state",
        "initial_weights": initial_weights,
        "initial_metrics": initial_metrics,
        "rounds": round_records,
        "latest_metrics": latest_metrics,
        "status": status,
    }


def run_trajectory(args: argparse.Namespace) -> Path:
    """Execute and independently verify one native training trajectory."""
    label = _safe_label(args.label)
    if args.seed < 0:
        raise ValueError("seed must be non-negative")
    if args.rounds < 1:
        raise ValueError("rounds must be positive")
    if args.condition not in CONDITIONS:
        raise ValueError(f"condition must be one of {CONDITIONS}")
    output_root = args.output_root.resolve()
    trajectory_path = output_root / label
    if trajectory_path.exists():
        raise FileExistsError(f"trajectory output already exists: {trajectory_path}")
    problem = make_problem(args.seed, args.distribution)
    trajectory_path.mkdir(parents=True, exist_ok=False)
    data_manifest = problem.save(trajectory_path)
    source_hashes = _freeze_sources(trajectory_path)
    initial = {node: problem.initial_weights.copy() for node in NODES}
    current = {node: value.copy() for node, value in initial.items()}
    initial_metrics = _metrics(current, problem)
    records: list[dict[str, Any]] = []
    latest_metrics = initial_metrics
    status = {
        "status": "running",
        "label": label,
        "seed": args.seed,
        "distribution": args.distribution,
        "condition": args.condition,
        "round_limit": args.rounds,
        "completed_rounds": 0,
    }
    _write_json(trajectory_path / "status.json", status)
    _write_json(trajectory_path / "config.json", {
        "schema": "announcement-native-training-run-v1",
        "label": label,
        "seed": args.seed,
        "distribution": args.distribution,
        "condition": args.condition,
        "rounds": args.rounds,
        "learning_problem": problem.config(),
        "learning_data_manifest": data_manifest,
        "frozen_source_hashes": source_hashes,
        "native": {
            "worker": "announcement_probe.py",
            "chunk_bytes": 16,
            "group": "four nodes, equal allocations, designated leader B",
            "network": "private loopback, new group and peer identities every round",
            "timeout_seconds": TIMEOUT_SECONDS,
            "package_root": str(args.package_root.resolve()),
            "dependencies": str(args.dependencies.resolve()),
        },
        "model_state": "controller preserves participant vectors between fresh native averaging groups",
    })
    initial_json = {node: value.tolist() for node, value in initial.items()}
    _write_json(trajectory_path / "trajectory.json", _trajectory_payload(
        label=label, seed=args.seed, rounds=args.rounds, distribution=args.distribution,
        condition=args.condition, initial_weights=initial_json, initial_metrics=initial_metrics,
        round_records=records, latest_metrics=latest_metrics, status="running",
    ))
    try:
        for round_index in range(1, args.rounds + 1):
            weights_before = {node: current[node].copy() for node in NODES}
            gradients = {node: problem.gradient(node, weights_before[node]) for node in NODES}
            requested = {node: gradients[node].tolist() for node in NODES}
            case_name = f"round_{round_index:03d}"
            case_path = trajectory_path / case_name
            inputs_path = trajectory_path / f"{case_name}_requested_inputs.json"
            outputs = _run_native_worker(
                condition=args.condition,
                seed=args.seed,
                case_path=case_path,
                inputs_path=inputs_path,
                inputs=requested,
                log_path=trajectory_path / f"round_{round_index:03d}.log",
                package_root=args.package_root,
                dependencies=args.dependencies,
            )
            record, current = _build_round_record(
                round_index=round_index,
                condition=args.condition,
                case_path=case_path,
                weights_before=weights_before,
                input_gradients=gradients,
                outputs=outputs,
            )
            reference_validation = _verify_native_reference(case_path)
            record["native_reference_validation"] = reference_validation
            latest_metrics = _metrics(current, problem)
            record["metrics_after"] = latest_metrics
            records.append(record)
            partial = _trajectory_payload(
                label=label, seed=args.seed, rounds=args.rounds, distribution=args.distribution,
                condition=args.condition, initial_weights=initial_json, initial_metrics=initial_metrics,
                round_records=records, latest_metrics=latest_metrics, status="running",
            )
            _write_json(case_path / "training.json", record)
            _write_json(trajectory_path / "trajectory.json", partial)
            status["completed_rounds"] = round_index
            _write_json(trajectory_path / "status.json", status)
            if reference_validation.get("available") and not reference_validation.get("valid"):
                raise RuntimeError(
                    f"independent native tensor validation failed in round {round_index}: "
                    f"{reference_validation.get('errors', reference_validation)}"
                )

        trajectory = _trajectory_payload(
            label=label, seed=args.seed, rounds=args.rounds, distribution=args.distribution,
            condition=args.condition, initial_weights=initial_json, initial_metrics=initial_metrics,
            round_records=records, latest_metrics=latest_metrics, status="completed",
        )
        verification = verify_trajectory(trajectory_path / "learning_data.npz", trajectory)
        _write_json(trajectory_path / "verification.json", verification)
        trajectory["status"] = "completed"
        _write_json(trajectory_path / "trajectory.json", trajectory)
        _write_json(trajectory_path / "summary.json", {
            "schema": "announcement-native-training-summary-v1",
            "label": label,
            "seed": args.seed,
            "distribution": args.distribution,
            "condition": args.condition,
            "rounds_completed": len(records),
            "initial_metrics": initial_metrics,
            "final_metrics": latest_metrics,
            "verification": verification,
        })
        status.update(status="completed", completed_rounds=len(records))
        _write_json(trajectory_path / "status.json", status)
        return trajectory_path
    except BaseException as exc:
        status.update(status="failed", completed_rounds=len(records), error=repr(exc))
        _write_json(trajectory_path / "status.json", status)
        partial = _trajectory_payload(
            label=label, seed=args.seed, rounds=args.rounds, distribution=args.distribution,
            condition=args.condition, initial_weights=initial_json, initial_metrics=initial_metrics,
            round_records=records, latest_metrics=latest_metrics, status="failed",
        )
        _write_json(trajectory_path / "trajectory.json", partial)
        raise


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--label", required=True, help="unique output directory name")
    parser.add_argument("--seed", type=int, default=201, help="training-data and native-readiness seed")
    parser.add_argument("--rounds", type=int, default=10)
    parser.add_argument("--distribution", choices=("iid", "distinctive"), default="iid")
    parser.add_argument("--condition", choices=CONDITIONS, required=True,
                        help="one trajectory only; invoke separately for each condition/distribution")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--package-root", type=Path, default=DEFAULT_PACKAGE_ROOT)
    parser.add_argument("--dependencies", type=Path, default=DEFAULT_DEPENDENCIES)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        path = run_trajectory(args)
    except BaseException as exc:
        print(f"ERROR: {exc}", file=sys.stderr, flush=True)
        return 1
    print(path, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
