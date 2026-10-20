"""Serial local calibration/held-out suite with explicit native dependencies."""
import json
import os
from pathlib import Path
import subprocess
import sys
import signal

from .config import load
from .native_validation import audit_native_exit, compare, validate_suite
from .runner import ROOT, REPOSITORY, run, write_json


def native_suite(label: str, package_root: Path, dependencies: Path,
                 results_root: Path | None = None, duration: float = 120.) -> Path:
    """Run small private native comparisons; stop at a failed calibration gate.

    Native processes use sockets/shared memory. Run this command in an execution
    environment permitting loopback; the simulator itself needs neither.
    """
    from .runner import _safe_label
    suite = (results_root or ROOT/"results")/_safe_label(label)
    suite.mkdir(parents=True, exist_ok=False)
    config = load(ROOT/"configs/native_validation.json")
    pairs, calibration_pairs, completed = [], [], []
    status = dict(status="running", completed=completed, scope="grouping", duration=duration)
    write_json(suite/"status.json", status)
    fixture_command = [sys.executable, str(REPOSITORY/"research/hivemind/simulator_native_validation.py"),
                       "--package-root", str(package_root.resolve()), "--config", str(ROOT/"configs/native_validation.json"),
                       "--seed", "0", "--label", "fixtures", "--output-root", str(suite.resolve()), "--fixtures-only"]
    with (suite/"fixtures.log").open("w") as log:
        subprocess.run(fixture_command, env=dict(os.environ, PYTHONPATH=str(dependencies.resolve())),
                       cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, timeout=60, check=True)
    if not json.loads((suite/"fixtures/fixtures.json").read_text())["passed"]:
        raise RuntimeError("Native decision fixtures failed")
    for phase, seeds in (("calibration", range(1, 6)), ("held_out", range(101, 106))):
        for condition, latency in (("low", 0.), ("injected", .025)):
            c = dict(config, latency_seconds=latency, registry_latency_seconds=latency,
                     horizon_seconds=duration, observation_seconds=duration)
            config_path = suite/f"{condition}.json"
            write_json(config_path, c)
            for seed in seeds:
                case = f"{phase}_{condition}_s{seed}"
                sim = run(c, seed, case+"_sim", suite)
                native = suite/(case+"_native")
                command = [sys.executable, str(REPOSITORY/"research/hivemind/simulator_native_validation.py"),
                           "--package-root", str(package_root.resolve()), "--config", str(config_path.resolve()),
                           "--seed", str(seed), "--label", native.name, "--output-root", str(suite.resolve())]
                env = dict(os.environ, PYTHONPATH=str(dependencies.resolve()))
                with (suite/(case+".log")).open("w") as log:
                    process = subprocess.Popen(command, env=env, cwd=ROOT, start_new_session=True,
                                               stdout=log, stderr=subprocess.STDOUT)
                    try:
                        code = process.wait(timeout=duration+240)
                    except (subprocess.TimeoutExpired, KeyboardInterrupt):
                        # This session was created exclusively for this trial.
                        os.killpg(process.pid, signal.SIGTERM)
                        try:
                            process.wait(timeout=10)
                        except subprocess.TimeoutExpired:
                            os.killpg(process.pid, signal.SIGKILL)
                            process.wait(timeout=10)
                        status.update(status="interrupted", failed_case=case)
                        write_json(suite/"status.json", status)
                        raise
                if code:
                    status.update(status="native_error", failed_case=case)
                    write_json(suite/"status.json", status)
                    raise RuntimeError(f"Native trial failed: {case}; see its log")
                audit_native_exit(native)
                comparison = compare(sim, native)
                write_json(suite/(case+"_comparison.json"), comparison)
                completed.append(dict(case=case, passed=comparison["passed"]))
                if phase == "calibration":
                    calibration_pairs.append(dict(simulation=str(sim.resolve()), native=str(native.resolve())))
                    write_json(suite/"calibration_pairs.json", calibration_pairs)
                write_json(suite/"status.json", status)
                if phase == "calibration" and not comparison["passed"]:
                    status.update(status="calibration_failed", failed_case=case)
                    write_json(suite/"status.json", status)
                    return suite
                if phase == "held_out":
                    pairs.append(dict(simulation=str(sim.resolve()), native=str(native.resolve())))
                    write_json(suite/"pairs.json", pairs)
    validation = validate_suite(suite/"pairs.json")
    write_json(suite/"validation.json", validation)
    status.update(status="completed" if validation["passed"] else "validation_failed")
    write_json(suite/"status.json", status)
    return suite
