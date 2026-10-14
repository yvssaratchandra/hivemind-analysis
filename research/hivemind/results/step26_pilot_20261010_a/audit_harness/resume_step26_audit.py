"""Resume frozen Step 26 with a separately pinned, measurement-scope RPC audit.

No experiment hook, native reconstruction, or raw trace is changed. The original
failed audit is moved intact beside its replacement and pinned in audit provenance.
Dry-run performs offline validation and starts no peers.
"""
from __future__ import annotations

import argparse
import fcntl
from datetime import datetime, timezone
from pathlib import Path

import step26_joint_sweep as frozen

AUDIT_FILES = ("resume_step26_audit.py", "step26_teardown_audit.py", "test_step26_teardown.py")
FAILED_TRIAL = "refresh_u3_f2_d150_s2601"
ORIGINAL = "joint_analysis_original_failure.json"


def evidence_plan(suite):
    state = frozen.inspect_suite(suite)
    if state["counts"]["blocked"] != 1 or state["counts"]["interrupted"]:
        raise ValueError("initial correction requires the one known closed audit failure")
    bad = next(row for row in state["jobs"] if row["status"] == "blocked")
    if bad["trial_dir"] != FAILED_TRIAL:
        raise ValueError("unexpected blocked trial")
    halt = frozen.read_json(suite / "halt.json")
    if halt.get("trial") != FAILED_TRIAL or halt.get("all_peers_shutdown") is not True:
        raise ValueError("missing post-trial shutdown evidence")
    hashes = {}
    for row in state["jobs"]:
        if row["status"] == "completed":
            for filename, digest in row["file_sha256"].items():
                hashes[f"{row['trial_dir']}/{filename}"] = digest
    path = suite / FAILED_TRIAL
    hashes.update({f"{FAILED_TRIAL}/{name}": frozen.sha256(path / name)
                   for name in (*frozen.RAW_FILES, "analysis.json")})
    hashes["suite.json"] = frozen.sha256(suite / "suite.json")
    hashes["provenance.json"] = frozen.sha256(suite / "provenance.json")
    hashes["halt_original_failure.json"] = frozen.sha256(suite / "halt.json")
    hashes[f"{FAILED_TRIAL}/{ORIGINAL}"] = frozen.sha256(path / "joint_analysis.json")
    return dict(experiment="step26", scope="audit only; native workload and all raw traces unchanged",
                created_utc=datetime.now(timezone.utc).isoformat(), recovered_trial=FAILED_TRIAL,
                before_file_sha256=hashes,
                audit_source_sha256={name: frozen.sha256(frozen.ROOT / name) for name in AUDIT_FILES},
                rule="Only start-only find/store RPCs strictly after all completed measurement spans; "
                     "their completion and delay are unobserved, not claimed as verified.")


def validate_audit(suite, provenance):
    if (provenance.get("recovered_trial") != FAILED_TRIAL
            or set(provenance.get("audit_source_sha256", {})) != set(AUDIT_FILES)):
        raise ValueError("audit provenance differs from the correction design")
    marker = suite / FAILED_TRIAL / "completion.json"
    if marker.exists():
        completion = frozen.read_json(marker)
        if completion.get("audit_provenance_sha256") != frozen.sha256(suite / "audit_provenance.json"):
            raise ValueError("audit provenance drift")
    for filename, digest in provenance["before_file_sha256"].items():
        path = suite / filename
        # Allow the two evidence moves to be unfinished following interruption.
        if not path.exists() and filename == "halt_original_failure.json":
            path = suite / "halt.json"
        if not path.exists() and filename == f"{FAILED_TRIAL}/{ORIGINAL}":
            path = suite / FAILED_TRIAL / "joint_analysis.json"
        if frozen.sha256(path) != digest:
            raise ValueError(f"pre-correction evidence changed: {filename}")
    for name, digest in provenance["audit_source_sha256"].items():
        if frozen.sha256(frozen.ROOT / name) != digest:
            raise ValueError(f"audit source drift: {name}")
        snapshot = suite / "audit_harness" / name
        if marker.exists() and not snapshot.is_file():
            raise ValueError(f"missing audit snapshot: {name}")
        if snapshot.exists() and frozen.sha256(snapshot) != digest:
            raise ValueError(f"audit snapshot drift: {name}")
    frozen.validate_sources(suite, frozen.read_json(suite / "provenance.json"))


def corrected_result(suite):
    import analyze_step26_joint as analyzer
    import step26_teardown_audit as correction
    correction.install(analyzer)
    path = suite / FAILED_TRIAL
    manifest = frozen.read_json(suite / "suite.json")
    job = next(job for job in manifest["jobs"] if frozen.trial_name(job) == FAILED_TRIAL)
    frozen.validate_trial_config(frozen.read_json(path / "config.json"), job)
    frozen.validate_trace_closed(path / "events.jsonl")
    summary = frozen.read_json(path / "summary.json")
    if summary.get("error") or summary.get("pending"):
        raise ValueError("native trial not closed cleanly")
    original = frozen.read_json(path / (ORIGINAL if (path / ORIGINAL).exists() else "joint_analysis.json"))
    result = analyzer.analyse_trial(path)
    if not result["integrity_pass"]:
        raise ValueError(f"audit correction does not resolve integrity: {result['integrity_errors']}")
    for key in ("case", "seed", "duration", "native_control_plane_pass", "trace_integrity_pass", "policy_integrity"):
        if result[key] != original[key]:
            raise ValueError(f"non-RPC analysis changed: {key}")
    rpc_metrics = {"rpc_counts", "delayed_completed_rpc_legs", "delay_activity_coverage", "unlinked_find_rpc_count"}
    for key, value in original["summary_metrics"].items():
        if key not in rpc_metrics and result["summary_metrics"].get(key) != value:
            raise ValueError(f"leadership/choice metric changed: {key}")
    return result


def prepare(suite, dry_run=False):
    provenance_path = suite / "audit_provenance.json"
    provenance = frozen.read_json(provenance_path) if provenance_path.exists() else evidence_plan(suite)
    validate_audit(suite, provenance)
    path = suite / FAILED_TRIAL
    if (path / "completion.json").exists():
        state = frozen.inspect_suite(suite)
        if not state["valid"]:
            raise ValueError(f"suite validation failed: {state['counts']}")
        return state
    result = corrected_result(suite)
    if dry_run:
        return dict(valid=True, correction_verified=True, starts_no_peers=True,
                    trial=FAILED_TRIAL, corrected_analysis=result,
                    pending=frozen.inspect_suite(suite)["counts"]["pending"])
    if not provenance_path.exists():
        frozen.atomic_json(provenance_path, provenance)
    snapshot_dir = suite / "audit_harness"
    snapshot_dir.mkdir(exist_ok=True)
    for name in AUDIT_FILES:
        target = snapshot_dir / name
        if not target.exists():
            target.write_bytes((frozen.ROOT / name).read_bytes())
    if not (suite / "halt_original_failure.json").exists():
        (suite / "halt.json").rename(suite / "halt_original_failure.json")
    if not (path / ORIGINAL).exists():
        (path / "joint_analysis.json").rename(path / ORIGINAL)
    frozen.atomic_json(path / "joint_analysis.json", result)
    hashes = {name: frozen.sha256(path / name)
              for name in (*frozen.RAW_FILES, "analysis.json", "joint_analysis.json")}
    frozen.atomic_json(path / "completion.json", dict(file_sha256=hashes, all_peers_shutdown=True,
                       finished_utc=datetime.now(timezone.utc).isoformat(),
                       audit_provenance_sha256=frozen.sha256(provenance_path),
                       original_failure_sha256=frozen.sha256(path / ORIGINAL),
                       recovery="offline measurement-scope audit; trial was not rerun"))
    state = frozen.inspect_suite(suite)
    if not state["valid"]:
        raise ValueError(f"corrected suite failed validation: {state['counts']}")
    state["active_trial"] = None
    frozen.atomic_json(suite / "checkpoint.json", state)
    return state


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--resume", required=True, type=Path)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    suite = args.resume.resolve()
    with (suite / ".controller.lock").open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        state = prepare(suite, args.dry_run)
    import json
    print(json.dumps({key: state[key] for key in ("valid", "counts", "correction_verified", "starts_no_peers", "trial", "pending") if key in state}, indent=2, sort_keys=True), flush=True)
    if args.dry_run:
        return
    # Only the parent offline analyzer uses this adapter; peer workload remains
    # exactly the frozen Step 26 runner, including native shutdown and raw events.
    import analyze_step26_joint as analyzer
    import step26_teardown_audit as correction
    correction.install(analyzer)
    frozen.run_suite(suite)
    validate_audit(suite, frozen.read_json(suite / "audit_provenance.json"))


if __name__ == "__main__":
    main()
