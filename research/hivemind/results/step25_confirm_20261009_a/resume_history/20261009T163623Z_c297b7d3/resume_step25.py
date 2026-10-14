"""Checkpoint and resume the frozen Step 25 confirmation manifest.

`--checkpoint` and `--dry-run` are offline. Only `--resume` imports the
experiment harness and starts loopback peers. Touch `STOP_AFTER_CURRENT` in the
suite directory to stop after the current job. The original suite manifest and
provenance are read-only; resume metadata is kept in separate files.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import random
import re
import shutil
import subprocess
import sys
import uuid


ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = ROOT.parents[1]
RAW_FILES = ("config.json", "events.jsonl", "summary.json")
REQUIRED_HARNESS = {
    "step25_choice_measurement.py", "step24_lifetime_sweep.py",
    "step22_refresh_leadership.py", "analyze_step24_lifetime.py",
    "analyze_step25_choices.py",
}
SEEDS = tuple(range(2501, 2511))
SCHEMA_VERSION = 1


class IncompleteRawTrial(ValueError):
    """Evidence indicates an interrupted write rather than a closed bad trial."""


def _utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json(path):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read valid JSON from {path}: {exc}") from exc


def _atomic_json(path, value):
    """Replace one metadata file atomically, keeping temp data on its filesystem."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("x", encoding="utf-8") as stream:
            json.dump(value, stream, indent=2, sort_keys=True, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _expected_cases():
    cases = []
    for name, lower, upper in (("range_0_1", 0.0, 1.0),
                               ("range_025_125", 0.25, 1.25),
                               ("range_1_2", 1.0, 2.0),
                               ("range_2_3", 2.0, 3.0),
                               ("range_3_4", 3.0, 4.0)):
        cases.append(dict(name=name, mode="refresh", minimum=4, target=4,
                          frequency=1.0, lifetime=upper, nominal_lower=lower,
                          nominal_upper=upper))
    cases.extend((
        dict(name="expire_2", mode="expire", minimum=4, target=4, lifetime=2.0),
        dict(name="passive_stock", mode="passive", minimum=4, target=4, lifetime=5.0),
        dict(name="stock", mode="stock", minimum=4, target=4, lifetime=5.0),
    ))
    return cases


def _job_key(job):
    return json.dumps(job, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _check_manifest(suite, require_original_design=True):
    if (suite.get("phase") != "confirm" or suite.get("run_kind") != "confirm" or
            suite.get("outcome") != "grouping" or suite.get("instrumentation") != "detailed"):
        raise ValueError("suite must be the original detailed Step 25 grouping confirmation")
    jobs = suite.get("jobs")
    if not isinstance(jobs, list) or not jobs:
        raise ValueError("suite manifest has no jobs")
    seen = set()
    for job in jobs:
        if not isinstance(job, dict) or not isinstance(job.get("case"), dict):
            raise ValueError("suite contains a malformed job")
        key = _job_key(job)
        if key in seen:
            raise ValueError("suite contains a duplicate job")
        seen.add(key)
    if require_original_design:
        expected_order = [dict(case=case, seed=seed, duration=60)
                          for seed in SEEDS for case in _expected_cases()]
        random.Random(250025).shuffle(expected_order)
        expected = {_job_key(job) for job in expected_order}
        if len(jobs) != 80 or seen != expected:
            raise ValueError("manifest differs from the original 8-policy x 10-seed x 60-second design")
        if suite.get("order_seed") != 250025:
            raise ValueError("suite manifest order seed differs from the original confirmation")
        if [_job_key(job) for job in jobs] != [_job_key(job) for job in expected_order]:
            raise ValueError("suite job order differs from the original seeded order")
        sigma = math.sqrt(math.log(1 + 0.6 ** 2))
        training = dict(mu=math.log(1.5) - sigma ** 2 / 2, sigma=sigma,
                        lower=0.1, upper=4.0)
        if suite.get("training_delay") != training:
            raise ValueError("suite training delay differs from the original confirmation")
    return jobs


def _validate_sources(suite_dir, provenance):
    harness = provenance.get("harness_sha256")
    if not isinstance(harness, dict) or not REQUIRED_HARNESS.issubset(harness):
        raise ValueError("suite provenance lacks required Step 25 harness digests")
    harness_results = {}
    for name, expected in harness.items():
        snapshot = suite_dir / "harness" / name
        live = ROOT / name
        for label, path in (("snapshot", snapshot), ("live", live)):
            if not path.is_file() or _sha256(path) != expected:
                raise ValueError(f"{label} harness hash mismatch: {name}")
        harness_results[name] = expected

    runtime_source = provenance.get("source")
    source_hashes = provenance.get("source_sha256")
    if not isinstance(runtime_source, str) or not isinstance(source_hashes, dict) or not source_hashes:
        raise ValueError("suite provenance lacks native runtime source hashes")
    runtime_source = Path(runtime_source)
    for relative, expected in source_hashes.items():
        for path in (runtime_source / relative, PROJECT_ROOT / "hivemind" / relative):
            if not path.is_file() or _sha256(path) != expected:
                raise ValueError(f"native source hash mismatch: {relative}")
    return dict(harness_sha256=harness_results, native_source_sha256=dict(source_hashes))


def _load_analyzers(analyze_trial_fn=None, analyze_choices_fn=None):
    if analyze_trial_fn is not None and analyze_choices_fn is not None:
        return analyze_trial_fn, analyze_choices_fn
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    if analyze_trial_fn is None:
        from analyze_step24_lifetime import analyse_trial as analyze_trial_fn
    if analyze_choices_fn is None:
        from analyze_step25_choices import analyse_trial_choices as analyze_choices_fn
    return analyze_trial_fn, analyze_choices_fn


def _validate_trial_config(config, job, require_original_design=True, training_delay=None):
    if (config.get("case") != job["case"] or config.get("seed") != job["seed"] or
            config.get("duration") != job["duration"] or config.get("outcome") != "grouping" or
            config.get("instrumentation") != "detailed" or config.get("experiment") != "step25"):
        raise ValueError("trial config differs from its manifest job")
    if not require_original_design:
        return
    controls = {
        "timeout": 12, "tensor_size": 128, "honest_min": 2, "target": 4,
        "honest_ad_lifetime": 5, "allreduce_timeout": 6, "store_delay": 0,
        "delay_location": "after observed FINISHED, before next step()",
        "group_bits": "", "scheduled_time": "native default",
        "network": "eight-peer loopback", "network_delay_injected": False,
        "request_timeout": 3, "allow_state_sharing": False,
        "attempt_unit": "step_start", "event_transport": "batched",
        "trace_batch_size": 32, "duplicate_read_views": False,
    }
    for name, expected in controls.items():
        if config.get(name) != expected:
            raise ValueError(f"trial control differs from original design: {name}")
    peer_ids = config.get("peer_ids")
    if (not isinstance(peer_ids, dict) or set(peer_ids) != {"X", *(f"H{i}" for i in range(7))} or
            len(set(peer_ids.values())) != 8):
        raise ValueError("trial does not contain eight unique peer identities")
    offsets = config.get("initial_offsets")
    if not isinstance(offsets, dict) or set(offsets) != set(peer_ids):
        raise ValueError("trial initial offsets do not cover the eight peers")
    rng = random.Random(job["seed"])
    honest_offsets = [rng.uniform(0.0, 3.0) for _ in range(7)]
    expected_offsets = {"X": rng.uniform(0.0, 3.0),
                        **{f"H{i}": value for i, value in enumerate(honest_offsets)}}
    if offsets != expected_offsets:
        raise ValueError("trial initial offsets differ from the original seeded launch schedule")
    if config.get("training_delay") != training_delay:
        raise ValueError("trial training-delay metadata differs from the suite manifest")


def _event_file_valid(path):
    count, closed = 0, 0
    try:
        with path.open(encoding="utf-8") as stream:
            for line_number, line in enumerate(stream, 1):
                if not line.strip():
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError as exc:
                    if closed == 0:
                        raise IncompleteRawTrial(f"trace ended before a closure marker at line {line_number}") from exc
                    raise ValueError(f"malformed row after trace closure at line {line_number}") from exc
                if not isinstance(event, dict):
                    raise ValueError("event row is not an object")
                count += 1
                closed += event.get("kind") == "trace_closed"
    except (OSError, ValueError) as exc:
        if isinstance(exc, IncompleteRawTrial):
            raise
        raise ValueError(f"invalid raw event trace at {path}: {exc}") from exc
    if closed == 0:
        raise IncompleteRawTrial(f"raw event trace has no trace_closed event: {path}")
    if count == 0 or closed != 1:
        raise ValueError(f"raw event trace has an invalid closure count: {path}")
    return count


def _baseline_hashes(suite_dir):
    files = sorted(suite_dir.glob("pre_resume_validation_*.json"))
    if not files:
        return None
    evidence = _json(files[-1])
    baseline = evidence.get("file_sha256_before_resume")
    if not isinstance(baseline, dict):
        raise ValueError("pre-resume preservation evidence lacks file hashes")
    for relative, digest in baseline.items():
        target = (suite_dir / relative).resolve()
        if suite_dir.resolve() not in target.parents or not target.is_file() or _sha256(target) != digest:
            raise ValueError(f"pre-resume file preservation check failed: {relative}")
    return dict(path=files[-1].name, sha256=_sha256(files[-1]), file_sha256=baseline)


def _prior_checkpoint_check(suite_dir, suite_digest, provenance_digest):
    path = suite_dir / "checkpoint.json"
    if not path.exists():
        return None
    prior = _json(path)
    if prior.get("suite_sha256") != suite_digest or prior.get("original_provenance_sha256") != provenance_digest:
        raise ValueError("existing checkpoint belongs to different original suite metadata")
    for job in prior.get("jobs", []):
        if str(job.get("status", "")).startswith("completed"):
            trial = suite_dir / job["trial_dir"]
            for relative, digest in job.get("file_sha256", {}).items():
                path = (trial / relative).resolve()
                if trial.resolve() not in path.parents or not path.is_file() or _sha256(path) != digest:
                    raise ValueError(f"completed-trial file changed since checkpoint: {job['trial_dir']}/{relative}")
        archive = job.get("archive")
        if isinstance(archive, dict):
            archive_dir = Path(archive.get("path", "")).resolve()
            expected_parent = (suite_dir.parent / f"{suite_dir.name}_interrupted").resolve()
            if archive_dir.parent != expected_parent or not archive_dir.is_dir():
                raise ValueError(f"interrupted archive is missing or moved: {archive_dir}")
            for relative, digest in archive.get("file_sha256", {}).items():
                archived = (archive_dir / relative).resolve()
                if archive_dir not in archived.parents or not archived.is_file() or _sha256(archived) != digest:
                    raise ValueError(f"interrupted archive changed: {archive_dir}/{relative}")
    return prior


def validate_checkpoint(suite_path, *, analyze_trial_fn=None, analyze_choices_fn=None,
                        verify_sources=True, require_original_design=True):
    """Inspect raw jobs without writing files or importing the experiment runtime."""
    suite_dir = Path(suite_path).expanduser().resolve()
    if not suite_dir.is_absolute() or not suite_dir.is_dir():
        raise ValueError(f"suite directory does not exist: {suite_dir}")
    manifest_path, provenance_path = suite_dir / "suite.json", suite_dir / "provenance.json"
    suite, provenance = _json(manifest_path), _json(provenance_path)
    jobs = _check_manifest(suite, require_original_design=require_original_design)
    expected_dirs = {f"{job['case']['name']}_s{job['seed']}" for job in jobs}
    allowed_dirs = expected_dirs | {"harness", "resume_history"}
    for child in suite_dir.iterdir():
        if child.is_dir() and child.name not in allowed_dirs:
            raise ValueError(f"unexpected directory in canonical suite: {child.name}")
    for config_path in suite_dir.rglob("config.json"):
        if config_path.parent.name not in expected_dirs or config_path.parent.parent != suite_dir:
            raise ValueError(f"unexpected trial config directory: {config_path.parent}")
    if verify_sources:
        source_digests = _validate_sources(suite_dir, provenance)
    else:
        source_digests = dict(harness_sha256=provenance.get("harness_sha256", {}),
                              native_source_sha256=provenance.get("source_sha256", {}))
    suite_digest, provenance_digest = _sha256(manifest_path), _sha256(provenance_path)
    prior = _prior_checkpoint_check(suite_dir, suite_digest, provenance_digest)
    baseline = _baseline_hashes(suite_dir)
    analyze_trial_fn, analyze_choices_fn = _load_analyzers(analyze_trial_fn, analyze_choices_fn)
    entries, recovery_payloads = [], {}
    previous_jobs = {row.get("job_key"): row for row in (prior or {}).get("jobs", [])}
    for index, job in enumerate(jobs, 1):
        trial_name = f"{job['case']['name']}_s{job['seed']}"
        trial_dir = suite_dir / trial_name
        entry = dict(index=index, job_key=_job_key(job), trial_dir=trial_name,
                     case=job["case"], seed=job["seed"], duration=job["duration"],
                     status="pending", missing_files=[])
        prior_job = previous_jobs.get(entry["job_key"])
        if prior_job:
            for name in ("archive", "choice_recovered", "analysis_recovered"):
                if name in prior_job:
                    entry[name] = prior_job[name]
        if not trial_dir.exists():
            entries.append(entry)
            continue
        missing = [name for name in RAW_FILES if not (trial_dir / name).is_file()]
        if missing:
            if any((trial_dir / name).exists() for name in ("analysis.json", "choices.json")):
                entry.update(status="blocked", missing_files=missing,
                             error="raw files are missing despite a derived completion cache")
            else:
                entry.update(status="interrupted", missing_files=missing,
                             archive_candidate=str(trial_dir))
            entries.append(entry)
            continue
        try:
            config = _json(trial_dir / "config.json")
            event_count = _event_file_valid(trial_dir / "events.jsonl")
            try:
                summary = _json(trial_dir / "summary.json")
            except ValueError as exc:
                if not any((trial_dir / name).exists() for name in ("analysis.json", "choices.json")):
                    entry.update(status="interrupted", missing_files=["complete summary"],
                                 archive_candidate=str(trial_dir), error=str(exc))
                    entries.append(entry)
                    continue
                raise
            _validate_trial_config(config, job, require_original_design=require_original_design,
                                   training_delay=suite.get("training_delay"))
            native = analyze_trial_fn(trial_dir)
            choices = analyze_choices_fn(trial_dir)
            if summary.get("error") or summary.get("pending"):
                raise ValueError("completed summary reports an error or pending steps")
            if (not native.get("control_plane_pass") or not choices.get("step24_control_plane_pass") or
                    not choices.get("trace_integrity_pass") or not choices.get("control_plane_pass")):
                raise ValueError("native or Step 25 integrity gate failed")
            # Existing derived files are integrity evidence. Rebuild absent files,
            # but never silently replace a present file that disagrees with raw data.
            analysis_path = trial_dir / "analysis.json"
            if analysis_path.exists() and _json(analysis_path) != native:
                raise ValueError("existing native analysis differs from current raw reconstruction")
            if not analysis_path.exists():
                entry["recover_analysis"] = True
                recovery_payloads[(trial_name, "analysis.json")] = native
            choices_path = trial_dir / "choices.json"
            if choices_path.exists() and _json(choices_path) != choices:
                raise ValueError("existing choice analysis differs from current raw reconstruction")
            if not choices_path.exists():
                entry["status"] = "recover_choices"
                entry["choice_recovered"] = False
                recovery_payloads[(trial_name, "choices.json")] = choices
            else:
                entry["status"] = "completed"
                entry["choice_recovered"] = bool(entry.get("choice_recovered", False))
            entry["event_count"] = event_count
            entry["control_plane_pass"] = True
            entry["trace_integrity_pass"] = True
            entry["file_sha256"] = _file_hashes(trial_dir)
        except IncompleteRawTrial as exc:
            if any((trial_dir / name).exists() for name in ("analysis.json", "choices.json")):
                entry.update(status="blocked", error=f"closed trial cache conflicts with incomplete trace: {exc}")
            else:
                entry.update(status="interrupted", missing_files=["trace_closed"],
                             archive_candidate=str(trial_dir), error=str(exc))
        except Exception as exc:
            entry.update(status="blocked", error=f"{type(exc).__name__}: {exc}")
        entries.append(entry)

    grouped = {status: [row["job_key"] for row in entries if row["status"] == status]
               for status in ("completed", "recover_choices", "pending", "interrupted", "blocked")}
    result = dict(schema_version=SCHEMA_VERSION, suite=str(suite_dir), phase=suite["phase"],
                  suite_sha256=suite_digest, original_provenance_sha256=provenance_digest,
                  source_digests=source_digests, pre_resume_baseline=baseline,
                  expected_jobs=len(jobs), jobs=entries,
                  completed_job_keys=grouped["completed"],
                  recoverable_choice_job_keys=grouped["recover_choices"],
                  pending_job_keys=grouped["pending"],
                  interrupted_job_keys=grouped["interrupted"],
                  blocked_job_keys=grouped["blocked"],
                  counts={name: len(values) for name, values in grouped.items()},
                  valid=not grouped["blocked"],
                  next_job=(next((row["job_key"] for row in entries
                                  if row["status"] in ("pending", "interrupted")), None)))
    result["_recovery_payloads"] = recovery_payloads
    result["_prior_checkpoint"] = prior
    result["_suite_manifest"] = suite
    return result


def _checkpoint_document(state):
    return {key: value for key, value in state.items()
            if not key.startswith("_")}


def _write_checkpoint(suite_dir, state):
    document = _checkpoint_document(state)
    document["updated_utc"] = _utc_now()
    document["resume_script"] = str(Path(__file__).resolve())
    document["resume_script_sha256"] = _sha256(__file__)
    _atomic_json(suite_dir / "checkpoint.json", document)
    return document


def checkpoint_suite(suite_path, *, analyze_trial_fn=None, analyze_choices_fn=None,
                     verify_sources=True, require_original_design=True):
    """Validate offline, cache missing valid choice analysis, then checkpoint."""
    suite_dir = Path(suite_path).expanduser().resolve()
    lock_path = suite_dir / ".resume_step25.lock"
    with lock_path.open("a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ValueError("another Step 25 resume/checkpoint process holds the suite lock") from exc
        try:
            return _checkpoint_suite_locked(suite_dir, analyze_trial_fn=analyze_trial_fn,
                                            analyze_choices_fn=analyze_choices_fn,
                                            verify_sources=verify_sources,
                                            require_original_design=require_original_design)
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def _checkpoint_suite_locked(suite_path, *, analyze_trial_fn=None, analyze_choices_fn=None,
                             verify_sources=True, require_original_design=True):
    state = validate_checkpoint(suite_path, analyze_trial_fn=analyze_trial_fn,
                                analyze_choices_fn=analyze_choices_fn,
                                verify_sources=verify_sources,
                                require_original_design=require_original_design)
    if state["blocked_job_keys"]:
        raise ValueError(f"checkpoint blocked by {len(state['blocked_job_keys'])} integrity failure(s)")
    suite_dir = Path(state["suite"])
    recovered_choices, recovered_analysis = [], []
    for (trial_name, filename), payload in state["_recovery_payloads"].items():
        _atomic_json(suite_dir / trial_name / filename, payload)
        (recovered_choices if filename == "choices.json" else recovered_analysis).append(trial_name)
    recovered = sorted(set(recovered_choices + recovered_analysis))
    if recovered:
        for row in state["jobs"]:
            if row["trial_dir"] in recovered:
                row["status"] = ("completed_recovered_choices" if row["trial_dir"] in recovered_choices
                                 else "completed_recovered_analysis")
                row["choice_recovered"] = row["trial_dir"] in recovered_choices
                row["analysis_recovered"] = row["trial_dir"] in recovered_analysis
                row.pop("recover_analysis", None)
                trial_dir = suite_dir / row["trial_dir"]
                row["file_sha256"] = {name: _sha256(trial_dir / name)
                                      for name in (*RAW_FILES, "analysis.json", "choices.json")
                                      if (trial_dir / name).is_file()}
        state["completed_job_keys"].extend(row["job_key"] for row in state["jobs"]
                                             if row["trial_dir"] in recovered and
                                             row["job_key"] not in state["completed_job_keys"])
        state["recoverable_choice_job_keys"] = []
        state["counts"]["recover_choices"] = 0
        state["counts"]["completed"] = sum(str(row["status"]).startswith("completed")
                                             for row in state["jobs"])
        state["counts"]["completed_recovered_choices"] = len(recovered_choices)
        state["counts"]["completed_recovered_analysis"] = len(recovered_analysis)
    prior = state.get("_prior_checkpoint") or {}
    state["recovered_choice_trials"] = sorted(set(prior.get("recovered_choice_trials", [])) |
                                               set(recovered_choices))
    state["recovered_analysis_trials"] = sorted(set(prior.get("recovered_analysis_trials", [])) |
                                                 set(recovered_analysis))
    state["counts"]["completed_recovered_choices"] = len(state["recovered_choice_trials"])
    state["counts"]["completed_recovered_analysis"] = len(state["recovered_analysis_trials"])
    state.pop("_recovery_payloads", None)
    state.pop("_prior_checkpoint", None)
    state.pop("_suite_manifest", None)
    return _write_checkpoint(suite_dir, state)


def dry_run(suite_path, *, analyze_trial_fn=None, analyze_choices_fn=None,
            verify_sources=True, require_original_design=True):
    """Report validation and next action without writing or importing Hivemind."""
    state = validate_checkpoint(suite_path, analyze_trial_fn=analyze_trial_fn,
                                analyze_choices_fn=analyze_choices_fn,
                                verify_sources=verify_sources,
                                require_original_design=require_original_design)
    state["would_recover_choices"] = [row["trial_dir"] for row in state["jobs"]
                                       if row["status"] == "recover_choices"]
    state["would_archive_interrupted"] = [row["trial_dir"] for row in state["jobs"]
                                            if row["status"] == "interrupted"]
    state.pop("_recovery_payloads", None)
    state.pop("_prior_checkpoint", None)
    state.pop("_suite_manifest", None)
    return state


def _validate_runtime(provenance):
    if Path(sys.executable).resolve() != Path(provenance["executable"]).resolve():
        raise ValueError("resume Python executable differs from the original verified runtime")
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    import step25_choice_measurement as measurement
    current = measurement.base.provenance()
    recorded = provenance
    for key in ("python", "torch", "source", "script_sha256", "p2pd_sha256"):
        if current.get(key) != recorded.get(key):
            raise ValueError(f"native runtime provenance differs: {key}")
    recorded_hashes = recorded.get("source_sha256", {})
    current_hashes = current.get("source_sha256", {})
    if not isinstance(recorded_hashes, dict) or not isinstance(current_hashes, dict):
        raise ValueError("native runtime source digests are missing")
    if any(recorded_hashes.get(name) != digest for name, digest in current_hashes.items()):
        raise ValueError("native runtime core source digests differ from the original suite")
    runtime_source = Path(recorded["source"])
    live_source_hashes = {name: _sha256(runtime_source / name) for name in recorded_hashes}
    if live_source_hashes != recorded_hashes:
        raise ValueError("native runtime source files differ from original provenance")
    packages = subprocess.check_output([sys.executable, "-m", "pip", "freeze"], text=True).splitlines()
    if packages != recorded.get("packages"):
        raise ValueError("installed package list differs from the original verified runtime")
    measurement.DETAIL = True
    measurement.mp.set_start_method("fork", force=True)
    measurement.base.torch.set_num_threads(1)
    runtime = dict(python=current["python"], torch=current["torch"], source=current["source"],
                   executable=str(Path(sys.executable).resolve()),
                   current_git_head=current.get("commit"),
                   source_sha256=live_source_hashes, script_sha256=current["script_sha256"],
                   p2pd_sha256=current["p2pd_sha256"], packages=packages)
    return measurement, runtime


def _file_hashes(trial_dir):
    trial_dir = Path(trial_dir)
    return {str(path.relative_to(trial_dir)): _sha256(path)
            for path in sorted(trial_dir.rglob("*")) if path.is_file()}


def _archive_interrupted(suite_dir, trial_dir):
    archive_root = suite_dir.parent / f"{suite_dir.name}_interrupted"
    archive_root.mkdir(parents=True, exist_ok=True)
    destination = archive_root / f"{trial_dir.name}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"
    os.replace(trial_dir, destination)
    return dict(path=str(destination), file_sha256=_file_hashes(destination))


def _record_state(state, suite_dir, event, runtime=None):
    state["last_resume_event"] = event
    if runtime is not None:
        state["runtime"] = runtime
    state.pop("_recovery_payloads", None)
    state.pop("_prior_checkpoint", None)
    state.pop("_suite_manifest", None)
    return _write_checkpoint(suite_dir, state)


def resume_suite(suite_path, *, run_trial=None, stop_flag=None,
                 analyze_trial_fn=None, analyze_choices_fn=None,
                 verify_sources=True, require_original_design=True):
    """Execute only unfinished manifest jobs, using the original frozen runner."""
    suite_dir = Path(suite_path).expanduser().resolve()
    lock_path = suite_dir / ".resume_step25.lock"
    with lock_path.open("a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ValueError("another Step 25 resume process holds the suite lock") from exc
        try:
            stop_path = Path(stop_flag).resolve() if stop_flag else suite_dir / "STOP_AFTER_CURRENT"
            # Explicit resume consumes a stale request before validation; new
            # requests created after this point stop at the next job boundary.
            try:
                stop_path.unlink()
            except FileNotFoundError:
                pass
            state = validate_checkpoint(suite_dir, analyze_trial_fn=analyze_trial_fn,
                                        analyze_choices_fn=analyze_choices_fn,
                                        verify_sources=verify_sources,
                                        require_original_design=require_original_design)
            if state["blocked_job_keys"]:
                raise ValueError(f"resume blocked by {len(state['blocked_job_keys'])} integrity failure(s)")
            suite = state["_suite_manifest"]
            provenance = _json(suite_dir / "provenance.json")
            if run_trial is None:
                measurement, runtime = _validate_runtime(provenance)
                run_trial = measurement.sweep.run_trial
            else:
                runtime = dict(test_runner_injected=True)
            analyze_trial_fn, analyze_choices_fn = _load_analyzers(analyze_trial_fn, analyze_choices_fn)

            # Recover valid offline work before opening the network runtime.
            recovery_files = list(state["_recovery_payloads"].items())
            recovered_choices, recovered_analysis = set(), set()
            for (trial_name, filename), payload in recovery_files:
                _atomic_json(suite_dir / trial_name / filename, payload)
                (recovered_choices if filename == "choices.json" else recovered_analysis).add(trial_name)
            for row in state["jobs"]:
                trial_name = row["trial_dir"]
                if trial_name in recovered_choices or trial_name in recovered_analysis:
                    row["status"] = ("completed_recovered_choices" if trial_name in recovered_choices
                                     else "completed_recovered_analysis")
                    row["choice_recovered"] = trial_name in recovered_choices
                    row["analysis_recovered"] = trial_name in recovered_analysis
                    row["file_sha256"] = _file_hashes(suite_dir / row["trial_dir"])
            state["completed_job_keys"].extend(row["job_key"] for row in state["jobs"]
                                                 if row["status"].startswith("completed_recovered") and
                                                 row["job_key"] not in state["completed_job_keys"])
            state["recoverable_choice_job_keys"] = []
            state["counts"]["recover_choices"] = 0
            state["counts"]["completed"] = sum(str(row["status"]).startswith("completed")
                                                 for row in state["jobs"])
            state["counts"]["completed_recovered_choices"] = sum(
                row.get("choice_recovered", False) for row in state["jobs"])
            state["counts"]["completed_recovered_analysis"] = sum(
                row.get("analysis_recovered", False) for row in state["jobs"])
            prior = state.get("_prior_checkpoint") or {}
            state["recovered_choice_trials"] = sorted(set(prior.get("recovered_choice_trials", [])) |
                                                       recovered_choices)
            state["recovered_analysis_trials"] = sorted(set(prior.get("recovered_analysis_trials", [])) |
                                                         recovered_analysis)
            state.pop("_recovery_payloads", None)

            resume_record = dict(started_utc=_utc_now(), suite_sha256=state["suite_sha256"],
                                 original_provenance_sha256=state["original_provenance_sha256"],
                                 resume_script=str(Path(__file__).resolve()),
                                 resume_script_sha256=_sha256(__file__), runtime=runtime,
                                 current_git_head=runtime.get("current_git_head"), completed_jobs=[],
                                 recovered_choice_trials=state["recovered_choice_trials"],
                                 recovered_analysis_trials=state["recovered_analysis_trials"])
            history_dir = suite_dir / "resume_history" / (
                datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:8])
            history_dir.mkdir(parents=True)
            history_script = history_dir / "resume_step25.py"
            shutil.copy2(__file__, history_script)
            resume_record["resume_script_snapshot"] = str(history_script)
            history_provenance = history_dir / "provenance.json"
            _atomic_json(history_provenance, resume_record)
            prior_index_path = suite_dir / "resume_provenance.json"
            if prior_index_path.exists():
                prior_index = _json(prior_index_path)
                if not isinstance(prior_index, dict) or not isinstance(prior_index.get("history"), list):
                    raise ValueError("existing resume provenance index is malformed")
                history = list(prior_index["history"])
            else:
                history = []
            history.append(str(history_provenance))
            _atomic_json(prior_index_path, dict(latest=str(history_provenance), history=history))
            _record_state(state, suite_dir, "resume_started", runtime)

            for row, job in zip(state["jobs"], suite["jobs"]):
                if str(row["status"]).startswith("completed"):
                    continue
                if stop_path.exists():
                    _record_state(state, suite_dir, "stopped_by_request", runtime)
                    break
                trial_dir = suite_dir / row["trial_dir"]
                if row["status"] == "interrupted" and trial_dir.exists():
                    row["archive"] = _archive_interrupted(suite_dir, trial_dir)
                elif trial_dir.exists():
                    # A complete but invalid raw triple is blocked above. This is
                    # only a defensive guard against a changed path after validation.
                    raise ValueError(f"trial directory appeared after validation: {trial_dir}")
                _record_state(state, suite_dir, f"running:{row['trial_dir']}", runtime)
                print(f"{row['index']}/{len(state['jobs'])} {row['trial_dir']}: starting", flush=True)
                native = run_trial(job["case"], job["seed"], trial_dir, job["duration"],
                                   outcome="grouping")
                if (not isinstance(native, dict) or native.get("pending") or native.get("error") or
                        native.get("control_integrity_errors") or not native.get("control_plane_pass")):
                    raise ValueError(f"native trial gate failed: {row['trial_dir']}")
                summary = _json(trial_dir / "summary.json")
                if summary.get("error") or summary.get("pending"):
                    raise ValueError(f"native summary is incomplete: {row['trial_dir']}")
                checked = analyze_choices_fn(trial_dir)
                if (not checked.get("trace_integrity_pass") or
                        not checked.get("step24_control_plane_pass") or
                        not checked.get("control_plane_pass")):
                    raise ValueError(f"Step 25 trace/control gate failed: {row['trial_dir']}")
                _atomic_json(trial_dir / "choices.json", checked)
                row.update(status="completed", choice_recovered=False,
                           control_plane_pass=True, trace_integrity_pass=True,
                           file_sha256=_file_hashes(trial_dir))
                state["completed_job_keys"].append(row["job_key"])
                state["pending_job_keys"] = [key for key in state["pending_job_keys"]
                                               if key != row["job_key"]]
                state["interrupted_job_keys"] = [key for key in state["interrupted_job_keys"]
                                                   if key != row["job_key"]]
                state["counts"]["completed"] = sum(item["status"].startswith("completed")
                                                     for item in state["jobs"])
                state["counts"]["pending"] = len(state["pending_job_keys"])
                state["counts"]["interrupted"] = len(state["interrupted_job_keys"])
                _record_state(state, suite_dir, f"completed:{row['trial_dir']}", runtime)
                resume_record["completed_jobs"].append(dict(job_key=row["job_key"],
                                                               trial_dir=row["trial_dir"],
                                                               file_sha256=row["file_sha256"]))
                _atomic_json(history_provenance, resume_record)
                print(f"{row['index']}/{len(state['jobs'])} {row['trial_dir']}: control pass; "
                      f"{state['counts']['completed']}/{len(state['jobs'])} complete", flush=True)

            unfinished = [row for row in state["jobs"]
                          if not str(row["status"]).startswith("completed")]
            if not unfinished:
                from analyze_step25_choices import analyse_suite_choices
                final = analyse_suite_choices(suite_dir, require_complete=True)
                if not final.get("control_plane_pass"):
                    raise ValueError("complete resumed suite failed the final control-plane gate")
                _atomic_json(suite_dir / "analysis.json", final)
                state["final_analysis_sha256"] = _sha256(suite_dir / "analysis.json")
                _record_state(state, suite_dir, "suite_complete", runtime)
                resume_record["finished_utc"] = _utc_now()
                resume_record["final_analysis_sha256"] = state["final_analysis_sha256"]
                _atomic_json(history_provenance, resume_record)
            return _checkpoint_document(state)
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", type=Path, required=True,
                        help="absolute path to the original Step 25 confirmation suite")
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--checkpoint", action="store_true", help="validate and save offline checkpoint")
    modes.add_argument("--dry-run", action="store_true", help="validate/report without writing or networking")
    modes.add_argument("--resume", action="store_true", help="run only pending original manifest jobs")
    args = parser.parse_args(argv)
    if not args.suite.is_absolute():
        parser.error("--suite must be an absolute path")
    try:
        if args.checkpoint:
            result = checkpoint_suite(args.suite)
        elif args.dry_run:
            result = dry_run(args.suite)
        else:
            result = resume_suite(args.suite)
        print(json.dumps(_checkpoint_document(result), indent=2, sort_keys=True, allow_nan=False))
        print(f"jobs completed={result['counts'].get('completed', 0)} "
              f"pending={result['counts'].get('pending', 0)} "
              f"interrupted={result['counts'].get('interrupted', 0)} "
              f"blocked={result['counts'].get('blocked', 0)}")
        return 0 if result.get("valid", True) and not result.get("blocked_job_keys") else 2
    except (ValueError, OSError, RuntimeError) as exc:
        print(f"Step 25 resume manager stopped: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
