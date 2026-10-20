"""The baseline report must distinguish fixed-window and end-of-run metrics."""
import importlib.util
import gzip
import json
from pathlib import Path
import shutil

import pytest

from trainsim.config import resolve
from trainsim.runner import run, source_manifest


def driver():
    path = Path(__file__).resolve().parents[2] / "run_controlled_baseline.py"
    spec = importlib.util.spec_from_file_location("controlled_baseline", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_baseline_window_excludes_later_work_and_checks_randomness(tmp_path):
    c = resolve(dict(population=8, populations=[8], profiles=["controlled"],
                     seeds=[1, 2], target_group_size=4, initial_bits=0,
                     payload_bytes=512, chunk_bytes=128, compute_seconds=.5,
                     initial_offset_seconds=.2, observation_seconds=.25,
                     horizon_seconds=20, target_per_node=1,
                     latency_seconds=.001, registry_latency_seconds=0))
    path = run(c, 1, "audit", tmp_path)
    audit = driver()
    row = audit.inspect_run(path, c, 1, source_manifest()["source_hashes"])
    assert row == json.loads((path / "baseline_audit.json").read_text())["row"]
    assert row["target_time"] > c["observation_seconds"]
    assert row["accepted_work"] >= 8
    assert row["group_count"] > 0
    assert row["groups_at_observation"] == 0
    assert row["accepted_work_at_observation"] == 0
    assert row["zero_participation_at_observation"] == 8
    assert row["randomized_readiness_verified"]
    assert row["readiness_min"] < row["readiness_max"]
    with pytest.raises(ValueError, match="Configuration mismatch"):
        audit.inspect_run(path, c, 2, source_manifest()["source_hashes"])


def test_baseline_censoring_is_visible_in_seed_statistics(tmp_path):
    rows = [dict(population=100, seed=1, status="completed", target_time=10),
            dict(population=100, seed=2, status="censored", target_time=None)]
    stats = driver().summarize(tmp_path, rows, {"populations": [100]})["100"]
    assert stats["completed"] == stats["censored"] == 1
    assert stats["metrics"]["target_time"]["n"] == 1
    assert len(json.loads((tmp_path / "runs.json").read_text())) == 2


def test_nonempty_group_counts_survive_saved_pilot_reaudit(tmp_path):
    c = resolve(dict(population=8, populations=[8], profiles=["controlled"],
                     seeds=[1, 2], target_group_size=4, initial_bits=0,
                     payload_bytes=512, chunk_bytes=128, compute_seconds=.5,
                     initial_offset_seconds=.2, observation_seconds=10,
                     horizon_seconds=20, target_per_node=1,
                     latency_seconds=.001, registry_latency_seconds=0))
    path = run(c, 1, "reuse", tmp_path)
    audit = driver()
    row = audit.inspect_run(path, c, 1, source_manifest()["source_hashes"])
    assert row["group_sizes_at_observation"]
    saved = json.loads((path / "baseline_audit.json").read_text())["row"]
    assert row == saved
    assert audit.inspect_run(path, c, 1, source_manifest()["source_hashes"]) == saved


def test_audit_rejects_duplicate_credit_after_observation(tmp_path):
    c = resolve(dict(population=8, populations=[8], profiles=["controlled"],
                     seeds=[1, 2], target_group_size=4, initial_bits=0,
                     payload_bytes=512, chunk_bytes=128, compute_seconds=.5,
                     initial_offset_seconds=.2, observation_seconds=.25,
                     horizon_seconds=20, target_per_node=1,
                     latency_seconds=.001, registry_latency_seconds=0))
    original = run(c, 1, "original", tmp_path)
    copied = tmp_path / "duplicated"
    shutil.copytree(original, copied)
    with gzip.open(copied / "events.jsonl.gz", "rt") as stream:
        credit = next(json.loads(line) for line in stream if '"kind":"work_accepted"' in line)
    credit["time"] = json.loads((copied / "summary.json").read_text())["final_time"]
    with gzip.open(copied / "events.jsonl.gz", "at") as stream:
        stream.write(json.dumps(credit) + "\n")
    with pytest.raises(ValueError, match="Duplicate credited work"):
        driver().inspect_run(copied, c, 1, source_manifest()["source_hashes"])


def test_audit_rejects_false_censoring(tmp_path):
    c = resolve(dict(population=8, populations=[8], profiles=["controlled"],
                     seeds=[1, 2], target_group_size=4, initial_bits=0,
                     payload_bytes=512, chunk_bytes=128, compute_seconds=.5,
                     initial_offset_seconds=.2, observation_seconds=.25,
                     horizon_seconds=20, target_per_node=1,
                     latency_seconds=.001, registry_latency_seconds=0))
    path = run(c, 1, "status", tmp_path)
    summary_path = path / "summary.json"
    summary = json.loads(summary_path.read_text())
    assert summary["status"] == "completed"
    summary["status"] = "censored"
    summary_path.write_text(json.dumps(summary))
    with pytest.raises(ValueError, match="Manifest status mismatch"):
        driver().inspect_run(path, c, 1, source_manifest()["source_hashes"])
