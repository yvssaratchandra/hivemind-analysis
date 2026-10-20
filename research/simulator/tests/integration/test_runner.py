"""Exercise persisted evidence, replay, deterministic runs and safety gates."""
import gzip
import json
import os
import subprocess
import sys

import pytest

from trainsim.config import resolve


def test_small_run_replay_and_determinism(tmp_path):
    from trainsim.runner import analyze, run
    c = resolve(dict(population=8, initial_bits=0, target_group_size=4,
                     payload_bytes=512, chunk_bytes=128, horizon_seconds=30,
                     observation_seconds=20, target_per_node=1,
                     compute_seconds=.5, initial_offset_seconds=.2,
                     registry_latency_seconds=0, latency_seconds=.001))
    a = run(c, 1, "a", tmp_path)
    b = run(c, 1, "b", tmp_path)
    sa, sb = [json.loads((p/"summary.json").read_text()) for p in (a, b)]
    assert sa["accepted_work"] >= 8
    assert sa["target_time"] == sb["target_time"]
    assert sa["status"] == "completed"
    assert analyze(a)["summary"]["accepted_work"] == sa["accepted_work"]
    with gzip.open(a/"events.jsonl.gz", "rt") as af, gzip.open(b/"events.jsonl.gz", "rt") as bf:
        assert af.read() == bf.read()
    with pytest.raises(FileExistsError):
        run(c, 1, "a", tmp_path)


def test_no_group_is_censored_not_success(tmp_path):
    from trainsim.runner import run
    c = resolve(dict(population=1, payload_bytes=128, chunk_bytes=128,
                     horizon_seconds=15, observation_seconds=10,
                     initial_offset_seconds=0, compute_seconds=.1))
    path = run(c, 4, "alone", tmp_path)
    summary = json.loads((path/"summary.json").read_text())
    assert summary["status"] == "censored"
    assert summary["accepted_work"] == 0
    assert summary["target_time"] is None


def test_sweep_requires_matching_validation(tmp_path):
    from trainsim.runner import sweep
    with pytest.raises(ValueError, match="requires --validation"):
        sweep(resolve(), "refused", tmp_path)
    assert not (tmp_path/"refused").exists()


def test_reproducibility_across_python_hash_seeds(tmp_path):
    config = tmp_path/"config.json"
    config.write_text(json.dumps(dict(population=8, initial_bits=0, target_group_size=4,
                                     payload_bytes=512, chunk_bytes=128, horizon_seconds=15,
                                     observation_seconds=15, target_per_node=1,
                                     compute_seconds=.5, initial_offset_seconds=.2,
                                     registry_latency_seconds=0, latency_seconds=.001)))
    for hash_seed in (1, 2):
        subprocess.run([sys.executable, "-m", "trainsim", "run", "--config", str(config),
                        "--seed", "7", "--label", f"hash{hash_seed}", "--results-root", str(tmp_path)],
                       env=dict(os.environ, PYTHONHASHSEED=str(hash_seed)),
                       check=True, capture_output=True, text=True)
    with gzip.open(tmp_path/"hash1/events.jsonl.gz", "rt") as a, gzip.open(tmp_path/"hash2/events.jsonl.gz", "rt") as b:
        assert a.read() == b.read()
