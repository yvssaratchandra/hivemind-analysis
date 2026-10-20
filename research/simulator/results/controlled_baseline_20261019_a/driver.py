"""Run and audit an explicitly controlled, honest simulation baseline.

Native deployment timing validation is a separate claim. Each child process uses
the normal runner, including source snapshots and aggregate-result replay.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys

from trainsim.analysis.progress import bootstrap_summary
from trainsim.config import load
from trainsim.randomness import rng
from trainsim.runner import ROOT, _safe_label, _write_csv, source_manifest, write_json


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def inspect_run(path, config, seed, expected_sources):
    """Audit persisted replay and compute genuinely fixed-window metrics."""
    manifest = json.loads((path / "manifest.json").read_text())
    resolved = json.loads((path / "resolved_config.json").read_text())
    summary = json.loads((path / "summary.json").read_text())
    replay = json.loads((path / "replay.json").read_text())["summary"]
    if manifest["source_hashes"] != expected_sources:
        raise ValueError(f"Source mismatch: {path}")
    if resolved != dict(config, seed=seed):
        raise ValueError(f"Configuration mismatch: {path}")
    if summary["status"] not in ("completed", "censored"):
        raise ValueError(f"Incomplete or failed run: {path}")
    for key in ("accepted_work", "accepted_work_at_observation", "target_time", "group_count"):
        if summary[key] != replay[key]:
            raise ValueError(f"Replay mismatch for {key}: {path}")
    window = config["observation_seconds"]
    offsets, accepted, origins, groups = {}, set(), set(), set()
    accepted_amount = 0.0
    ready, partners = {}, {f"N{i:04d}": set() for i in range(config["population"])}
    waits = []
    with gzip.open(path / "events.jsonl.gz", "rt") as stream:
        for line in stream:
            event = json.loads(line)
            kind, data, t = event["kind"], event["data"], event["time"]
            if kind == "node_phase" and data.get("phase") == "compute":
                offsets.setdefault(event["actor"], t)
            if t >= window:
                continue
            if kind == "work_ready":
                ready[data["work"]["work_id"]] = t
            elif kind == "work_accepted":
                work_id = data["work_id"]
                if work_id in accepted:
                    raise ValueError(f"Duplicate credited work: {path}")
                accepted.add(work_id)
                accepted_amount += data["amount"]
                origins.add(event["actor"])
                waits.append(t - ready[work_id])
            elif kind == "group_formed":
                roster = data["roster"]
                groups.add(roster["group_id"])
                members = set(roster["members"])
                for member in members:
                    partners[member].update(members - {member})
    expected_offsets = {
        node: rng(seed, "readiness", node).uniform(0, config["initial_offset_seconds"])
        for node in partners
    }
    if offsets != expected_offsets or len(set(offsets.values())) < 2:
        raise ValueError(f"Missing or incorrect randomized readiness: {path}")
    if accepted_amount != summary["accepted_work_at_observation"]:
        raise ValueError(f"Observation-window mismatch: {path}")
    import statistics
    row = {k: summary[k] for k in (
        "population", "profile", "seed", "status", "target_work", "target_time",
        "final_time", "accepted_work", "accepted_work_at_observation", "group_count",
        "zero_participation_nodes", "elapsed_wall_seconds", "process_peak_rss_kib",
        "trace_bytes",
    )}
    row.update(
        path=path.name,
        accepted_work_per_second_at_observation=accepted_amount / window,
        groups_at_observation=len(groups),
        zero_participation_at_observation=config["population"] - len(origins),
        median_ready_to_accept_seconds_at_observation=statistics.median(waits) if waits else None,
        median_unique_roster_partners_at_observation=statistics.median(map(len, partners.values())),
        readiness_min=min(offsets.values()), readiness_max=max(offsets.values()),
        replay_matches=True, randomized_readiness_verified=True,
    )
    write_json(path / "baseline_audit.json", dict(
        row=row,
        sha256={name: digest(path / name) for name in (
            "events.jsonl.gz", "resolved_config.json", "profiles.json", "manifest.json",
            "sources.tar.gz", "summary.json", "replay.json",
        )},
    ))
    return row


def summarize(suite, rows, config):
    rows = sorted(rows, key=lambda r: (r["population"], r["seed"]))
    write_json(suite / "runs.json", rows)
    _write_csv(suite / "runs.csv", rows)
    metrics = ["target_time", "accepted_work_per_second_at_observation",
               "groups_at_observation", "zero_participation_at_observation",
               "median_ready_to_accept_seconds_at_observation",
               "median_unique_roster_partners_at_observation"]
    stats = {}
    for n in config["populations"]:
        selected = [r for r in rows if r["population"] == n]
        stats[str(n)] = dict(
            completed=sum(r["status"] == "completed" for r in selected),
            censored=sum(r["status"] == "censored" for r in selected),
            metrics=bootstrap_summary(selected, metrics=metrics),
            completion_time_scope="Observed completions only; inspect censoring counts.",
        )
    write_json(suite / "statistics.json", stats)
    return stats


def write_report(suite, rows, config, stats):
    lines = ["# Controlled Hivemind simulation baseline", "",
             "Scope: honest fixed-resource simulation, not validated Internet deployment timing.",
             "", f"Audited runs: {len(rows)}. Seeds: {config['seeds']}.",
             f"Compute: {config['compute_seconds']} s; upload/download: {config['bandwidth_mbps']} Mbit/s.",
             f"Endpoint latency: {config['latency_seconds']} s (pairwise one-way delay is the sum of endpoints).",
             f"Payload: {config['payload_bytes']} bytes; chunks: {config['chunk_bytes']} bytes.",
             f"Initial readiness offsets: independent uniform [0, {config['initial_offset_seconds']}) s per node and seed.",
             f"Target: N * {config['target_per_node']} unique accepted contributions; observation: {config['observation_seconds']} s; cap: {config['horizon_seconds']} s.",
             "", "| Nodes | Completed / censored | Target time median [IQR], s | Accepted units/s median [IQR] | Zero contributors at observation, median |",
             "|---|---|---|---|---|"]
    def formatted(metric):
        return "n/a" if metric["median"] is None else (
            f"{metric['median']:.3f} [{metric['iqr'][0]:.3f}, {metric['iqr'][1]:.3f}]")
    for n in config["populations"]:
        s = stats[str(n)]
        lines.append(f"| {n} | {s['completed']} / {s['censored']} | "
                     f"{formatted(s['metrics']['target_time'])} | "
                     f"{formatted(s['metrics']['accepted_work_per_second_at_observation'])} | "
                     f"{s['metrics']['zero_participation_at_observation']['median']} |")
    lines += ["", "Uncertainty intervals in statistics.json bootstrap independent seeds, not individual groups.",
              "Target-time statistics include observed completions only; censored counts are explicit.",
              "Window metrics use [0, observation_seconds); final group/credit counts are labelled separately in runs.csv.",
              "Initial offsets are checked against the seeded generator in every raw trace. Each child runner replays aggregate results to rebuild work credit.",
              "", "One accepted unit represents one fresh batch contribution fully incorporated in a completed aggregate at another node, counted once globally.",
              "The target is global, not a per-node quota; inspect participation metrics separately.",
              "Compute and averaging do not overlap. There is no numerical model training, convergence model, churn, packet-level network, or full DHT routing.",
              "Grouping keys follow the configured population-dependent initial-bit rule; this is part of the scenario.",
              "The failed local native timing comparison remains unresolved and is not a validation pass.",
              "", "For attack/defence comparisons, preserve config.json, seeds, profiles and source snapshots; change only the intended treatment and use a new label.",
              "See runs.csv for per-seed values, statistics.json for seed-level summaries, and each child baseline_audit.json for evidence hashes."]
    (suite / "REPORT.md").write_text("\n".join(lines) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "configs/controlled_baseline.json")
    parser.add_argument("--label", required=True)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()
    c = load(args.config)
    if c["policies"] or c["profiles"] != ["controlled"] or c["initial_offset_seconds"] <= 0 or len(c["seeds"]) < 2:
        parser.error("Require honest controlled profiles, randomized readiness and multiple seeds")
    if c["observation_seconds"] <= 0 or not 1 <= args.workers <= 4:
        parser.error("Require a positive observation window and 1–4 workers")
    suite = ROOT / "results" / _safe_label(args.label)
    suite.mkdir(parents=True, exist_ok=False)
    write_json(suite / "config.json", c)
    shutil.copy2(__file__, suite / "driver.py")
    sources = source_manifest()["source_hashes"]
    write_json(suite / "provenance.json", dict(
        source_hashes=sources, driver_sha256=digest(Path(__file__)),
        scope="controlled_simulation_baseline",
        deployment_timing_validated=False,
        native_reference="native_validation_20261019_b/calibration_low_s1_comparison.json",
        started_utc=datetime.now(timezone.utc).isoformat(), workers=args.workers,
        command=sys.argv, python=sys.version,
    ))
    rows, errors = [], []
    total = len(c["populations"]) * len(c["seeds"])

    def execute(n, seed):
        label = f"n{n}_controlled_s{seed}"
        command = [sys.executable, "-m", "trainsim", "run", "--config", str(suite / "config.json"),
                   "--population", str(n), "--seed", str(seed), "--label", label,
                   "--results-root", str(suite)]
        with (suite / f"{label}.log").open("w") as log:
            subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=True)
        return inspect_run(suite / label, dict(c, population=n), seed, sources)

    write_json(suite / "status.json", dict(status="running", expected_runs=total, audited_runs=0))
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(execute, n, seed): (n, seed)
                   for n in c["populations"] for seed in c["seeds"]}
        for future in as_completed(futures):
            case = futures[future]
            try:
                row = future.result()
                rows.append(row)
                print(f"Audited {len(rows)}/{total}: N={case[0]} seed={case[1]} "
                      f"target={row['target_time']} status={row['status']}", flush=True)
            except Exception as exc:
                errors.append(dict(population=case[0], seed=case[1], error=repr(exc)))
                print(f"FAILED {case}: {exc}", flush=True)
            write_json(suite / "runs.json", rows)
            write_json(suite / "status.json", dict(status="running", expected_runs=total,
                                                  audited_runs=len(rows), errors=errors))
    stats = summarize(suite, rows, c)
    write_report(suite, rows, c, stats)
    unchanged = sources == source_manifest()["source_hashes"]
    status = "completed" if len(rows) == total and not errors and unchanged else "failed"
    write_json(suite / "status.json", dict(status=status, expected_runs=total,
                                          audited_runs=len(rows), errors=errors,
                                          source_unchanged=unchanged,
                                          finished_utc=datetime.now(timezone.utc).isoformat()))
    print(suite, flush=True)
    return 0 if status == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
