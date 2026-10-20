"""Run and audit an explicitly controlled, honest simulation baseline.

Native deployment timing validation is a separate claim. Each child process uses
the normal runner, including source snapshots and aggregate-result replay.
"""
import argparse
from datetime import datetime, timezone
from collections import Counter
import csv
import gzip
import hashlib
import json
import math
from pathlib import Path
import shutil
import subprocess
import sys
import statistics

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
    if manifest["status"] != summary["status"]:
        raise ValueError(f"Manifest status mismatch: {path}")
    target = config["population"] * config["target_per_node"]
    if summary["target_work"] != target:
        raise ValueError(f"Work target mismatch: {path}")
    if summary["status"] == "completed":
        if (summary["target_time"] is None or not math.isfinite(summary["target_time"])
                or summary["accepted_work"] < target
                or not 0 <= summary["target_time"] <= summary["final_time"] <= config["horizon_seconds"]):
            raise ValueError(f"Invalid completed run: {path}")
    elif (summary["target_time"] is not None or summary["accepted_work"] >= target
          or summary["final_time"] != config["horizon_seconds"]):
        raise ValueError(f"Invalid censored run: {path}")
    for key in ("accepted_work", "accepted_work_at_observation", "target_time", "group_count"):
        if summary[key] != replay[key]:
            raise ValueError(f"Replay mismatch for {key}: {path}")
    window = config["observation_seconds"]
    offsets, accepted, origins, groups = {}, set(), set(), set()
    accepted_amount = 0.0
    ready, partners = {}, {f"N{i:04d}": set() for i in range(config["population"])}
    waits, group_sizes, acceptance_times = [], [], []
    outcomes = Counter()
    last_time = -math.inf
    with gzip.open(path / "events.jsonl.gz", "rt") as stream:
        for line in stream:
            event = json.loads(line)
            kind, data, t = event["kind"], event["data"], event["time"]
            if not math.isfinite(t) or t < last_time or t < 0:
                raise ValueError(f"Nonmonotonic trace time: {path}")
            last_time = t
            if kind == "node_phase" and data.get("phase") == "compute":
                offsets.setdefault(event["actor"], t)
            if kind == "work_accepted":
                work_id = data["work_id"]
                if work_id in accepted:
                    raise ValueError(f"Duplicate credited work: {path}")
                accepted.add(work_id)
            if kind == "work_terminal":
                outcomes[data["outcome"]] += 1
            if t >= window:
                continue
            if kind == "work_ready":
                ready[data["work"]["work_id"]] = t
            elif kind == "work_accepted":
                work_id = data["work_id"]
                accepted_amount += data["amount"]
                origins.add(event["actor"])
                if work_id not in ready or t < ready[work_id]:
                    raise ValueError(f"Invalid ready-to-accept interval: {path}")
                waits.append(t - ready[work_id])
                acceptance_times.append((t, data["amount"]))
            elif kind == "group_formed":
                roster = data["roster"]
                groups.add(roster["group_id"])
                members = set(roster["members"])
                group_sizes.append(len(members))
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
    if len(accepted) != summary["accepted_work_items"] or last_time != summary["final_time"]:
        raise ValueError(f"Full-trace accounting mismatch: {path}")
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
        group_sizes_at_observation={str(size): count for size, count in sorted(Counter(group_sizes).items())},
        terminal_outcomes=dict(sorted(outcomes.items())),
        p90_ready_to_accept_seconds_at_observation=(sorted(waits)[math.ceil(.9 * len(waits)) - 1]
                                                    if waits else None),
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
             "", f"Audited runs: {len(rows)}. Seeds present: {[r['seed'] for r in rows]}.",
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
              "A one-seed pilot has no between-seed uncertainty; its repeated quartiles describe only that seed.",
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


def write_pilot_evidence(child, suite, config, row):
    """Readable control-plane evidence from the audited trace, without simulation input."""
    summary = json.loads((child / "summary.json").read_text())
    groups = {}
    accepts = []
    terminal = Counter()
    with gzip.open(child / "events.jsonl.gz", "rt") as stream:
        for line in stream:
            event = json.loads(line)
            kind, data = event["kind"], event["data"]
            if kind == "group_formed":
                groups.setdefault(data["roster"]["group_id"],
                                  dict(time=event["time"], **data["roster"]))
            elif kind == "work_accepted":
                accepts.append((event["time"], data["amount"], data["work_id"],
                                data["group_id"]))
            elif kind == "work_terminal":
                terminal[data["outcome"]] += 1
    with (suite / "accepted_work_curve.csv").open("w", newline="") as output:
        writer = csv.writer(output)
        writer.writerow(("simulated_seconds", "cumulative_accepted_work", "work_id", "group_id"))
        total = 0.0
        writer.writerow((0, 0, "", ""))
        for time, amount, work_id, group_id in accepts:
            total += amount
            writer.writerow((time, total, work_id, group_id))
    with (suite / "group_formations.csv").open("w", newline="") as output:
        writer = csv.writer(output)
        writer.writerow(("simulated_seconds", "group_id", "leader", "size", "members"))
        for group in sorted(groups.values(), key=lambda g: g["time"]):
            writer.writerow((group["time"], group["group_id"], group["leader"],
                             len(group["members"]), " ".join(group["members"])))
    # Small standalone SVG so the curve can be inspected without extra packages.
    width, height = 820, 340
    max_y = max(config["population"] * config["target_per_node"], total, 1)
    plotted_time = row["final_time"]
    coords = [(50, 300)]
    cumulative = 0.0
    for t, amount, _, _ in accepts:
        cumulative += amount
        coords.append((50 + 740 * min(t, plotted_time) / plotted_time,
                       300 - 260 * cumulative / max_y))
    points = " ".join(f"{x:.2f},{y:.2f}" for x, y in coords)
    (suite / "accepted_work_curve.svg").write_text(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">'
        '<rect width="820" height="340" fill="white"/><path d="M50 40V300H790" fill="none" stroke="black"/>'
        f'<polyline points="{points}" fill="none" stroke="#1565c0" stroke-width="2"/>'
        f'<path d="M{50+740*config["observation_seconds"]/plotted_time:.2f} 40V300" '
        'stroke="#a33" stroke-dasharray="5 4"/>'
        '<text x="55" y="30" font-size="14">Accepted work over simulated time (red: observation boundary)</text>'
        f'<text x="50" y="320" font-size="12">0 s</text><text x="735" y="320" font-size="12">{plotted_time:g} s</text>'
        f'<text x="5" y="47" font-size="12">{max_y:g}</text></svg>\n')
    first = min(groups.values(), key=lambda g: g["time"]) if groups else None
    example = (f"First group: at {first['time']:.3f} s, leader {first['leader']}, "
               f"members {', '.join(first['members'])}. "
               f"It accounts for {sum(group_id == first['group_id'] for _, _, _, group_id in accepts)} "
               "globally credited work items."
               if first else "No group formed; inspect discovery and matchmaking events.")
    phase = ", ".join(f"{key}={value:.1f}" for key, value in sorted(summary["phase_durations"].items()))
    passive = {}
    for label in ("5pct", "10pct", "20pct"):
        counts = [group["controlled_counts"].get(label, 0) for group in summary["groups"]]
        passive[label] = dict(groups_with_labelled_node=sum(value > 0 for value in counts),
                              groups_all_labelled=sum(value == len(group["members"]) for value, group in zip(counts, summary["groups"])))
    lines = ["# 100-node pilot: seed 1", "",
             "Controlled simulation evidence for research review; no real model was trained.",
             f"Status: {row['status']}; final simulated time: {row['final_time']:.3f} s; "
             f"target time: {row['target_time']}; accepted work: {row['accepted_work']}/{row['target_work']}.",
             f"Unprofiled runner time: {row['elapsed_wall_seconds']:.3f} s; "
             f"events: {summary['event_count']}; trace: {row['trace_bytes']} bytes; "
             f"peak RSS: {row['process_peak_rss_kib']} KiB.",
             f"Passed the former stalled timestamp: {row['final_time'] > 130.8432447158627}.",
             f"Groups by 180 s: {row['groups_at_observation']}; size counts: {row['group_sizes_at_observation']}.",
             f"Unique roster partners median: {row['median_unique_roster_partners_at_observation']}.",
             f"Ready-to-credit wait median/p90 by 180 s: "
             f"{row['median_ready_to_accept_seconds_at_observation']} / "
             f"{row['p90_ready_to_accept_seconds_at_observation']} s. This includes more than matchmaking.",
             f"Accepted work by 180 s: {row['accepted_work_at_observation']}; "
             f"zero-credit nodes by 180 s: {row['zero_participation_at_observation']}.",
             f"Full-run terminal outcomes: {dict(terminal)}; partial occurrences: {summary['partial_occurrences']}.",
             f"Passive research labels (all nodes behaved honestly): {passive}.",
             f"Aggregate node-seconds by phase: {phase}.",
             "", example, "",
             "Inspect group_formations.csv, accepted_work_curve.csv/SVG, the child's per_node.csv "
             "and groups.csv, and its events.jsonl.gz for roster changes, waiting, timeouts and credit.",
             "The trace has no numerical gradients or model convergence. A single seed cannot establish variability.",
             "Pilot is ready for joint research review; expansion requires explicit approval."]
    (suite / "PILOT_REVIEW.md").write_text("\n".join(lines) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "configs/controlled_baseline.json")
    parser.add_argument("--label", required=True)
    parser.add_argument("--mode", choices=("pilot", "expand"), default="pilot")
    parser.add_argument("--reviewed-pilot", type=Path)
    parser.add_argument("--run-timeout-seconds", type=float, default=600)
    args = parser.parse_args()
    c = load(args.config)
    if (c["policies"] or c["profiles"] != ["controlled"] or c["populations"] != [100]
            or c["seeds"] != list(range(1, 21)) or c["initial_offset_seconds"] <= 0):
        parser.error("Require honest 100-node controlled profile, randomized readiness, seeds 1–20")
    if not math.isfinite(args.run_timeout_seconds) or args.run_timeout_seconds <= 0:
        parser.error("Require a finite positive child timeout")
    if (args.mode == "expand") != (args.reviewed_pilot is not None):
        parser.error("Expansion requires --reviewed-pilot; pilot does not accept it")
    sources = source_manifest()["source_hashes"]
    driver_hash = digest(Path(__file__))
    pilot_path = args.reviewed_pilot.resolve() if args.reviewed_pilot else None
    if pilot_path:
        pilot_config = json.loads((pilot_path / "config.json").read_text())
        pilot_provenance = json.loads((pilot_path / "provenance.json").read_text())
        pilot_status = json.loads((pilot_path / "status.json").read_text())
        if (pilot_config != c or pilot_provenance["source_hashes"] != sources
                or pilot_provenance["driver_sha256"] != driver_hash
                or digest(pilot_path / "driver.py") != driver_hash
                or pilot_status["status"] != "awaiting_review"
                or pilot_status["audited_runs"] != 1):
            parser.error("Pilot configuration, source, driver, or status mismatch")
        child = pilot_path / "n100_controlled_s1"
        old_audit = json.loads((child / "baseline_audit.json").read_text())
        for name, expected in old_audit["sha256"].items():
            if digest(child / name) != expected:
                parser.error(f"Pilot evidence changed: {name}")
        # Recheck the trace and replay before copying the seed-1 evidence.
        if inspect_run(child, c, 1, sources) != old_audit["row"]:
            parser.error("Pilot re-audit changed")
    suite = ROOT / "results" / _safe_label(args.label)
    suite.mkdir(parents=True, exist_ok=False)
    write_json(suite / "config.json", c)
    shutil.copy2(__file__, suite / "driver.py")
    write_json(suite / "provenance.json", dict(
        source_hashes=sources, driver_sha256=driver_hash,
        scope="controlled_simulation_baseline",
        deployment_timing_validated=False,
        native_reference="native_validation_20261019_b/calibration_low_s1_comparison.json",
        started_utc=datetime.now(timezone.utc).isoformat(), mode=args.mode,
        reviewed_pilot=str(pilot_path) if pilot_path else None,
        run_timeout_seconds=args.run_timeout_seconds,
        command=sys.argv, python=sys.version,
    ))
    rows, errors = [], []
    total = 1 if args.mode == "pilot" else len(c["seeds"])
    if pilot_path:
        target = suite / "n100_controlled_s1"
        shutil.copytree(pilot_path / target.name, target)
        shutil.copy2(pilot_path / "n100_controlled_s1.log", suite / "n100_controlled_s1.log")
        rows.append(old_audit["row"])

    def execute(seed):
        label = f"n100_controlled_s{seed}"
        command = [sys.executable, "-m", "trainsim", "run", "--config", str(suite / "config.json"),
                   "--population", "100", "--seed", str(seed), "--label", label,
                   "--results-root", str(suite)]
        with (suite / f"{label}.log").open("w") as log:
            subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=True,
                           timeout=args.run_timeout_seconds)
        return inspect_run(suite / label, c, seed, sources)

    write_json(suite / "status.json", dict(status="running", expected_runs=total, audited_runs=len(rows)))
    seeds = [1] if args.mode == "pilot" else c["seeds"][1:]
    for seed in seeds:
        try:
            row = execute(seed)
            rows.append(row)
            print(f"Audited {len(rows)}/{total}: N=100 seed={seed} "
                  f"target={row['target_time']} status={row['status']}", flush=True)
        except (Exception, KeyboardInterrupt) as exc:
            errors.append(dict(population=100, seed=seed, error=repr(exc)))
            print(f"FAILED seed {seed}: {exc}", flush=True)
            break
        write_json(suite / "runs.json", rows)
        write_json(suite / "status.json", dict(status="running", expected_runs=total,
                                              audited_runs=len(rows), errors=errors))
    stats = summarize(suite, rows, c)
    write_report(suite, rows, c, stats)
    if args.mode == "pilot" and rows and not errors:
        write_pilot_evidence(suite / "n100_controlled_s1", suite, c, rows[0])
    unchanged = sources == source_manifest()["source_hashes"]
    status = ("awaiting_review" if args.mode == "pilot" else "completed") if (
        len(rows) == total and not errors and unchanged and driver_hash == digest(Path(__file__))) else "failed"
    write_json(suite / "status.json", dict(status=status, expected_runs=total,
                                          audited_runs=len(rows), errors=errors,
                                          source_unchanged=unchanged,
                                          finished_utc=datetime.now(timezone.utc).isoformat()))
    print(suite, flush=True)
    return 0 if status in ("completed", "awaiting_review") else 1


if __name__ == "__main__":
    raise SystemExit(main())
