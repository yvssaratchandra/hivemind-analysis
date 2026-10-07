#!/usr/bin/env python3
"""Summarize recorded baseline traces; does not launch or contact peers."""
import argparse
import datetime as dt
import json
from pathlib import Path
import re
import statistics


def analyze(directory):
    summary = json.loads((directory / "summary.json").read_text())
    samples = [json.loads(line) for line in (directory / "samples.jsonl").read_text().splitlines()]
    commands = [json.loads(line) for line in (directory / "commands.jsonl").read_text().splitlines()]
    stages = []
    for stage in summary["stages"]:
        rows = [s for s in samples if s["phase"] == stage["name"]]
        memory = {}
        for peer in stage["expected_fresh_hosts"]:
            observations = [s["peers"][peer] for s in rows if "docker_memory_bytes" in s["peers"][peer]]
            values = [s["docker_memory_bytes"] / 1024**2 for s in observations]
            if values:
                memory[peer] = {"docker_memory_median_mib": statistics.median(values),
                    "docker_memory_sampled_max_mib": max(values), "sample_count": len(values),
                    "docker_cpu_median_percent": statistics.median(
                        float(s["stats"]["CPUPerc"].rstrip("%")) for s in observations)}
        stages.append({**stage, "memory": memory})
    peak = {}
    oom = {}
    for sample in samples:
        for peer, data in sample["peers"].items():
            text = data.get("cgroup", {}).get("stdout", "")
            match = re.search(r"memory\.peak\n(\d+)", text)
            if match:
                peak[peer] = max(peak.get(peer, 0), int(match[1]))
            for field in ("oom", "oom_kill"):
                match = re.search(rf"^{field} (\d+)$", text, re.MULTILINE)
                if match:
                    oom.setdefault(peer, {})[field] = max(oom.get(peer, {}).get(field, 0), int(match[1]))
    timing = None
    kills = [c for c in commands if c["argv"][1:2] == ["kill"] and c["exit_code"] == 0]
    if kills:
        killed = dt.datetime.fromisoformat(kills[0]["time"])
        missing = [s for s in samples if s["phase"] == "peer3-crashed"
                   and all("localai-peer-3" not in p.get("fresh_hosts", ["localai-peer-3"])
                           for p in s["peers"].values())]
        if missing:
            timing = (dt.datetime.fromisoformat(missing[0]["time"]) - killed).total_seconds()
    final_views = {phase: next((s["peers"] for s in reversed(samples) if s["phase"] == phase), {})
                   for phase in ("peer3-crashed", "peer3-rejoined")}
    views = {}
    for phase, data in final_views.items():
        views[phase] = {peer: {"fresh_hosts": values.get("fresh_hosts"),
                             "listed_ids": [n["ID"] for n in values.get("membership", {}).get("federated_nodes") or []]}
                        for peer, values in data.items()}
    result = {"status": summary["status"], "source_revision": summary["source_revision"],
              "image_id": summary.get("image_id"), "stages": stages,
              "max_recorded_cgroup_peak_bytes": peak, "max_recorded_memory_events": oom,
              "first_sampled_peer3_freshness_expiry_seconds_after_kill": timing,
              "final_views": views,
              "limitations": "Single LAN run, no models. Memory includes probe overhead; "
                              "Docker memory is not RSS. Timing is sampled and includes command time. "
                              "cgroup peak is kernel-reported since container start, sampled before cleanup; "
                              "a restart may reset counters. Cached listed IDs are not verified live peers."}
    (directory / "analysis.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    print(json.dumps(analyze(args.directory), indent=2))
