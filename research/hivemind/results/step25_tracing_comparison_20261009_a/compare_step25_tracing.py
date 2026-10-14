"""Compare paired minimal/detailed pilots; timings are local logical reads."""
import argparse
import csv
import hashlib
import json
from pathlib import Path
import statistics

from analyze_step24_lifetime import analyse_suite, _event_stream, read_json, require


def pilot_metrics(suite):
    checked = analyse_suite(suite)
    require(checked["complete"] and checked["control_plane_pass"], "pilot must pass native control checks")
    result = {}
    for trial in checked["trials"]:
        path = Path(trial["source"])
        config = read_json(path/"config.json")
        honest = set(config["peer_ids"].values()) - {config["peer_ids"]["X"]}
        latencies, intervals, last = [], [], {}
        count = 0
        for event in _event_stream(path):
            if event["peer"] not in honest:
                continue
            peer, kind = event["peer"], event["kind"]
            if kind in ("search_start", "search_end"):
                last.pop(peer, None)
            elif kind == "ad_view":
                count += 1
                latencies.append(event["detail"]["latency"])
                if peer in last:
                    intervals.append(event["monotonic"]-last[peer])
                last[peer] = event["monotonic"]
        require(latencies and intervals, "pilot has no usable read timing")
        result[(trial["case"], trial["seed"])] = dict(
            case=trial["case"], seed=trial["seed"], reads=count,
            read_latency_median=statistics.median(latencies),
            same_background_search_interval_median=statistics.median(intervals),
            total_groups=trial["total_assembled_groups"], x_led=trial["x_assembled_leads"])
    return result


def compare(minimal, detailed):
    low, high = pilot_metrics(minimal), pilot_metrics(detailed)
    require(set(low) == set(high), "pilots must contain identical paired jobs")
    manifests = [read_json(Path(suite)/"provenance.json") for suite in (minimal, detailed)]
    for name in ("step24_lifetime_sweep.py", "step22_refresh_leadership.py", "analyze_step24_lifetime.py"):
        require(manifests[0]["harness_sha256"][name] == manifests[1]["harness_sha256"][name],
                f"native workload dependency differs: {name}")
    for name, digest in manifests[0]["source_sha256"].items():
        require(manifests[1]["source_sha256"].get(name) == digest, f"native runtime differs: {name}")
    rows = []
    for key in sorted(low):
        configs = [read_json(Path(suite)/f"{key[0]}_s{key[1]}"/"config.json")
                   for suite in (minimal, detailed)]
        require(configs[0]["instrumentation"] == "minimal" and configs[1]["instrumentation"] == "detailed",
                "expected minimal versus detailed tracing")
        for name in ("case", "seed", "duration", "timeout", "tensor_size", "initial_offsets",
                     "training_delay", "honest_min", "target", "honest_ad_lifetime", "allreduce_timeout",
                     "store_delay", "delay_location", "group_bits", "scheduled_time", "network"):
            require(configs[0][name] == configs[1][name], f"paired workload control differs: {name}")
        row = dict(case=key[0], seed=key[1])
        for metric in ("reads", "read_latency_median", "same_background_search_interval_median",
                       "total_groups", "x_led"):
            row[f"minimal_{metric}"] = low[key][metric]
            row[f"detailed_{metric}"] = high[key][metric]
            row[f"relative_change_{metric}"] = (high[key][metric]/low[key][metric]-1
                                                 if low[key][metric] else None)
        rows.append(row)
    return dict(pairs=rows, interpretation=(
        "Small paired local calibration, not a statistical overhead bound. Read latency is the existing "
        "ad_view instrumentation and excludes newly added read-end logging. Completion intervals "
        "within the same background search may cross follower pauses. Group counts can vary with scheduling."))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("minimal", type=Path)
    parser.add_argument("detailed", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    require(not args.output.exists(), "output already exists")
    result = compare(args.minimal, args.detailed)
    args.output.mkdir(parents=True)
    (args.output/"analysis.json").write_text(json.dumps(result, indent=2)+"\n")
    rows = result["pairs"]
    with (args.output/"pairs.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    source = Path(__file__)
    (args.output/source.name).write_bytes(source.read_bytes())
    paths = [p for suite in (args.minimal, args.detailed) for p in suite.rglob("*")
             if p.is_file() and p.name in ("config.json", "events.jsonl", "summary.json", "suite.json", "provenance.json")]
    (args.output/"input_sha256.json").write_text(json.dumps(
        {str(p.resolve()): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(paths)}, indent=2)+"\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
