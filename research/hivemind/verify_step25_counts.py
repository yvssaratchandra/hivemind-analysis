"""Independently check Step 25 counts and enabled read rates from raw events.

Uses only the standard library, without importing either experiment analyzer.
Run after a complete suite; writes to a fresh JSON path supplied by the caller.
"""
import argparse
import hashlib
import json
import math
from pathlib import Path


def digest(path):
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024*1024), b""):
            result.update(block)
    return result.hexdigest()


def check_trial(path):
    config = json.loads((path/"config.json").read_text())
    measured = json.loads((path/"choices.json").read_text())
    summary = json.loads((path/"summary.json").read_text())
    assert measured["control_plane_pass"] and summary["error"] is None and not summary["pending"]
    peers = config["peer_ids"]
    assert len(peers) == len(set(peers.values())) == 8
    x = peers["X"]
    honest = set(peers.values()) - {x}
    events = sorted((json.loads(line) for line in (path/"events.jsonl").open()),
                    key=lambda event: (event["monotonic"], event["pid"], event["event_number"]))
    starts, ends, first_search, groups = {}, {}, {}, {}
    for event in events:
        kind, detail = event["kind"], event["detail"]
        key = (event["peer"], detail.get("step_id"))
        if kind in ("step_start", "step_result"):
            rows = starts if kind == "step_start" else ends
            assert key not in rows
            rows[key] = event
        elif kind == "search_span_start":
            first_search.setdefault(key, event["monotonic"])
        elif kind == "assembled":
            group_id = detail["group_id"]
            if group_id in groups:
                assert groups[group_id]["peer"] == event["peer"]
                assert groups[group_id]["detail"]["roster"] == detail["roster"]
            else:
                groups[group_id] = event
            assert event["peer"] in detail["roster"]
    assert starts.keys() == ends.keys()
    assert len(summary["outcomes"]) == len(ends)
    x_starts = {key: event for key, event in starts.items() if key[0] == x}
    x_groups = [event for event in groups.values() if event["peer"] == x]
    first_assembly = {}
    for event in x_groups:
        key = (x, event["detail"]["step_id"])
        assert key in x_starts and key in first_search
        first_assembly.setdefault(key, event["monotonic"])
    latencies = [when-first_search[key] for key, when in first_assembly.items()]
    attempts = measured["metrics"]["x_attempts"]
    assert len(x_starts) == attempts["denominator"]
    assert len(first_assembly) == attempts["success_count"]
    assert len(groups) == measured["total_assembled_groups"]
    assert len(x_groups) == measured["x_assembled_leads"]
    assert all(value >= 0 for value in latencies)
    early = {str(n): sum(value <= n for value in latencies) for n in (1, 2, 5)}
    for n in (1, 2, 5):
        assert early[str(n)] == measured["metrics"]["x_attempts"][f"success_by_{n}s"]

    start = config["started_monotonic"]
    cutoff = start+config["duration"]
    drain_end = max(cutoff, max(event["monotonic"] for event in ends.values()))
    bounds = {"submission": (start, cutoff), "drain": (cutoff, drain_end)}
    active = {}
    enabled = dict.fromkeys(bounds, 0.0)
    reads = dict.fromkeys(bounds, 0)
    for event in events:
        if event["peer"] not in honest:
            continue
        key = (event["peer"], event["detail"].get("search_id"))
        kind, when = event["kind"], event["monotonic"]
        if kind in ("read_enabled", "read_resumed"):
            assert key not in active
            active[key] = when
        elif kind in ("read_paused", "read_disabled"):
            began = active.pop(key)
            for phase, (lower, upper) in bounds.items():
                enabled[phase] += max(0.0, min(when, upper)-max(began, lower))
        elif kind == "ad_read_start" and key in active:
            if start <= when < cutoff:
                reads["submission"] += 1
            elif cutoff <= when <= drain_end:
                reads["drain"] += 1
    assert not active
    for phase in bounds:
        checked = measured["metrics"]["cadence"]["honest_by_phase"][phase]
        assert reads[phase] == checked["read_start_count"]
        assert math.isclose(enabled[phase], checked["enabled_seconds"], abs_tol=1e-7, rel_tol=1e-8)
    return dict(case=config["case"]["name"], seed=config["seed"], trial=path.name,
                total_groups=len(groups), x_led_groups=len(x_groups), x_attempts=len(x_starts),
                x_led_attempts=len(first_assembly), success_by_seconds=early,
                mean_x_led_group_size=(sum(len(e["detail"]["roster"]) for e in x_groups)/len(x_groups)
                                       if x_groups else None),
                honest_enabled_seconds=enabled, honest_read_starts=reads,
                raw_sha256=digest(path/"events.jsonl"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("suite", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    assert not args.output.exists(), "verification output already exists"
    manifest = json.loads((args.suite/"suite.json").read_text())
    trials = [check_trial(args.suite/f"{job['case']['name']}_s{job['seed']}")
              for job in manifest["jobs"]]
    result = dict(pass_=True, trials=trials, total_groups=sum(t["total_groups"] for t in trials),
                  x_led_groups=sum(t["x_led_groups"] for t in trials),
                  verifier_sha256=digest(Path(__file__)), suite_sha256=digest(args.suite/"suite.json"))
    args.output.write_text(json.dumps(result, indent=2)+"\n")
    print(f"Independent raw checks passed: {len(trials)} trials, {result['total_groups']} groups, "
          f"{result['x_led_groups']} X-led")


if __name__ == "__main__":
    main()
