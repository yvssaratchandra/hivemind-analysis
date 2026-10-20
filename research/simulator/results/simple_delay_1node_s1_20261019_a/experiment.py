"""One paired seed: N0000 delays aggregation messages by one second.

Uses the frozen 100-node baseline configuration and existing policy hook.
Saves configuration, runner traces, audits, and a compact paired comparison.
"""
import argparse
from collections import Counter
import gzip
import json
from pathlib import Path
import shutil
import subprocess
import sys

from run_controlled_baseline import digest, inspect_run
from trainsim.runner import ROOT, _safe_label, source_manifest, write_json


def trace_metrics(path, window):
    credit = Counter()
    failures = Counter()
    actions = []
    exposed = set()
    curve = [(0., 0.)]
    offsets = {}
    total = 0.
    with gzip.open(path / "events.jsonl.gz", "rt") as stream:
        for line in stream:
            event = json.loads(line)
            kind, data, time = event["kind"], event["data"], event["time"]
            if kind == "node_phase" and data.get("phase") == "compute":
                offsets.setdefault(event["actor"], time)
            if time >= window:
                continue
            if kind == "work_accepted":
                credit[event["actor"]] += data["amount"]
                total += data["amount"]
                curve.append((time, total))
            elif kind == "work_terminal" and data["outcome"] == "failed":
                failures[event["actor"]] += 1
            elif kind == "policy_action":
                actions.append(event)
            elif kind == "group_formed" and "N0000" in data["roster"]["members"]:
                exposed.update(set(data["roster"]["members"]) - {"N0000"})
    return dict(honest_credit=sum(v for n, v in credit.items() if n != "N0000"),
                controlled_credit=credit["N0000"], failed_attempts=sum(failures.values()),
                honest_failed_attempts=sum(v for n, v in failures.items() if n != "N0000"),
                honest_peers_sharing_roster_with_N0000=len(exposed),
                action_count=len(actions), actions=actions, curve=curve, offsets=offsets,
                per_node_credit=dict(credit))


def report(suite, baseline, attack, before, after):
    specs = [
        ("Time to 500 credits (simulated s)", baseline["target_time"], attack["target_time"]),
        ("All credits by 180 s", baseline["accepted_work_at_observation"], attack["accepted_work_at_observation"]),
        ("Honest-node credits by 180 s (same 99 nodes)", before["honest_credit"], after["honest_credit"]),
        ("N0000 credits by 180 s", before["controlled_credit"], after["controlled_credit"]),
        ("Groups formed by 180 s", baseline["groups_at_observation"], attack["groups_at_observation"]),
        ("Nodes without credit by 180 s", baseline["zero_participation_at_observation"], attack["zero_participation_at_observation"]),
        ("Failed work attempts by 180 s", before["failed_attempts"], after["failed_attempts"]),
        ("Median ready-to-credit wait (s)", baseline["median_ready_to_accept_seconds_at_observation"], attack["median_ready_to_accept_seconds_at_observation"]),
    ]
    rows = [dict(metric=name, baseline=b, attack=a, difference=a-b if a is not None and b is not None else None)
            for name, b, a in specs]
    target_slowdown = attack["target_time"] / baseline["target_time"] - 1 if attack["target_time"] is not None else None
    honest_reduction = 1 - after["honest_credit"] / before["honest_credit"]
    write_json(suite / "comparison.json", dict(seed=1, controlled_nodes=["N0000"],
        delay_seconds=1, target_time_increase_fraction=target_slowdown,
        honest_credit_reduction_fraction=honest_reduction, metrics=rows,
        policy_actions_in_window=after["action_count"],
        honest_peers_sharing_attacker_roster=after["honest_peers_sharing_roster_with_N0000"]))
    write_json(suite / "policy_actions.json", after["actions"])
    def fmt(v):
        return "censored" if v is None else f"{v:.2f}"
    table = "\n".join(f"| {r['metric']} | {fmt(r['baseline'])} | {fmt(r['attack'])} | {fmt(r['difference'])} |" for r in rows)
    explanation = ("Seed 1; 100 nodes; N0000 delays every aggregation input and result message by 1 s, "
                   "including local reducer messages. Chunk payloads and group-selection policy are unchanged. "
                   "This is a per-message send delay, not a single 1 s pause per round. "
                   "Both runs use identical initial offsets and resources. No numerical gradients are computed.")
    (suite / "REPORT.md").write_text("# One-node delay comparison\n\n" + explanation +
        "\n\n| Metric | Honest seed 1 | Delay seed 1 | Difference |\n|---|---:|---:|---:|\n" + table +
        f"\n\nLogged policy actions before 180 s: {after['action_count']}. "
        f"Distinct honest peers sharing a roster with N0000: {after['honest_peers_sharing_roster_with_N0000']}. "
        "Roster exposure is not a causal estimate for each peer. One seed demonstrates a scenario; "
        "it does not establish a general attack effect. See comparison.json and policy_actions.json.\n")
    max_y = max(before["curve"][-1][1], after["curve"][-1][1], 500)
    paths = []
    for values, color in ((before["curve"], "#2879a5"), (after["curve"], "#d37638")):
        points = " ".join(f"{45+t/180*900:.2f},{285-v/max_y*260:.2f}" for t,v in values)
        paths.append(f'<polyline points="{points}" fill="none" stroke="{color}" stroke-width="2.5"/>')
    cells = ''.join(f"<tr><td>{r['metric']}</td><td>{fmt(r['baseline'])}</td><td>{fmt(r['attack'])}</td><td>{fmt(r['difference'])}</td></tr>" for r in rows)
    (suite / "COMPARISON_DASHBOARD.html").write_text(f'''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>One-node delay · seed 1</title>
<style>body{{font-family:system-ui,sans-serif;background:#f4f7fa;color:#173047}}main{{max-width:1100px;margin:24px auto;padding:24px;background:white;border-radius:14px}}h1{{margin:0}}p{{line-height:1.5;color:#526579}}svg{{width:100%}}table{{width:100%;border-collapse:collapse}}td,th{{padding:9px;text-align:right;border-bottom:1px solid #e0e6ec}}td:first-child,th:first-child{{text-align:left}}a{{color:#2879a5}}</style></head><body><main>
<h1>One delayed node · paired seed 1</h1><p>{explanation}</p>
<p><strong style="color:#2879a5">Blue: honest baseline</strong> · <strong style="color:#d37638">Orange: N0000 delays aggregation messages</strong></p>
<svg viewBox="0 0 980 325" role="img" aria-label="Accepted work over simulated time"><path d="M45 25V285H945" fill="none" stroke="#b0bfcc"/>{''.join(paths)}<text x="5" y="30">{max_y:g}</text><text x="15" y="285">0</text><text x="45" y="310">0 s</text><text x="900" y="310">180 s</text></svg>
<table><thead><tr><th>Metric</th><th>Honest</th><th>Delay</th><th>Difference</th></tr></thead><tbody>{cells}</tbody></table>
<p>{after['action_count']} policy actions were logged before 180 s. The same 99 honest nodes are compared in both runs. One seed cannot establish the attack's general effect.</p>
<a href="REPORT.md">Full report</a> · <a href="comparison.json">Comparison data</a> · <a href="policy_actions.json">Recorded delay actions</a></main></body></html>''')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--label", required=True)
    args = parser.parse_args()
    control_suite = ROOT / "results/controlled_baseline_100_20261019_a"
    control = control_suite / "n100_controlled_s1"
    config = json.loads((control_suite / "config.json").read_text())
    sources = source_manifest()["source_hashes"]
    baseline_manifest = json.loads((control / "manifest.json").read_text())
    if baseline_manifest["source_hashes"] != sources:
        raise ValueError("Runtime source mismatch: obtain a matching control before comparing")
    saved = json.loads((control / "baseline_audit.json").read_text())
    for name, expected in saved["sha256"].items():
        if digest(control / name) != expected:
            raise ValueError(f"Baseline evidence mismatch: {name}")
    baseline = inspect_run(control, config, 1, sources)
    suite = ROOT / "results" / _safe_label(args.label)
    suite.mkdir(exist_ok=False)
    shutil.copy2(__file__, suite / "experiment.py")
    config["policies"] = {"N0000": {"kind": "fixture", "delay_transfer_seconds": 1.0}}
    write_json(suite / "config.json", config)
    write_json(suite / "status.json", dict(status="running", seed=1))
    try:
        with (suite / "run.log").open("w") as output:
            subprocess.run([sys.executable, "-m", "trainsim", "run", "--config", str(suite / "config.json"),
                            "--seed", "1", "--label", "delay_s1", "--results-root", str(suite)],
                           stdout=output, stderr=subprocess.STDOUT, timeout=600, check=True)
        attack_path = suite / "delay_s1"
        attack = inspect_run(attack_path, config, 1, sources)
        before = trace_metrics(control, config["observation_seconds"])
        after = trace_metrics(attack_path, config["observation_seconds"])
        assert before["offsets"] == after["offsets"]
        assert before["action_count"] == 0 and after["action_count"] > 0
        assert all(e["actor"] == "N0000" and e["data"]["hook"] in ("input_transfer", "result_transfer")
                   and e["data"]["selected"]["delay_seconds"] == 1 for e in after["actions"])
        assert digest(control / "profiles.json") == digest(attack_path / "profiles.json")
        assert source_manifest()["source_hashes"] == sources
        report(suite, baseline, attack, before, after)
        write_json(suite / "status.json", dict(status="completed", seed=1, control=str(control),
            identical_readiness=True, identical_profiles=True, identical_runtime_sources=True,
            treatment_only="N0000 aggregation input/result message delay = 1 second"))
    except BaseException as error:
        write_json(suite / "status.json", dict(status="failed", error=repr(error)))
        raise
    print(suite)


if __name__ == "__main__":
    main()
