"""Verify continuous step22 counts from raw traces and tensor outputs."""
import argparse
import collections
import json
import math
import statistics
from pathlib import Path


def analyse(path):
    summary = json.loads((path/"summary.json").read_text())
    config = json.loads((path/"config.json").read_text())
    events = [json.loads(line) for line in (path/"events.jsonl").read_text().splitlines()]
    outcomes = [event["detail"] for event in events if event["kind"] == "step_result"]
    assert outcomes == summary["outcomes"]
    indexed = {(row["name"], row["sequence"]): row for row in outcomes}
    assert len(indexed) == len(outcomes)
    for row in outcomes:
        if row["status"] == "completed":
            expected = statistics.mean(data["value"] for data in row["members"].values())
            assert abs(expected-row["expected"]) < 1e-6
            correct = all(abs(value-expected) <= 1e-4+1e-5*abs(expected) for value in row["actual"])
            assert correct == row["correct"]
    raw_groups = {event["detail"]["group_id"]: event for event in events if event["kind"] == "assembled"}
    assert set(raw_groups) == {group["group_id"] for group in summary["groups"]}
    for group in summary["groups"]:
        raw = raw_groups[group["group_id"]]
        assert group["leader"] == raw["detail"]["user"]["name"]
        assert config["peer_ids"][group["leader"]] == raw["peer"]
        row = indexed.get((group["leader"], group["sequence"]))
        members = row.get("members", {}) if row else {}
        verified = bool(set(members) == set(raw["detail"]["roster"]) and
                        all(indexed.get((data["name"], data["sequence"]), {}).get("correct", False)
                            and indexed[(data["name"], data["sequence"])].get("members") == members
                            for data in members.values()))
        assert verified == group["verified"]
        if group["leader"] == "X" and config["case"]["mode"] != "stock":
            assert group["size"] >= config["case"]["minimum"]
    x_id = config["peer_ids"]["X"]
    ads = [event for event in events if event["kind"] == "ad_started" and event["peer"] == x_id
           and event["detail"]["active"]]
    assert len(ads) == summary["x_ad_attempts"]
    assert not any(event["kind"].startswith("forbidden_") for event in events)
    if config["case"]["mode"] != "stock":
        assert not any(event["kind"] == "outbound_join" and event["peer"] == x_id for event in events)
    x_leads = sum(group["verified"] and group["leader"] == "X" for group in summary["groups"])
    all_groups = sum(group["verified"] for group in summary["groups"])
    assert (x_leads, all_groups) == (summary["x_verified_leads"], summary["all_verified_groups"])
    receipts = [event for event in events if event["kind"] == "holder_store"
                and event["detail"]["subkey"] == x_id and event["detail"]["active"]]
    codes = collections.Counter(event["detail"]["code"] for event in events
                                if event["kind"] == "join_reply" and event["peer"] == x_id)
    return dict(case=summary["case"], seed=summary["seed"], policy=config["case"],
                duration=summary["duration"], x_leads=x_leads, all_groups=all_groups,
                honest_verified=summary["honest_verified"], honest_steps=summary["honest_steps"],
                degraded_steps=summary["degraded_steps"], x_ads=len(ads),
                remote_store_requests=len(receipts), rejected_remote_stores=sum(not event["detail"]["accepted"] for event in receipts),
                follower_reply_codes=dict(codes), source=str(path))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("suites", type=Path, nargs="+")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    trials = [analyse(path.parent) for suite in args.suites for path in sorted(suite.glob("*/summary.json"))]
    grouped = collections.defaultdict(list)
    for trial in trials:
        grouped[trial["case"]].append(trial)
    aggregate = []
    for case, rows in sorted(grouped.items()):
        sums = {key: sum(row[key] for row in rows) for key in
                ("duration", "x_leads", "all_groups", "honest_verified", "honest_steps", "degraded_steps", "x_ads", "remote_store_requests", "rejected_remote_stores")}
        row = dict(case=case, policy=rows[0]["policy"], trials=len(rows), **sums)
        aggregate.append(row)
        print(f"{case:18} X groups {row['x_leads']}/{row['all_groups']} honest {row['honest_verified']}/{row['honest_steps']} "
              f"ads {row['x_ads']} degraded {row['degraded_steps']} time {row['duration']}s")
    args.output.write_text(json.dumps(dict(trials=trials, aggregate=aggregate), indent=2)+"\n")


if __name__ == "__main__":
    main()
