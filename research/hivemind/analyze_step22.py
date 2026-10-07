"""Recalculate step22 results from individual summaries and raw JSONL traces."""

import argparse
import collections
import json
import statistics
from pathlib import Path


def analyse_trial(path):
    summary = json.loads((path/"summary.json").read_text())
    config = json.loads((path/"config.json").read_text())
    events = [json.loads(line) for line in (path/"events.jsonl").read_text().splitlines()]
    x_id = config["peer_ids"]["X"]
    ads = [event for event in events if event["kind"] == "ad_started" and event["peer"] == x_id
           and event["detail"]["active"]]
    assert len(ads) == summary["x_ad_attempts"]
    begin = [event for event in events if event["kind"] == "join_reply" and event["peer"] == x_id
             and event["detail"]["code"] == "BEGIN_ALLREDUCE"]
    groups = {event["detail"]["group_id"]: event["detail"]["roster"] for event in begin}
    assert groups == summary["x_groups"]
    assert summary["x_completed_lead"] == bool(groups and summary["x_outcome"]["status"] == "completed")
    assert summary["honest_completed"] == sum(summary["outcomes"][f"H{i}"]["status"] == "completed"
                                              for i in range(7))
    assert not summary["hard_cancelled"] and not summary["stage_leaks"]
    assert not summary["forbidden"]
    if config["case"]["mode"] != "stock":
        assert not any(event["kind"] == "outbound_join" and event["peer"] == x_id for event in events)
        assert all(len(roster) >= config["case"]["minimum"] for roster in groups.values())
    expirations = [event["detail"]["expiration"] for event in ads]
    assert all(after > before for before, after in zip(expirations, expirations[1:]))
    receipts = [event for event in events if event["kind"] == "holder_store"
                and event["detail"]["subkey"] == x_id and event["detail"]["active"]]
    accepted = [event for event in receipts if event["detail"]["accepted"]]
    rejected = len(receipts)-len(accepted)
    follower_codes = collections.Counter(event["detail"]["code"] for event in events
                                         if event["kind"] == "join_reply" and event["peer"] == x_id)
    holder_gaps = {}
    for holder in {event["peer"] for event in accepted}:
        records = sorted((event["detail"]["received"], event["detail"]["expiration"])
                         for event in accepted if event["peer"] == holder)
        covered_until = records[0][1]
        gaps = []
        for received, expiration in records[1:]:
            if received > covered_until:
                gaps.append(received-covered_until)
            covered_until = max(covered_until, expiration)
        holder_gaps[holder] = dict(interior_gaps=len(gaps), total_gap=sum(gaps), max_gap=max(gaps, default=0))
    verified = summary["x_completed_lead"] and not summary["tensor_errors"]
    if "x_verified_lead" in summary:
        verified = summary["x_verified_lead"]
    summary["x_verified_lead"] = bool(verified)
    return dict(**summary, policy=config["case"], raw_events=len(events),
                accepted_remote_receipts=len(accepted), rejected_remote_receipts=rejected,
                follower_reply_codes=dict(follower_codes), holder_gaps=holder_gaps,
                renewal_intervals=[after["wall_time"]-before["wall_time"]
                                   for before, after in zip(ads, ads[1:])])


def aggregate(trials):
    by_case = collections.defaultdict(list)
    for trial in trials:
        by_case[trial["case"]].append(trial)
    rows = []
    for case, group in by_case.items():
        row = dict(case=case, policy=group[0]["policy"], trials=len(group),
                   completed_leads=sum(trial["x_verified_lead"] for trial in group),
                   degraded_trials=sum(bool(trial["tensor_errors"]) for trial in group),
                   honest_completed=sum(trial["honest_completed"] for trial in group),
                   honest_steps=7*len(group),
                   total_x_ad_attempts=sum(trial["x_ad_attempts"] for trial in group),
                   median_ad_attempts=statistics.median(trial["x_ad_attempts"] for trial in group),
                   median_elapsed=statistics.median(trial["elapsed"] for trial in group),
                   rejected_remote_receipts=sum(trial["rejected_remote_receipts"] for trial in group),
                   interior_holder_gaps=sum(value["interior_gaps"] for trial in group
                                            for value in trial["holder_gaps"].values()),
                   patterns={})
        for pattern in {trial["pattern"] for trial in group}:
            selected = [trial for trial in group if trial["pattern"] == pattern]
            row["patterns"][pattern] = dict(trials=len(selected),
                                           completed_leads=sum(trial["x_verified_lead"] for trial in selected),
                                           honest_completed=sum(trial["honest_completed"] for trial in selected))
        rows.append(row)
    return sorted(rows, key=lambda row: (-row["completed_leads"]/row["trials"],
                                         -row["honest_completed"]/row["honest_steps"],
                                         row["total_x_ad_attempts"]/row["trials"], row["case"]))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("suites", nargs="+", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    trials = [analyse_trial(path.parent) for suite in args.suites for path in sorted(suite.glob("*/summary.json"))]
    result = dict(trials=trials, aggregate=aggregate(trials))
    args.output.write_text(json.dumps(result, indent=2)+"\n")
    for row in result["aggregate"]:
        print(f"{row['case']:20} led {row['completed_leads']}/{row['trials']} "
              f"honest {row['honest_completed']}/{row['honest_steps']} "
              f"ads {row['total_x_ad_attempts']} median_elapsed {row['median_elapsed']:.2f}s "
              f"holder_gaps {row['interior_holder_gaps']}")


if __name__ == "__main__":
    main()
