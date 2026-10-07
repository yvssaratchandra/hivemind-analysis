"""Recalculate persistence observations and holder intervals from step22 raw traces."""
import argparse
import json
import math
import statistics
from pathlib import Path


def analyse(path):
    summary = json.loads((path/"summary.json").read_text())
    config = json.loads((path/"config.json").read_text())
    events = [json.loads(line) for line in (path/"events.jsonl").read_text().splitlines()]
    sends = [event["detail"] for event in events if event["kind"] == "endurance_send"]
    probes = [event["detail"] for event in events if event["kind"] == "endurance_probe"]
    assert sends == summary["sends"] and probes == summary["probes"]
    assert summary["writes"] == len(sends)
    assert summary["active_probes"] == sum(row["active"] for row in probes)
    assert all(after["expiration"] > before["expiration"] for before, after in zip(sends, sends[1:]))
    after_initial = [row for row in probes if row["offset"] > config["case"]["lifetime"]]
    assert summary["active_after_initial_expiry"] == sum(row["active"] for row in after_initial)
    assert summary["probes_after_initial_expiry"] == len(after_initial)
    lookup = {row["expiration"]: row["sent_wall"] for row in sends}
    receipts = [event for event in events if event["kind"] == "holder_store"
                and event["detail"]["subkey"] == config["peer_id"]
                and event["detail"]["active"] and event["detail"]["accepted"]]
    latency = [event["detail"]["received"]-lookup[event["detail"]["expiration"]] for event in receipts]
    gaps, max_gap = 0, 0.0
    for holder in {event["peer"] for event in receipts}:
        intervals = sorted((event["detail"]["received"], event["detail"]["expiration"])
                           for event in receipts if event["peer"] == holder)
        until = intervals[0][1]
        for received, expiration in intervals[1:]:
            if received > until:
                gaps += 1
                max_gap = max(max_gap, received-until)
            until = max(until, expiration)
    result = {key: summary[key] for key in ("case", "writes", "accepted_writes", "active_probes", "probe_count",
                                           "active_after_initial_expiry", "probes_after_initial_expiry")}
    result.update(remote_receipts=len(receipts), receipt_median=statistics.median(latency),
                  receipt_p95=sorted(latency)[math.ceil(.95*len(latency))-1], receipt_max=max(latency),
                  interior_holder_gaps=gaps, max_interior_gap=max_gap)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("suite", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    rows = [analyse(path.parent) for path in sorted(args.suite.glob("*/summary.json"))]
    args.output.write_text(json.dumps(rows, indent=2)+"\n")
    for row in rows:
        print(f"{row['case']}: available {row['active_probes']}/{row['probe_count']} "
              f"accepted writes {row['accepted_writes']}/{row['writes']} holder gaps {row['interior_holder_gaps']}")


if __name__ == "__main__":
    main()
