"""Derive the causal timing table for step20 from its saved raw event CSV.

Example:
    .venv/bin/python research/analyze_step20.py \
        research/results/step20_forged_priority_sweep_sweep_20261002_a_rounds.csv \
        research/results/step20_forged_priority_sweep_sweep_20261002_a_events.csv
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path


def read_csv(path: Path) -> list[dict]:
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def key(row: dict) -> tuple[str, str, str, str]:
    return row["window"], row["region"], row["arm"], row["seed"]


def derive(row: dict, events: list[dict]) -> dict:
    forgeries = sorted((event for event in events if event["kind"] == "forgery_complete"),
                       key=lambda event: float(event["emitted_at"]))
    forged = forgeries[0] if forgeries else None
    chosen = row["earlier_honest_id"]
    target = row["target_id"]
    first_join = float(row["first_target_join_at"]) if row["first_target_join_at"] else None
    first_changed = None
    first_changed_after_write = None
    for event in sorted(events, key=lambda item: float(item["emitted_at"])):
        if event["kind"] != "candidate_view" or event["peer"] != target:
            continue
        observed_at = float(event["emitted_at"])
        candidates = json.loads(event["detail"])["candidates"]
        for forgery in forgeries:
            forged_at = float(forgery["emitted_at"])
            if observed_at < forged_at:
                continue
            forged_peer = forgery["other"]
            entry = next((item for item in candidates if item[0] == forged_peer), None)
            changed = (row["arm"] == "withdraw" and entry is None) or (
                row["arm"] == "inflate" and entry is not None and entry[1] > forged_at + 1000
            )
            if changed:
                first_changed = observed_at
                first_changed_after_write = observed_at - forged_at
                break
        if first_changed is not None:
            break
    return dict(
        window=row["window"], region=row["region"], arm=row["arm"], seed=row["seed"],
        target_eligible=row["target_eligible"],
        forgery_accepted=row["forgery_accepted"],
        forgery_writes=len(forgeries),
        forgery_writes_accepted=sum(json.loads(event["detail"])["accepted"] for event in forgeries),
        target_joined_attacker=row["target_joined_attacker"],
        target_completed_step=row["target_completed_step"],
        target_first_choice_was_forged_peer=int(bool(chosen) and
                                                row["first_target_join_peer"] == chosen),
        target_saw_forgery=int(first_changed is not None),
        target_saw_forgery_before_first_join=int(first_changed is not None and
                                                  first_join is not None and first_changed < first_join),
        first_join_minus_selection_s="" if first_join is None or not row["selected_at"] else
                                     f"{first_join - float(row['selected_at']):.3f}",
        first_join_minus_forgery_s="" if first_join is None or forged is None else
                                   f"{first_join - float(forged['emitted_at']):.3f}",
        changed_view_minus_forgery_s="" if first_changed_after_write is None else
                                      f"{first_changed_after_write:.3f}",
        honest_completed=row["honest_completed"],
        hard_cancellations=row["hard_cancellations"],
        stage_leaks=row["stage_leaks"],
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("rounds", type=Path)
    parser.add_argument("events", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    rows = read_csv(args.rounds)
    grouped = defaultdict(list)
    for event in read_csv(args.events):
        grouped[key(event)].append(event)
    if len({key(row) for row in rows}) != len(rows):
        raise ValueError("duplicate region/window/arm/seed row")
    if any(key(row) not in grouped for row in rows):
        raise ValueError("a summary row has no matching event trace")
    derived = [derive(row, grouped[key(row)]) for row in rows]
    output = args.output or args.rounds.with_name(args.rounds.stem.replace("_rounds", "_analysis") + ".csv")
    with output.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(derived[0]))
        writer.writeheader()
        writer.writerows(derived)
    print(f"wrote {output}")
    for window in sorted({row["window"] for row in derived}):
        for arm in ("control", "withdraw", "inflate"):
            subset = [row for row in derived if row["window"] == window and row["arm"] == arm]
            if not subset:
                continue
            total = len(subset)
            print(
                f"{window:>5} {arm:<8} capture {sum(int(row['target_joined_attacker']) for row in subset)}/{total}; "
                f"forgery accepted {sum(row['forgery_accepted'] == '1' for row in subset)}/{total}; "
                f"target saw forgery before join "
                f"{sum(int(row['target_saw_forgery_before_first_join']) for row in subset)}/{total}; "
                f"honest steps {sum(int(row['honest_completed']) for row in subset)}/{3 * total}"
            )


if __name__ == "__main__":
    main()
