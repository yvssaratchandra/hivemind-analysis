# Tracking composition smoke: waters_20261021_b

Status: **complete**

This local marker-tensor smoke combines bucket tracking, attacker leadership, selective admission, and attacker-local roster omission. It uses two independent native `DecentralizedAverager` channels (`gradient_probe`, `state_probe`) and does not run Optimizer training.

| Arm | Channel | Captured rounds led by X | Longest streak | Exact target next-bucket predictions | X tracked target next bucket | Omission rounds | Attacker output target coefficient |
|---|---|---:|---:|---:|---:|---|---|
| capture_control | gradient_probe | 4 | 4 | 4 | 4 | [] | n/a |
| capture_control | state_probe | 3 | 2 | 3 | 2 | [] | n/a |
| capture_exclusion | gradient_probe | 2 | 2 | 1 | 1 | [3, 4] | 0, 0 |
| capture_exclusion | state_probe | 5 | 5 | 4 | 3 | [0, 1, 2, 3, 4] | 0, 0, 0, 0, 0 |

The bootstrap places A and X in the same starting shard so X can observe A's first group. This is explicit setup assistance, not a measured discovery rate. Group size two leaves no honest follower in the captured group; A receives the full native roster. The omission result therefore describes X's local averaging view and does not establish that an honest follower receives a rewritten roster.

`state_probe` is a separately matched native averager instance. It does not reproduce Optimizer state averaging cadence, progress tracking, or state recovery. Every step attempt, including no-group outcomes, is retained in `events.jsonl`; per-group native/effective rosters, leader, bucket, and marker values are also retained.
