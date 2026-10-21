# Actual Optimizer capture/exclusion pilot: independent review

Seed 501; four sequential 180-second conditions; 45 local DHT nodes in each. A is paced at 896 examples/s and X/D/C at 128 each. All four raw reanalyses exactly match saved analysis; all clocks and cleanup checks pass.

| Condition | D/C target crossing | Confirmation | Final worst D/C loss | Final A loss |
|---|---:|---:|---:|---:|
| baseline | 62s | 71s | 0.000508365 | 0.000447316 |
| capture_control | 153s | 162s | 0.0466232 | 0.0378947 |
| gradient_exclusion | not reached by 180s | — | 0.17096 | 0.17096 |
| dual_exclusion | not reached by 180s | — | 0.247617 | 0.223211 |

The target is both D and C below 10% of their own initial fixed-set loss for ten consecutive one-second evaluations. No time is imputed for a censored condition.

| Condition | Channel | Assembled groups | X-led with A | Omitted D/C announcements | Completed omitted D/C outputs |
|---|---|---:|---:|---:|---:|
| baseline | gradient | 40 | 0 | 0 | 0 |
| baseline | state | 39 | 1 | 0 | 0 |
| capture_control | gradient | 18 | 18 | 0 | 0 |
| capture_control | state | 14 | 14 | 0 | 0 |
| gradient_exclusion | gradient | 10 | 10 | 12 | 10 |
| gradient_exclusion | state | 6 | 6 | 0 | 0 |
| dual_exclusion | gradient | 6 | 6 | 9 | 9 |
| dual_exclusion | state | 3 | 3 | 4 | 4 |

The combined mechanism is established locally during real Optimizer training: in dual_exclusion, X-led rosters produced nine completed omitted D/C gradient outputs and four omitted parameter-averaging outputs. Both exclusion arms failed to reach the target by 180 seconds, while capture_control reached it at 153 seconds. This is a positive mechanism result and a single-seed learning-harm observation. It does not establish the size or reproducibility of the slowdown, or whether parameter exclusion adds an effect beyond gradient exclusion.

Capture itself is expensive in this fixture. Ordinary baseline uses matchmaking_time=1s; capture conditions use 0.35s at X and 3s at honest peers plus the A-first hold. Thus baseline vs capture_control combines discovery, admission and scheduling differences. Use capture_control as the matched control for causal omission comparisons.

Group counts refer to assembled groups, not every matchmaking attempt. Timeouts and missing groups remain in the training timeline and raw traces. No deployment capture rate is estimated. 9 per-node reducer groups are incomplete across the four cases; these are retained separately from the 476 reconstructed completed groups.

Native state downloads remain enabled. The dual arm recorded two successful X-to-D downloads and no direct A-to-D/C download. That absence in one window does not prove complete isolation or removal of indirect influence. Submitted examples and actual SGD counts are preserved in review.json; submitted work is not equated with useful incorporated work.

Prior smoke _a retains the evaluator X/B naming failure. Smoke _b passed with gradient exposure only. Pilot _a retains its failed capture-control checks: received late chunks and buffer snapshots after unlock were insufficient evidence. Pilot _b observes accepted reducer inputs and samples aggregate tensors under their native lock. No workload or attack settings were tuned.

Simulation remains unvalidated. No simulation timing or compute-share sweep is implied by these native results.
