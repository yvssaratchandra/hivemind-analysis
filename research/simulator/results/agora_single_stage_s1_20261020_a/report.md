# Agora single-stage simulation

Run status: **completed**. Seed: **1**. Population: **100**.

Accepted sparse snapshot contributions: **500**. Observation-window total: **500**. Target time: **222.73 s**; first accepted contribution: **62.73 s**.

Roles: **4 reducers**, **96 contributors**. Contributors have simulated contribution weight 1 and reducer allocation 0; reducers contribute weight 1.
Nodes advertised: 100; observed in rosters: 100.

Ad-to-roster wait: n=500, mean=5.29 s, median=4.72 s, max=11.04 s.

Credit unit: one unique `(origin, epoch, tensor layout, sparse partition)` fully applied at one remote recipient. Partial and fallback applications receive no credit; coverage is never combined across recipients or epochs.

## Scope

- single stage with fixed assignment; no Authorizer or pipeline model
- all nodes honest; no churn, retry or attack
- synthetic equal endpoint capacities; abstract timed registry, not replicated DHT
- continuous local compute; averaging opportunities at configured step intervals, skipped if busy (harness convention)
- harness convention: first averaging round is 1; sparse selection uses round modulo partitions, step keys use local step
- symbolic sparse snapshots; no numerical gradients or convergence measurement
- node-local roster and reducer views; no globally reconciled roster
- bounded synthetic stream window; native prefetch=3 is not an end-to-end wire limit
- zero compute cost for symbolic reduction/compression; network bytes and delays are modeled
- allreduce-wide deadline follows the native baseline harness; per-stream timeouts model the runner
- Synthetic timing and resource capacities; no Internet deployment measurements.
- One simulated stage; stage placement, Authorizer behavior, and full-pipeline effects are not modeled.
- Local work is represented by delays; this run does not establish model convergence.

## Epoch results

| Epoch | Expected peers | Observed peers | Roster variants | Accepted contributions |
|---:|---:|---:|---:|---:|
| 1 | 100–100 | 4–100 | 1 | 100 |
| 2 | 100–100 | 4–100 | 1 | 100 |
| 3 | 100–100 | 4–100 | 1 | 100 |
| 4 | 100–100 | 4–100 | 1 | 100 |
| 5 | 100–100 | 4–100 | 1 | 100 |

## Outcomes

Averaging attempts: complete=500, partial=0, failed=0.
Per-origin/recipient application receipts: accepted units=500, fully applied receipts=49500, partial=0, failed=0, fallback=0.
Other protocol failure events: 0; timeout events: 0; skipped opportunities: 0.
No failures, partial applications, or skipped opportunities were recorded. No divergent local rosters were observed.

The detailed, interactive event walkthrough is in `DASHBOARD.html`. Raw trace events and resolved configuration remain the evidence source.
