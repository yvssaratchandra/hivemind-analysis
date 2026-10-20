# Controlled Hivemind simulation baseline

Scope: honest fixed-resource simulation, not validated Internet deployment timing.

Audited runs: 20. Seeds present: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20].
Compute: 10.0 s; upload/download: 100.0 Mbit/s.
Endpoint latency: 0.025 s (pairwise one-way delay is the sum of endpoints).
Payload: 67108864 bytes; chunks: 4194304 bytes.
Initial readiness offsets: independent uniform [0, 10.0) s per node and seed.
Target: N * 5 unique accepted contributions; observation: 180.0 s; cap: 600.0 s.

| Nodes | Completed / censored | Target time median [IQR], s | Accepted units/s median [IQR] | Zero contributors at observation, median |
|---|---|---|---|---|
| 100 | 20 / 0 | 120.668 [120.285, 121.363] | 4.425 [4.388, 4.500] | 0.0 |

Uncertainty intervals in statistics.json bootstrap independent seeds, not individual groups.
A one-seed pilot has no between-seed uncertainty; its repeated quartiles describe only that seed.
Target-time statistics include observed completions only; censored counts are explicit.
Window metrics use [0, observation_seconds); final group/credit counts are labelled separately in runs.csv.
Initial offsets are checked against the seeded generator in every raw trace. Each child runner replays aggregate results to rebuild work credit.

One accepted unit represents one fresh batch contribution fully incorporated in a completed aggregate at another node, counted once globally.
The target is global, not a per-node quota; inspect participation metrics separately.
Compute and averaging do not overlap. There is no numerical model training, convergence model, churn, packet-level network, or full DHT routing.
Grouping keys follow the configured population-dependent initial-bit rule; this is part of the scenario.
The failed local native timing comparison remains unresolved and is not a validation pass.

For attack/defence comparisons, preserve config.json, seeds, profiles and source snapshots; change only the intended treatment and use a new label.
See runs.csv for per-seed values, statistics.json for seed-level summaries, and each child baseline_audit.json for evidence hashes.

## Completion audit and ordinary failed attempts

All 20 seeds passed saved-evidence hash checks and raw full-trace unique-credit
accounting (15,965 accepted contributions in total). No node was uncredited
by 180 s in any seed. Six individual work attempts failed across seeds 5, 8,
and 20: every trace shows searching phase and 12 s elapsed since readiness,
consistent with the configured search deadline. There were no partial
aggregation occurrences. Whole-run completion was 20/20; this is separate
from the six failed work attempts. See `suite_verification.json` for each event.

The expansion took 270.228 wall seconds. A reporting-key serialization fix
required repeating seed 1; its decoded event trace and runtime source hashes
matched the reviewed pilot exactly (`pilot_reuse_verification.json`).

[Open the one-page dashboard](BASELINE_DASHBOARD.html).
