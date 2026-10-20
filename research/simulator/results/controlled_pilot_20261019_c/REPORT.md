# Controlled Hivemind simulation baseline

Scope: honest fixed-resource simulation, not validated Internet deployment timing.

Audited runs: 1. Seeds present: [1].
Compute: 10.0 s; upload/download: 100.0 Mbit/s.
Endpoint latency: 0.025 s (pairwise one-way delay is the sum of endpoints).
Payload: 67108864 bytes; chunks: 4194304 bytes.
Initial readiness offsets: independent uniform [0, 10.0) s per node and seed.
Target: N * 5 unique accepted contributions; observation: 180.0 s; cap: 600.0 s.

| Nodes | Completed / censored | Target time median [IQR], s | Accepted units/s median [IQR] | Zero contributors at observation, median |
|---|---|---|---|---|
| 100 | 1 / 0 | 120.843 [120.843, 120.843] | 4.406 [4.406, 4.406] | 0.0 |

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
