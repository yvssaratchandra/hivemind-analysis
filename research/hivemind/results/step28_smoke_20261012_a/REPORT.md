# Step 28 reliable advertisement comparison

Phase: **smoke**. Trials: **2/2**. Integrity: **True**.

Local loopback experiment with 0.15-second one-way delay on find/store/join control RPC legs. Leadership and honest attempt denominators use every raw `step_start`, including starts after the submission cutoff. X leadership is tied to the same X peer and step. Honest first group classification is tied to the same honest peer and step. Capped means assign 12 seconds to attempts without an observed assembly; the lead-time CDF counts only observed raw lead latencies at or below each threshold, with all raw X starts in its denominator.

Per-seed differences and the confirm signal are descriptive. No speed threshold is used.

## Pooled attempt outcomes

Counts pool all trials in each arm. Rates use all raw starts of the named peer cohort.

| Arm | X led / X attempts | X-led rate | X capped mean | X success-only mean / median / p90 | Honest capped mean | Honest X-led first group / honest attempts | Honest X-led rate | First outgoing X / other / none (counts, all honest starts) | Any X tried / honest starts | Accepted replies X / other |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| reliable_single | 3 / 4 | 0.750 | 4.357 | 1.810 / 1.666 / 2.123 | 2.336 | 9 / 29 | 0.310 | 13 / 16 / 0 (n=29) | 19 / 29 | 11 / 19 |
| reliable_renewed | 4 / 5 | 0.800 | 3.931 | 1.913 / 1.866 / 2.172 | 2.496 | 11 / 29 | 0.379 | 19 / 10 / 0 (n=29) | 26 / 29 | 15 / 17 |

## X lead-time CDF

A threshold count is the number of raw X attempts with observed own-step leadership latency at or below the threshold. Missing and later-than-threshold successes do not count; all raw X attempts remain in the denominator.

| Arm | ≤2 s | ≤3 s | ≤5 s | ≤12 s |
|---|---:|---:|---:|---:|
| reliable_single | 2/4 | 3/4 | 3/4 | 3/4 |
| reliable_renewed | 2/5 | 4/5 | 4/5 | 4/5 |

## Trial outcomes and publication cadence

Requested refresh frequency is blank for the single-publication arm. Achieved frequency uses the median interval between actual active publications within X searches.

| Arm | Seed | Groups | X-led groups | X led / attempts | X-led rate | Honest X-led first group / honest starts | Honest capped mean | Requested / achieved publications per second |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| reliable_single | 2800 | 8 | 3 | 3 / 4 | 0.750 | 9 / 29 | 2.336 | — / — |
| reliable_renewed | 2800 | 9 | 4 | 4 / 5 | 0.800 | 11 / 29 | 2.496 | 2.000 / 1.083 |

## Per-seed renewed minus single differences

The participation delta is the rate of first assembled groups led by X among all honest starts. The additional honest success-fraction delta counts first-group membership led by any peer.

| Seed | Δ X-led rate | Δ honest first-group X-led rate | Δ honest first-group success fraction | Δ honest capped mean (s) | Δ X capped mean (s) |
|---:|---:|---:|---:|---:|---:|

## Smoke coverage

Smoke gate: **True**. Delayed find/store/join counts: `{"find": 1756, "join": 164, "store": 378}`.
Checks: `{"native_group_coverage_pass": true, "reliable_renewed_refresh_observed_pass": true, "reliable_renewed_searches_with_multiple_active_publications": 5, "reliable_single_one_active_publication_per_search_pass": true}`.

## Descriptive confirm signal

Signal: `null`.
