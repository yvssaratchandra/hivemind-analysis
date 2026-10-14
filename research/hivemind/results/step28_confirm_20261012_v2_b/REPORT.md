# Step 28 reliable advertisement comparison

Phase: **confirm**. Trials: **6/6**. Integrity: **True**.

Local loopback experiment with 0.15-second one-way delay on find/store/join control RPC legs. Leadership and honest attempt denominators use every raw `step_start`, including starts after the submission cutoff. X leadership is tied to the same X peer and step. Honest first group classification is tied to the same honest peer and step. Capped means assign 12 seconds to attempts without an observed assembly; the lead-time CDF counts only observed raw lead latencies at or below each threshold, with all raw X starts in its denominator.

Per-seed differences and the confirm signal are descriptive. No speed threshold is used.

## Pooled attempt outcomes

Counts pool all trials in each arm. Rates use all raw starts of the named peer cohort.

| Arm | X led / X attempts | X-led rate | X capped mean | X success-only mean / median / p90 | Honest capped mean | Honest X-led first group / honest attempts | Honest X-led rate | First outgoing X / other / none (counts, all honest starts) | Any X tried / honest starts | Accepted replies X / other |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| reliable_single | 41 / 54 | 0.759 | 4.290 | 1.845 / 1.865 / 2.115 | 2.164 | 123 / 536 | 0.229 | 119 / 402 / 15 (n=536) | 206 / 536 | 150 / 333 |
| reliable_renewed | 74 / 75 | 0.987 | 2.329 | 2.198 / 2.036 / 2.837 | 2.080 | 222 / 547 | 0.406 | 250 / 280 / 17 (n=547) | 371 / 547 | 232 / 278 |

## X lead-time CDF

A threshold count is the number of raw X attempts with observed own-step leadership latency at or below the threshold. Missing and later-than-threshold successes do not count; all raw X attempts remain in the denominator.

| Arm | ≤2 s | ≤3 s | ≤5 s | ≤12 s |
|---|---:|---:|---:|---:|
| reliable_single | 31/54 | 41/54 | 41/54 | 41/54 |
| reliable_renewed | 34/75 | 68/75 | 74/75 | 74/75 |

## Trial outcomes and publication cadence

Requested refresh frequency is blank for the single-publication arm. Achieved frequency uses the median interval between actual active publications within X searches.

| Arm | Seed | Groups | X-led groups | X led / attempts | X-led rate | Honest X-led first group / honest starts | Honest capped mean | Requested / achieved publications per second |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| reliable_single | 2801 | 50 | 16 | 16 / 20 | 0.800 | 48 / 181 | 2.042 | — / — |
| reliable_renewed | 2801 | 51 | 24 | 24 / 24 | 1.000 | 72 / 178 | 2.177 | 2.000 / 1.082 |
| reliable_renewed | 2802 | 54 | 25 | 25 / 25 | 1.000 | 75 / 189 | 1.991 | 2.000 / 1.081 |
| reliable_single | 2802 | 49 | 13 | 13 / 17 | 0.765 | 39 / 183 | 2.170 | — / — |
| reliable_single | 2803 | 47 | 12 | 12 / 17 | 0.706 | 36 / 172 | 2.286 | — / — |
| reliable_renewed | 2803 | 52 | 25 | 25 / 26 | 0.962 | 75 / 180 | 2.079 | 2.000 / 1.083 |

## Per-seed renewed minus single differences

The participation delta is the rate of first assembled groups led by X among all honest starts. The additional honest success-fraction delta counts first-group membership led by any peer.

| Seed | Δ X-led rate | Δ honest first-group X-led rate | Δ honest first-group success fraction | Δ honest capped mean (s) | Δ X capped mean (s) |
|---:|---:|---:|---:|---:|---:|
| 2801 | 0.200 | 0.139 | 0.000 | 0.135 | -1.739 |
| 2802 | 0.235 | 0.184 | 0.000 | -0.179 | -2.133 |
| 2803 | 0.256 | 0.207 | 0.000 | -0.207 | -2.113 |
| Unweighted mean | 0.230 | 0.177 | 0.000 | -0.084 | -1.995 |

## Smoke coverage

Prerequisite smoke gate: **passed**, two completed trials validated by the versioned checker. Delayed find/store/join counts: `{"find": 31888, "join": 2567, "store": 6222}`.
Checks: `null`.

## Descriptive confirm signal

Signal: `{"all_three_seed_x_led_rate_deltas_positive": true, "each_arm_each_seed_has_at_least_10_x_attempts": true, "interpretation": "Descriptive signal only; not a speed gate, significance test, or deployment rate.", "mean_x_led_rate_delta": 0.23031674208144795, "mean_x_led_rate_delta_at_least_0p10": true, "pass_": true}`.

## Interpretation and final verification

Renewal improved eventual X leadership in all three paired seeds: single
16/20, 13/17, 12/17; renewed 24/24, 25/25, 25/26. The pooled difference is
22.7 percentage points; the unweighted mean paired difference is 23.0 points.
The predefined descriptive signal passes. These are local observations, not
significance tests or deployment success probabilities.

Across all attempts, capped mean X waiting fell from 4.290s to 2.329s. Successful
single searches averaged 1.845s versus 2.198s with renewal. The tradeoff is visible
in the CDF: within two seconds, single 31/54 (57.4%) versus renewed 34/75 (45.3%);
within three seconds, single 41/54 (75.9%) versus renewed 68/75 (90.7%). Renewal
avoids many failed searches, but does not make every successful search faster.
Honest first outgoing choices to X rose from 119/536 (22.2%) to 250/547 (45.7%);
first X-led assembled-group membership rose from 123/536 (22.9%) to 222/547 (40.6%).
Selection/attempt, native acceptance, and assembled membership remain separate.

The lone validated renewed failure, X:26 in seed 2803, started 119.442s into the
120-second submission window. No fresh peer steps were submitted after cutoff.
That boundary context is recorded; it does not establish the failure's cause,
and the attempt remains in every primary denominator. No boundary filtering was
used to produce a 100% claim.

All trials use eight private loopback peers, X refusing outbound joins in both
arms, X minimum/target size four, honest minimum two/target four, and initial
2-second X ads. Requested renewal frequency was 2/sec; achieved active
publication cadence was approximately 1.081–1.083/sec. Selected remote control
RPC legs are delayed by 150ms each. This is application delay emulation, not a
full WAN. Tensor correctness is diagnostic only, and native post-group processing
can affect subsequent search timing.

Two trace-incomplete seed-2803 runs were preserved in hashed archives and excluded
from primary totals: single diagnostic 11/16 X-led attempts, renewed diagnostic
26/26. Their unary DHT RPCs lacked complete closing trace records and did not
qualify for the frozen teardown exception. Both jobs were rerun with the same
seeds, configuration, and frozen native experiment source. All other completed
trials were skipped. Their failure reports and archive hashes remain in the
checkpoint and retry_diagnostic_1.json / retry_diagnostic_2.json. The excluded
counts are inspectable and are not mixed into the six validated comparisons.

The initial export checker had clock-field and join-ID indexing mismatches. Its
source and failure report are retained. A separate versioned checker validates
every join export row, clock samples, deadlines, and exact member-step linkage.
Its import path was corrected before any confirmation peers started. The failed
startup label and log are retained. Original experiment source stayed unchanged.

Final independent raw verification confirms **303 distinct groups, 115 X-led
groups**, all-attempt timing and choices, completion/source hashes, and all
**2,479 prior source/result hashes unchanged**. See raw_verification_v2.json.
All 15 focused tests pass. Offline resume reports six completed and zero pending,
interrupted, or blocked jobs, including validation of both archived traces.
Every trial records peer shutdown; the host process check found no controller or
runtime peer daemons. Proofs: ../step28_final_resume_validation_20261012_b.json
and ../step28_shutdown_20261012_b.json. It is safe to suspend. No further
experiments are running or scheduled.
