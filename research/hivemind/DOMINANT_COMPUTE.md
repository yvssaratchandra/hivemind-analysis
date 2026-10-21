# Dominant-contributor gradient exclusion

Approved 2026-10-21. Native Hivemind optimizer first, including native state
sharing; simulation uses the existing framework only after behavioral and timing
validation. Root Hivemind source remains unchanged. This is a new programme,
separate from the completed E0–E5 fixed-round experiment.

| Implementation status | State |
|---|---|
| C0 sample-weight marker checks | Complete: 9/9; current frozen harness marker suite `_d` |
| C1 persistent native harness and pilot | Complete: `_b`, all three native cases pass |
| C2 five paired confirmation seeds | Replacement batch stopped: 5 pass; final baseline303 evidence failure; retain clean 304–306 |
| C3 compute-share simulation | Candidate committed (`d537373`, `9b3c1d0`) and verified fail-closed: 15 tests pass, `validate_stage` stops at the numerical gate; no timing/sweep. One baseline partial response stays unmodeled |

## Contract

Three conditions: baseline (A/B/D/C), exclusion (B removes A only from its
gradient announcements to D/C and its own gradient roster), absent (B/D/C).
B is the designated gradient leader under the same research fixture in all
conditions. Followers keep NODE reducer mode, suppress gradient leadership,
and offer B their existing gradient-step deadline as the join-request expiry;
optimizer readiness and the step deadline remain native. Marker checks use a
common scheduled-time anchor. This controlled-leader fixture is not a test of
leader-election capture. State matchmaking and sharing are not attacked. A keeps publishing
progress in exclusion. Record actual exposure when A falls behind or leaves a
proposed gradient group; do not discard these outcomes.

Persistent workers use 48-dimensional noiseless iid Gaussian linear regression,
shared seeded unit target, zero initialization, float32 SGD lr 0.1, no momentum.
Microbatches always contain 32 real examples; B/D/C are paced at 128 examples/s
and A at 896 examples/s. Native sample counts determine weights. Rate limits
restart after optimizer stalls, without catch-up. Target batch size 4096;
state averaging every epoch, native progress refresh and state recovery,
offload_optimizer=False, no delayed gradient/optimizer update. Loopback
matchmaking 1s, averaging deadline 15s, allreduce timeout 5s, next-chunk 2s.

Pilot seed 301 and confirmation seeds 302–306, each three sequential conditions,
300s measured training per condition (approximately 90 minutes). Rotate condition
order. Time starts after DHT identity discovery and optimizer initialization at a common
release barrier. Native progress advertisements begin only after real work, so
progress discovery is included in the measured cold start.
Separate evaluator reads completed model snapshots every second on 4096 fixed
examples. Primary endpoint: both D/C below 10% of their initial loss for ten
consecutive evaluations; gaps >1.5s interrupt the streak. Preserve crossing and
confirmation time and censor at 300s. Record actual model snapshot age/evaluation
lag. No outcome-driven tuning or automatic replacement of treatment failures.

## Architecture and implementation order

1. Pure workload helpers generate named iid streams, identifiers/hashes, pacing,
   evaluation and censoring. Native hooks instrument gradient/state channels and
   implement gradient-only roster policy. Controller owns persistent subprocess
   lifecycle, evaluator, clocks, immutable labels, inventories and gates.
2. C0 uses native GradientAverager with seven equal microbatches at A and one
   elsewhere, three marker seeds and all conditions. Independently reconstruct
   accumulators, weight totals, weighted means and fallback; tolerance
   atol=1e-5, rtol=1e-4. Marker evidence must pass before the training pilot.
3. C1/C2 record sample counts, readiness/epoch progress, tensor streams, model
   updates, state application/downloads, failures and owned-process cleanup.
   Pilot must show usable baseline learning and complete instrumentation before
   confirmation. Null attack effects and state-sharing compensation are valid.
4. Extend numerical aggregation with optional sample weights, preserving default
   behavior. Add no timing projection until native event replay and held-out
   timing validation pass. A bounded event model must include compute, progress
   visibility, readiness, gradient completion, state application and recovery.
   Require numerical tolerance above and median timing within 20%; retain
   censored outcomes and disclose per-seed discrepancies. On failure, stop with
   native results. If valid, simulate shares 25/50/70/90%, seeds 401–410, three
   conditions, 300 virtual seconds, B/D/C rates fixed.

## Tests and completion

Test weight arithmetic, default regression, deterministic batches, pacing,
threshold/censoring, namespace isolation, malformed evidence and cleanup.
Native package identity includes optimizer/progress/gradient/state source files.
Report D/C mean/worst, A separately, incorporated and gradient-excluded samples,
state-sharing paths and time decomposition. Do not equate gradient exclusion
with all work being wasted. Freeze source/configuration/raw traces and inventory
bulky local evidence. Commit and push reviewed stages; stop at declared gates.

## C0 evidence (2026-10-21)

`results/compute_c0_markers_20261021_c` is the complete marker matrix. All nine
cases pass raw sample/weight, announcement, chunk, fallback, output and cleanup
checks. The existing simulator numerical backend also matches all nine; compact
results are in `results/compute_c0_report_20261021_a/report.json`. Baseline weights
are A=224 and B/D/C=32 each. In the three exclusion cases, B/D/C's A-origin
coordinate is zero and each remaining origin has weight 1/3; A retains its own
input through local reduction and fallback. These are three controlled local
exclusion observations, not a deployment success rate.

Earlier marker labels `_a` and `_b` stopped on checker errors (unbound condition,
then an unused undefined fallback variable); native evidence remains inventoried.
Smoke labels `_a`–`_c` preserve startup/instrumentation failures; `_d` completed
10 seconds with all four workers and finite models. Native shutdown can produce
unconsumed all-zero-weight averaging rounds with NaNs; these are explicitly
classified only after every participant's training terminal event, never treated
as valid learning outputs.

## Pilot lifecycle correction

`compute_c1_pilot_20261021_a` preserves the initial baseline and exclusion
attempts. Both reached the full 300-second evaluation horizon, with observed
D/C crossings at 40 and 57 seconds respectively. Baseline passed all checks;
exclusion replayed all 126 numerical groups, but the controller classified A's
post-training shutdown delay as a harness failure (`exitcode=None` after a
ten-second join). All four training-terminal events are present and the owned
process audit found no survivors. The absent arm did not run. These are
diagnostic pilot observations, not a completed three-arm comparison.

The controller now records post-training cleanup deadline overruns separately
from training completion, then terminates and audits owned processes as before.
Evaluator failure and child crashes remain errors. No training, pacing, attack,
or optimizer settings changed. Fresh labels retain the strict frozen-source
reference rule: marker suite `compute_c0_markers_20261021_d`, followed by pilot
`compute_c1_pilot_20261021_b`. The original evidence remains unchanged.

The corrected pilot completed all three arms: baseline D/C crossing 40 seconds
(confirmed 49), exclusion 57 (confirmed 66), absent 121 (confirmed 130). All
three native evidence checks pass. Exclusion and absent numerical replays pass;
baseline has one confirmed partial-response replay discrepancy. Exclusion records A's
post-training cleanup deadline overrun with no surviving owned processes. The
compact report is `results/compute_c1_report_20261021_a/REPORT.md`. These remain
single-seed pilot results. Confirmation completed with unchanged settings, but
system suspension invalidated `exclusion_s302`, `absent_s302`, and
`exclusion_s303`. Seeds 304–306 have uninterrupted three-arm comparisons.
The latest sleep occurred after batch completion. See
`results/compute_suspend_audit_20261021_b/REPORT.md`; replace affected paired
seeds before full five-seed confirmation claims.

Recovery implementation uses a separate `run_compute_recovery.py` wrapper so
the native harness and reference hashes remain unchanged. It verifies the
original inventory and suspension audit, derives affected seeds independently
of outcomes, and reruns every arm for each affected seed in the original order.
Each case records monotonic, boot and wall elapsed time; a suspension or clock
jump over one second stops the batch. Only verified complete replacements enter
the new clean suite, alongside original seeds 304–306. `selection.json` maps
every case to its immutable source inventory. The existing report builder then
produces the five-seed comparison. Focused tests cover clock rejection and
paired-seed selection; acceptance requires six valid replacements, clean clock
audits, verified inventories and a complete fifteen-case report. Inspect those
checks before accepting results. Labels: `compute_c2_recovery_20261021_a`,
`compute_c2_confirmation_20261021_clean_a`, `compute_c2_report_20261021_a`.

`results/compute_c3_pilot_gate_20261021_a` records the genuine numerical gate
failure: at about 14.38 seconds in the baseline, B registered two of three
response parts from C and preserved its own input on coordinates `[32:36]`.
Native reconstruction verifies that fallback, while the numerical backend
assumes reliable complete delivery and predicts the full weighted mean (maximum
output discrepancy 0.05872). This occurred about 60 ms after B's aggregation
entry; its underlying stream-termination cause is unknown, and it is not the
attack-induced two-second wait. Do not discard or rerun this otherwise valid
native result to obtain a passing simulation gate. Timing calibration/validation
and the share sweep are therefore not enabled. Candidate event-model code is
unvalidated; synthetic tests do not override this gate.

## C3 candidate implementation and review gates

The candidate uses `trainsim.Engine` and the existing announcement policy and
weighted numerical backend. A new Hivemind compute model owns per-node paced
microbatches, progress publication/polling and readiness, gradient rendezvous,
completion deadlines, SGD, and delayed state results/application. Workload
callbacks supply actual iid gradients and evaluation; the policy alone changes
rosters. Samples, not assigned compute shares, determine aggregation weights.
Post-step stalls delay the next microbatch without catch-up.

`compute_calibration.py` extracts component measurements from the pilot: ordinary
step overhead excluding update calls, inter-batch overhead, fast gradient service,
fallback waits, and state service after the last group input (excluding rendezvous
wait). Progress visibility inferred from step snapshots is only an upper bound;
there is no exact DHT publication/fetch trace. Unobserved recovery durations must
not be invented. Unsupported recovery, membership, or state-executor overlap
must stop the candidate explicitly rather than silently approximate it.

`compute_simulation.py` will verify the evidence chain, freeze calibration and
model sources, replay native numerical groups, run the candidate at the native
70% configured share, and compare held-out seeds 302–306. For each condition,
require median D/C update interval and uncensored threshold crossing within 20%
of native values, disclose every seed's discrepancy, and require matching censor
classification. A missing endpoint is not a synthetic 300-second observation.
No held-out outcome fitting is allowed. Only a passing artifact enables the
predeclared share sweep; unsupported sweep paths remain explicit failures.

Implementation order: component extraction and event model independently;
main-agent interface integration and synthetic scheduling checks; pilot-only
calibration; held-out validation; then conditional sweep/report. Review gates
cover policy/model separation, sample accounting, delayed state application
after gradient computation, fallback-induced pacing stalls, and fail-closed
unsupported behavior. A numerical replay pass alone cannot enable the sweep.
