# Dominant-contributor gradient exclusion

Approved 2026-10-21. Native Hivemind optimizer first, including native state
sharing; simulation uses the existing framework only after behavioral and timing
validation. Root Hivemind source remains unchanged. This is a new programme,
separate from the completed E0–E5 fixed-round experiment.

| Implementation status | State |
|---|---|
| C0 sample-weight marker checks | Complete: 9/9 native cases and numerical replays |
| C1 persistent native harness and pilot | Pilot running, seed 301 × three conditions |
| C2 five paired confirmation seeds | Gated on pilot |
| C3 compute-share simulation | Gated on native behavior and timing |

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
