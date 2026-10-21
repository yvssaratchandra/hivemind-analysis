# Record-holder capture and exclusion during native training

Approved 2026-10-21. Connect the completed two-channel marker mechanism to the
native Optimizer and measure sustained D/C target-loss time. All nodes use private
loopback; root Hivemind remains unchanged. This is one pilot seed, not a success
rate or a confirmed convergence-effect estimate.

| Implementation status | State |
|---|---|
| Native integration and focused checks | Complete; 58 focused checks pass |
| Bounded four-condition training pilot | Complete under capture_training_pilot_20261021_b; four cases valid |
| Raw evidence review, report and commits | Complete; independent raw reanalysis matches all four cases |

## Longer time-to-target configuration — prepared, not started

User requested a higher cap to observe actual target times, then explicitly asked
to configure only. No native run or smoke is authorized by this configuration step.

| Implementation status | State |
|---|---|
| Saved four-condition preset, seed 501, 1800s cap each | Configured only |
| Stop after ten consecutive D/C target evaluations | Implemented; offline checks pass |
| Additional seconds and ratios vs both controls | Implemented; censored pairs never imputed |
| Longer native execution | Not started; wait for user instruction |

Preset: `configs/capture_training_until_target_20261021_a.json`. All four conditions
retain the previous workload, topology, policies, rates and target definition.
Each may train for up to 30 minutes; all four could therefore consume up to two
hours plus startup/cleanup. They stop early once both D/C stay below 10% of their
own initial loss for ten consecutive one-second evaluations. Crossing and
confirmation remain distinct. Measurement resolution is one second; raising a
finite cap cannot guarantee that a condition converges.

The evaluator atomically records `target_reached.json` before signalling a
separate training-stop event. Workers finish current native calls, while holders
stay available until the controller owns shutdown. The analyzer independently
checks the stop record against raw evaluations and rejects unexplained early
termination. Legacy fixed-duration execution remains the default. Reports add
observed crossing differences and ratios against baseline and capture_control;
final losses in early-stop runs are not a common-time comparison. No completed
native artifacts have been changed.

Prepared command for a later explicit start (not executed):

```sh
OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 \
PYTHONPATH=research/hivemind:research/agora/.runtime/venv/lib/python3.11/site-packages \
/home/sarat/.local/bin/python3.11 research/hivemind/run_capture_training.py \
  --config research/hivemind/configs/capture_training_until_target_20261021_a.json
```

The JSON is validated before launch and copied into the future suite inventory.
`--configure-only FILE` writes settings and exits before any worker is started.

## Approach and data flow

Use four actual Optimizers A/X/D/C, forty DHT-only holders (twenty near each
actual gradient/state advertisement key), and one bootstrap in every condition.
Use fixed empty group bits, as in the marker smoke; native key updates preserve
empty bits. Honest peers retain native advertisements, leader choices, progress,
readiness and recovery. Only controlled holders filter discovery responses.
X's early declaration and A-first admission policy are enabled in the three
capture conditions. The plain baseline uses native timing and pass-through holders.
Record group assembly leadership before native transient leader fields clear.
Omit A only when X actually leads; an escaped group is an outcome, never a retry.

Reuse the deterministic 48-dimensional iid regression workload, real batches of
32, SGD lr .1, target batch 4096, A=896 examples/s, others=128, no pacing catch-up,
and native state averaging each epoch. Trace state application and downloads;
do not block downloads or assert complete isolation. Map X to the earlier B data
stream explicitly to preserve paired workload provenance.

## Coding order and interfaces

1. Add an optional `leader_fixture=False` mode to existing trace hooks, preserving
   default behavior. A separate capture-policy module wraps native matchmaking:
   candidate/assembly logs, bounded A-first hold, announcement rewrite, leader-only
   local roster rewrite on explicitly named gradient/state prefixes. Install before
   Optimizer construction; preserve RPC annotations and opaque metadata alignment.
2. A separate runner owns topology, source/config snapshots, process barriers,
   actual Optimizers, evaluator and exact PID/create-time cleanup. Reuse existing
   workload/evaluator and holder implementation. Keep both averagers native;
   never replace scheduled parameter averaging with an independent probe.
3. Analyze raw events for each channel's attempted groups, actual X leadership,
   target presence, rewritten rosters, D/C completions, fallback and state recovery.
   Reconstruct applied reducer outputs, completed SGD updates, deterministic batches
   and evaluation losses. Evidence failures are separate from attack failures.
4. Run focused tests and a 20-second capture smoke, then one four-condition pilot
   with seed 501 and 180 seconds of training per condition. Order: baseline,
   capture_control, gradient_exclusion, dual_exclusion. Preserve startup/cleanup
   separately from training. No automatic seed expansion or simulation sweep.

## Measurements and acceptance

Primary endpoint: both D/C below 10% of their own initial evaluation loss for ten
consecutive one-second evaluations; gaps over 1.5s break the streak. Censor at the
horizon and retain noncompletion. Report first crossing and confirmation, final
D/C losses, A separately, actual exposure by channel and state-download routes.
Compare dual exclusion to capture_control and plain baseline; gradient-only gives
the incremental parameter-exclusion contrast. Report overhead of capture itself.

Tests must cover unknown/non-X leadership (no local omission), honest native
advertising, exact prefix scoping, A's unchanged announcement, aligned metadata,
actual state prefix, threshold/censoring and evidence tampering. Before accepting
any native result require finite work/evaluations, source/runtime identity, valid
monotonic/boot/wall clocks and zero surviving owned processes. Missing groups or
no slowdown remain valid outcomes; no state-channel capture is inconclusive for
combined exclusion. Stop on harness/clock failure and diagnose before retrying
under a fresh label. Freeze bulky sources/traces locally with a tracked inventory.
Commit implementation and evidence separately, then push. Simulation remains
gated by the earlier numerical and held-out timing validation requirements.

Smoke `_a` preserves a completed native run whose report failed because the old
evaluator omitted attacker label X (the old workload names it B). An explicit
naming adapter fixes that integration error. Fresh smoke `_b` passes raw checks,
clocks and cleanup; X leads one gradient group containing A and one C output
excludes A. No state group forms in 20 seconds, so dual-channel exposure is not
established by the smoke. Preserve these outcomes; the 180-second pilot tests
the actual scheduled state path without forcing honest leadership choices.

Pilot `_a`: baseline passed (crossing 50s, confirmed 59s); capture_control
completed training (diagnostic crossing 140s, confirmed 149s) but failed seven
borrowed numerical checks. Raw evidence shows wire chunks arriving after native
sender bans and an outer aggregate snapshot of zero after the tensor lock was
released. Neither run is overwritten. A dedicated observation layer now samples
inside `_run_allreduce_inplace_` while the native lock is held and records successful
`TensorPartReducer.accumulate_part` returns. Arithmetic uses accepted contributions,
not merely received chunks. Regression tests cover late banned data, buffer reset,
zero weights and tampered reducer results. The fresh `_b` four-condition pilot
keeps the same workload/attack settings and repeats the baseline to match the new
observation overhead. Earlier timings remain diagnostic and are not pooled.

Ordinary baseline uses matchmaking_time=1s; the capture fixture retains the marker
smoke's 0.35s X / 3s honest settings. Therefore the ordinary-baseline contrast
includes these scheduling settings. The causal omission comparison is against
capture_control, which shares all scheduling, topology and admission settings.

## Completed pilot

All four `_b` cases and immutable inventories pass independent reanalysis. D/C
crossing/confirmation: baseline 62/71s; capture_control 153/162s; neither exclusion
condition reaches the threshold within 180s. Final worst D/C losses are 0.000508365,
0.0466232, 0.170960 and 0.247617 respectively. Dual exclusion produces nine completed
A-excluded D/C gradient outputs and four parameter outputs, on six and three
X-led native groups containing A respectively. This is one-seed evidence of the
complete local mechanism and learning harm, not a reproducible slowdown estimate.
Parameter omission's incremental causal effect remains unestablished.

All clocks and cleanup checks pass; no native workers remain. Independent review
preserves incomplete groups, actual submitted samples and SGD counts, A's final
loss and native state-download routes. See
`results/capture_training_review_20261021_a/REPORT.md` and its inventory. No further
seeds or simulation sweep are authorized by completing this pilot.
