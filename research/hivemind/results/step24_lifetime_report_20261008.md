# Step 24: lifetime ranges with equal training delays

Private eight-peer loopback sweep. This experiment does not inject network latency,
run an Optimizer, or interact with a public swarm. Core `hivemind/` is unchanged.

## Implementation status

| Work | Status |
|---|---|
| Research harness, native timing, and reducer traces | Implemented; six focused checks pass |
| Independent raw-evidence analyzer | Implemented; ten focused evidence checks pass |
| Eight-arm, ten-second smoke | Halted on trial 2/8: incorrect tensor |
| Five-seed, sixty-second comparison | Not run; failed smoke gate |
| Bounded diagnostic replays | One passes; second captures failed reducer stream and incorrect completion |
| Raw count recalculation and handoff | Complete for partial smoke and diagnostic evidence |

## Delay and matchmaking formulas

After observing peer i's averaging attempt k at FINISHED, the harness schedules its
next attempt with `ready = C[i,k] + D[i,k]`. Actual submission occurs at
`S[i,k+1] = C[i,k] + D[i,k] + epsilon[i,k]`, where epsilon is measured scheduling
lateness, including the polling loop and time spent submitting other peers.

For every peer, including X, independently:

```
sigma = sqrt(log(1 + 0.6**2))
mu = log(1.5) - sigma**2 / 2
D[i,k] = clip(exp(mu + sigma*Z[i,k]), 0.1, 4.0), Z ~ Normal(0,1)
```

Before clipping, mean is 1.5 seconds and coefficient of variation is 0.6. These are
simulated training delays, not measured model-training times. Per-peer RNGs use
`Random(seed*100 + peer_index)`, with X indexed zero and H0 through H6 indexed one
through seven. Policies reuse each peer's delay sequence and initial staggered
arrival offsets. Completion times can diverge across policies; arrivals are not
fixed to an identical absolute schedule. No delay is added inside the DHT or RPC.

For periodic publication, U is upper lifetime, Delta = 1 second, p[n] is send time,
and the native step search deadline caps the absolute expiration:

```
E[n] = min(search_end, max(nextafter(E[n-1], +infinity), p[n] + U))
R(t) = E[n] - t
Nominal R(t) in (U - Delta, U], away from search-end capping
Continuity margin: U > Delta + remote_write_latency + scheduling_jitter
```

Slow stores serialize writes and may lower achieved frequency. Nominal ranges are
not promises about remaining lifetime at remote readers. The (0,1] arm has no
positive overlap margin and intentionally tests gaps. Capture write start/finish,
remote accepted stores, and ad views; report holder intervals separately from
actual native DHT observations. Do not introduce observer reads that change discovery.

The native leader join handler computes `W = max(0, E_X(a) - a)` when it starts
waiting after admitting a follower at a. The trace captures the exact timeout
argument to `asyncio.wait` and the then-current declared expiration, without
changing either futures or timeout. Later renewal does not extend that timer.
With X's target and minimum both four, it needs three simultaneously accepted
followers before a captured wait expires and disbands the undersized group.
Training delays shift the times at which peers begin searching; native discovery,
selection, RPC latency, and admission determine when they actually join X.
No theoretical leadership probability is assumed.

## Policies and controls

| Policy | Nominal remaining lifetime | Publication |
|---|---|---|
| range_0_1 | (0,1] s | 1 Hz |
| range_025_125 | (0.25,1.25] s | 1 Hz |
| range_1_2 | (1,2] s | 1 Hz |
| range_2_3 | (2,3] s | 1 Hz |
| range_3_4 | (3,4] s | 1 Hz |
| expire_2 | Up to 2 s | After expiration |
| passive_stock | Native 5-second publisher | X does not follow |
| stock | Native publisher and following | X may follow |

Seven honest peers use native admission, 5-second matchmaking lifetime, minimum two,
and target four. X uses minimum and target four in all arms. All non-stock X arms
refuse outbound joins and do not read competitor ads through matchmaking.
Empty group bits keep repeated steps under the same key. Native default scheduled
start, 12-second search timeout, 6-second allreduce timeout, and bounded 20-second
drain allowance are retained. Failed attempts can retry natively within a step.

Smoke: seed 2400, ten-second submission windows, all eight arms.
Confirmation: seeds 2401-2405, sixty-second submission windows, all eight arms.
Arm order is shuffled with seed 240024. Fresh networks are used per trial; peer IDs
and operating-system scheduling are logged but not deterministic from the seed.
Counts include draining after the submission window; timing durations are explicit.

## Evidence and gates

Save per-trial configuration, JSONL events, outcomes, independently reconstructed
analysis, and source/package provenance with matching harness snapshots. Never
reuse an output label. Verify each group against actual step inputs, identities,
rosters, and every member's returned tensor. Keep assembled counts separate from
verified completed counts. An incorrect tensor or conflicting group membership
halts the suite; preserve failures and diagnose before rerunning.

The final table will contain total assembled groups, X-led assembled groups, total
verified groups, X-led verified groups, and verified X leadership share, per seed
and pooled. Also report mean X-led size, writes, stream disband replies, measured
training/scheduler delays, native wait windows, ad observations, and holder gaps.
Failures are reported explicitly rather than counted as successful averaging.

## Results: partial smoke, not a range ranking

[Preserved smoke suite](step24_smoke_20261008_a/) and [log](step24_smoke_20261008_a.log).
Both trials use seed 2400. Randomized execution reached only two of the eight arms.
Counts include draining; the second trial stopped submitting before its planned
ten-second window ended. The remaining six policies, including (1,2], were not run.

| Nominal range | Status | Total assembled | X-led assembled | Total verified | X-led verified | Verified X share | Mean verified X-led size | X active write attempts / accepted |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| (0,1] s | First smoke trial completed | 10 | 5 | 10 | 5 | 50% | 4 | 18 / 18 |
| (2,3] s | Second smoke trial aborted | 4 | 1 | 3 | 0 | 0/3 | — | 3 / 2 |

The (0,1] trial recorded eleven X stream disband replies and one failed final X
attempt during drain. These are distinct from incorrectly completed averaging.
The aborted trial recorded one incorrect completed step and five pending steps
that were cancelled on shutdown. These rows have unequal effective exposure and
are not evidence that the first range beats the second.

### Failure diagnosis

In `range_2_3_s2400`, X assembled group
`75b6aa0c2a90f2b124f75eb6d849703c5af1ebe5` at 1.539 seconds with
`X:1=17`, `H0:1=10`, `H1:1=11`, and `H2:1=12`. Its expected mean was **12.5**.
X logged sending BEGIN_ALLREDUCE to all three followers. X, H0, and H1 entered
the group's allreduce with exactly the expected input tensors; H2 did not enter
that collective. At 2.271 seconds H2 assembled a different group using the same
`H2:1` step, then correctly averaged with H4, H5, and H6.

At about 7.595 seconds, X and H1's allreduces ended with CancelledError near the
six-second limit. H0's native allreduce call returned without an exception, with
its tensor still **10.0**, and its StepControl reported completion. The harness
observed that same incorrect tensor under the tensor lock and halted submissions.
The incorrect result is established by both child and parent traces; it is not
an analyzer count error. All entered collective inputs matched logged step values.
The trace does not establish why H2 failed to enter X's collective or why H0's
native call returned successfully. No native behavior was repaired or bypassed.

The distinction is between overlapping *candidate rosters* and simultaneous
collectives: H2 appears in X's candidate roster and its own roster, but the trace
shows H2 entered only its own collective. Sequential failed native retries are
allowed by the analyzer and do not automatically count as overlap or success.

Added research-only client response/exception tracing forwards the original
`asyncio.wait_for` awaitable, timeout, result, and exception unchanged. A fresh
bounded [diagnostic replay](step24_diagnostic_20261008_a/) of the same policy and
seed, with new peer identities and runtime scheduling, produced **10 assembled and
verified groups, five led by X**, all X-led groups of size four. It used 16 accepted
active ad writes, recorded two X disband replies, and one failed final X attempt.
H2 received ACCEPTED and BEGIN_ALLREDUCE from X in that replay. This clean replay
does not explain or invalidate the original failing trace and is not a replacement
for the full smoke sweep. Confirmation remains gated off.

### Second diagnostic: failed reducer stream and zero update

A second fresh [diagnostic replay](step24_diagnostic_20261008_b/), with unchanged
native behavior plus per-peer reducer-exchange tracing, halted with another
incorrect completed step. This was a different failure occurrence; it does not
prove the first smoke's missing-H2 cause.

X assembled group `930e3ade6ca69a8f263a67247a8b1c1bd06835b6` at 4.575 seconds,
with `X:3=37`, `H6:2=26`, `H0:2=20`, and `H1:3=31`; expected mean **28.5**.
All four peers received/entered that group and started with the correct tensors.
X, H0, and H1 produced 28.5. H6's reducer exchange with X raised
`AllreduceException: ... sent 0 parts, but we expected 1`; after native failure
handling, H6's tensor container recorded **128 failed elements**. H6's native
allreduce call then returned without an exception, with output still **26.0**.
The parent independently observed that same output and stopped submissions.

This trace establishes a failed reducer stream followed by native fallback and
successful-but-incorrect completion relative to the full roster mean. The code
explicitly handles failed reducers by registering zero deltas in
`TensorPartContainer.register_failed_reducer` (`partition.py:128-135`), and
`AllReduceRunner.run` logs/suppresses exceptions while awaiting communication tasks
in its cleanup (`allreduce.py:183-191`). A focused native-container test confirms
that failing the reducer assigned all 128 elements produces a zero update. This
is native fallback behavior, not an ad-policy repair. Why the stream delivered
zero parts remains unresolved; no claim is made that renewal caused that stream
failure. The all-participant verification criterion therefore remains necessary.

| Replay | Status | Total assembled | X-led assembled | Total verified | X-led verified | Incorrect completed steps |
|---|---|---:|---:|---:|---:|---:|
| diagnostic_a, (2,3] | Completed ten-second window and drain | 10 | 5 | 10 | 5 | 0 |
| diagnostic_b, (2,3] | Aborted at about 4.64 seconds | 4 | 3 | 3 | 2 | 1 |

The latter used four accepted active X writes and recorded no X disband replies.
These diagnostic rows have different exposure and are not repeatability estimates
or policy comparisons. Inspect the [failure timeline](step24_reanalysis_20261008_b/reducer_failure_timeline.json)
alongside the raw events for source attribution.

### Measured timing and validation

All saved training draws match the configured per-peer seeded formula. In the
completed (0,1] smoke trial, mean simulated delay was **1.483 seconds**, and mean
observed scheduler epsilon across subsequent submissions was **4.223 ms**. In the
clean diagnostic replay these were **1.483 seconds** and **3.998 ms** respectively.
These are samples, not exact distribution means.

Captured X leader wait arguments ranged from **0.115 to 0.987 seconds** in the
(0,1] smoke and **2.095 to 2.991 seconds** in the (2,3] replay. These measurements
illustrate how the expiration policy changes waiting time; they do not determine
the probability of correct leadership. Remote holder gaps also include intentional
withdrawals and training intervals and are labelled receipt diagnostics rather
than proof of contention-time DHT availability.

Independent [final reanalysis and inspectable count CSV](step24_reanalysis_20261008_b/)
preserve the current checker and harness snapshots. Recalculation reproduces all
counts above. Each original run retains its own matching execution snapshot and
runtime provenance. Six focused harness tests and ten evidence tests pass,
including unchanged wait semantics, seeded delay checks, input tampering, missing
trace closure, incomplete gates, repeated rosters, failed sequential retries, and
overlapping collectives, native reducer fallback, and unchanged exception propagation.
[Saved test output](step24_checks_20261008.txt). Syntax checks and `git diff --check` pass. CLI checks reject
both the incomplete smoke and the clean diagnostic as confirmation gates, without
starting a network or creating a confirmation output directory.

## Run commands and next step

Use a fresh label for every execution. The local runtime requires PyTorch shared
memory and loopback sockets; the restricted sandbox blocked shared memory, while
the authorized outside-sandbox preflight initialized all eight private DHT peers.

```bash
PYTHONPATH=/tmp/hm-step23-runtime /tmp/hm-step23-env/bin/python \
  research/hivemind/step24_lifetime_sweep.py --phase smoke --label NEW_LABEL

PYTHONPATH=/tmp/hm-step23-runtime /tmp/hm-step23-env/bin/python \
  research/hivemind/step24_lifetime_sweep.py --phase confirm --label NEW_LABEL \
  --gate research/hivemind/results/COMPLETE_PASSING_SMOKE_DIRECTORY
```

Next: resolve missing follower/response delivery and determine how to handle native
fallback under the approved full-roster correctness criterion, then run a new full
smoke before confirmation. The harness supports a bounded diagnostic replay with
`--phase diagnostic --diagnose FAILED_TRIAL_DIRECTORY` and extra client/reducer traces. No lifetime
range, including (1,2], is established as best by these partial results.
