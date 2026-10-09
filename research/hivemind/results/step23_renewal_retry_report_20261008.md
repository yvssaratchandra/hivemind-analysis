# Step 23: own-ad renewal and within-search retries

Private local experiments. Core `hivemind/` is unchanged. This experiment adds an
explicit unreliable-leader behavior; renewal alone is not treated as harmful.

## Implementation status

| Work | Status |
| --- | --- |
| Isolated runtime and source checks | Complete: Step 22 dependency versions, pinned daemon hash, eleven runtime source hashes |
| Instrumentation and independent analyzer | Complete; seventeen focused evidence checks pass |
| Mechanism gate | Complete: five paired trials demonstrate retries after advancing expiration |
| Four-condition smoke | Complete: all sixteen trials pass trace and tensor checks |
| Confirmation and conditional diagnostic | Confirmation halted after 15/160 trials on the wrong-tensor guard; diagnostic gated off |
| Final raw-data analysis | Complete for mechanism and smoke; confirmation intentionally unanalyzed as a harm suite |

## Question and controls

Does publishing a newer expiration let one unreliable peer repeatedly attract the
same honest peer within a single matchmaking search, and does frequent renewal
cause additional delay or failed averaging compared with expiration-only renewal?

The protocol remembers attempted `(PeerID, expiration)` pairs. This set is reset
between searches. Instrumentation assigns separate step, search, and attempt IDs,
and records the selected **leader advertisement expiration** separately from the
follower's **request expiration**. Honest code performs native discovery and retries;
the experiment does not inject candidates or force honest reads.

The mechanism test uses X, one honest averager, and a third DHT observer. X rejects
joins immediately. Its initial advertisement lasts two seconds. An observer seeing
that record gates the honest step's launch. After the first join, X waits 0.25 seconds
and either repeats the same expiration or publishes a newer one with the same
two-second remaining lifetime. Both steps and searches are bounded.

The comparison has four arms: reliable/unreliable X × expiry-only/one-write-per-second
publication. Every arm uses two-second own-ad lifetimes. Unreliable X retains native
admission checks and follower bookkeeping, but deliberately suppresses assembly,
accepts eligible followers, waits 0.5 seconds, then sends `GROUP_DISBANDED`.
Reliable X delegates to the native join handler. X never follows or reads competitors.

Eight ordinary loopback DHT positions, five default replicas, target four, honest
minimum two, X minimum four, native honest five-second matchmaking setting,
three-second request timeout, twelve-second search timeout, six-second allreduce
timeout. Each peer submits exactly one step with fixed 128-element input tensors
and equal weights. Empty group bits keep the aggregation key fixed. Independent
staggered and bursty arrival schedules reuse Step 22's schedule generator; X's start
is independent and not informed by honest schedules. Schedules and DHT positions are
paired, but fresh PeerIDs and runtime execution are not deterministic.

## Measurements and decision rule

The analyzer checks process event sequences, closed traces, complete searches and
streams, selected expirations against preceding native DHT views, original gathered
inputs, matching contributor rosters, every returned tensor, and follower cleanup.
Incorrect tensors count as failures and pause interpretation for diagnosis.

Report within-search attempts/retries, waiting on X, verified honest completions,
advertisement write counts, and a penalized time to correct output. Failed/incorrect
steps receive eighteen seconds in that last measure; actual waiting and failure
counts are also reported so this penalty is not mistaken for elapsed waiting time.

The primary contrast is unreliable frequent renewal minus unreliable expiry-only.
The interaction subtracts the same publication contrast among reliable controls.
Average the two arrival patterns within each seed, then bootstrap whole seeds
10,000 times with analysis seed 230023 for 95% intervals.

The prespecified local-harm criterion requires observed renewal-induced retries,
both primary and interaction intervals excluding zero in the harmful direction,
and at least 0.5 additional penalized seconds per honest peer or 0.5 fewer verified
honest completions per trial in the primary comparison. Do not select new frequencies
or hold times after inspecting confirmation outcomes.

If this criterion passes, the diagnostic compares native attempt exclusion against
remembering attempted PeerIDs regardless of expiration, within one search. That
changes honest behavior only in a clearly labeled research wrapper. It is an
exploratory explanation test, not a measured production defence.

## Evidence so far

The initial [sandbox setup attempt](step23_mechanism_20261008_a/) stopped before
starting a swarm because Torch's shared-memory helper could not run in that sandbox.
The failed trial and provenance are preserved; it contributes no experimental result.

The final [mechanism suite](step23_mechanism_20261008_b/) uses seeds 2301–2305.
Its [independent analysis](step23_mechanism_20261008_b/analysis.json) verifies all ten
trials. Every newer-expiration arm produced exactly one additional attempt in the
same search before the initial expiration; none of the unchanged-expiration controls
retried. There are intentionally no other honest averaging partners in this gate,
so its failed averaging steps are not evidence of harm.

Smoke uses seeds 2306–2307. Confirmation uses 2311–2330. The conditional diagnostic
uses 2351–2356. These are distinct seed sets; smoke outcomes do not tune confirmation.

The [smoke suite](step23_smoke_20261008_a/analysis.json) completes all sixteen trials
with no incorrect returned tensors. Across its four trials per arm, reliable
expiry-only and reliable refresh each verify 28/28 honest steps. Unreliable
expiry-only verifies 25/28, with 23 within-search retries; unreliable refresh
verifies 24/28, with 50 retries. The primary penalized-time difference is 0.487
seconds per honest peer with a two-seed interval spanning zero. This is a pilot,
not a confirmation of the harm threshold. No parameters were tuned from it.

## Why confirmation halted

The held-out confirmation wrote fifteen checked trials, then stopped at
[`reliable_expire_native_bursty_s2322`](step23_confirm_20261008_a/reliable_expire_native_bursty_s2322/).
This is a reliable-leader control. H0's gathered contributors require output 1.75,
but every output element was 0. Three distinct groups assembled with overlapping
rosters: the first was `{H4,H0,H2,H1}`, the second `{H6,X,H3,H1}`, and the third
`{H2,H4,H5}`. The trace does not establish why these groups overlapped or why H0
returned zero. The correctness guard stopped the run before interpreting harm.
The [stop record](step23_confirm_20261008_a/halt.json) preserves the trigger and
the known roster overlap. The partial confirmation suite has no harm analysis and
cannot pass the confirmation gate. Do not run its dependent PeerID-filter diagnostic
until the overlap and tensor error are diagnosed and a new clean confirmation is run.

## Reproduction

The runner needs a functioning Hivemind environment whose eleven checked source
files match this repository, generated protobuf bindings, and the pinned p2pd binary.
The execution here uses `/tmp/hm-step23-env` and a source copy under
`/tmp/hm-step23-runtime`; neither path is a portable prerequisite. Runtime versions
and source hashes are in each suite's `provenance.json`.

```sh
PYTHONPATH=/tmp/hm-step23-runtime /tmp/hm-step23-env/bin/python -B \
  research/hivemind/step23_renewal_retry.py --phase mechanism --label NEW_LABEL

PYTHONPATH=/tmp/hm-step23-runtime /tmp/hm-step23-env/bin/python -B \
  research/hivemind/step23_renewal_retry.py --phase smoke --label NEW_LABEL \
  --gate research/hivemind/results/step23_mechanism_20261008_b/analysis.json

python3 -B research/hivemind/analyze_step23.py \
  research/hivemind/results/step23_mechanism_20261008_b \
  --output /tmp/step23-rechecked.json

python3 -B -m unittest discover -s research/hivemind -p test_step23.py -v
```

Confirmation requires a passing smoke `analysis.json`; diagnostic requires a passing
confirmation harm gate. A correctness failure stops its suite and must be diagnosed
before confirmation restarts with a new label. Every phase requires a new label and refuses an existing
output directory. Each trial preserves config, raw events, summary and independent
checks; each suite preserves job order, gate evidence, packages and harness snapshots.

## Limits

These are bounded standalone-averager tests with simulated arrivals, not real model
training, Optimizer-driven scheduling, WAN-delay emulation, or a public swarm.
Changing renewal cadence changes the evolution of both availability and deadline
priority. The factorial comparison estimates this publication policy's effect;
the optional retry guard helps investigate its explanation. Neither alone proves
a universal denial attack or an optimal advertising policy. The five earlier Step 22
continuous-workload tensor errors remain unresolved by choosing separate rounds.
