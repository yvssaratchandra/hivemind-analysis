# Native announcement validation

## Learning-impact programme (2026-10-20)

Approved sequence: repair evidence checks (E0); three fresh equal-allocation
four-condition sets, seeds 2–4, and one unequal-bandwidth boundary set, seed 5
(E1); six native ten-round gradient-SGD trajectories, seed 201 (E2); numerical
simulator differential validation (E3); 200-round convergence and recovery
pilots followed by five paired confirmation seeds 202–206 (E4/E5).

| Implementation status | State |
|---|---|
| E0 evidence gate and report direction | Complete: 30 focused tests; original raw suite revalidated in `results/announcement_e0_review_20261020_a` |
| E1 repeatability and allocation boundary | Complete: equal seeds 2–4 and unequal seed 5; all 16 condition checks passed |
| E2 native learning reference | Running six ten-round trajectories; iid baseline and membership complete |
| E3 numerical protocol model | Pending native reference |
| E4/E5 convergence and recovery | Pending differential validation |

E1 local results (2026-10-21): `announcement_e1_equal_s{2,3,4}_20261021_a`
reproduced coordinate errors of 18 for A and 6 for B/D/C in ordering; membership
returned the B/D/C mean and left A unchanged (324 remote fallback elements,
108 own-only elements). `announcement_e1_unequal_s5_20261021_b` reproduced
ordering errors of 12 for A and 4 for B/D/C; membership left A unchanged with
288 fallback elements and 144 own-only elements. Controls were accurate within
5.1e-6. These are identifiable marker values, not loss measurements or deployment
success rates. Every completed set has a raw-file SHA256 inventory.

The unequal `_a` attempt stopped after its successful native baseline because
the checker incorrectly required NODE allocation mode for zero-allocation B.
Native `AllReduceRunner` defaults such peers to CLIENT. The corrected checker
passed a temporary-copy reanalysis; the stopped attempt remains preserved and
the full set uses `_b`. The numerical learning backend remains scoped to equal
allocations; this boundary result does not expand its validation scope.

The native loop computes full-batch gradients for a 48-coefficient noiseless
linear regression, averages them, then applies float32 SGD (learning rate 0.1,
no momentum or parameter synchronization). Each peer has 512 Gaussian examples;
evaluation uses 4,096 independent examples. The distinctive-data arm zeros the
last twelve features outside A. A fallback preserves A's **gradient**, so A
continues taking local SGD updates. Native inputs, returned gradients, models,
rosters, delta traces and shutdown evidence must reconstruct every update.

The numerical workload owns model state; protocol code owns local roster views,
coordinate allocation, reduction and fallback; policies choose recipient-specific
announcements. Existing throughput workloads remain available independently.
Implementation order is evidence checker, native fixtures, training controller,
numerical model, differential checks, then paired learning runs. Native source
under root `hivemind/` remains unchanged.

E4 arms are baseline, random benign omission at 25%/100%, targeted membership
omission at 25%/100%, and targeted ordering corruption at 25%/100%. E5 adds
each targeted attack for rounds 1–100 followed by honest rounds 101–200.
Both data distributions use matched problem and attack-schedule seeds. Random
omission chooses A/D/C uniformly; all 25% arms share affected rounds.

Review gates require valid controls, fully explained native outcomes, numerical
agreement (markers atol 1e-4; gradients/models atol 1e-5, rtol 1e-4), preserved
baseline accounting, and valid pilot evidence. Confirmation is not contingent on
observing harm. Report per-honest-node/mean/worst test loss, exclusive-feature
loss, distance to optimum, disagreement, final and trajectory-average loss,
and first sustained ten-round crossing of 10% initial loss (otherwise censored).
These are rounds/compute comparisons with designated leadership, not deployment
timing or leadership-capture estimates. Stop after E5; no automatic broader sweep.

Private loopback experiment; one seed, four independent conditions, one averaging
operation per active node. No training or public network. Root Hivemind source
is unchanged. Prior Step 17 already demonstrated recipient-specific ordering;
this probe adds targeted A ordering, membership omission, a three-member control,
and embedded origin/coordinate markers.

| Implementation status | State |
|---|---|
| Harness / runtime preflight | Done; preflight confirms native source identity and native 108×4 / 144×3 chunk allocations |
| Independent analyzer / tests | Done; 28 unit tests + 16 subtests pass; analyzer run over raw evidence |
| Four-condition set | Done; `results/announcement_s1_20261020_c`, seed 1, suite `completed`, all four `valid_evidence` |
| Evidence review / commit / push | Done (this change) |

## Results (seed 1, `announcement_s1_20261020_c`)

All four conditions produced valid evidence and shut down cleanly (exit 0, no
surviving processes). Both attacks produced incorrect or degraded outcomes with
no failure returned by the averaging operation. Membership omission emitted
handler and partial-aggregation warnings in the native logs.

| Condition | Outcome |
|---|---|
| baseline4 (control) | Correct four-member mean at every node; all origin/coordinate markers correct. |
| control3 (control) | Correct B,D,C mean; A idle; A absent from every row. |
| ordering (attack) | A was announced `[A,D,B,C]` while B,D,C saw `[B,D,A,C]`. All four completed with `success` and full streams, but **coordinate markers are wrong for all four** and no node matches the true mean (max error 6 at B,D,C; 18 at A); A agrees with nobody. Inconsistent ordering silently corrupts the averaged coordinates across the whole group with no error. |
| membership (attack) | B announced `[B,D,C]` to D,C and aggregated as `[B,D,C]`, but announced `[B,D,A,C]` to A. B,D,C computed the correct three-member mean with **A absent from all 48 rows (clean exclusion)**. A, believing it was in a four-member group, had its chunks rejected by B,D,C's reducers (native `ValueError('<peer id> is not in list')`), banned them, hit `missing_part_fallback` for its 324 remote-reduced elements, and **fell back to its own local value while returning `success`** (its output is 50 off the true mean). Its own 108-element reducer share completed using only its local input. The operation returned success despite warnings and degraded aggregation. |

Clean exclusion is asserted only for B,D,C, which each hold a completed,
numerically supported B,D,C mean with no A origin markers and no fallback. A's
native `success` is recorded as a local fallback, not a clean exclusion; the
fully recorded fallback is valid evidence of a degraded outcome and does not
invalidate the condition (the controls still reject any fallback). These results
describe only these local runs; they do not estimate public-network attack
rates, Internet performance, or model convergence.

## Architecture and implementation

Controller launches isolated native subprocesses for baseline4, control3,
ordering, membership. Each subprocess starts four loopback DHTs and averagers.
A is idle in control3. All averagers are NODE, weight 1, bandwidth unspecified,
NoCompression; native equal allocations. Prefixes and identities are fresh.
Inputs have 48 rows, nine lanes: data, four one-hot origins, four origin-specific
coordinate tags. Chunk144bytes = four rows; 12 chunks per tensor. Four reducers
get3chunks each; three get4each. Tensors are symbolic probes, not model gradients.

B is designated via earlier readiness/expiration; canonical B,D,A,C ordering is
an explicit B-only in-memory assembly fixture. Native follower code is observed,
not patched to accept altered messages. Each announced roster retains its own
correctly aligned metadata. B/D/C use B,D,A,C; A uses A,D,B,C in ordering.
In membership, B's matchmaking initially assembles4 but its local aggregation
uses B,D,C; D/C receive3 while A receives4. This B aggregation override is part
of the adversarial behavior and is not a follower intervention. control3 uses
B,D,C throughout. Native group registration is retained.

Successful B advertisement gates follower startup; independent seeded offsets
0..0.25s avoid identical readiness. B scheduled +10s, followers +15s;
min matchmaking5s, request3s, step45s, sender5s, reducer10s, allreduce30s.
Retries disabled. No artificial network delays; native caching defaults.
Unexpected leaders/members invalidate evidence; failures are preserved, not
silently retried. A condition has a120s outer cap and bounded owned-process
cleanup. Control numerical/evidence failures halt before attacks. Attack
outcomes may fail or be partial without invalidating faithful evidence.

Native raw announcements (protobuf hex), local rosters, allocation ranges,
authenticated sender chunks/results, fallback and terminal tensor states are
saved. Offline analysis imports no Hivemind and requires evidence alignment,
independent means, correct markers and cleanup. Only a complete B/D/C mean
without A's markers or fallback qualifies as clean exclusion. Missing proof
never becomes attack success. Absolute tolerance1e-4, relative0.

## Checks and acceptance

Test roster transformation/metadata pairing, analysis of both controls,
coordinate misplacement, exclusion, fallback, missing evidence and allocations.
Preflight checks native source equality and native chunk allocations. Execute
one set then review all traces. No repeat, simulator refactor or scaling implied.
Freeze script/native hashes and snapshots; preserve raw artifacts locally with
hash inventory. Commit code separately from reports, update handoff and queue,
and push after review.
