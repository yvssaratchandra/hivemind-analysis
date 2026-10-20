# Native announcement validation

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
no error surfaced to the participants.

| Condition | Outcome |
|---|---|
| baseline4 (control) | Correct four-member mean at every node; all origin/coordinate markers correct. |
| control3 (control) | Correct B,D,C mean; A idle; A absent from every row. |
| ordering (attack) | A was announced `[A,D,B,C]` while B,D,C saw `[B,D,A,C]`. All four completed with `success` and full streams, but **coordinate markers are wrong for all four** and no node matches the true mean (max error 6 at B,D,C; 18 at A); A agrees with nobody. Inconsistent ordering silently corrupts the averaged coordinates across the whole group with no error. |
| membership (attack) | B announced `[B,D,C]` to D,C and aggregated as `[B,D,C]`, but announced `[B,D,A,C]` to A. B,D,C computed the correct three-member mean with **A absent from all 48 rows (clean exclusion)**. A, believing it was in a four-member group, had its chunks rejected by B,D,C's reducers (native `ValueError('<peer id> is not in list')`), banned them, hit `missing_part_fallback` for its whole share, and **fell back to its own local value while returning `success`** (its output is 50 off the true mean). The excluded victim is silently isolated, not errored. |

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
