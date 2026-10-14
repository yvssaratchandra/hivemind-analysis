# Step 27 control-plane screen

Phase: **pilot**. Trials: **10/10**. Integrity: **True**.

This is a bounded local screening experiment with 0.15-second one-way control RPC delay. The capped latency uses native `step_start` through first relevant native assembly; missing assembly is assigned 12 seconds. Honest group memberships count deduplicated native groups assembled during each submission window.

Per-seed flags are preliminary descriptive checks. They are not significance tests, deployment success rates, or deployment claims. The report separates disband emission, the server-side response event after its delay, and the native client's reply record; server-side response evidence alone does not establish client receipt.

## Interpretation and pooled counts

All ten trials completed. Each used a fresh private eight-peer loopback swarm,
paired seeds 2701/2702, and a 60-second submission window plus bounded drain.
The totals below include groups assembled during drain; window-only memberships
and per-seed comparisons appear separately. X refused outbound joins in every
arm, including the native-ad baseline. It used minimum/target group size four;
honest peers used minimum two and target four. Unreliable X accepted followers,
held them, and disbanded, so zero X-led groups in those arms is intentional.

| Arm | Total groups | X-led groups / X attempts | X led within 2s / attempts | Honest mean capped time to group |
|---|---:|---:|---:|---:|
| Native ads | 49 | 17/18 | 0/18 | 2.023s |
| Reliable single 2s ad | 49 | 13/18 | 9/18 | 2.183s |
| Reliable renewed 2s ad | 51 | 25/25 | 13/25 | 2.110s |
| Unreliable single 2s ad | 43 | 0/9 | 0/9 | 2.619s |
| Unreliable renewed 2s ad | 43 | 0/10 | 0/10 | 3.518s |

Honest means above weight every submitted honest attempt equally across the two
seeds; missing assembly receives 12 seconds. Counts were independently rebuilt
from raw native assembly events and exact member step IDs: **235 groups, 55
X-led groups**. An attempt is not the same denominator as all network groups.

Reliable renewal's eventual 25/25 versus single advertising's 13/18 is an
observed difference worth checking with more exposure. It does not yet establish
reliable renewal-specific advantage: the early-lead contrast was +13.1 and -2.1
percentage points across seeds, and one single-ad control submitted only seven
X attempts (the screen required eight). Renewed short ads versus native ads
passed the separate early-lead policy screen (+41.7/+61.5 points), but that
comparison changes both ad lifetime and renewal. A two-second threshold also
favors shorter recruitment deadlines; it measures speed, not eventual election
probability.

Renewal alone slightly reduced honest capped delay (-0.077/-0.069 seconds).
For unreliable X, the renewal-added delay, adjusted for reliable renewal, was
**0.806/1.128 seconds**:

`D = (L_unreliable,renewed - L_unreliable,single) - (L_reliable,renewed - L_reliable,single)`.

Unreliable renewal yielded **49/58 exact retry witnesses**: honest peers selected
X again during the same still-open search, with the old `(PeerID, expiration)`
remembered and a newly published greater expiration eligible. Directional delay
increases appeared in both seeds. The predefined exploratory harm flag required
at least one second in both seeds and did not pass. A 60-second screen does not
establish sustained harm. No longer confirmation stage has run.

The one-way application delay is `d = 0.15s`, placed before each remote DHT
find/store handler and response, and before join handling and each streamed
reply. Unreliable hold `h = 0.5s` starts after forwarding ACCEPTED. Thus the ideal
server-side request-to-disband-emission span is `h + 3d = 0.95s`, plus processing;
native timeouts may truncate it. This emulates selected control delays, not a
full WAN. Requested renewal cadence is two writes/sec; serialized RPC work can
reduce achieved cadence. Cached reads, pings, and tensor transfer are unchanged.
Measured achieved active renewal frequency was **1.082–1.084 writes/sec**, with
median publication spacing **0.922–0.924s**; these trials do not represent an
achieved two-writes/sec schedule or a guaranteed remaining-lifetime band.

Tensor-transfer warnings do not filter these control-plane counts. Native
post-assembly processing can still affect when subsequent searches start, so
throughput includes that scheduling effect. These measurements establish
assembly and retry behavior, not tensor correctness or model-training harm.

**Auxiliary receipt correction:** the frozen analyzer's per-hold
`disband_received_by_native_client` linkage compares the selected leader's
expiration to the follower's request expiration. Native matchmaking sends the
follower's own expiration, so that equality is invalid and undercounts receipts.
Do not interpret the original receipt count or false per-hold flags as evidence
of missing delivery. The separate read-only
`research/hivemind/audit_step27_receipts.py` matches the actual request expiration,
caller/leader, timing, and ACCEPTED/DISBANDED on the same native client join ID.
Its event-referenced `receipt_audit.json` finds **179/182** emitted disbands with
matched receipts: single arms **13/13 and 15/15**, renewed arms **67/69 and
84/85**. The three unmatched events remain unconfirmed, not proved undelivered.
Frozen source, original exports, and primary assembly/retry/delay metrics remain
unchanged. Correct this auxiliary join in a distinct future analyzer version.

## Trial metrics

| Case | Seed | X led / submitted | X led by 2s | Honest capped latency | Honest memberships/minute | Groups | Retries | Holds | Integrity |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| unreliable_single | 2701 | 0/4 | 0/4 | 2.717093080552448 | 10.714285714285714 | 21 | 0 | 13 | True |
| unreliable_single | 2702 | 0/5 | 0/5 | 2.5287147805526997 | 11.714285714285714 | 22 | 0 | 15 | True |
| reliable_renewed | 2701 | 12/12 | 5/12 | 2.106280774286555 | 12.0 | 25 | 9 | 0 | True |
| reliable_renewed | 2702 | 13/13 | 8/13 | 2.113329965922215 | 12.857142857142858 | 26 | 7 | 0 | True |
| reliable_single | 2702 | 10/11 | 7/11 | 2.1823179090534213 | 12.285714285714286 | 26 | 0 | 0 | True |
| unreliable_renewed | 2702 | 0/5 | 0/5 | 3.587426923097949 | 9.714285714285714 | 22 | 58 | 85 | True |
| reliable_single | 2701 | 3/7 | 2/7 | 2.1828046888154837 | 12.142857142857142 | 23 | 0 | 0 | True |
| native_baseline | 2702 | 9/9 | 0/9 | 2.050082074477056 | 12.428571428571429 | 25 | 0 | 0 | True |
| unreliable_renewed | 2701 | 0/5 | 0/5 | 3.4467046227397025 | 9.428571428571429 | 21 | 49 | 69 | True |
| native_baseline | 2701 | 8/9 | 0/9 | 1.9952401164432145 | 12.0 | 24 | 0 | 0 | True |

## Paired-seed screening flags

Suite flags: `{"leadership_gain": false, "native_policy_leadership_gain": true, "renewal_alone_harm": false, "renewal_amplified_harm": false}`. Status: `{"leadership_gain": "insufficient observations", "native_policy_leadership_gain": "screen flag", "renewal_alone_harm": "no screen flag", "renewal_amplified_harm": "no screen flag"}`.

| Seed | Δ X-led-by-2s rate, renewed vs once | Δ X-led-by-2s rate, renewed vs native | Renewal-alone Δ honest capped latency | Harm difference-in-differences | Flags |
|---:|---:|---:|---:|---:|---|
| 2701 | 0.130952380952381 | 0.4166666666666667 | -0.07652391452892893 | 0.8061354567161834 | renewal_leadership_gain_vs_native, native_policy_leadership_gain, eventual_drop_within_0p10_vs_reliable_single, eventual_drop_within_0p10_vs_native, eventual_drop_within_0p10, native_x_attempt_floor, honest_attempt_floor, unreliable_renewal_retry_observed |
| 2702 | -0.020979020979020935 | 0.6153846153846154 | -0.06898794313120638 | 1.1277000856764556 | renewal_leadership_gain_vs_native, native_policy_leadership_gain, eventual_drop_within_0p10_vs_reliable_single, eventual_drop_within_0p10_vs_native, eventual_drop_within_0p10, x_attempt_floor, native_x_attempt_floor, harm_did_at_least_1s, renewal_amplified_harm, honest_attempt_floor, unreliable_renewal_retry_observed |

## Integrity and coverage

Prerequisite smoke gate: **passed**, two of two trials in
`../step27_smoke_20261012_a`. The pilot's `smoke_gate_pass: null` summary field
is not an additional pilot gate result; the smoke summary records the pass and
the frozen pilot `suite.json` records the prerequisite suite path.

Delayed correlated find/store/join RPC counts: `{"find": 25756, "join": 2174, "store": 5133}`.

All detailed rows are event-referenced in the CSV exports. `selection_observed_x_expiration` comes from the selection queue snapshot; `latest_published_x_expiration` comes from X's latest active publication event. Queue alternatives are observed in the local native queue and do not establish global DHT availability.

Final independent verification passed, including completion/provenance hashes,
raw group/member-attempt linkage, retry references, and all 1,595 preserved
prior source/raw file hashes. The frozen harness remains unchanged. Offline
resume validation reports ten completed and zero pending/interrupted/blocked
jobs. Every trial records peer shutdown, and the host process check found no
remaining controller or runtime peer daemons. Proofs are
`../step27_final_resume_validation_20261012_a.json` and
`../step27_shutdown_20261012_a.json`. It is safe to suspend.
