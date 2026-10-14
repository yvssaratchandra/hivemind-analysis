# Hivemind own-ad renewal — speaker notes

Four slides · 7 October 2026 · Sarat Yellapragada, Secura Lab

## 1. Leadership through own-ad renewal

Step 22 tests a different policy from adaptive undercutting. X does not read competitor advertisements through the matchmaking API and does not forge another peer's records. Its publisher uses a short remaining lifetime d, renews at requested frequency f, and waits for inbound followers. The harness retains the stock admission checks and allreduce.

Hivemind stores availability under the group key and a PeerID subkey. The expiration serves both as the record lifetime and as leader priority. Peers prefer an earlier (expiration, PeerID-bytes) tuple, while a holder only replaces an existing record when the incoming expiration is strictly greater. The publisher therefore uses E_new = max(nextafter(E_old, +infinity), now + d), capped at the current search deadline. It never declares a remaining lifetime above five seconds. A renewal normally advances E by approximately one send period; it does not keep priority fixed. Requested frequency can exceed achieved frequency when stores serialize.

For overlapping holder visibility, d > 1/f + write latency + jitter margin is a useful condition, not a leadership guarantee. DHT positions are ordinary seeded positions, not crafted identities. Sources: [publisher](../../step22_refresh_leadership.py), repository `hivemind/averaging/key_manager.py`, `hivemind/utils/timed_storage.py`, and `hivemind/averaging/matchmaking.py`. The diagram shows logical decisions, not measured packet timing.

## 2. Eight peers, controls, repeated rounds

All trials are private eight-peer loopback experiments with standalone DecentralizedAverager instances and 128-element tensors. They are not full Optimizer training. There is no explicit scheduled_time. Honest peers have the default five-second matchmaking window, a two-member minimum, and a three-second join-request timeout. The target is four. Each step has a twelve-second matchmaking limit and a six-second allreduce limit. Empty group bits keep repeated steps under the same group key. Core Hivemind source is unchanged.

The grouping wave uses two seeds (2202, 2203) with staggered and bursty arrivals: four scenarios per policy, fresh networks per trial. X starts independently over three seconds. In this sweep the stock control has a two-member minimum and may follow; the passive control refuses outbound joins and has minimum four. All 56 final trials returned correct tensors; one honest step did not find a group. The 0.5/s, d=2.085764 s policy led 4/4 scenarios using five active ad writes; both controls led 3/4. These are small counts, not deployment success rates.

Continuous trials use held-out seeds 2211–2213. New submissions last thirty seconds and already submitted steps then drain. X has minimum four in every continuous arm, including stock. Honest re-entry delays are lognormal with mean 1.5 s, coefficient of variation 0.6, clipped to 0.1–4 s. X returns after 0.05 s in every arm. That fast re-entry gives X more opportunities and prevents interpreting these counts as an ad-only advantage over equally paced peers. The output check matches contributor identities, step numbers, input values, and every member's tensor; a successful future is insufficient. See the [full methods](../../results/step22_refresh_report_20261007.md) and exact tables in the notes appendix.

## 3. Short ads lead often; writes add cost

The table selects the controls, expiration-only alternative, and the lowest and highest tested refresh frequencies. The full seven-policy table is in the appendix. Numerators are verified X-led groups; denominators are all verified groups in that policy's three runs. These are accumulated observations including drain after the thirty-second submission windows, not requests per second or estimated long-run probabilities.

The 0.5/s policy produced 104 verified X-led groups with 117 active ad writes; 4/s produced 107 with 393 writes, a 3.36-fold write count. Three extra groups across three seeds do not establish a repeatable gain. Frequency and lifetime both change, so this is not a causal estimate of frequency alone. Expiration-only 1.25-second ads produced 106 verified X-led groups with 143 writes; periodic overlap is therefore not necessary for high observed leadership counts in this workload. Its purpose is record continuity, a distinct property.

Five completed steps returned incorrect tensors: one in 0.5/s, three in 1/s, and one in 2/s. Groups containing these steps were excluded from the verified-group counts. The traces establish the errors but do not establish their cause or a causal link to advertisement policy. No forbidden X competitor reads or outbound joins occurred. Writes count high-level active ad attempts, excluding withdrawals, DHT routing walks and other traffic; bytes were not measured. Evidence: [continuous raw traces](../../results/step22_continuous_20261007_a), [analysis](../../results/step22_continuous_analysis_20261007.json).

## 4. A visible ad can still lose its group

The timeline is schematic. In rpc_join_group, after ACCEPTED, the native leader handler computes a wait timeout from its current declared expiration once. Later publisher renewals do not replace that already captured timer. When the earlier timer fires, the leader can assemble if its native minimum is met; otherwise it can disband an undersized group despite a renewed DHT advertisement. Setting target at least minimum is necessary because the target-size assembly path otherwise bypasses the minimum-size timeout check.

The continuous 4/s policy emitted 300 GROUP_DISBANDED replies, versus nine at 0.5/s. These are replies on join streams, not counts of unique groups. The comparison also varies remaining lifetime and reflects this local workload; faster writes did not reliably cure assembly churn.

Persistence is a separate record-visibility test: fixed 40 ms remote rpc_store processing delay, eight DHT peers, independent latest=True probes every 0.1 s, thirty seconds of own-ad writes, no matchmaking. Safe 0.5/s renewal was active at 292/292 probes; safe 2/s at 295/295. A 2/s control with d=0.2 s was active at only 80/295 probes despite 60/60 accepted writes. This is storage-delay emulation, not WAN-delay emulation or proof of indefinite availability. Interior holder intervals exclude local self-stores, initial propagation and final expiration.

A separate capability check with native minimum five and target five formed nine verified X-led five-member groups on one seed during a twelve-second submission window plus drain; 36/37 honest steps completed correctly. It establishes feasibility in that setup, not assured assembly. Next tests should restore full Optimizer-driven epoch scheduling and then add routed delays under controlled conditions. Longer-run behavior, WAN loss/churn, and the five incorrect-tensor steps remain unresolved. Source: repository `hivemind/averaging/matchmaking.py` around rpc_join_group; [persistence traces](../../results/step22_endurance_20261007_a), [five-member traces](../../results/step22_five_members_20261007_a).

## Appendix — exact experiment tables

These tables supplement the four slides; they are not additional slides.

### Grouping sweep: 56 trials

Two seeds × two arrival patterns per policy; fresh networks per trial.

| Policy | f (/s) | d (s) | X-led / trials | Honest completed / steps | Active ad writes |
| --- | ---: | ---: | ---: | ---: | ---: |
| f0p5_m0p05 | 0.5 | 2.085764 | 4/4 | 28/28 | 5 |
| f0p5_m0p5 | 0.5 | 2.535764 | 4/4 | 28/28 | 5 |
| expire125 | At expiration | 1.25 | 4/4 | 28/28 | 7 |
| f1_d125 | 1 | 1.25 | 4/4 | 28/28 | 9 |
| f1_m0p5 | 1 | 1.535764 | 4/4 | 28/28 | 9 |
| f1_m0p05 | 1 | 1.085764 | 4/4 | 28/28 | 10 |
| f2_d125 | 2 | 1.25 | 4/4 | 28/28 | 14 |
| f2_m0p5 | 2 | 1.035764 | 4/4 | 28/28 | 14 |
| f2_m0p05 | 2 | 0.585764 | 4/4 | 28/28 | 15 |
| f4_d125 | 4 | 1.25 | 4/4 | 28/28 | 27 |
| f4_m0p05 | 4 | 0.335764 | 4/4 | 28/28 | 28 |
| stock | At expiration | 5 | 3/4 | 28/28 | 4 |
| passive5 | At expiration | 5 | 3/4 | 28/28 | 5 |
| f4_m0p5 | 4 | 0.785764 | 3/4 | 27/28 | 63 |

### Continuous workload: 21 trials

Three seeds per policy; thirty-second submission windows plus drain. X re-enters after 0.05 s in every arm.

| Policy | f (/s) | d (s) | Verified X-led / all groups | Verified honest / attempted | Active ad writes | Incorrect steps | Disband replies |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| expire125 | At expiration | 1.25 | 106/108 | 326/329 | 143 | 0 | 42 |
| f0p5_m0p05 | 0.5 | 2.085764 | 104/108 | 328/330 | 117 | 1 | 9 |
| f1_m0p05 | 1 | 1.085764 | 96/103 | 320/322 | 138 | 3 | 39 |
| f2_m0p05 | 2 | 0.585764 | 104/106 | 322/326 | 242 | 1 | 189 |
| f4_m0p05 | 4 | 0.335764 | 107/110 | 331/331 | 393 | 0 | 300 |
| passive5 | At expiration | 5 | 75/101 | 327/328 | 86 | 0 | 4 |
| stock4 | At expiration | 5 | 67/113 | 339/339 | 115 | 0 | 1 |

### Persistence: thirty seconds, fixed 40 ms storage delay

| Policy | Accepted writes | Active probes | Active after initial expiry | Interior holder gaps |
| --- | ---: | ---: | ---: | ---: |
| f0p5_safe | 15/15 | 292/292 | 271/271 | 0 |
| f2_safe | 60/60 | 295/295 | 289/289 | 0 |
| f2_too_short | 60/60 | 80/295 | 78/293 | 236 |

### Native minimum-five capability check

Seed 2211, f=1/s, d=1.085764 s, twelve-second submission window plus drain: 9 verified X-led groups, each of size five; 36/37 honest steps verified; zero incorrect returned tensors.

### Evidence and source pin

Runtime source commit: `937090e2a5659d49195a6759cf6bbcda5bb593b1`. The renderer checked all eight runtime source digests against the unchanged repository source and reanalysed all 81 final trials.

- [Full report](../../results/step22_refresh_report_20261007.md)
- [Raw grouping trials](../../results/step22_bounded_sweep_20261007_a)
- [Raw continuous trials](../../results/step22_continuous_20261007_a)
- [Raw persistence trials](../../results/step22_endurance_20261007_a)
- [Raw minimum-five trial](../../results/step22_five_members_20261007_a)
- [Recalculated presentation evidence](evidence-check.json)
