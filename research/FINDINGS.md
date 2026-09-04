# Findings — Hivemind DHT control plane security

**Canonical results reference.** Every number below was read back from the committed CSV that produced it,
not transcribed from console output. Each result names its source file and script so it can be re-checked or
re-run. Uncertainty is stated where it exists.

- **Date of this result set:** 2026-09-04
- **Branch:** `research/net-security` · **Hivemind version:** 1.2.0.dev0 (fork point `4bd43b7`)
- **Environment:** Python 3.13.9, torch 2.14.0+cpu, Linux, 16 GB RAM, CPU only, loopback only
- **Raw data:** `research/results/*.csv` (one row per repetition) + `*.meta.json` (parameters and seeds)
- **Narrative + method:** [`README.md`](README.md) · **Design log and deviations:** [`PLAN.md`](PLAN.md)

---

## 0. Root cause

Hivemind peers carry two identities. Record custody is decided by XOR distance over the **unverified** one.

| | libp2p **PeerID** | **node_id** (DHTID) |
|---|---|---|
| Decides | how packets reach the peer | **which records the peer stores** |
| Produced by | `multihash(sha256(pubkey))` | `DHTID.generate()` — random, **or caller-supplied** |
| Verified by peers? | yes (TLS proves key possession) | **no** — self-reported, accepted verbatim |
| Source | `hivemind/p2p/p2p_daemon_bindings/datastructures.py:18` | `hivemind/dht/node.py:171`, `hivemind/dht/protocol.py:150` |

Because `node_id` is self-chosen and unbound to the authenticated `PeerID`, an attacker computes any key's
address and places identities beside it. The cryptographically sound identity never enters the routing decision.

Three properties compound it:

| # | property | source |
|---|---|---|
| 1 | Averaging discovery records are **unsigned**; subkey is an unverified `peer_id` | `hivemind/averaging/key_manager.py:46` |
| 2 | Group **leadership is won by earliest self-declared expiration** | `hivemind/averaging/matchmaking.py:478` |
| 3 | Aggregation is an **unbounded weighted mean with a sender-supplied weight** | `hivemind/averaging/partition.py:242`, `allreduce.py:339` |

For contrast, `ProgressTracker` *does* sign its records (`hivemind/optim/progress_tracker.py:108`) — the
machinery exists; averaging keys were never given it. A repo-wide search for
`krum|byzantine|median|trimmed|robust` returns nothing.

---

## A. Eclipsing a DHT record

### A0 — Attack cost does not grow with the network
`track_a_cost.csv` · `track_a_eclipse.py` · 40 repeats per point · `num_replicas=5`

| honest swarm | identities for a **total** eclipse |
|---|---|
| 50 | 5.00 ± 0.00 |
| 100 → 5 000 | 5.00 ± 0.00 |
| 10 000 | 5.00 ± 0.00 |

**Cost is exactly `num_replicas`, with zero variance, independent of network size.** Scale offers no protection.
Cause: honest `node_id`s are uniform over 160 bits, so the nearest honest peer to a given key sits ~10⁴⁴–10⁴⁶
away in XOR distance, while a self-placed attacker sits at distance 0.

### A1 — Theory matches a live swarm exactly
`track_a_live.csv` · 40 honest peers, real `p2pd` daemons

| Sybils | structural | live lookup | actual store placement |
|---|---|---|---|
| 1 | 0.20 | 0.20 | 0.20 |
| 3 | 0.60 | 0.60 | 0.60 |
| 5 | **1.00** | **1.00** | **1.00** |

At 5 Sybils an honest peer's `store` landed on **5 attacker nodes and 0 honest nodes**.

### A2 — Custody becomes censorship; a control isolates the cause
`track_a_censorship.csv` · 30 honest peers · 5 identities · n=8 per cell

| node_ids | caching | custody | censorship success |
|---|---|---|---|
| **crafted** | off | 1.00 | **8/8 (100%)** |
| **crafted** | on (default) | 1.00 | 5/8 (~50–75%, see note) |
| random (control) | off | 0.18 | **0/8 (0%)** |
| random (control) | on (default) | 0.15 | **0/8 (0%)** |

**The control is the finding:** an attacker with the *same five identities* but random addresses censors
nothing. Damage is attributable to address choice, not to owning peers.

> **Uncertainty.** The crafted + caching-on cell is genuinely noisy: 50%, 62% and 75% across three runs at n=8.
> This arm runs on a live swarm, so the seed fixes node_id selection but not network timing or caching races.
> The 100% and 0% cells were stable across all runs. Treat the caching-on figure as "roughly half", not 62%.

**Incidental mitigation.** Hivemind's opportunistic caching (`cache_locally`, `cache_nearest=1`,
`cache_on_store`) roughly halves censorship success. This is a side effect of a *performance* feature, not a
security control — caches expire and the attacker still holds every authoritative replica.

---

## B. From a small global fraction to a large local one

### B1 — One process reaches ~97% of a victim's candidate list
`track_b_live.csv` · 12 honest peers + **1 attacker process** · `f_global = 0.077`

| fabricated declarations | candidates seen | fake | `f_local` | amplification | leader captured |
|---|---|---|---|---|---|
| 1 | 13 | 1 | 0.077 | ×1.0 | **yes** |
| 5 | 17 | 5 | 0.294 | ×3.8 | yes |
| 25 | 37 | 25 | 0.676 | ×8.8 | yes |
| 100 | 112 | 100 | 0.893 | ×11.6 | yes |
| 400 | 412 | 400 | **0.971** | **×12.6** | yes |

A fabricated averager identity costs **one DHT write** — no keypair, no process, no connection — because the
declaration is unsigned and its subkey is never checked against the writer.
**Group leadership costs exactly one fabricated declaration.**

Simulated at 100 honest peers (`f_global` = 0.0099, one attacker process), `track_b_amplification.csv`,
200 repeats — `f_local` at 100 fabricated declarations:

| buckets | 1 | 4 | 16 | 64 |
|---|---|---|---|---|
| `f_local` | 0.500 | 0.802 | 0.941 | 0.984 |

**Amplification ×50 to ×99.** Two multipliers compound: identity multiplication (writes are free) and
targeting (honest peers scatter across buckets; the attacker concentrates in the victim's).

### B2 — Group re-formation provides no decay against a tracking attacker
`track_b_decay.csv` · 20 rounds · 16 buckets · 200 repeats

The re-bucketing shuffle (`hivemind/averaging/key_manager.py:94`) is seeded on `group_id` and computed entirely
from values **every group member already holds**. `predict_next_group_bits` reproduces Hivemind's own
`update_key_on_group_assembled` **144/144** across group ids, target sizes and bit-widths.

| attacker | `f_local` round 0 | rounds 1–19 (mean, min–max) |
|---|---|---|
| naive (floods a fixed bucket) | 0.891 | **0.058** (0.030–0.086) |
| tracking (predicts victim's bucket) | 0.893 | **0.891** (0.885–0.897) |

Re-formation is unpredictable to an outsider but transparent to a participant. **It sheds only an attacker who
declines to use public information.** Any defense relying on re-formation for dilution needs the shuffle to
depend on a secret the attacker does not hold.

> **Note on reporting.** The naive attacker's post-round-0 value fluctuates (the victim re-buckets uniformly and
> lands back in the attacker's bucket with probability 1/16, spiking `f_local` to ~0.9 that round), so the range
> is quoted rather than a single round.

---

## C. From a captured group to a damaged model

Synthetic distributed least-squares with a known optimum `x*`; 16 peers, 64 dims, 60 rounds, 3 seeds.
Clean run converges to **0.002**. Defenders get a *conservative guess* at `f`, never the true count.

### C1 — Plain averaging breaks at one malicious peer in sixteen
`track_c_damage.csv` — final distance to optimum

| attack | rule | f_l=0.06 | 0.25 | 0.44 | 0.50 |
|---|---|---|---|---|---|
| sign_flip | **weighted mean (today)** | **4.37** | 9.82 | 364.8 | diverged |
| sign_flip | multi-Krum | 0.003 | 0.003 | 0.003 | diverged |
| targeted stealth | **weighted mean (today)** | 0.257 | 1.001 | 1.635 | 1.799 |
| targeted stealth | coordinate median | 0.002 | 0.003 | 0.004 | 2.594 |

**Plain averaging breaks at `f_l` = 0.0625 for all four attacks** — while B1 reaches `f_l` ≈ 0.9, about 14×
more than required.

### C2 — The unvalidated weight (sharpest single finding)
`track_c_weight.csv` — one malicious peer in sixteen, changing **only** its claimed weight

| rule | weight = 1 | weight = 10⁶ |
|---|---|---|
| **weighted mean (today)** | 4.37 | **diverged** |
| krum | 0.0062 | 0.0062 (unchanged) |
| coordinate median | 0.0023 | 0.0023 (unchanged) |

The weight crosses the wire and is applied verbatim; its honest value is a *self-reported sample count*
(`hivemind/optim/grad_averager.py:196`). A one-line attack that no group-composition defense touches.

### C3 — Targeted poisoning is invisible to magnitude checks
`track_c_stealth.csv` — 8 of 64 coordinates targeted

| `f_l` | targeted error | attacker / honest contribution norm |
|---|---|---|
| 0.06 | 0.253 | **0.95** |
| 0.25 | 0.982 | 0.96 |
| 0.50 | 1.752 | 0.96 |

The attacker is consistently **quieter than honest peers**. No norm- or magnitude-based filter would fire.
Robust aggregation, not anomaly detection, is the answer.

### C4 — Where the robust rules themselves fail
At `f_l` = 0.5 attackers are a majority and **every** rule fails — a fundamental limit, not an implementation
flaw. Before that: `trimmed_mean` breaks at ~0.31; `krum`/`multi_krum` hold to ~0.44 against sign-flip but break
at ~0.31 against targeted stealth, because Sybils driven by one process submit **identical** vectors and so form
the tightest cluster — exactly what Krum's "most agreed-with" selection rewards. `coordinate_median` was the
most durable overall.

---

## D. Prototype defenses

All are **standalone prototypes**; core Hivemind is never modified (see the standing constraint in `PLAN.md`).

| | defense | measured effect | source |
|---|---|---|---|
| **D1** | bind `node_id` to public key | **5/5** crafted addresses rejected; **0/50** honest false positives; 64 legitimately-derived attacker addresses capture only **2/5** slots | `track_d_id_binding.csv` |
| **D2** | signed averager declarations | **0/200** fabricated accepted (was 200/200); honest signed accepted 1/1; owner-tagged-but-unsigned rejected 0/1 | `track_d_signed_declare.csv` |
| **D3** | robust aggregation | displacement with one extreme peer in 16: **5.7e6 → 0.68–4.21** | `track_d_defenses.py` |
| **D4** | bound the weight on receipt | sanitize alone: **no effect** on weight=10⁶; sanitize + cap at 10× group median: diverged → **11.77** | `track_d_weight_bound.csv` |

**D1 cost model.** Under binding, hitting a target key requires grinding keypairs: `2^prefix_bits` expected
generations each, so `num_replicas × 2^prefix_bits` for a total eclipse — 8.4e7 at a 24-bit prefix, 2.1e10 at 32.
*These are derived arithmetic (geometric distribution), not empirically ground keypairs.*

**Caveats that matter:**
- **D2 does not stop Sybils.** Signing proves key possession, not distinctness. It removes *impersonation* and
  raises a fake identity's price from one DHT write to one keypair plus one write. Precondition for D1, not a
  substitute.
- **D4 corrected a plan assumption.** Sanitizing for finite/non-negative does **not** stop `weight=1e6` — that
  value is both. Only the relative cap contains it, and a 10× cap still grants 10× influence by design.
  D4 is the cheapest patch: Hivemind already asserts this on the *sender* side
  (`averaging/control.py:95-96`, `averager.py:401`); D4 moves the check to the receiver.

**No single defense suffices.** D1 keeps `f_l` low, D3 tolerates what remains, D4 closes a channel neither
touches, D2 makes identity claims meaningful at all.

---

## E. Tier-2 validation (separate OS processes, randomised topology)
`tier2_validation.csv` · 18 honest + 5 Sybil peers, one process each, non-star bootstrap

| claim | Tier 1 | Tier 2 |
|---|---|---|
| structural replica capture | 1.00 | **1.00** |
| live lookup capture | 1.00 | **1.00** (5/5 returned are Sybil) |
| `f_local` from fabricated declarations | 0.971 (400 writes) | **0.949** (150 writes, 150/158 seen) |

Neither shared-process scheduling nor star topology was responsible for the Tier-1 results.

---

## Limitations

Stated so the results are not over-read:

1. **Defenses are not integrated.** They are evaluated standalone; core Hivemind is deliberately untouched.
2. **D1's grinding costs are derived, not measured** (2^bits arithmetic; no keypairs were actually ground).
3. **Statistical strength varies.** A0 is 40 repeats and B1/B2 are 200; the censorship arm is n=8 and Track C is
   3 seeds — the latter two are indicative, not tight.
4. **Track C is a synthetic convex task**, chosen so damage is unambiguous against a known optimum. It is *not*
   evidence about transformer training dynamics.
5. **Live swarms are small** (≤40 peers; Tier-2 23) because each peer runs its own `p2pd` (~57 MB). Scale claims
   rest on the analytical layer, which the live layer cross-validates exactly but only at small n.
6. **Live-swarm arms are not fully seed-deterministic** — network timing and caching races are not controlled by
   `--seed`. Simulation arms are fully reproducible (verified across processes with `PYTHONHASHSEED=random`).

## Open question

The re-formation shuffle is seeded on the public `group_id`, which is *why* B2 shows no decay. Seeding it on
something an attacker cannot compute — e.g. a value contributed by all members and revealed only afterwards —
would restore the dilution the design appears to intend. That is a design change and the most interesting
remaining research direction; it requires no modification to core Hivemind to study.
