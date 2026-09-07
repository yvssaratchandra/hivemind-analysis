# Code Walkthrough — the `research/` tree

What each module does, why it is shaped that way, and which design decisions carry methodological weight.
Written for a Python developer: no syntax explanations, no library tutorials. The focus is *why the code is
arranged like this*, since that is what determines whether the results mean anything.

**Companion documents**

| File | Purpose |
|---|---|
| [`README.md`](README.md) | Narrative overview and headline findings |
| [`FINDINGS.md`](FINDINGS.md) | Canonical results, with provenance and uncertainty |
| [`PLAN.md`](PLAN.md) | Design log and deviations |
| [`HIVEMIND_EXPLAINED.md`](HIVEMIND_EXPLAINED.md) | How Hivemind itself works |
| [`QA_NOTES.md`](QA_NOTES.md) | Q&A log, including corrections |
| **this file** | **How the research code is built** |

---

## 1. Constraints that shaped everything

Three rules, applied without exception. Most of the structural oddities below follow from them.

1. **Core Hivemind is never modified.** Not one line under `hivemind/`. Every defense is a standalone
   prototype that *reimplements* the check it proposes rather than patching the call site. This keeps the
   study honest (results describe upstream as it ships, not a fork) and keeps the diff reviewable.
2. **Loopback only.** Every swarm binds `127.0.0.1` (`LOCALHOST_MADDRS`). No public Hivemind, IPFS, or
   any other live network is contacted. Attack code exists to measure defenses on a swarm we own.
3. **Raw observations reach disk.** One CSV row per *repetition*, never per aggregate, so any result can be
   re-analysed without re-running a swarm.

---

## 2. Layout and dependency direction

```
research/
├── common/          infrastructure — no attack logic
│   ├── metrics.py            Recorder, seeding, summary stats
│   ├── sybil.py              the attacker's arithmetic (pure functions)
│   ├── swarm.py              Tier 1: in-process swarm
│   ├── multiprocess_swarm.py Tier 2: one OS process per peer
│   └── plotting.py           optional matplotlib helpers
├── attacks/         one module per track — mechanism, no I/O policy
│   ├── eclipse_key.py        Track A: record custody
│   ├── sybil_matchmaking.py  Track B: group capture
│   └── poison_averaging.py   Track C: learning harm
├── defenses/        one module per prototype defense
│   ├── id_binding.py         D1: node_id ↔ PeerID
│   ├── signed_declare.py     D2: signed averager declarations
│   ├── robust_agg.py         D3: aggregation rules (incl. Hivemind's own)
│   └── weight_bound.py       D4: receiver-side weight validation
└── experiments/     runnable scripts — sweeps, CSVs, figures
    ├── smoke_test.py
    ├── track_a_eclipse.py  track_b_amplification.py
    ├── track_c_learning_harm.py  track_d_defenses.py
    └── tier2_validation.py
```

Dependencies flow strictly one way: `experiments → attacks/defenses → common → hivemind`. Nothing in
`common/` imports from `attacks/`, and no library module writes files or prints. **Only `experiments/` does
I/O**, which is what makes the attack and defense modules importable from a notebook or a test without
side effects.

---

## 3. `common/` — infrastructure

### `metrics.py`

Defines the three quantities the study reports (replica capture, local malicious fraction `f_l`,
learning harm) and the plumbing to record them.

**`stable_seed(*parts)` is the one piece worth dwelling on.** It derives a seed via `blake2b` over
`repr()`-joined parts rather than `hash()`:

```python
digest = hashlib.blake2b("|".join(repr(part) for part in parts).encode(), digest_size=8)
return int.from_bytes(digest.digest(), "big")
```

Python salts string hashing per process (`PYTHONHASHSEED`), so a seed derived from `hash()` silently differs
on every run *even when the caller passes a fixed `--seed`*. That produced non-reproducible results early on;
this is the fix, and it is verified by running the simulation arms under `PYTHONHASHSEED=random` and
comparing outputs. A reproducibility bug that only manifests across processes is exactly the kind that
survives casual testing, so it is worth the named helper rather than an inline call.

**`Recorder`** accumulates `dict` rows and writes `<name>.csv` plus `<name>.meta.json` (parameters, seed, row
count). The `columns` property unions keys across rows in insertion order, so experiments can add fields to
later rows without breaking the header. The sidecar exists so a CSV is never orphaned from the parameters
that produced it.

**`summarize`** returns mean/std/min/max/n, filtering `None` and `NaN`. Population variance (`/n`), not
sample variance — the repeats are the whole population of runs for that cell.

### `sybil.py` — the attacker's arithmetic

Pure functions over `DHTID`. No networking, no state, no I/O. This is deliberate: the entire eclipse
primitive is arithmetic, and isolating it means the analytical layer can sweep to 10⁴ peers instantly while
the live layer reuses the *same* functions, so the two layers cannot silently diverge.

| Function | What it computes |
|---|---|
| `key_to_dhtid(key)` | `DHTID.generate(source=key)` — mirrors `node.py:386,564` exactly, so the attacker targets the same address the honest network does |
| `closest_ids(target, count)` | `[DHTID(target ^ i) for i in range(count)]` — optimal placement, since `target ^ i` has XOR distance exactly `i` |
| `ids_sharing_prefix(...)` | a *weaker* attacker aiming at a keyspace region; the right model for a grinding attacker under D1 |
| `random_ids(count)` | the honest baseline — and the **control condition** |
| `capture_count(...)` | of the `num_replicas` slots nearest the target, how many the attacker holds |

`capture_count` is the *structural* metric: given everyone's node_ids, who would an ideal lookup converge on?
It deliberately ignores routing-table state, because separating "the placement rule is broken" from "routing
happened to be imperfect" is the point. Live lookups are measured separately and compared.

### `swarm.py` — Tier 1

`build_swarm(num_honest, sybil_node_ids)` puts every peer in one event loop via `DHTNode.create`. These are
real Hivemind nodes with real `p2pd` daemons and real libp2p transport — only the *process boundary* is
shared.

Three decisions that matter:

- **Sybils join last.** The honest network forms first, then the attacker arrives — the realistic case. Joining
  first would let attacker peers seed everyone's routing tables and overstate the result.
- **Batched joins** (`join_batch_size=16`) so a few hundred peers don't open every connection simultaneously.
- **`running_swarm` is an async context manager.** Without guaranteed teardown, one failed assertion leaves
  hundreds of `p2pd` processes running. `shutdown` uses `return_exceptions=True` so one bad node cannot leak
  the rest.

### `multiprocess_swarm.py` — Tier 2

Exists to kill two specific alternative explanations for the Tier-1 results:

- *shared-process scheduling* — all peers progressing in lockstep on one event loop;
- *star topology* — everyone bootstrapping off the same node, giving unusually uniform routing tables.

So: one `mp.Process` per peer, and each joiner bootstraps off a **randomly chosen** already-running peer.
The first `num_sequential=3` peers come up serially to seed the network; the rest join in parallel.

This follows `tests/test_utils/dht_swarms.py`, with one necessary divergence: that helper forwards a single
`**kwargs` to every node and therefore cannot give each peer a *different* `node_id` — precisely what a Sybil
experiment needs. Hence the reimplementation. `node_id` crosses the process boundary as a plain `int` (a
`DHTID` is an `int` subclass; pickling the subclass through fork is pointless work).

`shutdown()` escalates SIGTERM → join(10) → kill → join(5), because a failed run otherwise strands dozens of
`p2pd` daemons. RAM-bound at ~20–30 peers (~57 MB per `p2pd`), which is why it validates headline numbers
rather than running sweeps.

### `plotting.py`

Thin matplotlib wrapper. `available()` lets experiments degrade gracefully — matplotlib is not a Hivemind
dependency, and **a missing plotting library must never prevent a CSV from being written**. One consistent
semantic throughout: red = attacker advantage, blue = honest baseline. `reference_line` draws the
"no attack advantage" marker (e.g. `f_l == f_g`) that every attacker curve should be read against.

---

## 4. `attacks/`

### `eclipse_key.py` — Track A

Two layers answering different questions, sharing the same `sybil.py` primitives:

- **`analytical_capture`** — pure XOR math. Honest peers at uniform node_ids, attacker at distances 0,1,2,…
  No processes, so it scales to 10⁴ peers and isolates the placement rule from any routing artifact.
- **`live_capture`** — a real swarm; runs a real `store` and a real lookup, and reports three separate
  numbers: structural capture, live lookup capture, and **actual store placement**.

That third one is the ground truth. `_storage_holders` reads each node's `protocol.storage` directly and asks
*where did the honest peer's data physically land* — which is what an eclipse actually is. Inferring custody
from lookup results alone would conflate routing with storage.

**`censorship_test` contains the methodological core of Track A.** Custody is turned into demonstrated harm
(the attacker stops serving, modelled by shutting the Sybils down after the honest write), but the important
part is the control:

```python
sybil_ids = closest_ids(target, num_sybils) if crafted else random_ids(num_sybils, rng=rng)
```

The identical experiment runs twice with the *same number of identities* — once with crafted addresses, once
with random ones. Both attackers have identical resources; only the freedom to choose a routing address
differs. Any gap is therefore attributable to that freedom alone, and not to "the attacker owns more peers."
Without this arm, the headline result would be unfalsifiable. The measured gap is 8/8 vs 0/8.

`sybils_needed_for_full_capture` scans upward for the smallest count achieving total capture — the attack-cost
headline. `nearest_honest_distance` reports why that number is so small.

### `sybil_matchmaking.py` — Track B

`fabricate_peer_id(index)` manufactures a syntactically valid `PeerID` **with no keypair behind it**. That is
not a shortcut; it *is* the finding. The matchmaking layer requires nothing more of a candidate than a
well-formed identifier in a DHT record, because those records are unsigned.

**`predict_next_group_bits` is the module's centrepiece** — a faithful reimplementation of
`GroupKeyManager.update_key_on_group_assembled`, showing that every input (`group_id`, ordered `peer_ids`,
`target_group_size`) is visible to every group member. Reimplementing rather than calling upstream is the
point: it demonstrates an attacker can compute the victim's future bucket independently.

Which raises the obvious objection — *how do you know the reimplementation is faithful?*
**`verify_prediction_matches_hivemind` is the oracle.** It builds a `GroupKeyManager` against a stub DHT
(the constructor only reads `.peer_id`), runs Hivemind's own `update_key_on_group_assembled`, and returns
`(predicted, actual)` for comparison. The experiment runs this across 144 combinations of group ids, target
sizes and bit-widths. This is what upgrades "we think we modelled it right" to a checked claim, and it doubles
as a regression detector if upstream ever changes the shuffle.

`simulate_amplification` models the victim's candidate view without a swarm, and deliberately keeps the two
multipliers separable — **identity multiplication** (one process, many declarations) and **targeting** (all of
them in the victim's bucket). Note the asymmetry encoded in the arithmetic, which is the finding in one line:

```python
global_fraction = attacker_processes / (num_honest + attacker_processes)   # real peers
local_fraction  = num_fake_declarations / victim_candidates                # fabricated records
```

`simulate_tracking` runs the same setup over rounds with `tracking=True|False`, contrasting an attacker that
recomputes the victim's next bucket with a naive one that floods a fixed bucket. The naive arm is what makes
the tracking arm meaningful: it shows re-formation *does* work against an attacker who declines to use public
information.

> **Caveat surfaced after these were written** (see [`QA_NOTES.md`](QA_NOTES.md#q14-do-peers-switch-rooms-after-each-epoch)):
> `initial_group_bits` defaults to `""`, so under `hivemind.Optimizer` defaults there is only one bucket and
> re-bucketing never occurs. The tracking analysis therefore describes the best case Hivemind offers when the
> feature is enabled; the default is strictly weaker.

### `poison_averaging.py` — Track C

`LeastSquaresTask` is a 64-dimensional convex problem with a **known optimum**, sharded across 16 peers
(`b_i = A_i x* + noise`). Deliberately not a neural network: because `x*` is known exactly, any displacement is
attributable to the attack rather than to training noise in a model you cannot inspect. Rows are unit-scaled so
`AᵀA/rows ≈ I`, keeping a fixed learning rate convergent in tens of rounds.

Attacker strategies share one signature `(honest, current, task, **kwargs) -> Tensor` and are dispatched
through the `ATTACKS` dict, so adding a strategy requires no changes to the runner. They receive the honest
contributions because **in Hivemind an attacker genuinely can see them** — it is a full participant in the
all-reduce, not an outsider.

`attack_targeted_stealth` is the one worth reading closely: it starts from the honest mean, so its norm and
direction sit *inside* the honest distribution, then offsets only the target coordinates. This is what produces
the C3 result that the attacker is consistently quieter than honest peers — and therefore that no norm-based
filter fires.

`run_training` holds everything fixed except the `(aggregator, attack)` pair. Two details:

- Sybils driven by one process submit **identical** vectors (`malicious.unsqueeze(0).repeat(num_malicious, 1)`).
  Realistic, and it is the direct cause of the C4 finding that Krum degrades — identical vectors form the
  tightest cluster, which is exactly what Krum's "most agreed-with" selection rewards.
- The defender never receives the true `f`. `estimate_num_byzantine(num_peers, assumed_fraction)` gives a
  *guess*, so robust rules are evaluated under the uncertainty a real deployment has.

---

## 5. `defenses/`

Each module is a standalone prototype: it reimplements the proposed check rather than patching Hivemind, but
exposes it as the predicate an upstream patch would call.

### `robust_agg.py` (D3)

Contains Hivemind's **current** rule alongside the alternatives. `weighted_mean` reproduces
`TensorPartReducer.accumulate_part` faithfully **including its lack of guards** — no non-negativity check, no
finiteness check, no cap. Reproducing the flaw rather than a sanitised version is what makes the comparison
against `krum` / `multi_krum` / `coordinate_median` / `trimmed_mean` legitimate.

One deliberate asymmetry: **the robust rules ignore the sender-supplied weight entirely.** That is the field an
attacker controls, so a rule that honoured it would hand back the leverage it exists to remove. This is why
C2 shows Krum and median unchanged at `weight = 10⁶` while the weighted mean diverges.

### `id_binding.py` (D1)

`derive_node_id(peer_id)` returns `DHTID.generate(source=peer_id.to_bytes())` — reusing the hash Hivemind
already applies to record keys, so no new primitive is introduced. `is_node_id_bound` is one hash, intended for
`rpc_ping`, `update_routing_table`, and the neighbour lists from `call_find` (`protocol.py:303`), which are
ingested unverified today.

`expected_keypairs_for_prefix(b)` returns `2**b` from the geometric distribution. **Stated honestly in the
docstring and in `FINDINGS.md`: this is derived arithmetic, not measured — no keypairs were actually ground.**
The `track_d` runner separately checks what a realistic budget achieves by sampling 64 legitimately-derived
addresses and measuring their capture (2/5), which is an empirical complement to the analytic figure.

### `signed_declare.py` (D2)

Requires every averaging declaration to be owner-tagged and signed. The design note worth reading is *why a
custom validator is needed*: `SchemaValidator` binds a fixed field name to a key, but averaging group keys are
dynamic (`{prefix}.0b{bits}`, changing per round), so the constraint must be a pattern over keys.

The docstring is explicit about the limit: `RSASignatureValidator` is **fail-open** — a record lacking an
`[owner:...]` marker is accepted from anyone (`crypto.py:47`). D2 closes that for averaging keys, eliminating
*impersonation* and raising a fabricated identity's price from one DHT write to one keypair plus one write.
It does **not** stop Sybils, and says so.

### `weight_bound.py` (D4)

Three escalating levels — `sanitize_weight` (finite, non-negative), `clip_weight` (cap at `max_ratio` × group
**median**; the median specifically because the mean is what the attacker is moving), and
`reconcile_with_progress` (cross-check against the peer's already-signed progress record).

The framing that makes D4 the cheapest patch: Hivemind **already asserts** finiteness and non-negativity — but
only on the sender's own value (`control.py:95-96`, `averager.py:401`). D4 moves an existing check to the
receiving side. It also records a corrected assumption: sanitizing alone does *not* stop `weight = 1e6`, since
that value is both finite and non-negative. Only the relative cap contains it.

---

## 6. `experiments/`

Runnable scripts. The only layer that prints, writes files, or parses arguments.

**Common shape:** module-level constants for the sweep grid → one `run_*` function per result set → `main()`
with `argparse`. Expensive arms are opt-in flags (`--live`, `--censorship`), so the default invocation is fast
and laptop-safe.

**The seeding pattern** appears in every sweep:

```python
rng = seeded_rng(seed, swarm_size, num_sybils, repeat)
```

A fresh generator per cell, derived from the run seed *and* the cell coordinates. Consequences: cells are
independent (no cross-contamination through shared generator state), any single cell can be re-run in
isolation and reproduce exactly, and adding a grid point does not perturb existing ones.

| Script | Produces |
|---|---|
| `smoke_test.py` | Minimal end-to-end check of the two load-bearing claims: node_ids are honoured, and 5 crafted ids capture the replica set. Run this first when something breaks. |
| `track_a_eclipse.py` | `track_a_analytical`, `track_a_cost`, `--live` → `track_a_live`, `--censorship` → `track_a_censorship` |
| `track_b_amplification.py` | `track_b_amplification`, `track_b_decay`, the 144-case prediction check, `--live` → `track_b_live` |
| `track_c_learning_harm.py` | `track_c_damage`, `track_c_weight`, `track_c_stealth` |
| `track_d_defenses.py` | `track_d_id_binding`, `track_d_signed_declare`, `track_d_weight_bound`, D3 summary |
| `tier2_validation.py` | `tier2_validation` — headline claims on separate OS processes |

`track_d_defenses.run_d1_identity_binding` is a good template for how a defense arm is structured: (1) the
Track A attacker's addresses are checked against admission; (2) 50 honest derived addresses are checked for
false positives; (3) the analytic grinding cost is tabulated; (4) an empirical check of what a realistic
grinding budget actually captures. **Attack effectiveness and honest false-positive rate are always measured
together** — a defense that rejects everything would otherwise look perfect.

---

## 7. Cross-cutting decisions

Consolidated, since these are what a reviewer should push on.

| Decision | Rationale |
|---|---|
| **Two fidelity tiers** | Analytical/in-process layers give statistical power (40–200 repeats); the live and Tier-2 layers give validity. Neither alone suffices; agreement between them is the argument. |
| **Structural vs. live separated** | Keeps "the placement rule is broken" distinct from "routing was imperfect." They are reported as separate columns, never merged. |
| **Controls carry the claims** | `censorship_test`'s random-node_id arm and D1's honest-peer arm. An attack number without a control is not a finding. |
| **Upstream oracle** | `verify_prediction_matches_hivemind` checks the reimplementation against Hivemind's own code path, 144/144. |
| **Cross-process determinism** | `stable_seed` via blake2b; verified under `PYTHONHASHSEED=random`. Live arms are explicitly *not* fully deterministic (network timing, caching races) and this is stated rather than hidden. |
| **Upstream behaviour reproduced faithfully, flaws included** | `weighted_mean` keeps the missing guards; otherwise the robust-rule comparison would be against a strawman. |
| **Defenses are prototypes, not patches** | Honours the never-modify-core constraint and keeps results descriptive of shipped Hivemind. The cost — no integration testing — is stated as limitation #1 in `FINDINGS.md`. |
| **Guaranteed teardown everywhere** | `running_swarm` context manager, escalating `shutdown()`. Prevents leaked `p2pd` daemons from a failed run. |
| **Graceful degradation** | Missing matplotlib skips figures but never blocks a CSV. |

---

## 8. Running it

```bash
.venv/bin/python -m research.experiments.smoke_test              # start here (~30 s)

.venv/bin/python -m research.experiments.track_a_eclipse         # analytical sweeps, fast
.venv/bin/python -m research.experiments.track_a_eclipse --live --censorship   # + real swarms, slow
.venv/bin/python -m research.experiments.track_b_amplification --live
.venv/bin/python -m research.experiments.track_c_learning_harm
.venv/bin/python -m research.experiments.track_d_defenses
.venv/bin/python -m research.experiments.tier2_validation        # RAM-heavy, ~23 peers
```

All accept `--seed` (defaults: 1234, or 7 for Tier 2). Outputs land in `research/results/` as
`<name>.csv` + `<name>.meta.json`, with PNGs where matplotlib is available. Simulation arms reproduce exactly
across processes; live arms do not (stated in `FINDINGS.md` limitation #6).

---

## 9. Known gaps and where to extend

Honest list of what the code does *not* currently do.

1. **Defenses are never integrated.** They are evaluated standalone. Wiring D1 into a running swarm would
   require modifying core Hivemind, which the standing constraint forbids — so the natural next step is a
   subclass or a monkeypatched `DHTProtocol` living entirely under `research/`.
2. **D1's grinding cost is arithmetic, not measured.** Actually grinding keypairs at a small prefix (say 16
   bits) would convert a derived number into an observed one.
3. **Only `grad_averager` is modelled.** `hivemind.Optimizer` runs a *second* averager on
   `{run_id}_state_averager` that averages weights directly. The Track B/C attacks should apply unchanged and
   arguably more directly (no optimizer between attacker input and parameters). Untested.
4. **Group formation is modelled as bucket occupancy, not as a race.** `simulate_amplification` assumes the
   victim picks from whoever is in its bucket; real matchmaking resolves by earliest declared deadline and
   then by connection arrival order. Modelling the race would let the leader-election attack be quantified
   directly rather than inferred.
5. **Track C is a synthetic convex task** — deliberately, but it means nothing here speaks to transformer
   training dynamics.
6. **Live swarms are small** (≤40 Tier 1, 23 Tier 2), bounded by `p2pd` memory. Scale claims rest on the
   analytical layer, cross-validated exactly but only at small *n*.

---

*Documentation only; part of the additive `research/` tree. No core Hivemind code was modified.*
