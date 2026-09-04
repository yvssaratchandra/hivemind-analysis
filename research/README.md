# Sybil and Eclipse attacks on Hivemind's DHT control plane

A reproducible study of how Hivemind's peer-discovery layer can be captured, what that
capture costs an attacker, and what it does to a training run. Everything here runs on
loopback on a single laptop, with no GPU and no real deep-learning training.

The aim is constructive: every attack is paired with a prototype defense and a
before/after measurement, so the findings can inform upstream hardening.

> **Scope.** All experiments bind to `127.0.0.1` only. No public Hivemind, IPFS, or
> other live network is touched. Attack code exists to measure defenses on a swarm the
> operator owns.

---

## The core issue: two identities, only one of them verified

Every Hivemind peer carries two identifiers, and the security of the control plane
depends on a link between them that does not exist.

| | **libp2p PeerID** | **DHTID (`node_id`)** |
|---|---|---|
| What it decides | how packets reach the peer | **which records the peer stores** |
| How it is produced | `multihash(sha256(public key))` — [`datastructures.py`](../hivemind/p2p/p2p_daemon_bindings/datastructures.py) | `DHTID.generate()`, random **or caller-supplied** — [`node.py:171`](../hivemind/dht/node.py) |
| Verified by peers? | yes, TLS proves key possession | **no** — self-reported and taken at face value ([`protocol.py:150`](../hivemind/dht/protocol.py)) |

Record custody is decided by XOR distance over `node_id`. Because `node_id` is chosen
by the peer and unbound to the authenticated `PeerID`, an attacker can compute any
key's address and place identities next to it. The cryptographically sound identity
never enters the routing decision.

Three further properties compound this, each confirmed in code and by measurement:

1. **Averaging discovery records are unsigned.** `declare_averager` writes a subkey of
   `peer_id.to_bytes()` with no validator attached ([`key_manager.py:46`](../hivemind/averaging/key_manager.py)),
   so a fabricated identity costs *one DHT write* — no keypair, no process, no connection.
   By contrast `ProgressTracker` **does** sign its records ([`progress_tracker.py:108`](../hivemind/optim/progress_tracker.py)),
   so the machinery exists; the averaging keys were simply never given it.
2. **Group leadership is self-declared.** Leaders win by earliest declared expiration
   ([`matchmaking.py:478`](../hivemind/averaging/matchmaking.py)), a value the declarer picks.
3. **Aggregation is an unbounded weighted mean with a sender-supplied weight.**
   `accumulator.add_(part, alpha=weight); denominator += weight`
   ([`partition.py:242`](../hivemind/averaging/partition.py)); the weight arrives on the wire
   unvalidated ([`allreduce.py:339`](../hivemind/averaging/allreduce.py)). A repo-wide search for
   `krum|byzantine|median|trimmed|robust` returns nothing.

---

## Findings

### A. Eclipsing a record costs 5 identities, at any network size

Honest `node_id`s are uniform over a 160-bit space, so the nearest honest peer to a
given key sits ~10⁴⁴–10⁴⁶ away in XOR distance. An attacker who picks its own address
sits at distance 0.

| honest swarm | identities for a **total** eclipse | nearest honest peer's distance |
|---|---|---|
| 50 | 5.00 ± 0.00 | ~2.6e46 |
| 1 000 | 5.00 ± 0.00 | ~1.3e45 |
| 10 000 | 5.00 ± 0.00 | ~1.3e44 |

**Cost is exactly `num_replicas`, with zero variance, and does not grow with the
network.** Scale provides no protection. On a live 40-peer swarm, theory matched
reality exactly (structural = live-lookup = actual store placement), and at 5 Sybils an
honest peer's `store` landed on **5 attacker nodes and 0 honest nodes**.

Turning that custody into censorship, with a control that isolates the cause:

| condition | custody | censorship success |
|---|---|---|
| crafted `node_id`s, caching off | 1.00 | **100%** |
| crafted `node_id`s, caching on (default) | 1.00 | 50% |
| **random `node_id`s (control)** | 0.15–0.25 | **0%** |

The control is the point: an attacker with the *same five identities* but random
addresses censors nothing. The damage is attributable to address choice, not to owning
peers. Hivemind's opportunistic caching halves the success rate — a real but incidental
mitigation from a *performance* feature, not a security control (caches expire, and the
attacker still holds every authoritative replica).

### B. One process reaches ~95% of a victim's candidate list

Because declarations are unsigned, a single process fabricated hundreds of averager
identities. Live, 12 honest peers + 1 attacker process (global fraction `f_g` = 0.077):

| fabricated declarations | victim's candidates | fake | `f_local` | amplification | leader captured |
|---|---|---|---|---|---|
| 1 | 13 | 1 | 0.077 | ×1.0 | **yes** |
| 25 | 37 | 25 | 0.676 | ×8.8 | yes |
| 400 | 412 | 400 | 0.971 | ×12.6 | yes |

**Group leadership costs exactly one fabricated declaration.** At 100 honest peers
(`f_g` = 0.0099), 100 declarations give `f_local` = 0.50–0.98 depending on bucket count —
**amplification ×50 to ×99**.

### C. Group re-formation does not shed a determined attacker

Hivemind re-buckets peers after every group
([`key_manager.py:94`](../hivemind/averaging/key_manager.py)). That shuffle is seeded on
`group_id` and computed entirely from values **every group member already holds**, so a
peer who shared *one* group with the victim can compute all of its future buckets. Our
`predict_next_group_bits` reproduces Hivemind's own re-bucketing **144/144** across
group ids, target sizes and bit-widths.

| attacker | `f_local` round 0 | round 19 |
|---|---|---|
| naive (floods a fixed bucket) | 0.893 | **0.046** |
| tracking (predicts victim's bucket) | 0.889 | **0.893** |

Re-formation is unpredictable to an outsider but transparent to a participant. It sheds
only an attacker who declines to use public information. **Any defense relying on
re-formation for dilution needs the shuffle to depend on a secret the attacker lacks.**

### D. Plain averaging breaks at one malicious peer in sixteen

Distributed least-squares with a known optimum, 16 peers, 3 seeds (clean run = 0.002):

| attack | rule | f_l=0.06 | 0.25 | 0.44 | 0.50 |
|---|---|---|---|---|---|
| sign_flip | weighted mean (today) | **4.37** | 9.82 | 364.8 | diverged |
| sign_flip | multi-Krum | 0.003 | 0.003 | 0.003 | diverged |
| targeted stealth | weighted mean (today) | 0.257 | 1.001 | 1.635 | 1.799 |
| targeted stealth | coordinate median | 0.002 | 0.003 | 0.004 | 2.594 |

**Plain averaging breaks at `f_l` = 0.0625 for all four attacks** — while Track B
reaches `f_l` ≈ 0.9, roughly 14× more than needed.

The **unvalidated weight** is the sharpest finding. One peer in sixteen, changing
nothing but its claimed weight:

| rule | weight = 1 | weight = 10⁶ |
|---|---|---|
| weighted mean (today) | 4.37 | **diverged** |
| Krum / coordinate median | 0.006 / 0.002 | unchanged |

And the targeted attack is invisible to magnitude-based checks: it drives its 8 chosen
coordinates off by 0.25 → 1.75 while its contribution norm stays at **0.95–0.96× the
honest mean** — consistently *quieter* than honest peers.

**Where the robust rules fail, stated plainly.** At `f_l` = 0.5 attackers are a majority
and *every* rule fails — a fundamental limit, not an implementation flaw. Before that,
`trimmed_mean` breaks at 0.31, and Krum breaks *earlier* against the targeted attack
(~0.31) than against sign-flip (~0.44), because Sybils driven by one process submit
**identical** vectors and so form the tightest cluster — exactly what Krum's
"most agreed-with" selection rewards. `coordinate_median` was most durable overall.

---

## Prototype defenses, re-measured

| | defense | effect |
|---|---|---|
| **D1** | bind `node_id` to the public key (`node_id == DHTID.generate(peer_id)`) | **5/5** crafted addresses rejected, **0/50** honest false positives; eclipse cost goes from *free* to 8.4e7 keypair generations (24-bit prefix). 64 legitimately-derived attacker addresses capture only **2/5** slots. |
| **D2** | signed averager declarations | **0/200** fabricated declarations accepted (was 200/200); honest signed ones accepted; owner-tagged-but-unsigned forgeries rejected. |
| **D3** | robust aggregation (Krum, Multi-Krum, median, trimmed mean) | displacement with one extreme peer in 16: **5.7e6 → 0.68–4.21**. |
| **D4** | bound the averaging weight on receipt | sanitizing alone does **not** stop `weight=1e6` (it is finite and positive); only a cap relative to the group median does (diverged → 11.77). |

Two caveats we consider important rather than incidental:

- **D2 does not stop Sybils.** Signing proves key possession, not distinctness, and
  keypairs are cheap. It removes *impersonation* and raises a fake identity's price from
  one DHT write to one keypair plus one write. It is a precondition for D1, not a
  substitute.
- **D4 is the cheapest patch of the four.** Hivemind already asserts exactly this
  property on the *sender* side ([`control.py:95-96`](../hivemind/averaging/control.py),
  [`averager.py:401`](../hivemind/averaging/averager.py)); D4 just moves the check to the
  receiver. Clipping *bounds* rather than removes the advantage (a 10× cap still grants
  10× influence) — a deliberate policy choice.

**No single defense suffices.** D1 keeps `f_l` low, D3 tolerates what remains, D4 closes
a channel neither touches, and D2 makes identity claims meaningful at all.

---

## Reproducing

```bash
python -m venv .venv && source .venv/bin/activate
pip install torch --index-url https://download.pytorch.org/whl/cpu   # CPU-only
pip install grpcio-tools && pip install -e . --no-build-isolation
pip install -r requirements.txt -r requirements-dev.txt matplotlib
export HIVEMIND_MEMORY_SHARING_STRATEGY=file_descriptor
```

> **Note:** `pip install -e .` currently installs **no dependencies**. `pyproject.toml`'s
> `[project]` table declares neither `dependencies` nor `optional-dependencies` and does
> not mark them `dynamic`, so setuptools ignores `setup.py`'s `install_requires` /
> `extras_require` (and `.[dev]` warns that the extra does not exist). Introduced by the
> PEP 517 change (#670); hence the explicit requirements install above. Unrelated to
> security, but worth a separate upstream fix.

```bash
python -m research.experiments.smoke_test                                # 30s, sanity check
python -m research.experiments.track_a_eclipse --live --censorship       # Track A
python -m research.experiments.track_b_amplification --live              # Track B
python -m research.experiments.track_c_learning_harm                     # Track C
python -m research.experiments.track_d_defenses                          # Track D
python -m research.experiments.tier2_validation                          # multi-process validation
```

All results land in `research/results/` as CSV (raw per-repetition rows) plus PNG
figures and a `.meta.json` recording parameters and seeds. Every script takes `--seed`.

**Resource note.** Each peer spawns its own `p2pd` daemon (~57 MB RSS), so a live swarm
is RAM-bound, not CPU-bound: 25 peers ≈ 2.6 s, 50 ≈ 7.8 s, 100 ≈ 30 s and 5.7 GB. On a
16 GB machine ~100 peers is the ceiling. Sweeps beyond that use the analytical layer,
which is pure XOR math over `node_id`s and scales to 10⁴ peers instantly.

## Layout

```
research/
  PLAN.md          living plan + dated progress log (design decisions and deviations)
  common/          swarm construction, Sybil identity crafting, metrics, plotting
  attacks/         eclipse_key.py (A), sybil_matchmaking.py (B), poison_averaging.py (C)
  defenses/        id_binding.py (D1), signed_declare.py (D2), robust_agg.py (D3), weight_bound.py (D4)
  experiments/     runnable drivers, one per track
  results/         CSV + PNG + run metadata
```

Environment used: Python 3.13.9, torch 2.14 (CPU), Linux. Hivemind's CI covers 3.9–3.12;
3.13 ran the DHT suite clean (`test_dht.py` 4/4) and `test_averaging.py` 20 passed /
2 skipped / 1 flaky-under-load failure that passes in isolation.
