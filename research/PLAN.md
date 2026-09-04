# Hivemind Network Security Research: Sybil / Eclipse on the DHT Control Plane → Learning Harm

## Context

Hivemind's control plane is a **custom pure-Python Kademlia DHT** (`hivemind/dht/`) layered on libp2p. It has the classic Kademlia weaknesses to **Sybil** and **Eclipse** attacks, and code inspection confirms these are *unmitigated by default*. The goal of this project is to (1) reproduce a known IPFS/Kademlia-style Sybil/Eclipse attack inside Hivemind and measure its cost, (2) quantify how a small **global** malicious fraction becomes a large **local** malicious fraction at a victim and how it decays under Hivemind's per-round group re-formation, (3) couple this network harm to learning harm (averaging distortion / targeted poisoning under plain averaging vs. Krum), and (4) prototype defenses. Everything is **simulation on one modest laptop, no real DL training**, aimed at an eventual upstream security contribution.

Decisions locked with the user:
- **Fidelity:** both tiers — in-process for scale sweeps + a small real-libp2p multi-process swarm to validate headline numbers.
- **Threat model:** free identities (worst case) — measure structural vulnerability vs. number of Sybils.
- **Learning task:** mock tensors — synthetic parameter/gradient vectors with a known ground-truth target; a lightweight synthetic *convex* objective (distributed least-squares / mean estimation) gives convergence curves without any deep-learning training.
- **Deliverable:** a `research/` tree with attack harness + metrics/plots + prototype defenses, framed for a possible upstream PR.

## Glossary (plain language — goes into `research/PLAN.md` and `research/README.md`)

**The two identities (the root of the vulnerability):**
- **libp2p PeerID** — the peer's network "phone number", derived as `hash(public key)`. Cryptographically verified by TLS; you cannot fake one without the private key.
- **node_id (DHTID)** — the peer's *address in the lookup table*, deciding which data it is responsible for storing. It is **random or freely self-chosen** and **announced, never verified**. The DHT routes by XOR "distance" between a data key and node_ids (nearest node_ids store that key). Because node_id is attacker-chosen, an attacker can place themselves right next to any chosen key and become one of its ~5 responsible replicas. The secure PeerID does not help, because routing uses the *unsecured* node_id. This decoupling is what the whole project studies.

**Attacks:**
- **Sybil attack** — one attacker pretends to be many independent peers. Identities are effectively free here, so one laptop can pose as hundreds.
- **Eclipse attack** — surrounding one victim (or one data key) with only attacker-controlled peers, so the victim's entire view of the network is fake.
- **Targeted poisoning** — instead of loudly wrecking everything, attackers make small stealthy nudges so the model fails on one specific thing while appearing normal elsewhere.

**Measurements:**
- **Global malicious fraction (`f_g`)** — the share of malicious peers across the *whole* swarm (e.g. 5 bad out of 100 = 5%).
- **Local malicious fraction (`f_l`)** — the share of malicious peers in the small group the *victim* actually averages with. The headline finding is amplification: a small `f_g` can produce a large `f_l`.
- **Per-round group re-formation** — peers do not average with fixed partners; every round they reshuffle into fresh small groups. **"Decay"** asks whether that reshuffling naturally dilutes an attacker's `f_l` over successive rounds, and how fast.
- **Network harm vs. learning harm** — *network harm* is damage at the plumbing level (attacker controls the victim's lookups/connections); *learning harm* is the end damage (slower convergence, lower accuracy, hidden bias). Connecting the two is the point of the project.
- **Distortion** — how far the group's average is pushed away from the honest value by attackers.
- **Structural vulnerability** — how weak the design is *by construction*, assuming the best case for the attacker (free identities); not "how hard in practice" but "how bad could it be".

**Method terms:**
- **Plain averaging** — Hivemind's current rule: a naive arithmetic mean that blindly trusts every submitted number, so one garbage value drags the result toward garbage.
- **Krum** — a known *robust* alternative: instead of averaging everything, pick the update most in agreement with the majority and discard outliers. Hivemind has no such defense today.
- **Fidelity / two tiers** — how realistic the simulation is. **Tier 1 (in-process)**: all peers inside one Python program — fast, hundreds of peers, laptop-friendly. **Tier 2 (real swarm)**: each peer a real process with the real networking stack — heavy, ~20–30 peers, used to confirm Tier-1 results are not an artifact of the shortcut.
- **Scale sweep** — rerunning an experiment while varying one knob (e.g. swarm size 50→500) to see how results scale.
- **Headline numbers** — the few key results worth an abstract, e.g. "5% Sybils → 60% local → 40% accuracy drop". These are what Tier 2 re-checks.
- **Synthetic convex objective / least-squares / ground-truth target** — instead of real AI training, a tiny *math* problem (like line-fitting) whose correct answer we know **in advance** (the ground-truth target). Peers "train" on it through the real averaging machinery, so we can measure exactly how far attackers pushed the result off. CPU-trivial, no GPU or datasets, yet still yields genuine convergence/accuracy curves.
- **Prototype defenses** — draft fixes written to *test whether they help* (measure attack → add defense → re-measure), not yet production-polished code.

## Key code findings (the vulnerability surface these experiments target)

Confirmed by reading the code — cite these in the writeup:

1. **Node ID is self-chosen and unbound to the crypto identity.** `self.node_id = node_id if node_id is not None else DHTID.generate()` (`hivemind/dht/node.py:171`). `DHTID.generate()` is 160-bit SHA1 of random bits (`hivemind/dht/routing.py:262`). `DHTNode.create(node_id=...)` lets a peer *pick* its ID. The libp2p `PeerID` **is** crypto-derived (`multihash(sha256(pubkey))`, `hivemind/p2p/p2p_daemon_bindings/datastructures.py:18`) and TLS-authenticated, but nothing binds `DHTID` to `PeerID`. → An attacker mints IDs arbitrarily close to any target key and becomes one of the `num_replicas=5` closest replicas. This is the Eclipse primitive.
2. **Node IDs are self-reported in RPCs and accepted verbatim** (`hivemind/dht/protocol.py:150`); `update_routing_table` also ingests unverified neighbor lists from `call_find` responses (`protocol.py:303-304`, `node.py:316`). The `validate=True` ping path checks reachability + clock skew only, and only at bootstrap.
3. **No Sybil/Eclipse defense, no rate limiting, no reputation.** Only `Blacklist` (`node.py:897`) — a *local, liveness-only* timeout list, never triggered by misbehavior. Bucket refresh is **off** by default (`refresh_timeout=None`). No PoW, no ID↔key binding, no bucket IP-diversity, no signed neighbor records.
4. **Averaging discovery records are unsigned.** `GroupKeyManager.declare_averager` writes `key=group_key, subkey=peer_id.to_bytes(), value=looking_for_group` with **no validator** (`hivemind/averaging/key_manager.py:46-68`). Peers discover groupmates via `get_averagers` (`dht.get(group_key, latest=True)`). → Sybils can flood the "looking for group" record and dominate a victim's candidate view. (Contrast: the *progress tracker* records ARE RSA-signed — `hivemind/optim/progress_tracker.py:108-110` — proving the signing machinery exists and is reusable.)
5. **Group re-formation each round.** After every assembled group, `update_key_on_group_assembled` rewrites `group_bits` via `random.Random(group_info.group_id).sample(...)` (`key_manager.py:94-105`) — this is the "periodic group re-formation" whose decay effect we measure. Note: with default `initial_group_bits=""` (what `hivemind.Optimizer` uses) all peers share one global bucket, so re-shuffling is driven by the leader race, not bucket bits — a nuance worth measuring both ways.
6. **Aggregation is plain unbounded weighted mean; no Byzantine resistance anywhere.** Whole-repo grep for `krum|byzantine|median|trimmed|robust` = 0 hits. Core rule is three lines: `accumulator.add_(tensor_part, alpha=weight); denominator += weight; ... accumulator.div_(denominator)` (`hivemind/averaging/partition.py:242-260`). The **weight travels on the wire and is applied unchecked** by the receiver (`hivemind/averaging/allreduce.py:339-347`); honest weight = self-claimed sample count (`hivemind/optim/grad_averager.py:196`). → "Krum vs. plain averaging" must be built by us.

## Reusable harness already in the repo (do not reinvent)

- `tests/test_utils/dht_swarms.py`:
  - `launch_dht_instances(n, **kwargs) -> List[DHT]` — star topology, one process + one `p2pd` per peer (Tier-2 realism, ≤~30 on a laptop).
  - `launch_star_shaped_swarm(n, **kwargs) -> List[DHTNode]` — async, all nodes in one process/event loop (cheapest, best for large in-process sweeps).
  - `launch_swarm_in_separate_processes(n_peers, n_sequential_peers, **kwargs)` — randomized (non-star) bootstrap; returns `(processes, {PeerID:DHTID}, swarm_maddrs)`. Passes `**kwargs` (incl. `node_id`) to `DHTNode.create` → the Sybil-ID injection point.
- `benchmarks/benchmark_dht.py` — `NodeKiller` (scheduled random churn via `--failure_rate`) + store/get latency & success measurement. Reuse for churn/decay.
- `benchmarks/benchmark_averaging.py` — N `DecentralizedAverager`s with `--target_group_size`, `--initial_group_bits`. Reuse for group-formation sweeps.
- `hivemind/averaging/partition.py::TensorPartReducer` — the aggregation seam to subclass for Krum/median.
- `hivemind/utils/limits.py::increase_file_limit()` — call before large swarms (avoids EMFILE).
- CLI `hivemind-dht` (`hivemind/hivemind_cli/run_dht.py`) with `--host_maddrs /ip4/127.0.0.1/tcp/0` for a manual localhost swarm; `report_status` already logs routing-table size + storage keys (`HIVEMIND_LOGLEVEL=DEBUG` dumps the full table).

## Environment setup (Step 0)

```bash
cd /home/jamesblack/Projects/hivemind
python -m venv .venv && source .venv/bin/activate
pip install torch --index-url https://download.pytorch.org/whl/cpu   # CPU-only, small
pip install grpcio-tools
pip install -e .[dev] --no-build-isolation                          # downloads hash-checked p2pd binary
export HIVEMIND_MEMORY_SHARING_STRATEGY=file_descriptor
cd tests && pytest test_dht.py -v   # smoke test the swarm harness works
```
Python ≥3.9. No GPU needed. Work on a branch: `git checkout -b research/net-security`.

**Resource budget (measured 2026-09-04):** `/home` has **28 GB free**; the project needs **~2.5–3 GB** — CPU-only torch ~1.0–1.5 GB (the CUDA build would be ~5 GB, hence the explicit CPU index-url), other deps ~500 MB, dev deps ~300 MB, `p2pd` binary ~30 MB, results < 100 MB. Use `--no-cache-dir` for the large wheels (pip cache is already 691 MB). **RAM (~15 GB) is the binding constraint, not disk** — each Tier-2 peer spawns its own `p2pd` process, which is why big sweeps stay in-process (Tier 1) and Tier 2 is capped at ~20–30 peers.

**First action of Step 0:** create `research/` and copy this plan to **`research/PLAN.md`** (in-repo, version-controlled, next to the code) including the Glossary above. That file — not the harness's internal plan path — becomes the living plan: it gets a `## Progress log` section at the bottom, updated as each track lands (date, what ran, headline numbers, what changed in the plan). The user can read/edit it directly at `/home/jamesblack/Projects/hivemind/research/PLAN.md`.

## Proposed `research/` layout

```
research/
  common/
    swarm.py         # thin wrappers over launch_* helpers + metrics collection + teardown
    sybil.py         # SybilController: mint N DHTNodes with chosen node_ids / behaviors
    metrics.py       # local-fraction, eclipse-success, replica-capture, decay curves
    plotting.py      # matplotlib (already a transitive dep) → PNGs
  attacks/
    eclipse_key.py       # Track A: eclipse a specific DHT key (record-level)
    sybil_matchmaking.py # Track B: flood averager discovery, measure local fraction + decay
    poison_averaging.py  # Track C: inject poisoned tensors/weights into a group
  defenses/
    id_binding.py    # D1: bind DHTID to PeerID
    signed_declare.py# D2: signed averager declarations
    robust_agg.py    # D3: Krum / coord-median TensorPartReducer
    weight_bound.py  # D4: bound/reconcile averaging weight
  experiments/       # runnable scripts producing CSV + PNG per track
  results/           # CSV + figures (gitignored or kept small)
  README.md          # threat model, how to reproduce, findings
```

## Track A — Eclipse cost on the DHT (record-level)

**Goal:** reproduce the IPFS/Kademlia targeted-eclipse: for a chosen key, capture all `num_replicas` closest slots so honest `get(key, latest=True)` returns attacker-controlled (or no) value. Measure cost = #Sybils and wall-clock time vs. swarm size.

**Method:**
- Build an honest swarm of size `H` (Tier-1: `launch_star_shaped_swarm`; validate a few points in Tier-2 `launch_swarm_in_separate_processes`).
- Compute `target = DHTID.generate(source=key)`; mint Sybils with `node_id` chosen so `xor_distance(node_id, target)` is minimal (e.g. flip low bits of `target`), all sharing one or few PeerIDs (free-identity model). Inject via `DHTNode.create(node_id=..., initial_peers=...)`.
- **Metrics:** fraction of the true `num_replicas=5` nearest that are Sybil (replica-capture); whether an honest reader's `get` resolves to the attacker's value; #Sybils needed for ≥50%/100% capture; time-to-eclipse. Sweep `H ∈ {50,100,200,500}` (in-process), `bucket_size`, `num_replicas`, and `refresh_timeout` on/off (show refresh raises cost).
- Reuse `benchmark_dht.py`'s store/get + `NodeKiller` to add churn and observe re-capture cost.

## Track B — Global→local amplification at the victim + decay under re-formation

**Goal:** the headline result. A small **global** malicious fraction `f_g` produces a large **local** malicious fraction `f_l` in the victim's averaging candidate set, and quantify decay under per-round group re-formation.

**Method:**
- Stand up an averaging swarm (`DecentralizedAverager` per DHT, as in `tests/test_averaging.py`), one honest **victim**, `H` honest peers, `S` Sybils with `f_g = S/(S+H+1)`.
- Sybils spam `GroupKeyManager.declare_averager` under the victim's `group_key` (many `peer_id` subkeys, aggressively early `expiration_time` to win the leader race — see `matchmaking.py:469-498`). Observe the victim's `get_averagers` result and the group it actually assembles.
- **Metrics per round:** `f_l` = (# Sybil groupmates)/(group size) at the victim; leader-capture rate; distribution of `f_l` over many rounds; **decay curve** = how `f_l` for a *fixed* victim evolves across consecutive re-formations (does shuffling dilute a targeted attacker, and how fast?). Compare `initial_group_bits=""` (single bucket) vs. non-empty (bucketed) since re-shuffle behavior differs (finding #5).
- Sweep `f_g ∈ {0.05,0.1,0.2,...}`, `target_group_size ∈ {4,8,16}`, `min_matchmaking_time`. Plot `f_l` vs `f_g` (amplification curve) and `f_l` vs round index (decay curve).

## Track C — Coupling to learning harm (mock tensors, synthetic convex task)

**Goal:** map local malicious fraction `f_l` → learning damage, under plain averaging vs. Krum, including **stealthy targeted subgroup poisoning**.

**Method (no DL training):**
- Model each honest peer as holding a parameter/gradient vector for a synthetic **convex** objective (distributed least-squares / mean-estimation) with a known optimum `x*` — iterate: local step + all-reduce averaging. This yields real convergence/accuracy curves while remaining pure tensor math on CPU.
- Attacks: (a) **untargeted** — Sybils send large/opposite/NaN vectors and/or inflated `weight` (unchecked per finding #6) to blow up or stall convergence; (b) **stealthy targeted subgroup poisoning** — Sybils nudge a specific coordinate/subgroup (e.g. a target "class" direction) with small per-round perturbations that pass casual inspection.
- **Metrics:** distance-to-`x*` / loss vs. round (convergence), a targeted-accuracy proxy for the poisoned subgroup, and averaging L2-drift, all **as a function of `f_l`** from Track B. Overlay **plain averaging vs. Krum** (Track D3).
- Implement at the `AllReduceRunner`/`TensorPartReducer` seam, or a standalone averaging simulator that faithfully re-implements the `add_/div_` rule — the latter is lighter and sufficient for the aggregation-math comparison; use the real `DecentralizedAverager` for a validation subset.

## Track D — Prototype defenses, then re-run A/B/C to show reduction

- **D1 ID↔PeerID binding** (`research/defenses/id_binding.py`): require `node_id == DHTID.generate(source=peer_id.to_bytes())` (or an S/Kademlia-style crypto puzzle) and enforce it in the routing-table ingestion paths (`protocol.py` `rpc_ping`/`update_routing_table`, neighbor-list ingestion). Re-run Track A → cost should jump (attacker can no longer choose IDs freely; Sybil cost tied to keypair grinding).
- **D2 Signed averager declarations** (`research/defenses/signed_declare.py`): reuse `RSASignatureValidator` + `SchemaValidator` with the `BytesWithPublicKey` idiom (`schema.py:179`) so a peer can only declare *itself* under a `[owner:...]`-tagged subkey — mirrors what `progress_tracker.py:108-110` already does for progress records. Re-run Track B → Sybil flooding of others' declarations blocked.
- **D3 Robust aggregation** (`research/defenses/robust_agg.py`): implement **Krum** (and optionally coordinate-wise median / trimmed mean) as an alternative to `TensorPartReducer`'s weighted mean. Re-run Track C → damage vs. `f_l` should flatten until Krum's tolerance (`f_l < (n-2)/2n`) breaks.
- **D4 Weight bounding** (`research/defenses/weight_bound.py`): enforce receiver-side `weight >= 0 && isfinite && weight <= cap`, and/or reconcile the wire `weight` against the peer's *signed* progress record. Cheap mitigation for the unchecked-weight finding.

Frame D1/D2/D4 as small, review-sized upstream patches; D3 as an optional pluggable aggregator.

## Metrics, outputs, reproducibility

- Every experiment script: fixed `--seed`, writes a CSV to `research/results/`, and a PNG via `research/common/plotting.py`. Headline figures: (A) replica-capture & time-to-eclipse vs #Sybils/swarm-size; (B) `f_l` vs `f_g` amplification + `f_l` vs round decay; (C) convergence/targeted-accuracy vs `f_l`, plain-avg vs Krum; (D) before/after defense on the same axes.
- `research/README.md`: threat model, exact commands, environment, and a findings summary suitable for an issue/PR discussion.

## Verification

1. **Harness sanity:** `cd tests && pytest test_dht.py test_averaging.py -k "not slow" -v` passes; a 10-node `launch_star_shaped_swarm` stores/gets correctly.
2. **Attack validity (positive control):** Track A demonstrably flips an honest `get(key, latest=True)`; Track B shows `f_l > f_g` for a targeted victim; Track C shows measurable convergence degradation rising with `f_l`.
3. **Baseline (negative control):** with 0 Sybils, `f_l ≈ 0`, convergence matches the clean run, and defenses don't regress it.
4. **Defense efficacy:** re-running A/B/C with D1/D2/D3/D4 enabled shows the expected cost increase / damage reduction on identical seeds and axes.
5. **Tier-2 cross-check:** reproduce ≥1 headline point from each track on a real multi-process swarm (`launch_swarm_in_separate_processes`, ≤~20–30 nodes) to confirm the in-process result holds with real libp2p routing.
6. **No upstream regressions:** existing `tests/` still pass; all new code lives under `research/` (attacks) and behind opt-in flags (defenses), respecting ruff (`ruff==0.11.2`, line-length 119).

## Notes / scope guards

- This is authorized, self-hosted, localhost security research; no attacks leave the machine and no public Hivemind/IPFS network is targeted.
- Start Tier-1/in-process to iterate fast; only promote validated results to Tier-2. Cap real-swarm size to what the laptop tolerates (p2pd process + fds per peer).

---

# Progress log

Newest entries at the bottom. Each entry: date, what ran, headline numbers, and any change to the plan above.

## 2026-09-04 — Step 0: environment + scaffolding

- Branched `research/net-security` off `master` (`4bd43b7`).
- Created `research/` tree (`common/`, `attacks/`, `defenses/`, `experiments/`, `results/`) and copied this plan in as the living document.
- **Deviation from plan:** only Python 3.13/3.14 are available on this machine; Hivemind's CI tops out at 3.12. Proceeding on **3.13** — flag here if any dependency or runtime breakage traces back to that.
- Venv at `.venv/`, CPU-only torch installing.

- Upstream bug found while installing: `pyproject.toml`'s `[project]` table declares neither `dependencies` nor
  `optional-dependencies`, and does not mark them `dynamic`, so setuptools ignores `setup.py`'s `install_requires`
  /`extras_require`. Result: **`pip install -e .` installs no dependencies at all**, and `.[dev]` warns
  "does not provide the extra 'dev'". Introduced by the PEP 517 commit (#670). Worked around with
  `pip install -r requirements.txt -r requirements-dev.txt`. Candidate upstream fix, unrelated to security.
- Env verified on Python 3.13: `pytest tests/test_dht.py` 4/4 pass; `test_averaging.py` 20 passed / 2 skipped /
  1 failed, where the failure (`test_averaging_trigger`) passes in isolation — flaky timing under parallel load,
  not a 3.13 issue.

## 2026-09-04 — Harness built + first attack confirmation

`research/common/{sybil,swarm,metrics,plotting}.py` written; `research/experiments/smoke_test.py` runs green.

**First empirical result (20 honest + 5 Sybils, `num_replicas=5`):**
- `DHTNode.create(node_id=...)` honours attacker-chosen routing addresses — injection confirmed.
- Structural replica capture: **5/5**. Live `find_nearest_nodes` returned **5 peers, all 5 Sybil** — a complete
  eclipse of the target key using 5 identities against 20 honest peers.
- The asymmetry is stark: crafted Sybils sit at XOR distance 0–4 from the target, while the nearest *honest*
  peer sits at ~8.7e45. Choosing your own node_id is not a marginal advantage, it is decisive.

**Deviation from plan — Tier-1 is not as cheap as assumed.** Every `DHTNode.create` starts its own `p2pd`
child process (~57 MB RSS), so an "in-process" swarm is only in-process on the Python side:
25 nodes → 2.6 s, 50 → 7.8 s, **100 → 30 s and 5.7 GB**. On this 15 GB laptop ~100–120 live peers is the ceiling,
not the "hundreds" the plan assumed. Track A is therefore split into two layers:
- **A0 analytical** — pure node_id/XOR math over the placement rule, no processes. Scales to 10⁴ peers instantly
  and gives the theory curve.
- **A1 live** — real DHT nodes at 20–100 peers, confirming the analytical curve matches live lookups and real
  store placement.
This is a strictly better design than the original single-layer sweep: the analytical layer supplies scale, the
live layer supplies validity.

## 2026-09-04 — Track A complete: eclipsing a DHT record

Code: `research/attacks/eclipse_key.py`, driver `research/experiments/track_a_eclipse.py`.
Results: `track_a_{analytical,cost,live,censorship}.csv` + 3 PNGs in `research/results/`.

**A0 analytical (40 repeats/point, swarm sizes 50 … 10 000).** Honest peers at random node_ids, attacker at
XOR distance 0,1,2,… from the target key:

| honest swarm | identities for a *total* eclipse | nearest honest peer's XOR distance |
|---|---|---|
| 50 | 5.00 ± 0.00 | ~2.6e46 |
| 1 000 | 5.00 ± 0.00 | ~1.3e45 |
| 10 000 | 5.00 ± 0.00 | ~1.3e44 |

**Headline: the cost of a total eclipse is exactly `num_replicas` (5) identities, and it does not grow with the
network — 10 000 honest peers are no harder to defeat than 50, with zero variance.** Scale offers no protection.
The reason is visible in the third column: honest node_ids are uniform over a 160-bit space, so the nearest honest
peer to any given key sits ~10⁴⁴–10⁴⁶ away, while an attacker who picks its own address sits at distance 0.

**A1 live (real swarm, 40 honest peers).** Theory matched reality exactly — structural = live-lookup = actual
store placement at every point (0.20 / 0.60 / 1.00 for 1 / 3 / 5 Sybils). At 5 Sybils an honest peer's `store`
landed on **5 attacker nodes and 0 honest nodes**.

**A2 censorship, with a control that isolates the bug (n=8 per cell).** Attacker takes custody, then stops
serving the record:

| condition | custody | censorship success |
|---|---|---|
| crafted node_ids, caching off | 1.00 | **100%** |
| crafted node_ids, caching on (default) | 1.00 | 50% |
| random node_ids (control), caching off | 0.15 | 0% |
| random node_ids (control), caching on | 0.25 | 0% |

The control matters: an attacker with the *same five identities* but random node_ids censors **nothing**. The
damage is attributable specifically to self-chosen routing addresses, not to owning peers.

**Incidental finding:** Hivemind's opportunistic caching (`cache_locally`, `cache_nearest=1`, `cache_on_store`)
halves censorship success (100% → 50%). This is a side effect of a *performance* feature, not a security control —
caches expire and the attacker still holds every authoritative replica — but it is a real, measurable partial
mitigation and is reported as such rather than assumed away.

## 2026-09-04 — Track B complete: global → local amplification, and the decay question answered

Code: `research/attacks/sybil_matchmaking.py`, driver `research/experiments/track_b_amplification.py`.
Results: `track_b_{amplification,decay,live}.csv` + 3 PNGs.

**The matchmaking Sybil is far cheaper than the plan assumed.** Averaging discovery records are unsigned
(`declare_averager`, `key_manager.py:46`) and the subkey is just `peer_id.to_bytes()` with nothing checking the
writer owns it. So **a fabricated averager identity costs one DHT write** — no keypair, no process, no
connection. Measured live: *one* process registered 400 fabricated averagers. The plan's "one identity ≈ one
peer" cost model understated this by orders of magnitude, and Track B is scoped accordingly: the attacker is a
**single process** in every experiment below.

**B1 amplification (live, 12 honest peers + 1 attacker process, f_global = 0.077).**

| fabricated declarations | victim's candidates | fake | f_local | amplification | leader captured |
|---|---|---|---|---|---|
| 0 | 12 | 0 | 0.000 | — | no |
| 1 | 13 | 1 | 0.077 | ×1.0 | **yes** |
| 25 | 37 | 25 | 0.676 | ×8.8 | yes |
| 400 | 412 | 400 | 0.971 | ×12.6 | yes |

**Leadership costs exactly one fabricated declaration.** Leaders are chosen by earliest declared expiration
(`matchmaking.py:478`), a self-chosen value, and the leader picks the group's membership and `group_id`.

Simulated at 100 honest peers (f_global = 0.0099, one attacker process), 100 fabricated declarations give
f_local = 0.50 / 0.80 / 0.94 / 0.98 for 1 / 4 / 16 / 64 buckets — **amplification ×50 to ×99**. Two multipliers
compound: identity multiplication (writes are free) and targeting (honest peers scatter across buckets while the
attacker concentrates in the victim's).

**B2 decay — this answers the project's core question, and the answer is "no decay".** Re-bucketing after each
group (`update_key_on_group_assembled`, `key_manager.py:94`) is seeded on `group_id` and computed from values
**every group member already has**. `predict_next_group_bits` reproduces Hivemind's own re-bucketing
**144/144** across group ids, target sizes and bit-widths. So an attacker who shares *one* group with the victim
computes all its future buckets:

| attacker | f_local round 0 | f_local round 19 |
|---|---|---|
| naive (floods a fixed bucket) | 0.893 | **0.046** (decays as the design suggests) |
| tracking (predicts victim's bucket) | 0.889 | **0.893** (no decay at all) |

Periodic group re-formation is unpredictable to an outsider but fully transparent to a participant. It sheds only
an attacker who declines to use public information. **Any defense that leans on re-formation for dilution needs
the shuffle to depend on a secret the attacker does not hold.**

