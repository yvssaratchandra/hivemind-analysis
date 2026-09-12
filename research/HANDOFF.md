# Handoff — read this first

Everything a new session needs: what the project is, where it stands, what is known, and what to do
next. Written to be read without any prior conversation.

Branch `research/net-security` · remote `origin` (the fork) · last updated 2026-09-11

---

## 1. The project

A security study of Hivemind's peer-discovery and averaging layer. The brief:

> Deploy a small Hivemind / libp2p swarm locally training a toy model, then: (1) reproduce a known
> eclipse against a victim node and measure its cost (number of Sybil peers, time); (2) measure how a
> small global fraction of malicious peers becomes a large local fraction at the victim, and how it
> decays under Hivemind's periodic group re-formation; (3) couple network control to learning harm:
> convergence degradation and stealthy targeted poisoning vs. the victim's local malicious fraction,
> under plain averaging and robust aggregation (Krum).

Two quantities run through all of it:

- **α** = attacker-controlled peers / all peers (global fraction)
- **β** = attacker-controlled peers / members of one averaging group (local fraction)

The research question: is β something an attacker *has* (β ≈ α, as the Byzantine-ML literature assumes)
or something an attacker *chooses*?

**Standing constraint: never modify anything under `hivemind/`.** All work is additive under
`research/`, via supported hooks or subclasses.

### The three architectural facts everything rests on

**Peers carry two identities.** The libp2p **PeerID** is `multihash(sha256(pubkey))`, TLS-authenticated,
unforgeable. The Kademlia **`node_id`** — which decides *which DHT records a peer stores* — is
self-chosen (`dht/node.py:171`) and accepted verbatim off the wire (`dht/protocol.py:150`). Record
custody is decided by the unverified identity.

**Group formation is a race, not a draw.** Each peer writes an advertisement with a self-chosen
*deadline* to a DHT key (`averaging/key_manager.py:46`); every peer reads the same key, sorts by
`(deadline, peer_id)`, and follows the earliest — otherwise it waits to be contacted and leads
(`matchmaking.py:478-481`). The leader then admits followers in arrival order (`matchmaking.py:273`),
and `_check_reasons_to_reject` (`matchmaking.py:341-368`) runs on the *leader's own machine* with the
requester's authenticated PeerID in hand.

**Two different thresholds decide what an attack costs, and both are the victim's own DHT settings —
not properties of the network.**

| | parameter | default | what it buys |
|---|---|---|---|
| **Custody** of a record | `num_replicas` | 5 | you are one of the peers that *stores* the key (§4.1) |
| **Control of a lookup** | `bucket_size` | 20 | you are *every* peer the victim asks (§4.6) |

Custody is not control. A lookup seeds its traversal from the `bucket_size` nodes nearest the key in the
victim's routing table (`node.py:629-633`), so holding the 5 replicas still leaves 15 honest peers being
asked, and `add_candidate` merges their answers back in (`node.py:834-837`).

**`bucket_size` is not the size of the routing table.** It is Kademlia's *k*, the capacity of one
k-bucket; the table is many buckets and splits as it fills (`routing.py:31-33`). The same number is
*reused* as the `k` passed to `get_nearest_neighbors`, which walks outward from the bucket nearest the
key across the whole table until it has `k` candidates. Only that second meaning matters here: it is how
many peers a lookup asks. Confusing the two cost this project a falsified hypothesis — see §4.6.

Neither threshold scales with swarm size, which is why the attacks in §4.1 and §4.6 cost the same at
10 000 peers as at 50.

---

## 2. Status

| Brief item | Status |
|---|---|
| (1) eclipse cost | **done** — §4.1 |
| (2) α → β | **done** at three layers — §4.2, §4.3, §4.5, §4.6 (group capture is total at Hivemind's defaults, given ~`bucket_size` identities) |
| (2b) decay under re-formation | **done** — §4.4 |
| (3) β → learning harm, plain vs Krum | **done** — §4.11 |

### Code

| File | What it does |
|---|---|
| `step1_baseline.py` | N peers, real DHT + `hivemind.Optimizer`, linear regression with known optimum `w*`. The clean baseline. |
| `step2_groups.py` | Same, plus records which peers shared a group each round. The control for β. |
| `analyse_groups.py` | Tests recorded membership against a uniform-random-partition null, by Monte Carlo. |
| `step3_eclipse.py` | Eclipse cost as arithmetic over node_ids. No processes. |
| `step3_eclipse_live.py` | The same on a live swarm of real `DHTNode`s. |
| `step3_candidates.py` | Attacker share of the victim's candidate list: flooding vs eclipse. |
| `step3_capture.py` | Malicious leader admitting only the victim and its own peers. |
| `step4_reformation.py` | Does re-formation shed the attacker? Needs sharding switched on. |
| `step5_selective.py` | Lying to the victim on read instead of censoring everyone on write. |
| `step6_isolation.py` | Does list capture win the *group*? Adds DHT-only Sybils holding the key's replicas. |
| `step7_poison.py` | Brief item 3: poisoning the all-reduce, plain averaging vs coordinate median. |

Results are CSVs in `research/results/`, tracked in git, named for their configuration:
`step2_groups_16x8.csv` = 16 peers at group size 8; `_b` = independent repeat;
`step6_isolation_<arm>_k<k>_n<n>_<cache>[_b<bucket_size>][_r<repeat>].csv`, where `<cache>` is
`cached`, `nocache`, or `no` plus the knobs disabled, and the bucket and repeat suffixes appear only
when they differ from the default;
`step7_poison_<attack>_w<weight>_<agg>_b<beta>_s<seed>.csv`.

### Canonical baseline

`step2_groups.py --num-peers 8 --group-size 8 --max-epoch 20` → **0.1492** distance to `w*` at epoch 20,
spread across peers 0.0000, 20 rounds, no missing epochs, ~104 s. **0.1492 is what "unbroken" means**;
(An earlier machine recorded 0.1479 for the same config; the 0.9 % gap is run-to-run variation in how
many local steps land per epoch, not a behaviour change.)

**It is not comparable to every experiment.** `step2` leaves the gradient averager's
`target_group_size` unset, so its groups close on deadline rather than filling. Any script that *binds*
it — `step3_capture`, `step6`, `step7` — converges ~4x worse with no attacker present (0.148 unbound vs
0.575 bound, 8 peers at group size 8). **Those scripts must be compared against their own no-attack
arm**, never against 0.1492. See §4.11.

---

## 3. Two traps that cost the most time

### Trap 1 — a swarm that looks like it is training together, but is not

Gradient averaging is skipped entirely unless the progress tracker sees more than one peer
(`optim/optimizer.py:526`). This model is small enough that an unpaced step takes microseconds, so a
peer reaches `target_batch_size` alone long before the DHT propagates anyone else's progress.
`num_peers` stays 0, no averaging happens, **and the loss still goes down** because every peer solves
the same regression independently. This produced a clean-looking but meaningless convergence curve once.

Two guards, both in the code — **do not remove them**:

1. **Pace the loop.** `--batch-time` (default 0.2 s) plus `target_batch_size = 100 × num_peers` gives
   ~25 steps ≈ 5 s per epoch, comparable to `matchmaking_time`. `benchmarks/benchmark_optimizer.py`
   paces for the same reason.
2. **Record `peers_seen`** (`optimizer.tracker.global_progress.num_peers`) on every row; the scripts
   warn loudly if it never exceeds 1.

Quick check: with **one group per round**, real averaging gives a spread of exactly **0.0000** across
peers. With more than one group the spread is legitimately non-zero — it peaks in the first few epochs
(up to ~0.28) and decays two orders of magnitude by epoch 20. In that case use `peers_seen` and the
*direction of travel*: real averaging shrinks the spread, independent training grows it.

### Trap 2 — Python 3.14 hangs instead of failing

Hivemind averagers are `mp.Process` subclasses holding a DHT, asyncio primitives and shared-memory
tensors. **Python 3.14 changed the default start method on Linux from `fork` to `forkserver`**, which
pickles the process object — and an averager does not pickle. Every peer dies inside
`Optimizer.__init__`, and the parent waits forever, so you see a hang with the real traceback buried in
the child's output. Every script calls `mp.set_start_method("fork", force=True)`. Any new script needs
it. **If a swarm fails to start, read the child traceback first.**

---

## 4. Findings

### 4.1 Eclipsing a record costs five identities, at any network size

`step3_eclipse.py` (arithmetic, 40 swarms per cell) and `step3_eclipse_live.py` (real `DHTNode`s).

| honest swarm | crafted node_ids for total eclipse | capture with 5 *random* node_ids |
|---|---|---|
| 50 | 5 | 0.09 |
| 1 000 | 5 | 0.01 |
| 10 000 | 5 | 0.00 |

**Cost is exactly `num_replicas`, zero variance, at every size.** Scale offers no protection: honest
`node_id`s are uniform over 160 bits, so at 10 000 peers the nearest honest address sits ~2^146 away in
XOR distance while a crafted one sits at 0–4. The random-node_id control captures at chance and never
reaches a total eclipse — so the damage is attributable to *address choice*, not to owning peers.

A live swarm agrees exactly: in **all 30 rows** the record landed on precisely the theoretical replica
set. Cost in time is **~0.1 s per Sybil**, near-perfectly linear; a total eclipse takes ~0.5 s.

**Custody is not yet censorship.** These nodes store and serve honestly; they merely own the addresses.
What the eclipse establishes is that every replica sits on attacker hardware.

### 4.2 Five identities take the whole candidate list — after one advertisement lifetime

`step3_candidates.py`, live swarms, through the real `GroupKeyManager`.

| arm | attacker processes | α | share of victim's list | honest peers still visible |
|---|---|---|---|---|
| honest control | 0 | 0.000 | 0.00 | 32/32 |
| flood, 128 writes | 1 | 0.029 | 0.80 | 32/32 |
| **eclipse, 5 nodes** | 5 | 0.132 | **1.00** | **0/32** |

**Flooding is cheap but linear** — an advertisement is an unsigned record whose subkey nobody verifies,
so one process writes thousands, but share is k/(k+honest) and it only *dilutes*. **The eclipse
removes**: five crafted node_ids own every replica, and `record_validator` (a supported
`DHTNode.create` argument, gating writes at `protocol.py:260`) lets each refuse honest advertisements.

**Qualified by §4.7:** this holds for a key with no live prior copies. Measured cold here.

**A free detection signal, discarded.** All 32 honest peers got `False` back from their own
`declare_averager` — the DHT told each one its advertisement was refused everywhere. Nothing reads it:
`_declare_averager_periodically` (`matchmaking.py:535,544`) awaits the call and throws the result away,
and no other caller exists.

### 4.3 The admission filter is exact; leadership is the bottleneck

`step3_capture.py`, 16 peers, group size 8, peer 0 the victim.

| arm | k | α | led captures | β when captured | mean β |
|---|---|---|---|---|---|
| honest | 1 | 0.062 | 0 % | — | 0.000 |
| malicious (tuned) | 1 | 0.062 | **57 %** | **0.500** | **0.315** |
| malicious (tuned) | 2 | 0.125 | 50 % | 0.667 | 0.508 |
| malicious (tuned) | 4 | 0.250 | 33 % | 0.800 | 0.555 |

**When the attacker leads, admission control is total**: β is exactly k/(k+1), no variance. One
identity beside the victim is half the group — an **8× amplification over α**. So β is *conditionally*
chosen: total control when leading, nothing when not.

**Winning the election needed one non-obvious fix.** `Optimizer` gates pre-scheduling on
`estimated_next_update_time - now <= matchmaking_time` (`optimizer.py:562`) *and* passes that same value
to the averager as `min_matchmaking_time` (`optimizer.py:307`), which sets the declared deadline. The
gate fires at `E - t` and the deadline becomes `(E - t) + t = E`: **every peer declares the shared
epoch-end estimate whatever it configures.** Overriding only the averager's copy — advertise early
(large `Optimizer matchmaking_time`), declare a short rolling deadline (small averager
`min_matchmaking_time`) — is the only configuration that declares earlier than `E`. Declare times
0.5–1.5 s give 57–67 % capture; 3.0 s collapses to 5 % because the attacker's own group closes before
the victim arrives.

**More attackers made capture rarer** (57 % → 33 %): with several Sybils admitting each other the leader
reaches `min_group_size = 2` on its own peers and assembles before the victim arrives. The attacker
therefore holds the group open until the victim has joined.

### 4.4 Re-formation sheds a naive attacker, not a tracking one

`step4_reformation.py`, 16 peers, group size 4, 4 buckets, sharding **switched on** (the default has
none — see §4.8).

| arm | shared victim's group | mean β | victim's next bucket predicted |
|---|---|---|---|
| none | 0/6 | 0.000 | — |
| naive (freezes its bucket) | 1/5 | 0.067 | — |
| **tracking** | **5/7** | **0.179** | **5/5 exact** |

The tracking attacker locks on at round 2 and is in every subsequent group, following the victim across
bucket changes.

**The root cause is transparency to participants, not predictability.** `group_id = DHTID.generate()`
hashes 255 random bits (`routing.py:270`) — genuinely unpredictable from outside. What breaks it is that
every input to the reshuffle is handed to every member in the leader's broadcast, so a peer that shared
**one** group computes the victim's bucket exactly, forever. The implied fix: seed the shuffle on a
value contributed by members and revealed only after placement.

**Note the mechanism is stronger than "randomise" when it runs.** `rng.sample(range(num_buckets),
group_size)` returns *distinct* buckets, so groupmates are **forced apart** and cannot meet next round.
It defends against honest drift; it does not defend against an attacker that computes where the victim
is going.

### 4.5 Lying on read: same capture, no detection signal, no collateral

`step5_selective.py`. `rpc_find` receives `context: P2PContext` (`protocol.py:333`), so a responder
knows the authenticated PeerID of the reader before replying. An attacker owning the replicas can store
everything honestly, serve everyone honestly, and filter only the victim's reads.

| arm | honest declarations accepted | victim sees | bystander sees |
|---|---|---|---|
| honest | 12/12 | 12 honest | 12 honest |
| censor (§4.2) | **0/12** | 0 honest, 1 attacker | **0 honest** — swarm broken |
| **selective** | **12/12** | **0 honest, 1 attacker** | **12 honest** — untouched |

Same capture; only `censor` also fires the detection signal and destroys discovery swarm-wide. Cost is
unchanged at five identities, because dictionaries from multiple responders are **merged**
(`node.py:834-837`) so every replica holder must lie the same way.

### 4.6 Owning the list is necessary but not sufficient; blinding works at Hivemind's defaults

`step6_isolation.py`, 8 peers, group size 4, training attackers plus DHT-only Sybils holding the key's
replicas. (α counts *training* peers — only those submit gradients. DHT-only Sybils cost a process and
no compute.)

**Filtering alone converts list capture into group capture only partly** — 0/12 → 4–5/13 rounds, β
0.104 → 0.250 — because capture also needs the attacker to *win leadership*. Worse, **filtering the
victim's reads makes it more likely to lead** (0/12 → 5/12): starved of candidates it finds nothing
beating its own deadline, so it waits to be contacted. The attack partly defeats itself.

**`chase` removes the race instead of winning it.** Serve the victim an *empty* list, so it leads by
construction; the attackers declare late deadlines and simply join it; honest peers are hidden from the
victim's key so they cannot.

**The cost is `bucket_size` identities, not `num_replicas`.** At Hivemind's default `bucket_size = 20`,
**caching fully enabled**:

| crafted Sybils | victim led | captured | mean β | honest in victim's group | filtered responses |
|---|---|---|---|---|---|
| 5 | 15/16 | **0/16** | 0.500 | 16/16 | 3.9k |
| **25** | 10/10 | **10/10** | **0.733** | **0/10** | **81k** |
| **40** | 11/11 | **11/11** | **0.742** | **0/11** | 39k |

A lookup seeds its traversal from the `bucket_size` nodes nearest the key in the victim's routing table
(`node.py:629-633`, `bucket_size` default 20 at `node.py:104`). Five crafted identities win *custody* —
they are the `num_replicas = 5` nearest, so they hold the record (§4.1) — but they are only 5 of the 20
seeds. The other 15 are honest, they answer truthfully, and `add_candidate` merges their answer back in
(`node.py:834-837`). Minting 20+ identities makes the whole seed set attacker-owned and the filtering
becomes total. The filtered-response count rising from 3.9k to 81k is that made visible: every lookup
now goes to the attacker and nobody else.

**This costs essentially nothing.** §4.1 measured crafted identities at ~0.1 s each and no compute, so
the step from 5 to 25 is about two seconds of minting. The attack does not need a caching-free network,
a shrunken routing table, or a small swarm.

**Two hypotheses were falsified getting here. Do not re-run them.**

- *"Caching defeats it."* This section originally said so: `rpc_find` also answers from `self.cache`
  (`protocol.py:344`) and results are merged, so a cached honest copy undoes the filtering. Caching is
  real but it is not the barrier — at 25 Sybils the attack is total **with caching on**. Caching has
  still never been isolated on its own; `--no-cache-locally` / `--no-cache-nearest` /
  `--no-cache-on-store` exist for that.
- *"The swarm must be larger than the routing table."* Shrinking `bucket_size` to 4 restores the attack
  (7/11 captured, 70k filtered) which looked like confirmation. It is not: padding the swarm with
  honest DHT-only nodes to **14 → 26 → 54 → 104 nodes** at `bucket_size 20` left capture at 0–1/15
  throughout, with filtered counts flat at 3–13k. Idle filler nodes never enter the victim's routing
  table, so from the victim's point of view the swarm never grew. Total node count is irrelevant; what
  matters is who occupies the seed set. `--filler-nodes` exists and is not worth re-running for this.

The `bucket_size` ladder at 5 Sybils locates the boundary exactly where the seed-set account predicts —
between 4 and 8, i.e. at the number of crafted identities, **not** near the 14-node swarm size:

| bucket size | captured | filtered |
|---|---|---|
| 4 | **7/11** | 70k |
| 8 | 0/15 | 5.4k |
| 12 | 0/15 | 4.7k |
| 20 | 0/16 | 3.9k |

**The honest control is healthy at every configuration** and is what rules out "the swarm broke": 0
captures, an honest peer in the victim's group every round, all 8 training peers visible throughout, at
bucket sizes 4 and 20 and at 104 nodes.

**The principle across §4.5, §4.6 and §4.7, corrected: an attacker with custody can *add* to what a
victim sees but cannot reliably *remove* — until it owns the victim's whole seed set.** Removal needs
every responder the victim asks to cooperate. Custody (5 identities) is not enough, because the victim
asks 20 peers, not 5. Owning all 20 is the real precondition, and it is cheap.

*The attack backfires when under-resourced:* at 5 Sybils `chase` gives β at or below the honest control,
because the attacker declares late deadlines to follow and so participates less than an honest peer
would. That is the arm failing, not the mechanism.

### 4.7 History delays the discovery attacks by one record lifetime; it does not defeat them

`step5_selective.py --warmup W`. §4.2 and §4.5 measured a **cold** key.

| warm-up state | censor: victim sees | selective: victim sees |
|---|---|---|
| cold (W=0) | 0 honest | 0 honest |
| **warm, records live** | **12 honest — fails** | **12 honest — fails** |
| warm, records expired (5 s lifetime, 8 s settle) | 0 honest | 0 honest |

One warm-up round takes capture from 1.00 to 0.00. **The mechanism is residual storage, not caching** —
identical with caching fully disabled. Honest peers that stored the record before the attacker arrived
keep it until expiry and remain reachable.

**But the window is one record lifetime.** Averaging advertisements expire at the peer's matchmaking
deadline — seconds, not minutes — so once pre-attack copies lapse the attacker owns every replica that
receives the refresh. Net: capture holds **from ~one advertisement lifetime after the Sybils arrive**,
not immediately. For a defender that is the detection window.

### 4.8 Membership does not mix, and nothing in the protocol makes it

`analyse_groups.py` compares recorded membership against a uniform-random-partition null over 20 000
Monte Carlo draws.

| run | groups/round | co-occurrence sd | null sd | P(null ≥ observed) | distinct partitions |
|---|---|---|---|---|---|
| 16 peers, group 8 (×2) | 2 | 5.53 / 5.45 | 2.23 | < 5e-5 | 13/20, 10/20 |
| 16 peers, group 4 | 4 | 4.96 | 1.78 | < 5e-5 | 15/20 |
| 24 peers, group 8 (×2) | 3 | 1.96 / 2.01 | 2.06 | 0.85 / 0.70 | 20/20 |
| 24 peers, group 12 | 2 | 2.24 | 2.23 | 0.46 | 20/20 |

**At 16 peers the swarm froze into cliques in all three runs** — one pair shared a group 20/20 rounds
(p ≈ 3e-7 for a given pair). **At 24 peers, same code, indistinguishable from a fair draw.** The
distinguishing variable is peer count, not group count: 24-at-group-12 gives the same two groups per
round and mixes fairly.

**Scope — read this before citing it.** These runs used `hivemind.Optimizer` defaults, where
`initial_group_bits = ""` makes the reshuffle a **no-op** (`key_manager.py:104` discards its own output
when the bits are empty). So this measures a swarm with *no redistribution running*. With sharding
enabled, groupmates are forced into distinct buckets (§4.4) and cliques of this kind cannot form the
same way. **The finding is about the shipped default, not about a sharded deployment.**

How well-founded is "the shipped default is what deployments run"? Three levels of evidence:

- **Verified in code, and the strongest of the three because it does not depend on surveying anyone:**
  `hivemind.Optimizer` exposes **no parameter** for `initial_group_bits` — nothing under
  `hivemind/optim/` references it — and `averager_opts` cannot substitute, because it reaches only the
  *state* averager and never `_make_gradient_averager` (see §5). So an Optimizer-based deployment
  **cannot** enable sharding on the averager whose groups β is computed from without reaching past the
  public API with a custom `grad_averager_factory`.
- **Verified by inspection.** In-repo, the only code setting it is `benchmarks/benchmark_averaging.py`
  (which drives `DecentralizedAverager` directly, not `Optimizer`) and `tests/test_averaging.py`. The
  ALBERT example — the repo's only `Optimizer`-based example — passes `averager_opts` whose
  `AveragerArguments` (`examples/albert/arguments.py:47`) contains *only* `target_group_size`.
  Externally, `mryab/collaborative-training/run_trainer.py` — the DeDLOC / sahajBERT public run, by
  hivemind's lead author — contains no occurrence of `initial_group_bits` at all.
- **Not checked.** Petals: GitHub code search needs a login and the public docs do not answer it.
  Petals is inference plus *client-side* fine-tuning, so it may not exercise `hivemind.Optimizer`'s
  gradient averaging at all — but that is a **conjecture**, not evidence. Private deployments are
  likewise unknown. If either turns out to configure a custom `grad_averager_factory`, this section's
  scope narrows.

**Why it matters for β.** The assumption that β ≈ α rests on membership being a fresh random draw.
Hivemind does not implement one in this configuration. It produced a fair draw at 24 peers and did not
at 16, and the difference was machine load. An attacker need not defeat a mixing mechanism when none is
running.

**Unsettled:** whether the clique or fair regime is realistic. The experiment that would decide it —
fix peer count, vary timing noise deliberately — has not been run.

### 4.9 Sharding starves peers and degrades even when provisioned correctly

Found while running §4.4. All three effects trace to `update_key_on_not_enough_peers`
(`key_manager.py:107-109`) being `pass  # to be implemented in subclasses`, with no implementation
anywhere, while `matchmaking.py:538` calls it for real.

- **Starvation.** `group_bits` changes *only* inside `update_key_on_group_assembled`, so a peer that
  cannot assemble a group cannot change bucket — it is stuck permanently. At 16 peers: **8/16 never
  joined a group** at group size 4, **12/16** at group size 2.
- **`matchmaking_time = 5 s` is too short once sharded.** Leaders close on deadline with 2 of 4 members;
  the undersized groups scatter unevenly and strand peers. At 15 s: 16/16 peers, every group full, zero
  failures, 96 s instead of 337 s.
- **Metastability.** The permutation preserves balance only while every bucket forms one full group. One
  undersized group breaks the invariant permanently. Clean at 4 epochs, 63 failures by 10.

### 4.10 Other code-level facts, unmeasured

- **Krum is not implemented**, nor is any robust aggregation. Aggregation is a plain weighted mean
  (`partition.py:242-260`) with a **sender-supplied weight applied verbatim**, read off the wire at
  `allreduce.py:340`. Krum needs pairwise distances between *full* vectors; under reduce-scatter peer
  *i* only ever holds slice *i*. Coordinate-wise rules (median, trimmed mean) *are* implementable
  locally, because per-slice is identical to whole-vector for them.
- **Hivemind never distributes data.** No dataset or sharding code exists in the library. Each peer owns
  its pipeline; the ALBERT example has every peer load the whole corpus and shuffle with a seed derived
  from its own public key.

Background: `HIVEMIND_EXPLAINED.md` (how the system works), `GROUP_FORMATION.md` (group formation and
attacker control), `QA_NOTES.md` (Q&A log + literature review — **no published work analyses Hivemind's
DHT security**).

---

### 4.11 Brief item 3: what β costs the model, and what robust rules can promise

`step7_poison.py`, 8 peers, group size 4, β = 1/4 (one poisoning member), 3 seeds, 20 epochs.
Poisoning is applied to the **send path only** (`_generate_input_for_peer`, `allreduce.py:247`), so the
attacker's own tensors stay honest and the damage measured is damage to *honest* peers.

| configuration | seed 0 | seed 1 | seed 2 | ratio to control |
|---|---|---|---|---|
| control, no attack | 0.6721 | 0.5485 | 0.7436 | — |
| `sign_flip`, honest weight | 0.5403 | — | — | ≈ control (within noise) |
| **`sign_flip` + declared weight 10⁶** | **30.71** | **81.44** | **26.03** | **35–148×** |
| same, coordinate median | 0.6194 | 1.2700 | 0.3723 | 0.5–2.3× |

**One peer in four moves the model 35–148× further from the optimum**, and the damage comes almost
entirely from one unvalidated scalar. The same poisoned gradient at an honest weight is indistinguishable
from the control, because the mean dilutes it 1-in-4.

**The weight is a multiplier, not a poison.** `partition.py:242` applies the sender's declared weight
verbatim (`accumulator.add_(part, alpha=weight); denominator += weight`), read straight off the wire
(`allreduce.py:340`). An honest gradient at weight 10⁶ does almost nothing (measured 1.22× — the group
average simply becomes that peer's own valid gradient, losing the benefit of averaging but not the
direction). Combined with a poison it scales that poison without bound. Report the two factors
separately; `--declared-weight` is orthogonal to `--attack` for this reason.

**Coordinate-wise median recovers nearly all of it** — 0.5–2.3× the control against 35–148× — at **zero
extra communication**, because per-slice is identical to whole-vector for a coordinate-wise rule. It
also ignores the declared weight by construction, which is the point: that is the field the attacker
controls. Recovery is not perfect (seed 1 at 1.27 sits above the control band), so state it as
"recovers most of the damage", not "eliminates".

#### What robust aggregation can promise at Hivemind's group sizes

Published conditions, verified from source:

| rule | condition | source |
|---|---|---|
| Krum / Multi-Krum | **2f + 2 < n** | Blanchard et al., NeurIPS 2017 |
| Coordinate median, trimmed mean | **q < n/2** (trim level ≥ q) | Yin et al., ICML 2018 |

| group size n | max f for Krum | max β Krum | max β median |
|---|---|---|---|
| **2** (`min_group_size` default) | **0** | **0** | **0** |
| **4** (our runs) | **0** | **0** | 0.25 |
| 8 | 2 | 0.25 | 0.375 |
| 256 (ALBERT `target_group_size`) | 126 | ≈0.49 | ≈0.49 |

**Krum has no tolerance below n = 5.** And β measured in §4.3 is exactly k/(k+1) — 0.500, 0.667, 0.800 —
with 0.646 in §4.6. **Every one exceeds every bound.**

**The decisive point: the attacker chooses n as well as f.** Robust-aggregation theory takes group size
as exogenous. Here the attacker *is the leader* and closes the group when it likes
(`matchmaking.py:277-297`), and `min_group_size = 2` lets it close at two. A {attacker, victim} group is
n = 2, f = 1: Krum needs 4 < 2, median needs 1 < 1. **Every precondition fails by construction, before a
single gradient is crafted.** No aggregation rule helps when the adversary picks the denominator.

*Stated precisely:* these are **sufficient** conditions. Exceeding one removes the guarantee; it does not
prove the rule fails. The measured arm above is what speaks to actual damage.

**Krum was not implemented**, for two independent reasons: its bound is violated at these group sizes
anyway, and it cannot run under reduce-scatter, needing pairwise distances between *full* vectors while
peer *i* holds only slice *i*. Running it would mean concentrating the whole vector on one reducer —
discarding the property that makes Hivemind scale.

**Caveats.**
- **The 0.1492 baseline in §2 is not comparable to these numbers.** It was produced by `step2_groups.py`,
  whose factory leaves the gradient averager's `target_group_size` unset, so its groups close on deadline
  rather than filling. Binding it — which `step7` does, so β has a stable denominator — costs ~4× on its
  own: 0.148 unbound vs 0.575 bound, 8 peers at group size 8, no attacker. `step7 --no-bind-group-size`
  returns 0.1481, which is what confirms the reducer subclass is faithful to Hivemind's own averaging.
  **Always compare against this script's own `--attack none` arm.**
- Control variance is 0.55–0.74 across seeds. The 35–148× effect is far outside it; the median result
  (0.5–2.3×) is not, so the defence claim rests on three seeds rather than one.

## 5. Methodology notes

**What replicates and what does not.** Results where the mechanism *removes* a race are stable across
runs: 5 identities = `num_replicas` exactly (zero variance), β = k/(k+1) when leading, 8/8 capture under
blinding. Results where the attacker must *win* a race are noisy at n ≈ 12 — an independent repeat of
the cached arms in §4.6 moved victim-led from 1/12 to 7/13 and β from 0.417 to 0.231, while the
*conclusion* (capture ≈ 0) held. **Quote ranges, not point estimates, for anything on a caching swarm at
this sample size, and repeat before believing a rate.**

**Key configuration traps, in addition to §3.**

- **`averager_opts` never reaches the gradient averager.** `Optimizer` forwards it only to the *state*
  averager (`optimizer.py:263`), never to `_make_gradient_averager` (`optimizer.py:300-315`), despite
  the docstring at `optimizer.py:157` claiming both. Verified empirically: capturing the
  `GradientAverager`'s real kwargs with `averager_opts=dict(target_group_size=3)` shows it absent, while
  a bogus key raises `TypeError` from the state averager. **The only route to the averager whose groups
  β is computed from is `grad_averager_factory`.** This was wrong here for two commits and is invisible
  whenever `num_peers == group_size`.
- **`target_group_size` must be set explicitly**, or leaders close on their deadline with whoever turned
  up and β has an unstable denominator. But binding it at **16 peers starves the swarm** — leader
  election is global, so groups form sequentially and only one or two assemble per matchmaking window.
  Three 16-peer attempts gave 4–6 of 16 peers ever averaging and **zero** victim rounds. Use 8 peers.
- **Custody is a precondition that is easy to omit.** An attacker with a random `node_id` holds no
  replicas and its read-filter almost never fires — 55 responses in a whole run versus 394 with custody.
  **Check the filter count before believing any number from `step5`/`step6`.**
- **`offload_optimizer=False`** so `model.weight` is the live value being measured.
- **Each peer needs its own `RSAPrivateKey`** via `tracker_opts`; forked children otherwise share one
  process-wide key and collide when publishing progress.
- **Filenames must encode every variable that changes the result** (e.g. `_cached` / `_nocache`), or
  runs overwrite each other. This happened once and lost data.

**Resource envelope.** Each peer is **4 Python processes plus one `p2pd`** (trainer, DHT, gradient
averager, state averager), because `DecentralizedAverager` extends `mp.Process`. ~350–520 MB per peer.
16 peers ≈ 5 GB; 24 fits on a 32 GB box; 32 untried. Measure rather than assume.

---

## 6. Next steps

**All four brief items are measured** (§4.1-4.11). What follows is work that would strengthen the
result, in priority order.

### Step 8 — the forged withdrawal (do this first; strongest remaining candidate)

**Status: code-level claim, verified by reading, NOT yet measured.** Everything below cites the line
that supports it, but no experiment has been run. Treat it as a designed attack, not a result.

Every read-filtering attack so far (§4.5, §4.6) fails on this testbed, for one structural reason: the
attacker can only **remove** entries from a response, and `add_candidate` (`node.py:834-837`) *merges*
every responder's dictionary, so one honest peer that answers undoes the filtering (§4.7, §4.6).
Removal needs universal cooperation.

**Read §4.6 before leaning on this.** This step was introduced as the only attack that works against a
real deployment with DHT caching enabled. **That premise is false.** §4.6 now measures the blinding
attack capturing 10/10 and 11/11 rounds at Hivemind's *default* `bucket_size` with caching fully on,
once the attacker mints 25 identities instead of 5. Read-filtering is not defeated by caching; it was
under-resourced. Caching alone has never been isolated as a defence.

**The case for this step is the custody row, and that row still stands.** Read-filtering needs ~20
crafted identities *and* the custody they buy; a forged withdrawal needs **no custody at all** — any
peer can write the record. That is a genuine difference in precondition, and it is what makes this
worth building. The "versus cached copies" row should be read as untested rather than as an advantage.



**A forged withdrawal inverts that: it is an addition, so merging, caching and replication all work
for the attacker instead of against it.** Four facts compose:

1. **Hivemind withdraws a peer by writing `False` under its subkey**, with
   `expiration = nextafter(expiration_time, +inf)` (`key_manager.py:61`) — deliberately the smallest
   increment *above*, so a withdrawal outranks the declaration it cancels.
2. **Later expiration wins, and an earlier one is rejected** (`timed_storage.py:81-84`):
   `if self.data[key].expiration_time < expiration_time: overwrite` else `return False`.
3. **`get_averagers(only_active=True)` skips `False` entries** (`key_manager.py:87`) — the victim
   performs the removal itself.
4. **The subkey is an unverified `peer_id.to_bytes()`** (`key_manager.py:64`); nothing checks the
   writer owns it, `PeerID.__init__` validates nothing, and **there is no `record_validator` on
   averaging keys at all** (verified: no reference to `record_validator` anywhere in
   `hivemind/averaging/`).

So: **write `value=False` under an honest peer's subkey with a far-future expiration, and that peer
disappears from every peer's candidate list.** Because of fact 2 this is not a per-round race — it also
**blocks that peer's own re-declarations** until the forged expiration lapses, since its honest
`declare_averager` carries a nearer expiration and is rejected. Its `declare_averager` returns `False`,
which nothing in the tree reads (§4.2's unused detection signal).

**What it costs, and why it matters for a real deployment:**

| | read-filtering (§4.5–4.6) | forged withdrawal |
|---|---|---|
| Mechanism | remove entries from a response | add a record |
| Versus merge semantics | fights them | **exploits them** |
| Versus cached copies | *untested — §4.6's caching claim was falsified, not confirmed* | **later expiration overwrites them** |
| Needs custody (5 crafted node_ids)? | yes | **no — any peer can write** |
| Detection signal | honest `declare_averager` returns False | same, and equally unread |

Dropping the custody precondition is the point: no eclipse, no crafted `node_id`s, no five identities.
One DHT write per peer to be hidden, refreshed when the forged expiration nears. Target PeerIDs are
readable straight off the group key.

**This also completes the follower path.** §4.6 showed an attacker can capture a victim's group by
joining it as a follower, but only if honest peers are excluded — which needs read-filtering, which
needs custody *and* ~`bucket_size` crafted identities. A forged withdrawal excludes them **without
custody and without winning any race**, which is the standing advantage.

**Build it as `research/step8_withdrawal.py`**, modelled on `step5_selective.py` (which already drives
the real `GroupKeyManager` through the `NodeAsDHT` adapter, so key construction and subkey choice stay
Hivemind's). Arms: `honest` control; `forge` writing withdrawals for all honest peers but the victim.
Measure the victim's candidate list, a bystander's list, whether honest `declare_averager` calls start
returning `False`, and **run it with caching on** — that is the whole point.

**Verify before believing it, in this order:**
1. A forged `False` under another peer's subkey is accepted at all (fact 4). If a validator or
   signature check rejects it, the attack is dead and that is the finding.
2. It survives merging with cached honest copies (facts 1-2) — the claim caching cannot defeat it.
3. The suppressed peer's later honest re-declaration is **rejected** (fact 2), which is the difference
   between a one-shot suppression and a per-round race.
4. Whether the DHT bounds how far in the future an expiration may be. `timed_storage.py:76` only
   rejects expirations in the *past*; if some upper bound exists elsewhere it limits how long one write
   suppresses a peer. Not checked.

### Loose ends, in descending order of interest

- **Isolate caching on its own** (§4.6). Reach was measured and is the dominant cause; caching was
  assumed and never tested separately. `step6_isolation.py` now takes `--no-cache-locally`,
  `--no-cache-nearest` and `--no-cache-on-store` individually, and `--bucket-size`. The run that
  settles it is `chase` at `--bucket-size 4` with each cache knob disabled singly: if capture is
  already total with all caching on, caching contributes nothing at realistic routing, and the
  "accidental defence" framing should go entirely.
- **Does the identity cost track `bucket_size` as the account predicts?** §4.6 says the attacker needs
  to own the victim's whole seed set, so the cost should be ~`bucket_size` identities and should rise
  if `bucket_size` does. Measured at the default 20 (25 identities suffice, 5 do not). A run at
  `--bucket-size 40` with 25 and 45 Sybils would confirm the relationship rather than the single point.

- **Interception latency.** §4.1 measures Sybil build time and settled capture, never the interval. A
  Sybil intercepts nothing until it is in the routing tables of peers routing toward the key. Have the
  victim poll on a fixed interval, introduce Sybils at t=0, record capture against wall-clock. This is
  what a defender would need to catch the attack in flight.
- **Settle §4.8** by fixing peer count and varying timing noise deliberately (pin cores, inject
  latency), to see whether clique structure follows the noise rather than the peer count.
- **Grind a low `peer_id`.** The tie-break for equal deadlines is the lowest `peer_id`
  (`matchmaking.py:478-481`) and `identity_path` makes PeerIDs choosable, so they can be ground offline.
  That is the lever for the ~40 % of rounds the attacker still loses (§4.3).
- **Censorship as opposed to custody.** §4.1's attacker nodes serve honestly. Refusing to serve on
  `rpc_find` is a separate experiment; §4.2 does the store-side half.
- **A free detection signal nobody uses** (§4.2), if the project turns to defences.

### Things that did not work — do not retry without reading why

- **Forged deadlines.** Expiration is simultaneously the record's **lifetime** and its **leadership
  priority** — one field (`key_manager.py:60`, read back at `:88`). Lowering it to win the election
  makes the advertisement expire instead of winning.
- **16-peer runs with a bound `target_group_size`** — see §5.
- **`chase` against a caching swarm** — β below the honest control (§4.6).

Before claiming novelty on β thresholds, read *Secure Distributed Training at Scale* (BTARD, ICML 2022,
arXiv:2106.11257), especially its threat model and Appendices F and G. It assumes a **fixed** Byzantine
set with |B| ≤ δn, δ < ½, and defends the *values*. It does not address who ends up in which group,
which is the gap this project targets.

---

## 7. Setup on a new machine

```bash
git clone git@github.com:yvssaratchandra/hivemind-analysis.git hivemind
cd hivemind
git checkout research/net-security

python3 -m venv .venv                                    # 3.13 and 3.14 both used
.venv/bin/pip install -U pip setuptools wheel
.venv/bin/pip install "grpcio-tools>=1.75.1" "protobuf>=6.0"
.venv/bin/pip install torch --index-url https://download.pytorch.org/whl/cpu
.venv/bin/pip install -e . --no-build-isolation          # also downloads p2pd (gitignored)
.venv/bin/pip install -r requirements.txt
.venv/bin/pip install psutil pytest pytest-asyncio       # only for the test suite

.venv/bin/pytest tests/test_dht.py -x -q                 # expect 4 passed
.venv/bin/python research/step2_groups.py --num-peers 8 --group-size 8 --max-epoch 20
```

Two steps are not obvious and both cost time to rediscover:

- **`--no-build-isolation` on Python 3.14.** `pyproject.toml` pins `grpcio-tools>=1.33.2,<1.68`, but
  cp314 wheels start at 1.75.1, so an isolated build compiles a pre-3.14 grpcio-tools from source. The
  pin only governs running protoc at build time (`setup.py:39-58`), so installing a current
  grpcio-tools first and skipping isolation is sound. On 3.13, plain `pip install -e .` works.
- **`requirements.txt` must be installed by hand.** The editable install produced a dist-info with no
  `Requires-Dist`, so pip installed hivemind with no dependencies and reported success. The failure
  surfaces much later as a missing `numpy` or `cryptography`.

**Remote convention: `origin` is the fork; nothing is ever pushed to Hivemind upstream.** A fresh clone
satisfies this. If you add upstream, disable pushing to it:

```bash
git remote add upstream git@github.com:learning-at-home/hivemind.git
git remote set-url --push upstream DISABLED_no_pushing_to_upstream
```

Check `git remote -v` before pushing on an unfamiliar machine — one machine had these reversed.

Reproducing the adversarial results, cheapest first:

```bash
.venv/bin/python research/step3_eclipse.py                       # seconds, no swarm
.venv/bin/python research/step3_eclipse_live.py                  # ~3 min
.venv/bin/python research/step3_candidates.py --sizes 8 16 32    # ~6 min
.venv/bin/python research/step3_capture.py --arm malicious --attackers 1 \
    --attacker-matchmaking-time 15.0 --attacker-declare-time 1.0 # ~2.5 min
.venv/bin/python research/step6_isolation.py --arm chase --num-peers 8 \
    --group-size 4 --attackers 3 --no-cache                      # ~2.5 min
```

The two attacker flags on `step3_capture.py` are not decoration — without them leadership is won 14 % of
the time instead of 57 % (§4.3). Run anything longer in the background rather than waiting on it.

**What does not travel with the repo**, and is why this file exists: conversation history, the plan file
under `~/.claude-account2/plans/`, and the assistant memory directory. **If you learn something that
matters, add it here rather than leaving it in a chat log.**
