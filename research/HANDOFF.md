# Handoff — read this first

State of the project, why it is built this way, and how to pick it up on another machine. Written so a
reader with no prior context (human or assistant) can continue without the original conversation.

Last updated: 2026-09-10 · branch `research/net-security` · remote `analysis`

Second machine, second day: the environment notes in §5 and the traps in §3 have both changed.

---

## 1. What this project is

A security study of Hivemind's peer-discovery and averaging layer. The brief:

> Deploy a small Hivemind / libp2p swarm locally training a toy model, then: (1) reproduce a known
> eclipse against a victim node and measure its cost (number of Sybil peers, time); (2) measure how a
> small global fraction of malicious peers becomes a large local fraction at the victim, and how it
> decays under Hivemind's periodic group re-formation; (3) couple network control to learning harm:
> convergence degradation and stealthy targeted poisoning vs. the victim's local malicious fraction,
> under plain averaging and robust aggregation (Krum).

Two quantities run through all of it:

- **α** = attacker-controlled peers / all peers in the network (global fraction)
- **β** = attacker-controlled peers / members of one averaging group (local fraction)

The interesting question is whether β is something an attacker *has* (β ≈ α, as the Byzantine-ML
literature assumes) or something an attacker *chooses*. See §6 — and note that §6.1 finds β is not
reliably pinned to α even with nobody attacking.

**Standing constraint: never modify anything under `hivemind/`.** All work is additive under
`research/`. Every extension uses a supported hook or a subclass.

---

## 2. Where things stand

**Done and verified:**

| File | What it does |
|---|---|
| `research/step1_baseline.py` | N peers, real DHT + real `hivemind.Optimizer`, training linear regression with a known optimum `w*`. Establishes the clean baseline. |
| `research/step2_groups.py` | Same, plus records **which peers shared a group each round**. This is the control for β. |
| `research/analyse_groups.py` | Tests a recorded run's membership against a uniform-random-partition null, by Monte Carlo. |
| `research/step3_eclipse.py` | Cost of owning a DHT record, as arithmetic over node_ids. Brief item 1, analytical layer. |

Results are in `research/results/*.csv` (tracked in git), tagged with the configuration that produced
them — `step2_groups_16x8.csv` is 16 peers at group size 8. A trailing `_b` is an independent repeat.

**Canonical baseline** (`step2_groups.py --num-peers 8 --group-size 8 --max-epoch 20`):

| epoch | mean distance to `w*` | spread across peers | peers seen |
|---|---|---|---|
| 0 | 7.9173 | 0.0000 | 8 |
| 10 | 1.0579 | 0.0000 | 8 |
| 20 | **0.1492** | 0.0000 | 8 |

20 rounds, 160 group observations, group size always 8, no missing epochs, 104 s wall clock.
**0.1492 is the number "unbroken" means.** Every later attack result is compared against it.

The first machine recorded **0.1479** for the same configuration, and that number appears in earlier
commits. The 0.9 % gap is not a behaviour change: peers race to `target_batch_size`, so the number of
local steps each one lands per epoch varies slightly between runs. `step1_baseline.py`, re-run here
independently, also gives 0.1492 at epoch 20.

**Multi-group runs**, all 20 epochs, all with every observed group at exactly the requested size and
no epochs missing peers:

| peers | group size | groups/round | epoch-20 distance | spread at epoch 20 | peak spread | wall clock |
|---|---|---|---|---|---|---|
| 8 | 8 | 1 | 0.1492 | 0.0000 | 0.0000 | 104 s |
| 16 | 4 | 4 | 0.1266 | 0.0015 | 0.2766 | 104 s |
| 16 | 8 | 2 | 0.1226 / 0.1228 | 0.0017 / 0.0007 | 0.0703 / 0.0998 | 107 s / 104 s |
| 24 | 8 | 3 | 0.1233 / 0.1234 | 0.0045 / 0.0036 | 0.0413 / 0.0514 | 109 s / 106 s |
| 24 | 12 | 2 | 0.1229 | 0.0021 | 0.0190 | 108 s |

Both spread columns are there because quoting only the epoch-20 one is misleading: the peak arrives in
the first few epochs and is up to two orders of magnitude larger. See §3.

Larger swarms converge slightly further per epoch because averaging over more peers gives a less noisy
gradient; each peer still takes ~25 local steps per epoch at every size, since `target_batch_size`
scales with the peer count. What these runs were actually for is in §6.1.

**Not started:** anything adversarial. Steps 1–2 are the honest control only.

---

## 3. The two things that will bite you

### Trap 1 — a swarm that looks like it is training together, but is not

**A Hivemind swarm can look like it is training together when every peer is actually training alone,
and it still converges, so the graphs look fine.**

Gradient averaging is skipped entirely unless the progress tracker sees more than one peer
(`hivemind/optim/optimizer.py:526`). This model is small enough that an unpaced training step takes
microseconds, so a peer reaches `target_batch_size` by itself long before the DHT can propagate anyone
else's progress. `num_peers` stays 0, no averaging ever happens, and the loss still goes down because
each peer is solving the same regression problem independently.

This happened. The first version of step 1 produced a clean-looking convergence curve that was
entirely local training.

**Two guards, both already in the code — do not remove them:**

1. **Pace the loop.** `--batch-time` (default 0.2 s) sleeps between steps, and `target_batch_size`
   defaults to `100 × num_peers`, so each peer does ~25 steps ≈ 5 s per epoch — comparable to
   `matchmaking_time`. `benchmarks/benchmark_optimizer.py` paces for the same reason.
2. **Record `peers_seen`.** Every CSV row carries `optimizer.tracker.global_progress.num_peers`, and
   both scripts print a loud warning if it never exceeds 1.

**How to tell real averaging from fake at a glance:** the spread of `distance_to_optimum` across peers.
Real averaging → spread is exactly **0.0000** (peers hold identical weights). No averaging → spread is
0.1–0.8 and grows, as peers drift apart. Check this on every run.

That shortcut holds only while the whole swarm is **one group**. With more than one group per round,
peers in different groups average different things, so the spread is non-zero and that is correct
behaviour rather than a symptom. It starts at 0.0000, peaks within the first few epochs — 0.28 at
epoch 1 in the 16-peer group-4 run, 0.07 at 16 peers with group 8, 0.04 at 24 — and then decays by
roughly two orders of magnitude, to 0.001–0.005 by epoch 20.

**Note that the peak overlaps the 0.1–0.8 quoted just above**, so on a multi-group run the magnitude
of the spread cannot tell the two cases apart, and the early epochs are exactly where it misleads.
Two things do tell them apart: `peers_seen`, at any group count, and the direction of travel — real
averaging makes the spread shrink over the run, while peers training alone drift steadily further
apart.

### Trap 2 — on Python 3.14 the swarm does not start at all, and hangs instead of failing

Hivemind's averagers are `mp.Process` subclasses (`averaging/averager.py:50`) holding a `DHT`, asyncio
primitives and shared-memory tensors, started with whatever multiprocessing start method is the
default. **Python 3.14 changed that default on Linux from `fork` to `forkserver`**, which pickles the
process object rather than inheriting it — and an averager does not pickle. Every peer dies inside
`Optimizer.__init__` at `popen_forkserver._launch`, and the parent then waits forever for peers that
are already gone, so what you see is a hang, with the real traceback buried in the child's output.

`main()` in both scripts now calls `mp.set_start_method("fork", force=True)`. **Do not remove it.**
Any new script that starts a swarm needs the same line. If a swarm fails to start, read the *child*
traceback first.

---

## 4. Decisions already made, and why

- **Model: linear regression as `nn.Linear(64, 1, bias=False)`, MSE loss, data generated from a known
  `w*`.** Real enough to exercise the whole stack through `hivemind.Optimizer`, but with an exactly
  known answer so any displacement is attributable to an attack rather than to noise in a model you
  cannot inspect. A real neural net would remove that — you would be arguing about whether a loss dip
  was the attack or ordinary variance.
- **Peer count is bounded by memory.** Each peer is **4 Python processes plus one `p2pd`** (trainer,
  DHT, gradient averager, state averager), because `DecentralizedAverager` extends `mp.Process`. The
  first machine measured ~350 MB per peer in PSS terms and 15.2 GB total, which capped it at 16. On the
  current 32 GB box, system memory in use peaked at 13.3 GB for 16 peers and 15.7 GB for 24, against
  ~5 GB with nothing running — so 450–520 MB per peer, and 24 fits with room to spare. 32 peers has not
  been tried. Adjust for your box, and measure rather than assume.
- **β granularity is 1/group_size**, so group size 8 gives steps of 0.125. Raise the *group* size, not
  the peer count, for a finer β sweep.
- **`target_group_size` is set explicitly, and must be set on the *gradient* averager.** Left at its
  `None` default, the leader closes each group when its own deadline passes with whoever turned up
  (`matchmaking.py:290-297`) rather than when the group is full (`matchmaking.py:276-280`), so group
  size varies round to round and β has an unstable denominator. Passing it through `averager_opts` does
  **not** achieve this: `Optimizer` forwards `averager_opts` only to the state averager
  (`optim/optimizer.py:263`) and never to `_make_gradient_averager` (`optimizer.py:300-315`), despite
  the docstring at `optimizer.py:157` saying it reaches both. The only supported route to the averager
  that forms the groups β is computed from is `grad_averager_factory`. This was wrong here for two
  commits; it is invisible whenever `num_peers == group_size`, which is why the 8-peer results were
  unaffected and correct. `report()` now warns when an observed group size disagrees with the requested
  one, which is the check that would have caught it.
- **`offload_optimizer=False`** so `model.weight` is the live value being measured.
- **Each peer gets its own `RSAPrivateKey`** via `tracker_opts`. Forked children would otherwise share
  one process-wide key and collide when publishing progress records.
- **Group membership is read through `grad_averager_factory`** (`hivemind/optim/optimizer.py:192`), by
  subclassing `GradientAverager` and overriding `_aggregate_with_group`. Note that this override runs
  inside the *averager's own process*, which is why observations come back over an `mp.Queue`.

---

## 5. Continuing on another machine

```bash
git clone git@github.com:yvssaratchandra/hivemind-analysis.git hivemind
cd hivemind
git checkout research/net-security

python3 -m venv .venv                                    # 3.14.7 here; the first machine used 3.13
.venv/bin/pip install -U pip setuptools wheel
.venv/bin/pip install "grpcio-tools>=1.75.1" "protobuf>=6.0"
.venv/bin/pip install torch --index-url https://download.pytorch.org/whl/cpu
.venv/bin/pip install -e . --no-build-isolation          # also downloads p2pd (gitignored)
.venv/bin/pip install -r requirements.txt
.venv/bin/pip install psutil pytest pytest-asyncio       # only to run the test suite

.venv/bin/pytest tests/test_dht.py -x -q                 # expect 4 passed
.venv/bin/python research/step2_groups.py --num-peers 8 --group-size 8 --max-epoch 20
```

Two steps in there are not obvious, and both cost time to rediscover:

- **`--no-build-isolation` is needed on Python 3.14.** `pyproject.toml` pins the build requirement to
  `grpcio-tools>=1.33.2,<1.68`, and cp314 wheels for `grpcio-tools` start only at 1.75.1 — so an
  isolated build tries to compile a pre-3.14 grpcio-tools from source. That pin only governs running
  protoc at build time (`setup.py:39-58`), so installing a current grpcio-tools into the venv first and
  skipping isolation is sound. On Python 3.13 none of this applies; plain `pip install -e .` works.
- **`requirements.txt` has to be installed by hand.** The editable install here produced a dist-info
  with no `Requires-Dist` at all, so pip installed hivemind with none of its dependencies and reported
  success. The failure then shows up much later as a missing `numpy` or `cryptography`.

Expect the 8-peer numbers in §2. If `peers visible to each other` reports 0 or 1, re-read §3 before
trusting anything — and raise `--batch-time` if the machine is slower. On a run with more than one
group per round, do not expect a 0.0000 spread; see §3.

**What does not travel with the repo**, and is why this file exists: the conversation history, the plan
file under `~/.claude-account2/plans/`, and the assistant memory directory. Everything load-bearing has
been written into `research/` instead. If you learn something that matters, add it here rather than
leaving it in a chat log.

---

## 6. Findings

### 6.1 Measured: membership does not mix, and nothing in the protocol says it should

This is what the multi-group runs in §2 were for. `analyse_groups.py` compares a recorded run against
the null the Byzantine-ML literature implicitly assumes — that each round's membership is a fresh,
uniformly random partition of the swarm into groups of the observed size — over 20 000 Monte Carlo
draws:

| run | groups/round | co-occurrence sd | null sd | P(null ≥ observed) | distinct partitions |
|---|---|---|---|---|---|
| 16 peers, group 8 | 2 | 5.53 | 2.23 | < 5e-5 | 13/20 |
| 16 peers, group 8 (repeat) | 2 | 5.45 | 2.23 | < 5e-5 | 10/20 |
| 16 peers, group 4 | 4 | 4.96 | 1.78 | < 5e-5 | 15/20 |
| 24 peers, group 8 | 3 | 1.96 | 2.06 | 0.85 | 20/20 |
| 24 peers, group 8 (repeat) | 3 | 2.01 | 2.06 | 0.70 | 20/20 |
| 24 peers, group 12 | 2 | 2.24 | 2.23 | 0.46 | 20/20 |

The *mean* co-occurrence carries no information: with a fixed group size it is settled by arithmetic
and matches the null exactly in every run. The spread is where the signal is.

**At 16 peers the swarm froze into cliques, in all three runs.** One pair of peers shared a group in
all 20 of 20 rounds — probability under the null (7/15)^20 ≈ 3e-7 for a given pair — another pair never
met at all, and only 10–15 distinct partitions appeared across 20 rounds where the null gives 20.0
essentially always. **At 24 peers, same code, same defaults: indistinguishable from a fair draw, every
time.**

**The distinguishing variable is peer count, not group count.** 24 peers at group size 12 gives two
groups per round, exactly as 16 peers at group size 8 does, and it mixes fairly — so the tempting
explanation that with two groups the leader's rejects have nowhere else to go is wrong. And 16 peers
cliques at group size 4 as well, with four groups per round.

What is left is arrival order. With `initial_group_bits = ""` (`averaging/averager.py:120`) no peer
ever changes group key, so every round is the same race among the same peers: earliest declared
deadline leads (`matchmaking.py:479-497`), and the leader admits followers in arrival order
(`matchmaking.py:273`). **Nothing in the protocol re-randomises anything.** Whether membership mixes
therefore depends entirely on whether arrival order varies from round to round, which is a property of
the environment's timing noise rather than of Hivemind. At 16 peers this machine was quiet enough for
the order to repeat; at 24, the scheduling jitter of ~120 processes across 18 cores was apparently
enough to shuffle it.

**Why this matters for β.** The assumption that β ≈ α rests on membership being a fresh random draw.
Hivemind does not implement one. It happened to produce a fair draw at 24 peers here, and did not at
16, and the difference was machine load. An attacker does not have to defeat a mixing mechanism,
because there is no mixing mechanism to defeat.

Two honest limits on this result:

- Whether the clique regime or the fair regime is the realistic one is **not settled by this testbed**.
  Persistent per-peer latency differences on a real network resemble the stable-ordering regime that
  produced cliques here, but that is a hypothesis, not a measurement.
- The experiment that would settle it has not been run: hold the peer count fixed and vary the timing
  noise deliberately — pin peers to cores, or inject per-peer latency — and see whether the clique
  structure follows the noise rather than the peer count.

### 6.2 Measured: eclipsing a record costs five identities, at any network size

`step3_eclipse.py`, arithmetic over node_ids, 40 independent honest swarms per cell,
`num_replicas = 5`. Target is the key averaging advertisements live under,
`step2_grad_averager.0b`, whose address is `DHTID.generate(source=key)` — the same
computation `DHTNode` performs (`dht/node.py:386`).

| honest swarm | crafted node_ids for a total eclipse | capture with 5 random node_ids |
|---|---|---|
| 50 | 5 | 0.09 |
| 100 | 5 | 0.04 |
| 1 000 | 5 | 0.01 |
| 10 000 | 5 | 0.00 |

**Cost is exactly `num_replicas`, with zero variance, at every swarm size from 50 to 10 000.**
Capture rises as k/5 for k < 5 — the attacker takes precisely the k nearest slots — and total
eclipse lands at k = 5. Scale offers no protection whatsoever.

**The control arm carries the claim.** The same number of attacker peers with *random*
node_ids captures at chance and never reaches a total eclipse at any k tested, so the damage
is attributable to address choice, not to owning peers. The crafted arm is identical across
seeds (it does not depend on the random draw); the control arm varies with them, as it should.

The mechanism in one number: with 10 000 honest peers the nearest honest address to the
target sits about 2^146 away in XOR distance, while a crafted address sits at 0–4. Honest
`node_id`s are uniform over 160 bits (`DHTID.generate` hashes with SHA1), so they cannot
crowd a specific key; an attacker who picks its own address does not have to.

Why this is possible at all: `DHTNode.create` accepts a caller-supplied `node_id`
(`dht/node.py:171`) and peers accept the self-reported value verbatim off the wire
(`dht/protocol.py:150`). The libp2p PeerID *is* cryptographically sound, but routing never
consults it, so that soundness buys nothing here.

Not yet measured: whether a live swarm agrees — that a real `store` lands on attacker nodes,
and how long standing the Sybils up actually takes. That is the next step.

### 6.3 From reading the source, not yet measured

These came out of reading `hivemind/averaging/matchmaking.py` and are the reason the project is worth
finishing. All are code-level claims with line references; none has been experimentally confirmed.

- **Group membership is a race, not a random draw.** A peer advertises a self-chosen *deadline*;
  everyone sorts candidates by `(deadline, peer_id)` and the earliest becomes leader
  (`matchmaking.py:479-497`); the leader then admits followers in arrival order
  (`matchmaking.py:273`). Both criteria are attacker-controlled.
- **A leader chooses its own group.** `_check_reasons_to_reject` (`matchmaking.py:341-368`) runs on the
  leader's own machine with the requester's TLS-authenticated PeerID already in hand. A modified leader
  can admit exactly one target plus its own Sybils. With `min_group_size = 2`
  (`averaging/averager.py:120`) that yields **β = 0.5 from a single identity**, and β = k/g in general —
  meaning β decouples from α entirely.
- **Group re-formation is off by default.** `initial_group_bits` defaults to `""`
  (`averaging/averager.py:120`) and `hivemind.Optimizer` never overrides it, so
  `update_key_on_group_assembled` (`averaging/key_manager.py:102`) discards its own output and no peer
  ever changes group key. The brief's "decay under periodic re-formation" therefore has nothing to
  decay under, unless the feature is switched on explicitly. §6.1 is the measured consequence of this
  one, and it is the only item in this list that is no longer just a code reading.
- **Krum is not implemented, and cannot be dropped in.** No robust aggregation exists anywhere in the
  tree; aggregation is a plain weighted mean (`averaging/partition.py:242-260`) with a sender-supplied
  weight applied verbatim. Krum needs pairwise distances between *full* vectors, which no peer holds —
  in reduce-scatter, peer *i* only ever sees slice *i*. Coordinate-wise rules (median, trimmed mean)
  *are* implementable locally, because computing them per-slice is identical to computing them over the
  whole vector.
- **Hivemind never distributes data.** There is no dataset or sharding code in the library at all. Each
  peer owns its own pipeline; the ALBERT example has every peer load the whole corpus and shuffle it
  with a seed derived from its own public key.

Background written up in `research/HIVEMIND_EXPLAINED.md` (how the system works),
`research/GROUP_FORMATION.md` (group formation and attacker control), and `research/QA_NOTES.md`
(Q&A log plus a literature review — no published work analyses Hivemind's DHT security).

---

## 7. Next steps

1. **Step 3: a malicious leader.** Two pieces, and only the second needs a subclass:
   - *Winning leadership needs no modified client at all.* The declared deadline is
     `min(max(step.scheduled_time, now + min_matchmaking_time), search_end_time)`
     (`matchmaking.py:527-531`), and `min_matchmaking_time` comes straight from `Optimizer`'s
     `matchmaking_time` argument (`optimizer.py:287,307`). A peer that simply configures a smaller
     `matchmaking_time` than everyone else declares the earliest deadline and is elected leader by
     every honest peer's own sorting (`matchmaking.py:479-497`). Note the trade-off in
     `GROUP_FORMATION.md` §3: an earlier deadline also closes the group sooner.
   - *Selective admission does.* Override `rpc_join_group` (which delegates at
     `averaging/averager.py:574-578`) to filter on `context.remote_id`, admitting the victim and the
     attacker's own Sybils and rejecting everyone else. The upstream test suite already subclasses this
     way — `tests/test_allreduce_fault_tolerance.py:32,87` define `FaultyAverager` and
     `FaultyAllReduceRunner` — so core Hivemind stays untouched.

   Measure β achieved vs. attacker count k, with an honest-leader control arm. Note that §6.1 has
   changed what that control means: it must be measured, not assumed to give β ≈ α, and it should be
   run at both 16 and 24 peers because those two behave differently.

   The identities have to be distributed before the attack can be selective. `run_peer` already
   publishes its `PeerID` to the parent over the metrics queue (`step2_groups.py:144`); the parent will
   need to collect all of them and hand the roster back before each peer builds its `Optimizer`, since
   the filtering runs inside the averager's own process and can only see what was bound into the
   factory.

2. **Then** couple β to learning harm, reusing `distance_to_optimum` against the 0.1492 baseline.
3. **Optional, and cheap:** settle the §6.1 limitation by varying timing noise at a fixed peer count.

Before claiming novelty on β thresholds, read *Secure Distributed Training at Scale* (BTARD, ICML 2022,
arXiv:2106.11257), especially its threat model and Appendices F and G. It assumes a **fixed** Byzantine
set with |B| ≤ δn, δ < ½, and defends the *values*. It does not address who ends up in which group,
which is the gap this project is aimed at.

---

*Documentation only; part of the additive `research/` tree. No core Hivemind code has been modified.*
