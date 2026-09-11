# Handoff — read this first

State of the project, why it is built this way, and how to pick it up on another machine. Written so a
reader with no prior context (human or assistant) can continue without the original conversation.

Last updated: 2026-09-11 · branch `research/net-security` · remote `origin`

Written on the second machine. If you are arriving on a third: §5 has the setup that actually works,
§3 has the two failure modes that cost the most time, and §2 says what is measured and what is not.

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
| `research/step3_eclipse_live.py` | The same, on a live swarm of real `DHTNode`s. Brief item 1, live layer. |
| `research/step3_candidates.py` | Attacker share of the victim's candidate list, by flooding and by eclipse. Brief item 2, discovery layer. |
| `research/step3_capture.py` | A malicious leader admitting only the victim and its own peers. Brief item 2, at the averaging group itself. |
| `research/step4_reformation.py` | Does re-formation shed the attacker? Brief item 2b. Needs sharding switched on. |
| `research/step5_selective.py` | Lying to the victim on read instead of censoring everyone on write. |
| `research/step6_isolation.py` | Does list capture win the *group*? Adds DHT-only Sybils holding the key's replicas. |

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

**Adversarial results**, all on live swarms, all detailed in §6:

| brief item | question | answer | where |
|---|---|---|---|
| 1 | cost of an eclipse | **5 identities, ~0.5 s**, at any network size from 50 to 10 000 | §6.2 |
| 2 | α → β at the discovery layer | 5 identities take **100 %** of the victim's candidate list, **from ~one advertisement lifetime after they arrive** | §6.3, §6.9 |
| 2 | α → β at the averaging group | α 0.062 → mean β **0.315**, a 5× amplification from one identity | §6.4 |
| 2b | decay under re-formation | naive sheds (1/5), **tracking does not (5/7)**; next bucket predicted **5/5** | §6.5 |
| 2 | α → β, quietly | selective read gives the same capture with **0 detection signal, 0 collateral** | §6.7 |
| — | does history protect the victim? | no: warm records delay capture by **one record lifetime**, then it is total | §6.9 |
| 2 | does owning the list win the group? | **partly** — capture 0/12 → 4-5/13, β 0.104 → 0.250; filtering also makes the victim *lead* more often | §6.10 |
| 2 | blind the victim entirely (`chase`) | **8/8 total capture, 0 honest peers, β 0.646** — but only with DHT caching off; **caching defeats it** (2/12) | §6.11 |
| 3 | β → learning harm, plain vs robust aggregation | **not started** | — |

**Reading the results directory.** Filenames carry the configuration:

| pattern | what it is |
|---|---|
| `step2_groups_<n>x<g>.csv` | honest group membership, n peers at group size g; `_b` is a repeat |
| `step3_eclipse_analytical.csv` | XOR arithmetic sweep, no processes |
| `step3_eclipse_live.csv` | the same on real `DHTNode`s |
| `step3_candidates_live.csv` | candidate-list capture, honest / flood / eclipse arms |
| `step3_capture_groups_<arm>_k<k>_n<n>.csv` | malicious leader, naive configuration |
| `step3_capture_groups_forced_mmt<t>_k1.csv` | the fix that **failed** — kept because §6.4 explains why |
| `step3_capture_groups_decoupled_d<t>_k<k>.csv` | the fix that works, `<t>` the attacker's declare time |
| `step4_reformation_<arm>.csv` | re-formation arms: none / naive / tracking |
| `step5_selective*.csv` | selective-read vs write-censorship; `_cached` caching on, `_warmN` N warm-up rounds, `_settleN` N seconds for warm records to expire |
| `step6_isolation_<arm>_k<k>_n<n>.csv` | group capture under selective / bidirectional / chase filtering |

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
  that forms the groups β is computed from is `grad_averager_factory`.

  **Verified empirically, not just read from source**, since this one is easy to disbelieve. Capturing
  the `GradientAverager`'s actual kwargs while passing `averager_opts=dict(target_group_size=3)` shows
  `target_group_size` absent — it receives only what `_make_gradient_averager` hardcodes. Passing
  `averager_opts=dict(bogus_kwarg_xyz=1)` instead raises
  `TypeError: DecentralizedAverager.__init__() got an unexpected keyword argument 'bogus_kwarg_xyz'`,
  which is the state averager (constructed first, `optimizer.py:253`) rejecting it. So the option
  reaches one averager and not the other, exactly as the code reads. The docstring is an upstream
  defect: following it gives silent non-application rather than an error. This was wrong here for two
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

**Remote convention: `origin` is the fork, and nothing is ever pushed to Hivemind upstream.** A fresh
clone of the fork already satisfies this. If you add upstream to pull in changes, add it under a
different name and disable pushing to it:

```bash
git remote add upstream git@github.com:learning-at-home/hivemind.git
git remote set-url --push upstream DISABLED_no_pushing_to_upstream
```

The first machine had these the other way round — `origin` pointing at `learning-at-home/hivemind` and
the fork as `analysis` — so a bare `git push` there would have targeted upstream. Check
`git remote -v` before pushing on a machine you have not used before.

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

To reproduce the adversarial results, cheapest first:

```bash
.venv/bin/python research/step3_eclipse.py                    # seconds, no swarm
.venv/bin/python research/step3_eclipse_live.py               # ~3 min, 24 DHT-only peers
.venv/bin/python research/step3_candidates.py --sizes 8 16 32 # ~6 min
.venv/bin/python research/step3_capture.py --arm honest    --attackers 1
.venv/bin/python research/step3_capture.py --arm malicious --attackers 1 \
    --attacker-matchmaking-time 15.0 --attacker-declare-time 1.0
```

The last two are ~2.5 min each at 16 peers. The two attacker flags are not optional decoration —
without them leadership is won in 14 % of rounds instead of 57 %, for the reason in §6.4. Anything
longer than three 16-peer configurations should be run in the background rather than waited on.

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

**A live swarm agrees exactly.** `step3_eclipse_live.py` runs real `DHTNode`s — each with its
own `p2pd`, real Kademlia routing, real `store` and `get` RPCs — with 24 honest peers, 3
repeats per cell, caching off so custody is measured rather than copies:

| crafted identities | theoretical capture | where the record actually landed | build time |
|---|---|---|---|
| 1 | 0.20 | 0.20 | 0.10 s |
| 3 | 0.60 | 0.60 | 0.30 s |
| 5 | **1.00** | **1.00** | 0.51 s |
| 8 | 1.00 | 1.00 | 0.80 s |

In **all 30 rows** the record landed on exactly the theoretical replica set: predicted and
actual capture never differed, and the holder count was always exactly `num_replicas` = 5.
Live routing has no slack the arithmetic missed. The random-node_id control never exceeded
0.6 and never achieved a total eclipse.

**Cost in time is ~0.1 s per Sybil**, near-perfectly linear (0.098–0.102 s across every
configuration), on a swarm that is already running. Standing up a total eclipse of a key
takes about half a second.

**Custody is not yet censorship, and the table above should not be read as if it were.**
These attacker nodes store and serve honestly; they simply own the addresses. An honest
reader still got the value in all 30 rows. What the eclipse establishes is that every
replica of that record sits on attacker hardware — so the attacker *could* refuse to serve,
serve a stale value, or serve different values to different readers. Measuring that needs a
node that misbehaves on `rpc_find`, which is a separate experiment and has not been run.

### 6.3 Measured: five identities take the whole candidate list, at any swarm size

`step3_candidates.py`, live `hivemind.DHT` swarms, going through the real
`GroupKeyManager.declare_averager` / `get_averagers` rather than a reimplementation. The
victim's **candidate list** is the set of peers it reads back when looking for averaging
partners.

| arm | attacker processes | α | attacker share of list | honest peers still visible |
|---|---|---|---|---|
| honest control | 0 | 0.000 | 0.00 | 32/32 |
| flood, 8 writes | 1 | 0.029 | 0.20 | 32/32 |
| flood, 32 writes | 1 | 0.029 | 0.50 | 32/32 |
| flood, 128 writes | 1 | 0.029 | 0.80 | 32/32 |
| **eclipse, 5 nodes** | 5 | 0.132 | **1.00** | **0/32** |

**Flooding is cheap but linear.** An advertisement is an unsigned record whose subkey is a
PeerID nobody verifies (`key_manager.py:46`; `PeerID.__init__` validates nothing), so one
process writes as many as it likes for peers that do not exist. But share is
k/(k + honest): 128 fabricated advertisements reach 0.80 against 32 honest peers and would
need four times as many to hold that share against four times the swarm. It also only
*dilutes* — every honest peer stayed visible in every flood run.

**The eclipse does not dilute, it removes.** Five nodes with crafted node_ids own every
replica of the group key (§6.2), and `record_validator` — a supported `DHTNode.create`
argument that gates incoming writes at `protocol.py:260` — lets each of them refuse to
store anyone else's advertisement. Honest advertisements then have nowhere to land. The
victim's candidate list came back with **exactly one entry, the attacker's**, at every
swarm size tested.

**This is the amplification the brief asks about.** As the swarm grows from 8 to 32 peers, α
falls 0.357 → 0.227 → 0.132 while β at the candidate list stays pinned at 1.00. §6.2 showed
five identities suffice at 10 000 peers, where α would be 0.0005 — and there is no reason
from the mechanism for β to move.

**A free detection signal, currently discarded.** All 32 honest peers got `False` back from
their own `declare_averager` call: the DHT told each of them its advertisement had been
refused by every replica. Nothing in Hivemind looks. `_declare_averager_periodically`
(`matchmaking.py:535`, and again at 544) awaits the call and throws the result away, and a
tree-wide search finds no other caller. A peer being erased from the discovery layer already
has the evidence in hand and drops it.

**What this is not.** The candidate list is not the averaging group. A peer picks one leader
from this list and the group is assembled by a separate race (`GROUP_FORMATION.md` §3), so
owning the list is not yet owning the group — that is the malicious-leader experiment, and
these two must not be reported as the same quantity.

### 6.4 Measured: the admission filter is exact, and leadership can be won after all

`step3_capture.py`, 16 peers, group size 8, 20 epochs, peer 0 the victim and peers 1..k the
attacker. All peers run one averager class, because RPC handles are named
`f"{cls.__name__}.{method_name}"` (`p2p/servicer.py:148`) and a caller builds its stub from
its own `type(self)` — two differently-named subclasses in one swarm register handlers
nobody calls.

A **led capture** below means the victim's group was exactly what the filter should produce:
k attackers plus the victim and nobody else.

| arm | k | α | led captures | β when captured | mean β over all rounds |
|---|---|---|---|---|---|
| honest | 1 | 0.062 | 0 % | — | 0.000 |
| honest | 2 | 0.125 | 0 % | — | 0.069 |
| honest | 4 | 0.250 | 0 % | — | 0.275 |
| malicious | 1 | 0.062 | 14 % | **0.500** | 0.143 |
| malicious | 2 | 0.125 | 10 % | **0.667** | 0.075 |
| malicious | 4 | 0.250 | 10 % | **0.800** | 0.080 |

**When the attacker leads, admission control is total.** β when captured is 0.500, 0.667,
0.800 — exactly k/(k+1) in every case, with no variance. One identity beside the victim is
half the group. That is an amplification of **8×** over α at k=1, and it confirms the
`_check_reasons_to_reject` reading (`matchmaking.py:341-368`) on a live swarm.

**The attack's bottleneck is winning the leadership race, not admission.** With the naive
configuration leadership was won in only 10–20 % of rounds. Fixing that took two attempts and
is written up below, because the failed one is the more informative.

**Correction to an earlier claim.** This file previously said a peer that configures a
smaller `matchmaking_time` "is elected leader by every honest peer's own sorting". That is
overstated. Measured at k=1, holding everything else fixed:

| attacker `matchmaking_time` | led captures |
|---|---|
| 5.0 (same as honest) | **0 %** |
| 2.0 | 14 % |
| 0.5 | 20 % |

It is a real lever and it points the right way, but it is weak, not decisive. The code says
why. The declared deadline is `max(step.scheduled_time, now + min_matchmaking_time)`
(`matchmaking.py:527-531`); `scheduled_time` is set to `now + min_matchmaking_time` at the
moment averaging is pre-scheduled (`averager.py:394-395`), and that pre-scheduling fires
when `estimated_next_update_time - now <= matchmaking_time` (`optimizer.py:562`). `Optimizer`
passes the same number to both, so the two cancel: **`scheduled_time` lands on the shared
epoch-end estimate for every peer, whatever its `matchmaking_time`.** The residual effect is
most likely on *re-declaration* — `_declare_averager_periodically` sleeps until its own
deadline lapses and then declares `now + min_matchmaking_time` with a stale `scheduled_time`
(`matchmaking.py:536`), which is the one place a small value binds. That last part is
inferred from the code and not yet directly measured.

**More attackers made capture rarer, not commoner** (14 % → 10 % from k=1 to k=4), which is a
mechanism worth naming: with several attackers admitting each other, the leader reaches
`min_group_size = 2` from its own peers alone and assembles on its deadline before the victim
arrives. In the k=2 run the attackers' own groups were size 2 in 36 of 42 observations — the
two of them pairing up, without the victim.

**The honest control did its job**, and §6.1 is why it was needed. Honest β was 0.000 at
k=1, 0.069 at k=2 and 0.275 at k=4 against an α of 0.062, 0.125 and 0.250 — at or below α,
not tracking it, exactly as a swarm that does not mix would behave. Assuming β ≈ α would have
been wrong in both directions.

#### Winning the race: one failed fix, and the one that works

**The obvious fix does not work.** `schedule_step` accepts an explicit `scheduled_time`
(`optim/grad_averager.py:150`), so forcing it into the present should make
`max(scheduled_time, now + min_matchmaking_time)` fall through to the term the attacker
controls. Measured, it changed nothing: 5 %, 10 %, 0 %, 15 % at `matchmaking_time` 0.5, 1.0,
2.0, 3.0, against a 14 % baseline. The reason is that declaration happens immediately after
scheduling, which happens at `E - matchmaking_time`, so `now + min_matchmaking_time` still
evaluates to `E`. Removing the `scheduled_time` term leaves the other one landing in the same
place.

**The coupling is between two different uses of the same number.** `Optimizer` gates
pre-scheduling on `estimated_next_update_time - now <= matchmaking_time`
(`optimizer.py:562`) *and* passes that same value to the averager as `min_matchmaking_time`
(`optimizer.py:307`), which sets the declared deadline. The gate fires at `E - t` and the
deadline becomes `(E - t) + t = E`. **Every peer declares the shared epoch-end estimate,
whatever it configures** — which is why `matchmaking_time` looked like a lever and was not.

**Breaking the coupling is what works.** Override only the averager's copy of
`min_matchmaking_time` in the factory, so the attacker advertises *early* (a large `Optimizer`
`matchmaking_time`, gate fires at `E - 15`) while declaring a *short rolling deadline* (a
small `min_matchmaking_time`). That is the only configuration that declares earlier than `E`.
At k=1:

| attacker declare time | led captures |
|---|---|
| baseline, no decoupling | 14 % |
| 0.5 s | 60 % |
| 1.0 s | 57 % |
| 1.5 s | **67 %** |
| 3.0 s | 5 % |

**The window `GROUP_FORMATION.md` §3 predicted is real and sharp.** Below ~1.5 s the attacker
wins the election; by 3 s its deadline is too close to `E` to win at all. The victim has to
arrive inside a window that a shorter deadline opens earlier and closes sooner, and between
0.5 s and 1.5 s that trade-off is flat enough not to matter.

**With leadership won, β is what the victim actually lives with:**

| k | α | led captures | β when captured | mean β over all rounds | β/α |
|---|---|---|---|---|---|
| 1 | 0.062 | 57 % | 0.500 | 0.315 | **5.0×** |
| 2 | 0.125 | 50 % | 0.667 | 0.508 | 4.1× |
| 4 | 0.250 | 33 % | 0.800 | 0.555 | 2.2× |

β when captured is still exactly k/(k+1), so none of the timing work touched the admission
filter. The strongest result is the cheapest one: **a single attacker identity, α = 0.062,
puts the victim in a two-peer group with it in 57 % of rounds and takes mean β to 0.315.**

**Holding the group open for the victim mattered.** The attacker now admits its own peers only
once the victim has joined, because otherwise the leader reaches `min_group_size = 2` on its
own Sybils and assembles before the victim arrives. That took k=4 capture from 10 % to 33 %.
Capture still falls as k rises — more peers to gather inside the same short window — so
the cheap attack is also the reliable one.

**Side effect worth recording:** honest group sizes became ragged under attack (183 groups of
8, but also 9 of size 3, 6 of size 4, 3 of size 5, 6 of size 6, 3 of size 7 in the k=4 run).
Rejected honest peers re-form into whatever is left. The attack degrades the honest swarm's
group formation as well as capturing the victim, which was not its aim.

**Still untried:** the tie-break for equal deadlines is the lowest `peer_id`
(`matchmaking.py:478-481`), and `identity_path` makes PeerIDs choosable, so they can be ground
offline. That would matter for the rounds the attacker still loses.

### 6.5 Measured: re-formation sheds a naive attacker, not a tracking one

`step4_reformation.py`, 16 peers, group size 4, 4 buckets, staggered start, 5 epochs. Brief item 2b.
Sharding has to be switched on first (§6.6 below explains why the default gives nothing to decay
under); the only route is `grad_averager_factory`.

| arm | attacker behaviour | shared victim's group | mean β | prediction |
|---|---|---|---|---|
| none | no attacker | 0/6 | 0.000 | — |
| naive | freezes its bucket, stays put | 1/5 | 0.067 | — |
| **tracking** | recomputes the victim's next bucket | **5/7** | **0.179** | **5/5 exact** |

**The tracking attacker locks on and never loses the victim**, including across bucket changes:

```
r0  bucket=10  [0 4 8 12]   β=0.000
r1  bucket=00  [11 0]       β=0.000
r2  bucket=00  [0 7 1 15]   β=0.250   <- locks on
r3  bucket=00  [0 2 14 1]   β=0.250
r4  bucket=01  [0 12 1 5]   β=0.250   <- victim rotates, attacker follows
r5  bucket=10  [6 1 0 11]   β=0.250
r6  bucket=10  [1 3 0 5]    β=0.250
```

The naive arm's 1/5 matches the 1-in-4 chance of a frozen bucket coinciding with the victim's, so
**re-formation does shed an attacker that declines to use information it already holds**. It sheds
nothing otherwise: every input to `update_key_on_group_assembled` is in the leader's broadcast.

**The root cause is transparency to participants, not predictability.** This is worth stating
precisely, because the obvious reading is wrong. `group_id = DHTID.generate()` with no `source` hashes
255 random bits (`routing.py:270`) — it is unpredictable to anyone outside the group, and no amount of
observation from outside yields the victim's next bucket. What breaks it is that the shuffle's *every*
input (`group_id`, `peer_ids`, public config) is handed to every member in the leader's broadcast, so
a peer that has shared **one** group with the victim computes the answer exactly, forever after. The
implied fix follows from that framing and not from the other one: seed the shuffle on a value
contributed by members and revealed only after placement, so holding the broadcast is not enough.

**Prediction accuracy is the sturdier claim.** 5/5 exact, and it is measured independently of whether
matchmaking then cooperated — the attacker wins the group only ~57 % of the time (§6.4), so β alone
understates how good the prediction is. Join predictions to outcomes on `group_id`, never on `round`:
`round` is a per-peer counter that desyncs permanently the first time two peers sit in different
groups. That error produced a spurious 2/5 before it was caught.

**Honest limits.** 5–7 rounds is a thin decay curve; the arms separate clearly but this is suggestive,
not conclusive. Longer runs are blocked by §6.6.

### 6.6 Measured: sharding starves peers, and degrades even when provisioned correctly

Found while trying to run §6.5, and all three effects trace to the same missing hook:
`update_key_on_not_enough_peers` (`key_manager.py:107-109`) is `pass  # to be implemented in
subclasses`, no subclass implements it, and `matchmaking.py:538` calls it for real.

- **Starvation.** `group_bits` only ever changes inside `update_key_on_group_assembled`
  (`key_manager.py:94`), so a peer that cannot assemble a group cannot change bucket — it is stuck
  where it is, permanently. At 16 peers: **8/16 never joined a single group** at group size 4,
  **12/16** at group size 2.
- **`matchmaking_time = 5 s` is too short once sharded.** A bucket's candidate pool is small, so
  leaders close on their deadline with 2 of 4 members; the undersized groups scatter unevenly, strand
  peers, and it cascades. At 15 s: **16/16 peers, every group full, zero failures**, and 96 s instead
  of 337 s. This is now the script default.
- **Metastability.** `rng.sample(range(num_buckets), group_size)` is a permutation only while
  `num_buckets == group_size` *and* every bucket forms one full group, so a balanced layout sustains
  itself but a single undersized group breaks the invariant permanently. Clean at 4 epochs, 63
  failures by 10. Bucket occupancy is a random walk with absorbing states.

Consequence for the brief: "decay under periodic re-formation" cannot be measured over long runs on a
swarm this size, because the mechanism degrades faster than the decay it is meant to produce.

### 6.7 Measured: lie to the victim on read — same power, no detection signal, no collateral

`step5_selective.py`, 12 honest peers + 5 crafted node_ids, one process. The §6.3 attack refuses to
*store* honest advertisements; this one stores everything honestly and filters only what the victim
*reads*. `rpc_find` receives `context: P2PContext` (`protocol.py:333`), so a responder knows the
TLS-authenticated PeerID of the reader before building its reply — the same primitive the malicious
leader uses on `context.remote_id`.

| arm | honest declarations accepted | victim sees | bystander sees |
|---|---|---|---|
| honest (control) | 12/12 | 12 honest, 0 attacker | 12 honest, 0 attacker |
| censor (§6.3) | **0/12** | 0 honest, 1 attacker | **0 honest** — swarm broken |
| **selective** | **12/12** | **0 honest, 1 attacker** | **12 honest, 1 attacker** |

**Same capture, none of the noise.** Both arms reduce the victim's candidate list to the attacker
alone. Only `censor` also (a) makes every honest `declare_averager` return False — the detection
signal §6.3 found nobody reads, but which a one-line upstream fix would expose — and (b) destroys
discovery for the whole swarm, which stalls training for everyone and is operationally obvious. The
selective arm leaves both intact: the bystander's view is identical to the control's.

**Cost is unchanged**: five identities, because dictionaries from multiple responders are merged
(`node.py:834-838`), so every replica holder must lie the same way. Total custody (§6.2) is a
precondition, not an optimisation. Exactly 5 responses were filtered per run, one per replica.

**Caching does not leak here.** `rpc_find` also answers from `self.cache` (`protocol.py:344`) and
caching is on by default, so an honest peer on the lookup path could in principle hand the victim a
cached honest copy. Measured with `--caching`: **identical result, no leak.** Stated narrowly — this
is a cold first read. A victim that had already read the key in an earlier round might answer from its
own cache, and that has not been tested.

**Why §6.3 did it the loud way:** `record_validator` is a supported `DHTNode.create` argument, so
write-censorship needs no subclassing. Selective read needs `DHTProtocol.rpc_find` patched before any
node is built (handlers bind at registration, `servicer.py:117`) and `DHTNode` rather than `DHT`, since
`DHT` hides its node in a forked subprocess. Path of least resistance, not a considered choice.

**Root cause, stated precisely.** `group_id` plays no part in this attack — do not carry the §6.5
framing across. Two properties carry it: `node_id` is self-chosen (`node.py:171`) and accepted verbatim
off the wire (`protocol.py:150`), which grants custody; and `rpc_find` receives `context: P2PContext`
(`protocol.py:333`), which lets a custodian answer differently depending on who asked. Remove either
and the attack collapses — binding `node_id` to the PeerID removes custody, and stripping requester
identity from the read path removes selectivity (though it would also break the routing-table
maintenance that `rpc_find` performs at `protocol.py:338-339`).

**Two limits on this result**, both being measured (see §6.9 once run):
- It captures the **candidate list**, not the assembled **group**. That gap is §6.3's stated caveat and
  this section inherits it. It matters especially when the victim *leads*: a peer reads the list
  regardless, and `pop_next_leader` (`matchmaking.py:479-497`) makes it wait to be contacted if no
  candidate beats it, at which point honest peers — whose reads are *not* filtered here — can still
  join it.
- The swarm was built fresh, so no cached copies existed to leak. `add_candidate` **merges**
  dictionaries from every responder (`node.py:834-837`), so a single honest peer holding a cached copy
  reintroduces every honest entry, and `cache_nearest=1` (`node.py:112`) plants such a copy after every
  successful lookup. The `--caching` arm shows no leak only because it is a cold first read.

### 6.9 Measured: discovery attacks are delayed by one record lifetime, not defeated by history

`step5_selective.py --warmup W`, 12 honest peers + 5 crafted node_ids. This qualifies §6.3 and §6.7,
both of which built the swarm fresh and so measured a **cold first read** — a key nobody had ever
written to. A real key has history.

| warm-up state | honest declarations | censor: victim sees | selective: victim sees |
|---|---|---|---|
| cold (W=0) | 12/12 sel, 0/12 cen | 0 honest, 1 attacker | 0 honest, 1 attacker |
| **warm, records live** (W≥1, 600 s lifetime) | 12/12 sel, 0/12 cen | **12 honest** — fails | **12 honest** — fails |
| warm, records expired (5 s lifetime, 8 s settle) | 12/12 sel, 0/12 cen | 0 honest, 1 attacker | 0 honest, 1 attacker |

**One warm-up round defeats both attacks outright, while any live copy remains.** Capture goes from
1.00 to 0.00. This is not a partial leak — the victim sees the entire honest swarm.

**The mechanism is residual storage, not caching.** Running with caching fully disabled gives the same
failure, so it is not the victim's own cached copy. Honest peers that stored the record *before* the
attacker arrived keep it in `protocol.storage` until it expires, they remain reachable in routing
tables, and `add_candidate` **merges** every responder's dictionary (`node.py:834-837`) — so one
surviving honest custodian reintroduces all twelve entries. The attacker is still lying: the filter
fired 10 times rather than 5. It simply is not the only source any more.

**This also qualifies the §6.3 censor arm**, which was committed before this was measured. Censoring
writes stops *new* advertisements (0/12 accepted, as before) but does nothing about copies already
stored elsewhere; the victim reads those instead.

**But the window is one record lifetime.** Re-running with a realistic 5 s advertisement lifetime and
an 8 s settle restores capture to 1.00 for both arms. Averaging advertisements are short-lived by
design — `declare_averager` sets expiration to the peer's matchmaking deadline, seconds not minutes
(`matchmaking.py:527-531`) — so in steady state every copy is refreshed continuously, and once the
pre-attack copies lapse, the attacker owns every replica that receives the refresh.

**Net effect on the claim.** "Five identities take 100 % of the victim's candidate list" stands, with a
precondition that was previously implicit and is now measured: **it holds from roughly one
advertisement lifetime after the Sybils arrive, not immediately.** For a defender that is the detection
window; for an attacker it is a short wait. §6.2's custody result is unaffected — the attacker owns the
addresses either way; what takes a lifetime is converting custody into control of what a reader sees.

### 6.10 Measured: owning the list is necessary but not sufficient, and filtering backfires

`step6_isolation.py`, 8 peers, group size 4, 12 epochs, one training attacker (α = 0.125) plus **five
DHT-only Sybils** with crafted node_ids holding the key's replicas. Answers the question §6.7 could not:
does owning the victim's *candidate list* win the victim's *group*?

| arm | victim led | group captured | mean β | honest peer in victim's group | responses filtered |
|---|---|---|---|---|---|
| honest (control) | 0/12 | 0/12 | 0.104 | 12/12 | — |
| selective | **5/12** | 4/12 | 0.188 | 8/12 | 394 |
| isolated | 3/13 | 5/13 | 0.250 | 8/13 | 21331 |

**Partly, and less than §6.7 implies.** Capture goes from 0/12 to 4-5 of 12-13 rounds, and β roughly
doubles over α. Real, but far from the 100 % list capture §6.7 reports — because capture requires the
attacker to *also win leadership*. When it merely joins the victim's group alongside honest peers, β is
diluted and nothing is captured. **Owning the candidate list is necessary, not sufficient.**

**Filtering the victim's reads makes it more likely to lead — the attack partly defeats itself.**
0/12 rounds in the control, **5/12** under `selective`. Starved of candidates, the victim finds nothing
beating its own declared deadline, so `pop_next_leader` (`matchmaking.py:478-481`) stops searching and
waits to be contacted. Hiding the victim from everyone else (`isolated`) pushes it back down to 3/13
and capture up to 5/13, the predicted direction — but at n ≈ 12 that difference is **not significant**
and should not be claimed as one.

**Two structural findings from building this, both more durable than the numbers:**

- **A declared deadline cannot be forged downward.** The `forged` arm was designed to show the victim a
  fabricated early expiration, decoupling "win the election" from "have time to assemble" — the tension
  §6.4 found limiting. It cannot work: expiration is simultaneously the record's **lifetime** and its
  **leadership priority**, one field (`key_manager.py:60`, read back at `:88`). Lowering it to win the
  election makes the advertisement expire instead of winning. The arm was abandoned rather than
  reported.
- **Custody is a precondition that is easy to omit.** The first version of this experiment gave the
  attacker a random `node_id`, so it never held the key's replicas and filtered only **55** responses
  across a whole run — the capture it reported (42-50 %) was admission control from §6.4 with the DHT
  filter barely engaged. Adding five DHT-only Sybils took that to 394. **Check the filter count before
  believing any number from this script.**

**Threat-model note.** α counts *training* peers only, because only those submit gradients. The five
Sybils hold DHT records and do no compute, which is the cheap half of the attack — a distinction worth
keeping when comparing α across experiments.

**Limits.** 12-13 rounds per arm, single run, no repeats: the ordering `honest < selective ≤ isolated`
is clear but the gap between the two attack arms is not. 8 peers rather than 16, because **16 peers
starve when `target_group_size` is bound on the gradient averager** — leader election is global, so
groups form sequentially and only one or two assemble per matchmaking window. Three 16-peer attempts
gave 4-6 of 16 peers ever averaging and **zero** victim rounds. §6.1's clique finding is the same
pathology. Note that §2's step2 runs avoided it only because `averager_opts` never reached the gradient
averager, so their leaders closed on deadline instead of holding groups open.

### 6.11 Measured: blind the victim and it leads every round — but DHT caching defeats it

`step6_isolation.py --arm chase`, 8 peers, group size 4, 3 training attackers (α = 0.375) plus 5
DHT-only Sybils, 12 epochs.

**The `chase` design.** Rather than fight the leadership race (§6.4, §6.10), remove it. Serve the victim
an **empty** candidate list: with nothing to compare against, `pop_next_leader` finds no candidate
beating the victim's own deadline, so it stops searching and waits to be contacted — it leads **by
construction**. The attackers then declare *late* deadlines so they prefer to follow, and simply join
it. Honest peers are hidden from the victim's key so they do not join too.

| config | victim led | group captured | mean β | honest peer in victim's group |
|---|---|---|---|---|
| honest / cached | 1/12 | 0/12 | 0.417 | 12/12 |
| honest / no-cache | 6/13 | 0/13 | 0.308 | 13/13 |
| chase / cached | 7/12 | 2/12 | 0.167 | 10/12 |
| **chase / no-cache** | **8/8** | **8/8** | **0.646** | **0/8** |

**With caching off the capture is total**: every round, zero honest peers in the victim's group, β =
0.646 against α = 0.375. Both honest controls give β ≈ α, which is what says the measurement is sound.

**With caching on — the shipped default — the same attack fails**, 2/12. This is the sharpest defensive
result in the project so far, and it generalises §6.9: `rpc_find` also answers from `self.cache`
(`protocol.py:344`) and `add_candidate` **merges** every responder's dictionary (`node.py:834-837`), so
any honest peer that has cached the key becomes an unwitting responder that undoes the filtering.
`cache_nearest=1` (`node.py:112`) plants such a copy after every successful lookup, continuously, in a
live training swarm.

**The principle across §6.7, §6.9 and §6.11:** an attacker holding custody can **add** to what the
victim sees but cannot reliably **remove**. Removal needs every responder to cooperate, and caching
keeps recruiting new honest ones. DHT caching is a performance feature that works as an accidental
defence against this whole class of read-filtering attack.

Stated carefully, it is an obstacle and not a guarantee: it depends on cache hit rates and record
lifetimes, and §6.9 showed that a short enough advertisement lifetime lets the attacker regain control
once pre-attack copies lapse.

**Two honest caveats.**
- *Confound.* Disabling caching by itself raises the victim's leading rate (1/12 → 6/13), so the "leads
  every round" figure is not purely the blinding. Capture going 0/13 → 8/8 is unambiguous regardless.
- *The attack can backfire.* `chase / cached` gives β = 0.167, **below** the 0.417 honest control: the
  attacker declares late deadlines to position itself as a follower, so when the blinding fails it
  simply participates less than an honest peer would. An attacker choosing this strategy against a
  caching swarm is worse off than doing nothing.

### 6.8 From reading the source, not yet measured

These came out of reading `hivemind/averaging/matchmaking.py` and are the reason the project is worth
finishing. They are code-level claims with line references. Two of them have since been measured and
say so inline; the rest have not.

- **Group membership is a race, not a random draw.** A peer advertises a self-chosen *deadline*;
  everyone sorts candidates by `(deadline, peer_id)` and the earliest becomes leader
  (`matchmaking.py:479-497`); the leader then admits followers in arrival order
  (`matchmaking.py:273`). Both criteria are attacker-controlled. **Measured in §6.4**, with one
  correction: the deadline is attacker-controllable only once the averager's `min_matchmaking_time` is
  set independently of `Optimizer`'s `matchmaking_time`, which otherwise makes every peer declare the
  same value.
- **A leader chooses its own group.** `_check_reasons_to_reject` (`matchmaking.py:341-368`) runs on the
  leader's own machine with the requester's TLS-authenticated PeerID already in hand. A modified leader
  can admit exactly one target plus its own Sybils. With `min_group_size = 2`
  (`averaging/averager.py:120`) that yields **β = 0.5 from a single identity**, and β = k/g in general —
  meaning β decouples from α entirely. **Measured in §6.4 and confirmed exactly**: β when captured is
  k/(k+1) with no variance, in 57 % of rounds at k=1.
- **Group re-formation is implemented and works; what is missing is the sharding it operates on.**
  Earlier versions of this file said it was "off by default", which was misleading. `initial_group_bits`
  is not an on/off switch — its *length* declares how many buckets exist (2^b for b bits), and the
  rotation at `key_manager.py:104` is a fixed-length sliding window, so the bucket count never changes
  after construction. The default `""` (`averaging/averager.py:120`) means **one** bucket, and with one
  bucket there is nothing to rotate between. For a swarm where `num_peers ≈ target_group_size` that is
  the correct configuration, not a defect.

  What is genuinely absent is any way to reach it from the main API, plus the adaptive sizing that
  would make it automatic:
  - `hivemind.Optimizer` exposes **no parameter** for `initial_group_bits` (nothing in `hivemind/optim/`
    references it), and `averager_opts` does not reach the gradient averager (see §4). The only route is
    binding it through `grad_averager_factory`.
  - `update_key_on_not_enough_peers` (`key_manager.py:107-109`) — the hook that would re-bucket a peer
    that cannot find partners — is `pass  # to be implemented in subclasses`, and **no subclass in the
    tree implements it**. It is called for real at `matchmaking.py:538` and does nothing.

  So a swarm sized by `hivemind.Optimizer` has one bucket and no peer ever changes group key, which is
  what §6.1 measures the consequence of. Sizing buckets to the swarm (b ≈ log2(N / group_size)) is left
  to the operator, and the operator is given no handle to do it with.

  The sharper problem is still unmeasured: even with sharding switched on, the reshuffle is seeded on
  `random.Random(group_info.group_id)` and indexed by position in `group_info.peer_ids`
  (`key_manager.py:94-104`) — and a peer sharing the victim's group holds both, *and* holds the same
  current `group_bits`, since matching group keys is a precondition for being in the same group
  (`BAD_GROUP_KEY`, `matchmaking.py:355`). Every input to the victim's next bucket is therefore known to
  the attacker. See §7 item 2.
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

Brief items 1 and 2 are measured (§6.2, §6.3, §6.4). Item 3 is the remaining one.

1. **Brief item 3: couple β to learning harm.** The pieces are all in place — `step3_capture.py`
   already produces a victim sitting in a group with a known β, and `distance_to_optimum` measures
   damage against the 0.1492 baseline. What is missing is a poisoning attacker and a defence.

   - *Poisoning* goes in an overridden `_generate_input_for_peer` on an `AllReduceRunner` subclass,
     so only what is **sent** is poisoned and the attacker's own state stays consistent — the upstream
     `FaultyAllReduceRunner` (`tests/test_allreduce_fault_tolerance.py:87`) overrides exactly that
     method. Worth covering sign-flip and large-norm (crude, should be caught by anything), a
     **stealthy targeted** bias sized to stay inside the honest gradient's norm range, and the
     **unvalidated weight** — `partition.py:242` applies a sender-supplied weight verbatim, read
     straight off the wire at `allreduce.py:340`, so a large declared weight costs nothing and needs
     no gradient crafting at all.
   - *Defence* is coordinate-wise median or trimmed mean, **not Krum**. Computing them per-slice is
     identical to computing them over the whole vector, so they work inside reduce-scatter unchanged;
     Krum needs pairwise distances between full vectors and under reduce-scatter peer *i* only ever
     holds slice *i*, so it could only be done by forcing every full vector onto one peer, which
     discards the property that makes Hivemind scale. Implement by subclassing `TensorPartReducer` to
     buffer each sender's part rather than summing it (`accumulate_part` already receives
     `sender_index`), then replacing `self.tensor_part_reducer` in an `AllReduceRunner` subclass.
   - *One design point that is itself a finding:* in reduce-scatter each peer reduces only its own
     slice and ships the result to everyone, so a robust rule protects exactly the slices its users
     own. **Every honest peer must run it or it does nothing** — one undefended slice owner re-admits
     the poison for that slice. Measure the single-defender case once to show why.

2. **Brief item 2b: does re-formation shed the attacker?** This is the half of brief item 2 that is
   still unmeasured — "how it decays under Hivemind's periodic group re-formation" — and it is the
   experiment to run next after item 3, or before it if the brief is being answered in order.

   **Turn sharding on first**, since the default gives one bucket and therefore nothing to decay under
   (§6.5). Bind `initial_group_bits` through `grad_averager_factory`, the same route `step2_groups.py`
   already uses for `target_group_size`. Size it so buckets are not empty: with 16 peers and group size
   4, `b = 2` gives 4 buckets of ~4. Verify sharding is live by checking that observed group keys differ
   across peers within a round — if every peer still advertises on `prefix.0b`, nothing was enabled.

   **Three arms, all with one attacker identity (α = 1/16):**

   | arm | attacker behaviour | prediction |
   |---|---|---|
   | none | no attacker; honest baseline | β = 0 |
   | naive | joins the victim's bucket once, then stays put | β decays toward chance as the victim rotates away |
   | tracking | recomputes the victim's next bucket each round and moves with it | β does not decay |

   **The tracking computation, from `key_manager.py:94-104`.** After any round shared with the victim,
   the attacker holds every input it needs:

   ```python
   rng = random.Random(group_info.group_id)                    # from the leader's broadcast
   index = group_info.peer_ids.index(victim_peer_id)           # same broadcast
   num_buckets = target_group_size or next_power_of_two(group_info.group_size)
   generalized_index = rng.sample(range(num_buckets), group_info.group_size)[index]
   new_bits = bin(generalized_index)[2:].rjust(int(np.ceil(np.log2(num_buckets))), "0")
   victim_next = (current_bits + new_bits)[-len(current_bits):]
   ```

   `current_bits` is the attacker's own, and it is necessarily the victim's too: matching group keys is
   a precondition for having shared a group at all (`BAD_GROUP_KEY`, `matchmaking.py:355`). So one
   shared round is enough to follow the victim indefinitely.

   **Measure**, per round: the victim's bucket, the attacker's bucket, whether they shared a group, and
   β. Plot β against round for all three arms. **Validate the predictor independently** by recording
   the attacker's predicted next bucket alongside the victim's actual next bucket and reporting exact
   agreement — that is a cleaner claim than β alone, because it isolates the prediction from the
   matchmaking race that §6.4 showed the attacker only wins ~57 % of the time.

   If it holds, the finding is that re-formation is unpredictable to an outsider but transparent to a
   participant, so it sheds only an attacker that declines to use information it already has.

3. **Loose ends, in descending order of interest:**
   - **Grind a low `peer_id`.** The tie-break for equal declared deadlines is the lowest `peer_id`
     (`matchmaking.py:478-481`) and `identity_path` makes PeerIDs choosable, so they can be ground
     offline. That is the lever for the ~40 % of rounds the attacker still loses (§6.4).
   - **Settle the §6.1 limitation** by holding the peer count fixed and varying timing noise
     deliberately, to see whether the clique structure follows the noise rather than the peer count.
   - **Censorship, as opposed to custody.** §6.2's attacker nodes store and serve honestly. Refusing
     to serve on `rpc_find` is a separate experiment; §6.3 already does the store-side half with
     `record_validator`.

4. **A free detection signal nobody uses**, if the project ever turns to defences: honest peers whose
   advertisement is refused by every replica get `False` back from `declare_averager` and throw it
   away (`matchmaking.py:535`, and again at 544; no other caller checks it). §6.3 measured 0/32
   honest peers noticing they had been erased from discovery.

Before claiming novelty on β thresholds, read *Secure Distributed Training at Scale* (BTARD, ICML 2022,
arXiv:2106.11257), especially its threat model and Appendices F and G. It assumes a **fixed** Byzantine
set with |B| ≤ δn, δ < ½, and defends the *values*. It does not address who ends up in which group,
which is the gap this project is aimed at.

---

*Documentation only; part of the additive `research/` tree. No core Hivemind code has been modified.*
