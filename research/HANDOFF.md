# Handoff — read this first

State of the project, why it is built this way, and how to pick it up on another machine. Written so a
reader with no prior context (human or assistant) can continue without the original conversation.

Last updated: 2026-09-10 · branch `research/net-security` · remote `analysis`

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
literature assumes) or something an attacker *chooses*. See §6.

**Standing constraint: never modify anything under `hivemind/`.** All work is additive under
`research/`. Every extension uses a supported hook or a subclass.

---

## 2. Where things stand

**Done and verified:**

| File | What it does |
|---|---|
| `research/step1_baseline.py` | N peers, real DHT + real `hivemind.Optimizer`, training linear regression with a known optimum `w*`. Establishes the clean baseline. |
| `research/step2_groups.py` | Same, plus records **which peers shared a group each round**. This is the control for β. |

Results in `research/results/*.csv` (tracked in git).

**Verified 8-peer baseline** (`step2_groups.py --num-peers 8 --group-size 8 --max-epoch 20`):

| epoch | mean distance to `w*` | spread across peers | peers seen |
|---|---|---|---|
| 0 | 7.9173 | 0.0000 | 8 |
| 10 | 1.0504 | 0.0000 | 8 |
| 20 | **0.1479** | 0.0000 | 8 |

20 rounds, 160 group observations, group size always 8, no missing epochs, 122 s wall clock.
**0.1479 is the number "unbroken" means.** Every later attack result is compared against it.

**In flight, not yet recorded:** a 16-peer run with `--group-size 8`, giving two concurrent groups per
round. That is the first configuration where group membership is a real choice and the co-occurrence
matrix means anything. If it did not complete, just re-run it.

**Not started:** anything adversarial. Steps 1–2 are the honest control only.

---

## 3. The one thing that will bite you

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

---

## 4. Decisions already made, and why

- **Model: linear regression as `nn.Linear(64, 1, bias=False)`, MSE loss, data generated from a known
  `w*`.** Real enough to exercise the whole stack through `hivemind.Optimizer`, but with an exactly
  known answer so any displacement is attributable to an attack rather than to noise in a model you
  cannot inspect. A real neural net would remove that — you would be arguing about whether a loss dip
  was the attack or ordinary variance.
- **16 peers, group size 8.** Memory measured on the dev machine: **~350 MB per peer** in PSS terms,
  4.9 GB for 16 peers. Each peer is **4 Python processes plus one `p2pd`** (trainer, DHT, gradient
  averager, state averager) because `DecentralizedAverager` extends `mp.Process`. On a 15.2 GB machine
  with ~8.5 GB otherwise in use, 16 is the practical ceiling; 24–32 will not fit. Adjust for your box.
- **β granularity is 1/group_size**, so group size 8 gives steps of 0.125. Raise the *group* size, not
  the peer count, for a finer β sweep.
- **`target_group_size` is set explicitly.** Left at its `None` default, groups close on a timer and
  their size varies run to run, which would make β an unstable denominator.
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

python -m venv .venv                 # Python 3.13 used on the dev machine
.venv/bin/pip install -e .           # also downloads the p2pd binary (gitignored)
.venv/bin/pip install torch --index-url https://download.pytorch.org/whl/cpu

.venv/bin/python research/step2_groups.py --num-peers 8 --group-size 8 --max-epoch 20
```

Expect the 8-peer numbers in §2. If `peers visible to each other` reports 0 or 1, or the spread is not
0.0000, re-read §3 before trusting anything — and raise `--batch-time` if the machine is slower.

**What does not travel with the repo**, and is why this file exists: the conversation history, the plan
file under `~/.claude-account2/plans/`, and the assistant memory directory. Everything load-bearing has
been written into `research/` instead. If you learn something that matters, add it here rather than
leaving it in a chat log.

---

## 6. Findings so far (from reading the source, not yet measured)

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
  decay under, unless the feature is switched on explicitly.
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

1. **Finish the 16-peer run** and look at the co-occurrence matrix. Under fair sampling each pair of
   peers should land together roughly half the time. Strongly peaked instead would mean stable cliques
   form on their own — a finding about Hivemind itself, since nothing forces mixing when group keys
   never change.
2. **Step 3: a malicious leader.** Subclass `DecentralizedAverager` and override `rpc_join_group`
   (which delegates at `averaging/averager.py:574-578`) to filter on `context.remote_id`. The upstream
   test suite already does this kind of subclassing —
   `tests/test_allreduce_fault_tolerance.py:32,87` defines `FaultyAverager` and `FaultyAllReduceRunner`
   — so core Hivemind stays untouched. Measure β achieved vs. attacker count k, with an honest-leader
   control arm that must reproduce β ≈ α.
3. **Then** couple β to learning harm, reusing `distance_to_optimum` against the 0.1479 baseline.

Before claiming novelty on β thresholds, read *Secure Distributed Training at Scale* (BTARD, ICML 2022,
arXiv:2106.11257), especially its threat model and Appendices F and G. It assumes a **fixed** Byzantine
set with |B| ≤ δn, δ < ½, and defends the *values*. It does not address who ends up in which group,
which is the gap this project is aimed at.

---

*Documentation only; part of the additive `research/` tree. No core Hivemind code has been modified.*
