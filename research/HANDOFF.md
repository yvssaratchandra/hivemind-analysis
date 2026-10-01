# Handoff — read this first

Everything a new session needs: what the project is, where it stands, what is known, and what to do
next. Written to be read without any prior conversation.

Branch `research/net-security` · remote `origin` (the fork) · last updated 2026-10-01

---

## Current thread — prioritized attack experiments (2026-09-30)

**Moving-machine handoff (2026-10-01).** The latest WAN-like timing result is a *local emulation*,
not a crawl of a public swarm. Its source measurements, harness, exact CSV traces and interpretation
are in `results/ripe_anchor_rtt_20260930.json`, `step19_wan_delay_leader.py`,
`results/step19_wan_delay_{rounds,events}_*.csv` and `CONTROL_DATA_AUDIT.md` C12.S. The profile has
five fully connected regions (SE, US, BR, IN, AU), with a median cross-region ICMP RTT of 233.4 ms
and pair medians from 99.1 to 503.0 ms. It is a small spatial sample, **not** a representative
worldwide latency distribution or a measurement of Hivemind TCP/libp2p traffic.

The decisive paired short-window runs used 28 DHT peers, four averagers and a 5–7 s honest deadline
window: the one-ULP undercut captured **3/3** zero-delay groups but **0/3** with measured RTTs. One
delayed ad became visible yet drew no followers. With an 11–15 s window, a matched delayed run gave
**1/1** capture for undercut versus **0/1** for the late-deadline control. All honest steps completed.
The local emulator delays DHT find/store and join-stream RPCs, but not packet loss, churn, DHT pings,
connection setup or all-reduce. The exact group key is known to the attacker. Do not report these
counts as a public-WAN success rate or as proof of 100% leadership.

To inspect or rerun on the new machine, use the repository Python environment from the repo root:

```text
python research/step19_wan_delay_leader.py --phase smoke --scales 0 1 --arms undercut --fillers 23 --rounds 1 --seed 1922 --schedule-min 5 --schedule-max 7 --label NEW_SHORT_CHECK
python research/step19_wan_delay_leader.py --phase smoke --scales 1 --arms late undercut --fillers 23 --rounds 1 --seed 1925 --schedule-min 11 --schedule-max 15 --label NEW_LONG_CHECK
```

Use a new label so existing CSVs are not overwritten. `step19` imports `step15_adaptive_leader_live.py`
and uses the saved RIPE profile by default; it starts many local processes and needs localhost socket
access. First inspect the committed CSVs before rerunning. The next *small* observational test is a
passive reader on a CPU-only local Hivemind swarm: record each DHT read duration, the earliest active
advertised deadline, and remaining time **at read completion**. This tests the crawler's method, not
public-swarm availability. Scale to a multi-host test only if this gate produces usable measurements.

**Prime Intellect lead.** Its [April 2024 overview](https://www.primeintellect.ai/blog/our-approach-to-distributed-training)
mentions Hivemind and a base image for launching one's own run, not a public bootstrap and group key.
Its Hivemind-based [OpenDiLoCo](https://github.com/PrimeIntellect-ai/OpenDiLoCo) is explicitly marked
unmaintained. The [INTELLECT-1 run](https://www.primeintellect.ai/blog/intellect-1-release) has ended;
its [PRIME framework](https://github.com/PrimeIntellect-ai/prime-diloco) publicly documents an
ElasticDeviceMesh rather than the Hivemind matchmaking path studied here. No currently joinable public
Hivemind averaging swarm was verified from these sources. Prime's published
[OpenDiLoCo training script](https://github.com/PrimeIntellect-ai/OpenDiLoCo/blob/main/open_diloco/train_fsdp.py)
requires CUDA/NCCL;
the original machine had CPU-only PyTorch, so the relevant low-cost path is a toy Hivemind control-plane
swarm, not full OpenDiLoCo training. A new machine's hardware should be checked afresh.

`CONTROL_DATA_AUDIT.md` Part 3 is the canonical **four-track** queue. The rule for every track is fixed:
start with the smallest mechanism test, inspect its trace and controls, and run a larger confirmation
only when that gate shows a signal.

Track 1—adaptive deadline undercutting—has **conditional local evidence, not public-WAN attack
evidence**. `step14`/`step15` used only five localhost DHT nodes below the default bucket size.
The new `step19` follow-up used 28 DHT peers (23 routing-only), real Hivemind matchmaking and
all-reduce, and local delays derived from RIPE Atlas anchor pings. With the same known key and DHT
IDs across paired arms, a 5–7 s deadline window yielded **3/3** zero-delay captures but **0/3**
measured-delay captures. At 11–15 s, the measured-delay undercut captured **1/1** against **0/1**
late-deadline controls; two earlier seeds also captured **2/2**. This is a latency-sensitive routed
localhost result, not a live deployment result: ICMP pings are only a small proxy sample, and the
emulator omits loss, churn, DHT ping and connection-setup delay. A genuinely unknown group key remains
untargetable. C12.S has the exact traces and limitations.

Track 2—malicious-leader roster/index equivocation—is now confirmed and is the stronger result.
The roster position in `BEGIN_ALLREDUCE` is the reducer index, but followers do not verify that peers
received one common roster. `step16_roster_equivocation.py` first used the real libp2p all-reduce with
two peers: the honest view produced `[5,550]`, while views `[L,F]` and `[F,L]` completed without errors
but produced `[500,55]` and `[55,500]`. `step17_roster_equivocation_live.py` then used four real
`DecentralizedAverager` peers and rewrote the actual `BEGIN_ALLREDUCE` message separately for each
follower, retaining the same four authenticated PeerIDs and group ID.

After a 1/1 smoke gate, five fresh paired confirmations produced **0/5** silent corruptions in the
common-roster control and **5/5** under equivocation. Every control output was correct; all **20/20**
attacked peer outputs were wrong even though all steps completed. There were zero reducer bans,
fallback elements, errors, timeouts, harness cancellations or cleanup leaks. This attack assumes the
adversary is already the legitimate group leader; that precondition was forced rather than measured.
The malicious action itself uses existing authenticated join streams and does not rely on DHT timing.
Results are under `research/results/step17_roster_live_*_20260928_{smoke,confirm}_*.csv`.

Track 3—routing admission near the group key—is complete as a mechanism test. `step18_routing_admission.py`
used real DHT/libp2p pings and find RPCs. Its small gate used 9 honest nodes, 4 attackers and
`bucket_size=4`; four paired default-parameter trials used 21 honest nodes, 20 attackers and
`bucket_size=20`. Across the five closest honest nodes, targeted identities occupied **96, 100, 96,
and 96 of 100** returned lookup slots, versus **60, 45, 50, and 45** for random identity controls.
The last trial found the five targets by an ordinary DHT traversal, and an honest advertisement
remained readable. **No live honest routing entry was displaced.** Some attacker identities stayed
on replacement lists and were not returned. This reinforces the admission precondition for C7.S;
flooding alone did not cause denial or censorship. All runs were localhost and warmed the honest
tables before introducing the attacker, so they do not measure public-WAN timing or churn.

The next queued experiment is track 4: controlled failure of the five closest DHT nodes, measuring
lookup success, latency and recovery.

## Previous thread — public-WAN matchmaking feasibility (2026-09-28)

The remaining WAN question is whether reading current averaging advertisements, choosing the earliest
*joinable* deadline, and publishing a slightly earlier one can win leadership across the public
internet. `step15` observed **10/10** local captures versus **0/10** late controls, but that
below-`bucket_size` result is rejected as evidence for this WAN claim. The earlier 100% follower-share
figure also came from an election calculation without the live group's capacity limit. `step19` now
tests read-to-publication and end-to-end election on a routed local swarm with measured-delay emulation;
its results are conditional on the deadline window, and **no public-WAN result has been measured**.

The agreed first study is observational: find an active public collaboration using Hivemind's actual
`DecentralizedAverager`/`Optimizer` matchmaker and a published bootstrap peer plus exact averaging
group key (or run prefix and current group bits). Sample active advertisements and timed DHT reads
frequently enough to see deadlines that may last only seconds, over a shorter period than the original
24-hour/every-few-minutes suggestion. Record the deadline margin **at read completion**. Publication,
visibility to other peers, and follower-contact times need a separate measurement; a read-only crawl
cannot establish them, and a positive timing margin would be necessary but not sufficient evidence
of an election win. The cadence and duration are not yet chosen. As of 2026-10-01, the user wants to
start with a small **local passive-crawler gate** before any public-swarm work. Do not contact a live
swarm without verifying its operator, bootstrap address, exact key and permission/scope.

**Target search, 2026-09-28:** no suitable *currently live* public averaging swarm was verified from
project sources and status pages. Petals advertises inference servers, not this averaging election,
and its [health dashboard](https://health.petals.dev/) could not be checked. [Gensyn RL Swarm](https://docs.gensyn.ai/testnet/rl-swarm)
is officially paused; its Hivemind use is DHT gossip, not averaging matchmaking. [Pluralis Node0](https://github.com/PluralisResearch/node0)
was a genuine public Hivemind training run but is historical. Current [Agora](https://github.com/PluralisResearch/agora/blob/main/agora_server/src/agora_server/core/all_reduce/matchmaking.py)
forms groups by collected peer IDs rather than earliest expiration, and its [published run config](https://github.com/PluralisResearch/agora/blob/main/run.json)
has no seeds. [OpenDiLoCo](https://github.com/PrimeIntellect-ai/OpenDiLoCo) used Hivemind averaging
but is unmaintained. This does **not** prove that no community run exists. The search did not connect
to any swarm; next find a current operator-published bootstrap and run prefix, then verify its code
path before designing the observation.

**Local-only draft:** `research/observe_dht_key.py` is untracked, unrun and not on the remote. It was
created before the user asked for code; its defaults do not reflect the shorter, more frequent study
now under discussion. Do not treat it as an approved or validated crawler, and do not commit it as
part of this handoff.

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

Beyond the brief: **§4.12** — a forged withdrawal removes a peer from discovery with one DHT write, no
crafted identities and no custody, and the peer cannot undo it. Measured with a control, it does **not**
convert into group capture; it is a censorship and availability primitive. Do not cite it as a capture
attack.

**§4.13** — a leader that accepts a follower and then goes silent can deny it averaging, at the cost
of one identity and a `sleep()`. The E01 `reject` control is complete: immediate refusal lost no steps
while stalling lost 24/360 at 15 honest peers. The original two-peer spike overstated the general
severity; recovery depends on how many alternative peers are available (§4.16).

**§4.15** — checkpoints 11 and 12. An attacker that answers `rpc_find` late, but inside
`wait_timeout`, is never blacklisted and makes advertisements at hivemind's default 5 s lifetime
invisible; cost is one crafted address. Reading the group key and undercutting the earliest joinable
deadline wins the *comparison* in the local simulation; §4.16 then confirms leadership in a live local
swarm while limiting actual followers to `target_group_size - 1`. No public-WAN timing result exists.

**§4.14** — four short GitHub-issue-format reproductions of the identity flaws, written to be handed
to a reviewer on their own. These formalise what §4.1 assumes rather than adding new attacks, except
`forged_pairs_report.md`, which is a **negative** result worth keeping.

**Thesis supervision material is separate from the research.** `THESIS_AD.md` is the advertisement
for the 30 hp projects starting 21 January 2027, and now opens with a portal-format short
description under the chosen title *Securing Aggregation Coordination in Decentralized AI Training*.
`THESIS_TOPIC.md` holds the delegation split: Petals and the Hivemind depth stay on this track,
smaller platforms go to students. Two decisions in the ad are load-bearing and easy to undo by
accident — the scope is the **whole aggregation pipeline** (when a round starts, who is admitted,
what role and weight), not matchmaking alone; and it is written in **functions rather than Hivemind's
mechanisms**, so that a student on a gossip- or coordinator-based platform does not conclude the
project fails to apply.

**`CONTROL_DATA_AUDIT.md` is where the work is now driven from.** It maps 21 control checkpoints
across six layers, crossed with observe / delay / deny / tamper / select, and carries a worksheet per
filled cell. It supersedes this file for *what to do next*: the map decides the order, not intuition.
Two definitions in it matter and are not obvious. The control plane is defined **by function, not by
layer**, so all 21 checkpoints qualify even where the surrounding layer moves tensors. And a cell is a
family of experiments, split by direct versus induced and by **omission versus commission**, which is
the distinction that turned out to matter most: every defence hivemind has detects silence, and every
attack demonstrated in this project is a peer that answers and lies.

**`FINDINGS.md` is the argument on its own** — background, the six control-plane surfaces, the findings
that matter, limitations, future work — written to be handed to someone with no context. This file is
the working record behind it: every run, every trap, every falsified hypothesis. Start there, come here
for detail.

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
| `step8_withdrawal.py` | Forging a withdrawal: deleting a peer from discovery with one DHT write. |
| `step9_stall.py` | §4.13. A leader accepts a follower then sends nothing. `--no-attack` is the baseline. |
| `reproduce_node_id.py` | A peer chooses its own `node_id`; honest peers accept it and route records to it. |
| `reproduce_id_pairing.py` | `(node_id, peer_id)` pairs ride in every lookup answer, asserted and unchecked. |
| `reproduce_forged_pairs.py` | Whether forging those pairs is *exploitable*. Mostly not — see §4.14. |
| `reproduce_group_key_capture.py` | The group-key hash protects nothing: K is a forward hash of a public string. |
| `step10_delay.py` | §4.15. Six phases at checkpoints 11 and 12: observe, delay, staleness, end-to-end averaging, leader election, and baseline write/read cost. |
| `step11_abstain.py` | E01. Advertise, never follow, and vary the response to arrivals: honest, serve, reject, stall. Carries the `reject` control for C13.D. |
| `step13_blind_leader.py` | Fixed-deadline passive-leader pilot. Useful mechanism check, not the realistic adaptive result. |
| `step14_adaptive_leader.py` | Track 1 stage A: read real DHT advertisements, undercut the earliest joinable deadline, and verify ordering independently. |
| `step15_adaptive_leader_live.py` | Track 1 stage B: the same adaptive policy against real local matchmaking and all-reduce, with a late-deadline control. |
| `collect_ripe_anchor_rtt.py` | Collect a small reproducible RIPE Atlas anchor-mesh ping matrix for WAN-delay emulation. |
| `step19_wan_delay_leader.py` | Track 1 routed local follow-up: compare adaptive leadership with and without measured pairwise RTTs. |
| `step16_roster_equivocation.py` | Track 2 stage A: smallest real-libp2p test of conflicting reducer indices and silent coordinate mixing. |
| `step17_roster_equivocation_live.py` | Track 2 stage B: a malicious leader rewrites actual per-follower `BEGIN_ALLREDUCE` rosters in a live local swarm. |
| `step18_routing_admission.py` | Track 3: compare random versus key-adjacent identities in five nearest honest peers' routing tables and actual find responses. |
| `make_map_sheet.py` | Renders Part 1 of the audit as a one-page PDF. Parses the audit, so it cannot drift. |
| `make_timing_sheet.py` | Renders the DHT timing reference as a one-page PDF from the result CSVs. |

Each `reproduce_*.py` has a matching short report: `node_id_creation_report.md`,
`id_pairing_report.md`, `forged_pairs_report.md`, `group_key_capture_report.md`, plus
`stall_report.md` for `step9`. These are written in the user's own voice for a supervisor — keep that
register if you edit them. `REPORT_node_id.md` is a longer earlier version of the first.

Results are CSVs in `research/results/`, tracked in git, named for their configuration:
`step2_groups_16x8.csv` = 16 peers at group size 8; `_b` = independent repeat;
`step4_reformation_<arm>_n<peers>_g<group_size>[_k<attackers>][_r<repeat>].csv`;
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

## 3. Four traps that cost the most time

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

### Trap 3 — the RPC handler name embeds the servicer class name

Cost two full runs on `step9_stall.py`. Handlers are registered under `<prefix>::<ClassName>.<method>`,
so a swarm mixing stock `DecentralizedAverager` with a subclass **fails to connect in both directions**:

```
ControlFailure: Connect failed. msg=failed to negotiate protocol:
  protocols not supported: [stall_spike::DecentralizedAverager.rpc_join_group]
```

It presents as ordinary matchmaking failure — `failed to request potential leader`, repeating forever —
not as a naming error, and only the buried `ControlFailure` names the cause. **Every peer must be the
same class**, with attacker behaviour switched by a constructor flag. `step6_isolation.py` does this
already (all peers are `IsolatingGradientAverager`, honest ones with `admit=None`); `step9_stall.py`
takes `stall=False`.

Related, and also load-bearing: overriding an RPC handler **requires type annotations**, because
`p2p/servicer.py:_collect_rpc_handlers` reads them to register the handler and raises at startup
without them.

### Trap 4: `pgrep -f` matches the shell that is running it

Cost a 37 minute dead job and, worse, three wrong status reports. A background run was wrapped in
`while pgrep -f "step10_delay.py --phase 5" >/dev/null; do sleep 5; done` so it would wait for an
earlier run to finish. That pattern matches the wrapper's **own** command line, so the condition was
permanently true and the queued work never started. Checking on it with `pgrep -f "phase 6"` had the
same fault and kept reporting "still running" when nothing was.

Two habits fix it. Use a bracket pattern, `pgrep -f "[s]tep10_delay"`, which cannot match the literal
string in its own argv. And when a check disagrees with reality, resolve it by PID
(`ps -eo pid,etime,args`, then `kill -0`) rather than trusting a second grep of the same shape.

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

#### Scaled: the prediction never degrades, converting it to co-occurrence does

`group_size` is also the bucket count, so **peers per bucket = `num_peers / group_size`** is the
variable that matters, not swarm size. At 16 peers a bucket holds exactly one group, so predicting the
bucket *is* joining the group. Swept with **4 attacker identities** throughout, 2 repeats, 12 epochs:

| peers | per bucket | none | naive | **tracking** | bucket predicted |
|---|---|---|---|---|---|
| 16 | 4 = 1 group | 0 % | 56 % | **84 %** | 12/12, 13/13 |
| 32 | 8 = 2 groups | 0 % | 40 % | **72 %** | 14/14, 11/11 |
| 48 | 12 = 3 groups | 0 % | 18 % | **38 %** | 3/3, 10/10 |

**The prediction mechanism is exact at every size: 63 of 63 across six runs.** It cannot degrade with
scale, because it is a shared seed and an index — neither depends on how many peers exist.

What degrades is turning a correct bucket into a shared *group*: 84 % → 72 % → 38 % as a bucket goes
from holding one group to three. **Tracking still roughly doubles the naive control at every size**
(84/56, 72/40, 38/18), and the `none` arm is 0 % throughout, so the effect is the tracking itself and
not the extra identities.

**Two things this scaling exposed, both worth carrying forward:**

- **Tracking has a bootstrap problem.** A peer can only predict if it is *in* the victim's group to read
  the `group_id` and member list. With a **single** attacker at 48 peers it never got in across 9
  rounds — no seed, no tracking, nothing to measure. Identities are ~0.1 s each (§4.1), so the realistic
  attacker runs several and needs one of them to land beside the victim once. `--attackers` now makes
  this a variable, which turns "does tracking work" into "how many identities does tracking cost".
- **That same flag inflates the controls**, because four frozen identities get four independent chances.
  The naive rates above are far higher than §4.4's original single-attacker 1/5. **Compare tracking
  against naive at matched `k`**, never against the older numbers.

**Where this points:** tracking reduces "find the victim in the whole swarm" to "find the victim in one
bucket", exactly and indefinitely. Converting that to group membership is a separate step, and §4.3's
admission control is the thing that does it exactly. The two compose and have not been measured together.

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

**The cost is `bucket_size` crafted identities, not `num_replicas`.** A 28-run campaign, every run 15
epochs at 8 training peers / group size 4 / 3 training attackers, **caching on unless stated**.
"Captured" counts rounds where the victim's group held no honest peer.

| block | config | runs | capture rate | mean β | honest in victim's group |
|---|---|---|---|---|---|
| control | 5 Sybils (custody only) | 2 | **0 %** | 0.350 | present every round |
| threshold | 10 Sybils | 2 | 3 % | 0.391 | present |
| threshold | 15 Sybils | 2 | 13 % | 0.305 | present |
| threshold | 18 Sybils | 2 | 14 % | 0.436 | present |
| **threshold** | **20 Sybils = `bucket_size`** | 2 | **100 %** | 0.657 | **none** |
| threshold | 22 Sybils | 2 | 100 % | 0.688 | none |
| headline | 25 Sybils | 3 | **100 %** | 0.706 | **none** |
| headline | 40 Sybils | 2 | 100 % | 0.696 | none |

**The step is exactly at `bucket_size`.** 18 identities give 14 %, 20 give 100 %. Below the threshold the
attack is partial and probabilistic — honest peers left in the seed set sometimes fail to answer in time
— and at the threshold it is total.

**Three controls, each ruling out a different alternative explanation.**

| control | result | rules out |
|---|---|---|
| 5 crafted + **20 honest filler nodes** (same node count as 25 crafted) | **0 %**, honest present | node count, process load |
| `--no-cache` at 25 Sybils | **100 %**, identical to caching on | caching being the barrier |
| honest arm at `bucket_size` 20, 40, and with 20 fillers | **0 %** captures, honest present | the swarm being broken |

**The causal prediction was tested and held.** If the cost is `bucket_size`, doubling `bucket_size`
should double it. At `--bucket-size 40`:

| Sybils | capture rate | honest in group |
|---|---|---|
| 25 (enough for bucket 20, not for 40) | **9 %** | present |
| 45 | **100 %** | none |

The same 25 identities that capture totally at `bucket_size` 20 fail at 40. That is a falsifiable
prediction of the seed-set account which the swarm-size and caching accounts do not make.

**Why.** A lookup seeds its traversal from the `bucket_size` nodes nearest the key in the victim's
routing table (`node.py:629-633`, default 20 at `node.py:104`). Five crafted identities win *custody* —
they are the `num_replicas = 5` nearest, so they hold the record (§4.1) — but they are only 5 of the 20
seeds. The other 15 are honest, answer truthfully, and `add_candidate` merges their answer back in
(`node.py:834-837`). Own all 20 and the filtering is total. The filtered-response count rising from
~2.7k at 5 Sybils to 34–52k at 25–40 is the same fact seen from the attacker's side.

**This costs essentially nothing.** §4.1 measured crafted identities at ~0.1 s each with no compute, so
20 identities is about two seconds of minting. The attack needs no caching-free network, no shrunken
routing table, and no small swarm.

**Caching sets the price: it is what makes 5 identities insufficient.** The full 2×2, from the recorded
runs:

| Sybils | caching | captured |
|---|---|---|
| 5 | **off** | **8/8** |
| 5 | on | **0/15** |
| 25 | on | 15/15 |
| 25 | off | 7/7 |

**With caching off, `num_replicas` = 5 identities are enough.** `rpc_find` answers `NOT_FOUND` unless the
peer holds the key in `storage` *or* `cache` (`protocol.py:343-351`), so with caching disabled only the 5
replicas can answer at all and owning them is total control. Caching gives the other 15 seeds copies to
answer with — `cache_on_store` (a peer caches its own advertisement after writing it), `cache_locally`
(caches what it reads), `cache_nearest=1` (each lookup plants a copy on a peer along the path) — and then
you need all 20.

So the law is not "you need `bucket_size` identities" flatly. It is: **own every peer the victim asks
that has a copy.** Caching decides how many that is, raising the cost 4× from `num_replicas` to
`bucket_size`. It is not a defence — 20 identities is ~2 s of minting (§4.1) — but it is not irrelevant
either, and an earlier version of this section was wrong in both directions.

**Two hypotheses were falsified getting here. Do not re-run them.**

- *"Caching defeats it."* Half right, and the correction matters. Caching does not *defeat* the attack:
  at 25 Sybils capture is 100 % with caching on and with all three knobs disabled. But the claim that
  capture is "identical with caching on and off" was only ever tested at 25 Sybils, where the seed set is
  already fully owned. **At 5 Sybils it is 8/8 off versus 0/15 on.** Caching changes the required number
  of identities, not the outcome once you have them.
- *"The swarm must be larger than the routing table."* Shrinking `bucket_size` to 4 restores the attack
  at 5 Sybils (7/11 captured) which looked like confirmation — but that is the same seed-set law, since
  5 crafted identities cover a seed set of 4. Padding the swarm with honest DHT-only nodes to
  **26 → 54 → 104 nodes** at `bucket_size 20` left capture at 0–1/15 throughout. Idle fillers never
  enter the victim's routing table, so from the victim's point of view the swarm never grew. Total node
  count is irrelevant; seed-set occupancy is everything.

**All 28 runs had all 8 training peers visible to each other**, so averaging was real throughout (§3).

**The principle across §4.5, §4.6 and §4.7, corrected: an attacker with custody can *add* to what a
victim sees but cannot reliably *remove* — until it owns the victim's whole seed set.** Removal needs
every responder the victim asks to cooperate. Custody (5 identities) is not enough, because the victim
asks `bucket_size` peers, not `num_replicas`. Owning all of them is the real precondition, and it is
cheap.

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

### 4.15 Checkpoints 11 and 12: answering late, and winning every election

`step10_delay.py`. Full worksheets are in `CONTROL_DATA_AUDIT.md` under C11.Obs, C11.Delay and C12.S;
this is the summary.

**Observation is free and total (C11.Obs).** One ordinary peer, random `node_id`, no custody, sees
8/8 advertisers in 3 ms with every declared expiration readable. That expiration *is* the leadership
priority (`matchmaking.py:478-481`), so an ordinary peer reads the election order before the election
happens. Every Select result in this project assumed this capability; now it is measured.

**A slow answer is not diluted (C11.Delay).** `get_averagers` calls `dht.get(key, latest=True)`
(`key_manager.py:79`), which sets `sufficient_expiration_time` to infinity (`node.py:542-543`), and
`add_candidate` ends a search only when `expiration >= sufficient` (`node.py:844-845`), which against
infinity is never. **A matchmaking read cannot short-circuit**, so one slow responder sets the whole
read time: 0.5 -> 0.504 s, 1.5 -> 1.506, 2.9 -> 2.913, against 0.003 control. It does **not stack**:
1, 2 and 4 slow peers all give ~2.91 s, because traversal is parallel and tracks the slowest peer
contacted.

**There is a cliff at `wait_timeout`, and it cuts both ways.** Below 3 s the attacker is never
blacklisted. At 3.5 s latency returns to control *and the peer is blacklisted*, the RPC having timed
out. Crossing the timeout converts commission into omission and the existing defence then works. This
is the first time in this project that one of hivemind's own defences has been observed firing.

**The harm is to a moving advertiser set.** A new advertiser reaches the control in ~50 ms every
time; under delay it can be missed entirely, and the invisibility threshold tracks the delay (about
2x). `min_matchmaking_time` defaults to 5.0 s (`averager.py:121`) and the advertisement expiration is
`now + min_matchmaking_time`, so **the shipped lifetime sits inside the band a 2.9 s attacker makes
invisible**. The record is fetched and then dropped: candidates merge per subkey into a
`DictionaryDHTValue` (`node.py:832-835`), but `get_averagers` reads it with `.items()`, which calls
`_remove_outdated()` first (`timed_storage.py:96-99`).

**Cost is one crafted address, not one lucky peer.** With a *random* `node_id` the effect decays with
swarm size: reliable at 8 and 24 peers, only 3 of 6 repeats at 48. With a `node_id` at XOR distance 0
from the group key, 4 of 4 at 48 peers. Attacker count is identical in both arms, so the difference is
address choice alone. End to end, stock averagers plus one peer that contributes no gradients: at 4
peers 17/24 steps averaged against 24/24 control; at 8 peers with group size 4 the pattern was
0/8, 0/8, 0/8, 8/8, bimodal rather than graded.

**Leadership can be won outright (C12.S).** `pop_next_leader` compares **tuples**,
`(expiration, peer_id.to_bytes())`, so an attacker can either match the earliest joinable deadline and
take the tie on a low `peer_id`, or undercut it by `np.nextafter`, which at these absolute timestamps
(~1.79e9) costs **2.4e-7 s** of advertisement life. In the *independent per-peer election
calculation*, both were preferred by 100% of followers at 32 and 128 peers, two repeats each; the
undercut variant held the lowest `peer_id` in **0 of 40** samples. Both tolerated 2 s of injected
think-time between reading the key and declaring, but DHT operations were loopback. The comparison
does not decay with swarm size; live follower share does, because of group capacity (§4.16).

**This corrects C11.T.** That worksheet recorded undercutting as *prevented, do not retry*, on the
grounds that lowering the deadline shortens the advertisement. True for lowering by seconds, false for
one float step. The verdict was written without a bound and that is what made it wrong; the map now
reads `partly X`.

### 4.16 E01: abstention is free and invisible, and harms nobody

`step11_abstain.py`, 15 honest peers plus one attacker, group size 4, 6 repeats. Peer count chosen
so no arm carries a structural orphan. Full worksheet in the audit's Part 1b.

| arm | averaged | median | p90 |
|---|---|---|---|
| honest baseline | 384/384 | 0.11 s | 0.16 s |
| abstain + serve | 360/360 | 0.10 s | 0.14 s |
| abstain + reject | 360/360 | 0.10 s | **5.25 s** |
| abstain + stall | **336/360** | **3.09 s** | 5.25 s |

The attacker sent **zero** `rpc_join_group` messages in every arm while receiving 36, 60 and 88, so
abstention works exactly as described and requires no training contribution while it is not selected.
Its CPU, memory and network costs were not measured. But **it harms nobody**: serving and
rejecting are indistinguishable from the baseline. Declining to follow is a free and untraceable
*position*, not an attack. All the harm is the hostile act performed from it.

**The `reject` control settles §4.13: the stall result stands.** Refusing immediately cost no steps
at all; stalling cost 24 of 360 and 31x the median latency. Stalling is strictly worse than refusing.

**But §4.13's denial is conditional and the section does not say so.** It measured total denial with
**one** honest peer, which had no alternative. At 15 the same attack denies **6.7 %** of steps,
because a stalled peer recovers inside a single `step()`. Severity tracks the victim's redundancy,
not the attack.

**Refusing is not free to the victim either.** `reject` loses no steps but its p90 is 5.25 s against
the baseline's 0.16 s: a peer that picks the attacker burns about five seconds finding another leader.

**Reconciled against live matchmaking, and the headline does not survive.** `step12_election_live.py`
runs the same strategies against real `DecentralizedAverager` swarms. At 15 honest peers, group size 4,
4 rounds, 3 repeats: the attacker **leads 4 of 4 rounds** in both the blind and the undercut arm, with
normal toy all-reduce service when selected, confirming the leadership claim live. But **followers per round is pinned at 3 in every
repeat and both arms**, which is `target_group_size - 1`: 43 % of honest peers at 7, **20 % at 15**.
The share halves as the swarm doubles.

**The 100 % was an artifact of a missing capacity constraint.** The simulated election asked,
independently for each peer, whether it would prefer the attacker, and every peer did. A live leader
seats `target_group_size - 1` followers and the rest form their own group. The *comparison* does not
decay with swarm size; the *outcome* does, so §4.15's scale-independence claim applies to the wrong
half. Blind and undercut are also indistinguishable live, both leading 4/4 and seating 3, where the
simulation separated them 15-20 % against 100 %.

**And it is not a denial attack.** Honest availability was 60/60 in both attack arms against 64/64
baseline. Leading every round costs the swarm nothing by itself; what it buys is
`target_group_size - 1` seats, which is where C13.S's admission control would have to do the rest.

The low `peer_id` cost is still reasoned rather than measured, and all DHT operations are loopback.

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

### 4.12 A forged withdrawal deletes a peer from discovery — but discovery is not membership

`step8_withdrawal.py`, real `DHTNode`s driving the real `GroupKeyManager`. No training — this is a
DHT-layer result, so a run takes seconds.

**What a withdrawal is.** The group key is a DHT dictionary: subkey = a peer's PeerID, value = `True`
("looking for a group") or `False` ("no longer looking"), each with an expiration.
`get_averagers(only_active=True)` skips `False` entries (`key_manager.py:87`), so a withdrawn peer is
invisible to anyone seeking partners. A subkey holds one value and `TimedStorage.store` overwrites only
if the new expiration is **strictly later** (`timed_storage.py:81-82`), so Hivemind's own withdrawal uses
`np.nextafter(expiration, +inf)` (`key_manager.py:61`) — later by ~1e-16 s, enough to win the comparison
without extending the record's life.

**The attack is to pick a different number.** Nothing checks who writes under a subkey: it is an
unverified `peer_id.to_bytes()`, and there is no `record_validator` anywhere in `hivemind/averaging/`.
Write `False` under an honest peer's subkey with a *far-future* expiration.

| check | result |
|---|---|
| control: 12 peers declare, all visible to a bystander | 12/12 |
| **1. forged write under another peer's subkey accepted?** | **yes** |
| **2. victim hidden from a *third* peer?** | **yes**, other 11 unaffected |
| **3. victim's own re-declaration?** | **rejected** — `declare_averager` returns `False`, stays invisible |
| 4. expiration accepted up to | **+10 years**; no upper bound found |

Check 4 finds a *floor*, not a ceiling: a forgery below the victim's current expiry is rejected, which is
`timed_storage.py:81-82` working as documented. Above it, nothing caps the value — one write suppresses a
peer for as long as the attacker cares to name.

**Check 3 is what makes this a suppression rather than a per-round race.** The victim's honest
re-declaration carries a near-term expiration and loses the comparison, so it cannot advertise itself back
into existence. Measured directly: 30 re-declarations over 30 seconds, **visible on 0 of them**.

**It scales cleanly and needs no custody** (2 repeats each, at 12 and at 40 honest peers):

| swarm | hidden / attempted | survivors still visible | writes accepted |
|---|---|---|---|
| 12 peers | 1/1, 3/3, 6/6, 11/11 | all | all |
| 40 peers | 1/1, 20/20, 39/39 | all | all |

Perfectly targeted — every intended peer disappears, every other peer remains. And the attacker's
`node_id` makes **no difference**: random and crafted behave identically. That is the headline contrast
with §4.6, where read-filtering needs `bucket_size` = 20 crafted identities *and* the custody they buy.

| | read-filtering (§4.5–4.6) | forged withdrawal |
|---|---|---|
| Mechanism | remove entries from a response | **add a record** |
| Versus merge semantics | fights them | **exploits them** |
| Crafted identities needed | **~20** (`bucket_size`) | **0** |
| Custody of the key's replicas | required | **not required** |
| Cost per hidden peer | — | **one DHT write** |
| Victim can undo it | n/a | **no** |

**Detection.** The suppressed peer's own `declare_averager` returns `False` every time — the DHT tells it
exactly what is happening. Nothing in Hivemind reads that value (`matchmaking.py:535,544`; no other
caller), which is §4.2's unused signal showing up again as the one clean way to notice this.

**It does not convert into group capture, and that is measured.** `step6_isolation.py --arm withdraw`
runs the §4.3 malicious leader on a training swarm with a forger suppressing every honest peer's
advertisement, and `--no-forge` is the same arm with the forger switched off — so the difference between
them is the forgery's own contribution, and nothing else.

First attempt, 8 peers / group 4 / **3 attackers**, was useless: both arms captured ~100 % (17/17,
19/19, 21/21 without the forger). At α = 0.375 the malicious leader alone is already **saturated**, so
there was no headroom for the forgery to show in. A ceiling is not a result.

Repeated at **1 attacker**, where §4.3 leaves room, 3 repeats each:

| arm | capture per run | mean | honest peer in victim's group |
|---|---|---|---|
| leader only (`--no-forge`) | 69 %, 31 %, 75 % | 58 % | 31 %, 69 %, 25 % |
| leader + forged withdrawals | 44 %, 88 %, 56 % | 62 % | 56 %, 12 %, 44 % |

**The spread inside each arm is far larger than the gap between them.** At this sample size the forgery
has no detectable effect on capture. Distinguishing a difference this small against this variance would
need roughly 20 runs per arm, not 3.

**And the mechanism says not to expect one.** Forging a withdrawal hides a peer's *advertisement*, so
the victim cannot **find** it. It does not stop that peer **reading** the group key, seeing the victim,
and asking to join its group. Group membership is settled at the admission layer — who the leader lets
in (`matchmaking.py:341-368`) — not at the discovery layer. Suppressing a peer from discovery does not
keep it out of a group it can still see, and excluding it still takes the admission control §4.3 already
provides.

**So what this primitive actually is: a censorship and availability attack, not a capture attack.** A
suppressed peer cannot be found by anyone, so nobody recruits it — it is excluded from training
altogether, cannot re-advertise itself, and the DHT tells it so in a return value nothing reads. That is
real harm, and unusually cheap and durable harm, but it is *availability*, not control over whose
gradients mix with whose. Do not report it as the latter.

---

### 4.13 A leader can accept a follower and stall it — **controlled in E01**

Prompted by the supervisor's framing: at each step, ask whether an attacker can **Deny, Delay or
Tamper**. Every finding above is *capture*; this is the first **Deny** result, where the attacker gains
nothing and the victim simply loses the round.

`step9_stall.py`, 1 attacker + 1 honest averager, driving `DecentralizedAverager` directly (no
Optimizer, no model — deliberately the smallest harness that answers the question). The attacker wins
leadership by declaring the earlier deadline, then forwards `ACCEPTED` and withholds everything after
it.

| arm | result |
|---|---|
| `--no-attack` (honest leader) | averaged in **0.03 s** |
| stall | 5 full stalls + 1 truncated; **120 s step consumed, never averaged** |

**The timeout law holds exactly.** Measured 22.97–23.00 s per stall against a predicted 23.0 s. The
prediction is `matchmaking.py:216`: `time_to_expiration + request_timeout`, where `time_to_expiration`
derives from `get_request_expiration_time()` — the **follower's own** declared expiration. So the
victim's patience is set by the victim (`min_matchmaking_time` 20 + `request_timeout` 3). The attacker
cannot lengthen it, and does not need to.

**Why the victim does not learn.** There *is* an exclusion list, but `past_attempts` is keyed on
`(peer_id, expiration_time)` pairs (`:497`, filtered at `:510`) — an *advertisement*, not a *peer*.
Every averager re-declares with a fresh expiration each `min_matchmaking_time` (`:529-536`), minting a
pair that is not in the set. The attacker chose its own `min_matchmaking_time` (6 s against a 23 s
stall), so it refreshes three or four times per stall and is never excluded for more than one cycle.
Laundering needs no special behaviour — re-declaring is ordinary; the attacker only sets the rate.

**Same structural pattern as the `node_id` finding: the security decision keys on a field the attacker
chooses.** Worth carrying into the thesis framework as a second instance on a different axis.

**Nothing penalises delay.** Two ban mechanisms exist and neither can see this:

| mechanism | catches | why it misses |
|---|---|---|
| `allreduce.py:115` `banned_senders` | peers not sending chunks in time | scoped to one `AllReduceRunner`; matchmaking never reached allreduce |
| `dht/node.py:182` `Blacklist(blacklist_time=5.0, backoff_rate)` | **non-responsive** peers, exponential backoff | the attacker answers `rpc_find` and replied `ACCEPTED` — it is never silent |

Both key on *silence*; this attacker is talkative and useless. Hivemind already has the concept of a
backoff blacklist, just nowhere it would fire here.

**The `reject` control is complete.** E01 (§4.16) found no lost steps under immediate refusal, versus
24/360 under stalling at 15 honest peers, so the stall is worse than a failed join. The two-peer result
above is a worst case with no alternative partner; do not generalise its total denial to larger swarms.
Still unmeasured: whether `pause_search` (`:214`) silences the victim's own advertisement during the
wait, and harm across a real training run.

**Cheapest candidate defence**, if the project turns to defences: make `past_attempts` peer-keyed with
backoff instead of pair-keyed. Small, and testable against this exact harness — which would give the
defence datapoint `FINDINGS.md` §5 currently lists as missing.

---

### 4.14 The identity flaws, written up as standalone reproductions

Four short GitHub-issue-format reports with matching scripts (listed in §2). They formalise what §4.1
already assumes rather than adding attacks — except the third, which is a negative result.

- **`node_id` is caller-supplied** (`node.py:171`) and never compared against the authenticated
  `context.remote_id`, which appears exactly once in the whole `hivemind/dht/` tree (`protocol.py:151`)
  and is never read again. `DHTID.generate()` is SHA1 of random bits; the keypair is never an input.
- **The `(node_id, peer_id)` pairing is transmitted, not derived** — two parallel arrays in every
  lookup answer (`protocol.py:366-367`), zipped back together by the receiver (`:301-306`) with no
  check. The routing table is literally two dicts, a stored association rather than a computation.
  This is the IPFS ID-mapping question answered: there is no such table, the mapping rides the wire.
- **Forging those pairs is mostly not exploitable** — *negative result, keep it.* One attacker serving
  4 forged pairs at distance 0 took **4 of 5** returned lookup slots, but the record still stored on
  **5/5 real replicas** and read back fine, because `store` skips candidates that do not answer
  (`node.py:412-431`). The asymmetry is the point: inventing peers that are not there gains little,
  owning the peers that genuinely are nearest takes the record outright.
- **The group-key hash protects nothing.** The objection "K is a hash, how can the attacker sit next to
  it?" dissolves because K is a *forward* hash of a public string — `f"{prefix}.0b{group_bits}"`
  (`key_manager.py:42-44`), the run name every peer shares. The hash constrains where the *record*
  goes, not where the *attacker* goes, since `node_id` is a free parameter rather than a hash output
  the attacker must produce. Measured: 5 crafted nodes at K, K^1…K^4 took **5/5 replicas, 0/6 honest**.
  Contrast with S/Kademlia puzzles, where the ID *is* constrained to be a hash output and proximity
  genuinely costs work.

---

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
- **Filenames must encode every variable that changes the result** — and this has now bitten twice.
  The second time, `--sybil-nodes` was missing from `step6_isolation.py`'s output name, so runs at 5,
  25 and 40 crafted identities all wrote the same file and silently overwrote each other. The summaries
  survived only because they were also in the run logs. When adding a knob, add it to the filename in
  the same edit. **This has now happened four times.** Twice more in `step10_delay.py`: a stale-sweep
  name keyed only on `--reads`, which that phase does not even use, and an election name that omitted
  the arm set, so a rerun with different arms silently replaced the previous one. Both names now carry
  every variable that changes the result. The rule is not "name the important knobs", it is **name
  every knob the run varies**, including ones that feel like presentation.

- **A control must match on everything except the behaviour under test, including population.**
  E01's first arms compared a four-peer honest baseline against five-peer attack arms, because the
  attacker was simply added. At group size 2 an odd number of declared peers always orphans one, so
  the attacker's mere presence changed the outcome regardless of what it did. The baseline now puts an
  ordinary honest peer where the attacker goes. Ask what the arm changes *other* than behaviour.

- **An attacker that is never invoked produces a clean-looking null.** E01's first version constructed
  the attacker but never called `step()` on it, so it never entered matchmaking, never advertised and
  sat inert while honest peers paired among themselves. Every arm looked identical and the run would
  have read as "abstention has no effect". The guard is to assert the attacker actually acted:
  E01 checks that the abstaining peer emitted abstentions and received join requests.

- **Do not identify a participant by its behaviour.** The same run inferred which peer was the attacker
  from "whoever abstained", which made its join count zero by construction in the arms where it never
  needed to abstain. Record identities explicitly.

- **A control that finishes early is not a control.** `step10_delay.py` phase 3 first gave both arms
  the same *number of reads* rather than the same *wall-clock window*. The fast arm completed all its
  reads before the advertisement under test was even written, so both arms observed nothing and the
  result looked like a null. Matching arms means matching the thing the attacker changes: if the
  attack changes how long an operation takes, equalise time, not iterations.

**Resource envelope.** Each peer is **4 Python processes plus one `p2pd`** (trainer, DHT, gradient
averager, state averager), because `DecentralizedAverager` extends `mp.Process`. ~350–520 MB per peer.
16 peers ≈ 5 GB; 24 fits on a 32 GB box; 32 untried. Measure rather than assume.

---

## 6. Next steps

**All four brief items are measured** (§4.1-4.11). What follows is work that would strengthen the
result, in priority order.

### Step 8 — the forged withdrawal: **measured, and narrower than hoped (§4.12)**

Built as `step8_withdrawal.py` and `step6_isolation.py --arm withdraw`. At the discovery layer it does
everything the design predicted: the forged write is accepted, the victim vanishes from third parties'
lists, its own re-declaration is rejected, +10 year expirations are accepted, it scales to hiding 39 of
40 peers with no collateral, and the attacker's `node_id` is irrelevant — no custody, no crafted
identities, one write per peer.

**But it does not convert into group capture.** Against a `--no-forge` control that keeps the malicious
leader and drops only the forgery, capture was 58 % without it and 62 % with it, against within-arm
spreads of 31–75 % and 44–88 %. No detectable effect, and §4.12 gives the mechanism: discovery and
membership are different layers, and a peer hidden from discovery can still read the key, see the
victim, and ask to join.

**It is therefore a censorship/availability primitive**, and should be written up as one. That is still
worth having — it is cheap, durable, targeted, and the victim cannot undo it — but it is not a cheaper
route to β, and §4.3's admission control remains the thing that actually decides membership.

**If anyone wants to settle the capture question properly**, it needs ~20 runs per arm at 1 attacker,
not 3. The variance at this swarm size is what §5 warns about, and it is larger than any effect the
forgery could plausibly have.

### Where this sits in the literature (searched 2026-09; `QA_NOTES.md` Q15 predates this)

Q15 established that nobody has analysed Hivemind's DHT security, and that both Hivemind papers state
the uniform-`node_id` assumption and defer the analysis. That still holds. **What Q15 does not cover is
the group-formation thesis**, and there the space is much busier than it looks:

- **GRANITE** (arXiv:2504.17471) is the closest work and says it in the abstract: *"Byzantine nodes can
  poison models and manipulate peer sampling to amplify their influence."* Its defence adjusts
  aggregation thresholds on *"estimated Byzantine density in the neighbourhood of each node"*, giving
  exponential decay. That is α → β amplification and its decay, already with a defence.
- **Byzantine-tolerant peer sampling is an established subfield** — Honeybee (arXiv:2402.16201), Basalt,
  RAPTEE.
- **Pasquini, Raynal & Troncoso** (IEEE S&P 2023, arXiv:2205.08443) already argue decentralized learning
  *"does not offer any security advantage over federated learning"*. The **same EPFL group** wrote the
  sandtrap paper (arXiv:2303.03829) — they own this space, so check their recent output first.
- **Near miss worth knowing precisely:** sandtrap isolates a victim, but by *crafted values on a fixed
  graph* — the paper says the effect is *"similar to an attack in which the connections of the victim
  to the network are removed"*. Similar to. It does not remove them. This project's attacks act on
  membership, which is a different mechanism.

**So do not frame this as "robust aggregation's threat model is wrong".** That is taken. What appears
untouched is **election-and-admission group assembly** — every competitor above is random peer sampling
over a graph, where Hivemind elects a leader by self-declared deadline and lets it admit by identity.
That is a different protocol shape with a different attack surface, and it is where §4.3, §4.6 and §4.4
live. **Verify that gap before any novelty claim rests on it**; four searches is not a literature review.

The framing model to copy is Sridhar et al., *Content Censorship in IPFS* (NDSS 2024): a known attack
class, one deployed system, rigorous measurement, mitigation. Venue shape is NDSS / USENIX / EuroS&P,
not an ML theory venue.

PDFs of all of the above were downloaded to `~/Downloads/hivemind-security-papers/` on the second
machine — **not in the repo**, so re-fetch them if that machine is gone.

### From the audit, in the order the map gives

`CONTROL_DATA_AUDIT.md` Part 3 is the canonical queue; do not maintain a second copy here. It now
contains four attack tracks requested for the presentation: adaptive training-conditional leadership,
malicious-leader roster/index equivocation, routing-table flooding around the group key, and simulated
failure of the five closest peers. Track 1 now has a routed local measured-delay follow-up but remains
unproven on a public WAN. Track 2 is confirmed end to end after its smoke gate. Track 3 confirmed routing
admission but no standalone disruption; track 4 is next. Every track advances only after its smallest
test and controls have been inspected.

### Loose ends, in descending order of interest

- **Walk the whole protocol as Deny / Delay / Tamper / Select.** The supervisor's frame, plus a fourth
  axis. Deny/Delay/Tamper is the standard Byzantine message-handling decomposition and exhausts what a
  participant can do to a message it *handles* — but it cannot express control over *who is in the
  group*, which is where β = k/(k+1) lives and where the literature is thinnest. §4.13 is the only Deny
  cell filled. Inconsistent `ordered_peer_ids` is now C14.T's demonstrated Tamper result; wrong
  `group_id` and destination-conditioned all-reduce streams remain open.

- **Compose tracking with admission control.** §4.4 scaled shows tracking puts an attacker in the
  victim's *bucket* exactly and indefinitely (63/63 predictions), but converting that to the victim's
  *group* falls to 38 % once a bucket holds three groups. §4.3's admission control converts group
  presence into capture exactly. The two have never been run together, and that composition is the
  obvious strongest attack this project has not measured.

- **Does the §4.6 law hold at a third `bucket_size`?** Measured at 20 (step at 20) and 40 (25 fail, 45
  work). A run at `--bucket-size 8` with 5 and 10 Sybils would give a third point on the line at almost
  no cost, and would also check the low end where `num_replicas = 5` and `bucket_size` are close.

- **Interception latency.** §4.1 measures Sybil build time and settled capture, never the interval. A
  Sybil intercepts nothing until it is in the routing tables of peers routing toward the key. Have the
  victim poll on a fixed interval, introduce Sybils at t=0, record capture against wall-clock. This is
  what a defender would need to catch the attack in flight.
- **Settle §4.8** by fixing peer count and varying timing noise deliberately (pin cores, inject
  latency), to see whether clique structure follows the noise rather than the peer count.
- **Grind a low `peer_id` only for the equal-deadline variant.** The tie-break is the lowest `peer_id`
  (`matchmaking.py:478-481`) and `identity_path` makes PeerIDs choosable, so they can be ground offline.
  The one-float-step undercut wins the comparison without this advantage (C12.S).
- **Censorship as opposed to custody.** §4.1's attacker nodes serve honestly. Refusing to serve on
  `rpc_find` is a separate experiment; §4.2 does the store-side half.
- **A free detection signal nobody uses** (§4.2), if the project turns to defences.

### Things that did not work — do not retry without reading why

- **Large deadline undercuts.** Expiration is simultaneously the record's **lifetime** and its
  **leadership priority** (`key_manager.py:60`, read back at `:88`). Lowering it by seconds can make the
  advertisement expire; C12.S showed that undercutting by one float step avoids this failure.
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

**Turn on the hooks. This is per clone and git will not do it for you:**

```bash
git config core.hooksPath research/hooks
```

`research/hooks/pre-commit` regenerates `control_data_map.pdf` whenever
`CONTROL_DATA_AUDIT.md` is staged, and `hivemind_dht_timings.pdf` whenever one of the timing CSVs is.
Both generators parse their sources so they cannot disagree with them about content, but they only run
when invoked -- which is how the map sheet spent a commit showing three stale cells. The hook stages
the PDF only when the rendered text actually changes, so a reflow does not create noise commits. Skip
it for one commit with `git commit --no-verify`.

Three steps are not obvious and all cost time to rediscover:

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
