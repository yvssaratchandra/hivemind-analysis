# Hivemind control-data security review

**Status:** working document — the map, plus the evidence gathered so far.
**Scope:** Hivemind 1.2.0.dev0, branch `research/net-security`. Training path only.
**Purpose:** guide the technical research. Not a thesis proposal.

## Question

What control data do Hivemind peers exchange from startup to shutdown, and where can an attacker:

- **delay** it;
- **deny** it, by dropping, withholding, or rejecting it;
- **tamper** with it, by forging, changing, replaying, or sending conflicting versions;
- **select** which peers, records, or messages are allowed to affect the victim?

**Observe/predict comes first.** It is a prerequisite, not a fifth action. A selective attack usually
needs the attacker to recognise the victim, predict its next key, or see its request. If it cannot
observe, the other four often do not arise.

## Scope: what counts as the control plane here

**The control plane is defined by function, not by layer.** Anything that carries a decision — who is
trusted, who is involved, what happens next — is control plane, wherever in the stack it travels. A
layer is "data plane" only in respect of the payload it moves.

So **all 21 checkpoints below are control plane.** `FINDINGS.md` labels L4 and L5 as data plane, which
is true of the tensors they move and misleading about everything else they carry. Role assignment,
session recovery and donor selection are decisions driven by peer-supplied data, and they belong here
even though the layers around them move tensors.

The clearest case is C17.T, the largest number in the project. All-reduce moves tensors, but the
35–148× displacement was **not** caused by tensor values — a sign-flipped gradient alone was lost in
the average. The damage came from the sender-supplied `weight`, applied exactly as received. The
control field did the work.

### In scope

- An attacker must not be able to decide **which group it sits in**, or **who else is in that group**.
- An attacker must not be able to **manipulate control messages** — forge them, suppress them, delay
  them, or send different versions to different peers — to gain an advantage.

### Out of scope

- **Making aggregation Byzantine-tolerant.** Robust aggregation rules are a separate research area.
  This project measures what control-plane capture *costs* the model, but fixing the aggregation is
  not the goal. C17.T is in scope because the unvalidated `weight` is a control field; crafting better
  gradients is not.
- **Payload contents.** For checkpoints 17 and 20 the control fields are in scope and the tensors
  themselves are only the yardstick for harm.

## How this document is organised

Two views, answering two different questions.

- **Layer** — *where a fix would go.* Binding `node_id` to the PeerID fixes seven checkpoints at once.
  Signing advertisements fixes another five. This is the defence roadmap.
- **Control checkpoint** — *what the attacker does.* Either a message exchange, or a **local decision**
  whose inputs came from other peers. Local decisions send no packet but stay in scope, because
  peer-supplied data drives them; choosing a leader is the clearest case.

Checkpoints are **not** a linear lifecycle. Bootstrap happens once. Routing, record refresh, progress
tracking and donor advertising run continuously. Matchmaking and averaging repeat. State download is a
conditional recovery branch.

**The empty cells matter as much as the full ones.** Empty means nobody has looked, not that it is safe.

**A local decision has no independent attack surface.** Checkpoint 12 makes its choice inside a peer's
own process, over checkpoint-11 records, and sends nothing. An attacker cannot delay, deny or tamper
with a computation running in someone else's process; it can only act on the inputs, which is what
C11.Delay, C11.D and C11.T already record. Observe is the same: the rule is deterministic over inputs
C11.Obs showed are public, so predicting another peer's choice is reading checkpoint 11. Those four
cells therefore read `= 11` rather than `open`, in the same sense row 14 reads `= 13`.

**The grid is not the whole scope.** Absent checks do belong in it, sited at the decision they should
protect, which is what C3.T is. But strategies that chain checkpoints, and questions about what an
attacker costs, span rows; those live in the experiment register, Part 1b, and are cross-referenced
from the cells they touch.

## A cell is a family of experiments, not one

Within any one cell, distinguish:

|  | **by omission** (stay silent) | **by commission** (answer, but uselessly or falsely) |
|---|---|---|
| **direct** (attacker acts; needs position) | refuse to serve a lookup | leader accepts, then stalls |
| **induced** (honest peers act; needs no position) | — | forged withdrawal: honest peers filter the victim |

Also record, per result: **targeted or blanket** (one victim, or everyone), and **persistence** (does the
effect end when the attacker stops?).

**Why this matters more than it looks.** Every defence in Hivemind detects *omission*. `banned_senders`
fires on peers that miss a chunk deadline. The DHT `Blacklist` fires on peers that stop responding.
`request_timeout`, `next_chunk_timeout`, `sender_timeout`, `reducer_timeout` — all of them watch for
silence.

This does **not** mean Hivemind accepts every active message. Record validators and protocol checks
reject some malformed, stale or incompatible input — `BAD_SCHEMA_HASH`, `BAD_EXPIRATION_TIME`,
`BAD_GROUP_ID`, `BAD_GROUP_KEY`, `DUPLICATE_PEER_ID`, `PROTOCOL_VIOLATION`. The narrower gap is that
none of these detects a **protocol-valid lie**: a false but well-formed routing identity, a selective
lookup answer, a forged withdrawal, an accepted-then-stalled join, or a dishonest averaging weight.

That is why the stall worked: the attacker answered every DHT query and actively replied `ACCEPTED`, so
it was never silent and never visible. Stated generally, **Hivemind's fault-tolerance model is
dominant fault-handling path is crash/omission-oriented; protocol-valid Byzantine behaviour is only
partly checked.** C11.Delay is the exception that proves it: past `wait_timeout` the attacker's
commission becomes omission, and the `Blacklist` then fires.

And it shows in the results below: **every demonstrated attack in this project is by commission. Not
one is by omission.** That is a gap in what has been tried as much as a property of the system.

## Things that change how the map is read

**Layers 3–6 run twice.** `{run_id}_grad_averager` and `{run_id}_state_averager` are two separate
averagers, each with its own group key, matchmaking and leader election. Everything measured so far
attacked the gradient averager. The state averager runs the same code over optimizer state.

**Checkpoints 13 and 14 are one channel.** `group_id` and `ordered_peer_ids` arrive in the
`BEGIN_ALLREDUCE` message on the same `rpc_join_group` stream. Whoever controls 13 controls 14 for free.

**Attacker position matters.** An ordinary peer, a record custodian, a lookup responder, a group leader,
a group member and a state donor do not have the same powers. libp2p authenticates the transport and
the PeerID, so an outsider cannot alter someone else's stream — but an authenticated peer can lie
inside a valid message, and nothing checks the contents.

**The actions compose.** Routing control enables selective lookup results, which control leader choice,
which controls admission, which exposes the victim to a stall or a malicious reduction.

---

## Part 1 — the map

`D` = demonstrated in a live swarm. `X` = prevented, with reason. `open` = nobody has looked.
`—` = does not apply.

### L1 — libp2p transport · *control plane*

`hivemind/p2p/`. Authenticated connections. `FINDINGS.md` calls this layer sound, which is true of the
transport crypto — but **bootstrap peer selection also lives here and is unexamined.** Which peers you
first learn the network from is a real surface that "authenticated connections" does not cover.

| # | Checkpoint | Obs | Delay | Deny | Tamper | Select |
|---:|---|---|---|---|---|---|
| 1 | Bootstrap and transport discovery | open | open | open | open | open |

### L2 — Kademlia DHT · *control plane*

`hivemind/dht/`. Who stores which record, who answers a lookup. **A single fix — deriving `node_id`
from the authenticated key — would change every row here.**

| # | Checkpoint | Obs | Delay | Deny | Tamper | Select |
|---:|---|---|---|---|---|---|
| 2 | DHT reachability and time check | open | open | open | open | open |
| 3 | Routing admission, liveness, eviction | open | open | open | **D — C3.T** | **D — C3.S** |
| 4 | Lookup referrals | open | open | open | **D — C7.T** | open |
| 5 | Record responsibility transfer | open | open | open | open | open |
| 6 | Record publication and replication | open | open | open | open | **D — C6.S** |
| 7 | Record lookup and local-view construction | open | open | open | **D — C7.T** | **D — C7.S** |
| 8 | Record refresh, withdrawal, expiration | open | open | open | **D — C8.T** | open |

### L3 — Matchmaking · *control plane*

`hivemind/averaging/matchmaking.py`, `key_manager.py`. Advertise, elect a leader, admit members.
**A single fix — signing advertisement records — would change rows 11 and 15.**

| # | Checkpoint | Obs | Delay | Deny | Tamper | Select |
|---:|---|---|---|---|---|---|
| 11 | Averaging availability exchange *(also E01)* | **D — C11.Obs** | **D — C11.Delay** | **D — C11.D** | **partly X — C11.T** | **D — C7.S** |
| 12 | Candidate priority and leader choice *(local, also E01)* | = 11 | = 11 | = 11 | = 11 | **D — C12.S** |
| 13 | Join negotiation and admission *(also E01)* | open | open | **D — C13.D** | open | **D — C13.S** |
| 14 | Group roster distribution | open | open | = 13 | **D — C14.T** | = 13 |
| 15 | Next-group-key derivation *(local)* | open | open | open | open | **D — C15.S** |

### L4 — All-reduce · *control data: roles (16), recovery (18), the weight and group id in 17*

`hivemind/averaging/allreduce.py`, `partition.py`. Reduce-scatter over the assembled group.

| # | Checkpoint | Obs | Delay | Deny | Tamper | Select |
|---:|---|---|---|---|---|---|
| 16 | Role and work assignment *(local)* | open | open | open | = 14 | **D — C14.T** |
| 17 | All-reduce exchange *(payload too)* | open | open | open | **D — C17.T** | open |
| 18 | Session failure and recovery | open | open | open | open | open |

The leader supplies the ordered roster that assigns reducers, and follower-side assembly only checks
self-inclusion and aligned list lengths. C14.T demonstrates the cross-layer consequence: personalized
roster permutations make several peers derive the same local reducer index and silently mix unrelated
tensor coordinates. Select remains open at 17: a group member also holds a separate stream per reducer
and could condition its behaviour on the destination or shard, but that action is still untested.

Checkpoint 11 Select reuses C7.S: that experiment controlled which averaging advertisements reached
the victim. It is the L3 consequence of a demonstrated L2 lookup attack, not a new experiment.

### L5 — Optimizer · *control data: the progress records that drive 9 and 10*

`hivemind/optim/optimizer.py`, `progress_tracker.py`. Accumulate, average, apply.
**Cross-layer note:** progress records ride the DHT but are **signed** (`progress_tracker.py:108`),
while L3's advertisements are not. Same machinery, applied in one place and not the other.

| # | Checkpoint | Obs | Delay | Deny | Tamper | Select |
|---:|---|---|---|---|---|---|
| 9 | Training-progress exchange | open | open | open | open | open |
| 10 | Progress and synchronisation decision *(local)* | open | open | open | open | open |

### L6 — State transfer · *control data: donor priority and selection (19), state metadata (20)*

`hivemind/averaging/averager.py` + `hivemind/optim/state_averager.py`. **Not one of the five layers in
`FINDINGS.md`** — it is neither all-reduce nor matchmaking nor the plain optimizer step, and it needs
naming separately. Donor priority is a number each peer declares about itself; downloaded tensors are
installed with no validation. Every other finding *nudges* a model; this one would **replace** it.

| # | Checkpoint | Obs | Delay | Deny | Tamper | Select |
|---:|---|---|---|---|---|---|
| 19 | State-donor advertisement and selection | open | open | open | open | open |
| 20 | State download and installation *(payload too)* | open | open | open | open | open |

### Spanning L2 / L3 / L5

| # | Checkpoint | Obs | Delay | Deny | Tamper | Select |
|---:|---|---|---|---|---|---|
| 21 | Shutdown and withdrawal | open | open | **D — C21.D** | open | — |

### Coverage

| layer | checkpoints | cells with evidence |
|---|---|---|
| L1 libp2p | 1 | **0** |
| L2 DHT | 2–8 | 6 |
| L3 Matchmaking | 11–15 | 9 |
| L4 All-reduce | 16–18 | 2 |
| L5 Optimizer | 9–10 | **0** |
| L6 State transfer | 19–20 | **0** |
| spanning | 21 | 1 |

**18 of roughly 90 cells.** The work sits almost entirely in L2 and L3 — correct for the research
question, since those are the two control-plane layers. The **Observe** and **Delay** columns are empty
**everywhere except checkpoint 11**, including under findings already treated as settled, where the
attacker's ability to see the victim was assumed rather than measured. C11.Obs is the first direct
measurement of that assumption at the availability exchange, and C11.Delay is the project's first
Delay result of any kind.

---

### Runtime trace — messages mapped to checkpoints

#### Question

Does the 21-checkpoint map match what honest nodes actually send, and which checkpoints have no
separate network message?

#### Work performed

`trace_control_messages.py` ran four honest peers for 18 seconds. It captured messages at the caller's
P2P boundary, exercised both averagers, and explicitly triggered state download. It kept public DHT
operations separate from wire RPCs because the plain key is hashed before transmission.

The full trace is in `results/trace_lifecycle.jsonl`; concise inventories are in
`results/trace_lifecycle_inventory.md` and `results/trace_lifecycle_inventory.json`. Every one of the
18,274 wire frames has its exact protobuf bytes, decoded control fields, sender, receiver, correlation
ID, sequence number, description and outcome. All stored protobufs were decoded again and their lengths
and SHA-256 hashes were checked.

#### Observations

The trace found **9,127 RPC calls across all six training RPC families**:

| RPC | Calls | What it carried | Main checkpoints |
|---|---:|---|---|
| `rpc_ping` | 8 | node identity, time and reachability result | 2, 3 |
| `rpc_store` | 841 | hashed key, value, expiration, cache flag and acceptance | 3, 5, 6, 8; transports 9, 11, 19, 21 |
| `rpc_find` | 8,238 | hashed key, stored value and nearest-peer referrals | 3, 4, 7; transports 9, 11, 19 |
| `rpc_join_group` | 20 | join request, admission, redirect/error or final roster | 13, 14, 18; supplies 15 and 16 |
| `rpc_aggregate_part` | 18 | group, peer, weight, tensor shard and result/error code | 17, 18 |
| `rpc_download_state` | 2 | state request, metadata and tensor chunks | 20 |

The earlier result of 39,454 `rpc_ping` messages was wrong. A received `rpc_find` or `rpc_store` calls
`rpc_ping` as a **local helper** to update routing state. Handler-side tracing counted those function
calls as network pings. Caller-side tracing found eight real ping requests and eight responses: four
requested reachability validation and four did not.

Logical DHT activity was also classified before key hashing: 1,463 advertisement reads, 27 writes and
24 withdrawals; 28 progress reads, 91 writes and 4 withdrawals; and 2 donor-list reads and 64 donor
writes. These are logical operations, not extra wire messages. One logical operation may cause many
iterative `rpc_find` calls, and one `rpc_store` may carry several records.

| # | Trace evidence | What the trace establishes |
|---:|---|---|
| 1 | Four `bootstrap_start` / `bootstrap_complete` pairs | Node startup occurred, but Identify, TLS, NAT and relay traffic is inside p2pd and was not captured. |
| 2 | 8 `PingRequest` + 8 `PingResponse` | Reachability, DHT time and availability are real wire fields. |
| 3 | Ping plus the `peer.node_id` fields in DHT RPCs | Peers exchange routing identities; routing-table admission and eviction are local consequences. |
| 4 | `FindResponse.nearest_node_ids` + `nearest_peer_ids` | Lookup responders send the node-ID/PeerID referrals used by the caller. |
| 5 | `rpc_store`; 840 ordinary and 1 cache record | Responsibility transfer uses the store protocol, but cannot be distinguished from other replication from the protobuf alone. |
| 6 | 841 store request/response pairs; 795 accepted and 46 rejected records | Publication and replica acceptance cross the wire explicitly. |
| 7 | 8,238 find request/response pairs; 8,085 found dictionaries and 153 misses | Iterative lookups construct the caller's view from values and referrals. |
| 8 | Expiration in every store; advertisement/progress writes and withdrawals | Refresh and withdrawal send stores. Natural expiration is local and sends no message. |
| 9 | 91 progress writes, 28 reads, 4 withdrawals | Progress exchange is carried by DHT store/find, not a separate optimizer RPC. |
| 10 | No distinct message | Synchronisation is a local decision over progress records already received at checkpoint 9. |
| 11 | 27 advertisement writes, 1,463 reads, 24 withdrawals | Averaging availability is carried by DHT records, not a matchmaking RPC. |
| 12 | No distinct message | Candidate ranking and leader choice are local decisions over checkpoint-11 records. |
| 13 | 20 `JoinRequest`; 20 `ACCEPTED`; 2 `GROUP_DISBANDED` | Join admission and refusal/recovery codes share the `rpc_join_group` stream. |
| 14 | 18 `BEGIN_ALLREDUCE` frames | The leader sends `group_id`, ordered peer IDs and gathered metadata on the join stream. |
| 15 | No distinct message | The next group key is derived locally from the roster and group ID received at checkpoint 14. |
| 16 | No distinct message | Reduction roles and shard ownership are derived locally from the ordered roster. |
| 17 | 18 `PART_FOR_AVERAGING` + 18 `AVERAGED_PART` frames | Each all-reduce frame carries `group_id`, `peer_id`, sender weight and tensor data. |
| 18 | `GROUP_DISBANDED` observed; all error/cancellation codes retained in the static inventory | Session control shares the join and aggregation streams; a healthy run does not exercise every failure code. |
| 19 | 64 donor-priority writes and 2 donor-list reads | Donor discovery and priority are DHT records; donor selection is local. |
| 20 | 2 download requests + 2 data frames, one per averager | State metadata and tensor chunks cross `rpc_download_state`; installation is local. |
| 21 | Progress withdrawals from all 4 peers; advertisement withdrawals also observed | Shutdown/withdrawal reuses DHT store. Donor records normally disappear by expiration rather than a separate shutdown message. |

#### Result

The runtime trace supports the 21-checkpoint map, but it shows that the checkpoints are **not 21
message types**. There are six application RPC families. Several checkpoints share one RPC; four are
wholly local decisions, and others combine a received message with a local decision. The trace also
confirms that the gradient and state averagers each run their own matchmaking, all-reduce and download
paths.

This is application-level coverage, not whole-stack packet coverage. The tracer records protobufs at
the Python caller boundary, not bytes sniffed from the transport. It does not record p2p framing,
`END_OF_STREAM` markers, exact `RPCError` frames, connection-close/reset packets, or internal
p2pd/libp2p discovery, handshake, NAT, relay and multiplexing traffic. The unrelated MoE expert RPCs
also remain outside the training-path scope.

Nor does one healthy 18-second run prove that every valid branch occurred. It observed `ACCEPTED`,
`BEGIN_ALLREDUCE`, `PART_FOR_AVERAGING`, `AVERAGED_PART` and `GROUP_DISBANDED`; the remaining defined
failure and cancellation codes are present in the static inventory but were not exercised. Different
client modes, churn, network paths and failure injections can produce additional instances of the same
protocol families.

#### Decision

**Use this mapping as the message inventory for the training path.** It validates where control data
crosses a peer boundary, but it does **not** fill the map's `Obs` security column: recording an honest
run is not evidence that an attacker can observe or predict a victim's traffic.

#### Next step

Use the correlation IDs and exact protobufs as the baseline for one controlled mutation at a time:
delay, deny, tamper or select. Add separate instrumentation for local decisions at checkpoints 3, 10,
12, 15, 16, 19 and 20. Capture P2P framing, exact error/end markers and connection closure for
checkpoint 18, and add p2pd-level tracing if checkpoint 1 becomes an experimental target. Repeat the
inventory under client mode, churn and injected failure before calling the dynamic trace exhaustive.

---

## Part 1b — experiment register

Some questions do not sit in one cell. An absent check belongs at the decision it should protect, so
absences *are* cell-shaped: C3.T is exactly that, the missing comparison between `node_id` and the
authenticated `context.remote_id`. But a strategy that chains several checkpoints, or that asks what a
whole attacker *costs*, spans rows. Those are registered here and cross-referenced from the cells they
touch, so a reader arriving from the grid or from Part 3 finds them either way.

| ID | Checkpoints | Question | Status |
|---|---|---|---|
| **E01** | 11 availability, 12 leader choice, 13 admission | Can one peer repeatedly deny averaging to others without contributing any training work, and what does each unit of attacker resource buy in victim disruption? | **done.** Abstention is free and invisible but harms nobody; all the harm is the stall. Carries the `reject` control for C13.D. |
| **E02** | 8 record renewal, 11 availability, 12 leader choice, 13 admission | Can one peer keep short own-ad deadlines renewed, refuse to follow, and repeatedly lead groups of four or five without reading competitors' ads? | **Tested locally, step22.** Sliding expirations persist beyond the initial lifetime. Leadership and write cost measured on eight peers. No indefinite or public-WAN guarantee. |
| **E03** | 8 record renewal, 12 repeated leader selection, 13 admission/retry | Does advancing an unreliable leader's own-ad expiration cause within-search retries and additional honest averaging delay or failure? | **In progress, step23.** Five paired local mechanism trials demonstrate retries; sixteen smoke trials pass. Confirmation halted after 15/160 trials when a reliable control returned an incorrect tensor alongside overlapping assembled group rosters. Cause unresolved; no harm result. |
| **E04** | 8 record renewal, 11 availability, 12 leader choice, 13 admission | Which own-ad remaining-lifetime range yields more assembled groups and X-led assemblies at fixed 1 Hz refresh with equal simulated training delays? | **Completed locally, step24 control plane.** 8/8 smoke and 40/40 confirmation trials; 2,485 distinct assemblies, 1,053 X-led. Five 1 Hz ranges produce 147-156 X-led each; no clear shortest-range advantage. Winner/X deadlines and follower views exported; tensor outcomes diagnostic. 36 checks pass. |
| **E05** | 8 record renewal, 11 availability, 12 leader choice, 13 admission | Does renewal improve X's per-attempt leadership, recruitment speed, and honest peers' actual selections; how often do searching peers read advertisements? | **Completed locally, step25.** 80/80 confirmation jobs pass; independent raw verification: 4,995 groups, 2,124 X-led. Passive stock leads 214/220 attempts; shorter-ad arms 99.0–99.7%, with all 1,665 interior non-stock attempts leading. Conditional first-eligible honest choices: 64.6% passive, 100% through `(2,3]`, 96.4% `(3,4]`. Successful assembly median 1.158s passive versus 0.276–0.327s shorter ads. Expiration-only 2s similarly strong; renewal-specific benefit/best range/sustained retry harm unestablished. Honest logical read medians 9.06–12.27ms, local instrumented calls. 88 focused checks pass; checkpoint preserves original 12, resumes only remaining 68 with frozen hashes. [Scalar table](results/step25_leadership_summary_20261009_a.csv), [read cadence](results/step25_read_cadence_summary_20261009_a.csv), [raw verification](results/step25_confirm_raw_verification_20261009_a.json). |
| **E06** | 8 record renewal, 11 availability, 12 leader choice, 13 admission | How do initial lifetime, refresh frequency, and remote control-message delay interact in X's per-attempt leadership and recruitment speed? | **Completed, step26 preliminary screen.** 14/14 jobs, one seed, 30-second windows, eight peers; 319 groups, 120 X-led, 129 X attempts. All U=1/3s × f=0.5/2/s × one-way control-delay=0/150ms combinations plus six passive/single-ad controls. Under delay, single-ad U=3s leads 6/6; U=1s at 0.5/s leads 0/3 and at 2/s leads 1/3. Fast U=3s renewal reduces eligible first choices from 29/29 to 21/30. Seven extra X selections across six honest searches demonstrate bounded renewed-pair retries (four after assembly); three joins reject a newly later X expiration. Original source/raw traces preserved; separate teardown/closure audit corrections pass; all 410 Step25 baseline hashes unchanged. No deployment-rate, optimal-policy or sustained-harm claim. [Report and full table](results/step26_pilot_20261010_a/REPORT.md), [raw checks](results/step26_pilot_20261010_a/raw_verification_corrected.json), [mechanism witnesses](results/step26_pilot_20261010_a/mechanism_evidence.json). |
| **E07** | 8 renewal, 12 leader choice/retry, 13 admission | Does renewal improve per-attempt leadership or add honest grouping delay, alone or with an unreliable X? | **Steps 27–28 complete.** Step 28 six validated comparisons support local renewal benefit: 74/75 versus 41/54 X-led attempts, lower failure-inclusive wait and higher honest participation in all three seeds. Step 27 observed unreliable-renewal delay; sustained harm and public-network reliability remain unestablished. |
| **E08** | 8 publication/expiry, 12 native choice, 13 admission | Does refreshing before expiry improve leadership beyond exact native 1/2/5-second publication, and how much comes from refusing to follow? | **Step29 complete.** Eight 45-second pilot combinations at one paired seed, 373 groups/170 X-led/188 X attempts. Normal native 5s: 6/24 led, 18 followed; native 1/2s and renewal: 25/25 each. Recruit-only 5s already leads 15/15. Native normal 2s and renewal both yield 73/161 honest first X-led rosters; own-led successful means 0.362/0.290s. Only one search in each renewal arm republished, so reliable renewal advantage remains unestablished. Honest peers native 5s; all min2/target4; default request timeout3 retained (native warning at1/2s); zero added network delay. Raw/CSV/offline-resume checks pass; no larger confirmation started. [Report](results/step29_pilot_20261012_a/REPORT.md). |
| **E09** | 8 publication/expiry, 12 choice/retry, 13 admission | Does pre-expiry renewal add harm beyond stock2s republication for equally unreliable recruit-only X? | **Step30 closed for now: 6/6 verified, all peers shut down. Recommendation: stop further renewal sweeps; reopen only with a concrete new hypothesis predicting materially greater harm and user direction.** Eight private peers,3 paired seeds, seeded100–200ms control-leg delays, no tensor averaging. Added waits +0.274/+0.290/+0.314s (paired mean +0.293s), below material thresholds; wait effect persists in later windows. First X choices40.1%→56.5%, retries268→745; failures5/1076→2/1022, fixed-window groups316→313. All exposure/cadence gates pass; no leadership-success claim and no further trials. [Final report](results/step30_confirm_20261012_v3_a/FINAL_REPORT.md). |

### E04: lifetime ranges with equal training delays

The completed user-approved rerun measures **grouping and leadership**: unique
native leader assembly IDs and X-led IDs regardless of tensor correctness. All
eight smoke trials and forty confirmation trials pass the control-plane checks.
The confirmation has eight peers, five seeds and sixty-second windows per policy,
plus bounded drain: 2,485 assemblies, 1,053 X-led. X-led / total: stock 36/317,
passive stock 106/297, (0,1] 152/311, (0.25,1.25] 156/315, (1,2] 151/307,
(2,3] 150/311, (3,4] 147/312, expiry-only 155/315. All X-led groups have four
members. These small local counts do not establish a best range or deployment
success rate. The ranges are close, and expiry-only is close to 1 Hz refresh.

Leader and X expirations at assembly, their signed gap, active state, roster and
latest logged follower views before handshake are exported with source-event
references. X's ad declaration was inactive at 1,209 of the 1,432 other-led assemblies;
at the remaining 223, the leader was earlier in 210 and X earlier in 13. Deadline
ordering at assembly does not alone identify why a follower chose its leader.
Leader/roster identity, delay schedules, provenance, completeness and bounded drain
remain checked. Thirty-six focused checks pass; raw evidence and hash manifests
are preserved. [Final control-plane report and artifacts](results/step24_grouping_report_20261008.md).
Offline state reconstruction now pairs all 1,184 X searches: 1,053 led, 122
followed and nine without a group. The five refresh arms led 98.7-100% of their own
recorded searches; these local assembly fractions use a different denominator from
network-wide leader share. Of the 1,432 other-led assemblies, 1,178 were Between
rounds, 129 Searching, 94 Following, three Leading, 24 initial launch and four
transition. Withdrawal alone does not end Leading. All 28 stock Searching-phase
groups already included X before its client acceptance was logged. Complete state
windows, original raw hashes and group totals reconcile; 43 focused checks pass.
[State interpretation, per-search records and deadline comparisons](results/step24_x_states_confirm_grouping_20261008_a/report.md).

The strict-averaging evidence below is retained as historical context.

Step 24 compares nominal (0,1], (0.25,1.25], (1,2], (2,3], and (3,4] second
remaining lifetimes at one-second refresh intervals, plus two-second expiration-only,
passive stock-publisher, and stock-following controls. Eight loopback peers use
identical clipped lognormal training-delay distributions, independently seeded per
peer and paired across policies. Delay is after observed completion and before the
next averaging attempt; it is not RPC latency injection. Separate assembled and
verified group counts prevent matchmaking alone from counting as correct averaging.
Native leader timeout and locked tensor traces support failure diagnosis.
[Formulas and status](results/step24_lifetime_report_20261008.md). First smoke:
(0,1] formed ten verified groups, five X-led. The next (2,3] trial aborted after
H0 returned 10.0 instead of 12.5; H2 was named in X's assembled roster but entered
another collective. A fresh diagnostic replay formed ten verified groups, five
X-led, without resolving the original error. A second replay records all four
members entering X's group, then H6 receiving zero expected reducer parts, native
fallback registering 128 failed elements, and a successful future returning 26.0
instead of 28.5. The latter assembled four groups, three verified, two X-led.
This does not establish renewal as the cause of the stream failure.
Confirmation is gated off and (1,2]
was not reached. Counts have unequal exposure and must not be used to rank ranges.

### E07: leadership benefit and control-plane harm (Steps 27–28)

Step 28 is authorized as a direct reliable-single versus reliable-renewed
comparison: two 20-second smoke checks, then three paired fresh seeds (2801–2803)
and 120-second windows. Primary outcome: leadership success per submitted X
attempt. Separately measure failure-inclusive formation time, unconditional
honest first choices, any tries of X, native ACCEPTED replies, and first
assembled-group membership under X versus other leaders. Keep 150ms control
delay and all other Step 27 controls. Implementation and 15 offline checks are
complete. Two smoke trials completed (17 groups, seven X-led), and their raw
traces pass the separately versioned checker. The original checker had clock
schema/join-ID indexing mismatches; original source and failure evidence remain
preserved, and no smoke trial was rerun. All six comparisons completed under
`results/step28_confirm_20261012_v2_b`. Two RPC-trace-incomplete seed-2803 runs
were fully archived and excluded; their replacements used unchanged source
and passed. Smoke counts establish operation, not policy ranking.

Step 28 directly supports renewal benefit in this local setup. X-led attempts:
single **41/54 (75.9%)**, renewed **74/75 (98.7%)**, with improvements in all
three seeds and a passing predefined descriptive signal (mean paired gain
23.0 percentage points; all six trials meet the ten-attempt floor). Honest
first-choice-to-X rates rise **22.2% → 45.7%**; first X-led group membership
rises **22.9% → 40.6%**. X mean capped waiting falls **4.290s → 2.329s**, while
successful-only formation is slower with renewal (**1.845s → 2.198s**).
Thus renewal improves eventual success and participation, with a timing tradeoff.
Total groups are 146 versus 157, including drain. Raw verification confirms
303 total/115 X-led groups and all exact member-step links. All 2,479 prior
hashes remain unchanged; all peers shut down; no further experiments started.
The one failed renewed attempt began 0.558s before the submission cutoff and
stays in the denominator. This is descriptive local evidence, not guaranteed
100% leadership, public-network reliability, or sustained training harm.
Inspect [the Step 28 report](results/step28_confirm_20261012_v2_b/REPORT.md).

Authorized preliminary screen: five publication/response arms, paired seeds
2701/2702, 60-second windows, eight peers, 150ms one-way remote control delay.
Compare native advertisement policy, genuine single 2-second ads, and renewal
at requested 2 writes/sec; single/renewed are each tested with reliable X and
X deliberately accepting, holding 0.5 seconds, then disbanding. X does not
follow in any arm. Two 10-second smoke trials gate the 10-trial screen.

Measure failure-inclusive X leadership per submitted attempt and early assembly
from step_start, honest time-to-group capped at 12 seconds, no-group fractions,
and honest roster memberships per peer-minute. Record expirations at selection,
admission, and assembly; distinguish observed X deadlines from its latest
publication. Classify renewed-pair retries before versus after X assembly.
Use seed-level reliable renewal contrasts and a difference in differences for
renewal-amplified unreliable behavior. Stop after the screen and report; longer
reliability/sustained-harm stages remain deferred. Core and prior source/raw
evidence stay unchanged.

The screen completed all ten trials with passing control integrity and independent
raw/export checks. Counts across seeds: native ads 49 groups and 17/18 X-led
attempts; reliable single 49 and 13/18; reliable renewed 51 and 25/25; unreliable
single 43 and 0/9; unreliable renewed 43 and 0/10. Unreliable X never assembles
by design. The baseline uses native ads but still refuses outbound joins.

The reliable renewed policy led within two seconds more often than native ads
in both seeds (+41.7/+61.5 percentage points), meeting the exploratory policy
screen. Versus the genuine single-ad control the differences were +13.1/-2.1
points, and one control had only seven X attempts: renewal-specific leadership
is inconclusive. Reliable renewal alone did not increase honest capped delay
(-0.077/-0.069 seconds). Unreliable renewal produced 49/58 exact retry witnesses
and adjusted honest delay increases of 0.806/1.128 seconds; the predefined
at-least-one-second-in-both-seeds harm screen did not flag. These 60-second
local measurements do not establish sustained harm or deployment reliability.
All peers shut down; all 1,595 prior source/raw hashes are unchanged. No longer
stage started. Inspect [the report](results/step27_pilot_20261012_a/REPORT.md),
raw traces, per-seed contrasts, and independent `raw_verification.json` in that
suite for exact denominators and provenance.
Requested renewal cadence was 2/sec but achieved about 1.08/sec. An auxiliary
client-receipt linkage bug was found after the run; separate raw receipt audit
confirms 179/182 disbands (151/154 renewed). Original receipt flags undercount;
primary group/retry/delay results are unaffected. Frozen sources remain intact.

### E03: renewal-induced retries

Step 23 tests native exclusion of attempted `(PeerID, expiration)` pairs. Attempt
memory resets between searches, so instrumentation distinguishes averaging steps,
individual searches, and join attempts. The selected leader expiration is recorded
separately from the follower's request expiration. Five paired local trials confirm
that advancing X's expiration permits one additional attempt within the same search
before the original ad expires; identical-expiration controls produce none.

The harm comparison uses fresh eight-peer loopback swarms, reliable/unreliable X,
expiry-only/1 Hz publication, and the same two-second own-ad lifetime. Unreliable
X explicitly accepts, waits 0.5 seconds, and disbands while retaining native admission.
That behavior is additional to renewal. Honest code remains native apart from tracing.
The sixteen-trial smoke passes. Held-out confirmation halted after fifteen trials:
in `reliable_expire_native_bursty_s2322`, H0 returned 0 where its roster required
1.75. The raw trace shows three assembled groups with different overlapping rosters
(H1 overlaps two; H2 and H4 share two), but the cause is unresolved. This invalidates
harm interpretation and blocks the retry-filter diagnostic until this case is
understood and a clean confirmation is possible. No Optimizer workload or WAN-delay
model is included. Full controls, failure rules,
seed sets and raw evidence: [Step 23 report](results/step23_renewal_retry_report_20261008.md).

### E02: short own-ad lifetimes and periodic renewal

Completed on 2026-10-07. See [the step22 report](results/step22_refresh_report_20261007.md)
for source checks, the full fourteen-policy table, configs, and raw traces. X uses
`E_new=max(nextafter(E_previous,+infinity), now+d)`, refuses outbound joins, and retains
native admission and allreduce. It does not read competitors through the matchmaking API.

The final 56-trial grouping sweep compares stock, passive stock-publisher, expiration-only,
and periodic-refresh arms with independently staggered or bursty starts. All returned
tensors were correct. In four scenarios, 0.5 writes/s with a 2.085764-second lifetime
led 4/4 groups using five active ad writes, versus 3/4 for each control. These are small
local counts, not a deployment success-rate estimate.

Twenty-one continuous trials use three new seeds, thirty-second submission windows,
independent simulated training delays, and the same fast X return in every arm.
The 0.5/s policy led 104 verified groups with 117 writes; 4/s led 107 with 393;
expiration-only 1.25-second ads led 106 with 143. Passive stock ads led 75 and stock
minimum-four led 67. Five incorrect-tensor steps were excluded from verified groups.
These results distinguish short-deadline leadership from continuous ad availability.

With a fixed 40 ms storage-processing delay, two safe renewal policies remained visible
at every probe for thirty seconds, while an insufficient-lifetime control was visible
at 80/295 probes despite every write being accepted. Native minimum-five and target-five
also formed nine verified five-member groups in a separate capability test.

**Constraint confirmed in source:** a follower's waiting timeout is captured once and
is not extended by later advertisements. An undersized group can therefore disband
while X's DHT record remains renewed. The 4/s continuous policy sent 300 disband replies
versus nine for 0.5/s. These are stream replies, not counts of distinct groups.
No full Optimizer workload, public swarm, or WAN network model was tested.

### E01 — advertise, never follow, and decide what to do with whoever arrives

- **Why it is not a single cell.** The strategy touches three checkpoints and its point is the
  *absence* of any link between advertising and contributing. Leadership is passive
  (`matchmaking.py:157`: the follower initiates), so declining to follow emits no signal and is
  indistinguishable from losing a matchmaking race. The attacker never enters a state where anything
  is expected of it.
- **This is also where the `reject` control lives.** C13.D measured a leader that accepts a follower
  and stalls it, but never ran the control that decides whether stalling costs the victim more than an
  immediate refusal would. Arms 3 and 4 below are that control. **Do not run it separately.**

- **Arms.** Advertising policy is identical across all four, so the only variable is what the attacker
  does with an arriving follower. Adaptive deadline manipulation (C12.S) is tested separately, which
  means the attacker leads only sometimes here; that is deliberate, and isolates the question.

  1. honest baseline, no attacker;
  2. attacker never follows, but serves incoming followers honestly;
  3. attacker never follows, and rejects incoming requests immediately;
  4. attacker never follows, and accepts then stalls.

  Arm 2 is the one that is easy to skip and should not be. It separates abstention from malice: if the
  swarm degrades in arm 2 alone, that is a finding about occupying leadership without contributing,
  independent of any hostile act.

- **Swarm.** Enough honest peers that alternative groups can form, otherwise arm 2 is indistinguishable
  from arm 3 by construction.

- **Measure.** Join requests actually received by the attacker; victim averaging success and recovery
  time; how often the victim re-selects the attacker across rounds (`past_attempts` is keyed on
  `(peer, expiration)` at `:497` and `:510`, so re-declaration launders it); **whether the attacker is
  ever excluded or blacklisted**, since the claim under test is that abstention leaves no trace; and
  attacker CPU, memory and traffic, which are the first real entries for a cost model.

- **Result.** `step11_abstain.py`, 15 honest peers plus one attacker, group size 4, 4 rounds, **6
  repeats**. Peer count chosen so no arm carries a structural orphan: the attack arms have 16
  participants (4+4+4+4) and `reject` effectively has 15 (4+4+4+3, all at or above `min_group_size`).

  | arm | averaged | median | p90 | errors |
  |---|---|---|---|---|
  | honest baseline | **384/384** | 0.11 s | 0.16 s | none |
  | abstain + serve | **360/360** | 0.10 s | 0.14 s | none |
  | abstain + reject | **360/360** | 0.10 s | **5.25 s** | none |
  | abstain + stall | **336/360** | **3.09 s** | 5.25 s | 24 `AllreduceException` |

  **Abstention is free and invisible, and harms nobody.** The attacker sent **zero** `rpc_join_group`
  messages in every arm while receiving 36, 60 and 88, so the strategy works exactly as described and
  costs no compute. But arms 2 and 3 are indistinguishable from the honest baseline. Declining to
  follow, on its own, disrupts nothing.

  **All of the harm is the stall**, and it is consistent rather than noisy: **56 of 60 in every one of
  the six repeats**, never varying. The `serve` arm lost 12 steps in an earlier 3-repeat run and then
  scored 60/60 six times, so that was a one-off and is not a finding.

  **A tail cost with no failures.** `reject` never loses a step but its p90 is 5.25 s against the
  baseline's 0.16 s: a peer that picks the attacker wastes about five seconds finding another leader.
  Refusing is not free to the victim, it is merely survivable.

- **Decision.** **Demonstrated, and largely negative for the abstention hypothesis.** Abstention is a
  free and untraceable *position*, not an attack. What costs the swarm anything is the hostile act
  performed from that position. That is worth knowing precisely because the position is unpoliced:
  nothing in hivemind ties the right to be followed to having contributed, and `auxiliary` and
  `client_mode` (`averager.py:78`, `allreduce.py:103`) make non-contribution a supported role.
- **Next step.** None for E01 itself. The redundancy finding below belongs in C13.D.

---

## Part 2 — worksheets

Template: Question / Attacker position / Quadrant / Work performed / Observations / Result / Decision /
Next step. **Decision** is `demonstrated`, `possible` (code inference only), or `prevented`.

### C3.T — Checkpoint 3, Tamper: can a peer lie about its DHT identity?

- **Question.** Is the Kademlia `node_id` checked against the identity libp2p already authenticated?
- **Attacker position.** Ordinary peer. One identity, no special position.
- **Quadrant.** direct · commission · blanket · persists while the attacker is online.
- **Work performed.** Code read plus `reproduce_node_id.py`, `reproduce_id_pairing.py`.
- **Observations.** `node_id` is a plain argument of `DHTNode.create()`, used as given (`node.py:171`).
  It arrives self-reported two lines from the authenticated `context.remote_id`, and the two are never
  compared (`protocol.py:150-151`); `context.remote_id` appears exactly once in the whole
  `hivemind/dht/` tree. The `(node_id, peer_id)` pairing travels in every lookup answer as two parallel
  arrays (`:366-367`), which the receiver zips and keeps (`:301-306`).
- **Result.** The two identities are independent and the link between them is sent, not derived.
  Nothing verifies it — not at advertisement, not on contact.
- **Decision.** **Demonstrated.** Root cause the other L2 results depend on.
- **Next step.** None, settled.

### C3.S — Checkpoint 3, Select: can targeted peers enter the five nearest honest nodes' tables?

- **Question.** Can attacker-owned DHT nodes near a known averaging key enter the routing tables of
  the five nearest honest peers and dominate their referrals for that key? Does a full bucket evict
  live honest entries?
- **Attacker position.** Ordinary reachable DHT peers with chosen `node_id` values; the key is known.
- **Work performed.** `step18_routing_admission.py` used real localhost DHT/libp2p pings and find
  RPCs. It compared random versus key-adjacent identities against the same honest node IDs. It first
  warmed honest routing tables, then measured target admission before and after direct pings. A small
  gate used 9 honest nodes and 4 attackers at `bucket_size=4`; four paired confirmations used 21
  honest nodes and 20 attackers at Hivemind's default `bucket_size=20`. The last confirmation found
  the five target peers by an ordinary DHT traversal rather than using the test harness's global view,
  and checked whether an honest advertisement could still be read.
- **Observations.** The small gate returned crafted peers in **20/20** referral slots versus **8/20**
  for random peers. At the default size, the crafted arms returned attacker peers in **96, 100, 96,
  and 96 of 100** slots across the five targets; paired random arms returned **60, 45, 50, and 45**.
  The DHT traversal correctly identified all five closest honest peers in the last paired trial.
  Some crafted peers remained in replacement lists rather than active buckets, and **no live honest
  table entry was evicted** in any arm. The honest advertisement remained readable in both arms of
  the final trial.
- **Result.** Chosen identities plus ordinary contact can dominate referrals from the five nearest
  honest peers. **Admission alone did not censor a record or disrupt a read.** The damaging selective
  lookup behaviour requires a separate malicious response, as already tested in C7.S.
- **Decision.** **Demonstrated as a local routing mechanism, not a new standalone attack.** The
  direct-contact runs assumed the attacker knew the targets; the final DHT traversal discharged that
  assumption in one local default-size swarm. These localhost, warmed-table runs do not measure WAN
  admission time, churn, or the success rate of a complete attack on a public swarm.
- **Next step.** Track 4: independently test failure of the five closest nodes. Do not present this
  admission result as denial or as evidence that honest entries were forced out.

### C6.S — Checkpoint 6, Select: can an attacker choose which records it stores?

- **Question.** How many identities to become custodian of a chosen record, and does hashing the key
  protect it?
- **Attacker position.** Ordinary peers, `num_replicas` of them.
- **Quadrant.** direct · commission · targeted · persists while online.
- **Work performed.** `step3_eclipse.py`, `step3_eclipse_live.py`, `reproduce_group_key_capture.py`.
- **Observations.** Cost is exactly `num_replicas` (5), zero variance, identical from 50 to 10 000
  peers; ~0.1 s each to mint. The group key is `f"{prefix}.0b{group_bits}"` (`key_manager.py:42-44`), a
  public string, so `K = hash(key)` is a *forward* hash anyone can compute — nothing is inverted.
  Against the real averaging key, 5 crafted nodes took **5/5 replicas, 0/6 honest**.
- **Result.** The hash decides where the *record* goes, not where the *attacker* goes, because
  `node_id` is a free parameter rather than a hash output the attacker must produce.
- **Decision.** **Demonstrated.** The contrast with S/Kademlia puzzles, where the identifier really is
  constrained and proximity costs work.
- **Next step.** None, settled.

### C7.T — Checkpoints 4 and 7, Tamper: can an attacker change what a lookup returns?

- **Question.** Can a peer answer lookups untruthfully, and does it gain anything?
- **Attacker position.** Lookup responder — must be in the victim's routing table.
- **Quadrant.** direct · commission · targeted (no collateral damage) · persists while online.
- **Work performed.** `step5_selective.py`, `step6_isolation.py`, `reproduce_forged_pairs.py`.
- **Observations.** Lying about *real* peers gives the same capture as censoring on write, with no
  detection signal. **Inventing peers does not work**: 4 forged pairs at distance 0 took 4 of 5 returned
  slots, yet the record still landed on 5/5 real replicas and read back fine, because `store` skips
  candidates that do not answer (`node.py:412-431`).
- **Result.** Lying about real peers is effective; fabricating peers is close to useless.
- **Decision.** **Demonstrated**, and the negative half is worth keeping. Same missing check, very
  different payoff.
- **Next step.** None, settled.

### C7.S — Checkpoint 7, Select: how many identities control what a victim sees?

- **Question.** Custody is 5 identities (C6.S). Is that enough to control the victim's *view*?
- **Attacker position.** Must occupy the `bucket_size` slots nearest the key in the victim's table.
- **Quadrant.** direct · commission · targeted · persists while online.
- **Work performed.** 28-run campaign in `step6_isolation.py`; two `bucket_size` values; caching on and
  off; padded to 104 peers.
- **Observations.** Capture steps from 14 % at 18 identities to **100 % at exactly 20** = `bucket_size`.
  Doubling `bucket_size` to 40 makes 25 identities fail where 45 work. With caching **off**, 5
  identities suffice (**8/8**); with caching **on**, the same 5 give **0/15**.
- **Result.** Not a flat number: **own every peer the victim asks that has a copy.** Caching sets how
  many that is, raising the price 4× from `num_replicas` to `bucket_size`.
- **Decision.** **Demonstrated.** Caching is not a defence — 20 identities is ~2 s of minting — but it
  is not irrelevant.
- **Next step.** Confirm the law at a third `bucket_size` (8).

### C8.T — Checkpoint 8, Tamper: can a peer write records about someone else?

- **Question.** Advertisements are unsigned. Can an attacker forge one for another peer?
- **Attacker position.** Ordinary peer. No custody, no crafted identities.
- **Quadrant.** **induced** · commission · targeted · **persists after the attacker leaves**.
- **Work performed.** `step8_withdrawal.py`, with a `--no-forge` control.
- **Observations.** A withdrawal is the same record with a marginally later expiration
  (`key_manager.py:61`, `nextafter`), writable by anyone under the victim's subkey. Later expiration
  always wins (`timed_storage.py:81-84`), so the victim cannot overwrite it.
- **Result.** One DHT write removes a peer from discovery, durably. The honest peers do the filtering,
  so the observable misbehaviour belongs to innocent nodes.
- **WAN-delay follow-up (2026-10-02).** `step20_wan_forged_priority.py` paired a later-advertising
  attacker with a no-forgery control, a forged `False` withdrawal, and a forged `True` record with
  far-future expiration. One random-position DHT identity, 28 DHT nodes, five attacker regions, and
  5–7 s / 11–15 s honest windows; one paired seed per region/window. Each attack trial had an accepted
  forgery (**10/10** per attack; **29/29** individual writes), but the latest-scheduled honest peer
  joined the attacker **0/10** per attack and **0/10** in the control. All **90/90** honest steps
  completed. In **0/20** attack trials did a recorded victim read show the intended forged state
  before its first join. This policy forges only one peer chosen from the first usable read. In five
  of ten region/window setups, that peer differed from the victim's first choice; where it matched,
  the victim selected that peer before the forged view arrived. Fourteen
  trials later showed the intended state in the target's lookup, **0.48–1.60 s after the write**;
  these were all too late for its first join. See the raw
  `step20_forged_priority_sweep_sweep_20261002_a_{rounds,events}.csv` and derived `_analysis.csv`.
  This is local RTT emulation and a single-seed policy test, not a public-WAN failure rate or a
  general impossibility result; forging all earlier honest peers was not tested.
- **Decision.** **Demonstrated, but narrower than it looks.** Against the control it does **not** become
  group capture. Availability and censorship, not control over who trains with whom.
- **Next step.** None, settled.

### C11.Obs — Checkpoint 11, Observe: what can an attacker see of who is looking for a group?

- **Question.** Discovery records are public by construction. What does that hand an attacker, and at
  what cost? Every Select result in this project assumes the attacker can recognise its victim; before
  this, only C15.S had measured any part of that.
- **Attacker position.** One ordinary peer. Random `node_id`, no custody, no crafted identity.
- **Quadrant.** **observe/predict** — a prerequisite capability, not an action.
- **Work performed.** `step10_delay.py --phase 1`, 8 honest peers, 2 repeats.
- **Observations.** The observer saw **8/8 advertisers in 3 ms**, with each advertiser's declared
  expiration readable. That expiration is not incidental metadata: it *is* the leadership priority,
  sorted on at `matchmaking.py:478-481`. An ordinary peer therefore reads the election order off the
  DHT before the election happens.
- **Result.** Observation here is free and total, and needs none of the position C6.S and C7.S require.
- **Decision.** **Demonstrated.** Confirms the capability the Select findings assumed.
- **Next step.** None for this cell. The Observe column elsewhere is still empty.

### C11.Delay — Checkpoint 11, Delay: can a slow answer hide a peer that is looking for a group?

- **Question.** Every defence Hivemind has detects silence. What does an attacker that answers, but
  late, cost the victim?
- **Attacker position.** **One** ordinary peer. Contributes no gradients, holds no custody, crafts no
  identity.
- **Quadrant.** direct · **commission** · blanket (selective is available via `context.remote_id`) ·
  the effect ends when the attacker stops.
- **Work performed.** `step10_delay.py --phase 2/3/4`; 8-peer DHT swarms for the mechanism, 4-peer
  averaging swarms plus one slow DHT-only peer for the end-to-end arm.

- **Observations.**

  *Why a slow answer is not diluted.* `get_averagers` calls `dht.get(key, latest=True)`
  (`key_manager.py:79`), which sets `sufficient_expiration_time = float("inf")` (`node.py:542-543`).
  `_SearchState.add_candidate` ends a search only when
  `expiration_time >= sufficient_expiration_time` (`node.py:844-845`) — never, against infinity. **A
  matchmaking read cannot short-circuit on a good answer,** so fast honest responders cannot rescue
  the victim.

  *Latency.* One slow responder of eight sets the victim's whole read time: 0.5 s → 0.504,
  1.5 s → 1.506, 2.9 s → 2.913, against a 0.003 s control. **It does not stack** — 1, 2 and 4 slow
  peers all give ~2.91 s, because traversal is parallel and tracks the single slowest responder.
  Extra attackers buy nothing, which is what makes one peer the entire cost.

  *The cliff at `wait_timeout`, in both directions.* At 0.5/1.5/2.9 s the slow peer is **never
  blacklisted**. At 3.5 s the latency returns to 0.003 s and the peer **is** blacklisted, the RPC
  having timed out (`protocol.py:292`, default 3 s). Crossing the timeout converts commission into
  omission, and Hivemind's existing defence then works exactly as designed. 2.9 s is maximum damage
  at zero detection.

  *Harm: a moving advertiser set, not a static one.* Delay does not shrink a static set — 8/8 seen in
  every arm. It shrinks a changing one. A newly advertising peer is seen by the control in ~50 ms
  every time; under delay it can be missed entirely, and the threshold moves with the delay:

  | advertisement lifetime | no delay | 0.5 s | 1.5 s | 2.9 s |
  |---|---|---|---|---|
  | 1 s | 3/3 | 2/3 | **0/3** | **0/3** |
  | 2 s | 3/3 | 3/3 | **0/3** | **0/3** |
  | 3 s | 3/3 | 3/3 | 3/3 | **0/3** |
  | **5 s** | 3/3 | 3/3 | 3/3 | **0/3** |
  | 8 s | 3/3 | 3/3 | 3/3 | 3/3 |

  Observed staleness is 1.1–2.8× the delay throughout, so the invisibility threshold is ≈ 2× delay —
  **a number the attacker chooses.** This is the falsifiable prediction of the account below, tested
  at three delays and held at all three.

  **`min_matchmaking_time` defaults to 5.0 s** (`averager.py:121`) and the advertisement expiration is
  `get_dht_time() + min_matchmaking_time`, so Hivemind's shipped advertisement lifetime sits inside
  the band a 2.9 s attacker makes invisible.

  *Why the record disappears.* It is fetched, then discarded. Candidates merge per subkey into a
  `DictionaryDHTValue` (`node.py:832-835`), but `get_averagers` reads the result with `.items()`
  (`key_manager.py:84`), which calls `_remove_outdated()` first (`timed_storage.py:96-99`). An
  advertisement that expires during a read the attacker stretched is collected and then dropped on
  the way out.

  *End to end.* Stock `DecentralizedAverager` peers — no subclass anywhere, because the attack is
  purely at the DHT layer — with one extra peer that contributes no gradients and only answers
  `rpc_find` slowly. 4 peers, `min_matchmaking_time` 5.0 s, 6 repeats:

  | arm | peer-steps averaged | elapsed | errors |
  |---|---|---|---|
  | control | **24/24** | 0.0–0.1 s | none |
  | delay 2.9 s | **17/24** | 5.9–92.9 s, median 92.9 s | 7 × `AllreduceException: could not find a group` |

  Per repeat the delay arm gave 4/4, **0/4**, 3/4, 4/4, 3/4, 3/4 — one repeat lost the whole swarm's
  round. **The control arm contains the same attacker node with the delay switched off**, so node
  count and process load are controlled for and §4.6's filler-node confound cannot apply.

  At 8 peers with group size 4, 4 repeats: control 8/8 every time at 0.0–0.1 s; delay gave
  **0/8, 0/8, 0/8, 8/8**. The outcome is bimodal — three repeats lost the entire round and one was
  untouched — not a graded slowdown. Either the attacker sits in the critical path or it does not.

  *Scale, and the correction it forces.* A **random** `node_id` is not enough once the swarm
  outgrows the routing table. One slow responder still set the read latency at 8 and 24 peers
  (~2.91 s against ~0.005 s), but at 48 peers only **3 of 6 repeats** showed any effect at all; the
  rest ran at control latency. This is C7.S's seed-set law again: a slow responder only matters if
  the victim actually asks it.

  *Address choice fixes that, and costs 0.1 s.* Giving the attacker a `node_id` at XOR distance 0
  from the group key — C6.S's crafted address, one identity — makes it a replica, so it is asked
  every time:

  | 48-peer swarm, one attacker | effect present | latency | blacklisted |
  |---|---|---|---|
  | random `node_id` | **3/6** | 0.007 s or 2.91 s | 0 |
  | **crafted `node_id`** | **4/4** | 2.904–2.906 s | **0** |

  The attacker count is identical in both arms — only the address differs — so the difference is
  attributable to address choice and nothing else, which is the same control §4.1 used for custody.
  **C11.Delay therefore composes with C6.S:** one crafted identity buys the position, and one
  `sleep()` inside `wait_timeout` does the rest.

- **Result.** **One peer with one crafted address** can stretch every matchmaking read to just under
  `wait_timeout`, which makes advertisements at Hivemind's default lifetime invisible and costs
  averaging outright — at 8 peers, three rounds in four. It never trains, contributes no gradients,
  and is never blacklisted. Compare C7.S, where interfering with a victim's *view* needed
  `bucket_size` = 20 crafted identities plus custody; this needs **one**, because it does not have to
  outvote the honest answers, only outlast them.
- **Decision.** **Demonstrated**, for a crafted address. For a *random* address it is probabilistic
  and decays with swarm size (3/6 at 48 peers) — state which one is meant. The between-repeat spread
  is bimodal rather than noisy: quote the pattern (0/8, 0/8, 0/8, 8/8), not a mean.
- **Next step.** Three, in order. **Selective delay** — `rpc_find` receives `context: P2PContext`
  (`protocol.py:333`), so the attacker can slow one victim and serve everyone else honestly, as C7.S
  does for filtering; that removes the collateral and should remove any chance of the swarm noticing.
  **Does a starved candidate list make the victim lead?** §4.6 measured 0/12 → 5/12 under filtering;
  if delay reproduces it, this becomes a Select result at a twentieth of the cost. **And the cheapest
  defence to test:** `get_averagers` asks for `latest=True`, which forbids the search from ever
  short-circuiting; whether it genuinely needs the very latest value, or would tolerate a bounded
  staleness, decides whether this whole cell closes with a one-line change.

### C11.D — Checkpoint 11, Deny: can an attacker remove a peer's advertisement?

- **Question.** Can a peer be stopped from advertising availability?
- **Attacker position.** Ordinary peer.
- **Quadrant.** **induced** · commission · targeted · persists.
- **Work performed.** `step8_withdrawal.py` — C8.T seen from the victim's side.
- **Observations.** The forged record marks the victim as no longer looking for a group;
  `get_averagers(only_active=True)` then filters it out for everyone.
- **Result.** The victim becomes invisible to recruiters and cannot advertise over it. The DHT reports
  the failure in a return value nothing reads.
- **Decision.** **Demonstrated**, same availability caveat as C8.T.
- **Next step.** None, settled.

### C11.T — Checkpoint 11, Tamper: can an attacker forge an early deadline?

**Own-record renewal follow-up:** E02/step22 confirms that X can choose a short remaining
lifetime on each own-ad write and advance the absolute expiration as time passes.
This differs from the competitor-reading `nextafter(min,-infinity)` strategy below.
It preserves the strictly-later replacement rule and can maintain visibility beyond
the original expiration, but it does not extend existing follower wait timers.

- **Question.** Leadership goes to the earliest declared deadline. Can it be faked?
- **Attacker position.** Would require custody to show the victim a doctored record.
- **Quadrant.** n/a — the attack does not exist.
- **Work performed.** Attempted as a `forged` arm in `step6_isolation.py`; abandoned after code reading.
- **Observations.** Expiration is one field doing two jobs — the record's **lifetime** and its
  **leadership priority** (`key_manager.py:60`, read back at `:88`).
- **Result.** Lowering it *by a meaningful amount* to win the election makes the advertisement expire
  instead. §4.3's blind attacker declaring `now + 1` against `now + 5` pays four fifths of its window.
- **Decision.** **Partly wrong as originally recorded — see C12.S.** The verdict was written as
  "lowering it to win makes it expire", without a bound, and that over-generalises. Expirations are
  absolute timestamps around 1.79e9, where one float step is **2.4e-7 s**. Undercutting the minimum
  by `np.nextafter(min, -inf)` wins the comparison outright and costs 240 nanoseconds of
  advertisement life — not a tradeoff in any practical sense. Measured in C12.S: 100 % of honest
  peers followed the attacker at 32 peers, with the attacker holding the lowest `peer_id` in **0 of
  40** samples, so the win came from the deadline alone.
- **Next step.** The original arm — forging a *doctored record for someone else*, which needs custody
  — is still not worth retrying, and that part of the verdict stands. What was wrong is the general
  claim that the data model prevents undercutting. It prevents *large* undercuts. It is the same
  primitive hivemind itself uses for withdrawal (`key_manager.py:60`, `nextafter` upward to win the
  strictly-later comparison), pointed the other way.

### C12.S — Checkpoint 12, Select: can an attacker win the leader election at will?

**Latest local evidence:** E02/step22 measures periodic own-ad renewal without competitor
reads. Short deadlines and refusing to follow increased verified leadership in the
tested standalone-averager workload. Increasing frequency from 0.5/s to 4/s changed
verified X-led counts from 104 to 107 across three continuous workloads while raising
active ad writes from 117 to 393. This does not establish capture at will or a universal
optimal frequency. See the report and its matched controls before extrapolating.

- **Question.** Leadership is the bottleneck for everything else — C13.S showed admission is exact
  once the attacker leads (β = k/(k+1), no variance), so whatever decides the election decides
  capture. C11.T established that the deadline cannot be *undercut*. Can it be won anyway?
- **Attacker position.** One ordinary peer. No custody and no crafted `node_id`. A low `peer_id` is
  needed only for the equal-deadline variant; either kind of earlier deadline wins without it.
- **Quadrant.** direct · commission · blanket · persists while the attacker keeps declaring.
- **Work performed.** `step10_delay.py --phase 5`, swarms of 8, 32, 64 and 128 honest peers,
  4 rounds per arm, the election sampled at five points across each matchmaking window.

- **Observations.**

  *The move.* `pop_next_leader` compares **tuples**: `(entry.expiration_time, leader.to_bytes())`
  against `(self.declared_expiration_time, self.peer_id.to_bytes())` (`matchmaking.py:478-481`).
  At equal expiration it falls through to `peer_id`, lowest wins. So the attacker does not need an
  earlier deadline — it needs the **same** deadline and a lower `peer_id`. Matching costs no
  lifetime, which is exactly what C11.T's data-model defence fails to cover: that defence stops you
  going *below* the minimum and says nothing about matching it.

  *The input is free.* Reading the current deadlines is C11.Obs: 3 ms, ordinary peer, no custody.

  *Mirroring the global minimum is fragile.* Peers re-declare continuously, so the earliest record
  on the key is often one about to expire; copying it buys a dead advertisement. Measured directly
  — at 32 peers the naive attacker alternated between mirroring a fresh 5.0 s deadline and a 0.48 s
  one, and was invisible in half the samples. **Mirror the earliest *joinable* deadline instead**:
  a leader closer than `request_timeout` cannot be joined in time anyway (`matchmaking.py:53`
  requires `request_timeout < min_matchmaking_time`).

  *Result, honest peers following the attacker:*

  | swarm | blind (§4.3's strategy) | mirror global min | **mirror joinable min** |
  |---|---|---|---|
  | 32 | 20 % (ad alive 4/20 samples) | 50 % (10/20) | **100 % (20/20)** |
  | 128 | 15 % (3/20) | 100 % (20/20) | **100 % (20/20)** |

  Two repeats each, identical both times. **Whenever the attacker's advertisement was alive and its
  `peer_id` lowest, it took 100 % of followers — 640/640 and 2560/2560.** The election outcome is
  deterministic; only how long the advertisement survives varies, and matching the deadline is what
  makes it survive the whole window.

  *It does not decay with swarm size*, unlike every other attack here. C11.Delay fell to 3/6 at 48
  peers with a random address and C15.S fell from 84 % to 38 %, because both depend on the attacker
  occupying a position among many. This one is a comparison between two tuples whose components the
  attacker controls, so more honest peers simply means more peers reaching the same answer.

  *The low `peer_id` turns out to be unnecessary.* Matching the deadline exactly needs the tie-break,
  but **undercutting it by one float step does not.** Expirations are absolute timestamps around
  1.79e9, so `np.nextafter(min, -inf)` wins outright at a cost of 2.4e-7 s of advertisement life.
  At 32 peers, 2 repeats, the `undercut` arm took **100 % of followers while holding the lowest
  `peer_id` in 0 of 40 samples** — the win came from the deadline alone. This is what corrects
  C11.T, whose "prevented" verdict was written without a bound.

  *Network latency does not close the window.* The attacker must read the key and then declare, and
  on a real network both cross the wire. Sweeping an artificial delay between the read and the
  declaration, at 32 peers with the default 5 s window:

  | attacker latency | none | mirror joinable + low id | undercut |
  |---|---|---|---|
  | 0 s | 0 % | 100 % | 100 % |
  | 0.5 s | 0 % | 100 % | 100 % |
  | 1 s | 0 % | 100 % | 100 % |
  | 2 s | 0 % | 100 % | 100 % |

  Both variants tolerate 2 s between reading and declaring. **This is still loopback for the DHT
  operations themselves**, so it bounds the attacker's own think-time, not the full round-trip cost
  of a real deployment — the honest reading is that the margin exists, not that it has been measured
  on a real network.

  *Cost of the low `peer_id`, if the matching variant is used anyway.* `PeerID` is a hash of a public key, so being lowest of N costs about
  N keypairs in expectation — linear, not exponential — and `identity_path` makes it choosable
  offline (`HANDOFF.md` §6 lists this as an open lever). **Not measured here:** the role was
  assigned to whichever node already held the lowest id, which measures what grinding buys without
  grinding. The linear cost is an argument, not a measurement.

  *A latent inconsistency, worth its own look.* The election uses **two different orderings of the
  same identity**. `leader_queue.top()` breaks ties through `HeapEntry(order=True)`
  (`timed_storage.py:44-47`) on `PeerID.__lt__`, which compares **base58 strings**
  (`datastructures.py:51-54`); the follow decision compares **raw bytes**
  (`matchmaking.py:478-481`). They agree for equal-length identifiers, so nothing breaks today.

- **Result.** An attacker that reads the key, matches the earliest joinable deadline and holds a low
  `peer_id` is followed by **every honest peer, in every sample, at 32 and at 128** — against 15–20 %
  for the blind strategy §4.3 measured. C11.T is sidestepped rather than defeated: it forbids
  undercutting, and this never undercuts.
  *Reconciled against live matchmaking, and the headline does not survive.* `step12_election_live.py`
  runs the same strategies against real `DecentralizedAverager` swarms, with the attacker's
  declaration patched into `PotentialLeaders._declare_averager_periodically`. 15 honest peers, group
  size 4, 4 rounds, 3 repeats:

  | arm | led | followers per round | honest availability |
  |---|---|---|---|
  | none | 0/4 | — | 64/64 |
  | blind (`now + 1 s`) | **4/4** | **3** | 60/60 |
  | undercut (`nextafter`) | **4/4** | **3** | 60/60 |

  **Leadership transfers. Follower share does not.** The attacker wins the election every round, live,
  while serving the toy all-reduce normally when selected. But followers per round is pinned at **3
  in every repeat and both arms**, which is `target_group_size - 1`. At 7 honest peers that is 43 % of
  them; at 15 it is 20 %. **The share halves as the swarm doubles.**

  **The 100 % was an artifact of the simulated election having no capacity constraint.** It computed,
  independently for every peer, whether that peer would prefer the attacker, and every peer did. A
  live leader seats `target_group_size - 1` followers and the rest form their own group. The comparison
  does not decay with swarm size; the outcome does.

  **Blind and undercut are indistinguishable live**, both leading 4/4 and seating 3, where the
  simulation separated them 15-20 % against 100 %. That separation was about how long an advertisement
  survives across sampling points, which stops mattering once a single leadership win per round is
  enough.

  **It is not a denial attack.** Honest availability is 60/60 in both attack arms against a 64/64
  baseline. Leading every round costs the swarm nothing by itself.

  *Blind fixed-deadline pilot.* `step13_blind_leader.py` removed the adaptive lookup. The attacker
  knew the group key, as any peer must in order to advertise on it, but did not call `get_averagers`
  or send an outbound join request. In one localhost run with 7 honest peers, one attacker, group size
  4, one launch-jitter seed and 10 rounds per arm, a fixed `now + 4 s` deadline led **10/10** rounds
  versus **8/10** for the passive `now + 5 s` control. Every win seated exactly three followers. All **140**
  honest peer-steps completed. The guards recorded zero DHT-advertisement reads, zero attacker join
  attempts, zero cleanup leaks and zero harness cancellations, and **69/69** incoming follower
  expirations were deliberately ignored. The two control-arm losses ended in an attacker
  `AllreduceException` after its deadline, without harming the honest peers. Raw output:
  `results/step13_blind_leader_{rounds,events}_full_20260928_final_full_n7_g4_r10_s1313.csv`.

  This passes the pilot's pre-registered signal (`early >= 8/10` and `early > normal`), but **10/10 is
  an observed count, not evidence that the true probability is 100%**. This is not an honest-peer
  baseline: both arms wait passively to lead, and the control already led 8/10. Each arm used a
  freshly generated PeerID set; launch jitter and DHT node positions were seeded. The fixed arm order
  is another pilot limitation. The result has not been repeated across seeds, identities, arm orders,
  swarm sizes or network conditions. It shows that reading competing deadlines was unnecessary in
  this setup, not that the group key can be unknown or that a fixed one-second advantage universally
  guarantees leadership. The attacker also served an ordinary four-element all-reduce when selected,
  so this experiment does not establish a resource-free attack.

  *Adaptive known-key test on the real DHT and matchmaker.* The fixed-deadline pilot was not a sound
  answer to the question "how does the attacker know what earlier means?" Two staged tests now answer
  that narrower question without reading any hidden schedule from the harness.

  1. `step14_adaptive_leader.py` used real Hivemind DHT nodes and five independently advertised
     deadlines. Given only its own `GroupKeyManager`, the attacker called `get_averagers`, selected the
     earliest deadline with enough time left to join, and published `np.nextafter(deadline, -inf)`.
     An independent observer saw all six records and the attacker strictly first in **10/10** fresh-key
     confirmation trials (after a 1/1 smoke test). This proves DHT visibility and ordering, not
     leadership.
  2. `step15_adaptive_leader_live.py` then used stock `DecentralizedAverager` matchmaking and all-reduce
     over the real local DHT/libp2p stack: three honest peers, one attacker, target group size four.
     Each paired trial used fresh DHT and averager processes; arm order alternated. The attacker polled
     the DHT every 50 ms, observed whatever viable records arrived for 100 ms, and acted without knowing
     the honest population or their schedules. It never sent a join request. In the final confirmation,
     the one-ULP undercut produced a complete capture—three followers and a completed attacker
     all-reduce—in **10/10 observed trials**. Publishing one ULP after the latest observed deadline
     produced **0/10** captures. All **60/60** honest peer-steps completed, with no hard cancellations,
     cleanup leaks or outbound attacker joins. The undercut group completed in a median **0.497 s**;
     the late control took **4.714 s** before the honest group completed and the unused attacker was
     cancelled. Raw output:
     `results/step15_adaptive_live_{rounds,events}_confirm_20260928_trace_confirm_h3_g4_r10_s1515.csv`.

  The trace also exposed a DHT acknowledgement subtlety: in two successful undercut trials the honest
  peers found the record and joined before the attacker's aggregate `store()` call returned, so the
  declaration task was cancelled without a completed store result. A pre-await event is therefore the
  causal write-attempt record; the three received joins are the stronger evidence that the publication
  propagated. This matches Hivemind's stock declaration loop, which does not condition matchmaking on
  the aggregate store boolean.

  **Scope.** The participant knows the exact current group key, as it must to advertise. Hivemind offers
  no practical way to enumerate an actually unknown key. This is a five-node, below-`bucket_size`,
  localhost mechanism test with favourable latency—not a WAN or public-swarm result. The attacker is
  **training-conditional**, not free: it avoids following and training when it loses, but it still
  polls and writes the DHT, serves join streams and performs the ordinary all-reduce when selected.
  Finally, 10/10 is an observed count under these conditions, not proof of 100% probability.

  *Routed, measured-delay follow-up (2026-09-30).* `collect_ripe_anchor_rtt.py` saved public
  [RIPE Atlas anchor-mesh](https://atlas.ripe.net/docs/apis/rest-api-reference/anchor-measurements/)
  IPv4 ping results for seven countries in
  `results/ripe_anchor_rtt_20260930.json`. Five countries (SE, US, BR, IN, AU) had a complete directed
  RTT matrix; median cross-region link RTT was **233.4 ms**, with pair medians from **99.1 to 503.0 ms**.
  `step19_wan_delay_leader.py` reused Hivemind's DHT, libp2p matchmaking and all-reduce in fresh local
  swarms with **28 DHT peers** (23 routing-only fillers, above default `bucket_size=20`) and four
  averagers. It imposed one measured pairwise RTT per DHT find/store RPC and a half-RTT per
  join-stream direction. Within each paired delay comparison, the key, DHT node IDs, attacker policy
  and schedule seed were held fixed; the swarm was restarted between arms, producing fresh PeerIDs.
  Instrumentation counted 20 distinct attacker lookup
  responders per run, including responders outside its initial nearest-neighbour seed in one pair.

  | Honest deadline window | No added delay | Measured RTTs | Control |
  |---|---|---|---|
  | 5–7 s, three paired seeds | undercut captured **3/3** | undercut captured **0/3** | all **18/18** honest steps completed |
  | 11–15 s, one matched seed | — | undercut captured **1/1** | late-deadline arm **0/1**, all **6/6** honest steps completed |

  In the short-window delayed runs, attacker reads took a median **0.79–0.93 s** versus
  **0.06–0.07 s** without delay. Two delayed trials never published; one published an independently
  DHT-visible undercut but got no followers. The long-window delayed run read in **0.92 s**, wrote in
  **1.16 s**, and seated all three followers. Two earlier long-window delayed seeds also captured
  **2/2** against **0/2** late controls; these were before the fixed-key cross-delay pairing change,
  though each attack/control pair itself used the same key. Raw paired outputs are
  `results/step19_wan_delay_{rounds,events}_{smoke_20260930_short_paired_f23_r1_s1922,confirm_20260930_short_confirm_f23_r2_s1923,smoke_20260930_long_matched_f23_r1_s1925}.csv`.

- **Decision.** **Latency-sensitive local attack, not a demonstrated public-WAN attack.** The
  below-bucket `step15` result alone was inadequate. The routed follow-up shows both a successful
  measured-delay case and a deadline boundary where the same policy fails. Its RIPE sample is a small
  spatial sample of ICMP pings, not a full distribution of Hivemind traffic. The emulator omits
  packet loss, churn, DHT ping and connection-setup delay, and all-reduce delay. It does not establish
  how often this attack succeeds on a real deployment or that an unknown key can be targeted.
- **Next step.** If presenting this as a real-network attack, measure on a multi-host deployment or
  an authorized live averaging swarm, with TCP/libp2p RTT, loss, churn, lookup paths and the margin
  remaining when the DHT read finishes. Otherwise present it only as a conditional local result.

### C13.D — Checkpoint 13, Deny: can a leader accept a peer and then stall it?

- **Question.** If the attacker leads, can it simply never start the averaging?
- **Attacker position.** Group leader. One identity.
- **Quadrant.** direct · **commission** · targeted · ends when the attacker stops.
- **Work performed.** `step9_stall.py`, 1 attacker + 1 honest peer, with an honest-leader baseline.
- **Observations.** Baseline averaged in **0.03 s**. Under attack the victim was held **22.97–23.00 s**
  against a predicted 23.0 s, five times over, consuming its whole 120 s step without averaging. The
  wait is set by the victim's own settings (`matchmaking.py:216`), not the attacker's. Giving up does
  not help: the retry-exclusion list is keyed on `(peer, expiration)` pairs (`:497`, `:510`), and
  re-declaring with a fresh expiration — ordinary behaviour, every `min_matchmaking_time` — mints a pair
  it has never seen. Neither ban mechanism applies: both watch for peers that go *silent*.
- **Result.** A leader can deny a chosen peer its round for one identity and a `sleep()`. The clearest
  case of commission defeating omission-based defences.
- **Decision.** **Demonstrated, but not yet attributable.** The control has not been run — a leader that
  refuses immediately. If refusal costs the victim the same, this reduces to "a failed round takes a
  round" and there is no finding.
- **The `reject` control, run as arms 3 and 4 of E01.** **The stall result stands.** Refusing
  immediately cost the victim no steps at all (360/360) while stalling cost 24 of 360, and the median
  latency was 0.10 s against 3.09 s, a **31x** difference. So stalling is strictly worse than
  refusing, and C13.D is not merely "a failed round takes a round".
- **But the severity is a property of the deployment, not of the attack, and the original worksheet
  does not say so.** C13.D measured total denial with **one** honest peer, which had no alternative.
  At 15 honest peers the same attack denies only **6.7 %** of steps, because a stalled peer recovers
  *within* a single `step()`: the stall times out, matchmaking continues, another leader is found.
  Quote the denial as conditional on scarce alternatives.
- **Next step.** None. The control is run and E01 carries the numbers.

### C13.S — Checkpoint 13, Select: can a leader choose its group?

- **Question.** If the attacker leads, how much of the group can it make its own?
- **Attacker position.** Group leader, plus *k* peers of its own.
- **Quadrant.** direct · commission · targeted · ends when the attacker stops.
- **Work performed.** `step3_capture.py`, `step6_isolation.py`.
- **Observations.** The filter is exact: β = k/(k+1) with no variance — 0.500, 0.667, 0.800 for one, two
  and four attacker peers. One attacker beside the victim turns α = 0.062 into a mean β of 0.315.
- **Result.** β is chosen, not inherited. The bottleneck is winning leadership, not filtering.
- **Decision.** **Demonstrated.** The project's main result.
- **Next step.** Grind a low `peer_id` — the tie-break for equal deadlines (`matchmaking.py:478-481`).

### C14.T — Checkpoints 14 and 16, Tamper/Select: can a leader give peers conflicting reducer indices?

- **Question.** The leader broadcasts the ordered group roster in `BEGIN_ALLREDUCE`, and roster position
  becomes the reducer index. Can a malicious leader send a different permutation to each follower so
  several peers believe they own the same index?
- **Attacker position.** Current group leader. No DHT custody, routing control, Sybils or network race
  is needed after leadership has been obtained.
- **Quadrant.** direct · commission · blanket within the group · model corruption can persist.
- **Mechanism.** `leader_assemble_group` creates and shuffles the roster, then each follower receives it
  on a separate authenticated join stream (`matchmaking.py:371-388`, `:317-323`). A follower checks only
  that its own PeerID appears and that the roster and `gathered` lists have equal length (`:391-405`).
  It does not check a common roster digest, agreement with admitted members, uniqueness, or cross-peer
  consistency. `group_id` is leader supplied and is not bound to the roster. All-reduce assigns shard
  *i* to the peer at roster position *i*; data messages carry the common group ID but no roster hash or
  logical coordinate. Equal-shaped shards from different coordinates are therefore accepted.
- **Small gate.** `step16_roster_equivocation.py` ran Hivemind's real `AllReduceRunner` RPC over libp2p
  with two peers and two equal shards. The common-roster control completed with the correct average
  `[5, 550]`. With leader view `[L,F]` and follower view `[F,L]`, both peers completed cleanly with no
  bans or fallback but produced `[500,55]` and `[55,500]`. This established silent corruption before
  adding matchmaking.
- **Wire-level confirmation.** `step17_roster_equivocation_live.py` used four real
  `DecentralizedAverager` peers, the local DHT, authenticated join streams and stock all-reduce. The
  designated leader was an explicit precondition, not an election result. In the attack arm it kept
  the same four unique PeerIDs and group ID but swapped each recipient into the leader's own roster
  index, reordering `gathered` in lockstep. Every peer began with the identical tensor `[0,1,2,4]`, so
  correct averaging must leave it unchanged; four one-float shards made coordinate confusion
  shape-valid.
- **Observed result.** After a 1/1 paired smoke test, five fresh paired trials produced **0/5** silent
  corruptions in the common-roster control and **5/5** under equivocation. All **40/40** steps across
  both arms completed. Every control output was correct; all **20/20 attacked peer outputs were
  wrong**, with maximum absolute error between **2.25 and 3.0**. All 15 malicious `BEGIN_ALLREDUCE`
  messages retained exactly the canonical membership set and assigned the recipient the leader's
  reducer index. There were zero reducer bans, fallback elements, RPC/all-reduce errors, timeouts,
  hard cancellations or cleanup leaks. Raw output:
  `results/step17_roster_live_{rounds,events}_confirm_20260928_confirm_t5_s1717.csv`.
- **Result.** **A malicious leader can silently corrupt honest tensors without changing a model update.**
  It changes only control data: the order of authenticated PeerIDs in the final roster. Network delay
  does not remove the capability because the conflicting messages travel on the leader's existing join
  streams; delay affects completion time, not whether the recipient-specific payload is valid.
- **Decision.** **Demonstrated end to end on the real local Hivemind protocol path.** The conditional
  statement matters: this establishes what a malicious leader can do, not how often it becomes leader.
  Unlike the deadline-undercut experiment, the attack itself contains no DHT timing assumption.
- **Next step.** Move to routing-table flooding, the third presentation track. A defence should make
  the ordered roster independently derivable or bind it into a transcript/group identifier that peers
  cross-check; a leader signature alone is insufficient because the leader is the equivocator.
- **Follow-up (2026-10-20, `announcement_probe.py`).** A four-node loopback probe
  re-ran this with embedded origin/coordinate markers, a designated leader B, a
  single victim A, and a legitimate three-member control, under one averaging step
  per node (`results/announcement_s1_20261020_c`, seed 1). It reproduces the
  ordering result with explicit markers — A announced `[A,D,B,C]` vs `[B,D,A,C]`
  for B,D,C, all four `success`, **coordinate markers wrong for all four**, no node
  at the true mean (max error 6 at B,D,C, 18 at A) — and adds a **membership
  omission** arm: B announces `[B,D,C]` to D,C and aggregates `[B,D,C]` but
  announces `[B,D,A,C]` to A. B,D,C compute the correct three-member mean with A
  absent from all 48 rows (**clean exclusion**); A's chunks are rejected by B,D,C's
  reducers (`ValueError('<peer id> is not in list')`), it bans them, hits
  `missing_part_fallback` for its whole share, and **falls back to its own local
  value while returning `success`** (output 50 off the true mean). So beyond silent
  corruption, an omitting leader can cleanly drop a participant from the aggregate
  and silently isolate it. Controls match the independent mean to ≤ 5.1e-6; all
  conditions shut down cleanly. See `ANNOUNCEMENT_PROBE.md` and HANDOFF §4.17.

### C15.S — Checkpoint 15, Select: does re-forming groups shed the attacker?

- **Question.** Groups reshuffle between rounds. Does that dilute an attacker's presence?
- **Attacker position.** Group member — needs to have shared **one** group with the victim.
- **Quadrant.** **observe/predict** — a prerequisite capability, not an action. It enables Select at
  checkpoint 13 rather than doing anything itself. Targeted, and persists indefinitely once established.
- **Work performed.** `step4_reformation.py` at 16, 32 and 48 peers.
- **Observations.** The shuffle is seeded on the group id and indexed by each member's position
  (`key_manager.py:94-104`), both known to every member. Prediction was correct **63 times out of 63**.
  Scale changes only how often the right bucket is also the same group: 84 % / 72 % / 38 % at 16 / 32 /
  48 peers, against 56 % / 40 % / 18 % without tracking.
- **Result.** Re-formation sheds a naive attacker, not a tracking one.
- **Decision.** **Demonstrated.** The one filled cell in the Observe dimension, which is why it composes
  with everything else.
- **Next step.** Combine with C13.S. Tracking gets the attacker into the victim's bucket; admission
  converts that into the group. Never run together.

### C17.T — Checkpoint 17, Tamper: what does a poisoned contribution cost the model?

- **Question.** With β = 1/4, how far can an attacker move the model?
- **Attacker position.** Ordinary group member. No leadership needed.
- **Quadrant.** direct · commission · blanket within the group · **the model damage persists** even
  though the attack does not.
- **Work performed.** `step7_poison.py`, 8 peers, group size 4, 3 seeds, poisoning the send path only so
  the attacker's own tensors stay honest.
- **Observations.** A sign-flipped gradient alone is lost in the average. Combined with the
  sender-supplied weight, applied exactly as received (`partition.py:242`, read off the wire at
  `allreduce.py:340`), the same gradient moves the model **35–148×** further from the optimum.
  Coordinate-wise median brings it back to 0.5–2.3×.
- **Result.** The damage comes from the unchecked weight, not from clever gradients.
- **Decision.** **Demonstrated.**
- **Next step.** None, settled.

### C21.D — Checkpoint 21, Deny: can an attacker retire a peer early?

- **Question.** Can a peer be made to look as though it has left?
- **Attacker position.** Ordinary peer.
- **Quadrant.** **induced** · commission · targeted · persists.
- **Work performed.** Same mechanism as C8.T / C11.D.
- **Observations.** Departure is announced by re-writing the advertisement with
  `looking_for_group=False` (`matchmaking.py:545`). Nothing proves the announcement came from the peer
  it describes.
- **Result.** Any peer can be retired at any time.
- **Decision.** **Demonstrated**, same availability caveat.
- **Next step.** None, settled.

---

## Part 3 — what to do next

This is the canonical **four-track** experiment queue. The staging rule is permanent: for every track,
run the smallest mechanism test first, inspect the trace and controls, and scale only if that gate
produces a signal. A successful smoke test is not itself the headline result.

1. **Adaptive, training-conditional leader capture — conditional local evidence, not a public-WAN claim.** This combines the
   first two questions: learn real advertised deadlines from the known group key, undercut the earliest
   viable one, never follow another leader, and act only when followers select the attacker.
   `step14_adaptive_leader.py` passed 1/1 smoke plus 10/10 independent-observer DHT trials.
   `step15_adaptive_leader_live.py` then passed 1/1 smoke plus **10/10 complete captures** against
   **0/10** late-deadline controls on real local Hivemind matchmaking; all 60 honest peer-steps
   completed. This remains known-key, below-`bucket_size`, localhost evidence with unrealistically short
   DHT paths. It repeats an existing timing mechanism and is not accepted as evidence that the attack
   works on a real routed Hivemind deployment. `step19_wan_delay_leader.py` now adds 23 DHT-only
   peers and measured inter-region RTTs: at a 5–7 s deadline window, undercut captured **3/3**
   zero-delay groups but **0/3** delayed groups; at 11–15 s it captured **1/1** delayed groups against
   **0/1** late controls. This tests a routed, latency-sensitive local mechanism, not public-WAN
   success. See C12.S for the RTT source and emulation limits.
2. **Malicious-leader roster/index equivocation — confirmed.** `step16_roster_equivocation.py` first
   showed silent coordinate mixing over the real libp2p all-reduce. `step17_roster_equivocation_live.py`
   then injected recipient-specific rosters through actual `BEGIN_ALLREDUCE` streams. Five fresh paired
   trials gave **0/5** corrupt controls and **5/5** silently corrupt attack runs; all 20 attacked peers
   completed with wrong tensors and zero bans, fallback elements or errors. This attack assumes the
   adversary is already leader, but the malicious action itself has no DHT timing dependency.
3. **Flood the routing tables of the five peers closest to the group key — admission confirmed,
   standalone disruption not shown.** `step18_routing_admission.py` found that key-adjacent identities
   dominated **96–100/100** lookup referrals across five targets at default `bucket_size=20`, versus
   **45–60/100** for random identities in four paired trials. A stock DHT lookup found all five target
   peers in the final trial, and an honest advertisement was still readable. No live honest routing
   entry was displaced. This is an enabling mechanism for C7.S, not a fifth independent attack.
4. **Simulate failure of the five peers closest to the group key — next.** In a local testbed, stop or blackhole
   those peers and measure lookup success, latency and recovery. This is a controlled resilience test,
   not authorization to disrupt a public network.

After those four presentation tracks:

5. **Open L6 entirely.** Donor priority is self-declared, installed tensors are unchecked. The only
   place an attacker replaces a model rather than nudging it, and every cell is empty.
6. **Open L5.** False or selectively hidden progress, forced catch-up, forced state download. Note its
   records are signed and L3's are not — understand why before proposing signing as a defence.
7. **Try an omission attack, anywhere.** Every demonstrated result is still commission. C11.Delay
   partly discharges this: it does not perform an omission attack, but it **locates the boundary**
   between the two. The same attacker, same position, same method, is invisible at 2.9 s and detected
   and neutralised at 3.5 s — the first time in this project that one of Hivemind's existing defences
   has been observed firing. What remains is a deliberate omission arm, refusing to serve `rpc_find`,
   to check the defence works when the silence is sustained rather than incidental.
8. **Fill the Observe column.** Several findings assume the attacker can recognise the victim. Only
   C15.S measured it.

**Method.** Test single actions before chains. Use the smallest swarm that exposes the mechanism, then
repeat at a size where the routing table does not hold the whole network. Measure **recovery** as well
as success — how fast honest refresh, caching, retries or the next averaging round undo the attack.

**Chains worth testing, once the parts are measured:**

- routing control → false progress view → forced state synchronisation
- routing control → selected advertisements → malicious leader
- malicious leader → conflicting roster → stall or malicious reduction
- donor-list control → selected donor → malicious state installation

**Defences, only after behaviour is measured.** Fix at the layer, not the checkpoint — that is what the
L1–L6 grouping is for. Deriving `node_id` from the authenticated key changes all of L2; signing
advertisements changes L3's forgery rows. Re-run the same experiment afterwards to show whether the
defence blocks the attack, moves it, or merely raises its cost.
