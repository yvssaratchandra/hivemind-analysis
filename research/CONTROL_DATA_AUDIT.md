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
silence. **Nothing detects commission.**

That is why the stall worked: the attacker answered every DHT query and actively replied `ACCEPTED`, so
it was never silent and never visible. Stated generally, **Hivemind's fault-tolerance model is
crash-fault; a Byzantine peer that stays online and keeps talking is outside it entirely.**

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
| 3 | Routing admission, liveness, eviction | open | open | open | **D — C3.T** | open |
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
| 11 | Averaging availability exchange | **D — C11.Obs** | **D — C11.Delay** | **D — C11.D** | **partly X — C11.T** | — |
| 12 | Candidate priority and leader choice *(local)* | open | open | open | open | **D — C12.S** |
| 13 | Join negotiation and admission | open | open | **D — C13.D** | open | **D — C13.S** |
| 14 | Group roster distribution | open | open | = 13 | open | = 13 |
| 15 | Next-group-key derivation *(local)* | open | open | open | open | **D — C15.S** |

### L4 — All-reduce · *control data: roles (16), recovery (18), the weight and group id in 17*

`hivemind/averaging/allreduce.py`, `partition.py`. Reduce-scatter over the assembled group.

| # | Checkpoint | Obs | Delay | Deny | Tamper | Select |
|---:|---|---|---|---|---|---|
| 16 | Role and work assignment *(local)* | open | open | open | open | — |
| 17 | All-reduce exchange *(payload too)* | open | open | open | **D — C17.T** | — |
| 18 | Session failure and recovery | open | open | open | open | open |

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
| L2 DHT | 2–8 | 5 |
| L3 Matchmaking | 11–15 | 8 |
| L4 All-reduce | 16–18 | 1 |
| L5 Optimizer | 9–10 | **0** |
| L6 State transfer | 19–20 | **0** |
| spanning | 21 | 1 |

**15 of roughly 90 cells.** The work sits almost entirely in L2 and L3 — correct for the research
question, since those are the two control-plane layers. The **Observe** and **Delay** columns are empty
**everywhere except checkpoint 11**, including under findings already treated as settled, where the
attacker's ability to see the victim was assumed rather than measured. C11.Obs is the first direct
measurement of that assumption at the availability exchange, and C11.Delay is the project's first
Delay result of any kind.

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

- **Question.** Leadership is the bottleneck for everything else — C13.S showed admission is exact
  once the attacker leads (β = k/(k+1), no variance), so whatever decides the election decides
  capture. C11.T established that the deadline cannot be *undercut*. Can it be won anyway?
- **Attacker position.** One ordinary peer. No custody, no crafted `node_id`. It does need a low
  `peer_id`, which is cheap — see below.
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
- **Decision.** **Demonstrated for the election itself.** The election is computed by applying
  Hivemind's own comparison rule to real records written by the real `GroupKeyManager`, sampled
  across the window — faithful to the rule, but **not a live matchmaking run**. §4.3's blind attacker
  reached 57–67 % in live matchmaking against this method's 15–20 %, so the two must be reconciled
  before the 100 % is quoted as a capture rate. **Do not report this as capture yet.**
- **Next step.** Rerun against live `DecentralizedAverager` matchmaking and compare. If it holds,
  this composes with C13.S — leadership at will, then exact admission — which is the strongest chain
  this project has. Then: does `past_attempts` exclusion erode it across rounds, since the attacker
  re-declares the same deadline each time?

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
- **Next step.** **TODO — run the `reject` arm.** Cheapest open item in the project.

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

The map decides the order, not intuition.

1. **Run the `reject` control (C13.D).** Cheapest, and it decides whether the stall result stands.
2. **Open L6 entirely.** Donor priority is self-declared, installed tensors are unchecked. The only
   place an attacker replaces a model rather than nudging it, and every cell is empty.
3. **Open checkpoint 14 Tamper.** A leader controls the roster for free, so sending *conflicting*
   rosters to different followers costs it nothing extra. Untested.
4. **Open L5.** False or selectively hidden progress, forced catch-up, forced state download. Note its
   records are signed and L3's are not — understand why before proposing signing as a defence.
5. **Try an omission attack, anywhere.** Every demonstrated result is still commission. C11.Delay
   partly discharges this: it does not perform an omission attack, but it **locates the boundary**
   between the two. The same attacker, same position, same method, is invisible at 2.9 s and detected
   and neutralised at 3.5 s — the first time in this project that one of Hivemind's existing defences
   has been observed firing. What remains is a deliberate omission arm, refusing to serve `rpc_find`,
   to check the defence works when the silence is sustained rather than incidental.
6. **Fill the Observe column.** Several findings assume the attacker can recognise the victim. Only
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
