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

### L1 — libp2p transport

`hivemind/p2p/`. Authenticated connections. `FINDINGS.md` calls this layer sound, which is true of the
transport crypto — but **bootstrap peer selection also lives here and is unexamined.** Which peers you
first learn the network from is a real surface that "authenticated connections" does not cover.

| # | Checkpoint | Obs | Delay | Deny | Tamper | Select |
|---:|---|---|---|---|---|---|
| 1 | Bootstrap and transport discovery | open | open | open | open | open |

### L2 — Kademlia DHT

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

### L3 — Matchmaking

`hivemind/averaging/matchmaking.py`, `key_manager.py`. Advertise, elect a leader, admit members.
**A single fix — signing advertisement records — would change rows 11 and 15.**

| # | Checkpoint | Obs | Delay | Deny | Tamper | Select |
|---:|---|---|---|---|---|---|
| 11 | Averaging availability exchange | open | open | **D — C11.D** | **X — C11.T** | — |
| 12 | Candidate priority and leader choice *(local)* | open | open | open | open | open |
| 13 | Join negotiation and admission | open | open | **D — C13.D** | open | **D — C13.S** |
| 14 | Group roster distribution | open | open | = 13 | open | = 13 |
| 15 | Next-group-key derivation *(local)* | open | open | open | open | **D — C15.S** |

### L4 — All-reduce

`hivemind/averaging/allreduce.py`, `partition.py`. Reduce-scatter over the assembled group.

| # | Checkpoint | Obs | Delay | Deny | Tamper | Select |
|---:|---|---|---|---|---|---|
| 16 | Role and work assignment *(local)* | open | open | open | open | — |
| 17 | All-reduce exchange | open | open | open | **D — C17.T** | — |
| 18 | Session failure and recovery | open | open | open | open | open |

### L5 — Optimizer

`hivemind/optim/optimizer.py`, `progress_tracker.py`. Accumulate, average, apply.
**Cross-layer note:** progress records ride the DHT but are **signed** (`progress_tracker.py:108`),
while L3's advertisements are not. Same machinery, applied in one place and not the other.

| # | Checkpoint | Obs | Delay | Deny | Tamper | Select |
|---:|---|---|---|---|---|---|
| 9 | Training-progress exchange | open | open | open | open | open |
| 10 | Progress and synchronisation decision *(local)* | open | open | open | open | open |

### L6 — State transfer

`hivemind/averaging/averager.py` + `hivemind/optim/state_averager.py`. **Not one of the five layers in
`FINDINGS.md`** — it is neither all-reduce nor matchmaking nor the plain optimizer step, and it needs
naming separately. Donor priority is a number each peer declares about itself; downloaded tensors are
installed with no validation. Every other finding *nudges* a model; this one would **replace** it.

| # | Checkpoint | Obs | Delay | Deny | Tamper | Select |
|---:|---|---|---|---|---|---|
| 19 | State-donor advertisement and selection | open | open | open | open | open |
| 20 | State download and installation | open | open | open | open | open |

### Spanning L2 / L3 / L5

| # | Checkpoint | Obs | Delay | Deny | Tamper | Select |
|---:|---|---|---|---|---|---|
| 21 | Shutdown and withdrawal | open | open | **D — C21.D** | open | — |

### Coverage

| layer | checkpoints | cells with evidence |
|---|---|---|
| L1 libp2p | 1 | **0** |
| L2 DHT | 2–8 | 5 |
| L3 Matchmaking | 11–15 | 5 |
| L4 All-reduce | 16–18 | 1 |
| L5 Optimizer | 9–10 | **0** |
| L6 State transfer | 19–20 | **0** |
| spanning | 21 | 1 |

**12 of roughly 90 cells.** The work sits almost entirely in L2 and L3 — correct for the research
question, since those are the two control-plane layers. The **Observe** and **Delay** columns are empty
everywhere, including under findings already treated as settled, where the attacker's ability to see
the victim was assumed rather than measured.

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
- **Result.** Lowering it to win the election makes the advertisement expire instead.
- **Decision.** **Prevented** — not by a check, but by the data model. Recorded so nobody retries it.
- **Next step.** None. Do not retry.

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
5. **Try an omission attack, anywhere.** Every demonstrated result so far is commission. Refusing to
   serve `rpc_find` is the obvious first one, and it is the only quadrant where Hivemind's existing
   defences would actually fire — which makes it the honest test of whether they work.
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
