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

For each answer, record the immediate effect, the downstream effect on training, the attacker
position required, and whether it was **inferred from code** or **demonstrated in a live swarm**.

## Why "checkpoint" and not "lifecycle stage"

This is not one linear lifecycle, and treating it as one is misleading. Bootstrap happens once.
Routing, record refresh, progress tracking and donor advertising run continuously in the background.
Matchmaking and averaging repeat. State download is a conditional recovery branch.

So the unit of analysis is a **control checkpoint**: either a message exchange, or a **local decision**
whose inputs came from other peers. Local decisions send no packet but are still in scope, because
peer-supplied data drives them — choosing a leader is the clearest example.

**The empty cells matter as much as the full ones.** Empty means nobody has looked, not that it is safe.

## Things that change how the map is read

**Checkpoints 11–17 happen twice.** `{run_id}_grad_averager` and `{run_id}_state_averager` are two
separate averagers, each with its own group key, matchmaking and leader election. Everything measured
so far attacked the gradient averager. The state averager runs the same code over optimizer state and
has never been touched.

**Checkpoints 13 and 14 are one channel.** The roster is not delivered separately — `group_id` and
`ordered_peer_ids` arrive in the `BEGIN_ALLREDUCE` message on the same `rpc_join_group` stream. Whoever
controls 13 controls 14 for free.

**Attacker position matters.** An ordinary peer, a record custodian, a lookup responder, a group
leader, a group member and a state donor do not have the same powers. libp2p authenticates the
transport and the PeerID, so an outsider cannot alter someone else's stream — but an authenticated peer
can still lie inside a valid message, and nothing checks the contents.

**The actions compose.** Routing control enables selective lookup results, which control leader choice,
which controls admission, which exposes the victim to a stall or a malicious reduction.

---

## Part 1 — the control-checkpoint map

`D` = demonstrated in a live swarm (worksheet below). `P` = possible, inferred from code but not run.
`X` = prevented, with reason. `open` = nobody has looked. `—` = does not apply.

| # | Control checkpoint | Control data or decision | Obs | Delay | Deny | Tamper | Select |
|---:|---|---|---|---|---|---|---|
| 1 | Bootstrap and transport discovery | initial multiaddresses, authenticated PeerIDs, learned addresses | open | open | open | open | open |
| 2 | DHT reachability and time check | `node_id`, availability, DHT time in ping | open | open | open | open | open |
| 3 | Routing admission, liveness, eviction | claimed `(node_id, PeerID)` association; whether a peer answers | open | open | open | **D — C3.T** | open |
| 4 | Lookup referrals | sets of nearest `(node_id, PeerID)` pairs returned | open | open | open | **D — C7.T** | open |
| 5 | Record responsibility transfer | records handed to a newly learned closer node | open | open | open | open | open |
| 6 | Record publication and replication | key, subkey, value, expiration, chosen custodians, `store_ok` | open | open | open | open | **D — C6.S** |
| 7 | Record lookup and local-view construction | returned values, referrals, result merging, caching | open | open | open | **D — C7.T** | **D — C7.S** |
| 8 | Record refresh, withdrawal, expiration | re-publication and replacement values; time-based expiry | open | open | open | **D — C8.T** | open |
| 9 | Training-progress exchange | identity, epoch, samples, speed, timestamp, client mode | open | open | open | open | open |
| 10 | Progress and synchronisation decision | **local:** advance, average, wait, or download state | open | open | open | open | open |
| 11 | Averaging availability exchange | group key, PeerID subkey, active flag, self-declared deadline | open | open | **D — C11.D** | **X — C11.T** | — |
| 12 | Candidate priority and leader choice | **local:** order by deadline and PeerID, choose whom to contact | open | open | open | open | open |
| 13 | Join negotiation and admission | schema hash, deadline, gathered data, group key, code, redirect | open | open | **D — C13.D** | open | **D — C13.S** |
| 14 | Group roster distribution | leader-generated group id, ordered PeerIDs, gathered metadata | open | open | = 13 | open | = 13 |
| 15 | Next-group-key derivation | **local:** derive next bucket from group id and roster position | open | open | open | open | **D — C15.S** |
| 16 | Role and work assignment | **local:** advertised bandwidth, mode, roster position | open | open | open | open | — |
| 17 | All-reduce exchange | group id, framing, **sender-supplied weight**, tensor parts, codes | open | open | open | **D — C17.T** | — |
| 18 | Session failure and recovery | silence, deadlines, cancellation, disband, banning, retries | open | open | open | open | open |
| 19 | State-donor advertisement and selection | donor PeerID, **self-declared priority**, expiration, ordering | open | open | open | open | open |
| 20 | State download and installation | request, metadata, tensor chunks, validation, installation | open | open | open | open | open |
| 21 | Shutdown and withdrawal | progress, averaging and donor withdrawals; records left to expire | open | open | **D — C21.D** | open | — |

**12 of roughly 90 cells have evidence.** Wholly untouched checkpoints: **1, 2, 5, 9, 10, 12, 16, 18,
19, 20**. The **Observe** and **Delay** columns are empty everywhere — including under findings that
already exist, where the attacker's ability to see the victim was assumed rather than measured.

Three gaps stand out.

**Checkpoint 9 versus 11.** Progress records are signed (`progress_tracker.py:108`,
`RSASignatureValidator`). Averaging advertisements are not. Same DHT, same peer — the machinery is in
the tree and simply is not applied to the second one.

**Checkpoints 19 and 20.** Donor priority is a number each peer declares about itself, and downloaded
tensors are installed with no validation. Every finding so far *nudges* a model; this would **replace**
it.

**Checkpoint 12.** A local decision entirely driven by records other peers wrote. It is the hinge
between the DHT results and the matchmaking results, and it has never been attacked directly.

---

## Part 2 — worksheets

Template: Question / Attacker position / Work performed / Observations / Result / Decision / Next step.
**Decision** is one of `demonstrated`, `possible` (code inference only), or `prevented`.

### C3.T — Checkpoint 3, Tamper: can a peer lie about its DHT identity?

- **Question.** Is the Kademlia `node_id` checked against the identity libp2p already authenticated?
- **Attacker position.** Ordinary peer. One identity, no special position.
- **Work performed.** Code read plus `reproduce_node_id.py`, `reproduce_id_pairing.py`.
- **Observations.** `node_id` is a plain argument of `DHTNode.create()`, used as given (`node.py:171`).
  It arrives self-reported two lines from the authenticated `context.remote_id`, and the two are never
  compared (`protocol.py:150-151`); `context.remote_id` appears exactly once in the whole
  `hivemind/dht/` tree. The `(node_id, peer_id)` pairing travels in every lookup answer as two parallel
  arrays (`:366-367`), which the receiver zips and keeps (`:301-306`).
- **Result.** The two identities are independent and the link between them is sent, not derived.
  Nothing verifies it — not at advertisement, not on contact.
- **Decision.** **Demonstrated.** Root cause the other DHT results depend on.
- **Next step.** None, settled.

### C6.S — Checkpoint 6, Select: can an attacker choose which records it stores?

- **Question.** How many identities does it take to become custodian of a chosen record, and does
  hashing the key protect it?
- **Attacker position.** Ordinary peers, `num_replicas` of them.
- **Work performed.** `step3_eclipse.py`, `step3_eclipse_live.py`, `reproduce_group_key_capture.py`.
- **Observations.** Cost is exactly `num_replicas` (5), zero variance, identical from 50 to 10 000
  peers; ~0.1 s to mint each. The group key is `f"{prefix}.0b{group_bits}"` (`key_manager.py:42-44`), a
  public string, so `K = hash(key)` is a *forward* hash anyone can compute — nothing is inverted.
  Against the real averaging key, 5 crafted nodes took **5/5 replicas, 0/6 honest**.
- **Result.** The hash decides where the *record* goes, not where the *attacker* goes, because
  `node_id` is a free parameter rather than a hash output the attacker must produce.
- **Decision.** **Demonstrated.** This is the contrast with S/Kademlia puzzles, where the identifier
  really is constrained and proximity costs work.
- **Next step.** None, settled.

### C7.T — Checkpoints 4 and 7, Tamper: can an attacker change what a lookup returns?

- **Question.** Can a peer answer lookups untruthfully, and does it gain anything?
- **Attacker position.** Lookup responder — must be in the victim's routing table.
- **Work performed.** `step5_selective.py`, `step6_isolation.py`, `reproduce_forged_pairs.py`.
- **Observations.** Lying about *real* peers gives the same capture as censoring on write, with no
  detection signal and no collateral damage. **Inventing peers does not work**: 4 forged pairs at
  distance 0 took 4 of 5 returned slots, yet the record still landed on 5/5 real replicas and read back
  fine, because `store` skips candidates that do not answer (`node.py:412-431`).
- **Result.** Lying about real peers is effective; fabricating peers is close to useless.
- **Decision.** **Demonstrated**, and the negative half is worth keeping. Same missing check, very
  different payoff.
- **Next step.** None, settled.

### C7.S — Checkpoint 7, Select: how many identities control what a victim sees?

- **Question.** Custody is 5 identities (C6.S). Is that enough to control the victim's *view*?
- **Attacker position.** Must occupy the `bucket_size` slots nearest the key in the victim's table.
- **Work performed.** 28-run campaign in `step6_isolation.py`; two `bucket_size` values; caching on and
  off; padded to 104 peers.
- **Observations.** Capture steps from 14 % at 18 identities to **100 % at exactly 20** = `bucket_size`.
  Doubling `bucket_size` to 40 makes 25 identities fail where 45 work. With caching **off**, 5
  identities suffice (**8/8**); with caching **on**, the same 5 give **0/15**.
- **Result.** The rule is not a flat number: **own every peer the victim asks that has a copy.** Caching
  sets how many that is, raising the price 4× from `num_replicas` to `bucket_size`.
- **Decision.** **Demonstrated.** Caching is not a defence — 20 identities is ~2 s of minting — but it
  is not irrelevant.
- **Next step.** Confirm the law at a third `bucket_size` (8).

### C8.T — Checkpoint 8, Tamper: can a peer write records about someone else?

- **Question.** Advertisements are unsigned. Can an attacker forge one for another peer?
- **Attacker position.** Ordinary peer. No custody, no crafted identities.
- **Work performed.** `step8_withdrawal.py`, with a `--no-forge` control.
- **Observations.** A withdrawal is the same record with a marginally later expiration
  (`key_manager.py:61`, `nextafter`), writable by anyone under the victim's subkey. Later expiration
  always wins (`timed_storage.py:81-84`), so the victim cannot overwrite it.
- **Result.** One DHT write removes a peer from discovery, durably.
- **Decision.** **Demonstrated, but narrower than it looks.** Against the control it does **not** become
  group capture. Availability and censorship, not control over who trains with whom.
- **Next step.** None, settled.

### C11.D — Checkpoint 11, Deny: can an attacker remove a peer's advertisement?

- **Question.** Can a peer be stopped from advertising availability?
- **Attacker position.** Ordinary peer.
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
- **Work performed.** Attempted as a `forged` arm in `step6_isolation.py`; abandoned after code reading.
- **Observations.** Expiration is one field doing two jobs — the record's **lifetime** and its
  **leadership priority** (`key_manager.py:60`, read back at `:88`).
- **Result.** Lowering it to win the election makes the advertisement expire instead.
- **Decision.** **Prevented** — not by a check, but by the data model. Recorded so nobody retries it.
- **Next step.** None. Do not retry.

### C13.D — Checkpoint 13, Deny: can a leader accept a peer and then stall it?

- **Question.** If the attacker leads, can it simply never start the averaging?
- **Attacker position.** Group leader. One identity.
- **Work performed.** `step9_stall.py`, 1 attacker + 1 honest peer, with an honest-leader baseline.
- **Observations.** Baseline averaged in **0.03 s**. Under attack the victim was held **22.97–23.00 s**
  against a predicted 23.0 s, five times over, consuming its whole 120 s step without averaging. The
  wait is set by the victim's own settings (`matchmaking.py:216`), not the attacker's. Giving up does
  not help: the retry-exclusion list is keyed on `(peer, expiration)` pairs (`:497`, `:510`), and
  re-declaring with a fresh expiration — ordinary behaviour, every `min_matchmaking_time` — mints a pair
  it has never seen. Neither ban mechanism applies: both watch for peers that go *silent*, and this one
  answers everything.
- **Result.** A leader can deny a chosen peer its round for one identity and a `sleep()`.
- **Decision.** **Demonstrated, but not yet attributable.** The control has not been run — a leader that
  refuses immediately. If refusal costs the victim the same, this reduces to "a failed round takes a
  round" and there is no finding.
- **Next step.** **TODO — run the `reject` arm.** Cheapest open item in the project.

### C13.S — Checkpoint 13, Select: can a leader choose its group?

- **Question.** If the attacker leads, how much of the group can it make its own?
- **Attacker position.** Group leader, plus *k* peers of its own.
- **Work performed.** `step3_capture.py`, `step6_isolation.py`.
- **Observations.** The filter is exact: β = k/(k+1) with no variance — 0.500, 0.667, 0.800 for one, two
  and four attacker peers. One attacker beside the victim turns α = 0.062 into a mean β of 0.315.
- **Result.** β is chosen, not inherited. The bottleneck is winning leadership, not filtering.
- **Decision.** **Demonstrated.** The project's main result.
- **Next step.** Grind a low `peer_id` — the tie-break for equal deadlines (`matchmaking.py:478-481`) —
  to win the rounds the attacker still loses.

### C15.S — Checkpoint 15, Select: does re-forming groups shed the attacker?

- **Question.** Groups reshuffle between rounds. Does that dilute an attacker's presence?
- **Attacker position.** Group member — needs to have shared **one** group with the victim.
- **Work performed.** `step4_reformation.py` at 16, 32 and 48 peers.
- **Observations.** The shuffle is seeded on the group id and indexed by each member's position
  (`key_manager.py:94-104`), both known to every member. Prediction was correct **63 times out of 63**.
  Scale changes only how often the right bucket is also the same group: 84 % / 72 % / 38 % at 16 / 32 /
  48 peers, against 56 % / 40 % / 18 % without tracking.
- **Result.** Re-formation sheds a naive attacker, not a tracking one.
- **Decision.** **Demonstrated.**
- **Next step.** Combine with C13.S. Tracking gets the attacker into the victim's bucket; admission
  converts that into the group. Never run together.

### C17.T — Checkpoint 17, Tamper: what does a poisoned contribution cost the model?

- **Question.** With β = 1/4, how far can an attacker move the model?
- **Attacker position.** Ordinary group member. No leadership needed.
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
2. **Open checkpoints 19 and 20.** Donor priority is self-declared, installed tensors are unchecked.
   The only place an attacker replaces a model rather than nudging it, and every cell is empty.
3. **Open checkpoint 14 Tamper.** A leader controls the roster for free, so sending *conflicting*
   rosters to different followers costs it nothing extra. Untested.
4. **Open checkpoint 9.** False or selectively hidden progress, forced catch-up, forced state download.
   Note its records are signed and checkpoint 11's are not — understand why before proposing signing as
   a defence.
5. **Attack the state averager.** Every result here targeted the gradient averager; the same code runs
   over optimizer state.
6. **Fill the Observe column.** Several findings assume the attacker can recognise the victim. That was
   never measured.

**Method.** Test single actions before chains. Use the smallest swarm that exposes the mechanism, then
repeat at a size where the routing table does not hold the whole network. Measure **recovery** as well
as success — how fast honest refresh, caching, retries or the next averaging round undo the attack.

**Chains worth testing, once the parts are measured:**

- routing control → false progress view → forced state synchronisation
- routing control → selected advertisements → malicious leader
- malicious leader → conflicting roster → stall or malicious reduction
- donor-list control → selected donor → malicious state installation

Only after behaviour is measured, evaluate narrow defences at the checkpoint where the attacker first
gains control, and re-run the same experiment to show whether the defence blocks the attack, moves it,
or merely raises its cost.
