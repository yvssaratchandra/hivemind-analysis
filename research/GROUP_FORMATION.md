# Group Formation in Hivemind, and How an Attacker Can Control It

Hivemind peers train a model together by repeatedly averaging their gradients. Averaging happens in small
**groups** of peers that assemble themselves, with no coordinating server. This document describes exactly how
a group is assembled, which factors decide its membership, and which of those factors an attacker controls.

Code references are to Hivemind 1.2.0.dev0 (fork point `4bd43b7`). Primary file:
`hivemind/averaging/matchmaking.py`.

---

## 0. Terms used

| Term | Meaning |
|---|---|
| **Peer** | One participant: a process training the model and talking to others. |
| **PeerID** | A peer's cryptographic identity, derived from its public key. Verified by TLS: it cannot be forged. |
| **DHT** | A key→value store spread across all peers, with no server. Any peer can write a value under a name and any peer can read all values written under that name. |
| **Group key** | The DHT name peers use to advertise for partners. Format `prefix.0bBITS` (`averaging/key_manager.py:44`). |
| **All-reduce** | The operation a group performs: average every member's vector and give the result to every member. |
| **Leader** | The peer that assembles a group. Not a privileged role in the training itself. |
| **Follower** | A peer that asks to join a leader's group. |
| **Expiration / deadline** | A timestamp each peer publishes, meaning "I intend to average before this moment". |

---

## 1. How a group is formed

Five steps. All peers run the same procedure; there is no coordinator.

### Step 1 — Each peer advertises itself

A peer ready to average writes one record to the DHT (`key_manager.py:46-67`):

| field | value |
|---|---|
| key | the group key, e.g. `bert_training.0b` |
| subkey | the peer's own PeerID |
| value | `True` ("looking for a group") |
| expiration | the peer's chosen **deadline** |

The deadline is not only a delete-after time. It is a declaration of intent, and it decides who leads. Each
peer sets it as (`matchmaking.py:527-529`):

```python
new_expiration_time = min(max(step.scheduled_time, get_dht_time() + self.min_matchmaking_time),
                          self.search_end_time)
```

That is: at least `min_matchmaking_time` (default 5 s) into the future, so other peers have time to see it. A
background task republishes the record as it expires and withdraws it on exit.

### Step 2 — Each peer reads the advertisements

A second background task polls the same group key every few seconds (`_update_queue_periodically`,
`matchmaking.py:500-518`) and builds a list of every peer currently advertising there, with each one's
deadline. Peers already tried, and the reader itself, are skipped.

Because all peers read the same DHT key, **all peers see the same list**.

### Step 3 — The peer with the earliest deadline becomes leader

Each peer sorts the list by the pair `(expiration_time, peer_id)` and compares the first entry against itself
(`pop_next_leader`, `matchmaking.py:479-497`):

```python
if maybe_next_leader is None or (entry.expiration_time, maybe_next_leader.to_bytes()) > (
    self.declared_expiration_time, self.peer_id.to_bytes()
):
    ... wait to be contacted instead ...
```

- If some other peer's deadline is earlier, this peer sends that peer a join request. It becomes a **follower**.
- If this peer's own deadline is earliest, it stops searching and waits. It becomes the **leader**.

The PeerID breaks ties between identical deadlines.

No messages are exchanged to reach this decision. It is a fixed calculation over information every peer
already has, so every peer independently computes the same answer. The earliest deadline is chosen because
that peer will average soonest, so assembling around it wastes the least time.

### Step 4 — The leader admits followers and closes the group

Followers call `rpc_join_group` on the leader. The leader checks each request
(`_check_reasons_to_reject`, `matchmaking.py:341-368`) and may reject it:

| Rejection | Meaning |
|---|---|
| `BAD_SCHEMA_HASH` | your tensors have a different shape or dtype; averaging is impossible |
| `BAD_GROUP_KEY` | you are advertising under a different group key |
| `BAD_EXPIRATION_TIME` | your deadline is earlier than mine, so you should be leading, not me |
| `NOT_A_LEADER` | I am already following someone else (the reply names them, so you can be redirected) |
| `DUPLICATE_PEER_ID` | you are already in my list |
| `GROUP_IS_FULL` | I already hold `target_group_size` members |
| `PROTOCOL_VIOLATION` | malformed request |

Accepted followers are appended in the order their requests arrive (`matchmaking.py:275`):

```python
self.current_followers[context.remote_id] = request
```

The leader closes the group when either condition is met first:

- **full** — `len(current_followers) + 1 >= target_group_size` (`matchmaking.py:277-280`); or
- **out of time** — its own declared deadline passes, and it has at least `min_group_size` members
  (`matchmaking.py:290-297`). Otherwise it disbands.

Closing produces a `GroupInfo` (`leader_assemble_group`, `matchmaking.py:371-388`):

```python
group_id = DHTID.generate().to_bytes()
ordered_peer_ids = list(self.current_followers)
ordered_peer_ids.append(self.peer_id)
random.shuffle(ordered_peer_ids)
```

It contains a fresh random `group_id`, the member list in a shuffled order, and each member's metadata aligned
to that order. The leader sends it to every member; each follower checks it is present in the list and adopts
it unchanged (`follower_assemble_group`, `matchmaking.py:390-405`). All members now hold an identical
description of the group, and averaging begins.

The order matters because it is a work assignment: member *i* is responsible for averaging slice *i* of the
vector, so all members must agree on the same mapping. It is shuffled so the same peers do not repeatedly
receive the same slice.

### Step 5 — Members recompute their group key

After assembly, each member derives a new value for the `BITS` part of its group key
(`update_key_on_group_assembled`, `key_manager.py:94-104`):

```python
rng = random.Random(group_info.group_id)          # seed shared by all members
index = group_info.peer_ids.index(self.peer_id)   # this peer's own position
generalized_index = rng.sample(range(num_buckets), group_info.group_size)[index]
```

The shared seed means every member draws the same sequence; reading it at their own index gives each member a
different result. The group therefore splits across different group keys without exchanging any further
messages, so that over successive rounds every peer eventually averages with every other peer.

### Default configuration

These defaults change the behaviour above substantially, and `hivemind.Optimizer` overrides none of them:

| Parameter | Default | Source | Effect |
|---|---|---|---|
| `initial_group_bits` | `""` | `averager.py:120` | `BITS` is empty, so the key is `prefix.0b`. **One group key for the whole run.** Step 5 becomes a no-op: the final line of `update_key_on_group_assembled` discards its result when the bits are empty, so **no peer ever changes group key.** |
| `target_group_size` | `None` | `averager.py:118` | No "full" condition. **Groups close when the leader's deadline passes**, with whoever has joined. |
| `min_group_size` | `2` | `averager.py:120` | **A group of two peers is enough to proceed.** |
| `min_matchmaking_time` | `5.0` s | `averager.py:121` | Earliest deadline a peer will declare. |
| `request_timeout` | `3.0` s | `averager.py:122` | How long a follower waits for a leader's reply. |

Under these defaults every peer in a run advertises on the same key, group membership is decided anew each
round, and a group can be as small as two peers.

A caller using `hivemind.Optimizer` cannot simply override `target_group_size` for the averager that
matters. `Optimizer` accepts an `averager_opts` dictionary and its docstring (`optim/optimizer.py:157`)
says it is "forwarded to both GradientAverager and TrainingStateAverager", but the code forwards it only
to the state averager (`optimizer.py:263`); `_make_gradient_averager` (`optimizer.py:300-315`) never
receives it. The gradient averager therefore keeps the `None` default — and it is the one whose groups
decide who your gradients are mixed with. The only supported route to it is the `grad_averager_factory`
argument (`optimizer.py:192`), binding the keyword into the factory, since `GradientAverager` passes
unrecognised keywords through to `DecentralizedAverager`.

**Measured consequence.** Because no peer ever changes group key under these defaults, every round is
the same race between the same peers, and nothing re-randomises membership. Whether membership mixes is
then decided by whether arrival order varies from round to round — a property of the environment, not
of the protocol. In 20-round runs on one machine, 16-peer swarms froze into near-constant cliques (a
pair sharing a group in all 20 rounds; 10–15 distinct partitions where a uniform random partition gives
20) while 24-peer swarms were statistically indistinguishable from a fair draw. See `HANDOFF.md` §6.1.

---

## 2. The factors that decide membership

| # | Factor | What it decides | Set by |
|---|---|---|---|
| 1 | **Group key** (`prefix.0bBITS`) | which pool of peers you draw from | run configuration; a single pool by default |
| 2 | **Declared deadline** | who becomes leader | **each peer, freely** |
| 3 | **PeerID** | ties between equal deadlines | the peer's keypair |
| 4 | **Leader's admission decisions** | which requesters are accepted | **the leader** |
| 5 | **Arrival order of join requests** | who gets in before the group closes | network latency and timing |
| 6 | **`schema_hash`** | which peers are compatible at all | the model being trained |
| 7 | **`target_group_size` / `min_group_size`** | when the leader closes the group | run configuration |
| 8 | **`random.shuffle`** | member order inside the group | the leader's random number generator |

Group membership is therefore **not a random draw from the swarm**. It is the outcome of a race, decided by
who declared the earliest deadline and whose join request arrived first. The only genuinely random elements
are the `group_id` and the ordering inside an assembled group, both chosen by the leader.

---

## 3. Can an attacker control how many Sybil nodes are in a group?

**Yes.** A *Sybil node* here means one of many peer identities operated by a single attacker. Two of the eight
factors are directly attacker-controlled (2 and 4), and a third is strongly influenceable (5).

Three routes, in decreasing order of control.

### Route A — Become the leader, then choose the members

This gives near-deterministic control and follows directly from the mechanism above.

1. The attacker declares a deadline **earlier than every honest peer's**. Factor 2 is a free choice, and
   nothing validates it, so this reliably makes the attacker the leader.
2. Because every peer sorts the same list and contacts the earliest-deadline peer first, **all peers in the
   group key contact the attacker**.
3. The leader sees each requester's authenticated PeerID (`context.remote_id`) *before* deciding to admit it,
   so admission can be made conditional on identity.
4. The attacker admits exactly the peers it wants — for example one target peer plus its own Sybils — and
   rejects the rest with any of the codes in Step 4.

With `min_group_size = 2` as the default, the smallest viable outcome is a group of exactly
**{attacker, target}**, in which the attacker constitutes **half the group with a single identity**. More
generally the attacker sets the malicious fraction to any value it likes by choosing how many of its own
identities to admit alongside the target.

Two constraints on this route, both worth measuring rather than assuming:

- **The deadline is a trade-off.** An earlier deadline wins leadership but closes the group sooner, leaving
  less time for the intended target to send its join request. There is a window in which the attacker's
  deadline must be earlier than every honest peer's but late enough for the target to arrive.
- **The behaviour requires a modified client.** An unmodified leader admits everyone who passes the checks.
  Selective admission means overriding `rpc_join_group`. Hivemind's own test suite already does this kind of
  subclassing (`tests/test_allreduce_fault_tolerance.py:32,87` define `FaultyAverager` and
  `FaultyAllReduceRunner`), so no change to Hivemind itself is needed.

### Route B — Join an honest leader's group as several peers

If the attacker does not lead, it can still raise its share of an honest leader's group by running many peers
that all request to join. Admission is first-come-first-served (factor 5), so peers with lower latency to the
leader are admitted preferentially. This is probabilistic rather than deterministic: the attacker raises the
expected malicious fraction but does not fix it.

### Route C — Advertisements without processes

An entry in the group key is only a DHT record. The record is unsigned and its subkey is an unverified
PeerID (`key_manager.py:46`), so an attacker can write many advertisements without running the corresponding
peers. This is very cheap — one DHT write each.

**But these identities cannot poison a gradient.** With no process behind them they cannot complete an
all-reduce. Their effect is disruption: a peer that selects a non-existent leader waits for a reply, times
out, and loses a round. Route C therefore raises the attacker's share of the *candidate list*, while Routes A
and B raise its share of the *assembled group*. Only the latter affects averaging, and the two should not be
reported as the same quantity.

---

## 4. Which factors are controllable, and how

| Factor | Attacker-controllable? | How |
|---|---|---|
| Group key | Partly | The key is derived from the run name, which is public. By default there is one key, so no targeting is needed. |
| **Declared deadline** | **Fully** | Any value. Declaring the earliest deadline wins leadership. |
| PeerID | Indirectly | Fixed per keypair, but keypairs are free to generate, so the tie-break value can be ground. |
| **Leader admission** | **Fully, once leading** | Accept the target and own identities; reject everyone else. |
| Arrival order | Strongly | Low latency, co-location, or pre-opened connections to the leader. |
| `schema_hash` | Not a barrier | Any real participant in the run matches it. |
| `target` / `min_group_size` | No | Set by run configuration; but the default `min_group_size = 2` is what makes a two-peer group possible. |
| Member ordering | Fully, once leading | The shuffle uses the leader's own RNG and nobody verifies it. |

---

## 5. Status of these claims

- **Verified in code:** the five formation steps, the eight factors, the default values, and the fact that the
  deadline and admission decisions are unvalidated. Every claim above cites the line that supports it.
- **Measured:** that membership is not a fair draw in general, and that whether it looks like one depends
  on the testbed rather than on the protocol (`HANDOFF.md` §6.1). This is the honest-swarm baseline any
  attacker result has to be compared against, and it is not the β ≈ α that the literature assumes.
- **Also measured, and cheaper than expected:** winning leadership needs no modified client. The declared
  deadline is `min(max(scheduled_time, now + min_matchmaking_time), search_end_time)`
  (`matchmaking.py:527-531`), and `min_matchmaking_time` is just `Optimizer`'s `matchmaking_time`
  argument (`optimizer.py:287,307`), so a peer configured with a smaller value than its neighbours
  declares the earliest deadline and is elected leader by their own sorting. Only the *selective
  admission* half of Route A needs a subclass.
- **Not yet measured:** the practical success rate of Route A. The deadline trade-off in §3 means the attacker
  must win leadership *and* keep the group open long enough for the target to join. Whether that window is
  wide in practice is an experimental question, not one the source code answers.
- **Related measurements already made:** `FINDINGS.md` §B measures Route C (advertisement flooding) and §C
  measures the damage caused by a given malicious fraction, but neither measures Route A.

---

*Documentation only; part of the additive `research/` tree. No core Hivemind code was modified.*
