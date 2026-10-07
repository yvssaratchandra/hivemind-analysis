# Q&A Notes — Hivemind DHT security

A running log of questions asked during the analysis and the answers, with every claim pinned to code
(`file.py:line`) or to a retrieved citation. Companion to [`FINDINGS.md`](FINDINGS.md) (results) and
[`HIVEMIND_EXPLAINED.md`](HIVEMIND_EXPLAINED.md) (how the system works).

**Status of claims here:** code claims were checked against the tree at fork point `4bd43b7` (Hivemind
1.2.0.dev0). Two items are marked **CORRECTION** — they revise statements made earlier in the analysis that
turned out to be wrong. Literature claims come from a web search performed 2026-09-07; the headline result
there is a *negative* one and is stated as such.

---

## Contents

- [Q1. Does Hivemind implement S/Kademlia's protections?](#q1-does-hivemind-implement-skademlias-protections)
- [Q2. Do Geth v1.9.0 / IPFS-Kubo hardening apply here?](#q2-do-geth-v190--ipfskubo-hardening-apply-here)
- [Q3. What model does the research code train?](#q3-what-model-does-the-research-code-train)
- [Q4. Is a PeerID enough to reach a peer?](#q4-is-a-peerid-enough-to-reach-a-peer)
- [Q5. Why does peer *ordering* in a group matter?](#q5-why-does-peer-ordering-in-a-group-matter)
- [Q6. How does an attacker target a node, and know which is valuable?](#q6-how-does-an-attacker-target-a-node-and-know-which-is-valuable)
- [Q7. Can `node_id` really be set manually?](#q7-can-node_id-really-be-set-manually)
- [Q8. If picking `node_id` were disabled, would anything break?](#q8-if-picking-node_id-were-disabled-would-anything-break)
- [Q9. How do disjoint lookup paths help against Sybils?](#q9-how-do-disjoint-lookup-paths-help-against-sybils)
- [Q10. How do per-group averages become one model?](#q10-how-do-per-group-averages-become-one-model)
- [Q11. Is the hash mechanism fixed? Can keys be enumerated?](#q11-is-the-hash-mechanism-fixed-can-keys-be-enumerated)
- [Q12. Why do five identities give total custody?](#q12-why-do-five-identities-give-total-custody)
- [Q13. How are groups formed — is it random?](#q13-how-are-groups-formed--is-it-random)
- [Q14. Do peers switch rooms after each epoch?](#q14-do-peers-switch-rooms-after-each-epoch)
- [Q15. Has anyone published on this attack?](#q15-has-anyone-published-on-this-attack)
- [Corrections index](#corrections-index)

---

## Q1. Does Hivemind implement S/Kademlia's protections?

> *Crypto puzzles for node IDs, mandatory disjoint lookup paths — are any of these in the code?*

**No, none of them.**

| S/Kademlia control | present? | evidence |
|---|---|---|
| Crypto puzzle on node ID (static) | **no** | `DHTID.generate()` — `dht/routing.py:262-273` |
| Crypto puzzle per lookup (dynamic) | **no** | no hits for `proof.of.work\|puzzle\|difficulty` repo-wide |
| Node ID bound to a keypair at all | **no** | `dht/node.py:171` accepts a caller-supplied `node_id` verbatim |
| Disjoint lookup paths | **no** | `dht/traverse.py`, `node.py:328,663` — one shared heap + one shared visited set |
| Sibling list | **no** | no hits for `sibling` |
| Per-IP/subnet bucket limits | **no** | `KBucket.add_or_update_node` (`routing.py:185-207`) keys on `node_id` only |

`DHTID.generate()` hashes either 255 random bits or a caller-supplied `source` with SHA-1 and accepts the
result unconditionally — no leading-zero constraint on `H(H(pubkey))`, no per-lookup challenge.

`num_workers=4` (`node.py:42`) *looks* like S/Kademlia's *d* parallel lookups but is not: all workers share one
candidate heap and one `visited_nodes` set, so they are four hands in one contaminated bucket, not four
independent paths. See [Q9](#q9-how-do-disjoint-lookup-paths-help-against-sybils).

**What does exist** (none of it Sybil-relevant):

- Kademlia §2.2 preference for long-lived peers — `routing.py:209`, acted on at `protocol.py:397-401`.
- `Blacklist` (`node.py:897-931`) — liveness backoff keyed on PeerID, 5 s base, cleared on any success.
- A reachability check: `rpc_ping(validate=True)` reverse-pings the sender and compares
  (`protocol.py:153-155`). Only invoked at bootstrap (`node.py:226`); confirms *consistency*, not entitlement.
- `RSASignatureValidator` (`dht/crypto.py:12`) for records whose key/subkey embeds `[owner:ssh-rsa ...]`.
  Opt-in per record type. The RSA key is a *third* identity, distinct from PeerID and node_id.
- `TokenAuthorizerBase` (`utils/auth.py:48`) — optional centralized admission control. **Off by default.**

---

## Q2. Do Geth v1.9.0 / IPFS-Kubo hardening apply here?

### Geth v1.9.0

| Geth measure | applies to Hivemind? |
|---|---|
| Node ID = f(secp256k1 pubkey), unchoosable | **Yes — highest-value port.** Hivemind already has an authenticated pubkey (PeerID). `node_id = H(PeerID)` needs no new crypto. This is defense **D1**. |
| Endpoint proof before table insertion | **Partly redundant.** Defeats UDP source spoofing; Hivemind is TCP + libp2p TLS (`tls=True` default), so the address is already authenticated. Extending the existing `validate=True` ping to every insert is cheap but buys little alone. |
| IP limits (2 per /24 per bucket, 10 per table) | **Implementable, poor fit.** (a) `use_relay=True`/`use_auto_relay` mean the observable multiaddr is often a relay's; (b) legitimate volunteer swarms share /24s (university clusters, HPC allocations) — a strict limit self-partitions; (c) does not address cheap identities, since a cloud attacker spans subnets freely. |
| ENR: signed, sequence-numbered records | **Yes — already prototyped as D2** for averager declarations. Machinery exists (`dht/crypto.py`), already used by `ProgressTracker`. |

### IPFS / Kubo

**Layering caveat first:** Kubo's hardening targets **libp2p's Kad-DHT**, which is *not* the DHT where these
attacks land. Hivemind implements its own Kademlia in `hivemind/dht/` over libp2p streams; the libp2p DHT
(`dht_mode="server"`, `p2p_daemon.py:64-68`) is used only for peer routing. Kubo fixes are ideas to
reimplement, not code to adopt.

- **Provider-record / IPNS signing** → conceptually D2; implementation doesn't transfer.
- **Routing-table IP-diversity filters** (`RoutingTableFilter`, `PublicQueryFilter`) → same idea as Geth's IP
  limits, same objections.
- **Gossipsub peer scoring** → **not applicable**; Hivemind uses no pubsub.
- **Resource manager / connmgr** → `conn_manager=True` already default (via `go-libp2p-daemon`, which predates
  the resource manager). DoS hygiene, not Sybil defense.
- **Quorum on GetValue** (Kubo default 16 for IPNS) → **applicable in form, inert in effect.** It presumes an
  honest majority of replicas, which A1 shows is absent at 5 Sybils; and the highest-expiration-wins
  short-circuit (`node.py:838-845`) would have to be replaced first.

**Bottom line.** Exactly one measure transfers cleanly and with high value: Geth's unchoosable, pubkey-derived
node ID (= D1). Every quorum- or diversity-based control is downstream of it — without binding, they filter
identities that cost nothing to regenerate.

---

## Q3. What model does the research code train?

**No neural network.** A synthetic linear least-squares problem, `LeastSquaresTask`
(`research/hivemind/attacks/poison_averaging.py:32`), used only by Track C.

- **64 parameters** (`dim=64`) — a single vector, no layers, no nonlinearity.
- Ground-truth optimum drawn once: `optimum = torch.randn(64)` (line 53).
- **16 peers**, 128 rows each: `A_i` random, `b_i = A_i x* + noise`, `noise_std=0.01` (lines 55-61).
- Closed-form MSE gradient `2·Aᵀ(Ax − b)/rows` (line 66); metric is `‖x − x*‖` (line 75).
- 60 rounds, 3 seeds.

**Why not a real model** (docstring, line 33): because `x*` is known exactly, any displacement is attributable
to the attack rather than to training noise in a model you cannot inspect. Clean run converges to 0.002, so a
final distance of 4.37 is unambiguously the attacker's doing. Line 56 notes rows are unit-scaled so
`AᵀA/rows ≈ I`, keeping the problem well-conditioned.

Already recorded as limitation #4 in `FINDINGS.md`: **not** evidence about transformer training dynamics.

Tracks A, B, D and Tier-2 train nothing at all — they run real `p2pd` swarms and measure record capture,
`f_local`, and defense behaviour.

---

## Q4. Is a PeerID enough to reach a peer?

> **CORRECTION.** An earlier draft of `HIVEMIND_EXPLAINED.md` claimed a peer could "send bytes to any peer it
> knows the PeerID of." That was wrong and has been fixed.

**No. A PeerID is a name, not an address.** `stream_open` takes a PeerID only
(`p2p_daemon_bindings/control.py:370-375`):

```python
stream_open_req = p2pd_pb.StreamOpenRequest(peer=peer_id.to_bytes(), proto=list(protocols))
```

No addresses anywhere — so `p2pd` must resolve the name itself, from a **peerstore** filled by four routes:

1. **Explicit addresses** — `connect(peer_id, maddrs)` (`control.py:332`). How bootstrap peers are reached;
   requires no lookup, which is what avoids a chicken-and-egg problem at startup.
2. **The identify protocol** — connected peers exchange listen addresses automatically.
3. **libp2p's Kad-DHT** `FindPeer` — the general fallback, enabled by `dht_mode="server"`.
4. **Relay addresses** — `/ip4/<relay>/.../p2p-circuit/p2p/<target>`; routable because the relay is.

**Consequence for the layer above.** Hivemind's own DHT transmits **bare PeerIDs and no addresses** —
`proto/dht.proto:58` is `repeated bytes nearest_peer_ids`, reconstructed at `protocol.py:304`. When Layer 2
says "peer X is nearest to this key," it returns a *name* and assumes `p2pd` can resolve it. **Layer 2 depends
on Layer 1's peer routing rather than replacing it.**

| | libp2p Kad-DHT (in `p2pd`) | Hivemind DHT (Python) |
|---|---|---|
| Answers | "where is PeerID X?" | "who holds the value for key K?" |
| Stores | peer address records | Hivemind application records |
| Enabled by | `dht_mode="server"` | always — it *is* Hivemind |

---

## Q5. Why does peer *ordering* in a group matter?

Not for the arithmetic — averaging is commutative. **Ordering is a job assignment.**

`GroupInfo.peer_ids` is a `Tuple` (`group_info.py:13`); a peer's position is its index, looked up at
`allreduce.py:124`:

```python
peer_id_index = self.ordered_peer_ids.index(self.peer_id)
```

**Why an assignment is needed.** All-reduce is not "everyone sends everything to everyone" — that melts home
uplinks. The vector is cut into `group_size` slices and each peer becomes sole **reducer** for one slice. That
requires all members to independently agree on the *same* slice↔peer mapping; if two peers both think they own
slice 3, then slice 3 is averaged twice and slice 5 by nobody. The ordered list *is* that mapping, distributed
once inside `GroupInfo` — no negotiation round.

**The index is then used as an array subscript throughout:** `_input_parts_by_peer[assigned_peer_index]`
(`partition.py:57`), `accumulate_part(sender_index, ...)`, `sender_failed_after[sender_index]`,
`sender_peer_ids.index(peer_id)` when banning (`allreduce.py:321`).

**Ordering also carries the sizes.** `peer_fractions` is a parallel list aligned by position, computed from
bandwidth (`averager.py:525`) and turned into slice boundaries (`partition.py:69`):

```python
pivots = (np.cumsum(peer_fractions) / np.sum(peer_fractions) * self.total_size).astype(np.int64)
```

`GroupInfo.gathered` is aligned the same way. **Position is the join key** across who / how much / what
metadata.

**Analogy.** Eight people each hold a marked-up copy of the same 800-page manuscript. Rather than each reading
all eight copies (6,400 pages), person *i* merges pages *i*·100…(*i*+1)·100 from all eight copies and
circulates the result. Everyone gets the merged manuscript having read ~800 pages — but only if everyone
agreed in advance who takes which range. The assignment is arbitrary; it must be *shared*.

**Why shuffled** (`matchmaking.py:375-378`):

```python
group_id = DHTID.generate().to_bytes()  # note: both groupd_id and the order of peer_ids must be random
ordered_peer_ids = list(self.current_followers)
ordered_peer_ids.append(self.peer_id)
random.shuffle(ordered_peer_ids)
```

The index is a role, so it shouldn't be systematically inherited (the leader would always be index 0).
*Security note:* the shuffle is drawn by the **leader**, with the leader's own RNG. Nobody verifies it.

---

## Q6. How does an attacker target a node, and know which is valuable?

### Targets are derivable offline, not discovered

Every key name is structural and public:

| Key | Format | Source |
|---|---|---|
| Progress record | `f"{prefix}_progress"` | `progress_tracker.py:99` |
| Averaging rooms | `f"{prefix}.0b{bits}"` | `key_manager.py:44` |
| Gradient averager | `f"{run_id}_grad_averager"` | `optimizer.py:305` |
| State averager | `f"{run_id}_state_averager"` | `optimizer.py:286` |

`prefix` is the `run_id`, which **cannot** be secret — every participant needs it to join. See
[Q11](#q11-is-the-hash-mechanism-fixed-can-keys-be-enumerated).

### Targeting a specific *node*

Cheaper than routing-table eclipse: dominate the **candidate list** the victim reads. The victim calls
`get_averagers(group_key)` and averages with whoever it finds, so flooding that key with fabricated
declarations reaches `f_local` ≈ 0.97 from one process (Track B). You attack the victim's room, not the victim.

For persistent tracking, B2 applies: `predict_next_group_bits` reproduces `update_key_on_group_assembled`
144/144, because the shuffle is seeded on `group_id` and needs only `peer_ids.index(victim)` — both held by
every group member. One round in a group with the victim buys permanent tracking. *(Caveat: under default
config there is only one room, so tracking is moot — see [Q14](#q14-do-peers-switch-rooms-after-each-epoch).)*

### How the attacker ranks victims

The swarm publishes a contributor leaderboard. `LocalTrainingProgress` (`progress_tracker.py:31-37`), refreshed
continuously into the DHT for every peer:

```python
peer_id: bytes
epoch: conint(ge=0, strict=True)
samples_accumulated: conint(ge=0, strict=True)
samples_per_second: confloat(ge=0.0, strict=True)   # how strong this peer is
time: StrictFloat
client_mode: StrictBool                              # can it accept inbound connections?
```

One DHT read ranks every participant by throughput. `samples_per_second` identifies the biggest GPUs — whose
gradients carry the most weight and whose loss hurts most. `client_mode` flags peers accepting no inbound
connections. Combined with `get_averagers` (every PeerID in a room, with deadlines) and `GroupInfo` (full
membership of any group joined), an attacker can passively map the swarm before acting.

Note the irony: these records **are signed** (`progress_tracker.py:108`), so the leaderboard is *authentic*.
Signing protects integrity, not confidentiality.

---

## Q7. Can `node_id` really be set manually?

**Yes — confirmed at three levels.**

**Python API.** `DHT.__init__` stores `**kwargs` (`dht.py:70`) and forwards to `DHTNode.create`
(`dht.py:106-112`), landing at `node.py:171`:

```python
self.node_id = node_id if node_id is not None else DHTID.generate()
```

So `hivemind.DHT(node_id=DHTID(...), start=True)` works from the public API — no private access needed.

**Empirically.** `research/hivemind/common/multiprocess_swarm.py:55` passes crafted `node_id`s into peers running as
separate OS processes with real `p2pd` daemons; the swarm accepted them (Tier-2: capture 1.00, 5/5 returned
Sybil).

**On the wire — the part that matters.** Each node puts its `node_id` into every request (`protocol.py:77`) and
the receiver reads it straight off the message (`protocol.py:150`):

```python
sender_id = DHTID.from_bytes(request.peer.node_id)
sender_peer_id = context.remote_id     # this one IS cryptographically verified
```

Both values sit side by side — one TLS-authenticated, one simply believed. Nothing compares them.

---

## Q8. If picking `node_id` were disabled, would anything break?

Two answers pointing in opposite directions.

### Functionality: essentially nothing breaks

Grepping `hivemind/`, `tests/`, `benchmarks/`, `examples/` for callers passing `node_id=` to `DHTNode.create`
returns **none**. Every hit is `RoutingTable.get(node_id=...)`, a protobuf field, or research code. The
parameter is dead in-tree.

Three things checked that could have broken:

- **Test determinism** — no test relies on it, and `identity_path` (`p2p_daemon.py:94`) already gives a
  deterministic PeerID, hence a deterministic derived `node_id`.
- **Collisions** — `replicate_p2p` (`dht.py:320`) shares the daemon with *averagers and servicers*, not with a
  second `DHTNode`. One `DHTNode` per `p2pd`, so `H(PeerID)` is unique.
- **Research tooling** — the only real cost, and it's ours, not Hivemind's.

### Security: disabling it fixes nothing

`node_id` is not something Hivemind *grants* — it is something the peer **asserts in every packet**
(`protocol.py:77` → `protocol.py:150`). Deleting the kwarg constrains only peers running unmodified Hivemind,
i.e. honest ones. An attacker edits one line, or writes a small client speaking the same protobuf.

> **A restriction enforced by the claimant is not a security control. Only the verifier's check counts.**

So the root cause is not "the API lets you pick a `node_id`" but **"peers accept a self-reported `node_id`
without binding it to anything verified."** The parameter is a symptom.

The fix belongs at the receiver, which already holds the authenticated identity — roughly, at
`protocol.py:150`:

```python
sender_id = DHTID.from_bytes(request.peer.node_id)
if sender_id != DHTID.generate(source=context.remote_id.to_bytes()):
    return  # reject: claimed routing address doesn't match authenticated PeerID
```

That is D1, and it is why D1 is a *verifier-side* defense — the only kind that works.

**Cost caveat.** Binding does not make eclipse impossible, it makes it *priced*: the attacker grinds libp2p
keypairs until one hashes near the target (`num_replicas × 2^prefix_bits`). Cost goes from **zero** to ~2^b,
not to infinity. Closing that gap is what S/Kademlia's crypto puzzle is for.

---

## Q9. How do disjoint lookup paths help against Sybils?

### The failure they fix

Hivemind's lookup is one beam search with **one shared candidate heap and one shared visited set**
(`node.py:328`, `traverse.py`). That creates a sequential dependency: each step depends on the previous step's
answers. A single Sybil entering the candidate set steers everything after it — and because it claims a small
XOR distance it sits at the *head* of the heap and is expanded *preferentially*, returning more Sybils, which
are also nearest. One poisoned entry captures the search.

### What disjointness buys

S/Kademlia runs *d* searches under a hard invariant: **no node may appear in more than one path**, each path
seeded from a different bucket. Paths become statistically independent, so if one path is captured with
probability *p*, all *d* failing is ~*p^d*. Accept a result if **at least one** path returns an honest answer.
It converts a single point of failure into a vote.

### But it would not have changed the Track A results

Two distinct places a Sybil can sit:

| | Sybil **en route** | Sybil **at the destination** |
|---|---|---|
| What it does | intercepts the search and steers it | legitimately owns the address closest to the key |
| Disjoint paths help? | **yes** — other paths route around it | **no** — every path correctly converges to the same nodes |

Hivemind's eclipse is a *destination* attack. At 5 crafted Sybils the attacker owns all `num_replicas`
addresses nearest the key, so *d* disjoint paths faithfully find the same five attackers by five different
routes and unanimously agree. A0 (cost exactly `num_replicas`, zero variance, independent of N) is unaffected
by routing.

**Ordering: D1 first, disjoint paths second.** Binding stops destination ownership; disjoint paths then handle
the residual case where a grinding attacker owns *some* fraction and steers searches.

### A Hivemind-specific extra step

Even with *d* disjoint paths returning *d* values, Hivemind discards the benefit, because resolution is
highest-expiration-wins with early exit (`node.py:838-845`):

```python
elif candidate.expiration_time > (self.expiration_time or float("-inf")):
    self.binary_value = candidate.value
```

No cross-path comparison — one malicious path claiming a far-future expiration beats *d*−1 honest ones, and
`finish_search()` may fire before honest paths report. **Disjoint paths in Hivemind would require replacing the
resolution rule too** (quorum or cross-path agreement), otherwise you pay for *d* searches and still take the
attacker's answer. *Not currently in `FINDINGS.md`; relevant to its "Open question".*

---

## Q10. How do per-group averages become one model?

**There is no global aggregation step.** No layer above the group ever runs.

After all-reduce, each member holds *its group's* averaged gradient, loads it into the optimizer, runs Adam,
steps the scheduler, increments the epoch. Nobody reports upward. So after epoch 1 with 500 peers in ~60 groups
of 8, there are **~60 slightly different copies of the model**. The model has genuinely forked.

Consensus is reached **over time instead of over a tree** — three mechanisms:

**1. Reshuffling mixes the population.** Members scatter to new rooms after each group
(`key_manager.py:94`), so next epoch you average with different peers carrying traces of *their* previous
groups. Influence spreads exponentially, reaching everyone in ~log(N)/log(group_size) epochs. This is classical
**gossip averaging** (decentralized SGD). *(But see [Q14](#q14-do-peers-switch-rooms-after-each-epoch) — under
default config this mechanism is inert.)*

**2. Parameters are averaged too, not just gradients.** `hivemind.Optimizer` runs **two independent averagers
on two separate DHT prefixes**:

| Averager | Prefix | Averages | When |
|---|---|---|---|
| `GradientAverager` | `{run_id}_grad_averager` (`optimizer.py:305`) | gradients, before the optimizer step | every epoch |
| `TrainingStateAverager` | `{run_id}_state_averager` (`optimizer.py:286`) | **parameters + optimizer statistics** | every `average_state_every` epochs — **default 1** (`optimizer.py:187`) |

A typical epoch (`_update_global_epoch`, `optimizer.py:438-491`): average gradients with one group → optimizer
step → `state_averager.step(..., averaging_round=True)` averages the resulting *weights* with a
**differently-composed** group. That second round is the corrective. The docstring is explicit about the
trade-off (`optimizer.py:137-141`): raising `average_state_every` cuts bandwidth but lets parameters diverge.

**3. A hard resync for stragglers.** `_should_load_state_from_peers()` (`optimizer.py:655`) checks each epoch
whether this peer lags the swarm's `global_epoch`; if so it discards local progress and downloads parameters
wholesale (`optimizer.py:400-403`).

**The epoch counter is the only global thing.** `ProgressTracker` sums per-peer sample counts and triggers an
epoch at `target_batch_size`; `next_epoch = max(self.local_epoch + 1, self.tracker.global_epoch)`
(`optimizer.py:462`). The DHT coordinates *when* everyone averages; it never carries gradients or weights,
which move only peer-to-peer inside a group.

**Implications for the threat model** (not currently in `FINDINGS.md`):

- **Two averaging channels, not one.** Tracks B/C are framed around `grad_averager`. The state averager runs
  the same unsigned matchmaking on `{run_id}_state_averager` and averages *weights directly* — arguably a more
  direct poisoning target, with no optimizer between attacker input and parameters.
- **Gossip cuts both ways.** The same mixing that spreads honest updates spreads poisoned ones; damage does not
  stay local, and `load_state_from_peers` can pull poisoned weights wholesale.

---

## Q11. Is the hash mechanism fixed? Can keys be enumerated?

**Yes, fixed — and the attacker does not need to "try" anything.**

`DHTID.generate` (`routing.py:262-273`) is plain SHA-1 with **no salt, no per-network nonce, no keying**:

```python
HASH_FUNC = hashlib.sha1
...
source = MSGPackSerializer.dumps(source) if not isinstance(source, bytes) else source
raw_uid = cls.HASH_FUNC(source).digest()
return cls(int(raw_uid.hex(), 16))
```

Two consequences:

1. **Deterministic and network-independent.** The same key maps to the same DHTID in every swarm, forever. The
   attacker reproduces it offline with a one-line computation — `research/hivemind/common/sybil.py:40` does exactly this.
2. **No search is involved.** This is worth stating precisely, since "tries each one" implies brute force.
   Computing the target is a *direct evaluation*, one hash per key, not a search over a space:
   `target = SHA1(key)`, then `node_ids = [target ^ i for i in range(5)]` (`sybil.py:55-68`). Total work for a
   complete eclipse: **five XORs.** Nothing is guessed, and nothing scales with swarm size.

The only thing the attacker must *know* (not guess) is the key string, and the formats are fixed (see the table
in [Q6](#q6-how-does-an-attacker-target-a-node-and-know-which-is-valuable)) with `run_id` public by necessity.

**Where a keyed hash *would* help — and where it wouldn't.** Salting the hash with a run-specific secret
(`DHTID.generate(source=key, salt=run_secret)`) would stop *offline precomputation by outsiders*. It would not
stop a participant: anyone who joins the run learns the secret, and participation is open by design. It is a
speed bump against non-participants, not a fix. The fix remains binding (D1), which makes the *placement*
impossible regardless of whether the attacker can compute the target.

---

## Q12. Why do five identities give total custody?

Because **five is the entire replica set**, and the attacker can occupy positions no honest peer can reach.

**Step 1 — the replication factor is 5.** `num_replicas: int = 5` (`node.py:105`). `store` sends the record to
the `num_replicas` nearest node_ids and stops (`node.py:431-450`). There is no sixth copy anywhere.

**Step 2 — the attacker occupies distances 0,1,2,3,4.** `target ^ i` has XOR distance *exactly* `i`, so
iterating `i = 0..4` enumerates the five closest addresses that can exist (`sybil.py:55-68`):

```python
return [DHTID(int(target) ^ i) for i in range(offset, offset + count)]
```

**Step 3 — honest peers cannot compete.** Honest node_ids are uniform over 2^160. With *N* honest peers, the
nearest honest node_id to a given key sits at expected distance ~2^160/*N* — for *N* = 10,000 that is ~10⁴⁶.
The attacker sits at distance 0–4. The comparison is not close, and it is not probabilistic.

**Step 4 — therefore capture is deterministic.** Any correct Kademlia implementation, asked for the 5 nearest,
returns the attacker's 5. This is A0: cost **exactly** `num_replicas`, standard deviation **0.00**, unchanged
from 50 to 10,000 honest peers. Growing the swarm makes honest peers *further* from the key, never closer.

**Why "scale doesn't help" is the counterintuitive part.** In most security settings a bigger network dilutes
an attacker. Here the attacker's position is absolute, not relative — distance 0 is distance 0 regardless of
who else exists.

### The corrected key-count claim

> **CORRECTION.** Earlier in this analysis I said group keys are "16 targets that each affect ~1/16 of the
> swarm." That is wrong under default configuration.

`initial_group_bits: str = ""` (`averager.py:120`), and `hivemind.Optimizer` never overrides it — the only
in-tree callers that set it are `benchmarks/benchmark_averaging.py:65` and `tests/test_averaging.py`. With
empty bits, `current_key` is `f"{prefix}.0b"` — **one key for the whole run** (verified: `'run42.0b'`, and
`is_valid_group` returns `True`, since `GROUP_PATTERN` allows `[01]*`).

So the accurate statement is:

| configuration | number of averaging keys | identities for total custody of all of them |
|---|---|---|
| **default** (`initial_group_bits=""`) | **1** per averager | **5** per averager (grad + state = 10 total) |
| operator sets *b* bits | 2^*b* | 5 × 2^*b* |

The default is therefore **more** exposed than the 16-room figure suggested, not less: there is a single
averaging key per averager, and five identities own it outright.

---

## Q13. How are groups formed — is it random?

**No. Group composition is emergent from timing, not sampled.** The only randomness is *within* an assembled
group.

The sequence:

1. **Declare.** Each ready peer writes to the room key with a self-chosen deadline
   (`key_manager.py:46-67`), subkey = its own PeerID.
2. **Read.** Everyone reads the same room and sees the same list of (PeerID, deadline) pairs
   (`key_manager.py:69-92`).
3. **Elect deterministically.** Sort by `(expiration_time, peer_id)` and follow the smallest
   (`matchmaking.py:479-497`). Everyone computes the same answer with no messages exchanged. The PeerID breaks
   ties.
4. **Accept in arrival order.** The leader takes followers first-come-first-served
   (`rpc_join_group`, `matchmaking.py:262-280`): `self.current_followers[context.remote_id] = request`, then
   assembles once `len(current_followers) + 1 >= target_group_size` — or, on timeout, once `min_group_size` is
   met (`matchmaking.py:295-297`).
5. **Randomize only the internal ordering** (`matchmaking.py:375-378`): fresh random `group_id`, then
   `random.shuffle(ordered_peer_ids)`.

So **who ends up in your group is decided by who declared an early deadline and who connected to the leader
first** — a race, not a draw. Rejection reasons (`matchmaking.py:355-368`) are structural only: wrong group
key, not declared, bad expiration, not-a-leader, duplicate peer, group full.

**Security-relevant consequences:**

- Both selection criteria — the declared deadline and connection speed — are **fully attacker-controlled**. An
  attacker declares the earliest deadline to become leader, or connects fastest to fill a victim's group.
- A leader chooses its own followers. Nothing requires the choice to be representative.
- `target_group_size` defaults to `None` (`averager.py:118`) and `hivemind.Optimizer` never sets it, so in the
  default path groups assemble on the `min_group_size` + timeout branch rather than filling to a target.

---

## Q14. Do peers switch rooms after each epoch?

> **CORRECTION.** Earlier I said peers "scatter to new rooms" every epoch. **Under default configuration they
> never switch rooms at all.**

The reshuffle line (`key_manager.py:102`):

```python
self.group_bits = (self.group_bits + new_bits)[-len(self.group_bits) :] if self.group_bits else ""
```

If `group_bits` is `""` it is falsy, so the `else` branch returns `""` — the newly computed `new_bits` are
**discarded**. Verified by executing the real code path:

```
DEFAULT initial_group_bits = ""  (averager.py:120)
  current_key    : 'run42.0b' | valid group key: True
  after epoch 0: bits='' key='run42.0b'
  after epoch 1: bits='' key='run42.0b'
  after epoch 2: bits='' key='run42.0b'
  after epoch 3: bits='' key='run42.0b'

CONTRAST: operator sets initial_group_bits="0000"
  after epoch 0: bits='0011' key='run42.0b0011'
  after epoch 1: bits='1011' key='run42.0b1011'
  after epoch 2: bits='1011' key='run42.0b1011'
```

So:

| configuration | rooms | switching |
|---|---|---|
| **default** (Optimizer, Petals-style usage) | 1 | **never** |
| `initial_group_bits` of length *b* | 2^*b* | on **every group assembled** (not per epoch — `update_key_on_group_assembled` is called from `leader_assemble_group`, `matchmaking.py:387`) |

Note the mechanism is also *bit-shifting*, not resampling: new bits are appended and the string truncated to
its original length, so the room index is a sliding window of recent group placements.

**Why this matters for the research.** Track B2 studied re-bucketing with 16 buckets and found a tracking
attacker suffers no decay (`f_local` 0.891 → 0.891). That result stands as an analysis of the *mechanism*, and
`predict_next_group_bits` matching 144/144 is still correct. But under default configuration **there is no
re-bucketing to defeat** — every peer sits in one room permanently. B2's conclusion ("re-formation sheds only
an attacker who declines to use public information") should therefore be read as *the best case Hivemind offers
when the feature is switched on*; the default is strictly weaker. This makes the attack easier than B2 assumes,
not harder.

---

## Q15. Has anyone published on this attack?

**No.** No paper directly analyzes Hivemind's DHT security, and none analyzes attacker-chosen `node_id` as a
route to custody of specific DHT keys. This was verified by searching Hivemind/Petals-specific terms and
sweeping the citation graphs of the four Ryabinin-lab papers (~600 citing works) filtered on security keywords.
Every security-flavoured citer sits at the **compute** layer (gradient/activation poisoning, backdoors, prompt
inversion); none at the **control** layer.

More useful than the absence: **the Hivemind authors state the assumption and defer the analysis, in writing.**

### The gap, in the authors' own words

**Ryabinin & Gusev, "Towards Crowdsourced Training of Large Neural Networks using Decentralized Mixture-of-Experts", NeurIPS 2020** — <https://arxiv.org/abs/2002.04013>
- §"DHT Node": *"For simplicity, we use unmodified Kademlia protocol, leaving further investigation to future work."*
- Broader impact, §"Security": names *"Sybil attacks, Eclipse attacks"* explicitly, then refers the reader to a
  DHT-security survey rather than adopting a mitigation.
- Appendix: participants' IDs are *"sampled uniformly from the space of possible outputs of the hash function"*
  — stated as an **assumption**, not an enforced property.

**Gorbunov, Borzunov, Diskin, Ryabinin, "Secure Distributed Training at Scale" (BTARD), ICML 2022** —
<https://arxiv.org/abs/2106.11257> · [PMLR PDF](https://proceedings.mlr.press/v162/gorbunov22a/gorbunov22a.pdf)
The most directly relevant paper, and it does **not** cover this attack:
- Threat model §3.1 assumes Byzantine set *B* is **fixed**, |B| ≤ δn with δ < ½, and peers declare public keys
  and sign messages. Fixed membership, authenticated identities, honest majority — all assumed.
- §3.3 / Appendix F Sybil defense is a **compute-proportionality heuristic** (prove honest gradient computation
  before contributing). Bounds an attacker's *voting weight*; says nothing about *where in the keyspace* an
  identity sits.
- Appendix G restates the uniform-ID assumption verbatim and defers DHT hardening to the same survey. Its own
  DHT defenses are signatures on stored values and expiration binding — which defeat **forgery and rollback**,
  not **custody**. A peer that has "legitimately" chosen a node_id near `hash(key)` is the authorized storer and
  can withhold or selectively serve without forging anything.

**Diskin et al., DeDLOC, NeurIPS 2021** — <https://arxiv.org/abs/2106.10207> — security content is an
allowlist (Appendix I.5) plus "backbone" peers for DHT availability. Both orthogonal: an allowlist gates *who*
joins, not *which node_id* an admitted peer picks.

**Petals** — [ACL 2023](https://arxiv.org/abs/2209.01188) and
[NeurIPS 2023](https://arxiv.org/abs/2312.08361) — have explicit Security/Privacy sections, both compute-layer
(faulty servers returning wrong outputs; token recovery). The DHT appears only as infrastructure. **No DHT
threat model.**

### The correct structural analogues (adjacent systems)

| Work | Venue | Relevance |
|---|---|---|
| Sridhar et al., **Content Censorship in the InterPlanetary File System** — [NDSS 2024](https://www.ndss-symposium.org/ndss-paper/content-censorship-in-the-interplanetary-file-system/) · [arXiv](https://arxiv.org/abs/2307.12212) | NDSS 2024 | **Closest analogue.** Places Sybils so they *"replace honest resolvers for a given CID"*, making content unretrievable. Map CID → Hivemind DHT key and it is this attack, one system over. Includes detection (99.6%) and mitigations. |
| Prünster, Marsalek, Zefferer, **Total Eclipse of the Heart — Disrupting IPFS** — [USENIX Sec 2022](https://www.usenix.org/conference/usenixsecurity22/presentation/prunster) | USENIX Sec 2022 | Node-level eclipse (isolate a victim peer), CVE-2020-10937. Complementary to the key-custody half. |
| Henningsen et al., **Eclipsing Ethereum Peers with False Friends** — [arXiv](https://arxiv.org/abs/1908.10141) | EuroS&PW 2019 | **The same mechanism:** precomputed NodeIDs guarantee a victim's lookups return only Sybils. 45/50 eclipses in 24 h from two hosts. Fixed in **Geth v1.9.0** — the origin of the hardening compared in [Q2](#q2-do-geth-v190--ipfskubo-hardening-apply-here). |
| Marcus, Heilman, Goldberg, **Low-Resource Eclipse Attacks on Ethereum** — [ePrint 2018/236](https://eprint.iacr.org/2018/236) | 2018 | Eclipse from two hosts, one IP each. Establishes the low-resource-attacker framing. |
| Heilman et al., **Eclipse Attacks on Bitcoin's P2P Network** — [USENIX Sec 2015](https://www.usenix.org/conference/usenixsecurity15/technical-sessions/presentation/heilman) | USENIX Sec 2015 | Canonical eclipse paper. Not Kademlia — cite for the concept and the analysis + live-measurement methodology. |
| Baumgart & Mies, **S/Kademlia** — [PDF](https://telematics.tm.kit.edu/publications/Files/267/SKademlia_2007.pdf) | ICPADS 2007 | **The defense citation.** Disjoint paths, **crypto puzzles limiting free nodeId generation**, sibling broadcast. The middle one is the exact countermeasure; Hivemind's own paper points at "variations of the DHT protocol" and this is that variation. |
| Douceur, **The Sybil Attack** | IPTPS 2002 | The impossibility result underpinning all of it. |

Also useful rhetorically: **Urdaneta, Pierre, van Steen, "A Survey of DHT Security Techniques"** (ACM CSUR
2011) — *literally the reference both Hivemind papers hand the reader in lieu of doing the analysis*.

### Looks relevant, is not

- **Cihangiroglu & Nocera**, *Integrity of P2P distributed LLM inference under malicious nodes*
  ([arXiv:2607.19490](https://arxiv.org/abs/2607.19490)) — motivated by Petals, so it *sounds* like this work.
  It is canary inputs + activation-drift detection. Compute-layer; no DHT/Sybil/eclipse/node_id content.
- **Ersoy et al.**, *Backdoor Attacks on Decentralised Post-Training*
  ([arXiv:2604.02372](https://arxiv.org/abs/2604.02372)) — assumes the adversary already holds a pipeline
  stage, i.e. *presupposes* the placement this attack achieves.
- **SybilWall** ([arXiv:2306.15044](https://arxiv.org/abs/2306.15044)) — "Sybil" in the title, but it is a
  Sybil-resistant *aggregation function* over an assumed gossip topology. No DHT / node-ID / eclipse content.
- **BALANCE** ([arXiv:2406.10416](https://arxiv.org/abs/2406.10416)) and the ClippedGossip line — assume a
  **fixed communication graph** with exogenous membership. They defend bad *values on existing edges*; this
  attack manipulates *which edges exist* and *who holds which key*. Explicitly out of scope.

### Suggested framing

The literature bifurcates and leaves a hole in the middle. Byzantine-ML work (BTARD and descendants) secures
the *values* under assumed-honest, assumed-fixed, assumed-uniformly-distributed membership. P2P security work
(S/Kademlia, the Ethereum/IPFS eclipse line) secures *membership and routing* but has never been applied to a
collaborative-training DHT. Hivemind sits at the intersection and inherits the unverified assumption from both
sides — provable with two quotes: *"we use unmodified Kademlia protocol, leaving further investigation to
future work"* and BTARD Appendix G's *"each participant samples a unique binary identifier (ID) sampled
uniformly."* In the deployed library, the attacker chooses it. That one word — "samples" — is the gap.

---

## Corrections index

Statements made earlier in this analysis that were revised, kept here so nothing silently changes:

| # | Original claim | Correction | Where |
|---|---|---|---|
| 1 | "A peer can send bytes to any peer it knows the PeerID of." | A PeerID is a name, not an address; it must be resolved via peerstore / identify / libp2p Kad-DHT / relay. Hivemind's DHT carries bare PeerIDs (`dht.proto:58`), so Layer 2 depends on Layer 1 peer routing. | [Q4](#q4-is-a-peerid-enough-to-reach-a-peer) |
| 2 | "Group keys are 16 targets, each affecting ~1/16 of the swarm." | Default `initial_group_bits=""` ⇒ **one** averaging key per averager (`run42.0b`). 2^*b* only if an operator sets *b* bits. | [Q12](#q12-why-do-five-identities-give-total-custody) |
| 3 | "Peers scatter to new rooms every epoch." | Under default config peers **never** switch rooms — the reshuffle line discards the new bits when `group_bits` is empty. Verified by execution. | [Q14](#q14-do-peers-switch-rooms-after-each-epoch) |

### Items not yet in `FINDINGS.md`

Surfaced here, worth folding in if they survive scrutiny:

1. **Disjoint paths are inert without replacing highest-expiration-wins** ([Q9](#q9-how-do-disjoint-lookup-paths-help-against-sybils)).
2. **Removing the `node_id` parameter is functionally free but security-irrelevant** ([Q8](#q8-if-picking-node_id-were-disabled-would-anything-break)).
3. **Two averaging channels exist**, and the state averager poisons weights directly ([Q10](#q10-how-do-per-group-averages-become-one-model)).
4. **Default config has no re-bucketing**, making B2's premise the best case rather than the norm ([Q14](#q14-do-peers-switch-rooms-after-each-epoch)).
5. **No prior work analyzes this**, and the authors' own papers state the assumption explicitly ([Q15](#q15-has-anyone-published-on-this-attack)).

---

*Documentation only; part of the additive `research/hivemind/` tree. No core Hivemind code was modified.*
