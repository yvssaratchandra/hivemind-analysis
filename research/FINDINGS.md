# Hivemind's control plane is unguarded

A self-contained summary of what this project found. Written to be read without any prior
context. Detail, method and every recorded run are in `HANDOFF.md`; this file is the argument.

Also published as a formatted page:
<https://claude.ai/code/artifact/2615f5b9-21ae-439d-ba7d-0746eb9ab2a7>

> Byzantine-robust aggregation secures *what* peers send. In Hivemind, an attacker decides
> *who averages with whom* — and that decision is made by code nobody has examined.

System: hivemind 1.2.0.dev0 · Method: live swarms of 8–104 nodes · Core Hivemind never modified.

---

## 1. Background: how Hivemind decides who trains together

Hivemind trains one model across volunteer machines with no coordinating server. Peers
accumulate gradients, then periodically average them with a small **group** of other peers.
Groups assemble themselves, every round, out of whoever is available.

The system stacks five layers. Robust-aggregation research lives almost entirely in the top
one; the machinery that decides group membership sits two layers below it.

| layer | | plane |
|---|---|---|
| 5 | **Optimizer** — accumulate, average, apply | data |
| 4 | **All-reduce** — reduce-scatter over the assembled group | data |
| 3 | **Matchmaking** — advertise, elect a leader, admit members | **control** |
| 2 | **Kademlia DHT** — who stores which record, who answers a lookup | **control** |
| 1 | **libp2p transport** — authenticated connections | sound |

### Two identities, one of them unverified

Every peer carries a libp2p **PeerID** — a hash of its public key, TLS-authenticated,
unforgeable. It also carries a Kademlia **`node_id`**, which decides *which DHT records it
stores*. The `node_id` is chosen by the peer itself and accepted verbatim by everyone else.
**Record custody is settled by the identity nobody checks.**

### Group formation is a race, not a draw

A peer ready to average writes an advertisement to a DHT key carrying a **deadline it picks
itself**. Every peer reads the same key, sorts candidates by `(deadline, peer_id)`, and
contacts the earliest — or, if its own deadline is earliest, waits to be contacted. That peer
is the **leader**, and it decides which requests to admit, running that check on its own
machine with each requester's authenticated PeerID in hand.

Nothing in that sequence is sampled. Membership is the outcome of a deadline race and an
admission decision, both made by parties who may be adversarial.

---

## 2. Where the control plane is decided

Six points where an attacker's input is taken on trust. They compose: custody enables lookup
control, which feeds the election, which decides admission.

| # | surface | source |
|---|---|---|
| 01 | **Record custody** — which peers store the advertisement key, by XOR distance from a self-chosen `node_id` | `dht/node.py:171`, `dht/protocol.py:150` |
| 02 | **Lookup seeding** — a lookup asks the `bucket_size` peers nearest the key; whoever holds those slots decides what the reader sees | `dht/node.py:629-633` |
| 03 | **Advertisement records** — unsigned, unverified PeerID as subkey, no validator anywhere in the averaging module | `averaging/key_manager.py:46-67` |
| 04 | **Leader election** — won by the earliest self-declared deadline; nothing validates the claim | `averaging/matchmaking.py:478-481` |
| 05 | **Admission** — the leader accepts or rejects each requester, on its own machine, by identity | `averaging/matchmaking.py:341-368` |
| 06 | **Re-formation** — the reshuffle meant to mix peers is seeded on a value every group member holds | `averaging/key_manager.py:94-104` |

---

## 3. Findings

Write **α** for the attacker's share of the whole swarm and **β** for its share of one
averaging group. The Byzantine-ML literature treats β as roughly α, because membership is
assumed to be a fresh random draw.

### 3.1 β is chosen, not inherited

When the attacker wins the election it admits exactly whom it likes. Measured β is **k/(k+1)**
with no variance — 0.500, 0.667, 0.800 for one, two and four attacker peers. A single identity
beside the victim is half the group: α = 0.062 becomes a mean β of 0.315, an **8×
amplification**.

### 3.2 Control costs a fixed number of identities, whatever the network size

| capability | parameter | cost | grows with swarm? |
|---|---|---|---|
| store a given record | `num_replicas` | **5 identities** | no |
| decide what a lookup returns | `bucket_size` | **20 identities** | no |

Both thresholds are the victim's own DHT parameters, not properties of the network. Identities
cost ~0.1 s each to mint.

Custody alone is not control: five identities hold the record, but a reader asks twenty peers
and the fifteen honest answers are merged back in. Confirmed over a 28-run campaign — capture
steps from 14 % at 18 identities to **100 % at exactly 20**, and doubling `bucket_size` to 40
makes 25 identities fail where 45 succeed. Padding the swarm to 104 nodes changes nothing.

### 3.3 The mechanism meant to dilute exposure does not

Hivemind can reshuffle peers into new groups between rounds. The shuffle is seeded on the group
identifier and indexed by each member's position — both handed to every member. A peer that
shares **one** group with the victim can compute where the victim goes next, forever. Measured:
the tracking attacker predicted the victim's next bucket **5 of 5** times and was in its group
in 5 of 7 rounds, against 1 of 5 for an attacker that does not track.

### 3.4 A quarter of a group moves the model 35–148× off target

With β = ¼ a sign-flipped gradient alone is indistinguishable from noise — the mean dilutes it.
Combined with the sender-supplied weight, applied verbatim, the same gradient moves the model
**35–148×** further from the optimum across three seeds. Coordinate-wise median recovers most
of it at zero extra communication, because it ignores the declared weight by construction.

### 3.5 The attacker chooses the denominator

Published guarantees are conditional on group size: Krum needs `2f + 2 < n`, coordinate median
needs `q < n/2`. Both treat *n* as given. Here the attacker is the leader, closes the group when
it likes, and the minimum group size is two.

| group size *n* | max β for Krum | max β for median | β achieved |
|---|---|---|---|
| 2 (the minimum) | 0 | 0 | **0.500** |
| 4 | 0 | 0.25 | **0.667** |
| 8 | 0.25 | 0.375 | **0.800** |

These are *sufficient* conditions — exceeding one removes the guarantee, it does not prove the
rule fails. But no aggregation rule can help when the adversary picks the denominator before a
single gradient is crafted.

---

## 4. Limitations

- **Scale.** Every swarm is 8–104 nodes on one machine. This has already misled once: a
  mechanism that looked confirmed at 14 nodes was falsified at 104, and the real explanation was
  different.
- **The model is a toy.** Linear regression over 64 parameters, chosen so displacement is
  attributable rather than arguable. Not evidence about transformer training dynamics.
- **Whether membership mixes in practice is unsettled.** At 16 peers the swarm froze into stable
  cliques far outside a random-partition null; at 24 peers, same code, it was indistinguishable
  from a fair draw. The difference tracked machine load, not protocol.
- **No defence has been built or evaluated.** Everything here is measurement of attacks.

---

## 5. Future work

**Build the two defences already identified.** Both are implementable against the existing
harness, so the attacks can be measured failing. Hivemind already signs one class of DHT record
and not the other — the machinery exists and is simply not applied to averaging keys. And
seeding the re-formation shuffle on a value contributed by members and revealed only after
placement would break the tracking attacker in §3.3.

The honest result of doing both is the interesting one: **the cheap bugs have cheap fixes, and
the structural problem does not.** Signing records does nothing about an attacker who wins the
election and admits whom it pleases.

**Settle the scale question.** A swarm large enough that routing actually routes, rather than
every peer knowing every other. This is the first question a reviewer will ask, and the one
limitation above that could overturn a result rather than merely narrow it.

**Measure interception latency.** Everything here measures settled capture. Nobody has measured
the interval between an attacker arriving and control taking effect — the only window in which a
defender could act.

**Establish what is actually new.** Adversarial peer sampling in decentralized learning is an
active area with both attacks and defences published (GRANITE, Honeybee, and the
Raynal/Pasquini/Troncoso line). What appears untouched is *election-and-admission* group
assembly, as opposed to random peer sampling over a graph — a different protocol shape with a
different attack surface. That distinction needs verifying before any novelty claim rests on it.
