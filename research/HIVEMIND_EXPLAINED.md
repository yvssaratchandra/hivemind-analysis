# How Hivemind Actually Works — A Beginner's Walkthrough

**Who this is for.** Someone who knows a little about networks and a little about machine learning, but has
never read the Hivemind source. No prior knowledge of DHTs, Kademlia, libp2p, or distributed training is
assumed. Every technical term is defined the first time it appears.

**What this is.** A step-by-step description of what actually happens, in code, when volunteers train a neural
network together with Hivemind. Claims are pinned to real files and line numbers (`file.py:123`) so you can
open the source and check.

**Scope.** This document describes how the system *works*. It deliberately says little about how it can be
*attacked*; that is [`FINDINGS.md`](FINDINGS.md). But because the design is easier to understand when you know
which parts are trusted and which are verified, §9 gives a short honest summary of where trust sits.

**Version.** Written against Hivemind 1.2.0.dev0 (fork point `4bd43b7`), branch `research/net-security`.

---

## Table of contents

1. [The problem Hivemind solves](#1-the-problem-hivemind-solves)
2. [The five layers, in one picture](#2-the-five-layers-in-one-picture)
3. [Layer 1 — Transport: getting bytes from A to B](#3-layer-1--transport-getting-bytes-from-a-to-b)
4. [Layer 2 — The DHT: the shared noticeboard](#4-layer-2--the-dht-the-shared-noticeboard)
5. [Layer 3 — Matchmaking: finding people to average with](#5-layer-3--matchmaking-finding-people-to-average-with)
6. [Layer 4 — All-reduce: actually averaging the numbers](#6-layer-4--all-reduce-actually-averaging-the-numbers)
7. [Layer 5 — The Optimizer: gluing it to PyTorch](#7-layer-5--the-optimizer-gluing-it-to-pytorch)
8. [End-to-end: one training step, start to finish](#8-end-to-end-one-training-step-start-to-finish)
9. [Where trust sits](#9-where-trust-sits)
10. [Glossary](#10-glossary)
11. [Map of the codebase](#11-map-of-the-codebase)

---

## 1. The problem Hivemind solves

Training a large neural network normally happens in one datacenter: hundreds of GPUs on the same fast local
network, managed by one owner, all machines reliable and trusted.

Hivemind is for the opposite situation. **Volunteers** across the internet — different countries, home
internet connections, laptops that get closed mid-training — want to train one model together. That changes
four things:

| | Datacenter training | Hivemind |
|---|---|---|
| **Who is in charge** | one central coordinator | nobody; peers self-organise |
| **Who the participants are** | a fixed, known list | anyone, joining and leaving constantly |
| **The network** | fast, reliable, private | slow, lossy, behind firewalls |
| **Failure** | rare, and a big deal | constant, and must be routine |

So Hivemind's job is: *let a changing crowd of untrusted-ish machines cooperatively compute the same thing a
datacenter would compute, without any central server.*

### The one ML idea you need

Neural network training is a loop:

1. Take a batch of training examples.
2. Run them through the model, compare with the right answers, get a **loss** (a number: how wrong we are).
3. Compute the **gradient** — a big list of numbers, one per model parameter, saying "nudge this parameter
   this much to reduce the loss". A model with 100 million parameters has a 100-million-number gradient.
4. Nudge all the parameters accordingly. That is one **optimizer step**.

The crucial fact for distributed training: **gradients from different batches can be averaged.** If I compute
a gradient on my 8 examples and you compute one on your 8 examples, the average of our two gradients is
(essentially) the gradient we'd get from a single batch of 16. This is why the whole thing is possible: peers
work on different data, then average.

So the distributed-training problem reduces to one repeated operation: **every peer has a big vector of
numbers; compute the average of everyone's vector and give the result back to everyone.** That operation is
called an **all-reduce**, and Layers 3–4 of Hivemind exist entirely to make it happen among strangers.

---

## 2. The five layers, in one picture

```
┌──────────────────────────────────────────────────────────────────────┐
│  Layer 5  hivemind.Optimizer          "when should we average?"      │
│           drop-in replacement for torch.optim; counts batches        │
├──────────────────────────────────────────────────────────────────────┤
│  Layer 4  AllReduceRunner             "average these numbers"        │
│           splits vectors into parts, sums them, hands back results   │
├──────────────────────────────────────────────────────────────────────┤
│  Layer 3  Matchmaking                 "who do I average with?"       │
│           finds ~8-16 peers ready at the same moment, forms a group  │
├──────────────────────────────────────────────────────────────────────┤
│  Layer 2  DHT (Kademlia)              "the shared noticeboard"       │
│           a key→value store spread across all peers, no server       │
├──────────────────────────────────────────────────────────────────────┤
│  Layer 1  libp2p / p2pd               "get these bytes to that peer" │
│           connections, encryption, firewall traversal                │
└──────────────────────────────────────────────────────────────────────┘
```

Read it bottom-up: each layer only knows how to talk to the one below it. Layer 3 uses Layer 2 as a
noticeboard to find partners; Layer 4 then talks to those partners directly over Layer 1.

---

## 3. Layer 1 — Transport: getting bytes from A to B

### The problem

Two home computers usually **cannot** just connect to each other. Both sit behind a **NAT** (Network Address
Translation — your home router shares one public IP address among all your devices) and a firewall. Incoming
connections get dropped. Normally we solve this with a central server that both sides connect *out* to — but
Hivemind has no central server.

### The solution: libp2p

Hivemind does not write its own networking. It uses **libp2p**, a networking library originally built for
IPFS (a peer-to-peer file system). libp2p handles encryption, NAT traversal, and connection management.

libp2p is written in Go, and Hivemind is written in Python. So Hivemind **launches a separate Go program as a
child process** and talks to it over a local Unix socket. That program is `p2pd` (the "libp2p daemon").

> **Daemon** = a background program with no user interface that provides a service to other programs.

Every Hivemind peer therefore runs at least two processes: your Python training script, and a `p2pd` child.
You can see the launch in `hivemind/p2p/p2p_daemon.py:205-220` — it builds a command line and runs it:

```python
proc_args = self._make_process_args(
    str(p2pd_path),
    autoRelay=use_auto_relay,  autonat=auto_nat,   b=need_bootstrap,
    connManager=conn_manager,  idleTimeout=f"{idle_timeout}s",
    natPortMap=nat_port_map,   relay=use_relay,    tls=tls,
    ...
)
self._child = await asyncio.subprocess.create_subprocess_exec(*proc_args, ...)
```

### PeerID: your cryptographic name

When `p2pd` starts, it generates a **keypair**.

> **Public-key cryptography, in one paragraph.** You generate two matched keys. The **private key** you keep
> secret; the **public key** you hand out freely. Anything signed with the private key can be *verified* by
> anyone holding the public key, and only the holder of the private key could have produced that signature.
> This lets you prove "I am the same entity that owns this public key" without ever revealing the secret.

Your **PeerID** is derived from your public key by hashing it.

> **Hash function** = a one-way function turning any input into a fixed-size fingerprint. Same input → same
> fingerprint, always. Different input → different fingerprint, essentially always. You cannot run it
> backwards to recover the input.

So `PeerID = multihash(sha256(public_key))` (`hivemind/p2p/p2p_daemon_bindings/datastructures.py:18`). This is
important and worth stating clearly:

> **A PeerID is unforgeable.** You cannot claim someone else's PeerID, because to complete an encrypted
> connection you must prove you hold the matching private key. When Hivemind receives a message and reads
> `context.remote_id`, that PeerID is *cryptographically verified*, not merely claimed.

Remember this. In §9 it turns out that Hivemind has a *second* identity that is **not** verified, and the
difference between the two is the single most important thing in this document.

### Multiaddrs, NAT traversal, and relays

An address in libp2p is a **multiaddr** — a self-describing path, e.g.
`/ip4/203.0.113.7/tcp/31337/p2p/QmYyQSo1c...`, read left to right: "over IPv4, at this address, TCP port 31337,
the peer with this PeerID". Being self-describing means new transports can be added without changing the
format.

Defaults in `p2p_daemon.py:84-110` that matter:

- `tls=True` — all connections are encrypted and mutually authenticated. **On by default.**
- `auto_nat=True` — the peer asks others "can you reach me?" to learn whether it is publicly reachable.
- `nat_port_map=True` — try to ask the home router to open a port automatically (UPnP/NAT-PMP).
- `use_relay=True` — if a direct connection is impossible, traffic can be **relayed** through a third peer
  that both sides *can* reach. Like a mutual friend passing notes between two people who can't meet.
- `conn_manager=True` and `idle_timeout=30` — cap and prune connections so a peer doesn't drown in sockets.
- `dht_mode="server"` — libp2p has its own DHT, used for **peer routing** (finding *where a peer is*). This is
  a completely separate DHT from Hivemind's, and it matters — see the next subsection.

### PeerID is a name, not an address

A common confusion, and an important one. Knowing a peer's PeerID does **not** mean you can reach it. The
PeerID is derived from a public key and says nothing about where in the world that peer is. To open a
connection you need a **multiaddr** — an actual route. Turning the first into the second is called **peer
routing** or **address resolution**.

You can see the gap in the code. `stream_open` (`p2p_daemon_bindings/control.py:370-375`) asks to open a
connection given only a PeerID:

```python
stream_open_req = p2pd_pb.StreamOpenRequest(peer=peer_id.to_bytes(), proto=list(protocols))
```

No addresses anywhere. So `p2pd` must already know, or be able to find out, where that peer lives. It draws on
a **peerstore** — its local cache of "PeerID → known addresses" — filled from four sources:

1. **Explicit addresses.** `connect(peer_id, maddrs)` (`control.py:332`) is the version that *does* take
   addresses. This is how bootstrap peers are reached: you were handed their full multiaddrs up front.
2. **The identify protocol.** Whenever two libp2p peers connect, they exchange basic facts about each other,
   including the addresses they listen on and the address each *observes* the other coming from. So merely
   talking to a peer teaches you how to reach it again — and helps it discover its own public address.
3. **libp2p's Kad-DHT.** The general mechanism: a `FindPeer` query walks that DHT to locate the current
   address record for an arbitrary PeerID. This is what `dht_mode="server"` turns on.
4. **Relay addresses.** For an unreachable peer, the address is of the form
   `/ip4/<relay>/tcp/<port>/p2p/<relay_id>/p2p-circuit/p2p/<target_id>` — literally "reach the target *via*
   this relay". Routable, because the relay is.

**So you were right to push back:** without some peer-routing mechanism, a bare PeerID is not dialable. In
practice paths 1 and 2 cover most of what a Hivemind peer needs — you bootstrap to known addresses, and
addresses propagate as connections form — with the libp2p DHT as the general fallback.

This has a consequence for the layer above. Hivemind's own DHT (Layer 2) passes around **bare PeerIDs and no
addresses at all** — `hivemind/proto/dht.proto:58` is literally
`repeated bytes nearest_peer_ids;  // libp2p PeerIDs of the nearest peers`, and `protocol.py:304` reconstructs
them as `map(PeerID, result.nearest_peer_ids)`. When Hivemind's DHT says "peer X is near this key", it is
handing you a *name* and assuming `p2pd` can find the address. **Layer 2 is built on top of Layer 1's peer
routing, not a replacement for it.** The two DHTs do different jobs:

| | libp2p Kad-DHT (Layer 1, inside `p2pd`) | Hivemind DHT (Layer 2, in Python) |
|---|---|---|
| Answers | "where is PeerID X?" | "who holds the value for key K?" |
| Stores | peer address records | Hivemind's application records |
| Written in | Go, part of `p2pd` | Python, `hivemind/dht/` |
| Turned on by | `dht_mode="server"` | always, it *is* Hivemind |

### Bootstrapping

A brand-new peer knows nobody. It is given `initial_peers` — a handful of multiaddrs of already-running peers,
passed in by whoever organises the training run (typically published in a README or a chat channel). It
connects to those, learns about their neighbours, and grows its own view from there. This is called
**bootstrapping**, and those first contacts are **bootstrap peers**.

**At the end of Layer 1**, a peer can send arbitrary bytes to any other peer **whose address it knows or can
resolve** — via the peerstore, the identify protocol, libp2p's peer-routing DHT, or a relay — encrypted and
authenticated, even through firewalls. A PeerID on its own is not enough; something must first turn that name
into a route.

---

## 4. Layer 2 — The DHT: the shared noticeboard

### What a DHT is and why we need one

Peers need somewhere to post messages like *"I am peer X, I'm ready to average, find me here."* In a normal
system that's a database on a server. With no server, we need a **distributed hash table (DHT)**: a key→value
store whose contents are spread across all the participating peers, where any peer can look up any key without
knowing in advance who holds it.

Think of it as a phone book that no single person owns — each person memorises a few pages, and there's a rule
that lets you work out who memorised the page you need.

Hivemind implements **Kademlia**, the most widely used DHT design (also used by BitTorrent and Ethereum).
`hivemind/dht/routing.py:20-23` says so explicitly.

### The core trick: XOR distance

Kademlia's central idea is a made-up notion of "distance" that has nothing to do with geography.

Every peer gets a **node_id**: a 160-bit number (`DHTID`, `routing.py:252-256` — 160 bits because it comes from
SHA-1, which produces 20 bytes). Every *key* you might store also gets hashed into a 160-bit number in the same
space (`node.py:386`: `DHTID.generate(source=key)`).

The distance between two of these numbers is their **XOR**.

> **XOR (exclusive or)** compares two numbers bit by bit. Each output bit is 1 if the input bits *differ*, 0 if
> they match. So `1011 XOR 1001 = 0010` (= 2, small: they mostly agree), while `1011 XOR 0100 = 1111` (= 15,
> large: they disagree everywhere). Numbers sharing a long leading prefix are "close"; numbers differing early
> are "far".

`routing.py:275-283`:
```python
def xor_distance(self, other):
    return int(self) ^ int(other)
```

This is the whole basis of the system. It gives one rule that answers every question:

> **The rule: a record with key *K* is stored on the peers whose node_ids are numerically closest to
> `hash(K)` in XOR distance.**

Everyone can compute this rule independently, with no coordination. To store, you find the closest peers and
send it to them. To fetch, you find the closest peers and ask them. Both operations converge on the same
peers, so it works.

### The routing table: what each peer remembers

No peer knows all the others — that wouldn't scale. Each peer keeps a **routing table**
(`RoutingTable`, `routing.py:20`) that is deliberately lopsided: **detailed knowledge of its own
neighbourhood, vague knowledge of far-away regions.**

The table is a list of **k-buckets** (`KBucket`, `routing.py:167`). Each bucket covers a range of the ID space
and holds at most `bucket_size` peers — default **20** (`node.py:104`); this is Kademlia's parameter *k*.

- Buckets covering ID ranges *near* your own node_id get split repeatedly as they fill, so nearby space is
  finely resolved.
- Buckets covering far-away ranges stay coarse: a single bucket of 20 peers might represent half the entire
  ID space.

The result: each peer stores roughly *log(N)* buckets for a network of N peers — about 20-ish buckets for a
million peers. Tiny memory, and still enough to route to anyone.

**What happens when a bucket is full** (`routing.py:185-207`, `routing.py:49-75`):

1. If the peer is already in the bucket, move it to the front (most recently seen).
2. Else if there's room, add it.
3. Else if the bucket covers your own node_id, split the bucket in two and retry.
4. Else, put the newcomer on a **replacement list** and return the *least recently seen* current occupant so
   the protocol can ping it (`routing.py:209-214`, acted on in `protocol.py:397-401`). If that old peer still
   answers, it stays and the newcomer is turned away.

Step 4 is Kademlia's deliberate bias: **long-lived peers are preferred over new ones**, on the empirical
observation that a peer online for an hour is likely to stay another hour. It also makes it mildly harder to
flood someone's routing table with fresh arrivals.

### The four RPCs

An **RPC** (remote procedure call) is "call a function on another machine as if it were local". Hivemind's DHT
speaks four, defined in `hivemind/dht/protocol.py`:

| RPC | Meaning | Code |
|---|---|---|
| `rpc_ping` | "Are you alive? Here's who I am." | `protocol.py:143` |
| `rpc_store` | "Please remember this key→value." | `protocol.py:232` |
| `rpc_find` | "Who do you know that's near this key? And do you have its value?" | `protocol.py:280` |
| (`rpc_ping` with `validate=True`) | "Also check you can reach me back." | `protocol.py:153-155` |

`rpc_find` doing *both* jobs at once is a nice efficiency: one round trip either finds the value or gets you
closer to it.

### Looking something up: iterative beam search

You want the value for key *K*, and you don't know who has it. You do **not** ask a server, and you do **not**
forward your request through a chain. Instead — this is Kademlia's second key idea — **you stay in control the
whole time and repeatedly ask "who's closer?"**

The algorithm is in `hivemind/dht/traverse.py` (`simple_traverse_dht` is the readable version; `traverse_dht`
is the real, concurrent one). Step by step:

1. Compute `key_id = hash(K)`.
2. From your own routing table, pick the `beam_size` peers you know that are closest to `key_id`
   (default `beam_size` = `bucket_size` = 20). Put them in a **candidate list**, sorted nearest-first.
3. Send `rpc_find(key_id)` to the nearest candidates you haven't asked yet.
4. Each replies with *the peers it knows that are closest to `key_id`* — plus the value, if it has it.
5. Add any new peers to your candidate list, re-sort, and repeat from step 3.
6. Stop when a round of asking produces nobody closer than you already had, or when you have the value.

Each round strictly decreases the distance to the target, so you converge in about **log₂(N)** rounds — around
20 hops for a million peers. Hivemind runs `num_workers` = **4** of these requests concurrently
(`node.py:42`) to hide latency.

> **Terminology:** this is called *iterative* lookup (you do the walking) as opposed to *recursive* (each peer
> forwards on your behalf). Iterative is slower but keeps you informed of every peer involved.

**One detail that matters later.** Values in Hivemind's DHT carry an **expiration time** — a timestamp after
which the record is discarded. When a lookup receives the same key from several peers with different
expirations, the tie-break is in `node.py:838-845`:

```python
elif candidate.expiration_time > (self.expiration_time or float("-inf")):
    self.binary_value = candidate.value
```

**The copy with the furthest-future expiration wins**, and as soon as one candidate is good enough the search
stops early (`finish_search()`). There is no voting or cross-checking between the replicas. The reasoning is
that a later expiration means fresher information — which is true when everyone is honest.

### Storing something

`DHTNode.store` (`node.py:365-470`):

1. `key_id = DHTID.generate(source=key)`.
2. Run the lookup above to find the peers closest to `key_id`.
3. Send `rpc_store` to the **`num_replicas` nearest** — default **5** (`node.py:105`).
4. Count successes; if a peer fails to answer, fall back to the next-nearest candidate.

> **Replication** = storing the same record on several peers so it survives some of them going offline. Five
> copies means the record survives until all five leave.

Records also carry an expiration and simply vanish afterwards. That's the garbage-collection strategy: no
deletion protocol, just let things rot. Peers that still care re-publish before expiry.

**Caching.** By default a peer that looks something up also keeps a copy (`cache_locally=True`), stores a copy
on one peer it passed on the way (`cache_nearest=1`), and caches values it is asked to store
(`cache_on_store=True`) — `node.py:112-116`. This is a *speed* optimisation for popular keys.

### Subkeys: many writers, one key

A plain key holds one value. But "everyone who wants to average right now" needs *many* peers writing under
one key without overwriting each other. Hivemind's answer is **subkeys**: a key can hold a dictionary, and each
writer writes under its own subkey with its own independent expiration (`DictionaryDHTValue`,
`hivemind/dht/storage.py`; merge logic at `node.py:834-838`).

Layer 3 uses exactly this: key = "the group", subkey = "my PeerID".

### Validators: optional rules about what may be stored

Before accepting a record, a peer can run it through a **validator**
(`protocol.py:407-424`, interface in `hivemind/dht/validation.py`). Two ship with Hivemind:

- **`SchemaValidator`** — checks the value has the expected shape/type. Stops malformed data.
- **`RSASignatureValidator`** (`hivemind/dht/crypto.py:12`) — introduces **protected records**. If a key or
  subkey contains the text `[owner:ssh-rsa AAAA...]`, then the value must carry a valid **signature** from that
  public key, or it is rejected. In effect: *only the owner of this key may write under it.*

This is genuinely good machinery. Note two things, though, because they come up in §9:

1. It is **opt-in per record type**. Each subsystem chooses whether to use it.
2. The RSA key here is a *third* identity — a process-wide RSA key from `hivemind/utils/crypto.py` — separate
   from both the libp2p PeerID and the DHT node_id.

There is also an optional `AuthorizerBase` / `TokenAuthorizerBase` (`hivemind/utils/auth.py:32-48`) for running
a **moderated** network where a central authority issues access tokens. It is **off by default**.

### Practical note: the DHT is its own process

`hivemind.DHT` (`hivemind/dht/dht.py:22`) is a `ForkProcess` — a separate OS process running an asyncio event
loop, talking to your training script over a pipe. This is so that heavy GPU work in your main process can
never stall DHT heartbeats and get you dropped from the network. So a Hivemind peer is really **three**
processes: training script, DHT process, `p2pd`.

**At the end of Layer 2**, any peer can post a message under any key and any peer can find it, with no server.

---

## 5. Layer 3 — Matchmaking: finding people to average with

Now the interesting part. Say 1,000 peers are training. You are ready to average *right now*. You need to find
about 8–16 others who are also ready right now, agree with them that you form a group, and agree on the order
of members — all in a few seconds, with no coordinator.

### Group keys: dividing the crowd into rooms

If all 1,000 peers used one DHT key, that key would be a hotspot and everyone would try to average with
everyone. Instead peers are split into **buckets**, each identified by a **group key** of the form
`prefix.0bBITS` (`hivemind/averaging/key_manager.py:13`):

```
bert_training.0b0110
└─ prefix ──┘ └ bits ┘
```

The prefix names the training run (everyone in the run shares it). The bits pick a bucket — think of it as a
room number. `0b0110` is room 6 of 16. Peers only average with others in the same room, which keeps groups
small; 4 bits of suffix means 16 rooms, so 1,000 peers become ~60 per room.

### Step 1: announce yourself

`declare_averager` (`key_manager.py:46-67`) is one DHT write:

```python
return await self.dht.store(
    key=group_key,                # the room, e.g. "bert_training.0b0110"
    subkey=peer_id.to_bytes(),    # who I am
    value=looking_for_group,      # True = ready to average
    expiration_time=expiration_time,
)
```

The **expiration_time** here is doing double duty and this is the subtle bit: it is not just "delete this after
a while", it is a **promise** — *"I intend to run an all-reduce before this moment."* It is your declared
deadline.

### Step 2: read the room

`get_averagers` (`key_manager.py:69-92`) does one DHT read of the group key and gets back the dictionary of
every peer currently declared in that room, with each one's promised deadline.

### Step 3: pick a leader — earliest deadline wins

Everyone in the room now sees the same list. They need to converge on *one* peer to assemble the group, and
they need to do it without talking. The rule (`matchmaking.py:479-497`, `pop_next_leader`) is:

> **Sort the room by `(expiration_time, peer_id)` and follow the smallest.**

Concretely, each peer compares the earliest-deadline candidate in the room against itself:

```python
if maybe_next_leader is None or (entry.expiration_time, maybe_next_leader.to_bytes()) > (
    self.declared_expiration_time, self.peer_id.to_bytes()
):
    ... wait to be contacted instead ...
```

If someone declared an earlier deadline than mine, I send them a join request. If *I* have the earliest
deadline, I don't follow anyone — I wait, and requests come to me. The PeerID is a tie-break so that two peers
with identical deadlines still agree who's first.

Why the earliest deadline? Because that peer is the most urgent — it will run its all-reduce soonest, so
gathering around it wastes the least time. It is a scheduling heuristic, and it works nicely as a
leader election because it's a deterministic function of public information: everyone computes the same answer
independently.

### Step 4: the leader assembles the group

Followers send `rpc_join_group` to the leader. The leader accepts until it has `target_group_size` members (or
`min_group_size` and time runs out), then broadcasts a `GroupInfo` to all of them containing:

- a random **`group_id`** — a fresh identifier for this specific all-reduce run;
- the **ordered list of PeerIDs** in the group — the order matters in Layer 4;
- each member's assigned **fraction** of the work.

Everyone who receives it now has an identical picture of the group. Matchmaking is done.

### Step 5: reshuffle the rooms

If peers stayed in the same room forever, you'd get 16 isolated cliques and the model would never truly
converge globally — averaging must eventually mix everyone. So after every successful group, each member
changes room (`update_key_on_group_assembled`, `key_manager.py:94-104`):

```python
rng = random.Random(group_info.group_id)          # seeded on the group id
index = group_info.peer_ids.index(self.peer_id)   # my position in the group
generalized_index = rng.sample(range(num_buckets), group_info.group_size)[index]
new_bits = bin(generalized_index)[2:].rjust(nbits, "0")
self.group_bits = (self.group_bits + new_bits)[-len(self.group_bits):]
```

Read it slowly: seed a random number generator with the shared `group_id`, draw a permutation, and each member
takes the entry at its own index. Because the seed is shared, everyone computes the *same* permutation, but
each member reads off a *different* room from it — so the group scatters, deterministically and without
sending a single extra message. Elegant. Over many rounds, everyone mixes with everyone.

**At the end of Layer 3**, you have a group of ~8–16 peers, all of whom agree on the membership, the ordering,
and a `group_id`.

---

## 6. Layer 4 — All-reduce: actually averaging the numbers

Now the group must average its vectors. Every member has, say, 100 million numbers; the group must end up with
each member holding the (weighted) mean of all members' vectors.

### The naive way, and why it's bad

Everyone sends their full vector to everyone: with *n* peers and vector size *V*, that's *n×(n−1)×V* numbers
crossing the network, and the coordinator's uplink melts. Home connections cannot do this.

### The good way: reduce-scatter then all-gather

Split the work by *position in the vector* rather than by peer. `AllReduceRunner`
(`hivemind/averaging/allreduce.py:31`) implements this:

**Phase 1 — reduce-scatter.** The vector is cut into *n* chunks. Peer *i* is made **responsible** for chunk *i*
(this is why the group's peer *ordering* mattered). Every peer sends its own chunk *i* to peer *i* — and
nothing else. Peer *i* sums all *n* copies of chunk *i* it receives and divides: it now holds the finished
average of that chunk, and nobody else has computed that chunk at all.

**Phase 2 — all-gather.** Each peer *i* sends its finished chunk *i* back to everyone. Every peer reassembles
the *n* finished chunks into the complete averaged vector.

Each peer sends and receives roughly *2V* numbers regardless of group size — **no hotspot, no coordinator**.
This is the standard technique from datacenter training (it's what NCCL does); Hivemind's contribution is
running it among peers who met thirty seconds ago.

Hivemind adds two refinements:

- **Uneven shares.** `peer_fractions` gives faster peers bigger chunks (`hivemind/averaging/load_balancing.py`
  solves a small linear program for this). A phone and an A100 in the same group both contribute usefully.
- **Deltas, not values.** Peers return the *difference* between the averaged chunk and what you sent
  (`allreduce.py:352`), which is numerically better-behaved and compresses well.

### The actual arithmetic

`TensorPartReducer.accumulate_part` (`hivemind/averaging/partition.py:220-247`) is where numbers are combined:

```python
self.accumulator.add_(tensor_part, alpha=weight)   # running sum, scaled by weight
self.current_part_accumulated_from += 1
self.denominator += weight                         # running total of weights
...
self.current_part_future.set_result(self.accumulator.div_(self.denominator))
```

That is a **weighted mean**: `Σ(weightᵢ × vectorᵢ) / Σ(weightᵢ)`.

**What is the weight?** It travels in each message (`weight=msg.weight`, `allreduce.py:338`) and is a scalar
sent by the peer contributing the data. Its intended meaning is *"how many training samples this gradient is
based on"* — set at `hivemind/optim/grad_averager.py:196`:

```python
control.weight = self.local_samples_accumulated if weight is None else weight
```

This is correct and necessary: a peer that processed 512 examples should count more than one that processed 8,
otherwise the average is biased toward slow peers. Note for later that it is **self-reported**.

### Handling failures

Volunteers vanish mid-transfer. `AllReduceRunner` handles this by **banning** (`allreduce.py:318-322`): if a
peer stops sending its chunks within `sender_timeout`, it is added to `banned_senders`, excluded from the
running sums, and the all-reduce completes without it. The ban is per-round and purely about **liveness** —
"you stopped talking", never "I don't like your numbers". There is also a basic sanity check that the sender is
actually in this group (`_check_reasons_to_reject`, `allreduce.py:325-334`).

**At the end of Layer 4**, every surviving member holds the weighted average of the group's vectors.

---

## 7. Layer 5 — The Optimizer: gluing it to PyTorch

`hivemind.Optimizer` (`hivemind/optim/optimizer.py:32`) wraps everything above so users barely change their
code:

```python
dht = hivemind.DHT(initial_peers=INITIAL_PEERS, start=True)
opt = hivemind.Optimizer(dht=dht, run_id="run_42", batch_size_per_step=4,
                         target_batch_size=4096, params=model.parameters(),
                         optimizer=lambda params: torch.optim.Adam(params))
while True:
    loss = compute_loss_on_batch(model, batch_size=4)
    opt.zero_grad(); loss.backward()
    opt.step()          # <-- trains collaboratively
```

### Epochs: the unit of coordination

The design goal is stated in the docstring: **the result should be equivalent to normal training with
`target_batch_size`, no matter how many peers there are.** So peers do not average every step. Instead:

1. Each peer trains locally, accumulating gradients and **counting samples processed**.
2. Peers continuously report their counts to the DHT.
3. When the **collective** count reaches `target_batch_size` (e.g. 4096), everyone triggers an averaging
   round: matchmaking (Layer 3) → all-reduce (Layer 4) → apply the averaged gradient → learning-rate scheduler
   step.
4. That completes one **epoch** in Hivemind's sense — an interval between synchronisations, *not* a pass over
   the dataset.

If peers join, the target is reached faster and epochs come more often. Hyperparameters stay valid as the
crowd grows and shrinks — which is the whole point.

### The ProgressTracker

Step 2 is `hivemind/optim/progress_tracker.py`: a background thread publishing this peer's progress to a DHT
key `f"{prefix}_progress"` and reading everyone else's, so each peer can estimate the global sample count and
predict when the epoch will end.

Worth noting precisely, because it's the model for how the rest *could* work
(`progress_tracker.py:108-110`):

```python
signature_validator = RSASignatureValidator(private_key)
self._local_public_key = signature_validator.local_public_key
dht.add_validators([SchemaValidator(TrainingProgressSchema, prefix=prefix), signature_validator])
```

The progress tracker **signs its DHT records** and rejects unsigned or wrongly-signed ones. Records here are
authenticated.

### Catching up

A peer that joins late, or falls behind, downloads current parameters and optimizer state from a peer that is
up to date (`load_state_from_peers`). Newcomers don't drag the swarm backwards.

---

## 8. End-to-end: one training step, start to finish

Putting all five layers together. You are peer **P** in a 500-peer run training BERT.

| # | What happens | Layer | Code |
|---|---|---|---|
| 1 | Your script calls `loss.backward()`; you have a fresh local gradient. | — | PyTorch |
| 2 | `opt.step()` adds your 4 samples to the local counter. Not enough yet — return immediately, keep training. | 5 | `optimizer.py` |
| 3 | In the background, the ProgressTracker publishes "P has done 128 samples", **signed**, to `run_42_progress`. It reads others' reports; global total is 4,050 of 4,096. | 5→2 | `progress_tracker.py:108` |
| 4 | Target reached. P decides to average, and picks a deadline ~5s out. | 5 | `optimizer.py` |
| 5 | P computes its room: `bert.0b0110`. It writes to the DHT: key `bert.0b0110`, subkey = its PeerID, value `True`, expiration = its deadline. | 3→2 | `key_manager.py:46` |
| 6 | That write is a DHT store: hash the key, beam-search for the 5 nearest node_ids, `rpc_store` to each. | 2→1 | `node.py:365` |
| 7 | P reads the same key back. Twelve peers are in the room, each with a deadline. | 3→2 | `key_manager.py:69` |
| 8 | P sorts by `(deadline, peer_id)`. Peer L's deadline is earlier than P's ⇒ L is the leader. P sends `rpc_join_group` to L. | 3 | `matchmaking.py:479` |
| 9 | L collects 8 members, mints a random `group_id`, fixes the member order and each member's fraction, broadcasts `GroupInfo`. | 3 | `matchmaking.py` |
| 10 | P is member #3 of 8, so it is responsible for chunk 3. It sends chunk *i* of its gradient to member *i*, for every *i*, tagged with `weight = 128` (its sample count). | 4 | `allreduce.py:31` |
| 11 | P receives 8 copies of chunk 3, accumulates `Σ wᵢ·xᵢ`, divides by `Σ wᵢ`. Chunk 3 is now averaged. | 4 | `partition.py:242` |
| 12 | P broadcasts finished chunk 3 (as a delta); receives finished chunks 1,2,4…8 from the others; reassembles the full averaged gradient. One member times out and is banned; the average proceeds over 7. | 4 | `allreduce.py:318` |
| 13 | P applies the averaged gradient with Adam, steps the LR scheduler, increments its local epoch. | 5 | `optimizer.py` |
| 14 | P reshuffles: seeds `random.Random(group_id)`, reads off its new room `bert.0b1011`. All 8 members scatter to different rooms. | 3 | `key_manager.py:94` |
| 15 | Back to step 1. | | |

Total elapsed: a few seconds. No server was involved at any point.

---

## 9. Where trust sits

You now know how the system works. A short, honest note on which parts are verified and which are taken on
faith — because that's genuinely part of understanding the design, and because it's what
[`FINDINGS.md`](FINDINGS.md) is about.

### Two identities, only one of them verified

This is the single most important structural fact in Hivemind:

| | **PeerID** | **node_id** (DHTID) |
|---|---|---|
| What it decides | how packets reach you | **which records you store** |
| Where it comes from | `multihash(sha256(public_key))` | `DHTID.generate()` — random, **or supplied by the caller** |
| Checked by other peers? | **yes** — TLS proves you hold the private key | **no** — you say it, others believe you |
| Source | `p2p_daemon_bindings/datastructures.py:18` | `node.py:171`, `protocol.py:150` |

Look at `node.py:171`:

```python
self.node_id = node_id if node_id is not None else DHTID.generate()
```

The default is random, which is what an honest peer does. But a caller may pass any 160-bit number, and no
other peer checks it against anything. Since §4's rule is "the peers closest to `hash(K)` store *K*", **a peer
that chooses its own node_id chooses which records it stores.** The unforgeable identity (PeerID) never enters
the routing decision.

### Things that are trusted as stated

Working through what a peer accepts from others without verification:

1. **node_id** — as above. Custody is decided by an unverified number.
2. **Averaging declarations.** In `declare_averager` the subkey is the peer's own `peer_id.to_bytes()`
   (`key_manager.py:60`), but the DHT never checks that the writer is that peer, and these records are not
   signed. Contrast the ProgressTracker, which *is* signed (`progress_tracker.py:108`) — the machinery exists
   and is used elsewhere; averaging keys just never got it.
3. **The declared deadline.** Leadership is a function of a self-chosen timestamp (`matchmaking.py:487`).
   Nothing constrains what you declare.
4. **The aggregation weight.** `weight` crosses the wire and is used verbatim as a multiplier
   (`partition.py:242`). Its honest meaning is a self-reported sample count.
5. **The gradient contents.** The average is a plain weighted mean. There is no robust aggregation — no
   median, no trimming, no outlier rejection anywhere in the tree.
6. **Liveness only, for bans.** `banned_senders` fires on timeouts, never on suspicious values.

### Why it's built this way

Not carelessness — a different threat model. Hivemind was designed for **cooperative** swarms: research groups,
volunteer projects like [Petals](https://petals.dev), lab collaborations, where participants are broadly known
and the adversary of concern is *unreliability*, not malice. Every design choice above is the right call for
that setting: self-chosen node_ids allow deterministic testing; unsigned declarations save a signature per
round; the raw weighted mean is statistically optimal *if everyone is honest*; expiration-wins is the right
freshness rule among honest reporters.

The security question — what changes when a participant is adversarial rather than merely flaky, and what it
would cost to fix — is the subject of [`FINDINGS.md`](FINDINGS.md). Hivemind's own documentation is upfront
that public open swarms need the optional authorizer.

---

## 10. Glossary

| Term | Meaning |
|---|---|
| **All-gather** | Second phase of all-reduce: each peer broadcasts its finished chunk so everyone can reassemble the whole vector. |
| **All-reduce** | Combine one vector from each peer (here: average) and give the result to every peer. |
| **Bootstrap peer** | An already-running peer whose address a newcomer is given, so it has somewhere to start. |
| **Daemon** | A background program providing a service to other programs. Here: `p2pd`. |
| **DHT (distributed hash table)** | A key→value store spread across many peers with no server, where a rule determines who holds what. |
| **Epoch (Hivemind sense)** | The interval between two synchronisation points — i.e. `target_batch_size` samples processed swarm-wide. *Not* a pass over the dataset. |
| **Expiration time** | Timestamp after which a DHT record is discarded. In matchmaking it doubles as a declared deadline. |
| **Gradient** | One number per model parameter, saying which way to nudge it to reduce the loss. |
| **Group key** | DHT key naming an averaging bucket: `prefix.0bBITS`, e.g. `bert.0b0110`. |
| **Hash function** | One-way function mapping any input to a fixed-size fingerprint. |
| **Iterative lookup** | Lookup style where the searcher contacts each hop itself, rather than having requests forwarded. |
| **k-bucket** | Routing-table slot holding up to *k* (=20) peers within one range of the ID space. |
| **Kademlia** | The DHT design Hivemind implements; organises peers by XOR distance. |
| **libp2p** | Modular P2P networking library (from IPFS) handling encryption, NAT traversal, connections. |
| **Identify protocol** | libp2p handshake extra in which two connecting peers exchange their listen addresses and the address each observes the other from. A major source of peerstore entries. |
| **Multiaddr** | Self-describing network address, e.g. `/ip4/1.2.3.4/tcp/5/p2p/QmXyz`. Unlike a PeerID, it is routable. |
| **Peer routing** | Resolving a PeerID (a name) into a multiaddr (a route). Done via the peerstore, identify, libp2p's Kad-DHT, or relay addresses. |
| **Peerstore** | A peer's local cache mapping known PeerIDs to their known addresses. |
| **NAT** | Network Address Translation: your router sharing one public IP among devices; blocks unsolicited inbound connections. |
| **node_id (DHTID)** | 160-bit number deciding which DHT records a peer stores. Self-chosen; **not** verified. |
| **PeerID** | Cryptographic identity derived from a peer's public key. **Verified** by TLS. A *name*, not an address — it must be resolved before you can dial it. |
| **Private/public key** | Matched key pair; the private key signs, the public key verifies. |
| **Reduce-scatter** | First phase of all-reduce: peer *i* collects and averages chunk *i* from everyone. |
| **Relay** | A third peer forwarding traffic between two peers that cannot connect directly. |
| **Replication (`num_replicas`)** | Storing a record on several peers (default 5) so it survives departures. |
| **RPC** | Remote procedure call: invoking a function on another machine. |
| **Signature** | Proof, verifiable with a public key, that the holder of the private key authored a message. |
| **Subkey** | Second-level key letting many writers share one DHT key without overwriting each other. |
| **Validator** | Optional rule run before accepting a DHT record (schema check, signature check). |
| **Weight** | Scalar multiplier on a peer's contribution to the average; honestly, its sample count. |
| **XOR distance** | `a ^ b` treated as a distance. Long shared prefix ⇒ close. The basis of Kademlia. |

---

## 11. Map of the codebase

If you want to read the source, this is a sensible order.

| Path | What's in it |
|---|---|
| `hivemind/p2p/p2p_daemon.py` | Launches and wraps the Go `p2pd`. Start at `create()` (line 84). |
| `hivemind/dht/routing.py` | `DHTID` (XOR distance), `KBucket`, `RoutingTable`. **The best first file to read.** |
| `hivemind/dht/traverse.py` | The lookup algorithm. Read `simple_traverse_dht` first — it's the documented, single-worker version. |
| `hivemind/dht/protocol.py` | The four RPCs, on the wire. |
| `hivemind/dht/node.py` | `DHTNode.store` / `.get`, caching, `Blacklist`. The DHT's brain. |
| `hivemind/dht/dht.py` | Process wrapper; the API your code actually calls. |
| `hivemind/dht/crypto.py`, `validation.py` | Record signing and validation. |
| `hivemind/averaging/key_manager.py` | Group keys, declaring, reshuffling. **Short and very readable.** |
| `hivemind/averaging/matchmaking.py` | Leader election and group assembly. The hardest file here. |
| `hivemind/averaging/allreduce.py` | Reduce-scatter / all-gather, banning. |
| `hivemind/averaging/partition.py` | Chunking and the weighted-mean arithmetic. |
| `hivemind/optim/optimizer.py` | The user-facing `hivemind.Optimizer`. Docstring alone is worth reading. |
| `hivemind/optim/progress_tracker.py` | Swarm-wide sample counting; an example of signed DHT records. |

**Suggested reading order:** `routing.py` → `traverse.py` → `node.py` → `key_manager.py` → `allreduce.py` →
`optimizer.py`.

---

*This file is documentation only and is part of the additive `research/` tree; no core Hivemind code was
modified to produce it.*
