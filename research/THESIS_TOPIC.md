# Thesis topic: what makes a DHT + libp2p platform vulnerable?

Working notes for a Master's thesis proposal. Builds on the Hivemind control-plane study in this
folder (`node_id_creation_report.md`, `id_pairing_report.md`, `group_key_capture_report.md`,
`forged_pairs_report.md`, `FINDINGS.md`).

## The framing

The question is not "are DHTs attackable". The libp2p Kademlia DHT is reasonably sound on its own,
because the DHT key *is* the PeerID, which is a hash of the public key. Placing yourself next to a
target costs keypair grinding. The sharper statement is:

> libp2p hands every platform an authenticated identity for free. Vulnerability appears where a
> platform introduces a **second** namespace, for placement, routing or membership, and fails to
> bind it to the first.

Hivemind's `node_id` is one instance of that: a second identifier, unbound to the PeerID, that
decides record custody. Measured cost of choosing it: one keyword argument.

The contribution is a comparative framework of the control plane, with axes that predict
exploitability, validated across several systems rather than asserted from one.

| axis | Hivemind | libp2p Kad-DHT | Ethereum discv5 |
|---|---|---|---|
| routing ID bound to a key? | **no** | yes (ID = PeerID) | yes (secp256k1, post Geth 1.9) |
| cost of chosen placement | **one kwarg** | keypair grinding | keypair grinding |
| target address predictable? | **yes** (public string) | varies | varies |
| membership sampled or elected? | **elected + admission** | sampled | sampled |
| control records authenticated? | **no** | n/a | ENR signed |
| what the control plane gates | model weights | content | consensus |

The Hivemind column is already filled by the work in this folder. The thesis fills the others and
tests whether the axes actually predict anything.

## Candidate targets, ranked by risk to finishing

### 1. Petals — primary
Python, built directly on Hivemind's own DHT, so the existing code and expertise transfer with
almost no ramp-up. Control plane is which servers hold which transformer blocks, and how a client's
inference chain is routed across them.

The novelty gap is confirmed, not assumed. The one security paper on Petals
([arXiv 2607.19490](https://arxiv.org/html/2607.19490), July 2026) is **data plane only**: it attacks
tampered activations and explicitly assumes the adversary is already in the chain. It never touches
DHT routing or server selection.

So the result is a composition rather than a port: **the control plane is what makes their
data-plane attack deployable on demand, instead of a matter of luck about placement.**

### 2. Waku (Status / Logos) — best non-ML low hanging fruit
Its **Peer Exchange** protocol deliberately bypasses discv5 for resource-limited light clients: a
single peer supplies the peer list, with none of the DHT's ID binding. Status' own documentation
concedes that rendezvous points can be blocked by an adversary. No audit of it found.

Real users, openly published research (vac.dev), Nim / Go / JS. The control plane gates messaging
privacy, so eclipse becomes metadata deanonymisation and censorship — a different severity story
from the Hivemind one, which is what makes it worth including.

### 3. Celestia / data availability sampling — backup
Best severity story: eclipse a light node and it accepts unavailable data, so the control plane
reaches consensus safety. But it is the most studied of the three already
([2402.09993](https://arxiv.org/pdf/2402.09993), [2306.11456](https://arxiv.org/pdf/2306.11456),
[PANDAS](https://arxiv.org/pdf/2507.00824)). Keep as a fallback.

### Avoid
Prime Intellect archived its peer-to-peer protocol and pivoted to agents; Gensyn's flagship has not
moved since January 2026. Dying targets make bad theses.

### Others if more breadth is wanted
Lattica (Rust, libp2p, decentralised AI), Exo, Kalavai, Bacalhau, Fluence, Codex, drand.

## Ethereum

Not "more of the same if time permits". Treat it as the **control case**: discv5 already has the
binding Hivemind lacks, since the node ID is derived from the secp256k1 key. Measuring the residual
attack surface where the fix is *present* is what turns the framework from a list of bugs into
something with predictive power. That is the part that reads as research rather than pentesting.

## Constraint to settle early

Everything runs on local devnets, as the Hivemind work did (all of it bound to `127.0.0.1`). Probing
live Waku or Celestia nodes would be unauthorised testing against third-party infrastructure. Worth
stating explicitly in the proposal, and it also filters the target list, since not every candidate is
straightforward to run locally. Petals and Waku both are.

## Next step

A feasibility spike before committing in writing: stand up a 3-node local Petals swarm and confirm
the block-routing path is reachable and observable from the DHT.

## Sources

- <https://arxiv.org/html/2607.19490> — Petals inference integrity, data plane only
- <https://docs.waku.org/learn/concepts/peer-exchange/> — Waku Peer Exchange
- <https://docs.waku.org/learn/concepts/discv5> — Waku discv5
- <https://docs.libp2p.io/concepts/security/security-considerations/> — libp2p security considerations
- <https://arxiv.org/pdf/2402.09993> — Kademlia DHT scalability limits for DAS
- <https://arxiv.org/pdf/2306.11456> — DAS P2P networking requirements
- <https://arxiv.org/pdf/2507.00824> — PANDAS
- <https://libp2p.io/reports/annual-reports/2025/> — libp2p 2025 annual report
