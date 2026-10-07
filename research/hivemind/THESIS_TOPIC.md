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
Vetted September 2026 and confirmed to run libp2p with a Kademlia DHT: Lattica (Rust, cross-NAT
inference and training), SwarmLLM (Rust, single binary), KwaaiNet (Hivemind/Petals-compatible),
Hyperspace (libp2p v3, large deployment). Unvetted: Bacalhau, Fluence, Codex, drand.
**Rejected on inspection: Exo and Kalavai** — see the delegation split below for why.

## Ethereum and IPFS

Not "more of the same if time permits". These serve two distinct purposes, and both are needed.

**As the control case.** discv5 already has the binding Hivemind lacks, since the node ID is
derived from the secp256k1 key. Measuring the residual attack surface where the fix is *present*
is what turns the framework from a list of bugs into something with predictive power. That is the
part that reads as research rather than pentesting, and "secure for the most part" is not the same
as secure — where the mature designs still fall short is itself a research question.

**As the source of the fixes.** The mitigations these systems deploy are concrete, battle-tested
and retrofittable, which is what lets a study of a weaker platform end in an engineering result
rather than a bug report. The two mechanisms that matter here:

- **Node ID derived from the key.** discv5 computes the DHT identifier from the secp256k1 public
  key, so placing yourself next to a chosen target costs key grinding rather than a constructor
  argument. libp2p's own Kad-DHT goes further and identifies peers by the `PeerID` itself. This is
  exactly the binding Hivemind omits, and it is the fix a retrofit would port.
- **Signed, sequence-numbered records.** An Ethereum node record is signed by the node's own key
  and carries a sequence number, so a third party can neither forge it nor roll it back to an
  earlier state. Compare Hivemind, where there is no `record_validator` anywhere under
  `hivemind/averaging/` and a forged withdrawal is accepted with an arbitrary future expiration
  (`FINDINGS.md`, `HANDOFF.md` §4.12).

Each defence also has a cost — grinding raises the price of chosen placement without making it
infinite, and signatures add latency and complexity a small project may have declined on purpose.
Recovering *why* a project skipped the binding is as much a part of the framework as showing that
it did.

## Delegation split

The thesis projects advertised in `THESIS_AD.md` start 21 January 2027 and must run parallel to
this track, not consume it.

**Mine.** The comparative framework itself; the Hivemind depth, including the open items in
`HANDOFF.md` §6 — the `reject` control for §4.13 and the roster-equivocation experiment — and the
Petals feasibility spike below. Petals is the primary target of *this* track precisely because it
reuses Hivemind's own DHT, and for the same reason it should not be advertised to a student.

**Delegated.** One smaller platform per student, applying the method already validated on
Hivemind, ending in a retrofit borrowed from discv5 or the libp2p Kad-DHT. Each student fills a
column of the table above and their mitigation result feeds back into the framework. Candidates,
vetted September 2026: Lattica, SwarmLLM, KwaaiNet, with Hyperspace as a larger-deployment option
and Waku as the non-ML alternative. **Kalavai and Exo were checked and rejected** — Kalavai is
pool-based around a seed node acting as a central control plane, and Exo's discovery is
local-network only (mDNS, UDP broadcast, Thunderbolt), so neither has an open-internet DHT
control plane to attack.

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
