# Master Thesis: Identity Binding Flaws in Peer-to-Peer AI Platforms (30 hp)

*Draft advertisement for the Chalmers career portal. Start: 21 January 2027.*

Alternative titles, same project:

- Master Thesis: Identity Flaws in Peer-to-Peer AI Platforms (30 hp)
- Master Thesis: Control-Plane Security in Peer-to-Peer AI Platforms (30 hp)

---

## Motivation

Training and serving large neural networks normally requires a datacentre. A growing number of
projects propose an alternative: volunteers contribute ordinary machines over the open
internet, and the work is coordinated collectively with no central server and no trusted
party. Hivemind, Petals, Lattica, SwarmLLM, KwaaiNet and Hyperspace all take this approach, and
all of them build peer discovery on the same two components — a Kademlia distributed hash table
running over libp2p.

Security research on these systems has concentrated on the **data plane**: the gradients or
activations peers exchange. A large literature on Byzantine-robust aggregation asks how to
combine contributions when some are malicious, and proves tolerance bounds of the form *fewer
than half the participants may be adversarial*. Those proofs assume that who you are grouped
with is a fair random draw.

The **control plane** — how peers find each other, how groups form, who is admitted — has
received far less attention, and it is where that assumption is decided.

Our ongoing work on Hivemind suggests the assumption does not survive contact with the
implementation, for a reason that is architectural rather than incidental. Peers carry **two
identities**: the libp2p `PeerID`, which is a hash of a public key and therefore unforgeable,
and a second identifier the DHT uses to decide routing and record placement. In Hivemind the
second identifier is chosen freely by the peer and accepted verbatim by everyone else. What we
have measured locally:

- Owning every replica of a discovery record costs exactly **five crafted identities**, with
  zero variance, and costs the same in a swarm of 50 peers as in a swarm of 10 000 — it does
  not scale with network size. Each identity takes about 0.1 seconds to create.
- Controlling everything a victim peer *sees* when it looks for training partners costs
  **twenty identities**, roughly two seconds of work.
- An attacker that wins the group-leader election controls admission exactly, taking a share of
  the victim's group eightfold larger than its share of the network.
- A single forged record hides a chosen peer from discovery indefinitely, and the hidden peer
  cannot advertise itself back into existence.

**The mature systems do not have this problem, and that is the interesting part.** Ethereum's
discv5 derives the node's DHT identifier from its secp256k1 public key, so placing yourself
next to a chosen target costs key grinding rather than a constructor argument, and node records
are signed and sequence-numbered so they cannot be forged or rolled back by a third party.
libp2p's own Kademlia DHT, as used by IPFS, identifies peers by the `PeerID` itself. These are
deployed, battle-tested designs that have already absorbed a decade of attacks and academic
scrutiny.

So the defences exist and are well understood. What keeps happening is that they are lost again
one layer up: libp2p supplies an authenticated identity, and the application then builds its own
overlay for placement, routing or membership on top of it, and does not carry the guarantee
across the boundary.

## Goals and research questions

The strongest question here is therefore not "is this platform attackable". It is:

> **Why are known security requirements lost when developers build application-specific overlays
> on secure networking libraries — and how cheaply can they be restored?**

Both halves matter. The first asks what is lost and where: which requirement, at which boundary,
and whether it was abandoned through unawareness or as a deliberate trade for latency, simplicity
or reach. The second makes that answerable rather than speculative — if restoring the binding
turns out to be cheap, the loss was an oversight; if it is expensive, the project made a choice,
and the framework needs to say what it bought. Measuring the cost of the fix is what tells you
which of those you are looking at.

The thesis answers this on one system, in three steps.

**RQ1 — What breaks, and what does it cost the attacker?** Select one decentralized AI platform
built on libp2p and a Kademlia DHT, smaller and younger than Hivemind. The shortlist is
**Lattica**, a Rust cross-NAT framework for distributed inference and training; **SwarmLLM**, a
peer-to-peer inference network that ships as a single Rust binary, which makes local multi-node
swarms cheap to stand up; and **KwaaiNet**, which is explicitly Hivemind- and Petals-compatible,
so our existing tooling transfers most directly. **Hyperspace**, a substantially larger
deployment, is a further option. The choice is made together during the first weeks.

Map the chosen system's control plane message by message and, at each step, ask four questions:
can an attacker **deny** the step, **delay** it, **tamper** with the coordination data it
carries, or **select** who participates? Measure the cost of each attack in identities,
processes and wall-clock time, not merely whether it is possible.

A student who would rather work on a non-AI system is welcome to: the same analysis applies to
any libp2p platform that introduces its own routing or membership namespace, and Waku's
peer-exchange protocol — which deliberately bypasses the authenticated discovery path for
resource-limited clients — is a good candidate.

This is deliberately the low-risk part of the thesis. Our experience with Hivemind is that
systems in this class have unbound identifiers and unauthenticated coordination records, so a
careful analysis is very likely to find something. The thesis does not depend on it being
something dramatic.

**RQ2 — What do Ethereum and IPFS do differently, and what does it cost them?** Study how
discv5 and the libp2p Kad-DHT solve the same problems: identifier derivation, record
authentication, freshness and replay resistance, routing-table admission and eviction policy.
The goal is a precise account of which mechanism stops which attack — several of these defences
were added in response to specific published attacks, and tracing that history is part of the
work. Equally important is what they cost: key grinding raises the price of chosen placement,
but does not make it infinite, and signed records add latency and complexity a small project
may have consciously declined.

**RQ3 — How cheaply can the requirement be restored?** Implement one mechanism borrowed from
the mature designs in the system studied in RQ1, and re-run the RQ1 attacks against it. Measure
both the security gain and the cost to honest peers: a defence that breaks legitimate slow or
intermittently connected participants is not a defence. This is the half of the research question
that keeps the other half empirical — a cheap fix means the requirement was dropped through
oversight, an expensive one means it was traded away for something, and the price names what.
It is also what turns the thesis from a bug report into an engineering result, and where a
recommendation to the upstream
project comes from.

**Optional extension, for a student who wants it.** "Secure for the most part" is not the same
as secure. Ethereum's discovery layer has its own open questions and an active research
literature, and a strong student may end the thesis by examining where the mature designs still
fall short rather than by stopping at the retrofit. Surveying the current state of that
literature is an early task in either case, since it determines whether this extension is
realistic in the time available.

## Work packages

30 hp over one semester, suited to one or two students. Two students would divide by target
system, sharing RQ2 and comparing their retrofits.

**WP0 — Orientation and target selection (2 weeks).** Reproduce our existing Hivemind attacks
on a local swarm to learn the method on a system where the answers are already known. You
inherit a working experimental harness — a dozen instrumented attack scripts, recorded results,
and a detailed handover document covering the traps that cost us time. Choose the target system
at the end of this package, against explicit criteria: runs locally, actively maintained,
readable codebase.

**WP1 — Control-plane mapping (3 weeks).** Enumerate every coordination message in the target:
who sends it, what the receiver validates, and which later decision depends on it. Produce the
deny/delay/tamper/select matrix and identify the unchecked fields. This deliverable stands on
its own whatever the experiments then show.

**WP2 — Adversarial implementation and measurement (5 weeks).** Implement the most promising
manipulations, against matched honest controls that differ in exactly one behaviour. Measure
attacker cost, effect size, and variance across repeats — results that depend on winning a race
are noisy and must be repeated before they are believed. All work runs on local swarms bound to
the loopback interface.

**WP3 — Comparative study of the mature designs (3 weeks).** RQ2. Source-level reading of
discv5 and the libp2p Kad-DHT, the attacks that motivated their defences, and the costs those
defences impose.

**WP4 — Retrofit and evaluation (4 weeks).** RQ3. One mechanism, implemented and measured,
against both the attack and an honest baseline.

**WP5 — Writing and disclosure (3 weeks).** Thesis, presentation, and coordinated disclosure to
the upstream project where a genuine defect is confirmed.

## Expectation

**What we expect from you.** Comfort reading and extending an unfamiliar codebase of moderate
size; this is the single most important prerequisite, and depending on the target it may be
Python, Rust or Go. A course in computer networks or distributed systems, and enough curiosity
about protocol details to enjoy working out why a timeout is 23 seconds rather than 20.
Familiarity with peer-to-peer systems, Kademlia or libp2p is welcome but can be acquired during
WP0 and WP1. Machine learning knowledge is **not** required: the models involved are
deliberately small and exist only to give the coordination layer something to coordinate. You
should be willing to report a negative result as a result.

**What you can expect from us.** A defined research question with a real unknown in it rather
than a survey; an existing measurement framework and a method already validated on a comparable
system; and a supervisor working on the same problem daily. Weekly supervision. The findings
from this line of work are, as far as we can establish, not covered by published literature, and
the natural venue for them is a network-security conference.

**What we do not promise.** That the flaws you find will be severe. The thesis is graded on a
reproducible investigation — explicit threat model, matched controls, honest treatment of
variance, and a mitigation that is measured rather than asserted — and a rigorous demonstration
that a suspected weakness is adequately contained is a perfectly good outcome. Several of our
own most useful results are negative ones.

**Ethics and scope.** All experiments run on local, self-hosted networks. We do not probe live
deployments or third-party infrastructure, and confirmed defects are disclosed to the upstream
maintainers before publication.

**Practical details.** 30 hp, one or two students, starting 21 January 2027.
Department of Computer Science and Engineering, Chalmers.
Contact: Sarat Chandra Yellapragada, venkatac@chalmers.se.
Supervisor and examiner: *to be completed.*
Application deadline: *to be completed.*
