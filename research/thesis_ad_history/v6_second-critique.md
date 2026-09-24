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
all of them reach for the same two components when peers need to find each other — libp2p for
authenticated transport, and a Kademlia distributed hash table for discovery. How much weight
each project puts on the DHT varies: some route everything through it, others also keep bootstrap
anchors, local-network discovery, cached peers or a peer-exchange protocol alongside it.

Security research on these systems has concentrated on the **data plane**: the gradients or
activations peers exchange. A large literature on Byzantine-robust aggregation asks how to
combine contributions when some are malicious, and proves tolerance bounds on the adversarial
count *within the group being averaged* — Krum tolerates `f` faulty workers among `n`
participants while `2f + 2 < n`, coordinate-wise median while fewer than half are faulty.

Those are conditions on the group. What an operator can reason about is the network: some
estimate of what fraction of all participants might be adversarial. **A bound on the adversarial
fraction of the network does not establish the same bound inside a particular group**, and
nothing makes the two correspond unless group membership is formed in a way that preserves it.

The **control plane** — how peers find each other, how groups form, who is admitted — is what
decides whether the correspondence holds. It has received far less attention.

Our ongoing work on Hivemind suggests that correspondence does not survive contact with the
implementation, for a reason that is architectural rather than incidental. Peers carry **two
identities**: the libp2p `PeerID`, which is a hash of a public key, so nobody can authenticate as
its owner without holding that key; and a second identifier the DHT uses to decide routing and
record placement. In Hivemind the
second identifier is chosen freely by the peer and accepted verbatim by everyone else. What we
have measured locally:

- **Routing coordinates are not bound to authenticated identities.** Owning every replica of a
  discovery record costs exactly `num_replicas` — five — crafted identities, with no variation
  observed across 40 swarms per cell, and the figure is the same at 50 peers and at 10 000,
  because it is a configured constant rather than a property of the network. Minting an identity
  takes about 0.1 seconds; how long it then takes to intercept traffic is a separate quantity we
  have not measured.
- **Lookups have no integrity or route diversity guarantee.** Controlling everything a victim
  sees when it looks for training partners took `bucket_size` — twenty — identities in the tested
  configuration, a sharp step from 14 % capture at eighteen to total at twenty. This is a
  *different* threshold from custody: a lookup seeds its traversal from the `bucket_size` nodes
  nearest the key in the victim's own routing table, so owning the five replicas still leaves
  fifteen honest peers in that seed set, answering truthfully and having their answers merged
  back in. Doubling `bucket_size` to forty doubled the requirement, which is the prediction that
  makes seed-set occupancy the best available explanation.
- **Nothing constrains admission.** In a 16-peer swarm the attacker won the group-leader election
  in 57 % of rounds, and in the rounds it led, its share of the victim's group was exactly
  `k/(k+1)` — one identity beside the victim makes half the group.
- **Application records carry no write authorization.** A single forged record hides a chosen peer
  from discovery, and the hidden peer's own re-advertisement is rejected — invisible on all 30
  re-declarations we observed over 30 seconds, with expirations accepted up to ten years out and
  no upper bound found. This one needs **no crafted identities and no custody at all.**

These are four different missing guarantees, not four symptoms of one, and that distinction is
what makes a retrofit's effect attributable. But the attacks are not as separable as the
guarantees: binding the routing identifier supplies neither lookup integrity nor record
authorization nor admission fairness, yet it would still raise the price of the second finding,
whose attack needs twenty *crafted* identities and would then have to grind for each of them. The
fourth is the clean contrast — the forged withdrawal behaves identically with random and crafted
identifiers, so binding does nothing to it at all.

Guarantees are independent; attacks share capabilities. Working out which attacks a single
mechanism actually raises the cost of, and by how much, is the kind of result this thesis is
for.

**The mature designs prevent one specific capability, and it is worth being exact about which.**
Ethereum's discv5 derives the node's DHT identifier from its secp256k1 public key, and libp2p's
own Kademlia DHT, as used by IPFS, identifies peers by the `PeerID` itself. In both, an
identifier cannot be chosen freely under a fixed authenticated identity: moving to a chosen
coordinate means grinding keys until one hashes where you want it, which is work rather than a
constructor argument. That is the property Hivemind lacks.

It is not a complete solution, and the limits matter for what a retrofit can promise. Grinding
raises the price of chosen placement without making it infinite. Signed, sequence-numbered node
records let a verifier reject an older record in favour of a newer one **it already knows about**
— they give no freshness guarantee to a newcomer, who has nothing to compare against. And an
Ethereum node record describes a node; it is not the equivalent of every mutable application
record a system like Hivemind keeps in the DHT. The comparison is worth making guarantee by
guarantee, not system by system.

So the defences exist and are well understood. What keeps happening is that they are lost again
one layer up: libp2p supplies an authenticated identity, and the application then builds its own
overlay for placement, routing or membership on top of it, and does not carry the guarantee
across the boundary.

## Goals and research questions

The strongest question here is therefore not "is this platform attackable". It is:

> **Where are known security requirements lost when applications build their own overlays on
> secure networking libraries, what follows from the loss, and what does restoring them cost?**

Each clause is one of the research questions below: which guarantee is dropped and at which
boundary (RQ1), how the systems that keep it do so and what that costs them (RQ2), and what it
takes to put it back (RQ3). The question is deliberately about the architecture rather than about
the developers — what a measurement can establish is where a guarantee is absent and what its
absence permits, not why anyone decided to omit it.

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
any libp2p platform that introduces its own routing or membership namespace. Waku is a good
candidate — its documentation describes a peer-exchange protocol that supplies peer lists to
resource-limited clients as an alternative to running discv5 themselves, and what that substitutes
for the discovery path's guarantees is exactly the sort of question this thesis asks.

Note that this is a question, not a prediction. Sharing libp2p and Kademlia does not establish
that a system repeats Hivemind's design — it may bind its identifiers, authenticate its records,
or route through several independent discovery paths, any of which would be a finding worth
reporting. The deliverable is an accurate account of which guarantees the system holds and which
it does not, and that account is equally valid as a thesis outcome in either direction.

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
intermittently connected participants is not a defence. A restoration cost is a result in its own
right: it tells an operator what the guarantee is worth to them, it tells the comparative
framework what the mature systems are paying for the same property, and it is what turns the
thesis from a bug report into an engineering result. It is also where a recommendation to the
upstream project comes from.

**If the guarantee turns out to be present**, RQ3 does not evaporate — it inverts. Remove the
mechanism in a controlled local ablation and measure what its absence would have permitted, or
put it beside the alternative another system uses for the same property and compare cost and
coverage. Either gives the same deliverable: a number for what the guarantee is worth. This is
the path to take if WP0 or WP1 finds the system better defended than expected, and it is a
normal outcome rather than a fallback.

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
and a detailed handover document covering the traps that cost us time.

Then choose the target at the end of this package, and choose it on evidence. Two things must be
established rather than assumed, and neither has been checked for any candidate on the shortlist:
that a multi-node swarm can actually be stood up locally — the thesis permits no other kind of
testing — and what the system's discovery and membership architecture really is, including how
many independent paths a peer has to find another and whether its identifiers are derived from
keys. A candidate that fails the first is unworkable; one that turns out to have bound
identifiers and authenticated records is still a perfectly good target, and RQ2 and RQ3 then have
a positive example to work from.

**WP1 — Control-plane mapping (3 weeks).** Map **one complete workflow** — either discovery or
group membership, chosen in WP0, not both. For every coordination message in it: who sends it,
what the receiver validates, and which later decision depends on it. Produce the
deny/delay/tamper/select matrix for that workflow and identify the unchecked fields. Mapping the
remaining workflows is a stretch goal, not a requirement; one workflow done properly is the
deliverable, and it stands on its own whatever the experiments then show.

End this package by selecting **one principal attack hypothesis** to carry forward. Everything
after this point follows from that choice.

**WP2 — Adversarial implementation and measurement (5 weeks).** Implement the chosen attack
against matched honest controls that differ in exactly one behaviour. Measure attacker cost,
effect size, and variance across repeats — results that depend on winning a race are noisy and
must be repeated before they are believed. Swarms run locally; where the result could plausibly
depend on timing, introduce controlled latency, loss and churn rather than relying on loopback
conditions, which represent no real participant.

**WP3 — Comparative study of the mature designs (3 weeks).** RQ2. Source-level reading of
discv5 and the libp2p Kad-DHT, the attacks that motivated their defences, and the costs and
limits of those defences. **Start this reading during WP0** and run it in the background
throughout — three weeks of dedicated time is enough to finish and write it up, not enough to
begin it cold.

**WP4 — Retrofit and evaluation (4 weeks).** RQ3. **One mechanism**, implemented and measured
against the attack, against an honest baseline including the degraded network conditions from
WP2 — and against **an attacker that adapts to the fix**. If the retrofit derives identifiers
from keys, the adapted attacker grinds keys; evaluating only the original attack script would
overstate what the mechanism bought. If WP1 found the guarantee already present, this package
becomes the ablation or comparison described under RQ3 instead.

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
from this line of work are, as far as we can establish, not covered by published literature. Whether
any particular result reaches a venue depends on its novelty and the strength of its evidence, but
the work is aimed at the standard a network-security venue would expect.

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
