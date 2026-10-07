# Master Thesis: Hijacking Peer Discovery in Decentralized AI Training (30 hp)

*Draft advertisement for the Chalmers career portal. Start: 21 January 2027.*

Alternative titles, same project:

- Master Thesis: Who Decides Who You Train With? Control-Plane Security in Decentralized AI (30 hp)
- Master Thesis: Control-Plane Attacks on DHT-Based Decentralized Machine Learning (30 hp)
- Master Thesis: Identity Binding Failures in libp2p Peer-to-Peer Control Planes (30 hp)

---

## Motivation

Training a large neural network normally requires a datacentre. Projects such as Hivemind,
Petals and Prime Intellect propose an alternative: volunteers contribute ordinary machines
over the open internet, and the model is trained collectively with no central server and no
trusted coordinator. The approach works. It has produced real models, and it is the technical
foundation of a growing number of decentralized AI platforms.

Security research on these systems has concentrated almost entirely on the **data plane** —
the gradients themselves. A large literature on Byzantine-robust aggregation asks how to
average a set of contributions when some of them are malicious, and proves tolerance bounds
of the form *fewer than half the group may be adversarial*.

Those proofs share an assumption that nobody has checked: that group membership is a fair
random draw, so an attacker controlling a small fraction of the network appears in each
averaging group at roughly that same small fraction.

Our ongoing work on Hivemind indicates that this assumption does not hold, and that the
reason is architectural rather than incidental. Peers in these systems carry **two
identities**: the libp2p `PeerID`, which is a hash of a public key and is therefore
unforgeable, and a second identifier used by the distributed hash table to decide routing
and record placement. In Hivemind that second identifier is chosen freely by the peer itself
and accepted verbatim by everyone else. libp2p hands every platform an authenticated
identity for free; vulnerability appears where a platform introduces a second namespace for
placement, routing or membership and fails to bind it to the first.

The consequences we have already measured in a local swarm:

- Owning every replica of a discovery record costs exactly **five crafted identities**, with
  zero variance, and the cost is identical in a swarm of 50 peers and a swarm of 10 000 —
  it does not scale with network size. Each identity takes about 0.1 seconds to mint.
- Controlling everything a victim peer *sees* when it looks for training partners costs
  **twenty identities**, roughly two seconds of work, after which no honest peer is visible
  to the victim at all.
- An attacker that wins the group leader election controls admission exactly: its share of
  the victim's averaging group becomes *k/(k+1)*, an eightfold amplification over its share
  of the network.
- A single forged record hides a chosen peer from discovery permanently, and the hidden peer
  cannot advertise itself back into existence.
- One malicious peer in a group of four, combined with a single unvalidated numeric field in
  the averaging protocol, moves the trained model **35-148x further** from its correct
  solution.

Mature peer-to-peer systems such as Ethereum's discv5 and IPFS bind the routing identifier to
the cryptographic key and do not have this problem. Smaller and younger platforms frequently
do. That gap is what this thesis investigates.

## Goals and research questions

The project asks whether the coordination layer of a decentralized training system — the
control plane, as distinct from the gradients it carries — can be manipulated by a
participant that follows the protocol exactly and simply lies, or stays silent, at the right
moment.

The protocol is walked step by step, and at each step four questions are asked:

| | question |
|---|---|
| **Deny** | can an attacker prevent this step from completing? |
| **Delay** | can it make the step slow or wasteful while appearing responsive? |
| **Tamper** | can it feed honest peers inconsistent or misleading coordination data? |
| **Select** | can it increase the chance that honest peers end up grouped with *it*? |

Three research questions follow from that frame, and the project is scoped so that a student
addresses one of them in depth rather than all three.

**RQ1 — Coordination integrity.** In Hivemind, a temporary group leader distributes three
fields to its followers: a group identifier, an ordered list of members, and the metadata
each member supplied. Everything that follows is *derived locally* from those three fields by
each peer independently — including which slice of the model each peer is responsible for
averaging. A follower verifies only that it appears in the list and that two lists have equal
length. **No peer ever checks that it received the same roster as anyone else.** Can a leader
send different but individually plausible rosters to different followers, and does the result
fail loudly or produce a silently incorrect average?

**RQ2 — Denial without resources.** We have measured that a leader can accept a follower and
then send nothing, costing the victim its entire training round at the price of one identity
and a `sleep()`. The victim never learns to avoid the attacker, because the exclusion list is
keyed on an advertisement rather than on a peer, and ordinary re-advertisement launders the
attacker back into eligibility. The open question is the control: if a leader that *refuses
immediately* costs the victim the same, there is no finding here. Does stalling cost more
than refusing, and what is the cheapest protocol change that penalises it?

**RQ3 — Does the diagnosis generalize?** If the root cause is an unbound second namespace,
then the same analysis should predict which *other* DHT + libp2p platforms are vulnerable and
which are not. Candidate targets include Petals (decentralized inference, built directly on
Hivemind's DHT, where the control plane decides which servers hold which parts of the model)
and Waku (decentralized messaging, whose peer-exchange protocol deliberately bypasses the
authenticated discovery path for light clients). Ethereum's discv5 serves as the control
case, where the binding is present. Does the framework predict exploitability, or merely
describe what has already been found?

## Work packages

The thesis is 30 hp over one semester and suits one or two students. Two students would take
RQ1 and RQ2 as complementary tracks, or one student would take RQ3 as a comparative study.

**WP0 — Environment and baseline (2 weeks).** Bring up a local swarm on your own machine and
reproduce the project's existing clean baseline. You inherit a working experimental
harness — roughly a dozen instrumented attack scripts, recorded results, and a detailed
handover document covering the traps that cost us time. You will not spend the first month
fighting the framework.

**WP1 — Protocol mapping (3 weeks).** Enumerate every coordination message in the chosen
subsystem: who sends it, what the receiver validates, and which later decision depends on it.
Produce the Deny/Delay/Tamper/Select matrix for that subsystem and identify the unchecked
fields. This deliverable is useful on its own, whatever the experiments then show.

**WP2 — Adversarial implementation (4 weeks).** Implement the chosen manipulation as a
subclass of the real components — the library itself is never modified, which keeps every
result attributable to the shipped code. All work runs on local swarms bound to the loopback
interface.

**WP3 — Measurement (5 weeks).** Every adversarial arm is run against a matched honest
control that differs in exactly one behaviour. Measure attacker cost (identities, processes,
wall-clock), effect size, and variance across repeats. Results that depend on winning a race
are noisy at small swarm sizes and must be repeated before they are believed.

**WP4 — Mitigation (3 weeks).** Design and evaluate one concrete, minimal countermeasure —
for RQ1, a shared transcript digest compared before averaging begins; for RQ2, making the
exclusion list peer-keyed with backoff. Measure its cost on an honest swarm, not only its
effect on the attack. A defence that breaks legitimate slow peers is not a defence.

**WP5 — Writing (3 weeks).** Thesis, presentation, and — where a genuine defect is confirmed
— a coordinated disclosure to the upstream project.

## Expectation

**What we expect from you.** Comfort reading and extending an unfamiliar Python codebase of
moderate size; this is the single most important prerequisite. A course in computer networks
or distributed systems, and enough curiosity about protocol details to enjoy tracing why a
timeout is 23 seconds rather than 20. Familiarity with peer-to-peer systems, Kademlia, or
libp2p is welcome but can be learned during WP0-WP1. Machine learning knowledge is **not**
required: the models we train are deliberately small and exist only to give the coordination
layer something to coordinate. You should be willing to report a negative result as a result.

**What you can expect from us.** A defined research question with a real unknown in it, not a
survey. An existing measurement framework and a supervisor working on the same problem daily.
Weekly supervision, and the material to write a genuine security paper: this project has
already produced findings that, as far as we can establish, no published work covers, and the
venue shape for them is a network-security conference rather than a machine-learning one.

**What we do not promise.** That you will find a vulnerability. The thesis is graded on a
reproducible investigation — explicit threat model, matched controls, honest measurement of
variance, and one mitigation comparison — and a rigorous demonstration that a suspected
weakness is in fact adequately contained is a perfectly good outcome. Several of our own most
useful results are negative ones.

**Ethics and scope.** All experiments run on local, self-hosted swarms. We do not probe live
networks or third-party infrastructure, and confirmed defects are disclosed to the upstream
maintainers before publication.

**Practical details.** 30 hp, one or two students, starting 21 January 2027.
Department of Computer Science and Engineering, Chalmers.
Contact: Sarat Chandra Yellapragada, venkatac@chalmers.se.
Supervisor and examiner: *to be completed.*
Application deadline: *to be completed.*
