# Supervisor meeting — 2 October 2026

Topic: the five-region WAN timing experiment, and whether the deadline-undercut attack is practical.

---

## 1. What the attack is

Hivemind peers who want to average announce themselves in the DHT with a **deadline they choose
themselves**. Whoever announces the earliest deadline becomes the **leader** of the round, and the
leader decides who is allowed into the group.

So the attack is three steps: read the announcements, take the earliest deadline, write your own
with a slightly earlier one.

Undercutting costs nothing. `np.nextafter` lowers the deadline by one floating-point step —
**0.00000024 s** at these timestamps — which wins the comparison while giving up no meaningful
advertisement lifetime.

No computation, no fake identities, no privileged position. Read, subtract, write.

---

## 2. The attack step by step, with measured costs

| # | Step | What it needs | Cost under emulated delays |
|---|---|---|---|
| 1 | Compute the group key `prefix.0b<bits>` | the run name, a public string | free |
| 2 | Poll the key to watch who is advertising | nothing | ~2 s of earlier polls |
| 3 | **Final read** of current ads and deadlines | nothing | **0.9 – 1.7 s** |
| 4 | Undercut the earliest deadline by one float step | nothing | free |
| 5 | **Write** its own advertisement | nothing | **1.2 – 2.1 s** |
| 6 | Wait for honest peers to poll, see it, and send joins | nothing | 1.2 – 1.7 s |
| 7 | Accept joins and lead the group | nothing | — |

**Steps 3 and 5 are the critical path.** Without delay they cost **0.17 s**; with emulated WAN delays they
cost **2.1 – 3.7 s**, i.e. **12–21× more**. That single fact drives every result below.

---

## 3. The network we tested on

**28 DHT nodes**, which is above Hivemind's `bucket_size` of 20 — so no routing table contains the
whole network and lookups genuinely have to route.

| Role | Count |
|---|---|
| Bootstrap | 1 |
| **Attacker** | 1 |
| Honest averagers | 3 |
| Routing-only filler peers | 23 |

Every node is assigned to one of five regions, round-robin, so each region holds **5 or 6 nodes**.
Delays are **injected** into a local swarm, sized from RIPE Atlas ICMP ping medians between
anchor hosts (30 September 2026, three replies per directed pair). The ping times are real; the
Hivemind traffic is not — nothing in this experiment crossed a WAN.

### Measured link RTT (ms), row → column

| from \ to | Stockholm | New York | São Paulo | Mumbai | Sydney |
|---|---|---|---|---|---|
| **Stockholm** | 9 | 99 | 209 | 147 | 298 |
| **New York** | 99 | 13 | 125 | 285 | 240 |
| **São Paulo** | 209 | 111 | 16 | 228 | 305 |
| **Mumbai** | 147 | 240 | 228 | 23 | 468 |
| **Sydney** | 299 | 239 | 305 | 503 | 12 |

Median across regions **233 ms**, range **99–503 ms**. The matrix is slightly asymmetric because
each direction was measured separately.

### How many DHT hops? — we do not assume a number, and that matters

This was worth checking carefully. Delay is injected **per RPC**, one-way on both the request and
the response, based on the two endpoints' regions. Hivemind then performs its **real** iterative
Kademlia lookup, and every RPC in that lookup independently pays its own regional RTT. So hop count
is **emergent and measured, not a parameter we picked**.

What one real DHT read looked like:

- **0.91 s** wall clock
- **65** delayed `find` RPCs served during it
- their RTTs spanned **13–503 ms**

Note that 65 × 209 ms = 13.6 s, but the read finished in 0.91 s. Kademlia runs **4 lookups
concurrently** (`DEFAULT_NUM_WORKERS = 4`), so wall-clock follows the **critical path** — sequential
hop depth × the slowest peer answering at each depth — not the total RPC count.

We can now put a number on it. Fitting operation time against the attacker's mean RTT across the
five origins gives a slope of **4.8 round trips for a read and 5.8 for a write**, with intercepts of
−0.02 s and 0.09 s. The near-zero intercepts say the cost is **almost pure network time with no
meaningful local computation**, and the slopes are the effective sequential depth. A write costs one
round trip more than a read because it reads first and then stores to the replicas.

Practical consequence: the measured read and write times **already contain the DHT walk**. Adding a
separate hop-count term on top would double-count.

---

## 4. Results — all five regions as attacker origin

30 runs: 5 attacker origins × 2 deadline windows × 3 seeds. Within a seed the group key and the
honest peers' schedule are held fixed, so the **only** thing changing across the five rows is where
the attacker sits.

### Outcome: the attacker's location does not matter

| Attacker origin | 11–15 s window | 5–7 s window |
|---|---|---|
| Stockholm | **3/3** | 0/3 |
| New York | **3/3** | 0/3 |
| São Paulo | **3/3** | 0/3 |
| Mumbai | **3/3** | 0/3 |
| Sydney | **3/3** | 0/3 |
| **Total** | **15/15** | **0/15** |

This surprised us. We expected Sydney to fail, since it is the worst-connected region
(Sydney↔Mumbai is 503 ms). It captured the group every time.

### Cost: location matters a great deal

| Attacker origin | Mean RTT to other regions | Attacker read + write |
|---|---|---|
| Stockholm | 188 ms | 2.09 s |
| New York | 187 ms | 2.12 s |
| São Paulo | 213 ms | 2.30 s |
| Mumbai | 271 ms | 2.86 s |
| Sydney | 336 ms | **3.71 s** |

The attacker's critical path scales **almost perfectly linearly** with its mean distance to the rest
of the swarm: **Pearson r = 0.996**, and the cost ratio (1.78×) matches the RTT ratio (1.80×) nearly
exactly.

**Figure:** `research/results/step19_region_sweep.png` — (a) the RTT matrix, (b) the capture table
above, (c) where each origin's seconds go and how much slack is left. Panel (c) shows the cost
gradient directly: Stockholm spends ~2.1 s on read plus write, Sydney ~3.7 s, and both still lead.
Regenerate with `.venv/bin/python research/plot_step19_regions.py` (needs `matplotlib`).

### Putting the two together — the actual finding

**Geography sets the attacker's cost. The deadline window decides whether it succeeds.**

Sydney pays 78 % more than Stockholm and still wins, because an 11–15 s window leaves ~5.6 s of
slack even after its slower 3.71 s. Shrink the window to 5–7 s and *everyone* fails, Stockholm
included.

### Why the short window fails — and it is not what I assumed

The attacker fails *earlier* than expected. It is not losing a race to write; in 14 of 15 short-window
runs it never published at all.

| stage | 11–15 s | 5–7 s |
|---|---|---|
| runs | 15 | 15 |
| found a target worth undercutting | 15 | **5** |
| published its own deadline | 15 | **1** |
| led the group | 15 | **0** |

The attacker only undercuts an advertisement that still has **3.5 s of life left when its read
returns** — `request_timeout` (2.5 s) plus a 1 s safety margin. That threshold is mostly principled:
an advertisement with less than `request_timeout` remaining cannot complete a join handshake anyway.

With a 5–7 s window the honest advertisements are simply too short-lived. The attacker *sees* three
advertisers in every run, but by the time its ~1 s read returns they no longer clear the bar. It has
nothing viable to undercut, so it never writes.

### We tested whether that was our own fault — it was not

Part of the 3.5 s threshold is padding *we* chose, so the obvious objection is that the attack failed
because our attacker was too cautious, not because Hivemind is safe. We removed the caution and re-ran
at the 5–7 s default, from Stockholm, the cheapest origin.

Two knobs, both ours, both set to zero:

- **safety margin** 1.0 s → 0: it will now undercut an advertisement that is right on the edge of
  being joinable at all.
- **observation window** 0.3 s → 0: it commits on first sighting rather than waiting to confirm.

| | cautious (1.0 / 0.3) | aggressive (0 / 0) |
|---|---|---|
| published an advertisement | 1/3 | **3/3** |
| won leadership | 0/3 | **1/3** |
| **assembled a full group** | **0/3** | **0/3** |
| followers drawn (3 needed) | 0 | 1 |

Behaviour changed; the outcome did not. The aggressive attacker publishes every round instead of one
in three, and once it won the leadership comparison, but it never assembled a group. **So the 0/15
result at Hivemind's default is not an artefact of our conservatism** — it is protocol timing. By the
time a 5–7 s advertisement reaches the attacker, there is not enough life left in it for followers to
find and join the undercut before it expires.

This matters for how the finding is stated. The failure at the shipped default is now attributable to
Hivemind, not to a parameter we happened to pick.

**But the result is noisy, and that should be said out loud.** An earlier single run with the same seed
and the same aggressive settings drew **2 followers and cost one honest peer its step**; the sweep run
gave 1 follower and no honest losses. Identical configuration, different outcome. This is a race, and
`HANDOFF.md` §5 already records that race-dependent results vary at this scale. So "the aggressive
attacker can occasionally cause partial disruption at 5–7 s" is supported; any rate for it is not, on
three seeds.

Scope: Stockholm only, deliberately — it is the cheapest origin, so an attack that fails from there
will not do better from Sydney.

---

## 5. How practical is this attack?

**Short answer: the mechanism always works; the timing usually does not — and the deciding factor is
a setting the operator controls.**

### What makes it practical

- **Free.** One identity, no compute, no custody of DHT records.
- **Invisible.** A one-ULP undercut is a legal value. Nothing checks whether a declared deadline is
  reasonable, so there is no misbehaviour to detect.
- **No penalty for failing.** A failed attempt costs nothing; retry next round.
- **Location-independent.** Even the worst-connected region succeeded 3/3. The attacker does not
  need to be well placed.
- **Public inputs.** Advertisements are unsigned and readable by anyone who knows the group key.

### What makes it impractical

- **It is a race the attacker cannot control.** The clock belongs to the honest peers; the attacker
  cannot slow them down, and its own read-then-write costs 2–4 s at WAN-scale latency.
- **Hivemind's shipped default is 5 seconds** (`averager.py:121`; the ALBERT example uses the same).
  That sits inside the band where the attack failed **0/15**, and still fails when the attacker drops
  its own caution to zero.
- **It needs the group key.** A deployment that does not publish its run name is not targetable by
  this method.

### The uncomfortable part — the main thing I want to discuss

Hivemind's own ALBERT documentation tells operators:

> *"In case of high network latency, you may want to increase `--matchmaking_time` by a few seconds"*

That is backwards from a security standpoint. **High latency is what defeats this attack, and the
documented response to high latency is to raise the one setting that restores it.** An operator
following official advice on a globally distributed swarm moves from the 0/15 band into the 15/15
band.

So the honest framing is not "this attack works". It is:

> **Exploitability is governed by a tuning parameter that operators are advised to raise for
> performance reasons.**

That is a more interesting claim, and it points straight at a cheap mitigation — a documented
security ceiling on `matchmaking_time`, or making the deadline not be the sole leadership criterion.

### Open question for the meeting

Is there a defensible upper bound on `matchmaking_time`, or does a real WAN deployment genuinely
need more than 5 s? **We have measured what the attacker needs; we have not measured what honest
peers need.** If honest peers on a WAN really require 10 s+, then the attack is practical in exactly
the deployments this project cares about. That is the obvious next experiment and it is small.

---

## 6. What I would not claim

- This is a **local emulator**, not a real deployment. Delays were injected from measured ping times;
  all processes ran on one machine.
- It does **not** model packet loss, peer churn, DHT keepalive pings, connection setup, or delay
  during the averaging itself.
- The attacker was **given the exact group key**.
- Five regions is a **small spatial sample**, and ICMP ping is a proxy for Hivemind's TCP traffic.
- 3 seeds per cell is small. The result is a clear **threshold effect**, not a success probability.
- One Sydney run (seed 1925) led with 3 followers but its post-write verification event is missing
  from the log, so its timing is averaged over 2 samples rather than 3. The capture itself is sound.

So: *"the attack is latency-sensitive, its cost scales linearly with the attacker's distance, and the
deadline window — not geography — decides the outcome"* — **not** *"the attack succeeds X % of the
time on the public internet"*.

---

## 7. Agenda

1. The 15/15 vs 0/15 table, and why location affects cost but not outcome (10 min)
2. Why 0/15 survives an attacker with no safety margin — the default looks genuinely safe (5 min)
3. The `matchmaking_time` tension — is that the right framing for a paper? (10 min)
4. Next experiment: what deadline window do *honest* peers need on a WAN? (5 min)
5. Where this sits in the four-track queue in `CONTROL_DATA_AUDIT.md` (5 min)
