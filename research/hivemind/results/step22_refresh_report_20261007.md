# Step 22: periodic own-advertisement renewal and leadership

Local eight-peer experiments. Core Hivemind source is unchanged. No public swarm is involved.

## Plan

1. Verify expiration ordering, admission, and follower timeouts against source.
2. Measure local write latency, then compare refresh frequency and lifetime with honest controls.
3. Check repeated contention with independent, seeded simulated training delays.
4. Recalculate results from raw traces and report performance costs and limits.

## Implementation status

| Work | Status |
|---|---|
| Source checks and local latency calibration | Complete |
| Eight-peer frequency and lifetime sweep | Complete: 56 bounded trials |
| Continuous independent peer rounds | Complete: 21 trials on three held-out seeds |
| Persistence and five-member checks | Complete |
| Raw-trace validation and final findings | Complete |

The first sweep exposed degraded tensors and unbounded allreduce waits. Those traces are preserved.
Subsequent runs explicitly use a six-second allreduce timeout and distinguish successful
matchmaking from correctly completed averaging.

## Verified mechanism

An advertisement is stored under `(group_key, PeerID)` with an absolute expiration `E`.
X can supply `E` on each write. A holder only replaces a stored value if the new
expiration is strictly greater. Earlier expiration has higher matchmaking priority,
with PeerID bytes as the tie-breaker. DHT node IDs do not decide this priority.

Let `f` be writes per second, `T = 1/f`, and `d` the remaining lifetime at send time.
The tested renewal rule is:

```text
E_new = max(nextafter(E_previous, +infinity), now + d)
```

The publisher also caps `E_new` at the current step's search deadline. It never uses
a lifetime above five seconds. Keeping `d` small does not mean increasing `E` by
only one floating-point step. With regular sends, `E` advances by approximately `T`.
This sliding expiration can extend availability past the original expiration.

For consecutive writes to overlap at a holder, a useful condition is
`d > T + write_latency + jitter_margin`. This condition concerns record availability.
It does not guarantee leadership, discovery before expiration, or enough followers.
Serial writes can also reduce the achieved frequency when a write takes longer than `T`.

X can refuse outbound joins. The test changes only X's behavior to wait for inbound
followers, while keeping the original admission checks and allreduce. X does not
read competitors' advertisements through the matchmaking API or forge their records.

Setting `min_group_size=4` lets X reject an undersized group when its waiting period
ends. The test also sets `target_group_size=4`. The target must be at least the minimum:
the target-size assembly path otherwise bypasses the minimum-size timeout check.
Five is a supported target, although Hivemind recommends a power of two.

**Refreshing an ad does not reset an already accepted follower's waiting timeout.**
That timeout captures X's declared expiration when the stream starts waiting.
If that timer fires before enough peers have joined, the stock leader disbands the
group even if X has since published a later expiration. This is a main flaw in the
idea that continuous advertising alone can hold a growing group together.

Source: [`declare_averager`](../../../hivemind/averaging/key_manager.py),
[`TimedStorage.store`](../../../hivemind/utils/timed_storage.py), and
[`pop_next_leader`, `rpc_join_group`, and `_check_reasons_to_reject`](../../../hivemind/averaging/matchmaking.py).

## Setup and calibration

Eight loopback DHT peers use ordinary, seeded DHT positions. X is one of the eight.
The DHT uses its default five replicas. These tests do not use forged positions,
public peers, packet loss, or a WAN network model.

Honest peers keep the five-second default matchmaking window, two-member minimum,
and three-second join-request timeout. All peers use a four-member target and
empty group bits, so repeated steps remain under the same aggregation key. Those
two choices are experiment settings, not claims about every Hivemind deployment.
No explicit `scheduled_time` is passed. Steps have a twelve-second matchmaking
deadline and a six-second allreduce limit. All peers use the same averager class.

Calibration used twenty writes across eight local DHT peers. The measured remote
holder receipt latency had median **9.37 ms**, p95 **15.76 ms**, and maximum **18.96 ms**.
The median high-level store duration was **10.99 ms**. The guard is p95 plus 20 ms,
or **35.76 ms**. These are local measurements, not Internet latency estimates.

For the coupled grid, `d = 1/f + guard + spare`, with spare values 0.05 and 0.5 seconds.
A second comparison holds `d=1.25 s` fixed while changing `f`.

## One grouping wave

Four timing scenarios per policy use seeds 2202 and 2203 with two arrival patterns.
Staggered starts are independent uniform draws over three seconds. Bursty starts
put 70% of honest peers in the first 0.3 seconds and the rest between 1.5 and 3 seconds.
X's start is independently uniform over three seconds. These are workload models,
not measured training schedules. Scenarios are reused across policies, with fresh
networks each time. Peer IDs and runtime scheduling are not deterministic.

The stock control can follow other leaders and uses the default two-member minimum.
The passive control refuses to follow, requires four members, and uses the stock
five-second publisher. Other attack arms keep the same passive behavior and minimum.

| Policy | `f` (/s) | `d` (s) | Verified X-led groups / scenarios | Honest completed / steps | Active ad writes |
|---|---:|---:|---:|---:|---:|
| Stock | At expiration | 5 | 3/4 | 28/28 | 4 |
| Passive, stock publisher | At expiration | 5 | 3/4 | 28/28 | 5 |
| Renew only at expiration | At expiration | 1.25 | 4/4 | 28/28 | 7 |
| Refresh | 0.5 | 2.085764 | 4/4 | 28/28 | 5 |
| Refresh | 0.5 | 2.535764 | 4/4 | 28/28 | 5 |
| Refresh | 1 | 1.085764 | 4/4 | 28/28 | 10 |
| Refresh | 1 | 1.535764 | 4/4 | 28/28 | 9 |
| Refresh | 2 | 0.585764 | 4/4 | 28/28 | 15 |
| Refresh | 2 | 1.035764 | 4/4 | 28/28 | 14 |
| Refresh | 4 | 0.335764 | 4/4 | 28/28 | 28 |
| Refresh | 4 | 0.785764 | 3/4 | 27/28 | 63 |
| Fixed lifetime | 1 | 1.25 | 4/4 | 28/28 | 9 |
| Fixed lifetime | 2 | 1.25 | 4/4 | 28/28 | 14 |
| Fixed lifetime | 4 | 1.25 | 4/4 | 28/28 | 27 |

All 56 final trials had no incorrect returned tensors, stuck stages, forbidden
attacker reads, or forbidden attacker outbound joins. One honest step failed to
find a group. Four-point counts are not deployment success rates.

The cheapest successful refresh setting in this sample is **one write every two
seconds with a 2.086-second lifetime**. Faster writes brought no extra wins in these
four scenarios. Renewing only at expiration also won all four scenarios, so these
results do not establish that overlapping periodic renewal caused the leadership gain.

The `f=4, d=0.785764` failure had **24 ACCEPTED replies followed by 24 GROUP_DISBANDED
replies** and no assembled X group. Its advertisement remained renewed in the logged
remote-holder intervals. Availability and group assembly are separate requirements.

The expiration-only 1.25-second policy had 14 logged interior holder gaps.
The periodic refresh arms had none in these traces. Gap counts come from intervals
between accepted remote stores. They exclude initial propagation, final expiration,
and X's local self-store, and are not a proof of uninterrupted availability everywhere.

## Repeated rounds and persistence

Seven policies use three new seeds, 2211–2213.
Each accepts new steps for thirty seconds, then drains already started steps.
Honest peers return after independent lognormal simulated training delays with mean
1.5 seconds and coefficient of variation 0.6, clipped to 0.1–4 seconds. X returns
after 0.05 seconds in every policy, including controls. This deliberately gives X
more opportunities than honest peers and must not be confused with an ad-only effect.
The stock control uses the same four-member minimum as the passive and refresh arms.

Every completed group is checked against its gathered contributor identities,
step numbers, input values, and every member's output tensor. A successful future
alone is not counted as verified averaging.

Counts include the drain after each thirty-second submission window. The totals
below cover three windows per policy. They are local measurements, not estimated
long-run probabilities or requests-per-second figures.

| Policy | `f` (/s) | `d` (s) | Verified X-led / all verified groups | Verified honest steps / attempted | Active ad writes |
|---|---:|---:|---:|---:|---:|
| Stock, minimum four | At expiration | 5 | 67/113 | 339/339 | 115 |
| Passive, stock publisher | At expiration | 5 | 75/101 | 327/328 | 86 |
| Renew only at expiration | At expiration | 1.25 | 106/108 | 326/329 | 143 |
| Refresh | 0.5 | 2.085764 | 104/108 | 328/330 | 117 |
| Refresh | 1 | 1.085764 | 96/103 | 320/322 | 138 |
| Refresh | 2 | 0.585764 | 104/106 | 322/326 | 242 |
| Refresh | 4 | 0.335764 | 107/110 | 331/331 | 393 |

Five completed steps returned incorrect tensors: one in the 0.5/s arm, three in
the 1/s arm, and one in the 2/s arm. Groups containing those steps are not counted
as verified groups. The traces establish the failures, but not their causal link
to advertisement policy. There were no forbidden attacker reads or outbound joins.

The 4/s policy produced three more verified X-led groups than the 0.5/s policy while
making 3.36 times as many active ad writes. Three workload seeds cannot establish
that this small difference is a repeatable gain. The expiration-only short-lifetime
policy produced 106 groups with 143 writes, so periodic overlap is not necessary
for high leadership counts in this workload. Overlap is relevant to ad continuity.

**Practical tested choice:** if continuity matters and write cost matters, start with
`f=0.5/s, d≈2.086 s` on this local setup. If the goal is only the highest observed
group count, 4/s had the largest count in this sample, at much greater write cost.
Neither is a proven global optimum. Frequencies below 0.5/s were not tested.
Under different latency, recompute the guard and check follower waiting behavior.

The 0.5/s policy sent nine `GROUP_DISBANDED` replies across these runs, compared with
300 for the 4/s policy. These are replies on join streams, not counts of distinct
groups. The shortest lifetimes increased join churn without a comparable increase
in verified leadership.

The ad counts are high-level active write attempts. Withdrawals, routing walks,
dictionary transfers, and other DHT traffic add costs. The raw traces also record
remote store receipts, but do not measure total network bytes.

### Persistence with a fixed storage delay

Each policy writes its own advertisement for thirty seconds across eight DHT peers,
with a fixed 40 ms delay added to remote `rpc_store` handling. This is storage-delay
emulation, not WAN-delay emulation. Actual remote receipt latency was approximately
53 ms at the median. An independent honest peer probes the key using `latest=True`
every 0.1 seconds. This test does not perform matchmaking.

| `f` (/s) | `d` (s) | Accepted writes | Active probes | Active probes after initial expiry | Logged interior holder gaps |
|---:|---:|---:|---:|---:|---:|
| 0.5 | 2.085764 | 15/15 | 292/292 | 271/271 | 0 |
| 2 | 0.585764 | 60/60 | 295/295 | 289/289 | 0 |
| 2 | 0.2 | 60/60 | 80/295 | 78/293 | 236 |

All deadlines increased strictly. The safe policies kept the record visible at all
probes well past its initial expiration. The insufficient-lifetime control had many
gaps despite every write being accepted. This verifies a thirty-second observation,
not an indefinite availability guarantee. Holder intervals exclude local self-stores.

### Five-member minimum

With native `min_group_size=5` and `target_group_size=5`, X led **nine verified groups,
all of size five**, in a twelve-second submission window on seed 2211, followed by
drain. The policy was `f=1/s, d=1.085764 s`. All 36 completed honest steps had correct
tensors, and one other honest step failed to find a group. There were no degraded
steps, forbidden reads, or attacker outbound joins. This establishes that waiting
for five members is possible in this setup, not that it always succeeds.

## Reproduction and artifacts

Primary scripts: [`step22_refresh_leadership.py`](../step22_refresh_leadership.py),
[`step22_continuous.py`](../step22_continuous.py), and
[`step22_endurance.py`](../step22_endurance.py). Analysis scripts:
[`analyze_step22.py`](../analyze_step22.py) and
[`analyze_step22_continuous.py`](../analyze_step22_continuous.py), plus
[`analyze_step22_endurance.py`](../analyze_step22_endurance.py).

Run with a working Hivemind environment. Each output directory must be new.
The final run directories include the case lists, seeds, provenance, and per-trial
configs and raw JSONL traces. Matching harness snapshots are included for final runs.
Provenance checks eight relevant runtime source files byte-for-byte against the
repository and records their SHA-256 digests, Python and Torch versions, the Git
commit, and the p2pd binary digest. The source commit is
`937090e2a5659d49195a6759cf6bbcda5bb593b1`.

```bash
python research/hivemind/step22_refresh_leadership.py suite \
  --cases /path/to/cases.json --seeds 2202 2203 --patterns staggered bursty \
  --output research/hivemind/results/NEW_LABEL

python research/hivemind/analyze_step22.py \
  research/hivemind/results/step22_bounded_sweep_20261007_a \
  --output /tmp/step22_checked.json
```

Calibration: [`step22_calibration_20261007_c`](step22_calibration_20261007_c).
Final grouping grid: [`step22_bounded_sweep_20261007_a`](step22_bounded_sweep_20261007_a)
and [`recalculated results`](step22_bounded_analysis_20261007.json).
Final continuous rounds: [`step22_continuous_20261007_a`](step22_continuous_20261007_a).
Recalculated continuous results: [`step22_continuous_analysis_20261007.json`](step22_continuous_analysis_20261007.json).
Persistence traces: [`step22_endurance_20261007_a`](step22_endurance_20261007_a), with
[`recalculated observations`](step22_endurance_analysis_20261007.json).
Five-member traces: [`step22_five_members_20261007_a`](step22_five_members_20261007_a),
with [`recalculated results`](step22_five_members_analysis_20261007.json).
The [`runtime package list`](step22_runtime_packages_20261007.txt) records the temporary
Python environment used for these runs. Python was 3.14.7 with Torch 2.14.1+cpu.
Earlier `step22_sweep_20261007_a/b` runs are diagnostic: they include degraded tensors
and an unbounded transfer wait. The first continuous smoke run had a tracing-wrapper
bug and is excluded. The corrected smoke run passes its raw-data and tensor checks.

## Limits

These are standalone averager tests with tiny tensors, not full model training.
They do not reproduce Optimizer-driven epoch scheduling, a public multi-bucket swarm,
heterogeneous hardware, WAN loss, churn, or long-running outages. A finite persistence
run cannot prove indefinite contention or eventual leadership. There is no universal
optimal `f,d` established here: latency and follower arrivals change the trade-off.
