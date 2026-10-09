# Step 24 rerun: control-plane grouping and leadership

User scope update: measure group assembly and leadership. Tensor correctness is a
recorded diagnostic and no longer stops the sweep or excludes a group from the
primary counts. The earlier strict-averaging runs remain preserved separately.

## Implementation status

| Work | Status |
|---|---|
| Outcome mode and independent control-plane counting | Complete; 22 harness/analyzer and 14 exporter checks pass |
| Eight-arm smoke, seed 2400, ten seconds each | Complete, 8/8 control-plane checks pass |
| Five-seed confirmation, sixty seconds per arm | Complete; 40/40 control-plane checks pass |
| Raw counts, pooled table, and handoff | Complete; independent recount and export agree |

## Method and timing

Eight private loopback peers: X and H0-H6. All peers have independently seeded
simulated training delays with the same distribution. Honest minimum two, X minimum
four, target four; X follows only in the stock baseline. No injected network delay,
competitor reads by passive X, core edits, or public swarm.

```
S[i,k+1] = C[i,k] + D[i,k] + epsilon[i,k]
sigma = sqrt(log(1 + 0.6**2))
mu = log(1.5) - sigma**2 / 2
D[i,k] = clip(exp(mu + sigma*Z[i,k]), 0.1, 4), Z ~ Normal(0,1)
E[n] = min(search_end, max(nextafter(E[n-1], +infinity), publish_time[n] + U))
Nominal remaining lifetime between 1 Hz sends: (U - 1, U]
```

Delay is added after an attempt reaches native FINISHED and before the next
`step()` call. Native averaging still runs to release each peer for its next round;
its latency/failure/retry behavior therefore affects subsequent availability.
Group counts do not depend on correct averaging. This is a standalone-averager
closed-loop workload, not isolated matchmaking throughput or full model training.

Policies: (0,1], (0.25,1.25], (1,2], (2,3], (3,4] seconds at 1 Hz; expiration-only
2 seconds; passive stock 5-second publisher; stock publisher/following control.
Nominal intervals can deviate at remote readers due to propagation, scheduling,
and search-deadline capping. All arms reuse seeds and per-peer delay sequences;
actual completion and arrival times may differ. Fresh peer IDs are logged.

Smoke seed 2400: eight ten-second submission windows and bounded drain.
Confirmation seeds 2401-2405: eight policies per seed, sixty-second windows and
bounded drain. Each policy totals five windows / 300 seconds of submission time.
Counts include drain. Execution order shuffled with seed 240024.

## What is counted

Primary: distinct native leader `assembled` group IDs across all peers, and the
subset led by X. Validate leader identity and roster from raw traces; deduplicate
by group ID. Assembly records do not guarantee all followers received the final
announcement or completed averaging. Tensor failures and overlapping candidate
rosters do not erase leader announcements. Average X-led size uses assembled
rosters. Leadership share = X-led assembled / all assembled.

Trace loss, conflicting group IDs/leaders, invalid roster identities, broken delay
schedules, infrastructure failures, or incomplete drain remain failures of the
measurement. Tensor mismatches alone do not halt grouping-mode execution. The CLI
records `--outcome grouping` in both trial and suite configuration; smoke and
confirmation must use the same outcome, so an old strict-averaging smoke cannot
silently qualify a new control-plane run.

## Per-group winner and deadline record

The user also requested every formed group's leader and deadline. The existing
raw trace logs `assembled` with leader PeerID and group ID, and `ad_started` for
all peers with absolute ad expiration. Both native and research publishers set
`declared_expiration_time` immediately before calling the traced publisher.
Reconstruct declaration state in the same process's `event_number` order and
attach the last active declaration preceding each assembly. A subsequent inactive
announcement represents cleared declaration state and must not be mistaken for an
active deadline. This export does not change or interrupt the running harness.

Export columns include policy, seed, group ID, leader name/PeerID, step ID,
assembly timestamp, declared absolute expiration, remaining seconds = expiration
minus assembly timestamp, roster, and source event references. Last successful
active store expiration is recorded separately where available. X's current active
declaration is reconstructed at the same assembly monotonic timestamp. Inactive or
never-declared X has a null current expiration. Report `expiration_gap_x_minus_leader`
= X expiration minus leader expiration: positive means the leader's deadline is
earlier, negative means X's is earlier. Negative remaining lifetime is retained
when an active declaration has expired. X-in-roster, current/latest step ID, and
both declaration group keys are retained.

For every matching client `BEGIN_ALLREDUCE` reply, export the latest logged DHT
view before that follower's matching handshake: leader/X observed expirations,
X visibility, and their observed gap. This is a bounded view correlation, not a
record of the exact queue dequeue. Missing views are labeled unavailable. Only
followers with a matching client reply contribute these view observations; missing
client replies do not remove a leader assembly from the primary count. The leader's
current declared expiration may differ from what an earlier follower DHT read saw
or what an already-started follower wait captured. This records local leader
assemblies, not a global network election. `node_id` here refers to the matchmaking
PeerID; a DHT routing ID is a different identifier.

## Artifacts and reproduction

- Smoke raw traces/configurations/provenance: `step24_smoke_grouping_20261008_a/`.
- Confirmation raw traces/configurations/provenance: `step24_confirm_grouping_20261008_a/`.
- Election/deadline CSV and JSON: `step24_elections_grouping_20261008_a/`.
- `confirm_partial16/` is a preserved intermediate export of 16 completed trials,
  **not** the final comparison. Smoke contains 79 assembly records.

The harness and independent analyzer are snapshotted and hashed in each suite.
The election exporter and focused tests are independently snapshotted and hashed
in the export directory. Runtime package versions and core source hashes are
preserved in the suite provenance. No core source changes were required.

Commands used (private local source-verified runtime):

```sh
PYTHONPATH=/tmp/hm-step23-runtime /tmp/hm-step23-env/bin/python research/hivemind/step24_lifetime_sweep.py --phase smoke --outcome grouping --label grouping_20261008_a
PYTHONPATH=/tmp/hm-step23-runtime /tmp/hm-step23-env/bin/python research/hivemind/step24_lifetime_sweep.py --phase confirm --outcome grouping --gate research/hivemind/results/step24_smoke_grouping_20261008_a --label grouping_20261008_a
python research/hivemind/export_step24_elections.py research/hivemind/results/step24_confirm_grouping_20261008_a --output-dir research/hivemind/results/step24_elections_grouping_20261008_a/confirm
```

Use a fresh label to repeat the network run; existing suite directories are
preserved. Independent raw analysis uses `analyze_step24_lifetime.py` and accepts
no tensor mismatches as a control-plane exclusion in grouping mode.

## Confirmation results

All forty held-out trials completed. Independent raw reconstruction exactly agrees
with the saved suite analysis and the election export: **2,485 distinct leader
assemblies, 1,053 led by X**, zero control integrity errors and zero pending steps.
Eight peers; each policy has five sixty-second submission windows, plus drain.
All X-led assembled groups have four members. These are assembly counts, regardless
of tensor outcome.

| Policy | All assembled | X-led | Honest-led | X share | X-led per seed (min-max) | Accepted X ad writes |
|---|---:|---:|---:|---:|---:|---:|
| Stock following | 317 | 36 | 281 | 11.4% | 5-11 | 131 |
| Passive stock (5s native publisher) | 297 | 106 | 191 | 35.7% | 18-25 | 115 |
| (0,1] at 1 Hz | 311 | 152 | 159 | 48.9% | 25-34 | 160 |
| (0.25,1.25] at 1 Hz | 315 | 156 | 159 | 49.5% | 26-33 | 157 |
| (1,2] at 1 Hz | 307 | 151 | 156 | 49.2% | 25-33 | 181 |
| (2,3] at 1 Hz | 311 | 150 | 161 | 48.2% | 26-33 | 178 |
| (3,4] at 1 Hz | 312 | 147 | 165 | 47.1% | 24-33 | 155 |
| Expiration-only (2s) | 315 | 155 | 160 | 49.2% | 27-33 | 158 |

The five 1 Hz refresh ranges yielded 147-156 X-led assemblies. The shortest
(0,1] range did not have the highest count. (0.25,1.25] had the highest observed
count, 156; expiry-only had 155 and (1,2] at 1 Hz had 151. This small local sample
does not establish a reliable best range. All refresh arms and expiry-only exceeded
passive stock in observed X-led counts. Passive stock holds refusal-to-follow fixed;
the stock-following control additionally changes X's willingness to join other leaders.

## Deadline comparisons when another peer led

At each assembly, compare the winner's local declared expiration with X's local
declared expiration at the same monotonic timestamp. An inactive declaration has
a null current expiration; a separately retained historical expiration is not a
current deadline. These are local publisher states, not guarantees of every remote
DHT reader's state.

| Policy | Other-led groups | X ad inactive | X active, expired | X live, leader earlier | X live, X earlier |
|---|---:|---:|---:|---:|---:|
| Stock following | 281 | 159 | 0 | 121 | 1 |
| Passive stock (5s native publisher) | 191 | 111 | 0 | 79 | 1 |
| (0,1] at 1 Hz | 159 | 156 | 0 | 0 | 3 |
| (0.25,1.25] at 1 Hz | 159 | 158 | 0 | 0 | 1 |
| (1,2] at 1 Hz | 156 | 154 | 0 | 0 | 2 |
| (2,3] at 1 Hz | 161 | 159 | 0 | 0 | 2 |
| (3,4] at 1 Hz | 165 | 155 | 0 | 10 | 0 |
| Expiration-only (2s) | 160 | 157 | 0 | 0 | 3 |

Across all arms, 1,209 of 1,432 other-led assemblies occurred while X's local
declaration was inactive. Of the remaining 223, the leader had the earlier deadline
in 210 and X had the earlier deadline in 13; none had an active expired X declaration
at assembly. This describes state at assembly and does not identify a causal reason
for each group. A later renewal can change this comparison after a follower joined.

Eight peers can form separate groups of four. Another peer leading a group while
X is assembling or finishing its own group is not a failed X election. Earlier
expiration is a priority rule among eligible candidates visible to a follower;
it does not ensure global visibility, admission, or enough followers. Matching
client replies preserve the latest logged DHT view before each follower handshake
so the assembly-time gap and earlier observed gap can be inspected separately.

[Every group: winner PeerID, leader/X expirations, signed gap, roster, and follower views](step24_elections_grouping_20261008_a/confirm/elections.csv).
[JSON with source event references](step24_elections_grouping_20261008_a/confirm/elections.json).
[Per-seed counts](step24_elections_grouping_20261008_a/confirm/counts.csv),
[pooled counts and group-size averages](step24_elections_grouping_20261008_a/confirm/pooled_counts.csv),
[deadline categories and median signed gaps](step24_elections_grouping_20261008_a/confirm/deadline_comparison.csv),
[independent raw analysis](step24_elections_grouping_20261008_a/confirm/independent_analysis.json),
[raw input hash manifest](step24_elections_grouping_20261008_a/confirm/input_sha256.json).

## Two inspectable deadline examples

- `(3,4]`, seed 2401, group `180d3b42f369af034eb30e14cda2d2cef3c3c2fd`:
  H0 led with expiration **1791471090.003183**; X's expiration was
  **1791471090.3155932**. H0 was earlier by **0.312410s**. H5's latest logged
  view before joining contained both expirations with that ordering; the other
  two confirmed followers' latest views did not contain X.
- `(1,2]`, seed 2401, group `91f4467f07b2cd0e6d67dc35c59cf744ac7353af`:
  H2 led with expiration **1791470411.2148166**; X's expiration was
  **1791470408.9037192**. X was earlier by **2.311097s** at assembly. All three
  confirmed followers' latest logged views before joining H2 lacked X. This
  illustrates why the assembly-time deadline gap alone cannot explain selection.

Full PeerIDs, timestamps, group keys, and event references are in the election
export. These are traced local examples, with the same latest-view correlation
limits stated above.

Thirty-six focused checks passed: fourteen analyzer, eight harness, and fourteen
election exporter checks. Compilation and whitespace checks passed. Core source
remains unchanged. Raw configurations, seeds, traces, snapshots, and earlier halted
strict-averaging runs are preserved.

## Completed offline reconstruction of X's state

No additional network experiment was run. The existing smoke and confirmation
traces were reconstructed with `analyze_step24_x_states.py`. Fifteen focused state
tests plus twenty-eight existing analyzer/exporter tests pass. Original input
hashes and existing assembly counts are unchanged. Both the eight-trial smoke and
forty-trial confirmation pass complete state-duration and search-attribution checks.

[State analysis: definitions, tables, and artifacts](step24_x_states_confirm_grouping_20261008_a/report.md).
[Per-policy flat columns](step24_x_states_confirm_grouping_20261008_a/policy_summary.csv),
[per-trial counts and durations](step24_x_states_confirm_grouping_20261008_a/trial_summary.csv),
[per-search records](step24_x_states_confirm_grouping_20261008_a/searches.csv),
[other groups formed while X was Searching, including deadlines](step24_x_states_confirm_grouping_20261008_a/other_groups_while_searching.csv).

| Policy | X-led searches / searches | X-led/search | Searching time | Between-round time | Other groups while Searching (submission / drain) |
|---|---:|---:|---:|---:|---:|
| Stock following | 36/158 | 22.8% | 10.9% | 75.7% | 28 / 0 |
| Passive stock | 106/110 | 96.4% | 42.4% | 53.0% | 78 / 2 |
| (0,1], 1 Hz | 152/152 | 100.0% | 21.2% | 74.0% | 3 / 0 |
| (0.25,1.25], 1 Hz | 156/156 | 100.0% | 19.4% | 75.8% | 1 / 0 |
| (1,2], 1 Hz | 151/153 | 98.7% | 21.6% | 73.5% | 2 / 0 |
| (2,3], 1 Hz | 150/152 | 98.7% | 20.7% | 72.5% | 1 / 1 |
| (3,4], 1 Hz | 147/147 | 100.0% | 23.6% | 71.5% | 10 / 0 |
| Expiration-only, 2s | 155/156 | 99.4% | 20.7% | 74.2% | 2 / 1 |

Time percentages use each trial's first sixty seconds. The remaining time is
Leading, Following, initial launch or transition; all durations reconcile to the
window. Between rounds is split into scheduled training delay and later waiting.
Search outcomes include later assembly, and retries have separate IDs. No searches
started during drain in this confirmation.

Across all other-led assemblies including drain, X was Between rounds in **1,178**,
Searching in **129**, Following in **94**, Leading in **3**, initially waiting in
**24**, and in transition in **4**. The earlier 1,209 inactive-ad observations do
not all mean Between rounds: they include the three Leading observations.

All 28 stock-control groups formed while X was observed Searching already included
X in their roster, before its client acceptance was logged. Those rows remain in
the export with explicit membership and are excluded from potential deadline
competition. The other 101 Searching observations remain descriptive comparisons,
not automatic X losses; X may subsequently assemble a different group.
