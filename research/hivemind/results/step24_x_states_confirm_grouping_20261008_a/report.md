# Step 24: X's state and search outcomes

Offline reconstruction of existing private local traces. No new network experiment.

## Implementation status

| Work | Status |
|---|---|
| Trace validation and state reconstruction | Complete |
| Search metrics and deadline comparisons | Complete |
| Count, duration, and provenance reconciliation | Complete |

Input phase: **confirm**. 40 trials; 1184 X searches: 1053 led, 122 followed, 9 without a group.

## Searches

| Policy | Searches | Led | Followed | No group | Led/search |
|---|---:|---:|---:|---:|---:|
| expire_2 | 156 | 155 | 0 | 1 | 99.4% |
| passive_stock | 110 | 106 | 0 | 4 | 96.4% |
| range_025_125 | 156 | 156 | 0 | 0 | 100.0% |
| range_0_1 | 152 | 152 | 0 | 0 | 100.0% |
| range_1_2 | 153 | 151 | 0 | 2 | 98.7% |
| range_2_3 | 152 | 150 | 0 | 2 | 98.7% |
| range_3_4 | 147 | 147 | 0 | 0 | 100.0% |
| stock | 158 | 36 | 122 | 0 | 22.8% |

A search producing an X-led assembly is credited even if that group later fails.
The table counts searches starting within submission windows, including their later outcomes.
Retries have distinct search IDs within the same step; any starting during drain are separate.
These finite local fractions
are not deployment success rates. Existing group counts and tensor diagnostics are unchanged.

## Time within the submission window

Each policy uses equal submission windows. Percentages pool seconds across seeds.
The extra completion time is reported separately in the CSV/JSON outputs.

| Policy | Searching | Leading | Following | Between rounds | Initial/transition |
|---|---:|---:|---:|---:|---:|
| expire_2 | 20.7% | 2.8% | 0.0% | 74.2% | 2.4% |
| passive_stock | 42.4% | 2.3% | 0.0% | 53.0% | 2.3% |
| range_025_125 | 19.4% | 2.4% | 0.0% | 75.8% | 2.3% |
| range_0_1 | 21.2% | 2.4% | 0.0% | 74.0% | 2.3% |
| range_1_2 | 21.6% | 2.5% | 0.0% | 73.5% | 2.3% |
| range_2_3 | 20.7% | 4.5% | 0.0% | 72.5% | 2.3% |
| range_3_4 | 23.6% | 2.6% | 0.0% | 71.5% | 2.3% |
| stock | 10.9% | 0.5% | 10.6% | 75.7% | 2.3% |

## Other-led assemblies during submission

| Policy | X searching | X leading | X following | X between rounds | Initial/transition |
|---|---:|---:|---:|---:|---:|
| expire_2 | 2 | 0 | 0 | 153 | 3 |
| passive_stock | 78 | 0 | 0 | 108 | 3 |
| range_025_125 | 1 | 0 | 0 | 151 | 3 |
| range_0_1 | 3 | 0 | 0 | 153 | 3 |
| range_1_2 | 2 | 0 | 0 | 149 | 5 |
| range_2_3 | 1 | 3 | 0 | 149 | 5 |
| range_3_4 | 10 | 0 | 0 | 149 | 3 |
| stock | 28 | 0 | 93 | 154 | 3 |

## Definitions and limits

Searching starts at a logged search entry and stops at acceptance by another leader,
X's assembly, or search exit. Following includes waiting after ACCEPTED and participating
in the formed group. Leading starts at X's assembly and lasts through observed step
completion or entry into a retry. Advertisement withdrawal and background search exit
do not end Leading. Between rounds starts at observed completion and ends at the next
step submission; it is split into time before training readiness and subsequent waiting.

Initial launch delay and intervals between recorded lifecycle transitions are explicit
residual states. Timings use the common local monotonic clock and logged boundaries;
step completion is observed by harness polling. No sub-event native stage precision is claimed.

The primary time window is [trial start, submission cutoff). Drain ends at the last
observed step result across all peers, excluding process shutdown. Searching outcomes
include later assembly/closure. Other-led assemblies are labeled submission or drain.

Advertisement activity describes local publisher declaration state, independently of role.
Inactive X has no current declared expiration; historical expiration is retained separately.
Signed gap = X expiration minus leader expiration: positive means the leader is earlier.
Deadline comparisons and latest logged follower views are descriptive; the exact queue
dequeue and causal reason for every leader choice were not instrumented.

A potential deadline comparison requires X to be Searching and outside the winner's roster,
both declarations unexpired
at assembly, and matching advertised group keys. It does not establish visibility to every
follower, admission, sufficient followers, or an X loss. Multiple groups can coexist.

## Artifacts

- `timeline.csv`: intervals, roles, independent ad state, source references and period.
- `searches.csv`: search ordinals, outcomes and exact group attribution.
- `other_groups.csv`: every other-led assembly annotated with X's state and both expirations.
- `other_groups_while_searching.csv`: the Searching subset, including drain labels.
- `trial_summary.csv`, `policy_summary.csv`, `analysis.json`: counts and time totals.
- `provenance.json`, `harness/`: hashed inputs and snapshotted extraction code.

Repeat with a fresh label:

```sh
python research/hivemind/analyze_step24_x_states.py <complete-grouping-suite> --label <fresh-label>
```

## Verification notes

Fifteen state tests and twenty-eight existing analyzer/exporter tests passed.
Compilation and whitespace checks passed. CSV counts, state-window coverage,
extraction snapshots and original raw-input hashes were independently reconciled.
See `checks.txt` (and confirmation `validation.json`).

The `duration` column in `searches.csv` is the full native background search
episode from its entry to exit, which can include follower waiting and assembly
cleanup. Searching-only time is computed from the state timeline and reported
separately in the time tables.

## Confirmation interpretation

The five refresh arms led **98.7-100% of X's own recorded searches** using group
assembly as the outcome. Their roughly half-network leadership shares are a
different denominator. Neither observation is a deployment success-rate claim.

Of 1,432 other-led assemblies including drain, X was Between rounds for **1,178**,
Searching for **129**, Following for **94**, Leading for **3**, in initial launch
for **24**, and in transition for **4**. Of the 129 Searching observations, all
28 stock-control groups already included X in the announced roster before the
client acceptance event was logged; those are labeled as membership and excluded
from the potential deadline-comparison flag. The other 101 observations have the
comparison flag set, but are not automatically failed X searches.

The older 1,209 advertisement-inactive observations comprise 1,178 Between-round
observations, 24 initial launch, 4 transition and 3 Leading. Advertisement withdrawal
therefore cannot be used as a stand-in for entering the training delay.
