# Step 24: X's state and search outcomes

Offline reconstruction of existing private local traces. No new network experiment.

## Implementation status

| Work | Status |
|---|---|
| Trace validation and state reconstruction | Complete |
| Search metrics and deadline comparisons | Complete |
| Count, duration, and provenance reconciliation | Complete |

Input phase: **smoke**. 8 trials; 46 X searches: 35 led, 4 followed, 7 without a group.

## Searches

| Policy | Searches | Led | Followed | No group | Led/search |
|---|---:|---:|---:|---:|---:|
| expire_2 | 6 | 5 | 0 | 1 | 83.3% |
| passive_stock | 4 | 3 | 0 | 1 | 75.0% |
| range_025_125 | 6 | 5 | 0 | 1 | 83.3% |
| range_0_1 | 6 | 5 | 0 | 1 | 83.3% |
| range_1_2 | 6 | 5 | 0 | 1 | 83.3% |
| range_2_3 | 6 | 5 | 0 | 1 | 83.3% |
| range_3_4 | 6 | 5 | 0 | 1 | 83.3% |
| stock | 6 | 2 | 4 | 0 | 33.3% |

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
| expire_2 | 31.8% | 2.5% | 0.0% | 62.0% | 3.7% |
| passive_stock | 61.8% | 2.4% | 0.0% | 32.2% | 3.6% |
| range_025_125 | 31.9% | 2.6% | 0.0% | 61.9% | 3.6% |
| range_0_1 | 32.5% | 1.9% | 0.0% | 61.9% | 3.7% |
| range_1_2 | 32.3% | 2.1% | 0.0% | 62.0% | 3.7% |
| range_2_3 | 31.7% | 2.9% | 0.0% | 61.8% | 3.7% |
| range_3_4 | 32.2% | 2.2% | 0.0% | 61.9% | 3.7% |
| stock | 20.0% | 0.8% | 13.6% | 61.9% | 3.7% |

## Other-led assemblies during submission

| Policy | X searching | X leading | X following | X between rounds | Initial/transition |
|---|---:|---:|---:|---:|---:|
| expire_2 | 0 | 0 | 0 | 4 | 0 |
| passive_stock | 3 | 0 | 0 | 2 | 0 |
| range_025_125 | 0 | 0 | 0 | 4 | 0 |
| range_0_1 | 0 | 0 | 0 | 4 | 0 |
| range_1_2 | 0 | 0 | 0 | 4 | 0 |
| range_2_3 | 0 | 0 | 0 | 4 | 0 |
| range_3_4 | 0 | 0 | 0 | 4 | 0 |
| stock | 0 | 0 | 3 | 4 | 0 |

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
