# Step 29 local advertisement-policy pilot

Phase: `smoke`; behavior set: `both`.

This is a local eight-peer loopback comparison with zero injected network delay. The pilot uses one seed and is descriptive; it does not support a significance claim.

Normal behavior allows X to follow another peer. `recruit_only` prohibits X outbound joins; the no-outbound-join integrity check applies only to those cases.

Each row in `choices.csv` is an honest native `step_start`. Its first outgoing destination (X, another peer, or none), accepted native replies, and first assembled roster are reported as separate outcomes.

The X capped lead score assigns 12 seconds to any X step without an own-led assembly, including followed steps. It is a score, not an observed wait for followed attempts. Successful own-led times are summarized separately. The prior default-behavior/high-lead control is a confounded prior control and is excluded from this comparison.

| Case | X-led / starts (%) | Followed / no group / failed | Honest first choice X / other / none | Honest first roster X / other / none | X-led groups (mean roster) | Successful own-led median (s) | Capped lead score mean (s) | All groups |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| normal_native_1s (pooled) | 6/6 (100.0%) | 0/0/0 | 14/13/3 | 18/12/0 | 6 (4.000) | 0.285 | 0.346 | 9 |
| normal_renew_1_2 (pooled) | 6/6 (100.0%) | 0/0/0 | 13/15/3 | 18/12/1 | 6 (4.000) | 0.216 | 0.261 | 9 |

The native expiration audit checks each X publication against `min(max(scheduled_time, publication_time + configured minimum), search_deadline)`. Renewal cadence is checked against the 1-second pre-expiry publication loop. See `groups.csv`, `attempts.csv`, `choices.csv`, and `joins.csv` for raw event references.

Suite integrity pass: `True`.
