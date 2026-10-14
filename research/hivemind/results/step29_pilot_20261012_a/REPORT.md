# Step 29 local advertisement-policy pilot

Phase: `pilot`; behavior set: `both`.

This is a local eight-peer loopback comparison with zero injected network delay. The pilot uses one seed and is descriptive; it does not support a significance claim.

Normal behavior allows X to follow another peer. `recruit_only` prohibits X outbound joins; the no-outbound-join integrity check applies only to those cases.

Each row in `choices.csv` is an honest native `step_start`. Its first outgoing destination (X, another peer, or none), accepted native replies, and first assembled roster are reported as separate outcomes.

The X capped lead score assigns 12 seconds to any X step without an own-led assembly, including followed steps. It is a score, not an observed wait for followed attempts. Successful own-led times are summarized separately. The prior default-behavior/high-lead control is a confounded prior control and is excluded from this comparison.

| Case | X-led / starts (%) | Followed / no group / failed | Honest first choice X / other / none | Honest first roster X / other / none | X-led groups (mean roster) | Successful own-led median (s) | Capped lead score mean (s) | All groups |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| normal_native_1s (pooled) | 25/25 (100.0%) | 0/0/0 | 52/87/21 | 72/88/0 | 25 (3.880) | 0.193 | 0.328 | 47 |
| normal_native_2s (pooled) | 25/25 (100.0%) | 0/0/0 | 48/92/21 | 73/88/0 | 25 (3.920) | 0.187 | 0.362 | 47 |
| normal_native_5s (pooled) | 6/24 (25.0%) | 18/0/0 | 16/106/40 | 16/146/0 | 6 (3.667) | 0.749 | 9.393 | 47 |
| normal_renew_1_2 (pooled) | 25/25 (100.0%) | 0/0/0 | 51/88/22 | 73/88/0 | 25 (3.920) | 0.155 | 0.290 | 47 |
| recruit_only_native_1s (pooled) | 25/25 (100.0%) | 0/0/0 | 50/88/22 | 72/88/0 | 25 (3.880) | 0.197 | 0.303 | 47 |
| recruit_only_native_2s (pooled) | 24/24 (100.0%) | 0/0/0 | 47/94/21 | 72/90/0 | 24 (4.000) | 0.105 | 0.298 | 47 |
| recruit_only_native_5s (pooled) | 15/15 (100.0%) | 0/0/0 | 44/88/29 | 45/116/0 | 15 (4.000) | 1.021 | 1.253 | 44 |
| recruit_only_renew_1_2 (pooled) | 25/25 (100.0%) | 0/0/0 | 53/89/21 | 75/88/0 | 25 (4.000) | 0.141 | 0.287 | 47 |

The native expiration audit checks each X publication against `min(max(scheduled_time, publication_time + configured minimum), search_deadline)`. Renewal cadence is checked against the 1-second pre-expiry publication loop. See `groups.csv`, `attempts.csv`, `choices.csv`, and `joins.csv` for raw event references.

Suite integrity pass: `True`.

## Interpretation and comparison

All eight 45-second pilot combinations completed at workload seed 2901. Each used eight private loopback peers, native minimum group size 2 and target 4, honest matchmaking time 5s, and no injected network delay. Raw verification checks 373 groups, 170 X-led groups, 188 X attempts and 1,290 honest attempts; per-attempt, group, and join CSVs match the independent raw reader. No pending or interrupted jobs remain.

The native 1/2/5s arms call the original publication loop with actual min_matchmaking_time. This native parameter also affects default scheduled_time and queue assurance; these are not TTL-only interventions. Recruit-only retains the previous passive search loop: X neither reads competitor ads nor sends outbound joins, and awaits follower-driven assembly. The behavior contrast therefore includes this passive loop.

Normal five-second X led 6/24 attempts and followed in the other 18; recruit-only five-second X led 15/15. That high passive per-attempt success is eventual own-group formation, not the share of all network groups. The latter is 15/44 in this pilot. This explains the older passive-stock 214/220 result, which was 214/597 total network groups.

| Behavior | Policy | X-led / attempts | Successful mean / median to X-led group | Honest first join X / other / none | Honest first roster X / other / no group |
|---|---|---:|---:|---:|---:|
| normal | native_5s | 6/24 (25.0%) | 1.573s / 0.749s | 16 / 106 / 40 | 16 / 146 / 0 |
| normal | native_1s | 25/25 (100.0%) | 0.328s / 0.193s | 52 / 87 / 21 | 72 / 88 / 0 |
| normal | native_2s | 25/25 (100.0%) | 0.362s / 0.187s | 48 / 92 / 21 | 73 / 88 / 0 |
| normal | renew_1_2 | 25/25 (100.0%) | 0.290s / 0.155s | 51 / 88 / 22 | 73 / 88 / 0 |
| recruit_only | native_5s | 15/15 (100.0%) | 1.253s / 1.021s | 44 / 88 / 29 | 45 / 116 / 0 |
| recruit_only | native_1s | 25/25 (100.0%) | 0.303s / 0.197s | 50 / 88 / 22 | 72 / 88 / 0 |
| recruit_only | native_2s | 24/24 (100.0%) | 0.298s / 0.105s | 47 / 94 / 21 | 72 / 90 / 0 |
| recruit_only | renew_1_2 | 25/25 (100.0%) | 0.287s / 0.141s | 53 / 89 / 21 | 75 / 88 / 0 |

Time starts at actual step_start. Successful own-led timing excludes followed attempts; it must not be interpreted as all-attempt wait. Honest choice rates use all honest step starts, including no-outgoing-choice leader attempts. First assembled rosters, ACCEPTED replies, BEGIN_ALLREDUCE receipts and participation evidence remain separately exported. X-led mean group sizes range from 3.667 to 4.0; short-deadline minimum-size groups are retained.

Two-second native normal X and normal (1,2] renewal both led 25/25 attempts and both put 73/161 honest attempts in first X-led rosters. Their successful mean times were 0.362s and 0.290s, and first outgoing X choices were 48/161 and 51/161. Each renewal behavior had only one search with more than one active publication, out of 25. Most attempts therefore measure the initial short deadline rather than a need for renewed availability. These one-seed local counts do not establish a reliable renewal advantage, sustained harm, or a deployment probability.

Native request_timeout stays at its default 3s in every arm. Hivemind warns that it recommends this timeout smaller than min_matchmaking_time; that warning was observed and preserved for the 1/2s settings. This setup is a zero-added-delay native-parameter test, not a recommended WAN configuration.

The first smoke completed before a reporting-only duplicate-key error stopped export. Completed raw/source/provenance and partial exports were preserved; run_step29_policies_v2.py fixes summary metadata merging. Resume skipped that completed trial. Original Step29 runtime/analyzer/workload/test sources remain frozen. Eleven focused checks pass, including a real completed-smoke exporter regression. All eight per-trial raw checks, final CSV checks and offline resume checks pass. Host process check finds no remaining controller or private P2P daemon; safe to suspend.


## Observed renewal signal and grouping time

In normal_renew_1_2/X:14, first ad_started is offset 27.107393s (ref 2556790:5517), and the existing native leader wait captures that first expiration (ref 2556790:5647). Second ad_started is offset 28.107519s (ref 2556790:6066); a two-member group assembles at 28.107960s (ref 2556790:6068), about 0.000441s later and before the first ad expiration. The second declaration call is cancelled on search completion (ad_cancelled at raw line 316464); replica holder_store records later accept the active value, after the group has already assembled.

Code explanation: declaration sets PotentialLeaders.declared_expiration; pop_next_leader can raise TimeoutError when that event invalidates its wait, and _request_join_potential_leaders handles it by assembling when min_group_size is met (native matchmaking.py:165 and :495). The measured immediate group and old wait expiration support this local signal explanation; the grouping gain occurs before renewed DHT availability could help new followers. The passive recruit-only loop does not use pop_next_leader. This is one observed event, not a sustained effect. Do not attribute the entire mean-time contrast to improved advertisement availability.
