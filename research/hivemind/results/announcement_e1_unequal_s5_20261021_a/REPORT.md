**Suite run status:** suite status is not completed: {'status': 'review_required', 'completed': ['baseline4'], 'seed': 5, 'failed': 'baseline4'}

# Hivemind inconsistent-announcement probe

This report analyzes local native Hivemind runs. It distinguishes a completed, numerically supported exclusion from an error, missing result, or local fallback.

## Matchmaking and aggregation rosters

B's assembled view is shown separately from its local aggregation view because the membership condition changes B's roster after assembly.

| Condition | B assembled | B aggregate | D announced / aggregate | A announced / aggregate | C announced / aggregate |
|---|---|---|---|---|---|
| baseline4 | B,D,A,C | B,D,A,C | announce [B,D,A,C] / aggregate [B,D,A,C] | announce [B,D,A,C] / aggregate [B,D,A,C] | announce [B,D,A,C] / aggregate [B,D,A,C] |

| Condition | Evidence valid | Active nodes | Local-roster means match | Streams complete | A cleanly excluded | Coordinate mismatch | Fallback evidence | Evidence errors |
|---|---:|---|---:|---:|---|---|---|---:|
| baseline4 | False | B, D, A, C | True | True | — | — | — | 4 |

## baseline4

Evidence gaps:
- A: expected NODE mode for every roster member, got [1, 0, 0, 0]
- B: expected NODE mode for every roster member, got [1, 0, 0, 0]
- C: expected NODE mode for every roster member, got [1, 0, 0, 0]
- D: expected NODE mode for every roster member, got [1, 0, 0, 0]

| Node | Operation status | Local-roster mean | Stream proof | Applied deltas | Origins observed | Row coverage | Coordinates correct | Fallback | A cleanly excluded | Announcement/aggregation to terminal (s) | Agrees with | Max mean error |
|---|---|---:|---:|---:|---|---|---:|---:|---:|---:|---|---:|
| B | success | True | True | True | B, D, A, C | B:48, D:48, A:48, C:48 | True | False | False | 0.026 | D, A, C | 0 |
| D | success | True | True | True | B, D, A, C | B:48, D:48, A:48, C:48 | True | False | False | 0.031 | B, A, C | 0 |
| A | success | True | True | True | B, D, A, C | B:48, D:48, A:48, C:48 | True | False | False | 0.031 | B, D, C | 0 |
| C | success | True | True | True | B, D, A, C | B:48, D:48, A:48, C:48 | True | False | False | 0.031 | B, D, A | 0 |

Timeline (seconds are the recorded event times):

- `0.767372s` **step_started** B
- `0.775245s` **leader_advertised** B
- `0.837137s` **step_started** C
- `0.843765s` **join_accepted** B follower=C
- `0.871353s` **step_started** D
- `0.874327s` **step_started** A
- `0.878259s` **join_accepted** B follower=D
- `0.886199s` **join_accepted** B follower=A
- `0.886497s` **leader_assembled** B members=['B', 'D', 'A', 'C']
- `0.886720s` **announcement_sent** B → C members=['B', 'D', 'A', 'C']
- `0.886855s` **announcement_sent** B → D members=['B', 'D', 'A', 'C']
- `0.886974s` **announcement_sent** B → A members=['B', 'D', 'A', 'C']
- `0.886976s` **announcement_received** C members=['B', 'D', 'A', 'C']
- `0.887078s` **announcement_received** D members=['B', 'D', 'A', 'C']
- `0.887184s` **announcement_received** A members=['B', 'D', 'A', 'C']
- `0.891046s` **local_roster** A members=['B', 'D', 'A', 'C']
- `0.891048s` **local_roster** C members=['B', 'D', 'A', 'C']
- `0.891104s` **local_roster** D members=['B', 'D', 'A', 'C']
- `0.891722s` **local_roster** B members=['B', 'D', 'A', 'C']
- `0.917525s` **averaging_terminal** B status=success
- `0.917582s` **averaging_terminal** C status=success
- `0.917785s` **averaging_terminal** D status=success
- `0.917830s` **averaging_terminal** A status=success

## Interpretation limits

The report covers the exact local process runs and tensor values saved in these artifacts. It does not estimate attack probability, Internet performance, or model-convergence impact. A failed or partial aggregation is not evidence that a participant was cleanly excluded.
