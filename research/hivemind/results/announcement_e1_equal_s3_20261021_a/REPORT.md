# Hivemind inconsistent-announcement probe

This report analyzes local native Hivemind runs. It distinguishes a completed, numerically supported exclusion from an error, missing result, or local fallback.

## Matchmaking and aggregation rosters

B's assembled view is shown separately from its local aggregation view because the membership condition changes B's roster after assembly.

| Condition | B assembled | B aggregate | D announced / aggregate | A announced / aggregate | C announced / aggregate |
|---|---|---|---|---|---|
| baseline4 | B,D,A,C | B,D,A,C | announce [B,D,A,C] / aggregate [B,D,A,C] | announce [B,D,A,C] / aggregate [B,D,A,C] | announce [B,D,A,C] / aggregate [B,D,A,C] |
| control3 | B,D,C | B,D,C | announce [B,D,C] / aggregate [B,D,C] | no request / idle | announce [B,D,C] / aggregate [B,D,C] |
| membership | B,D,A,C | B,D,C | announce [B,D,C] / aggregate [B,D,C] | announce [B,D,A,C] / aggregate [B,D,A,C] | announce [B,D,C] / aggregate [B,D,C] |
| ordering | B,D,A,C | B,D,A,C | announce [B,D,A,C] / aggregate [B,D,A,C] | announce [A,D,B,C] / aggregate [A,D,B,C] | announce [B,D,A,C] / aggregate [B,D,A,C] |

| Condition | Evidence valid | Active nodes | Local-roster means match | Streams complete | A cleanly excluded | Coordinate mismatch | Fallback evidence | Evidence errors |
|---|---:|---|---:|---:|---|---|---|---:|
| baseline4 | True | B, D, A, C | True | True | — | — | — | 0 |
| control3 | True | B, D, C | True | True | B, D, C | — | — | 0 |
| membership | True | B, D, A, C | False | False | B, D, C | — | A | 0 |
| ordering | True | B, D, A, C | False | True | — | B, D, A, C | — | 0 |

## baseline4

| Node | Operation status | Local-roster mean | Stream proof | Applied deltas | Origins observed | Row coverage | Coordinates correct | Fallback | A cleanly excluded | Announcement/aggregation to terminal (s) | Agrees with | Max mean error |
|---|---|---:|---:|---:|---|---|---:|---:|---:|---:|---|---:|
| B | success | True | True | True | B, D, A, C | B:48, D:48, A:48, C:48 | True | False | False | 0.025 | D, A, C | 0 |
| D | success | True | True | True | B, D, A, C | B:48, D:48, A:48, C:48 | True | False | False | 0.028 | B, A, C | 0 |
| A | success | True | True | True | B, D, A, C | B:48, D:48, A:48, C:48 | True | False | False | 0.028 | B, D, C | 0 |
| C | success | True | True | True | B, D, A, C | B:48, D:48, A:48, C:48 | True | False | False | 0.028 | B, D, A | 0 |

Timeline (seconds are the recorded event times):

- `0.732894s` **step_started** B
- `0.743549s` **leader_advertised** B
- `0.824853s` **step_started** D
- `0.831343s` **join_accepted** B follower=D
- `0.965479s` **step_started** C
- `0.971572s` **join_accepted** B follower=C
- `0.985354s` **step_started** A
- `0.998038s` **join_accepted** B follower=A
- `0.998260s` **leader_assembled** B members=['B', 'D', 'A', 'C']
- `0.998422s` **announcement_sent** B → D members=['B', 'D', 'A', 'C']
- `0.998543s` **announcement_sent** B → C members=['B', 'D', 'A', 'C']
- `0.998633s` **announcement_sent** B → A members=['B', 'D', 'A', 'C']
- `0.998696s` **announcement_received** D members=['B', 'D', 'A', 'C']
- `0.998791s` **announcement_received** C members=['B', 'D', 'A', 'C']
- `0.998817s` **announcement_received** A members=['B', 'D', 'A', 'C']
- `1.002098s` **local_roster** B members=['B', 'D', 'A', 'C']
- `1.002124s` **local_roster** A members=['B', 'D', 'A', 'C']
- `1.002127s` **local_roster** D members=['B', 'D', 'A', 'C']
- `1.002364s` **local_roster** C members=['B', 'D', 'A', 'C']
- `1.026960s` **averaging_terminal** B status=success
- `1.027008s` **averaging_terminal** D status=success
- `1.027040s` **averaging_terminal** C status=success
- `1.027074s` **averaging_terminal** A status=success

## control3

| Node | Operation status | Local-roster mean | Stream proof | Applied deltas | Origins observed | Row coverage | Coordinates correct | Fallback | A cleanly excluded | Announcement/aggregation to terminal (s) | Agrees with | Max mean error |
|---|---|---:|---:|---:|---|---|---:|---:|---:|---:|---|---:|
| B | success | True | True | True | B, D, C | B:48, D:48, A:0, C:48 | True | False | True | 0.029 | D, C | 5.08626e-06 |
| D | success | True | True | True | B, D, C | B:48, D:48, A:0, C:48 | True | False | True | 0.066 | B, C | 5.08626e-06 |
| A | idle | False | False | False | A | B:0, D:0, A:48, C:0 | True | False | False | — | — | — |
| C | success | True | True | True | B, D, C | B:48, D:48, A:0, C:48 | True | False | True | 0.066 | B, D | 5.08626e-06 |

Timeline (seconds are the recorded event times):

- `1.001953s` **step_started** B
- `1.017911s` **leader_advertised** B
- `1.094915s` **step_started** D
- `1.106655s` **join_accepted** B follower=D
- `1.254108s` **step_started** C
- `1.262970s` **join_accepted** B follower=C
- `11.004538s` **leader_assembled** B members=['B', 'D', 'C']
- `11.004720s` **announcement_sent** B → D members=['B', 'D', 'C']
- `11.004864s` **announcement_sent** B → C members=['B', 'D', 'C']
- `11.005070s` **announcement_received** D members=['B', 'D', 'C']
- `11.005130s` **announcement_received** C members=['B', 'D', 'C']
- `11.037265s` **local_roster** D members=['B', 'D', 'C']
- `11.037972s` **local_roster** C members=['B', 'D', 'C']
- `11.042457s` **local_roster** B members=['B', 'D', 'C']
- `11.071236s` **averaging_terminal** B status=success
- `11.071313s` **averaging_terminal** D status=success
- `11.071361s` **averaging_terminal** C status=success

## membership

| Node | Operation status | Local-roster mean | Stream proof | Applied deltas | Origins observed | Row coverage | Coordinates correct | Fallback | A cleanly excluded | Announcement/aggregation to terminal (s) | Agrees with | Max mean error |
|---|---|---:|---:|---:|---|---|---:|---:|---:|---:|---|---:|
| B | success | True | True | True | B, D, C | B:48, D:48, A:0, C:48 | True | False | True | 0.023 | D, C | 5.08626e-06 |
| D | success | True | True | True | B, D, C | B:48, D:48, A:0, C:48 | True | False | True | 0.037 | B, C | 5.08626e-06 |
| A | success | False | False | True | A | B:0, D:0, A:48, C:0 | True | True | False | 5.025 | — | 50 |
| C | success | True | True | True | B, D, C | B:48, D:48, A:0, C:48 | True | False | True | 0.037 | B, D | 5.08626e-06 |

Timeline (seconds are the recorded event times):

- `0.998023s` **step_started** B
- `1.011945s` **leader_advertised** B
- `1.092388s` **step_started** D
- `1.103261s` **join_accepted** B follower=D
- `1.234279s` **step_started** C
- `1.245610s` **join_accepted** B follower=C
- `1.258979s` **step_started** A
- `1.277621s` **join_accepted** B follower=A
- `1.277956s` **leader_assembled** B members=['B', 'D', 'A', 'C']
- `1.278300s` **announcement_sent** B → D members=['B', 'D', 'C']
- `1.278506s` **announcement_sent** B → C members=['B', 'D', 'C']
- `1.278653s` **announcement_sent** B → A members=['B', 'D', 'A', 'C']
- `1.278829s` **announcement_received** D members=['B', 'D', 'C']
- `1.279194s` **announcement_received** A members=['B', 'D', 'A', 'C']
- `1.279313s` **announcement_received** C members=['B', 'D', 'C']
- `1.290520s` **local_roster** C members=['B', 'D', 'C']
- `1.291264s` **local_roster** D members=['B', 'D', 'C']
- `1.292643s` **local_roster** A members=['B', 'D', 'A', 'C']
- `1.292657s` **local_roster** B members=['B', 'D', 'C']
- `1.298039s` **stream_error** A → B error=ValueError('<libp2p.peer.id.ID (12D3KooWMgUc5wVMWFnqt5zbnXLY2b36g9Z3y6HGFMc5yeu571Ke)> is not in list')
- `1.298808s` **stream_error** A → D error=ValueError('<libp2p.peer.id.ID (12D3KooWMgUc5wVMWFnqt5zbnXLY2b36g9Z3y6HGFMc5yeu571Ke)> is not in list')
- `1.299831s` **stream_error** A → C error=ValueError('<libp2p.peer.id.ID (12D3KooWMgUc5wVMWFnqt5zbnXLY2b36g9Z3y6HGFMc5yeu571Ke)> is not in list')
- `1.316057s` **averaging_terminal** B status=success
- `1.316128s` **averaging_terminal** D status=success
- `1.316250s` **averaging_terminal** C status=success
- `6.303985s` **averaging_terminal** A status=success

## ordering

| Node | Operation status | Local-roster mean | Stream proof | Applied deltas | Origins observed | Row coverage | Coordinates correct | Fallback | A cleanly excluded | Announcement/aggregation to terminal (s) | Agrees with | Max mean error |
|---|---|---:|---:|---:|---|---|---:|---:|---:|---:|---|---:|
| B | success | False | True | True | B, D, A, C | B:48, D:48, A:48, C:48 | False | False | False | 0.033 | D, C | 6 |
| D | success | False | True | True | B, D, A, C | B:48, D:48, A:48, C:48 | False | False | False | 0.044 | B, C | 6 |
| A | success | False | True | True | B, D, A, C | B:48, D:48, A:48, C:48 | False | False | False | 0.043 | — | 18 |
| C | success | False | True | True | B, D, A, C | B:48, D:48, A:48, C:48 | False | False | False | 0.043 | B, D | 6 |

Timeline (seconds are the recorded event times):

- `0.799097s` **step_started** B
- `0.807474s` **leader_advertised** B
- `0.883073s` **step_started** D
- `0.890642s` **join_accepted** B follower=D
- `1.023799s` **step_started** C
- `1.029737s` **join_accepted** B follower=C
- `1.061548s` **step_started** A
- `1.088411s` **join_accepted** B follower=A
- `1.089217s` **leader_assembled** B members=['B', 'D', 'A', 'C']
- `1.089789s` **announcement_sent** B → D members=['B', 'D', 'A', 'C']
- `1.090159s` **announcement_sent** B → C members=['B', 'D', 'A', 'C']
- `1.090540s` **announcement_received** D members=['B', 'D', 'A', 'C']
- `1.090614s` **announcement_sent** B → A members=['A', 'D', 'B', 'C']
- `1.091668s` **announcement_received** A members=['A', 'D', 'B', 'C']
- `1.091725s` **announcement_received** C members=['B', 'D', 'A', 'C']
- `1.100947s` **local_roster** B members=['B', 'D', 'A', 'C']
- `1.101551s` **local_roster** C members=['B', 'D', 'A', 'C']
- `1.101672s` **local_roster** D members=['B', 'D', 'A', 'C']
- `1.101918s` **local_roster** A members=['A', 'D', 'B', 'C']
- `1.134267s` **averaging_terminal** B status=success
- `1.134394s` **averaging_terminal** D status=success
- `1.134486s` **averaging_terminal** C status=success
- `1.134567s` **averaging_terminal** A status=success

## Interpretation limits

The report covers the exact local process runs and tensor values saved in these artifacts. It does not estimate attack probability, Internet performance, or model-convergence impact. A failed or partial aggregation is not evidence that a participant was cleanly excluded.
