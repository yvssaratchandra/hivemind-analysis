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
| B | success | True | True | True | B, D, A, C | B:48, D:48, A:48, C:48 | True | False | False | 0.029 | D, A, C | 0 |
| D | success | True | True | True | B, D, A, C | B:48, D:48, A:48, C:48 | True | False | False | 0.041 | B, A, C | 0 |
| A | success | True | True | True | B, D, A, C | B:48, D:48, A:48, C:48 | True | False | False | 0.041 | B, D, C | 0 |
| C | success | True | True | True | B, D, A, C | B:48, D:48, A:48, C:48 | True | False | False | 0.041 | B, D, A | 0 |

Timeline (seconds are the recorded event times):

- `0.985903s` **step_started** B
- `1.001206s` **leader_advertised** B
- `1.100622s` **step_started** A
- `1.116819s` **join_accepted** B follower=A
- `1.129781s` **step_started** C
- `1.140979s` **join_accepted** B follower=C
- `1.264990s` **step_started** D
- `1.276506s` **join_accepted** B follower=D
- `1.276921s` **leader_assembled** B members=['B', 'D', 'A', 'C']
- `1.277227s` **announcement_sent** B → A members=['B', 'D', 'A', 'C']
- `1.277395s` **announcement_sent** B → C members=['B', 'D', 'A', 'C']
- `1.277536s` **announcement_sent** B → D members=['B', 'D', 'A', 'C']
- `1.277762s` **announcement_received** C members=['B', 'D', 'A', 'C']
- `1.277890s` **announcement_received** D members=['B', 'D', 'A', 'C']
- `1.278134s` **announcement_received** A members=['B', 'D', 'A', 'C']
- `1.289445s` **local_roster** B members=['B', 'D', 'A', 'C']
- `1.289716s` **local_roster** A members=['B', 'D', 'A', 'C']
- `1.289719s` **local_roster** C members=['B', 'D', 'A', 'C']
- `1.290818s` **local_roster** D members=['B', 'D', 'A', 'C']
- `1.318875s` **averaging_terminal** B status=success
- `1.319016s` **averaging_terminal** A status=success
- `1.319104s` **averaging_terminal** C status=success
- `1.319159s` **averaging_terminal** D status=success

## control3

| Node | Operation status | Local-roster mean | Stream proof | Applied deltas | Origins observed | Row coverage | Coordinates correct | Fallback | A cleanly excluded | Announcement/aggregation to terminal (s) | Agrees with | Max mean error |
|---|---|---:|---:|---:|---|---|---:|---:|---:|---:|---|---:|
| B | success | True | True | True | B, D, C | B:48, D:48, A:0, C:48 | True | False | True | 0.023 | D, C | 5.08626e-06 |
| D | success | True | True | True | B, D, C | B:48, D:48, A:0, C:48 | True | False | True | 0.054 | B, C | 5.08626e-06 |
| A | idle | False | False | False | A | B:0, D:0, A:48, C:0 | True | False | False | — | — | — |
| C | success | True | True | True | B, D, C | B:48, D:48, A:0, C:48 | True | False | True | 0.054 | B, D | 5.08626e-06 |

Timeline (seconds are the recorded event times):

- `0.993185s` **step_started** B
- `1.008094s` **leader_advertised** B
- `1.137990s` **step_started** C
- `1.150574s` **join_accepted** B follower=C
- `1.274095s` **step_started** D
- `1.286414s` **join_accepted** B follower=D
- `10.996081s` **leader_assembled** B members=['B', 'D', 'C']
- `10.996296s` **announcement_sent** B → C members=['B', 'D', 'C']
- `10.996489s` **announcement_sent** B → D members=['B', 'D', 'C']
- `10.996688s` **announcement_received** C members=['B', 'D', 'C']
- `10.996824s` **announcement_received** D members=['B', 'D', 'C']
- `11.022907s` **local_roster** C members=['B', 'D', 'C']
- `11.023139s` **local_roster** D members=['B', 'D', 'C']
- `11.027938s` **local_roster** B members=['B', 'D', 'C']
- `11.050490s` **averaging_terminal** B status=success
- `11.050561s` **averaging_terminal** C status=success
- `11.050605s` **averaging_terminal** D status=success

## membership

| Node | Operation status | Local-roster mean | Stream proof | Applied deltas | Origins observed | Row coverage | Coordinates correct | Fallback | A cleanly excluded | Announcement/aggregation to terminal (s) | Agrees with | Max mean error |
|---|---|---:|---:|---:|---|---|---:|---:|---:|---:|---|---:|
| B | success | True | True | True | B, D, C | B:48, D:48, A:0, C:48 | True | False | True | 0.027 | D, C | 5.08626e-06 |
| D | success | True | True | True | B, D, C | B:48, D:48, A:0, C:48 | True | False | True | 0.031 | B, C | 5.08626e-06 |
| A | success | False | False | True | A | B:0, D:0, A:48, C:0 | True | True | False | 5.026 | — | 50 |
| C | success | True | True | True | B, D, C | B:48, D:48, A:0, C:48 | True | False | True | 0.031 | B, D | 5.08626e-06 |

Timeline (seconds are the recorded event times):

- `0.703004s` **step_started** B
- `0.711664s` **leader_advertised** B
- `0.814169s` **step_started** A
- `0.826315s` **join_accepted** B follower=A
- `0.836753s` **step_started** C
- `0.842757s` **join_accepted** B follower=C
- `0.958871s` **step_started** D
- `0.965436s` **join_accepted** B follower=D
- `0.965654s` **leader_assembled** B members=['B', 'D', 'A', 'C']
- `0.965795s` **announcement_sent** B → A members=['B', 'D', 'A', 'C']
- `0.965890s` **announcement_sent** B → C members=['B', 'D', 'C']
- `0.965959s` **announcement_sent** B → D members=['B', 'D', 'C']
- `0.966032s` **announcement_received** A members=['B', 'D', 'A', 'C']
- `0.966190s` **announcement_received** C members=['B', 'D', 'C']
- `0.966202s` **announcement_received** D members=['B', 'D', 'C']
- `0.969894s` **local_roster** B members=['B', 'D', 'C']
- `0.970069s` **local_roster** D members=['B', 'D', 'C']
- `0.970243s` **local_roster** C members=['B', 'D', 'C']
- `0.970315s` **local_roster** A members=['B', 'D', 'A', 'C']
- `0.973859s` **stream_error** A → B error=ValueError('<libp2p.peer.id.ID (12D3KooWRk5jWiKhmrgNNqMxiMEotaHo9epwbvf9gzdsuVSwsWRX)> is not in list')
- `0.973988s` **stream_error** A → D error=ValueError('<libp2p.peer.id.ID (12D3KooWRk5jWiKhmrgNNqMxiMEotaHo9epwbvf9gzdsuVSwsWRX)> is not in list')
- `0.974185s` **stream_error** A → C error=ValueError('<libp2p.peer.id.ID (12D3KooWRk5jWiKhmrgNNqMxiMEotaHo9epwbvf9gzdsuVSwsWRX)> is not in list')
- `0.996696s` **averaging_terminal** B status=success
- `0.996736s` **averaging_terminal** C status=success
- `0.996767s` **averaging_terminal** D status=success
- `5.992399s` **averaging_terminal** A status=success

## ordering

| Node | Operation status | Local-roster mean | Stream proof | Applied deltas | Origins observed | Row coverage | Coordinates correct | Fallback | A cleanly excluded | Announcement/aggregation to terminal (s) | Agrees with | Max mean error |
|---|---|---:|---:|---:|---|---|---:|---:|---:|---:|---|---:|
| B | success | False | True | True | B, D, A, C | B:48, D:48, A:48, C:48 | False | False | False | 0.045 | D, C | 6 |
| D | success | False | True | True | B, D, A, C | B:48, D:48, A:48, C:48 | False | False | False | 0.068 | B, C | 6 |
| A | success | False | True | True | B, D, A, C | B:48, D:48, A:48, C:48 | False | False | False | 0.068 | — | 18 |
| C | success | False | True | True | B, D, A, C | B:48, D:48, A:48, C:48 | False | False | False | 0.068 | B, D | 6 |

Timeline (seconds are the recorded event times):

- `0.977540s` **step_started** B
- `0.991209s` **leader_advertised** B
- `1.104436s` **step_started** A
- `1.121679s` **join_accepted** B follower=A
- `1.134183s` **step_started** C
- `1.146914s` **join_accepted** B follower=C
- `1.258752s` **step_started** D
- `1.270535s` **join_accepted** B follower=D
- `1.270910s` **leader_assembled** B members=['B', 'D', 'A', 'C']
- `1.271242s` **announcement_sent** B → A members=['A', 'D', 'B', 'C']
- `1.271412s` **announcement_sent** B → C members=['B', 'D', 'A', 'C']
- `1.271535s` **announcement_sent** B → D members=['B', 'D', 'A', 'C']
- `1.271983s` **announcement_received** A members=['A', 'D', 'B', 'C']
- `1.272022s` **announcement_received** D members=['B', 'D', 'A', 'C']
- `1.272446s` **announcement_received** C members=['B', 'D', 'A', 'C']
- `1.287997s` **local_roster** A members=['A', 'D', 'B', 'C']
- `1.288815s` **local_roster** D members=['B', 'D', 'A', 'C']
- `1.294575s` **local_roster** B members=['B', 'D', 'A', 'C']
- `1.299860s` **local_roster** C members=['B', 'D', 'A', 'C']
- `1.339741s` **averaging_terminal** B status=success
- `1.339823s` **averaging_terminal** A status=success
- `1.339979s` **averaging_terminal** C status=success
- `1.340060s` **averaging_terminal** D status=success

## Interpretation limits

The report covers the exact local process runs and tensor values saved in these artifacts. It does not estimate attack probability, Internet performance, or model-convergence impact. A failed or partial aggregation is not evidence that a participant was cleanly excluded.
