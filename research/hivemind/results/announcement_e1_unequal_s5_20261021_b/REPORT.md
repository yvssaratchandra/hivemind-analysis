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
| B | success | True | True | True | B, D, A, C | B:48, D:48, A:48, C:48 | True | False | False | 0.027 | D, A, C | 0 |
| D | success | True | True | True | B, D, A, C | B:48, D:48, A:48, C:48 | True | False | False | 0.033 | B, A, C | 0 |
| A | success | True | True | True | B, D, A, C | B:48, D:48, A:48, C:48 | True | False | False | 0.033 | B, D, C | 0 |
| C | success | True | True | True | B, D, A, C | B:48, D:48, A:48, C:48 | True | False | False | 0.033 | B, D, A | 0 |

Timeline (seconds are the recorded event times):

- `0.884462s` **step_started** B
- `0.894033s` **leader_advertised** B
- `0.952876s` **step_started** C
- `0.959236s` **join_accepted** B follower=C
- `0.992005s` **step_started** D
- `0.995070s` **step_started** A
- `0.999222s` **join_accepted** B follower=D
- `1.011179s` **join_accepted** B follower=A
- `1.011454s` **leader_assembled** B members=['B', 'D', 'A', 'C']
- `1.011765s` **announcement_sent** B → C members=['B', 'D', 'A', 'C']
- `1.011900s` **announcement_sent** B → D members=['B', 'D', 'A', 'C']
- `1.012009s` **announcement_sent** B → A members=['B', 'D', 'A', 'C']
- `1.012017s` **announcement_received** C members=['B', 'D', 'A', 'C']
- `1.012145s` **announcement_received** D members=['B', 'D', 'A', 'C']
- `1.012202s` **announcement_received** A members=['B', 'D', 'A', 'C']
- `1.017255s` **local_roster** A members=['B', 'D', 'A', 'C']
- `1.017649s` **local_roster** D members=['B', 'D', 'A', 'C']
- `1.017697s` **local_roster** B members=['B', 'D', 'A', 'C']
- `1.017924s` **local_roster** C members=['B', 'D', 'A', 'C']
- `1.044924s` **averaging_terminal** B status=success
- `1.044980s` **averaging_terminal** C status=success
- `1.045021s` **averaging_terminal** D status=success
- `1.045248s` **averaging_terminal** A status=success

## control3

| Node | Operation status | Local-roster mean | Stream proof | Applied deltas | Origins observed | Row coverage | Coordinates correct | Fallback | A cleanly excluded | Announcement/aggregation to terminal (s) | Agrees with | Max mean error |
|---|---|---:|---:|---:|---|---|---:|---:|---:|---:|---|---:|
| B | success | True | True | True | B, D, C | B:48, D:48, A:0, C:48 | True | False | True | 0.022 | D, C | 5.08626e-06 |
| D | success | True | True | True | B, D, C | B:48, D:48, A:0, C:48 | True | False | True | 0.027 | B, C | 5.08626e-06 |
| A | idle | False | False | False | A | B:0, D:0, A:48, C:0 | True | False | False | — | — | — |
| C | success | True | True | True | B, D, C | B:48, D:48, A:0, C:48 | True | False | True | 0.027 | B, D | 5.08626e-06 |

Timeline (seconds are the recorded event times):

- `0.746352s` **step_started** B
- `0.754752s` **leader_advertised** B
- `0.802229s` **step_started** C
- `0.808852s` **join_accepted** B follower=C
- `0.838869s` **step_started** D
- `0.845507s` **join_accepted** B follower=D
- `10.805976s` **leader_assembled** B members=['B', 'D', 'C']
- `10.806091s` **announcement_sent** B → C members=['B', 'D', 'C']
- `10.806195s` **announcement_sent** B → D members=['B', 'D', 'C']
- `10.806435s` **announcement_received** C members=['B', 'D', 'C']
- `10.806618s` **announcement_received** D members=['B', 'D', 'C']
- `10.811244s` **local_roster** C members=['B', 'D', 'C']
- `10.811281s` **local_roster** D members=['B', 'D', 'C']
- `10.811381s` **local_roster** B members=['B', 'D', 'C']
- `10.833461s` **averaging_terminal** B status=success
- `10.833512s` **averaging_terminal** C status=success
- `10.833660s` **averaging_terminal** D status=success

## membership

| Node | Operation status | Local-roster mean | Stream proof | Applied deltas | Origins observed | Row coverage | Coordinates correct | Fallback | A cleanly excluded | Announcement/aggregation to terminal (s) | Agrees with | Max mean error |
|---|---|---:|---:|---:|---|---|---:|---:|---:|---:|---|---:|
| B | success | True | True | True | B, D, C | B:48, D:48, A:0, C:48 | True | False | True | 0.025 | D, C | 5.08626e-06 |
| D | success | True | True | True | B, D, C | B:48, D:48, A:0, C:48 | True | False | True | 0.030 | B, C | 5.08626e-06 |
| A | success | False | False | True | A | B:0, D:0, A:48, C:0 | True | True | False | 5.025 | — | 50 |
| C | success | True | True | True | B, D, C | B:48, D:48, A:0, C:48 | True | False | True | 0.030 | B, D | 5.08626e-06 |

Timeline (seconds are the recorded event times):

- `0.732889s` **step_started** B
- `0.743106s` **leader_advertised** B
- `0.800287s` **step_started** C
- `0.807933s` **join_accepted** B follower=C
- `0.841442s` **step_started** D
- `0.845193s` **step_started** A
- `0.849374s` **join_accepted** B follower=D
- `0.858097s` **join_accepted** B follower=A
- `0.858372s` **leader_assembled** B members=['B', 'D', 'A', 'C']
- `0.858696s` **announcement_sent** B → C members=['B', 'D', 'C']
- `0.858832s` **announcement_sent** B → D members=['B', 'D', 'C']
- `0.858950s` **announcement_sent** B → A members=['B', 'D', 'A', 'C']
- `0.858973s` **announcement_received** C members=['B', 'D', 'C']
- `0.859135s` **announcement_received** D members=['B', 'D', 'C']
- `0.859136s` **announcement_received** A members=['B', 'D', 'A', 'C']
- `0.863366s` **local_roster** A members=['B', 'D', 'A', 'C']
- `0.863412s` **local_roster** C members=['B', 'D', 'C']
- `0.863534s` **local_roster** B members=['B', 'D', 'C']
- `0.864017s` **local_roster** D members=['B', 'D', 'C']
- `0.870158s` **stream_error** A → C error=ValueError('<libp2p.peer.id.ID (12D3KooWDmvZebPjj5CGNNauBdjamkR1njxZ6jpkCfGTHK5L5KDq)> is not in list')
- `0.870392s` **stream_error** A → D error=ValueError('<libp2p.peer.id.ID (12D3KooWDmvZebPjj5CGNNauBdjamkR1njxZ6jpkCfGTHK5L5KDq)> is not in list')
- `0.888612s` **averaging_terminal** B status=success
- `0.888933s` **averaging_terminal** C status=success
- `0.889127s` **averaging_terminal** D status=success
- `5.883848s` **averaging_terminal** A status=success

## ordering

| Node | Operation status | Local-roster mean | Stream proof | Applied deltas | Origins observed | Row coverage | Coordinates correct | Fallback | A cleanly excluded | Announcement/aggregation to terminal (s) | Agrees with | Max mean error |
|---|---|---:|---:|---:|---|---|---:|---:|---:|---:|---|---:|
| B | success | False | True | True | B, D, A, C | B:48, D:48, A:48, C:48 | False | False | False | 0.031 | D, C | 4 |
| D | success | False | True | True | B, D, A, C | B:48, D:48, A:48, C:48 | False | False | False | 0.036 | B, C | 4 |
| A | success | False | True | True | B, D, A, C | B:48, D:48, A:48, C:48 | False | False | False | 0.036 | — | 12 |
| C | success | False | True | True | B, D, A, C | B:48, D:48, A:48, C:48 | False | False | False | 0.036 | B, D | 4 |

Timeline (seconds are the recorded event times):

- `0.813671s` **step_started** B
- `0.823161s` **leader_advertised** B
- `0.871802s` **step_started** C
- `0.878953s` **join_accepted** B follower=C
- `0.911417s` **step_started** D
- `0.919032s` **join_accepted** B follower=D
- `0.932639s` **step_started** A
- `0.948431s` **join_accepted** B follower=A
- `0.948955s` **leader_assembled** B members=['B', 'D', 'A', 'C']
- `0.949254s` **announcement_sent** B → C members=['B', 'D', 'A', 'C']
- `0.949424s` **announcement_sent** B → D members=['B', 'D', 'A', 'C']
- `0.949559s` **announcement_received** C members=['B', 'D', 'A', 'C']
- `0.949560s` **announcement_sent** B → A members=['A', 'D', 'B', 'C']
- `0.949671s` **announcement_received** D members=['B', 'D', 'A', 'C']
- `0.949805s` **announcement_received** A members=['A', 'D', 'B', 'C']
- `0.953650s` **local_roster** A members=['A', 'D', 'B', 'C']
- `0.953808s` **local_roster** C members=['B', 'D', 'A', 'C']
- `0.953832s` **local_roster** D members=['B', 'D', 'A', 'C']
- `0.953892s` **local_roster** B members=['B', 'D', 'A', 'C']
- `0.985263s` **averaging_terminal** B status=success
- `0.985338s` **averaging_terminal** C status=success
- `0.985392s` **averaging_terminal** D status=success
- `0.985452s` **averaging_terminal** A status=success

## Interpretation limits

The report covers the exact local process runs and tensor values saved in these artifacts. It does not estimate attack probability, Internet performance, or model-convergence impact. A failed or partial aggregation is not evidence that a participant was cleanly excluded.
