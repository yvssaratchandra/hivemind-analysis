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
| B | success | True | True | True | B, D, A, C | B:48, D:48, A:48, C:48 | True | False | False | 0.030 | D, A, C | 0 |
| D | success | True | True | True | B, D, A, C | B:48, D:48, A:48, C:48 | True | False | False | 0.040 | B, A, C | 0 |
| A | success | True | True | True | B, D, A, C | B:48, D:48, A:48, C:48 | True | False | False | 0.040 | B, D, C | 0 |
| C | success | True | True | True | B, D, A, C | B:48, D:48, A:48, C:48 | True | False | False | 0.040 | B, D, A | 0 |

Timeline (seconds are the recorded event times):

- `0.885462s` **step_started** B
- `0.893484s` **leader_advertised** B
- `1.001571s` **step_started** C
- `1.007639s` **join_accepted** B follower=C
- `1.091468s` **step_started** D
- `1.097523s` **step_started** A
- `1.105256s` **join_accepted** B follower=D
- `1.120402s` **join_accepted** B follower=A
- `1.121333s` **leader_assembled** B members=['B', 'D', 'A', 'C']
- `1.122139s` **announcement_sent** B → C members=['B', 'D', 'A', 'C']
- `1.122543s` **announcement_sent** B → D members=['B', 'D', 'A', 'C']
- `1.122838s` **announcement_sent** B → A members=['B', 'D', 'A', 'C']
- `1.122880s` **announcement_received** C members=['B', 'D', 'A', 'C']
- `1.123149s` **announcement_received** D members=['B', 'D', 'A', 'C']
- `1.123354s` **announcement_received** A members=['B', 'D', 'A', 'C']
- `1.133555s` **local_roster** B members=['B', 'D', 'A', 'C']
- `1.133764s` **local_roster** D members=['B', 'D', 'A', 'C']
- `1.133908s` **local_roster** C members=['B', 'D', 'A', 'C']
- `1.133911s` **local_roster** A members=['B', 'D', 'A', 'C']
- `1.163060s` **averaging_terminal** B status=success
- `1.163130s` **averaging_terminal** C status=success
- `1.163181s` **averaging_terminal** D status=success
- `1.163404s` **averaging_terminal** A status=success

## control3

| Node | Operation status | Local-roster mean | Stream proof | Applied deltas | Origins observed | Row coverage | Coordinates correct | Fallback | A cleanly excluded | Announcement/aggregation to terminal (s) | Agrees with | Max mean error |
|---|---|---:|---:|---:|---|---|---:|---:|---:|---:|---|---:|
| B | success | True | True | True | B, D, C | B:48, D:48, A:0, C:48 | True | False | True | 0.018 | D, C | 5.08626e-06 |
| D | success | True | True | True | B, D, C | B:48, D:48, A:0, C:48 | True | False | True | 0.049 | B, C | 5.08626e-06 |
| A | idle | False | False | False | A | B:0, D:0, A:48, C:0 | True | False | False | — | — | — |
| C | success | True | True | True | B, D, C | B:48, D:48, A:0, C:48 | True | False | True | 0.049 | B, D | 5.08626e-06 |

Timeline (seconds are the recorded event times):

- `0.998306s` **step_started** B
- `1.016965s` **leader_advertised** B
- `1.112837s` **step_started** C
- `1.123057s` **join_accepted** B follower=C
- `1.195024s` **step_started** D
- `1.207120s` **join_accepted** B follower=D
- `10.999173s` **leader_assembled** B members=['B', 'D', 'C']
- `10.999361s` **announcement_sent** B → C members=['B', 'D', 'C']
- `10.999512s` **announcement_sent** B → D members=['B', 'D', 'C']
- `11.000419s` **announcement_received** D members=['B', 'D', 'C']
- `11.000790s` **announcement_received** C members=['B', 'D', 'C']
- `11.027608s` **local_roster** D members=['B', 'D', 'C']
- `11.027819s` **local_roster** C members=['B', 'D', 'C']
- `11.031849s` **local_roster** B members=['B', 'D', 'C']
- `11.049622s` **averaging_terminal** B status=success
- `11.049708s` **averaging_terminal** C status=success
- `11.049754s` **averaging_terminal** D status=success

## membership

| Node | Operation status | Local-roster mean | Stream proof | Applied deltas | Origins observed | Row coverage | Coordinates correct | Fallback | A cleanly excluded | Announcement/aggregation to terminal (s) | Agrees with | Max mean error |
|---|---|---:|---:|---:|---|---|---:|---:|---:|---:|---|---:|
| B | success | True | True | True | B, D, C | B:48, D:48, A:0, C:48 | True | False | True | 0.034 | D, C | 5.08626e-06 |
| D | success | True | True | True | B, D, C | B:48, D:48, A:0, C:48 | True | False | True | 0.046 | B, C | 5.08626e-06 |
| A | success | False | False | True | A | B:0, D:0, A:48, C:0 | True | True | False | 5.025 | — | 50 |
| C | success | True | True | True | B, D, C | B:48, D:48, A:0, C:48 | True | False | True | 0.046 | B, D | 5.08626e-06 |

Timeline (seconds are the recorded event times):

- `0.986726s` **step_started** B
- `1.000336s` **leader_advertised** B
- `1.101698s` **step_started** C
- `1.111489s` **join_accepted** B follower=C
- `1.183083s` **step_started** D
- `1.186710s` **step_started** A
- `1.193685s` **join_accepted** B follower=D
- `1.211773s` **join_accepted** B follower=A
- `1.212112s` **leader_assembled** B members=['B', 'D', 'A', 'C']
- `1.213190s` **announcement_sent** B → C members=['B', 'D', 'C']
- `1.213366s` **announcement_sent** B → D members=['B', 'D', 'C']
- `1.213504s` **announcement_sent** B → A members=['B', 'D', 'A', 'C']
- `1.213559s` **announcement_received** C members=['B', 'D', 'C']
- `1.213746s` **announcement_received** A members=['B', 'D', 'A', 'C']
- `1.213832s` **announcement_received** D members=['B', 'D', 'C']
- `1.223638s` **local_roster** C members=['B', 'D', 'C']
- `1.224973s` **local_roster** D members=['B', 'D', 'C']
- `1.225405s` **local_roster** B members=['B', 'D', 'C']
- `1.226581s` **local_roster** A members=['B', 'D', 'A', 'C']
- `1.237187s` **stream_error** A → B error=ValueError('<libp2p.peer.id.ID (12D3KooWBPP9pxGBJiVA58nMGempEG1639oqPLQt9bbUEnDt9v2X)> is not in list')
- `1.241829s` **stream_error** A → D error=ValueError('<libp2p.peer.id.ID (12D3KooWBPP9pxGBJiVA58nMGempEG1639oqPLQt9bbUEnDt9v2X)> is not in list')
- `1.246073s` **stream_error** A → C error=ValueError('<libp2p.peer.id.ID (12D3KooWBPP9pxGBJiVA58nMGempEG1639oqPLQt9bbUEnDt9v2X)> is not in list')
- `1.259450s` **averaging_terminal** B status=success
- `1.259602s` **averaging_terminal** C status=success
- `1.259684s` **averaging_terminal** D status=success
- `6.239190s` **averaging_terminal** A status=success

## ordering

| Node | Operation status | Local-roster mean | Stream proof | Applied deltas | Origins observed | Row coverage | Coordinates correct | Fallback | A cleanly excluded | Announcement/aggregation to terminal (s) | Agrees with | Max mean error |
|---|---|---:|---:|---:|---|---|---:|---:|---:|---:|---|---:|
| B | success | False | True | True | B, D, A, C | B:48, D:48, A:48, C:48 | False | False | False | 0.074 | D, C | 6 |
| D | success | False | True | True | B, D, A, C | B:48, D:48, A:48, C:48 | False | False | False | 0.086 | B, C | 6 |
| A | success | False | True | True | B, D, A, C | B:48, D:48, A:48, C:48 | False | False | False | 0.086 | — | 18 |
| C | success | False | True | True | B, D, A, C | B:48, D:48, A:48, C:48 | False | False | False | 0.086 | B, D | 6 |

Timeline (seconds are the recorded event times):

- `0.967190s` **step_started** B
- `0.977415s` **leader_advertised** B
- `1.095326s` **step_started** C
- `1.111223s` **join_accepted** B follower=C
- `1.160964s` **step_started** A
- `1.187085s` **join_accepted** B follower=A
- `1.205290s` **step_started** D
- `1.224862s` **join_accepted** B follower=D
- `1.225756s` **leader_assembled** B members=['B', 'D', 'A', 'C']
- `1.226533s` **announcement_sent** B → C members=['B', 'D', 'A', 'C']
- `1.226961s` **announcement_sent** B → A members=['A', 'D', 'B', 'C']
- `1.227291s` **announcement_sent** B → D members=['B', 'D', 'A', 'C']
- `1.227324s` **announcement_received** C members=['B', 'D', 'A', 'C']
- `1.227831s` **announcement_received** A members=['A', 'D', 'B', 'C']
- `1.228215s` **announcement_received** D members=['B', 'D', 'A', 'C']
- `1.239008s` **local_roster** B members=['B', 'D', 'A', 'C']
- `1.239918s` **local_roster** C members=['B', 'D', 'A', 'C']
- `1.240106s` **local_roster** A members=['A', 'D', 'B', 'C']
- `1.240901s` **local_roster** D members=['B', 'D', 'A', 'C']
- `1.313486s` **averaging_terminal** B status=success
- `1.313577s` **averaging_terminal** C status=success
- `1.313640s` **averaging_terminal** A status=success
- `1.313740s` **averaging_terminal** D status=success

## Interpretation limits

The report covers the exact local process runs and tensor values saved in these artifacts. It does not estimate attack probability, Internet performance, or model-convergence impact. A failed or partial aggregation is not evidence that a participant was cleanly excluded.
