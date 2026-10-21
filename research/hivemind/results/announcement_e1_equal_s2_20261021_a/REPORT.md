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
| D | success | True | True | True | B, D, A, C | B:48, D:48, A:48, C:48 | True | False | False | 0.034 | B, A, C | 0 |
| A | success | True | True | True | B, D, A, C | B:48, D:48, A:48, C:48 | True | False | False | 0.034 | B, D, C | 0 |
| C | success | True | True | True | B, D, A, C | B:48, D:48, A:48, C:48 | True | False | False | 0.034 | B, D, A | 0 |

Timeline (seconds are the recorded event times):

- `0.733533s` **step_started** B
- `0.741242s` **leader_advertised** B
- `0.801437s` **step_started** A
- `0.818298s` **join_accepted** B follower=A
- `0.941004s` **step_started** C
- `0.947985s` **join_accepted** B follower=C
- `0.995816s` **step_started** D
- `1.002195s` **join_accepted** B follower=D
- `1.002415s` **leader_assembled** B members=['B', 'D', 'A', 'C']
- `1.002555s` **announcement_sent** B → A members=['B', 'D', 'A', 'C']
- `1.002646s` **announcement_sent** B → C members=['B', 'D', 'A', 'C']
- `1.002723s` **announcement_sent** B → D members=['B', 'D', 'A', 'C']
- `1.002812s` **announcement_received** A members=['B', 'D', 'A', 'C']
- `1.002881s` **announcement_received** C members=['B', 'D', 'A', 'C']
- `1.002900s` **announcement_received** D members=['B', 'D', 'A', 'C']
- `1.006671s` **local_roster** C members=['B', 'D', 'A', 'C']
- `1.006729s` **local_roster** A members=['B', 'D', 'A', 'C']
- `1.006798s` **local_roster** D members=['B', 'D', 'A', 'C']
- `1.006826s` **local_roster** B members=['B', 'D', 'A', 'C']
- `1.036850s` **averaging_terminal** B status=success
- `1.036913s` **averaging_terminal** A status=success
- `1.036960s` **averaging_terminal** C status=success
- `1.037008s` **averaging_terminal** D status=success

## control3

| Node | Operation status | Local-roster mean | Stream proof | Applied deltas | Origins observed | Row coverage | Coordinates correct | Fallback | A cleanly excluded | Announcement/aggregation to terminal (s) | Agrees with | Max mean error |
|---|---|---:|---:|---:|---|---|---:|---:|---:|---:|---|---:|
| B | success | True | True | True | B, D, C | B:48, D:48, A:0, C:48 | True | False | True | 0.035 | D, C | 5.08626e-06 |
| D | success | True | True | True | B, D, C | B:48, D:48, A:0, C:48 | True | False | True | 0.066 | B, C | 5.08626e-06 |
| A | idle | False | False | False | A | B:0, D:0, A:48, C:0 | True | False | False | — | — | — |
| C | success | True | True | True | B, D, C | B:48, D:48, A:0, C:48 | True | False | True | 0.061 | B, D | 5.08626e-06 |

Timeline (seconds are the recorded event times):

- `0.999616s` **step_started** B
- `1.013732s` **leader_advertised** B
- `1.226858s` **step_started** C
- `1.237742s` **join_accepted** B follower=C
- `1.269196s` **step_started** D
- `1.280342s` **join_accepted** B follower=D
- `11.000114s` **leader_assembled** B members=['B', 'D', 'C']
- `11.000303s` **announcement_sent** B → D members=['B', 'D', 'C']
- `11.000574s` **announcement_received** D members=['B', 'D', 'C']
- `11.004105s` **announcement_sent** B → C members=['B', 'D', 'C']
- `11.005999s` **announcement_received** C members=['B', 'D', 'C']
- `11.027318s` **local_roster** C members=['B', 'D', 'C']
- `11.028350s` **local_roster** D members=['B', 'D', 'C']
- `11.031144s` **local_roster** B members=['B', 'D', 'C']
- `11.066544s` **averaging_terminal** B status=success
- `11.066606s` **averaging_terminal** C status=success
- `11.066653s` **averaging_terminal** D status=success

## membership

| Node | Operation status | Local-roster mean | Stream proof | Applied deltas | Origins observed | Row coverage | Coordinates correct | Fallback | A cleanly excluded | Announcement/aggregation to terminal (s) | Agrees with | Max mean error |
|---|---|---:|---:|---:|---|---|---:|---:|---:|---:|---|---:|
| B | success | True | True | True | B, D, C | B:48, D:48, A:0, C:48 | True | False | True | 0.029 | D, C | 5.08626e-06 |
| D | success | True | True | True | B, D, C | B:48, D:48, A:0, C:48 | True | False | True | 0.038 | B, C | 5.08626e-06 |
| A | success | False | False | True | A | B:0, D:0, A:48, C:0 | True | True | False | 5.020 | — | 50 |
| C | success | True | True | True | B, D, C | B:48, D:48, A:0, C:48 | True | False | True | 0.038 | B, D | 5.08626e-06 |

Timeline (seconds are the recorded event times):

- `1.015735s` **step_started** B
- `1.029199s` **leader_advertised** B
- `1.088705s` **step_started** A
- `1.114014s` **join_accepted** B follower=A
- `1.243456s` **step_started** C
- `1.262710s` **join_accepted** B follower=C
- `1.289050s` **step_started** D
- `1.298606s` **join_accepted** B follower=D
- `1.299004s` **leader_assembled** B members=['B', 'D', 'A', 'C']
- `1.299286s` **announcement_sent** B → A members=['B', 'D', 'A', 'C']
- `1.299476s` **announcement_sent** B → C members=['B', 'D', 'C']
- `1.299618s` **announcement_sent** B → D members=['B', 'D', 'C']
- `1.299720s` **announcement_received** A members=['B', 'D', 'A', 'C']
- `1.300868s` **announcement_received** C members=['B', 'D', 'C']
- `1.301433s` **announcement_received** D members=['B', 'D', 'C']
- `1.310159s` **local_roster** C members=['B', 'D', 'C']
- `1.310165s` **local_roster** B members=['B', 'D', 'C']
- `1.310802s` **local_roster** A members=['B', 'D', 'A', 'C']
- `1.311124s` **local_roster** D members=['B', 'D', 'C']
- `1.316992s` **stream_error** A → B error=ValueError('<libp2p.peer.id.ID (12D3KooWNJ5VZzZQaCB7Zz3PmGPX1ZTPXUC3kCPVy6z2pUoh2Wjp)> is not in list')
- `1.317844s` **stream_error** A → D error=ValueError('<libp2p.peer.id.ID (12D3KooWNJ5VZzZQaCB7Zz3PmGPX1ZTPXUC3kCPVy6z2pUoh2Wjp)> is not in list')
- `1.318581s` **stream_error** A → C error=ValueError('<libp2p.peer.id.ID (12D3KooWNJ5VZzZQaCB7Zz3PmGPX1ZTPXUC3kCPVy6z2pUoh2Wjp)> is not in list')
- `1.339144s` **averaging_terminal** B status=success
- `1.339217s` **averaging_terminal** C status=success
- `1.339266s` **averaging_terminal** D status=success
- `6.319803s` **averaging_terminal** A status=success

## ordering

| Node | Operation status | Local-roster mean | Stream proof | Applied deltas | Origins observed | Row coverage | Coordinates correct | Fallback | A cleanly excluded | Announcement/aggregation to terminal (s) | Agrees with | Max mean error |
|---|---|---:|---:|---:|---|---|---:|---:|---:|---:|---|---:|
| B | success | False | True | True | B, D, A, C | B:48, D:48, A:48, C:48 | False | False | False | 0.025 | D, C | 6 |
| D | success | False | True | True | B, D, A, C | B:48, D:48, A:48, C:48 | False | False | False | 0.028 | B, C | 6 |
| A | success | False | True | True | B, D, A, C | B:48, D:48, A:48, C:48 | False | False | False | 0.028 | — | 18 |
| C | success | False | True | True | B, D, A, C | B:48, D:48, A:48, C:48 | False | False | False | 0.028 | B, D | 6 |

Timeline (seconds are the recorded event times):

- `0.739089s` **step_started** B
- `0.746373s` **leader_advertised** B
- `0.809205s` **step_started** A
- `0.824660s` **join_accepted** B follower=A
- `0.951013s` **step_started** C
- `0.957056s` **join_accepted** B follower=C
- `0.998150s` **step_started** D
- `1.004092s` **join_accepted** B follower=D
- `1.004379s` **leader_assembled** B members=['B', 'D', 'A', 'C']
- `1.004586s` **announcement_sent** B → A members=['A', 'D', 'B', 'C']
- `1.004720s` **announcement_sent** B → C members=['B', 'D', 'A', 'C']
- `1.004777s` **announcement_received** A members=['A', 'D', 'B', 'C']
- `1.004826s` **announcement_sent** B → D members=['B', 'D', 'A', 'C']
- `1.004960s` **announcement_received** C members=['B', 'D', 'A', 'C']
- `1.005038s` **announcement_received** D members=['B', 'D', 'A', 'C']
- `1.008271s` **local_roster** B members=['B', 'D', 'A', 'C']
- `1.008404s` **local_roster** C members=['B', 'D', 'A', 'C']
- `1.008439s` **local_roster** D members=['B', 'D', 'A', 'C']
- `1.008445s` **local_roster** A members=['A', 'D', 'B', 'C']
- `1.032795s` **averaging_terminal** B status=success
- `1.032844s` **averaging_terminal** A status=success
- `1.032877s` **averaging_terminal** C status=success
- `1.032913s` **averaging_terminal** D status=success

## Interpretation limits

The report covers the exact local process runs and tensor values saved in these artifacts. It does not estimate attack probability, Internet performance, or model-convergence impact. A failed or partial aggregation is not evidence that a participant was cleanly excluded.
