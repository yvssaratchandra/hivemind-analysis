# Agora 8B private baseline results

Completed: two seeds, five logical rounds each; eight peers per round. Both pilots passed. All peers/controllers are stopped. An independent check found no survivors among 192 recorded process IDs across all attempts.

| Seed | Verified rounds | Group size | Mean ready-to-roster | Mean native roster time | Initial mean loss | Final mean loss | Max numerical error |
|---|---:|---:|---:|---:|---:|---:|---:|
| 8101 | 5/5 | 8 | 1.087s | 0.977s | 0.795260 | 0.001876 | 2.61e-08 |
| 8102 | 5/5 | 8 | 0.952s | 0.840s | 1.172358 | 0.001494 | 2.05e-08 |

Time means use 40 peer-round observations per seed, not 40 separate groups. There are ten logical rounds total. Native group ID bytes are constant; trial plus epoch identifies a round. Agora does not elect a Hivemind-style leader here. All four reducers received parts in every peer-round, and all eight NODE/CLIENT senders contributed weight 1. The oracle checked mean(snapshot) minus own snapshot, plus application to the current locally trained weights.

26 focused checks passed. Independent raw calculations and analyzer replays with pinned Python 3.11 match both reports. Frozen source/file-set hashes and completed-result hashes validate. See [VERIFICATION.json](VERIFICATION.json), [checkpoint.json](checkpoint.json), and [INDEPENDENT_SHUTDOWN.json](INDEPENDENT_SHUTDOWN.json).

Seed 8101 attempt01 suspended for 2315.853 seconds after its first round. The whole attempt is invalid and excluded; its traces remain preserved. Attempt02 reran the seed from the beginning. [SUSPEND_PRESERVATION.json](SUSPEND_PRESERVATION.json) retains the evidence and checksums.

The first smoke, label a, agreed on all eight peers but dropped one tensor part and failed the oracle. Diagnostics and proper StepControl attachment were added; labels b and c passed exactly. The original cause remains unresolved. The successful pilots do not erase that diagnostic failure.

This is a private CPU toy baseline using sampled 8B RTTs and native protocol timings. A remote message leg receives `(RTT_i + RTT_j) / 2` milliseconds of delay. All eight are full DHT nodes; four act as reducers and four as averaging clients. Synthetic data, two-second training steps, readiness offsets, tiny payloads, equal reducer fractions and a fixed eight-peer population are explicit assumptions. No public churn, bandwidth, full pipeline execution or public throughput is reproduced. No public swarm was joined.

No experiments remain queued. Offline validation launches no peers:

```sh
python research/agora/run_8b_baseline.py --resume research/agora/results/baseline8b_pilot_20261013_a --dry-run
```

Exact resume (validates and skips both completed jobs):

```sh
python research/agora/run_8b_baseline.py --resume research/agora/results/baseline8b_pilot_20261013_a
```

Future suites need a unique label and a matching passed smoke. Frozen native packages, harness, input and dependency lock remain in this suite.
