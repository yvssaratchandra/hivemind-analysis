# Agora private baseline handoff

## Local reducer-mode probe (2026-10-15)

Completed and stopped: `probe_reducer_mode.py`, results
`results/reducer_mode_20261015_c/REPORT.txt`. Three loopback peers, one aggregation
round per arm, no authorization or integrity service. Native matchmaking and
allocation give X zero reduction elements advertising CLIENT, versus 32/96
advertising NODE; X returns four eight-element correction chunks to each honest
peer in the NODE arm. Independent raw arithmetic checks agree within 9.94e-8.
All peers stop normally; independent recorded-PID checks find no survivors.
This tests locally configured mode and resulting advertised behavior, not
deployment admission or a modified-client integrity bypass. No poisoning tested.
The source and evidence of the existing eight-peer baseline remain unchanged.
Labels a/b preserve successful CLIENT controls halted by a pre-worker-exit
shutdown-check mistake; final parent checks found no survivors. Label c fixes
closure ordering, completes both arms, and preserves the earlier failures.

## Eight-peer baseline

Approved scope: eight honest private peers, four reducers and four contributors,
one stage, synthetic CPU training. Native Agora DHT discovery, local roster
construction, sparse partitioning and all-reduce; upstream source unchanged.

## Implementation status

| Work | Status |
|---|---|
| Pinned supported CPU runtime and workload | Implemented |
| Frozen source, raw tracing, checkpoint/resume and oracle | Implemented; 26 focused checks passed |
| Eight-peer smoke, seed 8100 | Labels b and c passed; label a retained as a numerical failure |
| Two five-round pilots, seeds 8101 and 8102 | Completed and passed; no pending jobs |

**Stopped and safe to suspend.** All peers/controllers exited. Independent
process verification found no survivors among 192 recorded process IDs across
the smoke, interrupted and completed attempts. No further experiments are queued.

| Seed | Verified rounds | Roster size | Mean ready-to-roster | Initial mean loss | Final mean loss |
|---|---:|---:|---:|---:|---:|
| 8101 | 5/5 | 8 | 1.087s | 0.795260 | 0.001876 |
| 8102 | 5/5 | 8 | 0.952s | 1.172358 | 0.001494 |

These are ten logical rounds, verified through 80 peer-round results, with no
failed tensor parts. Maximum correction/application error was 2.61e-8 against
the 1e-5 gate. Each round assigned work to all four reducers. Both roles
contributed weight 1. The timing mean uses 40 peer-round observations per seed.
Native Agora group ID bytes are constant; identify logical rounds by trial and
epoch. No Hivemind-style leader election occurs in this path.

See [the result report](results/baseline8b_pilot_20261013_a/REPORT.md),
[independent verification](results/baseline8b_pilot_20261013_a/VERIFICATION.json),
and [shutdown audit](results/baseline8b_pilot_20261013_a/INDEPENDENT_SHUTDOWN.json).
Analyzer replays with pinned Python 3.11 reproduce both saved reports exactly;
26 focused tests pass. Frozen file sets, source hashes and completed raw-result
hashes validate. The root Hivemind source and Agora upstream checkout are unchanged.

Input: `RUN_8B_DATA_20261013.json`. Agora commit
`37d29a966528675635161007d7b43388012a98b2`; Hivemind commit
`4d5c41495be082490ea44cce4e9dd58f9926bb4e`. The runtime uses Python 3.11 and
CPU PyTorch 2.7. Native timings: 40s matchmaking, 150s averaging, 1s ad polling,
30s/60s sender/reducer timeout, stage announcements every 30s with 60s TTL,
replication factor 5. Training steps are artificially two seconds; five rounds
at steps 20, 40, 60, 80, 100. No attackers or departures.

The delay model samples four operator and four contributor dashboard records
within the UTC paper-date proxy, retaining positive RTTs <=80ms. Each remote
request and response leg is delayed by `(RTT_i + RTT_j) / 2` milliseconds; self
traffic is not delayed. Aggregate DHT telemetry is a comparison reference,
not another additive delay. Dashboard record timing and pairwise RTTs are
unknown. This is local delay emulation, not public-run replay.

Pass gates: all eight independently construct the same ordered roster each
round; all eight contribute to snapshot averaging; correction/application error
<=1e-5; zero failed tensor parts; finite training and decreased mean held-out
loss in pilots; clean closure and no surviving peer processes. Model weights
need not remain identical while independent training continues.

Each suite retains native packages, harness, dependency lock, input checksum,
seed/profile configuration, raw worker and DHT traces, numerical analysis,
shutdown audit and checkpoint. Offline resume validation launches no peers.
Suspend/clock discontinuities invalidate the trial. Use `--request-stop PATH`
and wait for a successful shutdown audit before suspending.

Smoke label a had one missing tensor part in R1 despite unanimous eight-peer rosters. The numerical oracle rejected it. Additional stream diagnostics and proper StepControl trigger attachment were added; labels b and c passed with zero error. The original intermittent failure is preserved and not claimed resolved.

Frozen pilot resume command (from the repository root):

```sh
python research/agora/run_8b_baseline.py --resume research/agora/results/baseline8b_pilot_20261013_a --dry-run
python research/agora/run_8b_baseline.py --resume research/agora/results/baseline8b_pilot_20261013_a
```

Pilot seed 8101 attempt01 was interrupted by a confirmed suspend
(boottime-minus-monotonic gap 2315.853 seconds), approximately 15:00:53–15:39:30
Stockholm time. Its first round had matching eight-peer rosters and correction
error 4.66e-9, but the whole attempt is invalid and excluded. Its raw traces,
configuration and checksums remain preserved in `seed8101_attempt01/` and
`SUSPEND_PRESERVATION.json`. Attempt02 reran the same seed from the beginning
using the frozen harness/native source and identical profile/training seeds.
Attempt02 and seed 8102 attempt01 both completed successfully.

The resume command above now validates and skips both completed jobs. Offline
dry-run was verified without launching peers. Seeded inputs control RTT sampling,
toy data, partitions and readiness offsets; native peer/DHT identities are fresh
and recorded per attempt. Keep the unresolved first-smoke dropped part visible
when interpreting future baseline reliability.

## Single-stage simulator pilot (2026-10-20)

Implemented the separate Agora adapter in `research/simulator/src/trainsim/protocols/agora/`.
Fixed assignment to one stage, 96 CLIENT contributors and 4 NODE reducers;
no Authorizer, placement algorithm, full pipeline or attack. Synthetic timings,
continuous local compute, randomized initial offsets, native-derived local
matchmaking, sparse tensor partitioning, CLIENT input-close behavior, and
per-stream timeouts. Three-part transport window remains an explicit approximation.

Pilot: `research/simulator/results/agora_single_stage_s1_20261020_a/`. Seed 1,
500 accepted sparse snapshots by 222.725724 simulated seconds, 240-second window;
five per node, five unanimous 100-member roster epochs, 500 completed local
averages, no failures/partials/skips. Driver finished in 250.598 wall seconds.
105 tests passed; final allocation adjustment passed 16 focused checks. Frozen
sources, raw trace, replay and independent receipt audit are retained. Open
`DASHBOARD.html`; observed-peer ranges there include intermediate discovery
polls, while final roster membership is shown separately. No additional seed
or attack run: await user review. This is not a WAN or convergence result.
