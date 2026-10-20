# Simulator implementation

The agreed specification is implemented in dependency order. Existing Hivemind
and Agora results are read-only references. No public swarm interaction.

| Implementation status | State |
|---|---|
| A: contracts, packaging and configuration | Implemented |
| B: virtual engine, network, discovery | Implemented; analytic and differential tests |
| C: workload and accounting | Implemented; persisted trace replay agrees |
| D/E: Hivemind matchmaking and aggregation | Implemented as a documented model; not a full DHT replica |
| F: security metrics and policy fixtures | Implemented; exclusion, concentration, delay and credit fixtures |
| G: native validation | Driver and native fixtures implemented; calibration gates not yet passed |
| H: baseline suite | Serial 80-run driver implemented; full matrix withheld behind validation |

## Agreed experiment scope (2026-10-19)

| Baseline establishment | Implementation status |
|---|---|
| Fixed-resource design, 100 nodes, seeds 1–20 | Configured in `configs/controlled_baseline.json` |
| Numerical progress fix and focused correctness checks | Implemented; 68 tests passed |
| One honest pilot, seed 1, with frozen sources and raw trace | Completed; `results/controlled_pilot_20261019_b/` |
| Joint research review before expansion | User approved expansion on 2026-10-19 |
| Remaining 19 seeds and seed-level statistics | Complete: 20/20 audited; `results/controlled_baseline_100_20261019_a/` |

The earlier 40-run sweep was interrupted for profiling. Its partial outputs
are preserved under `results/controlled_baseline_20261019_a/` and are not a
baseline. Profiling artifacts are in `results/profile_cost_20261019_a/`.
The network completion timer now advances to the next representable simulated
time when a positive duration would round back to the current timestamp.
Run one corrected pilot, review whether the results support control-plane
research, then run the remaining seeds only after explicit approval.
The reviewed pilot completed at 180 simulated seconds, crossed the prior stall
time, credited 793 unique contributions (target 500 reached at 120.843 s), and
formed 118 groups. Its online accounting, replay, full-trace audit and source
fingerprints agree. These are one-seed controlled simulation observations;
the complete 20-seed baseline now measures seed-level variability.

The controlled baseline uses 64 MiB contributions in 4 MiB chunks, a global
target of five contributions per population member (5*N, not a per-node quota),
a 180-second observation window and a 600-second cap. Four-MiB chunks reduce
event volume while retaining chunked transfers; this is an explicit scenario
choice, not a claim about native deployed chunk sizes. Later attack/defence
comparisons must use the same configuration. The earlier 1,000-node matrix and 80-run heterogeneous
matrix is a separate, unexecuted design.

The goal is control-plane security research, not exact deployment replay. The
initial baseline may use constant honest-node compute times, bandwidth and
nonzero delays, with no churn. Randomized per-node initial readiness offsets
and multiple seeds are required for baseline, attack and defence comparisons;
pair those comparisons using the same seeds and environment settings. Existing
defaults provide uniform 0–10 second initial offsets and sweep seeds 1–20.
Check findings across delay settings and later modest jitter to detect dependence
on artificial synchronization or predictable timing.

Real deployment measurements inform plausible scenarios but are not required for
every input. The failed local timing calibration remains evidence of a fidelity
gap; assess its relevance to each security claim rather than treating it as a
blanket prohibition on controlled simulation experiments. The existing validated
sweep gate is still implemented for that separate native-validation workflow. The controlled
20-seed baseline is established in `results/controlled_baseline_100_20261019_a/`.

## Frozen collaboration contracts

Records live in `src/trainsim/types.py`. Do not change their fields without main-agent coordination.

- `Engine(seed=..., sink=None)` owns a SimPy environment, `now`, `stop_requested`,
  `schedule(delay, callback, *args) -> TimerHandle`, `call_at(time, callback, *args)`,
  `run(until)`, `emit(kind, actor="", **data)`. Sink signature is
  `sink(time, kind, actor, **data)`. No protocol state belongs here.
- `Network(engine, profiles)` implements `send(message, deliver) -> TransferHandle`
  and `cancel(handle)`. `deliver(message)` fires after transmission and propagation.
  Profiles is a mapping by node ID. Registry endpoint ID is `__registry__` with
  infinite bandwidth and 0.025s endpoint latency.
- `Discovery(engine, network)` implements `publish(node_id, advertisement, callback=None)`
  and `lookup(node_id, key, callback)`; callback receives `list[Advertisement]`.
- `Observer(path=None, labels=None)` exposes `emit(time, kind, actor="", **data)`,
  `summary() -> dict`, `close()`, `accepted_work` (float), `target_time` (optional).
  `target_work` is set by runner. Engine sink points to observer.emit.
- Observer consumes `work_ready` with `work=asdict(WorkItem)`, `group_formed`
  with `roster=asdict(GroupRoster)`, `aggregate_result` with `result=asdict(LocalAggregateResult)`.
  Other events are retained. Observer handles credit and emits `work_accepted`.
- `HivemindAdapter(engine, network, discovery, profiles, config, on_terminal,
  policies=None)`; methods `start(node_id)`, `on_work_ready(node_id, work)`,
  `on_message(node_id, message)`, `stop(node_id)`. `on_terminal(node_id, outcome)`
  starts the next compute, once per work item. Config is the resolved dictionary.
- Config keys: `population`, `profile`, `target_group_size`, `min_group_size`,
  `min_matchmaking_time`, `request_timeout`, `search_timeout`, `allreduce_timeout`,
  `sender_timeout`, `reducer_timeout`, `payload_bytes`, `chunk_bytes`, `target_per_node`,
  `observation_seconds`, `horizon_seconds`, `seeds`, `protocol`, `initial_bits`.
  `initial_bits=None` chooses floor(log2(N/(4*g))), lower-bounded at zero.
  `registry_latency_seconds` defaults to 0.025; low-delay native reference uses 0.
- RNG helper: `rng(seed, subsystem, identity="", sequence=0) -> random.Random`
  based on SHA-256, never built-in hash().
- Protocol emits `node_phase` on transitions and `work_terminal` on terminal operations.
  All deadlines reject arrivals at or after deadline. Timer cancellation uses handles
  plus attempt generation checks. Network resource update uses float seconds/bytes.

## Scientific boundaries

Fresh independent batch contributions; no recursive model state or convergence claim.
One full work credit requires all coordinates incorporated into one completed aggregate
at a different recipient. Partial results cannot be stitched across recipients or groups.
Control traffic and chunk-stream buffering are explicit approximations. Deployment
performance claims require relevant validation; controlled simulation claims must
state their assumptions. Validation failures remain inspectable and must not be
silently relabelled as passes.

## Completed controlled baseline (2026-10-19)

All 20 seeds reached 500 credits; none was censored. Target-time median is
120.668 simulated seconds (IQR 120.285–121.363; range 114.182–122.294).
Median fixed-window throughput is 4.425 contributions/s. Every node earned
credit in every seed by 180 s. Six work attempts failed across seeds 5, 8 and
20; raw traces show all six in searching phase at the 12 s search deadline.
No partial aggregation occurrences were observed. Expansion took 270.228 wall
seconds, including child runs and their audits. `suite_verification.json`
confirms evidence hashes and raw full-trace accounting across all seeds.

A reporting-only fix made group-size dictionary keys JSON-stable; five focused
integration tests passed. Seed 1 was repeated under the corrected driver and
its decoded trace exactly matched the reviewed pilot. The equivalence evidence
is saved in `pilot_reuse_verification.json`. Use this suite’s frozen config and
seeds for subsequent paired attack comparisons.

## First paired delay experiment

General plan: run only seed 1 with N0000 delaying each aggregation input/result
message by one second, including local reducer messages. Use the same baseline
configuration except the policy field; preserve the existing chunk model.
Architecture: the experiment driver invokes the existing runner and policy,
then reads raw traces and the baseline audit to compare fixed-window work,
the same 99 honest nodes, target time, grouping and failed work attempts.
Coding: `run_simple_delay.py` saves its config and script, verifies baseline
hashes, runs the child with a timeout, reuses `inspect_run`, checks identical
profiles/readiness/runtime sources, and writes JSON, a report and an HTML chart.
Acceptance: replay and audit pass; only N0000 records input/result delay actions;
compare one seed without interpreting it as a population-wide attack estimate.

| Implementation status | State |
|---|---|
| Existing-hook experiment driver and comparison | Implemented |
| Seed 1 execution and evidence checks | Complete; identical profiles, initial offsets and runtime sources; replay/audit pass |
| One-page paired result and interpretation | `results/simple_delay_1node_s1_20261019_a/COMPARISON_DASHBOARD.html` |

Observed seed-1 effect: target time 120.843 → 120.944 s; total credit at 180 s
793 → 771; same 99 honest nodes' credit 785 → 764 (2.675% lower). All nodes
still received credit; no failed work attempts or partial aggregation occurred.
The trace records 261 policy actions (128 input, 133 result). The first remote
action at 19.085461635 s corresponds to the first matching send at
20.085461635 s, confirming a one-second delay. See `mechanism_check.json`.
This one paired seed supports the experiment mechanism and an observed effect,
not a multi-seed attack efficacy claim. No additional attack seeds were run.

## Agora single-stage implementation (2026-10-20)

General approach: implement an honest, single-stage Agora model alongside the
existing Hivemind model. Run one 100-node seed-1 pilot (96 CLIENT contributors,
4 NODE reducers), then stop for review. Stage assignment is fixed; Authorizer,
pipeline flow, attacks and seed expansion are outside this implementation.

Architecture/data flow: config dispatch selects the Agora adapter and accounting.
The shared Engine and Network carry timed stage registration, step advertisement,
lookup and aggregation messages. Each node owns its expected count, accumulated
peer advertisements, sorted local roster and aggregation state. The registry is
an abstract timed service, not a replicated DHT. Local training continues during
averaging; every twentieth step takes a symbolic sparse snapshot when idle.
Receipts and application events feed unique sparse-snapshot accounting and a
separate offline replay. Raw traces feed the one-page epoch walkthrough.

Coding order and interfaces:
1. `protocols/agora/config.py`: strict scenario defaults and deterministic profiles;
   generic config/runner dispatch leaves the Hivemind branch intact.
2. `matchmaking.py`, `adapter.py`, `workload.py`, `aggregation.py`, `partitioning.py`:
   `AgoraAdapter(engine, network, profiles, config, seed).start()/stop()`;
   no shared canonical roster. Stage startup must finish within 60 seconds.
   Emit `measurement_started` only after each node has observed the population.
   Measurement windows are relative to that event; initial starts are seeded.
3. `accounting.py`: `Accounting(config).consume(event).summary()`;
   one complete selected snapshot from an origin incorporated at another
   recipient earns one credit, once globally. Never union partial recipient
   coverage. This unit differs from Hivemind's independent batch contributions.
4. `runner.py`: source/config/profile/seed snapshots, bounded execution, raw
   compressed events, online/offline reconciliation. `report.py`: standalone HTML
   with work curve, local roster timing, disagreement and per-epoch walkthrough.
5. Native pure-mechanics fixtures and focused tests; preserve old results. Run
   existing simulator regressions, then one pilot with a 600-second wall cap.

Acceptance: verify sparse selector cardinalities, byte-order sorting, reducer
allocation, timeout origin, stale/empty views, divergent rosters, workload overlap,
full/partial/duplicate accounting, replay and Hivemind regressions. Document any
remaining model limitations explicitly. No claim of Internet timing fidelity or
model convergence. Review the one pilot before any expansion or attack.

| Implementation status | State |
|---|---|
| Protocol config and experiment orchestration | Complete |
| Local Agora matchmaking, workload and aggregation | Complete; node-local rosters and stream timeout tests |
| Native fixtures, accounting and dashboard | Complete; online replay and independent receipt audit agree |
| Regression checks and single seed-1 pilot | Complete; 105 tests, plus 16 focused checks after final allocation adjustment; pilot finished |
| User review before expansion or attacks | Pending |

Agora pilot `agora_single_stage_s1_20261020_a` completed: 500 unique accepted
sparse snapshots (five per node), target reached at 222.725724 simulated seconds
after setup, observation ended at 240 seconds. All five epochs had one ordered
roster variant of 100 peers, observed independently by all 100 nodes. All 500
local averages completed; no failed/partial applications or skipped rounds.
Initial offsets ranged from 0.128017 to 9.951730 seconds. Online accounting,
trace replay and independent delivered-receipt reconstruction agree. Driver
runtime was 250.598 wall seconds including reporting/audits, below the 600-second
cap. This is a synthetic single-stage workload, not model convergence or a WAN
measurement. No attack or additional research seed was run. Review the dashboard
before expansion. The presentation-only label clarification is recorded in
`presentation_revision.json`; the original renderer remains in the source archive.
