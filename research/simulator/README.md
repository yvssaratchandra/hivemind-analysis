# Decentralized training coordination simulator

`trainsim` models computation, discovery, grouping and tensor exchange in virtual
time, with separate Hivemind batch and Agora sparse-snapshot workloads.
Its research outputs are accepted-work throughput,
participant exclusion/representation, controlled-node concentration and waiting.
It does **not** train a model, measure convergence, execute Krum, or reproduce a
deployed Authorizer. Resource profiles are synthetic.

## Current verification (2026-10-19)

The **100-node controlled baseline is complete: 20 audited seeds**. Open the
[one-page baseline dashboard](results/controlled_baseline_100_20261019_a/BASELINE_DASHBOARD.html)
or [full report](results/controlled_baseline_100_20261019_a/REPORT.md). All seeds
reached 500 credits; median target time was 120.668 simulated seconds. All nodes
earned credit in each seed. Six individual work attempts expired at their
search deadline across three seeds; retain that normal failure count in attack
comparisons. Native timing validation remains a separate unresolved question.

The code and experiment drivers are implemented. **Timing fidelity has not passed
native calibration**, so the full 80-run baseline has not been executed.
The controlled 100-node baseline follows a separate pilot review workflow:
run seed 1, inspect its group formation, waiting, credit and replay evidence,
then run seeds 2–20 only after research review. The earlier 40-run attempt was
interrupted for profiling and is not a baseline.
The corrected seed-1 pilot is available at
[`controlled_pilot_20261019_b`](results/controlled_pilot_20261019_b/PILOT_REVIEW.md):
793 accepted contributions by 180 simulated seconds, target 500 reached at
120.843 seconds, 118 groups, and no uncredited node in this one seed. The
pilot passed source, readiness, trace and replay checks; it does not establish
between-seed variability. The simulator test suite passed 68 tests.
The [one-page pilot dashboard](results/controlled_pilot_20261019_b/PILOT_DASHBOARD.html)
visualizes progress, group sizes, node participation, and waiting. Rebuild it
from the saved pilot evidence with
`python research/simulator/render_pilot_dashboard.py research/simulator/results/controlled_pilot_20261019_b`.

```sh
research/simulator/.venv/bin/python research/simulator/run_controlled_baseline.py --mode pilot --label controlled_pilot_YYYYMMDD_a
```

The pilot saves `PILOT_REVIEW.md`, an accepted-work curve, group formation table,
and the full child trace. Expansion is a separate command with
`--mode expand --reviewed-pilot research/simulator/results/<pilot-label> --label <new-label>`.
The driver verifies the pilot's frozen configuration, sources, driver and evidence
before reusing seed 1. Expansion is meant for use only after explicit review.

The fixed-source comparison in
[`native_validation_20261019_b`](results/native_validation_20261019_b/calibration_low_s1_comparison.json)
passed configuration/source matching, native shutdown, completed-group rate and
group-size checks. Median/p90 grouping waits failed: about 0.4/0.4 ms simulated
versus 127/194 ms native in this local 120-second trial. Do not use simulated
timing as a validated Hivemind prediction. The abstract discovery/transport model
omits native routing and software scheduling costs; identifying and calibrating
their contribution is further work, not an established explanation of the gap.

Diagnostics completed for 100 nodes with 64 MiB contributions and for 1,000 nodes
in both resource profiles with 512 KiB contributions. The smaller-payload runs
check scale and accounting, not full-payload performance or model convergence.
Their manifests, frozen sources and raw events are retained under `results/`.
The executable test command and source hashes are recorded in
[`implementation_verification_20261019_a`](results/implementation_verification_20261019_a/verification.json).

## Install and focused checks

Python 3.11 is required. From the repository root:

```sh
uv venv --python 3.11 research/simulator/.venv
uv pip install --python research/simulator/.venv/bin/python -r research/simulator/requirements.lock
uv pip install --python research/simulator/.venv/bin/python --no-deps -e research/simulator
research/simulator/.venv/bin/python -m pytest research/simulator/tests -q
```

The simulator runtime imports neither Hivemind nor PyTorch. The small native
reference harness needs those packages in a separate dependency environment.

## First diagnostic run

```sh
research/simulator/.venv/bin/trainsim validate --config research/simulator/configs/baseline.json
research/simulator/.venv/bin/trainsim run --config research/simulator/configs/baseline.json --seed 1 --label my_smoke --population 100 --horizon-seconds 60 --observation-seconds 60 --target-per-node 1
research/simulator/.venv/bin/trainsim analyze --run research/simulator/results/my_smoke
```

Labels are unique; existing output directories are never overwritten. Single
runs describe a controlled scenario; deployment timing claims require separate
validation. The completed 20-seed suite establishes the controlled baseline.

Each directory contains resolved configuration, profiles, source/dependency
manifest, compressed raw events, online summary, independent replay, per-node
and group CSV files and a report. An incomplete work target is **censored**,
not silently discarded or called a successful completion. RSS is process peak
RSS, not an incremental per-run allocation measurement.

## Architecture and units

```text
configuration -> runner -> Engine (SimPy)
                        -> Workload -> protocol adapter
                        -> Network / Discovery
                        -> Observer -> raw trace -> offline analysis
```

- Nodes receive delivered messages and local observations. Registry/global
  observer state is not a protocol input.
- All capacities named `upload_bps`/`download_bps` in the internal profile record
  are **bytes per second**; the field names are historical contract names.
  Configuration `bandwidth_mbps` is decimal megabits/s, converted once.
- Endpoint upload/download capacity is shared by progressive max-min filling.
  Serialization is followed by propagation. There is no TCP or intermediate-link
  simulation. Control message sizes are synthetic and explicit in source.
- Discovery is an abstract expiring-record service, with no routing, replication
  or custodianship. Reads snapshot at request arrival and filter expiry again on
  delivery. Baseline registry endpoint latency is 25 ms; low-delay calibration
  sets it to zero.
- Each node computes one fresh batch, coordinates/averages, then computes again.
  Computation and averaging do not overlap. The input is not recursive model
  state, and repeated mixing is not measured as final model influence.
- A complete accepted work unit requires every coordinate of one contribution
  to appear in a completed aggregate at another recipient. Coverage cannot be
  combined across different recipients/groups. Retries/deliveries count once.
- Chunk receipts preserve effective contributor work IDs and fallback. A native
  operation can finish while retaining local values for missing reducer chunks.
  Such fallback is not full accepted work.
- Policies can alter actions at explicit hooks. They cannot grant themselves
  leadership, edit honest state, or inspect hidden observer labels.

## Baseline matrix

The first one-seed delay comparison is available in the
[paired dashboard](results/simple_delay_1node_s1_20261019_a/COMPARISON_DASHBOARD.html).
It delays N0000's aggregation input/result messages by one second using the
existing policy hook. The saved configuration changes only that policy from
baseline seed 1. The run, raw actions, audits and mechanism check are retained
in the same directory. Reproduce under a new label with
`research/simulator/.venv/bin/python research/simulator/run_simple_delay.py --label <new-label>`.

`configs/baseline.json` resolves to 100/1,000 nodes, controlled/heterogeneous
resources, and seeds 1–20 (80 runs). Each target is `100*N` accepted units; a run
also observes the first 600 s and has a 7,200 s horizon.

Controlled resources: 10 s compute, 100 Mbit/s in each direction and 25 ms
endpoint latency. Heterogeneous resources independently shuffle balanced classes
of 5/10/20 s, 50/100/200 Mbit/s and 10/25/50 ms. These are sensitivity assumptions.
Initial group-bit length is `max(0,floor(log2(N/(4*target_group_size))))`.
The reported Hivemind bandwidth is `None`: equal-score partition allocation is
separate from actual network capacities.

Nested passive labels cover 5%, 10% and 20% of the same population. All labelled
nodes remain honest in the baseline. Report roster concentration separately
from effective per-chunk membership and mixed-group honest exposure. The Krum
diagnostic only evaluates `n > 2*f + 2`; it proves neither poisoning nor failure
of an aggregation rule.

## Native validation

The new reference is `research/hivemind/simulator_native_validation.py`. It binds
only loopback and accepts no public bootstrap peers. It uses process/socket
facilities unavailable inside some sandboxes. Source Python files are staged
from this checkout with an existing installation's generated protobuf and p2pd;
their hashes are recorded and checked. Existing installations remain untouched.

Example staging, using an existing native dependency environment:

```python
from pathlib import Path
from trainsim.native_validation import prepare_native
prepare_native(
    Path.cwd(),
    Path("research/agora/.runtime/venv/lib/python3.11/site-packages/hivemind"),
    Path("research/simulator/.native-runtime/my_snapshot"),
)
```

Then run the private suite (approximately 40 minutes of native trial windows):

```sh
research/simulator/.venv/bin/trainsim native-suite --label my_validation --native-package-root research/simulator/.native-runtime/my_snapshot --native-dependencies research/agora/.runtime/venv/lib/python3.11/site-packages
```

The suite first runs deterministic native fixtures, then five calibration seeds
per condition. It stops on a failed calibration comparison. Held-out seeds
101–105 are tested in low/injected-delay conditions only after calibration passes.
Native child exit and source/configuration matching are gates too.

Grouping gates are median/p90 latency within 20% (50 ms absolute allowance),
completed-group rate within 20%, and group-size CDF difference <=0.15.
Held-out results are never used to tune parameters. Full DHT and bulk transfer
bandwidth fidelity are outside these grouping gates and remain explicit model
limitations. A failed suite is preserved; do not relabel it as validated.

Run the full baseline only with matching passed evidence:

```sh
research/simulator/.venv/bin/trainsim sweep --config research/simulator/configs/baseline.json --label my_baseline --validation research/simulator/results/my_validation/validation.json
```

`--diagnostic` permits an explicitly unvalidated matrix. It does not bypass a
scientific claim boundary: those outputs must remain labelled diagnostic.
Source changes invalidate earlier validation evidence.

## Tests and implementation status

See `IMPLEMENTATION.md` for module contracts and current status. Tests cover
analytic network sharing, record expiry, retries/late messages, complete/partial
credit, deterministic replay, concentration and a test-only non-Hivemind adapter.
Native artifacts are under `results/` with distinct labels; their manifests are
the source of truth for which experiments actually ran.

## Agora single-stage pilot

The Agora adapter is separate from the Hivemind independent-batch workload.
Its first scenario fixes one stage with 96 CLIENT contributors and 4 NODE
reducers. Reducers also contribute snapshots with weight one. Local compute
continues during sparse averaging; busy averaging opportunities are skipped.
The startup population check is a setup barrier only. Each averaging operation
subsequently discovers its own roster through timed registry messages.

The default synthetic workload has 16 float32 tensors of 1,048,576 elements,
selects one of 20 sparse partitions every 20 local steps (2 seconds per step),
and preserves tensor boundaries with a 2 MiB chunk limit. Every node has
100 Mbit/s upload/download; pairwise propagation is 50 ms. Initial compute
starts have independent seeded offsets in [0, 10] seconds after setup.

One **accepted sparse snapshot contribution** means a complete selected snapshot
from one origin has been incorporated at another recipient. Credit is unique
across recipients; partial coverage cannot be stitched across them. This unit
must not be compared directly with Hivemind's independent batch credit.

```sh
# Exactly seed 1; 600 wall-second cap; no automatic retries or expansion.
research/simulator/.venv/bin/python research/simulator/run_agora_pilot.py --label agora_single_stage_s1_YYYYMMDD_a
# Recheck the saved source archive and replay the saved trace.
research/simulator/.venv/bin/python -m trainsim analyze --run research/simulator/results/<label>
```

The experiment stops after the 240-second observation window and reaching 500
credits, or at a 600-second simulated horizon. Both windows start after setup.
The output includes `DASHBOARD.html`, `report.md`, `summary.json`, `replay.json`,
`events.jsonl.gz`, an independent `receipt_audit.json`, configuration, profiles and a hashed source archive. Inspect
this pilot before adding seeds or controlled-node behavior. No Authorizer,
stage-placement policy, full pipeline or numerical training is simulated.
Native fixtures check selected mechanics; they do not validate WAN timing.

Transport detail: `stream_window_parts=3` is an explicit synthetic bound on
in-flight parts per stream. Native Agora's prefetch of three refers to background
compression/deserialization, and its receiver queue can hold 64 parts; gRPC adds
further buffering. The simulation window is not a demonstrated native wire
limit. Timing conclusions involving buffering require sensitivity checks before
being generalized beyond this controlled scenario.

First Agora pilot completed: [one-page dashboard](results/agora_single_stage_s1_20261020_a/DASHBOARD.html)
and [report](results/agora_single_stage_s1_20261020_a/report.md). Seed 1 reached
500 accepted sparse snapshots at 222.726 simulated seconds; all 100 nodes
contributed five times. All local averages completed, with no failures or skips.
Online/replay accounting and the independent receipt audit agree. Driver wall
time: 250.598 seconds. Expansion and attacks await review.
