# Large-population simulation of contribution exclusion

Saved 2026-10-23. Status: plan only; implementation and experiments have not started.
This preserves the proposed plan from the conversation. Native behavioral gates
and timing limitations below remain unresolved.

## Objective and experiment definitions

Measure how gradient exclusion, parameter exclusion, and their combination affect
time to convergence at 100 and 1,000 nodes.

Use the existing simulator framework, asynchronous matchmaking, target group size
eight and minimum two. Build fresh learning baselines: the earlier large-population
baseline measured accepted work, not model convergence.

| Condition | Exclusion scope | Attacked channels |
|---|---|---|
| Baseline | None | None |
| Single-gradient | One follower per formed group | Gradients |
| Single-parameter | One follower per formed group | Parameters |
| Single-both | One follower per formed group | Both |
| Group-gradient | Each group led by designated node X | Gradients |
| Group-parameter | Each group led by designated node X | Parameters |
| Group-both | Each group led by designated node X | Both |

**Single-node policy:** choose a nonleader uniformly using a reproducible policy
RNG. Use the previously tested announcement behavior: other participants omit the
target, while the target receives the original roster. The protocol model
determines accepted contributions and fallback; do not implement exclusion by
simply assigning zero weight.

**Whole-group policy:** send each follower a singleton roster containing only that
follower, with aligned metadata. X also uses a singleton local roster. Validate
that native Hivemind accepts this and produces unchanged local tensors before
interpreting it as local-only learning. This mechanism may complete without the
timeout experienced by the single-node omission attack.

**Leadership assumption:** X never follows another leader and forms successive
groups as peers become available. Apply this same fixture in every condition,
including baseline. Model no record-holder capture or acquisition probability.
With asynchronous matchmaking, there is no global round: report X's successive
groups, their changing membership, and actual exposure frequency. The single-node
conditions separately assume exclusion capability at every group leader.

## Learning, timing, and measurement

- Give every node equal compute speed, batch size, bandwidth and contribution
  weight. Use independent iid data streams keyed by seed, node and local batch index.
- Use the established synthetic workload: 48-dimensional noiseless Gaussian
  linear regression, zero initialization, batches of 32, and SGD learning rate 0.1.
- Define each local cycle as compute gradient → gradient averaging → SGD update
  → parameter averaging → next cycle. Parameter matchmaking uses a separate
  namespace and may select different peers. Disable overlap for this first study.
- This is a controlled distributed SGD workload using native averaging behavior.
  It does not emulate the entire Hivemind Optimizer's global progress and recovery
  machinery.
- Reuse baseline resource assumptions: ten simulated seconds per batch,
  100 Mbit/s upload/download, 25 ms endpoint latency, and initial readiness offsets
  uniformly distributed over ten seconds. Preserve existing matchmaking and
  stream deadlines.
- Model 64 MiB transfers with 4 MiB chunks on both channels. Keep numerical
  tensors compact using a virtual zero-padded representation: three trainable
  coordinates at identical offsets in each of sixteen chunks. Partitioning and
  wire bytes use the full virtual layout. This makes communication costs explicit
  without allocating large tensors per simulated node.
- Label elapsed-time results as simulated seconds under these synthetic
  resources. Existing native timing calibration has not passed.

The primary endpoint is the first time mean per-node evaluation loss remains
below 10% of its initial value for ten simulated seconds. This averages losses
of individual models, not the loss of an averaged model.

Maintain evaluation loss incrementally after model changes, using the fixed
4,096-example evaluation set's sufficient statistics. Verify this calculation
against direct evaluation. Schedule a confirmation event when the threshold is
crossed and cancel it if the mean rises again.

Also report:

- Absolute time difference and time ratio against the paired baseline.
- Median, p90 and worst-node loss; model disagreement.
- Loss and participation of attacked nodes.
- Completed updates, generated samples, incorporated contributions and fallback.
- Actual group sizes, attacked groups, affected participants, communication
  volume and waiting time.

Use an 86,400 simulated-second maximum, stopping early once convergence is
confirmed. An unfinished run remains censored; never substitute its cap for
convergence time. Support deterministic reruns with a larger horizon and verify
the common event prefix when extending a run.

## Architecture and implementation order

Extend the existing framework under `research/simulator/`; keep native reference
harnesses and research documentation under `research/hivemind/`. Do not modify
root `hivemind/`.

| Implementation status | Initial state | Completion gate |
|---|---|---|
| Experiment contract and configuration | Planned | Definitions, seeds and assumptions frozen |
| Native behavioral references | Existing omission evidence; singleton unvalidated | Roster, output and fallback checks pass |
| Multi-group numerical integration | Missing | Protocol and numerical replay agree |
| Learning controller and reporting | Missing | Independent endpoint reconstruction passes |
| Scale verification and configured suite | Missing | 1,000-node resource check passes |
| Experiment execution | Not started | Separate execution step after review |

Implement in this order:

1. **Configuration and native reference harness.** Add a separate learning
   experiment configuration and configure-only entrypoint. Prepare identifiable
   tensor probes for honest, single-node omission and singleton announcements
   on both channels, covering formed group sizes two through eight. Reuse
   existing evidence where it matches exactly.
2. **Generic protocol support.** Extend the event-driven all-reduce model to
   retain recipient-specific rosters, coordinate allocations, accepted sender
   streams and partial results. Use actual stream outcomes for numerical
   updates. Preserve local coordinates when native fallback applies; do not
   replace partial results with a fabricated complete mean. Keep omission and
   singleton choices in policies.
3. **Two-channel learning controller.** Own per-node models, deterministic batch
   generation and update counters. Instantiate gradient and parameter adapters
   with separate namespaces over the same network and physical endpoint
   capacities. Pass immutable input snapshots into aggregation and apply each
   returned result exactly once. Expose an optional result callback carrying
   per-chunk provenance and numerical output; preserve existing throughput callers.
4. **Evidence and reporting.** Add streaming traces, source/configuration
   manifests, independent numerical replay and convergence analysis. Record
   attempted and effective rosters separately, including singleton announcements.
   Produce paired comparisons and loss-over-time plots.
5. **Scale preparation.** Bound retained state to active groups and stream
   completed evidence to disk. Avoid storing historical tensors in memory.
   Add distinct commands for configuration, validation, pilot and confirmation
   so implementation does not launch a sweep.

Keep the older dominant-compute experiment and its evidence intact. Its
unequal-rate sweep and C0–C3 execution chain are not prerequisites for this study;
the new model gets its own focused validation gates.

## Tests and acceptance gates

### Native behavior gate

Validate local outputs, accepted source coordinates, fallback ranges,
completion/failure status and relevant timeout behavior. Singleton acceptance is
currently supported by source inspection, not a completed native reference. If it
behaves differently, preserve that result and revise the model before running
convergence comparisons.

### Numerical and integration gate

Cover honest averaging, omission, singleton groups, minimum-size groups, unequal
chunk allocations, partial streams, timeout boundaries, channel independence and
both-channel exclusion. Compare small trajectories against native references:
marker tolerance `1e-4`; gradients/models `atol=1e-5`, `rtol=1e-4`. Unsupported
behavior must stop the run explicitly.

Verify that no attack-specific decisions enter aggregation code, loss replay
reconstructs the reported endpoint, and unchanged throughput scenarios retain
their deterministic accounting.

### Scale gate

Run a short 1,000-node diagnostic before a full pilot. Measure wall time, peak
memory, event count and trace growth. Require bounded active-state memory and
successful evidence replay. Any runtime optimization must preserve outputs and
timing; do not silently reduce payloads or change protocol semantics to make the
run cheaper.

### Experiment sequence

- Pilot: seed 601, seven conditions at both populations — 14 runs.
- After correctness and resource review: seeds 602–606, same matrix —
  70 confirmation runs.
- Pair problem, data, readiness and policy RNG streams across conditions. Allow
  group trajectories to diverge naturally when treatments alter timing.
- Report paired seed-level results and uncertainty, including negative or
  inconclusive results. Do not automatically expand seeds or add parameter sweeps.

## Deliverable and boundaries

Deliver configured experiments, focused tests, validation reports, and this
inspectable research document with its implementation-status table. Review
changes, commit them in logical units, and push while preserving unrelated work.

Exclude recovery downloads, heterogeneous compute, record-holder simulation,
capture acquisition, poisoning, additional group-size sweeps and public-network
experiments from this study.

Respect the existing do-not-run-yet instruction: implementation may perform
offline checks, but native probes and simulation experiments remain explicit
subsequent execution steps.
