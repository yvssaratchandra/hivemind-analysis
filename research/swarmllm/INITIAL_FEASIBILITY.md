# SwarmLLM: initial local-testbed feasibility

**2026-10-07. Assumed target: [enapt/SwarmLLM](https://github.com/enapt/SwarmLLM), pending clarification of “LocalLLM.”** This is a preliminary source/document assessment and a measured environment preflight, not a running testbed. No peer was started and no public swarm was joined. Exact upstream revision and configuration must be pinned before running.

| Question | Current answer |
|---|---|
| Can we create a local testbed? | **Likely yes.** Upstream [architecture documentation](https://github.com/enapt/SwarmLLM/blob/main/docs/ARCHITECTURE.md) includes separate node data directories, ports, and loopback bootstrapping for multi-node tests. Actual isolated startup remains untested here. |
| RAM consumed per node? | **Unknown; not measured.** Separate empty-node startup/idle peaks, discovery/churn peaks, and model-loaded inference. The README's 8 GB recommendation concerns running the application/models, not a measured model-free node minimum. |
| Can this machine run basic tests? | **Likely:** preflight reports 30.9 GiB total, 13.6 GiB available RAM, 16 logical CPUs, and about 922 GiB disk free. These are host-visible readings, not guaranteed limits. CPU-only control-plane tests are the first target; GPU capability was not checked. |
| What currently prevents measurement? | Docker daemon access returns permission denied; Rust/Cargo are absent; a direct source fetch failed DNS resolution under current network restrictions. Web documentation access works. No dependency or launcher changes were made. |

## First testbed

Start with one empty peer, then two honest peers, then a third controlled publisher. Give each a separate identity, storage directory, ports, and logs. Establish a private network with outbound Internet access blocked during runtime; explicitly disable public bootstrap, relays, automatic model downloads, and updates. A custom gossip name alone is insufficient isolation. Confirm no external connections before any behavioral test. Exact settings require source inspection and a pinned build.

For profiling, initially **choose a 1 GiB ceiling per model-free peer**, with three peers plus 3 GiB spare host RAM: **6 GiB available RAM budget**. This is an experimental cap, not observed consumption or a demonstrated minimum. If startup exceeds it, record the failure and revise the budget. Build-time memory is separate and remains unknown. There is no evidence yet that clearing RAM is necessary for the initial model-free stage.

Capture whole-process-tree RSS/PSS, container/cgroup current and peak memory, OOM/swap events, CPU, traffic, and raw logs. Record one peer's startup peak and 5-minute idle baseline, then the same for 2 and 3 peers. Do not extrapolate to 64 peers before profiling connection count and traffic at increasing sizes. Use synthetic model metadata where appropriate; it does not establish working inference.

## Primary experiments, in order

| Step | Experiment | Evidence to collect |
|---|---|---|
| 1 | **Isolation and resource baseline:** empty nodes at 1 → 2 → 3 peers | Unique identities, external connection absence, startup/idle RAM, CPU and traffic. |
| 2 | **Honest discovery:** advertise a bounded test capability/shard entry | Agreement on peer and holder identity; discovery latency; consistent registry state. |
| 3 | **Churn and recovery:** graceful stop, crash, restart, partition/rejoin | Withdrawal/staleness timing, retries, duplicate records, and recovery. Added delays are local emulation. |
| 4 | **Identity and freshness:** controlled publisher varies one identity/capability claim or replays an old advertisement | Which fields are authenticated; acceptance versus honest control; whether accepted metadata changes routing decisions. A lying advertisement alone is not demonstrated inference impact. |
| 5 | **Bounded load:** gradually increase local peers and advertisement rate under fixed limits | Memory growth, backpressure, message/connection counts, and whether honest discovery remains responsive. |
| 6 | **Optional CPU inference smoke test:** one supported small quantized model, short input, one request; compare local versus split execution | Actual inference RAM, successful tensor transfer, output validity and failover. Run only after model-free checks pass. |

Upstream documents existing integration/unit tests and explicitly says its proposed tiny synthetic multi-process model fixture was never built. A model-free harness is therefore not equivalent to an already available tiny-model end-to-end test. Tests here must be selected and executed before claiming upstream coverage is sufficient.

Advance to a thorough audit only after the honest baseline is reproducible, isolation and resource budgets are demonstrated, and an experiment has an observable security consequence consistent with the promised trust boundary. Preserve the revision, configuration, seeds, controls, schedules, and a unique result label for every run.

Evidence: [preflight script](preflight.py) and [raw preflight](results/preflight-20261007-001/preflight.json). Repeat with a new label:

```sh
python3 research/swarmllm/preflight.py research/swarmllm/results/preflight-YYYYMMDD-002
```
