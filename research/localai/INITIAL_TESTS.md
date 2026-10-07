# LocalAI preliminary tests

Purpose: establish an inexpensive, repeatable local control-plane baseline before testing vulnerabilities. Research scripts live here; the independent source clone is `upstream/` and is unchanged.

## Implementation status

| Step | Status |
| --- | --- |
| Inspect cloned P2P source and model-free startup path | Complete; source commit recorded per run |
| Prepare private 1 → 2 → 3 peer runner and focused harness checks | Complete; six harness checks passed |
| Run upstream healthcheck regression and Docker preflight | Passed after session permissions changed |
| Measure actual LocalAI peers and crash/rejoin behavior | Complete: 1/2/3-peer LAN baseline, crash, restart and cleanup passed |

Successful run: [`results/baseline-20261007T121444/REPORT.md`](results/baseline-20261007T121444/REPORT.md), with [derived measurements](results/baseline-20261007T121444/analysis.json) and raw traces alongside it. Earlier attempts, including [`preliminary-20261007-001`](results/preliminary-20261007-001/REPORT.md), remain preserved as Docker permission failures. Source commit: `a3d555653c0cbfd73ab59ab15beab80903e96484`; the downloaded image's OCI revision matched it exactly. Upstream source and the Podman launcher are unchanged. The upstream regression script stubs curl and checks healthcheck URL/exit handling; the separate container run exercised actual LAN P2P discovery.

## Measured result: 2026-10-07

| Configuration | Median Docker memory per peer | First sampled complete peer views |
| --- | --- | --- |
| One peer | 61.1 MiB | 7.4 s |
| Two peers | 65.5 / 67.2 MiB | 19.7 s after starting this stage |
| Three peers | 65.2 / 73.1 / 65.5 MiB | 15.5 s after starting this stage |
| Three peers after restart | 67.8 / 73.9 / 61.2 MiB | 25.8 s after starting this stage |

These medians include discovery and the subsequent observation window. CPU medians were below 0.23% per peer across all stages. Highest recorded kernel cgroup `memory.peak` was **83.3 MiB** for one container, including sampling subprocesses. Sampled Docker memory reached 74.3 MiB. Recorded OOM counters were zero and final container states had `OOMKilled=false`. Empty-model API checks passed on all three peers. All experiment containers, their anonymous volumes and the internal network were removed successfully.

After SIGKILL, the first sample with both survivors excluding peer 3 from their **fresh** views began about 33.7 seconds after the kill command completed. The runner finished collecting that sample around 36.9 seconds into the crash stage; probing is sequential, so this is a sampled interval, not an exact expiry deadline. After restart, the survivors listed four service IDs: their own two, peer 3's expired old ID and its fresh new ID. The restarted peer listed three. The cache discrepancy is a useful observation for churn tests; no malicious manipulation or inference-routing consequence was tested.

**Feasibility:** this machine successfully ran three real, separate model-free LocalAI processes. Host-visible memory is 30.9 GiB with 16 logical CPUs. Basic discovery/liveness/churn experiments are inexpensive and required no RAM clearing or GPU. These figures apply to the stripped configuration below; adding a model, larger peer populations, private DHT discovery or WAN delays requires separate measurements.

## Run

From the repository root, with Python 3 and a working Docker connection:

```sh
python3 research/localai/run_baseline.py
```

Or use the **existing**, unchanged Podman launcher from your terminal:

```sh
./research/hivemind/codex-container/run.sh --docker command python3 research/localai/run_baseline.py
```

Each run creates a distinct timestamped directory under `results/`; reuse of a label is rejected. The script fetches the CPU image `localai/localai:sha-<cloned-commit-prefix>` if absent and verifies its OCI revision matches the clone. Upstream publishes this tag pattern in `.github/workflows/image_build.yml`. This run successfully fetched `localai/localai:sha-a3d5556`; availability for another source revision needs its own registry check. An unpublished image stops the test; there is no fallback to a different revision. `--image REFERENCE` accepts an already built image with the same revision label. No source build or model/backend download is performed.

Runtime uses a newly created **internal Docker bridge**, a new LocalAI-generated token and separate container hostnames. There are no host mounts, published ports, GPU devices, public pool tokens or host networking. DHT is disabled (`LOCALAI_P2P_DISABLE_DHT=true`); discovery uses mDNS. This checks LAN P2P federation, not the Internet/DHT path. Docker bridge multicast support still needs runtime verification; discovery failure must be diagnosed rather than counted as a LocalAI vulnerability.

The source's `run --p2p --federated` path advertises the HTTP service without needing model weights. `/v1/models` and `/system` must confirm there are no configured or loaded models before each new peer is admitted to the baseline. Gallery auto-loading, agents, assistant, MCP, runtime settings and UI are disabled. Results therefore describe this stripped model-free configuration, not default full-feature startup or inference.

## Measurements and controls

| Test | Observation |
| --- | --- |
| One peer; then two and three honest peers | Readiness, heartbeat advertisements, Docker memory/CPU and cgroup memory counters |
| SIGKILL peer 3; observe two survivors for 90 seconds | When its heartbeat expires, whether survivors remain healthy, and whether cached entries persist |
| Restart peer 3; observe convergence | Rediscovery and resource use; a restarted process may advertise a new service identity |

Observation is 60 seconds after first matching membership for each idle/rejoin stage, with a 180-second discovery timeout. Sampling runs approximately every five seconds plus command time. Reports preserve listed advertisements and separately classify locally timestamped records younger than 40 seconds as fresh. This is an advertisement/freshness measurement, **not proof of a working inference route or a full mesh of connections**. Cached stale records alone are not an attack finding.

Each peer has a **chosen 1 GiB memory ceiling and one CPU limit**, with swap disabled. Three peers therefore have a combined configured memory ceiling of 3 GiB; this is **not measured RAM consumption or a demonstrated minimum**. Docker storage/page cache and the host/container running the harness add overhead. Actual median and sampled maximum container memory are summarized; raw cgroup `memory.peak`/`memory.events` are retained where supported. Docker memory is not process RSS, and the short curl/shell probes perturb it slightly.

Artifacts: `summary.json`, `commands.jsonl`, `source-healthcheck-test.json`, `REPORT.md`; after startup, also `image.json`, `network.json`, `samples.jsonl`, empty-model controls, process states and redacted peer logs. Cleanup removes only this run's containers, their anonymous volumes and network. LocalAI provides no random-seed flag here: identity randomness is recorded through actual advertisements and the token hash; a repeat creates a fresh network rather than identical identities.

Recalculate memory, expiry timing and cached-ID observations from raw traces:

```sh
python3 research/localai/analyze_baseline.py research/localai/results/baseline-20261007T121444
```

## Before a thorough audit

1. Establish honest discovery, stable resource use and crash/rejoin behavior using this baseline.
2. Add bounded membership controls: a different token should not discover the honest services; separately check how `P2PNetworkID` changes advertised groups. Observe both discovery and actual service access before interpreting isolation.
3. Test partition/rejoin and stale/future timestamps in an isolated controlled setup. Measure routing consequences, not just node-list changes.
4. Construct a private DHT bootstrap network using the pinned EdgeVPN dependency, then repeat discovery controls with mDNS bypassed. The current LocalAI configuration keeps mDNS enabled, so a passing LAN test does not establish DHT behavior.
5. Add one tiny CPU model or a source-supported mock backend to verify actual routing/failover. Only then test bounded metadata manipulation/resource exhaustion with two honest peers and one controlled peer.

Source references in `upstream/`: `core/application/p2p.go` (`StartP2P`, model-state sync), `core/p2p/p2p.go` (discovery, service ads, `newNodeOpts`), `core/p2p/node.go` (cached node list), `core/schema/localai.go` (40-second heartbeat check), and `core/http/endpoints/localai/p2p.go` (node/token API). These explain code behavior and experiment design. Runtime findings above are supported by separate local traces; neither code inspection nor this honest baseline establishes an exploitable vulnerability.
