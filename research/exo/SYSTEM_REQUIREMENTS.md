# Exo requirements for basic local experiments

Checked against local Exo `21a54c5e`, 2026-10-06. **Resource figures are initial planning budgets, not measured minimums.** These basic experiments need no GPU, model weights, or training.

## Prerequisites

| Requirement | Basic tests and model-free peers |
| --- | --- |
| Platform | This lab targets Linux x86_64 |
| Native tools | Python 3.13, uv, Rust, C compiler, pkg-config; our lab uses Rust nightly |
| Docker alternative | Docker Engine + Compose v2; the image supplies build tools |
| CPU budget | 2 host cores to start; 4+ preferred for several peers/builds |
| Disk allowance | 20 GiB free for dependencies/image, Rust cache, and logs |
| Network | Unit tests use memory channels. Real peers need IPv6 sockets; discovery between peers needs non-loopback IPv6 and working multicast |

Dependency/image builds need Internet access. Build the image once and reuse it. Its Rust build uses two concurrent jobs; allow roughly **8 GiB available RAM during building**, separately from runtime budgets. This allowance is unmeasured and build RAM peak remains unknown. The image inspected during 005 (built under label 004) reports a **4.595 GiB logical image size**.

## Basic tests first

| Existing test | Exercises |
| --- | --- |
| [Election](../../exo/src/exo/shared/tests/test_election.py) | Candidate selection and election messages through memory channels |
| [Master](../../exo/src/exo/master/tests/test_master.py) | Coordinator event handling with fabricated events/model metadata |
| [Placement](../../exo/src/exo/master/tests/test_placement.py) | Assignment decisions using synthetic memory/network profiles |

These tests start no inference workers and load no weights. With native build tools installed, run from the repository root:

```bash
cd exo
EXO_DASHBOARD_DIR=/tmp uv run --extra mlx-none --group dev pytest -q \
  src/exo/shared/tests/test_election.py \
  src/exo/master/tests/test_master.py \
  src/exo/master/tests/test_placement.py
```

The command is source-reviewed but unrun here; it may first build dependencies. Explicit file selection avoids MLX-specific test collection. The peer Docker image omits pytest/dev dependencies. Unit-test RAM usage is unknown; the following caps apply to peer containers.

## RAM for real model-free peers

The [two-peer Compose setup](two-peer/compose.yaml) and [three-peer audit](audit/compose.yaml) choose **2 GiB per-peer caps**, configurable with `EXO_PEER_MEMORY`. Budget the sum of those caps plus **2 GiB spare host memory**:

| Peers | Sum of chosen caps | Available host RAM to budget |
| --- | --- | --- |
| 1 | 2 GiB | 4 GiB |
| 2 | 4 GiB | 6 GiB |
| 3: two honest + attacker | 6 GiB | 8 GiB |
| 4 | 8 GiB | 10 GiB |
| 8 | 16 GiB | 18 GiB |

This allocation rule, `2 × peers + 2 GiB`, covers all peers reaching their chosen caps simultaneously. A [Docker memory cap](https://docs.docker.com/engine/containers/resource_constraints/#memory) is a ceiling, not preallocated RAM. **This table does not establish a maximum peer count: 64 model-free peers remain a profiling target.** Measure actual memory, CPU, and election convergence at 2 → 8 → 16 → 32 → 64 peers before deciding what fits. Containers share physical CPU/RAM; record swap use because it can alter timing.

The 2026-10-06 model-free 005 audit ran five arms with two honest peers and a bounded publisher, each capped at 2 GiB. Across the 15 valid container-lifetime cgroup captures, the largest `memory.peak` was **143.8 MiB** for an honest peer (150,777,856 bytes) and **96.4 MiB** for the publisher (101,072,896 bytes); these peaks include startup and observer processes. There were no OOM events. This is observed usage from one short coordinator-only audit, not a minimum or a reason to lower the caps; it excludes host/build peaks and inference. See the [audit report](results/exo-audit-20261006-005/REPORT.md) and its raw results.

The [networking code](../../exo/rust/networking/src/lib.rs) attempts a direct connection for each mutually discoverable peer pair: 64 peers imply 2,016 attempted links, or 63 neighbors each. This is code inference, not an observed connection count; network buffers, discovery traffic, and election load also need profiling. The audit validated shared-image startup and IPv6 discovery, and an election between two honest peers with a third Router publisher that did not participate in the election; the API was enabled. The separate two-peer `--no-api` recipe remains unrun.

An earlier audit-preparation snapshot on an Intel Core Ultra 7 366H showed approximately **30 GiB total / 11 GiB available RAM**. The successful 005 preflight later reported **20,183,543,808 bytes (18.8 GiB) available** before launch. These are dated host-visible observations, not guarantees of a container's effective allowance.

## Experiments in the Hivemind style

1. **One peer:** startup/self-election, idle CPU, peak memory, and log growth.
2. **Two honest peers:** mutual discovery and agreement on a master; separately test graceful stop, crash, restart, and partition/rejoin.
3. **Attack/control comparisons:** add an attacker, preserve the honest control, and vary one condition at a time.

Follow [the two-peer instructions](two-peer/README.md) for honest startup or [the initial audit](audit/README.md) for two honest peers and one controlled publisher. The audit includes baseline, identity-claim, stale-session, and replay probes; partition/delay experiments remain to be added. Save revisions, peer IDs, limits, schedules/seeds where applicable, raw logs, and `docker stats` samples; sampling can miss brief peaks. Restarts generate new IDs. Injected local delays are emulation, not WAN measurements.

For comparison, the [recorded 64-peer Hivemind election test](../hivemind/results/step10_election_n64_l5_b1_blind-blindL-mirror-mirrorL-none.csv) used [DHT nodes in one Python process and sampled election decisions](../hivemind/step10_delay.py). A comparable Exo component harness could exercise election/placement at 64 simulated peers; it would need to be added and validated separately from real-process/network tests.

## Optional later: inference smoke test

The [Qwen3 0.6B 4-bit card](../../exo/resources/inference_model_cards/mlx-community--Qwen3-0.6B-4bit.toml) lists CPU support and **327 MiB of artifacts**, not runtime RAM. A tentative budget is **4 GiB per inference peer / 6 GiB available host RAM**, with short inputs and one request. CPU-backend compatibility and peak usage need validation; the current image disables inference workers.

For worker-enabled placement, [memory reporting](../../exo/src/exo/shared/types/profiling.py) has no explicit cgroup-limit handling. [OVERRIDE_MEMORY_MB](../../exo/src/exo/utils/info_gatherer/info_gatherer.py) changes advertised available RAM only; Docker enforces the actual cap. Verify both agree.

Sources: [dependencies](../../exo/pyproject.toml), [lab image](two-peer/Dockerfile), [startup flags](../../exo/src/exo/main.py), [discovery](../../exo/rust/networking/src/discovery.rs). The image build and three-peer model-free audit ran locally; the upstream Exo pytest recipe and standalone two-peer `--no-api` run remain unrun. No model weights or inference workers were used.
