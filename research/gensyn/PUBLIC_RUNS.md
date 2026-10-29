# Gensyn RL Swarm: public-run evidence

This folder contains an immutable, bounded snapshot of publicly accessible RL Swarm evidence and an official full-history source checkout. It does **not** contain model weights or results from a training job run by this project.

## What the public evidence shows

The latest dashboard snapshot was collected on 29 October 2026 from 10:13:06.439859 to 10:16:59.222721 UTC. The dashboard APIs are point-in-time endpoints, not an archive of past run records. The primary dashboard and the legacy math dashboard returned separate data; do not combine their counters as though they describe one training run.

| Public source at retrieval | Observed response | Interpretation limit |
|---|---|---|
| [Primary dashboard](https://dashboard.gensyn.ai/) | `nodes-connected` 20,901; `round-stage` 28,239 / stage 0; 100 leaderboard entries; 10 top-reward entries; `unique-voters` 177,244; 2 gossip messages | A snapshot of those endpoint values. The API does not name the model attached to each peer or expose hardware time. |
| [Legacy math dashboard](https://dashboard-math.gensyn.ai/) | `round-stage` 927 / stage 2; 100 leaderboard entries; 10 top-reward entries; `unique-voters` 66,475; 100 gossip messages. Its `nodes-connected` endpoint returned HTTP 500 in both recorded attempts. | A separate legacy endpoint response captured at retrieval time; its exact historical relationship to the primary dashboard is not established by the returned data. |
| [Legacy swarm host](https://swarm.gensyn.ai/) | Dashboard HTML and its same-host JavaScript bundle returned HTTP 200. | The host was accessed directly and redirects were not followed for this probe. It did not return a 403 in this snapshot. |

The raw leaderboard schema includes `peerId`, `peerName`, `reward`, `score`, and `online`. The dashboard JavaScript maps `score` to the displayed `participation` value and passes through `reward`. These are platform counters; the endpoints do not define them as GPU-hours, tokens processed, wall-clock contribution, or an allocation of compute. For example, the first primary-dashboard row had `score=141832`, `reward=746058`, `online=false`; the first legacy math row had `score=839`, `reward=150632`, `online=false`. These examples are not comparable across the two dashboards without a documented common scale. The separate `unique-voters` count is not the number of rows returned by the top-100 leaderboard.

The snapshot preserves 102 gossip-message records across the two dashboards, the current top-reward lists, and both leaderboard responses. It contains no time series for model loss or per-node training progress. The public dashboard endpoints therefore do not answer how much compute an individual peer supplied or how a particular model converged.

## Models and phases

The [Gensyn RL Swarm technical report](https://github.com/gensyn-ai/rl-swarm/blob/286c17e5391fa3e9ebba64a8e683bb36e47376a8/technical_report.pdf) describes an earlier experiment: three Qwen-2.5-1.5B models jointly trained on a mathematical reasoning dataset for 500 iterations, compared with solo GRPO. The report plots pointwise and time-averaged relative regret and answer length and reports qualitative differences in answer quality. Its extracted text spells the dataset name once as “GMS8K”; the commonly named dataset is GSM8K. The report is a short research artifact, not raw per-node telemetry: it does not provide downloadable loss curves, model checkpoints, or node-by-node training histories.

The source repository and official docs describe the later CodeZero phase as collaborative coding, following the Reasoning Gym/math phase. The repository README lists Qwen2.5-Coder-0.5B-Instruct as a Solver and Qwen2.5-Coder-1.5B-Instruct as a frozen Evaluator. The checked-in CodeZero configuration is more flexible: it permits an environment-selected model and has DeepSeek-Coder/Qwen-Coder defaults. The [CodeZero docs](https://docs.gensyn.ai/testnet/rl-swarm/how-it-works/codezero) describe Qwen 2.5 Coder models for Solvers and Qwen 3 4B models for Proposer/Evaluator roles. These versioned descriptions differ, and none is joined to a dashboard peer ID in the collected data. Treat them as configuration/documentation evidence, not proof of which model a public peer actually ran.

The full-history clone also preserves two useful historical configurations. At tag `v0.6.4` (17 October 2025), the Reasoning Gym configuration selected from large/small model pools, with `MODEL_NAME` as an override; those are defaults, not evidence of which model an individual node actually used. At tag `v0.7.0` (12 November 2025), the CodeZero configuration similarly permits an environment override and lists Qwen/DeepSeek defaults. Their commit and content hashes are recorded in [`SOURCE.json`](SOURCE.json) so the exact versions can be recovered from `upstream/`.

The public [Hugging Face RL Swarm collection](https://huggingface.co/collections/Gensyn/rl-swarm) returned five curated model repositories. The two Qwen2.5-0.5B/1.5B repositories carry `rl-swarm` tags in their card metadata; the collection also contains Qwen2.5 7B, 32B and 72B repositories. Their card metadata and README model cards are saved in the snapshot. A separate paginated name search returned 11,223 distinct repositories containing “Gensyn-Swarm” in the name. Those are candidate self-published repositories, not verified training participants. Neither collection membership nor a model-card tag proves that a model was used in a particular public run.

Official phase context is archived from the [Reasoning Gym/CodeZero overview](https://docs.gensyn.ai/testnet/rl-swarm/how-it-works), [legacy-environment docs](https://docs.gensyn.ai/testnet/rl-swarm/how-it-works/legacy-environments), and Gensyn’s [CodeZero announcement of 12 November 2025](https://www.gensyn.ai/news/codezero-extending-rl-swarm-toward-cooperative-coding-agents). The RL Swarm docs page captured in this snapshot stated that no official swarms were running at retrieval time, while CodeZero-specific pages and parts of the repository README used “active/current” language. These statements are preserved as dated evidence rather than resolved into a claim that training was live at collection time.

## What was not found

The identified public dashboard endpoints expose current counters, ranked rows, and bounded gossip responses, but no historical per-node train-step traces or model-convergence series. The official source repository’s CodeZero YAML sets `log_with: wandb`, and the README describes W&B logs as dependent on configuration; however, the repository and official pages consulted do not link a public global run/project or a peer-specific W&B run. No such public run history was therefore available to join to the dashboard rows in this collection. This is a discovery limitation, not a claim that no W&B runs exist.

No Internet Archive response is archived for this snapshot, so archive availability was not verified. See the [discovery log](DISCOVERY_LOG.md).

On-chain history reconstruction was outside the approved scope. No login or account-only dashboard routes were queried.

## Reproducibility and files

- [`SOURCE.json`](SOURCE.json) records the official source URL, full-history commit, license, and hashes for the checked-out README, technical report, and license. The clone is in ignored `upstream/` and is at commit `286c17e5391fa3e9ebba64a8e683bb36e47376a8`.
- [`INVENTORY.json`](INVENTORY.json) is the tracked inventory for the latest snapshot. It records the snapshot and manifest hashes, every requested URL, response path/status/hash/byte count/retrieval time, and references to earlier preserved attempts. It also links to the [discovery log](DISCOVERY_LOG.md), which documents the gap in retained Internet Archive evidence.
- Latest ignored raw snapshot: `data/public_runs_20261029T101259Z/`. Its manifest has 48 response entries (including failed endpoint retries); 47 entries have saved response bodies, totaling 18,403,466 bytes. The entries record 46 HTTP 200 responses and 2 HTTP 500 responses. Hugging Face search pagination completed after 12 pages; the final page had no `Link: rel="next"` header.
- Normalized files are `data/public_runs_20261029T101259Z/normalized/nodes.csv`, `top_rewards.csv`, `models.csv`, `metrics.csv`, `gossip_messages.csv`, and `summary.json`. The complete raw bodies and per-response hashes remain in the ignored snapshot.
- Earlier exploratory snapshots remain preserved under `data/` and are listed in `INVENTORY.json`; the latest normalized output supersedes their partial/error results.

The collector only makes public GET requests, waits at least 1.05 seconds between requests to the same host, retries transient failures a bounded number of times, and refuses to overwrite or silently replace a saved response whose hash or size no longer matches its manifest. Raw collection is capped at 4 GiB. It fetches model metadata and model-card README text, not model weights.

Run the focused offline tests with:

```sh
python -m unittest discover -s research/gensyn -p 'test_*.py' -v
```

Regenerate normalized files and verify all saved raw hashes without network access with:

```sh
python research/gensyn/collect_public_runs.py \
  --output research/gensyn/data/public_runs_20261029T101259Z \
  --analyze-only
```

For further collection, use the same snapshot path with `--resume`; verified successful responses are reused, and incomplete/failed requests can be retried. Choose a fresh timestamped directory for a new point-in-time snapshot.
