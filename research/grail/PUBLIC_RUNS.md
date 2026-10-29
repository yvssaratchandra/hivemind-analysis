# Grail public-run evidence

The [official `grail-v0` article](https://templarresearch.substack.com/p/grail-v0-how-we-built-a-fully-open) reports one live-chain Qwen2.5-1.5B-Instruct run over 100 windows, roughly 320 trainer updates after evaluation-only windows. Its narrative reports pass@1 rising from about 3% to 41%, pass@5 from about 10% to 63%, and final results of 72.2% on GSM8K, 47.6% on MATH, and 25% on AMC 2023. These are publisher-reported summaries, not recovered per-step values; the article says the results come from one run. It discusses a trained checkpoint, but this archive does not verify an API-level finished state or independent run history.

The public Forge W&B page remained at “Loading project.” Unauthenticated metadata-only GraphQL requests to Forge and `api.wandb.ai` both returned HTTP 200 with `project: null`, so no run list or numerical history could be exported. The first Grafana attempt was partial because the capture helper blocked its WebSocket. A later capture allowed that connection and saved the public home dashboard. A narrowly scoped read-only Loki query then retrieved the miner leaderboard for the selected six-hour window: HTTP/query status 200 with zero rows. This does not establish whether older periods contain data; older Grafana windows were not exported. Home, leaderboard and operations dashboard definitions are preserved. The Substack article is archived as HTML/rendered DOM, PDF, and MHTML. The Qwen Hugging Face card is for the upstream base model, not a Grail checkpoint; no model weights or datasets were collected.

Evidence and reproducible artifacts:

- [Detailed capture report](data/extended_20261029/reports/public-data.md)
- [Source catalog and claim assessment](SOURCE.json)
- [Official article browser capture](data/extended_20261029/grail_v0_blog_public/manifest.json)
- [W&B project browser capture](data/extended_20261029/grail_wandb_project_public/manifest.json)
- [Successful Grafana leaderboard capture and query evidence](data/extended_20261029/grail_leaderboard_read_queries/manifest.json)
- [Original partial Grafana browser capture](data/extended_20261029/grail_grafana_public/manifest.json)
- [W&B metadata-only HTTP requests](data/extended_20261029/http/manifest.json)
- [Normalized public run metadata](data/extended_20261029/normalized/grail_wandb_runs.csv)
- [Numerical history availability table](data/extended_20261029/normalized/grail_wandb_numeric_history.csv)
- [Summary JSON](data/extended_20261029/summary.json)

The official source repository is preserved with full Git history at `upstream/`; its pinned `vendor/affinetes` submodule could not be initialized because GitHub requested credentials.

The Grafana-specific capture wrapper is [capture_grail_dashboard.py](../public-networks/capture_grail_dashboard.py). It permits only the official host's `/api/ds/query` POST endpoint, Loki datasource reads over at most six hours, and at most 24 query requests per capture. Other POST requests remain blocked. Exact read-query bodies and raw responses are included in each capture manifest.

To capture another dated leaderboard snapshot, use a fresh output directory:

```sh
research/public-networks/.capture-env/bin/python research/public-networks/capture_grail_dashboard.py \
  --url 'https://grail-grafana.tplr.ai/d/grail-leaderboard?from=now-6h&to=now&refresh=' \
  --output research/grail/data/NEW_LABEL/grail_leaderboard_read_queries \
  --allow-websocket-host grail-grafana.tplr.ai \
  --max-bytes 64000000 --max-response-bytes 16000000 --wait-seconds 15
```
