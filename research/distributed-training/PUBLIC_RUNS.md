# Distributed Training public-run snapshot

**Status: bounded snapshot captured on 2026-10-29; network collection is stopped.** W&B run-list pagination reached the advertised run counts for DeMoCo (5/5), miners (2,627/2,627), and validators (92/92), but that does not mean their metric histories are complete. Of 364 runs whose sampled history count differed from or was unavailable in `historyLineCount`, step-window scans cover 100; 92 pass both archived `lastStep`-range and row-count comparisons, leaving 272 candidates unscanned or not fully validated. The scanned subset returned 1,438 of 1,515 expected ranges and 465,293 unique rows. Treat this as historical partial evidence, not a complete training-history archive.

The ignored snapshot directory occupies about 14.1 GB, including 9,060,911,067 bytes of archived raw responses and normalized sampled/full-history JSONL outputs. The `--max-bytes 12884901888` (12 GiB) value used for capture limits archived raw response bytes only; it does not cap derived output, which is why the directory is larger than the raw-byte limit. Further network collection needs a separately chosen storage budget that includes both raw and normalized files. Do not resume this snapshot as the routine next step.

The public source checkout is at `upstream/`. It is pinned in [SOURCE.json](SOURCE.json) to commit `082f3a632cd412e688a325356af08e34f01d8b8e`; the upstream README identifies the code as MIT licensed. `upstream/` is intentionally ignored so its history and any large Git LFS objects do not enter this research repository.

The collector is [collect_public_runs.py](collect_public_runs.py). It stores public dashboard responses, public W&B run metadata and metric histories, and linked Hugging Face model metadata under `data/public_20261029/`. Large snapshots are ignored by Git. The root `manifest.json` records the URL, retrieval time, response status, stored and decoded byte counts, content encoding, and SHA-256 of the exact saved response bytes. W&B gzip responses are archived in compressed form and decoded with a separate 128 MiB safety cap for analysis. [INVENTORY.json](INVENTORY.json) gives a tracked index into that snapshot; [REQUESTS.json](REQUESTS.json) stores the safe GraphQL query bodies and variables for reproducibility.

W&B run pages request identity, state, timestamps, reported history counts, and sampled history. Miner and DeMoCo pages request `samples:5000`. Validator pages use one run per request and fall back from 5,000 to 100 then 10 samples when a decoded response exceeds the collector's 128 MiB cap; each query variant has its own manifest entry. For histories whose sampled row count differs from or is unavailable in `historyLineCount`, the collector queries the public SDK's `historyKeys.lastStep` and requests bounded step windows using the public `HistoryScan` pattern. Validator scans use 100-step windows and batch up to five aliases per request; miners and DeMoCo use 1,000-step windows and batch up to five aliases. If a decoded multi-window response exceeds the cap, it is retried as smaller alias batches. The normalized sampled and window-scan histories are streamed as sparse gzip JSONL, retaining each record's metric dictionary without padding thousands of absent columns. Per-run coverage records expected/returned ranges, raw and unique row counts, duplicate rows, and count comparisons. A matching row count is evidence, not proof that W&B's event store was fully captured. The production collector excludes full run configs; no full configs were archived, and a safe config projection was not verified.

The dashboard's Mechanism 1 history and strategy records are a separate dataset. Its `best_loss`, `best_communication`, and `best_throughput` values summarize selected best metrics for a block; they are not one global model-convergence curve. Strategy rows preserve the benchmark flag and are deduplicated when the same record appears more than once.

Model and dataset names are source context, not per-run attribution. The repository defaults are model `llama-4b-ws-4`, tokenizer `dstrbtd/llama-1b`, and dataset `HuggingFaceFW/fineweb-edu`. The dashboard also links `distributed/llama-4b`. The collected public evidence does not establish which model, tokenizer, dataset, or source-code revision produced any particular historical W&B run. No model weights, dataset contents, configs, or blockchain data are collected.

To regenerate the summaries from the already captured raw snapshot without making network requests (this rewrites the derived tables):

```bash
python3 research/distributed-training/collect_public_runs.py \
  --output research/distributed-training/data/public_20261029 \
  --max-bytes 12884901888 --analyze-only
```

Only if a larger follow-up capture is separately budgeted, resume with a newly selected raw-response limit; the value must not be mistaken for a total disk cap:

```bash
python3 research/distributed-training/collect_public_runs.py \
  --output research/distributed-training/data/public_20261029 \
  --max-bytes <new_raw_byte_budget> --resume
```

The generated `REPORT.md` and `summary.json` describe public run-count coverage, history scan coverage, failed or withheld responses, and dashboard rows. See each JSONL record's `raw_ref` or the CSV tables' `raw_ref` values to trace observations back to immutable raw evidence.
