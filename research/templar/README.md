# Templar public-source archive

This directory keeps a full-history clone of the official `one-covenant/templar` repository, dated public-dashboard snapshots, and compact normalized records. The dashboard’s `Covenant 72B` model-training view and its newer `QWEN30_MOE_003` pipeline view are distinct sources; the Qwen pipeline API responses retain their own observed `run_id` and are not attributed to the 72B run.

Run the bounded metadata collector from the repository root:

```sh
python research/templar/collect_public_data.py \
  --output research/templar/data/extended_20261029 \
  --max-bytes 3000000000 --resume
```

`collect_public_runs.py` is the shared-runner entrypoint; both scripts expose the same `--output`, `--max-bytes`, `--resume`, and `--analyze-only` interface. `--output` is the whole batch root, including dated browser snapshots, `http/`, normalized tables, and reports. The cap applies to all files below that root. Raw HTTP evidence is capped at the smaller of half the limit and 1,000,000,000 bytes. Use `--resume` to verify and reuse existing HTTP evidence, or `--analyze-only` to rebuild normalized tables and reports without network access. Dashboard snapshots must already exist inside the selected batch root before collection; the collector discovers them there and never falls back to a different label.

Omit `--resume` only when using a fresh output label. See [`PUBLIC_RUNS.md`](PUBLIC_RUNS.md), [`SOURCE.json`](SOURCE.json), the [dated capture report](data/extended_20261029/reports/public-data.md), and [`INVENTORY.json`](INVENTORY.json) for source provenance, findings, gaps, and file hashes.
