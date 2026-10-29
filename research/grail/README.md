# Grail public-source archive

This directory preserves the official [`one-covenant/grail`](https://github.com/one-covenant/grail) implementation with full Git history, the dated public W&B/Grafana/Substack captures, and compact normalized metadata. LFS objects were not fetched. The upstream `vendor/affinetes` submodule could not be initialized because its public GitHub fetch requested credentials; it is recorded as unavailable, not represented by a clone.

Run the bounded collector from the repository root:

```sh
python research/grail/collect_public_runs.py \
  --output research/grail/data/extended_20261029 \
  --max-bytes 3000000000 --resume
```

`--output` is the complete batch root, including browser captures, `http/`, normalized files, and the report. Its maximum is applied to the entire batch; raw HTTP evidence is capped at the smaller of half the limit and 1,000,000,000 bytes. Use `--resume` to verify and reuse saved HTTP evidence, or `--analyze-only` to regenerate local outputs without network access. The collector discovers snapshots only under the requested batch label.

The W&B requests are unauthenticated and metadata-only, bounded to ten run rows, and omit config/history fields. A project-null response is recorded as a visibility gap; no run history is inferred from the blog’s plots. The blog reports one live-chain run over 100 windows/roughly 320 trainer updates, but the captured prose does not establish a machine-readable finished state. Hugging Face responses are for the Qwen base model and a search for Covenant-authored Grail cards; they do not request weights.

Omit `--resume` only for a fresh batch. See [`PUBLIC_RUNS.md`](PUBLIC_RUNS.md), [`SOURCE.json`](SOURCE.json), the [dated capture report](data/extended_20261029/reports/public-data.md), and [`INVENTORY.json`](INVENTORY.json) for provenance, findings, gaps, and hashes.
