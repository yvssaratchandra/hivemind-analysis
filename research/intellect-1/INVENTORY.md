# INTELLECT-1 source and artifact inventory

| Item | Location | Purpose / status |
| --- | --- | --- |
| Source provenance | [SOURCE.json](SOURCE.json) | Repository URLs, full-clone commit, historical refs, submodule omission, artifact URLs, and scope. |
| Historical training source | `upstream/` (ignored) | Full `prime-diloco` Git history; LFS smudge disabled; submodule not initialized. |
| Collector | [collect_public_runs.py](collect_public_runs.py) | Bounded GET-only HTTP archive and read-only dashboard/release-page captures. |
| Focused tests | [test_collect_public_runs.py](test_collect_public_runs.py) | Manifest normalization, capture integrity/status, required-manifest, and raw/total budget checks. |
| Public evidence findings | [PUBLIC_RUNS.md](PUBLIC_RUNS.md) | Reported model, node, and convergence evidence with coverage limits. |
| Snapshot output | `data/extended_20261029/public_sources/` (ignored) | HTTP raw bodies + hash/status manifest, browser HTML/PNG/PDF/MHTML captures + manifests, normalized source table, summary, and report. |
| Weights / dataset contents | Not collected | Excluded by scope. Only model card, model metadata, and dataset collection metadata are requested. |
| Run-history exports | Not found in linked primary sources | No raw W&B run URL was linked; no speculative lookup performed. |

The local snapshot is intentionally ignored by Git. Use `--resume` with the same output root; `--analyze-only` verifies and rebuilds summaries from that output without network access.
