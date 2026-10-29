# INTELLECT-2 source and artifact inventory

| Item | Location | Purpose / status |
| --- | --- | --- |
| Source provenance | [SOURCE.json](SOURCE.json) | Full-clone refs, supporting repos, submodule omissions, source URLs, and collection scope. |
| Main source | `upstream/` (ignored) | Full `prime-rl` Git history; LFS smudge disabled; five pinned source submodules checked out at recorded commits. Current `main` is evolving. |
| Supporting sources | `supporting/toploc/`, `supporting/toploc-experiments/`, `supporting/protocol/` (ignored) | Officially linked inference validation and protocol code; protocol's pinned direct `smart-contracts` submodule is checked out. Its nested Foundry/OpenZeppelin libraries remain uninitialized; no dependencies were installed or run. |
| Collector | [collect_public_runs.py](collect_public_runs.py) | Bounded GET-only HTTP archive and read-only dashboard/release-page captures. |
| Focused tests | [test_collect_public_runs.py](test_collect_public_runs.py) | Manifest normalization, capture integrity/status, required-manifest, and raw/total budget checks. |
| Public evidence findings | [PUBLIC_RUNS.md](PUBLIC_RUNS.md) | Reward/loss convergence, node evidence, and known gaps. |
| Snapshot output | `data/extended_20261029/public_sources/` (ignored) | HTTP raw bodies + hash/status manifest, browser HTML/PNG/PDF/MHTML captures + manifests, normalized source table, summary, and report. |
| W&B run histories | Not found in linked primary sources | A debug-config W&B project change is not evidence of a public run; no guessed endpoint lookups. |
| Model weights / dataset contents | Not collected | Only the model card, model metadata, and RL dataset metadata are requested. |

All Git clones are full-history. The five pinned `prime-rl` source submodules and protocol's direct pinned `smart-contracts` submodule are checked out at their recorded commits; protocol's two nested library submodules remain uninitialized. The ignored data snapshots are local. Use `--resume` with the same output root; `--analyze-only` verifies and rebuilds summaries without network access.
