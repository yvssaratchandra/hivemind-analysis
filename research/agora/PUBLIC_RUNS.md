# Public Agora run evidence

Operator-reported dashboard exports, not an independent rerun.

| Run | Model | Peer records | Leaderboard rows | Loss points | First / last sampled loss |
| --- | --- | ---: | ---: | ---: | --- |
| pluralis-13b-test | Pluralis-13B | 1215 | 100 | 1500 | 12.259 / 3.006 |
| pluralis-8b | Pluralis-8B | 592 | 176 | 1500 | 12.197 / 2.438 |

## Coverage and interpretation

- `range=all` covers the exposed time window; it does not guarantee unsampled training steps. The series have gaps in step indices.
- Telemetry CSV contains non-null matrix cells; raw matrices retain every null. Missing samples are not zero.
- Worker names in telemetry and peer names are retained verbatim, without speculative identity matching.
- Leaderboard score and active_time retain native units; no conversion to verified compute contribution is made.
- The 1.3T dataset label is not the number of tokens processed by a run.
- GPU population group indices are preserved without assuming disjoint populations or summing them.
- Evaluation capability does not imply available evaluations; inspect eval_tasks and the raw response.
- Archived queue/status fields describe the source snapshot, not present training activity.
- Current source code is not evidence of the exact historical deployment version.
- See manifest.json for HTTP failures and raw provenance; summary.json and tables/ are reproducible offline.

## Local archive

Snapshot: `research/agora/data/public_runs_20261029T095600Z`. Raw responses and tables are ignored in Git.
Tracked provenance: [PUBLIC_RUNS_INVENTORY.json](PUBLIC_RUNS_INVENTORY.json).

Regenerate analysis without network:

```sh
python research/agora/collect_public_runs.py --output research/agora/data/public_runs_20261029T095600Z --analyze-only
```
