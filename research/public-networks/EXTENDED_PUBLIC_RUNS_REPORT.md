# Six-project public-run archive

Collection batch: `extended_20261029`. New local evidence totals **305,659,445 bytes (about 306 MB)**, excluding source clones. **68 focused tests pass**; 1,797 saved manifest payloads verify. Read the individual coverage reports before using these artifacts as research evidence.

| Project | Source checkout | Public-run evidence and limits |
| --- | --- | --- |
| [Psyche](../psyche/PUBLIC_RUNS.md) | `research/psyche/upstream` — PsycheFoundation/nousnet | Official announcements and model metadata/cards saved. Dashboard attempts timed out or failed DNS resolution in this environment; the docs request timed out. No raw run or node-contribution history was retrieved. |
| [Templar](../templar/PUBLIC_RUNS.md) | `research/templar/upstream` — one-covenant/templar | Covenant 72B selected in the dashboard; model-training JSON contains 600 bits-per-byte points through displayed step 6410. Benchmark JSON and screenshots saved. Evaluated-miners endpoint reports a count, while the incentive table exposes no rows. Qwen pipeline data is a separate run. |
| [Grail](../grail/PUBLIC_RUNS.md) | `research/grail/upstream` — one-covenant/grail | Official documentation and public dashboard access attempts. Anonymous W&B queries returned a null project; a loading page is not numerical history. Grafana home and miner leaderboard are captured; the selected six-hour read query returned zero rows. Older Grafana windows were not exported. The official article and its figures are saved. |
| [IOTA](../iota/PUBLIC_RUNS.md) | `research/iota/upstream` — macrocosm-os/iota | Overview, miner, leaderboard and pipeline snapshots; all 180 chart/config resources advertised for ten runs saved. Normalized output contains 473,567 rows, including 1,415 leaderboard rows. The reviewed overview selects Orion 16B (`orion-16b-v9`), not a verified 100B-parameter run. |
| [INTELLECT-1](../intellect-1/PUBLIC_RUNS.md) | `research/intellect-1/upstream` — PrimeIntellect-ai/prime-diloco | Technical report, release page and model metadata. Historical dashboard currently returns 404. Published curves and provider counts are author-reported evidence, not raw per-provider histories. |
| [INTELLECT-2](../intellect-2/PUBLIC_RUNS.md) | `research/intellect-2/upstream` — PrimeIntellect-ai/prime-rl; supporting TOPLOC and protocol | Release/report figures and model metadata. Historical dashboard currently returns 404. No exact deployed training commit or complete per-node history is established. |

## Inspect locally

- [Screenshot gallery](EXTENDED_DASHBOARDS.html): links to saved PNG, PDF, MHTML and rendered HTML artifacts.
- [File inventory and integrity results](EXTENDED_VERIFICATION.json): SHA-256, byte counts and unavailable responses. Nested snapshot files are counted once.
- [Source checks](EXTENDED_SOURCE_CHECKS.json): full source clones and initialized submodules are checked with `git fsck --full`; exact revisions and results are recorded.
- [Normalization checks](EXTENDED_NORMALIZATION_CHECK.json): independent raw-to-CSV comparison of all 113,008 IOTA overview-loss points, 1,415 IOTA leaderboard rows, and 600 Templar bits-per-byte points.
- [Offline checks](EXTENDED_OFFLINE_CHECK.json): representative MHTML pages open with networking disabled. IOTA's canvas charts disappear in MHTML; PNG preserves their appearance and API bodies preserve the available values.
- [Test results](EXTENDED_TEST_RESULTS.json): focused tests and six-project offline analysis.
- [Implementation and repeat commands](EXTENDED_COLLECTION.md).

## Research interpretation

These are passive archives of operator-published evidence. They do not establish that operators' reported measurements are independently correct. Bits per byte, loss, rewards, activations, tokens and incentives have different meanings; do not compare them as a single contribution or convergence metric. Dashboard current values are dated observations, and absence of public data is not evidence that the underlying run did not occur.

Full repository history does not establish which commit a historical run deployed. Source provenance, public metrics and paper claims are recorded separately. No model weights, dataset payloads, live training or public-swarm interaction were collected.

The bulk archive is stored locally and excluded from Git. The pushed repository contains tools, reports and inventories; it is **not a backup of the captured bodies or cloned Git histories**. Copy the project `data/`, `upstream/` and applicable `supporting/` directories to independent storage if protection against local disk loss is required.
