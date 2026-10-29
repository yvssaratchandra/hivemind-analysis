# Templar public-run evidence

The selected **Covenant 72B** dashboard view exposes 600 distinct BPB-by-outer-step points, from 2.0091 at step 10 to 0.7128 at step 6410, plus eight benchmark rows at global step 6410. The static paper’s plotted curve is a separate publication snapshot with a terminal point around outer step 6190; we do not merge that figure with the dashboard’s dated API series or treat 6410 as the paper’s final step. Its paper also plots aggregate contributing peers and cumulative unique participants. The evaluated-miner endpoint returned a count only; the visible incentive table had no miner rows, so individual reward scores or identities are not available here, and the paper’s plot values were not digitized.

The newer **QWEN30_MOE_003** dashboard pipeline is a separate run, with observed API `run_id` `qwen2048-20260928-pp4dp4-r2-h31-b8192-lr4-a`. Its normalized loss/token/MFU/round series, participant summaries, per-UID bandwidth scalars, and pipeline-link rows remain labeled Qwen. The open selector snapshot mentions Covenant 72B as an option while Qwen is selected; that view is excluded from the 72B series. Crusades is a separate Subnet 3 activity described by the official repository and is not treated as 72B training evidence.

Evidence and reproducible artifacts:

- [Detailed capture report](data/extended_20261029/reports/public-data.md)
- [Source catalog and limitations](SOURCE.json)
- [Normalized series](data/extended_20261029/normalized/templar_series.csv)
- [Normalized benchmarks](data/extended_20261029/normalized/templar_benchmarks.csv)
- [Participants, pipeline links, and scalars](data/extended_20261029/normalized/templar_participants.csv), [links](data/extended_20261029/normalized/templar_links.csv), [scalars](data/extended_20261029/normalized/templar_scalars.csv)
- [Summary JSON](data/extended_20261029/summary.json)
- [Raw HTTP manifest](data/extended_20261029/http/manifest.json)
- [Selected 72B browser snapshot](data/extended_20261029/templar_covenant72b_selected/manifest.json)
- [Covenant-72B report PDF](https://arxiv.org/pdf/2603.08163) (archived response path and hash are in the raw HTTP manifest)

The paper is a static publication source; the 6410-step value above is from the dashboard API captured on 2026-10-29. They are separate observations, and no exact deployed source commit was established by matching the full-history repository to the run. Paper and dashboard observations are kept separate from the current pipeline view. No model weights or datasets were collected.
