# Node0 public run collection

Source checkout: [PluralisResearch/node0](https://github.com/PluralisResearch/node0) at `32bd084de15dbdc8e30723ad66db7d01acf70c03` (Apache-2.0).

Latest collection: `research/node0/data/public_runs_20261029T100000Z`; run `node0-7-5b`; 9089861 raw bytes across 6 requests.
See [INVENTORY.json](INVENTORY.json) for per-endpoint coverage and errors. The bulk responses and normalized tables are in the ignored snapshot directory.

## Run findings

- Registry model: **OLMo2-7.5B**, 7.5B parameters; dataset `FineWeb-Edu`; published `tokens` field `1.3T` (not assumed to be processed tokens).
- Dashboard peer records: 1642 across 304 usernames; leaderboard records: 303 across 303 usernames.
- Peer `flop_pflop` was nonzero on 560 rows; sum 464631.2744162898; this field is preserved without inferred unit or scale.
- Exported training loss: 1500 points, from `11.31` at step `0` to `2.734` at step `8719`. This curve alone does not establish convergence or generalization.
- The dashboard UI describes leaderboard score as `(FLOPs + presence credit) / scaling`; do not read it as a raw FLOP total. Its source asset and hash are noted in the generated report.

## Reproduction

`python research/node0/collect_public_runs.py --output research/node0/data/public_runs_20261029T100000Z --analyze-only`


The Node0 README links `https://dashboard.pluralis.ai/`, which returned HTTP 403 to the public read-only collector. Accessible historical telemetry was collected from the shared Agora registry API. This reports dashboard observations, not independently verified compute contribution.
