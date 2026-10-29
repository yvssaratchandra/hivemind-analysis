# Macrocosmos IOTA source and public run evidence

Snapshot date: 2026-10-29. Dashboard metrics are operator-reported evidence;
they are not independently reproduced training measurements.

## Identity and current source

The canonical source is [macrocosm-os/iota](https://github.com/macrocosm-os/iota),
cloned with full Git history at commit
[`ed0fecb190ab7277ebb1e76ddba920eb4dcf7543`](https://github.com/macrocosm-os/iota/tree/ed0fecb190ab7277ebb1e76ddba920eb4dcf7543).
The checkout is ignored in Git; [SOURCE.json](SOURCE.json) records the pin and
clone policy. The source has no Git submodules. No runtime environment, model
weights, or datasets were installed or fetched.

## README and 100B claims

At the pinned revision, the README labels its current run a 1.5B-parameter,
three-layer Llama-inspired configuration. It lists 15B, 50B, and 100B
**parameter models** under “Future Run Information.” That section is roadmap
language, not evidence that those models ran.

The README links the [IOTA primer PDF](https://www.macrocosmos.ai/research/iota_primer.pdf)
and its [arXiv paper](https://arxiv.org/abs/2507.17766). The paper describes an
experimental 1.5B model and up to 400M training tokens; its “100 billion”
validation horizon refers to **tokens**, not parameters. The README PDF URL returned HTTP 404 in the archived manifest; the arXiv
HTML and PDF copies were retrieved successfully.

Current source also contains a `LLAMA32_CONFIG_100B` with 99.6B parameters and
a `LLAMA32_CONFIG_SIMULATED_100B` whose model name/parameter count are about
26.1B. Config constants demonstrate code/configuration support only; neither
constant identifies a deployed run. The public run observed here is a 16.2B
parameter run, so the README’s 1.5B “current run” description is stale relative
to the dated dashboard snapshot.

## Dated dashboard snapshot

The README-linked `/dashboard/mainnet` path returned HTTP 404 and is preserved
as a gap in `data/extended_20261029/dashboard_mainnet/`. The current
[overview](https://iota.macrocosmos.ai/dashboard/overview) route succeeded.
The page selected “Orion 16B,” and the captured same-origin data manifest
identified it as `research/orion-16b-v9` / run state `28876`, with
`baked_at=2026-10-29T16:37:09Z`. The page reports 16.2B parameters, 45.52B
tokens, loss 2.7181, 32.4K tokens/s, 107 active nodes, 23.00% MFU, 10 stages,
and 10 replicas. The top-level 14B/166B numbers found in stale indexed material
were not substituted for this live page response.

The overview provides PNG, HTML, PDF, and MHTML captures plus the raw chart
responses. The PNG visibly preserves the loss, throughput, and MFU plots; the
offline MHTML retains page structure but does not preserve interactive canvas
charts. Use the PNG and original API data for those charts.

The chart manifest advertises ten run records and eighteen chart types. The
bounded collector fetched all 180 chart resources it advertised, across the
current run and nine historical run states. It preserved each raw response and
hash in `data/extended_20261029/http/manifest.json`; compact long-form values are
in `data/extended_20261029/normalized/public_run_series.csv.gz`, and the
per-file inventory records failed paths. The Miners, Leaderboard, and Pipeline
tabs were separately captured for run `28876`. The current leaderboard has
144 ranked rows with native fields such as activations, tokens, tokens/sec,
layer count, submissions, GPU, and reported hardware/network values. The
historical exports include per-miner activation, weight-upload, partition, and
leaderboard resources where advertised; all ten leaderboard tables normalize
to 1,415 rows total. The nine prior run states advertised in the manifest are
`4.13.7.22-tah` (43682), `.21-tah` (43681), `.20-tah` (43680), `.19-tah`
(43679), `.18-tah` (43678), `.17-tah` (43677), `.16-tah` (43676), `.15-tah`
(43675), and `.14-tah` (43674). These are node-reported records, not a
verified allocation of useful compute.

The overview loss export includes a raw-loss trace and two moving-average
segments. The raw response is NDJSON with separate `x_time` and `x_tokens`
axes; the normalized file preserves both axes, the header trace names, and the
two moving-average chunks. The moving average falls from 12.228 to 2.718 across
its reported token axis, ending near 45.52B tokens; the raw series ends at
2.880. This is evidence of decreasing reported training loss, not proof of
convergence or generalization. The chart samples’ latest timestamp is
2026-10-02, although the page and data manifest were updated/baked on
2026-10-29. Treat the metrics as a dated export, not evidence that the training
process was still active on October 29.

## Coverage and interpretation

- The README-linked `/dashboard/mainnet` path returned HTTP 404 in its browser
  capture. The overview, Miners, Leaderboard, and Pipeline captures are
  available under `data/extended_20261029/`; each has a hash manifest. The
  archived run manifest and all 180 advertised chart responses are preserved
  separately under `data/extended_20261029/http/`.
- All ten run records are represented in the normalized file (473,567 chart
  rows), including 113,008 loss points and the node-level tables advertised by
  the dashboard. It includes overview convergence series and per-node activity
  tables where available. The 3 GB project cap and 1 GB raw-response cap were
  not approached; the complete evidence tree is about 170 MB.
- Loss, tokens/sec, MFU, and per-miner leaderboard values preserve the site’s
  native fields. The dashboard does not establish independent measurement or
  what threshold would constitute convergence.
- No public model card for the specific Orion 16B checkpoint was observed.
  The metadata-only `HuggingFaceFW/fineweb-edu-2` API request returned HTTP 401;
  this records the endpoint response and does not establish whether the dataset
  is gated or unavailable. No checkpoint or dataset files were downloaded.
- The source repository’s current HEAD is pinned above. No historical source
  revision has been assigned to the dashboard run without deployment metadata.
