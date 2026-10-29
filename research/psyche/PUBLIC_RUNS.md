# Nous Psyche source and public run evidence

Snapshot date: 2026-10-29. This report separates the project’s current source,
historical operator claims, and what the public deployment returned during the
bounded collection attempt.

## Identity and current source

The canonical source is [PsycheFoundation/nousnet](https://github.com/PsycheFoundation/nousnet),
cloned with full Git history at commit
[`0bdb13d94a05675da93d69ffa5d4c4e5a680736c`](https://github.com/PsycheFoundation/nousnet/tree/0bdb13d94a05675da93d69ffa5d4c4e5a680736c).
The checkout is ignored in Git; [SOURCE.json](SOURCE.json) records its pin and
clone policy. No model weights or datasets were fetched.

The current source describes Psyche as distributed transformer training. Its
website frontend selects a backend URL through `VITE_BACKEND_URL`; the checked-in
`dev-remote` command points to `https://psyche.network/api`. The backend source
implements public GET handlers for `/status`, `/runs`, `/run/:runId/:index`,
`/contributionInfo`, and `/contributionInfo/:address`. `/runs` returns run
summaries and aggregate token-rate fields; `/run/...` returns a run state. Both
can stream newline-delimited JSON when the client requests it. `/status` reports
the deployed backend commit and indexed chain slots. These are source-verified
routes, not proof that the service answered during this collection.

## Run claims and defaults

The checked-in [Consilience config](https://github.com/PsycheFoundation/nousnet/blob/0bdb13d94a05675da93d69ffa5d4c4e5a680736c/config/consilience/state.toml)
identifies `consilience-40b`, specifies 325,000 training steps and estimates
about 22T tokens. It is a source configuration; it does not establish that the
public run reached that target. The project’s current top-level README contains
no live-run state or general 40B default.

Nous’s May 2025 [Psyche announcement](https://nousresearch.com/nous-psyche)
describes the planned Consilience 40B training effort and data scale. The
September 2025 [project update](https://nousresearch.tech/the-next-phase-of-psyche)
reports a completed testnet Consilience run. The December 2025
[Hermes 4.3 report](https://nousresearch.tech/introducing-hermes-4-3) reports
post-training over 24 nodes and a peak throughput figure. These are attributed
operator reports; no independent raw training trace was found in the public
source tree.

The [Consilience 40B model card](https://huggingface.co/PsycheFoundation/consilience-40b-7Y9v38s5)
and [Hermes 4.3 model card](https://huggingface.co/NousResearch/Hermes-4.3-36B)
are separate artifacts. The Consilience card’s planned/ongoing dataset counts
and training-duration field must not be read as the final token count of a
particular distributed run. Only lightweight model metadata is in scope; no
weights or datasets are included.

## Public deployment observation

The standard [Psyche runs page](https://psyche.network/runs) timed out in
Chromium. The saved browser evidence is
`data/extended_20261029/dashboard_runs/`; it records a connection timeout and
an `about:blank` page at 2026-10-29 16:49 UTC. The one
[mainnet runs-page](https://mainnet.psyche.network/runs) fallback attempt ended
with `ERR_NAME_NOT_RESOLVED` at 16:54 UTC. These are collection observations,
not proof the service is permanently unavailable. Neither page yielded a
successful API response, so actual deployed API availability, run-history
depth, node-contribution exports, and convergence-series availability remain
unknown.

The browser helper used GET/HEAD-only requests and a bounded response budget;
the captures are preserved in the ignored `data/extended_20261029/` tree. The
official [Psyche docs site](https://docs.psyche.network/) also timed out during
the HTTP archive attempt. The archived Nous primary-source announcements and
reports, plus metadata and README model cards for
[Consilience 40B](https://huggingface.co/PsycheFoundation/consilience-40b-7Y9v38s5)
and [Hermes 4.3 36B](https://huggingface.co/NousResearch/Hermes-4.3-36B), are
saved under `data/extended_20261029/http/`. The collection saved only model
metadata and card text, not model weights or datasets.

The model-card text describes Consilience as an automatically updated model
whose “Training Duration” is still “TBD”; that card is not a final run report.
Hermes 4.3's card identifies it as the first Hermes model trained in a
decentralized manner using Psyche, while the December 2025 post-training scale
and throughput remain operator-reported claims. The archived API metadata has
2 model records; no public training-loss/convergence series or node-level run
contribution export was retrieved.

## What this evidence can support

- Current source and configuration are pinned to the SHA in `SOURCE.json`;
  moving `main` is not an acceptable substitute for this revision.
- Official blog/model-card claims are labeled by their source and date; they are
  not independently verified training telemetry.
- The live dashboard/API remains an availability gap in this collection. The
  source-code `/runs`, `/run/...`, and `/contributionInfo...` routes establish
  the intended data types, not deployed availability. A failed request is not
  an empty run history, and source code does not prove a route is deployed.
- The 40B model’s BF16 weights exceed the project’s 20 GB additional-data
  allocation; they were intentionally excluded.
