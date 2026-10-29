# Public training-run archives

This workspace collects **passive public evidence**, not new training experiments.
The project workspaces are `research/node0`, `research/gensyn`,
`research/distributed-training`, and `research/agora`. Each has an independent
ignored source checkout or preserves its existing checkout, a collector, and
tracked source/collection inventories. Raw exports and normalized tables stay in
ignored, uniquely labelled `data/` snapshots on this machine.

Start with the [research findings and coverage report](PUBLIC_RUNS_REPORT.md).

## Collect and inspect

Run from the repository root. Collection uses Python's standard library; it does
not install or execute the projects' training dependencies.

```sh
# Inspect the known public source catalog without downloading anything.
python research/public-networks/collect_public_runs.py --discover-only

# Make a NEW dated snapshot across projects (20 GB total budget).
python research/public-networks/collect_public_runs.py --label public_runs_MY_DATE

# Resume the same archive without overwriting successful raw responses.
python research/public-networks/collect_public_runs.py --label public_runs_MY_DATE --resume

# Rebuild tables from an existing collection without network requests.
python research/public-networks/collect_public_runs.py --label public_runs_MY_DATE --analyze-only
```

Use `--project node0`, `gensyn`, `distributed-training`, or `agora` for one
project. `--max-bytes` sets the combined budget (maximum 20,000,000,000 bytes).
The wrapper runs projects sequentially, assigns 80% to raw exports, redistributes
unused shares to remaining projects, and reserves 20% for derived files. Raw
downloads are bounded before saving. Derived storage is checked after each
project finishes normalization; this is not a filesystem quota, so an unusually
large derived export can exceed the target before the wrapper stops. The final
verification records the actual total for this collection. Current project collectors use public
read-only HTTP exports; GraphQL requests, where used, are queries rather than
mutations. Source clones are excluded from this data budget. Authentication
failures and unavailable histories remain visible in manifests and reports.

For the first collection, agents used distinct project labels. Consult each
project's inventory or the collection report for exact local paths; the example
label above is a placeholder, not an existing snapshot.

Full W&B history collection can take substantially longer than fetching run
listings: sampled exports require additional requests across each run's step
ranges. Successful responses are reused on resume. Check the snapshot manifest
for progress and the generated coverage report for gaps; a running collector or
a complete run listing does not imply complete history coverage.

## Integrity and plots

```sh
python research/public-networks/verify_public_runs.py \
  --snapshot research/agora/data/public_runs_20261029T095600Z \
  --output /tmp/agora-verification.json
```

Repeat `--snapshot` to verify several projects. A passing integrity check means
saved files match the manifest; it does **not** mean every historical record was
publicly available or that the operator's measurements were independently verified.

Some Distributed Training responses are stored as the exact gzip-compressed HTTP
body, even when the filename ends in `.json`. Their manifest records
`content_encoding`; hashes refer to the stored bytes. The collector decodes these
for analysis and limits both stored and decompressed response size. Its sparse
W&B exports are `.jsonl.gz`: one observation per line, with original metric names
inside a `metrics` object and source/run identifiers alongside it.

Static plots use a separate optional environment. The pinned plotting packages
were validated with Python 3.14.8:

```sh
uv venv --python 3.14 research/public-networks/.plot-env
uv pip install --python research/public-networks/.plot-env/bin/python \
  -r research/public-networks/plot-requirements.txt
research/public-networks/.plot-env/bin/python research/public-networks/plot_public_runs.py \
  --snapshot research/agora/data/public_runs_20261029T095600Z \
  --snapshot research/node0/data/public_runs_20261029T100000Z \
  --snapshot research/distributed-training/data/public_20261029 \
  --output research/public-networks/plots
```

## Reading the evidence

- Every metric must retain its project/run identity and raw-response reference.
- Dashboard scores, participation rounds, GPU specifications, and useful compute
  contribution are different quantities. Unverified units stay in native form.
- Sampled loss curves describe observed training progress; they do not by
  themselves establish convergence or generalization.
- Strategy benchmark loss is separate from a model's sequential training loss.
- Missing or denied exports are not zero observations. Null telemetry cells are
  not zero activity. Current source defaults are not historical run configurations.
- The acquisition excludes model weights, datasets, credentials, account-gated
  sources, blockchain reconstruction, and public-swarm participation.

Small reports, plots, collectors and inventories are versioned. Bulky artifacts
are local: clone the listed source repository and rerun collection to obtain
them elsewhere. Sources can change or disappear; hashes identify this collection
but cannot guarantee a future re-download will be byte-identical.

## Dashboard presentation snapshots

Open [DASHBOARDS.html](DASHBOARDS.html) locally for dated screenshots and links to
saved HTML, PDF, MHTML and browser logs. [DASHBOARD_SNAPSHOTS.json](DASHBOARD_SNAPSHOTS.json)
records each selected run/tab, artifact hashes and missing responses. These are
individual page captures, not one simultaneous snapshot of every service.

The browser helper is `capture_dashboard.py`; its optional Playwright environment
is specified in `capture-requirements.txt`. It uses the installed Chromium,
explicit host allowlists, GET/HEAD requests and fresh output directories. Ordered
`--click-selector` arguments select a run and tab. `--scroll-selector` captures
bounded viewport tiles for an internal scrolling pane. Public dashboard
WebSockets require a separate `--allow-websocket-host`; no training peers are
started. A capture's `complete_with_errors` status can reflect blocked analytics,
missing response bodies or unavailable panels: inspect its logs and manifest.

```bash
research/public-networks/.capture-env/bin/python \
  research/public-networks/capture_dashboard.py \
  --url https://agora.pluralis.ai/ \
  --output research/agora/data/NEW_UNIQUE_LABEL/overview \
  --click-selector '[title^="pluralis-8b ·"]' \
  --scroll-selector 'div.overflow-y-auto.p-3'
```

HTML and MHTML alone do not guarantee visual or interactive offline replay.
In the offline test, Agora's MHTML retained layout and labels but lost its canvas
charts. Use the saved PNGs (including scroll tiles) for their appearance, and the
raw API exports for numerical analysis. Scroll tiles are capped at 12 per view;
long tables may extend beyond those images. The earlier raw peer/leaderboard
exports preserve the returned records independently of the visible table.
Original HTML may reference live URLs; the gallery uses only local images.

The browser helper bounds **saved payload/artifact bytes**, not network transfer
or Chromium's total memory. Response bodies are decoded by Chromium; their hashes
identify these decoded bytes. Large bodies can be evicted from its inspector
cache and remain explicit gaps. Original morning HTTP archives are retained
separately; they are not silently substituted into later browser captures.

Snapshots and upstream checkouts are ignored by Git. Pushing this repository
preserves tools, inventories and reports, **not the bulk data**. An independent
backup must include the ignored `data/` and `upstream/` directories as well.
