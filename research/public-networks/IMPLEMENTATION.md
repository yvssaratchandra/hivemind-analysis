# Public training-run archive

Approved approach: independent source checkouts and passive public exports, followed
by provenance-preserving normalization and static research reports. No training,
swarm participation, weights, or blockchain reconstruction. Data budget: 20 GB.

| Component | Owner | Implementation status |
| --- | --- | --- |
| Node0 checkout, exports, tests | node0_collect agent | Collected; parent reviewed code, raw counts and source citation; 12 tests pass |
| Gensyn checkout, accessible evidence, tests | gensyn_collect agent | Collected; parent reviewed all three dashboard hosts, counts and source limitations; 5 tests pass |
| Distributed Training checkout and histories | sn38_collect agent | Stopped bounded snapshot; parent reviewed coverage fix and verified all 666 saved raw bodies; 23 tests pass |
| Agora exports, orchestration, verification, report | Parent agent | Collected; integrity and deterministic offline regeneration pass; integrated into combined report and gallery |

Each collector accepts an output directory and byte budget, stores raw responses
and a hash manifest, and derives tables/report without altering original evidence.
Project ownership is exclusive. The parent reviews source, tests, raw samples,
manifest coverage and byte totals before accepting agent output. Implementation
order: clone/inventory, fetch, normalize, focused tests, offline verification,
report/plots, logical commits and push. Existing unrelated changes are preserved.

## Dashboard snapshot completion

The user clarified that dated public dashboard snapshots are the priority.
Further exhaustive W&B history downloading is paused; its existing evidence is
being normalized offline. Preserve all earlier captures and their timestamps.

| Component | Owner | Implementation status |
| --- | --- | --- |
| Bounded browser capture helper and focused tests | gensyn_collect agent | Complete; parent reviewed fixes and live captures; 10 tests pass |
| Exact named-run dashboard routes | node0_collect agent | Complete; all 24 selected run/tab states verified in saved DOM |
| Existing SN38 evidence normalization and inventory | sn38_collect agent | Complete; 14.1 GB snapshot, explicitly partial history |
| Browser setup, captures, visual/offline review, final integration | Parent agent | Complete with documented gaps; final inventory and gallery prepared |

Data flow: explicit public URL/allowed hosts → Chromium rendering → timestamped
HTML, DOM, screenshot, PDF/MHTML and bounded GET-response archive → hash manifest
and tracked inventory. Capture each named Agora/Node0 run and the Gensyn/SN38
dashboards separately; blocked pages remain documented failures. No login,
training, swarm participation or mutation requests are allowed.

The capture helper owns browser/network limits and provenance; the parent owns
target URLs, dated output directories, cross-project storage accounting and visual
review. First test byte limits and immutable output behavior, then capture one
page at a time. Acceptance requires readable screenshots, recorded timestamps
and URLs, verified artifact hashes, and an offline MHTML rendering check. A failed
offline rendering check must remain explicit; static screenshots/PDFs still
preserve appearance. Keep raw plus derived collection data below 20 GB, record
remaining coverage gaps, then commit and push the reviewed tracked artifacts.

Final integration: 30 dashboard views indexed; five representative offline MHTML
checks reviewed with canvas limitations recorded; 65 focused tests passed; all
manifest-listed files verified. Collection storage is 14,576,825,259 bytes, below
20 GB. A local gallery and derived-file checksums are included.
