# Hivemind own-ad renewal — 7 October 2026

Ready-to-present [four-slide PDF](../rendered/261007_hivemind_renewal.pdf), using the existing
Chalmers layout. [Speaker notes](speaker-notes.md) contain fuller explanations and an appendix
with all fourteen grouping policies, seven continuous policies, persistence results, and the
minimum-five check. The appendix is in the notes; the presentation remains exactly four slides.

| Slide | Content |
| --- | --- |
| 1 | Own-ad renewal and expiration-based leader priority |
| 2 | Eight-peer setup, controls, repeated workload, and output verification |
| 3 | Verified leadership counts versus active advertisement writes |
| 4 | Captured follower timeout, visibility versus assembly, and next tests |

## Implementation status

| Work | Status |
| --- | --- |
| Source and raw-trace checks | Complete: eight runtime source digests and 81 final trials checked |
| Four slides and notes appendix | Complete |
| PDF rendering and page count | Complete: exactly four pages |
| Visual and text-bound review | Complete |

These are private local standalone averager experiments, not public-WAN or full Optimizer
measurements. The continuous workload gives X the same fast re-entry in every arm, and the
frequency comparison also changes remaining lifetime. Five incorrect-tensor steps are excluded
from verified groups; their cause is unresolved. Disband figures count stream replies, not groups.

## Edit and rebuild

Edit [render_hivemind_renewal.py](render_hivemind_renewal.py). It rechecks the preserved raw traces
using the existing Step 22 analyzers, checks runtime source hashes, and creates editable SVGs,
individual PDFs, notes, Beamer sources, [recalculated evidence](evidence-check.json), a combined
PDF, previews, and an Overleaf archive. It starts no peers and makes no network requests.

From the repository root, with `rsvg-convert` and Poppler installed:

```sh
python3 -B research/hivemind/presentations/261007_hivemind_renewal/render_hivemind_renewal.py
```

`--svg-only` creates artwork, notes, and evidence without PDF tools. Combined outputs and previews
live under `../rendered/`; the deck folder contains the editable sources and individual slide PDFs.

## Overleaf

Upload [the source archive](../261007_hivemind_renewal-overleaf.zip). Choose
`hivemind-renewal.tex` as the main file, or `hivemind-renewal-notes.tex` to show presenter notes beside
the slides. Both embed the generated slide PDFs, preserving the layout. The wrappers use standard
Beamer, TikZ, graphicx, and pgfpages; the full table appendix remains in `speaker-notes.md`.
No TeX compiler is installed here, so the wrappers are supplied but uncompiled. The presentation
PDF was rendered directly from SVG and visually checked.

Evidence: [Step 22 report](../../results/step22_refresh_report_20261007.md), with linked raw suites,
configs, seeds, provenance, and harness snapshots. Template artwork and helpers were adapted from
the existing Hivemind and Exo presentations. The copied emblem retains its [NOTICE](NOTICE) and
[LICENSE](LICENSE).
