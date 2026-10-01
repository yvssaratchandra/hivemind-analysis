# Hivemind Attacks — 2 October 2026

This folder contains an eight-slide talk on two separately demonstrated attacks:
targeted matchmaking eclipse and malicious-leader roster equivocation. A ninth,
backup slide covers the conditional WAN-like deadline-undercutting result.

- Main document: `hivemind-attacks.tex`
- Slides with speaker notes: `hivemind-attacks-notes.tex`
- Compiler: pdfLaTeX, run from this directory so the Chalmers theme can find
  its `logographics/` assets.

Build locally with:

```sh
latexmk -pdf -interaction=nonstopmode -halt-on-error hivemind-attacks.tex
latexmk -pdf -interaction=nonstopmode -halt-on-error hivemind-attacks-notes.tex
```

The resulting PDFs are `hivemind-attacks.pdf` and
`hivemind-attacks-notes.pdf`. For Overleaf, upload this folder and select
`hivemind-attacks.tex` as the main document. The previous September talk remains
in `hivemind-control-plane.tex`; it is not the main document for this meeting.

Evidence and qualifications are in the new source's speaker notes. Primary
research sources are `research/HANDOFF.md`, `research/CONTROL_DATA_AUDIT.md`,
`research/ATTACK_PRESENTATION.md`, and `research/control_data_map.pdf`.
`LICENSE` and `NOTICE` cover the bundled theme and logos.
