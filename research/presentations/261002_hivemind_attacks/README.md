# Hivemind control-plane attacks — 2 October 2026

The revised, ready-to-present file is [`hivemind-attacks-revised.pdf`](hivemind-attacks-revised.pdf); `hivemind-attacks.pdf` contains the same updated deck. It contains a title slide, one slide on last week's open questions, three slides each on forged withdrawal, deadline inflation, and the post-leader reducer-index attack, plus three appendix tables. The public live crawl has **not** been run; the deck does not present a live-network result.

[`speaker-notes.md`](speaker-notes.md) has the full explanation and evidence qualifications for each slide. Slide 5 links to appendix A1 (local withdrawal discovery), which links onward to A2 (forged-record timing trials). Slide 8 links to A2; slide 11 links to A3 (reducer-index trials).

The editable source is [`render_hivemind_attacks.py`](render_hivemind_attacks.py), which creates individual SVG and PDF slides in `slides/`, the combined PDF, speaker notes, and the Beamer wrapper `hivemind-attacks.tex`. Rebuild with:

```sh
python research/presentations/261002_hivemind_attacks/render_hivemind_attacks.py
```

That command requires `rsvg-convert` and `pdfunite`. If pdfLaTeX is available, compile `hivemind-attacks.tex` for a Beamer version; compile `hivemind-attacks-notes.tex` for slides alongside presenter notes. The Beamer wrapper was generated but could not be compiled in this environment because no TeX compiler is installed. The supplied combined PDF was rendered and visually checked here.

Primary evidence: `research/step8_withdrawal.py`; `research/step20_wan_forged_priority.py` and its `research/results/step20_forged_priority_sweep_sweep_20261002_a_*.csv` results; `research/step17_roster_equivocation_live.py` and its `research/results/step17_roster_live_*_confirm_20260928_confirm_t5_s1717.csv` results. See also `research/CONTROL_DATA_AUDIT.md` C8.T and C14.T. The Chalmers emblem is from the bundled `logographics/` assets; see `LICENSE` and `NOTICE`.
