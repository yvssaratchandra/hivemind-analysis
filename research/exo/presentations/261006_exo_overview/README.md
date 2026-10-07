# Exo overview — 6 October 2026

Ready-to-present [PDF](../rendered/261006_exo_overview.pdf): seven main slides and two appendices, matching the previous Hivemind deck's layout. [Speaker notes](speaker-notes.md) contain fuller explanations, source links, and qualifications.

| Question | Slides |
| --- | --- |
| What problem does Exo solve? | 2 |
| How does it work; what is the architecture? | 3–4 |
| Where does the control plane come in? | 5 |
| What problems are worth investigating? | 6–7 |
| Exact experiment results and sources | 8–9 |

## Implementation status

| Step | Status |
| --- | --- |
| Architecture and research questions | Checked against pinned Exo source |
| Measured finding and controls | Checked against audit 005 raw records and snapshots |
| Slides, notes, editable artwork | Generated |
| PDF and visual review | Rendered and checked |

The deck separates the observed resource-identity claim from untested placement/inference consequences. It describes commit `21a54c5ea0230a3bec1e1a786d200126c7e34ec6`. The [local audit](../../results/exo-audit-20261006-005/REPORT.md) ran one private trial per arm, without models or inference workers.

## Edit and rebuild

Edit [render_exo_overview.py](render_exo_overview.py). It regenerates `slides/*.svg`, individual PDFs, speaker notes, and Beamer sources. The combined PDF and PDF previews go in `../rendered/`, outside this folder.

If `rsvg-convert` and Poppler tools are installed:

```sh
python3 research/exo/presentations/261006_exo_overview/render_exo_overview.py
```

Otherwise use the supplied rendering image, from the repository root:

```sh
docker build -f research/exo/presentations/261006_exo_overview/Containerfile \
  -t exo-presentation-renderer:20261006 \
  research/exo/presentations/261006_exo_overview
docker run --rm --network none --user "$(id -u):$(id -g)" \
  --cap-drop ALL --security-opt no-new-privileges \
  --mount "type=bind,src=$PWD,dst=$PWD" \
  --workdir "$PWD/research/exo/presentations/261006_exo_overview" \
  exo-presentation-renderer:20261006
```

The image build installs rendering tools inside the image. Rendering mounts only this repository and needs no network. `--svg-only` generates artwork and notes with host Python alone. The renderer reads the preserved audit data; it starts no Exo peers.

## Overleaf

The [source archive](../261006_exo_overview-overleaf.zip) contains this folder. Use `exo-overview.tex` as the main file, or `exo-overview-notes.tex` for slides beside presenter notes. Both embed the generated `slides/*.pdf`; diagrams retain their positions. The wrapper uses standard Beamer, TikZ, graphicx, and pgfpages. No TeX compiler is installed here, so the wrapper is supplied but uncompiled; the presentation PDF was rendered directly.

The artwork uses the two Chalmers emblem assets already bundled with the earlier presentation. Their [NOTICE](NOTICE) and [LICENSE](LICENSE) are included.
