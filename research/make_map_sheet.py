"""
Build a printable one-page version of Part 1 of `CONTROL_DATA_AUDIT.md` — the 21-checkpoint
control-data map, crossed with observe / delay / deny / tamper / select.

Parses the audit itself so the sheet cannot drift from the source of truth. Renders through
headless chromium (no LaTeX on this machine). Output: `research/control_data_map.pdf`.

The point of the sheet is to be readable across a table: filled cells are evidence, empty
cells are unexamined. Empty means nobody has looked, not that it is safe.

Run:
    python3 research/make_map_sheet.py
"""

from __future__ import annotations

import html as _html
import re
import subprocess
from pathlib import Path

RESEARCH = Path(__file__).resolve().parent
AUDIT = RESEARCH / "CONTROL_DATA_AUDIT.md"

# One short gloss per layer. The audit carries the full prose; this is the meeting version.
GLOSS = {
    "L1": "Bootstrap peer selection lives here and is unexamined.",
    "L2": "One fix, <i>node_id</i> derived from the authenticated key, changes every row.",
    "L3": "One fix, signed advertisements, changes rows 11 and 15.",
    "L4": "Control data: roles (16), recovery (18), weight and group id (17).",
    "L5": "Progress records <i>are</i> signed; L3&rsquo;s advertisements are not.",
    "L6": "Donor priority self-declared, tensors installed unvalidated. Replaces a model, not nudges it.",
    "SP": "",
}


def parse():
    text = AUDIT.read_text()
    part = text[text.index("## Part 1"): text.index("### Coverage")]
    layers, current = [], None
    for line in part.splitlines():
        if line.startswith("### "):
            head = line[4:].strip()
            key = "SP" if head.startswith("Spanning") else head.split(" ")[0]
            # The audit's heading repeats its own description after the middle dot;
            # the gloss below already carries that, so keep only the layer name.
            name = re.sub(r"\*", "", head).split(" · ")[0].strip().replace(" — ", ": ")
            current = {"key": key, "name": name, "rows": []}
            layers.append(current)
        elif re.match(r"^\|\s*\d+\s*\|", line) and current is not None:
            cells = [c.strip() for c in line.strip().strip("|").split("|")]
            current["rows"].append(cells)
    return [l for l in layers if l["rows"]]


def cell(raw):
    """One grid cell, coloured by security verdict rather than by evidence label.

    red = a vulnerability was demonstrated; amber = the defence holds only within a bound;
    green = prevented, with a reason; grey = nobody has looked, which is **not** the same as
    safe. Row 14 inherits row 13's verdict because they share one channel.
    """
    v = re.sub(r"\*\*(.+?)\*\*", r"\1", raw).strip()
    ref = _html.escape(v.split("—")[-1].strip())
    if v == "open":
        return '<td class="open">untested</td>'
    if v == "—":
        return '<td class="na"></td>'
    if v.startswith("= "):
        return f'<td class="inherit">as&nbsp;{_html.escape(v[2:])}</td>'
    if v.startswith("partly X"):
        return f'<td class="partly">{ref}</td>'
    if v.startswith("X"):
        return f'<td class="prev">{ref}</td>'
    if v.startswith("D"):
        return f'<td class="dem">{ref}</td>'
    return f"<td>{_html.escape(v)}</td>"


def build():
    layers = parse()
    body = ""
    for layer in layers:
        body += f'<tr class="layer"><td colspan="7"><b>{_html.escape(layer["name"])}</b> &nbsp; <span class="gl">{GLOSS.get(layer["key"], "")}</span></td></tr>'
        for r in layer["rows"]:
            num, name = r[0], re.sub(r"\*(.+?)\*", r"<i>\1</i>", r[1])
            cells = "".join(cell(c) for c in r[2:7])
            body += f'<tr><td class="num">{num}</td><td class="chk">{name}</td>{cells}</tr>'

    return f"""<!doctype html><meta charset="utf-8"><title>Hivemind control-data map</title>
<style>
@page {{ size: A4 portrait; margin: 10mm 10mm 8mm; }}
body {{ font: 8.1pt/1.2 "Helvetica Neue", Helvetica, Arial, sans-serif; color:#16181c; margin:0;
        -webkit-font-smoothing:antialiased; }}
header {{ border-bottom:1.6pt solid #16181c; padding-bottom:1.6mm; margin-bottom:2.6mm; }}
h1 {{ font-size:14pt; font-weight:600; letter-spacing:-0.2pt; margin:0; }}
table {{ border-collapse:collapse; width:100%; table-layout:fixed; }}
th {{ font-size:7pt; font-weight:600; letter-spacing:0.5pt; text-transform:uppercase;
      color:#5a6069; text-align:center; padding:0 0 1.1mm; border-bottom:0.5pt solid #16181c; }}
th.l {{ text-align:left; }}
td {{ padding:1.0mm 1.2mm; border-bottom:0.3pt solid #dfe2e6; font-size:7.4pt; }}
td.num {{ width:6mm; text-align:right; color:#9aa0a8; font-variant-numeric:tabular-nums; }}
td.chk {{ width:58mm; color:#16181c; }}
tr.layer td {{ border-bottom:0.5pt solid #16181c; border-top:0.5pt solid #16181c;
               padding:1.2mm 1.2mm 1.0mm; background:#f2f3f5; }}
.lname {{ font-weight:600; font-size:8pt; }}
.gl {{ color:#5a6069; font-weight:400; font-size:7.4pt; }}
.dem   {{ background:#edc9c4; color:#6d2b22; text-align:center; font-weight:600; font-size:7pt; }}
.partly{{ background:#f2e2c2; color:#6b5120; text-align:center; font-weight:600; font-size:7pt; }}
.prev  {{ background:#cfdcc9; color:#2f4a2a; text-align:center; font-weight:600; font-size:7pt; }}
.inherit {{ background:#f6e0dc; color:#8a4c42; text-align:center; font-size:6.5pt; font-style:italic; }}
.open  {{ background:#ececed; color:#8d9298; text-align:center; font-size:6.4pt; }}
.na    {{ background:#fafbfc; }}
footer {{ margin-top:3mm; }}
.key {{ font-size:6.7pt; color:#3d434b; display:flex; gap:4.5mm; flex-wrap:nowrap;
        justify-content:space-between; }}
.key i {{ display:inline-block; width:3.4mm; height:2.3mm; vertical-align:-0.3mm; margin-right:1mm;
          border:0.3pt solid #c9ced4; }}
.key div {{ white-space:nowrap; }}
</style>
<header><h1>Hivemind control-data map</h1></header>
<table>
<tr><th class="l">#</th><th class="l">Checkpoint</th><th>Observe</th><th>Delay</th><th>Deny</th>
<th>Tamper</th><th>Select</th></tr>
{body}
</table>
<footer>
  <div class="key">
    <div><i style="background:#edc9c4"></i><b>vulnerability demonstrated</b> in a live swarm</div>
    <div><i style="background:#f2e2c2"></i><b>defence holds only within a bound</b></div>
    <div><i style="background:#cfdcc9"></i><b>no vulnerability</b>, prevented with a reason</div>
    <div><i style="background:#ececed"></i><b>untested</b>, nobody has looked &nbsp;&nbsp; blank = n/a</div>
  </div>
</footer>
"""


def main():
    out_html = RESEARCH / "control_data_map.html"
    out_pdf = RESEARCH / "control_data_map.pdf"
    out_html.write_text(build())
    for flags in (["--headless=new"], ["--headless"]):
        cmd = ["chromium", *flags, "--disable-gpu", "--no-pdf-header-footer",
               f"--print-to-pdf={out_pdf}", out_html.as_uri()]
        if subprocess.run(cmd, capture_output=True).returncode == 0 and out_pdf.exists():
            print(f"wrote {out_pdf} ({out_pdf.stat().st_size // 1024} KB)")
            return
    raise SystemExit("chromium failed to render the PDF")


if __name__ == "__main__":
    main()
