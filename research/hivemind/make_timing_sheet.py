"""
Build a printable one-page timing reference for Hivemind's DHT control plane.

Reads the CSVs under `research/hivemind/results/` so the numbers regenerate rather than rot, and
renders through headless chromium (no LaTeX on this machine). Output:
`research/hivemind/hivemind_dht_timings.pdf`.

Everything measured here is loopback on one machine, so the latencies are **floors**: a
real deployment pays network round trips on top. The configured values are read off the
source and cited, so they can be checked without trusting this script.

Run:
    .venv/bin/python research/hivemind/make_timing_sheet.py
"""

from __future__ import annotations

import csv
import glob
import statistics
import subprocess
from pathlib import Path

RESEARCH = Path(__file__).resolve().parent
RESULTS = RESEARCH / "results"

# Read off the source. Line references are what a reader should check, not decoration.
CONFIGURED = [
    ("wait_timeout", "3.0 s", "dht/node.py:108", "One DHT RPC: rpc_ping, rpc_store, rpc_find. Past this the peer is deemed lost."),
    ("blacklist_time", "5.0 s", "dht/node.py:119", "How long a non-responsive peer is excluded, multiplied by backoff_rate each time."),
    ("request_timeout", "3.0 s", "averaging/averager.py:122", "Waiting for a leader's reply after asking to join. Must be &lt; min_matchmaking_time."),
    ("min_matchmaking_time", "5.0 s", "averaging/averager.py:121", "Advertisement lifetime: expiration = get_dht_time() + this. Also the leadership priority."),
    ("num_replicas", "5", "dht/node.py:138", "Peers asked to store a record. The cost of custody."),
    ("bucket_size", "20", "dht/node.py:104", "Peers seeding a lookup traversal. The cost of controlling what a victim sees."),
]

MESSAGE_MIX = [
    ("rpc_ping + rpc_find", "~87 % of all traffic", "Routing upkeep and lookups."),
    ("join requests + rosters", "164 of 86,822", "The messages that actually decide group membership."),
    ("total observed", "86,822 messages, 15 kinds", "4 honest peers, 75 s, every RPC handler wrapped."),
]


def load(pattern):
    out = []
    for f in glob.glob(str(RESULTS / pattern)):
        with open(f) as fh:
            out += list(csv.DictReader(fh))
    return out


def med(xs):
    return statistics.median(xs) if xs else None


def baseline_rows():
    rows = load("step10_baseline_rw_n*.csv")
    by_n = {}
    for r in rows:
        by_n.setdefault(int(r["num_honest"]), {"w": [], "r": []})
        by_n[int(r["num_honest"])]["w"].append(float(r["write_s"]) * 1000)
        by_n[int(r["num_honest"])]["r"].append(float(r["read_s"]) * 1000)
    out = []
    for n in sorted(by_n):
        w, r = by_n[n]["w"], by_n[n]["r"]
        out.append((n, f"{med(w):.1f}", f"{min(w):.1f}–{max(w):.1f}", f"{med(r):.1f}", f"{min(r):.1f}–{max(r):.1f}", len(w)))
    return out


def attack_rows():
    rows = load("step10_delay_n8_k1_reads5.csv")
    by_d = {}
    for r in rows:
        d = float(r["delay_s"])
        by_d.setdefault(d, []).append(float(r["read_latency_s"]) * 1000)
    out = []
    for d in sorted(by_d):
        lat = by_d[d]
        note = {
            0.0: "no attacker present",
            0.5: "attacker answers late, never blacklisted",
            1.5: "attacker answers late, never blacklisted",
            2.9: "just under wait_timeout, maximum damage, still undetected",
            3.5: "past wait_timeout, RPC times out, attacker IS blacklisted, attack fails",
        }.get(d, "")
        out.append((f"{d:g}", f"{med(lat):.1f}", note))
    return out


def visibility_rows():
    rows = load("step10_stale_n8_w20*.csv")
    agg = {}
    for r in rows:
        k = (float(r["ad_lifetime_s"]), float(r["delay_s"]))
        a = agg.setdefault(k, [0, 0])
        a[0] += int(r["flasher_observed"])
        a[1] += 1
    lifes = sorted({l for l, _ in agg})
    delays = sorted({d for _, d in agg})
    return lifes, delays, agg


def html():
    base = baseline_rows()
    atk = attack_rows()
    lifes, delays, vis = visibility_rows()

    def tr(cells, th=False):
        tag = "th" if th else "td"
        return "<tr>" + "".join(f"<{tag}>{c}</{tag}>" for c in cells) + "</tr>"

    cfg = "".join(tr([n, f"<b>{v}</b>", f"<code>{src}</code>", d]) for n, v, src, d in CONFIGURED)
    bas = "".join(tr([f"{n} peers", f"<b>{wm}</b>", wr, f"<b>{rm}</b>", rr, s]) for n, wm, wr, rm, rr, s in base) \
        or tr(["<i>pending</i>", "", "", "", "", ""])
    atk_html = "".join(tr([f"{d} s", f"<b>{m}</b>", note]) for d, m, note in atk)
    mix = "".join(tr([a, f"<b>{b}</b>", c]) for a, b, c in MESSAGE_MIX)

    vh = tr(["advertisement lifetime"] + [("no delay" if d == 0 else f"attacker delays {d:g} s") for d in delays], th=True)
    vb = ""
    for l in lifes:
        cells = [f"<b>{l:g} s</b>" + (" (Hivemind default)" if l == 5 else "")]
        for d in delays:
            seen, n = vis.get((l, d), (0, 0))
            cls = "bad" if n and seen == 0 else ("ok" if n and seen == n else "mid")
            cells.append(f"<span class='{cls}'>{seen}/{n}</span>" if n else ",")
        vb += tr(cells)

    return f"""<!doctype html><meta charset="utf-8"><title>Hivemind DHT control-plane timings</title>
<style>
@page {{ size: A4 portrait; margin: 10mm 10mm 8mm; }}
body {{ font: 7.8pt/1.18 "Helvetica Neue", Helvetica, Arial, sans-serif; color:#16181c; margin:0;
        -webkit-font-smoothing:antialiased; }}
header {{ border-bottom:1.6pt solid #16181c; padding-bottom:1.8mm; margin-bottom:2.4mm; }}
h1 {{ font-size:14pt; font-weight:600; letter-spacing:-0.2pt; margin:0; }}
.sub {{ font-size:7.4pt; color:#5a6069; margin:0.9mm 0 0; }}
h2 {{ font-size:7.7pt; font-weight:600; letter-spacing:0.4pt; text-transform:uppercase; color:#16181c;
      margin:2.6mm 0 1mm; padding-bottom:0.8mm; border-bottom:0.5pt solid #16181c; }}
table {{ border-collapse:collapse; width:100%; }}
th {{ font-size:6.9pt; font-weight:600; letter-spacing:0.4pt; text-transform:uppercase; color:#5a6069;
      text-align:left; padding:0 1.2mm 1mm; border-bottom:0.5pt solid #16181c; }}
td {{ padding:0.85mm 1.2mm; border-bottom:0.3pt solid #dfe2e6; font-size:7.2pt; vertical-align:top; }}
td:first-child {{ white-space:nowrap; }}
code {{ font-family:"SF Mono", Menlo, Consolas, monospace; font-size:7pt; }}
.note {{ font-size:7pt; color:#5a6069; margin:1mm 0 0; }}
.bad {{ background:#23303f; color:#fff; padding:0 1.4mm; font-weight:600; }}
.ok  {{ background:#eceef0; padding:0 1.4mm; }}
.mid {{ background:#a8b1bb; padding:0 1.4mm; }}
.caveat {{ border-left:1.6pt solid #23303f; padding:1.2mm 0 1.2mm 2.4mm; margin-top:3mm; font-size:7.2pt; }}
</style>
<header><h1>Hivemind DHT control plane: timing reference</h1>
<p class="sub">Writes and reads on the averaging-availability key, against the protocol&rsquo;s own
timeouts. Measurements are loopback on one machine and are therefore <b>floors</b>. hivemind 1.2.0.dev0.</p></header>

<h2>1 &nbsp; What the protocol allows</h2>
<table>{tr(["parameter", "default", "source", "what it bounds"], th=True)}{cfg}</table>
<p class="note">Advertisement lifetime and leadership priority are <b>the same field</b>
(<code>key_manager.py:60</code>, read at <code>:88</code>), so lowering the deadline to win also
shortens the advertisement. That only bites at second scale: expirations are absolute timestamps near
1.79e9, where <b>one float step is 2.4e-7&nbsp;s</b>. Undercutting by <code>nextafter</code> wins
outright for 240&nbsp;ns of lifetime, so the coupling is not the defence it appears to be.</p>

<h2>2 &nbsp; Cost of a write and a read, healthy swarm</h2>
<table>{tr(["swarm", "write median (ms)", "write range", "read median (ms)", "read range", "samples"], th=True)}{bas}</table>
<p class="note"><code>declare_averager</code> writes to <code>num_replicas</code> peers;
<code>get_averagers</code> reads with <code>latest=True</code>, which sets
<code>sufficient_expiration_time</code> to infinity (<code>node.py:542</code>) so the traversal
<b>cannot stop early on a good answer</b>. 30 samples per row.</p>

<h2>3 &nbsp; What one slow peer does to a read (8 peers, one attacker)</h2>
<table>{tr(["attacker delay", "victim read median (ms)", "outcome"], th=True)}{atk_html}</table>
<p class="note">The attacker sets the victim&rsquo;s whole read time, because the traversal waits for
the slowest responder it contacts. More slow peers change nothing: 1, 2 and 4 all give ~2.91&nbsp;s.</p>

<h2>4 &nbsp; Whether a new advertisement is ever seen (observed / trials)</h2>
<table>{vh}{vb}</table>
<p class="note">A stretched read outlives the advertisement it fetched: entries are dropped by
<code>_remove_outdated()</code> on read-out (<code>timed_storage.py:96</code>). The invisibility
threshold is about twice the attacker&rsquo;s delay, a number the attacker chooses.</p>

<h2>5 &nbsp; Timings that decide the leader election</h2>
<table>
<tr><th>quantity</th><th>value</th><th>consequence</th></tr>
<tr><td>advertisement lifetime</td><td><b>5.0 s</b></td><td>Set by <code>min_matchmaking_time</code>; also the election priority.</td></tr>
<tr><td>one float step at these timestamps</td><td><b>2.4e-7 s</b></td><td>The cost of undercutting the earliest deadline by <code>nextafter</code>.</td></tr>
<tr><td>blind undercut, §4.3 strategy</td><td><b>1.0 s declared</b></td><td>Wins while alive, but the advertisement survives about 20&nbsp;% of the window.</td></tr>
<tr><td>attacker read then declare budget</td><td><b>&ge; 2 s</b></td><td>Measured tolerance between reading the key and declaring, at a 5&nbsp;s window.</td></tr>
</table>
<p class="note">Measured over 32-peer swarms, 2 repeats: an attacker undercutting by one float step was
followed by <b>100&nbsp;% of honest peers</b> while holding the lowest <code>peer_id</code> in
<b>0 of 40</b> samples , so the win came from the deadline, not from identity. Scope: the election is
computed by applying hivemind's own comparison rule to real DHT records, not observed in live
matchmaking.</p>

<div class="caveat"><b>Reading these numbers.</b> Loopback removes network latency, so sections 2 to 3
are lower bounds and the attack margins are upper bounds. The claim to check first is section 4: an
advertisement at hivemind&rsquo;s shipped 5&nbsp;s lifetime was never observed by a victim whose reads
were stretched to 2.9&nbsp;s, across three trials.</div>
"""


def main():
    out_html = RESEARCH / "hivemind_dht_timings.html"
    out_pdf = RESEARCH / "hivemind_dht_timings.pdf"
    out_html.write_text(html())
    for flags in (["--headless=new"], ["--headless"]):
        cmd = ["chromium", *flags, "--disable-gpu", "--no-pdf-header-footer",
               f"--print-to-pdf={out_pdf}", out_html.as_uri()]
        if subprocess.run(cmd, capture_output=True).returncode == 0 and out_pdf.exists():
            print(f"wrote {out_pdf} ({out_pdf.stat().st_size // 1024} KB)")
            return
    raise SystemExit("chromium failed to render the PDF")


if __name__ == "__main__":
    main()
