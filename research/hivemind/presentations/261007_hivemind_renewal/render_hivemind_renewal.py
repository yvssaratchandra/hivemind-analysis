#!/usr/bin/env python3
"""Render four Step 22 slides using the existing Chalmers presentation layout.

Counts are recalculated from the preserved raw traces; no peers are launched.
"""
from __future__ import annotations

import argparse
import base64
import html
import hashlib
import importlib.util
import json
import math
from pathlib import Path
import re
import shutil
import subprocess
import unicodedata
import xml.etree.ElementTree as ET
import zipfile

DECK = Path(__file__).resolve().parent
PROJECT = DECK.parents[1]
RESULTS = PROJECT / "results"
RENDERED = DECK.parent / "rendered"
SLIDES = DECK / "slides"
STEM = "261007_hivemind_renewal"
W, H, TOTAL = 1280, 720, 4
P, PL = "#472DBE", "#F1EEFF"
RED, RL = "#D52515", "#FCEEEB"
GREEN, GL = "#078945", "#EAF8F0"
INK, GRAY, LINE, BG = "#222222", "#666666", "#D7D3E8", "#FFFFFF"

def escape(value: object) -> str:
    return html.escape(str(value), quote=True)


class Slide:
    def __init__(self, title: str, number: int, section: str, *, cover: bool = False):
        self.title, self.number = title, number
        self.parts = [f'<svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" width="{W}" height="{H}" viewBox="0 0 {W} {H}">']
        self.rect(0, 0, W, H, P if cover else BG)
        self.raw(f"<title>{escape(title)}</title>")
        if not cover:
            self.text(64, 94, title, 47, P, weight="bold")
            self.image(DECK / "logographics/Chalmers-emblem-purple.png", 1147, 20, 83, 76)
            self.line(64, 686, 1218, 686, P, 1)
            self.text(64, 709, "2026-10-07", 18, P)
            self.text(640, 709, section, 18, P, anchor="middle")
            self.text(1218, 709, f"{number} / {TOTAL}", 18, P, anchor="end")

    def raw(self, value: str) -> None:
        self.parts.append(value)

    def rect(self, x, y, w, h, fill=BG, stroke="none", sw=1, radius=0) -> None:
        self.raw(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{radius}" fill="{fill}" stroke="{stroke}" stroke-width="{sw}"/>')

    def box(self, x, y, w, h, *, fill=BG, stroke=LINE, radius=12) -> None:
        self.rect(x, y, w, h, fill, stroke, 2, radius)

    def line(self, x1, y1, x2, y2, color=LINE, sw=2, dash=None) -> None:
        dashed = f' stroke-dasharray="{dash}"' if dash else ""
        self.raw(f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{color}" stroke-width="{sw}"{dashed}/>')

    def arrow(self, x1, y1, x2, y2, color=P, sw=3) -> None:
        self.line(x1, y1, x2, y2, color, sw)
        angle, radius = math.atan2(y2 - y1, x2 - x1), 12
        a = (x2 - radius * math.cos(angle - .48), y2 - radius * math.sin(angle - .48))
        b = (x2 - radius * math.cos(angle + .48), y2 - radius * math.sin(angle + .48))
        self.raw(f'<polygon points="{x2},{y2} {a[0]:.1f},{a[1]:.1f} {b[0]:.1f},{b[1]:.1f}" fill="{color}"/>')

    def text(self, x, y, value, size=26, color=INK, *, weight="normal", anchor="start") -> None:
        self.raw(f'<text x="{x}" y="{y}" fill="{color}" font-family="Liberation Sans, Arial, sans-serif" font-size="{size}" font-weight="{weight}" text-anchor="{anchor}">{escape(value)}</text>')

    def lines(self, x, y, values, size=26, color=INK, gap=36, **kwargs) -> None:
        for i, value in enumerate(values):
            self.text(x, y + i * gap, value, size, color, **kwargs)

    def image(self, path: Path, x, y, w, h) -> None:
        encoded = base64.b64encode(path.read_bytes()).decode("ascii")
        self.raw(f'<image x="{x}" y="{y}" width="{w}" height="{h}" xlink:href="data:image/png;base64,{encoded}"/>')

    def pill(self, x, y, label, width, *, color=P, fill=PL) -> None:
        self.rect(x, y, width, 36, fill, radius=18)
        self.text(x + width / 2, y + 25, label, 17, color, weight="bold", anchor="middle")

    def circle(self, x, y, label, color=P, fill=PL, radius=32) -> None:
        self.raw(f'<circle cx="{x}" cy="{y}" r="{radius}" fill="{fill}" stroke="{color}" stroke-width="2"/>')
        self.text(x, y + 11, label, 29, color, weight="bold", anchor="middle")

    def save(self) -> Path:
        path = SLIDES / f"{self.number:02d}.svg"
        artwork = "\n".join([*self.parts, "</svg>"])
        ET.fromstring(artwork)
        path.write_text(artwork + "\n", encoding="utf-8")
        return path


def analyzer(name):
    spec = importlib.util.spec_from_file_location(name, PROJECT / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_evidence():
    bounded = analyzer("analyze_step22")
    continuous = analyzer("analyze_step22_continuous")
    endurance = analyzer("analyze_step22_endurance")
    wave_dir = RESULTS / "step22_bounded_sweep_20261007_a"
    wave_trials = [bounded.analyse_trial(p.parent) for p in sorted(wave_dir.glob("*/summary.json"))]
    wave = bounded.aggregate(wave_trials)
    saved_wave = json.loads((RESULTS / "step22_bounded_analysis_20261007.json").read_text())
    assert wave == saved_wave["aggregate"] and len(wave_trials) == 56
    round_dir = RESULTS / "step22_continuous_20261007_a"
    round_trials = [continuous.analyse(p.parent) for p in sorted(round_dir.glob("*/summary.json"))]
    saved_rounds = json.loads((RESULTS / "step22_continuous_analysis_20261007.json").read_text())
    rounds = {row["case"]: row for row in saved_rounds["aggregate"]}
    assert len(round_trials) == 21 and len(rounds) == 7
    for name, saved in rounds.items():
        trials = [row for row in round_trials if row["case"] == name]
        assert len(trials) == saved["trials"] == 3
        for key in ("x_leads", "all_groups", "x_ads", "honest_verified", "honest_steps", "degraded_steps"):
            assert sum(row[key] for row in trials) == saved[key]
    persistence_dir = RESULTS / "step22_endurance_20261007_a"
    persistence = [endurance.analyse(p.parent) for p in sorted(persistence_dir.glob("*/summary.json"))]
    assert persistence == json.loads((RESULTS / "step22_endurance_analysis_20261007.json").read_text())
    five_dir = RESULTS / "step22_five_members_20261007_a"
    five = [continuous.analyse(p.parent) for p in five_dir.glob("*/summary.json")]
    assert len(five) == 1 and five[0]["x_leads"] == five[0]["all_groups"] == 9
    disbands = {name: sum(row["follower_reply_codes"].get("GROUP_DISBANDED", 0)
                         for row in round_trials if row["case"] == name) for name in rounds}
    assert disbands["f0p5_m0p05"] == 9 and disbands["f4_m0p05"] == 300
    provenance = json.loads((round_dir / "provenance.json").read_text())
    repo = PROJECT.parents[1]
    for name, digest in provenance["source_sha256"].items():
        assert hashlib.sha256((repo / "hivemind" / name).read_bytes()).hexdigest() == digest
    result = dict(wave=wave, rounds=rounds, persistence=persistence, five=five[0],
                  disband_replies=disbands, raw_trials_checked=81,
                  source_commit=provenance["commit"], source_sha256=provenance["source_sha256"],
                  suites=[str(path.relative_to(PROJECT)) for path in
                          (wave_dir, round_dir, persistence_dir, five_dir)])
    (DECK / "evidence-check.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def make_deck(evidence):
    slides, notes = [], []

    def add(slide, note):
        slides.append(slide)
        notes.append(note.strip())

    slide = Slide("Leadership through own-ad renewal", 1, "Step 22 · Mechanism")
    slide.text(640, 165, "Can a peer lead repeatedly using only its own advertisements?", 30, anchor="middle")
    slide.pill(321, 203, "Earlier expiration → higher leader priority", 638)
    cards = [(64, 245, "Peer X", ["Publishes a short-lived ad", "Waits for inbound followers"]),
             (373, 265, "DHT group key", ["X: active, expiration E", "Later E replaces earlier E"]),
             (702, 235, "Honest peers", ["Read advertisements", "Join an earlier candidate"]),
             (1001, 215, "Group", ["Native admission", "Native allreduce"])]
    for i, (x, width, title, body) in enumerate(cards):
        slide.box(x, 297, width, 164, fill=PL, stroke="none")
        slide.text(x + width / 2, 343, title, 28, P, weight="bold", anchor="middle")
        slide.lines(x + width / 2, 393, body, 21, gap=32, anchor="middle")
        if i < 3:
            slide.arrow(x + width + 8, 379, cards[i + 1][0] - 10, 379)
    slide.text(640, 533, "Renew with an increasing expiration: E_new = max(nextafter(E_old, +∞), now + d)",
               25, P, anchor="middle")
    slide.text(640, 588, "d is the remaining lifetime; f is the requested write frequency.", 25, GRAY, anchor="middle")
    slide.text(640, 637, "X changes its own publisher and refuses outbound joins.", 28, P, weight="bold", anchor="middle")
    add(slide, """Step 22 tests a different policy from adaptive undercutting. X does not read competitor advertisements through the matchmaking API and does not forge another peer's records. Its publisher uses a short remaining lifetime d, renews at requested frequency f, and waits for inbound followers. The harness retains the stock admission checks and allreduce.

Hivemind stores availability under the group key and a PeerID subkey. The expiration serves both as the record lifetime and as leader priority. Peers prefer an earlier (expiration, PeerID-bytes) tuple, while a holder only replaces an existing record when the incoming expiration is strictly greater. The publisher therefore uses E_new = max(nextafter(E_old, +infinity), now + d), capped at the current search deadline. It never declares a remaining lifetime above five seconds. A renewal normally advances E by approximately one send period; it does not keep priority fixed. Requested frequency can exceed achieved frequency when stores serialize.

For overlapping holder visibility, d > 1/f + write latency + jitter margin is a useful condition, not a leadership guarantee. DHT positions are ordinary seeded positions, not crafted identities. Sources: [publisher](../../step22_refresh_leadership.py), repository `hivemind/averaging/key_manager.py`, `hivemind/utils/timed_storage.py`, and `hivemind/averaging/matchmaking.py`. The diagram shows logical decisions, not measured packet timing.""")

    slide = Slide("Eight peers, controls, repeated rounds", 2, "Step 22 · Local experiment")
    slide.text(640, 165, "Seven honest peers + X · loopback DHT · tiny tensors", 30, anchor="middle")
    cx, cy, radius = 295, 367, 145
    for i in range(8):
        angle = math.radians(-90 + i * 45)
        x, y = cx + radius * math.cos(angle), cy + radius * math.sin(angle)
        slide.line(cx, cy, x, y, LINE, 2)
        slide.circle(x, y, "X" if i == 0 else f"H{i}", RED if i == 0 else P,
                     RL if i == 0 else PL, 29)
    slide.circle(cx, cy, "DHT", P, PL, 48)
    slide.lines(295, 555, ["Target: four members", "Native admission and allreduce"],
                25, P, gap=36, anchor="middle")
    for y, title, body in [(209, "56 grouping trials", "14 policies × 4 seeded arrival scenarios"),
                           (319, "21 continuous trials", "7 policies × 3 new seeds; 30 s windows + drain"),
                           (429, "Verified completion", "Contributor IDs, steps, inputs, every output tensor")]:
        slide.box(565, y, 652, 93, fill=PL, stroke="none")
        slide.text(590, y + 35, title, 29, P, weight="bold")
        slide.text(590, y + 69, body, 23)
    slide.text(590, 565, "X returns after 0.05 s in every continuous arm.", 25, RED, weight="bold")
    slide.text(590, 599, "Honest simulated training delay: mean 1.5 s.", 25, GRAY)
    slide.text(640, 649, "Compare stock, passive stock, expiration-only, and periodic refresh.", 27, P, anchor="middle")
    add(slide, """All trials are private eight-peer loopback experiments with standalone DecentralizedAverager instances and 128-element tensors. They are not full Optimizer training. There is no explicit scheduled_time. Honest peers have the default five-second matchmaking window, a two-member minimum, and a three-second join-request timeout. The target is four. Each step has a twelve-second matchmaking limit and a six-second allreduce limit. Empty group bits keep repeated steps under the same group key. Core Hivemind source is unchanged.

The grouping wave uses two seeds (2202, 2203) with staggered and bursty arrivals: four scenarios per policy, fresh networks per trial. X starts independently over three seconds. In this sweep the stock control has a two-member minimum and may follow; the passive control refuses outbound joins and has minimum four. All 56 final trials returned correct tensors; one honest step did not find a group. The 0.5/s, d=2.085764 s policy led 4/4 scenarios using five active ad writes; both controls led 3/4. These are small counts, not deployment success rates.

Continuous trials use held-out seeds 2211–2213. New submissions last thirty seconds and already submitted steps then drain. X has minimum four in every continuous arm, including stock. Honest re-entry delays are lognormal with mean 1.5 s, coefficient of variation 0.6, clipped to 0.1–4 s. X returns after 0.05 s in every arm. That fast re-entry gives X more opportunities and prevents interpreting these counts as an ad-only advantage over equally paced peers. The output check matches contributor identities, step numbers, input values, and every member's tensor; a successful future is insufficient. See the [full methods](../../results/step22_refresh_report_20261007.md) and exact tables in the notes appendix.""")

    slide = Slide("Short ads lead often; writes add cost", 3, "Step 22 · Measured continuous workload")
    slide.text(640, 162, "Totals across three 30-second submission windows, including drain", 27, GRAY, anchor="middle")
    slide.rect(64, 205, 1152, 57, P, radius=8)
    slide.text(86, 243, "Policy / remaining lifetime", 26, BG, weight="bold")
    slide.text(860, 243, "X-led / verified", 26, BG, weight="bold", anchor="middle")
    slide.text(1120, 243, "Ad writes", 26, BG, weight="bold", anchor="middle")
    rows = [("stock4", "Stock, minimum four / 5 s"),
            ("passive5", "Passive stock publisher / 5 s"),
            ("expire125", "Expiration-only / 1.25 s"),
            ("f0p5_m0p05", "Refresh 0.5/s / 2.086 s"),
            ("f4_m0p05", "Refresh 4/s / 0.336 s")]
    for i, (name, label) in enumerate(rows):
        y = 263 + i * 55
        color = GREEN if name == "f0p5_m0p05" else INK
        slide.rect(64, y, 1152, 54, GL if name == "f0p5_m0p05" else (PL if i % 2 == 0 else BG))
        row = evidence["rounds"][name]
        slide.text(86, y + 36, label, 27, color, weight="bold" if name == "f0p5_m0p05" else "normal")
        slide.text(860, y + 36, f"{row['x_leads']} / {row['all_groups']}", 29, color, anchor="middle")
        slide.text(1120, y + 36, row["x_ads"], 29, color, anchor="middle")
    low, high = evidence["rounds"]["f0p5_m0p05"], evidence["rounds"]["f4_m0p05"]
    slide.text(640, 591, f"4/s: +{high['x_leads'] - low['x_leads']} verified X-led groups; {high['x_ads'] / low['x_ads']:.2f}× ad writes", 33, P, weight="bold", anchor="middle")
    slide.text(640, 631, "Five incorrect-tensor steps excluded across the seven tested policies.", 24, GRAY, anchor="middle")
    slide.text(640, 662, "Local observations; frequency and lifetime vary together in the refresh comparison.", 20, GRAY, anchor="middle")
    add(slide, """The table selects the controls, expiration-only alternative, and the lowest and highest tested refresh frequencies. The full seven-policy table is in the appendix. Numerators are verified X-led groups; denominators are all verified groups in that policy's three runs. These are accumulated observations including drain after the thirty-second submission windows, not requests per second or estimated long-run probabilities.

The 0.5/s policy produced 104 verified X-led groups with 117 active ad writes; 4/s produced 107 with 393 writes, a 3.36-fold write count. Three extra groups across three seeds do not establish a repeatable gain. Frequency and lifetime both change, so this is not a causal estimate of frequency alone. Expiration-only 1.25-second ads produced 106 verified X-led groups with 143 writes; periodic overlap is therefore not necessary for high observed leadership counts in this workload. Its purpose is record continuity, a distinct property.

Five completed steps returned incorrect tensors: one in 0.5/s, three in 1/s, and one in 2/s. Groups containing these steps were excluded from the verified-group counts. The traces establish the errors but do not establish their cause or a causal link to advertisement policy. No forbidden X competitor reads or outbound joins occurred. Writes count high-level active ad attempts, excluding withdrawals, DHT routing walks and other traffic; bytes were not measured. Evidence: [continuous raw traces](../../results/step22_continuous_20261007_a), [analysis](../../results/step22_continuous_analysis_20261007.json).""")

    slide = Slide("A visible ad can still lose its group", 4, "Step 22 · Constraint and next test")
    slide.text(640, 165, "A later advertisement does not extend an accepted follower's wait.", 30, anchor="middle")
    slide.text(65, 278, "DHT ad", 25, P, weight="bold")
    slide.text(65, 373, "Captured wait", 25, RED, weight="bold")
    slide.arrow(285, 284, 1185, 284, GRAY, 2)
    slide.line(333, 284, 1135, 284, GREEN, 10)
    slide.line(333, 380, 930, 380, RED, 8)
    for x, title in [(333, "Join accepted"), (627, "Renew ad"), (930, "Old expiry E₁"), (1135, "New expiry E₂")]:
        slide.line(x, 250, x, 398, LINE, 2, "6,5")
        slide.text(x, 228, title, 23, P if x != 930 else RED, anchor="middle")
    slide.text(627, 326, "Advertisement remains visible", 24, GREEN, anchor="middle")
    slide.text(930, 425, "Undersized group → disband", 26, RED, weight="bold", anchor="middle")
    slide.box(64, 466, 551, 123, fill=RL, stroke="none")
    slide.text(88, 505, "Disband replies: 9 vs 300", 29, RED, weight="bold")
    slide.text(88, 543, "0.5/s vs 4/s across continuous runs", 24)
    slide.text(88, 575, "Stream replies, not distinct groups", 20, GRAY)
    slide.box(640, 466, 576, 123, fill=GL, stroke="none")
    slide.text(664, 505, "Continuity: 292/292 active probes", 29, GREEN, weight="bold")
    slide.text(664, 543, "Safe 0.5/s renewal over 30 seconds", 24)
    slide.text(664, 575, "40 ms storage-processing delay; no matchmaking", 20, GRAY)
    slide.text(640, 635, "Next: full Optimizer scheduling and routed-delay contention", 29, P, weight="bold", anchor="middle")
    slide.text(640, 664, "WAN behavior, longer runs, and the tensor-error cause remain open.", 22, GRAY, anchor="middle")
    add(slide, """The timeline is schematic. In rpc_join_group, after ACCEPTED, the native leader handler computes a wait timeout from its current declared expiration once. Later publisher renewals do not replace that already captured timer. When the earlier timer fires, the leader can assemble if its native minimum is met; otherwise it can disband an undersized group despite a renewed DHT advertisement. Setting target at least minimum is necessary because the target-size assembly path otherwise bypasses the minimum-size timeout check.

The continuous 4/s policy emitted 300 GROUP_DISBANDED replies, versus nine at 0.5/s. These are replies on join streams, not counts of unique groups. The comparison also varies remaining lifetime and reflects this local workload; faster writes did not reliably cure assembly churn.

Persistence is a separate record-visibility test: fixed 40 ms remote rpc_store processing delay, eight DHT peers, independent latest=True probes every 0.1 s, thirty seconds of own-ad writes, no matchmaking. Safe 0.5/s renewal was active at 292/292 probes; safe 2/s at 295/295. A 2/s control with d=0.2 s was active at only 80/295 probes despite 60/60 accepted writes. This is storage-delay emulation, not WAN-delay emulation or proof of indefinite availability. Interior holder intervals exclude local self-stores, initial propagation and final expiration.

A separate capability check with native minimum five and target five formed nine verified X-led five-member groups on one seed during a twelve-second submission window plus drain; 36/37 honest steps completed correctly. It establishes feasibility in that setup, not assured assembly. Next tests should restore full Optimizer-driven epoch scheduling and then add routed delays under controlled conditions. Longer-run behavior, WAN loss/churn, and the five incorrect-tensor steps remain unresolved. Source: repository `hivemind/averaging/matchmaking.py` around rpc_join_group; [persistence traces](../../results/step22_endurance_20261007_a), [five-member traces](../../results/step22_five_members_20261007_a).""")
    assert len(slides) == len(notes) == TOTAL
    return slides, notes


def evidence_appendix(evidence):
    lines = ["## Appendix — exact experiment tables", "", "These tables supplement the four slides; they are not additional slides.", "",
             "### Grouping sweep: 56 trials", "", "Two seeds × two arrival patterns per policy; fresh networks per trial.", "",
             "| Policy | f (/s) | d (s) | X-led / trials | Honest completed / steps | Active ad writes |",
             "| --- | ---: | ---: | ---: | ---: | ---: |"]
    for row in evidence["wave"]:
        policy = row["policy"]
        lines.append(f"| {row['case']} | {policy.get('frequency', 'At expiration')} | {policy.get('lifetime', 5)} | {row['completed_leads']}/{row['trials']} | {row['honest_completed']}/{row['honest_steps']} | {row['total_x_ad_attempts']} |")
    lines += ["", "### Continuous workload: 21 trials", "", "Three seeds per policy; thirty-second submission windows plus drain. X re-enters after 0.05 s in every arm.", "",
              "| Policy | f (/s) | d (s) | Verified X-led / all groups | Verified honest / attempted | Active ad writes | Incorrect steps | Disband replies |",
              "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for row in evidence["rounds"].values():
        policy = row["policy"]
        lines.append(f"| {row['case']} | {policy.get('frequency', 'At expiration')} | {policy.get('lifetime', 5)} | {row['x_leads']}/{row['all_groups']} | {row['honest_verified']}/{row['honest_steps']} | {row['x_ads']} | {row['degraded_steps']} | {evidence['disband_replies'][row['case']]} |")
    lines += ["", "### Persistence: thirty seconds, fixed 40 ms storage delay", "",
              "| Policy | Accepted writes | Active probes | Active after initial expiry | Interior holder gaps |",
              "| --- | ---: | ---: | ---: | ---: |"]
    for row in evidence["persistence"]:
        lines.append(f"| {row['case']} | {row['accepted_writes']}/{row['writes']} | {row['active_probes']}/{row['probe_count']} | {row['active_after_initial_expiry']}/{row['probes_after_initial_expiry']} | {row['interior_holder_gaps']} |")
    five = evidence["five"]
    lines += ["", "### Native minimum-five capability check", "",
              f"Seed 2211, f=1/s, d=1.085764 s, twelve-second submission window plus drain: {five['x_leads']} verified X-led groups, each of size five; {five['honest_verified']}/{five['honest_steps']} honest steps verified; zero incorrect returned tensors.", "",
              "### Evidence and source pin", "",
              f"Runtime source commit: `{evidence['source_commit']}`. The renderer checked all eight runtime source digests against the unchanged repository source and reanalysed all {evidence['raw_trials_checked']} final trials.", "",
              "- [Full report](../../results/step22_refresh_report_20261007.md)",
              "- [Raw grouping trials](../../results/step22_bounded_sweep_20261007_a)",
              "- [Raw continuous trials](../../results/step22_continuous_20261007_a)",
              "- [Raw persistence trials](../../results/step22_endurance_20261007_a)",
              "- [Raw minimum-five trial](../../results/step22_five_members_20261007_a)",
              "- [Recalculated presentation evidence](evidence-check.json)", ""]
    return "\n".join(lines)


def latex_text(value):
    value = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", value)
    value = value.translate(str.maketrans({"’": "'", "–": "-", "—": "-", "→": "to", "×": "x"}))
    value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode()
    mapping = {"\\": r"\textbackslash{}", "&": r"\&", "%": r"\%", "$": r"\$", "#": r"\#", "_": r"\_", "{": r"\{", "}": r"\}", "~": r"\textasciitilde{}", "^": r"\textasciicircum{}"}
    return "".join(mapping.get(c, c) for c in value)


def write_notes(slides, notes, evidence):
    markdown = ["# Hivemind own-ad renewal — speaker notes", "", "Four slides · 7 October 2026 · Sarat Yellapragada, Secura Lab", ""]
    tex = [r"\documentclass[aspectratio=169]{beamer}", r"\usepackage{graphicx,tikz,pgfpages}", r"\setbeamertemplate{navigation symbols}{}", r"\ifdefined\presenternotes\setbeameroption{show notes on second screen=right}\else\setbeameroption{hide notes}\fi", r"\begin{document}"]
    for slide, note in zip(slides, notes):
        markdown += [f"## {slide.number}. {slide.title}", "", note, ""]
        tex += [r"\begin{frame}[plain]", r"\begin{tikzpicture}[remember picture,overlay]\node at (current page.center) {\includegraphics[width=\paperwidth,height=\paperheight]{slides/" + f"{slide.number:02d}.pdf" + r"}};\end{tikzpicture}", r"\note{" + latex_text(note) + "}", r"\end{frame}"]
    markdown.append(evidence_appendix(evidence))
    tex.append(r"\end{document}")
    (DECK / "speaker-notes.md").write_text("\n".join(markdown).rstrip() + "\n")
    (DECK / "hivemind-renewal.tex").write_text("\n".join(tex) + "\n")
    (DECK / "hivemind-renewal-notes.tex").write_text("\\def\\presenternotes{1}\n\\input{hivemind-renewal.tex}\n")


def make_previews(pdf):
    previews = RENDERED / f"{STEM}-previews"
    previews.mkdir(exist_ok=True)
    subprocess.run(["pdftoppm", "-png", "-scale-to-x", "1280", "-scale-to-y", "-1", str(pdf), str(previews / "slide")], check=True, capture_output=True)
    pictures = sorted(previews.glob("slide-*.png"), key=lambda p: int(p.stem.split("-")[-1]))
    assert len(pictures) == TOTAL
    parts = ['<svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" width="1320" height="780">', '<rect width="1320" height="780" fill="#EEEEF4"/>']
    for i, picture in enumerate(pictures):
        x, y = 20 + (i % 2) * 650, 20 + (i // 2) * 380
        encoded = base64.b64encode(picture.read_bytes()).decode()
        parts.append(f'<image x="{x}" y="{y}" width="630" height="354.375" xlink:href="data:image/png;base64,{encoded}"/>')
    parts.append("</svg>")
    sheet = previews / "contact-sheet.svg"
    sheet.write_text("\n".join(parts) + "\n")
    subprocess.run(["rsvg-convert", "-o", str(previews / "contact-sheet.png"), str(sheet)], check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--svg-only", action="store_true")
    args = parser.parse_args()
    if not args.svg_only and any(shutil.which(tool) is None for tool in ("rsvg-convert", "pdfunite", "pdftoppm", "pdfinfo", "pdftotext")):
        parser.error("PDF tools missing; use --svg-only to generate editable artwork and notes.")
    SLIDES.mkdir(exist_ok=True)
    evidence = load_evidence()
    slides, notes = make_deck(evidence)
    write_notes(slides, notes, evidence)
    pdfs = []
    for slide in slides:
        svg = slide.save()
        if not args.svg_only:
            pdf = svg.with_suffix(".pdf")
            subprocess.run(["rsvg-convert", "-f", "pdf", "-o", str(pdf), str(svg)], check=True)
            pdfs.append(str(pdf))
    if args.svg_only:
        print(f"Wrote four editable slides and checked {evidence['raw_trials_checked']} raw trials.")
        return
    RENDERED.mkdir(exist_ok=True)
    combined = RENDERED / f"{STEM}.pdf"
    subprocess.run(["pdfunite", *pdfs, str(combined)], check=True)
    info = subprocess.run(["pdfinfo", str(combined)], check=True, capture_output=True, text=True).stdout
    assert re.search(r"^Pages:\s+4$", info, re.M)
    (RENDERED / f"{STEM}-pdfinfo.txt").write_text(info)
    subprocess.run(["pdftotext", "-bbox", str(combined), str(RENDERED / f"{STEM}-text-bounds.html")], check=True)
    make_previews(combined)
    archive = DECK.parent / f"{STEM}-overleaf.zip"
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as packed:
        for path in sorted(DECK.rglob("*")):
            if path.is_file() and "__pycache__" not in path.parts:
                packed.write(path, path.relative_to(DECK))
    print(f"Rendered four slides: {combined}\nEditable Overleaf archive: {archive}\nRaw trials rechecked: {evidence['raw_trials_checked']}")


if __name__ == "__main__":
    main()

