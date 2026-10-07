#!/usr/bin/env python3
"""Render the Exo overview in the style of the earlier Hivemind talk.

Uses stdlib for editable SVGs/notes/Beamer sources; librsvg and Poppler render
PDFs and previews. Run with --svg-only when those tools are unavailable.
"""
from __future__ import annotations

import argparse
import base64
import html
import json
import math
from pathlib import Path
import re
import shutil
import subprocess
import unicodedata
import xml.etree.ElementTree as ET


DECK = Path(__file__).resolve().parent
PROJECT = DECK.parents[1]
RESULTS = PROJECT / "results" / "exo-audit-20261006-005"
RENDERED = DECK.parent / "rendered"
SLIDES = DECK / "slides"
STEM = "261006_exo_overview"
REVISION = "21a54c5ea0230a3bec1e1a786d200126c7e34ec6"
SOURCE = f"https://github.com/exo-explore/exo/blob/{REVISION}/"
W, H, TOTAL = 1280, 720, 9
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
            self.text(64, 94, title, 49, P, weight="bold")
            self.image(DECK / "logographics/Chalmers-emblem-purple.png", 1147, 20, 83, 76)
            self.line(64, 686, 1218, 686, P, 1)
            self.text(64, 709, "2026-10-06", 18, P)
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


def device(slide: Slide, x, y, label, width=175) -> None:
    slide.box(x, y, width, 92, stroke=P, radius=8)
    slide.text(x + width / 2, y + 55, label, 27, P, weight="bold", anchor="middle")
    slide.line(x - 8, y + 103, x + width + 8, y + 103, P, 4)


def load_evidence() -> tuple[dict, dict[str, dict], dict[str, list[dict]]]:
    summary = json.loads((RESULTS / "summary.json").read_text())
    manifest = json.loads((RESULTS / "manifest.json").read_text())
    if not summary["run_complete"] or not summary["positive_control_established"]:
        raise ValueError("Presentation requires a completed, controlled run.")
    if manifest["source_revision"] != REVISION:
        raise ValueError("Source revision differs from presentation references.")
    results, records = {}, {}
    for item in summary["results"]:
        arm = item["arm"]
        results[arm] = item
        records[arm] = [json.loads(line) for line in (RESULTS / arm / "probe.jsonl").read_text().splitlines()]
        if not item["session_unchanged"] or not item["honest_identities_unchanged"]:
            raise ValueError(f"Unstable arm: {arm}")
    forged = results["forged"]["observations"]
    if forged["outcome"] != "accepted" or forged["target_node_id"] == forged["publisher_node_id"]:
        raise ValueError("Raw result does not support the identity example.")
    return manifest, results, records


def make_deck() -> tuple[list[Slide], list[str]]:
    manifest, results, records = load_evidence()
    slides, notes = [], []

    def add(slide: Slide, note: str) -> None:
        slides.append(slide)
        notes.append(note.strip())

    # 1: a short introduction, in the established Chalmers cover layout.
    slide = Slide("Exo", 1, "Introduction", cover=True)
    slide.image(DECK / "logographics/Chalmers-emblem-white.png", 1120, 25, 112, 102)
    slide.text(64, 270, "Exo", 84, BG, weight="bold")
    slide.text(67, 344, "Several devices. One AI model.", 38, BG)
    slide.text(67, 403, "How it works · control plane · security questions", 28, BG)
    slide.text(67, 535, "Sarat Yellapragada", 28, BG)
    slide.text(67, 575, "Secura Lab", 27, BG)
    slide.text(67, 622, "Chalmers University of Technology   |   6 October 2026", 25, BG)
    add(slide, f"""Exo connects devices into a local AI inference cluster. A large model can be divided among devices rather than requiring its entire memory footprint on one device. This talk answers four questions: what problem Exo solves, how its architecture works, where the control plane sits, and which security problems are worth investigating. The architecture is source-reviewed at commit {REVISION}; our measurements are one private, model-free local trial per experiment arm. We do not present inference benchmarks or a public-swarm result.

Sources: [pinned Exo README]({SOURCE}README.md), [local audit report](../../results/exo-audit-20261006-005/REPORT.md).""")

    # 2: the model capacity problem, without an invented hardware budget.
    slide = Slide("What problem does Exo solve?", 2, "1 · The problem")
    slide.text(640, 167, "A model can be too large for one device.", 31, INK, anchor="middle")
    slide.text(258, 221, "One device", 29, P, weight="bold", anchor="middle")
    slide.text(877, 221, "Several devices", 29, P, weight="bold", anchor="middle")
    slide.box(76, 248, 365, 316, fill="#F8F7FC", stroke="none")
    slide.box(550, 248, 652, 316, fill="#F8F7FC", stroke="none")
    slide.box(116, 283, 285, 84, fill=RL, stroke=RED)
    slide.text(258, 320, "Full model", 28, RED, weight="bold", anchor="middle")
    slide.text(258, 350, "Does not fit", 24, RED, anchor="middle")
    slide.arrow(258, 372, 258, 407, RED)
    device(slide, 176, 416, "Device", 164)
    for i, x in enumerate((586, 790, 994), 1):
        slide.box(x + 8, 283, 159, 84, fill=GL, stroke=GREEN)
        slide.lines(x + 88, 320, ["Model part", str(i)], 25, GREEN, 31, weight="bold", anchor="middle")
        slide.arrow(x + 88, 372, x + 88, 408, GREEN)
        device(slide, x, 416, f"Device {i}")
    slide.text(640, 628, "Exo splits the model and coordinates the work.", 31, P, weight="bold", anchor="middle")
    add(slide, f"""The main problem is model capacity. Model weights plus runtime state may exceed one device's available memory. Exo can place parts of a model on multiple compatible devices so that they jointly execute inference. The drawing is illustrative: it does not assert a particular RAM requirement, equal-sized partitions, or a speedup. The actual partition depends on model constraints, available memory, supported backends, and usable network links. Exo supports pipeline and tensor parallel arrangements; the picture simply shows that devices share the model's work. Adding devices also adds communication, so faster inference is not guaranteed by this capacity argument.

Sources: [purpose and features]({SOURCE}README.md), [placement constraints]({SOURCE}src/exo/master/placement.py#L106).""")

    # 3: plain-language sequence; detailed component order stays in notes.
    slide = Slide("How does it work?", 3, "2 · From devices to an answer")
    slide.text(640, 168, "Each device runs Exo and joins the local cluster.", 30, INK, anchor="middle")
    steps = [
        (76, "Find peers", ["Discover devices", "on the network"]),
        (365, "Elect a master", ["Choose a peer", "to coordinate work"]),
        (654, "Place the model", ["Use memory and links", "to select workers"]),
        (943, "Run inference", ["Workers compute", "the answer together"]),
    ]
    for i, (x, title, body) in enumerate(steps, 1):
        slide.box(x, 282, 260, 225, fill=PL, stroke="none")
        slide.circle(x + 130, 262, i)
        slide.text(x + 130, 342, title, 27, P, weight="bold", anchor="middle")
        slide.lines(x + 130, 410, body, 25, INK, 35, anchor="middle")
        if i < 4:
            slide.arrow(x + 266, 380, x + 283, 380, P)
    slide.text(640, 591, "The master is one of the devices in the cluster.", 29, P, weight="bold", anchor="middle")
    slide.text(640, 633, "It can change when peers join, leave, or lose connectivity.", 25, GRAY, anchor="middle")
    add(slide, f"""Devices discover namespace-compatible peers using IPv6 multicast, then establish Zenoh connections. Election selects a coordinator, called the Master in the source. Each normal worker reports information such as available RAM, network interfaces, and supported backends. The master uses device profiles and a separately constructed placement topology to choose a model instance's workers and assign shards. That topology includes usable socket or RDMA links; it is not merely the list of discovered peers. Selected workers load their assignments. For an inference request, an API issues a command, the master creates tasks for a suitable model instance, and the assigned workers execute them and return output through the event stream. Discovery and elections also happen as connectivity changes; this is a high-level lifecycle, not a new election for every prompt. A connection change can trigger an election without changing the final master. Recovery behavior after failures still needs testing.

Sources: [discovery]({SOURCE}rust/networking/src/discovery.rs), [election]({SOURCE}src/exo/shared/election.py), [device reports]({SOURCE}src/exo/utils/info_gatherer/info_gatherer.py#L444), [placement]({SOURCE}src/exo/master/placement.py), [request scheduling]({SOURCE}src/exo/master/main.py#L183).""")

    # 4: host placement of logical roles, and a selected model group.
    slide = Slide("High-level architecture", 4, "2 · Roles inside the cluster")
    slide.text(640, 165, "Devices are peers; one elected peer coordinates.", 29, INK, anchor="middle")
    slide.box(278, 209, 938, 366, fill="#F8F7FC", stroke=LINE, radius=16)
    slide.text(306, 245, "Exo cluster", 24, P, weight="bold")
    for x, name in ((302, "Device A"), (612, "Device B"), (922, "Device C")):
        slide.box(x, 272, 266, 267, fill=BG)
        slide.text(x + 133, 310, name, 28, P, weight="bold", anchor="middle")
    slide.box(319, 338, 232, 64, fill=PL, stroke="none")
    slide.text(435, 379, "API / shared state", 24, P, anchor="middle")
    slide.box(629, 338, 232, 64, fill=P, stroke="none")
    slide.text(745, 366, "Elected master", 25, BG, weight="bold", anchor="middle")
    slide.text(745, 392, "Shares state and tasks", 18, BG, anchor="middle")
    slide.box(939, 338, 232, 64, fill=PL, stroke="none")
    slide.text(1055, 379, "API / shared state", 24, P, anchor="middle")
    slide.lines(154, 340, ["App /", "user"], 29, P, 36, weight="bold", anchor="middle")
    slide.arrow(219, 369, 310, 369, P)
    slide.text(260, 347, "request", 20, GRAY, anchor="middle")
    slide.arrow(618, 369, 557, 369, P)
    slide.arrow(871, 369, 929, 369, P)
    slide.rect(316, 420, 550, 110, GL, GREEN, 1, 10)
    slide.box(332, 442, 210, 63, fill=BG, stroke=GREEN)
    slide.text(437, 483, "Worker · part 1", 23, GREEN, weight="bold", anchor="middle")
    slide.box(642, 442, 210, 63, fill=BG, stroke=GREEN)
    slide.text(747, 483, "Worker · part 2", 23, GREEN, weight="bold", anchor="middle")
    slide.arrow(550, 474, 631, 474, GREEN)
    slide.arrow(631, 474, 550, 474, GREEN)
    slide.box(939, 442, 232, 63, fill="#F4F4F4", stroke=LINE)
    slide.text(1055, 483, "Worker · available", 23, GRAY, anchor="middle")
    slide.text(589, 563, "Selected group for this model", 23, GREEN, weight="bold", anchor="middle")
    slide.text(640, 624, "A model can use a subset of the wider cluster.", 29, P, weight="bold", anchor="middle")
    add(slide, f"""This is a diagram of logical roles inside three physical devices. The API can run on any device; the app enters through A's API in this example. Every normal node has an API and worker unless disabled. B is the elected master and may also execute inference as a worker. All nodes initially create a Master object, but election results determine which one is active. The active master indexes events and coordinates placement and tasks. A and B are selected for this model; C is discovered but its worker is not selected for this instance. Instance assignments therefore form a group within the wider cluster, unlike Hivemind's temporary gradient-averaging groups. An Exo model instance's group exists to execute its model. The shaded group is not a separate discovery network. The figure omits B's own API and replicated-state box for space. The master is hosted by a peer, rather than drawn as a permanent external service. The purple arrows show the master distributing shared state and tasks; API commands travel back through peer messaging, which is omitted to keep the drawing simple. This is not a picture of physical network hops or the full peer connection graph. The green link summarizes worker communication without specifying a particular parallelism mode.

Sources: [node components]({SOURCE}src/exo/main.py#L53), [promotion and demotion]({SOURCE}src/exo/main.py#L178), [instance assignments]({SOURCE}src/exo/master/placement.py#L254), [per-instance topology]({SOURCE}src/exo/shared/topology.py#L200).""")

    # 5: functions define the planes; the transport caveat goes in notes.
    slide = Slide("Where is the control plane?", 5, "3 · Decisions before computation")
    slide.box(76, 202, 1126, 178, fill=PL, stroke="none")
    slide.text(108, 246, "Control plane", 31, P, weight="bold")
    slide.text(1167, 246, "Decides who does what", 27, P, anchor="end")
    items = [(256, "Membership / election"), (640, "Resources / placement"), (1024, "Tasks / shared state")]
    for x, text in items:
        slide.text(x, 325, text, 25, P, weight="bold", anchor="middle")
    slide.arrow(640, 392, 640, 440, P)
    slide.text(671, 422, "assignments and tasks", 23, GRAY)
    slide.box(76, 453, 1126, 135, fill=GL, stroke="none")
    slide.text(108, 494, "Data plane", 31, GREEN, weight="bold")
    slide.text(1167, 494, "Workers compute the model", 27, GREEN, anchor="end")
    for x, label in ((328, "Worker A"), (640, "Worker B"), (952, "Worker C")):
        slide.text(x, 553, label, 25, GREEN, weight="bold", anchor="middle")
    slide.arrow(400, 544, 563, 544, GREEN)
    slide.arrow(563, 544, 400, 544, GREEN)
    slide.arrow(712, 544, 875, 544, GREEN)
    slide.arrow(875, 544, 712, 544, GREEN)
    slide.text(640, 582, "Model data exchanged between selected workers", 20, GREEN, anchor="middle")
    slide.text(640, 638, "A wrong plan can affect the work before computation starts.", 29, INK, anchor="middle")
    add(slide, f"""The control plane is the part that decides who belongs to the cluster, which peer is master, which device information to trust, where to place a model, and which tasks should run. The master broadcasts indexed events so peers maintain a common view. Model execution is the work those decisions direct: workers use their model shards and exchange model-parallel data. This separation is by function, not by packet size or a perfectly separate transport. Zenoh pub/sub carries election messages, commands, state events, and also inference task payloads and generated output chunks. MLX distributed carries communication between model workers, including tensor send/receive and gather operations. For security research we can initially test decisions and state with synthetic metadata, without loading a large model. A false accepted advertisement may lead to a wrong plan, but we must test that next step rather than assume it.

Sources: [typed topics]({SOURCE}src/exo/routing/topics.py#L40), [Zenoh messaging]({SOURCE}rust/networking/src/swarm.rs#L152), [master indexing]({SOURCE}src/exo/master/main.py#L492), [MLX group initialization]({SOURCE}src/exo/worker/engines/mlx/utils_mlx.py#L90), [model-parallel communication]({SOURCE}src/exo/worker/engines/mlx/auto_parallel.py#L132).""")

    # 6: the measured result, with named roles rather than opaque hexadecimal IDs.
    slide = Slide("What did our first audit find?", 6, "Measured · one local trial per arm")
    slide.pill(76, 152, "2 honest peers + 1 controlled publisher", 377)
    slide.pill(870, 152, "No models or inference", 332, color=GRAY, fill="#F4F4F4")
    xs = (222, 640, 1057)
    slide.circle(xs[0], 284, "C", RED, RL, 43)
    slide.circle(xs[1], 284, "B", P, PL, 43)
    slide.circle(xs[2], 284, "A+B", P, PL, 49)
    slide.text(xs[0], 361, "Controlled publisher", 27, RED, weight="bold", anchor="middle")
    slide.text(xs[1], 361, "Honest master", 27, P, weight="bold", anchor="middle")
    slide.text(xs[2], 361, "Both honest peers", 27, P, weight="bold", anchor="middle")
    slide.arrow(281, 284, 582, 284, RED)
    slide.text(432, 231, "“B has this much RAM”", 25, RED, anchor="middle")
    slide.arrow(699, 284, 988, 284, P)
    slide.text(844, 231, "Master indexes the claim", 24, P, anchor="middle")
    slide.box(96, 414, 1090, 102, fill=RL, stroke="none")
    slide.text(641, 458, "Both peers recorded C’s claim under B’s identity.", 30, RED, weight="bold", anchor="middle")
    slide.text(641, 497, "The entry was absent before the message.", 25, INK, anchor="middle")
    slide.text(640, 578, "Metadata acceptance observed. Placement impact remains open.", 27, INK, anchor="middle")
    slide.text(640, 635, "Exact controls and results → appendix A1", 23, P, anchor="middle")
    add(slide, """The local experiment used two honest, unmodified Exo coordinator processes and a controlled publisher using the real Router. C had no master, worker, API, or election role. After A and B had converged and C's transport was connected, C sent one resource event whose claimed node ID was B's, although C's transport ID was distinct. The synthetic available RAM value was exactly 136,164,352 bytes. B happened to be the elected master in this arm. It indexed the event, emitted a matching global event with the current master origin/session, and both honest API snapshots then showed the value under B's ID. The entry was absent in both before snapshots. Peer identities and sessions stayed unchanged. The self-advertisement positive control also worked, and the no-send baseline produced no entry. This is measured acceptance of a claim about another node, not proof that a real worker's report was overwritten or that inference was affected: workers were disabled. We also have not established that this behavior crosses the project's intended authorization boundary. The finding is a concrete lead for further control-plane research.

Evidence: [report](../../results/exo-audit-20261006-005/REPORT.md), [probe envelopes](../../results/exo-audit-20261006-005/forged/probe.jsonl), [before](../../results/exo-audit-20261006-005/forged/before.json), [after](../../results/exo-audit-20261006-005/forged/after.json), [control](../../results/exo-audit-20261006-005/forged/control.json).""")

    # 7: four research questions, with measured versus untested labels.
    slide = Slide("Problems worth investigating", 7, "4 · Research questions")
    cards = [
        (76, 168, "1   Resource claims", ["Can false RAM or link reports", "change model placement?"], "Entry accepted · impact open", RED, RL),
        (660, 168, "2   Command authority", ["Can a peer trigger", "unauthorized actions?"], "Untested", P, PL),
        (76, 413, "3   Leader selection", ["Can a peer bias the election", "or keep forcing new elections?"], "Untested", P, PL),
        (660, 413, "4   Shared state", ["Can missing or old messages", "make peers disagree or stall?"], "Untested", P, PL),
    ]
    for x, y, title, body, status, color, fill in cards:
        slide.box(x, y, 542, 218, fill=fill, stroke="none")
        slide.text(x + 25, y + 48, title, 29, color, weight="bold")
        slide.lines(x + 25, y + 103, body, 27, INK, 36)
        slide.text(x + 25, y + 191, status, 21, color, weight="bold")
    add(slide, f"""These are research questions, not four validated vulnerabilities. Our first priority is to connect the measured resource-claim acceptance to a consequential decision. Use an honest baseline, fake model metadata, and a controlled changed RAM or network profile; then compare selected nodes and assignments. This can test placement without downloading weights. We still need to test competition with normal worker reports and the trust boundary.

Command authority asks which peers or clients may create, cancel, or delete work and whether those actions are bound to an authorized origin. Inspect both API admission and network command/event handling; a local open API's existence alone is not a vulnerability. Leader selection asks whether self-reported election fields, especially clock, seniority, and commands seen, can bias leadership or cause repeated elections. This requires an attacker able to reach the same cluster's messaging path; it is not an Internet-wide takeover claim. Shared state asks what happens under gaps, conflicting ordering, stale sessions, partitions, restart, and rejoin. Our exact duplicate test indexed once; the constructed session mismatch was not observed. Those controls do not establish safety for other sequences or recovery conditions. All active tests should remain in the private lab until a public operator and scope are agreed.

Sources: [placement]({SOURCE}src/exo/master/placement.py), [commands]({SOURCE}src/exo/shared/types/commands.py), [master command handling]({SOURCE}src/exo/master/main.py), [election fields]({SOURCE}src/exo/shared/election.py#L21), [replicated event handling]({SOURCE}src/exo/routing/event_router.py), [ordered buffer]({SOURCE}src/exo/utils/event_buffer.py).""")

    # 8: direct counts from preserved results, not a success-rate summary.
    slide = Slide("A1 · Exact local experiment results", 8, "Appendix · measured evidence")
    slide.text(640, 151, "One fresh trial per arm · private Docker network · seed 1901", 25, GRAY, anchor="middle")
    columns = (88, 390, 500, 720)
    for x, label in zip(columns, ("Arm", "Sends", "Global indices", "Both peers’ target entry")):
        slide.text(x, 214, label, 23, P, weight="bold")
    slide.line(80, 232, 1200, 232, LINE, 2)
    labels = {"baseline": "No-send baseline", "self": "Self claim", "forged": "Claim honest-b", "stale-session": "Session mismatch", "replay": "Exact replay"}
    for i, arm in enumerate(labels):
        y = 281 + i * 60
        published = [record for record in records[arm] if record["type"] == "published"]
        indices = results[arm]["observations"]["indexed_occurrences"]
        entry = "Absent → absent" if arm in ("baseline", "stale-session") else "Absent → 136,164,352 B"
        if arm == "forged":
            slide.rect(80, y - 32, 1120, 48, RL, radius=6)
        for x, label in zip(columns, (labels[arm], str(len(published)), ", ".join(map(str, indices)) or "None", entry)):
            slide.text(x, y, label, 23, RED if arm == "forged" else INK)
    slide.line(80, 550, 1200, 550, LINE, 1)
    slide.text(88, 590, "Peer IDs and sessions stayed unchanged in every arm.", 25, INK)
    slide.text(88, 627, "Constructed mismatch: −1 vs 0. Replay: same envelope, origin, and local index.", 23, GRAY)
    add(slide, """Each row is one fresh private arm, with two honest peers and the same third-transport role, not a deployment success rate. The no-send arm had zero publications. The self and forged arms each had one publication and one distinct observed global index. Both targets were absent before and held 136,164,352 available bytes afterward. In the forged arm the target was honest-b, which was also the master. The mismatch arm used a constructed clock of -1 while the current session clock was 0; no matching global event or target entry was observed during its bounded window. This was not a message captured from a previously active session, and there was no explicit rejection response. The replay arm recorded two byte-identical local publications, sent the second after the first indexed acknowledgement, and observed one global index. The master trace contained one matching indexing line. It did not exercise a fresh origin or a fresh local index. Separate code inspection shows that the master filters session mismatches and the ordered buffer drops an already-consumed local index. Those explanations are source inferences alongside these measurements. No models were loaded. Helper checks passed, all outcomes reproduced from saved raw records, and run containers/networks were removed while evidence and state volumes were retained.

Evidence: [summary](../../results/exo-audit-20261006-005/summary.json), [manifest and lab copies](../../results/exo-audit-20261006-005/manifest.json), [raw outcome verification](../../results/exo-audit-20261006-005/evidence-validation.json), [resource captures](../../results/exo-audit-20261006-005/resource-summary.json), [cleanup](../../results/exo-audit-20261006-005/cleanup-verification.json). Each arm directory retains probe.jsonl, before/after snapshots, consensus records, peer logs, and command records.""")

    # 9: small, inspectable source map; full paths live in linked speaker notes.
    slide = Slide("A2 · Sources and scope", 9, "Appendix · source map")
    slide.text(88, 166, "Architecture: pinned Exo source", 29, P, weight="bold")
    rows = [(223, "Discovery and roles", "discovery.rs · main.py"),
            (280, "Election and placement", "election.py · placement.py"),
            (337, "Messages and model computation", "topics.py · event_router.py · MLX engines")]
    for y, label, source in rows:
        slide.text(88, y, label, 26, INK)
        slide.text(644, y, source, 24, GRAY)
    slide.line(88, 377, 1198, 377, LINE, 1)
    slide.text(88, 425, "Measurements: local audit 005", 29, P, weight="bold")
    slide.text(88, 474, "Exact envelopes · before/after state · logs · controls", 27, INK)
    slide.text(88, 526, "Open questions: placement impact, authority, leadership, recovery", 25, GRAY)
    slide.pill(88, 589, "Source revision 21a54c5e", 324)
    slide.pill(442, 589, "Full links in speaker notes", 336)
    add(slide, f"""The deck describes the reviewed Exo implementation, not a claim about every future version or deployment. The source pin is {REVISION}. All primary source links below use that full commit. The observed result comes from the preserved 2026-10-06 local audit; no public swarm or WAN experiment was run. Proposed consequences and new attacks remain open questions.

Primary sources:

- [Exo purpose]({SOURCE}README.md)
- [Node roles]({SOURCE}src/exo/main.py)
- [IPv6 discovery]({SOURCE}rust/networking/src/discovery.rs)
- [Zenoh transport]({SOURCE}rust/networking/src/swarm.rs)
- [Election]({SOURCE}src/exo/shared/election.py)
- [Worker reports]({SOURCE}src/exo/utils/info_gatherer/info_gatherer.py)
- [Placement]({SOURCE}src/exo/master/placement.py)
- [Master processing]({SOURCE}src/exo/master/main.py)
- [Shared event handling]({SOURCE}src/exo/routing/event_router.py)
- [MLX communication]({SOURCE}src/exo/worker/engines/mlx/auto_parallel.py)
- [Local report and evidence](../../results/exo-audit-20261006-005/REPORT.md)""")
    if len(slides) != TOTAL:
        raise ValueError("Slide count differs from footer total.")
    return slides, notes


def latex_text(value: str) -> str:
    # Links are retained in Markdown notes; Beamer notes use human-readable text.
    value = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", value)
    value = value.translate(str.maketrans({"’": "'", "“": '"', "”": '"', "–": "-", "—": "-", "→": "to", "×": "x", "−": "-", "·": ";"}))
    value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode()
    mapping = {"\\": r"\textbackslash{}", "&": r"\&", "%": r"\%", "$": r"\$", "#": r"\#", "_": r"\_", "{": r"\{", "}": r"\}", "~": r"\textasciitilde{}", "^": r"\textasciicircum{}"}
    return "".join(mapping.get(character, character) for character in value)


def write_notes(slides: list[Slide], notes: list[str]) -> None:
    markdown = ["# Exo overview — speaker notes", "", "Seven main slides; two evidence/source appendices.", ""]
    tex = [r"\documentclass[aspectratio=169]{beamer}", r"\usepackage{graphicx,tikz,pgfpages}", r"\setbeamertemplate{navigation symbols}{}", r"\ifdefined\presenternotes\setbeameroption{show notes on second screen=right}\else\setbeameroption{hide notes}\fi", r"\begin{document}"]
    for slide, note in zip(slides, notes):
        markdown.extend([f"## {slide.number}. {slide.title}", "", note, ""])
        tex.extend([r"\begin{frame}[plain]", r"\begin{tikzpicture}[remember picture,overlay]\node at (current page.center) {\includegraphics[width=\paperwidth,height=\paperheight]{slides/" + f"{slide.number:02d}.pdf" + r"}};\end{tikzpicture}", r"\note{" + latex_text(note) + "}", r"\end{frame}"])
    tex.append(r"\end{document}")
    (DECK / "speaker-notes.md").write_text("\n".join(markdown) + "\n", encoding="utf-8")
    (DECK / "exo-overview.tex").write_text("\n".join(tex) + "\n", encoding="utf-8")
    (DECK / "exo-overview-notes.tex").write_text(r"\def\presenternotes{1}" + "\n" + r"\input{exo-overview.tex}" + "\n")


def make_previews(pdf: Path, slides: list[Slide]) -> None:
    previews = RENDERED / f"{STEM}-previews"
    previews.mkdir(exist_ok=True)
    subprocess.run(["pdftoppm", "-png", "-scale-to-x", "1280", "-scale-to-y", "-1", str(pdf), str(previews / "slide")], check=True, capture_output=True)
    pictures = sorted(previews.glob("slide-*.png"), key=lambda p: int(p.stem.split("-")[-1]))
    if len(pictures) != len(slides):
        raise ValueError("PDF preview count differs from slide count.")
    parts = ['<svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" width="1320" height="860">', '<rect width="1320" height="860" fill="#EEEEF4"/>']
    for i, (picture, slide) in enumerate(zip(pictures, slides)):
        x, y = 20 + (i % 3) * 435, 20 + (i // 3) * 280
        encoded = base64.b64encode(picture.read_bytes()).decode()
        parts.append(f'<image x="{x}" y="{y}" width="420" height="236.25" xlink:href="data:image/png;base64,{encoded}"/>')
        parts.append(f'<text x="{x}" y="{y + 259}" font-family="Liberation Sans, sans-serif" font-size="17" fill="{INK}">{slide.number}. {escape(slide.title)}</text>')
    parts.append("</svg>")
    sheet = previews / "contact-sheet.svg"
    sheet.write_text("\n".join(parts), encoding="utf-8")
    subprocess.run(["rsvg-convert", "-o", str(previews / "contact-sheet.png"), str(sheet)], check=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--svg-only", action="store_true", help="Write SVGs, notes and Beamer sources without PDF tools.")
    args = parser.parse_args()
    required = ("rsvg-convert", "pdfunite", "pdftoppm", "pdfinfo", "pdftotext")
    if not args.svg_only and any(shutil.which(tool) is None for tool in required):
        parser.error("PDF tools missing; use the documented renderer container or --svg-only.")
    SLIDES.mkdir(exist_ok=True)
    slides, notes = make_deck()
    write_notes(slides, notes)
    pdfs = []
    for slide in slides:
        svg = slide.save()
        if not args.svg_only:
            pdf = svg.with_suffix(".pdf")
            subprocess.run(["rsvg-convert", "-f", "pdf", "-o", str(pdf), str(svg)], check=True)
            pdfs.append(str(pdf))
    if args.svg_only:
        print(f"Wrote {len(slides)} SVGs, speaker notes, and Beamer sources to {DECK}.")
        return 0
    RENDERED.mkdir(exist_ok=True)
    combined = RENDERED / f"{STEM}.pdf"
    subprocess.run(["pdfunite", *pdfs, str(combined)], check=True)
    info = subprocess.run(["pdfinfo", str(combined)], check=True, capture_output=True, text=True).stdout
    if re.search(rf"^Pages:\s+{TOTAL}$", info, re.M) is None:
        raise ValueError("Combined PDF has an unexpected page count.")
    (RENDERED / f"{STEM}-pdfinfo.txt").write_text(info)
    subprocess.run(["pdftotext", "-bbox", str(combined), str(RENDERED / f"{STEM}-text-bounds.html")], check=True)
    make_previews(combined, slides)
    print(f"Rendered {len(slides)} slides: {combined}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
