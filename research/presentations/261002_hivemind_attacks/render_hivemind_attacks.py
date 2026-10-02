#!/usr/bin/env python3
"""Render the 2 October Hivemind talk as fixed-layout SVG and PDF slides.

Run from any directory: python research/presentations/261002_hivemind_attacks/render_hivemind_attacks.py
Requires rsvg-convert and pdfunite. The SVGs remain editable source artwork.
"""

from __future__ import annotations

import base64
import html
import re
import subprocess
import unicodedata
from pathlib import Path


ROOT = Path(__file__).resolve().parent
SLIDES = ROOT / "slides"
W, H = 1280, 720
P = "#472DBE"
PD = "#332085"
PL = "#F1EEFF"
RED = "#D52515"
RL = "#FCEEEB"
GREEN = "#078945"
GL = "#EAF8F0"
INK = "#222222"
GRAY = "#666666"
LINE = "#D7D3E8"
BG = "#FFFFFF"
TOTAL = 18


def e(s: object) -> str:
    return html.escape(str(s), quote=True)


class Slide:
    def __init__(self, title: str, number: int, section: str, *, cover: bool = False):
        self.title, self.number, self.section, self.cover = title, number, section, cover
        self.parts = [f'<svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" width="{W}" height="{H}" viewBox="0 0 {W} {H}">']
        self.rect(0, 0, W, H, P if cover else BG)
        if not cover:
            self.text(64, 94, title, 49, P, weight="bold")
            logo = ROOT / "logographics" / "Chalmers-emblem-purple.png"
            self.image(logo, 1147, 20, 83, 76)
            self.line(64, 686, 1218, 686, P, 1)
            self.text(64, 709, "2026-10-02", 18, P)
            self.text(640, 709, section, 18, P, anchor="middle")
            self.text(1218, 709, f"{number} / {TOTAL}", 18, P, anchor="end")

    def raw(self, value: str):
        self.parts.append(value)

    def rect(self, x, y, w, h, fill=BG, stroke="none", sw=1, radius=0):
        self.raw(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{radius}" fill="{fill}" stroke="{stroke}" stroke-width="{sw}"/>')

    def line(self, x1, y1, x2, y2, color=LINE, sw=2, dash=None):
        d = f' stroke-dasharray="{dash}"' if dash else ""
        self.raw(f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{color}" stroke-width="{sw}"{d}/>')

    def arrow(self, x1, y1, x2, y2, color=P, sw=3):
        self.line(x1, y1, x2, y2, color, sw)
        import math
        angle = math.atan2(y2-y1, x2-x1)
        r = 12
        a = (x2-r*math.cos(angle-0.48), y2-r*math.sin(angle-0.48))
        b = (x2-r*math.cos(angle+0.48), y2-r*math.sin(angle+0.48))
        self.raw(f'<polygon points="{x2},{y2} {a[0]:.1f},{a[1]:.1f} {b[0]:.1f},{b[1]:.1f}" fill="{color}"/>')

    def text(self, x, y, value, size=24, color=INK, *, weight="normal", anchor="start"):
        self.raw(f'<text x="{x}" y="{y}" fill="{color}" font-family="Liberation Sans, Arial, sans-serif" font-size="{size}" font-weight="{weight}" text-anchor="{anchor}">{e(value)}</text>')

    def lines(self, x, y, values, size=23, color=INK, gap=33, *, weight="normal", anchor="start"):
        for i, value in enumerate(values):
            self.text(x, y+i*gap, value, size, color, weight=weight, anchor=anchor)

    def image(self, path: Path, x, y, w, h):
        encoded = base64.b64encode(path.read_bytes()).decode("ascii")
        self.raw(f'<image x="{x}" y="{y}" width="{w}" height="{h}" xlink:href="data:image/png;base64,{encoded}"/>')

    def pill(self, x, y, label, *, color=P, fill=PL, width=None):
        width = width or max(100, 22+len(label)*12)
        self.rect(x, y, width, 36, fill, radius=18)
        self.text(x+width/2, y+25, label, 16, color, weight="bold", anchor="middle")

    def box(self, x, y, w, h, *, fill=BG, stroke=LINE, radius=12, sw=2):
        self.rect(x, y, w, h, fill, stroke, sw, radius)

    def save(self, index: int):
        self.raw("</svg>")
        name = SLIDES / f"{index:02d}.svg"
        name.write_text("\n".join(self.parts), encoding="utf-8")
        return name


notes: list[tuple[str, str]] = []
slides: list[Slide] = []


def add(slide: Slide, note: str):
    slides.append(slide)
    notes.append((slide.title, note.strip()))


# Visual vocabulary follows the reference talk: large type, semantic diagrams,
# restrained colour, and one explanatory job per slide.
def peer(s, x, y, label, color=P, fill=PL, r=29):
    s.raw(f'<circle cx="{x}" cy="{y}" r="{r}" fill="{fill}" stroke="{color}" stroke-width="1.5"/>')
    s.text(x, y+10, label, 27, color, weight="bold", anchor="middle")


def tensor(s, x, y, values, width=54, height=51, wrong=False):
    colors = ["#E8F7FF", "#E7F7EB", "#EEE9FC", "#F5F0E7"]
    for i, val in enumerate(values):
        s.rect(x+i*(width+5), y, width, height, RL if wrong else colors[i%4], RED if wrong else "#BCB4D5", 1)
        s.text(x+i*(width+5)+width/2, y+height/2+10, val, 28, RED if wrong else INK, weight="bold", anchor="middle")


def exact_tables(s, appendix):
    s.text(1205, 654, f"Exact tables → {appendix}", 21, P, anchor="end")


def process(s, x, y, width, title, color=P):
    s.box(x, y, width, 64, fill=PL if color==P else RL, stroke="none", radius=10)
    s.text(x+width/2, y+41, title, 26, color, weight="bold", anchor="middle")


# Cover
s = Slide("Hivemind", 1, "Cover", cover=True)
s.image(ROOT / "logographics" / "Chalmers-emblem-white.png", 1120, 25, 112, 102)
s.text(64, 288, "Hivemind", 76, BG, weight="bold")
s.text(67, 352, "Three control-plane attacks", 37, BG)
s.text(67, 535, "Sarat Yellapragada", 28, BG)
s.text(67, 575, "Secura Lab", 27, BG)
s.text(67, 622, "Chalmers University of Technology   |   2 October 2026", 25, BG)

add(s, "Today I will revisit last week's open questions, then discuss three attack ideas. Forged withdrawal and deadline inflation target public availability records; the third attack assumes the adversary has already become group leader. The evidence is local or from WAN-delay emulation; there is no public live-swarm measurement yet.")

# Last week's minutes
s = Slide("Last week → today", 2, "Meeting context")
s.text(78, 179, "Last week’s questions", 28, P, weight="bold")
s.text(720, 179, "Current status", 28, P, weight="bold")
s.line(76, 199, 1205, 199, LINE, 2)
rows=[(253, ["Can we force leadership", "without knowing the group key K?"], ["Ongoing"]),
      (377, ["Crawl a live swarm every d seconds", "and record who has advertised."], ["No live averaging swarm found.", "Need a known group key to crawl."]),
      (501, ["Measure lookup time and", "the shortest advertised deadline."], ["Local tests using RIPE Atlas ping RTTs:", "Sweden, US, Brazil, India, Australia."])]
for y, question, status in rows:
    s.lines(78, y, question, 28, INK, 36)
    s.lines(720, y, status, 26, GRAY, 36)

add(s, "The minutes asked whether leadership can be made certain without knowing group key K, and proposed a live Hivemind crawler to collect advertisers, deadlines, and read latency. The search had not identified a live public averaging swarm. For this crawler, group_key is the correct term: it is the exact matchmaking advertisement key, constructed as prefix.0b<group_bits>. Joining a training run also requires its bootstrap peers, shared averaging prefix, and compatible training configuration; the group key alone is not enough. Our earlier key-targeted strategies do not establish 100% success for an unknown key. A passive observer draft exists at research/observe_dht_key.py, but it has not been run on a verified public averaging swarm. The latest timing evidence is a local 28-node test using delays derived from RIPE Atlas anchor-to-anchor ICMP ping RTTs in Sweden, the United States, Brazil, India, and Australia. The saved source profile is research/results/ripe_anchor_rtt_20260930.json. These are injected delays, not measurements of a public Hivemind network. These questions remain open. Today's three attacks are separate experiments, not a proven end-to-end chain.")

# Withdrawal mechanism: show authorship, storage, and discovery separately.
s = Slide("1. Forged withdrawal", 3, "Forged withdrawal · mechanism")
s.text(640, 151, "Known group key K", 27, GRAY, anchor="middle")
xs=[150,440,760,1100]
for x,label in zip(xs,["Honest H","Attacker A","DHT at key K","Reader V"]):
    s.text(x, 213, label, 27, RED if x==440 else P, weight="bold", anchor="middle")
    s.line(x, 235, x, 535, LINE, 2, "5 7")
s.arrow(150, 282, 752, 282, P, 3)
s.text(340, 263, "H: ready = True, expiry t", 26, P)
s.arrow(440, 374, 752, 374, RED, 3)
s.text(465, 350, "H: False, far future", 26, RED)
s.rect(749, 355, 22, 58, RL, RED, 1)
s.text(620, 415, "later expiry wins", 24, RED, anchor="middle")
s.arrow(1100, 438, 773, 438, P, 3)
s.text(930, 421, "lookup K", 24, P, anchor="middle")
s.arrow(773, 495, 1092, 495, P, 3)
s.text(930, 478, "H’s forged record", 26, P, anchor="middle")
s.text(1100, 544, "V filters out H", 24, P, anchor="middle")
s.text(640, 588, "H stays online, but disappears from group discovery.", 30, INK, anchor="middle")
s.text(640, 630, "H’s refresh expires sooner; the DHT keeps the record that expires later.", 26, GRAY, anchor="middle")

add(s, "Hivemind stores availability under a group key and a peer-ID subkey. An ordinary DHT peer can publish a False record under another peer's ID, with a far-future expiration. This says the peer is no longer looking for a group. Since these availability records are not owner-bound and the storage rule keeps the later expiration, the honest peer's normal shorter re-advertisement can be rejected. The effect is on discovery: a reader omits that peer. It does not prove the peer stops training. The attacker does not need to choose a DHT ID near K or control its replicas.")

# Withdrawal methodology: two separate questions, each with a concrete test.
s = Slide("Withdrawal: two separate tests", 4, "Forged withdrawal · method")
s.text(82, 190, "1   Can an attacker hide honest peers?", 31, P, weight="bold")
s.text(82, 248, "12 or 40 honest advertisers, plus one attacker.", 29, INK)
s.text(82, 294, "Check whether forged withdrawals hide peers, even after they advertise again.", 28, INK)
s.line(82, 360, 1198, 360, LINE, 1)
s.text(82, 425, "2   Does hiding a peer make the target join the attacker?", 31, P, weight="bold")
s.text(82, 483, "Use RIPE Atlas delays in a local swarm.", 29, INK)
s.text(82, 529, "Compare who the target joins, with and without a forged withdrawal.", 28, INK)

add(s, "The local mechanism test is research/step8_withdrawal.py. It compares a no-forgery control to an attack, checks visibility through a separate reader, and lets the honest peer try to publish again. A scale run tests targeted hiding among 12 and 40 honest peers. The follow-up is research/step20_wan_forged_priority.py: 28 localhost DHT nodes, 23 routing-only fillers, three honest averagers and one attacker; RTTs are injected from five geographic profiles. The attacker reads once, selects one earlier honest advertiser, publishes its own deadline one representable float later, and concurrently forges that selected peer's withdrawal. A paired control does not forge. The scored target is the latest-scheduled honest peer, identified after the run. There is one seed per region/window, so this follow-up is exploratory. No public network was involved.")

# Withdrawal result: report discovery and capture as different outcomes.
s = Slide("Withdrawal: results", 5, "Forged withdrawal · results")
s.text(96, 191, "Check", 29, P, weight="bold")
s.text(758, 191, "Observed result", 29, P, weight="bold")
s.line(90, 214, 1195, 214, LINE, 2)
results=[
    (271, "Targeted peers hidden", "39/39 (40 honest peers)"),
    (349, "Refreshes that restored the peer", "0/30 attempts"),
    (447, "Target joined attacker, with withdrawal", "0/10 trials"),
    (525, "Target joined attacker, without withdrawal", "0/10 trials"),
]
for y, label, result in results:
    s.text(96, y, label, 28, INK)
    s.text(758, y, result, 28, INK)
s.line(90, 391, 1195, 391, LINE, 1)
s.text(640, 606, "Group capture was tested locally with delays derived from RIPE Atlas pings.", 25, GRAY, anchor="middle")
exact_tables(s, "A1")

add(s, "The strongest result is discovery censorship. The scale test hid all 39 intended targets in a 40-peer swarm while preserving the single nontarget’s visibility. In the re-advertisement test the honest victim did not restore its record in 30 attempts. The separate WAN-delay emulation shows that the attacker did not capture the target under this reactive policy: 0 of 10 withdrawal trials, exactly as in 0 of 10 controls. All 10 attack trials accepted a forgery; the target never saw the forged state in a recorded read before its first join. All 30 honest steps in that arm completed. The result does not establish that forged withdrawal cannot ever redirect group formation. It establishes that a one-peer first-read strategy did not do so here. See Appendix A1 for local discovery and Appendix A2 for the step20 timing follow-up.")

# Inflation mechanism: the same availability field is also the priority.
s = Slide("2. Deadline inflation", 6, "Deadline inflation · mechanism")
s.text(640, 153, "Keep H visible, but move its deadline behind the attacker’s.", 28, INK, anchor="middle")
for x,label in [(91,"Before"),(746,"After forged write")]:
    s.text(x, 226, label, 29, P if x==91 else RED, weight="bold")
    s.text(x, 277, "Peer", 25, GRAY)
    s.text(x+133, 277, "Advertised deadline", 25, GRAY)
    s.line(x, 293, x+446, 293, LINE, 2)
s.rect(82, 310, 465, 67, PL, radius=7)
s.text(102, 355, "H", 31, P, weight="bold")
s.text(227, 355, "now + 5 s", 30, INK)
s.text(102, 431, "A", 31, RED, weight="bold")
s.text(227, 431, "now + 6 s", 30, INK)
s.rect(737, 310, 465, 67, RL, radius=7)
s.text(757, 355, "A", 31, RED, weight="bold")
s.text(882, 355, "now + 6 s", 30, INK)
s.text(757, 431, "H", 31, P, weight="bold")
s.text(882, 431, "now + 1 hour", 30, RED)
s.arrow(567, 365, 718, 365, RED, 3)
s.lines(642, 407, ["forge H’s", "expiration"], 24, RED, 30, anchor="middle")
s.text(315, 511, "V would contact H", 29, P, anchor="middle")
s.text(970, 511, "V may contact A", 29, RED, anchor="middle")
s.text(640, 589, "Only if V reads the changed record before choosing whom to join.", 28, INK, anchor="middle")
s.text(640, 635, "Illustrative times · H’s ready flag stays True · no crafted DHT identity", 23, GRAY, anchor="middle")

add(s, "This is a second use of the unsigned availability record. Instead of writing False, the attacker writes True under honest peer H's ID, with a far-future expiration. The record remains visible, but H appears to have a later matchmaking deadline; the attacker's own deadline is then earlier in the local ordering. The timestamps on the slide are illustrative. In the implementation, expiration is both record lifetime and leader priority. This is why a far-future expiration can both win storage and move H behind A. But the target must see the forged value before it chooses whom to contact; merely getting the write accepted is insufficient. This attack still needs the group key K to target its record, but not a crafted DHT node ID.")

# Inflation method: the key measurement crosses three actors.
s = Slide("Inflation: measure the race to V’s view", 7, "Deadline inflation · method")
s.text(640, 151, "A’s own deadline stays later than H’s; only H’s record is forged.", 27, INK, anchor="middle")
for x,label in [(190,"Attacker A"),(650,"DHT at K")]:
    s.text(x, 216, label, 29, RED if x==190 else P, weight="bold", anchor="middle")
    s.line(x, 237, x, 577, LINE, 2, "5 7")
s.arrow(190, 278, 642, 278, P, 3)
s.text(410, 263, "read candidates", 25, P, anchor="middle")
s.arrow(650, 335, 198, 335, P, 3)
s.text(410, 324, "H + deadline", 25, P, anchor="middle")
s.arrow(190, 408, 642, 408, RED, 3)
s.text(410, 388, "forge H + publish A", 25, RED, anchor="middle")
s.text(1020, 216, "Target V", 29, P, weight="bold", anchor="middle")
s.box(845, 252, 350, 292, fill="#F8F7FC", stroke="none", radius=12)
s.text(1020, 294, "Record two timestamps", 25, P, weight="bold", anchor="middle")
s.text(1020, 365, "First forged view", 27, INK, anchor="middle")
s.line(889, 399, 1151, 399, LINE, 1)
s.text(1020, 450, "First join request", 27, INK, anchor="middle")
s.arrow(658, 509, 831, 509, P, 3)
s.text(744, 486, "trace reads", 22, P, anchor="middle")
s.text(640, 611, "Compare timestamps: forged view before first join request?", 30, INK, anchor="middle")
s.text(640, 657, "28 local DHT nodes · 5 RTT origins · 5–7 s / 11–15 s windows · paired control", 23, GRAY, anchor="middle")

add(s, "This is the inflation arm of research/step20_wan_forged_priority.py. In each run the attacker waits for its first usable DHT read, selects one earlier honest advertiser H, gives its own ad a deadline one floating-point step after H, and concurrently writes a True record for H with expiration one hour later. The control arm makes no forgery. We compare the target's candidate view before first join, actual first contact, and whether it ends up with the attacker. There are five attacker regional RTT profiles, two honest deadline windows (5–7 s and 11–15 s), and one paired seed per region/window. This is local RTT emulation. The critical sequence is read, choose, write, DHT visibility to the target, then target join; measuring only write acceptance misses the race.")

# Inflation result: distinguish storage acceptance, visibility, and choice.
s = Slide("Accepted writes did not change V’s join", 8, "Deadline inflation · results")
s.text(106, 190, "Measured event", 28, P, weight="bold")
s.text(990, 190, "Attack trials", 28, P, weight="bold", anchor="middle")
s.line(100, 210, 1193, 210, LINE, 2)
for y,label,result,col in [(266,"DHT accepted the forged record","10 / 10",INK),(340,"Forged view recorded before V’s first request","0 / 10",RED),(414,"V joined the attacker","0 / 10",RED)]:
    s.text(106, y, label, 30, INK)
    s.text(990, y, result, 31, col, weight="bold", anchor="middle")
s.text(640, 467, "No-forgery control: 0 / 10 targets joined A.", 25, GRAY, anchor="middle")
s.line(100, 492, 1193, 492, LINE, 1)
s.text(103, 548, "In 7/10 trials:", 26, P, weight="bold")
s.text(339, 548, "V picked a peer", 27, INK)
s.arrow(615, 539, 711, 539, P, 2)
s.text(747, 548, "forged view appeared later", 27, INK)
s.text(640, 611, "One reactive, single-peer policy; other strategies remain untested.", 26, GRAY, anchor="middle")
exact_tables(s, "A2")

add(s, "All ten inflation attack trials got an accepted forgery. Yet none of the recorded target reads before its first join showed the intended altered state, and none of the targets joined the attacker. The paired no-forgery controls also captured zero of ten. Seven of the ten target views later showed the inflated record 0.504 to 1.579 seconds after the write, after the first join decision. Looking across both forgery arms, five of ten region/window settings targeted a peer other than the target's first choice; in the remaining settings the target selected H before the modified value arrived. The scope is a reactive strategy that forges one peer from the attacker's first usable read. Other strategies and actual public-WAN propagation remain untested.")

# Roster mechanism: each personalised roster assigns its recipient the same index.
s = Slide("3. One group, conflicting reducer indices", 9, "Post-leader attack · mechanism")
s.text(640, 150, "Precondition: A is already the group leader.", 28, RED, weight="bold", anchor="middle")
peer(s, 177, 351, "A", RED, RL, 42)
s.text(177, 419, "leader", 27, RED, anchor="middle")
s.lines(177, 476, ["Same group ID", "Same four peers"], 25, GRAY, 34, anchor="middle")
s.arrow(233, 347, 456, 347, RED, 3)
s.lines(343, 288, ["different", "ordered rosters"], 25, RED, 32, anchor="middle")
s.text(845, 218, "Reducer index", 25, GRAY, anchor="middle")
start=714
for i in range(4):
    s.text(start+i*110+45, 259, str(i), 27, P, weight="bold", anchor="middle")
rosters=[("A’s own view",["A","F1","F2","F3"]),("Sent to F1",["F1","A","F2","F3"]),("Sent to F2",["F2","F1","A","F3"]),("Sent to F3",["F3","F1","F2","A"])]
for row,(label,roster) in enumerate(rosters):
    yy=280+row*65
    s.text(689, yy+37, label, 26, INK, anchor="end")
    for i,pid in enumerate(roster):
        s.rect(start+i*110, yy, 92, 51, RL if i==0 else PL, RED if i==0 else LINE, 1)
        s.text(start+i*110+46, yy+36, pid, 27, RED if i==0 else P, weight="bold", anchor="middle")
s.text(640, 593, "Each recipient claims A’s reducer slot (index 0 in this example).", 29, INK, anchor="middle")
s.text(640, 634, "Equal-sized slices can be mixed; followers do not cross-check one common roster.", 23, GRAY, anchor="middle")

add(s, "This attack begins only after the adversary is group leader. The leader sends a separate BEGIN_ALLREDUCE message over each follower's authenticated join stream. All messages can contain the same group ID and exactly the same four peer IDs, while permuting their order. A roster position determines a reducer index for a tensor shard. Since followers do not compare a common roster commitment, they can assign the same shard to different reducers, and equal-shaped shards can be mixed without a protocol error. The pictured permutations are illustrative. This slide is explicitly conditional; the experiment deliberately arranged leadership and says nothing about WAN election probability.")

# Roster method: preserve inputs and change only the coordination message.
s = Slide("Roster test: change only the ordering", 10, "Post-leader attack · method")
s.text(202, 175, "1   Same inputs", 29, INK, weight="bold", anchor="middle")
s.text(657, 175, "2   Two conditions", 29, INK, weight="bold", anchor="middle")
s.text(1080, 175, "3   Check outputs", 29, INK, weight="bold", anchor="middle")
for i,label in enumerate(["A","F1","F2","F3"]):
    y=235+i*73
    s.text(107, y+35, label, 27, P, weight="bold", anchor="end")
    tensor(s, 125, y, [0,1,2,4], 49, 51)
s.text(246, 574, "Four real local averagers", 25, GRAY, anchor="middle")
s.arrow(368, 370, 438, 370, P, 3)
process(s, 462, 262, 382, "One common roster")
process(s, 462, 412, 382, "Different roster per peer", RED)
s.text(653, 365, "5 paired trials", 27, GRAY, anchor="middle")
s.arrow(856, 370, 916, 370, P, 3)
s.text(1079, 273, "Expected average", 26, P, weight="bold", anchor="middle")
tensor(s, 944, 300, [0,1,2,4], 56, 52)
s.lines(946, 439, ["Did it complete?", "Is the tensor correct?", "Were failures recorded?"], 26, INK, 45)
s.text(640, 634, "Real DHT + join streams + stock all-reduce · leadership arranged in advance", 24, GRAY, anchor="middle")

add(s, "Research/step17_roster_equivocation_live.py used four real local Hivemind DecentralizedAverager peers, a local DHT, authenticated matchmaking join streams, and the stock all-reduce implementation. Each peer starts with the same tensor [0,1,2,4]; a correct average must leave it unchanged. Four equal one-float shards make coordinate mixups type- and shape-valid. Five fresh paired trials compare a control with one common ordered roster to an attack where the predesignated leader sends each follower a personalized permutation of the same four unique IDs. The gathered list is reordered consistently and the group ID is unchanged. We check every output and the protocol's bans, fallback elements, errors, timeouts, and completion status. Leadership is a controlled precondition, not an observed election result.")

# Roster result: make the corruption visible, then give the paired counts.
s = Slide("Wrong tensors, successful steps", 11, "Post-leader attack · results")
s.text(84, 186, "One recorded trial", 29, P, weight="bold")
s.text(818, 186, "Across five paired trials", 29, P, weight="bold")
s.text(84, 241, "Correct average", 27, INK)
tensor(s, 84, 260, [0,1,2,4], 92, 62)
s.text(84, 382, "One attacked peer’s output", 27, INK)
tensor(s, 84, 401, [1.75,0.75,1.5,3.0], 92, 62, True)
s.line(676, 181, 676, 481, LINE, 1)
s.text(816, 252, "Corrupted trials", 26, GRAY)
s.text(816, 311, "Control", 29, INK)
s.text(1166, 311, "0 / 5", 30, INK, weight="bold", anchor="end")
s.text(816, 374, "Attack", 29, INK)
s.text(1166, 374, "5 / 5", 30, RED, weight="bold", anchor="end")
s.text(816, 447, "20/20 attacked outputs wrong", 26, RED, weight="bold")
s.line(82, 505, 1197, 505, LINE, 1)
s.text(640, 553, "All 40 steps completed; tested failure indicators stayed clear.", 29, INK, anchor="middle")
s.text(640, 605, "Tensor corruption measured locally; downstream training damage was not tested.", 24, GRAY, anchor="middle")
exact_tables(s, "A3")

add(s, "In five paired local trials, none of the common-roster controls corrupted output and all five equivocation trials did. All 20 attacked peer outputs were wrong; their maximum absolute errors were 2.25 to 3.0. All 40 averaging steps across control and attack arms completed. The tested failure indicators—reducer bans, fallback elements, RPC and all-reduce errors, timeouts, hard cancellations—stayed clear. That does not mean the attack is unobservable: the conflicting messages and outputs are observable if captured. Nor does it establish downstream optimizer behavior or lasting model damage. It establishes silent corruption of averaged tensors under an already malicious leader, through the real local protocol path. The concrete attacked tensor shown is the first recorded peer output from attack trial 0: [1.75, 0.75, 1.5, 3.0], compared with the correct [0, 1, 2, 4]. See Appendix A3 and the step17 raw CSV.")

# Attack 4 mechanism: read every advertised deadline, then declare one step earlier.
s = Slide("4. Deadline undercutting", 12, "Deadline undercutting · mechanism")
s.text(640, 151, "Read every advertised deadline, then declare one step earlier than all of them.", 27, INK, anchor="middle")
s.text(91, 221, "Advertised at K", 29, P, weight="bold")
s.text(91, 269, "Peer", 24, GRAY)
s.text(291, 269, "Deadline", 24, GRAY)
s.line(91, 285, 537, 285, LINE, 2)
for i, (name, when) in enumerate([("H1", "now + 11.4 s"), ("H2", "now + 12.8 s"), ("H3", "now + 13.8 s")]):
    s.text(111, 334 + i * 60, name, 29, P, weight="bold")
    s.text(291, 334 + i * 60, when, 28, INK)
s.rect(82, 496, 465, 62, RL, radius=7)
s.text(111, 538, "A", 29, RED, weight="bold")
s.text(291, 538, "now + 11.4 s − 1 ulp", 28, RED)
s.arrow(567, 400, 711, 400, RED, 3)
s.lines(639, 356, ["undercut the", "earliest"], 24, RED, 30, anchor="middle")
s.box(737, 262, 465, 200, fill=RL, stroke="none", radius=12)
s.text(969, 311, "Leader election", 27, RED, weight="bold", anchor="middle")
s.text(969, 369, "earliest deadline wins", 26, INK, anchor="middle")
s.text(969, 423, "A leads; A admits whom it likes", 26, RED, anchor="middle")
s.text(969, 525, "Cost to A: one identity,", 26, GRAY, anchor="middle")
s.text(969, 560, "no compute, no crafted node ID", 26, GRAY, anchor="middle")
s.text(640, 633, "One ulp is 2.4×10⁻⁷ s here · enough to win the comparison, no advertisement life lost", 23, GRAY, anchor="middle")

add(s, "This is the third use of the unsigned, self-declared deadline, and the simplest. The attacker does not forge anyone else's record: it reads the advertisements at the group key, takes the earliest deadline it can see, and declares its own one floating-point step earlier. np.nextafter moves the value by 2.4e-7 seconds at these timestamps, which wins the ordering in pop_next_leader while giving up no meaningful advertisement lifetime. Winning the election is what matters, because the leader decides admission. The cost is one identity, no computation and no crafted DHT node ID; it does require the group key. Unlike attacks 1 and 2 this needs no forgery at all, so there is no third-party record to validate.")

# Attack 4 method: isolate the attacker's origin, then isolate its own caution.
s = Slide("Undercutting: two experiments", 13, "Deadline undercutting · method")
s.text(640, 149, "Same key, node IDs and honest schedule; only the attacker's region changes.", 27, INK, anchor="middle")
process(s, 82, 196, 232, "Read ads at K")
s.arrow(322, 228, 374, 228, P, 3)
process(s, 382, 196, 232, "Undercut earliest")
s.arrow(622, 228, 674, 228, P, 3)
process(s, 682, 196, 232, "Publish own ad", RED)
s.arrow(922, 228, 974, 228, RED, 3)
process(s, 982, 196, 215, "Wait for joins", RED)
s.box(82, 300, 540, 230, fill="#F8F7FC", stroke="none", radius=12)
s.text(352, 346, "Experiment 1 · where the attacker sits", 26, P, weight="bold", anchor="middle")
s.lines(112, 400, ["5 origins: Stockholm, New York, São Paulo,", "Mumbai, Sydney · RTTs from RIPE Atlas pings",
                   "2 honest windows: 5–7 s and 11–15 s", "3 seeds each → 30 runs"], 25, INK, 38)
s.box(658, 300, 540, 230, fill=RL, stroke="none", radius=12)
s.text(928, 346, "Experiment 2 · is the failure ours?", 26, RED, weight="bold", anchor="middle")
s.lines(688, 400, ["Our attacker skips any ad with under 3.5 s left", "(request_timeout 2.5 + our own 1.0 margin)",
                   "Set both that margin and the 0.3 s confirmation", "wait to zero · Stockholm, the cheapest origin"], 25, INK, 38)
s.text(640, 589, "28 local DHT nodes, 5–6 per region · above bucket_size 20, so lookups must route", 26, INK, anchor="middle")
s.text(640, 639, "Injected delays on one machine · no loss, churn or connection setup · key known to A", 23, GRAY, anchor="middle")

add(s, "Two experiments on research/step19_wan_delay_leader.py. The first varies only where the attacker sits: within a seed the group key, DHT node IDs and the honest peers' schedule are held fixed, so the five rows differ only in the attacker's region. Five origins, two honest deadline windows and three seeds give 30 runs. The second experiment answers the obvious objection to the first. Our attacker ignores any advertisement with less than 3.5 seconds of life left when its read returns; 2.5 of that is request_timeout, a real protocol floor, but 1.0 second is padding we chose, and it waits a further 0.3 seconds to confirm a candidate before committing. Both of those are ours, so we set them to zero and re-ran from Stockholm, the cheapest origin. The swarm is 28 local DHT nodes, above the default bucket size of 20, so lookups genuinely route. Delays are injected on one machine from RIPE Atlas ICMP medians; no Hivemind traffic crossed a wide-area network.")

# Where the delays came from: the measured RTT profile behind attacks 2 and 4.
s = Slide("The delay profile: RIPE Atlas anchor RTTs", 14, "Deadline undercutting · method")
s.text(640, 148, "RIPE Atlas is a global measurement network whose anchor hosts continuously ping", 26, INK, anchor="middle")
s.text(640, 182, "each other; we took the median of three ICMP replies per directed pair.", 26, INK, anchor="middle")

rtt = [[9, 99, 209, 147, 298],
       [99, 13, 125, 285, 240],
       [209, 111, 16, 228, 305],
       [147, 240, 228, 23, 468],
       [299, 239, 305, 503, 12]]
names = ["Stockholm", "New York", "São Paulo", "Mumbai", "Sydney"]
codes = ["SE", "US", "BR", "IN", "AU"]
x0, y0, cw, ch = 243, 282, 97, 59

def heat(value):
    # white to muted red; the deck's RED at the top of the observed range
    t = min(value / 520.0, 1.0) ** 0.85
    r0, g0, b0 = 0xFF, 0xFF, 0xFF
    r1, g1, b1 = 0xD5, 0x25, 0x15
    return "#%02X%02X%02X" % (round(r0 + (r1 - r0) * t), round(g0 + (g1 - g0) * t), round(b0 + (b1 - b0) * t))

s.text(x0 - 23, 262, "from \\ to", 21, GRAY, anchor="end")
for j, code in enumerate(codes):
    s.text(x0 + j * cw + cw / 2, 262, code, 24, P, weight="bold", anchor="middle")
for i, name in enumerate(names):
    s.text(x0 - 23, y0 + i * ch + 38, name, 24, INK, anchor="end")
    for j, value in enumerate(rtt[i]):
        s.rect(x0 + j * cw, y0 + i * ch, cw - 3, ch - 3, heat(value), LINE, 1)
        s.text(x0 + j * cw + (cw - 3) / 2, y0 + i * ch + 38, value,
               24, "#FFFFFF" if value > 330 else INK, weight="bold", anchor="middle")

s.box(792, 268, 406, 230, fill="#F8F7FC", stroke="none", radius=12)
s.text(995, 316, "233 ms", 46, P, weight="bold", anchor="middle")
s.text(995, 352, "median between regions", 24, GRAY, anchor="middle")
s.line(830, 386, 1160, 386, LINE, 1)
s.text(995, 432, "99 – 503 ms", 38, P, weight="bold", anchor="middle")
s.text(995, 468, "range across the 20 links", 24, GRAY, anchor="middle")
s.text(995, 546, "Each region holds 5–6 of the", 24, INK, anchor="middle")
s.text(995, 578, "28 nodes in the swarm", 24, INK, anchor="middle")

s.text(640, 639, "Link RTTs, not DHT operation times · measured 30 Sep 2026 · profile saved as ripe_anchor_rtt_20260930.json", 22, GRAY, anchor="middle")

add(s, "RIPE Atlas is a global internet measurement network run by the RIPE NCC: thousands of small probes and a few hundred better-connected anchor hosts that continuously ping one another, with the results published openly. We took anchor-to-anchor ICMP round-trip times between the five regions on 30 September 2026 and used the median of three replies per directed pair. The matrix is slightly asymmetric because each direction was measured separately. Median between regions is 233 milliseconds, with the twenty inter-region links spanning 99 to 503 milliseconds; Sydney to Mumbai is the worst at 503. These are link round-trip times, not the cost of a DHT operation: one Hivemind read or write takes roughly five sequential round trips on top of this. The numbers feed the emulator used for both the forged-priority and the undercutting experiments, with each region holding five or six of the 28 nodes. The ping times are real measurements; the Hivemind traffic they delay is local.")

# Attack 4 result: the window decides, not the attacker's position.
s = Slide("The window decides, not the attacker", 15, "Deadline undercutting · results")
s.text(106, 186, "Attacker origin", 27, P, weight="bold")
s.text(700, 186, "11–15 s window", 27, P, weight="bold", anchor="middle")
s.text(1010, 186, "5–7 s window", 27, P, weight="bold", anchor="middle")
s.line(100, 206, 1193, 206, LINE, 2)
for i, name in enumerate(["Stockholm", "New York", "São Paulo", "Mumbai", "Sydney"]):
    y = 252 + i * 46
    s.text(106, y, name, 27, INK)
    s.text(700, y, "3 / 3", 28, GREEN, weight="bold", anchor="middle")
    s.text(1010, y, "0 / 3", 28, RED, weight="bold", anchor="middle")
s.line(100, 500, 1193, 500, LINE, 1)
s.text(106, 547, "Location changes cost, not outcome:", 26, P, weight="bold")
s.text(640, 547, "read + write 2.1 s from Stockholm, 3.7 s from Sydney", 26, INK)
s.text(106, 596, "Removing our own caution:", 26, RED, weight="bold")
s.text(474, 596, "published 1/3 → 3/3, led 0/3 → 1/3, captured 0/3 either way", 26, INK)
s.text(640, 655, "Figure: results/step19_region_sweep.png · raw runs: step19_wan_delay_*_r5_*.csv", 23, GRAY, anchor="middle")

add(s, "The attacker's location does not change whether it wins. All five origins captured three of three rounds with an 11 to 15 second honest deadline window, and zero of three with a 5 to 7 second window, which is Hivemind's shipped default. What location does change is cost: read plus write takes 2.1 seconds from Stockholm and 3.7 from Sydney, and fitting that against mean round-trip time gives roughly 4.8 sequential round trips for a read and 5.8 for a write, with near-zero intercepts, so the critical path is almost pure network time. The short window fails earlier than expected: in 14 of 15 runs the attacker never published at all, because by the time its read returns no advertisement still has 3.5 seconds of life. Since 1.0 second of that threshold was our own padding, we removed it along with the confirmation wait. The attacker then published in every run and won leadership once, but still never assembled a group, so the failure at the default is protocol timing rather than our conservatism. One caveat: this is noisy. An earlier run with the same seed and settings drew two followers and cost an honest peer its step, where the sweep run drew one and cost none.")

# Appendix A1: local discovery and refresh evidence.
s = Slide("A1. Withdrawal discovery tests", 16, "Appendix · exact results")
s.text(640, 150, "One ordinary random-ID writer · counts below matched in both repeats", 25, GRAY, anchor="middle")
cols=[85,310,534,764,1005]
for x,h in zip(cols,["Honest peers","Targets / run","Hidden / run","Others visible","Repeats"]):
    s.text(x, 216, h, 25, P, weight="bold")
s.line(80, 239, 1200, 239, LINE, 2)
import csv
row_index=0
for count,file in [(12,"step8_withdrawal_phase2.csv"),(40,"step8_withdrawal_phase2_n40.csv")]:
    with (ROOT.parent.parent/"results"/file).open(newline="") as handle:
        scale=[r for r in csv.DictReader(handle) if r["check"]=="scale"]
    targets=sorted({int(r["written"]) for r in scale})
    for target in targets:
        group=[r for r in scale if int(r["written"])==target]
        assert all(int(r["hidden"])==target and int(r["survivors"])==count-target for r in group)
        y=285+row_index*38
        values=[str(count),str(target),str(target),f"{count-target} / {count-target}",str(len(group))]
        for x,value in zip(cols,values):s.text(x,y,value,26,INK)
        row_index+=1
s.text(640, 578, "Separate refresh test: 0/30 attempts restored H in each swarm size.", 25, INK, anchor="middle")
s.text(640, 614, "Source: step8_withdrawal_phase2.csv and step8_withdrawal_phase2_n40.csv", 21, GRAY, anchor="middle")
exact_tables(s, "A2")
add(s, "These are the recorded local discovery scale checks from step8_withdrawal_phase2.csv and step8_withdrawal_phase2_n40.csv. There are two repeats for every target count. Both repeats hid all selected targets and preserved every nontarget. The 40-peer largest setting hid 39 of 39 selected targets, leaving one non-target visible. The separate 30-second refresh test in each swarm size attempted 30 honest re-advertisements; none restored the victim. The ordinary random-ID writer had no chosen placement near K. These results concern discovery, not proof that targets stopped training. For the separate WAN-delay group-capture follow-up, use Appendix A2.")

# Appendix: every RTT/window setting, not just the aggregate.
s = Slide("A2. Forged-record trials", 17, "Appendix · exact results")
s.text(640, 151, "Targets that joined A / trials · one seed per region and deadline window", 25, GRAY, anchor="middle")
cols=[82,302,523,745,960]
for x,h in zip(cols,["RTT origin","Window","Control","Withdrawal","Inflation"]):
    s.text(x, 204, h, 25, P, weight="bold")
s.line(80, 222, 1200, 222, LINE, 2)
import csv
with (ROOT.parent.parent / "results" / "step20_forged_priority_sweep_sweep_20261002_a_analysis.csv").open(newline="") as handle:
    wan = list(csv.DictReader(handle))
regions={"SE":"Sweden","US":"United States","BR":"Brazil","IN":"India","AU":"Australia"}
controls=[r for r in wan if r["arm"]=="control"]
for i,row in enumerate(controls):
    y=260+i*32
    if i==5: s.line(80, y-24, 1200, y-24, LINE, 1)
    result=[]
    for arm in ["control","withdraw","inflate"]:
        a=[r for r in wan if r["region"]==row["region"] and r["window"]==row["window"] and r["arm"]==arm]
        result.append(f'{sum(int(r["target_joined_attacker"]) for r in a)} / {len(a)}')
    for x,value in zip(cols,[regions[row["region"]],row["window"]+" s",*result]):
        s.text(x,y,value,23,INK)
s.text(640, 606, "Forgery accepted in 20/20 attack trials (29/29 writes); all 90 honest steps completed.", 23, INK, anchor="middle")
s.text(640, 650, "28-node local RTT emulation · source: step20 sweep_20261002_a analysis + event CSVs", 21, GRAY, anchor="middle")

add(s, "This table lists all ten region/window settings: five regional RTT profiles, 5–7-second and 11–15-second windows, and one seed per setting. Each cell shows target captures divided by recorded trials. The rows are generated directly from the saved analysis CSV. The attacker used one random-position DHT identity and forged one peer selected from its first usable read. The capture score is whether the retrospectively selected latest-scheduled honest target joined the attacker. 'View before join' means the recorded target lookup showed the intended changed state before first join. Across both attacks, 29 of 29 individual forged writes were accepted; some trials attempted more than one write. This is local WAN-delay emulation, not a real public network. Raw rounds and event traces are saved beside the derived analysis CSV.")

# Appendix: exact per-trial tensor correctness and error.
s = Slide("A3. Reducer-index trials", 18, "Appendix · exact results")
s.text(640, 153, "Four peers · five paired local trials · identical input [0, 1, 2, 4]", 26, GRAY, anchor="middle")
cols=[90,263,518,781,1080]
heads=["Trial","Control correct","Attack correct","Max. abs. error","Steps"]
for x,h in zip(cols,heads):
    s.text(x, 228, h, 25, P, weight="bold")
s.line(84, 249, 1205, 249, LINE, 2)
with (ROOT.parent.parent / "results" / "step17_roster_live_rounds_confirm_20260928_confirm_t5_s1717.csv").open(newline="") as handle:
    data = list(csv.DictReader(handle))
for i in range(5):
    ctrl=next(r for r in data if r["arm"]=="control" and r["trial"]==str(i))
    atk=next(r for r in data if r["arm"]=="equivocate" and r["trial"]==str(i))
    y=304+i*52
    vals=[str(i+1),f'{ctrl["correct"]} / 4',f'{atk["correct"]} / 4',f'{float(atk["max_abs_error"]):.2f}',f'{int(ctrl["completed"])+int(atk["completed"])} / 8']
    for x,value in zip(cols,vals):s.text(x,y,value,28,INK)
s.text(640, 595, "No bans, fallback elements, RPC/all-reduce errors, timeouts, or hard cancellations.", 24, INK, anchor="middle")
s.text(640, 646, "Source: step17_roster_live_rounds_confirm_20260928_confirm_t5_s1717.csv", 22, GRAY, anchor="middle")

add(s, "Each row is one fresh paired control and equivocation trial. 'Correct' counts peers whose output equals the unchanged input tensor. Maximum absolute error is computed from attacked peer output, and 'Steps' counts successful completions in both arms. Consult the raw CSV for every tensor output and associated events for the transmitted rosters. These five trials are a local confirmation, not a WAN deployment or a model-convergence study.")

def main():
    SLIDES.mkdir(exist_ok=True)
    pdfs = []
    for i, slide in enumerate(slides, 1):
        svg = slide.save(i)
        pdf = SLIDES / f"{i:02d}.pdf"
        subprocess.run(["rsvg-convert", "-f", "pdf", "-o", str(pdf), str(svg)], check=True)
        pdfs.append(str(pdf))
    combined = ROOT / "hivemind-attacks.pdf"
    subprocess.run(["pdfunite", *pdfs, str(combined)], check=True)
    # Add invisible internal links over the appendix references on result slides.
    # pdfunite emits a conventional xref table, so an incremental PDF update
    # avoids introducing a Python PDF dependency just for three annotations.
    source = combined.read_bytes()
    old_xref = int(re.findall(rb"startxref\s*(\d+)\s*%%EOF", source)[-1])
    kids = re.search(rb"/Type /Pages /Kids \[([^\]]+)\]", source)
    if kids is None:
        raise RuntimeError("Could not find the PDF page tree")
    page_ids = [int(value) for value in re.findall(rb"(\d+) 0 R", kids.group(1))]
    if len(page_ids) != len(slides):
        raise RuntimeError("PDF page tree does not match rendered slides")
    root = int(re.findall(rb"/Root (\d+) 0 R", source)[-1])
    last_object = max(int(value) for value in re.findall(rb"(?:^|\n)(\d+) 0 obj\b", source))
    targets = [(5, 16), (8, 17), (11, 18), (16, 17)]
    additions: list[tuple[int, bytes]] = []
    for offset, (from_page, to_page) in enumerate(targets, 1):
        annot_id = last_object + offset
        page_id = page_ids[from_page - 1]
        dest_id = page_ids[to_page - 1]
        match = re.search(rb"\b" + str(page_id).encode() + rb" 0 obj\s*(.*?)\s*endobj", source, re.S)
        if match is None or b"/Type /Page " not in match.group(1):
            raise RuntimeError(f"Could not find page object {page_id}")
        page = re.sub(rb">>\s*$", f"/Annots [{annot_id} 0 R] >>".encode(), match.group(1))
        if page == match.group(1):
            raise RuntimeError(f"Could not attach link to page {from_page}")
        additions.append((page_id, page))
        annot = (f"<< /Type /Annot /Subtype /Link /Rect [738 40 915 66] "
                 f"/Border [0 0 0] /Dest [{dest_id} 0 R /Fit] >>").encode()
        additions.append((annot_id, annot))
    tail = bytearray()
    xref_offsets = {}
    for number, body in additions:
        xref_offsets[number] = len(source) + len(tail)
        tail.extend(f"\n{number} 0 obj\n".encode() + body + b"\nendobj\n")
    xref_at = len(source) + len(tail)
    tail.extend(b"xref\n")
    for number in sorted(xref_offsets):
        tail.extend(f"{number} 1\n{xref_offsets[number]:010d} 00000 n\n".encode())
    tail.extend((f"trailer\n<< /Size {last_object + len(targets) + 1} "
                 f"/Root {root} 0 R /Prev {old_xref} >>\n"
                 f"startxref\n{xref_at}\n%%EOF\n").encode())
    combined.write_bytes(source + tail)
    with (ROOT / "speaker-notes.md").open("w", encoding="utf-8") as handle:
        handle.write("# Hivemind control-plane attacks — speaker notes\n\n")
        for i, (title, note) in enumerate(notes, 1):
            handle.write(f"## {i}. {title}\n\n{note}\n\n")
        handle.write("## Evidence files\n\n")
        handle.write("- `research/step8_withdrawal.py` and `research/CONTROL_DATA_AUDIT.md` (C8.T).\n")
        handle.write("- `research/step20_wan_forged_priority.py`, `research/analyze_step20.py`, and `research/results/step20_forged_priority_sweep_sweep_20261002_a_{rounds,events,analysis}.csv`.\n")
        handle.write("- `research/step17_roster_equivocation_live.py` and `research/results/step17_roster_live_{rounds,events}_confirm_20260928_confirm_t5_s1717.csv`.\n")
        handle.write("- `research/step19_wan_delay_leader.py`, `research/plot_step19_regions.py`, and `research/results/step19_wan_delay_{rounds,events}_smoke_{r5,mg}_*.csv`.\n")
        handle.write("- `research/results/ripe_anchor_rtt_20260930.json` is the saved RIPE Atlas anchor RTT profile behind slides 14 and the emulated delays.\n")
        handle.write("- `research/observe_dht_key.py` is an unrun draft, not a source of live measurements.\n")
    # Keep the traditional Beamer entry points current for Overleaf/presenter notes.
    # The fixed-layout SVG/PDF artwork is the source of truth for visual placement.
    def tex_note(value: str) -> str:
        value = value.translate(str.maketrans({"’": "'", "–": "-", "—": "-", "→": "to", "‑": "-", "×": "x"}))
        value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
        mapping = {"\\": r"\textbackslash{}", "&": r"\&", "%": r"\%", "$": r"\$", "#": r"\#", "_": r"\_", "{": r"\{", "}": r"\}"}
        return re.sub(r"[\\&%$#_{}]", lambda match: mapping[match.group()], value)

    tex = [
        "% Generated by render_hivemind_attacks.py; rerun that script after slide changes.",
        r"\documentclass[aspectratio=169]{beamer}",
        r"\usepackage{graphicx}",
        r"\usepackage{tikz}",
        r"\usepackage{pgfpages}",
        r"\setbeamertemplate{navigation symbols}{}",
        r"\ifdefined\presenternotes\setbeameroption{show notes on second screen=right}\else\setbeameroption{hide notes}\fi",
        r"\begin{document}",
    ]
    for i, (_, note) in enumerate(notes, 1):
        tex.extend([
            r"\begin{frame}[plain]",
            rf"\begin{{tikzpicture}}[remember picture,overlay]\node at (current page.center) {{\includegraphics[width=\paperwidth,height=\paperheight]{{slides/{i:02d}.pdf}}}};\end{{tikzpicture}}",
            rf"\note{{{tex_note(note)}}}",
            r"\end{frame}",
        ])
    tex.append(r"\end{document}")
    (ROOT / "hivemind-attacks.tex").write_text("\n".join(tex)+"\n", encoding="utf-8")
    (ROOT / "hivemind-attacks-revised.pdf").write_bytes(combined.read_bytes())
    print(f"Rendered {len(slides)} slides to {ROOT / 'hivemind-attacks.pdf'}")


if __name__ == "__main__":
    main()
