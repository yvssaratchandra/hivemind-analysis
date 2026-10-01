"""
Figure for the five-region attacker-origin sweep (step19).

Panels:
    (a) inter-region link RTT, from RIPE Atlas ICMP medians
    (b) capture outcome by attacker origin and deadline window
    (c) where the attacker's seconds go, per origin, and how much slack remains

Plain style on purpose: one serif typeface, three matte colours, no decoration.
Everything is computed from the committed CSVs, so the figure cannot drift from
the data.

    .venv/bin/python research/plot_step19_regions.py

Needs matplotlib, which is not in the project requirements.
"""

from __future__ import annotations

import csv
import glob
import json
import re
import statistics
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.colors import LinearSegmentedColormap

RESULTS = Path(__file__).parent / "results"
PROFILE = RESULTS / "ripe_anchor_rtt_20260930.json"
CODES = ("SE", "US", "BR", "IN", "AU")
CITY = {"SE": "Stockholm", "US": "New York", "BR": "São Paulo", "IN": "Mumbai", "AU": "Sydney"}

# Matte versions of the slide deck's green and red: the deck hues blended ~40%
# toward grey, so they stay recognisable without the saturation.
GREEN = "#339e67"
RED = "#b34940"
INK = "#222222"
RULE = "#7f7f7f"

# Bars use lighter fills with the darker hue as the outline; lines in (b) keep the
# darker hue, which reads correctly at 1 px.
GREEN_FILL = "#86c4a3"
RED_FILL = "#d49b96"

HEAT = LinearSegmentedColormap.from_list("matte_red", ["#fdf4f3", "#e9c3bd", "#cf857a", RED])

# The attacker only treats an advertisement as worth undercutting if it still has
# request_timeout + safety_margin left when the read returns (step15:113-121).
VIABILITY_S = 2.5 + 1.0


def rtt_matrix() -> np.ndarray:
    profile = json.loads(PROFILE.read_text())
    values = {(e["source"], e["destination"]): statistics.median(e["rtt_ms"]) for e in profile["edges"]}
    values.update({(i["country"], i["country"]): statistics.median(i["rtt_ms"]) for i in profile["same_region"]})
    return np.array([[values[(a, b)] for b in CODES] for a in CODES])


def sweep() -> tuple[dict, dict, dict]:
    captures: dict = {}
    costs: dict = {}
    funnel: dict = {w: {"runs": 0, "viable": 0, "published": 0, "captured": 0} for w in ("long", "short")}

    for path in glob.glob(str(RESULTS / "step19_wan_delay_rounds_smoke_r5_*.csv")):
        window, region, _ = re.search(r"r5_(long|short)_([A-Z]{2})_s(\d+)", path).groups()
        for row in csv.DictReader(open(path)):
            if row["arm"] == "undercut" and row["delay_scale"] == "1.0":
                captures.setdefault((window, region), []).append(int(row["attacker_led"]))
                funnel[window]["runs"] += 1
                funnel[window]["captured"] += int(row["attacker_led"])

    for path in glob.glob(str(RESULTS / "step19_wan_delay_events_smoke_r5_*.csv")):
        window, region, _ = re.search(r"r5_(long|short)_([A-Z]{2})_s(\d+)", path).groups()
        events = [e for e in csv.DictReader(open(path)) if e["delay_scale"] == "1.0"]
        reads = [json.loads(e["detail"]) for e in events if e["kind"] == "adaptive_read"]
        if any(r["observed_candidate_count"] > 0 for r in reads):
            funnel[window]["viable"] += 1
        if any(e["kind"] == "adaptive_publication_started" for e in events):
            funnel[window]["published"] += 1
        for event in events:
            if event["kind"] == "adaptive_publication":
                detail = json.loads(event["detail"])
                costs.setdefault((window, region), []).append(
                    (detail["read_latency_s"], detail["write_latency_s"],
                     detail.get("remaining_at_write_s", 0.0))
                )
    return captures, costs, funnel


def panel_rtt(ax, rtt: np.ndarray) -> None:
    ax.imshow(rtt, cmap=HEAT, vmin=0, vmax=560)
    for i in range(5):
        for j in range(5):
            ax.text(j, i, f"{rtt[i, j]:.0f}", ha="center", va="center", fontsize=8.5,
                    color="white" if rtt[i, j] > 380 else INK)
    ax.set_xticks(range(5), [CITY[c] for c in CODES], rotation=35, ha="right", fontsize=8.5)
    ax.set_yticks(range(5), [CITY[c] for c in CODES], fontsize=8.5)
    ax.tick_params(length=0)
    for spine in ax.spines.values():
        spine.set_color(RULE)


def panel_capture(ax, captures: dict) -> None:
    y = np.arange(5)[::-1]
    height = 0.36
    for offset, window, colour, edge, label in (
        (height / 2, "long", GREEN_FILL, GREEN, "11–15 s deadline window"),
        (-height / 2, "short", RED_FILL, RED, "5–7 s deadline window"),
    ):
        values = [sum(captures.get((window, c), [])) for c in CODES]
        totals = [len(captures.get((window, c), [])) for c in CODES]
        ax.barh(y + offset, values, height=height, color=colour, edgecolor=edge,
                linewidth=0.9, label=label, zorder=2)
        for yi, value, total in zip(y + offset, values, totals):
            ax.text(value + 0.09, yi, f"{value}/{total}", va="center", fontsize=8, color=INK)

    ax.set_yticks(y, [CITY[c] for c in CODES], fontsize=8.5)
    ax.set_xlim(0, 3.75)
    ax.set_xticks(range(4))
    ax.set_xlabel("rounds led by the attacker, out of 3 seeds", fontsize=9)
    ax.tick_params(labelsize=8.5, length=0)
    ax.legend(fontsize=8, frameon=False, loc="upper center", bbox_to_anchor=(0.5, -0.22),
              ncol=1, handlelength=1.3)
    ax.grid(axis="x", color=RULE, linewidth=0.4, alpha=0.5, zorder=0)
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(RULE)


def panel_budget(ax, costs: dict) -> None:
    """Where the attacker's seconds go, per origin, and how much slack was left."""
    rows = []
    for code in ("SE", "US", "BR", "IN", "AU"):
        samples = costs.get(("long", code), [])
        if samples:
            rows.append((f"{CITY[code]}  ·  11–15 s", samples, True))
    for code in CODES:
        samples = costs.get(("short", code), [])
        if samples:
            rows.append((f"{CITY[code]}  ·  5–7 s", samples, False))

    y = np.arange(len(rows))[::-1]
    for yi, (_, samples, led) in zip(y, rows):
        read = statistics.median(r for r, _, _ in samples)
        write = statistics.median(w for _, w, _ in samples)
        left = statistics.median(rem for _, _, rem in samples)
        mid, dark, pale = (
            (GREEN_FILL, GREEN, "#e6f3ec") if led else (RED_FILL, RED, "#f9ebe9")
        )
        edge = GREEN if led else RED
        for width, start, face in ((read, 0.0, mid), (write, read, dark), (left, read + write, pale)):
            ax.barh(yi, width, left=start, height=0.52, color=face, edgecolor=edge,
                    linewidth=0.9, zorder=2)
        ax.text(read + write + left + 0.15, yi, "led the group" if led else "no peer joined",
                va="center", fontsize=8.5, color=edge)

    ax.set_yticks(y, [r[0] for r in rows], fontsize=8.5)
    ax.set_xlim(0, 12.6)
    ax.set_xlabel("seconds, from the moment the attacker starts its final DHT read", fontsize=9)
    ax.tick_params(labelsize=8.5, length=0)
    ax.grid(axis="x", color=RULE, linewidth=0.4, alpha=0.45, zorder=0)
    ax.set_axisbelow(True)

    handles = [
        plt.Rectangle((0, 0), 1, 1, facecolor=GREEN_FILL, edgecolor=GREEN, linewidth=0.9),
        plt.Rectangle((0, 0), 1, 1, facecolor=GREEN, edgecolor=GREEN, linewidth=0.9),
        plt.Rectangle((0, 0), 1, 1, facecolor="#e6f3ec", edgecolor=GREEN, linewidth=0.9),
    ]
    ax.legend(handles, ["DHT read", "DHT write", "slack before its own deadline expires"],
              fontsize=8.5, frameon=False, loc="upper center", bbox_to_anchor=(0.5, -0.20),
              ncol=3, handlelength=1.4)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(RULE)


def main() -> None:
    plt.rcParams.update({
        "font.family": "sans-serif",
        "font.sans-serif": ["Liberation Sans", "DejaVu Sans"],
        "mathtext.fontset": "dejavusans",
        "axes.linewidth": 0.7,
        "figure.facecolor": "white",
        "axes.facecolor": "white",
        "savefig.facecolor": "white",
    })

    rtt = rtt_matrix()
    captures, costs, _ = sweep()

    fig = plt.figure(figsize=(9.8, 8.0))
    grid = fig.add_gridspec(2, 2, left=0.125, right=0.975, top=0.905, bottom=0.145,
                            height_ratios=(1.0, 0.88), hspace=0.75, wspace=0.34)
    ax_rtt = fig.add_subplot(grid[0, 0])
    ax_cap = fig.add_subplot(grid[0, 1])
    ax_bud = fig.add_subplot(grid[1, :])

    panel_rtt(ax_rtt, rtt)
    panel_capture(ax_cap, captures)
    panel_budget(ax_bud, costs)

    for ax, title in zip(
        (ax_rtt, ax_cap, ax_bud),
        (
            "(a)  Link RTT between regions (ms), row to column",
            "(b)  Rounds led by the attacker, by its origin",
            "(c)  Where the attacker's time goes, and what is left over",
        ),
    ):
        ax.set_title(title, fontsize=9.5, loc="left", pad=9)

    fig.suptitle("Deadline undercutting in a 28-node Hivemind swarm across five regions",
                 fontsize=11.5, x=0.125, ha="left", y=0.966)
    fig.text(0.125, 0.018,
             "Local emulation. Delays are injected into one machine, sized from RIPE Atlas ICMP ping medians "
             "between anchor hosts (30 Sep 2026).\nNo packet loss, churn or connection setup; the attacker is "
             "given the group key. No Hivemind traffic crossed a wide-area network.",
             fontsize=7.5, color="#555555")

    for extension in ("png", "svg"):
        out = RESULTS / f"step19_region_sweep.{extension}"
        fig.savefig(out, dpi=200)
        print(f"wrote {out}")


if __name__ == "__main__":
    main()
