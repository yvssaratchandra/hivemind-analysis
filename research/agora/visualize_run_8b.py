"""Render a compact, source-labelled summary of the collected 8B data.

Usage: python research/agora/visualize_run_8b.py
Requires matplotlib; reads the adjacent JSON without modifying it.
"""

from pathlib import Path
import json

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch


ROOT = Path(__file__).resolve().parent
INPUT = ROOT / "RUN_8B_DATA_20261013.json"
OUTPUT = ROOT / "RUN_8B_OVERVIEW_20261013"
BG, INK, MUTED = "#F4F7FB", "#172B46", "#5E7088"
BLUE, TEAL, ORANGE = "#3879C7", "#138C87", "#D99532"


def panel(fig, x, y, width, height):
    fig.add_artist(FancyBboxPatch(
        (x, y), width, height, transform=fig.transFigure,
        boxstyle="round,pad=0.006,rounding_size=0.012",
        facecolor="white", edgecolor="#DFE6EF", linewidth=0.8, zorder=-1,
    ))


def chart(fig, x, y, xlim, xlabel):
    ax = fig.add_axes([x, y, 0.35, 0.125])
    ax.set_facecolor("white")
    ax.set_xlim(*xlim)
    ax.set_xlabel(xlabel, color=MUTED, fontsize=9, labelpad=3)
    ax.tick_params(axis="both", colors=MUTED, labelsize=9, length=0, pad=5)
    ax.spines[["top", "right", "left"]].set_visible(False)
    ax.spines["bottom"].set_color("#DCE4EE")
    ax.grid(axis="x", color="#E9EEF5", linewidth=0.8, zorder=0)
    return ax


def title(fig, x, y, heading, subtitle):
    fig.text(x, y, heading, color=INK, fontsize=13, weight="bold")
    fig.text(x, y - 0.03, subtitle, color=MUTED, fontsize=9.5)


def main():
    data = json.loads(INPUT.read_text())
    window = "paper_calendar_dates_utc_proxy"
    summaries = data["telemetry"]["summaries"]
    sample = lambda name: summaries[name]["by_window"][window]["all_exported_worker_labels"]
    mm, get, store, group = map(sample, ["mm_time", "dht_get_latency", "dht_store_latency", "formed_groupsize"])
    settings, paper = data["published_client_settings"], data["paper_reported_reference"]
    plt.rcParams.update({"font.family": "DejaVu Sans", "svg.fonttype": "none"})
    fig = plt.figure(figsize=(11.4, 8.2), facecolor=BG)
    fig.text(0.05, 0.958, "Agora 8B · control-plane reference", fontsize=22, weight="bold", color=INK)
    fig.text(0.05, 0.925, "Paper: 14 May–23 June 2026   |   Dashboard archive extends to 4 July", fontsize=10.5, color=MUTED)

    cards = [
        (f"{settings['matchmaking_window']['value']}s", "Matchmaking window", "Published configuration"),
        (f"{settings['averaging_timeout']['value']}s", "Averaging timeout", "Published configuration"),
        (f"{settings['matchmaking_poll_sleep_default']['value']}s", "Sleep between ad checks", "Code default; read time is additional"),
        (str(settings['averaging_cadence']['value']), "Steps between averaging", "5% of parameters per round"),
    ]
    for index, (value, label, note) in enumerate(cards):
        x = 0.05 + index * 0.23
        panel(fig, x, 0.802, 0.212, 0.093)
        fig.text(x + 0.012, 0.849, value, fontsize=25, color=BLUE, weight="bold")
        fig.text(x + 0.012, 0.825, label, fontsize=10, color=INK)
        fig.text(x + 0.012, 0.809, note, fontsize=7.6, color=MUTED)

    for x in [0.05, 0.52]:
        for y in [0.467, 0.137]:
            panel(fig, x, y, 0.43, 0.31)

    title(fig, 0.068, 0.743, "Group formation time", "Dashboard summaries · seconds")
    ax = chart(fig, 0.112, 0.552, (0, 43), "Seconds")
    values = [mm["p50"], mm["p95"]]
    ax.barh([1, 0], values, height=0.46, color=[BLUE, ORANGE], zorder=3)
    ax.set_yticks([1, 0], ["Median", "P95"])
    ax.set_ylim(-0.65, 1.6)
    for y, v in zip([1, 0], values):
        ax.text(v + 0.6, y, f"{v:.2f}s", va="center", color=INK, fontsize=10, weight="bold")
    ax.axvline(settings["matchmaking_window"]["value"], color=MUTED, linestyle=(0, (3, 3)), linewidth=1)
    ax.text(39.5, 1.6, "40s window", ha="right", color=MUTED, fontsize=8)
    fig.text(0.068, 0.486, f"{mm['n']:,} exported worker/time cells; not unique groups.", fontsize=8.4, color=MUTED)

    title(fig, 0.538, 0.743, "DHT operation latency", "Percentiles of exported interval means · milliseconds")
    ax = chart(fig, 0.575, 0.552, (0, 630), "Milliseconds")
    for index, stats in enumerate([get, store]):
        y = 1 - index
        for offset, key, color in [(0.17, "p50", BLUE), (-0.17, "p95", ORANGE)]:
            v = stats[key]
            ax.barh(y + offset, v, height=0.28, color=color, zorder=3)
            ax.text(v + 8, y + offset, f"{v:.0f}", va="center", fontsize=9, color=INK)
    ax.set_yticks([1, 0], ["Get", "Store"])
    ax.set_ylim(-0.58, 1.58)
    fig.text(0.76, 0.685, "● Median", fontsize=9, color=BLUE)
    fig.text(0.85, 0.685, "● P95", fontsize=9, color=ORANGE)
    fig.text(0.538, 0.486, "Not individual-call percentiles or ad propagation delay.", fontsize=8.4, color=MUTED)

    title(fig, 0.068, 0.413, "Members per local group", "Dashboard summaries · roster size")
    ax = chart(fig, 0.112, 0.222, (0, 21), "Peers")
    vals = [group["mean"], group["p50"], group["p95"]]
    ax.barh([2, 1, 0], vals, height=0.47, color=[TEAL, BLUE, ORANGE], zorder=3)
    ax.set_yticks([2, 1, 0], ["Mean", "Median", "P95"])
    ax.set_ylim(-0.65, 2.65)
    ax.set_xticks([0, 5, 10, 15, 20])
    for y, v in zip([2, 1, 0], vals):
        ax.text(v + 0.4, y, f"{v:.1f}" if y == 2 else f"{v:g}", va="center", fontsize=10, color=INK, weight="bold")
    fig.text(0.068, 0.156, f"Observed cell range: {group['min']:g}–{group['max']:g} peers. Same-stage groups.", fontsize=8.4, color=MUTED)

    title(fig, 0.538, 0.413, "Contributor turnover", "Paper-reported events ÷ reported 40-day duration")
    ax = chart(fig, 0.615, 0.222, (0, 21), "Events per day, averaged over the whole run")
    ax.set_position([0.615, 0.222, 0.31, 0.125])
    joins, departures = paper["join_events_per_day_over_reported_40_days"], paper["departure_events_per_day_over_reported_40_days"]
    ax.barh([1, 0], [joins, departures], height=0.46, color=[TEAL, ORANGE], zorder=3)
    ax.set_yticks([1, 0], ["Joins", "Departures"])
    ax.set_ylim(-0.65, 1.6)
    for y, v, total in [(1, joins, paper["join_events"]), (0, departures, paper["departure_events"])]:
        ax.text(v + 0.35, y, f"{v:.1f}", va="center", fontsize=10, color=INK, weight="bold")
        ax.text(0.5, y, f"{total} total", va="center", fontsize=9, color="white", weight="bold")
    fig.text(0.538, 0.156, f"{paper['distinct_contributor_nodes']} distinct contributors; admission cap {paper['concurrent_contributor_cap']} at once.", fontsize=8.4, color=MUTED)

    fig.text(0.05, 0.098, "Dashboard stats use May 14–June 23 UTC dates (41 calendar days); exact paper timestamps are unknown.", fontsize=8.5, color=MUTED)
    fig.text(0.05, 0.075, "The dashboard grid is ~3 hours. Churn bars are whole-run averages, not a stationary failure model.", fontsize=8.5, color=MUTED)
    fig.text(0.05, 0.042, "Source: RUN_8B_DATA_20261013.json · Agora paper §5 · exported API data and preserved source", fontsize=8.3, color=MUTED)

    fig.savefig(OUTPUT.with_suffix(".png"), dpi=150, facecolor=BG)
    fig.savefig(OUTPUT.with_suffix(".svg"), facecolor=BG, metadata={"Date": None})
    plt.close(fig)
    print(OUTPUT.with_suffix(".png"))
    print(OUTPUT.with_suffix(".svg"))


if __name__ == "__main__":
    main()
