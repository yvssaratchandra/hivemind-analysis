"""Plot five-region RTT inputs beside measured local deadline timelines.

The RTT heatmap shows RIPE Atlas ICMP measurements used to delay local RPCs.
The three timelines are illustrative trials, not region-matched causal estimates.
The attacker was actually placed in SE, US, and BR; IN and AU were link-delay
inputs only, so this figure does not infer DHT operation times for those origins.

Run from the repository root:
    python research/plot_step19_five_regions.py
"""

from __future__ import annotations

import csv
import json
import statistics
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.colors import LinearSegmentedColormap

HERE = Path(__file__).resolve().parent
RESULTS = HERE / "results"
PROFILE = RESULTS / "ripe_anchor_rtt_20260930.json"
LONG_EVENTS = RESULTS / "step19_wan_delay_events_smoke_20260930_long_matched_f23_r1_s1925.csv"
SHORT_EVENTS = RESULTS / "step19_wan_delay_events_confirm_20260930_short_confirm_f23_r2_s1923.csv"
STOCKHOLM_EVENTS = RESULTS / "step19_wan_delay_events_smoke_20261001_se_long_smoke_f23_r1_s1925.csv"
OUT_STEM = RESULTS / "step19_five_region_deadline_view"

REGIONS = ("SE", "US", "BR", "IN", "AU")
CITY = {
    "SE": "Stockholm",
    "US": "New York",
    "BR": "São Paulo",
    "IN": "Mumbai",
    "AU": "Sydney",
}
COLORS = {
    "poll": "#697b99",
    "read": "#3975c6",
    "write": "#e89235",
    "join": "#19986b",
    "spare": "#d6e2ee",
    "no_join": "#d66a6a",
}


def load_rtt_matrix() -> np.ndarray:
    profile = json.loads(PROFILE.read_text())
    values = {
        (edge["source"], edge["destination"]): statistics.median(edge["rtt_ms"])
        for edge in profile["edges"]
    }
    values.update(
        {
            (item["country"], item["country"]): statistics.median(item["rtt_ms"])
            for item in profile["same_region"]
        }
    )
    return np.array([[values[(source, destination)] for destination in REGIONS] for source in REGIONS])


def select_events(path: Path, *, round_id: str) -> tuple[dict, list[dict]]:
    with path.open(newline="") as handle:
        events = [
            event
            for event in csv.DictReader(handle)
            if event["round"] == round_id and event["arm"] == "undercut" and event["delay_scale"] == "1.0"
        ]
    publications = [event for event in events if event["kind"] == "adaptive_publication"]
    if len(publications) != 1:
        raise ValueError(f"expected one publication in {path.name}, round {round_id}")
    return publications[0], events


def publication_timeline(path: Path, *, round_id: str, expected_joins: int) -> list[tuple[str, float]]:
    publication, events = select_events(path, round_id=round_id)
    detail = json.loads(publication["detail"])
    reads = sorted(
        (
            event
            for event in events
            if event["kind"] == "adaptive_read"
            and event["peer"] == publication["peer"]
            and float(event["emitted_at"]) <= float(publication["emitted_at"])
        ),
        key=lambda event: float(event["emitted_at"]),
    )
    if not reads:
        raise ValueError(f"no attacker reads in {path.name}, round {round_id}")
    first_read_start = float(reads[0]["emitted_at"]) - json.loads(reads[0]["detail"])["read_latency_s"]
    final_read_start = float(reads[-1]["emitted_at"]) - json.loads(reads[-1]["detail"])["read_latency_s"]
    earlier_polls = final_read_start - first_read_start
    joins = [
        event
        for event in events
        if event["kind"] == "join_received" and event["peer"] == publication["peer"]
    ]
    if len({event["other"] for event in joins}) != expected_joins:
        raise ValueError(f"unexpected join count in {path.name}, round {round_id}")

    segments = [("poll", earlier_polls), ("read", detail["read_latency_s"]), ("write", detail["write_latency_s"])]
    if expected_joins:
        # The publication event is emitted after a verification read. Its
        # elapsed time overlaps the wait for incoming joins; reconstruct when
        # the store returned rather than appending verification as a new bar.
        store_return = float(publication["emitted_at"]) - detail["verify_latency_s"]
        last_join = max(float(event["emitted_at"]) for event in joins)
        until_join = last_join - store_return
        spare = detail["declared_expiration"] - last_join
        if until_join <= 0 or spare <= 0:
            raise ValueError(f"unexpected successful timeline in {path.name}")
        segments.extend((("join", until_join), ("spare", spare)))
    else:
        # Remaining deadline time with no incoming joins, not a single timed RPC.
        segments.append(("no_join", detail["remaining_at_write_s"]))
    return segments


def main() -> None:
    rtt = load_rtt_matrix()
    stockholm_segments = publication_timeline(STOCKHOLM_EVENTS, round_id="0", expected_joins=3)
    long_segments = publication_timeline(LONG_EVENTS, round_id="0", expected_joins=3)
    short_segments = publication_timeline(SHORT_EVENTS, round_id="1", expected_joins=0)
    cross_region = [rtt[i, j] for i in range(5) for j in range(5) if i != j]

    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 11})
    fig = plt.figure(figsize=(14.2, 10.1), facecolor="#f7f9fc")
    grid = fig.add_gridspec(
        2,
        2,
        left=0.13,
        right=0.95,
        top=0.86,
        bottom=0.255,
        width_ratios=(1.16, 0.84),
        height_ratios=(1.08, 0.92),
        hspace=0.54,
        wspace=0.34,
    )
    fig.text(0.13, 0.966, "Five-region delays in the deadline attack", fontsize=23, fontweight="bold", color="#17243a")
    fig.text(
        0.13,
        0.925,
        "RIPE Atlas ICMP RTTs supplied per-link delay to a local Hivemind swarm with 28 DHT peers.",
        fontsize=12,
        color="#50627a",
    )

    heat = fig.add_subplot(grid[0, 0])
    heat.set_facecolor("white")
    cmap = LinearSegmentedColormap.from_list("wan", ["#eaf4fb", "#9fc9e9", "#f6ca6b", "#d97669"])
    image = heat.imshow(rtt, cmap=cmap, vmin=0, vmax=520, aspect="auto")
    heat.set_title(
        "Regional link RTT (ms) · from row to column",
        loc="left",
        pad=15,
        fontweight="bold",
        color="#17243a",
    )
    heat.set_xticks(range(5), [CITY[region] for region in REGIONS], rotation=27, ha="right")
    heat.set_yticks(range(5), [CITY[region] for region in REGIONS])
    heat.tick_params(length=0)
    for row in range(5):
        for column in range(5):
            value = rtt[row, column]
            heat.text(
                column,
                row,
                f"{value:.0f}",
                ha="center",
                va="center",
                fontsize=13,
                fontweight="bold",
                color="white" if value >= 380 else "#16233a",
            )
    for spine in heat.spines.values():
        spine.set_visible(False)
    colorbar = fig.colorbar(image, ax=heat, orientation="horizontal", fraction=0.055, pad=0.24)
    colorbar.set_label("Median of three ping replies per directed pair (ms)", fontsize=10)

    notes = fig.add_subplot(grid[0, 1])
    notes.set_axis_off()
    notes.set_facecolor("#ffffff")
    notes.text(0.0, 0.96, "How to read the numbers", transform=notes.transAxes, fontsize=16, fontweight="bold", color="#17243a")
    notes.text(0.0, 0.77, f"{statistics.median(cross_region):.0f} ms", transform=notes.transAxes, fontsize=29, fontweight="bold", color="#245f9c")
    notes.text(0.0, 0.67, "median cross-region link RTT", transform=notes.transAxes, fontsize=12, color="#50627a")
    notes.text(0.0, 0.47, "99–503 ms", transform=notes.transAxes, fontsize=25, fontweight="bold", color="#245f9c")
    notes.text(0.0, 0.37, "range across the selected links", transform=notes.transAxes, fontsize=12, color="#50627a")
    notes.text(
        0.0,
        0.12,
        "These are link RTTs—not DHT operation times.\n"
        "Timed reads/writes include their DHT walk.\n"
        "Mumbai and Sydney were not attacker origins.",
        transform=notes.transAxes,
        fontsize=11,
        color="#394c65",
        va="center",
        linespacing=1.6,
    )

    timeline = fig.add_subplot(grid[1, :])
    timeline.set_facecolor("#ffffff")
    timeline.set_title("Observed timelines · honest schedule window shown at left", loc="left", pad=15, fontweight="bold", color="#17243a")
    timeline.set_xlim(0, 12)
    timeline.set_ylim(-0.6, 2.6)
    timeline.set_yticks(
        [2, 1, 0],
        ["Stockholm · 11–15 s", "New York · 11–15 s", "São Paulo · 5–7 s"],
    )
    timeline.tick_params(axis="y", length=0, pad=14)
    timeline.set_xlabel("Seconds from the start of the attacker's first DHT read", labelpad=10)
    timeline.set_xticks(range(0, 13, 2))
    timeline.grid(axis="x", color="#e4ebf4", linewidth=1)
    timeline.set_axisbelow(True)
    for spine in timeline.spines.values():
        spine.set_visible(False)

    names = {
        "poll": "earlier polls",
        "read": "final read",
        "write": "DHT write",
        "join": "to 3rd join",
        "spare": "time left",
        "no_join": "waiting: 0 joins",
    }
    for y, segments in ((2, stockholm_segments), (1, long_segments), (0, short_segments)):
        left = 0.0
        for kind, duration in segments:
            timeline.barh(y, duration, left=left, height=0.42, color=COLORS[kind], edgecolor="white", linewidth=1)
            if duration >= 0.75:
                timeline.text(
                    left + duration / 2,
                    y,
                    f"{names[kind]}\n{duration:.2f} s",
                    ha="center",
                    va="center",
                    fontsize=9.5,
                    fontweight="bold",
                    color="#203149" if kind == "spare" else "white",
                )
            left += duration
        timeline.plot([left, left], [y - 0.29, y + 0.29], color="#a62c44", linewidth=3)
        timeline.text(left, y + 0.32, "attacker expiry", ha="center", va="bottom", fontsize=10, color="#a62c44")

    fig.text(
        0.13,
        0.125,
        "Prior delayed runs: 5–7 s captured 0/3; 11–15 s captured 3/3. Stockholm smoke: long captured 1/1; short did not publish.",
        fontsize=11.5,
        fontweight="bold",
        color="#17243a",
    )
    fig.text(
        0.13,
        0.078,
        "Grey = earlier DHT reads + waits; blue/orange = complete final read/write calls, including DHT walks. Bootstrap omitted.",
        fontsize=10,
        color="#52627a",
    )
    fig.text(
        0.13,
        0.052,
        "Green/red begin when store returns and include a verification DHT read. Green ends at third join request; red had zero joins.",
        fontsize=10,
        color="#52627a",
    )

    fig.savefig(OUT_STEM.with_suffix(".png"), dpi=160, facecolor=fig.get_facecolor())
    fig.savefig(OUT_STEM.with_suffix(".svg"), facecolor=fig.get_facecolor())
    plt.close(fig)
    print(f"wrote {OUT_STEM.with_suffix('.png')} and {OUT_STEM.with_suffix('.svg')}")


if __name__ == "__main__":
    main()
