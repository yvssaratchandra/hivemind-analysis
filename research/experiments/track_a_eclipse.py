"""Track A experiment: what does it cost to eclipse a Hivemind DHT record?

Produces three result sets:

* ``track_a_analytical`` -- capture vs. number of crafted identities, swept across
  swarm sizes up to 10^4 peers (pure XOR math, no processes).
* ``track_a_cost``       -- the headline cost curve: identities needed for a *total*
  eclipse as a function of swarm size.
* ``track_a_live``       -- a real Hivemind swarm confirming the theory, including
  where an honest peer's data physically lands.

Run:
    .venv/bin/python -m research.experiments.track_a_eclipse            # analytical only (fast)
    .venv/bin/python -m research.experiments.track_a_eclipse --live     # + live swarm (slow, RAM-heavy)
"""

from __future__ import annotations

import argparse
import asyncio
import random
from pathlib import Path

from research.attacks.eclipse_key import (
    analytical_capture,
    censorship_test,
    live_capture,
    nearest_honest_distance,
    sybils_needed_for_full_capture,
)
from research.common import plotting
from research.common.metrics import Recorder, summarize

RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"
KEY = "hivemind_run_alpha.all_averagers"  # shaped like a real Hivemind key

ANALYTICAL_SWARM_SIZES = (50, 100, 200, 500, 1000, 2000, 5000, 10000)
SYBIL_COUNTS = (0, 1, 2, 3, 4, 5, 6, 8, 12, 16)
NUM_REPLICAS = 5
REPEATS = 40


def run_analytical(seed: int) -> None:
    """Capture vs. #identities, across swarm sizes. Repeats average over the random
    draw of honest node_ids (the only stochastic element)."""
    recorder = Recorder(
        "track_a_analytical",
        params={"key": KEY, "num_replicas": NUM_REPLICAS, "repeats": REPEATS, "seed": seed},
    )
    series, errors = {}, {}

    for swarm_size in ANALYTICAL_SWARM_SIZES:
        means, stds = [], []
        for num_sybils in SYBIL_COUNTS:
            captures = []
            for repeat in range(REPEATS):
                rng = random.Random((seed, swarm_size, num_sybils, repeat).__hash__())
                result = analytical_capture(KEY, swarm_size, num_sybils, num_replicas=NUM_REPLICAS, rng=rng)
                captures.append(result.structural_capture)
                recorder.record(**result.as_row(repeat=repeat, layer="analytical"))
            stats = summarize(captures)
            means.append(stats["mean"])
            stds.append(stats["std"])
        series[f"{swarm_size} honest peers"] = (SYBIL_COUNTS, means)
        errors[f"{swarm_size} honest peers"] = stds

    csv_path = recorder.write(RESULTS_DIR)
    print(f"wrote {csv_path}")

    plotting.line_plot(
        series,
        path=RESULTS_DIR / "track_a_capture_vs_sybils.png",
        title=f"Eclipse of one DHT record (num_replicas={NUM_REPLICAS})",
        xlabel="number of attacker identities (free under the threat model)",
        ylabel="share of replica set controlled",
        errorbars=errors,
        reference_line=1.0,
        reference_label="total eclipse",
        ylim=(0, 1.05),
    )
    print(f"wrote {RESULTS_DIR / 'track_a_capture_vs_sybils.png'}")


def run_cost_curve(seed: int) -> None:
    """How many identities does a *total* eclipse need, as the swarm grows?

    The striking result is that this barely grows: the attacker only has to beat the
    nearest few honest peers, and honest peers are uniformly scattered over a 160-bit
    space, so they are never close to any particular key.
    """
    recorder = Recorder(
        "track_a_cost",
        params={"key": KEY, "num_replicas": NUM_REPLICAS, "repeats": REPEATS, "seed": seed},
    )
    xs, means, stds = [], [], []

    for swarm_size in ANALYTICAL_SWARM_SIZES:
        needed, distances = [], []
        for repeat in range(REPEATS):
            rng = random.Random((seed, "cost", swarm_size, repeat).__hash__())
            count = sybils_needed_for_full_capture(KEY, swarm_size, num_replicas=NUM_REPLICAS, rng=rng)
            rng2 = random.Random((seed, "cost", swarm_size, repeat).__hash__())
            distance = nearest_honest_distance(KEY, swarm_size, rng=rng2)
            needed.append(count)
            distances.append(distance)
            recorder.record(
                num_honest=swarm_size,
                repeat=repeat,
                sybils_for_total_eclipse=count,
                nearest_honest_xor_distance=distance,
                num_replicas=NUM_REPLICAS,
            )
        stats = summarize([float(value) for value in needed])
        xs.append(swarm_size)
        means.append(stats["mean"])
        stds.append(stats["std"])
        print(
            f"  {swarm_size:>6} honest peers -> {stats['mean']:.2f} +/- {stats['std']:.2f} identities "
            f"for a total eclipse (nearest honest peer sits at XOR ~{sum(distances) / len(distances):.2e})"
        )

    csv_path = recorder.write(RESULTS_DIR)
    print(f"wrote {csv_path}")

    plotting.line_plot(
        {"identities needed for total eclipse": (xs, means)},
        path=RESULTS_DIR / "track_a_cost_vs_swarm_size.png",
        title="Attack cost barely grows with swarm size",
        xlabel="honest swarm size",
        ylabel=f"identities needed to own all {NUM_REPLICAS} replicas",
        errorbars={"identities needed for total eclipse": stds},
        logx=True,
    )
    print(f"wrote {RESULTS_DIR / 'track_a_cost_vs_swarm_size.png'}")


async def run_live(seed: int) -> None:
    """Live confirmation on a real swarm. Capped at 60 honest peers: each peer runs
    its own p2pd (~57 MB), so this is the RAM-bound layer."""
    recorder = Recorder("track_a_live", params={"key": KEY, "num_replicas": NUM_REPLICAS, "seed": seed})

    conditions = [(40, count) for count in (0, 1, 3, 5, 8)]
    for num_honest, num_sybils in conditions:
        result = await live_capture(KEY, num_honest, num_sybils, num_replicas=NUM_REPLICAS)
        recorder.record(**result.as_row(layer="live"))
        print(
            f"  {num_honest} honest + {num_sybils:>2} sybils | "
            f"structural {result.structural_capture:.2f} | "
            f"lookup {result.lookup_capture if result.lookup_capture is None else f'{result.lookup_capture:.2f}'} | "
            f"store {result.store_capture if result.store_capture is None else f'{result.store_capture:.2f}'} | "
            f"honest read ok: {result.honest_read_ok} | build {result.build_seconds:.1f}s"
        )

    csv_path = recorder.write(RESULTS_DIR)
    print(f"wrote {csv_path}")

    rows = recorder.rows
    xs = [row["num_sybils"] for row in rows]
    plotting.line_plot(
        {
            "structural (theory)": (xs, [row["structural_capture"] for row in rows]),
            "live lookup": (xs, [row["lookup_capture"] or 0.0 for row in rows]),
            "actual store placement": (xs, [row["store_capture"] or 0.0 for row in rows]),
        },
        path=RESULTS_DIR / "track_a_live_capture.png",
        title=f"Live swarm: theory vs. reality ({conditions[0][0]} honest peers)",
        xlabel="number of attacker identities",
        ylabel="share controlled by attacker",
        reference_line=1.0,
        reference_label="total eclipse",
        ylim=(0, 1.05),
    )
    print(f"wrote {RESULTS_DIR / 'track_a_live_capture.png'}")


async def run_censorship(seed: int) -> None:
    """Does eclipse custody translate into actual censorship -- and is it really the
    self-chosen node_id that causes it?

    Each condition is run with crafted node_ids and again with random ones. Identical
    attacker budget; only address-choice differs.

    Also split by DHT caching. Hivemind opportunistically caches records
    (``cache_locally``, ``cache_nearest=1``, ``cache_on_store``); that turns out to
    blunt this attack sometimes, so it is measured rather than assumed away. It is an
    incidental side effect of a performance feature, not a security control -- caches
    expire, and the attacker still holds every authoritative replica.
    """
    recorder = Recorder("track_a_censorship", params={"key": KEY, "num_replicas": NUM_REPLICAS, "seed": seed})
    num_honest, repeats, num_sybils = 30, 8, 5
    caching_arms = {
        "caching on (default)": {},
        "caching off": {"cache_locally": False, "cache_nearest": 0, "cache_on_store": False},
    }

    for cache_label, cache_kwargs in caching_arms.items():
        for crafted in (True, False):
            label = "crafted node_ids" if crafted else "random node_ids (control)"
            for repeat in range(repeats):
                rng = random.Random((seed, "censor", cache_label, crafted, repeat).__hash__())
                row = await censorship_test(
                    KEY,
                    num_honest,
                    num_sybils,
                    crafted=crafted,
                    num_replicas=NUM_REPLICAS,
                    node_kwargs=cache_kwargs,
                    rng=rng,
                )
                recorder.record(**row, repeat=repeat, caching=cache_label)
            subset = [
                row for row in recorder.rows if row["caching"] == cache_label and row["crafted_node_ids"] is crafted
            ]
            rate = sum(1 for row in subset if row["censored"]) / len(subset)
            custody = sum(row["custody_fraction"] for row in subset) / len(subset)
            print(f"  {cache_label:<22} | {label:<28} custody {custody:.2f} | censored {rate:.0%} (n={len(subset)})")

    csv_path = recorder.write(RESULTS_DIR)
    print(f"wrote {csv_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=1234)
    parser.add_argument("--live", action="store_true", help="also run the live swarm layer (slow, RAM-heavy)")
    parser.add_argument("--censorship", action="store_true", help="also run the censorship demo + control")
    args = parser.parse_args()

    print("== Track A: analytical capture sweep ==")
    run_analytical(args.seed)

    print("\n== Track A: attack cost vs swarm size ==")
    run_cost_curve(args.seed)

    if args.live:
        print("\n== Track A: live swarm confirmation ==")
        asyncio.run(run_live(args.seed))

    if args.censorship:
        print("\n== Track A: censorship, crafted vs random node_ids ==")
        asyncio.run(run_censorship(args.seed))


if __name__ == "__main__":
    main()
