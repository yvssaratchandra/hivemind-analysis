"""Track B experiment: small global malicious fraction -> large local fraction at a victim,
and what per-round group re-formation actually does about it.

Result sets:

* ``track_b_live``          -- real DHT + real ``GroupKeyManager``: how polluted does a
  victim's candidate list get, and who wins the leader race.
* ``track_b_amplification`` -- amplification (f_local / f_global) swept over attacker
  budget and bucket count.
* ``track_b_decay``         -- local fraction across re-formation rounds, for a naive
  attacker versus one that predicts the victim's next bucket.

Run:
    .venv/bin/python -m research.experiments.track_b_amplification          # simulation only
    .venv/bin/python -m research.experiments.track_b_amplification --live   # + real swarm
"""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
from typing import List

from hivemind import DHT
from hivemind.averaging.key_manager import GroupKeyManager
from hivemind.utils.timed_storage import get_dht_time
from research.attacks.sybil_matchmaking import (
    fabricate_peer_id,
    simulate_amplification,
    simulate_tracking,
    verify_prediction_matches_hivemind,
)
from research.common import plotting
from research.common.metrics import Recorder, seeded_rng, summarize

RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"
PREFIX = "run"
LOCALHOST = ["/ip4/127.0.0.1/tcp/0"]

FAKE_BUDGETS = (0, 1, 2, 5, 10, 25, 50, 100, 200, 500)
BUCKET_COUNTS = (1, 4, 16, 64)
REPEATS = 200


def run_amplification(seed: int) -> None:
    """f_local / f_global as the attacker's write budget grows, for several bucket counts."""
    recorder = Recorder(
        "track_b_amplification",
        params={"repeats": REPEATS, "seed": seed, "num_honest": 100, "attacker_processes": 1},
    )
    num_honest = 100
    series, errors = {}, {}

    for num_buckets in BUCKET_COUNTS:
        means, stds = [], []
        for budget in FAKE_BUDGETS:
            locals_ = []
            for repeat in range(REPEATS):
                rng = seeded_rng(seed, num_buckets, budget, repeat)
                result = simulate_amplification(
                    num_honest, budget, num_buckets=num_buckets, attacker_processes=1, rng=rng
                )
                locals_.append(result.local_malicious_fraction)
                recorder.record(
                    num_honest=result.num_honest,
                    num_buckets=result.num_buckets,
                    num_fake_declarations=result.num_fake_declarations,
                    attacker_processes=result.attacker_processes,
                    global_malicious_fraction=result.global_malicious_fraction,
                    local_malicious_fraction=result.local_malicious_fraction,
                    amplification=result.amplification,
                    victim_candidates=result.victim_candidates,
                    repeat=repeat,
                )
            stats = summarize(locals_)
            means.append(stats["mean"])
            stds.append(stats["std"])
        label = f"{num_buckets} bucket" + ("s" if num_buckets > 1 else "")
        series[label] = (FAKE_BUDGETS, means)
        errors[label] = stds

    global_fraction = 1 / (num_honest + 1)
    csv_path = recorder.write(RESULTS_DIR)
    print(f"wrote {csv_path}")
    print(f"  attacker is ONE process among {num_honest + 1} real peers -> f_global = {global_fraction:.4f}")
    for label, (_, means) in series.items():
        print(f"  {label:>11}: f_local at 100 fake declarations = {means[FAKE_BUDGETS.index(100)]:.3f}")

    plotting.line_plot(
        series,
        path=RESULTS_DIR / "track_b_amplification.png",
        title=f"One attacker process vs. {num_honest} honest peers",
        xlabel="fabricated declarations (one DHT write each)",
        ylabel="malicious share of victim's candidate list (f_local)",
        errorbars=errors,
        reference_line=global_fraction,
        reference_label=f"global fraction f_g = {global_fraction:.3f}",
        ylim=(0, 1.05),
    )
    print(f"wrote {RESULTS_DIR / 'track_b_amplification.png'}")


def run_decay(seed: int) -> None:
    """Local fraction over successive re-formations: naive attacker vs. tracking attacker."""
    recorder = Recorder("track_b_decay", params={"seed": seed, "repeats": REPEATS})
    rounds, num_buckets, num_honest, budget = 20, 16, 100, 50
    series, errors = {}, {}

    for tracking in (True, False):
        per_round: List[List[float]] = [[] for _ in range(rounds)]
        for repeat in range(REPEATS):
            rng = seeded_rng(seed, "decay", tracking, repeat)
            history = simulate_tracking(
                rounds,
                num_buckets=num_buckets,
                tracking=tracking,
                num_honest=num_honest,
                num_fake_declarations=budget,
                rng=rng,
            )
            for entry in history:
                per_round[int(entry["round"])].append(entry["local_malicious_fraction"])
                recorder.record(**entry, repeat=repeat)
        means = [summarize(values)["mean"] for values in per_round]
        stds = [summarize(values)["std"] for values in per_round]
        label = "tracking attacker (predicts victim's bucket)" if tracking else "naive attacker (fixed bucket)"
        series[label] = (list(range(rounds)), means)
        errors[label] = stds
        # Report the post-attack regime as a mean over rounds 1..N-1, not a single round.
        # A naive attacker's f_l is noisy after round 0: the victim re-buckets uniformly, so it
        # lands back in the attacker's bucket with probability 1/num_buckets and f_l spikes to ~0.9
        # in those rounds. Quoting one round would overstate the precision either way.
        steady = summarize(means[1:])
        print(
            f"  {label:<46} round 0: {means[0]:.3f} -> rounds 1-{rounds - 1} mean: "
            f"{steady['mean']:.3f} (min {steady['min']:.3f}, max {steady['max']:.3f})"
        )

    csv_path = recorder.write(RESULTS_DIR)
    print(f"wrote {csv_path}")

    plotting.line_plot(
        series,
        path=RESULTS_DIR / "track_b_decay.png",
        title=f"Does group re-formation shed the attacker? ({num_buckets} buckets)",
        xlabel="group re-formation round",
        ylabel="malicious share of victim's candidates (f_local)",
        errorbars=errors,
        ylim=(0, 1.05),
    )
    print(f"wrote {RESULTS_DIR / 'track_b_decay.png'}")


def run_prediction_check() -> None:
    """Confirm the tracking attack reproduces Hivemind's own re-bucketing exactly."""
    matches = total = 0
    for group_id in (b"group-a", b"group-b", b"\x01\x02\x03", b"seed-xyz"):
        for target_group_size in (4, 8, 16):
            for bits in ("0000", "101010", "01"):
                peers = [fabricate_peer_id(i, salt="member") for i in range(4)]
                for victim in peers:
                    predicted, actual = verify_prediction_matches_hivemind(
                        group_id, peers, victim, target_group_size, bits
                    )
                    total += 1
                    matches += predicted == actual
    print(f"  tracking attack reproduces GroupKeyManager.update_key_on_group_assembled: {matches}/{total}")


async def run_live(seed: int) -> None:
    """Real DHT nodes and the real GroupKeyManager: pollute a victim's candidate list."""
    recorder = Recorder("track_b_live", params={"seed": seed, "prefix": PREFIX})
    num_honest = 12

    dhts: List[DHT] = [DHT(start=True, host_maddrs=LOCALHOST)]
    initial_peers = dhts[0].get_visible_maddrs()
    dhts += [
        DHT(start=True, initial_peers=initial_peers, host_maddrs=LOCALHOST, await_ready=False)
        for _ in range(num_honest)
    ]
    for dht in dhts[1:]:
        dht.wait_until_ready()

    try:
        victim_dht, attacker_dht = dhts[0], dhts[-1]

        for budget in (0, 1, 5, 25, 100, 400):
            expiration = get_dht_time() + 120
            # Each budget gets its own group key so conditions stay independent -- declarations
            # persist until they expire, so reusing one key would let earlier budgets accumulate.
            prefix = f"{PREFIX}{budget}"
            group_key = f"{prefix}.0b"
            victim_km = GroupKeyManager(victim_dht, prefix, "", 4)
            attacker_km = GroupKeyManager(attacker_dht, prefix, "", 4)

            # honest peers announce themselves normally
            await asyncio.gather(
                *(
                    GroupKeyManager(dht, prefix, "", 4).declare_averager(group_key, dht.peer_id, expiration)
                    for dht in dhts[:-1]
                )
            )
            # the attacker writes fabricated identities, each with an earlier expiration so it
            # also wins the leader race (matchmaking.py:478 orders by expiration then peer_id)
            await asyncio.gather(
                *(
                    attacker_km.declare_averager(group_key, fabricate_peer_id(i, salt=f"b{budget}"), expiration - 60)
                    for i in range(budget)
                )
            )

            seen = await victim_km.get_averagers(group_key, only_active=True)
            honest_peer_ids = {dht.peer_id for dht in dhts[:-1]}
            fakes = sum(1 for peer_id, _ in seen if peer_id not in honest_peer_ids)
            local_fraction = fakes / len(seen) if seen else 0.0

            # who would win leadership? lowest (expiration, peer_id bytes) wins
            leader = min(seen, key=lambda item: (item[1], item[0].to_bytes())) if seen else None
            leader_is_fake = leader is not None and leader[0] not in honest_peer_ids

            # one attacker process among (num_honest + 1) real peers
            global_fraction = 1 / (num_honest + 1)
            recorder.record(
                num_honest=num_honest,
                num_fake_declarations=budget,
                attacker_processes=1,
                candidates_seen=len(seen),
                fake_candidates=fakes,
                local_malicious_fraction=local_fraction,
                global_malicious_fraction=global_fraction,
                amplification=local_fraction / global_fraction if global_fraction else 0.0,
                leader_is_attacker=leader_is_fake,
            )
            print(
                f"  {budget:>3} fabricated | victim sees {len(seen):>3} candidates, "
                f"{fakes:>3} fake | f_local {local_fraction:.3f} "
                f"(f_global {global_fraction:.3f}, x{local_fraction / global_fraction:>5.1f}) | "
                f"leader is attacker: {leader_is_fake}"
            )
    finally:
        for dht in dhts:
            dht.shutdown()

    csv_path = recorder.write(RESULTS_DIR)
    print(f"wrote {csv_path}")

    rows = recorder.rows
    plotting.line_plot(
        {
            "measured on a real swarm": (
                [r["num_fake_declarations"] for r in rows],
                [r["local_malicious_fraction"] for r in rows],
            )
        },
        path=RESULTS_DIR / "track_b_live.png",
        title=f"Live swarm: one attacker process vs. {num_honest} honest peers",
        xlabel="fabricated declarations (one DHT write each)",
        ylabel="malicious share of victim's candidate list",
        reference_line=1 / (num_honest + 1),
        reference_label="global fraction f_g",
        ylim=(0, 1.05),
    )
    print(f"wrote {RESULTS_DIR / 'track_b_live.png'}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=1234)
    parser.add_argument("--live", action="store_true", help="also run against a real swarm")
    args = parser.parse_args()

    print("== Track B: tracking attack fidelity check ==")
    run_prediction_check()

    print("\n== Track B: amplification (f_local / f_global) ==")
    run_amplification(args.seed)

    print("\n== Track B: decay across group re-formation ==")
    run_decay(args.seed)

    if args.live:
        print("\n== Track B: live swarm ==")
        asyncio.run(run_live(args.seed))


if __name__ == "__main__":
    main()
