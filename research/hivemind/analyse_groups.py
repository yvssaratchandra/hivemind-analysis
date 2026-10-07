"""
Was group membership a fair draw, or did the same peers keep meeting?

`step2_groups.py` records which peers shared a group each round. That record answers a
question the convergence curve cannot: over many rounds, does a peer average with the
whole swarm, or with the same handful of partners every time?

It matters for the attacker model. Beta -- the attacker's share of one averaging group --
is assumed in the Byzantine-ML literature to be roughly alpha, the attacker's share of the
whole swarm, because group membership is taken to be a fresh random draw. If Hivemind's
own membership is not a fresh random draw even with nobody attacking, then beta is not
pinned to alpha before an attacker does anything at all.

The test is a Monte Carlo one. Under the null hypothesis membership is a uniformly random
partition of the swarm into groups of the observed size, redrawn every round. We simulate
that null many times and ask where the real run falls, on three statistics:

  * the spread (standard deviation) of how often each pair of peers shared a group,
  * the largest number of rounds any single pair spent together,
  * the number of distinct partitions seen across the run.

Note that the *mean* co-occurrence carries no information: with a fixed group size it is
determined by arithmetic, and matches the null exactly whatever the peers do. The spread
is where the signal is.

Run:
    .venv/bin/python research/hivemind/analyse_groups.py research/hivemind/results/step2_groups_16x8.csv
    .venv/bin/python research/hivemind/analyse_groups.py research/hivemind/results/step2_groups_*.csv
"""

from __future__ import annotations

import argparse
import collections
import csv
import itertools
import math
import random
from pathlib import Path


def load_partitions(path: Path) -> tuple[list[list[tuple[int, ...]]], set]:
    """Read a step2 groups CSV into one partition (list of member tuples) per round.

    The CSV holds one row per (peer, round): every member of a group reports that group
    independently, so a group of 8 appears 8 times. Collapsing on (round, group_id) first
    is what stops every pair being counted group_size times over.
    """
    rows = list(csv.DictReader(open(path)))
    groups: dict[tuple[int, str], tuple[int, ...]] = {}
    peers = set()
    for row in rows:
        members = tuple(sorted(int(m) for m in row["member_peers"].split() if m != "?"))
        groups[(int(row["round"]), row["group_id"])] = members
        peers.update(members)

    by_round = collections.defaultdict(list)
    for (round_index, _group_id), members in groups.items():
        by_round[round_index].append(members)
    return [by_round[r] for r in sorted(by_round)], peers


def co_occurrence(partitions: list[list[tuple[int, ...]]], peers: list[int]) -> list[int]:
    """How many rounds each pair of peers spent in the same group, including the zeroes."""
    together: collections.Counter = collections.Counter()
    for round_groups in partitions:
        for members in round_groups:
            for pair in itertools.combinations(members, 2):
                together[pair] += 1
    return [together.get(pair, 0) for pair in itertools.combinations(sorted(peers), 2)]


def summarise(partitions: list[list[tuple[int, ...]]], peers: list[int]) -> dict:
    counts = co_occurrence(partitions, peers)
    mean = sum(counts) / len(counts)
    return dict(
        mean=mean,
        sd=math.sqrt(sum((c - mean) ** 2 for c in counts) / len(counts)),
        low=min(counts),
        high=max(counts),
        partitions=len({tuple(sorted(round_groups)) for round_groups in partitions}),
    )


def simulate(num_peers: int, group_size: int, num_rounds: int, trials: int, seed: int) -> list[dict]:
    """The null: every round, deal the peers into groups of group_size uniformly at random."""
    rng = random.Random(seed)
    peers = list(range(num_peers))
    results = []
    for _ in range(trials):
        rounds = []
        for _ in range(num_rounds):
            rng.shuffle(peers)
            rounds.append(
                [tuple(sorted(peers[i : i + group_size])) for i in range(0, num_peers, group_size)]
            )
        results.append(summarise(rounds, peers))
    return results


def analyse(path: Path, trials: int, seed: int) -> None:
    partitions, peer_set = load_partitions(path)
    peers = sorted(peer_set)
    num_peers, num_rounds = len(peers), len(partitions)
    sizes = collections.Counter(len(m) for round_groups in partitions for m in round_groups)
    per_round = collections.Counter(len(round_groups) for round_groups in partitions)

    print(f"\n=== {path.name} ===")
    print(f"peers {num_peers}, rounds {num_rounds}, group sizes {dict(sizes)}, groups per round {dict(per_round)}")

    if len(sizes) != 1:
        print("  !! group size is not constant -- the null below assumes it is, so the p-values do not apply")
        return
    group_size = next(iter(sizes))
    if num_peers % group_size:
        print(f"  !! {num_peers} peers do not divide into groups of {group_size}; skipping the null")
        return

    observed = summarise(partitions, peers)
    null = simulate(num_peers, group_size, num_rounds, trials, seed)

    def p_value(key: str, at_least: bool) -> float:
        hits = sum((n[key] >= observed[key]) if at_least else (n[key] <= observed[key]) for n in null)
        return hits / len(null)

    null_sd = sorted(n["sd"] for n in null)
    print(f"pair co-occurrence, in rounds out of {num_rounds}:")
    print(f"  observed   mean {observed['mean']:6.2f}  sd {observed['sd']:5.2f}  min {observed['low']:3d}  max {observed['high']:3d}")
    print(f"  null       mean {null[0]['mean']:6.2f}  sd {sum(null_sd) / len(null_sd):5.2f}  (99th pct {null_sd[int(0.99 * len(null_sd))]:.2f}, max seen {null_sd[-1]:.2f})")
    print(f"distinct partitions: observed {observed['partitions']}/{num_rounds}, null mean {sum(n['partitions'] for n in null) / len(null):.1f}")
    print(f"P(null spread   >= observed) = {p_value('sd', True):.1e}")
    print(f"P(null max pair >= observed) = {p_value('high', True):.1e}")
    print(f"P(null partitions <= observed) = {p_value('partitions', False):.1e}")
    print(f"  ({trials} trials, so a p-value of 0.0e+00 means below 1/{trials})")

    together: collections.Counter = collections.Counter()
    for round_groups in partitions:
        for members in round_groups:
            for pair in itertools.combinations(members, 2):
                together[pair] += 1
    joined = ", ".join(f"{a}&{b}:{n}" for (a, b), n in together.most_common(5))
    print(f"most persistent pairs: {joined}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("paths", type=Path, nargs="+", help="step2_groups_*.csv files to analyse")
    parser.add_argument("--trials", type=int, default=20000, help="Monte Carlo draws from the null")
    parser.add_argument("--seed", type=int, default=0)
    args = parser.parse_args()

    for path in args.paths:
        analyse(path, args.trials, args.seed)


if __name__ == "__main__":
    main()
