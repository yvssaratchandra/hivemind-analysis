"""
Step 3, part 1 -- what does it cost to own a DHT record?

Hivemind decides *which peers store a key* by XOR distance: the key is hashed to a
160-bit address, every peer has a 160-bit `node_id`, and the nearest `num_replicas`
peers hold the record. So owning a record means owning the addresses next to it.

The attack is that a peer chooses its own `node_id`. `DHTNode.create` takes it as an
argument (hivemind/dht/node.py:171) and remote peers accept the self-reported value
verbatim off the wire (hivemind/dht/protocol.py:150). It is never bound to the
libp2p PeerID, which *is* cryptographically sound -- but routing never consults the
PeerID, so that soundness buys nothing here.

An attacker therefore computes the target key's address and places identities at
distance 0, 1, 2, ... from it, while honest peers land uniformly at random across
160 bits. This script measures what that asymmetry is worth.

The question is whether the cost grows with the network. If it does, the attack is a
scaling problem and large swarms are safer. If it does not, size is not a defence.

No processes and no network: this is arithmetic over node_ids, which is why it can
reach 10^4 peers. Whether a live swarm agrees is the next step, not this one.

**The control arm is the finding.** Every configuration is also run with the attacker
using *random* node_ids. An attacker holding 5 of 55 peers will own some replicas by
luck alone, so the crafted numbers mean nothing on their own -- the gap between the
two arms is what attributes the damage to address choice rather than to owning peers.

Run:
    .venv/bin/python research/hivemind/step3_eclipse.py
    .venv/bin/python research/hivemind/step3_eclipse.py --sizes 50 500 5000 --repeats 100
"""

from __future__ import annotations

import argparse
import csv
import random
import statistics
from pathlib import Path

from hivemind.dht.routing import DHTID

RESULTS_DIR = Path(__file__).resolve().parent / "results"

# The key averaging advertisements actually live under. GroupKeyManager builds it as
# f"{prefix}.0b{group_bits}" (hivemind/averaging/key_manager.py:44), the prefix is
# f"{run_id}_grad_averager" (hivemind/optim/optimizer.py:305), and group_bits is ""
# by default -- so this is the single key every peer in a step2 run advertises on.
DEFAULT_KEY = "step2_grad_averager.0b"


# --------------------------------------------------------------------------
# Addresses
# --------------------------------------------------------------------------


def key_to_address(key: str) -> DHTID:
    """The routing address responsible for a key.

    This is exactly what DHTNode does internally when storing or fetching
    (`DHTID.generate(source=key)`, hivemind/dht/node.py:386), so the attacker computes
    the same target the honest network does. Subkeys do not change it: every averager
    advertising under this key shares one address.
    """
    return DHTID.generate(source=key)


def crafted_ids(target: DHTID, count: int) -> list[DHTID]:
    """The `count` addresses closest to `target` that exist at all.

    `target ^ i` sits at XOR distance exactly `i`, so this enumerates distances
    0, 1, 2, ... -- the optimal placement, and the strongest form of the attack.
    Nothing in Hivemind prevents it, because node_ids are self-asserted.
    """
    return [DHTID(int(target) ^ i) for i in range(count)]


def random_ids(count: int, rng: random.Random) -> list[DHTID]:
    """Honest addresses: uniform over the 160-bit space.

    `DHTID.generate()` hashes random input with SHA1 (hivemind/dht/routing.py), whose
    output is uniform over 160 bits, so drawing the integer directly gives the same
    distribution far more cheaply -- which is what makes a 10^4-peer sweep free.
    """
    return [DHTID(rng.getrandbits(8 * DHTID.HASH_NBYTES)) for _ in range(count)]


# --------------------------------------------------------------------------
# The measurement
# --------------------------------------------------------------------------


def capture(honest: list[DHTID], attacker: list[DHTID], target: DHTID, num_replicas: int) -> float:
    """Share of the `num_replicas` peers responsible for `target` that the attacker holds.

    At 1.0 every replica of that record lives on attacker hardware: honest peers can
    still ask for it, but only attacker-controlled nodes can answer.

    Ties in XOR distance are impossible between distinct addresses, so no tie-break
    policy is needed -- distance to the target is injective over node_ids.
    """
    ranked = sorted(
        [(int(node_id) ^ int(target), True) for node_id in attacker]
        + [(int(node_id) ^ int(target), False) for node_id in honest]
    )
    replicas = ranked[:num_replicas]
    return sum(is_attacker for _distance, is_attacker in replicas) / len(replicas)


def nearest_honest_bits(honest: list[DHTID], target: DHTID) -> float:
    """log2 of the closest honest address's distance -- the room the attacker moves into.

    Reported because it is the whole mechanism in one number: a crafted address sits at
    distance 0-4, and this says how far away the best honest competitor is.
    """
    closest = min(int(node_id) ^ int(target) for node_id in honest)
    return closest.bit_length()


# --------------------------------------------------------------------------
# The sweep
# --------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--key", type=str, default=DEFAULT_KEY, help="DHT key to eclipse")
    parser.add_argument("--sizes", type=int, nargs="+", default=[50, 100, 500, 1000, 5000, 10000],
                        help="honest swarm sizes to sweep")
    parser.add_argument("--max-attackers", type=int, default=8)
    parser.add_argument("--repeats", type=int, default=40, help="independent honest swarms per size")
    parser.add_argument("--num-replicas", type=int, default=5,
                        help="peers responsible for a key; hivemind/dht/node.py:105 defaults to 5")
    parser.add_argument("--seed", type=int, default=1234)
    parser.add_argument("--tag", type=str, default="analytical")
    args = parser.parse_args()

    target = key_to_address(args.key)
    print(f"key    : {args.key}")
    print(f"address: {target}")
    print(f"replicas per record: {args.num_replicas}   repeats per cell: {args.repeats}\n")

    rng = random.Random(args.seed)
    rows = []
    honest_gap = []

    for size in args.sizes:
        for repeat in range(args.repeats):
            # One honest swarm, reused across attacker counts, so the arms differ only
            # in the attacker and not in the network they are attacking.
            honest = random_ids(size, rng)
            honest_gap.append((size, nearest_honest_bits(honest, target)))

            for count in range(args.max_attackers + 1):
                for arm, attacker in (
                    ("crafted", crafted_ids(target, count)),
                    ("random", random_ids(count, rng)),
                ):
                    rows.append(
                        dict(
                            swarm_size=size,
                            attacker_count=count,
                            arm=arm,
                            repeat=repeat,
                            num_replicas=args.num_replicas,
                            global_fraction=count / (size + count) if size + count else 0.0,
                            capture=capture(honest, attacker, target, args.num_replicas),
                        )
                    )

    write_results(rows, args.tag)
    report(rows, honest_gap, args)


def write_results(rows: list, tag: str) -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    path = RESULTS_DIR / f"step3_eclipse_{tag}.csv"
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["swarm_size", "attacker_count", "arm", "repeat", "num_replicas", "global_fraction", "capture"],
        )
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {path}  ({len(rows)} rows)\n")


def report(rows: list, honest_gap: list, args) -> None:
    grouped = {}
    for row in rows:
        grouped.setdefault((row["swarm_size"], row["attacker_count"], row["arm"]), []).append(row["capture"])

    sizes = sorted({row["swarm_size"] for row in rows})
    counts = sorted({row["attacker_count"] for row in rows})

    for arm in ("crafted", "random"):
        print(f"--- capture of the {args.num_replicas} replicas, {arm} node_ids ---")
        print("  swarm |" + "".join(f"{c:>8}" for c in counts) + "   <- attacker identities")
        for size in sizes:
            cells = []
            for count in counts:
                values = grouped[(size, count, arm)]
                cells.append(f"{statistics.mean(values):>8.2f}")
            print(f"{size:>7} |" + "".join(cells))
        print()

    # The claim the whole thing rests on: how much closer is a crafted address than the
    # best honest one? Crafted sits at distance 0..k-1, i.e. a handful of bits.
    print("--- room the attacker moves into ---")
    for size in sizes:
        bits = [b for s, b in honest_gap if s == size]
        print(f"{size:>7} honest peers: nearest honest address is ~2^{statistics.mean(bits):.1f} away, "
              f"crafted sits at 0..{args.max_attackers - 1}")

    # Cheapest total eclipse per arm, which is the cost figure the brief asks for.
    print("\n--- identities needed for a total eclipse (capture = 1.0 in every repeat) ---")
    for arm in ("crafted", "random"):
        for size in sizes:
            total = next(
                (c for c in counts if all(v == 1.0 for v in grouped[(size, c, arm)])),
                None,
            )
            print(f"  {arm:<8} swarm {size:>6}: {total if total is not None else f'>{max(counts)}'}")


if __name__ == "__main__":
    main()
