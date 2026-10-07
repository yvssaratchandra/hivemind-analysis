"""
Step 3, part 1b -- does a real swarm agree that five identities own a record?

`step3_eclipse.py` answered the arithmetic: the nearest `num_replicas` addresses to a
key hold it, an attacker can place addresses at XOR distance 0, 1, 2, ..., and honest
peers land uniformly over 160 bits, so five crafted identities own any key at any
network size. That is a claim about a sorted list of integers.

This script checks it against a live swarm: real `DHTNode`s, each with its own `p2pd`
daemon, real Kademlia routing, real `store` and `get` RPCs. Three things the
arithmetic cannot tell us:

  * where a record **actually lands** when an honest peer stores it -- routing is a
    traversal with timeouts and k-buckets, not a global sort, so the peers that end up
    holding a record need not be the theoretical nearest five;
  * whether an honest reader can **still get the value** once the attacker holds it;
  * what the attack **costs in wall clock** to stand up.

Every peer here is a bare `DHTNode` -- no `Optimizer`, no model, no averaging. That is
deliberate: the eclipse is a routing-layer attack and involves none of that, and it
keeps the swarm small enough in memory to run tens of peers.

The nodes share one event loop in this process, but each still runs its own `p2pd` and
talks over real libp2p. Sharing the loop is what lets the script read every node's
`protocol.storage` directly, which is the only honest way to see where a record went.

**Control arm, as before.** Every configuration is also run with the attacker using
random node_ids. Without it, "the attacker holds the record" says nothing.

Run:
    .venv/bin/python research/hivemind/step3_eclipse_live.py
    .venv/bin/python research/hivemind/step3_eclipse_live.py --num-honest 32 --attackers 0 1 3 5 8
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import random
import time
from pathlib import Path

from hivemind.dht.node import DHTNode
from hivemind.dht.routing import DHTID
from hivemind.utils.timed_storage import get_dht_time

RESULTS_DIR = Path(__file__).resolve().parent / "results"
LOCALHOST = ["/ip4/127.0.0.1/tcp/0"]

DEFAULT_KEY = "step2_grad_averager.0b"


def key_to_address(key: str) -> DHTID:
    """Same computation DHTNode performs internally (hivemind/dht/node.py:386)."""
    return DHTID.generate(source=key)


def crafted_ids(target: DHTID, count: int) -> list[DHTID]:
    """Addresses at XOR distance 0, 1, ... from the target -- the closest that exist."""
    return [DHTID(int(target) ^ i) for i in range(count)]


def random_ids(count: int, rng: random.Random) -> list[DHTID]:
    """The honest distribution: uniform over 160 bits."""
    return [DHTID(rng.getrandbits(8 * DHTID.HASH_NBYTES)) for _ in range(count)]


async def spawn(node_id: DHTID, initial_peers, cache: bool) -> DHTNode:
    return await DHTNode.create(
        node_id=node_id,
        initial_peers=initial_peers,
        host_maddrs=LOCALHOST,
        # Caching is separated from custody deliberately. With these on, a record is
        # also copied to the storing node and to one node found during a lookup, which
        # would inflate "who holds it" with peers that are not responsible for it.
        cache_locally=cache,
        cache_on_store=cache,
        cache_nearest=1 if cache else 0,
    )


async def run_trial(
    honest_ids: list[DHTID],
    attacker_ids: list[DHTID],
    target: DHTID,
    key: str,
    cache: bool,
    num_replicas: int,
) -> dict:
    """One swarm, one store, one read. Returns a row of measurements.

    `honest_ids` is passed in rather than drawn here so the crafted and random arms attack
    the *same* honest swarm. Otherwise the two arms differ in two things at once and the
    comparison carries nothing.
    """
    num_honest = len(honest_ids)
    bootstrap = await spawn(DHTID.generate(), None, cache)
    initial_peers = await bootstrap.get_visible_maddrs()

    honest = []
    for node_id in honest_ids:
        honest.append(await spawn(node_id, initial_peers, cache))

    # The cost figure the brief asks for: wall clock to stand the attackers up, on a
    # swarm that is already running.
    started = time.perf_counter()
    attackers = []
    for node_id in attacker_ids:
        attackers.append(await spawn(node_id, initial_peers, cache))
    build_seconds = time.perf_counter() - started

    attacker_id_set = {int(node_id) for node_id in attacker_ids}

    # What the arithmetic predicts, computed over the node_ids this swarm actually has.
    ranked = sorted(
        [(int(n) ^ int(target), int(n) in attacker_id_set) for n in honest_ids + attacker_ids]
    )
    structural = sum(is_att for _d, is_att in ranked[:num_replicas]) / num_replicas

    # An honest peer publishes an averaging-style advertisement: same key, same subkey
    # shape as GroupKeyManager.declare_averager (averaging/key_manager.py:46-67).
    writer = honest[0]
    stored_ok = await writer.store(
        key=key,
        subkey=b"advertisement",
        value=True,
        expiration_time=get_dht_time() + 600,
    )

    # Where did it land? Read every node's local storage directly. This is the
    # measurement the arithmetic cannot make.
    holders_storage, holders_cache = [], []
    for node in [bootstrap] + honest + attackers:
        is_attacker = int(node.node_id) in attacker_id_set
        if target in node.protocol.storage:
            holders_storage.append(is_attacker)
        if target in node.protocol.cache:
            holders_cache.append(is_attacker)

    store_capture = (sum(holders_storage) / len(holders_storage)) if holders_storage else 0.0

    # Can a different honest peer still read it?
    reader = honest[-1]
    found = await reader.get(key, latest=True)
    honest_read_ok = found is not None and bool(found.value)

    for node in [bootstrap] + honest + attackers:
        await node.shutdown()

    return dict(
        num_honest=num_honest,
        attacker_count=len(attacker_ids),
        global_fraction=len(attacker_ids) / (num_honest + len(attacker_ids)) if attacker_ids else 0.0,
        structural_capture=structural,
        store_capture=store_capture,
        store_holders=len(holders_storage),
        cache_holders=len(holders_cache),
        stored_ok=bool(stored_ok),
        honest_read_ok=bool(honest_read_ok),
        build_seconds=round(build_seconds, 3),
    )


async def main_async(args) -> None:
    target = key_to_address(args.key)
    print(f"key    : {args.key}")
    print(f"address: {target}")
    print(f"{args.num_honest} honest peers, caching {'on' if args.cache else 'off'}, "
          f"num_replicas {args.num_replicas}\n")

    # Two independent streams. The honest swarm is redrawn per repeat but is identical
    # across arms; the attacker's random addresses come from their own stream, so drawing
    # them cannot shift the honest swarm out from under the comparison.
    honest_rng = random.Random(args.seed)
    attacker_rng = random.Random(args.seed + 1)
    honest_per_repeat = [random_ids(args.num_honest, honest_rng) for _ in range(args.repeats)]

    rows = []
    for count in args.attackers:
        for arm in ("crafted", "random"):
            for repeat in range(args.repeats):
                attacker_ids = (
                    crafted_ids(target, count) if arm == "crafted" else random_ids(count, attacker_rng)
                )
                row = await run_trial(
                    honest_per_repeat[repeat], attacker_ids, target, args.key, args.cache, args.num_replicas
                )
                row.update(arm=arm, repeat=repeat)
                rows.append(row)
                print(f"  {arm:<8} k={count} rep={repeat}: structural {row['structural_capture']:.2f} "
                      f"store {row['store_capture']:.2f} ({row['store_holders']} holders) "
                      f"read_ok {row['honest_read_ok']} build {row['build_seconds']}s")

    write_results(rows, args.tag)
    report(rows, args)


def write_results(rows: list, tag: str) -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    path = RESULTS_DIR / f"step3_eclipse_{tag}.csv"
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(f"\nwrote {path}  ({len(rows)} rows)")


def report(rows: list, args) -> None:
    counts = sorted({row["attacker_count"] for row in rows})
    for arm in ("crafted", "random"):
        print(f"\n--- {arm} node_ids ---")
        print(f"{'k':>3} {'structural':>11} {'store':>8} {'holders':>8} {'read ok':>8} {'build s':>9}")
        for count in counts:
            cells = [r for r in rows if r["arm"] == arm and r["attacker_count"] == count]
            if not cells:
                continue
            mean = lambda field: sum(c[field] for c in cells) / len(cells)  # noqa: E731
            print(f"{count:>3} {mean('structural_capture'):>11.2f} {mean('store_capture'):>8.2f} "
                  f"{mean('store_holders'):>8.1f} "
                  f"{sum(c['honest_read_ok'] for c in cells)}/{len(cells):<6} {mean('build_seconds'):>9.2f}")

    print("\nstructural = share of the theoretical nearest-k addresses held by the attacker")
    print("store      = share of the peers that actually ended up holding the record")
    print("a gap between them means live routing did not reach the theoretical replica set")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--key", type=str, default=DEFAULT_KEY)
    parser.add_argument("--num-honest", type=int, default=24)
    parser.add_argument("--attackers", type=int, nargs="+", default=[0, 1, 3, 5, 8])
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--num-replicas", type=int, default=5)
    parser.add_argument("--cache", action=argparse.BooleanOptionalAction, default=False,
                        help="hivemind caches by default; off here so custody is measured, not copies")
    parser.add_argument("--seed", type=int, default=1234)
    parser.add_argument("--tag", type=str, default="live")
    args = parser.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
