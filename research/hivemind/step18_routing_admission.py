"""Test routing admission at the five honest DHT peers nearest an averaging key.

All peers run Hivemind DHTNode and exchange real libp2p RPCs on localhost. This is
an admission/lookup test, not a model-training test or a claim about WAN reliability.

Smoke: .venv/bin/python research/hivemind/step18_routing_admission.py --phase smoke
Confirm: .venv/bin/python research/hivemind/step18_routing_admission.py --phase confirm
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import random
from pathlib import Path

from hivemind.dht.node import DHTNode
from hivemind.dht.routing import DHTID
from hivemind.utils.timed_storage import get_dht_time

RESULTS = Path(__file__).resolve().parent / "results"
HOST = ["/ip4/127.0.0.1/tcp/0"]
KEY = "step18_grad_averager.0b"


async def spawn(node_id: DHTID, initial_peers: list | None, bucket_size: int) -> DHTNode:
    return await DHTNode.create(
        node_id=node_id,
        initial_peers=initial_peers,
        host_maddrs=HOST,
        bucket_size=bucket_size,
        num_replicas=5,
        refresh_timeout=None,
    )


def snapshot(node: DHTNode, key_id: DHTID, attacker_ids: set[DHTID], width: int) -> dict:
    table = node.protocol.routing_table
    bucket = table.buckets[table.get_bucket_index(key_id)]
    # uid_to_peer_id also includes replacement candidates; only nodes_to_peer_id
    # entries are eligible to appear in rpc_find's nearest-neighbor response.
    active = {node_id for item in table.buckets for node_id in item.nodes_to_peer_id}
    nearest = [item for item, _ in table.get_nearest_neighbors(key_id, width)]
    return {
        "bucket_depth": bucket.depth,
        "bucket_active": len(bucket.nodes_to_peer_id),
        "bucket_replacements": len(bucket.replacement_nodes),
        "active_attacker": len(active & attacker_ids),
        "replacement_attacker": len(set(bucket.replacement_nodes) & attacker_ids),
        "nearest_attacker": len(set(nearest) & attacker_ids),
        "active_ids": active,
    }


async def lookup_neighbors(reader: DHTNode, target: DHTNode, key_id: DHTID) -> set[DHTID]:
    answer = await reader.protocol.call_find(target.peer_id, [key_id])
    if answer is None or key_id not in answer:
        raise RuntimeError("target did not answer rpc_find")
    return set(answer[key_id][1])


async def run_arm(
    *,
    arm: str,
    trial: int,
    honest_ids: list[DHTID],
    attacker_ids: list[DHTID],
    key_id: DHTID,
    bucket_size: int,
    phase: str,
    discover_targets: bool,
) -> list[dict]:
    nodes: list[DHTNode] = []
    try:
        bootstrap = await spawn(honest_ids[0], None, bucket_size)
        nodes.append(bootstrap)
        initial_peers = await bootstrap.get_visible_maddrs()
        for node_id in honest_ids[1:]:
            nodes.append(await spawn(node_id, initial_peers, bucket_size))

        honest = list(nodes)
        expected = sorted(honest, key=lambda node: int(node.node_id) ^ int(key_id))[:5]
        reader = max(honest, key=lambda node: int(node.node_id) ^ int(key_id))
        if discover_targets:
            result = await reader.find_nearest_nodes(
                [key_id], k_nearest=5, beam_size=max(bucket_size, 5), exclude_self=True
            )
            by_id = {node.node_id: node for node in honest}
            discovered_ids = list(result[key_id])
            if len(discovered_ids) != 5 or any(node_id not in by_id for node_id in discovered_ids):
                raise RuntimeError(f"scout found {len(discovered_ids)} usable honest peers, expected 5")
            targets = [by_id[node_id] for node_id in discovered_ids]
            discovery_exact = {node.node_id for node in targets} == {node.node_id for node in expected}
        else:
            targets = expected
            discovery_exact = None

        # Make the five target peers aware of the honest peers before measuring admission.
        # These are ordinary libp2p pings, and the table may still reject entries when full.
        warmup = await asyncio.gather(
            *(node.protocol.call_ping(target.peer_id) for target in targets for node in honest if node != target)
        )
        if any(result is None for result in warmup):
            raise RuntimeError("honest warmup ping failed")
        await asyncio.sleep(0.4)
        before = {target.node_id: snapshot(target, key_id, set(), bucket_size) for target in targets}

        attackers = []
        for node_id in attacker_ids:
            attacker = await spawn(node_id, initial_peers, bucket_size)
            attackers.append(attacker)
            nodes.append(attacker)
        attacker_set = set(attacker_ids)
        await asyncio.sleep(0.4)
        after_join = {
            target.node_id: snapshot(target, key_id, attacker_set, bucket_size) for target in targets
        }

        flood = await asyncio.gather(
            *(attacker.protocol.call_ping(target.peer_id) for target in targets for attacker in attackers)
        )
        if any(result is None for result in flood):
            raise RuntimeError("attacker-to-target ping failed")
        await asyncio.sleep(0.6)

        rows = []
        stored = await targets[0].store(
            key=KEY, subkey="honest", value=True, expiration_time=get_dht_time() + 60
        )
        found = await reader.get(KEY, latest=True)
        honest_read_ok = bool(stored and found is not None and found.value)
        for rank, target in enumerate(targets):
            current = snapshot(target, key_id, attacker_set, bucket_size)
            wire = await lookup_neighbors(reader, target, key_id)
            baseline = before[target.node_id]
            joined = after_join[target.node_id]
            rows.append(
                {
                    "phase": phase,
                    "arm": arm,
                    "trial": trial,
                    "bucket_size": bucket_size,
                    "honest_nodes": len(honest_ids),
                    "attacker_nodes": len(attacker_ids),
                    "target_rank": rank,
                    "target_id": int(target.node_id),
                    "target_source": "dht_lookup" if discover_targets else "test_oracle",
                    "discovery_exact": discovery_exact,
                    "baseline_bucket_active": baseline["bucket_active"],
                    "baseline_bucket_depth": baseline["bucket_depth"],
                    "join_active_attacker": joined["active_attacker"],
                    "flood_active_attacker": current["active_attacker"],
                    "flood_replacement_attacker": current["replacement_attacker"],
                    "flood_bucket_active": current["bucket_active"],
                    "flood_bucket_replacements": current["bucket_replacements"],
                    "flood_nearest_attacker": current["nearest_attacker"],
                    "wire_attacker": len(wire & attacker_set),
                    "wire_count": len(wire),
                    "honest_active_lost": len((baseline["active_ids"] & set(honest_ids)) - current["active_ids"]),
                    "all_attackers_active": attacker_set <= current["active_ids"],
                    "honest_read_ok": honest_read_ok,
                }
            )
        return rows
    finally:
        for node in reversed(nodes):
            await node.shutdown()


async def main_async(args: argparse.Namespace) -> None:
    key_id = DHTID.generate(source=KEY)
    rng = random.Random(args.seed)
    rows = []
    for trial in range(args.trials):
        honest_ids = [DHTID(rng.getrandbits(160)) for _ in range(args.honest)]
        random_attackers = [DHTID(rng.getrandbits(160)) for _ in range(args.attackers)]
        crafted_attackers = [DHTID(int(key_id) ^ index) for index in range(args.attackers)]
        for arm, attacker_ids in (("random", random_attackers), ("crafted", crafted_attackers)):
            arm_rows = await run_arm(
                arm=arm,
                trial=trial,
                honest_ids=honest_ids,
                attacker_ids=attacker_ids,
                key_id=key_id,
                bucket_size=args.bucket_size,
                phase=args.phase,
                discover_targets=args.discover_targets,
            )
            rows.extend(arm_rows)
            print(
                f"{arm} trial={trial}: "
                f"join_active={sum(row['join_active_attacker'] for row in arm_rows)}/"
                f"{5 * args.attackers}, "
                f"flood_active={sum(row['flood_active_attacker'] for row in arm_rows)}/"
                f"{5 * args.attackers}, "
                f"lookup_attacker={sum(row['wire_attacker'] for row in arm_rows)}/"
                f"{sum(row['wire_count'] for row in arm_rows)}, "
                f"honest_lost={sum(row['honest_active_lost'] for row in arm_rows)}",
                f"discovery_exact={arm_rows[0]['discovery_exact']}",
                f"read_ok={arm_rows[0]['honest_read_ok']}",
                flush=True,
            )

    RESULTS.mkdir(parents=True, exist_ok=True)
    source = "discovered" if args.discover_targets else "oracle"
    path = RESULTS / f"step18_routing_admission_{args.phase}_{source}_h{args.honest}_a{args.attackers}_b{args.bucket_size}_t{args.trials}_s{args.seed}.csv"
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {path}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=("smoke", "confirm"), default="smoke")
    parser.add_argument("--seed", type=int, default=1818)
    parser.add_argument("--trials", type=int, default=1)
    parser.add_argument("--honest", type=int, default=9)
    parser.add_argument("--attackers", type=int, default=4)
    parser.add_argument("--bucket-size", type=int, default=4)
    parser.add_argument("--discover-targets", action="store_true")
    args = parser.parse_args()
    if args.honest < 6 or args.attackers < 1 or args.bucket_size < 2:
        parser.error("need at least six honest peers, one attacker, and bucket_size >= 2")
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
