"""Harness smoke test: does a small swarm with injected Sybil node_ids behave as expected?

Verifies the two claims the whole project rests on, at minimum scale:
  1. ``DHTNode.create(node_id=...)`` really does let a peer choose its routing address.
  2. A handful of chosen node_ids really do capture the replica slots for a target key.

Run:  .venv/bin/python -m research.experiments.smoke_test
"""

from __future__ import annotations

import asyncio

from research.common.swarm import running_swarm
from research.common.sybil import capture_count, closest_ids, key_to_dhtid, rank_by_distance, xor_distance

TARGET_KEY = "hivemind_security_demo"
NUM_HONEST = 20
NUM_SYBILS = 5
NUM_REPLICAS = 5


async def main() -> None:
    target = key_to_dhtid(TARGET_KEY)
    print(f"target key {TARGET_KEY!r} -> DHTID {target}")

    sybil_ids = closest_ids(target, NUM_SYBILS)
    print(f"crafted {NUM_SYBILS} sybil node_ids, XOR distances: {[xor_distance(i, target) for i in sybil_ids]}")

    async with running_swarm(NUM_HONEST, sybil_ids, node_kwargs={"num_replicas": NUM_REPLICAS}) as swarm:
        print(
            f"swarm up: {len(swarm.honest)} honest + {len(swarm.sybils)} sybils, f_g={swarm.global_malicious_fraction:.3f}"
        )

        # (1) did the nodes actually adopt the node_ids we asked for?
        adopted = [int(node.node_id) for node in swarm.sybils]
        assert adopted == [int(node_id) for node_id in sybil_ids], "node_id was not honoured!"
        print("node_id injection: CONFIRMED (peers adopted attacker-chosen routing addresses)")

        # (2) structural capture of the replica set
        captured = capture_count(target, swarm.honest_node_ids, swarm.sybil_node_ids, NUM_REPLICAS)
        print(f"structural replica capture: {captured}/{NUM_REPLICAS}")

        honest_distances = sorted(xor_distance(i, target) for i in swarm.honest_node_ids)[:3]
        print(f"nearest honest XOR distances: {honest_distances}")

        # (3) live lookup: who does the DHT actually route to?
        prober = swarm.honest[0]
        nearest = await prober.find_nearest_nodes([target], k_nearest=NUM_REPLICAS)
        found = list(nearest[target].keys())
        sybil_found = sum(1 for node_id in found if swarm.is_sybil(node_id))
        print(f"live lookup returned {len(found)} peers, {sybil_found} of them sybil")

        ranked = rank_by_distance(target, swarm.honest_node_ids + swarm.sybil_node_ids)[:NUM_REPLICAS]
        print(
            f"ideal nearest-{NUM_REPLICAS} sybil share: {sum(1 for i in ranked if swarm.is_sybil(i))}/{NUM_REPLICAS}"
        )

    print("\nsmoke test OK")


if __name__ == "__main__":
    asyncio.run(main())
