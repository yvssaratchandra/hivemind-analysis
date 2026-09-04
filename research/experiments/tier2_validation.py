"""Tier-2 validation: do the headline results survive a real multi-process swarm?

Tier 1 shares one Python process and bootstraps every peer off a single node. This
re-runs the headline claim from Tracks A and B on a swarm of genuinely separate OS
processes with randomised (non-star) bootstrap, to rule out both as artefacts.

Claims under test:
  A. Five crafted routing addresses capture the whole replica set for a target key,
     and an honest peer's store lands entirely on attacker nodes.
  B. Fabricated averager declarations dominate a victim's candidate list, written by
     one process with no keypairs behind them.

Run:  .venv/bin/python -m research.experiments.tier2_validation
"""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from hivemind.dht.node import DHTNode
from hivemind.utils.timed_storage import get_dht_time
from research.attacks.sybil_matchmaking import fabricate_peer_id
from research.common.metrics import Recorder
from research.common.multiprocess_swarm import LOCALHOST_MADDRS, launch_multiprocess_swarm
from research.common.sybil import capture_count, closest_ids, key_to_dhtid

RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"
KEY = "hivemind_run_alpha.all_averagers"
NUM_REPLICAS = 5
NUM_HONEST = 18
NUM_SYBILS = 5


async def _probe(initial_peers, coro):
    """Join the swarm as an ordinary peer, run a measurement, then leave."""
    node = await DHTNode.create(initial_peers=initial_peers, host_maddrs=LOCALHOST_MADDRS, num_replicas=NUM_REPLICAS)
    try:
        return await coro(node)
    finally:
        await node.shutdown()


async def validate_track_a(swarm, recorder: Recorder) -> None:
    target = key_to_dhtid(KEY)
    initial_peers = swarm.maddrs[0]

    structural = capture_count(target, swarm.honest_node_ids, swarm.sybil_node_ids, NUM_REPLICAS) / NUM_REPLICAS

    async def measure(node: DHTNode):
        nearest = await node.find_nearest_nodes([target], k_nearest=NUM_REPLICAS)
        found = list(nearest[target].keys())
        lookup = sum(1 for node_id in found if swarm.is_sybil(node_id)) / len(found) if found else 0.0
        stored = await node.store(KEY, "honest-value", expiration_time=get_dht_time() + 600)
        return lookup, len(found), stored

    lookup_capture, num_found, stored_ok = await _probe(initial_peers, measure)

    async def read_back(node: DHTNode):
        result = await node.get(KEY, latest=True)
        return result is not None and result.value == "honest-value"

    readable = await _probe(initial_peers, read_back)

    print(f"  structural capture      : {structural:.2f}")
    print(f"  live lookup capture     : {lookup_capture:.2f}  ({num_found} peers returned)")
    print(f"  honest store succeeded  : {stored_ok}")
    print(f"  honest value readable   : {readable}")

    recorder.record(
        track="A",
        num_honest=NUM_HONEST,
        num_sybils=NUM_SYBILS,
        structural_capture=structural,
        lookup_capture=lookup_capture,
        peers_returned=num_found,
        store_ok=stored_ok,
        readable=readable,
    )


async def validate_track_b(swarm, recorder: Recorder) -> None:
    """One process writes fabricated averager declarations; a victim reads them back.

    Uses ``DHTNode.store``/``get`` with subkeys directly -- the same call
    ``GroupKeyManager.declare_averager`` makes underneath (``key_manager.py:62``).
    """
    initial_peers = swarm.maddrs[0]
    group_key = "run.0b"
    num_fabricated = 150

    async def attack(node: DHTNode):
        expiration = get_dht_time() + 300
        # a handful of honest peers announce themselves
        for index in range(8):
            await node.store(
                group_key,
                True,
                expiration_time=expiration,
                subkey=fabricate_peer_id(index, salt="t2honest").to_bytes(),
            )
        # one process fabricates many more, each with an earlier expiration to win leadership
        for index in range(num_fabricated):
            await node.store(
                group_key,
                True,
                expiration_time=expiration - 60,
                subkey=fabricate_peer_id(index, salt="t2sybil").to_bytes(),
            )
        return True

    await _probe(initial_peers, attack)

    async def read(node: DHTNode):
        return await node.get(group_key, latest=True)

    result = await _probe(initial_peers, read)
    entries = result.value if result is not None and isinstance(result.value, dict) else {}
    honest_subkeys = {fabricate_peer_id(index, salt="t2honest").to_bytes() for index in range(8)}
    fake_seen = sum(1 for subkey in entries if subkey not in honest_subkeys)
    local_fraction = fake_seen / len(entries) if entries else 0.0

    print(f"  victim sees {len(entries)} declarations, {fake_seen} fabricated")
    print(f"  local malicious fraction: {local_fraction:.3f}")

    recorder.record(
        track="B",
        declarations_seen=len(entries),
        fabricated_seen=fake_seen,
        local_malicious_fraction=local_fraction,
        fabricated_written=num_fabricated,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()

    target = key_to_dhtid(KEY)
    sybil_ids = closest_ids(target, NUM_SYBILS)

    print(f"launching {NUM_HONEST} honest + {NUM_SYBILS} sybil peers as separate OS processes...")
    swarm = launch_multiprocess_swarm(
        NUM_HONEST, sybil_ids, node_kwargs={"num_replicas": NUM_REPLICAS}, seed=args.seed
    )
    recorder = Recorder(
        "tier2_validation",
        params={"num_honest": NUM_HONEST, "num_sybils": NUM_SYBILS, "num_replicas": NUM_REPLICAS, "seed": args.seed},
    )
    try:
        print(f"swarm up: {len(swarm.honest_node_ids)} honest, {len(swarm.sybil_node_ids)} sybil\n")
        print("== Tier 2 / Track A: eclipse of a DHT record ==")
        asyncio.run(validate_track_a(swarm, recorder))
        print("\n== Tier 2 / Track B: fabricated averager declarations ==")
        asyncio.run(validate_track_b(swarm, recorder))
    finally:
        swarm.shutdown()

    csv_path = recorder.write(RESULTS_DIR)
    print(f"\nwrote {csv_path}")


if __name__ == "__main__":
    main()
