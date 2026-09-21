import asyncio

from hivemind.dht.node import DHTNode
from hivemind.dht.routing import DHTID
from hivemind.utils.timed_storage import get_dht_time

LOCALHOST = ["/ip4/127.0.0.1/tcp/0"]

# the averaging group key, exactly as GroupKeyManager.current_key builds it:
#   f"{prefix}.0b{group_bits}"   (key_manager.py:42-44)
# the prefix is the run name every peer in the swarm already shares.
GROUP_KEY = "my_experiment.0b"

# K is a forward hash of a public string. Anyone can compute it; nobody needs to invert it.
K = DHTID.generate(source=GROUP_KEY)

NUM_REPLICAS = 5  # DHTNode default


async def main() -> None:
    bootstrap = await DHTNode.create(host_maddrs=LOCALHOST)
    initial_peers = [str(addr) for addr in await bootstrap.get_visible_maddrs()]
    honest = [await DHTNode.create(initial_peers=initial_peers, host_maddrs=LOCALHOST) for _ in range(5)]

    # no grinding: the attacker simply *asks* for the addresses it wants, at and beside K
    attackers = [
        await DHTNode.create(initial_peers=initial_peers, host_maddrs=LOCALHOST, node_id=DHTID(int(K) ^ offset))
        for offset in range(NUM_REPLICAS)
    ]

    print(f"group key : {GROUP_KEY!r}")
    print(f"K = hash  : {int(K):#042x}   (forward hash of a public string)\n")

    print("attacker node_ids, requested and adopted:")
    for node in attackers:
        print(f"  {int(node.node_id):#042x}  d(K)={int(K.xor_distance(node.node_id)):#x}")
    print("\nnearest honest peers for comparison:")
    for node in sorted(honest, key=lambda n: K.xor_distance(n.node_id))[:3]:
        print(f"  {int(node.node_id):#042x}  d(K)={int(K.xor_distance(node.node_id)):#x}")

    await asyncio.sleep(3)

    # an honest averager advertises itself, exactly as declare_averager does (key_manager.py:63-68)
    advertiser = honest[0]
    declared = await advertiser.store(
        key=GROUP_KEY,
        subkey=advertiser.peer_id.to_bytes(),
        value=True,
        expiration_time=get_dht_time() + 600,
    )
    await asyncio.sleep(2)

    print(f"\nhonest peer declared itself looking-for-group: {declared}")

    held_by_attackers = sum(node.protocol.storage.get(K) is not None for node in attackers)
    held_by_honest = sum(node.protocol.storage.get(K) is not None for node in [bootstrap, *honest])
    print(f"advertisement now stored on {held_by_attackers}/{len(attackers)} attacker peers")
    print(f"advertisement now stored on {held_by_honest}/{len(honest) + 1} honest peers")

    for node in [bootstrap, *honest, *attackers]:
        await node.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
