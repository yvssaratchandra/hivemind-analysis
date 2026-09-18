import asyncio

from hivemind.dht.node import DHTNode
from hivemind.dht.protocol import DHTProtocol
from hivemind.dht.routing import DHTID
from hivemind.p2p import P2PContext, PeerID
from hivemind.proto import dht_pb2

LOCALHOST = ["/ip4/127.0.0.1/tcp/0"]
captured = []

_original_rpc_find = DHTProtocol.rpc_find


async def logging_rpc_find(
    self, request: dht_pb2.FindRequest, context: P2PContext
) -> dht_pb2.FindResponse:
    response = await _original_rpc_find(self, request, context)
    for result in response.results:
        captured.append((list(result.nearest_node_ids), list(result.nearest_peer_ids)))
    return response


DHTProtocol.rpc_find = logging_rpc_find


async def main() -> None:
    bootstrap = await DHTNode.create(host_maddrs=LOCALHOST)
    initial_peers = [str(addr) for addr in await bootstrap.get_visible_maddrs()]
    nodes = [await DHTNode.create(initial_peers=initial_peers, host_maddrs=LOCALHOST) for _ in range(4)]

    truth = {int(node.node_id): str(node.peer_id) for node in [bootstrap, *nodes]}
    await asyncio.sleep(2)

    captured.clear()
    await nodes[0].find_nearest_nodes([DHTID.generate(source="anything")], k_nearest=4)

    node_ids, peer_ids = captured[0]
    print("one real rpc_find response carried:")
    print(f"  nearest_node_ids : {len(node_ids)} entries")
    print(f"  nearest_peer_ids : {len(peer_ids)} entries")
    print(f"  same length      : {len(node_ids) == len(peer_ids)}\n")
    print("  the pairs, checked against who those peers actually are:")
    for raw_node_id, raw_peer_id in zip(node_ids, peer_ids):
        node_id, peer_id = int(DHTID.from_bytes(raw_node_id)), str(PeerID(raw_peer_id))
        print(f"    {node_id:#042x} -> {peer_id[:20]}...  correct pair: {truth.get(node_id) == peer_id}")

    for node in [bootstrap, *nodes]:
        await node.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
