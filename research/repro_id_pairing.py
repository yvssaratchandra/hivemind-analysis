"""
Minimal reproduction: `node_id` and `peer_id` travel together in every lookup
answer, and the pairing is asserted rather than proven.

Hivemind peers carry two independent identities — the Kademlia `node_id` (which
decides custody) and the libp2p `PeerID` (which decides how to dial). Because
they are independent, a peer cannot *derive* one from the other, so the pairing
has to be transmitted: `rpc_find` returns two aligned arrays,
`nearest_node_ids` and `nearest_peer_ids` (`dht/protocol.py:366-367`,
`proto/dht.proto:57-58`).

Contrast libp2p's own Kad-DHT, where a peer's DHT position *is* a function of
its PeerID, so nothing needs carrying.

This script wraps `rpc_find`, captures one genuine response, and checks each
pair against ground truth built from the node objects themselves.

Expected: both arrays present, equal length, index-aligned, every pair correct.

They are correct here because every node is honest. **Nothing in the protocol
checks the pairing** — a responder may name any (node_id, peer_id) couple it
likes and the receiver files it in its routing table. Dialling later proves the
PeerID via TLS, but never that the peer was entitled to that node_id. See
`REPORT_node_id.md`.

Run:
    .venv/bin/python research/repro_id_pairing.py

Loopback only; ~15 s.
"""

import asyncio

from hivemind.dht.node import DHTNode
from hivemind.dht.protocol import DHTProtocol
from hivemind.dht.routing import DHTID
from hivemind.p2p import P2PContext, PeerID
from hivemind.proto import dht_pb2

LOCALHOST = ["/ip4/127.0.0.1/tcp/0"]
captured = []

# Wrap rpc_find *before* any node exists: handlers are bound at registration
# (`getattr(servicer, method_name)`, `p2p/servicer.py:117`), so a later
# replacement would never be seen. The type annotations are load-bearing too —
# `_collect_rpc_handlers` reads them and raises at startup without them.
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

    # ground truth: who those node_ids actually belong to
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
