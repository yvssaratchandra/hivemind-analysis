## Issue Description
Every DHT lookup answer carries `(node_id, peer_id)` pairs supplied by the responder. The receiver files them in its routing table without checking that the responder was entitled to name that pair.
Component: hivemind.dht
Version: 1.2.0.dev0 (Fork point: `4bd43b7`)
Effect: this is what makes a crafted `node_id` reachable. A peer that chooses its own DHT address still needs others to learn where to dial it, and the lookup response is the channel that carries it. Also lets a responder inject pairs it made up.

## Expected Behavior
The routing address should be derivable from the authenticated identity, so no pairing needs to be transmitted at all. libp2p's own Kademlia DHT needs no such arrays, because a peer's DHT position *is* a function of its PeerID. Hivemind has two independent identities, so the mapping has to travel, and once it travels it has to be trusted.

## Related code
Two parallel arrays, appended in lockstep and matched back up by index:

```python
# hivemind/dht/protocol.py:366-367  (responder side)
item.nearest_node_ids.append(node_id.to_bytes())
item.nearest_peer_ids.append(peer_id.to_bytes())
```

```python
# hivemind/dht/protocol.py:301-306  (receiver side)
nearest = dict(
    zip(
        map(DHTID.from_bytes, result.nearest_node_ids),
        map(PeerID, result.nearest_peer_ids),
    )
)
```

The receiver `zip`s the two arrays and keeps the result. There is no check anywhere that the responder had any business claiming that pairing. Dialling the peer later proves the PeerID (TLS proves key possession), but never proves its claim to that `node_id`.

So there is no separate ID-mapping table as in IPFS. The pairing is transmitted, not derived, and asserted, not proven.

## Steps to Reproduce
```
git clone https://github.com/learning-at-home/hivemind
cd hivemind
python3 -m venv .venv
.venv/bin/pip install -e .
.venv/bin/pip install -r requirements.txt
.venv/bin/python reproduce_id_pairing.py
```

The script wraps `rpc_find` before any node exists, starts a bootstrap node and four honest peers, runs one real lookup, and checks the captured pairs against ground truth built from the node objects themselves:

```python
_original_rpc_find = DHTProtocol.rpc_find

async def logging_rpc_find(
    self, request: dht_pb2.FindRequest, context: P2PContext
) -> dht_pb2.FindResponse:
    response = await _original_rpc_find(self, request, context)
    for result in response.results:
        captured.append((list(result.nearest_node_ids), list(result.nearest_peer_ids)))
    return response

DHTProtocol.rpc_find = logging_rpc_find
```

Note: the type annotations are required. `_collect_rpc_handlers` (`p2p/servicer.py`) reads them to register the handler and raises at startup without them.

## Result
```
one real rpc_find response carried:
  nearest_node_ids : 3 entries
  nearest_peer_ids : 3 entries
  same length      : True

  the pairs, checked against who those peers actually are:
    0x010c7aeb0bb718b1b53b73f850463fec175e600d -> 12D3KooWP2eTFYWmzJWZ...  correct pair: True
    0xd76a65c0f918f2e96b67b87ba505fa2c6ed1c7b9 -> 12D3KooWSMgx5PFeTgfb...  correct pair: True
    0xa03ade1cf5bb49c1d96787420b3ecd46759bbe83 -> 12D3KooWF2qFkTXgeD2V...  correct pair: True
```

Both arrays are present, equal in length, index-aligned, and every pair is correct. They are correct here only because every peer in this swarm is honest. Nothing in the code would have caught it if they were not.

## Notes
This is the companion to `node_id_creation_report.md` and answers the question that one raises: if `node_id` is self-chosen and unrelated to the PeerID, how does anyone know where to dial? Because the answer ships the address alongside the identifier, on the responder's word.
