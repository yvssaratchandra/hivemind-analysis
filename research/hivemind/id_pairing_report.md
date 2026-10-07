## Issue Description
DHT lookup responses carry `(node_id, peer_id)` pairs. The receiver trusts whatever the responder says, so a responder can claim any pairing it likes.
Component: hivemind.dht
Version: 1.2.0.dev0 (Fork point: `4bd43b7`)
Effect: this is what makes a crafted node_id reachable. A peer that picks its own DHT address still needs others to learn where to dial it, and this is what carries it.

## Expected Behavior
The address should be derivable from the authenticated identity, so nothing needs to be sent. libp2p's own Kademlia DHT doesn't need these arrays at all, because a peer's DHT position is a function of its PeerID. Hivemind has two separate identities, so the mapping has to travel, and once it travels it has to be trusted.

## Related code
Two parallel arrays, appended together and matched back up by index:

```python
# hivemind/dht/protocol.py:366-367  (sender)
item.nearest_node_ids.append(node_id.to_bytes())
item.nearest_peer_ids.append(peer_id.to_bytes())

# hivemind/dht/protocol.py:301-306  (receiver)
nearest = dict(zip(map(DHTID.from_bytes, result.nearest_node_ids),
                   map(PeerID, result.nearest_peer_ids)))
```

The receiver just zips them and keeps the result. Dialling the peer later proves the PeerID over TLS, but never proves its claim to that node_id. So there is no separate ID mapping table like IPFS has. The pairing is sent, not derived, and asserted, not proven.

## Steps to Reproduce
```
.venv/bin/python reproduce_id_pairing.py
```
(same setup as `node_id_creation_report.md`)

The script wraps `rpc_find`, runs one real lookup on a swarm of five honest peers, and checks the captured pairs against who those peers actually are.

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

Both arrays present, same length, index aligned, every pair correct. They are correct only because every peer here is honest. Nothing in the code would have caught it otherwise.
