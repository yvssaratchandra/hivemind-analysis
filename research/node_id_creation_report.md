## Issue Description
DHT node_id is caller supplied, and is never verified, so a peer can place itself strategically in the network
Component: hivemind.dht
Version: 1.2.0.dev0 (Fork point: `4bd43b7`)
Effect: enables Sybil/Eclipse attack of any DHT record at a fixed swarm size independent cost (Depends on replication factor (num_replicas) and Bucket size (bucket_size))

## Expected Behavior
A peer's routing address should be derived from, or bound to its authenticated public key similar to libp2p's Kademlia DHT PeerID key or Ethereum's (Geth) node ID derived from secp256k1 key.

## Related code
```python
# hivemind/dht/node.py:171
self.node_id = node_id if node_id is not None else DHTID.generate()
```
`node_id` is public param of `DHTNode.create()`. If you provide it, it will use it. The generation only happens if you don't provide it.

A repo-wide search for any check binding `node_id` to the PeerID, or validating it at all, returns nothing.

## Steps to Reproduce
```
git clone https://github.com/learning-at-home/hivemind
cd hivemind
python3 -m venv .venv
.venv/bin/pip install -e .
.venv/bin/pip install -r requirements.txt
.venv/bin/python reproduce_node_id.py
```

The script starts a bootstrap node, four honest peers, and one peer that asks for a specific ID: `0xDEADBEEF...` repeated to 160 bits (clearly not a hash output)

Specifically, this is the snippet in the script that lets creates a peer with this ID:

```python
CHOSEN = DHTID(0xDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF)
attacker = await DHTNode.create(initial_peers=initial_peers, node_id=CHOSEN)
```

## Result
```
requested node_id: 0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef
actual node_id: 0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef
identical: True
its PeerID: 12D3KooWG8XvoAK49DJxNaPd3E9TRn3GxCKzGmSX9m8UjjWUMDVP (Genuine hash - unrelated to node_id above)
```

- The 