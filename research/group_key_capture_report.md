## Issue Description
The averaging advertisements live on the DHT nodes nearest to K, where K is the hash of the group key. An attacker does not need to invert that hash. He computes it forward and then asks to be placed on top of it.
Component: hivemind.dht, hivemind.averaging
Version: 1.2.0.dev0 (Fork point: `4bd43b7`)
Effect: all `num_replicas` copies of the advertisement record land on attacker peers, and none on honest ones.

## Why the hash is not a defence
The group key is a plain string, built by `GroupKeyManager.current_key` (`key_manager.py:42-44`) as `f"{prefix}.0b{group_bits}"`. The prefix is the run name that every peer in the swarm already shares. So `K = DHTID.generate(source=group_key)` is a forward hash of a public value. Anyone can compute it.

The hash decides where the *record* goes. It does not decide where the *attacker* goes, because `node_id` is not a hash of anything he has to produce. It is a free parameter of `DHTNode.create()` (see `node_id_creation_report.md`). So there is no preimage to find and nothing to grind:

```python
K = DHTID.generate(source=GROUP_KEY)              # forward, public
node = await DHTNode.create(..., node_id=DHTID(int(K) ^ offset))   # just ask for it
```

This is the difference from Bitcoin style proof of work or S/Kademlia's crypto puzzles, where the ID *is* constrained to be a hash output and landing near a target does cost work.

## Steps to Reproduce
```
.venv/bin/python reproduce_group_key_capture.py
```

Bootstrap, 5 honest peers, and 5 attacker peers created at K, K^1 ... K^4. One honest peer then advertises itself exactly as `declare_averager` does (`key_manager.py:63-68`), with its PeerID as the subkey.

## Result
```
group key : 'my_experiment.0b'
K = hash  : 0x0c9f019423e7c9dc36cce7c80dfed0cd1a8241eb   (forward hash of a public string)

attacker node_ids, requested and adopted:
  0x0c9f019423e7c9dc36cce7c80dfed0cd1a8241eb  d(K)=0x0
  0x0c9f019423e7c9dc36cce7c80dfed0cd1a8241ea  d(K)=0x1
  0x0c9f019423e7c9dc36cce7c80dfed0cd1a8241e9  d(K)=0x2
  0x0c9f019423e7c9dc36cce7c80dfed0cd1a8241e8  d(K)=0x3
  0x0c9f019423e7c9dc36cce7c80dfed0cd1a8241ef  d(K)=0x4

nearest honest peers for comparison:
  0x6061bb9bc1d85e854c3b44a4dea4db3fc8de15d3  d(K)=0x6cfe...
  0x707e0f8cccb95da3cfa0ae90424099ad26ebd061  d(K)=0x7ce1...
  0x888ff683a2a71c5d35da1cf4ddfcaa8cd17db366  d(K)=0x8410...

honest peer declared itself looking-for-group: True
advertisement now stored on 5/5 attacker peers
advertisement now stored on 0/6 honest peers
```

The attackers sit at distance 0 to 4. The nearest honest peer is at roughly 2^158, because honest node_ids are spread uniformly over the whole space and nothing is pulling them towards K. So the 5 nearest peers to K are all attackers, and every replica goes to them.

## Notes
Custody is not yet full control of what a reader sees. A reader asks `bucket_size` (20) peers, not `num_replicas` (5), and caching lets the other 15 answer from copies they picked up. That is the 5 vs 20 question measured separately.

What this file settles is the narrower point: the hash in the middle of the addressing scheme protects nothing, because it is applied to a public string and the attacker's position is unconstrained.
