## Issue Description
An attacker can serve made up `(node_id, peer_id)` pairs in its lookup answers, and the victim accepts all of them. But it does not get him much. The record still stores and still reads back.
Component: hivemind.dht
Version: 1.2.0.dev0 (Fork point: `4bd43b7`)
Effect: wasted lookups and dead dials. Not loss of the record. This is the weaker cousin of `node_id_creation_report.md`, not another way in.

## Expected Behavior
A peer should not be able to name peers that don't exist. Nothing checks the pairing (see `id_pairing_report.md`), so it can.

## Steps to Reproduce
```
.venv/bin/python reproduce_forged_pairs.py
```

One attacker in a swarm of six. Its `rpc_find` throws away the real answer and replies with 4 node_ids sitting right on top of the target key, each paired with a PeerID belonging to a node we started and killed, so nobody is there:

```python
for result in response.results:
    del result.nearest_node_ids[:]
    del result.nearest_peer_ids[:]
    for node_id, peer_id in forged_pairs:
        result.nearest_node_ids.append(node_id.to_bytes())
        result.nearest_peer_ids.append(peer_id.to_bytes())
```

## Result
```
what the victim's lookup returned:
  0x871dc4ec2021754bcbdbddaa29d023138b5f45dc  d=0x0  FORGED (nobody is there)
  0x871dc4ec2021754bcbdbddaa29d023138b5f45dd  d=0x1  FORGED (nobody is there)
  0x871dc4ec2021754bcbdbddaa29d023138b5f45de  d=0x2  FORGED (nobody is there)
  0x871dc4ec2021754bcbdbddaa29d023138b5f45df  d=0x3  FORGED (nobody is there)
  0x98b557eab81344d7b169560ea3195e1ab15a4e3e  d=0x1fa8...  real peer

forged entries in the answer : 4/5

victim stored the record     : True
another honest peer reads it : b'honest value'
peers actually holding it    : 5 (num_replicas default is 5)
```

The forged pairs take 4 of the 5 slots, ranked top because the attacker put them at distance 0. So the poisoning works completely at the answer level.

It still costs him nothing and gains him nothing. `store` walks down its candidate list and keeps going when one doesn't answer (`node.py:412-431`), so the dead entries are skipped and all 5 replicas are placed on real peers anyway. The record reads back fine.

## Notes
So this is a nuisance, not a capture. The victim burns a timeout per forged entry, which is worth knowing if anyone measures lookup latency, but the data survives.

Worth saying what this does not test: one attacker, six peers, and every other route out of the victim was honest. The attack that actually takes a record is the custody one in `node_id_creation_report.md`, where the attacker owns the peers that are genuinely nearest instead of inventing peers that aren't there.
