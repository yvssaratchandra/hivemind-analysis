# Kademlia `node_id` is caller-supplied and never verified, so a peer can choose which records it stores

**Component:** `hivemind.dht` · **Version:** 1.2.0.dev0 (fork point `4bd43b7`) · **Severity:** enables Sybil/eclipse of any DHT record at fixed, swarm-size-independent cost

---

## Summary

Hivemind peers carry two identities:

| | libp2p **PeerID** | Kademlia **`node_id`** |
|---|---|---|
| Produced by | `multihash(sha256(public_key))` | `DHTID.generate()` — **or supplied by the caller** |
| Verified by peers? | **yes**, TLS proves key possession | **no**, accepted verbatim |
| Decides | how packets reach the peer | **which DHT records the peer stores** |

Record custody is decided purely by XOR distance between a key's hash and each peer's `node_id`. Because `node_id` is chosen by the peer itself and is not bound to the authenticated PeerID, **a peer can place itself at any address in the keyspace and become the custodian of any record it likes.** The cryptographically sound identity never enters the routing decision.

## Affected code

```python
# hivemind/dht/node.py:171
self.node_id = node_id if node_id is not None else DHTID.generate()
```

`node_id` is a public parameter of `DHTNode.create()`. When supplied it is used verbatim; the random default applies only when it is omitted. It is also self-reported across the wire — `dht.proto:10` documents the field as *"sender's own node id"* — and the receiver deserialises it without comparing against the authenticated `context.remote_id`:

```python
# hivemind/dht/protocol.py:150
sender_id = DHTID.from_bytes(request.peer.node_id)
sender_peer_id = context.remote_id          # authenticated, but never compared
```

A repo-wide search for any check binding `node_id` to the PeerID, or validating it at all, returns nothing.

## Steps to reproduce

```bash
git clone <this repo> && cd hivemind
python3 -m venv .venv
.venv/bin/pip install -e .          # also fetches the p2pd binary
.venv/bin/pip install -r requirements.txt
.venv/bin/python research/hivemind/reproduce_node_id.py
```

The script (`research/hivemind/reproduce_node_id.py`, ~60 lines) starts a bootstrap node, four ordinary peers, and one peer that asks for a deliberately arbitrary identifier — `0xDEADBEEF` repeated to 160 bits, visibly not a hash output:

```python
CHOSEN = DHTID(0xDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF)
attacker = await DHTNode.create(initial_peers=initial_peers, node_id=CHOSEN)
```

Everything binds to `127.0.0.1`; no external network is touched. Runtime ~15 s.

## Actual result

```
requested node_id : 0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef
adopted  node_id  : 0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef
identical         : True
its PeerID        : 12D3KooWG8XvoAK49DJxNaPd3E9TRn3GxCKzGmSX9m8UjjWUMDVP
                    (a genuine hash of its keypair -- but unrelated to the node_id above)

an honest peer asked 'who is nearest to 0xdeadbeef...beef?'
  1. 0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef  <- the crafted peer, XOR distance 0
  2. 0x5b2e792e0f9fd1066bf470981d09bf86ba0eeef9
  3. 0x5461e0c95bb54764f2c039cc80909c453b628fe3
accepted into that honest peer's routing table: True

targeting the record 'some_valuable_key' (address 0x871dc4ec...45dc):
  nearest honest peer sits at XOR distance 2^159
  crafted peer sits at XOR distance 0
  an honest peer stored the record; it landed on the crafted peer: YES
```

Two points about what this shows:

- **The verdict comes from an independent honest peer**, not the attacker's own state. That peer ranks the crafted identity nearest and admits it to its routing table.
- **The attacker's PeerID is genuine.** It is a real hash of a real keypair and would survive any TLS check. The cryptographic identity is sound; it simply has no bearing on custody.

**A companion check** (`research/hivemind/reproduce_id_pairing.py`, ~15 s) shows why the crafted address is reachable
at all: `rpc_find` returns `nearest_node_ids` and `nearest_peer_ids` as two aligned arrays
(`protocol.py:366-367`), so every lookup answer hands over the dialling address for each `node_id` it
names. The pairing is *transmitted*, never derived — and never checked. A responder may name any
`(node_id, peer_id)` couple and the receiver files it in its routing table; dialling later proves the
PeerID via TLS, but never the entitlement to that `node_id`.

## Expected result

A peer's routing address should be derived from, or bound to, its authenticated public key — as in libp2p's own Kad-DHT (keyed on PeerID) or Ethereum's discovery after Geth v1.9.0 (node ID derived from the secp256k1 key).

## Impact

Custody of an arbitrary record costs exactly `num_replicas` identities (default **5**), with no variance and **no growth with swarm size** — measured identical from 50 to 10 000 peers, because honest `node_id`s are uniform over 160 bits while a crafted one sits at distance 0. Minting an identity costs ~0.1 s and no compute.

Control of what a victim's *lookup returns* costs `bucket_size` identities (default **20**), since a lookup seeds from the `bucket_size` peers nearest the key (`node.py:629-633`) and merges every answer. Also independent of swarm size.

Downstream, the DHT key that carries averaging advertisements is the rendezvous point for group formation, so custody of it is the entry point to influencing who trains with whom.

## Notes

This is vanilla Kademlia's known weakness rather than an implementation slip. Kademlia (2002) never bound IDs to keys; S/Kademlia (2007) added crypto puzzles specifically to fix it. Hivemind adopted the unmodified protocol knowingly — the NeurIPS 2020 paper states *"we use unmodified Kademlia protocol, leaving further investigation to future work"*, names Sybil and eclipse attacks in its broader-impact section, and assumes in the appendix that identifiers are *"sampled uniformly from the space of possible outputs of the hash function"*. That is an assumption, not an enforced property, and this report is what happens when it does not hold.

Binding `node_id = H(PeerID)` would close the placement primitive at no protocol cost, though it would not by itself prevent an attacker grinding keypairs to land near a target — which is what S/Kademlia's puzzles address.
