# Duplicate DHT node IDs on a loopback Hivemind swarm

## Question and scope

Can two reachable peers use the same DHT `node_id`, and what does an honest routing table do? This is a local experiment with real Hivemind DHT RPCs and libp2p daemons. It does not contact a public swarm or run model averaging. No `hivemind/` source file was changed.

Source checkout: `937090e2a5659d49195a6759cf6bbcda5bb593b1`. The package was copied to `/tmp` to compile its protobufs and add the pinned p2pd release binary without modifying the checkout. The p2pd SHA-256 was `42f8f48e62583b97cdba3c31439c08029fb2b9fc506b5bdd82c46b7cc1d279d8`, matching `setup.py`. Python 3.14.7, PyTorch 2.14.1+cpu, protobuf 7.36.2, grpcio 1.84.0, and NumPy 2.5.3 were used. Python dependencies were installed into a temporary virtual environment.

## Method

The [four-peer reproduction](node_id_collision_short_20261007T1446Z/) used one bootstrap, one observer, and two claimants. The [first eight-peer collision trial](node_id_collision_full_20261007T1448Z/) and [its distinct-ID control](node_id_distinct_full_20261007T1449Z/) used one bootstrap, five other honest observers, and two claimants. A [second collision trial](node_id_collision_full_20261007T1452Z_r2/) and [its control](node_id_distinct_full_20261007T1453Z_r2/) repeated that setup with fresh libp2p identities. The exact runnable drivers are preserved as [v1](node_id_collision_full_20261007T1448Z/driver_v1.txt) and [v2](node_id_collision_full_20261007T1452Z_r2/driver_v2.txt). V2 adds forward/reverse map snapshots; it does not change the contact sequence. Each output directory has `config.json` and timestamped, unaggregated `events.jsonl`.

All listeners bound to `127.0.0.1` only. Honest DHT IDs and the common target ID were derived from fixed labels. In a collision arm, A and B both claimed the target ID while having different libp2p PeerIDs. In a control arm, B claimed `target XOR 1`. Each observer contacted other honest peers, then A, then B, then A again using `call_ping(validate=True)`. After each stage, the driver recorded the routing table. Finally, each observer performed a real nearest-node lookup for the target, and the bootstrap pinged both claimants directly by PeerID. Seed values and actual PeerIDs are in the configs and start events. The seed does not determine libp2p keypairs, which are generated separately.

## Result

| Run | Observers | Mapping after A → B → A | Entries for target ID per observer | Total forward entries per observer | Claimants in final nearest-node lookup |
| --- | ---: | --- | --- | ---: | --- |
| Four-peer collision | 2 | A → B → A, both observers | 1 at every stage | 2 | A only, 2/2 lookups |
| Eight-peer collision, trial 1 | 6 | A → B → A, all observers | 1 at every stage | 6 | A only, 6/6 lookups |
| Eight-peer collision, trial 2 | 6 | A → B → A, all observers | 1 at every stage | 6 | A only, 6/6 lookups |
| Eight-peer distinct control, trial 1 | 6 | A remains at target; B has its own ID | 1 for target, plus 1 for B | 7 | A and B, 6/6 lookups |
| Eight-peer distinct control, trial 2 | 6 | A remains at target; B has its own ID | 1 for target, plus 1 for B | 7 | A and B, 6/6 lookups |

Every logged claimant ping returned the claimed DHT ID. The direct final reachability check reached **both** claimants in every run, including the collision arms. Thus both processes existed and responded, but each observer's DHT forward table had only one slot for their shared ID.

V2 also recorded the reverse map. In all six observers at each collision stage, both claimants' PeerIDs mapped back to the same DHT ID, while the forward map pointed only to the most recently contacted claimant. The distinct-ID control had separate reverse mappings. This is consistent with `RoutingTable.add_or_update_node()` and `KBucket.add_or_update_node()`: they assign the new PeerID to the existing node-ID key and do not remove the old reverse entry (`hivemind/dht/routing.py:49–65, 185–205`).

The experiment confirms **local routing-table overwrite, not a global uniqueness rule**. Contact order determined which claimant each observer associated with the shared ID during these short runs. Six observers were deliberately contacted in the same order; this does not establish how a naturally asynchronous WAN swarm would distribute its mappings. It does not test record storage, eviction after failures, lasting convergence, or an averaging outcome. The small observed counts are not a deployment success rate.

The first unprivileged attempt, [recorded separately](node_id_collision_short_20261007T1445Z/), stopped before starting a node because the workspace sandbox denied loopback sockets. The successful runs used an approved execution context with the same loopback-only driver. The failed attempt is excluded from the table.
