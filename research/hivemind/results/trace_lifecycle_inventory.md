# Control-message trace inventory

Scope: Hivemind Python application-level training protocols; excludes p2pd/libp2p internals and MoE expert RPCs.

Recorded 18274 wire messages and 1703 logical DHT operations. Exact protobuf bytes are present for 18274 wire messages.

## Observed wire messages

| RPC | Direction | Protobuf | Code | Count | Purpose |
| --- | --- | --- | --- | --- | --- |
| rpc_aggregate_part | request | AveragingData | PART_FOR_AVERAGING | 18 | Send a tensor shard and its averaging weight. |
| rpc_aggregate_part | response | AveragingData | AVERAGED_PART | 18 | Return the averaged tensor shard. |
| rpc_download_state | request | DownloadRequest | - | 2 | Request a complete shared state snapshot. |
| rpc_download_state | response | DownloadData | - | 2 | Return one metadata or tensor chunk from shared state. |
| rpc_find | request | FindRequest | - | 8238 | Ask for DHT records and nearer peers. |
| rpc_find | response | FindResponse | - | 8238 | Return found records and routing referrals. |
| rpc_join_group | request | JoinRequest | - | 20 | Request admission using schema, deadline, mode, group key, and gathered metadata. |
| rpc_join_group | response | MessageFromLeader | ACCEPTED | 20 | Leader provisionally accepts the follower. |
| rpc_join_group | response | MessageFromLeader | BEGIN_ALLREDUCE | 18 | Leader sends the final ordered group roster. |
| rpc_join_group | response | MessageFromLeader | GROUP_DISBANDED | 2 | Leader closes the proposed group. |
| rpc_ping | request | PingRequest | - | 8 | Ask for identity, liveness, time, and optional reachability validation. |
| rpc_ping | response | PingResponse | - | 8 | Return identity, DHT time, and reachability result. |
| rpc_store | request | StoreRequest | - | 841 | Ask a DHT peer to retain expiring records. |
| rpc_store | response | StoreResponse | - | 841 | Report which DHT records were accepted. |

## Logical DHT operations

| Kind | Count | Purpose |
| --- | --- | --- |
| advertisement.get | 1463 | Publish, read, or withdraw an expiring matchmaking advertisement. |
| advertisement.store | 27 | Publish, read, or withdraw an expiring matchmaking advertisement. |
| advertisement.withdraw | 24 | Publish, read, or withdraw an expiring matchmaking advertisement. |
| donor.get | 2 | Publish, read, or withdraw state-donor priority. |
| donor.store | 64 | Publish, read, or withdraw state-donor priority. |
| progress.get | 28 | Publish, read, or withdraw expiring training progress. |
| progress.store | 91 | Publish, read, or withdraw expiring training progress. |
| progress.withdraw | 4 | Publish, read, or withdraw expiring training progress. |

## RPC-family coverage

| RPC | Layer | Request | Response | Streaming | Observed |
| --- | --- | --- | --- | --- | --- |
| rpc_ping | L2 DHT | PingRequest | PingResponse | False | True |
| rpc_store | L2 DHT | StoreRequest | StoreResponse | False | True |
| rpc_find | L2 DHT | FindRequest | FindResponse | False | True |
| rpc_join_group | L3 Matchmaking | JoinRequest | MessageFromLeader | True | True |
| rpc_aggregate_part | L4 All-reduce | AveragingData | AveragingData | True | True |
| rpc_download_state | L6 State transfer | DownloadRequest | DownloadData | True | True |

## Averaging control-code coverage

| Number | Code | Observed | Meaning |
| --- | --- | --- | --- |
| 0 | NO_CODE | 0 | No explicit averaging control code. |
| 1 | REQUEST_JOIN | 0 | Defined enum value; normal join requests use JoinRequest and do not carry this code. |
| 2 | ACCEPTED | 20 | Leader provisionally accepts the follower. |
| 3 | BEGIN_ALLREDUCE | 18 | Leader sends the final ordered group roster. |
| 4 | PART_FOR_AVERAGING | 18 | Send a tensor shard and its averaging weight. |
| 5 | AVERAGED_PART | 18 | Return the averaged tensor shard. |
| 6 | NOT_DECLARED | 0 | Reject a request for an undeclared group. |
| 7 | NOT_A_LEADER | 0 | Redirect the requester to the current leader. |
| 8 | BAD_EXPIRATION_TIME | 0 | Reject an incompatible group deadline. |
| 9 | BAD_SCHEMA_HASH | 0 | Reject an incompatible tensor schema. |
| 10 | BAD_GROUP_ID | 0 | Reject an unknown group identifier. |
| 11 | DUPLICATE_PEER_ID | 0 | Reject a duplicate peer identity. |
| 12 | GROUP_IS_FULL | 0 | Reject because the group has no free slot. |
| 13 | NOT_LOOKING_FOR_GROUP | 0 | Reject because the peer is unavailable. |
| 14 | PROTOCOL_VIOLATION | 0 | Report an invalid protocol sequence or value. |
| 15 | INTERNAL_ERROR | 0 | Report an internal averaging failure. |
| 16 | CANCELLED | 0 | Cancel participation in the current all-reduce. |
| 17 | GROUP_DISBANDED | 2 | Leader closes the proposed group. |
| 18 | BAD_GROUP_KEY | 0 | Reject an obsolete or incompatible group key. |

## RPC outcomes

| RPC | Outcome | Count |
| --- | --- | --- |
| rpc_aggregate_part | completed | 18 |
| rpc_download_state | completed | 2 |
| rpc_find | completed | 8238 |
| rpc_join_group | consumer_closed | 20 |
| rpc_ping | completed | 8 |
| rpc_store | completed | 841 |

## Boundary

- Caller-side capture records sent and returned application protobufs, including failed calls with no response.
- Plain DHT keys exist only in logical_operation rows; wire DHT messages contain hashed key IDs.
- Unobserved enum codes remain in the static inventory; most are error or cancellation branches, while REQUEST_JOIN is defined but not carried by JoinRequest.
- A synthetic trace_ended outcome marks a call whose caller process ended before it reported completion or failure.
- Transport, security-handshake, peer-discovery, NAT, relay, and multiplexing messages inside p2pd are outside this trace.
