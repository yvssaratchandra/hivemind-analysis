# LocalAI control messages through the peer lifecycle

Purpose: observe control messages exchanged by real, model-free LocalAI peers in a private Docker LAN, then label the records by their role and the lifecycle phase in which they were captured.

## Implementation status

| Step | Status |
| --- | --- |
| Inspect pinned LocalAI/EdgeVPN protocols and capture hooks | Complete; source-inferred catalogue in `message_protocol_reference.json` |
| Build a diagnostic peer that records decrypted gossip and discovery | Complete; pinned image built, mDNS and decrypted gossip captured |
| Prepare bounded lifecycle runner and safety checks | Complete; 13 runner checks, 12 analyzer checks, six Go decoder checks and six existing baseline checks passed |
| Validate capture with a small pilot | Complete; four-peer and two-peer captures passed; failed attempts preserved |
| Record steady operation, service access, departure and rejoin | Complete; 32 service peers, one gateway and one diagnostic observer |
| Produce a lifecycle catalogue with raw evidence and limitations | Complete; [full report](results/control-messages-lifecycle32-20261007T1350/LIFECYCLE_REPORT.md) and [analysis](results/control-messages-lifecycle32-20261007T1350/analysis.json) |

LocalAI source: `a3d555653c0cbfd73ab59ab15beab80903e96484`; EdgeVPN: `v0.34.0`; libp2p: `v0.48.0`. Upstream source and the host launcher stay unchanged.

The experiment uses no models or GPU and makes no inference request. DHT is disabled; mDNS discovery is confined to an internal Docker bridge. A diagnostic observer records decrypted ledger envelopes at the receiving endpoint; encrypted packets alone would not reveal their contents. The observer participates in gossip and ledger synchronization but advertises no LocalAI service or heartbeat. A separate LocalAI federation gateway retries read-only `/v1/models` requests until one routes successfully through a service tunnel.

Lifecycle phases: **bootstrap/discovery → admission/advertisement → steady maintenance → service routing → departure/expiry → rejoin/resynchronization → shutdown**. A control record can occur in several phases. Several record types share one full-ledger gossip envelope; record observations are not counts of distinct wire packets. The immediate delivering peer is not necessarily the original author of a record.

The completed run used **32 service peers**, with a 256 MiB memory ceiling and 0.5 CPU allowance per peer. Steady observation lasted 974 seconds to include the unchanged 900-second heartbeat cleanup window. The purpose is to identify message types by lifecycle stage, not to establish a maximum peer count; **four peers are sufficient for future lifecycle captures**.

Follow-up: [short DHT-enabled capture](DHT_MESSAGES.md) records `FIND_NODE`, `ADD_PROVIDER` and `GET_PROVIDERS` requests with two LocalAI peers and a private diagnostic DHT server.

## Coordinator and leader roles

LocalAI has no elected, network-wide inference master in its P2P modes. An operator starts a llama.cpp server to coordinate a sharded inference job, or a federation gateway to route requests. Multiple such entry points can coexist. Managed distributed mode instead relies on shared PostgreSQL/NATS infrastructure. P2P discovery and metadata exchange therefore coexist with per-request or per-job coordinating roles.

The pinned EdgeVPN dependency has a separate **heartbeat cleanup leader**. Each peer reads its local `healthcheck` ledger, considers peer IDs with timestamps newer than 15 minutes, and chooses the ID with the highest FNV-1a 32-bit hash. There is no vote, consensus exchange, or privileged preconfigured identity in this selection. Different ledger views can give different answers. After its 900-second scrub interval, the selected peer deletes the entire healthcheck bucket; subsequent heartbeats repopulate it. This role does not assign models, route inference, or lead a `NetworkID` group. See [pinned selection code](https://github.com/mudler/edgevpn/blob/v0.34.0/pkg/utils/leader.go), [heartbeat implementation](https://github.com/mudler/edgevpn/blob/v0.34.0/pkg/services/alive.go), and [LocalAI modes](https://localai.io/docs/features/distribute/index.print.html).

## Lifecycle catalogue

All five ledger record types below were directly captured in the completed run. A lifecycle category describes a record's purpose; advertisements and heartbeats recur in later phases as well.

| Lifecycle category | Message or record | Information exchanged | Evidence |
| --- | --- | --- | --- |
| 1. Discovery / connection | mDNS query and response; libp2p connect event | Token-derived LAN discovery name; PTR/SRV/TXT records include peer addresses | Decoded UDP/5353 packets and observer connection callbacks |
| 2. Service advertisement | `services[service-name]` | Service name and libp2p `PeerID`: how to reach a service | Decrypted ledger snapshots |
| 2. Group advertisement | `<NetworkID>_federated[service-name]` | LocalAI node ID, name and `LastSeen`; tunnel fields were empty | Decrypted ledger snapshots; `_worker` and `_mlx_worker` variants are source-only here |
| 3. Liveness | `healthcheck[PeerID]`; advertisement `LastSeen` refresh | Peer heartbeat timestamps and application-service freshness | Decrypted ledger snapshots; separate freshness thresholds in code |
| 3. Metadata synchronization | `shared_state[models]` | Model-name list; `[]` in this model-free lab | Decrypted ledger snapshots; no model weights transferred |
| 3. Ledger replication | Gzip-compressed full block over encrypted GossipSub | `Index`, `Timestamp`, `Storage`, `Hash`, `PrevHash`; several record types together | Exact decrypted body retained as base64 with SHA-256 and parsed block |
| 3. Periodic housekeeping | Heartbeat-bucket reset; local cleanup-leader calculation | Full snapshots without `healthcheck`; no dedicated election record | Three receptions at 14:07:26 UTC lacked the bucket while service metadata remained; origin/acceptance not proven |
| 4. Service access / authorization | `users[PeerID]`; federation service tunnel | Gateway registration with `PeerID`/timestamp; gateway chooses a service locally | `users` captured; `/v1/models` routed successfully; service stream contents were not decoded |
| 5. Crash / expiry | Disconnect event; aging `LastSeen` | A crashed peer stops refreshing its advertisement; survivors exclude it from fresh membership | Connection callbacks and sampled peer APIs; no explicit LocalAI leave record in the inspected path |
| 6. Restart / rejoin | New discovery, service identity and advertisements | Restarted peer registers again; old IDs may remain in cached metadata | Observer snapshots and fresh-membership controls |
| 7. Shutdown | Peer disconnects and observer listen-address closure | Transport connections close; no explicit LocalAI leave record in the inspected path | Observer callbacks; no mDNS TTL-zero records captured |

## Measured outcome

[Run summary](results/control-messages-lifecycle32-20261007T1350/summary.json): **passed**, 2026-10-07 13:48:26–14:12:56 UTC. All admission and rejoin stages ended with full-peer fresh-membership checks passing. The gateway request succeeded; all 33 configured/loaded-model probes were empty. No OOM, log-capture errors, new controller process-limit events, or cleanup failures occurred. All lab containers and networks were removed.

- **Capture:** 7,800 gossip receptions, 3,637 distinct block hashes, 19,991 mDNS packets, and 68 connection / 68 disconnection callbacks. No malformed/unknown observer events or gzip/body-hash errors. These are one observer's counts, not a census of all network packets.
- **RAM:** steady service-peer Docker memory median 86.7 MiB; sampled maximum 93.2 MiB. Lowest sampled host memory available: 16.77 GiB. No RAM clearing was needed; these figures exclude loaded models and are not an inference sizing guide.
- **Freshness:** steady witnesses ranged from **30–32 fresh peers**, despite 32 running peers. After the intentional crash, all three departure witnesses reached 31 fresh peers; after restart all 32 peer APIs reported 32 fresh hosts. Most survivors still listed 33 cached service IDs, including the old one. A listed record is not proof of a currently available peer.
- **Cleanup:** `observer.jsonl:22697–22699` contains the three late snapshots without `healthcheck` (block indices 2611/2612). This matches the source's bucket-reset behavior, but neither original authorship nor network-wide acceptance is established by the trace.

Raw evidence: [compressed observer JSONL](results/control-messages-lifecycle32-20261007T1350/observer.jsonl.gz), [phase log](results/control-messages-lifecycle32-20261007T1350/phases.jsonl), [API/resource samples](results/control-messages-lifecycle32-20261007T1350/samples.jsonl), and [empty-model controls](results/control-messages-lifecycle32-20261007T1350/model-controls.jsonl). The run's `instrumentation/` directory preserves the runner and observer sources; image IDs, configuration, source hashes and randomized identity/token details are recorded in the summary.

The large observer trace is stored as a lossless gzip archive to fit GitHub's per-file limit.
[Extraction and checksum instructions](results/control-messages-lifecycle32-20261007T1350/TRACE_ARCHIVE.md)
restore `observer.jsonl` before running the existing analyzer. Raw line references are unchanged.

## Capture quality and preserved attempts

Failed prefixes remain separate from completed captures; no attempt is a deployment success-rate estimate.

| Preserved attempt | Outcome | Relevant finding / correction |
| --- | --- | --- |
| [Four-peer pilot](results/control-messages-pilot-20261007T1322/LIFECYCLE_REPORT.md) | Passed | All lifecycle stages captured; 165 gossip receptions, no decode errors |
| [First pilot](results/control-messages-pilot-20261007T1318/pilot-completion-audit.json) | Failed | Runner passed a phase argument twice; fixed |
| [CLI collector](results/control-messages-main-20261007T1327/LIFECYCLE_REPORT.md) | Failed | Docker CLI followers exhausted the controller's 512-process/thread limit; replaced with socket streams |
| [Socket pilot](results/control-messages-socket-pilot-20261007T1336/LIFECYCLE_REPORT.md) | Failed | Logs API rejected an ISO restart timestamp; converted to Unix seconds |
| [Restart retry](results/control-messages-socket-rejoin-pilot-20261007T1340/summary.json) | Passed | Two-peer lifecycle completed without capture errors |
| [Freshness check](results/control-messages-main-socket-20261007T1341/summary.json) | Failed | Some peer views held an advertisement older than 40 seconds; added bounded convergence retries |

The final runner streams logs over the Docker Unix socket, without a CLI process per peer. Unexpected EOF and forced stream cancellation mark capture incomplete. Chosen container ceilings are safety limits, not required RAM. Both the observer's decompression limit and the log reader's frame limit are 64 MiB; encountering either is reported, rather than silently treated as a complete capture.

All results are from a private Docker LAN with DHT disabled, one participating observer, no models and no inference. This does not measure WAN/DHT traffic, llama.cpp RPC tensor exchange, or managed PostgreSQL/NATS traffic. Received snapshots can be old or forked; differences between them are not proof of accepted ledger operations, original authorship, or successful heartbeat-leader cleanup.

## Reproduce

Use the pinned LocalAI image and build the [observer](control-observer/README.md), then reproduce the recorded configuration with a new output label:

```sh
python3 research/localai/run_control_messages.py --label lifecycle-repeat-001 --peers 32 --peer-cpus 0.5
python3 research/localai/analyze_control_messages.py research/localai/results/lifecycle-repeat-001
```

Use `--peers 4` for a smaller capture of the same stages. The default steady window includes periodic cleanup; reduce `--steady-seconds` only if that maintenance window is not needed. Membership and cleanup-role identities vary because the token, service IDs and peer keys are freshly generated; the runner has no identity seed interface.
