# LocalAI control-message lifecycle observations

Run: `control-messages-pilot-20261007T1318`  
Source revision: `a3d555653c0cbfd73ab59ab15beab80903e96484`

## Capture totals

- Gossip envelope receptions: **7**.
- Unique block snapshots seen by observers: **5**; repeated snapshot receptions: **2**.
- Peer connection/disconnection events: **2 / 2**.
- Captured mDNS packets: **16 queries / 27 responses**.
- Empty-model controls: **passed** across 1 probes; gateway service probe: **not completed or failed**.
- One envelope carries a whole ledger block; the records listed below are state differences between observed snapshots, not separate packets.

## Lifecycle catalogue

| Order | Lifecycle category | Observed records/events | What the trace can show |
|---:|---|---|---|
| 1 | **discovery_and_connections** | listen address active at observer registration: 4; mDNS queries: 16; mDNS responses: 27; peer connections: 2 | Peer discovery and transport connection events (mDNS is visible only if captured; DHT traffic may be outside this observer). |
| 2 | **membership_and_authorization** | none recorded | User/peer registration records used by EdgeVPN admission checks. |
| 3 | **service_registration_and_worker_discovery** | `localai-control-2302e21fe525_federated` changed: 2; `localai-control-2302e21fe525_federated` present_at_first_observation: 1; `services` present_at_first_observation: 1 | EdgeVPN services and LocalAI worker advertisement records. |
| 4 | **health_and_keepalive** | `healthcheck` changed: 2; `healthcheck` present_at_first_observation: 1 | Periodic health heartbeats and refreshed LastSeen fields. |
| 5 | **shared_model_state** | none recorded | Federated application state such as synchronized model-name lists, when enabled. |
| 6 | **ledger_replication** | gossip receptions: 7 | Encrypted gossip carries a compressed full-block ledger snapshot; the envelope can contain several record buckets. |
| 7 | **churn_and_expiry** | listen address closed: 3; peer disconnections: 2 | Observed state disappearance, branch/resync differences, stale/future records, and peer disconnects; none is an explicit leave message by itself. |
| 8 | **unknown_or_unclassified** | none recorded | Unknown events or buckets retained for inspection rather than silently discarded. |

## Source-derived message catalogue

These entries describe code paths and expected message types; their runtime status is shown separately.

| Source lifecycle stage | Message / record | Bucket | Runtime evidence |
|---|---|---|---|
| 1. Discovery and network join | mDNS peer discovery and libp2p connection setup | — | mDNS packets observed |
| 1. Discovery and network join | GossipSub room join, subscription and mesh maintenance | — | not directly visible in ledger capture |
| 1. Discovery and network join | Explicit membership/join ledger record | — | source says no explicit record exists |
| 2. Initial service advertisement and authorization | EdgeVPN service route | services | bucket present in observed snapshots: services |
| 2. Initial service advertisement and authorization | LocalAI federated/worker service advertisement | <P2PNetworkID>_federated, <P2PNetworkID>_worker, or <P2PNetworkID>_mlx_worker; if P2PNetworkID is empty: federated, worker, or mlx_worker | bucket present in observed snapshots: localai-control-2302e21fe525_federated |
| 2. Initial service advertisement and authorization | Peer authorization/user record | users | no matching bucket observed |
| 3. Steady-state liveness, service discovery and ledger sync | EdgeVPN peer heartbeat | healthcheck | bucket present in observed snapshots: healthcheck |
| 3. Steady-state liveness, service discovery and ledger sync | LocalAI service heartbeat/LastSeen refresh | <P2PNetworkID>_<service ID>; current federation bucket is <P2PNetworkID>_federated; other defined service IDs are <P2PNetworkID>_worker and <P2PNetworkID>_mlx_worker | bucket present in observed snapshots: localai-control-2302e21fe525_federated |
| 3. Steady-state liveness, service discovery and ledger sync | Full-ledger synchronization | — | 7 gossip envelope receptions observed |
| 4. Federated selection and service access | Worker selection | — | not directly visible in ledger capture |
| 4. Federated selection and service access | Service stream open and authorization check | — | not directly visible in ledger capture |
| 5. Optional model-state synchronization (federated application mode only) | Shared model-name list | shared_state | no matching bucket observed |
| 6. Graceful stop, crash expiry and rejoin | Explicit leave/de-registration | — | not directly visible in ledger capture |
| 6. Graceful stop, crash expiry and rejoin | LocalAI service expiry | — | not directly visible in ledger capture |
| 6. Graceful stop, crash expiry and rejoin | EdgeVPN heartbeat expiry and scrub | — | not directly visible in ledger capture |
| 6. Graceful stop, crash expiry and rejoin | Rejoin after restart | — | not directly visible in ledger capture |
| 7. Periodic heartbeat-bucket maintenance leader | Deterministic leader calculation | — | not directly visible in ledger capture |

## Record examples

| Category | Bucket / action | Meaning and snapshot lineage | Count | Example raw pointer |
|---|---|---|---:|---|
| service_registration_and_worker_discovery | `localai-control-2302e21fe525_federated` / changed | LocalAI federated/worker service advertisement (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 2 | 2 | `observer.jsonl:44` (`1da650bf81c7…` ) [block index 4; direct_child_or_initial] |
| service_registration_and_worker_discovery | `localai-control-2302e21fe525_federated` / present_at_first_observation | LocalAI federated/worker service advertisement (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 1 | 1 | `observer.jsonl:31` (`1b0f1ed218e2…` ) [block index 3; direct_child_or_initial] |
| service_registration_and_worker_discovery | `services` / present_at_first_observation | EdgeVPN service route (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 1 | 1 | `observer.jsonl:31` (`1b0f1ed218e2…` ) [block index 3; direct_child_or_initial] |
| health_and_keepalive | `healthcheck` / changed | EdgeVPN peer heartbeat (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 2 | 2 | `observer.jsonl:45` (`7e1b0cd3719a…` ) [block index 5; direct_child_or_initial] |
| health_and_keepalive | `healthcheck` / present_at_first_observation | EdgeVPN peer heartbeat (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 1 | 1 | `observer.jsonl:31` (`1b0f1ed218e2…` ) [block index 3; direct_child_or_initial] |

## Bucket-level state transitions

No complete bucket disappearance was visible between the unique snapshots received by this observer.

## mDNS TTL-zero resource records

No TTL-zero DNS resource records were observed; the capture cannot infer a LocalAI leave from this absence.

## Freshness checks

| Bucket | Status | Count | Example |
|---|---|---:|---|
| `healthcheck` | fresh | 5 | `observer.jsonl:31` |
| `localai-control-2302e21fe525_federated` | fresh | 5 | `observer.jsonl:31` |

## Membership and resource samples

Target service peers: **4**; actual peers: **not recorded**.

| Phase | Samples | Fresh nodes per witness (median; range) | Listed API nodes (median; range) | Final stage check | Lowest host MemAvailable |
|---|---:|---|---|---|---:|
| bootstrap | 5 | 1.0 (0–1) | 1.0 (0–1) | pass | 20.48 GiB |

Highest sampled container memory peaks:
- `bootstrap` / `localai-control-2302e21fe525-p01`: median 58.4 MiB, sampled maximum 58.9 MiB; median CPU 0.10%.
- `bootstrap` / `localai-control-2302e21fe525-observer`: median 23.3 MiB, sampled maximum 23.8 MiB; median CPU 0.09%.

## Interpretation limits

- **Partial/failed run:** process exit code 1 (`runner_failure_after_bootstrap`); lifecycle observations below are from the captured prefix, not a completed experiment.
- One observer's local network vantage; relay IDs do not identify the original ledger publisher.
- The lab disables DHT; no WAN behavior is measured. If mDNS capture is absent, discovery traffic may be outside this trace.
- The run uses model-free peers. The gateway /v1/models probe is not model inference and no sharded inference workload is measured.
- Observed removals are ledger state differences, not explicit leave messages. Peer disconnect, service loss, and eventual record expiry are reported as separate evidence.
- A record or bucket removal means it was absent from a later snapshot received by this observer. That block may be from a stale, forked, or otherwise nonaccepted branch; block index and parent-hash lineage do not prove canonical acceptance. This trace cannot establish authoritative deletion, explicit departure, or prove that a leader-driven heartbeat-bucket scrub did or did not occur.
- Repeated full snapshots can contain unchanged records. Counts of changed records are observed state differences, not record-level packet counts.
