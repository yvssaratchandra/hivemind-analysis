# LocalAI control-message lifecycle observations

Run: `control-messages-socket-rejoin-pilot-20261007T1340`  
Source revision: `a3d555653c0cbfd73ab59ab15beab80903e96484`
Run status: **passed**.

## Capture totals

- Gossip envelope receptions: **65**.
- Unique block snapshots seen by observers: **38**; repeated snapshot receptions: **27**.
- Peer connection/disconnection events: **8 / 8**.
- Captured mDNS packets: **54 queries / 110 responses**.
- Empty-model controls: **passed** across 3 probes; gateway service probe: **passed**.
- One envelope carries a whole ledger block; the records listed below are state differences between observed snapshots, not separate packets.

## Lifecycle catalogue

| Order | Lifecycle category | Observed records/events | What the trace can show |
|---:|---|---|---|
| 1 | **discovery_and_connections** | listen address active at observer registration: 4; mDNS queries: 54; mDNS responses: 110; peer connections: 8 | Peer discovery and transport connection events (mDNS is visible only if captured; DHT traffic may be outside this observer). |
| 2 | **membership_and_authorization** | `users` added: 1; `users` changed: 7 | User/peer registration records used by EdgeVPN admission checks. |
| 3 | **service_registration_and_worker_discovery** | `localai-control-7cb6742d52ac_federated` added: 2; `localai-control-7cb6742d52ac_federated` changed: 10; `localai-control-7cb6742d52ac_federated` present_at_first_observation: 1; `services` added: 2; `services` present_at_first_observation: 1 | EdgeVPN services and LocalAI worker advertisement records. |
| 4 | **health_and_keepalive** | `healthcheck` added: 3; `healthcheck` changed: 11; `healthcheck` present_at_first_observation: 1 | Periodic health heartbeats and refreshed LastSeen fields. |
| 5 | **shared_model_state** | `shared_state` added: 1 | Federated application state such as synchronized model-name lists, when enabled. |
| 6 | **ledger_replication** | gossip receptions: 65 | Encrypted gossip carries a compressed full-block ledger snapshot; the envelope can contain several record buckets. |
| 7 | **churn_and_expiry** | listen address closed: 3; peer disconnections: 8 | Observed state disappearance, branch/resync differences, stale/future records, and peer disconnects; none is an explicit leave message by itself. |
| 8 | **unknown_or_unclassified** | none recorded | Unknown events or buckets retained for inspection rather than silently discarded. |

## Source-derived message catalogue

These entries describe code paths and expected message types; their runtime status is shown separately.

| Source lifecycle stage | Message / record | Bucket | Runtime evidence |
|---|---|---|---|
| 1. Discovery and network join | mDNS peer discovery and libp2p connection setup | — | mDNS packets observed |
| 1. Discovery and network join | GossipSub room join, subscription and mesh maintenance | — | not directly visible in ledger capture |
| 1. Discovery and network join | Explicit membership/join ledger record | — | source says no explicit record exists |
| 2. Initial service advertisement and authorization | EdgeVPN service route | services | bucket present in observed snapshots: services |
| 2. Initial service advertisement and authorization | LocalAI federated/worker service advertisement | <P2PNetworkID>_federated, <P2PNetworkID>_worker, or <P2PNetworkID>_mlx_worker; if P2PNetworkID is empty: federated, worker, or mlx_worker | bucket present in observed snapshots: localai-control-7cb6742d52ac_federated |
| 2. Initial service advertisement and authorization | Peer authorization/user record | users | bucket present in observed snapshots: users |
| 3. Steady-state liveness, service discovery and ledger sync | EdgeVPN peer heartbeat | healthcheck | bucket present in observed snapshots: healthcheck |
| 3. Steady-state liveness, service discovery and ledger sync | LocalAI service heartbeat/LastSeen refresh | <P2PNetworkID>_<service ID>; current federation bucket is <P2PNetworkID>_federated; other defined service IDs are <P2PNetworkID>_worker and <P2PNetworkID>_mlx_worker | bucket present in observed snapshots: localai-control-7cb6742d52ac_federated |
| 3. Steady-state liveness, service discovery and ledger sync | Full-ledger synchronization | — | 65 gossip envelope receptions observed |
| 4. Federated selection and service access | Worker selection | — | not directly visible in ledger capture |
| 4. Federated selection and service access | Service stream open and authorization check | — | not directly visible in ledger capture |
| 5. Optional model-state synchronization (federated application mode only) | Shared model-name list | shared_state | bucket present in observed snapshots: shared_state |
| 6. Graceful stop, crash expiry and rejoin | Explicit leave/de-registration | — | not directly visible in ledger capture |
| 6. Graceful stop, crash expiry and rejoin | LocalAI service expiry | — | not directly visible in ledger capture |
| 6. Graceful stop, crash expiry and rejoin | EdgeVPN heartbeat expiry and scrub | — | not directly visible in ledger capture |
| 6. Graceful stop, crash expiry and rejoin | Rejoin after restart | — | not directly visible in ledger capture |
| 7. Periodic heartbeat-bucket maintenance leader | Deterministic leader calculation | — | not directly visible in ledger capture |

## Record examples

| Category | Bucket / action | Meaning and snapshot lineage | Count | Example raw pointer |
|---|---|---|---:|---|
| membership_and_authorization | `users` / added | Peer authorization/user record (application ledger record inside full-block envelope; conditionally emitted by LocalAI gateway/tunnel listener); lineage counts: direct_child_or_initial: 1 | 1 | `observer.jsonl:148` (`0b6c5c0d402b…` ) [block index 17; direct_child_or_initial] |
| membership_and_authorization | `users` / changed | Peer authorization/user record (application ledger record inside full-block envelope; conditionally emitted by LocalAI gateway/tunnel listener); lineage counts: direct_child_or_initial: 7 | 7 | `observer.jsonl:164` (`c449c48a6bba…` ) [block index 21; direct_child_or_initial] |
| service_registration_and_worker_discovery | `localai-control-7cb6742d52ac_federated` / added | LocalAI federated/worker service advertisement (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 2 | 2 | `observer.jsonl:90` (`5784e093cb6b…` ) [block index 8; direct_child_or_initial] |
| service_registration_and_worker_discovery | `localai-control-7cb6742d52ac_federated` / changed | LocalAI federated/worker service advertisement (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 10 | 10 | `observer.jsonl:61` (`3fc4b9059285…` ) [block index 6; direct_child_or_initial] |
| service_registration_and_worker_discovery | `localai-control-7cb6742d52ac_federated` / present_at_first_observation | LocalAI federated/worker service advertisement (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 1 | 1 | `observer.jsonl:34` (`41661b88138f…` ) [block index 4; direct_child_or_initial] |
| service_registration_and_worker_discovery | `services` / added | EdgeVPN service route (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 2 | 2 | `observer.jsonl:92` (`9dcf5d512274…` ) [block index 10; direct_child_or_initial] |
| service_registration_and_worker_discovery | `services` / present_at_first_observation | EdgeVPN service route (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 1 | 1 | `observer.jsonl:34` (`41661b88138f…` ) [block index 4; direct_child_or_initial] |
| health_and_keepalive | `healthcheck` / added | EdgeVPN peer heartbeat (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 3 | 3 | `observer.jsonl:91` (`a0b235fe3643…` ) [block index 9; direct_child_or_initial] |
| health_and_keepalive | `healthcheck` / changed | EdgeVPN peer heartbeat (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 11 | 11 | `observer.jsonl:56` (`58ce74e80cda…` ) [block index 5; direct_child_or_initial] |
| health_and_keepalive | `healthcheck` / present_at_first_observation | EdgeVPN peer heartbeat (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 1 | 1 | `observer.jsonl:34` (`41661b88138f…` ) [block index 4; direct_child_or_initial] |
| shared_model_state | `shared_state` / added | Shared model-name list (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 1 | 1 | `observer.jsonl:150` (`7510a4c36e39…` ) [block index 19; direct_child_or_initial] |

## Bucket-level state transitions

No complete bucket disappearance was visible between the unique snapshots received by this observer.

## mDNS TTL-zero resource records

No TTL-zero DNS resource records were observed; the capture cannot infer a LocalAI leave from this absence.

## Freshness checks

| Bucket | Status | Count | Example |
|---|---|---:|---|
| `healthcheck` | fresh | 102 | `observer.jsonl:34` |
| `localai-control-7cb6742d52ac_federated` | fresh | 64 | `observer.jsonl:34` |
| `localai-control-7cb6742d52ac_federated` | stale | 15 | `observer.jsonl:178` |

## Membership and resource samples

Target service peers: **2**; actual peers: **2**.

| Phase | Samples | Fresh nodes per witness (median; range) | Listed API nodes (median; range) | Final stage check | Lowest host MemAvailable |
|---|---:|---|---|---|---:|
| bootstrap | 4 | 1.0 (0–1) | 1.0 (0–1) | pass | 20.00 GiB |
| join-2 | 4 | 2.0 (0–2) | 2.0 (0–2) | pass | 20.02 GiB |
| peer-departure | 7 | 1.0 (1–2) | 2.0 (2–2) | pass | 19.86 GiB |
| peer-rejoin | 4 | 2.0 (0–2) | 2.0 (0–3) | pass | 19.71 GiB |
| steady-maintenance | 3 | 2.0 (2–2) | 2.0 (2–2) | pass | 20.05 GiB |

Highest sampled container memory peaks:
- `peer-departure` / `localai-control-7cb6742d52ac-p01`: median 69.7 MiB, sampled maximum 74.1 MiB; median CPU 0.15%.
- `peer-rejoin` / `localai-control-7cb6742d52ac-p01`: median 73.8 MiB, sampled maximum 73.9 MiB; median CPU 0.12%.
- `steady-maintenance` / `localai-control-7cb6742d52ac-p01`: median 65.7 MiB, sampled maximum 65.7 MiB; median CPU 0.14%.
- `join-2` / `localai-control-7cb6742d52ac-p01`: median 63.6 MiB, sampled maximum 63.9 MiB; median CPU 0.16%.
- `bootstrap` / `localai-control-7cb6742d52ac-p01`: median 63.1 MiB, sampled maximum 63.7 MiB; median CPU 0.26%.

## Interpretation limits

- One observer's local network vantage; relay IDs do not identify the original ledger publisher.
- The lab disables DHT; no WAN behavior is measured. If mDNS capture is absent, discovery traffic may be outside this trace.
- The run uses model-free peers. The gateway /v1/models probe is not model inference and no sharded inference workload is measured.
- Observed removals are ledger state differences, not explicit leave messages. Peer disconnect, service loss, and eventual record expiry are reported as separate evidence.
- A record or bucket removal means it was absent from a later snapshot received by this observer. That block may be from a stale, forked, or otherwise nonaccepted branch; block index and parent-hash lineage do not prove canonical acceptance. This trace cannot establish authoritative deletion, explicit departure, or prove that a leader-driven heartbeat-bucket scrub did or did not occur.
- Repeated full snapshots can contain unchanged records. Counts of changed records are observed state differences, not record-level packet counts.
