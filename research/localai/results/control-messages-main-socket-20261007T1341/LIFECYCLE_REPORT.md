# LocalAI control-message lifecycle observations

Run: `control-messages-main-socket-20261007T1341`  
Source revision: `a3d555653c0cbfd73ab59ab15beab80903e96484`
Run status: **failed**.


**Partial capture:** the lifecycle experiment did not complete successfully. Full-peer membership control failed in join-32
## Capture totals

- Gossip envelope receptions: **1030**.
- Unique block snapshots seen by observers: **551**; repeated snapshot receptions: **479**.
- Peer connection/disconnection events: **64 / 64**.
- Captured mDNS packets: **260 queries / 2828 responses**.
- Empty-model controls: **passed** across 32 probes; gateway service probe: **not completed or failed**.
- One envelope carries a whole ledger block; the records listed below are state differences between observed snapshots, not separate packets.

## Lifecycle catalogue

| Order | Lifecycle category | Observed records/events | What the trace can show |
|---:|---|---|---|
| 1 | **discovery_and_connections** | listen address active at observer registration: 4; mDNS queries: 260; mDNS responses: 2828; peer connections: 64 | Peer discovery and transport connection events (mDNS is visible only if captured; DHT traffic may be outside this observer). |
| 2 | **membership_and_authorization** | none recorded | User/peer registration records used by EdgeVPN admission checks. |
| 3 | **service_registration_and_worker_discovery** | `localai-control-38f60ec7bec5_federated` added: 238; `localai-control-38f60ec7bec5_federated` changed: 215; `localai-control-38f60ec7bec5_federated` present_at_first_observation: 2; `services` added: 257; `services` present_at_first_observation: 2 | EdgeVPN services and LocalAI worker advertisement records. |
| 4 | **health_and_keepalive** | `healthcheck` added: 252; `healthcheck` changed: 172; `healthcheck` present_at_first_observation: 2 | Periodic health heartbeats and refreshed LastSeen fields. |
| 5 | **shared_model_state** | `shared_state` added: 16 | Federated application state such as synchronized model-name lists, when enabled. |
| 6 | **ledger_replication** | gossip receptions: 1030 | Encrypted gossip carries a compressed full-block ledger snapshot; the envelope can contain several record buckets. |
| 7 | **churn_and_expiry** | `healthcheck` removed: 222; `localai-control-38f60ec7bec5_federated` removed: 208; `services` removed: 227; `shared_state` removed: 15; listen address closed: 3; peer disconnections: 64 | Observed state disappearance, branch/resync differences, stale/future records, and peer disconnects; none is an explicit leave message by itself. |
| 8 | **unknown_or_unclassified** | none recorded | Unknown events or buckets retained for inspection rather than silently discarded. |

## Source-derived message catalogue

These entries describe code paths and expected message types; their runtime status is shown separately.

| Source lifecycle stage | Message / record | Bucket | Runtime evidence |
|---|---|---|---|
| 1. Discovery and network join | mDNS peer discovery and libp2p connection setup | — | mDNS packets observed |
| 1. Discovery and network join | GossipSub room join, subscription and mesh maintenance | — | not directly visible in ledger capture |
| 1. Discovery and network join | Explicit membership/join ledger record | — | source says no explicit record exists |
| 2. Initial service advertisement and authorization | EdgeVPN service route | services | bucket present in observed snapshots: services |
| 2. Initial service advertisement and authorization | LocalAI federated/worker service advertisement | <P2PNetworkID>_federated, <P2PNetworkID>_worker, or <P2PNetworkID>_mlx_worker; if P2PNetworkID is empty: federated, worker, or mlx_worker | bucket present in observed snapshots: localai-control-38f60ec7bec5_federated |
| 2. Initial service advertisement and authorization | Peer authorization/user record | users | no matching bucket observed |
| 3. Steady-state liveness, service discovery and ledger sync | EdgeVPN peer heartbeat | healthcheck | bucket present in observed snapshots: healthcheck |
| 3. Steady-state liveness, service discovery and ledger sync | LocalAI service heartbeat/LastSeen refresh | <P2PNetworkID>_<service ID>; current federation bucket is <P2PNetworkID>_federated; other defined service IDs are <P2PNetworkID>_worker and <P2PNetworkID>_mlx_worker | bucket present in observed snapshots: localai-control-38f60ec7bec5_federated |
| 3. Steady-state liveness, service discovery and ledger sync | Full-ledger synchronization | — | 1030 gossip envelope receptions observed |
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
| service_registration_and_worker_discovery | `localai-control-38f60ec7bec5_federated` / added | LocalAI federated/worker service advertisement (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 32, not_direct_child_of_previous_observed_block: 206 | 238 | `observer.jsonl:93` (`2d7b7a6fa5c9…` ) [block index 14; direct_child_or_initial] |
| service_registration_and_worker_discovery | `localai-control-38f60ec7bec5_federated` / changed | LocalAI federated/worker service advertisement (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 199, not_direct_child_of_previous_observed_block: 16 | 215 | `observer.jsonl:46` (`7bef982aec2a…` ) [block index 7; direct_child_or_initial] |
| service_registration_and_worker_discovery | `localai-control-38f60ec7bec5_federated` / present_at_first_observation | LocalAI federated/worker service advertisement (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 1, not_direct_child_of_previous_observed_block: 1 | 2 | `observer.jsonl:27` (`1c84e41b544d…` ) [block index 4; direct_child_or_initial] |
| service_registration_and_worker_discovery | `services` / added | EdgeVPN service route (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 34, not_direct_child_of_previous_observed_block: 223 | 257 | `observer.jsonl:92` (`f7bd871d8da7…` ) [block index 13; direct_child_or_initial] |
| service_registration_and_worker_discovery | `services` / present_at_first_observation | EdgeVPN service route (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 1, not_direct_child_of_previous_observed_block: 1 | 2 | `observer.jsonl:27` (`1c84e41b544d…` ) [block index 4; direct_child_or_initial] |
| health_and_keepalive | `healthcheck` / added | EdgeVPN peer heartbeat (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 33, not_direct_child_of_previous_observed_block: 219 | 252 | `observer.jsonl:87` (`c17041ffef99…` ) [block index 12; direct_child_or_initial] |
| health_and_keepalive | `healthcheck` / changed | EdgeVPN peer heartbeat (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 151, not_direct_child_of_previous_observed_block: 21 | 172 | `observer.jsonl:39` (`603738538091…` ) [block index 5; direct_child_or_initial] |
| health_and_keepalive | `healthcheck` / present_at_first_observation | EdgeVPN peer heartbeat (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 1, not_direct_child_of_previous_observed_block: 1 | 2 | `observer.jsonl:27` (`1c84e41b544d…` ) [block index 4; direct_child_or_initial] |
| shared_model_state | `shared_state` / added | Shared model-name list (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 1, not_direct_child_of_previous_observed_block: 15 | 16 | `observer.jsonl:86` (`c1f11f713fd7…` ) [block index 11; direct_child_or_initial] |
| churn_and_expiry | `healthcheck` / removed | EdgeVPN peer heartbeat (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 1, not_direct_child_of_previous_observed_block: 221 | 222 | `observer.jsonl:213` (`8e744e96dbaf…` ) [block index 1; not_direct_child_of_previous_observed_block] |
| churn_and_expiry | `localai-control-38f60ec7bec5_federated` / removed | LocalAI federated/worker service advertisement (application ledger record inside full-block envelope); lineage counts: not_direct_child_of_previous_observed_block: 208 | 208 | `observer.jsonl:213` (`8e744e96dbaf…` ) [block index 1; not_direct_child_of_previous_observed_block] |
| churn_and_expiry | `services` / removed | EdgeVPN service route (application ledger record inside full-block envelope); lineage counts: not_direct_child_of_previous_observed_block: 227 | 227 | `observer.jsonl:213` (`8e744e96dbaf…` ) [block index 1; not_direct_child_of_previous_observed_block] |
| churn_and_expiry | `shared_state` / removed | Shared model-name list (application ledger record inside full-block envelope); lineage counts: not_direct_child_of_previous_observed_block: 15 | 15 | `observer.jsonl:213` (`8e744e96dbaf…` ) [block index 1; not_direct_child_of_previous_observed_block] |

## Bucket-level state transitions

| Bucket | Observation | Count | Records removed from observed state | Snapshot lineage | Example raw pointer |
|---|---|---:|---:|---|---|
| `healthcheck` | absent from a later snapshot | 13 | 54 | direct_child_or_initial: 1, not_direct_child_of_previous_observed_block: 12 | `observer.jsonl:214` |
| `localai-control-38f60ec7bec5_federated` | absent from a later snapshot | 21 | 44 | not_direct_child_of_previous_observed_block: 21 | `observer.jsonl:213` |
| `services` | absent from a later snapshot | 24 | 56 | not_direct_child_of_previous_observed_block: 24 | `observer.jsonl:213` |
| `shared_state` | absent from a later snapshot | 15 | 15 | not_direct_child_of_previous_observed_block: 15 | `observer.jsonl:213` |
A whole-bucket disappearance is inferred from consecutive received snapshots; it is not a distinct DELETE packet or proof of an explicit peer leave.

## mDNS TTL-zero resource records

No TTL-zero DNS resource records were observed; the capture cannot infer a LocalAI leave from this absence.

## Freshness checks

| Bucket | Status | Count | Example |
|---|---|---:|---|
| `healthcheck` | fresh | 10563 | `observer.jsonl:27` |
| `localai-control-38f60ec7bec5_federated` | fresh | 10230 | `observer.jsonl:27` |

## Membership and resource samples

Target service peers: **60**; actual peers: **16**.

| Phase | Samples | Fresh nodes per witness (median; range) | Listed API nodes (median; range) | Final stage check | Lowest host MemAvailable |
|---|---:|---|---|---|---:|
| bootstrap | 6 | 1.0 (0–1) | 1.0 (0–1) | pass | 19.90 GiB |
| join-16 | 7 | 16.0 (0–16) | 16.0 (0–16) | pass | 18.86 GiB |
| join-2 | 6 | 2.0 (0–2) | 2.0 (0–2) | pass | 19.82 GiB |
| join-32 | 8 | 31.0 (0–32) | 32.0 (0–32) | not_recorded | 17.13 GiB |
| join-8 | 6 | 8.0 (0–8) | 8.0 (0–8) | pass | 19.53 GiB |

Highest sampled container memory peaks:
- `join-32` / `localai-control-38f60ec7bec5-p01`: median 89.4 MiB, sampled maximum 90.1 MiB; median CPU 2.15%.
- `join-32` / `localai-control-38f60ec7bec5-p08`: median 88.5 MiB, sampled maximum 89.0 MiB; median CPU 2.02%.
- `join-32` / `localai-control-38f60ec7bec5-p06`: median 88.4 MiB, sampled maximum 89.0 MiB; median CPU 1.89%.
- `join-32` / `localai-control-38f60ec7bec5-p07`: median 88.5 MiB, sampled maximum 89.0 MiB; median CPU 2.05%.
- `join-32` / `localai-control-38f60ec7bec5-p31`: median 88.3 MiB, sampled maximum 88.7 MiB; median CPU 2.14%.

## Interpretation limits

- One observer's local network vantage; relay IDs do not identify the original ledger publisher.
- The lab disables DHT; no WAN behavior is measured. If mDNS capture is absent, discovery traffic may be outside this trace.
- The run uses model-free peers. The gateway /v1/models probe is not model inference and no sharded inference workload is measured.
- Observed removals are ledger state differences, not explicit leave messages. Peer disconnect, service loss, and eventual record expiry are reported as separate evidence.
- A record or bucket removal means it was absent from a later snapshot received by this observer. That block may be from a stale, forked, or otherwise nonaccepted branch; block index and parent-hash lineage do not prove canonical acceptance. This trace cannot establish authoritative deletion, explicit departure, or prove that a leader-driven heartbeat-bucket scrub did or did not occur.
- Repeated full snapshots can contain unchanged records. Counts of changed records are observed state differences, not record-level packet counts.
