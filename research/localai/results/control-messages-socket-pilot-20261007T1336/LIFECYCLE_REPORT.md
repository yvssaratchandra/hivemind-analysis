# LocalAI control-message lifecycle observations

Run: `control-messages-socket-pilot-20261007T1336`  
Source revision: `a3d555653c0cbfd73ab59ab15beab80903e96484`
Run status: **failed**.


**Partial capture:** the lifecycle experiment did not complete successfully. Docker log stream failed (400): {"message":"invalid value for \"since\": invalid timestamp \"2026-10-07T13:37:40.121611+00:00\": invalid seconds \"2026-10-07T13:37:40\": invalid syntax"}

## Capture totals

- Gossip envelope receptions: **128**.
- Unique block snapshots seen by observers: **69**; repeated snapshot receptions: **59**.
- Peer connection/disconnection events: **12 / 12**.
- Captured mDNS packets: **65 queries / 187 responses**.
- Empty-model controls: **passed** across 4 probes; gateway service probe: **passed**.
- One envelope carries a whole ledger block; the records listed below are state differences between observed snapshots, not separate packets.

## Lifecycle catalogue

| Order | Lifecycle category | Observed records/events | What the trace can show |
|---:|---|---|---|
| 1 | **discovery_and_connections** | listen address active at observer registration: 4; mDNS queries: 65; mDNS responses: 187; peer connections: 12 | Peer discovery and transport connection events (mDNS is visible only if captured; DHT traffic may be outside this observer). |
| 2 | **membership_and_authorization** | `users` added: 1; `users` changed: 7 | User/peer registration records used by EdgeVPN admission checks. |
| 3 | **service_registration_and_worker_discovery** | `localai-control-600b987d09c8_federated` added: 3; `localai-control-600b987d09c8_federated` changed: 27; `localai-control-600b987d09c8_federated` present_at_first_observation: 1; `services` added: 3; `services` present_at_first_observation: 1 | EdgeVPN services and LocalAI worker advertisement records. |
| 4 | **health_and_keepalive** | `healthcheck` added: 4; `healthcheck` changed: 22; `healthcheck` present_at_first_observation: 1 | Periodic health heartbeats and refreshed LastSeen fields. |
| 5 | **shared_model_state** | `shared_state` added: 1 | Federated application state such as synchronized model-name lists, when enabled. |
| 6 | **ledger_replication** | gossip receptions: 128 | Encrypted gossip carries a compressed full-block ledger snapshot; the envelope can contain several record buckets. |
| 7 | **churn_and_expiry** | listen address closed: 3; peer disconnections: 12 | Observed state disappearance, branch/resync differences, stale/future records, and peer disconnects; none is an explicit leave message by itself. |
| 8 | **unknown_or_unclassified** | none recorded | Unknown events or buckets retained for inspection rather than silently discarded. |

## Source-derived message catalogue

These entries describe code paths and expected message types; their runtime status is shown separately.

| Source lifecycle stage | Message / record | Bucket | Runtime evidence |
|---|---|---|---|
| 1. Discovery and network join | mDNS peer discovery and libp2p connection setup | — | mDNS packets observed |
| 1. Discovery and network join | GossipSub room join, subscription and mesh maintenance | — | not directly visible in ledger capture |
| 1. Discovery and network join | Explicit membership/join ledger record | — | source says no explicit record exists |
| 2. Initial service advertisement and authorization | EdgeVPN service route | services | bucket present in observed snapshots: services |
| 2. Initial service advertisement and authorization | LocalAI federated/worker service advertisement | <P2PNetworkID>_federated, <P2PNetworkID>_worker, or <P2PNetworkID>_mlx_worker; if P2PNetworkID is empty: federated, worker, or mlx_worker | bucket present in observed snapshots: localai-control-600b987d09c8_federated |
| 2. Initial service advertisement and authorization | Peer authorization/user record | users | bucket present in observed snapshots: users |
| 3. Steady-state liveness, service discovery and ledger sync | EdgeVPN peer heartbeat | healthcheck | bucket present in observed snapshots: healthcheck |
| 3. Steady-state liveness, service discovery and ledger sync | LocalAI service heartbeat/LastSeen refresh | <P2PNetworkID>_<service ID>; current federation bucket is <P2PNetworkID>_federated; other defined service IDs are <P2PNetworkID>_worker and <P2PNetworkID>_mlx_worker | bucket present in observed snapshots: localai-control-600b987d09c8_federated |
| 3. Steady-state liveness, service discovery and ledger sync | Full-ledger synchronization | — | 128 gossip envelope receptions observed |
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
| membership_and_authorization | `users` / added | Peer authorization/user record (application ledger record inside full-block envelope; conditionally emitted by LocalAI gateway/tunnel listener); lineage counts: direct_child_or_initial: 1 | 1 | `observer.jsonl:277` (`5692beb98c4f…` ) [block index 45; direct_child_or_initial] |
| membership_and_authorization | `users` / changed | Peer authorization/user record (application ledger record inside full-block envelope; conditionally emitted by LocalAI gateway/tunnel listener); lineage counts: direct_child_or_initial: 7 | 7 | `observer.jsonl:291` (`33dba078364f…` ) [block index 48; direct_child_or_initial] |
| service_registration_and_worker_discovery | `localai-control-600b987d09c8_federated` / added | LocalAI federated/worker service advertisement (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 3 | 3 | `observer.jsonl:79` (`e40e43a276f4…` ) [block index 8; direct_child_or_initial] |
| service_registration_and_worker_discovery | `localai-control-600b987d09c8_federated` / changed | LocalAI federated/worker service advertisement (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 27 | 27 | `observer.jsonl:45` (`69432e735b91…` ) [block index 5; direct_child_or_initial] |
| service_registration_and_worker_discovery | `localai-control-600b987d09c8_federated` / present_at_first_observation | LocalAI federated/worker service advertisement (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 1 | 1 | `observer.jsonl:27` (`5fb83d849fe5…` ) [block index 4; direct_child_or_initial] |
| service_registration_and_worker_discovery | `services` / added | EdgeVPN service route (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 3 | 3 | `observer.jsonl:82` (`52002b3ef9ea…` ) [block index 10; direct_child_or_initial] |
| service_registration_and_worker_discovery | `services` / present_at_first_observation | EdgeVPN service route (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 1 | 1 | `observer.jsonl:27` (`5fb83d849fe5…` ) [block index 4; direct_child_or_initial] |
| health_and_keepalive | `healthcheck` / added | EdgeVPN peer heartbeat (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 4 | 4 | `observer.jsonl:81` (`d4b32a523c5a…` ) [block index 9; direct_child_or_initial] |
| health_and_keepalive | `healthcheck` / changed | EdgeVPN peer heartbeat (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 22 | 22 | `observer.jsonl:54` (`49423786a5b8…` ) [block index 6; direct_child_or_initial] |
| health_and_keepalive | `healthcheck` / present_at_first_observation | EdgeVPN peer heartbeat (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 1 | 1 | `observer.jsonl:27` (`5fb83d849fe5…` ) [block index 4; direct_child_or_initial] |
| shared_model_state | `shared_state` / added | Shared model-name list (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 1 | 1 | `observer.jsonl:142` (`21858b657656…` ) [block index 17; direct_child_or_initial] |

## Bucket-level state transitions

No complete bucket disappearance was visible between the unique snapshots received by this observer.

## mDNS TTL-zero resource records

No TTL-zero DNS resource records were observed; the capture cannot infer a LocalAI leave from this absence.

## Freshness checks

| Bucket | Status | Count | Example |
|---|---|---:|---|
| `healthcheck` | fresh | 264 | `observer.jsonl:27` |
| `localai-control-600b987d09c8_federated` | fresh | 217 | `observer.jsonl:27` |
| `localai-control-600b987d09c8_federated` | stale | 19 | `observer.jsonl:320` |

## Membership and resource samples

Target service peers: **4**; actual peers: **4**.

| Phase | Samples | Fresh nodes per witness (median; range) | Listed API nodes (median; range) | Final stage check | Lowest host MemAvailable |
|---|---:|---|---|---|---:|
| bootstrap | 5 | 1.0 (0–1) | 1.0 (0–1) | pass | 20.27 GiB |
| join-2 | 5 | 2.0 (0–2) | 2.0 (0–2) | pass | 20.19 GiB |
| join-4 | 5 | 4.0 (0–4) | 4.0 (0–4) | pass | 19.96 GiB |
| peer-departure | 8 | 3.0 (3–4) | 4.0 (4–4) | pass | 19.92 GiB |
| steady-maintenance | 5 | 4.0 (4–4) | 4.0 (4–4) | pass | 19.95 GiB |

Highest sampled container memory peaks:
- `peer-departure` / `localai-control-600b987d09c8-p03`: median 73.5 MiB, sampled maximum 73.8 MiB; median CPU 0.18%.
- `peer-departure` / `localai-control-600b987d09c8-p01`: median 71.6 MiB, sampled maximum 72.1 MiB; median CPU 0.25%.
- `steady-maintenance` / `localai-control-600b987d09c8-p03`: median 70.0 MiB, sampled maximum 71.7 MiB; median CPU 0.37%.
- `steady-maintenance` / `localai-control-600b987d09c8-p01`: median 71.4 MiB, sampled maximum 71.4 MiB; median CPU 0.24%.
- `join-4` / `localai-control-600b987d09c8-p01`: median 69.8 MiB, sampled maximum 71.3 MiB; median CPU 0.32%.

## Interpretation limits

- One observer's local network vantage; relay IDs do not identify the original ledger publisher.
- The lab disables DHT; no WAN behavior is measured. If mDNS capture is absent, discovery traffic may be outside this trace.
- The run uses model-free peers. The gateway /v1/models probe is not model inference and no sharded inference workload is measured.
- Observed removals are ledger state differences, not explicit leave messages. Peer disconnect, service loss, and eventual record expiry are reported as separate evidence.
- A record or bucket removal means it was absent from a later snapshot received by this observer. That block may be from a stale, forked, or otherwise nonaccepted branch; block index and parent-hash lineage do not prove canonical acceptance. This trace cannot establish authoritative deletion, explicit departure, or prove that a leader-driven heartbeat-bucket scrub did or did not occur.
- Repeated full snapshots can contain unchanged records. Counts of changed records are observed state differences, not record-level packet counts.
