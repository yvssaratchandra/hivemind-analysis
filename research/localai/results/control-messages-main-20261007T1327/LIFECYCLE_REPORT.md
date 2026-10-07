# LocalAI control-message lifecycle observations

Run: `control-messages-main-20261007T1327`  
Source revision: `a3d555653c0cbfd73ab59ab15beab80903e96484`
Run status: **failed**.


**Partial capture:** the lifecycle experiment did not complete successfully. Membership witness failed; inspect its state and logs
## Capture totals

- Gossip envelope receptions: **862**.
- Unique block snapshots seen by observers: **478**; repeated snapshot receptions: **384**.
- Peer connection/disconnection events: **68 / 68**.
- Captured mDNS packets: **247 queries / 2364 responses**.
- Empty-model controls: **passed** across 32 probes; gateway service probe: **not completed or failed**.
- One envelope carries a whole ledger block; the records listed below are state differences between observed snapshots, not separate packets.

## Lifecycle catalogue

| Order | Lifecycle category | Observed records/events | What the trace can show |
|---:|---|---|---|
| 1 | **discovery_and_connections** | listen address active at observer registration: 4; mDNS queries: 247; mDNS responses: 2364; peer connections: 68 | Peer discovery and transport connection events (mDNS is visible only if captured; DHT traffic may be outside this observer). |
| 2 | **membership_and_authorization** | none recorded | User/peer registration records used by EdgeVPN admission checks. |
| 3 | **service_registration_and_worker_discovery** | `localai-control-5d55bb8f7297_federated` added: 268; `localai-control-5d55bb8f7297_federated` changed: 182; `localai-control-5d55bb8f7297_federated` present_at_first_observation: 1; `services` added: 263; `services` present_at_first_observation: 2 | EdgeVPN services and LocalAI worker advertisement records. |
| 4 | **health_and_keepalive** | `healthcheck` added: 264; `healthcheck` changed: 127; `healthcheck` present_at_first_observation: 2 | Periodic health heartbeats and refreshed LastSeen fields. |
| 5 | **shared_model_state** | `shared_state` added: 13 | Federated application state such as synchronized model-name lists, when enabled. |
| 6 | **ledger_replication** | gossip receptions: 862 | Encrypted gossip carries a compressed full-block ledger snapshot; the envelope can contain several record buckets. |
| 7 | **churn_and_expiry** | `healthcheck` removed: 234; `localai-control-5d55bb8f7297_federated` removed: 237; `services` removed: 233; `shared_state` removed: 12; listen address closed: 3; peer disconnections: 68 | Observed state disappearance, branch/resync differences, stale/future records, and peer disconnects; none is an explicit leave message by itself. |
| 8 | **unknown_or_unclassified** | none recorded | Unknown events or buckets retained for inspection rather than silently discarded. |

## Source-derived message catalogue

These entries describe code paths and expected message types; their runtime status is shown separately.

| Source lifecycle stage | Message / record | Bucket | Runtime evidence |
|---|---|---|---|
| 1. Discovery and network join | mDNS peer discovery and libp2p connection setup | — | mDNS packets observed |
| 1. Discovery and network join | GossipSub room join, subscription and mesh maintenance | — | not directly visible in ledger capture |
| 1. Discovery and network join | Explicit membership/join ledger record | — | source says no explicit record exists |
| 2. Initial service advertisement and authorization | EdgeVPN service route | services | bucket present in observed snapshots: services |
| 2. Initial service advertisement and authorization | LocalAI federated/worker service advertisement | <P2PNetworkID>_federated, <P2PNetworkID>_worker, or <P2PNetworkID>_mlx_worker; if P2PNetworkID is empty: federated, worker, or mlx_worker | bucket present in observed snapshots: localai-control-5d55bb8f7297_federated |
| 2. Initial service advertisement and authorization | Peer authorization/user record | users | no matching bucket observed |
| 3. Steady-state liveness, service discovery and ledger sync | EdgeVPN peer heartbeat | healthcheck | bucket present in observed snapshots: healthcheck |
| 3. Steady-state liveness, service discovery and ledger sync | LocalAI service heartbeat/LastSeen refresh | <P2PNetworkID>_<service ID>; current federation bucket is <P2PNetworkID>_federated; other defined service IDs are <P2PNetworkID>_worker and <P2PNetworkID>_mlx_worker | bucket present in observed snapshots: localai-control-5d55bb8f7297_federated |
| 3. Steady-state liveness, service discovery and ledger sync | Full-ledger synchronization | — | 862 gossip envelope receptions observed |
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
| service_registration_and_worker_discovery | `localai-control-5d55bb8f7297_federated` / added | LocalAI federated/worker service advertisement (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 32, not_direct_child_of_previous_observed_block: 236 | 268 | `observer.jsonl:93` (`13c28f7d5095…` ) [block index 14; direct_child_or_initial] |
| service_registration_and_worker_discovery | `localai-control-5d55bb8f7297_federated` / changed | LocalAI federated/worker service advertisement (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 158, not_direct_child_of_previous_observed_block: 24 | 182 | `observer.jsonl:48` (`5628a48ed908…` ) [block index 6; direct_child_or_initial] |
| service_registration_and_worker_discovery | `localai-control-5d55bb8f7297_federated` / present_at_first_observation | LocalAI federated/worker service advertisement (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 1 | 1 | `observer.jsonl:41` (`3ed120bc57a0…` ) [block index 4; direct_child_or_initial] |
| service_registration_and_worker_discovery | `services` / added | EdgeVPN service route (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 37, not_direct_child_of_previous_observed_block: 226 | 263 | `observer.jsonl:86` (`4b0e989de8c8…` ) [block index 12; direct_child_or_initial] |
| service_registration_and_worker_discovery | `services` / present_at_first_observation | EdgeVPN service route (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 1, not_direct_child_of_previous_observed_block: 1 | 2 | `observer.jsonl:41` (`3ed120bc57a0…` ) [block index 4; direct_child_or_initial] |
| health_and_keepalive | `healthcheck` / added | EdgeVPN peer heartbeat (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 30, not_direct_child_of_previous_observed_block: 234 | 264 | `observer.jsonl:94` (`1a3f927d1e07…` ) [block index 15; direct_child_or_initial] |
| health_and_keepalive | `healthcheck` / changed | EdgeVPN peer heartbeat (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 116, not_direct_child_of_previous_observed_block: 11 | 127 | `observer.jsonl:42` (`e39b99fdccab…` ) [block index 5; direct_child_or_initial] |
| health_and_keepalive | `healthcheck` / present_at_first_observation | EdgeVPN peer heartbeat (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 1, not_direct_child_of_previous_observed_block: 1 | 2 | `observer.jsonl:41` (`3ed120bc57a0…` ) [block index 4; direct_child_or_initial] |
| shared_model_state | `shared_state` / added | Shared model-name list (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 1, not_direct_child_of_previous_observed_block: 12 | 13 | `observer.jsonl:87` (`57547600cd12…` ) [block index 13; direct_child_or_initial] |
| churn_and_expiry | `healthcheck` / removed | EdgeVPN peer heartbeat (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 1, not_direct_child_of_previous_observed_block: 233 | 234 | `observer.jsonl:198` (`c32a1d035c86…` ) [block index 3; not_direct_child_of_previous_observed_block] |
| churn_and_expiry | `localai-control-5d55bb8f7297_federated` / removed | LocalAI federated/worker service advertisement (application ledger record inside full-block envelope); lineage counts: not_direct_child_of_previous_observed_block: 237 | 237 | `observer.jsonl:198` (`c32a1d035c86…` ) [block index 3; not_direct_child_of_previous_observed_block] |
| churn_and_expiry | `services` / removed | EdgeVPN service route (application ledger record inside full-block envelope); lineage counts: not_direct_child_of_previous_observed_block: 233 | 233 | `observer.jsonl:198` (`c32a1d035c86…` ) [block index 3; not_direct_child_of_previous_observed_block] |
| churn_and_expiry | `shared_state` / removed | Shared model-name list (application ledger record inside full-block envelope); lineage counts: not_direct_child_of_previous_observed_block: 12 | 12 | `observer.jsonl:198` (`c32a1d035c86…` ) [block index 3; not_direct_child_of_previous_observed_block] |

## Bucket-level state transitions

| Bucket | Observation | Count | Records removed from observed state | Snapshot lineage | Example raw pointer |
|---|---|---:|---:|---|---|
| `healthcheck` | absent from a later snapshot | 5 | 5 | direct_child_or_initial: 1, not_direct_child_of_previous_observed_block: 4 | `observer.jsonl:200` |
| `localai-control-5d55bb8f7297_federated` | absent from a later snapshot | 29 | 113 | not_direct_child_of_previous_observed_block: 29 | `observer.jsonl:199` |
| `services` | absent from a later snapshot | 27 | 87 | not_direct_child_of_previous_observed_block: 27 | `observer.jsonl:199` |
| `shared_state` | absent from a later snapshot | 12 | 12 | not_direct_child_of_previous_observed_block: 12 | `observer.jsonl:198` |
A whole-bucket disappearance is inferred from consecutive received snapshots; it is not a distinct DELETE packet or proof of an explicit peer leave.

## mDNS TTL-zero resource records

No TTL-zero DNS resource records were observed; the capture cannot infer a LocalAI leave from this absence.

## Freshness checks

| Bucket | Status | Count | Example |
|---|---|---:|---|
| `healthcheck` | fresh | 8268 | `observer.jsonl:41` |
| `localai-control-5d55bb8f7297_federated` | fresh | 8210 | `observer.jsonl:41` |
| `localai-control-5d55bb8f7297_federated` | stale | 7 | `observer.jsonl:3541` |

## Membership and resource samples

Target service peers: **63**; actual peers: **16**.

| Phase | Samples | Fresh nodes per witness (median; range) | Listed API nodes (median; range) | Final stage check | Lowest host MemAvailable |
|---|---:|---|---|---|---:|
| bootstrap | 6 | 1.0 (0–1) | 1.0 (0–1) | pass | 20.41 GiB |
| join-16 | 6 | 16.0 (0–16) | 16.0 (0–16) | pass | 19.03 GiB |
| join-2 | 6 | 2.0 (0–2) | 2.0 (0–2) | pass | 20.24 GiB |
| join-32 | 6 | 31.0 (0–32) | 32.0 (0–32) | not_recorded | 17.01 GiB |
| join-8 | 6 | 8.0 (0–8) | 8.0 (0–8) | pass | 19.94 GiB |

Highest sampled container memory peaks:
- `join-32` / `localai-control-5d55bb8f7297-p01`: median 88.6 MiB, sampled maximum 88.9 MiB; median CPU 2.23%.
- `join-32` / `localai-control-5d55bb8f7297-p30`: median 86.5 MiB, sampled maximum 88.9 MiB; median CPU 1.49%.
- `join-32` / `localai-control-5d55bb8f7297-p04`: median 87.5 MiB, sampled maximum 88.5 MiB; median CPU 2.39%.
- `join-32` / `localai-control-5d55bb8f7297-p29`: median 86.5 MiB, sampled maximum 87.0 MiB; median CPU 2.06%.
- `join-32` / `localai-control-5d55bb8f7297-p05`: median 86.3 MiB, sampled maximum 86.7 MiB; median CPU 2.42%.

## Interpretation limits

- One observer's local network vantage; relay IDs do not identify the original ledger publisher.
- The lab disables DHT; no WAN behavior is measured. If mDNS capture is absent, discovery traffic may be outside this trace.
- The run uses model-free peers. The gateway /v1/models probe is not model inference and no sharded inference workload is measured.
- Observed removals are ledger state differences, not explicit leave messages. Peer disconnect, service loss, and eventual record expiry are reported as separate evidence.
- A record or bucket removal means it was absent from a later snapshot received by this observer. That block may be from a stale, forked, or otherwise nonaccepted branch; block index and parent-hash lineage do not prove canonical acceptance. This trace cannot establish authoritative deletion, explicit departure, or prove that a leader-driven heartbeat-bucket scrub did or did not occur.
- Repeated full snapshots can contain unchanged records. Counts of changed records are observed state differences, not record-level packet counts.
