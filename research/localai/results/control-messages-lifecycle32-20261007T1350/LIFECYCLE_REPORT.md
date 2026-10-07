# LocalAI control-message lifecycle observations

Run: `control-messages-lifecycle32-20261007T1350`  
Source revision: `a3d555653c0cbfd73ab59ab15beab80903e96484`
Run status: **passed**.

## Capture totals

- Gossip envelope receptions: **7800**.
- Unique block snapshots seen by observers: **3637**; repeated snapshot receptions: **4163**.
- Peer connection/disconnection events: **68 / 68**.
- Captured mDNS packets: **835 queries / 19156 responses**.
- Empty-model controls: **passed** across 33 probes; gateway service probe: **passed**.
- One envelope carries a whole ledger block; the records listed below are state differences between observed snapshots, not separate packets.

## Lifecycle catalogue

| Order | Lifecycle category | Observed records/events | What the trace can show |
|---:|---|---|---|
| 1 | **discovery_and_connections** | listen address active at observer registration: 4; mDNS queries: 835; mDNS responses: 19156; peer connections: 68 | Peer discovery and transport connection events (mDNS is visible only if captured; DHT traffic may be outside this observer). |
| 2 | **membership_and_authorization** | `users` added: 1; `users` changed: 21 | User/peer registration records used by EdgeVPN admission checks. |
| 3 | **service_registration_and_worker_discovery** | `localai-control-e742dad6c85e_federated` added: 192; `localai-control-e742dad6c85e_federated` changed: 2163; `localai-control-e742dad6c85e_federated` present_at_first_observation: 2; `services` added: 199; `services` present_at_first_observation: 1 | EdgeVPN services and LocalAI worker advertisement records. |
| 4 | **health_and_keepalive** | `healthcheck` added: 223; `healthcheck` changed: 1467; `healthcheck` present_at_first_observation: 2 | Periodic health heartbeats and refreshed LastSeen fields. |
| 5 | **shared_model_state** | `shared_state` added: 10 | Federated application state such as synchronized model-name lists, when enabled. |
| 6 | **ledger_replication** | gossip receptions: 7800 | Encrypted gossip carries a compressed full-block ledger snapshot; the envelope can contain several record buckets. |
| 7 | **churn_and_expiry** | `healthcheck` removed: 191; `localai-control-e742dad6c85e_federated` removed: 161; `services` removed: 167; `shared_state` removed: 9; listen address closed: 3; peer disconnections: 68 | Observed state disappearance, branch/resync differences, stale/future records, and peer disconnects; none is an explicit leave message by itself. |
| 8 | **unknown_or_unclassified** | none recorded | Unknown events or buckets retained for inspection rather than silently discarded. |

## Source-derived message catalogue

These entries describe code paths and expected message types; their runtime status is shown separately.

| Source lifecycle stage | Message / record | Bucket | Runtime evidence |
|---|---|---|---|
| 1. Discovery and network join | mDNS peer discovery and libp2p connection setup | — | mDNS packets observed |
| 1. Discovery and network join | GossipSub room join, subscription and mesh maintenance | — | not directly visible in ledger capture |
| 1. Discovery and network join | Explicit membership/join ledger record | — | source says no explicit record exists |
| 2. Initial service advertisement and authorization | EdgeVPN service route | services | bucket present in observed snapshots: services |
| 2. Initial service advertisement and authorization | LocalAI federated/worker service advertisement | <P2PNetworkID>_federated, <P2PNetworkID>_worker, or <P2PNetworkID>_mlx_worker; if P2PNetworkID is empty: federated, worker, or mlx_worker | bucket present in observed snapshots: localai-control-e742dad6c85e_federated |
| 2. Initial service advertisement and authorization | Peer authorization/user record | users | bucket present in observed snapshots: users |
| 3. Steady-state liveness, service discovery and ledger sync | EdgeVPN peer heartbeat | healthcheck | bucket present in observed snapshots: healthcheck |
| 3. Steady-state liveness, service discovery and ledger sync | LocalAI service heartbeat/LastSeen refresh | <P2PNetworkID>_<service ID>; current federation bucket is <P2PNetworkID>_federated; other defined service IDs are <P2PNetworkID>_worker and <P2PNetworkID>_mlx_worker | bucket present in observed snapshots: localai-control-e742dad6c85e_federated |
| 3. Steady-state liveness, service discovery and ledger sync | Full-ledger synchronization | — | 7800 gossip envelope receptions observed |
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
| membership_and_authorization | `users` / added | Peer authorization/user record (application ledger record inside full-block envelope; conditionally emitted by LocalAI gateway/tunnel listener); lineage counts: direct_child_or_initial: 1 | 1 | `observer.jsonl:25294` (`f954ada43a1f…` ) [block index 2940; direct_child_or_initial] |
| membership_and_authorization | `users` / changed | Peer authorization/user record (application ledger record inside full-block envelope; conditionally emitted by LocalAI gateway/tunnel listener); lineage counts: direct_child_or_initial: 19, not_direct_child_of_previous_observed_block: 2 | 21 | `observer.jsonl:25384` (`311182d4f0dc…` ) [block index 2972; direct_child_or_initial] |
| service_registration_and_worker_discovery | `localai-control-e742dad6c85e_federated` / added | LocalAI federated/worker service advertisement (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 35, not_direct_child_of_previous_observed_block: 157 | 192 | `observer.jsonl:105` (`0da0abafadc1…` ) [block index 15; direct_child_or_initial] |
| service_registration_and_worker_discovery | `localai-control-e742dad6c85e_federated` / changed | LocalAI federated/worker service advertisement (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 1941, not_direct_child_of_previous_observed_block: 222 | 2163 | `observer.jsonl:47` (`321319853699…` ) [block index 5; direct_child_or_initial] |
| service_registration_and_worker_discovery | `localai-control-e742dad6c85e_federated` / present_at_first_observation | LocalAI federated/worker service advertisement (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 1, not_direct_child_of_previous_observed_block: 1 | 2 | `observer.jsonl:40` (`8f6b74f8569f…` ) [block index 4; direct_child_or_initial] |
| service_registration_and_worker_discovery | `services` / added | EdgeVPN service route (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 36, not_direct_child_of_previous_observed_block: 163 | 199 | `observer.jsonl:97` (`b52a1b9d1d37…` ) [block index 12; direct_child_or_initial] |
| service_registration_and_worker_discovery | `services` / present_at_first_observation | EdgeVPN service route (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 1 | 1 | `observer.jsonl:40` (`8f6b74f8569f…` ) [block index 4; direct_child_or_initial] |
| health_and_keepalive | `healthcheck` / added | EdgeVPN peer heartbeat (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 63, not_direct_child_of_previous_observed_block: 160 | 223 | `observer.jsonl:98` (`3e1fa4c317e9…` ) [block index 13; direct_child_or_initial] |
| health_and_keepalive | `healthcheck` / changed | EdgeVPN peer heartbeat (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 1304, not_direct_child_of_previous_observed_block: 163 | 1467 | `observer.jsonl:56` (`fa37c7533475…` ) [block index 6; direct_child_or_initial] |
| health_and_keepalive | `healthcheck` / present_at_first_observation | EdgeVPN peer heartbeat (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 1, not_direct_child_of_previous_observed_block: 1 | 2 | `observer.jsonl:40` (`8f6b74f8569f…` ) [block index 4; direct_child_or_initial] |
| shared_model_state | `shared_state` / added | Shared model-name list (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 1, not_direct_child_of_previous_observed_block: 9 | 10 | `observer.jsonl:103` (`1252a53448e1…` ) [block index 14; direct_child_or_initial] |
| churn_and_expiry | `healthcheck` / removed | EdgeVPN peer heartbeat (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 33, not_direct_child_of_previous_observed_block: 158 | 191 | `observer.jsonl:214` (`8f19ad494d8a…` ) [block index 3; not_direct_child_of_previous_observed_block] |
| churn_and_expiry | `localai-control-e742dad6c85e_federated` / removed | LocalAI federated/worker service advertisement (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 1, not_direct_child_of_previous_observed_block: 160 | 161 | `observer.jsonl:214` (`8f19ad494d8a…` ) [block index 3; not_direct_child_of_previous_observed_block] |
| churn_and_expiry | `services` / removed | EdgeVPN service route (application ledger record inside full-block envelope); lineage counts: direct_child_or_initial: 1, not_direct_child_of_previous_observed_block: 166 | 167 | `observer.jsonl:214` (`8f19ad494d8a…` ) [block index 3; not_direct_child_of_previous_observed_block] |
| churn_and_expiry | `shared_state` / removed | Shared model-name list (application ledger record inside full-block envelope); lineage counts: not_direct_child_of_previous_observed_block: 9 | 9 | `observer.jsonl:214` (`8f19ad494d8a…` ) [block index 3; not_direct_child_of_previous_observed_block] |

## Bucket-level state transitions

| Bucket | Observation | Count | Records removed from observed state | Snapshot lineage | Example raw pointer |
|---|---|---:|---:|---|---|
| `healthcheck` | absent from a later snapshot | 6 | 52 | direct_child_or_initial: 2, not_direct_child_of_previous_observed_block: 4 | `observer.jsonl:252` |
| `localai-control-e742dad6c85e_federated` | absent from a later snapshot | 27 | 97 | direct_child_or_initial: 1, not_direct_child_of_previous_observed_block: 26 | `observer.jsonl:249` |
| `services` | absent from a later snapshot | 18 | 72 | direct_child_or_initial: 1, not_direct_child_of_previous_observed_block: 17 | `observer.jsonl:249` |
| `shared_state` | absent from a later snapshot | 9 | 9 | not_direct_child_of_previous_observed_block: 9 | `observer.jsonl:214` |
A whole-bucket disappearance is inferred from consecutive received snapshots; it is not a distinct DELETE packet or proof of an explicit peer leave.

## mDNS TTL-zero resource records

No TTL-zero DNS resource records were observed; the capture cannot infer a LocalAI leave from this absence.

## Freshness checks

| Bucket | Status | Count | Example |
|---|---|---:|---|
| `healthcheck` | fresh | 109227 | `observer.jsonl:40` |
| `localai-control-e742dad6c85e_federated` | fresh | 109218 | `observer.jsonl:40` |
| `localai-control-e742dad6c85e_federated` | stale | 863 | `observer.jsonl:3739` |

## Membership and resource samples

Target service peers: **32**; actual peers: **32**.

| Phase | Samples | Fresh nodes per witness (median; range) | Listed API nodes (median; range) | Final stage check | Lowest host MemAvailable |
|---|---:|---|---|---|---:|
| bootstrap | 6 | 1.0 (0–1) | 1.0 (0–1) | pass | 19.85 GiB |
| join-16 | 6 | 16.0 (0–16) | 16.0 (0–16) | pass | 18.75 GiB |
| join-2 | 6 | 2.0 (0–2) | 2.0 (0–2) | pass | 19.68 GiB |
| join-32 | 6 | 32.0 (0–32) | 32.0 (0–32) | pass | 17.38 GiB |
| join-8 | 6 | 8.0 (0–8) | 8.0 (0–8) | pass | 19.28 GiB |
| peer-departure | 8 | 31.0 (30–32) | 32.0 (32–32) | pass | 17.02 GiB |
| peer-rejoin | 6 | 32.0 (0–32) | 33.0 (0–33) | pass | 17.07 GiB |
| steady-maintenance | 81 | 32.0 (30–32) | 32.0 (32–32) | pass | 16.77 GiB |

Highest sampled container memory peaks:
- `steady-maintenance` / `localai-control-e742dad6c85e-p20`: median 86.8 MiB, sampled maximum 93.2 MiB; median CPU 2.14%.
- `peer-rejoin` / `localai-control-e742dad6c85e-p20`: median 92.5 MiB, sampled maximum 93.0 MiB; median CPU 2.25%.
- `peer-departure` / `localai-control-e742dad6c85e-p20`: median 92.5 MiB, sampled maximum 93.0 MiB; median CPU 2.37%.
- `peer-rejoin` / `localai-control-e742dad6c85e-p28`: median 91.8 MiB, sampled maximum 92.5 MiB; median CPU 2.29%.
- `steady-maintenance` / `localai-control-e742dad6c85e-p28`: median 89.3 MiB, sampled maximum 92.2 MiB; median CPU 2.28%.

## Interpretation limits

- One observer's local network vantage; relay IDs do not identify the original ledger publisher.
- The lab disables DHT; no WAN behavior is measured. If mDNS capture is absent, discovery traffic may be outside this trace.
- The run uses model-free peers. The gateway /v1/models probe is not model inference and no sharded inference workload is measured.
- Observed removals are ledger state differences, not explicit leave messages. Peer disconnect, service loss, and eventual record expiry are reported as separate evidence.
- A record or bucket removal means it was absent from a later snapshot received by this observer. That block may be from a stale, forked, or otherwise nonaccepted branch; block index and parent-hash lineage do not prove canonical acceptance. This trace cannot establish authoritative deletion, explicit departure, or prove that a leader-driven heartbeat-bucket scrub did or did not occur.
- Repeated full snapshots can contain unchanged records. Counts of changed records are observed state differences, not record-level packet counts.
