# Coordination-layer table: evidence

This document backs the coordination-layer comparison table cell by cell. Each cell gives:
- the claim;
- the source lines (file:line at the pinned commit, or an archived page);
- an evidence status.

Evidence status:
- **Code** — read in the pinned source.
- **Docs** — stated in the project's own documentation or public dashboard.
- **Measured** — observed in a local experiment in this repository.
- **Inferred** — follows from code, but not run.
- **Open** — not yet checked.

Nothing here comes from a public-swarm experiment.

Scope so far: the Place, Lead, Combine and Credit cells for Node0 and Agora. Join and Sync cells, and all SN38, RL Swarm and Hivemind cells, are not yet reviewed here.

## Sources

| Project | Repository | Commit | Local path |
|---|---|---|---|
| Node0 | PluralisResearch/node0 | `32bd084de15dbdc8e30723ad66db7d01acf70c03` | `research/node0/upstream/` |
| Agora | PluralisResearch/agora | `37d29a966528675635161007d7b43388012a98b2` | `research/agora/upstream/` |
| SN38 | dstrbtd/DistributedTraining | `082f3a632cd412e688a325356af08e34f01d8b8e` | `research/distributed-training/upstream/` |
| RL Swarm | gensyn-ai/rl-swarm (CodeZero) | `286c17e5391fa3e9ebba64a8e683bb36e47376a8` | `research/gensyn/upstream/` |
| Hivemind | this fork | working tree | `hivemind/` |

The `upstream/` checkouts and `research/node0/data/` are git-ignored, so they exist only locally. To reproduce them, re-clone at the pinned commits. Key archived files are identified by SHA-256:
- Node0 dashboard bundle `response_30ef8ef71b08edaac24a_15.body`: `59bc716902fbb28cb46c8a071ddbe4feb4c35dd775ca31cffea86db441931bd1`. This matches the UI chunk hash recorded in `research/node0/data/public_runs_20261029T100000Z/report.md:71`.
- Agora reducer-mode probe report `research/agora/results/reducer_mode_20261015_c/REPORT.txt` (tracked in git): `92de4846531edab2544f7ab7fef0d6cdce995e3b71ea452eb77a7a86e3b021ed`.

Most file:line citations below were checked by a script confirming that the cited line contains the quoted token: 46 checks, plus 15 for the candidate-decision section and 26 for the strategy section (2026-11-04). Paper line numbers refer to `pdftotext research/agora/2607.pdf -` output numbered with `grep -n`. Where a citation is a range, the token falls inside the range. The Place-rebinding rows (added 2026-11-04) cite `hivemind/dht/schema.py:80-82`, `hivemind/dht/crypto.py:44-46` and the validator/dht_partition files; these were read directly, not re-run through the token-check script.

Abbreviations used below:
- `N0` = `research/node0/upstream/src/node0/`
- `AG` = `research/agora/upstream/agora_server/src/agora_server/`
- `AGC` = `research/agora/upstream/agora/src/agora/`
- `AGD` = `research/agora/upstream/docs/`

## Definitions used in the table

| Term | Definition |
|---|---|
| **O — Operator** | A value set by the party running the network: an auth server, shipped configuration, or an operator-run service. |
| **S — Shared state** | A value that no single operator or participant controls, and that every node reads the same way. Example: the outcome of chain consensus. Classify by **who writes the value**, not where it is stored. |
| **C — Participant claim** | A value a participant asserts about itself or others, which nobody else checks. |

How S is applied:
- **Not S:** DHT records, because participants write them and different nodes can see different values.
- **Not S:** keys derived from the stage or step, which are protocol rules, not inputs.
- **Not S:** chain values written by a privileged role, which count as O. Example: RL Swarm's `currentRound`.

What each decision means:
- **Place** — who works with whom.
- **Lead** — who runs the round. It does not cover when a round fires or what counts as progress.
- **Combine** — whose updates are averaged. Weight (how much each counts) is a number carried inside the all-reduce messages, so it is a data-plane input and is excluded.
- **Credit** — which contributions are rewarded.

Abstention: any participant can stay out of any round by not announcing or not sending. This holds for every project, so it is not recorded per cell.

---

## Node0

### Place — `O: stage; C: stage announcement`

| Claim | Evidence | Status |
|---|---|---|
| The auth server assigns the pipeline stage | `N0/security/authorization.py:183` `self.pipeline_stage = response["stage_type"]`; `N0/run_server.py:167` `pipeline_stage = str(authorizer.pipeline_stage)` | Code |
| The stage fixes the averaging key, a single fixed key per stage | `N0/server/matchmaking.py:65` `self._fixed_key = f"{self.prefix}.0b"`. The averaging prefix is `{run_id}_grad_averager`, and `run_id` is the stage name (`N0/server/node0_server.py:289` `run_id=expert_uid.split(UID_DELIMITER)[0]`) | Code |
| The group is every replica of the stage. The expected size is the stage roster (`<stage>.0.`) | `N0/server/matchmaking.py:175-176` `max_peers = len(self.peer_table)`; `:195` reads `self.prefix.split("_")[0] + ".0."`; `:214-250` collects announcements until `len(all_peerIds) == self.max_peers` or timeout | Code |
| The replica index (UID) does not affect grouping | `N0/server/matchmaking.py:252` the group is simply `sorted(all_peerIds)` | Code |
| **The assigned stage is not re-bound after Join.** The averaging key `{stage}_grad_averager.0b` is not one of the three schema-validated keys, so `SchemaValidator` returns `allow_extra_keys=True` for it (`hivemind/dht/schema.py:80-82`). The validators cover only `{prefix}_{stage}_worker_metrics`, `{prefix}_{peer_id}_worker_ports` and `{stage}_paramstore` (`N0/security/validation.py` `make_validators`) | `N0/security/validation.py`; `hivemind/dht/schema.py:80-82` | Code |
| The signature validator does not bind it either: it enforces a signature only for records whose key or subkey carries an `[owner:ssh-rsa …]` marker, else returns True unconditionally. The averaging announcement's subkey is the raw `peer_id.to_bytes()`, with no marker | `hivemind/dht/crypto.py:44-46`; `N0/server/matchmaking.py:84-89` | Code |
| The per-stage DHT split is computed client-side from the node's own stage, so it is not an operator-enforced boundary | `N0/utils/dht_partition.py:83-96` | Code |
| So a modified client is not prevented from announcing under another stage's key: placement is a participant claim re-asserted at the averaging layer, not just the auth server's assignment | Above rows | Code |
| **Effect of a foreign announcement is inferred, not tested.** Stages have different parameter shapes, so a participant whose tensors mismatch the target stage breaks that stage's all-reduce round (disruption). Clean inclusion (poisoning) would additionally need shape-correct tensors and surviving matchmaking, which is untested | — | **Inferred / untested** |
| Footnote: the client also sends a self-reported hardware profile (device, GPU memory, bandwidth) to the auth server on join, but nothing in the repo or docs says it influences the stage. So it is not listed as an input | `N0/security/authorization.py:134-140` | Code (input sent); effect **unknown** |

### Lead — `none`

| Claim | Evidence | Status |
|---|---|---|
| No peer runs the round. Every replica builds the same group locally from DHT reads | `N0/server/matchmaking.py:214-260`: no leader election; a constant `group_id` (`:257`) | Code |
| Every replica is a reducer, with no client mode | `client_mode` defaults to `dht.client_mode` (`N0/server/HM_gradient_averager.py:102`); no Node0 code sets the DHT to client mode (grep of `src/`) | Code / Inferred |
| What counts as progress (the round trigger) is excluded from Lead | User definition, above | — |

### Combine — `O: membership (via Place); C: reducer's ban verdict`

| Claim | Evidence | Status |
|---|---|---|
| Whose updates are averaged = the group from Place | See Place | Code |
| A reducer bans senders it did not receive data from in time; their parts drop out of that round | `N0/server/ar_runner.py:217-224` `_handle_missing_senders` bans senders not in `active_senders`; `:336` bans on exceptions; `:343-346` `_ban_sender` | Code |
| No other peer can check that verdict, so it is a participant claim | No cross-check in `ar_runner.py` | Inferred |
| Weight is self-reported (excluded from the cell as data-plane) | `N0/server/HM_gradient_averager.py:211` `control.weight = self.local_samples_accumulated` | Code |
| Can a node manipulate the roster or announcements to change others' groups? | Depends on DHT validation | **Open** |

### Credit — `O: documented rule; C: worker metrics`

| Claim | Evidence | Status |
|---|---|---|
| The node computes and writes its own FLOPs and active time to the DHT | `N0/utils/monitor.py:228-240`: `WorkerMetricsV1(peer_id, num_flop, active_time)` stored at `{experiment_prefix}_{stage}_worker_metrics` | Code |
| The scoring rule is on the operator's dashboard, not in the repo: `score = (FLOPs + presence credit) / scaling`; FLOPs = "sum of completed FLOPs across all this contributor's nodes"; presence credit = "fixed PFLOP-per-hour rate × total active time" | Archived dashboard: `research/node0/data/dashboard_20261029_final/node0-7-5b_overview/raw/response_30ef8ef71b08edaac24a_15.body` (captured 2026-10-29). Repo README only says contribution is shown "in the dashboard" (`research/node0/upstream/README.md:35`) | Docs |
| Caveat: the same UI component appears in the Agora capture, so it may be a site-wide description added after the Node0 run | `research/node0/data/public_runs_20261029T100000Z/report.md:71` | Docs |
| Caveat: archived leaderboard scores sum exactly to the peers' `flop_pflop`, so the presence credit is not visible in the data | `research/node0/data/public_runs_20261029T100000Z/report.md:72` | Measured (archive arithmetic) |
| No separate Node0 scoring documentation was found online (searched 2026-11-03) | — | Docs (negative search) |

---

## Agora

### Place — `O: stage; C: hardware report, stage announcement`

| Claim | Evidence | Status |
|---|---|---|
| The auth server assigns both the stage and the exact expert UID | `AGC/security/authorizer.py:122-123` `self.pipeline_stage = response["stage_type"]`, `self.expert_uid = response["uid"]`; used at `AGC/run_server.py:271, 276` | Code |
| The Authorizer places each node on "a stage that requires additional capacity" and "can place more capable contributors on the heavier stages" | Agora paper, `research/agora/2607.pdf` (`pdftotext` lines 1617-1625, "Stage placement") | Docs (paper) |
| Capability comes from a hardware profile the node reports about itself (device, GPU memory, RAM, bandwidth, latency) | `AGC/security/authorizer.py:100-116` | Code |
| The node learns its stage at authorization, before it trains: "a single log line confirms the slot and the assigned stage", and it then downloads that stage's checkpoint | `AGD/agora-system/startup-sequence.md:13, 19` | Docs |
| There is one group per stage per step. Group bits are fixed to empty, so every replica posts under the same step key | `AG/core/all_reduce/matchmaking.py:55` `self.group_bits = ""`; `:57-59` key `f"{prefix}_s{global_step}.0b"` | Code |
| The expected size is the stage roster (`<stage>.0.`) | `AG/core/all_reduce/matchmaking.py:179-189` `get_max_peers` counts entries at `prefix.split("_")[0] + ".0."` | Code |
| Each node waits up to its own deadline, then forms the group from whoever it has seen. No agreement step exists, so views can differ (late node, divergent DHT reads) | `AG/core/all_reduce/matchmaking.py:191-245` | Code |
| Mismatched views break the round rather than silently corrupt it: a sender missing from a reducer's list raises in `sender_peer_ids.index(...)`, and the constant `group_id` cannot detect the mismatch | `AG/core/all_reduce/ar_runner.py:333`; `matchmaking.py:239` | Inferred (untested) |
| **The assigned stage is not re-bound after Join**, as in Node0. Agora's DHT validators cover only the terminate record (`AG/security/validator_schema.py`); every other key, including the group key, passes under `allow_extra_keys=True`. The token carries only username, public key, expiry and signature (`AGC/security/authorizer.py:94-101`), not a stage. So a modified client is not prevented from announcing under another stage's key | `AG/security/validator_schema.py`; `AGC/security/authorizer.py:94-101`; `hivemind/dht/schema.py:80-82` | Code |
| Effect of a foreign announcement is inferred, not tested: at least round disruption (shape mismatch, as in the row above); clean poisoning untested | — | **Inferred / untested** |

**Limitation — deployment enforcement.** The stage-binding claim above concerns the inspected public peer code. The Authorizer service implementation is not available in the public repository, and we cannot inspect its server-side admission or renewal checks. The paper describes checks for client modifications at admission; the public client invokes an integrity check and submits its result (`AGC/security/authorizer.py:90-111`), but its coverage and server-side enforcement are unknown. Separately, the paper states that the Health Monitor compares DHT stage announcements against admission records ([§3.6.2, Health checks](https://arxiv.org/html/2607.13332v1#S3.SS6.SSS2)). Whether this monitoring or another operator-side check covers **averaging-group announcements**, and whether it prevents or detects a foreign-stage announcement before it affects a round, remains unknown. Consequently, this code finding does not establish that a modified client can pass admission and perform the manipulation in the deployed system, or that the deployment has no stage checks. This limitation also applies to Agora's Place Claim strategy entry and the deployment interpretation of open question 1.

### Lead — `C: averaging mode`

| Claim | Evidence | Status |
|---|---|---|
| The peers that reduce are those in NODE mode; CLIENT peers get a zero share of the reduction | `AG/core/all_reduce/ar_runner.py:145-149`; enum `AveragingMode` (`NODE`, `CLIENT`, `AUX`) at `:53-56` | Code |
| The averager takes its mode from its own local argument, and nothing signs or validates it | `AG/core/averaging/averager.py:180-185` | Code |
| Workers ship with `averaging_mode: client`. The optimizer's own default is `"node"`, so a client that drops this config line would advertise NODE | `AGC/configs/default.yaml:31`; `AG/core/optimization/optimizer_sync.py:138` | Code |
| The auth server does not assign the mode. The node sends `role: "worker"`, and the role that comes back only labels metrics | `AGC/run_server.py:230, 305-312` | Code |
| No parameter store, reducer allowlist or trusted-peer list exists in the client | Search of `agora/` and `agora_server/` for paramstore, allowlist, whitelist, trusted (2026-11-04) | Code (negative) |
| A closed-source integrity check sends a client-computed hash on join. What it covers is unknown (the binary's strings are compressed), and it can be switched off. If it covers the config, it acts at Join, not Lead | `AGC/security/authorizer.py:90-111`; `AGC/run_server.py:222` `check_integrity` | Code; coverage **unknown** |
| The mode is **advertised by the peer itself**. A peer that advertises NODE receives reduction work | `research/agora/HANDOFF.md:3-13`; `research/agora/results/reducer_mode_20261015_c/REPORT.txt` (local, 3 loopback peers, no auth or integrity service) | Measured (local) |
| Whether the deployment's auth or integrity checks stop a modified client advertising NODE | Not tested | **Open** |
| In deployment, only Pluralis nodes are assigned the reducer role: "Only Pluralis nodes, which have fast, reliable network connections, are assigned the reducer role, receiving data from all peers and performing the reduction (averaging), while contributors act only as senders" | Agora paper, arXiv 2607.13332 (`research/agora/2607.pdf`, §All-reduce; `pdftotext` lines 759-761) | Docs (paper) |
| There is no group leader: peers announce in the DHT, wait for the stage to register, then "members are ordered deterministically so that every peer derives the same layout" | Same paper, lines 757-759 | Docs (paper) |

Corrections:
- An earlier draft of this cell read `none`. That is wrong, because the reducers run the reduction.
- A later draft read `O: reducers`, then `O: shipped config; C: advertised NODE mode`. The operator is not an input to this decision: it never assigns the mode, and the protocol acts only on each peer's advert. The operator's intent (only Pluralis nodes reduce, per the paper) works only through shipped defaults, and possibly through Join if the integrity check covers the config. That belongs in a footnote, not the cell.

### Combine — `O: membership (via Place); C: reducer's ban verdict`

| Claim | Evidence | Status |
|---|---|---|
| Whose updates are averaged = the group from Place | See Place | Code |
| What is averaged is a 5% sparse parameter slice every 20 local steps; there is no gradient all-reduce | `AGD/agora-system/research-foundations.md:38`; `AGD/agora-system/memory-communication.md:73, 89-91`; `AGD/index.md:67` | Docs |
| Reducers ban senders that fail or time out; their parts drop out of that round | `AG/core/all_reduce/ar_runner.py:260-267` (missing senders), `:384`, `:391-396` `_ban_sender`; `AGD/agora-system/fault-tolerance.md:13` | Code + Docs |
| Only NODE peers reduce, so only they hold this verdict | See Lead | Code |
| Weight is self-reported, and 0 in ghost phase (excluded from the cell as data-plane); reducers use the weight without checking it | `AG/core/optimization/optimizer_sparta_async_nstep.py:296`; `AG/core/all_reduce/partition.py:311-313` | Code |

### Credit — `O: documented rule, presence; C: FLOP reports`

| Claim | Evidence | Status |
|---|---|---|
| Score = compute PFLOPs + 1 PFLOP per active hour, summed per HuggingFace account, capped at 2 peers; no financial reward yet | `AGD/quick-start/points.md`; `AGD/index.md:53`; online: https://pluralis.ai/docs/quick-start/points/ | Docs |
| FLOPs are computed by the node from its own logs and pushed to the operator's metrics server | `AG/prometheus/monitor.py:588-589` (samples), `:616-652` (FLOPs / MFU) | Code |
| Presence is observed by the operator ("registered and visible in the swarm"); the node does not claim a duration. The exact mechanism is opaque | `AGD/quick-start/points.md` | Docs |
| Contributors that consistently fail are removed from the swarm (Credit and liveness feeding back into Join) | `AGD/agora-system/fault-tolerance.md:15` | Docs |

---

## Strategy feasibility (Claim manipulation and Shared-state anticipation)

This table marks whether an attacker can influence each decision through two strategies:
- **Claim manipulation (Claim):** lying in a participant-claim (C) input to the decision.
- **Shared-state anticipation (State):** predicting a shared-state (S) input in advance and positioning for it.

Marks:
- **✓** — feasible. The basis is code or docs unless the cell says measured or inferred.
- **–** — the decision has no input of that type, so the strategy does not apply.
- **×** — the decision has an input of that type, but the strategy fails, e.g. because the input is checked. No cell currently has this mark. Every claim input found here goes unchecked, and neither project has a shared-state input.

| System | Place Claim | Place State | Lead Claim | Lead State | Combine Claim | Combine State | Credit Claim | Credit State |
|---|---|---|---|---|---|---|---|---|
| Node0 | ✓ (inferred) | – | – | – | ✓ | – | ✓ | – |
| Agora | ✓ (inferred) | – | ✓ | – | ✓ | – | ✓ | – |

**No State cell applies.** Neither project has a chain or an external randomness source. Every value these decisions use is either fixed in the code or derived from participant claims. The table below lists each value and why it is not shared state.

| Value | Where it is used | Source | Shared state? |
|---|---|---|---|
| Stage | Place | Auth server (`N0/security/authorization.py:183`; `AGC/security/authorizer.py:122`) | No, O |
| Group key: Node0 `{stage}.0b`, Agora `{stage}_s{step}.0b` | Place, Combine | Derived from stage and step (`N0/server/matchmaking.py:65`; `AG/core/all_reduce/matchmaking.py:57-59`). Predictable, but not validator-bound to the assigned stage (see Place Claim), so it is a claim surface, not shared state | No, protocol rule |
| Global step | Agora group key, SPARTA slice | Highest step claimed by non-client peers (`AG/core/optimization/progress_tracker.py:335-338`) | No, C |
| Averaging mode (who reduces) | Agora Lead | Each peer's advert (`AG/core/averaging/averager.py:180-185`) | No, C |
| Which 5% of parameters are averaged | Agora Combine (data plane) | Fixed seed `1337` plus the global step (`AG/core/averaging/state_averager_sparta.py:104, 109-118`) | No, a constant plus C |
| Which reducer owns which slice | Combine | Group sorted by peer ID; the *i*-th peer reduces the *i*-th part (`AG/core/all_reduce/ar_runner.py:123`; `matchmaking.py:238-241`; Node0 `N0/server/matchmaking.py:252`) | No. Peer IDs are self-generated keys |
| Scoring rule, presence | Credit | Operator (dashboard, docs) | No, O |

Note on the earlier draft of this table: it marked several State cells ✓ or ×, because the cells then listed group keys as shared state (for example Combine `S: step key; C: records, weight`). The keys were later reclassified as protocol rules, so those marks became –.

### Node0

| Cell | Mark | Proof | Status |
|---|---|---|---|
| Place Claim | ✓ (inferred) | The auth server assigns the stage, but nothing re-binds it after Join. The averaging key `{stage}_grad_averager.0b` is not a schema-validated key (`hivemind/dht/schema.py:80-82`), and the signature validator ignores records without an `[owner:]` marker, which the announcement lacks (`hivemind/dht/crypto.py:44-46`; `N0/server/matchmaking.py:84-89`). The per-stage DHT split is computed client-side (`N0/utils/dht_partition.py:83-96`). So a modified client can announce under a non-assigned stage's key. Demonstrated effect is round disruption (shape mismatch); clean poisoning untested | Code; effect **inferred** |
| Place State | – | No S input; see the table above | Code |
| Lead Claim | – | There is no leader and no reducer role to claim. Every replica reduces (`N0/server/HM_gradient_averager.py:102`). A node's bandwidth or mode advert cannot change its share, because Node0 replaces every member's advertised data with the node's own (`N0/server/matchmaking.py:258` `gathered = tuple(step.data_for_gather for peer_id in sorted_peer_ids)`). A node that deviates can make layouts disagree and break the round, but cannot take it over | Code; disruption **inferred** |
| Lead State | – | No leader, no S input | Code |
| Combine Claim | ✓ | Every replica is a reducer and can ban a sender as "missing" or failed (`N0/server/ar_runner.py:217-224, 336, 343-346`). No peer can check that verdict, so a false ban drops an honest sender's update from that part of the round | Code; exploitation **inferred** (not tested) |
| Combine State | – | No S input | Code |
| Credit Claim | ✓ | The worker metrics are FLOPs and active time computed by the node itself and written to the DHT (`N0/utils/monitor.py:228-240`). The dashboard score sums FLOPs plus presence from active time (archived dashboard bundle, see Credit above). Nothing in the repo checks the values | Code + Docs; exploitation **inferred** (the operator may cross-check out of view) |
| Credit State | – | No S input; the rule is set by the operator | Docs |

### Agora

| Cell | Mark | Proof | Status |
|---|---|---|---|
| Place Claim | ✓ (inferred) | Two claim inputs. (1) The Authorizer "can place more capable contributors on the heavier stages" (paper, lines 1617-1625) from the node's self-reported hardware (`AGC/security/authorizer.py:100-116`), so over-reporting could steer the assignment. (2) As in Node0, the assigned stage is not re-bound after Join: validators cover only the terminate record (`AG/security/validator_schema.py`), and the token carries no stage (`AGC/security/authorizer.py:94-101`), so a modified client can announce under a non-assigned stage's key. Demonstrated effect is disruption; poisoning untested | Paper + Code; server closed, **inferred** |
| Place State | – | No S input. See the related tactics below | Code |
| Lead Claim | ✓ | Reducer status is the peer's own averaging-mode advert (`AG/core/all_reduce/ar_runner.py:145-149`; `AG/core/averaging/averager.py:180-185`). In a local probe, a peer advertising NODE was given 32 of 96 reduction elements, and 0 when it advertised CLIENT | **Measured (local)**: `research/agora/HANDOFF.md:3-13`, `research/agora/results/reducer_mode_20261015_c/REPORT.txt`. Run with 3 loopback peers, no auth or integrity service; deployment enforcement untested |
| Lead State | – | No S input; see the table above | Code |
| Combine Claim | ✓ | NODE peers ban senders that fail or time out (`AG/core/all_reduce/ar_runner.py:260-267, 384, 391-396`; `AGD/agora-system/fault-tolerance.md:13`). The verdict cannot be checked. Anyone can become a NODE peer through the Lead claim | Code + Docs; exploitation **inferred** |
| Combine State | – | No S input. The SPARTA slice and slice ownership are predictable, but come from a constant and claims; see the table above | Code |
| Credit Claim | ✓ | Compute points are FLOPs the node computes from its own logs and pushes to the operator's metrics server (`AG/prometheus/monitor.py:616-652`). The documented score uses those FLOPs (`AGD/quick-start/points.md`) | Code + Docs; exploitation **inferred** (the operator may cross-check out of view) |
| Credit State | – | No S input. The rule and presence are operator-side (`AGD/quick-start/points.md`) | Docs |

### Related tactics outside the two strategies

These are footnotes, not table cells. All are inferred and untested.

| Tactic | What it exploits | Evidence |
|---|---|---|
| **Stage re-roll** (Agora): quit, delete `private.key`, rejoin, and repeat until the target stage is assigned | Cheap node identities (Join). Each try must pass the reachability check, the join rate limit, the queue and a full state download | The stage is known at authorization (`AGD/agora-system/startup-sequence.md:13, 19`). A fresh key gives a new identity (`AGD/quick-start/advanced.md:150-159`; `research/agora/upstream/README.md:335-341`). The cap is 2 active peers per HF account (`AGD/quick-start/advanced.md:23-24`). Admission checks are described in paper lines 1582-1605 |
| **Predicting the stage** (Agora): join when the target stage is the most under-served | The operator's balancing rule plus per-stage occupancy that the operator publishes (not S) | Paper lines 1617-1621; per-stage occupancy shown on the dashboard (`AGD/agora-system/startup-sequence.md`, Queue section) |
| **Record capture** (both): put Sybils near the predictable group key to control what nodes read | The DHT, not a decision input | Keys at `N0/server/matchmaking.py:65` and `AG/core/all_reduce/matchmaking.py:57-59`. Hivemind measurements in `research/hivemind/HANDOFF.md` §4.1 and §4.6 |
| **Slice targeting** (Agora): poison exactly the 5% of parameters about to be averaged | The data plane; the slice is computable in advance | `AG/core/averaging/state_averager_sparta.py:104, 109-118` |
| **Slice-ownership grinding** (both): generate keys until the peer ID sorts into a chosen reducer position | Self-generated peer IDs | `AG/core/all_reduce/ar_runner.py:123`; `AG/core/all_reduce/matchmaking.py:238-241` |

---

## Candidate decisions not yet in the table

These were found by reviewing what the six decisions miss. They are proposals, not agreed cells.

### Progress: what is the current step? (proposed new decision)

The global step decides whether a node is stale. Staleness triggers Sync; in Agora it also triggers ghost mode (weight 0). In Agora the global step also sets the group key.

| Project | Claim | Evidence | Status |
|---|---|---|---|
| Both | The global epoch is the **highest epoch claimed** by any non-client peer. Peers advance when the **summed self-reported samples** reach the target | Hivemind tracker: `hivemind/optim/progress_tracker.py:303-306` (max over non-client claims), `:128-133` (`ready_to_update_epoch`). Agora's copy: `AG/core/optimization/progress_tracker.py:335-338` | Code |
| Node0 | Uses Hivemind's tracker through its `Optimizer` subclass. Every replica is non-client (see Lead), so every replica's claim counts | `N0/server/optim.py:36` (`AutoStepOptimizer(Optimizer)`), `:78-81` | Code |
| Node0 | A node more than `max_allowed_stale` epochs behind the global epoch loads state from peers (Sync) | `N0/server/optim.py:85` | Code |
| Agora | Workers are CLIENT, so only NODE peers' claims set the global epoch | `AG/core/optimization/progress_tracker.py:335-338`; `AGC/configs/default.yaml:31` | Code |
| Agora | A node more than `max_allowed_stale` behind enters ghost mode | `AG/core/optimization/optimizer_sparta_async_nstep.py:104-107` | Code |
| Agora | The group key for each round is derived from the global epoch | `AG/core/averaging/averager.py:491` `look_for_group(step, global_step=int(self.global_epoch))` | Code |

Proposed cells: **Node0 `C: epoch claims (all replicas)`**, **Agora `C: epoch claims (NODE peers)`**.

### Route: which worker gets each microbatch? (proposed: fold into Place)

Place currently covers only the averaging group inside one stage. Routing decides the pipeline path, i.e. which replica in each stage works with which, and therefore who does work and earns FLOP credit.

| Project | Claim | Evidence | Status |
|---|---|---|---|
| Agora | Trainers route microbatches to a worker in each stage, balance load and supply data shards. They "run on Pluralis-owned infrastructure rather than on contributor nodes" | `AGD/agora-system/training-architecture.md:149` | Docs |
| Agora | Trainers find workers from the DHT announcements, pick the least-loaded by accumulated virtual runtime, and short-ban unreachable workers for 30 s | `AGD/agora-system/training-architecture.md:195, 294, 308` | Docs |
| Agora | Trainer code is not in the repository | Search of `research/agora/upstream/` | Code (negative) |
| Node0 | No trainer code or routing description exists in the repository. The same design as Agora is assumed | Search of `research/node0/upstream/` | **Inferred** |

Proposed cells (as part of Place): **Agora `O: trainers route`**, **Node0 `O: trainers route (inferred)`**.

### Removal: who is removed after joining? (proposed: fold into Join)

| Project | Claim | Evidence | Status |
|---|---|---|---|
| Agora | The operator's monitor can write a terminate record for a peer, signed with `monitor_public_key`. The node reads it and **kills itself** | `AG/logging/log_monitor.py:163-181`; schema `AG/security/validator_schema.py:39` | Code |
| Agora | Enforcement depends on the client cooperating. A modified client could ignore the record | Same lines: the kill is a local `os.killpg` | Inferred |
| Agora | Contributors that consistently fail are removed from the swarm | `AGD/agora-system/fault-tolerance.md:15` | Docs |
| Node0 | The auth server can refuse (re-)joins: 401/403/429 are treated as "not in allowlist / blocked" | `N0/security/authorization.py:197-202` | Code |

Proposed: read Join as "who takes part, and who is removed".

### Considered and left out

- **Run parameters.** Always operator-set (Node0's signed `paramstore`, shipped configs), so they don't differ between projects.
- **Task and data assignment.** This is what nodes train on (data plane), not a coordination decision.

## Open questions

1. **Answered (code):** the DHT validators do **not** bind a peer's announcement to its assigned stage. In both projects the validators cover only a fixed set of record types (Node0: metrics, ports, paramstore; Agora: terminate), and every other key, including the averaging group key, passes under `allow_extra_keys=True` (`hivemind/dht/schema.py:80-82`); the signature validator applies only to records with an `[owner:]` marker, which the announcement lacks. See the Place cells. **Still untested:** the end-to-end effect of a foreign-stage announcement. A controlled local experiment (one honest node per stage plus one modified client announcing under a foreign key) is needed to show whether the round disrupts, degrades, or is poisoned. Give it its own output label.
2. In deployment, does Agora's auth or integrity service stop a modified client from advertising NODE mode?
3. When Agora groups disagree (late node), does the round fail, shrink, or produce a wrong average? This needs a local experiment with its own label.
