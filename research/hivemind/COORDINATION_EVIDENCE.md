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

Every file:line citation below was checked on 2026-11-03 by a script confirming that the cited line contains the quoted token: 46 checks. Where a citation is a range, the token falls inside the range.

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

### Place — `O: stage`

| Claim | Evidence | Status |
|---|---|---|
| The auth server assigns the pipeline stage | `N0/security/authorization.py:183` `self.pipeline_stage = response["stage_type"]`; `N0/run_server.py:167` `pipeline_stage = str(authorizer.pipeline_stage)` | Code |
| The stage fixes the averaging key, a single fixed key per stage | `N0/server/matchmaking.py:65` `self._fixed_key = f"{self.prefix}.0b"` | Code |
| The group is every replica of the stage. The expected size is the stage roster (`<stage>.0.`) | `N0/server/matchmaking.py:175-176` `max_peers = len(self.peer_table)`; `:195` reads `self.prefix.split("_")[0] + ".0."`; `:214-250` collects announcements until `len(all_peerIds) == self.max_peers` or timeout | Code |
| The replica index (UID) does not affect grouping | `N0/server/matchmaking.py:252` the group is simply `sorted(all_peerIds)` | Code |
| A node's self-announcement is liveness, not placement | Same lines: a node is included only if it announced and was seen | Code |
| Can a node announce under a stage it was not assigned? | DHT validators not yet checked (`N0/security/validation.py`) | **Open** |

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

### Place — `O: stage`

| Claim | Evidence | Status |
|---|---|---|
| The auth server assigns both the stage and the exact expert UID | `AGC/security/authorizer.py:122-123` `self.pipeline_stage = response["stage_type"]`, `self.expert_uid = response["uid"]`; used at `AGC/run_server.py:271, 276` | Code |
| There is one group per stage per step. Group bits are fixed to empty, so every replica posts under the same step key | `AG/core/all_reduce/matchmaking.py:55` `self.group_bits = ""`; `:57-59` key `f"{prefix}_s{global_step}.0b"` | Code |
| The expected size is the stage roster (`<stage>.0.`) | `AG/core/all_reduce/matchmaking.py:179-189` `get_max_peers` counts entries at `prefix.split("_")[0] + ".0."` | Code |
| Each node waits up to its own deadline, then forms the group from whoever it has seen. No agreement step exists, so views can differ (late node, divergent DHT reads) | `AG/core/all_reduce/matchmaking.py:191-245` | Code |
| Mismatched views break the round rather than silently corrupt it: a sender missing from a reducer's list raises in `sender_peer_ids.index(...)`, and the constant `group_id` cannot detect the mismatch | `AG/core/all_reduce/ar_runner.py:333`; `matchmaking.py:239` | Inferred (untested) |
| Can a node announce under a stage it was not assigned? | DHT validators not yet checked | **Open** |

### Lead — `O: shipped config; C: advertised NODE mode`

| Claim | Evidence | Status |
|---|---|---|
| The peers that reduce are those in NODE mode; CLIENT peers get a zero share of the reduction | `AG/core/all_reduce/ar_runner.py:145-149` | Code |
| Workers ship with `averaging_mode: client` | `AGC/configs/default.yaml:31` | Code |
| The mode is **advertised by the peer itself**. A peer that advertises NODE receives reduction work | `research/agora/HANDOFF.md:3-13`; `research/agora/results/reducer_mode_20261015_c/REPORT.txt` (local, 3 loopback peers, no auth or integrity service) | Measured (local) |
| Whether the deployment's auth or integrity checks stop a modified client advertising NODE | Not tested | **Open** |
| Who runs the operator's NODE peers in deployment | Not in repo | Inferred (probably the operator) |

Correction: an earlier draft of this cell read `O: reducers`. The local probe shows reducer status is self-advertised, so a C component is required.

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

## Open questions

1. Do Node0 or Agora DHT validators stop a peer from announcing under a stage or UID it was not assigned, or from inflating the stage roster?
2. In deployment, does Agora's auth or integrity service stop a modified client from advertising NODE mode?
3. When Agora groups disagree (late node), does the round fail, shrink, or produce a wrong average? This needs a local experiment with its own label.
