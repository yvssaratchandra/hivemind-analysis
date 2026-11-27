# Coordination-layer vulnerabilities in federated, decentralized and hybrid training: literature survey

Compiled 2026-11-27. Scope: published or preprinted work in which an adversary attacks, or a defense protects,
the *coordination* of distributed training rather than only the values exchanged.

## How this was produced, and how far to trust it

- Three parallel web searches on 2026-11-27: server-based FL, peer-to-peer decentralized learning (DL), and
  hybrid or permissionless training. Each listed item was checked online at least at abstract level by the
  search agent. **None of these papers has been read in full** except *When Topology Betrays Privacy*
  (full PDF read 2026-11-26).
- Five items that bear most on novelty were re-checked independently: DMTT, LIFT, Pac-Man, BPI placement and
  AirTrojan. Corrections from that re-check are applied below.
- The *Decision* and *Strategy* columns are our classification against the model in
  `COORDINATION_EVIDENCE.md`, not the authors' wording. "Gap" statements are inferences from searches that found
  nothing. They are not a proof of absence. Re-check them before any novelty claim depends on one.
- Evaluation labels: *deployed* (a live public system), *testbed* (real nodes, controlled), *sim* (simulation or
  emulation), *theory*.

## Lens

Decisions (from `COORDINATION_EVIDENCE.md` and the draft framing in `HANDOFF.md`):
**Join** (admission, identity cost), **Place** (who works with whom: client selection, topology, peer sampling,
group or stage assignment), **Lead** (who runs or aggregates a round), **Combine** (whose updates enter, with
what weight), **Credit** (rewards, verification of work), **Sync/Progress** (deadlines, staleness, step counters).

Strategies: **claim** (an unverified self-report is read by a decision), **anticipate** (predict or grind
shared state or randomness), **Sybil/eclipse**, **malicious coordinator**, **placement** (the attacker chooses
which positions it holds), **correct-values** (harm through coordination while sending honest values).

## Main findings

1. **A malicious *server* steering selection is well covered in FL; a malicious *client* steering it is not.**
   Pasquini et al. (CCS 2022), Boenisch et al. (EuroS&P 2023), So et al. (AAAI 2023) and Lotto
   (USENIX Sec 2024) show that a server that picks participants can defeat secure aggregation. The defenses
   converge on verifiable randomness (VRF, blockchain, TEE audit). Client-side claim manipulation of selection
   has one direct instance: AirTrojan (forged channel state, IEEE CNS 2024). Lotto notes that Oort's
   self-reported loss and latency can be gamed. Inflated sample-count weighting appears only in mid-tier
   defense papers (FedRA, FedDua).
2. **In DL, robust aggregation assumes a bounded *local* Byzantine fraction on a *given* graph, and few papers
   ask how an attacker gets that fraction.** ClippedGossip says the global fraction is "irrelevant" and only
   adjacent weight matters. Bhattacharya et al. and Peng et al. show robust rules fail when local contamination
   exceeds global. Placement papers (Syros; Piaseczny; Gabrielli & Tolomei 2026) let the attacker *choose*
   positions as a budgeted compromise. They do not model *earning* a position through join, peer sampling or ID
   choice. GRANITE (peer-sampling flooding) and DMTT (false topology claims, testbed) come closest.
3. **Peer-sampling security is mature in P2P and blockchain work, but it measures view pollution, not learning
   damage.** This covers Brahms, BASALT, RAPTEE, AUPE, Hopper and Honeybee. LIFT (2026) finds that 2% Byzantine
   nodes subvert the Elevator hub-sampling protocol, and fixes it with a CSPRNG for hub selection. The abstract
   does not state the mechanism. Hopper's *delayed balanced attack* is a timing attack on debiasing that could
   carry over to learning.
4. **Topology-driven privacy is the most active DL thread in 2024–2026.** This covers El Mrini (ICML 2024),
   Touat (Middleware 2025), Feng (topology inference) and Yu et al. (CCS 2026). All of them assume the graph is
   given. No paper found combines *manipulating* placement with reconstruction.
5. **In hybrid and permissionless training, verification covers compute, not coordination.** Examples are the
   PoL attack/defense line, Verde, TOPLOC, SENTINEL and BTARD. None checks whether placement, liveness,
   throughput or progress claims were truthful, or whether witness or validator selection can be steered.
   BTARD is the only system that argues explicitly that its randomness (coin-tossing MPRNG) cannot be
   influenced. Biscotti seeds its VRF with the previous block hash and Psyche uses coordinator randomness;
   neither is analysed for grinding in the material read.
6. **Correct-values coordination attacks are rare.** Found so far:
   - Pac-Man (DL random walks, killed silently). The current version is self-marked incomplete.
   - Adversarial client unavailability (Su et al., theory).
   - FoolsGold's "training inflation" DoS.
   - Free-riding (Fraboni et al.).

   None measures this against a deployed system.
7. **No paper found measures attacks against a live permissionless training run.** Deployed systems
   (Gauntlet/Covenant, INTELLECT-2, Agora, Psyche, Petals) report training outcomes, not adversarial results.
   The only independent analysis of a deployed incentive layer is the Bittensor stake-concentration study, which
   is passive.

### Relevance to the draft research questions

| RQ | Closest prior work | What appears open (inferred) |
|---|---|---|
| RQ1 Exposure: decisions steerable by claims or predictable state | AirTrojan (claimed CSI), FedRA/FedDua (claimed quantity), DMTT (topology claims), ClippedGossip App. C.2 (claimed degree), LIFT (hub selection) | A cross-system model of coordination decisions and their input sources. Claim manipulation of deadlines, progress or stage announcements. Grinding of DHT IDs or keys used for matchmaking |
| RQ2 Cost in identities and compute | Sybil cost: FoolsGold, MPAF, SyDeLP (PoW+VDF), Biscotti/Bittensor (stake), BTARD (compute-proportional admission). Honeybee, BASALT and Brahms for overlays | Measured identity cost of steering coordination in *deployed* training code, including under DHT hardening |
| RQ3 Impact while sending correct values; robust aggregation accepts | Su et al. (unavailability, theory), Pac-Man (liveness), Raynal et al. (victim exclusion; correct values not confirmed), Allouah et al. (subsampling raises local Byzantine fraction, honest sampler assumed) | Empirical training harm from steered coordination with honest values, on real systems. Showing robust aggregation passes every round |

## A. Server-based federated learning

| Paper | Venue | Decision | Strategy | Type · Eval | Note |
|---|---|---|---|---|---|
| Pasquini et al., *Eluding Secure Aggregation in FL via Model Inconsistency* ([arXiv 2111.07380](https://arxiv.org/abs/2111.07380)) | CCS 2022 | Combine, Place | malicious coordinator | attack · sim | Server sends different models to different users and defeats any SA |
| Boenisch et al., *Reconstructing Individual Data Points in FL Hardened with DP and SA* ([arXiv 2301.04017](https://arxiv.org/abs/2301.04017)) | EuroS&P 2023 (venue via DBLP search result) | Join, Place | Sybil + malicious coordinator | attack · sim | Server-injected Sybil clients isolate a user. Root cause: users have no guarantee about who else participates |
| So et al., *Securing Secure Aggregation: Multi-Round Privacy Leakage* ([AAAI](https://ojs.aaai.org/index.php/AAAI/article/view/26177)) | AAAI 2023 | Place | curious coordinator | defense · theory+sim | Random per-round selection leaks individual models over rounds. Structured selection bounds it |
| Nguyen et al., *Blockchain-based Secure Client Selection in FL* ([arXiv 2205.05611](https://arxiv.org/abs/2205.05611)) | IEEE ICBC 2022 | Place, Join | malicious coordinator | attack+defense · sim | Biased selection recovers a target model. Selection enforced on chain |
| Jiang et al., *Lotto: Secure Participant Selection against Adversarial Servers* ([arXiv 2401.02880](https://arxiv.org/abs/2401.02880)) | USENIX Sec 2024 | Place, Join | malicious coordinator | defense · sim | VRF self-selection. Notes that Oort's reported metrics can be gamed |
| Hossain et al., *AdRo-FL* ([arXiv 2506.17805](https://arxiv.org/abs/2506.17805)) | arXiv 2025 | Place | malicious coordinator | defense · sim | Utility ranking plus VRF final pick |
| Wu et al., *DIST-FL* ([arXiv 2606.04899](https://arxiv.org/abs/2606.04899)) | arXiv 2026 | Place, Combine | malicious coordinator (TEE rollback) | attack+defense · WAN prototype | TEE servers can still bias selection via rollback |
| Takagi & Hasegawa, *TEE-based private FL with client auditing* ([arXiv 2509.08709](https://arxiv.org/abs/2509.08709)) | PoPETs 2026 (arXiv comment) | Place, Sync | malicious coordinator (forking) | defense · theory+sim | Clients audit TEE proofs of server actions |
| Kairouz et al., *DP-FTRL* ([PMLR](https://proceedings.mlr.press/v139/kairouz21b.html)) | ICML 2021 | Place | removes sampling assumption | design · theory+sim | Context: DP amplification depends on honest sampling |
| Allouah et al., *Byzantine-Robust FL: Client Subsampling and Local Updates* ([arXiv 2402.12780](https://arxiv.org/abs/2402.12780)) | ICML 2024 | Place | (analysis) | theory+sim | Subsampling raises the effective Byzantine fraction. Assumes an honest sampler |
| Lyu et al., *AirTrojan: Adversarial Attacks on FL Revisited, a Client-Selection Perspective* ([project page](https://xingyushu.github.io/AirTrojan)) | IEEE CNS 2024 | Place | **claim** (forged CSI) | attack · not stated | Raises own selection or denies a benign client. Re-checked |
| Yi et al., *FedRA: Robust Quantity-Aware Aggregation* ([arXiv 2205.10848](https://arxiv.org/abs/2205.10848)) | arXiv 2022 | Combine | **claim** (inflated quantity) | defense · sim | Defines quantity-enhanced attacks |
| Wu et al., *FedDua* ([arXiv 2504.12577](https://arxiv.org/abs/2504.12577)) | ICME 2025 (arXiv comment) | Combine | **claim** (misreported volume) | defense · sim | Estimates quantity from gradients |
| Fung et al., *FoolsGold: Limitations of FL in Sybil Settings* ([RAID](https://www.usenix.org/conference/raid2020/presentation/fung)) | RAID 2020 | Join, Combine, Sync | Sybil | taxonomy+defense · sim | Includes the "training inflation" DoS |
| Cao & Gong, *MPAF: Fake-Client Poisoning* ([arXiv 2203.08669](https://arxiv.org/abs/2203.08669)) | CVPR-W 2022 | Join | Sybil | attack · sim | Injected fake clients |
| Fraboni et al., *Free-rider Attacks on Model Aggregation* ([PMLR](https://proceedings.mlr.press/v130/fraboni21a.html)) | AISTATS 2021 | Combine, Credit | correct-values (fake participation) | attack · theory+sim | Free-riders still converge to the honest aggregate |
| Xu et al., *ACE: Poisoning Contribution Evaluation* ([arXiv 2405.20975](https://arxiv.org/abs/2405.20975)) | USENIX Sec 2024 | Credit | crafted updates | attack · sim | Fools five evaluation methods while keeping accuracy |
| Zheng et al., *Data Overvaluation Attack and Truthful Valuation* ([arXiv 2502.00494](https://arxiv.org/abs/2502.00494)) | arXiv 2025 | Credit | strategic reporting | attack+defense · theory+sim | Every linear valuation metric is manipulable |
| Wei et al., *Truth-FedBan* ([arXiv 2402.04485](https://arxiv.org/abs/2402.04485)) | ICLR 2024 | Join, Credit | **claim** (cost) | mechanism design · theory | Federated bandits, not FedAvg |
| Su et al., *FL with Adversarial Client Unavailability* ([arXiv 2305.19971](https://arxiv.org/abs/2305.19971)) | arXiv 2023 | Sync, Join | correct-values (selective silence) | theory | Tight error bound when an adversary drops clients |
| Fang et al., *AFLGuard* ([arXiv 2212.06325](https://arxiv.org/abs/2212.06325)) | ACSAC 2022 | Sync | poisoning under asynchrony | defense · sim | Context: staleness taken as given |

Context, not coordination attacks: Oort (OSDI 2021, utility-based selector); Bonawitz et al. system design
(MLSys 2019, pace steering); Bonawitz et al. SA (CCS 2017).

## B. Decentralized (peer-to-peer) learning

| Paper | Venue | Decision | Strategy | Type · Eval | Note |
|---|---|---|---|---|---|
| Pasquini, Raynal, Troncoso, *On the (In)security of P2P Decentralized ML* ([arXiv 2205.08443](https://arxiv.org/abs/2205.08443)) | IEEE S&P 2023 | Place, Combine | active neighbors | attack · sim | DL widens the attack surface. Privacy needs a fully connected graph |
| Raynal et al., *Can DL be more robust than FL?* ([arXiv 2303.03829](https://arxiv.org/abs/2303.03829)) | arXiv 2023 | Combine, Place | Byzantine; victim exclusion | attack · sim | DL is always less robust than FL. Correct-values not confirmed |
| Belal et al., *GRANITE* ([arXiv 2504.17471](https://arxiv.org/abs/2504.17471)) | arXiv 2025 | Place, Combine | Sybil/eclipse on peer sampling | defense · sim+theory | Threat explicitly includes manipulating peer sampling to amplify influence |
| Vaishnav et al., *DMTT: Trust-Aware Topology Learning for Dynamic DFL* ([arXiv 2608.03156](https://arxiv.org/abs/2608.03156)) | arXiv 2026 | Place, Combine, Credit | **claim** (topology claims) | defense · sim + testbed (ZeroMQ, research cloud) | Signed claims, witnesses, Beta trust. Re-checked: the abstract names topology manipulation but not specific lie types |
| Legheraba et al., *LIFT: Byzantine Resilient Hub-Sampling* ([arXiv 2601.20368](https://arxiv.org/abs/2601.20368)) | arXiv 2026 | Lead/Place (hub selection) | likely **anticipate** (inferred from CSPRNG fix) | attack+defense · sim | 2% Byzantine nodes subvert Elevator. FL is only a motivating application. Re-checked |
| Mukam et al., *Hopper* ([arXiv 2609.19893](https://arxiv.org/abs/2609.19893)) | IEEE NCA 2026 | Place, Sync | timing (delayed balanced attack) | attack+defense · sim | Peer sampling, not learning |
| Mukam et al., *AUPE* ([arXiv 2609.18563](https://arxiv.org/abs/2609.18563)) | NCA 2024 | Place | trusted nodes | defense · sim | |
| Zhang & Bojja Venkatakrishnan, *Honeybee* ([arXiv 2402.16201](https://arxiv.org/abs/2402.16201)) | arXiv 2024 | Place, Join | verifiable random walks | defense · sim | Argues Kademlia and GossipSub discovery are not Sybil-secure |
| Auvolat et al., *BASALT* ([arXiv 2102.04063](https://arxiv.org/abs/2102.04063)) | arXiv 2021 | Place, Join | eclipse | defense · theory + live crypto platform | |
| Bortnikov et al., *Brahms* ([PDF](https://people.csail.mit.edu/idish/ftp/Brahms-Comnet-Mar09.pdf)) | PODC 2008 / journal (unconfirmed) | Place | Byzantine membership | defense · theory+sim | |
| Pigaglio et al., *RAPTEE* ([arXiv 2203.04258](https://arxiv.org/abs/2203.04258)) | ICDCS 2022 (unconfirmed) | Place | TEE nodes | defense · sim | |
| Yu et al., *When Topology Betrays Privacy* ([arXiv 2609.08476](https://arxiv.org/abs/2609.08476)) | CCS 2026 | Place → privacy | colluding semi-honest | attack · sim (NetworkX graphs, one round) | Full text read. No deployed system, no Hivemind |
| El Mrini et al., *Privacy Attacks in Decentralized Learning* ([PMLR](https://proceedings.mlr.press/v235/mrini24a.html)) | ICML 2024 | Place → privacy | attacker position | attack · sim | Reconstructs data of non-neighbors |
| Touat et al., *DL vulnerability to MIA via graph mixing* ([arXiv 2412.12837](https://arxiv.org/abs/2412.12837)) | Middleware 2025 | Place, Combine | (measurement) | sim+theory | Dynamic graphs help |
| Feng et al., *Topology Inference Attack in DFL* ([arXiv 2501.03119](https://arxiv.org/abs/2501.03119)) | arXiv 2025 | Place (reconnaissance) | inference | attack · sim | |
| Syros et al., *Backdoor Attacks in P2P FL* ([arXiv 2301.09732](https://arxiv.org/abs/2301.09732)) | arXiv 2023 | Place | placement (centrality) | attack+defense · sim | |
| Piaseczny et al., *Adversarial Node Placement in DFL* ([arXiv 2511.06742](https://arxiv.org/abs/2511.06742)) | IEEE IoT J. 2025 (arXiv comment) | Place | placement | attack · sim+theory | |
| Gabrielli & Tolomei, *Optimizing Byzantine Node Placement in DFL* ([arXiv 2609.01495](https://arxiv.org/abs/2609.01495)) | arXiv 2026 | Place | placement under budget | attack · sim | Calls placement "a critical but under-modeled dimension" of DFL threat models. Re-checked |
| Werthenbach & Pouwelse, *SybilWall* ([arXiv 2306.15044](https://arxiv.org/abs/2306.15044)) | arXiv 2023 | Join, Combine | Sybil | defense · sim | |
| Mosqueda González et al., *SyDeLP* ([page](https://redeem-pepria.github.io/en/publication/mosquedagonzalez-mitigation-sybilbased-poisoning-2025-a)) | IEEE ICBC 2025 | Join, Combine | Sybil (PoW+VDF admission) | defense · sim | Borders hybrid |
| Martínez Beltrán et al., *Moving Target Defense in DFL* ([arXiv 2307.11730](https://arxiv.org/abs/2307.11730)) | Wireless Networks 2024 (unconfirmed) | Place | eclipse | defense · emulation (Fedstellar) | |
| Chen et al., *Random Walk Learning and the Pac-Man Attack* ([arXiv 2508.05663](https://arxiv.org/abs/2508.05663)) | arXiv 2025, v6 self-marked incomplete | Sync/Progress (walk liveness) | correct-values (silently kill walks) | attack+defense · theory+sim | Coordination-only attack. Treat as provisional. Re-checked |
| Biswas et al., *Shatter* ([PoPETs](https://petsymposium.org/popets/2025/popets-2025-0043.php)) | PoPETs 2025 | Place, Join | virtual identities (defense) | defense · sim+theory | Multi-identity by design, so it conflicts with Sybil counting |
| He et al., *ClippedGossip* ([arXiv 2202.01545](https://arxiv.org/abs/2202.01545)) | arXiv 2022 | Combine | **claim** (degree, App. C.2) | defense · theory+sim | Bounded adjacent Byzantine weight. "Global fraction irrelevant" |
| Gaucher et al., *Unified Breakdown Analysis for Byzantine Robust Gossip* ([PMLR](https://proceedings.mlr.press/v267/gaucher25a.html)) | ICML 2025 | Combine | — | defense · theory+sim | Guarantees depend on the honest subgraph |
| Bhattacharya et al., *Topology and Byzantine Resilience in DFL* ([arXiv 2407.05141](https://arxiv.org/abs/2407.05141)) | arXiv 2024 | Combine | — | measurement · sim | Robust rules fail on large sparse graphs |
| Peng et al., *Topology-Independent Robustness of the Weighted Mean* ([arXiv 2601.02682](https://arxiv.org/abs/2601.02682)) | arXiv 2026 | Combine | local vs global contamination | theory+sim | |
| Cyffers et al., *Muffliato* ([NeurIPS](https://proceedings.neurips.cc/paper/2022/hash/65d32185f73cbf4535449a792c63926f-Abstract-Conference.html)) | NeurIPS 2022 | Place → privacy | — | theory+sim | Privacy grows with graph distance, so attacker position matters |

## C. Hybrid and permissionless training

| Paper | Venue | Decision | Strategy | Type · Eval | Note |
|---|---|---|---|---|---|
| Ryabinin & Gusev, *Learning@home* ([arXiv 2002.04013](https://arxiv.org/abs/2002.04013)) | NeurIPS 2020 | Place, Join | — | system · sim | States unmodified Kademlia and defers security (`QA_NOTES.md` Q15) |
| Ryabinin et al., *Moshpit SGD* ([arXiv 2103.03239](https://arxiv.org/abs/2103.03239)) | NeurIPS 2021 | Place, Sync | — | system · experiments | No adversary in abstract |
| Diskin et al., *DeDLOC* ([arXiv 2106.10207](https://arxiv.org/abs/2106.10207)) | NeurIPS 2021 | Join, Sync, Place | — | system · real 40-peer run | Authentication in App. I.5 (not read) |
| Gorbunov et al., *BTARD: Secure Distributed Training at Scale* ([arXiv 2106.11257](https://arxiv.org/abs/2106.11257)) | ICML 2022 | Combine, Lead/Credit, Join, Sync | defends **anticipate** (MPRNG), Sybil heuristic | defense · experiments | Fixed Byzantine set <½, predefined peers (see `QA_NOTES.md`) |
| Ryabinin et al., *SWARM Parallelism* ([arXiv 2301.11913](https://arxiv.org/abs/2301.11913)) | ICML 2023 | Place (stage, routing) | — | system · experiments | Routing on local throughput estimates; failures, not adversaries |
| Borzunov et al., *Petals* ([arXiv 2209.01188](https://arxiv.org/abs/2209.01188)) | arXiv 2022 | Place, Credit | discusses malicious servers | system · deployed swarm | |
| Avraham et al., *Agora* ([arXiv 2607.13332](https://arxiv.org/abs/2607.13332)) | arXiv 2026 | Join, Place | — | system · deployed (330 nodes) | No threat model in abstract |
| Lidin et al., *Gauntlet / Templar* ([arXiv 2505.21684](https://arxiv.org/abs/2505.21684)) | NeurIPS 2025 OPT workshop | Credit, Combine, Join, Sync | **claim**, copying | incentive design · deployed (Bittensor) | |
| Lidin et al., *Covenant-72B* ([arXiv 2603.08163](https://arxiv.org/abs/2603.08163)) | arXiv 2026 | Join, Credit, Combine | — | system · deployed | |
| Quinque et al., *IOTA* ([arXiv 2507.17766](https://arxiv.org/abs/2507.17766)) | arXiv 2025 | Place, Credit, Combine | collusion detection | system · preliminary | Orchestrator trust unstated |
| Prime Intellect, *INTELLECT-2* ([arXiv 2505.07291](https://arxiv.org/abs/2505.07291)) | arXiv 2025 | Credit, Combine | **claim** (inference config) | system · deployed | TOPLOC verifies rollouts |
| Lu et al., *Robustness of Pipeline-Parallel Decentralized Training* ([PMLR](https://proceedings.mlr.press/v235/lu24c.html)) | ICML 2024 (position) | Place (stage trust) | Byzantine stage | attack+defense · sim | |
| Dai et al., *Activation Inversion in Decentralized Training* ([arXiv 2502.16086](https://arxiv.org/abs/2502.16086)) | arXiv 2025 | Place → privacy | curious stage | attack · experiments | |
| Ersoy et al., *Backdoor Attacks on Decentralised Post-Training* ([arXiv 2604.02372](https://arxiv.org/abs/2604.02372)) | ICLR 2026 workshop | Place | Byzantine stage | attack · experiments | |
| Dolatabadi et al., *SENTINEL* ([arXiv 2603.03592](https://arxiv.org/abs/2603.03592)) | arXiv 2026 | Combine/Credit | Byzantine stage | defense · 176 untrusted workers | |
| Jia et al., *Proof-of-Learning* ([arXiv 2103.05633](https://arxiv.org/abs/2103.05633)) | IEEE S&P 2021 | Credit | **claim** (training) | defense | |
| Zhang et al., *"Adversarial Examples" for PoL* ([arXiv 2108.09454](https://arxiv.org/abs/2108.09454)) | IEEE S&P 2022 | Credit | spoofing | attack | |
| Fang et al., *PoL is Currently More Broken Than You Think* ([arXiv 2208.03567](https://arxiv.org/abs/2208.03567)) | **EuroS&P 2023** | Credit | spoofing | attack + impossibility | Venue corrected (not S&P) |
| Zhao et al., *PoL with Incentive Security* ([arXiv 2404.09005](https://arxiv.org/abs/2404.09005)) | arXiv 2024 | Credit | collusion, verifier's dilemma | defense · theory | |
| Srivastava et al., *Optimistic Verifiable Training* ([arXiv 2403.09603](https://arxiv.org/abs/2403.09603)) | NeurIPS 2024 | Credit | — | defense | |
| Arun et al., *Verde* (Gensyn) ([arXiv 2502.19405](https://arxiv.org/abs/2502.19405)) | arXiv 2025 | Credit | — | defense | At least one honest provider |
| Ong et al., *TOPLOC* ([PMLR](https://proceedings.mlr.press/v267/ong25a.html)) | ICML 2025 | Credit | substitution | defense · experiments | Empirical, no formal guarantee |
| Shayan et al., *Biscotti* ([arXiv 1811.09904](https://arxiv.org/abs/1811.09904)) | arXiv 2018 | Lead (VRF+stake), Combine, Join | stake as Sybil bound | system+analysis · sim | VRF seeded by previous block hash (grinding not analysed) |
| Li et al., *BFLC* ([arXiv 2004.00773](https://arxiv.org/abs/2004.00773)) | arXiv 2020 | Lead (committee), Combine | collusion | system · sim | |
| Che et al., *CMFL* ([arXiv 2108.00365](https://arxiv.org/abs/2108.00365)) | IEEE TPDS 2022 | Lead (committee election), Combine | — | defense · theory+sim | |
| Hallaji et al., *TrustChain* ([arXiv 2502.16406](https://arxiv.org/abs/2502.16406)) | IEEE TBD (accepted) | Lead | rogue aggregator after election | defense · sim | |
| Alqattan et al., *Security Assessment of Hierarchical FL* ([arXiv 2408.10752](https://arxiv.org/abs/2408.10752)) | ICANN 2024 | Place (client-to-edge) | Byzantine clients | assessment · sim | |
| Tavallaie et al., *SHFL* ([arXiv 2409.15067](https://arxiv.org/abs/2409.15067)) | arXiv 2024 | Join, Combine | poisoning | defense · sim | |
| Lui & Sun, *Bittensor Protocol: Critical and Empirical Analysis* ([arXiv 2507.02951](https://arxiv.org/abs/2507.02951)) | arXiv 2025 (MARBLE) | Credit, Join | stake coalitions | measurement · on-chain data | Under 2% of wallets can command 51% in most subnets |
| Lui et al., *SoK: Blockchain-Based Decentralized AI* ([arXiv 2411.17461](https://arxiv.org/abs/2411.17461)) | IEEE SaTML 2026 | all | — | SoK | |

Non-peer-reviewed project material (labelled as such): Bittensor weight copying and commit-reveal (Opentensor
docs; CCS 2024 poster PDF hosted by the authors); Psyche architecture (Nous blog, May 2025: coordinator
randomness picks witnesses; verification called "an open problem"); Gensyn Verde blog; Petals security wiki.

## Unverified leads

Not read beyond search snippets, or venue unconfirmed:

- **Coordination-relevant, worth checking first**
  - Yang, *DFL Based on Committees and Blockchain* (arXiv 2205.11137). A snippet mentions a near-costless Sybil
    attack on deposit-based committee election.
  - ClusterGuard (IACR ePrint 2024/2082): VRF cluster selection.
  - PoCQ (arXiv 2606.05642): verifiable lottery for leader election.
  - Kaswan & Ulukus timestomping in age-sensitive gossip (arXiv 2212.14421, 2205.08510). Forged freshness claims;
    the nearest analogue to model-age claims in gossip learning.
  - Pisces (arXiv 1208.6326): a degree-inflation attack on random-walk sampling.
  - The original Elevator hub-sampling paper that LIFT attacks.
- **FL**
  - PoisonedFL.
  - LOKI (arXiv 2303.12233).
  - Flamingo (S&P 2023, arXiv 2308.09883): whether it handles server-forged dropouts.
  - AHSecAgg (arXiv 2312.04937).
  - Dordis (EuroSys 2024).
  - FedCS (ICC 2019).
  - Privacy amplification via random participation (arXiv 2205.01556).
- **Hybrid**
  - Gensyn litepaper (primary document not located).
  - FLock (arXiv 2507.15349).
  - PoGO (arXiv 2504.07540).
  - Trustless FL at Edge-Scale (arXiv 2511.21118).
  - SAPO / RL Swarm (arXiv 2509.08721).
  - Protocol Models (arXiv 2506.01260).
  - OpenDiLoCo and the INTELLECT-1 report.
  - Hivemind/Moshpit security appendices and DeDLOC App. I.5.
- **Surveys**
  - Hallaji et al., *DFL: A Survey on Security and Privacy* (IEEE TBD 2024).
  - Gabrielli et al. (arXiv 2308.04604).

## Suggested next reads, in order

1. Gabrielli & Tolomei 2026 and Syros et al.: the placement-attack framing nearest to RQ1/RQ3. Check whether any
   model an attacker *earning* position.
2. GRANITE and DMTT: the two learning papers that treat peer sampling or topology claims as the attack surface.
3. LIFT: find the mechanism behind the 2% subversion. If it is predictable selection, it is the closest prior
   instance of the anticipate strategy.
4. Lotto and Boenisch et al.: the FL side of "who chooses participants", and the VRF defense family relevant to
   the shared-randomness mitigation.
5. BTARD §3 and the Psyche post: how each claims its coordination randomness is unbiasable.
