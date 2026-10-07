# DHT project survey

Scope: distributed training and inference applications with a DHT—a keyed directory whose ownership and lookup are distributed across nodes. P2P transport, gossip, replicated indexes, and hashing within one process do not establish DHT use.

Grouping **Yes** means node subsets form teams or pools within the larger network; full-network membership alone does not count.

Checked **2026-10-06**. Stars are rounded values from retrieved GitHub pages. Commit entries are the newest visible default-branch commits in **cached histories**, not verified live HEADs; these dates do not establish inactivity. Findings are code/document review, not attack tests. All **Control plane attacks** cells remain blank.

Public swarm status updated **2026-10-07**: **Yes** means evidence of a current publicly joinable network was found; **No** means no current public swarm was found after searching (not proof that none exists); **Not verified** means not separately checked, or a published network exists but its current availability remains unclear. Public inference APIs alone do not count.

## Qualifying projects

| Project | GitHub stars | Last recorded commit | DHT screening | Matchmaking / grouping | Public swarms | Control plane attacks |
|---|---:|---|---|---|---|---|
| [LocalAI](https://github.com/mudler/LocalAI) | ≈49.4k (Oct 6 snapshot) | 2026-10-06 · [690f399](https://github.com/mudler/LocalAI/commit/690f3994b04cee7616b23ac5e12306da077b8dcc) (Oct 6 snapshot) | **Yes** — [EdgeVPN/libp2p DHT discovery](https://github.com/mudler/LocalAI/blob/5537f4b1efbe4ec65702be0b80cff6e6b0875ea5/core/p2p/p2p.go). | **Yes** — [Network IDs group instances within a shared-token network](https://github.com/mudler/LocalAI/blob/5537f4b1efbe4ec65702be0b80cff6e6b0875ea5/core/cli/run.go). | No | |

LocalAI public swarms: historical community pools advertised, but the explorer and directory returned HTTP 521 on Oct 7; no current alternate pool invitation verified. Population remains unknown. [Check and evidence](../localai/PUBLIC_SWARM_CHECK.md).

## Screened candidates (excluded from the DHT survey)

| Project | GitHub stars | Last recorded commit (cached) | DHT screening | Matchmaking / grouping | Public swarms | Control plane attacks |
|---|---:|---|---|---|---|---|
| [Mooncake](https://github.com/kvcache-ai/Mooncake) | ≈6.7k | 2026-09-11 · [2ccf4cb](https://github.com/kvcache-ai/Mooncake/commits/main) | **No** — Elected master owns the keyspace; [hash shards are in-process](https://github.com/kvcache-ai/Mooncake/blob/main/mooncake-store/include/master_service.h). | **No** — [Replica placement](https://github.com/kvcache-ai/Mooncake/blob/main/docs/source/design/store/mooncake-store.md); no node-team/pool formation identified. | Not verified | |
| [llm-d](https://github.com/llm-d/llm-d) | ≈4.7k | 2026-09-11 · [f97c2c5](https://github.com/llm-d/llm-d/commits/main) | **No** — [Replicated local KV indexes or shared Redis/Valkey](https://github.com/llm-d/llm-d/blob/main/docs/architecture/advanced/kv-management/kv-indexer.md); Kubernetes discovery. | **Yes** — [InferencePool endpoint groups](https://github.com/llm-d/llm-d-router/blob/main/docs/discovery.md); cache/load-aware routing within a pool. | Not verified | |
| [LMCache](https://github.com/LMCache/LMCache) | ≈12.0k | 2026-09-11 · [05e122f](https://github.com/LMCache/LMCache/commits/dev) | **No** — [Central coordinator directory](https://docs.lmcache.ai/mp/coordinator.html); direct peer queries are not DHT lookups. | **No** — [One deployment-wide peer fleet](https://github.com/LMCache/LMCache/blob/dev/docs/source/mp/p2p.rst); no subgroups identified. | Not verified | |
| [exo](https://github.com/exo-explore/exo) | ≈47.8k | 2026-08-25 · [21a54c5](https://github.com/exo-explore/exo/commits/main) | **No** — Current [multicast/Zenoh discovery](https://github.com/exo-explore/exo/blob/main/rust/networking/src/discovery.rs); no DHT identified. | **Yes** — [Topology/resource-based device groups](https://github.com/exo-explore/exo/blob/main/src/exo/master/placement.py) for model sharding. | Not verified | |
| [Dynamo](https://github.com/ai-dynamo/dynamo) | ≈8.2k | 2026-09-10 · [5d66517](https://github.com/ai-dynamo/dynamo/commits/main) | **No** — [Kubernetes/KV-store discovery](https://github.com/ai-dynamo/dynamo/blob/main/lib/runtime/src/distributed.rs); router index sharding is local. | **Yes** — [Namespace/component/endpoint worker groups](https://github.com/ai-dynamo/dynamo/blob/main/docs/fern/pages/developer-guide/knowledge-base/concepts/system-architecture/distributed-runtime.md); cache-aware routing. | Not verified | |
| [PANDA](https://arxiv.org/html/2609.38482v1) | — | — | **Not Sure** — Local capability registries propagated by GossipSub; no DHT described. Agent orchestration, outside tensor-training/serving scope. | **Yes** — Capability-matched teams; least-loaded candidates; member replacement. | Not verified | |

PANDA: public code release **unverified** after expanded paper/author/lab/repository searches (2026-10-06); no repository or release statement found. The first author's [GitHub account](https://github.com/mlaws21) is identified in his [résumé](https://mattdlaws.com/resume.pdf), but its repository listing could not be retrieved. Stars/commit remain unverified.

Deferred: [Walrus (2505.05370)](https://arxiv.org/abs/2505.05370) is decentralized storage, outside this phase. Its DHT references concern IPFS in related work, not evidence of Walrus using a DHT.

## Decentralization check

Checked **2026-10-06**. **Yes** means the network's coordination can operate without a required network-wide central registry, scheduler, or authority. An elected/replicated central role still counts as central; bootstrap peers and per-task leaders alone do not. This property is separate from DHT use.

| Project | Without a network-wide central service? | Architecture and qualification |
|---|---|---|
| Mooncake | **No** | [Store requires a master](https://github.com/kvcache-ai/Mooncake/blob/main/docs/source/deployment/mooncake-store-deployment-guide.md); HA elects an active primary. Alternative [P2P Store](https://kvcache-ai.github.io/Mooncake/design/p2p-store.html) removes the master but requires etcd for global metadata. |
| PANDA | **Yes — paper design** | [Local registries, GossipSub, local scheduling and web-of-trust](https://arxiv.org/html/2609.38482v1); no network-wide authority. Public implementation remains unverified. Star orchestration has a per-team coordinator; genesis-anchor mode shares an initial trust root, while self-anchor mode permits local trust anchors. |
| llm-d | **No** | [Router/EPP](https://github.com/llm-d/llm-d/blob/main/docs/architecture/README.md) controls request placement; standard [discovery](https://github.com/llm-d/llm-d-router/blob/main/docs/discovery.md) uses Kubernetes InferencePool/Pods. Standalone file/custom discovery replaces membership input, not routing authority. P2P KV transfers and replicated indexes do not remove these roles. |
| LMCache | **No — shared P2P mode** | [P2P configuration requires a coordinator](https://docs.lmcache.ai/mp/configuration.html), which maintains [fleet membership and the key directory](https://docs.lmcache.ai/mp/coordinator.html). Local caching can omit it, but does not provide a decentralized shared peer fleet. |
| exo | **No — elected central role** | Peers [elect a swarm-wide master](https://github.com/exo-explore/exo/blob/main/src/exo/main.py), which performs [placement and task selection](https://github.com/exo-explore/exo/blob/main/src/exo/master/main.py). No fixed external master server is required; peer discovery and master failover do not make scheduling coordinator-free. |

Mooncake's standalone [Transfer Engine](https://github.com/kvcache-ai/Mooncake/blob/main/docs/source/design/transfer-engine/index.md) supports `P2PHANDSHAKE` direct transfers without a shared metadata service when endpoints/buffer details are exchanged out of band. This is a transport capability, not a coordinator-free serving/cache network.

PANDA is the closest architectural match. Its paper's benchmark uses a hosted LLM and an MCP tool server, so **Yes** above concerns agent coordination, not a verified end-to-end server-free deployment. It orchestrates agents rather than distributing a model's tensor inference across peers.
