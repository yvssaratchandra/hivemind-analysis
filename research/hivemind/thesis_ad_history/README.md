# THESIS_AD.md — reconstructed version history

The live advertisement is `../THESIS_AD.md`; the research direction behind it is
`../THESIS_TOPIC.md`. This folder is a record of how the ad got there, kept because the
reasoning behind several of the reversals is worth more than the final text.

Seven states the advertisement passed through on 22-23 September 2026. Only v6 and v7 were
ever committed; v1-v5 existed only in the working tree and were overwritten in place, so they
are reconstructed here from the session transcript. `v7_current-committed.md` duplicates
`../THESIS_AD.md` as of `869242b` — kept so the sequence can be read end to end without
switching files, but the live document is the one to edit.

**Provenance.** v1 and v2 are verbatim — they were written in single operations and the full
text is recorded. v3, v4 and v5 are *derived*: built by replaying the recorded edits onto v2 in
order. v6 and v7 come straight from git. The v5 -> v6 delta was checked against the round-2
critique items and matches exactly, which confirms the derived chain lands where git picks up.

| file | title | what changed, and why |
|---|---|---|
| `v1_hijacking-peer-discovery.md` | Hijacking Peer Discovery in Decentralized AI Training | First draft. **Abandoned for two reasons:** the title named the layer §4.12 shows is *not* where membership is decided, and RQ1/RQ2 handed students the supervisor's own open items (the `reject` control, roster equivocation) which will be finished long before January. |
| `v2_identity-binding-unvetted.md` | Identity Binding Flaws in Peer-to-Peer AI Platforms | Rebuilt around the three-part arc: find flaws in a small system, study how the mature ones avoid them, retrofit. Delegation corrected — Petals stays on the supervisor's track. Candidate targets still unvetted (Lattica, Exo, Kalavai). |
| `v3_vetted-candidates.md` | (same) | Targets vetted. **Kalavai rejected** (pool-based, seed node is a central control plane); **Exo rejected** (discovery is local-network only). SwarmLLM, KwaaiNet, Hyperspace added. Scope settled: AI-forward title, non-AI targets accepted. |
| `v4_why-framing.md` | (same) | Research question introduced: *"Why are known security requirements lost ... and how cheaply can they be restored?"* Relocated the unit of study from a project to the overlay-on-library boundary. |
| `v5_errors-corrected.md` | (same) | **Critique round 1.** Question narrowed to *where* (retrofit cost cannot establish intent). Krum stated correctly as `2f + 2 < n` rather than as a randomness assumption. Four findings split into four distinct guarantees. Numbers bounded — five and twenty named as `num_replicas` and `bucket_size`, "zero variance" → 40 swarms per cell, minting separated from interception latency, 8x split into 57 % election wins and β conditional on leading. "Very likely to find something" deleted. |
| `v6_second-critique.md` | (same) — **committed as `51994d1`** | **Critique round 2.** Mature-systems claims narrowed to the specific capability prevented; newcomer-freshness limit on signed sequence numbers stated. **Twenty corrected from "how many peers a lookup asks" to the seed set it traverses from** (`node.py:629-632`; `traverse_dht` expands beyond it). "Leaves the other three untouched" → guarantees independent, attacks share capabilities. RQ3 inverts to ablation if the guarantee is present. WP1 scoped to one workflow, WP4 gains an adaptive key-grinding attacker. |
| `v7_current-committed.md` | Control-Plane Security in Peer-to-Peer AI Platforms | **Committed as `869242b`.** Retitle only. Identity binding is one of four guarantees and the one the document says fixes least; a student taking the membership workflow would never touch it. |

## Open at the time of writing

Not yet folded in, and discussed after v7:

- **Two overlaps with the supervisor's own track.** WP3 (the comparative study of discv5 and
  the libp2p Kad-DHT) is the framework contribution itself, and the "optional extension" on
  where mature designs fall short is the control case — neither should be delegated.
- **Novelty is thinner than the ad implies.** Chosen-placement eclipse on Kademlia is a
  twenty-year-old, well-defended problem (S/Kademlia 2007; IPFS eclipse work published through
  2025). The unclaimed ground is election-and-admission group assembly.
- **The retrofit premise may be weak.** Keypair generation is microseconds, so key-derived
  identifiers raise the price of chosen placement far less than "grinding" suggests.
- **SwarmLLM already ships Sybil resistance** (Ed25519-signed reports, peer reputation), so it
  is a comparison case rather than low-hanging fruit.
- A proposed narrowing toward **peer selection** rather than identity binding, on the grounds
  that eclipse is saturated and selection is not.
- Placeholders never filled: supervisor, examiner, application deadline.
