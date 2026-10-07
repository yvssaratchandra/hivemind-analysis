# LocalAI preliminary baseline

Status: **passed**.

Source: `a3d555653c0cbfd73ab59ab15beab80903e96484`.

No models, inference, WAN delay, DHT discovery or adversarial injection are tested.

| Stage | Peer | Docker memory median MiB | Sampled max MiB | CPU median % |
| --- | --- | ---: | ---: | ---: |
| idle-1-peer | localai-peer-1 | 61.1 | 65.9 | 0.10 |
| idle-2-peer | localai-peer-1 | 65.5 | 66.2 | 0.07 |
| idle-2-peer | localai-peer-2 | 67.2 | 73.1 | 0.11 |
| idle-3-peer | localai-peer-1 | 65.2 | 65.6 | 0.17 |
| idle-3-peer | localai-peer-2 | 73.1 | 73.4 | 0.16 |
| idle-3-peer | localai-peer-3 | 65.5 | 69.4 | 0.18 |
| peer3-crashed | localai-peer-1 | 65.5 | 67.7 | 0.09 |
| peer3-crashed | localai-peer-2 | 73.2 | 74.2 | 0.13 |
| peer3-rejoined | localai-peer-1 | 67.8 | 68.0 | 0.15 |
| peer3-rejoined | localai-peer-2 | 73.9 | 74.3 | 0.11 |
| peer3-rejoined | localai-peer-3 | 61.2 | 61.7 | 0.22 |

Memory above is Docker's displayed container usage, not process RSS. Sampling includes startup/discovery; inspect timestamps to select a settled interval. Sampling probes briefly add curl/shell processes. Kernel memory.current, memory.peak and memory.events are retained in samples.jsonl where cgroup v2 is available.

Raw evidence: summary.json, commands.jsonl, source-healthcheck-test.json, and (when peers run) samples.jsonl, image.json, network.json and peer logs.
