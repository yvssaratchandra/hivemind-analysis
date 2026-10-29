# Pluralis Agora research workspace

Source: [PluralisResearch/agora](https://github.com/PluralisResearch/agora), the code
repository linked by [Agora: Collective and Permissionless Internet-Scale
Pretraining of Large Language Models](https://arxiv.org/abs/2607.13332).

Cloned on 2026-10-13 into `upstream/` as an independent Git repository with full
history. Checked-out branch: `main`. Commit:
`37d29a966528675635161007d7b43388012a98b2` (2026-07-04T18:12:04+09:30).
Machine-readable provenance is in [SOURCE.json](SOURCE.json).
The upstream checkout is excluded from the parent research repository.

Public dashboard histories are archived separately from the local experiments;
see [public-run evidence](PUBLIC_RUNS.md) and its hash inventory for the 8B and
13B runs. The existing source checkout and October 13 archives are preserved.

The upstream [README](upstream/README.md) credits Hivemind; its
[third-party license list](upstream/THIRD_PARTY_LICENSES.md) lists
`hivemind 1.2.0.dev0`. This establishes project lineage, not identical behavior
between Agora and this workspace's Hivemind fork.

Source entry points for later inspection:

- [All-reduce matchmaking](upstream/agora_server/src/agora_server/core/all_reduce/matchmaking.py).
- [Runtime configuration](upstream/run.json).
- [Server launcher](upstream/agora/src/agora/run_server.py).

The private eight-peer baseline is implemented in [baseline_8b.py](baseline_8b.py),
with a frozen-source launcher in [run_8b_baseline.py](run_8b_baseline.py) and an
independent numerical analyzer in [analyze_8b_baseline.py](analyze_8b_baseline.py).
See [HANDOFF.md](HANDOFF.md) for validation and run status.

The CPU runtime is isolated in `.runtime/` (Python 3.11, PyTorch 2.7). Its resolved
dependency lock and both native packages are copied into each result suite.
All peers bind to loopback and bootstrap from the first of the eight peers.
The harness does not join a public swarm or download models or training data.

The input is [RUN_8B_DATA_20261013.json](RUN_8B_DATA_20261013.json). Native protocol
timings are preserved; training uses synthetic two-second steps and a 100-value
parameter. Four reducers and four contributors use native matchmaking and
all-reduce. Each round averages five scalar values using the native sparse
partition selector. Both roles contribute snapshots; only reducers aggregate
chunks. Remote message legs use `(RTT_i + RTT_j) / 2` milliseconds, an explicit
assumption from sampled dashboard RTTs. This does not reproduce bandwidth,
the full model pipeline, public churn, or the public run's training throughput.

Prepare: `python research/agora/run_8b_baseline.py --prepare`.
Create a unique smoke label: `python research/agora/run_8b_baseline.py --phase smoke --label LABEL`.
The pilot requires its passed smoke suite with `--phase pilot --label LABEL --smoke PATH`.
Use `--request-stop PATH` to finish the current seed and shut down its peers.
Use `--resume PATH --dry-run` for offline validation, and `--resume PATH` to run
only unfinished seeds. Interrupted attempts remain intact and receive a new
attempt directory on rerun. An explicit resume clears the old pause request.
