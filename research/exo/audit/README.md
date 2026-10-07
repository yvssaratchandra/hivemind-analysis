# Initial local Exo control-plane audit

Two honest Exo coordinators and one controlled Router publisher on a private Docker bridge. **No models, downloads, inference workers, training, or upstream edits.** The honest APIs are read locally inside their containers; no ports are published to the host. The attacker has no Node, Master, Worker, API, or election role.

The first question is whether the coordinator binds a resource advertisement's claimed node identity to its publisher. Session handling and duplicate indexing provide controls. Synthetic RAM metadata can reveal state acceptance; it does not establish actual memory availability, inference impact, or a violated security boundary.

## Implementation status

| Step | Status |
| --- | --- |
| Private three-peer configuration and bounded probe | Implemented |
| Evidence parser, control validation, runner regression checks | 37 stdlib tests passed |
| Docker image build | [Succeeded](../results/exo-audit-build-20261006-004/build-result.json) |
| Real peer discovery, election, probe, resource measurements | [All five arms completed](../results/exo-audit-20261006-005/REPORT.md), one local trial each |

Source reviewed: `21a54c5ea0230a3bec1e1a786d200126c7e34ec6`, 2026-10-06. Live results must be checked before making empirical claims.

[Measured report](../results/exo-audit-20261006-005/REPORT.md): self and forged-identity advertisements appeared in both honest peers' state; the constructed session mismatch was not observed; exact replay produced one observed global index. Sessions and peer IDs stayed stable. Inference impact and a security vulnerability remain unvalidated.

[Preparation evidence](../results/exo-audit-setup-20261006-001/validation.json) records these host checks and prerequisite limitations; it contains no peer experiment results.

[Docker access retry](../results/exo-docker-access-20261006-155823/preflight.json)
on 2026-10-06 found the client, Compose, and socket present. File access checks
passed, but [socket connect](../results/exo-docker-access-20261006-155823/connect-stage.json)
returned `EPERM` while this Codex session reported networking disabled. No image
build or peer experiment started. Available RAM was 11.4 GiB, above the initial
8 GiB planning budget. That access failure was resolved before the successful
build and run linked above. Runtime cgroup peaks were captured; build peak RAM
remains unmeasured.

Attempt [004](../results/exo-audit-20261006-004/summary.json) preserved connected
peer logs but stopped before publication: ANSI color sequences broke the
observer's startup-ID parser. After fixing and testing the parser, attempt
[005](../results/exo-audit-20261006-005/summary.json) completed. Both attempts'
containers and networks were [removed](../results/exo-audit-20261006-005/cleanup-verification.json),
with state volumes and evidence retained.

## Run

From the repository root, with the clean independent `exo/` checkout, Python 3, Docker Engine, and Compose v2 available:

```bash
python3 research/exo/audit/run.py preflight
python3 research/exo/audit/run.py build --label exo-audit-build-20261006-006
python3 research/exo/audit/run.py run --label exo-audit-20261006-006 --seed 1901
```

Use a **fresh label on every retry**; existing evidence directories and project state are refused. The image is tagged and checked against the full source commit; reuse the existing image and skip the build command when the source revision has not changed. The shared [Dockerfile](../two-peer/Dockerfile) supplies Python 3.13 and native build dependencies, with the `mlx-none` extra. Building needs Internet access; peer runtime uses an [internal network](https://docs.docker.com/reference/compose-file/networks/#internal).

The chosen runtime budget is **8 GiB available RAM**: three 2 GiB container caps plus 2 GiB spare. The successful run's preflight reported **18.8 GiB available host RAM**, so clearing RAM was unnecessary. Captured container-lifetime peaks reached **143.8 MiB for an honest peer and 96.4 MiB for the publisher**, with zero OOM events across all 15 containers; see [raw resource figures](../results/exo-audit-20261006-005/resource-summary.json). These are short, model-free measurements, not minimum requirements or inference budgets. Build separately, allowing an unmeasured 8 GiB; its peak remains unknown. Caps are ceilings, not allocations. This configuration sets memory plus swap to the same cap to disable container swap where supported; see [Docker memory limits](https://docs.docker.com/engine/containers/resource_constraints/#memory). Host-visible figures may differ from a restricted session's effective allowance.

Working IPv6 multicast on the Docker bridge is required. Startup failure or election disagreement stops publication and preserves logs. An empty model-free topology alone does not prove a connection failure. Change neither the test scope nor network to work around failed discovery; diagnose the saved startup evidence first.

## Arms

Each arm creates fresh peer state, a separate namespace and network, and attaches the Router before waiting for honest master/session agreement. The Router's readiness means its local subscriptions are established; the positive control tests remote delivery.

| Arm | Publication | Interpretation |
| --- | --- | --- |
| `baseline` | None; attacker transport attached | No-send control with the same third transport |
| `self` | One synthetic RAM advertisement for the publisher's own ID | Positive control: master acknowledgement and matching state at both honest peers |
| `forged` | Same type of advertisement claiming `honest-b` | Tests acceptance of another node's claimed identity |
| `stale-session` | Own-ID advertisement with election clock reduced by one | Absence is recorded as not observed, not proven rejection |
| `replay` | Exact same own-ID envelope twice; second send waits for the first indexed acknowledgement | Counts distinct global indices; duplicate receipt at one index is not reindexing |

Default order is the table order. A reduced run can use `--arms baseline,self,forged`; every non-baseline run must include `self`. The seed selects a deterministic event marker and synthetic available-memory value; actual transport IDs, clocks, and timestamps are recorded rather than assumed deterministic. At most two publications occur per arm. The attacker stays connected through observation, then the runner stops its own project.

## Evidence and limits

Evidence goes to `research/exo/results/<label>/`. The top level retains preflight, source/image information, lab hashes and copies, settings, and `summary.json`. Each arm saves resolved Compose configuration, command records, convergence checks, before/after API state, exact published/received envelopes, peer logs, container inspection, sampled Docker statistics, and cgroup memory peaks/events when supported. Sampling alone can miss short peaks.

An acceptance observation requires a matching event ID, session and master origin in the indexed global stream, plus the synthetic marker in both honest snapshots. Honest identities and the committed session must remain unchanged across publication. A failed positive control, partial run, missing evidence, or cleanup failure prevents a successful run status. Exit codes: `0` complete observations, `1` incomplete/inconclusive run, `2` prerequisite/configuration error. None of these is a vulnerability verdict.

Cleanup removes only the run's containers and private network. Named state volumes and host evidence are preserved; a project with pre-existing containers or volumes is not started or cleaned. If the host runner is interrupted, inspect `commands.jsonl` and the project printed there before manually stopping it. The publisher has a finite timeout, but honest containers may remain after a hard interruption.

These are local protocol experiments with synthetic metadata. Command authority, election manipulation, partitions, WAN delay, and inference impact remain future experiments. The current setup does not contact public swarms.

Host-only verification:

```bash
python3 -m unittest discover -s research/exo/audit/tests -v
```

Relevant source: [Router](../../../exo/src/exo/routing/router.py), [forwarder envelopes](../../../exo/src/exo/shared/types/events.py), [master indexing](../../../exo/src/exo/master/main.py), and [state application](../../../exo/src/exo/shared/apply.py).
