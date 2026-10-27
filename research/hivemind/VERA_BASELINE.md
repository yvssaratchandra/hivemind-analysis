# First Vera baseline: manual operator runbook

This experiment is **one private four-peer aggregation**, seed 1, honest
`baseline4` only. It is not training, a reordering attack, a resource benchmark,
or a public-swarm test. There is no automatic next experiment or retry.

Agents implement and test code locally. **The user runs every command on Vera**
and shares selected output for review. Agents have no cluster access; local
tests do not establish that this works on Vera.

| Implementation status | State |
|---|---|
| Native baseline and single-condition analysis | Reused; no native core changes |
| Slurm wrapper and bounded controller | Implemented; local verification recorded in HANDOFF |
| Vera account, checkout, environment and quota | Operator verified account/main/quota; setup 10549676 passed dependency/import checks |
| Vera preflight | Earlier failures 10549158/10549166 preserved; setup preflight passed |
| Vera baseline execution | 10549678 completed native work/cleanup but failed analysis due to omitted harness manifest; offline review pending |

## 1. Inspect first — user on the login node

Run from the clone and share output. Do not paste secrets or credentials. Review
pip configuration locally before sharing: index URLs can contain credentials.

```bash
cd "$(git rev-parse --show-toplevel)"
hostname
git status --short
git branch --show-current
git log -3 --oneline
projinfo
C3SE_quota
module list
python --version
```

The reviewed implementation must be on `main`. If the clone is still on the old
`research/net-security` branch, inspect local changes before switching. Pull only
while no experiment is using the checkout. Do not overwrite local changes.

Inspect `python -m pip config -v list` without publishing authentication details.
C3SE documents `--no-user`, `--no-cache-dir`, and `--no-build-isolation` defaults.
Confirm an authorized account for `vera` via `projinfo`; never copy an example
account. `C3SE_quota` reports both byte and file limits.

Before proceeding, review the selected account, current commit, sufficient quota,
and whether the existing environment already works. Preserve an existing
environment rather than reinstalling blindly.

## 2. Prepare software only if needed — user on Vera

Use the same `Python/3.11.5-GCCcore-13.2.0` module for creation and execution.
The expected venv is `research/hivemind/.cluster-venv`. Use a different explicitly
reviewed location only by updating the batch wrapper too. Never install into
`~/.local` or a shared module installation.

For a **new** environment, perform substantial installation/build work in a
separate setup allocation (one task, two CPUs, 8 GB, at most 15 minutes).
Do not run installations in the baseline job. Prepare CPU wheels/dependency
downloads before the allocation if compute nodes cannot reach package servers;
do not assume internet access there. Installation failures require diagnosis,
not repeated allocations or larger limits.

Commands to put in that setup job, after loading the Python module:

```bash
python -m venv research/hivemind/.cluster-venv
HM_PYTHON="$PWD/research/hivemind/.cluster-venv/bin/python"
"$HM_PYTHON" -m pip install 'setuptools>=64' wheel 'grpcio-tools>=1.33.2,<1.68'
"$HM_PYTHON" -m pip install torch==2.7.0 --index-url https://download.pytorch.org/whl/cpu
"$HM_PYTHON" -m pip install -r requirements.txt . psutil
"$HM_PYTHON" -m pip check
```

The explicit build dependencies account for Vera's no-build-isolation default.
The `<1.68` build requirement comes from this repository's `pyproject.toml`.
This is a starting installation recipe, **not a Vera-validated dependency lock**.
Stop at the first failure. Reusing an older installed Hivemind may require a
reviewed reinstall: preflight rejects native files that differ from the checkout.
Do not copy the Agora lock verbatim; it contains local machine paths.

**Existing incomplete Vera environment (job 10549166):** the installed inventory
contains CPU Torch, psutil and Torch's dependencies, but no NumPy or most of
`requirements.txt`. Hivemind's installed metadata has no declared requirements,
so `pip check` alone did not expose the omission. Retain the failed run evidence.
Use the reviewed dependency-repair job below, not another baseline submission:

```bash
sbatch --test-only -A ACTUAL_ACCOUNT research/hivemind/baseline_setup.slurm
sbatch -A ACTUAL_ACCOUNT research/hivemind/baseline_setup.slurm
```

Run the second command only if request validation succeeds. This single-node
setup job requests two CPUs, 8 GiB (effective memory subject to Vera's policy)
and 15 minutes, with no GPU or requeue. It repairs the **existing** venv, pins
the already installed `torch==2.7.0+cpu`, explicitly installs repository
requirements, avoids native-heavy source builds, and bounds network attempts
and pip stages. It runs `pip check`, checks every repository requirement against
installed metadata, and runs the native import preflight, which starts no peers.
It does not rebuild Hivemind or fix package metadata. If package-server access
or an available wheel is missing, stop and inspect the log; do not lift guards
or repeatedly resubmit. A failed pip operation may leave partial changes inside
the venv, which must be reviewed before further work.

Setup evidence is under `results/vera_baseline_setup_JOBID/`: installer logs,
input requirements, constraints, installed versions after successful installation,
explicit dependency check, import preflight and `exitcode.txt`. Share the Slurm
log and the failing stage's log. Success requires exit code zero and a successful
preflight; stop afterward to review before manually submitting any baseline.
The installation recipe itself has not been executed on Vera by an agent.

The local-only `.gitignore` in this directory excludes the venv and first-run
results. Keep bulky artifacts out of Git; the controller writes a hash inventory.

## 3. Review and submit — user from the repository root

First inspect the actual script. It requests **one node, one task, four Slurm
CPUs, an 8 GiB memory request, five minutes**, no GPU, no exclusive node and no requeue. C3SE states
that `-N 1` does not reserve an entire node. CPU affinity must contain exactly
four logical CPUs; the controller refuses a different binding rather than
assuming a CPU equals a physical core. If the check fails, inspect Slurm's
allocation/binding before changing the request.

**Observed Vera allocation:** preflight job 10549158 received four CPUs and
47,400 MiB despite the 8 GiB request. C3SE documents proportional memory for
partial-node jobs. The controller accepts exactly 8,192 MiB or this observed
47,400 MiB, records the assigned value, and still rejects other values or more
than four CPUs. The larger allocation is not a measured application requirement
or actual RAM consumption. Do not describe this as an 8 GiB enforced job limit.
The original job stopped before native imports/peers and before creating its
result directory, so its evidence is the operator-provided Slurm output, not a
missing successful run report. Retain that original log; do not overwrite it.

```bash
cat research/hivemind/baseline.slurm
bash -n research/hivemind/baseline.slurm
```

Replace `ACTUAL_ACCOUNT` below using the verified `projinfo` account. Submission
validation checks the request but does not prove the runtime works:

```bash
sbatch --test-only -A ACTUAL_ACCOUNT research/hivemind/baseline.slurm
```

Submit **one** job, then review its result before another submission:

```bash
sbatch -A ACTUAL_ACCOUNT research/hivemind/baseline.slurm
```

If setup uncertainty warrants a separate preflight allocation, submit with
`--preflight-only` instead. That job starts no peers; after review, the baseline
requires a separate manual submission. Do not submit both together.

```bash
sbatch -A ACTUAL_ACCOUNT research/hivemind/baseline.slurm --preflight-only
```

The normal baseline already runs preflight before starting peers. It compares
installed native source with the checkout, loads generated protobufs, checks
`p2pd` availability and verifies allocation arithmetic. Preflight does not
prove native transport will work; the baseline tests that.

Monitor, replacing `JOBID` with the submitted ID:

```bash
squeue --me
scontrol show job JOBID
tail -f slurm-JOBID.out
job_stats.py JOBID
```

Inspect the actual allocation as soon as it starts. Cancel if it reserves
unexpected resources, requests GPUs, or behaves unexpectedly:

```bash
scancel JOBID
```

`Ctrl+C` while following a log stops viewing, not the job. Short jobs can finish
before dashboard data appears. Do not lengthen the job simply to obtain samples.

## 4. Evidence and acceptance — user shares results, agent reviews

Outputs are under `research/hivemind/results/vera_baseline_JOBID/`, created without
overwriting earlier runs. Share `status.json`, `REPORT.md`, `validation.json`,
`controller_cleanup.json`, and relevant logs. Failures may lack reports; share
the status and failed stage's log instead. The inventory lists all retained
files with SHA256 hashes; it excludes itself.

```bash
cat research/hivemind/results/vera_baseline_JOBID/status.json
cat research/hivemind/results/vera_baseline_JOBID/controller_cleanup.json
sacct -j JOBID --format=JobID,State,ExitCode,Elapsed,AllocCPUS,ReqMem,MaxRSS
```

Acceptance requires all four peers to succeed, correct saved-input mean and
coordinate markers (`atol=1e-4`, `rtol=0`), valid roster/stream/provenance proofs,
zero fallback, zero bans, and no cleanup errors, forced cleanup or survivors.
The native worker must exit zero and have an explicit post-exit process audit.
The controller must exit zero with status `passed`; Slurm must report success.
Check all of these, not the portal's display label alone. MaxRSS may be missing
for a very short job; do not manufacture a measurement or automatically rerun.

The controller caps preflight at 30 seconds, native work at 120 seconds, audit
at 25 seconds and analysis at 45 seconds, within a 240-second stage deadline.
Final cleanup has a bounded grace period before the five-minute Slurm limit.
Thread limits are one per numerical library. Output is monitored at 256 MiB,
with a per-file hard limit; brief polling overshoot is possible. Core dumps are
disabled. Cancellation attempts cleanup of recorded PID/create-time pairs only;
hard kill/OOM can prevent evidence finalization, in which case acceptance fails.
Slurm remains the outer allocation/time boundary. Sampling cannot guarantee
observation of every extremely short-lived descendant; no claim of perfect
process containment is made from userspace monitoring alone.

Keep this small experiment on `/cephyr`; do not assume that this choice scales
to high-I/O experiments. Stop after one baseline. Review numerical results,
Slurm CPU/memory usage, trace size and cleanup before planning reordering.

## Offline provenance review of job 10549678 — no new allocation

The operator-provided report shows correct means, four successful peers and
clean shutdown; its only evidence error is `suite harness_hashes.json is missing`.
The new controller had saved `source_hashes.json` and frozen harness files but
omitted the analyzer's required basename-keyed `harness_hashes.json`. The writer
is now corrected. This is a launcher integration failure, not justification to
skip provenance validation or claim the failed job passed as submitted.

After pulling the correction, run this lightweight offline review from the
repository root. It imports no native Hivemind and submits/starts no jobs/peers:

```bash
research/hivemind/.cluster-venv/bin/python research/hivemind/run_vera_baseline.py \
  --review-run research/hivemind/results/vera_baseline_10549678 \
  --review-output research/hivemind/results/vera_baseline_10549678_review1
```

The review verifies the original inventory, successful worker and clean controller
audit. It cross-checks recorded source hashes against the original frozen files,
copies the run without overwriting anything, derives the missing manifest in
the copy, and reruns all existing single-condition checks. It rejects modified
or incomplete evidence and preserves other numerical/stream failures. It does
not extract an archive onto the filesystem or use the current working harness
as a substitute for the original recorded source.

Share these generated outputs:

```bash
cat research/hivemind/results/vera_baseline_10549678_review1/review.json
cat research/hivemind/results/vera_baseline_10549678_review1/REPORT.md
```

`review.json` is the derived offline review result. The original `status.json`
remains `failed`, including in the review copy; do not relabel the original job
as a successful execution. The derived inventory retains the original inventory
as `original_artifact_inventory.json` and records review-script provenance.
If review fails, share the error and stop; do not resubmit or alter raw evidence.
Native baseline acceptance remains pending until this review's output is checked.

## Sources

- [C3SE batch jobs](https://www.c3se.chalmers.se/documentation/submitting_jobs/running_jobs/)
- [Vera introduction: proportional memory for partial nodes](https://www.c3se.chalmers.se/documentation/first_time_users/intro-vera/slides/)
- [Python environments and pip defaults](https://www.c3se.chalmers.se/documentation/module_system/python/)
- [Storage, quota and temporary files](https://www.c3se.chalmers.se/documentation/file_transfer/filesystem/)
- [Monitoring](https://www.c3se.chalmers.se/documentation/submitting_jobs/monitoring/)
- [Open OnDemand, including job-status caveats](https://www.c3se.chalmers.se/documentation/connecting/ondemand/)
