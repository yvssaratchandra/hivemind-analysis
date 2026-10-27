#!/usr/bin/env bash
#SBATCH --job-name=hm-baseline
#SBATCH --partition=vera
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=8G
#SBATCH --time=00:05:00
#SBATCH --no-requeue
#SBATCH --output=slurm-%j.out

# Vera may replace this memory request with its proportional per-CPU share.
# The controller accepts 8192 or the observed 47400 MiB; it still requires 4 CPUs.

set -eo pipefail
cd "${SLURM_SUBMIT_DIR:?Submit from the repository root}"
test -f research/hivemind/run_vera_baseline.py
module purge
module load Python/3.11.5-GCCcore-13.2.0
HM_PYTHON="$PWD/research/hivemind/.cluster-venv/bin/python"
test -x "$HM_PYTHON"
export PYTHONNOUSERSITE=1
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1
export BLIS_NUM_THREADS=1 NUMEXPR_NUM_THREADS=1 VECLIB_MAXIMUM_THREADS=1
export CUDA_VISIBLE_DEVICES=""
ulimit -c 0
module list
"$HM_PYTHON" --version
scontrol show job "$SLURM_JOB_ID"
HM_SITE=$("$HM_PYTHON" -c 'import sysconfig; print(sysconfig.get_path("purelib"))')
# Optional --preflight-only runs the same checks without starting native peers.
# The controller rejects unknown arguments, including attack/seed/sweep flags.
srun --ntasks=1 --cpus-per-task=4 "$HM_PYTHON" \
  research/hivemind/run_vera_baseline.py \
  --package-root "$HM_SITE" --dependencies "$HM_SITE" "$@"
