#!/usr/bin/env bash
# Repair dependencies in the existing venv; never start averaging peers.
#SBATCH --job-name=hm-setup
#SBATCH --partition=vera
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G
#SBATCH --time=00:15:00
#SBATCH --no-requeue
#SBATCH --output=slurm-%j.out

set -eo pipefail
: "${SLURM_JOB_ID:?Submit this setup script through sbatch}"
test "${SLURM_JOB_NUM_NODES:-}" = 1
test "${SLURM_NTASKS:-}" = 1
test "${SLURM_CPUS_PER_TASK:-}" = 2
case "${SLURM_JOB_CPUS_PER_NODE:-}" in
  2|'2(x1)') ;;
  *) echo 'Refusing an unexpected CPU allocation' >&2; exit 1 ;;
esac
test -z "${SLURM_JOB_GPUS:-}"
cd "${SLURM_SUBMIT_DIR:?Submit from the repository root}"
test -f research/hivemind/announcement_probe.py
module purge
module load Python/3.11.5-GCCcore-13.2.0
export PYTHONNOUSERSITE=1
unset PYTHONPATH
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1
export BLIS_NUM_THREADS=1 NUMEXPR_NUM_THREADS=1 VECLIB_MAXIMUM_THREADS=1
export MAX_JOBS=1 CMAKE_BUILD_PARALLEL_LEVEL=1
export CUDA_VISIBLE_DEVICES=""
ulimit -c 0
HM_PYTHON="$PWD/research/hivemind/.cluster-venv/bin/python"
test -x "$HM_PYTHON"
RUN_DIR="$PWD/research/hivemind/results/vera_baseline_setup_${SLURM_JOB_ID}"
mkdir -p research/hivemind/results
mkdir "$RUN_DIR"
trap 'printf "%s\n" "$?" > "$RUN_DIR/exitcode.txt"' EXIT
module list
scontrol show job "$SLURM_JOB_ID"
git rev-parse HEAD > "$RUN_DIR/commit.txt"
cp requirements.txt "$RUN_DIR/requirements.txt"
cp research/hivemind/baseline_setup.slurm "$RUN_DIR/"

# Only repair the reviewed environment, not a different Python/GPU Torch build.
"$HM_PYTHON" - <<'PY'
import importlib.metadata as m
import sys
assert sys.version_info[:2] == (3, 11), sys.version
assert sys.prefix != sys.base_prefix, 'A virtual environment is required'
assert m.version('torch') == '2.7.0+cpu', 'Review Torch before changing this environment'
PY

cat > "$RUN_DIR/constraints.txt" <<'CONSTRAINTS'
torch==2.7.0+cpu
grpcio-tools>=1.33.2,<1.68
CONSTRAINTS

# Avoid native-heavy source builds and indefinite downloads. Existing CPU Torch
# must satisfy the exact constraint; no CUDA package is requested.
PIP_LIMITS=(--require-virtualenv install --retries 0 --timeout 20
  --constraint "$RUN_DIR/constraints.txt"
  --only-binary=torch,numpy,scipy,grpcio,grpcio-tools,cryptography,pydantic-core,uvloop,PyYAML)
timeout --kill-after=10s 180s "$HM_PYTHON" -m pip "${PIP_LIMITS[@]}" \
  'setuptools>=64' wheel 'grpcio-tools>=1.33.2,<1.68' \
  > "$RUN_DIR/build_dependencies.log" 2>&1
timeout --kill-after=10s 480s "$HM_PYTHON" -m pip "${PIP_LIMITS[@]}" \
  -r "$RUN_DIR/requirements.txt" psutil > "$RUN_DIR/install.log" 2>&1
timeout --kill-after=5s 30s "$HM_PYTHON" -m pip check > "$RUN_DIR/pip_check.log" 2>&1
timeout --kill-after=5s 30s "$HM_PYTHON" -m pip freeze > "$RUN_DIR/installed.txt"

# The installed Hivemind metadata on Vera omitted its dependencies. Check the
# repository requirements explicitly instead of relying only on `pip check`.
"$HM_PYTHON" - "$RUN_DIR/requirements.txt" > "$RUN_DIR/dependency_check.log" <<'PY'
import importlib.metadata as metadata
from pathlib import Path
import sys
from packaging.requirements import Requirement
for line in Path(sys.argv[1]).read_text().splitlines():
    if not line.strip() or line.lstrip().startswith('#'):
        continue
    req = Requirement(line)
    if req.marker and not req.marker.evaluate():
        continue
    version = metadata.version(req.name)
    if version not in req.specifier:
        raise RuntimeError(f'{req.name}=={version} does not satisfy {req}')
    print(f'{req.name}=={version}: OK')
assert metadata.version('torch') == '2.7.0+cpu'
PY

HM_SITE=$("$HM_PYTHON" -c 'import sysconfig; print(sysconfig.get_path("purelib"))')
timeout --kill-after=5s 45s "$HM_PYTHON" research/hivemind/announcement_probe.py \
  --package-root "$HM_SITE" --dependencies "$HM_SITE" --preflight \
  > "$RUN_DIR/preflight.log" 2>&1
echo "Dependency checks and import preflight passed: $RUN_DIR"
echo 'No averaging peers were started. Stop here and review the logs.'
