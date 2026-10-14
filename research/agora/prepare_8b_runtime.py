"""Prepare an isolated supported CPU runtime; never import a public launcher."""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import tarfile

ROOT = Path(__file__).resolve().parent
RUNTIME = ROOT / '.runtime'
HM_COMMIT = '4d5c41495be082490ea44cce4e9dd58f9926bb4e'
AGORA_COMMIT = '37d29a966528675635161007d7b43388012a98b2'


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def prepare():
    revision = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT/'upstream', text=True).strip()
    if revision != AGORA_COMMIT or subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT/'upstream'):
        raise RuntimeError('Agora checkout must be clean at the pinned revision')
    RUNTIME.mkdir(exist_ok=True)
    env = dict(os.environ, UV_CACHE_DIR='/tmp/agora8b-uv-cache',
               UV_PYTHON_INSTALL_DIR='/tmp/agora8b-python')
    python = RUNTIME / 'venv/bin/python'
    if not python.exists():
        subprocess.run(['uv', 'python', 'install', '3.11'], env=env, check=True)
        subprocess.run(['uv', 'venv', '--python', '3.11', str(RUNTIME/'venv')], env=env, check=True)
    source = RUNTIME / 'hivemind-source'
    if not source.exists():
        source.mkdir()
        archive = subprocess.check_output(['git', 'archive', HM_COMMIT], cwd=ROOT.parents[1])
        with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
            tar.extractall(source, filter='data')
    marker = RUNTIME / 'prepared.json'
    if marker.exists():
        previous = json.loads(marker.read_text())
        if previous.get('hivemind_commit') != HM_COMMIT or previous.get('agora_commit') != AGORA_COMMIT:
            raise RuntimeError('Prepared runtime source revision differs; use a new isolated runtime.')
        if sha256(RUNTIME/'requirements.lock') != previous['lock_sha256']:
            raise RuntimeError('Prepared runtime dependency lock changed')
        print('Runtime already prepared:', python, flush=True)
        return
    subprocess.run(['uv', 'pip', 'install', '--python', str(python), 'torch==2.7.0',
                    '--index-url', 'https://download.pytorch.org/whl/cpu'], env=env, check=True)
    build_constraints = RUNTIME / 'build-constraints.txt'
    build_constraints.write_text('setuptools==80.9.0\n')
    subprocess.run(['uv', 'pip', 'install', '--python', str(python), str(source),
                    str(ROOT/'upstream/agora_server'), 'pytest', 'numpy<2.4',
                    '--build-constraint', str(build_constraints)], env=env, check=True)
    # Freeze resolved packages after installation, before a trial can use them.
    lock = subprocess.check_output(['uv', 'pip', 'freeze', '--python', str(python)], env=env)
    (RUNTIME/'requirements.lock').write_bytes(lock)
    probe = subprocess.check_output([str(python), '-c',
        "import sys,torch,hivemind; from agora_server.core.all_reduce.matchmaking import Matchmaking; "
        "from agora_server.core.all_reduce.ar_runner import AllReduceRunner; "
        "from agora_server.core.averaging.state_averager_sparta import PartitionedIndexSelector; "
        "import json; print(json.dumps(dict(python=sys.version,torch=torch.__version__,hivemind=hivemind.__file__)))"],
        cwd='/tmp')
    marker.write_text(json.dumps(dict(hivemind_commit=HM_COMMIT, agora_commit=AGORA_COMMIT,
                                     probe=json.loads(probe), lock_sha256=sha256(RUNTIME/'requirements.lock')),
                                 indent=2)+'\n')
    print('Prepared:', marker, flush=True)


if __name__ == '__main__':
    prepare()
