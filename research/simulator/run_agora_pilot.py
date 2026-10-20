#!/usr/bin/env python3
"""Run exactly one honest Agora pilot with a wall-clock cap; never retry/tune."""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--label', required=True)
    args = parser.parse_args()
    if not args.label or any(ch not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-' for ch in args.label):
        parser.error('Use an alphanumeric label with underscores or hyphens')
    path = ROOT/'results'/args.label
    if path.exists():
        parser.error(f'Refusing to overwrite {path}')
    command = [sys.executable, '-m', 'trainsim', 'run', '--config',
               str(ROOT/'configs/agora_single_stage.json'), '--seed', '1', '--label', args.label]
    started = time.perf_counter()
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=600)
        evidence = dict(command=command, wall_limit_seconds=600, returncode=result.returncode,
                        stdout=result.stdout, stderr=result.stderr, timed_out=False)
    except subprocess.TimeoutExpired as exc:
        evidence = dict(command=command, wall_limit_seconds=600, returncode=None, timed_out=True,
                        stdout=(exc.stdout or b'').decode() if isinstance(exc.stdout, bytes) else exc.stdout,
                        stderr=(exc.stderr or b'').decode() if isinstance(exc.stderr, bytes) else exc.stderr)
    evidence['wall_seconds'] = time.perf_counter()-started
    if path.exists():
        (path/'execution.json').write_text(json.dumps(evidence, indent=2)+'\n')
        if evidence['timed_out'] and (path/'manifest.json').exists():
            manifest = json.loads((path/'manifest.json').read_text())
            manifest.update(status='wall_timeout',
                            error='Child exceeded 600 wall seconds; trace may be incomplete. No retry was made.')
            (path/'manifest.json').write_text(json.dumps(manifest, indent=2)+'\n')
    print(json.dumps(evidence, indent=2))
    return 0 if evidence['returncode'] == 0 else 2


if __name__ == '__main__':
    raise SystemExit(main())
