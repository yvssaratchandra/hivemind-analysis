#!/usr/bin/env python3
"""One bounded, private baseline in a verified Slurm allocation.

No submission, installation, retry, public bootstrap, or attack entry point.
The offline --analyze-case mode starts no peers and needs no Slurm allocation.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import resource
import runpy
import shutil
import signal
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
OUTPUT_LIMIT = 256 * 1024 * 1024
# 8192 is our request. Vera job 10549158 assigned 47400 MiB with four CPUs
# (site-documented proportional node memory). Do not accept arbitrary increases.
ALLOWED_MEMORY_MIB = (8192, 47400)
THREAD_VARS = ('OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS',
               'BLIS_NUM_THREADS', 'NUMEXPR_NUM_THREADS', 'VECLIB_MAXIMUM_THREADS')
STOP_SIGNAL = None


def write(path, data):
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(data, indent=2, allow_nan=False) + '\n')
    temporary.replace(path)


def allocation(env):
    """Fail closed before any native import; Slurm CPUs need not be physical cores."""
    expected = {'SLURM_JOB_NUM_NODES': '1', 'SLURM_NTASKS': '1',
                'SLURM_CPUS_PER_TASK': '4'}
    if not env.get('SLURM_JOB_ID', '').isdigit():
        raise ValueError('A Slurm job is required; do not launch peers on a login node')
    for key, value in expected.items():
        if env.get(key) != value:
            raise ValueError(f'Unexpected allocation: {key}={env.get(key)!r}; expected {value}')
    if env.get('SLURM_MEM_PER_NODE') not in tuple(map(str, ALLOWED_MEMORY_MIB)):
        raise ValueError(f'Unexpected SLURM_MEM_PER_NODE={env.get("SLURM_MEM_PER_NODE")!r}; '
                         f'allowed MiB: {ALLOWED_MEMORY_MIB}')
    if env.get('SLURM_JOB_CPUS_PER_NODE') not in ('4', '4(x1)'):
        raise ValueError('Expected four total allocated CPUs; refuse a larger/exclusive allocation')
    if not env.get('SLURM_JOB_ACCOUNT'):
        raise ValueError('Missing Slurm account')
    if env.get('SLURM_JOB_PARTITION') != 'vera':
        raise ValueError('Expected the verified vera partition')
    if env.get('SLURM_JOB_GPUS') or env.get('SLURM_STEP_GPUS'):
        raise ValueError('This experiment must not allocate GPUs')
    affinity = len(os.sched_getaffinity(0))
    if affinity != 4:
        raise ValueError(f'Expected affinity to four allocated CPUs, got {affinity}')
    return {key: value for key, value in env.items()
            if key in (*expected, 'SLURM_JOB_ID', 'SLURM_JOB_ACCOUNT',
                       'SLURM_JOB_PARTITION', 'SLURM_JOB_NODELIST', 'SLURM_JOB_CPUS_PER_NODE',
                       'SLURM_MEM_PER_NODE')}


def child_limits():
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    resource.setrlimit(resource.RLIMIT_FSIZE, (OUTPUT_LIMIT, OUTPUT_LIMIT))


def output_bytes(directory):
    total = 0
    for path in directory.rglob('*'):
        try:
            if path.is_file():
                total += path.stat().st_size
        except FileNotFoundError:
            pass  # A supervised stage can atomically rename a temporary JSON file.
    return total


def live_owned(owned):
    import psutil
    live = []
    for pid, created in owned.items():
        try:
            proc = psutil.Process(int(pid))
            if proc.create_time() == created and proc.status() != psutil.STATUS_ZOMBIE:
                live.append(proc)
        except psutil.NoSuchProcess:
            pass
    return live


def cleanup(owned, grace=15):
    """Only exact PID/create-time matches; no pkill, user sweep or shared sockets."""
    import psutil
    until = time.monotonic() + grace
    while live_owned(owned) and time.monotonic() < until:
        time.sleep(.1)
    forced = []
    for proc in live_owned(owned):
        forced.append(proc.pid)
        try:
            proc.terminate()
        except psutil.NoSuchProcess:
            pass
    _, remaining = psutil.wait_procs(live_owned(owned), timeout=2)
    for proc in remaining:
        try:
            proc.kill()
        except psutil.NoSuchProcess:
            pass
    psutil.wait_procs(remaining, timeout=2)
    return {'forced_cleanup': forced, 'surviving_pids': [p.pid for p in live_owned(owned)]}


def bounded(command, directory, name, seconds, deadline, owned):
    """Track descendants while alive, including when the worker fails before shutdown."""
    import psutil
    reason = None
    started = time.monotonic()
    with (directory / f'{name}.log').open('wb') as log:
        process = subprocess.Popen(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
                                   start_new_session=True, preexec_fn=child_limits)
        try:
            parent = psutil.Process(process.pid)
            owned[parent.pid] = parent.create_time()
        except psutil.NoSuchProcess:
            parent = None  # A short metadata command can already have exited.
        try:
            while True:
                try:
                    for child in parent.children(recursive=True) if parent else ():
                        try:
                            owned[child.pid] = child.create_time()
                        except psutil.NoSuchProcess:
                            pass
                except psutil.NoSuchProcess:
                    pass
                if STOP_SIGNAL:
                    reason = f'signal {STOP_SIGNAL}'
                elif time.monotonic() >= min(started + seconds, deadline):
                    reason = 'timeout'
                elif output_bytes(directory) > OUTPUT_LIMIT:
                    reason = 'output_limit'
                code = process.poll()
                if reason or code is not None:
                    break
                time.sleep(.1)
        except BaseException:
            cleanup(owned, grace=0)
            process.wait(timeout=5)
            raise
        if reason:
            cleanup(owned, grace=0)
        code = process.wait(timeout=5)
    result = {'exitcode': code, 'reason': reason,
              'elapsed_seconds': time.monotonic() - started}
    write(directory / f'{name}_outcome.json', result)
    if reason or code:
        raise RuntimeError(f'{name} failed: {result}; see {name}.log')
    return result


def validate_case(case, analyze_condition=None):
    if analyze_condition is None:
        from analyze_announcement_probe import analyze_condition
    analysis = analyze_condition(case)
    outputs = json.loads((case / 'outputs.json').read_text())
    shutdown = json.loads((case / 'shutdown.json').read_text())
    checks = {
        'baseline_only': analysis['condition'] == 'baseline4',
        'evidence_valid': analysis['valid_evidence'],
        'correct_mean': analysis['control_mean_check']['active_outputs_match'],
        'four_successes': set(outputs) == {'A', 'B', 'C', 'D'} and all(
            node.get('status') == 'success' and node.get('error') is None
            and node.get('failed_size') == 0 and node.get('banned_senders') == []
            and node.get('failed_peer_ids') == [] for node in outputs.values()),
        'clean_exit': shutdown.get('safe') is True and shutdown.get('surviving_pids') == []
            and shutdown.get('forced_cleanup') == [] and shutdown.get('error') is None
            and shutdown.get('worker_exitcode') == 0
            and shutdown.get('controller_audited_after_worker_exit') is True,
    }
    # Cleanup warnings must not be hidden by an otherwise safe final audit.
    checks['no_cleanup_errors'] = not any(
        json.loads(line).get('kind') == 'cleanup_error'
        for path in [*case.glob('events_*.jsonl'), case / 'controller.jsonl']
        if path.exists() for line in path.read_text().splitlines() if line.strip())
    write(case.parent / 'validation.json', checks)
    passed = all(checks.values())
    (case.parent / 'REPORT.md').write_text(
        '# Vera four-peer baseline\n\n' + ('PASS' if passed else 'FAIL') + '\n\n'
        + '\n'.join(f'- {key}: {value}' for key, value in checks.items())
        + '\n\nOne local aggregation, seed 1; no training, attack, or deployment-rate claim.\n')
    return passed


def inventory(directory):
    write(directory / 'artifact_inventory.json', {'files': [
        {'path': str(path.relative_to(directory)), 'bytes': path.stat().st_size,
         'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
        for path in sorted(directory.rglob('*'))
        if path.is_file() and path.name != 'artifact_inventory.json']})


def review_run(source, destination):
    """Verify original evidence, reconstruct only the omitted manifest in a copy.

    This never starts peers, audits current processes or changes the original run.
    Keep the original failed execution status separate from the offline review.
    """
    source, destination = source.resolve(), destination.resolve()
    if destination.exists() or destination.is_relative_to(source):
        raise ValueError('Review output must be a new directory outside the original run')
    if output_bytes(source) > OUTPUT_LIMIT or any(p.is_symlink() for p in source.rglob('*')):
        raise ValueError('Refusing oversized or symlink-containing evidence')
    original_inventory = json.loads((source / 'artifact_inventory.json').read_text())
    verified = set()
    for entry in original_inventory['files']:
        relative = Path(entry['path'])
        path = (source / relative).resolve()
        if relative.is_absolute() or not path.is_relative_to(source) or relative in verified:
            raise ValueError('Unsafe or duplicate inventory path')
        data = path.read_bytes()
        if len(data) != entry['bytes'] or hashlib.sha256(data).hexdigest() != entry['sha256']:
            raise ValueError(f'Original inventory mismatch: {relative}')
        verified.add(relative)
    present = {p.relative_to(source) for p in source.rglob('*') if p.is_file()}
    if present != verified | {Path('artifact_inventory.json')}:
        raise ValueError('Original run has uninventoried or missing files')
    if (source / 'harness_hashes.json').exists():
        raise ValueError('This review only reconstructs the missing harness manifest')
    status = json.loads((source / 'status.json').read_text())
    if status.get('status') != 'failed' or status.get('preflight_only') is not False:
        raise ValueError('Expected a failed native baseline run, not a preflight or accepted run')
    worker = json.loads((source / 'worker_outcome.json').read_text())
    cleanup_info = json.loads((source / 'controller_cleanup.json').read_text())
    if worker.get('exitcode') != 0 or worker.get('reason') is not None:
        raise ValueError('Original native worker did not complete successfully')
    if cleanup_info != {'forced_cleanup': [], 'surviving_pids': []}:
        raise ValueError('Original controller cleanup was not clean')
    hashes = json.loads((source / 'source_hashes.json').read_text())
    reconstructed = {}
    for name, digest in hashes.items():
        relative = Path(name)
        if relative.is_absolute() or '..' in relative.parts:
            raise ValueError('Unsafe recorded source path')
        snapshot = source / 'harness' / relative.name
        if hashlib.sha256(snapshot.read_bytes()).hexdigest() != digest:
            raise ValueError(f'Frozen harness mismatch: {name}')
        if relative.name in reconstructed:
            raise ValueError('Ambiguous frozen harness names')
        reconstructed[relative.name] = digest
    if 'announcement_probe.py' not in reconstructed:
        raise ValueError('Missing recorded announcement harness hash')
    config = json.loads((source / 'baseline4/config.json').read_text())
    if config.get('condition') != 'baseline4' or config.get('seed') != 1:
        raise ValueError('Expected the seed-1 baseline only')
    shutil.copytree(source, destination)
    (destination / 'artifact_inventory.json').rename(destination / 'original_artifact_inventory.json')
    write(destination / 'harness_hashes.json', reconstructed)
    # Preserve review code separately from the original experiment's harness.
    # Execute the saved analyzer so a cached import cannot differ from the record.
    review_sources = destination / 'review_code'
    review_sources.mkdir(exist_ok=False)
    review_code = {}
    for path in (Path(__file__).resolve(), HERE / 'analyze_announcement_probe.py'):
        data = path.read_bytes()
        snapshot = review_sources / path.name
        snapshot.write_bytes(data)
        review_code[path.name] = {'source_path': str(path),
                                 'snapshot': str(snapshot.relative_to(destination)),
                                 'sha256': hashlib.sha256(data).hexdigest()}
    analyzer = runpy.run_path(str(review_sources / 'analyze_announcement_probe.py'))
    passed = validate_case(destination / 'baseline4', analyzer['analyze_condition'])
    write(destination / 'review.json', {
        'status': 'passed' if passed else 'failed', 'source_run': str(source),
        'original_execution_status': status, 'verified_original_files': len(verified),
        'reconstructed_file': 'harness_hashes.json',
        'derivation': 'Recorded source_hashes.json cross-checked with original frozen harness files',
        'analysis_harness': str(Path(__file__).resolve()),
        'analysis_harness_sha256': review_code[Path(__file__).name]['sha256'],
        'review_code': review_code,
        'new_native_execution': False})
    inventory(destination)
    return passed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--package-root', type=Path)
    parser.add_argument('--dependencies', type=Path)
    parser.add_argument('--output-root', type=Path, default=HERE / 'results')
    parser.add_argument('--preflight-only', action='store_true')
    parser.add_argument('--analyze-case', type=Path, help=argparse.SUPPRESS)
    parser.add_argument('--review-run', type=Path, help='Offline review of a saved run; starts no peers')
    parser.add_argument('--review-output', type=Path, help='New directory for derived review evidence')
    args = parser.parse_args()
    if args.review_run or args.review_output:
        if (not args.review_run or not args.review_output or args.analyze_case
                or args.preflight_only or args.package_root or args.dependencies):
            parser.error('Use --review-run and --review-output together, without execution options')
        return 0 if review_run(args.review_run, args.review_output) else 1
    if args.analyze_case:
        return 0 if validate_case(args.analyze_case.resolve()) else 1
    allocation_info = allocation(os.environ)
    if sys.version_info[:2] != (3, 11) or sys.prefix == sys.base_prefix:
        raise RuntimeError('Use the verified Python 3.11 virtual environment')
    if args.package_root is None or args.dependencies is None:
        parser.error('--package-root and --dependencies are required')
    package_root, dependencies = args.package_root.resolve(), args.dependencies.resolve()
    for key in THREAD_VARS:
        os.environ[key] = '1'
    os.environ['PYTHONNOUSERSITE'] = '1'
    os.environ['CUDA_VISIBLE_DEVICES'] = ''
    # The dedicated venv owns dependencies; do not inherit an arbitrary import path.
    os.environ.pop('PYTHONPATH', None)
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    directory = args.output_root.resolve() / ('vera_baseline_' + os.environ['SLURM_JOB_ID'])
    directory.mkdir(parents=True, exist_ok=False)
    started = time.monotonic()
    deadline = started + 240
    owned, status = {}, {'status': 'running', 'condition': 'baseline4', 'seed': 1,
                         'preflight_only': args.preflight_only, 'allocation': allocation_info}
    write(directory / 'status.json', status)
    def stop(signum, _frame):
        global STOP_SIGNAL
        STOP_SIGNAL = signum
    old_handlers = {sig: signal.signal(sig, stop) for sig in (signal.SIGTERM, signal.SIGINT)}
    case = directory / 'baseline4'
    try:
        sources = directory / 'harness'
        sources.mkdir()
        files = [Path(__file__).resolve(), HERE / 'announcement_probe.py',
                 HERE / 'analyze_announcement_probe.py', HERE / 'baseline.slurm']
        hashes = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in files}
        for path in files:
            (sources / path.name).write_bytes(path.read_bytes())
        write(directory / 'source_hashes.json', hashes)
        write(directory / 'harness_hashes.json', {path.name: hashes[str(path.relative_to(ROOT))]
                                                 for path in files})
        write(directory / 'host.json', {'hostname': os.uname().nodename,
              'python': sys.version, 'executable': sys.executable,
              'affinity': sorted(os.sched_getaffinity(0)),
              'thread_limits': {key: os.environ[key] for key in THREAD_VARS}})
        bounded(['git', 'rev-parse', 'HEAD'], directory, 'commit', 5, deadline, owned)
        bounded(['git', 'diff', '--', 'hivemind', 'research/hivemind'],
                directory, 'working_tree', 5, deadline, owned)
        bounded([sys.executable, '-m', 'pip', 'freeze'], directory, 'requirements', 15, deadline, owned)
        probe = [sys.executable, str(HERE / 'announcement_probe.py'),
                 '--package-root', str(package_root), '--dependencies', str(dependencies)]
        bounded(probe + ['--preflight'], directory, 'preflight', 30, deadline, owned)
        if not args.preflight_only:
            bounded(probe + ['--condition', 'baseline4', '--seed', '1', '--case-path', str(case)],
                    directory, 'worker', 120, deadline, owned)
            # Reuse native audit semantics, bounded by a supervised subprocess.
            audit = ('from pathlib import Path; from announcement_probe import audit_shutdown; '
                     'import sys; audit_shutdown(Path(sys.argv[1]), 0, Path(sys.argv[2]))')
            bounded([sys.executable, '-c', 'import sys; sys.path.insert(0, sys.argv.pop(1)); ' + audit,
                     str(HERE), str(case), str(dependencies)], directory, 'audit', 25, deadline, owned)
            bounded([sys.executable, str(Path(__file__).resolve()), '--analyze-case', str(case)],
                    directory, 'analysis', 45, deadline, owned)
        if any(hashlib.sha256((ROOT / name).read_bytes()).hexdigest() != digest
               for name, digest in hashes.items()):
            raise RuntimeError('Harness source changed during the job')
        status['status'] = 'preflight_passed' if args.preflight_only else 'passed'
    except BaseException as exc:
        status.update(status='failed', error=f'{type(exc).__name__}: {exc}')
    finally:
        final_cleanup = cleanup(owned)
        write(directory / 'controller_cleanup.json', final_cleanup)
        if final_cleanup['forced_cleanup'] or final_cleanup['surviving_pids'] or STOP_SIGNAL:
            status.update(status='failed', cleanup_or_signal_failure=True)
        write(directory / 'owned_processes.json', owned)
        if output_bytes(directory) > OUTPUT_LIMIT:
            status.update(status='failed', output_limit_exceeded=True)
        status['elapsed_seconds'] = time.monotonic() - started
        write(directory / 'status.json', status)
        inventory(directory)
        for sig, handler in old_handlers.items():
            signal.signal(sig, handler)
    print(json.dumps({'path': str(directory), **status}, indent=2))
    return 0 if status['status'] in ('passed', 'preflight_passed') else 1


if __name__ == '__main__':
    raise SystemExit(main())
