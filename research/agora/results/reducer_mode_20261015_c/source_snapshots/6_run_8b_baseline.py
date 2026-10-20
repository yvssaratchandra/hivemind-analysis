"""Freeze, run and resume the private Agora 8B baseline.

python research/agora/run_8b_baseline.py --prepare
python research/agora/run_8b_baseline.py --phase smoke --label 20261013_a
python research/agora/run_8b_baseline.py --phase pilot --label 20261013_a --smoke PATH
python research/agora/run_8b_baseline.py --resume PATH --dry-run
python research/agora/run_8b_baseline.py --resume PATH
python research/agora/run_8b_baseline.py --request-stop PATH
"""
import argparse
import base64
from datetime import datetime, timezone
import fcntl
import gzip
import hashlib
import importlib.metadata
import json
import multiprocessing as mp
import os
from pathlib import Path
import random
import re
import shutil
import signal
import subprocess
import sys
import time
import traceback

ROOT = Path(__file__).resolve().parent
FILES = ['run_8b_baseline.py', 'baseline_8b.py', 'analyze_8b_baseline.py',
         'test_8b_analysis.py', 'test_8b_baseline.py', 'prepare_8b_runtime.py']
NAMES = [f'R{i}' for i in range(4)] + [f'C{i}' for i in range(4)]
STOP = False


def digest(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for chunk in iter(lambda: f.read(1024*1024), b''):
            h.update(chunk)
    return h.hexdigest()


def atomic(path, value):
    path = Path(path)
    temporary = path.with_suffix(path.suffix+'.tmp')
    temporary.write_text(json.dumps(value, indent=2, allow_nan=False)+'\n')
    temporary.replace(path)


def read(path):
    return json.loads(Path(path).read_text())


def event(path, kind, **fields):
    record = dict(kind=kind, monotonic=time.monotonic(), wall=time.time(), **fields)
    with Path(path).open('a') as f:
        f.write(json.dumps(record, allow_nan=False)+'\n')


def make_profiles(data_path, seed, phase):
    data = read(data_path)
    blob = data['embedded_evidence']
    decoded = gzip.decompress(base64.b64decode(blob['data_base64'], validate=True))
    if hashlib.sha256(decoded).hexdigest() != blob['decoded_sha256']:
        raise ValueError('8B embedded input checksum differs')
    evidence = json.loads(decoded)
    w = data['run_identity_and_windows']['windows_for_export_summaries']['paper_calendar_dates_utc_proxy']
    rows = evidence['peer_table_selected_fields']
    rng = random.Random(seed)
    sampled = []
    for is_pluralis in [True, False]:
        pool = [r for r in rows if r['is_pluralis']==is_pluralis and
                isinstance(r['latency'], (int, float)) and 0 < r['latency'] <= 80 and
                w['start_epoch'] <= r['joined_at'] < w['end_epoch']]
        sampled.extend(rng.sample(pool, 4))
    return {name: dict(role='reducer' if index < 4 else 'contributor',
                      rtt_ms=row['latency'], source_record=row['name'],
                      source_joined_at=row['joined_at'], measured_at_unknown=True)
            for index, (name, row) in enumerate(zip(NAMES, sampled))}


def tree_hashes(root):
    return {str(p.relative_to(root)): digest(p) for p in sorted(Path(root).rglob('*'))
            if p.is_file() and '__pycache__' not in p.parts and not p.name.endswith('.pyc')}


def freeze(suite, phase, label, smoke):
    import hivemind
    import agora_server
    runtime = ROOT/'.runtime'
    marker = read(runtime/'prepared.json')
    if marker['agora_commit'] != '37d29a966528675635161007d7b43388012a98b2':
        raise ValueError('Wrong Agora revision')
    if marker['hivemind_commit'] != '4d5c41495be082490ea44cce4e9dd58f9926bb4e':
        raise ValueError('Wrong Hivemind revision')
    if digest(runtime/'requirements.lock') != marker['lock_sha256']:
        raise ValueError('Prepared dependency lock changed')
    if phase=='pilot':
        if smoke is None:
            raise ValueError('Pilot requires --smoke pointing to a verified frozen smoke suite')
        validate(smoke)
        smoke_state = read(Path(smoke)/'checkpoint.json')
        if not smoke_state['jobs'] or any(j['status']!='completed' or not j.get('pass') for j in smoke_state['jobs']):
            raise ValueError('Smoke has not passed')
        for name in FILES:
            if digest(ROOT/name) != digest(Path(smoke)/'harness'/name):
                raise ValueError(f'Harness changed since smoke: {name}')
        if digest(ROOT/'RUN_8B_DATA_20261013.json') != digest(Path(smoke)/'data.json'):
            raise ValueError('Input data changed since smoke')
        for module in [hivemind, agora_server]:
            if tree_hashes(Path(module.__file__).parent) != tree_hashes(Path(smoke)/'native'/module.__name__):
                raise ValueError(f'Native package changed since smoke: {module.__name__}')
        if digest(runtime/'requirements.lock') != digest(Path(smoke)/'requirements.lock'):
            raise ValueError('Dependencies changed since smoke')
    suite.mkdir(parents=True, exist_ok=False)
    (suite/'harness').mkdir()
    (suite/'native').mkdir()
    for name in FILES:
        shutil.copy2(ROOT/name, suite/'harness'/name)
    shutil.copy2(ROOT/'RUN_8B_DATA_20261013.json', suite/'data.json')
    shutil.copy2(runtime/'requirements.lock', suite/'requirements.lock')
    for module in [hivemind, agora_server]:
        shutil.copytree(Path(module.__file__).parent, suite/'native'/module.__name__,
                        ignore=shutil.ignore_patterns('__pycache__', '*.pyc'))
    provenance = dict(label=label, phase=phase, created_utc=datetime.now(timezone.utc).isoformat(),
                      runtime=marker, python=str(runtime/'venv/bin/python'),
                      packages={d.metadata['Name']:d.version for d in importlib.metadata.distributions()},
                      frozen_sha256=tree_hashes(suite), private_loopback=True,
                      smoke_gate=str(Path(smoke).resolve()) if smoke else None)
    atomic(suite/'provenance.json', provenance)
    seeds = [8100] if phase=='smoke' else [8101,8102]
    atomic(suite/'checkpoint.json', dict(phase=phase, jobs=[dict(seed=seed, status='pending', attempts=[]) for seed in seeds]))


def validate(suite):
    suite = Path(suite).resolve()
    provenance = read(suite/'provenance.json')
    failures = [str(p) for p, h in provenance['frozen_sha256'].items()
                if not (suite/p).is_file() or digest(suite/p)!=h]
    if failures:
        raise ValueError('Frozen files changed: '+', '.join(failures[:10]))
    for job in read(suite/'checkpoint.json')['jobs']:
        if job['status']=='completed':
            for name,h in job['result_sha256'].items():
                if digest(suite/job['attempt']/name)!=h:
                    raise ValueError('Completed result changed: '+name)
    return provenance


def trial_config(suite, seed, phase):
    profiles = make_profiles(suite/'data.json', seed, phase)
    suffix = hashlib.sha256(f'{suite.name}:{seed}'.encode()).hexdigest()[:12]
    stage = 'a8b'+suffix
    rng = random.Random(seed+4096)
    return dict(seed=seed, phase=phase, rounds=1 if phase=='smoke' else 5,
                node_names=NAMES, profiles=profiles, stage_base=stage, prefix=stage+'_avg',
                network_delay_enabled=phase!='smoke', network_formula='remote message leg=(RTT_i+RTT_j)/2',
                initial_offsets={name:rng.uniform(0,1) for name in NAMES},
                tensor_values=100, averaged_values=5, batch_size=32, learning_rate=.01,
                adam_betas=[.9,.95], weight_decay=0, training_step_seconds=2,
                matchmaking_window=40, averaging_timeout=150, sender_timeout=30,
                reducer_timeout=60, poll_sleep=1, averaging_every_steps=20,
                stage_announcement_seconds=30, stage_lifetime_seconds=60, num_replicas=5,
                synthetic_data=True, departures=False, attacks=False, independent_rosters=True,
                numerical_tolerance=1e-5, hard_limit_seconds=600,
                worker_start_method='spawn', native_dht_start_method='fork',
                payload_layout='five scalar tensors to exercise four reducers',
                full_pipeline_or_bandwidth_simulation=False)


def stop_process_tree(process):
    import psutil
    try:
        descendants = psutil.Process(process.pid).children(recursive=True)
    except psutil.NoSuchProcess:
        descendants = []
    if process.is_alive():
        process.terminate()
    for child in descendants:
        try:
            child.terminate()
        except psutil.NoSuchProcess:
            pass
    process.join(3)
    _, alive = psutil.wait_procs(descendants, timeout=3)
    for child in alive:
        try:
            child.kill()
        except psutil.NoSuchProcess:
            pass
    if process.is_alive():
        process.kill()
        process.join(3)


def run_trial(config, path):
    import psutil
    from baseline_8b import worker_entry, clock_sample, clock_discontinuous
    ctx = mp.get_context('spawn')
    processes, pipes, identities, finished = [], [], {}, set()
    log = path/'controller.jsonl'
    error = None
    inventory = set()
    began = time.monotonic()
    atomic(path/'config.json', config)

    def poll(until, expected, deadline, expected_kind):
        nonlocal error
        while len(until) < expected:
            if time.monotonic() > deadline:
                raise TimeoutError('Trial startup/finish deadline exceeded')
            for i, conn in enumerate(pipes):
                while conn.poll():
                    message = conn.recv()
                    event(log, 'worker_message', message=message)
                    if message['kind']=='error':
                        raise RuntimeError(f"{message['name']}: {message['error']}")
                    if message['kind']==expected_kind:
                        until[message['name']] = message
                if not processes[i].is_alive():
                    raise RuntimeError(f'Worker {NAMES[i]} exited before shutdown')
            time.sleep(.02)

    try:
        boot = {}
        for index in range(8):
            parent, child = ctx.Pipe()
            seeds = [] if index==0 else boot['R0']['maddrs']
            process = ctx.Process(target=worker_entry, args=(config,index,str(path),child,seeds),
                                  name='agora8b-'+NAMES[index])
            process.start()
            inventory.add(process.pid)
            child.close()
            processes.append(process)
            pipes.append(parent)
            # Boot R0 supplies the only private bootstrap address. No ninth node.
            if index==0:
                poll(boot, 1, began+90, 'boot')
        poll(boot, 8, began+150, 'boot')
        mapping = {entry['peer_id']:dict(name=name, **config['profiles'][name]) for name,entry in boot.items()}
        config['peer_ids'] = {name:entry['peer_id'] for name,entry in boot.items()}
        for conn in pipes:
            conn.send(dict(mapping=mapping))
        ready = {}
        poll(ready, 8, began+240, 'ready')
        for process in processes:
            inventory.add(process.pid)
            inventory.update(p.pid for p in psutil.Process(process.pid).children(recursive=True))
        config['process_inventory'] = sorted(inventory)
        start = time.monotonic()+.5
        config['measurement_start_monotonic'] = start
        atomic(path/'config.json', config)
        for conn in pipes:
            conn.send(dict(start_monotonic=start))
        event(log, 'trial_started', seed=config['seed'], phase=config['phase'], process_inventory=sorted(inventory))
        last_clock, last_live = clock_sample(), 0
        while len(finished) < 8:
            if time.monotonic()-start > config['hard_limit_seconds']:
                raise TimeoutError('Trial exceeded ten-minute measurement cap')
            current = clock_sample()
            if clock_discontinuous(last_clock, current):
                event(log, 'clock_discontinuity', before=last_clock, after=current)
                raise RuntimeError('Suspend or clock discontinuity; trial must be rerun')
            last_clock = current
            for i, conn in enumerate(pipes):
                while conn.poll():
                    message = conn.recv()
                    event(log, 'worker_message', message=message)
                    if message['kind']=='error':
                        raise RuntimeError(f"{message['name']}: {message['error']}")
                    if message['kind']=='finished':
                        finished.add(message['name'])
                if not processes[i].is_alive():
                    raise RuntimeError(f'Worker {NAMES[i]} exited unexpectedly')
            if time.monotonic()-last_live > 10:
                last_live = time.monotonic()
                results = sum(f.read_text().count('"kind":"round_result"') for f in path.glob('events_*.jsonl'))
                print(f"LIVE seed={config['seed']} elapsed={last_live-start:.1f}s peer-round-results={results}/{8*config['rounds']} workers-finished={len(finished)}/8", flush=True)
                event(log, 'clock_sample', boottime=current['boottime'])
            time.sleep(.05)
    except BaseException as exc:
        error = repr(exc)
        event(log, 'trial_error', error=error, traceback=traceback.format_exc())
    finally:
        for process in processes:
            try:
                inventory.update(p.pid for p in psutil.Process(process.pid).children(recursive=True))
            except psutil.NoSuchProcess:
                pass
        for conn in pipes:
            try:
                conn.send(dict(stop=True))
            except (BrokenPipeError,OSError):
                pass
        cleanup_end = time.monotonic()+25
        for process in processes:
            process.join(max(0,cleanup_end-time.monotonic()))
            if process.is_alive():
                stop_process_tree(process)
        # Audit the observed inventory, ignoring resource tracker helpers only
        # when their command line is explicitly Python's resource tracker.
        survivors = []
        for pid in inventory:
            try:
                proc = psutil.Process(pid)
                if proc.status()!=psutil.STATUS_ZOMBIE:
                    survivors.append(dict(pid=pid,cmdline=proc.cmdline()))
            except psutil.NoSuchProcess:
                pass
        atomic(path/'shutdown.json', dict(safe=not survivors, survivors=survivors,
                                         worker_exitcodes={NAMES[i]:p.exitcode for i,p in enumerate(processes)},
                                         error=error, elapsed_seconds=time.monotonic()-began))
    from analyze_8b_baseline import analyze_trial
    analysis = analyze_trial(path)
    if error or survivors:
        analysis['pass'] = analysis['valid'] = False
        analysis['errors'].append(error or 'Peer processes survived cleanup')
    atomic(path/'analysis.json', analysis)
    return analysis, error


def report(suite, state):
    rows = []
    for job in state['jobs']:
        if job.get('attempt'):
            a = read(suite/job['attempt']/'analysis.json')
            m = a.get('metrics', {})
            rows.append(f"| {job['seed']} | {job['status']} | {a['pass']} | {m.get('agreed_rounds','—')} | {m.get('mean_initial_loss','—')} | {m.get('mean_final_loss','—')} |")
    text = ('# Agora 8B private baseline\n\n'
            'Eight peers; one stage; native DHT, matchmaking and tensor averaging. '
            'Synthetic two-second training steps and an assumed RTT pairing model. '
            'This is a local delay emulation, not a replay of the public run.\n\n'
            '| Seed | Status | Pass | Agreed rounds | Initial mean loss | Final mean loss |\n'
            '|---|---|---|---|---|---|\n'+'\n'.join(rows)+'\n\n'
            f"Resume: `{read(suite/'provenance.json')['python']} {suite/'harness/run_8b_baseline.py'} --resume {suite}`\n")
    (suite/'REPORT.md').write_text(text)


def resume(suite, dry_run=False):
    suite = Path(suite).resolve()
    provenance = validate(suite)
    if dry_run:
        state = read(suite/'checkpoint.json')
        print(json.dumps(dict(valid=True, jobs=[dict(seed=j['seed'],status=j['status']) for j in state['jobs']]),indent=2))
        return
    # Execute frozen code with frozen native packages, never the live checkout.
    if Path(__file__).resolve() != suite/'harness/run_8b_baseline.py':
        env = dict(os.environ, PYTHONPATH=str(suite/'native'), PYTHONDONTWRITEBYTECODE='1')
        os.execve(provenance['python'], [provenance['python'], str(suite/'harness/run_8b_baseline.py'), '--resume', str(suite)], env)
    current_packages = {d.metadata['Name']:d.version for d in importlib.metadata.distributions()}
    if current_packages != provenance['packages']:
        raise ValueError('Installed dependency versions changed since freezing')
    with (suite/'controller.lock').open('w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        state = read(suite/'checkpoint.json')
        stopfile = suite/'STOP_AFTER_TRIAL'
        # This invocation is an explicit resume. Consume the previous pause request;
        # a new request arriving during this controller run remains effective.
        if stopfile.exists():
            stopfile.unlink()
            print('Previous pause request cleared by explicit resume.', flush=True)
    
        def request_stop(signum, frame):
            global STOP
            STOP = True
            print('Stop requested: finishing current trial and shutting down its peers.',flush=True)
        signal.signal(signal.SIGINT,request_stop)
        signal.signal(signal.SIGTERM,request_stop)
        for job in state['jobs']:
            if job['status']=='completed':
                print(f"SKIP completed seed={job['seed']}",flush=True)
                continue
            if STOP or stopfile.exists():
                break
            attempt = f"seed{job['seed']}_attempt{len(job['attempts'])+1:02d}"
            path = suite/attempt
            path.mkdir()
            job['status'],job['attempt'] = 'running',attempt
            job['attempts'].append(attempt)
            atomic(suite/'checkpoint.json',state)
            config = trial_config(suite,job['seed'],state['phase'])
            print(f"START {state['phase']} seed={job['seed']} attempt={attempt}",flush=True)
            analysis,error = run_trial(config,path)
            job['status'] = 'interrupted' if error else 'completed'
            job['pass'] = analysis['pass']
            job['result_sha256'] = tree_hashes(path)
            atomic(suite/'checkpoint.json',state)
            report(suite,state)
            print(f"DONE seed={job['seed']} status={job['status']} pass={job['pass']}",flush=True)
            if error:
                break
        validate(suite)
        print('Controller stopped; trial shutdown audit is saved.',flush=True)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--prepare',action='store_true')
    parser.add_argument('--phase',choices=['smoke','pilot'])
    parser.add_argument('--label')
    parser.add_argument('--smoke',type=Path)
    parser.add_argument('--resume',type=Path)
    parser.add_argument('--dry-run',action='store_true')
    parser.add_argument('--request-stop',type=Path)
    args=parser.parse_args()
    if args.prepare:
        from prepare_8b_runtime import prepare
        prepare()
        return
    if args.request_stop:
        validate(args.request_stop)
        (args.request_stop/'STOP_AFTER_TRIAL').write_text(datetime.now(timezone.utc).isoformat()+'\n')
        print('Stop after current trial requested.')
        return
    if args.resume:
        resume(args.resume,args.dry_run)
        return
    if args.dry_run:
        parser.error('--dry-run requires --resume')
    if not args.phase or not args.label or not re.fullmatch(r'[A-Za-z0-9_-]+',args.label):
        parser.error('Use --phase and a unique alphanumeric --label')
    runtime=ROOT/'.runtime/venv/bin/python'
    if not runtime.exists() or not (ROOT/'.runtime/prepared.json').exists():
        parser.error('Prepare the isolated runtime first: --prepare')
    if os.environ.get('AGORA8B_RUNTIME')!='1':
        env=dict(os.environ,AGORA8B_RUNTIME='1',PYTHONDONTWRITEBYTECODE='1')
        os.execve(str(runtime),[str(runtime),str(__file__),*sys.argv[1:]],env)
    suite=ROOT/'results'/f'baseline8b_{args.phase}_{args.label}'
    freeze(suite,args.phase,args.label,args.smoke)
    print('FROZEN',suite,flush=True)
    resume(suite)


if __name__=='__main__':
    main()
