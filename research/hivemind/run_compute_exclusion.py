#!/usr/bin/env python3
"""Private, persistent native optimizer experiment for dominant-contributor exclusion."""
from __future__ import annotations

import argparse
import dataclasses
import math
import hashlib
import json
import os
from pathlib import Path
import queue
import shutil
import signal
import subprocess
import sys
import time
import traceback
import uuid

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(HERE))
from announcement_artifacts import inventory, verify
from compute_workload import NODES, CONDITIONS

PACKAGE = ROOT / 'research/simulator/.native-runtime/root_snapshot_a'
DEPENDENCIES = ROOT / 'research/agora/.runtime/venv/lib/python3.11/site-packages'
SOURCES = ('run_compute_exclusion.py', 'compute_native_hooks.py', 'compute_workload.py',
           'analyze_compute_exclusion.py', 'announcement_artifacts.py', 'announcement_probe.py')
OPTIM_FILES = ('optim/optimizer.py', 'optim/grad_averager.py', 'optim/state_averager.py',
               'optim/progress_tracker.py', 'utils/performance_ema.py')


def plain(value):
    """Serialize native progress records without losing nonfinite ETA information."""
    if dataclasses.is_dataclass(value):
        return plain(dataclasses.asdict(value))
    if hasattr(value, "dict"):
        return plain(value.dict())
    if isinstance(value, dict):
        return {str(k): plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [plain(v) for v in value]
    if isinstance(value, bytes):
        return {"bytes_hex": value.hex()}
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    return value


def write(path, value):
    path = Path(path)
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(plain(value), indent=2, allow_nan=False) + '\n')
    temporary.replace(path)


def label_path(label):
    if not label or Path(label).name != label or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-' for c in label):
        raise ValueError('Label must be a new simple directory name')
    return HERE / 'results' / label


def native_imports():
    from announcement_probe import native_imports as prepare
    package, provenance = prepare(argparse.Namespace(package_root=PACKAGE, dependencies=DEPENDENCIES))
    for relative in OPTIM_FILES:
        expected, actual = ROOT / 'hivemind' / relative, package / relative
        if expected.read_bytes() != actual.read_bytes():
            raise RuntimeError(f'Native optimizer snapshot mismatch: {relative}')
        provenance['hashes'][relative] = hashlib.sha256(actual.read_bytes()).hexdigest()
    provenance.pop('allocations', None)  # inherited probe used a different tensor shape
    provenance['experiment_vector_elements'] = 48
    return provenance


def freeze(destination):
    (destination / 'harness').mkdir()
    hashes = {}
    for name in SOURCES:
        source = HERE / name
        shutil.copyfile(source, destination / 'harness' / name)
        hashes[name] = hashlib.sha256(source.read_bytes()).hexdigest()
    write(destination / 'source_hashes.json', hashes)


def check_sources(destination):
    hashes = json.loads((destination / 'source_hashes.json').read_text())
    changed = [name for name, digest in hashes.items()
               if hashlib.sha256((HERE / name).read_bytes()).hexdigest() != digest]
    if changed:
        raise ValueError(f'Harness changed during experiment: {changed}')


def check_reference(reference, mode):
    if verify(reference):
        raise ValueError('Reference inventory mismatch')
    previous = json.loads((reference / 'status.json').read_text())
    expected = 'markers' if mode == 'pilot' else 'pilot'
    if previous.get('mode') != expected or previous.get('status') != 'complete':
        raise ValueError(f'Requires complete {expected} reference')
    seeds = (11, 12, 13) if expected == 'markers' else (301,)
    required = {f'{condition}_s{seed}' for seed in seeds for condition in CONDITIONS}
    cases = previous.get('cases', [])
    if len(cases) != len(required) or {case['path'] for case in cases} != required:
        raise ValueError('Reference case matrix incomplete')
    check_sources(reference)
    from analyze_compute_exclusion import analyze_case
    for case in cases:
        result = analyze_case(reference / case['path'])
        if not result['valid']:
            raise ValueError(f'Reference evidence invalid: {case["path"]}')
        if mode == 'confirmation' and case['path'].startswith('baseline_') and (result['learning']['censored']
                or not json.loads((reference/case['path']/'outcome.json').read_text())['completed']):
            raise ValueError('Pilot baseline did not pass the learning gate')
    return hashlib.sha256((reference / 'artifact_inventory.json').read_bytes()).hexdigest()


def marker_samples(seed, node, batch_index):
    """Exactly representable analytic per-example gradients, with origin lanes."""
    import numpy as np
    data = np.zeros((32, 48), dtype=np.float32)
    i = NODES.index(node)
    data[:, i] = 1
    data[:, 4 + i] = batch_index + 1
    data[:, 8:] = (np.arange(40, dtype=np.float32) + seed + 10 * i + batch_index) / 32
    return data


def event_writer(path, node, origin):
    def emit(kind, **fields):
        if 'node' in fields and fields['node'] != node:
            raise ValueError('Event node disagrees with worker')
        record = dict(fields, kind=kind, node=node, pid=os.getpid(),
                      monotonic=time.monotonic(),
                      time=time.monotonic() - origin.value if origin.value else None)
        # Each process has its own append-only stream, including native child processes.
        with (path / f'events_{node}_{os.getpid()}.jsonl').open('a') as stream:
            stream.write(json.dumps(plain(record), allow_nan=False) + '\n')
    return emit


def evaluator(path, seed, arrays, origin, duration, stop):
    from compute_workload import evaluation, evaluate_models
    import numpy as np
    x, y = evaluation(seed)
    np.savez_compressed(path / 'evaluation_data.npz', x=x, y=y)
    initial = evaluate_models({n: np.zeros(48, dtype=np.float32) for n in arrays}, x, y)
    thresholds = {n: .1 * initial['per_node'][n] for n in ('D', 'C')}
    with (path / 'evaluations.jsonl').open('w') as stream:
        for tick in range(int(duration) + 1):
            until = origin.value + tick
            while time.monotonic() < until and not stop.is_set():
                stop.wait(min(.05, max(0, until - time.monotonic())))
            if stop.is_set():
                break
            captured = time.monotonic()
            models, timestamps = {}, {}
            for node, values in arrays.items():
                with values.get_lock():
                    snapshot = list(values)
                if snapshot[49]:
                    models[node] = np.asarray(snapshot[:48], dtype=np.float32)
                    timestamps[node] = snapshot[48] - origin.value
            result = evaluate_models(models, x, y)
            row = dict(time=captured-origin.value, scheduled_time=tick,
                       capture_lag=max(0, captured-until), model_publication_times=timestamps,
                       weights={n: v.tolist() for n,v in models.items()}, metrics=result,
                       thresholds=thresholds, evaluation_seconds=time.monotonic()-captured)
            stream.write(json.dumps(row, allow_nan=False) + '\n'); stream.flush()


def peer_worker(path, config, node, bootstrap, identities, identities_ready,
                release, origin, stop, messages, snapshots, marker_anchor):
    import torch
    from functools import partial
    from hivemind import DHT, Optimizer
    from hivemind.utils import get_dht_time
    from hivemind.optim.grad_averager import GradientAverager
    from hivemind.utils.crypto import RSAPrivateKey
    from compute_workload import batch, next_batch_delay, SHARE70_SAMPLES_PER_SECOND
    from compute_native_hooks import install_hooks
    emit = event_writer(path, node, origin)
    torch.set_num_threads(1)
    dht = optimizer = averager = None
    model = torch.nn.Parameter(torch.zeros(48, dtype=torch.float32))

    def publish():
        if snapshots is not None:
            vector = model.detach().tolist()
            with snapshots[node].get_lock():
                snapshots[node][:] = vector + [time.monotonic(), 1.]
            return vector

    try:
        dht = DHT(initial_peers=bootstrap, host_maddrs=['/ip4/127.0.0.1/tcp/0'],
                  auto_nat=False, nat_port_map=False, use_relay=False, start=True)
        addresses = [str(a) for a in dht.get_visible_maddrs()]
        if not addresses or any(not a.startswith('/ip4/127.0.0.1/tcp/') for a in addresses):
            raise RuntimeError('Non-loopback DHT listener')
        identities[str(dht.peer_id)] = node
        emit('dht_started', peer_id=str(dht.peer_id), addresses=addresses)
        if not identities_ready.wait(45):
            raise TimeoutError('Peer identity barrier')
        prefix = config['run_id'] + '_grad_averager'
        install_hooks(node=node, condition=config['condition'], emit=emit,
                      gradient_prefix=prefix, identities=identities, snapshot=publish)
        active = config['active']
        common = dict(target_group_size=len(active), min_group_size=3,
                      initial_group_bits='', part_size_bytes=16,
                      sender_timeout=2, reducer_timeout=5, request_timeout=.5)
        if config['mode'] == 'markers':
            averager = GradientAverager([model], dht=dht, prefix=prefix,
                        min_matchmaking_time=1, allreduce_timeout=5, next_chunk_timeout=2,
                        shutdown_timeout=3, start=True, **common)
            count = 7 if node == 'A' else 1
            gradients = []
            for index in range(count):
                values = torch.from_numpy(marker_samples(config['seed'], node, index))
                model.grad = None
                (values @ model).mean().backward()
                gradients.append(model.grad.tolist())
                averager.accumulate_grads_(32)
                emit('marker_batch', batch_index=index, sample_count=32,
                     sample_ids=[f'{config["seed"]}:{node}:{index}:{i}' for i in range(32)],
                     gradient=model.grad.tolist())
            averager.load_accumulators_into_averager_()
            with averager.get_tensors() as tensors:
                initial = tensors[0].tolist()
            emit('marker_accumulated', initial=initial, weight=averager.local_samples_accumulated,
                 batches=averager.local_times_accumulated)
            messages.put(('ready', node))
            if not release.wait(45):
                raise TimeoutError('Marker release barrier')
            result = averager.step(wait=True, timeout=15, scheduled_time=marker_anchor.value,
                                   allow_retries=False)
            with averager.get_tensors() as tensors:
                final = tensors[0].tolist()
            write(path / f'output_{node}.json', dict(initial=initial, final=final,
                  weight=32*count, batches=count, status='success', gathered=str(result)))
            emit('marker_terminal', initial=initial, final=final, weight=32*count, status='success')
        else:
            class ObservedOptimizer(Optimizer):
                def _check_and_accumulate_gradients(self, size, scaler):
                    accepted = super()._check_and_accumulate_gradients(size, scaler)
                    emit('accumulation', accepted=accepted, sample_count=size,
                         batch_index=getattr(self, 'experiment_batch_index', None),
                         epoch=self.local_epoch, local_samples=self.grad_averager.local_samples_accumulated,
                         local_batches=self.grad_averager.local_times_accumulated)
                    return accepted

                def _update_global_epoch(self, scaler):
                    emit('update_begin', epoch=self.local_epoch,
                         batch_index=getattr(self, 'experiment_batch_index', None),
                         local_samples=self.grad_averager.local_samples_accumulated,
                         local_batches=self.grad_averager.local_times_accumulated,
                         global_progress=self.tracker.global_progress,
                         parameters=model.detach().tolist())
                    result = super()._update_global_epoch(scaler)
                    publish()
                    emit('update_end', epoch=self.local_epoch,
                         batch_index=getattr(self, 'experiment_batch_index', None),
                         parameters=model.detach().tolist())
                    return result

                def _load_averaged_gradients_into_optimizer_(self):
                    result = super()._load_averaged_gradients_into_optimizer_()
                    emit('gradient_loaded', epoch=self.local_epoch, gradient=model.grad.tolist())
                    return result

            optimizer = ObservedOptimizer(dht=dht, run_id=config['run_id'], params=[model],
                optimizer=partial(torch.optim.SGD, lr=.1), target_batch_size=4096,
                batch_size_per_step=32, matchmaking_time=1, averaging_timeout=15,
                allreduce_timeout=5, next_chunk_timeout=2, load_state_timeout=15,
                offload_optimizer=False, use_local_updates=False,
                delay_optimizer_step=False, delay_grad_averaging=False,
                delay_state_averaging=True, average_state_every=1,
                grad_averager_factory=partial(GradientAverager, **common),
                averager_opts=common, tracker_opts=dict(private_key=RSAPrivateKey()),
                shutdown_timeout=3, verbose=False)
            # Native progress is only advertised after real work begins. The common
            # barrier covers DHT identities and initialized optimizers; progress
            # discovery remains native and is included in measured cold-start time.
            emit('optimizer_ready', local_progress=optimizer.local_progress,
                 global_progress=optimizer.tracker.global_progress)
            publish(); messages.put(('ready', node))
            if not release.wait(60):
                raise TimeoutError('Training release barrier')
            index = 0
            while not stop.is_set() and time.monotonic() < origin.value + config['duration']:
                began = time.monotonic()
                x, y, metadata = batch(config['seed'], node, index)
                optimizer.zero_grad()
                before = model.detach().tolist()
                prediction = torch.from_numpy(x) @ model
                loss = .5 * ((prediction - torch.from_numpy(y)) ** 2).mean()
                loss.backward()
                computed = time.monotonic()
                delay = next_batch_delay(began, computed, 32/SHARE70_SAMPLES_PER_SECOND[node])
                if stop.wait(delay):
                    break
                if time.monotonic() >= origin.value + config['duration']:
                    break
                emit('microbatch', **metadata, parameters=before, gradient=model.grad.tolist(),
                     compute_seconds=computed-began, pacing_seconds=delay)
                step_started = time.monotonic()
                optimizer.experiment_batch_index = index
                optimizer.step()
                publish()
                emit('step_complete', batch_index=index, epoch=optimizer.local_epoch,
                     seconds=time.monotonic()-step_started, parameters=model.detach().tolist(),
                     local_progress=optimizer.local_progress,
                     global_progress=optimizer.tracker.global_progress)
                index += 1
            emit('training_terminal', batches=index, epoch=optimizer.local_epoch,
                 parameters=model.detach().tolist())
        messages.put(('done', node))
    except BaseException as exc:
        emit('worker_error', error=repr(exc), traceback=traceback.format_exc())
        messages.put(('error', node, repr(exc)))
    finally:
        for component in (optimizer, averager, dht):
            if component is not None:
                try:
                    component.shutdown()
                except BaseException as exc:
                    emit('cleanup_error', error=repr(exc))


def join_completed_workers(children, outcome):
    """Separate completed training from a native worker's shutdown liveness.

    A worker sends ``done`` before calling native shutdown, which can wait for
    peers that have already left. The caller still audits/cleans every owned
    process. Evaluator failure and actual child crashes remain harness errors.
    """
    pending, skipped = [], []
    for process in children:
        if outcome['status'] == 'native_noncompletion' and process.name != 'compute_evaluator':
            skipped.append(process.name.removeprefix('compute_'))
            continue
        process.join(timeout=10)
        if process.exitcode is None and process.name != 'compute_evaluator':
            pending.append(process.name.removeprefix('compute_'))
        elif process.exitcode != 0:
            raise RuntimeError(f'Child {process.name} exitcode={process.exitcode}')
    outcome['cleanup_status'] = ('training_incomplete' if skipped else
                                 'deadline_exceeded' if pending else 'joined')
    outcome['cleanup_pending_nodes'] = pending
    outcome['cleanup_skipped_nodes'] = skipped


def trial_worker(path):
    import multiprocessing as mp
    path = Path(path).resolve()
    config = json.loads((path / 'config.json').read_text())
    provenance = native_imports()
    import torch, psutil
    from hivemind import DHT
    torch.set_num_threads(1)
    write(path / 'provenance.json', provenance)
    ctx = mp.get_context('fork')
    owned, children, forced_cleanup = {}, [], []
    origin = ctx.Value('d', 0.)
    marker_anchor = ctx.Value('d', 0.)
    stop, release, identities_ready = ctx.Event(), ctx.Event(), ctx.Event()
    messages = ctx.Queue()
    manager = ctx.Manager()
    identities = manager.dict()
    snapshots = {n: ctx.Array('d', 50, lock=True) for n in config['active']}
    bootstrap = None
    error = None
    outcome = dict(status='initializing', completed=False)
    def sample_owned():
        for proc in psutil.Process().children(recursive=True):
            try:
                owned[proc.pid] = proc.create_time()
            except psutil.NoSuchProcess:
                pass
    try:
        bootstrap = DHT(host_maddrs=['/ip4/127.0.0.1/tcp/0'], auto_nat=False,
                        nat_port_map=False, use_relay=False, start=True)
        addresses = [str(a) for a in bootstrap.get_visible_maddrs()]
        for node in config['active']:
            process = ctx.Process(target=peer_worker, args=(path, config, node, addresses, identities,
                identities_ready, release, origin, stop, messages, snapshots, marker_anchor), name=f'compute_{node}')
            process.start(); children.append(process)
        ready, done = set(), set()
        startup_deadline = time.monotonic()+100
        while len(ready) < len(config['active']):
            sample_owned()
            failed = [(p.name, p.exitcode) for p in children if p.exitcode is not None]
            if failed:
                raise RuntimeError(f'Worker exited during initialization: {failed}')
            if len(identities) == len(config['active']):
                identities_ready.set()
            if time.monotonic() > startup_deadline:
                raise TimeoutError(f'Startup barrier: ready={sorted(ready)}')
            try:
                message = messages.get(timeout=.1)
            except queue.Empty:
                continue
            if message[0] == 'error':
                raise RuntimeError(str(message))
            if message[0] == 'ready':
                ready.add(message[1])
        write(path / 'identities.json', dict(identities))
        from hivemind.utils import get_dht_time
        origin.value = time.monotonic()
        marker_anchor.value = get_dht_time() + 3
        write(path / 'release.json', dict(monotonic=origin.value, ready=sorted(ready),
                                          marker_anchor=marker_anchor.value))
        if config['mode'] != 'markers':
            process = ctx.Process(target=evaluator, args=(path, config['seed'], snapshots,
                                  origin, config['duration'], stop), name='compute_evaluator')
            process.start(); children.append(process)
        release.set()
        deadline = origin.value + (30 if config['mode'] == 'markers' else config['duration'] + 35)
        while len(done) < len(config['active']):
            sample_owned()
            if time.monotonic() > deadline:
                if config['mode'] == 'markers':
                    raise TimeoutError(f'Marker completion: done={sorted(done)}')
                outcome = dict(status='native_noncompletion', completed=False,
                               stuck_nodes=sorted(set(config['active'])-done),
                               observed_seconds=time.monotonic()-origin.value,
                               measured_horizon=config['duration'])
                break
            try:
                message = messages.get(timeout=.1)
            except queue.Empty:
                continue
            if message[0] == 'error':
                raise RuntimeError(str(message))
            if message[0] == 'done':
                done.add(message[1])
        if len(done) == len(config['active']):
            outcome = dict(status='completed', completed=True,
                           observed_seconds=time.monotonic()-origin.value)
        join_completed_workers(children, outcome)
    except BaseException as exc:
        error = repr(exc)
        outcome = dict(status='harness_failure', completed=False, error=error)
        write(path / 'failure.json', dict(error=error, traceback=traceback.format_exc()))
    finally:
        sample_owned(); stop.set(); release.set(); identities_ready.set()
        for process in children:
            process.join(timeout=3)
            if process.is_alive():
                forced_cleanup.append(dict(pid=process.pid, action="terminate"))
                process.terminate(); process.join(timeout=2)
            if process.is_alive():
                process.kill(); process.join(timeout=2)
        for component in (bootstrap, manager):
            if component is not None:
                try:
                    component.shutdown()
                except BaseException as exc:
                    error = error or repr(exc)
        survivors = []
        for pid, created in owned.items():
            try:
                proc = psutil.Process(pid)
                if proc.create_time() == created and proc.status() != psutil.STATUS_ZOMBIE:
                    survivors.append(pid)
            except psutil.NoSuchProcess:
                pass
        write(path / 'outcome.json', outcome)
        write(path / 'shutdown.json', dict(safe=not survivors, owned=owned,
              surviving_pids=survivors, forced_cleanup=forced_cleanup, error=error))
    return 1 if error else 0


def launch_case(path, *, mode, condition, seed, duration=300):
    path.mkdir()
    config = dict(schema='compute-native-case-v1', mode=mode, condition=condition, seed=seed,
                  duration=duration, active=[n for n in NODES if n != 'A' or condition != 'absent'],
                  run_id='compute_'+uuid.uuid4().hex, target_batch_size=4096,
                  rates={'B':128,'D':128,'A':896,'C':128}, microbatch_size=32,
                  state_sharing=True, native_gradient_leader='B', dim=48, learning_rate=.1,
                  matchmaking_time=1, averaging_timeout=15, allreduce_timeout=5,
                  sender_timeout=2, reducer_timeout=5, next_chunk_timeout=2, request_timeout=.5,
                  load_state_timeout=15, delay_state_averaging=True, average_state_every=1,
                  offload_optimizer=False, use_local_updates=False, delay_grad_averaging=False,
                  delay_optimizer_step=False, compression='NoCompression', part_size_bytes=16,
                  evaluation_examples=4096, evaluation_interval=1, threshold_fraction=.1,
                  confirmation_evaluations=10, maximum_evaluation_gap=1.5)
    write(path / 'config.json', config)
    env = dict(os.environ, OPENBLAS_NUM_THREADS='1', OMP_NUM_THREADS='1')
    import psutil
    externally_owned = {}
    supervisor_error = None
    with (path / 'worker.log').open('w') as log:
        process = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), 'trial-worker',
                                    '--case-dir', str(path.resolve())], env=env,
                                   stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        expires = time.monotonic() + (duration if mode != 'markers' else 30) + 150
        try:
            while process.poll() is None:
                try:
                    for child in psutil.Process(process.pid).children(recursive=True):
                        try:
                            externally_owned[str(child.pid)] = child.create_time()
                        except psutil.NoSuchProcess:
                            pass
                except psutil.NoSuchProcess:
                    pass
                write(path / 'owned_processes.json', externally_owned)
                if time.monotonic() >= expires:
                    raise TimeoutError('Native supervisor deadline exceeded')
                try:
                    process.wait(timeout=1)
                except subprocess.TimeoutExpired:
                    pass
            code = process.returncode
        except BaseException as exc:
            supervisor_error = exc
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL); process.wait()
            code = process.returncode
    shutdown_path = path / 'shutdown.json'
    if shutdown_path.exists():
        shutdown = json.loads(shutdown_path.read_text())
        shutdown['owned'].update(externally_owned)
    else:
        shutdown = dict(safe=False, owned=externally_owned, surviving_pids=[],
                        forced_cleanup=[], error='Missing native worker shutdown evidence')
    write(shutdown_path, shutdown)
    from announcement_probe import audit_shutdown
    shutdown = audit_shutdown(path, code, DEPENDENCIES)
    if supervisor_error is not None:
        raise supervisor_error
    if code != 0 or not shutdown['safe']:
        raise RuntimeError(f'Native case failed: {path}; inspect worker.log/failure.json')
    from analyze_compute_exclusion import analyze_case
    report = analyze_case(path)
    write(path / 'analysis.json', report)
    compact = {key: report.get(key) for key in ('schema', 'valid', 'errors', 'mode', 'condition',
                                               'seed', 'event_count', 'active_nodes')}
    if report.get('learning') is not None:
        compact['learning'] = {key: report['learning'].get(key) for key in (
            'censored', 'crossing_time', 'confirmed_time', 'censor_reason', 'final_metrics', 'nodes')}
    write(path / 'checks.json', compact)
    if not report['valid']:
        raise RuntimeError(f'Native case evidence failed: {report["errors"][:8]}')
    return report


def run_suite(args):
    destination = label_path(args.label)
    reference_hash = None
    if destination.exists():
        raise FileExistsError(destination)
    if args.command in ('pilot','confirmation'):
        if args.reference is None:
            raise ValueError('A matching completed reference suite is required')
        reference = args.reference.resolve()
        reference_hash = check_reference(reference, args.command)
    destination.mkdir(); freeze(destination)
    seeds = (11,12,13) if args.command == 'markers' else ((301,) if args.command == 'pilot' else tuple(range(302,307)))
    state = dict(schema='compute-native-suite-v1', mode=args.command, status='running', cases=[],
                 seeds=list(seeds), duration=300, baseline_learning_passed=False,
                 reference=str(args.reference.resolve()) if args.reference else None,
                 reference_inventory_sha256=reference_hash)
    write(destination / 'status.json', state)
    try:
        for index, seed in enumerate(seeds):
            order = CONDITIONS[index % 3:] + CONDITIONS[:index % 3]
            for condition in order:
                name = f'{condition}_s{seed}'
                print('RUN',name,flush=True)
                check_sources(destination)
                result = launch_case(destination / name, mode=args.command, condition=condition, seed=seed)
                state['cases'].append(dict(path=name, valid=result['valid']))
                if condition == 'baseline' and args.command == 'pilot':
                    state['baseline_learning_passed'] = (not result['learning']['censored']
                        and json.loads((destination/name/'outcome.json').read_text())['completed'])
                write(destination / 'status.json', state)
        check_sources(destination)
        state['status'] = 'complete'
    except BaseException as exc:
        state.update(status='failed',error=repr(exc))
        raise
    finally:
        write(destination / 'status.json', state)
        write(destination / 'artifact_inventory.json', inventory(destination))
    return destination


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('preflight','markers','pilot','confirmation','validate','simulate','trial-worker'))
    parser.add_argument('--label')
    parser.add_argument('--reference', type=Path)
    parser.add_argument('--case-dir', type=Path)
    args = parser.parse_args()
    if args.command == 'trial-worker':
        code = trial_worker(args.case_dir)
        sys.stdout.flush(); sys.stderr.flush()
        os._exit(code)
    if args.command == 'preflight':
        print(json.dumps(native_imports(),indent=2)); return
    if not args.label:
        parser.error('--label required')
    if args.command in ('validate', 'simulate'):
        from compute_simulation import run_stage
        print(run_stage(args))
        return
    print(run_suite(args))


if __name__ == '__main__':
    main()
