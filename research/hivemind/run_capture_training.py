#!/usr/bin/env python3
"""Private native Optimizer pilot: record-holder capture and dual-channel omission."""
from __future__ import annotations
import argparse
import hashlib
import json
import multiprocessing as mp
import os
from pathlib import Path
import queue
import signal
import subprocess
import sys
import time
import traceback
import uuid

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import run_compute_exclusion as base
import recordholder_exclusion_smoke as holders
from capture_training_hooks import CONDITIONS
from announcement_artifacts import inventory, verify
from run_compute_exclusion import event_writer, write, native_imports

NODES = ('A', 'X', 'D', 'C')
SOURCES = ('run_capture_training.py', 'capture_training_hooks.py', 'analyze_capture_training.py',
           'run_compute_exclusion.py', 'compute_native_hooks.py', 'compute_workload.py',
           'capture_training_workload.py', 'capture_training_evidence.py',
           'recordholder_exclusion_smoke.py', 'tracking_combination_smoke.py',
           'announcement_artifacts.py', 'announcement_probe.py', 'analyze_compute_exclusion.py')


def evaluator(path, seed, arrays, origin, duration, stop):
    from compute_workload import evaluation
    from capture_training_workload import evaluate_models
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
    from hivemind.optim.grad_averager import GradientAverager
    from hivemind.utils.crypto import RSAPrivateKey
    from compute_workload import batch, next_batch_delay
    from compute_native_hooks import install_hooks
    from capture_training_hooks import install_capture_policy
    from capture_training_evidence import install_evidence
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
                      gradient_prefix=prefix, identities=identities, snapshot=publish, leader_fixture=False)
        install_capture_policy(condition=config['treatment'], gradient_prefix=prefix,
                               state_prefix=config['run_id']+'_state_averager',
                               identities=identities, emit=emit)
        install_evidence(emit)
        active = config['active']
        common = dict(target_group_size=len(active), min_group_size=3,
                      initial_group_bits='', part_size_bytes=16,
                      sender_timeout=2, reducer_timeout=5, request_timeout=4.5)
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

        class ObservedSGD(torch.optim.SGD):
            def step(self, closure=None):
                before = model.detach().tolist()
                gradient = model.grad.detach().tolist() if model.grad is not None else None
                result = super().step(closure)
                emit('sgd_step', parameters_before=before, gradient=gradient,
                     parameters_after=model.detach().tolist())
                return result

        optimizer = ObservedOptimizer(dht=dht, run_id=config['run_id'], params=[model],
            optimizer=partial(ObservedSGD, lr=.1), target_batch_size=4096,
            batch_size_per_step=32, matchmaking_time=(1 if config['treatment'] == 'baseline' else .35 if node == 'X' else 3.), averaging_timeout=15,
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
            x, y, metadata = batch(config['seed'], 'B' if node == 'X' else node, index)
            metadata['source_node'] = metadata['node']
            metadata['node'] = node
            optimizer.zero_grad()
            before = model.detach().tolist()
            prediction = torch.from_numpy(x) @ model
            loss = .5 * ((prediction - torch.from_numpy(y)) ** 2).mean()
            loss.backward()
            computed = time.monotonic()
            delay = next_batch_delay(began, computed, 32/config['rates'][node])
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


def holder_worker(addresses, specs, identities, holder_ids, policies, events, ready, stop):
    holders.CHANNELS = ('gradient', 'state')
    holders._START = time.monotonic()
    holders._run_holders(addresses, specs, identities, holder_ids, policies,
                         events, ready, stop, 'capture_training')


def trial_worker(path):
    path = Path(path).resolve()
    config = json.loads((path/'config.json').read_text())
    write(path/'provenance.json', native_imports())
    import torch, psutil
    from hivemind import DHT
    from hivemind.dht.routing import DHTID
    torch.set_num_threads(1)
    ctx = mp.get_context('fork')
    manager = ctx.Manager()
    identities, holder_ids = manager.dict(), manager.dict()
    policies = manager.dict(ready=False, keys={})
    messages, holder_events = ctx.Queue(), manager.Queue()
    ready_holders, identities_ready, release, stop = (ctx.Event() for _ in range(4))
    origin, anchor = ctx.Value('d', 0.), ctx.Value('d', 0.)
    snapshots = {n: ctx.Array('d', 50, lock=True) for n in NODES}
    children, owned = [], {}
    bootstrap = None
    outcome = dict(completed=False, status='initializing')
    error = None
    holder_log = (path/'holder_events.jsonl').open('w', buffering=1)

    def monitor():
        for proc in psutil.Process().children(recursive=True):
            try:
                owned[str(proc.pid)] = proc.create_time()
            except psutil.NoSuchProcess:
                pass
        while True:
            try:
                event = holder_events.get_nowait()
            except queue.Empty:
                break
            holder_log.write(json.dumps(event)+'\n')
            if event['kind'] == 'holder_error':
                raise RuntimeError(f'Holder failed: {event}')

    try:
        bootstrap = DHT(host_maddrs=holders.LOCALHOST, auto_nat=False, nat_port_map=False,
                        use_relay=False, start=True)
        addresses = holders._loopback_addresses(bootstrap, 'bootstrap')
        identities[str(bootstrap.peer_id)] = 'bootstrap'
        specs = {}
        for channel, suffix in (('gradient', '_grad_averager'), ('state', '_state_averager')):
            prefix = config['run_id'] + suffix
            key = prefix+'.0b'
            key_id = DHTID.generate(source=key)
            specs[channel] = dict(prefix=prefix, key=key, target_id=int(key_id),
                                  key_hex=key_id.to_bytes().hex())
        write(path/'keys.json', specs)
        proc = ctx.Process(target=holder_worker, args=(addresses, specs, identities, holder_ids,
                           policies, holder_events, ready_holders, stop), name='capture_holders')
        proc.start(); children.append(proc)
        deadline = time.monotonic()+90
        while not ready_holders.wait(.1):
            monitor()
            if proc.exitcode is not None or time.monotonic() > deadline:
                raise RuntimeError('Holder startup failed')
        monitor()
        if len(holder_ids) != 40:
            raise RuntimeError(f'Expected 40 holders, found {len(holder_ids)}')
        for node in NODES:
            proc = ctx.Process(target=peer_worker, args=(path, config, node, addresses,
                identities, identities_ready, release, origin, stop, messages, snapshots, anchor),
                name=f'capture_{node}')
            proc.start(); children.append(proc)
        deadline = time.monotonic()+100
        ready, done = set(), set()
        filter_initialized = False
        while len(ready) != 4:
            monitor()
            if time.monotonic() > deadline:
                raise TimeoutError(f'Optimizer startup: {sorted(ready)}')
            if any(p.exitcode is not None for p in children):
                raise RuntimeError('Child exited during startup')
            if not filter_initialized and set(NODES).issubset(set(identities.values())):
                x_id = next(k for k, v in identities.items() if v == 'X')
                policies['keys'] = {s['key_hex']: dict(arm=config['treatment'], channel=channel,
                    filtered_requesters=[] if config['treatment'] == 'baseline' else ['A','D','C'],
                    allowed_peer_ids=[x_id]) for channel,s in specs.items()}
                policies['ready'] = True
                # No pre-seeded advertisements; retain native storage and caching.
                settle = time.monotonic()+9
                while time.monotonic() < settle:
                    monitor(); time.sleep(.1)
                identities_ready.set()
                filter_initialized = True
            try:
                message = messages.get(timeout=.1)
            except queue.Empty:
                continue
            if message[0] == 'error':
                raise RuntimeError(str(message))
            if message[0] == 'ready':
                ready.add(message[1])
        write(path/'identities.json', dict(identities))
        origin.value = time.monotonic()
        write(path/'release.json', dict(monotonic=origin.value, ready=sorted(ready)))
        proc = ctx.Process(target=evaluator, args=(path, config['seed'], snapshots,
                          origin, config['duration'], stop), name='capture_evaluator')
        proc.start(); children.append(proc)
        release.set()
        deadline = origin.value+config['duration']+35
        while len(done) != 4 and time.monotonic() < deadline:
            monitor()
            try:
                message = messages.get(timeout=.1)
            except queue.Empty:
                continue
            if message[0] == 'error':
                raise RuntimeError(str(message))
            if message[0] == 'done':
                done.add(message[1])
        outcome = dict(completed=len(done)==4, status='completed' if len(done)==4 else 'native_noncompletion',
                       done=sorted(done), observed_seconds=time.monotonic()-origin.value)
    except BaseException as exc:
        error = repr(exc)
        write(path/'failure.json', dict(error=error, traceback=traceback.format_exc()))
    finally:
        monitor()
        stop.set(); release.set(); identities_ready.set()
        # Allow ordinary native shutdown first; preserve every forced termination.
        until = time.monotonic()+20
        while any(p.is_alive() for p in children) and time.monotonic() < until:
            monitor(); time.sleep(.1)
        forced = []
        for proc in children:
            if proc.is_alive():
                forced.append(dict(pid=proc.pid, action='terminate'))
                proc.terminate()
            proc.join(2)
            if proc.is_alive():
                proc.kill(); proc.join(2)
        monitor()
        holder_log.close()
        if bootstrap is not None:
            bootstrap.shutdown()
        manager.shutdown()
        write(path/'outcome.json', dict(outcome, error=error))
        write(path/'shutdown.json', dict(owned=owned, forced_cleanup=forced,
                                         safe=False, surviving_pids=[], error=error))
    return 1 if error else 0


def launch_case(path, treatment, seed, duration):
    path.mkdir()
    config = dict(schema='capture-training-v1', mode='pilot', condition='baseline',
        treatment=treatment, seed=seed, duration=duration, active=list(NODES),
        run_id='capture_'+uuid.uuid4().hex, rates={'A':896,'X':128,'D':128,'C':128},
        workload_stream_alias={'X':'B'}, gradient_leader_fixture=False,
        topology=dict(participants=4, holders=40, bootstrap=1),
        state_sharing=True, average_state_every=1, fixed_empty_group_bits=True,
        dim=48, target_batch_size=4096, learning_rate=.1, microbatch_size=32,
        threshold_fraction=.1, confirmation_evaluations=10, maximum_evaluation_gap=1.5)
    write(path/'config.json', config)
    import psutil
    before = holders.smoke.clock_sample()
    owned = {}
    suspended = False
    with (path/'worker.log').open('w') as log:
        proc = subprocess.Popen([sys.executable, str(Path(__file__).resolve()),
            '--worker', str(path.resolve())], stdout=log, stderr=subprocess.STDOUT,
            env=dict(os.environ, OPENBLAS_NUM_THREADS='1', OMP_NUM_THREADS='1'), start_new_session=True)
        until = time.monotonic()+duration+210
        previous = before
        try:
            while proc.poll() is None:
                now = holders.smoke.clock_sample()
                if not holders.smoke.clock_delta(previous, now)['valid']:
                    suspended = True
                    raise RuntimeError('Suspension or clock jump detected')
                previous = now
                try:
                    for child in psutil.Process(proc.pid).children(recursive=True):
                        try:
                            owned[str(child.pid)] = child.create_time()
                        except psutil.NoSuchProcess:
                            pass
                except psutil.NoSuchProcess:
                    pass
                write(path/'owned_processes.json', owned)
                if time.monotonic() > until:
                    raise TimeoutError('Case supervisor deadline')
                try:
                    proc.wait(timeout=.5)
                except subprocess.TimeoutExpired:
                    pass
        finally:
            if proc.poll() is None:
                os.killpg(proc.pid, signal.SIGTERM)
                try:
                    proc.wait(5)
                except subprocess.TimeoutExpired:
                    os.killpg(proc.pid, signal.SIGKILL); proc.wait()
            shutdown = json.loads((path/'shutdown.json').read_text()) if (path/'shutdown.json').exists() else dict(owned={}, safe=False)
            shutdown['owned'].update(owned)
            write(path/'shutdown.json', shutdown)
            from announcement_probe import audit_shutdown
            shutdown = audit_shutdown(path, proc.returncode, base.DEPENDENCIES)
            write(path/'clock_audit.json', dict(holders.smoke.clock_delta(before, holders.smoke.clock_sample()),
                                                suspension_detected=suspended))
    from analyze_capture_training import analyze_case
    report = analyze_case(path)
    write(path/'analysis.json', report)
    if proc.returncode != 0 or not shutdown['safe'] or not report['valid']:
        raise RuntimeError(f'Case evidence failed: {path}: {report["errors"][:8]}')
    return report


def run_suite(args):
    destination = base.label_path(args.label)
    destination.mkdir()
    (destination/'harness').mkdir()
    import shutil
    hashes = {}
    for name in SOURCES:
        source = HERE/name
        shutil.copyfile(source, destination/'harness'/name)
        hashes[name] = hashlib.sha256(source.read_bytes()).hexdigest()
    write(destination/'source_hashes.json', hashes)
    status = dict(status='running', seed=args.seed, duration=args.duration,
                  conditions=args.conditions, cases=[])
    write(destination/'status.json', status)
    try:
        for condition in args.conditions:
            print(f'Start {condition}, seed {args.seed}, {args.duration}s', flush=True)
            report = launch_case(destination/condition, condition, args.seed, args.duration)
            status['cases'].append(dict(condition=condition, valid=report['valid']))
            write(destination/'status.json', status)
            print(f'Done {condition}: crossing={report["learning"]["crossing_time"]}, exposure={report["exposure"]}', flush=True)
        changed = [name for name,digest in hashes.items() if hashlib.sha256((HERE/name).read_bytes()).hexdigest()!=digest]
        if changed:
            raise RuntimeError(f'Sources changed: {changed}')
        status['status']='complete'
    except BaseException as exc:
        status.update(status='failed', error=repr(exc))
        raise
    finally:
        write(destination/'status.json', status)
        from analyze_capture_training import report_suite
        report_suite(destination)
        write(destination/'artifact_inventory.json', inventory(destination))
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--worker', type=Path)
    parser.add_argument('--label')
    parser.add_argument('--seed', type=int, default=501)
    parser.add_argument('--duration', type=int, default=180)
    parser.add_argument('--conditions', nargs='+', choices=CONDITIONS, default=list(CONDITIONS))
    args = parser.parse_args()
    if args.worker:
        return trial_worker(args.worker)
    if not args.label or args.duration < 10:
        parser.error('A fresh label and duration >=10 seconds are required')
    return run_suite(args)


if __name__ == '__main__':
    raise SystemExit(main())
