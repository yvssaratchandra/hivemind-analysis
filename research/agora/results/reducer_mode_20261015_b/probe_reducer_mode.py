"""Bounded private test of self-declared averaging mode; no authorization service.

Run with .runtime/venv/bin/python probe_reducer_mode.py --label UNIQUE_LABEL.
Uses native Agora matchmaking, allocation, and all-reduce. Instrumentation only
records arguments/results; the candidate's local mode is the sole arm difference.
"""
import argparse
import asyncio
import hashlib
import inspect
import json
import multiprocessing as mp
import os
from pathlib import Path
import shutil
import sys
import time
import traceback

import psutil
import torch
from agora_server.core.all_reduce.ar_runner import AllReduceRunner
from agora_server.core.averaging.averager import DecentralizedAverager
from agora_server.core.all_reduce.matchmaking import Matchmaking
from agora_server.core.server.dht_handler import declare_experts
from hivemind import DHT
from hivemind.utils import get_dht_time
from hivemind.utils.serializer import MSGPackSerializer

ROOT = Path(__file__).resolve().parent
NAMES = ('H0', 'H1', 'X')


def emit(path, kind, **fields):
    record = dict(kind=kind, pid=os.getpid(), monotonic=time.monotonic(), **fields)
    with open(path, 'a') as stream:
        stream.write(json.dumps(record, allow_nan=False) + '\n')


def observe(path):
    native_init = AllReduceRunner.__init__
    def init(self, *args, **kwargs):
        native_init(self, *args, **kwargs)
        emit(path, 'allocation', peers=list(map(str, self.ordered_peer_ids)),
             peer_id=str(self.peer_id), fractions=list(self.peer_fractions),
             modes=[m.name for m in self.modes],
             parts=list(self.tensor_part_container.num_parts_by_peer))
    AllReduceRunner.__init__ = init

    native_rpc = AllReduceRunner.rpc_aggregate_part
    async def rpc(self, stream, context):
        count = 0
        emit(path, 'reducer_rpc_start', sender=str(context.remote_id))
        async for result in native_rpc(self, stream, context):
            count += 1
            emit(path, 'reducer_response', sender=str(context.remote_id),
                 code=int(result.code), tensor_bytes=result.tensor_part.SerializeToString().hex())
            yield result
        emit(path, 'reducer_rpc_end', sender=str(context.remote_id), responses=count)
    AllReduceRunner.rpc_aggregate_part = rpc

    native_group = Matchmaking.look_for_group
    async def group(self, *args, **kwargs):
        result = await native_group(self, *args, **kwargs)
        if result is not None:
            metadata = [MSGPackSerializer.loads(v) for v in result.gathered]
            emit(path, 'roster', peers=list(map(str, result.peer_ids)),
                 advertised=[dict(bandwidth=v[0], mode=v[1],
                                  application=MSGPackSerializer.loads(v[2])) for v in metadata])
        return result
    Matchmaking.look_for_group = group


def worker(index, mode, trial, initial_peers, connection):
    mp.set_start_method('fork', force=True)
    torch.set_num_threads(1)
    trial = Path(trial)
    name = NAMES[index]
    path = trial / f'{name}.jsonl'
    log = open(trial / f'{name}.log', 'a', buffering=1)
    os.dup2(log.fileno(), 1)
    os.dup2(log.fileno(), 2)
    observe(path)
    dht = averager = None
    tracked = []
    try:
        dht = DHT(initial_peers=initial_peers, host_maddrs=['/ip4/127.0.0.1/tcp/0'],
                  use_ipfs=False, num_replicas=3, start=True, startup_timeout=20)
        addresses = list(map(str, dht.get_visible_maddrs()))
        assert addresses and all('/ip4/127.0.0.1/' in a for a in addresses)
        peer_id = str(dht.peer_id)
        emit(path, 'identity', name=name, peer_id=peer_id, declared_mode=mode,
             authorization='not used; private aggregation-path test')
        connection.send(dict(kind='boot', name=name, peer_id=peer_id, addresses=addresses))
        command = connection.recv()
        stage = command['stage']
        declare_experts(dht, [f'{stage}.0.{index}'], expiration_time=get_dht_time()+90)
        tensors = [torch.arange(96, dtype=torch.float32)/96 + index]
        averager = DecentralizedAverager(
            tensors, dht, start=True, prefix=stage+'_state_averager',
            mode=mode, client_mode=False, bandwidth=1.0,
            min_matchmaking_time=6, check_interval=.1,
            allreduce_timeout=20, sender_timeout=8, reducer_timeout=12,
            part_size_bytes=32, allow_state_sharing=False, shutdown_timeout=5)
        averager.global_epoch = 19
        tracked = [(p.pid, p.create_time()) for p in psutil.Process().children(recursive=True)]
        emit(path, 'children', processes=tracked)
        connection.send(dict(kind='ready', name=name, processes=tracked))
        assert connection.recv()['kind'] == 'run'
        with averager.get_tensors() as values:
            emit(path, 'before', values=values[0].tolist())
        result = averager.step(gather=dict(name=name), timeout=30, allow_retries=False)
        with averager.get_tensors() as values:
            actual = values[0].clone()
        expected = torch.arange(96, dtype=torch.float32)/96 + 1
        error = float((actual-expected).abs().max())
        emit(path, 'result', values=actual.tolist(), max_error=error,
             gathered_peers=list(map(str, result)))
        connection.send(dict(kind='result', name=name, max_error=error))
        assert connection.recv()['kind'] == 'stop'
    except BaseException:
        detail = traceback.format_exc()
        emit(path, 'error', detail=detail)
        connection.send(dict(kind='error', name=name, detail=detail))
    finally:
        if averager is not None:
            try:
                averager.shutdown()
            except BaseException:
                emit(path, 'shutdown_error', detail=traceback.format_exc())
        if dht is not None:
            try:
                dht.shutdown()
                dht.join(timeout=5)
            except BaseException:
                emit(path, 'shutdown_error', detail=traceback.format_exc())
        # Native p2pd can exit just after its owning Python process. Allow that
        # bounded normal teardown to finish before writing the closure marker.
        pending = []
        for pid, created in tracked:
            try:
                p = psutil.Process(pid)
                if p.create_time() == created:
                    pending.append(p)
            except psutil.NoSuchProcess:
                pass
        psutil.wait_procs(pending, timeout=3)
        survivors = []
        for pid, created in tracked:
            try:
                p = psutil.Process(pid)
                if p.create_time() == created and p.is_running() and p.status() != psutil.STATUS_ZOMBIE:
                    survivors.append(pid)
            except psutil.NoSuchProcess:
                pass
        emit(path, 'closed', survivors=survivors)
        connection.close()


def receive(pipe, expected, deadline):
    if not pipe.poll(max(0, deadline-time.monotonic())):
        raise TimeoutError(f'Waiting for {expected}')
    message = pipe.recv()
    if message['kind'] != expected:
        raise RuntimeError(str(message))
    return message


def run_arm(suite, arm, candidate_mode):
    trial = suite / arm
    trial.mkdir()
    config = dict(arm=arm, names=NAMES, candidate='X', candidate_mode=candidate_mode,
                  honest_modes=['node', 'node'], bandwidths=[1, 1, 1],
                  tensor_elements=96, epoch=19, authentication=False,
                  network='loopback only', rounds=1, random_seed=None,
                  initialization='arange(96)/96 + peer index',
                  matchmaking_seconds=6, aggregation_seconds=20)
    (trial/'config.json').write_text(json.dumps(config, indent=2)+'\n')
    context = mp.get_context('spawn')
    peers = []
    children = []
    initial = []
    identities = {}
    deadline = time.monotonic()+90
    stage = 'modeprobe'+hashlib.sha256((suite.name+arm).encode()).hexdigest()[:10]
    try:
        for index, name in enumerate(NAMES):
            parent, child = context.Pipe()
            process = context.Process(target=worker,
                args=(index, candidate_mode if name=='X' else 'node', str(trial), initial, child))
            process.start()
            child.close()
            peers.append((name, process, parent))
            boot = receive(parent, 'boot', deadline)
            identities[name] = boot['peer_id']
            if not initial:
                initial = boot['addresses']
        (trial/'identities.json').write_text(json.dumps(identities, indent=2)+'\n')
        for _, _, pipe in peers:
            pipe.send(dict(kind='configure', stage=stage))
        for _, _, pipe in peers:
            children.extend(receive(pipe, 'ready', deadline)['processes'])
        # Separate registry warm-up from the measured matchmaking operation.
        time.sleep(.5)
        for _, _, pipe in peers:
            pipe.send(dict(kind='run'))
        for _, _, pipe in peers:
            receive(pipe, 'result', deadline)
    finally:
        for _, process, pipe in peers:
            if process.is_alive():
                try:
                    pipe.send(dict(kind='stop'))
                except (BrokenPipeError, OSError):
                    pass
        for _, process, pipe in peers:
            process.join(timeout=15)
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
            pipe.close()
        # Reap only recorded descendants; do not touch unrelated processes.
        remaining = []
        forced = []
        for pid, created in children:
            try:
                p = psutil.Process(pid)
                if p.create_time()!=created or p.status()==psutil.STATUS_ZOMBIE:
                    continue
                if p.is_running():
                    p.terminate()
                    forced.append(pid)
                    try:
                        p.wait(timeout=3)
                    except psutil.TimeoutExpired:
                        p.kill()
                        p.wait(timeout=3)
                    if p.is_running() and p.status()!=psutil.STATUS_ZOMBIE:
                        remaining.append(pid)
            except psutil.NoSuchProcess:
                pass
        shutdown = dict(exitcodes={name:p.exitcode for name,p,_ in peers},
                        forced_descendants=forced, survivors=remaining)
        (trial/'shutdown.json').write_text(json.dumps(shutdown, indent=2)+'\n')
    return trial


def verify(trial, expected_mode):
    identities=json.loads((trial/'identities.json').read_text())
    target=identities['X']
    counts={}
    allocations={}
    rosters=[]
    max_error=0
    for name in NAMES:
        events=[json.loads(line) for line in (trial/f'{name}.jsonl').read_text().splitlines()]
        assert not any(r['kind'] in ('error','shutdown_error') for r in events)
        roster=[r for r in events if r['kind']=='roster']
        allocation=[r for r in events if r['kind']=='allocation']
        result=[r for r in events if r['kind']=='result']
        assert len(roster)==len(allocation)==len(result)==1
        roster=roster[0]; allocation=allocation[0]; result=result[0]
        assert set(roster['peers'])==set(identities.values())
        rosters.append(tuple(roster['peers']))
        idx=allocation['peers'].index(target)
        mode=roster['advertised'][roster['peers'].index(target)]['mode']
        assert mode==expected_mode
        allocations[name]=dict(candidate_allocation=allocation['fractions'][idx],
                               candidate_parts=allocation['parts'][idx])
        assert (allocation['parts'][idx]>0)==(expected_mode==0)
        assert result['max_error']<=1e-5
        max_error=max(max_error,result['max_error'])
        ends=[r for r in events if r['kind']=='reducer_rpc_end']
        counts[name]={r['sender']:r['responses'] for r in ends}
        assert events[-1]['kind']=='closed' and not events[-1]['survivors']
    assert len(set(rosters))==1
    if expected_mode==0:
        assert set(counts['X'])=={identities['H0'],identities['H1']}
        assert all(v>0 for v in counts['X'].values())
    else:
        assert not counts['X']
    shutdown=json.loads((trial/'shutdown.json').read_text())
    assert not shutdown['survivors'] and not shutdown['forced_descendants']
    assert all(code==0 for code in shutdown['exitcodes'].values())
    return dict(passed=True, allocations=allocations, inbound_reduction_responses=counts,
                max_error=max_error, roster_agreement=True)


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--label', required=True)
    args=parser.parse_args()
    if not args.label or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-' for c in args.label):
        raise ValueError('Use a simple distinct output label')
    suite=ROOT/'results'/('reducer_mode_'+args.label)
    suite.mkdir(exist_ok=False)
    shutil.copy2(__file__,suite/'probe_reducer_mode.py')
    files={Path(inspect.getfile(cls)).resolve() for cls in
           (DHT,DecentralizedAverager,AllReduceRunner,Matchmaking)}
    files.update(ROOT/name for name in ('baseline_8b.py','run_8b_baseline.py','analyze_8b_baseline.py'))
    before={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in files}
    (suite/'source_hashes.json').write_text(json.dumps(before,indent=2)+'\n')
    snapshots=suite/'source_snapshots'
    snapshots.mkdir()
    for index,p in enumerate(sorted(files)):
        shutil.copy2(p,snapshots/f'{index}_{p.name}')
    (suite/'runtime.json').write_text(json.dumps(dict(python=sys.version,
        executable=sys.executable, torch=torch.__version__, psutil=psutil.__version__,
        expected_agora_revision='37d29a966528675635161007d7b43388012a98b2',
        expected_hivemind_revision='4d5c41495be082490ea44cce4e9dd58f9926bb4e'),indent=2)+'\n')
    try:
        summaries={}
        for arm,mode,mode_id in [('client_control','client',1),('node_claim','node',0)]:
            trial=run_arm(suite,arm,mode)
            summaries[arm]=verify(trial,mode_id)
            print(arm,'passed',flush=True)
        assert all(hashlib.sha256(Path(p).read_bytes()).hexdigest()==h for p,h in before.items())
        (suite/'verification.json').write_text(json.dumps(dict(passed=True,arms=summaries,
            checked_sources_unchanged=True,scope='No admission, integrity service, stage placement, or public network tested'),indent=2)+'\n')
    except BaseException:
        (suite/'failure.txt').write_text(traceback.format_exc())
        raise
    print(suite,flush=True)


if __name__=='__main__':
    main()
