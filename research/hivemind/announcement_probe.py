#!/usr/bin/env python3
"""Private native Hivemind announcement probe; no public bootstrap addresses.

Hooks are confined to this process and its forked native averagers. Core source
files are never edited. B's canonical order is an explicit experimental fixture.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import inspect
import importlib.metadata
import json
import os
from pathlib import Path
import queue
import random
import signal
import subprocess
import sys
import tarfile
import time
import traceback
import uuid

ROOT = Path(__file__).resolve().parents[2]
NODES = ('B', 'D', 'A', 'C')
CONDITIONS = ('baseline4', 'control3', 'ordering', 'membership')
NATIVE_FILES = ('averaging/matchmaking.py', 'averaging/key_manager.py',
                'averaging/allreduce.py', 'averaging/partition.py',
                'averaging/averager.py', 'averaging/load_balancing.py',
                'averaging/group_info.py', 'proto/averaging.proto',
                'dht/node.py', 'dht/dht.py', 'dht/protocol.py',
                'p2p/p2p_daemon.py', 'utils/serializer.py',
                'compression/serialization.py')
EVENTS = None
START = 0.0
IDENTITIES = {}


def write(path, value):
    path.write_text(json.dumps(value, indent=2, allow_nan=False) + '\n')


def emit(kind, node='', **fields):
    EVENTS.put(dict(kind=kind, node=node, time=time.monotonic()-START,
                    pid=os.getpid(), **fields))


def name(peer):
    return IDENTITIES.get(str(peer), str(peer))


def roster_for(condition, node):
    if condition == 'control3' or (condition == 'membership' and node != 'A'):
        return ['B', 'D', 'C']
    return ['A', 'D', 'B', 'C'] if condition == 'ordering' and node == 'A' else list(NODES)


def reorder(group, order):
    """Keep peer IDs and their original opaque metadata aligned."""
    from hivemind.averaging.group_info import GroupInfo
    records = {name(peer): (peer, metadata) for peer, metadata in zip(group.peer_ids, group.gathered)}
    if (len(group.peer_ids) != len(group.gathered) or len(order) != len(set(order))
            or len(records) != len(group.peer_ids) or not set(order).issubset(records)):
        raise ValueError('Invalid roster transformation')
    return GroupInfo(group.group_id, tuple(records[n][0] for n in order),
                     tuple(records[n][1] for n in order))


def native_imports(args):
    os.chdir('/tmp')
    sys.path[:] = [p for p in sys.path if p and Path(p).resolve() != ROOT]
    sys.path.insert(0, str(args.dependencies.resolve()))
    sys.path.insert(0, str(args.package_root.resolve()))
    import hivemind
    import torch
    from hivemind.averaging.load_balancing import load_balance_peers
    from hivemind.averaging.partition import TensorPartContainer
    package = Path(hivemind.__file__).resolve().parent
    if package.parent != args.package_root.resolve():
        raise RuntimeError(f'Wrong native import: {package}')
    hashes = {}
    for relative in NATIVE_FILES:
        source, imported = ROOT/'hivemind'/relative, package/relative
        # Some prepared packages omit .proto; compare generated protobuf separately.
        if relative.endswith('.proto') and not imported.exists():
            continue
        if source.read_bytes() != imported.read_bytes():
            raise RuntimeError(f'Native snapshot differs from repository: {relative}')
        hashes[relative] = hashlib.sha256(imported.read_bytes()).hexdigest()
    from hivemind.proto import averaging_pb2, runtime_pb2, dht_pb2, p2pd_pb2
    binary = package/'hivemind_cli/p2pd'
    if not os.access(binary, os.X_OK):
        raise RuntimeError(f'p2pd unavailable or not executable: {binary}')
    allocations = {}
    for count in (3, 4):
        allocation = load_balance_peers(432, [None]*count)
        container = TensorPartContainer([torch.zeros(48, 9)], allocation, part_size_bytes=144)
        if tuple(allocation) != (432//count,)*count or container.num_parts_by_peer != (12//count,)*count:
            raise RuntimeError('Native allocation differs from declared fixture')
        allocations[str(count)] = dict(elements=list(map(int, allocation)),
                                       chunks=list(container.num_parts_by_peer))
    from hivemind.dht.node import DHTNode
    cache_defaults = {key: param.default for key, param in inspect.signature(DHTNode.create).parameters.items()
                      if key.startswith('cache_') or key == 'reuse_get_requests'}
    return package, dict(allocations=allocations, cache_defaults=cache_defaults,
                        executable=sys.executable, dependencies=str(args.dependencies.resolve()),
                        dependency_versions={key:importlib.metadata.version(key) for key in
                                             ('torch','numpy','protobuf','grpcio','psutil')},
                        package=str(package), hashes=hashes, python=sys.version,
                        torch=torch.__version__, hivemind=hivemind.__version__,
                        proto_hashes={p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                                      for p in (package/'proto').glob('*_pb2.py')},
                        p2pd_sha256=hashlib.sha256((package/'hivemind_cli/p2pd').read_bytes()).hexdigest())


def install_hooks(condition):
    from hivemind.averaging.matchmaking import Matchmaking
    from hivemind.averaging.group_info import GroupInfo
    from hivemind.averaging import DecentralizedAverager
    from hivemind.averaging.allreduce import AllReduceRunner
    from hivemind.averaging.key_manager import GroupKeyManager
    from hivemind.averaging.partition import TensorPartContainer
    from hivemind.proto import averaging_pb2 as pb
    from hivemind.dht.routing import DHTID
    from hivemind.utils.serializer import MSGPackSerializer
    from hivemind.compression import deserialize_torch_tensor

    def gathered_names(gathered):
        return [MSGPackSerializer.loads(MSGPackSerializer.loads(g)[2])['node'] for g in gathered]

    def payload_fields(msg):
        from hivemind.p2p import PeerID
        return dict(members=[name(PeerID(p)) for p in msg.ordered_peer_ids],
                    gathered_nodes=gathered_names(msg.gathered),
                    payload_hex=msg.SerializeToString().hex(), group_id=msg.group_id.hex())

    declare = GroupKeyManager.declare_averager
    async def observed_declare(self, *args, **kwargs):
        result = await declare(self, *args, **kwargs)
        if name(self.peer_id) == 'B' and result and kwargs.get('looking_for_group', True):
            emit('leader_advertised', 'B', group_key=args[0] if args else kwargs['group_key'],
                 expiration_time=kwargs.get('expiration_time', args[2] if len(args)>2 else None))
        return result
    GroupKeyManager.declare_averager = observed_declare

    # Mirrors the small native method, changing only its random ordering for B.
    # Non-B leader assembly remains fully native and invalidates the experiment.
    assemble = Matchmaking.leader_assemble_group
    async def fixed_assemble(self):
        if name(self.peer_id) != 'B':
            emit('unexpected_leader', name(self.peer_id))
            return await assemble(self)
        assert self.lock_looking_for_group.locked() and self.lock_request_join_group.locked()
        assert not self.client_mode and not self.assembled_group.done()
        gathered = {name(p): (p, r.gather) for p, r in self.current_followers.items()}
        gathered['B'] = (self.peer_id, self.step_control.data_for_gather)
        order = ['B', 'D', 'C'] if condition == 'control3' else list(NODES)
        if set(gathered) != set(order):
            raise RuntimeError(f'Unexpected B membership: {list(gathered)}')
        group = GroupInfo(DHTID.generate().to_bytes(), tuple(gathered[n][0] for n in order),
                          tuple(gathered[n][1] for n in order))
        await self.group_key_manager.update_key_on_group_assembled(group)
        self.assembled_group.set_result(group)
        emit('leader_assembled', 'B', members=order, native_group_id=group.group_id.hex(),
             fixture='B-only canonical ordering replacing native shuffle')
        return group
    Matchmaking.leader_assemble_group = fixed_assemble

    join = Matchmaking.rpc_join_group
    async def observed_join(self, request, context):
        async for response in join(self, request, context):
            if response.code == pb.ACCEPTED:
                emit('join_accepted', name(self.peer_id), follower=name(context.remote_id))
            if response.code == pb.BEGIN_ALLREDUCE:
                if name(self.peer_id) == 'B':
                    from hivemind.p2p import PeerID
                    original = GroupInfo(response.group_id,
                        tuple(PeerID(p) for p in response.ordered_peer_ids), tuple(response.gathered))
                    changed = reorder(original, roster_for(condition, name(context.remote_id)))
                    response = pb.MessageFromLeader(code=pb.BEGIN_ALLREDUCE,
                        group_id=changed.group_id, ordered_peer_ids=[p.to_bytes() for p in changed.peer_ids],
                        gathered=changed.gathered)
                emit('announcement_sent', name(self.peer_id), recipient=name(context.remote_id),
                     **payload_fields(response))
            yield response
    Matchmaking.rpc_join_group = observed_join

    follow = Matchmaking.follower_assemble_group
    async def observed_follow(self, leader, msg):
        emit('announcement_received', name(self.peer_id), sender=name(leader), **payload_fields(msg))
        return await follow(self, leader, msg)
    Matchmaking.follower_assemble_group = observed_follow

    aggregate = DecentralizedAverager._aggregate_with_group
    async def local_aggregate(self, group_info, *args, **kwargs):
        if name(self.peer_id) == 'B' and condition == 'membership':
            group_info = reorder(group_info, ['B', 'D', 'C'])
        decoded = [MSGPackSerializer.loads(g) for g in group_info.gathered]
        emit('local_roster', name(self.peer_id), members=[name(p) for p in group_info.peer_ids],
             bandwidths=[g[0] for g in decoded], modes=[g[1] for g in decoded],
             native_group_id=group_info.group_id.hex(), gathered_nodes=gathered_names(group_info.gathered))
        return await aggregate(self, group_info, *args, **kwargs)
    DecentralizedAverager._aggregate_with_group = local_aggregate

    init = AllReduceRunner.__init__
    def observed_init(self, *args, **kwargs):
        init(self, *args, **kwargs)
        container = self.tensor_part_container
        container._probe_owner = name(self.peer_id)
        container._probe_peers = [name(p) for p in self.ordered_peer_ids]
        container._probe_failed = []
        container._probe_group = self.group_id.hex()
        base = kwargs['tensors'][0]
        ranges = {}
        for peer, parts in zip(self.ordered_peer_ids, container._input_parts_by_peer):
            ranges[name(peer)] = [[(part.data_ptr()-base.data_ptr())//4,
                                  (part.data_ptr()-base.data_ptr())//4+part.numel()] for part, _ in parts]
        emit('allocation', name(self.peer_id), members=[name(p) for p in self.ordered_peer_ids],
             allocations=list(self.peer_fractions), modes=[m.value for m in self.modes],
             weight=self.weight, compression=type(container.compression).__name__,
             parts={name(p): n for p, n in zip(self.ordered_peer_ids, container.num_parts_by_peer)},
             part_ranges=ranges, native_group_id=self.group_id.hex())
    AllReduceRunner.__init__ = observed_init

    rpc = AllReduceRunner.rpc_aggregate_part
    async def observed_rpc(self, stream, context):
        me, sender = name(self.peer_id), name(context.remote_id)
        stream_id = f'{self.group_id.hex()}:{sender}->{me}'
        fields = dict(native_group_id=self.group_id.hex(), stream_id=stream_id)
        async def inputs():
            index = 0
            async for message in stream:
                emit('input_chunk', me, sender=sender, receiver=me, part_index=index,
                     values=deserialize_torch_tensor(message.tensor_part).flatten().tolist(),
                     code=int(message.code), input_group_id=message.group_id.hex(),
                     weight=message.weight, compression=int(message.tensor_part.compression), **fields)
                index += 1
                yield message
        emit('stream_started', me, sender=sender, receiver=me, **fields)
        try:
            index = 0
            async for message in rpc(self, inputs(), context):
                if message.code == pb.AVERAGED_PART:
                    emit('result_chunk', me, sender=me, receiver=sender, part_index=index,
                         values=deserialize_torch_tensor(message.tensor_part).flatten().tolist(), **fields)
                    index += 1
                else:
                    emit('stream_code', me, receiver=sender, code=int(message.code), **fields)
                yield message
        except BaseException as exc:
            emit('stream_error', me, sender=sender, receiver=me, error=repr(exc), **fields)
            raise
        finally:
            emit('stream_closed', me, sender=sender, receiver=me, **fields)
    AllReduceRunner.rpc_aggregate_part = observed_rpc

    processed = TensorPartContainer.register_processed_part
    def observed_processed(self, peer_index, part_index, part):
        result = processed(self, peer_index, part_index, part)
        emit('processed_chunk', self._probe_owner, reducer=self._probe_peers[peer_index],
             part_index=part_index, values=part.flatten().tolist(), native_group_id=self._probe_group)
        return result
    TensorPartContainer.register_processed_part = observed_processed

    ban = AllReduceRunner._ban_sender
    async def observed_ban(self, peer_id):
        await ban(self, peer_id)
        emit('sender_banned', name(self.peer_id), sender=name(peer_id), native_group_id=self.group_id.hex())
    AllReduceRunner._ban_sender = observed_ban

    failed = TensorPartContainer.register_failed_reducer
    def observed_failed(self, peer_index):
        self._probe_failed.append(self._probe_peers[peer_index])
        emit('missing_part_fallback', self._probe_owner, reducer=self._probe_peers[peer_index],
             registered_parts=self._outputs_registered_by_peer[peer_index], native_group_id=self._probe_group)
        return failed(self, peer_index)
    TensorPartContainer.register_failed_reducer = observed_failed

    finalize = AllReduceRunner.finalize
    def observed_finalize(self, *args, **kwargs):
        try:
            return finalize(self, *args, **kwargs)
        finally:
            c = self.tensor_part_container
            emit('runner_finalized', name(self.peer_id), failed_size=c.failed_size,
                 failed_peer_ids=list(c._probe_failed),
                 banned_senders=[name(p) for p in self.banned_senders], native_group_id=self.group_id.hex())
    AllReduceRunner.finalize = observed_finalize


def run_condition(args):
    global EVENTS, START, IDENTITIES
    package, provenance = native_imports(args)
    import multiprocessing as mp
    import psutil
    import torch
    from hivemind import DHT
    from hivemind.averaging import DecentralizedAverager
    from hivemind.utils import get_dht_time
    from hivemind.averaging.control import AveragingStage
    path = args.case_path
    path.mkdir(exist_ok=False)
    condition = args.condition
    active = ['B', 'D', 'C'] if condition == 'control3' else list(NODES)
    config = dict(condition=condition, seed=args.seed, active=active, tensor_shape=[48,9],
                  chunk_bytes=144, mode='NODE', weight=1, bandwidth=None,
                  sender_timeout=5, reducer_timeout=10, allreduce_timeout=30, step_timeout=45,
                  target_group_size=4, min_group_size=3, caching='native defaults',
                  artificial_delay=False, allow_retries=False,
                  cache_defaults=provenance['cache_defaults'],
                  prefix='announce_'+uuid.uuid4().hex+'_'+condition,
                  fixture='B-only canonical assembly order; native registration and key update',
                  request_timeout=3, min_matchmaking_time=5, condition_cap_seconds=120)
    write(path/'config.json', config)
    write(path/'provenance.json', provenance)
    with tarfile.open(path/'sources.tar.gz', 'w:gz') as archive:
        archive.add(Path(__file__), arcname='announcement_probe.py')
        for relative in provenance['hashes']:
            archive.add(package/relative, arcname='hivemind/'+relative)
    ctx = mp.get_context('fork')
    EVENTS, START = ctx.Queue(), time.monotonic()
    logs = {n:(path/f'events_{n}.jsonl').open('w') for n in NODES}
    controller = (path/'controller.jsonl').open('w')
    collected, initial, outputs, dhts, peers, controls, owned = [], {}, {}, [], {}, {}, {}
    error = None
    torch.set_num_threads(1)
    def drain():
        while True:
            try: event = EVENTS.get_nowait()
            except queue.Empty: break
            collected.append(event)
            stream = logs.get(event['node'], controller)
            stream.write(json.dumps(event)+'\n'); stream.flush()
    def sample():
        for proc in psutil.Process().children(recursive=True):
            try: owned[proc.pid] = proc.create_time()
            except psutil.NoSuchProcess: pass
    try:
        for n in NODES:
            dhts.append(DHT(initial_peers=dhts[0].get_visible_maddrs() if dhts else None,
                           host_maddrs=['/ip4/127.0.0.1/tcp/0'],
                           auto_nat=False, nat_port_map=False, use_relay=False, start=True))
            sample()
            addresses = [str(a) for a in dhts[-1].get_visible_maddrs()]
            if not addresses or any(not a.startswith('/ip4/127.0.0.1/tcp/') for a in addresses):
                raise RuntimeError(f'Non-loopback listener: {addresses}')
            emit('dht_started', n, addresses=addresses)
        IDENTITIES = {str(d.peer_id):n for n,d in zip(NODES,dhts)}
        write(path/'identities.json', {n:str(d.peer_id) for n,d in zip(NODES,dhts)})
        install_hooks(condition)
        for i,(n,dht) in enumerate(zip(NODES,dhts)):
            tensor = torch.zeros(48,9)
            tensor[:,0] = torch.arange(1,49)+100*i
            tensor[:,1+i] = 1
            tensor[:,5+i] = torch.arange(1,49)
            initial[n] = tensor.tolist()
            peers[n] = DecentralizedAverager([tensor], dht, prefix=config['prefix'],
                initial_group_bits='', bandwidth=None, target_group_size=4, min_group_size=3,
                min_matchmaking_time=5, request_timeout=3, allreduce_timeout=30,
                sender_timeout=5, reducer_timeout=10, part_size_bytes=144,
                averaging_alpha=1, allow_state_sharing=False, client_mode=False,
                shutdown_timeout=3, start=True)
            sample()
        sample()
        anchor = get_dht_time()+10
        emit('step_started','B',scheduled_time=anchor)
        controls['B'] = peers['B'].step(gather={'node':'B'}, wait=False, scheduled_time=anchor,
                                       timeout=45, allow_retries=False, weight=1)
        advertisement_deadline = time.monotonic()+5
        while not any(e['kind']=='leader_advertised' for e in collected):
            drain(); sample()
            if time.monotonic()>advertisement_deadline: raise RuntimeError('B advertisement not observed')
            time.sleep(.01)
        offsets = {n:random.Random(f'{args.seed}:{n}').uniform(0,.25) for n in active if n!='B'}
        config['readiness_offsets'] = offsets
        config['B_scheduled_time'] = anchor
        write(path/'config.json', config)
        launch = time.monotonic()
        pending = dict(offsets)
        cutoff = launch+78
        while len(outputs)<len(active):
            drain(); sample()
            now = time.monotonic()
            if now>cutoff: raise TimeoutError('Native drain deadline exceeded')
            for n,offset in list(pending.items()):
                if now>=launch+offset:
                    emit('step_started',n,scheduled_time=anchor+5,readiness_offset=offset)
                    controls[n] = peers[n].step(gather={'node':n}, wait=False, scheduled_time=anchor+5,
                                                timeout=45, allow_retries=False, weight=1)
                    del pending[n]
            for n,c in list(controls.items()):
                if n in outputs or not c.done() or c.stage != AveragingStage.FINISHED: continue
                try:
                    value = c.result()
                    status, exc = ('success',None) if isinstance(value,dict) else ('error',repr(value))
                except BaseException as exception:
                    status,exc='error',repr(exception)
                with peers[n].get_tensors() as tensors: final=tensors[0].tolist()
                drain()
                finals=[e for e in collected if e['kind']=='runner_finalized' and e['node']==n]
                stats={k:finals[-1][k] for k in ('failed_size','failed_peer_ids','banned_senders')} if finals else {}
                outputs[n]=dict(initial=initial[n],final=final,status=status,error=exc,
                                elapsed_seconds=now-launch,**stats)
                emit('averaging_terminal',n,**{k:v for k,v in outputs[n].items() if k not in ('initial','final')})
            time.sleep(.01)
        if condition=='control3':
            outputs['A']=dict(initial=initial['A'],final=initial['A'],status='idle',error=None,elapsed_seconds=0)
        write(path/'outputs.json',outputs)
    except BaseException as exc:
        error=repr(exc)
        emit('infrastructure_error',error=error,traceback=traceback.format_exc())
    finally:
        sample()
        if initial:
            for n in initial:
                if n not in outputs:
                    outputs[n] = dict(initial=initial[n], final=initial[n], status='error', error=error)
            write(path/'outputs.json', outputs)
        for peer in reversed(list(peers.values())):
            try: peer.shutdown()
            except BaseException as exc: emit('cleanup_error',error=repr(exc))
        for dht in reversed(dhts):
            try:dht.shutdown()
            except BaseException as exc:emit('cleanup_error',error=repr(exc))
        def live():
            result=[]
            for pid,created in owned.items():
                try:
                    p=psutil.Process(pid)
                    if p.create_time()==created and p.status()!=psutil.STATUS_ZOMBIE:result.append(p)
                except psutil.NoSuchProcess:pass
            return result
        until=time.monotonic()+5
        while live() and time.monotonic()<until:time.sleep(.1)
        # PyTorch's shared-memory manager must outlive its owner. The outer
        # controller audits it after this interpreter exits; killing it here
        # can abort the owner at interpreter teardown.
        def helper(p):
            try:return 'torch_shm_manag' in p.name()
            except psutil.NoSuchProcess:return False
        exit_pending=[p.pid for p in live() if helper(p)]
        targets=[p for p in live() if not helper(p)]
        forced=[p.pid for p in targets]
        for p in targets:p.terminate()
        _,remaining=psutil.wait_procs(targets,timeout=2)
        for p in remaining:p.kill()
        psutil.wait_procs(remaining,timeout=2)
        drain()
        write(path/'shutdown.json',dict(safe=not live(),surviving_pids=[p.pid for p in live()],
             forced_cleanup=forced,exit_pending_helpers=exit_pending,error=error,owned=owned,elapsed_seconds=time.monotonic()-START))
        for stream in logs.values():stream.close()
        EVENTS.close()
        EVENTS.join_thread()
        controller.close()
    if error:raise RuntimeError(error)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--label')
    parser.add_argument('--seed',type=int,default=1)
    parser.add_argument('--package-root',type=Path,default=ROOT/'research/simulator/.native-runtime/root_snapshot_a')
    parser.add_argument('--dependencies',type=Path,default=ROOT/'research/agora/.runtime/venv/lib/python3.11/site-packages')
    parser.add_argument('--output-root',type=Path,default=ROOT/'research/hivemind/results')
    parser.add_argument('--condition',choices=CONDITIONS)
    parser.add_argument('--case-path',type=Path)
    parser.add_argument('--preflight',action='store_true')
    args=parser.parse_args()
    if args.preflight:
        _,info=native_imports(args);print(json.dumps(info,indent=2));return
    if args.condition:
        exit_code=0
        try:
            run_condition(args)
        except BaseException:
            traceback.print_exc()
            exit_code=1
        # run_condition's finally already performed graceful peer/DHT shutdown and
        # flushed every artifact (outputs.json, events_*.jsonl, shutdown.json). The
        # native p2pd/torch static destructors can still abort the interpreter with a
        # broken-pipe std::system_error at teardown, which would be a nonzero exit the
        # controller misreads as an infrastructure failure. Exit now via os._exit to
        # skip those destructors. A genuine mid-run crash never reaches this point, so
        # real failures still surface as a nonzero exit, a timeout, or error==non-null.
        sys.stdout.flush();sys.stderr.flush()
        os._exit(exit_code)
    if not args.label or Path(args.label).name!=args.label:parser.error('A safe unique --label is required')
    suite=args.output_root/args.label
    suite.mkdir(exist_ok=False)
    from analyze_announcement_probe import analyze_condition,analyze_suite
    status={'status':'running','completed':[],'seed':args.seed}
    write(suite/'status.json',status)
    frozen = suite/'harness'
    frozen.mkdir()
    for filename in ('announcement_probe.py','analyze_announcement_probe.py','test_announcement_probe.py'):
        source = ROOT/'research/hivemind'/filename
        (frozen/filename).write_bytes(source.read_bytes())
    write(suite/'harness_hashes.json', {p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in frozen.iterdir()})
    for condition in CONDITIONS:
        command=[sys.executable,str(Path(__file__).resolve()),'--condition',condition,'--case-path',str((suite/condition).resolve()),
                 '--package-root',str(args.package_root.resolve()),'--dependencies',str(args.dependencies.resolve()),'--seed',str(args.seed)]
        print('RUN',condition,flush=True)
        with (suite/f'{condition}.log').open('w') as log:
            process=subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT,start_new_session=True,cwd='/tmp')
            try:code=process.wait(timeout=120)
            except BaseException:
                os.killpg(process.pid,signal.SIGTERM)
                try:process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid,signal.SIGKILL);process.wait(timeout=5)
                status.update(status='interrupted',failed=condition);write(suite/'status.json',status);raise
        # Final process audit belongs outside the native interpreter, so its
        # legitimate shared-memory helper has an opportunity to exit normally.
        sys.path.insert(0,str(args.dependencies.resolve()))
        import psutil
        shutdown_path=suite/condition/'shutdown.json'
        if shutdown_path.exists():
            shutdown=json.loads(shutdown_path.read_text())
            def survivors():
                found=[]
                for pid,created in shutdown['owned'].items():
                    try:
                        proc=psutil.Process(int(pid))
                        if proc.create_time()==created and proc.status()!=psutil.STATUS_ZOMBIE:found.append(proc)
                    except psutil.NoSuchProcess:pass
                return found
            audit_until=time.monotonic()+15
            while survivors() and time.monotonic()<audit_until:time.sleep(.1)
            for proc in survivors():
                shutdown['forced_cleanup'].append(proc.pid);proc.terminate()
            _,remaining=psutil.wait_procs(survivors(),timeout=2)
            for proc in remaining:proc.kill()
            psutil.wait_procs(remaining,timeout=2)
            shutdown['worker_closing_survivors']=shutdown['surviving_pids']
            shutdown['surviving_pids']=[p.pid for p in survivors()]
            shutdown['safe']=not shutdown['surviving_pids']
            shutdown['controller_audited_after_worker_exit']=True
            shutdown['worker_exitcode']=code
            write(shutdown_path,shutdown)
        if code:
            status.update(status='infrastructure_error',failed=condition);write(suite/'status.json',status)
            raise RuntimeError(f'{condition}: subprocess exit {code}; see log')
        analysis=analyze_condition(suite/condition)
        write(suite/condition/'analysis.json',analysis)
        status['completed'].append(condition)
        print('ANALYSIS',condition,json.dumps(analysis)[:700],flush=True)
        gate=analysis['valid_evidence']
        if condition in ('baseline4','control3'):
            gate=gate and all(v['status']=='success' for n,v in json.loads((suite/condition/'outputs.json').read_text()).items() if n!='A' or condition!='control3')
            gate=gate and analysis['control_mean_check']['active_outputs_match']
        if not gate:
            status.update(status='review_required',failed=condition);write(suite/'status.json',status)
            analyze_suite(suite);return
        write(suite/'status.json',status)
    status['status']='completed';write(suite/'status.json',status)
    analyze_suite(suite)
    print('REPORT',suite/'REPORT.md',flush=True)


if __name__=='__main__':main()
