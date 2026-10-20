"""Private eight-peer workload using unmodified pinned Agora protocol classes.

The parent launcher imports this only inside the isolated, frozen runtime.
"""
import asyncio
from functools import partial, wraps
import json
import logging
import multiprocessing as mp
import os
from pathlib import Path
import re
import time
import traceback
from typing import AsyncIterator

import psutil
import torch
from agora_server.core.all_reduce.ar_runner import AllReduceRunner, AveragingMode
from agora_server.core.all_reduce.matchmaking import Matchmaking
from agora_server.core.averaging.state_averager_sparta import PartitionedIndexSelector
from agora_server.core.server.dht_handler import declare_experts
from hivemind import DHT
from hivemind.averaging.control import StepControl
from hivemind.dht.node import DHTNode
from hivemind.dht.protocol import DHTProtocol
from hivemind.p2p import P2PContext, PeerID, ServicerBase
from hivemind.proto import averaging_pb2
from hivemind.utils import get_dht_time, MPFuture
from hivemind.utils.serializer import MSGPackSerializer

NAMES = [f'R{i}' for i in range(4)] + [f'C{i}' for i in range(4)]
NETWORK = None
NATIVE_RPC = {}
SERIALIZER = MSGPackSerializer


def clock_sample():
    return {'monotonic': time.monotonic(), 'wall': time.time(),
            'boottime': time.clock_gettime(time.CLOCK_BOOTTIME)}


def clock_discontinuous(before, after, threshold=.5):
    mono = after['monotonic']-before['monotonic']
    return (abs((after['wall']-before['wall'])-mono) > threshold or
            abs((after['boottime']-before['boottime'])-mono) > threshold)


def append(path, name, kind, **fields):
    event = dict(kind=kind, peer=name, pid=os.getpid(), **clock_sample(), **fields)
    # One append syscall per record; DHT and worker use separate trace files.
    raw = (json.dumps(event, allow_nan=False, separators=(',', ':'))+'\n').encode()
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        os.write(fd, raw)
    finally:
        os.close(fd)


def leg_delay(source, destination, mapping):
    if source == destination:
        return 0.0
    return (mapping[source]['rtt_ms']+mapping[destination]['rtt_ms'])/2000.0


def install_dht_hooks():
    """Install observers before DHT handlers are registered; activate later."""
    for rpc in ['ping', 'find', 'store']:
        method = 'rpc_'+rpc
        original = getattr(DHTProtocol, method)
        NATIVE_RPC[method] = original

        def make_handler(native, rpc_name):
            @wraps(native)
            async def handler(self, request, context):
                profile = NETWORK  # Do not change an in-flight bootstrap call.
                if profile is None:
                    return await native(self, request, context)
                remote, local = str(context.remote_id), str(self.p2p.peer_id)
                mapping = profile['mapping']
                if remote not in mapping or local not in mapping:
                    raise RuntimeError('Unexpected peer on the private DHT')
                delay = leg_delay(remote, local, mapping) if profile['enabled'] else 0.0
                fields = dict(rpc=rpc_name, source=remote, destination=local,
                              injected_leg_seconds=delay,
                              key_hashes=[bytes(k).hex() for k in getattr(request, 'keys', [])])
                if rpc_name == 'store':
                    fields.update(expirations=list(request.expiration_time),
                                  values_hex=[bytes(v).hex() for v in request.values],
                                  subkeys_hex=[bytes(k).hex() for k in request.subkeys],
                                  in_cache=list(request.in_cache))
                begin = time.monotonic()
                append(profile['path'], profile['name'], 'rpc_start', **fields)
                try:
                    await asyncio.sleep(delay)
                    result = await native(self, request, context)
                    await asyncio.sleep(delay)
                    append(profile['path'], profile['name'], 'rpc_end', duration_s=time.monotonic()-begin,
                           store_ok=list(getattr(result, 'store_ok', [])), **fields)
                    return result
                except BaseException as exc:
                    append(profile['path'], profile['name'], 'rpc_error', error=repr(exc), **fields)
                    raise
            handler.__name__ = 'rpc_'+rpc_name
            return handler
        setattr(DHTProtocol, method, make_handler(original, rpc))

    for method in ['get_many', 'store_many']:
        native = getattr(DHTNode, method)

        def make_operation(original, operation):
            async def wrapped(self, *args, **kwargs):
                profile = NETWORK
                begin = time.monotonic()
                try:
                    result = await original(self, *args, **kwargs)
                except BaseException as exc:
                    if profile:
                        append(profile['path'], profile['name'], 'dht_operation', operation=operation,
                               duration_s=time.monotonic()-begin, status='error', error=repr(exc))
                    raise
                if profile:
                    append(profile['path'], profile['name'], 'dht_operation', operation=operation,
                           duration_s=time.monotonic()-begin, status='returned')
                return result
            return wrapped
        setattr(DHTNode, method, make_operation(native, method))


async def configure_dht(dht, node, *, mapping, path, name, enabled):
    global NETWORK
    NETWORK = dict(mapping=mapping, path=path, name=name, enabled=enabled)
    append(path, name, 'dht_profile', node_id=str(node.node_id), num_replicas=node.num_replicas)
    return dict(node_id=str(node.node_id), num_replicas=node.num_replicas, pid=os.getpid())


class NativeGroupLog(logging.Handler):
    def __init__(self, owner):
        super().__init__()
        self.owner = owner

    def emit(self, record):
        match = re.search(r'Formed group with (\d+) peers out of (\d+) in ([\d.]+) secs', record.getMessage())
        if match:
            self.owner.native_roster_s = float(match[3])
            self.owner.emit('native_roster_log', group_size=int(match[1]),
                            expected_peers=int(match[2]), seconds=float(match[3]),
                            round=self.owner.round)


class ToyService(ServicerBase):
    """Dispatch native aggregation RPCs; never edit or reconcile a roster."""
    def __init__(self, owner):
        self.owner = owner
        self.runner = None
        self.ready = asyncio.Event()

    async def rpc_aggregate_part(
        self, stream: AsyncIterator[averaging_pb2.AveragingData], context: P2PContext
    ) -> AsyncIterator[averaging_pb2.AveragingData]:
        source, destination = str(context.remote_id), self.owner.peer_id
        delay = leg_delay(source, destination, self.owner.mapping) if self.owner.config['network_delay_enabled'] else 0.0
        # The first request includes the first tensor part. Later chunks get
        # their own simulated leg; tiny scalar tensors still use real streams.
        async def delayed_inputs():
            async for message in stream:
                await asyncio.sleep(delay)
                self.owner.emit('tensor_received', source=source, round=self.owner.round,
                                group_id=message.group_id.hex(), injected_leg_seconds=delay,
                                raw_hex=message.SerializeToString().hex())
                yield message

        try:
            await asyncio.wait_for(self.ready.wait(), timeout=self.owner.config['averaging_timeout'])
            runner = self.runner
            if runner is None:
                raise RuntimeError('Native runner was not registered')
            async for response in runner.rpc_aggregate_part(delayed_inputs(), context):
                await asyncio.sleep(delay)
                self.owner.emit('tensor_response', destination=source, round=self.owner.round,
                                code=response.code, injected_leg_seconds=delay,
                                raw_hex=response.SerializeToString().hex())
                yield response
            self.owner.emit('tensor_stream_closed', source=source, round=self.owner.round)
        except BaseException as exc:
            self.owner.emit('tensor_rpc_error', source=source, round=self.owner.round, error=repr(exc))
            raise


class Worker:
    def __init__(self, config, index, trial, connection, initial_peers):
        self.config, self.index = config, index
        self.name, self.role = NAMES[index], 'reducer' if index < 4 else 'contributor'
        self.trial, self.conn = Path(trial), connection
        self.path = self.trial/f'events_{self.name}.jsonl'
        self.dht_path = str(self.trial/f'dht_{self.name}.jsonl')
        self.initial_peers = initial_peers
        self.mapping, self.round, self.native_roster_s = {}, 0, None
        self.dht = self.p2p = None
        self.theta = torch.nn.Parameter(torch.zeros(100))
        self.generator = torch.Generator().manual_seed(config['seed']*1000+index+1)
        truth = torch.Generator().manual_seed(config['seed']*1000+500)
        self.target = torch.randn(100, generator=truth)*.1
        validation = torch.Generator().manual_seed(config['seed']*1000+501)
        self.validation_x = torch.randn(256, 100, generator=validation)
        self.validation_y = self.validation_x@self.target
        self.optimizer = torch.optim.AdamW([self.theta], lr=.01, betas=(.9, .95), weight_decay=0)
        # Shared native partition table; dedicated generators keep examples independent.
        torch.manual_seed(config['seed']*1000+502)
        self.selector = PartitionedIndexSelector(.05, self.theta)
        self.step = 0
        self.service = ToyService(self)

    def emit(self, kind, **fields):
        append(self.path, self.name, kind, **fields)

    def loss(self):
        with torch.no_grad():
            return float(((self.validation_x@self.theta-self.validation_y)**2).mean())

    def train_step(self):
        x = torch.randn(32, 100, generator=self.generator)
        y = x@self.target+torch.randn(32, generator=self.generator)*.01
        self.optimizer.zero_grad()
        loss = ((x@self.theta-y)**2).mean()
        loss.backward()
        self.optimizer.step()
        self.step += 1
        self.emit('training_step', step=self.step, batch_loss=float(loss.detach()), heldout_loss=self.loss())

    async def announce(self):
        uid = f"{self.config['stage_base']}.0.{self.index}"
        result = declare_experts(self.dht, [uid], expiration_time=get_dht_time()+60, wait=False)
        await result
        self.emit('stage_announcement', uid=uid, lifetime_s=60)

    async def periodic_announcements(self):
        while True:
            await asyncio.sleep(30)
            await self.announce()

    async def average(self, round_number, epoch):
        self.round, self.native_roster_s = round_number, None
        ready = time.monotonic()
        indices = self.selector.get_indices(self.theta, round_number).nonzero().flatten().tolist()
        assert len(indices) == 5
        snapshot = self.theta.detach()[indices].clone()
        tensors = [x.clone() for x in snapshot.unbind()]
        self.emit('round_snapshot', round=round_number, epoch=epoch, indices=indices,
                  values=snapshot.tolist(), weight=1, train_step=self.step)
        gather = SERIALIZER.dumps(dict(name=self.name, role=self.role, indices=indices, epoch=epoch, weight=1))
        control = StepControl(scheduled_time=get_dht_time()+40, deadline=float('inf'),
                              allow_retries=False, weight=1, data_for_gather=gather)
        control.attach(MPFuture(), MPFuture())
        control.allow_allreduce()
        gm = self.matchmaking.group_key_manager
        original_join, original_get = gm.join_group, gm.get_averagers
        published = None

        async def observed_join(expiration_time, global_step, data_for_gather):
            nonlocal published
            begin = time.monotonic()
            self.emit('advertisement_start', round=round_number, epoch=epoch,
                      key=gm.key_for_step(global_step), expiration=expiration_time)
            result = await original_join(expiration_time, global_step, data_for_gather)
            published = time.monotonic()
            self.emit('advertisement_end', round=round_number, epoch=epoch,
                      accepted=bool(result), duration_s=published-begin, expiration=expiration_time)
            return result

        async def observed_get(global_step, only_active=True):
            begin = time.monotonic()
            peers = await original_get(global_step, only_active=only_active)
            self.emit('ad_read', round=round_number, epoch=epoch, duration_s=time.monotonic()-begin,
                      key=gm.key_for_step(global_step),
                      advertisements=[dict(peer_id=str(p), expiration=e) for p, _, e in peers])
            return peers

        gm.join_group, gm.get_averagers = observed_join, observed_get
        runner = None
        try:
            group = await asyncio.wait_for(self.matchmaking.look_for_group(control, global_step=epoch), timeout=190)
            if group is None:
                raise RuntimeError('Native matchmaking returned no roster')
            roster = [str(p) for p in group.peer_ids]
            formed = time.monotonic()
            self.emit('roster', round=round_number, epoch=epoch, members=roster,
                      native_group_id=group.group_id.hex(), ready_to_roster_s=formed-ready,
                      native_roster_s=self.native_roster_s if self.native_roster_s is not None else formed-published)
            metadata = [SERIALIZER.loads(v) for v in group.gathered]
            if any(m['indices'] != indices or m['epoch'] != epoch for m in metadata):
                raise RuntimeError('Native roster contains incompatible sparse partition metadata')
            modes = tuple(AveragingMode.NODE if m['role']=='reducer' else AveragingMode.CLIENT for m in metadata)
            reducers = sum(m['role']=='reducer' for m in metadata)
            fractions = tuple(1/reducers if m['role']=='reducer' else 0 for m in metadata)
            runner = AllReduceRunner(p2p=self.p2p, servicer_type=ToyService,
                                    prefix=self.config['prefix'], group_id=group.group_id,
                                    tensors=tensors, ordered_peer_ids=group.peer_ids,
                                    peer_fractions=fractions, modes=modes, weight=1,
                                    sender_timeout=30, reducer_timeout=60, part_size_bytes=2097152)
            self.emit('reducer_assignment', round=round_number, epoch=epoch, members=roster,
                      fractions=fractions, parts=runner.tensor_part_container.num_parts_by_peer)
            self.service.runner = runner
            self.service.ready.set()

            async def collect():
                return [float(delta) async for delta in runner]
            deltas = await asyncio.wait_for(collect(), timeout=150)
            if len(deltas) != 5:
                raise RuntimeError(f'Expected five corrections, received {len(deltas)}')
            before = self.theta.detach()[indices].clone()
            with torch.no_grad():
                self.theta[indices] += torch.tensor(deltas)
            self.emit('round_result', round=round_number, epoch=epoch, indices=indices,
                      delta=deltas, before_apply=before.tolist(), after_apply=self.theta.detach()[indices].tolist(),
                      failed_size=runner.tensor_part_container.failed_size,
                      failed_peer_ids={str(k):v for k,v in runner.tensor_part_container.failed_peer_ids.items()},
                      failure_reasons={str(k):v.value for k,v in runner.tensor_part_container.failure_reasons.items()},
                      averaging_elapsed_s=time.monotonic()-formed,
                      roster=roster, status='completed')
        except Exception as exc:
            self.emit('round_result', round=round_number, epoch=epoch, indices=indices,
                      status='failed', error=repr(exc), traceback=traceback.format_exc())
        finally:
            if runner is not None:
                runner.finalize()
            self.service.ready.clear()
            self.service.runner = None
            gm.join_group, gm.get_averagers = original_join, original_get

    async def run(self):
        logger = logging.getLogger('agora_server.core.all_reduce.matchmaking')
        handler = NativeGroupLog(self)
        logger.addHandler(handler)
        announcer = averaging = None
        clean = False
        try:
            self.dht = DHT(initial_peers=self.initial_peers, start=True, startup_timeout=45,
                           host_maddrs=['/ip4/127.0.0.1/tcp/0'], use_ipfs=False, num_replicas=5)
            self.peer_id = str(self.dht.peer_id)
            maddrs = [str(a) for a in self.dht.get_visible_maddrs()]
            if any('/ip4/127.0.0.1/' not in m for m in maddrs):
                raise RuntimeError('Private DHT exposed a non-loopback address')
            self.emit('identity', peer_id=self.peer_id, role=self.role, dht_pid=self.dht.pid,
                      child_pids=[p.pid for p in psutil.Process().children(recursive=True)])
            self.conn.send(dict(kind='boot', name=self.name, peer_id=self.peer_id, maddrs=maddrs))
            while not self.conn.poll():
                await asyncio.sleep(.02)
            config = self.conn.recv()
            self.mapping = config['mapping']
            identity = self.dht.run_coroutine(partial(configure_dht, mapping=self.mapping,
                                                      path=self.dht_path, name=self.name,
                                                      enabled=self.config['network_delay_enabled']))
            self.emit('runtime_dht', dht_info=identity)
            self.p2p = await self.dht.replicate_p2p()
            await self.service.add_p2p_handlers(self.p2p, namespace=self.config['prefix'])
            self.matchmaking = Matchmaking(self.dht, self.config['prefix'], min_matchmaking_time=40, check_interval=1)
            await self.announce()
            announcer = asyncio.create_task(self.periodic_announcements())
            warmup_end = time.monotonic()+60
            while self.matchmaking.get_max_peers() != 8:
                if time.monotonic() >= warmup_end:
                    raise RuntimeError('Eight-member stage presence did not become visible')
                await asyncio.sleep(.1)
            self.conn.send(dict(kind='ready', name=self.name))
            while not self.conn.poll():
                await asyncio.sleep(.02)
            start = self.conn.recv()['start_monotonic']
            last_clock = clock_sample()
            if self.config['phase']=='smoke':
                with torch.no_grad():
                    self.theta.fill_(self.index)
            self.emit('training_initial', loss=self.loss())
            if self.config['phase']=='smoke':
                await self.average(1, 20)
            else:
                for step in range(1, 101):
                    due = start+self.config['initial_offsets'][self.name]+2*step
                    await asyncio.sleep(max(0, due-time.monotonic()))
                    current = clock_sample()
                    self.emit('clock_sample')
                    if clock_discontinuous(last_clock, current):
                        self.emit('clock_discontinuity')
                        raise RuntimeError('Suspend or wall-clock discontinuity detected')
                    last_clock = current
                    self.train_step()
                    if step % 20 == 0:
                        if averaging is not None and not averaging.done():
                            self.emit('round_skipped', round=step//20, epoch=step)
                        else:
                            if averaging is not None:
                                await averaging
                            averaging = asyncio.create_task(self.average(step//20, step))
                if averaging:
                    await averaging
            self.emit('training_final', loss=self.loss(), step=self.step,
                      finite=bool(torch.isfinite(self.theta).all()))
            self.conn.send(dict(kind='finished', name=self.name))
            # Keep serving until every peer has finished. This is only a
            # teardown barrier, not a per-round roster/averaging barrier.
            while not self.conn.poll():
                await asyncio.sleep(.05)
            self.conn.recv()
            clean = True
        except BaseException as exc:
            self.emit('worker_error', error=repr(exc), traceback=traceback.format_exc())
            try:
                self.conn.send(dict(kind='error', name=self.name, error=repr(exc)))
            except OSError:
                pass
        finally:
            if averaging is not None and not averaging.done():
                averaging.cancel()
                await asyncio.gather(averaging, return_exceptions=True)
            if announcer:
                announcer.cancel()
                await asyncio.gather(announcer, return_exceptions=True)
            if self.p2p:
                await self.p2p.shutdown()
            if self.dht:
                self.dht.shutdown()
            self.emit('trace_closed', clean=clean)
            logger.removeHandler(handler)


def worker_entry(config, index, trial, connection, initial_peers):
    # Supported Linux native DHT processes inherit pre-registration observers.
    mp.set_start_method('fork', force=True)
    torch.set_num_threads(1)
    torch.use_deterministic_algorithms(True)
    log = open(Path(trial)/f'worker_{NAMES[index]}.log', 'a', buffering=1)
    os.dup2(log.fileno(), 1)
    os.dup2(log.fileno(), 2)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s %(levelname)s %(message)s')
    logging.getLogger('agora_server.core.all_reduce.ar_runner').setLevel(logging.DEBUG)
    install_dht_hooks()
    worker = Worker(config, index, trial, connection, initial_peers)
    asyncio.run(worker.run())
