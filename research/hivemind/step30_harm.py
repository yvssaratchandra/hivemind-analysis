"""Final two-policy, control-only unreliable-X comparison.

No core edits. Source is frozen before smoke. Resume/dry-run validates evidence
without loading Hivemind or opening sockets. STOP_AFTER_CURRENT is honored only
after complete trial teardown.
"""
from __future__ import annotations

import argparse
import asyncio
from collections import defaultdict
from datetime import datetime, timezone
import fcntl
import functools
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import time

import step28_confirmation as ancestor

ROOT = Path(__file__).resolve().parent
EXTRA_FILES = ('step30_harm.py', 'step30_workload.py', 'analyze_step30_harm.py',
               'verify_step30_harm.py', 'test_step30_harm.py', 'analyze_step29_policies.py')
FROZEN_FILES = ancestor.FROZEN_FILES + EXTRA_FILES
EVIDENCE_FILES = ('config.json', 'events.jsonl', 'summary.json', 'analysis.json',
                  'control_analysis.json', 'raw_verification.json')
PROFILE = dict(distribution='uniform', lower=.1, upper=.2)
DECISION = dict(wait_increase_seconds=.5, failure_increase_fraction=.05,
                group_rate_decrease_fraction=.15, minimum_threshold_seeds=2,
                require_third_same_direction=True, minimum_late_windows=3,
                minimum_persistent_seeds=2, honest_starts_per_trial=100,
                x_client_acceptances_per_trial=20, x_multi_publication_searches=10,
                maximum_renewal_to_stock_median_interval=.75)
TRAINING = ancestor.TRAINING
sha256, atomic_json, read_json = ancestor.sha256, ancestor.atomic_json, ancestor.read_json
check_clock = ancestor.check_clock
OBS = None
NETWORK = None
RPC_NUMBER = 0
RPC_COUNTS = defaultdict(int)
RPC_INFO = {}


def make_cases():
    cases = []
    for name, policy in (('stock_2s', 'native'), ('renew_1_2', 'renew')):
        case = dict(name=name, mode='passive', behavior='recruit_only',
                    publication_policy=policy, lifetime=2., minimum=2, target=4,
                    response='unreliable', hold_seconds=.5, one_way_delay_seconds=.15)
        if policy == 'renew':
            case.update(frequency=1., nominal_lower=1., nominal_upper=2.)
        cases.append(case)
    return cases


def make_jobs(phase):
    cases = make_cases()
    if phase == 'smoke':
        return [dict(case=case, seed=3000, duration=20.) for case in cases]
    if phase != 'confirm':
        raise ValueError('phase must be smoke or confirm')
    return [dict(case=case, seed=seed, duration=300.)
            for seed in (3001, 3002, 3003)
            for case in (list(reversed(cases)) if seed == 3002 else cases)]


def deterministic_delay(seed, source, destination, rpc_type, leg, occurrence, response_index=0):
    payload = json.dumps([int(seed), source, destination, rpc_type, leg,
                          int(occurrence), int(response_index)], separators=(',', ':'))
    uniform = int.from_bytes(hashlib.sha256(payload.encode()).digest()[:8], 'big') / 2**64
    return PROFILE['lower'] + (PROFILE['upper']-PROFILE['lower'])*uniform


def native_expiration(now, scheduled, minimum, search_end):
    return float(min(max(scheduled, now+minimum), search_end))


def renewal_sleep(expiration, now, write_elapsed, period=1.):
    return max(0., min(expiration-now, period-write_elapsed))


def configure_network(seed, identities):
    global NETWORK, RPC_COUNTS
    NETWORK = dict(seed=int(seed), names={str(peer): name for name, peer in identities.items()})
    # Bootstrap RPCs may still be finishing. Preserve their zero-delay context;
    # their subsequent legs must not acquire the measured profile mid-request.
    RPC_COUNTS = defaultdict(int)
    if OBS is not None:
        OBS.emit('network_config_installed', network_seed=seed, identities=identities,
                 delay_profile=PROFILE)


async def configure_dht_network(dht, node, *, seed, identities):
    configure_network(seed, identities)
    return dict(pid=os.getpid(), network_seed=seed, configured=True)


def rpc_start(self, request, context, rpc_type):
    global RPC_NUMBER
    RPC_NUMBER += 1
    rpc_id = f'{os.getpid()}:rpc{RPC_NUMBER}'
    peer = getattr(self, 'peer_id', None)
    if peer is None:
        peer = self.p2p.peer_id
    source = destination = None
    occurrence = 0
    if NETWORK is not None:
        source = NETWORK['names'][str(context.remote_id)]
        destination = NETWORK['names'][str(peer)]
        key = (source, destination, rpc_type)
        RPC_COUNTS[key] += 1
        occurrence = RPC_COUNTS[key]
    info = dict(measured=NETWORK is not None, source_name=source,
                destination_name=destination, rpc_type=rpc_type, occurrence=occurrence,
                network_seed=NETWORK['seed'] if NETWORK else None, response_index=0)
    RPC_INFO[rpc_id] = info
    request_detail = {}
    if rpc_type == 'join':
        try:
            caller = OBS.sweep.MSGPackSerializer.loads(
                OBS.sweep.MSGPackSerializer.loads(request.gather)[2])
            caller_step_id = caller.get('step_id') if isinstance(caller, dict) else None
        except (ValueError, TypeError, IndexError):
            caller_step_id = None
        request_detail = dict(request_expiration=request.expiration,
                              caller_step_id=caller_step_id,
                              request_group_key=request.group_key,
                              request_gather_sha256=hashlib.sha256(request.gather).hexdigest())
    OBS.emit('control_rpc_start', peer, context.remote_id, rpc_id=rpc_id,
             key_hashes=[bytes(key).hex() for key in getattr(request, 'keys', ())],
             delay_seconds=.15 if NETWORK else 0.,
             delay_seconds_semantics='profile mean; actual per-leg draws recorded separately',
             delay_profile=PROFILE if NETWORK else None, **info, **request_detail)
    return rpc_id, .15 if NETWORK else 0., peer


async def injected_sleep(rpc_id, leg, unused_delay):
    info = RPC_INFO[rpc_id]
    index = info['response_index'] if leg == 'response' else 0
    if leg == 'response':
        info['response_index'] += 1
    delay = (deterministic_delay(info['network_seed'], info['source_name'],
             info['destination_name'], info['rpc_type'], leg, info['occurrence'], index)
             if info['measured'] else 0.)
    started = time.monotonic()
    cancelled = False
    try:
        if delay:
            await asyncio.sleep(delay)
    except asyncio.CancelledError:
        cancelled = True
        raise
    finally:
        OBS.emit('control_rpc_delay', rpc_id=rpc_id, leg=leg, response_index=index,
                 requested_delay=delay, actual_delay=time.monotonic()-started,
                 cancelled=cancelled)


async def renewed_native_publisher(self, step, key_manager):
    base = OBS.base
    async with self.lock_declare:
        try:
            while True:
                await self.running.wait()
                expiration = native_expiration(base.get_dht_time(), step.scheduled_time,
                                               self.min_matchmaking_time, self.search_end_time)
                self.declared_group_key = key = key_manager.current_key
                self.declared_expiration_time = expiration
                self.declared_expiration.set()
                sent = time.monotonic()
                await key_manager.declare_averager(key, self.peer_id, expiration_time=expiration)
                await asyncio.sleep(renewal_sleep(expiration, base.get_dht_time(), time.monotonic()-sent))
                if self.running.is_set() and len(self.leader_queue) == 0:
                    await key_manager.update_key_on_not_enough_peers()
        finally:
            if self.declared_group_key is not None:
                key, expiration = self.declared_group_key, self.declared_expiration_time
                self.declared_group_key, self.declared_expiration_time = None, float('inf')
                self.leader_queue = type(self.leader_queue)()
                self.max_assured_time = float('-inf')
                await key_manager.declare_averager(key, self.peer_id, expiration, looking_for_group=False)


async def control_round_completion(self, group_info, **kwargs):
    """Complete metadata after native matchmaking has drained its followers.

    This hook runs only after look_for_group has returned. No averaging runner
    or tensor RPC is created. Mark the native registration placeholder finished
    so its normal context-manager cleanup does not report a failed allreduce.
    """
    members = {peer: self.serializer.loads(self.serializer.loads(data)[2])
               for peer, data in zip(group_info.peer_ids, group_info.gathered)}
    future = self._running_groups.get(group_info.group_id)
    if future is not None and not future.done():
        future.set_result(None)
    OBS.emit('control_round_complete', self.peer_id,
             step_id=OBS.sweep.user_data(self._matchmaking.step_control)['step_id']
             if self._matchmaking.step_control is not None else members[self.peer_id]['step_id'],
             group_id=group_info.group_id.hex(),
             members={str(peer): data for peer, data in members.items()}, hook='metadata_only')
    return members


def install_runtime():
    global OBS
    if OBS is not None:
        return OBS
    ancestor.rpc_start, ancestor.injected_sleep = rpc_start, injected_sleep
    OBS = ancestor.install_runtime()
    base = OBS.base
    original = base.ORIGINAL_DECLARE

    async def publisher(self, step, key_manager):
        policy = base.CASE['publication_policy'] if base.ATTACKER else 'native'
        if not getattr(self, '_step30_publisher_logged', False):
            self._step30_publisher_logged = True
            OBS.emit('runtime_publication_policy', self.peer_id,
                     publication_policy=policy, native_original=policy == 'native',
                     expiration_formula='native', period_seconds=1. if policy == 'renew' else None,
                     original_source=original.__code__.co_filename,
                     original_sha256=sha256(original.__code__.co_filename))
        if policy == 'renew':
            return await renewed_native_publisher(self, step, key_manager)
        return await original(self, step, key_manager)

    native_request = base.Matchmaking._request_join_potential_leaders

    async def runtime_config(self, step):
        if not getattr(self, '_step30_config_logged', False):
            self._step30_config_logged = True
            OBS.emit('runtime_matchmaking_config', self.peer_id,
                     min_matchmaking_time=self.potential_leaders.min_matchmaking_time,
                     min_group_size=self.min_group_size, target_group_size=self.target_group_size,
                     request_timeout=self.request_timeout,
                     behavior='recruit_only' if base.ATTACKER else 'normal',
                     publication_policy=base.CASE['publication_policy'] if base.ATTACKER else 'native',
                     response_policy='unreliable' if base.ATTACKER else 'native',
                     step_id=OBS.sweep.user_data(step)['step_id'])
        return await native_request(self, step)

    original_tensor_rpc = base.RefreshAverager.rpc_aggregate_part
    @functools.wraps(original_tensor_rpc)
    async def forbidden_tensor_rpc(self, stream, context):
        OBS.emit('forbidden_tensor_rpc', self.peer_id, context.remote_id)
        raise AssertionError('control-only trial received a tensor RPC')
        yield  # preserve async-generator registration shape
    from typing import get_type_hints
    forbidden_tensor_rpc.__annotations__ = get_type_hints(original_tensor_rpc)
    base.RefreshAverager.rpc_aggregate_part = forbidden_tensor_rpc
    base.RefreshAverager._aggregate_with_group = control_round_completion
    native_follower_assemble = base.Matchmaking.follower_assemble_group

    async def follower_assemble(self, leader, message):
        group = await native_follower_assemble(self, leader, message)
        user = OBS.sweep.user_data(self.step_control)
        members = {str(peer): OBS.sweep.MSGPackSerializer.loads(
                   OBS.sweep.MSGPackSerializer.loads(data)[2])
                   for peer, data in zip(group.peer_ids, group.gathered)}
        OBS.emit('follower_assembled', self.peer_id, leader, step_id=user['step_id'],
                 group_id=group.group_id.hex(), roster=[str(p) for p in group.peer_ids],
                 user=user, members=members)
        return group

    base.Matchmaking.follower_assemble_group = follower_assemble
    base.PotentialLeaders._declare_averager_periodically = publisher
    base.Matchmaking._request_join_potential_leaders = runtime_config
    OBS.sweep.write_json = atomic_json
    import step30_workload
    OBS.sweep.run_trial = step30_workload.run_trial
    return OBS


def snapshot(suite):
    install_runtime()
    ancestor.snapshot(suite)
    provenance = read_json(suite/'provenance.json')
    source = Path(provenance['source'])
    provenance['native_source_snapshot_sha256'] = {}
    for path in source.rglob('*'):
        if path.is_file() and path.suffix in ('.py', '.proto'):
            relative = str(path.relative_to(source))
            saved = suite/'native_source'/relative
            saved.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(path, saved)
            provenance['native_source_snapshot_sha256'][relative] = sha256(saved)
            repository = ROOT.parents[1]/'hivemind'/relative
            if not repository.is_file() and relative.startswith('proto/') and path.name.endswith('_pb2.py'):
                # The verified runtime contains generated protobuf modules;
                # the repository retains their schemas rather than generated Python.
                provenance['runtime_files_sha256'][str(path.resolve())] = sha256(path)
                continue
            if not repository.is_file() or sha256(path) != sha256(repository):
                raise ValueError(f'full native runtime source differs: {relative}')
            provenance['source_sha256'][relative] = sha256(path)
    for name in EXTRA_FILES:
        shutil.copyfile(ROOT/name, suite/'harness'/name)
        provenance['harness_sha256'][name] = sha256(ROOT/name)
    provenance.update(experiment='step30', entrypoint=str(Path(__file__).resolve()),
                      entrypoint_sha256=sha256(__file__), script_sha256=sha256(__file__),
                      observation_engine='native control plane; metadata-only completion; stable seeded RPC jitter')
    atomic_json(suite/'provenance.json', provenance)


def validate_sources(suite):
    provenance = read_json(Path(suite)/'provenance.json')
    native_snapshot = provenance.get('native_source_snapshot_sha256', {})
    required_native = {'averaging/matchmaking.py', 'averaging/averager.py',
                       'proto/averaging_pb2.py', 'proto/dht_pb2.py'}
    if provenance.get('experiment') != 'step30' or not required_native.issubset(native_snapshot):
        raise ValueError('missing frozen native source snapshot')
    if not set(FROZEN_FILES).issubset(provenance.get('harness_sha256', {})):
        raise ValueError('missing frozen source closure')
    ancestor.validate_sources(suite, provenance)
    for relative, digest in native_snapshot.items():
        if sha256(Path(suite)/'native_source'/relative) != digest:
            raise ValueError(f'frozen native snapshot changed: {relative}')
    if provenance.get('entrypoint_sha256') != sha256(__file__):
        raise ValueError('entrypoint changed after freeze')


def inspect_suite(suite):
    suite = Path(suite).resolve()
    manifest = read_json(suite/'suite.json')
    if (manifest.get('experiment') != 'step30' or manifest['jobs'] != make_jobs(manifest['phase'])
            or manifest.get('training_delay') != TRAINING
            or manifest.get('delay_profile') != PROFILE
            or manifest.get('decision_rule') != DECISION
            or manifest.get('outcome') != 'grouping' or manifest.get('control_only') is not True):
        raise ValueError('manifest differs from exact Step30 design')
    validate_sources(suite)
    rows = []
    for job in manifest['jobs']:
        path = suite/ancestor.trial_name(job)
        row = dict(job=job, trial_dir=path.name, status='pending')
        if path.exists():
            row['status'] = 'interrupted'
            if (path/'completion.json').exists():
                done = read_json(path/'completion.json')
                if done.get('all_peers_shutdown') is not True or set(done.get('file_sha256', {})) != set(EVIDENCE_FILES):
                    raise ValueError('incomplete completion evidence')
                for name, digest in done['file_sha256'].items():
                    if sha256(path/name) != digest:
                        raise ValueError(f'completed evidence changed: {path.name}/{name}')
                config = read_json(path/'config.json')
                if any(config.get(k) != job[k] for k in ('case', 'seed', 'duration')):
                    raise ValueError('completed config differs from manifest')
                ancestor.validate_trace_closed(path/'events.jsonl')
                if not read_json(path/'control_analysis.json')['integrity_pass'] or not read_json(path/'raw_verification.json')['pass']:
                    raise ValueError('completed trial failed verification')
                row['status'] = 'completed'
        rows.append(row)
    for archive_file in suite.glob('archive_*.json'):
        archive = read_json(archive_file)
        for name, digest in archive['file_sha256'].items():
            if sha256(Path(archive['path'])/name) != digest:
                raise ValueError('archived evidence changed')
    return dict(experiment='step30', suite=str(suite), jobs=rows,
                counts={s: sum(r['status'] == s for r in rows) for s in ('completed', 'pending', 'interrupted')}, valid=True)


def run_suite(suite):
    suite = Path(suite).resolve()
    with (suite/'.controller.lock').open('a+') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        state = inspect_suite(suite)
        atomic_json(suite/'checkpoint.json', state)
        if ancestor.stop_requested(suite):
            print('STOP_AFTER_CURRENT present; no peers started; safe to suspend', flush=True)
            return
        obs = install_runtime()
        from analyze_step30_harm import analyse_trial, export_suite
        from verify_step30_harm import verify_trial, verify_exports
        import multiprocessing as mp
        for index, row in enumerate(state['jobs'], 1):
            if row['status'] == 'completed':
                print(f'SKIP {index}/{len(state["jobs"])} {row["trial_dir"]}', flush=True)
                continue
            if ancestor.stop_requested(suite):
                break
            if row['status'] == 'interrupted':
                atomic_json(suite/f'archive_{row["trial_dir"]}.json', ancestor.archive_interrupted(suite, row['trial_dir']))
            job = row['job']; path = suite/row['trial_dir']
            validate_sources(suite)
            state.update(active_trial=path.name, controller_pid=os.getpid())
            atomic_json(suite/'checkpoint.json', state)
            print(f'START {index}/{len(state["jobs"])} {path.name}: {json.dumps(job["case"], sort_keys=True)}', flush=True)
            existing_children = {child.pid for child in mp.active_children()}
            try:
                try:
                    native = obs.sweep.run_trial(job['case'], job['seed'], path, job['duration'], outcome='grouping')
                finally:
                    ancestor.verify_trial_shutdown(existing_children)
                result = analyse_trial(path)
                atomic_json(path/'control_analysis.json', result)
                raw = verify_trial(path, result)
                atomic_json(path/'raw_verification.json', raw)
                if native.get('error') or not result['integrity_pass'] or not raw['pass']:
                    raise ValueError(f'trial failed: native={native.get("error")}; analysis={result["integrity_errors"]}; raw={raw.get("errors")}')
                atomic_json(path/'completion.json', dict(all_peers_shutdown=True,
                            file_sha256={f: sha256(path/f) for f in EVIDENCE_FILES},
                            finished_utc=datetime.now(timezone.utc).isoformat()))
                state = inspect_suite(suite)
                atomic_json(suite/'checkpoint.json', state)
                metrics = result['summary_metrics']
                print(f'DONE {path.name}: groups={metrics["total_groups"]}, honest wait={metrics["honest_mean_capped_wait_seconds"]:.3f}s; all peers shut down', flush=True)
                export_suite(suite)
                exports = verify_exports(suite)
                atomic_json(suite/'raw_verification_exports.json', exports)
                if not exports['pass']:
                    raise ValueError(f'export verification failed: {exports.get("errors")}')
            except BaseException as exc:
                atomic_json(suite/'halt.json', dict(trial=path.name, error=f'{type(exc).__name__}: {exc}',
                            all_peers_shutdown=not any(c.pid not in existing_children for c in mp.active_children())))
                raise
        report = export_suite(suite)
        exports = verify_exports(suite)
        atomic_json(suite/'raw_verification_exports.json', exports)
        if not exports['pass']:
            raise ValueError(f'export verification failed: {exports.get("errors")}')
        print(f'Saved {suite}: {inspect_suite(suite)["counts"]}; safe to suspend', flush=True)
        return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--phase', choices=('smoke', 'confirm'))
    parser.add_argument('--label')
    parser.add_argument('--gate', type=Path)
    parser.add_argument('--resume', type=Path)
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args(argv)
    if args.resume:
        if args.dry_run:
            print(json.dumps(inspect_suite(args.resume), indent=2))
        else:
            run_suite(args.resume)
        return
    if not args.phase:
        parser.error('provide --phase or --resume')
    if args.dry_run:
        print(json.dumps(dict(experiment='step30', phase=args.phase, jobs=make_jobs(args.phase)), indent=2))
        return
    if not args.label or not re.fullmatch(r'[A-Za-z0-9_-]+', args.label):
        parser.error('provide a distinct --label')
    if args.phase == 'confirm':
        if not args.gate:
            parser.error('confirm requires --gate')
        gate = inspect_suite(args.gate)
        if gate['counts']['completed'] != 2 or read_json(args.gate/'suite.json')['phase'] != 'smoke':
            parser.error('smoke incomplete')
        # Rebuild the derived gate from hash-validated completed analyses;
        # a stale or manually changed summary cannot authorize confirmation.
        from analyze_step30_harm import export_suite
        from verify_step30_harm import verify_exports
        report = export_suite(args.gate)
        if not verify_exports(args.gate)['pass']:
            parser.error('smoke exports failed independent checks')
        if not report.get('smoke_gate_pass'):
            parser.error('smoke traffic/measurement gates failed')
    suite = ROOT/'results'/f'step30_{args.phase}_{args.label}'
    suite.mkdir()
    snapshot(suite)
    atomic_json(suite/'suite.json', dict(experiment='step30', phase=args.phase, jobs=make_jobs(args.phase),
                training_delay=TRAINING, delay_profile=PROFILE, outcome='grouping', control_only=True,
                decision_rule=DECISION,
                gate=str(args.gate.resolve()) if args.gate else None))
    run_suite(suite)


if __name__ == '__main__':
    # The workload imports this module to configure globals in forked actors.
    # Run through its canonical module instance, rather than a second __main__
    # copy whose RPC hooks would otherwise have a different network context.
    import step30_harm as entrypoint
    entrypoint.main()
