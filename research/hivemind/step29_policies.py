"""Exact native 5/1/2-second publishers versus (1,2] renewal, local pilot.

Normal matchmaking is primary. --behaviors both additionally isolates refusal
to follow. No core edits; all predecessors stay frozen. STOP_AFTER_CURRENT is
checked only at fully shut-down trial boundaries. Offline --dry-run opens no
sockets. Run from /tmp with the verified Step23 runtime/interpreter.
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
import fcntl
import json
import math
import os
from pathlib import Path
import random
import re
import shutil

import step28_confirmation as ancestor

ROOT = Path(__file__).resolve().parent
EXTRA_FILES = ('step29_policies.py', 'step29_workload.py',
               'analyze_step29_policies.py', 'test_step29_policies.py', 'verify_step28_exports_v2.py')
FROZEN_FILES = ancestor.FROZEN_FILES + EXTRA_FILES
RAW_FILES = ancestor.RAW_FILES
sha256, read_json, atomic_json = ancestor.sha256, ancestor.read_json, ancestor.atomic_json
check_clock = ancestor.check_clock
TRAINING = ancestor.TRAINING
OBS = None


def make_cases(behaviors='normal'):
    if behaviors not in ('normal', 'both'):
        raise ValueError('behaviors must be normal or both')
    cases = []
    for behavior in ('normal', 'recruit_only') if behaviors == 'both' else ('normal',):
        for policy, lifetime in (('native', 5.), ('native', 1.), ('native', 2.), ('renew', 2.)):
            suffix = f'native_{int(lifetime)}s' if policy == 'native' else 'renew_1_2'
            case = dict(name=f'{behavior}_{suffix}', behavior=behavior,
                        mode='stock' if behavior == 'normal' else 'passive',
                        publication_policy=policy, lifetime=lifetime, minimum=2,
                        target=4, one_way_delay_seconds=0., response='reliable', hold_seconds=0.)
            if policy == 'renew':
                case.update(frequency=1., nominal_lower=1., nominal_upper=2.)
            cases.append(case)
    return cases


def make_jobs(phase, behaviors='normal'):
    cases = make_cases(behaviors)
    if phase == 'smoke':
        return [dict(case=c, seed=2900, duration=10.) for c in cases
                if c['behavior'] == 'normal' and
                (c['publication_policy'] == 'renew' or c['lifetime'] == 1.)]
    if phase != 'pilot':
        raise ValueError('phase must be smoke or pilot')
    random.Random(290029).shuffle(cases)
    return [dict(case=c, seed=2901, duration=45.) for c in cases]


def native_expiration(now, scheduled, minimum, search_end):
    return float(min(max(scheduled, now + minimum), search_end))


def renewal_sleep(expiration, now, write_elapsed, period=1.):
    return max(0., min(expiration - now, period - write_elapsed))


async def renewed_native_publisher(self, step, key_manager):
    """Native declaration loop with only pre-expiry send cadence changed.

Expiration formula, running gate, locks, declaration event, key housekeeping,
and withdrawal follow the native code. Request timers are never reset.
"""
    base = OBS.base
    async with self.lock_declare:
        try:
            while True:
                await self.running.wait()
                expiration = native_expiration(base.get_dht_time(), step.scheduled_time,
                                               self.min_matchmaking_time, self.search_end_time)
                self.declared_group_key = group_key = key_manager.current_key
                self.declared_expiration_time = expiration
                self.declared_expiration.set()
                sent = base.time.monotonic()
                await key_manager.declare_averager(group_key, self.peer_id, expiration_time=expiration)
                await asyncio.sleep(renewal_sleep(expiration, base.get_dht_time(),
                                                 base.time.monotonic() - sent))
                if self.running.is_set() and len(self.leader_queue) == 0:
                    await key_manager.update_key_on_not_enough_peers()
        finally:
            if self.declared_group_key is not None:
                previous_key, previous_expiration = self.declared_group_key, self.declared_expiration_time
                self.declared_group_key, self.declared_expiration_time = None, float('inf')
                self.leader_queue = type(self.leader_queue)()
                self.max_assured_time = float('-inf')
                await key_manager.declare_averager(previous_key, self.peer_id, previous_expiration,
                                                  looking_for_group=False)


def install_runtime():
    global OBS
    if OBS is not None:
        return OBS
    OBS = ancestor.install_runtime()
    base = OBS.base

    async def publisher(self, step, key_manager):
        if base.ATTACKER and base.CASE['publication_policy'] == 'renew':
            return await renewed_native_publisher(self, step, key_manager)
        # Exact original function, captured before any research wrappers.
        return await base.ORIGINAL_DECLARE(self, step, key_manager)

    native_request = base.Matchmaking._request_join_potential_leaders

    async def traced_configuration(self, step):
        if not getattr(self, '_step29_config_logged', False):
            self._step29_config_logged = True
            OBS.emit('runtime_matchmaking_config', self.peer_id,
                     min_matchmaking_time=self.potential_leaders.min_matchmaking_time,
                     min_group_size=self.min_group_size, target_group_size=self.target_group_size,
                     request_timeout=self.request_timeout,
                     behavior=base.CASE['behavior'] if base.ATTACKER else 'normal',
                     publication_policy=base.CASE['publication_policy'] if base.ATTACKER else 'native',
                     step_id=OBS.sweep.user_data(step)['step_id'])
        return await native_request(self, step)

    def write_config(path, value):
        if Path(path).name == 'config.json' and 'peer_ids' in value:
            value = dict(value, experiment='step29', instrumentation='detailed',
                         attempt_unit='step_start', network_delay_injected=False,
                         one_way_delay_seconds=0., request_timeout=3, allow_state_sharing=False,
                         event_transport='batched', trace_batch_size=32, duplicate_read_views=False,
                         cached_reads_delayed=False, tensor_transfer_delayed=False,
                         response_policy='reliable', leadership_latency_origin='step_start',
                         harm_timeout_cap=12, clock_monitor=True, clock_jump_limit_seconds=.25,
                         submission_cutoff_checked_per_peer=True,
                         control_delay_model='remote find/store request+response; join request+each response')
        atomic_json(path, value)

    base.PotentialLeaders._declare_averager_periodically = publisher
    base.Matchmaking._request_join_potential_leaders = traced_configuration
    OBS.sweep.write_json = write_config
    import step29_workload
    OBS.sweep.run_trial = step29_workload.run_trial
    return OBS


def snapshot(suite):
    install_runtime()
    ancestor.snapshot(suite)
    provenance = read_json(suite / 'provenance.json')
    for name in EXTRA_FILES:
        shutil.copyfile(ROOT / name, suite / 'harness' / name)
        provenance['harness_sha256'][name] = sha256(ROOT / name)
    provenance.update(experiment='step29', entrypoint=str(Path(__file__).resolve()),
                      entrypoint_sha256=sha256(__file__), script_sha256=sha256(__file__),
                      observation_engine='Frozen Step28 RPC/clock and Step25 native-choice observation; Step29 native-policy workload')
    atomic_json(suite / 'provenance.json', provenance)


def validate_sources(suite):
    provenance = read_json(suite / 'provenance.json')
    if not set(FROZEN_FILES).issubset(provenance.get('harness_sha256', {})):
        raise ValueError('missing frozen source closure')
    ancestor.validate_sources(suite, provenance)
    if provenance.get('entrypoint_sha256') != sha256(__file__):
        raise ValueError('entrypoint changed since source freeze')


def inspect_suite(suite):
    suite = Path(suite).resolve()
    manifest = read_json(suite / 'suite.json')
    if manifest.get('experiment') != 'step29' or manifest['jobs'] != make_jobs(manifest['phase'], manifest['behaviors']):
        raise ValueError('manifest differs from exact design')
    validate_sources(suite)
    rows = []
    for job in manifest['jobs']:
        name = ancestor.trial_name(job)
        path = suite / name
        row = dict(job=job, trial_dir=name, status='pending')
        if path.exists():
            if not (path / 'completion.json').exists():
                row['status'] = 'interrupted'
            else:
                completion = read_json(path / 'completion.json')
                if completion.get('all_peers_shutdown') is not True:
                    raise ValueError('completion lacks peer shutdown')
                if set(completion.get('file_sha256', {})) != {*RAW_FILES, 'analysis.json', 'control_analysis.json', 'raw_verification.json'}:
                    raise ValueError('completion lacks full raw and derived evidence hashes')
                for filename, digest in completion['file_sha256'].items():
                    if sha256(path / filename) != digest:
                        raise ValueError(f'completed evidence changed: {name}/{filename}')
                config = read_json(path / 'config.json')
                if any(config[key] != job[key] for key in ('case', 'seed', 'duration')):
                    raise ValueError('completed config differs from manifest')
                ancestor.validate_trace_closed(path / 'events.jsonl')
                if not read_json(path / 'control_analysis.json')['integrity_pass']:
                    raise ValueError('completed evidence failed integrity')
                if read_json(path / 'raw_verification.json').get('pass') is not True:
                    raise ValueError('completed evidence failed independent raw checks')
                row['status'] = 'completed'
        rows.append(row)
    for archive_file in suite.glob('archive_*.json'):
        archive = read_json(archive_file)
        for filename, digest in archive['file_sha256'].items():
            if sha256(Path(archive['path']) / filename) != digest:
                raise ValueError(f'archived interrupted evidence changed: {filename}')
    return dict(experiment='step29', suite=str(suite), jobs=rows,
                counts={s: sum(r['status'] == s for r in rows) for s in ('completed', 'pending', 'interrupted')},
                valid=True)


def run_suite(suite):
    suite = Path(suite).resolve()
    with (suite / '.controller.lock').open('a+') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        state = inspect_suite(suite)
        atomic_json(suite / 'checkpoint.json', state)
        if ancestor.stop_requested(suite):
            print('STOP_AFTER_CURRENT present; no peers started; safe to suspend', flush=True)
            return
        obs = install_runtime()
        from analyze_step29_policies import analyse_trial, export_suite
        import multiprocessing as mp
        for index, row in enumerate(state['jobs'], 1):
            if row['status'] == 'completed':
                print(f'SKIP {index}/{len(state["jobs"])} {row["trial_dir"]}', flush=True)
                continue
            if ancestor.stop_requested(suite):
                break
            if row['status'] == 'interrupted':
                archive = ancestor.archive_interrupted(suite, row['trial_dir'])
                atomic_json(suite / f'archive_{row["trial_dir"]}.json', archive)
            job = row['job']; path = suite / row['trial_dir']
            validate_sources(suite)
            state.update(active_trial=path.name, controller_pid=os.getpid())
            atomic_json(suite / 'checkpoint.json', state)
            print(f'START {index}/{len(state["jobs"])} {path.name}: {json.dumps(job["case"], sort_keys=True)}', flush=True)
            existing_children = {child.pid for child in mp.active_children()}
            try:
                try:
                    native = obs.sweep.run_trial(job['case'], job['seed'], path, job['duration'], outcome='grouping')
                finally:
                    ancestor.verify_trial_shutdown(existing_children)
                result = analyse_trial(path)
                atomic_json(path / 'control_analysis.json', result)
                if obs.sweep.trial_should_halt(native, 'grouping') or not result['integrity_pass']:
                    raise ValueError(f'trial integrity failed: {result["integrity_errors"]}')
                import verify_step28_exports_v2 as independent
                raw = independent.raw_trial(path)
                errors = list(raw['errors'])
                essential = ('total_groups', 'x_led_groups', 'x_submitted_attempts', 'x_led_attempts',
                             'x_led_rate', 'x_mean_capped_latency_seconds',
                             'x_success_only_mean_first_lead_latency_seconds',
                             'x_success_only_median_first_lead_latency_seconds',
                             'honest_submitted_attempts', 'honest_first_choice_x_attempts',
                             'honest_first_choice_other_attempts', 'honest_no_outgoing_choice_attempts',
                             'honest_first_group_x_led_attempts', 'honest_first_group_other_led_attempts',
                             'honest_no_group_attempts', 'honest_accepted_x_reply_count',
                             'honest_accepted_other_reply_count')
                for key in essential:
                    if key not in raw['metrics'] or key not in result['summary_metrics']:
                        errors.append(f'missing independently checked metric {key}')
                    elif not independent._same_value(raw['metrics'][key], result['summary_metrics'][key]):
                        errors.append(f'independent raw metric differs: {key}')
                atomic_json(path / 'raw_verification.json', dict(pass_=not errors, **{'pass': not errors},
                            errors=errors, checked_metrics={k: raw['metrics'].get(k) for k in essential},
                            raw_sha256=sha256(path / 'events.jsonl')))
                if errors:
                    raise ValueError(f'independent raw checks failed: {errors}')
                files = {f: sha256(path / f) for f in (*RAW_FILES, 'analysis.json', 'control_analysis.json', 'raw_verification.json')}
                atomic_json(path / 'completion.json', dict(file_sha256=files, all_peers_shutdown=True,
                            finished_utc=datetime.now(timezone.utc).isoformat()))
                row['status'] = 'completed'
                state = inspect_suite(suite)
                atomic_json(suite / 'checkpoint.json', state)
                m = result['summary_metrics']
                print(f'DONE {path.name}: groups={m["total_groups"]}, X-led={m["x_led_attempts"]}/{m["x_submitted_attempts"]}; all peers shut down', flush=True)
                export_suite(suite)
            except BaseException as exc:
                atomic_json(suite / 'halt.json', dict(trial=path.name, error=f'{type(exc).__name__}: {exc}',
                            all_peers_shutdown=not any(c.pid not in existing_children for c in mp.active_children())))
                raise
        report = export_suite(suite)
        print(f'Saved {suite}: {inspect_suite(suite)["counts"]}; safe to suspend', flush=True)
        return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--phase', choices=('smoke', 'pilot'))
    parser.add_argument('--behaviors', choices=('normal', 'both'), default='normal')
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
        print(json.dumps(dict(experiment='step29', phase=args.phase, jobs=make_jobs(args.phase, args.behaviors)), indent=2))
        return
    if not args.label or not re.fullmatch(r'[A-Za-z0-9_-]+', args.label):
        parser.error('provide a distinct --label')
    if args.phase == 'pilot':
        if not args.gate:
            parser.error('pilot requires the completed smoke --gate')
        gate = inspect_suite(args.gate)
        if gate['counts']['completed'] != 2 or read_json(args.gate / 'suite.json')['phase'] != 'smoke':
            parser.error('smoke gate incomplete')
    suite = ROOT / 'results' / f'step29_{args.phase}_{args.label}'
    suite.mkdir()
    snapshot(suite)
    atomic_json(suite / 'suite.json', dict(experiment='step29', phase=args.phase, behaviors=args.behaviors,
                jobs=make_jobs(args.phase, args.behaviors), training_delay=TRAINING,
                order_seed=290029, gate=str(args.gate.resolve()) if args.gate else None,
                outcome='grouping', instrumentation='detailed', preliminary=True))
    run_suite(suite)


if __name__ == '__main__':
    main()
