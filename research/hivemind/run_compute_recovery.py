"""Replace suspension-affected paired seeds without changing the native harness."""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import time

import run_compute_exclusion as native
from announcement_artifacts import inventory, verify


def clock_sample():
    return dict(monotonic=time.monotonic(), boot=time.clock_gettime(time.CLOCK_BOOTTIME), wall=time.time())


def clock_check(before, after):
    elapsed = {key: after[key]-before[key] for key in before}
    suspend = elapsed['boot']-elapsed['monotonic']
    wall_jump = elapsed['wall']-elapsed['boot']
    return dict(valid=abs(suspend) <= 1 and abs(wall_jump) <= 1,
                elapsed=elapsed, suspend_seconds=suspend, wall_clock_jump_seconds=wall_jump,
                tolerance_seconds=1)


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def replacement_seeds(audit):
    cases = audit['cases']
    expected = {f'{arm}_s{seed}' for seed in range(302, 307) for arm in native.CONDITIONS}
    if len(cases) != 15 or {r['case'] for r in cases} != expected:
        raise ValueError('Suspension audit must cover the complete original matrix')
    affected = {r['case'] for r in cases if r['timing_eligible'] is False}
    if affected != set(audit['invalidated_cases']):
        raise ValueError('Audit invalidation lists disagree')
    if any(not isinstance(r['timing_eligible'], bool) for r in cases):
        raise ValueError('Audit eligibility must be explicit')
    return sorted({int(name.rsplit('_s', 1)[1]) for name in affected})


def compose(original, recovery, destination, seeds, audit_path):
    """Copy verified cases with an explicit source mapping; retain all originals."""
    for suite in (original, recovery):
        if verify(suite):
            raise ValueError(f'Source inventory mismatch: {suite}')
        native.check_sources(suite)
        if json.loads((suite/'status.json').read_text())['status'] != 'complete':
            raise ValueError('Composition requires completed sources')
    old_status = json.loads((original/'status.json').read_text())
    destination.mkdir()
    native.freeze(destination)
    cases, sources = [], {}
    for seed in range(302, 307):
        for arm in native.CONDITIONS:
            name = f'{arm}_s{seed}'
            source = recovery if seed in seeds else original
            case = source/name
            checks = json.loads((case/'checks.json').read_text())
            if checks['valid'] is not True:
                raise ValueError(f'Invalid source case: {case}')
            if seed in seeds and not json.loads((case/'clock_audit.json').read_text())['valid']:
                raise ValueError(f'Interrupted replacement: {case}')
            shutil.copytree(case, destination/name)
            cases.append(dict(path=name, valid=True))
            sources[name] = dict(source_case=str(case.resolve()),
                                source_inventory_sha256=digest(source/'artifact_inventory.json'))
    native.write(destination/'selection.json', dict(
        reason='Replace complete paired seeds affected by independently recorded system suspension; no outcome-based selection.',
        replaced_seeds=seeds, audit=str(audit_path.resolve()),
        audit_inventory_sha256=digest(audit_path/'artifact_inventory.json'), cases=sources))
    native.write(destination/'status.json', dict(schema='compute-native-suite-v1', mode='confirmation',
        status='complete', cases=cases, seeds=list(range(302, 307)), duration=300,
        reference=old_status['reference'], reference_inventory_sha256=old_status['reference_inventory_sha256'],
        composition='Clean original seeds plus complete replacement pairs; see selection.json'))
    native.write(destination/'artifact_inventory.json', inventory(destination))


def run(args):
    original, audit_path = args.original.resolve(), args.audit.resolve()
    if verify(original) or verify(audit_path):
        raise ValueError('Original or audit inventory mismatch')
    audit = json.loads((audit_path/'report.json').read_text())
    if Path(audit['suite']).resolve() != original or audit['suite_inventory_sha256'] != digest(original/'artifact_inventory.json'):
        raise ValueError('Audit does not identify this original suite')
    seeds = replacement_seeds(audit)
    if not seeds:
        raise ValueError('No interrupted paired seeds to replace')
    native.check_sources(original)
    previous = json.loads((original/'status.json').read_text())
    if previous['mode'] != 'confirmation' or previous['status'] != 'complete':
        raise ValueError('Expected complete original confirmation batch')
    pilot = Path(previous['reference'])
    native.check_reference(pilot, 'confirmation')
    recovery = native.label_path(args.label)
    clean = native.label_path(args.clean_label)
    report = native.label_path(args.report_label)
    if any(p.exists() for p in (recovery, clean, report)):
        raise FileExistsError('Recovery, composition and report labels must be fresh')
    recovery.mkdir(); native.freeze(recovery)
    wrapper = Path(__file__).resolve()
    wrapper_hash = digest(wrapper)
    shutil.copyfile(wrapper, recovery/'harness'/wrapper.name)
    state = dict(mode='confirmation_recovery', status='running', seeds=seeds, cases=[],
        duration=300, original=str(original), original_inventory_sha256=digest(original/'artifact_inventory.json'),
        reference=str(pilot), audit=str(audit_path), wrapper_sha256=wrapper_hash)
    native.write(recovery/'status.json', state)
    try:
        for seed in seeds:
            offset = (seed-302) % 3
            order = native.CONDITIONS[offset:] + native.CONDITIONS[:offset]
            for arm in order:
                native.check_sources(recovery)
                if digest(wrapper) != wrapper_hash:
                    raise ValueError('Recovery wrapper changed during execution')
                name = f'{arm}_s{seed}'
                print('RUN', name, flush=True)
                before = clock_sample()
                try:
                    result = native.launch_case(recovery/name, mode='confirmation', condition=arm, seed=seed)
                finally:
                    after = clock_sample()
                    timing = dict(clock_check(before, after), before=before, after=after)
                    native.write(recovery/name/'clock_audit.json', timing)
                if not timing['valid']:
                    raise RuntimeError(f'Suspension or wall-clock jump detected in {name}; preserve and stop')
                state['cases'].append(dict(path=name, valid=result['valid']))
                native.write(recovery/'status.json', state)
        native.check_sources(recovery)
        state['status'] = 'complete'
    except BaseException as exc:
        state.update(status='failed', error=repr(exc))
        raise
    finally:
        native.write(recovery/'status.json', state)
        native.write(recovery/'artifact_inventory.json', inventory(recovery))
    compose(original, recovery, clean, seeds, audit_path)
    from report_compute_exclusion import build
    build(clean, report)
    print('COMPLETE', clean, report, flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--original', type=Path, required=True)
    parser.add_argument('--audit', type=Path, required=True)
    parser.add_argument('--label', required=True)
    parser.add_argument('--clean-label', required=True)
    parser.add_argument('--report-label', required=True)
    run(parser.parse_args())
