"""Native evidence gates for the dominant-compute simulator extension.

Numerical replay alone never enables a timing sweep. The timing model must be
calibrated on the pilot and validated on separate confirmation seeds.
"""
from __future__ import annotations
import json
import hashlib
import math
import shutil
import csv
import sys
from dataclasses import asdict
from pathlib import Path
from statistics import median

from announcement_artifacts import inventory, verify

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]


def _write(path, value):
    path.write_text(json.dumps(value, indent=2, allow_nan=False) + '\n')


def _native_cases(suite):
    status = json.loads((suite / 'status.json').read_text())
    return [suite / item['path'] for item in status['cases']]


def numerical_gate(chain):
    """Retain every numerical replay failure; a timing result cannot override it."""
    from validate_compute_exclusion import replay_case
    reports = []
    for stage in ('markers', 'pilot', 'confirmation'):
        for case in _native_cases(chain[stage]):
            raw = replay_case(case)
            reports.append(dict(stage=stage, case=str(case), valid=raw['valid'],
                                checks=raw['checks'], failures=raw['failures']))
    return dict(valid=bool(reports) and all(r['valid'] for r in reports), cases=reports)


def native_endpoints(case):
    from compute_timing import summarize_case
    analysis = json.loads((case / 'analysis.json').read_text())
    if not analysis['valid']:
        raise ValueError(f'Invalid native evidence: {case}')
    timing = summarize_case(case)
    if timing['errors']:
        raise ValueError(f'Invalid native timings: {timing["errors"]}')
    learning = analysis['learning']
    return dict(condition=analysis['condition'], seed=analysis['seed'],
                crossing_time=learning['crossing_time'], censored=learning['censored'],
                unmodeled_native_paths=(['state_recovery'] if
                    timing['state'].get('state_recovery_fetch', 0) or
                    timing['state'].get('state_recovery_application', 0) else []),
                update_intervals={n: timing['nodes'][n]['median_update_interval_seconds']
                                  for n in ('D', 'C')})


def calibrate(pilot):
    """Pool measured components, never optimize against convergence outcomes."""
    from compute_calibration import calibrate_case
    cases = [calibrate_case(case) for case in _native_cases(pilot)]
    if any(case['errors'] or not case['pilot_calibration_eligible'] or
           case['suite_status'] != 'complete' for case in cases):
        raise ValueError('Calibration requires error-free completed pilot evidence')
    def pooled(values, label):
        observed = [float(v) for v in values if isinstance(v, (int, float))
                    and not isinstance(v, bool) and math.isfinite(v) and v >= 0]
        if not observed:
            raise ValueError(f'No measured pilot component: {label}')
        return median(observed)
    parameters = {}
    for parameter, key in (
        ('step_overhead_seconds', 'ordinary_optimizer_step_seconds'),
        ('interbatch_overhead_seconds', 'inter_batch_overhead_seconds')):
        parameters[parameter] = {node: pooled([
            case.get('nodes', {}).get(node, {}).get(key, {}).get('median_seconds')
            for case in cases], f'{node} {key}') for node in ('B', 'D', 'A', 'C')}
    for parameter, key in (
        ('gradient_latency_seconds', 'gradient_group_service_latency_seconds'),
        ('state_latency_seconds', 'state_service_latency_seconds')):
        parameters[parameter] = pooled([case.get(key, {}).get('median_seconds') for case in cases], key)
    parameters['progress_latency_seconds'] = 0.0
    return dict(parameters=parameters, cases=cases,
        method='Median of available pilot case component medians; no loss or time-to-target fitting.',
        assumptions=[
            'Zero progress visibility latency is a local-loopback approximation, not a measured DHT latency.',
            'Fallback deadline is the native configured two seconds, separate from measured fast service.',
            'Recovery and unrepresented state/membership paths stop the candidate; their timing is not invented.',
            'Validation at the native configured 70% rate share does not directly validate other rate shares.',
        ])


def predict(seed, condition, share, calibration, destination):
    """Run real seeded microbatch arithmetic on the existing virtual-time engine."""
    import numpy as np
    from trainsim.protocols.hivemind.compute import ComputeConfig, run_compute
    from trainsim.policies.announcements import AnnouncementPolicy
    from compute_workload import batch, evaluation, evaluate_models, summarize_evaluations
    nodes = ('B', 'D', 'C') if condition == 'absent' else ('B', 'D', 'A', 'C')
    rates = {node: (384*share/(1-share) if node == 'A' else 128) for node in nodes}
    x, y = evaluation(seed)
    initial = evaluate_models({n: np.zeros(48, dtype=np.float32) for n in nodes}, x, y)
    thresholds = {n: .1*initial['per_node'][n] for n in ('D', 'C')}
    def gradient(seed, node, index, weights):
        bx, by, _ = batch(seed, node, index)
        return np.asarray(bx.T @ (bx @ weights - by) / 32, dtype=np.float32)
    def evaluate(time, models):
        return dict(time=time, metrics=evaluate_models(models, x, y), thresholds=thresholds)
    parameters = dict(calibration['parameters'])
    for key in ('step_overhead_seconds', 'interbatch_overhead_seconds'):
        parameters[key] = {n: parameters[key][n] for n in nodes}
    config = ComputeConfig(seed=seed, horizon_seconds=300, rates=rates, members=nodes, **parameters)
    policy = AnnouncementPolicy('membership' if condition == 'exclusion' else 'honest')
    destination.mkdir()
    _write(destination / 'config.json', dict(model=asdict(config), condition=condition,
                                            configured_A_share=share))
    raw = run_compute(config, policy, gradient, evaluate)
    for key in ('evaluations', 'trace'):
        with (destination / f'{key}.jsonl').open('w') as stream:
            for row in raw[key]:
                stream.write(json.dumps(row, allow_nan=False)+'\n')
    learning = summarize_evaluations(raw['evaluations'])
    intervals = {}
    for node in ('D', 'C'):
        times = raw.get('update_begin_times', raw['update_times']).get(node, [])
        gaps = [b-a for a, b in zip(times, times[1:])]
        intervals[node] = median(gaps) if gaps else None
    summary = dict(seed=seed, condition=condition, configured_A_share=share,
        supported=not raw['unsupported'], unsupported_reasons=raw['unsupported_reasons'],
        crossing_time=learning['crossing_time'], censored=learning['censored'],
        update_intervals=intervals, learning=learning,
        model_statistics={k: v for k, v in raw.items() if k not in ('evaluations', 'trace')})
    _write(destination / 'summary.json', summary)
    return summary


def reference_chain(reference):
    """Require the complete C0 -> C1 -> C2 evidence chain before C3."""
    current = Path(reference).resolve()
    chain = {}
    for expected in ('confirmation', 'pilot', 'markers'):
        if verify(current):
            raise ValueError(f'Inventory mismatch: {current}')
        status = json.loads((current / 'status.json').read_text())
        if status.get('mode') != expected or status.get('status') != 'complete':
            raise ValueError(f'C3 requires a complete {expected} suite')
        seeds = {'confirmation': range(302, 307), 'pilot': (301,), 'markers': (11, 12, 13)}[expected]
        required = {f'{arm}_s{seed}' for arm in ('baseline', 'exclusion', 'absent') for seed in seeds}
        cases = status.get('cases', [])
        if (len(cases) != len(required) or {c.get('path') for c in cases} != required
                or any(c.get('valid') is not True for c in cases)):
            raise ValueError(f'Incomplete or invalid {expected} case matrix')
        chain[expected] = current
        if expected != 'markers':
            if not status.get('reference'):
                raise ValueError('Native reference chain is incomplete')
            current = Path(status['reference'])
            digest = hashlib.sha256((current / 'artifact_inventory.json').read_bytes()).hexdigest()
            if digest != status.get('reference_inventory_sha256'):
                raise ValueError('Native reference inventory identity changed')
    return chain


def assess_timing(native, simulated, tolerance=.20):
    """Compare a fixed prediction matrix without imputing censored endpoints."""
    if not 0 < tolerance < 1:
        raise ValueError('Timing tolerance must be between zero and one')
    errors, details, comparisons = [], [], {}
    def indexed(rows):
        result = {}
        for row in rows:
            key = (row['condition'], row['seed'])
            if key in result:
                raise ValueError(f'Duplicate timing case {key}')
            result[key] = row
        return result
    actual, predicted = indexed(native), indexed(simulated)
    if set(actual) != set(predicted) or not actual:
        return dict(valid=False, errors=['Native/predicted case matrices differ or are empty'],
                    cases=[], conditions={})
    def positive(value):
        return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value > 0
    def discrepancy(a, b):
        return abs(b-a)/a if positive(a) and positive(b) else None
    for key in sorted(actual):
        a, b = actual[key], predicted[key]
        if a.get('unmodeled_native_paths'):
            errors.append(f'{key}: native path outside candidate scope: {a["unmodeled_native_paths"]}')
        if not b.get('supported', False):
            errors.append(f'{key}: unsupported simulated path')
        same_censor = a['censored'] == b['censored']
        if not same_censor:
            errors.append(f'{key}: censor classification differs')
        for row in (a, b):
            if not row['censored'] and not positive(row.get('crossing_time')):
                errors.append(f'{key}: invalid observed crossing')
            if any(not positive(row.get('update_intervals', {}).get(n)) for n in ('D', 'C')):
                errors.append(f'{key}: missing D/C update interval')
        details.append(dict(condition=key[0], seed=key[1],
            native_crossing=a.get('crossing_time'), simulated_crossing=b.get('crossing_time'),
            native_censored=a['censored'], simulated_censored=b['censored'],
            crossing_relative_error=(discrepancy(a.get('crossing_time'), b.get('crossing_time'))
                                     if not a['censored'] and not b['censored'] else None),
            update_interval_relative_errors={n: discrepancy(a.get('update_intervals', {}).get(n),
                b.get('update_intervals', {}).get(n)) for n in ('D', 'C')}))
    for condition in sorted({k[0] for k in actual}):
        keys = [k for k in actual if k[0] == condition]
        endpoints = {}
        for name in ('crossing_time', 'update_interval'):
            av, bv = [], []
            for key in keys:
                a, b = actual[key], predicted[key]
                if name == 'crossing_time':
                    if a['censored'] or b['censored']:
                        continue
                    pairs = [(a.get(name), b.get(name))]
                else:
                    pairs = [(a.get('update_intervals', {}).get(n), b.get('update_intervals', {}).get(n))
                             for n in ('D', 'C')]
                for x, y in pairs:
                    if positive(x) and positive(y):
                        av.append(x); bv.append(y)
            x, y = (median(av), median(bv)) if av else (None, None)
            error = discrepancy(x, y)
            endpoints[name] = dict(native_median=x, simulated_median=y,
                relative_error=error, paired_observations=len(av),
                within_tolerance=error <= tolerance if error is not None else None)
            if error is not None and error > tolerance:
                errors.append(f'{condition}: median {name} exceeds {tolerance:.0%}')
        comparisons[condition] = endpoints
    return dict(valid=not errors, tolerance=tolerance, errors=errors,
                cases=details, conditions=comparisons)


def model_sources():
    paths = [HERE / name for name in (
        'compute_simulation.py', 'compute_calibration.py', 'compute_timing.py',
        'compute_workload.py', 'validate_compute_exclusion.py', 'report_compute_exclusion.py')]
    base = ROOT / 'research/simulator/src/trainsim'
    paths += [base / name for name in ('engine.py', 'types.py',
        'protocols/hivemind/compute.py', 'protocols/hivemind/numerical.py',
        'protocols/partitioning.py', 'policies/announcements.py', 'policies/base.py')]
    return {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}


def _freeze_model(destination):
    hashes = model_sources()
    for relative in hashes:
        target = destination / 'harness' / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / relative, target)
    _write(destination / 'source_hashes.json', hashes)
    import numpy, simpy
    _write(destination/'runtime.json', dict(python=sys.version, numpy=numpy.__version__, simpy=simpy.__version__))
    return hashes


def _sweep_report(destination, state):
    from report_compute_exclusion import paired_summary
    rows = []
    for case in state['cases']:
        result = json.loads((destination/case['path']/'summary.json').read_text())
        rows.append({k: result[k] for k in ('seed', 'condition', 'configured_A_share',
                                           'supported', 'crossing_time', 'censored')})
    pairs = {str(share): paired_summary([r for r in rows if r['configured_A_share'] == share])
             for share in sorted({r['configured_A_share'] for r in rows})}
    _write(destination/'report.json', dict(status=state['status'], scope=state['scope'], rows=rows, paired=pairs))
    if rows:
        with (destination/'timings.csv').open('w') as stream:
            writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
            writer.writeheader(); writer.writerows(rows)
    lines = ['# Conditional compute-share simulation', '', state['scope'], '',
             f"Status: {state['status']}. Unsupported paths stop the sweep and remain in the results.", '',
             '| Configured A share | Condition | Uncensored median crossing (virtual s) | Censored | Observed runs |',
             '|---|---|---:|---:|---:|']
    for share in sorted({r['configured_A_share'] for r in rows}):
        for arm in ('baseline', 'exclusion', 'absent'):
            subset = [r for r in rows if r['configured_A_share'] == share and r['condition'] == arm]
            observed = [r['crossing_time'] for r in subset if not r['censored'] and r['supported']]
            value = f'{median(observed):.2f}' if observed else '—'
            lines.append(f'| {share:.0%} | {arm} | {value} | {sum(r["censored"] for r in subset)} | {len(subset)} |')
    lines += ['', 'Shares specify processing-rate caps. Actual contributions depend on optimizer stalls and scheduling. '
              'The absent condition has no A; its other worker rates stay fixed. State sharing remains enabled. '
              'These predictions do not establish performance on real unequal hardware or a public swarm.']
    (destination/'REPORT.md').write_text('\n'.join(lines)+'\n')


def validate_stage(reference, destination):
    chain = reference_chain(reference)
    destination.mkdir()
    hashes = _freeze_model(destination)
    status = dict(mode='validate', status='running', valid=False, reference=str(reference.resolve()),
        reference_inventory_sha256=hashlib.sha256((reference/'artifact_inventory.json').read_bytes()).hexdigest())
    try:
        replay = numerical_gate(chain)
        _write(destination/'numerical_replay.json', replay)
        if not replay['valid']:
            status.update(status='gate_failed', gate='numerical_replay')
            return destination
        calibration = calibrate(chain['pilot'])
        _write(destination/'calibration.json', calibration)
        pilot_predictions = [predict(301, arm, .7, calibration, destination/f'pilot_{arm}_s301')
                             for arm in ('baseline', 'exclusion', 'absent')]
        pilot = assess_timing([native_endpoints(p) for p in _native_cases(chain['pilot'])], pilot_predictions)
        _write(destination/'pilot_timing.json', pilot)
        if not pilot['valid']:
            status.update(status='gate_failed', gate='pilot_model_check')
            return destination
        predictions = []
        for path in _native_cases(chain['confirmation']):
            config = json.loads((path/'config.json').read_text())
            predictions.append(predict(config['seed'], config['condition'], .7, calibration,
                                       destination/f'heldout_{path.name}'))
        heldout = assess_timing([native_endpoints(p) for p in _native_cases(chain['confirmation'])], predictions)
        _write(destination/'heldout_timing.json', heldout)
        status.update(status='complete' if heldout['valid'] else 'gate_failed',
                      valid=heldout['valid'], gate='heldout_timing')
    except BaseException as exc:
        status.update(status='error', error=repr(exc))
        raise
    finally:
        if hashes != model_sources():
            status.update(status='error', valid=False, error='Model sources changed during validation')
        _write(destination/'status.json', status)
        text = ('# Dominant-compute simulation validation\n\n'
                f"Status: {status['status']}. Gate: {status.get('gate', 'setup')}.\n\n"
                'Numerical replay, pilot component calibration, and held-out timing are separate gates. '
                'A gate failure leaves the native findings intact and does not authorize the share sweep. '
                'Calibration uses pilot component measurements only; missing/censored endpoints are retained.\n')
        (destination/'REPORT.md').write_text(text)
        _write(destination/'artifact_inventory.json', inventory(destination))
    return destination


def simulate_stage(reference, destination):
    if verify(reference):
        raise ValueError('Validation artifact inventory mismatch')
    validation = json.loads((reference/'status.json').read_text())
    if validation.get('mode') != 'validate' or validation.get('status') != 'complete' or validation.get('valid') is not True:
        raise ValueError('Share sweep requires passing native behavioral and held-out timing validation')
    if json.loads((reference/'source_hashes.json').read_text()) != model_sources():
        raise ValueError('Model differs from the validated version')
    native_reference = Path(validation['reference'])
    reference_chain(native_reference)
    if hashlib.sha256((native_reference/'artifact_inventory.json').read_bytes()).hexdigest() != validation['reference_inventory_sha256']:
        raise ValueError('Native validation reference changed')
    calibration = json.loads((reference/'calibration.json').read_text())
    destination.mkdir()
    hashes = _freeze_model(destination)
    state = dict(mode='simulate', status='running', reference=str(reference.resolve()), cases=[],
        reference_inventory_sha256=hashlib.sha256((reference/'artifact_inventory.json').read_bytes()).hexdigest(),
        scope='Conditional local-loopback model predictions at new configured rates; no native validation at those rates.')
    try:
        for share in (.25, .50, .70, .90):
            for seed in range(401, 411):
                for arm in ('baseline', 'exclusion', 'absent'):
                    name = f'share{round(100*share)}_{arm}_s{seed}'
                    result = predict(seed, arm, share, calibration, destination/name)
                    state['cases'].append(dict(path=name, supported=result['supported']))
                    if not result['supported']:
                        state.update(status='stopped_unsupported', unsupported_reasons=result['unsupported_reasons'])
                        return destination
        state['status'] = 'complete'
    except BaseException as exc:
        state.update(status='error', error=repr(exc))
        raise
    finally:
        if hashes != model_sources():
            state.update(status='error', error='Model sources changed during sweep')
        _write(destination/'status.json', state)
        _sweep_report(destination, state)
        _write(destination/'artifact_inventory.json', inventory(destination))
    return destination


def run_stage(args):
    from run_compute_exclusion import label_path
    if args.reference is None:
        raise ValueError('A native confirmation reference or validated simulation artifact is required')
    destination = label_path(args.label)
    if destination.exists():
        raise FileExistsError(destination)
    if args.command == 'validate':
        return validate_stage(args.reference, destination)
    return simulate_stage(args.reference, destination)
