"""Build paired native timing tables from verified dominant-compute suites."""
from __future__ import annotations
import argparse
import csv
import io
import json
from pathlib import Path
from statistics import median
from announcement_artifacts import inventory, verify
from compute_timing import summarize_case
from validate_compute_exclusion import replay_case


NODES = ('B', 'D', 'A', 'C')
NODE_METRICS = (
    'submitted_samples', 'accepted_samples', 'discarded_samples', 'pending_samples',
    'submitted_samples_per_second', 'completed_updates', 'median_update_interval_seconds',
)
STATE_EVENTS = (
    'state_application', 'state_recovery_fetch', 'state_recovery_application',
    'state_download_chunk', 'state_download_terminal',
)


def check_timing_eligibility(suite, audit_root=None):
    """External interruption audits supersede a case's protocol-only checks."""
    audit_root = audit_root or Path(__file__).resolve().parent / 'results'
    affected = []
    for source in audit_root.glob('compute_suspend_audit_*/report.json'):
        audit = json.loads(source.read_text())
        if Path(audit['suite']).name == suite.name:
            affected.extend(audit.get('invalidated_cases', []))
    if affected:
        raise ValueError(f'System suspension invalidated timing cases: {sorted(set(affected))}; see suspension audit')


def _compact_native(raw):
    nodes = {}
    for node, metrics in raw.get('nodes', {}).items():
        nodes[node] = {key: metrics.get(key) for key in NODE_METRICS}
        nodes[node]['accepted_into_accumulator_samples'] = nodes[node].pop('accepted_samples')
        nodes[node]['discarded_submitted_samples'] = nodes[node].pop('discarded_samples')
        nodes[node]['pending_submitted_samples'] = nodes[node].pop('pending_samples')
    roster = raw.get('roster_contribution_summary', {})
    state = raw.get('state', {})
    gradient_weights = {node: sum(group['weight_by_node'].get(node, 0)
        for group in raw.get('unique_group_weight_totals', []) if group['channel'] == 'gradient')
        for node in nodes}
    return {
        'measured_horizon_seconds': raw.get('measured_horizon_seconds'),
        'nodes': nodes,
        'gradient_group_input_sample_weights_by_origin': gradient_weights,
        'gradient_input_note': ('Sample-count weights submitted to numerical gradient groups, counted once '
            'per origin and group. These differ from accepted accumulations that may never reach a group; '
            'they are not a count of end-to-end parameter influence through state sharing.'),
        'group_timing_seconds': raw.get('group_timing_seconds', {}),
        'roster_omission': {
            'A_native_present_group_count': roster.get('A_native_present_group_count'),
            'A_proposed_view_count': roster.get('A_proposed_view_count'),
            'A_effective_present_view_count': roster.get('A_effective_present_view_count'),
            'A_omitted_view_count': roster.get('A_omitted_view_count'),
            'A_omitted_unique_group_count': roster.get('A_omitted_unique_group_count'),
            'A_gradient_weight_in_groups_with_an_omitted_view':
                roster.get('A_weight_in_groups_with_an_omitted_view'),
            'interpretation': 'Roster omission is local to that reducer view and does not mean all of A’s work was wasted.',
        },
        'state_sharing': {
            'event_counts': {kind: state.get(kind, 0) for kind in STATE_EVENTS},
            'downloads_by_path': state.get('downloads_by_path', {}),
            'A_paths': state.get('A_paths', {}),
        },
        'untraced_boundary_examples_upper_bound_per_node':
            raw.get('untraced_boundary_examples_upper_bound_per_node', 32),
        'sample_counting_note': raw.get('sample_counting_note'),
        'accumulation_note': (
            'Accepted samples count submissions accepted into a local gradient accumulator; '
            'they do not by themselves establish incorporation into final parameters.'
        ),
        'errors': raw.get('errors', []),
    }


def _compact_replay(raw):
    groups = raw.get('groups', [])
    return {
        'available': True,
        'valid': raw.get('valid') is True,
        'simulation_gate_eligible': raw.get('simulation_gate_eligible'),
        'checks': raw.get('checks', {}),
        'group_status_counts': {
            status: sum(group.get('status') == status for group in groups)
            for status in ('replayed', 'unmodeled', 'excluded_shutdown')
        },
        'failure_count': len(raw.get('failures', [])),
        'failures': raw.get('failures', []),
    }


def _replay_summary(path):
    try:
        return _compact_replay(replay_case(path))
    except Exception as exc:  # Replay is supplementary to analyzer-validated native results.
        return {'available': False, 'valid': False, 'checks': {},
                'group_status_counts': {}, 'failure_count': 1,
                'failures': [f'{type(exc).__name__}: {exc}']}


def _csv_row(row):
    flat = {key: row.get(key) for key in (
        'seed', 'condition', 'crossing_time', 'confirmed_time', 'censored',
        'final_DC_mean', 'final_DC_worst', 'final_A', 'outcome', 'cleanup_status')}
    flat['cleanup_pending_nodes'] = ','.join(row.get('cleanup_pending_nodes', []))
    native = row['native_summary']
    flat['measured_horizon_seconds'] = native['measured_horizon_seconds']
    for node in NODES:
        metrics = native['nodes'].get(node, {})
        for key in ('submitted_samples', 'accepted_into_accumulator_samples',
                    'discarded_submitted_samples', 'pending_submitted_samples',
                    'submitted_samples_per_second', 'completed_updates',
                    'median_update_interval_seconds'):
            flat[f'{node}_{key}'] = metrics.get(key)
        flat[f'{node}_gradient_group_input_sample_weight'] = native['gradient_group_input_sample_weights_by_origin'].get(node)
    timing = native['group_timing_seconds']
    for channel in ('gradient', 'state'):
        stats = timing.get(channel, {})
        for key in ('count', 'mean', 'median', 'p95'):
            flat[f'{channel}_group_time_{key}'] = stats.get(key)
    roster = native['roster_omission']
    for key, value in roster.items():
        if key != 'interpretation':
            flat[key] = value
    for key, value in native['state_sharing']['event_counts'].items():
        flat[f'{key}_count'] = value
    flat['state_downloads_by_path_json'] = json.dumps(
        native['state_sharing']['downloads_by_path'], sort_keys=True)
    flat['state_A_paths_json'] = json.dumps(native['state_sharing']['A_paths'], sort_keys=True)
    flat['untraced_boundary_examples_upper_bound_per_node'] = native[
        'untraced_boundary_examples_upper_bound_per_node']
    replay = row['numerical_replay']
    flat['numerical_replay_available'] = replay.get('available')
    flat['numerical_replay_valid'] = replay.get('valid')
    flat['numerical_replay_checks_json'] = json.dumps(replay.get('checks', {}), sort_keys=True)
    flat['numerical_replay_failure_count'] = replay.get('failure_count')
    flat['numerical_replay_failures_json'] = json.dumps(replay.get('failures', []), ensure_ascii=False)
    return flat


def paired_summary(rows):
    """Report paired deltas only where both endpoints were observed."""
    result = {}
    for condition in ('exclusion', 'absent'):
        pairs, censored = [], []
        for seed in sorted({row['seed'] for row in rows}):
            arms = {row['condition']: row for row in rows if row['seed'] == seed}
            if 'baseline' not in arms or condition not in arms:
                continue
            base, arm = arms['baseline'], arms[condition]
            if base['censored'] or arm['censored']:
                censored.append(dict(seed=seed, baseline=base['censored'], treatment=arm['censored']))
            else:
                pairs.append(dict(seed=seed, baseline_seconds=base['crossing_time'],
                    treatment_seconds=arm['crossing_time'],
                    difference_seconds=arm['crossing_time']-base['crossing_time']))
        result[condition] = dict(observed_pairs=pairs, censored_pairs=censored,
            median_paired_difference_seconds=median([p['difference_seconds'] for p in pairs]) if pairs else None)
    return result


def build(suite, destination):
    check_timing_eligibility(suite)
    if verify(suite):
        raise ValueError('Suite inventory mismatch')
    status = json.loads((suite / 'status.json').read_text())
    if status['mode'] not in ('pilot', 'confirmation') or status['status'] != 'complete':
        raise ValueError('Requires a completed native training suite')
    rows = []
    for case in status['cases']:
        path = suite / case['path']
        analysis = json.loads((path / 'analysis.json').read_text())
        if not analysis['valid']:
            raise ValueError(f'Invalid case: {path}')
        learning = analysis['learning']
        metrics = learning['final_metrics']
        native_summary = _compact_native(summarize_case(path))
        replay_summary = _replay_summary(path)
        outcome = json.loads((path / 'outcome.json').read_text())
        rows.append(dict(seed=analysis['seed'], condition=analysis['condition'],
            crossing_time=learning['crossing_time'], confirmed_time=learning['confirmed_time'],
            censored=learning['censored'], final_DC_mean=metrics['DC_mean'],
            final_DC_worst=metrics['DC_worst'], final_A=metrics['A'],
            outcome=outcome['status'], cleanup_status=outcome.get('cleanup_status', 'not_recorded'),
            cleanup_pending_nodes=outcome.get('cleanup_pending_nodes', []),
            native_summary=native_summary, numerical_replay=replay_summary))
    destination.mkdir()
    result = dict(suite=str(suite.resolve()), mode=status['mode'], rows=rows,
                  paired=paired_summary(rows),
                  scope='Private native loopback; controlled gradient leader; state sharing enabled.',
                  numerical_replay_scope=(
                      'Numerical replay is supplementary; replay failures are retained and do not override '
                      'analyzer-validated native measurements.'))
    (destination / 'report.json').write_text(json.dumps(result, indent=2)+'\n')
    stream = io.StringIO()
    csv_rows = [_csv_row(row) for row in rows]
    writer = csv.DictWriter(stream, fieldnames=list(csv_rows[0]))
    writer.writeheader(); writer.writerows(csv_rows)
    (destination / 'timings.csv').write_text(stream.getvalue())
    lines = ['# Dominant-compute native results', '', result['scope'], '',
             '| Seed | Condition | D/C crossing (s) | Confirmed (s) | Final D/C worst loss | Outcome | Cleanup |',
             '|---|---|---:|---:|---:|---|---|']
    for row in rows:
        crossing = 'censored at 300' if row['censored'] else f"{row['crossing_time']:.2f}"
        confirmed = '—' if row['confirmed_time'] is None else f"{row['confirmed_time']:.2f}"
        lines.append(f"| {row['seed']} | {row['condition']} | {crossing} | {confirmed} | {row['final_DC_worst']:.6g} | {row['outcome']} | {row['cleanup_status']} |")
    lines += ['', 'Crossing requires D and C each below 10% of initial loss for ten consecutive one-second evaluations. '
              'Censored pairs are retained and excluded from arithmetic differences; they are not assigned a crossing time.', '',
              'Per-node submitted-sample throughput divides logged batches by the measured horizon. Up to 32 computed boundary examples per node can be unlogged if the horizon arrives during pacing. '
              'Accepted-sample counts mean accepted into a local gradient accumulator; they do not by themselves establish incorporation into final parameters.', '',
              'Cleanup deadline overruns are retained separately from training completion. The controller terminates and audits owned processes; a completed training run is not evidence of clean native shutdown.', '',
              'A roster omission applies to that reducer view and does not imply that all of A’s work is lost. Native state averaging and recovery remain enabled. '
              'Numerical replay is supplementary; failures are included in the report without invalidating analyzer-validated native measurements.', '',
              'These timings do not establish WAN performance, uncontrolled leader exposure, or a deployment success rate.']
    (destination / 'REPORT.md').write_text('\n'.join(lines)+'\n')
    (destination / 'artifact_inventory.json').write_text(json.dumps(inventory(destination), indent=2)+'\n')
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('suite', type=Path)
    parser.add_argument('destination', type=Path)
    args = parser.parse_args()
    print(json.dumps(build(args.suite, args.destination)['paired'], indent=2))
