"""Build paired native timing tables from verified dominant-compute suites."""
from __future__ import annotations
import argparse
import csv
import io
import json
from pathlib import Path
from statistics import median
from announcement_artifacts import inventory, verify


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
        rows.append(dict(seed=analysis['seed'], condition=analysis['condition'],
            crossing_time=learning['crossing_time'], confirmed_time=learning['confirmed_time'],
            censored=learning['censored'], final_DC_mean=metrics['DC_mean'],
            final_DC_worst=metrics['DC_worst'], final_A=metrics['A'],
            outcome=json.loads((path / 'outcome.json').read_text())['status']))
    destination.mkdir()
    result = dict(suite=str(suite.resolve()), mode=status['mode'], rows=rows,
                  paired=paired_summary(rows), scope='Private native loopback; controlled gradient leader; state sharing enabled.')
    (destination / 'report.json').write_text(json.dumps(result, indent=2)+'\n')
    stream = io.StringIO()
    writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
    writer.writeheader(); writer.writerows(rows)
    (destination / 'timings.csv').write_text(stream.getvalue())
    lines = ['# Dominant-compute native results', '', result['scope'], '',
             '| Seed | Condition | D/C crossing (s) | Confirmed (s) | Final D/C worst loss | Outcome |',
             '|---|---|---:|---:|---:|---|']
    for row in rows:
        crossing = 'censored at 300' if row['censored'] else f"{row['crossing_time']:.2f}"
        confirmed = '—' if row['confirmed_time'] is None else f"{row['confirmed_time']:.2f}"
        lines.append(f"| {row['seed']} | {row['condition']} | {crossing} | {confirmed} | {row['final_DC_worst']:.6g} | {row['outcome']} |")
    lines += ['', 'Crossing requires D and C each below 10% of initial loss for ten consecutive one-second evaluations. '
              'Censored pairs are retained and excluded from arithmetic differences; they are not assigned a crossing time.', '',
              'Gradient exclusion does not imply that all of A’s work is lost: native state averaging and recovery remain enabled. '
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
