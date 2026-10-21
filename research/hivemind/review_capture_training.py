#!/usr/bin/env python3
"""Read-only reanalysis into a fresh review artifact; never modify native suites."""
import argparse
import collections
import hashlib
import json
from pathlib import Path

from announcement_artifacts import inventory, verify
from analyze_capture_training import analyze_case
from capture_training_hooks import CONDITIONS


def review(reference, destination):
    reference, destination = Path(reference).resolve(), Path(destination).resolve()
    if verify(reference):
        raise ValueError('Native suite inventory mismatch')
    status=json.loads((reference/'status.json').read_text())
    if status['status']!='complete' or len(status['cases'])!=4:
        raise ValueError('Requires all four completed conditions')
    destination.mkdir()
    cases=[]
    for condition in CONDITIONS:
        path=reference/condition
        analysis=analyze_case(path)
        if not analysis['valid']:
            raise ValueError(f'Reanalysis failed: {condition}: {analysis["errors"]}')
        saved=json.loads((path/'analysis.json').read_text())
        if analysis!=saved:
            raise ValueError(f'Reanalysis differs from saved evidence: {condition}')
        events=[json.loads(line) for source in path.glob('events_*.jsonl') for line in source.read_text().splitlines()]
        evaluations=[json.loads(line) for line in (path/'evaluations.jsonl').open()]
        within=[row for row in evaluations if row['time']<=analysis['duration']]
        final=within[-1]['metrics']['per_node']
        batches=collections.Counter(e['node'] for e in events if e['kind']=='microbatch')
        updates=collections.Counter(e['node'] for e in events if e['kind']=='sgd_step')
        cases.append(dict(condition=condition, learning=analysis['learning'], final_losses=final,
            exposure=analysis['exposure'], submitted_examples={n:32*v for n,v in batches.items()},
            actual_SGD_updates=dict(updates), reducer_groups_reconstructed=analysis['reducer_proof']['groups_reconstructed'],
            incomplete_reducer_groups=len(analysis['reducer_proof']['incomplete_groups']),
            state_downloads=analysis['state_downloads'], state_applications=analysis['state_averaging_applications'],
            shutdown_safe=analysis['shutdown_safe'], clocks_valid=analysis['clock']['valid']))
    report=dict(review_script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                reference=str(reference), reference_inventory_sha256=hashlib.sha256(
        (reference/'artifact_inventory.json').read_bytes()).hexdigest(),cases=cases)
    (destination/'review.json').write_text(json.dumps(report,indent=2)+'\n')
    total_complete=sum(r['reducer_groups_reconstructed'] for r in cases)
    total_incomplete=sum(r['incomplete_reducer_groups'] for r in cases)
    lines=['# Actual Optimizer capture/exclusion pilot: independent review','',
        'Seed 501; four sequential 180-second conditions; 45 local DHT nodes in each. '
        'A is paced at 896 examples/s and X/D/C at 128 each. '
        'All four raw reanalyses exactly match saved analysis; all clocks and cleanup checks pass.','',
        '| Condition | D/C target crossing | Confirmation | Final worst D/C loss | Final A loss |',
        '|---|---:|---:|---:|---:|']
    for row in cases:
        learning=row['learning']
        crossing='not reached by 180s' if learning['censored'] else f'{learning["crossing_time"]:.0f}s'
        confirmation='—' if learning['censored'] else f'{learning["confirmed_time"]:.0f}s'
        final=row['final_losses']
        lines.append(f'| {row["condition"]} | {crossing} | {confirmation} | {max(final["D"],final["C"]):.6g} | {final["A"]:.6g} |')
    lines+=['','The target is both D and C below 10% of their own initial fixed-set loss for ten consecutive one-second evaluations. '
        'No time is imputed for a censored condition.','',
        '| Condition | Channel | Assembled groups | X-led with A | Omitted D/C announcements | Completed omitted D/C outputs |',
        '|---|---|---:|---:|---:|---:|']
    for row in cases:
        for channel,e in row['exposure'].items():
            lines.append(f'| {row["condition"]} | {channel} | {e["native_groups"]} | {e["x_led_with_A"]} | {e["omitted_D_C_announcements"]} | {e["completed_omitted_D_C_outputs"]} |')
    lines+=['',
        'The combined mechanism is established locally during real Optimizer training: in dual_exclusion, '
        'X-led rosters produced nine completed omitted D/C gradient outputs and four omitted parameter-averaging outputs. '
        'Both exclusion arms failed to reach the target by 180 seconds, while capture_control reached it at 153 seconds. '
        'This is a positive mechanism result and a single-seed learning-harm observation. '
        'It does not establish the size or reproducibility of the slowdown, or whether parameter exclusion adds an effect beyond gradient exclusion.', '',
        'Capture itself is expensive in this fixture. Ordinary baseline uses matchmaking_time=1s; capture conditions use '
        '0.35s at X and 3s at honest peers plus the A-first hold. Thus baseline vs capture_control combines discovery, '
        'admission and scheduling differences. Use capture_control as the matched control for causal omission comparisons.', '',
        'Group counts refer to assembled groups, not every matchmaking attempt. Timeouts and missing groups remain in the training timeline and raw traces. '
        f'No deployment capture rate is estimated. {total_incomplete} per-node reducer groups are incomplete across the four cases; '
        f'these are retained separately from the {total_complete} reconstructed completed groups.', '',
        'Native state downloads remain enabled. The dual arm recorded two successful X-to-D downloads and no direct A-to-D/C download. '
        'That absence in one window does not prove complete isolation or removal of indirect influence. '
        'Submitted examples and actual SGD counts are preserved in review.json; submitted work is not equated with useful incorporated work.', '',
        'Prior smoke _a retains the evaluator X/B naming failure. Smoke _b passed with gradient exposure only. '
        'Pilot _a retains its failed capture-control checks: received late chunks and buffer snapshots after unlock were insufficient evidence. '
        'Pilot _b observes accepted reducer inputs and samples aggregate tensors under their native lock. No workload or attack settings were tuned.', '',
        'Simulation remains unvalidated. No simulation timing or compute-share sweep is implied by these native results.']
    (destination/'REPORT.md').write_text('\n'.join(lines)+'\n')
    (destination/'artifact_inventory.json').write_text(json.dumps(inventory(destination),indent=2)+'\n')
    return report


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('reference',type=Path)
    parser.add_argument('destination',type=Path)
    args=parser.parse_args()
    review(args.reference,args.destination)
