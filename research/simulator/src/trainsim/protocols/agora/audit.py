"""Independent full-credit reconstruction from reducer receipts and delivery events.

This deliberately does not call Accounting or trust application cardinality totals.
It follows references from a terminal application back to delivered reducer parts
and their originating snapshot/component identities.
"""
import gzip
import hashlib
import json
from collections import Counter


def audit_trace(path, config):
    snapshots = {}
    receipts = {}
    delivered = {}
    credits = {}
    start = None
    offsets = {}
    steps = Counter()
    applications = 0
    declaration_errors = []
    with gzip.open(path, 'rt') as stream:
        for line in stream:
            e = json.loads(line)
            kind, actor, time = e['kind'], e.get('actor', ''), e['time']
            if kind == 'measurement_started':
                start = time
            elif kind == 'agora_initial_compute_offset':
                offsets[actor] = e['offset_seconds']
            elif kind == 'agora_local_step':
                steps[actor] += 1
                expected = start + offsets[actor] + e['local_step']*config['compute_seconds']
                if abs(time-expected) > 1e-8:
                    raise RuntimeError(f'Local compute cadence mismatch for {actor}')
            elif kind == 'agora_snapshot_created':
                sizes = e['tensor_cardinalities']
                cap = config['chunk_bytes']//4
                parts = {}
                for tensor, count in enumerate(sizes):
                    for chunk, offset in enumerate(range(0, count, cap)):
                        parts[f't{tensor}:c{chunk}'] = min(cap, count-offset)
                if (set(parts) != set(e['expected_component_ids'])
                        or len(parts) != len(e['expected_component_ids'])
                        or sum(parts.values()) != e['selected_cardinality']):
                    declaration_errors.append([actor, e['epoch']])
                snapshots[actor, e['epoch']] = dict(e, parts=parts)
            elif kind == 'agora_sparse_part_receipt':
                key = (actor, e['epoch'], e['component_id'])
                if key in receipts:
                    raise RuntimeError(f'Duplicate reducer receipt {key}')
                receipts[key] = e
                # The reducer applies its own averaged part locally.
                delivered[actor, e['epoch'], e['component_id']] = key
            elif kind == 'agora_part_result_received':
                key = (e['reducer'], e['reducer_epoch'], e['reducer_component_id'])
                if key not in receipts:
                    raise RuntimeError('Result was delivered without its reducer receipt')
                delivered[actor, e['recipient_epoch'], e['component_id']] = key
            elif kind == 'agora_sparse_application':
                applications += 1
                origin, epoch = e['origin'], e['epoch']
                source = snapshots.get((origin, epoch))
                recipient = snapshots.get((actor, e['recipient_epoch']))
                if source is None or recipient is None or actor == origin:
                    continue
                # The destination must apply the values to the same selected coordinates.
                if (source['epoch'], source['layout'], source['partition']) != (
                        recipient['epoch'], recipient['layout'], recipient['partition']):
                    continue
                valid = True
                for component, size in source['parts'].items():
                    key = delivered.get((actor, e['recipient_epoch'], component))
                    receipt = receipts.get(key)
                    if receipt is None or receipt['part_cardinality'] != size or receipt.get('fallback'):
                        valid = False
                        break
                    contribution = next((v for v in receipt['contributors'] if v['node_id'] == origin), None)
                    if (contribution is None or contribution.get('weight', 0) <= 0
                            or contribution.get('source_component_cardinality') != size):
                        valid = False
                        break
                    expected = (source['snapshot_id'], epoch, source['layout'], source['partition'], component)
                    observed = (contribution['snapshot_id'], contribution['epoch'], contribution['layout'],
                                contribution['partition'], contribution.get('source_component_id'))
                    if observed != expected:
                        valid = False
                        break
                if valid and source['parts']:
                    unit = (origin, epoch, source['layout'], source['partition'])
                    credits.setdefault(unit, time)
    if declaration_errors:
        raise RuntimeError(f'Invalid snapshot component declarations: {declaration_errors[:3]}')
    times = sorted(credits.values())
    target = config['target_work']
    units = sorted([[str(value) for value in key] for key in credits])
    return dict(
        accepted_work=len(credits),
        target_time=times[target-1]-start if len(times) >= target and start is not None else None,
        accepted_work_at_observation=sum(t < start+config['observation_seconds'] for t in times) if start is not None else None,
        measurement_start=start, source_snapshot_count=len(snapshots),
        reducer_part_receipts=len(receipts), terminal_application_rows=applications,
        credited_unit_sha256=hashlib.sha256(json.dumps(units, sort_keys=True).encode()).hexdigest(),
        per_origin=dict(sorted(Counter(key[0] for key in credits).items())),
        initial_offsets=offsets, local_steps=dict(steps),
        evidence='independent reconstruction from delivered reducer parts and source snapshot/component identities',
    )
