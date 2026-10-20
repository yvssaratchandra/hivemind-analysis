"""Independent evidence audit rejects plausible but unsupported credit claims."""
import gzip
import json
import pytest
from trainsim.protocols.agora.audit import audit_trace


@pytest.mark.parametrize('fault', [None, 'missing_delivery', 'wrong_epoch', 'wrong_component'])
def test_receipt_audit_requires_delivered_exact_snapshot_parts(tmp_path, fault):
    events = [dict(time=0, kind='measurement_started', actor='')]
    for node in ('A', 'R'):
        events.append(dict(time=1, kind='agora_snapshot_created', actor=node, epoch=1,
            layout='L', partition=1, tensor_cardinalities=[8], selected_cardinality=8,
            snapshot_id=f'{node}:1', expected_component_ids=['t0:c0', 't0:c1']))
    for part in range(2):
        component = f't0:c{part}'
        contribution = dict(node_id='A', snapshot_id='A:1', epoch=1, layout='L',
                            partition=1, source_component_id=component,
                            source_component_cardinality=4, weight=1)
        if part == 1 and fault == 'wrong_epoch':
            contribution['epoch'] = 2
        if part == 1 and fault == 'wrong_component':
            contribution['source_component_id'] = 't0:c0'
        events.append(dict(time=2, kind='agora_sparse_part_receipt', actor='D', epoch=1,
            component_id=component, part_cardinality=4, fallback=False, contributors=[contribution]))
        if not (part == 1 and fault == 'missing_delivery'):
            events.append(dict(time=3, kind='agora_part_result_received', actor='R', recipient_epoch=1,
                component_id=component, reducer='D', reducer_epoch=1, reducer_component_id=component))
    # Application claims full coverage twice. Only the raw receipts establish it.
    for _ in range(2):
        events.append(dict(time=4, kind='agora_sparse_application', actor='R', origin='A',
            epoch=1, recipient_epoch=1, status='complete', applied_cardinality=8))
    path = tmp_path/'events.jsonl.gz'
    with gzip.open(path, 'wt') as out:
        for event in events:
            out.write(json.dumps(event)+'\n')
    result = audit_trace(path, dict(chunk_bytes=16, target_work=1, observation_seconds=5, compute_seconds=2))
    assert result['accepted_work'] == (1 if fault is None else 0)
    assert result['target_time'] == (4 if fault is None else None)
