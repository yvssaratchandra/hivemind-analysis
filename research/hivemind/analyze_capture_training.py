"""Independent raw validation and bounded pilot report for capture training."""
from __future__ import annotations
import json
from pathlib import Path
import numpy as np
from compute_workload import batch, evaluation, summarize_evaluations
from capture_training_workload import evaluate_models
from capture_training_hooks import CONDITIONS, attacked_prefixes
from analyze_compute_exclusion import (_read_jsonl, _ranges, _group_key, _parts_vector,
    _vec, _same, DIMENSIONS, _validate_opaque_chunk_pairs)


def _verify_group_outputs(events: list[dict[str, Any]], errors: list[str],
                         *, allow_incomplete: bool = False) -> dict[str, Any]:
    grouped: dict[tuple[Any, Any, Any], list[dict[str, Any]]] = {}
    for event in events:
        if event.get("channel") in {"gradient", "state"} and event.get("group_id") is not None:
            grouped.setdefault(_group_key(event), []).append(event)
    checked, weighted_checked = 0, 0
    summaries, incomplete, shutdown_groups = [], [], []
    for key, rows in grouped.items():
        allocs = [row for row in rows if row.get("kind") == "allocation"]
        if not allocs:
            continue
        if len(allocs) != 1:
            errors.append(f"group {key} has {len(allocs)} allocation records")
            continue
        allocation = allocs[0]
        parts = _ranges(allocation, errors)
        local_rosters = [row for row in rows if row.get("kind") == "local_roster"]
        if len(local_rosters) != 1:
            errors.append(f"group {key} lacks one local_roster record")
        else:
            roster = local_rosters[0]
            for field in ("members", "peer_ids_hex", "gathered_hex"):
                if field in allocation and roster.get(field) != allocation.get(field):
                    errors.append(f"group {key} local roster differs from native allocation ({field})")
        local_input_rows = [row for row in rows if row.get("kind") == "aggregate_input"]
        output_rows = [row for row in rows if row.get("kind") == "aggregate_output"]
        if len(local_input_rows) != 1 or len(output_rows) != 1:
            incomplete.append({"node": key[0], "group_id": key[2],
                               "aggregate_inputs": len(local_input_rows),
                               "aggregate_outputs": len(output_rows)})
            if not allow_incomplete:
                errors.append(f"group {key} lacks one aggregate input/output record")
            continue
        local_input = _parts_vector(local_input_rows[0].get("values"))
        aggregate_output = _parts_vector(output_rows[0].get("values"))
        if local_input is None or aggregate_output is None:
            terminal_times = {
                event.get("node"): event.get("monotonic")
                for event in events if event.get("kind") == "training_terminal"
            }
            members = allocation.get("members", [])
            shutdown_only = key[1].endswith("_grad_averager") and bool(members)
            for member in members if isinstance(members, list) else []:
                member_rows = grouped.get((member, key[1], key[2]), [])
                member_alloc = [event for event in member_rows if event.get("kind") == "allocation"]
                starts = [event.get("monotonic") for event in member_rows
                          if event.get("kind") in {"aggregate_input", "allocation"}]
                terminal_time = terminal_times.get(member)
                shutdown_only = shutdown_only and len(member_alloc) == 1 and member_alloc[0].get("weight") == 0
                shutdown_only = shutdown_only and isinstance(terminal_time, (int, float)) and starts
                shutdown_only = shutdown_only and terminal_time < min(starts)
                shutdown_only = shutdown_only and not any(
                    event.get("node") == member
                    and event.get("kind") in {"gradient_loaded", "update_begin", "update_end"}
                    and event.get("monotonic", 0) > terminal_time for event in events)
            if shutdown_only:
                shutdown_groups.append({"node": key[0], "group_id": key[2],
                                        "reason": "post_training_zero_weight_shutdown_group",
                                        "excluded_from_training_proof": True})
                continue
            errors.append(f"group {key} has malformed or non-finite aggregate vectors")
            continue
        deltas = np.zeros(DIMENSIONS, dtype=np.float64)
        covered = np.zeros(DIMENSIONS, dtype=np.int32)
        for row in (item for item in rows if item.get("kind") == "processed_chunk"):
            reducer, part_index = row.get("reducer"), row.get("part_index")
            peer_ranges = parts.get(reducer)
            delta = _vec(row.get("values"), length=(peer_ranges[part_index][1] - peer_ranges[part_index][0])
                         if isinstance(part_index, int) and peer_ranges is not None and 0 <= part_index < len(peer_ranges)
                         else -1)
            if delta is None:
                errors.append(f"group {key} processed chunk does not match its reducer range")
                continue
            start, end = peer_ranges[part_index]
            deltas[start:end] = delta
            covered[start:end] += 1
        for row in (item for item in rows if item.get("kind") == "fallback"):
            reducer = row.get("reducer")
            peer_ranges = parts.get(reducer, [])
            first, last = row.get("registered_parts"), row.get("part_count")
            if not isinstance(first, int) or not isinstance(last, int) or first < 0 or last != len(peer_ranges) or first > last:
                errors.append(f"group {key} fallback range is invalid")
                continue
            coords = []
            for start, end in peer_ranges[first:]:
                coords.extend(range(start, end))
            summaries.append({"node": key[0], "group_id": key[2], "fallback_reducer": reducer,
                              "coordinates": coords})
            for row_chunk in (item for item in rows if item.get("kind") == "processed_chunk"
                              and item.get("reducer") == reducer and item.get("part_index", -1) >= first):
                if not _same(row_chunk.get("values"), np.zeros(len(row_chunk.get("values", [])))):
                    errors.append(f"group {key} fallback chunk was not a zero delta")
        if not np.all(covered == 1):
            incomplete.append({"node": key[0], "group_id": key[2],
                               "processed_coordinates": int(np.count_nonzero(covered))})
            if not allow_incomplete:
                errors.append(f"group {key} processed chunks do not cover every coordinate exactly once")
            continue
        rebuilt = local_input + deltas
        if not _same(rebuilt, aggregate_output):
            errors.append(f"group {key} reducer deltas do not reconstruct aggregate output")
        checked += 1

        # Independently check the coordinates owned by this local reducer from
        # the exact chunks and weights it observed. Failed senders are absent
        # from those chunks and are reported by the native fallback/banning logs.
        node = key[0]
        own_ranges = parts.get(node, [])
        weight = allocation.get("weight")
        inbound = {}
        for row in rows:
            if row.get("kind") == "input_chunk" and row.get("direction") == "received" and row.get("receiver") == node:
                inbound.setdefault((row.get("sender"), row.get("part_index")), row)
        for index, (start, end) in enumerate(own_ranges):
            values = [(local_input[start:end], float(weight))]
            for sender in allocation.get("members", []):
                if sender == node:
                    continue
                incoming = inbound.get((sender, index))
                if incoming is None:
                    continue
                chunk = _vec(incoming.get("values"), length=end - start)
                incoming_weight = incoming.get("weight")
                if chunk is None or not isinstance(incoming_weight, (int, float)) or incoming_weight < 0:
                    errors.append(f"group {key} inbound reducer chunk is malformed")
                    continue
                values.append((chunk, float(incoming_weight)))
            if len(values) < 2 and len(allocation.get("members", [])) > 1:
                banned = any(row.get("kind") == "sender_banned" and row.get("sender") in allocation.get("members", [])
                             for row in rows)
                if not banned:
                    errors.append(f"group {key} reducer input lacks remote chunks and a sender-ban record")
                    continue
            numerator = sum(vector * sample_weight for vector, sample_weight in values)
            denominator = sum(sample_weight for _, sample_weight in values)
            if denominator <= 0:
                errors.append(f"group {key} reducer has nonpositive total weight")
                continue
            expected = numerator / denominator
            if not _same(expected, aggregate_output[start:end]):
                errors.append(f"group {key} native weighted reducer result differs on coordinates {start}:{end}")
            else:
                weighted_checked += end - start
    return {"groups_reconstructed": checked, "weighted_coordinates_checked": weighted_checked,
            "fallbacks": summaries, "incomplete_groups": incomplete,
            "shutdown_zero_weight_groups": shutdown_groups}


def validate_announcement(event, should_remove, errors):
    from analyze_compute_exclusion import _validate_message_payload, _roster_fields, _protobuf_fields
    _validate_message_payload(event, errors, native=True)
    _validate_message_payload(event, errors)
    native = _roster_fields(event, errors, native=True)
    actual = _roster_fields(event, errors)
    wanted = [row for row in native if row[0] != 'A'] if native and should_remove else native
    if actual != wanted:
        errors.append('Opaque announcement rewrite changed the wrong IDs or metadata')
    try:
        native_wire = _protobuf_fields(event['native_payload_hex'])
        actual_wire = _protobuf_fields(event['payload_hex'])
        for field in (1, 2, 3):
            if native_wire.get(field) != actual_wire.get(field):
                errors.append('Announcement code, group ID or suggested leader changed')
    except ValueError as exc:
        errors.append(str(exc))


def analyze_case(path):
    path = Path(path)
    errors = []
    config = json.loads((path/'config.json').read_text())
    if config['treatment'] not in CONDITIONS:
        raise ValueError('Unknown treatment')
    events = []
    for source in sorted(path.glob('events_*.jsonl')):
        events.extend(_read_jsonl(source, errors))
    events.sort(key=lambda e: e['monotonic'])
    holder_rows = _read_jsonl(path/'holder_events.jsonl', errors)
    clock = json.loads((path/'clock_audit.json').read_text())
    if not clock['valid'] or clock['suspension_detected']:
        errors.append('Invalid clock audit')
    shutdown = json.loads((path/'shutdown.json').read_text())
    if not shutdown['safe']:
        errors.append('Owned process survivors')
    outcome = json.loads((path/'outcome.json').read_text())
    if outcome.get('error'):
        errors.append('Worker/controller error: '+outcome['error'])
    identities = json.loads((path/'identities.json').read_text())
    if len(identities)!=45 or not set(config['active']).issubset(set(identities.values())):
        errors.append('Incorrect 45-node topology')
    for event in events:
        if event['kind'] in ('worker_error','observer_error'):
            errors.append(f'{event["kind"]}: {event.get("error")}')
        if event['kind'] == 'leader_advertisement_suppressed':
            errors.append('Honest native leader advertisement was suppressed')
    gp, sp = config['run_id']+'_grad_averager', config['run_id']+'_state_averager'
    attacks = attacked_prefixes(config['treatment'], gp, sp)
    announcements = [e for e in events if e['kind']=='capture_announcement']
    assemblies = [e for e in events if e['kind']=='capture_assembly']
    rosters = [e for e in events if e['kind']=='capture_local_roster']
    outputs = {(e['node'],e['prefix'],e['group_id']) for e in events if e['kind']=='aggregate_output'}
    for event in announcements + rosters:
        native, members = event['native_members'], event['members']
        is_sender = event['kind']=='capture_announcement'
        should_remove = (event['prefix'] in attacks and 'A' in native and
                         (event['sender']=='X' and event['recipient']!='A' if is_sender else
                          event['peer']==event['leader']=='X'))
        wanted = [peer for peer in native if peer!='A'] if should_remove else native
        if event['omitted'] != should_remove or members != wanted:
            errors.append('Capture policy changed an unexpected roster')
        if is_sender:
            validate_announcement(event, should_remove, errors)
            received = [e for e in events if e['kind']=='announcement_received' and
                        e['node']==event['recipient'] and e['prefix']==event['prefix'] and
                        e['group_id']==event['group_id'] and e['sender']==event['sender']]
            for other in received:
                if other['payload_hex']!=event['payload_hex'] or other['members']!=members:
                    errors.append('Received announcement differs from capture sender bytes')
    exposure = {}
    for channel,prefix in (('gradient',gp),('state',sp)):
        native_groups = {(e['group_id'],e['leader']) for e in assemblies if e['prefix']==prefix}
        x_groups = {e['group_id'] for e in assemblies if e['prefix']==prefix and
                    e['peer']==e['leader']=='X' and 'A' in e['members']}
        omissions = [e for e in announcements if e['prefix']==prefix and e['omitted'] and
                     e['recipient'] in ('D','C')]
        exposure[channel] = dict(native_groups=len(native_groups), x_led_with_A=len(x_groups),
            omitted_D_C_announcements=len(omissions),
            completed_omitted_D_C_outputs=sum((e['recipient'],prefix,e['group_id']) in outputs for e in omissions),
            D_C_outputs=sum(node in ('D','C') and p==prefix for node,p,g in outputs),
            filtered_holder_responses=sum(e['kind']=='holder_find' and e.get('channel')==channel and
                                          e.get('filtered',False) for e in holder_rows))
    proof = _verify_group_outputs(events, errors, allow_incomplete=True)
    chunks = _validate_opaque_chunk_pairs(events, errors)
    x,y = evaluation(config['seed'])
    rows = _read_jsonl(path/'evaluations.jsonl', errors)
    if not rows:
        errors.append('No evaluations')
    for row in rows:
        if set(row.get('weights',{})) != set(config['active']):
            errors.append('Missing evaluation model')
            continue
        expected = evaluate_models({n:np.asarray(v,np.float32) for n,v in row['weights'].items()},x,y)
        for n in config['active']:
            if not np.isclose(expected['per_node'][n], row['metrics']['per_node'][n], atol=1e-7,rtol=1e-5):
                errors.append('Evaluation loss differs from saved model')
    initial = evaluate_models({n:np.zeros(48,np.float32) for n in config['active']},x,y)
    thresholds = {n:.1*initial['per_node'][n] for n in ('D','C')}
    for row in rows:
        if row['thresholds'] != thresholds:
            errors.append('Threshold differs from initial loss')
    learning = summarize_evaluations(rows, horizon=config['duration'])
    batches = 0
    for event in events:
        if event['kind']!='microbatch':
            continue
        source = 'B' if event['node']=='X' else event['node']
        bx,by,metadata = batch(config['seed'], source, event['batch_index'])
        if any(event.get(k)!=metadata[k] for k in ('batch_sha256','sample_ids','x_sha256','y_sha256')):
            errors.append('Microbatch provenance mismatch')
        parameters = np.asarray(event['parameters'],np.float32)
        gradient = bx.T@(bx@parameters-by)/32
        if not np.allclose(gradient,event['gradient'],atol=1e-5,rtol=1e-4):
            errors.append('Microbatch gradient mismatch')
        batches+=1
    sgd_steps = 0
    for event in events:
        if event['kind'] == 'sgd_step':
            before, gradient, after = (_vec(event.get(key)) for key in
                                      ('parameters_before', 'gradient', 'parameters_after'))
            if before is None or gradient is None or after is None or not _same(before-.1*gradient,after):
                errors.append('Actual SGD step differs from lr .1 update')
            sgd_steps += 1
    state_routes = [dict(sender=e.get('sender'),recipient=e.get('recipient'),success=e.get('success'))
                    for e in events if e['kind']=='state_download_terminal']
    state_apply = sum(e['kind']=='state_application' and e['phase']=='after' and
                      e['method']=='_apply_averaging_results_' for e in events)
    finite_nodes = {e['node'] for e in events if e['kind']=='training_terminal' and
                    np.isfinite(e['parameters']).all()}
    if outcome['completed'] and finite_nodes!=set(config['active']):
        errors.append('Completed outcome lacks finite training terminals')
    return dict(valid=not errors, errors=errors, treatment=config['treatment'], seed=config['seed'],
                duration=config['duration'], outcome=outcome, learning=learning, exposure=exposure,
                reducer_proof=proof, chunk_pairs=chunks, microbatches_verified=batches, sgd_steps_verified=sgd_steps,
                state_averaging_applications=state_apply, state_downloads=state_routes,
                state_recovery_fetches=sum(e['kind']=='state_recovery_fetch' for e in events),
                clock=clock, shutdown_safe=shutdown['safe'])


def report_suite(path):
    path = Path(path)
    rows = []
    for treatment in CONDITIONS:
        source = path/treatment/'analysis.json'
        if source.exists():
            rows.append(json.loads(source.read_text()))
    lines = ['# Native capture and exclusion training pilot', '',
        'One seed; private loopback; 45 DHT nodes per condition; A has 70% of configured compute. '
        'State downloads remain native. Censored crossings are not assigned the horizon.', '',
        '| Condition | Evidence valid | D/C crossing (s) | Confirmed (s) | D/C final worst loss |',
        '|---|---|---:|---:|---:|']
    for row in rows:
        learning=row['learning']
        loss=learning.get('final_metrics',{}).get('per_node',{})
        worst=max(loss.get('D',float('nan')),loss.get('C',float('nan')))
        crossing='censored' if learning['censored'] else f'{learning["crossing_time"]:.2f}'
        confirmed=learning.get('confirmed_time')
        lines.append(f'| {row["treatment"]} | {row["valid"]} | {crossing} | {confirmed} | {worst:.6g} |')
    lines += ['', '| Condition | Channel | Native groups | X-led with A | D/C omission announcements | Completed omitted D/C outputs |',
              '|---|---|---:|---:|---:|---:|']
    for row in rows:
        for channel,e in row['exposure'].items():
            lines.append(f'| {row["treatment"]} | {channel} | {e["native_groups"]} | {e["x_led_with_A"]} | {e["omitted_D_C_announcements"]} | {e["completed_omitted_D_C_outputs"]} |')
    lines+=['','Actual Optimizer gradient/state prefixes are used. X must win native leadership; honest advertisements are not suppressed. '
             'Missing groups and escapes remain in the exposure denominator. Averages with incomplete streams retain native fallback. '
             'This pilot does not validate simulation timing or establish population success rates.']
    (path/'REPORT.md').write_text('\n'.join(lines)+'\n')
    (path/'report.json').write_text(json.dumps(dict(cases=rows),indent=2)+'\n')
