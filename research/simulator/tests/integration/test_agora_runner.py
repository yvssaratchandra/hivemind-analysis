"""Small scenario checks; the 100-node pilot is separate research evidence."""
import gzip
import json
import pytest
from trainsim.config import resolve


def test_agora_configuration_is_separate():
    c = resolve({'protocol': 'agora'})
    assert c['population'] == 100 and c['reducers'] == 4
    assert c['tensor_elements'] == [1048576]*16
    assert resolve()['protocol'] == 'hivemind'
    for bad in ({'reducers': 101}, {'stage': 'bad_prefix'}, {'chunk_bytes': 3},
                {'partitions': 0}, {'bandwidth_mbps': float('nan')}, {'policies': {}}):
        with pytest.raises(ValueError):
            resolve(dict(protocol='agora', **bad))


def test_agora_small_run_replay_and_determinism(tmp_path):
    from trainsim.runner import run, analyze
    c = resolve(dict(protocol='agora', population=8, reducers=4,
        tensor_elements=[100]*5, partitions=20, compute_seconds=.1,
        initial_offset_seconds=.2, average_every=20, poll_seconds=.1,
        observation_seconds=7, horizon_seconds=15, target_work=8,
        latency_seconds=.001, registry_latency_seconds=.001))
    a = run(c, 1, 'agora_a', tmp_path)
    b = run(c, 1, 'agora_b', tmp_path)
    sa = json.loads((a/'summary.json').read_text())
    assert sa['status'] == 'completed'
    assert sa['accepted_work'] == 24
    assert sa['measurement_start'] > 0
    assert analyze(a)['online_replay_agree']
    with gzip.open(a/'events.jsonl.gz', 'rt') as af, gzip.open(b/'events.jsonl.gz', 'rt') as bf:
        decoded = af.read()
        assert decoded == bf.read()
    events = [json.loads(line) for line in decoded.splitlines()]
    offsets = {e['actor']: e['offset_seconds'] for e in events if e['kind'] == 'agora_initial_compute_offset'}
    assert len(set(offsets.values())) == 8
    assert all(0 <= value <= c['initial_offset_seconds'] for value in offsets.values())
    for event in events:
        if event['kind'] == 'agora_local_step':
            expected = sa['measurement_start'] + offsets[event['actor']] + event['local_step']*c['compute_seconds']
            assert event['time'] == pytest.approx(expected)
    first = next(e for e in events if e['kind'] == 'agora_snapshot_created')
    assert first['epoch'] == first['partition'] == 1
    finished = [e for e in events if e['kind'] == 'agora_aggregate_finished']
    assert len(finished) == 24
    assert all(e['status'] == 'complete' for e in finished)
    assert not any(e['kind'] == 'round_skipped' for e in events)
    assert (a/'DASHBOARD.html').exists()
    with pytest.raises(FileExistsError):
        run(c, 1, 'agora_a', tmp_path)
    manifest_path = a/'manifest.json'
    manifest = json.loads(manifest_path.read_text())
    key = next(iter(manifest['source_hashes']))
    manifest['source_hashes'][key] = '0'*64
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(RuntimeError, match='checksum mismatch'):
        analyze(a)


def test_agora_setup_failure_keeps_evidence(tmp_path):
    from trainsim.runner import run
    c = resolve(dict(protocol='agora', population=2, reducers=1,
        tensor_elements=[20], latency_seconds=100, setup_timeout_seconds=.1,
        observation_seconds=1, horizon_seconds=2))
    with pytest.raises(RuntimeError, match='Agora run failed'):
        run(c, 1, 'setup_failure', tmp_path)
    result = json.loads((tmp_path/'setup_failure/summary.json').read_text())
    assert result['status'] == 'error'
    assert result['measurement_start'] is None
    assert result['accepted_work'] == 0
