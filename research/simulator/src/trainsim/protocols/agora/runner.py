"""Isolated Agora orchestration; existing Hivemind results retain their format."""
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path
import resource
import subprocess
import tarfile
import time

from ...engine import Engine
from ...network import Network
from .config import make_profiles, resolve


def _value(value):
    if is_dataclass(value):
        return _value(asdict(value))
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, dict):
        return {str(k): _value(v) for k, v in value.items()}
    if isinstance(value, set):
        return [_value(v) for v in sorted(value, key=repr)]
    if isinstance(value, (list, tuple)):
        return [_value(v) for v in value]
    return value


def source_manifest():
    from ...runner import source_manifest as shared_manifest, ROOT, REPOSITORY
    manifest = shared_manifest()
    native = REPOSITORY/'research/agora/upstream'
    paths = list((native/'agora_server/src/agora_server/core/all_reduce').rglob('*.py'))
    paths += list((native/'agora_server/src/agora_server/core/averaging').rglob('*.py'))
    paths += list((ROOT/'fixtures').glob('agora*.json'))
    fixture = ROOT/'fixtures/agora_native.json'
    if fixture.exists():
        evidence = json.loads(fixture.read_text())
        for relative, expected in evidence.get('provenance', {}).get('source_sha256', {}).items():
            source = REPOSITORY/relative
            if hashlib.sha256(source.read_bytes()).hexdigest() != expected:
                raise RuntimeError(f'Native fixture source has changed: {relative}')
            paths.append(source)
    paths += list((ROOT/'tests').rglob('test_agora*.py'))
    paths += [p for p in (ROOT/'generate_agora_fixture.py', ROOT/'run_agora_pilot.py',
                          REPOSITORY/'research/agora/baseline_8b.py') if p.exists()]
    for p in sorted(paths):
        manifest['source_hashes'][str(p.relative_to(REPOSITORY))] = hashlib.sha256(p.read_bytes()).hexdigest()
    if native.exists():
        manifest['agora_revision'] = subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=native,
            capture_output=True, text=True, check=True).stdout.strip()
    manifest['assumptions'] = [
        'single stage with fixed assignment; no Authorizer or pipeline model',
        'all nodes honest; no churn, retry or attack',
        'synthetic equal endpoint capacities; abstract timed registry, not replicated DHT',
        'continuous local compute; averaging opportunities at configured step intervals, skipped if busy (harness convention)',
        'harness convention: first averaging round is 1; sparse selection uses round modulo partitions, step keys use local step',
        'symbolic sparse snapshots; no numerical gradients or convergence measurement',
        'node-local roster and reducer views; no globally reconciled roster',
        'bounded synthetic stream window; native prefetch=3 is not an end-to-end wire limit',
        'zero compute cost for symbolic reduction/compression; network bytes and delays are modeled',
        'allreduce-wide deadline follows the native baseline harness; per-stream timeouts model the runner',
    ]
    return manifest


def run(config, seed, label, results_root=None):
    from ...runner import ROOT, REPOSITORY, _safe_label, write_json
    from .adapter import AgoraAdapter
    from .accounting import Accounting
    c = resolve(config)
    if type(seed) is not int or seed < 0:
        raise ValueError('seed must be a nonnegative integer')
    path = (Path(results_root) if results_root is not None else ROOT/'results')/_safe_label(label)
    path.mkdir(parents=True, exist_ok=False)
    profiles = make_profiles(c, seed)
    write_json(path/'resolved_config.json', dict(c, seed=seed))
    write_json(path/'profiles.json', {n: dict(_value(p), role='NODE' if i < c['reducers'] else 'CLIENT')
        for i, (n, p) in enumerate(profiles.items())})
    manifest = dict(source_manifest(), seed=seed, label=label, status='running',
                    started_utc=datetime.now(timezone.utc).isoformat())
    write_json(path/'manifest.json', manifest)
    with tarfile.open(path/'sources.tar.gz', 'w:gz') as archive:
        for relative in manifest['source_hashes']:
            archive.add(REPOSITORY/relative, arcname=relative, recursive=False)
    accounting = Accounting(c)
    engine = Engine(seed)
    trace = gzip.open(path/'events.jsonl.gz', 'wt')
    sequence = 0
    origin = None
    def emit(t, kind, actor='', **data):
        nonlocal sequence, origin
        event = dict(time=t, kind=kind, actor=actor, sequence=sequence, **_value(data))
        sequence += 1
        trace.write(json.dumps(event, sort_keys=True, allow_nan=False)+'\n')
        accounting.consume(event)
        if kind == 'measurement_started':
            origin = t
            engine.schedule(c['observation_seconds'], engine.emit, 'observation_boundary')
            engine.schedule(c['horizon_seconds'], engine.request_stop)
        if origin is not None and t >= origin+c['observation_seconds'] and accounting.target_time is not None:
            engine.request_stop()
    engine.sink = emit
    network = Network(engine, profiles, c['registry_latency_seconds'])
    adapter = AgoraAdapter(engine, network, profiles, c, seed)
    started = time.perf_counter()
    error = None
    status = 'error'
    def check_setup():
        if origin is None:
            raise RuntimeError('Stage startup exceeded the configured setup deadline')
    try:
        engine.emit('run_started', config=c, seed=seed)
        adapter.start()
        engine.schedule(c['setup_timeout_seconds'], check_setup)
        engine.run(c['setup_timeout_seconds']+c['horizon_seconds']+1)
        if origin is None:
            raise RuntimeError('Stage startup did not complete before the setup deadline')
        status = 'completed' if accounting.summary()['target_time'] is not None else 'censored'
    except KeyboardInterrupt:
        status = 'interrupted'
    except Exception as exc:
        status = 'error'
        error = dict(type=type(exc).__name__, message=str(exc))
    finally:
        engine.request_stop()
        try:
            adapter.stop()
        except Exception as exc:
            status = 'error'
            error = dict(type=type(exc).__name__, message=f'Cleanup: {exc}')
        try:
            engine.emit('run_finished', status=status, error=error)
        finally:
            trace.close()
    summary = dict(accounting.summary(), status=status, error=error, seed=seed,
        population=c['population'], final_time=engine.now,
        elapsed_wall_seconds=time.perf_counter()-started,
        process_peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        trace_bytes=(path/'events.jsonl.gz').stat().st_size)
    write_json(path/'summary.json', summary)
    manifest.update(status=status, error=error, finished_utc=datetime.now(timezone.utc).isoformat())
    write_json(path/'manifest.json', manifest)
    analyze(path)
    if error:
        raise RuntimeError(f'Agora run failed; retained evidence at {path}: {error}')
    if status == 'interrupted':
        raise KeyboardInterrupt
    return path


def analyze(path):
    from ...runner import write_json
    from .accounting import Accounting
    from .report import render
    from .audit import audit_trace
    path = Path(path)
    manifest = json.loads((path/'manifest.json').read_text())
    with tarfile.open(path/'sources.tar.gz', 'r:gz') as archive:
        for relative, expected in manifest['source_hashes'].items():
            entry = archive.extractfile(relative)
            if entry is None or hashlib.sha256(entry.read()).hexdigest() != expected:
                raise RuntimeError(f'Source snapshot checksum mismatch: {relative}')
    c = json.loads((path/'resolved_config.json').read_text())
    replay = Accounting(c)
    count = 0
    with gzip.open(path/'events.jsonl.gz', 'rt') as events:
        for line in events:
            replay.consume(json.loads(line))
            count += 1
    offline = replay.summary()
    online = json.loads((path/'summary.json').read_text())
    for key in offline:
        if offline[key] != online.get(key):
            raise RuntimeError(f'Agora online/replay mismatch: {key}')
    audit = audit_trace(path/'events.jsonl.gz', c)
    for key in ('accepted_work', 'target_time', 'accepted_work_at_observation', 'measurement_start'):
        if audit[key] != offline[key]:
            raise RuntimeError(f'Agora independent receipt audit mismatch: {key}')
    units = sorted([[str(row[k]) for k in ('origin', 'epoch', 'layout', 'partition')]
                    for row in offline['accepted_contributions']])
    digest = hashlib.sha256(json.dumps(units, sort_keys=True).encode()).hexdigest()
    if digest != audit['credited_unit_sha256']:
        raise RuntimeError('Agora independent receipt audit credited different snapshot identities')
    audit['agrees_with_online_and_replay'] = True
    write_json(path/'receipt_audit.json', audit)
    result = dict(summary=offline, online_replay_agree=True, events=count,
        trace_sha256=hashlib.sha256((path/'events.jsonl.gz').read_bytes()).hexdigest(),
        source_snapshot_verified=True, independent_receipt_audit_agrees=True)
    write_json(path/'replay.json', result)
    render(path)
    return result
