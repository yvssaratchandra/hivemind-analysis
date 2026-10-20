"""Strict, explicitly synthetic single-stage Agora scenario configuration."""
import math

DEFAULTS = dict(
    protocol="agora", population=100, reducers=4, stage="agorasim",
    compute_seconds=2.0, average_every=20, partitions=20,
    tensor_elements=[1048576]*16, chunk_bytes=2*2**20,
    stream_window_parts=3,
    initial_offset_seconds=10.0, bandwidth_mbps=100.0, latency_seconds=0.025,
    registry_latency_seconds=0.025, matchmaking_seconds=40.0, poll_seconds=1.0,
    stage_refresh_seconds=30.0, stage_ttl_seconds=60.0, setup_timeout_seconds=60.0,
    allreduce_timeout=150.0, sender_timeout=30.0, reducer_timeout=60.0,
    observation_seconds=240.0, horizon_seconds=600.0, target_work=500,
)


def resolve(values):
    unknown = set(values)-set(DEFAULTS)
    if unknown:
        raise ValueError(f"Unknown Agora configuration keys: {sorted(unknown)}")
    c = dict(DEFAULTS, **values)
    if c['protocol'] != 'agora':
        raise ValueError('Expected Agora protocol')
    for k in ('population', 'reducers', 'average_every', 'partitions', 'chunk_bytes', 'target_work', 'stream_window_parts'):
        if type(c[k]) is not int or c[k] <= 0:
            raise ValueError(f'{k} must be a positive integer')
    if not 1 <= c['reducers'] <= c['population'] or c['population'] < 2:
        raise ValueError('Require population >= 2 and 1 <= reducers <= population')
    if not isinstance(c['tensor_elements'], list) or not c['tensor_elements'] or any(type(n) is not int or n < c['partitions'] for n in c['tensor_elements']):
        raise ValueError('tensor_elements must list integer sizes >= partitions')
    if c['chunk_bytes'] % 4:
        raise ValueError('Float32 chunks require a byte size divisible by four')
    if not isinstance(c['stage'], str) or not c['stage'].isalnum():
        raise ValueError('stage must be an alphanumeric namespace without underscores')
    zero_ok = ('initial_offset_seconds', 'latency_seconds', 'registry_latency_seconds')
    positive = ('compute_seconds', 'bandwidth_mbps', 'matchmaking_seconds', 'poll_seconds',
                'stage_refresh_seconds', 'stage_ttl_seconds', 'setup_timeout_seconds',
                'allreduce_timeout', 'sender_timeout', 'reducer_timeout',
                'observation_seconds', 'horizon_seconds')
    for k in zero_ok + positive:
        if isinstance(c[k], bool) or not isinstance(c[k], (float, int)) or not math.isfinite(c[k]) or c[k] < 0 or (k in positive and c[k] == 0):
            raise ValueError(f'{k} must be finite and {"nonnegative" if k in zero_ok else "positive"}')
    if c['stage_refresh_seconds'] >= c['stage_ttl_seconds']:
        raise ValueError('Stage refresh must precede expiry')
    if c['sender_timeout'] >= c['reducer_timeout']:
        raise ValueError('Require sender_timeout < reducer_timeout')
    if c['observation_seconds'] > c['horizon_seconds']:
        raise ValueError('Observation cannot exceed horizon')
    return c


def make_profiles(c, seed):
    from ...randomness import rng
    from ...types import NodeProfile
    return {name: NodeProfile(name, rng(seed, 'identity', name).randbytes(32),
                c['compute_seconds'], c['bandwidth_mbps']*1e6/8,
                c['bandwidth_mbps']*1e6/8, c['latency_seconds'])
            for name in (f'N{i:04d}' for i in range(c['population']))}
