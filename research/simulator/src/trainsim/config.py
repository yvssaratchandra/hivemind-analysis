"""Strict experiment configuration and synthetic profile construction."""
import json
import math
from pathlib import Path

from .randomness import rng
from .types import NodeProfile

DEFAULTS = dict(
    protocol="hivemind", population=100, profile="controlled",
    populations=[100, 1000], profiles=["controlled", "heterogeneous"],
    seeds=list(range(1, 21)), target_group_size=8, min_group_size=2,
    min_matchmaking_time=5.0, request_timeout=3.0, search_timeout=12.0,
    allreduce_timeout=120.0, sender_timeout=30.0, reducer_timeout=60.0,
    payload_bytes=64*2**20, chunk_bytes=2**19, target_per_node=100,
    observation_seconds=600.0, horizon_seconds=7200.0, initial_bits=None,
    compute_seconds=10.0, bandwidth_mbps=100.0, latency_seconds=0.025,
    initial_offset_seconds=10.0, policies={},
    registry_latency_seconds=0.025,
)


def resolve(values: dict | None = None) -> dict:
    """Resolve defaults and reject misspelled or incompatible settings."""
    if values is None:
        values = {}
    if not isinstance(values, dict) or any(not isinstance(k, str) for k in values):
        raise ValueError("Configuration must be a JSON object with string keys")
    if values.get("protocol") == "agora":
        from .protocols.agora.config import resolve as resolve_agora
        return resolve_agora(values)
    unknown = set(values)-set(DEFAULTS)
    if unknown:
        raise ValueError(f"Unknown configuration keys: {sorted(unknown)}")
    c = dict(DEFAULTS, **values)
    for key in ("population", "target_group_size", "min_group_size", "payload_bytes", "chunk_bytes"):
        if type(c[key]) is not int or c[key] <= 0:
            raise ValueError(f"{key} must be a positive integer")
    for key in ("min_matchmaking_time", "request_timeout", "search_timeout", "allreduce_timeout",
                "sender_timeout", "reducer_timeout", "target_per_node", "horizon_seconds",
                "compute_seconds", "bandwidth_mbps"):
        if isinstance(c[key], bool) or not isinstance(c[key], (int, float)) or not math.isfinite(c[key]) or c[key] <= 0:
            raise ValueError(f"{key} must be finite and positive")
    for key in ("observation_seconds", "latency_seconds", "initial_offset_seconds", "registry_latency_seconds"):
        if isinstance(c[key], bool) or not isinstance(c[key], (int, float)) or not math.isfinite(c[key]) or c[key] < 0:
            raise ValueError(f"{key} must be finite and nonnegative")
    if c["protocol"] != "hivemind":
        raise ValueError("Only the hivemind adapter is supported")
    if c["profile"] not in ("controlled", "heterogeneous"):
        raise ValueError("Unknown resource profile")
    g = c["target_group_size"]
    if g & (g-1) or not 2 <= c["min_group_size"] <= g:
        raise ValueError("target must be a power of two; 2 <= minimum <= target")
    if not c["request_timeout"] < c["min_matchmaking_time"] < c["search_timeout"]:
        raise ValueError("Require request_timeout < min_matchmaking_time < search_timeout")
    if c["reducer_timeout"] <= c["sender_timeout"]:
        raise ValueError("reducer_timeout must exceed sender_timeout (native constraint)")
    if c["payload_bytes"] % 4 or c["chunk_bytes"] % 4:
        raise ValueError("Float32 payload and chunk sizes must be divisible by four")
    if c["chunk_bytes"] > c["payload_bytes"]:
        raise ValueError("chunk_bytes cannot exceed payload_bytes")
    if c["observation_seconds"] > c["horizon_seconds"]:
        raise ValueError("observation window cannot exceed horizon")
    if c["initial_bits"] is not None and (type(c["initial_bits"]) is not int or c["initial_bits"] < 0):
        raise ValueError("initial_bits must be a nonnegative length or null")
    if not isinstance(c["policies"], dict):
        raise ValueError("policies must map node IDs to policy configurations")
    known_nodes = {f"N{i:04d}" for i in range(c["population"])}
    if set(c["policies"])-known_nodes:
        raise ValueError("Policy names must refer to existing node IDs")
    for node, policy in c["policies"].items():
        if not isinstance(policy, dict) or policy.get("kind", "honest") not in ("honest", "fixture"):
            raise ValueError(f"Invalid policy configuration for {node}")
        allowed = {"kind", "delay_transfer_seconds", "reject_join_from", "prefer_candidates", "delay_announcement_seconds"}
        if set(policy)-allowed:
            raise ValueError(f"Unknown policy fields for {node}")
        for key in ("delay_transfer_seconds", "delay_announcement_seconds"):
            value = policy.get(key, 0.)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
                raise ValueError(f"{node}.{key} must be finite and nonnegative")
        for key in ("reject_join_from", "prefer_candidates"):
            value = policy.get(key, [])
            if not isinstance(value, list) or any(not isinstance(n, str) or n not in known_nodes for n in value):
                raise ValueError(f"{node}.{key} must list existing node IDs")
    if not isinstance(c["seeds"], list) or not c["seeds"] or any(type(s) is not int or s < 0 for s in c["seeds"]):
        raise ValueError("seeds must be a nonempty list of nonnegative integers")
    if not c["populations"] or any(type(n) is not int or n < 2 for n in c["populations"]):
        raise ValueError("populations must contain integers >= 2")
    if not c["profiles"] or any(p not in ("controlled", "heterogeneous") for p in c["profiles"]):
        raise ValueError("Unknown sweep profile")
    for key in ("seeds", "populations", "profiles"):
        if not isinstance(c[key], list) or len(set(c[key])) != len(c[key]):
            raise ValueError(f"{key} must be a list without duplicates")
    return c


def load(path: str | Path) -> dict:
    """Read a JSON configuration with no executable content."""
    return resolve(json.loads(Path(path).read_text()))


def make_profiles(c: dict, seed: int) -> dict[str, NodeProfile]:
    """Build reproducible, independently shuffled synthetic resource classes."""
    n = c["population"]
    names = [f"N{i:04d}" for i in range(n)]
    bits = c["initial_bits"]
    if bits is None:
        bits = max(0, math.floor(math.log2(n/(4*c["target_group_size"]))))
    compute = [c["compute_seconds"]]*n
    bandwidth = [c["bandwidth_mbps"]]*n
    latency = [c["latency_seconds"]]*n
    if c["profile"] == "heterogeneous":
        compute = [[5., 10., 20.][i % 3] for i in range(n)]
        bandwidth = [[50., 100., 200.][i % 3] for i in range(n)]
        latency = [[.010, .025, .050][i % 3] for i in range(n)]
        for tag, values in (("compute", compute), ("bandwidth", bandwidth), ("latency", latency)):
            rng(seed, "profiles", tag).shuffle(values)
    return {name: NodeProfile(
        node_id=name, peer_id=rng(seed, "identity", name).randbytes(32),
        compute_seconds=compute[i], upload_bps=bandwidth[i]*1e6/8,
        download_bps=bandwidth[i]*1e6/8, latency_seconds=latency[i],
        initial_group_bits=format(rng(seed, "bits", name).randrange(2**bits), f"0{bits}b") if bits else "",
    ) for i, name in enumerate(names)}


def make_labels(profiles: dict[str, NodeProfile], seed: int) -> dict[str, set[str]]:
    """Nested passive controlled-node labels; never supplied to honest nodes."""
    names = sorted(profiles)
    rng(seed, "labels").shuffle(names)
    return {f"{p}pct": set(names[:len(names)*p//100]) for p in (5, 10, 20)}
