"""Configuration and independent synthetic assignments."""
import pytest
from trainsim.config import make_labels, make_profiles, resolve


def test_defaults_and_resources():
    c = resolve()
    p = make_profiles(c, 1)
    assert len(p) == 100
    assert p["N0000"].upload_bps == 12_500_000
    assert len(p["N0000"].initial_group_bits) == 1
    assert p == make_profiles(c, 1)
    labels = make_labels(p, 1)
    assert len(labels["5pct"]) == 5
    assert labels["5pct"] < labels["10pct"] < labels["20pct"]


@pytest.mark.parametrize("changes", [
    {"sender_timeout": 30, "reducer_timeout": 30},
    {"search_timeout": 3}, {"chunk_bytes": 5}, {"min_group_size": 1},
    {"profile": "madeup"}, {"initial_bits": -1}, {"typo": 0},
    {"horizon_seconds": float("nan")}, {"request_timeout": True},
    {"seeds": [1, 1]}, {"latency_seconds": True},
])
def test_reject_invalid(changes):
    with pytest.raises(ValueError):
        resolve(changes)


def test_balanced_independent_classes():
    p = make_profiles(resolve({"profile": "heterogeneous"}), 1)
    for field in ("compute_seconds", "upload_bps", "latency_seconds"):
        counts = [sum(getattr(node, field) == value for node in p.values())
                  for value in {getattr(node, field) for node in p.values()}]
        assert max(counts)-min(counts) <= 1


def test_labels_independent_of_resources():
    for seed in range(3):
        a = make_profiles(resolve(), seed)
        b = make_profiles(resolve({"profile": "heterogeneous"}), seed)
        assert make_labels(a, seed) == make_labels(b, seed)
        assert {n: p.peer_id for n, p in a.items()} == {n: p.peer_id for n, p in b.items()}


@pytest.mark.parametrize("value", [[], False, "settings", {1: 2}])
def test_configuration_must_be_an_object(value):
    with pytest.raises(ValueError, match="JSON object"):
        resolve(value)
