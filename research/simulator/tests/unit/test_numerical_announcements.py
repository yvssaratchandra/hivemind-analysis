"""Analytic protocol checks; these do not substitute for native validation."""
import pytest
np = pytest.importorskip("numpy")

from trainsim.policies.announcements import AnnouncementPolicy
from trainsim.policies.base import PolicyObservation
from trainsim.protocols.hivemind.numerical import aggregate, announced_views


def test_two_peer_coordinate_swap():
    inputs = {"L": [0., 100.], "F": [10., 1000.]}
    normal = aggregate(inputs, {"L": ("L", "F"), "F": ("L", "F")}, chunk_elements=1)
    for value in normal.values.values():
        np.testing.assert_array_equal(value, [5., 550.])
    wrong = aggregate(inputs, {"L": ("L", "F"), "F": ("F", "L")}, chunk_elements=1)
    np.testing.assert_array_equal(wrong.values["L"], [500., 55.])
    np.testing.assert_array_equal(wrong.values["F"], [55., 500.])


def test_membership_retains_victim_input_and_records_ranges():
    nodes = ("B", "D", "A", "C")
    inputs = {node: np.arange(48, dtype=np.float32) + i * 100 for i, node in enumerate(nodes)}
    views = announced_views(nodes, "B", AnnouncementPolicy("membership"))
    result = aggregate(inputs, views)
    mean = (inputs["B"] + inputs["D"] + inputs["C"]) / 3
    for node in ("B", "D", "C"):
        np.testing.assert_allclose(result.values[node], mean, atol=1e-5)
        assert result.fallback_ranges[node] == []
    np.testing.assert_array_equal(result.values["A"], inputs["A"])
    assert sum(end-start for start, end in result.fallback_ranges["A"]) == 36
    assert set(result.banned_senders["A"]) == {"B", "D", "C"}
    assert len([e for e in result.trace if e.get("kind") == "processed_chunk" and e["node"] == "A"]) == 12


def test_coherent_omission_has_same_vectors_as_inconsistent_membership():
    nodes = ("B", "D", "A", "C")
    inputs = {node: np.full(48, i, dtype=np.float32) for i, node in enumerate(nodes)}
    attack = aggregate(inputs, announced_views(nodes, "B", AnnouncementPolicy("membership")))
    benign = aggregate(inputs, announced_views(("B", "D", "C"), "B", AnnouncementPolicy("honest")))
    for node in nodes:
        np.testing.assert_array_equal(attack.values[node], benign.values[node])
    assert benign.fallback_ranges["A"] == []


def test_policy_requires_leadership_and_preserves_metadata():
    proposal = {"members": ["B", "D", "A", "C"], "work_ids": ["b", "d", "a", "c"]}
    policy = AnnouncementPolicy("ordering")
    changed = policy.decide(PolicyObservation("B", "group_announcement", {"leader": "B", "recipient": "A"}), proposal)
    assert changed == {"members": ["A", "D", "B", "C"], "work_ids": ["a", "d", "b", "c"]}
    assert policy.decide(PolicyObservation("D", "group_announcement", {"leader": "B", "recipient": "A"}), proposal) == proposal
    assert proposal["members"] == ["B", "D", "A", "C"]


def test_arrival_order_changes_neither_membership_nor_coordinates():
    nodes = ("B", "D", "A", "C")
    inputs = {node: np.arange(48, dtype=np.float32) + i for i, node in enumerate(nodes)}
    for kind in ("honest", "ordering", "membership"):
        views = announced_views(nodes, "B", AnnouncementPolicy(kind))
        forward = aggregate(inputs, views)
        reverse = aggregate(inputs, views, arrival_order=list(reversed(nodes)))
        for node in nodes:
            np.testing.assert_allclose(forward.values[node], reverse.values[node], atol=1e-5)


def test_unsupported_stream_lengths_fail_explicitly():
    with pytest.raises(ValueError, match="stream lengths"):
        aggregate({"L": [0., 1., 2.], "F": [3., 4., 5.]},
                  {"L": ("L", "F"), "F": ("F", "L")}, chunk_elements=1)


def test_weighted_aggregation_matches_independent_mean():
    inputs = {"A": [1., 2.], "B": [11., 20.], "C": [4., 8.]}
    views = {node: ("A", "B", "C") for node in inputs}
    weights = {"A": 2., "B": 1., "C": 3.}

    result = aggregate(inputs, views, chunk_elements=1, weights=weights)

    expected = (2 * np.asarray(inputs["A"]) + np.asarray(inputs["B"])
                + 3 * np.asarray(inputs["C"])) / 6
    for value in result.values.values():
        np.testing.assert_allclose(value, expected, atol=1e-6)


def test_omitted_high_weight_node_does_not_affect_remaining_peers():
    nodes = ("B", "D", "A", "C")
    inputs = {node: np.full(8, value, dtype=np.float32)
              for node, value in zip(nodes, (10., 20., 1000., 40.))}
    views = announced_views(nodes, "B", AnnouncementPolicy("membership"))
    weights = {"A": 7., "B": 1., "D": 1., "C": 1.}

    result = aggregate(inputs, views, weights=weights)

    expected = np.full(8, (10. + 20. + 40.) / 3, dtype=np.float32)
    for node in ("B", "D", "C"):
        np.testing.assert_allclose(result.values[node], expected, atol=1e-6)
    np.testing.assert_array_equal(result.values["A"], inputs["A"])


def test_zero_weight_sender_does_not_contribute_but_receives_mean():
    inputs = {"L": [1., 5.], "F": [9., 13.]}
    views = {node: ("L", "F") for node in inputs}

    result = aggregate(inputs, views, weights={"L": 0., "F": 2.})

    for value in result.values.values():
        np.testing.assert_array_equal(value, np.asarray(inputs["F"], dtype=np.float32))


def test_zero_total_accepted_weight_fails_explicitly():
    inputs = {"L": [1., 5.], "F": [9., 13.]}
    views = {node: ("L", "F") for node in inputs}

    with pytest.raises(ValueError, match="positive total"):
        aggregate(inputs, views, weights={"L": 0., "F": 0.})


@pytest.mark.parametrize("weights", [
    {"L": 1.},
    {"L": 1., "F": 1., "extra": 2.},
    {"L": -1., "F": 1.},
    {"L": np.nan, "F": 1.},
    {"L": np.inf, "F": 1.},
    {"L": "1", "F": 1.},
    {"L": True, "F": 1.},
    [1., 1.],
])
def test_malformed_weights_fail_explicitly(weights):
    inputs = {"L": [1., 5.], "F": [9., 13.]}
    views = {node: ("L", "F") for node in inputs}

    with pytest.raises(ValueError, match="Weights"):
        aggregate(inputs, views, weights=weights)


def test_default_and_equal_weights_are_identical():
    inputs = {"L": [0., 10., 30.], "F": [6., 4., 0.]}
    views = {node: ("L", "F") for node in inputs}

    default = aggregate(inputs, views, chunk_elements=1)
    explicit = aggregate(inputs, views, chunk_elements=1, weights={"L": 1., "F": 1.})

    for node in inputs:
        np.testing.assert_array_equal(default.values[node], explicit.values[node])


def test_weighted_outputs_are_stable_under_reverse_arrival_order():
    nodes = ("B", "D", "A", "C")
    inputs = {node: np.arange(48, dtype=np.float32) + i * 0.125
              for i, node in enumerate(nodes)}
    views = {node: nodes for node in nodes}
    weights = {"B": 0.25, "D": 2., "A": 1.5, "C": 4.}

    forward = aggregate(inputs, views, weights=weights)
    reverse = aggregate(inputs, views, arrival_order=list(reversed(nodes)), weights=weights)

    for node in nodes:
        np.testing.assert_allclose(forward.values[node], reverse.values[node], atol=1e-5)
