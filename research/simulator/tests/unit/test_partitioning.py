from trainsim.protocols.partitioning import (
    allocate_elements,
    assign_chunks,
    chunk_sizes,
    hagenbach_bischoff,
)


def test_equal_unknown_bandwidth_uses_largest_remainder_by_peer_order():
    assert allocate_elements(10, [None, None, None]) == (4, 3, 3)


def test_hagenbach_bischoff_is_conservative_and_breaks_ties_stably():
    allocated = hagenbach_bischoff(11, [1.0, 1.0, 1.0])
    assert allocated == (4, 4, 3)
    assert sum(allocated) == 11


def test_native_chunk_assignment_uses_largest_overlap_and_earlier_tie():
    assert assign_chunks(10, 4, [3, 3, 4]) == ((0,), (1,), (2,))
    assert assign_chunks(12, 5, [6, 6]) == ((0,), (1, 2))


def test_chunk_sizes_cover_the_payload_including_short_tail():
    sizes = chunk_sizes(19, 8)
    assert sizes == (8, 8, 3)
    assert sum(sizes) == 19
