"""D1 -- bind the Kademlia routing address to the cryptographic identity.

Track A's whole attack rests on one gap: a peer's ``node_id`` (which decides what it
stores) is unrelated to its ``PeerID`` (which is a hash of its public key and is
authenticated by TLS). ``DHTNode.create`` takes ``node_id`` as a free parameter
(``hivemind/dht/node.py:171``) and ``rpc_ping`` accepts whatever a peer claims
(``hivemind/dht/protocol.py:150``).

The fix is to stop treating them as independent::

    node_id == DHTID.generate(source=peer_id.to_bytes())

Now a routing address is a deterministic function of a key the attacker cannot forge,
so aiming at a particular key means *grinding keypairs* until one happens to hash
nearby. :func:`expected_keypairs_for_prefix` quantifies what that costs.

This module deliberately exposes the check as a standalone predicate plus a
verifying wrapper rather than patching ``hivemind/`` in place: the research tree
stays additive, and the same predicate is what an upstream patch would call from
``DHTProtocol.update_routing_table``.
"""

from __future__ import annotations

from hivemind.dht.routing import DHTID
from hivemind.p2p import PeerID

__all__ = ["derive_node_id", "is_node_id_bound", "expected_keypairs_for_prefix", "BindingPolicy"]


def derive_node_id(peer_id: PeerID) -> DHTID:
    """The only routing address a given PeerID is allowed to occupy.

    Uses ``DHTID.generate``, the same hash Hivemind already applies to record keys,
    so no new primitive is introduced.
    """
    return DHTID.generate(source=peer_id.to_bytes())


def is_node_id_bound(node_id: DHTID, peer_id: PeerID) -> bool:
    """Does this peer's claimed routing address match its cryptographic identity?

    Cheap: one hash. Intended to be called wherever a ``(node_id, peer_id)`` pair
    enters routing state -- ``rpc_ping``, ``update_routing_table``, and the neighbour
    lists returned by ``call_find`` (``protocol.py:303``), which are ingested
    unverified today.
    """
    return int(derive_node_id(peer_id)) == int(node_id)


def expected_keypairs_for_prefix(prefix_bits: int) -> float:
    """Expected keypairs to grind for a node_id sharing ``prefix_bits`` with a target.

    Each keypair yields a uniformly random address, so matching a ``b``-bit prefix is
    a geometric trial with success probability ``2**-b``: on average ``2**b`` attempts.
    Under D1 the attacker must pay this to get *near* a key; today the same placement
    is free and exact.
    """
    return float(2**prefix_bits)


class BindingPolicy:
    """Enforcement wrapper, so the cost of the check can be measured, not just asserted.

    ``strict=False`` mirrors a staged rollout: violations are counted and reported but
    still admitted, which is how a real network would deploy this without partitioning
    itself from unpatched peers.
    """

    def __init__(self, strict: bool = True) -> None:
        self.strict = strict
        self.checked = 0
        self.rejected = 0

    def admit(self, node_id: DHTID, peer_id: PeerID) -> bool:
        """Return whether this (node_id, peer_id) pair may enter the routing table."""
        self.checked += 1
        if is_node_id_bound(node_id, peer_id):
            return True
        self.rejected += 1
        return not self.strict

    @property
    def rejection_rate(self) -> float:
        return self.rejected / self.checked if self.checked else 0.0

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(strict={self.strict}, checked={self.checked}, "
            f"rejected={self.rejected}, rate={self.rejection_rate:.3f})"
        )


def best_achievable_prefix(keypair_budget: int) -> float:
    """Longest prefix an attacker can expect to match with a given grinding budget.

    Inverse of :func:`expected_keypairs_for_prefix`. Converts "attacker owns a big
    machine" into "attacker gets this many bits closer", which is what actually
    determines whether they land in a key's replica set.
    """
    if keypair_budget < 1:
        return 0.0
    from math import log2

    return log2(keypair_budget)


def eclipse_cost_under_binding(prefix_bits: int, num_replicas: int = 5) -> float:
    """Keypair generations for a total eclipse once D1 is enforced.

    Today the same outcome costs ``num_replicas`` *free* identities (Track A). Under
    D1 each of those identities must be ground out until its address lands within the
    target's neighbourhood, so the cost becomes
    ``num_replicas * 2**prefix_bits`` keypair generations.
    """
    return num_replicas * expected_keypairs_for_prefix(prefix_bits)
