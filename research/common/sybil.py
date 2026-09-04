"""Sybil identity crafting for Hivemind DHT security experiments.

Threat model: **free identities**. Hivemind peers carry two independent identities,
and neither one costs the attacker anything meaningful:

* **libp2p PeerID** -- crypto-derived (``multihash(sha256(pubkey))``, see
  ``hivemind/p2p/p2p_daemon_bindings/datastructures.py``) and authenticated by TLS.
  It cannot be *forged*, but minting a fresh keypair is free, so an attacker can
  still mint unlimited *distinct* PeerIDs.
* **DHTID (node_id)** -- the Kademlia routing address that decides which keys a peer
  is responsible for. It is **not bound to the PeerID in any way**:
  ``DHTNode.create(node_id=...)`` accepts an arbitrary 160-bit value
  (``hivemind/dht/node.py:171``) and remote peers accept the self-reported value
  verbatim (``hivemind/dht/protocol.py:150``).

That second point is the eclipse primitive this module exists to exercise: because
the attacker picks their own routing address, they can sit arbitrarily close (in XOR
metric) to any target key and become one of the ``num_replicas`` peers responsible
for it. The cryptographically sound PeerID does not help, because routing decisions
never consult it.
"""

from __future__ import annotations

import random
from typing import Iterable, List, Optional, Sequence, Set

from hivemind.dht.routing import DHTID

__all__ = [
    "key_to_dhtid",
    "closest_ids",
    "ids_sharing_prefix",
    "random_ids",
    "xor_distance",
    "rank_by_distance",
]


def key_to_dhtid(key: str) -> DHTID:
    """Map a DHT record key to the routing address that owns it.

    Mirrors exactly what ``DHTNode.store_many``/``get_many`` do internally
    (``DHTID.generate(source=key)``, ``hivemind/dht/node.py:386,564``), so the
    attacker computes the same target the honest network does.
    """
    return DHTID.generate(source=key)


def xor_distance(a: DHTID, b: DHTID) -> int:
    """Kademlia XOR metric. Lower means 'more responsible for that key'."""
    return int(a) ^ int(b)


def closest_ids(target: DHTID, count: int, *, offset: int = 0) -> List[DHTID]:
    """The ``count`` node_ids with the smallest possible XOR distance to ``target``.

    ``target ^ i`` has XOR distance exactly ``i``, so iterating ``i = 0, 1, 2, ...``
    enumerates routing addresses in strictly increasing distance order -- the
    optimal placement an attacker can achieve. ``offset`` skips the first few
    (useful to leave the exact-match slot alone, or to place a second wave).

    This is the strongest form of the attack: nothing in Hivemind prevents it,
    because node_ids are self-asserted.
    """
    if count < 0:
        raise ValueError(f"count must be non-negative, got {count}")
    return [DHTID(int(target) ^ i) for i in range(offset, offset + count)]


def ids_sharing_prefix(
    target: DHTID,
    count: int,
    prefix_bits: int,
    *,
    rng: Optional[random.Random] = None,
) -> List[DHTID]:
    """node_ids that match ``target`` on the top ``prefix_bits`` and are random below.

    Models a weaker / cheaper attacker than :func:`closest_ids`: one who aims at a
    *region* of the keyspace rather than grinding for the exact nearest slots. Also
    the right generator for studying how much precision the attack actually needs.
    """
    total_bits = DHTID.HASH_NBYTES * 8
    if not 0 <= prefix_bits <= total_bits:
        raise ValueError(f"prefix_bits must be in [0, {total_bits}], got {prefix_bits}")
    rng = rng or random.Random()

    suffix_bits = total_bits - prefix_bits
    # keep the top prefix_bits of target, randomise the rest
    prefix_mask = ((1 << prefix_bits) - 1) << suffix_bits
    kept = int(target) & prefix_mask

    ids, seen = [], set()
    while len(ids) < count:
        candidate = kept | rng.getrandbits(suffix_bits) if suffix_bits else kept
        if candidate in seen:
            if suffix_bits == 0 or len(seen) >= 2**suffix_bits:
                raise ValueError(
                    f"cannot draw {count} distinct ids with prefix_bits={prefix_bits} "
                    f"(keyspace below the prefix holds only 2**{suffix_bits})"
                )
            continue
        seen.add(candidate)
        ids.append(DHTID(candidate))
    return ids


def random_ids(count: int, *, rng: Optional[random.Random] = None) -> List[DHTID]:
    """Uniformly random node_ids -- the honest-peer baseline / control condition."""
    rng = rng or random.Random()
    total_bits = DHTID.HASH_NBYTES * 8
    return [DHTID(rng.getrandbits(total_bits)) for _ in range(count)]


def rank_by_distance(target: DHTID, candidates: Iterable[DHTID]) -> List[DHTID]:
    """Sort candidate node_ids nearest-first around ``target`` (Kademlia order)."""
    return sorted(candidates, key=lambda node_id: xor_distance(node_id, target))


def capture_count(
    target: DHTID,
    honest_ids: Sequence[DHTID],
    sybil_ids: Sequence[DHTID],
    num_replicas: int,
) -> int:
    """How many of the ``num_replicas`` slots nearest ``target`` the attacker holds.

    This is the *structural* (routing-table-independent) view of the eclipse: given
    everyone's node_ids, who would an ideal lookup converge on? Live lookups are
    measured separately, since real routing is imperfect.
    """
    sybil_set: Set[int] = {int(node_id) for node_id in sybil_ids}
    nearest = rank_by_distance(target, list(honest_ids) + list(sybil_ids))[:num_replicas]
    return sum(1 for node_id in nearest if int(node_id) in sybil_set)
