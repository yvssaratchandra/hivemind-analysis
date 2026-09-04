"""Track B -- from a small global malicious fraction to a large local one at a victim.

Hivemind peers find averaging partners through the DHT: each peer publishes "I am
looking for a group" under a *group key* (``GroupKeyManager.declare_averager``,
``hivemind/averaging/key_manager.py:46``) and reads the others back with
``get_averagers``. Three properties of that record make it the cheapest attack
surface in the system:

1. **It is unsigned.** No ``RecordValidator`` is attached to averaging keys, unlike
   the progress-tracker records which *are* RSA-signed
   (``hivemind/optim/progress_tracker.py:108``). The subkey is simply
   ``peer_id.to_bytes()``, and nothing checks that the writer owns that peer_id.
2. **So an identity costs one DHT write.** Not a process, not a keypair, not a
   connection. A single process can register thousands of fabricated averagers.
3. **Leadership is won by the earliest declared expiration**
   (``matchmaking.py:478``), a value the declarer chooses freely.

The victim's *local* malicious fraction is therefore set by how many records the
attacker wrote into the victim's bucket, while the *global* fraction counts real
peers. That gap is the amplification measured here.

The second question is what Hivemind's per-round group re-formation does about it.
After every group, peers re-bucket themselves via ``update_key_on_group_assembled``
(``key_manager.py:94``). Crucially that shuffle is seeded on ``group_id`` and driven
entirely by values every group member sees, so a peer who shared *one* group with
the victim can compute the victim's future buckets exactly -- see
:func:`predict_next_group_bits`. Re-formation is unpredictable to an outsider but
fully transparent to a participant.
"""

from __future__ import annotations

import hashlib
import random
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np

from hivemind.averaging.group_info import GroupInfo
from hivemind.p2p import PeerID

__all__ = [
    "fabricate_peer_id",
    "predict_next_group_bits",
    "bucket_of",
    "simulate_amplification",
    "simulate_tracking",
]

# A libp2p PeerID is an identity-multihash of a public key. On the wire -- and in a
# declare_averager subkey -- it is just bytes, and the averaging layer never checks
# that the declarer can actually authenticate as it.
_PEER_ID_PREFIX = b"\x00\x24\x08\x01\x12\x20"  # identity multihash header + 32-byte digest


def fabricate_peer_id(index: int, *, salt: str = "sybil") -> PeerID:
    """Manufacture a syntactically valid PeerID with no keypair behind it.

    Sufficient to occupy a slot in a group key's record, which is all the
    matchmaking layer requires of a candidate.
    """
    return PeerID(_PEER_ID_PREFIX + hashlib.sha256(f"{salt}{index}".encode()).digest())


def predict_next_group_bits(
    current_bits: str,
    group_id: bytes,
    peer_ids: Sequence[PeerID],
    peer_id: PeerID,
    target_group_size: Optional[int],
) -> str:
    """Compute where ``peer_id`` will re-bucket itself after this group -- from public data only.

    This is a faithful re-implementation of ``GroupKeyManager.update_key_on_group_assembled``
    (``hivemind/averaging/key_manager.py:94-105``). Every input is visible to every
    member of the group: ``group_id`` and the ordered ``peer_ids`` arrive in the
    leader's message, and ``target_group_size`` is a published run parameter.

    Consequence: group re-formation does not shed a determined attacker. Having shared
    a single group with the victim, the attacker can follow it indefinitely.
    """
    rng = random.Random(group_id)
    index = list(peer_ids).index(peer_id)
    group_size = len(peer_ids)
    num_buckets = target_group_size
    if num_buckets is None:
        num_buckets = 1 if group_size == 0 else 2 ** (group_size - 1).bit_length()
    generalized_index = rng.sample(range(num_buckets), group_size)[index]
    nbits = int(np.ceil(np.log2(num_buckets)))
    new_bits = bin(generalized_index)[2:].rjust(nbits, "0")
    return (current_bits + new_bits)[-len(current_bits) :] if current_bits else ""


def bucket_of(group_bits: str) -> str:
    """The group key suffix a peer is currently listening on."""
    return f"0b{group_bits}"


@dataclass
class AmplificationResult:
    num_honest: int
    num_buckets: int
    num_fake_declarations: int
    attacker_processes: int
    global_malicious_fraction: float  # attacker share of *real* peers
    local_malicious_fraction: float  # attacker share of the victim's candidate list
    amplification: float
    victim_candidates: int


def simulate_amplification(
    num_honest: int,
    num_fake_declarations: int,
    *,
    num_buckets: int = 1,
    attacker_processes: int = 1,
    rng: Optional[random.Random] = None,
) -> AmplificationResult:
    """Model the victim's candidate view without standing up a swarm.

    Honest peers scatter uniformly over ``num_buckets`` buckets (what the re-bucketing
    shuffle does at equilibrium). The attacker spends its whole budget on the victim's
    bucket, because fabricated declarations are free to place anywhere.

    Two multipliers compound here, and the model separates them:
      * **identity multiplication** -- one process writes many declarations;
      * **targeting** -- all of them land in the one bucket the victim reads.
    """
    rng = rng or random.Random()

    honest_in_victim_bucket = sum(1 for _ in range(num_honest) if rng.randrange(num_buckets) == 0)
    victim_candidates = honest_in_victim_bucket + num_fake_declarations

    # global fraction counts real participants, not fabricated records
    global_fraction = attacker_processes / (num_honest + attacker_processes)
    local_fraction = num_fake_declarations / victim_candidates if victim_candidates else 0.0

    return AmplificationResult(
        num_honest=num_honest,
        num_buckets=num_buckets,
        num_fake_declarations=num_fake_declarations,
        attacker_processes=attacker_processes,
        global_malicious_fraction=global_fraction,
        local_malicious_fraction=local_fraction,
        amplification=local_fraction / global_fraction if global_fraction else float("inf"),
        victim_candidates=victim_candidates,
    )


def simulate_tracking(
    rounds: int,
    *,
    num_buckets: int,
    tracking: bool,
    num_honest: int,
    num_fake_declarations: int,
    rng: Optional[random.Random] = None,
) -> List[Dict[str, float]]:
    """Does per-round re-bucketing dilute the attacker? Depends entirely on whether it follows.

    ``tracking=True`` models the attacker of :func:`predict_next_group_bits`: it
    recomputes the victim's next bucket each round and moves its declarations there,
    so the victim's local fraction never decays.

    ``tracking=False`` models a naive attacker that keeps flooding one fixed bucket.
    The victim re-buckets away from it and the attack washes out -- this is the decay
    the design appears to promise, and it only materialises against an attacker who
    declines to use public information.
    """
    rng = rng or random.Random()
    history: List[Dict[str, float]] = []

    victim_bucket = 0
    attacker_bucket = 0

    for round_index in range(rounds):
        honest_here = sum(1 for _ in range(num_honest) if rng.randrange(num_buckets) == 0)
        fakes_here = num_fake_declarations if attacker_bucket == victim_bucket else 0
        candidates = honest_here + fakes_here
        history.append(
            {
                "round": round_index,
                "local_malicious_fraction": fakes_here / candidates if candidates else 0.0,
                "victim_bucket": victim_bucket,
                "attacker_bucket": attacker_bucket,
                "tracking": tracking,
            }
        )

        # the victim re-buckets (shuffle seeded on the group_id it just participated in)
        victim_bucket = rng.randrange(num_buckets)
        # a tracking attacker recomputes that value; a naive one does not
        attacker_bucket = victim_bucket if tracking else attacker_bucket

    return history


def verify_prediction_matches_hivemind(
    group_id: bytes,
    peer_ids: Sequence[PeerID],
    peer_id: PeerID,
    target_group_size: Optional[int],
    current_bits: str,
) -> Tuple[str, str]:
    """Cross-check :func:`predict_next_group_bits` against Hivemind's own code path.

    Returns ``(predicted, actual)``. They must be equal -- if upstream ever changes
    the shuffle, this is the check that catches it.
    """
    from hivemind.averaging.key_manager import GroupKeyManager

    class _StubDHT:  # GroupKeyManager only reads .peer_id during __init__
        def __init__(self, peer_id: PeerID):
            self.peer_id = peer_id

    manager = GroupKeyManager(_StubDHT(peer_id), "run", current_bits, target_group_size)
    group = GroupInfo(group_id=group_id, peer_ids=tuple(peer_ids), gathered=tuple(b"" for _ in peer_ids))

    import asyncio

    asyncio.run(manager.update_key_on_group_assembled(group))
    predicted = predict_next_group_bits(current_bits, group_id, peer_ids, peer_id, target_group_size)
    return predicted, manager.group_bits
