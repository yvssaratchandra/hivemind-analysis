"""Track A -- eclipsing a DHT record by choosing routing addresses.

The attack in one line: Hivemind decides *who stores a key* by XOR distance between
the key's DHTID and each peer's node_id, and lets every peer pick its own node_id
(``hivemind/dht/node.py:171``, accepted verbatim at ``hivemind/dht/protocol.py:150``).
So an attacker computes the target key's DHTID and mints identities adjacent to it.

Two layers, because they answer different questions:

* :func:`analytical_capture` -- pure XOR math over node_ids. No processes, so it
  scales to 10^4 peers and gives the *theory*: how many crafted identities are
  needed to own the replica set, as a function of swarm size.
* :func:`live_capture` -- a real Hivemind swarm. Slower and capped by RAM (each
  peer spawns a ``p2pd``), but shows whether live routing and real ``store``
  placement actually match the theory.

Both report the same primary metric: the share of the ``num_replicas`` peers
responsible for the key that the attacker controls. At 1.0 the record is fully
eclipsed -- every honest replica of that key lives on attacker hardware.
"""

from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from hivemind.dht.node import DHTNode
from hivemind.utils.timed_storage import get_dht_time
from research.common.swarm import Swarm, running_swarm
from research.common.sybil import capture_count, closest_ids, key_to_dhtid, random_ids, xor_distance

__all__ = ["analytical_capture", "live_capture", "EclipseResult"]


@dataclass
class EclipseResult:
    """Outcome of one eclipse attempt."""

    num_honest: int
    num_sybils: int
    num_replicas: int
    structural_capture: float  # share of the ideal nearest-k set held by the attacker
    lookup_capture: Optional[float] = None  # share of a live lookup's answer that is attacker-controlled
    store_capture: Optional[float] = None  # share of the peers that actually stored the record
    honest_read_ok: Optional[bool] = None  # could an honest peer still read the honest value?
    build_seconds: Optional[float] = None  # wall-clock cost of standing the Sybils up

    def as_row(self, **extra: Any) -> Dict[str, Any]:
        row = {
            "num_honest": self.num_honest,
            "num_sybils": self.num_sybils,
            "num_replicas": self.num_replicas,
            "global_malicious_fraction": self.num_sybils / max(self.num_honest + self.num_sybils, 1),
            "structural_capture": self.structural_capture,
            "lookup_capture": self.lookup_capture,
            "store_capture": self.store_capture,
            "honest_read_ok": self.honest_read_ok,
            "build_seconds": self.build_seconds,
        }
        row.update(extra)
        return row


def analytical_capture(
    key: str,
    num_honest: int,
    num_sybils: int,
    *,
    num_replicas: int = 5,
    rng: Optional[random.Random] = None,
) -> EclipseResult:
    """Theory layer: with honest peers at random node_ids and Sybils placed optimally,
    what share of the responsible replica set does the attacker hold?

    Honest peers draw uniform node_ids (what Hivemind does by default); the attacker
    places itself at XOR distance 0, 1, 2, ... from the target. No networking involved
    -- this is the placement rule itself, so it isolates the structural weakness from
    any routing imperfection.
    """
    rng = rng or random.Random()
    target = key_to_dhtid(key)
    honest_ids = random_ids(num_honest, rng=rng)
    sybil_ids = closest_ids(target, num_sybils)

    captured = capture_count(target, honest_ids, sybil_ids, num_replicas)
    return EclipseResult(
        num_honest=num_honest,
        num_sybils=num_sybils,
        num_replicas=num_replicas,
        structural_capture=captured / num_replicas,
    )


async def _storage_holders(swarm: Swarm, key: str) -> List[DHTNode]:
    """Which live peers actually hold the record for ``key`` in local storage.

    Reads each node's ``protocol.storage`` directly. This is the ground truth for
    'where did the honest peer's data physically land', which is what an eclipse
    is really about -- the attacker becomes the custodian of the record and can
    censor, stale-serve, or simply drop it.
    """
    key_id = key_to_dhtid(key)
    return [node for node in swarm.nodes if key_id in node.protocol.storage]


async def live_capture(
    key: str,
    num_honest: int,
    num_sybils: int,
    *,
    num_replicas: int = 5,
    node_kwargs: Optional[Dict[str, Any]] = None,
    value: Any = "honest-value",
    expiration_seconds: float = 600.0,
) -> EclipseResult:
    """Live layer: stand up a real swarm, run a real store and a real lookup.

    Sequence mirrors a real attack: the honest network forms, the attacker joins with
    crafted node_ids, then an honest writer publishes a record and an honest reader
    tries to retrieve it.
    """
    node_kwargs = {"num_replicas": num_replicas, **(node_kwargs or {})}
    target = key_to_dhtid(key)
    sybil_ids = closest_ids(target, num_sybils)

    build_start = time.perf_counter()
    async with running_swarm(num_honest, sybil_ids, node_kwargs=node_kwargs) as swarm:
        build_seconds = time.perf_counter() - build_start

        structural = capture_count(target, swarm.honest_node_ids, swarm.sybil_node_ids, num_replicas) / num_replicas

        # an honest peer publishes the record
        writer = swarm.honest[1] if len(swarm.honest) > 1 else swarm.honest[0]
        await writer.store(key, value, expiration_time=get_dht_time() + expiration_seconds)

        # where did it physically land?
        holders = await _storage_holders(swarm, key)
        store_capture = sum(1 for node in holders if swarm.is_sybil(node.peer_id)) / len(holders) if holders else None

        # who does live routing consider responsible?
        prober = swarm.honest[0]
        nearest = await prober.find_nearest_nodes([target], k_nearest=num_replicas)
        found = list(nearest[target].keys())
        lookup_capture = sum(1 for node_id in found if swarm.is_sybil(node_id)) / len(found) if found else None

        # can an honest reader still get the honest value back?
        reader = swarm.honest[-1]
        result = await reader.get(key, latest=True)
        honest_read_ok = result is not None and result.value == value

        return EclipseResult(
            num_honest=num_honest,
            num_sybils=num_sybils,
            num_replicas=num_replicas,
            structural_capture=structural,
            lookup_capture=lookup_capture,
            store_capture=store_capture,
            honest_read_ok=honest_read_ok,
            build_seconds=build_seconds,
        )


async def censorship_test(
    key: str,
    num_honest: int,
    num_sybils: int,
    *,
    crafted: bool = True,
    num_replicas: int = 5,
    node_kwargs: Optional[Dict[str, Any]] = None,
    value: Any = "honest-value",
    expiration_seconds: float = 600.0,
    rng: Optional[random.Random] = None,
) -> Dict[str, Any]:
    """Turn custody into demonstrated harm, with a control that isolates the bug.

    An eclipse makes the attacker the *custodian* of a record. Exercising that is
    trivial -- here the attacker simply stops serving it (modelled by shutting the
    Sybils down after the honest write, i.e. the record is dropped rather than
    handed back).

    The control is the important part. We run the identical experiment twice:

    * ``crafted=True``  -- node_ids chosen adjacent to the target key.
    * ``crafted=False`` -- the same number of identities with *random* node_ids.

    Both attackers have identical resources; only the freedom to choose a routing
    address differs. So any gap between them is attributable to that freedom alone,
    not to the attacker simply owning more peers.
    """
    rng = rng or random.Random()
    node_kwargs = {"num_replicas": num_replicas, **(node_kwargs or {})}
    target = key_to_dhtid(key)
    sybil_ids = closest_ids(target, num_sybils) if crafted else random_ids(num_sybils, rng=rng)

    async with running_swarm(num_honest, sybil_ids, node_kwargs=node_kwargs) as swarm:
        writer = swarm.honest[1] if len(swarm.honest) > 1 else swarm.honest[0]
        await writer.store(key, value, expiration_time=get_dht_time() + expiration_seconds)

        holders = await _storage_holders(swarm, key)
        sybil_holders = sum(1 for node in holders if swarm.is_sybil(node.peer_id))
        honest_holders = len(holders) - sybil_holders

        # the attacker exercises custody: stop serving the record
        import asyncio as _asyncio

        await _asyncio.gather(*(node.shutdown() for node in swarm.sybils), return_exceptions=True)
        swarm.sybils.clear()  # already down; keep teardown from double-shutting them

        # a reader that has never touched this key tries to fetch it
        reader = swarm.honest[-1]
        result = await reader.get(key, latest=True)
        readable = result is not None and result.value == value

        return {
            "crafted_node_ids": crafted,
            "num_honest": num_honest,
            "num_sybils": num_sybils,
            "num_replicas": num_replicas,
            "sybil_holders": sybil_holders,
            "honest_holders": honest_holders,
            "custody_fraction": sybil_holders / len(holders) if holders else 0.0,
            "readable_after_attack": readable,
            "censored": not readable,
        }


def sybils_needed_for_full_capture(
    key: str,
    num_honest: int,
    *,
    num_replicas: int = 5,
    rng: Optional[random.Random] = None,
    max_sybils: int = 64,
) -> Optional[int]:
    """Smallest number of crafted identities that captures every replica slot.

    This is the attack-cost headline: under the free-identity threat model the cost
    is exactly this count, because each identity is just a keypair and a node_id.
    Returns None if ``max_sybils`` was not enough.
    """
    rng = rng or random.Random()
    target = key_to_dhtid(key)
    honest_ids = random_ids(num_honest, rng=rng)

    for count in range(1, max_sybils + 1):
        sybil_ids = closest_ids(target, count)
        if capture_count(target, honest_ids, sybil_ids, num_replicas) == num_replicas:
            return count
    return None


def nearest_honest_distance(key: str, num_honest: int, *, rng: Optional[random.Random] = None) -> int:
    """XOR distance from the target to the closest *honest* peer.

    Context for why the attack is so cheap: honest node_ids are uniform over a
    160-bit space, so the nearest honest peer sits astronomically far from the key,
    while an attacker sits at distance 0.
    """
    rng = rng or random.Random()
    target = key_to_dhtid(key)
    return min(xor_distance(node_id, target) for node_id in random_ids(num_honest, rng=rng))
