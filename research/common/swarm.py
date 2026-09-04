"""Swarm construction for the Hivemind network-security experiments.

Two fidelity tiers, per the project plan:

* **Tier 1 (in-process)** -- :func:`build_swarm` puts every peer in one event loop
  via ``DHTNode.create``. Hundreds of peers, seconds per run, laptop-friendly.
  Used for all scale sweeps.
* **Tier 2 (real swarm)** -- ``tests/test_utils/dht_swarms.launch_swarm_in_separate_processes``
  gives one OS process and one ``p2pd`` daemon per peer. Heavy (~20-30 peers max
  here) but exercises the real libp2p stack; used only to confirm headline numbers.

Both tiers reach the same injection point: ``DHTNode.create(node_id=...)`` accepts an
attacker-chosen routing address (``hivemind/dht/node.py:171``), which is exactly the
weakness under study.
"""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence

from hivemind.dht.node import DHTNode
from hivemind.dht.routing import DHTID
from hivemind.p2p import PeerID

__all__ = ["Swarm", "build_swarm", "LOCALHOST_MADDRS"]

# Keep every experiment on loopback: nothing in this research reaches the outside world.
LOCALHOST_MADDRS = ("/ip4/127.0.0.1/tcp/0",)


@dataclass
class Swarm:
    """A running set of in-process DHT nodes, split into honest and Sybil peers.

    ``nodes`` holds every node (bootstrap first). ``honest`` and ``sybils`` are
    disjoint views used to attribute observations to one side or the other.
    """

    honest: List[DHTNode] = field(default_factory=list)
    sybils: List[DHTNode] = field(default_factory=list)

    @property
    def nodes(self) -> List[DHTNode]:
        return self.honest + self.sybils

    @property
    def sybil_node_ids(self) -> List[DHTID]:
        return [node.node_id for node in self.sybils]

    @property
    def honest_node_ids(self) -> List[DHTID]:
        return [node.node_id for node in self.honest]

    @property
    def sybil_peer_ids(self) -> set[PeerID]:
        return {node.peer_id for node in self.sybils}

    @property
    def global_malicious_fraction(self) -> float:
        """f_g -- share of malicious peers across the whole swarm."""
        total = len(self.honest) + len(self.sybils)
        return len(self.sybils) / total if total else 0.0

    def is_sybil(self, identity: PeerID | DHTID) -> bool:
        if isinstance(identity, PeerID):
            return identity in self.sybil_peer_ids
        return any(int(node_id) == int(identity) for node_id in self.sybil_node_ids)

    async def shutdown(self) -> None:
        """Tear every node down, tolerating partial failures so one bad node
        cannot leak the rest of the swarm's processes."""
        await asyncio.gather(*(node.shutdown() for node in self.nodes), return_exceptions=True)


async def _create_node(
    initial_peers: Optional[Sequence[str]] = None,
    node_id: Optional[DHTID] = None,
    **kwargs: Any,
) -> DHTNode:
    return await DHTNode.create(
        node_id=node_id,
        initial_peers=initial_peers,
        host_maddrs=LOCALHOST_MADDRS,
        **kwargs,
    )


async def build_swarm(
    num_honest: int,
    sybil_node_ids: Sequence[DHTID] = (),
    *,
    node_kwargs: Optional[Dict[str, Any]] = None,
    sybil_kwargs: Optional[Dict[str, Any]] = None,
    join_batch_size: int = 16,
) -> Swarm:
    """Build a Tier-1 swarm: ``num_honest`` honest peers plus one Sybil per supplied node_id.

    Honest peers get random node_ids (Hivemind's default). Sybils get the exact
    routing addresses the caller chose -- see :mod:`research.common.sybil`.

    Sybils join **after** the honest swarm has formed, matching the realistic case
    where an attacker targets an existing network. Joins are batched so that a few
    hundred peers do not open every connection at once on a laptop.
    """
    if num_honest < 1:
        raise ValueError("need at least one honest node to bootstrap the swarm")
    node_kwargs = dict(node_kwargs or {})
    sybil_kwargs = {**node_kwargs, **(sybil_kwargs or {})}

    # bootstrap peer -- every later peer joins through it
    bootstrap = await _create_node(**node_kwargs)
    initial_peers = [str(addr) for addr in await bootstrap.get_visible_maddrs()]
    swarm = Swarm(honest=[bootstrap])

    async def join_many(count: int, ids: Sequence[Optional[DHTID]], kwargs: Dict[str, Any]) -> List[DHTNode]:
        created: List[DHTNode] = []
        for start in range(0, count, join_batch_size):
            batch = ids[start : start + join_batch_size]
            created.extend(
                await asyncio.gather(
                    *(_create_node(initial_peers=initial_peers, node_id=node_id, **kwargs) for node_id in batch)
                )
            )
        return created

    remaining_honest = num_honest - 1
    swarm.honest.extend(await join_many(remaining_honest, [None] * remaining_honest, node_kwargs))
    swarm.sybils.extend(await join_many(len(sybil_node_ids), list(sybil_node_ids), sybil_kwargs))
    return swarm


@contextlib.asynccontextmanager
async def running_swarm(*args: Any, **kwargs: Any):
    """``async with running_swarm(...) as swarm:`` -- guarantees teardown.

    Without this, a failed assertion mid-experiment leaves hundreds of peers (and
    their p2pd processes) running.
    """
    swarm = await build_swarm(*args, **kwargs)
    try:
        yield swarm
    finally:
        await swarm.shutdown()
