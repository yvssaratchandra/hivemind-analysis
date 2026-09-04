"""Tier 2 -- a swarm of genuinely separate OS processes with randomised bootstrap.

Tier 1 (:mod:`research.common.swarm`) already uses real ``p2pd`` daemons and real
libp2p transport, but every ``DHTNode`` shares one Python process and one event loop,
and every peer bootstraps off the same node (a star). Two artefacts could in principle
flatter the results:

* shared-process scheduling -- all peers progress in lockstep on one loop;
* star topology -- everyone knows the same bootstrap node, so routing tables are
  unusually uniform.

This module removes both: one ``mp.Process`` per peer, and each joining peer bootstraps
off a **randomly chosen** already-running peer, following
``tests/test_utils/dht_swarms.launch_swarm_in_separate_processes``. The one thing that
helper cannot do is give each peer a *different* ``node_id`` -- it forwards a single
``**kwargs`` to every node -- which is precisely what a Sybil experiment needs, so the
per-peer node_id is threaded through here.

Used only to confirm headline numbers; it is RAM-bound at roughly 20-30 peers.
"""

from __future__ import annotations

import asyncio
import multiprocessing as mp
import random
import signal
import threading
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence

from hivemind.dht.node import DHTNode
from hivemind.dht.routing import DHTID
from hivemind.p2p import PeerID

__all__ = ["MultiprocessSwarm", "launch_multiprocess_swarm"]

LOCALHOST_MADDRS = ["/ip4/127.0.0.1/tcp/0"]


def _run_node(initial_peers: Sequence[str], info_queue: mp.Queue, node_id_int: Optional[int], **kwargs: Any) -> None:
    """Child-process entry point: bring up one DHTNode and park it on its event loop.

    ``node_id`` crosses the process boundary as a plain int because ``DHTID`` is an
    ``int`` subclass and pickling it through the spawn/fork boundary is needless work.
    """
    if asyncio.get_event_loop().is_running():
        asyncio.get_event_loop().stop()
        asyncio.set_event_loop(asyncio.new_event_loop())
    loop = asyncio.get_event_loop()

    node = loop.run_until_complete(
        DHTNode.create(
            initial_peers=list(initial_peers),
            node_id=DHTID(node_id_int) if node_id_int is not None else None,
            host_maddrs=LOCALHOST_MADDRS,
            **kwargs,
        )
    )
    maddrs = loop.run_until_complete(node.get_visible_maddrs())
    info_queue.put((int(node.node_id), node.peer_id.to_bytes(), [str(addr) for addr in maddrs]))

    async def shutdown() -> None:
        await node.shutdown()
        loop.stop()

    loop.add_signal_handler(signal.SIGTERM, lambda: loop.create_task(shutdown()))
    loop.run_forever()


@dataclass
class MultiprocessSwarm:
    """Handle on a running multi-process swarm."""

    processes: List[mp.Process] = field(default_factory=list)
    honest_node_ids: List[DHTID] = field(default_factory=list)
    sybil_node_ids: List[DHTID] = field(default_factory=list)
    honest_peer_ids: List[PeerID] = field(default_factory=list)
    sybil_peer_ids: List[PeerID] = field(default_factory=list)
    maddrs: List[List[str]] = field(default_factory=list)

    def is_sybil(self, identity: PeerID | DHTID) -> bool:
        if isinstance(identity, PeerID):
            return identity in set(self.sybil_peer_ids)
        return any(int(node_id) == int(identity) for node_id in self.sybil_node_ids)

    def shutdown(self) -> None:
        """SIGTERM every child, then join. Children that ignore it are killed, so a
        failed run cannot leave dozens of p2pd daemons behind."""
        for process in self.processes:
            if process.is_alive():
                process.terminate()
        for process in self.processes:
            process.join(timeout=10)
            if process.is_alive():
                process.kill()
                process.join(timeout=5)


def launch_multiprocess_swarm(
    num_honest: int,
    sybil_node_ids: Sequence[DHTID] = (),
    *,
    num_sequential: int = 3,
    node_kwargs: Optional[Dict[str, Any]] = None,
    seed: int = 0,
    timeout: float = 180.0,
) -> MultiprocessSwarm:
    """Start ``num_honest`` honest peers plus one Sybil per supplied node_id.

    The first ``num_sequential`` honest peers come up one at a time to seed the
    network; the rest join in parallel, each bootstrapping off a random existing peer
    so the topology is not a star. Sybils join last, as an attacker would.
    """
    node_kwargs = dict(node_kwargs or {})
    rng = random.Random(seed)
    swarm = MultiprocessSwarm()
    info_queue: mp.Queue = mp.Queue()
    lock = threading.RLock()

    def spawn(initial_peers: Sequence[str], node_id: Optional[DHTID]) -> mp.Process:
        process = mp.Process(
            target=_run_node,
            args=(list(initial_peers), info_queue, int(node_id) if node_id is not None else None),
            kwargs=node_kwargs,
            daemon=True,
        )
        process.start()
        swarm.processes.append(process)
        return process

    def collect(count: int, into_sybils: bool) -> None:
        for _ in range(count):
            node_id_int, peer_id_bytes, maddrs = info_queue.get(timeout=timeout)
            with lock:
                node_id, peer_id = DHTID(node_id_int), PeerID(peer_id_bytes)
                if into_sybils:
                    swarm.sybil_node_ids.append(node_id)
                    swarm.sybil_peer_ids.append(peer_id)
                else:
                    swarm.honest_node_ids.append(node_id)
                    swarm.honest_peer_ids.append(peer_id)
                swarm.maddrs.append(maddrs)

    # seed the network sequentially so later peers have someone to bootstrap from
    for index in range(min(num_sequential, num_honest)):
        initial = rng.choice(swarm.maddrs) if swarm.maddrs else []
        spawn(initial, None)
        collect(1, into_sybils=False)

    # the remaining honest peers join in parallel off random existing peers
    remaining = num_honest - min(num_sequential, num_honest)
    for _ in range(remaining):
        spawn(rng.choice(swarm.maddrs), None)
    collect(remaining, into_sybils=False)

    # the attacker joins an already-formed network, with chosen routing addresses
    for node_id in sybil_node_ids:
        spawn(rng.choice(swarm.maddrs), node_id)
    collect(len(sybil_node_ids), into_sybils=True)

    return swarm
