"""
Step 10 -- checkpoint 11, the Observe and Delay cells.

`CONTROL_DATA_AUDIT.md` checkpoint 11 is the averaging availability exchange. Deny is
demonstrated (C11.D, the forged withdrawal), Tamper is prevented by the data model
(C11.T, expiration is both lifetime and leadership priority), Select does not apply.
Observe and Delay are empty -- as they are across the whole map.

Delay matters more than it looks. Every defence Hivemind has watches for *silence*:
`banned_senders` fires on a missed chunk deadline, the DHT `Blacklist` fires on peers
that stop responding, and every timeout is a silence detector. A peer that answers
slowly is never silent. The audit's own item 5 asks for an attack in that quadrant.

**The mechanism, from code reading.** `get_averagers` calls
`dht.get(group_key, latest=True)` (`key_manager.py:79`). `latest=True` sets
`sufficient_expiration_time = float("inf")` (`node.py:542-543`), and
`_SearchState.add_candidate` ends a search only when
`expiration_time >= sufficient_expiration_time` (`node.py:844-845`) -- against infinity,
never. So a matchmaking read **cannot short-circuit on a good answer**; it runs its
traversal to exhaustion, bounded only by `wait_timeout` (default 3 s,
`protocol.py:292`). Fast honest answers cannot rescue the victim, because the search
still waits for the slow one.

If that holds, the cost is one ordinary peer: no crafted identities, no custody. Compare
section 4.6, where removing honest peers from a victim's view needed `bucket_size` = 20
crafted identities *and* the custody they buy.

**Why it might convert into an advantage.** Section 4.6 measured that a victim starved of
candidates becomes *more* likely to lead (0/12 -> 5/12): finding nothing that beats its
own declared deadline, it waits to be contacted instead. Delay may reach the same
leadership manipulation far more cheaply. That is the escalation, not this file.

**Patching, not subclassing.** Subclassing renames the RPC handler to
`<prefix>::<ClassName>.<method>` and a swarm whose peers disagree on the name cannot
connect (HANDOFF Trap 3). Handlers are also bound from the instance at registration time
(`p2p/servicer.py:117`), so patching a live node's method does nothing. The wrapper is
therefore installed on `DHTProtocol` *before* any node is created, and decides per-call
whether this particular responder is slow.

Run:
    .venv/bin/python research/hivemind/step10_delay.py --phase 1     # Observe, seconds
    .venv/bin/python research/hivemind/step10_delay.py --phase 2     # Delay sweep, ~2 min
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import statistics
import time
from pathlib import Path

import multiprocessing as mp

import numpy as np
import torch

import hivemind
from hivemind.averaging import DecentralizedAverager
from hivemind.averaging.key_manager import GroupKeyManager
from hivemind.dht.node import DHTNode
from hivemind.dht.routing import DHTID
from hivemind.dht.protocol import DHTProtocol
from hivemind.p2p import P2PContext, PeerID
from hivemind.proto import dht_pb2
from hivemind.utils.timed_storage import get_dht_time

RESULTS_DIR = Path(__file__).resolve().parent / "results"
LOCALHOST = ["/ip4/127.0.0.1/tcp/0"]

REQUEST_TIMEOUT = 3.0  # hivemind default (averager.py:122)
PREFIX = "step10_grad_averager"
GROUP_KEY = f"{PREFIX}.0b"

# id(protocol) -> (delay_seconds, victim_peer_id_bytes or None for everyone)
# Used in phases 1-3, where every DHTNode lives in this process.
SLOW: dict[int, tuple[float, bytes | None]] = {}
DELAYS_APPLIED: list[float] = []

# Phase 4 uses `hivemind.DHT`, which forks a child process holding the DHTNode, so
# `id(protocol)` is meaningless across the boundary. Instead this module-level value is
# set in the parent immediately before forking a peer's DHT; the child inherits whatever
# it held at fork time. Requires the fork start method, which Trap 2 mandates anyway.
SLOW_THIS_PROCESS: float = 0.0
SLOW_ONLY_FOR: bytes | None = None

_ORIGINAL_RPC_FIND = DHTProtocol.rpc_find


async def _slow_rpc_find(self, request: dht_pb2.FindRequest, context: P2PContext) -> dht_pb2.FindResponse:
    """Answer, but late. Never silent, so no silence detector can see it.

    The annotations are load-bearing: `p2p/servicer.py:_collect_rpc_handlers` reads them
    to register the handler and raises at startup without them (HANDOFF Trap 3).
    """
    entry = SLOW.get(id(self))
    if entry is None and SLOW_THIS_PROCESS > 0:
        entry = (SLOW_THIS_PROCESS, SLOW_ONLY_FOR)
    if entry is not None:
        delay, only_for = entry
        if only_for is None or context.remote_id.to_bytes() == only_for:
            DELAYS_APPLIED.append(delay)
            await asyncio.sleep(delay)
    return await _ORIGINAL_RPC_FIND(self, request, context)


DHTProtocol.rpc_find = _slow_rpc_find


class NodeAsDHT:
    """Lets `GroupKeyManager` drive a `DHTNode` directly (same adapter as step5/step8)."""

    def __init__(self, node: DHTNode):
        self._node = node
        self.peer_id = node.peer_id

    async def store(self, *args, return_future: bool = False, **kwargs):
        return await self._node.store(*args, **kwargs)

    async def get(self, *args, return_future: bool = False, **kwargs):
        return await self._node.get(*args, **kwargs)


def manager(node: DHTNode) -> GroupKeyManager:
    return GroupKeyManager(NodeAsDHT(node), prefix=PREFIX, initial_group_bits="", target_group_size=None)


async def build_swarm(num_honest: int):
    """Bootstrap, `num_honest` honest peers, one observer with a random node_id.

    The observer's node_id is deliberately *not* crafted. If Observe needs custody, the
    cheapness claim is gone and that is the result.
    """
    bootstrap = await DHTNode.create(host_maddrs=LOCALHOST)
    initial = [str(a) for a in await bootstrap.get_visible_maddrs()]
    honest = [await DHTNode.create(initial_peers=initial, host_maddrs=LOCALHOST) for _ in range(num_honest)]
    observer = await DHTNode.create(initial_peers=initial, host_maddrs=LOCALHOST)
    return bootstrap, honest, observer


async def shutdown(*groups):
    for g in groups:
        nodes = g if isinstance(g, list) else [g]
        for n in nodes:
            await n.shutdown()


# --------------------------------------------------------------------------- phase 1


async def phase1(num_honest: int, lifetime: float, repeat: int):
    """Observe: what can one ordinary peer see, and how far ahead?

    The group key is a forward hash of a public string (C6.S), so full visibility is the
    expectation. The number worth having is **advance notice** -- how long before a
    victim's declared deadline the attacker can know the victim is looking for a group.
    Every Select attack in this project assumes that window exists; none has measured it.
    """
    bootstrap, honest, observer = await build_swarm(num_honest)
    rows = []
    try:
        deadline = get_dht_time() + lifetime
        declared_at = {}
        for node in honest:
            t0 = time.perf_counter()
            ok = await manager(node).declare_averager(GROUP_KEY, node.peer_id, expiration_time=deadline)
            declared_at[node.peer_id.to_bytes()] = (t0, ok)

        # how long until an uninvolved peer can see them, and with what notice
        t_poll = time.perf_counter()
        seen = await manager(observer).get_averagers(GROUP_KEY, only_active=True)
        poll_elapsed = time.perf_counter() - t_poll
        now = get_dht_time()

        for peer_id, expiration in seen:
            rows.append(
                dict(
                    repeat=repeat,
                    num_honest=num_honest,
                    peer=peer_id.to_bytes().hex()[:16],
                    deadline_visible=round(expiration, 3),
                    advance_notice_s=round(expiration - now, 3),
                    read_latency_s=round(poll_elapsed, 4),
                )
            )
        print(
            f"  observer saw {len(seen)}/{num_honest} advertisers in {poll_elapsed:.3f}s; "
            f"deadlines readable: {all(r['deadline_visible'] > 0 for r in rows)}; "
            f"advance notice {min((r['advance_notice_s'] for r in rows), default=0):.2f}"
            f"-{max((r['advance_notice_s'] for r in rows), default=0):.2f}s"
        )
    finally:
        await shutdown(bootstrap, honest, observer)
    return rows


# --------------------------------------------------------------------------- phase 3


async def phase3_arm(num_honest: int, delay: float, ad_lifetime: float, window: float, repeat: int):
    """Does a stale view cost the victim anything?

    Phase 2 showed delay does not shrink a *static* advertiser set. But matchmaking is not
    static: peers declare, re-declare and expire continuously, and leadership goes to the
    earliest declared deadline (`matchmaking.py:478-481`). Two things to measure:

      * **staleness** -- the lag between a peer advertising and the victim observing it;
      * **misses** -- whether an advertisement whose lifetime is shorter than the victim's
        read can be seen at all.

    The second is the real question. If a short-lived advertisement expires inside the
    victim's blind window, delay achieves section 4.6's censorship with one ordinary peer
    instead of `bucket_size` crafted identities and the custody they buy.

    **Both arms poll for the same wall-clock `window`**, not the same number of reads --
    that is the whole point, since the delayed victim completes far fewer reads in it. An
    earlier version fixed the read count instead and the control finished before the
    advertisement was even written, which is a broken control rather than a result.
    """
    SLOW.clear()
    bootstrap, honest, victim = await build_swarm(num_honest)
    flasher = await DHTNode.create(
        initial_peers=[str(a) for a in await bootstrap.get_visible_maddrs()], host_maddrs=LOCALHOST
    )
    rows = []
    try:
        background = get_dht_time() + 120.0
        for node in honest:
            await manager(node).declare_averager(GROUP_KEY, node.peer_id, expiration_time=background)
        await manager(victim).get_averagers(GROUP_KEY, only_active=True)  # warm both arms alike

        if delay > 0:
            SLOW[id(honest[0].protocol)] = (delay, None)

        flasher_key = flasher.peer_id.to_bytes()
        declared_at = None
        first_seen = None
        reads_done = 0

        async def flash():
            nonlocal declared_at
            await asyncio.sleep(0.5)
            declared_at = time.perf_counter()
            await manager(flasher).declare_averager(
                GROUP_KEY, flasher.peer_id, expiration_time=get_dht_time() + ad_lifetime
            )

        flash_task = asyncio.create_task(flash())
        t_end = time.perf_counter() + window
        first_start_after_declare = None
        while time.perf_counter() < t_end:
            t_start = time.perf_counter()
            if first_start_after_declare is None and declared_at is not None and t_start >= declared_at:
                first_start_after_declare = t_start
            seen = await manager(victim).get_averagers(GROUP_KEY, only_active=True)
            reads_done += 1
            if first_seen is None and any(p.to_bytes() == flasher_key for p, _ in seen):
                first_seen = time.perf_counter()
                break
            await asyncio.sleep(0.05)  # same inter-read pause in both arms
        await flash_task

        rows.append(
            dict(
                repeat=repeat,
                num_honest=num_honest,
                delay_s=delay,
                ad_lifetime_s=ad_lifetime,
                window_s=window,
                reads_completed=reads_done,
                flasher_observed=int(first_seen is not None),
                staleness_s=round(first_seen - declared_at, 3) if first_seen and declared_at else None,
                # when the victim first got a chance to sample while the record was live;
                # if this exceeds ad_lifetime_s the record expired inside the blind window
                first_sample_after_declare_s=(
                    round(first_start_after_declare - declared_at, 3)
                    if first_start_after_declare and declared_at
                    else None
                ),
            )
        )
    finally:
        SLOW.clear()
        await shutdown(bootstrap, honest, victim, flasher)
    return rows


# --------------------------------------------------------------------------- phase 4


def phase4_arm(num_peers: int, delay: float, matchmaking_time: float, run_id: str, repeat: int,
               group_size: int | None = None):
    """End to end: does DHT-layer delay degrade real averaging?

    Phases 1-3 measure the DHT read in isolation. This runs stock `DecentralizedAverager`
    peers -- **no subclass anywhere**, because the attack is entirely at the DHT layer, so
    Trap 3 does not arise -- alongside one extra peer that contributes no gradients and
    only answers `rpc_find` slowly.

    That peer is the whole attacker: one ordinary process, a random `node_id`, no custody,
    no crafted identities. Compare section 4.6, which needed `bucket_size` = 20 crafted
    identities plus custody to interfere with a victim's view.

    Reported per peer: whether its averaging step succeeded, and how long it took.
    """
    global SLOW_THIS_PROCESS
    bootstrap = hivemind.DHT(start=True, host_maddrs=LOCALHOST)
    initial = bootstrap.get_visible_maddrs()

    # fork the slow peer's DHT while the flag is set, so its child inherits it
    SLOW_THIS_PROCESS = delay
    attacker_dht = hivemind.DHT(initial_peers=initial, start=True, host_maddrs=LOCALHOST)
    SLOW_THIS_PROCESS = 0.0

    dhts = [hivemind.DHT(initial_peers=initial, start=True, host_maddrs=LOCALHOST) for _ in range(num_peers)]
    averagers = [
        DecentralizedAverager(
            [torch.zeros(4)],
            dht,
            prefix=run_id,
            # HANDOFF section 5: binding target_group_size at 16 peers starves the swarm,
            # so vary swarm size and group size independently rather than tying them.
            target_group_size=group_size or num_peers,
            min_group_size=2,
            min_matchmaking_time=matchmaking_time,
            request_timeout=3.0,
            start=True,
        )
        for dht in dhts
    ]

    rows = []
    try:
        time.sleep(2.0)  # let routing tables settle in both arms alike
        started = time.perf_counter()
        controls = [a.step(wait=False, allow_retries=False, timeout=90) for a in averagers]
        for i, control in enumerate(controls):
            try:
                control.result()
                ok, err = 1, ""
            except Exception as exc:
                ok, err = 0, type(exc).__name__
            rows.append(
                dict(
                    repeat=repeat,
                    peer=i,
                    num_peers=num_peers,
                    delay_s=delay,
                    matchmaking_time_s=matchmaking_time,
                    averaged=ok,
                    elapsed_s=round(time.perf_counter() - started, 2),
                    error=err,
                )
            )
    finally:
        for a in averagers:
            a.shutdown()
        for d in dhts + [attacker_dht, bootstrap]:
            d.shutdown()
    return rows


# --------------------------------------------------------------------------- phase 5


def would_follow(own_peer_id: PeerID, own_expiration: float, candidates: list) -> PeerID | None:
    """Hivemind's leader choice, reproduced exactly. Returns the leader, or None if this peer leads.

    Two steps, and they use **two different orderings of the same identity**:

      1. `leader_queue.top()` returns the earliest entry. The queue is a `TimedStorage`
         whose heap holds `HeapEntry(expiration_time, key)` as an `order=True` dataclass
         (`timed_storage.py:44-47`), so ties on expiration fall through to `key`, a
         `PeerID` — and `PeerID.__lt__` compares **base58 strings**
         (`datastructures.py:51-54`).
      2. The follow decision compares that candidate against this peer's own declaration
         using **raw bytes**: `(entry.expiration_time, leader.to_bytes()) > (own_expiration,
         own.to_bytes())` (`matchmaking.py:478-481`). Greater means "do not follow", i.e. lead.

    The two orderings agree for equal-length identifiers but are not the same comparison.
    """
    if not candidates:
        return None
    leader, leader_expiration = min(candidates, key=lambda c: (c[1], c[0].to_base58()))
    if (leader_expiration, leader.to_bytes()) > (own_expiration, own_peer_id.to_bytes()):
        return None
    return leader


async def phase5_arm(
    num_honest: int, arm: str, blind_time: float, lifetime: float, rounds: int, repeat: int,
    attacker_lowest_id: bool = False, attacker_latency: float = 0.0,
):
    """Can an attacker that *reads* the advertisements win the leader election?

    Leadership is the whole bottleneck: section 4.3 showed that when the attacker leads,
    admission is exact (beta = k/(k+1), no variance). So the election is where capture is
    decided.

    C11.T established that an attacker cannot *undercut* the earliest deadline, because
    expiration is simultaneously the record's lifetime and its priority -- declaring earlier
    makes the advertisement expire rather than win. This tests the move that sidesteps that:
    read the current advertisements (C11.Obs, free), **match the minimum exactly**, and take
    the tie on `peer_id`. Matching costs no lifetime at all.

    Arms:
      `none`   -- no attacker. Baseline for how leadership falls among honest peers.
      `blind`  -- section 4.3's strategy: declare a fixed early deadline without reading.
      `mirror` -- declare exactly the minimum observed expiration.

    **The election is sampled repeatedly across the matchmaking window, not once.** That is
    the whole point of C11.T: an advertisement declared early wins while it exists and then
    expires, so a single snapshot flatters the blind attacker enormously. Sampling over the
    window is what makes the lifetime cost visible.

    The tie-break is decided by the attacker's `peer_id`, which it does not choose here, so
    each sample records whether the attacker's id was the lowest. Reporting the follow rate
    conditional on that separates the two ingredients -- mirroring and the tie-break --
    without needing to grind an identity first.
    """
    bootstrap, honest, attacker = await build_swarm(num_honest)
    if attacker_lowest_id:
        # Grinding a low peer_id is cheap -- PeerID is a hash of a public key, and to be the
        # lowest of N you need about N keypairs in expectation, not exponential work. Rather
        # than grind here, the role is assigned to whichever node already holds the lowest
        # id, which measures exactly what grinding would buy. `identity_path` makes PeerIDs
        # choosable offline in a real attack (HANDOFF section 6).
        pool = honest + [attacker]
        pool.sort(key=lambda n: n.peer_id.to_bytes())
        attacker, honest = pool[0], pool[1:]
    rows = []
    try:
        for rnd in range(rounds):
            now = get_dht_time()
            declared = {}
            for node in honest:
                exp = now + lifetime
                await manager(node).declare_averager(GROUP_KEY, node.peer_id, expiration_time=exp)
                declared[node.peer_id] = exp

            observed = await manager(attacker).get_averagers(GROUP_KEY, only_active=True)
            if attacker_latency:
                # A real attacker's read and write cross a network. Loopback makes both ~3 ms,
                # which is the best case and must not be reported as the general one. This
                # spends the budget between reading and declaring, where it actually hurts:
                # every millisecond here is lifetime the advertisement never gets.
                await asyncio.sleep(attacker_latency)
            attacker_exp = None
            if arm == "blind":
                attacker_exp = now + blind_time
            elif arm == "mirror" and observed:
                attacker_exp = min(e for _, e in observed)  # verbatim, no undercut
            elif arm == "undercut" and observed:
                # C11.T calls undercutting prevented, on the grounds that lowering the
                # deadline to win makes the advertisement expire instead. That holds for
                # lowering by seconds. np.nextafter lowers it by ~2.4e-7 s at these
                # timestamps -- 240 nanoseconds of lifetime for an outright win that needs
                # no low peer_id at all. Same primitive hivemind itself uses for withdrawal
                # (key_manager.py:60), pointed the other way.
                live = [e for _, e in observed if e - now > REQUEST_TIMEOUT]
                attacker_exp = float(np.nextafter(min(live), -np.inf)) if live else None
            elif arm == "mirror_live" and observed:
                # Mirroring the *global* minimum is fragile: peers re-declare continuously, so
                # the earliest record on the key is often one about to expire, and copying it
                # buys a dead advertisement. A leader whose deadline is closer than
                # `request_timeout` cannot be joined in time anyway -- matchmaking.py:53
                # requires request_timeout < min_matchmaking_time for that reason. So mirror
                # the earliest deadline that is still joinable.
                live = [e for _, e in observed if e - now > REQUEST_TIMEOUT]
                attacker_exp = min(live) if live else None
            if attacker_exp is not None:
                await manager(attacker).declare_averager(
                    GROUP_KEY, attacker.peer_id, expiration_time=attacker_exp
                )

            # sample the election across the window the honest peers declared for
            for frac in (0.1, 0.3, 0.5, 0.7, 0.9):
                target = now + lifetime * frac
                while get_dht_time() < target:
                    await asyncio.sleep(min(0.1, target - get_dht_time()))
                board = await manager(honest[0]).get_averagers(GROUP_KEY, only_active=True)
                ids = [pid for pid, _ in board]
                attacker_visible = attacker.peer_id in ids
                attacker_lowest = bool(ids) and min(ids, key=lambda x: x.to_bytes()) == attacker.peer_id

                follows_attacker = leaders = 0
                for node in honest:
                    cands = [(pid, e) for pid, e in board if pid != node.peer_id]
                    leader = would_follow(node.peer_id, declared[node.peer_id], cands)
                    if leader is None:
                        leaders += 1
                    elif leader == attacker.peer_id:
                        follows_attacker += 1

                rows.append(
                    dict(
                        repeat=repeat,
                        round=rnd,
                        window_frac=frac,
                        num_honest=num_honest,
                        arm=arm,
                        attacker_lowest_id_by_design=int(attacker_lowest_id),
                        attacker_latency_s=attacker_latency,
                        lifetime_s=lifetime,
                        attacker_declared=round(attacker_exp - now, 3) if attacker_exp else None,
                        attacker_visible=int(attacker_visible),
                        attacker_id_lowest=int(attacker_lowest),
                        honest_following_attacker=follows_attacker,
                        honest_leading_themselves=leaders,
                        advertisers_visible=len(board),
                    )
                )
    finally:
        await shutdown(bootstrap, honest, attacker)
    return rows


# --------------------------------------------------------------------------- phase 6


async def phase6_arm(num_honest: int, samples: int, repeat: int):
    """Baseline cost of a DHT write and a DHT read, on an otherwise healthy swarm.

    Everything else in this file measures what an attacker does to these numbers. This
    measures the numbers themselves, which is what the protocol's own timeouts have to be
    read against: `wait_timeout` 3 s bounds one RPC, `min_matchmaking_time` 5 s is the whole
    advertisement lifetime.

    `declare_averager` is the write (`key_manager.py:57-68`, a `dht.store` to `num_replicas`
    peers). `get_averagers` is the read (`key_manager.py:79`, a `dht.get` with `latest=True`,
    which forbids the traversal from short-circuiting). Loopback, so these are floors: a real
    deployment pays network round trips on top.
    """
    bootstrap, honest, observer = await build_swarm(num_honest)
    rows = []
    try:
        for i in range(samples):
            deadline = get_dht_time() + 30.0
            node = honest[i % len(honest)]
            t0 = time.perf_counter()
            ok = await manager(node).declare_averager(GROUP_KEY, node.peer_id, expiration_time=deadline)
            t_write = time.perf_counter() - t0

            t0 = time.perf_counter()
            seen = await manager(observer).get_averagers(GROUP_KEY, only_active=True)
            t_read = time.perf_counter() - t0

            rows.append(
                dict(
                    repeat=repeat,
                    sample=i,
                    num_honest=num_honest,
                    write_s=round(t_write, 5),
                    read_s=round(t_read, 5),
                    write_accepted=int(bool(ok)),
                    advertisers_seen=len(seen),
                )
            )
    finally:
        await shutdown(bootstrap, honest, observer)
    return rows


# --------------------------------------------------------------------------- phase 2


async def phase2_arm(
    num_honest: int, num_slow: int, delay: float, lifetime: float, reads: int, repeat: int, craft: bool = False
):
    """Delay: does one slow responder stretch a victim's candidate read, and does it
    change what the victim sees?

    `num_slow=0` with `delay=0` is the matched honest control, run in the same process
    and the same swarm shape, differing in exactly one behaviour.
    """
    SLOW.clear()
    DELAYS_APPLIED.clear()
    bootstrap, honest, victim = await build_swarm(num_honest)
    initial_addrs = [str(a) for a in await bootstrap.get_visible_maddrs()]
    crafted = None
    rows = []
    try:
        deadline = get_dht_time() + lifetime
        accepted = 0
        for node in honest:
            if await manager(node).declare_averager(GROUP_KEY, node.peer_id, expiration_time=deadline):
                accepted += 1

        # a warm read so routing tables are populated in both arms alike
        await manager(victim).get_averagers(GROUP_KEY, only_active=True)

        # arm the slow responders -- everyone except the victim is a candidate.
        # With --craft the crafted node is the *only* attacker, otherwise the arm would
        # contain two of them and conflate address choice with attacker count.
        if not craft:
            for node in honest[:num_slow]:
                SLOW[id(node.protocol)] = (delay, None)

        # With a random node_id the attacker is only slow *if the victim happens to ask it*,
        # which stops being reliable once the swarm outgrows the routing table -- the same
        # seed-set law as C7.S. Crafting the id to XOR distance 0 from the group key puts the
        # attacker among the replicas, so it is asked every time. Cost is C6.S's: ~0.1 s.
        crafted = None
        if craft and delay > 0:
            target = DHTID.generate(source=GROUP_KEY)
            crafted = await DHTNode.create(
                node_id=DHTID(int(target)), initial_peers=initial_addrs, host_maddrs=LOCALHOST
            )
            SLOW[id(crafted.protocol)] = (delay, None)

        for i in range(reads):
            t0 = time.perf_counter()
            seen = await manager(victim).get_averagers(GROUP_KEY, only_active=True)
            elapsed = time.perf_counter() - t0
            watch = honest[:num_slow] + ([crafted] if crafted is not None else [])
            banned = sum(1 for n in watch if n.peer_id in victim.blacklist)
            rows.append(
                dict(
                    repeat=repeat,
                    read=i,
                    num_honest=num_honest,
                    num_slow=num_slow,
                    delay_s=delay,
                    read_latency_s=round(elapsed, 4),
                    advertisers_seen=len(seen),
                    advertisers_declared=accepted,
                    slow_peers_blacklisted=banned,
                    crafted_node_id=int(craft),
                )
            )
    finally:
        SLOW.clear()
        await shutdown(bootstrap, honest, victim)
        if crafted is not None:
            await crafted.shutdown()
    return rows


def summarise(rows, label):
    lat = [r["read_latency_s"] for r in rows]
    seen = [r["advertisers_seen"] for r in rows]
    banned = max(r["slow_peers_blacklisted"] for r in rows)
    print(
        f"  {label:<28} latency {statistics.median(lat):6.3f}s "
        f"(min {min(lat):.3f} max {max(lat):.3f})   "
        f"seen {min(seen)}-{max(seen)}/{rows[0]['advertisers_declared']}   "
        f"blacklisted {banned}"
    )


def write_csv(rows, name):
    RESULTS_DIR.mkdir(exist_ok=True)
    path = RESULTS_DIR / name
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"  -> {path.relative_to(Path(__file__).resolve().parents[2])}")


async def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--phase", type=int, required=True, choices=[1, 2, 3, 4, 5, 6])
    ap.add_argument("--samples", type=int, default=12, help="phase 6 write/read samples")
    ap.add_argument("--rounds", type=int, default=6, help="phase 5 election rounds per arm")
    ap.add_argument("--blind-time", type=float, default=1.0, help="phase 5 blind arm deadline")
    ap.add_argument("--attacker-latency", type=float, nargs="+", default=[0.0],
                    help="phase 5: seconds between the attacker reading and declaring")
    ap.add_argument("--num-peers", type=int, default=4, help="phase 4 averaging peers")
    ap.add_argument("--matchmaking-time", type=float, default=5.0, help="phase 4, hivemind default 5.0")
    ap.add_argument("--group-size", type=int, default=None, help="phase 4 target_group_size (default: all peers)")
    ap.add_argument("--num-honest", type=int, default=8)
    ap.add_argument("--lifetime", type=float, default=30.0, help="declared availability lifetime (s)")
    ap.add_argument("--reads", type=int, default=5, help="candidate reads per arm (phase 2)")
    ap.add_argument("--repeats", type=int, default=2)
    ap.add_argument("--delays", type=float, nargs="+", default=[0.0, 0.5, 1.5, 2.9, 3.5])
    ap.add_argument("--slow", type=int, nargs="+", default=[1], help="how many responders are slow")
    ap.add_argument("--craft", action="store_true",
                    help="phase 2: the slow peer gets a node_id at XOR distance 0 from the group key")
    ap.add_argument("--ad-lifetimes", type=float, nargs="+", default=[1.0, 2.0, 5.0, 20.0])
    ap.add_argument("--window", type=float, default=12.0, help="phase 3 polling window (s), same in both arms")
    args = ap.parse_args()

    if args.phase == 1:
        print(f"phase 1 -- Observe, {args.num_honest} honest peers, one ordinary observer")
        all_rows = []
        for r in range(args.repeats):
            print(f" repeat {r}")
            all_rows += await phase1(args.num_honest, args.lifetime, r)
        write_csv(all_rows, f"step10_observe_n{args.num_honest}_l{args.lifetime:g}.csv")
        return

    if args.phase == 3:
        print(f"phase 3 -- does a stale view cost anything? {args.num_honest} honest peers")
        all_rows = []
        for r in range(args.repeats):
            print(f" repeat {r}")
            for life in args.ad_lifetimes:
                for d in [0.0] + [x for x in args.delays if x > 0 and x < 3.0]:
                    rows = await phase3_arm(args.num_honest, d, life, args.window, r)
                    row = rows[0]
                    arm = "control" if d == 0 else f"delay {d}s"
                    print(
                        f"  ad_lifetime {life:>5.1f}s  {arm:<12} "
                        f"observed={'YES' if row['flasher_observed'] else ' NO'}  "
                        f"staleness={row['staleness_s']}  reads={row['reads_completed']}"
                    )
                    all_rows += rows
        # HANDOFF section 5: the filename must encode every variable that changes the
        # result. An earlier version keyed only on --reads, which is not even used by this
        # phase, so a second sweep with different lifetimes and delays silently overwrote
        # the first. Window, delays and lifetimes all change the answer; all three go in.
        tag = (
            f"w{args.window:g}"
            f"_d{'-'.join(f'{x:g}' for x in args.delays)}"
            f"_l{'-'.join(f'{x:g}' for x in args.ad_lifetimes)}"
        )
        write_csv(all_rows, f"step10_stale_n{args.num_honest}_{tag}.csv")
        return

    if args.phase == 6:
        print(f"phase 6 -- baseline DHT write and read cost, {args.num_honest} honest peers")
        all_rows = []
        for r in range(args.repeats):
            rows = await phase6_arm(args.num_honest, args.samples, r)
            w = sorted(x["write_s"] for x in rows)
            rd = sorted(x["read_s"] for x in rows)
            print(
                f"  repeat {r}  write median {w[len(w)//2]*1000:7.2f} ms (min {w[0]*1000:.2f} max {w[-1]*1000:.2f})"
                f"   read median {rd[len(rd)//2]*1000:7.2f} ms (min {rd[0]*1000:.2f} max {rd[-1]*1000:.2f})"
            )
            all_rows += rows
        write_csv(all_rows, f"step10_baseline_rw_n{args.num_honest}_s{args.samples}.csv")
        return

    if args.phase == 5:
        print(f"phase 5 -- leader election, {args.num_honest} honest peers + 1 attacker")
        all_rows = []
        for r in range(args.repeats):
            # undercut carries NO low peer_id -- that is the point of the arm.
            arms_run = tuple(
                (arm, low, lat)
                for lat in args.attacker_latency
                for arm, low in (("none", False), ("mirror_live", True), ("undercut", False))
            )
            for arm, low, lat in arms_run:
                rows = await phase5_arm(
                    args.num_honest, arm, args.blind_time, args.lifetime, args.rounds, r,
                    attacker_lowest_id=low, attacker_latency=lat,
                )
                tot = sum(x["honest_following_attacker"] for x in rows)
                den = sum(x["num_honest"] for x in rows)
                low = sum(x["attacker_id_lowest"] for x in rows)
                vis = sum(x["attacker_visible"] for x in rows)
                won = sum(x["honest_following_attacker"] for x in rows if x["attacker_id_lowest"])
                wdn = sum(x["num_honest"] for x in rows if x["attacker_id_lowest"])
                cond = f"{won}/{wdn}" if wdn else "n/a"
                label = arm + ("+lowid" if low else "") + (f"@{lat:g}s" if lat else "")
                print(
                    f"  repeat {r} {label:<14} followers {tot:>5}/{den:<6} "
                    f"({100*tot/den:5.1f}%)  ad visible {vis}/{len(rows)} samples"
                    f"  id lowest {low}/{len(rows)}  followers when lowest: {cond}"
                )
                all_rows += rows
        # HANDOFF section 5, and this is the second time it has bitten in one session: the
        # arm set changes the result, so it must be in the filename. Without it a rerun with
        # different arms silently overwrites the previous one.
        armtag = "-".join(sorted({a + ("L" if low else "") for a, low, _ in arms_run}))
        armtag += "_lat" + "-".join(f"{x:g}" for x in args.attacker_latency)
        write_csv(
            all_rows,
            f"step10_election_n{args.num_honest}_l{args.lifetime:g}_b{args.blind_time:g}_{armtag}.csv",
        )
        return

    if args.phase == 4:
        print(
            f"phase 4 -- end to end, {args.num_peers} averaging peers + 1 slow DHT-only peer, "
            f"min_matchmaking_time={args.matchmaking_time}s"
        )
        all_rows = []
        for r in range(args.repeats):
            for d in [0.0] + [x for x in args.delays if 0 < x < 3.0]:
                rows = phase4_arm(args.num_peers, d, args.matchmaking_time, f"step10d{r}{int(d*10)}", r,
                                  group_size=args.group_size)
                ok = sum(x["averaged"] for x in rows)
                el = [x["elapsed_s"] for x in rows]
                arm = "control" if d == 0 else f"delay {d}s"
                print(f"  repeat {r} {arm:<12} averaged {ok}/{len(rows)}  elapsed {min(el):.1f}-{max(el):.1f}s")
                all_rows += rows
        write_csv(all_rows, f"step10_e2e_n{args.num_peers}_g{args.group_size or args.num_peers}_mm{args.matchmaking_time:g}.csv")
        return

    print(f"phase 2 -- Delay, {args.num_honest} honest peers, wait_timeout=3.0s")
    all_rows = []
    for r in range(args.repeats):
        print(f" repeat {r}")
        control = await phase2_arm(args.num_honest, 0, 0.0, args.lifetime, args.reads, r)
        summarise(control, "control (no delay)")
        all_rows += control
        for k in args.slow:
            for d in args.delays:
                if d == 0.0:
                    continue
                rows = await phase2_arm(args.num_honest, k, d, args.lifetime, args.reads, r, craft=args.craft)
                summarise(rows, f"{k} slow @ {d}s")
                all_rows += rows
    write_csv(
        all_rows,
        f"step10_delay_n{args.num_honest}_k{'-'.join(map(str, args.slow))}"
        f"_d{'-'.join(f'{x:g}' for x in args.delays)}_{'crafted' if args.craft else 'random'}.csv",
    )


if __name__ == "__main__":
    # Trap 2: python 3.14 defaults to forkserver on linux, which cannot pickle an
    # averager; it also breaks the fork-inherited SLOW_THIS_PROCESS flag above.
    mp.set_start_method("fork", force=True)
    asyncio.run(main())
