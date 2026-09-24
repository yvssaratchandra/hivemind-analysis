"""
C12.S, reconciled against live matchmaking.

C12.S demonstrated that an attacker which reads the group key and either matches the earliest
joinable deadline (taking the tie on a low `peer_id`) or undercuts it by one float step is
followed by every honest peer, at 32 and at 128 peers. **But that election was computed by
applying hivemind's own comparison rule to real DHT records, not observed in a running swarm.**

The worksheet says not to quote the 100 % as a capture rate until that is closed, and for a
concrete reason: the *same* blind strategy scores 57-67 % in §4.3's live matchmaking and only
15-20 % under the simulation. The simulation is already known to disagree with reality on the
one arm where both numbers exist, so the disagreement has to be explained before the headline
number means anything.

**Arm 2 is what makes this interpretable.** It reruns §4.3's blind strategy here. If this
harness reproduces 57-67 %, a difference in arm 3 is attributable to the strategy. If it does
not, nothing in the run is interpretable and the harness is what needs fixing.

**The patch.** `_declare_averager_periodically` (`matchmaking.py:524`) computes
`now + min_matchmaking_time` and declares it. For the attacker, read the group key first and
declare `nextafter(earliest joinable, -inf)` instead. Expirations are absolute timestamps near
1.79e9, where one float step is ~2.4e-7 s, so the undercut costs 240 nanoseconds of
advertisement life for an outright win. `Matchmaking` is not a servicer, so patching it at class
level does not rename an RPC handler and Trap 3 does not apply.

**No abstention logic is needed.** Setting `declared_expiration_time` to the earliest value is
self-consistent: `pop_next_leader` then finds no candidate beating the attacker's own
declaration, so it waits to be contacted and leads by construction.

Run:
    .venv/bin/python research/step12_election_live.py --honest 7 --group-size 4 --rounds 4
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import multiprocessing as mp
import time
from pathlib import Path
from typing import AsyncIterator, Optional

import numpy as np
import torch

import hivemind
from hivemind.averaging import DecentralizedAverager
from hivemind.averaging.matchmaking import PotentialLeaders
from hivemind.p2p import P2PContext
from hivemind.proto import averaging_pb2
from hivemind.utils.timed_storage import get_dht_time

RESULTS_DIR = Path(__file__).resolve().parent / "results"
LOCALHOST = ["/ip4/127.0.0.1/tcp/0"]
ARMS = ("none", "blind", "undercut")

EVENTS: Optional[mp.Queue] = None

# Set in the parent immediately before forking the attacker; the child inherits it. "" means an
# ordinary peer. Same mechanism as step10's SLOW_THIS_PROCESS and step11's ABSTAIN_THIS_PROCESS.
STRATEGY_THIS_PROCESS = ""
BLIND_TIME = 1.0
# PotentialLeaders does not carry request_timeout, so the joinability floor is a constant
# here, matching hivemind's default (averager.py:122).
REQUEST_TIMEOUT = 3.0

# The declaration loop lives on PotentialLeaders, not Matchmaking. Neither is a
# servicer, so a class-level patch renames no RPC handler and Trap 3 does not apply.
_ORIGINAL_DECLARE = PotentialLeaders._declare_averager_periodically


async def _patched_declare(self, step, key_manager) -> None:
    """Declare on the attacker's terms, or fall through to hivemind's own loop.

    Honest peers are untouched, so the only difference between arms is what the attacker
    writes to the availability key.
    """
    if not STRATEGY_THIS_PROCESS:
        return await _ORIGINAL_DECLARE(self, step, key_manager)

    async with self.lock_declare:
        try:
            while True:
                await self.running.wait()
                now = get_dht_time()
                honest_time = float(
                    min(max(step.scheduled_time, now + self.min_matchmaking_time), self.search_end_time)
                )

                if STRATEGY_THIS_PROCESS == "blind":
                    # §4.3's strategy: a fixed early deadline, declared without reading anything.
                    declared = now + BLIND_TIME
                else:
                    # Read what everyone else declared, then go one float step below the earliest
                    # one that is still joinable. A deadline closer than request_timeout cannot be
                    # joined in time anyway (matchmaking.py:53), and mirroring one that is about to
                    # expire buys a dead advertisement -- C12.S measured that failure directly.
                    try:
                        seen = await key_manager.get_averagers(key_manager.current_key, only_active=True)
                    except Exception:
                        seen = []
                    live = [e for _, e in seen if e - now > REQUEST_TIMEOUT]
                    declared = float(np.nextafter(min(live), -np.inf)) if live else honest_time

                self.declared_group_key = group_key = key_manager.current_key
                self.declared_expiration_time = declared
                self.declared_expiration.set()
                ok = await key_manager.declare_averager(group_key, self.peer_id, expiration_time=declared)
                if EVENTS is not None:
                    # Log what was actually declared, so the patch is verified rather than assumed.
                    EVENTS.put(("declared", str(self.peer_id), f"{declared - now:.4f}",
                                f"honest_would_be={honest_time - now:.4f} accepted={bool(ok)}"))
                await asyncio.sleep(max(0.0, self.declared_expiration_time - get_dht_time()))
                if self.running.is_set() and len(self.leader_queue) == 0:
                    await key_manager.update_key_on_not_enough_peers()
        finally:
            if self.declared_group_key is not None:
                prev_key, prev_exp = self.declared_group_key, self.declared_expiration_time
                self.declared_group_key, self.declared_expiration_time = None, float("inf")
                await key_manager.declare_averager(prev_key, self.peer_id, prev_exp, looking_for_group=False)


PotentialLeaders._declare_averager_periodically = _patched_declare


class ElectionAverager(DecentralizedAverager):
    """One class for every peer (Trap 3). Only logs; the strategy lives in the patch above."""

    async def rpc_join_group(
        self, request: averaging_pb2.JoinRequest, context: P2PContext
    ) -> AsyncIterator[averaging_pb2.MessageFromLeader]:
        # Annotations are load-bearing (`p2p/servicer.py:_collect_rpc_handlers`).
        if EVENTS is not None:
            EVENTS.put(("join_received", str(self.peer_id), str(context.remote_id), ""))
        async for response in super().rpc_join_group(request, context):
            yield response


def drain(q: mp.Queue) -> list:
    out = []
    while True:
        try:
            out.append(q.get_nowait())
        except Exception:
            return out


def run_arm(arm: str, honest: int, group_size: int, rounds: int, mm_time: float, run_id: str, repeat: int):
    global STRATEGY_THIS_PROCESS, EVENTS
    EVENTS = mp.Queue()

    bootstrap = hivemind.DHT(start=True, host_maddrs=LOCALHOST)
    initial = bootstrap.get_visible_maddrs()
    dhts = [hivemind.DHT(initial_peers=initial, start=True, host_maddrs=LOCALHOST) for _ in range(honest + 1)]

    common = dict(prefix=run_id, target_group_size=group_size, min_group_size=2,
                  request_timeout=3.0, min_matchmaking_time=mm_time, start=True)

    peers, attacker = [], None
    if arm == "none":
        # Match on participant count, not just behaviour: E01 showed that adding a peer changes
        # group divisibility and that effect gets misattributed to the attacker.
        peers.append(ElectionAverager([torch.zeros(4)], dhts[0], **common))
    else:
        STRATEGY_THIS_PROCESS = arm
        attacker = ElectionAverager([torch.zeros(4)], dhts[0], **common)
        STRATEGY_THIS_PROCESS = ""
    for i in range(honest):
        peers.append(ElectionAverager([torch.zeros(4)], dhts[i + 1], **common))

    attacker_id = str(attacker.peer_id) if attacker is not None else ""
    rows, events = [], []
    try:
        time.sleep(2.0)
        for rnd in range(rounds):
            # The attacker must declare *after* the honest peers, or it has nothing to read and
            # the undercut degrades to an honest declaration. This is the opposite of E01, where
            # the abstainer had to be advertising first. It is also a real constraint rather than
            # a harness detail: `TimedStorage.store` only overwrites a subkey when the new
            # expiration is strictly later (`timed_storage.py:81-82`), so an undercut can be
            # declared once per record lifetime and cannot be walked downwards.
            started = time.perf_counter()
            controls = [p.step(wait=False, allow_retries=True, timeout=60) for p in peers]
            if attacker is not None:
                time.sleep(0.5)
                attacker.step(wait=False, allow_retries=True, timeout=60)
            done: dict[int, tuple] = {}
            while len(done) < len(controls) and time.perf_counter() - started < 90:
                for i, c in enumerate(controls):
                    if i in done or not c.done():
                        continue
                    try:
                        c.result(); done[i] = (1, "", time.perf_counter() - started)
                    except Exception as exc:
                        done[i] = (0, type(exc).__name__, time.perf_counter() - started)
                time.sleep(0.05)
            for i in range(len(controls)):
                ok, err, el = done.get(i, (0, "NotFinished", time.perf_counter() - started))
                rows.append(dict(repeat=repeat, round=rnd, peer=i, arm=arm, honest=honest,
                                 group_size=group_size, averaged=ok, elapsed_s=round(el, 2), error=err))
            events += [(e, rnd) for e in drain(EVENTS)]
    finally:
        events += [(e, -1) for e in drain(EVENTS)]
        for p in peers:
            p.shutdown()
        if attacker is not None:
            attacker.shutdown()
        for d in dhts + [bootstrap]:
            d.shutdown()
        EVENTS = None
    return rows, events, attacker_id


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--honest", type=int, default=7)
    ap.add_argument("--group-size", type=int, default=4)
    ap.add_argument("--rounds", type=int, default=4)
    ap.add_argument("--repeats", type=int, default=2)
    ap.add_argument("--matchmaking-time", type=float, default=5.0)
    ap.add_argument("--blind-time", type=float, default=1.0)
    ap.add_argument("--arms", nargs="+", default=list(ARMS), choices=list(ARMS))
    args = ap.parse_args()
    global BLIND_TIME
    BLIND_TIME = args.blind_time

    print(f"C12.S live -- {args.honest} honest + 1 attacker, group {args.group_size}, "
          f"{args.rounds} rounds x {args.repeats} repeats, arms {args.arms}")
    all_rows, all_events = [], []
    for r in range(args.repeats):
        print(f" repeat {r}")
        for arm in args.arms:
            rows, events, aid = run_arm(arm, args.honest, args.group_size, args.rounds,
                                        args.matchmaking_time, f"e12{r}{arm}", r)
            joins = [(e, r) for e, r in events if e[0] == "join_received"]
            to_att = sum(1 for e, _ in joins if e[1] == aid)
            decl = [e for e, _ in events if e[0] == "declared"]
            ok = sum(x["averaged"] for x in rows)
            # Share of all joins is capped by group size over population: with 8 peers at group
            # 4 the attacker can lead one group, so its ceiling is about 3/7. Section 4.3 reports
            # *rounds in which the attacker led*, which is the comparable quantity.
            led_rounds = len({r for e, r in joins if e[1] == aid and r >= 0})
            d = f" declared +{decl[0][2]}s (honest +{decl[0][3].split('=')[1].split()[0]}s)" if decl else ""
            print(f"  {arm:<9} honest averaged {ok:>3}/{len(rows):<4} "
                  f"followers {to_att:>3}/{len(joins):<4}  led {led_rounds}/{args.rounds} rounds{d}")
            all_rows += rows
            all_events += [dict(repeat=r, round=rnd, arm=arm, kind=e[0], peer=e[1], other=e[2],
                                extra=e[3], attacker_id=aid) for e, rnd in events]

    RESULTS_DIR.mkdir(exist_ok=True)
    tag = (f"n{args.honest}_g{args.group_size}_rounds{args.rounds}_rep{args.repeats}"
           f"_mm{args.matchmaking_time:g}_b{args.blind_time:g}_{'-'.join(args.arms)}")
    for name, data in (("step12_election_steps", all_rows), ("step12_election_events", all_events)):
        if not data:
            continue
        path = RESULTS_DIR / f"{name}_{tag}.csv"
        with open(path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(data[0].keys()))
            w.writeheader(); w.writerows(data)
        print(f"  -> {path.relative_to(Path(__file__).resolve().parent.parent)}")


if __name__ == "__main__":
    mp.set_start_method("fork", force=True)  # Trap 2, and what carries the strategy flag
    main()
