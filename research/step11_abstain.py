"""
E01 -- advertise, never follow, and decide what to do with whoever arrives.

Registered in `CONTROL_DATA_AUDIT.md` Part 1b. Spans checkpoints 11 (availability), 12
(leader choice) and 13 (admission), which is why it is a register entry rather than a cell.

**The question.** Can one peer repeatedly deny averaging to others without contributing any
training work, and what does each unit of attacker resource buy in victim disruption?

**Why it is interesting.** Leadership is passive. `matchmaking.py:157` has the *follower*
open the stream, and the healthy-swarm trace agrees on the wire: followers send exactly one
message kind, `rpc_join_group`, and every leader message is a reply on that stream. So a peer
that declines to follow emits no signal at all, and is indistinguishable from one that lost a
matchmaking race. It never enters a state where anything is expected of it, which is why the
audit's omission-versus-commission split does not reach this case: it is neither.

**This also carries the `reject` control that C13.D has been missing.** Arms `reject` and
`stall` differ in exactly one behaviour, so their comparison decides whether stalling costs a
victim more than an immediate refusal would. Do not run that control separately.

**Abstention is surgical.** `_request_join_potential_leaders` (`matchmaking.py:149`) does
three jobs in one coroutine: `begin_search(...)` is what *declares the advertisement*;
`pop_next_leader` then `_request_join_group` is the following; and the `TimeoutError` branch
is what assembles a group once followers have arrived. Suppressing the coroutine would stop
the peer advertising and stop it ever leading, which is the opposite of the experiment. So the
intercept is `_request_join_group` alone: return `None` without touching the network, and the
peer keeps advertising, never sends a join request, and still assembles a group when someone
follows it.

Run:
    .venv/bin/python research/step11_abstain.py --honest 4 --group-size 2 --rounds 3
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import multiprocessing as mp
import time
from pathlib import Path
from typing import AsyncIterator, Optional

import torch

import hivemind
from hivemind.averaging import DecentralizedAverager
from hivemind.averaging.group_info import GroupInfo
from hivemind.averaging.matchmaking import Matchmaking
from hivemind.p2p import P2PContext
from hivemind.proto import averaging_pb2

RESULTS_DIR = Path(__file__).resolve().parent / "results"
LOCALHOST = ["/ip4/127.0.0.1/tcp/0"]

ARMS = ("none", "serve", "reject", "stall")

# Children are separate processes, so everything observed inside them comes back this way.
EVENTS: Optional[mp.Queue] = None

# Set in the parent immediately before constructing the attacker and cleared after; the child
# inherits whatever it held at fork. Same mechanism as step10_delay.py's SLOW_THIS_PROCESS,
# and it is why Trap 2's fork start method is load-bearing beyond the pickling problem.
ABSTAIN_THIS_PROCESS = False

_ORIGINAL_REQUEST_JOIN = Matchmaking._request_join_group


async def _patched_request_join_group(self, leader) -> Optional[GroupInfo]:
    """Log every leader a peer decides to follow, and let the attacker decline silently.

    `Matchmaking` is not a servicer, so patching it at class level does not rename any RPC
    handler and Trap 3 does not apply. Honest peers fall through to the original call, which
    is what makes this the single point where "who did each peer try to follow" is visible.
    """
    if ABSTAIN_THIS_PROCESS:
        # Never contact the leader. No packet leaves this peer, so nothing is declined,
        # timed out or refused; from outside it looks like a peer that simply did not win.
        if EVENTS is not None:
            EVENTS.put(("abstained", str(self.peer_id), str(leader), time.perf_counter()))
        return None
    if EVENTS is not None:
        EVENTS.put(("follow_attempt", str(self.peer_id), str(leader), time.perf_counter()))
    return await _ORIGINAL_REQUEST_JOIN(self, leader)


Matchmaking._request_join_group = _patched_request_join_group


class E01Averager(DecentralizedAverager):
    """One class for every peer, with behaviour switched by a flag.

    HANDOFF Trap 3: the RPC handler name embeds the servicer class name, so a swarm mixing
    `DecentralizedAverager` with a subclass fails to connect in both directions and the error
    reads like a matchmaking failure rather than a naming one.
    """

    def __init__(self, *args, response: str = "serve", **kwargs):
        assert response in ("serve", "reject", "stall")
        self._response = response
        super().__init__(*args, **kwargs)

    async def rpc_join_group(
        self, request: averaging_pb2.JoinRequest, context: P2PContext
    ) -> AsyncIterator[averaging_pb2.MessageFromLeader]:
        # Type annotations are load-bearing: handlers are discovered by reading them
        # (`p2p/servicer.py:_collect_rpc_handlers`), which raises at startup without these.
        if EVENTS is not None:
            EVENTS.put(("join_received", str(self.peer_id), str(context.remote_id), time.perf_counter()))

        if self._response == "reject":
            # An honest, immediate refusal. This is the control for the stall: the follower
            # learns at once and can go elsewhere with its window mostly intact.
            yield averaging_pb2.MessageFromLeader(code=averaging_pb2.NOT_LOOKING_FOR_GROUP)
            return

        async for response in super().rpc_join_group(request, context):
            yield response
            if self._response == "stall" and response.code == averaging_pb2.ACCEPTED:
                accepted_at = time.perf_counter()
                if EVENTS is not None:
                    EVENTS.put(("stall_begin", str(self.peer_id), str(context.remote_id), accepted_at))
                try:
                    # Do not return: returning closes the stream and the follower fails fast.
                    await asyncio.sleep(600)
                except BaseException:
                    if EVENTS is not None:
                        EVENTS.put(
                            ("stall_end", str(self.peer_id), str(context.remote_id),
                             time.perf_counter() - accepted_at)
                        )
                    raise
                return


def drain(queue: mp.Queue) -> list:
    out = []
    while True:
        try:
            out.append(queue.get_nowait())
        except Exception:
            return out


def run_arm(arm: str, honest: int, group_size: int, rounds: int, matchmaking_time: float,
            run_id: str, repeat: int):
    """One arm. Advertising policy is identical across all of them; only the response differs."""
    global ABSTAIN_THIS_PROCESS, EVENTS
    EVENTS = mp.Queue()

    bootstrap = hivemind.DHT(start=True, host_maddrs=LOCALHOST)
    initial = bootstrap.get_visible_maddrs()
    dhts = [hivemind.DHT(initial_peers=initial, start=True, host_maddrs=LOCALHOST)
            for _ in range(honest + 1)]

    common = dict(
        prefix=run_id,
        target_group_size=group_size,
        min_group_size=2,
        request_timeout=3.0,
        min_matchmaking_time=matchmaking_time,
        start=True,
    )

    peers, attacker = [], None
    if arm == "none":
        # The baseline must match on participant count, not just on behaviour. With group
        # size 2 an odd number of declared peers always orphans one, so a four-peer baseline
        # against five-peer attack arms would attribute a parity effect to the attacker.
        # The extra peer here is an ordinary honest one: same count, one behaviour different.
        peers.append(E01Averager([torch.zeros(4)], dhts[0], response="serve", **common))
    else:
        # fork the attacker while the flag is set, so its child inherits the abstention
        ABSTAIN_THIS_PROCESS = True
        attacker = E01Averager([torch.zeros(4)], dhts[0], response=arm, **common)
        ABSTAIN_THIS_PROCESS = False
    for i in range(honest):
        peers.append(E01Averager([torch.zeros(4)], dhts[i + 1], response="serve", **common))

    attacker_id = str(attacker.peer_id) if attacker is not None else ""
    rows, events = [], []
    try:
        time.sleep(2.0)  # let routing tables settle, in every arm alike
        for rnd in range(rounds):
            # The attacker has to enter matchmaking or it never advertises and is inert: an
            # abstaining peer still calls step(), it just never follows anyone. Start it first
            # so its advertisement is on the key before the honest peers look, and never wait
            # on its control, which only completes if somebody follows it.
            if attacker is not None:
                attacker.step(wait=False, allow_retries=False, timeout=60)
                time.sleep(0.5)
            started = time.perf_counter()
            controls = [p.step(wait=False, allow_retries=False, timeout=60) for p in peers]

            # Poll rather than collecting results in order. Reading control.result()
            # sequentially charges every later peer the wait of an earlier stalled one, so a
            # peer that averaged instantly records the stall's duration. That is
            # time-to-collect, not time-to-average, and it makes the stall arm look uniform.
            done: dict[int, tuple[int, str, float]] = {}
            while len(done) < len(controls) and time.perf_counter() - started < 75:
                for i, control in enumerate(controls):
                    if i in done or not control.done():
                        continue
                    try:
                        control.result()
                        done[i] = (1, "", time.perf_counter() - started)
                    except Exception as exc:
                        done[i] = (0, type(exc).__name__, time.perf_counter() - started)
                time.sleep(0.05)
            for i in range(len(controls)):
                ok, err, el = done.get(i, (0, "NotFinished", time.perf_counter() - started))
                rows.append(dict(
                    repeat=repeat, round=rnd, peer=i, arm=arm, honest=honest,
                    group_size=group_size, averaged=ok,
                    elapsed_s=round(el, 2), error=err,
                ))
            events += drain(EVENTS)
    finally:
        events += drain(EVENTS)
        for p in peers:
            p.shutdown()
        if attacker is not None:
            attacker.shutdown()
        for d in dhts + [bootstrap]:
            d.shutdown()
        EVENTS = None
    return rows, events, attacker_id


def summarise(arm, rows, events):
    ok = sum(r["averaged"] for r in rows)
    el = [r["elapsed_s"] for r in rows]
    kinds = {}
    for e in events:
        kinds[e[0]] = kinds.get(e[0], 0) + 1
    print(f"  {arm:<7} averaged {ok:>3}/{len(rows):<4} elapsed {min(el):.1f}-{max(el):.1f}s   "
          f"joins_recv {kinds.get('join_received', 0):>3}  abstained {kinds.get('abstained', 0):>3}  "
          f"follow_attempts {kinds.get('follow_attempt', 0):>3}  stalls {kinds.get('stall_begin', 0):>3}")
    return kinds


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--honest", type=int, default=4)
    ap.add_argument("--group-size", type=int, default=2)
    ap.add_argument("--rounds", type=int, default=3)
    ap.add_argument("--repeats", type=int, default=2)
    ap.add_argument("--matchmaking-time", type=float, default=5.0)
    ap.add_argument("--arms", nargs="+", default=list(ARMS), choices=list(ARMS))
    args = ap.parse_args()

    print(f"E01 -- {args.honest} honest + 1 attacker, group size {args.group_size}, "
          f"{args.rounds} rounds, arms {args.arms}")
    all_rows, all_events = [], []
    for r in range(args.repeats):
        print(f" repeat {r}")
        for arm in args.arms:
            rows, events, attacker_id = run_arm(arm, args.honest, args.group_size, args.rounds,
                                                args.matchmaking_time, f"e01{r}{arm}", r)
            summarise(arm, rows, events)
            all_rows += rows
            all_events += [dict(repeat=r, arm=arm, kind=e[0], peer=e[1], other=e[2], t=e[3],
                                attacker_id=attacker_id)
                           for e in events]

    RESULTS_DIR.mkdir(exist_ok=True)
    # HANDOFF section 5: every knob that changes the result goes in the name.
    # Fifth time this has bitten in this project: --repeats changes the result and was not in
    # the name, so a three-repeat run silently replaced a two-repeat one. Name every knob.
    tag = (f"n{args.honest}_g{args.group_size}_rounds{args.rounds}_rep{args.repeats}"
           f"_mm{args.matchmaking_time:g}_{'-'.join(args.arms)}")
    for name, data in (("step11_e01_steps", all_rows), ("step11_e01_events", all_events)):
        if not data:
            continue
        path = RESULTS_DIR / f"{name}_{tag}.csv"
        with open(path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(data[0].keys()))
            w.writeheader()
            w.writerows(data)
        print(f"  -> {path.relative_to(Path(__file__).resolve().parent.parent)}")


if __name__ == "__main__":
    # Trap 2: python 3.14 defaults to forkserver, which cannot pickle an averager, and fork is
    # also what carries ABSTAIN_THIS_PROCESS into the child.
    mp.set_start_method("fork", force=True)
    main()
