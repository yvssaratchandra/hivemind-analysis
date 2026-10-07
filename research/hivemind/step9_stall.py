"""
Step 9 (spike) -- can a leader accept a follower and then simply stall it?

Supervisor's framing: at each protocol step, ask whether an attacker can Deny, Delay
or Tamper. Everything measured so far in this project is *capture* -- who ends up in
the group. This asks the Deny question instead: the attacker gains nothing, the victim
just loses the round.

The attacker wins leadership by declaring an earlier deadline (smaller
min_matchmaking_time), then overrides rpc_join_group to forward ACCEPTED and withhold
everything after it, holding the stream open.

What the code predicts. An accepted follower waits at `matchmaking.py:216`:

    message = await asyncio.wait_for(anext(stream), time_to_expiration + self.request_timeout)

`time_to_expiration` comes from `get_request_expiration_time()` -- the follower's *own*
declared expiration, i.e. its own min_matchmaking_time. So the predicted wait is
follower's min_matchmaking_time + request_timeout, and the attacker cannot extend it.

Note the stream must stay *open*. Returning from the generator closes it, `anext` raises
StopAsyncIteration, and the follower fails fast (`matchmaking.py:241`) -- the opposite of
a stall. Holding it open is the whole attack.

Run:
    .venv/bin/python research/hivemind/step9_stall.py
    .venv/bin/python research/hivemind/step9_stall.py --honest 2
"""

from __future__ import annotations

import argparse
import asyncio
import multiprocessing as mp
import time
from typing import AsyncIterator

import torch

import hivemind
from hivemind.averaging.averager import DecentralizedAverager
from hivemind.p2p import P2PContext
from hivemind.proto import averaging_pb2

LOCALHOST = ["/ip4/127.0.0.1/tcp/0"]

# the attacker reports who it accepted, so we can tell a stall from a rejection
accepted_queue: mp.Queue = None


class StallingAverager(DecentralizedAverager):
    """Accept the join request, then send nothing further while keeping the stream open.

    Every peer must be this same class, honest ones with stall=False. The RPC handler name
    embeds the servicer class name, so a swarm mixing DecentralizedAverager with a subclass
    fails to connect in *both* directions with

        protocols not supported: [<prefix>::DecentralizedAverager.rpc_join_group]

    which looks like a matchmaking failure rather than a naming one. `step6_isolation.py`
    sidesteps this by giving every peer the same subclass; do the same here.
    """

    def __init__(self, *args, stall: bool = False, **kwargs):
        self._stall = stall
        super().__init__(*args, **kwargs)

    async def rpc_join_group(
        self, request: averaging_pb2.JoinRequest, context: P2PContext
    ) -> AsyncIterator[averaging_pb2.MessageFromLeader]:
        # Type annotations are load-bearing here: RPC handlers are discovered by reading
        # them (`p2p/servicer.py:_collect_rpc_handlers`).
        async for response in super().rpc_join_group(request, context):
            yield response
            if self._stall and response.code == averaging_pb2.ACCEPTED:
                accepted_at = time.perf_counter()
                accepted_queue.put(("accepted", str(context.remote_id), 0.0))
                try:
                    # Do not return: returning closes the stream and the follower fails fast.
                    await asyncio.sleep(600)
                except BaseException:
                    # the follower gave up and tore the stream down -- that is the stall
                    accepted_queue.put(("released", str(context.remote_id), time.perf_counter() - accepted_at))
                    raise
                return


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--honest", type=int, default=1)
    parser.add_argument("--no-attack", action="store_true", help="baseline: leader behaves honestly")
    parser.add_argument("--honest-matchmaking-time", type=float, default=20.0)
    parser.add_argument("--attacker-matchmaking-time", type=float, default=6.0)
    parser.add_argument("--request-timeout", type=float, default=3.0)
    parser.add_argument("--run-id", default="stall_spike")
    args = parser.parse_args()
    assert "." not in args.run_id, "hivemind forbids '.' in a group prefix (averager.py:141)"

    predicted = args.honest_matchmaking_time + args.request_timeout

    bootstrap = hivemind.DHT(start=True, host_maddrs=LOCALHOST)
    initial_peers = bootstrap.get_visible_maddrs()
    dhts = [
        hivemind.DHT(initial_peers=initial_peers, start=True, host_maddrs=LOCALHOST)
        for _ in range(args.honest + 1)
    ]

    common = dict(
        prefix=args.run_id,
        target_group_size=args.honest + 1,
        min_group_size=2,
        request_timeout=args.request_timeout,
        start=True,
    )

    attacker = StallingAverager(
        [torch.zeros(4)], dhts[0], stall=not args.no_attack,
        min_matchmaking_time=args.attacker_matchmaking_time, **common
    )
    honest = [
        StallingAverager(
            [torch.zeros(4)], dhts[i + 1], stall=False,
            min_matchmaking_time=args.honest_matchmaking_time, **common
        )
        for i in range(args.honest)
    ]

    print(f"attacker declares deadline at +{args.attacker_matchmaking_time}s (leads)")
    print(f"honest   declare  deadline at +{args.honest_matchmaking_time}s")
    print(f"predicted stall   = {args.honest_matchmaking_time} + {args.request_timeout} = {predicted}s\n")

    attacker.step(wait=False)  # start declaring so the honest peers find a leader
    time.sleep(1)

    started = time.perf_counter()
    controls = [peer.step(wait=False, allow_retries=False, timeout=120) for peer in honest]

    for index, control in enumerate(controls):
        try:
            control.result()
            outcome = "AVERAGED (attack failed)"
        except BaseException as error:
            outcome = f"no group: {type(error).__name__}"
        print(f"honest peer {index}: waited {time.perf_counter() - started:6.2f}s  ->  {outcome}")

    print()
    events = []
    while not accepted_queue.empty():
        events.append(accepted_queue.get())
    for kind, peer, seconds in events:
        if kind == "accepted":
            print(f"leader ACCEPTED {peer[:16]}...")
        else:
            print(f"  -> follower held on for {seconds:.2f}s before giving up (predicted {predicted}s)")
    if not events:
        print("leader accepted nobody -- leadership was not won, this measures the wrong thing")

    for peer in [*honest, attacker, *dhts, bootstrap]:
        peer.shutdown()


if __name__ == "__main__":
    mp.set_start_method("fork", force=True)
    accepted_queue = mp.Queue()
    main()
