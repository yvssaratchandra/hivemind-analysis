"""
Step 3, part 2b -- a malicious leader, and beta at the averaging group itself.

`step3_candidates.py` showed an attacker can own the victim's candidate *list*. That is
not the same as owning the group it averages with: a peer picks one leader from the list
and the group is assembled by a separate race (`GROUP_FORMATION.md` §3). This measures
the thing that actually matters -- beta at the assembled group, where the victim's
gradients are really mixed.

The attack has two halves, and only the second needs a modified client.

* **Winning leadership costs nothing.** A peer's declared deadline is
  `min(max(scheduled_time, now + min_matchmaking_time), search_end_time)`
  (`matchmaking.py:527-531`), `min_matchmaking_time` is just `Optimizer`'s ordinary
  `matchmaking_time` argument (`optimizer.py:287,307`), and the earliest declared
  deadline wins the leadership election that every honest peer computes for itself
  (`matchmaking.py:479-497`). So the attacker simply configures a smaller number.

* **Selective admission needs a subclass.** `_check_reasons_to_reject` runs on the
  leader's own machine with the requester's TLS-authenticated PeerID already in hand
  (`matchmaking.py:341-368`, reached via `averager.py:574-578`). Overriding
  `rpc_join_group` to admit the victim and the attacker's own peers, and reject everyone
  else, decides the group's composition outright.

**Why one class rather than an honest and a malicious subclass.** RPC handles are named
`f"{cls.__name__}.{method_name}"` (`hivemind/p2p/servicer.py:148`) and a caller builds
its stub from its *own* `type(self)` (`averager.py:182,545`). Two differently-named
subclasses in one swarm therefore register handlers that nobody calls, and every join
request fails for a reason that looks nothing like the real cause. So all peers run
`CapturingGradientAverager` and behaviour is switched by a constructor argument.

**The honest arm is the control and is not optional.** It is the same swarm with the same
peer counts, with the attackers behaving normally. §6.1 found that honest membership does
not mix reliably, so the honest beta has to be *measured* rather than assumed to be alpha.

Run one configuration at a time:
    .venv/bin/python research/step3_capture.py --arm honest    --attackers 1
    .venv/bin/python research/step3_capture.py --arm malicious --attackers 1
"""

from __future__ import annotations

import argparse
import csv
import multiprocessing as mp
import time
from collections import defaultdict
from functools import partial
from pathlib import Path
from typing import AsyncIterator

import torch
import torch.nn.functional as F

from hivemind import DHT, Optimizer
from hivemind.optim.grad_averager import GradientAverager
from hivemind.p2p import P2PContext
from hivemind.proto import averaging_pb2
from hivemind.utils.crypto import RSAPrivateKey

# The task is imported rather than copied so damage is comparable against the 0.1492
# baseline. step2_groups guards its main() behind __name__, so importing is side-effect
# free; this script lives beside it, so research/ is already on sys.path.
from step2_groups import distance_to_optimum, make_data, make_model, true_weights

RESULTS_DIR = Path(__file__).resolve().parent / "results"
LOCALHOST = ["/ip4/127.0.0.1/tcp/0"]

VICTIM = 0  # peer 0 is the target; peers 1..k are the attacker; the rest are honest


class CapturingGradientAverager(GradientAverager):
    """Records the group it lands in, and -- if given an admission list -- controls it.

    `admit=None` is an ordinary honest averager. Given a set of PeerID strings, this peer
    admits only those and rejects everyone else, which is the whole of Route A's second
    half. The rejection code is GROUP_IS_FULL because it is an outcome a requester
    already expects and moves on from, so the attack does not announce itself.

    Note that rejecting a requester does not consume a slot: `_check_reasons_to_reject`
    runs before the follower is recorded (`matchmaking.py:268-273`), so honest peers
    hammering the leader cannot crowd the victim out.
    """

    def __init__(self, *args, group_log: mp.Queue = None, peer_index: int = -1, admit=None, **kwargs):
        self._group_log = group_log
        self._peer_index = peer_index
        self._admit = admit
        self._round = 0
        super().__init__(*args, **kwargs)

    async def rpc_join_group(
        self, request: averaging_pb2.JoinRequest, context: P2PContext
    ) -> AsyncIterator[averaging_pb2.MessageFromLeader]:
        # The annotations are load-bearing, not decoration: handlers are discovered by
        # reading type hints (hivemind/p2p/servicer.py:_collect_rpc_handlers), and an
        # override without them raises at averager startup.
        if self._admit is not None and str(context.remote_id) not in self._admit:
            yield averaging_pb2.MessageFromLeader(code=averaging_pb2.GROUP_IS_FULL)
            return
        async for response in super().rpc_join_group(request, context):
            yield response

    async def _aggregate_with_group(self, group_info, min_vector_size, **kwargs):
        if self._group_log is not None:
            self._group_log.put(
                dict(
                    peer=self._peer_index,
                    round=self._round,
                    group_id=group_info.group_id.hex()[:16],
                    group_size=group_info.group_size,
                    members=" ".join(str(peer_id) for peer_id in group_info.peer_ids),
                )
            )
            self._round += 1
        return await super()._aggregate_with_group(group_info, min_vector_size, **kwargs)


def run_peer(
    peer_index: int,
    initial_peers: list,
    metrics: mp.Queue,
    group_log: mp.Queue,
    roster: dict,
    barrier,
    done_count: mp.Value,
    all_done: mp.Event,
    args: argparse.Namespace,
) -> None:
    torch.set_num_threads(1)

    model = make_model()
    optimum = true_weights()
    inputs, targets = make_data(args.samples_per_peer, seed=1000 + peer_index)

    dht = DHT(initial_peers=initial_peers, host_maddrs=LOCALHOST, start=True)

    # Two-phase startup. Selective admission runs inside the averager's own process and
    # can only see what was bound into the factory, so every peer has to know the roster
    # before it builds its Optimizer -- and PeerIDs do not exist until the DHT is up.
    roster[peer_index] = str(dht.peer_id)
    metrics.put(dict(kind="identity", peer=peer_index, peer_id=str(dht.peer_id)))
    barrier.wait()

    attacker_indices = set(range(1, args.attackers + 1))
    is_attacker = peer_index in attacker_indices
    attacking = is_attacker and args.arm == "malicious"

    # The attacker admits the victim and its own peers, and nobody else.
    admit = None
    if attacking:
        admit = {roster[i] for i in attacker_indices} | {roster[VICTIM]}

    # An earlier deadline wins leadership. In the honest arm every peer uses the same
    # value, so nobody is favoured.
    matchmaking_time = args.attacker_matchmaking_time if attacking else args.matchmaking_time

    # Every peer, attacker included, uses the run's group size. Giving the attacker a
    # smaller target_group_size looks tempting -- close the group the moment the victim is
    # in -- but it breaks the moment the attacker joins someone else's group instead:
    # update_key_on_group_assembled draws rng.sample(range(target_group_size), group_size)
    # (key_manager.py:100) and raises when the real group is larger than its own target.
    # It is also unnecessary. The attacker's short deadline already closes its group early,
    # with whoever it admitted, which is the same mechanism that won it the leadership
    # (matchmaking.py:290-297). target_group_size still has to be bound into the factory,
    # since averager_opts never reaches the gradient averager (optimizer.py:263).

    optimizer = Optimizer(
        dht=dht,
        run_id=args.run_id,
        params=model.parameters(),
        optimizer=partial(torch.optim.SGD, lr=args.lr),
        target_batch_size=args.target_batch_size,
        batch_size_per_step=args.batch_size,
        matchmaking_time=matchmaking_time,
        averaging_timeout=args.averaging_timeout,
        averager_opts=dict(target_group_size=args.group_size),
        tracker_opts=dict(private_key=RSAPrivateKey()),
        offload_optimizer=False,
        grad_averager_factory=partial(
            CapturingGradientAverager,
            group_log=group_log,
            peer_index=peer_index,
            admit=admit,
            target_group_size=args.group_size,
        ),
        verbose=False,
    )

    started = time.perf_counter()
    counted_done = False
    previous_step = time.perf_counter()

    def record() -> None:
        metrics.put(
            dict(
                kind="train",
                peer=peer_index,
                epoch=optimizer.local_epoch,
                distance_to_optimum=distance_to_optimum(model, optimum),
                seconds=time.perf_counter() - started,
                peers_seen=optimizer.tracker.global_progress.num_peers,
            )
        )

    while not all_done.is_set():
        # Pace the loop, or the swarm never synchronises and no averaging happens at all
        # -- see HANDOFF.md section 3, trap 1.
        time.sleep(max(0.0, previous_step + args.batch_time - time.perf_counter()))
        previous_step = time.perf_counter()
        record()

        if optimizer.local_epoch >= args.max_epoch and not counted_done:
            counted_done = True
            with done_count.get_lock():
                done_count.value += 1
                if done_count.value >= args.num_peers:
                    all_done.set()

        batch = torch.randint(0, len(inputs), (args.batch_size,))
        loss = F.mse_loss(model(inputs[batch]), targets[batch])
        loss.backward()
        optimizer.step()
        optimizer.zero_grad()

    record()

    try:
        optimizer.shutdown()
    except Exception as error:
        print(f"peer {peer_index}: optimizer shutdown: {error!r}")
    try:
        dht.shutdown()
    except Exception as error:
        print(f"peer {peer_index}: dht shutdown: {error!r}")


def main() -> None:
    # Python 3.14 defaults to forkserver, which cannot start a Hivemind averager --
    # see HANDOFF.md section 3, trap 2.
    mp.set_start_method("fork", force=True)

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--arm", choices=["honest", "malicious"], required=True)
    parser.add_argument("--attackers", type=int, default=1, help="peers 1..k, with peer 0 the victim")
    parser.add_argument("--num-peers", type=int, default=16)
    parser.add_argument("--group-size", type=int, default=8)
    parser.add_argument("--max-epoch", type=int, default=20)
    parser.add_argument("--batch-size", type=int, default=4)
    parser.add_argument("--batch-time", type=float, default=0.2)
    parser.add_argument("--target-batch-size", type=int, default=None)
    parser.add_argument("--samples-per-peer", type=int, default=512)
    parser.add_argument("--lr", type=float, default=0.1)
    parser.add_argument("--matchmaking-time", type=float, default=5.0)
    parser.add_argument("--attacker-matchmaking-time", type=float, default=2.0,
                        help="smaller than everyone else's, which is what wins leadership")
    parser.add_argument("--averaging-timeout", type=float, default=30.0)
    parser.add_argument("--run-id", type=str, default=None)
    parser.add_argument("--tag", type=str, default=None)
    args = parser.parse_args()

    if args.target_batch_size is None:
        args.target_batch_size = 100 * args.num_peers
    if args.run_id is None:
        args.run_id = f"step3_{args.arm}_{args.attackers}"
    if args.tag is None:
        args.tag = f"{args.arm}_k{args.attackers}_n{args.num_peers}"

    print(f"{args.arm} arm: {args.num_peers} peers, victim is peer {VICTIM}, "
          f"attackers are peers 1..{args.attackers}, group size {args.group_size}")
    if args.arm == "malicious":
        print(f"attacker declares matchmaking_time {args.attacker_matchmaking_time}s against "
              f"{args.matchmaking_time}s, and admits only itself plus the victim")
    print()

    bootstrap = DHT(host_maddrs=LOCALHOST, start=True)
    initial_peers = bootstrap.get_visible_maddrs()

    manager = mp.Manager()
    roster = manager.dict()
    barrier = manager.Barrier(args.num_peers)
    metrics: mp.Queue = mp.Queue()
    group_log: mp.Queue = mp.Queue()
    done_count = mp.Value("i", 0)
    all_done = mp.Event()

    processes = [
        mp.Process(
            target=run_peer,
            args=(index, initial_peers, metrics, group_log, roster, barrier, done_count, all_done, args),
            name=f"peer-{index}",
            daemon=False,
        )
        for index in range(args.num_peers)
    ]

    train_rows, group_rows, identities = [], [], {}
    started = time.perf_counter()

    def drain() -> None:
        while not metrics.empty():
            row = metrics.get()
            if row.pop("kind") == "identity":
                identities[row["peer_id"]] = row["peer"]
            else:
                train_rows.append(row)
        while not group_log.empty():
            group_rows.append(group_log.get())

    try:
        for process in processes:
            process.start()
        while any(process.is_alive() for process in processes):
            drain()
            # A peer that dies during startup would otherwise leave the parent waiting
            # for an epoch count that can never arrive, which looks like a hang and
            # hides the child's traceback. Stop instead, and say so.
            dead = [p.name for p in processes if p.exitcode not in (None, 0)]
            if dead:
                print(f"\n!! {', '.join(dead)} exited early -- read their traceback above")
                all_done.set()
                break
            time.sleep(0.5)
        for process in processes:
            process.join(timeout=60)
        drain()
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
        try:
            bootstrap.shutdown()
        except Exception:
            pass

    write_results(train_rows, group_rows, identities, args)
    report(train_rows, group_rows, args, elapsed=time.perf_counter() - started)


def write_results(train_rows: list, group_rows: list, identities: dict, args) -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    best = {}
    for row in train_rows:
        best[(row["peer"], row["epoch"])] = row
    training = sorted(best.values(), key=lambda row: (row["epoch"], row["peer"]))

    path = RESULTS_DIR / f"step3_capture_training_{args.tag}.csv"
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["peer", "epoch", "distance_to_optimum", "seconds", "peers_seen"])
        writer.writeheader()
        writer.writerows(training)
    print(f"\nwrote {path}  ({len(training)} rows)")

    attacker_indices = set(range(1, args.attackers + 1))
    for row in group_rows:
        members = [identities.get(peer_id, "?") for peer_id in row["members"].split()]
        row["member_peers"] = " ".join(str(m) for m in members)
        known = [m for m in members if m != "?"]
        row["attackers_in_group"] = sum(m in attacker_indices for m in known)
        row["victim_in_group"] = int(VICTIM in known)
        row["beta"] = row["attackers_in_group"] / row["group_size"] if row["group_size"] else 0.0

    path = RESULTS_DIR / f"step3_capture_groups_{args.tag}.csv"
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["peer", "round", "group_id", "group_size", "member_peers",
                        "attackers_in_group", "victim_in_group", "beta", "members"],
        )
        writer.writeheader()
        writer.writerows(sorted(group_rows, key=lambda row: (row["round"], row["peer"])))
    print(f"wrote {path}  ({len(group_rows)} rows)")


def report(train_rows: list, group_rows: list, args, elapsed: float) -> None:
    if not train_rows:
        print("no results -- peers produced nothing")
        return

    best = {}
    for row in train_rows:
        best[(row["peer"], row["epoch"])] = row
    by_epoch = defaultdict(list)
    for (_peer, epoch), row in best.items():
        by_epoch[epoch].append(row["distance_to_optimum"])
    epochs = sorted(by_epoch)

    print(f"\n{'epoch':>6} {'peers':>6} {'mean dist to w*':>16}")
    for epoch in epochs:
        if epoch % 5 == 0 or epoch == epochs[-1]:
            values = by_epoch[epoch]
            print(f"{epoch:>6} {len(values):>6} {sum(values) / len(values):>16.4f}")

    peers_seen = [row["peers_seen"] for row in best.values()]
    print(f"\npeers visible to each other: min {min(peers_seen)}, max {max(peers_seen)}")
    if max(peers_seen) <= 1:
        print("  !! NO AVERAGING HAPPENED -- this is local training, see HANDOFF.md section 3")

    if not group_rows:
        print("no group observations recorded")
        return

    # Beta as the victim experienced it: only the victim's own observations count, since
    # beta is a property of the group the victim landed in, not of the swarm.
    victim_rounds = [row for row in group_rows if row["peer"] == VICTIM]
    if not victim_rounds:
        print("the victim recorded no averaging rounds at all")
        return

    captured = [row for row in victim_rounds if row["attackers_in_group"] > 0]
    beta_values = [row["beta"] for row in victim_rounds]
    sizes = [row["group_size"] for row in victim_rounds]

    alpha = args.attackers / args.num_peers
    print(f"\n--- beta at the victim, {args.arm} arm, k={args.attackers} ---")
    print(f"victim averaging rounds   : {len(victim_rounds)}")
    print(f"rounds with any attacker  : {len(captured)} ({len(captured) / len(victim_rounds):.0%})")
    print(f"victim's group size       : min {min(sizes)}, max {max(sizes)}")
    print(f"alpha (global fraction)   : {alpha:.3f}")
    print(f"beta  (mean over rounds)  : {sum(beta_values) / len(beta_values):.3f}")
    if captured:
        captured_beta = [row["beta"] for row in captured]
        print(f"beta  (when captured)     : {sum(captured_beta) / len(captured_beta):.3f}")
    print(f"amplification beta/alpha  : {(sum(beta_values) / len(beta_values)) / alpha:.2f}x")

    print(f"\nwall clock: {elapsed:.1f}s")


if __name__ == "__main__":
    main()
