"""
Step 2 -- record which peers end up in a group together, with no attacker present.

Step 1 showed the swarm converges. This step answers a different question: when a
peer averages, *who does it average with*? That set is what beta is computed from
later (beta = attacker members / group size), so we need to know what it looks
like under honest conditions first. Without that control, an attacker result has
nothing to be compared against.

Hivemind knows the answer but never exposes it: the group membership arrives as a
`GroupInfo` inside the averager (hivemind/averaging/group_info.py). We reach it
through a supported extension point -- `Optimizer` accepts `grad_averager_factory`
(hivemind/optim/optimizer.py:192) -- and subclass the averager to log the group
before delegating to the real implementation. Nothing in hivemind/ is modified.

This also fixes two defects found in step 1:
  * epoch-skipping: a lagging peer jumps straight to the swarm's epoch, so
    recording only on change silently lost rows. Now every step records and we
    de-duplicate at write time.
  * untidy shutdown: peers used to exit at different times, stranding the
    stragglers with too few partners to form a group. Now they finish together.

What this produces, tagged with the configuration so runs do not overwrite each other:
    research/results/step2_training_<tag>.csv   one row per (peer, epoch)
    research/results/step2_groups_<tag>.csv     one row per (peer, averaging round)

Run:
    .venv/bin/python research/step2_groups.py --num-peers 8  --group-size 8 --max-epoch 20
    .venv/bin/python research/step2_groups.py --num-peers 16 --group-size 8 --max-epoch 20 --run-id step2_16
    .venv/bin/python research/step2_groups.py --num-peers 24 --group-size 8 --max-epoch 20 --run-id step2_24
"""

from __future__ import annotations

import argparse
import csv
import multiprocessing as mp
import time
from collections import defaultdict
from functools import partial
from pathlib import Path

import torch
import torch.nn.functional as F
from torch import nn

from hivemind import DHT, Optimizer
from hivemind.optim.grad_averager import GradientAverager
from hivemind.utils.crypto import RSAPrivateKey

RESULTS_DIR = Path(__file__).resolve().parent / "results"
LOCALHOST = ["/ip4/127.0.0.1/tcp/0"]

DIM = 64
WEIGHTS_SEED = 1234
MODEL_INIT_SEED = 0


# --------------------------------------------------------------------------
# The task (unchanged from step 1)
# --------------------------------------------------------------------------


def true_weights() -> torch.Tensor:
    generator = torch.Generator().manual_seed(WEIGHTS_SEED)
    return torch.randn(DIM, generator=generator)


def make_model() -> nn.Module:
    torch.manual_seed(MODEL_INIT_SEED)
    return nn.Linear(DIM, 1, bias=False)


def make_data(num_samples: int, seed: int, noise_std: float = 0.01):
    generator = torch.Generator().manual_seed(seed)
    inputs = torch.randn(num_samples, DIM, generator=generator)
    targets = inputs @ true_weights() + noise_std * torch.randn(num_samples, generator=generator)
    return inputs, targets.unsqueeze(1)


def distance_to_optimum(model: nn.Module, optimum: torch.Tensor) -> float:
    with torch.no_grad():
        return float(torch.norm(model.weight.detach().flatten() - optimum))


# --------------------------------------------------------------------------
# The observer
# --------------------------------------------------------------------------


class ObservingGradientAverager(GradientAverager):
    """A GradientAverager that reports the group it was placed in, then behaves normally.

    `_aggregate_with_group` is called once per averaging round, after matchmaking
    has settled and before the all-reduce runs, with the full member list. This
    runs inside the averager's own process (DecentralizedAverager is an
    mp.Process), so the observation is sent back over a queue.
    """

    def __init__(self, *args, group_log: mp.Queue = None, peer_index: int = -1, **kwargs):
        self._group_log = group_log
        self._peer_index = peer_index
        self._round = 0
        super().__init__(*args, **kwargs)

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


# --------------------------------------------------------------------------
# One peer
# --------------------------------------------------------------------------


def run_peer(
    peer_index: int,
    initial_peers: list,
    metrics: mp.Queue,
    group_log: mp.Queue,
    done_count: mp.Value,
    all_done: mp.Event,
    args: argparse.Namespace,
) -> None:
    torch.set_num_threads(1)

    model = make_model()
    optimum = true_weights()
    inputs, targets = make_data(args.samples_per_peer, seed=1000 + peer_index)

    dht = DHT(initial_peers=initial_peers, host_maddrs=LOCALHOST, start=True)

    # Tell the parent which PeerID belongs to this peer, so the member lists in
    # the group log can be resolved back to peer numbers.
    metrics.put(dict(kind="identity", peer=peer_index, peer_id=str(dht.peer_id)))

    optimizer = Optimizer(
        dht=dht,
        run_id=args.run_id,
        params=model.parameters(),
        optimizer=partial(torch.optim.SGD, lr=args.lr),
        target_batch_size=args.target_batch_size,
        batch_size_per_step=args.batch_size,
        matchmaking_time=args.matchmaking_time,
        averaging_timeout=args.averaging_timeout,
        # `averager_opts` reaches only the *state* averager
        # (hivemind/optim/optimizer.py:263), despite the docstring at optimizer.py:157
        # claiming it goes to both: _make_gradient_averager (optimizer.py:300-315) never
        # receives it. So target_group_size has to be passed twice, and the copy that
        # matters is the one below -- the gradient averager forms the groups beta is
        # computed from. Left unset it defaults to None (averaging/averager.py:117), and
        # the leader then closes the group when its own deadline passes with whoever
        # turned up (matchmaking.py:290-297) instead of when the group is full
        # (matchmaking.py:276-280), so the group size varies round to round.
        averager_opts=dict(target_group_size=args.group_size),
        tracker_opts=dict(private_key=RSAPrivateKey()),
        offload_optimizer=False,
        # The observer hook. `partial` binds our extra arguments; Optimizer
        # supplies the rest (hivemind/optim/optimizer.py:303). GradientAverager
        # forwards unknown kwargs to DecentralizedAverager, which is how
        # target_group_size gets through.
        grad_averager_factory=partial(
            ObservingGradientAverager,
            group_log=group_log,
            peer_index=peer_index,
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
                # How many peers this one can see. If it stays 0, gradient
                # averaging is silently skipped (hivemind/optim/optimizer.py:526
                # only averages when num_peers > 1) and every peer is training
                # alone -- which still converges, so it is easy to miss.
                peers_seen=optimizer.tracker.global_progress.num_peers,
            )
        )

    # Keep training until every peer has reached max_epoch. A peer that stopped
    # as soon as *it* finished would strand the others with too few partners to
    # assemble a group.
    while not all_done.is_set():
        # Pace the loop. This model is so small that an unpaced step takes
        # microseconds, so a peer reaches target_batch_size on its own long
        # before the DHT can propagate anyone else's progress. The swarm then
        # never synchronises: every peer trains alone and no averaging happens.
        # Real trainers take ~1s per batch; this simulates that. See
        # benchmarks/benchmark_optimizer.py, which paces for the same reason.
        time.sleep(max(0.0, previous_step + args.batch_time - time.perf_counter()))
        previous_step = time.perf_counter()

        # Recording every step (rather than on epoch change) cannot miss an
        # epoch, even when a lagging peer jumps forward to catch up.
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

    # Teardown errors are noise once training is over; log them, do not raise.
    try:
        optimizer.shutdown()
    except Exception as error:
        print(f"peer {peer_index}: optimizer shutdown: {error!r}")
    try:
        dht.shutdown()
    except Exception as error:
        print(f"peer {peer_index}: dht shutdown: {error!r}")


# --------------------------------------------------------------------------
# The swarm
# --------------------------------------------------------------------------


def main() -> None:
    # Hivemind's averagers are mp.Process subclasses (hivemind/averaging/averager.py:50)
    # holding a DHT, asyncio primitives and shared-memory tensors, and they are started
    # with the default multiprocessing context. Python 3.14 changed that default on Linux
    # from "fork" to "forkserver", which pickles the process object instead of inheriting
    # it -- and an averager does not pickle. Without this line every peer dies inside
    # Optimizer.__init__ and the parent then waits forever for peers that are already gone.
    mp.set_start_method("fork", force=True)

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--num-peers", type=int, default=8)
    parser.add_argument("--group-size", type=int, default=8)
    parser.add_argument("--max-epoch", type=int, default=30)
    parser.add_argument("--batch-size", type=int, default=4)
    parser.add_argument(
        "--batch-time",
        type=float,
        default=0.2,
        help="seconds per local step; without this the loop outruns matchmaking and no averaging happens",
    )
    parser.add_argument("--target-batch-size", type=int, default=None, help="defaults to 100 x num_peers")
    parser.add_argument("--samples-per-peer", type=int, default=512)
    parser.add_argument("--lr", type=float, default=0.1)
    parser.add_argument("--matchmaking-time", type=float, default=5.0)
    parser.add_argument("--averaging-timeout", type=float, default=30.0)
    parser.add_argument("--run-id", type=str, default="step2")
    parser.add_argument(
        "--tag",
        type=str,
        default=None,
        help="suffix for the output filenames; defaults to NUMPEERSxGROUPSIZE so configurations "
        "written in sequence do not overwrite each other",
    )
    args = parser.parse_args()

    if args.tag is None:
        args.tag = f"{args.num_peers}x{args.group_size}"

    if args.target_batch_size is None:
        # Sized so each peer takes ~25 steps per epoch. With --batch-time 0.2s
        # that is ~5s of local work per epoch, comparable to matchmaking_time,
        # which gives the swarm time to actually coordinate.
        args.target_batch_size = 100 * args.num_peers

    steps_per_epoch = args.target_batch_size / (args.batch_size * args.num_peers)
    expected_groups = max(1, args.num_peers // args.group_size)
    print(f"launching {args.num_peers} peers, group size {args.group_size}, {args.max_epoch} epochs")
    print(f"expecting ~{expected_groups} group(s) per round")
    print(f"~{steps_per_epoch:.0f} steps/peer/epoch at {args.batch_time}s each = ~{steps_per_epoch * args.batch_time:.1f}s per epoch\n")

    bootstrap = DHT(host_maddrs=LOCALHOST, start=True)
    initial_peers = bootstrap.get_visible_maddrs()

    metrics: mp.Queue = mp.Queue()
    group_log: mp.Queue = mp.Queue()
    done_count = mp.Value("i", 0)
    all_done = mp.Event()

    processes = [
        mp.Process(
            target=run_peer,
            args=(index, initial_peers, metrics, group_log, done_count, all_done, args),
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

    write_results(train_rows, group_rows, identities, args.tag)
    report(train_rows, group_rows, identities, args, elapsed=time.perf_counter() - started)


def write_results(train_rows: list, group_rows: list, identities: dict, tag: str) -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    # Keep the last observation per (peer, epoch) -- recording every step means
    # many rows per epoch, and the last one is the state entering the next epoch.
    best = {}
    for row in train_rows:
        best[(row["peer"], row["epoch"])] = row
    training = sorted(best.values(), key=lambda row: (row["epoch"], row["peer"]))

    path = RESULTS_DIR / f"step2_training_{tag}.csv"
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["peer", "epoch", "distance_to_optimum", "seconds", "peers_seen"])
        writer.writeheader()
        writer.writerows(training)
    print(f"\nwrote {path}  ({len(training)} rows)")

    # Resolve the PeerIDs in each member list back to peer numbers.
    for row in group_rows:
        row["member_peers"] = " ".join(
            str(identities.get(peer_id, "?")) for peer_id in row["members"].split()
        )

    path = RESULTS_DIR / f"step2_groups_{tag}.csv"
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["peer", "round", "group_id", "group_size", "member_peers", "members"])
        writer.writeheader()
        writer.writerows(sorted(group_rows, key=lambda row: (row["round"], row["peer"])))
    print(f"wrote {path}  ({len(group_rows)} rows)")


def report(train_rows: list, group_rows: list, identities: dict, args, elapsed: float) -> None:
    if not train_rows:
        print("no results -- peers produced nothing")
        return

    # --- convergence, as in step 1 -------------------------------------
    best = {}
    for row in train_rows:
        best[(row["peer"], row["epoch"])] = row
    by_epoch = defaultdict(list)
    for (peer, epoch), row in best.items():
        by_epoch[epoch].append(row["distance_to_optimum"])

    epochs = sorted(by_epoch)
    print(f"\n{'epoch':>6} {'peers':>6} {'mean dist to w*':>16} {'spread':>9}")
    for epoch in epochs:
        if epoch % 5 == 0 or epoch == epochs[-1]:
            values = by_epoch[epoch]
            print(f"{epoch:>6} {len(values):>6} {sum(values) / len(values):>16.4f} {max(values) - min(values):>9.4f}")

    incomplete = [e for e in epochs if len(by_epoch[e]) < args.num_peers and e <= args.max_epoch]
    print(f"\nepochs missing peers (up to max_epoch): {incomplete if incomplete else 'none'}")

    # The check that would have caught step 1's mistake.
    peers_seen = [row["peers_seen"] for row in best.values()]
    print(f"peers visible to each other: min {min(peers_seen)}, max {max(peers_seen)}")
    if max(peers_seen) <= 1:
        print("  !! peers never saw each other -- NO AVERAGING HAPPENED, this is local training")

    # --- group composition ---------------------------------------------
    if not group_rows:
        print("no group observations recorded")
        return

    sizes = [row["group_size"] for row in group_rows]
    distinct_groups = {row["group_id"] for row in group_rows}
    print(f"\naveraging rounds observed : {len(group_rows)} (across all peers)")
    print(f"distinct groups           : {len(distinct_groups)}")
    print(f"group size                : min {min(sizes)}, max {max(sizes)}, mean {sum(sizes) / len(sizes):.2f}")

    # The check that would have caught the target_group_size defect. When the swarm has
    # at least group_size peers, every group should be exactly that size. If it is not,
    # target_group_size did not reach the gradient averager, the leader is closing groups
    # on a timer instead, and beta has an unstable denominator. This is invisible when
    # num_peers == group_size, which is why it survived the 8-peer run.
    if args.num_peers >= args.group_size and set(sizes) != {args.group_size}:
        print(
            f"  !! expected every group to hold {args.group_size} peers, saw {sorted(set(sizes))}"
            f" -- either target_group_size is not in force on the gradient averager, or peers"
            f" dropped out of matchmaking"
        )

    # How often did each pair of peers end up together? Under fair sampling this should
    # be roughly flat. A strongly uneven spread, with no attacker present, means stable
    # cliques are forming on their own.
    #
    # Every member of a group reports that group, so the CSV holds one row per (peer,
    # round) and a group of eight appears eight times. Collapsing on (round, group_id)
    # first is what keeps this a count of *rounds* rather than of observations.
    groups = {}
    for row in group_rows:
        members = tuple(sorted(int(m) for m in row["member_peers"].split() if m != "?"))
        groups[(row["round"], row["group_id"])] = members

    together = defaultdict(int)
    for members in groups.values():
        for i, a in enumerate(members):
            for b in members[i + 1 :]:
                together[(a, b)] += 1

    if together and args.num_peers > args.group_size:
        rounds = len({round_index for round_index, _group_id in groups})
        # Include the pairs that never met, or a swarm split into two halves that never
        # mix looks perfectly even.
        counts = sorted(
            together.get((a, b), 0)
            for a in range(args.num_peers)
            for b in range(a + 1, args.num_peers)
        )
        print(f"\npair co-occurrence, in rounds out of {rounds}:")
        print(f"  min {counts[0]}, median {counts[len(counts) // 2]}, max {counts[-1]}")
        print("  (flat = fair sampling; peaked = stable cliques)")
        print("  run research/analyse_groups.py on the CSV to test this against a random-partition null")

    print(f"\nwall clock: {elapsed:.1f}s")


if __name__ == "__main__":
    main()
