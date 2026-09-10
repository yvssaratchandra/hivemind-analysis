"""
Step 1 -- a clean Hivemind swarm with no attacker.

Launches N peers on loopback. Every peer runs a real hivemind.DHT and a real
hivemind.Optimizer, so the full stack is exercised: DHT writes, matchmaking,
group formation, all-reduce.

The model is linear regression written as a torch module. That matters: the
optimal weights `w*` are chosen by us, so at any moment we can measure exactly
how far a peer is from the right answer. Later, when an attacker is added, any
movement away from `w*` is attributable to the attack rather than to noise in a
model we cannot inspect.

What this produces:
    research/results/step1_baseline.csv   one row per (peer, epoch)

Run:
    .venv/bin/python research/step1_baseline.py --num-peers 4
    .venv/bin/python research/step1_baseline.py --num-peers 8
"""

from __future__ import annotations

import argparse
import csv
import multiprocessing as mp
import time
from functools import partial
from pathlib import Path

import torch
import torch.nn.functional as F
from torch import nn

from hivemind import DHT, Optimizer
from hivemind.optim.grad_averager import GradientAverager
from hivemind.utils.crypto import RSAPrivateKey

RESULTS_DIR = Path(__file__).resolve().parent / "results"

# Everything stays on loopback. Nothing in this project touches a real network.
LOCALHOST = ["/ip4/127.0.0.1/tcp/0"]

DIM = 64  # number of model parameters
WEIGHTS_SEED = 1234  # fixes w*, the answer every peer should converge to
MODEL_INIT_SEED = 0  # every peer starts from identical weights


# --------------------------------------------------------------------------
# The task: y = X @ w*, with w* known to us but not to the model
# --------------------------------------------------------------------------


def true_weights() -> torch.Tensor:
    """The optimum. Same on every peer because the seed is fixed."""
    generator = torch.Generator().manual_seed(WEIGHTS_SEED)
    return torch.randn(DIM, generator=generator)


def make_model() -> nn.Module:
    """One linear layer, no bias: the model's weights are directly comparable to w*."""
    torch.manual_seed(MODEL_INIT_SEED)  # all peers must start from the same point
    return nn.Linear(DIM, 1, bias=False)


def make_data(num_samples: int, seed: int, noise_std: float = 0.01):
    """This peer's private training data. A different seed per peer means each
    peer sees different samples -- which is the entire point of training together."""
    generator = torch.Generator().manual_seed(seed)
    inputs = torch.randn(num_samples, DIM, generator=generator)
    targets = inputs @ true_weights() + noise_std * torch.randn(num_samples, generator=generator)
    return inputs, targets.unsqueeze(1)


def distance_to_optimum(model: nn.Module, optimum: torch.Tensor) -> float:
    """How far this peer's model is from the right answer. 0.0 would be perfect."""
    with torch.no_grad():
        return float(torch.norm(model.weight.detach().flatten() - optimum))


# --------------------------------------------------------------------------
# One peer
# --------------------------------------------------------------------------


def run_peer(
    peer_index: int,
    initial_peers: list,
    metrics: mp.Queue,
    args: argparse.Namespace,
) -> None:
    """Runs in its own OS process. One peer = one DHT + one Optimizer + one model."""
    torch.set_num_threads(1)  # many peers share this machine; don't let them fight over cores

    model = make_model()
    optimum = true_weights()
    inputs, targets = make_data(args.samples_per_peer, seed=1000 + peer_index)

    dht = DHT(initial_peers=initial_peers, host_maddrs=LOCALHOST, start=True)

    optimizer = Optimizer(
        dht=dht,
        run_id=args.run_id,
        params=model.parameters(),
        optimizer=partial(torch.optim.SGD, lr=args.lr),
        # An epoch ends when the *whole swarm* has collectively processed this
        # many samples. That is when averaging is triggered.
        target_batch_size=args.target_batch_size,
        batch_size_per_step=args.batch_size,
        # How long a peer waits to gather groupmates. The 15s default is tuned
        # for the open internet and is needlessly slow on loopback.
        matchmaking_time=args.matchmaking_time,
        averaging_timeout=args.averaging_timeout,
        # Fix the group size. Left at None, groups close on a timer instead and
        # their size varies run to run, which would make later measurements noisy.
        #
        # This has to be said twice. `averager_opts` reaches only the *state*
        # averager (hivemind/optim/optimizer.py:263) -- despite the docstring at
        # optimizer.py:157 saying it goes to both -- because _make_gradient_averager
        # (optimizer.py:300-315) never receives it. The gradient averager is the one
        # that forms the groups we care about, and the only supported way to configure
        # it is through the factory.
        averager_opts=dict(target_group_size=args.group_size),
        grad_averager_factory=partial(GradientAverager, target_group_size=args.group_size),
        # Each peer needs its own key: forked children would otherwise share one
        # process-wide key and collide when publishing progress to the DHT.
        tracker_opts=dict(private_key=RSAPrivateKey()),
        # Keep the optimizer on the model's own parameters so `model.weight` is
        # always the live value we measure.
        offload_optimizer=False,
        verbose=(peer_index == 0),
    )

    started = time.perf_counter()
    last_epoch = -1
    previous_step = time.perf_counter()

    while optimizer.local_epoch < args.max_epoch:
        # Pace the loop. This model is tiny, so an unpaced step takes
        # microseconds and a peer reaches target_batch_size on its own long
        # before the DHT can propagate anyone else's progress. Without this the
        # swarm never synchronises: num_peers stays 0, gradient averaging is
        # skipped entirely (hivemind/optim/optimizer.py:526), and every peer
        # trains alone -- which still converges, so it is easy to miss.
        time.sleep(max(0.0, previous_step + args.batch_time - time.perf_counter()))
        previous_step = time.perf_counter()

        # Record once per epoch, i.e. once per completed averaging round.
        if optimizer.local_epoch != last_epoch:
            last_epoch = optimizer.local_epoch
            metrics.put(
                dict(
                    peer=peer_index,
                    epoch=optimizer.local_epoch,
                    distance_to_optimum=distance_to_optimum(model, optimum),
                    seconds=time.perf_counter() - started,
                    peers_seen=optimizer.tracker.global_progress.num_peers,
                )
            )

        batch = torch.randint(0, len(inputs), (args.batch_size,))
        loss = F.mse_loss(model(inputs[batch]), targets[batch])
        loss.backward()

        # opt.step() accumulates this gradient locally and reports the sample
        # count to the swarm. Only when the swarm total crosses
        # target_batch_size does it actually average with a group.
        optimizer.step()
        optimizer.zero_grad()

    metrics.put(
        dict(
            peer=peer_index,
            epoch=optimizer.local_epoch,
            distance_to_optimum=distance_to_optimum(model, optimum),
            seconds=time.perf_counter() - started,
            peers_seen=optimizer.tracker.global_progress.num_peers,
        )
    )

    optimizer.shutdown()
    dht.shutdown()


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
    parser.add_argument("--num-peers", type=int, default=4)
    parser.add_argument("--group-size", type=int, default=None, help="defaults to --num-peers (one group)")
    parser.add_argument("--max-epoch", type=int, default=30)
    parser.add_argument("--batch-size", type=int, default=4, help="samples per local step")
    parser.add_argument("--batch-time", type=float, default=0.2, help="seconds per local step (see run_peer)")
    parser.add_argument("--target-batch-size", type=int, default=None, help="defaults to 100 x num_peers")
    parser.add_argument("--samples-per-peer", type=int, default=512)
    parser.add_argument("--lr", type=float, default=0.1)
    parser.add_argument("--matchmaking-time", type=float, default=5.0)
    parser.add_argument("--averaging-timeout", type=float, default=30.0)
    parser.add_argument("--run-id", type=str, default="step1")
    args = parser.parse_args()

    if args.group_size is None:
        args.group_size = args.num_peers
    if args.target_batch_size is None:
        args.target_batch_size = 100 * args.num_peers

    print(f"launching {args.num_peers} peers, group size {args.group_size}, {args.max_epoch} epochs")
    print(f"an epoch ends after the swarm collectively processes {args.target_batch_size} samples\n")

    # A bootstrap peer that does no training. Everyone else joins through it,
    # which is how a peer with no prior knowledge finds the swarm.
    bootstrap = DHT(host_maddrs=LOCALHOST, start=True)
    initial_peers = bootstrap.get_visible_maddrs()

    metrics: mp.Queue = mp.Queue()
    processes = [
        mp.Process(target=run_peer, args=(index, initial_peers, metrics, args), name=f"peer-{index}", daemon=False)
        for index in range(args.num_peers)
    ]

    rows = []
    started = time.perf_counter()
    try:
        for process in processes:
            process.start()

        # Drain the queue while peers run, so it never fills up and blocks them.
        alive = True
        while alive:
            while not metrics.empty():
                rows.append(metrics.get())
            alive = any(process.is_alive() for process in processes)
            time.sleep(0.5)

        for process in processes:
            process.join(timeout=60)
        while not metrics.empty():
            rows.append(metrics.get())
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
        bootstrap.shutdown()

    write_results(rows)
    report(rows, elapsed=time.perf_counter() - started)


def write_results(rows: list) -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    path = RESULTS_DIR / "step1_baseline.csv"
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["peer", "epoch", "distance_to_optimum", "seconds", "peers_seen"])
        writer.writeheader()
        writer.writerows(sorted(rows, key=lambda row: (row["epoch"], row["peer"])))
    print(f"\nwrote {path}  ({len(rows)} rows)")


def report(rows: list, elapsed: float) -> None:
    """Print the two numbers that matter: did it converge, and how fast."""
    if not rows:
        print("no results -- peers produced nothing")
        return

    by_epoch = {}
    for row in rows:
        by_epoch.setdefault(row["epoch"], []).append(row["distance_to_optimum"])

    print(f"\n{'epoch':>6}  {'mean distance to w*':>20}  {'spread across peers':>20}")
    for epoch in sorted(by_epoch):
        values = by_epoch[epoch]
        mean = sum(values) / len(values)
        spread = max(values) - min(values)
        print(f"{epoch:>6}  {mean:>20.4f}  {spread:>20.4f}")

    seen = [row["peers_seen"] for row in rows]
    print(f"\npeers visible to each other: min {min(seen)}, max {max(seen)}")
    if max(seen) <= 1:
        print("  !! peers never saw each other -- NO AVERAGING HAPPENED, this is local training")

    final = by_epoch[max(by_epoch)]
    print(f"\nfinal mean distance to optimum: {sum(final) / len(final):.4f}")
    print(f"wall clock: {elapsed:.1f}s for {max(by_epoch)} epochs")


if __name__ == "__main__":
    main()
