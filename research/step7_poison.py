"""
Step 7 (brief item 3) -- what does a given beta cost the model?

Items 1-2 established that an attacker can *reach* a given beta cheaply. This asks
what beta costs. Beta is set here by fiat -- by choosing how many group members
poison -- because how it is reached is already answered, and setting it directly
gives a clean sweep with no leadership race to win.

**Poisoning goes on the send path only.** `_generate_input_for_peer`
(`allreduce.py:247`) is what an attacker overrides to corrupt what it *sends*,
leaving its own tensors intact; poisoning local tensors instead would corrupt the
attacker too and confound the damage measurement. Upstream's
`FaultyAllReduceRunner` (`tests/test_allreduce_fault_tolerance.py:87`) overrides
exactly this method.

**Hivemind has no robust aggregation at all** -- a tree-wide search for
`krum|byzantine|median|trimmed|robust` returns nothing, and the only rule is the
weighted mean at `partition.py:242,260`. So `--aggregator median` is a defence this
project *adds*, as a standalone prototype, with hivemind/ untouched. The comparison
is Hivemind's real averaging (`--aggregator mean`) against that addition, on the
same live swarm under the same attacks.

**Compare against this script's own `--attack none` arm, not against the 0.1492
figure in HANDOFF section 2.** That baseline was produced by step2, whose factory
leaves the gradient averager's `target_group_size` unset, so its groups close on
deadline rather than filling. Binding it -- which this script does, so that beta has
a stable denominator -- costs about 4x in convergence on its own: measured here,
8 peers at group size 8 give 0.148 unbound and 0.575 bound, with no attacker in
either. `--no-bind-group-size` reproduces the step2 configuration and does return
0.148, which is what says this subclass is faithful to Hivemind's own averaging.

Coordinate-wise median is the only rule implemented, because it is the only one that
fits reduce-scatter unchanged: computing it per-slice is identical to computing it
over the whole vector, so it needs zero extra communication. Krum does not fit --
it needs pairwise distances between *full* vectors and peer i holds only slice i --
and is reported analytically instead (HANDOFF section 4.11), where its own bound
2f + 2 < n rules it out at these group sizes anyway.

Produces:
    research/results/step7_poison_<attack>_<aggregator>_b<beta>_s<seed>.csv

Run:
    .venv/bin/python research/step7_poison.py --attack none --aggregator mean
    .venv/bin/python research/step7_poison.py --attack inflated_weight --aggregator mean --attackers 1
    .venv/bin/python research/step7_poison.py --attack inflated_weight --aggregator median --attackers 1
"""

from __future__ import annotations

import argparse
import csv
import multiprocessing as mp
import time
from collections import defaultdict
from functools import partial
from pathlib import Path
from typing import AsyncIterator, Sequence

import torch
import torch.nn.functional as F
from torch import nn

from hivemind import DHT, Optimizer
from hivemind.averaging.allreduce import AllReduceRunner, AveragingMode
from hivemind.averaging.partition import TensorPartReducer
from hivemind.optim.grad_averager import GradientAverager
from hivemind.proto import averaging_pb2
from hivemind.utils.asyncio import anext
from hivemind.utils.crypto import RSAPrivateKey

RESULTS_DIR = Path(__file__).resolve().parent / "results"
LOCALHOST = ["/ip4/127.0.0.1/tcp/0"]

DIM = 64
WEIGHTS_SEED = 1234
MODEL_INIT_SEED = 0
TARGET_COORDS = tuple(range(8))  # what `targeted_stealth` aims at


# --------------------------------------------------------------------------
# The task (identical to steps 1-6)
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
        return float(torch.norm(torch.nan_to_num(model.weight.detach().flatten(), nan=float("inf")) - optimum))


def targeted_error(model: nn.Module, optimum: torch.Tensor) -> float:
    """Displacement restricted to the coordinates `targeted_stealth` aims at."""
    index = torch.tensor(TARGET_COORDS, dtype=torch.long)
    with torch.no_grad():
        weights = torch.nan_to_num(model.weight.detach().flatten(), nan=float("inf"))
        return float(torch.norm(weights[index] - optimum[index]))


# --------------------------------------------------------------------------
# The defence: a buffering reducer
# --------------------------------------------------------------------------


class RobustTensorPartReducer(TensorPartReducer):
    """Buffers each sender's contribution instead of summing it, then applies a rule.

    The base class folds every arriving part straight into one accumulator
    (`partition.py:242`) and divides at the barrier (`:260`), so the individual
    contributions are gone by the time a robust rule could look at them. The barrier
    itself is already in the right place: `accumulate_part` receives each sender's
    part tagged with `sender_index`, and `check_current_part_finished` fires exactly
    when every *currently active* sender has contributed.

    `mean` reproduces the base behaviour exactly and is the control. Verified: with
    `--no-bind-group-size` (the step2 configuration) it returns 0.1481 against step2's
    0.1492, so the subclass changes nothing about how Hivemind averages.

    Note `num_current_senders`: a sender banned mid-round (`on_sender_failed`,
    `:248`) reduces it, so the rule must aggregate over however many parts actually
    arrived rather than a hardcoded group size.
    """

    def __init__(self, *args, aggregator: str = "mean", **kwargs):
        self._aggregator = aggregator
        self._parts: list = []
        self._weights: list = []
        super().__init__(*args, **kwargs)

    def reset_accumulators(self):
        self._parts = []
        self._weights = []
        return super().reset_accumulators()

    async def accumulate_part(self, sender_index: int, part_index: int, tensor_part, weight: float = 1.0):
        if self._aggregator == "mean":
            return await super().accumulate_part(sender_index, part_index, tensor_part, weight=weight)

        # Mirror the base class's ordering and failure checks, then buffer rather
        # than accumulate. Keeping this in step with `partition.py:218-246` matters:
        # the waiting loop is what serialises parts, and the banned-sender check is
        # what stops a failed peer's data being counted.
        assert 0 <= sender_index < self.num_senders, "invalid sender index"
        assert 0 <= part_index < self.num_parts, "invalid part index"
        self.num_parts_received[sender_index] += 1

        import asyncio

        from hivemind.averaging.partition import AllreduceException, BannedException

        while part_index > self.current_part_index:
            await asyncio.wait(
                {self.current_part_future, asyncio.create_task(self.finished.wait())},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if self.finished.is_set():
                raise AllreduceException(f"attempted to aggregate part in a finalized {self.__class__.__name__}")

        if self.sender_failed_after[sender_index] != float("inf"):
            raise BannedException(f"sender {sender_index} was banned in background")
        assert part_index == self.current_part_index

        current_part_future = self.current_part_future

        if part_index < self.sender_failed_after[sender_index]:
            self._parts.append(tensor_part.clone())
            self._weights.append(weight)
            self.current_part_accumulated_from += 1
            self.check_current_part_finished()
        return await current_part_future

    def check_current_part_finished(self):
        if self._aggregator == "mean":
            return super().check_current_part_finished()

        assert self.current_part_accumulated_from <= self.num_current_senders
        if self.current_part_accumulated_from == self.num_current_senders:
            stacked = torch.stack(self._parts)
            if self._aggregator == "median":
                # Coordinate-wise median. Per-slice is identical to whole-vector for
                # a coordinate-wise rule, so this needs no extra communication --
                # which is exactly why it fits reduce-scatter and Krum does not.
                # Note it deliberately ignores the sender-supplied weight: that is
                # the field an attacker controls (`allreduce.py:340`), so honouring
                # it would hand back the leverage the rule exists to remove.
                result = stacked.median(dim=0).values
            else:
                raise ValueError(f"unknown aggregator {self._aggregator!r}")
            self.current_part_future.set_result(result)
            self.reset_accumulators()


# --------------------------------------------------------------------------
# The attacker: poison what is sent, nothing else
# --------------------------------------------------------------------------


class PoisoningAllReduceRunner(AllReduceRunner):
    def __init__(self, *args, attack: str = "none", aggregator: str = "mean",
                 declared_weight: float = None, **kwargs):
        self._attack = attack
        self._declared_weight = declared_weight
        super().__init__(*args, **kwargs)
        # Swap in the defended reducer. Done after super().__init__ because that is
        # where the base reducer is built (`allreduce.py:127`).
        if aggregator != "mean":
            self.tensor_part_reducer = RobustTensorPartReducer(
                tuple(part.shape for part in self.parts_for_local_averaging),
                len(self.sender_peer_ids),
                aggregator=aggregator,
            )

    def _poison(self, part: torch.Tensor) -> torch.Tensor:
        if self._attack == "sign_flip":
            return -5.0 * part
        if self._attack == "targeted_stealth":
            # Stay inside the honest norm range so no magnitude filter would fire;
            # bias only a few coordinates. Which coordinates of the *full* vector a
            # slice holds is not known here, so the bias is applied to the leading
            # entries of each slice -- enough to move a fixed coordinate subset.
            poisoned = part.clone()
            width = min(len(TARGET_COORDS), poisoned.numel())
            poisoned[:width] = poisoned[:width] + 0.35 * (1.0 + poisoned[:width].abs())
            return poisoned
        return part  # `none` sends an honest gradient

    async def _generate_input_for_peer(self, peer_index: int) -> AsyncIterator[averaging_pb2.AveragingData]:
        """Poison the *sent* parts only; self.tensor_part_container is left alone.

        Mirrors `allreduce.py:247-257`. The declared `weight` crosses the wire in
        every message and is applied verbatim by the receiver
        (`partition.py:242`: `accumulator.add_(part, alpha=weight)`), so
        `inflated_weight` needs no gradient crafting at all.
        """
        from hivemind.compression import deserialize_torch_tensor, serialize_torch_tensor

        weight = self._declared_weight if self._declared_weight is not None else self.weight

        def corrupt(serialized):
            if self._attack == "none":
                return serialized
            tensor = deserialize_torch_tensor(serialized)
            return serialize_torch_tensor(self._poison(tensor), serialized.compression)

        parts_aiter = self.tensor_part_container.iterate_input_parts_for(peer_index)
        first_part = await anext(parts_aiter)
        yield averaging_pb2.AveragingData(
            code=averaging_pb2.PART_FOR_AVERAGING,
            group_id=self.group_id,
            tensor_part=corrupt(first_part),
            weight=weight,
        )
        async for part in parts_aiter:
            yield averaging_pb2.AveragingData(tensor_part=corrupt(part), weight=weight)


class PoisoningGradientAverager(GradientAverager):
    """Installs the poisoning runner. All peers share one class -- RPC handles are
    named `f"{cls.__name__}.{method}"` (`p2p/servicer.py:148`) and a caller builds
    its stub from its own `type(self)`, so two differently-named subclasses in one
    swarm register handlers nobody calls."""

    def __init__(self, *args, attack: str = "none", aggregator: str = "mean",
                 declared_weight: float = None, peer_index: int = -1, **kwargs):
        self._attack = attack
        self._declared_weight = declared_weight
        self._aggregator = aggregator
        self._peer_index = peer_index
        super().__init__(*args, **kwargs)

    async def _run_allreduce_inplace_(self, tensors, group_info, group_id=None, **kwargs):
        from hivemind.utils.asyncio import as_aiter, azip
        from hivemind.utils.timed_storage import get_dht_time

        group_id = group_info.group_id if group_id is None else group_id
        runner = PoisoningAllReduceRunner(
            p2p=self._p2p,
            servicer_type=type(self),
            prefix=self.prefix,
            tensors=tensors,
            group_id=group_id,
            ordered_peer_ids=group_info.peer_ids,
            attack=self._attack,
            aggregator=self._aggregator,
            declared_weight=self._declared_weight,
            **kwargs,
        )
        assert group_id in self._running_groups
        self._running_groups[group_id].set_result(runner)

        if runner.modes[group_info.peer_ids.index(self.peer_id)] != AveragingMode.AUX:
            async for tensor, update in azip(as_aiter(*tensors), runner):
                tensor.add_(update, alpha=self._averaging_alpha)
                self.last_updated = get_dht_time()
                self._state_updated.set()
        else:
            async for _ in runner:
                raise ValueError("aux peers should not receive averaged tensors")


# --------------------------------------------------------------------------
# One peer
# --------------------------------------------------------------------------


def run_peer(peer_index, initial_peers, metrics, done_count, all_done, args) -> None:
    torch.set_num_threads(1)

    model = make_model()
    optimum = true_weights()
    inputs, targets = make_data(args.samples_per_peer, seed=1000 + peer_index + 100 * args.seed)

    attacking = peer_index < args.attackers and args.attack != "none"
    dht = DHT(initial_peers=initial_peers, host_maddrs=LOCALHOST, start=True)

    optimizer = Optimizer(
        dht=dht,
        run_id=args.run_id,
        params=model.parameters(),
        optimizer=partial(torch.optim.SGD, lr=args.lr),
        target_batch_size=args.target_batch_size,
        batch_size_per_step=args.batch_size,
        matchmaking_time=args.matchmaking_time,
        averaging_timeout=args.averaging_timeout,
        tracker_opts=dict(private_key=RSAPrivateKey()),
        offload_optimizer=False,
        grad_averager_factory=partial(
            PoisoningGradientAverager,
            attack=args.attack if attacking else "none",
            declared_weight=args.declared_weight if attacking else None,
            aggregator=args.aggregator,
            peer_index=peer_index,
            # averager_opts never reaches the gradient averager (optimizer.py:263),
            # so the group size has to be bound here -- but see --no-bind-group-size:
            # binding it changes convergence, which is why the 0.1492 baseline from
            # step2 (which does *not* bind it) is not a valid comparison here.
            **(dict(target_group_size=args.group_size) if args.bind_group_size else {}),
        ),
        verbose=False,
    )

    started = time.perf_counter()
    previous_step = time.perf_counter()
    counted_done = False

    while not all_done.is_set():
        time.sleep(max(0.0, previous_step + args.batch_time - time.perf_counter()))
        previous_step = time.perf_counter()

        metrics.put(
            dict(
                peer=peer_index,
                epoch=optimizer.local_epoch,
                distance_to_optimum=distance_to_optimum(model, optimum),
                targeted_error=targeted_error(model, optimum),
                peers_seen=optimizer.tracker.global_progress.num_peers,
                attacking=int(attacking),
                seconds=round(time.perf_counter() - started, 1),
            )
        )

        if optimizer.local_epoch >= args.max_epoch and not counted_done:
            counted_done = True
            with done_count.get_lock():
                done_count.value += 1
                if done_count.value >= args.num_peers:
                    all_done.set()

        batch = torch.randint(0, len(inputs), (args.batch_size,))
        loss = F.mse_loss(model(inputs[batch]), targets[batch])
        if torch.isfinite(loss):
            loss.backward()
        optimizer.step()
        optimizer.zero_grad()

    for shutdown in (optimizer.shutdown, dht.shutdown):
        try:
            shutdown()
        except Exception as error:
            print(f"peer {peer_index}: {shutdown.__qualname__}: {error!r}")


# --------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--attack", choices=["none", "sign_flip", "targeted_stealth"], default="none",
                        help="what the attacker sends")
    parser.add_argument("--declared-weight", type=float, default=None,
                        help="the weight the attacker claims; unvalidated (partition.py:242). "
                             "Orthogonal to --attack: it multiplies whatever is sent, so on its own it "
                             "only removes the benefit of averaging, and combined with a poison it "
                             "scales that poison.")
    parser.add_argument("--aggregator", choices=["mean", "median"], default="mean")
    parser.add_argument("--attackers", type=int, default=0, help="peers 0..k-1 poison; beta = k / group_size")
    parser.add_argument("--num-peers", type=int, default=8)
    parser.add_argument("--group-size", type=int, default=4)
    parser.add_argument("--max-epoch", type=int, default=20)
    parser.add_argument("--batch-size", type=int, default=4)
    parser.add_argument("--batch-time", type=float, default=0.2)
    parser.add_argument("--target-batch-size", type=int, default=None)
    parser.add_argument("--samples-per-peer", type=int, default=512)
    parser.add_argument("--lr", type=float, default=0.1)
    parser.add_argument("--matchmaking-time", type=float, default=5.0)
    parser.add_argument("--averaging-timeout", type=float, default=30.0)
    parser.add_argument("--no-bind-group-size", dest="bind_group_size", action="store_false",
                        help="leave the gradient averager's target_group_size unset, as step2 does")
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--run-id", type=str, default=None)
    args = parser.parse_args()

    if args.target_batch_size is None:
        args.target_batch_size = 100 * args.num_peers
    # The run_id becomes the averaging group-key prefix, and a prefix may not contain
    # "." -- that is the separator in `prefix.0bBITS` (asserted at averager.py:141).
    # A float weight like 1e6 formats as "1000000.0", which trips it.
    weight_tag = f"{args.declared_weight:.0f}" if args.declared_weight else "1"
    if args.run_id is None:
        args.run_id = f"step7_{args.attack}_w{weight_tag}_{args.aggregator}_k{args.attackers}_s{args.seed}"
    assert "." not in args.run_id, f"run_id may not contain '.': {args.run_id}"
    args.weight_tag = weight_tag

    beta = args.attackers / args.group_size
    print(f"attack={args.attack}  declared_weight={args.declared_weight or 'honest'}  aggregator={args.aggregator}  "
          f"{args.attackers}/{args.group_size} poisoning -> beta={beta:.2f}  seed={args.seed}")
    control = "unbound (step2-comparable)" if not args.bind_group_size else "bound (compare to this script's own --attack none arm)"
    print(f"{args.num_peers} peers, {args.max_epoch} epochs, group size {control}\n")

    mp.set_start_method("fork", force=True)  # 3.14 defaults to forkserver; averagers do not pickle

    bootstrap = DHT(host_maddrs=LOCALHOST, start=True)
    initial_peers = bootstrap.get_visible_maddrs()

    metrics: mp.Queue = mp.Queue()
    done_count = mp.Value("i", 0)
    all_done = mp.Event()

    processes = [
        mp.Process(target=run_peer, args=(index, initial_peers, metrics, done_count, all_done, args),
                   name=f"peer-{index}", daemon=False)
        for index in range(args.num_peers)
    ]

    rows = []

    def drain():
        while not metrics.empty():
            rows.append(metrics.get())

    started = time.perf_counter()
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

    report(rows, args, beta, elapsed=time.perf_counter() - started)


def report(rows, args, beta, elapsed) -> None:
    if not rows:
        print("no results -- peers produced nothing")
        return

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    best = {}
    for row in rows:
        best[(row["peer"], row["epoch"])] = row
    ordered = sorted(best.values(), key=lambda r: (r["epoch"], r["peer"]))

    path = RESULTS_DIR / f"step7_poison_{args.attack}_w{args.weight_tag}_{args.aggregator}_b{beta:.2f}_s{args.seed}.csv"
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(ordered[0]))
        writer.writeheader()
        writer.writerows(ordered)
    print(f"wrote {path}  ({len(ordered)} rows)")

    # Damage is measured on the *honest* peers: an attacker's own model is not what
    # the attack is trying to move, and including it would flatter or distort the
    # number depending on the attack.
    by_epoch = defaultdict(list)
    targeted = defaultdict(list)
    for row in ordered:
        if not row["attacking"]:
            by_epoch[row["epoch"]].append(row["distance_to_optimum"])
            targeted[row["epoch"]].append(row["targeted_error"])

    epochs = sorted(by_epoch)
    print(f"\n{'epoch':>6} {'honest peers':>13} {'mean dist to w*':>17} {'targeted err':>13}")
    for epoch in epochs:
        if epoch % 5 == 0 or epoch == epochs[-1]:
            values = by_epoch[epoch]
            print(f"{epoch:>6} {len(values):>13} {sum(values) / len(values):>17.4f} "
                  f"{sum(targeted[epoch]) / len(targeted[epoch]):>13.4f}")

    final = by_epoch[epochs[-1]]
    final_distance = sum(final) / len(final)
    reference = 0.1492 if not args.bind_group_size else None
    if reference is not None:
        print(f"\nfinal distance (honest peers): {final_distance:.4f}   step2 baseline {reference}   "
              f"ratio {final_distance / reference:.1f}x")
    else:
        print(f"\nfinal distance (honest peers): {final_distance:.4f}")
        print("  (group size is bound, so compare with this script's --attack none arm, not 0.1492)")

    seen = [row["peers_seen"] for row in ordered]
    print(f"peers visible to each other: min {min(seen)}, max {max(seen)}")
    if max(seen) <= 1:
        print("  !! NO AVERAGING HAPPENED -- see HANDOFF section 3 Trap 1")
    print(f"wall clock: {elapsed:.1f}s")


if __name__ == "__main__":
    main()
