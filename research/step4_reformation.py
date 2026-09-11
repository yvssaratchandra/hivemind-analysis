"""
Step 4 (brief item 2b) -- does periodic group re-formation shed an attacker?

The brief asks how a local malicious fraction "decays under Hivemind's periodic
group re-formation". Answering it needs two things that are not true by default.

**Sharding has to be turned on.** `initial_group_bits` is not an on/off switch:
its *length* declares how many buckets exist (2^b), and the default "" means one
bucket, so there is nothing to rotate between. `hivemind.Optimizer` exposes no
parameter for it and `averager_opts` does not reach the gradient averager, so the
only route is `grad_averager_factory` -- which is what this script uses.

**The re-bucketing rule has to be reproduced.** After each group, every member
recomputes its bucket (`hivemind/averaging/key_manager.py:94-104`):

    rng = random.Random(group_info.group_id)
    index = group_info.peer_ids.index(self.peer_id)
    generalized_index = rng.sample(range(num_buckets), group_info.group_size)[index]

Every input is in the leader's broadcast, which every member receives. So a peer
that shares one group with the victim can compute where the victim goes next.
That is the claim under test.

Three arms, one attacker identity (peer 1), victim is peer 0:

    none      no attacker; what the victim's group looks like normally
    naive     attacker freezes its bucket after the first group and stays put
    tracking  attacker recomputes the victim's next bucket and moves with it

Prediction: `naive` decays as the victim rotates away, `tracking` does not.

Configuration note: `--group-bits` is chosen so its length equals nbits =
log2(num_buckets). The rotation is a fixed-length sliding window, so when those
are equal the previous bits wash out entirely and the next bucket depends *only*
on the broadcast -- the attacker needs no history at all. The script asserts this.

Produces:
    research/results/step4_reformation_<arm>.csv    one row per (peer, round)

Run:
    .venv/bin/python research/step4_reformation.py --arm none
    .venv/bin/python research/step4_reformation.py --arm naive
    .venv/bin/python research/step4_reformation.py --arm tracking
"""

from __future__ import annotations

import argparse
import csv
import multiprocessing as mp
import random
import time
from collections import defaultdict
from functools import partial
from pathlib import Path

import numpy as np
import torch
import torch.nn.functional as F
from torch import nn

from hivemind import DHT, Optimizer
from hivemind.averaging.key_manager import next_power_of_two
from hivemind.optim.grad_averager import GradientAverager
from hivemind.p2p import PeerID
from hivemind.utils.crypto import RSAPrivateKey

RESULTS_DIR = Path(__file__).resolve().parent / "results"
LOCALHOST = ["/ip4/127.0.0.1/tcp/0"]

DIM = 64
WEIGHTS_SEED = 1234
MODEL_INIT_SEED = 0

VICTIM = 0
ATTACKER = 1


# --------------------------------------------------------------------------
# The task (identical to steps 1-2)
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


# --------------------------------------------------------------------------
# Hivemind's re-bucketing rule, reproduced
# --------------------------------------------------------------------------


def predict_next_bits(
    group_id: bytes,
    peer_ids,
    target_peer_id: PeerID,
    target_group_size,
    current_bits: str,
) -> str:
    """Where `target_peer_id` will advertise after this group.

    A line-for-line mirror of `GroupKeyManager.update_key_on_group_assembled`
    (`hivemind/averaging/key_manager.py:94-104`). Everything it reads comes from
    the leader's broadcast, which every group member holds.
    """
    rng = random.Random(group_id)
    peer_ids = list(peer_ids)
    index = peer_ids.index(target_peer_id)
    group_size = len(peer_ids)
    num_buckets = target_group_size if target_group_size is not None else next_power_of_two(group_size)
    generalized_index = rng.sample(range(num_buckets), group_size)[index]
    nbits = int(np.ceil(np.log2(num_buckets)))
    new_bits = bin(generalized_index)[2:].rjust(nbits, "0")
    return (current_bits + new_bits)[-len(current_bits) :] if current_bits else ""


# --------------------------------------------------------------------------
# One averager class for every peer
# --------------------------------------------------------------------------
# All peers must share one class: RPC handles are named f"{cls.__name__}.{method}"
# (`hivemind/p2p/servicer.py:148`) and a caller builds its stub from its own
# type(self), so two differently-named subclasses in one swarm register handlers
# nobody calls. Behaviour is switched by `mode`, not by class.


class ReformationAverager(GradientAverager):
    def __init__(self, *args, log: mp.Queue = None, peer_index: int = -1, mode: str = "honest",
                 victim_peer_id: bytes = None, **kwargs):
        self._log = log
        self._peer_index = peer_index
        self._mode = mode
        self._victim = PeerID(victim_peer_id) if victim_peer_id else None
        self._round = 0
        self._frozen_bits = None
        super().__init__(*args, **kwargs)

    async def _aggregate_with_group(self, group_info, min_vector_size, **kwargs):
        # By the time this runs, matchmaking has already applied the peer's own
        # re-bucketing (`matchmaking.py:387` -> update_key_on_group_assembled),
        # so `group_bits` below is the *post*-update value.
        manager = self._matchmaking.group_key_manager
        bits_after_own_rule = manager.group_bits

        predicted = ""
        victim_present = self._victim is not None and self._victim in group_info.peer_ids

        if self._mode == "naive":
            # Join once, then never move again.
            if self._frozen_bits is None:
                self._frozen_bits = bits_after_own_rule
            manager.group_bits = self._frozen_bits

        elif self._mode == "tracking" and victim_present:
            # Same group as the victim implies the same group key, so the bits
            # this peer held going in were the victim's too. With len(bits) ==
            # nbits that history washes out anyway.
            predicted = predict_next_bits(
                group_info.group_id, group_info.peer_ids, self._victim,
                manager.target_group_size, manager.group_bits,
            )
            manager.group_bits = predicted

        if self._log is not None:
            self._log.put(
                dict(
                    peer=self._peer_index,
                    round=self._round,
                    group_id=group_info.group_id.hex()[:12],
                    group_size=group_info.group_size,
                    members=" ".join(str(peer_id) for peer_id in group_info.peer_ids),
                    # the bucket this peer will advertise in next round
                    bits_next=manager.group_bits,
                    # what its own honest rule would have chosen
                    bits_own_rule=bits_after_own_rule,
                    predicted_victim_bits=predicted,
                    victim_in_group=victim_present,
                )
            )
            self._round += 1

        return await super()._aggregate_with_group(group_info, min_vector_size, **kwargs)


# --------------------------------------------------------------------------
# One peer
# --------------------------------------------------------------------------


def run_peer(peer_index, initial_peers, metrics, log, done_count, all_done, victim_peer_id, args):
    torch.set_num_threads(1)

    model = make_model()
    inputs, targets = make_data(args.samples_per_peer, seed=1000 + peer_index)
    dht = DHT(initial_peers=initial_peers, host_maddrs=LOCALHOST, start=True)
    metrics.put(dict(kind="identity", peer=peer_index, peer_id=str(dht.peer_id)))

    mode = "honest"
    if args.arm != "none" and peer_index == ATTACKER:
        mode = args.arm

    # Spread peers across buckets at startup. Every peer being handed the same
    # `initial_group_bits` puts the whole swarm in one bucket, and the only way
    # out of a bucket is to assemble a group (`key_manager.py:94`), which is
    # rate-limited -- so most peers never escape and are stuck for the whole run.
    # Measured at 16 peers: 8 of 16 never joined a single group at group size 4,
    # 12 of 16 at group size 2.
    #
    # Once balanced the layout sustains itself, because num_buckets equals
    # target_group_size (`key_manager.py:98`) and rng.sample of group_size items
    # from num_buckets is then a permutation -- one member to each bucket. The
    # rotation preserves balance but cannot create it, and
    # update_key_on_not_enough_peers, the hook that would, is a stub.
    num_buckets = args.group_size
    nbits = len(args.group_bits)
    initial_bits = format(peer_index % num_buckets, f"0{nbits}b") if args.stagger else args.group_bits

    optimizer = Optimizer(
        dht=dht,
        run_id=args.run_id,
        params=model.parameters(),
        optimizer=partial(torch.optim.SGD, lr=args.lr),
        target_batch_size=args.target_batch_size,
        batch_size_per_step=args.batch_size,
        matchmaking_time=args.matchmaking_time,
        averaging_timeout=args.averaging_timeout,
        offload_optimizer=False,
        tracker_opts=dict(private_key=RSAPrivateKey()),
        # The only route that reaches the *gradient* averager -- the one whose
        # groups beta is computed from. averager_opts would configure the state
        # averager instead (verified; see HANDOFF section 4).
        grad_averager_factory=partial(
            ReformationAverager,
            log=log,
            peer_index=peer_index,
            mode=mode,
            victim_peer_id=victim_peer_id,
            target_group_size=args.group_size,
            initial_group_bits=initial_bits,
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
                kind="train",
                peer=peer_index,
                epoch=optimizer.local_epoch,
                seconds=time.perf_counter() - started,
                peers_seen=optimizer.tracker.global_progress.num_peers,
            )
        )

        if optimizer.local_epoch >= args.max_epoch and not counted_done:
            counted_done = True
            with done_count.get_lock():
                done_count.value += 1
                if done_count.value >= args.num_peers:
                    all_done.set()

        batch = torch.randint(0, len(inputs), (args.batch_size,))
        F.mse_loss(model(inputs[batch]), targets[batch]).backward()
        optimizer.step()
        optimizer.zero_grad()

    for shutdown in (optimizer.shutdown, dht.shutdown):
        try:
            shutdown()
        except Exception as error:
            print(f"peer {peer_index}: {shutdown.__qualname__}: {error!r}")


# --------------------------------------------------------------------------
# The swarm
# --------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--arm", choices=("none", "naive", "tracking"), default="tracking")
    parser.add_argument("--num-peers", type=int, default=16)
    parser.add_argument("--group-size", type=int, default=4, help="also the bucket count")
    parser.add_argument("--group-bits", type=str, default="00", help="length must equal log2(group-size)")
    parser.add_argument("--no-stagger", dest="stagger", action="store_false",
                        help="give every peer the same starting bucket (reproduces the starvation, see run_peer)")
    parser.add_argument("--max-epoch", type=int, default=20)
    parser.add_argument("--batch-size", type=int, default=4)
    parser.add_argument("--batch-time", type=float, default=0.5)
    parser.add_argument("--target-batch-size", type=int, default=None)
    parser.add_argument("--samples-per-peer", type=int, default=512)
    parser.add_argument("--lr", type=float, default=0.1)
    parser.add_argument("--matchmaking-time", type=float, default=15.0,
                        help="15s not 5s: with sharding a bucket's candidate pool is small and "
                             "5s closes groups early, which starves peers (see module docstring)")
    parser.add_argument("--averaging-timeout", type=float, default=30.0)
    parser.add_argument("--run-id", type=str, default=None)
    args = parser.parse_args()

    if args.target_batch_size is None:
        args.target_batch_size = 100 * args.num_peers
    if args.run_id is None:
        args.run_id = f"step4_{args.arm}"

    nbits = int(np.ceil(np.log2(args.group_size)))
    assert len(args.group_bits) == nbits, (
        f"--group-bits must be {nbits} chars for group size {args.group_size}, so that the sliding "
        f"window in key_manager.py:104 leaves the next bucket a function of the broadcast alone"
    )

    print(f"arm={args.arm}  {args.num_peers} peers  group size {args.group_size}  "
          f"{2 ** nbits} buckets ('{args.group_bits}')  {args.max_epoch} epochs")
    print(f"victim = peer {VICTIM}" + (f", attacker = peer {ATTACKER}" if args.arm != "none" else "") + "\n")

    mp.set_start_method("fork", force=True)  # Python 3.14 defaults to forkserver; averagers do not pickle

    bootstrap = DHT(host_maddrs=LOCALHOST, start=True)
    initial_peers = bootstrap.get_visible_maddrs()

    metrics: mp.Queue = mp.Queue()
    log: mp.Queue = mp.Queue()
    done_count = mp.Value("i", 0)
    all_done = mp.Event()

    identities, train_rows, log_rows = {}, [], []

    def drain():
        while not metrics.empty():
            row = metrics.get()
            if row.pop("kind") == "identity":
                identities[row["peer_id"]] = row["peer"]
            else:
                train_rows.append(row)
        while not log.empty():
            log_rows.append(log.get())

    # The victim starts first so the attacker can be told which PeerID to follow.
    processes = []
    victim = mp.Process(
        target=run_peer,
        args=(VICTIM, initial_peers, metrics, log, done_count, all_done, None, args),
        name=f"peer-{VICTIM}", daemon=False,
    )
    victim.start()
    processes.append(victim)

    victim_peer_id = None
    while victim_peer_id is None:
        drain()
        for peer_id, index in identities.items():
            if index == VICTIM:
                victim_peer_id = PeerID.from_base58(peer_id).to_bytes()
        time.sleep(0.2)
    print(f"victim PeerID resolved; launching {args.num_peers - 1} more peers\n")

    for index in range(1, args.num_peers):
        process = mp.Process(
            target=run_peer,
            args=(index, initial_peers, metrics, log, done_count, all_done, victim_peer_id, args),
            name=f"peer-{index}", daemon=False,
        )
        process.start()
        processes.append(process)

    started = time.perf_counter()
    try:
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

    write_results(log_rows, train_rows, identities, args)
    report(log_rows, identities, args, elapsed=time.perf_counter() - started)


def write_results(log_rows, train_rows, identities, args) -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    for row in log_rows:
        row["member_peers"] = " ".join(str(identities.get(p, "?")) for p in row["members"].split())

    path = RESULTS_DIR / f"step4_reformation_{args.arm}.csv"
    fields = ["peer", "round", "group_id", "group_size", "member_peers", "bits_next",
              "bits_own_rule", "predicted_victim_bits", "victim_in_group", "members"]
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(sorted(log_rows, key=lambda r: (r["round"], r["peer"])))
    print(f"\nwrote {path}  ({len(log_rows)} rows)")

    seen = [row["peers_seen"] for row in train_rows] or [0]
    print(f"peers visible to each other: min {min(seen)}, max {max(seen)}")
    if max(seen) <= 1:
        print("  !! NO AVERAGING HAPPENED -- see HANDOFF section 3")


def report(log_rows, identities, args, elapsed: float) -> None:
    if not log_rows:
        print("no group observations recorded")
        return

    # Did sharding actually switch on? If every peer sits in one bucket it did not.
    buckets = {row["bits_next"] for row in log_rows}
    print(f"\nbuckets actually used: {sorted(buckets)}")
    if len(buckets) <= 1:
        print("  !! only one bucket -- sharding did not take effect, results are meaningless")

    # Starvation check. group_bits only ever changes inside
    # update_key_on_group_assembled (`key_manager.py:94`), so a peer that cannot
    # assemble a group cannot change bucket either -- it is stuck where it is,
    # permanently. The hook meant to rescue it, update_key_on_not_enough_peers
    # (`key_manager.py:107`), is an unimplemented stub. Sharding therefore needs
    # enough peers per bucket that this never happens: num_buckets equals
    # target_group_size (`key_manager.py:98`), so aim for num_peers >> group_size^2.
    joined = defaultdict(int)
    for row in log_rows:
        joined[row["peer"]] += 1
    starved = [peer for peer in range(args.num_peers) if joined[peer] == 0]
    thin = [peer for peer in range(args.num_peers) if 0 < joined[peer] <= 2]
    print(f"\ngroups joined per peer: min {min(joined.values(), default=0)}, "
          f"max {max(joined.values(), default=0)}")
    if starved:
        print(f"  !! {len(starved)} peer(s) never joined any group: {starved}")
        print("     stuck in an under-occupied bucket with no way out -- raise num_peers or lower group_size")
    elif thin:
        print(f"  ! {len(thin)} peer(s) joined <=2 groups: {thin}")

    by_peer_round = {(row["peer"], row["round"]): row for row in log_rows}
    victim_rounds = sorted(r for (p, r) in by_peer_round if p == VICTIM)

    print(f"\n{'round':>5} {'victim bucket':>14} {'victim group':>22} {'beta':>6}")
    betas = []
    for rnd in victim_rounds:
        row = by_peer_round[(VICTIM, rnd)]
        members = [m for m in row["member_peers"].split() if m != "?"]
        attackers = sum(1 for m in members if int(m) == ATTACKER) if args.arm != "none" else 0
        beta = attackers / len(members) if members else 0.0
        betas.append(beta)
        print(f"{rnd:>5} {row['bits_next']:>14} {' '.join(members):>22} {beta:>6.3f}")

    if betas:
        early = betas[: max(1, len(betas) // 3)]
        late = betas[-max(1, len(betas) // 3) :]
        print(f"\nmean beta, first third : {sum(early) / len(early):.3f}")
        print(f"mean beta, last third  : {sum(late) / len(late):.3f}")
        print(f"mean beta, all rounds  : {sum(betas) / len(betas):.3f}")
        print(f"rounds attacker shared the victim's group: "
              f"{sum(1 for b in betas if b > 0)}/{len(betas)}")

    # Prediction accuracy, independent of whether matchmaking then cooperated.
    #
    # Join on group_id, not on round. `round` is a per-peer counter incremented
    # once per averaging round that peer takes part in, so two peers that were in
    # different groups for a round have permanently desynced counters. group_id
    # identifies one specific assembly, which is what has to line up: the victim's
    # `bits_next` recorded against that group_id *is* the value the attacker was
    # trying to predict from that same broadcast.
    if args.arm == "tracking":
        victim_bits_by_group = {
            row["group_id"]: row["bits_next"] for row in log_rows if row["peer"] == VICTIM
        }
        hits, misses = 0, []
        for row in log_rows:
            if row["peer"] != ATTACKER or not row["predicted_victim_bits"]:
                continue
            actual = victim_bits_by_group.get(row["group_id"])
            if actual is None:
                continue  # victim's own row for this group was not recorded
            if row["predicted_victim_bits"] == actual:
                hits += 1
            else:
                misses.append((row["group_id"], row["predicted_victim_bits"], actual))
        total = hits + len(misses)
        if total:
            print(f"\nvictim's next bucket predicted correctly: {hits}/{total}")
            for group_id, predicted, actual in misses[:5]:
                print(f"    miss: group {group_id} predicted {predicted!r}, actual {actual!r}")
            print("(prediction only; whether the attacker then wins a group with the victim is separate)")

    print(f"\nwall clock: {elapsed:.1f}s")


if __name__ == "__main__":
    main()
