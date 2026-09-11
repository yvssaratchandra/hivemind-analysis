"""
Step 6 -- does owning the victim's candidate list actually win the victim's group?

`step5_selective.py` showed an attacker owning a key's replicas can show the victim a
candidate list containing only itself, quietly. But a candidate list is not a group
(`GROUP_FORMATION.md` section 3), and filtering the victim's *reads* leaves one path
uncontrolled: if the victim's own declared deadline is earlier than anything it is
shown, `pop_next_leader` (`matchmaking.py:478-481`) makes it **stop reading and wait to
be contacted**. Honest peers -- whose reads are not filtered -- still see the victim
and can join it, and the attack silently fails that round.

The comparison that decides this is a tuple:

    follow iff (candidate_expiration, candidate_peer_id) <= (own_declared_expiration, own_peer_id)

so the fix is to make sure what the victim is shown always compares lower. Four arms,
each adding one mechanism to the last:

    honest      no attacker. How often does the victim lead on its own?
    selective   admission control + filter the victim's reads (step5 + step3_capture)
    isolated    also hide the victim from everyone else's reads, so that if it does
                lead, no honest peer knows to join it
    forged      also show the victim a *fabricated* early expiration

`forged` is the interesting one. HANDOFF section 6.4 found declaring early and having
time to assemble are in tension: a 3.0s declare time won the election but collapsed
capture to 5%, because the attacker's own group closes before the victim arrives.
That tension only exists if the attacker must *really* declare early. It need not:
`get_averagers` reads each candidate's expiration straight out of the record
(`key_manager.py:88`), and the filter already rebuilds that dictionary, so the victim
can be shown an early deadline while the attacker's real declaration stays late.
The mismatch would normally be caught leader-side (`matchmaking.py:358`) but the
attacker already overrides `_check_reasons_to_reject`.

Measured per round: whether the victim led, its full group membership, beta, and
whether any honest peer joined it.

Run:
    .venv/bin/python research/step6_isolation.py --arm honest
    .venv/bin/python research/step6_isolation.py --arm selective
    .venv/bin/python research/step6_isolation.py --arm isolated
    .venv/bin/python research/step6_isolation.py --arm forged
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
from torch import nn

from hivemind import DHT, Optimizer
from hivemind.dht.protocol import DHTProtocol
from hivemind.dht.routing import DHTID
from hivemind.dht.storage import DictionaryDHTValue
from hivemind.optim.grad_averager import GradientAverager
from hivemind.p2p import P2PContext
from hivemind.proto import averaging_pb2, dht_pb2
from hivemind.utils.crypto import RSAPrivateKey
from hivemind.utils.timed_storage import get_dht_time

RESULTS_DIR = Path(__file__).resolve().parent / "results"
LOCALHOST = ["/ip4/127.0.0.1/tcp/0"]

DIM = 64
WEIGHTS_SEED = 1234
MODEL_INIT_SEED = 0
VICTIM = 0


# --------------------------------------------------------------------------
# The task (identical to steps 1-5)
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
# The DHT-level filter
# --------------------------------------------------------------------------
# This runs inside the *DHT's* process, which `DHT` forks from the peer's process
# (`hivemind/dht/dht.py:22`). Two consequences shape the wiring:
#
#   * the class must be patched at import, before any fork -- handlers bind at
#     registration (`getattr(servicer, name)`, `p2p/servicer.py:117`);
#   * the configuration is not known at fork time, because it names the victim's
#     PeerID and PeerIDs do not exist until DHTs are up. So it lives in a
#     multiprocessing Manager dict: the proxy is inherited at fork, and reads
#     later see values written after it.
#
# Honest peers leave _FILTER as None and fall straight through to the original.

_FILTER = None
_ORIGINAL_RPC_FIND = DHTProtocol.rpc_find


async def _filtering_rpc_find(self, request: dht_pb2.FindRequest, context) -> dht_pb2.FindResponse:
    response = await _ORIGINAL_RPC_FIND(self, request, context)

    config = _FILTER
    if config is None or not config.get("ready"):
        return response

    victim = config["victim"]
    requester = str(context.remote_id)
    asking_is_victim = requester == victim

    # The attacker's own peers always get the truth. In the `chase` arm they must be
    # able to see the victim in order to join it, and hiding it from them would make
    # the attack hide the target from itself.
    if requester in set(config.get("attacker_peers", [])):
        return response

    # Everyone except the victim is served honestly, unless we are also hiding the
    # victim from them (the `isolated` and `chase` arms).
    if not asking_is_victim and not config.get("hide_victim_from_others"):
        return response

    for index, key_id in enumerate(map(DHTID.from_bytes, request.keys)):
        if key_id.to_bytes() != config["key_id"]:
            continue  # every other key in the DHT is answered truthfully
        item = response.results[index]
        if item.type != dht_pb2.FOUND_DICTIONARY:
            continue

        original = self.serializer.loads(item.value)
        filtered = DictionaryDHTValue(maxsize=original.maxsize)
        allowed = set(config["allowed"])
        victim_subkey = config["victim_subkey"]
        forged = config.get("forged_expiration")

        # `chase`: show the victim *nothing*. With an empty candidate list
        # `pop_next_leader` has nothing to compare against, so the victim stops
        # searching and waits to be contacted -- it leads every round, by
        # construction. That converts the leadership race from something the
        # attacker must win into something it decides, and the attacker then
        # simply joins the victim instead of the other way round.
        if asking_is_victim and config.get("blind_victim"):
            item.value = self.serializer.dumps(DictionaryDHTValue(maxsize=original.maxsize))
            config["lies"] = config.get("lies", 0) + 1
            continue

        for subkey, entry in original.items():
            if asking_is_victim:
                if subkey not in allowed:
                    continue  # the victim sees only the attacker
                # Show a deadline that compares lower than anything the victim can
                # declare, so `pop_next_leader` never lets it wait to be contacted.
                expiration = get_dht_time() + forged if forged is not None else entry.expiration_time
                filtered.store(subkey, entry.value, expiration)
            else:
                if subkey == victim_subkey:
                    continue  # hide the victim, so nobody joins it if it does lead
                filtered.store(subkey, entry.value, entry.expiration_time)

        item.value = self.serializer.dumps(filtered)
        config["lies"] = config.get("lies", 0) + 1

    return response


DHTProtocol.rpc_find = _filtering_rpc_find


# --------------------------------------------------------------------------
# The averager: admission control, plus recording who led
# --------------------------------------------------------------------------


class IsolatingGradientAverager(GradientAverager):
    """Admission control as in step3_capture, plus a record of whether this peer led.

    `current_followers` is drained once a group completes (`matchmaking.py:161-167`),
    so it cannot be inspected after the fact. Counting accepted join requests as they
    arrive is reliable: a peer that accepted at least one was the leader.
    """

    def __init__(
        self,
        *args,
        group_log: mp.Queue = None,
        peer_index: int = -1,
        admit=None,
        victim_id: str = None,
        force_early_schedule: bool = False,
        override_min_matchmaking_time: float = None,
        **kwargs,
    ):
        if override_min_matchmaking_time is not None:
            kwargs["min_matchmaking_time"] = override_min_matchmaking_time
        self._group_log = group_log
        self._peer_index = peer_index
        self._admit = admit
        self._victim_id = victim_id
        self._force_early_schedule = force_early_schedule
        self._round = 0
        self._accepted_this_round = 0
        super().__init__(*args, **kwargs)

    def schedule_step(self, scheduled_time=None, **kwargs):
        # See step3_capture: Optimizer ties the pre-scheduling gate and the declared
        # deadline to the same number, so they cancel and every peer declares the
        # shared epoch-end estimate. Forcing scheduled_time into the present lets the
        # attacker's own min_matchmaking_time decide the deadline instead.
        if self._force_early_schedule:
            scheduled_time = get_dht_time()
        return super().schedule_step(scheduled_time=scheduled_time, **kwargs)

    async def rpc_join_group(
        self, request: averaging_pb2.JoinRequest, context: P2PContext
    ) -> AsyncIterator[averaging_pb2.MessageFromLeader]:
        # Type annotations are load-bearing: handlers are discovered by reading them
        # (`p2p/servicer.py:_collect_rpc_handlers`).
        if self._admit is not None:
            requester = str(context.remote_id)
            if requester not in self._admit:
                yield averaging_pb2.MessageFromLeader(code=averaging_pb2.GROUP_IS_FULL)
                return
            # Hold the group open for the victim: admitting our own peers first lets
            # the leader hit min_group_size=2 on Sybils alone and assemble before the
            # victim arrives.
            if requester != self._victim_id and not self._victim_is_present():
                yield averaging_pb2.MessageFromLeader(code=averaging_pb2.GROUP_IS_FULL)
                return
        accepted = False
        async for response in super().rpc_join_group(request, context):
            if response.code == averaging_pb2.ACCEPTED:
                accepted = True
            yield response
        if accepted:
            self._accepted_this_round += 1

    def _victim_is_present(self) -> bool:
        followers = getattr(self._matchmaking, "current_followers", {})
        return any(str(peer_id) == self._victim_id for peer_id in followers)

    async def _aggregate_with_group(self, group_info, min_vector_size, **kwargs):
        if self._group_log is not None:
            self._group_log.put(
                dict(
                    peer=self._peer_index,
                    round=self._round,
                    group_id=group_info.group_id.hex()[:16],
                    group_size=group_info.group_size,
                    members=" ".join(str(peer_id) for peer_id in group_info.peer_ids),
                    led=int(self._accepted_this_round > 0),
                )
            )
            self._round += 1
        self._accepted_this_round = 0
        return await super()._aggregate_with_group(group_info, min_vector_size, **kwargs)


# --------------------------------------------------------------------------
# One peer
# --------------------------------------------------------------------------


def run_peer(peer_index, initial_peers, metrics, group_log, roster, filter_config, barrier,
             done_count, all_done, args) -> None:
    global _FILTER
    torch.set_num_threads(1)

    model = make_model()
    optimum = true_weights()
    inputs, targets = make_data(args.samples_per_peer, seed=1000 + peer_index)

    attacker_indices = set(range(1, args.attackers + 1))
    attacking = peer_index in attacker_indices and args.arm != "honest"

    # Bind the shared config *before* creating the DHT, so the forked DHT process
    # inherits the proxy. It is still empty at this point; `ready` is set after the
    # barrier, once PeerIDs exist.
    if attacking:
        _FILTER = filter_config

    # Caching is a first-class variable here, not a detail. `rpc_find` also answers
    # from `self.cache` (`protocol.py:344`) and `add_candidate` *merges* every
    # responder's dictionary (`node.py:834-837`), so any honest peer that has cached
    # the key becomes an unwitting responder that undoes the attacker's filtering.
    # With cache_nearest=1 (`node.py:112`) every successful lookup plants such a copy.
    cache_opts = dict(cache_locally=False, cache_nearest=0, cache_on_store=False, cache_size=0) if args.no_cache else {}
    dht = DHT(initial_peers=initial_peers, host_maddrs=LOCALHOST, start=True, **cache_opts)

    roster[peer_index] = str(dht.peer_id)
    metrics.put(dict(kind="identity", peer=peer_index, peer_id=str(dht.peer_id)))
    barrier.wait()

    if attacking and peer_index == min(attacker_indices):
        group_key = f"{args.run_id}_grad_averager.0b"
        filter_config["victim"] = roster[VICTIM]
        filter_config["victim_subkey"] = _peer_id_bytes(roster[VICTIM])
        filter_config["key_id"] = DHTID.generate(source=group_key).to_bytes()
        filter_config["allowed"] = [_peer_id_bytes(roster[i]) for i in attacker_indices]
        filter_config["hide_victim_from_others"] = args.arm in ("isolated", "chase")
        filter_config["blind_victim"] = args.arm == "chase"
        filter_config["attacker_peers"] = [roster[i] for i in attacker_indices]
        filter_config["forged_expiration"] = None
        filter_config["ready"] = True
    barrier.wait()

    admit = None
    if attacking:
        admit = {roster[i] for i in attacker_indices} | {roster[VICTIM]}

    matchmaking_time = args.attacker_matchmaking_time if attacking else args.matchmaking_time

    optimizer = Optimizer(
        dht=dht,
        run_id=args.run_id,
        params=model.parameters(),
        optimizer=partial(torch.optim.SGD, lr=args.lr),
        target_batch_size=args.target_batch_size,
        batch_size_per_step=args.batch_size,
        matchmaking_time=matchmaking_time,
        averaging_timeout=args.averaging_timeout,
        tracker_opts=dict(private_key=RSAPrivateKey()),
        offload_optimizer=False,
        grad_averager_factory=partial(
            IsolatingGradientAverager,
            group_log=group_log,
            peer_index=peer_index,
            admit=admit,
            victim_id=roster[VICTIM],
            # In `chase` the attacker wants to be a follower, not a leader: it declares
            # a late deadline so `pop_next_leader` ranks the victim above it and it
            # requests to join. In the other arms it declares early to win the election.
            force_early_schedule=attacking and args.arm != "chase",
            override_min_matchmaking_time=(
                args.chase_declare_time if attacking and args.arm == "chase"
                else args.attacker_declare_time if attacking else None
            ),
            # averager_opts never reaches the gradient averager (optimizer.py:263),
            # so the group size has to be bound here.
            target_group_size=args.group_size,
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
                distance_to_optimum=distance_to_optimum(model, optimum),
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


def _peer_id_bytes(peer_id_str: str) -> bytes:
    from hivemind.p2p import PeerID

    return PeerID.from_base58(peer_id_str).to_bytes()


def crafted_node_ids(target_key: str, count: int) -> list:
    """node_ids at XOR distance 0, 1, ... from the key's address (step3_eclipse.py)."""
    target = DHTID.generate(source=target_key)
    return [DHTID(int(target) ^ i) for i in range(count)]


def run_sybil_dht(index, initial_peers, filter_config, stop_event, args) -> None:
    """A DHT-only Sybil: holds a replica of the averaging key and filters reads of it.

    Without these the filter is nearly inert. `rpc_find` only filters responses *this
    node serves*, and custody of a key belongs to the peers whose node_ids are nearest
    it (step3_eclipse.py: five crafted identities own every replica). An attacker with
    a random node_id is rarely the one asked, so it rarely gets to lie -- measured at
    55 filtered responses across a whole run, versus thousands once custody is held.

    These nodes do no training, which is the point: they are the cheap half of the
    attack. `alpha` counts *training* peers, because only those can submit gradients;
    holding DHT records costs a process and no compute.
    """
    global _FILTER
    _FILTER = filter_config  # bound before the fork, so the DHT child inherits it

    group_key = f"{args.run_id}_grad_averager.0b"
    node_id = crafted_node_ids(group_key, args.sybil_nodes)[index]

    cache_opts = dict(cache_locally=False, cache_nearest=0, cache_on_store=False, cache_size=0) if args.no_cache else {}
    dht = DHT(initial_peers=initial_peers, host_maddrs=LOCALHOST, node_id=node_id, start=True, **cache_opts)
    try:
        stop_event.wait()
    finally:
        try:
            dht.shutdown()
        except Exception:
            pass


# --------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--arm", choices=["honest", "selective", "isolated", "chase"], required=True)
    parser.add_argument("--attackers", type=int, default=1, help="peers 1..k; peer 0 is the victim")
    parser.add_argument("--num-peers", type=int, default=16)
    parser.add_argument("--group-size", type=int, default=8)
    parser.add_argument("--max-epoch", type=int, default=15)
    parser.add_argument("--batch-size", type=int, default=4)
    parser.add_argument("--batch-time", type=float, default=0.2)
    parser.add_argument("--target-batch-size", type=int, default=None)
    parser.add_argument("--samples-per-peer", type=int, default=512)
    parser.add_argument("--lr", type=float, default=0.1)
    parser.add_argument("--matchmaking-time", type=float, default=5.0)
    parser.add_argument("--attacker-matchmaking-time", type=float, default=15.0,
                        help="large: start advertising early (see HANDOFF 6.4)")
    parser.add_argument("--attacker-declare-time", type=float, default=1.5,
                        help="small: short rolling deadline, the value that wins the election")
    parser.add_argument("--chase-declare-time", type=float, default=25.0,
                        help="chase arm: a late deadline, so the attacker follows the victim rather than leading")
    parser.add_argument("--no-cache", action="store_true",
                        help="disable DHT caching swarm-wide; isolates whether caches defeat read-filtering")
    parser.add_argument("--sybil-nodes", type=int, default=5,
                        help="DHT-only Sybils holding the key's replicas; 5 = total custody (step3_eclipse)")
    parser.add_argument("--averaging-timeout", type=float, default=30.0)
    parser.add_argument("--run-id", type=str, default=None)
    args = parser.parse_args()

    if args.target_batch_size is None:
        args.target_batch_size = 100 * args.num_peers
    if args.run_id is None:
        args.run_id = f"step6_{args.arm}"

    print(f"arm={args.arm}  {args.num_peers} peers  group size {args.group_size}  "
          f"{args.attackers} training attacker(s) + "
          f"{args.sybil_nodes if args.arm != 'honest' else 0} DHT-only sybil(s)  victim = peer {VICTIM}\n")

    mp.set_start_method("fork", force=True)  # 3.14 defaults to forkserver; averagers do not pickle

    bootstrap = DHT(host_maddrs=LOCALHOST, start=True)
    initial_peers = bootstrap.get_visible_maddrs()

    manager = mp.Manager()
    roster = manager.dict()
    filter_config = manager.dict()
    barrier = manager.Barrier(args.num_peers)
    metrics: mp.Queue = mp.Queue()
    group_log: mp.Queue = mp.Queue()
    done_count = mp.Value("i", 0)
    all_done = mp.Event()

    processes = [
        mp.Process(
            target=run_peer,
            args=(index, initial_peers, metrics, group_log, roster, filter_config, barrier,
                  done_count, all_done, args),
            name=f"peer-{index}", daemon=False,
        )
        for index in range(args.num_peers)
    ]

    identities, train_rows, group_rows = {}, [], []

    def drain():
        while not metrics.empty():
            row = metrics.get()
            if row.pop("kind") == "identity":
                identities[row["peer_id"]] = row["peer"]
            else:
                train_rows.append(row)
        while not group_log.empty():
            group_rows.append(group_log.get())

    # DHT-only Sybils first, so they are in routing tables and holding the key's
    # replicas before any honest peer advertises on it.
    stop_sybils = mp.Event()
    sybils = []
    if args.arm != "honest":
        for index in range(args.sybil_nodes):
            sybil = mp.Process(
                target=run_sybil_dht,
                args=(index, initial_peers, filter_config, stop_sybils, args),
                name=f"sybil-{index}", daemon=False,
            )
            sybil.start()
            sybils.append(sybil)
        time.sleep(3)  # let them join and announce before the swarm forms

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
        stop_sybils.set()
        for sybil in sybils:
            sybil.join(timeout=15)
            if sybil.is_alive():
                sybil.terminate()
        try:
            bootstrap.shutdown()
        except Exception:
            pass

    lies = filter_config.get("lies", 0)
    write_results(group_rows, train_rows, identities, args)
    report(group_rows, identities, args, lies, elapsed=time.perf_counter() - started)


def write_results(group_rows, train_rows, identities, args) -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    for row in group_rows:
        row["member_peers"] = " ".join(str(identities.get(p, "?")) for p in row["members"].split())

    path = RESULTS_DIR / f"step6_isolation_{args.arm}_k{args.attackers}_n{args.num_peers}.csv"
    fields = ["peer", "round", "group_id", "group_size", "member_peers", "led", "members"]
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(sorted(group_rows, key=lambda r: (r["round"], r["peer"])))
    print(f"\nwrote {path}  ({len(group_rows)} rows)")

    seen = [row["peers_seen"] for row in train_rows] or [0]
    print(f"peers visible to each other: min {min(seen)}, max {max(seen)}")
    if max(seen) <= 1:
        print("  !! NO AVERAGING HAPPENED -- see HANDOFF section 3")


def report(group_rows, identities, args, lies, elapsed) -> None:
    if not group_rows:
        print("no group observations recorded")
        return

    attackers = set(range(1, args.attackers + 1))
    victim_rows = sorted((r for r in group_rows if r["peer"] == VICTIM), key=lambda r: r["round"])

    print(f"\n{'round':>5} {'led':>4} {'victim group':>26} {'beta':>6} {'captured':>9}")
    betas, led_count, captured = [], 0, 0
    for row in victim_rows:
        members = [int(m) for m in row["member_peers"].split() if m != "?"]
        attacker_here = sum(1 for m in members if m in attackers)
        beta = attacker_here / len(members) if members else 0.0
        betas.append(beta)
        led_count += row["led"]
        # a clean capture: the victim plus attackers and nobody else
        clean = bool(members) and set(members) <= (attackers | {VICTIM}) and attacker_here > 0
        captured += clean
        print(f"{row['round']:>5} {row['led']:>4} {' '.join(map(str, members)):>26} "
              f"{beta:>6.3f} {'yes' if clean else '':>9}")

    rounds = len(victim_rows)
    print(f"\nvictim led                : {led_count}/{rounds} rounds")
    print(f"victim's group captured   : {captured}/{rounds} rounds")
    print(f"mean beta over all rounds : {sum(betas) / rounds:.3f}" if rounds else "")
    if betas:
        nonzero = [b for b in betas if b > 0]
        print(f"beta when attacker present: {sum(nonzero) / len(nonzero):.3f} ({len(nonzero)}/{rounds})"
              if nonzero else "attacker never shared the victim's group")
    honest_joined = sum(
        1 for row in victim_rows
        if any(int(m) not in attackers and int(m) != VICTIM for m in row["member_peers"].split() if m != "?")
    )
    print(f"rounds an honest peer was in the victim's group: {honest_joined}/{rounds}")
    if args.arm != "honest":
        print(f"filtered DHT responses    : {lies}")
    print(f"\nwall clock: {elapsed:.1f}s")


if __name__ == "__main__":
    main()
