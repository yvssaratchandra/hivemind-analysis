"""
Step 13 -- can a peer win leadership with a fixed early deadline and no deadline read?

This is a small live-matchmaking experiment, not a DHT election simulation.  Both arms contain
seven honest peers and one attacker, use a target group size of four, and run all-reduce over a
four-element tensor.  The attacker knows the group key (it could not advertise otherwise), but:

* never calls ``GroupKeyManager.get_averagers``;
* never inspects a follower's incoming expiration field;
* never sends an outbound ``rpc_join_group`` request; and
* serves an ordinary all-reduce if honest followers select it as their leader.

The intended treatment is the attacker's self-declared deadline:

* ``normal``: fixed ``now + 5 s``, the honest matchmaking interval;
* ``early``: fixed ``now + 4 s``.

Both fresh arm networks reuse the same group key, seeded DHT node positions and launch jitter. They
generate fresh PeerIDs, so this remains a mechanism pilot rather than a paired effect-size estimate.

Run the one-round smoke test first, then the ten-round comparison only if it passes:

    .venv/bin/python research/hivemind/step13_blind_leader.py --phase smoke --rounds 1
    .venv/bin/python research/hivemind/step13_blind_leader.py --phase full --rounds 10

Each child event reads the round number from shared memory when the event is emitted.  Every
StepControl, including the attacker's, must reach FINISHED before the next round begins.  These two
details avoid the overlap and retrospective event-labelling problems in the earlier step-12 harness.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import multiprocessing as mp
import queue
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncIterator, Optional

import torch

import hivemind
from hivemind.averaging import DecentralizedAverager
from hivemind.averaging.control import AveragingStage, StepControl
from hivemind.averaging.group_info import GroupInfo
from hivemind.averaging.key_manager import GroupKeyManager
from hivemind.averaging.matchmaking import Matchmaking, PotentialLeaders
from hivemind.dht.routing import DHTID
from hivemind.p2p import P2PContext
from hivemind.proto import averaging_pb2
from hivemind.utils.timed_storage import get_dht_time

RESULTS_DIR = Path(__file__).resolve().parent / "results"
LOCALHOST = ["/ip4/127.0.0.1/tcp/0"]
ARMS = ("normal", "early")
OFFSETS = {"normal": 5.0, "early": 4.0}

# These objects are created by the parent before it forks any averager.  Child processes inherit
# their handles.  The process-local flag and offset are set only while the attacker is forked.
EVENTS: Optional[mp.Queue] = None
CURRENT_ROUND: Optional[mp.Value] = None
ATTACKER_THIS_PROCESS = False
ATTACKER_OFFSET_THIS_PROCESS = 5.0


def _record(kind: str, peer: object = "", other: object = "", detail: str = "") -> None:
    """Emit an event with the round sampled inside the child process."""
    if EVENTS is None:
        return
    round_id = int(CURRENT_ROUND.value) if CURRENT_ROUND is not None else -1
    EVENTS.put((round_id, kind, str(peer), str(other), detail, time.time()))


# Keep the original methods so every honest process follows Hivemind's unmodified path.
_ORIGINAL_GET_AVERAGERS = GroupKeyManager.get_averagers
_ORIGINAL_UPDATE_QUEUE = PotentialLeaders._update_queue_periodically
_ORIGINAL_DECLARE = PotentialLeaders._declare_averager_periodically
_ORIGINAL_REQUEST_POTENTIAL = Matchmaking._request_join_potential_leaders
_ORIGINAL_REQUEST_JOIN = Matchmaking._request_join_group
_ORIGINAL_CHECK_REJECTION = Matchmaking._check_reasons_to_reject


async def _guarded_get_averagers(self, *args, **kwargs):
    """Make an attacker DHT-advertisement read observable and fail closed."""
    if ATTACKER_THIS_PROCESS:
        _record("forbidden_dht_advertisement_read", self.peer_id, kwargs.get("group_key", ""))
        raise AssertionError("blind attacker attempted to call get_averagers")
    return await _ORIGINAL_GET_AVERAGERS(self, *args, **kwargs)


async def _patched_update_queue(self, key_manager: GroupKeyManager) -> None:
    """Do not populate the attacker's leader queue, which would read competitor deadlines."""
    if not ATTACKER_THIS_PROCESS:
        return await _ORIGINAL_UPDATE_QUEUE(self, key_manager)
    _record("leader_queue_disabled", self.peer_id, key_manager.current_key)
    await asyncio.Future()  # begin_search cancels this task during ordinary step cleanup


async def _patched_declare(self, step: StepControl, key_manager: GroupKeyManager) -> None:
    """Publish a fixed deadline in the attacker process; preserve the stock withdrawal path."""
    if not ATTACKER_THIS_PROCESS:
        return await _ORIGINAL_DECLARE(self, step, key_manager)

    async with self.lock_declare:
        try:
            while True:
                await self.running.wait()
                now = get_dht_time()
                declared = float(min(now + ATTACKER_OFFSET_THIS_PROCESS, self.search_end_time))
                self.declared_group_key = group_key = key_manager.current_key
                self.declared_expiration_time = declared
                self.declared_expiration.set()
                accepted = await key_manager.declare_averager(
                    group_key, self.peer_id, expiration_time=declared
                )
                _record(
                    "declared",
                    self.peer_id,
                    group_key,
                    f"offset={declared - now:.6f};accepted={bool(accepted)}",
                )
                await asyncio.sleep(max(0.0, declared - get_dht_time()))
                if self.running.is_set() and len(self.leader_queue) == 0:
                    await key_manager.update_key_on_not_enough_peers()
        finally:
            if self.declared_group_key is not None:
                previous_key = self.declared_group_key
                previous_expiration = self.declared_expiration_time
                self.declared_group_key = None
                self.declared_expiration_time = float("inf")
                await key_manager.declare_averager(
                    previous_key,
                    self.peer_id,
                    previous_expiration,
                    looking_for_group=False,
                )
                _record("withdrew", self.peer_id, previous_key)


async def _patched_request_potential(self, step: StepControl) -> GroupInfo:
    """Advertise and wait for followers without reading or selecting another leader."""
    if not ATTACKER_THIS_PROCESS:
        return await _ORIGINAL_REQUEST_POTENTIAL(self, step)

    async with self.potential_leaders.begin_search(
        step, self.group_key_manager, declare=not self.client_mode
    ):
        _record("passive_leader_wait", self.peer_id, self.group_key_manager.current_key)
        # The outer look_for_group coroutine also waits on this future.  Shielding prevents
        # cancellation of this background task from cancelling the shared assembled_group.
        return await asyncio.shield(self.assembled_group)


async def _guarded_request_join(self, leader) -> Optional[GroupInfo]:
    """Record and suppress any attacker path that would contact a competing leader."""
    if ATTACKER_THIS_PROCESS:
        _record("forbidden_outbound_join", self.peer_id, leader)
        return None
    _record("outbound_join", self.peer_id, leader)
    return await _ORIGINAL_REQUEST_JOIN(self, leader)


def _blind_check_rejection(
    self, request: averaging_pb2.JoinRequest, context: P2PContext
) -> Optional[averaging_pb2.MessageFromLeader]:
    """Apply ordinary compatibility checks without reading ``request.expiration``.

    Stock admission compares the incoming expiration with the leader's declaration. That happens
    after leadership selection, but it is still a competitor-deadline read. The blind attacker
    deliberately ignores that field and accepts these locally generated, otherwise valid requests.
    """
    if not ATTACKER_THIS_PROCESS:
        return _ORIGINAL_CHECK_REJECTION(self, request, context)

    _record("inbound_deadline_ignored", self.peer_id, context.remote_id)
    if not self.is_looking_for_group or self.assembled_group.done():
        return averaging_pb2.MessageFromLeader(code=averaging_pb2.NOT_LOOKING_FOR_GROUP)
    if (
        not isinstance(request.schema_hash, bytes)
        or len(request.schema_hash) == 0
        or self.client_mode
        or not isinstance(request.group_key, str)
    ):
        return averaging_pb2.MessageFromLeader(code=averaging_pb2.PROTOCOL_VIOLATION)
    if request.schema_hash != self.schema_hash:
        return averaging_pb2.MessageFromLeader(code=averaging_pb2.BAD_SCHEMA_HASH)
    if request.group_key != self.group_key_manager.current_key:
        return averaging_pb2.MessageFromLeader(code=averaging_pb2.BAD_GROUP_KEY)
    if self.potential_leaders.declared_group_key is None:
        return averaging_pb2.MessageFromLeader(code=averaging_pb2.NOT_DECLARED)
    if self.current_leader is not None:
        return averaging_pb2.MessageFromLeader(
            code=averaging_pb2.NOT_A_LEADER, suggested_leader=self.current_leader.to_bytes()
        )
    if context.remote_id == self.peer_id or context.remote_id in self.current_followers:
        return averaging_pb2.MessageFromLeader(code=averaging_pb2.DUPLICATE_PEER_ID)
    if self.target_group_size is not None and len(self.current_followers) + 1 >= self.target_group_size:
        return averaging_pb2.MessageFromLeader(code=averaging_pb2.GROUP_IS_FULL)
    return None


GroupKeyManager.get_averagers = _guarded_get_averagers
PotentialLeaders._update_queue_periodically = _patched_update_queue
PotentialLeaders._declare_averager_periodically = _patched_declare
Matchmaking._request_join_potential_leaders = _patched_request_potential
Matchmaking._request_join_group = _guarded_request_join
Matchmaking._check_reasons_to_reject = _blind_check_rejection


class BlindLeaderAverager(DecentralizedAverager):
    """A common servicer class for honest and attacker peers; only child globals alter behavior."""

    async def rpc_join_group(
        self, request: averaging_pb2.JoinRequest, context: P2PContext
    ) -> AsyncIterator[averaging_pb2.MessageFromLeader]:
        # The annotations are required by Hivemind's RPC handler discovery.
        _record("join_received", self.peer_id, context.remote_id)
        async for response in super().rpc_join_group(request, context):
            code = averaging_pb2.MessageCode.Name(response.code)
            _record("join_reply", self.peer_id, context.remote_id, code)
            if response.code == averaging_pb2.BEGIN_ALLREDUCE:
                _record(
                    "begin_allreduce_sent",
                    self.peer_id,
                    context.remote_id,
                    f"group_size={len(response.ordered_peer_ids)}",
                )
            yield response


def _drain_until_quiet(events: mp.Queue, quiet_seconds: float = 0.25, cap_seconds: float = 2.0) -> list[tuple]:
    """Drain queue feeder threads without assigning round IDs in the parent."""
    drained: list[tuple] = []
    stop_at = time.perf_counter() + cap_seconds
    quiet_until = time.perf_counter() + quiet_seconds
    while time.perf_counter() < stop_at and time.perf_counter() < quiet_until:
        timeout = max(0.0, min(0.05, quiet_until - time.perf_counter()))
        try:
            drained.append(events.get(timeout=timeout))
            quiet_until = min(stop_at, time.perf_counter() + quiet_seconds)
        except queue.Empty:
            pass
    return drained


def _wait_for_finished(controls: dict[str, StepControl], timeout: float) -> list[str]:
    """Wait for child cleanup; StepControl.done() alone is insufficient after cancel()."""
    deadline = time.perf_counter() + timeout
    while time.perf_counter() < deadline:
        unfinished = [name for name, control in controls.items() if control.stage != AveragingStage.FINISHED]
        if not unfinished:
            return []
        time.sleep(0.05)
    return [name for name, control in controls.items() if control.stage != AveragingStage.FINISHED]


def _control_outcome(control: StepControl) -> tuple[str, str]:
    if not control.done():
        return "not_finished", "NotFinished"
    if control.cancelled():
        return "cancelled", "CancelledError"
    try:
        control.result()
        return "completed", ""
    except BaseException as exc:
        return "failed", type(exc).__name__


def _parse_accepted(detail: str) -> bool:
    return "accepted=True" in detail


def run_arm(
    arm: str,
    *,
    honest: int,
    group_size: int,
    rounds: int,
    seed: int,
    jitter: float,
    step_timeout: float,
    run_id: str,
    node_ids: list[DHTID],
) -> tuple[list[dict], list[dict], str]:
    """Run one fixed-deadline arm on a fresh local swarm."""
    global ATTACKER_OFFSET_THIS_PROCESS, ATTACKER_THIS_PROCESS, CURRENT_ROUND, EVENTS

    EVENTS = mp.Queue()
    CURRENT_ROUND = mp.Value("i", -1)
    rng = random.Random(seed)  # paired arms receive exactly the same launch-jitter sequence

    bootstrap = hivemind.DHT(
        start=True,
        host_maddrs=LOCALHOST,
        node_id=node_ids[0],
    )
    initial_peers = bootstrap.get_visible_maddrs()
    dhts = [
        hivemind.DHT(
            initial_peers=initial_peers,
            start=True,
            host_maddrs=LOCALHOST,
            node_id=node_ids[index + 1],
        )
        for index in range(honest + 1)
    ]

    common = dict(
        prefix=run_id,
        target_group_size=group_size,
        min_group_size=2,
        min_matchmaking_time=5.0,
        request_timeout=3.0,
        start=True,
    )

    ATTACKER_THIS_PROCESS = True
    ATTACKER_OFFSET_THIS_PROCESS = OFFSETS[arm]
    attacker = BlindLeaderAverager([torch.zeros(4)], dhts[0], **common)
    ATTACKER_THIS_PROCESS = False
    honest_peers = [
        BlindLeaderAverager([torch.zeros(4)], dhts[index + 1], **common)
        for index in range(honest)
    ]
    all_peers = [attacker, *honest_peers]
    attacker_id = str(attacker.peer_id)

    summaries: list[dict] = []
    raw_events: list[dict] = []
    active_controls: dict[str, StepControl] = {}
    try:
        time.sleep(2.0)  # identical routing-table warm-up in both arms
        for round_id in range(rounds):
            # There must be no preceding step still emitting events when this value changes.
            stale = _drain_until_quiet(EVENTS, quiet_seconds=0.05, cap_seconds=0.2)
            if stale:
                raise RuntimeError(f"{len(stale)} stale events arrived before round {round_id}")
            CURRENT_ROUND.value = round_id

            actors = [("attacker", attacker)] + [
                (f"honest_{index}", peer) for index, peer in enumerate(honest_peers)
            ]
            launches = sorted((rng.uniform(0.0, jitter), name, peer) for name, peer in actors)
            started = time.perf_counter()
            active_controls = {}
            for launch_after, name, peer in launches:
                remaining = started + launch_after - time.perf_counter()
                if remaining > 0:
                    time.sleep(remaining)
                active_controls[name] = peer.step(
                    gather={"round": round_id, "role": name},
                    wait=False,
                    allow_retries=False,
                    timeout=step_timeout,
                )

            # Poll all controls concurrently.  Give the child a short grace period beyond the
            # advertised step timeout, then explicitly cancel anything unresolved.
            hard_stop = time.perf_counter() + step_timeout + 5.0
            while time.perf_counter() < hard_stop and not all(
                control.done() for control in active_controls.values()
            ):
                time.sleep(0.05)
            cancelled_by_harness: set[str] = set()
            for name, control in active_controls.items():
                if not control.done():
                    cancelled_by_harness.add(name)
                    control.cancel()

            stage_leaks = _wait_for_finished(active_controls, timeout=10.0)
            if stage_leaks:
                raise RuntimeError(
                    "refusing to advance with unfinished StepControls: " + ", ".join(stage_leaks)
                )
            round_events_raw = _drain_until_quiet(EVENTS)
            elapsed = time.perf_counter() - started

            for event_round, kind, peer, other, detail, emitted_at in round_events_raw:
                raw_events.append(
                    dict(
                        arm=arm,
                        round=event_round,
                        kind=kind,
                        peer=peer,
                        other=other,
                        detail=detail,
                        emitted_at=f"{emitted_at:.6f}",
                    )
                )

            round_events = [event for event in raw_events if event["arm"] == arm and event["round"] == round_id]
            begin_events = [
                event
                for event in round_events
                if event["kind"] == "begin_allreduce_sent" and event["peer"] == attacker_id
            ]
            follower_ids = {event["other"] for event in begin_events}
            forbidden_reads = sum(
                event["kind"] == "forbidden_dht_advertisement_read" and event["peer"] == attacker_id
                for event in round_events
            )
            forbidden_joins = sum(
                event["kind"] == "forbidden_outbound_join" and event["peer"] == attacker_id
                for event in round_events
            )
            declarations = [
                event
                for event in round_events
                if event["kind"] == "declared" and event["peer"] == attacker_id
            ]
            joins_received = sum(
                event["kind"] == "join_received" and event["peer"] == attacker_id
                for event in round_events
            )
            inbound_deadlines_ignored = sum(
                event["kind"] == "inbound_deadline_ignored" and event["peer"] == attacker_id
                for event in round_events
            )

            outcomes = {name: _control_outcome(control) for name, control in active_controls.items()}
            honest_completed = sum(
                outcomes[f"honest_{index}"][0] == "completed" for index in range(honest)
            )
            honest_errors = ";".join(
                f"honest_{index}:{outcomes[f'honest_{index}'][1]}"
                for index in range(honest)
                if outcomes[f"honest_{index}"][0] != "completed"
            )
            attacker_status, attacker_error = outcomes["attacker"]
            summaries.append(
                dict(
                    arm=arm,
                    round=round_id,
                    deadline_offset_s=OFFSETS[arm],
                    attacker_id=attacker_id,
                    attacker_led=int(bool(begin_events)),
                    follower_count=len(follower_ids),
                    attacker_status=attacker_status,
                    attacker_error=attacker_error,
                    honest_completed=honest_completed,
                    honest_failed=honest - honest_completed,
                    honest_errors=honest_errors,
                    declarations=len(declarations),
                    declaration_accepted=int(bool(declarations) and all(_parse_accepted(e["detail"]) for e in declarations)),
                    forbidden_dht_advertisement_reads=forbidden_reads,
                    forbidden_outbound_joins=forbidden_joins,
                    attacker_joins_received=joins_received,
                    inbound_deadlines_ignored=inbound_deadlines_ignored,
                    harness_cancellations=";".join(sorted(cancelled_by_harness)),
                    stage_leaks=";".join(stage_leaks),
                    elapsed_s=f"{elapsed:.3f}",
                )
            )
            print(
                f"  {arm:<6} round {round_id:>2}: led={int(bool(begin_events))} "
                f"followers={len(follower_ids)} honest={honest_completed}/{honest} "
                f"attacker={attacker_status} elapsed={elapsed:.2f}s"
            )
            CURRENT_ROUND.value = -1
            active_controls = {}
    finally:
        for control in active_controls.values():
            if not control.done():
                control.cancel()
        if active_controls:
            _wait_for_finished(active_controls, timeout=10.0)
        for peer in all_peers:
            peer.shutdown()
        for dht in [*dhts, bootstrap]:
            dht.shutdown()
        if EVENTS is not None:
            EVENTS.close()
        EVENTS = None
        CURRENT_ROUND = None
        ATTACKER_THIS_PROCESS = False

    return summaries, raw_events, attacker_id


def validate_run(phase: str, summaries: list[dict]) -> tuple[bool, list[str]]:
    """Validate experiment controls separately from the leadership outcome."""
    problems: list[str] = []
    for row in summaries:
        label = f"{row['arm']} round {row['round']}"
        if row["declarations"] < 1 or not row["declaration_accepted"]:
            problems.append(f"{label}: attacker declaration was not observed and accepted")
        if row["forbidden_dht_advertisement_reads"]:
            problems.append(f"{label}: attacker attempted a DHT advertisement read")
        if row["forbidden_outbound_joins"]:
            problems.append(f"{label}: attacker attempted an outbound join")
        if row["attacker_joins_received"] != row["inbound_deadlines_ignored"]:
            problems.append(f"{label}: an incoming follower deadline was not ignored")
        if row["stage_leaks"]:
            problems.append(f"{label}: StepControl cleanup leak ({row['stage_leaks']})")
        if row["harness_cancellations"]:
            problems.append(f"{label}: harness had to cancel controls ({row['harness_cancellations']})")
        if row["attacker_status"] == "not_finished":
            problems.append(f"{label}: attacker control did not resolve")
        if row["honest_failed"]:
            problems.append(f"{label}: {row['honest_failed']} honest controls failed")

    if phase == "smoke":
        early = [row for row in summaries if row["arm"] == "early"]
        if not early or not any(row["attacker_led"] for row in early):
            problems.append("early smoke arm produced no leadership signal; do not scale yet")
    return not problems, problems


def _write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    with path.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--phase", choices=("smoke", "full"), default="smoke")
    parser.add_argument("--honest", type=int, default=7)
    parser.add_argument("--group-size", type=int, default=4)
    parser.add_argument("--rounds", type=int, default=None)
    parser.add_argument("--seed", type=int, default=1313)
    parser.add_argument("--jitter", type=float, default=0.2)
    parser.add_argument("--step-timeout", type=float, default=12.0)
    parser.add_argument("--arms", nargs="+", choices=ARMS, default=list(ARMS))
    parser.add_argument("--label", default="")
    args = parser.parse_args()

    rounds = args.rounds if args.rounds is not None else (1 if args.phase == "smoke" else 10)
    if args.honest < 1 or args.group_size < 2 or rounds < 1:
        parser.error("honest, group-size, and rounds must be positive (group-size at least 2)")
    if not 0.0 <= args.jitter <= 1.0:
        parser.error("jitter must be between 0 and 1 second")
    if args.step_timeout <= 5.0:
        parser.error("step-timeout must exceed the 5-second honest matchmaking interval")

    label = args.label or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    print(
        f"Step 13 {args.phase}: {args.honest} honest + 1 attacker, group {args.group_size}, "
        f"{rounds} round(s), seed {args.seed}, arms {args.arms}"
    )

    summaries: list[dict] = []
    events: list[dict] = []
    # Use the same group key and DHT positions in both fresh arm networks. PeerIDs are regenerated:
    # restarting identical libp2p identities proved unstable in this testbed, so identity/tie-break
    # variation remains an explicit pilot limitation rather than an unreported harness failure.
    node_ids = [DHTID.generate(f"step13-{args.seed}-{index}".encode()) for index in range(args.honest + 2)]
    for arm in args.arms:
        print(f" arm {arm}: attacker declares now + {OFFSETS[arm]:g}s")
        arm_summaries, arm_events, _ = run_arm(
            arm,
            honest=args.honest,
            group_size=args.group_size,
            rounds=rounds,
            seed=args.seed,
            jitter=args.jitter,
            step_timeout=args.step_timeout,
            run_id=f"e13_{label}",
            node_ids=node_ids,
        )
        summaries.extend(arm_summaries)
        events.extend(arm_events)

    RESULTS_DIR.mkdir(exist_ok=True)
    tag = f"{args.phase}_{label}_n{args.honest}_g{args.group_size}_r{rounds}_s{args.seed}"
    summary_path = RESULTS_DIR / f"step13_blind_leader_rounds_{tag}.csv"
    events_path = RESULTS_DIR / f"step13_blind_leader_events_{tag}.csv"
    _write_csv(summary_path, summaries)
    _write_csv(events_path, events)

    valid, problems = validate_run(args.phase, summaries)
    normal_led = sum(row["attacker_led"] for row in summaries if row["arm"] == "normal")
    early_led = sum(row["attacker_led"] for row in summaries if row["arm"] == "early")
    print(f" leadership: normal {normal_led}/{rounds}, early {early_led}/{rounds}")
    if args.phase == "full" and set(args.arms) == set(ARMS):
        required_early = (4 * rounds + 4) // 5
        interesting = early_led >= required_early and early_led > normal_led
        print(f" signal rule (early >= {required_early}/{rounds} and early > normal): {interesting}")
    print(f" wrote {summary_path.relative_to(Path.cwd())}")
    print(f" wrote {events_path.relative_to(Path.cwd())}")

    if not valid:
        print(" control validation: FAIL", file=sys.stderr)
        for problem in problems:
            print(f"  - {problem}", file=sys.stderr)
        return 2
    print(" control validation: PASS")
    return 0


if __name__ == "__main__":
    mp.set_start_method("fork", force=True)
    raise SystemExit(main())
