"""
Track 1, stage B -- adaptive deadline undercut against live Hivemind matchmaking.

This stage runs real ``DecentralizedAverager`` processes over a localhost Hivemind DHT. Honest peers
receive heterogeneous scheduled times that are not exposed to the attacker. The attacker observes
actual DHT advertisements for a fixed interval and either:

* ``late``: publishes one floating-point step after the latest observed deadline (control), or
* ``undercut``: publishes one floating-point step before the earliest observed deadline (attack).

It never initiates a join request. If followers select it, it uses Hivemind's stock admission checks
and serves an ordinary four-element all-reduce. Leadership is counted only when the attacker sends
``BEGIN_ALLREDUCE`` to followers.

Run the one-round smoke test first. Run the ten-round confirmation only if the smoke gate passes:

    .venv/bin/python research/step15_adaptive_leader_live.py --phase smoke
    .venv/bin/python research/step15_adaptive_leader_live.py --phase confirm
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import math
import multiprocessing as mp
import queue
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncIterator, Callable, Optional

import numpy as np
import torch

import hivemind
from hivemind.averaging import DecentralizedAverager
from hivemind.averaging.control import AveragingStage, StepControl
from hivemind.averaging.group_info import GroupInfo
from hivemind.averaging.matchmaking import Matchmaking, PotentialLeaders
from hivemind.dht.routing import DHTID
from hivemind.p2p import P2PContext
from hivemind.proto import averaging_pb2
from hivemind.utils.timed_storage import get_dht_time

RESULTS_DIR = Path(__file__).resolve().parent / "results"
LOCALHOST = ["/ip4/127.0.0.1/tcp/0"]
ARMS = ("late", "undercut")

EVENTS: Optional[mp.Queue] = None
CURRENT_ROUND: Optional[mp.Value] = None
STRATEGY_THIS_PROCESS = ""
REQUEST_TIMEOUT_THIS_PROCESS = 1.0
SAFETY_MARGIN_THIS_PROCESS = 0.5
OBSERVATION_WINDOW_THIS_PROCESS = 0.1
POLL_INTERVAL_THIS_PROCESS = 0.05


def _record(kind: str, peer: object = "", other: object = "", detail: Optional[dict] = None) -> None:
    if EVENTS is None:
        return
    round_id = int(CURRENT_ROUND.value) if CURRENT_ROUND is not None else -1
    EVENTS.put(
        (
            round_id,
            kind,
            str(peer),
            str(other),
            json.dumps(detail or {}, sort_keys=True),
            time.time(),
        )
    )


_ORIGINAL_UPDATE_QUEUE = PotentialLeaders._update_queue_periodically
_ORIGINAL_DECLARE = PotentialLeaders._declare_averager_periodically
_ORIGINAL_REQUEST_POTENTIAL = Matchmaking._request_join_potential_leaders
_ORIGINAL_REQUEST_JOIN = Matchmaking._request_join_group


async def _patched_update_queue(self, key_manager) -> None:
    """Disable the attacker's stock read loop so every attacker DHT read is measured below."""
    if not STRATEGY_THIS_PROCESS:
        return await _ORIGINAL_UPDATE_QUEUE(self, key_manager)
    _record("leader_queue_disabled", self.peer_id, key_manager.current_key)
    await asyncio.Future()


async def _patched_declare(self, step: StepControl, key_manager) -> None:
    """Observe actual advertisements, choose a deadline, and verify that the write is visible."""
    if not STRATEGY_THIS_PROCESS:
        return await _ORIGINAL_DECLARE(self, step, key_manager)

    async with self.lock_declare:
        observed_candidates = {}
        first_candidate_at = None
        try:
            while get_dht_time() < self.search_end_time:
                await self.running.wait()
                read_started = time.perf_counter()
                try:
                    seen = await key_manager.get_averagers(key_manager.current_key, only_active=True)
                    read_error = ""
                except Exception as exc:
                    seen = []
                    read_error = type(exc).__name__
                read_latency = time.perf_counter() - read_started
                read_finished = get_dht_time()
                required_remaining = REQUEST_TIMEOUT_THIS_PROCESS + SAFETY_MARGIN_THIS_PROCESS
                current_candidates = [
                    (peer_id, expiration)
                    for peer_id, expiration in seen
                    if peer_id != self.peer_id
                    and math.isfinite(expiration)
                    and expiration > read_finished + required_remaining
                ]
                for peer_id, expiration in current_candidates:
                    observed_candidates[peer_id] = expiration
                observed_candidates = {
                    peer_id: expiration
                    for peer_id, expiration in observed_candidates.items()
                    if expiration > read_finished + required_remaining
                }
                if not observed_candidates:
                    first_candidate_at = None
                elif first_candidate_at is None:
                    first_candidate_at = read_finished
                _record(
                    "adaptive_read",
                    self.peer_id,
                    key_manager.current_key,
                    {
                        "candidate_count": len(current_candidates),
                        "read_error": read_error,
                        "read_latency_s": read_latency,
                        "observed_candidate_count": len(observed_candidates),
                        "seen_count": len(seen),
                    },
                )
                if (
                    not observed_candidates
                    or first_candidate_at is None
                    or read_finished < first_candidate_at + OBSERVATION_WINDOW_THIS_PROCESS
                ):
                    await asyncio.sleep(POLL_INTERVAL_THIS_PROCESS)
                    continue

                candidates = list(observed_candidates.items())

                if STRATEGY_THIS_PROCESS == "undercut":
                    target_peer, target_expiration = min(candidates, key=lambda item: item[1])
                    declared = float(np.nextafter(target_expiration, -np.inf))
                else:
                    target_peer, target_expiration = max(candidates, key=lambda item: item[1])
                    declared = float(np.nextafter(target_expiration, np.inf))

                self.declared_group_key = group_key = key_manager.current_key
                self.declared_expiration_time = declared
                self.declared_expiration.set()
                write_started = time.perf_counter()
                _record(
                    "adaptive_publication_started",
                    self.peer_id,
                    target_peer,
                    {
                        "candidate_count": len(candidates),
                        "declared_expiration": declared,
                        "remaining_before_write_s": declared - get_dht_time(),
                        "strategy": STRATEGY_THIS_PROCESS,
                        "target_expiration": target_expiration,
                    },
                )
                store_return = await key_manager.declare_averager(
                    group_key, self.peer_id, expiration_time=declared
                )
                write_latency = time.perf_counter() - write_started
                write_finished = get_dht_time()
                verify_started = time.perf_counter()
                try:
                    after_write = await key_manager.get_averagers(group_key, only_active=True)
                    verify_error = ""
                except Exception as exc:
                    after_write = []
                    verify_error = type(exc).__name__
                verify_latency = time.perf_counter() - verify_started
                visible_after_write = any(
                    peer_id == self.peer_id and expiration == declared
                    for peer_id, expiration in after_write
                )
                usable = bool(store_return or visible_after_write)
                _record(
                    "adaptive_publication",
                    self.peer_id,
                    target_peer,
                    {
                        "accepted": bool(store_return),
                        "candidate_count": len(candidates),
                        "declared_expiration": declared,
                        "read_latency_s": read_latency,
                        "remaining_at_read_s": declared - read_finished,
                        "remaining_at_write_s": declared - write_finished,
                        "strategy": STRATEGY_THIS_PROCESS,
                        "target_expiration": target_expiration,
                        "usable": usable,
                        "verify_error": verify_error,
                        "verify_latency_s": verify_latency,
                        "visible_after_write": visible_after_write,
                        "write_latency_s": write_latency,
                    },
                )
                # Match the stock publisher: the aggregate store result is diagnostic, not a
                # prerequisite for serving joins. Some replicas may contain the record even when
                # the aggregate return value (and an immediate read) says otherwise.
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
    """Advertise and await followers without ever selecting another leader."""
    if not STRATEGY_THIS_PROCESS:
        return await _ORIGINAL_REQUEST_POTENTIAL(self, step)
    async with self.potential_leaders.begin_search(
        step, self.group_key_manager, declare=not self.client_mode
    ):
        _record("passive_leader_wait", self.peer_id, self.group_key_manager.current_key)
        return await asyncio.shield(self.assembled_group)


async def _guarded_request_join(self, leader) -> Optional[GroupInfo]:
    if STRATEGY_THIS_PROCESS:
        _record("forbidden_outbound_join", self.peer_id, leader)
        return None
    _record("outbound_join", self.peer_id, leader)
    return await _ORIGINAL_REQUEST_JOIN(self, leader)


PotentialLeaders._update_queue_periodically = _patched_update_queue
PotentialLeaders._declare_averager_periodically = _patched_declare
Matchmaking._request_join_potential_leaders = _patched_request_potential
Matchmaking._request_join_group = _guarded_request_join


class AdaptiveLeaderAverager(DecentralizedAverager):
    """One RPC class for honest peers and attacker; process globals select attacker behavior."""

    async def rpc_join_group(
        self, request: averaging_pb2.JoinRequest, context: P2PContext
    ) -> AsyncIterator[averaging_pb2.MessageFromLeader]:
        _record("join_received", self.peer_id, context.remote_id)
        async for response in super().rpc_join_group(request, context):
            code = averaging_pb2.MessageCode.Name(response.code)
            _record("join_reply", self.peer_id, context.remote_id, {"code": code})
            if response.code == averaging_pb2.BEGIN_ALLREDUCE:
                _record(
                    "begin_allreduce_sent",
                    self.peer_id,
                    context.remote_id,
                    {"group_size": len(response.ordered_peer_ids)},
                )
            yield response


def _drain_until_quiet(events: mp.Queue, quiet_seconds: float = 0.25, cap_seconds: float = 2.0) -> list:
    drained = []
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
    deadline = time.perf_counter() + timeout
    while time.perf_counter() < deadline:
        unfinished = [name for name, control in controls.items() if control.stage != AveragingStage.FINISHED]
        if not unfinished:
            return []
        time.sleep(0.05)
    return [name for name, control in controls.items() if control.stage != AveragingStage.FINISHED]


def _outcome(control: StepControl) -> tuple[str, str]:
    if not control.done():
        return "not_finished", "NotFinished"
    if control.cancelled():
        return "cancelled", "CancelledError"
    try:
        control.result()
        return "completed", ""
    except BaseException as exc:
        return "failed", type(exc).__name__


def run_arm(
    arm: str,
    *,
    honest_count: int,
    group_size: int,
    rounds: int,
    seed: int,
    request_timeout: float,
    safety_margin: float,
    observation_window: float,
    schedule_min: float,
    schedule_max: float,
    step_timeout: float,
    jitter: float,
    run_id: str,
    node_ids: list[DHTID],
    round_offset: int = 0,
    filler_count: int = 0,
    on_averagers_ready: Optional[
        Callable[[list[AdaptiveLeaderAverager], list[hivemind.DHT]], None]
    ] = None,
) -> tuple[list[dict], list[dict]]:
    global CURRENT_ROUND, EVENTS, OBSERVATION_WINDOW_THIS_PROCESS
    global REQUEST_TIMEOUT_THIS_PROCESS, SAFETY_MARGIN_THIS_PROCESS, STRATEGY_THIS_PROCESS

    EVENTS = mp.Queue()
    CURRENT_ROUND = mp.Value("i", -1)
    rng = random.Random(seed)

    bootstrap = hivemind.DHT(start=True, host_maddrs=LOCALHOST, node_id=node_ids[0])
    initial_peers = bootstrap.get_visible_maddrs()
    dhts = [
        hivemind.DHT(
            initial_peers=initial_peers,
            start=True,
            host_maddrs=LOCALHOST,
            node_id=node_ids[index + 1],
        )
        for index in range(honest_count + 1)
    ]
    fillers = [
        hivemind.DHT(
            initial_peers=initial_peers,
            start=True,
            host_maddrs=LOCALHOST,
            node_id=node_ids[honest_count + 2 + index],
        )
        for index in range(filler_count)
    ]

    common = dict(
        prefix=run_id,
        target_group_size=group_size,
        min_group_size=2,
        min_matchmaking_time=max(2.0, request_timeout + 0.5),
        request_timeout=request_timeout,
        start=True,
    )
    STRATEGY_THIS_PROCESS = arm
    REQUEST_TIMEOUT_THIS_PROCESS = request_timeout
    SAFETY_MARGIN_THIS_PROCESS = safety_margin
    OBSERVATION_WINDOW_THIS_PROCESS = observation_window
    attacker = AdaptiveLeaderAverager([torch.zeros(4)], dhts[0], **common)
    STRATEGY_THIS_PROCESS = ""
    honest_peers = [
        AdaptiveLeaderAverager([torch.zeros(4)], dhts[index + 1], **common)
        for index in range(honest_count)
    ]
    all_peers = [attacker, *honest_peers]
    if on_averagers_ready is not None:
        on_averagers_ready(all_peers, [bootstrap, *dhts, *fillers])
    attacker_id = str(attacker.peer_id)

    summaries: list[dict] = []
    raw_events: list[dict] = []
    active_controls: dict[str, StepControl] = {}
    try:
        time.sleep(2.0)
        for local_round in range(rounds):
            round_id = round_offset + local_round
            stale = _drain_until_quiet(EVENTS, quiet_seconds=0.05, cap_seconds=0.2)
            if stale:
                raise RuntimeError(f"{len(stale)} stale events before round {round_id}")
            CURRENT_ROUND.value = round_id

            base = get_dht_time()
            scheduled_times = [base + rng.uniform(schedule_min, schedule_max) for _ in honest_peers]
            actors = [(f"honest_{index}", peer) for index, peer in enumerate(honest_peers)]
            actors.append(("attacker", attacker))
            launches = sorted((rng.uniform(0.0, jitter), name, peer) for name, peer in actors)
            started = time.perf_counter()
            active_controls = {}
            for launch_after, name, peer in launches:
                remaining = started + launch_after - time.perf_counter()
                if remaining > 0:
                    time.sleep(remaining)
                kwargs = dict(wait=False, allow_retries=False, timeout=step_timeout)
                if name != "attacker":
                    index = int(name.split("_")[1])
                    kwargs["scheduled_time"] = scheduled_times[index]
                active_controls[name] = peer.step(
                    gather={"round": round_id, "role": name},
                    **kwargs,
                )

            hard_stop = time.perf_counter() + step_timeout + 5.0
            hard_cancellations: set[str] = set()
            expected_control_cancel = False
            while time.perf_counter() < hard_stop:
                if all(control.done() for control in active_controls.values()):
                    break
                honest_done = all(
                    active_controls[f"honest_{index}"].done() for index in range(honest_count)
                )
                if arm == "late" and honest_done and not active_controls["attacker"].done():
                    active_controls["attacker"].cancel()
                    expected_control_cancel = True
                time.sleep(0.05)
            for name, control in active_controls.items():
                if not control.done():
                    hard_cancellations.add(name)
                    control.cancel()

            stage_leaks = _wait_for_finished(active_controls, timeout=10.0)
            if stage_leaks:
                raise RuntimeError("unfinished StepControls: " + ", ".join(stage_leaks))
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
            publications = [
                json.loads(event["detail"])
                for event in round_events
                if event["kind"] == "adaptive_publication" and event["peer"] == attacker_id
            ]
            publication_attempts = [
                json.loads(event["detail"])
                for event in round_events
                if event["kind"] == "adaptive_publication_started" and event["peer"] == attacker_id
            ]
            accepted_publications = [publication for publication in publications if publication["accepted"]]
            usable_publications = [publication for publication in publications if publication["usable"]]
            outbound_violations = sum(
                event["kind"] == "forbidden_outbound_join" and event["peer"] == attacker_id
                for event in round_events
            )
            outcomes = {name: _outcome(control) for name, control in active_controls.items()}
            honest_completed = sum(
                outcomes[f"honest_{index}"][0] == "completed" for index in range(honest_count)
            )
            min_remaining = min(
                (publication["remaining_before_write_s"] for publication in publication_attempts),
                default=float("nan"),
            )
            full_capture = len(follower_ids) == honest_count and outcomes["attacker"][0] == "completed"
            summaries.append(
                dict(
                    arm=arm,
                    round=round_id,
                    attacker_id=attacker_id,
                    honest_count=honest_count,
                    group_size=group_size,
                    hidden_schedule_min_s=f"{min(scheduled_times) - base:.6f}",
                    hidden_schedule_max_s=f"{max(scheduled_times) - base:.6f}",
                    attacker_led=int(bool(begin_events)),
                    full_capture=int(full_capture),
                    follower_count=len(follower_ids),
                    adaptive_reads=sum(event["kind"] == "adaptive_read" for event in round_events),
                    publication_attempts=len(publication_attempts),
                    publications=len(publications),
                    accepted_publications=len(accepted_publications),
                    usable_publications=len(usable_publications),
                    min_remaining_before_write_s=f"{min_remaining:.6f}",
                    attacker_status=outcomes["attacker"][0],
                    attacker_error=outcomes["attacker"][1],
                    honest_completed=honest_completed,
                    honest_failed=honest_count - honest_completed,
                    forbidden_outbound_joins=outbound_violations,
                    expected_control_cancel=int(expected_control_cancel),
                    hard_cancellations=";".join(sorted(hard_cancellations)),
                    stage_leaks=";".join(stage_leaks),
                    elapsed_s=f"{elapsed:.3f}",
                )
            )
            print(
                f"  {arm:<8} round {round_id:>2}: led={int(bool(begin_events))} "
                f"followers={len(follower_ids)} writes={len(publication_attempts)} "
                f"honest={honest_completed}/{honest_count} elapsed={elapsed:.2f}s"
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
        for dht in [*dhts, *fillers, bootstrap]:
            dht.shutdown()
        if EVENTS is not None:
            EVENTS.close()
        EVENTS = None
        CURRENT_ROUND = None
        STRATEGY_THIS_PROCESS = ""
    return summaries, raw_events


def validate_rows(phase: str, rows: list[dict], honest_count: int, rounds: int) -> tuple[bool, list[str]]:
    problems = []
    for row in rows:
        label = f"{row['arm']} round {row['round']}"
        if row["adaptive_reads"] < 1:
            problems.append(f"{label}: no DHT read was observed")
        if row["publication_attempts"] < 1:
            problems.append(f"{label}: no attacker publication was attempted")
        if row["arm"] == "late" and row["usable_publications"] < 1:
            problems.append(f"{label}: late-control publication was not confirmed usable")
        if row["forbidden_outbound_joins"]:
            problems.append(f"{label}: attacker attempted an outbound join")
        if row["hard_cancellations"]:
            problems.append(f"{label}: hard cancellation ({row['hard_cancellations']})")
        if row["stage_leaks"]:
            problems.append(f"{label}: StepControl cleanup leak ({row['stage_leaks']})")
        if row["honest_completed"] != honest_count:
            problems.append(f"{label}: only {row['honest_completed']}/{honest_count} honest peers completed")

    undercut = [row for row in rows if row["arm"] == "undercut"]
    late = [row for row in rows if row["arm"] == "late"]
    if phase == "smoke":
        if len(undercut) != 1 or not undercut[0]["full_capture"]:
            problems.append("adaptive smoke arm did not lead all three followers")
        if len(late) != 1 or late[0]["full_capture"]:
            problems.append("late control unexpectedly led a group")
    else:
        required = int(math.ceil(0.8 * rounds))
        undercut_captures = sum(row["full_capture"] for row in undercut)
        late_captures = sum(row["full_capture"] for row in late)
        if undercut_captures < required or undercut_captures <= late_captures:
            problems.append(
                "confirmation signal failed: "
                f"undercut {undercut_captures}/{rounds}, late {late_captures}/{rounds}"
            )
    return not problems, problems


def write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    with path.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--phase", choices=("smoke", "confirm"), default="smoke")
    parser.add_argument("--rounds", type=int, default=None)
    parser.add_argument("--honest", type=int, default=3)
    parser.add_argument("--group-size", type=int, default=4)
    parser.add_argument("--seed", type=int, default=1515)
    parser.add_argument("--request-timeout", type=float, default=1.0)
    parser.add_argument("--safety-margin", type=float, default=0.5)
    parser.add_argument("--observation-window", type=float, default=0.1)
    parser.add_argument("--schedule-min", type=float, default=4.0)
    parser.add_argument("--schedule-max", type=float, default=6.0)
    parser.add_argument("--step-timeout", type=float, default=9.0)
    parser.add_argument("--jitter", type=float, default=0.1)
    parser.add_argument("--fillers", type=int, default=0, help="DHT-only peers; no averaging or training")
    parser.add_argument("--arms", nargs="+", choices=ARMS, default=list(ARMS))
    parser.add_argument("--label", default="")
    args = parser.parse_args()

    rounds = args.rounds if args.rounds is not None else (1 if args.phase == "smoke" else 10)
    if args.honest < 2 or args.group_size < 2 or rounds < 1 or args.fillers < 0:
        parser.error("honest/group-size/rounds must be positive, with at least two honest peers")
    if args.group_size != args.honest + 1:
        parser.error("this controlled test requires group-size == honest + 1")
    if not 0 < args.request_timeout + args.safety_margin < args.schedule_min < args.schedule_max:
        parser.error("require 0 < request-timeout+safety-margin < schedule-min < schedule-max")
    if args.step_timeout <= args.schedule_max:
        parser.error("step-timeout must exceed schedule-max")
    if args.observation_window <= 0:
        parser.error("observation-window must be positive")
    if len(set(args.arms)) != len(args.arms):
        parser.error("arms must not contain duplicates")

    label = args.label or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    print(
        f"Step 15 live {args.phase}: {args.honest} honest + 1 adaptive peer, "
        f"group {args.group_size}, {rounds} round(s), seed {args.seed}"
    )
    summaries: list[dict] = []
    events: list[dict] = []
    for round_id in range(rounds):
        arm_order = args.arms if round_id % 2 == 0 else list(reversed(args.arms))
        for arm in arm_order:
            # A peer excluded from a group may advance to a different matchmaking key. Use a fresh
            # DHT and fresh averagers for every trial so no arm or round inherits that protocol state.
            trial_seed = args.seed + round_id
            node_ids = [
                DHTID.generate(f"step15-{args.seed}-{round_id}-{index}".encode())
                for index in range(args.honest + 2 + args.fillers)
            ]
            arm_rows, arm_events = run_arm(
                arm,
                honest_count=args.honest,
                group_size=args.group_size,
                rounds=1,
                seed=trial_seed,
                request_timeout=args.request_timeout,
                safety_margin=args.safety_margin,
                observation_window=args.observation_window,
                schedule_min=args.schedule_min,
                schedule_max=args.schedule_max,
                step_timeout=args.step_timeout,
                jitter=args.jitter,
                run_id=f"e15_{label}_{round_id}",
                node_ids=node_ids,
                round_offset=round_id,
                filler_count=args.fillers,
            )
            summaries.extend(arm_rows)
            events.extend(arm_events)

    valid, problems = validate_rows(args.phase, summaries, args.honest, rounds)
    RESULTS_DIR.mkdir(exist_ok=True)
    filler_tag = f"_f{args.fillers}" if args.fillers else ""
    tag = f"{args.phase}_{label}_h{args.honest}_g{args.group_size}_r{rounds}_s{args.seed}{filler_tag}"
    summary_path = RESULTS_DIR / f"step15_adaptive_live_rounds_{tag}.csv"
    events_path = RESULTS_DIR / f"step15_adaptive_live_events_{tag}.csv"
    write_csv(summary_path, summaries)
    write_csv(events_path, events)

    late_led = sum(row["attacker_led"] for row in summaries if row["arm"] == "late")
    undercut_led = sum(row["attacker_led"] for row in summaries if row["arm"] == "undercut")
    late_captures = sum(row["full_capture"] for row in summaries if row["arm"] == "late")
    undercut_captures = sum(row["full_capture"] for row in summaries if row["arm"] == "undercut")
    print(f" leadership: late {late_led}/{rounds}, undercut {undercut_led}/{rounds}")
    print(f" full capture: late {late_captures}/{rounds}, undercut {undercut_captures}/{rounds}")
    print(f" wrote {summary_path.relative_to(Path.cwd())}")
    print(f" wrote {events_path.relative_to(Path.cwd())}")
    if not valid:
        print(" validation: FAIL", file=sys.stderr)
        for problem in problems:
            print(f"  - {problem}", file=sys.stderr)
        return 2
    print(" validation: PASS")
    return 0


if __name__ == "__main__":
    mp.set_start_method("fork", force=True)
    raise SystemExit(main())
