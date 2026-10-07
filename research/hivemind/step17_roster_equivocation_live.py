"""
Track 2, stage B -- inject conflicting rosters through real BEGIN_ALLREDUCE streams.

Four real ``DecentralizedAverager`` peers use the localhost Hivemind DHT and libp2p stack. A designated
leader is established as an experiment precondition; leadership probability is not part of this test.
The arms differ only in what that leader sends after accepting three followers:

* ``control`` sends the same stock roster to every follower;
* ``equivocate`` swaps each recipient into the leader's own roster position and reorders ``gathered``
  in lockstep. Every view contains the same four unique authenticated PeerIDs and the same group ID.

All peers start with the identical tensor ``[0, 1, 2, 4]``. Correct averaging must leave it unchanged.
``part_size_bytes=4`` forces four equal one-float shards, so an inconsistent logical index cannot hide
behind a shape error. The attack counts as silent corruption only if every step completes, no reducer
is banned, no elements fall back locally, and the outputs are nevertheless wrong.

Run the one-pair smoke gate before the five-pair confirmation:

    .venv/bin/python research/hivemind/step17_roster_equivocation_live.py --phase smoke
    .venv/bin/python research/hivemind/step17_roster_equivocation_live.py --phase confirm
"""

from __future__ import annotations

import argparse
import csv
import json
import multiprocessing as mp
import queue
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncIterator, Optional, Sequence

import torch

import hivemind
from hivemind.averaging import DecentralizedAverager
from hivemind.averaging.allreduce import AllReduceRunner, AveragingMode
from hivemind.averaging.control import AveragingStage, StepControl
from hivemind.averaging.group_info import GroupInfo
from hivemind.dht.routing import DHTID
from hivemind.p2p import P2PContext, PeerID
from hivemind.proto import averaging_pb2
from hivemind.utils.asyncio import as_aiter, azip
from hivemind.utils.timed_storage import get_dht_time

RESULTS_DIR = Path(__file__).resolve().parent / "results"
LOCALHOST = ["/ip4/127.0.0.1/tcp/0"]
ARMS = ("control", "equivocate")
EVENTS: Optional[mp.Queue] = None


def _record(kind: str, peer: object = "", other: object = "", detail: Optional[dict] = None) -> None:
    if EVENTS is not None:
        EVENTS.put((kind, str(peer), str(other), json.dumps(detail or {}, sort_keys=True), time.time()))


class RosterEquivocationAverager(DecentralizedAverager):
    """Common RPC class for every peer; only the designated leader may equivocate."""

    def __init__(self, *args, equivocate: bool = False, **kwargs):
        self.equivocate = equivocate
        super().__init__(*args, **kwargs)

    async def rpc_join_group(
        self, request: averaging_pb2.JoinRequest, context: P2PContext
    ) -> AsyncIterator[averaging_pb2.MessageFromLeader]:
        async for response in super().rpc_join_group(request, context):
            if response.code != averaging_pb2.BEGIN_ALLREDUCE:
                yield response
                continue

            canonical_ids = list(response.ordered_peer_ids)
            canonical_gathered = list(response.gathered)
            permutation = list(range(len(canonical_ids)))
            leader_index = canonical_ids.index(self.peer_id.to_bytes())
            follower_index = canonical_ids.index(context.remote_id.to_bytes())
            if self.equivocate:
                permutation[leader_index], permutation[follower_index] = (
                    permutation[follower_index],
                    permutation[leader_index],
                )

            sent_ids = [canonical_ids[index] for index in permutation]
            sent_gathered = [canonical_gathered[index] for index in permutation]
            outgoing = averaging_pb2.MessageFromLeader()
            outgoing.CopyFrom(response)
            outgoing.ClearField("ordered_peer_ids")
            outgoing.ClearField("gathered")
            outgoing.ordered_peer_ids.extend(sent_ids)
            outgoing.gathered.extend(sent_gathered)
            _record(
                "begin_allreduce_sent",
                self.peer_id,
                context.remote_id,
                {
                    "canonical_roster": [str(PeerID(item)) for item in canonical_ids],
                    "group_id": response.group_id.hex(),
                    "leader_index": leader_index,
                    "recipient_index": sent_ids.index(context.remote_id.to_bytes()),
                    "sent_roster": [str(PeerID(item)) for item in sent_ids],
                },
            )
            yield outgoing

    async def _run_allreduce_inplace_(
        self,
        tensors: Sequence[torch.Tensor],
        group_info: GroupInfo,
        group_id: Optional[bytes] = None,
        **kwargs,
    ):
        """Run stock all-reduce while recording whether fault-tolerance masked a protocol error."""
        group_id = group_info.group_id if group_id is None else group_id
        runner = AllReduceRunner(
            p2p=self._p2p,
            servicer_type=type(self),
            prefix=self.prefix,
            tensors=tensors,
            group_id=group_id,
            ordered_peer_ids=group_info.peer_ids,
            **kwargs,
        )
        assert group_id in self._running_groups
        self._running_groups[group_id].set_result(runner)
        error = ""
        try:
            if runner.modes[group_info.peer_ids.index(self.peer_id)] != AveragingMode.AUX:
                async for tensor, update in azip(as_aiter(*tensors), runner):
                    tensor.add_(update, alpha=self._averaging_alpha)
                    self.last_updated = get_dht_time()
                    self._state_updated.set()
            else:
                async for _ in runner:
                    raise ValueError("aux peers should not receive averaged tensors")
        except BaseException as exc:
            error = type(exc).__name__
            raise
        finally:
            _record(
                "allreduce_diagnostics",
                self.peer_id,
                "",
                {
                    "banned_senders": len(runner.banned_senders),
                    "error": error,
                    "failed_elements": runner.tensor_part_container.failed_size,
                    "group_id": group_id.hex(),
                    "roster": [str(peer_id) for peer_id in group_info.peer_ids],
                    "self_index": group_info.peer_ids.index(self.peer_id),
                },
            )


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


def run_trial(
    arm: str,
    *,
    trial: int,
    run_id: str,
    node_ids: list[DHTID],
    step_timeout: float,
) -> tuple[dict, list[dict]]:
    global EVENTS
    EVENTS = mp.Queue()
    bootstrap = hivemind.DHT(start=True, host_maddrs=LOCALHOST, node_id=node_ids[0])
    initial_peers = bootstrap.get_visible_maddrs()
    dhts = [
        hivemind.DHT(
            initial_peers=initial_peers,
            start=True,
            host_maddrs=LOCALHOST,
            node_id=node_ids[index + 1],
        )
        for index in range(4)
    ]

    initial_tensor = torch.tensor([0.0, 1.0, 2.0, 4.0])
    tensors = [initial_tensor.clone() for _ in range(4)]
    common = dict(
        prefix=run_id,
        target_group_size=4,
        min_group_size=4,
        min_matchmaking_time=2.0,
        request_timeout=1.0,
        part_size_bytes=4,
        allreduce_timeout=5.0,
        next_chunk_timeout=1.0,
        start=True,
    )
    leader = RosterEquivocationAverager(
        [tensors[0]], dhts[0], equivocate=arm == "equivocate", **common
    )
    followers = [
        RosterEquivocationAverager([tensors[index]], dhts[index], **common)
        for index in range(1, 4)
    ]
    peers = [leader, *followers]
    leader_id = str(leader.peer_id)
    controls: dict[str, StepControl] = {}
    hard_cancellations: set[str] = set()
    try:
        time.sleep(2.0)
        base = get_dht_time()
        controls["leader"] = leader.step(
            gather={"role": "leader", "trial": trial},
            scheduled_time=base + 4.0,
            timeout=step_timeout,
            allow_retries=False,
            wait=False,
        )
        time.sleep(0.1)
        for index, follower in enumerate(followers, start=1):
            controls[f"follower_{index}"] = follower.step(
                gather={"role": f"follower_{index}", "trial": trial},
                scheduled_time=base + 6.0,
                timeout=step_timeout,
                allow_retries=False,
                wait=False,
            )

        hard_stop = time.perf_counter() + step_timeout + 5.0
        while time.perf_counter() < hard_stop and not all(control.done() for control in controls.values()):
            time.sleep(0.05)
        for name, control in controls.items():
            if not control.done():
                hard_cancellations.add(name)
                control.cancel()
        stage_leaks = _wait_for_finished(controls, timeout=10.0)
        if stage_leaks:
            raise RuntimeError("unfinished StepControls: " + ", ".join(stage_leaks))

        raw = _drain_until_quiet(EVENTS)
        events = [
            dict(
                arm=arm,
                trial=trial,
                kind=kind,
                peer=peer,
                other=other,
                detail=detail,
                emitted_at=f"{emitted_at:.6f}",
            )
            for kind, peer, other, detail, emitted_at in raw
        ]
        outcomes = {name: _outcome(control) for name, control in controls.items()}
        outputs = []
        for peer in peers:
            with peer.get_tensors() as current:
                outputs.append(current[0].detach().clone())
        correct = [bool(torch.allclose(output, initial_tensor, atol=1e-6, rtol=0)) for output in outputs]
        all_agree = all(torch.allclose(outputs[0], output, atol=1e-6, rtol=0) for output in outputs[1:])
        max_error = max(float(torch.max(torch.abs(output - initial_tensor))) for output in outputs)

        begin_events = [
            event
            for event in events
            if event["kind"] == "begin_allreduce_sent" and event["peer"] == leader_id
        ]
        diagnostics = [event for event in events if event["kind"] == "allreduce_diagnostics"]
        diagnostic_details = [json.loads(event["detail"]) for event in diagnostics]
        clean_allreduce = (
            len(diagnostics) == 4
            and all(not item["banned_senders"] for item in diagnostic_details)
            and all(not item["failed_elements"] for item in diagnostic_details)
            and all(not item["error"] for item in diagnostic_details)
        )
        all_completed = all(status == "completed" for status, _ in outcomes.values())
        silent_wrong = all_completed and clean_allreduce and not any(correct)
        summary = dict(
            arm=arm,
            trial=trial,
            leader_id=leader_id,
            begin_messages=len(begin_events),
            completed=sum(status == "completed" for status, _ in outcomes.values()),
            failed=sum(status != "completed" for status, _ in outcomes.values()),
            clean_allreduce=int(clean_allreduce),
            correct=sum(correct),
            all_agree=int(all_agree),
            silent_wrong=int(silent_wrong),
            max_abs_error=f"{max_error:.6f}",
            outputs=json.dumps([output.tolist() for output in outputs]),
            hard_cancellations=";".join(sorted(hard_cancellations)),
            stage_leaks=";".join(stage_leaks),
        )
        return summary, events
    finally:
        for control in controls.values():
            if not control.done():
                control.cancel()
        if controls:
            _wait_for_finished(controls, timeout=10.0)
        for peer in peers:
            peer.shutdown()
        for dht in [*dhts, bootstrap]:
            dht.shutdown()
        if EVENTS is not None:
            EVENTS.close()
        EVENTS = None


def validate(phase: str, summaries: list[dict], events: list[dict], trials: int) -> tuple[bool, list[str]]:
    problems = []
    for row in summaries:
        label = f"{row['arm']} trial {row['trial']}"
        if row["begin_messages"] != 3:
            problems.append(f"{label}: designated leader sent {row['begin_messages']}/3 BEGIN messages")
        if row["completed"] != 4:
            problems.append(f"{label}: only {row['completed']}/4 steps completed")
        if not row["clean_allreduce"]:
            problems.append(f"{label}: reducer error, ban, fallback, or missing diagnostics")
        if row["hard_cancellations"] or row["stage_leaks"]:
            problems.append(f"{label}: harness cancellation or cleanup leak")
        if row["arm"] == "control" and (row["correct"] != 4 or not row["all_agree"]):
            problems.append(f"{label}: control did not produce the correct common tensor")
        if row["arm"] == "equivocate" and not row["silent_wrong"]:
            problems.append(f"{label}: equivocation was not silent corruption")

    for arm in ARMS:
        arm_events = [
            json.loads(event["detail"])
            for event in events
            if event["arm"] == arm and event["kind"] == "begin_allreduce_sent"
        ]
        if arm == "control" and any(item["sent_roster"] != item["canonical_roster"] for item in arm_events):
            problems.append("control altered a BEGIN roster")
        if arm == "equivocate":
            if any(item["recipient_index"] != item["leader_index"] for item in arm_events):
                problems.append("equivocation did not assign every recipient the leader's reducer index")
            if any(sorted(item["sent_roster"]) != sorted(item["canonical_roster"]) for item in arm_events):
                problems.append("equivocation changed membership instead of only permuting it")

    if phase == "confirm":
        attacks = [row for row in summaries if row["arm"] == "equivocate"]
        if sum(row["silent_wrong"] for row in attacks) != trials:
            problems.append("not every confirmation attack trial produced silent corruption")
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
    parser.add_argument("--trials", type=int, default=None)
    parser.add_argument("--seed", type=int, default=1717)
    parser.add_argument("--step-timeout", type=float, default=10.0)
    parser.add_argument("--label", default="")
    args = parser.parse_args()
    trials = args.trials if args.trials is not None else (1 if args.phase == "smoke" else 5)
    if trials < 1 or args.step_timeout <= 6.0:
        parser.error("trials must be positive and step-timeout must exceed six seconds")

    label = args.label or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    print(f"Step 17 live roster equivocation {args.phase}: {trials} paired trial(s)")
    summaries = []
    events = []
    for trial in range(trials):
        arm_order = ARMS if trial % 2 == 0 else tuple(reversed(ARMS))
        for arm in arm_order:
            node_ids = [
                DHTID.generate(f"step17-{args.seed}-{trial}-{index}".encode()) for index in range(5)
            ]
            summary, trial_events = run_trial(
                arm,
                trial=trial,
                run_id=f"e17_{label}_{trial}",
                node_ids=node_ids,
                step_timeout=args.step_timeout,
            )
            summaries.append(summary)
            events.extend(trial_events)
            print(
                f"  {arm:<10} trial {trial}: completed={summary['completed']}/4 "
                f"correct={summary['correct']}/4 clean={summary['clean_allreduce']} "
                f"silent_wrong={summary['silent_wrong']} output={summary['outputs']}"
            )

    valid, problems = validate(args.phase, summaries, events, trials)
    RESULTS_DIR.mkdir(exist_ok=True)
    tag = f"{args.phase}_{label}_t{trials}_s{args.seed}"
    summary_path = RESULTS_DIR / f"step17_roster_live_rounds_{tag}.csv"
    events_path = RESULTS_DIR / f"step17_roster_live_events_{tag}.csv"
    write_csv(summary_path, summaries)
    write_csv(events_path, events)
    control_wrong = sum(row["silent_wrong"] for row in summaries if row["arm"] == "control")
    attack_wrong = sum(row["silent_wrong"] for row in summaries if row["arm"] == "equivocate")
    print(f" silent corruption: control {control_wrong}/{trials}, equivocate {attack_wrong}/{trials}")
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
