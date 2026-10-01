"""
Track 2, stage A -- smallest roster-equivocation mechanism test.

This test assumes that a malicious leader has already sent BEGIN_ALLREDUCE. It then runs Hivemind's
real ``AllReduceRunner`` RPC over localhost libp2p with two individually valid views of the same group:

* control: both peers use ``[leader, follower]``;
* equivocate: the leader uses ``[leader, follower]`` and the follower uses ``[follower, leader]``.

The position in ``ordered_peer_ids`` determines the reducer index. The attack therefore makes both
peers believe that they own index zero. Two one-float shards keep the shapes equal, allowing this test
to distinguish silent incorrect aggregation from a detected protocol failure.

This is deliberately not an election or DHT test. If the smoke gate passes, the next stage must inject
the conflicting rosters through real BEGIN_ALLREDUCE messages in ``DecentralizedAverager``.

    .venv/bin/python research/step16_roster_equivocation.py
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import torch

from hivemind.averaging.allreduce import AllReduceRunner
from hivemind.p2p import P2P
from hivemind.utils.asyncio import aenumerate

RESULTS_DIR = Path(__file__).resolve().parent / "results"
LOCALHOST = ["/ip4/127.0.0.1/tcp/0"]
ARMS = ("control", "equivocate")


async def _run_protocol(runner: AllReduceRunner) -> None:
    async for tensor_index, delta in aenumerate(runner):
        runner.tensor_part_container.local_tensors[tensor_index].add_(delta)


async def run_arm(arm: str, timeout: float, label: str) -> tuple[list[dict], dict]:
    p2ps = [await P2P.create(host_maddrs=LOCALHOST)]
    initial_peers = await p2ps[0].get_visible_maddrs()
    p2ps.append(await P2P.create(initial_peers=initial_peers, host_maddrs=LOCALHOST))

    peer_ids = tuple(p2p.peer_id for p2p in p2ps)
    leader_view = peer_ids
    follower_view = peer_ids if arm == "control" else tuple(reversed(peer_ids))
    views = (leader_view, follower_view)
    assert all(len(set(view)) == 2 and peer_ids[index] in view for index, view in enumerate(views))

    initial_tensors = (
        torch.tensor([0.0, 100.0]),
        torch.tensor([10.0, 1000.0]),
    )
    expected = (initial_tensors[0] + initial_tensors[1]) / 2
    group_id = hashlib.sha256(f"step16-{label}-{arm}".encode()).digest()[:20]
    namespace = f"step16_{label}_{arm}"

    runners = []
    try:
        for p2p, tensor, view in zip(p2ps, initial_tensors, views):
            runner = AllReduceRunner(
                p2p=p2p,
                servicer_type=AllReduceRunner,
                prefix=namespace,
                group_id=group_id,
                tensors=[tensor.clone()],
                weight=1.0,
                ordered_peer_ids=view,
                peer_fractions=(1.0, 1.0),
                part_size_bytes=4,
                sender_timeout=1.0,
                reducer_timeout=2.0,
            )
            await runner.add_p2p_handlers(p2p, namespace=namespace)
            runners.append(runner)

        tasks = [asyncio.create_task(_run_protocol(runner)) for runner in runners]
        try:
            outcomes = await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True), timeout=timeout
            )
            timed_out = False
        except asyncio.TimeoutError:
            timed_out = True
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            outcomes = [TimeoutError(f"exceeded {timeout:.1f}s") for _ in tasks]

        outputs = [runner.tensor_part_container.local_tensors[0].detach().clone() for runner in runners]
        completed = [outcome is None for outcome in outcomes]
        correct = [bool(torch.allclose(output, expected, atol=1e-6, rtol=0)) for output in outputs]
        banned_counts = [len(runner.banned_senders) for runner in runners]
        failed_elements = [runner.tensor_part_container.failed_size for runner in runners]
        peers_agree = bool(torch.allclose(outputs[0], outputs[1], atol=1e-6, rtol=0))
        clean_transport = (
            not timed_out and all(completed) and not any(banned_counts) and not any(failed_elements)
        )
        silent_wrong = clean_transport and not any(correct)

        rows = []
        for peer_index, (peer_id, view, initial, output, outcome) in enumerate(
            zip(peer_ids, views, initial_tensors, outputs, outcomes)
        ):
            rows.append(
                dict(
                    arm=arm,
                    peer_index=peer_index,
                    peer_id=str(peer_id),
                    roster_indices=";".join(str(peer_ids.index(item)) for item in view),
                    self_roster_index=view.index(peer_id),
                    initial=json.dumps(initial.tolist()),
                    expected=json.dumps(expected.tolist()),
                    output=json.dumps(output.tolist()),
                    completed=int(completed[peer_index]),
                    correct=int(correct[peer_index]),
                    banned_senders=banned_counts[peer_index],
                    failed_elements=failed_elements[peer_index],
                    error="" if outcome is None else type(outcome).__name__,
                )
            )

        summary = dict(
            arm=arm,
            timed_out=int(timed_out),
            completed=sum(completed),
            correct=sum(correct),
            banned_senders=sum(banned_counts),
            failed_elements=sum(failed_elements),
            peers_agree=int(peers_agree),
            silent_wrong=int(silent_wrong),
        )
        return rows, summary
    finally:
        for runner, p2p in zip(runners, p2ps):
            runner.finalize(cancel=True)
            await runner.remove_p2p_handlers(p2p, namespace=namespace)
        for p2p in p2ps:
            await p2p.shutdown()


async def run_experiment(timeout: float, label: str) -> tuple[list[dict], list[dict]]:
    rows = []
    summaries = []
    for arm in ARMS:
        arm_rows, summary = await run_arm(arm, timeout, label)
        rows.extend(arm_rows)
        summaries.append(summary)
        print(
            f"  {arm:<10} completed={summary['completed']}/2 "
            f"correct={summary['correct']}/2 agree={summary['peers_agree']} "
            f"bans={summary['banned_senders']} failed={summary['failed_elements']} "
            f"silent_wrong={summary['silent_wrong']}"
        )
        for row in arm_rows:
            print(f"    peer {row['peer_index']}: roster={row['roster_indices']} output={row['output']}")
    return rows, summaries


def write_csv(path: Path, rows: list[dict]) -> None:
    with path.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--timeout", type=float, default=5.0)
    parser.add_argument("--label", default="")
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("timeout must be positive")

    label = args.label or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    print("Step 16 roster-equivocation smoke: two peers, two equal shards")
    rows, summaries = asyncio.run(run_experiment(args.timeout, label))

    control = next(summary for summary in summaries if summary["arm"] == "control")
    attack = next(summary for summary in summaries if summary["arm"] == "equivocate")
    problems = []
    if (
        control["completed"] != 2
        or control["correct"] != 2
        or not control["peers_agree"]
        or control["banned_senders"]
        or control["failed_elements"]
    ):
        problems.append("control did not complete with the correct common average")
    if attack["completed"] != 2:
        problems.append("equivocation was detected, failed, or timed out")
    if not attack["silent_wrong"]:
        problems.append("equivocation did not silently produce incorrect outputs for both peers")
    if attack["peers_agree"]:
        problems.append("equivocation did not make the peer outputs diverge")

    RESULTS_DIR.mkdir(exist_ok=True)
    output_path = RESULTS_DIR / f"step16_roster_smoke_{label}.csv"
    write_csv(output_path, rows)
    print(f" wrote {output_path.relative_to(Path.cwd())}")
    if problems:
        print(" validation: FAIL", file=sys.stderr)
        for problem in problems:
            print(f"  - {problem}", file=sys.stderr)
        return 2
    print(" validation: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
