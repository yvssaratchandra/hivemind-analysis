"""Rerun adaptive Hivemind leadership with measured per-link WAN delays.

This reuses step15's real DHT, matchmaking and all-reduce, but adds DHT-only
peers so the network exceeds the default k=20. Each node is assigned one of
five RIPE Atlas anchor regions. Incoming DHT find/store RPCs and averaging
join streams are delayed according to that pair's measured RTT. This is a
local application-level latency emulation, not a public-WAN deployment.

Small gate: .venv/bin/python research/step19_wan_delay_leader.py --phase smoke
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import math
import multiprocessing as mp
import statistics
import sys
from datetime import datetime, timezone
from functools import partial
from pathlib import Path
from typing import AsyncIterator, Optional

import step15_adaptive_leader_live as baseline

from hivemind.dht.protocol import DHTProtocol
from hivemind.dht.routing import DHTID
from hivemind.p2p import P2PContext
from hivemind.proto import averaging_pb2, dht_pb2

RESULTS = Path(__file__).resolve().parent / "results"
DEFAULT_PROFILE = RESULTS / "ripe_anchor_rtt_20260930.json"
REGIONS = ("SE", "US", "BR", "IN", "AU")

REGION_BY_ID: dict[int, str] = {}
PEER_REGIONS = None
RTT_MS: dict[tuple[str, str], float] = {}
SCALE = 0.0
TARGET_KEY_ID: Optional[DHTID] = None

_ORIGINAL_FIND = DHTProtocol.rpc_find
_ORIGINAL_STORE = DHTProtocol.rpc_store
_ORIGINAL_JOIN = baseline.AdaptiveLeaderAverager.rpc_join_group


def load_profile(path: Path) -> tuple[dict[tuple[str, str], float], dict]:
    data = json.loads(path.read_text())
    profile = {}
    for edge in data["edges"]:
        if edge["source"] in REGIONS and edge["destination"] in REGIONS:
            profile[(edge["source"], edge["destination"])] = statistics.median(edge["rtt_ms"])
    for item in data["same_region"]:
        if item["country"] in REGIONS:
            profile[(item["country"], item["country"])] = statistics.median(item["rtt_ms"])
    expected = {(a, b) for a in REGIONS for b in REGIONS}
    missing = expected - set(profile)
    if missing:
        raise ValueError(f"RTT profile missing pairs: {sorted(missing)}")
    if any(value <= 0 or not math.isfinite(value) for value in profile.values()):
        raise ValueError("RTT profile contains a nonpositive or nonfinite value")
    return profile, data


def one_way_seconds(source: str, destination: str) -> float:
    if not source or not destination:
        return 0.0
    return SCALE * RTT_MS[(source, destination)] / 2000.0


async def _delayed_find(
    self: DHTProtocol, request: dht_pb2.FindRequest, context: P2PContext
) -> dht_pb2.FindResponse:
    source_id = DHTID.from_bytes(request.peer.node_id) if request.peer.node_id else None
    source = REGION_BY_ID.get(int(source_id)) if source_id is not None else None
    destination = REGION_BY_ID.get(int(self.node_id))
    one_way = one_way_seconds(source, destination)
    if one_way:
        await asyncio.sleep(one_way)
    response = await _ORIGINAL_FIND(self, request, context)
    if one_way:
        await asyncio.sleep(one_way)
    if (
        TARGET_KEY_ID is not None
        and baseline.CURRENT_ROUND is not None
        and baseline.CURRENT_ROUND.value >= 0
        and TARGET_KEY_ID.to_bytes() in request.keys
    ):
        baseline._record(
            "wan_target_rpc_find",
            self.node_id,
            source_id,
            {"source_region": source, "destination_region": destination, "rtt_ms": one_way * 2000},
        )
    return response


async def _delayed_store(
    self: DHTProtocol, request: dht_pb2.StoreRequest, context: P2PContext
) -> dht_pb2.StoreResponse:
    source_id = DHTID.from_bytes(request.peer.node_id) if request.peer.node_id else None
    source = REGION_BY_ID.get(int(source_id)) if source_id is not None else None
    destination = REGION_BY_ID.get(int(self.node_id))
    one_way = one_way_seconds(source, destination)
    if one_way:
        await asyncio.sleep(one_way)
    response = await _ORIGINAL_STORE(self, request, context)
    if one_way:
        await asyncio.sleep(one_way)
    if (
        TARGET_KEY_ID is not None
        and baseline.CURRENT_ROUND is not None
        and baseline.CURRENT_ROUND.value >= 0
        and TARGET_KEY_ID.to_bytes() in request.keys
    ):
        baseline._record(
            "wan_target_rpc_store",
            self.node_id,
            source_id,
            {"source_region": source, "destination_region": destination, "rtt_ms": one_way * 2000},
        )
    return response


async def _delayed_join(
    self: baseline.AdaptiveLeaderAverager,
    request: averaging_pb2.JoinRequest,
    context: P2PContext,
) -> AsyncIterator[averaging_pb2.MessageFromLeader]:
    source = PEER_REGIONS.get(str(context.remote_id)) if PEER_REGIONS is not None else None
    destination = PEER_REGIONS.get(str(self.peer_id)) if PEER_REGIONS is not None else None
    one_way = one_way_seconds(source, destination)
    if one_way:
        await asyncio.sleep(one_way)
    async for response in _ORIGINAL_JOIN(self, request, context):
        if one_way:
            await asyncio.sleep(one_way)
        yield response


# Install before DHT/averager child processes register RPC handlers.
DHTProtocol.rpc_find = _delayed_find
DHTProtocol.rpc_store = _delayed_store
baseline.AdaptiveLeaderAverager.rpc_join_group = _delayed_join


def write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


async def _routing_seed(_dht, node, *, key_id: DHTID) -> dict:
    table = node.protocol.routing_table
    active = {node_id for bucket in table.buckets for node_id in bucket.nodes_to_peer_id}
    return {
        "active_table_size": len(active),
        "initial_responders": [
            str(node_id)
            for node_id, _ in table.get_nearest_neighbors(key_id, table.bucket_size, exclude=node.node_id)
        ],
    }


def run_trial(
    *,
    arm: str,
    scale: float,
    round_id: int,
    seed: int,
    fillers: int,
    label: str,
    schedule_min: float,
    schedule_max: float,
) -> tuple[dict, list[dict]]:
    global REGION_BY_ID, SCALE, TARGET_KEY_ID
    SCALE = scale
    trial_seed = seed + round_id
    node_ids = [
        DHTID.generate(f"step19-{seed}-{round_id}-{index}".encode())
        for index in range(5 + fillers)
    ]
    REGION_BY_ID = {int(node_id): REGIONS[(index + round_id) % len(REGIONS)] for index, node_id in enumerate(node_ids)}
    # Fresh DHT processes isolate arms, so reuse the exact key across delay
    # conditions; key-dependent routing must not confound the comparison.
    run_id = f"e19_{label}_{round_id}"
    TARGET_KEY_ID = DHTID.generate(source=f"{run_id}.0b")
    PEER_REGIONS.clear()

    seed_snapshot: dict = {}

    def register_peers(peers: list[baseline.AdaptiveLeaderAverager], dht_nodes: list) -> None:
        for index, peer in enumerate(peers, start=1):
            PEER_REGIONS[str(peer.peer_id)] = REGION_BY_ID[int(node_ids[index])]
        seed_snapshot.update(dht_nodes[1].run_coroutine(partial(_routing_seed, key_id=TARGET_KEY_ID)))

    rows, events = baseline.run_arm(
        arm,
        honest_count=3,
        group_size=4,
        rounds=1,
        seed=trial_seed,
        request_timeout=2.5,
        safety_margin=1.0,
        observation_window=0.3,
        schedule_min=schedule_min,
        schedule_max=schedule_max,
        step_timeout=28.0,
        jitter=0.3,
        run_id=run_id,
        node_ids=node_ids,
        round_offset=round_id,
        filler_count=fillers,
        on_averagers_ready=register_peers,
    )
    if len(rows) != 1:
        raise RuntimeError(f"expected one round, got {len(rows)}")
    row = rows[0]
    row.update(
        delay_scale=scale,
        fillers=fillers,
        profile="ripe_anchor_rtt_20260930.json",
        schedule_min_s=schedule_min,
        schedule_max_s=schedule_max,
    )
    current = [event for event in events if event["round"] == round_id]
    row["target_rpc_find"] = sum(event["kind"] == "wan_target_rpc_find" for event in current)
    row["target_rpc_store"] = sum(event["kind"] == "wan_target_rpc_store" for event in current)
    attacker_find = [
        event
        for event in current
        if event["kind"] == "wan_target_rpc_find" and event["other"] == str(node_ids[1])
    ]
    row["attacker_rpc_find"] = len(attacker_find)
    responders = {event["peer"] for event in attacker_find}
    row["attacker_find_responders"] = len(responders)
    row["attacker_initial_seed"] = len(seed_snapshot["initial_responders"])
    row["attacker_active_table"] = seed_snapshot["active_table_size"]
    row["responders_outside_seed"] = len(responders - set(seed_snapshot["initial_responders"]))
    reads = [
        json.loads(event["detail"])
        for event in current
        if event["kind"] == "adaptive_read" and event["peer"] == row["attacker_id"]
    ]
    publications = [
        json.loads(event["detail"])
        for event in current
        if event["kind"] == "adaptive_publication" and event["peer"] == row["attacker_id"]
    ]
    row["median_attacker_read_s"] = (
        f"{statistics.median(item['read_latency_s'] for item in reads):.4f}" if reads else ""
    )
    row["max_attacker_write_s"] = (
        f"{max(item['write_latency_s'] for item in publications):.4f}" if publications else ""
    )
    for event in events:
        event["delay_scale"] = scale
        event["fillers"] = fillers
    return row, events


def main() -> int:
    global PEER_REGIONS, RTT_MS
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=("smoke", "confirm"), default="smoke")
    parser.add_argument("--profile", type=Path, default=DEFAULT_PROFILE)
    parser.add_argument("--fillers", type=int, default=23)
    parser.add_argument("--rounds", type=int, default=None)
    parser.add_argument("--seed", type=int, default=1919)
    parser.add_argument("--schedule-min", type=float, default=11.0)
    parser.add_argument("--schedule-max", type=float, default=15.0)
    parser.add_argument("--label", default="")
    parser.add_argument("--scales", nargs="+", type=float, default=[0.0, 1.0])
    parser.add_argument("--arms", nargs="+", choices=baseline.ARMS, default=list(baseline.ARMS))
    args = parser.parse_args()
    rounds = args.rounds if args.rounds is not None else (1 if args.phase == "smoke" else 3)
    if args.fillers < 17 or rounds < 1:
        parser.error("need at least 17 DHT-only fillers, making 22 total DHT peers (> default k=20)")
    if not 3.5 < args.schedule_min < args.schedule_max < 28.0:
        parser.error("require 3.5 < schedule-min < schedule-max < step-timeout (28s)")
    if any(scale < 0 or not math.isfinite(scale) for scale in args.scales):
        parser.error("delay scales must be finite and nonnegative")
    if len(set(args.scales)) != len(args.scales) or len(set(args.arms)) != len(args.arms):
        parser.error("scales and arms must not contain duplicates")

    RTT_MS, data = load_profile(args.profile)
    cross = [RTT_MS[(a, b)] for a in REGIONS for b in REGIONS if a != b]
    print(
        f"RIPE profile collected {data['collected_at_utc']}; selected {REGIONS}; "
        f"cross-region median RTT {statistics.median(cross):.1f} ms "
        f"(range {min(cross):.1f}-{max(cross):.1f} ms)",
        flush=True,
    )
    label = args.label or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    rows: list[dict] = []
    events: list[dict] = []
    with mp.Manager() as manager:
        PEER_REGIONS = manager.dict()
        for round_id in range(rounds):
            combinations = [(scale, arm) for scale in args.scales for arm in args.arms]
            if round_id % 2:
                combinations.reverse()
            for scale, arm in combinations:
                print(f"starting round={round_id} scale={scale} arm={arm}", flush=True)
                row, arm_events = run_trial(
                    arm=arm,
                    scale=scale,
                    round_id=round_id,
                    seed=args.seed,
                    fillers=args.fillers,
                    label=label,
                    schedule_min=args.schedule_min,
                    schedule_max=args.schedule_max,
                )
                rows.append(row)
                events.extend(arm_events)
                print(
                    f"  capture={row['full_capture']} followers={row['follower_count']} "
                    f"reads={row['adaptive_reads']} median_read={row['median_attacker_read_s']}s "
                    f"writes={row['publication_attempts']} max_write={row['max_attacker_write_s']}s "
                    f"find_rpc={row['target_rpc_find']} store_rpc={row['target_rpc_store']} "
                    f"attacker_responders={row['attacker_find_responders']} "
                    f"outside_seed={row['responders_outside_seed']} "
                    f"honest={row['honest_completed']}/3",
                    flush=True,
                )
        PEER_REGIONS = None

    RESULTS.mkdir(exist_ok=True)
    tag = f"{args.phase}_{label}_f{args.fillers}_r{rounds}_s{args.seed}"
    summary_path = RESULTS / f"step19_wan_delay_rounds_{tag}.csv"
    events_path = RESULTS / f"step19_wan_delay_events_{tag}.csv"
    write_csv(summary_path, rows)
    write_csv(events_path, events)
    print(f"wrote {summary_path}", flush=True)
    print(f"wrote {events_path}", flush=True)

    problems = []
    for row in rows:
        if row["honest_completed"] != 3 or row["hard_cancellations"] or row["stage_leaks"]:
            problems.append(f"{row['delay_scale']} {row['arm']} round {row['round']}: incomplete or leaked")
        if row["adaptive_reads"] < 1:
            problems.append(f"{row['delay_scale']} {row['arm']} round {row['round']}: no adaptive read")
        if row["forbidden_outbound_joins"]:
            problems.append(f"{row['delay_scale']} {row['arm']} round {row['round']}: attacker joined")
        if row["delay_scale"] > 0 and row["target_rpc_find"] == 0:
            problems.append(f"{row['delay_scale']} {row['arm']} round {row['round']}: no delayed key lookup")
    if problems:
        for problem in problems:
            print(f"WARNING: {problem}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    mp.set_start_method("fork", force=True)
    raise SystemExit(main())
