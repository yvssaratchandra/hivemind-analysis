"""Private eight-peer experiment for repeated own-ad renewal and passive leadership.

No library files are edited. Honest peers use stock deadlines, joins, and admission.
Attacker changes are explicit: no following, configurable own-ad publisher, and minimum size.
Run with a working Hivemind environment. Each output label must be new.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import multiprocessing as mp
import os
import platform
import queue
import random
import statistics
import subprocess
import time
from pathlib import Path
from typing import AsyncIterator

import torch

import hivemind
from hivemind.averaging import DecentralizedAverager
from hivemind.averaging.control import AveragingStage
from hivemind.averaging.key_manager import GroupKeyManager
from hivemind.averaging.matchmaking import Matchmaking, PotentialLeaders
from hivemind.dht.protocol import DHTProtocol
from hivemind.dht.routing import DHTID
from hivemind.p2p import P2PContext, PeerID
from hivemind.proto import averaging_pb2, dht_pb2
from hivemind.utils.timed_storage import get_dht_time


EVENTS = None
ATTACKER = False
CASE = {}
STORE_DELAY = 0.0
LOCALHOST = ["/ip4/127.0.0.1/tcp/0"]
REPO = Path(__file__).resolve().parents[2]


def record(kind, peer="", other="", **detail):
    if EVENTS is not None:
        EVENTS.put(dict(kind=kind, peer=str(peer), other=str(other),
                        wall_time=get_dht_time(), monotonic=time.monotonic(), detail=detail))


ORIGINAL_DECLARE = PotentialLeaders._declare_averager_periodically
ORIGINAL_UPDATE = PotentialLeaders._update_queue_periodically
ORIGINAL_REQUEST = Matchmaking._request_join_potential_leaders
ORIGINAL_JOIN = Matchmaking._request_join_group
ORIGINAL_AD = GroupKeyManager.declare_averager
ORIGINAL_READ = GroupKeyManager.get_averagers
ORIGINAL_STORE = DHTProtocol.rpc_store


async def logged_ad(self, group_key, peer_id, expiration_time, looking_for_group=True):
    started = time.monotonic()
    record("ad_started", peer_id, group_key, expiration=expiration_time,
           remaining=expiration_time-get_dht_time(), active=looking_for_group)
    try:
        accepted = await ORIGINAL_AD(self, group_key, peer_id, expiration_time, looking_for_group)
        record("ad_finished", peer_id, group_key, expiration=expiration_time,
               remaining=expiration_time-get_dht_time(), active=looking_for_group,
               accepted=bool(accepted), latency=time.monotonic()-started)
        return accepted
    except asyncio.CancelledError:
        record("ad_cancelled", peer_id, group_key, expiration=expiration_time,
               active=looking_for_group, latency=time.monotonic()-started)
        raise


async def logged_read(self, group_key, only_active):
    if ATTACKER and CASE.get("mode") != "stock":
        record("forbidden_attacker_read", self.peer_id)
        raise AssertionError("passive attacker read competitor advertisements")
    started = time.monotonic()
    result = await ORIGINAL_READ(self, group_key, only_active)
    record("ad_view", self.peer_id, group_key, latency=time.monotonic()-started,
           entries=[dict(peer=str(peer), expiration=expiration) for peer, expiration in result])
    return result


async def publisher(self, step, key_manager):
    if not ATTACKER or CASE.get("mode") in ("stock", "passive"):
        return await ORIGINAL_DECLARE(self, step, key_manager)
    period = 1.0 / CASE["frequency"] if CASE["mode"] == "refresh" else None
    lifetime = CASE["lifetime"]
    previous = float("-inf")
    async with self.lock_declare:
        try:
            while get_dht_time() < self.search_end_time:
                await self.running.wait()
                now = get_dht_time()
                expiration = min(max(math.nextafter(previous, math.inf), now+lifetime),
                                 self.search_end_time)
                if expiration <= now or expiration <= previous:
                    return
                if expiration-now > 5.0+1e-6:
                    record("lifetime_cap", self.peer_id, remaining=expiration-now)
                    return
                self.declared_group_key = group_key = key_manager.current_key
                self.declared_expiration_time = previous = expiration
                self.declared_expiration.set()
                sent = time.monotonic()
                await key_manager.declare_averager(group_key, self.peer_id, expiration)
                # Frequency is a requested send cadence. Slow writes serialize it and are logged.
                delay = expiration-get_dht_time() if period is None else period-(time.monotonic()-sent)
                await asyncio.sleep(max(0.0, delay))
        finally:
            if self.declared_group_key is not None:
                group_key, expiration = self.declared_group_key, self.declared_expiration_time
                self.declared_group_key, self.declared_expiration_time = None, float("inf")
                await key_manager.declare_averager(group_key, self.peer_id, expiration,
                                                   looking_for_group=False)


async def update_queue(self, key_manager):
    if not ATTACKER or CASE.get("mode") == "stock":
        return await ORIGINAL_UPDATE(self, key_manager)
    record("passive_queue", self.peer_id)
    await asyncio.Future()


async def request_potential(self, step):
    if not ATTACKER or CASE.get("mode") == "stock":
        return await ORIGINAL_REQUEST(self, step)
    async with self.potential_leaders.begin_search(step, self.group_key_manager,
                                                  declare=not self.client_mode):
        record("passive_wait", self.peer_id)
        return await asyncio.shield(self.assembled_group)


async def request_join(self, leader):
    if ATTACKER and CASE.get("mode") != "stock":
        record("forbidden_attacker_join", self.peer_id, leader)
        raise AssertionError("passive attacker attempted an outbound join")
    record("outbound_join", self.peer_id, leader)
    return await ORIGINAL_JOIN(self, leader)


async def logged_store(self, request: dht_pb2.StoreRequest, context: P2PContext) -> dht_pb2.StoreResponse:
    if STORE_DELAY:
        await asyncio.sleep(STORE_DELAY)
    received = get_dht_time()
    response = await ORIGINAL_STORE(self, request, context)
    for key, subkey, value, expiration, accepted in zip(
            request.keys, request.subkeys, request.values, request.expiration_time, response.store_ok):
        if subkey in (self.IS_DICTIONARY, self.IS_REGULAR_VALUE):
            continue
        try:
            subkey_value = self.serializer.loads(subkey)
            active = self.serializer.loads(value)
            if isinstance(subkey_value, bytes) and isinstance(active, bool):
                record("holder_store", self.p2p.peer_id, context.remote_id,
                       subkey=str(PeerID(subkey_value)), key=bytes(key).hex(), active=active,
                       expiration=expiration, received=received, accepted=bool(accepted))
        except (ValueError, TypeError):
            continue
    return response


GroupKeyManager.declare_averager = logged_ad
GroupKeyManager.get_averagers = logged_read
PotentialLeaders._declare_averager_periodically = publisher
PotentialLeaders._update_queue_periodically = update_queue
Matchmaking._request_join_potential_leaders = request_potential
Matchmaking._request_join_group = request_join
DHTProtocol.rpc_store = logged_store


class RefreshAverager(DecentralizedAverager):
    async def rpc_join_group(self, request: averaging_pb2.JoinRequest,
                             context: P2PContext) -> AsyncIterator[averaging_pb2.MessageFromLeader]:
        record("join_received", self.peer_id, context.remote_id, expiration=request.expiration)
        async for response in super().rpc_join_group(request, context):
            record("join_reply", self.peer_id, context.remote_id,
                   code=averaging_pb2.MessageCode.Name(response.code),
                   group_id=response.group_id.hex(),
                   roster=[str(PeerID(peer)) for peer in response.ordered_peer_ids])
            yield response


def drain(events, quiet=0.04, cap=0.4):
    rows = []
    stop = time.monotonic()+cap
    until = time.monotonic()+quiet
    while time.monotonic() < min(until, stop):
        try:
            rows.append(events.get(timeout=min(0.02, max(0.0001, until-time.monotonic()))))
            until = time.monotonic()+quiet
        except queue.Empty:
            pass
    return rows


def make_dhts(seed):
    dhts = []
    try:
        for index in range(8):
            dhts.append(hivemind.DHT(
                initial_peers=dhts[0].get_visible_maddrs() if dhts else None,
                host_maddrs=LOCALHOST, node_id=DHTID.generate(f"s22:{seed}:{index}"),
                start=True))
        return dhts
    except BaseException:
        for node in reversed(dhts):
            node.shutdown()
        raise


def launch_offsets(seed, pattern):
    rng = random.Random(seed)
    if pattern == "staggered":
        honest = [rng.uniform(0.0, 3.0) for _ in range(7)]
    elif pattern == "bursty":
        honest = [rng.uniform(0.0, 0.3) if rng.random() < 0.7 else rng.uniform(1.5, 3.0)
                  for _ in range(7)]
    else:
        raise ValueError(pattern)
    # X does not know the schedule; its own start is also paired and independently staggered.
    return {"X": rng.uniform(0.0, 3.0),
            **{f"H{i}": value for i, value in enumerate(honest)}}


def provenance():
    source = Path(hivemind.__file__).resolve().parent
    files = ("dht/node.py", "dht/protocol.py", "dht/routing.py", "dht/storage.py",
             "averaging/averager.py", "averaging/matchmaking.py", "averaging/key_manager.py",
             "utils/timed_storage.py")
    hashes = {}
    for relative in files:
        runtime = (source/relative).read_bytes()
        repository = (REPO/"hivemind"/relative).read_bytes()
        if runtime != repository:
            raise RuntimeError(f"runtime source differs: {relative}")
        hashes[relative] = hashlib.sha256(runtime).hexdigest()
    return dict(python=platform.python_version(), torch=torch.__version__, source=str(source),
                commit=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip(),
                source_sha256=hashes, script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                p2pd_sha256=hashlib.sha256((source/"hivemind_cli/p2pd").read_bytes()).hexdigest())


def outcome(control):
    if not control.done():
        return {"status": "unfinished"}
    if control.cancelled():
        return {"status": "cancelled"}
    try:
        result = control.result()
        return dict(status="completed" if isinstance(result, dict) else "no_group",
                    roster=[str(peer) for peer in result] if isinstance(result, dict) else [])
    except BaseException as exc:
        return dict(status="failed", error=type(exc).__name__)


def run_trial(case, seed, pattern, path, timeout=12.0):
    global EVENTS, ATTACKER, CASE
    path.mkdir()
    offsets = launch_offsets(seed, pattern)
    config = dict(case=case, seed=seed, pattern=pattern, launch_offsets=offsets,
                  participants=8, honest_min=2, target=case.get("target", 4), honest_ad_lifetime=5,
                  request_timeout=3, step_timeout=timeout, store_delay=STORE_DELAY)
    config["allreduce_timeout"] = 6.0
    (path/"config.json").write_text(json.dumps(config, indent=2)+"\n")
    EVENTS = mp.Queue()
    CASE = case
    ATTACKER = False
    dhts, peers, controls, raw = [], {}, {}, []
    raw_file = (path/"events.jsonl").open("w")

    def collect(quiet=0.003, cap=0.02):
        rows = drain(EVENTS, quiet=quiet, cap=cap)
        raw.extend(rows)
        for row in rows:
            raw_file.write(json.dumps(row, sort_keys=True)+"\n")
        raw_file.flush()

    try:
        dhts = make_dhts(seed)
        prefix = f"s22_{seed}_{pattern}"
        common = dict(prefix=prefix, target_group_size=case.get("target", 4), min_matchmaking_time=5.0,
                      request_timeout=3.0, allreduce_timeout=6.0, allow_state_sharing=False, start=True)
        ATTACKER = True
        peers["X"] = RefreshAverager([torch.full((4,), 7.0)], dhts[7],
                                     min_group_size=case.get("minimum", 4), **common)
        ATTACKER = False
        for i in range(7):
            peers[f"H{i}"] = RefreshAverager([torch.full((4,), float(i))], dhts[i],
                                             min_group_size=2, **common)
        identities = {name: str(peer.peer_id) for name, peer in peers.items()}
        config["peer_ids"] = identities
        config["node_ids"] = [str(int(DHTID.generate(f"s22:{seed}:{i}"))) for i in range(8)]
        (path/"config.json").write_text(json.dumps(config, indent=2)+"\n")
        time.sleep(0.5)
        collect(quiet=0.04, cap=0.3)
        started = time.monotonic()
        for name in sorted(peers, key=offsets.get):
            while time.monotonic() < started+offsets[name]:
                collect()
                time.sleep(min(0.01, max(0, started+offsets[name]-time.monotonic())))
            record("step_start", identities[name], planned_offset=offsets[name],
                   actual_offset=time.monotonic()-started, name=name)
            controls[name] = peers[name].step(gather={"name": name}, wait=False,
                                             allow_retries=True, timeout=timeout)
        expected_cancel = False
        hard_stop = started+max(offsets.values())+timeout+3
        while time.monotonic() < hard_stop:
            collect()
            if all(control.done() for control in controls.values()):
                break
            if case["mode"] != "stock" and all(controls[f"H{i}"].done() for i in range(7)):
                if not controls["X"].done():
                    controls["X"].cancel()
                    expected_cancel = True
            time.sleep(0.01)
        hard_cancelled = [name for name, control in controls.items() if not control.done()]
        for name in hard_cancelled:
            controls[name].cancel()
        cleanup_stop = time.monotonic()+8
        while time.monotonic() < cleanup_stop:
            collect()
            if all(control.stage == AveragingStage.FINISHED for control in controls.values()):
                break
            time.sleep(0.02)
        leaks = [name for name, control in controls.items() if control.stage != AveragingStage.FINISHED]
        collect(quiet=0.1, cap=0.5)
        outcomes = {name: outcome(control) for name, control in controls.items()}
        tensor_errors, tensor_checks = [], {}
        initial = {identities[name]: float(7 if name == "X" else int(name[1:])) for name in peers}
        for name, result in outcomes.items():
            if result["status"] == "completed":
                expected = statistics.mean(initial[peer] for peer in result["roster"])
                with peers[name].get_tensors() as tensors:
                    correct = torch.allclose(tensors[0], torch.full_like(tensors[0], expected), atol=1e-5)
                    tensor_checks[name] = dict(expected=expected, actual=tensors[0].tolist(), correct=correct)
                    if not correct:
                        tensor_errors.append(name)
        x_id = identities["X"]
        begins = [event for event in raw if event["kind"] == "join_reply" and event["peer"] == x_id
                  and event["detail"]["code"] == "BEGIN_ALLREDUCE"]
        x_groups = {event["detail"]["group_id"]: event["detail"]["roster"] for event in begins}
        completed_lead = outcomes["X"]["status"] == "completed" and bool(x_groups)
        x_roster_names = {name for name, peer in identities.items()
                          if peer in outcomes["X"].get("roster", [])}
        verified_lead = completed_lead and all(tensor_checks.get(name, {}).get("correct", False)
                                               for name in x_roster_names)
        honest_completed = sum(outcomes[f"H{i}"]["status"] == "completed" for i in range(7))
        advertisements = [event for event in raw if event["kind"] == "ad_started" and event["peer"] == x_id
                          and event["detail"]["active"]]
        forbidden = [event for event in raw if event["kind"].startswith("forbidden_")]
        result = dict(case=case["name"], seed=seed, pattern=pattern,
                      x_completed_lead=bool(completed_lead), x_groups=x_groups,
                      x_verified_lead=bool(verified_lead), tensor_checks=tensor_checks,
                      x_group_sizes=[len(roster) for roster in x_groups.values()],
                      x_outcome=outcomes["X"], outcomes=outcomes,
                      honest_completed=honest_completed, x_ad_attempts=len(advertisements),
                      elapsed=time.monotonic()-started, expected_unused_x_cancel=expected_cancel,
                      hard_cancelled=hard_cancelled, stage_leaks=leaks,
                      tensor_errors=tensor_errors, forbidden=len(forbidden))
        (path/"summary.json").write_text(json.dumps(result, indent=2)+"\n")
        print(f"{case['name']:20} {pattern:10} s{seed}: lead={int(completed_lead)} "
              f"size={result['x_group_sizes']} honest={honest_completed}/7 ads={len(advertisements)} "
              f"elapsed={result['elapsed']:.2f}s", flush=True)
        if forbidden:
            raise RuntimeError(f"invalid trial: {result}")
        return result
    finally:
        for control in controls.values():
            if not control.done():
                control.cancel()
        for peer in peers.values():
            collect()
            peer.shutdown()
        for dht in reversed(dhts):
            collect()
            dht.shutdown()
        collect(quiet=0.08, cap=0.5)
        raw_file.close()
        EVENTS.close()
        EVENTS = None
        ATTACKER = False
        CASE = {}


def calibrate(path, seed=2200):
    global EVENTS
    EVENTS = mp.Queue()
    dhts = []
    try:
        dhts = make_dhts(seed)
        time.sleep(0.5)
        caller = dhts[7]
        key = f"s22_calibration_{seed}.0b"
        samples = []
        for index in range(20):
            before = get_dht_time()
            mono = time.monotonic()
            accepted = caller.store(key, True, subkey=caller.peer_id.to_bytes(),
                                    expiration_time=before+5)
            samples.append(dict(index=index, before=before, latency=time.monotonic()-mono,
                                accepted=bool(accepted), expiration=before+5))
        raw = drain(EVENTS, quiet=0.1, cap=1)
        receipts = []
        for event in raw:
            if event["kind"] == "holder_store" and event["detail"]["subkey"] == str(caller.peer_id):
                before = event["detail"]["expiration"]-5
                receipts.append(event["detail"]["received"]-before)
        reads = []
        for _ in range(10):
            before = time.monotonic()
            dhts[0].get(key, latest=True)
            reads.append(time.monotonic()-before)
        ordered = sorted(receipts)
        p95 = ordered[min(len(ordered)-1, math.ceil(len(ordered)*0.95)-1)]
        result = dict(samples=samples, receipt_latencies=receipts, read_latencies=reads,
                      receipt_median=statistics.median(receipts), receipt_p95=p95,
                      receipt_max=max(receipts), store_median=statistics.median(s["latency"] for s in samples),
                      constant_guard=p95+max(0.02, 0.25*p95), store_delay=STORE_DELAY)
        (path/"calibration.json").write_text(json.dumps(result, indent=2)+"\n")
        (path/"events.jsonl").write_text("".join(json.dumps(row)+"\n" for row in raw))
        print(json.dumps({key: value for key, value in result.items() if not isinstance(value, list)}, indent=2))
        return result
    finally:
        for dht in reversed(dhts):
            drain(EVENTS)
            dht.shutdown()
        EVENTS.close()
        EVENTS = None


def main():
    global STORE_DELAY
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("calibrate", "suite"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cases", type=Path)
    parser.add_argument("--seeds", nargs="+", type=int, default=[2201])
    parser.add_argument("--patterns", nargs="+", choices=("staggered", "bursty"), default=["staggered"])
    parser.add_argument("--timeout", type=float, default=12)
    parser.add_argument("--store-delay", type=float, default=0)
    parser.add_argument("--skip-suite", type=Path, help="Preserve and skip existing trials with passing tensor checks")
    args = parser.parse_args()
    mp.set_start_method("fork", force=True)
    torch.set_num_threads(1)
    STORE_DELAY = args.store_delay
    args.output.mkdir(parents=True, exist_ok=False)
    (args.output/"provenance.json").write_text(json.dumps(provenance(), indent=2)+"\n")
    if args.command == "calibrate":
        calibrate(args.output)
        return
    if args.cases is None:
        parser.error("suite requires --cases")
    cases = json.loads(args.cases.read_text())
    (args.output/"suite.json").write_text(json.dumps(dict(cases=cases, seeds=args.seeds,
                                                        patterns=args.patterns, timeout=args.timeout,
                                                        store_delay=STORE_DELAY,
                                                        skip_suite=str(args.skip_suite) if args.skip_suite else None), indent=2)+"\n")
    jobs = [(case, seed, pattern) for seed in args.seeds for pattern in args.patterns for case in cases]
    random.Random(220022).shuffle(jobs)
    summaries = []
    for case, seed, pattern in jobs:
        label = f"{case['name']}_{pattern}_s{seed}"
        if args.skip_suite and (args.skip_suite/label/"summary.json").exists():
            old = json.loads((args.skip_suite/label/"summary.json").read_text())
            if not any(old[key] for key in ("tensor_errors", "stage_leaks", "hard_cancelled", "forbidden")):
                continue
        summaries.append(run_trial(case, seed, pattern, args.output/label, args.timeout))
        (args.output/"summaries.json").write_text(json.dumps(summaries, indent=2)+"\n")


if __name__ == "__main__":
    main()
