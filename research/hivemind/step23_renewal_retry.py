"""Bounded, private Step 23 renewal/retry experiments; core Hivemind is unchanged.

Run with the source-verified runtime, e.g. PYTHONPATH=/tmp/hm-step23-runtime
python research/hivemind/step23_renewal_retry.py --phase mechanism --label DATE_a.
Native honest peers are instrumented, not given extra discovery or scheduling cues.
"""
import argparse
import asyncio
import contextlib
import hashlib
import json
import math
import multiprocessing as mp
import os
from pathlib import Path
import random
import re
import subprocess
import time
from typing import AsyncIterator

import step22_refresh_leadership as base
from hivemind.p2p import P2PContext
from hivemind.proto import averaging_pb2

ROOT = Path(__file__).resolve().parent
NAME = ""
SEARCH = None
EVENT_NUMBER = 0
FIRST_JOIN = None
ORIGINAL_BEGIN = base.PotentialLeaders.begin_search
ORIGINAL_POP = base.PotentialLeaders.pop_next_leader
ORIGINAL_ASSEMBLE = base.Matchmaking.leader_assemble_group


def emit(kind, peer="", other="", **detail):
    global EVENT_NUMBER
    if base.EVENTS is not None:
        EVENT_NUMBER += 1
        detail.setdefault("search_id", SEARCH)
        base.EVENTS.put(dict(kind=kind, peer=str(peer), other=str(other),
                             pid=os.getpid(), event_number=EVENT_NUMBER,
                             monotonic=time.monotonic(), wall_time=base.get_dht_time(), detail=detail))


base.record = emit


@contextlib.asynccontextmanager
async def begin_search(self, step, key_manager, declare=True):
    global SEARCH
    self._s23_search_number = getattr(self, "_s23_search_number", 0) + 1
    SEARCH = self._s23_search_id = f"{NAME}:1:search{self._s23_search_number}"
    self._s23_tried_ids = set()
    emit("search_start", self.peer_id, step_id=f"{NAME}:1", deadline=step.deadline)
    try:
        async with ORIGINAL_BEGIN(self, step, key_manager, declare=declare):
            yield
    finally:
        emit("search_end", self.peer_id, step_id=f"{NAME}:1")
        SEARCH = None


async def pop_leader(self):
    while True:
        before = self.past_attempts.copy()
        leader = await ORIGINAL_POP(self)
        added = self.past_attempts - before
        if len(added) != 1:
            raise AssertionError("selected advertisement cannot be identified uniquely")
        selected, expiration = added.pop()
        assert selected == leader
        if not base.ATTACKER and base.CASE.get("guard") == "peer_id" and leader in self._s23_tried_ids:
            emit("guard_skipped", self.peer_id, leader, selected_expiration=expiration)
            continue
        self._s23_tried_ids.add(leader)
        self._s23_selected = (leader, expiration)
        emit("leader_selected", self.peer_id, leader, selected_expiration=expiration)
        return leader


async def join(self, leader):
    if base.ATTACKER:
        emit("forbidden_attacker_join", self.peer_id, leader)
        raise AssertionError("X must not follow")
    selected = getattr(self.potential_leaders, "_s23_selected", None)
    expiration = selected[1] if selected and selected[0] == leader else None
    self.potential_leaders._s23_selected = None
    self._s23_attempt_number = getattr(self, "_s23_attempt_number", 0) + 1
    attempt = f"{NAME}:1:attempt{self._s23_attempt_number}"
    emit("join_start", self.peer_id, leader, attempt_id=attempt,
         selected_expiration=expiration, request_expiration=self.get_request_expiration_time())
    try:
        return await base.ORIGINAL_JOIN(self, leader)
    finally:
        emit("join_end", self.peer_id, leader, attempt_id=attempt)


async def assemble(self):
    group = await ORIGINAL_ASSEMBLE(self)
    emit("assembled", self.peer_id, group_id=group.group_id.hex(),
         roster=[str(peer) for peer in group.peer_ids])
    return group


async def mechanism_publisher(self, step, key_manager):
    if not base.ATTACKER:
        return await base.ORIGINAL_DECLARE(self, step, key_manager)
    async with self.lock_declare:
        try:
            expiration = base.get_dht_time() + 2
            self.declared_group_key = key_manager.current_key
            self.declared_expiration_time = expiration
            self.declared_expiration.set()
            await key_manager.declare_averager(self.declared_group_key, self.peer_id, expiration)
            while not FIRST_JOIN.is_set():
                await asyncio.sleep(0.005)
            await asyncio.sleep(0.25)
            if base.CASE["mode"] == "advance":
                expiration = max(math.nextafter(expiration, math.inf), base.get_dht_time() + 2)
                self.declared_expiration_time = expiration
                self.declared_expiration.set()
            await key_manager.declare_averager(self.declared_group_key, self.peer_id, expiration)
            await asyncio.Future()
        finally:
            if self.declared_group_key is not None:
                key, expiration = self.declared_group_key, self.declared_expiration_time
                self.declared_group_key, self.declared_expiration_time = None, math.inf
                await key_manager.declare_averager(key, self.peer_id, expiration, looking_for_group=False)


async def publisher(self, step, key_manager):
    if base.CASE.get("phase") == "mechanism":
        return await mechanism_publisher(self, step, key_manager)
    return await base.publisher(self, step, key_manager)


base.PotentialLeaders.begin_search = begin_search
base.PotentialLeaders.pop_next_leader = pop_leader
base.PotentialLeaders._declare_averager_periodically = publisher
base.Matchmaking._request_join_group = join
base.Matchmaking.leader_assemble_group = assemble


class RetryAverager(base.DecentralizedAverager):
    """Same servicer class for everyone; only X's response behavior changes."""

    async def rpc_join_group(self, request: averaging_pb2.JoinRequest,
                             context: P2PContext) -> AsyncIterator[averaging_pb2.MessageFromLeader]:
        emit("join_received", self.peer_id, context.remote_id, request_expiration=request.expiration)
        if not base.ATTACKER or base.CASE["response"] == "reliable":
            async for response in super().rpc_join_group(request, context):
                emit("join_reply", self.peer_id, context.remote_id,
                     code=averaging_pb2.MessageCode.Name(response.code))
                yield response
            return
        if base.CASE["response"] == "reject":
            FIRST_JOIN.set()
            emit("join_reply", self.peer_id, context.remote_id, code="NOT_LOOKING_FOR_GROUP")
            yield averaging_pb2.MessageFromLeader(code=averaging_pb2.NOT_LOOKING_FOR_GROUP)
            return
        matchmaking = self._matchmaking
        registered = False
        try:
            async with matchmaking.lock_request_join_group:
                reason = matchmaking._check_reasons_to_reject(request, context)
                if reason is not None:
                    emit("join_reply", self.peer_id, context.remote_id,
                         code=averaging_pb2.MessageCode.Name(reason.code))
                    yield reason
                    return
                matchmaking.current_followers[context.remote_id] = request
                registered = True
                emit("join_reply", self.peer_id, context.remote_id, code="ACCEPTED")
                yield averaging_pb2.MessageFromLeader(code=averaging_pb2.ACCEPTED)
            # Deliberately suppress X's assembly. No honest admission/timeout code is changed.
            await asyncio.sleep(0.5)
            emit("join_reply", self.peer_id, context.remote_id, code="GROUP_DISBANDED")
            yield averaging_pb2.MessageFromLeader(code=averaging_pb2.GROUP_DISBANDED)
        finally:
            if registered:
                matchmaking.current_followers.pop(context.remote_id, None)
                matchmaking.follower_was_discarded.set()
                emit("follower_released", self.peer_id, context.remote_id)


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")


def make_dhts(seed, count):
    nodes = []
    try:
        for index in range(count):
            nodes.append(base.hivemind.DHT(
                initial_peers=nodes[0].get_visible_maddrs() if nodes else None,
                host_maddrs=base.LOCALHOST, node_id=base.DHTID.generate(f"s23:{seed}:{index}"), start=True))
        return nodes
    except BaseException:
        for node in reversed(nodes):
            node.shutdown()
        raise


def run_trial(case, seed, pattern, path):
    global NAME, FIRST_JOIN
    path.mkdir()
    mechanism = case["phase"] == "mechanism"
    count, timeout = (3, 6) if mechanism else (8, 12)
    names = ["X", "H0"] if mechanism else ["X"] + [f"H{i}" for i in range(7)]
    offsets = {"X": 0, "H0": 0} if mechanism else base.launch_offsets(seed, pattern)
    config = dict(case=case, seed=seed, pattern=pattern, offsets=offsets, dht_count=count,
                  timeout=timeout, allreduce_timeout=6, correct_output_horizon=18,
                  lifetime=2, frequency=1 if case["mode"] == "refresh" else None,
                  hold=0.5, target=4, honest_min=2, x_min=4, request_timeout=3,
                  honest_min_matchmaking_time=5, initial_group_bits="", tensor_size=128)
    base.EVENTS, FIRST_JOIN = mp.Queue(), mp.Event()
    base.CASE, base.ATTACKER = case, False
    nodes, peers, controls, events, outcomes, starts = [], {}, {}, [], {}, {}
    raw_file = (path / "events.jsonl").open("w")

    def collect():
        for event in base.drain(base.EVENTS, quiet=0.002, cap=0.02):
            events.append(event)
            raw_file.write(json.dumps(event, allow_nan=False) + "\n")
        raw_file.flush()

    def start(name):
        starts[name] = time.monotonic()
        value = float(7 if name == "X" else int(name[1:]))
        emit("step_start", peers[name].peer_id, name=name, step_id=f"{name}:1", value=value)
        controls[name] = peers[name].step(gather=dict(name=name, step_id=f"{name}:1", value=value),
                                         wait=False, allow_retries=True, timeout=timeout)

    def finish():
        for name, control in controls.items():
            if name in outcomes or not control.done() or control.stage != base.AveragingStage.FINISHED:
                continue
            result = dict(name=name, step_id=f"{name}:1", elapsed=time.monotonic()-starts[name],
                          status="cancelled" if control.cancelled() else "failed")
            if not control.cancelled():
                try:
                    members = control.result()
                    if isinstance(members, dict):
                        with peers[name].get_tensors() as tensors:
                            actual = tensors[0].clone().tolist()
                        result.update(status="completed", members={str(peer): data for peer, data in members.items()},
                                      actual=actual)
                    else:
                        result["status"] = "no_group"
                except Exception as exc:
                    result.update(error=type(exc).__name__, message=str(exc))
            outcomes[name] = result
            emit("step_result", peers[name].peer_id, **result)

    try:
        nodes = make_dhts(seed, count)
        common = dict(prefix=f"s23_{seed}_{pattern}", target_group_size=4,
                      min_matchmaking_time=5, request_timeout=3, allreduce_timeout=6,
                      allow_state_sharing=False, start=True)
        for name in names:
            NAME, base.ATTACKER = name, name == "X"
            node = nodes[0] if name == "X" else nodes[int(name[1:])+1]
            value = float(7 if name == "X" else int(name[1:]))
            peers[name] = RetryAverager([base.torch.full((128,), value)], node,
                                       min_group_size=4 if name == "X" else 2, **common)
        NAME, base.ATTACKER = "controller", False
        config["peer_ids"] = {name: str(peer.peer_id) for name, peer in peers.items()}
        config["node_ids"] = [str(int(base.DHTID.generate(f"s23:{seed}:{index}"))) for index in range(count)]
        write_json(path / "config.json", config)
        time.sleep(0.5)
        began = time.monotonic()
        if mechanism:
            start("X")
            subkey = peers["X"].peer_id.to_bytes()
            # Observer is only a start barrier; it does not supply candidates to H0.
            while time.monotonic()-began < 2:
                view = nodes[2].get(f"{common['prefix']}.0b", latest=True)
                entry = view.value.get(subkey) if view is not None and isinstance(view.value, dict) else None
                if entry and entry.value and entry.expiration_time > base.get_dht_time():
                    emit("observer_ready", nodes[2].peer_id, expiration=entry.expiration_time)
                    break
                collect()
                time.sleep(0.01)
            else:
                raise RuntimeError("initial X advertisement not observed")
            start("H0")
        else:
            for name in sorted(names, key=offsets.get):
                while time.monotonic() < began+offsets[name]:
                    collect()
                    finish()
                    time.sleep(0.005)
                start(name)
        hard_stop = max(starts.values()) + timeout + 6 + 8
        expected_x_cancel = False
        while time.monotonic() < hard_stop:
            collect()
            finish()
            if all(name in outcomes for name in names if name != "X") and "X" not in outcomes:
                if not controls["X"].done():
                    controls["X"].cancel()
                    expected_x_cancel = True
            if len(outcomes) == len(names):
                break
            time.sleep(0.005)
        if len(outcomes) != len(names):
            raise RuntimeError(f"unfinished steps or stuck stages: {set(names)-set(outcomes)}")
        collect()
        if any(event["kind"].startswith("forbidden_") for event in events):
            raise RuntimeError("forbidden attacker action")
        write_json(path / "summary.json", dict(outcomes=outcomes, expected_x_cancel=expected_x_cancel))
    except BaseException as exc:
        write_json(path / "invalid.json", dict(error=type(exc).__name__, message=str(exc)))
        raise
    finally:
        for control in controls.values():
            if not control.done():
                control.cancel()
        for peer in peers.values():
            collect()
            peer.shutdown()
        for node in reversed(nodes):
            collect()
            node.shutdown()
        emit("trace_closed", participants=names)
        collect()
        base.EVENTS.close()
        base.EVENTS.join_thread()
        raw_file.close()
        base.EVENTS, base.ATTACKER = None, False
    from analyze_step23 import analyse_trial
    checked = analyse_trial(path)
    write_json(path / "checked.json", checked)
    return checked


def jobs_for(phase):
    if phase == "mechanism":
        cases = [dict(name=mode, mode=mode, response="reject", phase=phase, guard="native")
                 for mode in ("same", "advance")]
        return [(case, seed, "mechanism") for seed in range(2301, 2306) for case in cases]
    seeds = range(2306, 2308) if phase == "smoke" else range(2311, 2331) if phase == "confirm" else range(2351, 2357)
    responses = ("unreliable",) if phase == "diagnostic" else ("reliable", "unreliable")
    guards = ("native", "peer_id") if phase == "diagnostic" else ("native",)
    cases = [dict(name=f"{response}_{mode}_{guard}", mode=mode, response=response, phase=phase,
                  guard=guard, minimum=4, lifetime=2, frequency=1 if mode == "refresh" else None)
             for response in responses for mode in ("expire", "refresh") for guard in guards]
    return [(case, seed, pattern) for seed in seeds for pattern in ("staggered", "bursty") for case in cases]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=("mechanism", "smoke", "confirm", "diagnostic"), required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--gate", type=Path, help="Previous phase's independently analyzed JSON")
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9_-]+", args.label):
        parser.error("label must contain letters, numbers, underscores or hyphens")
    if args.phase != "mechanism":
        if args.gate is None:
            parser.error("a passing --gate is required")
        from analyze_step23 import analyse_suite
        gate = analyse_suite(args.gate.parent)
        required = {"smoke": "mechanism_pass", "confirm": "smoke_pass", "diagnostic": "harm_pass"}[args.phase]
        if not gate.get(required):
            parser.error(f"prior analysis did not pass {required}")
    mp.set_start_method("fork", force=True)
    base.torch.set_num_threads(1)
    provenance = base.provenance()
    source = Path(base.hivemind.__file__).resolve().parent
    for relative in ("averaging/control.py", "p2p/servicer.py", "utils/serializer.py"):
        assert (source / relative).read_bytes() == (base.REPO / "hivemind" / relative).read_bytes()
        provenance["source_sha256"][relative] = hashlib.sha256((source / relative).read_bytes()).hexdigest()
    output = ROOT / "results" / f"step23_{args.phase}_{args.label}"
    output.mkdir()
    snapshots = output / "harness"
    snapshots.mkdir()
    for name in ("step23_renewal_retry.py", "analyze_step23.py", "step22_refresh_leadership.py"):
        data = (ROOT / name).read_bytes()
        (snapshots / name).write_bytes(data)
        provenance.setdefault("harness_sha256", {})[name] = hashlib.sha256(data).hexdigest()
    provenance["packages"] = subprocess.check_output([os.sys.executable, "-m", "pip", "freeze"], text=True).splitlines()
    write_json(output / "provenance.json", provenance)
    jobs = jobs_for(args.phase)
    random.Random(230023).shuffle(jobs)
    write_json(output / "suite.json", dict(phase=args.phase, jobs=[dict(case=c, seed=s, pattern=p) for c, s, p in jobs],
                                           gate=gate if args.gate else None))
    for index, (case, seed, pattern) in enumerate(jobs, 1):
        checked = run_trial(case, seed, pattern, output / f"{case['name']}_{pattern}_s{seed}")
        print(f"{index}/{len(jobs)} {case['name']} {pattern} s{seed}: "
              f"correct={checked['honest_correct']} retries={checked['x_retries']} "
              f"waiting={checked['x_wait_seconds']:.3f}s", flush=True)
        if checked["incorrect_steps"]:
            raise RuntimeError("incorrect tensor: preserve evidence and diagnose before continuing")
    from analyze_step23 import analyse_suite
    write_json(output / "analysis.json", analyse_suite(output))
    print(f"Saved {output}", flush=True)


if __name__ == "__main__":
    main()
