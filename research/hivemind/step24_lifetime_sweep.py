"""Private lifetime sweep with equal simulated training delays and native timing traces.

No core source edits or network latency injection. Use the source-verified runtime:
PYTHONPATH=/tmp/hm-step23-runtime /tmp/hm-step23-env/bin/python \
  research/hivemind/step24_lifetime_sweep.py --phase smoke --label 20261008_a
"""
import argparse
import asyncio
import hashlib
import json
import math
import multiprocessing as mp
import os
from pathlib import Path
import random
import re
import statistics
import subprocess
import sys
import time

import step22_refresh_leadership as base
from hivemind.utils.serializer import MSGPackSerializer
from hivemind.averaging.allreduce import AllReduceRunner

ROOT = Path(__file__).resolve().parent
NAMES = ["X"] + [f"H{i}" for i in range(7)]
SIGMA = math.sqrt(math.log(1 + 0.6**2))
MU = math.log(1.5) - SIGMA**2 / 2
TRAINING = dict(mu=MU, sigma=SIGMA, lower=0.1, upper=4.0)
EVENT_NUMBER = 0
CURRENT_STEP = None
ORIGINAL_ASSEMBLE = base.Matchmaking.leader_assemble_group
ORIGINAL_JOIN_RPC_CODE = base.Matchmaking.rpc_join_group.__code__
ORIGINAL_WAIT = asyncio.wait
ORIGINAL_WAIT_FOR = asyncio.wait_for
ORIGINAL_CLIENT_CODE = base.ORIGINAL_JOIN.__code__
ORIGINAL_COMMUNICATE = AllReduceRunner._communicate_with_peer


def emit(kind, peer="", other="", **detail):
    global EVENT_NUMBER
    if base.EVENTS is not None:
        EVENT_NUMBER += 1
        detail.setdefault("step_id", CURRENT_STEP)
        base.EVENTS.put(dict(kind=kind, peer=str(peer), other=str(other),
                             pid=os.getpid(), event_number=EVENT_NUMBER,
                             monotonic=time.monotonic(), wall_time=base.get_dht_time(), detail=detail))


base.record = emit


def user_data(step):
    return MSGPackSerializer.loads(MSGPackSerializer.loads(step.data_for_gather)[2])


ORIGINAL_REQUEST = base.Matchmaking._request_join_potential_leaders


async def traced_search(self, step):
    global CURRENT_STEP
    CURRENT_STEP = user_data(step)["step_id"]
    emit("search_start", self.peer_id, deadline=step.deadline,
         scheduled_time=step.scheduled_time, group_key=self.group_key_manager.current_key)
    try:
        return await ORIGINAL_REQUEST(self, step)
    finally:
        emit("search_end", self.peer_id)
        # Inbound join streams may still be draining: explicit IDs on those events.
        CURRENT_STEP = None


async def traced_assemble(self):
    group = await ORIGINAL_ASSEMBLE(self)
    user = user_data(self.step_control)
    members = {str(peer): MSGPackSerializer.loads(MSGPackSerializer.loads(data)[2])
               for peer, data in zip(group.peer_ids, group.gathered)}
    emit("assembled", self.peer_id, step_id=user["step_id"],
         group_id=group.group_id.hex(), roster=[str(peer) for peer in group.peer_ids], user=user, members=members)
    return group


async def traced_wait(fs, *, timeout=None, return_when=asyncio.ALL_COMPLETED):
    # Observe the existing native timeout argument. Never change it or the futures.
    frame = sys._getframe(1)
    if frame.f_code is ORIGINAL_JOIN_RPC_CODE and timeout is not None:
        mm, context = frame.f_locals["self"], frame.f_locals["context"]
        wall = base.get_dht_time()
        user = user_data(mm.step_control)
        emit("native_leader_wait", mm.peer_id, context.remote_id,
             step_id=user["step_id"], captured_expiration=mm.potential_leaders.declared_expiration_time,
             timeout=timeout, timer_started_wall=wall,
             followers=[str(peer) for peer in mm.current_followers])
    del frame
    return await ORIGINAL_WAIT(fs, timeout=timeout, return_when=return_when)


async def traced_wait_for(awaitable, timeout):
    frame = sys._getframe(1)
    native_client = frame.f_code is ORIGINAL_CLIENT_CODE
    detail, peer, leader = {}, "", ""
    if native_client:
        mm, leader = frame.f_locals["self"], frame.f_locals["leader"]
        peer = mm.peer_id
        detail = dict(step_id=user_data(mm.step_control)["step_id"], timeout=timeout,
                      request_expiration=frame.f_locals.get("request_expiration_time"),
                      phase="handshake" if mm.current_leader is None else "waiting_for_group")
        emit("native_client_wait", peer, leader, **detail)
    del frame
    try:
        result = await ORIGINAL_WAIT_FOR(awaitable, timeout)
        if native_client:
            emit("native_client_reply", peer, leader, **detail,
                 code=base.averaging_pb2.MessageCode.Name(result.code),
                 group_id=result.group_id.hex())
        return result
    except BaseException as exc:
        if native_client:
            emit("native_client_exception", peer, leader, **detail,
                 error=type(exc).__name__, error_message=str(exc))
        raise


async def traced_communicate(self, peer_id):
    emit("reducer_exchange_start", self.peer_id, peer_id, group_id=self.group_id.hex())
    try:
        return await ORIGINAL_COMMUNICATE(self, peer_id)
    except BaseException as exc:
        emit("reducer_exchange_failed", self.peer_id, peer_id, group_id=self.group_id.hex(),
             error=type(exc).__name__, error_message=str(exc),
             failed_elements=self.tensor_part_container.failed_size)
        raise
    finally:
        emit("reducer_exchange_end", self.peer_id, peer_id, group_id=self.group_id.hex(),
             failed_elements=self.tensor_part_container.failed_size)


base.Matchmaking._request_join_potential_leaders = traced_search
base.Matchmaking.leader_assemble_group = traced_assemble
asyncio.wait = traced_wait
asyncio.wait_for = traced_wait_for
AllReduceRunner._communicate_with_peer = traced_communicate


class TimingAverager(base.RefreshAverager):
    async def _run_allreduce_inplace_(self, tensors, group_info, **kwargs):
        members = {str(peer): MSGPackSerializer.loads(MSGPackSerializer.loads(data)[2])
                   for peer, data in zip(group_info.peer_ids, group_info.gathered)}
        own = members[str(self.peer_id)]
        emit("allreduce_start", self.peer_id, step_id=own["step_id"],
             group_id=group_info.group_id.hex(), members=members,
             actual_input=tensors[0].clone().tolist())
        error = None
        try:
            return await super()._run_allreduce_inplace_(tensors, group_info, **kwargs)
        except BaseException as exc:
            error = type(exc).__name__
            raise
        finally:
            emit("allreduce_end", self.peer_id, step_id=own["step_id"],
                 group_id=group_info.group_id.hex(), error=error,
                 actual_output=tensors[0].clone().tolist())


def policies():
    cases = [dict(name=name, mode="refresh", minimum=4, target=4, frequency=1.0, lifetime=upper,
                  nominal_lower=upper-1.0, nominal_upper=upper)
             for name, upper in (("range_0_1", 1.0), ("range_025_125", 1.25),
                                 ("range_1_2", 2.0), ("range_2_3", 3.0), ("range_3_4", 4.0))]
    cases.extend([dict(name="expire_2", mode="expire", minimum=4, target=4, lifetime=2.0),
                  dict(name="passive_stock", mode="passive", minimum=4, target=4, lifetime=5.0),
                  dict(name="stock", mode="stock", minimum=4, target=4, lifetime=5.0)])
    return cases


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, allow_nan=False)+"\n")


def run_trial(case, seed, path, duration, outcome="averaging"):
    path.mkdir()
    base.EVENTS = mp.Queue()
    base.CASE, base.ATTACKER, base.STORE_DELAY = case, False, 0.0
    dhts, peers, controls, outcomes = [], {}, {}, []
    rngs = {name: random.Random(seed*100+index) for index, name in enumerate(NAMES)}
    offsets = base.launch_offsets(seed, "staggered")
    config = dict(case=case, seed=seed, duration=duration, outcome=outcome, timeout=12, tensor_size=128,
                  initial_offsets=offsets, training_delay=TRAINING, honest_min=2, target=4,
                  honest_ad_lifetime=5, allreduce_timeout=6, store_delay=0,
                  delay_location="after observed FINISHED, before next step()",
                  group_bits="", scheduled_time="native default", network="eight-peer loopback")
    raw_file = (path/"events.jsonl").open("w")
    pending = []
    error = None

    def collect():
        events = base.drain(base.EVENTS, quiet=0.002, cap=0.02)
        for event in events:
            raw_file.write(json.dumps(event, allow_nan=False)+"\n")
        raw_file.flush()
        return len(events)

    try:
        dhts = base.make_dhts(seed)
        common = dict(prefix=f"s24_{seed}", target_group_size=4, min_matchmaking_time=5,
                      request_timeout=3, allreduce_timeout=6, allow_state_sharing=False, start=True)
        for index, name in enumerate(NAMES):
            base.ATTACKER = name == "X"
            value = 7 if name == "X" else int(name[1:])
            peers[name] = TimingAverager([base.torch.full((128,), float(value))],
                                         dhts[7 if name == "X" else index-1],
                                         min_group_size=4 if name == "X" else 2, **common)
        base.ATTACKER = False
        identities = {name: str(peer.peer_id) for name, peer in peers.items()}
        config["peer_ids"] = identities
        time.sleep(0.5)
        started = time.monotonic()
        config.update(started_monotonic=started, started_wall=base.get_dht_time())
        write_json(path/"config.json", config)
        ready = {name: started+offsets[name] for name in NAMES}
        sequences = {name: 0 for name in NAMES}
        hard_stop = started+duration+12+8
        while time.monotonic() < hard_stop:
            collect()
            now = time.monotonic()
            for name, control in list(controls.items()):
                if not control.done() or control.stage != base.AveragingStage.FINISHED:
                    continue
                row = dict(name=name, sequence=sequences[name], step_id=f"{name}:{sequences[name]}",
                           finished=now, status="cancelled" if control.cancelled() else "failed")
                if not control.cancelled():
                    try:
                        result = control.result()
                        if isinstance(result, dict):
                            members = {str(peer): data for peer, data in result.items()}
                            expected = statistics.mean(data["value"] for data in members.values())
                            with peers[name].get_tensors() as tensors:
                                actual = tensors[0].clone()
                            row.update(status="completed", members=members, expected=expected,
                                       actual=actual.tolist(), correct=bool(base.torch.allclose(
                                           actual, base.torch.full_like(actual, expected), atol=1e-4)))
                        else:
                            row["status"] = "no_group"
                    except Exception as exc:
                        row.update(error=type(exc).__name__, error_message=str(exc))
                outcomes.append(row)
                emit("step_result", identities[name], **row)
                del controls[name]
                delay = min(4.0, max(0.1, rngs[name].lognormvariate(MU, SIGMA)))
                ready[name] = now+delay
                emit("training_scheduled", identities[name], name=name, sequence=sequences[name],
                     step_id=row["step_id"], finished=now, delay=delay, ready=ready[name])
                if outcome == "averaging" and row.get("correct") is False:
                    error = "incorrect returned tensor; stop submissions and preserve diagnostic trace"
            if error:
                break
            if now < started+duration:
                for name in NAMES:
                    if name not in controls and now >= ready[name]:
                        sequences[name] += 1
                        value = float((7 if name == "X" else int(name[1:]))+10*sequences[name])
                        with peers[name].get_tensors() as tensors:
                            tensors[0].fill_(value)
                        data = dict(name=name, sequence=sequences[name], step_id=f"{name}:{sequences[name]}", value=value)
                        emit("step_start", identities[name], **data)
                        controls[name] = peers[name].step(gather=data, wait=False, allow_retries=True, timeout=12)
            elif not controls:
                break
            time.sleep(0.005)
        pending = [f"{name}:{sequences[name]}" for name in controls]
        if pending and not error:
            error = "steps exceeded bounded drain allowance"
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
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
        # All writers have exited; queue delivery precedes closure marker.
        while collect():
            pass
        emit("trace_closed", participants=list(peers), pending=pending, error=error)
        while collect():
            pass
        raw_file.close()
        base.EVENTS.close()
        base.EVENTS.join_thread()
        base.EVENTS = None
        base.ATTACKER = False
        write_json(path/"summary.json", dict(outcomes=outcomes, pending=pending, duration=duration, error=error))
    from analyze_step24_lifetime import analyse_trial
    checked = analyse_trial(path)
    write_json(path/"analysis.json", checked)
    return checked


def trial_should_halt(checked, outcome):
    if checked.get("pending") or checked.get("error") or checked.get("control_integrity_errors"):
        return True
    return outcome == "averaging" and bool(checked.get("incorrect_steps") or checked.get("integrity_errors"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=("smoke", "confirm", "diagnostic"), required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--outcome", choices=("grouping", "averaging"), default="grouping",
                        help="Grouping counts all native assembled groups; tensor correctness is diagnostic")
    parser.add_argument("--gate", type=Path)
    parser.add_argument("--diagnose", type=Path, help="Failed trial to replay with native client tracing")
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9_-]+", args.label):
        parser.error("label must contain letters, numbers, underscores or hyphens")
    from analyze_step24_lifetime import analyse_suite
    if args.phase == "confirm":
        if not args.gate:
            parser.error("confirmation requires a complete passing smoke suite via --gate")
        try:
            gate = analyse_suite(args.gate)
        except ValueError as exc:
            parser.error(f"smoke gate rejected: {exc}")
        gate_pass = gate.get("control_plane_pass") if args.outcome == "grouping" else gate.get("averaging_pass", gate.get("pass"))
        if gate.get("phase") != "smoke" or gate.get("outcome", "averaging") != args.outcome or not gate_pass:
            parser.error("smoke suite failed integrity/completeness checks")
    if args.phase == "diagnostic" and args.diagnose is None:
        parser.error("diagnostic requires --diagnose pointing to the preserved failed trial")
    mp.set_start_method("fork", force=True)
    base.torch.set_num_threads(1)
    output = ROOT/"results"/f"step24_{args.phase}_{args.label}"
    output.mkdir()
    provenance = base.provenance()
    provenance["executable"] = sys.executable
    provenance["packages"] = subprocess.check_output(
        [sys.executable, "-m", "pip", "freeze"], text=True).splitlines()
    source = Path(base.hivemind.__file__).resolve().parent
    for relative in ("averaging/control.py", "utils/serializer.py",
                     "averaging/allreduce.py", "averaging/partition.py"):
        data = (source/relative).read_bytes()
        if data != (base.REPO/"hivemind"/relative).read_bytes():
            raise RuntimeError(f"runtime source differs: {relative}")
        provenance["source_sha256"][relative] = hashlib.sha256(data).hexdigest()
    (output/"harness").mkdir()
    for name in ("step24_lifetime_sweep.py", "analyze_step24_lifetime.py", "step22_refresh_leadership.py"):
        data = (ROOT/name).read_bytes()
        (output/"harness"/name).write_bytes(data)
        provenance.setdefault("harness_sha256", {})[name] = hashlib.sha256(data).hexdigest()
    write_json(output/"provenance.json", provenance)
    seeds = [2400] if args.phase == "smoke" else list(range(2401, 2406))
    duration = 10 if args.phase == "smoke" else 60
    jobs = [dict(case=case, seed=seed, duration=duration) for seed in seeds for case in policies()]
    if args.phase == "diagnostic":
        failed = json.loads((args.diagnose/"config.json").read_text())
        jobs = [dict(case=failed["case"], seed=failed["seed"], duration=10)]
        duration = 10
    random.Random(240024).shuffle(jobs)
    write_json(output/"suite.json", dict(phase=args.phase, outcome=args.outcome, jobs=jobs, gate=str(args.gate) if args.gate else None,
                                         diagnose=str(args.diagnose) if args.diagnose else None,
                                         order_seed=240024, training_delay=TRAINING))
    for index, job in enumerate(jobs, 1):
        case, seed = job["case"], job["seed"]
        trial = output/f"{case['name']}_s{seed}"
        try:
            checked = run_trial(case, seed, trial, duration, outcome=args.outcome)
            print(f"{index}/{len(jobs)} {case['name']} s{seed}: X-led assembled "
                  f"{checked['x_assembled_leads']}/{checked['total_assembled_groups']} "
                  f"tensor mismatches (diagnostic)={len(checked['incorrect_steps'])}", flush=True)
            if trial_should_halt(checked, args.outcome):
                raise RuntimeError("trial failed selected outcome guard; preserve raw evidence")
            summary = json.loads((trial/"summary.json").read_text())
            if summary.get("error"):
                raise RuntimeError(summary["error"])
        except Exception as exc:
            write_json(output/"halt.json", dict(trial=trial.name, checked_trials=index,
                                                reason=f"{type(exc).__name__}: {exc}"))
            try:
                write_json(output/"analysis.json", analyse_suite(output, require_complete=False))
            except Exception as analysis_error:
                write_json(output/"analysis_error.json", dict(error=str(analysis_error)))
            raise
        write_json(output/"analysis.json", analyse_suite(output, require_complete=False))
    write_json(output/"analysis.json", analyse_suite(output))
    print(f"Saved {output}", flush=True)


if __name__ == "__main__":
    main()
