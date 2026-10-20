"""Private native reference for trainsim. Run with Python 3.11 and native dependencies.

Example (a prepared native package must contain generated protobuf and p2pd):
  python simulator_native_validation.py --package-root PATH --config CONFIG \
      --seed 101 --label native_s101 --output-root RESULTS
No public bootstrap peers are used. All DHT listeners bind loopback.
"""
import argparse
import asyncio
from dataclasses import asdict
import gzip
from functools import wraps
import hashlib
import json
import multiprocessing as mp
import os
from pathlib import Path
import queue
import sys
import time
import traceback
import tarfile

EVENTS = None
START = 0.
DELAY = 0.


def emit(kind, actor="", **data):
    EVENTS.put(dict(time=time.monotonic()-START, kind=kind, actor=actor, data=data, parent=None))


def install_hooks():
    from hivemind.averaging import DecentralizedAverager
    from hivemind.averaging.matchmaking import Matchmaking
    from hivemind.dht.protocol import DHTProtocol
    from hivemind.utils.serializer import MSGPackSerializer
    for name in ("leader_assemble_group", "follower_assemble_group"):
        original = getattr(Matchmaking, name)
        def wrap(native):
            @wraps(native)
            async def method(self, *args, **kwargs):
                result = await native(self, *args, **kwargs)
                gathered = [MSGPackSerializer.loads(MSGPackSerializer.loads(g)[2]) for g in result.gathered]
                own = gathered[result.peer_ids.index(self.peer_id)]
                leader_peer = self.peer_id if native.__name__ == "leader_assemble_group" else args[0]
                leader_name = gathered[result.peer_ids.index(leader_peer)]["node"]
                emit("group_formed", own["node"], roster=dict(
                    group_id=result.group_id.hex(), leader=leader_name,
                    members=[g["node"] for g in gathered], work_ids=[g["work_id"] for g in gathered],
                    formation_time=time.monotonic()-START))
                return result
            return method
        setattr(Matchmaking, name, wrap(original))
    # Delay modeled one-way legs. This models latency, not endpoint bandwidth.
    for name in ("rpc_find", "rpc_store"):
        original = getattr(DHTProtocol, name)
        def wrap_rpc(native):
            @wraps(native)
            async def method(self, request, context):
                await asyncio.sleep(DELAY)
                result = await native(self, request, context)
                await asyncio.sleep(DELAY)
                return result
            return method
        setattr(DHTProtocol, name, wrap_rpc(original))
    original = Matchmaking.rpc_join_group
    @wraps(original)
    async def delayed_join(self, request, context):
        await asyncio.sleep(DELAY)
        async for response in original(self, request, context):
            await asyncio.sleep(DELAY)
            yield response
    Matchmaking.rpc_join_group = delayed_join
    aggregate = DecentralizedAverager._aggregate_with_group
    async def logged_aggregate(self, group_info, *args, **kwargs):
        result = await aggregate(self, group_info, *args, **kwargs)
        own = result[self.peer_id]
        emit("group_completed", own["node"], group_id=group_info.group_id.hex())
        return result
    DecentralizedAverager._aggregate_with_group = logged_aggregate


def decision_fixtures():
    """Compare pure native rules without starting DHT or peer processes."""
    from types import SimpleNamespace
    from hivemind.averaging.load_balancing import load_balance_peers
    from hivemind.averaging.partition import TensorPartContainer
    from hivemind.averaging.key_manager import GroupKeyManager
    from hivemind.averaging.matchmaking import Matchmaking
    from hivemind.proto import averaging_pb2
    from trainsim.protocols.partitioning import allocate_elements, assign_chunks
    from trainsim.protocols.hivemind.adapter import HivemindAdapter
    from trainsim.types import GroupRoster
    import torch
    checks = []
    for n in (2, 3, 4, 8):
        for total in (17, 129, 1024):
            chunk = 16
            native = load_balance_peers(total, [None]*n)
            simulated = allocate_elements(total, [None]*n)
            checks.append(dict(case=f"allocation_{n}_{total}", passed=tuple(native) == tuple(simulated)))
            container = TensorPartContainer([torch.arange(total, dtype=torch.float32)], native, part_size_bytes=chunk*4)
            native_assignment = tuple(tuple(int(part[0].item())//chunk for part in container.get_raw_input_parts(i)) for i in range(n))
            checks.append(dict(case=f"partition_{n}_{total}", passed=native_assignment == assign_chunks(total, chunk, simulated)))
    for length in (0, 1, 4):
        gid = b"fixture-group"
        peers = tuple(f"P{i}" for i in range(4))
        for i, peer in enumerate(peers):
            manager = GroupKeyManager.__new__(GroupKeyManager)
            manager.peer_id, manager.group_bits, manager.target_group_size = peer, "1"*length, 4
            group = SimpleNamespace(group_id=gid, peer_ids=peers, group_size=4)
            asyncio.run(manager.update_key_on_group_assembled(group))
            simulated_adapter = HivemindAdapter.__new__(HivemindAdapter)
            simulated_adapter.config = dict(target_group_size=4)
            state = SimpleNamespace(node_id=peer, group_bits="1"*length)
            roster = GroupRoster(gid.hex(), peers[0], peers, tuple(f"{p}:0" for p in peers), 0.)
            simulated_adapter._update_group_bits(state, roster)
            checks.append(dict(case=f"key_{length}_{i}", passed=manager.group_bits == state.group_bits))
    fake = SimpleNamespace(is_looking_for_group=True, assembled_group=SimpleNamespace(done=lambda: False),
                           client_mode=False, schema_hash=b"schema", current_leader=None,
                           current_followers={}, target_group_size=4, peer_id="self",
                           group_key_manager=SimpleNamespace(current_key="experiment.0b"),
                           potential_leaders=SimpleNamespace(declared_group_key="experiment.0b", declared_expiration_time=1.))
    request = averaging_pb2.JoinRequest(schema_hash=b"schema", expiration=2., group_key="experiment.0b")
    context = SimpleNamespace(remote_id="other")
    reject = Matchmaking._check_reasons_to_reject
    checks.append(dict(case="join_valid", passed=reject(fake, request, context) is None))
    request.schema_hash = b"wrong"
    checks.append(dict(case="join_schema", passed=reject(fake, request, context).code == averaging_pb2.BAD_SCHEMA_HASH))
    request.schema_hash = b"schema"
    fake.current_followers = {"a": 1, "b": 1, "c": 1}
    checks.append(dict(case="join_full", passed=reject(fake, request, context).code == averaging_pb2.GROUP_IS_FULL))
    fake.current_followers = {"other": 1}
    checks.append(dict(case="join_duplicate", passed=reject(fake, request, context).code == averaging_pb2.DUPLICATE_PEER_ID))
    return dict(passed=all(c["passed"] for c in checks), checks=checks)


def main(argv=None):
    global EVENTS, START, DELAY
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package-root", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--simulator-src", type=Path, default=Path(__file__).resolve().parents[1]/"simulator/src")
    parser.add_argument("--fixtures-only", action="store_true")
    args = parser.parse_args(argv)
    sys.path.insert(0, str(args.package_root.resolve()))
    sys.path.insert(1, str(args.simulator_src.resolve()))
    from trainsim.config import load, make_profiles
    from trainsim.randomness import rng
    from hivemind import DHT
    from hivemind.averaging import DecentralizedAverager
    from hivemind.averaging.control import AveragingStage
    import hivemind
    import torch
    import psutil
    c = load(args.config)
    if c["population"] > 16 or c["profile"] != "controlled" or c["initial_bits"] != 0:
        raise ValueError("Native reference limited to <=16 controlled peers with empty group bits")
    if not args.label or Path(args.label).name != args.label:
        raise ValueError("Invalid output label")
    path = args.output_root/args.label
    path.mkdir(parents=True, exist_ok=False)
    if args.fixtures_only:
        result = decision_fixtures()
        from trainsim.runner import source_manifest
        result["simulator_source_hashes"] = source_manifest()["source_hashes"]
        package = Path(hivemind.__file__).parent
        result["native_source_hashes"] = {name: hashlib.sha256((package/"averaging"/name).read_bytes()).hexdigest()
                                           for name in ("matchmaking.py", "key_manager.py", "partition.py", "allreduce.py", "averager.py")}
        repository = Path(__file__).resolve().parents[2]
        with tarfile.open(path/"sources.tar.gz", "w:gz") as archive:
            for relative in result["simulator_source_hashes"]:
                archive.add(repository/relative, arcname=relative, recursive=False)
        (path/"fixtures.json").write_text(json.dumps(result, indent=2)+"\n")
        print(json.dumps(result, indent=2))
        return
    profiles = make_profiles(c, args.seed)
    ctx = mp.get_context("fork")
    EVENTS = ctx.Queue()
    START = time.monotonic()
    DELAY = 2*c["latency_seconds"]
    install_hooks()
    package = Path(hivemind.__file__).parent
    native_files = ["averaging/matchmaking.py", "averaging/key_manager.py", "averaging/allreduce.py",
                    "averaging/partition.py", "averaging/averager.py", "hivemind_cli/p2pd"]
    generated_files = [str(p.relative_to(package)) for p in sorted((package/"proto").glob("*_pb2.py"))]
    manifest = dict(status="running", seed=args.seed, config=c, duration_seconds=c["horizon_seconds"],
                    pid_namespace=os.readlink("/proc/self/ns/pid"),
                    package=str(package), source_hashes={f: hashlib.sha256((package/f).read_bytes()).hexdigest() for f in native_files},
                    generated_proto_hashes={f: hashlib.sha256((package/f).read_bytes()).hexdigest() for f in generated_files},
                    script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                    torch=torch.__version__, python=sys.version, survivors=None,
                    delay_scope="DHT find/store and join legs only; no transfer shaping")
    (path/"manifest.json").write_text(json.dumps(manifest, indent=2)+"\n")
    with tarfile.open(path/"sources.tar.gz", "w:gz") as archive:
        archive.add(Path(__file__), arcname="simulator_native_validation.py")
        for name in native_files+generated_files:
            archive.add(package/name, arcname="hivemind/"+name)
    (path/"resolved_config.json").write_text(json.dumps(c, indent=2)+"\n")
    dhts, peers, controls, sequences, ready = [], {}, {}, {}, {}
    events = gzip.open(path/"events.jsonl.gz", "wt")
    count = 0
    known_children = {}
    prior_children = {p.pid: p.create_time() for p in psutil.Process().children(recursive=True)}
    def drain():
        nonlocal count
        while True:
            try:
                record = EVENTS.get_nowait()
            except queue.Empty:
                break
            record["sequence"] = count
            count += 1
            events.write(json.dumps(record)+"\n")
    try:
        torch.set_num_threads(1)
        for name in profiles:
            dhts.append(DHT(initial_peers=dhts[0].get_visible_maddrs() if dhts else None,
                            host_maddrs=["/ip4/127.0.0.1/tcp/0"], start=True))
            peers[name] = DecentralizedAverager(
                [torch.zeros(c["payload_bytes"]//4)], dhts[-1],
                prefix=f"trainsim_{args.label}_{args.seed}", initial_group_bits="", bandwidth=None,
                target_group_size=c["target_group_size"], min_group_size=c["min_group_size"],
                min_matchmaking_time=c["min_matchmaking_time"], request_timeout=c["request_timeout"],
                allreduce_timeout=c["allreduce_timeout"], sender_timeout=c["sender_timeout"],
                reducer_timeout=c["reducer_timeout"], part_size_bytes=c["chunk_bytes"],
                allow_state_sharing=False, start=True)
        # Children use the parent's original monotonic origin, so preserve START.
        manifest["peer_ids"] = {name: peer.peer_id.to_bytes().hex() for name, peer in peers.items()}
        started = time.monotonic()
        baseline_offset = started-START
        manifest["trace_start_offset"] = baseline_offset
        for name in profiles:
            ready[name] = started+rng(args.seed, "readiness", name).uniform(0, c["initial_offset_seconds"])+profiles[name].compute_seconds
            sequences[name] = 0
        cutoff = started+c["horizon_seconds"]
        hard_stop = cutoff+c["search_timeout"]+c["allreduce_timeout"]+5
        last_process_sample = float("-inf")
        while time.monotonic() < hard_stop:
            drain()
            now = time.monotonic()
            if now-last_process_sample >= 1:
                for child in psutil.Process().children(recursive=True):
                    try:
                        known_children[child.pid] = child.create_time()
                    except psutil.NoSuchProcess:
                        pass
                last_process_sample = now
            for name, control in list(controls.items()):
                if not control.done() or control.stage != AveragingStage.FINISHED:
                    continue
                try:
                    result = control.result()
                    status = "complete" if isinstance(result, dict) else "failed"
                except Exception as exc:
                    status = "failed"
                    emit("native_error", name, error=repr(exc))
                emit("work_terminal", name, outcome=status)
                del controls[name]
                ready[name] = now+profiles[name].compute_seconds
            if now >= cutoff and not controls:
                break
            for name in profiles:
                if now < cutoff and name not in controls and now >= ready[name]:
                    seq = sequences[name]
                    sequences[name] += 1
                    work = dict(work_id=f"{name}:{seq}", origin=name, sequence=seq,
                                compute_start=ready[name]-profiles[name].compute_seconds-START,
                                compute_finish=ready[name]-START, amount=1., payload_bytes=c["payload_bytes"])
                    emit("work_ready", name, work=work)
                    with peers[name].get_tensors() as tensors:
                        tensors[0].fill_(float(seq*len(profiles)+list(profiles).index(name)))
                    controls[name] = peers[name].step(gather=dict(node=name, work_id=work["work_id"]),
                                                     wait=False, timeout=c["search_timeout"], allow_retries=True)
            time.sleep(.002)
        if controls:
            raise RuntimeError("Native drain deadline exceeded")
        manifest["status"] = "completed"
    except BaseException as exc:
        manifest.update(status="error", error=repr(exc), traceback=traceback.format_exc())
        raise
    finally:
        cleanup_errors = []
        for child in psutil.Process().children(recursive=True):
            try:
                if prior_children.get(child.pid) != child.create_time():
                    known_children[child.pid] = child.create_time()
            except psutil.NoSuchProcess:
                pass
        for peer in reversed(list(peers.values())):
            try:
                peer.shutdown()
            except Exception as exc:
                cleanup_errors.append(repr(exc))
        for dht in reversed(dhts):
            try:
                dht.shutdown()
            except Exception as exc:
                cleanup_errors.append(repr(exc))
        time.sleep(.1)
        drain()
        events.close()
        def live_owned_children():
            alive = []
            for pid, creation in known_children.items():
                try:
                    proc = psutil.Process(pid)
                    if proc.create_time() == creation and proc.status() != psutil.STATUS_ZOMBIE:
                        alive.append(pid)
                except psutil.NoSuchProcess:
                    pass
            return alive
        shutdown_deadline = time.monotonic()+5
        survivors = live_owned_children()
        while survivors and time.monotonic() < shutdown_deadline:
            time.sleep(.05)
            survivors = live_owned_children()
        exit_pending_helpers = []
        for pid in survivors:
            try:
                if psutil.Process(pid).name() == "torch_shm_manag":
                    exit_pending_helpers.append(pid)
                elif "torch_shm_manager" in psutil.Process(pid).cmdline()[0]:
                    exit_pending_helpers.append(pid)
            except (psutil.NoSuchProcess, IndexError):
                pass
        forced_cleanup = [pid for pid in survivors if pid not in exit_pending_helpers]
        for pid in forced_cleanup:
            try:
                process = psutil.Process(pid)
                if process.create_time() == known_children[pid]:
                    process.terminate()
            except psutil.NoSuchProcess:
                pass
        for pid in forced_cleanup:
            try:
                process = psutil.Process(pid)
                if process.create_time() == known_children[pid]:
                    try:
                        process.wait(timeout=2)
                    except psutil.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=2)
            except psutil.NoSuchProcess:
                pass
        survivors = live_owned_children()
        manifest["survivors"] = survivors
        manifest["forced_cleanup"] = forced_cleanup
        manifest["cleanup_errors"] = cleanup_errors
        manifest["exit_pending_helpers"] = exit_pending_helpers
        manifest["owned_children"] = {str(pid): creation for pid, creation in known_children.items()}
        (path/"manifest.json").write_text(json.dumps(manifest, indent=2)+"\n")
        # Normalize trace time to the first workload compute schedule.
        records = []
        with gzip.open(path/"events.jsonl.gz", "rt") as file:
            for line in file:
                record = json.loads(line)
                offset = manifest.get("trace_start_offset", 0.)
                record["time"] -= offset
                if record["kind"] == "work_ready":
                    for key in ("compute_start", "compute_finish"):
                        record["data"]["work"][key] -= offset
                elif record["kind"] == "group_formed":
                    record["data"]["roster"]["formation_time"] -= offset
                records.append(record)
        with gzip.open(path/"events.jsonl.gz", "wt") as file:
            for record in records:
                file.write(json.dumps(record)+"\n")
        EVENTS.close()
        EVENTS.join_thread()
    print(path)


if __name__ == "__main__":
    main()
