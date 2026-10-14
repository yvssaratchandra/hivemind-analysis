"""Step 28: direct reliable single versus renewed short-ad confirmation.

Run from /tmp with PYTHONPATH=/tmp/hm-step23-runtime and the verified interpreter.
Offline --dry-run does not import Hivemind or open sockets. STOP_AFTER_CURRENT in
the suite directory stops at a peer-shutdown boundary. Core source stays untouched.
"""
from __future__ import annotations

import argparse
import asyncio
import contextvars
from datetime import datetime, timezone
import fcntl
import functools
import hashlib
import json
import math
import os
from pathlib import Path
import random
import re
import shutil
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parent
BASE = OBS = None
RPC_NUMBER = 0
VISIT_NUMBER = 0
ORIGINAL_JOIN_RESPONSE = None
SERVER_JOIN_RPC = contextvars.ContextVar("step28_server_join_rpc", default=None)
FROZEN_FILES = ("step28_confirmation.py", "step28_workload.py", "analyze_step28_confirmation.py", "test_step28_confirmation.py", "verify_step28_confirmation.py",
                "analyze_step27_control_plane.py", "verify_step27_raw.py", "audit_step27_receipts.py",
                "step26_joint_sweep.py", "analyze_step26_joint.py", "step26_teardown_audit.py", "step26_teardown_audit_v2.py",
                "step25_choice_measurement.py", "step24_lifetime_sweep.py",
                "step22_refresh_leadership.py", "analyze_step24_lifetime.py",
                "analyze_step25_choices.py", "test_step25_harness.py", "test_step25_choices.py")
RAW_FILES = ("config.json", "events.jsonl", "summary.json")
TRAINING_SIGMA = math.sqrt(math.log(1 + .6 ** 2))
TRAINING = dict(mu=math.log(1.5)-TRAINING_SIGMA**2/2, sigma=TRAINING_SIGMA, lower=.1, upper=4.)
CLOCK_JUMP_LIMIT = .25


class ClockDiscontinuity(RuntimeError):
    """Preserved trial must be rerun after a suspend or clock jump."""


def check_clock(samples):
    """Check wall/boot offsets against monotonic; suspend excludes monotonic time."""
    errors = []
    for field in ("clock_wall", "clock_boottime"):
        offsets = [s[field]-s["clock_monotonic"] for s in samples]
        if offsets and max(offsets)-min(offsets) > CLOCK_JUMP_LIMIT:
            errors.append(f"clock discontinuity: {field} minus monotonic exceeded {CLOCK_JUMP_LIMIT}s")
    return errors


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024*1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_json(path):
    return json.loads(Path(path).read_text())


def atomic_json(path, value):
    path = Path(path)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("x") as stream:
            json.dump(value, stream, indent=2, sort_keys=True, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    finally:
        temporary.unlink(missing_ok=True)


def make_cases():
    cases = []
    for mode, suffix in (("once", "single"), ("refresh", "renewed")):
        case = dict(name=f"reliable_{suffix}", mode=mode, response="reliable",
                    lifetime=2., hold_seconds=0.)
        if mode == "refresh":
            case.update(frequency=2., nominal_lower=1.5, nominal_upper=2.)
        cases.append(case)
    for case in cases:
        case.update(minimum=4, target=4, one_way_delay_seconds=.15)
    return cases


def make_jobs(phase):
    if phase not in ("smoke", "confirm"):
        raise ValueError("phase must be smoke or confirm")
    cases = make_cases()
    if phase == "smoke":
        return [dict(case=c, seed=2800, duration=20) for c in cases]
    return [dict(case=c, seed=seed, duration=120)
            for seed in (2801, 2802, 2803)
            for c in (list(reversed(cases)) if seed == 2802 else cases)]


def emit(kind, peer="", other="", **detail):
    if OBS is not None:
        return OBS.emit(kind, peer, other, **detail)


async def injected_sleep(rpc_id, leg, delay):
    started = time.monotonic()
    cancelled = False
    try:
        if delay:
            await asyncio.sleep(delay)
    except asyncio.CancelledError:
        cancelled = True
        raise
    finally:
        emit("control_rpc_delay", rpc_id=rpc_id, leg=leg, requested_delay=delay,
             actual_delay=time.monotonic()-started, cancelled=cancelled)


def rpc_start(self, request, context, rpc_type):
    global RPC_NUMBER
    RPC_NUMBER += 1
    rpc_id = f"{os.getpid()}:rpc{RPC_NUMBER}"
    peer = getattr(self, "peer_id", None)
    if peer is None:
        peer = self.p2p.peer_id
    d = BASE.CASE["one_way_delay_seconds"]
    keys = [bytes(key).hex() for key in getattr(request, "keys", ())]
    emit("control_rpc_start", peer, context.remote_id, rpc_id=rpc_id, rpc_type=rpc_type,
         key_hashes=keys, delay_seconds=d)
    return rpc_id, d, peer


def wrap_unary(original, rpc_type):
    @functools.wraps(original)
    async def delayed(self, request, context):
        rpc_id, d, peer = rpc_start(self, request, context, rpc_type)
        error = None
        try:
            await injected_sleep(rpc_id, "request", d)
            emit("control_rpc_handler_enter", peer, context.remote_id, rpc_id=rpc_id)
            result = await original(self, request, context)
            await injected_sleep(rpc_id, "response", d)
            emit("control_rpc_response", peer, context.remote_id, rpc_id=rpc_id, response_index=0)
            return result
        except BaseException as exc:
            error = type(exc).__name__
            raise
        finally:
            emit("control_rpc_end", peer, context.remote_id, rpc_id=rpc_id,
                 cancelled=error in ("CancelledError", "GeneratorExit"), error=error)
    return delayed


def wrap_join(original):
    @functools.wraps(original)
    async def delayed(self, request, context):
        rpc_id, d, peer = rpc_start(self, request, context, "join")
        token = SERVER_JOIN_RPC.set(rpc_id)
        error = None
        stream = None
        try:
            await injected_sleep(rpc_id, "request", d)
            emit("control_rpc_handler_enter", peer, context.remote_id, rpc_id=rpc_id)
            matchmaking = getattr(self, "_matchmaking", None)
            if matchmaking is not None:
                leaders = matchmaking.potential_leaders
                own = leaders.declared_expiration_time
                emit("control_join_admission_state", peer, context.remote_id, rpc_id=rpc_id,
                     leader_expiration=own if math.isfinite(own) else None,
                     request_expiration=request.expiration,
                     is_looking_for_group=matchmaking.is_looking_for_group,
                     declared_group_key=leaders.declared_group_key,
                     leader_step_id=getattr(leaders, "_s25_step_id", None),
                     current_leader=str(matchmaking.current_leader) if matchmaking.current_leader else None,
                     followers=[str(p) for p in matchmaking.current_followers])
            stream = original(self, request, context)
            index = 0
            async for response in stream:
                await injected_sleep(rpc_id, "response", d)
                emit("control_rpc_response", peer, context.remote_id, rpc_id=rpc_id,
                     response_index=index, code=BASE.averaging_pb2.MessageCode.Name(response.code))
                index += 1
                yield response
        except BaseException as exc:
            error = type(exc).__name__
            raise
        finally:
            try:
                if stream is not None:
                    await stream.aclose()
            finally:
                emit("control_rpc_end", peer, context.remote_id, rpc_id=rpc_id,
                     cancelled=error in ("CancelledError", "GeneratorExit"), error=error)
                try:
                    SERVER_JOIN_RPC.reset(token)
                except ValueError:
                    # Async-generator finalization can run in a fresh context;
                    # its private context disappears with that finalizer task.
                    pass
    return delayed


async def publish_once(self, step, key_manager):
    """One active publication per SEARCH; never republish on expiry or resume."""
    async with self.lock_declare:
        try:
            await self.running.wait()
            now = BASE.get_dht_time()
            expiration = min(now+BASE.CASE["lifetime"], self.search_end_time)
            if expiration <= now:
                return
            self.declared_group_key = group_key = key_manager.current_key
            self.declared_expiration_time = expiration
            self.declared_expiration.set()
            emit("once_publication", self.peer_id, group_key,
                 search_id=getattr(self, "_s25_search_id", None), expiration=expiration)
            await key_manager.declare_averager(group_key, self.peer_id, expiration)
            # The native begin_search context cancels this task at search completion.
            await asyncio.Future()
        finally:
            if self.declared_group_key is not None:
                group_key, expiration = self.declared_group_key, self.declared_expiration_time
                self.declared_group_key, self.declared_expiration_time = None, math.inf
                await key_manager.declare_averager(group_key, self.peer_id, expiration, looking_for_group=False)


async def unreliable_join(self, request, context):
    """Only X changes its response policy; honest peers keep native behavior."""
    global VISIT_NUMBER
    if not BASE.ATTACKER or BASE.CASE["response"] == "reliable":
        async for response in ORIGINAL_JOIN_RESPONSE(self, request, context):
            yield response
        return
    mm = self._matchmaking
    VISIT_NUMBER += 1
    visit_id = f"{os.getpid()}:visit{VISIT_NUMBER}"
    detail = dict(visit_id=visit_id, rpc_id=SERVER_JOIN_RPC.get(),
                  leader_step_id=getattr(mm.potential_leaders, "_s25_step_id", None),
                  leader_search_id=getattr(mm.potential_leaders, "_s25_search_id", None),
                  request_expiration=request.expiration, delta=BASE.CASE["hold_seconds"])
    registered = False
    try:
        async with mm.lock_request_join_group:
            reason = mm._check_reasons_to_reject(request, context)
            if reason is not None:
                emit("unreliable_reject", self.peer_id, context.remote_id, **detail,
                     code=BASE.averaging_pb2.MessageCode.Name(reason.code))
                yield reason
                return
            mm.current_followers[context.remote_id] = request
            registered = True
            emit("unreliable_accept", self.peer_id, context.remote_id, **detail)
            yield BASE.averaging_pb2.MessageFromLeader(code=BASE.averaging_pb2.ACCEPTED)
        # The outer delay wrapper has forwarded ACCEPTED before resuming here.
        emit("unreliable_hold_start", self.peer_id, context.remote_id, **detail)
        await asyncio.sleep(BASE.CASE["hold_seconds"])
        emit("unreliable_disband", self.peer_id, context.remote_id, **detail)
        yield BASE.averaging_pb2.MessageFromLeader(code=BASE.averaging_pb2.GROUP_DISBANDED)
    except BaseException as exc:
        emit("unreliable_cancel", self.peer_id, context.remote_id, **detail, error=type(exc).__name__)
        raise
    finally:
        if registered:
            mm.current_followers.pop(context.remote_id, None)
            mm.follower_was_discarded.set()
            emit("unreliable_release", self.peer_id, context.remote_id, **detail)


def install_runtime():
    global BASE, OBS
    if OBS is not None:
        return OBS
    import step25_choice_measurement as observation
    OBS, BASE = observation, observation.base
    original = BASE.PotentialLeaders._declare_averager_periodically

    async def publisher(self, step, key_manager):
        if BASE.ATTACKER and BASE.CASE["mode"] == "once":
            return await publish_once(self, step, key_manager)
        return await original(self, step, key_manager)

    def write_config(path, value):
        if Path(path).name == "config.json" and "peer_ids" in value:
            d = value["case"]["one_way_delay_seconds"]
            value = dict(value, experiment="step28", instrumentation="detailed", attempt_unit="step_start",
                         network_delay_injected=d > 0, one_way_delay_seconds=d,
                         request_timeout=3, allow_state_sharing=False, event_transport="batched",
                         trace_batch_size=observation.BATCH_SIZE, duplicate_read_views=False,
                         control_delay_model="remote find/store request+response; join request+each response",
                         cached_reads_delayed=False, tensor_transfer_delayed=False,
                         once_unit="one active publication per native SEARCH; withdrawals separate",
                         response_policy=value["case"]["response"],
                         leadership_latency_origin="step_start", harm_timeout_cap=12,
                         clock_monitor=True, clock_jump_limit_seconds=CLOCK_JUMP_LIMIT,
                         submission_cutoff_checked_per_peer=True)
        atomic_json(path, value)

    observation.sweep.write_json = write_config
    BASE.PotentialLeaders._declare_averager_periodically = publisher
    BASE.DHTProtocol.rpc_find = wrap_unary(BASE.DHTProtocol.rpc_find, "find")
    BASE.DHTProtocol.rpc_store = wrap_unary(BASE.DHTProtocol.rpc_store, "store")
    global ORIGINAL_JOIN_RESPONSE
    ORIGINAL_JOIN_RESPONSE = BASE.RefreshAverager.rpc_join_group
    # ServicerBase discovers protobuf types from annotations. Resolve them in
    # the original method's globals before installing a research-local handler.
    from typing import get_type_hints
    hints = get_type_hints(ORIGINAL_JOIN_RESPONSE)
    unreliable_join.__annotations__ = hints
    join_handler = wrap_join(unreliable_join)
    # Python 3.14 wraps can replace lazy annotation metadata; assign the final
    # registered wrapper explicitly rather than relying on metadata copying.
    join_handler.__annotations__ = hints
    BASE.RefreshAverager.rpc_join_group = join_handler
    native_assemble = BASE.Matchmaking.leader_assemble_group

    async def deadline_assemble(self):
        expiration = self.potential_leaders.declared_expiration_time
        emit("assembly_deadline_state", self.peer_id,
             leader_expiration=expiration if math.isfinite(expiration) else None,
             step_id=observation.sweep.user_data(self.step_control)["step_id"])
        if BASE.ATTACKER and BASE.CASE["response"] == "unreliable":
            raise AssertionError("unreliable X must never assemble a group")
        return await native_assemble(self)

    BASE.Matchmaking.leader_assemble_group = deadline_assemble
    import step28_workload
    observation.sweep.run_trial = step28_workload.run_trial
    import multiprocessing as mp
    mp.set_start_method("fork", force=True)
    BASE.torch.set_num_threads(1)
    return observation


def snapshot(suite):
    observation = install_runtime()
    observation.snapshot(suite)
    provenance = read_json(suite/"provenance.json")
    # Step25 snapshot also captures native source checks; extend rather than edit it.
    for name in FROZEN_FILES:
        shutil.copyfile(ROOT/name, suite/"harness"/name)
        provenance["harness_sha256"][name] = sha256(ROOT/name)
    source = Path(provenance["source"])
    for relative in ("p2p/servicer.py", "p2p/p2p_daemon.py", "dht/dht.py"):
        if sha256(source/relative) != sha256(ROOT.parents[1]/"hivemind"/relative):
            raise ValueError(f"RPC registration runtime source differs: {relative}")
        provenance["source_sha256"][relative] = sha256(source/relative)
    runtime_paths = [Path(sys.executable).resolve(), source/"hivemind_cli/p2pd"]
    # Installed package RECORDs lock the package manifests; pip freeze and versions
    # additionally remain in provenance, alongside the exact native source hashes.
    import importlib.metadata
    for dist in importlib.metadata.distributions():
        for entry in dist.files or ():
            if str(entry).endswith(".dist-info/RECORD"):
                runtime_paths.append(Path(dist.locate_file(entry)).resolve())
    provenance["runtime_files_sha256"] = {str(p): sha256(p) for p in runtime_paths if p.is_file()}
    provenance["experiment"] = "step28"
    provenance["ancestor_script_sha256"] = provenance["script_sha256"]
    provenance["script_sha256"] = sha256(__file__)
    provenance["observation_engine"] = "Step28 response/delay wrappers over frozen Step25 hooks; Step28-local Step24 workload with cutoff/clock monitoring"
    atomic_json(suite/"provenance.json", provenance)


def validate_sources(suite, provenance):
    suite = Path(suite)
    hashes = provenance.get("harness_sha256", {})
    if not set(FROZEN_FILES).issubset(hashes):
        raise ValueError("missing frozen harness hashes")
    for name, digest in hashes.items():
        for path in (ROOT/name, suite/"harness"/name):
            if not path.is_file() or sha256(path) != digest:
                raise ValueError(f"frozen source drift: {path}")
    source = Path(provenance["source"])
    for relative, digest in provenance["source_sha256"].items():
        for path in (source/relative, ROOT.parents[1]/"hivemind"/relative):
            if not path.is_file() or sha256(path) != digest:
                raise ValueError(f"native source drift: {path}")
    for path, digest in provenance.get("runtime_files_sha256", {}).items():
        if not Path(path).is_file() or sha256(path) != digest:
            raise ValueError(f"runtime drift: {path}")


def trial_name(job):
    return f"{job['case']['name']}_s{job['seed']}"


def validate_trial_config(config, job):
    if any(config.get(k) != job[k] for k in ("case", "seed", "duration")):
        raise ValueError("trial config differs from manifest")
    expected = dict(experiment="step28", outcome="grouping", instrumentation="detailed",
                    timeout=12, request_timeout=3, tensor_size=128, honest_min=2, target=4,
                    honest_ad_lifetime=5, allreduce_timeout=6, store_delay=0, group_bits="",
                    allow_state_sharing=False, scheduled_time="native default", training_delay=TRAINING,
                    network="eight-peer loopback", attempt_unit="step_start", event_transport="batched",
                    trace_batch_size=32, duplicate_read_views=False, cached_reads_delayed=False,
                    tensor_transfer_delayed=False, one_way_delay_seconds=job["case"]["one_way_delay_seconds"],
                    network_delay_injected=job["case"]["one_way_delay_seconds"] > 0)
    for key, value in expected.items():
        if config.get(key) != value:
            raise ValueError(f"trial control differs: {key}")
    for key, value in dict(response_policy=job["case"]["response"],
                           leadership_latency_origin="step_start", harm_timeout_cap=12,
                           clock_monitor=True, clock_jump_limit_seconds=CLOCK_JUMP_LIMIT,
                           submission_cutoff_checked_per_peer=True,
                           once_unit="one active publication per native SEARCH; withdrawals separate").items():
        if config.get(key) != value:
            raise ValueError(f"Step 28 outcome control differs: {key}")
    ids = config.get("peer_ids", {})
    names = {"X", *(f"H{i}" for i in range(7))}
    if set(ids) != names or len(set(ids.values())) != 8:
        raise ValueError("expected eight distinct peer identities")
    rng = random.Random(job["seed"])
    honest = [rng.uniform(0., 3.) for _ in range(7)]
    offsets = dict(X=rng.uniform(0., 3.), **{f"H{i}": value for i, value in enumerate(honest)})
    if config.get("initial_offsets") != offsets:
        raise ValueError("seeded launch offsets differ")


def validate_trace_closed(path):
    closed = False
    with Path(path).open() as stream:
        for line in stream:
            if not line.strip():
                continue
            event = json.loads(line)
            if closed or not isinstance(event, dict):
                raise ValueError("invalid event or data after trace closure")
            closed = event.get("kind") == "trace_closed"
    if not closed:
        raise ValueError("completed trial lacks trace_closed")


def inspect_suite(suite_dir, verify_sources=True, analyze_fn=None):
    suite = Path(suite_dir).resolve()
    manifest, provenance = read_json(suite/"suite.json"), read_json(suite/"provenance.json")
    if (manifest.get("experiment") != "step28" or manifest.get("jobs") != make_jobs(manifest.get("phase"))
            or manifest.get("training_delay") != TRAINING or manifest.get("order_seed") != 280028
            or manifest.get("outcome") != "grouping" or manifest.get("instrumentation") != "detailed"):
        raise ValueError("manifest differs from the exact preliminary design")
    if verify_sources:
        validate_sources(suite, provenance)
    md, pd = sha256(suite/"suite.json"), sha256(suite/"provenance.json")
    prior = read_json(suite/"checkpoint.json") if (suite/"checkpoint.json").exists() else None
    if prior and (prior["suite_sha256"] != md or prior["provenance_sha256"] != pd):
        raise ValueError("checkpoint metadata drift")
    prior_jobs = {r["trial_dir"]: r for r in (prior or {}).get("jobs", [])}
    rows = []
    for job in manifest["jobs"]:
        name = trial_name(job)
        path = suite/name
        row = dict(job=job, trial_dir=name, status="pending")
        previous = prior_jobs.get(name, {})
        if previous.get("status") == "completed":
            for filename, digest in previous["file_sha256"].items():
                if not (path/filename).is_file() or sha256(path/filename) != digest:
                    raise ValueError(f"completed evidence changed: {name}/{filename}")
        if previous.get("archives"):
            row["archives"] = previous["archives"]
            for archive in row["archives"]:
                for filename, digest in archive["file_sha256"].items():
                    if sha256(Path(archive["path"])/filename) != digest:
                        raise ValueError("interrupted archive drift")
        if path.exists():
            if not (path/"completion.json").exists():
                row["status"] = "interrupted"
                halt = read_json(suite/"halt.json") if (suite/"halt.json").exists() else {}
                interrupted = halt.get("trial") == name and halt.get("interrupted") is True
                if interrupted:
                    pass  # Preserve and rerun even if interruption reached partial analysis.
                elif (path/"control_analysis.json").exists() and not read_json(path/"control_analysis.json").get("integrity_pass"):
                    row.update(status="blocked", error="closed trial failed integrity")
                elif (path/"summary.json").exists():
                    try:
                        summary = read_json(path/"summary.json")
                        halt = read_json(suite/"halt.json") if (suite/"halt.json").exists() else {}
                        interrupted = halt.get("trial") == name and halt.get("interrupted") is True
                        if (summary.get("error") or summary.get("pending")) and not interrupted:
                            row.update(status="blocked", error="closed trial reports a controller/drain failure")
                    except ValueError:
                        pass  # An unfinished JSON write is an interrupted trial.
            else:
                try:
                    completion = read_json(path/"completion.json")
                    if (set(completion["file_sha256"]) != {*RAW_FILES, "analysis.json", "control_analysis.json"}
                            or completion.get("all_peers_shutdown") is not True):
                        raise ValueError("completion marker lacks full post-shutdown evidence")
                    for filename, digest in completion["file_sha256"].items():
                        if sha256(path/filename) != digest:
                            raise ValueError(f"trial evidence changed: {filename}")
                    config = read_json(path/"config.json")
                    validate_trial_config(config, job)
                    validate_trace_closed(path/"events.jsonl")
                    summary = read_json(path/"summary.json")
                    if summary.get("pending") or summary.get("error"):
                        raise ValueError("trial has unfinished peers or controller error")
                    result = analyze_fn(path) if analyze_fn else read_json(path/"control_analysis.json")
                    if not result.get("integrity_pass"):
                        raise ValueError("trial integrity failed")
                    row.update(status="completed", file_sha256={f: sha256(path/f) for f in (*RAW_FILES, "analysis.json", "control_analysis.json", "completion.json")})
                except (KeyError, OSError, ValueError) as exc:
                    row.update(status="blocked", error=str(exc))
        rows.append(row)
    counts = {s: sum(r["status"] == s for r in rows) for s in ("completed", "pending", "interrupted", "blocked")}
    return dict(experiment="step28", suite=str(suite), suite_sha256=md, provenance_sha256=pd,
                phase=manifest["phase"], jobs=rows, counts=counts, valid=not counts["blocked"])


def stop_requested(suite_dir):
    return (Path(suite_dir)/"STOP_AFTER_CURRENT").exists()


def archive_interrupted(suite_dir, trial_dir):
    suite = Path(suite_dir)
    path = suite/Path(trial_dir).name
    archive_root = suite.parent/f"{suite.name}_interrupted"
    archive_root.mkdir(exist_ok=True)
    target = archive_root/f"{path.name}_{uuid.uuid4().hex}"
    hashes = {str(p.relative_to(path)): sha256(p) for p in path.rglob("*") if p.is_file()}
    path.rename(target)
    return dict(path=str(target.resolve()), file_sha256=hashes)


def verify_trial_shutdown(existing_children):
    """Reap this trial's actors, never touch children that predated the trial."""
    import multiprocessing as mp
    unexpected = []
    for child in mp.active_children():
        if child.pid in existing_children:
            continue
        child.join(timeout=1.)
        if child.is_alive():
            unexpected.append(child.pid)
            # Cleanup on an exceptional path; do not count the trial as valid.
            try:
                child.shutdown()
            except Exception:
                pass
            child.join(timeout=1.)
            if child.is_alive():
                child.terminate()
                child.join(timeout=3.)
            if child.is_alive():
                child.kill()
                child.join(timeout=3.)
            if child.is_alive():
                raise RuntimeError(f"trial actor {child.pid} is still alive; unsafe to suspend")
    if unexpected:
        raise RuntimeError(f"trial required additional shutdown cleanup: {unexpected}; preserved, integrity stop")


def run_suite(suite_dir):
    suite = Path(suite_dir).resolve()
    with (suite/".controller.lock").open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        state = inspect_suite(suite)
        if not state["valid"]:
            raise ValueError(f"blocked trial(s): {state['counts']}")
        atomic_json(suite/"checkpoint.json", state)
        if stop_requested(suite):
            print("STOP_AFTER_CURRENT is present; no peers started. Safe to suspend.", flush=True)
            return
        observation = install_runtime()
        from analyze_step28_confirmation import analyse_trial, export_suite
        for index, row in enumerate(state["jobs"], 1):
            if row["status"] == "completed":
                print(f"SKIP {index}/{len(state['jobs'])} {row['trial_dir']}: verified completed", flush=True)
                continue
            if stop_requested(suite):
                break
            if row["status"] == "interrupted":
                archive = archive_interrupted(suite, row["trial_dir"])
                row.setdefault("archives", []).append(archive)
                row["status"] = "pending"
                atomic_json(suite/"checkpoint.json", state)
            job, path = row["job"], suite/row["trial_dir"]
            validate_sources(suite, read_json(suite/"provenance.json"))
            state["active_trial"] = path.name
            state["controller_pid"] = os.getpid()
            atomic_json(suite/"checkpoint.json", state)
            print(f"START {index}/{len(state['jobs'])} {path.name} {json.dumps(job['case'], sort_keys=True)}", flush=True)
            try:
                import multiprocessing as mp
                existing_children = {child.pid for child in mp.active_children()}
                try:
                    native = observation.sweep.run_trial(job["case"], job["seed"], path, job["duration"], outcome="grouping")
                finally:
                    verify_trial_shutdown(existing_children)
                if "clock discontinuity" in (read_json(path/"summary.json").get("error") or ""):
                    raise ClockDiscontinuity("clock discontinuity; archive and rerun this trial on resume")
                result = analyse_trial(path)
                atomic_json(path/"control_analysis.json", result)
                if observation.sweep.trial_should_halt(native, "grouping") or not result["integrity_pass"]:
                    raise ValueError(f"trial integrity failed: {result.get('integrity_errors', result)}")
                complete = {f: sha256(path/f) for f in (*RAW_FILES, "analysis.json", "control_analysis.json")}
                atomic_json(path/"completion.json", dict(file_sha256=complete, all_peers_shutdown=True,
                            finished_utc=datetime.now(timezone.utc).isoformat()))
                row.update(status="completed", file_sha256={**complete, "completion.json": sha256(path/"completion.json")})
                state["active_trial"] = None
                state["counts"] = {s: sum(r["status"] == s for r in state["jobs"]) for s in ("completed", "pending", "interrupted", "blocked")}
                atomic_json(suite/"checkpoint.json", state)
                metrics = result["summary_metrics"]
                print(f"DONE {index}/{len(state['jobs'])} {path.name}: "
                      f"groups={metrics['total_groups']}, X-led groups={metrics['x_led_groups']}, "
                      f"X-led attempts={metrics['x_led_attempts']}/{metrics['x_submitted_attempts']}, "
                      f"led within 2s={metrics['x_led_by_2s_count']}, "
                      f"X capped wait={metrics['x_mean_capped_latency_seconds']}s, "
                      f"honest X-led memberships={metrics['honest_first_group_x_led_attempts']}/"
                      f"{metrics['honest_submitted_attempts']}; integrity pass; all peers shut down", flush=True)
            except BaseException as exc:
                all_shutdown = not any(child.pid not in existing_children for child in mp.active_children())
                raw_summary = read_json(path/"summary.json") if (path/"summary.json").exists() else {}
                interrupted_clock = "clock discontinuity" in (raw_summary.get("error") or "")
                atomic_json(suite/"halt.json", dict(trial=path.name, error=f"{type(exc).__name__}: {exc}",
                            all_peers_shutdown=all_shutdown, interrupted=interrupted_clock or isinstance(
                                exc, (KeyboardInterrupt, SystemExit, ClockDiscontinuity))))
                raise
        report = export_suite(suite)
        all_completed = state["counts"]["completed"] == len(state["jobs"])
        if all_completed and (not report.get("complete") or not report.get("integrity_pass")):
            raise ValueError(f"complete suite failed final validation: {report.get('integrity_errors')}")
        if all_completed:
            from verify_step28_confirmation import verify_suite
            verification = verify_suite(suite)
            atomic_json(suite/"raw_verification.json", verification)
            if not verification.get("pass"):
                raise ValueError("independent raw verification failed; evidence preserved")
        print(f"Saved {suite}: {state['counts']}; safe to suspend; report {report.get('integrity_pass')}", flush=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=("smoke", "confirm"))
    parser.add_argument("--label")
    parser.add_argument("--gate", type=Path)
    parser.add_argument("--resume", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    if args.resume:
        if args.phase or args.label or args.gate:
            parser.error("--resume uses the existing manifest; omit --phase/--label/--gate")
        if args.dry_run:
            print(json.dumps(inspect_suite(args.resume), indent=2))
        else:
            run_suite(args.resume)
        return
    if not args.phase:
        parser.error("provide --phase or --resume")
    if args.dry_run:
        print(json.dumps(dict(experiment="step28", phase=args.phase, jobs=make_jobs(args.phase)), indent=2))
        return
    if not args.label or not re.fullmatch(r"[A-Za-z0-9_-]+", args.label):
        parser.error("provide a distinct alphanumeric/underscore/hyphen --label")
    if args.phase == "confirm":
        if not args.gate:
            parser.error("confirm requires a complete matching smoke suite via --gate")
        gate = inspect_suite(args.gate)
        if not gate["valid"] or gate["phase"] != "smoke" or gate["counts"]["completed"] != 2:
            parser.error("smoke gate incomplete or invalid")
        gate_report = read_json(args.gate/"summary.json")
        if (not gate_report.get("complete") or not gate_report.get("integrity_pass") or
                not gate_report.get("smoke_gate_pass")):
            parser.error("smoke gate lacks validated delayed advertisement/join traffic")
        from verify_step28_confirmation import verify_suite
        if not verify_suite(args.gate).get("pass"):
            parser.error("smoke gate failed independent raw verification")
    suite = ROOT/"results"/f"step28_{args.phase}_{args.label}"
    suite.mkdir()
    snapshot(suite)
    atomic_json(suite/"suite.json", dict(experiment="step28", phase=args.phase, jobs=make_jobs(args.phase),
                training_delay=TRAINING, order_seed=280028, gate=str(args.gate.resolve()) if args.gate else None,
                outcome="grouping", instrumentation="detailed", preliminary=True))
    run_suite(suite)


if __name__ == "__main__":
    main()
