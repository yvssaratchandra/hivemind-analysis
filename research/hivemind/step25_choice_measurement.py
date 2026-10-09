"""Private Step 25: observe native choices and reads without changing matchmaking.

Run using the source-verified Step 24 environment. All peers use loopback only.
Existing Step 24 controls, workload, tensors and native timeout arguments remain.
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import contextvars
import hashlib
import json
import math
import multiprocessing as mp
import os
from pathlib import Path
import random
import re
import subprocess
import sys
import time

import step24_lifetime_sweep as sweep
from hivemind.utils.timed_storage import TimedStorage

base = sweep.base
ROOT = Path(__file__).resolve().parent
DETAIL = True
JOIN_CONTEXT = contextvars.ContextVar("step25_join", default=None)
ORIGINAL_WRITE = sweep.write_json
ORIGINAL_DRAIN = base.drain
ORIGINAL_BEGIN = base.PotentialLeaders.begin_search
ORIGINAL_PAUSE = base.PotentialLeaders.pause_search
ORIGINAL_TOP = TimedStorage.top
ORIGINAL_DELETE = TimedStorage.__delitem__
POP_CODE = base.PotentialLeaders.pop_next_leader.__code__
ORIGINAL_JOIN = base.Matchmaking._request_join_group
BUFFER = []
BUFFER_PID = os.getpid()
BATCH_SIZE = 32
READ_EVENTS = {"ad_read_start", "ad_read_end", "ad_view"}


def flush_events():
    global BUFFER
    if BUFFER:
        packet, BUFFER = BUFFER, []
        base.EVENTS.put({"step25_events": packet})


def emit(kind, peer="", other="", **detail):
    global BUFFER, BUFFER_PID
    if base.EVENTS is None:
        return None
    pid = os.getpid()
    if pid != BUFFER_PID:
        # Forked averagers have independent sender buffers. No parent rows are
        # replayed into a child's stream.
        BUFFER, BUFFER_PID = [], pid
    if DETAIL and kind in ("native_client_reply", "native_client_exception", "native_client_wait"):
        detail["join_id"] = JOIN_CONTEXT.get()
    sweep.EVENT_NUMBER += 1
    detail.setdefault("step_id", sweep.CURRENT_STEP)
    event = dict(kind=kind, peer=str(peer), other=str(other), pid=pid,
                 event_number=sweep.EVENT_NUMBER, monotonic=time.monotonic(),
                 wall_time=base.get_dht_time(), detail=detail)
    if DETAIL:
        BUFFER.append(event)
        # Dense read observations are transported together; every lifecycle,
        # selection, ad and closure event flushes immediately. Timestamps and
        # event numbers belong to observation time, not transport time.
        if len(BUFFER) >= BATCH_SIZE or kind not in READ_EVENTS:
            flush_events()
    else:
        base.EVENTS.put(event)
    return f"{pid}:{sweep.EVENT_NUMBER}"


def drain_events(events, **kwargs):
    rows = []
    for packet in ORIGINAL_DRAIN(events, **kwargs):
        if isinstance(packet, dict) and "step25_events" in packet:
            rows.extend(packet["step25_events"])
        else:
            rows.append(packet)
    return rows


def context(leaders):
    return dict(step_id=leaders._s25_step_id, search_id=leaders._s25_search_id,
                group_key=leaders._s25_key_manager.current_key)


@contextlib.asynccontextmanager
async def begin_search(self, step, key_manager, declare=True):
    if not DETAIL:
        async with ORIGINAL_BEGIN(self, step, key_manager, declare=declare):
            yield self
        return
    self._s25_ordinal = getattr(self, "_s25_ordinal", 0) + 1
    self._s25_step_id = sweep.user_data(step)["step_id"]
    self._s25_search_id = f"{self.peer_id}:{self._s25_step_id}:search{self._s25_ordinal}"
    self._s25_key_manager = key_manager
    key_manager._s25_leaders = self
    key_manager._s25_latest_read = None
    self._s25_selected = None
    emit("search_span_start", self.peer_id, **context(self))
    self._s25_enabled = False
    try:
        async with ORIGINAL_BEGIN(self, step, key_manager, declare=declare):
            self._s25_enabled = True
            emit("read_enabled", self.peer_id, **context(self))
            yield self
    finally:
        if self._s25_enabled:
            emit("read_disabled", self.peer_id, **context(self))
            self._s25_enabled = False
        emit("search_span_end", self.peer_id, **context(self))


@contextlib.asynccontextmanager
async def pause_search(self):
    if not DETAIL:
        async with ORIGINAL_PAUSE(self):
            yield
        return
    was_enabled = self._s25_enabled
    try:
        async with ORIGINAL_PAUSE(self):
            if was_enabled:
                self._s25_enabled = False
                emit("read_paused", self.peer_id, **context(self))
            yield
    finally:
        if was_enabled and self.running.is_set():
            self._s25_enabled = True
            emit("read_resumed", self.peer_id, **context(self))


async def read_ads(self, group_key, only_active):
    if not DETAIL:
        return await base.logged_read(self, group_key, only_active)
    leaders = self._s25_leaders
    if base.ATTACKER and base.CASE.get("mode") != "stock":
        emit("forbidden_attacker_read", leaders.peer_id)
        raise AssertionError("passive attacker read competitor advertisements")
    self._s25_read_number = getattr(self, "_s25_read_number", 0) + 1
    read_id = f"{os.getpid()}:read{self._s25_read_number}"
    detail = dict(context(leaders), group_key=group_key, read_id=read_id)
    started = time.monotonic()
    emit("ad_read_start", leaders.peer_id, **detail)
    native_started = time.monotonic()
    try:
        # The new end observation already preserves the complete native view.
        # Avoid serializing a duplicate legacy ad_view for each logical read.
        result = await base.ORIGINAL_READ(self, group_key, only_active)
    except BaseException as exc:
        emit("ad_read_end", leaders.peer_id, **detail, latency=time.monotonic()-started,
             native_await_latency=time.monotonic()-native_started,
             entries=[], error=type(exc).__name__, cancelled=isinstance(exc, asyncio.CancelledError))
        raise
    native_latency = time.monotonic()-native_started
    emit("ad_read_end", leaders.peer_id, **detail, latency=time.monotonic()-started,
         native_await_latency=native_latency,
         entries=[dict(peer=str(peer), expiration=expiration) for peer, expiration in result],
         error=None, cancelled=False)
    self._s25_latest_read = read_id
    return result


def queue_top(self):
    if DETAIL:
        frame = sys._getframe(1)
        if frame.f_code is POP_CODE:
            frame.f_locals["self"]._s25_top_wall = base.get_dht_time()
        del frame
    return ORIGINAL_TOP(self)


def queue_delete(self, key):
    # This is the native pop's final synchronous decision point. Reading .data
    # avoids the expiry pruning performed by .items(), .get() and .top().
    if DETAIL:
        frame = sys._getframe(1)
        if frame.f_code is POP_CODE:
            leaders = frame.f_locals["self"]
            assert self is leaders.leader_queue
            selected = frame.f_locals["entry"]
            own = leaders.declared_expiration_time
            ref = emit("native_pop_selected", leaders.peer_id, key, **context(leaders),
                       selected_expiration=selected.expiration_time,
                       own_expiration=own if math.isfinite(own) else None,
                       own_peer_bytes_hex=leaders.peer_id.to_bytes().hex(),
                       selected_peer_bytes_hex=key.to_bytes().hex(),
                       selection_top_wall=leaders._s25_top_wall,
                       read_id=leaders._s25_key_manager._s25_latest_read,
                       queue=[dict(peer=str(peer), expiration=value.expiration_time,
                                   peer_bytes_hex=peer.to_bytes().hex())
                              for peer, value in self.data.items()],
                       past_attempts=[dict(peer=str(peer), expiration=expiration)
                                      for peer, expiration in leaders.past_attempts])
            leaders._s25_selected = (key, ref)
        del frame
    return ORIGINAL_DELETE(self, key)


async def join_leader(self, leader):
    if not DETAIL:
        return await ORIGINAL_JOIN(self, leader)
    leaders = self.potential_leaders
    selected = leaders._s25_selected
    leaders._s25_selected = None
    self._s25_join_number = getattr(self, "_s25_join_number", 0) + 1
    join_id = f"{os.getpid()}:join{self._s25_join_number}"
    detail = dict(context(leaders), join_id=join_id,
                  origin="queue" if selected and selected[0] == leader else "redirect",
                  selection_event_ref=selected[1] if selected and selected[0] == leader else None)
    token = JOIN_CONTEXT.set(join_id)
    emit("join_attempt_start", self.peer_id, leader, **detail)
    error = None
    try:
        return await ORIGINAL_JOIN(self, leader)
    except BaseException as exc:
        error = type(exc).__name__
        raise
    finally:
        emit("join_attempt_end", self.peer_id, leader, **detail, error=error)
        JOIN_CONTEXT.reset(token)


def write_json(path, value):
    if path.name == "config.json" and "peer_ids" in value:
        value = dict(value, experiment="step25", instrumentation="detailed" if DETAIL else "minimal",
                     attempt_unit="step_start", network_delay_injected=False,
                     request_timeout=3, allow_state_sharing=False,
                     event_transport="batched" if DETAIL else "single",
                     trace_batch_size=BATCH_SIZE if DETAIL else 1,
                     duplicate_read_views=False if DETAIL else None)
    ORIGINAL_WRITE(path, value)


sweep.emit = base.record = emit
base.drain = drain_events
sweep.write_json = write_json
base.PotentialLeaders.begin_search = begin_search
base.PotentialLeaders.pause_search = pause_search
base.GroupKeyManager.get_averagers = read_ads
TimedStorage.top = queue_top
TimedStorage.__delitem__ = queue_delete
base.Matchmaking._request_join_group = join_leader


def snapshot(output):
    provenance = base.provenance()
    provenance.update(executable=sys.executable, argv=sys.argv,
                      packages=subprocess.check_output([sys.executable, "-m", "pip", "freeze"], text=True).splitlines())
    source = Path(base.hivemind.__file__).resolve().parent
    for relative in ("averaging/control.py", "utils/serializer.py", "averaging/allreduce.py",
                     "averaging/partition.py", "p2p/p2p_daemon_bindings/datastructures.py"):
        data = (source/relative).read_bytes()
        if data != (base.REPO/"hivemind"/relative).read_bytes():
            raise RuntimeError(f"runtime source differs: {relative}")
        provenance["source_sha256"][relative] = hashlib.sha256(data).hexdigest()
    (output/"harness").mkdir()
    names = [Path(__file__).name, "step24_lifetime_sweep.py", "step22_refresh_leadership.py",
             "analyze_step24_lifetime.py", "test_step25_harness.py"]
    if DETAIL:
        names.extend(["analyze_step25_choices.py", "test_step25_choices.py"])
    for name in names:
        data = (ROOT/name).read_bytes()
        (output/"harness"/name).write_bytes(data)
        provenance.setdefault("harness_sha256", {})[name] = hashlib.sha256(data).hexdigest()
    ORIGINAL_WRITE(output/"provenance.json", provenance)


def main(argv=None):
    global DETAIL
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=("pilot", "smoke", "confirm"), required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--gate", type=Path)
    parser.add_argument("--instrumentation", choices=("minimal", "detailed"), default="detailed")
    args = parser.parse_args(argv)
    if not re.fullmatch(r"[A-Za-z0-9_-]+", args.label):
        parser.error("label must contain letters, numbers, underscores or hyphens")
    DETAIL = args.instrumentation == "detailed"
    if not DETAIL and args.phase != "pilot":
        parser.error("minimal tracing is only for overhead pilots")
    if args.phase == "confirm":
        if args.gate is None:
            parser.error("confirmation requires a passing detailed smoke suite")
        from analyze_step25_choices import analyse_suite_choices
        checked = analyse_suite_choices(args.gate)
        gate_manifest = json.loads((args.gate/"suite.json").read_text())
        expected_jobs = [dict(case=case, seed=2500, duration=10) for case in sweep.policies()]
        job_key = lambda job: json.dumps(job, sort_keys=True)
        if (checked["phase"] != "smoke" or not checked.get("control_plane_pass") or
                gate_manifest.get("run_kind") != "smoke" or
                gate_manifest.get("instrumentation") != "detailed" or
                sorted(map(job_key, gate_manifest["jobs"])) != sorted(map(job_key, expected_jobs))):
            parser.error("invalid smoke gate")
        gate_provenance = json.loads((args.gate/"provenance.json").read_text())
        for name in (Path(__file__).name, "step24_lifetime_sweep.py", "step22_refresh_leadership.py",
                     "analyze_step24_lifetime.py", "analyze_step25_choices.py"):
            if hashlib.sha256((ROOT/name).read_bytes()).hexdigest() != gate_provenance["harness_sha256"].get(name):
                parser.error(f"smoke gate used different code: {name}")
        controls = dict(timeout=12, request_timeout=3, tensor_size=128, honest_min=2,
                        target=4, honest_ad_lifetime=5, allreduce_timeout=6, store_delay=0,
                        group_bits="", scheduled_time="native default", network="eight-peer loopback",
                        network_delay_injected=False, allow_state_sharing=False,
                        delay_location="after observed FINISHED, before next step()",
                        attempt_unit="step_start", experiment="step25", instrumentation="detailed",
                        event_transport="batched", trace_batch_size=BATCH_SIZE,
                        duplicate_read_views=False)
        for job in expected_jobs:
            config = json.loads((args.gate/f"{job['case']['name']}_s2500"/"config.json").read_text())
            for name, expected in controls.items():
                if config.get(name) != expected:
                    parser.error(f"smoke gate used different workload control: {name}")
    mp.set_start_method("fork", force=True)
    base.torch.set_num_threads(1)
    output = ROOT/"results"/f"step25_{args.phase}_{args.label}"
    output.mkdir()
    snapshot(output)
    seeds = [2498, 2499] if args.phase == "pilot" else [2500] if args.phase == "smoke" else list(range(2501, 2511))
    policies = sweep.policies()
    if args.phase == "pilot":
        policies = [case for case in policies if case["name"] in ("passive_stock", "range_0_1")]
    duration = 15 if args.phase == "pilot" else 10 if args.phase == "smoke" else 60
    jobs = [dict(case=case, seed=seed, duration=duration) for seed in seeds for case in policies]
    random.Random(250025).shuffle(jobs)
    ORIGINAL_WRITE(output/"suite.json", dict(phase="smoke" if args.phase == "pilot" else args.phase,
                   run_kind=args.phase, outcome="grouping", instrumentation=args.instrumentation,
                   jobs=jobs, gate=str(args.gate) if args.gate else None, order_seed=250025,
                   training_delay=sweep.TRAINING))
    for index, job in enumerate(jobs, 1):
        case, seed = job["case"], job["seed"]
        trial = output/f"{case['name']}_s{seed}"
        try:
            checked = sweep.run_trial(case, seed, trial, duration, outcome="grouping")
            if sweep.trial_should_halt(checked, "grouping"):
                raise RuntimeError("native control-plane integrity gate failed")
            if DETAIL:
                from analyze_step25_choices import analyse_trial_choices
                measured = analyse_trial_choices(trial)
                ORIGINAL_WRITE(trial/"choices.json", measured)
                if not measured.get("trace_integrity_pass") or not measured.get("step24_control_plane_pass"):
                    raise RuntimeError(f"choice/read observation integrity gate failed: {measured.get('trace_integrity_errors')}")
            print(f"{index}/{len(jobs)} {trial.name}: X-led {checked['x_assembled_leads']}, "
                  f"all groups {checked['total_assembled_groups']}; control pass", flush=True)
        except Exception as exc:
            ORIGINAL_WRITE(output/"halt.json", dict(trial=trial.name, checked_trials=index,
                           reason=f"{type(exc).__name__}: {exc}"))
            raise
    if DETAIL:
        from analyze_step25_choices import analyse_suite_choices
        checked = analyse_suite_choices(output)
    else:
        from analyze_step24_lifetime import analyse_suite
        checked = analyse_suite(output)
    ORIGINAL_WRITE(output/"analysis.json", checked)
    if not checked.get("control_plane_pass"):
        raise RuntimeError("complete suite failed control-plane or observation validation")
    print(f"Saved {output}", flush=True)


if __name__ == "__main__":
    main()
