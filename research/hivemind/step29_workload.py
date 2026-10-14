"""Step29-local workload for exact native advertisement controls.

Native matchmaking, workload RNGs, tensors, and teardown behavior are preserved.
"""
import json
import math
import multiprocessing as mp
import random
import statistics
import time
import step25_choice_measurement as observation
from step29_policies import check_clock

sweep = observation.sweep
base = sweep.base
NAMES, MU, SIGMA, TRAINING = sweep.NAMES, sweep.MU, sweep.SIGMA, sweep.TRAINING
TimingAverager = sweep.TimingAverager


def emit(*args, **kwargs):
    return sweep.emit(*args, **kwargs)


def write_json(*args, **kwargs):
    return sweep.write_json(*args, **kwargs)


def clock_sample():
    return dict(clock_monotonic=time.monotonic(), clock_wall=time.time(),
                clock_boottime=time.clock_gettime(time.CLOCK_BOOTTIME))


def run_trial(case, seed, path, duration, outcome="averaging"):
    path.mkdir()
    base.EVENTS = mp.Queue()
    base.CASE, base.ATTACKER, base.STORE_DELAY = case, False, 0.0
    dhts, peers, controls, outcomes = [], {}, {}, []
    rngs = {name: random.Random(seed*100+index) for index, name in enumerate(NAMES)}
    offsets = base.launch_offsets(seed, "staggered")
    config = dict(case=case, seed=seed, duration=duration, outcome=outcome, timeout=12, tensor_size=128,
                  initial_offsets=offsets, training_delay=TRAINING, honest_min=2, target=4,
                  honest_ad_lifetime=5, x_min_matchmaking_time=case["lifetime"], x_min_group_size=2, allreduce_timeout=6, store_delay=0,
                  delay_location="after observed FINISHED, before next step()",
                  group_bits="", scheduled_time="native default", network="eight-peer loopback")
    raw_file = (path/"events.jsonl").open("w")
    pending = []
    error = None
    first_clock = clock_sample()
    last_clock_emit = float('-inf')
    last_progress = float('-inf')
    seen_groups, x_groups, x_steps = set(), set(), set()

    def collect():
        events = base.drain(base.EVENTS, quiet=0.002, cap=0.02)
        for event in events:
            raw_file.write(json.dumps(event, allow_nan=False)+"\n")
            if event['kind'] == 'assembled':
                seen_groups.add(event['detail']['group_id'])
                if event['peer'] == identities.get('X'):
                    x_groups.add(event['detail']['group_id'])
            elif event['kind'] == 'step_start' and event['peer'] == identities.get('X'):
                x_steps.add(event['detail']['step_id'])
        raw_file.flush()
        return len(events)

    try:
        identities = {}
        emit('controller_clock', **first_clock)
        dhts = base.make_dhts(seed)
        common = dict(prefix=f"s24_{seed}", target_group_size=4, min_matchmaking_time=5,
                      request_timeout=3, allreduce_timeout=6, allow_state_sharing=False, start=True)
        for index, name in enumerate(NAMES):
            base.ATTACKER = name == "X"
            value = 7 if name == "X" else int(name[1:])
            peers[name] = TimingAverager([base.torch.full((128,), float(value))],
                                         dhts[7 if name == "X" else index-1],
                                         min_group_size=2, **dict(common, min_matchmaking_time=case["lifetime"] if name == "X" else 5))
        base.ATTACKER = False
        identities = {name: str(peer.peer_id) for name, peer in peers.items()}
        config["peer_ids"] = identities
        config["runtime_matchmaking_config"] = {
            identities[name]: dict(min_matchmaking_time=case["lifetime"] if name == "X" else 5,
                                   min_group_size=2, target_group_size=4, request_timeout=3,
                                   behavior=case["behavior"] if name == "X" else "normal",
                                   publication_policy=case["publication_policy"] if name == "X" else "native")
            for name in NAMES}
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
            current_clock = clock_sample()
            clock_errors = check_clock([first_clock, current_clock])
            if now-last_clock_emit >= 1. or clock_errors:
                emit('controller_clock', **current_clock)
                last_clock_emit = now
            if clock_errors:
                error = '; '.join(clock_errors)
                emit('clock_discontinuity', error=error, **current_clock)
                break
            if now-last_progress >= 10.:
                print(f"LIVE {case['name']}_s{seed}: {now-started:.1f}s, "
                      f"groups={len(seen_groups)}, X-led groups={len(x_groups)}, "
                      f"X submitted={len(x_steps)}, active steps={len(controls)}", flush=True)
                last_progress = now
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
                    if time.monotonic() >= started+duration:
                        break
                    if name not in controls and now >= ready[name]:
                        sequences[name] += 1
                        value = float((7 if name == "X" else int(name[1:]))+10*sequences[name])
                        with peers[name].get_tensors() as tensors:
                            tensors[0].fill_(value)
                        data = dict(name=name, sequence=sequences[name], step_id=f"{name}:{sequences[name]}", value=value)
                        if time.monotonic() >= started+duration:
                            sequences[name] -= 1
                            break
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
        final_clock = clock_sample()
        emit('controller_clock', **final_clock)
        clock_errors = check_clock([first_clock, final_clock])
        if clock_errors:
            error = '; '.join(clock_errors)
            emit('clock_discontinuity', error=error, **final_clock)
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
