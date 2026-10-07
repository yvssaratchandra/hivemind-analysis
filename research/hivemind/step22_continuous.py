"""Repeated local averaging with independent, seeded simulated training delays.

Uses step22's research-only publisher and tracing, without modifying library files.
The simulated delays are a workload model, not measurements of real training.
"""
import argparse
import hashlib
import json
import math
import multiprocessing as mp
import random
import statistics
import time
from pathlib import Path

import step22_refresh_leadership as base
from hivemind.utils.serializer import MSGPackSerializer


ORIGINAL_ASSEMBLE = base.Matchmaking.leader_assemble_group


async def logged_assemble(self):
    group = await ORIGINAL_ASSEMBLE(self)
    own = MSGPackSerializer.loads(self.step_control.data_for_gather)
    user = MSGPackSerializer.loads(own[2])
    base.record("assembled", self.peer_id, group_id=group.group_id.hex(),
                roster=[str(peer) for peer in group.peer_ids], user=user)
    return group


base.Matchmaking.leader_assemble_group = logged_assemble


def run(case, seed, path, duration, timeout=12):
    path.mkdir()
    base.EVENTS = mp.Queue()
    base.CASE = case
    base.ATTACKER = False
    dhts, peers, controls, raw, outcomes = [], {}, {}, [], []
    sequences = {}
    rngs = {name: random.Random(seed*100+index) for index, name in enumerate(["X"]+[f"H{i}" for i in range(7)])}
    offsets = base.launch_offsets(seed, "staggered")
    config = dict(case=case, seed=seed, duration=duration, timeout=timeout,
                  initial_offsets=offsets, training_delay="lognormal mean 1.5s CV 0.6, clipped [0.1,4]",
                  attacker_reentry=0.05, honest_min=2, target=case.get("target", 4), honest_ad_lifetime=5,
                  store_delay=base.STORE_DELAY, tensor_size=128, allreduce_timeout=6)
    raw_file = (path/"events.jsonl").open("w")

    def collect():
        events = base.drain(base.EVENTS, quiet=0.001, cap=0.01)
        raw.extend(events)
        for event in events:
            raw_file.write(json.dumps(event, sort_keys=True)+"\n")
        raw_file.flush()

    def training_delay(name):
        if name == "X":
            return 0.05
        sigma = math.sqrt(math.log(1+0.6**2))
        return min(4.0, max(0.1, rngs[name].lognormvariate(math.log(1.5)-sigma*sigma/2, sigma)))

    try:
        dhts = base.make_dhts(seed)
        common = dict(prefix=f"s22continuous_{seed}", target_group_size=case.get("target", 4),
                      min_matchmaking_time=5, request_timeout=3, allreduce_timeout=6,
                      allow_state_sharing=False, start=True)
        base.ATTACKER = True
        peers["X"] = base.RefreshAverager([base.torch.full((128,), 7.0)], dhts[7],
                                           min_group_size=case.get("minimum", 4), **common)
        base.ATTACKER = False
        for i in range(7):
            peers[f"H{i}"] = base.RefreshAverager([base.torch.full((128,), float(i))], dhts[i],
                                                  min_group_size=2, **common)
        identities = {name: str(peer.peer_id) for name, peer in peers.items()}
        config["peer_ids"] = identities
        (path/"config.json").write_text(json.dumps(config, indent=2)+"\n")
        time.sleep(0.5)
        started = time.monotonic()
        ready = {name: started+offsets[name] for name in peers}
        sequences = {name: 0 for name in peers}
        hard_stop = started+duration+timeout+8
        while time.monotonic() < hard_stop:
            collect()
            now = time.monotonic()
            for name, control in list(controls.items()):
                if not control.done() or control.stage != base.AveragingStage.FINISHED:
                    continue
                row = dict(name=name, sequence=sequences[name], finished=now-started,
                           status="cancelled" if control.cancelled() else "failed")
                if not control.cancelled():
                    try:
                        result = control.result()
                        if isinstance(result, dict):
                            members = {str(peer): data for peer, data in result.items()}
                            expected = statistics.mean(data["value"] for data in members.values())
                            with peers[name].get_tensors() as tensors:
                                actual = tensors[0].clone()
                            row.update(status="completed", members=members, expected=expected,
                                       actual=actual.tolist(),
                                       correct=base.torch.allclose(actual, base.torch.full_like(actual, expected), atol=1e-4))
                        else:
                            row["status"] = "no_group"
                    except Exception as exc:
                        row["error"] = type(exc).__name__
                outcomes.append(row)
                base.record("step_result", identities[name], **row)
                del controls[name]
                ready[name] = now+training_delay(name)
            if now < started+duration:
                for name in peers:
                    if name not in controls and now >= ready[name]:
                        sequences[name] += 1
                        # Model the new local update, using the public shared-tensor lock after FINISHED.
                        value = float((7 if name == "X" else int(name[1:]))+10*sequences[name])
                        with peers[name].get_tensors() as tensors:
                            tensors[0].fill_(value)
                        data = dict(name=name, sequence=sequences[name], value=value)
                        base.record("step_start", identities[name], **data, offset=now-started)
                        controls[name] = peers[name].step(gather=data, wait=False, allow_retries=True, timeout=timeout)
            elif not controls:
                break
            time.sleep(0.005)
        pending = list(controls)
        for control in controls.values():
            control.cancel()
        if pending:
            raise RuntimeError(f"steps did not finish within drain allowance: {pending}")
        collect()
        # A group is verified only when every named step completed with the correct tensor.
        indexed = {(row["name"], row["sequence"]): row for row in outcomes}
        assembled = {event["detail"]["group_id"]: event for event in raw if event["kind"] == "assembled"}
        groups = []
        for group_id, event in assembled.items():
            user = event["detail"]["user"]
            leader = indexed.get((user["name"], user["sequence"]))
            members = leader.get("members", {}) if leader else {}
            same_roster = set(members) == set(event["detail"]["roster"])
            verified = bool(same_roster and all(indexed.get((data["name"], data["sequence"]), {}).get("correct", False)
                                                and indexed[(data["name"], data["sequence"])].get("members") == members
                                                for data in members.values()))
            groups.append(dict(group_id=group_id, leader=user["name"], sequence=user["sequence"],
                               size=len(event["detail"]["roster"]), verified=verified, members=members))
        forbidden = [event for event in raw if event["kind"].startswith("forbidden_")]
        if forbidden:
            raise RuntimeError("attacker read or followed another peer")
        result = dict(case=case["name"], seed=seed, duration=duration, elapsed=time.monotonic()-started,
                      outcomes=outcomes, groups=groups,
                      x_verified_leads=sum(group["leader"] == "X" and group["verified"] for group in groups),
                      all_verified_groups=sum(group["verified"] for group in groups),
                      honest_completed=sum(row["name"] != "X" and row["status"] == "completed" for row in outcomes),
                      honest_verified=sum(row["name"] != "X" and row.get("correct", False) for row in outcomes),
                      honest_steps=sum(row["name"] != "X" for row in outcomes),
                      degraded_steps=sum(row["status"] == "completed" and not row["correct"] for row in outcomes),
                      x_ad_attempts=sum(event["kind"] == "ad_started" and event["peer"] == identities["X"]
                                        and event["detail"]["active"] for event in raw),
                      forbidden=len(forbidden))
        (path/"summary.json").write_text(json.dumps(result, indent=2)+"\n")
        print(f"{case['name']} s{seed}: verified X groups={result['x_verified_leads']}/{result['all_verified_groups']} "
              f"honest={result['honest_verified']}/{result['honest_steps']} degraded={result['degraded_steps']} "
              f"ads={result['x_ad_attempts']}", flush=True)
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
        collect()
        raw_file.close()
        base.EVENTS.close()
        base.EVENTS = None
        base.ATTACKER = False


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cases", type=Path, required=True)
    parser.add_argument("--seeds", nargs="+", type=int, default=[2210])
    parser.add_argument("--duration", type=float, default=40)
    parser.add_argument("--store-delay", type=float, default=0)
    args = parser.parse_args()
    mp.set_start_method("fork", force=True)
    base.torch.set_num_threads(1)
    base.STORE_DELAY = args.store_delay
    args.output.mkdir(parents=True)
    provenance = base.provenance()
    provenance["continuous_script_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    (args.output/"provenance.json").write_text(json.dumps(provenance, indent=2)+"\n")
    cases = json.loads(args.cases.read_text())
    (args.output/"suite.json").write_text(json.dumps(dict(cases=cases, seeds=args.seeds,
                                                        duration=args.duration, store_delay=args.store_delay), indent=2)+"\n")
    jobs = [(case, seed) for seed in args.seeds for case in cases]
    random.Random(220023).shuffle(jobs)
    summaries = []
    for case, seed in jobs:
        summaries.append(run(case, seed, args.output/f"{case['name']}_s{seed}", args.duration))
        (args.output/"summaries.json").write_text(json.dumps(summaries, indent=2)+"\n")


if __name__ == "__main__":
    main()
