"""Observe a renewed own-ad record beyond its initial expiration on eight local DHT peers."""
import argparse
import hashlib
import json
import math
import multiprocessing as mp
import time
from pathlib import Path

import step22_refresh_leadership as base


def run(case, seed, path, duration):
    path.mkdir()
    base.EVENTS = mp.Queue()
    dhts, raw, sends, probes = [], [], [], []
    output = (path/"events.jsonl").open("w")

    def collect():
        events = base.drain(base.EVENTS, quiet=0.001, cap=0.01)
        raw.extend(events)
        for event in events:
            output.write(json.dumps(event)+"\n")
        output.flush()

    try:
        dhts = base.make_dhts(seed)
        caller, observer = dhts[7], dhts[0]
        key = f"s22endurance_{seed}.0b"
        subkey = caller.peer_id.to_bytes()
        config = dict(case=case, seed=seed, duration=duration, key=key, peer_id=str(caller.peer_id),
                      probe_interval=0.1, store_delay=base.STORE_DELAY)
        (path/"config.json").write_text(json.dumps(config, indent=2)+"\n")
        started = time.monotonic()
        previous = float("-inf")
        next_send = next_probe = started
        while time.monotonic() < started+duration:
            collect()
            now = time.monotonic()
            if now >= next_send:
                sent_wall = base.get_dht_time()
                expiration = max(math.nextafter(previous, math.inf), sent_wall+case["lifetime"])
                previous = expiration
                sent_mono = time.monotonic()
                accepted = caller.store(key, True, subkey=subkey, expiration_time=expiration)
                row = dict(offset=sent_mono-started, sent_wall=sent_wall, expiration=expiration,
                           latency=time.monotonic()-sent_mono, accepted=bool(accepted))
                sends.append(row)
                base.record("endurance_send", caller.peer_id, **row)
                next_send = sent_mono+1/case["frequency"]
            if time.monotonic() >= next_probe:
                probe_started = time.monotonic()
                observed = observer.get(key, latest=True)
                record = observed.value.get(subkey) if observed is not None and isinstance(observed.value, dict) else None
                active = bool(record and record.value and record.expiration_time > base.get_dht_time())
                row = dict(offset=probe_started-started, latency=time.monotonic()-probe_started,
                           active=active, expiration=record.expiration_time if record else None)
                probes.append(row)
                base.record("endurance_probe", observer.peer_id, **row)
                next_probe = probe_started+0.1
            time.sleep(0.002)
        collect()
        assert all(after["expiration"] > before["expiration"] for before, after in zip(sends, sends[1:]))
        later = [probe for probe in probes if probe["offset"] > case["lifetime"]]
        result = dict(case=case["name"], seed=seed, duration=duration, sends=sends, probes=probes,
                      writes=len(sends), accepted_writes=sum(row["accepted"] for row in sends),
                      active_probes=sum(row["active"] for row in probes), probe_count=len(probes),
                      active_after_initial_expiry=sum(row["active"] for row in later),
                      probes_after_initial_expiry=len(later))
        (path/"summary.json").write_text(json.dumps(result, indent=2)+"\n")
        print(f"{case['name']}: accepted {result['accepted_writes']}/{result['writes']} "
              f"available {result['active_probes']}/{result['probe_count']}", flush=True)
    finally:
        for node in reversed(dhts):
            collect()
            node.shutdown()
        collect()
        output.close()
        base.EVENTS.close()
        base.EVENTS = None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--duration", type=float, default=30)
    parser.add_argument("--store-delay", type=float, default=0.04)
    args = parser.parse_args()
    mp.set_start_method("fork", force=True)
    base.torch.set_num_threads(1)
    base.STORE_DELAY = args.store_delay
    args.output.mkdir(parents=True)
    provenance = base.provenance()
    provenance["endurance_script_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    (args.output/"provenance.json").write_text(json.dumps(provenance, indent=2)+"\n")
    cases = [dict(name="f0p5_safe", frequency=0.5, lifetime=2.085764),
             dict(name="f2_safe", frequency=2, lifetime=0.585764),
             dict(name="f2_too_short", frequency=2, lifetime=0.2)]
    for case in cases:
        run(case, 2220, args.output/case["name"], args.duration)


if __name__ == "__main__":
    main()
