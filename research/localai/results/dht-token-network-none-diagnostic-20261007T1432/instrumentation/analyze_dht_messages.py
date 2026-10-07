#!/usr/bin/env python3
"""Summarize a short endpoint DHT capture without implying network-wide coverage."""
import argparse
import base64
from collections import Counter, defaultdict
import datetime as dt
import gzip
import hashlib
import json
from pathlib import Path

from run_baseline import memory_bytes


def timestamp(value):
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))


def rows(path):
    if path.exists():
        for line in path.read_text().splitlines():
            if line.strip():
                yield json.loads(line)


def raw_lines(path):
    with path.open() as source:
        yield from source


def analyze(directory):
    directory = Path(directory)
    summary = json.loads((directory / "summary.json").read_text())
    begins = sorted((timestamp(e["timestamp"]), e["phase"]) for e in rows(directory / "phases.jsonl")
                    if e.get("action") == "begin")
    events, types, directions, phase_events = Counter(), Counter(), Counter(), defaultdict(Counter)
    fidelity = Counter()
    phase_dht = defaultdict(Counter)
    examples, dht_peers, keys, buckets = {}, set(), set(), Counter()
    first, last, integrity_errors, rpc_errors = None, None, [], []
    for line_no, line in enumerate(raw_lines(directory / "observer.jsonl"), 1):
        try:
            e = json.loads(line)
            when = timestamp(e["timestamp_utc"])
        except (KeyError, ValueError, TypeError) as error:
            integrity_errors.append({"line": line_no, "error": str(error)})
            continue
        phase = "preflight"
        for began, name in begins:
            if began <= when:
                phase = name
            else:
                break
        first = min(first, when) if first else when
        last = max(last, when) if last else when
        event = e.get("event", "missing_event")
        events[event] += 1
        phase_events[phase][event] += 1
        if event == "dht_message":
            kind, direction = e["message_type"], e["direction"]
            types[kind] += 1
            directions[f"{direction}:{kind}"] += 1
            phase_dht[phase][f"{direction}:{kind}"] += 1
            fidelity[e["capture_fidelity"]] += 1
            examples.setdefault(kind, {"observer_line": line_no, "phase": phase, "message": e})
            remote = e.get("from_peer_id") if direction.startswith("inbound") else e.get("to_peer_id")
            if remote:
                dht_peers.add(remote)
            if e.get("key_sha256"):
                keys.add(e["key_sha256"])
            try:
                for prefix in ("protobuf_body", "protobuf_frame"):
                    if prefix == "protobuf_frame" and prefix + "_base64" not in e:
                        continue
                    payload = base64.b64decode(e[prefix + "_base64"], validate=True)
                    if hashlib.sha256(payload).hexdigest() != e[prefix + "_sha256"]:
                        raise ValueError(prefix + " SHA-256 mismatch")
            except (KeyError, ValueError, TypeError) as error:
                integrity_errors.append({"line": line_no, "error": str(error)})
        elif event == "dht_rpc_error":
            rpc_errors.append({"observer_line": line_no, **e})
        elif event == "gossip_message":
            if e.get("decode_error") or e.get("payload_truncated"):
                integrity_errors.append({"line": line_no, "error": "Gossip decode error/truncation"})
            block = e.get("block_payload") or {}
            try:
                body = base64.b64decode(e["message_base64"], validate=True)
                if hashlib.sha256(body).hexdigest() != e["message_sha256"]:
                    raise ValueError("Gossip body SHA-256 mismatch")
                if json.loads(gzip.decompress(body)) != block:
                    raise ValueError("Decoded gossip body differs from saved block payload")
            except (KeyError, ValueError, TypeError, OSError, EOFError) as error:
                integrity_errors.append({"line": line_no, "error": str(error)})
            for bucket in (block.get("Storage") or {}):
                buckets[bucket] += 1
    memory = defaultdict(list)
    lowest_available = None
    for sample in rows(directory / "samples.jsonl"):
        available = sample["resources"]["memory_bytes"]["MemAvailable"]
        lowest_available = min(lowest_available, available) if lowest_available is not None else available
        for stat in sample["stats"]:
            memory[stat["Name"]].append(memory_bytes(stat["MemUsage"].split("/")[0]))
    observed = {"FIND_NODE", "ADD_PROVIDER", "GET_PROVIDERS"}.issubset(types)
    result = {
        "run_status": summary["status"], "label": summary["label"],
        "capture_first": first.isoformat() if first else None,
        "capture_last": last.isoformat() if last else None,
        "event_counts": dict(events), "dht_message_types": dict(types),
        "dht_directions_and_types": dict(directions),
        "dht_capture_fidelity": dict(fidelity),
        "phase_events": {p: dict(c) for p, c in phase_events.items()},
        "phase_dht": {p: dict(c) for p, c in phase_dht.items()},
        "dht_remote_peers": sorted(dht_peers), "dht_distinct_keys": sorted(keys),
        "dht_examples": examples, "ledger_bucket_snapshot_observations": dict(buckets),
        "integrity_errors": integrity_errors, "dht_rpc_errors": rpc_errors,
        "all_expected_discovery_requests_observed": observed,
        "maximum_sampled_docker_memory_bytes": {n: max(v) for n, v in memory.items()},
        "lowest_host_mem_available_bytes": lowest_available,
        "limitations": [
            "Counts cover the diagnostic endpoint only; requests/replies are distinct records, not packet counts.",
            "Inbound requests are captured before handler validation; receipt alone does not prove provider storage or reply acceptance.",
            "Inbound-hook requests are decoded and re-encoded; replies and exact encrypted/length-delimited frames are not captured.",
            "LocalAI mDNS and GossipSub bootstrap direct peers remain enabled; convergence is not DHT-only evidence.",
            "The forced-server diagnostic bootstrap is a lab role; LocalAI peers use the unmodified automatic DHT mode.",
            "No model, inference, public swarm, WAN delay or provider-expiry interval is tested.",
        ],
    }
    (directory / "dht-analysis.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    result = analyze(args.directory)
    print(json.dumps({k: result[k] for k in ("run_status", "dht_message_types",
        "dht_directions_and_types", "all_expected_discovery_requests_observed", "integrity_errors")}, indent=2))
    raise SystemExit(0 if result["run_status"] == "passed" and not result["integrity_errors"]
                     and result["all_expected_discovery_requests_observed"] else 1)


if __name__ == "__main__":
    main()
