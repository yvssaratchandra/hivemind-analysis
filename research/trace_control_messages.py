"""
Trace every control message an honest swarm sends, and say what each one is for.

`CONTROL_DATA_AUDIT.md` lists 21 control checkpoints, derived by reading the code.
This script checks that list against a running swarm: it wraps every RPC handler
and the DHT's public read/write API, runs ordinary training with no attacker, and
records what actually crossed the wire.

Two things it is meant to settle:
  * which checkpoints really fire in a normal run, and which are code paths that
    never execute under these settings;
  * whether anything shows up that the map does not predict.

Nothing in hivemind/ is modified. The handlers are wrapped by monkeypatching the
classes themselves, not by subclassing -- subclassing would change the RPC handler
name, which is registered as `<prefix>::<ClassName>.<method>`, and a swarm whose
peers disagree on that name cannot connect at all (HANDOFF, Trap 3).

Writes:
    research/results/trace_<run-id>.jsonl   one row per message
    console                                  first sighting of each kind, then a summary

Run:
    .venv/bin/python research/trace_control_messages.py
    .venv/bin/python research/trace_control_messages.py --num-peers 4 --seconds 120
"""

from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import time
from collections import Counter
from functools import partial
from pathlib import Path
from typing import AsyncIterator

import torch
import torch.nn as nn

from hivemind import DHT, Optimizer
from hivemind.averaging.averager import DecentralizedAverager
from hivemind.dht.protocol import DHTProtocol
from hivemind.p2p import P2PContext
from hivemind.proto import averaging_pb2, dht_pb2
from hivemind.utils.crypto import RSAPrivateKey

LOCALHOST = ["/ip4/127.0.0.1/tcp/0"]
RESULTS = Path(__file__).parent / "results"

# Set in main() before any process is forked, so every child inherits it.
TRACE: mp.Queue = None
WHOAMI = "?"


def emit(checkpoint: int, layer: str, kind: str, summary: str, detail: str = "") -> None:
    if TRACE is not None:
        TRACE.put(
            dict(
                t=time.time(),
                peer=WHOAMI,
                checkpoint=checkpoint,
                layer=layer,
                kind=kind,
                summary=summary,
                detail=detail[:400],
            )
        )


def short(value) -> str:
    text = str(value)
    return text[:12] + "..." if len(text) > 15 else text


# ---------------------------------------------------------------------------
# L2 -- the DHT wire protocol
# ---------------------------------------------------------------------------

_ping, _store, _find = DHTProtocol.rpc_ping, DHTProtocol.rpc_store, DHTProtocol.rpc_find


async def rpc_ping(self, request: dht_pb2.PingRequest, context: P2PContext) -> dht_pb2.PingResponse:
    response = await _ping(self, request, context)
    emit(
        2,
        "L2 DHT",
        "rpc_ping",
        "are you alive, and is your clock close to mine?",
        f"caller claims node_id={short(request.peer.node_id.hex())} validate={request.validate}",
    )
    return response


async def rpc_store(self, request: dht_pb2.StoreRequest, context: P2PContext) -> dht_pb2.StoreResponse:
    response = await _store(self, request, context)
    emit(
        6,
        "L2 DHT",
        "rpc_store",
        "keep this record for me until it expires",
        f"{len(request.keys)} key(s), in_cache={list(request.in_cache)[:3]}, "
        f"accepted={list(response.store_ok)[:3]}",
    )
    return response


async def rpc_find(self, request: dht_pb2.FindRequest, context: P2PContext) -> dht_pb2.FindResponse:
    response = await _find(self, request, context)
    found = sum(1 for r in response.results if r.type != dht_pb2.NOT_FOUND)
    referrals = sum(len(r.nearest_node_ids) for r in response.results)
    emit(
        7,
        "L2 DHT",
        "rpc_find",
        "do you have this record, and who else is near it?",
        f"asked for {len(request.keys)} key(s), held {found}, referred {referrals} peer(s)",
    )
    return response


DHTProtocol.rpc_ping, DHTProtocol.rpc_store, DHTProtocol.rpc_find = rpc_ping, rpc_store, rpc_find

# ---------------------------------------------------------------------------
# The readable side of the DHT: plain keys, before they are hashed to DHTIDs.
# The wire never carries these strings, so this is the only place to see them.
# ---------------------------------------------------------------------------

_dht_store, _dht_get = DHT.store, DHT.get


def classify(key) -> tuple:
    """Which checkpoint a DHT access belongs to, decided by the key.

    One `rpc_store` carries several different checkpoints' data, so classifying by
    RPC alone is too coarse. The plain key is the only thing that separates them --
    and the wire never carries it, because it is hashed to a DHTID first.
    """
    name = str(key)
    if name.endswith("_progress"):
        return 9, "L5 Optimizer", "progress", "training progress: epoch, samples, speed"
    if ".all_averagers" in name:
        return 19, "L6 State transfer", "donor-priority", "I can serve model state, at this priority"
    if "_averager.0b" in name:
        return 11, "L3 Matchmaking", "advertisement", "I am looking for a group, deadline attached"
    return 6, "L2 DHT", "other-record", "some other record"


def dht_store(self, key, value, expiration_time, subkey=None, **kwargs):
    checkpoint, layer, label, meaning = classify(key)
    emit(
        checkpoint,
        layer,
        f"put {label}",
        f"publish {key!r} -- {meaning}",
        f"subkey={short(subkey)} value={short(value)}",
    )
    return _dht_store(self, key, value, expiration_time, subkey=subkey, **kwargs)


def dht_get(self, key, latest=False, **kwargs):
    checkpoint, layer, label, meaning = classify(key)
    emit(
        checkpoint,
        layer,
        f"get {label}",
        f"look up {key!r} -- {meaning}",
        f"latest={latest}",
    )
    return _dht_get(self, key, latest=latest, **kwargs)


DHT.store, DHT.get = dht_store, dht_get

# ---------------------------------------------------------------------------
# L3/L4/L6 -- the averaging RPCs
# ---------------------------------------------------------------------------

_join = DecentralizedAverager.rpc_join_group
_aggregate = DecentralizedAverager.rpc_aggregate_part
_download = DecentralizedAverager.rpc_download_state


async def rpc_join_group(
    self, request: averaging_pb2.JoinRequest, context: P2PContext
) -> AsyncIterator[averaging_pb2.MessageFromLeader]:
    emit(
        13,
        "L3 Matchmaking",
        "rpc_join_group (in)",
        "may I join your averaging group?",
        f"requester={short(context.remote_id)} deadline={request.expiration:.1f} "
        f"group_key={request.group_key}",
    )
    async for response in _join(self, request, context):
        name = averaging_pb2.MessageCode.Name(response.code)
        if response.code == averaging_pb2.BEGIN_ALLREDUCE:
            emit(
                14,
                "L3 Matchmaking",
                "BEGIN_ALLREDUCE",
                "here is the group: who is in it and in what order",
                f"group_id={short(response.group_id.hex())} members={len(response.ordered_peer_ids)}",
            )
        else:
            emit(13, "L3 Matchmaking", f"reply {name}", f"answer to a join request: {name}")
        yield response


async def rpc_aggregate_part(
    self, stream: AsyncIterator[averaging_pb2.AveragingData], context: P2PContext
) -> AsyncIterator[averaging_pb2.AveragingData]:
    emit(
        17,
        "L4 All-reduce",
        "rpc_aggregate_part",
        "here is my slice of the gradient; send back the sum",
        "carries the sender-supplied weight, applied as received",
    )
    async for response in _aggregate(self, stream, context):
        yield response


async def rpc_download_state(
    self, request: averaging_pb2.DownloadRequest, context: P2PContext
) -> AsyncIterator[averaging_pb2.DownloadData]:
    emit(
        20,
        "L6 State transfer",
        "rpc_download_state",
        "send me your whole model and optimizer state",
        f"requester={short(context.remote_id)}",
    )
    async for response in _download(self, request, context):
        yield response


DecentralizedAverager.rpc_join_group = rpc_join_group
DecentralizedAverager.rpc_aggregate_part = rpc_aggregate_part
DecentralizedAverager.rpc_download_state = rpc_download_state


# ---------------------------------------------------------------------------
# An ordinary swarm, no attacker
# ---------------------------------------------------------------------------


def make_model() -> nn.Module:
    torch.manual_seed(0)
    return nn.Linear(8, 1, bias=False)


def run_peer(peer_index: int, initial_peers: list, trace: mp.Queue, stop: mp.Event, args) -> None:
    global TRACE, WHOAMI
    TRACE, WHOAMI = trace, f"peer{peer_index}"
    torch.set_num_threads(1)

    model = make_model()
    inputs = torch.randn(256, 8)
    targets = torch.randn(256, 1)

    emit(1, "L1 libp2p", "bootstrap", "join the network via a known peer", f"{len(initial_peers)} entry point(s)")
    dht = DHT(initial_peers=initial_peers, host_maddrs=LOCALHOST, start=True)

    optimizer = Optimizer(
        dht=dht,
        run_id=args.run_id,
        params=model.parameters(),
        optimizer=partial(torch.optim.SGD, lr=0.1),
        target_batch_size=args.target_batch_size,
        batch_size_per_step=4,
        matchmaking_time=args.matchmaking_time,
        averaging_timeout=30.0,
        averager_opts=dict(target_group_size=args.num_peers),
        tracker_opts=dict(private_key=RSAPrivateKey()),
        offload_optimizer=False,
        verbose=False,
    )

    cursor, previous = 0, time.perf_counter()
    while not stop.is_set():
        time.sleep(max(0.0, previous + 0.2 - time.perf_counter()))
        previous = time.perf_counter()
        batch = slice(cursor % 252, cursor % 252 + 4)
        cursor += 4
        loss = ((model(inputs[batch]) - targets[batch]) ** 2).mean()
        loss.backward()
        optimizer.step()

    optimizer.shutdown()
    dht.shutdown()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--num-peers", type=int, default=4)
    parser.add_argument("--seconds", type=float, default=90.0)
    parser.add_argument("--matchmaking-time", type=float, default=5.0)
    parser.add_argument("--target-batch-size", type=int, default=80)
    parser.add_argument("--run-id", type=str, default="trace")
    args = parser.parse_args()
    assert "." not in args.run_id, "hivemind forbids '.' in a group prefix (averager.py:141)"

    global TRACE, WHOAMI
    TRACE, WHOAMI = mp.Queue(), "bootstrap"

    print(f"tracing {args.num_peers} honest peers for {args.seconds:.0f}s\n")
    bootstrap = DHT(host_maddrs=LOCALHOST, start=True)
    initial_peers = bootstrap.get_visible_maddrs()

    stop = mp.Event()
    peers = [
        # not daemon=True: each peer starts its own DHT process, and a daemonic
        # process may not have children (AssertionError inside DHT.__init__)
        mp.Process(target=run_peer, args=(i, initial_peers, TRACE, stop, args))
        for i in range(args.num_peers)
    ]
    for peer in peers:
        peer.start()

    records, seen, deadline = [], {}, time.perf_counter() + args.seconds
    while time.perf_counter() < deadline:
        try:
            record = TRACE.get(timeout=1.0)
        except Exception:
            continue
        records.append(record)
        if record["kind"] not in seen:
            seen[record["kind"]] = record
            elapsed = len(seen)
            print(
                f"  [{elapsed:2d}] cp{record['checkpoint']:<2} {record['layer']:<18} "
                f"{record['kind']:<24} {record['summary']}"
            )

    stop.set()
    for peer in peers:
        peer.join(timeout=20)
        if peer.is_alive():
            peer.terminate()
    while True:
        try:
            records.append(TRACE.get(timeout=1.0))
        except Exception:
            break
    bootstrap.shutdown()

    RESULTS.mkdir(exist_ok=True)
    out = RESULTS / f"trace_{args.run_id}.jsonl"
    with out.open("w") as handle:
        for record in sorted(records, key=lambda r: r["t"]):
            handle.write(json.dumps(record) + "\n")

    counts = Counter(r["kind"] for r in records)
    checkpoints = sorted({r["checkpoint"] for r in records})
    print(f"\n{len(records)} messages, {len(counts)} kinds, written to {out}\n")
    print(f"{'kind':<26}{'cp':<5}{'layer':<20}count")
    for kind, count in counts.most_common():
        row = seen.get(kind) or next(r for r in records if r["kind"] == kind)
        print(f"{kind:<26}{row['checkpoint']:<5}{row['layer']:<20}{count}")

    predicted = set(range(1, 22))
    print(f"\ncheckpoints observed : {checkpoints}")
    print(f"never fired          : {sorted(predicted - set(checkpoints))}")
    print("\n(a checkpoint that never fired is either unreachable in this configuration")
    print(" or a local decision that sends no message -- check the map before concluding)")


if __name__ == "__main__":
    mp.set_start_method("fork", force=True)
    main()
