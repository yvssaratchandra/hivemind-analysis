"""Trace Hivemind's application-level control traffic during a small training run.

The tracer records two deliberately separate views:

* ``logical_operation`` records public DHT reads and writes before keys are hashed.
  This is where a human-readable key such as ``*_progress`` can still be seen.
* ``wire_message`` records each protobuf request and response at the caller-side
  P2P boundary. This avoids counting the local ``rpc_ping`` helper calls made by
  DHT handlers as extra network pings.

For each wire message, the trace contains decoded control fields plus the exact
serialized protobuf (base64) when it fits ``--max-exact-bytes``. Larger messages
retain their byte length and SHA-256 digest. Stream frames have a correlation ID
and sequence number; a separate ``wire_outcome`` says whether the RPC completed.

The trace covers Hivemind's Python application protocols. It does not claim to
observe libp2p/p2pd internals such as Identify, TLS handshakes, NAT traversal,
relay negotiation, connection multiplexing, or transport acknowledgements.

Writes:
    research/results/trace_<run-id>.jsonl
    research/results/trace_<run-id>_inventory.json
    research/results/trace_<run-id>_inventory.md

Run:
    .venv/bin/python research/trace_control_messages.py
    .venv/bin/python research/trace_control_messages.py --seconds 30 --run-id lifecycle
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import json
import multiprocessing as mp
import queue
import time
import uuid
from collections import Counter
from collections.abc import AsyncIterable
from functools import partial
from pathlib import Path
from typing import Any, AsyncIterator, Dict, Optional

import torch
import torch.nn as nn
from google.protobuf.message import Message

from hivemind import DHT, Optimizer
from hivemind.dht.routing import DHTID
from hivemind.p2p import P2P
from hivemind.p2p.p2p_daemon_bindings.datastructures import PeerID
from hivemind.proto import averaging_pb2, dht_pb2, runtime_pb2
from hivemind.utils.crypto import RSAPrivateKey
from hivemind.utils.serializer import MSGPackSerializer
from hivemind.utils.timed_storage import ValueWithExpiration

LOCALHOST = ["/ip4/127.0.0.1/tcp/0"]
RESULTS = Path(__file__).parent / "results"

# Set before processes are forked, so every child inherits the queue and label.
TRACE: Optional[mp.Queue] = None
WHOAMI = "?"
MAX_EXACT_BYTES = 1024 * 1024

RPC_SCHEMAS = {
    "rpc_ping": {
        "layer": "L2 DHT",
        "checkpoints": [2, 3],
        "request": "PingRequest",
        "response": "PingResponse",
        "stream": False,
    },
    "rpc_store": {
        "layer": "L2 DHT",
        "checkpoints": [3, 5, 6, 7, 8],
        "request": "StoreRequest",
        "response": "StoreResponse",
        "stream": False,
    },
    "rpc_find": {
        "layer": "L2 DHT",
        "checkpoints": [3, 4, 7],
        "request": "FindRequest",
        "response": "FindResponse",
        "stream": False,
    },
    "rpc_join_group": {
        "layer": "L3 Matchmaking",
        "checkpoints": [13, 14, 18],
        "request": "JoinRequest",
        "response": "MessageFromLeader",
        "stream": True,
    },
    "rpc_aggregate_part": {
        "layer": "L4 All-reduce",
        "checkpoints": [17, 18],
        "request": "AveragingData",
        "response": "AveragingData",
        "stream": True,
    },
    "rpc_download_state": {
        "layer": "L6 State transfer",
        "checkpoints": [20],
        "request": "DownloadRequest",
        "response": "DownloadData",
        "stream": True,
    },
}

MESSAGE_CODE_DESCRIPTIONS = {
    "NO_CODE": "No explicit averaging control code.",
    "REQUEST_JOIN": "Defined enum value; normal join requests use JoinRequest and do not carry this code.",
    "ACCEPTED": "Leader provisionally accepts the follower.",
    "BEGIN_ALLREDUCE": "Leader sends the final ordered group roster.",
    "PART_FOR_AVERAGING": "Send a tensor shard and its averaging weight.",
    "AVERAGED_PART": "Return the averaged tensor shard.",
    "NOT_DECLARED": "Reject a request for an undeclared group.",
    "NOT_A_LEADER": "Redirect the requester to the current leader.",
    "BAD_EXPIRATION_TIME": "Reject an incompatible group deadline.",
    "BAD_SCHEMA_HASH": "Reject an incompatible tensor schema.",
    "BAD_GROUP_ID": "Reject an unknown group identifier.",
    "DUPLICATE_PEER_ID": "Reject a duplicate peer identity.",
    "GROUP_IS_FULL": "Reject because the group has no free slot.",
    "NOT_LOOKING_FOR_GROUP": "Reject because the peer is unavailable.",
    "PROTOCOL_VIOLATION": "Report an invalid protocol sequence or value.",
    "INTERNAL_ERROR": "Report an internal averaging failure.",
    "CANCELLED": "Cancel participation in the current all-reduce.",
    "GROUP_DISBANDED": "Leader closes the proposed group.",
    "BAD_GROUP_KEY": "Reject an obsolete or incompatible group key.",
}


def emit(event_type: str, **fields: Any) -> None:
    if TRACE is not None:
        TRACE.put(
            {"t_wall": time.time(), "t_monotonic": time.monotonic(), "peer": WHOAMI, "event": event_type, **fields}
        )


def peer_text(peer: Any) -> Optional[str]:
    if peer is None:
        return None
    try:
        return peer.to_base58()
    except AttributeError:
        return str(peer)


def byte_record(data: bytes, include_exact: bool = True) -> Dict[str, Any]:
    raw = bytes(data)
    result: Dict[str, Any] = {"size": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}
    if include_exact and len(raw) <= MAX_EXACT_BYTES:
        result.update(exact=True, base64=base64.b64encode(raw).decode("ascii"))
    else:
        result.update(exact=False, omitted_reason="exceeds max-exact-bytes" if include_exact else "summary field")
    return result


def json_safe(value: Any, depth: int = 0) -> Any:
    """Make decoded msgpack values readable without losing byte identities."""
    if depth > 8:
        return {"truncated": "maximum nesting depth"}
    if isinstance(value, bytes):
        return byte_record(value, include_exact=len(value) <= 4096)
    if isinstance(value, ValueWithExpiration):
        return {"value": json_safe(value.value, depth + 1), "expiration_time": value.expiration_time}
    if isinstance(value, dict):
        return {str(json_safe(key, depth + 1)): json_safe(item, depth + 1) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item, depth + 1) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return {"type": type(value).__name__, "repr": repr(value)[:500]}


def msgpack_record(value: Any) -> Dict[str, Any]:
    try:
        packed = MSGPackSerializer.dumps(value)
    except BaseException as exc:
        return {"serializable": False, "error": f"{type(exc).__name__}: {exc}", "decoded": json_safe(value)}
    return {"serializable": True, "serialized": byte_record(packed), "decoded": json_safe(value)}


def auth_present(message: Message) -> bool:
    auth = getattr(message, "auth", None)
    return bool(auth is not None and auth.ListFields())


def node_id_hex(message: Any) -> Optional[str]:
    node = getattr(message, "peer", None)
    raw = getattr(node, "node_id", b"") if node is not None else b""
    return bytes(raw).hex() or None


def tensor_record(tensor: runtime_pb2.Tensor) -> Dict[str, Any]:
    return {
        "shape": list(tensor.size),
        "dtype": tensor.dtype or None,
        "compression": runtime_pb2.CompressionType.Name(tensor.compression),
        "chunks": tensor.chunks,
        "requires_grad": tensor.requires_grad,
        "buffer": byte_record(tensor.buffer, include_exact=False),
    }


def decode_gather(raw: bytes) -> Any:
    if not raw:
        return None
    try:
        value = MSGPackSerializer.loads(raw)
    except BaseException as exc:
        return {"bytes": byte_record(raw, include_exact=False), "decode_error": f"{type(exc).__name__}: {exc}"}
    # Matchmaking serializes (bandwidth, mode, application gather data).
    if isinstance(value, (list, tuple)) and len(value) == 3:
        return {"bandwidth": value[0], "mode": json_safe(value[1]), "application_data": json_safe(value[2])}
    return json_safe(value)


def protobuf_fields(message: Message) -> Dict[str, Any]:
    if isinstance(message, dht_pb2.PingRequest):
        return {
            "sender_node_id": node_id_hex(message),
            "validate_reachability": message.validate,
            "auth_present": auth_present(message),
        }
    if isinstance(message, dht_pb2.PingResponse):
        return {
            "responder_node_id": node_id_hex(message),
            "dht_time": message.dht_time,
            "available": message.available,
            "auth_present": auth_present(message),
        }
    if isinstance(message, dht_pb2.StoreRequest):
        return {
            "sender_node_id": node_id_hex(message),
            "auth_present": auth_present(message),
            "records": [
                {
                    "key_id": bytes(key).hex(),
                    "subkey": byte_record(subkey, include_exact=False),
                    "value": byte_record(value, include_exact=False),
                    "expiration_time": expiration,
                    "in_cache": in_cache,
                }
                for key, subkey, value, expiration, in_cache in zip(
                    message.keys, message.subkeys, message.values, message.expiration_time, message.in_cache
                )
            ],
        }
    if isinstance(message, dht_pb2.StoreResponse):
        return {
            "responder_node_id": node_id_hex(message),
            "accepted": list(message.store_ok),
            "auth_present": auth_present(message),
        }
    if isinstance(message, dht_pb2.FindRequest):
        return {
            "sender_node_id": node_id_hex(message),
            "key_ids": [bytes(key).hex() for key in message.keys],
            "auth_present": auth_present(message),
        }
    if isinstance(message, dht_pb2.FindResponse):
        return {
            "responder_node_id": node_id_hex(message),
            "auth_present": auth_present(message),
            "results": [
                {
                    "type": dht_pb2.ResultType.Name(result.type),
                    "value": byte_record(result.value, include_exact=False),
                    "expiration_time": result.expiration_time,
                    "nearest": [
                        {"node_id": bytes(node).hex(), "peer_id": peer_text(PeerID(bytes(peer)))}
                        for node, peer in zip(result.nearest_node_ids, result.nearest_peer_ids)
                    ],
                }
                for result in message.results
            ],
        }
    if isinstance(message, averaging_pb2.JoinRequest):
        return {
            "schema_hash": bytes(message.schema_hash).hex(),
            "expiration": message.expiration,
            "gather": decode_gather(message.gather),
            "client_mode": message.client_mode,
            "group_key": message.group_key,
        }
    if isinstance(message, averaging_pb2.MessageFromLeader):
        return {
            "code": averaging_pb2.MessageCode.Name(message.code),
            "group_id": bytes(message.group_id).hex() or None,
            "suggested_leader": peer_text(PeerID(bytes(message.suggested_leader)))
            if message.suggested_leader
            else None,
            "ordered_peer_ids": [peer_text(PeerID(bytes(item))) for item in message.ordered_peer_ids],
            "gathered": [decode_gather(item) for item in message.gathered],
        }
    if isinstance(message, averaging_pb2.AveragingData):
        return {
            "code": averaging_pb2.MessageCode.Name(message.code),
            "group_id": bytes(message.group_id).hex() or None,
            "peer_id": peer_text(PeerID(bytes(message.peer_id))) if message.peer_id else None,
            "weight": message.weight,
            "tensor_part": tensor_record(message.tensor_part),
        }
    if isinstance(message, averaging_pb2.DownloadRequest):
        return {}
    if isinstance(message, averaging_pb2.DownloadData):
        metadata: Any = None
        if message.metadata:
            try:
                metadata = json_safe(MSGPackSerializer.loads(message.metadata))
            except BaseException as exc:
                metadata = {"decode_error": f"{type(exc).__name__}: {exc}"}
        return {
            "metadata": metadata,
            "metadata_bytes": byte_record(message.metadata, include_exact=False),
            "tensor_part": tensor_record(message.tensor_part),
        }
    return {"decode_warning": f"no field decoder for {message.DESCRIPTOR.full_name}"}


def rpc_name(handle_name: str) -> str:
    return handle_name.rsplit(".", 1)[-1]


def rpc_namespace(handle_name: str) -> Optional[str]:
    return handle_name.split("::", 1)[0] if "::" in handle_name else None


def wire_description(rpc: str, direction: str, message: Message) -> str:
    if rpc == "rpc_ping":
        return (
            "Ask for identity, liveness, time, and optional reachability validation."
            if direction == "request"
            else "Return identity, DHT time, and reachability result."
        )
    if rpc == "rpc_store":
        return (
            "Ask a DHT peer to retain expiring records."
            if direction == "request"
            else "Report which DHT records were accepted."
        )
    if rpc == "rpc_find":
        return (
            "Ask for DHT records and nearer peers."
            if direction == "request"
            else "Return found records and routing referrals."
        )
    if rpc == "rpc_join_group":
        if direction == "request":
            return "Request admission using schema, deadline, mode, group key, and gathered metadata."
        code = averaging_pb2.MessageCode.Name(message.code)
        return MESSAGE_CODE_DESCRIPTIONS.get(code, "Answer a group-join request.")
    if rpc == "rpc_aggregate_part":
        code = averaging_pb2.MessageCode.Name(message.code)
        return MESSAGE_CODE_DESCRIPTIONS.get(code, "Exchange one all-reduce stream frame.")
    if rpc == "rpc_download_state":
        return (
            "Request a complete shared state snapshot."
            if direction == "request"
            else "Return one metadata or tensor chunk from shared state."
        )
    return "Application protobuf message."


def emit_wire_message(
    *,
    correlation_id: str,
    handle_name: str,
    local_peer: Any,
    remote_peer: Any,
    direction: str,
    sequence: int,
    message: Message,
) -> None:
    rpc = rpc_name(handle_name)
    serialized = message.SerializeToString()
    emit(
        "wire_message",
        correlation_id=correlation_id,
        rpc=rpc,
        namespace=rpc_namespace(handle_name),
        handle_name=handle_name,
        layer=RPC_SCHEMAS.get(rpc, {}).get("layer", "unclassified"),
        checkpoints=RPC_SCHEMAS.get(rpc, {}).get("checkpoints", []),
        direction=direction,
        sequence=sequence,
        sender=peer_text(local_peer) if direction == "request" else peer_text(remote_peer),
        receiver=peer_text(remote_peer) if direction == "request" else peer_text(local_peer),
        local_peer_id=peer_text(local_peer),
        remote_peer_id=peer_text(remote_peer),
        message_type=message.DESCRIPTOR.full_name,
        description=wire_description(rpc, direction, message),
        control=protobuf_fields(message),
        protobuf=byte_record(serialized),
    )


def emit_wire_outcome(
    correlation_id: str,
    handle_name: str,
    remote_peer: Any,
    started: float,
    outcome: str,
    request_frames: int,
    response_frames: int,
    error: Optional[BaseException] = None,
) -> None:
    emit(
        "wire_outcome",
        correlation_id=correlation_id,
        rpc=rpc_name(handle_name),
        namespace=rpc_namespace(handle_name),
        remote_peer_id=peer_text(remote_peer),
        outcome=outcome,
        duration_seconds=time.monotonic() - started,
        request_frames=request_frames,
        response_frames=response_frames,
        error=None if error is None else {"type": type(error).__name__, "message": str(error)[:1000]},
    )


# Caller-side interception counts each application protobuf once. In particular,
# it does not see DHTProtocol.rpc_find/rpc_store invoking rpc_ping as a local helper.
_p2p_unary = P2P._call_unary_protobuf_handler
_p2p_stream = P2P.iterate_protobuf_handler


async def traced_unary(
    self: P2P, peer_id: PeerID, handle_name: str, input: Message, output_protobuf_type: type
) -> Message:
    correlation_id, started = uuid.uuid4().hex, time.monotonic()
    emit_wire_message(
        correlation_id=correlation_id,
        handle_name=handle_name,
        local_peer=self.peer_id,
        remote_peer=peer_id,
        direction="request",
        sequence=0,
        message=input,
    )
    try:
        response = await _p2p_unary(self, peer_id, handle_name, input, output_protobuf_type)
    except BaseException as exc:
        emit_wire_outcome(correlation_id, handle_name, peer_id, started, "failed", 1, 0, exc)
        raise
    emit_wire_message(
        correlation_id=correlation_id,
        handle_name=handle_name,
        local_peer=self.peer_id,
        remote_peer=peer_id,
        direction="response",
        sequence=0,
        message=response,
    )
    emit_wire_outcome(correlation_id, handle_name, peer_id, started, "completed", 1, 1)
    return response


async def traced_stream(
    self: P2P,
    peer_id: PeerID,
    handle_name: str,
    input: Any,
    output_protobuf_type: type,
) -> AsyncIterator[Message]:
    correlation_id, started = uuid.uuid4().hex, time.monotonic()
    requests = input if isinstance(input, AsyncIterable) else _single_item_stream(input)
    frame_counts = {"request": 0, "response": 0}

    async def trace_requests() -> AsyncIterator[Message]:
        async for request in requests:
            sequence = frame_counts["request"]
            emit_wire_message(
                correlation_id=correlation_id,
                handle_name=handle_name,
                local_peer=self.peer_id,
                remote_peer=peer_id,
                direction="request",
                sequence=sequence,
                message=request,
            )
            frame_counts["request"] += 1
            yield request

    try:
        responses = await _p2p_stream(self, peer_id, handle_name, trace_requests(), output_protobuf_type)
    except BaseException as exc:
        emit_wire_outcome(correlation_id, handle_name, peer_id, started, "open_failed", 0, 0, exc)
        raise

    async def trace_responses() -> AsyncIterator[Message]:
        try:
            async for response in responses:
                sequence = frame_counts["response"]
                emit_wire_message(
                    correlation_id=correlation_id,
                    handle_name=handle_name,
                    local_peer=self.peer_id,
                    remote_peer=peer_id,
                    direction="response",
                    sequence=sequence,
                    message=response,
                )
                frame_counts["response"] += 1
                yield response
        except GeneratorExit as exc:
            emit_wire_outcome(
                correlation_id,
                handle_name,
                peer_id,
                started,
                "consumer_closed",
                frame_counts["request"],
                frame_counts["response"],
                exc,
            )
            raise
        except asyncio.CancelledError as exc:
            emit_wire_outcome(
                correlation_id,
                handle_name,
                peer_id,
                started,
                "cancelled",
                frame_counts["request"],
                frame_counts["response"],
                exc,
            )
            raise
        except BaseException as exc:
            emit_wire_outcome(
                correlation_id,
                handle_name,
                peer_id,
                started,
                "failed",
                frame_counts["request"],
                frame_counts["response"],
                exc,
            )
            raise
        else:
            emit_wire_outcome(
                correlation_id,
                handle_name,
                peer_id,
                started,
                "completed",
                frame_counts["request"],
                frame_counts["response"],
            )

    return trace_responses()


async def _single_item_stream(item: Message) -> AsyncIterator[Message]:
    yield item


P2P._call_unary_protobuf_handler = traced_unary
P2P.iterate_protobuf_handler = traced_stream


# Logical DHT operations retain plain keys that are irreversibly hashed on wire.
_dht_store, _dht_get = DHT.store, DHT.get


def classify_logical(key: Any, operation: str, value: Any = ...) -> Dict[str, Any]:
    name = str(key)
    if name.endswith("_progress"):
        action = "withdraw" if operation == "store" and value is None else operation
        return {
            "kind": f"progress.{action}",
            "layer": "L5 Optimizer",
            "checkpoints": [9, 21],
            "description": "Publish, read, or withdraw expiring training progress.",
        }
    if ".all_averagers" in name:
        action = "withdraw" if operation == "store" and value is None else operation
        return {
            "kind": f"donor.{action}",
            "layer": "L6 State transfer",
            "checkpoints": [19, 21],
            "description": "Publish, read, or withdraw state-donor priority.",
        }
    if "_averager.0b" in name:
        action = "withdraw" if operation == "store" and value is False else operation
        return {
            "kind": f"advertisement.{action}",
            "layer": "L3 Matchmaking",
            "checkpoints": [8, 11],
            "description": "Publish, read, or withdraw an expiring matchmaking advertisement.",
        }
    return {
        "kind": f"dht_record.{operation}",
        "layer": "L2 DHT",
        "checkpoints": [6, 7],
        "description": "Access another application DHT record.",
    }


def logical_base(dht: DHT, key: Any) -> Dict[str, Any]:
    return {
        "local_peer_id": peer_text(getattr(dht, "_peer_id", None)),
        "plain_key": msgpack_record(key),
        "key_id": DHTID.generate(key).to_bytes().hex(),
    }


def result_record(value: Any) -> Dict[str, Any]:
    return msgpack_record(value)


def attach_logical_outcome(operation_id: str, result: Any, started: float) -> Any:
    def done(future: Any) -> None:
        try:
            value = future.result()
        except BaseException as exc:
            emit(
                "logical_outcome",
                operation_id=operation_id,
                outcome="failed",
                duration_seconds=time.monotonic() - started,
                error={"type": type(exc).__name__, "message": str(exc)[:1000]},
            )
        else:
            emit(
                "logical_outcome",
                operation_id=operation_id,
                outcome="completed",
                duration_seconds=time.monotonic() - started,
                result=result_record(value),
            )

    if hasattr(result, "add_done_callback"):
        result.add_done_callback(done)
    else:
        emit(
            "logical_outcome",
            operation_id=operation_id,
            outcome="completed",
            duration_seconds=time.monotonic() - started,
            result=result_record(result),
        )
    return result


def traced_dht_store(
    self: DHT,
    key: Any,
    value: Any,
    expiration_time: float,
    subkey: Any = None,
    **kwargs: Any,
) -> Any:
    operation_id, started = uuid.uuid4().hex, time.monotonic()
    classification = classify_logical(key, "store", value)
    emit(
        "logical_operation",
        operation_id=operation_id,
        operation="store",
        **classification,
        **logical_base(self, key),
        value=msgpack_record(value),
        subkey=msgpack_record(subkey),
        expiration_time=expiration_time,
        options=json_safe(kwargs),
    )
    try:
        result = _dht_store(self, key, value, expiration_time, subkey=subkey, **kwargs)
    except BaseException as exc:
        emit(
            "logical_outcome",
            operation_id=operation_id,
            outcome="failed",
            duration_seconds=time.monotonic() - started,
            error={"type": type(exc).__name__, "message": str(exc)[:1000]},
        )
        raise
    return attach_logical_outcome(operation_id, result, started)


def traced_dht_get(self: DHT, key: Any, latest: bool = False, **kwargs: Any) -> Any:
    operation_id, started = uuid.uuid4().hex, time.monotonic()
    classification = classify_logical(key, "get")
    emit(
        "logical_operation",
        operation_id=operation_id,
        operation="get",
        **classification,
        **logical_base(self, key),
        latest=latest,
        options=json_safe(kwargs),
    )
    try:
        result = _dht_get(self, key, latest=latest, **kwargs)
    except BaseException as exc:
        emit(
            "logical_outcome",
            operation_id=operation_id,
            outcome="failed",
            duration_seconds=time.monotonic() - started,
            error={"type": type(exc).__name__, "message": str(exc)[:1000]},
        )
        raise
    return attach_logical_outcome(operation_id, result, started)


DHT.store, DHT.get = traced_dht_store, traced_dht_get


def make_model() -> nn.Module:
    torch.manual_seed(0)
    return nn.Linear(8, 1, bias=False)


def run_peer(peer_index: int, initial_peers: list, trace: mp.Queue, stop: mp.Event, args: argparse.Namespace) -> None:
    global TRACE, WHOAMI
    TRACE, WHOAMI = trace, f"peer{peer_index}"
    torch.set_num_threads(1)

    model = make_model()
    inputs, targets = torch.randn(256, 8), torch.randn(256, 1)
    emit(
        "lifecycle",
        stage="bootstrap_start",
        description="Start a DHT node using known entry points.",
        initial_peer_count=len(initial_peers),
    )
    dht = DHT(initial_peers=initial_peers, host_maddrs=LOCALHOST, start=True)
    emit(
        "lifecycle",
        stage="bootstrap_complete",
        description="DHT process is ready.",
        visible_maddrs=[str(maddr) for maddr in dht.get_visible_maddrs()],
    )

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
    emit("lifecycle", stage="optimizer_ready", description="Optimizer, progress tracker, and averagers are running.")

    cursor, previous, started = 0, time.perf_counter(), time.monotonic()
    state_download_attempted = False
    while not stop.is_set():
        time.sleep(max(0.0, previous + 0.2 - time.perf_counter()))
        previous = time.perf_counter()
        batch = slice(cursor % 252, cursor % 252 + 4)
        cursor += 4
        loss = ((model(inputs[batch]) - targets[batch]) ** 2).mean()
        loss.backward()
        optimizer.step()

        if (
            peer_index == 0
            and not state_download_attempted
            and time.monotonic() - started >= args.state_download_after
        ):
            state_download_attempted = True
            for name, averager in (
                ("optimizer_state", optimizer.state_averager),
                ("gradient_state", optimizer.grad_averager),
            ):
                if averager is None:
                    continue
                emit(
                    "lifecycle",
                    stage="state_download_start",
                    description="Explicitly exercise donor discovery and state download.",
                    averager=name,
                )
                try:
                    averager.load_state_from_peers(timeout=args.state_download_timeout)
                except BaseException as exc:
                    emit(
                        "lifecycle",
                        stage="state_download_failed",
                        description="State download attempt failed.",
                        averager=name,
                        error={"type": type(exc).__name__, "message": str(exc)[:1000]},
                    )
                else:
                    emit(
                        "lifecycle",
                        stage="state_download_complete",
                        description=(
                            "State download call returned; use rpc_download_state frames to determine whether a donor replied."
                        ),
                        averager=name,
                    )

    emit("lifecycle", stage="shutdown_start", description="Stop averagers and withdraw expiring records.")
    optimizer.shutdown()
    dht.shutdown()
    emit("lifecycle", stage="shutdown_complete", description="Peer processes have stopped.")


def build_inventory(records: list[Dict[str, Any]]) -> Dict[str, Any]:
    wire = [record for record in records if record["event"] == "wire_message"]
    logical = [record for record in records if record["event"] == "logical_operation"]
    outcomes = [record for record in records if record["event"] == "wire_outcome"]

    wire_counts: Counter = Counter()
    code_counts: Counter = Counter()
    for record in wire:
        code = record.get("control", {}).get("code")
        key = (record["rpc"], record["direction"], record["message_type"], code or "")
        wire_counts[key] += 1
        if code:
            code_counts[code] += 1

    observed_wire = [
        {
            "rpc": rpc,
            "direction": direction,
            "message_type": message_type,
            "code": code or None,
            "count": count,
            "description": next(
                item["description"]
                for item in wire
                if item["rpc"] == rpc
                and item["direction"] == direction
                and item["message_type"] == message_type
                and (item.get("control", {}).get("code") or "") == code
            ),
        }
        for (rpc, direction, message_type, code), count in sorted(wire_counts.items())
    ]

    rpc_coverage = []
    observed_rpcs = {record["rpc"] for record in wire}
    for name, schema in RPC_SCHEMAS.items():
        rpc_coverage.append({"rpc": name, **schema, "observed": name in observed_rpcs})

    message_code_coverage = []
    for name, number in sorted(averaging_pb2.MessageCode.items(), key=lambda item: item[1]):
        message_code_coverage.append(
            {
                "number": number,
                "code": name,
                "observed_count": code_counts[name],
                "description": MESSAGE_CODE_DESCRIPTIONS[name],
            }
        )

    return {
        "scope": "Hivemind Python application-level training protocols; excludes p2pd/libp2p internals and MoE expert RPCs.",
        "record_count": len(records),
        "wire_message_count": len(wire),
        "logical_operation_count": len(logical),
        "exact_wire_messages": sum(record["protobuf"]["exact"] for record in wire),
        "observed_wire": observed_wire,
        "logical_operations": [
            {
                "kind": kind,
                "count": count,
                "description": next(item["description"] for item in logical if item["kind"] == kind),
            }
            for kind, count in sorted(Counter(item["kind"] for item in logical).items())
        ],
        "rpc_family_coverage": rpc_coverage,
        "averaging_message_code_coverage": message_code_coverage,
        "wire_outcomes": [
            {"rpc": rpc, "outcome": outcome, "count": count}
            for (rpc, outcome), count in sorted(Counter((item["rpc"], item["outcome"]) for item in outcomes).items())
        ],
        "boundary": [
            "Caller-side capture records sent and returned application protobufs, including failed calls with no response.",
            "Plain DHT keys exist only in logical_operation rows; wire DHT messages contain hashed key IDs.",
            "Unobserved enum codes remain in the static inventory; most are error or cancellation branches, while REQUEST_JOIN is defined but not carried by JoinRequest.",
            "A synthetic trace_ended outcome marks a call whose caller process ended before it reported completion or failure.",
            "Transport, security-handshake, peer-discovery, NAT, relay, and multiplexing messages inside p2pd are outside this trace.",
        ],
    }


def append_trace_ended_outcomes(records: list[Dict[str, Any]]) -> None:
    """Account for calls still open when a worker process finishes or is stopped."""
    outcomes = {record["correlation_id"] for record in records if record["event"] == "wire_outcome"}
    frames: Dict[str, Dict[str, Any]] = {}
    for record in records:
        if record["event"] != "wire_message":
            continue
        correlation_id = record["correlation_id"]
        state = frames.setdefault(
            correlation_id,
            {
                "first": record,
                "request_frames": 0,
                "response_frames": 0,
            },
        )
        state[f"{record['direction']}_frames"] += 1

    ended_at_wall, ended_at_monotonic = time.time(), time.monotonic()
    for correlation_id, state in frames.items():
        if correlation_id in outcomes:
            continue
        first = state["first"]
        records.append(
            {
                "t_wall": ended_at_wall,
                "t_monotonic": ended_at_monotonic,
                "peer": first["peer"],
                "event": "wire_outcome",
                "correlation_id": correlation_id,
                "rpc": first["rpc"],
                "namespace": first["namespace"],
                "remote_peer_id": first["remote_peer_id"],
                "outcome": "trace_ended",
                "duration_seconds": max(0.0, ended_at_monotonic - first["t_monotonic"]),
                "request_frames": state["request_frames"],
                "response_frames": state["response_frames"],
                "error": None,
                "synthetic": True,
            }
        )


def markdown_table(headers: list[str], rows: list[list[Any]]) -> list[str]:
    lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join("---" for _ in headers) + " |"]
    lines.extend("| " + " | ".join(str(cell).replace("|", "\\|") for cell in row) + " |" for row in rows)
    return lines


def inventory_markdown(inventory: Dict[str, Any]) -> str:
    lines = [
        "# Control-message trace inventory",
        "",
        f"Scope: {inventory['scope']}",
        "",
        f"Recorded {inventory['wire_message_count']} wire messages and {inventory['logical_operation_count']} logical DHT operations. "
        f"Exact protobuf bytes are present for {inventory['exact_wire_messages']} wire messages.",
        "",
        "## Observed wire messages",
        "",
    ]
    lines += markdown_table(
        ["RPC", "Direction", "Protobuf", "Code", "Count", "Purpose"],
        [
            [row["rpc"], row["direction"], row["message_type"], row["code"] or "-", row["count"], row["description"]]
            for row in inventory["observed_wire"]
        ],
    )
    lines += ["", "## Logical DHT operations", ""]
    lines += markdown_table(
        ["Kind", "Count", "Purpose"],
        [[row["kind"], row["count"], row["description"]] for row in inventory["logical_operations"]],
    )
    lines += ["", "## RPC-family coverage", ""]
    lines += markdown_table(
        ["RPC", "Layer", "Request", "Response", "Streaming", "Observed"],
        [
            [row["rpc"], row["layer"], row["request"], row["response"], row["stream"], row["observed"]]
            for row in inventory["rpc_family_coverage"]
        ],
    )
    lines += ["", "## Averaging control-code coverage", ""]
    lines += markdown_table(
        ["Number", "Code", "Observed", "Meaning"],
        [
            [row["number"], row["code"], row["observed_count"], row["description"]]
            for row in inventory["averaging_message_code_coverage"]
        ],
    )
    lines += ["", "## RPC outcomes", ""]
    lines += markdown_table(
        ["RPC", "Outcome", "Count"],
        [[row["rpc"], row["outcome"], row["count"]] for row in inventory["wire_outcomes"]],
    )
    lines += ["", "## Boundary", ""] + [f"- {item}" for item in inventory["boundary"]]
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--num-peers", type=int, default=4)
    parser.add_argument("--seconds", type=float, default=30.0)
    parser.add_argument("--matchmaking-time", type=float, default=5.0)
    parser.add_argument("--target-batch-size", type=int, default=80)
    parser.add_argument("--state-download-after", type=float, default=10.0)
    parser.add_argument("--state-download-timeout", type=float, default=10.0)
    parser.add_argument("--max-exact-bytes", type=int, default=1024 * 1024)
    parser.add_argument("--run-id", type=str, default="lifecycle")
    args = parser.parse_args()
    assert "." not in args.run_id, "Hivemind forbids '.' in an averaging group prefix"
    assert args.num_peers >= 2, "use at least two peers to exercise remote RPCs"

    global TRACE, WHOAMI, MAX_EXACT_BYTES
    TRACE, WHOAMI, MAX_EXACT_BYTES = mp.Queue(), "bootstrap", args.max_exact_bytes

    print(f"tracing {args.num_peers} honest peers for {args.seconds:.0f}s")
    bootstrap = DHT(host_maddrs=LOCALHOST, start=True)
    initial_peers = bootstrap.get_visible_maddrs()

    stop = mp.Event()
    peers = [
        mp.Process(target=run_peer, args=(index, initial_peers, TRACE, stop, args)) for index in range(args.num_peers)
    ]
    for peer in peers:
        peer.start()

    records: list[Dict[str, Any]] = []
    first_seen: set[tuple] = set()
    deadline = time.monotonic() + args.seconds
    while time.monotonic() < deadline:
        try:
            record = TRACE.get(timeout=min(1.0, max(0.01, deadline - time.monotonic())))
        except queue.Empty:
            continue
        records.append(record)
        if record["event"] == "wire_message":
            identity = (record["rpc"], record["direction"], record.get("control", {}).get("code"))
            if identity not in first_seen:
                first_seen.add(identity)
                code = f" [{identity[2]}]" if identity[2] else ""
                print(f"  {record['rpc']} {record['direction']}{code}: {record['description']}")

    stop.set()
    # Keep consuming while workers shut down. Otherwise a worker can block while
    # its multiprocessing queue feeder flushes the (potentially large) exact
    # protobuf rows, and the parent can deadlock waiting in join().
    shutdown_deadline = time.monotonic() + 45
    while any(peer.is_alive() for peer in peers) and time.monotonic() < shutdown_deadline:
        try:
            records.append(TRACE.get(timeout=0.2))
        except queue.Empty:
            pass
        for peer in peers:
            peer.join(timeout=0)

    for peer in peers:
        if peer.is_alive():
            records.append(
                {
                    "t_wall": time.time(),
                    "t_monotonic": time.monotonic(),
                    "peer": "bootstrap",
                    "event": "lifecycle",
                    "stage": "forced_termination",
                    "description": "Peer did not stop within the grace period.",
                    "process_pid": peer.pid,
                }
            )
            peer.terminate()
            peer.join(timeout=5)

    bootstrap.shutdown()
    empty_polls = 0
    while empty_polls < 3:
        try:
            records.append(TRACE.get(timeout=0.5))
            empty_polls = 0
        except queue.Empty:
            empty_polls += 1

    append_trace_ended_outcomes(records)
    records.sort(key=lambda record: (record["t_wall"], record["t_monotonic"]))
    inventory = build_inventory(records)
    RESULTS.mkdir(exist_ok=True)
    trace_path = RESULTS / f"trace_{args.run_id}.jsonl"
    json_path = RESULTS / f"trace_{args.run_id}_inventory.json"
    md_path = RESULTS / f"trace_{args.run_id}_inventory.md"
    with trace_path.open("w") as handle:
        for record in records:
            handle.write(json.dumps(record, sort_keys=True) + "\n")
    json_path.write_text(json.dumps(inventory, indent=2, sort_keys=True) + "\n")
    md_path.write_text(inventory_markdown(inventory))

    print(f"\n{len(records)} total events; {inventory['wire_message_count']} wire messages")
    print(f"exact protobuf bytes: {inventory['exact_wire_messages']}/{inventory['wire_message_count']}")
    print(f"trace:     {trace_path}")
    print(f"inventory: {md_path}")


if __name__ == "__main__":
    mp.set_start_method("fork", force=True)
    main()
