"""Process-local tracing and narrowly scoped fixtures for native Hivemind averaging.

Call :func:`install_hooks` in each worker before constructing an optimizer or
averager. Hooks are inherited by Hivemind's forked averager processes. Only the
gradient averager whose prefix exactly matches ``gradient_prefix`` is subject
to the leader and exclusion fixtures; state averaging and progress tracking
keep their native behavior.
"""
from __future__ import annotations

import functools
import hashlib
from math import isfinite
import os
import contextvars
import contextlib
from collections.abc import Callable, Mapping, Sequence
from typing import Any, get_type_hints


_ACTIVE_UNINSTALL = None


def _event_scope(prefix: str | None, gradient_prefix: str) -> dict[str, str]:
    """Return stable channel and trace-prefix fields for every native event."""
    traceprefix = "" if prefix is None else str(prefix)
    return {
        "channel": "gradient" if traceprefix == gradient_prefix else "state",
        "prefix": traceprefix,
        "traceprefix": traceprefix,
    }


def _filter_aligned_roster(
    peer_ids: Sequence[Any], gathered: Sequence[bytes], excluded_name: str, name_peer: Callable[[Any], str]
) -> tuple[tuple[Any, ...], tuple[bytes, ...], bool]:
    """Remove one named peer while preserving every other opaque metadata blob."""
    if len(peer_ids) != len(gathered):
        raise ValueError("peer IDs and gathered metadata must have equal length")
    keep = [index for index, peer_id in enumerate(peer_ids) if name_peer(peer_id) != excluded_name]
    removed = len(keep) != len(peer_ids)
    return tuple(peer_ids[index] for index in keep), tuple(gathered[index] for index in keep), removed


def _json_safe(value: Any) -> Any:
    """Convert common tensor/container values to JSON-compatible primitives."""
    if isinstance(value, (bytes, bytearray)):
        return {"bytes_hex": bytes(value).hex()}
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    detach = getattr(value, "detach", None)
    if callable(detach):
        try:
            return _json_safe(detach().cpu().tolist())
        except Exception:
            pass
    if hasattr(value, "item"):
        try:
            return _json_safe(value.item())
        except Exception:
            pass
    return repr(value)


def _tensor_values(tensor: Any) -> list[Any]:
    return _json_safe(tensor.detach().cpu().reshape(-1).tolist())


def _raw_bytes(value: Any) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    to_bytes = getattr(value, "to_bytes", None)
    if callable(to_bytes):
        return to_bytes()
    return bytes(value)


def install_hooks(
    *,
    node: str,
    condition: str,
    emit: Callable[..., Any],
    gradient_prefix: str,
    identities: Mapping[str, str],
    snapshot: Callable[[], Any] | None = None,
    leader_fixture: bool = True,
) -> Callable[[], None]:
    """Install native averaging hooks and return an uninstaller.

    ``identities`` is normally a multiprocessing-manager mapping keyed by
    ``str(peer_id)``. Workers should populate it before releasing any optimizer
    step, so both the parent and forked averager children can resolve names.
    ``leader_fixture=False`` retains native leader declarations and join expiry;
    use condition="baseline" with this option for observation-only hooks.
    ``snapshot`` is an optional no-argument callback returning a JSON-safe
    model/state snapshot. It is called only in the installing process, around
    state application methods; it is not invoked from averager children.
    """
    global _ACTIVE_UNINSTALL
    if not node:
        raise ValueError("node must be a non-empty label")
    if condition not in {"baseline", "exclusion", "absent"}:
        raise ValueError("condition must be baseline, exclusion, or absent")
    if not gradient_prefix:
        raise ValueError("gradient_prefix must be non-empty")
    if not callable(emit):
        raise TypeError("emit must be callable")
    if not isinstance(identities, Mapping) and not callable(getattr(identities, "get", None)):
        raise TypeError("identities must be a mapping or mapping proxy with get()")
    if _ACTIVE_UNINSTALL is not None:
        raise RuntimeError("native hooks are already installed in this process")

    # Import lazily so the research harness can select its native snapshot before
    # any Hivemind module is imported.
    from hivemind.averaging.allreduce import AllReduceRunner
    from hivemind.averaging.averager import DecentralizedAverager
    from hivemind.averaging.group_info import GroupInfo
    from hivemind.averaging.key_manager import GroupKeyManager
    from hivemind.averaging.matchmaking import Matchmaking, PotentialLeaders
    from hivemind.averaging.partition import TensorPartContainer
    from hivemind.optim.state_averager import TrainingStateAverager
    from hivemind.proto import averaging_pb2
    from hivemind.p2p import PeerID
    from hivemind.compression import deserialize_torch_tensor

    install_pid = os.getpid()
    originals: list[tuple[Any, str, Any, Any]] = []
    recovery_fetch = contextvars.ContextVar("compute_native_recovery_fetch", default=None)

    def name_peer(peer: Any) -> str:
        try:
            key = str(peer)
            result = identities.get(key)
            if result is None:
                result = identities.get(_raw_bytes(peer).hex())
            return str(result) if result is not None else key
        except Exception:
            return str(peer)

    def prefix_of(obj: Any) -> str:
        prefix = getattr(obj, "_prefix", None)
        if prefix is None:
            prefix = getattr(obj, "prefix", None)
        return "" if prefix is None else str(prefix)

    def scoped(kind: str, prefix: str | None = None, **fields: Any) -> None:
        scope = _event_scope(prefix, gradient_prefix)
        payload = {"worker": node, "condition": condition, **scope, **fields}
        try:
            emit(kind, **payload)
        except Exception as exc:
            # Report a failed observation if the output path still accepts a
            # small error record; instrumentation must not change protocol work.
            failure = {
                "worker": node,
                **scope,
                "event_kind": kind,
                "error": repr(exc),
            }
            try:
                emit("observer_error", **failure)
            except Exception:
                pass
            return

    def peer_fields(peer_ids: Sequence[Any], gathered: Sequence[bytes]) -> dict[str, Any]:
        raw_ids = [_raw_bytes(peer_id) for peer_id in peer_ids]
        return {
            "members": [name_peer(peer_id) for peer_id in peer_ids],
            "peer_ids_hex": [peer_id.hex() for peer_id in raw_ids],
            "gathered_hex": [_raw_bytes(item).hex() for item in gathered],
        }

    def group_fields(group_id: Any, peer_ids: Sequence[Any], gathered: Sequence[bytes]) -> dict[str, Any]:
        return {"group_id": _raw_bytes(group_id).hex(), **peer_fields(peer_ids, gathered)}

    def message_fields(message: Any) -> dict[str, Any]:
        peer_ids = [PeerID(raw) for raw in message.ordered_peer_ids]
        gathered = tuple(message.gathered)
        return {
            **group_fields(message.group_id, peer_ids, gathered),
            "payload_hex": message.SerializeToString().hex(),
        }

    def exclude_group(group_info: Any, excluded_name: str) -> tuple[Any, bool]:
        peer_ids, gathered, removed = _filter_aligned_roster(
            group_info.peer_ids, group_info.gathered, excluded_name, name_peer
        )
        if not removed:
            return group_info, False
        return GroupInfo(group_info.group_id, peer_ids, gathered), True

    def exclude_message(message: Any, excluded_name: str) -> tuple[Any, bool]:
        peer_ids = [PeerID(raw) for raw in message.ordered_peer_ids]
        peer_ids, gathered, removed = _filter_aligned_roster(
            peer_ids, tuple(message.gathered), excluded_name, name_peer
        )
        if not removed:
            return message, False
        rewritten = averaging_pb2.MessageFromLeader(
            code=message.code,
            group_id=message.group_id,
            suggested_leader=message.suggested_leader,
            ordered_peer_ids=[_raw_bytes(peer_id) for peer_id in peer_ids],
            gathered=gathered,
        )
        return rewritten, True

    def call_snapshot() -> Any:
        if snapshot is None or os.getpid() != install_pid:
            return None
        try:
            return _json_safe(snapshot())
        except Exception as exc:
            return {"snapshot_error": repr(exc)}

    def state_values(averager: Any) -> Any:
        if os.getpid() != install_pid:
            return None
        try:
            tensors = list(averager._local_tensors())
            return [_tensor_values(tensor) for tensor in tensors]
        except Exception:
            return None

    def install(owner: Any, method_name: str, replacement: Any) -> None:
        original = getattr(owner, method_name)
        functools.update_wrapper(replacement, original)
        setattr(owner, method_name, replacement)
        originals.append((owner, method_name, original, replacement))

    # Suppress the native periodic declaration task for non-B gradient peers.
    # They remain normal NODE peers and still run the ordinary leader search.
    begin_search = PotentialLeaders.begin_search

    # `begin_search` is an async context manager, so preserve its enter/exit
    # lifecycle while changing only the declaration flag for follower peers.
    @contextlib.asynccontextmanager
    async def observed_begin_search(self, step, key_manager, declare=True):
        prefix = str(getattr(key_manager, "prefix", ""))
        peer_name = name_peer(getattr(self, "peer_id", None))
        suppress = leader_fixture and prefix == gradient_prefix and peer_name != "B" and declare
        if suppress:
            scoped(
                "leader_advertisement_suppressed",
                prefix,
                peer=peer_name,
                group_key=str(getattr(key_manager, "current_key", "")),
                step_deadline=getattr(step, "deadline", None),
                requested_declare=True,
                effective_declare=False,
                method="PotentialLeaders.begin_search",
                mode="NODE",
            )
        async with begin_search(self, step, key_manager, declare=False if suppress else declare):
            yield self

    install(PotentialLeaders, "begin_search", observed_begin_search)

    native_request_expiration = Matchmaking.get_request_expiration_time

    def observed_request_expiration(self):
        prefix = prefix_of(self)
        peer_name = name_peer(getattr(self, "peer_id", None))
        step = getattr(self, "step_control", None)
        deadline = getattr(step, "deadline", None)
        if leader_fixture and prefix == gradient_prefix and peer_name != "B" and isinstance(deadline, (int, float)) and isfinite(deadline):
            expiration_time = float(deadline)
            basis = "step_deadline"
        else:
            expiration_time = native_request_expiration(self)
            basis = "native"
        scoped(
            "follower_join_expiration",
            prefix,
            peer=peer_name,
            expiration_time=expiration_time,
            finite=isfinite(expiration_time),
            step_deadline=deadline,
            basis=basis,
            mode="NODE",
        )
        return expiration_time

    install(Matchmaking, "get_request_expiration_time", observed_request_expiration)

    # Keep a direct declaration guard for uncommon callers that bypass
    # PotentialLeaders.begin_search; normal matchmaking is handled above.
    declare = GroupKeyManager.declare_averager

    async def observed_declare(self, group_key, peer_id, expiration_time, looking_for_group=True):
        prefix = prefix_of(self)
        peer_name = name_peer(peer_id)
        if (
            leader_fixture
            and prefix == gradient_prefix
            and looking_for_group
            and peer_name != "B"
        ):
            scoped(
                "leader_advertisement_suppressed",
                prefix,
                peer=peer_name,
                group_key=str(group_key),
                expiration_time=expiration_time,
                mode="NODE",
            )
            return False
        result = await declare(self, group_key, peer_id, expiration_time, looking_for_group)
        if looking_for_group and prefix == gradient_prefix:
            scoped(
                "leader_advertisement",
                prefix,
                peer=peer_name,
                group_key=str(group_key),
                expiration_time=expiration_time,
                declared=bool(result),
                mode="NODE",
            )
        return result

    install(GroupKeyManager, "declare_averager", observed_declare)

    leader_assemble = Matchmaking.leader_assemble_group

    async def observed_leader_assemble(self):
        prefix = prefix_of(self)
        group_info = await leader_assemble(self)
        scoped(
            "group_assembled",
            prefix,
            role="leader",
            **group_fields(group_info.group_id, group_info.peer_ids, group_info.gathered),
        )
        return group_info

    install(Matchmaking, "leader_assemble_group", observed_leader_assemble)

    follower_assemble = Matchmaking.follower_assemble_group

    async def observed_follower_assemble(self, leader, message):
        prefix = prefix_of(self)
        fields = message_fields(message)
        scoped("announcement_received", prefix, sender=name_peer(leader), **fields)
        group_info = await follower_assemble(self, leader, message)
        scoped(
            "group_assembled",
            prefix,
            role="follower",
            leader=name_peer(leader),
            **group_fields(group_info.group_id, group_info.peer_ids, group_info.gathered),
        )
        return group_info

    install(Matchmaking, "follower_assemble_group", observed_follower_assemble)

    rpc_join = Matchmaking.rpc_join_group

    async def observed_rpc_join(self, request, context):
        prefix = prefix_of(self)
        local_name = name_peer(self.peer_id)
        recipient = name_peer(context.remote_id)
        async for message in rpc_join(self, request, context):
            if message.code == averaging_pb2.BEGIN_ALLREDUCE:
                native_fields = message_fields(message)
                rewritten, excluded = message, False
                if (
                    prefix == gradient_prefix
                    and condition == "exclusion"
                    and local_name == "B"
                    and recipient != "A"
                ):
                    rewritten, excluded = exclude_message(message, "A")
                fields = message_fields(rewritten)
                scoped(
                    "announcement_sent",
                    prefix,
                    sender=local_name,
                    recipient=recipient,
                    excluded=["A"] if excluded else [],
                    native_members=native_fields["members"],
                    native_peer_ids_hex=native_fields["peer_ids_hex"],
                    native_gathered_hex=native_fields["gathered_hex"],
                    native_payload_hex=native_fields["payload_hex"],
                    **fields,
                )
                message = rewritten
            yield message

    install(Matchmaking, "rpc_join_group", observed_rpc_join)

    aggregate = DecentralizedAverager._aggregate_with_group

    async def observed_aggregate(self, group_info, min_vector_size, **kwargs):
        prefix = prefix_of(self)
        native_group = group_info
        local_name = name_peer(self.peer_id)
        if (
            prefix == gradient_prefix
            and condition == "exclusion"
            and local_name == "B"
        ):
            group_info, _ = exclude_group(group_info, "A")
        scoped(
            "local_roster",
            prefix,
            peer=local_name,
            native_members=[name_peer(peer) for peer in native_group.peer_ids],
            native_peer_ids_hex=[_raw_bytes(peer).hex() for peer in native_group.peer_ids],
            native_gathered_hex=[_raw_bytes(item).hex() for item in native_group.gathered],
            excluded=["A"] if len(group_info.peer_ids) != len(native_group.peer_ids) else [],
            **group_fields(group_info.group_id, group_info.peer_ids, group_info.gathered),
        )
        input_values = None
        try:
            with self.get_tensors() as tensors:
                input_values = [_tensor_values(tensor) for tensor in tensors]
        except Exception:
            pass
        scoped(
            "aggregate_input",
            prefix,
            peer=local_name,
            **group_fields(group_info.group_id, group_info.peer_ids, group_info.gathered),
            values=input_values,
        )
        result = await aggregate(self, group_info, min_vector_size, **kwargs)
        output_values = None
        try:
            with self.get_tensors() as tensors:
                output_values = [_tensor_values(tensor) for tensor in tensors]
        except Exception:
            pass
        scoped(
            "aggregate_output",
            prefix,
            peer=local_name,
            **group_fields(group_info.group_id, group_info.peer_ids, group_info.gathered),
            values=output_values,
        )
        return result

    install(DecentralizedAverager, "_aggregate_with_group", observed_aggregate)

    def part_ranges(container: Any) -> dict[str, list[dict[str, int]]]:
        bases = []
        total_before = 0
        for tensor_index, tensor in enumerate(container.local_tensors):
            bases.append((tensor_index, tensor.data_ptr(), tensor.numel(), tensor.element_size(), total_before))
            total_before += tensor.numel()
        result: dict[str, list[dict[str, int]]] = {}
        for peer, peer_parts in zip(container._hook_peers, container._input_parts_by_peer):
            ranges = []
            for part, _info in peer_parts:
                offset = part.data_ptr()
                item = next(
                    (
                        base
                        for base in bases
                        if base[1] <= offset <= base[1] + base[2] * base[3]
                    ),
                    None,
                )
                if item is None or (offset - item[1]) % item[3]:
                    ranges.append({"tensor_index": -1, "start": -1, "end": -1, "elements": part.numel()})
                else:
                    local_start = (offset - item[1]) // item[3]
                    start = item[4] + local_start
                    ranges.append(
                        {
                            "tensor_index": item[0],
                            "start": start,
                            "end": start + part.numel(),
                            "elements": part.numel(),
                        }
                    )
            result[peer] = ranges
        return result

    runner_init = AllReduceRunner.__init__

    def observed_runner_init(self, *args, **kwargs):
        runner_init(self, *args, **kwargs)
        prefix = prefix_of(self)
        self._compute_hook_prefix = prefix
        self._compute_hook_node = node
        self._compute_hook_peers = [name_peer(peer) for peer in self.ordered_peer_ids]
        self._compute_hook_group = _raw_bytes(self.group_id).hex()
        container = self.tensor_part_container
        container._compute_hook_prefix = prefix
        container._compute_hook_node = node
        container._hook_peers = list(self._compute_hook_peers)
        container._compute_hook_group = self._compute_hook_group
        scoped(
            "allocation",
            prefix,
            peer=name_peer(self.peer_id),
            group_id=self._compute_hook_group,
            members=list(self._compute_hook_peers),
            peer_ids_hex=[_raw_bytes(peer).hex() for peer in self.ordered_peer_ids],
            peer_fractions=list(map(float, self.peer_fractions)),
            weight=float(self.weight),
            modes=[str(getattr(mode, "value", mode)) for mode in self.modes],
            parts=list(map(int, container.num_parts_by_peer)),
            part_ranges=part_ranges(container),
            compression=type(container.compression).__name__,
            total_elements=int(container.total_size),
        )

    install(AllReduceRunner, "__init__", observed_runner_init)

    def decoded_values(message: Any) -> Any:
        try:
            return _tensor_values(deserialize_torch_tensor(message.tensor_part))
        except Exception as exc:
            return {"decode_error": repr(exc)}

    generate_input = AllReduceRunner._generate_input_for_peer

    async def observed_generate_input(self, peer_index):
        async for message in generate_input(self, peer_index):
            receiver = self._compute_hook_peers[peer_index]
            scoped(
                "input_chunk",
                self._compute_hook_prefix,
                direction="sent",
                sender=name_peer(self.peer_id),
                receiver=receiver,
                group_id=self._compute_hook_group,
                part_index=getattr(self, "_compute_hook_sent_parts", {}).get(receiver, 0),
                weight=float(message.weight),
                code=int(message.code),
                payload_hex=message.SerializeToString().hex(),
                input_group_id=_raw_bytes(message.group_id).hex(),
                values=decoded_values(message),
            )
            sent_parts = getattr(self, "_compute_hook_sent_parts", None)
            if sent_parts is None:
                sent_parts = self._compute_hook_sent_parts = {}
            sent_parts[receiver] = sent_parts.get(receiver, 0) + 1
            yield message

    install(AllReduceRunner, "_generate_input_for_peer", observed_generate_input)

    rpc_aggregate = AllReduceRunner.rpc_aggregate_part

    async def observed_rpc_aggregate(self, stream, context):
        prefix = self._compute_hook_prefix
        sender = name_peer(context.remote_id)
        receiver = name_peer(self.peer_id)
        incoming_index = 0

        async def observed_input():
            nonlocal incoming_index
            async for message in stream:
                scoped(
                    "input_chunk",
                    prefix,
                    direction="received",
                    sender=sender,
                    receiver=receiver,
                    group_id=self._compute_hook_group,
                    part_index=incoming_index,
                    weight=float(message.weight),
                    code=int(message.code),
                    payload_hex=message.SerializeToString().hex(),
                    input_group_id=_raw_bytes(message.group_id).hex(),
                    values=decoded_values(message),
                )
                incoming_index += 1
                yield message

        outgoing_index = 0
        try:
            async for message in rpc_aggregate(self, observed_input(), context):
                if message.code == averaging_pb2.AVERAGED_PART:
                    kind = "result_chunk"
                    values = decoded_values(message)
                    index = outgoing_index
                    outgoing_index += 1
                else:
                    kind = "stream_terminal"
                    values = None
                    index = None
                scoped(
                    kind,
                    prefix,
                    sender=receiver,
                    receiver=sender,
                    group_id=self._compute_hook_group,
                    part_index=index,
                    code=int(message.code),
                    payload_hex=message.SerializeToString().hex(),
                    values=values,
                )
                yield message
        except BaseException as exc:
            scoped(
                "stream_terminal",
                prefix,
                sender=sender,
                receiver=receiver,
                group_id=self._compute_hook_group,
                outcome="error",
                error=repr(exc),
            )
            raise
        finally:
            scoped(
                "stream_terminal",
                prefix,
                sender=sender,
                receiver=receiver,
                group_id=self._compute_hook_group,
                outcome="closed",
                input_chunks=incoming_index,
                output_chunks=outgoing_index,
            )

    install(AllReduceRunner, "rpc_aggregate_part", observed_rpc_aggregate)

    processed = TensorPartContainer.register_processed_part

    def observed_processed(self, peer_index, part_index, part):
        result = processed(self, peer_index, part_index, part)
        scoped(
            "processed_chunk",
            getattr(self, "_compute_hook_prefix", ""),
            reducer=self._hook_peers[peer_index],
            part_index=int(part_index),
            group_id=self._compute_hook_group,
            values=_tensor_values(part),
        )
        return result

    install(TensorPartContainer, "register_processed_part", observed_processed)

    failed = TensorPartContainer.register_failed_reducer

    def observed_failed(self, peer_index):
        scoped(
            "fallback",
            getattr(self, "_compute_hook_prefix", ""),
            reducer=self._hook_peers[peer_index],
            part_count=int(self.num_parts_by_peer[peer_index]),
            registered_parts=int(self._outputs_registered_by_peer[peer_index]),
            group_id=self._compute_hook_group,
        )
        return failed(self, peer_index)

    install(TensorPartContainer, "register_failed_reducer", observed_failed)

    container_finalize = TensorPartContainer.finalize

    def observed_container_finalize(self):
        was_finished = self.finished.is_set()
        result = container_finalize(self)
        if not was_finished:
            scoped(
                "finalize",
                getattr(self, "_compute_hook_prefix", ""),
                group_id=getattr(self, "_compute_hook_group", None),
                failed_size=int(self.failed_size),
                total_elements=int(self.total_size),
            )
        return result

    install(TensorPartContainer, "finalize", observed_container_finalize)

    ban_sender = AllReduceRunner._ban_sender

    async def observed_ban_sender(self, peer_id):
        await ban_sender(self, peer_id)
        scoped(
            "sender_banned",
            self._compute_hook_prefix,
            sender=name_peer(peer_id),
            peer=name_peer(self.peer_id),
            group_id=self._compute_hook_group,
        )

    install(AllReduceRunner, "_ban_sender", observed_ban_sender)

    runner_finalize = AllReduceRunner.finalize

    def observed_runner_finalize(self, *, cancel=False, exception=None):
        try:
            return runner_finalize(self, cancel=cancel, exception=exception)
        finally:
            scoped(
                "terminal",
                self._compute_hook_prefix,
                peer=name_peer(self.peer_id),
                group_id=self._compute_hook_group,
                outcome="cancelled" if cancel else "error" if exception else "complete",
                error=repr(exception) if exception is not None else None,
                failed_size=int(self.tensor_part_container.failed_size),
                banned_senders=[name_peer(peer) for peer in self.banned_senders],
            )

    install(AllReduceRunner, "finalize", observed_runner_finalize)

    # State donors stream a metadata blob followed by serialized tensor parts.
    # Keep the native stream byte-for-byte intact while recording both ends of
    # the transfer; small payloads (such as the flat 48-value fixture) are
    # included in full, while hashes remain available for larger chunks.
    native_rpc_download_state = DecentralizedAverager.rpc_download_state

    async def observed_rpc_download_state(self, _request, _context):
        prefix = prefix_of(self)
        try:
            sender_peer = self.peer_id
        except Exception:
            sender_peer = None
        recipient_peer = getattr(_context, "remote_id", None)
        sender = name_peer(sender_peer) if sender_peer is not None else None
        recipient = name_peer(recipient_peer) if recipient_peer is not None else None
        sender_peer_id_hex = _raw_bytes(sender_peer).hex() if sender_peer is not None else None
        recipient_peer_id_hex = _raw_bytes(recipient_peer).hex() if recipient_peer is not None else None
        chunk_count = 0
        payload_bytes = 0
        metadata_sent = False
        outcome = "incomplete"
        completed = False
        error = None
        try:
            async for message in native_rpc_download_state(self, _request, _context):
                chunk_index = chunk_count
                chunk_count += 1
                try:
                    metadata = bytes(message.metadata)
                    tensor_part = message.tensor_part
                    payload = bytes(tensor_part.buffer)
                    serialized_chunk = tensor_part.SerializeToString()
                    serialized_payload = message.SerializeToString()
                    payload_bytes += len(payload)
                    metadata_sent = metadata_sent or bool(metadata)
                    scoped(
                        "state_download_chunk",
                        prefix,
                        sender=sender,
                        recipient=recipient,
                        sender_peer_id_hex=sender_peer_id_hex,
                        recipient_peer_id_hex=recipient_peer_id_hex,
                        chunk_index=chunk_index,
                        metadata_hex=metadata.hex() if len(metadata) <= 4096 else None,
                        metadata_hex_truncated=len(metadata) > 4096,
                        metadata_sha256=hashlib.sha256(metadata).hexdigest() if metadata else None,
                        tensor_payload_hex=payload.hex() if len(payload) <= 4096 else None,
                        tensor_payload_hex_truncated=len(payload) > 4096,
                        tensor_payload_sha256=hashlib.sha256(payload).hexdigest(),
                        tensor_payload_bytes=len(payload),
                        serialized_chunk_hex=(serialized_chunk.hex() if len(serialized_chunk) <= 4096 else None),
                        serialized_chunk_hex_truncated=len(serialized_chunk) > 4096,
                        serialized_chunk_sha256=hashlib.sha256(serialized_chunk).hexdigest(),
                        serialized_chunk_bytes=len(serialized_chunk),
                        serialized_message_sha256=hashlib.sha256(serialized_payload).hexdigest(),
                        dtype=tensor_part.dtype,
                        shape=list(tensor_part.size),
                        compression=int(tensor_part.compression),
                        chunks=int(tensor_part.chunks),
                    )
                except Exception as exc:
                    scoped(
                        "observer_error",
                        prefix,
                        method="rpc_download_state",
                        observation="serialize_download_chunk",
                        chunk_index=chunk_index,
                        error=repr(exc),
                    )
                yield message
            completed = True
            outcome = "complete"
        except BaseException as exc:
            outcome = "cancelled" if type(exc).__name__ in {"CancelledError", "GeneratorExit"} else "error"
            error = repr(exc)
            raise
        finally:
            scoped(
                "state_download_terminal",
                prefix,
                sender=sender,
                recipient=recipient,
                sender_peer_id_hex=sender_peer_id_hex,
                recipient_peer_id_hex=recipient_peer_id_hex,
                success=completed,
                outcome=outcome,
                error=error,
                chunks=chunk_count,
                payload_bytes=payload_bytes,
                metadata_sent=metadata_sent,
            )

    install(DecentralizedAverager, "rpc_download_state", observed_rpc_download_state)
    # The wrapper lives in this research module, while the original postponed
    # annotations refer to names from hivemind.averaging.averager. Resolve them
    # before exposing the method to ServicerBase's RPC handler discovery.
    observed_rpc_download_state.__annotations__ = get_type_hints(native_rpc_download_state)

    # These methods execute in the worker process, after the optimizer/state
    # tensors have actually been copied into the model. Child averagers inherit
    # the patch but cannot call the snapshot closure because of the PID guard.
    def wrap_state_application(method_name: str):
        original = getattr(TrainingStateAverager, method_name)

        def observed(self, *args, **kwargs):
            prefix = prefix_of(self)
            before = state_values(self)
            snapshot_before = call_snapshot()
            scoped(
                "state_application",
                prefix,
                method=method_name,
                phase="before",
                local_tensors=before,
                snapshot=snapshot_before,
            )
            try:
                return original(self, *args, **kwargs)
            finally:
                scoped(
                    "state_application",
                    prefix,
                    method=method_name,
                    phase="after",
                    local_tensors=state_values(self),
                    snapshot=call_snapshot(),
                )

        install(TrainingStateAverager, method_name, observed)

    wrap_state_application("_apply_averaging_results_")
    wrap_state_application("_apply_optimizer_parameters_")

    native_load_state = DecentralizedAverager.load_state_from_peers

    def observed_native_load_state(self, *args, **kwargs):
        prefix = prefix_of(self)
        try:
            result = native_load_state(self, *args, **kwargs)
        except BaseException as exc:
            record = {"available": False, "pending": False, "error": repr(exc)}
            recovery_fetch.set(record)
            scoped(
                "state_recovery_fetch",
                prefix,
                wait=bool(kwargs.get("wait", args[0] if args else True)),
                available=False,
                pending=False,
                outcome="error",
                error=repr(exc),
            )
            raise
        is_loaded = isinstance(result, tuple) and len(result) == 2
        if is_loaded:
            metadata, tensors = result
            try:
                record = {
                    "available": True,
                    "metadata": _json_safe(metadata),
                    "values": [_tensor_values(tensor) for tensor in tensors],
                }
            except Exception as exc:
                record = {"available": True, "capture_error": repr(exc)}
        else:
            record = {"available": False, "pending": not (result is None)}
        recovery_fetch.set(record)
        scoped(
            "state_recovery_fetch",
            prefix,
            wait=bool(kwargs.get("wait", args[0] if args else True)),
            available=record["available"],
            pending=record.get("pending", False),
            outcome="loaded" if record["available"] else "pending" if record.get("pending") else "empty",
            metadata=record.get("metadata"),
            capture_error=record.get("capture_error"),
            values=record.get("values"),
            result_type=type(result).__name__,
        )
        return result

    install(DecentralizedAverager, "load_state_from_peers", observed_native_load_state)

    training_load_state = TrainingStateAverager.load_state_from_peers

    def observed_training_load_state(self, *args, **kwargs):
        prefix = prefix_of(self)
        epoch_before = getattr(self, "local_epoch", None)
        before = state_values(self)
        snapshot_before = call_snapshot()
        token = recovery_fetch.set(None)
        scoped(
            "state_recovery_application",
            prefix,
            phase="before",
            local_epoch=epoch_before,
            local_tensors=before,
            snapshot=snapshot_before,
        )
        try:
            return training_load_state(self, *args, **kwargs)
        finally:
            fetched = recovery_fetch.get()
            metadata = fetched.get("metadata") if isinstance(fetched, dict) else None
            metadata_epoch = metadata.get("epoch") if isinstance(metadata, dict) else None
            epoch_after = getattr(self, "local_epoch", None)
            scoped(
                "state_recovery_application",
                prefix,
                phase="after",
                fetch_available=bool(isinstance(fetched, dict) and fetched.get("available")),
                fetch_pending=bool(isinstance(fetched, dict) and fetched.get("pending")),
                fetch_error=fetched.get("error") if isinstance(fetched, dict) else None,
                fetch_metadata=metadata,
                metadata_epoch=metadata_epoch,
                metadata_epoch_eligible=(
                    isinstance(metadata_epoch, int)
                    and isinstance(epoch_before, int)
                    and metadata_epoch >= epoch_before
                ),
                local_epoch_before=epoch_before,
                local_epoch_after=epoch_after,
                local_epoch_advanced=(
                    isinstance(epoch_before, int)
                    and isinstance(epoch_after, int)
                    and epoch_after > epoch_before
                ),
                local_epoch_matches_metadata=(
                    isinstance(metadata_epoch, int) and epoch_after == metadata_epoch
                ),
                local_tensors=state_values(self),
                snapshot=call_snapshot(),
            )
            recovery_fetch.reset(token)

    install(TrainingStateAverager, "load_state_from_peers", observed_training_load_state)

    uninstalling = False

    def uninstall() -> None:
        nonlocal uninstalling
        global _ACTIVE_UNINSTALL
        if uninstalling:
            return
        uninstalling = True
        for owner, method_name, original, replacement in reversed(originals):
            if getattr(owner, method_name, None) is replacement:
                setattr(owner, method_name, original)
        if _ACTIVE_UNINSTALL is uninstall:
            _ACTIVE_UNINSTALL = None

    _ACTIVE_UNINSTALL = uninstall
    return uninstall
