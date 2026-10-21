#!/usr/bin/env python3
"""Private loopback smoke combining controlled DHT holders and roster exclusion.

Four native ``DecentralizedAverager`` participants (A, X, D, C) use fixed,
unsharded gradient_probe and state_probe keys. For each key, twenty local
DHT-only nodes use node IDs at XOR distances 0..19; with Hivemind's explicit
``bucket_size=20`` and ``num_replicas=5``, the first five are the designed
replica holders. Only those controlled DHT processes install the read filter.

Both arms use the same holder filtering, early X deadline, and admission policy.
X holds D/C join requests until A has joined, then admits them. The exclusion arm
removes A from X's local roster and from announcements sent to D/C; A receives
the complete native announcement. Markers are reset before each round so outputs
can be compared with the exact expected mean for the effective roster.

This is a bounded local mechanism test. It does not run Optimizer, training,
state-averaging scheduling, recovery, WAN delay, or a public swarm. All listeners
and bootstrap peers are hard-coded to loopback; the CLI accepts no remote peers.

Example with the prepared local runtime:
    PYTHONPATH=research/agora/.runtime/venv/lib/python3.11/site-packages \\
      /home/sarat/.local/bin/python3.11 research/hivemind/recordholder_exclusion_smoke.py \\
      --label rh_exclusion_20261021_a
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
import hashlib
import json
import multiprocessing as mp
import os
from pathlib import Path
import queue
import signal
import subprocess
import sys
import tarfile
import time
import traceback
from typing import Any, Mapping, Sequence

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = Path(__file__).resolve()
HELPER_PATH = ROOT / "research/hivemind/tracking_combination_smoke.py"
sys.path.insert(0, str(SCRIPT.parent))
import tracking_combination_smoke as smoke  # noqa: E402

NODES = ("A", "X", "D", "C")
CHANNELS = ("gradient_probe", "state_probe")
ARMS = ("capture_control", "capture_exclusion")
HOLDER_IDS_PER_KEY = 20
NUM_REPLICAS = 5
BUCKET_SIZE = 20
MARKER_BLOCKS = 48
MARKER_WIDTH = 4
VECTOR_SIZE = MARKER_BLOCKS * MARKER_WIDTH  # 192; block-aligned for 3- and 4-member rosters
LOCALHOST = ["/ip4/127.0.0.1/tcp/0"]

# Set only in the dedicated record-holder process before its DHT subprocesses fork.
_FILTER = None
_IDENTITIES = None
_EVENTS = None
_START = 0.0


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def elapsed() -> float:
    return time.monotonic() - _START


def _emit(kind: str, **fields: Any) -> None:
    if _EVENTS is None:
        return
    item = {"kind": kind, "time": elapsed(), "pid": os.getpid(), **fields}
    try:
        _EVENTS.put(item)
    except Exception:
        pass


def xor_nearby_ids(target: int, count: int) -> list[int]:
    """Return crafted IDs in ascending XOR distance from one exact DHT key."""
    if count < 1:
        raise ValueError("count must be positive")
    return [target ^ distance for distance in range(count)]


def marker_vector(node: str, blocks: int = MARKER_BLOCKS) -> list[float]:
    """Repeated one-hot origin markers; every block identifies one participant."""
    if node not in NODES:
        raise ValueError(f"unknown participant {node!r}")
    result = [0.0] * (blocks * MARKER_WIDTH)
    origin = NODES.index(node)
    for block in range(blocks):
        result[block * MARKER_WIDTH + origin] = 1.0
    return result


def expected_marker_mean(peers: Sequence[str], blocks: int = MARKER_BLOCKS) -> list[float]:
    """Expected equal-weight marker mean for the actual aggregation roster."""
    if not peers or any(peer not in NODES for peer in peers):
        raise ValueError("expected a non-empty roster of known participants")
    result = [0.0] * (blocks * MARKER_WIDTH)
    for peer in peers:
        origin = NODES.index(peer)
        for block in range(blocks):
            result[block * MARKER_WIDTH + origin] += 1.0 / len(peers)
    return result


def mean_vectors(vectors: Sequence[Sequence[float]]) -> list[float]:
    if not vectors or len({len(vector) for vector in vectors}) != 1:
        raise ValueError("expected equally sized, non-empty input vectors")
    scale = 1.0 / len(vectors)
    return [sum(float(vector[index]) for vector in vectors) * scale
            for index in range(len(vectors[0]))]


def vector_matches(actual: Sequence[float] | None, expected: Sequence[float], tol: float = 1e-4) -> bool:
    return (actual is not None and len(actual) == len(expected) and
            all(abs(float(a) - float(b)) <= tol for a, b in zip(actual, expected)))


def filtered_parallel_roster(peer_ids: Sequence[str], gathered: Sequence[bytes], excluded: str
                             ) -> tuple[tuple[str, ...], tuple[bytes, ...], bool]:
    """Filter IDs and their opaque metadata together, preserving index alignment."""
    return smoke.filter_roster(peer_ids, gathered, excluded, lambda peer: str(peer))


def filter_dictionary_subkeys(dictionary, allowed_subkeys: set[bytes], dictionary_type):
    """Copy only allowed DHT subkeys while preserving each native stored value/expiry."""
    filtered = dictionary_type(maxsize=dictionary.maxsize)
    for subkey, entry in dictionary.items():
        if bytes(subkey) in allowed_subkeys:
            filtered.store(subkey, entry.value, entry.expiration_time)
    return filtered


def rewrite_leader_announcement(message, excluded: str, peer_id_type, message_type, label_peer):
    """Build a protobuf announcement with peer IDs and gathered metadata aligned."""
    native_ids = tuple(peer_id_type(raw) for raw in message.ordered_peer_ids)
    native_gathered = tuple(message.gathered)
    filtered_ids, filtered_gathered, removed = smoke.filter_roster(
        native_ids, native_gathered, excluded, label_peer)
    native_members = [label_peer(peer) for peer in native_ids]
    if not removed:
        return message, False, native_members, native_members
    rewritten = message_type(
        code=message.code,
        group_id=message.group_id,
        suggested_leader=message.suggested_leader,
        ordered_peer_ids=[peer.to_bytes() if hasattr(peer, "to_bytes") else bytes(peer)
                          for peer in filtered_ids],
        gathered=filtered_gathered,
    )
    return rewritten, True, native_members, [label_peer(peer) for peer in filtered_ids]


def announcement_for_recipient(message, arm: str, requester: str, peer_id_type,
                               message_type, label_peer):
    """Apply omission only to non-target recipients in the treatment arm."""
    native_ids = tuple(peer_id_type(raw) for raw in message.ordered_peer_ids)
    native_members = [label_peer(peer) for peer in native_ids]
    if arm != "capture_exclusion" or requester == "A":
        return message, False, native_members, native_members
    return rewrite_leader_announcement(message, "A", peer_id_type, message_type, label_peer)


def _peer_label(peer: Any) -> str:
    try:
        return _IDENTITIES.get(str(peer), str(peer)) if _IDENTITIES is not None else str(peer)
    except Exception:
        return str(peer)


def _record_labels(dictionary, peer_id_type) -> list[str]:
    labels = []
    for subkey, _entry in dictionary.items():
        try:
            labels.append(_peer_label(peer_id_type(subkey)))
        except Exception:
            labels.append(bytes(subkey).hex())
    return labels


def _install_record_holder_filter(filter_config, identities, events) -> None:
    """Install a response filter only in the controlled-holder process."""
    global _FILTER, _IDENTITIES, _EVENTS
    from hivemind.dht.protocol import DHTProtocol
    from hivemind.dht.routing import DHTID
    from hivemind.dht.storage import DictionaryDHTValue
    from hivemind.proto import dht_pb2
    from hivemind.p2p import P2PContext, PeerID

    _FILTER, _IDENTITIES, _EVENTS = filter_config, identities, events
    original = DHTProtocol.rpc_find

    async def filtered_rpc_find(self, request, context):
        response = await original(self, request, context)
        config = _FILTER
        if config is None or not config.get("ready"):
            return response

        requester_id = str(context.remote_id)
        requester = _peer_label(context.remote_id)
        key_configs = config.get("keys", {})
        for index, key_id in enumerate(request.keys):
            key_hex = bytes(key_id).hex()
            policy = key_configs.get(key_hex)
            if policy is None:
                continue
            item = response.results[index]
            storage_present = self.storage.get(DHTID.from_bytes(key_id)) is not None
            cache_present = self.cache.get(DHTID.from_bytes(key_id)) is not None
            before = []
            after = []
            changed = False
            if item.type == dht_pb2.FOUND_DICTIONARY:
                original_dictionary = self.serializer.loads(item.value)
                before = _record_labels(original_dictionary, PeerID)
                if requester in policy["filtered_requesters"]:
                    allowed = {PeerID.from_base58(peer).to_bytes()
                               for peer in policy["allowed_peer_ids"]}
                    filtered = filter_dictionary_subkeys(
                        original_dictionary, allowed, DictionaryDHTValue)
                    after = _record_labels(filtered, PeerID)
                    changed = before != after
                    item.value = self.serializer.dumps(filtered)
                else:
                    after = list(before)
            nearest = [
                {"node_id": bytes(node_id).hex(),
                 "peer_id": _peer_label(PeerID(peer_id))}
                for node_id, peer_id in zip(item.nearest_node_ids, item.nearest_peer_ids)
            ]
            _emit("holder_find", arm=policy["arm"], holder=_peer_label(self.p2p.peer_id),
                  holder_node_id=self.node_id.to_bytes().hex(),
                  requester=requester, requester_peer_id=requester_id, channel=policy["channel"],
                  key_hex=key_hex, result_type=int(item.type), before=before, after=after,
                  filtered=changed, storage_present=storage_present, cache_present=cache_present,
                  responder_nearest_nodes=nearest)
        return response

    filtered_rpc_find.__annotations__ = {
        "request": dht_pb2.FindRequest,
        "context": P2PContext,
        "return": dht_pb2.FindResponse,
    }

    DHTProtocol.rpc_find = filtered_rpc_find


def _loopback_addresses(dht, label: str) -> list[str]:
    addresses = [str(addr) for addr in dht.get_visible_maddrs()]
    if not addresses or any(not addr.startswith("/ip4/127.0.0.1/tcp/") for addr in addresses):
        raise RuntimeError(f"non-loopback listener for {label}: {addresses}")
    return addresses


def _run_holders(initial_peers, key_specs, identities, holder_peerids, filter_config,
                 events, ready, stop, label: str) -> None:
    """Own the forty crafted local DHT identities and their DHT subprocesses."""
    from hivemind import DHT
    from hivemind.dht.routing import DHTID

    _install_record_holder_filter(filter_config, identities, events)
    dhts = []
    error = None
    try:
        for channel in CHANNELS:
            target = int(key_specs[channel]["target_id"])
            for distance, node_id_value in enumerate(xor_nearby_ids(target, HOLDER_IDS_PER_KEY)):
                holder = f"R_{channel}_{distance:02d}"
                dht = DHT(
                    initial_peers=initial_peers, host_maddrs=LOCALHOST,
                    auto_nat=False, nat_port_map=False, use_relay=False,
                    bucket_size=BUCKET_SIZE, num_replicas=NUM_REPLICAS,
                    node_id=DHTID(node_id_value), start=True,
                )
                addresses = _loopback_addresses(dht, holder)
                dhts.append(dht)
                peer_id = str(dht.peer_id)
                identities[peer_id] = holder
                holder_peerids[holder] = peer_id
                _emit("holder_started", holder=holder, peer_id=peer_id,
                      node_id_hex=DHTID(node_id_value).to_bytes().hex(),
                      xor_distance=distance, channel=channel,
                      designed_replica=distance < NUM_REPLICAS, addresses=addresses)
        ready.set()
        _emit("holder_topology_ready", holders=len(dhts), channels=list(CHANNELS),
              bucket_size=BUCKET_SIZE, replicas=NUM_REPLICAS)
        while not stop.wait(0.1):
            pass
    except BaseException as exc:
        error = repr(exc)
        _emit("holder_error", error=error, traceback=traceback.format_exc())
        ready.set()
    finally:
        for dht in reversed(dhts):
            try:
                dht.shutdown()
            except BaseException as exc:
                _emit("holder_cleanup_error", error=repr(exc))
        _emit("holder_process_exit", error=error, count=len(dhts))


def _install_candidate_observer(prefix_channels: Mapping[str, str], current_round, identity_map,
                                arm: str) -> None:
    """Read-only observation of the native candidate list returned to each averager."""
    from hivemind.averaging.key_manager import GroupKeyManager

    original = GroupKeyManager.get_averagers

    async def observed(self, group_key, only_active):
        result = await original(self, group_key, only_active)
        prefix = str(self.prefix)
        channel = prefix_channels.get(prefix)
        if channel is not None:
            peer = identity_map.get(str(self.peer_id), str(self.peer_id))
            round_no = current_round.get((peer, channel), -1)
            _emit("candidate_list", arm=arm, peer=peer, channel=channel, round=round_no,
                  group_key=str(group_key), only_active=bool(only_active),
                  candidates=[identity_map.get(str(candidate), str(candidate)) for candidate, _exp in result],
                  candidate_peer_ids=[str(candidate) for candidate, _exp in result])
        return result

    GroupKeyManager.get_averagers = observed


def _tensor_values(averager) -> list[float]:
    with averager.get_tensors() as tensors:
        return [float(value) for value in tensors[0].detach().cpu().reshape(-1).tolist()]


def _set_marker(averager, node: str) -> None:
    import torch
    marker = torch.tensor(marker_vector(node), dtype=torch.float32)
    with averager.get_tensors() as tensors:
        tensors[0].copy_(marker)


def _stage_name(stage: Any) -> str:
    name = getattr(stage, "name", None)
    return str(name) if name is not None else str(getattr(stage, "value", stage))


def _make_averager_class(identity_map, current_round, prefix_channels, arm: str,
                         victim_peer_id: str, attacker_peer_id: str):
    from hivemind.averaging import DecentralizedAverager
    from hivemind.averaging.group_info import GroupInfo
    from hivemind.p2p import P2PContext, PeerID
    from hivemind.proto import averaging_pb2
    from typing import AsyncIterator

    class RecordHolderSmokeAverager(DecentralizedAverager):
        def __init__(self, *args, node_label: str, channel: str, shared_round,
                     identity_map, arm: str, **kwargs):
            self._node_label = node_label
            self._channel = channel
            self._shared_round = shared_round
            self._identity_map = identity_map
            self._arm = arm
            super().__init__(*args, **kwargs)

        def _round_no(self) -> int:
            return int(self._shared_round.value)

        def _label(self, peer: Any) -> str:
            return self._identity_map.get(str(peer), str(peer))

        def _target_is_present(self) -> bool:
            followers = getattr(self._matchmaking, "current_followers", {})
            return any(str(peer_id) == victim_peer_id for peer_id in followers)

        async def rpc_join_group(
            self, request: averaging_pb2.JoinRequest, context: P2PContext
        ) -> AsyncIterator[averaging_pb2.MessageFromLeader]:
            requester_id = str(context.remote_id)
            requester = self._label(context.remote_id)
            if self._node_label == "X" and requester not in ("A", "X"):
                # Keep the join RPC open briefly so A's request can be accepted first.
                # The requester has a matching finite RPC timeout; failed waits remain
                # in the attempt denominator and are recorded as rejections.
                deadline = time.monotonic() + 3.5
                while not self._target_is_present() and time.monotonic() < deadline:
                    await asyncio.sleep(0.01)
                if not self._target_is_present():
                    _emit("admission", arm=self._arm, channel=self._channel,
                          round=self._round_no(), leader="X", requester=requester,
                          requester_peer_id=requester_id, admitted=False,
                          reason="target_not_yet_accepted_at_hold_deadline")
                    yield averaging_pb2.MessageFromLeader(code=averaging_pb2.GROUP_IS_FULL)
                    return
            if self._node_label == "X":
                _emit("admission", arm=self._arm, channel=self._channel,
                      round=self._round_no(), leader="X", requester=requester,
                      requester_peer_id=requester_id, admitted=True,
                      reason="target_present_or_target_request")

            async for response in super().rpc_join_group(request, context):
                if response.code == averaging_pb2.BEGIN_ALLREDUCE:
                    excluded = False
                    if self._node_label == "X":
                        response, excluded, native_members, effective_members = announcement_for_recipient(
                            response, self._arm, requester, PeerID,
                            averaging_pb2.MessageFromLeader, self._label)
                    else:
                        native_members = [self._label(PeerID(raw)) for raw in response.ordered_peer_ids]
                        effective_members = list(native_members)
                    _emit("announcement_sent", arm=self._arm, channel=self._channel,
                          round=self._round_no(), leader="X", leader_peer_id=attacker_peer_id,
                          recipient=requester, recipient_peer_id=requester_id,
                          group_id=response.group_id.hex(), native_members=native_members,
                          effective_members=effective_members, excluded_target=excluded,
                          target_recipient=requester == "A")
                yield response

        async def _aggregate_with_group(self, group_info, min_vector_size, **kwargs):
            participant_view_members = [self._label(peer) for peer in group_info.peer_ids]
            leader_peer = getattr(self._matchmaking, "current_leader", None)
            leader = self._node_label if leader_peer is None else self._label(leader_peer)
            effective_group = group_info
            omitted = False
            if (self._node_label == "X" and self._arm == "capture_exclusion" and
                    "A" in participant_view_members):
                filtered_ids, filtered_data, omitted = smoke.filter_roster(
                    group_info.peer_ids, group_info.gathered, "A", self._label)
                if omitted:
                    effective_group = GroupInfo(group_info.group_id, filtered_ids, filtered_data)
            before = _tensor_values(self)
            effective_members = [self._label(peer) for peer in effective_group.peer_ids]
            round_no = self._round_no()
            _emit("aggregate_input", arm=self._arm, channel=self._channel, round=round_no,
                  peer=self._node_label, role="leader" if leader == self._node_label else "follower",
                  leader=leader, group_id=group_info.group_id.hex(),
                  participant_view_members=participant_view_members,
                  leader_native_members=(participant_view_members if leader == self._node_label else None),
                  effective_members=effective_members, target_omitted_locally=omitted,
                  tensor_before=before)
            try:
                result = await super()._aggregate_with_group(effective_group, min_vector_size, **kwargs)
            except BaseException as exc:
                _emit("aggregate_error", arm=self._arm, channel=self._channel, round=round_no,
                      peer=self._node_label, leader=leader, group_id=group_info.group_id.hex(),
                      error=repr(exc), traceback=traceback.format_exc())
                raise
            after = _tensor_values(self)
            _emit("aggregate_output", arm=self._arm, channel=self._channel, round=round_no,
                  peer=self._node_label, role="leader" if leader == self._node_label else "follower",
                  leader=leader, group_id=group_info.group_id.hex(),
                  participant_view_members=participant_view_members,
                  leader_native_members=(participant_view_members if leader == self._node_label else None),
                  effective_members=effective_members, target_omitted_locally=omitted,
                  tensor_before=before, tensor_after=after, status="returned")
            return result

    RecordHolderSmokeAverager.rpc_join_group.__annotations__ = {
        "request": averaging_pb2.JoinRequest,
        "context": P2PContext,
        "return": AsyncIterator[averaging_pb2.MessageFromLeader],
    }
    return RecordHolderSmokeAverager


def _run_arm(args) -> None:
    global _EVENTS, _START, _IDENTITIES, _FILTER
    from hivemind import DHT
    from hivemind.dht.routing import DHTID
    from hivemind.utils import get_dht_time
    import psutil
    import torch

    _FILTER = None  # participant DHTs are intentionally constructed unfiltered
    out = args.case_dir
    out.mkdir(parents=True, exist_ok=True)
    ctx = mp.get_context("fork")
    manager = ctx.Manager()
    identities = manager.dict()
    holder_peerids = manager.dict()
    filter_config = manager.dict({"ready": False, "keys": {}})
    events = manager.Queue()
    _EVENTS, _START, _IDENTITIES = events, time.monotonic(), identities
    torch.set_num_threads(1)
    all_events: list[dict[str, Any]] = []
    event_path = out / "events.jsonl"
    event_stream = event_path.open("w", encoding="utf-8", buffering=1)

    def drain() -> None:
        while True:
            try:
                event = events.get_nowait()
            except queue.Empty:
                break
            except Exception:
                break
            all_events.append(event)
            event_stream.write(json.dumps(event, sort_keys=True, allow_nan=False) + "\n")
            event_stream.flush()

    key_specs: dict[str, dict[str, Any]] = {}
    for channel in CHANNELS:
        prefix = f"rhsmoke_{args.label}_{channel}"
        if "." in prefix:
            raise ValueError("averaging prefixes cannot contain periods")
        key = f"{prefix}.0b"
        key_id = DHTID.generate(source=key)
        key_specs[channel] = {"prefix": prefix, "key": key,
                              "key_hex": key_id.to_bytes().hex(),
                              "target_id": int(key_id)}

    bootstrap = None
    holder_process = None
    holder_ready = ctx.Event()
    holder_stop = ctx.Event()
    dhts: dict[str, Any] = {}
    averagers: dict[tuple[str, str], Any] = {}
    controls: dict[tuple[str, str], Any] = {}
    owned: dict[int, float] = {}
    arm_start = smoke.clock_sample()
    arm_deadline = time.monotonic() + args.max_seconds
    error = None
    status = "failed"
    try:
        bootstrap = DHT(host_maddrs=LOCALHOST, auto_nat=False, nat_port_map=False,
                        use_relay=False, bucket_size=BUCKET_SIZE, num_replicas=NUM_REPLICAS,
                        start=True)
        bootstrap_addresses = _loopback_addresses(bootstrap, "bootstrap")
        identities[str(bootstrap.peer_id)] = "bootstrap"
        owned.update(smoke._owned_snapshot(psutil))
        _emit("bootstrap_started", peer_id=str(bootstrap.peer_id), addresses=bootstrap_addresses)

        holder_process = ctx.Process(
            target=_run_holders,
            args=(bootstrap_addresses, key_specs, identities, holder_peerids, filter_config,
                  events, holder_ready, holder_stop, args.label),
            name="recordholder-local-process",
            daemon=False,
        )
        holder_process.start()
        hold_deadline = time.monotonic() + min(70.0, args.max_seconds * 0.55)
        while not holder_ready.is_set() and holder_process.is_alive() and time.monotonic() < hold_deadline:
            drain()
            owned.update(smoke._owned_snapshot(psutil))
            time.sleep(0.05)
        if not holder_ready.is_set():
            raise TimeoutError("controlled record holders did not become ready before the startup cap")
        drain()
        if len(holder_peerids) != len(CHANNELS) * HOLDER_IDS_PER_KEY:
            raise RuntimeError(f"holder startup incomplete: {len(holder_peerids)} identities")

        # Participant DHTs are ordinary, unfiltered local peers. _FILTER stays None.
        if _FILTER is not None:
            raise RuntimeError("holder filter leaked into the participant process")
        for node in NODES:
            dht = DHT(initial_peers=bootstrap_addresses, host_maddrs=LOCALHOST,
                      auto_nat=False, nat_port_map=False, use_relay=False,
                      bucket_size=BUCKET_SIZE, num_replicas=NUM_REPLICAS, start=True)
            addresses = _loopback_addresses(dht, node)
            dhts[node] = dht
            identities[str(dht.peer_id)] = node
            owned.update(smoke._owned_snapshot(psutil))
            _emit("participant_dht_started", peer=node, peer_id=str(dht.peer_id), addresses=addresses,
                  filter_installed=False)

        attacker_peer_id = str(dhts["X"].peer_id)
        victim_peer_id = str(dhts["A"].peer_id)
        policies = {}
        for channel, spec in key_specs.items():
            policies[spec["key_hex"]] = {
                "arm": args.arm,
                "channel": channel,
                "filtered_requesters": ["A", "D", "C"],
                "allowed_peer_ids": [attacker_peer_id],
            }
        filter_config["keys"] = policies
        filter_config["ready"] = True

        # Allow local routing tables to settle before the first native lookup. This is
        # a fixed delay only; no DHT values are pre-seeded and no public bootstrap exists.
        settle_started = time.monotonic()
        while time.monotonic() - settle_started < args.topology_settle and time.monotonic() < arm_deadline:
            drain()
            time.sleep(0.05)
        if time.monotonic() >= arm_deadline:
            raise TimeoutError("topology settle exceeded the arm cap")
        _emit("topology_settled", seconds=args.topology_settle,
              bootstrap_peer_id=str(bootstrap.peer_id), holder_count=len(holder_peerids),
              participant_count=len(dhts), caching="native defaults")

        # Read-only native GroupKeyManager observation in the participant process.
        current_round = manager.dict()
        prefix_channels = {spec["prefix"]: channel for channel, spec in key_specs.items()}
        _install_candidate_observer(prefix_channels, current_round, identities, args.arm)
        averager_cls = _make_averager_class(identities, current_round, prefix_channels,
                                            args.arm, victim_peer_id, attacker_peer_id)
        round_values = {}
        for channel, spec in key_specs.items():
            for node in NODES:
                round_value = ctx.Value("i", -1)
                round_values[(node, channel)] = round_value
                initial = torch.tensor(marker_vector(node), dtype=torch.float32)
                averager = averager_cls(
                    [initial], dhts[node], prefix=spec["prefix"], start=True,
                    node_label=node, channel=channel, shared_round=round_value,
                    identity_map=identities, arm=args.arm,
                    target_group_size=4, min_group_size=2, initial_group_bits="",
                    min_matchmaking_time=0.35 if node == "X" else 3.0,
                    request_timeout=4.5, allreduce_timeout=args.allreduce_timeout,
                    sender_timeout=4.0, reducer_timeout=args.allreduce_timeout,
                    part_size_bytes=args.part_size_bytes, averaging_alpha=1.0,
                    allow_state_sharing=False, client_mode=False,
                    shutdown_timeout=0.75,
                )
                averagers[(node, channel)] = averager
                owned.update(smoke._owned_snapshot(psutil))

        expected_holders = {}
        for channel, spec in key_specs.items():
            peers = [holder_peerids[f"R_{channel}_{i:02d}"] for i in range(HOLDER_IDS_PER_KEY)]
            expected_holders[channel] = {
                "designed_replica_peer_ids": peers[:NUM_REPLICAS],
                "all_nearby_peer_ids": peers,
                "key": spec["key"], "key_hex": spec["key_hex"],
                "xor_distances": list(range(HOLDER_IDS_PER_KEY)),
            }
        config = {
            "label": args.label, "arm": args.arm, "nodes": list(NODES),
            "channels": list(CHANNELS), "rounds_requested": args.rounds,
            "channel_keys": key_specs, "holders": expected_holders,
            "dht": {"bucket_size": BUCKET_SIZE, "num_replicas": NUM_REPLICAS,
                    "cache_policy": "native defaults retained", "host_maddrs": LOCALHOST,
                    "bootstrap": "private loopback only"},
            "participants": {node: str(dhts[node].peer_id) for node in NODES},
            "holder_count": len(holder_peerids), "topology_settle_seconds": args.topology_settle,
            "group": {"target_group_size": 4, "minimum_group_size": 2,
                      "initial_group_bits": "", "fixed_unsharded_keys": True},
            "admission": "X holds D/C join RPCs until authenticated A is present in X's current followers; A is admitted directly",
            "record_filter": "only controlled holder DHTProtocol.rpc_find handlers; exact two key IDs; A/D/C see only X while X and other requesters receive native responses",
            "roster_exclusion": ("off" if args.arm == "capture_control" else
                                 "X removes A from its own GroupInfo and announcements to D/C; A receives full native roster"),
            "marker": {"vector_size": VECTOR_SIZE, "repeated_blocks": MARKER_BLOCKS,
                       "coordinates_per_block": MARKER_WIDTH, "reset_each_round": True,
                       "order": list(NODES), "part_size_bytes": args.part_size_bytes},
            "time_limits": {"arm_cap_seconds": args.max_seconds,
                            "round_timeout_seconds": args.round_timeout,
                            "allreduce_timeout_seconds": args.allreduce_timeout},
            "limits": ["not full Optimizer training", "not scheduled model state averaging",
                       "no progress tracking or recovery", "no WAN delay", "no public swarm",
                       "cold prepositioned holder topology; holder acquisition is not measured"],
        }
        smoke.write_json(out / "config.json", config)
        smoke.write_json(out / "identities.json", {
            "bootstrap": str(bootstrap.peer_id),
            "participants": config["participants"],
            "holders": dict(holder_peerids),
        })

        for channel in CHANNELS:
            for round_no in range(args.rounds):
                if time.monotonic() >= arm_deadline:
                    _emit("arm_measurement_deadline", channel=channel, round=round_no)
                    raise TimeoutError("arm measurement cap reached")
                before_clock = smoke.clock_sample()
                scheduled = get_dht_time() + args.scheduled_lead
                round_controls = {}
                for node in NODES:
                    key = (node, channel)
                    _set_marker(averagers[key], node)
                    round_values[key].value = round_no
                    current_round[(node, channel)] = round_no
                    _emit("step_started", arm=args.arm, channel=channel, round=round_no,
                          peer=node, group_key=key_specs[channel]["key"],
                          scheduled_time=scheduled, input_marker=marker_vector(node))
                    control = averagers[key].step(
                        gather={"peer": node, "channel": channel, "round": round_no},
                        scheduled_time=scheduled, weight=1.0, timeout=args.round_timeout,
                        allow_retries=False, wait=False,
                    )
                    controls[key] = control
                    round_controls[key] = control

                round_deadline = min(time.monotonic() + args.round_timeout + 3.0, arm_deadline)
                pending = set(round_controls)
                while pending and time.monotonic() < round_deadline:
                    drain()
                    for key in list(pending):
                        control = round_controls[key]
                        if not control.done():
                            continue
                        node, channel_name = key
                        try:
                            result = control.result()
                            result_status = "success" if result is not None else "no_group"
                            error_text = None
                        except BaseException as exc:
                            result_status, error_text = "error", repr(exc)
                        try:
                            output = _tensor_values(averagers[key])
                        except BaseException as exc:
                            output = None
                            error_text = f"{error_text}; output read failed: {exc!r}" if error_text else repr(exc)
                        _emit("step_result", arm=args.arm, channel=channel_name, round=round_no,
                              peer=node, status=result_status, stage=_stage_name(control.stage),
                              error=error_text, output=output,
                              target_origin_coordinates=(output[0::MARKER_WIDTH] if output is not None else None))
                        pending.remove(key)
                    if pending:
                        time.sleep(0.025)
                if pending:
                    for key in sorted(pending):
                        control = round_controls[key]
                        try:
                            control.cancel()
                        except BaseException:
                            pass
                        node, channel_name = key
                        _emit("step_result", arm=args.arm, channel=channel_name, round=round_no,
                              peer=node, status="round_deadline_cancelled",
                              stage=_stage_name(control.stage), error=None,
                              output=_tensor_values(averagers[key]))
                    drain()
                    raise TimeoutError(f"round deadline reached with pending steps: {sorted(pending)}")
                audit = smoke.clock_delta(before_clock, smoke.clock_sample())
                _emit("round_clock_audit", arm=args.arm, channel=channel, round=round_no, **audit)
                if not audit["valid"]:
                    raise RuntimeError(f"suspend or wall-clock jump detected: {audit}")
                drain()

        status = "complete"
    except BaseException as exc:
        error = repr(exc)
        _emit("worker_error", arm=args.arm, error=error, traceback=traceback.format_exc())
    finally:
        for control in controls.values():
            try:
                if not control.done():
                    control.cancel()
            except BaseException:
                pass
        for averager in reversed(list(averagers.values())):
            try:
                averager.shutdown()
            except BaseException as exc:
                _emit("cleanup_error", component="averager", error=repr(exc))
        for dht in reversed(list(dhts.values())):
            try:
                dht.shutdown()
            except BaseException as exc:
                _emit("cleanup_error", component="participant_dht", error=repr(exc))
        if holder_process is not None:
            holder_stop.set()
            holder_process.join(timeout=12.0)
            if holder_process.is_alive():
                holder_process.terminate()
                holder_process.join(timeout=2.0)
            if holder_process.is_alive():
                holder_process.kill()
                holder_process.join(timeout=2.0)
        if bootstrap is not None:
            try:
                bootstrap.shutdown()
            except BaseException as exc:
                _emit("cleanup_error", component="bootstrap", error=repr(exc))
        owned.update(smoke._owned_snapshot(psutil))
        drain()
        arm_clock = smoke.clock_delta(arm_start, smoke.clock_sample())
        event_stream.flush()
        event_stream.close()
        write_status = {"arm": args.arm, "status": status if not error else "failed",
                        "error": error, "elapsed_seconds": elapsed(), "clock_audit": arm_clock,
                        "holder_worker_exitcode": holder_process.exitcode if holder_process is not None else None,
                        "owned_processes": {str(pid): created for pid, created in owned.items()}}
        smoke.write_json(out / "status.json", write_status)
        try:
            events.close()
            events.join_thread()
        except BaseException:
            pass
        try:
            manager.shutdown()
        except BaseException:
            pass
    if error:
        raise RuntimeError(error)


def summarize_events(events: Sequence[Mapping[str, Any]], arm: str, rounds: int) -> dict[str, Any]:
    channel_rows = []
    for channel in CHANNELS:
        steps = [e for e in events if e.get("kind") == "step_result" and
                 e.get("arm") == arm and e.get("channel") == channel]
        outputs = [e for e in events if e.get("kind") == "aggregate_output" and
                   e.get("arm") == arm and e.get("channel") == channel]
        inputs = [e for e in events if e.get("kind") == "aggregate_input" and
                  e.get("arm") == arm and e.get("channel") == channel]
        announcements = [e for e in events if e.get("kind") == "announcement_sent" and
                         e.get("arm") == arm and e.get("channel") == channel]
        step_starts = [e for e in events if e.get("kind") == "step_started" and
                       e.get("arm") == arm and e.get("channel") == channel]
        output_index = {(int(e["round"]), str(e["peer"])): e for e in outputs}
        input_index = {(int(e["round"]), str(e["peer"])): e for e in inputs}
        announcement_index = {(int(e["round"]), str(e["recipient"])): e for e in announcements}
        step_input_index = {(int(e["round"]), str(e["peer"])): e["input_marker"]
                            for e in step_starts}
        node_results = []
        for peer in NODES:
            peer_steps = [e for e in steps if e.get("peer") == peer]
            successful = []
            for step in peer_steps:
                round_no = int(step["round"])
                output = output_index.get((round_no, peer))
                input_event = input_index.get((round_no, peer))
                effective_members = (output or input_event or {}).get("effective_members", [])
                saved_inputs = {member: step_input_index.get((round_no, member))
                                for member in effective_members}
                expected = (mean_vectors(list(saved_inputs.values()))
                            if effective_members and all(vector is not None for vector in saved_inputs.values())
                            else None)
                actual = (output or {}).get("tensor_after") or step.get("output")
                completed = step.get("status") == "success" and output is not None
                exact = bool(completed and expected is not None and vector_matches(actual, expected))
                leader_announcement = announcement_index.get((round_no, peer))
                leader_native_members = (output or input_event or {}).get("leader_native_members")
                if leader_announcement is not None:
                    leader_native_members = leader_announcement.get("native_members", leader_native_members)
                native_full = bool(leader_native_members and set(leader_native_members) == set(NODES))
                if completed:
                    successful.append({
                        "round": round_no,
                        "leader": (output or {}).get("leader"),
                        "group_id": (output or {}).get("group_id"),
                        "local_effective_members": effective_members,
                        "leader_native_members": leader_native_members,
                        "leader_native_group_full": native_full,
                        "received_roster": (leader_announcement or {}).get("effective_members"),
                        "saved_group_input_vectors": saved_inputs,
                        "expected_effective_mean": expected,
                        "actual_output": actual,
                        "matches_full_effective_mean": exact,
                        "a_origin_coordinates": (actual[0::MARKER_WIDTH] if actual else None),
                        "a_origin_zero": bool(actual is not None and all(abs(float(v)) <= 1e-4
                                                                         for v in actual[0::MARKER_WIDTH])),
                    })
            node_results.append({
                "peer": peer,
                "step_attempts": len(peer_steps),
                "successes": sum(e.get("status") == "success" for e in peer_steps),
                "no_group": sum(e.get("status") == "no_group" for e in peer_steps),
                "errors_or_timeouts": sum(e.get("status") not in ("success", "no_group") for e in peer_steps),
                "outputs": successful,
            })
        dc_rows = [row for row in node_results if row["peer"] in ("D", "C")]
        dc_successes = [output for row in dc_rows for output in row["outputs"]]
        dc_full_native = [output for output in dc_successes if output["leader_native_group_full"]]
        dc_partial_native = [output for output in dc_successes if not output["leader_native_group_full"]]
        x_announcements = [e for e in announcements if e.get("leader") == "X"]
        holder_reads = [e for e in events if e.get("kind") == "holder_find" and
                        e.get("arm") == arm and e.get("channel") == channel]
        candidate_rows = [e for e in events if e.get("kind") == "candidate_list" and
                          e.get("arm") == arm and e.get("channel") == channel]
        channel_rows.append({
            "channel": channel, "rounds_requested": rounds,
            "step_attempts": len(steps), "step_successes": sum(e.get("status") == "success" for e in steps),
            "step_no_group": sum(e.get("status") == "no_group" for e in steps),
            "announcement_count": len(announcements),
            "attacker_leader_full_native_groups": len({(int(e["round"]), str(e["group_id"]))
                                                        for e in x_announcements
                                                        if set(e.get("native_members", [])) == set(NODES)}),
            "attacker_announcements_to_D_or_C": [
                {"round": int(e["round"]), "recipient": e["recipient"],
                 "group_id": e["group_id"], "native_members": e["native_members"],
                 "effective_members": e["effective_members"],
                 "excluded_target": e["excluded_target"]}
                for e in x_announcements if e.get("recipient") in ("D", "C")],
            "honest_D_C_successful_outputs": dc_successes,
            "honest_D_C_full_native_group_outputs": dc_full_native,
            "honest_D_C_partial_or_unconfirmed_group_outputs": dc_partial_native,
            "honest_D_C_exact_effective_mean_count": sum(e["matches_full_effective_mean"] for e in dc_full_native),
            "honest_D_C_zero_A_origin_count": sum(e["a_origin_zero"] for e in dc_full_native),
            "holder_find_events": len(holder_reads),
            "holder_contacts_by_requester": {
                peer: sum(e.get("requester") == peer for e in holder_reads) for peer in NODES
            },
            "holder_filter_events": sum(e.get("filtered") for e in holder_reads),
            "holder_storage_hits": sum(e.get("storage_present") for e in holder_reads),
            "holder_cache_hits": sum(e.get("cache_present") for e in holder_reads),
            "controlled_holders_with_storage_hits": sorted({e["holder"] for e in holder_reads
                                                              if e.get("storage_present")}),
            "controlled_holders_with_cache_hits": sorted({e["holder"] for e in holder_reads
                                                            if e.get("cache_present")}),
            "candidate_list_observations": [
                {"peer": e["peer"], "round": int(e["round"]), "candidates": e["candidates"]}
                for e in candidate_rows],
            "nodes": node_results,
        })
    return {"arm": arm, "channels": channel_rows}


def _source_manifest() -> dict[str, str]:
    paths = {
        "script": SCRIPT,
        "tracking_combination_helper": HELPER_PATH,
        "step6_record_holder_filter_reference": ROOT / "research/hivemind/step6_isolation.py",
        "step3_crafted_id_reference": ROOT / "research/hivemind/step3_eclipse.py",
    }
    return {name: sha256(path) for name, path in paths.items()}


def _run_isolated_worker(args, arm: str, out: Path, psutil) -> dict[str, Any]:
    command = [sys.executable, str(SCRIPT), "--_worker", "--label", args.label,
               "--arm", arm, "--case-dir", str(out),
               "--package-root", str(args.package_root), "--dependencies", str(args.dependencies),
               "--rounds", str(args.rounds), "--round-timeout", str(args.round_timeout),
               "--allreduce-timeout", str(args.allreduce_timeout),
               "--max-seconds", str(args.max_seconds), "--topology-settle", str(args.topology_settle),
               "--scheduled-lead", str(args.scheduled_lead),
               "--part-size-bytes", str(args.part_size_bytes)]
    out.mkdir(parents=True, exist_ok=False)
    started = smoke.clock_sample()
    observed: dict[int, float] = {}
    with (out / "worker.log").open("wb") as log:
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT,
                                   start_new_session=True, cwd=str(ROOT))
        deadline = time.monotonic() + args.max_seconds + 30.0
        suspended = False
        previous = smoke.clock_sample()
        while process.poll() is None and time.monotonic() < deadline:
            try:
                for child in psutil.Process(process.pid).children(recursive=True):
                    observed[child.pid] = child.create_time()
            except psutil.Error:
                pass
            current = smoke.clock_sample()
            delta = smoke.clock_delta(previous, current)
            previous = current
            if not delta["valid"]:
                suspended = True
                break
            time.sleep(0.1)
        timed_out = process.poll() is None
        if timed_out:
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                process.wait(timeout=3.0)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.wait(timeout=3.0)
        exitcode = process.wait()
    try:
        owned = json.loads((out / "status.json").read_text()).get("owned_processes", {})
        observed.update({int(pid): float(created) for pid, created in owned.items()})
    except (OSError, ValueError, TypeError):
        pass
    process_audit = smoke._audit_child_processes(psutil, process.pid, observed)
    audit = {"arm": arm, "worker_exitcode": exitcode, "worker_timed_out": timed_out,
             "suspension_detected_by_controller": suspended,
             "controller_clock_audit": smoke.clock_delta(started, smoke.clock_sample()),
             "worker_process_audit": process_audit}
    smoke.write_json(out / "controller_audit.json", audit)
    status_path = out / "status.json"
    if status_path.exists():
        status = json.loads(status_path.read_text())
        status["controller_audit"] = audit
        clocks_valid = (audit["controller_clock_audit"].get("valid", False) and
                        status.get("clock_audit", {}).get("valid", False))
        status["status"] = "complete" if (exitcode == 0 and status.get("status") == "complete" and
                                             process_audit["safe"] and not suspended and clocks_valid) else "failed"
        smoke.write_json(status_path, status)
    return audit


def _render_report(report: Mapping[str, Any]) -> str:
    lines = [f"# Record-holder exclusion smoke: {report['label']}", "",
             f"Status: **{report['status']}**", "",
             "Two private loopback arms use the same controlled-holder filtering, X leadership policy, and X admission hold. The exclusion arm additionally removes A from X's own roster and from announcements to D/C. Each channel uses a fixed unsharded native averaging key and 192-coordinate markers reset before every round.", "",
             "| Arm | Channel | Step successes / attempts | X announcements with all four native peers | D/C complete effective means | D/C zero A-origin outputs | Holder Find requests | Actual filtered holder responses |",
             "|---|---|---:|---:|---:|---:|---:|---:|"]
    for arm in report["arm_summaries"]:
        for row in arm["channels"]:
            lines.append(f"| {arm['arm']} | {row['channel']} | {row['step_successes']} / {row['step_attempts']} | {row['attacker_leader_full_native_groups']} | {row['honest_D_C_exact_effective_mean_count']} | {row['honest_D_C_zero_A_origin_count']} | {row['holder_find_events']} | {row['holder_filter_events']} |")
    lines += ["", "The DHT filter is installed only in the dedicated local holder process and checks the exact channel key plus authenticated requester PeerID. The DHT-only identities at XOR distances 0–19 make a fixed nearest-first fixture; the first five are the designed replicas under the recorded five-replica setting. Raw holder responses distinguish stored values from cached values, and requests from each participant establish which controlled identities actually answered.", "",
              "Read D/C outcomes only when a successful native step, a full four-peer native group, and its actual effective roster are all recorded. A zero A coefficient is insufficient by itself: each complete D/C output is also checked coordinate by coordinate against the expected equal-weight mean of the effective roster's fresh inputs. No-group attempts, errors, malformed outputs, and incomplete groups remain in the denominator.", "",
              "Scope: native decentralized averagers with fixed keys, default DHT caching, local loopback, and a cold prepositioned holder topology. This does not measure acquiring holder control, training convergence, scheduled Optimizer state averaging, recovery, WAN behavior, or public-swarm success rates.", ""]
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--label", default=None, help="unique local output label; existing output is never overwritten")
    parser.add_argument("--output-root", type=Path, default=ROOT / "research/hivemind/results")
    parser.add_argument("--package-root", type=Path,
                        default=ROOT / "research/simulator/.native-runtime/root_snapshot_a")
    parser.add_argument("--dependencies", type=Path,
                        default=ROOT / "research/agora/.runtime/venv/lib/python3.11/site-packages")
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--round-timeout", type=float, default=10.0)
    parser.add_argument("--allreduce-timeout", type=float, default=8.0)
    parser.add_argument("--max-seconds", type=float, default=100.0)
    parser.add_argument("--topology-settle", type=float, default=9.0)
    parser.add_argument("--scheduled-lead", type=float, default=1.5)
    parser.add_argument("--part-size-bytes", type=int, default=64)
    parser.add_argument("--_worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--arm", choices=ARMS, help=argparse.SUPPRESS)
    parser.add_argument("--case-dir", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.rounds < 1 or args.rounds > 3:
        parser.error("rounds must be between 1 and 3")
    if not 10 <= args.max_seconds <= 120:
        parser.error("max-seconds must be between 10 and 120")
    if not 0 <= args.topology_settle <= 15:
        parser.error("topology-settle must be between 0 and 15 seconds")
    if args.round_timeout <= args.scheduled_lead or args.allreduce_timeout <= 0:
        parser.error("round timeout must exceed the scheduled lead and all-reduce timeout must be positive")
    if args.part_size_bytes < 16 or args.part_size_bytes % 16:
        parser.error("part-size-bytes must be a positive multiple of the 16-byte marker block")
    if args.label is None:
        args.label = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    if not args.label.replace("_", "").replace("-", "").isalnum():
        parser.error("label may contain only letters, digits, underscores, and hyphens")
    if len(args.label) > 32:
        parser.error("label must be at most 32 characters so DHT keys remain bounded")
    if not args._worker:
        for path_arg in (args.package_root, args.dependencies):
            if not path_arg.resolve().is_relative_to(ROOT):
                parser.error("native package and dependencies must be local workspace paths")

    if args._worker:
        if args.arm is None or args.case_dir is None:
            parser.error("internal worker requires --arm and --case-dir")
        args.provenance = smoke.configure_native(args)
        args.provenance["recordholder_script_sha256"] = sha256(SCRIPT)
        args.provenance["tracking_helper_sha256"] = sha256(HELPER_PATH)
        args.provenance["holder_filter_reference_sha256"] = sha256(ROOT / "research/hivemind/step6_isolation.py")
        args.provenance["crafted_id_reference_sha256"] = sha256(ROOT / "research/hivemind/step3_eclipse.py")
        mp.set_start_method("fork", force=True)
        args.case_dir.mkdir(parents=True, exist_ok=True)
        smoke.write_json(args.case_dir / "provenance.json", args.provenance)
        with tarfile.open(args.case_dir / "frozen_sources.tar.gz", "w:gz") as archive:
            for path, arcname in (
                (SCRIPT, "recordholder_exclusion_smoke.py"),
                (HELPER_PATH, "tracking_combination_smoke.py"),
                (ROOT / "research/hivemind/step6_isolation.py", "step6_isolation.py"),
                (ROOT / "research/hivemind/step3_eclipse.py", "step3_eclipse.py"),
            ):
                archive.add(path, arcname=arcname)
            for relative in smoke.NATIVE_FILES:
                imported = Path(args.provenance["package"]) / relative
                if imported.exists():
                    archive.add(imported, arcname=f"native_snapshot/{relative}")
        try:
            _run_arm(args)
            return 0
        except BaseException:
            return 1

    args.output_root.mkdir(parents=True, exist_ok=True)
    root = args.output_root / f"recordholder_exclusion_smoke_{args.label}"
    root.mkdir(exist_ok=False)
    sys.path.insert(0, str(args.dependencies.resolve()))
    import psutil

    sources = _source_manifest()
    smoke.write_json(root / "source_manifest.json", sources)
    smoke.write_json(root / "config.json", {
        "label": args.label, "arms": list(ARMS), "channels": list(CHANNELS),
        "rounds": args.rounds, "round_timeout_seconds": args.round_timeout,
        "arm_hard_measurement_cap_seconds": args.max_seconds,
        "expected_total_wall_seconds_upper_bound": 2 * (args.max_seconds + 30.0) + 25.0,
        "native_package_root": str(args.package_root.resolve()),
        "dependencies": str(args.dependencies.resolve()),
        "network_scope": "loopback-only private DHT; no remote bootstrap or address inputs",
        "claim_boundary": "native holder-response, leader-roster, and marker exclusion mechanism smoke",
    })
    arm_audits = []
    for arm in ARMS:
        if _source_manifest() != sources:
            raise RuntimeError("script/helper/reference source changed between arms")
        arm_dir = root / "arms" / arm
        arm_dir.parent.mkdir(parents=True, exist_ok=True)
        arm_audits.append(_run_isolated_worker(args, arm, arm_dir, psutil))

    raw_events = []
    statuses = {}
    for arm in ARMS:
        arm_dir = root / "arms" / arm
        status_path = arm_dir / "status.json"
        statuses[arm] = (json.loads(status_path.read_text()) if status_path.exists()
                         else {"status": "missing"})
        event_path = arm_dir / "events.jsonl"
        if event_path.exists():
            for line in event_path.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    raw_events.append(json.loads(line))
    arm_summaries = [summarize_events(raw_events, arm, args.rounds) for arm in ARMS]
    report = {
        "label": args.label,
        "status": "complete" if all(statuses[arm].get("status") == "complete" for arm in ARMS) else "incomplete",
        "arm_status": statuses, "controller_audits": arm_audits,
        "arm_summaries": arm_summaries,
        "source_manifest": sources,
        "interpretation": {
            "matched_difference": "roster exclusion on versus off; DHT response filtering and leader/admission policy are shared",
            "mechanism_success_requires": ["controlled holder response filtered for A/D/C on exact key",
                                            "X leads a complete native four-peer group",
                                            "D/C receive the effective roster excluding A",
                                            "successful D/C outputs match the expected effective-roster mean"],
            "not_estimated": ["public deployment success rates", "holder acquisition cost", "training convergence",
                              "Optimizer state averaging schedule", "recovery", "WAN conditions"],
        },
    }
    smoke.write_json(root / "report.json", report)
    (root / "report.md").write_text(_render_report(report), encoding="utf-8")
    smoke.write_json(root / "artifact_inventory.json", smoke.inventory(root))
    print(json.dumps({"output": str(root), "status": report["status"],
                      "report": str(root / "report.json")}, indent=2))
    return 0 if report["status"] == "complete" else 1


if __name__ == "__main__":
    raise SystemExit(main())
