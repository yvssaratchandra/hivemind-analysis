#!/usr/bin/env python3
"""Bounded local smoke for composing victim tracking, admission, and roster omission.

This uses eight loopback DHT peers and two independent native
DecentralizedAverager marker channels (``gradient_probe`` and ``state_probe``).
The latter is a second averaging channel fixture, not Optimizer's scheduled
TrainingStateAverager.  Each arm has a 90 second hard measurement cap.

The bootstrap places A and attacker X in the same shard so X can observe A's
first native group.  That assistance is recorded in config.json.  Group size 2
means a captured group contains only A and X: the exclusion arm tests X's local
effective roster, while A still receives the native roster.  It cannot show an
honest follower receiving an attacker-rewritten announcement.

Use the prepared local native snapshot/runtime, for example:
    PYTHONPATH=research/agora/.runtime/venv/lib/python3.11/site-packages \\
      python3.11 research/hivemind/tracking_combination_smoke.py --label waters_20261021_a

No public bootstrap address is accepted; every advertised listener is checked
to be loopback before peers are constructed.
"""
from __future__ import annotations

import argparse
import asyncio
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import multiprocessing as mp
import os
from pathlib import Path
import signal
import subprocess
import sys
import tarfile
import time
import traceback
from typing import Any, AsyncIterator, Mapping, Sequence

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = Path(__file__).resolve()
NODES = ("A", "X", "H2", "H3", "H4", "H5", "H6", "H7")
VICTIM = "A"
ATTACKER = "X"
CHANNELS = ("gradient_probe", "state_probe")
ARMS = ("capture_control", "capture_exclusion")
LOCALHOST = ["/ip4/127.0.0.1/tcp/0"]
NATIVE_FILES = (
    "averaging/averager.py",
    "averaging/matchmaking.py",
    "averaging/key_manager.py",
    "averaging/group_info.py",
    "averaging/allreduce.py",
    "averaging/partition.py",
    "p2p/servicer.py",
    "proto/averaging.proto",
)

# Set in each isolated native worker before constructing DHTs/averagers.
EVENTS = None
START = 0.0
IDENTITIES: dict[str, str] = {}
PREFIX_INFO: dict[str, dict[str, Any]] = {}
ROUND_VALUES: dict[tuple[str, str], Any] = {}
LEADERS: dict[tuple[str, str], str] = {}
EXCLUDE_TARGET = False


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def clock_sample() -> dict[str, float]:
    boot = time.clock_gettime(time.CLOCK_BOOTTIME) if hasattr(time, "CLOCK_BOOTTIME") else time.monotonic()
    return {"monotonic": time.monotonic(), "boottime": boot, "wall": time.time()}


def clock_delta(before: Mapping[str, float], after: Mapping[str, float]) -> dict[str, Any]:
    suspend = (after["boottime"] - before["boottime"]) - (after["monotonic"] - before["monotonic"])
    wall_jump = (after["wall"] - before["wall"]) - (after["boottime"] - before["boottime"])
    return {"monotonic_elapsed": after["monotonic"] - before["monotonic"],
            "boottime_elapsed": after["boottime"] - before["boottime"],
            "wall_elapsed": after["wall"] - before["wall"],
            "suspend_seconds": suspend, "wall_jump_seconds": wall_jump,
            "valid": abs(suspend) <= 1.0 and abs(wall_jump) <= 1.0}


def filter_roster(peer_ids: Sequence[Any], gathered: Sequence[bytes], excluded: Any,
                  name_peer) -> tuple[tuple[Any, ...], tuple[bytes, ...], bool]:
    """Remove one peer while preserving its peers' opaque metadata alignment."""
    if len(peer_ids) != len(gathered):
        raise ValueError("peer IDs and gathered metadata lengths differ")
    keep = [i for i, peer in enumerate(peer_ids) if name_peer(peer) != excluded]
    return tuple(peer_ids[i] for i in keep), tuple(gathered[i] for i in keep), len(keep) != len(peer_ids)


def _stage_label(stage: Any) -> str:
    """Return a JSON-safe label for Hivemind's plain Enum averaging stage."""
    name = getattr(stage, "name", None)
    if name is not None:
        return str(name)
    value = getattr(stage, "value", stage)
    return str(value)


def summarize_events(events: Sequence[Mapping[str, Any]], arm: str, channel: str,
                     rounds: int, victim: str = VICTIM, attacker: str = ATTACKER) -> dict[str, Any]:
    """Summarize capture, next-bucket prediction, and attacker-local omission separately."""
    channel_events = [e for e in events if e.get("arm") == arm and e.get("channel") == channel]
    captures: dict[int, dict[str, Any]] = {}
    for event in channel_events:
        if event.get("kind") != "aggregate_view" or event.get("peer") != attacker:
            continue
        members = event.get("native_members", [])
        if victim in members and attacker in members and event.get("leader") == attacker:
            captures[int(event["round"])] = event

    capture_rounds = sorted(captures)
    longest = current = 0
    previous = None
    for round_no in capture_rounds:
        current = current + 1 if previous is not None and round_no == previous + 1 else 1
        longest = max(longest, current)
        previous = round_no

    advertisements: dict[tuple[int, str], list[str]] = {}
    for event in channel_events:
        if event.get("kind") == "bucket_advertisement" and event.get("looking_for_group"):
            key = (int(event["round"]), event["peer"])
            advertisements.setdefault(key, []).append(str(event["bucket"]))

    tracking = []
    for round_no, event in sorted(captures.items()):
        next_round = round_no + 1
        victim_next = advertisements.get((next_round, victim), [])
        attacker_next = advertisements.get((next_round, attacker), [])
        predicted = event.get("predicted_victim_next_bits")
        tracking.append({"round": round_no, "predicted_victim_next_bits": predicted,
                         "victim_next_advertised": victim_next,
                         "attacker_next_advertised": attacker_next,
                         "prediction_matches_victim": bool(predicted is not None and predicted in victim_next),
                         "attacker_tracks_victim": bool(predicted is not None and predicted in attacker_next)})

    attacker_views = [e for e in channel_events if e.get("kind") == "aggregate_view" and e.get("peer") == attacker]
    omitted = [e for e in attacker_views if victim in e.get("native_members", []) and
               victim not in e.get("effective_members", [])]
    attempt_rows = [e for e in channel_events if e.get("kind") == "step_result"]
    aggregate_outputs = {
        (int(event["round"]), str(event.get("peer")), str(event.get("group_id"))): event
        for event in channel_events if event.get("kind") == "aggregate_output"
    }
    omission_coefficients = []
    for view in omitted:
        output = aggregate_outputs.get((int(view["round"]), attacker, str(view.get("group_id"))), {})
        omission_coefficients.append(output.get("target_coefficient_after"))
    return {
        "arm": arm, "channel": channel, "rounds_requested": rounds,
        "step_attempt_count": len(attempt_rows),
        "step_attempts": attempt_rows,
        "captured_rounds_with_attacker_as_leader": capture_rounds,
        "capture_round_count": len(capture_rounds),
        "longest_consecutive_capture_streak": longest,
        "tracking_checks": tracking,
        "successful_victim_next_bucket_predictions": sum(x["prediction_matches_victim"] for x in tracking),
        "attacker_next_bucket_matches": sum(x["attacker_tracks_victim"] for x in tracking),
        "attacker_local_omission_rounds": [int(e["round"]) for e in omitted],
        "attacker_local_omission_target_coefficients": omission_coefficients,
        "target_marker_coefficients_by_peer": {
            peer: [{"round": int(event["round"]), "status": event.get("status"),
                    "target_coefficient": event.get("target_coefficient")}
                   for event in attempt_rows if event.get("peer") == peer]
            for peer in NODES
        },
        "victim_received_full_native_roster": True,
        "honest_follower_received_rewritten_roster_tested": False,
        "limitations": [
            "group_size=2 leaves no honest follower in a captured group",
            "the state_probe is a separately matched DecentralizedAverager channel, not Optimizer TrainingStateAverager scheduling",
            "no training, progress tracking, state recovery, WAN delay, or public swarm behavior is tested",
        ],
    }


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")


def inventory(root: Path) -> dict[str, str]:
    return {str(path.relative_to(root)): sha256(path) for path in sorted(root.rglob("*"))
            if path.is_file() and path.name != "artifact_inventory.json"}


def configure_native(args) -> dict[str, Any]:
    os.chdir("/tmp")
    sys.path[:] = [entry for entry in sys.path if entry and Path(entry).resolve() != ROOT]
    sys.path.insert(0, str(args.dependencies.resolve()))
    sys.path.insert(0, str(args.package_root.resolve()))
    import importlib.metadata
    import hivemind
    import torch
    import psutil
    package = Path(hivemind.__file__).resolve().parent
    if package.parent != args.package_root.resolve():
        raise RuntimeError(f"unexpected Hivemind import path: {package}")
    hashes = {}
    for relative in NATIVE_FILES:
        imported = package / relative
        source = ROOT / "hivemind" / relative
        if not imported.exists():
            if relative.endswith(".proto"):
                continue
            raise RuntimeError(f"native source missing: {imported}")
        if source.exists() and source.read_bytes() != imported.read_bytes():
            raise RuntimeError(f"native snapshot differs from repository source: {relative}")
        hashes[relative] = sha256(imported)
    binary = package / "hivemind_cli" / "p2pd"
    if not os.access(binary, os.X_OK):
        raise RuntimeError(f"local p2pd is missing or not executable: {binary}")
    return {"package": str(package), "hivemind_version": getattr(hivemind, "__version__", "unknown"),
            "python": sys.version, "python_executable": sys.executable,
            "torch": torch.__version__, "dependency_versions": {
                name: importlib.metadata.version(name) for name in ("numpy", "protobuf", "grpcio", "psutil")},
            "native_source_sha256": hashes, "p2pd_sha256": sha256(binary),
            "probe_script_sha256": sha256(SCRIPT),
            "tracking_reference_sha256": sha256(ROOT / "research/hivemind/step4_reformation.py"),
            "admission_reference_sha256": sha256(ROOT / "research/hivemind/step3_capture.py"),
            "announcement_reference_sha256": sha256(ROOT / "research/hivemind/compute_native_hooks.py"),
            "psutil_version": psutil.__version__}


def _emit(kind: str, **fields: Any) -> None:
    if EVENTS is None:
        return
    payload = {"kind": kind, "time": time.monotonic() - START, "pid": os.getpid(), **fields}
    try:
        EVENTS.put(payload)
    except Exception:
        pass


def _label(peer_id: Any) -> str:
    try:
        return IDENTITIES.get(str(peer_id), str(peer_id))
    except Exception:
        return str(peer_id)


def install_matchmaking_observers() -> None:
    """Record native leadership and each process's received announcement roster."""
    from hivemind.averaging.matchmaking import Matchmaking

    leader_assemble = Matchmaking.leader_assemble_group
    follower_assemble = Matchmaking.follower_assemble_group

    async def observed_leader(self):
        group = await leader_assemble(self)
        prefix = str(self._prefix)
        info = PREFIX_INFO.get(prefix, {})
        round_value = ROUND_VALUES.get((prefix, str(self.peer_id)))
        round_no = int(round_value.value) if round_value is not None else -1
        leader = _label(self.peer_id)
        LEADERS[(prefix, group.group_id.hex())] = leader
        _emit("group_role", arm=info.get("arm"), channel=info.get("channel"), round=round_no,
              peer=leader, role="leader", leader=leader, group_id=group.group_id.hex(),
              native_members=[_label(peer) for peer in group.peer_ids])
        return group

    async def observed_follower(self, leader, message):
        group = await follower_assemble(self, leader, message)
        prefix = str(self._prefix)
        info = PREFIX_INFO.get(prefix, {})
        round_value = ROUND_VALUES.get((prefix, str(self.peer_id)))
        round_no = int(round_value.value) if round_value is not None else -1
        leader_name = _label(leader)
        LEADERS[(prefix, group.group_id.hex())] = leader_name
        _emit("group_role", arm=info.get("arm"), channel=info.get("channel"), round=round_no,
              peer=_label(self.peer_id), role="follower", leader=leader_name,
              group_id=group.group_id.hex(), native_members=[_label(peer) for peer in group.peer_ids],
              received_roster=[_label(peer) for peer in group.peer_ids])
        return group

    Matchmaking.leader_assemble_group = observed_leader
    Matchmaking.follower_assemble_group = observed_follower


def install_bucket_observer() -> None:
    """Observe native bucket transitions and declarations from both probe channels."""
    from hivemind.averaging.key_manager import GroupKeyManager

    update = GroupKeyManager.update_key_on_group_assembled
    declare = GroupKeyManager.declare_averager

    async def observed_update(self, group_info):
        before = self.group_bits
        result = await update(self, group_info)
        prefix = str(self.prefix)
        info = PREFIX_INFO.get(prefix, {})
        round_value = ROUND_VALUES.get((prefix, str(self.peer_id)))
        _emit("bucket_transition", arm=info.get("arm"), channel=info.get("channel"),
              round=int(round_value.value) if round_value is not None else -1,
              peer=_label(self.peer_id), group_id=group_info.group_id.hex(),
              bucket_before=before, bucket_native_next=self.group_bits)
        return result

    async def observed_declare(self, group_key, peer_id, expiration_time, looking_for_group=True):
        result = await declare(self, group_key, peer_id, expiration_time, looking_for_group)
        prefix = str(self.prefix)
        info = PREFIX_INFO.get(prefix, {})
        round_value = ROUND_VALUES.get((prefix, str(peer_id)))
        _emit("bucket_advertisement", arm=info.get("arm"), channel=info.get("channel"),
              round=int(round_value.value) if round_value is not None else -1,
              peer=_label(peer_id), bucket=str(group_key).rsplit(".0b", 1)[-1],
              group_key=str(group_key), looking_for_group=bool(looking_for_group),
              declared=bool(result), expiration_time=float(expiration_time))
        return result

    GroupKeyManager.update_key_on_group_assembled = observed_update
    GroupKeyManager.declare_averager = observed_declare


def _tensor_values(averager) -> list[float]:
    with averager.get_tensors() as tensors:
        return [float(value) for value in tensors[0].detach().cpu().reshape(-1).tolist()]


def _rewrite_message(message, excluded_name: str, PeerID, averaging_pb2, name_peer):
    peer_ids = tuple(PeerID(raw) for raw in message.ordered_peer_ids)
    gathered = tuple(message.gathered)
    filtered_ids, filtered_data, removed = filter_roster(peer_ids, gathered, excluded_name, name_peer)
    if not removed:
        return message, False, [name_peer(peer) for peer in peer_ids]
    rewritten = averaging_pb2.MessageFromLeader(
        code=message.code, group_id=message.group_id, suggested_leader=message.suggested_leader,
        ordered_peer_ids=[peer.to_bytes() for peer in filtered_ids], gathered=filtered_data)
    return rewritten, True, [name_peer(peer) for peer in filtered_ids]


def make_averager_class():
    """One shared RPC service class; each channel is distinguished by prefix."""
    from hivemind.averaging import DecentralizedAverager
    from hivemind.averaging.group_info import GroupInfo
    from hivemind.proto import averaging_pb2
    from hivemind.p2p import P2PContext, PeerID
    from typing import AsyncIterator
    from step4_reformation import predict_next_bits

    class TrackingSmokeAverager(DecentralizedAverager):
        def __init__(self, *args, node_label: str, channel: str, arm: str, victim_peer_id: str,
                     shared_round, identity_map: Mapping[str, str], **kwargs):
            self._node_label = node_label
            self._channel = channel
            self._arm = arm
            self._victim_peer_id = victim_peer_id
            self._shared_round = shared_round
            self._identity_map = dict(identity_map)
            super().__init__(*args, **kwargs)

        def _round_no(self) -> int:
            return int(self._shared_round.value)

        def _peer_label(self, peer: Any) -> str:
            return self._identity_map.get(str(peer), str(peer))

        async def rpc_join_group(
            self, request: averaging_pb2.JoinRequest, context: P2PContext
        ) -> AsyncIterator[averaging_pb2.MessageFromLeader]:
            requester = str(context.remote_id)
            if self._node_label == ATTACKER and self._peer_label(context.remote_id) != VICTIM:
                _emit("admission", arm=self._arm, channel=self._channel, round=self._round_no(),
                      leader=ATTACKER, requester=self._peer_label(context.remote_id), admitted=False,
                      reason="selective_admission_fixture")
                yield averaging_pb2.MessageFromLeader(code=averaging_pb2.GROUP_IS_FULL)
                return
            if self._node_label == ATTACKER:
                _emit("admission", arm=self._arm, channel=self._channel, round=self._round_no(),
                      leader=ATTACKER, requester=self._peer_label(context.remote_id), admitted=True,
                      reason="target_allowed")
            async for response in super().rpc_join_group(request, context):
                if response.code == averaging_pb2.BEGIN_ALLREDUCE:
                    native_members = [self._peer_label(PeerID(raw)) for raw in response.ordered_peer_ids]
                    effective_members = native_members
                    excluded = False
                    if (self._node_label == ATTACKER and EXCLUDE_TARGET and
                            self._peer_label(context.remote_id) != VICTIM):
                        response, excluded, effective_members = _rewrite_message(
                            response, VICTIM, PeerID, averaging_pb2, self._peer_label)
                    _emit("announcement_sent", arm=self._arm, channel=self._channel,
                          round=self._round_no(), leader=self._node_label,
                          recipient=self._peer_label(context.remote_id), native_members=native_members,
                          effective_members=effective_members, excluded_target=excluded,
                          target_recipient=self._peer_label(context.remote_id) == VICTIM)
                yield response

        async def _aggregate_with_group(self, group_info, min_vector_size, **kwargs):
            native_members = [self._peer_label(peer) for peer in group_info.peer_ids]
            native_gathered = [item.hex() for item in group_info.gathered]
            leader = LEADERS.get((self.prefix, group_info.group_id.hex()))
            round_no = self._round_no()
            bits_after_native = self._matchmaking.group_key_manager.group_bits
            predicted = None
            if self._node_label == ATTACKER and VICTIM in native_members:
                manager = self._matchmaking.group_key_manager
                predicted = predict_next_bits(group_info.group_id, group_info.peer_ids,
                                              PeerID.from_base58(self._victim_peer_id), manager.target_group_size,
                                              manager.group_bits)
                # This is the step-4 tracking policy: follow the target's next
                # native shard after observing the complete group broadcast.
                manager.group_bits = predicted

            effective_group = group_info
            excluded = False
            if (self._node_label == ATTACKER and leader == ATTACKER and EXCLUDE_TARGET and
                    VICTIM in native_members):
                ids, metadata, excluded = filter_roster(
                    group_info.peer_ids, group_info.gathered, VICTIM, self._peer_label)
                if excluded:
                    effective_group = GroupInfo(group_info.group_id, ids, metadata)

            before = _tensor_values(self)
            effective_members = [self._peer_label(peer) for peer in effective_group.peer_ids]
            _emit("aggregate_view", arm=self._arm, channel=self._channel, round=round_no,
                  peer=self._node_label, role="leader" if leader == self._node_label else "follower",
                  leader=leader, group_id=group_info.group_id.hex(),
                  native_members=native_members, native_gathered_hex=native_gathered,
                  effective_members=effective_members, effective_gathered_hex=[x.hex() for x in effective_group.gathered],
                  target_omitted_locally=excluded, tensor_before=before,
                  target_coefficient_before=before[NODES.index(VICTIM)],
                  bucket_after_native_key_update=bits_after_native,
                  predicted_victim_next_bits=predicted,
                  bucket_after_tracker_override=self._matchmaking.group_key_manager.group_bits)
            try:
                result = await super()._aggregate_with_group(effective_group, min_vector_size, **kwargs)
            except BaseException as exc:
                _emit("aggregate_error", arm=self._arm, channel=self._channel, round=round_no,
                      peer=self._node_label, leader=leader, group_id=group_info.group_id.hex(),
                      error=repr(exc), traceback=traceback.format_exc())
                raise
            after = _tensor_values(self)
            _emit("aggregate_output", arm=self._arm, channel=self._channel, round=round_no,
                  peer=self._node_label, leader=leader, group_id=group_info.group_id.hex(),
                  status="returned", tensor_after=after,
                  target_coefficient_after=after[NODES.index(VICTIM)])
            return result

    # Hivemind's servicer registration resolves handler annotations against the
    # defining module. These protocol types are imported lazily above, so pin
    # concrete hints after defining the one shared service class.
    TrackingSmokeAverager.rpc_join_group.__annotations__ = {
        "request": averaging_pb2.JoinRequest,
        "context": P2PContext,
        "return": AsyncIterator[averaging_pb2.MessageFromLeader],
    }
    return TrackingSmokeAverager


def _owned_snapshot(psutil) -> dict[int, float]:
    result = {}
    try:
        children = psutil.Process().children(recursive=True)
    except psutil.Error:
        children = []
    for proc in children:
        try:
            result[proc.pid] = proc.create_time()
        except psutil.Error:
            pass
    return result


def _live_owned(psutil, owned: Mapping[int, float]):
    live = []
    for pid, created in owned.items():
        try:
            proc = psutil.Process(int(pid))
            if proc.create_time() == created and proc.status() != psutil.STATUS_ZOMBIE:
                live.append(proc)
        except psutil.Error:
            pass
    return live


def _run_arm(args) -> None:
    global EVENTS, START, IDENTITIES, PREFIX_INFO, ROUND_VALUES, LEADERS, EXCLUDE_TARGET
    from hivemind import DHT
    from hivemind.utils import get_dht_time
    import psutil
    import torch

    out = args.case_dir
    out.mkdir(parents=True, exist_ok=True)
    arm = args.arm
    EXCLUDE_TARGET = arm == "capture_exclusion"
    ctx = mp.get_context("fork")
    EVENTS = ctx.Queue()
    START = time.monotonic()
    IDENTITIES, PREFIX_INFO, ROUND_VALUES, LEADERS = {}, {}, {}, {}
    torch.set_num_threads(1)
    install_matchmaking_observers()
    install_bucket_observer()
    averager_cls = make_averager_class()
    dhts, averagers, controls, tensors = [], {}, {}, {}
    owned: dict[int, float] = {}
    all_events: list[dict[str, Any]] = []
    error = None
    arm_start = clock_sample()
    arm_deadline = time.monotonic() + args.max_seconds
    events_path = out / "events.jsonl"
    events_stream = events_path.open("a", encoding="utf-8", buffering=1)

    def sample_owned():
        owned.update(_owned_snapshot(psutil))

    def drain():
        while True:
            try:
                event = EVENTS.get_nowait()
                all_events.append(event)
                events_stream.write(json.dumps(event, sort_keys=True, allow_nan=False) + "\n")
                events_stream.flush()
            except Exception:
                break

    try:
        for node in NODES:
            bootstrap = dhts[0].get_visible_maddrs() if dhts else None
            dht = DHT(initial_peers=bootstrap, host_maddrs=LOCALHOST, auto_nat=False,
                      nat_port_map=False, use_relay=False, start=True)
            addresses = [str(addr) for addr in dht.get_visible_maddrs()]
            if not addresses or any(not addr.startswith("/ip4/127.0.0.1/tcp/") for addr in addresses):
                dht.shutdown()
                raise RuntimeError(f"non-loopback DHT listener for {node}: {addresses}")
            dhts.append(dht)
            sample_owned()
            IDENTITIES[str(dht.peer_id)] = node
            _emit("dht_started", arm=arm, peer=node, peer_id=str(dht.peer_id), addresses=addresses)
            time.sleep(0.01)
            drain()

        # The only bootstrap assistance is A+X in bucket 0. Four initial peers
        # per shard avoids relying on a single pair to create the shard layout.
        initial_bits = {node: ("0" if i < 4 else "1") for i, node in enumerate(NODES)}
        initial_bits[VICTIM] = initial_bits[ATTACKER] = "0"
        _emit("bootstrap_layout", arm=arm, explicit_assistance=True, victim=VICTIM,
              attacker=ATTACKER, initial_bits=initial_bits,
              note="A and X share an initial shard so X can observe A's first group")

        run_token = f"{args.label}_{arm}_{os.getpid()}"
        for channel in CHANNELS:
            prefix = f"tracksmoke_{run_token}_{channel}"
            if "." in prefix:
                raise ValueError("native averaging prefixes cannot contain periods")
            PREFIX_INFO[prefix] = {"arm": arm, "channel": channel}
            for i, (node, dht) in enumerate(zip(NODES, dhts)):
                round_value = ctx.Value("i", -1)
                ROUND_VALUES[(prefix, str(dht.peer_id))] = round_value
                marker = torch.zeros(len(NODES), dtype=torch.float32)
                marker[i] = 1.0
                tensors[(node, channel)] = marker
                is_attacker = node == ATTACKER
                averager = averager_cls(
                    [marker], dht, prefix=prefix, start=True,
                    node_label=node, channel=channel, arm=arm,
                    victim_peer_id=str(dhts[NODES.index(VICTIM)].peer_id),
                    shared_round=round_value, identity_map=IDENTITIES,
                    target_group_size=2, min_group_size=2, initial_group_bits=initial_bits[node],
                    min_matchmaking_time=0.5 if is_attacker else 5.0,
                    request_timeout=0.25, allreduce_timeout=5.0,
                    sender_timeout=3.0, reducer_timeout=5.0,
                    part_size_bytes=1024, averaging_alpha=1.0,
                    allow_state_sharing=False, client_mode=False, shutdown_timeout=0.75,
                )
                averagers[(node, channel)] = averager
                sample_owned()
                time.sleep(0.01)
                drain()

        config = {"arm": arm, "label": args.label, "nodes": list(NODES), "victim": VICTIM,
                  "attacker": ATTACKER, "channels": list(CHANNELS), "channel_implementation":
                  "two independent native DecentralizedAverager prefixes on each DHT peer",
                  "not_optimizer_training_state_averaging": True,
                  "group_size": 2, "min_group_size": 2, "bucket_count": 2,
                  "initial_group_bits": initial_bits, "bootstrap_assistance":
                  "A and X are pre-placed in the same initial shard; recorded above",
                  "marker_tensor": "8-element one-hot origin vector; coordinate i identifies node i",
                  "marker_values_reset_between_rounds": False,
                  "weight": 1.0, "sharding": "initial_group_bits length 1; native re-bucketing",
                  "attacker_min_matchmaking_time_seconds": 0.5,
                  "honest_min_matchmaking_time_seconds": 5.0,
                  "request_timeout_seconds": 0.25, "allreduce_timeout_seconds": 5.0,
                  "step_timeout_seconds": args.step_timeout,
                  "round_timeout_seconds": args.round_timeout,
                  "rounds_requested": args.rounds,
                  "arm_measurement_cap_seconds": args.max_seconds,
                  "exclusion": "attacker local GroupInfo projection; outgoing BEGIN_ALLREDUCE is filtered only for non-target recipients",
                  "target_announcement": "keeps native roster; group_size=2 means there is no non-target follower",
                  "training": False, "public_network": False, "artificial_wan_delay": False,
                  "leader_fixture": "earlier attacker advertisement plus selective admission of A only",
                  "native_peer_ids_and_group_ids": "cryptographically random; recorded raw",
                  "host_addresses": LOCALHOST}
        write_json(out / "config.json", config)
        write_json(out / "identities.json", {node: str(dht.peer_id) for node, dht in zip(NODES, dhts)})

        for round_no in range(args.rounds):
            if time.monotonic() >= arm_deadline:
                _emit("arm_measurement_deadline", arm=arm, round=round_no)
                break
            before = clock_sample()
            scheduled = get_dht_time() + 1.5
            round_controls = {}
            for node in NODES:
                for channel in CHANNELS:
                    key = (node, channel)
                    averagers[key]._shared_round.value = round_no
                    _emit("step_started", arm=arm, channel=channel, round=round_no,
                          peer=node, scheduled_time=scheduled)
                    control = averagers[key].step(
                        gather={"node": node, "channel": channel, "round": round_no},
                        scheduled_time=scheduled, weight=1.0, timeout=args.step_timeout,
                        allow_retries=False, wait=False)
                    controls[key] = control
                    round_controls[key] = control

            round_end = time.monotonic() + args.round_timeout
            pending = set(round_controls)
            results = {}
            while pending and time.monotonic() < round_end and time.monotonic() < arm_deadline:
                drain()
                for key in list(pending):
                    control = round_controls[key]
                    if not control.done():
                        continue
                    try:
                        result = control.result()
                        status, error_text = ("success", None) if result is not None else ("no_group", None)
                    except BaseException as exc:
                        result, status, error_text = None, "error", repr(exc)
                    node, channel = key
                    try:
                        output = _tensor_values(averagers[key])
                    except BaseException as exc:
                        output = None
                        error_text = f"{error_text}; tensor read: {exc!r}" if error_text else repr(exc)
                    results[key] = {"status": status, "error": error_text, "output": output}
                    _emit("step_result", arm=arm, channel=channel, round=round_no,
                          peer=node, status=status, stage=_stage_label(control.stage), error=error_text,
                          output_tensor=output,
                          target_coefficient=(output[NODES.index(VICTIM)] if output is not None else None))
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
                    node, channel = key
                    try:
                        output = _tensor_values(averagers[key])
                    except BaseException:
                        output = None
                    _emit("step_result", arm=arm, channel=channel, round=round_no,
                          peer=node, status="round_deadline_cancelled", stage=_stage_label(control.stage),
                          error=None, output_tensor=output,
                          target_coefficient=(output[NODES.index(VICTIM)] if output is not None else None))
                drain()
                _emit("round_incomplete", arm=arm, round=round_no,
                      pending=[{"peer": key[0], "channel": key[1]} for key in sorted(pending)])
                break

            after = clock_sample()
            audit = clock_delta(before, after)
            _emit("round_clock_audit", arm=arm, round=round_no, **audit)
            if not audit["valid"]:
                error = f"suspension or wall-clock change detected during round {round_no}: {audit}"
                break
            _emit("round_complete", arm=arm, round=round_no,
                  step_results=len(results), elapsed_seconds=audit["monotonic_elapsed"])
            drain()

        if error:
            raise RuntimeError(error)
    except BaseException as exc:
        error = repr(exc)
        _emit("worker_error", arm=arm, error=error, traceback=traceback.format_exc())
    finally:
        sample_owned()
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
                _emit("cleanup_error", arm=arm, component="averager", error=repr(exc))
        for dht in reversed(dhts):
            try:
                dht.shutdown()
            except BaseException as exc:
                _emit("cleanup_error", arm=arm, component="dht", error=repr(exc))
        until = time.monotonic() + 8.0
        while _live_owned(psutil, owned) and time.monotonic() < until:
            drain()
            time.sleep(0.1)
        live = _live_owned(psutil, owned)
        helpers = []
        targets = []
        for proc in live:
            try:
                if "torch_shm_manag" in proc.name():
                    helpers.append(proc)
                else:
                    targets.append(proc)
            except psutil.Error:
                targets.append(proc)
        forced = [proc.pid for proc in targets]
        for proc in targets:
            try:
                proc.terminate()
            except psutil.Error:
                pass
        _, remaining = psutil.wait_procs(targets, timeout=2.0)
        for proc in remaining:
            try:
                proc.kill()
            except psutil.Error:
                pass
        psutil.wait_procs(remaining, timeout=2.0)
        drain()
        events_stream.flush()
        events_stream.close()
        shutdown = {"safe_at_worker_exit": not _live_owned(psutil, owned),
                    "worker_exit_pending_helpers": [proc.pid for proc in helpers],
                    "forced_cleanup_pids": forced,
                    "owned_processes": {str(k): v for k, v in owned.items()},
                    "error": error,
                    "elapsed_seconds": time.monotonic() - START}
        write_json(out / "shutdown_worker.json", shutdown)
        status = {"arm": arm, "status": "failed" if error else "complete",
                  "error": error, "elapsed_seconds": time.monotonic() - START,
                  "clock_audit": clock_delta(arm_start, clock_sample()),
                  "rounds_observed": sorted({e.get("round") for e in all_events
                                              if e.get("kind") == "round_complete"}),
                  "events": len(all_events), "shutdown": shutdown}
        write_json(out / "status.json", status)
        try:
            EVENTS.close()
            EVENTS.join_thread()
        except BaseException:
            pass
    if error:
        raise RuntimeError(error)


def _audit_child_processes(psutil, worker_pid: int, observed: dict[int, float], timeout: float = 6.0) -> dict[str, Any]:
    until = time.monotonic() + timeout
    while time.monotonic() < until:
        try:
            parent = psutil.Process(worker_pid)
            for child in parent.children(recursive=True):
                observed[child.pid] = child.create_time()
        except psutil.Error:
            pass
        live = []
        for pid, created in observed.items():
            try:
                proc_obj = psutil.Process(pid)
                if proc_obj.create_time() == created and proc_obj.status() != psutil.STATUS_ZOMBIE:
                    live.append(proc_obj)
            except psutil.Error:
                pass
        if not live:
            break
        time.sleep(0.1)
    live = []
    for pid, created in observed.items():
        try:
            proc_obj = psutil.Process(pid)
            if proc_obj.create_time() == created and proc_obj.status() != psutil.STATUS_ZOMBIE:
                live.append(proc_obj)
        except psutil.Error:
            pass
    forced = []
    for proc_obj in live:
        forced.append(proc_obj.pid)
        try:
            proc_obj.terminate()
        except psutil.Error:
            pass
    _, remaining = psutil.wait_procs(live, timeout=2.0)
    for proc_obj in remaining:
        try:
            proc_obj.kill()
        except psutil.Error:
            pass
    psutil.wait_procs(remaining, timeout=2.0)
    survivors = []
    for pid, created in observed.items():
        try:
            proc_obj = psutil.Process(pid)
            if proc_obj.create_time() == created and proc_obj.status() != psutil.STATUS_ZOMBIE:
                survivors.append(pid)
        except psutil.Error:
            pass
    return {"observed_owned_pids": {str(pid): created for pid, created in observed.items()},
            "forced_cleanup_pids": forced, "surviving_pids": survivors,
            "safe": not survivors, "controller_audited_after_worker_exit": True}


def _run_isolated_worker(args, arm: str, out: Path, psutil) -> dict[str, Any]:
    command = [sys.executable, str(SCRIPT), "--_worker", "--label", args.label,
               "--arm", arm, "--case-dir", str(out),
               "--package-root", str(args.package_root), "--dependencies", str(args.dependencies),
               "--rounds", str(args.rounds), "--round-timeout", str(args.round_timeout),
               "--step-timeout", str(args.step_timeout), "--max-seconds", str(args.max_seconds)]
    log = (out / "worker.log")
    out.mkdir(parents=True, exist_ok=False)
    started = clock_sample()
    observed: dict[int, float] = {}
    worker_cap = args.max_seconds + 18.0
    with log.open("wb") as stream:
        process = subprocess.Popen(command, stdout=stream, stderr=subprocess.STDOUT,
                                   start_new_session=True, cwd=str(ROOT))
        deadline = time.monotonic() + worker_cap
        suspended = False
        previous = clock_sample()
        while process.poll() is None and time.monotonic() < deadline:
            try:
                for child in psutil.Process(process.pid).children(recursive=True):
                    observed[child.pid] = child.create_time()
            except psutil.Error:
                pass
            current = clock_sample()
            delta = clock_delta(previous, current)
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
    shutdown_path = out / "shutdown_worker.json"
    if shutdown_path.exists():
        try:
            worker_owned = json.loads(shutdown_path.read_text()).get("owned_processes", {})
            observed.update({int(pid): float(created) for pid, created in worker_owned.items()})
        except (OSError, ValueError, TypeError):
            pass
    process_audit = _audit_child_processes(psutil, process.pid, observed)
    result = {"arm": arm, "worker_exitcode": exitcode, "worker_timed_out": timed_out,
              "suspension_detected_by_controller": suspended,
              "controller_clock_audit": clock_delta(started, clock_sample()),
              "worker_process_audit": process_audit}
    write_json(out / "controller_audit.json", result)
    if (out / "status.json").exists():
        status = json.loads((out / "status.json").read_text())
        status["controller_audit"] = result
        clocks_valid = (result["controller_clock_audit"].get("valid", False) and
                        status.get("clock_audit", {}).get("valid", False))
        status["status"] = "complete" if (exitcode == 0 and status.get("status") == "complete" and
                                             process_audit["safe"] and not suspended and clocks_valid) else "failed"
        write_json(out / "status.json", status)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--label", default=None, help="unique output label; never reuses an existing directory")
    parser.add_argument("--output-root", type=Path, default=ROOT / "research/hivemind/results")
    parser.add_argument("--package-root", type=Path, default=ROOT / "research/simulator/.native-runtime/root_snapshot_a")
    parser.add_argument("--dependencies", type=Path, default=ROOT / "research/agora/.runtime/venv/lib/python3.11/site-packages")
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--round-timeout", type=float, default=12.0)
    parser.add_argument("--step-timeout", type=float, default=10.0)
    parser.add_argument("--max-seconds", type=float, default=90.0)
    parser.add_argument("--_worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--arm", choices=ARMS, help=argparse.SUPPRESS)
    parser.add_argument("--case-dir", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.rounds < 1 or args.rounds > 12 or args.round_timeout <= 0 or args.step_timeout <= 0:
        parser.error("rounds must be 1..12 and timeouts must be positive")
    if not 10 <= args.max_seconds <= 90:
        parser.error("max-seconds must be between 10 and 90")
    if args.label is None:
        args.label = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    if not args.label.replace("_", "").replace("-", "").isalnum():
        parser.error("label may contain only letters, digits, underscores, and hyphens")

    if args._worker:
        if args.arm is None or args.case_dir is None:
            parser.error("internal worker requires --arm and --case-dir")
        args.provenance = configure_native(args)
        mp.set_start_method("fork", force=True)
        args.case_dir.mkdir(parents=True, exist_ok=True)
        write_json(args.case_dir / "provenance.json", args.provenance)
        with tarfile.open(args.case_dir / "frozen_sources.tar.gz", "w:gz") as archive:
            archive.add(SCRIPT, arcname="tracking_combination_smoke.py")
            for relative in ("research/hivemind/step4_reformation.py",
                             "research/hivemind/step3_capture.py",
                             "research/hivemind/compute_native_hooks.py"):
                archive.add(ROOT / relative, arcname=relative)
            for relative in NATIVE_FILES:
                imported = Path(args.provenance["package"]) / relative
                if imported.exists():
                    archive.add(imported, arcname=f"native_snapshot/{relative}")
        try:
            _run_arm(args)
            return 0
        except BaseException:
            return 1

    args.output_root.mkdir(parents=True, exist_ok=True)
    root = args.output_root / f"tracking_combination_smoke_{args.label}"
    root.mkdir(exist_ok=False)
    # The controller imports psutil from the pinned local dependencies for
    # descendant enumeration and create-time checked cleanup.
    sys.path.insert(0, str(args.dependencies.resolve()))
    import psutil

    config = {"label": args.label, "arms": list(ARMS), "channels": list(CHANNELS),
              "rounds": args.rounds, "round_timeout_seconds": args.round_timeout,
              "step_timeout_seconds": args.step_timeout, "arm_hard_measurement_cap_seconds": args.max_seconds,
              "expected_total_wall_seconds_upper_bound": 2 * (args.max_seconds + 18.0) + 20,
              "native_package_root": str(args.package_root.resolve()),
              "dependencies": str(args.dependencies.resolve()),
              "authorization_scope": "private loopback network only",
              "claim_boundary": "capture and local marker omission mechanism smoke; not learning, deployment rate, or persistent isolation"}
    write_json(root / "config.json", config)
    arm_results = []
    frozen_script_hash = sha256(SCRIPT)
    write_json(root / "source_manifest.json", {
        "script_sha256": frozen_script_hash,
        "tracking_reference_sha256": sha256(ROOT / "research/hivemind/step4_reformation.py"),
        "admission_reference_sha256": sha256(ROOT / "research/hivemind/step3_capture.py"),
        "roster_reference_sha256": sha256(ROOT / "research/hivemind/compute_native_hooks.py"),
    })
    for arm in ARMS:
        if sha256(SCRIPT) != frozen_script_hash:
            raise RuntimeError("probe source changed between arms; refusing mixed-source comparison")
        arm_out = root / "arms" / arm
        arm_out.parent.mkdir(parents=True, exist_ok=True)
        result = _run_isolated_worker(args, arm, arm_out, psutil)
        arm_results.append(result)

    all_events = []
    for arm in ARMS:
        event_path = root / "arms" / arm / "events.jsonl"
        if event_path.exists():
            for line in event_path.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    all_events.append(json.loads(line))
    summaries = [summarize_events(all_events, arm, channel, args.rounds)
                 for arm in ARMS for channel in CHANNELS]
    statuses = {arm: (json.loads((root / "arms" / arm / "status.json").read_text())
                      if (root / "arms" / arm / "status.json").exists() else {"status": "missing"})
                for arm in ARMS}
    report = {"label": args.label, "status": "complete" if all(v.get("status") == "complete" for v in statuses.values()) else "incomplete",
              "arm_status": statuses, "channel_summaries": summaries,
              "controller_audits": arm_results,
              "interpretation": {
                  "combination_tested": "native bucket tracking + earlier attacker leader deadline + selective admission + attacker-local target roster omission",
                  "channels": "gradient_probe and state_probe are independent native DecentralizedAverager prefixes with identical marker fixtures",
                  "not_tested": ["Optimizer TrainingStateAverager scheduling", "honest follower receiving an altered announcement", "model learning/convergence", "progress tracking or recovery", "public/WAN behavior"],
                  "success_requires_observed_events": ["target and attacker in native group", "attacker owns leadership", "attacker predicts target's next bucket", "attacker advertises in target's next bucket", "target marker coefficient in attacker's output recorded"],
                  "sample_size_warning": "one local smoke per arm/channel; no probability estimate"}}
    write_json(root / "report.json", report)
    (root / "report.md").write_text(_render_report(report), encoding="utf-8")
    write_json(root / "artifact_inventory.json", inventory(root))
    print(json.dumps({"output": str(root), "status": report["status"],
                      "report": str(root / "report.json")}, indent=2))
    return 0 if report["status"] == "complete" else 1


def _render_report(report: Mapping[str, Any]) -> str:
    lines = [f"# Tracking composition smoke: {report['label']}", "",
             f"Status: **{report['status']}**", "",
             "This local marker-tensor smoke combines bucket tracking, attacker leadership, selective admission, and attacker-local roster omission. It uses two independent native `DecentralizedAverager` channels (`gradient_probe`, `state_probe`) and does not run Optimizer training.", "",
             "| Arm | Channel | Captured rounds led by X | Longest streak | Exact target next-bucket predictions | X tracked target next bucket | Omission rounds | Attacker output target coefficient |",
             "|---|---|---:|---:|---:|---:|---|---|"]
    for row in report["channel_summaries"]:
        coeffs = ", ".join("null" if x is None else f"{x:.4g}" for x in row["attacker_local_omission_target_coefficients"])
        lines.append(f"| {row['arm']} | {row['channel']} | {row['capture_round_count']} | {row['longest_consecutive_capture_streak']} | {row['successful_victim_next_bucket_predictions']} | {row['attacker_next_bucket_matches']} | {row['attacker_local_omission_rounds']} | {coeffs or 'n/a'} |")
    lines += ["", "The bootstrap places A and X in the same starting shard so X can observe A's first group. This is explicit setup assistance, not a measured discovery rate. Group size two leaves no honest follower in the captured group; A receives the full native roster. The omission result therefore describes X's local averaging view and does not establish that an honest follower receives a rewritten roster.", "",
              "`state_probe` is a separately matched native averager instance. It does not reproduce Optimizer state averaging cadence, progress tracking, or state recovery. Every step attempt, including no-group outcomes, is retained in `events.jsonl`; per-group native/effective rosters, leader, bucket, and marker values are also retained.", ""]
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
