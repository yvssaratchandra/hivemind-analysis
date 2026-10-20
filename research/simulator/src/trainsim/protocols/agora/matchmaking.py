"""Timed registry and node-local Agora stage/step matchmaking.

The registry is an abstract expiring key/value service. Requests and responses
traverse the simulator network, but replication, DHT routing, and consensus are
not modeled. Each matcher retains only its own accumulated lookup results.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
from typing import Any, Callable

from ...network import REGISTRY_ID, Network
from ...types import Message, NodeProfile, TimerHandle
from .partitioning import stage_namespace, step_key


def _stable_json_size(value: Any) -> int:
    return len(json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8"))


@dataclass(frozen=True)
class RegistryRecord:
    key: str
    peer_id: str
    value: dict[str, Any]
    expiry: float
    active: bool = True


class TimedRegistry:
    """A single abstract registry with network-timed reads and writes."""

    def __init__(self, engine, network: Network) -> None:
        self.engine, self.network = engine, network
        self._records: dict[tuple[str, str], RegistryRecord] = {}
        self._next_id = 0
        self._callbacks: dict[str, Callable] = {}
        self._expiry_handles: dict[tuple[str, str], TimerHandle] = {}

    def publish(
        self,
        node_id: str,
        key: str,
        peer_id: str,
        value: dict[str, Any],
        expiry: float,
        callback: Callable[[bool], None] | None = None,
        *,
        active: bool = True,
    ) -> None:
        """Publish a record; expiry is supplied by caller before RPC begins."""
        request_id = self._id(node_id, "publish")
        if callback is not None:
            self._callbacks[request_id] = callback
        body = {
            "key": key,
            "peer_id": peer_id,
            "value": value,
            "expiry": float(expiry),
            "active": bool(active),
        }
        message = Message(
            message_id=request_id,
            sender=node_id,
            recipient=REGISTRY_ID,
            kind="agora_registry_publish",
            body=body,
            wire_bytes=192 + _stable_json_size(value),
        )
        self.network.send(message, self._receive_publish)

    def lookup(
        self,
        node_id: str,
        key: str,
        callback: Callable[[list[RegistryRecord]], None],
        *,
        prefix: bool = False,
    ) -> None:
        request_id = self._id(node_id, "lookup")
        self._callbacks[request_id] = callback
        message = Message(
            message_id=request_id,
            sender=node_id,
            recipient=REGISTRY_ID,
            kind="agora_registry_lookup",
            body={"key": key, "prefix": prefix},
            wire_bytes=96 + len(key.encode("utf-8")),
        )
        self.network.send(message, self._receive_lookup)

    def _id(self, node_id: str, operation: str) -> str:
        self._next_id += 1
        return f"agora-registry:{node_id}:{operation}:{self._next_id}"

    def _receive_publish(self, message: Message) -> None:
        body = message.body
        expiry = float(body["expiry"])
        accepted = math.isfinite(expiry) and expiry > self.engine.now
        key, peer_id = str(body["key"]), str(body["peer_id"])
        if accepted:
            record = RegistryRecord(
                key=key,
                peer_id=peer_id,
                value=dict(body["value"]),
                expiry=expiry,
                active=bool(body.get("active", True)),
            )
            existing = self._records.get((key, peer_id))
            if existing is None or record.expiry > existing.expiry:
                self._records[(key, peer_id)] = record
                old_timer = self._expiry_handles.pop((key, peer_id), None)
                if old_timer is not None:
                    old_timer.cancel()
                self._expiry_handles[(key, peer_id)] = self.engine.call_at(
                    expiry, self._expire, key, peer_id, expiry
                )
            elif record.expiry == existing.expiry and record == existing:
                pass
            else:
                accepted = False

        self.engine.emit(
            "agora_registry_publish",
            message.sender,
            key=key,
            peer_id=peer_id,
            active=bool(body.get("active", True)),
            expiry=expiry,
            accepted=accepted,
        )
        request_id = message.message_id
        if request_id in self._callbacks:
            ack = Message(
                message_id=f"{request_id}:ack",
                sender=REGISTRY_ID,
                recipient=message.sender,
                kind="agora_registry_publish_ack",
                body={"request_id": request_id, "accepted": accepted},
                wire_bytes=64,
            )
            self.network.send(ack, self._receive_ack)

    def _receive_ack(self, message: Message) -> None:
        request_id = str(message.body["request_id"])
        callback = self._callbacks.pop(request_id, None)
        if callback is not None:
            callback(bool(message.body["accepted"]))

    def _receive_lookup(self, message: Message) -> None:
        key = str(message.body["key"])
        is_prefix = bool(message.body.get("prefix", False))
        records = [
            item for item in self._records.values()
            if item.active
            and item.expiry > self.engine.now
            and (item.key.startswith(key) if is_prefix else item.key == key)
        ]
        records.sort(key=lambda item: bytes.fromhex(item.peer_id))
        request_id = message.message_id
        self.engine.emit(
            "agora_registry_lookup_snapshot",
            message.sender,
            request_id=request_id,
            key=key,
            prefix=is_prefix,
            peer_ids=[item.peer_id for item in records],
            count=len(records),
        )
        payload = [
            {"key": item.key, "peer_id": item.peer_id, "value": item.value, "expiry": item.expiry}
            for item in records
        ]
        response = Message(
            message_id=f"{request_id}:response",
            sender=REGISTRY_ID,
            recipient=message.sender,
            kind="agora_registry_lookup_response",
            body={"request_id": request_id, "key": key, "records": payload},
            wire_bytes=96 + sum(48 + _stable_json_size(item["value"]) for item in payload),
        )
        self.network.send(response, self._receive_lookup_response)

    def _receive_lookup_response(self, message: Message) -> None:
        request_id = str(message.body["request_id"])
        records = [
            RegistryRecord(
                key=str(row["key"]),
                peer_id=str(row["peer_id"]),
                value=dict(row["value"]),
                expiry=float(row["expiry"]),
            )
            for row in message.body["records"]
            if float(row["expiry"]) > self.engine.now
        ]
        self.engine.emit(
            "agora_registry_lookup_response",
            message.recipient,
            request_id=request_id,
            key=str(message.body["key"]),
            peer_ids=[item.peer_id for item in records],
            count=len(records),
        )
        callback = self._callbacks.pop(request_id, None)
        if callback is not None:
            callback(records)

    def _expire(self, key: str, peer_id: str, expiry: float) -> None:
        current = self._records.get((key, peer_id))
        if current is None or current.expiry != expiry or current.expiry > self.engine.now:
            return
        self._records.pop((key, peer_id), None)
        self._expiry_handles.pop((key, peer_id), None)
        self.engine.emit("agora_registry_expiry", peer_id, key=key, expiry=expiry)


@dataclass
class _MatcherState:
    node_id: str
    epoch: int
    global_step: int
    advertised_at: float
    started_at: float
    deadline: float
    on_roster: Callable[[str, int, dict[str, Any] | None], None]
    expected: int = 0
    expected_ready: bool = False
    lookup_inflight: bool = False
    finished: bool = False
    accumulated: dict[str, dict[str, Any]] | None = None
    poll_timer: TimerHandle | None = None


class AgoraMatcher:
    """One independent native-style poller per node and averaging epoch."""

    def __init__(
        self,
        engine,
        registry: TimedRegistry,
        profiles: dict[str, NodeProfile],
        config: dict[str, Any],
        stage: str,
        step_prefix: str,
        on_roster: Callable[[str, int, dict[str, Any] | None], None],
    ) -> None:
        self.engine, self.registry = engine, registry
        self.profiles, self.config = profiles, config
        self.stage, self.step_prefix = stage, step_prefix
        self.on_roster = on_roster
        self._states: dict[tuple[str, int], _MatcherState] = {}
        self._node_by_peer = {profile.peer_id.hex(): node for node, profile in profiles.items()}

    def begin(
        self,
        node_id: str,
        epoch: int,
        global_step: int,
        gather: dict[str, Any],
        on_roster: Callable[[str, int, dict[str, Any] | None], None] | None = None,
    ) -> None:
        """Publish the step advert; begin the 40-second collection after ACK."""
        key = step_key(self.step_prefix, global_step)
        # Native code computes this before awaiting the DHT publish future.
        expiry = self.engine.now + float(self.config["matchmaking_seconds"])
        peer_id = self.profiles[node_id].peer_id.hex()
        state = _MatcherState(
            node_id=node_id,
            epoch=epoch,
            global_step=global_step,
            advertised_at=self.engine.now,
            started_at=self.engine.now,
            deadline=math.inf,
            on_roster=on_roster or self.on_roster,
            accumulated={},
        )
        self._states[(node_id, epoch)] = state
        self.engine.emit(
            "agora_step_advertised",
            node_id,
            stage=self.stage,
            epoch=epoch,
            global_step=global_step,
            key=key,
            expiry=expiry,
            role=gather["role"],
            advertised_bandwidth=gather.get("advertised_bandwidth"),
            weight=float(gather.get("weight", 1.0)),
            layout=gather["layout"],
            partition=gather["partition"],
            selected_cardinality=int(gather["selected_cardinality"]),
            tensor_cardinalities=list(gather["tensor_cardinalities"]),
            snapshot_id=gather["snapshot_id"],
        )
        self.engine.emit(
            "agora_stage_step_publish_started",
            node_id,
            stage=self.stage,
            epoch=epoch,
            key=key,
            expiry=expiry,
        )
        self.registry.publish(
            node_id,
            key,
            peer_id,
            dict(gather),
            expiry,
            lambda accepted, state=state, key=key: self._on_step_publish_ack(
                state, key, accepted
            ),
        )

    def _on_step_publish_ack(self, state: _MatcherState, key: str, accepted: bool) -> None:
        if state.finished:
            return
        state.started_at = self.engine.now
        state.deadline = state.started_at + float(self.config["matchmaking_seconds"])
        self.engine.emit(
            "agora_step_publish_ack",
            state.node_id,
            stage=self.stage,
            epoch=state.epoch,
            key=key,
            accepted=accepted,
            publish_ack_time=state.started_at,
            collection_deadline=state.deadline,
        )
        self._get_expected(state, initial=True)

    def _get_expected(self, state: _MatcherState, *, initial: bool = False) -> None:
        if state.finished:
            return
        namespace = stage_namespace(self.stage)

        def on_records(records: list[RegistryRecord]) -> None:
            if state.finished:
                return
            state.expected = len(records)
            state.expected_ready = True
            self.engine.emit(
                "agora_expected_count",
                state.node_id,
                stage=self.stage,
                epoch=state.epoch,
                expected_count=state.expected,
                initial=initial,
                lookup_time=self.engine.now,
            )
            if not state.lookup_inflight:
                if initial and self.engine.now - state.started_at < float(self.config["matchmaking_seconds"]):
                    self._lookup(state)
                elif initial:
                    self._finish(state, reason="deadline")
                else:
                    self._schedule_poll(state)

        self.registry.lookup(state.node_id, namespace, on_records, prefix=True)

    def _lookup(self, state: _MatcherState) -> None:
        if state.finished or state.lookup_inflight:
            return
        state.lookup_inflight = True
        key = step_key(self.step_prefix, state.global_step)

        def on_records(records: list[RegistryRecord]) -> None:
            state.lookup_inflight = False
            if state.finished:
                return
            assert state.accumulated is not None
            for record in records:
                state.accumulated[record.peer_id] = dict(record.value)
            ordered_ids = sorted(state.accumulated, key=bytes.fromhex)
            self.engine.emit(
                "agora_lookup",
                state.node_id,
                stage=self.stage,
                epoch=state.epoch,
                key=key,
                lookup_time=self.engine.now,
                observed_count=len(ordered_ids),
                peer_ids=ordered_ids,
                record_expiries={record.peer_id: record.expiry for record in records},
            )

            if len(state.accumulated) > state.expected:
                self._get_expected(state)
            elif state.expected > 0 and len(state.accumulated) == state.expected:
                self._finish(state, reason="expected_count")
            else:
                self._schedule_poll(state)

        self.registry.lookup(state.node_id, key, on_records)

    def _schedule_poll(self, state: _MatcherState) -> None:
        if state.finished or state.poll_timer is not None:
            return
        state.poll_timer = self.engine.call_at(
            self.engine.now + float(self.config["poll_seconds"]),
            self._poll,
            state,
        )

    def _poll(self, state: _MatcherState) -> None:
        state.poll_timer = None
        if not state.finished:
            # Native code checks its collection wall clock at the loop head;
            # a lookup in flight is still processed before the next check.
            if self.engine.now - state.started_at >= float(self.config["matchmaking_seconds"]):
                self._finish(state, reason="deadline")
            else:
                self._lookup(state)

    def _finish(self, state: _MatcherState, *, reason: str) -> None:
        if state.finished:
            return
        state.finished = True
        if state.poll_timer is not None:
            state.poll_timer.cancel()
        accumulated = state.accumulated or {}
        peer_ids = sorted(accumulated, key=bytes.fromhex)
        node_ids = [self._node_by_peer[peer] for peer in peer_ids if peer in self._node_by_peer]
        if not peer_ids:
            self.engine.emit(
                "agora_local_roster",
                state.node_id,
                stage=self.stage,
                epoch=state.epoch,
                expected_count=state.expected,
                observed_count=0,
                reason="empty" if reason == "deadline" else reason,
                publish_ack_time=state.started_at,
                started_at=state.started_at,
                formed_at=self.engine.now,
                wait_seconds=self.engine.now - state.started_at,
                collection_wait_seconds=self.engine.now - state.started_at,
                ad_to_roster_wait=self.engine.now - state.advertised_at,
                advertised_at=state.advertised_at,
                ordered_peer_ids=[],
                ordered_node_ids=[],
                roster_fingerprint=hashlib.sha256(b"").hexdigest(),
            )
            self.engine.emit(
                "agora_matchmaking_failed",
                state.node_id,
                stage=self.stage,
                epoch=state.epoch,
                reason="empty_roster",
                expected_count=state.expected,
            )
            state.on_roster(state.node_id, state.epoch, None)
            return

        fingerprint = hashlib.sha256("\n".join(peer_ids).encode("ascii")).hexdigest()
        roster = {
            "stage": self.stage,
            "epoch": state.epoch,
            "global_step": state.global_step,
            "expected_count": state.expected,
            "observed_count": len(peer_ids),
            "reason": reason,
            "publish_ack_time": state.started_at,
            "advertised_at": state.advertised_at,
            "started_at": state.started_at,
            "formed_at": self.engine.now,
            "wait_seconds": self.engine.now - state.started_at,
            "collection_wait_seconds": self.engine.now - state.started_at,
            "ad_to_roster_wait": self.engine.now - state.advertised_at,
            "ordered_peer_ids": peer_ids,
            "ordered_node_ids": node_ids,
            "roster_fingerprint": fingerprint,
            "gathered": [accumulated[peer] for peer in peer_ids],
        }
        self.engine.emit(
            "agora_local_roster",
            state.node_id,
            **{k: v for k, v in roster.items() if k != "gathered"},
        )
        state.on_roster(state.node_id, state.epoch, roster)
