"""Asynchronous expiring-record discovery service over the simulated network."""

from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import asdict

from .engine import Engine
from .network import REGISTRY_ID, Network
from .types import Advertisement, Message


class Discovery:
    """Provide a registry with request-arrival snapshots and expiring records.

    Registry processing has unlimited capacity. Client requests and responses
    consume modeled endpoint bandwidth and propagation time through ``Network``.
    Lookup results include inactive tombstones; protocol code decides whether to
    filter them. Records are filtered once when the request arrives and again
    when the response reaches the requester.
    """

    def __init__(self, engine: Engine, network: Network) -> None:
        self.engine = engine
        self.network = network
        self._records: dict[tuple[str, str], Advertisement] = {}
        self._sequence = 0
        self._publish_callbacks: dict[str, Callable[[bool], None]] = {}
        self._lookup_callbacks: dict[str, Callable[[list[Advertisement]], None]] = {}

    def publish(
        self,
        node_id: str,
        advertisement: Advertisement,
        callback: Callable[[bool], None] | None = None,
    ) -> None:
        """Publish or withdraw a record; optional callback receives write status."""
        request_id = self._request_id(node_id, "publish")
        if callback is not None:
            self._publish_callbacks[request_id] = callback
        message = Message(
            message_id=request_id,
            sender=node_id,
            recipient=REGISTRY_ID,
            kind="discovery_publish",
            body={"advertisement": asdict(advertisement)},
            wire_bytes=256,
        )
        self.network.send(message, self._receive_publish)

    def lookup(
        self,
        node_id: str,
        key: str,
        callback: Callable[[list[Advertisement]], None],
    ) -> None:
        """Request a key snapshot; callback fires after the response arrives."""
        if not callable(callback):
            raise TypeError("callback must be callable")
        request_id = self._request_id(node_id, "lookup")
        self._lookup_callbacks[request_id] = callback
        message = Message(
            message_id=request_id,
            sender=node_id,
            recipient=REGISTRY_ID,
            kind="discovery_lookup",
            body={"key": key},
            wire_bytes=128,
        )
        self.network.send(message, self._receive_lookup)

    def _request_id(self, node_id: str, operation: str) -> str:
        self._sequence += 1
        return f"discovery:{node_id}:{operation}:{self._sequence}"

    def _receive_publish(self, message: Message) -> None:
        body = message.body.get("advertisement")
        if not isinstance(body, dict):
            raise ValueError("malformed discovery publish request")
        advertisement = Advertisement(
            key=str(body["key"]),
            peer_id=str(body["peer_id"]),
            active=bool(body["active"]),
            expiry=float(body["expiry"]),
        )
        accepted, reason, stored = self._commit(advertisement)
        self.engine.emit(
            "discovery_publish",
            message.sender,
            request_id=message.message_id,
            key=advertisement.key,
            peer_id=advertisement.peer_id,
            active=advertisement.active,
            accepted=accepted,
            reason=reason,
            expiry=stored.expiry if stored is not None else advertisement.expiry,
        )
        if message.message_id in self._publish_callbacks:
            response = Message(
                message_id=f"{message.message_id}:ack",
                sender=REGISTRY_ID,
                recipient=message.sender,
                kind="discovery_publish_ack",
                body={"request_id": message.message_id, "accepted": accepted},
                wire_bytes=128,
            )
            self.network.send(response, self._receive_publish_ack)

    def _commit(self, advertisement: Advertisement) -> tuple[bool, str, Advertisement | None]:
        now = self.engine.now
        expiry = advertisement.expiry
        if not math.isfinite(expiry):
            return False, "invalid_expiry", None
        if expiry <= now:
            return False, "expired_on_arrival", None

        stored = advertisement
        if not advertisement.active:
            # A tombstone must sort after the active record it withdraws when
            # both originated from the same lease expiry.
            expiry = math.nextafter(expiry, math.inf)
            if not math.isfinite(expiry):
                return False, "invalid_withdrawal_expiry", None
            stored = Advertisement(
                key=advertisement.key,
                peer_id=advertisement.peer_id,
                active=False,
                expiry=expiry,
            )

        record_key = (stored.key, stored.peer_id)
        current = self._records.get(record_key)
        if current is None or stored.expiry > current.expiry:
            self._records[record_key] = stored
            return True, "stored", stored
        if stored.expiry < current.expiry:
            return False, "older_expiry", current
        if stored == current:
            return True, "idempotent", current
        # Conflicting equal-expiry values keep the first arrival, making the
        # abstract registry deterministic without inventing a merge rule.
        return False, "equal_expiry_conflict", current

    def _receive_publish_ack(self, message: Message) -> None:
        request_id = str(message.body["request_id"])
        callback = self._publish_callbacks.pop(request_id, None)
        if callback is not None:
            callback(bool(message.body["accepted"]))

    def _receive_lookup(self, message: Message) -> None:
        key = str(message.body["key"])
        snapshot = sorted(
            (
                advertisement
                for (record_key, _), advertisement in self._records.items()
                if record_key == key and advertisement.expiry > self.engine.now
            ),
            key=lambda advertisement: advertisement.peer_id,
        )
        self.engine.emit(
            "discovery_lookup_snapshot",
            message.sender,
            request_id=message.message_id,
            key=key,
            peer_ids=[item.peer_id for item in snapshot],
            count=len(snapshot),
        )
        response = Message(
            message_id=f"{message.message_id}:response",
            sender=REGISTRY_ID,
            recipient=message.sender,
            kind="discovery_lookup_response",
            body={
                "request_id": message.message_id,
                "key": key,
                "records": [asdict(advertisement) for advertisement in snapshot],
            },
            wire_bytes=128 + 64 * len(snapshot),
        )
        self.network.send(response, self._receive_lookup_response)

    def _receive_lookup_response(self, message: Message) -> None:
        request_id = str(message.body["request_id"])
        key = str(message.body["key"])
        records = [
            Advertisement(
                key=str(record["key"]),
                peer_id=str(record["peer_id"]),
                active=bool(record["active"]),
                expiry=float(record["expiry"]),
            )
            for record in message.body["records"]
            if float(record["expiry"]) > self.engine.now
        ]
        self.engine.emit(
            "discovery_lookup_response",
            message.recipient,
            request_id=request_id,
            key=key,
            peer_ids=[item.peer_id for item in records],
            count=len(records),
            records=[asdict(item) for item in records],
        )
        callback = self._lookup_callbacks.pop(request_id, None)
        if callback is not None:
            callback(records)
