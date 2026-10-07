#!/usr/bin/env python3
"""Controlled, model-free Exo control-plane publisher for local audits.

This process creates a Router only. It never starts an Exo Node, Master,
Worker, API, or Election and publishes only the explicitly selected
LOCAL_EVENTS probe (at most twice for the replay arm).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARMS = ("baseline", "self", "forged", "stale-session", "replay")
GIB = 1024**3
RAM_TOTAL_BYTES = 2 * GIB
MAX_CONTROL_BYTES = 16 * 1024
MAX_PUBLICATIONS = 2
MAX_TIMEOUT_SECONDS = 600
MAX_SEED = 2**31 - 1
MIN_SEED = -(2**31)
MAX_ELECTION_CLOCK = 2**31 - 1

CONTROL_KEYS = {
    "session",
    "node_ids",
    "target_node_id",
    "available_bytes",
    "origin",
    "origin_idx",
    "arm",
    "seed",
}


@dataclass(frozen=True)
class Control:
    master_node_id: str
    election_clock: int
    honest_a_node_id: str
    honest_b_node_id: str
    target_node_id: str
    available_bytes: int
    origin: str
    origin_idx: int
    arm: str
    seed: int


def valid_zid(value: str) -> bool:
    """Match the bounded hexadecimal Zenoh identity accepted by Exo's Rust shim."""
    if not value or len(value) > 32:
        return False
    if value[0] not in "123456789abcdef":
        return False
    return all(char in "0123456789abcdef" for char in value)


def make_publisher_node_id(random_bytes: bytes | None = None) -> str:
    """Create the same kind of random hexadecimal identity as Exo's NodeId."""
    value = (random_bytes if random_bytes is not None else os.urandom(16)).hex().lstrip("0")
    if not value:
        value = "1"
    if not valid_zid(value):
        raise ValueError("generated identity is not a valid Zenoh ID")
    return value


def _parse_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON value {value!r} is not allowed")


def _require_object(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{field} must be a JSON object")
    return value


def _require_string(value: Any, field: str, *, max_length: int = 128) -> str:
    if not isinstance(value, str) or not value or len(value) > max_length:
        raise ValueError(f"{field} must be a non-empty string of at most {max_length} characters")
    if value.strip() != value:
        raise ValueError(f"{field} must not have leading or trailing whitespace")
    return value


def _require_int(value: Any, field: str, *, minimum: int, maximum: int) -> int:
    # bool is an int subclass, so require the exact type.
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(f"{field} must be an integer between {minimum} and {maximum}")
    return value


def strict_json_loads(raw: str) -> Any:
    return json.loads(raw, parse_constant=_parse_constant)


def validate_control(
    value: Any,
    *,
    configured_arm: str,
    configured_seed: int,
    publisher_node_id: str,
) -> Control:
    """Validate the bounded control file before constructing any Exo payload."""
    control_obj = _require_object(value, "control")
    if set(control_obj) != CONTROL_KEYS:
        missing = sorted(CONTROL_KEYS - set(control_obj))
        extra = sorted(set(control_obj) - CONTROL_KEYS)
        raise ValueError(f"control fields differ from schema (missing={missing}, extra={extra})")

    if configured_arm not in ARMS:
        raise ValueError(f"unsupported configured arm {configured_arm!r}")
    if configured_seed < MIN_SEED or configured_seed > MAX_SEED:
        raise ValueError(f"configured seed must be between {MIN_SEED} and {MAX_SEED}")
    if not valid_zid(publisher_node_id):
        raise ValueError("publisher node ID is not a valid Zenoh ID")

    arm = _require_string(control_obj["arm"], "arm", max_length=32)
    if arm != configured_arm:
        raise ValueError(f"control arm {arm!r} does not match configured arm {configured_arm!r}")
    seed = _require_int(control_obj["seed"], "seed", minimum=MIN_SEED, maximum=MAX_SEED)
    if seed != configured_seed:
        raise ValueError(f"control seed {seed} does not match configured seed {configured_seed}")

    session_obj = _require_object(control_obj["session"], "session")
    if set(session_obj) != {"master_node_id", "election_clock"}:
        raise ValueError("session must contain only master_node_id and election_clock")
    master_node_id = _require_string(session_obj["master_node_id"], "session.master_node_id", max_length=32)
    if not valid_zid(master_node_id):
        raise ValueError("session.master_node_id is not a valid Zenoh ID")
    election_clock = _require_int(
        session_obj["election_clock"],
        "session.election_clock",
        minimum=0,
        maximum=MAX_ELECTION_CLOCK,
    )

    node_ids_obj = _require_object(control_obj["node_ids"], "node_ids")
    if set(node_ids_obj) != {"honest-a", "honest-b"}:
        raise ValueError("node_ids must contain exactly honest-a and honest-b")
    honest_a = _require_string(node_ids_obj["honest-a"], "node_ids.honest-a", max_length=32)
    honest_b = _require_string(node_ids_obj["honest-b"], "node_ids.honest-b", max_length=32)
    for field, node_id in (("honest-a", honest_a), ("honest-b", honest_b)):
        if not valid_zid(node_id):
            raise ValueError(f"node_ids.{field} is not a valid Zenoh ID")
    if len({honest_a, honest_b, publisher_node_id}) != 3:
        raise ValueError("honest peers and publisher must have distinct node IDs")
    if master_node_id not in {honest_a, honest_b}:
        raise ValueError("session master_node_id must identify one of the two honest peers")

    target_node_id = _require_string(control_obj["target_node_id"], "target_node_id", max_length=32)
    if not valid_zid(target_node_id):
        raise ValueError("target_node_id is not a valid Zenoh ID")
    expected_target = honest_b if configured_arm == "forged" else publisher_node_id
    if target_node_id != expected_target:
        raise ValueError(
            f"target_node_id for arm {configured_arm!r} must be {expected_target!r}"
        )

    available_bytes = _require_int(
        control_obj["available_bytes"],
        "available_bytes",
        minimum=0,
        maximum=RAM_TOTAL_BYTES,
    )
    origin = _require_string(control_obj["origin"], "origin", max_length=128)
    if origin in {publisher_node_id, honest_a, honest_b}:
        raise ValueError("origin must be a SystemId distinct from each NodeId")
    origin_idx = _require_int(control_obj["origin_idx"], "origin_idx", minimum=0, maximum=0)

    return Control(
        master_node_id=master_node_id,
        election_clock=election_clock,
        honest_a_node_id=honest_a,
        honest_b_node_id=honest_b,
        target_node_id=target_node_id,
        available_bytes=available_bytes,
        origin=origin,
        origin_idx=origin_idx,
        arm=arm,
        seed=seed,
    )


def read_control(path: Path, **validation: Any) -> Control:
    raw_bytes = path.read_bytes()
    if len(raw_bytes) > MAX_CONTROL_BYTES:
        raise ValueError(f"control file exceeds {MAX_CONTROL_BYTES} bytes")
    try:
        value = strict_json_loads(raw_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise ValueError(f"control file is not valid strict JSON: {exc}") from exc
    return validate_control(value, **validation)


def effective_election_clock(control: Control) -> int:
    if control.arm == "stale-session":
        return control.election_clock - 1
    return control.election_clock


def deterministic_event_id(seed: int, arm: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"exo-local-audit:{seed}:{arm}"))


def _append_jsonl(path: Path, record: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(record, sort_keys=True, separators=(",", ":"), allow_nan=False))
        stream.write("\n")
        stream.flush()


def _write_json_atomic(path: Path, value: dict[str, Any]) -> None:
    tmp_path = path.with_name(f".{path.name}.tmp")
    with tmp_path.open("w", encoding="utf-8") as stream:
        json.dump(value, stream, sort_keys=True, indent=2, allow_nan=False)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(tmp_path, path)


def _session_dict(master_node_id: str, election_clock: int) -> dict[str, Any]:
    return {"master_node_id": master_node_id, "election_clock": election_clock}


def _write_record(evidence: Path, record: dict[str, Any]) -> None:
    _append_jsonl(evidence / "probe.jsonl", record)


async def _wait_for_control(
    path: Path,
    *,
    deadline: float,
    configured_arm: str,
    configured_seed: int,
    publisher_node_id: str,
) -> Control:
    import anyio

    while anyio.current_time() < deadline:
        if path.exists():
            return read_control(
                path,
                configured_arm=configured_arm,
                configured_seed=configured_seed,
                publisher_node_id=publisher_node_id,
            )
        await anyio.sleep(min(0.2, max(0.01, deadline - anyio.current_time())))
    raise TimeoutError("timed out waiting for control.json")


class _AckTracker:
    def __init__(self, anyio_module: Any):
        self._anyio = anyio_module
        self.records: list[dict[str, Any]] = []
        self._keys: set[tuple[int, str]] = set()
        self.changed = anyio_module.Event()

    def add(self, record: dict[str, Any]) -> bool:
        key = (record["origin_idx"], record["event_id"])
        if key in self._keys:
            return False
        self._keys.add(key)
        self.records.append(record)
        old_event = self.changed
        self.changed = self._anyio.Event()
        old_event.set()
        return True

    async def wait_count(self, count: int, timeout: float) -> bool:
        deadline = self._anyio.current_time() + max(0.0, timeout)
        while len(self.records) < count:
            remaining = deadline - self._anyio.current_time()
            if remaining <= 0:
                return False
            event = self.changed
            with self._anyio.move_on_after(remaining):
                await event.wait()
            if not event.is_set():
                return False
        return True


def _make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--arm", required=True, choices=ARMS)
    parser.add_argument("--seed", required=True, type=int)
    parser.add_argument("--timeout", type=int, default=90)
    parser.add_argument("--evidence", type=Path, default=Path("/evidence"))
    return parser


async def _run(args: argparse.Namespace) -> int:
    import anyio

    if not args.namespace or len(args.namespace) > 256:
        raise ValueError("namespace must be a non-empty string of at most 256 characters")
    if args.timeout < 1 or args.timeout > MAX_TIMEOUT_SECONDS:
        raise ValueError(f"timeout must be between 1 and {MAX_TIMEOUT_SECONDS} seconds")
    if args.seed < MIN_SEED or args.seed > MAX_SEED:
        raise ValueError(f"seed must be between {MIN_SEED} and {MAX_SEED}")

    # Import Exo only inside the runtime so stdlib-only validation tests work on
    # the host Python even when the upstream Exo dependencies are unavailable.
    from exo.routing.router import Router
    from exo.routing.topics import GLOBAL_EVENTS, LOCAL_EVENTS
    from exo.shared.types.common import NodeId, SessionId, SystemId
    from exo.shared.types.events import (
        EventId,
        GlobalForwarderEvent,
        LocalForwarderEvent,
        NodeGatheredInfo,
    )
    from exo.shared.types.profiling import MemoryUsage

    args.evidence.mkdir(parents=True, exist_ok=True)
    jsonl_path = args.evidence / "probe.jsonl"
    jsonl_path.write_text("", encoding="utf-8")

    publisher_node_id = make_publisher_node_id()
    expected_topics = {LOCAL_EVENTS.topic, GLOBAL_EVENTS.topic}

    class ReadyRouter(Router):
        def __init__(self, handle: Any):
            super().__init__(handle)
            self._probe_subscriptions: set[str] = set()
            self.probe_subscriptions_ready = anyio.Event()

        async def _networking_subscribe(self, topic: str) -> None:
            await super()._networking_subscribe(topic)
            self._probe_subscriptions.add(topic)
            if expected_topics.issubset(self._probe_subscriptions):
                self.probe_subscriptions_ready.set()

    router = ReadyRouter.create(
        publisher_node_id,
        namespace=args.namespace,
        listen_port=52414,
        discovery_service_port=52413,
    )
    await router.register_topic(LOCAL_EVENTS)
    await router.register_topic(GLOBAL_EVENTS)
    global_events = router.receiver(GLOBAL_EVENTS)
    local_event_sender = router.sender(LOCAL_EVENTS)
    acks = _AckTracker(anyio)
    marker_event_id: str | None = None

    ready_record = {
        "type": "ready",
        "publisher_node_id": publisher_node_id,
        "namespace": args.namespace,
        "arm": args.arm,
        "role": "router-only",
    }

    async def receive_global_events() -> None:
        with global_events as events:
            async for message in events:
                if not isinstance(message, GlobalForwarderEvent) or marker_event_id is None:
                    continue
                received_event_id = str(message.event.event_id)
                if received_event_id != marker_event_id:
                    continue
                serialized = message.model_dump_json()
                record = {
                    "type": "received",
                    "observer": "attacker",
                    "observer_node_id": publisher_node_id,
                    "event_id": received_event_id,
                    "origin_idx": message.origin_idx,
                    "session": _session_dict(
                        str(message.session.master_node_id),
                        message.session.election_clock,
                    ),
                    "payload": json.loads(serialized),
                    "payload_json": serialized,
                }
                if acks.add(record):
                    _write_record(args.evidence, record)

    def record_published(serialized: str, *, publication_number: int, control: Control, event_id: str) -> None:
        record = {
            "type": "published",
            "publication_number": publication_number,
            "publisher_node_id": publisher_node_id,
            "event_id": event_id,
            "node_id": control.target_node_id,
            "origin_idx": control.origin_idx,
            "origin": control.origin,
            "session": _session_dict(
                control.master_node_id,
                effective_election_clock(control),
            ),
            "available_bytes": control.available_bytes,
            "payload": json.loads(serialized),
            "payload_json": serialized,
        }
        _write_record(args.evidence, record)

    start_time = anyio.current_time()
    deadline = start_time + args.timeout
    publication_count = 0
    control_summary: dict[str, Any] | None = None
    done_status = "error"

    try:
        async with anyio.create_task_group() as task_group:
            task_group.start_soon(router.run)
            task_group.start_soon(receive_global_events)
            ready_remaining = min(15.0, max(0.0, deadline - anyio.current_time()))
            with anyio.move_on_after(ready_remaining):
                await router.probe_subscriptions_ready.wait()
            if not router.probe_subscriptions_ready.is_set():
                raise TimeoutError("timed out waiting for LOCAL_EVENTS and GLOBAL_EVENTS subscriptions")

            _write_json_atomic(args.evidence / "attacker-ready.json", ready_record)
            _write_record(args.evidence, ready_record)

            control = await _wait_for_control(
                args.evidence / "control.json",
                deadline=deadline,
                configured_arm=args.arm,
                configured_seed=args.seed,
                publisher_node_id=publisher_node_id,
            )
            marker_event_id = deterministic_event_id(control.seed, control.arm)
            session_clock = effective_election_clock(control)
            control_summary = {
                "arm": control.arm,
                "seed": control.seed,
                "target_node_id": control.target_node_id,
                "session": _session_dict(control.master_node_id, session_clock),
                "origin": control.origin,
                "origin_idx": control.origin_idx,
                "available_bytes": control.available_bytes,
                "marker_event_id": marker_event_id,
            }

            node_info_event = NodeGatheredInfo(
                event_id=EventId(marker_event_id),
                node_id=NodeId(control.target_node_id),
                when=datetime.now(timezone.utc).isoformat(),
                info=MemoryUsage.from_bytes(
                    ram_total=RAM_TOTAL_BYTES,
                    ram_available=control.available_bytes,
                    swap_total=0,
                    swap_available=0,
                ),
            )
            envelope = LocalForwarderEvent(
                origin_idx=control.origin_idx,
                origin=SystemId(control.origin),
                session=SessionId(
                    master_node_id=NodeId(control.master_node_id),
                    election_clock=session_clock,
                ),
                event=node_info_event,
            )
            payload_json = envelope.model_dump_json()

            if control.arm == "baseline":
                done_status = "baseline-no-publication"
            else:
                if publication_count >= MAX_PUBLICATIONS:
                    raise RuntimeError("publication limit reached")
                await local_event_sender.send(envelope)
                publication_count += 1
                record_published(
                    payload_json,
                    publication_number=publication_count,
                    control=control,
                    event_id=marker_event_id,
                )

                remaining = max(0.0, min(5.0, deadline - anyio.current_time()))
                first_ack = await acks.wait_count(1, remaining)
                if control.arm == "replay" and first_ack:
                    if publication_count >= MAX_PUBLICATIONS:
                        raise RuntimeError("publication limit reached before replay")
                    # The exact same envelope preserves event ID, session, origin,
                    # index, timestamp, and bytes; only one replay is ever sent.
                    await local_event_sender.send(envelope)
                    publication_count += 1
                    record_published(
                        payload_json,
                        publication_number=publication_count,
                        control=control,
                        event_id=marker_event_id,
                    )
                    remaining = max(0.0, min(5.0, deadline - anyio.current_time()))
                    await acks.wait_count(2, remaining)
                    done_status = "replay-observed-first-ack" if len(acks.records) == 1 else "replay-produced-second-ack"
                elif control.arm == "replay":
                    done_status = "replay-skipped-no-first-ack"
                elif first_ack:
                    done_status = "published-and-observed"
                elif control.arm == "stale-session":
                    done_status = "stale-session-no-ack-window"
                else:
                    done_status = "published-no-ack-window"

            observation_remaining = max(0.0, min(5.0, deadline - anyio.current_time()))
            if observation_remaining:
                await anyio.sleep(observation_remaining)

            done_record = {
                "type": "done",
                "status": done_status,
                "publications": publication_count,
                "acks": len(acks.records),
                "marker_event_id": marker_event_id,
                "control": control_summary,
            }
            _write_json_atomic(args.evidence / "probe-done.json", done_record)
            _write_record(args.evidence, done_record)

            # Keep the transport alive for the runner's before/after API snapshots.
            remaining = max(0.0, deadline - anyio.current_time())
            if remaining:
                await anyio.sleep(remaining)
            task_group.cancel_scope.cancel()
        return 0
    except BaseException as exc:
        # Preserve cancellation semantics for orchestrator shutdowns, but record
        # ordinary failures before returning a non-zero status.
        if isinstance(exc, anyio.get_cancelled_exc_class()):
            raise
        error_record = {
            "type": "error",
            "error": type(exc).__name__,
            "message": str(exc),
            "publications": publication_count,
            "acks": len(acks.records),
            "control": control_summary,
        }
        _write_record(args.evidence, error_record)
        _write_json_atomic(args.evidence / "probe-done.json", error_record)
        return 1


def main(argv: list[str] | None = None) -> int:
    import anyio

    args = _make_parser().parse_args(argv)
    try:
        return anyio.run(_run, args)
    except Exception as exc:
        args.evidence.mkdir(parents=True, exist_ok=True)
        record = {"type": "error", "error": type(exc).__name__, "message": str(exc)}
        _write_record(args.evidence, record)
        _write_json_atomic(args.evidence / "probe-done.json", record)
        print(f"probe failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
