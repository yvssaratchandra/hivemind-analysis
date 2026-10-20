"""Endpoint-constrained fluid network with deterministic max-min sharing."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Callable

from .engine import Engine
from .types import Message, NodeProfile, TimerHandle, TransferHandle


REGISTRY_ID = "__registry__"
REGISTRY_LATENCY_SECONDS = 0.025
_EPSILON_BYTES = 1e-7


@dataclass
class _Transfer:
    handle: TransferHandle
    message: Message
    deliver: Callable[[Message], None]
    total_bytes: float
    remaining_bytes: float
    started_at: float
    updated_at: float
    rate_bytes_per_second: float = 0.0
    state: str = "transmitting"
    generation: int = 0
    completion_timer: TimerHandle | None = None
    propagation_timer: TimerHandle | None = None
    stalled_reported: bool = False


class Network:
    """Transfer messages over shared endpoint upload and download capacities.

    ``NodeProfile.upload_bps`` and ``download_bps`` are interpreted as
    **bytes per second**, matching the simulator's seconds/bytes unit system
    and profile construction (`Mbps * 1e6 / 8`). Pairwise propagation delay is
    the sum of endpoint latency components. The registry has infinite
    bandwidth and a 25 ms endpoint component. Transfers use max-min fair
    endpoint allocation; intermediate links and TCP behavior are out of scope.
    """

    def __init__(
        self,
        engine: Engine,
        profiles: Mapping[str, NodeProfile],
        registry_latency_seconds: float = REGISTRY_LATENCY_SECONDS,
    ) -> None:
        self.engine = engine
        self.profiles = dict(profiles)
        if not math.isfinite(registry_latency_seconds) or registry_latency_seconds < 0:
            raise ValueError("registry latency must be finite and nonnegative")
        self.registry_latency_seconds = float(registry_latency_seconds)
        self._next_transfer_id = 1
        self._active: dict[int, _Transfer] = {}
        self._pending: dict[int, _Transfer] = {}
        self._uses_by_transfer: dict[int, tuple[tuple[str, str], ...]] = {}
        self._resource_users: dict[tuple[str, str], set[int]] = {}
        self._positive_rate_transfers = 0

        for node_id, profile in self.profiles.items():
            if node_id == REGISTRY_ID:
                raise ValueError(f"{REGISTRY_ID!r} is a reserved network endpoint")
            if profile.node_id != node_id:
                raise ValueError(f"profile key {node_id!r} does not match its node_id")
            self._validate_capacity(profile.upload_bps, f"{node_id} upload")
            self._validate_capacity(profile.download_bps, f"{node_id} download")
            if not math.isfinite(profile.latency_seconds) or profile.latency_seconds < 0:
                raise ValueError(f"{node_id} latency must be finite and nonnegative")

    @staticmethod
    def _validate_capacity(value: float, label: str) -> None:
        if math.isnan(value) or value < 0:
            raise ValueError(f"{label} capacity must be nonnegative")

    def _endpoint(self, node_id: str) -> tuple[float, float, float]:
        if node_id == REGISTRY_ID:
            return math.inf, math.inf, self.registry_latency_seconds
        try:
            profile = self.profiles[node_id]
        except KeyError as exc:
            raise ValueError(f"unknown network endpoint: {node_id!r}") from exc
        return profile.upload_bps, profile.download_bps, profile.latency_seconds

    def send(self, message: Message, deliver: Callable[[Message], None]) -> TransferHandle:
        """Schedule a transfer and invoke ``deliver`` after transmission and delay."""
        if isinstance(message.wire_bytes, bool) or not isinstance(message.wire_bytes, int):
            raise TypeError("message.wire_bytes must be an integer byte count")
        if message.wire_bytes < 0:
            raise ValueError("message.wire_bytes must be nonnegative")
        if not callable(deliver):
            raise TypeError("deliver must be callable")
        self._endpoint(message.sender)
        self._endpoint(message.recipient)

        transfer_id = self._next_transfer_id
        self._next_transfer_id += 1
        handle = TransferHandle(transfer_id=transfer_id)
        transfer = _Transfer(
            handle=handle,
            message=message,
            deliver=deliver,
            total_bytes=float(message.wire_bytes),
            remaining_bytes=float(message.wire_bytes),
            started_at=self.engine.now,
            updated_at=self.engine.now,
        )
        self._pending[transfer_id] = transfer
        self.engine.emit(
            "transfer_started",
            message.sender,
            transfer_id=transfer_id,
            recipient=message.recipient,
            message_id=message.message_id,
            message_kind=message.kind,
            wire_bytes=message.wire_bytes,
        )
        if message.sender == message.recipient:
            transfer.state = "propagating"
            transfer.remaining_bytes = 0.
            transfer.propagation_timer = self.engine.schedule(0., self._deliver, transfer_id)
            return handle
        self._active[transfer_id] = transfer
        self._index_transfer(transfer)
        self._reallocate(self._connected_component((transfer_id,)))
        return handle

    def cancel(self, handle: TransferHandle) -> None:
        """Cancel transmission or propagation if the delivery is still pending."""
        transfer = self._pending.get(handle.transfer_id)
        if transfer is None:
            if 0 < handle.transfer_id < self._next_transfer_id:
                return
            raise KeyError(f"unknown transfer id: {handle.transfer_id}")
        if transfer.handle is not handle:
            raise ValueError("transfer handle does not belong to this Network")

        phase = transfer.state
        if transfer.state == "transmitting":
            affected = self._connected_component((handle.transfer_id,))
            self._advance_active(affected)
            if transfer.completion_timer is not None:
                transfer.completion_timer.cancel()
                transfer.completion_timer = None
            self._active.pop(handle.transfer_id, None)
            self._unindex_transfer(handle.transfer_id)
            self._set_rate(transfer, 0.0)
        elif transfer.state == "propagating" and transfer.propagation_timer is not None:
            transfer.propagation_timer.cancel()
            transfer.propagation_timer = None
        else:
            return

        transfer.state = "cancelled"
        handle.cancelled = True
        self._pending.pop(handle.transfer_id, None)
        self.engine.emit(
            "transfer_cancelled",
            transfer.message.sender,
            transfer_id=handle.transfer_id,
            recipient=transfer.message.recipient,
            message_id=transfer.message.message_id,
            phase=phase,
            remaining_bytes=transfer.remaining_bytes,
        )
        remaining = affected - {handle.transfer_id} if phase == "transmitting" else set()
        if remaining:
            self._reallocate(remaining)
        elif self._active:
            self._emit_reallocated()

    @property
    def transfer_id(self) -> int:
        """Last allocated transfer ID, intended for diagnostics and tests."""
        return self._next_transfer_id - 1

    def _advance_active(self, transfer_ids: set[int] | None = None) -> None:
        now = self.engine.now
        active = self._active.values() if transfer_ids is None else (
            self._active[transfer_id] for transfer_id in transfer_ids if transfer_id in self._active
        )
        for transfer in active:
            elapsed = now - transfer.updated_at
            if elapsed < 0:
                raise RuntimeError("simulation time moved backwards")
            if transfer.rate_bytes_per_second > 0 and elapsed > 0:
                transfer.remaining_bytes = max(
                    0.0,
                    transfer.remaining_bytes
                    - transfer.rate_bytes_per_second * elapsed,
                )
            transfer.updated_at = now

    def _reallocate(self, transfer_ids: set[int] | None = None) -> None:
        """Advance and recalculate rates for one or more connected components."""
        affected = set(self._active) if transfer_ids is None else (set(transfer_ids) & self._active.keys())
        if not affected:
            return
        self._advance_active(affected)

        completed = []
        for transfer_id in affected:
            transfer = self._active.get(transfer_id)
            if transfer is not None and transfer.remaining_bytes <= self._epsilon(transfer):
                completed.append(transfer)
        for transfer in completed:
            self._finish_transmission(transfer)

        still_active = affected & self._active.keys()
        if not still_active:
            if self._active:
                self._emit_reallocated()
            return

        rates = self._max_min_rates(tuple(self._active[transfer_id] for transfer_id in still_active))
        for transfer_id in still_active:
            transfer = self._active[transfer_id]
            rate = rates[transfer_id]
            if math.isinf(rate):
                # No finite serialization constraint: finish at this timestamp.
                # Scheduling a zero-duration rate timer would never consume bytes.
                transfer.remaining_bytes = 0.0
                self._finish_transmission(transfer)
                continue
            old_rate = transfer.rate_bytes_per_second
            if rate != old_rate:
                if transfer.completion_timer is not None:
                    transfer.completion_timer.cancel()
                    transfer.completion_timer = None
                self._set_rate(transfer, rate)
            transfer.updated_at = self.engine.now
            if rate > 0:
                transfer.stalled_reported = False
                if transfer.completion_timer is None:
                    self._schedule_completion(transfer)
            elif not transfer.stalled_reported:
                transfer.stalled_reported = True
                self.engine.emit(
                    "transfer_stalled",
                    transfer.message.sender,
                    transfer_id=transfer_id,
                    recipient=transfer.message.recipient,
                    remaining_bytes=transfer.remaining_bytes,
                )
        self._emit_reallocated()

    def _schedule_completion(self, transfer: _Transfer) -> None:
        transfer.generation += 1
        generation = transfer.generation
        now = self.engine.now
        deadline = now + transfer.remaining_bytes / transfer.rate_bytes_per_second
        # A positive duration can round back to ``now`` at large virtual times.
        # Repeating that timer never consumes the final residual bytes.
        if deadline <= now:
            deadline = math.nextafter(now, math.inf)
        transfer.completion_timer = self.engine.schedule(
            deadline - now,
            self._on_transmission_timer,
            transfer.handle.transfer_id,
            generation,
        )

    def _emit_reallocated(self) -> None:
        self.engine.emit(
            "network_reallocated",
            "__network__",
            active_transfers=len(self._active),
            positive_rate_transfers=self._positive_rate_transfers,
        )

    @staticmethod
    def _epsilon(transfer: _Transfer) -> float:
        return max(_EPSILON_BYTES, transfer.total_bytes * 1e-12)

    def _on_transmission_timer(self, transfer_id: int, generation: int) -> None:
        transfer = self._active.get(transfer_id)
        if transfer is None or transfer.generation != generation:
            return
        transfer.completion_timer = None
        self._reallocate(self._connected_component((transfer_id,)))

    def _finite_resources(self, transfer: _Transfer) -> tuple[tuple[str, str], ...]:
        up_src, _, _ = self._endpoint(transfer.message.sender)
        _, down_dst, _ = self._endpoint(transfer.message.recipient)
        resources = []
        if math.isfinite(up_src):
            resources.append(("upload", transfer.message.sender))
        if math.isfinite(down_dst):
            resources.append(("download", transfer.message.recipient))
        return tuple(resources)

    def _index_transfer(self, transfer: _Transfer) -> None:
        transfer_id = transfer.handle.transfer_id
        resources = self._finite_resources(transfer)
        self._uses_by_transfer[transfer_id] = resources
        for resource in resources:
            self._resource_users.setdefault(resource, set()).add(transfer_id)

    def _unindex_transfer(self, transfer_id: int) -> None:
        for resource in self._uses_by_transfer.pop(transfer_id, ()):
            users = self._resource_users[resource]
            users.remove(transfer_id)
            if not users:
                del self._resource_users[resource]

    def _connected_component(self, seeds) -> set[int]:
        """Return active transfers linked through any finite endpoint resource."""
        component: set[int] = set()
        stack = [transfer_id for transfer_id in seeds if transfer_id in self._active]
        while stack:
            transfer_id = stack.pop()
            if transfer_id in component:
                continue
            component.add(transfer_id)
            for resource in self._uses_by_transfer.get(transfer_id, ()):
                stack.extend(self._resource_users.get(resource, set()) - component)
        return component

    def _set_rate(self, transfer: _Transfer, rate: float) -> None:
        old_positive = transfer.rate_bytes_per_second > 0
        new_positive = rate > 0
        if old_positive and not new_positive:
            self._positive_rate_transfers -= 1
        elif new_positive and not old_positive:
            self._positive_rate_transfers += 1
        transfer.rate_bytes_per_second = rate

    def _finish_transmission(self, transfer: _Transfer) -> None:
        transfer.remaining_bytes = 0.0
        if transfer.completion_timer is not None:
            transfer.completion_timer.cancel()
            transfer.completion_timer = None
        self._set_rate(transfer, 0.0)
        transfer.state = "propagating"
        self._active.pop(transfer.handle.transfer_id, None)
        self._unindex_transfer(transfer.handle.transfer_id)
        _, _, sender_latency = self._endpoint(transfer.message.sender)
        _, _, recipient_latency = self._endpoint(transfer.message.recipient)
        propagation = sender_latency + recipient_latency
        self.engine.emit(
            "transfer_transmitted",
            transfer.message.sender,
            transfer_id=transfer.handle.transfer_id,
            recipient=transfer.message.recipient,
            message_id=transfer.message.message_id,
            wire_bytes=transfer.message.wire_bytes,
            transmission_seconds=self.engine.now - transfer.started_at,
            propagation_seconds=propagation,
        )
        transfer.propagation_timer = self.engine.schedule(
            propagation, self._deliver, transfer.handle.transfer_id
        )

    def _deliver(self, transfer_id: int) -> None:
        transfer = self._pending.pop(transfer_id, None)
        if transfer is None or transfer.state != "propagating":
            return
        transfer.state = "delivered"
        self.engine.emit(
            "transfer_delivered",
            transfer.message.recipient,
            transfer_id=transfer_id,
            sender=transfer.message.sender,
            message_id=transfer.message.message_id,
            message_kind=transfer.message.kind,
            wire_bytes=transfer.message.wire_bytes,
            elapsed_seconds=self.engine.now - transfer.started_at,
        )
        transfer.deliver(transfer.message)

    def _max_min_rates(self, transfers: tuple[_Transfer, ...]) -> dict[int, float]:
        """Allocate max-min rates independently over connected finite resources.

        A transfer consumes at most two finite resources: its source upload and
        destination download endpoint. Infinite-capacity endpoints (notably the
        registry) do not connect otherwise independent flows. Building the
        resource incidence graph first avoids rescanning the whole network for
        each progressive-filling step when most transfers are unrelated.
        """
        rates = {transfer.handle.transfer_id: 0.0 for transfer in transfers}
        finite_constraints: dict[tuple[str, str], float] = {}
        uses: dict[int, tuple[tuple[str, str], ...]] = {}
        resource_users: dict[tuple[str, str], set[int]] = {}

        for transfer in transfers:
            up_src, _, _ = self._endpoint(transfer.message.sender)
            _, down_dst, _ = self._endpoint(transfer.message.recipient)
            endpoints: list[tuple[str, str]] = []
            if math.isfinite(up_src):
                resource = ("upload", transfer.message.sender)
                finite_constraints[resource] = up_src
                endpoints.append(resource)
            if math.isfinite(down_dst):
                resource = ("download", transfer.message.recipient)
                finite_constraints[resource] = down_dst
                endpoints.append(resource)
            transfer_id = transfer.handle.transfer_id
            uses[transfer_id] = tuple(endpoints)
            for resource in endpoints:
                resource_users.setdefault(resource, set()).add(transfer_id)

        if not finite_constraints:
            # An infinite-to-infinite transfer has zero serialization time.
            return {transfer_id: math.inf for transfer_id in rates}

        # Connected components are independent max-min allocation problems.
        unseen = set(rates)
        components: list[set[int]] = []
        while unseen:
            seed = unseen.pop()
            component = {seed}
            stack = [seed]
            while stack:
                transfer_id = stack.pop()
                for resource in uses[transfer_id]:
                    for neighbor in resource_users[resource]:
                        if neighbor in unseen:
                            unseen.remove(neighbor)
                            component.add(neighbor)
                            stack.append(neighbor)
            components.append(component)

        for component in components:
            component_resources = {
                resource for transfer_id in component for resource in uses[transfer_id]
            }
            if not component_resources:
                # This component contains only infinite-capacity transfers.
                for transfer_id in component:
                    rates[transfer_id] = math.inf
                continue

            unfrozen = set(component)
            resource_used = {resource: 0.0 for resource in component_resources}
            while unfrozen:
                candidates: list[tuple[float, tuple[str, str], set[int], float]] = []
                for resource in component_resources:
                    active_users = resource_users[resource] & unfrozen
                    if active_users:
                        residual = max(0.0, finite_constraints[resource] - resource_used[resource])
                        candidates.append((residual / len(active_users), resource,
                                           active_users, residual))

                if not candidates:
                    # Any remaining flow consumes no finite endpoint resource.
                    for transfer_id in unfrozen:
                        rates[transfer_id] = math.inf
                    break

                increment = min(value for value, _, _, _ in candidates)
                for transfer_id in unfrozen:
                    rates[transfer_id] += increment
                # Track resource use incrementally, rather than summing every
                # flow's rate for every resource at every filling step.
                for _, resource, active_users, _ in candidates:
                    resource_used[resource] += increment * len(active_users)

                saturated: set[tuple[str, str]] = set()
                for _, resource, active_users, residual in candidates:
                    amount = increment * len(active_users)
                    tolerance = max(1e-9, finite_constraints[resource] * 1e-12)
                    if residual - amount <= tolerance:
                        saturated.add(resource)
                if not saturated:
                    # Floating point ties should still make monotonic progress.
                    saturated.add(min(candidates, key=lambda item: item[0])[1])
                newly_frozen = {
                    transfer_id
                    for resource in saturated
                    for transfer_id in (resource_users[resource] & unfrozen)
                }
                if not newly_frozen:
                    raise RuntimeError("max-min allocation made no progress")
                unfrozen.difference_update(newly_frozen)

        return rates
