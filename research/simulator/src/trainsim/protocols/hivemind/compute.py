"""Bounded event model for the native compute-share learning fixture.

This is a candidate model, not a claim of native validation. It uses a fixed
B-led group, native progress-readiness rules, sample-weighted numerical
aggregation and explicit delays supplied by a caller. Unsupported group,
recovery and state-future paths stop the run instead of inventing outcomes.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import math
from collections.abc import Callable, Mapping
from typing import Any

from ...engine import Engine
from ...policies.announcements import AnnouncementPolicy
from .numerical import aggregate, announced_views


DEFAULT_MEMBERS = ("B", "D", "A", "C")
PROGRESS_STARTUP_POLL_SECONDS = 3.0
PROGRESS_MIN_POLL_SECONDS = 0.5
PROGRESS_MAX_POLL_SECONDS = 10.0
PROGRESS_EXPECTED_DRIFT_PEERS = 3.0
PROGRESS_EXPECTED_DRIFT_RATE = 0.2
MATCHMAKING_WINDOW_SECONDS = 1.0
RENDEZVOUS_TIMEOUT_SECONDS = 15.0
MIN_GROUP_SIZE = 3
FALLBACK_DEADLINE_SECONDS = 2.0


class UnsupportedBehavior(RuntimeError):
    """A native path the bounded candidate does not model defensibly."""


@dataclass(frozen=True)
class ComputeConfig:
    """Inputs for one deterministic virtual-time candidate run.

    ``rates`` is the nominal number of real samples per second. A batch's
    paced interval is ``batch_size / rate`` plus the configured interbatch
    overhead. The gradient callback is evaluated at batch start and is treated
    as instantaneous; callers should put measured computation overhead in
    ``interbatch_overhead_seconds``. ``step_overhead_seconds`` applies only to
    ordinary, non-update optimizer calls.

    The two all-reduce latencies must be supplied from a declared calibration
    or marked as synthetic by the caller. Progress visibility defaults to zero
    as an explicit immediate-visibility assumption.
    """

    seed: int
    horizon_seconds: float
    rates: Mapping[str, float]
    gradient_latency_seconds: float
    state_latency_seconds: float
    step_overhead_seconds: Mapping[str, float] = field(default_factory=dict)
    interbatch_overhead_seconds: Mapping[str, float] = field(default_factory=dict)
    progress_latency_seconds: float = 0.0
    members: tuple[str, ...] | None = None
    dimension: int = 48
    target_batch_size: int = 4096
    batch_size: int = 32
    learning_rate: float = 0.1
    evaluation_interval_seconds: float = 1.0


def _number(value: Any, name: str, *, minimum: float = 0.0) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a finite number")
    result = float(value)
    if not math.isfinite(result) or result < minimum:
        raise ValueError(f"{name} must be finite and >= {minimum}")
    return result


def _positive_integer(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _per_node_values(values: Mapping[str, float], members: tuple[str, ...], name: str,
                     *, positive: bool = False) -> dict[str, float]:
    if not isinstance(values, Mapping) or set(values) - set(members):
        raise ValueError(f"{name} must map configured members to finite numbers")
    minimum = 1e-300 if positive else 0.0
    result = {node: 0.0 for node in members}
    for node, value in values.items():
        result[node] = _number(value, f"{name}[{node}]", minimum=minimum)
    if positive and any(value <= 0 for value in result.values()):
        raise ValueError(f"{name} must be positive for every member")
    return result


def _validate_config(config: ComputeConfig) -> dict[str, Any]:
    if not isinstance(config, ComputeConfig):
        raise TypeError("config must be a ComputeConfig")
    if isinstance(config.seed, bool) or not isinstance(config.seed, int) or config.seed < 0:
        raise ValueError("seed must be a non-negative integer")
    horizon = _number(config.horizon_seconds, "horizon_seconds")
    if config.members is None:
        if not isinstance(config.rates, Mapping):
            raise ValueError("rates must be a mapping")
        rate_nodes = set(config.rates)
        if not rate_nodes.issubset(DEFAULT_MEMBERS):
            raise ValueError("custom node names require an explicit members roster")
        members = tuple(node for node in DEFAULT_MEMBERS if node in rate_nodes)
    else:
        members = tuple(config.members)
    if (len(members) not in (3, 4) or len(set(members)) != len(members)
            or any(not isinstance(node, str) or not node for node in members)
            or "B" not in members):
        raise ValueError("members must be a unique 3- or 4-node roster with B as leader")
    rates = _per_node_values(config.rates, members, "rates", positive=True)
    step_overhead = _per_node_values(
        config.step_overhead_seconds, members, "step_overhead_seconds"
    )
    interbatch_overhead = _per_node_values(
        config.interbatch_overhead_seconds, members, "interbatch_overhead_seconds"
    )
    gradient_latency = _number(config.gradient_latency_seconds, "gradient_latency_seconds")
    state_latency = _number(config.state_latency_seconds, "state_latency_seconds")
    progress_latency = _number(config.progress_latency_seconds, "progress_latency_seconds")
    dimension = _positive_integer(config.dimension, "dimension")
    batch_size = _positive_integer(config.batch_size, "batch_size")
    target = _positive_integer(config.target_batch_size, "target_batch_size")
    learning_rate = _number(config.learning_rate, "learning_rate")
    evaluation_interval = _number(config.evaluation_interval_seconds, "evaluation_interval_seconds",
                                  minimum=1e-300)
    if target < batch_size:
        raise ValueError("target_batch_size must be at least one batch")
    return {
        "seed": config.seed,
        "horizon_seconds": horizon,
        "members": list(members),
        "target_group_size": len(members),
        "min_group_size": MIN_GROUP_SIZE,
        "rates": rates,
        "step_overhead_seconds": step_overhead,
        "interbatch_overhead_seconds": interbatch_overhead,
        "gradient_latency_seconds": gradient_latency,
        "state_latency_seconds": state_latency,
        "progress_latency_seconds": progress_latency,
        "dimension": dimension,
        "batch_size": batch_size,
        "target_batch_size": target,
        "learning_rate": learning_rate,
        "evaluation_interval_seconds": evaluation_interval,
        "progress_startup_poll_seconds": PROGRESS_STARTUP_POLL_SECONDS,
        "progress_min_poll_seconds": PROGRESS_MIN_POLL_SECONDS,
        "progress_max_poll_seconds": PROGRESS_MAX_POLL_SECONDS,
        "progress_expected_drift_peers": PROGRESS_EXPECTED_DRIFT_PEERS,
        "progress_expected_drift_rate": PROGRESS_EXPECTED_DRIFT_RATE,
        "matchmaking_window_seconds": MATCHMAKING_WINDOW_SECONDS,
        "rendezvous_timeout_seconds": RENDEZVOUS_TIMEOUT_SECONDS,
        "fallback_deadline_seconds": FALLBACK_DEADLINE_SECONDS,
        "assumptions": [
            "deterministic virtual time; fixed B-led gradient roster",
            "gradient callback is instantaneous and returns a microbatch-mean vector",
            "nominal sample pacing is 32/rate plus configured interbatch overhead",
            "ordinary step overhead applies only to non-update optimizer calls",
            "all-reduce delays are external calibration inputs; fallback adds the fixed 2s deadline",
            "progress visibility delay is caller-supplied; zero assumes immediate visibility",
            "state inputs are post-SGD parameters with equal native weights and honest local views",
            "recovery, partial gradient membership, rendezvous timeout, and unsupported overlap fail closed",
            "this candidate is not native validation or a convergence guarantee",
        ],
    }


def _json_safe(value: Any) -> Any:
    """Convert trace/evaluator values to ordinary JSON-compatible objects."""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_safe(item) for item in value]
    if hasattr(value, "item"):
        try:
            return _json_safe(value.item())
        except (TypeError, ValueError):
            pass
    if hasattr(value, "tolist"):
        try:
            return _json_safe(value.tolist())
        except (TypeError, ValueError):
            pass
    return repr(value)


@dataclass
class _Node:
    node_id: str
    parameters: Any
    gradient_sum: Any
    epoch: int = 0
    samples_accumulated: int = 0
    batch_index: int = 0
    samples_total: int = 0
    completed_updates: int = 0
    update_begin_times: list[float] = field(default_factory=list)
    update_times: list[float] = field(default_factory=list)
    busy: bool = False
    current_gradient: Any = None
    current_batch_index: int | None = None
    current_round: str | None = None
    last_report_samples: int = 0
    has_reported_samples: bool = False
    ema_alpha: float = 0.1
    ema_seconds_per_sample: float = 0.0
    ema_updates: int = 0
    samples_per_second: float = 1e-20
    ema_timestamp: float = 0.0
    cached_global_epoch: int = 0
    cached_global_samples: int = 0
    cached_eta_next_epoch: float = math.inf
    cached_num_peers: int = 0
    progress_poll_deferred: bool = False
    prescheduled_epoch: int | None = None
    pending_state: Any = None
    state_applied: set[str] = field(default_factory=set)


class _ComputeRun:
    def __init__(self, config: ComputeConfig, policy: Any,
                 gradient: Callable[[int, str, int, Any], Any],
                 evaluate: Callable[[float, Mapping[str, Any]], Any]):
        self.config = config
        self.resolved = _validate_config(config)
        self.members = tuple(self.resolved["members"])
        self.policy = policy
        if policy is None or not callable(getattr(policy, "decide", None)):
            raise TypeError("policy must be an announcement policy object with decide()")
        if not callable(gradient) or not callable(evaluate):
            raise TypeError("gradient and evaluate must be callable")
        import numpy as np

        self.np = np
        self.gradient_fn = gradient
        self.evaluate_fn = evaluate
        self.engine = Engine(seed=config.seed, sink=self._record)
        self.trace: list[dict[str, Any]] = []
        self.evaluations: list[dict[str, Any]] = []
        self.unsupported_reasons: list[str] = []
        self.progress: dict[str, dict[str, Any]] = {}
        self.requests: dict[str, float] = {}
        self.request_epoch: int | None = None
        self.request_timeout = None
        self.active_gradient: dict[str, Any] | None = None
        self.round_sequence = 0
        self.state_round: dict[str, Any] | None = None
        self.state_sequence = 0
        self.nodes: dict[str, _Node] = {
            node: _Node(node, np.zeros(config.dimension, dtype=np.float32),
                        np.zeros(config.dimension, dtype=np.float32))
            for node in self.members
        }

    def _record(self, time_value: float, kind: str, actor: str = "", **data: Any) -> None:
        self.trace.append({"time": float(time_value), "kind": kind, "actor": actor,
                           "data": _json_safe(data)})

    def _unsupported(self, reason: str, actor: str = "", **details: Any) -> None:
        if reason not in self.unsupported_reasons:
            self.unsupported_reasons.append(reason)
        self.engine.emit("unsupported_behavior", actor, reason=reason, **details)
        self.engine.request_stop()
        raise UnsupportedBehavior(reason)

    def _evaluate(self, time_value: float) -> None:
        models = {node: state.parameters.copy() for node, state in self.nodes.items()}
        row = self.evaluate_fn(float(time_value), models)
        if isinstance(row, Mapping):
            recorded = dict(row)
            recorded.setdefault("time", float(time_value))
        else:
            recorded = {"time": float(time_value), "value": row}
        self.evaluations.append(_json_safe(recorded))
        self.engine.emit("evaluation", "", time=float(time_value))

    def _schedule_evaluations(self) -> None:
        horizon = self.resolved["horizon_seconds"]
        interval = self.resolved["evaluation_interval_seconds"]
        tick = interval
        while tick < horizon:
            self.engine.call_at(tick, self._evaluate, tick)
            tick += interval

    def _start(self) -> None:
        # The evaluator owns time zero and the inclusive final boundary. No
        # evaluation timer is left exactly at Engine.run's half-open horizon.
        self._evaluate(0.0)
        self._schedule_evaluations()
        for node in self.members:
            state = self.nodes[node]
            state.ema_timestamp = 0.0
            self.engine.call_at(PROGRESS_STARTUP_POLL_SECONDS, self._poll_progress, node)
            self.engine.call_at(0.0, self._begin_batch, node)

    def _begin_batch(self, node_id: str) -> None:
        state = self.nodes[node_id]
        if state.busy or self.engine.stop_requested:
            return
        index = state.batch_index
        try:
            raw = self.gradient_fn(self.config.seed, node_id, index, state.parameters.copy())
            values = self.np.asarray(raw, dtype=self.np.float32).reshape(-1)
        except Exception as exc:
            self._unsupported("gradient_callback_failed", node_id,
                              batch_index=index, error=type(exc).__name__)
        if values.shape != (self.config.dimension,) or not self.np.isfinite(values).all():
            self._unsupported("invalid_gradient_vector", node_id, batch_index=index,
                              expected_dimension=self.config.dimension)
        state.current_gradient = values.copy()
        state.current_batch_index = index
        self.engine.emit("microbatch_computed", node_id, epoch=state.epoch,
                         batch_index=index, batch_size=self.config.batch_size,
                         model_version=state.completed_updates)
        delay = (self.config.batch_size / self.resolved["rates"][node_id]
                 + self.resolved["interbatch_overhead_seconds"][node_id])
        self.engine.schedule(delay, self._optimizer_call, node_id)

    def _optimizer_call(self, node_id: str) -> None:
        state = self.nodes[node_id]
        if state.busy or state.current_gradient is None:
            return
        self.engine.emit("optimizer_call", node_id, epoch=state.epoch,
                         batch_index=state.current_batch_index,
                         model_version=state.completed_updates)
        self._apply_completed_state(state, "optimizer_call")
        if state.cached_global_epoch > state.epoch:
            self._unsupported(
                "newer_global_epoch_transition_not_modeled", node_id,
                scope=("Conservative candidate guard: this epoch-skew transition is not modeled; "
                       "a newer epoch does not by itself prove native recovery is required."),
                local_epoch=state.epoch,
                global_epoch=state.cached_global_epoch,
                batch_index=state.current_batch_index,
            )

        state.gradient_sum += state.current_gradient * self.config.batch_size
        state.samples_accumulated += self.config.batch_size
        state.samples_total += self.config.batch_size
        batch_index = state.current_batch_index
        state.batch_index += 1
        state.current_gradient = None
        state.current_batch_index = None
        self._report_local_progress(state, state.samples_accumulated)
        self.engine.emit("gradient_accumulated", node_id, epoch=state.epoch,
                         batch_index=batch_index, sample_count=self.config.batch_size,
                         samples_accumulated=state.samples_accumulated,
                         samples_total=state.samples_total)
        self._maybe_preschedule(state)
        if self._ready_to_update(state):
            self._request_gradient_update(state)
            return

        overhead = self.resolved["step_overhead_seconds"][node_id]
        self.engine.emit("ordinary_step_begin", node_id, batch_index=batch_index,
                         step_overhead_seconds=overhead)
        self.engine.schedule(overhead, self._ordinary_step_done, node_id, batch_index)

    def _ordinary_step_done(self, node_id: str, batch_index: int) -> None:
        state = self.nodes[node_id]
        if state.busy or self.engine.stop_requested:
            return
        self.engine.emit("ordinary_step_end", node_id, batch_index=batch_index)
        self.engine.schedule(0.0, self._begin_batch, node_id)

    def _report_local_progress(self, state: _Node, samples: int) -> None:
        delta = samples - state.last_report_samples
        now = self.engine.now
        if delta > 0:
            interval = max(now - state.ema_timestamp, 1e-20)
            state.ema_seconds_per_sample = (
                state.ema_alpha * interval / delta
                + (1.0 - state.ema_alpha) * state.ema_seconds_per_sample
            )
            state.ema_updates += 1
            correction = 1.0 - (1.0 - state.ema_alpha) ** state.ema_updates
            state.samples_per_second = 1.0 / max(
                state.ema_seconds_per_sample / correction, 1e-20
            )
        else:
            # Native PerformanceEMA.reset_timer() keeps a stalled update out of
            # the next throughput interval.
            state.ema_timestamp = now
        if state.epoch == state.cached_global_epoch:
            state.cached_global_samples = max(0, state.cached_global_samples + delta)
        state.last_report_samples = samples
        if samples > 0:
            state.has_reported_samples = True
        record = {"epoch": state.epoch, "samples": samples,
                  "samples_per_second": state.samples_per_second,
                  "time": now, "node": state.node_id}
        self.engine.emit("progress_local", state.node_id, epoch=state.epoch,
                         samples=samples, samples_per_second=state.samples_per_second,
                         delta=delta)
        if state.has_reported_samples:
            self.engine.schedule(self.resolved["progress_latency_seconds"],
                                 self._publish_progress, state.node_id, record)
        if delta > 0:
            state.ema_timestamp = now

    def _publish_progress(self, node_id: str, record: dict[str, Any]) -> None:
        current = self.progress.get(node_id)
        if current is None or record["time"] >= current["time"]:
            self.progress[node_id] = dict(record, published_at=self.engine.now)
        self.engine.emit("progress_published", node_id, epoch=record["epoch"],
                         samples=record["samples"], report_time=record["time"])

    def _poll_progress(self, node_id: str) -> None:
        if self.engine.stop_requested:
            return
        state = self.nodes[node_id]
        if state.busy:
            state.progress_poll_deferred = True
            self.engine.emit("progress_poll_deferred", node_id,
                             epoch=state.epoch, reason="blocked_update")
            return
        now = self.engine.now
        records = list(self.progress.values())
        if not records:
            state.cached_global_epoch = state.epoch
            state.cached_global_samples = state.samples_accumulated
            rate = state.samples_per_second
            eta = (max(0, self.config.target_batch_size - state.samples_accumulated) / rate
                   if rate > 0 else math.inf)
            interval = PROGRESS_STARTUP_POLL_SECONDS
            num_peers = 0
        else:
            global_epoch = max([state.epoch] + [int(item["epoch"]) for item in records])
            current_records = [item for item in records if int(item["epoch"]) == global_epoch]
            global_samples = sum(int(item["samples"]) for item in current_records)
            estimated_samples = sum(
                int(item["samples"]) + max(0.0, now - float(item["time"]))
                * float(item["samples_per_second"])
                for item in current_records
            )
            total_rate = sum(float(item["samples_per_second"]) for item in records)
            samples_remaining = max(0.0, self.config.target_batch_size - estimated_samples)
            eta = samples_remaining / total_rate if total_rate > 0 else math.inf
            num_peers = len(records)
            state.cached_global_epoch = global_epoch
            state.cached_global_samples = global_samples
            if num_peers:
                denominator = max(num_peers + PROGRESS_EXPECTED_DRIFT_PEERS,
                                  num_peers * (1.0 + PROGRESS_EXPECTED_DRIFT_RATE))
                interval = min(PROGRESS_MAX_POLL_SECONDS,
                               max(PROGRESS_MIN_POLL_SECONDS, eta * num_peers / denominator))
            else:  # Kept explicit for the native zero-peer branch.
                interval = PROGRESS_STARTUP_POLL_SECONDS
        state.cached_eta_next_epoch = now + eta
        state.cached_num_peers = num_peers
        self.engine.emit("progress_polled", node_id,
                         global_epoch=state.cached_global_epoch,
                         global_samples=state.cached_global_samples,
                         num_peers=num_peers,
                         eta_seconds=eta,
                         next_poll_seconds=interval)
        self.engine.schedule(interval, self._poll_progress, node_id)

    def _ready_to_update(self, state: _Node) -> bool:
        ready = (state.cached_global_epoch > state.epoch
                 or state.cached_global_samples >= self.config.target_batch_size
                 or self.engine.now >= state.cached_eta_next_epoch)
        self.engine.emit("epoch_readiness", state.node_id, epoch=state.epoch,
                         global_epoch=state.cached_global_epoch,
                         samples_accumulated=state.cached_global_samples,
                         eta_next_epoch=state.cached_eta_next_epoch,
                         ready=ready)
        return ready

    def _maybe_preschedule(self, state: _Node) -> None:
        if state.prescheduled_epoch == state.epoch:
            return
        ready = (state.cached_global_epoch > state.epoch
                 or state.cached_global_samples >= self.config.target_batch_size
                 or self.engine.now >= state.cached_eta_next_epoch)
        eta = 0.0 if ready else state.cached_eta_next_epoch - self.engine.now
        if eta <= MATCHMAKING_WINDOW_SECONDS:
            state.prescheduled_epoch = state.epoch
            self.engine.emit("gradient_prescheduled", state.node_id,
                             epoch=state.epoch,
                             eta_seconds=max(eta, 0.0),
                             window_seconds=MATCHMAKING_WINDOW_SECONDS)

    def _request_gradient_update(self, state: _Node) -> None:
        if self.active_gradient is not None:
            self._unsupported("overlapping_gradient_rounds", state.node_id,
                              active_group=self.active_gradient["group_id"],
                              local_epoch=state.epoch)
        if self.request_epoch is None:
            self.request_epoch = state.epoch
            self.requests = {}
            self.request_timeout = self.engine.schedule(
                RENDEZVOUS_TIMEOUT_SECONDS, self._gradient_rendezvous_timeout,
                self.request_epoch,
            )
        if state.epoch != self.request_epoch:
            self._unsupported("gradient_epoch_skew", state.node_id,
                              request_epoch=self.request_epoch, local_epoch=state.epoch)
        if state.node_id in self.requests:
            self._unsupported("duplicate_gradient_request", state.node_id,
                              local_epoch=state.epoch)
        self.requests[state.node_id] = self.engine.now
        state.busy = True
        self.engine.emit("update_begin", state.node_id, epoch=state.epoch,
                         batch_index=state.batch_index - 1,
                         local_samples=state.samples_accumulated,
                         scheduled=state.prescheduled_epoch == state.epoch)
        state.update_begin_times.append(self.engine.now)
        self.engine.emit("gradient_join_request", state.node_id,
                         leader="B", epoch=state.epoch,
                         request_count=len(self.requests),
                         target_group_size=len(self.members),
                         min_group_size=MIN_GROUP_SIZE)
        if "B" in self.requests:
            self.engine.emit("gradient_leader_rendezvous", "B",
                             epoch=self.request_epoch,
                             target_group_size=len(self.members),
                             min_group_size=MIN_GROUP_SIZE,
                             timeout_seconds=RENDEZVOUS_TIMEOUT_SECONDS)
        if set(self.requests) == set(self.members):
            self._start_gradient_round()

    def _gradient_rendezvous_timeout(self, epoch: int) -> None:
        if self.request_epoch == epoch and self.active_gradient is None:
            missing = sorted(set(self.members) - set(self.requests))
            self._unsupported("gradient_rendezvous_timeout", "B",
                              epoch=epoch, joined=sorted(self.requests), missing=missing,
                              minimum=MIN_GROUP_SIZE, target=len(self.members))

    def _start_gradient_round(self) -> None:
        if self.request_epoch is None or "B" not in self.requests:
            self._unsupported("unsupported_gradient_leader", "B",
                              joined=sorted(self.requests))
        epoch = self.request_epoch
        if len(self.requests) < MIN_GROUP_SIZE or set(self.requests) != set(self.members):
            self._unsupported("unsupported_partial_gradient_membership", "B",
                              epoch=epoch, joined=sorted(self.requests), target=len(self.members))
        group_id = f"gradient-{self.round_sequence}-epoch-{epoch}"
        self.round_sequence += 1
        inputs: dict[str, Any] = {}
        weights: dict[str, int] = {}
        for node_id in self.members:
            state = self.nodes[node_id]
            if not state.busy or state.epoch != epoch or state.samples_accumulated <= 0:
                self._unsupported("invalid_gradient_group_input", node_id,
                                  group_id=group_id, epoch=state.epoch,
                                  samples=state.samples_accumulated)
            inputs[node_id] = state.gradient_sum / state.samples_accumulated
            weights[node_id] = state.samples_accumulated
        try:
            views = announced_views(self.members, "B", self.policy, group_id=group_id)
            numerical = aggregate(inputs, views, weights=weights)
        except Exception as exc:
            self._unsupported("unsupported_gradient_numerics_or_membership", "B",
                              group_id=group_id, error=type(exc).__name__,
                              detail=str(exc))
        round_state = {
            "group_id": group_id,
            "epoch": epoch,
            "members": self.members,
            "numerical": numerical,
            "completed": set(),
            "started_at": self.engine.now,
            "fallback_nodes": sorted(node for node, ranges in numerical.fallback_ranges.items()
                                      if ranges),
        }
        self.active_gradient = round_state
        if self.request_timeout is not None:
            self.request_timeout.cancel()
        self.engine.emit("gradient_group_started", "B", group_id=group_id,
                         epoch=epoch, members=self.members,
                         views=numerical.views,
                         weights=weights,
                         fallback_nodes=round_state["fallback_nodes"])
        for node_id in self.members:
            delay = self.resolved["gradient_latency_seconds"]
            fallback_ranges = numerical.fallback_ranges[node_id]
            if fallback_ranges:
                delay += FALLBACK_DEADLINE_SECONDS
            self.engine.schedule(delay, self._gradient_output_ready,
                                 node_id, group_id)

    def _gradient_output_ready(self, node_id: str, group_id: str) -> None:
        active = self.active_gradient
        if active is None or active["group_id"] != group_id:
            return
        if node_id in active["completed"]:
            self._unsupported("duplicate_gradient_output", node_id, group_id=group_id)
        active["completed"].add(node_id)
        numerical = active["numerical"]
        self.engine.emit("gradient_output_ready", node_id, group_id=group_id,
                         epoch=active["epoch"],
                         fallback_ranges=numerical.fallback_ranges[node_id],
                         delay_seconds=(self.engine.now - active["started_at"]))
        state_round = self.nodes[node_id].pending_state
        if state_round is not None:
            self._complete_state_if_due(state_round)
            if not state_round["complete"]:
                self._unsupported(
                    "prior_state_round_in_progress_at_optimizer_boundary", node_id,
                    state_group=state_round["group_id"], gradient_group=group_id,
                    state_rendezvous_started=state_round["started"],
                    state_complete_at=state_round["complete_at"],
                )
        self._commit_gradient_update(node_id, group_id)

    def _commit_gradient_update(self, node_id: str, group_id: str) -> None:
        active = self.active_gradient
        if active is None or active["group_id"] != group_id:
            return
        state = self.nodes[node_id]
        if state.pending_state is not None:
            self._complete_state_if_due(state.pending_state)
            if not state.pending_state["complete"]:
                self._unsupported("prior_state_round_in_progress_at_optimizer_boundary", node_id,
                                  state_group=state.pending_state["group_id"],
                                  gradient_group=group_id)
            self._apply_completed_state(state, "before_sgd")
        numerical = active["numerical"]
        output_gradient = numerical.values[node_id]
        state.parameters = self.np.asarray(
            state.parameters - self.config.learning_rate * output_gradient,
            dtype=self.np.float32,
        )
        previous_samples = state.samples_accumulated
        prior_epoch = state.epoch
        if prior_epoch != active["epoch"]:
            self._unsupported("gradient_epoch_skew_at_completion", node_id,
                              local_epoch=prior_epoch, group_epoch=active["epoch"])
        state.epoch = prior_epoch + 1
        state.gradient_sum = self.np.zeros(self.config.dimension, dtype=self.np.float32)
        state.samples_accumulated = 0
        state.last_report_samples = 0
        if state.epoch > state.cached_global_epoch:
            state.cached_global_epoch = state.epoch
            state.cached_global_samples = 0
            state.cached_eta_next_epoch = math.inf
        else:
            state.cached_global_samples = max(0, state.cached_global_samples - previous_samples)
        state.ema_timestamp = self.engine.now  # all-reduce blocking time is paused
        state.completed_updates += 1
        state.update_times.append(self.engine.now)
        state.busy = False
        state.current_round = None
        self.engine.emit("update_end", node_id, group_id=group_id,
                         prior_epoch=prior_epoch, epoch=state.epoch,
                         batch_index=state.batch_index - 1,
                         samples_accumulated=previous_samples,
                         completed_update_count=state.completed_updates,
                         model_version=state.completed_updates)
        self._report_local_progress(state, 0)
        self._offer_state(node_id, state.epoch)
        self.engine.schedule(0.0, self._begin_batch, node_id)
        if state.progress_poll_deferred:
            self.engine.schedule(0.0, self._resume_deferred_progress_poll, node_id)
        if active["completed"] == set(self.members):
            self.engine.emit("gradient_group_completed", "B", group_id=group_id,
                             epoch=active["epoch"],
                             completed_nodes=sorted(active["completed"]))
            self.active_gradient = None
            self.request_epoch = None
            self.requests = {}
            self.request_timeout = None

    def _resume_deferred_progress_poll(self, node_id: str) -> None:
        state = self.nodes[node_id]
        if not state.progress_poll_deferred:
            return
        state.progress_poll_deferred = False
        self.engine.emit("progress_poll_resumed", node_id, epoch=state.epoch)
        self._poll_progress(node_id)

    def _offer_state(self, node_id: str, epoch: int) -> None:
        if self.state_round is None:
            group_id = f"state-{self.state_sequence}-epoch-{epoch}"
            self.state_sequence += 1
            self.state_round = {
                "group_id": group_id,
                "epoch": epoch,
                "members": self.members,
                "offers": {},
                "started": False,
                "complete": False,
                "result": None,
                "complete_at": None,
                "timeout": None,
                "applied": set(),
            }
            self.state_round["timeout"] = self.engine.schedule(
                RENDEZVOUS_TIMEOUT_SECONDS, self._state_rendezvous_timeout, group_id
            )
        state_round = self.state_round
        if state_round["epoch"] != epoch:
            self._unsupported("state_epoch_skew", node_id,
                              state_group=state_round["group_id"],
                              expected_epoch=state_round["epoch"], actual_epoch=epoch)
        if node_id in state_round["offers"]:
            self._unsupported("duplicate_state_offer", node_id,
                              state_group=state_round["group_id"], epoch=epoch)
        state = self.nodes[node_id]
        if state.pending_state is not None:
            self._unsupported("overlapping_state_futures", node_id,
                              old_state_group=state.pending_state["group_id"],
                              new_state_group=state_round["group_id"])
        state_round["offers"][node_id] = state.parameters.copy()
        state.pending_state = state_round
        self.engine.emit("state_offer", node_id,
                         group_id=state_round["group_id"], epoch=epoch,
                         members=self.members, weight=1.0)
        if set(state_round["offers"]) == set(self.members):
            self._start_state_round(state_round)

    def _state_rendezvous_timeout(self, group_id: str) -> None:
        state_round = self.state_round
        if state_round is not None and state_round["group_id"] == group_id and not state_round["started"]:
            missing = sorted(set(self.members) - set(state_round["offers"]))
            self._unsupported("state_rendezvous_timeout", "B",
                              group_id=group_id, epoch=state_round["epoch"],
                              joined=sorted(state_round["offers"]), missing=missing)

    def _start_state_round(self, state_round: dict[str, Any]) -> None:
        if len(state_round["offers"]) < MIN_GROUP_SIZE:
            self._unsupported("unsupported_partial_state_membership", "B",
                              group_id=state_round["group_id"])
        inputs = {node: state_round["offers"][node] for node in self.members}
        honest_policy = AnnouncementPolicy("honest")
        try:
            views = announced_views(self.members, "B", honest_policy,
                                    group_id=state_round["group_id"])
            # Native state averaging uses equal weights. The numerical backend's
            # omitted-weight default is one per accepted peer.
            result = aggregate(inputs, views)
        except Exception as exc:
            self._unsupported("unsupported_state_numerics_or_membership", "B",
                              group_id=state_round["group_id"],
                              error=type(exc).__name__, detail=str(exc))
        state_round["started"] = True
        state_round["result"] = result
        state_round["started_at"] = self.engine.now
        state_round["complete_at"] = self.engine.now + self.resolved["state_latency_seconds"]
        if state_round["timeout"] is not None:
            state_round["timeout"].cancel()
        self.engine.emit("state_aggregation_started", "B",
                         group_id=state_round["group_id"], epoch=state_round["epoch"],
                         members=self.members, views=result.views,
                         weights="equal", latency_seconds=self.resolved["state_latency_seconds"])
        self.engine.schedule(self.resolved["state_latency_seconds"],
                             self._complete_state_round, state_round["group_id"])

    def _complete_state_if_due(self, state_round: dict[str, Any]) -> None:
        if not state_round["complete"] and state_round["complete_at"] is not None:
            if state_round["complete_at"] <= self.engine.now:
                state_round["complete"] = True
                self.engine.emit("state_aggregation_complete", "B",
                                 epoch=state_round["epoch"],
                                 latency_seconds=(self.engine.now - state_round["started_at"]))

    def _complete_state_round(self, group_id: str) -> None:
        state_round = self.state_round
        if state_round is None or state_round["group_id"] != group_id:
            return
        self._complete_state_if_due(state_round)

    def _apply_completed_state(self, state: _Node, occasion: str) -> None:
        state_round = state.pending_state
        if state_round is None:
            return
        self._complete_state_if_due(state_round)
        if not state_round["complete"]:
            return
        result = state_round["result"]
        state.parameters = self.np.asarray(result.values[state.node_id], dtype=self.np.float32).copy()
        state_round["applied"].add(state.node_id)
        state.pending_state = None
        state.state_applied.add(state_round["group_id"])
        self.engine.emit("state_applied", state.node_id,
                         group_id=state_round["group_id"], epoch=state_round["epoch"],
                         occasion=occasion, model_version=state.completed_updates)
        if state_round["applied"] == set(self.members):
            self.engine.emit("state_future_queue_drained", "B",
                             group_id=state_round["group_id"], epoch=state_round["epoch"])
            self.state_round = None

    def run(self) -> dict[str, Any]:
        self._start()
        horizon = self.resolved["horizon_seconds"]
        try:
            self.engine.run(horizon)
        except UnsupportedBehavior:
            pass
        if not self.unsupported_reasons:
            # Engine.run uses a half-open horizon; evaluate the exact final
            # snapshot once here so integer horizons are inclusive.
            if not self.evaluations or self.evaluations[-1]["time"] != horizon:
                self._evaluate(horizon)
        completed_counts = {node: state.completed_updates for node, state in self.nodes.items()}
        return {
            "supported": not self.unsupported_reasons,
            "unsupported": bool(self.unsupported_reasons),
            "unsupported_reasons": list(self.unsupported_reasons),
            "evaluations": list(self.evaluations),
            "update_begin_times": {node: list(state.update_begin_times)
                                   for node, state in self.nodes.items()},
            "update_times": {node: list(state.update_times)
                             for node, state in self.nodes.items()},
            "completed_update_counts": completed_counts,
            "sample_totals": {node: state.samples_total for node, state in self.nodes.items()},
            "final_time": self.engine.now,
            "config": self.resolved,
            "trace": list(self.trace),
        }


def run_compute(config: ComputeConfig, policy: Any,
                gradient: Callable[[int, str, int, Any], Any],
                evaluate: Callable[[float, Mapping[str, Any]], Any]) -> dict[str, Any]:
    """Run the bounded candidate and return JSON-safe rows and event evidence.

    ``gradient(seed, node, batch_index, parameters)`` must return a finite
    float-compatible vector of ``config.dimension`` microbatch-mean gradients.
    ``evaluate(time, models)`` receives a copy of each member's current model
    at time zero, each integer tick, and the inclusive final horizon.
    """
    return _ComputeRun(config, policy, gradient, evaluate).run()
