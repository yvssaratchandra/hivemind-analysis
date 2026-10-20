"""Deterministic virtual-time scheduler built on SimPy."""

from __future__ import annotations

import math
from collections.abc import Callable
from typing import Any

import simpy
from simpy.events import URGENT, Event

from .types import EventSink, TimerHandle


class Engine:
    """Own a SimPy environment and expose absolute-time scheduling helpers.

    ``run(until)`` treats ``until`` as an absolute simulated time and does not
    process callbacks scheduled exactly at that time. This makes a horizon a
    half-open interval and leaves deadline equality checks to protocol code.
    """

    def __init__(self, seed: int | str = 0, sink: EventSink | None = None) -> None:
        self.seed = seed
        self.env = simpy.Environment()
        self.sink = sink
        self.stop_requested = False

    @property
    def now(self) -> float:
        """Return the current simulated time in seconds."""
        return float(self.env.now)

    def schedule(
        self, delay: float, callback: Callable[..., Any], *args: Any
    ) -> TimerHandle:
        """Schedule a callback after a nonnegative simulated delay."""
        delay = float(delay)
        if not math.isfinite(delay) or delay < 0:
            raise ValueError("delay must be a finite nonnegative number")
        if not callable(callback):
            raise TypeError("callback must be callable")

        handle = TimerHandle()

        def waiter():
            yield self.env.timeout(delay)
            if not handle.cancelled and not self.stop_requested:
                callback(*args)

        self.env.process(waiter())
        return handle

    def call_at(
        self, time: float, callback: Callable[..., Any], *args: Any
    ) -> TimerHandle:
        """Schedule a callback at an absolute simulated time."""
        time = float(time)
        if not math.isfinite(time) or time < self.now:
            raise ValueError("time must be finite and no earlier than engine.now")
        return self.schedule(time - self.now, callback, *args)

    def request_stop(self) -> None:
        """Stop the current run after the callback requesting it returns."""
        self.stop_requested = True

    def run(self, until: float) -> None:
        """Advance events up to the absolute simulated time ``until``."""
        until = float(until)
        if not math.isfinite(until) or until < self.now:
            raise ValueError("until must be finite and no earlier than engine.now")
        if self.stop_requested or until == self.now:
            return

        # SimPy's numeric ``run(until=...)`` uses an urgent event to leave all
        # regular callbacks at the horizon pending. Reproduce that boundary
        # while stepping manually so a callback can request an earlier stop.
        horizon = Event(self.env)
        horizon._ok = True
        horizon._value = None
        self.env.schedule(horizon, priority=URGENT, delay=until - self.now)
        while not self.stop_requested and self.env.peek() <= until:
            self.env.step()
            if self.now >= until:
                break

    def emit(self, kind: str, actor: str = "", **data: Any) -> None:
        """Forward an event to the configured sink at the current time."""
        if self.sink is not None:
            self.sink(self.now, kind, actor, **data)
