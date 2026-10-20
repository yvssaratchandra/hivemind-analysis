"""Seed-level summaries with deterministic bootstrap uncertainty intervals."""

from __future__ import annotations

import math
import random
import statistics
from collections.abc import Iterable, Mapping
from typing import Any


def _quantile(values: list[float], probability: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction


def bootstrap_summary(
    run_summaries: Iterable[Mapping[str, Any]],
    metrics: Iterable[str] | None = None,
    resamples: int = 2000,
    seed: int = 0,
) -> dict[str, dict[str, Any]]:
    """Summarize independent run-level metrics and bootstrap their medians.

    Missing, null, non-numeric and non-finite observations are omitted per metric.
    Resampling is over runs (seeds), never over groups from a single run.
    """
    if type(resamples) is not int or resamples < 1:
        raise ValueError("resamples must be a positive integer")
    rows = list(run_summaries)
    if metrics is None:
        keys = {key for row in rows for key in row}
        metrics = sorted(key for key in keys if key in {
            "accepted_work",
            "accepted_work_at_observation",
            "first_accept_time",
            "target_time",
            "last_time",
            "final_time",
        })
    rng = random.Random(seed)
    result: dict[str, dict[str, Any]] = {}
    for metric in metrics:
        values = []
        for row in rows:
            value = row.get(metric)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                continue
            numeric = float(value)
            if math.isfinite(numeric):
                values.append(numeric)
        if not values:
            result[str(metric)] = {
                "n": 0,
                "median": None,
                "iqr": [None, None],
                "bootstrap_95_percentile_interval": [None, None],
            }
            continue
        boot_medians = []
        for _ in range(resamples):
            sample = [values[rng.randrange(len(values))] for _ in values]
            boot_medians.append(float(statistics.median(sample)))
        result[str(metric)] = {
            "n": len(values),
            "median": float(statistics.median(values)),
            "iqr": [_quantile(values, 0.25), _quantile(values, 0.75)],
            "bootstrap_95_percentile_interval": [
                _quantile(boot_medians, 0.025),
                _quantile(boot_medians, 0.975),
            ],
        }
    return result
