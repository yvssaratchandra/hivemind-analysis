"""Metrics and result recording for the Hivemind network-security experiments.

The three quantities that carry the whole study:

* **replica capture** (Track A) -- of the ``num_replicas`` peers responsible for a
  key, what share is attacker-controlled. Above ~1.0 the record is fully eclipsed.
* **local malicious fraction f_l** (Track B) -- of the peers a *specific victim*
  actually averages with, what share is attacker-controlled. Contrast with the
  **global** fraction f_g over the whole swarm; ``f_l / f_g`` is the amplification
  the attack achieves, and is the headline number of the project.
* **distortion / learning harm** (Track C) -- how far the aggregate is pushed from
  the value honest peers would have produced.
"""

from __future__ import annotations

import csv
import json
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

__all__ = ["fraction", "amplification", "Recorder", "summarize"]


def fraction(numerator: int, denominator: int) -> float:
    """Safe ratio: an empty denominator means 'no observation', reported as 0.0."""
    return numerator / denominator if denominator else 0.0


def amplification(f_local: float, f_global: float) -> float:
    """How much the attack concentrates malicious peers at the victim.

    ``f_l / f_g``. A value of 1.0 means the victim sees exactly the swarm-wide
    average (no attack advantage); values above 1.0 quantify the eclipse. Returns
    ``inf`` when the attacker achieves a local presence with zero global presence,
    which is only possible if f_g was mis-measured.
    """
    if f_global == 0:
        return math.inf if f_local > 0 else 1.0
    return f_local / f_global


@dataclass
class Recorder:
    """Accumulates experiment rows and writes them to CSV + a JSON sidecar.

    Every experiment writes one row per (condition, repetition) so that the raw
    observations -- not just the aggregates -- land on disk and results stay
    re-analysable without re-running the swarm.
    """

    name: str
    params: Dict[str, Any] = field(default_factory=dict)
    rows: List[Dict[str, Any]] = field(default_factory=list)

    def record(self, **row: Any) -> None:
        self.rows.append(row)

    def extend(self, rows: Iterable[Dict[str, Any]]) -> None:
        self.rows.extend(rows)

    @property
    def columns(self) -> List[str]:
        seen: Dict[str, None] = {}
        for row in self.rows:
            for key in row:
                seen.setdefault(key, None)
        return list(seen)

    def write(self, results_dir: Path | str) -> Path:
        """Write ``<name>.csv`` plus ``<name>.meta.json`` and return the CSV path."""
        results_dir = Path(results_dir)
        results_dir.mkdir(parents=True, exist_ok=True)

        csv_path = results_dir / f"{self.name}.csv"
        columns = self.columns
        with open(csv_path, "w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=columns)
            writer.writeheader()
            for row in self.rows:
                writer.writerow({column: row.get(column, "") for column in columns})

        meta_path = results_dir / f"{self.name}.meta.json"
        with open(meta_path, "w") as handle:
            json.dump({"name": self.name, "params": self.params, "num_rows": len(self.rows)}, handle, indent=2)
        return csv_path


def summarize(values: Sequence[float]) -> Dict[str, Optional[float]]:
    """mean / std / min / max / n over a set of repetitions."""
    values = [value for value in values if value is not None and not math.isnan(value)]
    if not values:
        return {"mean": None, "std": None, "min": None, "max": None, "n": 0}
    n = len(values)
    mean = sum(values) / n
    variance = sum((value - mean) ** 2 for value in values) / n if n > 1 else 0.0
    return {"mean": mean, "std": math.sqrt(variance), "min": min(values), "max": max(values), "n": n}
